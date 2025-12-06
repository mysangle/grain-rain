
use intrusive_collections::{intrusive_adapter, LinkedList, LinkedListLink};
use rustc_hash::FxHashMap;
use std::sync::Arc;
use super::pager::PageRef;

const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED: usize = 100_000;
const CLEAR: u8 = 0;
const REF_MAX: u8 = 3;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct PageCacheKey(usize);

impl PageCacheKey {
    pub fn new(pgno: usize) -> Self {
        Self(pgno)
    }
}

struct PageCacheEntry {
    key: PageCacheKey,
    page: PageRef,
    // 페이지에 대한 reference count. 0이면 evict. 최대값은 REF_MAX로 제한
    ref_bit: u8,
    link: LinkedListLink,
}

// struct에서 어떤 필드를 링크로 사용할지 지정
intrusive_adapter!(EntryAdapter = Box<PageCacheEntry>: PageCacheEntry { link: LinkedListLink });

impl PageCacheEntry {
    fn new(key: PageCacheKey, page: PageRef) -> Box<Self> {
        Box::new(Self {
            key,
            page,
            ref_bit: CLEAR,
            link: LinkedListLink::new(),
        })
    }

    #[inline]
    fn bump_ref(&mut self) {
        self.ref_bit = std::cmp::min(self.ref_bit + 1, REF_MAX);
    }

    #[inline]
    /// Returns the old value
    fn decrement_ref(&mut self) -> u8 {
        let old = self.ref_bit;
        self.ref_bit = old.saturating_sub(1);
        old
    }
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum CacheError {
    #[error("page {pgno} is dirty")]
    Dirty { pgno: usize },
    #[error("Page cache is full")]
    Full,
    #[error("{0}")]
    InternalError(String),
    #[error("key already exists")]
    KeyExists,
    #[error("page {pgno} is locked")]
    Locked { pgno: usize },
    #[error("page {pgno} is pinned")]
    Pinned { pgno: usize },
}

#[derive(Debug, PartialEq)]
pub enum CacheResizeResult {
    Done,
    PendingEvictions,
}

/// 자주 사용되는 페이지들을 메모리에 유지
/// *mut PageCacheEntry: 소유권을 갖지 않는 단순한 메모리 주소 값
/// 실제 동기화는 외부에서 관리될 것을 가정
///   - Pager의 경우 Arc<RwLock<PageCache>>로 사용
/// 
/// SIEVE/Clock류 알고리즘
///   - 완전한 LRU가 아니다.
///   - 단순 삽입만 하면 newest부터 삭제될 수도 있다
pub struct PageCache {
    capacity: usize,
    // 페이지를 빠르게 찾기 위한 index 역할
    map: FxHashMap<PageCacheKey, *mut PageCacheEntry>,
    // PageCacheEntry를 소유하고 관리(PageCacheEntry는 Box로 생성되므로 힘에 할당됨)
    queue: LinkedList<EntryAdapter>,
    // 다음에 검사할 페이지를 가리키는 포인터
    clock_hand: *mut PageCacheEntry,
}

unsafe impl Send for PageCache {}
unsafe impl Sync for PageCache {}

impl Default for PageCache {
    fn default() -> Self {
       PageCache::new(
            DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED,
        )
    }
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            map: FxHashMap::default(),
            queue: LinkedList::new(EntryAdapter::new()),
            clock_hand: std::ptr::null_mut(),
        }
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        self.map.contains_key(key)
    }

    #[inline]
    pub fn get(&mut self, key: &PageCacheKey) -> crate::Result<Option<PageRef>> {
        let Some(&entry_ptr) = self.map.get(key) else {
            return Ok(None);
        };

        let entry = unsafe { &mut *entry_ptr };
        let page = entry.page.clone();

        if !page.is_loaded() && !page.is_locked() {
            self.delete(*key)?;
            return Ok(None);
        }

        entry.bump_ref();
        Ok(Some(page))
    }

    #[inline]
    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, false)
    }

    #[inline]
    pub fn upsert_page(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, true)
    }

    #[inline]
    pub fn delete(&mut self, key: PageCacheKey) -> Result<(), CacheError> {
        tracing::trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    pub fn _insert(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
        update_in_place: bool,
    ) -> Result<(), CacheError> {
        tracing::trace!("insert(key={:?})", key);

        if let Some(&entry_ptr) = self.map.get(&key) {
            let entry = unsafe { &mut *entry_ptr };
            let p = &entry.page;

            if !p.is_loaded() && !p.is_locked() {
                // evict
                self._delete(key, true)?;
            } else {
                // reference count를 올린다.
                entry.bump_ref();
                if update_in_place {
                    entry.page = value;
                    return Ok(())
                } else {
                    assert!(
                        Arc::ptr_eq(&entry.page, &value),
                        "Attempted to insert different page with same key: {key:?}"
                    );
                    return Err(CacheError::KeyExists);
                }
            }
        }

        // 키가 없는 경우
        self.make_room_for(1)?;

        // 1. queue에 넣는다.
        // 2. queue에서 PageCacheEntry에의 포인터를 얻어온다.
        // 3. 포인터를 map에 넣는다.
        let entry = PageCacheEntry::new(key, value);
        if self.clock_hand.is_null() {
            // First entry - just push it
            self.queue.push_back(entry);
            let entry_ptr = self.queue.back().get().unwrap() as *const _ as *mut PageCacheEntry;
            self.map.insert(key, entry_ptr);
            self.clock_hand = entry_ptr;
        } else {
            // clock_hand 다음 위치에 entry를 넣는다.
            // Insert after clock hand (in circular list semantics, this makes it the new head/MRU)
            unsafe {
                let mut cursor = self.queue.cursor_mut_from_ptr(self.clock_hand);
                cursor.insert_after(entry);
                // The inserted entry is now at the next position after clock hand
                cursor.move_next();
                let entry_ptr = cursor.get().ok_or_else(|| {
                    CacheError::InternalError("Failed to get inserted entry pointer".into())
                })? as *const PageCacheEntry as *mut PageCacheEntry;
                self.map.insert(key, entry_ptr);
            }
        }

        Ok(())
    }

    fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        let Some(&entry_ptr) = self.map.get(&key) else {
            return Ok(());
        };

        let entry = unsafe { &mut *entry_ptr };
        let page = &entry.page;

        if page.is_locked() {
            return Err(CacheError::Locked {
                pgno: page.get().id,
            });
        }
        if page.is_dirty() {
            return Err(CacheError::Dirty {
                pgno: page.get().id,
            });
        }
        if page.is_pinned() {
            return Err(CacheError::Pinned {
                pgno: page.get().id,
            });
        }

        if clean_page {
            page.clear_loaded();
            let _ = page.get().contents.take();
        }

        self.map.remove(&key);

        if self.clock_hand == entry_ptr {
            // clock_hand가 delete하는 페이지를 가리키고 있다.
            self.advance_clock_hand();
            // 여전히 같은 엔트리를 가리키는지 한번 더 확인
            if self.clock_hand == entry_ptr {
                self.clock_hand = std::ptr::null_mut();
            }
        }

        // 큐에서 엔트리를 제거
        unsafe {
            let mut cursor = self.queue.cursor_mut_from_ptr(entry_ptr);
            cursor.remove();
        }

        Ok(())
    }

    #[inline]
    pub fn peek(&mut self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        let &entry_ptr = self.map.get(key)?;
        let entry = unsafe { &mut *entry_ptr };
        let page = entry.page.clone();
        if touch {
            entry.bump_ref();
        }
        Some(page)
    }

    /// Resizes the cache to a new capacity
    /// If shrinking, attempts to evict pages.
    /// If growing, simply increases capacity.
    pub fn resize(&mut self, new_cap: usize) -> CacheResizeResult {
        if new_cap == self.capacity {
            return CacheResizeResult::Done;
        }

        // Evict entries one by one until we're at new capacity
        while new_cap < self.len() {
            if self.evict_one().is_err() {
                return CacheResizeResult::PendingEvictions;
            }
        }

        self.capacity = new_cap;
        CacheResizeResult::Done
    }

    pub fn clear(&mut self, clear_dirty: bool) -> Result<(), CacheError> {
        // Check all pages are clean
        for &entry_ptr in self.map.values() {
            let entry = unsafe { &*entry_ptr };
            if entry.page.is_dirty() && !clear_dirty {
                return Err(CacheError::Dirty {
                    pgno: entry.page.get().id,
                });
            }
        }

        // Clean all pages
        for &entry_ptr in self.map.values() {
            let entry = unsafe { &*entry_ptr };
            entry.page.clear_loaded();
            let _ = entry.page.get().contents.take();
        }

        self.map.clear();
        self.queue.clear();
        self.clock_hand = std::ptr::null_mut();
        Ok(())
    }

    pub fn make_room_for(&mut self, n: usize) -> Result<(), CacheError> {
        if n > self.capacity {
            return Err(CacheError::Full);
        }
        let available = self.capacity - self.len();
        if n <= available {
            // n 크기 만큼 insert 가능
            return Ok(());
        }

        // insert를 위한 공간이 없는 경우 evict
        let need = n - available;
        for _ in 0..need {
            self.evict_one()?;
        }
        Ok(())
    }

    fn evict_one(&mut self) -> Result<(), CacheError> {
        if self.len() == 0 {
            return Err(CacheError::InternalError(
                "Cannot evict from empty cache".into(),
            ));
        }

        let mut examined = 0usize;
        let max_examinations = self.len().saturating_mul(REF_MAX as usize + 1);
        // clock_hand에서부터 eviction 가능 여부 확인
        //   - ref_bit가 0이면 evict
        //   - ref_bit가 0보다 크면 1을 줄인다.
        while examined < max_examinations {
            assert!(
                !self.clock_hand.is_null(),
                "clock hand is null but cache has {} entries",
                self.len()
            );

            let entry_ptr = self.clock_hand;
            let entry = unsafe { &mut *entry_ptr };
            let key = entry.key;
            let page = &entry.page;

            let evictable = !page.is_dirty() && !page.is_locked() && !page.is_pinned();
            if evictable && entry.ref_bit == CLEAR {
                // Evict this entry
                self.advance_clock_hand();
                // Check if clock hand wrapped back to the same entry (meaning this is the only/last entry)
                if self.clock_hand == entry_ptr {
                    self.clock_hand = std::ptr::null_mut();
                }

                self.map.remove(&key);

                // Clean the page
                page.clear_loaded();
                let _ = page.get().contents.take();

                // Remove from queue
                unsafe {
                    let mut cursor = self.queue.cursor_mut_from_ptr(entry_ptr);
                    cursor.remove();
                }

                // 아이템 하나를 evict 했으므로 리턴
                return Ok(());
            } else if evictable {
                // Decrement ref bit and continue
                entry.decrement_ref();
                self.advance_clock_hand();
                examined += 1;
            } else {
                // Skip unevictable page
                self.advance_clock_hand();
                examined += 1;
            }
        }

        // evict 할 entry가 없다.
        Err(CacheError::Full)
    }

    /// clock_hand를 다음 entry를 가리키도록 한다.
    /// 끝을 가리키고 있던 경우라면 처음을 가리키도록 한다.
    fn advance_clock_hand(&mut self) {
        if self.clock_hand.is_null() {
            return;
        }

        unsafe {
            let mut cursor = self.queue.cursor_mut_from_ptr(self.clock_hand);
            cursor.move_next();

            if cursor.get().is_some() {
                self.clock_hand =
                    cursor.as_cursor().get().unwrap() as *const _ as *mut PageCacheEntry;
            } else {
                // Reached end, wrap to front
                let front_cursor = self.queue.front_mut();
                if front_cursor.get().is_some() {
                    self.clock_hand =
                        front_cursor.as_cursor().get().unwrap() as *const _ as *mut PageCacheEntry;
                } else {
                    self.clock_hand = std::ptr::null_mut();
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}