
use arc_swap::ArcSwapOption;
use crate::{
    io_yield_one, return_if_io,
    error::GrainError,
    storage::{
        buffer_pool::BufferPool,
        database::DatabaseStorage,
        page_cache::{PageCache, PageCacheKey},
        sqlite3_ondisk::{
            self,
            begin_write_btree_page,
            DatabaseHeader, PageContent, PageSize, PageType,
        },
        wal::Wal,
    },
    types::IOCompletions,
    util::IOExt,
    Buffer, Completion, IOContext, IOResult, Result,
};
use parking_lot::{Mutex, RwLock};
use std::{
    cell::{RefCell, UnsafeCell},
    collections::BTreeSet,
    rc::Rc,
    sync::{
        atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use super::{btree::btree_init_page, page_cache::CacheError};

const RESERVED_SPACE_NOT_SET: u16 = u16::MAX;

/// 유효한 WAL tag가 설정되지 않음
pub const TAG_UNSET: u64 = u64::MAX;

/// Bit layout:
/// epoch: 20
/// frame: 44
const EPOCH_BITS: u32 = 20;
const FRAME_BITS: u32 = 64 - EPOCH_BITS;
const EPOCH_SHIFT: u32 = FRAME_BITS;
const EPOCH_MAX: u32 = (1u32 << EPOCH_BITS) - 1;
const FRAME_MAX: u64 = (1u64 << FRAME_BITS) - 1;

#[inline]
pub fn pack_tag_pair(frame: u64, seq: u32) -> u64 {
    ((seq as u64) << EPOCH_SHIFT) | (frame & FRAME_MAX)
}

#[derive(Clone, Debug)]
enum HeaderRefState {
    Start,
    CreateHeader { page: PageRef },
}

#[derive(Clone, Debug)]
pub struct HeaderRef(PageRef);

impl HeaderRef {
    pub fn from_pager(pager: &Pager) -> Result<IOResult<Self>> {
        loop {
            let state = pager.header_ref_state.read().clone();
            tracing::trace!("HeaderRef::from_pager - {:?}", state);
            match state {
                HeaderRefState::Start => {
                    if let Some(page1) = pager.init_page_1.load_full() {
                        return Ok(IOResult::Done(Self(page1)));
                    }

                    let (page, c) = pager.read_page(DatabaseHeader::PAGE_ID as i64)?;
                    *pager.header_ref_state.write() = HeaderRefState::CreateHeader { page };
                    // 페이지 캐시에서 페이지를 읽어온 경우에는 c가 None이므로 바로 다음 state로 간다.
                    if let Some(c) = c {
                        // io completion을 기다린다.
                        io_yield_one!(c);
                    }
                }
                HeaderRefState::CreateHeader { page } => {
                    assert!(page.is_loaded(), "page should be loaded");
                    assert!(
                        page.get().id == DatabaseHeader::PAGE_ID,
                        "incorrect header page id"
                    );
                    *pager.header_ref_state.write() = HeaderRefState::Start;
                    break Ok(IOResult::Done(Self(page)));
                }
            }
        }
    }

    pub fn borrow(&self) -> &DatabaseHeader {
        // TODO: Instead of erasing mutability, implement `get_mut_contents` and return a shared reference.
        let content: &PageContent = self.0.get_contents();
        bytemuck::from_bytes::<DatabaseHeader>(&content.buffer.as_slice()[0..DatabaseHeader::SIZE])
    }
}

#[derive(Debug)]
pub struct Page {
    pub inner: UnsafeCell<PageInner>,
}

unsafe impl Send for Page {}
unsafe impl Sync for Page {}

pub type PageRef = Arc<Page>;

/// Page is locked for I/O to prevent concurrent access.
const PAGE_LOCKED: usize = 0b010;
/// Page is dirty. Flush needed.
const PAGE_DIRTY: usize = 0b1000;
/// Page's contents are loaded in memory.
const PAGE_LOADED: usize = 0b10000;

impl Page {
    pub fn new(id: i64) -> Self {
        assert!(id >= 0, "page id should be positive");
        Self {
            inner: UnsafeCell::new(PageInner {
                flags: AtomicUsize::new(0),
                contents: None,
                id: id as usize,
                pin_count: AtomicUsize::new(0),
                wal_tag: AtomicU64::new(TAG_UNSET),
            }),
        }
    }

    pub fn get(&self) -> &mut PageInner {
        unsafe { &mut *self.inner.get() }
    }

    pub fn get_contents(&self) -> &mut PageContent {
        self.get().contents.as_mut().unwrap()
    }

    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        tracing::debug!("set_dirty(page={})", self.get().id);
        self.clear_wal_tag();
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        tracing::debug!("clear_dirty(page={})", self.get().id);
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::Release);
        self.clear_wal_tag();
    }

    pub fn is_locked(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::Acquire);
    }

    pub fn clear_locked(&self) {
        self.get().flags.fetch_and(!PAGE_LOCKED, Ordering::Release);
    }

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::Release);
    }

    pub fn clear_loaded(&self) {
        tracing::debug!("clear loaded {}", self.get().id);
        self.get().flags.fetch_and(!PAGE_LOADED, Ordering::Release);
    }

    pub fn is_pinned(&self) -> bool {
        self.get().pin_count.load(Ordering::Acquire) > 0
    }

    #[inline]
    pub fn clear_wal_tag(&self) {
        self.get().wal_tag.store(TAG_UNSET, Ordering::Release)
    }

    #[inline]
    /// Set the WAL tag from a (frame, epoch) pair.
    /// If inputs are invalid, stores TAG_UNSET, which will prevent
    /// the cached page from being used during checkpoint.
    pub fn set_wal_tag(&self, frame: u64, epoch: u32) {
        // use only first 20 bits for seq (max: 1048576)
        let e = epoch & EPOCH_MAX;
        self.get()
            .wal_tag
            .store(pack_tag_pair(frame, e), Ordering::Release);
    }
}

/// 메모리 내 개별 페이지의 변경 가능한 상태와 메타데이터를 관리
pub struct PageInner {
    // 페이지의 상태 플래그
    pub flags: AtomicUsize,
    pub contents: Option<PageContent>,
    // page id
    pub id: usize,
    // 페이지가 고정(pinned)된 횟수를 추적.
    // 고정된 페이지는 페이지 캐시에서 제거 대상에서 제외된다.
    pub pin_count: AtomicUsize,
    // 이 페이지가 로드된 WAL frame number. DB 파일에서 로드되었으면 0
    pub wal_tag: AtomicU64,
}

#[derive(Clone)]
enum AllocatePage1State {
    Start,
    Writing { page: PageRef },
    Done,
}

/// page 관리
/// WAL이나 DB에서 가져온 페이지를 PageCache로 관리
/// Page 1 : DB header + sqlite_schema table의 루트 페이지
pub struct Pager {
    pub db_file: Arc<dyn DatabaseStorage>,
    pub(crate) wal: Option<Rc<RefCell<dyn Wal>>>,
    pub io: Arc<dyn crate::io::IO>,
    pub(crate) page_size: AtomicU32,
    pub buffer_pool: Arc<BufferPool>,
    page_cache: Arc<RwLock<PageCache>>,
    dirty_pages: Arc<RwLock<BTreeSet<usize>>>,
    init_lock: Arc<Mutex<()>>,
    // DB가 초기화가 안된경우(Uninitialized) Start로 시작, 초기화중(Initializing)에 Writing으로 변경
    allocate_page1_state: RwLock<AllocatePage1State>,

    pub(crate) io_ctx: RwLock<IOContext>,
    // 값이 설정이 안되어 있는 경우: RESERVED_SPACE_NOT_SET 값으로 처리
    reserved_space: AtomicU16,
    // 스키마 버전 캐시
    schema_cookie: AtomicU64,
    header_ref_state: RwLock<HeaderRefState>,
    // In Memory Page 1 for Empty Dbs
    init_page_1: Arc<ArcSwapOption<Page>>,
}

unsafe impl Send for Pager {}
unsafe impl Sync for Pager {}

impl Pager {
    /// Schema cookie sentinel value that represents value not set.
    const SCHEMA_COOKIE_NOT_SET: u64 = u64::MAX;

    pub fn new(
        db_file: Arc<dyn DatabaseStorage>,
        wal: Option<Rc<RefCell<dyn Wal>>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: Arc<RwLock<PageCache>>,
        buffer_pool: Arc<BufferPool>,
        init_lock: Arc<Mutex<()>>,
        init_page_1: Arc<ArcSwapOption<Page>>,
    ) -> Result<Self> {
        let allocate_page1_state = if init_page_1.load().is_some() {
            RwLock::new(AllocatePage1State::Start)
        } else {
            RwLock::new(AllocatePage1State::Done)
        };

        Ok(Self {
            db_file,
            wal,
            io,
            page_size: AtomicU32::new(0),
            buffer_pool,
            page_cache,
            dirty_pages: Arc::new(RwLock::new(BTreeSet::new())),
            init_lock,
            allocate_page1_state,
            io_ctx: RwLock::new(IOContext::default()),
            reserved_space: AtomicU16::new(RESERVED_SPACE_NOT_SET),
            schema_cookie: AtomicU64::new(Self::SCHEMA_COOKIE_NOT_SET),
            header_ref_state: RwLock::new(HeaderRefState::Start),
            init_page_1,
        })
    }

    /// wal에 대해 read lock을 얻을 수 있으면 OK
    pub fn begin_read_tx(&self) -> Result<()> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };

        let changed = wal.borrow_mut().begin_read_tx()?;
        if changed {
            // 데이터베이스가 변경되었다.
            // page cache invalidate
            self.clear_page_cache(false);
        }

        Ok(())
    }

    /// 1. page 1이 있는지 확인
    /// 2. WAL에 대해 write lock을 얻을 수 있으면 OK
    pub fn begin_write_tx(&self) -> Result<IOResult<()>> {
        // page 1이 있는지 확인. 없으면 새로 할당
        return_if_io!(self.maybe_allocate_page1());

        let Some(wal) = self.wal.as_ref() else {
            return Ok(IOResult::Done(()));
        };
        Ok(IOResult::Done(wal.borrow_mut().begin_write_tx()?))
    }

    pub fn set_wal(&mut self, wal: Rc<RefCell<dyn Wal>>) {
        self.wal = Some(wal);
    }

    pub fn get_page_size(&self) -> Option<PageSize> {
        let value = self.page_size.load(Ordering::SeqCst);
        if value == 0 {
            None
        } else {
            PageSize::new(value)
        }
    }

    pub fn set_page_size(&self, size: PageSize) {
        self.page_size.store(size.get(), Ordering::SeqCst);
    }

    /// 페이지 캐시를 무효화
    /// begin_read_tx에서 호출되는 경우에는 dirty_pages가 비어있을 것이다.
    pub fn clear_page_cache(&self, clear_dirty: bool) {
        let dirty_pages = self.dirty_pages.read();
        let mut cache = self.page_cache.write();

        // dirty page가 캐시에 있으면 더티 비트를 clear
        // 실제 더티 페이지를 처리하는 건 다른 곳에서 이루어진다. 예: rollback
        for page_id in dirty_pages.iter() {
            let page_key = PageCacheKey::new(*page_id);
            if let Some(page) = cache.get(&page_key).unwrap_or(None) {
                page.clear_dirty();
            }
        }

        cache
            .clear(clear_dirty)
            .expect("Failed to clear page cache");
    }

    /// unchecked가 붙어 있는 이유: page_size에 제대로된 값이 들어가 있는지 확인하지 않는 함수라는 의미
    /// Pager에 저장되어 있는 page size에 올바르지 않은 값이 저장되어 있으면 panic
    pub fn get_page_size_unchecked(&self) -> PageSize {
        let value = self.page_size.load(Ordering::SeqCst);
        assert_ne!(value, 0, "page size not set");
        PageSize::new(value).expect("invalid page size stored")
    }

    pub fn set_reserved_space_bytes(&self, value: u8) {
        self.set_reserved_space(value);
    }

    pub fn get_reserved_space(&self) -> Option<u8> {
        let value = self.reserved_space.load(Ordering::SeqCst);
        if value == RESERVED_SPACE_NOT_SET {
            None
        } else {
            Some(value as u8)
        }
    }

    pub fn set_reserved_space(&self, space: u8) {
        self.reserved_space.store(space as u16, Ordering::SeqCst);
    }

    pub fn usable_space(&self) -> usize {
        let page_size = self.get_page_size().unwrap_or_else(|| {
            // 페이지 크기가 없으면 파일 헤더에서 얻어온다.
            let size = self
                .io
                .block(|| self.with_header(|header| header.page_size))
                .unwrap_or_default();
            self.page_size.store(size.get(), Ordering::SeqCst);
            size
        });

        let reserved_space = self.get_reserved_space().unwrap_or_else(|| {
            // reserved space가 없으면 파일 헤더에서 얻어온다.
            let space = self
                .io
                .block(|| self.with_header(|header| header.reserved_space))
                .unwrap_or_default();
            self.set_reserved_space(space);
            space
        });

        (page_size.get() as usize) - (reserved_space as usize)
    }

    pub fn reset_checksum_context(&self) {
        
    }

    /// 첫 번째 스레드가 초기화를 시작하고 Initializing 상태로 바꾼 뒤 I/O 대기에 들어가면,
    /// 다른 스레드가 그 사이에 잠금을 획득하고 Initializing 상태를 볼 수 있게 된다.
    /// 하지만, allocate_page1_state의 값 때문에 페이지 할당없이 그냥 리턴하게 된다.
    /// 
    /// DB의 db_state와 init_lock으로 한번에 하나의 쓰레드만 접근이 가능하도록 하고,
    /// allocate_page1_state를 통해 페이지 1 할당을 진행중인 스레드만 이 작업을 계속 하도록 한다.
    /// 
    /// (DbState::Uninitialized, false): 페이지 할당이 전혀 진행중이지 않은 경우
    /// (DbState::Initializing, true): 초기화를 시작한 쓰레드가 I/O 작업 후 중단된 지점부터 안전하게 작업을 진행할 수 있도록 하기 위한 장치
    pub fn maybe_allocate_page1(&self) -> Result<IOResult<()>> {
        if !self.db_initialized() {
            if let Some(_lock) = self.init_lock.try_lock() {
                if let IOResult::IO(c) = self.allocate_page1()? {
                    return Ok(IOResult::IO(c));
                } else {
                    return Ok(IOResult::Done(()));
                }
            } else {
                // Give a chance for the allocation to happen elsewhere
                io_yield_one!(Completion::new_yield());
            }
        }
        Ok(IOResult::Done(()))
    }

    fn allocating_page1(&self) -> bool {
        matches!(
            *self.allocate_page1_state.read(),
            AllocatePage1State::Writing { .. }
        )
    }

    pub fn allocate_page1(&self) -> Result<IOResult<PageRef>> {
        let state = self.allocate_page1_state.read().clone();
        match state {
            AllocatePage1State::Start => {
                assert!(!self.db_initialized());
                tracing::trace!("allocate_page1(Start)");

                let IOResult::Done(mut default_header) = self.with_header(|header| *header)? else {
                    panic!("DB should not be initialized and should not do any IO");
                };

                assert_eq!(default_header.database_size.get(), 0);
                // page 1을 생성할 예정이므로 1로 변경
                default_header.database_size = 1.into();

                // 체크섬을 사용여부 설정
                // reserved_space_bytes가 None이거나 크기가 CHECKSUM_REQUIRED_RESERVED_BYTES일때 체크섬 사용
                let reserved_space_bytes = {
                    let io_ctx = self.io_ctx.read();
                    io_ctx.get_reserved_space_bytes()
                };
                default_header.reserved_space = reserved_space_bytes;
                self.set_reserved_space(reserved_space_bytes);

                if let Some(size) = self.get_page_size() {
                    default_header.page_size = size;
                }

                tracing::debug!(
                    "allocate_page1(Start) page_size = {:?}, reserved_space = {}",
                    default_header.page_size,
                    default_header.reserved_space
                );

                self.buffer_pool
                    .finalize_with_page_size(default_header.page_size.get() as usize)?;
                let page = allocate_new_page(1, &self.buffer_pool, 0);

                // 페이지 1은 앞에 데이터베이스 헤더를 포함하고 있다.
                let contents = page.get_contents();
                contents.write_database_header(&default_header);

                let page1 = page;
                // 페이지 초기화
                btree_init_page(
                    &page1,
                    PageType::TableLeaf,
                    DatabaseHeader::SIZE,
                    (default_header.page_size.get() - default_header.reserved_space as u32)
                        as usize,
                );
                // 페이지 내용 파일에 쓰기
                let c = begin_write_btree_page(self, &page1)?;

                *self.allocate_page1_state.write() = AllocatePage1State::Writing { page: page1 };
                io_yield_one!(c);
            }
            AllocatePage1State::Writing { page } => {
                assert!(page.is_loaded(), "page should be loaded");
                tracing::trace!("allocate_page1(Writing done)");

                // 페이지 캐시에 넣기
                let page_key = PageCacheKey::new(page.get().id);
                let mut cache = self.page_cache.write();
                cache.insert(page_key, page.clone()).map_err(|e| {
                    GrainError::InternalError(format!("Failed to insert page 1 into cache: {e:?}"))
                })?;

                self.init_page_1.store(None);
                *self.allocate_page1_state.write() = AllocatePage1State::Done;
                Ok(IOResult::Done(page.clone()))
            }
            AllocatePage1State::Done => unreachable!("cannot try to allocate page 1 again"),
        }
    }

    pub fn db_initialized(&self) -> bool {
        self.init_page_1.load().is_none()
    }

    pub fn with_header<T>(&self, f: impl Fn(&DatabaseHeader) -> T) -> Result<IOResult<T>> {
        let header_ref = return_if_io!(HeaderRef::from_pager(self));
        let header = header_ref.borrow();
        // Update cached schema cookie when reading header
        self.set_schema_cookie(Some(header.schema_cookie.get()));
        Ok(IOResult::Done(f(header)))
    }

    /// Set the schema cookie cache.
    pub fn set_schema_cookie(&self, cookie: Option<u32>) {
        match cookie {
            Some(value) => {
                self.schema_cookie.store(value as u64, Ordering::SeqCst);
            }
            None => self
                .schema_cookie
                .store(Self::SCHEMA_COOKIE_NOT_SET, Ordering::SeqCst),
        }
    }

    pub fn read_page(&self, page_idx: i64) -> Result<(PageRef, Option<Completion>)> {
        assert!(page_idx >= 0, "pages in pager should be positive, negative might indicate unallocated pages from mvcc or any other nasty bug");
        tracing::debug!("read_page(page_idx = {})", page_idx);
        // 페이지 캐시 확인
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx as usize);
        if let Some(page) = page_cache.get(&page_key)? {
            // 페이지가 캐시되어 있으면 캐시된 페이지 리턴
            tracing::debug!("read_page(page_idx = {}) = cached", page_idx);
            assert!(
                page_idx as usize == page.get().id,
                "attempted to read page {page_idx} but got page {}",
                page.get().id
            );
            return Ok((page.clone(), None));
        }
        let (page, c) = self.read_page_no_cache(page_idx, None, false)?;
        assert!(
            page_idx as usize == page.get().id,
            "attempted to read page {page_idx} but got page {}",
            page.get().id
        );
        self.cache_insert(page_idx as usize, page.clone(), &mut page_cache)?;
        Ok((page, Some(c)))
    }

    /// WAL이나 DB file에서 페이지 읽기
    pub fn read_page_no_cache(&self, page_idx: i64, frame_watermark: Option<u64>, allow_empty_read: bool) -> Result<(PageRef, Completion)> {
        assert!(page_idx >= 0);
        tracing::debug!("read_page_no_cache(page_idx = {})", page_idx);

        let page = Arc::new(Page::new(page_idx));
        let io_ctx = self.io_ctx.read();
        let Some(wal) = self.wal.as_ref() else {
            assert!(
                matches!(frame_watermark, Some(0) | None),
                "frame_watermark must be either None or Some(0) because DB has no WAL and read with other watermark is invalid"
            );

            page.set_locked();
            let c = self.begin_read_disk_page(
                page_idx as usize,
                page.clone(),
                allow_empty_read,
                &io_ctx,
            )?;
            return Ok((page, c));
        };

        if let Some(frame_id) = wal.borrow().find_frame(page_idx as u64, frame_watermark)? {
            let c = wal
                .borrow()
                .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
            // TODO(pere) should probably first insert to page cache, and if successful,
            // read frame or page
            return Ok((page, c));
        }

        let c = self.begin_read_disk_page(page_idx as usize, page.clone(), allow_empty_read, &io_ctx)?;
        Ok((page, c))
    }

    fn begin_read_disk_page(
        &self,
        page_idx: usize,
        page: PageRef,
        allow_empty_read: bool,
        io_ctx: &IOContext,
    ) -> Result<Completion> {
        sqlite3_ondisk::begin_read_page(
            self.db_file.as_ref(),
            self.buffer_pool.clone(),
            page,
            page_idx,
            allow_empty_read,
            io_ctx,
        )
    }

    /// 페이지를 페이지 캐시에 추가
    /// 이미 같은 page id의 페이지가 있으면 에러
    fn cache_insert(
        &self,
        page_idx: usize,
        page: PageRef,
        page_cache: &mut PageCache,
    ) -> Result<()> {
        let page_key = PageCacheKey::new(page_idx);
        match page_cache.insert(page_key, page.clone()) {
            Ok(_) => {}
            Err(CacheError::KeyExists) => {
                unreachable!("Page should not exist in cache after get() miss")
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}

pub fn allocate_new_page(page_id: i64, buffer_pool: &Arc<BufferPool>, offset: usize) -> PageRef {
    let page = Arc::new(Page::new(page_id));
    {
        let buffer = buffer_pool.get_page();
        let buffer = Arc::new(buffer);

        page.set_loaded();
        page.clear_wal_tag();
        page.get().contents = Some(PageContent::new(offset, buffer));
    }
    page
}

pub fn default_page1() -> PageRef {
    // new Database header for empty Database
    let default_header = DatabaseHeader::default();
    let page = Arc::new(Page::new(DatabaseHeader::PAGE_ID as i64));

    let contents = PageContent::new(
        0,
        Arc::new(Buffer::new_temporary(
            default_header.page_size.get() as usize
        )),
    );

    contents.write_database_header(&default_header);
    page.set_loaded();
    page.clear_wal_tag();
    page.get().contents.replace(contents);

    btree_init_page(
        &page,
        PageType::TableLeaf,
        DatabaseHeader::SIZE, // offset of 100 bytes
        (default_header.page_size.get() - default_header.reserved_space as u32) as usize,
    );

    page
}
