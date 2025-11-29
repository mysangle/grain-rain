
use crate::{fast_lock::SpinLock, io::TEMP_BUFFER_CACHE, Buffer, GrainError, IO, Result};
use parking_lot::Mutex;
use std::{
    cell::UnsafeCell,
    ptr::NonNull,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc, Weak,
    },
};
use super::{
    slot_bitmap::SlotBitmap,
    sqlite3_ondisk::WAL_FRAME_HEADER_SIZE,
};

#[derive(Debug)]
pub struct ArenaBuffer {
    // 어떤 아레나에서 왔는가
    arena: Weak<Arena>,
    // 버퍼 시작 포인터
    ptr: NonNull<u8>,
    // 아레나의 id
    arena_id: u32,
    // 아레나 내부 슬롯의 인덱스. 연속 슬롯이라면 첫번째 슬롯
    slot_idx: u32,
    // 실제 할당 요청 크기(내부에서 실제로 할당한 크기는 더 클 수 있다.) 
    len: usize,
}

unsafe impl Sync for ArenaBuffer {}
unsafe impl Send for ArenaBuffer {}

impl ArenaBuffer {
    const fn new(
        arena: Weak<Arena>,
        ptr: NonNull<u8>,
        len: usize,
        arena_id: u32,
        slot_idx: u32,
    ) -> Self {
        ArenaBuffer {
            arena,
            ptr,
            arena_id,
            slot_idx,
            len,
        }
    }

    pub const fn logical_len(&self) -> usize {
        self.len
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.logical_len()) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.logical_len()) }
    }
}

impl Drop for ArenaBuffer {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.upgrade() {
            arena.free(self.slot_idx, self.logical_len());
        }
    }
}

impl std::ops::Deref for ArenaBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::ops::DerefMut for ArenaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// 데이터베이스 운영에 필요한 메모리 버퍼(Buffer)들을 효율적으로 할당하고 재사용
///   - 페이지를 요청하면 Arena로부터 페이지 크기의 Buffer를 리턴한다.
/// 
/// 1. 초기 단계 (Arena 없음):
///   BufferPool이 begin_init을 통해 처음 생성될 때, 아직 Arena는 만들어지지 않습니다. 이
///   시점에서 데이터베이스의 실제 page_size를 아직 모르기 때문입니다.
///   이 단계에서 버퍼가 필요하면, BufferPool은 Arena에서 버퍼를 가져오는 대신
///   Buffer::new_temporary(len)를 호출하여 임시 버퍼를 사용합니다. 이 임시 버퍼들은 스레드별
///   로컬 캐시인 TEMP_BUFFER_CACHE에 저장되고 재사용됩니다.
///   이 때 TEMP_BUFFER_CACHE는 BufferPool::DEFAULT_PAGE_SIZE (기본값, 4096) 크기의
///   버퍼들을 캐싱하도록 설정되어 있습니다.
///
/// 2. `finalize_with_page_size` 호출:
///   데이터베이스 파일 헤더를 읽는 등 실제 page_size가 확정되는 시점에 이 함수가
///   호출됩니다. 이제 BufferPool은 확정된 page_size에 맞는 Arena들을 생성할 준비를 합니다.
///   Arena가 꽉 차서 더이상 할당을 할 수 없는 경우에는 TEMP_BUFFER_CACHE를 사용합니다.
pub struct BufferPool {
    inner: UnsafeCell<PoolInner>,
}

unsafe impl Sync for BufferPool {}
unsafe impl Send for BufferPool {}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(Self::DEFAULT_ARENA_SIZE)
    }
}

impl BufferPool {
    /// 3MB Default size for each `Arena`.
    pub const DEFAULT_ARENA_SIZE: usize = 3 * 1024 * 1024;
    /// 4KB default page_size
    pub const DEFAULT_PAGE_SIZE: usize = 4096;
    /// Maximum size for each Arena (64MB total)
    const MAX_ARENA_SIZE: usize = 32 * 1024 * 1024;
    /// 64kb Minimum arena size
    const MIN_ARENA_SIZE: usize = 1024 * 64;

    fn new(arena_size: usize) -> Self {
        assert!(
            (Self::MIN_ARENA_SIZE..Self::MAX_ARENA_SIZE).contains(&arena_size),
            "Arena size needs to be between {}..{} bytes",
            Self::MIN_ARENA_SIZE,
            Self::MAX_ARENA_SIZE
        );
        Self {
            inner: UnsafeCell::new(PoolInner {
                arena_size: arena_size.into(),
                db_page_size: Self::DEFAULT_PAGE_SIZE.into(),
                io: None,
                page_arena: None,
                wal_frame_arena: None,
                init_lock: Mutex::new(()),
            })
        }
    }

    /// 이 함수 호출 시점에는 페이지 크기를 모르기 때문에
    /// DB가 초기화 될 때까지는 페이지 요청시 TEMP_BUFFER_CACHE가 사용된다
    pub fn begin_init(io: &Arc<dyn IO>, arena_size: usize) -> Arc<Self> {
        let pool = Arc::new(BufferPool::new(arena_size));
        let inner = pool.inner_mut();
        if inner.io.is_none() {
            inner.io = Some(Arc::clone(io));
        }
        pool
    }

    #[inline]
    pub fn get_page(&self) -> Buffer {
        let inner = self.inner_mut();
        inner.get_db_page_buffer()
    }

    /// DB가 초기화 되면 호출되는 함수.
    /// 지정한 페이지 크기에 따라 아레나를 생성한다.
    pub fn finalize_with_page_size(&self, page_size: usize) -> Result<()> {
        let inner = self.inner_mut();
        tracing::trace!("finalize page size called with size {page_size}");
        if page_size != BufferPool::DEFAULT_PAGE_SIZE {
            // 임시 버퍼 캐시를 clear 한다.
            TEMP_BUFFER_CACHE.with(|cache| {
                cache.borrow_mut().reinit_cache(page_size);
            });
        }
        if inner.page_arena.is_some() {
            return Ok(());
        }
        inner.db_page_size.store(page_size, Ordering::SeqCst);
        inner.init_arenas();
        Ok(())
    }

    #[inline]
    fn inner_mut(&self) -> &mut PoolInner {
        unsafe { &mut *self.inner.get() }
    }
}

struct PoolInner {
    arena_size: AtomicUsize,
    db_page_size: AtomicUsize,
    io: Option<Arc<dyn IO>>,
    page_arena: Option<Arc<Arena>>,
    wal_frame_arena: Option<Arc<Arena>>,
    init_lock: Mutex<()>,
}

unsafe impl Sync for PoolInner {}
unsafe impl Send for PoolInner {}

impl PoolInner {
    fn get_db_page_buffer(&mut self) -> Buffer {
        let db_page_size = self.db_page_size.load(Ordering::SeqCst);
        self.page_arena
            .as_ref()
            .and_then(|arena| Arena::try_alloc(arena, db_page_size))
            .unwrap_or_else(|| Buffer::new_temporary(db_page_size))
    }

    fn get_wal_frame_buffer(&mut self) -> Buffer {
        let wal_frame_size = self.db_page_size.load(Ordering::SeqCst) + WAL_FRAME_HEADER_SIZE;
        self.wal_frame_arena
            .as_ref()
            .and_then(|wal_arena| Arena::try_alloc(wal_arena, wal_frame_size))
            .unwrap_or_else(|| Buffer::new_temporary(wal_frame_size))
    }

    /// 페이지용 아레나와 WAL용 아레나 두개를 생성
    fn init_arenas(&mut self) -> Result<()> {
        let Some(_guard) = self.init_lock.try_lock() else {
            tracing::debug!("Buffer pool is already growing, skipping initialization");
            return Ok(());
        };
        let arena_size = self.arena_size.load(Ordering::SeqCst);
        let db_page_size = self.db_page_size.load(Ordering::SeqCst);
        let io = self.io.as_ref().expect("Pool not initialized").clone();

        // 페이지를 위한 아레나 할당
        match Arena::new(db_page_size, arena_size, &io) {
            Ok(arena) => {
                tracing::trace!(
                    "added arena {} with size {} MB and slot size {}",
                    arena.id,
                    arena_size / (1024 * 1024),
                    db_page_size
                );
                self.page_arena = Some(Arc::new(arena));
            }
            Err(e) => {
                tracing::error!("Failed to create arena: {:?}", e);
                return Err(GrainError::InternalError(format!(
                    "Failed to create arena: {e}",
                )));
            }
        }

        // WAL을 위한 아레나 할당
        let wal_frame_size = db_page_size + WAL_FRAME_HEADER_SIZE;
        match Arena::new(wal_frame_size, arena_size, &io) {
            Ok(arena) => {
                tracing::trace!(
                    "added WAL frame arena {} with size {} MB and slot size {}",
                    arena.id,
                    arena_size / (1024 * 1024),
                    wal_frame_size
                );
                self.wal_frame_arena = Some(Arc::new(arena));
            }
            Err(e) => {
                tracing::error!("Failed to create WAL frame arena: {:?}", e);
                return Err(GrainError::InternalError(format!(
                    "Failed to create WAL frame arena: {e}",
                )));
            }
        }
        
        Ok(())
    }
}

/// `io_uring`과 함께 할당된 아레나는 id가 0..=1이고,
/// `io_uring`과 함께 할당되지 않은 아레나는 id가 2에서 시작한다.
/// (페이지와 WAL frame용 2개를 할당할 것이므로 2와 3이 할당된다.)
const UNREGISTERED_START: u32 = 2;
static NEXT_ID: AtomicU32 = AtomicU32::new(UNREGISTERED_START);

/// 미리 큰 메모리 블록을 한번에 할당받은 뒤 작은 버퍼를 이 큰 블록 안에서 빠르게 할당
/// 슬롯이 할당되었는지 아닌지를 free_slots를 통해 판단한다.
struct Arena {
    id: u32,
    // 아레나의 시작 주소
    base: NonNull<u8>,
    // 사용중인 슬롯의 수
    allocated_slots: AtomicUsize,
    // free slot을 추적하는 값
    free_slots: SpinLock<SlotBitmap>,
    // 아레나의 크기
    arena_size: usize,
    // 아레나를 나누는 슬롯의 크기(페이지 크기, WAL frame의 크기)
    slot_size: usize,
}

impl Arena {
    /// 최소 크기는 slot_size * 64 이다.
    /// 하지만, MAX_ARENA_SIZE를 넘을 수는 없다.
    fn new(slot_size: usize, arena_size: usize, io: &Arc<dyn IO>) -> Result<Self, String> {
        let min_slots = arena_size.div_ceil(slot_size);
        // 64의 배수로 올림
        let rounded_slots = (min_slots.max(64) + 63) & !63;
        let rounded_bytes = rounded_slots * slot_size;
        if rounded_bytes > BufferPool::MAX_ARENA_SIZE {
            return Err(format!(
                "arena size {} B exceeds hard limit of {} B",
                rounded_bytes,
                BufferPool::MAX_ARENA_SIZE
            ));
        }
        let ptr = unsafe { arena::alloc(rounded_bytes) };
        let base = NonNull::new(ptr).ok_or("failed to allocate arena")?;
        let id = io
            .register_fixed_buffer(base, rounded_bytes)
            .unwrap_or_else(|_| {
                let next_id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
                tracing::trace!("Allocating arena with id {}", next_id);
                next_id
            });
        let map = SlotBitmap::new(rounded_slots as u32);
        Ok(Self {
            id,
            base,
            allocated_slots: AtomicUsize::new(0),
            free_slots: SpinLock::new(map),
            arena_size: rounded_bytes,
            slot_size,
        })
    }

    /// size 할당을 위해 몇개의 slot이 필요한지 찾은 후
    /// 이 개수의 할당이 가능한 slot의 위치를 찾는다.
    pub fn try_alloc(arena: &Arc<Arena>, size: usize) -> Option<Buffer> {
        // size 할당을 위해 몇개의 slot이 필요한지 찾는다.
        let slots = size.div_ceil(arena.slot_size) as u32;
        let mut freemap = arena.free_slots.lock();

        // 할당이 가능한 슬롯의 첫번째 index를 찾는다.
        let first_idx = if slots == 1 {
            // 가능한 큰 영역(continuous slots)은 남겨두는 최적화된 방법을 사용한다.
            freemap.alloc_one()?
        } else {
            freemap.alloc_run(slots)?
        };
        arena
            .allocated_slots
            .fetch_add(slots as usize, Ordering::SeqCst);
        // arena에서 slot의 위치를 계산
        let offset = first_idx as usize * arena.slot_size;
        let ptr = unsafe { NonNull::new_unchecked(arena.base.as_ptr().add(offset)) };
        Some(Buffer::new_pooled(ArenaBuffer::new(
            Arc::downgrade(arena),
            ptr,
            size,
            arena.id,
            first_idx,
        )))
    }

    pub fn free(&self, slot_idx: u32, size: usize) {
        let mut bm = self.free_slots.lock();
        let count = size.div_ceil(self.slot_size);

        // free 하려는 모든 슬롯이 이미 free 상태인지 확인
        assert!(
            !bm.check_run_free(slot_idx, count as u32),
            "must not already be marked free"
        );
        bm.free_run(slot_idx, count as u32);
        self.allocated_slots.fetch_sub(count, Ordering::SeqCst);
    }
}

mod arena {
    use libc::MAP_ANONYMOUS;
    use libc::{mmap, munmap, MAP_PRIVATE, PROT_READ, PROT_WRITE};
    use std::ffi::c_void;

    pub unsafe fn alloc(len: usize) -> *mut u8 {
        let ptr = mmap(
            std::ptr::null_mut(),
            len,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS,
            -1,
            0,
        );
        if ptr == libc::MAP_FAILED {
            panic!("mmap failed: {}", std::io::Error::last_os_error());
        }
        //#[cfg(target_os = "linux")]
        //{
        //    libc::madvise(ptr, len, libc::MADV_HUGEPAGE);
        //}
        ptr as *mut u8
    }

    pub unsafe fn dealloc(ptr: *mut u8, len: usize) {
        let result = munmap(ptr as *mut c_void, len);
        if result != 0 {
            panic!("munmap failed: {}", std::io::Error::last_os_error());
        }
    }
}
