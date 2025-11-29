
use crate::{
    io_yield_one, return_if_io,
    storage::{
        buffer_pool::BufferPool,
        database::DatabaseStorage,
        sqlite3_ondisk::{
            begin_write_btree_page,
            DatabaseHeader, PageContent, PageSize, PageType,
        },
        wal::Wal,
    },
    types::IOCompletions,
    Completion, IOContext, IOResult, Result,
};
use grain_macros::AtomicEnum;
use parking_lot::{Mutex, RwLock};
use std::{
    cell::{RefCell, UnsafeCell},
    rc::Rc,
    sync::{
        atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use super::btree::btree_init_page;

const RESERVED_SPACE_NOT_SET: u16 = u16::MAX;

/// 유효한 WAL tag가 설정되지 않음
pub const TAG_UNSET: u64 = u64::MAX;

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

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::Release);
    }

    #[inline]
    pub fn clear_wal_tag(&self) {
        self.get().wal_tag.store(TAG_UNSET, Ordering::Release)
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

/// AtomicEnum 매크로를 통해 AtomicDbState를 생성한다.
/// 
/// 자동으로 생성된 `AtomicDbState`는 내부적으로 `AtomicUsize`나 `AtomicU8` 같은 원자적 정수(atomic integer)를 가집니다.
/// 
/// 역할: 데이터베이스의 초기화 상태(`DbState`)를 여러 스레드에서 **경쟁 없이(lock-free) 안전하게 공유**하기 위해 사용됩니다. `Mutex` 같은 락(lock)을 사용하면 동시성 높은 환경에서 성능 저하가 발생할 수 있지만, 원자적(atomic) 연산을 사용하면 훨씬 효율적입니다.
/// 
/// 동작 방식:
///   1.  `DbState`의 각 상태(`Uninitialized`, `Initializing`, `Initialized`)를 내부적으로 정수(예: 0, 1, 2)에 매핑합니다.
///   2.  `get()` 같은 읽기 메서드는 내부 `AtomicUsize`에 대해 원자적 `load` 연산을 수행하여 현재 상태 값을 가져옵니다.
///   3.  `set()` 같은 쓰기 메서드는 원자적 `store` 연산을 통해 상태 값을 변경합니다.
#[derive(AtomicEnum, Clone, Copy, Debug)]
pub enum DbState {
    Uninitialized,
    Initializing,
    Initialized,
}

impl DbState {
    pub fn is_initialized(&self) -> bool {
        matches!(self, DbState::Initialized)
    }
}

#[derive(Clone)]
enum AllocatePage1State {
    Start,
    Writing { page: PageRef },
    Done,
}

pub struct Pager {
    pub db_file: Arc<dyn DatabaseStorage>,
    pub(crate) wal: Option<Rc<RefCell<dyn Wal>>>,
    pub io: Arc<dyn crate::io::IO>,
    pub db_state: Arc<AtomicDbState>,
    pub(crate) page_size: AtomicU32,
    pub buffer_pool: Arc<BufferPool>,
    init_lock: Arc<Mutex<()>>,
    // DB가 초기화가 안된경우(Uninitialized) Start로 시작, 초기화중(Initializing)에 Writing으로 변경
    allocate_page1_state: RwLock<AllocatePage1State>,

    pub(crate) io_ctx: RwLock<IOContext>,
    // 값이 설정이 안되어 있는 경우: RESERVED_SPACE_NOT_SET 값으로 처리
    reserved_space: AtomicU16,
}

unsafe impl Send for Pager {}
unsafe impl Sync for Pager {}

impl Pager {
    pub fn new(
        db_file: Arc<dyn DatabaseStorage>,
        wal: Option<Rc<RefCell<dyn Wal>>>,
        io: Arc<dyn crate::io::IO>,
        db_state: Arc<AtomicDbState>,
        buffer_pool: Arc<BufferPool>,
        init_lock: Arc<Mutex<()>>,
    ) -> Result<Self> {
        let allocate_page1_state = if !db_state.get().is_initialized() {
            RwLock::new(AllocatePage1State::Start)
        } else {
            RwLock::new(AllocatePage1State::Done)
        };

        Ok(Self {
            db_file,
            wal,
            io,
            db_state,
            page_size: AtomicU32::new(0),
            buffer_pool,
            init_lock,
            allocate_page1_state,
            io_ctx: RwLock::new(IOContext::default()),
            reserved_space: AtomicU16::new(RESERVED_SPACE_NOT_SET),
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
            // TODO: page cache invalidate
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

    pub fn set_reserved_space(&self, space: u8) {
        self.reserved_space.store(space as u16, Ordering::SeqCst);
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
        if !self.db_state.get().is_initialized() {
            // 여러 쓰레드가 페이지 1 할당을 하려는 것을 방지
            if let Some(_lock) = self.init_lock.try_lock() {
                match (self.db_state.get(), self.allocating_page1()) {
                    (DbState::Uninitialized, false) | (DbState::Initializing, true) => {
                        if let IOResult::IO(c) = self.allocate_page1()? {
                            return Ok(IOResult::IO(c))
                        } else {
                            return Ok(IOResult::Done(()));
                        }
                    }
                    _ => {}
                }
            } else {
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
                tracing::trace!("allocate_page1(Start)");

                self.db_state.set(DbState::Initializing);
                let mut default_header = DatabaseHeader::default();

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

                // TODO: 페이지 캐시에 넣기

                self.db_state.set(DbState::Initialized);
                *self.allocate_page1_state.write() = AllocatePage1State::Done;
                Ok(IOResult::Done(page.clone()))
            }
            AllocatePage1State::Done => unreachable!("cannot try to allocate page 1 again"),
        }
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
