
mod error;
mod fast_lock;
pub mod numeric;
pub mod mvcc;
pub mod io;
pub mod schema;
pub mod storage;
mod translate;
pub mod types;
mod util;

use arc_swap::{ArcSwap, ArcSwapOption};
use crate::{
    storage::{
        checksum::CHECKSUM_REQUIRED_RESERVED_BYTES,
        database::DatabaseFile,
        page_cache::PageCache,
        pager::{self, HeaderRef},
        sqlite3_ondisk::PageSize,
    },
    translate::emitter::TransactionMode,
};

pub use crate::{
    error::{CompletionError, GrainError},
    io::{File, MemoryIO, IO},
    storage::{
        buffer_pool::BufferPool,
        database::{DatabaseStorage, IOContext},
        pager::{Page, Pager},
        wal::{WalFile, WalFileShared},
    },
    util::IOExt,
};

use grain_macros::AtomicEnum;
use grain_parser::parser::Parser;
pub use io::{Buffer, Completion};
use parking_lot::{Mutex, RwLock};
use schema::Schema;
use std::{
    cell::RefCell,
    ops::Deref,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicU16, Ordering},
        Arc,
    },
};
use types::IOResult;

pub type Result<T, E = GrainError> = std::result::Result<T, E>;
pub(crate) type MvStore = mvcc::MvStore<mvcc::LocalClock>;

/// AtomicEnum은 2021에서 사용 가능. 2024에서는 에러
/// AtomicEnum이 무었인지는 DbState( in storage/pager.rs) 참고
#[derive(AtomicEnum, Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionState {
    Write { schema_did_change: bool },
    Read,
    PendingUpgrade,
    None,
}

/// 여러 connection 사이에 공유되는 Database
/// 따라서, Send와 Sync이어야 한다.
/// 
pub struct Database {
    path: String,
    wal_path: String,
    pub io: Arc<dyn IO>,
    // database file
    db_file: Arc<dyn DatabaseStorage>,
    // write-ahead log
    shared_wal: Arc<RwLock<WalFileShared>>,
    mv_store: ArcSwapOption<MvStore>,
    buffer_pool: Arc<BufferPool>,
    _shared_page_cache: Arc<RwLock<PageCache>>,
    schema: Mutex<Arc<Schema>>,
    init_lock: Arc<Mutex<()>>,
    // In Memory Page 1 for Empty Dbs
    // db를 초기화하기 전에 사용. 초기화 된 후에는 None으로 설정
    init_page_1: Arc<ArcSwapOption<Page>>,
}

/// Database 내부의 모든 필드가 Send, Sync가 아닐 수도 있지만
/// Database가 Send, Sync로 사용될 수 있다고 수동으로 컴파일러에게 알린다.
/// 따라서, 개발자가 수동으로 내부 구조를 쓰레드 안전하게 직접 관리해야 함
unsafe impl Send for Database {}
unsafe impl Sync for Database {}

/// IO: 데이터베이스 엔진과 실제 스토리지(파일 시스템, 메모리 등) 사이의 '어댑터' 역할
///   - MemoryIO의 open_file을 통해 실제 파일을 읽고 쓰는 MemoryFile을 얻는다.
/// DatabaseStorage: IO에 의해 생성된 File을 내부에 가지고 있다.
///   - File에 대해 페이지 단위 접근
impl Database {
    fn new(
        path: impl Into<String>,
        wal_path: impl Into<String>,
        io: &Arc<dyn IO>,
        db_file: Arc<dyn DatabaseStorage>,
    ) -> Result<Self> {
        let shared_wal = WalFileShared::new_noop();
        let mv_store = ArcSwapOption::empty();
        let db_size = db_file.size()?;
        let shared_page_cache = Arc::new(RwLock::new(PageCache::default()));
        let syms = SymbolTable::new();
        let arena_size = BufferPool::DEFAULT_ARENA_SIZE;
        let init_page_1 = if db_size == 0 {
            let default_page_1 = pager::default_page1();
            Some(default_page_1)
        } else {
            None
        };

        let db = Database {
            path: path.into(),
            wal_path: wal_path.into(),
            io: io.clone(),
            db_file,
            shared_wal,
            mv_store,
            buffer_pool: BufferPool::begin_init(&io, arena_size),
            _shared_page_cache: shared_page_cache.clone(),
            schema: Mutex::new(Arc::new(Schema::new())),
            init_lock: Arc::new(Mutex::new(())),
            init_page_1: Arc::new(ArcSwapOption::new(init_page_1)),
        };
        Ok(db)
    }

    pub fn connect(self: &Arc<Database>) -> Result<Arc<Connection>> {
        self._connect(false, None)
    }

    pub fn open_new(path: &str) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        let io = Self::io_for_path(path)?;
        let db = Self::open_file(io.clone(), path)?;
        Ok((io, db))
    }

    /// 파일 path로부터 IO 생성
    pub fn io_for_path(path: &str) -> Result<Arc<dyn IO>> {
        use crate::util::MEMORY_PATH;
        
        let io = match path.trim() {
            MEMORY_PATH => Arc::new(MemoryIO::new()),
            _ => {
                tracing::error!("not supported yet! use ':memory:' only at the moment");
                std::process::exit(0);
            }
        };
        Ok(io)
    }

    fn open_file(io: Arc<dyn IO>, path: &str) -> Result<Arc<Database>> {
        let file = io.open_file(path, true)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Self::open_with_internal(io, path, &format!("{path}-wal"), db_file)
    }

    fn open_with_internal(io: Arc<dyn IO>, path: &str, wal_path: &str, db_file: Arc<dyn DatabaseStorage>) -> Result<Arc<Database>> {
        let mut db = Self::new(path, wal_path, &io, db_file)?;

        // WAL 파일이 있으면 읽기
        let shared_wal = WalFileShared::open_shared_if_exists(&io, path)?;

        // MVCC 초기화
        let mv_store = {
            // MVCC 관련 로그 저장소: {path}-log
            let file = io.open_file(&format!("{path}-log"), false)?;
            let storage = mvcc::persistent_storage::Storage::new(file);
            let mv_store = MvStore::new(mvcc::LocalClock::new(), storage);
            Some(Arc::new(mv_store))
        };

        db.shared_wal = shared_wal;
        db.mv_store.store(mv_store);

        let db = Arc::new(db);

        let conn = db.connect()?;
        let syms = conn.syms.read();
        let pager = conn.pager.load().clone();

        // db.with_schema_mut(|schema| {
        //     let header_schema_cookie = pager
        //         .io
        //         .block(|| pager.with_header(|header| header.schema_cookie.get()))?;
        //     schema.schema_version = header_schema_cookie;
        //     let result = schema
        //         .make_from_btree(None, pager.clone(), &syms)
        //         .inspect_err(|_| pager.end_read_tx());
        //     match result {
        //         Err(GrainError::ExtensionError(e)) => {
        //             // this means that a vtab exists and we no longer have the module loaded. we print
        //             // a warning to the user to load the module
        //             eprintln!("Warning: {e}");
        //         }
        //         Err(e) => return Err(e),
        //         _ => {}
        //     }

        //     Ok(())
        // })?;

        {
            let mv_store = db.get_mv_store();
            let mv_store = mv_store.as_ref().unwrap();
            let mvcc_bootstrap_conn = db._connect(true, None)?;
            mv_store.bootstrap(mvcc_bootstrap_conn)?;
        }
        
        Ok(db)
    }

    // #[inline]
    // pub(crate) fn with_schema_mut<T>(&self, f: impl FnOnce(&mut Schema) -> Result<T>) -> Result<T> {
    //     let mut schema_ref = self.schema.lock();
    //     let schema = Arc::make_mut(&mut *schema_ref);
    //     f(schema)
    // }

    fn _connect(self: &Arc<Database>, is_mvcc_bootstrap_connection: bool, pager: Option<Arc<Pager>>) -> Result<Arc<Connection>> {
        let pager = if let Some(pager) = pager {
            pager
        } else {
            self._init()?
        };
        let page_size = pager.get_page_size_unchecked();
        let default_cache_size = pager
            .io
            .block(|| pager.with_header(|header| header.default_page_cache_size))
            .unwrap_or_default()
            .get();

        let conn = Arc::new(Connection {
            db: self.clone(),
            pager: ArcSwap::new(pager),
            schema: RwLock::new(self.schema.lock().clone()),
            cache_size: AtomicI32::new(default_cache_size),
            page_size: AtomicU16::new(page_size.get_raw()),
            syms: RwLock::new(SymbolTable::new()),
            mv_tx: RwLock::new(None),
            auto_commit: AtomicBool::new(true),
            transaction_state: AtomicTransactionState::new(TransactionState::None),
            closed: AtomicBool::new(false),
            is_mvcc_bootstrap_connection: AtomicBool::new(is_mvcc_bootstrap_connection),
        });
        Ok(conn)
    }

    fn _init(&self) -> Result<Arc<Pager>> {
        let pager = self.init_pager(None)?;
        let pager = Arc::new(pager);

        // TODO: auto vacuum
        // let header_ref = pager.io.block(|| HeaderRef::from_pager(&pager))?;
        // let header = header_ref.borrow();

        Ok(pager)
    }

    fn read_page_size_from_db_header(&self) -> Result<PageSize> {
        // TODO: 디비 헤더 읽기
        Ok(PageSize::default())
    }

    fn read_reserved_space_bytes_from_db_header(&self) -> Result<u8> {
        // TODO: 디비 헤더 읽기
        Ok(8)
    }

    pub fn get_mv_store(&self) -> impl Deref<Target = Option<Arc<MvStore>>> {
        self.mv_store.load()
    }

    /// 다음의 순서로 페이지 크기를 계산한다.
    ///   1. WAL 파일의 헤더
    ///   2. 데이터베이스 헤더
    ///   3. 기본(4096)
    fn determine_actual_page_size(&self, shared_wal: &WalFileShared, requested_page_size: Option<usize>) -> Result<PageSize> {
        if shared_wal.enabled.load(Ordering::SeqCst) {
            // WAL 파일이 있고 page size가 0이 아니면 WAL 파일에서 읽은 값을 사용
            let size_in_wal = shared_wal.page_size();
            if size_in_wal != 0 {
                let Some(page_size) = PageSize::new(size_in_wal) else {
                    bail_corrupt_error!("invalid page size in WAL: {size_in_wal}");
                };
                return Ok(page_size);
            }
        }
        if self.initialized() {
            Ok(self.read_page_size_from_db_header()?)
        } else {
            let Some(size) = requested_page_size else {
                return Ok(PageSize::default());
            };
            let Some(page_size) = PageSize::new(size as u32) else {
                bail_corrupt_error!("invalid requested page size: {size}");
            };
            Ok(page_size)
        }
    }

    /// 데이터베이스 파일이 초기화가 안된 시점에는 None을 리턴
    fn maybe_get_reserved_space_bytes(&self) -> Result<Option<u8>> {
        if self.initialized() {
            Ok(Some(self.read_reserved_space_bytes_from_db_header()?))
        } else {
            Ok(None)
        }
    }

    /// WalFileShared를 WalFile을 통해 참조한다.
    fn init_pager(&self, requested_page_size: Option<usize>) -> Result<Pager> {
        // 페이지의 reserved_space_bytes를 체크섬 확인용으로 사용
        // reserved_space_bytes가 None이거나 크기가 CHECKSUM_REQUIRED_RESERVED_BYTES일때 체크섬 사용
        let reserved_bytes = self.maybe_get_reserved_space_bytes()?;
        let disable_checksums = if let Some(reserved_bytes) = reserved_bytes {
            reserved_bytes != CHECKSUM_REQUIRED_RESERVED_BYTES
        } else {
            false
        };

        let shared_wal = self.shared_wal.read();
        if shared_wal.enabled.load(Ordering::SeqCst) {
            // WAL 파일이 있는 경우

            let page_size = self.determine_actual_page_size(&shared_wal, requested_page_size)?;
            drop(shared_wal);

            let buffer_pool = self.buffer_pool.clone();
            if self.initialized() {
                buffer_pool.finalize_with_page_size(page_size.get() as usize)?;
            }
            let wal = Rc::new(RefCell::new(WalFile::new(
                self.io.clone(),
                self.shared_wal.clone(),
                buffer_pool.clone(),
            )));
            let pager = Pager::new(
                self.db_file.clone(),
                Some(wal),
                self.io.clone(),
                Arc::new(RwLock::new(PageCache::default())),
                buffer_pool.clone(),
                self.init_lock.clone(),
                self.init_page_1.clone(),
            )?;
            pager.set_page_size(page_size);
            if let Some(reserved_bytes) = reserved_bytes {
                pager.set_reserved_space_bytes(reserved_bytes);
            }
            if disable_checksums {
                pager.reset_checksum_context();
            }
            return Ok(pager)
        }

        // WAL 파일을 새로 생성하는 경우

        let page_size = self.determine_actual_page_size(&shared_wal, requested_page_size)?;
        drop(shared_wal);

        let buffer_pool = self.buffer_pool.clone();

        if self.initialized() {
            buffer_pool.finalize_with_page_size(page_size.get() as usize)?;
        }

        let mut pager = Pager::new(
            self.db_file.clone(),
            None,
            self.io.clone(),
            Arc::new(RwLock::new(PageCache::default())),
            buffer_pool.clone(),
            self.init_lock.clone(),
            self.init_page_1.clone(),
        )?;

        pager.set_page_size(page_size);
        if let Some(reserved_bytes) = reserved_bytes {
            pager.set_reserved_space_bytes(reserved_bytes);
        }
        if disable_checksums {
            pager.reset_checksum_context();
        }
        let file = self.io.open_file(&self.wal_path, false)?;
        {
            let mut shared_wal = self.shared_wal.write();
            shared_wal.create(file)?;
        }

        let wal = Rc::new(RefCell::new(WalFile::new(
            self.io.clone(),
            self.shared_wal.clone(),
            buffer_pool.clone(),
        )));
        pager.set_wal(wal);

        Ok(pager)
    }

    #[inline]
    pub(crate) fn initialized(&self) -> bool {
        self.init_page_1.load().is_none()
    }
}

/// Connection에서는 한번에 하나의 트랜잭션만 가능하게 동작한다.
pub struct Connection {
    db: Arc<Database>,
    pager: ArcSwap<Pager>,
    schema: RwLock<Arc<Schema>>,
    cache_size: AtomicI32,
    page_size: AtomicU16,
    syms: RwLock<SymbolTable>,
    pub(crate) mv_tx: RwLock<Option<(crate::mvcc::database::TxID, TransactionMode)>>,

    // 커밋을 자동으로 할지 말지 여부
    auto_commit: AtomicBool,
    transaction_state: AtomicTransactionState,
    // 연결 유지 여부. 연결을 끊으면 true로 설정
    closed: AtomicBool,
    // mvcc bootstrap을 위한 연결 여부
    is_mvcc_bootstrap_connection: AtomicBool,
}

impl Connection {
    pub fn prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        self._prepare(sql)
    }

    pub fn _prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        if self.is_closed() {
            return Err(GrainError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(GrainError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }

        let sql = sql.as_ref();
        tracing::debug!("Preparing: {}", sql);

        Ok(Statement::new())
    }

    pub fn promote_to_regular_connection(&self) {
        assert!(self.is_mvcc_bootstrap_connection.load(Ordering::SeqCst));
        self.is_mvcc_bootstrap_connection.store(false, Ordering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn query_runner<'a>(self: &'a Arc<Connection>, sql: &'a [u8]) -> QueryRunner<'a> {
        QueryRunner::new(self, sql)
    }

    pub fn is_mvcc_bootstrap_connection(&self) -> bool {
        self.is_mvcc_bootstrap_connection.load(Ordering::SeqCst)
    }

    pub(crate) fn get_mv_tx_id(&self) -> Option<u64> {
        self.mv_tx.read().map(|(tx_id, _)| tx_id)
    }

    pub(crate) fn get_mv_tx(&self) -> Option<(u64, TransactionMode)> {
        *self.mv_tx.read()
    }

    pub(crate) fn set_mv_tx(&self, tx_id_and_mode: Option<(u64, TransactionMode)>) {
        *self.mv_tx.write() = tx_id_and_mode;
    }

    fn set_tx_state(&self, state: TransactionState) {
        self.transaction_state.set(state);
    }

    fn get_tx_state(&self) -> TransactionState {
        self.transaction_state.get()
    }
}

#[derive(Default)]
pub struct SymbolTable {

}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            
        }
    }
}

pub struct QueryRunner<'a> {
    parser: Parser<'a>,
    conn: &'a Arc<Connection>,
    statements: &'a [u8],
    last_offset: usize,
}

impl<'a> QueryRunner<'a> {
    pub(crate) fn new(conn: &'a Arc<Connection>, statements: &'a [u8]) -> Self {
        Self {
            parser: Parser::new(statements),
            conn,
            statements,
            last_offset: 0,
        }
    }
}

impl Iterator for QueryRunner<'_> {
    type Item = Result<Option<Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

pub struct Statement {

}

impl Statement {
    pub fn new() -> Self {
        Self {

        }
    }
}
