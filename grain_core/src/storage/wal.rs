
use crate::{
    fast_lock::SpinLock,
    io::{File, IO},
    storage::{buffer_pool::BufferPool, sqlite3_ondisk::WalHeader},
    GrainError, Result,
};
use parking_lot::RwLock;
use std::{
    array,
    fmt::{self, Debug},
    sync::{
        atomic::{AtomicBool, AtomicUsize, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};
use super::sqlite3_ondisk::{WAL_MAGIC_BE, WAL_MAGIC_LE};

pub const READMARK_NOT_USED: u32 = 0xffffffff;
const NO_LOCK_HELD: usize = usize::MAX;

pub struct GrainRwLock(AtomicU64);

/// 64 비트 read-write lock
/// 비트 0    : 쓰기 락이 획득되었는지 여부를 나타내는 비트
/// 비트 1-31 : 락을 획득한 읽기 쓰레드의 수
/// 비트 32-63: 락 상태와 함께 저장될 수 있는 추가적인 32비트 값
impl GrainRwLock {
    /// Bit 0: Writer flag
    const WRITER: u64 = 0b1;

    /// Reader increment value (bit 1)
    /// 비트 1부터 시작하는 값이므로 2를 증가시킨다.
    const READER_INC: u64 = 0b10;

    /// Reader count starts at bit 1
    const READER_SHIFT: u32 = 1;

    /// Mask for 31 reader bits [31:1]
    const READER_COUNT_MASK: u64 = 0x7fff_ffffu64 << Self::READER_SHIFT;

    /// Value starts at bit 32
    const VALUE_SHIFT: u32 = 32;

    /// Mask for 32 value bits [63:32]
    const VALUE_MASK: u64 = 0xffff_ffffu64 << Self::VALUE_SHIFT;

    #[inline]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    const fn has_writer(val: u64) -> bool {
        val & Self::WRITER != 0
    }

    const fn has_readers(val: u64) -> bool {
        val & Self::READER_COUNT_MASK != 0
    }

    #[inline]
    /// read lock 얻기
    pub fn read(&self) -> bool {
        let cur = self.0.load(Ordering::Acquire);
        if Self::has_writer(cur) {
            // 이미 write lock을 획득한 쓰레드가 있다.
            return false;
        }
        // read count를 증가시킨다.
        let desired = cur.wrapping_add(Self::READER_INC);
        self.0
            .compare_exchange(cur, desired, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[inline]
    /// write lock 얻기
    pub fn write(&self) -> bool {
        let cur = self.0.load(Ordering::Acquire);
        if Self::has_writer(cur) || Self::has_readers(cur) {
            // 다른 쓰레드에서 이미 락을 획득한 상태
            return false;
        }
        let desired = cur | Self::WRITER;
        self.0
            .compare_exchange(cur, desired, Ordering::Acquire, Ordering::SeqCst)
            .is_ok()
    }

    #[inline]
    /// write lock을 갖고 있으면 해제
    /// read lock을 갖고 있으면 read count 값을 줄인다.
    pub fn unlock(&self) {
        let cur = self.0.load(Ordering::Acquire);
        if (cur & Self::WRITER) != 0 {
            // write lock이 획득되어 있으므로 write 비트 제거
            let cur = self.0.fetch_and(!Self::WRITER, Ordering::Release);
            // read lock을 가지고 있는 reader가 있는지 확인
            assert!(!Self::has_readers(cur), "write lock was held with readers");
        } else {
            // read lock을 가지고 있는 reader가 있다.
            assert!(
                Self::has_readers(cur),
                "unlock called with no readers or writers"
            );
            self.0.fetch_sub(Self::READER_INC, Ordering::Release);
        }
    }

    #[inline]
    /// 추가로 설정한 값 얻기
    pub fn get_value(&self) -> u32 {
        (self.0.load(Ordering::Acquire) >> Self::VALUE_SHIFT) as u32
    }

    #[inline]
    /// write lock을 가지고 있는 경우에 value 설정
    pub fn set_value_exclusive(&self, v: u32) {
        // writer lock을 가지고 있는 경우에만 호출되어야 한다.
        let cur = self.0.load(Ordering::SeqCst);
        assert!(Self::has_writer(cur), "must hold exclusive lock");
        let desired = (cur & !Self::VALUE_MASK) | ((v as u64) << Self::VALUE_SHIFT);
        self.0.store(desired, Ordering::SeqCst);
    }
}

pub trait Wal: Debug {
    fn begin_read_tx(&mut self) -> Result<bool>;
    fn begin_write_tx(&mut self) -> Result<()>;
    fn end_read_tx(&self);
    fn end_write_tx(&self);
}

/// 개별 데이터베이스 연결(connection)이 WAL 시스템과 상호작용하기 위한 기본 핸들
pub struct WalFile {
    io: Arc<dyn IO>,
    shared: Arc<RwLock<WalFileShared>>,
    buffer_pool: Arc<BufferPool>,
    // 현재 connection에서 사용중인 read lock index
    max_frame_read_lock_index: AtomicUsize,

    max_frame: AtomicU64,
    min_frame: AtomicU64,
    last_checksum: (u32, u32),
    checkpoint_seq: AtomicU32,
    transaction_count: AtomicU64,

    // WalFile 생성 시점의 WAL header 스냅샷.
    // 디버깅 용도 또는 변하지 않는 값 빠르게 액세스 하기
    pub header: WalHeader,
}

impl WalFile {
    pub fn new(
        io: Arc<dyn IO>,
        shared: Arc<RwLock<WalFileShared>>,
        buffer_pool: Arc<BufferPool>
    ) -> Self {
        let (header, last_checksum, max_frame) = {
            let shared_guard = shared.read();
            // WalFile 생성 시점의 WAL 헤더의 내용을 가져와서 내부에 저장한다.
            let header = *shared_guard.wal_header.lock();
            (
                header,
                shared_guard.last_checksum,
                shared_guard.max_frame.load(Ordering::Acquire),
            )
        };
        Self {
            io,
            shared,
            buffer_pool,
            max_frame_read_lock_index: AtomicUsize::new(NO_LOCK_HELD),
            max_frame: AtomicU64::new(max_frame),
            min_frame: AtomicU64::new(0),
            last_checksum,
            checkpoint_seq: AtomicU32::new(0),
            transaction_count: AtomicU64::new(0),
            header,
        }
    }

    #[inline]
    fn with_shared_mut<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&mut WalFileShared) -> R,
    {
        let mut shared = self._get_shared_mut();
        func(&mut shared)
    }

    #[inline]
    fn with_shared<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&WalFileShared) -> R,
    {
        let shared = self._get_shared();
        func(&shared)
    }

    fn _get_shared_mut(&self) -> parking_lot::RwLockWriteGuard<'_, WalFileShared> {
        self.shared.write()
    }

    fn _get_shared(&self) -> parking_lot::RwLockReadGuard<'_, WalFileShared> {
        self.shared.read()
    }

    /// 다음의 5개의 값이 WalFile 생성 이후 변경되었는지 확인한다.
    /// shared_max, nbackfills, last_checksum, checkpoint_seq, transaction_count
    fn db_changed(&self, shared: &WalFileShared) -> bool {
        let shared_max = shared.max_frame.load(Ordering::Acquire);
        let nbackfills = shared.nbackfills.load(Ordering::Acquire);
        let last_checksum = shared.last_checksum;
        let checkpoint_seq = shared.wal_header.lock().checkpoint_seq;
        let transaction_count = shared.transaction_count.load(Ordering::Acquire);

        shared_max != self.max_frame.load(Ordering::Acquire)
            || last_checksum != self.last_checksum
            || checkpoint_seq != self.checkpoint_seq.load(Ordering::Acquire)
            || transaction_count != self.transaction_count.load(Ordering::Acquire)
            || nbackfills + 1 != self.min_frame.load(Ordering::Acquire)
    }
}

impl fmt::Debug for WalFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFile")
            //.field("syncing", &self.syncing.load(Ordering::SeqCst))
            //.field("page_size", &self.page_size())
            //.field("shared", &self.shared)
            //.field("ongoing_checkpoint", &self.ongoing_checkpoint)
            //.field("checkpoint_threshold", &self.checkpoint_threshold)
            .field("max_frame_read_lock_index", &self.max_frame_read_lock_index)
            .field("max_frame", &self.max_frame)
            .field("min_frame", &self.min_frame)
            // Excluding other fields
            .finish()
    }
}

impl Wal for WalFile {
    fn begin_read_tx(&mut self) -> Result<bool> {
        // 이전에 진행중인 트랜잭션이 없어야 한다.
        assert!(
            self.max_frame_read_lock_index
                .load(Ordering::Acquire)
                .eq(&NO_LOCK_HELD),
            "cannot start a new read tx without ending an existing one, lock_value={}, expected={}",
            self.max_frame_read_lock_index.load(Ordering::Acquire),
            NO_LOCK_HELD
        );

        // WalFileShared에서 확인이 필요한 값들 가져오기
        let (shared_max, nbackfills, last_checksum, checkpoint_seq, transaction_count) = self
            .with_shared(|shared| {
                let mx = shared.max_frame.load(Ordering::Acquire);
                let nb = shared.nbackfills.load(Ordering::Acquire);
                let ck = shared.last_checksum;
                let checkpoint_seq = shared.wal_header.lock().checkpoint_seq;
                let transaction_count = shared.transaction_count.load(Ordering::Acquire);
                (mx, nb, ck, checkpoint_seq, transaction_count)
            });
        let db_changed = self.with_shared(|shared| self.db_changed(shared));

        if shared_max == nbackfills {
            // WAL 의 내용이 데이버테이스 파일에 모두 적용되어 있다.
            tracing::debug!("begin_read_tx: WAL is already fully back‑filled into the main DB image, shared_max={}, nbackfills={}", shared_max, nbackfills);
            let lock_0_idx = 0;
        }

        Ok(false)
    }

    #[inline(always)]
    fn end_read_tx(&self) {
        let slot = self.max_frame_read_lock_index.load(Ordering::Acquire);
        if slot != NO_LOCK_HELD {
            // 이전에 획득했던 read lock을 unlock 한다.
            // NOTE: with_shared를 사용하지 않은 이유는?
            // 다른 쓰레드에서 'begin_read_tx'의 "슬롯 찾기 및 점유" 과정과 경쟁할 수 있어서 with_shared_mut를 사용하는 것으로 보임
            // => read_locks 배열 전체의 상태를 변경하는 작업들이 서로 직렬화되도록 한다.
            self.with_shared_mut(|shared| shared.read_locks[slot].unlock());
            // 현재 connection에서 사용중인 read lock 슬롯 정보 해제 
            self.max_frame_read_lock_index.store(NO_LOCK_HELD, Ordering::Release);
            tracing::debug!("end_read_tx(slot={slot})");
        } else {
            // read lock을 획득하지 않은 상태에서 end_read_tx 함수를 호출했으므로 무시
            tracing::debug!("end_read_tx(slot=no_lock)");
        }
    }

    fn begin_write_tx(&mut self) -> Result<()> {
        // WalFileShared를 RWLock으로 보호
        self.with_shared_mut(|shared| {
            // write 트랜잭션을 시작하려면 이미 read 트랜잭션을 가지고 있어야 한다.
            assert!(
                self.max_frame_read_lock_index.load(Ordering::Acquire) != NO_LOCK_HELD,
                "must have a read transaction to begin a write transaction"
            );

            if !shared.write_lock.write() {
                return Err(GrainError::Busy);
            }

            // read transaction 이후로 변경 사항이 있는지 확인
            let db_changed = self.db_changed(shared);
            if !db_changed {
                // WalFileShared의 내용이 변경되지 않았다.
                return Ok(());
            }

            // WalFileShared의 내용이 변경되었음
            tracing::debug!(
                "unable to upgrade transaction from read to write: snapshot is stale, give up and let caller retry from scratch, self.max_frame={}, shared_max={}",
                self.max_frame.load(Ordering::Acquire),
                shared.max_frame.load(Ordering::Acquire)
            );
            shared.write_lock.unlock();
            Err(GrainError::Busy)
        })
    }

    fn end_write_tx(&self) {
        tracing::debug!("end_write_txn");
        self.with_shared(|shared| shared.write_lock.unlock());
    }
}

/// 하나의 WAL 파일에 대한 관리
pub struct WalFileShared {
    // 저널링 모드가 WAL로 설정되었는지를 확인
    pub enabled: AtomicBool,
    // 실제 `.wal` 파일이 물리적으로 사용 준비가 되었는지를 확인
    pub initialized: AtomicBool,
    pub wal_header: Arc<SpinLock<WalHeader>>,
    pub min_frame: AtomicU64,
    pub max_frame: AtomicU64,
    pub nbackfills: AtomicU64,
    pub transaction_count: AtomicU64,
    pub last_checksum: (u32, u32),
    pub file: Option<Arc<dyn File>>,
    pub read_locks: [GrainRwLock; 5],
    pub write_lock: GrainRwLock,
}

impl WalFileShared {
    /// Arc<RwLock<T>>: 여러 스레드가 같은 객체에 접근해야 하고, 읽기 비중이 매우 높을 때
    pub fn open_shared_if_exists(io: &Arc<dyn IO>, path: &str) -> Result<Arc<RwLock<WalFileShared>>> {
        let file = io.open_file(path, false)?;
        if file.size()? == 0 {
            // WAL 파일이 없는 경우 새로 생성한다.
            return WalFileShared::new_noop();
        }

        // TODO: 이미 WAL 파일이 있는 경우 내용을 읽는다.
        WalFileShared::new_noop()
    }

    pub fn new_noop() -> Result<Arc<RwLock<WalFileShared>>> {
        let wal_header = WalHeader {
            magic: 0,
            file_format: 0,
            page_size: 0,
            checkpoint_seq: 0,
            salt_1: 0,
            salt_2: 0,
            checksum_1: 0,
            checksum_2: 0,
        };
        let read_locks = array::from_fn(|_| GrainRwLock::new());
        for (i, lock) in read_locks.iter().enumerate() {
            lock.write();
            lock.set_value_exclusive(if i < 2 { 0 } else { READMARK_NOT_USED });
            lock.unlock();
        }

        let shared = WalFileShared {
            enabled: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            wal_header: Arc::new(SpinLock::new(wal_header)),
            min_frame: AtomicU64::new(0),
            max_frame: AtomicU64::new(0),
            nbackfills: AtomicU64::new(0),
            transaction_count: AtomicU64::new(0),
            last_checksum: (0, 0),
            file: None,
            read_locks,
            write_lock: GrainRwLock::new(),
        };
        Ok(Arc::new(RwLock::new(shared)))
    }

    // WAL 헤더에 magic과 file format 설정
    // enabled -> true, initialized -> false
    pub fn create(&mut self, file: Arc<dyn File>) -> Result<()> {
        if self.enabled.load(Ordering::SeqCst) {
            return Ok(())
        }

        let magic = if cfg!(target_endian = "big") {
            WAL_MAGIC_BE
        } else {
            WAL_MAGIC_LE
        };

        *self.wal_header.lock() = WalHeader {
            magic,
            file_format: 3007000,
            page_size: 0,
            checkpoint_seq: 0,
            salt_1: 0,
            salt_2: 0,
            checksum_1: 0,
            checksum_2: 0,
        };

        self.file = Some(file);
        self.enabled.store(true, Ordering::SeqCst);
        self.initialized.store(false, Ordering::SeqCst);

        Ok(())
    }

    pub fn page_size(&self) -> u32 {
        self.wal_header.lock().page_size
    }
}
