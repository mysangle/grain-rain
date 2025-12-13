
use crate::{
    mvcc::{
        clock::LogicalClock,
        persistent_storage::Storage,
    },
    schema::Table,
    storage::{
        sqlite3_ondisk::DatabaseHeader,
        wal::GrainRwLock,
    },
    Connection, GrainError, IOExt, Result, Pager,
};
use crossbeam_skiplist::{SkipMap, SkipSet};
use parking_lot::RwLock;
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc,
};

const NO_EXCLUSIVE_TX: u64 = 0;
pub const SQLITE_SCHEMA_MVCC_TABLE_ID: MVTableId = MVTableId(-1);

/// MVCC의 테이블 ID
/// 항상 음수이다.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct MVTableId(i64);

impl MVTableId {
    pub fn new(value: i64) -> Self {
        assert!(value < 0, "MVCC table IDs are always negative");
        Self(value)
    }
}

impl From<i64> for MVTableId {
    fn from(value: i64) -> Self {
        assert!(value < 0, "MVCC table IDs are always negative");
        Self(value)
    }
}

impl From<MVTableId> for i64 {
    fn from(value: MVTableId) -> Self {
        value.0
    }
}

pub type TxID = u64;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RowKey {

}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowID {
    /// The table ID. Analogous to table's root page number.
    pub table_id: MVTableId,
    pub row_id: RowKey,
}

impl PartialOrd for RowID {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowID {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Make sure table id is first comparison so that we sort first by table_id and then by
        // rowid. Due to order of the struct, table_id is first which is fine but if we were to
        // change it we would bring chaos.
        match self.table_id.cmp(&other.table_id) {
            std::cmp::Ordering::Equal => self.row_id.cmp(&other.row_id),
            ord => ord,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum TransactionState {
    Active,
    Preparing,
    Aborted,
    Terminated,
    Committed(u64),
}

/// 내부적으로 Active, Preparing, Aborted, Terminated와 구별하기 위해 Committed는 최상위 비트를 1로 설정한다.
/// TransactionState::Committed(ts)의 경우 ts의 값을 0x7fff_ffff_ffff_ffff 만 사용한다.
impl TransactionState {
    pub fn encode(&self) -> u64 {
        match self {
            TransactionState::Active => 0,
            TransactionState::Preparing => 1,
            TransactionState::Aborted => 2,
            TransactionState::Terminated => 3,
            TransactionState::Committed(ts) => {
                // We only support 2*62 - 1 timestamps, because the extra bit
                // is used to encode the type.
                assert!(ts & 0x8000_0000_0000_0000 == 0);
                0x8000_0000_0000_0000 | ts
            }
        }
    }

    pub fn decode(v: u64) -> Self {
        match v {
            0 => TransactionState::Active,
            1 => TransactionState::Preparing,
            2 => TransactionState::Aborted,
            3 => TransactionState::Terminated,
            v if v & 0x8000_0000_0000_0000 != 0 => {
                TransactionState::Committed(v & 0x7fff_ffff_ffff_ffff)
            }
            _ => panic!("Invalid transaction state"),
        }
    }
}

/// TransactionState를 64 비트로 인코딩
#[derive(Debug)]
pub(crate) struct AtomicTransactionState {
    pub(crate) state: AtomicU64,
}

impl From<TransactionState> for AtomicTransactionState {
    fn from(state: TransactionState) -> Self {
        Self {
            state: AtomicU64::new(state.encode()),
        }
    }
}

impl From<AtomicTransactionState> for TransactionState {
    fn from(state: AtomicTransactionState) -> Self {
        let encoded = state.state.load(Ordering::Acquire);
        TransactionState::decode(encoded)
    }
}

impl std::cmp::PartialEq<TransactionState> for AtomicTransactionState {
    fn eq(&self, other: &TransactionState) -> bool {
        &self.load() == other
    }
}

impl AtomicTransactionState {
    fn store(&self, state: TransactionState) {
        self.state.store(state.encode(), Ordering::Release);
    }

    fn load(&self) -> TransactionState {
        TransactionState::decode(self.state.load(Ordering::Acquire))
    }
}

#[derive(Debug)]
pub struct Transaction {
    /// The state of the transaction.
    state: AtomicTransactionState,
    /// The transaction ID.
    tx_id: u64,
    /// The transaction begin timestamp.
    begin_ts: u64,
    /// The transaction write set.
    write_set: SkipSet<RowID>,
    /// The transaction read set.
    read_set: SkipSet<RowID>,
    /// The transaction header.
    header: RwLock<DatabaseHeader>,
}

impl Transaction {
    fn new(tx_id: u64, begin_ts: u64, header: DatabaseHeader) -> Transaction {
        Transaction {
            state: TransactionState::Active.into(),
            tx_id,
            begin_ts,
            write_set: SkipSet::new(),
            read_set: SkipSet::new(),
            header: RwLock::new(header),
        }
    }
}

#[derive(Debug)]
struct CommitCoordinator {
    pager_commit_lock: Arc<GrainRwLock>,
}

/// SkipMap: lock-free(또는 very-low-contention) 정렬 맵(sorted map)
#[derive(Debug)]
pub struct MvStore<Clock: LogicalClock> {
    pub table_id_to_rootpage: SkipMap<MVTableId, Option<u64>>,
    // unique timestamp
    clock: Clock,
    storage: Storage,
    txs: SkipMap<TxID, Transaction>,
    tx_ids: AtomicU64,
    // 새로운 테이블이나 인덱스가 생성될 때 해당 객체에 대한 임시 ID로 사용되는 값
    next_table_id: AtomicI64,
    exclusive_tx: AtomicU64,
    blocking_checkpoint_lock: Arc<GrainRwLock>,
    global_header: Arc<RwLock<Option<DatabaseHeader>>>,
    last_committed_tx_ts: AtomicU64,
    // 한번에 하나의 트랜잭션만 커밋을 할 수 있도록 한다.
    // 예: begin_exclusive_tx 시작시 write lock을 획득하여 다른 트랜잭션이 커밋을 시작하지 못하도록 한다.
    commit_coordinator: Arc<CommitCoordinator>,
}

impl<Clock: LogicalClock> MvStore<Clock> {
    pub fn new(clock: Clock, storage: Storage) -> Self {
        Self {
            table_id_to_rootpage: SkipMap::from_iter(vec![(SQLITE_SCHEMA_MVCC_TABLE_ID, Some(1))]),
            clock,
            storage,
            txs: SkipMap::new(),
            tx_ids: AtomicU64::new(1), // let's reserve transaction 0 for special purposes
            next_table_id: AtomicI64::new(-2), // table id -1 / root page 1 is always sqlite_schema.
            exclusive_tx: AtomicU64::new(NO_EXCLUSIVE_TX),
            blocking_checkpoint_lock: Arc::new(GrainRwLock::new()),
            global_header: Arc::new(RwLock::new(None)),
            last_committed_tx_ts: AtomicU64::new(0),
            commit_coordinator: Arc::new(CommitCoordinator {
                pager_commit_lock: Arc::new(GrainRwLock::new()),
            }),
        }
    }

    /// sqlite_schema 테이블과 logical log로부터 MV store 초기화
    /// 1. sqlite_schema에서 모든 root page 정보를 읽는다.
    /// 2. 테이블 ID와 root page를 mapping (table_id = -1 * root_page)
    /// 3. bootstrap connection을 regular connection으로 변경
    pub fn bootstrap(&self, bootstrap_conn: Arc<Connection>) -> Result<()> {
        {
            let schema = bootstrap_conn.schema.read();
            let sqlite_schema_root_pages = {
                schema.tables.values()
                    .filter_map(|t| {
                        // 테이블의 root page 읽기
                        if let Table::BTree(btree) = t.as_ref() {
                            Some(btree.root_page)
                        } else {
                            None
                        }
                    })
                    .chain(
                        // index의 root page 읽기
                        schema.indexes.values().flatten().map(|index| index.root_page)
                    )
            };

            // MV store에 root page 저장 (table id -> root page)
            for root_page in sqlite_schema_root_pages {
                assert!(root_page > 0, "root_page={root_page} must be positive");
                let root_page_as_table_id = MVTableId::from(-(root_page));
                self.insert_table_id_to_rootpage(root_page_as_table_id, Some(root_page as u64));
            }
        }

        bootstrap_conn.promote_to_regular_connection();

        if !self.maybe_recover_logical_log(bootstrap_conn.clone())? {
            // There was no logical log to recover, so we're done.
            return Ok(());
        }

        Ok(())
    }

    /// blocking_checkpoint_lock: begin_exclusive_tx or begin_tx -> remove_tx
    pub fn begin_exclusive_tx(&self, pager: Arc<Pager>, maybe_existing_tx_id: Option<TxID>) -> Result<TxID> {
        if !self.blocking_checkpoint_lock.read() {
            // If there is a stop-the-world checkpoint in progress, we cannot begin any transaction at all.
            return Err(GrainError::Busy);
        }
        // 에러의 경우에 unlock()을 호출하여 blocking_checkpoint_lock에 대한 lock을 놓는다.
        let unlock = || self.blocking_checkpoint_lock.unlock();
        let tx_id = maybe_existing_tx_id.unwrap_or_else(|| self.get_tx_id());
        let begin_ts = if let Some(tx_id) = maybe_existing_tx_id {
            self.txs
                .get(&tx_id)
                .ok_or(GrainError::NoSuchTransactionID(tx_id.to_string()))?
                .value()
                .begin_ts
        } else {
            self.get_timestamp()
        };

        self.acquire_exclusive_tx(&tx_id)
            .inspect_err(|_| unlock())?;

        let locked = self.commit_coordinator.pager_commit_lock.write();
        if !locked {
            tracing::debug!(
                "begin_exclusive_tx: tx_id={} failed with Busy on pager_commit_lock",
                tx_id
            );
            self.release_exclusive_tx(&tx_id);
            unlock();
            return Err(GrainError::Busy);
        }

        let header = self.get_new_transaction_database_header(&pager);
        let tx = Transaction::new(tx_id, begin_ts, header);
        tracing::trace!(
            "begin_exclusive_tx(tx_id={}) - exclusive write logical log transaction",
            tx_id
        );
        tracing::debug!("begin_exclusive_tx: tx_id={} succeeded", tx_id);
        self.txs.insert(tx_id, tx);
        Ok(tx_id)
    }

    /// 새 트랜잭션 생성
    pub fn begin_tx(&self, pager: Arc<Pager>) -> Result<TxID> {
        if !self.blocking_checkpoint_lock.read() {
            // If there is a stop-the-world checkpoint in progress, we cannot begin any transaction at all.
            return Err(GrainError::Busy);
        }
        // transaction id
        let tx_id = self.get_tx_id();
        // logical clock
        let begin_ts = self.get_timestamp();
        let header = self.get_new_transaction_database_header(&pager);
        let tx = Transaction::new(tx_id, begin_ts, header);
        tracing::trace!("begin_tx(tx_id={})", tx_id);
        self.txs.insert(tx_id, tx);
        Ok(tx_id)
    }

    pub fn remove_tx(&self, tx_id: TxID) {
        self.txs.remove(&tx_id);
        self.blocking_checkpoint_lock.unlock();
    }

    /// Acquires the exclusive transaction lock to the given transaction ID.
    fn acquire_exclusive_tx(&self, tx_id: &TxID) -> Result<()> {
        if let Some(tx) = self.txs.get(tx_id) {
            let tx = tx.value();
            if tx.begin_ts < self.last_committed_tx_ts.load(Ordering::Acquire) {
                // Another transaction committed after this transaction's begin timestamp, do not allow exclusive lock.
                // This mimics regular (non-CONCURRENT) sqlite transaction behavior.
                return Err(GrainError::Busy);
            }
        }
        match self.exclusive_tx.compare_exchange(
            NO_EXCLUSIVE_TX,
            *tx_id,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(_) => {
                // Another transaction already holds the exclusive lock
                Err(GrainError::Busy)
            }
        }
    }

    /// Release the exclusive transaction lock if held by the this transaction.
    fn release_exclusive_tx(&self, tx_id: &TxID) {
        tracing::trace!("release_exclusive_tx(tx_id={})", tx_id);
        let prev = self.exclusive_tx.swap(NO_EXCLUSIVE_TX, Ordering::Release);
        assert_eq!(
            prev, *tx_id,
            "Tried to release exclusive lock for tx {tx_id} but it was held by tx {prev}"
        );
    }

    fn get_new_transaction_database_header(&self, pager: &Arc<Pager>) -> DatabaseHeader {
        if self.global_header.read().is_none() {
            // db에서 헤더를 가져온다.
            // block 함수는 IOResult::Done이 될 때까지 loop를 돈다.
            pager
                .io
                .block(|| pager.maybe_allocate_page1())
                .expect("failed to allocate page1");
            let header = pager
                .io
                .block(|| pager.with_header(|header| *header))
                .expect("failed to read database header");
            // TODO: We initialize header here, maybe this needs more careful handling
            self.global_header.write().replace(header);
            tracing::debug!(
                "get_transaction_database_header create: header={:?}",
                header
            );
            header
        } else {
            // 저장되어 있는 헤더 리턴
            let header = self
                .global_header
                .read()
                .expect("global_header should be initialized");
            tracing::debug!("get_transaction_database_header read: header={:?}", header);
            header
        }
    }

    pub fn get_transaction_database_header(&self, tx_id: &TxID) -> DatabaseHeader {
        let tx = self
            .txs
            .get(tx_id)
            .expect("transaction not found when trying to get header");
        let header = tx.value();
        let header = header.header.read();
        tracing::debug!("get_transaction_database_header read: header={:?}", header);
        *header
    }

    /// root page로부터 table id를 얻는다.
    pub fn get_table_id_from_root_page(&self, root_page: i64) -> MVTableId {
        if root_page < 0 {
            // Not checkpointed table - table ID and root_page are both the same negative value
            // 아직 저장되지 않은 테이블
            root_page.into()
        } else {
            // Root page is positive: it is a checkpointed table and there should be a corresponding table ID
            let root_page = root_page as u64;
            // [table id -> root page]의 map 이므로 map의 value로부터 root page를 찾는다.  
            let table_id = self
                .table_id_to_rootpage
                .iter()
                .find(|entry| entry.value().is_some_and(|value| value == root_page))
                .map(|entry| *entry.key())
                .unwrap_or_else(|| {
                    panic!("Positive root page is not mapped to a table id: {root_page}")
                });
            table_id
        }
    }

    // (table id -> root page) 저장
    pub fn insert_table_id_to_rootpage(&self, table_id: MVTableId, root_page: Option<u64>) {
        self.table_id_to_rootpage.insert(table_id, root_page);
        let minimum: i64 = if let Some(root_page) = root_page {
            let root_page_as_table_id = MVTableId::from(-(root_page as i64));
            table_id.min(root_page_as_table_id).into()
        } else {
            table_id.into()
        };
        // 다음 테이블 ID 할당을 위한 준비(기존 테이블 ID와 겹치지 않아야 한다.)
        if minimum <= self.next_table_id.load(Ordering::SeqCst) {
            self.next_table_id.store(minimum - 1, Ordering::SeqCst);
        }
    }

    pub fn maybe_recover_logical_log(&self, connection: Arc<Connection>) -> Result<bool> {
        Ok(true)
    }

    pub fn get_next_table_id(&self) -> i64 {
        self.next_table_id.fetch_sub(1, Ordering::SeqCst)
    }

    /// Generates next unique transaction id
    pub fn get_tx_id(&self) -> u64 {
        self.tx_ids.fetch_add(1, Ordering::SeqCst)
    }

    /// Gets current timestamp
    pub fn get_timestamp(&self) -> u64 {
        self.clock.get_timestamp()
    }
}
