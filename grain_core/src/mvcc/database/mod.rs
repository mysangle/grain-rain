
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
    types::{ImmutableRecord, IndexInfo},
    Connection, GrainError, IOExt, Result, Pager,
};
use crossbeam_skiplist::{SkipMap, SkipSet};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc,
    },
};

const NO_EXCLUSIVE_TX: u64 = 0;
pub const SQLITE_SCHEMA_MVCC_TABLE_ID: MVTableId = MVTableId(-1);
pub const LOGICAL_LOG_RECOVERY_TRANSACTION_ID: u64 = 0;
pub const LOGICAL_LOG_RECOVERY_COMMIT_TIMESTAMP: u64 = 0;

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

/// Wrapper for index keys that implements collation-aware, ASC/DESC-aware ordering.
#[derive(Debug, Clone)]
pub struct SortableIndexKey {
    pub key: ImmutableRecord,
    pub metadata: Arc<IndexInfo>,
}

impl SortableIndexKey {
    pub fn new_from_record(key: ImmutableRecord, metadata: Arc<IndexInfo>) -> Self {
        Self { key, metadata }
    }
}

impl PartialEq for SortableIndexKey {
    fn eq(&self, other: &Self) -> bool {
        if self.key == other.key {
            return true;
        }

        self.compare(other)
            .map(|ord| ord == std::cmp::Ordering::Equal)
            .unwrap_or(false)
    }
}

impl Eq for SortableIndexKey {}

impl PartialOrd for SortableIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableIndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.compare(other).expect("Failed to compare IndexKeys")
    }
}

pub type TxID = u64;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RowKey {
    Int(i64),
    Record(SortableIndexKey),
}

impl RowKey {
    pub fn to_int_or_panic(&self) -> i64 {
        match self {
            RowKey::Int(row_id) => *row_id,
            _ => panic!("RowKey is not an integer"),
        }
    }

    pub fn is_int_key(&self) -> bool {
        matches!(self, RowKey::Int(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowID {
    /// The table ID. Analogous to table's root page number.
    pub table_id: MVTableId,
    pub row_id: RowKey,
}

impl RowID {
    pub fn new(table_id: MVTableId, row_id: RowKey) -> Self {
        Self { table_id, row_id }
    }
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

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Row {
    pub id: RowID,
    /// Data is None for index rows because the key holds all the data.
    pub data: Option<Vec<u8>>,
    pub column_count: usize,
}

impl Row {
    pub fn new_table_row(id: RowID, data: Vec<u8>, column_count: usize) -> Self {
        Self {
            id,
            data: Some(data),
            column_count,
        }
    }

    pub fn new_index_row(id: RowID, column_count: usize) -> Self {
        Self {
            id,
            data: None,
            column_count,
        }
    }
}

/// A row version.
/// TODO: we can optimize this by using bitpacking for the begin and end fields.
#[derive(Clone, Debug, PartialEq)]
pub struct RowVersion {
    pub begin: Option<TxTimestampOrID>,
    pub end: Option<TxTimestampOrID>,
    pub row: Row,
    /// Indicates this version was created for a row that existed in B-tree before
    /// MVCC was enabled (e.g., after switching from WAL to MVCC journal mode).
    /// This flag helps the checkpoint logic determine if a delete should be
    /// checkpointed to the B-tree file.
    pub btree_resident: bool,
}

impl RowVersion {
    pub fn is_visible_to(&self, tx: &Transaction, txs: &SkipMap<TxID, Transaction>) -> bool {
        is_begin_visible(txs, tx, self) && is_end_visible(txs, tx, self)
    }
}

fn is_begin_visible(txs: &SkipMap<TxID, Transaction>, tx: &Transaction, rv: &RowVersion) -> bool {
    match rv.begin {
        Some(TxTimestampOrID::Timestamp(rv_begin_ts)) => tx.begin_ts >= rv_begin_ts,
        Some(TxTimestampOrID::TxID(rv_begin)) => {
            let tb = txs
                .get(&rv_begin)
                .expect("transaction should exist in txs map");
            let tb = tb.value();
            let visible = match tb.state.load() {
                TransactionState::Active => tx.tx_id == tb.tx_id && rv.end.is_none(),
                TransactionState::Preparing => false, // NOTICE: makes sense for snapshot isolation, not so much for serializable!
                TransactionState::Committed(committed_ts) => tx.begin_ts >= committed_ts,
                TransactionState::Aborted => false,
                TransactionState::Terminated => {
                    tracing::debug!("TODO: should reread rv's end field - it should have updated the timestamp in the row version by now");
                    false
                }
            };
            tracing::trace!(
                "is_begin_visible: tx={tx}, tb={tb} rv = {:?}-{:?} visible = {visible}",
                rv.begin,
                rv.end
            );
            visible
        }
        None => false,
    }
}

fn is_end_visible(
    txs: &SkipMap<TxID, Transaction>,
    current_tx: &Transaction,
    row_version: &RowVersion,
) -> bool {
    match row_version.end {
        Some(TxTimestampOrID::Timestamp(rv_end_ts)) => current_tx.begin_ts < rv_end_ts,
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let other_tx = txs
                .get(&rv_end)
                .unwrap_or_else(|| panic!("Transaction {rv_end} not found"));
            let other_tx = other_tx.value();
            let visible = match other_tx.state.load() {
                // V's sharp mind discovered an issue with the hekaton paper which basically states that a
                // transaction can see a row version if the end is a TXId only if it isn't the same transaction.
                // Source: https://avi.im/blag/2023/hekaton-paper-typo/
                TransactionState::Active => current_tx.tx_id != other_tx.tx_id,
                TransactionState::Preparing => false, // NOTICE: makes sense for snapshot isolation, not so much for serializable!
                TransactionState::Committed(committed_ts) => current_tx.begin_ts < committed_ts,
                TransactionState::Aborted => false,
                TransactionState::Terminated => {
                    tracing::debug!("TODO: should reread rv's end field - it should have updated the timestamp in the row version by now");
                    false
                }
            };
            tracing::trace!(
                "is_end_visible: tx={current_tx}, te={other_tx} rv = {:?}-{:?}  visible = {visible}",
                row_version.begin,
                row_version.end
            );
            visible
        }
        None => true,
    }
}

/// A transaction timestamp or ID.
///
/// Versions either track a timestamp or a transaction ID, depending on the
/// phase of the transaction. During the active phase, new versions track the
/// transaction ID in the `begin` and `end` fields. After a transaction commits,
/// versions switch to tracking timestamps.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum TxTimestampOrID {
    /// A committed transaction's timestamp.
    Timestamp(u64),
    /// The ID of a non-committed transaction.
    TxID(TxID),
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


impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            TransactionState::Active => write!(f, "Active"),
            TransactionState::Preparing => write!(f, "Preparing"),
            TransactionState::Committed(ts) => write!(f, "Committed({ts})"),
            TransactionState::Aborted => write!(f, "Aborted"),
            TransactionState::Terminated => write!(f, "Terminated"),
        }
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

    fn insert_to_read_set(&self, id: RowID) {
        self.read_set.insert(id);
    }

    fn insert_to_write_set(&self, id: RowID) {
        self.write_set.insert(id);
    }
}

impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "{{ state: {}, id: {}, begin_ts: {}, write_set: [",
            self.state.load(),
            self.tx_id,
            self.begin_ts,
        )?;

        for (i, v) in self.write_set.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?
            }
            write!(f, "{:?}", *v.value())?;
        }

        write!(f, "], read_set: [")?;
        for (i, v) in self.read_set.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}", *v.value())?;
        }

        write!(f, "] }}")
    }
}

#[derive(Debug)]
struct CommitCoordinator {
    pager_commit_lock: Arc<GrainRwLock>,
}

/// 테이블에 대한 행 ID(row ID) 할당을 관리
#[derive(Debug)]
pub struct RowidAllocator {
    /// Lock to ensure that only one thread can initialize the rowid allocator at a time.
    /// In case of unitialized values, we will look for last rowid first.
    lock: GrainRwLock,
    /// last_rowid is the last rowid that was allocated.
    /// 테이블에 대해 현재까지 할당된 가장 큰 행 ID를 저장
    max_rowid: RwLock<Option<i64>>,
    /// 초기 설정을 완료했는지 여부
    initialized: AtomicBool,
}

impl RowidAllocator {
    /// 이전 max row id와 1 증가한 새 row id를 리턴
    pub fn get_next_rowid(&self) -> Option<(i64, Option<i64>)> {
        let mut last_rowid_guard = self.max_rowid.write();
        if last_rowid_guard.is_none() {
            // Case 1. Table is empty
            // Database is empty because there is no last rowid
            *last_rowid_guard = Some(1);
            tracing::trace!("get_next_rowid(empty, 1)");
            Some((1, None))
        } else {
            // Case 2. Table is not empty
            let last_rowid = last_rowid_guard.unwrap();
            if last_rowid == i64::MAX {
                // 이미 값이 max에 다다랐으면 None을 리턴한다.
                tracing::trace!("get_next_rowid(max)");
                None
            } else {
                let next_rowid = last_rowid + 1;
                // 이전 row id에서 1 증가한 값을 저장
                *last_rowid_guard = Some(next_rowid);
                tracing::trace!("get_next_rowid({next_rowid})");
                Some((next_rowid, Some(last_rowid)))
            }
        }
    }

    /// 새로운 rowid를 insert한 경우 last_rowid를 업데이트 해준다.
    pub fn insert_row_id_maybe_update(&self, rowid: i64) {
        let mut last_rowid = self.max_rowid.write();
        if let Some(last_rowid) = last_rowid.as_mut() {
            *last_rowid = (*last_rowid).max(rowid);
        } else {
            *last_rowid = Some(rowid);
        }
    }

    pub fn is_uninitialized(&self) -> bool {
        !self.initialized.load(Ordering::SeqCst)
    }

    pub fn initialize(&self, rowid: Option<i64>) {
        tracing::trace!("initialize({rowid:?})");

        let mut last_rowid = self.max_rowid.write();
        *last_rowid = rowid;
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub fn lock(&self) -> bool {
        self.lock.write()
    }

    pub fn unlock(&self) {
        self.lock.unlock()
    }
}

/// SkipMap: lock-free(또는 very-low-contention) 정렬 맵(sorted map)
#[derive(Debug)]
pub struct MvStore<Clock: LogicalClock> {
    pub rows: SkipMap<RowID, RwLock<Vec<RowVersion>>>,
    /// Unlike table rows which are stored in a single map, we have a separate map for every index
    /// because operations like last() on an index are much easier when we don't have to take the
    /// table identifier into account.
    pub index_rows: SkipMap<MVTableId, SkipMap<SortableIndexKey, RwLock<Vec<RowVersion>>>>,
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
    table_id_to_last_rowid: RwLock<HashMap<MVTableId, Arc<RowidAllocator>>>,
}

impl<Clock: LogicalClock> MvStore<Clock> {
    pub fn new(clock: Clock, storage: Storage) -> Self {
        Self {
            rows: SkipMap::new(),
            index_rows: SkipMap::new(),
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
            table_id_to_last_rowid: RwLock::new(HashMap::new()),
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

    /// Same as insert() but can insert to a table or an index, indicated by the `maybe_index_id` argument.
    pub fn insert_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        tracing::trace!("insert(tx_id={}, row.id={:?})", tx_id, row.id);

        let tx = self
            .txs
            .get(&tx_id)
            .ok_or(GrainError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        assert_eq!(tx.state, TransactionState::Active);
        let id = row.id.clone();
        match maybe_index_id {
            Some(index_id) => {
                let row_version = RowVersion {
                    begin: Some(TxTimestampOrID::TxID(tx.tx_id)),
                    end: None,
                    row: row.clone(),
                    btree_resident: false,
                };
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                tx.insert_to_write_set(id.clone());
                self.insert_index_version(index_id, sortable_key, row_version);
            }
            None => {
                let row_version = RowVersion {
                    begin: Some(TxTimestampOrID::TxID(tx.tx_id)),
                    end: None,
                    row,
                    btree_resident: false,
                };
                tx.insert_to_write_set(id.clone());
                let allocator = self.get_rowid_allocator(&id.table_id);
                allocator.insert_row_id_maybe_update(id.row_id.to_int_or_panic());
                self.insert_version(id, row_version);
            }
        }
        Ok(())
    }

    /// Inserts a new row version into the database, while making sure that
    /// the row version is inserted in the correct order.
    /// MvStore의 rows에 저장. 벡터로 관리
    fn insert_version(&self, id: RowID, row_version: RowVersion) {
        let versions = self.rows.get_or_insert_with(id, || RwLock::new(Vec::new()));
        let mut versions = versions.value().write();
        self.insert_version_raw(&mut versions, row_version)
    }

    /// MvStore의 index_rows에 저장. 벡터로 관리
    pub fn insert_index_version(
        &self,
        index_id: MVTableId,
        key: SortableIndexKey,
        row_version: RowVersion,
    ) {
        let index = self.index_rows.get_or_insert_with(index_id, SkipMap::new);
        let index = index.value();
        let versions = index.get_or_insert_with(key, || RwLock::new(Vec::new()));
        let mut versions = versions.value().write();
        self.insert_version_raw(&mut versions, row_version);
    }

    /// Inserts a new row version into the internal data structure for versions,
    /// while making sure that the row version is inserted in the correct order.
    /// 시간 순서에 가깝게 들어올 것으로 예상하므로 트리를 사용하지 않고 선형 탐색 및 삽입을 사용
    pub fn insert_version_raw(&self, versions: &mut Vec<RowVersion>, row_version: RowVersion) {
        // NOTICE: this is an insert a'la insertion sort, with pessimistic linear complexity.
        // However, we expect the number of versions to be nearly sorted, so we deem it worthy
        // to search linearly for the insertion point instead of paying the price of using
        // another data structure, e.g. a BTreeSet. If it proves to be too quadratic empirically,
        // we can either switch to a tree-like structure, or at least use partition_point()
        // which performs a binary search for the insertion point.
        let mut position = 0_usize;
        // 보통 시간 순서로 들어오므로 뒤에서부터 찾기 시작
        for (i, v) in versions.iter().enumerate().rev() {
            let existing_begin = self.get_begin_timestamp(&v.begin);
            let new_begin = self.get_begin_timestamp(&row_version.begin);
            if existing_begin <= new_begin {
                if existing_begin == new_begin
                    && existing_begin == LOGICAL_LOG_RECOVERY_COMMIT_TIMESTAMP
                {
                    // During recovery we may have both an insert for row R and a deletion for row R.
                    // In these cases just replace the version so that "find_last_visible_version()" doesn't return
                    // the non-deleted version since both of the versions have the same begin timestamp during recovery.
                    versions[position] = row_version;
                    return;
                } else {
                    position = i + 1;
                }
                break;
            }
        }
        // 찾은 삽입될 위치에 insert
        versions.insert(position, row_version);
    }

    // Extracts the begin timestamp from a transaction
    #[inline]
    fn get_begin_timestamp(&self, ts_or_id: &Option<TxTimestampOrID>) -> u64 {
        match ts_or_id {
            Some(TxTimestampOrID::Timestamp(ts)) => *ts,
            Some(TxTimestampOrID::TxID(tx_id)) => {
                self.txs
                    .get(tx_id)
                    .expect("transaction should exist in txs map")
                    .value()
                    .begin_ts
            }
            // This function is intended to be used in the ordering of row versions within the row version chain in `insert_version_raw`.
            //
            // The row version chain should be append-only (aside from garbage collection),
            // so the specific ordering handled by this function may not be critical. We might
            // be able to append directly to the row version chain in the future.
            //
            // The value 0 is used here to represent an infinite timestamp value. This is a deliberate
            // choice for a planned future bitpacking optimization, reserving 0 for this purpose,
            // while actual timestamps will start from 1.
            None => 0,
        }
    }

    /// Same as read() but can read from a table or an index, indicated by the `maybe_index_id` argument.
    /// 특정 행(`id`)에 대한 최신 버전의 데이터를 읽기
    pub fn read_from_table_or_index(
        &self,
        tx_id: TxID,
        id: RowID,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<Option<Row>> {
        tracing::trace!("read(tx_id={}, id={:?})", tx_id, id);

        let tx = self
            .txs
            .get(&tx_id)
            .ok_or(GrainError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        assert_eq!(tx.state, TransactionState::Active);
        match maybe_index_id {
            Some(index_id) => {
                let rows = self.index_rows.get_or_insert_with(index_id, SkipMap::new);
                let rows = rows.value();
                let RowKey::Record(sortable_key) = id.row_id else {
                    panic!("Index reads must have a record row_id");
                };
                let row_versions_opt = rows.get(&sortable_key);
                if let Some(ref row_versions) = row_versions_opt {
                    let row_versions = row_versions.value().read();
                    if let Some(rv) = row_versions
                        .iter()
                        .rev()
                        .find(|rv| rv.is_visible_to(tx, &self.txs))
                    {
                        return Ok(Some(rv.row.clone()));
                    }
                }
                Ok(None)
            }
            None => {
                if let Some(row_versions) = self.rows.get(&id) {
                    let row_versions = row_versions.value().read();
                    // visible한 가장 최신을 찾아야 하므로 뒤에서 부터 탐색
                    if let Some(rv) = row_versions
                        .iter()
                        .rev()
                        .find(|rv| rv.is_visible_to(tx, &self.txs))
                    {
                        tx.insert_to_read_set(id);
                        return Ok(Some(rv.row.clone()));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Same as update() but can update a table or an index, indicated by the `maybe_index_id` argument.    
    pub fn update_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<bool> {
        tracing::trace!("update(tx_id={}, row.id={:?})", tx_id, row.id);
        if !self.delete_from_table_or_index(tx_id, row.id.clone(), maybe_index_id)? {
            return Ok(false);
        }
        self.insert_to_table_or_index(tx_id, row, maybe_index_id)?;
        Ok(true)
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

    pub fn get_rowid_allocator(&self, table_id: &MVTableId) -> Arc<RowidAllocator> {
        let mut map = self.table_id_to_last_rowid.write();
        if map.contains_key(table_id) {
            map.get(table_id).unwrap().clone()
        } else {
            let allocator = Arc::new(RowidAllocator {
                lock: GrainRwLock::new(),
                max_rowid: RwLock::new(None),
                initialized: AtomicBool::new(false),
            });
            map.insert(*table_id, allocator.clone());
            allocator
        }
    }
}
