
use crate::{
    mvcc::{
        clock::LogicalClock,
        persistent_storage::Storage,
    },
    schema::Table,
    Connection, Result, Pager,
};
use crossbeam_skiplist::SkipMap;
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

#[derive(Debug)]
pub struct MvStore<Clock: LogicalClock> {
    pub table_id_to_rootpage: SkipMap<MVTableId, Option<u64>>,
    // unique timestamp
    clock: Clock,
    storage: Storage,

    tx_ids: AtomicU64,
    // 새로운 테이블이나 인덱스가 생성될 때 해당 객체에 대한 임시 ID로 사용되는 값
    next_table_id: AtomicI64,
    exclusive_tx: AtomicU64,
}

impl<Clock: LogicalClock> MvStore<Clock> {
    pub fn new(clock: Clock, storage: Storage) -> Self {
        Self {
            table_id_to_rootpage: SkipMap::from_iter(vec![(SQLITE_SCHEMA_MVCC_TABLE_ID, Some(1))]),
            clock,
            storage,
            tx_ids: AtomicU64::new(1), // let's reserve transaction 0 for special purposes
            next_table_id: AtomicI64::new(-2), // table id -1 / root page 1 is always sqlite_schema.
            exclusive_tx: AtomicU64::new(NO_EXCLUSIVE_TX),
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

    pub fn begin_exclusive_tx(&self, pager: Arc<Pager>, maybe_existing_tx_id: Option<TxID>) -> Result<TxID> {
        Ok(tx_id)
    }

    pub fn begin_tx(&self, pager: Arc<Pager>) -> Result<TxID> {
        Ok(tx_id)
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
