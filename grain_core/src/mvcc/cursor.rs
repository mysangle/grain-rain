
use crate::{
    mvcc::{
        clock::LogicalClock,
        database::{MvStore, MVTableId},
    },
    storage::btree::{BTreeKey, CursorTrait, BTreeCursor},
    types::IOResult,
    return_if_io, Result,
};
use parking_lot::RwLock;
use std::{
    any::Any,
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccCursorType {
    Table,
}

pub struct MvccLazyCursor<Clock: LogicalClock> {
    pub db: Arc<MvStore<Clock>>,
    tx_id: u64,
    table_id: MVTableId,
    mv_cursor_type: MvccCursorType,
    btree_cursor: Box<dyn CursorTrait>,
    next_rowid_lock: Arc<RwLock<()>>,
}

impl<Clock: LogicalClock + 'static> MvccLazyCursor<Clock> {
    pub fn new(
        db: Arc<MvStore<Clock>>,
        tx_id: u64,
        root_page_or_table_id: i64,
        mv_cursor_type: MvccCursorType,
        btree_cursor: Box<dyn CursorTrait>,
    ) -> Result<MvccLazyCursor<Clock>> {
        assert!(
            (&*btree_cursor as &dyn Any).is::<BTreeCursor>(),
            "BTreeCursor expected for mvcc cursor"
        );
        let table_id = db.get_table_id_from_root_page(root_page_or_table_id);

        Ok(Self {
            db,
            tx_id,
            table_id,
            mv_cursor_type,
            btree_cursor,
            next_rowid_lock: Arc::new(RwLock::new(())),
        })
    }

    /// 테이블에서 다음 rowid를 얻기
    pub fn get_next_rowid(&mut self) -> Result<IOResult<i64>> {
        let lock = self.next_rowid_lock.clone();
        let _lock = lock.write();
        return_if_io!(self.last());
    }
}

impl<Clock: LogicalClock + 'static> CursorTrait for MvccLazyCursor<Clock> {
    fn last(&mut self) -> Result<IOResult<()>> {

    }

    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>> {

    }
}
