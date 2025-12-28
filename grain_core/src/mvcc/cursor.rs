
use crate::{
    mvcc::{
        clock::LogicalClock,
        database::{MvStore, MVTableId, Row, RowID, RowKey, SortableIndexKey},
    },
    storage::btree::{BTreeKey, CursorTrait, BTreeCursor},
    types::{IOCompletions, IOResult, IndexInfo},
    Completion, GrainError, Result,
};
use parking_lot::RwLock;
use std::{
    any::Any,
    cell::RefCell,
    sync::Arc,
};

#[derive(Clone, Debug)]
enum CursorPosition {
    /// We haven't loaded any row yet.
    BeforeFirst,
    /// We have loaded a row. This position points to a rowid in either MVCC index or in BTree.
    Loaded {
        row_id: RowID,
        /// Indicates whether the rowid is pointing BTreeCursor or MVCC index.
        in_btree: bool,
    },
    /// We have reached the end of the table.
    End,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvccCursorType {
    Table,
    Index(Arc<IndexInfo>),
}

pub struct MvccLazyCursor<Clock: LogicalClock> {
    pub db: Arc<MvStore<Clock>>,
    tx_id: u64,
    table_id: MVTableId,
    mv_cursor_type: MvccCursorType,
    btree_cursor: Box<dyn CursorTrait>,
    next_rowid_lock: Arc<RwLock<()>>,
    // 새 rowid를 얻고 있는중인지 확인하는 플래그
    creating_new_rowid: bool,
    current_pos: RefCell<CursorPosition>,
}

pub enum NextRowidResult {
    Uninitialized,
    Next {
        new_rowid: i64,
        prev_rowid: Option<i64>,
    },
    FindRandom,
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
            creating_new_rowid: false,
            current_pos: RefCell::new(CursorPosition::BeforeFirst),
        })
    }

    /// 테이블에서 다음 rowid를 얻기
    /// start_new_rowid, initialize_max_rowid, end_new_rowid 이 세개의 함수가 하나의 세트
    pub fn start_new_rowid(&mut self) -> Result<IOResult<NextRowidResult>> {
        tracing::trace!("start_new_rowid");

        let allocator = self.db.get_rowid_allocator(&self.table_id);
        let locked = allocator.lock();
        if !locked {
            // lock을 얻지 못했다 -> 다른 커서가 rowid를 얻으려 하고 있는 중이다.
            return Ok(IOResult::IO(IOCompletions::Single(Completion::new_yield())));
        }

        // 새 rowid 가져오기를 실행중임을 표시
        self.creating_new_rowid = true;
        let res = if allocator.is_uninitialized() {
            NextRowidResult::Uninitialized
        } else if let Some((next_rowid, prev_max_rowid)) = allocator.get_next_rowid() {
            NextRowidResult::Next {
                new_rowid: next_rowid,
                prev_rowid: prev_max_rowid,
            }
        } else {
            NextRowidResult::FindRandom
        };

        Ok(IOResult::Done(res))
    }

    pub fn initialize_max_rowid(&mut self, max_rowid: Option<i64>) -> Result<()> {
        tracing::trace!("start_new_rowid");

        let allocator = self.db.get_rowid_allocator(&self.table_id);
        assert!(
            self.creating_new_rowid,
            "cursor didn't start creating new rowid"
        );
        allocator.initialize(max_rowid);
        Ok(())
    }

    /// rowid를 얻었으므로 lock을 해제
    pub fn end_new_rowid(&mut self) {
        tracing::trace!(
            "end_new_rowid creating_new_rowid={}",
            self.creating_new_rowid
        );

        if self.creating_new_rowid {
            let allocator = self.db.get_rowid_allocator(&self.table_id);
            allocator.unlock();
            // 새 rowid 가져오기가 끝났음을 표시
            self.creating_new_rowid = false;
        }
    }
}

impl<Clock: LogicalClock + 'static> CursorTrait for MvccLazyCursor<Clock> {
    fn rowid(&self) -> Result<IOResult<Option<i64>>> {

    }

    fn last(&mut self) -> Result<IOResult<()>> {
        
    }

    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>> {
        let row_id = match key {
            BTreeKey::TableRowId((rowid, _)) => RowID::new(self.table_id, RowKey::Int(*rowid)),
            BTreeKey::IndexKey(record) => {
                let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                    panic!("BTreeKey::IndexKey requires Index cursor type");
                };
                let sortable_key =
                    SortableIndexKey::new_from_record((*record).clone(), index_info.clone());
                RowID::new(self.table_id, RowKey::Record(sortable_key))
            }
        };
        let record_buf = key
            .get_record()
            .ok_or(GrainError::InternalError(
                "BTreeKey should have a record".to_string(),
            ))?
            .get_payload()
            .to_vec();
        let num_columns = match key {
            BTreeKey::IndexKey(record) => record.column_count(),
            BTreeKey::TableRowId((_, record)) => record
                .as_ref()
                .ok_or(GrainError::InternalError(
                    "TableRowId should have a record".to_string(),
                ))?
                .column_count(),
        };
        let row = match &self.mv_cursor_type {
            MvccCursorType::Table => Row::new_table_row(row_id, record_buf, num_columns),
            MvccCursorType::Index(_) => Row::new_index_row(row_id, num_columns),
        };

        // 현재 가리키고 있는 위치의 rowid와 같은지 확인.
        let was_btree_resident = match &*self.current_pos.borrow() {
            CursorPosition::Loaded {
                row_id: current_row_id,
                in_btree,
            } => *in_btree && *current_row_id == row.id,
            _ => false,
        };
        self.current_pos.replace(CursorPosition::Loaded {
            row_id: row.id.clone(),
            in_btree: false,
        });
        let maybe_index_id = match &self.mv_cursor_type {
            MvccCursorType::Index(_) => Some(self.table_id),
            MvccCursorType::Table => None,
        };

        if self
            .db
            .read_from_table_or_index(self.tx_id, row.id.clone(), maybe_index_id)?
            .is_some()
        {
            // 업데이트: delete and insert
            self.db
                .update_to_table_or_index(self.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos.replace(CursorPosition::BeforeFirst);
                })?;
        } else if was_btree_resident {
            // 디스크에만 존재하던 행을 처음으로 수정
            // The row exists in B-tree but not in MvStore - mark it as B-tree resident
            // so that checkpoint knows to write deletes to the B-tree file.
            self.db
                .insert_btree_resident_to_table_or_index(self.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos.replace(CursorPosition::BeforeFirst);
                })?;
        } else {
            // 새로운 행 삽입
            self.db
                .insert_to_table_or_index(self.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos.replace(CursorPosition::BeforeFirst);
                })?;
        }
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    fn seek_to_last(&mut self, always_seek: bool) -> Result<IOResult<()>> {

    }
}
