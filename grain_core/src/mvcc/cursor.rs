
use crossbeam_skiplist::map::Entry;
use crate::{
    return_if_io,
    mvcc::{
        clock::LogicalClock,
        database::{MvStore, MVTableId, Row, RowID, RowKey, RowVersion, SortableIndexKey},
    },
    storage::btree::{BTreeKey, CursorTrait, BTreeCursor},
    translate::plan::IterationDirection,
    types::{compare_immutable, IOCompletions, IOResult, ImmutableRecord, IndexInfo, SeekKey, SeekOp, SeekResult},
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

#[derive(Debug, Clone)]
enum ExistsState {
    ExistsBtree,
}

#[derive(Debug, Clone, Copy)]
/// State machine for advancing the btree cursor.
/// Advancing means advancing the btree iterator that could be going either forwards or backwards.
enum AdvanceBtreeState {
    RewindCheckBtreeKey, // Check if first key found is valid
    NextBtree,           // Advance to next key
    NextCheckBtreeKey,   // Check if next key found is valid, if it isn't go back to NextBtree
}

#[derive(Debug, Clone)]
/// Rewind state is used to track the state of the rewind **AND** last operation. Since both seem to do similiar
/// operations we can use the same enum for both.
enum RewindState {
    Advance,
}

#[derive(Debug, Clone)]
enum NextState {
    AdvanceUnitialized,
    CheckNeedsAdvance,
    Advance,
}
#[derive(Debug, Clone)]
enum PrevState {
    AdvanceUnitialized,
    CheckNeedsAdvance,
    Advance,
}

#[derive(Debug, Clone)]
enum SeekBtreeState {
    /// Seeking in btree (MVCC seek already done)
    SeekBtree,
    /// Advance to next key in btree (if we got [SeekResult::TryAdvance], or the current row is shadowed by MVCC)
    AdvanceBTree,
    /// Check if current row is visible (not shadowed by MVCC)
    CheckRow,
}

#[derive(Debug, Clone)]
enum SeekState {
    /// Seeking in btree (MVCC seek already done)
    SeekBtree(SeekBtreeState),
    /// Pick winner and finalize
    PickWinner,
}

#[derive(Debug, Clone)]
enum CountState {
    Rewind,
    NextBtree { count: usize },
    CheckBtreeKey { count: usize },
}
#[derive(Debug, Clone)]
enum MvccLazyCursorState {
    Next(NextState),
    Prev(PrevState),
    Rewind(RewindState),
    Exists(ExistsState),
    Seek(SeekState, IterationDirection),
}

/// We read rows from MVCC index or BTree in a dual-cursor approach.
/// This means we read rows from both cursors and then advance the cursor that was just consumed.
/// With DualCursorPeek we track the "peeked" next value for each cursor in the dual-cursor iteration,
/// so that we always return the correct 'next' value (e.g. if mvcc has 1 and 3 and btree has 2 and 4,
/// we should return 1, 2, 3, 4 in order).
#[derive(Debug, Clone)]
struct DualCursorPeek {
    /// Next row available from MVCC
    mvcc_peek: CursorPeek,
    /// Next row available from btree
    btree_peek: CursorPeek,
}

impl DualCursorPeek {
    /// Returns the next row key and whether the row is from the BTree.
    fn get_next(&self, dir: IterationDirection) -> Option<(RowKey, bool)> {
        tracing::trace!(
            "get_next: mvcc_key: {:?}, btree_key: {:?}",
            self.mvcc_peek.get_row_key(),
            self.btree_peek.get_row_key()
        );
        match (self.mvcc_peek.get_row_key(), self.btree_peek.get_row_key()) {
            (Some(mvcc_key), Some(btree_key)) => {
                if dir == IterationDirection::Forwards {
                    // In forwards iteration we want the smaller of the two keys
                    if mvcc_key <= btree_key {
                        Some((mvcc_key.clone(), false))
                    } else {
                        Some((btree_key.clone(), true))
                    }
                // In backwards iteration we want the larger of the two keys
                } else if mvcc_key >= btree_key {
                    Some((mvcc_key.clone(), false))
                } else {
                    Some((btree_key.clone(), true))
                }
            }
            (Some(mvcc_key), None) => Some((mvcc_key.clone(), false)),
            (None, Some(btree_key)) => Some((btree_key.clone(), true)),
            (None, None) => None,
        }
    }

    /// Returns a new [CursorPosition] based on the next row key
    pub fn cursor_position_from_next(
        &self,
        table_id: MVTableId,
        dir: IterationDirection,
    ) -> CursorPosition {
        match self.get_next(dir) {
            Some((row_key, in_btree)) => CursorPosition::Loaded {
                row_id: RowID {
                    table_id,
                    row_id: row_key,
                },
                in_btree,
            },
            None => match dir {
                IterationDirection::Forwards => CursorPosition::End,
                IterationDirection::Backwards => CursorPosition::BeforeFirst,
            },
        }
    }

    pub fn both_uninitialized(&self) -> bool {
        matches!(self.mvcc_peek, CursorPeek::Uninitialized)
            && matches!(self.btree_peek, CursorPeek::Uninitialized)
    }

    pub fn btree_uninitialized(&self) -> bool {
        matches!(self.btree_peek, CursorPeek::Uninitialized)
    }

    pub fn mvcc_exhausted(&self) -> bool {
        matches!(self.mvcc_peek, CursorPeek::Exhausted)
    }
    pub fn btree_exhausted(&self) -> bool {
        matches!(self.btree_peek, CursorPeek::Exhausted)
    }
}

#[derive(Debug, Clone)]
enum CursorPeek {
    Uninitialized,
    Row(RowKey),
    Exhausted,
}

impl CursorPeek {
    pub fn get_row_key(&self) -> Option<&RowKey> {
        match self {
            CursorPeek::Row(k) => Some(k),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvccCursorType {
    Table,
    Index(Arc<IndexInfo>),
}

pub(crate) type MvccIterator<'l, T> =
    Box<dyn Iterator<Item = Entry<'l, T, RwLock<Vec<RowVersion>>>>>;

pub struct MvccLazyCursor<Clock: LogicalClock> {
    pub db: Arc<MvStore<Clock>>,
    tx_id: u64,
    table_id: MVTableId,
    mv_cursor_type: MvccCursorType,
    btree_cursor: Box<dyn CursorTrait>,
    next_rowid_lock: Arc<RwLock<()>>,
    // 새 rowid를 얻고 있는중인지 확인하는 플래그
    creating_new_rowid: bool,
    current_pos: CursorPosition,
    // 효율성을 위해 현재 위치의 레코드 데이터를 reusable_immutable_record라는 내부 필드에 캐시
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: Option<ImmutableRecord>,
    null_flag: bool,
    state: RefCell<Option<MvccLazyCursorState>>,
    /// Stateful MVCC table iterator if this is a table cursor.
    table_iterator: Option<MvccIterator<'static, RowID>>,
    /// Stateful MVCC index iterator if this is an index cursor.
    index_iterator: Option<MvccIterator<'static, SortableIndexKey>>,
    /// Dual-cursor peek state for proper iteration
    dual_peek: RefCell<DualCursorPeek>,
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
            current_pos: CursorPosition::BeforeFirst,
            reusable_immutable_record: None,
            null_flag: false,
            state: RefCell::new(None),
            table_iterator: None,
            index_iterator: None,
            dual_peek: RefCell::new(DualCursorPeek {
                mvcc_peek: CursorPeek::Uninitialized,
                btree_peek: CursorPeek::Uninitialized,
            }),
        })
    }

    /// Returns the current row as an immutable record.
    pub fn current_row(&mut self) -> Result<IOResult<Option<&crate::types::ImmutableRecord>>> {
        let current_pos = &self.current_pos;
        tracing::trace!("current_row({:?})", current_pos);

        match current_pos {
            CursorPosition::Loaded {
                row_id: _,
                in_btree,
            } => {
                if *in_btree {
                    // btree에 있는 경우
                    self.btree_cursor.record()
                } else {
                    // mvcc에 있는 경우
                    let Some(row) = self.read_mvcc_current_row()? else {
                        return Ok(IOResult::Done(None));
                    };
                    {
                        let mut record = self.get_immutable_record_or_create();
                        let record = record.as_mut().ok_or_else(|| {
                            GrainError::InternalError(
                                "immutable record not initialized".to_string(),
                            )
                        })?;
                        record.invalidate();
                        record.start_serialization(row.payload());
                    }

                    let record_ref = self.reusable_immutable_record.as_ref().ok_or_else(|| {
                        GrainError::InternalError("immutable record not initialized".to_string())
                    })?;
                    Ok(IOResult::Done(Some(record_ref)))
                }
            }
            CursorPosition::BeforeFirst => {
                // Before first is not a valid position, so we return none.
                Ok(IOResult::Done(None))
            }
            CursorPosition::End => Ok(IOResult::Done(None)),
        }
    }

    pub fn read_mvcc_current_row(&self) -> Result<Option<Row>> {
        let row_id = match self.current_pos.clone() {
            CursorPosition::Loaded { row_id, in_btree } if !in_btree => row_id,
            _ => panic!("invalid position to read current mvcc row"),
        };
        let maybe_index_id = match &self.mv_cursor_type {
            MvccCursorType::Index(_) => Some(self.table_id),
            MvccCursorType::Table => None,
        };
        self.db
            .read_from_table_or_index(self.tx_id, row_id, maybe_index_id)
    }

    pub fn close(self) -> Result<()> {
        Ok(())
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

    fn get_immutable_record_or_create(&mut self) -> Option<&mut ImmutableRecord> {
        let reusable_immutable_record = &mut self.reusable_immutable_record;
        if reusable_immutable_record.is_none() {
            let record = ImmutableRecord::new(1024);
            reusable_immutable_record.replace(record);
        }
        reusable_immutable_record.as_mut()
    }

    fn get_current_pos(&self) -> CursorPosition {
        self.current_pos.clone()
    }
}

impl<Clock: LogicalClock + 'static> CursorTrait for MvccLazyCursor<Clock> {
    fn rowid(&self) -> Result<IOResult<Option<i64>>> {
        let rowid = match self.get_current_pos() {
            CursorPosition::Loaded {
                row_id,
                in_btree: _,
            } => match &row_id.row_id {
                RowKey::Int(id) => Some(*id),
                RowKey::Record(sortable_key) => {
                    // For index cursors, the rowid is stored in the last column of the index record
                    let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                        panic!("RowKey::Record requires Index cursor type");
                    };
                    if index_info.has_rowid {
                        match sortable_key.key.last_value() {
                            Some(Ok(crate::types::ValueRef::Integer(rowid))) => Some(rowid),
                            _ => {
                                crate::bail_parse_error!("Failed to parse rowid from index record")
                            }
                        }
                    } else {
                        crate::bail_parse_error!("Indexes without rowid are not supported in MVCC");
                    }
                }
            },
            CursorPosition::BeforeFirst => None,
            CursorPosition::End => None,
        };
        Ok(IOResult::Done(rowid))
    }

    fn last(&mut self) -> Result<IOResult<()>> {
        
    }

    fn record(&mut self) -> Result<IOResult<Option<&crate::types::ImmutableRecord>>> {
        self.current_row()
    }

    /// BTreeKey -> Row
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
        let was_btree_resident = match &self.current_pos {
            CursorPosition::Loaded {
                row_id: current_row_id,
                in_btree,
            } => *in_btree && *current_row_id == row.id,
            _ => false,
        };
        self.current_pos = CursorPosition::Loaded {
            row_id: row.id.clone(),
            in_btree: false,
        };
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
                    self.current_pos = CursorPosition::BeforeFirst;
                })?;
        } else if was_btree_resident {
            // 디스크에만 존재하던 행을 처음으로 수정
            // The row exists in B-tree but not in MvStore - mark it as B-tree resident
            // so that checkpoint knows to write deletes to the B-tree file.
            self.db
                .insert_btree_resident_to_table_or_index(self.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos = CursorPosition::BeforeFirst;
                })?;
        } else {
            // 새로운 행 삽입
            self.db
                .insert_to_table_or_index(self.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos = CursorPosition::BeforeFirst;
                })?;
        }
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    fn seek_to_last(&mut self, _always_seek: bool) -> Result<IOResult<()>> {
        match self.seek(SeekKey::TableRowId(i64::MAX), SeekOp::LE { eq_only: false })? {
            IOResult::Done(_) => Ok(IOResult::Done(())),
            IOResult::IO(iocompletions) => Ok(IOResult::IO(iocompletions)),
        }
    }

    fn seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        loop {
            let state = self.state.borrow().clone();
            match state {
                None => {
                    // Initial state: Reset and do MVCC seek
                    let _ = self.table_iterator.take();
                    let _ = self.index_iterator.take();
                    self.reset_dual_peek();
                    self.invalidate_record();
                    // We need to clear the null flag for the table cursor before seeking,
                    // because it might have been set to false by an unmatched left-join row during the previous iteration
                    // on the outer loop.
                    self.set_null_flag(false);

                    let direction = op.iteration_direction();
                    let inclusive = matches!(op, SeekOp::GE { .. } | SeekOp::LE { .. });

                    match &seek_key {
                        SeekKey::TableRowId(row_id) => {
                            let rowid = RowID {
                                table_id: self.table_id,
                                row_id: RowKey::Int(*row_id),
                            };

                            // Seek in MVCC (synchronous)
                            let mvcc_rowid = self.db.seek_rowid(
                                rowid.clone(),
                                inclusive,
                                direction,
                                self.tx_id,
                                &mut self.table_iterator,
                            );

                            // Set MVCC peek
                            {
                                let mut peek = self.dual_peek.borrow_mut();
                                peek.mvcc_peek = match &mvcc_rowid {
                                    Some(rid) => CursorPeek::Row(rid.row_id.clone()),
                                    None => CursorPeek::Exhausted,
                                };
                            }
                        }
                        SeekKey::IndexKey(index_key) => {
                            let index_info = {
                                let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                                    panic!("SeekKey::IndexKey requires Index cursor type");
                                };
                                Arc::new(IndexInfo {
                                    key_info: index_info.key_info.clone(),
                                    has_rowid: index_info.has_rowid,
                                    num_cols: index_key.column_count(),
                                })
                            };
                            let sortable_key =
                                SortableIndexKey::new_from_record((*index_key).clone(), index_info);

                            // Seek in MVCC (synchronous)
                            let mvcc_rowid = self.db.seek_index(
                                self.table_id,
                                sortable_key.clone(),
                                inclusive,
                                direction,
                                self.tx_id,
                                &mut self.index_iterator,
                            );

                            // Set MVCC peek
                            {
                                let mut peek = self.dual_peek.borrow_mut();
                                peek.mvcc_peek = match &mvcc_rowid {
                                    Some(rid) => CursorPeek::Row(rid.row_id.clone()),
                                    None => CursorPeek::Exhausted,
                                };
                            }
                        }
                    }

                    // Move to btree seek state
                    self.state.replace(Some(MvccLazyCursorState::Seek(
                        SeekState::SeekBtree(SeekBtreeState::SeekBtree),
                        direction,
                    )));
                }
                Some(MvccLazyCursorState::Seek(SeekState::SeekBtree(_), direction)) => {
                    return_if_io!(self.seek_btree_and_set_peek(seek_key.clone(), op));
                    self.state.replace(Some(MvccLazyCursorState::Seek(
                        SeekState::PickWinner,
                        direction,
                    )));
                }
                Some(MvccLazyCursorState::Seek(SeekState::PickWinner, direction)) => {
                    // Pick winner and return result
                    // Now pick the winner based on direction
                    let winner = self.dual_peek.borrow().get_next(direction);

                    // Clear seek state
                    self.state.replace(None);

                    if let Some((winner_key, in_btree)) = winner {
                        self.current_pos = CursorPosition::Loaded {
                            row_id: RowID {
                                table_id: self.table_id,
                                row_id: winner_key.clone(),
                            },
                            in_btree,
                        };

                        if op.eq_only() {
                            // Check if the winner matches the seek key
                            let found = match &seek_key {
                                SeekKey::TableRowId(row_id) => winner_key == RowKey::Int(*row_id),
                                SeekKey::IndexKey(index_key) => {
                                    let RowKey::Record(found_key) = &winner_key else {
                                        panic!("Found rowid is not a record");
                                    };
                                    let MvccCursorType::Index(index_info) = &self.mv_cursor_type
                                    else {
                                        panic!("Index cursor expected");
                                    };
                                    let key_info: Vec<_> = index_info
                                        .key_info
                                        .iter()
                                        .take(index_key.column_count())
                                        .cloned()
                                        .collect();
                                    let cmp = compare_immutable(
                                        index_key.get_values(),
                                        found_key.key.get_values(),
                                        &key_info,
                                    );
                                    cmp.is_eq()
                                }
                            };
                            if found {
                                return Ok(IOResult::Done(SeekResult::Found));
                            } else {
                                return Ok(IOResult::Done(SeekResult::NotFound));
                            }
                        } else {
                            return Ok(IOResult::Done(SeekResult::Found));
                        }
                    } else {
                        // Nothing found in either cursor
                        let forwards = matches!(op, SeekOp::GE { .. } | SeekOp::GT);
                        if forwards {
                            self.current_pos = CursorPosition::End;
                        } else {
                            self.current_pos = CursorPosition::BeforeFirst;
                        }
                        return Ok(IOResult::Done(SeekResult::NotFound));
                    }
                }
                _ => {
                    panic!("Invalid state in seek: {:?}", self.state.borrow());
                }
            }
        }
    }

    fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    /// 캐시된 reusable_immutable_record를 정리
    /// 커서가 다른 위치로 이동할 때 호출하여 잘못된 값을 가리키지 않도록 한다.
    fn invalidate_record(&mut self) {
        self.get_immutable_record_or_create()
            .as_mut()
            .expect("immutable record should be initialized")
            .invalidate();
    }
}
