
use crate::{
    io_yield_one, return_if_io,
    storage::{
        pager::Pager,
        sqlite3_ondisk::{BTreeCell, IndexInteriorCell, IndexLeafCell, PageType, TableLeafCell},
        state_machines::{MoveToRightState, SeekToLastState},
    },
    types::{compare_immutable, ImmutableRecord, IOResult, SeekKey, SeekOp, SeekResult},
    Completion, Result,
};
use super::pager::PageRef;
use std::{
    any::Any,
    sync::atomic::Ordering,
    cell::Cell,
    fmt::Debug,
    sync::Arc,
};

pub const BTCURSOR_MAX_DEPTH: usize = 20;

/// 페이지 헤더에 있는 값들의 오프셋
pub mod offset {
    // B-Tree 페이지의 유형
    pub const BTREE_PAGE_TYPE: usize = 0;
    // 페이지 내 첫번째 여유 공간 블록의 오프셋
    pub const BTREE_FIRST_FREEBLOCK: usize = 1;
    // 페이지에 저장된 셀의 개수
    pub const BTREE_CELL_COUNT: usize = 3;
    // 셀 내용이 시작되는 페이지 내의 오프셋
    // 셀을 끝부분부터 채우기 때문에 값은 점점 줄어든다.
    pub const BTREE_CELL_CONTENT_AREA: usize = 5;
    // 셀 내용내에 1,2, 또는 3바이트 크기로 단편화된 사용되지 않는 그룹의 개수
    // 작아서 freeblock으로 관리되지 않음
    pub const BTREE_FRAGMENTED_BYTES_COUNT: usize = 7;
    // 내부 페이지에만 존재. 현재 페이지의 모든 셀에 있는 키보다 큰 키를 가진 자식 페이지의 페이지 번호를 가리키는 포인터
    pub const BTREE_RIGHTMOST_PTR: usize = 8;
}

pub fn btree_init_page(page: &PageRef, page_type: PageType, offset: usize, usable_space: usize) {
    let contents = page.get_contents();
    tracing::debug!(
        "btree_init_page(id={}, offset={}, usable_space={})",
        page.get().id,
        offset,
        usable_space
    );

    contents.offset = offset;
    let id = page_type as u8;
    contents.write_page_type(id);
    contents.write_first_freeblock(0);
    contents.write_cell_count(0);

    // 셀을 페이지의 끝에서부터 채우기 때문에 처음 설정 값은 (page_size - reserved_bytes)인 usable_space가 된다.
    contents.write_cell_content_area(usable_space);

    contents.write_fragmented_bytes_count(0);
    contents.write_rightmost_ptr(0);

    {
        // we might get already used page from the pool. generally this is not a problem because
        // b tree access is very controlled. However, for encrypted pages (and also checksums) we want
        // to ensure that there are no reserved bytes that contain old data.
        let buffer_len = contents.buffer.len();
        assert!(
            usable_space <= buffer_len,
            "usable_space must be <= buffer_len"
        );
        // this is no op if usable_space == buffer_len
        contents.as_ptr()[usable_space..buffer_len].fill(0);
    }
}

#[derive(Clone, Debug)]
pub enum BTreeKey<'a> {
    TableRowId((i64, Option<&'a ImmutableRecord>)),
    // index로 지정된 column들의 값과 원본 테이블의 rowid를 갖는다.
    // (index column 1, index column2, ... , rowid)
    IndexKey(&'a ImmutableRecord),
}

impl BTreeKey<'_> {
    pub fn new_table_rowid(rowid: i64, record: Option<&ImmutableRecord>) -> BTreeKey<'_> {
        BTreeKey::TableRowId((rowid, record))
    }
    
    pub fn get_record(&self) -> Option<&'_ ImmutableRecord> {
        match self {
            BTreeKey::TableRowId((_, record)) => *record,
            BTreeKey::IndexKey(record) => Some(record),
        }
    }

    pub fn maybe_rowid(&self) -> Option<i64> {
        match self {
            BTreeKey::TableRowId((rowid, _)) => Some(*rowid),
            BTreeKey::IndexKey(_) => None,
        }
    }
}

/// Any 는 런타임에 타입을 식별하거나 다운캐스팅할 수 있도록 해주는 트레잇
pub trait CursorTrait: Any {
    fn rowid(&self) -> Result<IOResult<Option<i64>>>;
    fn last(&mut self) -> Result<IOResult<()>>;
    /// Get the record of the entry the cursor is poiting to if any
    fn record(&mut self) -> Result<IOResult<Option<&ImmutableRecord>>>;
    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>>;
    fn seek_to_last(&mut self, always_seek: bool) -> Result<IOResult<()>>;
    /// Move the cursor based on the key and the type of operation (op).
    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>>;
    fn set_null_flag(&mut self, flag: bool);
    fn get_null_flag(&self) -> bool;

    //
    fn invalidate_record(&mut self);
}

/// We store the cell index and cell count for each page in the stack.
/// The reason we store the cell count is because we need to know when we are at the end of the page,
/// without having to perform IO to get the ancestor pages.
#[derive(Clone, Copy, Debug, Default)]
struct BTreeNodeState {
    // 현재 페이지에서 커서가 가리키는 셀의 인덱스
    cell_idx: i32,
    // 페이지에 있는 셀의 수
    cell_count: Option<i32>,
}

impl BTreeNodeState {
    /// Check if the current cell index is at the end of the page.
    /// This information is used to determine whether a child page should move up to its parent.
    /// If the child page is the rightmost leaf page and it has reached the end, this means all of its ancestors have
    /// already reached the end, so it should not go up because there are no more records to traverse.
    /// cell_idx가 페이지의 끝에 도달했는지 여부를 확인
    fn is_at_end(&self) -> bool {
        let cell_count = self.cell_count.expect("cell_count is not set");
        // cell_idx == cell_count means: we will traverse to the rightmost pointer next.
        // cell_idx == cell_count + 1 means: we have already gone down to the rightmost pointer.
        self.cell_idx == cell_count + 1
    }
}

pub struct BTreeCursor {
    pub pager: Arc<Pager>,
    root_page: i64,
    // 커서의 물리적/논리적 위치
    state: CursorState,
    // 현재 위치의 데이터가 유효한지 여부
    pub valid_state: CursorValidState,
    // 실제 데이터를 가리키고 있는지 여부
    pub has_record: bool,
    // 페이지의 usable space에 대한 캐시.
    // page를 액세스해서 usable space를 가져오는 것은 비용이 비싸므로 캐시해 놓고 사용한다.
    usable_space_cached: usize,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: Option<ImmutableRecord>,
    /// Page stack used to traverse the btree.
    /// Each cursor has a stack because each cursor traverses the btree independently.
    stack: PageStack,
    /// State machine for [BTreeCursor::seek_to_last]
    seek_to_last_state: SeekToLastState,
    /// Whether the next call to [BTreeCursor::next()] should be a no-op.
    /// This is currently only used after a delete operation causes a rebalancing.
    /// Advancing is only skipped if the cursor is currently pointing to a valid record
    /// when next() is called.
    pub skip_advance: bool,
    seek_state: CursorSeekState,
    // 현재 커서가 유효한 레코드를 가리키고 있지 않음을 나타내는 플래그
    // 예: LEFT JOIN시 왼쪽 테이블의 행에 대해 오른쪽 테이블에서 일치하는 행을 찾지 못하는 경우 오늘쪽 테이블의 모든 열이 NULL 값으로 채워져야 한다.
    null_flag: bool,
    /// Index internal pages are consumed on the way up, so we store going upwards flag in case
    /// we just moved to a parent page and the parent page is an internal index page which requires
    /// to be consumed.
    /// 인덱스 B-트리에서 내부(Interior) 페이지의 분할 셀(divider cell)은 실제 키(payload)를 포함하므로 탐색중에 소비되어야 한다.
    /// going_upwards가 true인 경우 해당 페이지의 분할 셀을 방문한 다음 going_upwards를 false로 재설정한다.
    going_upwards: bool,
    /// State machine for [BTreeCursor::move_to_rightmost] and, optionally, the id of the rightmost page in the btree.
    /// If we know the rightmost page id and are already on that page, we can skip a seek.
    move_to_right_state: (MoveToRightState, Option<usize>),
}

impl BTreeCursor {
    pub fn new(pager: Arc<Pager>, root_page: i64, _num_columns: usize) -> Self {
        let valid_state = if root_page == 1 && !pager.db_initialized() {
            CursorValidState::Invalid
        } else {
            CursorValidState::Valid
        };
        let usable_space = pager.usable_space();
        Self {
            pager,
            root_page,
            state: CursorState::None,
            valid_state,
            has_record: false,
            usable_space_cached: usable_space,
            reusable_immutable_record: None,
            stack: PageStack {
                current_page: -1,
                node_states: [BTreeNodeState::default(); BTCURSOR_MAX_DEPTH + 1],
                stack: [const { None }; BTCURSOR_MAX_DEPTH + 1],
            },
            seek_to_last_state: SeekToLastState::Start,
            skip_advance: false,
            seek_state: CursorSeekState::Start,
            null_flag: false,
            going_upwards: false,
            move_to_right_state: (MoveToRightState::Start, None),
        }
    }

    pub fn new_table(pager: Arc<Pager>, root_page: i64, num_columns: usize) -> Self {
        Self::new(pager, root_page, num_columns)
    }

    fn insert_into_page(&mut self, bkey: &BTreeKey) -> Result<IOResult<()>> {
        let record = bkey
            .get_record()
            .expect("expected record present on insert");

        let record_values = record.get_values();
        if let CursorState::None = &self.state {
            self.state = CursorState::Write(WriteState::Start);
        }
        let usable_space = self.usable_space();
        let ret = loop {
            let CursorState::Write(write_state) = &mut self.state else {
                panic!("expected write state");
            };
            match write_state {
                WriteState::Start => {
                    let page = self.stack.top();

                    // get page and find cell
                    let cell_idx = {
                        self.pager.add_dirty(&page)?;
                        self.stack.current_cell_index()
                    };
                    if cell_idx == -1 {
                        // This might be a brand new table and the cursor hasn't moved yet. Let's advance it to the first slot.
                        self.stack.set_cell_index(0);
                    }
                    let cell_idx = self.stack.current_cell_index() as usize;
                    tracing::debug!(cell_idx);

                    // if the cell index is less than the total cells, check: if its an existing
                    // rowid, we are going to update / overwrite the cell
                    if cell_idx < page.get_contents().cell_count() {
                        let cell = page.get_contents().cell_get(cell_idx, usable_space)?;
                        match cell {
                            BTreeCell::TableLeafCell(tbl_leaf) => {
                                if tbl_leaf.rowid == bkey.to_rowid() {
                                    tracing::debug!("TableLeafCell: found exact match with cell_idx={cell_idx}, overwriting");
                                    self.has_record = true;
                                    *write_state = WriteState::Overwrite {
                                        page,
                                        cell_idx,
                                        state: Some(OverwriteCellState::AllocatePayload),
                                    };
                                    continue;
                                }
                            }
                            BTreeCell::IndexLeafCell(..) | BTreeCell::IndexInteriorCell(..) => {
                                return_if_io!(self.record());
                                let cmp = compare_immutable(
                                    record_values.as_slice(),
                                    self.get_immutable_record()
                                        .as_ref()
                                        .unwrap()
                                        .get_values().as_slice(),
                                        &self.index_info.as_ref().unwrap().key_info,
                                );
                                if cmp == Ordering::Equal {
                                    tracing::debug!("IndexLeafCell: found exact match with cell_idx={cell_idx}, overwriting");
                                    self.set_has_record(true);
                                    let CursorState::Write(write_state) = &mut self.state else {
                                        panic!("expected write state");
                                    };
                                    *write_state = WriteState::Overwrite {
                                        page,
                                        cell_idx,
                                        state: Some(OverwriteCellState::AllocatePayload),
                                    };
                                    continue;
                                } else {
                                    turso_assert!(
                                        !matches!(cell, BTreeCell::IndexInteriorCell(..)),
                                         "we should not be inserting a new index interior cell. the only valid operation on an index interior cell is an overwrite!"
                                    );
                                }
                            }
                            other => panic!("unexpected cell type, expected TableLeaf or IndexLeaf, found: {other:?}"),
                        }
                    }

                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    *write_state = WriteState::Insert {
                        page,
                        cell_idx,
                        new_payload: Vec::with_capacity(record_values.len() + 4),
                        fill_cell_payload_state: FillCellPayloadState::Start,
                    };
                    continue;
                }
                WriteState::Insert {
                    page,
                    cell_idx,
                    new_payload,
                    ref mut fill_cell_payload_state,
                } => {
                    return_if_io!(fill_cell_payload(
                        &PinGuard::new(page.clone()),
                        bkey.maybe_rowid(),
                        new_payload,
                        *cell_idx,
                        record,
                        usable_space,
                        self.pager.clone(),
                        fill_cell_payload_state,
                    ));

                    {
                        let contents = page.get_contents();
                        tracing::debug!(name: "overflow", cell_count = contents.cell_count());

                        insert_into_cell(
                            contents,
                            new_payload.as_slice(),
                            *cell_idx,
                            usable_space,
                        )?;
                    };
                    self.stack.set_cell_index(*cell_idx as i32);
                    let overflows = !page.get_contents().overflow_cells.is_empty();
                    if overflows {
                        *write_state = WriteState::Balancing;
                        assert!(matches!(self.balance_state.sub_state, BalanceSubState::Start), "There should be no balancing operation in progress when insert state is {:?}, got: {:?}", self.state, self.balance_state.sub_state);
                        // If we balance, we must save the cursor position and seek to it later.
                        self.save_context(CursorContext::seek_eq_only(bkey));
                    } else {
                        *write_state = WriteState::Finish;
                    }
                    continue;
                }
                WriteState::Overwrite {
                    page,
                    cell_idx,
                    ref mut state,
                } => {
                    assert!(page.is_loaded(), "page {}is not loaded", page.get().id);

                    let page = page.clone();

                    // Currently it's necessary to .take() here to prevent double-borrow of `self` in `overwrite_cell`.
                    // We insert the state back if overwriting returns IO.
                    let mut state = state.take().expect("state should be present");
                    let cell_idx = *cell_idx;
                    if let IOResult::IO(io) =
                        self.overwrite_cell(&page, cell_idx, record, &mut state)?
                    {
                        let CursorState::Write(write_state) = &mut self.state else {
                            panic!("expected write state");
                        };
                        *write_state = WriteState::Overwrite {
                            page,
                            cell_idx,
                            state: Some(state),
                        };
                        return Ok(IOResult::IO(io));
                    }
                    let overflows = !page.get_contents().overflow_cells.is_empty();
                    let underflows = !overflows && {
                        let free_space = compute_free_space(page.get_contents(), usable_space);
                        free_space * 3 > usable_space * 2
                    };
                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    if overflows || underflows {
                        *write_state = WriteState::Balancing;
                        assert!(matches!(self.balance_state.sub_state, BalanceSubState::Start), "There should be no balancing operation in progress when overwrite state is {:?}, got: {:?}", self.state, self.balance_state.sub_state);
                        // If we balance, we must save the cursor position and seek to it later.
                        self.save_context(CursorContext::seek_eq_only(bkey));
                    } else {
                        *write_state = WriteState::Finish;
                    }
                    continue;
                }
                WriteState::Balancing => {
                    return_if_io!(self.balance(None));
                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    *write_state = WriteState::Finish;
                }
                WriteState::Finish => {
                    break Ok(IOResult::Done(()));
                }
            };
        };
        if matches!(self.state, CursorState::Write(WriteState::Finish)) {
            // if there was a balance triggered, the cursor position is invalid.
            // it's probably not the greatest idea in the world to do this eagerly here,
            // but at least it works.
            return_if_io!(self.restore_context());
        }
        self.state = CursorState::None;
        ret
    }

    pub fn is_write_in_progress(&self) -> bool {
        matches!(self.state, CursorState::Write(_))
    }

    #[inline(always)]
    fn usable_space(&self) -> usize {
        self.usable_space_cached
    }

    fn get_immutable_record_or_create(&mut self) -> Option<&mut ImmutableRecord> {
        let reusable_immutable_record = &mut self.reusable_immutable_record;
        if reusable_immutable_record.is_none() {
            let page_size = self.pager.get_page_size_unchecked().get();
            let record = ImmutableRecord::new(page_size as usize);
            reusable_immutable_record.replace(record);
        }
        reusable_immutable_record.as_mut()
    }

    fn get_immutable_record(&self) -> Option<&ImmutableRecord> {
        self.reusable_immutable_record.as_ref()
    }

    #[inline]
    fn has_record(&self) -> bool {
        self.has_record
    }

    #[inline]
    fn set_has_record(&mut self, has_record: bool) {
        self.has_record = has_record
    }

    fn move_to_root(&mut self) -> Result<Option<Completion>> {
        self.seek_state = CursorSeekState::Start;
        self.going_upwards = false;
        tracing::trace!(root_page = self.root_page);
        let (mem_page, c) = self.read_page(self.root_page)?;
        self.stack.clear();
        self.stack.push(mem_page);
        Ok(c)
    }

    /// Move the cursor to the rightmost record in the btree.
    fn move_to_rightmost(&mut self, always_seek: bool) -> Result<IOResult<bool>> {
        loop {
            let (move_to_right_state, rightmost_page_id) = &self.move_to_right_state;
            match *move_to_right_state {
                MoveToRightState::Start => {
                    if !always_seek {
                        if let Some(rightmost_page_id) = rightmost_page_id {
                            // If we know the rightmost page and are already on it, we can skip a seek.
                            // This optimization is never performed if always_seek = true. always_seek is used
                            // in cases where we cannot be sure that the btree wasn't modified from under us
                            // e.g. by a trigger subprogram.
                            let current_page = self.stack.top_ref();
                            if current_page.get().id == *rightmost_page_id {
                                let contents = current_page.get_contents();
                                let cell_count = contents.cell_count();
                                self.stack.set_cell_index(cell_count as i32 - 1);
                                return Ok(IOResult::Done(cell_count > 0));
                            }
                        }
                    }
                    let rightmost_page_id = *rightmost_page_id;
                    let c = self.move_to_root()?;
                    self.move_to_right_state = (MoveToRightState::ProcessPage, rightmost_page_id);
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                MoveToRightState::ProcessPage => {
                    let mem_page = self.stack.top_ref();
                    let page_idx = mem_page.get().id;
                    let contents = mem_page.get_contents();
                    if contents.is_leaf() {
                        self.move_to_right_state = (MoveToRightState::Start, Some(page_idx));
                        if contents.cell_count() > 0 {
                            self.stack.set_cell_index(contents.cell_count() as i32 - 1);
                            return Ok(IOResult::Done(true));
                        }
                        return Ok(IOResult::Done(false));
                    }

                    match contents.rightmost_pointer() {
                        Some(right_most_pointer) => {
                            self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                            let (mem_page, c) = self.read_page(right_most_pointer as i64)?;
                            self.stack.push(mem_page);
                            if let Some(c) = c {
                                io_yield_one!(c);
                            }
                        }
                        None => {
                            unreachable!("interior page should have a rightmost pointer");
                        }
                    }
                }
            }
        }
    }

    pub fn read_page(&self, page_idx: i64) -> Result<(PageRef, Option<Completion>)> {
        btree_read_page(&self.pager, page_idx)
    }
}

impl CursorTrait for BTreeCursor {
    fn rowid(&self) -> Result<IOResult<Option<i64>>> {
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        if self.has_record() {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let page_type = contents.page_type();
            if page_type.is_table() {
                let cell_idx = self.stack.current_cell_index();
                let rowid = contents.cell_table_leaf_read_rowid(cell_idx as usize)?;
                Ok(IOResult::Done(Some(rowid)))
            } else {
                let _ = return_if_io!(self.record());
                Ok(IOResult::Done(self.get_index_rowid_from_record()))
            }
        } else {
            Ok(IOResult::Done(None))
        }
    }
    
    fn last(&mut self) -> Result<IOResult<()>> {
        let always_seek = false;
        let cursor_has_record = return_if_io!(self.move_to_rightmost(always_seek));
        self.set_has_record(cursor_has_record);
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    fn record(&mut self) -> Result<IOResult<Option<&ImmutableRecord>>> {
        if !self.has_record() {
            return Ok(IOResult::Done(None));
        }
        let invalidated = self
            .reusable_immutable_record
            .as_ref()
            .is_none_or(|record| record.is_invalidated());
        if !invalidated {
            return Ok(IOResult::Done(self.reusable_immutable_record.as_ref()));
        }

        let page = self.stack.top_ref();
        let contents = page.get_contents();
        let cell_idx = self.stack.current_cell_index();
        let cell = contents.cell_get(cell_idx as usize, self.usable_space())?;
        let (payload, payload_size, first_overflow_page) = match cell {
            BTreeCell::TableLeafCell(TableLeafCell {
                payload,
                payload_size,
                first_overflow_page,
                ..
            }) => (payload, payload_size, first_overflow_page),
            BTreeCell::IndexInteriorCell(IndexInteriorCell {
                payload,
                payload_size,
                first_overflow_page,
                ..
            }) => (payload, payload_size, first_overflow_page),
            BTreeCell::IndexLeafCell(IndexLeafCell {
                payload,
                first_overflow_page,
                payload_size,
            }) => (payload, payload_size, first_overflow_page),
            _ => unreachable!("unexpected page_type"),
        };
        if let Some(next_page) = first_overflow_page {
            return_if_io!(self.process_overflow_read(payload, next_page, payload_size))
        } else {
            self.get_immutable_record_or_create()
                .as_mut()
                .unwrap()
                .invalidate();
            self.get_immutable_record_or_create()
                .as_mut()
                .unwrap()
                .start_serialization(payload);
        };

        Ok(IOResult::Done(self.reusable_immutable_record.as_ref()))
    }

    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>> {
        // ?는 Debug 출력을 하라는 의미
        tracing::debug!(
            valid_state = ?self.valid_state,
            cursor_state = ?self.state,
            is_write_in_progress = self.is_write_in_progress()
        );

        return_if_io!(self.insert_into_page(key));
        if key.maybe_rowid().is_some() {
            // 삽입된 키가 TableRowId라면 실제 테이블 레코드가 삽입되었다는 의미
            self.set_has_record(true);
        }
        Ok(IOResult::Done(()))
    }

    fn seek_to_last(&mut self, always_seek: bool) -> Result<IOResult<()>> {
        loop {
            match self.seek_to_last_state {
                SeekToLastState::Start => {
                    let has_record = return_if_io!(self.move_to_rightmost(always_seek));
                    self.invalidate_record();
                    self.set_has_record(has_record);
                    if !has_record {
                        self.seek_to_last_state = SeekToLastState::IsEmpty;
                        continue;
                    }
                    return Ok(IOResult::Done(()));
                }
                SeekToLastState::IsEmpty => {
                    let is_empty = return_if_io!(self.is_empty_table());
                    assert!(is_empty);
                    self.seek_to_last_state = SeekToLastState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        self.skip_advance = false;
        // Empty trace to capture the span information
        tracing::trace!("");
        // We need to clear the null flag for the table cursor before seeking,
        // because it might have been set to false by an unmatched left-join row during the previous iteration
        // on the outer loop.
        self.set_null_flag(false);
        let seek_result = return_if_io!(self.do_seek(key, op));
        self.invalidate_record();
        // Reset seek state
        self.seek_state = CursorSeekState::Start;
        self.valid_state = CursorValidState::Valid;
        Ok(IOResult::Done(seek_result))
    }

    #[inline(always)]
    /// In outer joins, whenever the right-side table has no matching row, the query must still return a row
    /// for each left-side row. In order to achieve this, we set the null flag on the right-side table cursor
    /// so that it returns NULL for all columns until cleared.
    fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    #[inline(always)]
    fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    #[inline]
    fn invalidate_record(&mut self) {
        self.get_immutable_record_or_create()
            .as_mut()
            .unwrap()
            .invalidate();
    }
}

#[derive(Debug)]
enum WriteState {
    Start,
    Finish,
}

enum CursorState {
    None,
    Write(WriteState),
}

impl Debug for CursorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            //Self::Delete(..) => write!(f, "Delete"),
            //Self::Destroy(..) => write!(f, "Destroy"),
            Self::None => write!(f, "None"),
            //Self::ReadWritePayload(..) => write!(f, "ReadWritePayload"),
            Self::Write(..) => write!(f, "Write"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CursorValidState {
    Invalid,
    Valid,
}

/// Stack of pages representing the tree traversal order.
/// current_page represents the current page being used in the tree and current_page - 1 would be
/// the parent. Using current_page + 1 or higher is undefined behaviour.
struct PageStack {
    /// Pointer to the current page being consumed
    current_page: i32,
    /// List of pages in the stack. Root page will be in index 0
    pub stack: [Option<PageRef>; BTCURSOR_MAX_DEPTH + 1],
    /// List of cell indices in the stack.
    /// node_states[current_page] is the current cell index being consumed. Similarly
    /// node_states[current_page-1] is the cell index of the parent of the current page
    /// that we save in case of going back up.
    /// There are two points that need special attention:
    ///  If node_states[current_page] = -1, it indicates that the current iteration has reached the start of the current_page
    ///  If node_states[current_page] = `cell_count`, it means that the current iteration has reached the end of the current_page
    node_states: [BTreeNodeState; BTCURSOR_MAX_DEPTH + 1],
}

impl PageStack {
    fn _push(&mut self, page: PageRef, starting_cell_idx: i32) {
        tracing::trace!(current = self.current_page, new_page_id = page.get().id,);
        'validate: {
            let current = self.current_page;
            if current == -1 {
                break 'validate;
            }
            let current_top = self.stack[current as usize].as_ref();
            if let Some(current_top) = current_top {
                assert!(
                    current_top.get().id != page.get().id,
                    "about to push page {} twice",
                    page.get().id
                );
            }
        }
        self.populate_parent_cell_count();
        self.current_page += 1;
        assert!(self.current_page >= 0);
        let current = self.current_page as usize;
        assert!(
            current < BTCURSOR_MAX_DEPTH,
            "corrupted database, stack is bigger than expected"
        );

        // Pin the page to prevent it from being evicted while on the stack
        page.pin();

        self.stack[current] = Some(page);
        self.node_states[current] = BTreeNodeState {
            cell_idx: starting_cell_idx,
            cell_count: None, // we don't know the cell count yet, so we set it to None. any code pushing a child page onto the stack MUST set the parent page's cell_count.
        };
    }

    /// Populate the parent page's cell count.
    /// This is needed so that we can, from a child page, check of ancestor pages' position relative to its cell index
    /// without having to perform IO to get the ancestor page contents.
    ///
    /// This rests on the assumption that the parent page is already in memory whenever a child is pushed onto the stack.
    /// We currently ensure this by pinning all the pages on [PageStack] to the page cache so that they cannot be evicted.
    fn populate_parent_cell_count(&mut self) {
        let stack_empty = self.current_page == -1;
        if stack_empty {
            return;
        }
        let current = self.current();
        let page = self.stack[current].as_ref().unwrap();
        assert!(
            page.is_pinned(),
            "parent page {} is not pinned",
            page.get().id
        );
        assert!(
            page.is_loaded(),
            "parent page {} is not loaded",
            page.get().id
        );
        let contents = page.get_contents();
        let cell_count = contents.cell_count() as i32;
        self.node_states[current].cell_count = Some(cell_count);
    }

    fn push(&mut self, page: PageRef) {
        self._push(page, -1);
    }

    /// Get the top page on the stack.
    /// This is the page that is currently being traversed.
    fn top(&self) -> PageRef {
        let current = self.current();
        let page = self.stack[current].clone().unwrap();
        assert!(page.is_loaded(), "page should be loaded");
        page
    }

    fn top_ref(&self) -> &PageRef {
        let current = self.current();
        let page = self.stack[current].as_ref().unwrap();
        assert!(page.is_loaded(), "page should be loaded");
        page
    }

    /// Current page pointer being used
    #[inline(always)]
    fn current(&self) -> usize {
        assert!(self.current_page >= 0);
        self.current_page as usize
    }

    /// Cell index of the current page
    fn current_cell_index(&self) -> i32 {
        let current = self.current();
        self.node_states[current].cell_idx
    }

    fn clear(&mut self) {
        self.unpin_all_if_pinned();

        self.current_page = -1;
    }

    fn unpin_all_if_pinned(&mut self) {
        self.stack.iter_mut().flatten().for_each(|page| {
            let _ = page.try_unpin();
        });
    }

    fn set_cell_index(&mut self, idx: i32) {
        let current = self.current();
        self.node_states[current].cell_idx = idx;
    }

    fn has_parent(&self) -> bool {
        self.current_page > 0
    }
}

#[derive(Debug)]
/// State used for seeking
pub enum CursorSeekState {
    Start,
    MovingBetweenPages {
        eq_seen: Cell<bool>,
    },
    InteriorPageBinarySearch {
        state: InteriorPageBinarySearchState,
    },
    FoundLeaf {
        eq_seen: Cell<bool>,
    },
    LeafPageBinarySearch {
        state: LeafPageBinarySearchState,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct InteriorPageBinarySearchState {
    min_cell_idx: isize,
    max_cell_idx: isize,
    nearest_matching_cell: Option<usize>,
    eq_seen: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct LeafPageBinarySearchState {
    min_cell_idx: isize,
    max_cell_idx: isize,
    nearest_matching_cell: Option<usize>,
    /// Indicates if we have seen an exact match during the downwards traversal of the btree.
    /// This is only needed in index seeks, in cases where we need to determine whether we call
    /// an additional next()/prev() to fetch a matching record from an interior node. We will not
    /// do that if both are true:
    /// 1. We have not seen an EQ during the traversal
    /// 2. We are looking for an exact match ([SeekOp::GE] or [SeekOp::LE] with eq_only: true)
    eq_seen: bool,
    /// In multiple places, we do a seek that checks for an exact match (SeekOp::EQ) in the tree.
    /// In those cases, we need to know where to land if we don't find an exact match in the leaf page.
    /// For non-eq-only conditions (GT, LT, GE, LE), this is pretty simple:
    /// - If we are looking for GT/GE and don't find a match, we should end up beyond the end of the page (idx=cell count).
    /// - If we are looking for LT/LE and don't find a match, we should end up before the beginning of the page (idx=-1).
    ///
    /// For eq-only conditions (GE { eq_only: true } or LE { eq_only: true }), we need to know where to land if we don't find an exact match.
    /// For GE, we want to land at the first cell that is greater than the seek key.
    /// For LE, we want to land at the last cell that is less than the seek key.
    /// This is because e.g. when we attempt to insert rowid 666, we first check if it exists.
    /// If it doesn't, we want to land in the place where rowid 666 WOULD be inserted.
    target_cell_when_not_found: i32,
}

pub fn btree_read_page(pager: &Arc<Pager>, page_idx: i64) -> Result<(PageRef, Option<Completion>)> {
    pager.read_page(page_idx)
}

/// Returns the maximum payload size (X) that can be stored directly on a b-tree page without spilling to overflow pages.
///
/// For table leaf pages: X = usable_size - 35
/// For index pages: X = ((usable_size - 12) * 64/255) - 23
///
/// The usable size is the total page size less the reserved space at the end of each page.
/// These thresholds are designed to:
/// - Give a minimum fanout of 4 for index b-trees
/// - Ensure enough payload is on the b-tree page that the record header can usually be accessed
///   without consulting an overflow page
pub fn payload_overflow_threshold_max(page_type: PageType, usable_space: usize) -> usize {
    match page_type {
        PageType::IndexInterior | PageType::IndexLeaf => {
            ((usable_space - 12) * 64 / 255) - 23 // Index page formula
        }
        PageType::TableInterior | PageType::TableLeaf => {
            usable_space - 35 // Table leaf page formula
        }
    }
}

/// Returns the minimum payload size (M) that must be stored on the b-tree page before spilling to overflow pages is allowed.
///
/// For all page types: M = ((usable_size - 12) * 32/255) - 23
///
/// When payload size P exceeds max_local():
/// - If K = M + ((P-M) % (usable_size-4)) <= max_local(): store K bytes on page
/// - Otherwise: store M bytes on page
///
/// The remaining bytes are stored on overflow pages in both cases.
pub fn payload_overflow_threshold_min(_page_type: PageType, usable_space: usize) -> usize {
    // Same formula for all page types
    ((usable_space - 12) * 32 / 255) - 23
}
