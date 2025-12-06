
use crate::{
    return_if_io,
    storage::{
        pager::Pager,
        sqlite3_ondisk::{PageType},
    },
    types::{ImmutableRecord, IOResult},
    Result,
};
use super::pager::PageRef;
use std::{any::Any, cell::Cell, fmt::Debug, sync::Arc};

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
    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>>;
}

pub struct BTreeCursor {
    pub pager: Arc<Pager>,
    root_page: i64,
    // 커서의 물리적/논리적 위치
    state: CursorState,
    // 현재 위치의 데이터가 유효한지 여부
    pub valid_state: CursorValidState,
    // 실제 데이터를 가리키고 있는지 여부
    pub has_record: Cell<bool>,
    // 페이지의 usable space에 대한 캐시.
    // page를 액세스해서 usable space를 가져오는 것은 비용이 비싸므로 캐시해 놓고 사용한다.
    usable_space_cached: usize,
}

impl BTreeCursor {
    pub fn new(pager: Arc<Pager>, root_page: i64, num_columns: usize) -> Self {
        let valid_state = if root_page == 1 && !pager.db_state.get().is_initialized() {
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
            has_record: Cell::new(false),
            usable_space_cached: usable_space,
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
    }

    pub fn is_write_in_progress(&self) -> bool {
        matches!(self.state, CursorState::Write(_))
    }

    #[inline(always)]
    fn usable_space(&self) -> usize {
        self.usable_space_cached
    }
}

impl CursorTrait for BTreeCursor {
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
            self.has_record.replace(true);
        }
        Ok(IOResult::Done(()))
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
