
use crate::storage::sqlite3_ondisk::{PageType};
use super::pager::PageRef;

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
