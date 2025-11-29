
use bytemuck::{Pod, Zeroable};
use crate::{
    io::{Buffer, Completion},
    storage::{
        btree::offset::{
            BTREE_CELL_CONTENT_AREA, BTREE_CELL_COUNT, BTREE_FIRST_FREEBLOCK,
            BTREE_FRAGMENTED_BYTES_COUNT, BTREE_PAGE_TYPE, BTREE_RIGHTMOST_PTR,
        },
        pager::Pager,
    },
    CompletionError, Result,
};
use pack1::{I32BE, U16BE, U32BE};
use std::sync::Arc;
use super::pager::PageRef;

/// Copy로 인해 항상 복사가 일어난다.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Pod, Zeroable)]
#[repr(transparent)]
pub struct PageSize(U16BE);

/// 2의 거듭제곱만 가능하므로 u32를 u16으로 표현 가능.
/// 65536의 경우는 1로 mapping
impl PageSize {
    pub const MIN: u32 = 512;
    pub const MAX: u32 = 65536;
    pub const DEFAULT: u16 = 4096;

    // 512 ~ 65536 사이의 값만 허용
    // 65536인 경우 내부에 1로 저장
    pub const fn new(size: u32) -> Option<Self> {
        if size < PageSize::MIN || size > PageSize::MAX {
            return None;
        }

        // 페이지 크기는 반드시 2의 거듭제곱이어야 한다.
        // 2의 거듭제곱 수들은 항상 단 하나의 비트만 세트(set)되어 있기 때문에 1이어야 한다.
        if size.count_ones() != 1 {
            return None;
        }

        if size == PageSize::MAX {
            // 헤더 크기가 2 바이트라 65536을 저장할 수 없으므로 1인 경우 65536을 의미하도록 한다.
            return Some(Self(U16BE::new(1)));
        }

        Some(Self(U16BE::new(size as u16)))
    }

    /// 값이 1인 경우에 대한 처리 필요
    pub const fn get(self) -> u32 {
        match self.0.get() {
            1 => Self::MAX,
            v => v as u32,
        }
    }

    /// 내부 값 꺼내기
    pub const fn get_raw(self) -> u16 {
        self.0.get()
    }
}

impl Default for PageSize {
    fn default() -> Self {
        Self(U16BE::new(Self::DEFAULT))
    }
}

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy, Debug)]
#[repr(transparent)]
/// Read/Write file format version.
/// 캐시 크기가 DEFAULT인 경우 내부적으로 0으로 저장한다.
pub struct CacheSize(I32BE);

impl CacheSize {
    // The negative value means that we store the amount of pages a XKiB of memory can hold.
    // We can calculate "real" cache size by diving by page size.
    pub const DEFAULT: i32 = -2000;

    // Minimum number of pages that cache can hold.
    pub const MIN: i64 = 10;

    // SQLite uses this value as threshold for maximum cache size
    pub const MAX_SAFE: i64 = 2147450880;

    pub const fn new(size: i32) -> Self {
        match size {
            Self::DEFAULT => Self(I32BE::new(0)),
            v => Self(I32BE::new(v)),
        }
    }

    pub const fn get(self) -> i32 {
        match self.0.get() {
            0 => Self::DEFAULT,
            v => v,
        }
    }
}

/// FIXME: 내부 비트 표현과의 일관성을 위해 CacheSize::new를 불러야 할 것으로 보인다.
impl Default for CacheSize {
    fn default() -> Self {
        // 원본 코드: Self(I32BE::new(Self::DEFAULT))
        CacheSize::new(Self::DEFAULT)
    }
}

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy)]
#[repr(transparent)]
/// Read/Write file format version.
pub struct Version(u8);

impl Version {
    #![allow(non_upper_case_globals)]
    const Wal: Self = Self(2);
}

impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Wal => f.write_str("Version::Wal"),
            Self(v) => write!(f, "Version::Invalid({v})"),
        }
    }
}

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy)]
#[repr(transparent)]
/// Text encoding.
pub struct TextEncoding(U32BE);

impl TextEncoding {
    #![allow(non_upper_case_globals)]
    pub const Utf8: Self = Self(U32BE::new(1));
    pub const Utf16Le: Self = Self(U32BE::new(2));
    pub const Utf16Be: Self = Self(U32BE::new(3));
}

impl std::fmt::Display for TextEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Utf8 => f.write_str("UTF-8"),
            Self::Utf16Le => f.write_str("UTF-16le"),
            Self::Utf16Be => f.write_str("UTF-16be"),
            Self(v) => write!(f, "TextEncoding::Invalid({})", v.get()),
        }
    }
}

impl std::fmt::Debug for TextEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Utf8 => f.write_str("TextEncoding::Utf8"),
            Self::Utf16Le => f.write_str("TextEncoding::Utf16Le"),
            Self::Utf16Be => f.write_str("TextEncoding::Utf16Be"),
            Self(v) => write!(f, "TextEncoding::Invalid({})", v.get()),
        }
    }
}

impl Default for TextEncoding {
    fn default() -> Self {
        Self::Utf8
    }
}

pub const WAL_HEADER_SIZE: usize = 32;
pub const WAL_FRAME_HEADER_SIZE: usize = 24;
pub const WAL_MAGIC_LE: u32 = 0x377f0682;
pub const WAL_MAGIC_BE: u32 = 0x377f0683;

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)] 
/// WAL header
pub struct WalHeader {
    /// magic number: little endian에서 0x377f0682
    /// 0x377f0682 == 00110111011111110000011010000010
    /// big engian인 경우는 LSB를 1로 설정한다: 0x377f0683
    pub magic: u32,
    /// wAL format version. currently 3007000
    pub file_format: u32,
    /// database page size
    pub page_size: u32,
    pub checkpoint_seq: u32,
    pub salt_1: u32,
    pub salt_2: u32,
    pub checksum_1: u32,
    pub checksum_2:u32,
}

#[derive(Debug)]
pub struct PageContent {
    pub offset: usize,
    pub buffer: Arc<Buffer>,
    pub overflow_cells: Vec<OverflowCell>,
}

impl PageContent {
    pub fn new(offset: usize, buffer: Arc<Buffer>) -> Self {
        Self {
            offset,
            buffer,
            overflow_cells: Vec::new(),
        }
    }

    pub fn write_page_type(&self, value: u8) {
        self.write_u8(BTREE_PAGE_TYPE, value);
    }

    pub fn write_first_freeblock(&self, value: u16) {
        self.write_u16(BTREE_FIRST_FREEBLOCK, value);
    }

    pub fn write_cell_count(&self, value: u16) {
        self.write_u16(BTREE_CELL_COUNT, value);
    }

    pub fn write_cell_content_area(&self, value: usize) {
        debug_assert!(value <= PageSize::MAX as usize);
        let value = value as u16; // deliberate cast to u16 because 0 is interpreted as 65536
        self.write_u16(BTREE_CELL_CONTENT_AREA, value);
    }

    pub fn write_fragmented_bytes_count(&self, value: u8) {
        self.write_u8(BTREE_FRAGMENTED_BYTES_COUNT, value);
    }

    pub fn write_rightmost_ptr(&self, value: u32) {
        self.write_u32(BTREE_RIGHTMOST_PTR, value);
    }

    fn write_u8(&self, pos: usize, value: u8) {
        tracing::trace!("write_u8(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos] = value;
    }

    fn write_u16(&self, pos: usize, value: u16) {
        tracing::trace!("write_u16(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos..self.offset + pos + 2].copy_from_slice(&value.to_be_bytes());
    }

    fn write_u32(&self, pos: usize, value: u32) {
        tracing::trace!("write_u32(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos..self.offset + pos + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn as_ptr(&self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }

    pub fn write_database_header(&self, header: &DatabaseHeader) {
        let buf = self.as_ptr();
        buf[0..DatabaseHeader::SIZE].copy_from_slice(bytemuck::bytes_of(header));
    }
}

#[derive(Clone, Debug)]
pub struct OverflowCell {

}

#[derive(Pod, Zeroable, Clone, Copy, Debug)]
#[repr(C, packed)]
/// Database Header Format
pub struct DatabaseHeader {
    /// b"SQLite format 3\0"
    pub magic: [u8; 16],
    /// Page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    pub page_size: PageSize,
    /// File format write version. 2 for WAL.
    pub write_version: Version,
    /// File format read version. 2 for WAL.
    pub read_version: Version,
    /// Bytes of unused "reserved" space at the end of each page. Usually 0.
    pub reserved_space: u8,
    /// Maximum embedded payload fraction. Must be 64.
    pub max_embed_frac: u8,
    /// Minimum embedded payload fraction. Must be 32.
    pub min_embed_frac: u8,
    /// Leaf payload fraction. Must be 32.
    pub leaf_frac: u8,
    /// File change counter.
    pub change_counter: U32BE,
    /// Size of the database file in pages. The "in-header database size".
    pub database_size: U32BE,
    /// Page number of the first freelist trunk page.
    pub freelist_trunk_page: U32BE,
    /// Total number of freelist pages.
    pub freelist_pages: U32BE,
    /// The schema cookie.
    pub schema_cookie: U32BE,
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    pub schema_format: U32BE,
    /// Default page cache size.
    pub default_page_cache_size: CacheSize,
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    pub vacuum_mode_largest_root_page: U32BE,
    /// Text encoding.
    pub text_encoding: TextEncoding,
    /// The "user version" as read and set by the user_version pragma.
    pub user_version: I32BE,
    /// True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    pub incremental_vacuum_enabled: U32BE,
    /// The "Application ID" set by PRAGMA application_id.
    pub application_id: I32BE,
    /// Reserved for expansion. Must be zero.
    _padding: [u8; 20],
    /// The version-valid-for number.
    pub version_valid_for: U32BE,
    /// SQLITE_VERSION_NUMBER
    pub version_number: U32BE,
}

impl DatabaseHeader {
    pub const PAGE_ID: usize = 1;
    pub const SIZE: usize = size_of::<Self>();
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        Self {
            magic: *b"SQLite format 3\0",
            page_size: Default::default(),
            write_version: Version::Wal,
            read_version: Version::Wal,
            reserved_space: 0,
            max_embed_frac: 64,
            min_embed_frac: 32,
            leaf_frac: 32,
            change_counter: U32BE::new(1),
            database_size: U32BE::new(0),
            freelist_trunk_page: U32BE::new(0),
            freelist_pages: U32BE::new(0),
            schema_cookie: U32BE::new(0),
            schema_format: U32BE::new(4), // latest format, new sqlite3 databases use this format
            default_page_cache_size: Default::default(),
            vacuum_mode_largest_root_page: U32BE::new(0),
            text_encoding: TextEncoding::Utf8,
            user_version: I32BE::new(0),
            incremental_vacuum_enabled: U32BE::new(0),
            application_id: I32BE::new(0),
            _padding: [0; 20],
            version_valid_for: U32BE::new(3047000),
            version_number: U32BE::new(3047000),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PageType {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

/// 페이지의 내용을 파일에 쓰기
pub fn begin_write_btree_page(pager: &Pager, page: &PageRef) -> Result<Completion> {
    tracing::trace!("begin_write_btree_page(page={})", page.get().id);
    let page_source = &pager.db_file;
    let page_finish = page.clone();

    let page_id = page.get().id;
    tracing::trace!("begin_write_btree_page(page_id={})", page_id);
    let buffer = {
        let contents = page.get_contents();
        contents.buffer.clone()
    };

    // 파일에 내용을 다 쓰고 나면 호출되는 콜백함수
    let write_complete = {
        let buf_copy = buffer.clone();
        Box::new(move |res: Result<i32, CompletionError>| {
            let Ok(bytes_written) = res else {
                return;
            };
            tracing::trace!("finish_write_btree_page");
            let buf_copy = buf_copy.clone();
            let buf_len = buf_copy.len();

            page_finish.clear_dirty();
            assert!(
                bytes_written == buf_len as i32,
                "wrote({bytes_written}) != expected({buf_len})"
            );
        })
    };
    let c = Completion::new_write(write_complete);
    let io_ctx = pager.io_ctx.read();
    page_source.write_page(page_id, buffer.clone(), &io_ctx, c)
}
