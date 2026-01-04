
use bytemuck::{Pod, Zeroable};
use crate::{
    bail_corrupt_error,
    error::GrainError,
    io::{Buffer, Completion, ReadComplete},
    storage::{
        btree::{
            payload_overflow_threshold_max, payload_overflow_threshold_min,
            offset::{
                BTREE_CELL_CONTENT_AREA, BTREE_CELL_COUNT, BTREE_FIRST_FREEBLOCK,
                BTREE_FRAGMENTED_BYTES_COUNT, BTREE_PAGE_TYPE, BTREE_RIGHTMOST_PTR,
            },
        },
        buffer_pool::BufferPool,
        database::{DatabaseStorage, EncryptionOrChecksum},
        pager::Pager,
    },
    types::{SerialType, SerialTypeKind, TextRef, TextSubtype, ValueRef},
    CompletionError, File, IOContext, Result,
};
use pack1::{I32BE, U16BE, U32BE};
use std::sync::Arc;
use super::pager::PageRef;

/// The minimum size of a cell in bytes.
pub const MINIMUM_CELL_SIZE: usize = 4;

pub const CELL_PTR_SIZE_BYTES: usize = 2;
pub const INTERIOR_PAGE_HEADER_SIZE_BYTES: usize = 12;
pub const LEAF_PAGE_HEADER_SIZE_BYTES: usize = 8;
pub const LEFT_CHILD_PTR_SIZE_BYTES: usize = 4;

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

    pub fn page_type(&self) -> PageType {
        self.read_u8(BTREE_PAGE_TYPE).try_into().unwrap()
    }

    /// Read a u8 from the page content at the given offset, taking account the possible db header on page 1 (self.offset).
    /// Do not make this method public.
    fn read_u8(&self, pos: usize) -> u8 {
        let buf = self.as_ptr();
        buf[self.offset + pos]
    }

    /// Read a u16 from the page content at the given offset, taking account the possible db header on page 1 (self.offset).
    /// Do not make this method public.
    fn read_u16(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_be_bytes([buf[self.offset + pos], buf[self.offset + pos + 1]])
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

    /// The number of cells on the page.
    pub fn cell_count(&self) -> usize {
        self.read_u16(BTREE_CELL_COUNT) as usize
    }

    pub fn cell_get(&self, idx: usize, usable_size: usize) -> Result<BTreeCell> {
        tracing::trace!("cell_get(idx={})", idx);
        let buf = self.as_ptr();

        let ncells = self.cell_count();
        assert!(
            idx < ncells,
            "cell_get: idx out of bounds: idx={idx}, ncells={ncells}"
        );
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        let cell_pointer = self.read_u16(cell_pointer) as usize;

        // SAFETY: this buffer is valid as long as the page is alive. We could store the page in the cell and do some lifetime magic
        // but that is extra memory for no reason at all. Just be careful like in the old times :).
        let static_buf: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(buf) };
        read_btree_cell(static_buf, self, cell_pointer, usable_size)
    }

    /// The size of the page header in bytes.
    /// 8 bytes for leaf pages, 12 bytes for interior pages (due to storing rightmost child pointer)
    pub fn header_size(&self) -> usize {
        let is_interior = self.read_u8(BTREE_PAGE_TYPE) <= PageType::TableInterior as u8;
        (!is_interior as usize) * LEAF_PAGE_HEADER_SIZE_BYTES
            + (is_interior as usize) * INTERIOR_PAGE_HEADER_SIZE_BYTES
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

impl TryFrom<u8> for PageType {
    type Error = GrainError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::IndexInterior),
            5 => Ok(Self::TableInterior),
            10 => Ok(Self::IndexLeaf),
            13 => Ok(Self::TableLeaf),
            _ => Err(GrainError::Corrupt(format!("Invalid page type: {value}"))),
        }
    }
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

pub fn begin_read_page(
    db_file: &dyn DatabaseStorage,
    buffer_pool: Arc<BufferPool>,
    page: PageRef,
    page_idx: usize,
    allow_empty_read: bool,
    io_ctx: &IOContext,
) -> Result<Completion> {
    tracing::trace!("begin_read_btree_page(page_idx = {})", page_idx);
    let buf = buffer_pool.get_page();
    let buf = Arc::new(buf);
    let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
        let Ok((mut buf, bytes_read)) = res else {
            page.clear_locked();
            return;
        };
        let buf_len = buf.len();
        assert!(
            (allow_empty_read && bytes_read == 0) || bytes_read == buf_len as i32,
            "read({bytes_read}) != expected({buf_len})"
        );
        let page = page.clone();
        if bytes_read == 0 {
            buf = Arc::new(Buffer::new_temporary(0));
        }
        finish_read_page(page_idx, buf, page.clone());
    });
    let c = Completion::new_read(buf, complete);
    db_file.read_page(page_idx, io_ctx, c)
}

pub fn finish_read_page(page_idx: usize, buffer_ref: Arc<Buffer>, page: PageRef) {
    tracing::trace!("finish_read_page(page_idx = {page_idx})");
    let pos = if page_idx == DatabaseHeader::PAGE_ID {
        // page 1은 데이터베이스 헤더를 앞에 포함하고 있다.
        DatabaseHeader::SIZE
    } else {
        0
    };
    let inner = PageContent::new(pos, buffer_ref.clone());
    {
        page.get().contents.replace(inner);
        page.clear_locked();
        page.set_loaded();
        // we set the wal tag only when reading page from log, or in allocate_page,
        // we clear it here for safety in case page is being re-loaded.
        page.clear_wal_tag();
    }
}

pub fn begin_read_wal_frame<F: File + ?Sized>(
    io: &F,
    offset: u64,
    buffer_pool: Arc<BufferPool>,
    complete: Box<ReadComplete>,
    page_idx: usize,
    io_ctx: &IOContext,
) -> Result<Completion> {
    tracing::trace!(
        "begin_read_wal_frame(offset={}, page_idx={})",
        offset,
        page_idx
    );
    let buf = buffer_pool.get_page();
    let buf = Arc::new(buf);

    match io_ctx.encryption_or_checksum() {
        EncryptionOrChecksum::Checksum(ctx) => {
            let checksum_ctx = ctx.clone();
            let original_c = complete;
            let verify_complete =
                Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                    let Ok((buf, bytes_read)) = res else {
                        original_c(res);
                        return;
                    };
                    if bytes_read <= 0 {
                        tracing::trace!("Read page {page_idx} with {} bytes", bytes_read);
                        original_c(Ok((buf, bytes_read)));
                        return;
                    }

                    match checksum_ctx.verify_checksum(buf.as_mut_slice(), page_idx) {
                        Ok(_) => {
                            original_c(Ok((buf, bytes_read)));
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to verify checksum for page_id={page_idx}: {e}"
                            );
                            original_c(Err(e))
                        }
                    }
                });
            let c = Completion::new_read(buf, verify_complete);
            io.pread(offset, c)
        }
        EncryptionOrChecksum::None => {
            let c = Completion::new_read(buf, complete);
            io.pread(offset, c)
        }
    }
}

#[inline(always)]
pub fn read_varint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut v: u64 = 0;
    for i in 0..8 {
        match buf.get(i) {
            Some(c) => {
                v = (v << 7) + (c & 0x7f) as u64;
                if (c & 0x80) == 0 {
                    return Ok((v, i + 1));
                }
            }
            None => {
                crate::bail_corrupt_error!("Invalid varint");
            }
        }
    }
    match buf.get(8) {
        Some(&c) => {
            // Values requiring 9 bytes must have non-zero in the top 8 bits (value >= 1<<56).
            // Since the final value is `(v<<8) + c`, the top 8 bits (v >> 48) must not be 0.
            // If those are zero, this should be treated as corrupt.
            // Perf? the comparison + branching happens only in parsing 9-byte varint which is rare.
            if (v >> 48) == 0 {
                bail_corrupt_error!("Invalid varint");
            }
            v = (v << 8) + c as u64;
            Ok((v, 9))
        }
        None => {
            bail_corrupt_error!("Invalid varint");
        }
    }
}

/// Reads a value that might reference the buffer it is reading from. Be sure to store RefValue with the buffer
/// always.
#[inline(always)]
pub fn read_value<'a>(buf: &'a [u8], serial_type: SerialType) -> Result<(ValueRef<'a>, usize)> {
    match serial_type.kind() {
        SerialTypeKind::Null => Ok((ValueRef::Null, 0)),
        SerialTypeKind::I8 => {
            if buf.is_empty() {
                crate::bail_corrupt_error!("Invalid UInt8 value");
            }
            let val = buf[0] as i8;
            Ok((ValueRef::Integer(val as i64), 1))
        }
        SerialTypeKind::I16 => {
            if buf.len() < 2 {
                crate::bail_corrupt_error!("Invalid BEInt16 value");
            }
            Ok((
                ValueRef::Integer(i16::from_be_bytes([buf[0], buf[1]]) as i64),
                2,
            ))
        }
        SerialTypeKind::I24 => {
            if buf.len() < 3 {
                crate::bail_corrupt_error!("Invalid BEInt24 value");
            }
            let sign_extension = if buf[0] <= 127 { 0 } else { 255 };
            Ok((
                ValueRef::Integer(
                    i32::from_be_bytes([sign_extension, buf[0], buf[1], buf[2]]) as i64
                ),
                3,
            ))
        }
        SerialTypeKind::I32 => {
            if buf.len() < 4 {
                crate::bail_corrupt_error!("Invalid BEInt32 value");
            }
            Ok((
                ValueRef::Integer(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64),
                4,
            ))
        }
        SerialTypeKind::I48 => {
            if buf.len() < 6 {
                crate::bail_corrupt_error!("Invalid BEInt48 value");
            }
            let sign_extension = if buf[0] <= 127 { 0 } else { 255 };
            Ok((
                ValueRef::Integer(i64::from_be_bytes([
                    sign_extension,
                    sign_extension,
                    buf[0],
                    buf[1],
                    buf[2],
                    buf[3],
                    buf[4],
                    buf[5],
                ])),
                6,
            ))
        }
        SerialTypeKind::I64 => {
            if buf.len() < 8 {
                crate::bail_corrupt_error!("Invalid BEInt64 value");
            }
            Ok((
                ValueRef::Integer(i64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ])),
                8,
            ))
        }
        SerialTypeKind::F64 => {
            if buf.len() < 8 {
                crate::bail_corrupt_error!("Invalid BEFloat64 value");
            }
            Ok((
                ValueRef::Float(f64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ])),
                8,
            ))
        }
        SerialTypeKind::ConstInt0 => Ok((ValueRef::Integer(0), 0)),
        SerialTypeKind::ConstInt1 => Ok((ValueRef::Integer(1), 0)),
        SerialTypeKind::Blob => {
            let content_size = serial_type.size();
            if buf.len() < content_size {
                crate::bail_corrupt_error!("Invalid Blob value");
            }
            Ok((ValueRef::Blob(&buf[..content_size]), content_size))
        }
        SerialTypeKind::Text => {
            let content_size = serial_type.size();
            if buf.len() < content_size {
                crate::bail_corrupt_error!(
                    "Invalid String value, length {} < expected length {}",
                    buf.len(),
                    content_size
                );
            }

            // SAFETY: SerialTypeKind is Text so this buffer is a valid string
            let val = unsafe { str::from_utf8_unchecked(&buf[..content_size]) };
            Ok((
                ValueRef::Text(TextRef::new(val, TextSubtype::Text)),
                content_size,
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub enum BTreeCell {
    TableInteriorCell(TableInteriorCell),
    TableLeafCell(TableLeafCell),
    IndexInteriorCell(IndexInteriorCell),
    IndexLeafCell(IndexLeafCell),
}

#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    pub left_child_page: u32,
    pub rowid: i64,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub rowid: i64,
    /// Payload of cell, if it overflows it won't include overflowed payload.
    pub payload: &'static [u8],
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    pub left_child_page: u32,
    pub payload: &'static [u8],
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    pub payload: &'static [u8],
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

/// read_btree_cell contructs a BTreeCell which is basically a wrapper around pointer to the payload of a cell.
/// buffer input "page" is static because we want the cell to point to the data in the page in case it has any payload.
pub fn read_btree_cell(
    page: &'static [u8],
    page_content: &PageContent,
    pos: usize,
    usable_size: usize,
) -> Result<BTreeCell> {
    let page_type = page_content.page_type();
    let max_local = payload_overflow_threshold_max(page_type, usable_size);
    let min_local = payload_overflow_threshold_min(page_type, usable_size);
    match page_type {
        PageType::IndexInterior => {
            let mut pos = pos;
            let left_child_page =
                u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize);
            Ok(BTreeCell::IndexInteriorCell(IndexInteriorCell {
                left_child_page,
                payload,
                first_overflow_page,
                payload_size,
            }))
        }
        PageType::TableInterior => {
            let mut pos = pos;
            let left_child_page =
                u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            let (rowid, _) = read_varint(&page[pos..])?;
            Ok(BTreeCell::TableInteriorCell(TableInteriorCell {
                left_child_page,
                rowid: rowid as i64,
            }))
        }
        PageType::IndexLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize);
            Ok(BTreeCell::IndexLeafCell(IndexLeafCell {
                payload,
                first_overflow_page,
                payload_size,
            }))
        }
        PageType::TableLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;
            let (rowid, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize);
            Ok(BTreeCell::TableLeafCell(TableLeafCell {
                rowid: rowid as i64,
                payload,
                first_overflow_page,
                payload_size,
            }))
        }
    }
}

// read_payload takes in the unread bytearray with the payload size
/// and returns the payload on the page, and optionally the first overflow page number.
fn read_payload(unread: &'static [u8], payload_size: usize) -> (&'static [u8], Option<u32>) {
    let cell_len = unread.len();
    // We will let overflow be constructed back if needed or requested.
    if payload_size <= cell_len {
        // fit within 1 page
        (&unread[..payload_size], None)
    } else {
        // overflow
        let first_overflow_page = u32::from_be_bytes([
            unread[cell_len - 4],
            unread[cell_len - 3],
            unread[cell_len - 2],
            unread[cell_len - 1],
        ]);
        (&unread[..cell_len - 4], Some(first_overflow_page))
    }
}

/// Checks if payload will overflow a cell based on the maximum allowed size.
/// It will return the min size that will be stored in that case,
/// including overflow pointer
/// see e.g. https://github.com/sqlite/sqlite/blob/9591d3fe93936533c8c3b0dc4d025ac999539e11/src/dbstat.c#L371
pub fn payload_overflows(
    payload_size: usize,
    payload_overflow_threshold_max: usize,
    payload_overflow_threshold_min: usize,
    usable_size: usize,
) -> (bool, usize) {
    if payload_size <= payload_overflow_threshold_max {
        return (false, 0);
    }

    let mut space_left = payload_overflow_threshold_min
        + (payload_size - payload_overflow_threshold_min) % (usable_size - 4);
    if space_left > payload_overflow_threshold_max {
        space_left = payload_overflow_threshold_min;
    }
    (true, space_left + 4)
}
