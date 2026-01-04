
use crate::{
    error::GrainError,
    io::Completion,
    storage::{
        btree::CursorTrait,
        sqlite3_ondisk::read_varint,
    },
    translate::collate::CollationSeq,
    IO, Result,
};
use either::Either;
use grain_parser::ast::SortOrder;
use serde::Deserialize;
use std::{
    borrow::{Borrow, Cow},
    cell::UnsafeCell,
    fmt::Display,
    iter::Peekable,
    ops::Deref,
};

/// SQLite by default uses 2000 as maximum numbers in a row.
const MAX_COLUMN: usize = 2000;

#[derive(Debug)]
#[must_use]
pub enum IOResult<T> {
    Done(T),
    IO(IOCompletions),
}

#[derive(Debug)]
#[must_use]
pub enum IOCompletions {
    Single(Completion),
}

impl IOCompletions {
    /// Wais for the Completions to complete
    pub fn wait<I: ?Sized + IO>(self, io: &I) -> Result<()> {
        match self {
            IOCompletions::Single(c) => io.wait_for_completion(c),
        }
    }
}

#[macro_export]
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
            Err(err) => return Err(err),
        }
    };
}

fn float_to_string<S>(float: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format!("{float}"))
}

fn string_to_float<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match crate::numeric::str_to_f64(s) {
        Some(result) => Ok(result.into()),
        None => Err(serde::de::Error::custom("")),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum TextSubtype {
    Text,
    Json,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Text {
    pub value: Cow<'static, str>,
    pub subtype: TextSubtype,
}

impl Text {
    pub fn new(value: impl Into<Cow<'static, str>>) -> Self {
        Self {
            value: value.into(),
            subtype: TextSubtype::Text,
        }
    }

    pub fn json(value: String) -> Self {
        Self {
            value: value.into(),
            subtype: TextSubtype::Json,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TextRef<'a> {
    pub value: &'a str,
    pub subtype: TextSubtype,
}

impl<'a> TextRef<'a> {
    pub fn new(value: &'a str, subtype: TextSubtype) -> Self {
        Self { value, subtype }
    }

    #[inline]
    pub fn as_str(&self) -> &'a str {
        self.value
    }
}

impl<'a> Borrow<str> for TextRef<'a> {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<'a> Deref for TextRef<'a> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// Value는 데이터의 실체이며 소유권을 가지고 있는 객체이고,
/// ValueRef는 이 실체를 가리키는 포인터와 같은 개념으로 빠르고 효율적인 읽기 전용 접근을 제공
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Value {
    Null,
    Integer(i64),
    #[serde(
        serialize_with = "float_to_string",
        deserialize_with = "string_to_float"
    )]
    Float(f64),
    Text(Text),
    Blob(Vec<u8>),
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let (left, right) = (self.as_value_ref(), other.as_value_ref());
        left.cmp(&right)
    }
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        let (left, right) = (self.as_value_ref(), other.as_value_ref());
        left.eq(&right)
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Value) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Value {
    pub fn as_ref<'a>(&'a self) -> ValueRef<'a> {
        match self {
            Value::Null => ValueRef::Null,
            Value::Integer(v) => ValueRef::Integer(*v),
            Value::Float(v) => ValueRef::Float(*v),
            Value::Text(v) => ValueRef::Text(TextRef {
                value: &v.value,
                subtype: v.subtype,
            }),
            Value::Blob(v) => ValueRef::Blob(v.as_slice()),
        }
    }
}

#[derive(Clone, Copy)]
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(TextRef<'a>),
    Blob(&'a [u8]),
}

impl<'a> PartialEq<ValueRef<'a>> for ValueRef<'a> {
    fn eq(&self, other: &ValueRef<'a>) -> bool {
        match (self, other) {
            (Self::Integer(int_left), Self::Integer(int_right)) => int_left == int_right,
            (Self::Integer(int), Self::Float(float)) | (Self::Float(float), Self::Integer(int)) => {
                sqlite_int_float_compare(*int, *float).is_eq()
            }
            (Self::Float(float_left), Self::Float(float_right)) => float_left == float_right,
            (Self::Integer(_) | Self::Float(_), Self::Text(_) | Self::Blob(_)) => false,
            (Self::Text(_) | Self::Blob(_), Self::Integer(_) | Self::Float(_)) => false,
            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.as_bytes() == text_right.value.as_bytes()
            }
            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.eq(blob_right),
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl<'a> PartialEq<Value> for ValueRef<'a> {
    fn eq(&self, other: &Value) -> bool {
        let other = other.as_value_ref();
        self.eq(&other)
    }
}

impl<'a> Eq for ValueRef<'a> {}

impl<'a> PartialOrd<ValueRef<'a>> for ValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(int_left), Self::Integer(int_right)) => int_left.partial_cmp(int_right),
            (Self::Integer(int_left), Self::Float(float_right)) => {
                (*int_left as f64).partial_cmp(float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                float_left.partial_cmp(&(*int_right as f64))
            }
            (Self::Float(float_left), Self::Float(float_right)) => {
                float_left.partial_cmp(float_right)
            }
            // Numeric vs Text/Blob
            (Self::Integer(_) | Self::Float(_), Self::Text(_) | Self::Blob(_)) => {
                Some(std::cmp::Ordering::Less)
            }
            (Self::Text(_) | Self::Blob(_), Self::Integer(_) | Self::Float(_)) => {
                Some(std::cmp::Ordering::Greater)
            }

            (Self::Text(text_left), Self::Text(text_right)) => text_left
                .value
                .as_bytes()
                .partial_cmp(text_right.value.as_bytes()),
            // Text vs Blob
            (Self::Text(_), Self::Blob(_)) => Some(std::cmp::Ordering::Less),
            (Self::Blob(_), Self::Text(_)) => Some(std::cmp::Ordering::Greater),

            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.partial_cmp(blob_right),
            (Self::Null, Self::Null) => Some(std::cmp::Ordering::Equal),
            (Self::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Self::Null) => Some(std::cmp::Ordering::Greater),
        }
    }
}

impl<'a> Ord for ValueRef<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Display for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Float(fl) => write!(f, "{fl:?}"),
            Self::Text(s) => write!(f, "{}", s.as_str()),
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}

pub trait AsValueRef {
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a>;
}

impl<'b> AsValueRef for ValueRef<'b> {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        *self
    }
}

impl AsValueRef for Value {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        self.as_ref()
    }
}

impl AsValueRef for &mut Value {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        self.as_ref()
    }
}

impl<V1, V2> AsValueRef for Either<V1, V2>
where
    V1: AsValueRef,
    V2: AsValueRef,
{
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        match self {
            Either::Left(left) => left.as_value_ref(),
            Either::Right(right) => right.as_value_ref(),
        }
    }
}

impl<V: AsValueRef> AsValueRef for &V {
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        (*self).as_value_ref()
    }
}

/// 단일 레코드(행)를 직렬화된 바이너리 형식으로 저장
/// [헤더크기][데이터 타입 정보1][데이터 타입 정보2]...[실제 데이터1][실제 데이터2]...
/// 
/// SQL에서 정렬과 같은 작업을 처리하기 위해 Ord와 PartialOrd는 필수적이며, 그 외 다른 trait들도 중요한 역할을 한다.
/// 
/// 특징: 일단 원시 데이터(바이트 덩어리)로 가지고 있되, 꼭 필요할 때, 필요한 만큼만, 복사 없이 보여준다
pub struct ImmutableRecord {
    // 원시 바이트 데이터
    payload: Value,
    // payload를 지연 파싱
    cursor: UnsafeCell<RecordCursor>,
}

unsafe impl Send for ImmutableRecord {}
unsafe impl Sync for ImmutableRecord {}

impl Clone for ImmutableRecord {
    fn clone(&self) -> Self {
        Self {
            payload: self.payload.clone(),
            cursor: UnsafeCell::new(RecordCursor::new()), // Reset cursor state on clone
        }
    }
}

impl PartialEq for ImmutableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.payload == other.payload // Only compare payload, ignore cursor state
    }
}

impl Eq for ImmutableRecord {}

impl PartialOrd for ImmutableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImmutableRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.payload.cmp(&other.payload) // Only compare payload, ignore cursor state
    }
}

impl ImmutableRecord {
    pub fn new(payload_capacity: usize) -> Self {
        Self {
            payload: Value::Blob(Vec::with_capacity(payload_capacity)),
            cursor: UnsafeCell::new(RecordCursor::new()),
        }
    }

    pub fn from_bin_record(payload: Vec<u8>) -> Self {
        Self {
            payload: Value::Blob(payload),
            cursor: UnsafeCell::new(RecordCursor::new()),
        }
    }

    pub fn get_values(&self) -> Vec<ValueRef<'_>> {
        let cursor = self.cursor();
        cursor
            .get_values(self)
            .collect::<Result<Vec<_>>>()
            .unwrap_or_default()
    }

    pub fn get_payload(&self) -> &[u8] {
        self.as_blob()
    }

    pub fn as_blob(&self) -> &Vec<u8> {
        match &self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    #[inline]
    pub fn cursor(&self) -> &mut RecordCursor {
        // SAFETY: See the unsafe impl Send/Sync for ImmutableRecord
        unsafe { &mut *self.cursor.get() }
    }

    pub fn column_count(&self) -> usize {
        let cursor = self.cursor();
        cursor.parse_full_header(self).unwrap();
        cursor.serial_types.len()
    }

    #[inline]
    pub fn invalidate(&mut self) {
        self.as_blob_mut().clear();
        self.cursor().invalidate();
    }

    #[inline]
    pub fn is_invalidated(&self) -> bool {
        self.as_blob().is_empty()
    }

    #[inline]
    pub fn start_serialization(&mut self, payload: &[u8]) {
        self.as_blob_mut().extend_from_slice(payload);
    }

    #[inline]
    pub fn as_blob_mut(&mut self) -> &mut Vec<u8> {
        match &mut self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    pub fn last_value(&self) -> Option<Result<ValueRef<'_>>> {
        if self.is_invalidated() {
            return Some(Err(GrainError::InternalError(
                "Record is invalidated".into(),
            )));
        }
        let cursor = self.cursor();
        cursor.parse_full_header(self).unwrap();
        let last_idx = cursor.serial_types.len().checked_sub(1)?;
        Some(cursor.deserialize_column(self, last_idx))
    }
}

impl std::fmt::Debug for ImmutableRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.payload {
            Value::Blob(bytes) => {
                let preview = if bytes.len() > 20 {
                    format!("{:?} ... ({} bytes total)", &bytes[..20], bytes.len())
                } else {
                    format!("{bytes:?}")
                };
                write!(f, "ImmutableRecord {{ payload: {preview} }}")
            }
            Value::Text(s) => {
                let string = s.as_str();
                let preview = if string.len() > 20 {
                    format!("{:?} ... ({} chars total)", &string[..20], string.len())
                } else {
                    format!("{string:?}")
                };
                write!(f, "ImmutableRecord {{ payload: {preview} }}")
            }
            other => write!(f, "ImmutableRecord {{ payload: {other:?} }}"),
        }
    }
}

/// `[header_size][serial_type1][serial_type2]...[data1][data2]...`
#[derive(Debug, Default)]
pub struct RecordCursor {
    /// Parsed serial type values for each column.
    /// Serial types encode both the data type and size information.
    pub serial_types: Vec<u64>,
    /// Byte offsets where each column's data begins in the record payload.
    /// Always has one more entry than `serial_types` (the final offset marks the end).
    pub offsets: Vec<usize>,
    /// Total size of the record header in bytes.
    pub header_size: usize,
    /// Current parsing position within the header section.
    pub header_offset: usize,
}

impl RecordCursor {
    pub fn new() -> Self {
        Self {
            serial_types: Vec::new(),
            offsets: Vec::new(),
            header_size: 0,
            header_offset: 0,
        }
    }

    pub fn invalidate(&mut self) {
        self.serial_types.clear();
        self.offsets.clear();
        self.header_size = 0;
        self.header_offset = 0;
    }

    pub fn is_invalidated(&self) -> bool {
        self.serial_types.is_empty() && self.offsets.is_empty()
    }

    pub fn parse_full_header(&mut self, record: &ImmutableRecord) -> Result<()> {
        self.ensure_parsed_upto(record, MAX_COLUMN)
    }

    /// 레코드의 전체 데이터를 한 번에 파싱하는 대신, 필요한 열(column)까지만 헤더 정보를 파싱
    #[inline(always)]
    pub fn ensure_parsed_upto(
        &mut self,
        record: &ImmutableRecord,
        target_idx: usize,
    ) -> Result<()> {
        let payload = record.get_payload();
        if payload.is_empty() {
            return Ok(());
        }

        // Parse header size and initialize parsing
        if self.serial_types.is_empty() && self.offsets.is_empty() {
            let (header_size, bytes_read) = read_varint(payload)?;
            self.header_size = header_size as usize;
            self.header_offset = bytes_read;
            self.offsets.push(self.header_size); // First column starts after header
        }

        // Parse serial types incrementally
        while self.serial_types.len() <= target_idx
            && self.header_offset < self.header_size
            && self.header_offset < payload.len()
        {
            let (serial_type, read_bytes) = read_varint(&payload[self.header_offset..])?;
            self.serial_types.push(serial_type);
            self.header_offset += read_bytes;

            let serial_type_obj = SerialType::try_from(serial_type)?;
            let data_size = serial_type_obj.size();
            let prev_offset = *self.offsets.last().unwrap();
            self.offsets.push(prev_offset + data_size);
        }

        Ok(())
    }

    /// 바이너리 페이로드의 특정 부분(idx)을 실제 ValueRef 타입으로 해석
    /// ensure_parsed_upto를 통해 미리 해석이 되어 있어야 한다.
    pub fn deserialize_column<'a>(
        &self,
        record: &'a ImmutableRecord,
        idx: usize,
    ) -> Result<ValueRef<'a>> {
        if idx >= self.serial_types.len() {
            return Ok(ValueRef::Null);
        }

        let serial_type = self.serial_types[idx];
        let serial_type_obj = SerialType::try_from(serial_type)?;

        match serial_type_obj.kind() {
            SerialTypeKind::Null => return Ok(ValueRef::Null),
            SerialTypeKind::ConstInt0 => return Ok(ValueRef::Integer(0)),
            SerialTypeKind::ConstInt1 => return Ok(ValueRef::Integer(1)),
            _ => {} // continue
        }

        if idx + 1 >= self.offsets.len() {
            return Ok(ValueRef::Null);
        }

        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];
        let payload = record.get_payload();

        let slice = &payload[start..end];
        let (value, _) = crate::storage::sqlite3_ondisk::read_value(slice, serial_type_obj)?;
        Ok(value)
    }

    pub fn get_values<'a, 'b>(
        &'b mut self,
        record: &'a ImmutableRecord,
    ) -> Peekable<impl ExactSizeIterator<Item = Result<ValueRef<'a>>> + use<'a, 'b>> {
        struct GetValues<'a, 'b> {
            cursor: &'b mut RecordCursor,
            record: &'a ImmutableRecord,
            idx: usize,
        }

        impl<'a, 'b> Iterator for GetValues<'a, 'b> {
            type Item = Result<ValueRef<'a>>;

            fn next(&mut self) -> Option<Self::Item> {
                if self.idx == 0 {
                    // So that we can have the full length of serial types
                    if let Err(err) = self.cursor.parse_full_header(self.record) {
                        return Some(Err(err));
                    }
                }
                if !self.record.is_invalidated() && self.idx < self.cursor.serial_types.len() {
                    let res = self.cursor.deserialize_column(self.record, self.idx);
                    self.idx += 1;
                    Some(res)
                } else {
                    None
                }
            }
        }

        impl<'a, 'b> ExactSizeIterator for GetValues<'a, 'b> {
            fn len(&self) -> usize {
                self.cursor.serial_types.len() - self.idx
            }
        }

        let get_values = GetValues {
            cursor: self,
            record,
            idx: 0,
        };
        get_values.peekable()
    }
}

fn sqlite_int_float_compare(int_val: i64, float_val: f64) -> std::cmp::Ordering {
    if float_val.is_nan() {
        return std::cmp::Ordering::Greater;
    }

    if float_val < -9223372036854775808.0 {
        return std::cmp::Ordering::Greater;
    }
    if float_val >= 9223372036854775808.0 {
        return std::cmp::Ordering::Less;
    }

    let float_as_int = float_val as i64;
    match int_val.cmp(&float_as_int) {
        std::cmp::Ordering::Equal => {
            let int_as_float = int_val as f64;
            int_as_float
                .partial_cmp(&float_val)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        other => other,
    }
}

pub enum Cursor {
    BTree(Box<dyn CursorTrait>),
}

impl Cursor {
    pub fn new_btree(cursor: Box<dyn CursorTrait>) -> Self {
        Self::BTree(cursor)
    }

    pub fn as_btree_mut(&mut self) -> &mut dyn CursorTrait {
        match self {
            Self::BTree(cursor) => cursor.as_mut(),
            //_ => panic!("Cursor is not a btree"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyInfo {
    pub sort_order: SortOrder,
    pub collation: CollationSeq,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Metadata about an index, used for handling and comparing index keys.
///
/// This struct provides information about the sorting order of columns,
/// whether the index includes a row ID, and the total number of columns
/// in the index.
pub struct IndexInfo {
    // Specifies the sorting order (ascending or descending) for each column in the index.
    pub key_info: Vec<KeyInfo>,
    /// Indicates whether the index includes a row ID column.
    pub has_rowid: bool,
    /// The total number of columns in the index, including the row ID column if present.
    pub num_cols: usize,
}

const I8_LOW: i64 = -128;
const I8_HIGH: i64 = 127;
const I16_LOW: i64 = -32768;
const I16_HIGH: i64 = 32767;
const I24_LOW: i64 = -8388608;
const I24_HIGH: i64 = 8388607;
const I32_LOW: i64 = -2147483648;
const I32_HIGH: i64 = 2147483647;
const I48_LOW: i64 = -140737488355328;
const I48_HIGH: i64 = 140737488355327;

/// Sqlite Serial Types
/// https://www.sqlite.org/fileformat.html#record_format
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct SerialType(u64);

impl TryFrom<u64> for SerialType {
    type Error = GrainError;

    fn try_from(uint: u64) -> Result<Self> {
        if uint == 10 || uint == 11 {
            return Err(GrainError::Corrupt(format!("Invalid serial type: {uint}")));
        }
        Ok(SerialType(uint))
    }
}

impl SerialType {
    pub fn kind(&self) -> SerialTypeKind {
        match self.0 {
            0 => SerialTypeKind::Null,
            1 => SerialTypeKind::I8,
            2 => SerialTypeKind::I16,
            3 => SerialTypeKind::I24,
            4 => SerialTypeKind::I32,
            5 => SerialTypeKind::I48,
            6 => SerialTypeKind::I64,
            7 => SerialTypeKind::F64,
            8 => SerialTypeKind::ConstInt0,
            9 => SerialTypeKind::ConstInt1,
            n if n >= 12 => match n % 2 {
                0 => SerialTypeKind::Blob,
                1 => SerialTypeKind::Text,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    pub fn size(&self) -> usize {
        match self.kind() {
            SerialTypeKind::Null => 0,
            SerialTypeKind::I8 => 1,
            SerialTypeKind::I16 => 2,
            SerialTypeKind::I24 => 3,
            SerialTypeKind::I32 => 4,
            SerialTypeKind::I48 => 6,
            SerialTypeKind::I64 => 8,
            SerialTypeKind::F64 => 8,
            SerialTypeKind::ConstInt0 => 0,
            SerialTypeKind::ConstInt1 => 0,
            SerialTypeKind::Text => (self.0 as usize - 13) / 2,
            SerialTypeKind::Blob => (self.0 as usize - 12) / 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SerialTypeKind {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    ConstInt0,
    ConstInt1,
    Text,
    Blob,
}

pub fn compare_immutable<V1, V2, E1, E2, I1, I2>(
    l: I1,
    r: I2,
    column_info: &[KeyInfo],
) -> std::cmp::Ordering
where
    V1: AsValueRef,
    V2: AsValueRef,
    E1: ExactSizeIterator<Item = V1>,
    E2: ExactSizeIterator<Item = V2>,
    I1: IntoIterator<IntoIter = E1, Item = E1::Item>,
    I2: IntoIterator<IntoIter = E2, Item = E2::Item>,
{
    let (l, r): (E1, E2) = (l.into_iter(), r.into_iter());
    assert!(
        l.len() >= column_info.len(),
        "{} < {}",
        l.len(),
        column_info.len()
    );
    assert!(
        r.len() >= column_info.len(),
        "{} < {}",
        r.len(),
        column_info.len()
    );
    let (l, r) = (l.take(column_info.len()), r.take(column_info.len()));
    for (i, (l, r)) in l.zip(r).enumerate() {
        let column_order = column_info[i].sort_order;
        let collation = column_info[i].collation;
        let cmp = compare_immutable_single(l, r, collation);
        if !cmp.is_eq() {
            return match column_order {
                SortOrder::Asc => cmp,
                SortOrder::Desc => cmp.reverse(),
            };
        }
    }
    std::cmp::Ordering::Equal
}

pub fn compare_immutable_single<V1, V2>(l: V1, r: V2, collation: CollationSeq) -> std::cmp::Ordering
where
    V1: AsValueRef,
    V2: AsValueRef,
{
    let l = l.as_value_ref();
    let r = r.as_value_ref();
    match (l, r) {
        (ValueRef::Text(left), ValueRef::Text(right)) => collation.compare_strings(&left, &right),
        _ => l.partial_cmp(&r).unwrap(),
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SeekResult {
    /// Record matching the [SeekOp] found in the B-tree and cursor was positioned to point onto that record
    Found,
    /// Record matching the [SeekOp] doesn't exists in the B-tree
    NotFound,
    /// This result can happen only if eq_only for [SeekOp] is false
    /// In this case Seek can position cursor to the leaf page boundaries (before the start, after the end)
    /// (e.g. if leaf page holds rows with keys from range [1..10], key 10 is absent and [SeekOp] is >= 10)
    ///
    /// turso-db has this extra [SeekResult] in order to make [BTreeCursor::seek] method to position cursor at
    /// the leaf of potential insertion, but also communicate to caller the fact that current cursor position
    /// doesn't hold a matching entry
    /// (necessary for Seek{XX} VM op-codes, so these op-codes will try to advance cursor in order to move it to matching entry)
    TryAdvance,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
/// The match condition of a table/index seek.
pub enum SeekOp {
    /// If eq_only is true, this means in practice:
    /// We are iterating forwards, but we are really looking for an exact match on the seek key.
    GE {
        eq_only: bool,
    },
    GT,
    /// If eq_only is true, this means in practice:
    /// We are iterating backwards, but we are really looking for an exact match on the seek key.
    LE {
        eq_only: bool,
    },
    LT,
}

#[derive(Clone, PartialEq, Debug)]
pub enum SeekKey<'a> {
    TableRowId(i64),
    IndexKey(&'a ImmutableRecord),
}
