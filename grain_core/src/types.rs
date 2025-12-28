
use crate::{
    io::Completion,
    storage::btree::CursorTrait,
    IO, Result,
};
use serde::Deserialize;
use std::{
    borrow::{Borrow, Cow},
    fmt::Display,
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

impl AsValueRef for Value {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        self.as_ref()
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

/// 단일 레코드(행)를 직렬화된 바이너리 형식으로 저장
/// [헤더크기][데이터 타입 정보1][데이터 타입 정보2]...[실제 데이터1][실제 데이터2]...
/// 
/// SQL에서 정렬과 같은 작업을 처리하기 위해 Ord와 PartialOrd는 필수적이며, 그 외 다른 trait들도 중요한 역할을 한다.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct ImmutableRecord {
    payload: Value,
}

impl ImmutableRecord {
    pub fn get_values<'a>(&'a self) -> Vec<ValueRef<'a>> {
        let mut cursor = RecordCursor::new();
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

    pub fn column_count(&self) -> usize {
        let mut cursor = RecordCursor::new();
        cursor.parse_full_header(self).unwrap();
        cursor.serial_types.len()
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

}

impl RecordCursor {
    pub fn new() -> Self {
        Self {

        }
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

#[derive(Clone, Debug, Eq, PartialEq)]
/// Metadata about an index, used for handling and comparing index keys.
///
/// This struct provides information about the sorting order of columns,
/// whether the index includes a row ID, and the total number of columns
/// in the index.
pub struct IndexInfo {

}

