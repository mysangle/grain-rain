
use crate::translate::collate::CollationSeq;
use grain_parser::ast::{Expr, RefAct, SortOrder};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::Arc,
};

const SCHEMA_TABLE_NAME: &str = "sqlite_schema";

/// 데이터베이스의 루트 페이지(1)에서 스키마 정보를 읽어서 내부에 저장
/// 스키마 정보 업데이트(예: 'CREATE TABLE ...')가 필요한 SQL이 실행되면 업데이트 된다.
/// 
/// sqlite_schema:
///   type, name, tbl_name, rootpage, sql
#[derive(Debug)]
pub struct Schema {
    pub tables: HashMap<String, Arc<Table>>,
    pub indexes: HashMap<String, VecDeque<Arc<Index>>>,
    pub schema_version: u32,
}

impl Schema {
    pub fn new() -> Self {
        let mut tables = HashMap::new();
        let indexes: HashMap<String, VecDeque<Arc<Index>>> = HashMap::new();

        // SCHEMA_TABLE_NAME 테이블은 기본으로 추가
        tables.insert(
            SCHEMA_TABLE_NAME.to_string(),
            Arc::new(Table::BTree(sqlite_schema_table().into())),
        );
        Self {
            tables,
            indexes,
            schema_version: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Table {
    BTree(Arc<BTreeTable>),
}

impl Table {

}

#[derive(Clone, Debug)]
pub struct Index {
    pub name: String,
    pub table_name: String,
    pub root_page: i64,
}

#[derive(Clone, Debug)]
pub struct BTreeTable {
    pub root_page: i64,
    pub name: String,
    pub primary_key_columns: Vec<(String, SortOrder)>,
    pub columns: Vec<Column>,
    pub has_rowid: bool,
    pub is_strict: bool,
    pub has_autoincrement: bool,
    pub unique_sets: Vec<UniqueSet>,
    pub foreign_keys: Vec<Arc<ForeignKey>>,
}

/// sqlite_schema 테이블 구성
pub fn sqlite_schema_table() -> BTreeTable {
    BTreeTable {
        root_page: 1,
        name: "sqlite_schema".to_string(),
        has_rowid: true,
        is_strict: false,
        has_autoincrement: false,
        primary_key_columns: vec![],
        columns: vec![
            Column::new_default_text(Some("type".to_string()), "TEXT".to_string(), None),
            Column::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
            Column::new_default_text(Some("tbl_name".to_string()), "TEXT".to_string(), None),
            Column::new_default_integer(Some("rootpage".to_string()), "INT".to_string(), None),
            Column::new_default_text(Some("sql".to_string()), "TEXT".to_string(), None),
        ],
        foreign_keys: vec![],
        unique_sets: vec![],
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UniqueSet {
    pub columns: Vec<(String, SortOrder)>,
    pub is_primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// Columns in this table (child side)
    pub child_columns: Vec<String>,
    /// Referenced (parent) table
    pub parent_table: String,
    /// Parent-side referenced columns
    pub parent_columns: Vec<String>,
    pub on_delete: RefAct,
    pub on_update: RefAct,
    /// DEFERRABLE INITIALLY DEFERRED
    pub deferred: bool,
}

// flags
const F_PRIMARY_KEY: u16 = 1;
const F_ROWID_ALIAS: u16 = 2;
const F_NOTNULL: u16 = 4;
const F_UNIQUE: u16 = 8;
const F_HIDDEN: u16 = 16;

// pack Type and Collation in the remaining bits
const TYPE_SHIFT: u16 = 5;
const TYPE_MASK: u16 = 0b111 << TYPE_SHIFT;
const COLL_SHIFT: u16 = TYPE_SHIFT + 3;
const COLL_MASK: u16 = 0b11 << COLL_SHIFT;

#[derive(Debug, Clone)]
pub struct Column {
    // 컬럼의 이름
    pub name: Option<String>,
    // 컬럼의 데이터 타입 선언 문자열을 그대로 저장. 예: "INTEGER", "TEXT", ...
    pub ty_str: String,
    // 컬럼의 기본값을 표현하는 SQL Expr(표현식)을 저장
    // 표현식이 복잡할 수 있기 때문에 Box 사용
    pub default: Option<Box<Expr>>,
    // 컬럼에 대한 여러가지 속성을 비트로 저장
    //   primary_key(기본 키 여부):    bit 0
    //   rowid_alias(ROWID 별칭 여부): bit 1
    //   notnull(NULL 허용 여부):      bit 2
    //   unique(고유 제약 조건 여부):    bit 3
    //   hidden(숨겨진 컬럼 여부):      bit 4
    //   type(데이터 타입):            bit 5,6,7
    //   collationSeq(정렬 순서):      bit 8,9
    raw: u16,
}

impl Column {
    pub fn new_default_text(name: Option<String>, ty_str: String, default: Option<Box<Expr>>) -> Self {
        Self::new(name, ty_str, default, Type::Text, None, ColDef::default())
    }

    pub fn new_default_integer(name: Option<String>, ty_str: String, default: Option<Box<Expr>>) -> Self {
        Self::new(
            name,
            ty_str,
            default,
            Type::Integer,
            None,
            ColDef::default(),
        )
    }

    #[inline]
    pub const fn new(
        name: Option<String>,
        ty_str: String,
        default: Option<Box<Expr>>,
        ty: Type,
        col: Option<CollationSeq>,
        coldef: ColDef,
    ) -> Self {
        let mut raw = 0u16;
        raw |= (ty as u16) << TYPE_SHIFT;
        if let Some(c) = col {
            raw |= (c as u16) << COLL_SHIFT;
        }
        if coldef.primary_key {
            raw |= F_PRIMARY_KEY
        }
        if coldef.rowid_alias {
            raw |= F_ROWID_ALIAS
        }
        if coldef.notnull {
            raw |= F_NOTNULL
        }
        if coldef.unique {
            raw |= F_UNIQUE
        }
        if coldef.hidden {
            raw |= F_HIDDEN
        }
        Self {
            name,
            ty_str,
            default,
            raw,
        }
    }
}

#[derive(Default)]
pub struct ColDef {
    pub primary_key: bool,
    pub rowid_alias: bool,
    pub notnull: bool,
    pub unique: bool,
    pub hidden: bool,
}

/// 필드가 가질 수 있는 타입
/// 내부 저장시 비트로 저장된다.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Type {
    Null = 0,
    Text = 1,
    Numeric = 2,
    Integer = 3,
    Real = 4,
    Blob = 5,
}

impl Type {
    #[inline]
    const fn from_bits(bits: u8) -> Self {
        match bits {
            0 => Type::Null,
            1 => Type::Text,
            2 => Type::Numeric,
            3 => Type::Integer,
            4 => Type::Real,
            5 => Type::Blob,
            _ => Type::Null,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Null => "",
            Self::Text => "TEXT",
            Self::Numeric => "NUMERIC",
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Blob => "BLOB",
        };
        write!(f, "{s}")
    }
}
