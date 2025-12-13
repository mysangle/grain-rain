
use crate::storage::page_cache::CacheError;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum GrainError {
    #[error("Database is busy")]
    Busy,
    #[error(transparent)]
    CacheError(#[from] CacheError),
    #[error(transparent)]
    CompletionError(#[from] CompletionError),
    #[error("Corrupt database: {0}")]
    Corrupt(String),
    #[error("Extension error: {0}")]
    ExtensionError(String),
    #[error("Runtime error: integer overflow")]
    IntegerOverflow,
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Invalid argument supplied: {0}")]
    InvalidArgument(String),
    #[error("File is not a database")]
    NotADB,
    #[error("No such transaction ID: {0}")]
    NoSuchTransactionID(String),
    #[error(
        "Database is empty, header does not exist - page 1 should've been allocated before this"
    )]
    Page1NotAlloc,
}

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum CompletionError {
    #[error("Checksum mismatch on page {page_id}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        page_id: usize,
        expected: u64,
        actual: u64,
    },
    #[error("I/O error: {0}")]
    IOError(std::io::ErrorKind),
}

#[macro_export]
macro_rules! bail_corrupt_error {
    ($($arg:tt)*) => {
        return Err($crate::error::GrainError::Corrupt(format!($($arg)*)))
    };
}

impl From<std::io::Error> for GrainError {
    fn from(value: std::io::Error) -> Self {
        Self::CompletionError(CompletionError::IOError(value.kind()))
    }
}

impl From<std::io::Error> for CompletionError {
    fn from(value: std::io::Error) -> Self {
        CompletionError::IOError(value.kind())
    }
}
