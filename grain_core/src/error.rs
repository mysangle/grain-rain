
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
    #[error("Runtime error: integer overflow")]
    IntegerOverflow,
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Invalid argument supplied: {0}")]
    InvalidArgument(String),
}

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum CompletionError {
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
