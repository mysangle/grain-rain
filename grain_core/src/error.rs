
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum GrainError {
    #[error("Database is busy")]
    Busy,
    #[error("Corrupt database: {0}")]
    Corrupt(String),
    #[error("Runtime error: integer overflow")]
    IntegerOverflow,
    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum CompletionError {

}

#[macro_export]
macro_rules! bail_corrupt_error {
    ($($arg:tt)*) => {
        return Err($crate::error::GrainError::Corrupt(format!($($arg)*)))
    };
}
