
use crate::io::Completion;

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
