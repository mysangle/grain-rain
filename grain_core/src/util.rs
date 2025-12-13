
use crate::{types::IOResult, IO, Result};

pub const MEMORY_PATH: &str = ":memory:";

/// 매크로가 실제 사용되는 곳에 IOCompletions와 IOResult가 있어야 한다.
#[macro_export]
macro_rules! io_yield_one {
    ($c:expr) => {
        return Ok(IOResult::IO(IOCompletions::Single($c)));
    };
}

pub trait IOExt {
    fn block<T>(&self, f: impl FnMut() -> Result<IOResult<T>>) -> Result<T>;
}

impl<I: ?Sized + IO> IOExt for I {
    fn block<T>(&self, mut f: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
        Ok(loop {
            match f()? {
                IOResult::Done(v) => break v,
                IOResult::IO(io) => io.wait(self)?,
            }
        })
    }
}
