
pub mod clock;
mod memory;

pub use clock::Clock;
pub use memory::MemoryIO;
use crate::Result;
use std::sync::Arc;

pub trait File: Send + Sync {
    
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, direct: bool) -> Result<Arc<dyn File>>;
}
