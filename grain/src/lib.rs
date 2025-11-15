
mod error;
mod util;

pub use error::GrainError;
use std::sync::Arc;

pub type Result<T, E = GrainError> = std::result::Result<T, E>;

pub struct Database {

}

impl Database {
    pub fn open_new(path: &str) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        let io = Self::io_for_path(path)?;
        let db = Self::open_file()?;
        Ok((io, db))
    }

    pub fn io_for_path(path: &str) -> Result<Arc<dyn IO>> {
        use crate::util::MEMORY_PATH;
        
        let io = match path.trim() {
            MEMORY_PATH => {}
            _ => {
                tracing::error!("not supported yet!");
                std::process::exit(0);
            }
        };
        Ok(io)
    }

    fn open_file() -> Result<Arc<Database>> {

    }
}
