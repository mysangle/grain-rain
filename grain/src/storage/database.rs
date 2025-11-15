
use std::sync::Arc;

pub trait DatabaseStorage: Send + Sync {

}

#[derive(Clone)]
pub struct DatabaseFile {
    file: Arc<dyn crate::io::File>,
}

impl DatabaseFile {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}

impl DatabaseStorage for DatabaseFile {
    
}
