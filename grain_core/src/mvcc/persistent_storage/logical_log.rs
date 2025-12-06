
use crate::File;
use std::sync::Arc;

pub struct LogicalLog {
    pub file: Arc<dyn File>,
    pub offset: u64,
}

impl LogicalLog {
    pub fn new(file: Arc<dyn File>) -> Self {
        Self {
            file,
            offset: 0,
        }
    }
}
