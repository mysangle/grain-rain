
pub mod logical_log;

use crate::{
    mvcc::persistent_storage::logical_log::LogicalLog,
    File,
};
use parking_lot::RwLock;
use std::{fmt::Debug, sync::Arc};

pub struct Storage {
    pub logical_log: RwLock<LogicalLog>,
}

impl Storage {
    pub fn new(file: Arc<dyn File>) -> Self {
        Self {
            logical_log: RwLock::new(LogicalLog::new(file)),
        }
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalLog {{ logical_log }}")
    }
}
