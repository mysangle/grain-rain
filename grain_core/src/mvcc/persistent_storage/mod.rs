
use crate::File;
use std::{fmt::Debug, sync::Arc};

pub struct Storage {

}

impl Storage {
    pub fn new(file: Arc<dyn File>) -> Self {
        Self {

        }
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalLog {{ logical_log }}")
    }
}
