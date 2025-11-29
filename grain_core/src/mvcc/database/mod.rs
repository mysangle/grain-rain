
use crate::mvcc::{
    clock::LogicalClock,
    persistent_storage::Storage,
};

#[derive(Debug)]
pub struct MvStore<Clock: LogicalClock> {
    clock: Clock,
    storage: Storage,
}

impl<Clock: LogicalClock> MvStore<Clock> {
    pub fn new(clock: Clock, storage: Storage) -> Self {
        Self {
            clock,
            storage,
        }
    }
}
