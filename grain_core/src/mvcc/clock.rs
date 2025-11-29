
use std::sync::atomic::AtomicU64;

pub trait LogicalClock {

}

#[derive(Debug, Default)]
pub struct LocalClock {
    ts_sequence: AtomicU64,
}

impl LocalClock {
    pub fn new() -> Self {
        Self {
            ts_sequence: AtomicU64::new(0),
        }
    }
}

impl LogicalClock for LocalClock {
    
}
