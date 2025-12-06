
use std::sync::atomic::{AtomicU64, Ordering};

pub trait LogicalClock {
    fn get_timestamp(&self) -> u64;
}

/// sequence는 0에서 시작하여 1씩 증가하는 timestamp 값이다.
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
    fn get_timestamp(&self) -> u64 {
        self.ts_sequence.fetch_add(1, Ordering::SeqCst)
    }
}
