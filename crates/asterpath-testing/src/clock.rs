//! Controllable clock for deterministic timer tests.

use std::sync::atomic::{AtomicI64, Ordering};

use asterpath_core::{Clock, Timestamp};

/// Starts at a fixed epoch and only moves when `advance` is called.
#[derive(Debug)]
pub struct ManualClock {
    now: AtomicI64,
}

impl ManualClock {
    /// 2023-11-14T22:13:20Z; fixed so tests never depend on wall time.
    pub const BASE: Timestamp = 1_700_000_000_000;

    pub fn new() -> Self {
        Self {
            now: AtomicI64::new(Self::BASE),
        }
    }

    pub fn advance(&self, millis: i64) {
        self.now.fetch_add(millis, Ordering::SeqCst);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        self.now.load(Ordering::SeqCst)
    }
}
