use std::time::{SystemTime, UNIX_EPOCH};

/// Unix epoch milliseconds. The only time representation in the framework.
pub type Timestamp = i64;

/// Source of wall-clock time, injectable for tests.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// Production clock backed by the operating system.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as Timestamp)
            .unwrap_or_default()
    }
}
