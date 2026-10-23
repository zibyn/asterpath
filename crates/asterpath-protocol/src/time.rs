use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unix epoch milliseconds. The only time representation in the framework.
pub type Timestamp = i64;

/// 16 process-unique bytes: the injected clock in the high half, a process-wide
/// counter in the low half. Fixed clock, fixed call order -> deterministic.
pub fn unique_bytes(now: Timestamp) -> [u8; 16] {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let stamp = (now as u64) ^ u64::from(std::process::id()).rotate_left(17);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&stamp.to_le_bytes());
    bytes[8..].copy_from_slice(&counter.to_le_bytes());
    bytes
}

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
