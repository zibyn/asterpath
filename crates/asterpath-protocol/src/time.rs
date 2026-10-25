use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unix epoch milliseconds. The only time representation in the framework.
pub type Timestamp = i64;

/// 16 unique bytes: the injected clock in the high half, a per-process random
/// seed plus a process-wide counter in the low half. Fixed clock -> fixed
/// prefix; the random seed keeps ids from different processes sharing one
/// store apart.
pub fn unique_bytes(now: Timestamp) -> [u8; 16] {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static SEED: OnceLock<u64> = OnceLock::new();
    let seed = *SEED.get_or_init(|| RandomState::new().build_hasher().finish());
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let stamp = (now as u64) ^ u64::from(std::process::id()).rotate_left(17);
    compose(stamp, seed.wrapping_add(counter))
}

fn compose(stamp: u64, low: u64) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&stamp.to_le_bytes());
    bytes[8..].copy_from_slice(&low.to_le_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_clock_yields_shared_prefix_and_distinct_ids() {
        let first = unique_bytes(42);
        let second = unique_bytes(42);
        assert_eq!(first[..8], second[..8]);
        assert_ne!(first, second);
    }

    #[test]
    fn low_half_differs_across_processes() {
        const CHILD: &str = "ASTERPATH_UNIQUE_BYTES_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let bytes = unique_bytes(0);
            println!("low={:?}", &bytes[8..]);
            return;
        }
        let low = || {
            let out = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "time::tests::low_half_differs_across_processes",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            let stdout = String::from_utf8(out.stdout).unwrap();
            stdout
                .lines()
                .find(|line| line.starts_with("low="))
                .unwrap()
                .to_owned()
        };
        assert_ne!(low(), low());
    }
}
