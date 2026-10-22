//! Test doubles and backend-neutral contract suites.
//!
//! This crate is for test targets only. The in-memory backend is **not**
//! durable: it does not satisfy G1 (committed-prefix persistence).

pub mod contract;
pub mod memory;

pub use contract::{BackendFactory, BackendHandle, event_log_contract};
pub use memory::{InMemoryEventLog, InMemoryStorage, MemoryBackend};
