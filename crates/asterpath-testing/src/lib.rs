//! Test doubles and backend-neutral contract suites.
//!
//! This crate is for test targets only. The in-memory backend is **not**
//! durable: it does not satisfy G1 (committed-prefix persistence).

pub mod clock;
pub mod contract;
pub mod failpoint;
pub mod memory;
pub mod scripted;

pub use clock::ManualClock;
pub use contract::{BackendFactory, BackendHandle, event_log_contract, storage_contract};
pub use failpoint::{Action, Failpoint, FailpointEventLog, FailpointTool, Point};
pub use memory::{InMemoryEventLog, InMemoryStorage, MemoryBackend};
pub use scripted::{Fixture, FixtureStep, ScriptedProvider, ScriptedStep};
