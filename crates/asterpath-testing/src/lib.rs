//! Test doubles and backend-neutral contract suites.
//!
//! This crate is for test targets only. The in-memory backend is **not**
//! durable: it does not satisfy G1 (committed-prefix persistence).

pub mod clock;
pub mod contract;
pub mod failpoint;
pub mod fake_sandbox;
pub mod http_mock;
pub mod memory;
pub mod provider_contract;
pub mod scripted;

pub use clock::ManualClock;
pub use contract::{StoreFactory, session_store_contract};
pub use failpoint::{Action, Failpoint, FailpointEventLog, FailpointTool, Point};
pub use fake_sandbox::{FakeSandboxProvider, RecordedSpawnRequest};
pub use memory::{InMemoryEventLog, MemoryBackend};
pub use provider_contract::{ProviderWire, provider_contract};
pub use scripted::{Fixture, FixtureStep, ScriptedProvider, ScriptedStep};
