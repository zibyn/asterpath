//! Asterpath kernel: contract traits and value types.
//!
//! This crate holds mechanisms only. Concrete implementations (event log, storage,
//! executors, agent runner, providers) live in module crates; extension is
//! through the protocol `Hooks` trait (ADR-0025).

pub mod contracts;
pub mod error;
pub mod hash;
pub mod ids;
pub mod time;

pub use contracts::{
    ApprovalEndpoint, ApprovalGate, AwaitSignal, DedupKey, DurableExecutor, EventLog, LogEntry,
    LogQuery, NewEntry, SessionCatalog, SessionRetirement, SignalDelivery, SignalName,
    SignalPayload, Storage, WakeCause,
};
pub use error::{ExecutionError, RetirementError, StorageError};
pub use hash::Fnv;
pub use ids::{InputId, LogId, LogIdRange, SessionId, SignalId, StepId, ToolCallId, TurnId};
pub use time::{Clock, SystemClock, Timestamp, unique_bytes};
