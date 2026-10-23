//! Asterpath kernel: module host, typed event bus, contract traits and value types.
//!
//! This crate holds mechanisms only. Concrete implementations (event log, storage,
//! executors, agent runner, providers) live in module crates.

pub mod contracts;
pub mod error;
pub mod event;
pub mod hash;
pub mod ids;
pub mod module;
pub mod service;
pub mod time;

pub use contracts::{
    ApprovalEndpoint, ApprovalGate, AwaitSignal, DURABLE_EXECUTOR_SERVICE, DedupKey,
    DurableExecutor, EVENT_LOG_SERVICE, EventLog, LogEntry, LogQuery, NewEntry,
    SESSION_CATALOG_SERVICE, SESSION_RETIREMENT_SERVICE, STORAGE_SERVICE, SessionCatalog,
    SessionRetirement, SignalDelivery, SignalName, SignalPayload, Storage, WakeCause,
};
pub use error::{ExecutionError, LifecycleError, RetirementError, StorageError};
pub use event::{Event, Handler, Middleware, Next, WaterfallEvent};
pub use hash::Fnv;
pub use ids::{InputId, LogId, LogIdRange, SessionId, SignalId, StepId, ToolCallId, TurnId};
pub use module::{Effect, Module, ModuleCtx, ModuleHost};
pub use service::{ServiceId, ServiceKey, service_key};
pub use time::{Clock, SystemClock, Timestamp, unique_bytes};
