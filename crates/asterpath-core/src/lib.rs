//! Asterpath kernel: module host, typed event bus, contract traits and value types.
//!
//! This crate holds mechanisms only. Concrete implementations (event log, storage,
//! executors, agent runner, providers) live in module crates.

pub mod contracts;
pub mod error;
pub mod event;
pub mod ids;
pub mod module;
pub mod service;
pub mod time;

pub use contracts::{
    ApprovalEndpoint, ApprovalGate, AwaitSignal, DedupKey, DueTimer, DurableExecutor, EventLog,
    LogEntry, LogQuery, NewEntry, SessionCatalog, SessionRetirement, SignalDelivery, SignalIntent,
    SignalName, SignalPayload, Storage, TimerRequest, WakeCause,
};
pub use error::{ExecutionError, LifecycleError, RetirementError, StorageError};
pub use event::{Event, Handler, Middleware, Next, WaterfallEvent};
pub use ids::{
    InputId, LogId, LogIdRange, SessionId, SignalId, StepId, TimerId, ToolCallId, TurnId,
};
pub use module::{Effect, Module, ModuleCtx, ModuleHost};
pub use service::{ServiceId, ServiceKey, service_key};
pub use time::{Clock, SystemClock, Timestamp};
