//! Canonical protocol value model, event log decoding and folds.
//!
//! The core crate owns the opaque log envelope; this crate owns its typed view:
//! items, events, configuration snapshots, read-time upcasting, dedup-key
//! construction, open-intent folding and projection.

pub mod config;
pub mod decode;
pub mod dedup;
pub mod event;
pub mod ids;
pub mod intents;
pub mod item;
pub mod projection;
pub mod usage;

pub use config::{
    CompactionConfig, ConfigSnapshot, ContextPolicy, ModelSpec, Parallelism, Provenance,
    ReasoningEffort, ReasoningParams, ReplaySafety, RequestConfig, SamplingParams, SideEffect,
    SubagentConfig, ToolChoice, ToolDescriptor, TransportKind, TurnConfig,
};
pub use decode::{LOG_SCHEMA_VERSION, LogDecodeError, decode_entry, decode_payload};
pub use event::{
    AbortKind, CancelReason, ChildFailure, ChildOutcome, CompactionAbortReason, CompactionTrigger,
    ParentRef, ResumeReason, SessionEvent, StopReason, Summary, ToolErrorInfo, ToolErrorKind,
    ToolResultStatus, TurnEndReason,
};
pub use ids::{
    JsonSchema, JsonString, JsonValue, ModelAlias, ModelId, ProviderId, ToolArgs, ToolName,
};
pub use intents::{OpenIntents, SessionState, StepState};
pub use item::{ImageSource, Item, Part, RawProviderData, ReasoningBinding, Role};
pub use projection::{ProjectedEntry, ProjectedOrigin, ProjectionError, history, project};
pub use usage::{InputTokens, OutputTokens, Usage};
