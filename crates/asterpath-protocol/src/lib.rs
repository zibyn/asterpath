//! Canonical protocol value model, event log decoding and folds.
//!
//! The core crate owns the opaque log envelope; this crate owns its typed view:
//! items, events, configuration snapshots, read-time upcasting, dedup-key
//! construction, open-intent folding and projection.

pub mod agent;
pub mod config;
pub mod decode;
pub mod dedup;
pub mod encode;
pub mod event;
pub mod ids;
pub mod intents;
pub mod item;
pub mod projection;
pub mod provider;
pub mod tool;
pub mod usage;

pub use agent::{
    AgentError, AgentEvent, AgentRunner, ApprovalRequest, CompactResult, CompactionError,
    CompactionOutcome, RunRequest, RunTrigger, SessionSpec, ToolSelection, UserInput,
};

pub use config::{
    CompactionConfig, ConfigSnapshot, ContextPolicy, ModelSpec, Parallelism, Provenance,
    ReasoningEffort, ReasoningParams, ReplaySafety, RequestConfig, SamplingParams, SideEffect,
    SubagentConfig, ToolChoice, ToolConfig, ToolDescriptor, TransportKind, TurnConfig,
};
pub use decode::{LOG_SCHEMA_VERSION, LogDecodeError, decode_entry, decode_payload};
pub use encode::encode_entry;
pub use event::{
    AbortKind, CancelReason, ChildFailure, ChildOutcome, CompactionAbortReason, CompactionTrigger,
    ParentRef, ResumeReason, SessionEvent, StopReason, Summary, ToolErrorInfo, ToolErrorKind,
    ToolResultStatus, TurnEndReason,
};
pub use ids::{
    JsonSchema, JsonString, JsonValue, ModelAlias, ModelId, ProviderId, ToolArgs, ToolName,
};
pub use intents::{IntentDelta, OpenIntent, OpenIntents, SessionState, StepState};
pub use item::{ImageSource, Item, Part, RawProviderData, ReasoningBinding, Role};
pub use projection::{ProjectedEntry, ProjectedOrigin, ProjectionError, history, project};
pub use provider::{
    DuplicateProviderId, ModelRequest, PartDelta, Provider, ProviderCapabilities, ProviderCatalog,
    ProviderError, ProviderErrorKind, StreamEvent,
};
pub use tool::{
    APPROVAL_SIGNAL_PREFIX, ApprovalDecision, ApprovalPolicy, ApprovalVerdict, CancelToken,
    DefaultApprovalPolicy, NO_APPROVAL_ENDPOINT, PostExecuteError, PostExecuteEvent,
    PreExecuteError, PreExecuteEvent, TOOL_REGISTRY_SERVICE, Tool, ToolContext, ToolError,
    ToolOutput, ToolRegistry, approval_signal_id, approval_signal_name,
};
pub use usage::{InputTokens, OutputTokens, Usage};
