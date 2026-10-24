//! The contract crate: ids, the opaque log envelope and storage contracts,
//! clock, errors, adapter traits, hooks, and the typed protocol view of the
//! log (items, events, configuration snapshots, read-time upcasting, dedup-key
//! construction, session facts and projection). ADR-0026.

pub mod agent;
pub mod compactor;
pub mod config;
pub mod contracts;
pub mod decode;
pub mod dedup;
pub mod encode;
pub mod error;
pub mod event;
pub mod facts;
pub mod hash;
pub mod hooks;
pub mod ids;
pub mod item;
pub mod names;
pub mod projection;
pub mod provider;
pub mod sandbox;
pub mod time;
pub mod tool;
pub mod tracing;
pub mod usage;

pub use agent::{
    AgentError, AgentEvent, ApprovalRequest, CancelRegistration, CancellationRegistry,
    CompactResult, CompactionError, CompactionOutcome, RunRequest, RunTrigger, SessionSpec,
    ToolSelection, UserInput,
};

pub use compactor::{
    CompactPlan, CompactSummary, CompactionInput, CompactionJob, Compactor, UsageAnchor,
};
pub use config::{
    CompactionConfig, ConfigPatch, ConfigSnapshot, ContextPolicy, ModelSpec, Parallelism,
    Provenance, ReasoningEffort, ReasoningParams, ReplaySafety, RequestConfig, SamplingParams,
    SideEffect, SubagentConfig, ToolChoice, ToolConfig, ToolDescriptor, TransportKind, TurnConfig,
    effective_config,
};
pub use contracts::{
    ApprovalEndpoint, ApprovalGate, CatalogCursor, CatalogEntry, DedupKey, LogEntry, LogQuery,
    NewEntry, ParentFilter, PendingApproval, SessionQuery, SessionStore, scan_all,
};
pub use decode::{LOG_SCHEMA_VERSION, LogDecodeError, decode_entry, decode_payload};
pub use encode::encode_entry;
pub use error::{ExecutionError, RetirementError, StorageError};
pub use event::{
    AbortKind, CancelReason, ChildFailure, ChildOutcome, CompactionAbortReason, CompactionTrigger,
    ParentRef, ProviderErrorInfo, ResumeReason, SessionEvent, StopReason, Summary, ToolErrorInfo,
    ToolErrorKind, ToolResultStatus, TurnEndReason,
};
pub use facts::{
    OpenChild, OpenCompaction, OpenIntent, OpenStep, OpenTool, SessionFacts, SessionState,
    StepState,
};
pub use hash::Fnv;
pub use hooks::{
    CommittedRecord, Gate, HookError, Hooks, StepCtx, ToolCallCtx, ToolGate, ToolResultView,
    UsageLookup,
};
pub use ids::{
    InputId, JsonSchema, JsonString, JsonValue, LogId, LogIdRange, ModelAlias, ModelId, ProviderId,
    SessionId, StepId, ToolArgs, ToolCallId, ToolName, TurnId, UpdateId,
};
pub use item::{
    FileSource, ImageSource, Item, Part, ProviderFileId, RawProviderData, ReasoningBinding, Role,
    tools_hash,
};
pub use names::{
    ToolNameError, external_tool_name, sanitize_tool_name, validate_external_tool_name,
    validate_server_id, validate_tool_name,
};
pub use projection::{
    ProjectedEntry, ProjectedOrigin, ProjectionError, RequestHistory, history, project,
    request_history,
};
pub use provider::{
    DuplicateProviderId, ModelRequest, PartDelta, Provider, ProviderCapabilities, ProviderCatalog,
    ProviderError, ProviderErrorKind, StreamEvent,
};
pub use sandbox::{
    ConfinedSpawn, Enforcement, SandboxAspect, SandboxError, SandboxPolicy, SandboxProvider,
    SpawnRequest,
};
pub use time::{Clock, SystemClock, Timestamp, unique_bytes};
pub use tool::{
    ApprovalDecision, ApprovalPolicy, ApprovalVerdict, CancelToken, DefaultApprovalPolicy,
    NO_APPROVAL_ENDPOINT, Tool, ToolContext, ToolError, ToolOutput,
};
pub use tracing::{
    AttemptEvent, CompactionAttrs, ErrorType, ModelCallAttrs, ResumeEvent, SessionAttrs, Span,
    SpanAttributes, SpanFilter, SpanId, SpanKind, SpanStatus, SubagentAttrs, ToolCallAttrs,
    TraceError, TraceId, TurnAttrs, WaitAttrs, WaitKind,
};
pub use usage::{
    InputTokens, OutputTokens, Usage, UsageBucket, UsageEntry, UsageError, UsageFilter,
    UsageRecord, UsageReport, UsageScope, UsageTotals, usage_of,
};
