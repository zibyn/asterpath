//! Request, turn, compaction and subagent configuration value shapes, the
//! session config snapshot, and the tool descriptor.

use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::time::Duration;

use asterpath_core::LogId;
use serde::{Deserialize, Serialize};

use crate::ids::{JsonSchema, ModelId, ProviderId, ToolName};
use crate::item::Part;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSpec {
    pub provider: ProviderId,
    pub model: ModelId,
    pub context_window: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestConfig {
    pub max_output_tokens: NonZeroU32,
    pub tool_choice: ToolChoice,
    pub disable_parallel_tool_use: bool,
    pub sampling: Option<SamplingParams>,
    pub reasoning: Option<ReasoningParams>,
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            max_output_tokens: NonZeroU32::new(8192).expect("non-zero"),
            tool_choice: ToolChoice::Auto,
            disable_parallel_tool_use: false,
            sampling: None,
            reasoning: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolChoice {
    #[default]
    Auto,
    None,
    Required,
    Tool(ToolName),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SamplingParams {
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub top_k: Option<u32>,
    pub seed: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasoningParams {
    pub effort: Option<ReasoningEffort>,
    pub budget_tokens: Option<u32>,
    /// OpenAI `reasoning.summary`; ignored by Anthropic.
    pub summary: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnConfig {
    pub max_steps_per_turn: Option<u32>,
    /// Includes the first attempt.
    pub max_attempts: NonZeroU32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub request_timeout: Option<Duration>,
    pub cancel_grace: Duration,
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self {
            max_steps_per_turn: None,
            max_attempts: NonZeroU32::new(3).expect("non-zero"),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            request_timeout: Some(Duration::from_secs(600)),
            cancel_grace: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// `None`: `min(8192, context_window / 4)`.
    pub reserve_output_tokens: Option<u32>,
    pub target_ratio: f32,
    pub max_auto_compactions_per_turn: u32,
    pub max_summary_tokens: u32,
    /// `None`: the session model.
    pub summary_model: Option<ModelSpec>,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            reserve_output_tokens: None,
            target_ratio: 0.5,
            max_auto_compactions_per_turn: 1,
            max_summary_tokens: 2048,
            summary_model: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubagentConfig {
    pub default_context: ContextPolicy,
    pub max_depth: u8,
    pub max_concurrent_children: NonZeroUsize,
    pub max_live_children: NonZeroUsize,
    pub spawn_timeout: Option<Duration>,
    pub cancel_grace: Duration,
}

impl Default for SubagentConfig {
    fn default() -> Self {
        Self {
            default_context: ContextPolicy::Fresh,
            max_depth: 3,
            max_concurrent_children: NonZeroUsize::new(8).expect("non-zero"),
            max_live_children: NonZeroUsize::new(32).expect("non-zero"),
            spawn_timeout: None,
            cancel_grace: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextPolicy {
    Fresh,
    Fork { at: LogId },
}

/// The persistent projection of a session's construction-time configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub model: ModelSpec,
    /// Only `Part::Text` is valid.
    pub system: Vec<Part>,
    /// Resolved at session creation and then fixed for the session's lifetime.
    pub tools: Vec<ToolDescriptor>,
    pub request: RequestConfig,
    pub turn: TurnConfig,
    pub compaction: CompactionConfig,
    pub subagents: SubagentConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub name: ToolName,
    pub description: String,
    pub schema: JsonSchema,
    pub parallelism: Parallelism,
    pub replay: ReplaySafety,
    pub side_effect: SideEffect,
    pub provenance: Provenance,
    pub timeout: Option<Duration>,
    pub max_output_bytes: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Parallelism {
    Parallel,
    Exclusive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplaySafety {
    Safe,
    Unsafe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SideEffect {
    ReadOnly,
    Mutating,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Provenance {
    InProcess,
    External {
        transport: TransportKind,
    },
    /// Only recorded in `ToolCallRequested` when the requested tool is not registered.
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportKind {
    Stdio,
    Http,
}

/// Pipeline concurrency, truncation and external-tool timeout defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolConfig {
    pub max_parallel_calls: NonZeroUsize,
    pub max_output_bytes: usize,
    pub max_output_bytes_per_step: usize,
    pub external_timeout: Option<Duration>,
    pub kill_grace: Duration,
}

impl Default for ToolConfig {
    fn default() -> Self {
        Self {
            max_parallel_calls: NonZeroUsize::new(8).expect("non-zero"),
            max_output_bytes: 64 * 1024,
            max_output_bytes_per_step: 256 * 1024,
            external_timeout: Some(Duration::from_secs(120)),
            kill_grace: Duration::from_secs(5),
        }
    }
}
