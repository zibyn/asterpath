//! Request, turn, compaction and subagent configuration value shapes, the
//! session config snapshot, and the tool descriptor.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::LogId;
use serde::{Deserialize, Serialize};

use crate::ids::{JsonSchema, JsonString, ModelId, ProviderId, ToolName, UpdateId};
use crate::item::Part;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSpec {
    pub provider: ProviderId,
    pub model: ModelId,
    pub context_window: Option<u32>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestConfig {
    pub max_output_tokens: NonZeroU32,
    pub tool_choice: ToolChoice,
    pub disable_parallel_tool_use: bool,
    pub sampling: Option<SamplingParams>,
    pub reasoning: Option<ReasoningParams>,
    pub stop_sequences: Vec<String>,
    /// Per-provider JSON objects merged into the top level of that provider's
    /// request body; entries for other providers are ignored. Values are host policy.
    pub provider_options: BTreeMap<ProviderId, JsonString>,
    /// Constrain the reply to a JSON schema. The reply is still ordinary
    /// message text; the framework neither parses nor validates it.
    #[serde(default)]
    pub output_format: Option<OutputFormat>,
    /// Per-provider server-executed tool definitions (each a JSON object),
    /// appended verbatim after the function tools in that provider's
    /// request; entries for other providers are ignored (ADR-0031).
    #[serde(default)]
    pub server_tools: BTreeMap<ProviderId, Vec<JsonString>>,
}

/// A named JSON schema the model's reply must follow (structured output).
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputFormat {
    pub name: String,
    pub schema: JsonSchema,
}

impl OutputFormat {
    pub fn new(name: impl Into<String>, schema: JsonSchema) -> Self {
        Self {
            name: name.into(),
            schema,
        }
    }
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            max_output_tokens: NonZeroU32::new(8192).expect("non-zero"),
            tool_choice: ToolChoice::Auto,
            disable_parallel_tool_use: false,
            sampling: None,
            reasoning: None,
            stop_sequences: Vec::new(),
            provider_options: BTreeMap::new(),
            output_format: None,
            server_tools: BTreeMap::new(),
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
    /// Ask for reasoning summaries: OpenAI `reasoning.summary`, Anthropic
    /// `thinking.display: "summarized"` (on models that accept it).
    pub summary: bool,
}

/// A level the adapter maps to the provider's effort value. A model that does
/// not accept the level gets `Unsupported`; adapters never pick a nearby one.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReasoningEffort {
    /// No reasoning: OpenAI `none`; Anthropic sends neither thinking nor effort.
    Off,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
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
    pub spawn_timeout: Option<Duration>,
    pub cancel_grace: Duration,
}

impl Default for SubagentConfig {
    fn default() -> Self {
        Self {
            default_context: ContextPolicy::Fresh,
            max_depth: 3,
            max_concurrent_children: NonZeroUsize::new(8).expect("non-zero"),
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

/// A host change to a session's configuration (ADR-0030), validated by
/// `SessionHandle::update_config` and recorded as one `ConfigChanged`. Each
/// present field replaces the effective value whole; absent fields keep
/// theirs. `append_system` is appended, never replaced; `remove_tools` and
/// `add_tools` change the tool set by name.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigPatch {
    /// Dedup key: a retried patch with the same id is recorded once.
    /// Required when `append_system`, `add_tools` or `remove_tools` is
    /// non-empty, since applying those twice is not idempotent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<UpdateId>,
    #[serde(default)]
    pub model: Option<ModelSpec>,
    #[serde(default)]
    pub request: Option<RequestConfig>,
    /// Instructions appended to the system. Only `Part::Text` is valid.
    #[serde(default)]
    pub append_system: Vec<Part>,
    /// Tools the model may call from the next step on, recorded as the
    /// registry's descriptors; recovery reads them and never re-resolves.
    #[serde(default)]
    pub add_tools: Vec<ToolDescriptor>,
    /// Effective tools the model may no longer call from the next step on.
    #[serde(default)]
    pub remove_tools: Vec<ToolName>,
}

impl ConfigPatch {
    /// Whether the patch adds or removes tools.
    pub fn changes_tools(&self) -> bool {
        !self.add_tools.is_empty() || !self.remove_tools.is_empty()
    }

    /// Whether the patch has a place in history (appended instructions or a
    /// tool change); applying such a patch twice is not idempotent.
    pub fn is_positional(&self) -> bool {
        !self.append_system.is_empty() || self.changes_tools()
    }
}

/// The effective configuration at `watermark`: `SessionCreated.config` with
/// every `ConfigChanged` whose `seq < watermark` applied in log order. `None`
/// when the records hold no `SessionCreated`. The engine's only config fold:
/// the agent, scheduler, trace, facade and CLI all read config through it.
pub fn effective_config(
    records: &[(crate::LogEntry, crate::SessionEvent)],
    watermark: u64,
) -> Option<ConfigSnapshot> {
    let mut ordered: Vec<_> = records.iter().collect();
    ordered.sort_by_key(|(entry, _)| entry.seq);
    let mut config: Option<ConfigSnapshot> = None;
    for (entry, event) in ordered {
        match event {
            crate::SessionEvent::SessionCreated {
                config: created, ..
            } if config.is_none() => {
                config = Some(created.clone());
            }
            crate::SessionEvent::ConfigChanged { patch } if entry.seq < watermark => {
                if let Some(config) = &mut config {
                    config.apply(patch);
                }
            }
            _ => {}
        }
    }
    config
}

/// The persistent projection of a session's configuration. In
/// `SessionCreated` it is the initial value; `effective_config` folds later
/// changes onto it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub model: ModelSpec,
    /// Only `Part::Text` is valid. In `SessionCreated` the initial system;
    /// folded, it also holds every appended instruction in log order.
    pub system: Vec<Part>,
    /// In `SessionCreated` the tools resolved at creation; folded, the
    /// effective set after every `add_tools` / `remove_tools`.
    pub tools: Vec<ToolDescriptor>,
    pub request: RequestConfig,
    pub turn: TurnConfig,
    pub compaction: CompactionConfig,
    pub subagents: SubagentConfig,
}

impl ConfigSnapshot {
    /// Apply one `ConfigChanged` patch in place.
    pub fn apply(&mut self, patch: &ConfigPatch) {
        if let Some(model) = &patch.model {
            self.model = model.clone();
        }
        if let Some(request) = &patch.request {
            self.request = request.clone();
        }
        self.system.extend(patch.append_system.iter().cloned());
        change_tools(&mut self.tools, &patch.add_tools, &patch.remove_tools);
    }
}

/// Remove `removed`, then add `added`. An added name replaces a tool of the
/// same name, so the fold never holds two tools with one name.
pub(crate) fn change_tools(
    tools: &mut Vec<ToolDescriptor>,
    added: &[ToolDescriptor],
    removed: &[ToolName],
) {
    tools.retain(|tool| {
        !removed.contains(&tool.name) && !added.iter().any(|new| new.name == tool.name)
    });
    tools.extend(added.iter().cloned());
}

#[non_exhaustive]
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
    /// Provider strict schema adherence; `None` sends nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

impl ToolDescriptor {
    /// An in-process tool with the conservative defaults: exclusive,
    /// replay-unsafe, mutating, no timeout, output limit or strict flag.
    pub fn new(name: ToolName, description: impl Into<String>, schema: JsonSchema) -> Self {
        Self {
            name,
            description: description.into(),
            schema,
            parallelism: Parallelism::Exclusive,
            replay: ReplaySafety::Unsafe,
            side_effect: SideEffect::Mutating,
            provenance: Provenance::InProcess,
            timeout: None,
            max_output_bytes: None,
            strict: None,
        }
    }

    #[must_use]
    pub fn with_parallelism(mut self, parallelism: Parallelism) -> Self {
        self.parallelism = parallelism;
        self
    }

    #[must_use]
    pub fn with_replay(mut self, replay: ReplaySafety) -> Self {
        self.replay = replay;
        self
    }

    #[must_use]
    pub fn with_side_effect(mut self, side_effect: SideEffect) -> Self {
        self.side_effect = side_effect;
        self
    }

    #[must_use]
    pub fn with_provenance(mut self, provenance: Provenance) -> Self {
        self.provenance = provenance;
        self
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_max_output_bytes(mut self, max_output_bytes: Option<usize>) -> Self {
        self.max_output_bytes = max_output_bytes;
        self
    }

    #[must_use]
    pub fn with_strict(mut self, strict: Option<bool>) -> Self {
        self.strict = strict;
        self
    }
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

#[cfg(test)]
mod tests {
    use super::ReasoningEffort;

    #[test]
    fn efforts_persisted_before_the_new_levels_still_decode() {
        for (json, effort) in [
            ("\"Low\"", ReasoningEffort::Low),
            ("\"Medium\"", ReasoningEffort::Medium),
            ("\"High\"", ReasoningEffort::High),
        ] {
            assert_eq!(
                serde_json::from_str::<ReasoningEffort>(json).unwrap(),
                effort
            );
        }
    }
}
