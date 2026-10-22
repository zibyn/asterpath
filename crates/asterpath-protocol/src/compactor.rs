//! Optional compaction capability: the two-phase `Compactor` trait, its pure
//! planning input and the values exchanged with the agent loop.
//!
//! The compactor is read-only. The agent writes `CompactionStarted` after
//! `plan` returns `Summarize` and writes the terminal event from `compact`'s
//! result, so there is exactly one writer and one commit point.

use asterpath_core::{LogId, LogIdRange, ServiceKey, service_key};
use futures_core::future::BoxFuture;

use crate::agent::{CompactionError, CompactionOutcome};
use crate::config::{CompactionConfig, ModelSpec, TurnConfig};
use crate::event::CompactionTrigger;
use crate::item::Item;
use crate::projection::ProjectedEntry;
use crate::usage::Usage;

/// One compaction attempt's view of the session.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionInput {
    pub trigger: CompactionTrigger,
    /// The model of the next request; the only source of the context window.
    pub model: ModelSpec,
    pub config: CompactionConfig,
    /// Attempt, backoff and request timeout for the summary call.
    pub turn: TurnConfig,
    /// Model-visible projection, in order.
    pub history: Vec<ProjectedEntry>,
    /// `StepStarted` log ids of committed steps, ascending. The open step is
    /// never a cut point.
    pub step_starts: Vec<LogId>,
    /// The latest successful `AssistantMessage` still in the projection.
    pub last_usage: Option<UsageAnchor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageAnchor {
    pub origin: LogId,
    pub usage: Usage,
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CompactPlan {
    /// Threshold not reached; only the `Threshold` trigger returns this.
    NotNeeded,
    NothingToDo,
    /// No `ModelSpec.context_window` configured.
    NotConfigured,
    Summarize(CompactionJob),
}

/// Everything `compact` needs; produced by the pure `plan`.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionJob {
    /// A committed step boundary.
    pub first_kept: LogId,
    pub replaces: LogIdRange,
    /// The model actually used for the summary call.
    pub model: ModelSpec,
    /// Text-only view of the replaced range.
    pub summary_input: Vec<Item>,
    pub max_summary_tokens: u32,
    /// Derived from the session model, not the summary model.
    pub usable_window: u32,
    /// Estimated tokens retained from `first_kept` onwards.
    pub retained_tokens: u64,
    /// Attempt, backoff and request timeout for the summary call. Carried here
    /// because `compact` receives only the job, not the planning input.
    pub turn: TurnConfig,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)] // shape fixed by protocol.md §6.1
#[non_exhaustive]
pub enum CompactSummary {
    Applied(CompactionOutcome),
    Insufficient,
}

pub trait Compactor: Send + Sync {
    /// Pure: decides whether to compact and where to cut. No I/O.
    fn plan(&self, input: &CompactionInput) -> CompactPlan;

    /// Runs the summary call and the post-check. Only called for
    /// `CompactPlan::Summarize`.
    fn compact<'a>(
        &'a self,
        job: CompactionJob,
    ) -> BoxFuture<'a, Result<CompactSummary, CompactionError>>;
}

pub const COMPACTOR_SERVICE: ServiceKey<dyn Compactor> = service_key("Compactor");
