//! Optional compaction capability: the two-phase `Compactor` trait, its pure
//! planning input and the values exchanged with the agent loop.
//!
//! The compactor is read-only. The agent writes `CompactionStarted` after
//! `plan` returns `Summarize` and writes the terminal event from `compact`'s
//! result, so there is exactly one writer and one commit point.

use crate::{LogId, LogIdRange, SessionContext};
use futures_core::future::BoxFuture;

use crate::agent::{CompactionError, CompactionOutcome};
use crate::config::{CacheTtl, CompactionConfig, ModelSpec, TurnConfig};
use crate::event::CompactionTrigger;
use crate::item::Item;
use crate::projection::ProjectedEntry;
use crate::provider::ModelRequest;
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
    /// The session's request at this projection, rendered from `history`.
    /// `None` makes the summary call an independent request.
    pub request: Option<ModelRequest>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageAnchor {
    pub origin: LogId,
    pub usage: Usage,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)] // one plan per compaction, never stored
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
    /// The session compacted, for the summary request's
    /// [`ModelRequest::session`](crate::ModelRequest::session). `plan` leaves
    /// it `None`; the agent fills it in before `compact`.
    pub session: Option<SessionContext>,
    /// `CompactionInput.request` cut to the replaced range, so the summary
    /// call shares the session's cached prefix (#176). Used only when its
    /// model is `model`; otherwise the summary call is independent.
    pub prefix: Option<ModelRequest>,
    /// [`CompactionConfig::cache_ttl`]: the summary request's TTL whether or
    /// not it reuses `prefix`.
    pub cache_ttl: CacheTtl,
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
