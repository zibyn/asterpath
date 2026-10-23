//! Token usage in the exclusive normalization, plus the native usage-query
//! contract: buckets, scopes, records and rollup totals.

use asterpath_core::{LogId, SessionId, StepId, TurnId};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::config::ModelSpec;
use crate::event::SessionEvent;
use crate::ids::ModelId;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input: InputTokens,
    pub output: OutputTokens,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputTokens {
    pub uncached: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputTokens {
    /// Includes reasoning tokens.
    pub total: u64,
    /// Subset of `total`; never added on top.
    pub reasoning: u64,
}

impl Usage {
    pub fn input_total(&self) -> u64 {
        self.input.uncached + self.input.cache_read + self.input.cache_write
    }

    pub fn total(&self) -> u64 {
        self.input_total() + self.output.total
    }
}

/// Where one usage sample came from. Only `ModelCall` and `Compaction` enter
/// the total; `Partial` is a diagnostic lower bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UsageBucket {
    /// A successful `AssistantMessage`.
    ModelCall,
    /// A successful `CompactionApplied`.
    Compaction,
    /// A failed or interrupted attempt's best-effort usage.
    Partial,
}

/// What one usage-carrying record counts as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageEntry {
    pub bucket: UsageBucket,
    pub usage: Usage,
    pub model: ModelSpec,
    pub turn: Option<TurnId>,
    pub step: Option<StepId>,
}

/// The single bucket decision for one record; `None` when it carries no usage.
///
/// `compaction_turn` is the `turn` recorded on the `CompactionStarted` sharing
/// this record's dedup key; it is read only for `CompactionApplied`.
pub fn usage_of(event: &SessionEvent, compaction_turn: Option<TurnId>) -> Option<UsageEntry> {
    let (bucket, usage, model, turn, step) = match event {
        SessionEvent::AssistantMessage {
            turn_id,
            step_id,
            model,
            usage,
            ..
        } => (
            UsageBucket::ModelCall,
            usage,
            model,
            Some(*turn_id),
            Some(*step_id),
        ),
        SessionEvent::CompactionApplied { model, usage, .. } => {
            (UsageBucket::Compaction, usage, model, compaction_turn, None)
        }
        SessionEvent::ModelCallAborted {
            turn_id,
            step_id,
            model,
            usage: Some(usage),
            ..
        } => (
            UsageBucket::Partial,
            usage,
            model,
            Some(*turn_id),
            Some(*step_id),
        ),
        _ => return None,
    };
    Some(UsageEntry {
        bucket,
        usage: *usage,
        model: model.clone(),
        turn,
        step,
    })
}

/// The slice of the log a query folds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsageScope {
    Session(SessionId),
    /// The only scope that includes descendants.
    SessionSubtree(SessionId),
    Turn {
        session: SessionId,
        turn: TurnId,
    },
    Step {
        session: SessionId,
        turn: TurnId,
        step: StepId,
    },
}

/// Filter dimensions; all optional fields AND together.
#[derive(Clone, Debug, PartialEq)]
pub struct UsageFilter {
    pub scope: UsageScope,
    pub bucket: Option<UsageBucket>,
    pub model: Option<ModelId>,
    /// Inclusive; read from the log envelope's `recorded_at`.
    pub since: Option<asterpath_core::Timestamp>,
    /// Exclusive; read from the log envelope's `recorded_at`.
    pub until: Option<asterpath_core::Timestamp>,
    /// Only applies to `records()`; `totals()` ignores it.
    pub limit: Option<usize>,
}

/// One usage sample, anchored to the terminal record that produced it.
#[derive(Clone, Debug, PartialEq)]
pub struct UsageRecord {
    pub session: SessionId,
    pub turn: Option<TurnId>,
    pub step: Option<StepId>,
    pub model: ModelSpec,
    pub bucket: UsageBucket,
    pub usage: Usage,
    pub log: LogId,
    pub recorded_at: asterpath_core::Timestamp,
}

/// Per-bucket sums. `partial` never contributes to [`Self::total`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageTotals {
    pub model_calls: Usage,
    pub compaction: Usage,
    /// Diagnostic only; not part of the total.
    pub partial: Usage,
}

impl UsageTotals {
    /// `model_calls + compaction`; `partial` is deliberately excluded.
    pub fn total(&self) -> u64 {
        self.model_calls.total() + self.compaction.total()
    }
}

/// Own usage and, for `SessionSubtree`, its descendants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageReport {
    pub own: UsageTotals,
    pub descendants: UsageTotals,
}

/// Read-side capability: a pure fold over the event log.
pub trait UsageQuery: Send + Sync {
    fn records(&self, filter: UsageFilter) -> BoxStream<'_, Result<UsageRecord, UsageError>>;

    fn totals(&self, filter: UsageFilter) -> BoxFuture<'_, Result<UsageReport, UsageError>>;
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum UsageError {
    #[error("session {session:?} does not exist or has been retired")]
    NotFound { session: SessionId },
    #[error("corrupt session log for {session:?}: {detail}")]
    Corrupt { session: SessionId, detail: String },
    #[error("usage backend error: {0}")]
    Backend(String),
}
