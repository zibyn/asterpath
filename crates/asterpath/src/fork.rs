//! Context-fork snapshots, shared by spawn forks (subagents.md §4.2) and application
//! session forks (ADR-0033): which source records a fork copies, and the
//! projection it materializes.

use asterpath_protocol::{Item, LogEntry, LogId, Part, ProjectionError, SessionEvent};

/// What a fork materializes from its prefix.
pub(crate) struct Snapshot {
    /// The baseline system; the copied history already holds the
    /// instructions appended before the fork point.
    pub(crate) system: Vec<Part>,
    /// The projected history without `ToolsChanged` or `ReasoningChanged`:
    /// the fork's created tools and request are already the effective ones,
    /// so copied changes would re-add removed tools (ADR-0030).
    pub(crate) items: Vec<Item>,
}

pub(crate) fn snapshot(prefix: &[LogEntry]) -> Result<Snapshot, ProjectionError> {
    let history = asterpath_protocol::request_history(prefix)?;
    let mut items = history.items;
    items.retain(|item| {
        !matches!(
            item,
            Item::ToolsChanged { .. } | Item::ReasoningChanged { .. }
        )
    });
    Ok(Snapshot {
        system: history.system,
        items,
    })
}

/// A spawn fork's prefix at `at`, the spawn step's `StepStarted`.
/// `CompactionApplied` folds regardless of `at`, mirroring the agent's
/// request projection. `None` when `at` is not in `entries`.
pub(crate) fn spawn_prefix(
    entries: &[(LogEntry, SessionEvent)],
    at: LogId,
) -> Option<Vec<LogEntry>> {
    let at_seq = entries.iter().find(|(entry, _)| entry.log_id == at)?.0.seq;
    Some(
        entries
            .iter()
            .filter(|(entry, event)| {
                entry.seq <= at_seq || matches!(event, SessionEvent::CompactionApplied { .. })
            })
            .map(|(entry, _)| entry.clone())
            .collect(),
    )
}

/// A session fork's waterline and prefix (ADR-0033). The waterline is the
/// last `TurnCommitted`, or `SessionCreated` when none has committed; nothing
/// after it is copied, compactions included, so a turn in progress never
/// leaks. Inputs recorded after the waterline turn's last `StepStarted`
/// belong to the next turn. `None` for an empty log.
pub(crate) fn session_prefix(
    entries: &[(LogEntry, SessionEvent)],
) -> Option<(LogEntry, Vec<LogEntry>)> {
    let waterline = entries
        .iter()
        .rev()
        .find(|(_, event)| matches!(event, SessionEvent::TurnCommitted { .. }))
        .or_else(|| entries.first())?
        .0
        .clone();
    let last_step = entries
        .iter()
        .filter(|(entry, event)| {
            entry.seq <= waterline.seq && matches!(event, SessionEvent::StepStarted { .. })
        })
        .map(|(entry, _)| entry.seq)
        .max()
        .unwrap_or(0);
    let prefix = entries
        .iter()
        .filter(|(entry, event)| match event {
            SessionEvent::InputRecorded { .. } => entry.seq <= last_step,
            _ => entry.seq <= waterline.seq,
        })
        .map(|(entry, _)| entry.clone())
        .collect();
    Some((waterline, prefix))
}
