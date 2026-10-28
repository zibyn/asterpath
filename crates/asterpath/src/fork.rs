//! Context-fork snapshots, shared by spawn forks (subagents.md §4.2) and application
//! session forks (ADR-0033): which source records a fork copies, and the
//! projection it materializes from a prefix of the decoded log.

use asterpath_protocol::{Item, LogEntry, Part, ProjectionError, SystemSection};

/// What a fork materializes from its prefix.
pub(crate) struct Snapshot {
    /// The baseline system; the copied history already holds the
    /// instructions appended before the fork point.
    pub(crate) system: Vec<Part>,
    /// The baseline keyed sections, so the fork can still refresh them by key.
    pub(crate) system_sections: Vec<SystemSection>,
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
        system_sections: history.system_sections,
        items,
    })
}
