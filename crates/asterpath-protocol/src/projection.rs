//! Read-only projection of a session log into model-visible history.
//!
//! `CompactionApplied` is a rewrite operator: it drops the replaced range
//! (including any previous summary) and inserts the new summary immediately
//! before `first_kept`. The raw log is never modified.

use std::collections::HashMap;

use asterpath_core::{LogEntry, LogId, LogIdRange, ToolCallId};

use crate::decode::{LogDecodeError, decode_entry};
use crate::event::{SessionEvent, ToolResultStatus};
use crate::item::{Item, Part};

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedEntry {
    pub origin: ProjectedOrigin,
    pub items: Vec<Item>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProjectedOrigin {
    Log(LogId),
    Summary(LogIdRange),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProjectionError {
    #[error(transparent)]
    Decode(#[from] LogDecodeError),
    #[error("tool result refers to unknown tool call {tool_call:?}")]
    UnknownToolCall { tool_call: ToolCallId },
}

/// Fold a set of scanned entries into model-visible history, ordered by seq.
pub fn project(entries: &[LogEntry]) -> Result<Vec<ProjectedEntry>, ProjectionError> {
    let mut ordered: Vec<&LogEntry> = entries.iter().collect();
    ordered.sort_by_key(|entry| entry.seq);

    let mut history: Vec<ProjectedEntry> = Vec::new();
    let mut provider_calls: HashMap<ToolCallId, String> = HashMap::new();

    for entry in ordered {
        match decode_entry(entry)? {
            SessionEvent::ContextForked { items, .. } => push(&mut history, entry.log_id, items),
            SessionEvent::InputRecorded { input } => push(
                &mut history,
                entry.log_id,
                vec![Item::Message {
                    role: crate::item::Role::User,
                    parts: input,
                }],
            ),
            SessionEvent::AssistantMessage { items, .. } => push(&mut history, entry.log_id, items),
            SessionEvent::ToolCallRequested {
                tool_call_id,
                provider_call_id,
                ..
            } => {
                provider_calls.insert(tool_call_id, provider_call_id);
            }
            SessionEvent::ToolResult {
                tool_call_id,
                status,
                output,
                error,
                ..
            } => {
                let call_id = provider_calls.get(&tool_call_id).cloned().ok_or(
                    ProjectionError::UnknownToolCall {
                        tool_call: tool_call_id,
                    },
                )?;
                let (parts, is_error) = match status {
                    ToolResultStatus::Completed => (output.unwrap_or_default(), false),
                    ToolResultStatus::Failed => (
                        vec![Part::Text {
                            text: error.map(|info| info.message).unwrap_or_default(),
                        }],
                        true,
                    ),
                    ToolResultStatus::Interrupted => (
                        // I9: only an explicit Interrupted result enters history, never partials.
                        vec![Part::Text {
                            text: "tool call interrupted".to_owned(),
                        }],
                        true,
                    ),
                };
                push(
                    &mut history,
                    entry.log_id,
                    vec![Item::ToolResult {
                        call_id,
                        provider_item_id: None,
                        parts,
                        is_error,
                        raw: None,
                    }],
                );
            }
            SessionEvent::CompactionApplied {
                first_kept,
                summary,
                replaces,
                ..
            } => {
                history.retain(|projected| !overlaps(&projected.origin, &replaces));
                let index = history
                    .iter()
                    .position(|projected| match projected.origin {
                        ProjectedOrigin::Log(id) => id > first_kept,
                        ProjectedOrigin::Summary(range) => range.last > first_kept,
                    })
                    .unwrap_or(history.len());
                history.insert(
                    index,
                    ProjectedEntry {
                        origin: ProjectedOrigin::Summary(replaces),
                        items: vec![Item::Compaction { parts: summary }],
                    },
                );
            }
            _ => {}
        }
    }

    Ok(history)
}

/// Flatten a projection into the ordered item list a model request carries.
pub fn history(entries: &[LogEntry]) -> Result<Vec<Item>, ProjectionError> {
    Ok(project(entries)?
        .into_iter()
        .flat_map(|projected| projected.items)
        .collect())
}

fn push(history: &mut Vec<ProjectedEntry>, log_id: LogId, items: Vec<Item>) {
    history.push(ProjectedEntry {
        origin: ProjectedOrigin::Log(log_id),
        items,
    });
}

fn overlaps(origin: &ProjectedOrigin, range: &LogIdRange) -> bool {
    match origin {
        ProjectedOrigin::Log(id) => *id >= range.first && *id <= range.last,
        ProjectedOrigin::Summary(summary) => {
            summary.first <= range.last && range.first <= summary.last
        }
    }
}
