//! Read-only projection of a session log into model-visible history.
//!
//! `CompactionApplied` is a rewrite operator: it drops the replaced range
//! (including any previous summary) and inserts the new summary immediately
//! before `first_kept`. The raw log is never modified.
//!
//! An instruction, tool change or reasoning change recorded by `ConfigChanged` takes its place
//! at the next `StepStarted` (or the end of the scanned entries), so it always
//! follows the tool results of the step it was recorded in. A compaction that
//! replaces it moves it into the baseline system, tools or reasoning instead of the
//! summary (ADR-0030).

use std::collections::HashMap;

use crate::{LogEntry, LogId, LogIdRange, ToolCallId};

use crate::config::{ReasoningParams, SystemSection, ToolDescriptor, change_tools, set_section};
use crate::decode::{LogDecodeError, decode_entry};
use crate::event::{SessionEvent, StopReason, ToolResultStatus};
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

/// What a model request renders from the log (ADR-0030).
#[derive(Clone, Debug, PartialEq)]
pub struct RequestHistory {
    /// The baseline system: `SessionCreated.config.system` plus every
    /// instruction a compaction has replaced, in log order.
    pub system: Vec<Part>,
    /// The baseline keyed sections, rendered after `system`: the created
    /// sections with each replaced section change applied, by key.
    pub system_sections: Vec<SystemSection>,
    /// The baseline tools: `SessionCreated.config.tools` with every tool
    /// change a compaction has replaced applied, in log order.
    pub tools: Vec<ToolDescriptor>,
    /// The baseline reasoning: `SessionCreated.config.request.reasoning`, or
    /// the last reasoning change a compaction has replaced.
    pub reasoning: Option<ReasoningParams>,
    pub items: Vec<Item>,
}

#[derive(Default)]
struct Baseline {
    system: Vec<Part>,
    sections: Vec<SystemSection>,
    tools: Vec<ToolDescriptor>,
    reasoning: Option<ReasoningParams>,
}

impl Baseline {
    fn absorb(&mut self, items: &[Item]) {
        for item in items {
            match item {
                Item::Instruction { key: None, parts } => self.system.extend(parts.iter().cloned()),
                // The first part is the notice; the rest is the new value.
                Item::Instruction {
                    key: Some(key),
                    parts,
                } => set_section(
                    &mut self.sections,
                    &SystemSection::new(key.clone(), parts[1..].to_vec()),
                ),
                Item::ToolsChanged { added, removed } => {
                    change_tools(&mut self.tools, added, removed);
                }
                Item::ReasoningChanged { reasoning } => self.reasoning.clone_from(reasoning),
                _ => {}
            }
        }
    }
}

/// Fold a set of scanned entries into model-visible history, ordered by seq.
///
/// Tool results are emitted in `ToolCallRequested` order, not log-completion
/// order, so parallel calls reconstruct identically whatever order they finish.
pub fn project(entries: &[LogEntry]) -> Result<Vec<ProjectedEntry>, ProjectionError> {
    Ok(fold(entries)?.0)
}

/// The baseline system and the flattened history a model request carries.
pub fn request_history(entries: &[LogEntry]) -> Result<RequestHistory, ProjectionError> {
    let (history, baseline) = fold(entries)?;
    Ok(RequestHistory {
        system: baseline.system,
        system_sections: baseline.sections,
        tools: baseline.tools,
        reasoning: baseline.reasoning,
        items: history
            .into_iter()
            .flat_map(|projected| projected.items)
            .collect(),
    })
}

fn fold(entries: &[LogEntry]) -> Result<(Vec<ProjectedEntry>, Baseline), ProjectionError> {
    let mut ordered: Vec<&LogEntry> = entries.iter().collect();
    ordered.sort_by_key(|entry| entry.seq);

    let mut history: Vec<ProjectedEntry> = Vec::new();
    let mut intents: HashMap<ToolCallId, (String, u64)> = HashMap::new();
    let mut pending_results: Vec<(u64, ProjectedEntry)> = Vec::new();
    let mut pending_changes: Vec<ProjectedEntry> = Vec::new();
    let mut baseline: Option<Baseline> = None;
    // The effective reasoning, to tell which request patches change it.
    let mut reasoning: Option<ReasoningParams> = None;

    for entry in ordered {
        match decode_entry(entry)? {
            SessionEvent::SessionCreated { config, .. } if baseline.is_none() => {
                reasoning.clone_from(&config.request.reasoning);
                baseline = Some(Baseline {
                    system: config.system,
                    sections: config.system_sections,
                    tools: config.tools,
                    reasoning: config.request.reasoning,
                });
            }
            SessionEvent::ConfigChanged { patch } => {
                let changes_tools = patch.changes_tools();
                let mut items = Vec::new();
                if !patch.append_system.is_empty() {
                    items.push(Item::Instruction {
                        key: None,
                        parts: patch.append_system,
                    });
                }
                items.extend(patch.system_sections.into_iter().map(section_item));
                if changes_tools {
                    items.push(Item::ToolsChanged {
                        added: patch.add_tools,
                        removed: patch.remove_tools,
                    });
                }
                if let Some(request) = patch.request
                    && request.reasoning != reasoning
                {
                    reasoning.clone_from(&request.reasoning);
                    items.push(Item::ReasoningChanged {
                        reasoning: request.reasoning,
                    });
                }
                if !items.is_empty() {
                    pending_changes.push(ProjectedEntry {
                        origin: ProjectedOrigin::Log(entry.log_id),
                        items,
                    });
                }
            }
            SessionEvent::StepStarted { .. } if !pending_changes.is_empty() => {
                flush_results(&mut pending_results, &mut history);
                history.append(&mut pending_changes);
            }
            SessionEvent::ContextForked { items, .. } => {
                flush_results(&mut pending_results, &mut history);
                push(&mut history, entry.log_id, items);
            }
            SessionEvent::InputRecorded { input } => {
                flush_results(&mut pending_results, &mut history);
                push(
                    &mut history,
                    entry.log_id,
                    vec![Item::Message {
                        role: crate::item::Role::User,
                        parts: input,
                        raw: None,
                    }],
                );
            }
            // I9: a refused step's output never reaches history.
            SessionEvent::AssistantMessage {
                items, stop_reason, ..
            } if stop_reason != StopReason::Refusal => {
                flush_results(&mut pending_results, &mut history);
                push(&mut history, entry.log_id, items);
            }
            SessionEvent::ToolCallRequested {
                tool_call_id,
                provider_call_id,
                ..
            } => {
                intents.insert(tool_call_id, (provider_call_id, entry.seq));
            }
            SessionEvent::ToolResult {
                tool_call_id,
                status,
                output,
                error,
                ..
            } => {
                let (call_id, intent_seq) = intents.get(&tool_call_id).cloned().ok_or(
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
                pending_results.push((
                    intent_seq,
                    ProjectedEntry {
                        origin: ProjectedOrigin::Log(entry.log_id),
                        items: vec![Item::ToolResult {
                            call_id,
                            provider_item_id: None,
                            parts,
                            is_error,
                            raw: None,
                        }],
                    },
                ));
            }
            SessionEvent::CompactionApplied {
                first_kept,
                summary,
                replaces,
                ..
            } => {
                flush_results(&mut pending_results, &mut history);
                // Replaced changes join the baseline, never the summary.
                let baseline = baseline.get_or_insert_with(Baseline::default);
                for projected in history.iter().chain(&pending_changes) {
                    if overlaps(&projected.origin, &replaces) {
                        baseline.absorb(&projected.items);
                    }
                }
                history.retain(|projected| !overlaps(&projected.origin, &replaces));
                pending_changes.retain(|projected| !overlaps(&projected.origin, &replaces));
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
    flush_results(&mut pending_results, &mut history);
    history.append(&mut pending_changes);

    Ok((history, baseline.unwrap_or_default()))
}

/// A keyed section change as history renders it: a notice, then the new value.
fn section_item(section: SystemSection) -> Item {
    let notice = if section.parts.is_empty() {
        format!(
            "Instruction section `{}` removed; disregard its earlier text.",
            section.key
        )
    } else {
        format!(
            "Instruction section `{}` (supersedes any earlier text of this section):",
            section.key
        )
    };
    let mut parts = vec![Part::Text { text: notice }];
    parts.extend(section.parts);
    Item::Instruction {
        key: Some(section.key),
        parts,
    }
}

fn flush_results(pending: &mut Vec<(u64, ProjectedEntry)>, history: &mut Vec<ProjectedEntry>) {
    pending.sort_by_key(|(intent_seq, _)| *intent_seq);
    history.extend(pending.drain(..).map(|(_, projected)| projected));
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
