//! Session facts: one read-only fold of a session's log that answers what
//! recovery, spawn admission, wake discovery and inspection keep asking.
//!
//! The facts are attribution only (seq at open, owning turn/step, parent tool
//! call). How a caller uses them (skip, cancel, retry) stays with the caller.
//!
//! A session is idle exactly when no non-compaction intent is open. A dangling
//! summary intent does not make a session busy because discarding that call is
//! safe.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CompactionTrigger, DedupKey, LogEntry, LogId, ParentRef, SessionId, StepId, Timestamp,
    ToolCallId, ToolName, TurnId,
};

use crate::decode::{LogDecodeError, decode_entry};
use crate::dedup::compaction_frontier;
use crate::event::SessionEvent;

/// One open intent, as `inspect` lists them; compactions use the frontier log
/// id from their dedup key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum OpenIntent {
    Turn(TurnId),
    Step(StepId),
    Tool(ToolCallId),
    Child(SessionId),
    Compaction(LogId),
}

/// The open step and the seq of its `StepStarted` (the projection waterline).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpenStep {
    pub turn: TurnId,
    pub step: StepId,
    pub seq: u64,
}

/// A `ToolCallRequested` with no `ToolResult` yet.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpenTool {
    pub turn: TurnId,
    pub step: StepId,
    pub name: ToolName,
    /// Seq of the `ToolCallRequested`.
    pub seq: u64,
    /// The child spawned under this call, open or not.
    pub spawned: Option<SessionId>,
}

/// A `ChildSessionSpawned` with no `ChildSessionCompleted` yet.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpenChild {
    pub parent_tool_call: ToolCallId,
    pub deadline: Option<Timestamp>,
    /// Seq of the `ChildSessionSpawned`.
    pub seq: u64,
}

/// A keyed `CompactionStarted` with no terminal under the same key.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpenCompaction {
    pub key: DedupKey,
    pub trigger: CompactionTrigger,
    /// Seq of the `CompactionStarted`.
    pub seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionFacts {
    turns: BTreeSet<TurnId>,
    steps: BTreeMap<StepId, OpenStep>,
    tools: BTreeMap<ToolCallId, OpenTool>,
    children: BTreeMap<SessionId, OpenChild>,
    compactions: BTreeMap<DedupKey, OpenCompaction>,
    pending_input: bool,
    parent: Option<ParentRef>,
    last: Option<(u64, LogId)>,
}

impl SessionFacts {
    /// Fold a scanned log in `seq` order.
    pub fn fold(entries: &[LogEntry]) -> Result<Self, LogDecodeError> {
        let mut facts = Self::default();
        let mut ordered: Vec<&LogEntry> = entries.iter().collect();
        ordered.sort_by_key(|entry| entry.seq);
        for entry in ordered {
            let event = decode_entry(entry)?;
            facts.apply(entry.seq, entry.log_id, entry.dedup_key.as_ref(), &event);
        }
        Ok(facts)
    }

    /// Apply one decoded event with its envelope fields. Compaction intents
    /// are keyed by their dedup key, so terminals close exactly that intent.
    pub fn apply(
        &mut self,
        seq: u64,
        log_id: LogId,
        dedup_key: Option<&DedupKey>,
        event: &SessionEvent,
    ) {
        self.last = Some((seq, log_id));
        match event {
            SessionEvent::SessionCreated { parent, .. } => self.parent = parent.clone(),
            SessionEvent::InputRecorded { .. } => self.pending_input = true,
            SessionEvent::TurnStarted { turn_id } => {
                self.turns.insert(*turn_id);
            }
            SessionEvent::TurnCommitted { turn_id, .. } => {
                self.turns.remove(turn_id);
            }
            SessionEvent::StepStarted { turn_id, step_id } => {
                self.pending_input = false;
                self.steps.insert(
                    *step_id,
                    OpenStep {
                        turn: *turn_id,
                        step: *step_id,
                        seq,
                    },
                );
            }
            SessionEvent::StepCommitted { step_id, .. } => {
                self.steps.remove(step_id);
            }
            SessionEvent::ToolCallRequested {
                turn_id,
                step_id,
                tool_call_id,
                name,
                ..
            } => {
                self.tools.insert(
                    *tool_call_id,
                    OpenTool {
                        turn: *turn_id,
                        step: *step_id,
                        name: name.clone(),
                        seq,
                        spawned: None,
                    },
                );
            }
            SessionEvent::ToolResult { tool_call_id, .. } => {
                self.tools.remove(tool_call_id);
            }
            SessionEvent::ChildSessionSpawned {
                child,
                parent_tool_call_id,
                deadline,
                ..
            } => {
                if let Some(tool) = self.tools.get_mut(parent_tool_call_id) {
                    tool.spawned = Some(*child);
                }
                self.children.insert(
                    *child,
                    OpenChild {
                        parent_tool_call: *parent_tool_call_id,
                        deadline: *deadline,
                        seq,
                    },
                );
            }
            SessionEvent::ChildSessionCompleted { child, .. } => {
                self.children.remove(child);
            }
            SessionEvent::CompactionStarted { trigger, .. } => {
                if let Some(key) = dedup_key {
                    self.compactions.insert(
                        key.clone(),
                        OpenCompaction {
                            key: key.clone(),
                            trigger: *trigger,
                            seq,
                        },
                    );
                }
            }
            SessionEvent::CompactionApplied { .. } | SessionEvent::CompactionAborted { .. } => {
                if let Some(key) = dedup_key {
                    self.compactions.remove(key);
                }
            }
            _ => {}
        }
    }

    pub fn is_idle(&self) -> bool {
        self.turns.is_empty()
            && self.steps.is_empty()
            && self.tools.is_empty()
            && self.children.is_empty()
    }

    /// Wake discovery (ADR-0023): a non-compaction intent is open or an input
    /// waits for its step.
    pub fn is_wakeable(&self) -> bool {
        !self.is_idle() || self.pending_input
    }

    /// Classify the session state.
    pub fn state(&self) -> SessionState {
        match self.turns.iter().next() {
            Some(turn) => SessionState::TurnOpen {
                turn: *turn,
                step: (!self.steps.is_empty()).then_some(if self.children.is_empty() {
                    StepState::InProgress
                } else {
                    StepState::Waiting
                }),
            },
            None => SessionState::Idle,
        }
    }

    /// The latest started step that is not committed.
    pub fn open_step(&self) -> Option<OpenStep> {
        self.steps.values().max_by_key(|open| open.seq).copied()
    }

    pub fn open_tools(&self) -> impl Iterator<Item = (ToolCallId, &OpenTool)> {
        self.tools.iter().map(|(id, tool)| (*id, tool))
    }

    pub fn open_tool(&self, tool_call: ToolCallId) -> Option<&OpenTool> {
        self.tools.get(&tool_call)
    }

    pub fn open_children(&self) -> impl Iterator<Item = (SessionId, &OpenChild)> {
        self.children.iter().map(|(id, child)| (*id, child))
    }

    pub fn open_compactions(&self) -> impl Iterator<Item = &OpenCompaction> {
        self.compactions.values()
    }

    /// An `InputRecorded` with no `StepStarted` after it.
    pub fn has_pending_input(&self) -> bool {
        self.pending_input
    }

    /// The parent recorded in `SessionCreated`; `None` for a root session.
    pub fn parent(&self) -> Option<&ParentRef> {
        self.parent.as_ref()
    }

    /// Seq and log id of the last folded record.
    pub fn last(&self) -> Option<(u64, LogId)> {
        self.last
    }

    /// Every open intent, ordered by kind then id.
    pub fn open_intents(&self) -> Vec<OpenIntent> {
        let turns = self.turns.iter().map(|id| OpenIntent::Turn(*id));
        let steps = self.steps.keys().map(|id| OpenIntent::Step(*id));
        let tools = self.tools.keys().map(|id| OpenIntent::Tool(*id));
        let children = self.children.keys().map(|id| OpenIntent::Child(*id));
        let mut compactions: Vec<OpenIntent> = self
            .compactions
            .keys()
            .filter_map(compaction_frontier)
            .map(OpenIntent::Compaction)
            .collect();
        compactions.sort();
        turns
            .chain(steps)
            .chain(tools)
            .chain(children)
            .chain(compactions)
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Idle,
    TurnOpen {
        turn: TurnId,
        step: Option<StepState>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepState {
    InProgress,
    Waiting,
}
