//! Open-intent folding shared by recovery classification and storage indexes.
//!
//! A session is idle exactly when no non-compaction intent is open. A dangling
//! summary intent does not make a session busy because discarding that call is
//! safe.

use std::collections::BTreeSet;

use asterpath_core::{
    DedupKey, LogEntry, SessionId, SignalId, StepId, TimerId, ToolCallId, TurnId,
};

use crate::decode::{LogDecodeError, decode_entry};
use crate::event::SessionEvent;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenIntents {
    turns: BTreeSet<TurnId>,
    steps: BTreeSet<StepId>,
    tools: BTreeSet<ToolCallId>,
    timers: BTreeSet<TimerId>,
    children: BTreeSet<SessionId>,
    signals: BTreeSet<SignalId>,
    compactions: BTreeSet<DedupKey>,
}

impl OpenIntents {
    /// Apply one decoded event. Compaction intents are keyed by their dedup key
    /// (`compaction:<frontier>`), so terminals close exactly the started intent.
    pub fn apply(&mut self, event: &SessionEvent, dedup_key: Option<&DedupKey>) {
        match event {
            SessionEvent::TurnStarted { turn_id } => {
                self.turns.insert(*turn_id);
            }
            SessionEvent::TurnCommitted { turn_id, .. } => {
                self.turns.remove(turn_id);
            }
            SessionEvent::StepStarted { step_id, .. } => {
                self.steps.insert(*step_id);
            }
            SessionEvent::StepCommitted { step_id, .. } => {
                self.steps.remove(step_id);
            }
            SessionEvent::ToolCallRequested { tool_call_id, .. } => {
                self.tools.insert(*tool_call_id);
            }
            SessionEvent::ToolResult { tool_call_id, .. } => {
                self.tools.remove(tool_call_id);
            }
            SessionEvent::TimerScheduled { timer_id, .. } => {
                self.timers.insert(*timer_id);
            }
            SessionEvent::TimerFired { timer_id } | SessionEvent::TimerCancelled { timer_id } => {
                self.timers.remove(timer_id);
            }
            SessionEvent::ChildSessionSpawned { child, .. } => {
                self.children.insert(*child);
            }
            SessionEvent::ChildSessionCompleted { child, .. } => {
                self.children.remove(child);
            }
            SessionEvent::SignalAwaited { signal_id, .. } => {
                self.signals.insert(*signal_id);
            }
            SessionEvent::SignalReceived { signal_id, .. } => {
                self.signals.remove(signal_id);
            }
            SessionEvent::CompactionStarted { .. } => {
                if let Some(key) = dedup_key {
                    self.compactions.insert(key.clone());
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

    /// Fold a scanned log into its set of open intents.
    pub fn fold(entries: &[LogEntry]) -> Result<Self, LogDecodeError> {
        let mut intents = Self::default();
        let mut ordered: Vec<&LogEntry> = entries.iter().collect();
        ordered.sort_by_key(|entry| entry.seq);
        for entry in ordered {
            let event = decode_entry(entry)?;
            intents.apply(&event, entry.dedup_key.as_ref());
        }
        Ok(intents)
    }

    pub fn is_idle(&self) -> bool {
        self.turns.is_empty()
            && self.steps.is_empty()
            && self.tools.is_empty()
            && self.timers.is_empty()
            && self.children.is_empty()
            && self.signals.is_empty()
    }

    pub fn compactions_are_open(&self) -> bool {
        !self.compactions.is_empty()
    }

    /// Classify the session state from the same fold used by recovery.
    pub fn state(&self) -> SessionState {
        match self.turns.iter().next() {
            Some(turn) => SessionState::TurnOpen {
                turn: *turn,
                step: self.steps.iter().next().map(|_| {
                    if self.timers.is_empty() && self.children.is_empty() && self.signals.is_empty()
                    {
                        StepState::InProgress
                    } else {
                        StepState::Waiting
                    }
                }),
            },
            None => SessionState::Idle,
        }
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
