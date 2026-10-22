//! Construction of the canonical dedup keys from `event-log-recovery.md` §4.
//!
//! The key alone is not the identity: uniqueness is
//! `(session, event_kind, dedup_key)`, so two event kinds may deliberately
//! share one key (for example `ToolCallRequested` and `ToolResult`).

use asterpath_core::{
    DedupKey, InputId, LogId, SessionId, SignalId, StepId, TimerId, ToolCallId, TurnId,
};

pub fn session_created(session: SessionId) -> DedupKey {
    DedupKey::new(session.to_hex())
}

pub fn context_forked(child: SessionId) -> DedupKey {
    DedupKey::new(child.to_hex())
}

pub fn input_recorded(input: InputId) -> DedupKey {
    DedupKey::new(input.to_hex())
}

pub fn assistant_message(turn: TurnId, step: StepId) -> DedupKey {
    DedupKey::new(format!("{}/{}", turn.to_hex(), step.to_hex()))
}

pub fn tool_call(tool_call: ToolCallId) -> DedupKey {
    DedupKey::new(tool_call.to_hex())
}

pub fn tool_result(tool_call: ToolCallId) -> DedupKey {
    DedupKey::new(tool_call.to_hex())
}

pub fn step_committed(step: StepId) -> DedupKey {
    DedupKey::new(step.to_hex())
}

pub fn turn_committed(turn: TurnId) -> DedupKey {
    DedupKey::new(turn.to_hex())
}

/// Shared by `CompactionStarted`, `CompactionApplied` and `CompactionAborted`.
pub fn compaction(frontier: LogId) -> DedupKey {
    DedupKey::new(format!("compaction:{}", frontier.as_i64()))
}

pub fn child_session(child: SessionId) -> DedupKey {
    DedupKey::new(child.to_hex())
}

pub fn timer(timer: TimerId) -> DedupKey {
    DedupKey::new(timer.to_hex())
}

pub fn signal(signal: SignalId) -> DedupKey {
    DedupKey::new(signal.to_hex())
}
