//! `usage_of` is the single bucket decision for one log record.

use asterpath_protocol::*;
use asterpath_protocol::{LogId, LogIdRange, StepId, TurnId};

fn turn(byte: u8) -> TurnId {
    TurnId::from_bytes([byte; 16])
}

fn step(byte: u8) -> StepId {
    StepId::from_bytes([byte; 16])
}

fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new("m"),
        context_window: None,
    }
}

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached: input,
            cache_read: 0,
            cache_write: 0,
        },
        output: OutputTokens {
            total: output,
            reasoning: 0,
        },
    }
}

fn applied() -> SessionEvent {
    SessionEvent::CompactionApplied {
        first_kept: LogId::from_i64(5),
        summary: vec![],
        replaces: LogIdRange {
            first: LogId::from_i64(1),
            last: LogId::from_i64(4),
        },
        model: model(),
        usage: usage(50, 5),
        raw: None,
    }
}

fn aborted(usage: Option<Usage>) -> SessionEvent {
    SessionEvent::ModelCallAborted {
        turn_id: turn(1),
        step_id: step(2),
        attempt: 1,
        kind: AbortKind::Transport,
        partial: None,
        model: model(),
        backoff_ms: 0,
        usage,
        error: None,
    }
}

#[test]
fn usage_of_decides_bucket_turn_and_step_per_record() {
    let entry = |bucket, usage, turn, step| {
        Some(UsageEntry {
            bucket,
            usage,
            model: model(),
            turn,
            step,
        })
    };
    let cases: Vec<(&str, SessionEvent, Option<TurnId>, Option<UsageEntry>)> = vec![
        (
            "assistant message is a model call",
            SessionEvent::AssistantMessage {
                turn_id: turn(1),
                step_id: step(2),
                items: vec![],
                raw: None,
                model: model(),
                usage: usage(10, 2),
                stop_reason: StopReason::EndTurn,
            },
            None,
            entry(
                UsageBucket::ModelCall,
                usage(10, 2),
                Some(turn(1)),
                Some(step(2)),
            ),
        ),
        (
            "aborted call with usage is partial",
            aborted(Some(usage(3, 1))),
            None,
            entry(
                UsageBucket::Partial,
                usage(3, 1),
                Some(turn(1)),
                Some(step(2)),
            ),
        ),
        (
            "aborted call without usage carries nothing",
            aborted(None),
            None,
            None,
        ),
        (
            "threshold compaction belongs to its turn",
            applied(),
            Some(turn(1)),
            entry(UsageBucket::Compaction, usage(50, 5), Some(turn(1)), None),
        ),
        (
            "forced compaction belongs to its turn",
            applied(),
            Some(turn(3)),
            entry(UsageBucket::Compaction, usage(50, 5), Some(turn(3)), None),
        ),
        (
            "idle compaction has no turn",
            applied(),
            None,
            entry(UsageBucket::Compaction, usage(50, 5), None, None),
        ),
        (
            "compaction intent carries no usage",
            SessionEvent::CompactionStarted {
                trigger: CompactionTrigger::Threshold,
                replaces_from: LogId::from_i64(1),
                caused_by: LogId::from_i64(4),
                turn: Some(turn(1)),
            },
            Some(turn(1)),
            None,
        ),
        (
            "turn markers carry no usage",
            SessionEvent::TurnStarted { turn_id: turn(1) },
            None,
            None,
        ),
    ];

    for (name, event, compaction_turn, expected) in cases {
        assert_eq!(usage_of(&event, compaction_turn), expected, "{name}");
    }
}
