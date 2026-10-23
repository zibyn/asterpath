//! Public-behaviour tests for the deterministic trace projection (T1–T13).
//!
//! The log is built directly so the expected span tree is an independent
//! source of truth; no private folding helpers are exercised.

use std::sync::Arc;

use crate::views::trace::TracingQuery;
use asterpath_protocol::{
    AbortKind, ApprovalDecision, ChildOutcome, CompactionTrigger, ConfigSnapshot, ContextPolicy,
    ErrorType, InputTokens, Item, JsonString, ModelId, ModelSpec, OutputTokens, Parallelism,
    ParentRef, Part, Provenance, ProviderId, ReplaySafety, RequestConfig, ResumeReason, Role,
    SessionEvent, SideEffect, Span, SpanAttributes, SpanFilter, SpanKind, SpanStatus, StopReason,
    SubagentConfig, ToolDescriptor, ToolErrorInfo, ToolErrorKind, ToolName, ToolResultStatus,
    TraceError, TurnConfig, TurnEndReason, Usage, WaitKind, approval_signal_id,
    approval_signal_name, decode_entry, dedup, encode_entry,
};
use asterpath_protocol::{
    DedupKey, EventLog, LogEntry, LogId, LogQuery, SessionId, StepId, ToolCallId, TurnId,
};
use asterpath_testing::InMemoryEventLog;
use futures_util::StreamExt;
use futures_util::TryStreamExt;

fn sid(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn turn() -> TurnId {
    TurnId::from_bytes([0x21; 16])
}

fn step(byte: u8) -> StepId {
    StepId::from_bytes([byte; 16])
}

fn call(byte: u8) -> ToolCallId {
    ToolCallId::from_bytes([byte; 16])
}

fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new("model-1"),
        context_window: Some(100_000),
    }
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn descriptor(name: &str) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::Mutating,
        provenance: Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }
}

fn config() -> ConfigSnapshot {
    ConfigSnapshot {
        model: model(),
        system: vec![],
        tools: vec![descriptor("echo")],
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: Default::default(),
        subagents: SubagentConfig::default(),
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

fn created(_session: SessionId) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: config(),
        parent: None,
    }
}

fn assistant(turn: TurnId, step: StepId, parts: Vec<Part>) -> SessionEvent {
    SessionEvent::AssistantMessage {
        turn_id: turn,
        step_id: step,
        items: vec![Item::Message {
            role: Role::Assistant,
            parts,
        }],
        raw: None,
        model: model(),
        usage: usage(10, 5),
        stop_reason: StopReason::EndTurn,
    }
}

fn aborted(
    turn: TurnId,
    step: StepId,
    attempt: u32,
    kind: AbortKind,
    backoff_ms: u64,
) -> SessionEvent {
    SessionEvent::ModelCallAborted {
        turn_id: turn,
        step_id: step,
        attempt,
        kind,
        partial: None,
        model: model(),
        backoff_ms,
        usage: None,
    }
}

fn tool_requested(turn: TurnId, step: StepId, id: ToolCallId, name: &str) -> SessionEvent {
    SessionEvent::ToolCallRequested {
        turn_id: turn,
        step_id: step,
        tool_call_id: id,
        provider_call_id: format!("provider-{name}"),
        name: ToolName::new(name),
        raw_args: JsonString::new("{}"),
        replay: ReplaySafety::Safe,
        provenance: Provenance::InProcess,
    }
}

fn tool_result(id: ToolCallId, status: ToolResultStatus) -> SessionEvent {
    let error = match status {
        ToolResultStatus::Failed => Some(ToolErrorInfo {
            kind: ToolErrorKind::Denied,
            message: "denied".to_owned(),
        }),
        _ => None,
    };
    SessionEvent::ToolResult {
        tool_call_id: id,
        status,
        output: (status == ToolResultStatus::Completed).then(|| vec![text("ok")]),
        error,
        truncated: false,
    }
}

fn compaction_applied(from: LogId, to: LogId) -> SessionEvent {
    SessionEvent::CompactionApplied {
        first_kept: to,
        summary: vec![text("summary")],
        replaces: asterpath_protocol::LogIdRange {
            first: from,
            last: to,
        },
        model: model(),
        usage: usage(7, 3),
        raw: None,
    }
}

async fn append(
    log: &dyn EventLog,
    session: SessionId,
    event: &SessionEvent,
    key: Option<DedupKey>,
) -> LogId {
    let entry = encode_entry(session, key, event).expect("encode");
    log.append(entry).await.expect("append")
}

async fn entries(log: &dyn EventLog, session: SessionId) -> Vec<LogEntry> {
    log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    })
    .try_collect::<Vec<_>>()
    .await
    .expect("scan")
}

async fn fold(log: &Arc<dyn EventLog>, session: SessionId) -> Vec<Span> {
    let query = TracingQuery::new(Arc::clone(log));
    let mut stream = query.spans(SpanFilter {
        session: Some(session),
        ..SpanFilter::default()
    });
    let mut spans = Vec::new();
    while let Some(span) = stream.next().await {
        spans.push(span.expect("span"));
    }
    spans
}

fn span_of(spans: &[Span], kind: SpanKind) -> &Span {
    spans
        .iter()
        .find(|span| span.kind == kind)
        .unwrap_or_else(|| panic!("missing {kind:?} span"))
}

fn all_of(spans: &[Span], kind: SpanKind) -> Vec<&Span> {
    spans.iter().filter(|span| span.kind == kind).collect()
}

#[tokio::test]
async fn t2_hierarchy_is_session_turn_step_model_and_tool() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(1);
    let t = turn();
    let s1 = step(0x31);
    let s2 = step(0x32);
    let c = call(0x41);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s1,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s1, vec![text("calling")]),
        Some(dedup::assistant_message(t, s1)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_requested(t, s1, c, "echo"),
        Some(dedup::tool_call(c)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_result(c, ToolResultStatus::Completed),
        Some(dedup::tool_result(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s1,
        },
        Some(dedup::step_committed(s1)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s2,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s2, vec![text("done")]),
        Some(dedup::assistant_message(t, s2)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s2,
        },
        Some(dedup::step_committed(s2)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let recorded = entries(&*log, session).await;
    let spans = fold(&log, session).await;

    let session_span = span_of(&spans, SpanKind::Session);
    let turn_span = span_of(&spans, SpanKind::Turn);
    assert_eq!(session_span.parent_span, None);
    assert_eq!(session_span.ended_at, None);
    assert_eq!(turn_span.parent_span, Some(session_span.span));
    assert_eq!(turn_span.turn, Some(t));

    let steps = all_of(&spans, SpanKind::Step);
    assert_eq!(steps.len(), 2);
    for (index, step_span) in steps.iter().enumerate() {
        assert_eq!(step_span.parent_span, Some(turn_span.span));
        assert_eq!(step_span.step, Some(step(0x31 + index as u8)));
        assert_eq!(step_span.name, format!("step {}", index + 1));
        assert_eq!(step_span.status, SpanStatus::Ok);
        let start = recorded
            .iter()
            .find(|entry| entry.log_id == step_span.log_range.first)
            .unwrap();
        assert_eq!(step_span.started_at, start.recorded_at);
    }

    let models = all_of(&spans, SpanKind::ModelCall);
    assert_eq!(models.len(), 2);
    for model_span in &models {
        let parent = steps
            .iter()
            .find(|step_span| step_span.step == model_span.step)
            .unwrap();
        assert_eq!(model_span.parent_span, Some(parent.span));
        assert_eq!(model_span.status, SpanStatus::Ok);
        assert_eq!(model_span.name, "chat model-1");
    }

    let tool_span = span_of(&spans, SpanKind::ToolCall);
    let first_step = steps[0];
    assert_eq!(tool_span.parent_span, Some(first_step.span));
    assert_eq!(tool_span.tool_call, Some(c));
    assert_eq!(tool_span.name, "execute_tool echo");
    assert_eq!(tool_span.status, SpanStatus::Ok);
    assert!(matches!(
        &tool_span.attributes,
        SpanAttributes::ToolCall(attrs)
            if attrs.name == ToolName::new("echo") && attrs.side_effect == SideEffect::Mutating
    ));

    assert_eq!(turn_span.status, SpanStatus::Ok);
    let terminal = recorded
        .iter()
        .find(|entry| entry.log_id == turn_span.log_range.last)
        .unwrap();
    assert_eq!(turn_span.ended_at, Some(terminal.recorded_at));
}

#[tokio::test]
async fn t1_rebuilding_the_same_log_twice_is_identical() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(2);
    let t = turn();
    let s = step(0x51);
    let c = call(0x52);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &aborted(t, s, 1, AbortKind::Transport, 25),
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("ok")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_requested(t, s, c, "echo"),
        Some(dedup::tool_call(c)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_result(c, ToolResultStatus::Completed),
        Some(dedup::tool_result(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let first = fold(&log, session).await;
    let second = fold(&log, session).await;
    assert_eq!(first, second, "the same log must fold to the same trace");
}

#[tokio::test]
async fn t3_retries_fold_into_one_model_call_with_attempts() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(3);
    let t = turn();
    let s = step(0x61);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &aborted(t, s, 1, AbortKind::Transport, 100),
        None,
    )
    .await;
    append(
        &*log,
        session,
        &aborted(t, s, 2, AbortKind::Transport, 50),
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("done")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    let models = all_of(&spans, SpanKind::ModelCall);
    assert_eq!(models.len(), 1, "attempts never become spans");
    let model_span = models[0];
    assert_eq!(model_span.status, SpanStatus::Ok);
    assert_eq!(model_span.error_type, None);
    let SpanAttributes::ModelCall(attrs) = &model_span.attributes else {
        panic!("model span attributes");
    };
    assert_eq!(attrs.attempt_count, 3);
    assert_eq!(attrs.attempts.len(), 2);
    assert_eq!(attrs.attempts[0].attempt, 1);
    assert_eq!(attrs.attempts[0].backoff_before_ms, 0);
    assert_eq!(attrs.attempts[1].attempt, 2);
    assert_eq!(attrs.attempts[1].backoff_before_ms, 100);
    assert!(!attrs.attempts[0].replayed);
    assert_eq!(attrs.usage, Some(usage(10, 5)), "only the success counts");
}

#[tokio::test]
async fn t4_retry_exhaustion_marks_the_model_call_and_turn_error() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(4);
    let t = turn();
    let s = step(0x71);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &aborted(t, s, 1, AbortKind::Transport, 100),
        None,
    )
    .await;
    append(
        &*log,
        session,
        &aborted(t, s, 2, AbortKind::Transport, 0),
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Failed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    let model_span = span_of(&spans, SpanKind::ModelCall);
    assert_eq!(model_span.status, SpanStatus::Error);
    assert_eq!(model_span.error_type, Some(ErrorType::Transport));
    let turn_span = span_of(&spans, SpanKind::Turn);
    assert_eq!(turn_span.status, SpanStatus::Error);
    assert_eq!(turn_span.error_type, Some(ErrorType::Transport));
}

#[tokio::test]
async fn t5_tool_failure_does_not_fail_the_step_or_turn() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(5);
    let t = turn();
    let s = step(0x81);
    let c = call(0x82);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("call")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_requested(t, s, c, "echo"),
        Some(dedup::tool_call(c)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_result(c, ToolResultStatus::Failed),
        Some(dedup::tool_result(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    let tool_span = span_of(&spans, SpanKind::ToolCall);
    assert_eq!(tool_span.status, SpanStatus::Error);
    assert_eq!(tool_span.error_type, Some(ErrorType::ToolDenied));
    assert_eq!(span_of(&spans, SpanKind::Step).status, SpanStatus::Ok);
    assert_eq!(span_of(&spans, SpanKind::Turn).status, SpanStatus::Ok);
}

#[tokio::test]
async fn t6_interrupted_tool_cancels_the_step_and_turn() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(6);
    let t = turn();
    let s = step(0x91);
    let c = call(0x92);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("call")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_requested(t, s, c, "echo"),
        Some(dedup::tool_call(c)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_result(c, ToolResultStatus::Interrupted),
        Some(dedup::tool_result(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Cancelled,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    assert_eq!(
        span_of(&spans, SpanKind::ToolCall).status,
        SpanStatus::Interrupted
    );
    assert_eq!(
        span_of(&spans, SpanKind::Step).status,
        SpanStatus::Cancelled
    );
    assert_eq!(
        span_of(&spans, SpanKind::Turn).status,
        SpanStatus::Cancelled
    );
}

#[tokio::test]
async fn t7_approval_wait_is_a_child_span_with_its_deadline_and_cause() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(7);
    let t = turn();
    let s = step(0xa1);
    let c = call(0xa2);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("call")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    let request_id = append(
        &*log,
        session,
        &tool_requested(t, s, c, "echo"),
        Some(dedup::tool_call(c)),
    )
    .await;
    let signal_event = SessionEvent::SignalReceived {
        signal_id: approval_signal_id(c),
        name: approval_signal_name(c),
        payload: serde_json::to_value(ApprovalDecision::Allow).unwrap(),
    };
    append(&*log, session, &signal_event, Some(dedup::tool_call(c))).await;
    append(
        &*log,
        session,
        &tool_result(c, ToolResultStatus::Completed),
        Some(dedup::tool_result(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let recorded = entries(&*log, session).await;
    let spans = fold(&log, session).await;
    let wait = span_of(&spans, SpanKind::Wait);
    let SpanAttributes::Wait(attrs) = &wait.attributes else {
        panic!("wait attrs");
    };
    assert_eq!(attrs.wait, WaitKind::Approval);
    assert_eq!(attrs.caused_by, Some(request_id));
    assert_eq!(
        wait.parent_span,
        Some(span_of(&spans, SpanKind::ToolCall).span)
    );
    assert_eq!(wait.status, SpanStatus::Ok);

    let request_at = recorded
        .iter()
        .find(|entry| entry.log_id == request_id)
        .unwrap()
        .recorded_at;
    let signal_at = recorded
        .iter()
        .find(|entry| {
            matches!(
                decode_entry(entry).unwrap(),
                SessionEvent::SignalReceived { .. }
            )
        })
        .unwrap()
        .recorded_at;
    assert_eq!(wait.started_at, request_at);
    assert_eq!(wait.ended_at, Some(signal_at));
}

#[tokio::test]
async fn t8_resume_is_a_session_event() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(8);
    let t = turn();
    let s = step(0xb1);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    let step_id = append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StreamResumed {
            from: step_id,
            reason: ResumeReason::Crash,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("done")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    assert!(spans.iter().all(|span| span.kind != SpanKind::Wait));

    let session_span = span_of(&spans, SpanKind::Session);
    let SpanAttributes::Session(attrs) = &session_span.attributes else {
        panic!("session attrs");
    };
    assert_eq!(attrs.resumes.len(), 1);
    assert_eq!(attrs.resumes[0].from, step_id);
    assert_eq!(attrs.resumes[0].reason, ResumeReason::Crash);
}

#[tokio::test]
async fn t9_subagent_links_parent_and_child_traces() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let parent = sid(9);
    let child = sid(10);
    let t = turn();
    let s = step(0xc1);
    let c = call(0xc2);

    append(
        &*log,
        parent,
        &created(parent),
        Some(dedup::session_created(parent)),
    )
    .await;
    append(
        &*log,
        parent,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        parent,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        parent,
        &assistant(t, s, vec![text("spawn")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    let request_id = append(
        &*log,
        parent,
        &tool_requested(t, s, c, "spawn_agent"),
        Some(dedup::tool_call(c)),
    )
    .await;
    append(
        &*log,
        parent,
        &SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config(),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: c,
            caused_by: request_id,
            deadline: None,
        },
        Some(dedup::child_session(child)),
    )
    .await;
    append(
        &*log,
        parent,
        &SessionEvent::ChildSessionCompleted {
            child,
            outcome: ChildOutcome::Completed {
                summary: asterpath_protocol::Summary {
                    items: vec![text("summary")],
                },
            },
        },
        Some(dedup::child_session(child)),
    )
    .await;
    append(
        &*log,
        parent,
        &tool_result(c, ToolResultStatus::Completed),
        Some(dedup::tool_result(c)),
    )
    .await;
    append(
        &*log,
        parent,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        parent,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    append(
        &*log,
        child,
        &SessionEvent::SessionCreated {
            config: config(),
            parent: Some(ParentRef {
                session: parent,
                turn: t,
                tool_call: c,
            }),
        },
        Some(dedup::session_created(child)),
    )
    .await;
    append(
        &*log,
        child,
        &SessionEvent::InputRecorded {
            input: vec![text("task")],
        },
        Some(DedupKey::new("child-input")),
    )
    .await;
    append(
        &*log,
        child,
        &SessionEvent::TurnStarted { turn_id: turn() },
        None,
    )
    .await;
    append(
        &*log,
        child,
        &SessionEvent::StepStarted {
            turn_id: turn(),
            step_id: step(0xc3),
        },
        None,
    )
    .await;
    append(
        &*log,
        child,
        &assistant(turn(), step(0xc3), vec![text("done")]),
        Some(dedup::assistant_message(turn(), step(0xc3))),
    )
    .await;
    append(
        &*log,
        child,
        &SessionEvent::StepCommitted {
            turn_id: turn(),
            step_id: step(0xc3),
        },
        Some(dedup::step_committed(step(0xc3))),
    )
    .await;
    append(
        &*log,
        child,
        &SessionEvent::TurnCommitted {
            turn_id: turn(),
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(turn())),
    )
    .await;

    let parent_spans = fold(&log, parent).await;
    let subagent = span_of(&parent_spans, SpanKind::Subagent);
    assert_eq!(
        subagent.parent_span,
        Some(span_of(&parent_spans, SpanKind::ToolCall).span)
    );
    assert_eq!(
        subagent.child_trace,
        Some(asterpath_protocol::TraceId::from_session(child))
    );
    assert_eq!(subagent.status, SpanStatus::Ok);
    assert!(matches!(
        &subagent.attributes,
        SpanAttributes::Subagent(attrs)
            if attrs.child == child && matches!(attrs.outcome, Some(ChildOutcome::Completed { .. }))
    ));

    let child_spans = fold(&log, child).await;
    let root = span_of(&child_spans, SpanKind::Session);
    assert_eq!(
        root.parent_trace,
        Some(asterpath_protocol::TraceId::from_session(parent))
    );
    assert_eq!(root.parent_span, Some(subagent.span));
}

#[tokio::test]
async fn t10_recovered_tool_reuses_every_span_id_and_marks_replay() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(11);
    let t = turn();
    let s = step(0xd1);
    let c = call(0xd2);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("call")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &tool_requested(t, s, c, "echo"),
        Some(dedup::tool_call(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StreamResumed {
            from: LogId::from_i64(1),
            reason: ResumeReason::Crash,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &tool_result(c, ToolResultStatus::Completed),
        Some(dedup::tool_result(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    let tools = all_of(&spans, SpanKind::ToolCall);
    assert_eq!(tools.len(), 1, "recovery never duplicates a logical span");
    assert!(tools[0].replay);
    let results = entries(&*log, session)
        .await
        .iter()
        .filter(|entry| {
            matches!(
                decode_entry(entry).unwrap(),
                SessionEvent::ToolResult { .. }
            )
        })
        .count();
    assert_eq!(results, 1);
}

#[tokio::test]
async fn t11_recovered_model_stream_replays_the_attempt_in_the_same_span() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(12);
    let t = turn();
    let s = step(0xe1);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StreamResumed {
            from: LogId::from_i64(1),
            reason: ResumeReason::Crash,
        },
        None,
    )
    .await;
    append(&*log, session, &aborted(t, s, 1, AbortKind::Crash, 0), None).await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("done")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    let models = all_of(&spans, SpanKind::ModelCall);
    assert_eq!(models.len(), 1, "one logical model call");
    assert!(models[0].replay);
    let SpanAttributes::ModelCall(attrs) = &models[0].attributes else {
        panic!("model attrs");
    };
    assert_eq!(attrs.attempts.len(), 1);
    assert!(
        attrs.attempts[0].replayed,
        "the new attempt happened after the resume"
    );
    assert_eq!(attrs.attempts[0].kind, AbortKind::Crash);
    let messages = entries(&*log, session)
        .await
        .iter()
        .filter(|entry| {
            matches!(
                decode_entry(entry).unwrap(),
                SessionEvent::AssistantMessage { .. }
            )
        })
        .count();
    assert_eq!(messages, 1);
}

#[tokio::test]
async fn t12_compaction_parent_follows_the_open_step_turn_or_session() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(13);
    let t = turn();
    let s = step(0xf1);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    let step_start = append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;

    let forced_key = dedup::compaction(step_start);
    append(
        &*log,
        session,
        &SessionEvent::CompactionStarted {
            trigger: CompactionTrigger::ProviderError,
            replaces_from: step_start,
            caused_by: step_start,
            turn: Some(t),
        },
        Some(forced_key.clone()),
    )
    .await;
    append(
        &*log,
        session,
        &compaction_applied(step_start, step_start),
        Some(forced_key),
    )
    .await;

    append(
        &*log,
        session,
        &assistant(t, s, vec![text("done")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    let committed = append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;

    let between_key = dedup::compaction(committed);
    append(
        &*log,
        session,
        &SessionEvent::CompactionStarted {
            trigger: CompactionTrigger::Threshold,
            replaces_from: step_start,
            caused_by: committed,
            turn: Some(t),
        },
        Some(between_key.clone()),
    )
    .await;
    append(
        &*log,
        session,
        &compaction_applied(step_start, committed),
        Some(between_key),
    )
    .await;

    let turn_committed = append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;
    let idle_key = dedup::compaction(turn_committed);
    append(
        &*log,
        session,
        &SessionEvent::CompactionStarted {
            trigger: CompactionTrigger::Manual,
            replaces_from: step_start,
            caused_by: turn_committed,
            turn: None,
        },
        Some(idle_key.clone()),
    )
    .await;
    append(
        &*log,
        session,
        &compaction_applied(step_start, committed),
        Some(idle_key),
    )
    .await;

    let spans = fold(&log, session).await;
    let compactions = all_of(&spans, SpanKind::Compaction);
    assert_eq!(compactions.len(), 3);

    let step_span = span_of(&spans, SpanKind::Step);
    let turn_span = span_of(&spans, SpanKind::Turn);
    let session_span = span_of(&spans, SpanKind::Session);

    assert_eq!(
        compactions[0].parent_span,
        Some(step_span.span),
        "forced in-step"
    );
    assert_eq!(
        compactions[1].parent_span,
        Some(turn_span.span),
        "between steps"
    );
    assert_eq!(compactions[2].parent_span, Some(session_span.span), "idle");

    for (compaction, (trigger, expected_replaces)) in compactions.iter().zip([
        (
            CompactionTrigger::ProviderError,
            asterpath_protocol::LogIdRange {
                first: step_start,
                last: step_start,
            },
        ),
        (
            CompactionTrigger::Threshold,
            asterpath_protocol::LogIdRange {
                first: step_start,
                last: committed,
            },
        ),
        (
            CompactionTrigger::Manual,
            asterpath_protocol::LogIdRange {
                first: step_start,
                last: committed,
            },
        ),
    ]) {
        assert_eq!(compaction.status, SpanStatus::Ok);
        assert!(compaction.name.starts_with("compact "));
        let SpanAttributes::Compaction(attrs) = &compaction.attributes else {
            panic!("compaction attrs");
        };
        assert_eq!(attrs.trigger, trigger);
        assert_eq!(attrs.replaces, Some(expected_replaces));
        assert_eq!(attrs.model, Some(model()));
        assert_eq!(attrs.usage, Some(usage(7, 3)));
    }
}

#[tokio::test]
async fn t13_spans_never_carry_content() {
    const SECRET: &str = "SECRET-PROMPT-COMPLETION-ARGS";
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(14);
    let t = turn();
    let s = step(0x11);
    let c = call(0x12);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text(SECRET)]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::ToolCallRequested {
            turn_id: t,
            step_id: s,
            tool_call_id: c,
            provider_call_id: "p".to_owned(),
            name: ToolName::new("echo"),
            raw_args: JsonString::new(format!("{{\"secret\":\"{SECRET}\"}}")),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
        Some(dedup::tool_call(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::ToolResult {
            tool_call_id: c,
            status: ToolResultStatus::Completed,
            output: Some(vec![text(SECRET)]),
            error: None,
            truncated: false,
        },
        Some(dedup::tool_result(c)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    let rendered = serde_json::to_string(&spans).unwrap();
    assert!(
        !rendered.contains(SECRET),
        "content must only be reachable via log_range"
    );

    for span in &spans {
        assert!(span.log_range.first <= span.log_range.last);
        let range: Vec<_> = entries(&*log, session)
            .await
            .into_iter()
            .filter(|entry| {
                entry.log_id >= span.log_range.first && entry.log_id <= span.log_range.last
            })
            .collect();
        assert!(
            !range.is_empty(),
            "log_range must reference committed records"
        );
    }
}

#[tokio::test]
async fn retired_or_unknown_session_is_not_found() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let query = TracingQuery::new(Arc::clone(&log));
    let mut stream = query.spans(SpanFilter {
        session: Some(sid(99)),
        ..SpanFilter::default()
    });
    let error = stream
        .next()
        .await
        .expect("one error")
        .expect_err("not found");
    assert!(matches!(error, TraceError::NotFound { session } if session == sid(99)));
}

#[tokio::test]
async fn a_log_without_session_created_is_corrupt() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(15);
    append(
        &*log,
        session,
        &SessionEvent::InputRecorded {
            input: vec![text("hi")],
        },
        Some(DedupKey::new("input")),
    )
    .await;

    let query = TracingQuery::new(Arc::clone(&log));
    let mut stream = query.spans(SpanFilter {
        session: Some(session),
        ..SpanFilter::default()
    });
    let error = stream
        .next()
        .await
        .expect("one error")
        .expect_err("corrupt");
    assert!(matches!(error, TraceError::Corrupt { session: s, .. } if s == session));
}

#[tokio::test]
async fn forced_compaction_retry_keeps_the_model_call_ok() {
    let log: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let session = sid(18);
    let t = turn();
    let s = step(0x13);

    append(
        &*log,
        session,
        &created(session),
        Some(dedup::session_created(session)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnStarted { turn_id: t },
        None,
    )
    .await;
    let step_id = append(
        &*log,
        session,
        &SessionEvent::StepStarted {
            turn_id: t,
            step_id: s,
        },
        None,
    )
    .await;
    // Context exhaustion is not retried, but the loop forces a compaction and
    // then retries the same step, so `backoff_ms == 0` is not a terminal.
    append(
        &*log,
        session,
        &aborted(t, s, 1, AbortKind::Transport, 0),
        None,
    )
    .await;
    let key = dedup::compaction(step_id);
    append(
        &*log,
        session,
        &SessionEvent::CompactionStarted {
            trigger: CompactionTrigger::ProviderError,
            replaces_from: step_id,
            caused_by: step_id,
            turn: Some(t),
        },
        Some(key.clone()),
    )
    .await;
    append(
        &*log,
        session,
        &compaction_applied(step_id, step_id),
        Some(key),
    )
    .await;
    append(
        &*log,
        session,
        &assistant(t, s, vec![text("done")]),
        Some(dedup::assistant_message(t, s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::StepCommitted {
            turn_id: t,
            step_id: s,
        },
        Some(dedup::step_committed(s)),
    )
    .await;
    append(
        &*log,
        session,
        &SessionEvent::TurnCommitted {
            turn_id: t,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(t)),
    )
    .await;

    let spans = fold(&log, session).await;
    let model_span = span_of(&spans, SpanKind::ModelCall);
    assert_eq!(model_span.status, SpanStatus::Ok);
    let SpanAttributes::ModelCall(attrs) = &model_span.attributes else {
        panic!("model attrs");
    };
    assert_eq!(attrs.attempts.len(), 1);
    assert_eq!(attrs.attempt_count, 2);
    assert!(
        attrs.compacted,
        "the forced compaction precedes the final request"
    );
    assert_eq!(
        span_of(&spans, SpanKind::Compaction).parent_span,
        Some(span_of(&spans, SpanKind::Step).span)
    );
}
