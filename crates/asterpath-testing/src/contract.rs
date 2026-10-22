//! Backend-neutral event log contract suite (G2, G3, G4).
//!
//! The suite only depends on core/protocol types, so every backend runs the
//! same assertions. G1, `Corrupt`, retirement and backend-specific behaviour
//! are deliberately not here.

use std::sync::Arc;

use asterpath_core::{
    DedupKey, EventLog, LogEntry, LogId, LogIdRange, LogQuery, NewEntry, SessionId, SignalId,
    SignalName, StepId, Storage, StorageError, TimerId, ToolCallId, TurnId,
};
use asterpath_protocol::{
    ChildOutcome, CompactionAbortReason, CompactionTrigger, ConfigSnapshot, ContextPolicy,
    JsonString, LOG_SCHEMA_VERSION, ModelId, ModelSpec, Part, Provenance, ProviderId, ReplaySafety,
    SessionEvent, ToolName, ToolResultStatus, Usage, dedup,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

pub struct BackendHandle {
    pub log: Arc<dyn EventLog>,
    pub storage: Arc<dyn Storage>,
}

pub trait BackendFactory {
    /// A new handle to semantically the same backend (reopening it).
    fn open(&self) -> BackendHandle;
}

/// Run the shared event log contract against a backend.
pub fn event_log_contract(factory: &impl BackendFactory) {
    let backend = factory.open();
    idempotent_append(backend.log.as_ref());
    shared_dedup_key_pairs(backend.log.as_ref());
    monotonic_seq(backend.log.as_ref());
    cursor_scan(backend.log.as_ref());
    hard_errors(backend.log.as_ref());
}

fn session(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn turn() -> TurnId {
    TurnId::from_bytes([1; 16])
}

fn step() -> StepId {
    StepId::from_bytes([2; 16])
}

fn call(byte: u8) -> ToolCallId {
    ToolCallId::from_bytes([byte; 16])
}

fn signal() -> SignalId {
    SignalId::from_bytes([3; 16])
}

fn timer() -> TimerId {
    TimerId::from_bytes([4; 16])
}

fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("test"),
        model: ModelId::new("model"),
        context_window: Some(1000),
    }
}

fn config() -> ConfigSnapshot {
    ConfigSnapshot {
        model: model(),
        system: vec![],
        tools: vec![],
        request: Default::default(),
        turn: Default::default(),
        compaction: Default::default(),
        subagents: Default::default(),
    }
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn tool_call_requested(call_id: ToolCallId) -> SessionEvent {
    SessionEvent::ToolCallRequested {
        turn_id: turn(),
        step_id: step(),
        tool_call_id: call_id,
        provider_call_id: "provider-call".to_owned(),
        name: ToolName::new("echo"),
        raw_args: JsonString::new("{}"),
        replay: ReplaySafety::Safe,
        provenance: Provenance::InProcess,
    }
}

fn tool_result(call_id: ToolCallId) -> SessionEvent {
    SessionEvent::ToolResult {
        tool_call_id: call_id,
        status: ToolResultStatus::Completed,
        output: Some(vec![text("ok")]),
        error: None,
        truncated: false,
    }
}

fn compaction_started() -> SessionEvent {
    SessionEvent::CompactionStarted {
        trigger: CompactionTrigger::Manual,
        replaces_from: LogId::from_i64(1),
        caused_by: LogId::from_i64(1),
    }
}

fn compaction_applied() -> SessionEvent {
    SessionEvent::CompactionApplied {
        first_kept: LogId::from_i64(2),
        summary: vec![text("summary")],
        replaces: LogIdRange {
            first: LogId::from_i64(1),
            last: LogId::from_i64(1),
        },
        model: model(),
        usage: Usage::default(),
        raw: None,
    }
}

fn compaction_aborted() -> SessionEvent {
    SessionEvent::CompactionAborted {
        reason: CompactionAbortReason::Insufficient,
    }
}

fn child_spawned(child: SessionId) -> SessionEvent {
    SessionEvent::ChildSessionSpawned {
        child,
        task: vec![text("task")],
        config: config(),
        context: ContextPolicy::Fresh,
        parent_tool_call_id: call(1),
        caused_by: LogId::from_i64(1),
    }
}

fn child_completed(child: SessionId) -> SessionEvent {
    SessionEvent::ChildSessionCompleted {
        child,
        outcome: ChildOutcome::Cancelled {
            reason: asterpath_protocol::CancelReason::Host,
        },
    }
}

fn timer_scheduled() -> SessionEvent {
    SessionEvent::TimerScheduled {
        timer_id: timer(),
        deadline: 100,
        caused_by: LogId::from_i64(1),
    }
}

fn timer_fired() -> SessionEvent {
    SessionEvent::TimerFired { timer_id: timer() }
}

fn timer_cancelled() -> SessionEvent {
    SessionEvent::TimerCancelled { timer_id: timer() }
}

fn signal_awaited() -> SessionEvent {
    SessionEvent::SignalAwaited {
        signal_id: signal(),
        name: SignalName::new("wait"),
        caused_by: LogId::from_i64(1),
    }
}

fn signal_received() -> SessionEvent {
    SessionEvent::SignalReceived {
        signal_id: signal(),
        name: SignalName::new("wait"),
        payload: json!({}),
    }
}

fn session_created() -> SessionEvent {
    SessionEvent::SessionCreated {
        config: config(),
        parent: None,
    }
}

fn context_forked() -> SessionEvent {
    SessionEvent::ContextForked {
        source: asterpath_protocol::ParentRef {
            session: session(1),
            turn: turn(),
            tool_call: call(1),
        },
        source_log: LogId::from_i64(1),
        items: vec![],
    }
}

fn input_recorded() -> SessionEvent {
    SessionEvent::InputRecorded {
        input: vec![text("go")],
    }
}

fn entry_for(
    session: SessionId,
    schema_version: u16,
    event: &SessionEvent,
    dedup_key: Option<DedupKey>,
) -> NewEntry {
    NewEntry {
        session,
        schema_version,
        dedup_key,
        payload: serde_json::to_value(event).expect("serialize event"),
    }
}

fn append(
    log: &dyn EventLog,
    session: SessionId,
    event: &SessionEvent,
    dedup_key: Option<DedupKey>,
) -> LogId {
    append_raw(
        log,
        entry_for(session, LOG_SCHEMA_VERSION, event, dedup_key),
    )
    .expect("append should succeed")
}

fn append_raw(log: &dyn EventLog, entry: NewEntry) -> Result<LogId, StorageError> {
    futures_executor::block_on(log.append(entry))
}

fn scan(log: &dyn EventLog, session: SessionId) -> Vec<LogEntry> {
    scan_query(
        log,
        LogQuery {
            session,
            after: None,
            limit: None,
        },
    )
}

fn scan_query(log: &dyn EventLog, query: LogQuery) -> Vec<LogEntry> {
    let stream = log.scan(query);
    let results: Vec<Result<LogEntry, StorageError>> = futures_executor::block_on(stream.collect());
    results
        .into_iter()
        .map(|result| result.expect("scan should succeed"))
        .collect()
}

fn frontier(log: &dyn EventLog, session: SessionId) -> Option<LogId> {
    futures_executor::block_on(log.frontier(session)).expect("frontier should succeed")
}

fn idempotent_append(log: &dyn EventLog) {
    let session = session(10);
    let call_id = call(10);
    let requested = tool_call_requested(call_id);
    let result = tool_result(call_id);
    let key = dedup::tool_call(call_id);

    let first = append(log, session, &requested, Some(key.clone()));
    let second = append(log, session, &requested, Some(key.clone()));
    assert_eq!(
        first, second,
        "G2: duplicate append returns the original LogId"
    );
    assert_eq!(
        scan(log, session).len(),
        1,
        "G2: duplicate append writes nothing"
    );

    // Same key, different event kind: both records land.
    let result_id = append(log, session, &result, Some(key));
    assert_ne!(first, result_id, "G2: event kind is part of the identity");
    assert_eq!(scan(log, session).len(), 2, "G2: shared key types coexist");

    // Different key, same event kind: a new record lands.
    let other = append(log, session, &requested, Some(DedupKey::new("other")));
    assert_ne!(other, first);
    assert_eq!(scan(log, session).len(), 3);
}

fn shared_dedup_key_pairs(log: &dyn EventLog) {
    let call_id = call(20);
    let child = session(21);
    let groups: Vec<(SessionId, DedupKey, Vec<SessionEvent>)> = vec![
        (
            session(20),
            dedup::tool_call(call_id),
            vec![tool_call_requested(call_id), tool_result(call_id)],
        ),
        (
            session(22),
            dedup::compaction(LogId::from_i64(1)),
            vec![
                compaction_started(),
                compaction_applied(),
                compaction_aborted(),
            ],
        ),
        (
            session(23),
            dedup::child_session(child),
            vec![child_spawned(child), child_completed(child)],
        ),
        (
            session(24),
            dedup::timer(timer()),
            vec![timer_scheduled(), timer_fired(), timer_cancelled()],
        ),
        (
            session(25),
            dedup::signal(signal()),
            vec![signal_awaited(), signal_received()],
        ),
        (
            session(26),
            dedup::child_session(child),
            vec![session_created(), context_forked(), input_recorded()],
        ),
    ];

    for (session, key, events) in groups {
        let first_pass: Vec<LogId> = events
            .iter()
            .map(|event| append(log, session, event, Some(key.clone())))
            .collect();
        let mut unique = first_pass.clone();
        unique.sort_by_key(|id| id.as_i64());
        unique.dedup();
        assert_eq!(
            unique.len(),
            events.len(),
            "G2/S12: every event kind under one shared key lands"
        );
        assert_eq!(scan(log, session).len(), events.len());

        let second_pass: Vec<LogId> = events
            .iter()
            .map(|event| append(log, session, event, Some(key.clone())))
            .collect();
        assert_eq!(
            first_pass, second_pass,
            "G2: replaying a shared-key group is idempotent per event kind"
        );
        assert_eq!(scan(log, session).len(), events.len());
    }
}

fn monotonic_seq(log: &dyn EventLog) {
    let sid = session(30);
    for index in 0..5 {
        append(
            log,
            sid,
            &input_recorded(),
            Some(DedupKey::new(format!("input-{index}"))),
        );
    }
    let entries = scan(log, sid);
    let seqs: Vec<u64> = entries.iter().map(|entry| entry.seq).collect();
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4, 5],
        "G3: seq is strictly monotonic without holes"
    );
    assert!(
        entries
            .windows(2)
            .all(|pair| pair[0].log_id < pair[1].log_id),
        "G3: storage assigns increasing log ids"
    );

    let other = session(31);
    append(log, other, &input_recorded(), None);
    assert_eq!(
        scan(log, other)[0].seq,
        1,
        "G3: seq is per session, not global"
    );
}

fn cursor_scan(log: &dyn EventLog) {
    let sid = session(40);
    let ids: Vec<LogId> = (0..4)
        .map(|index| {
            append(
                log,
                sid,
                &input_recorded(),
                Some(DedupKey::new(format!("cursor-{index}"))),
            )
        })
        .collect();

    let after = scan_query(
        log,
        LogQuery {
            session: sid,
            after: Some(ids[1]),
            limit: None,
        },
    );
    assert_eq!(after.len(), 2, "G4: the cursor is exclusive");
    assert_eq!(
        after.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        vec![3, 4],
        "G4: scan is ordered by seq"
    );

    let limited = scan_query(
        log,
        LogQuery {
            session: sid,
            after: None,
            limit: Some(2),
        },
    );
    assert_eq!(limited.len(), 2, "G4: limit is bounded");
    assert_eq!(limited[0].seq, 1);

    let empty = scan_query(
        log,
        LogQuery {
            session: sid,
            after: Some(ids[3]),
            limit: None,
        },
    );
    assert!(empty.is_empty(), "G4: no records after the last id");

    assert_eq!(
        frontier(log, sid),
        Some(ids[3]),
        "G4: frontier is the last LogId"
    );
    assert_eq!(frontier(log, session(99)), None);
}

fn hard_errors(log: &dyn EventLog) {
    let session = session(50);
    let cases: Vec<(Value, u16)> = vec![
        (json!({"NoSuchEvent": {}}), LOG_SCHEMA_VERSION),
        (json!({"ToolResult": {}}), LOG_SCHEMA_VERSION),
        (json!("not an event"), LOG_SCHEMA_VERSION),
        (
            json!({"InputRecorded": {"input": []}}),
            LOG_SCHEMA_VERSION + 1,
        ),
    ];
    for (payload, schema_version) in cases {
        let error = append_raw(
            log,
            NewEntry {
                session,
                schema_version,
                dedup_key: None,
                payload,
            },
        )
        .expect_err("bad payload must be a hard error");
        assert!(
            matches!(error, StorageError::InvalidEntry(_)),
            "expected InvalidEntry, got {error:?}"
        );
    }
    assert_eq!(
        scan(log, session).len(),
        0,
        "rejected appends write nothing"
    );

    // Unknown fields are ignored, not rejected.
    let accepted = append_raw(
        log,
        NewEntry {
            session,
            schema_version: LOG_SCHEMA_VERSION,
            dedup_key: None,
            payload: json!({"InputRecorded": {"input": [], "extra_field": true}}),
        },
    );
    assert!(accepted.is_ok(), "unknown fields are ignored");
    assert_eq!(scan(log, session).len(), 1);
}
