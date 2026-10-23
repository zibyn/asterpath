//! Backend-neutral event log contract suite (G2, G3, G4).
//!
//! The suite only depends on core/protocol types, so every backend runs the
//! same assertions. G1, `Corrupt`, retirement and backend-specific behaviour
//! are deliberately not here.

use std::collections::BTreeSet;
use std::sync::Arc;

use asterpath_protocol::{
    ChildOutcome, CompactionAbortReason, CompactionTrigger, ConfigSnapshot, ContextPolicy,
    JsonString, LOG_SCHEMA_VERSION, ModelId, ModelSpec, Part, Provenance, ProviderId, ReplaySafety,
    SessionEvent, SessionFacts, ToolName, ToolResultStatus, Usage, dedup,
};
use asterpath_protocol::{
    DedupKey, EventLog, LogEntry, LogId, LogIdRange, LogQuery, NewEntry, SessionId, SignalId,
    SignalName, StepId, Storage, StorageError, Timestamp, ToolCallId, TurnId,
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
        turn: None,
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
        deadline: None,
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
    futures::executor::block_on(log.append(entry))
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
    let results: Vec<Result<LogEntry, StorageError>> =
        futures::executor::block_on(stream.collect());
    results
        .into_iter()
        .map(|result| result.expect("scan should succeed"))
        .collect()
}

fn frontier(log: &dyn EventLog, session: SessionId) -> Option<LogId> {
    futures::executor::block_on(log.frontier(session)).expect("frontier should succeed")
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
            session(25),
            dedup::signal(signal()),
            vec![signal_received()],
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

fn turn_with(byte: u8) -> TurnId {
    TurnId::from_bytes([byte; 16])
}

fn step_with(byte: u8) -> StepId {
    StepId::from_bytes([byte; 16])
}

fn turn_started_with(turn_id: TurnId) -> SessionEvent {
    SessionEvent::TurnStarted { turn_id }
}

fn turn_committed_with(turn_id: TurnId) -> SessionEvent {
    SessionEvent::TurnCommitted {
        turn_id,
        reason: asterpath_protocol::TurnEndReason::Completed,
    }
}

fn step_started_with(turn_id: TurnId, step_id: StepId) -> SessionEvent {
    SessionEvent::StepStarted { turn_id, step_id }
}

fn step_committed_with(turn_id: TurnId, step_id: StepId) -> SessionEvent {
    SessionEvent::StepCommitted { turn_id, step_id }
}

fn signal_received_with(signal_id: SignalId, name: &str) -> SessionEvent {
    SessionEvent::SignalReceived {
        signal_id,
        name: SignalName::new(name),
        payload: json!({}),
    }
}

/// Append an event and remember the session as part of this contract run.
fn append_in(
    log: &dyn EventLog,
    sessions: &mut BTreeSet<SessionId>,
    session: SessionId,
    event: &SessionEvent,
    dedup_key: Option<DedupKey>,
) -> LogId {
    sessions.insert(session);
    append(log, session, event, dedup_key)
}

/// Backend-neutral storage suite (G5, G6, G7 plus pending-input
/// semantics). The expected wake set is derived by folding the visible log, so
/// a materialized index that disagrees with the log fails here.
pub fn storage_contract(factory: &impl BackendFactory) {
    let backend = factory.open();
    let log = backend.log.as_ref();
    let storage = backend.storage.as_ref();
    let now: Timestamp = 1_000;
    let mut sessions = BTreeSet::new();

    // An open turn makes the session wakeable; committing it clears that.
    let s_turn = session(0x60);
    let turn = turn_with(1);
    append_in(log, &mut sessions, s_turn, &turn_started_with(turn), None);
    assert!(
        block_on(storage.wakeable(now))
            .expect("wakeable")
            .contains(&s_turn),
        "G5: an open turn is wakeable"
    );
    append_in(
        log,
        &mut sessions,
        s_turn,
        &turn_committed_with(turn),
        Some(dedup::turn_committed(turn)),
    );

    // Pending input is retained until StepStarted, and reported once.
    let s_input = session(0x61);
    append_in(
        log,
        &mut sessions,
        s_input,
        &input_recorded(),
        Some(DedupKey::new("input-61")),
    );
    for _ in 0..2 {
        let wakeable = block_on(storage.wakeable(now)).expect("wakeable");
        assert!(
            wakeable.contains(&s_input),
            "G5: pending input wakes until a step starts"
        );
        assert_eq!(
            wakeable.iter().filter(|s| **s == s_input).count(),
            1,
            "G5: a session appears at most once per call"
        );
    }
    let input_turn = turn_with(2);
    let input_step = step_with(2);
    append_in(
        log,
        &mut sessions,
        s_input,
        &step_started_with(input_turn, input_step),
        None,
    );
    append_in(
        log,
        &mut sessions,
        s_input,
        &step_committed_with(input_turn, input_step),
        Some(dedup::step_committed(input_step)),
    );

    // Children: an open spawn wakes; completion closes it.
    let s_child = session(0x66);
    let child = session(0x67);
    let spawned = append_in(
        log,
        &mut sessions,
        s_child,
        &child_spawned(child),
        Some(dedup::child_session(child)),
    );
    let repeated = append_in(
        log,
        &mut sessions,
        s_child,
        &child_spawned(child),
        Some(dedup::child_session(child)),
    );
    assert_eq!(
        repeated, spawned,
        "G5: repeating drive returns the original LogId"
    );
    assert!(
        block_on(storage.wakeable(now))
            .expect("wakeable")
            .contains(&s_child),
        "G5: an open child spawn is wakeable"
    );
    append_in(
        log,
        &mut sessions,
        s_child,
        &child_completed(child),
        Some(dedup::child_session(child)),
    );

    // Only a dangling compaction intent leaves the session idle.
    let s_compaction = session(0x68);
    let compaction_key = dedup::compaction(LogId::from_i64(1));
    append_in(
        log,
        &mut sessions,
        s_compaction,
        &compaction_started(),
        Some(compaction_key.clone()),
    );
    assert!(
        !block_on(storage.wakeable(now))
            .expect("wakeable")
            .contains(&s_compaction),
        "G5: compaction alone does not make a session wakeable"
    );
    append_in(
        log,
        &mut sessions,
        s_compaction,
        &compaction_applied(),
        Some(compaction_key),
    );

    // Approval closes nothing: SignalReceived leaves the tool intent open.
    let s_approval = session(0x69);
    let approval_call = call(9);
    append_in(
        log,
        &mut sessions,
        s_approval,
        &tool_call_requested(approval_call),
        Some(dedup::tool_call(approval_call)),
    );
    append_in(
        log,
        &mut sessions,
        s_approval,
        &signal_received_with(
            SignalId::from_bytes(*approval_call.as_bytes()),
            "tool.approval",
        ),
        Some(dedup::tool_call(approval_call)),
    );
    assert!(
        block_on(storage.wakeable(now))
            .expect("wakeable")
            .contains(&s_approval),
        "G5: an approval decision does not close the open tool intent"
    );
    append_in(
        log,
        &mut sessions,
        s_approval,
        &tool_result(approval_call),
        Some(dedup::tool_result(approval_call)),
    );

    // G6: each session stream is independent.
    let stream_a = session(0x70);
    let stream_b = session(0x71);
    let turn_a = turn_with(7);
    let turn_b = turn_with(8);
    append_in(
        log,
        &mut sessions,
        stream_a,
        &turn_started_with(turn_a),
        None,
    );
    append_in(
        log,
        &mut sessions,
        stream_b,
        &turn_started_with(turn_b),
        None,
    );
    append_in(
        log,
        &mut sessions,
        stream_a,
        &turn_committed_with(turn_a),
        Some(dedup::turn_committed(turn_a)),
    );
    assert_eq!(
        scan(log, stream_a)
            .iter()
            .map(|entry| entry.seq)
            .collect::<Vec<_>>(),
        vec![1, 2],
        "G6: another stream does not create holes or delay visibility"
    );
    assert_eq!(
        scan(log, stream_b)
            .iter()
            .map(|entry| entry.seq)
            .collect::<Vec<_>>(),
        vec![1],
        "G6: each stream allocates its own seq"
    );
    assert_eq!(
        frontier(log, stream_a),
        scan(log, stream_a).last().map(|entry| entry.log_id),
        "G6: frontier is per stream"
    );

    // G5/G7: the materialized wake index must equal the log fold.
    let expected_wakeable = expected_wakeable(log, &sessions);
    let wakeable = block_on(storage.wakeable(now)).expect("wakeable");
    assert_eq!(
        wakeable, expected_wakeable,
        "G5/G7: wakeable must match the open-intent and pending-input fold"
    );
}

fn expected_wakeable(log: &dyn EventLog, sessions: &BTreeSet<SessionId>) -> Vec<SessionId> {
    let mut wakeable = Vec::new();
    for &session in sessions {
        let entries = scan(log, session);
        let facts = SessionFacts::fold(&entries).expect("fold session facts");
        if !facts.is_idle() || facts.has_pending_input() {
            wakeable.push(session);
        }
    }
    wakeable
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    futures::executor::block_on(future)
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
