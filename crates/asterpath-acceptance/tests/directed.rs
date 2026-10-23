//! Directed recovery tests: the windows the v1 acceptance suite does not cover,
//! driven through failpoints, natural markers and fabricated committed prefixes.
#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath::{Asterpath, SessionHandle};
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalPolicy, ApprovalRequest, ApprovalVerdict,
    CompactionConfig, Item, JsonString, ModelId, ModelSpec, Parallelism, Part, Provenance,
    Provider, ProviderId, ReplaySafety, RequestConfig, Role, SessionEvent, SessionSpec, SideEffect,
    StopReason, SubagentConfig, Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName,
    ToolOutput, ToolResultStatus, TurnConfig, TurnEndReason, Usage, UserInput, approval_signal_id,
    approval_signal_name, decode_entry, dedup, encode_entry,
};
use asterpath_protocol::{
    Clock, DedupKey, EventLog, InputId, LogEntry, LogQuery, SessionId, StepId, SystemClock,
    ToolCallId, TurnId,
};
use asterpath_testing::ScriptedProvider;
use common::{Scenario, Testdrive, markers, scan, session_id};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

const SESSION_INPUT: InputId = InputId::from_bytes([42; 16]);
const TURN: TurnId = TurnId::from_bytes([1; 16]);
const STEP: StepId = StepId::from_bytes([2; 16]);
const CALL: ToolCallId = ToolCallId::from_bytes([3; 16]);

fn temp_db(tag: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join(format!(
        "{tag}-{}.sqlite",
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    (dir, path)
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new("model-1"),
        context_window: None,
    }
}

fn descriptor(name: &str, replay: ReplaySafety, provenance: Provenance) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: Parallelism::Parallel,
        replay,
        side_effect: SideEffect::Mutating,
        provenance,
        timeout: None,
        max_output_bytes: None,
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        model: model(),
        system: vec![text("be helpful")],
        tools: asterpath_protocol::ToolSelection::All,
        request: RequestConfig::default(),
        turn: TurnConfig {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..TurnConfig::default()
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

fn tool_call(call_id: &str, name: &str) -> Item {
    Item::ToolCall {
        call_id: call_id.to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new("{}"),
        raw: None,
    }
}

fn final_items() -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text("done")],
    }]
}

type InvokeFn = Arc<
    dyn Fn(ToolArgs, ToolContext) -> BoxFuture<'static, Result<ToolOutput, ToolError>>
        + Send
        + Sync,
>;

struct TestTool {
    descriptor: ToolDescriptor,
    calls: Arc<Mutex<Vec<ToolArgs>>>,
    invoke: InvokeFn,
}

impl TestTool {
    fn build(
        descriptor: ToolDescriptor,
        calls: Arc<Mutex<Vec<ToolArgs>>>,
        invoke: impl Fn(ToolArgs, ToolContext) -> BoxFuture<'static, Result<ToolOutput, ToolError>>
        + Send
        + Sync
        + 'static,
    ) -> Arc<dyn Tool> {
        Arc::new(Self {
            descriptor,
            calls,
            invoke: Arc::new(invoke),
        })
    }
}

impl Tool for TestTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.lock().unwrap().push(args.clone());
        (self.invoke)(args, ctx)
    }
}

fn echo(name: &str, provenance: Provenance) -> (Arc<dyn Tool>, Arc<Mutex<Vec<ToolArgs>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let tool = TestTool::build(
        descriptor(name, ReplaySafety::Safe, provenance),
        Arc::clone(&calls),
        |_args, _ctx| Box::pin(async { Ok(ToolOutput::new(vec![text("tool done")])) }),
    );
    (tool, calls)
}

struct AlwaysAsk;

impl ApprovalPolicy for AlwaysAsk {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        ApprovalVerdict::Ask
    }
}

async fn build(
    db: &Path,
    provider: Arc<ScriptedProvider>,
    tools: Vec<Arc<dyn Tool>>,
    approval: Option<Arc<dyn ApprovalPolicy>>,
) -> Asterpath {
    let provider: Arc<dyn Provider> = provider;
    let mut builder = asterpath::AsterpathBuilder::new()
        .database(db)
        .providers(vec![provider])
        .tools(tools);
    if let Some(approval) = approval {
        builder = builder.approval(approval);
    }
    builder.build().await.expect("facade builds")
}

async fn append(
    asterpath: &Asterpath,
    session: SessionId,
    event: SessionEvent,
    key: Option<DedupKey>,
) {
    let entry = encode_entry(session, key, &event).expect("encode");
    asterpath.event_log().append(entry).await.expect("append");
}

async fn scan_session(asterpath: &Asterpath, session: SessionId) -> Vec<LogEntry> {
    let log = asterpath.event_log();
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan"));
    }
    entries
}

async fn drive(handle: &SessionHandle) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive"));
    }
    events
}

fn input(value: &str) -> UserInput {
    UserInput {
        id: SESSION_INPUT,
        parts: vec![text(value)],
    }
}

fn results(entries: &[LogEntry]) -> Vec<(ToolCallId, ToolResultStatus, Option<String>)> {
    entries
        .iter()
        .filter_map(|entry| match decode_entry(entry).expect("decode") {
            SessionEvent::ToolResult {
                tool_call_id,
                status,
                error,
                output,
                ..
            } => {
                assert!(
                    (status != ToolResultStatus::Interrupted)
                        || (output.is_none() && error.is_none()),
                    "Interrupted carries neither output nor error"
                );
                Some((
                    tool_call_id,
                    status,
                    error.map(|error| format!("{:?}", error.kind)),
                ))
            }
            _ => None,
        })
        .collect()
}

fn turn_reasons(entries: &[LogEntry]) -> Vec<TurnEndReason> {
    entries
        .iter()
        .filter_map(|entry| match decode_entry(entry).expect("decode") {
            SessionEvent::TurnCommitted { reason, .. } => Some(reason),
            _ => None,
        })
        .collect()
}

fn abort_kinds(entries: &[LogEntry]) -> Vec<asterpath_protocol::AbortKind> {
    entries
        .iter()
        .filter_map(|entry| match decode_entry(entry).expect("decode") {
            SessionEvent::ModelCallAborted { kind, .. } => Some(kind),
            _ => None,
        })
        .collect()
}

// --- testdrive configuration errors ---------------------------------------

#[tokio::test]
async fn inspect_prints_the_committed_log_from_sqlite() {
    let (_dir, db) = temp_db("inspect");
    let asterpath = build(&db, Arc::new(ScriptedProvider::new("p")), Vec::new(), None).await;
    let handle = asterpath.create_session(spec()).await.expect("create");
    let session = handle.id();
    asterpath.shutdown().await.expect("shutdown");
    drop(handle);

    let output = assert_cmd::Command::cargo_bin("testdrive")
        .expect("cargo bin")
        .arg("inspect")
        .arg("--db")
        .arg(&db)
        .arg("--session")
        .arg(session.to_hex())
        .output()
        .expect("spawn inspect");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"kind\":\"SessionCreated\""), "{stdout}");
    assert!(
        stdout.contains("\"event\":\"inspect_complete\""),
        "{stdout}"
    );
}

#[test]
fn testdrive_rejects_unknown_failpoints_with_exit_code_two() {
    let (_dir, db) = temp_db("config");
    let effect = db.with_extension("effect");
    for failpoint in ["append:nowhere", "clock:advance"] {
        assert_cmd::Command::cargo_bin("testdrive")
            .expect("cargo bin")
            .arg("run")
            .arg("--db")
            .arg(&db)
            .arg("--fixture")
            .arg(common::fixture("a1_safe.json"))
            .arg("--failpoint")
            .arg(failpoint)
            .env("ASTERPATH_EFFECT_FILE", &effect)
            .assert()
            .code(2);
    }
}

// --- S5: idempotent append ------------------------------------------------

#[tokio::test]
async fn s5_same_key_appends_once_and_returns_the_same_log_id() {
    let (_dir, db) = temp_db("s5");
    let backend = asterpath_storage_sqlite::open(&db).expect("open");
    let log: Arc<dyn EventLog> = backend;
    let session = SessionId::from_bytes([5; 16]);
    let event = SessionEvent::InputRecorded {
        input: vec![text("go")],
    };
    let entry =
        encode_entry(session, Some(dedup::input_recorded(SESSION_INPUT)), &event).expect("encode");
    let first = log.append(entry.clone()).await.expect("first");
    let second = log.append(entry).await.expect("second");
    assert_eq!(first, second);
    drop(log);
    assert_eq!(
        common::kind_count(&scan(&db, session).await, "InputRecorded"),
        1
    );
}

// --- S18: submit_input, crash before drive, wakeable discovery -------------

#[tokio::test]
async fn s18_input_submitted_before_a_crash_is_projected_exactly_once() {
    let (_dir, db) = temp_db("s18");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        final_items(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = build(&db, provider, Vec::new(), None).await;
    let handle = asterpath.create_session(spec()).await.expect("create");
    let session = handle.id();
    handle.submit_input(input("go")).await.expect("submit");
    asterpath.shutdown().await.expect("shutdown");
    drop(handle);

    // The process is gone: only the committed log and derived indexes remain.
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        final_items(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = build(&db, Arc::clone(&provider), Vec::new(), None).await;
    let wakeable = asterpath
        .storage()
        .wakeable(SystemClock.now())
        .await
        .expect("wakeable");
    assert_eq!(wakeable, vec![session], "pending input wakes the session");

    let handle = asterpath.open_session(session).await.expect("open");
    let events = drive(&handle).await;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnStarted { .. }))
    );
    assert_eq!(provider.requests().len(), 1);
    let users: Vec<String> = provider.requests()[0]
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Message {
                role: Role::User,
                parts,
            } => Some(parts),
            _ => None,
        })
        .flatten()
        .filter_map(|part| match part {
            Part::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        users,
        vec!["go".to_owned()],
        "the input is projected exactly once"
    );

    let entries = scan_session(&asterpath, session).await;
    assert_eq!(common::kind_count(&entries, "InputRecorded"), 1);
    assert_eq!(common::kind_count(&entries, "StepStarted"), 1);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
    asterpath.shutdown().await.expect("shutdown");
}

// --- unclosed compaction without a compactor ------------------------------

#[tokio::test]
async fn unclosed_compaction_stays_open_without_making_the_session_busy() {
    let (_dir, db) = temp_db("compaction");
    let asterpath = build(&db, Arc::new(ScriptedProvider::new("p")), Vec::new(), None).await;
    let handle = asterpath.create_session(spec()).await.expect("create");
    let session = handle.id();
    let frontier = asterpath
        .event_log()
        .frontier(session)
        .await
        .expect("frontier")
        .expect("session created");
    append(
        &asterpath,
        session,
        SessionEvent::CompactionStarted {
            trigger: asterpath_protocol::CompactionTrigger::Manual,
            replaces_from: frontier,
            caused_by: frontier,
            turn: None,
        },
        Some(dedup::compaction(frontier)),
    )
    .await;

    assert_eq!(
        handle.state().await.expect("state"),
        asterpath_protocol::SessionState::Idle
    );
    assert!(matches!(
        handle.compact_now().await.expect("compact_now"),
        asterpath_protocol::CompactResult::NotConfigured
    ));
    assert!(
        !asterpath
            .storage()
            .wakeable(SystemClock.now())
            .await
            .expect("wakeable")
            .contains(&session),
        "a dangling summary intent is not wakeable"
    );
    let events = drive(&handle).await;
    assert!(matches!(events.as_slice(), [AgentEvent::Idle { .. }]));
    asterpath.shutdown().await.expect("shutdown");
    drop(handle);

    // The intent is still open after a restart, and the session is still Idle.
    let asterpath = build(&db, Arc::new(ScriptedProvider::new("p")), Vec::new(), None).await;
    let handle = asterpath.open_session(session).await.expect("open");
    assert_eq!(
        handle.state().await.expect("state"),
        asterpath_protocol::SessionState::Idle
    );
    let entries = scan_session(&asterpath, session).await;
    assert_eq!(
        common::kind_count(&entries, "CompactionAborted")
            + common::kind_count(&entries, "CompactionApplied"),
        0
    );
    assert_eq!(common::kind_count(&entries, "CompactionStarted"), 1);
    asterpath.shutdown().await.expect("shutdown");
}

// --- T3 / T4: approval decisions across a restart -------------------------

async fn approval_prefix(asterpath: &Asterpath, session: SessionId, decision: bool) {
    let external = Provenance::External {
        transport: asterpath_protocol::TransportKind::Stdio,
    };
    append(
        asterpath,
        session,
        SessionEvent::TurnStarted { turn_id: TURN },
        None,
    )
    .await;
    append(
        asterpath,
        session,
        SessionEvent::StepStarted {
            turn_id: TURN,
            step_id: STEP,
        },
        None,
    )
    .await;
    append(
        asterpath,
        session,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: vec![tool_call("c1", "guarded")],
            raw: None,
            model: model(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        asterpath,
        session,
        SessionEvent::ToolCallRequested {
            turn_id: TURN,
            step_id: STEP,
            tool_call_id: CALL,
            provider_call_id: "c1".to_owned(),
            name: ToolName::new("guarded"),
            raw_args: JsonString::new("{}"),
            replay: ReplaySafety::Unsafe,
            provenance: external,
        },
        Some(dedup::tool_call(CALL)),
    )
    .await;
    if decision {
        append(
            asterpath,
            session,
            SessionEvent::SignalReceived {
                signal_id: approval_signal_id(CALL),
                name: approval_signal_name(CALL),
                payload: serde_json::to_value(ApprovalDecision::Allow).unwrap(),
            },
            Some(dedup::signal(approval_signal_id(CALL))),
        )
        .await;
    }
}

#[tokio::test]
async fn t3_recorded_allow_survives_the_restart_and_executes_without_reprompting() {
    let (_dir, db) = temp_db("t3");
    let (tool, calls) = echo(
        "guarded",
        Provenance::External {
            transport: asterpath_protocol::TransportKind::Stdio,
        },
    );
    let asterpath = build(
        &db,
        Arc::new(ScriptedProvider::new("p")),
        vec![tool],
        Some(Arc::new(AlwaysAsk)),
    )
    .await;
    let session = asterpath.create_session(spec()).await.expect("create").id();
    approval_prefix(&asterpath, session, true).await;
    asterpath.shutdown().await.expect("shutdown");

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        final_items(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        vec![calls_tool(&calls)],
        Some(Arc::new(AlwaysAsk)),
    )
    .await;
    let _endpoint = asterpath.approval_gate().register();
    let handle = asterpath.open_session(session).await.expect("open");
    let events = drive(&handle).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_))),
        "a recorded decision is never re-prompted"
    );
    assert_eq!(calls.lock().unwrap().len(), 1);
    let entries = scan_session(&asterpath, session).await;
    assert_eq!(results(&entries)[0].1, ToolResultStatus::Completed);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
    asterpath.shutdown().await.expect("shutdown");
}

fn calls_tool(calls: &Arc<Mutex<Vec<ToolArgs>>>) -> Arc<dyn Tool> {
    TestTool::build(
        descriptor(
            "guarded",
            ReplaySafety::Safe,
            Provenance::External {
                transport: asterpath_protocol::TransportKind::Stdio,
            },
        ),
        Arc::clone(calls),
        |_args, _ctx| Box::pin(async { Ok(ToolOutput::new(vec![text("ran")])) }),
    )
}

#[tokio::test]
async fn t4_undecided_ask_resuspends_after_a_restart_and_waits_for_the_answer() {
    let (_dir, db) = temp_db("t4");
    let (tool, calls) = echo(
        "guarded",
        Provenance::External {
            transport: asterpath_protocol::TransportKind::Stdio,
        },
    );
    let asterpath = build(
        &db,
        Arc::new(ScriptedProvider::new("p")),
        vec![tool],
        Some(Arc::new(AlwaysAsk)),
    )
    .await;
    let session = asterpath.create_session(spec()).await.expect("create").id();
    approval_prefix(&asterpath, session, false).await;
    asterpath.shutdown().await.expect("shutdown");

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        final_items(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        vec![calls_tool(&calls)],
        Some(Arc::new(AlwaysAsk)),
    )
    .await;
    let _endpoint = asterpath.approval_gate().register();
    let handle = asterpath.open_session(session).await.expect("open");

    let mut stream = handle.drive();
    let request = loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval requested")
            .expect("event")
            .expect("no error");
        if let AgentEvent::ApprovalRequested(request) = event {
            break request;
        }
    };
    assert_eq!(request.tool_call_id, CALL);
    assert_eq!(request.tool.as_str(), "guarded");
    assert!(calls.lock().unwrap().is_empty());
    assert!(results(&scan_session(&asterpath, session).await).is_empty());
    // The public classifier derives the same state recovery used.
    assert_eq!(
        handle.state().await.expect("state"),
        asterpath_protocol::SessionState::TurnOpen {
            turn: TURN,
            step: Some(asterpath_protocol::StepState::InProgress),
        }
    );

    handle
        .approve(CALL, ApprovalDecision::Allow)
        .await
        .expect("approve");
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
    assert_eq!(calls.lock().unwrap().len(), 1);
    let entries = scan_session(&asterpath, session).await;
    assert_eq!(results(&entries)[0].1, ToolResultStatus::Completed);
    asterpath.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn t4_undecided_ask_with_no_endpoint_after_restart_fails_closed() {
    let (_dir, db) = temp_db("t4-closed");
    let (tool, _calls) = echo(
        "guarded",
        Provenance::External {
            transport: asterpath_protocol::TransportKind::Stdio,
        },
    );
    let asterpath = build(
        &db,
        Arc::new(ScriptedProvider::new("p")),
        vec![tool],
        Some(Arc::new(AlwaysAsk)),
    )
    .await;
    let session = asterpath.create_session(spec()).await.expect("create").id();
    approval_prefix(&asterpath, session, false).await;
    asterpath.shutdown().await.expect("shutdown");

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        final_items(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let (tool, calls) = echo(
        "guarded",
        Provenance::External {
            transport: asterpath_protocol::TransportKind::Stdio,
        },
    );
    let asterpath = build(&db, provider, vec![tool], Some(Arc::new(AlwaysAsk))).await;
    // No endpoint registered after the restart.
    let handle = asterpath.open_session(session).await.expect("open");
    let events = drive(&handle).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_)))
    );
    assert!(calls.lock().unwrap().is_empty());
    let entries = scan_session(&asterpath, session).await;
    let results = results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, ToolResultStatus::Failed);
    assert_eq!(results[0].2.as_deref(), Some("Denied"));
    asterpath.shutdown().await.expect("shutdown");
}

// --- S11: cancellation survives as a committed terminal -------------------

#[tokio::test]
async fn s11_cancelled_tool_result_is_not_retried_after_a_restart() {
    let (_dir, db) = temp_db("s11");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let blocking = TestTool::build(
        descriptor("blocking", ReplaySafety::Safe, Provenance::InProcess),
        Arc::clone(&calls),
        |_args, _ctx| {
            Box::pin(async { std::future::pending::<Result<ToolOutput, ToolError>>().await })
        },
    );
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("c1", "blocking")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = build(&db, provider, vec![blocking], None).await;
    let mut zero_grace = spec();
    zero_grace.turn.cancel_grace = Duration::ZERO;
    let handle = asterpath.create_session(zero_grace).await.expect("create");
    let session = handle.id();
    handle.submit_input(input("go")).await.expect("submit");

    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        if matches!(event, AgentEvent::ToolStarted { .. }) {
            handle.cancel().await.expect("cancel");
        }
    }
    drop(stream);
    let entries = scan_session(&asterpath, session).await;
    let results = results(&entries);
    assert_eq!(results.len(), 1, "one terminal for the cancelled call");
    assert_eq!(results[0].1, ToolResultStatus::Interrupted);
    assert_eq!(results[0].2, None);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Cancelled]);
    asterpath.shutdown().await.expect("shutdown");
    drop(handle);

    // Restart: a completed cancellation is not retried.
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = build(&db, Arc::clone(&provider), Vec::new(), None).await;
    let handle = asterpath.open_session(session).await.expect("open");
    let events = drive(&handle).await;
    assert!(matches!(events.as_slice(), [AgentEvent::Idle { .. }]));
    assert!(provider.requests().is_empty());
    assert_eq!(scan_session(&asterpath, session).await.len(), entries.len());
    asterpath.shutdown().await.expect("shutdown");
}

// --- L13: crash after ModelCallAborted{Cancelled}, before StepCommitted ----

#[tokio::test]
async fn l13_cancelled_abort_without_step_commit_finishes_on_recovery() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("l13.sqlite");
    let effect = dir.path().join("unused.json");

    let mut run = Testdrive::start(Scenario {
        run: true,
        db: &db,
        fixture: "l13_cancel_before_commit.json",
        effect: &effect,
        hold_ms: 30_000,
        failpoints: vec!["append:before_commit=6".to_owned()],
    });
    run.wait_marker("\"event\":\"failpoint\"");
    let run_lines = run.kill();
    let session = session_id(&run_lines);

    let resume_lines = Testdrive::start(Scenario {
        run: false,
        db: &db,
        fixture: "l13_cancel_before_commit.json",
        effect: &effect,
        hold_ms: 0,
        failpoints: Vec::new(),
    })
    .wait_success();

    let entries = common::scan(&db, session).await;
    assert_eq!(
        abort_kinds(&entries),
        vec![asterpath_protocol::AbortKind::Cancelled]
    );
    assert_eq!(common::kind_count(&entries, "StepCommitted"), 1);
    assert_eq!(common::kind_count(&entries, "StreamResumed"), 1);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Cancelled]);
    assert!(
        markers(&resume_lines, "model_request").is_empty(),
        "recovery closes the cancelled step without calling the model"
    );
}

// --- S15: crash between batch ToolCallRequested appends --------------------

#[tokio::test]
async fn s15_crash_between_intents_is_filled_without_duplicates_or_crossing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("s15.sqlite");
    let effect = dir.path().join("effect.log");

    let mut run = Testdrive::start(Scenario {
        run: true,
        db: &db,
        fixture: "s15_batch_intents.json",
        effect: &effect,
        hold_ms: 30_000,
        failpoints: vec!["append:before_commit=7".to_owned()],
    });
    run.wait_marker("\"event\":\"failpoint\"");
    let run_lines = run.kill();
    let session = session_id(&run_lines);

    let resume_lines = Testdrive::start(Scenario {
        run: false,
        db: &db,
        fixture: "s15_batch_intents.json",
        effect: &effect,
        hold_ms: 0,
        failpoints: Vec::new(),
    })
    .wait_success();

    let entries = common::scan(&db, session).await;
    let intents: Vec<(ToolCallId, String)> = entries
        .iter()
        .filter_map(|entry| match decode_entry(entry).expect("decode") {
            SessionEvent::ToolCallRequested {
                tool_call_id,
                provider_call_id,
                ..
            } => Some((tool_call_id, provider_call_id)),
            _ => None,
        })
        .collect();
    assert_eq!(
        intents.len(),
        2,
        "one intent per provider call, no duplicates"
    );
    let first = intents
        .iter()
        .find(|(_, provider)| provider == "call-1")
        .expect("call-1 intent")
        .0;
    let second = intents
        .iter()
        .find(|(_, provider)| provider == "call-2")
        .expect("call-2 intent")
        .0;
    let results = results(&entries);
    assert_eq!(results.len(), 2);
    assert_eq!(
        results
            .iter()
            .find(|(call, _, _)| *call == first)
            .expect("first result")
            .1,
        ToolResultStatus::Interrupted,
        "the recorded unsafe intent is not rerun"
    );
    assert_eq!(
        results
            .iter()
            .find(|(call, _, _)| *call == second)
            .expect("second result")
            .1,
        ToolResultStatus::Completed,
        "the missing sibling runs against its own call id"
    );
    assert_eq!(common::effect_lines(&effect), vec!["line".to_owned()]);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);

    let requests = markers(&resume_lines, "model_request");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]["tool_results"], 2,
        "both results project into the next request"
    );
}

// --- S17: crash after an unknown-tool intent is committed ------------------

#[tokio::test]
async fn s17_unknown_tool_intent_recovers_as_not_found_without_approval() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("s17.sqlite");
    let effect = dir.path().join("unused.json");

    let mut run = Testdrive::start(Scenario {
        run: true,
        db: &db,
        fixture: "s17_unknown_tool.json",
        effect: &effect,
        hold_ms: 30_000,
        failpoints: vec!["append:after_commit=6".to_owned()],
    });
    run.wait_marker("\"event\":\"failpoint\"");
    let run_lines = run.kill();
    let session = session_id(&run_lines);

    let resume_lines = Testdrive::start(Scenario {
        run: false,
        db: &db,
        fixture: "s17_unknown_tool.json",
        effect: &effect,
        hold_ms: 0,
        failpoints: Vec::new(),
    })
    .wait_success();

    let entries = common::scan(&db, session).await;
    let results = results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, ToolResultStatus::Failed);
    assert_eq!(results[0].2.as_deref(), Some("NotFound"));
    assert_eq!(common::kind_count(&entries, "SignalReceived"), 0);
    assert_eq!(common::kind_count(&entries, "ToolCallRequested"), 1);
    assert!(
        abort_kinds(&entries).is_empty(),
        "no model retry was needed"
    );
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);

    let requests = markers(&resume_lines, "model_request");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["tool_results"], 1);
}
