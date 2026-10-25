//! Approval pipeline through the facade: durable Ask, recorded decisions and
//! fail-closed behaviour when no endpoint is listening.
#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath::protocol::{
    AgentEvent, ApprovalDecision, ApprovalPolicy, ApprovalRequest, ApprovalVerdict,
    CompactionConfig, Item, JsonString, ModelId, ModelSpec, Parallelism, Part, Provider,
    ProviderId, ReplaySafety, RequestConfig, Role, SessionEvent, SessionSpec, SideEffect,
    StopReason, SubagentConfig, Tool, ToolArgs, ToolConfig, ToolDescriptor, ToolName, ToolOutput,
    ToolSelection, TurnConfig, Usage, UserInput, decode_entry, dedup, encode_entry,
};
use asterpath::protocol::{DedupKey, LogEntry, LogQuery, SessionId, SessionStore, ToolCallId};
use asterpath::{Asterpath, OwnedSession};
use asterpath_testing::{ScriptedProvider, TestTool};
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-approval-{tag}-{}-{}.db",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_file(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}-wal", path.display()));
    let _ = fs::remove_file(format!("{}-shm", path.display()));
    let _ = fs::remove_file(format!("{}.lock", path.display()));
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn descriptor(name: &str, timeout: Option<Duration>) -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new(name),
        String::new(),
        serde_json::json!({"type": "object"}),
    )
    .with_parallelism(Parallelism::Parallel)
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly)
    .with_timeout(timeout)
}

fn tool_call(name: &str) -> Item {
    Item::ToolCall {
        call_id: "call-1".to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(r#"{"q":"hi"}"#),
        raw: None,
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: vec![text("be helpful")],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: {
            let mut config = TurnConfig::default();
            config.initial_backoff = Duration::ZERO;
            config.max_backoff = Duration::ZERO;
            config
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

/// Classifies every call as `Ask`; the pipeline decides what to do with it.
struct AlwaysAsk;

impl ApprovalPolicy for AlwaysAsk {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        ApprovalVerdict::Ask
    }
}

fn echo_tool(calls: Arc<Mutex<Vec<ToolArgs>>>) -> Arc<dyn Tool> {
    TestTool::new(descriptor("echo", None), calls, |_args, _ctx| {
        Box::pin(async { Ok(ToolOutput::new(vec![text("tool says hi")])) })
    })
}

async fn scan(log: Arc<dyn SessionStore>, session: SessionId) -> Vec<LogEntry> {
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

fn kind_count(entries: &[LogEntry], kind: &str) -> usize {
    entries
        .iter()
        .filter(|entry| decode_entry(entry).expect("decodes").kind() == kind)
        .count()
}

fn tool_results(
    entries: &[LogEntry],
) -> Vec<(asterpath::protocol::ToolResultStatus, Option<String>)> {
    entries
        .iter()
        .filter_map(|entry| match decode_entry(entry).expect("decodes") {
            SessionEvent::ToolResult { status, error, .. } => {
                Some((status, error.map(|error| error.message)))
            }
            _ => None,
        })
        .collect()
}

fn text_input(id: u8) -> UserInput {
    UserInput {
        id: asterpath::protocol::InputId::from_bytes([id; 16]),
        parts: vec![text("go")],
    }
}

async fn drive_events(handle: &OwnedSession) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive"));
    }
    events
}

#[tokio::test]
async fn dropped_stream_redrive_requests_the_same_approval_again() {
    let db = temp_db("redrive");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("echo")],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                vec![Item::Message {
                    role: Role::Assistant,
                    parts: vec![text("done")],
                    raw: None,
                }],
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![echo_tool(Arc::clone(&calls))])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let _endpoint = asterpath.approval_gate().register(handle.id());
    handle.submit_input(text_input(1)).await.unwrap();

    let mut stream = handle.drive();
    let first = loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval is requested")
            .expect("event")
            .expect("no error");
        if let AgentEvent::ApprovalRequested(request) = event {
            break request.tool_call_id;
        }
    };
    // A client disconnect: the stream is dropped while the call waits.
    drop(stream);

    let mut stream = handle.drive();
    let second = loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval is requested again")
            .expect("event")
            .expect("no error");
        if let AgentEvent::ApprovalRequested(request) = event {
            break request.tool_call_id;
        }
    };
    assert_eq!(second, first, "re-drive re-emits the same request");

    handle
        .approve(second, ApprovalDecision::Allow)
        .await
        .expect("approve");
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        tool_results(&entries),
        vec![(asterpath::protocol::ToolResultStatus::Completed, None)]
    );
    assert_eq!(calls.lock().unwrap().len(), 1);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn ask_with_open_gate_suspends_then_allow_executes() {
    let db = temp_db("allow");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("echo")],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                vec![Item::Message {
                    role: Role::Assistant,
                    parts: vec![text("done")],
                    raw: None,
                }],
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![echo_tool(Arc::clone(&calls))])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let _endpoint = asterpath.approval_gate().register(handle.id());
    handle.submit_input(text_input(1)).await.unwrap();

    let mut stream = handle.drive();
    let request = loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval is requested")
            .expect("event")
            .expect("no error");
        if let AgentEvent::ApprovalRequested(request) = event {
            break request;
        }
    };
    assert_eq!(request.tool.as_str(), "echo");
    assert_eq!(
        asterpath.approval_gate().pending(),
        vec![request.clone()],
        "the waiting request is pending in process"
    );

    // Still suspended: no result recorded while waiting.
    let before = scan(asterpath.store(), handle.id()).await;
    assert!(tool_results(&before).is_empty());
    assert!(calls.lock().unwrap().is_empty());

    handle
        .approve(request.tool_call_id, ApprovalDecision::Allow)
        .await
        .expect("approve");

    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
    assert!(
        asterpath.approval_gate().pending().is_empty(),
        "a decided request is no longer pending"
    );

    let entries = scan(asterpath.store(), handle.id()).await;
    let results = tool_results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].0,
        asterpath::protocol::ToolResultStatus::Completed
    );
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(kind_count(&entries, "ApprovalDecided"), 1);

    // Duplicate delivery is absorbed by the signal dedup key.
    handle
        .approve(request.tool_call_id, ApprovalDecision::Allow)
        .await
        .expect("duplicate approve");
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(kind_count(&entries, "ApprovalDecided"), 1);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn dropping_the_drive_stream_removes_the_pending_request() {
    let db = temp_db("drop-pending");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("echo")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![echo_tool(Arc::clone(&calls))])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let _endpoint = asterpath.approval_gate().register(handle.id());
    handle.submit_input(text_input(1)).await.unwrap();

    let mut stream = handle.drive();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval is requested")
            .expect("event")
            .expect("no error");
        if matches!(event, AgentEvent::ApprovalRequested(_)) {
            break;
        }
    }
    assert_eq!(asterpath.approval_gate().pending().len(), 1);

    drop(stream);
    assert!(
        asterpath.approval_gate().pending().is_empty(),
        "the host dropped the wait"
    );

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn ask_with_open_gate_deny_records_denied_with_reason() {
    let db = temp_db("deny");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("echo")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![echo_tool(Arc::clone(&calls))])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let _endpoint = asterpath.approval_gate().register(handle.id());
    handle.submit_input(text_input(1)).await.unwrap();

    let mut stream = handle.drive();
    let request = loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval is requested")
            .expect("event")
            .expect("no error");
        if let AgentEvent::ApprovalRequested(request) = event {
            break request;
        }
    };
    handle
        .approve(
            request.tool_call_id,
            ApprovalDecision::Deny {
                reason: Some("not now".to_owned()),
            },
        )
        .await
        .expect("deny");
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }

    let entries = scan(asterpath.store(), handle.id()).await;
    let results = tool_results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, asterpath::protocol::ToolResultStatus::Failed);
    assert_eq!(results[0].1.as_deref(), Some("not now"));
    assert!(calls.lock().unwrap().is_empty());

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn ask_without_live_endpoint_fails_closed_without_requesting() {
    let db = temp_db("closed");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("echo")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![echo_tool(Arc::clone(&calls))])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    // RAII endpoint registered and dropped: the gate is closed again.
    drop(asterpath.approval_gate().register(handle.id()));
    handle.submit_input(text_input(1)).await.unwrap();

    let events = drive_events(&handle).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_)))
    );

    let entries = scan(asterpath.store(), handle.id()).await;
    let results = tool_results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, asterpath::protocol::ToolResultStatus::Failed);
    assert!(
        results[0]
            .1
            .as_deref()
            .is_some_and(|m| m.contains("no approval endpoint"))
    );
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(kind_count(&entries, "ApprovalDecided"), 0);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

/// ADR-0039: an endpoint attends one session tree. Another user's tree in
/// the same process, with nobody attending, fails closed instead of waiting.
#[tokio::test]
async fn an_endpoint_for_one_tree_leaves_other_trees_closed() {
    let db = temp_db("per-tree");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("echo")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![echo_tool(Arc::clone(&calls))])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build");
    let attended = asterpath.create_session(spec()).await.unwrap();
    let _endpoint = asterpath.approval_gate().register(attended.id());
    let unattended = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    unattended.submit_input(text_input(1)).await.unwrap();

    let events = tokio::time::timeout(Duration::from_secs(5), drive_events(&unattended))
        .await
        .expect("an unattended tree does not wait for a decision");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_)))
    );
    let entries = scan(asterpath.store(), unattended.id()).await;
    let results = tool_results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, asterpath::protocol::ToolResultStatus::Failed);
    assert_eq!(
        results[0].1.as_deref(),
        Some(asterpath::protocol::NO_APPROVAL_ENDPOINT)
    );
    assert!(calls.lock().unwrap().is_empty());
    drop(attended);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn recorded_decision_is_reused_without_reprompting() {
    let db = temp_db("recorded");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text("done")],
            raw: None,
        }],
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![echo_tool(Arc::clone(&calls))])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let session = handle.id();

    let turn = asterpath::protocol::TurnId::from_bytes([1; 16]);
    let step = asterpath::protocol::StepId::from_bytes([2; 16]);
    let call = ToolCallId::from_bytes([3; 16]);
    let log = asterpath.store();
    append(
        &log,
        session,
        SessionEvent::TurnStarted { turn_id: turn },
        None,
    )
    .await;
    append(
        &log,
        session,
        SessionEvent::StepStarted {
            turn_id: turn,
            step_id: step,
        },
        None,
    )
    .await;
    append(
        &log,
        session,
        SessionEvent::AssistantMessage {
            turn_id: turn,
            step_id: step,
            items: vec![tool_call("echo")],
            raw: None,
            model: spec().model,
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        },
        Some(dedup::assistant_message(turn, step)),
    )
    .await;
    append(
        &log,
        session,
        SessionEvent::ToolCallRequested {
            turn_id: turn,
            step_id: step,
            tool_call_id: call,
            provider_call_id: "call-1".to_owned(),
            name: ToolName::new("echo"),
            raw_args: JsonString::new(r#"{"q":"hi"}"#),
            replay: ReplaySafety::Safe,
            provenance: asterpath::protocol::Provenance::InProcess,
        },
        Some(dedup::tool_call(call)),
    )
    .await;
    append(
        &log,
        session,
        SessionEvent::ApprovalDecided {
            tool_call_id: call,
            decision: ApprovalDecision::Allow,
        },
        Some(dedup::approval_decided(call)),
    )
    .await;

    let events = drive_events(&handle).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_)))
    );
    assert_eq!(calls.lock().unwrap().len(), 1);
    let entries = scan(asterpath.store(), session).await;
    let results = tool_results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].0,
        asterpath::protocol::ToolResultStatus::Completed
    );

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn decision_recorded_while_an_earlier_call_waits_is_not_reprompted() {
    let db = temp_db("late");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text("done")],
            raw: None,
        }],
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![echo_tool(Arc::clone(&calls))])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let _endpoint = asterpath.approval_gate().register(handle.id());
    let session = handle.id();

    let turn = asterpath::protocol::TurnId::from_bytes([1; 16]);
    let step = asterpath::protocol::StepId::from_bytes([2; 16]);
    let first = ToolCallId::from_bytes([3; 16]);
    let second = ToolCallId::from_bytes([4; 16]);
    let call = |id: &str| Item::ToolCall {
        call_id: id.to_owned(),
        provider_item_id: None,
        name: ToolName::new("echo"),
        raw_args: JsonString::new(r#"{"q":"hi"}"#),
        raw: None,
    };
    let log = asterpath.store();
    append(
        &log,
        session,
        SessionEvent::TurnStarted { turn_id: turn },
        None,
    )
    .await;
    append(
        &log,
        session,
        SessionEvent::StepStarted {
            turn_id: turn,
            step_id: step,
        },
        None,
    )
    .await;
    append(
        &log,
        session,
        SessionEvent::AssistantMessage {
            turn_id: turn,
            step_id: step,
            items: vec![call("call-1"), call("call-2")],
            raw: None,
            model: spec().model,
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        },
        Some(dedup::assistant_message(turn, step)),
    )
    .await;
    for (id, provider_call_id) in [(first, "call-1"), (second, "call-2")] {
        append(
            &log,
            session,
            SessionEvent::ToolCallRequested {
                turn_id: turn,
                step_id: step,
                tool_call_id: id,
                provider_call_id: provider_call_id.to_owned(),
                name: ToolName::new("echo"),
                raw_args: JsonString::new(r#"{"q":"hi"}"#),
                replay: ReplaySafety::Safe,
                provenance: asterpath::protocol::Provenance::InProcess,
            },
            Some(dedup::tool_call(id)),
        )
        .await;
    }

    let mut stream = handle.drive();
    let request = loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval is requested")
            .expect("event")
            .expect("no error");
        if let AgentEvent::ApprovalRequested(request) = event {
            break request;
        }
    };
    assert_eq!(request.tool_call_id, first);
    // The second call is decided while the first still waits.
    handle
        .approve(second, ApprovalDecision::Allow)
        .await
        .expect("approve second");
    handle
        .approve(first, ApprovalDecision::Allow)
        .await
        .expect("approve first");
    let mut reprompted = Vec::new();
    while let Some(event) = stream.next().await {
        if let AgentEvent::ApprovalRequested(request) = event.expect("drive") {
            reprompted.push(request.tool_call_id);
        }
    }
    assert!(
        reprompted.is_empty(),
        "decided call re-prompted: {reprompted:?}"
    );
    assert_eq!(calls.lock().unwrap().len(), 2);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn cancel_before_deadline_wins_over_timeout() {
    let db = temp_db("cancel");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let stubborn = TestTool::new(
        descriptor("stubborn", Some(Duration::from_millis(10))),
        Arc::clone(&calls),
        |_args, ctx| {
            Box::pin(async move {
                // The tool notices cancellation, then hangs until dropped.
                ctx.cancel.cancel();
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(ToolOutput::new(vec![text("never")]))
            })
        },
    );
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("stubborn")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![stubborn])
        .tool({
            let mut config = ToolConfig::default();
            config.kill_grace = Duration::from_millis(20);
            config
        })
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    handle.submit_input(text_input(1)).await.unwrap();

    drive_events(&handle).await;

    let entries = scan(asterpath.store(), handle.id()).await;
    let results = tool_results(&entries);
    assert_eq!(results.len(), 1, "exactly one terminal record");
    assert_eq!(
        results[0].0,
        asterpath::protocol::ToolResultStatus::Interrupted,
        "cancel takes the terminal; no Failed{{Timeout}}"
    );
    assert_eq!(kind_count(&entries, "ToolResult"), 1);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn real_result_within_grace_beats_cancellation() {
    let db = temp_db("cancel-real");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let cooperative = TestTool::new(
        descriptor("cooperative", Some(Duration::from_millis(10))),
        Arc::clone(&calls),
        |_args, ctx| {
            Box::pin(async move {
                while !ctx.cancel.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok(ToolOutput::new(vec![text("finished anyway")]))
            })
        },
    );
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("cooperative")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![cooperative])
        .tool({
            let mut config = ToolConfig::default();
            config.kill_grace = Duration::from_millis(100);
            config
        })
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    handle.submit_input(text_input(1)).await.unwrap();

    drive_events(&handle).await;

    let entries = scan(asterpath.store(), handle.id()).await;
    let results = tool_results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].0,
        asterpath::protocol::ToolResultStatus::Completed
    );

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn default_policy_fails_closed_for_external_tools_until_an_endpoint_listens() {
    let db = temp_db("default-policy");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut remote = descriptor("remote", None);
    remote.provenance = asterpath::protocol::Provenance::External {
        transport: asterpath::protocol::TransportKind::Stdio,
    };
    let tool = TestTool::new(remote, Arc::clone(&calls), |_args, _ctx| {
        Box::pin(async { Ok(ToolOutput::new(vec![text("remote ok")])) })
    });
    let end_turn = |value: &str| {
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text(value)],
            raw: None,
        }]
    };
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("remote")],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(end_turn("done"), StopReason::EndTurn, Usage::default())
            .respond(
                vec![tool_call("remote")],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(end_turn("done"), StopReason::EndTurn, Usage::default()),
    );
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![tool])
        .build()
        .await
        .expect("build");

    // No endpoint: default policy denies without a prompt or a wait.
    let denied = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    denied.submit_input(text_input(1)).await.unwrap();
    let events = drive_events(&denied).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_)))
    );
    let entries = scan(asterpath.store(), denied.id()).await;
    let results = tool_results(&entries);
    assert_eq!(results[0].0, asterpath::protocol::ToolResultStatus::Failed);
    assert!(calls.lock().unwrap().is_empty());

    // With an endpoint the same tool asks and, once allowed, executes.
    let allowed = asterpath
        .create_session(spec())
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let _endpoint = asterpath.approval_gate().register(allowed.id());
    allowed.submit_input(text_input(2)).await.unwrap();
    let mut stream = allowed.drive();
    let request = loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval is requested")
            .expect("event")
            .expect("no error");
        if let AgentEvent::ApprovalRequested(request) = event {
            break request;
        }
    };
    allowed
        .approve(request.tool_call_id, ApprovalDecision::Allow)
        .await
        .expect("approve");
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
    let entries = scan(asterpath.store(), allowed.id()).await;
    let results = tool_results(&entries);
    assert_eq!(
        results[0].0,
        asterpath::protocol::ToolResultStatus::Completed
    );
    assert_eq!(calls.lock().unwrap().len(), 1);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

async fn append(
    log: &Arc<dyn SessionStore>,
    session: SessionId,
    event: SessionEvent,
    key: Option<DedupKey>,
) {
    let entry = encode_entry(session, key, &event).expect("encode");
    log.append(entry).await.expect("append");
}
