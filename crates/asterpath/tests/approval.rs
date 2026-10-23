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
    ToolSelection, TurnConfig, Usage, UserInput, approval_signal_id, approval_signal_name,
    decode_entry, dedup, encode_entry,
};
use asterpath::protocol::{DedupKey, LogEntry, LogQuery, SessionId, SessionStore, ToolCallId};
use asterpath::{Asterpath, SessionHandle};
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
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn descriptor(name: &str, timeout: Option<Duration>) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::ReadOnly,
        provenance: asterpath::protocol::Provenance::InProcess,
        timeout,
        max_output_bytes: None,
    }
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
        turn: TurnConfig {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..TurnConfig::default()
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
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

async fn drive_events(handle: &SessionHandle) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive"));
    }
    events
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
    let _endpoint = asterpath.approval_gate().register();
    let handle = asterpath.create_session(spec()).await.unwrap();
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

    let entries = scan(asterpath.store(), handle.id()).await;
    let results = tool_results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].0,
        asterpath::protocol::ToolResultStatus::Completed
    );
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(kind_count(&entries, "SignalReceived"), 1);

    // Duplicate delivery is absorbed by the signal dedup key.
    handle
        .approve(request.tool_call_id, ApprovalDecision::Allow)
        .await
        .expect("duplicate approve");
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(kind_count(&entries, "SignalReceived"), 1);

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
    let _endpoint = asterpath.approval_gate().register();
    let handle = asterpath.create_session(spec()).await.unwrap();
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
    // RAII endpoint registered and dropped: the gate is closed again.
    drop(asterpath.approval_gate().register());
    let handle = asterpath.create_session(spec()).await.unwrap();
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
    assert_eq!(kind_count(&entries, "SignalReceived"), 0);

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
    let handle = asterpath.create_session(spec()).await.unwrap();
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
        SessionEvent::SignalReceived {
            signal_id: approval_signal_id(call),
            name: approval_signal_name(call),
            payload: serde_json::to_value(ApprovalDecision::Allow).unwrap(),
        },
        Some(dedup::signal(approval_signal_id(call))),
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
    let _endpoint = asterpath.approval_gate().register();
    let handle = asterpath.create_session(spec()).await.unwrap();
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
        .tool(ToolConfig {
            kill_grace: Duration::from_millis(20),
            ..ToolConfig::default()
        })
        .build()
        .await
        .expect("build");
    let handle = asterpath.create_session(spec()).await.unwrap();
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
        .tool(ToolConfig {
            kill_grace: Duration::from_millis(100),
            ..ToolConfig::default()
        })
        .build()
        .await
        .expect("build");
    let handle = asterpath.create_session(spec()).await.unwrap();
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
    let denied = asterpath.create_session(spec()).await.unwrap();
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
    let _endpoint = asterpath.approval_gate().register();
    let allowed = asterpath.create_session(spec()).await.unwrap();
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
