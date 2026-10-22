//! Sub-agent spawn through the facade: parent/child event streams, config
//! inheritance, failure mapping and child-session approval durability.
#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use asterpath::{Asterpath, AsterpathBuilder, AsterpathConfig, SessionHandle};
use asterpath_core::{ApprovalGate, EventLog, InputId, LogEntry, LogQuery, SessionId, ToolCallId};
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalPolicy, ApprovalRequest, ApprovalVerdict, ChildFailure,
    ChildOutcome, CompactionConfig, ConfigSnapshot, Item, JsonString, ModelId, ModelSpec,
    Parallelism, Part, Provenance, Provider, ProviderId, ReplaySafety, RequestConfig, Role,
    SessionEvent, SessionSpec, SideEffect, StopReason, SubagentConfig, Tool, ToolArgs, ToolContext,
    ToolDescriptor, ToolError, ToolName, ToolOutput, ToolResultStatus, ToolSelection, TurnConfig,
    TurnEndReason, Usage, UserInput, decode_entry,
};
use asterpath_testing::ScriptedProvider;
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-subagents-{tag}-{}-{}.db",
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

fn assistant(value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
    }]
}

fn tool_call(name: &str, args: &str) -> Item {
    Item::ToolCall {
        call_id: format!("call-{name}"),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(args),
        raw: None,
    }
}

fn usage() -> Usage {
    Usage::default()
}

fn descriptor(name: &str) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::ReadOnly,
        provenance: Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }
}

struct EchoTool {
    descriptor: ToolDescriptor,
}

impl EchoTool {
    fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            descriptor: descriptor(name),
        })
    }
}

impl Tool for EchoTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        let name = self.descriptor.name.as_str().to_owned();
        Box::pin(async move { Ok(ToolOutput::new(vec![text(&format!("{name} output"))])) })
    }
}

fn spec(max_steps: Option<u32>) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: Some(200_000),
        },
        system: vec![text("inherited system")],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: TurnConfig {
            max_steps_per_turn: max_steps,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..TurnConfig::default()
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

fn simple_config(
    db: &Path,
    providers: Vec<Arc<dyn Provider>>,
    tools: Vec<Arc<dyn Tool>>,
) -> AsterpathConfig {
    AsterpathConfig {
        database: db.to_path_buf(),
        providers,
        tools,
        ..AsterpathConfig::default()
    }
}

async fn open(config: AsterpathConfig) -> Asterpath {
    AsterpathBuilder::from_config(config)
        .build()
        .await
        .expect("facade builds")
}

async fn scan(log: Arc<dyn EventLog>, session: SessionId) -> Vec<LogEntry> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan succeeds"));
    }
    entries
}

fn kinds(entries: &[LogEntry]) -> Vec<&'static str> {
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodable").kind())
        .collect()
}

fn decoded(entries: &[LogEntry]) -> Vec<SessionEvent> {
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodable"))
        .collect()
}

async fn drive_all(handle: &SessionHandle) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive succeeds"));
    }
    events
}

fn input(message: &str) -> UserInput {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    UserInput {
        id: InputId::from_bytes(bytes),
        parts: vec![text(message)],
    }
}

fn spawn_event(entries: &[LogEntry]) -> (SessionId, ConfigSnapshot) {
    decoded(entries)
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { child, config, .. } => Some((child, config)),
            _ => None,
        })
        .expect("parent recorded a child spawn")
}

fn completion(entries: &[LogEntry]) -> Option<ChildOutcome> {
    decoded(entries).into_iter().find_map(|event| match event {
        SessionEvent::ChildSessionCompleted { outcome, .. } => Some(outcome),
        _ => None,
    })
}

fn spawn_result(entries: &[LogEntry]) -> (ToolResultStatus, Vec<Part>) {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult { status, output, .. } => {
                Some((status, output.unwrap_or_default()))
            }
            _ => None,
        })
        .next()
        .expect("spawn tool result")
}

async fn wait_for_child(asterpath: &Asterpath, parent: SessionId) -> SessionId {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let entries = scan(asterpath.event_log(), parent).await;
            if let Some((child, _)) = decoded(&entries).into_iter().find_map(|event| match event {
                SessionEvent::ChildSessionSpawned { child, config, .. } => Some((child, config)),
                _ => None,
            }) {
                return child;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("child spawn appears")
}

async fn wait_for_intent(asterpath: &Asterpath, session: SessionId, name: &str) -> ToolCallId {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let entries = scan(asterpath.event_log(), session).await;
            if let Some(call) = decoded(&entries).into_iter().find_map(|event| match event {
                SessionEvent::ToolCallRequested {
                    tool_call_id,
                    name: tool,
                    ..
                } if tool.as_str() == name => Some(tool_call_id),
                _ => None,
            }) {
                return call;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("tool intent appears")
}

#[tokio::test]
async fn spawn_agent_runs_a_fresh_child_and_returns_its_summary() {
    let db = temp_db("fresh");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("spawn_agent", r#"{"task":"summarize the file"}"#)],
                StopReason::ToolUse,
                usage(),
            )
            .respond(assistant("child summary"), StopReason::EndTurn, usage())
            .respond(assistant("parent done"), StopReason::EndTurn, usage()),
    );
    let asterpath = open(simple_config(&db, vec![provider.clone()], Vec::new())).await;
    let handle = asterpath.create_session(spec(None)).await.unwrap();
    handle.submit_input(input("go")).await.unwrap();
    drive_all(&handle).await;

    let parent_entries = scan(asterpath.event_log(), handle.id()).await;
    assert_eq!(
        kinds(&parent_entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "AssistantMessage",
            "ToolCallRequested",
            "ChildSessionSpawned",
            "ChildSessionCompleted",
            "ToolResult",
            "StepCommitted",
            "StepStarted",
            "AssistantMessage",
            "StepCommitted",
            "TurnCommitted",
        ]
    );

    let (child, spawned_config) = spawn_event(&parent_entries);
    let child_entries = scan(asterpath.event_log(), child).await;
    assert_eq!(
        kinds(&child_entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "AssistantMessage",
            "StepCommitted",
            "TurnCommitted",
        ]
    );

    let child_events = decoded(&child_entries);
    let SessionEvent::SessionCreated { config, parent } = &child_events[0] else {
        panic!("child starts with SessionCreated");
    };
    assert_eq!(
        config, &spawned_config,
        "ChildSessionSpawned.config is field-for-field the child snapshot"
    );
    assert_eq!(
        parent.as_ref().map(|parent| parent.session),
        Some(handle.id())
    );
    assert_eq!(config.model, spec(None).model);
    assert_eq!(config.system, vec![text("inherited system")]);

    let (status, output) = spawn_result(&parent_entries);
    assert_eq!(status, ToolResultStatus::Completed);
    assert_eq!(output, vec![text("child summary")]);

    assert_eq!(
        completion(&parent_entries),
        Some(ChildOutcome::Completed {
            summary: asterpath_protocol::Summary {
                items: vec![text("child summary")]
            }
        })
    );

    // Fresh context: the child's first request is only the task.
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[1].items,
        vec![Item::Message {
            role: Role::User,
            parts: vec![text("summarize the file")],
        }]
    );
    assert_eq!(requests[1].system, vec![text("inherited system")]);

    // The parent projection sees the summary, never the child transcript.
    let tool_results: Vec<_> = requests[2]
        .items
        .iter()
        .filter_map(|item| match item {
            Item::ToolResult { parts, .. } => Some(parts.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(tool_results, vec![vec![text("child summary")]]);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn child_max_steps_is_a_failed_outcome_in_a_completed_tool_result() {
    let db = temp_db("max-steps");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("spawn_agent", r#"{"task":"keep going"}"#)],
                StopReason::ToolUse,
                usage(),
            )
            .respond(
                vec![
                    Item::Message {
                        role: Role::Assistant,
                        parts: vec![text("working")],
                    },
                    tool_call("echo", r#"{"q":"hi"}"#),
                ],
                StopReason::ToolUse,
                usage(),
            )
            .respond(
                vec![
                    Item::Message {
                        role: Role::Assistant,
                        parts: vec![text("still working")],
                    },
                    tool_call("echo", r#"{"q":"again"}"#),
                ],
                StopReason::ToolUse,
                usage(),
            )
            .respond(assistant("parent done"), StopReason::EndTurn, usage()),
    );
    let asterpath = open(simple_config(
        &db,
        vec![provider.clone()],
        vec![EchoTool::new("echo")],
    ))
    .await;
    // The parent needs two steps (spawn + answer); the child exceeds two.
    let handle = asterpath.create_session(spec(Some(2))).await.unwrap();
    handle.submit_input(input("go")).await.unwrap();
    drive_all(&handle).await;

    let parent_entries = scan(asterpath.event_log(), handle.id()).await;
    let (status, output) = spawn_result(&parent_entries);
    assert_eq!(
        status,
        ToolResultStatus::Completed,
        "a failed task is still a successful spawn tool call"
    );
    assert!(
        matches!(output.first(), Some(Part::Text { text }) if text.contains("MaxStepsExceeded") && text.contains("failed")),
        "output marks the failure: {output:?}"
    );
    assert_eq!(output.get(1), Some(&text("still working")));
    assert_eq!(
        completion(&parent_entries),
        Some(ChildOutcome::Failed {
            reason: ChildFailure::MaxStepsExceeded,
            summary: asterpath_protocol::Summary {
                items: vec![text("still working")]
            }
        })
    );
    assert!(matches!(
        decoded(&parent_entries).last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

struct AllowSpawnAskGated;

impl ApprovalPolicy for AllowSpawnAskGated {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict {
        if request.tool.as_str() == "gated" {
            ApprovalVerdict::Ask
        } else {
            ApprovalVerdict::Allow
        }
    }
}

async fn approval_fixture(
    db: &Path,
    provider: Arc<ScriptedProvider>,
    gate: Arc<ApprovalGate>,
) -> Asterpath {
    let config = AsterpathConfig {
        database: db.to_path_buf(),
        providers: vec![provider],
        tools: vec![EchoTool::new("gated")],
        gate,
        approval: Arc::new(AllowSpawnAskGated),
        ..AsterpathConfig::default()
    };
    open(config).await
}

fn approval_script() -> ScriptedProvider {
    ScriptedProvider::new("p")
        .respond(
            vec![tool_call("spawn_agent", r#"{"task":"use the gate"}"#)],
            StopReason::ToolUse,
            usage(),
        )
        .respond(
            vec![tool_call("gated", r#"{"q":"hi"}"#)],
            StopReason::ToolUse,
            usage(),
        )
        .respond(assistant("child done"), StopReason::EndTurn, usage())
        .respond(assistant("parent done"), StopReason::EndTurn, usage())
}

#[tokio::test]
async fn child_approval_is_scoped_to_the_child_session() {
    let db = temp_db("approval");
    let gate = Arc::new(ApprovalGate::new());
    let _endpoint = gate.register();
    let asterpath = approval_fixture(&db, Arc::new(approval_script()), gate).await;
    let handle = asterpath.create_session(spec(None)).await.unwrap();
    let parent = handle.id();
    handle.submit_input(input("go")).await.unwrap();

    let driver = tokio::spawn({
        let handle = handle.clone();
        async move { drive_all(&handle).await }
    });

    let child = wait_for_child(&asterpath, parent).await;
    let call = wait_for_intent(&asterpath, child, "gated").await;

    // A parent-session decision for the same call id must not reach the child.
    asterpath
        .open_session(parent)
        .await
        .unwrap()
        .approve(call, ApprovalDecision::Allow)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let child_entries = scan(asterpath.event_log(), child).await;
    assert!(
        !decoded(&child_entries).iter().any(|event| matches!(
            event,
            SessionEvent::ToolResult { tool_call_id, .. } if *tool_call_id == call
        )),
        "the parent approval does not close the child's wait"
    );
    assert!(!driver.is_finished(), "the child is still waiting");

    // The child's own session accepts the decision.
    asterpath
        .open_session(child)
        .await
        .unwrap()
        .approve(call, ApprovalDecision::Allow)
        .await
        .unwrap();
    let events = driver.await.expect("drive task");
    assert!(matches!(
        events.last(),
        Some(AgentEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));

    let parent_entries = scan(asterpath.event_log(), parent).await;
    let (status, output) = spawn_result(&parent_entries);
    assert_eq!(status, ToolResultStatus::Completed);
    assert_eq!(output, vec![text("child done")]);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn child_approval_wait_survives_a_restart() {
    let db = temp_db("durable-approval");
    let gate = Arc::new(ApprovalGate::new());
    let _endpoint = gate.register();
    let provider = Arc::new(approval_script());
    let asterpath = approval_fixture(&db, provider.clone(), gate.clone()).await;
    let handle = asterpath.create_session(spec(None)).await.unwrap();
    let parent = handle.id();
    handle.submit_input(input("go")).await.unwrap();

    let first_driver = tokio::spawn({
        let handle = handle.clone();
        async move { drive_all(&handle).await }
    });
    let child = wait_for_child(&asterpath, parent).await;
    let call = wait_for_intent(&asterpath, child, "gated").await;

    // Simulate a crash while the child approval wait is open.
    first_driver.abort();
    let _ = first_driver.await;
    drop(handle);
    asterpath.shutdown().await.unwrap();

    // The same logs, a new process.
    let reopened = approval_fixture(&db, provider, gate).await;
    let handle = reopened.open_session(parent).await.unwrap();
    let driver = tokio::spawn({
        let handle = handle.clone();
        async move { drive_all(&handle).await }
    });

    // The recorded approval signal is durable; the child resumes from its log.
    reopened
        .open_session(child)
        .await
        .unwrap()
        .approve(call, ApprovalDecision::Allow)
        .await
        .unwrap();
    let events = driver.await.expect("drive task");
    assert!(matches!(
        events.last(),
        Some(AgentEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));

    let parent_entries = scan(reopened.event_log(), parent).await;
    assert_eq!(
        parent_entries
            .iter()
            .filter(|entry| decode_entry(entry).unwrap().kind() == "ChildSessionCompleted")
            .count(),
        1
    );
    let (status, output) = spawn_result(&parent_entries);
    assert_eq!(status, ToolResultStatus::Completed);
    assert_eq!(output, vec![text("child done")]);

    reopened.shutdown().await.unwrap();
    cleanup(&db);
}
