//! Sub-agent spawn through the facade: parent/child event streams, config
//! inheritance, failure mapping and child-session approval durability.
#![cfg(feature = "sqlite")]

use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use asterpath::{Asterpath, AsterpathBuilder, AsterpathConfig, SessionHandle};
use asterpath_core::{ApprovalGate, EventLog, InputId, LogEntry, LogQuery, SessionId, ToolCallId};
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalPolicy, ApprovalRequest, ApprovalVerdict, ChildFailure,
    ChildOutcome, CompactionConfig, ConfigSnapshot, Item, JsonString, ModelId, ModelRequest,
    ModelSpec, Parallelism, Part, Provenance, Provider, ProviderCapabilities, ProviderError,
    ProviderErrorKind, ProviderId, ReplaySafety, RequestConfig, Role, SessionEvent, SessionSpec,
    SessionState, SideEffect, StopReason, StreamEvent, SubagentConfig, Tool, ToolArgs, ToolConfig,
    ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolName, ToolOutput, ToolResultStatus,
    ToolSelection, TurnConfig, TurnEndReason, Usage, UserInput, decode_entry,
};
use asterpath_testing::ScriptedProvider;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use tokio::sync::Notify;

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
    let spawn_tool = spawned_config
        .tools
        .iter()
        .find(|tool| tool.name.as_str() == "spawn_agent")
        .expect("the scheduler registers the spawn tool");
    assert_eq!(spawn_tool.parallelism, Parallelism::Parallel);
    assert_eq!(spawn_tool.replay, ReplaySafety::Safe);
    assert_eq!(spawn_tool.side_effect, SideEffect::Mutating);
    assert_eq!(spawn_tool.provenance, Provenance::InProcess);
    assert_eq!(spawn_tool.timeout, None);

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

// --- #48: fan-out, admission, cancellation and timeout ---------------------

/// A provider that routes by request shape: tool results mean the parent's
/// follow-up step, `task-*` user text is a child, anything else is the
/// parent's first step.
struct RoutingProvider {
    first: Vec<Item>,
    child: ChildReply,
    /// When set, every child waits for a permit before answering.
    quota: Option<Arc<Quota>>,
}

type ChildReply = Arc<dyn Fn(&str) -> Vec<Result<StreamEvent, ProviderError>> + Send + Sync>;

impl Provider for RoutingProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("p")
    }

    fn capabilities(&self, _spec: &ModelSpec) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        if request
            .items
            .iter()
            .any(|item| matches!(item, Item::ToolResult { .. }))
        {
            return stream::iter(response_events(
                assistant("parent done"),
                StopReason::EndTurn,
            ))
            .boxed();
        }
        let task = request.items.iter().find_map(|item| match item {
            Item::Message {
                role: Role::User,
                parts,
            } => parts.iter().find_map(|part| match part {
                Part::Text { text } if text.starts_with("task-") => Some(text.clone()),
                _ => None,
            }),
            _ => None,
        });
        let Some(task) = task else {
            return stream::iter(response_events(self.first.clone(), StopReason::ToolUse)).boxed();
        };
        let events = (self.child)(&task);
        let Some(quota) = &self.quota else {
            return stream::iter(events).boxed();
        };
        let quota = Arc::clone(quota);
        stream::once(async move {
            quota.take().await;
            events
        })
        .flat_map(stream::iter)
        .boxed()
    }
}

/// Permits handed out to gated children; enables deterministic fan-out.
#[derive(Default)]
struct Quota {
    permits: AtomicUsize,
    notify: Notify,
}

impl Quota {
    fn release(&self, count: usize) {
        self.permits.fetch_add(count, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    async fn take(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.permits.load(Ordering::SeqCst) > 0 {
                self.permits.fetch_sub(1, Ordering::SeqCst);
                return;
            }
            notified.await;
        }
    }
}

fn response_events(items: Vec<Item>, stop: StopReason) -> Vec<Result<StreamEvent, ProviderError>> {
    let mut sequence = 0;
    let mut events = vec![Ok(StreamEvent::ResponseStarted {
        sequence,
        request_id: None,
    })];
    for (index, item) in items.into_iter().enumerate() {
        sequence += 1;
        events.push(Ok(StreamEvent::ItemStarted {
            sequence,
            output_index: index as u32,
        }));
        sequence += 1;
        events.push(Ok(StreamEvent::ItemCompleted {
            sequence,
            output_index: index as u32,
            item,
        }));
    }
    sequence += 1;
    events.push(Ok(StreamEvent::ResponseEnded {
        sequence,
        stop_reason: stop,
        usage: Usage::default(),
    }));
    events
}

fn spawn_call(call_id: &str, args: &str) -> Item {
    Item::ToolCall {
        call_id: call_id.to_owned(),
        provider_item_id: None,
        name: ToolName::new("spawn_agent"),
        raw_args: JsonString::new(args),
        raw: None,
    }
}

fn spec_with(subagents: SubagentConfig, turn: TurnConfig) -> SessionSpec {
    SessionSpec {
        subagents,
        turn,
        ..spec(None)
    }
}

fn completions(entries: &[LogEntry]) -> Vec<ChildOutcome> {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ChildSessionCompleted { outcome, .. } => Some(outcome),
            _ => None,
        })
        .collect()
}

fn all_tool_results(
    entries: &[LogEntry],
) -> Vec<(ToolResultStatus, Vec<Part>, Option<ToolErrorKind>)> {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                status,
                output,
                error,
                ..
            } => Some((
                status,
                output.unwrap_or_default(),
                error.map(|error| error.kind),
            )),
            _ => None,
        })
        .collect()
}

fn spawned(entries: &[LogEntry]) -> Vec<ToolCallId> {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ChildSessionSpawned {
                parent_tool_call_id,
                ..
            } => Some(parent_tool_call_id),
            _ => None,
        })
        .collect()
}

fn spawned_children(entries: &[LogEntry]) -> Vec<SessionId> {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ChildSessionSpawned { child, .. } => Some(child),
            _ => None,
        })
        .collect()
}

fn requested(entries: &[LogEntry]) -> Vec<ToolCallId> {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolCallRequested { tool_call_id, .. } => Some(tool_call_id),
            _ => None,
        })
        .collect()
}

async fn wait_for(
    asterpath: &Asterpath,
    session: SessionId,
    what: &str,
    predicate: impl Fn(&[SessionEvent]) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let events = decoded(&scan(asterpath.event_log(), session).await);
            if predicate(&events) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn fan_out_partial_failure_keeps_siblings_independent() {
    let db = temp_db("fan-out");
    let child: ChildReply = Arc::new(|task| match task {
        "task-boom" => vec![Err(ProviderError::new(
            ProviderErrorKind::InvalidRequest,
            "boom",
        ))],
        "task-a" => response_events(assistant("a done"), StopReason::EndTurn),
        _ => response_events(assistant("b done"), StopReason::EndTurn),
    });
    let provider = Arc::new(RoutingProvider {
        first: vec![
            spawn_call("call-a", r#"{"task":"task-a"}"#),
            spawn_call("call-boom", r#"{"task":"task-boom"}"#),
            spawn_call("call-b", r#"{"task":"task-b"}"#),
        ],
        child,
        quota: None,
    });
    let asterpath = open(simple_config(
        &db,
        vec![provider as Arc<dyn Provider>],
        Vec::new(),
    ))
    .await;
    let handle = asterpath.create_session(spec(None)).await.unwrap();
    handle.submit_input(input("go")).await.unwrap();
    drive_all(&handle).await;

    let entries = scan(asterpath.event_log(), handle.id()).await;
    assert_eq!(spawned(&entries).len(), 3);
    let outcomes = completions(&entries);
    assert_eq!(outcomes.len(), 3);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ChildOutcome::Completed { .. }))
            .count(),
        2
    );
    assert!(outcomes.iter().any(|outcome| matches!(
        outcome,
        ChildOutcome::Failed {
            reason: ChildFailure::TurnFailed,
            ..
        }
    )));

    // One independent tool result per call; the failed task is still a
    // successful spawn result, and the siblings keep their own summaries.
    let results = all_tool_results(&entries);
    assert_eq!(results.len(), 3);
    assert!(
        results
            .iter()
            .all(|(status, _, _)| *status == ToolResultStatus::Completed)
    );
    let outputs: Vec<String> = results
        .iter()
        .flat_map(|(_, parts, _)| parts.clone())
        .filter_map(|part| match part {
            Part::Text { text } => Some(text),
            _ => None,
        })
        .collect();
    assert!(outputs.contains(&"a done".to_owned()));
    assert!(outputs.contains(&"b done".to_owned()));
    assert!(
        outputs.iter().any(|text| text.contains("TurnFailed")),
        "the failed sibling is marked: {outputs:?}"
    );
    assert!(matches!(
        decoded(&entries).last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn the_ninth_spawn_queues_until_a_slot_is_released() {
    let db = temp_db("admission");
    let quota = Arc::new(Quota::default());
    let child: ChildReply =
        Arc::new(|task| response_events(assistant(&format!("done {task}")), StopReason::EndTurn));
    let first: Vec<Item> = (0..9)
        .map(|index| {
            spawn_call(
                &format!("call-{index}"),
                &format!(r#"{{"task":"task-{index}"}}"#),
            )
        })
        .collect();
    let provider = Arc::new(RoutingProvider {
        first,
        child,
        quota: Some(Arc::clone(&quota)),
    });
    let config = AsterpathConfig {
        tool: ToolConfig {
            max_parallel_calls: NonZeroUsize::new(16).expect("non-zero"),
            ..ToolConfig::default()
        },
        ..simple_config(&db, vec![provider as Arc<dyn Provider>], Vec::new())
    };
    let asterpath = open(config).await;
    let handle = asterpath.create_session(spec(None)).await.unwrap();
    let parent = handle.id();
    handle.submit_input(input("go")).await.unwrap();
    let driver = tokio::spawn({
        let handle = handle.clone();
        async move { drive_all(&handle).await }
    });

    wait_for(&asterpath, parent, "eight admitted children", |events| {
        events
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
            .count()
            == 8
            && events
                .iter()
                .filter(|event| matches!(event, SessionEvent::ToolCallRequested { .. }))
                .count()
                == 9
    })
    .await;
    let entries = scan(asterpath.event_log(), parent).await;
    let calls = requested(&entries);
    let admitted = spawned(&entries);
    assert_eq!(admitted.len(), 8, "the ninth call is queued");
    assert_eq!(
        calls.iter().find(|call| !admitted.contains(call)),
        calls.get(8),
        "the highest-seq call waits"
    );

    // Releasing one child frees its slot; the queued call is admitted.
    quota.release(1);
    wait_for(&asterpath, parent, "ninth admission", |events| {
        events
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
            .count()
            == 9
    })
    .await;
    let entries = scan(asterpath.event_log(), parent).await;
    let admitted = spawned(&entries);
    assert_eq!(admitted.len(), 9);
    assert_eq!(admitted[8], calls[8], "admission is FIFO by seq");

    quota.release(8);
    let events = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("drive finishes")
        .expect("driver task");
    assert!(matches!(
        events.last(),
        Some(AgentEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));
    let entries = scan(asterpath.event_log(), parent).await;
    assert_eq!(completions(&entries).len(), 9);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn parent_cancel_cancels_the_child_and_drops_the_queued_spawn() {
    let db = temp_db("parent-cancel");
    let quota = Arc::new(Quota::default());
    let child: ChildReply =
        Arc::new(|_task| response_events(assistant("never"), StopReason::EndTurn));
    let provider = Arc::new(RoutingProvider {
        first: vec![
            spawn_call("call-slow", r#"{"task":"task-slow"}"#),
            spawn_call("call-queued", r#"{"task":"task-queued"}"#),
        ],
        child,
        quota: Some(Arc::clone(&quota)),
    });
    let subagents = SubagentConfig {
        max_concurrent_children: NonZeroUsize::new(1).expect("non-zero"),
        cancel_grace: Duration::from_millis(100),
        ..SubagentConfig::default()
    };
    let asterpath = open(simple_config(
        &db,
        vec![provider as Arc<dyn Provider>],
        Vec::new(),
    ))
    .await;
    let handle = asterpath
        .create_session(spec_with(
            subagents,
            TurnConfig {
                cancel_grace: Duration::from_millis(300),
                ..TurnConfig::default()
            },
        ))
        .await
        .unwrap();
    let parent = handle.id();
    handle.submit_input(input("go")).await.unwrap();
    let driver = tokio::spawn({
        let handle = handle.clone();
        async move { drive_all(&handle).await }
    });

    wait_for(&asterpath, parent, "one admitted child", |events| {
        events
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
            .count()
            == 1
            && events
                .iter()
                .filter(|event| matches!(event, SessionEvent::ToolCallRequested { .. }))
                .count()
                == 2
    })
    .await;
    handle.cancel().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("delivers cancellation")
        .expect("driver task");

    let entries = scan(asterpath.event_log(), parent).await;
    let events = decoded(&entries);
    let admitted = spawned(&entries);
    assert_eq!(admitted.len(), 1, "the queued spawn creates no child");
    assert_eq!(
        completions(&entries),
        vec![ChildOutcome::Cancelled {
            reason: asterpath_protocol::CancelReason::Parent
        }]
    );
    let results = all_tool_results(&entries);
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|(status, _, _)| *status == ToolResultStatus::Interrupted)
    );

    let position = |kind: &str| {
        events
            .iter()
            .position(|event| event.kind() == kind)
            .expect(kind)
    };
    assert!(position("ChildSessionCompleted") < position("ToolResult"));
    assert!(position("ToolResult") < position("StepCommitted"));
    assert!(position("StepCommitted") < position("TurnCommitted"));
    assert!(matches!(
        events.last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Cancelled,
            ..
        })
    ));

    // The child got the cancellation and closed its own turn.
    let child = spawned_children(&entries)[0];
    let child_events = decoded(&scan(asterpath.event_log(), child).await);
    assert!(matches!(
        child_events.last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Cancelled,
            ..
        })
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn cancel_with_zero_grace_closes_the_open_child_wait() {
    let db = temp_db("cancel-zero-grace");
    let quota = Arc::new(Quota::default());
    let child: ChildReply =
        Arc::new(|_task| response_events(assistant("never"), StopReason::EndTurn));
    let provider = Arc::new(RoutingProvider {
        first: vec![spawn_call("call-wait", r#"{"task":"task-wait"}"#)],
        child,
        quota: Some(Arc::clone(&quota)),
    });
    let subagents = SubagentConfig {
        cancel_grace: Duration::ZERO,
        ..SubagentConfig::default()
    };
    let asterpath = open(simple_config(
        &db,
        vec![provider as Arc<dyn Provider>],
        Vec::new(),
    ))
    .await;
    let handle = asterpath
        .create_session(spec_with(
            subagents,
            TurnConfig {
                cancel_grace: Duration::ZERO,
                ..TurnConfig::default()
            },
        ))
        .await
        .unwrap();
    let parent = handle.id();
    handle.submit_input(input("go")).await.unwrap();
    let driver = tokio::spawn({
        let handle = handle.clone();
        async move { drive_all(&handle).await }
    });

    wait_for(&asterpath, parent, "one admitted child", |events| {
        events
            .iter()
            .any(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
    })
    .await;
    handle.cancel().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("delivers cancellation")
        .expect("driver task");

    let entries = scan(asterpath.event_log(), parent).await;
    assert_eq!(
        completions(&entries),
        vec![ChildOutcome::Cancelled {
            reason: asterpath_protocol::CancelReason::Parent
        }],
        "the dropped spawn future still closes the child wait"
    );
    assert_eq!(
        all_tool_results(&entries)
            .into_iter()
            .map(|(status, _, _)| status)
            .collect::<Vec<_>>(),
        vec![ToolResultStatus::Interrupted]
    );
    assert_eq!(handle.state().await.unwrap(), SessionState::Idle);
    let intents = asterpath_protocol::OpenIntents::fold(&entries).expect("folds");
    assert!(intents.is_idle(), "no waiting intent survives cancellation");
    assert!(
        !matches!(
            handle.compact_now().await,
            Err(asterpath::AsterError::Agent(
                asterpath_protocol::AgentError::Busy { .. }
            ))
        ),
        "an idle session is not stuck behind the closed wait"
    );

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn spawn_timeout_fires_and_reports_failed_timeout() {
    let db = temp_db("timeout");
    let quota = Arc::new(Quota::default());
    let child: ChildReply =
        Arc::new(|_task| response_events(assistant("never"), StopReason::EndTurn));
    let provider = Arc::new(RoutingProvider {
        first: vec![spawn_call(
            "call-hang",
            r#"{"task":"task-hang","timeout_ms":50}"#,
        )],
        child,
        quota: Some(Arc::clone(&quota)),
    });
    let subagents = SubagentConfig {
        cancel_grace: Duration::from_millis(100),
        ..SubagentConfig::default()
    };
    let asterpath = open(simple_config(
        &db,
        vec![provider as Arc<dyn Provider>],
        Vec::new(),
    ))
    .await;
    let handle = asterpath
        .create_session(spec_with(subagents, TurnConfig::default()))
        .await
        .unwrap();
    let parent = handle.id();
    handle.submit_input(input("go")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), drive_all(&handle))
        .await
        .expect("timeout closes the wait");

    let entries = scan(asterpath.event_log(), parent).await;
    let events = decoded(&entries);
    let position = |kind: &str| {
        events
            .iter()
            .position(|event| event.kind() == kind)
            .expect(kind)
    };
    assert!(position("TimerScheduled") < position("TimerFired"));
    assert!(position("TimerFired") < position("ChildSessionCompleted"));
    assert!(position("ChildSessionCompleted") < position("ToolResult"));
    assert_eq!(
        completions(&entries),
        vec![ChildOutcome::Cancelled {
            reason: asterpath_protocol::CancelReason::Timeout
        }]
    );
    let (status, _, error) = all_tool_results(&entries)
        .into_iter()
        .next()
        .expect("spawn result");
    assert_eq!(status, ToolResultStatus::Failed);
    assert_eq!(error, Some(ToolErrorKind::Timeout));
    assert!(matches!(
        events.last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));

    let children = spawned_children(&entries);
    let child_events = decoded(&scan(asterpath.event_log(), children[0]).await);
    assert!(matches!(
        child_events.last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Cancelled,
            ..
        })
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn child_completion_cancels_the_timeout_timer() {
    let db = temp_db("timeout-cancelled");
    let child: ChildReply =
        Arc::new(|_task| response_events(assistant("child done"), StopReason::EndTurn));
    let provider = Arc::new(RoutingProvider {
        first: vec![spawn_call(
            "call-fast",
            r#"{"task":"task-fast","timeout_ms":1000}"#,
        )],
        child,
        quota: None,
    });
    let asterpath = open(simple_config(
        &db,
        vec![provider as Arc<dyn Provider>],
        Vec::new(),
    ))
    .await;
    let handle = asterpath.create_session(spec(None)).await.unwrap();
    let parent = handle.id();
    handle.submit_input(input("go")).await.unwrap();
    drive_all(&handle).await;

    let entries = scan(asterpath.event_log(), parent).await;
    let kinds = kinds(&entries);
    assert!(kinds.contains(&"TimerScheduled"));
    assert!(kinds.contains(&"TimerCancelled"));
    assert!(!kinds.contains(&"TimerFired"), "the child won the race");
    assert_eq!(
        completions(&entries),
        vec![ChildOutcome::Completed {
            summary: asterpath_protocol::Summary {
                items: vec![text("child done")]
            }
        }]
    );
    let (status, output, _) = all_tool_results(&entries)
        .into_iter()
        .next()
        .expect("spawn result");
    assert_eq!(status, ToolResultStatus::Completed);
    assert_eq!(output, vec![text("child done")]);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn the_facade_global_live_child_limit_queues_spawns() {
    let db = temp_db("global-admission");
    let quota = Arc::new(Quota::default());
    let child: ChildReply =
        Arc::new(|task| response_events(assistant(&format!("done {task}")), StopReason::EndTurn));
    let first: Vec<Item> = (0..3)
        .map(|index| {
            spawn_call(
                &format!("call-{index}"),
                &format!(r#"{{"task":"task-{index}"}}"#),
            )
        })
        .collect();
    let provider = Arc::new(RoutingProvider {
        first,
        child,
        quota: Some(Arc::clone(&quota)),
    });
    // The per-parent limit (8) does not bind here; the facade's global one does.
    let asterpath = AsterpathBuilder::from_config(simple_config(
        &db,
        vec![provider as Arc<dyn Provider>],
        Vec::new(),
    ))
    .max_live_children(NonZeroUsize::new(2).expect("non-zero"))
    .build()
    .await
    .expect("facade builds");
    let handle = asterpath.create_session(spec(None)).await.unwrap();
    let parent = handle.id();
    handle.submit_input(input("go")).await.unwrap();
    let driver = tokio::spawn({
        let handle = handle.clone();
        async move { drive_all(&handle).await }
    });

    wait_for(&asterpath, parent, "two admitted children", |events| {
        events
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
            .count()
            == 2
            && events
                .iter()
                .filter(|event| matches!(event, SessionEvent::ToolCallRequested { .. }))
                .count()
                == 3
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let entries = scan(asterpath.event_log(), parent).await;
    assert_eq!(spawned(&entries).len(), 2, "the third spawn is queued");

    quota.release(3);
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("drive finishes")
        .expect("driver task");
    let entries = scan(asterpath.event_log(), parent).await;
    assert_eq!(completions(&entries).len(), 3);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}
