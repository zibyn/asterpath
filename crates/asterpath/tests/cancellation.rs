//! Turn-level cancellation and step-boundary steering through the public
//! `SessionHandle` API.
#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use asterpath::protocol::{
    AbortKind, AgentEvent, ApprovalDecision, ApprovalPolicy, ApprovalRequest, ApprovalVerdict,
    CompactionConfig, Item, JsonString, ModelId, ModelSpec, Parallelism, Part, PartDelta, Provider,
    ProviderCapabilities, ProviderError, ProviderErrorKind, ProviderId, ReplaySafety,
    RequestConfig, Role, SessionEvent, SessionSpec, SideEffect, StopReason, StreamEvent,
    SubagentConfig, Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput,
    ToolResultStatus, ToolSelection, TurnConfig, TurnEndReason, Usage, UserInput, decode_entry,
};
use asterpath::protocol::{InputId, LogEntry, LogQuery, SessionId, SessionStore};
use asterpath::{Asterpath, SessionHandle};
use asterpath_testing::ScriptedProvider;
use asterpath_testing::{TestTool, ToolCalls};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use tokio::sync::Notify;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-cancel-{tag}-{}-{}.db",
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

fn assistant_items(value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }]
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

fn descriptor(name: &str) -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new(name),
        String::new(),
        serde_json::json!({"type": "object"}),
    )
    .with_parallelism(Parallelism::Parallel)
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly)
}

fn spec(turn: TurnConfig) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: vec![text("be helpful")],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn,
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

fn input(id: u8, value: &str) -> UserInput {
    UserInput {
        id: InputId::from_bytes([id; 16]),
        parts: vec![text(value)],
    }
}

fn closure_tool(
    name: &str,
    invoke: impl Fn(ToolArgs, ToolContext) -> BoxFuture<'static, Result<ToolOutput, ToolError>>
    + Send
    + Sync
    + 'static,
) -> Arc<dyn Tool> {
    TestTool::new(descriptor(name), ToolCalls::default(), invoke)
}

/// Blocks until released; used to hold a step open for steering.
fn blocking_tool(started: Arc<Notify>, release: Arc<Notify>) -> Arc<dyn Tool> {
    closure_tool("blocking", move |_args, _ctx| {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            Ok(ToolOutput::new(vec![text("tool done")]))
        })
    })
}

/// First call streams one delta and then hangs; later calls complete normally.
#[derive(Default)]
struct BlockingProvider {
    calls: AtomicUsize,
}

impl BlockingProvider {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Provider for BlockingProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("p")
    }

    fn capabilities(&self, _spec: &ModelSpec) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    fn stream(
        &self,
        _request: asterpath::protocol::ModelRequest,
    ) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            stream::iter(vec![
                Ok(StreamEvent::ResponseStarted {
                    sequence: 0,
                    request_id: None,
                }),
                Ok(StreamEvent::PartDelta {
                    sequence: 1,
                    output_index: 0,
                    part_index: 0,
                    delta: PartDelta::Text {
                        text: "thin".to_owned(),
                    },
                }),
            ])
            .chain(stream::pending())
            .boxed()
        } else {
            stream::iter(vec![
                Ok(StreamEvent::ResponseStarted {
                    sequence: 0,
                    request_id: None,
                }),
                Ok(StreamEvent::ItemCompleted {
                    sequence: 1,
                    output_index: 0,
                    item: Item::Message {
                        role: Role::Assistant,
                        parts: vec![text("done")],
                        raw: None,
                    },
                }),
                Ok(StreamEvent::ResponseEnded {
                    sequence: 2,
                    stop_reason: StopReason::EndTurn,
                    usage: Usage::default(),
                }),
            ])
            .boxed()
        }
    }
}

struct AlwaysAsk;

impl ApprovalPolicy for AlwaysAsk {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        ApprovalVerdict::Ask
    }
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

fn decoded(entries: &[LogEntry]) -> Vec<SessionEvent> {
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodes"))
        .collect()
}

fn kinds(entries: &[LogEntry]) -> Vec<&'static str> {
    decoded(entries).iter().map(SessionEvent::kind).collect()
}

fn tool_results(entries: &[LogEntry]) -> Vec<(ToolResultStatus, Option<String>, bool)> {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                status,
                output,
                error,
                ..
            } => Some((status, error.map(|error| error.message), output.is_some())),
            _ => None,
        })
        .collect()
}

fn turn_reasons(entries: &[LogEntry]) -> Vec<TurnEndReason> {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::TurnCommitted { reason, .. } => Some(reason),
            _ => None,
        })
        .collect()
}

fn abort_kinds(entries: &[LogEntry]) -> Vec<(u32, AbortKind)> {
    decoded(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ModelCallAborted { attempt, kind, .. } => Some((attempt, kind)),
            _ => None,
        })
        .collect()
}

async fn drive_all(handle: &SessionHandle) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive"));
    }
    events
}

async fn build(
    db: &Path,
    provider: Arc<dyn Provider>,
    tools: Vec<Arc<dyn Tool>>,
    approval: Option<Arc<dyn ApprovalPolicy>>,
) -> Asterpath {
    let mut builder = Asterpath::builder()
        .database(db)
        .providers(vec![provider])
        .tools(tools);
    if let Some(approval) = approval {
        builder = builder.approval(approval);
    }
    builder.build().await.expect("build")
}

// --- L3: cancel during the model stream ----------------------------------

#[tokio::test]
async fn cancel_during_model_stream_aborts_without_retry_and_allows_a_new_turn() {
    let db = temp_db("model-cancel");
    let provider = Arc::new(BlockingProvider::default());
    let asterpath = build(&db, provider.clone(), Vec::new(), None).await;
    let handle = asterpath
        .create_session(spec(TurnConfig::default()))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();

    let other = handle.clone();
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        if matches!(event, AgentEvent::ModelDelta { .. }) {
            // Any handle for the session reaches the active drive.
            other.cancel().await.unwrap();
            other.cancel().await.unwrap();
        }
    }

    // The finished stream still holds the single-flight guard until dropped.
    drop(stream);

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "ModelCallAborted",
            "StepCommitted",
            "TurnCommitted",
        ]
    );
    assert_eq!(abort_kinds(&entries), vec![(1, AbortKind::Cancelled)]);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Cancelled]);
    assert_eq!(provider.calls(), 1, "cancel does not retry");

    // Replaying the recorded turn is a no-op (full recovery is #42).
    let before = entries.len();
    let replay = drive_all(&handle).await;
    assert!(matches!(replay.as_slice(), [AgentEvent::Idle { .. }]));
    assert_eq!(scan(asterpath.store(), handle.id()).await.len(), before);
    assert_eq!(provider.calls(), 1);
    assert!(matches!(
        handle.state().await.unwrap(),
        asterpath::protocol::SessionState::Idle
    ));

    // The session still accepts input and starts a new turn.
    handle.submit_input(input(2, "again")).await.unwrap();
    let events = drive_all(&handle).await;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnStarted { .. }))
    );
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        turn_reasons(&entries),
        vec![TurnEndReason::Cancelled, TurnEndReason::Completed]
    );
    assert_eq!(provider.calls(), 2);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

// --- L4 / S11: cancel during tools ---------------------------------------

#[tokio::test]
async fn cancel_with_zero_grace_interrupts_a_blocked_tool() {
    let db = temp_db("tool-zero");
    let started = Arc::new(Notify::new());
    let tool = closure_tool("blocking", move |_args, _ctx| {
        let started = Arc::clone(&started);
        Box::pin(async move {
            started.notify_one();
            std::future::pending::<Result<ToolOutput, ToolError>>().await
        })
    });
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("call-1", "blocking")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = build(&db, provider, vec![tool], None).await;
    let handle = asterpath
        .create_session(spec({
            let mut config = TurnConfig::default();
            config.cancel_grace = Duration::ZERO;
            config
        }))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();

    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        if matches!(event, AgentEvent::ToolStarted { .. }) {
            handle.cancel().await.unwrap();
        }
    }

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        tool_results(&entries),
        vec![(ToolResultStatus::Interrupted, None, false)]
    );
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Cancelled]);
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "AssistantMessage",
            "ToolCallRequested",
            "ToolResult",
            "StepCommitted",
            "TurnCommitted",
        ]
    );
    assert!(matches!(
        handle.state().await.unwrap(),
        asterpath::protocol::SessionState::Idle
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn cancel_with_grace_keeps_a_tool_result_that_returns_in_time() {
    let db = temp_db("tool-grace");
    let tool = closure_tool("cooperative", |_args, ctx| {
        Box::pin(async move {
            ctx.cancel.cancelled().await;
            Ok(ToolOutput::new(vec![text("finished")]))
        })
    });
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("call-1", "cooperative")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = build(&db, provider, vec![tool], None).await;
    let handle = asterpath
        .create_session(spec({
            let mut config = TurnConfig::default();
            config.cancel_grace = Duration::from_secs(1);
            config
        }))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();

    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        if matches!(event, AgentEvent::ToolStarted { .. }) {
            handle.cancel().await.unwrap();
        }
    }

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        tool_results(&entries),
        vec![(ToolResultStatus::Completed, None, true)]
    );
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Cancelled]);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}
// --- signal / approval wait ----------------------------------------------

#[tokio::test]
async fn cancel_interrupts_an_approval_wait_with_an_interrupted_tool_result() {
    let db = temp_db("approval-cancel");
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let tool = closure_tool("guarded", move |_args, _ctx| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(ToolOutput::new(vec![text("ran")])) })
    });
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("call-1", "guarded")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = build(&db, provider, vec![tool], Some(Arc::new(AlwaysAsk))).await;
    let _endpoint = asterpath.approval_gate().register();
    let handle = asterpath
        .create_session(spec(TurnConfig::default()))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();

    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        if matches!(event, AgentEvent::ApprovalRequested(_)) {
            assert_eq!(asterpath.approval_gate().pending().len(), 1);
            handle.cancel().await.unwrap();
        }
    }
    assert!(
        asterpath.approval_gate().pending().is_empty(),
        "a cancelled wait is no longer pending"
    );

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0, "cancel skips the tool");
    assert_eq!(
        tool_results(&entries),
        vec![(ToolResultStatus::Interrupted, None, false)]
    );
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Cancelled]);
    assert!(matches!(
        handle.state().await.unwrap(),
        asterpath::protocol::SessionState::Idle
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn idempotent_cancel_during_a_tool_and_idle_cancel_is_a_no_op() {
    let db = temp_db("idempotent");
    let started = Arc::new(Notify::new());
    let tool = closure_tool("blocking", move |_args, _ctx| {
        let started = Arc::clone(&started);
        Box::pin(async move {
            started.notify_one();
            std::future::pending::<Result<ToolOutput, ToolError>>().await
        })
    });
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("call-1", "blocking")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = build(&db, provider, vec![tool], None).await;
    let handle = asterpath
        .create_session(spec({
            let mut config = TurnConfig::default();
            config.cancel_grace = Duration::ZERO;
            config
        }))
        .await
        .unwrap();

    // Idle: no active turn, so cancel writes nothing.
    handle.cancel().await.unwrap();
    handle.cancel().await.unwrap();
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(kinds(&entries), ["SessionCreated"]);

    handle.submit_input(input(1, "go")).await.unwrap();
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        if matches!(event, AgentEvent::ToolStarted { .. }) {
            handle.cancel().await.unwrap();
            handle.cancel().await.unwrap();
            handle.cancel().await.unwrap();
        }
    }

    let decoded = decoded(&scan(asterpath.store(), handle.id()).await);
    let committed: Vec<_> = decoded
        .iter()
        .filter(|event| matches!(event, SessionEvent::TurnCommitted { .. }))
        .collect();
    assert_eq!(committed.len(), 1, "cancellation is idempotent");
    assert!(matches!(
        committed[0],
        SessionEvent::TurnCommitted {
            reason: TurnEndReason::Cancelled,
            ..
        }
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

// --- L2 / L12: steering ---------------------------------------------------

#[tokio::test]
async fn steering_enters_the_next_step_projection_in_seq_order() {
    let db = temp_db("steering");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let tool = blocking_tool(Arc::clone(&started), Arc::clone(&release));
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("call-1", "blocking")],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                assistant_items("done"),
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = build(&db, provider.clone(), vec![tool], None).await;
    let handle = asterpath
        .create_session(spec(TurnConfig::default()))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();

    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        events.push(event.clone());
        if matches!(event, AgentEvent::ToolStarted { .. }) {
            // Two steering inputs land while step 1 is still open.
            handle.submit_input(input(2, "first steer")).await.unwrap();
            handle.submit_input(input(3, "second steer")).await.unwrap();
            release.notify_one();
        }
    }

    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::TurnStarted { .. }))
            .count(),
        1,
        "steering stays in the same turn"
    );
    assert_eq!(
        turn_reasons(&scan(asterpath.store(), handle.id()).await),
        vec![TurnEndReason::Completed]
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let user_texts = |items: &[Item]| -> Vec<String> {
        items
            .iter()
            .filter_map(|item| match item {
                Item::Message {
                    role: Role::User,
                    parts,
                    ..
                } => Some(parts),
                _ => None,
            })
            .flat_map(|parts| parts.iter())
            .filter_map(|part| match part {
                Part::Text { text } => Some(text.clone()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(
        user_texts(&requests[0].items),
        vec!["go"],
        "the current step's projection does not contain steering"
    );
    assert_eq!(
        user_texts(&requests[1].items),
        vec!["go", "first steer", "second steer"],
        "pending inputs enter the next projection by seq"
    );

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn input_during_a_persistent_wait_does_not_wake_it_but_steers_the_next_step() {
    let db = temp_db("wait-steer");
    let tool = closure_tool("guarded", |_args, _ctx| {
        Box::pin(async { Ok(ToolOutput::new(vec![text("ran")])) })
    });
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("call-1", "guarded")],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                assistant_items("done"),
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = build(&db, provider.clone(), vec![tool], Some(Arc::new(AlwaysAsk))).await;
    let _endpoint = asterpath.approval_gate().register();
    let handle = asterpath
        .create_session(spec(TurnConfig::default()))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();

    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        if let AgentEvent::ApprovalRequested(request) = event {
            handle.submit_input(input(2, "steer")).await.unwrap();
            // The persistent wait is only driven by its signal.
            let still_waiting =
                tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
            assert!(still_waiting.is_err(), "input must not wake the wait");
            handle
                .approve(request.tool_call_id, ApprovalDecision::Allow)
                .await
                .unwrap();
        }
    }

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let contains_steer = requests[1].items.iter().any(|item| {
        matches!(
            item,
            Item::Message { role: Role::User, parts, .. }
                if parts.iter().any(|part| matches!(part, Part::Text { text } if text == "steer"))
        )
    });
    assert!(contains_steer, "steering is visible to the next step");
    assert_eq!(
        turn_reasons(&scan(asterpath.store(), handle.id()).await),
        vec![TurnEndReason::Completed]
    );

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

// --- turn-end mapping -----------------------------------------------------

#[tokio::test]
async fn max_steps_per_turn_ends_with_max_steps_exceeded() {
    let db = temp_db("max-steps");
    let tool = closure_tool("echo", |_args, _ctx| {
        Box::pin(async { Ok(ToolOutput::new(vec![text("ok")])) })
    });
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("call-1", "echo")],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                assistant_items("done"),
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = build(&db, provider.clone(), vec![tool], None).await;
    let handle = asterpath
        .create_session(spec({
            let mut config = TurnConfig::default();
            config.max_steps_per_turn = Some(1);
            config
        }))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();
    drive_all(&handle).await;

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        turn_reasons(&entries),
        vec![TurnEndReason::MaxStepsExceeded]
    );
    assert_eq!(
        kinds(&entries)
            .iter()
            .filter(|kind| **kind == "StepStarted")
            .count(),
        1
    );
    assert_eq!(provider.requests().len(), 1);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn context_window_stop_reason_and_provider_error_map_to_context_exhausted() {
    let db = temp_db("context-stop");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_items("cut"),
        StopReason::ContextWindowExceeded,
        Usage::default(),
    ));
    let asterpath = build(&db, provider, Vec::new(), None).await;
    let handle = asterpath
        .create_session(spec(TurnConfig::default()))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();
    drive_all(&handle).await;
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        turn_reasons(&entries),
        vec![TurnEndReason::ContextExhausted]
    );
    let stop = decoded(&entries).into_iter().find_map(|event| match event {
        SessionEvent::AssistantMessage { stop_reason, .. } => Some(stop_reason),
        _ => None,
    });
    assert_eq!(stop, Some(StopReason::ContextWindowExceeded));
    asterpath.shutdown().await.unwrap();
    cleanup(&db);

    let db = temp_db("context-error");
    let provider = Arc::new(ScriptedProvider::new("p").fail(ProviderError::new(
        ProviderErrorKind::ContextExceeded,
        "too long",
    )));
    let asterpath = build(&db, provider.clone(), Vec::new(), None).await;
    let handle = asterpath
        .create_session(spec(TurnConfig::default()))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();
    drive_all(&handle).await;
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        turn_reasons(&entries),
        vec![TurnEndReason::ContextExhausted]
    );
    assert_eq!(abort_kinds(&entries), vec![(1, AbortKind::Rejected)]);
    assert_eq!(
        provider.requests().len(),
        1,
        "context exhaustion is not retried"
    );
    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn cancel_interrupts_the_retry_backoff_sleep() {
    let db = temp_db("backoff-cancel");
    let provider = Arc::new(
        ScriptedProvider::new("p").fail(ProviderError::new(ProviderErrorKind::Transport, "reset")),
    );
    let asterpath = build(&db, provider.clone(), Vec::new(), None).await;
    let handle = asterpath
        .create_session(spec({
            let mut config = TurnConfig::default();
            config.initial_backoff = Duration::from_secs(10);
            config.max_backoff = Duration::from_secs(30);
            config
        }))
        .await
        .unwrap();
    handle.submit_input(input(1, "go")).await.unwrap();

    let mut stream = handle.drive();
    let other = handle.clone();
    // No live event marks the backoff, so cancel once the drive is parked.
    let canceller = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        other.cancel().await.unwrap();
    };
    let driver = async {
        while let Some(event) = stream.next().await {
            event.expect("drive");
        }
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(driver, canceller);
    })
    .await
    .expect("cancel interrupts the backoff sleep");

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        abort_kinds(&entries),
        vec![(1, AbortKind::Transport), (2, AbortKind::Cancelled)],
        "the cancelled backoff is the last recorded attempt"
    );
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Cancelled]);
    assert_eq!(provider.requests().len(), 1, "the next attempt never runs");

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}
