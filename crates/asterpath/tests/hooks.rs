//! Typed hooks through the facade (ADR-0025): an embedder writes hooks and
//! tools against `asterpath` alone.
#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath::protocol::{
    AgentEvent, CommittedRecord, CompactionConfig, Gate, HookError, Hooks, InputTokens, Item,
    JsonString, ModelId, ModelSpec, Part, Provider, ProviderId, ReplaySafety, RequestConfig, Role,
    SessionEvent, SessionSpec, SideEffect, StepCtx, StopReason, SubagentConfig, Tool, ToolArgs,
    ToolCallCtx, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolGate, ToolName,
    ToolOutput, ToolResultStatus, ToolResultView, ToolSelection, TurnConfig, TurnEndReason, Usage,
    UserInput,
};
use asterpath::{Asterpath, InputId, SessionHandle, SessionId};
use asterpath_testing::ScriptedProvider;
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-hooks-{tag}-{}-{}.db",
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
        raw: None,
    }]
}

fn tool_call(call_id: &str, name: &str, args: &str) -> Item {
    Item::ToolCall {
        call_id: call_id.to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(args),
        raw: None,
    }
}

fn tokens(input: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached: input,
            ..InputTokens::default()
        },
        ..Usage::default()
    }
}

struct EchoTool {
    descriptor: ToolDescriptor,
    calls: Mutex<Vec<String>>,
}

impl EchoTool {
    fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            descriptor: ToolDescriptor::new(
                ToolName::new(name),
                String::new(),
                serde_json::json!({"type": "object"}),
            )
            .with_replay(ReplaySafety::Safe)
            .with_side_effect(SideEffect::ReadOnly),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl Tool for EchoTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.lock().unwrap().push(args.as_str().to_owned());
        Box::pin(async move { Ok(ToolOutput::new(vec![text("secret-token-123")])) })
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: Vec::new(),
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

fn input(message: &str) -> UserInput {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    UserInput {
        id: InputId::from_bytes(bytes),
        parts: vec![text(message)],
    }
}

async fn drive_all(handle: &SessionHandle) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive succeeds"));
    }
    events
}

fn end_reason(events: &[AgentEvent]) -> Option<TurnEndReason> {
    events.iter().find_map(|event| match event {
        AgentEvent::TurnCommitted { reason, .. } => Some(reason.clone()),
        _ => None,
    })
}

/// Collects every committed record it is shown.
#[derive(Default)]
struct Recorder(Mutex<Vec<CommittedRecord>>);

impl Recorder {
    fn events(&self, session: SessionId) -> Vec<SessionEvent> {
        self.records(session)
            .into_iter()
            .map(|record| record.event)
            .collect()
    }

    fn records(&self, session: SessionId) -> Vec<CommittedRecord> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record.session == session)
            .cloned()
            .collect()
    }
}

impl Hooks for Recorder {
    fn on_record(&self, record: &CommittedRecord) {
        self.0.lock().unwrap().push(record.clone());
    }
}

async fn run(
    tag: &str,
    provider: ScriptedProvider,
    tools: Vec<Arc<dyn Tool>>,
    hooks: Vec<Arc<dyn Hooks>>,
) -> (SessionId, Vec<AgentEvent>) {
    let db = temp_db(tag);
    let provider: Arc<dyn Provider> = Arc::new(provider);
    let mut builder = Asterpath::builder()
        .database(&db)
        .provider(provider)
        .tools(tools);
    for hook in hooks {
        builder = builder.hooks(hook);
    }
    let asterpath = builder.build().await.expect("facade builds");
    let handle = asterpath.create_session(spec()).await.expect("session");
    handle.submit_input(input("go")).await.expect("input");
    let events = drive_all(&handle).await;
    asterpath.shutdown().await.expect("shutdown");
    cleanup(&db);
    (handle.id(), events)
}

// --- after_tool -------------------------------------------------------------

struct Redact;

impl Hooks for Redact {
    fn after_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        result: &'a mut ToolResultView,
    ) -> BoxFuture<'a, Result<(), HookError>> {
        for part in &mut result.output {
            if let Part::Text { text } = part {
                *text = text.replace("secret-token-123", "[redacted]");
            }
        }
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn after_tool_redacts_the_output_before_it_is_logged() {
    let echo = EchoTool::new("echo");
    let recorder = Arc::new(Recorder::default());
    let provider = ScriptedProvider::new("p")
        .respond(
            vec![tool_call("c1", "echo", "{}")],
            StopReason::ToolUse,
            Usage::default(),
        )
        .respond(assistant("done"), StopReason::EndTurn, Usage::default());
    let (session, events) = run(
        "redact",
        provider,
        vec![echo],
        vec![Arc::new(Redact), recorder.clone()],
    )
    .await;

    assert_eq!(end_reason(&events), Some(TurnEndReason::Completed));
    let outputs: Vec<_> = recorder
        .events(session)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult { output, .. } => output,
            _ => None,
        })
        .collect();
    assert_eq!(outputs, [vec![text("[redacted]")]]);
}

// --- before_step + usage() --------------------------------------------------

struct Budget {
    limit: u64,
}

impl Hooks for Budget {
    fn before_step<'a>(&'a self, ctx: &'a StepCtx) -> BoxFuture<'a, Result<Gate, HookError>> {
        Box::pin(async move {
            let report = ctx.usage().await?;
            if report.own.total() + report.descendants.total() >= self.limit {
                return Ok(Gate::EndTurn {
                    reason: "budget".to_owned(),
                });
            }
            Ok(Gate::Continue)
        })
    }
}

#[tokio::test]
async fn before_step_ends_the_turn_from_usage() {
    let echo = EchoTool::new("echo");
    let provider = ScriptedProvider::new("p").respond(
        vec![tool_call("c1", "echo", "{}")],
        StopReason::ToolUse,
        tokens(50),
    );
    let (_, events) = run(
        "budget",
        provider,
        vec![echo],
        vec![Arc::new(Budget { limit: 40 })],
    )
    .await;

    assert_eq!(
        end_reason(&events),
        Some(TurnEndReason::Stopped {
            reason: "budget".to_owned()
        }),
        "the second step is refused once the first step's usage is committed"
    );
}

// --- before_tool: rewrite, deny, order, short-circuit -----------------------

struct Trace {
    name: &'static str,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Trace {
    fn note(&self, ctx: &ToolCallCtx) {
        self.seen
            .lock()
            .unwrap()
            .push(format!("{}:{}", self.name, ctx.tool.as_str()));
    }
}

/// Rewrites every call's arguments.
struct Rewrite(Trace);

impl Hooks for Rewrite {
    fn before_tool<'a>(
        &'a self,
        ctx: &'a ToolCallCtx,
        args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        self.0.note(ctx);
        *args = JsonString::new(r#"{"q":"rewritten"}"#);
        Box::pin(async { Ok(ToolGate::Allow) })
    }
}

/// Denies the `blocked` tool, and sees the rewritten arguments.
struct DenyBlocked(Trace);

impl Hooks for DenyBlocked {
    fn before_tool<'a>(
        &'a self,
        ctx: &'a ToolCallCtx,
        args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        self.0.note(ctx);
        assert_eq!(args.as_str(), r#"{"q":"rewritten"}"#);
        let gate = if ctx.tool.as_str() == "blocked" {
            ToolGate::Deny {
                reason: Some("not allowed".to_owned()),
            }
        } else {
            ToolGate::Allow
        };
        Box::pin(async move { Ok(gate) })
    }
}

/// Only observes.
struct Last(Trace);

impl Hooks for Last {
    fn before_tool<'a>(
        &'a self,
        ctx: &'a ToolCallCtx,
        _args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        self.0.note(ctx);
        Box::pin(async { Ok(ToolGate::Allow) })
    }
}

#[tokio::test]
async fn before_tool_hooks_rewrite_and_deny_in_registration_order() {
    let echo = EchoTool::new("echo");
    let blocked = EchoTool::new("blocked");
    let recorder = Arc::new(Recorder::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let trace = |name| Trace {
        name,
        seen: Arc::clone(&seen),
    };
    let provider = ScriptedProvider::new("p")
        .respond(
            vec![
                tool_call("c1", "echo", r#"{"q":"original"}"#),
                tool_call("c2", "blocked", r#"{"q":"original"}"#),
            ],
            StopReason::ToolUse,
            Usage::default(),
        )
        .respond(assistant("done"), StopReason::EndTurn, Usage::default());
    let (session, events) = run(
        "before-tool",
        provider,
        vec![echo.clone(), blocked.clone()],
        vec![
            Arc::new(Rewrite(trace("rewrite"))),
            Arc::new(DenyBlocked(trace("deny"))),
            Arc::new(Last(trace("last"))),
            recorder.clone(),
        ],
    )
    .await;

    assert_eq!(end_reason(&events), Some(TurnEndReason::Completed));
    assert_eq!(echo.calls(), [r#"{"q":"rewritten"}"#]);
    assert!(blocked.calls().is_empty(), "a denied call never runs");
    assert_eq!(
        *seen.lock().unwrap(),
        [
            "rewrite:echo",
            "deny:echo",
            "last:echo",
            "rewrite:blocked",
            "deny:blocked",
        ],
        "hooks run in registration order and the first Deny short-circuits"
    );

    let logged = recorder.events(session);
    let intents: Vec<_> = logged
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolCallRequested { raw_args, .. } => Some(raw_args.as_str().to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(
        intents,
        [r#"{"q":"rewritten"}"#, r#"{"q":"rewritten"}"#],
        "the recorded intent carries the rewritten arguments"
    );
    let denied = logged.iter().any(|event| {
        matches!(
            event,
            SessionEvent::ToolResult {
                status: ToolResultStatus::Failed,
                error: Some(info),
                ..
            } if info.kind == ToolErrorKind::Denied && info.message == "not allowed"
        )
    });
    assert!(denied, "the denial is recorded as a failed result");
}

// --- on_record ---------------------------------------------------------------

struct Panics;

impl Hooks for Panics {
    fn on_record(&self, _record: &CommittedRecord) {
        panic!("observer bug");
    }
}

#[tokio::test]
async fn on_record_sees_parent_and_child_records_in_order_despite_a_panicking_hook() {
    let recorder = Arc::new(Recorder::default());
    let provider = ScriptedProvider::new("p")
        .respond(
            vec![tool_call("c1", "spawn_agent", r#"{"task":"look"}"#)],
            StopReason::ToolUse,
            Usage::default(),
        )
        .respond(
            assistant("child done"),
            StopReason::EndTurn,
            Usage::default(),
        )
        .respond(
            assistant("parent done"),
            StopReason::EndTurn,
            Usage::default(),
        );
    let (parent, events) = run(
        "on-record",
        provider,
        Vec::new(),
        vec![Arc::new(Panics), recorder.clone()],
    )
    .await;
    assert_eq!(end_reason(&events), Some(TurnEndReason::Completed));

    let child = recorder
        .events(parent)
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { child, .. } => Some(child),
            _ => None,
        })
        .expect("the parent spawned a child");

    for session in [parent, child] {
        let records = recorder.records(session);
        let seqs: Vec<u64> = records.iter().map(|record| record.seq).collect();
        let expected: Vec<u64> = (seqs[0]..seqs[0] + seqs.len() as u64).collect();
        assert_eq!(seqs, expected, "every record once, in session order");
        assert!(matches!(
            records.first().map(|record| &record.event),
            Some(SessionEvent::SessionCreated { .. })
        ));
        assert!(matches!(
            records.last().map(|record| &record.event),
            Some(SessionEvent::TurnCommitted { .. })
        ));
    }
}
