//! Structured output modes and the output tool (ADR-0034).

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath::protocol::{
    AgentEvent, CompactionConfig, InputId, Item, JsonString, ModelId, ModelSpec, OutputFormat,
    OutputMode, Part, Provenance, ProviderCapabilities, ProviderErrorKind, ProviderId,
    ReplaySafety, RequestConfig, Role, SessionEvent, SessionSpec, SessionStore, SideEffect,
    StopReason, SubagentConfig, Tool, ToolArgs, ToolChoice, ToolContext, ToolDescriptor, ToolError,
    ToolName, ToolOutput, ToolResultStatus, ToolSelection, TurnConfig, TurnEndReason, Usage,
    UserInput, decode_entry, scan_all,
};
use asterpath::{AsterError, Asterpath, OwnedSession};
use asterpath_testing::{Failpoint, FailpointEventLog, InMemoryEventLog, Point, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

const OUTPUT: &str = "answer";

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn reply(value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }]
}

fn call(call_id: &str, name: &str, args: &str) -> Item {
    Item::ToolCall {
        call_id: call_id.to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(args),
        raw: None,
    }
}

fn capabilities(structured_output: bool, forced_tool_choice: bool) -> ProviderCapabilities {
    let mut capabilities = ProviderCapabilities::default();
    capabilities.structured_output = structured_output;
    capabilities.forced_tool_choice = forced_tool_choice;
    capabilities
}

fn provider(structured_output: bool, forced_tool_choice: bool) -> ScriptedProvider {
    ScriptedProvider::new("p")
        .with_capabilities(capabilities(structured_output, forced_tool_choice))
}

fn spec(mode: OutputMode, tool_choice: ToolChoice) -> SessionSpec {
    let mut request = RequestConfig::default();
    request.tool_choice = tool_choice;
    request.output_format =
        Some(OutputFormat::new(OUTPUT, serde_json::json!({"type": "object"})).with_mode(mode));
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: Vec::new(),
        tools: ToolSelection::All,
        request,
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

struct Echo {
    descriptor: ToolDescriptor,
    calls: Mutex<usize>,
}

impl Echo {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            descriptor: ToolDescriptor::new(
                ToolName::new("echo"),
                String::new(),
                serde_json::json!({"type": "object"}),
            )
            .with_replay(ReplaySafety::Safe)
            .with_side_effect(SideEffect::ReadOnly),
            calls: Mutex::new(0),
        })
    }
}

impl Tool for Echo {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        *self.calls.lock().unwrap() += 1;
        Box::pin(async { Ok(ToolOutput::new(vec![text("echoed")])) })
    }
}

async fn build(
    store: Arc<dyn SessionStore>,
    provider: Arc<ScriptedProvider>,
    tools: Vec<Arc<dyn Tool>>,
) -> Asterpath {
    Asterpath::builder()
        .store(store)
        .providers(vec![provider])
        .tools(tools)
        .build()
        .await
        .expect("build")
}

async fn drive(handle: &OwnedSession) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive"));
    }
    events
}

struct Run {
    log: Vec<SessionEvent>,
    events: Vec<AgentEvent>,
}

impl Run {
    fn reason(&self) -> Option<TurnEndReason> {
        self.events.iter().find_map(|event| match event {
            AgentEvent::TurnCommitted { reason, .. } => Some(reason.clone()),
            _ => None,
        })
    }

    /// Recorded intents as (name, provenance).
    fn intents(&self) -> Vec<(String, Provenance)> {
        self.log
            .iter()
            .filter_map(|event| match event {
                SessionEvent::ToolCallRequested {
                    name, provenance, ..
                } => Some((name.as_str().to_owned(), *provenance)),
                _ => None,
            })
            .collect()
    }

    fn results(&self) -> Vec<(ToolResultStatus, Option<Vec<Part>>)> {
        self.log
            .iter()
            .filter_map(|event| match event {
                SessionEvent::ToolResult { status, output, .. } => Some((*status, output.clone())),
                _ => None,
            })
            .collect()
    }
}

async fn run(
    provider: &Arc<ScriptedProvider>,
    spec: SessionSpec,
    tools: Vec<Arc<dyn Tool>>,
) -> Run {
    let store: Arc<dyn SessionStore> = Arc::new(InMemoryEventLog::new());
    let asterpath = build(store.clone(), provider.clone(), tools).await;
    let handle = asterpath
        .create_session(spec)
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    handle.submit_input(input()).await.expect("input");
    let events = drive(&handle).await;
    Run {
        log: logged(&*store, &handle).await,
        events,
    }
}

async fn logged(store: &dyn SessionStore, handle: &OwnedSession) -> Vec<SessionEvent> {
    scan_all(store, handle.id())
        .await
        .expect("scan")
        .iter()
        .map(|entry| decode_entry(entry).expect("decode"))
        .collect()
}

fn input() -> UserInput {
    UserInput {
        id: InputId::from_bytes([1; 16]),
        parts: vec![text("go")],
    }
}

fn delivered() -> (ToolResultStatus, Option<Vec<Part>>) {
    (
        ToolResultStatus::Completed,
        Some(vec![text("result delivered")]),
    )
}

#[test]
fn a_format_without_a_mode_decodes_as_native() {
    let format: OutputFormat =
        serde_json::from_str(r#"{"name":"answer","schema":{"type":"object"}}"#).unwrap();
    assert_eq!(format.mode, OutputMode::Native);
}

#[tokio::test]
async fn tool_mode_delivers_through_the_output_tool_and_ends_the_turn() {
    let provider = Arc::new(provider(false, true).respond(
        vec![call("c1", OUTPUT, r#"{"city":"Paris"}"#)],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let run = run(
        &provider,
        spec(OutputMode::Tool, ToolChoice::Auto),
        Vec::new(),
    )
    .await;

    let requests = provider.requests();
    assert_eq!(requests.len(), 1, "no model call after the output tool");
    let request = &requests[0];
    assert_eq!(request.output_format, None);
    assert_eq!(request.tool_choice, ToolChoice::Required);
    let tool = request
        .tools
        .iter()
        .find(|tool| tool.name.as_str() == OUTPUT);
    assert_eq!(tool.expect("output tool").strict, Some(true));
    assert!(
        request
            .baseline_tools
            .iter()
            .any(|tool| tool.name.as_str() == OUTPUT)
    );

    assert_eq!(run.reason(), Some(TurnEndReason::Completed));
    assert_eq!(run.intents(), [(OUTPUT.to_owned(), Provenance::Output)]);
    assert_eq!(run.results(), [delivered()]);
    let started: Vec<_> = run
        .events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolStarted { tool, args, .. } => Some((tool.as_str(), args.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(started, [(OUTPUT, r#"{"city":"Paris"}"#)]);
    assert!(run.events.iter().any(|event| matches!(
        event,
        AgentEvent::ToolFinished {
            status: ToolResultStatus::Completed,
            ..
        }
    )));
}

#[tokio::test]
async fn a_text_reply_in_tool_mode_still_completes() {
    let provider = Arc::new(provider(false, true).respond(
        reply("just text"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let run = run(
        &provider,
        spec(OutputMode::Tool, ToolChoice::Auto),
        Vec::new(),
    )
    .await;
    assert_eq!(run.reason(), Some(TurnEndReason::Completed));
    assert!(run.intents().is_empty());
}

#[tokio::test]
async fn auto_picks_native_then_tool_then_unsupported() {
    for (structured, forced, tool, reason) in [
        (true, true, false, TurnEndReason::Completed),
        (false, true, true, TurnEndReason::Completed),
        (false, false, false, TurnEndReason::Failed),
    ] {
        let provider = Arc::new(provider(structured, forced).respond(
            reply("{}"),
            StopReason::EndTurn,
            Usage::default(),
        ));
        let run = run(
            &provider,
            spec(OutputMode::Auto, ToolChoice::Auto),
            Vec::new(),
        )
        .await;
        assert_eq!(run.reason(), Some(reason.clone()), "{structured} {forced}");
        let unsupported = run.log.iter().any(|event| {
            matches!(
                event,
                SessionEvent::ModelCallAborted { error: Some(error), .. }
                    if error.kind == ProviderErrorKind::Unsupported
            )
        });
        assert_eq!(
            unsupported,
            reason == TurnEndReason::Failed,
            "{structured} {forced}"
        );
        if let Some(request) = provider.requests().first() {
            let has_tool = request.tools.iter().any(|t| t.name.as_str() == OUTPUT);
            assert_eq!(has_tool, tool, "{structured} {forced}");
            assert_eq!(
                request.output_format.is_none(),
                tool,
                "{structured} {forced}"
            );
        }
    }
}

#[tokio::test]
async fn auto_with_tool_choice_none_uses_native() {
    let provider =
        Arc::new(provider(true, true).respond(reply("{}"), StopReason::EndTurn, Usage::default()));
    let run = run(
        &provider,
        spec(OutputMode::Auto, ToolChoice::None),
        Vec::new(),
    )
    .await;
    assert_eq!(run.reason(), Some(TurnEndReason::Completed));
    let request = &provider.requests()[0];
    assert!(request.output_format.is_some());
    assert_eq!(request.tool_choice, ToolChoice::None);
}

#[tokio::test]
async fn other_calls_of_the_delivering_step_still_run() {
    let echo = Echo::new();
    let provider = Arc::new(provider(false, true).respond(
        vec![call("c1", "echo", "{}"), call("c2", OUTPUT, "{}")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let run = run(
        &provider,
        spec(OutputMode::Tool, ToolChoice::Auto),
        vec![echo.clone()],
    )
    .await;

    assert_eq!(*echo.calls.lock().unwrap(), 1);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(run.reason(), Some(TurnEndReason::Completed));
    assert_eq!(
        run.intents(),
        [
            ("echo".to_owned(), Provenance::InProcess),
            (OUTPUT.to_owned(), Provenance::Output),
        ]
    );
}

#[tokio::test]
async fn every_output_call_is_closed() {
    let provider = Arc::new(provider(false, true).respond(
        vec![
            call("c1", OUTPUT, r#"{"n":1}"#),
            call("c2", OUTPUT, r#"{"n":2}"#),
        ],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let run = run(
        &provider,
        spec(OutputMode::Tool, ToolChoice::Auto),
        Vec::new(),
    )
    .await;
    assert_eq!(run.results(), [delivered(), delivered()]);
    assert_eq!(run.reason(), Some(TurnEndReason::Completed));
}

#[tokio::test]
async fn recovery_closes_a_recorded_output_intent_without_a_model_call() {
    // Appends: SessionCreated, InputRecorded, TurnStarted, StepStarted,
    // AssistantMessage, ToolCallRequested, then the ToolResult dies.
    let shared = Arc::new(InMemoryEventLog::new());
    let failing: Arc<dyn SessionStore> = Arc::new(FailpointEventLog::new(
        shared.clone(),
        Failpoint::panic(Point::AppendBeforeCommit)
            .with_occurrence(NonZeroUsize::new(7).expect("non-zero")),
    ));
    let live = Arc::new(provider(false, true).respond(
        vec![call("c1", OUTPUT, "{}")],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = build(failing, live, Vec::new()).await;
    let handle = asterpath
        .create_session(spec(OutputMode::Auto, ToolChoice::Auto))
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    handle.submit_input(input()).await.expect("input");
    let driven = handle.clone();
    let crashed = tokio::spawn(async move { drive(&driven).await }).await;
    assert!(crashed.is_err_and(|error| error.is_panic()));

    // Auto now resolves to native: only the recorded intent marks the call.
    let recovered = Arc::new(provider(true, true));
    let asterpath = build(shared.clone(), recovered.clone(), vec![Echo::new()]).await;
    let handle = asterpath
        .open_session(handle.id())
        .await
        .expect("open")
        .claim()
        .await
        .unwrap();
    let events = drive(&handle).await;
    let run = Run {
        log: logged(&*shared, &handle).await,
        events,
    };

    assert!(recovered.requests().is_empty());
    assert_eq!(run.results(), [delivered()]);
    assert_eq!(run.reason(), Some(TurnEndReason::Completed));
}

#[tokio::test]
async fn contradictory_output_configs_are_rejected() {
    let provider = Arc::new(provider(true, true));
    let asterpath = build(
        Arc::new(InMemoryEventLog::new()),
        provider,
        vec![Echo::new()],
    )
    .await;
    let rejected = |spec| async { asterpath.create_session(spec).await.err() };

    for choice in [ToolChoice::None, ToolChoice::Tool(ToolName::new("echo"))] {
        let error = rejected(spec(OutputMode::Tool, choice)).await;
        assert!(
            matches!(error, Some(AsterError::Config { .. })),
            "{error:?}"
        );
    }
    let mut clash = spec(OutputMode::Auto, ToolChoice::Auto);
    clash.request.output_format = Some(
        OutputFormat::new("echo", serde_json::json!({"type": "object"}))
            .with_mode(OutputMode::Auto),
    );
    let error = rejected(clash).await;
    assert!(
        matches!(error, Some(AsterError::Config { .. })),
        "{error:?}"
    );
    assert!(
        rejected(spec(
            OutputMode::Tool,
            ToolChoice::Tool(ToolName::new(OUTPUT))
        ))
        .await
        .is_none()
    );
}
