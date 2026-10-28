//! Mid-session configuration changes (ADR-0030, #109, #110, #111):
//! `update_config` writes a `ConfigChanged`, and every step folds the config
//! at its `StepStarted`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use asterpath::protocol::{
    CompactionConfig, ConfigPatch, ImageSource, InputId, Item, JsonString, ModelId, ModelRequest,
    ModelSpec, Part, Provider, ProviderCapabilities, ProviderCatalog, ProviderError,
    ProviderErrorKind, ProviderId, RequestConfig, Role, SessionEvent, SessionSpec, StopReason,
    StreamEvent, SubagentConfig, SystemSection, ToolName, ToolSelection, TurnConfig, UpdateId,
    Usage, UserInput, decode_entry, scan_all,
};
use asterpath::protocol::{ContextPolicy, LogId, SessionStore, SpanAttributes, StorageError};
use asterpath::protocol::{
    Parallelism, ReplaySafety, SideEffect, Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError,
    ToolErrorKind, ToolOutput, ToolResultStatus,
};
use asterpath::{AsterError, Asterpath, DefaultCompactor, OwnedSession};
use asterpath_testing::{Failpoint, FailpointEventLog, InMemoryEventLog, Point, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use std::num::{NonZeroU32, NonZeroUsize};

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

fn model(name: &str) -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new(name),
        context_window: None,
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        system_sections: Vec::new(),
        model: model("model-1"),
        system: vec![text("be helpful")],
        tools: ToolSelection::Names(Vec::new()),
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

fn switch_to(name: &str) -> ConfigPatch {
    let mut update = ConfigPatch::default();
    update.model = Some(model(name));
    update
}

fn input(id: u8) -> UserInput {
    UserInput {
        id: InputId::from_bytes([id; 16]),
        parts: vec![text("go")],
    }
}

/// Wraps a scripted provider; the n-th call first applies the n-th queued
/// update to `handle`, so the change lands inside a step that is running.
struct Switching {
    inner: ScriptedProvider,
    handle: OnceLock<OwnedSession>,
    updates: Mutex<VecDeque<Option<ConfigPatch>>>,
}

impl Switching {
    fn new(inner: ScriptedProvider, updates: Vec<Option<ConfigPatch>>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            handle: OnceLock::new(),
            updates: Mutex::new(updates.into()),
        })
    }
}

/// Requests of different sessions compare equal apart from their context.
fn sessionless(requests: Vec<ModelRequest>) -> Vec<ModelRequest> {
    requests
        .into_iter()
        .map(|mut request| {
            request.session = None;
            request
        })
        .collect()
}

impl Provider for Switching {
    fn id(&self) -> ProviderId {
        self.inner.id()
    }

    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities {
        self.inner.capabilities(spec)
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let inner = self.inner.stream(request);
        let update = self
            .updates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .flatten();
        let Some(update) = update else {
            return inner;
        };
        let handle = self.handle.get().expect("handle set").clone();
        stream::once(async move {
            handle.update_config(update).await.expect("update accepted");
        })
        .filter_map(|()| async { None })
        .chain(inner)
        .boxed()
    }
}

async fn events(asterpath: &Asterpath, handle: &OwnedSession) -> Vec<SessionEvent> {
    scan_all(&*asterpath.store(), handle.id())
        .await
        .expect("scan")
        .iter()
        .map(|entry| decode_entry(entry).expect("decode"))
        .collect()
}

async fn drive(handle: &OwnedSession) {
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
}

fn retryable() -> ProviderError {
    let mut error = ProviderError::new(ProviderErrorKind::Overloaded, "busy");
    error.retryable = true;
    error
}

#[tokio::test]
async fn an_unregistered_provider_is_rejected_and_nothing_is_logged() {
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider])
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    let before = events(&asterpath, &handle).await;

    let mut update = ConfigPatch::default();
    update.model = Some(ModelSpec {
        provider: ProviderId::new("nope"),
        ..model("model-2")
    });
    let error = handle.update_config(update).await.unwrap_err();

    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
    assert_eq!(events(&asterpath, &handle).await, before);
}

#[tokio::test]
async fn a_mid_turn_change_applies_from_the_next_step_only() {
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .fail(retryable())
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default()),
        vec![Some(switch_to("model-2"))],
    );
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider.clone()])
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");

    drive(&handle).await;

    let requested: Vec<ModelId> = provider
        .inner
        .requests()
        .into_iter()
        .map(|request| request.spec.model)
        .collect();
    assert_eq!(
        requested,
        [
            ModelId::new("model-1"),
            ModelId::new("model-1"),
            ModelId::new("model-2")
        ],
        "the retry of the running step keeps its model; the next step switches"
    );
    let recorded: Vec<(&'static str, ModelId)> = events(&asterpath, &handle)
        .await
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ModelCallAborted { model, .. } => Some(("ModelCallAborted", model.model)),
            SessionEvent::AssistantMessage { model, .. } => Some(("AssistantMessage", model.model)),
            _ => None,
        })
        .collect();
    assert_eq!(
        recorded,
        [
            ("ModelCallAborted", ModelId::new("model-1")),
            ("AssistantMessage", ModelId::new("model-1")),
            ("AssistantMessage", ModelId::new("model-2")),
        ]
    );
}

fn switch_model_and_budget() -> ConfigPatch {
    let mut update = switch_to("model-2");
    let mut request = RequestConfig::default();
    request.max_output_tokens = NonZeroU32::new(512).expect("non-zero");
    update.request = Some(request);
    update
}

/// Switches config during step 1, which pauses the turn, so step 2 runs
/// under the change.
fn pausing_switcher() -> Arc<Switching> {
    Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default()),
        vec![Some(switch_model_and_budget())],
    )
}

async fn assemble(store: Arc<dyn SessionStore>, provider: Arc<dyn Provider>) -> Asterpath {
    Asterpath::builder()
        .store(store)
        .providers(vec![provider])
        .build()
        .await
        .expect("build")
}

async fn kinds(
    store: &dyn SessionStore,
    handle: &OwnedSession,
) -> Result<Vec<&'static str>, StorageError> {
    Ok(scan_all(store, handle.id())
        .await?
        .iter()
        .map(|entry| decode_entry(entry).expect("decode").kind())
        .collect())
}

#[tokio::test]
async fn recovery_after_a_change_renders_the_request_a_live_run_renders() {
    let live = pausing_switcher();
    let asterpath = assemble(Arc::new(InMemoryEventLog::new()), live.clone()).await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    live.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;
    let live_step_2 = sessionless(live.inner.requests())[1].clone();
    assert_eq!(live_step_2.spec, model("model-2"));
    assert_eq!(live_step_2.max_output_tokens, 512);

    // The same run dies appending step 2's `StepStarted`, the 8th append:
    // SessionCreated, InputRecorded, TurnStarted, StepStarted, ConfigChanged,
    // AssistantMessage, StepCommitted.
    let shared = Arc::new(InMemoryEventLog::new());
    let crashing = pausing_switcher();
    let failing: Arc<dyn SessionStore> = Arc::new(FailpointEventLog::new(
        shared.clone(),
        Failpoint::panic(Point::AppendBeforeCommit)
            .with_occurrence(NonZeroUsize::new(8).expect("non-zero")),
    ));
    let asterpath = assemble(failing, crashing.clone()).await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    crashing.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    let driven = handle.clone();
    let crashed = tokio::spawn(async move { drive(&driven).await }).await;
    assert!(crashed.is_err_and(|error| error.is_panic()));
    let prefix = kinds(shared.as_ref(), &handle).await.expect("scan");
    assert_eq!(prefix.last(), Some(&"StepCommitted"));
    assert!(prefix.contains(&"ConfigChanged"));

    let recovered = Arc::new(ScriptedProvider::new("p").respond(
        reply("second"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = assemble(shared.clone(), recovered.clone()).await;
    let handle = asterpath
        .open_session(handle.id())
        .await
        .expect("open")
        .claim()
        .await
        .unwrap();
    drive(&handle).await;

    assert_eq!(sessionless(recovered.requests()), [live_step_2]);
}

#[tokio::test]
async fn a_child_inherits_the_config_of_its_spawn_step() {
    // Calls: parent step 1 (switches to model-2), parent step 2 spawns and
    // switches to model-3, the child, then parent step 3.
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(
                vec![Item::ToolCall {
                    call_id: "call-spawn".to_owned(),
                    provider_item_id: None,
                    name: ToolName::new("spawn_agent"),
                    raw_args: JsonString::new(r#"{"task":"look"}"#),
                    raw: None,
                }],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(reply("child done"), StopReason::EndTurn, Usage::default())
            .respond(reply("parent done"), StopReason::EndTurn, Usage::default()),
        vec![Some(switch_to("model-2")), Some(switch_to("model-3"))],
    );
    let asterpath = assemble(Arc::new(InMemoryEventLog::new()), provider.clone()).await;
    let handle = asterpath
        .create_session(SessionSpec {
            tools: ToolSelection::All,
            ..spec()
        })
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;

    let requested: Vec<ModelId> = provider
        .inner
        .requests()
        .into_iter()
        .map(|request| request.spec.model)
        .collect();
    assert_eq!(
        requested,
        ["model-1", "model-2", "model-2", "model-3"].map(ModelId::new),
        "the child runs on the spawn step's model, not the parent's later change"
    );
    let spawned = events(&asterpath, &handle)
        .await
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { config, .. } => Some(config.model),
            _ => None,
        })
        .expect("spawned");
    assert_eq!(spawned, model("model-2"));
}

fn append(id: u8, value: &str) -> ConfigPatch {
    let mut update = ConfigPatch::default();
    update.id = Some(UpdateId::from_bytes([id; 16]));
    update.append_system = vec![text(value)];
    update
}

fn instruction(value: &str) -> Item {
    Item::Instruction {
        key: None,
        parts: vec![text(value)],
    }
}

/// Appends an instruction during step 1, which pauses the turn, so step 2
/// runs under it.
fn pausing_appender() -> Arc<Switching> {
    Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default()),
        vec![Some(append(1, "answer in French"))],
    )
}

#[tokio::test]
async fn an_appended_instruction_reaches_the_next_step_in_place() {
    let provider = pausing_appender();
    let asterpath = assemble(Arc::new(InMemoryEventLog::new()), provider.clone()).await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;

    let requests = provider.inner.requests();
    assert!(
        !requests[0].items.contains(&instruction("answer in French")),
        "the running step is unaffected"
    );
    let step_2 = &requests[1];
    assert_eq!(
        step_2.system,
        vec![text("be helpful")],
        "top-level system unchanged"
    );
    assert_eq!(step_2.items.last(), Some(&instruction("answer in French")));
    assert_eq!(step_2.prefix_epoch, 0);
}

fn refresh(id: u8, key: &str, value: &str) -> ConfigPatch {
    let mut update = ConfigPatch::default();
    update.id = Some(UpdateId::from_bytes([id; 16]));
    update.system_sections = vec![SystemSection::new(key, vec![text(value)])];
    update
}

#[tokio::test]
async fn a_keyed_refresh_reaches_the_next_step_and_keeps_the_system_prefix() {
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default()),
        vec![Some(refresh(1, "date", "Tuesday"))],
    );
    let asterpath = assemble(Arc::new(InMemoryEventLog::new()), provider.clone()).await;
    let mut spec = spec();
    spec.system_sections = vec![SystemSection::new("date", vec![text("Monday")])];
    let handle = asterpath
        .create_session(spec)
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;

    let requests = provider.inner.requests();
    let system = vec![text("be helpful"), text("Monday")];
    assert_eq!(requests[0].system, system);
    assert_eq!(
        requests[1].system, system,
        "the refresh does not rewrite it"
    );
    assert!(mentions(&requests[1].items, "Tuesday"));
}

#[tokio::test]
async fn invalid_section_keys_are_rejected_and_nothing_is_logged() {
    let asterpath = assemble(
        Arc::new(InMemoryEventLog::new()),
        Arc::new(ScriptedProvider::new("p")),
    )
    .await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    let before = events(&asterpath, &handle).await;

    let empty_key = refresh(1, "", "rule");
    let mut repeated = refresh(2, "date", "a");
    repeated
        .system_sections
        .push(SystemSection::new("date", vec![text("b")]));
    for update in [empty_key, repeated] {
        let error = handle.update_config(update).await.unwrap_err();
        assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
    }
    assert_eq!(events(&asterpath, &handle).await, before);
}

#[tokio::test]
async fn invalid_appends_are_rejected_and_nothing_is_logged() {
    let asterpath = assemble(
        Arc::new(InMemoryEventLog::new()),
        Arc::new(ScriptedProvider::new("p")),
    )
    .await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    let before = events(&asterpath, &handle).await;

    let mut without_id = append(1, "rule");
    without_id.id = None;
    let mut image = append(2, "rule");
    image.append_system.push(Part::Image {
        media_type: "image/png".to_owned(),
        source: ImageSource::Url {
            url: "https://example.com/a.png".to_owned(),
        },
    });
    for update in [without_id, image] {
        let error = handle.update_config(update).await.unwrap_err();
        assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
    }
    assert_eq!(events(&asterpath, &handle).await, before);
}

#[tokio::test]
async fn a_retried_append_is_recorded_once() {
    let asterpath = assemble(
        Arc::new(InMemoryEventLog::new()),
        Arc::new(ScriptedProvider::new("p")),
    )
    .await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    handle
        .update_config(append(1, "rule"))
        .await
        .expect("first");
    handle
        .update_config(append(1, "rule"))
        .await
        .expect("retry");
    handle
        .update_config(append(2, "rule"))
        .await
        .expect("another");
    let changes = events(&asterpath, &handle)
        .await
        .into_iter()
        .filter(|event| event.kind() == "ConfigChanged")
        .count();
    assert_eq!(changes, 2);
}

#[tokio::test]
async fn recovery_after_an_append_renders_the_request_a_live_run_renders() {
    let live = pausing_appender();
    let asterpath = assemble(Arc::new(InMemoryEventLog::new()), live.clone()).await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    live.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;
    let live_step_2 = sessionless(live.inner.requests())[1].clone();

    // Dies appending step 2's `StepStarted` (see the model-switch case).
    let shared = Arc::new(InMemoryEventLog::new());
    let crashing = pausing_appender();
    let failing: Arc<dyn SessionStore> = Arc::new(FailpointEventLog::new(
        shared.clone(),
        Failpoint::panic(Point::AppendBeforeCommit)
            .with_occurrence(NonZeroUsize::new(8).expect("non-zero")),
    ));
    let asterpath = assemble(failing, crashing.clone()).await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    crashing.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    let driven = handle.clone();
    let crashed = tokio::spawn(async move { drive(&driven).await }).await;
    assert!(crashed.is_err_and(|error| error.is_panic()));
    let prefix = kinds(shared.as_ref(), &handle).await.expect("scan");
    assert_eq!(prefix.last(), Some(&"StepCommitted"));

    let recovered = Arc::new(ScriptedProvider::new("p").respond(
        reply("second"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = assemble(shared.clone(), recovered.clone()).await;
    let handle = asterpath
        .open_session(handle.id())
        .await
        .expect("open")
        .claim()
        .await
        .unwrap();
    drive(&handle).await;

    assert_eq!(sessionless(recovered.requests()), [live_step_2]);
}

#[tokio::test]
async fn a_forked_child_sees_an_appended_instruction_once() {
    // Calls: parent step 1 (appends), parent step 2 spawns, the child, parent step 3.
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(
                vec![Item::ToolCall {
                    call_id: "call-spawn".to_owned(),
                    provider_item_id: None,
                    name: ToolName::new("spawn_agent"),
                    raw_args: JsonString::new(r#"{"task":"look"}"#),
                    raw: None,
                }],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(reply("child done"), StopReason::EndTurn, Usage::default())
            .respond(reply("parent done"), StopReason::EndTurn, Usage::default()),
        vec![Some(append(1, "answer in French"))],
    );
    let asterpath = assemble(Arc::new(InMemoryEventLog::new()), provider.clone()).await;
    let handle = asterpath
        .create_session(SessionSpec {
            tools: ToolSelection::All,
            subagents: {
                let mut config = SubagentConfig::default();
                config.default_context = ContextPolicy::Fork {
                    at: LogId::from_i64(0),
                };
                config
            },
            ..spec()
        })
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;

    let child = &provider.inner.requests()[2];
    assert_eq!(child.system, vec![text("be helpful")]);
    let copies = child
        .items
        .iter()
        .filter(|item| **item == instruction("answer in French"))
        .count();
    assert_eq!(copies, 1, "{:?}", child.items);
}

fn mentions(items: &[Item], needle: &str) -> bool {
    format!("{items:?}").contains(needle)
}

#[tokio::test]
async fn compaction_rebases_replaced_instructions_into_the_system() {
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default())
            .respond(reply("third"), StopReason::EndTurn, Usage::default())
            .respond(reply("the summary"), StopReason::EndTurn, Usage::default())
            .respond(reply("fourth"), StopReason::EndTurn, Usage::default()),
        vec![Some(append(1, "answer in French"))],
    );
    let catalog = ProviderCatalog::new(vec![provider.clone() as Arc<dyn Provider>]).expect("one");
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider.clone()])
        .compactor(Arc::new(DefaultCompactor::new(Arc::new(catalog))))
        .build()
        .await
        .expect("build");
    let mut session = spec();
    session.model.context_window = Some(100_000);
    let handle = asterpath
        .create_session(session)
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;
    handle.submit_input(input(2)).await.expect("input");
    drive(&handle).await;
    handle.compact_now().await.expect("compact");
    handle.submit_input(input(3)).await.expect("input");
    drive(&handle).await;

    let requests = provider.inner.requests();
    let summary = &requests[3];
    assert!(
        !mentions(&summary.items, "answer in French"),
        "the instruction is not summarized: {:?}",
        summary.items
    );
    assert!(mentions(&summary.items, "first"), "{:?}", summary.items);
    let after = &requests[4];
    assert_eq!(after.prefix_epoch, 1);
    assert_eq!(
        after.system,
        vec![text("be helpful"), text("answer in French")]
    );
    assert!(!after.items.contains(&instruction("answer in French")));
}

#[tokio::test]
async fn trace_attributes_each_model_call_to_the_model_it_used() {
    // Step 2 never gets an `AssistantMessage`: its span can only learn the
    // model from the fold or the abort record.
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .fail(ProviderError::new(ProviderErrorKind::InvalidRequest, "no")),
        vec![Some(switch_to("model-2"))],
    );
    let asterpath = assemble(Arc::new(InMemoryEventLog::new()), provider.clone()).await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;

    let calls: Vec<(String, ModelId)> = asterpath
        .trace(handle.id())
        .await
        .expect("trace")
        .into_iter()
        .filter_map(|span| match span.attributes {
            SpanAttributes::ModelCall(attrs) => Some((span.name, attrs.request_model.model)),
            _ => None,
        })
        .collect();
    assert_eq!(
        calls,
        [
            ("chat model-1".to_owned(), ModelId::new("model-1")),
            ("chat model-2".to_owned(), ModelId::new("model-2")),
        ]
    );
}

fn lookup() -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new("lookup"),
        "look something up",
        serde_json::json!({"type": "object"}),
    )
    .with_parallelism(Parallelism::Parallel)
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly)
}

struct Lookup {
    descriptor: ToolDescriptor,
    calls: AtomicUsize,
}

impl Tool for Lookup {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(ToolOutput::new(vec![text("found")])) })
    }
}

fn lookup_tool() -> Arc<Lookup> {
    Arc::new(Lookup {
        descriptor: lookup(),
        calls: AtomicUsize::new(0),
    })
}

async fn assemble_with_lookup(
    store: Arc<dyn SessionStore>,
    provider: Arc<dyn Provider>,
    tool: Arc<Lookup>,
) -> Asterpath {
    Asterpath::builder()
        .store(store)
        .providers(vec![provider])
        .tools(vec![tool])
        .build()
        .await
        .expect("build")
}

fn add_lookup(id: u8) -> ConfigPatch {
    let mut update = ConfigPatch::default();
    update.id = Some(UpdateId::from_bytes([id; 16]));
    update.add_tools = vec![lookup()];
    update
}

fn remove_lookup(id: u8) -> ConfigPatch {
    let mut update = ConfigPatch::default();
    update.id = Some(UpdateId::from_bytes([id; 16]));
    update.remove_tools = vec![ToolName::new("lookup")];
    update
}

fn call_lookup(call_id: &str) -> Vec<Item> {
    vec![Item::ToolCall {
        call_id: call_id.to_owned(),
        provider_item_id: None,
        name: ToolName::new("lookup"),
        raw_args: JsonString::new("{}"),
        raw: None,
    }]
}

fn names(tools: &[ToolDescriptor]) -> Vec<&str> {
    tools.iter().map(|tool| tool.name.as_str()).collect()
}

fn has_tools_changed(items: &[Item]) -> bool {
    items
        .iter()
        .any(|item| matches!(item, Item::ToolsChanged { .. }))
}

#[tokio::test]
async fn an_added_tool_is_callable_next_step_and_a_removed_one_is_not_found() {
    // Step 1 adds `lookup`, step 2 calls it, step 3 removes it, step 4 calls it again.
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(call_lookup("call-1"), StopReason::ToolUse, Usage::default())
            .respond(reply("third"), StopReason::PauseTurn, Usage::default())
            .respond(call_lookup("call-2"), StopReason::ToolUse, Usage::default())
            .respond(reply("done"), StopReason::EndTurn, Usage::default()),
        vec![Some(add_lookup(1)), None, Some(remove_lookup(2))],
    );
    let tool = lookup_tool();
    let asterpath = assemble_with_lookup(
        Arc::new(InMemoryEventLog::new()),
        provider.clone(),
        tool.clone(),
    )
    .await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;

    let requests = provider.inner.requests();
    assert!(
        requests[0].tools.is_empty(),
        "the running step is unaffected"
    );
    assert_eq!(names(&requests[1].tools), ["lookup"]);
    assert!(
        requests[1].baseline_tools.is_empty(),
        "the baseline is unchanged"
    );
    assert!(
        has_tools_changed(&requests[1].items),
        "{:?}",
        requests[1].items
    );
    assert!(requests[3].tools.is_empty());
    assert_eq!(
        tool.calls.load(Ordering::SeqCst),
        1,
        "only the first call runs"
    );

    let results: Vec<(ToolResultStatus, Option<ToolErrorKind>)> = events(&asterpath, &handle)
        .await
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult { status, error, .. } => {
                Some((status, error.map(|error| error.kind)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        results,
        [
            (ToolResultStatus::Completed, None),
            (ToolResultStatus::Failed, Some(ToolErrorKind::NotFound)),
        ]
    );
    let calls = requests[4]
        .items
        .iter()
        .filter(|item| matches!(item, Item::ToolCall { .. }))
        .count();
    assert_eq!(calls, 2, "calls to the removed tool stay in history");
}

#[tokio::test]
async fn invalid_tool_changes_are_rejected_and_nothing_is_logged() {
    let asterpath = assemble_with_lookup(
        Arc::new(InMemoryEventLog::new()),
        Arc::new(ScriptedProvider::new("p")),
        lookup_tool(),
    )
    .await;
    let handle = asterpath
        .create_session(SessionSpec {
            tools: ToolSelection::Names(vec![ToolName::new("lookup")]),
            ..spec()
        })
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    let before = events(&asterpath, &handle).await;

    let mut unregistered = add_lookup(1);
    unregistered.add_tools[0].name = ToolName::new("missing");
    let duplicate = add_lookup(2);
    let mut absent = remove_lookup(3);
    absent.remove_tools = vec![ToolName::new("missing")];
    let mut without_id = remove_lookup(4);
    without_id.id = None;
    for update in [unregistered, duplicate, absent, without_id] {
        let error = handle.update_config(update.clone()).await.unwrap_err();
        assert!(
            matches!(error, AsterError::Config { .. }),
            "{update:?}: {error:?}"
        );
    }
    assert_eq!(events(&asterpath, &handle).await, before);
}

#[tokio::test]
async fn an_added_descriptor_must_match_the_registry() {
    let asterpath = assemble_with_lookup(
        Arc::new(InMemoryEventLog::new()),
        Arc::new(ScriptedProvider::new("p")),
        lookup_tool(),
    )
    .await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    let mut changed = add_lookup(1);
    changed.add_tools[0].description = "something else".to_owned();
    let error = handle.update_config(changed).await.unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
}

#[tokio::test]
async fn a_retried_tool_change_is_recorded_once() {
    let asterpath = assemble_with_lookup(
        Arc::new(InMemoryEventLog::new()),
        Arc::new(ScriptedProvider::new("p")),
        lookup_tool(),
    )
    .await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    handle.update_config(add_lookup(1)).await.expect("first");
    handle.update_config(add_lookup(1)).await.expect("retry");
    let changes = events(&asterpath, &handle)
        .await
        .into_iter()
        .filter(|event| event.kind() == "ConfigChanged")
        .count();
    assert_eq!(changes, 1);
}

/// Adds `lookup` during step 1, which pauses the turn, so step 2 runs with it.
fn pausing_adder() -> Arc<Switching> {
    Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default()),
        vec![Some(add_lookup(1))],
    )
}

#[tokio::test]
async fn recovery_after_a_tool_change_renders_the_request_a_live_run_renders() {
    let live = pausing_adder();
    let asterpath = assemble_with_lookup(
        Arc::new(InMemoryEventLog::new()),
        live.clone(),
        lookup_tool(),
    )
    .await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    live.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;
    let live_step_2 = sessionless(live.inner.requests())[1].clone();
    assert_eq!(names(&live_step_2.tools), ["lookup"]);

    // Dies appending step 2's `StepStarted` (see the model-switch case).
    let shared = Arc::new(InMemoryEventLog::new());
    let crashing = pausing_adder();
    let failing: Arc<dyn SessionStore> = Arc::new(FailpointEventLog::new(
        shared.clone(),
        Failpoint::panic(Point::AppendBeforeCommit)
            .with_occurrence(NonZeroUsize::new(8).expect("non-zero")),
    ));
    let asterpath = assemble_with_lookup(failing, crashing.clone(), lookup_tool()).await;
    let handle = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    crashing.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    let driven = handle.clone();
    let crashed = tokio::spawn(async move { drive(&driven).await }).await;
    assert!(crashed.is_err_and(|error| error.is_panic()));
    let prefix = kinds(shared.as_ref(), &handle).await.expect("scan");
    assert_eq!(prefix.last(), Some(&"StepCommitted"));

    let recovered = Arc::new(ScriptedProvider::new("p").respond(
        reply("second"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = assemble_with_lookup(shared.clone(), recovered.clone(), lookup_tool()).await;
    let handle = asterpath
        .open_session(handle.id())
        .await
        .expect("open")
        .claim()
        .await
        .unwrap();
    drive(&handle).await;

    assert_eq!(sessionless(recovered.requests()), [live_step_2]);
}

#[tokio::test]
async fn compaction_rebases_replaced_tool_changes_into_the_baseline_tools() {
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default())
            .respond(reply("third"), StopReason::EndTurn, Usage::default())
            .respond(reply("the summary"), StopReason::EndTurn, Usage::default())
            .respond(reply("fourth"), StopReason::EndTurn, Usage::default()),
        vec![Some(add_lookup(1))],
    );
    let catalog = ProviderCatalog::new(vec![provider.clone() as Arc<dyn Provider>]).expect("one");
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider.clone()])
        .tools(vec![lookup_tool()])
        .compactor(Arc::new(DefaultCompactor::new(Arc::new(catalog))))
        .build()
        .await
        .expect("build");
    let mut session = spec();
    session.model.context_window = Some(100_000);
    let handle = asterpath
        .create_session(session)
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;
    handle.submit_input(input(2)).await.expect("input");
    drive(&handle).await;
    handle.compact_now().await.expect("compact");
    handle.submit_input(input(3)).await.expect("input");
    drive(&handle).await;

    let after = &provider.inner.requests()[4];
    assert_eq!(after.prefix_epoch, 1);
    assert_eq!(names(&after.baseline_tools), ["lookup"]);
    assert_eq!(after.baseline_tools, after.tools);
    assert!(!has_tools_changed(&after.items), "{:?}", after.items);
}

#[tokio::test]
async fn a_forked_child_starts_from_the_effective_tools_without_change_items() {
    // Calls: parent step 1 (adds lookup), parent step 2 spawns, the child, parent step 3.
    let provider = Switching::new(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(
                vec![Item::ToolCall {
                    call_id: "call-spawn".to_owned(),
                    provider_item_id: None,
                    name: ToolName::new("spawn_agent"),
                    raw_args: JsonString::new(r#"{"task":"look"}"#),
                    raw: None,
                }],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(reply("child done"), StopReason::EndTurn, Usage::default())
            .respond(reply("parent done"), StopReason::EndTurn, Usage::default()),
        vec![Some(add_lookup(1))],
    );
    let asterpath = assemble_with_lookup(
        Arc::new(InMemoryEventLog::new()),
        provider.clone(),
        lookup_tool(),
    )
    .await;
    let handle = asterpath
        .create_session(SessionSpec {
            tools: ToolSelection::Names(vec![ToolName::new("spawn_agent")]),
            subagents: {
                let mut config = SubagentConfig::default();
                config.default_context = ContextPolicy::Fork {
                    at: LogId::from_i64(0),
                };
                config
            },
            ..spec()
        })
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;

    let child = &provider.inner.requests()[2];
    assert_eq!(names(&child.tools), ["spawn_agent", "lookup"]);
    assert_eq!(child.baseline_tools, child.tools);
    assert!(!has_tools_changed(&child.items), "{:?}", child.items);
}

/// Removes `lookup` during step 1, whose reply calls it: the call belongs to
/// a step that started with `lookup`.
fn removing_caller() -> Arc<Switching> {
    Switching::new(
        ScriptedProvider::new("p")
            .respond(call_lookup("call-1"), StopReason::ToolUse, Usage::default())
            .respond(reply("done"), StopReason::EndTurn, Usage::default()),
        vec![Some(remove_lookup(1))],
    )
}

fn with_lookup() -> SessionSpec {
    SessionSpec {
        tools: ToolSelection::Names(vec![ToolName::new("lookup")]),
        ..spec()
    }
}

fn tool_results(events: Vec<SessionEvent>) -> Vec<ToolResultStatus> {
    events
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult { status, .. } => Some(status),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn trace_describes_a_call_by_the_tools_of_its_step() {
    let tool = Arc::new(Lookup {
        descriptor: lookup().with_side_effect(SideEffect::Mutating),
        calls: AtomicUsize::new(0),
    });
    let provider = removing_caller();
    let asterpath =
        assemble_with_lookup(Arc::new(InMemoryEventLog::new()), provider.clone(), tool).await;
    let handle = asterpath
        .create_session(with_lookup())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    provider.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;

    let effects: Vec<SideEffect> = asterpath
        .trace(handle.id())
        .await
        .expect("trace")
        .into_iter()
        .filter_map(|span| match span.attributes {
            SpanAttributes::ToolCall(attrs) => Some(attrs.side_effect),
            _ => None,
        })
        .collect();
    assert_eq!(effects, [SideEffect::Mutating]);
}

#[tokio::test]
async fn a_resumed_step_runs_its_calls_under_the_config_it_started_with() {
    let live = removing_caller();
    let asterpath = assemble_with_lookup(
        Arc::new(InMemoryEventLog::new()),
        live.clone(),
        lookup_tool(),
    )
    .await;
    let handle = asterpath
        .create_session(with_lookup())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    live.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;
    assert_eq!(
        tool_results(events(&asterpath, &handle).await),
        [ToolResultStatus::Completed]
    );

    // Dies appending `ToolCallRequested`, the 7th append: SessionCreated,
    // InputRecorded, TurnStarted, StepStarted, ConfigChanged, AssistantMessage.
    let shared = Arc::new(InMemoryEventLog::new());
    let crashing = removing_caller();
    let failing: Arc<dyn SessionStore> = Arc::new(FailpointEventLog::new(
        shared.clone(),
        Failpoint::panic(Point::AppendBeforeCommit)
            .with_occurrence(NonZeroUsize::new(7).expect("non-zero")),
    ));
    let asterpath = assemble_with_lookup(failing, crashing.clone(), lookup_tool()).await;
    let handle = asterpath
        .create_session(with_lookup())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    crashing.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    let driven = handle.clone();
    let crashed = tokio::spawn(async move { drive(&driven).await }).await;
    assert!(crashed.is_err_and(|error| error.is_panic()));
    let prefix = kinds(shared.as_ref(), &handle).await.expect("scan");
    assert_eq!(prefix.last(), Some(&"AssistantMessage"));

    let recovered = Arc::new(ScriptedProvider::new("p").respond(
        reply("done"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let tool = lookup_tool();
    let asterpath = assemble_with_lookup(shared.clone(), recovered, tool.clone()).await;
    let handle = asterpath
        .open_session(handle.id())
        .await
        .expect("open")
        .claim()
        .await
        .unwrap();
    drive(&handle).await;

    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        tool_results(events(&asterpath, &handle).await),
        [ToolResultStatus::Completed]
    );
}
