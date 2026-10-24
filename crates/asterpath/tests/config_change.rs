//! Mid-session configuration changes (ADR-0030, #109): `update_config` writes
//! a `ConfigChanged`, and every step folds the config at its `StepStarted`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use asterpath::protocol::{
    CompactionConfig, ConfigUpdate, ImageSource, InputId, Item, JsonString, ModelId, ModelRequest,
    ModelSpec, Part, Provider, ProviderCapabilities, ProviderCatalog, ProviderError,
    ProviderErrorKind, ProviderId, RequestConfig, Role, SessionEvent, SessionSpec, StopReason,
    StreamEvent, SubagentConfig, ToolName, ToolSelection, TurnConfig, UpdateId, Usage, UserInput,
    decode_entry, scan_all,
};
use asterpath::protocol::{ContextPolicy, LogId, SessionStore, SpanAttributes, StorageError};
use asterpath::{AsterError, Asterpath, DefaultCompactor, SessionHandle};
use asterpath_testing::{Failpoint, FailpointEventLog, InMemoryEventLog, Point, ScriptedProvider};
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
        model: model("model-1"),
        system: vec![text("be helpful")],
        tools: ToolSelection::Names(Vec::new()),
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

fn switch_to(name: &str) -> ConfigUpdate {
    let mut update = ConfigUpdate::default();
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
    handle: OnceLock<SessionHandle>,
    updates: Mutex<VecDeque<Option<ConfigUpdate>>>,
}

impl Switching {
    fn new(inner: ScriptedProvider, updates: Vec<Option<ConfigUpdate>>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            handle: OnceLock::new(),
            updates: Mutex::new(updates.into()),
        })
    }
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

async fn events(asterpath: &Asterpath, handle: &SessionHandle) -> Vec<SessionEvent> {
    scan_all(&*asterpath.store(), handle.id())
        .await
        .expect("scan")
        .iter()
        .map(|entry| decode_entry(entry).expect("decode"))
        .collect()
}

async fn drive(handle: &SessionHandle) {
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
    let handle = asterpath.create_session(spec()).await.expect("create");
    let before = events(&asterpath, &handle).await;

    let mut update = ConfigUpdate::default();
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
    let handle = asterpath.create_session(spec()).await.expect("create");
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

fn switch_model_and_budget() -> ConfigUpdate {
    let mut update = switch_to("model-2");
    update.request = Some(RequestConfig {
        max_output_tokens: NonZeroU32::new(512).expect("non-zero"),
        ..RequestConfig::default()
    });
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
    handle: &SessionHandle,
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
    let handle = asterpath.create_session(spec()).await.expect("create");
    live.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;
    let live_step_2 = live.inner.requests()[1].clone();
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
    let handle = asterpath.create_session(spec()).await.expect("create");
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
    let handle = asterpath.open_session(handle.id()).await.expect("open");
    drive(&handle).await;

    assert_eq!(recovered.requests(), [live_step_2]);
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
        .expect("create");
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

fn append(id: u8, value: &str) -> ConfigUpdate {
    let mut update = ConfigUpdate::default();
    update.id = Some(UpdateId::from_bytes([id; 16]));
    update.append_system = vec![text(value)];
    update
}

fn instruction(value: &str) -> Item {
    Item::Instruction {
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
    let handle = asterpath.create_session(spec()).await.expect("create");
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

#[tokio::test]
async fn invalid_appends_are_rejected_and_nothing_is_logged() {
    let asterpath = assemble(
        Arc::new(InMemoryEventLog::new()),
        Arc::new(ScriptedProvider::new("p")),
    )
    .await;
    let handle = asterpath.create_session(spec()).await.expect("create");
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
    let handle = asterpath.create_session(spec()).await.expect("create");
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
    let handle = asterpath.create_session(spec()).await.expect("create");
    live.handle.set(handle.clone()).expect("set once");
    handle.submit_input(input(1)).await.expect("input");
    drive(&handle).await;
    let live_step_2 = live.inner.requests()[1].clone();

    // Dies appending step 2's `StepStarted` (see the model-switch case).
    let shared = Arc::new(InMemoryEventLog::new());
    let crashing = pausing_appender();
    let failing: Arc<dyn SessionStore> = Arc::new(FailpointEventLog::new(
        shared.clone(),
        Failpoint::panic(Point::AppendBeforeCommit)
            .with_occurrence(NonZeroUsize::new(8).expect("non-zero")),
    ));
    let asterpath = assemble(failing, crashing.clone()).await;
    let handle = asterpath.create_session(spec()).await.expect("create");
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
    let handle = asterpath.open_session(handle.id()).await.expect("open");
    drive(&handle).await;

    assert_eq!(recovered.requests(), [live_step_2]);
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
            subagents: SubagentConfig {
                default_context: ContextPolicy::Fork {
                    at: LogId::from_i64(0),
                },
                ..SubagentConfig::default()
            },
            ..spec()
        })
        .await
        .expect("create");
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
    let handle = asterpath.create_session(session).await.expect("create");
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
    let handle = asterpath.create_session(spec()).await.expect("create");
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
