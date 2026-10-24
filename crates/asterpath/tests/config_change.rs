//! Mid-session configuration changes (ADR-0030, #109): `update_config` writes
//! a `ConfigChanged`, and every step folds the config at its `StepStarted`.

use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use asterpath::protocol::{
    CompactionConfig, ConfigUpdate, InputId, Item, ModelId, ModelRequest, ModelSpec, Part,
    Provider, ProviderCapabilities, ProviderError, ProviderErrorKind, ProviderId, RequestConfig,
    Role, SessionEvent, SessionSpec, StopReason, StreamEvent, SubagentConfig, ToolSelection,
    TurnConfig, Usage, UserInput, decode_entry, scan_all,
};
use asterpath::protocol::{SessionStore, StorageError};
use asterpath::{AsterError, Asterpath, SessionHandle};
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

/// Wraps a scripted provider; the first attempt calls `update_config` before
/// streaming, so the change lands inside a step that is already running.
struct SwitchOnFirstCall {
    inner: ScriptedProvider,
    handle: OnceLock<SessionHandle>,
    update: Mutex<Option<ConfigUpdate>>,
}

impl Provider for SwitchOnFirstCall {
    fn id(&self) -> ProviderId {
        self.inner.id()
    }

    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities {
        self.inner.capabilities(spec)
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let inner = self.inner.stream(request);
        let update = self
            .update
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
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
    let provider = Arc::new(SwitchOnFirstCall {
        inner: ScriptedProvider::new("p")
            .fail(retryable())
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default()),
        handle: OnceLock::new(),
        update: Mutex::new(Some(switch_to("model-2"))),
    });
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
fn pausing_switcher() -> Arc<SwitchOnFirstCall> {
    Arc::new(SwitchOnFirstCall {
        inner: ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::PauseTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default()),
        handle: OnceLock::new(),
        update: Mutex::new(Some(switch_model_and_budget())),
    })
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
