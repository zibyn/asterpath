//! New sessions, the background driver and the conversation view (#151).

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use asterpath::Asterpath;
use asterpath_app::desktop::{Access, ApprovalMode, ApprovalModes, Desktop, Status, ViewEvent};
use asterpath_app::workspace::{WorkRoot, Workspace};
use asterpath_protocol::{
    AgentEvent, ApprovalPolicy, ApprovalRequest, ApprovalVerdict, InputId, InputTokens, Item,
    JsonString, LogId, ModelId, ModelRequest, ModelSpec, OutputTokens, Part, PartDelta, Provider,
    ProviderCapabilities, ProviderError, ProviderId, Role, SessionEvent, SessionId, StepId,
    StopReason, StreamEvent, ToolName, TurnEndReason, TurnId, Usage, UserInput,
};
use asterpath_protocol::{
    CatalogEntry, Claim, LogEntry, LogQuery, NewEntry, OwnerToken, RetirementError, SessionQuery,
    SessionStore, StorageError,
};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use common::template;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::{Stream, StreamExt};
use tokio::sync::Semaphore;

fn workspace() -> Workspace {
    Workspace {
        tools: Some(vec!["read_file".to_owned()]),
        skills_enabled: false,
        ..Workspace::default()
    }
}

/// An engine assembled like `Desktop::open`: every session continued.
async fn engine(db: &Path, provider: Arc<dyn Provider>) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(vec![provider])
        .environment(Arc::new(WorkRoot {
            workspace: workspace(),
            new_session: false,
            pinned: None,
        }))
        .build()
        .await
        .expect("build")
}

struct Fixture {
    _dir: tempfile::TempDir,
    db: PathBuf,
    root: PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    Fixture {
        db: root.join("asterpath.db"),
        root,
        _dir: dir,
    }
}

async fn desktop(fixture: &Fixture, provider: Arc<dyn Provider>) -> Desktop {
    Desktop::new(
        engine(&fixture.db, provider).await,
        Arc::new(ApprovalModes::new(ApprovalMode::default())),
        workspace(),
        template(),
        fixture.root.join("projects.json"),
    )
}

fn reply(text: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Text {
            text: text.to_owned(),
        }],
        raw: None,
    }]
}

/// Read view events until `done` holds for one; that one is the last.
async fn until(
    view: &mut (impl Stream<Item = Result<ViewEvent, String>> + Unpin),
    done: impl Fn(&ViewEvent) -> bool,
) -> Vec<ViewEvent> {
    let mut seen = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = view.next().await {
            let event = event.expect("watch");
            let last = done(&event);
            seen.push(event);
            if last {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out; saw {seen:#?}"));
    seen
}

fn turn_committed(event: &ViewEvent) -> Option<&TurnEndReason> {
    match event {
        ViewEvent::Record {
            event: SessionEvent::TurnCommitted { reason, .. },
            ..
        } => Some(reason),
        _ => None,
    }
}

#[tokio::test]
async fn a_new_session_gets_the_new_session_tools_and_answers_in_the_background() {
    let fixture = fixture();
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        reply("hello"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let desktop = desktop(&fixture, provider.clone()).await;

    let created = desktop
        .create_session(&fixture.root, Some("fix"), None)
        .await
        .unwrap();
    let listed = desktop.sessions(&fixture.root).await.unwrap();
    assert_eq!(listed, std::slice::from_ref(&created));
    assert_eq!(created.name.as_deref(), Some("fix"));

    let id = created.id;
    let mut view = desktop.watch(id, None).await.unwrap();
    desktop.send(id, "hi".to_owned()).await.unwrap();
    let seen = until(&mut view, |event| turn_committed(event).is_some()).await;
    assert_eq!(
        turn_committed(seen.last().unwrap()),
        Some(&TurnEndReason::Completed)
    );
    assert!(
        seen.iter().any(|event| matches!(
            event,
            ViewEvent::Record {
                event: SessionEvent::AssistantMessage { .. },
                ..
            }
        )),
        "{seen:#?}"
    );

    // `[tools] enabled` and no skill loader: a new session, not a continued one.
    let tools: Vec<String> = provider.requests()[0]
        .tools
        .iter()
        .map(|tool| tool.name.as_str().to_owned())
        .collect();
    assert_eq!(tools, ["read_file"]);
}

/// Each model call waits for a permit, so a turn stays open until released.
struct Gated {
    inner: ScriptedProvider,
    permits: Arc<Semaphore>,
}

impl Provider for Gated {
    fn id(&self) -> ProviderId {
        self.inner.id()
    }

    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities {
        self.inner.capabilities(spec)
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let permits = Arc::clone(&self.permits);
        stream::once(async move { permits.acquire_owned().await.unwrap().forget() })
            .flat_map(move |()| self.inner.stream(request.clone()))
            .boxed()
    }
}

fn gated(inner: ScriptedProvider) -> (Arc<Gated>, Arc<Semaphore>) {
    let permits = Arc::new(Semaphore::new(0));
    let provider = Arc::new(Gated {
        inner,
        permits: Arc::clone(&permits),
    });
    (provider, permits)
}

fn user_texts(request: &ModelRequest) -> Vec<String> {
    request
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Message {
                role: Role::User,
                parts,
                ..
            } => Some(parts),
            _ => None,
        })
        .flatten()
        .filter_map(|part| match part {
            Part::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

fn turn_started(event: &ViewEvent) -> bool {
    matches!(
        event,
        ViewEvent::Live {
            event: AgentEvent::TurnStarted { .. }
        }
    )
}

#[tokio::test]
async fn input_sent_during_a_turn_steers_it_and_streams_live() {
    let fixture = fixture();
    let (provider, permits) = gated(
        ScriptedProvider::new("p")
            .respond(reply("first"), StopReason::EndTurn, Usage::default())
            .respond(reply("second"), StopReason::EndTurn, Usage::default()),
    );
    let desktop = desktop(&fixture, provider.clone()).await;
    let id = desktop
        .create_session(&fixture.root, None, None)
        .await
        .unwrap()
        .id;
    let mut view = desktop.watch(id, None).await.unwrap();

    desktop.send(id, "go".to_owned()).await.unwrap();
    until(&mut view, turn_started).await;
    assert_eq!(desktop.access(id).await.unwrap(), Access::Here);
    desktop.send(id, "steer".to_owned()).await.unwrap();
    permits.add_permits(2);

    let seen = until(&mut view, |event| {
        matches!(
            event,
            ViewEvent::Record {
                event: SessionEvent::AssistantMessage { .. },
                ..
            }
        ) && provider.inner.requests().len() == 2
    })
    .await;
    assert!(
        seen.iter().any(|event| matches!(
            event,
            ViewEvent::Live {
                event: AgentEvent::ModelDelta { .. }
            }
        )),
        "deltas stream to the view: {seen:#?}"
    );
    let requests = provider.inner.requests();
    assert_eq!(user_texts(&requests[0]), ["go"]);
    assert_eq!(user_texts(&requests[1]), ["go", "steer"]);

    // Idle again: the driver stops and releases the claim.
    tokio::time::timeout(Duration::from_secs(10), async {
        while desktop.access(id).await.unwrap() != Access::Free {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("released");
}

#[tokio::test]
async fn stop_cancels_the_running_turn() {
    let fixture = fixture();
    let (provider, _permits) = gated(ScriptedProvider::new("p"));
    let desktop = desktop(&fixture, provider).await;
    let id = desktop
        .create_session(&fixture.root, None, None)
        .await
        .unwrap()
        .id;
    let mut view = desktop.watch(id, None).await.unwrap();

    desktop.send(id, "go".to_owned()).await.unwrap();
    until(&mut view, turn_started).await;
    desktop.cancel(id).await.unwrap();
    let seen = until(&mut view, |event| turn_committed(event).is_some()).await;
    assert_eq!(
        turn_committed(seen.last().unwrap()),
        Some(&TurnEndReason::Cancelled)
    );
}

#[tokio::test]
async fn a_session_another_process_drives_is_read_only_but_watched() {
    let fixture = fixture();
    let cli = engine(
        &fixture.db,
        Arc::new(ScriptedProvider::new("p").respond(
            reply("from the cli"),
            StopReason::EndTurn,
            Usage::default(),
        )),
    )
    .await;
    let mut spec = template();
    spec.metadata = asterpath_app::workspace::metadata(&fixture.root, None);
    let owned = cli
        .create_session(spec)
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let id = owned.id();

    let desktop = desktop(&fixture, Arc::new(ScriptedProvider::new("p"))).await;
    assert_eq!(desktop.access(id).await.unwrap(), Access::Elsewhere);
    assert_eq!(
        desktop.sessions(&fixture.root).await.unwrap()[0].status,
        Status::Elsewhere
    );
    assert!(desktop.send(id, "mine".to_owned()).await.is_err());
    let mut view = desktop.watch(id, None).await.unwrap();
    until(&mut view, |event| *event == ViewEvent::Synced).await;

    owned
        .submit_input(UserInput {
            id: InputId::from_bytes([1; 16]),
            parts: vec![Part::Text {
                text: "theirs".to_owned(),
            }],
        })
        .await
        .unwrap();
    let mut drive = owned.drive();
    while drive.next().await.is_some() {}
    drop(drive);
    let seen = until(&mut view, |event| turn_committed(event).is_some()).await;
    let inputs: Vec<&Vec<Part>> = seen
        .iter()
        .filter_map(|event| match event {
            ViewEvent::Record {
                event: SessionEvent::InputRecorded { input },
                ..
            } => Some(input),
            _ => None,
        })
        .collect();
    assert_eq!(
        inputs,
        [&vec![Part::Text {
            text: "theirs".to_owned()
        }]],
        "the refused input was never logged"
    );
}

/// The JSON `src/lib/commands.ts` reads.
#[test]
fn view_events_serialize_as_the_frontend_types_them() {
    let turn = TurnId::from_bytes([1; 16]);
    let events = [
        ViewEvent::Record {
            log_id: LogId::from_i64(3),
            event: SessionEvent::InputRecorded {
                input: vec![Part::Text {
                    text: "hi".to_owned(),
                }],
            },
        },
        ViewEvent::Record {
            log_id: LogId::from_i64(4),
            event: SessionEvent::TurnCommitted {
                turn_id: turn,
                reason: TurnEndReason::Cancelled,
            },
        },
        ViewEvent::Synced,
        ViewEvent::Live {
            event: AgentEvent::ModelDelta {
                turn,
                step: StepId::from_bytes([2; 16]),
                output_index: 0,
                part_index: 1,
                delta: PartDelta::ReasoningSummary {
                    text: "hm".to_owned(),
                },
            },
        },
    ];
    let turn = "01".repeat(16);
    assert_eq!(
        serde_json::to_value(events).unwrap(),
        serde_json::json!([
            {"kind": "record", "log_id": 3, "event": {"InputRecorded": {"input": [{"Text": {"text": "hi"}}]}}},
            {"kind": "record", "log_id": 4, "event": {"TurnCommitted": {"turn_id": turn, "reason": "Cancelled"}}},
            {"kind": "synced"},
            {"kind": "live", "event": {
                "type": "model_delta", "turn": turn, "step": "02".repeat(16),
                "output_index": 0, "part_index": 1,
                "delta": {"type": "reasoning_summary", "text": "hm"},
            }},
        ])
    );
}

async fn wait_for_status(desktop: &Desktop, root: &Path, id: SessionId, status: Status) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let sessions = desktop.sessions(root).await.unwrap();
            if sessions.iter().any(|s| s.id == id && s.status == status) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{id:?} never became {status:?}"));
}

#[tokio::test]
async fn sessions_run_at_once_in_the_background_and_list_their_status() {
    let fixture = fixture();
    let (provider, permits) = gated(
        ScriptedProvider::new("p")
            .respond(reply("a"), StopReason::EndTurn, Usage::default())
            .respond(reply("b"), StopReason::EndTurn, Usage::default()),
    );
    let desktop = desktop(&fixture, provider).await;
    let a = desktop
        .create_session(&fixture.root, None, None)
        .await
        .unwrap();
    assert_eq!(a.status, Status::Idle);
    let b = desktop
        .create_session(&fixture.root, None, None)
        .await
        .unwrap();

    // Nobody watches either session: driving does not need a view.
    desktop.send(a.id, "go a".to_owned()).await.unwrap();
    desktop.send(b.id, "go b".to_owned()).await.unwrap();
    wait_for_status(&desktop, &fixture.root, a.id, Status::Running).await;
    wait_for_status(&desktop, &fixture.root, b.id, Status::Running).await;

    permits.add_permits(2);
    wait_for_status(&desktop, &fixture.root, a.id, Status::Idle).await;
    wait_for_status(&desktop, &fixture.root, b.id, Status::Idle).await;
}

/// Classifies every call as `Ask`.
struct AlwaysAsk;

impl ApprovalPolicy for AlwaysAsk {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        ApprovalVerdict::Ask
    }
}

#[tokio::test]
async fn a_tool_call_waiting_for_approval_lists_the_session_as_awaiting_approval() {
    let fixture = fixture();
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![Item::ToolCall {
            call_id: "call-1".to_owned(),
            provider_item_id: None,
            name: ToolName::new("read_file"),
            raw_args: JsonString::new(r#"{"path":"x"}"#),
            raw: None,
        }],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let asterpath = Asterpath::builder()
        .database(&fixture.db)
        .providers(vec![provider])
        .approval(Arc::new(AlwaysAsk))
        .environment(Arc::new(WorkRoot {
            workspace: workspace(),
            new_session: false,
            pinned: None,
        }))
        .build()
        .await
        .unwrap();
    let desktop = Desktop::new(
        asterpath,
        Arc::new(ApprovalModes::new(ApprovalMode::default())),
        workspace(),
        template(),
        fixture.root.join("projects.json"),
    );
    let id = desktop
        .create_session(&fixture.root, None, None)
        .await
        .unwrap()
        .id;

    desktop.send(id, "read".to_owned()).await.unwrap();
    wait_for_status(&desktop, &fixture.root, id, Status::AwaitingApproval).await;
    desktop.cancel(id).await.unwrap();
}

#[tokio::test]
async fn the_status_bar_shows_the_model_and_tokens_and_switches_the_model() {
    let fixture = fixture();
    let usage = Usage {
        input: InputTokens {
            uncached: 10,
            cache_read: 2,
            cache_write: 0,
        },
        output: OutputTokens {
            total: 5,
            reasoning: 0,
        },
    };
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(reply("one"), StopReason::EndTurn, usage)
            .respond(reply("two"), StopReason::EndTurn, usage),
    );
    let desktop = desktop(&fixture, provider.clone()).await;
    assert_eq!(desktop.models(), [template().model]);
    let id = desktop
        .create_session(&fixture.root, None, None)
        .await
        .unwrap()
        .id;
    let mut view = desktop.watch(id, None).await.unwrap();

    desktop.send(id, "one".to_owned()).await.unwrap();
    until(&mut view, |event| turn_committed(event).is_some()).await;
    let info = desktop.session_info(id).await.unwrap();
    assert_eq!(info.model, template().model);
    assert_eq!(info.tokens, 17);
    assert_eq!(info.error, None);

    let mut other = template().model;
    other.model = ModelId::new("model-2");
    desktop.set_model(id, other.clone()).await.unwrap();
    assert_eq!(desktop.session_info(id).await.unwrap().model, other);
    desktop.send(id, "two".to_owned()).await.unwrap();
    until(&mut view, |event| turn_committed(event).is_some()).await;
    assert_eq!(provider.requests()[1].spec, other);
    assert_eq!(desktop.session_info(id).await.unwrap().tokens, 34);
}

/// A model switch is an external write (ADR-0030): it needs no claim, so it
/// works while another process drives the session.
#[tokio::test]
async fn the_model_switches_while_another_process_drives() {
    let fixture = fixture();
    let cli = engine(&fixture.db, Arc::new(ScriptedProvider::new("p"))).await;
    let mut spec = template();
    spec.metadata = asterpath_app::workspace::metadata(&fixture.root, None);
    let owned = cli
        .create_session(spec)
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    let id = owned.id();

    let desktop = desktop(&fixture, Arc::new(ScriptedProvider::new("p"))).await;
    let mut other = template().model;
    other.model = ModelId::new("model-2");
    desktop.set_model(id, other.clone()).await.unwrap();
    assert_eq!(desktop.session_info(id).await.unwrap().model, other);
    assert_eq!(desktop.access(id).await.unwrap(), Access::Elsewhere);
}

#[tokio::test]
async fn a_drive_error_reaches_the_status_bar() {
    let fixture = fixture();
    // The CLI's config has a provider this app does not.
    let cli = engine(&fixture.db, Arc::new(ScriptedProvider::new("q"))).await;
    let mut spec = template();
    spec.model.provider = ProviderId::new("q");
    spec.metadata = asterpath_app::workspace::metadata(&fixture.root, None);
    let id = cli.create_session(spec).await.unwrap().id();

    let desktop = desktop(&fixture, Arc::new(ScriptedProvider::new("p"))).await;
    desktop.send(id, "hi".to_owned()).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(error) = desktop.session_info(id).await.unwrap().error {
                return error;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the error is kept");
    assert!(error.contains('q'), "{error}");
    assert_eq!(desktop.access(id).await.unwrap(), Access::Free);
}

/// A store whose next append fails once `fail_next` is set.
struct Flaky {
    inner: Arc<dyn SessionStore>,
    fail_next: AtomicBool,
}

impl SessionStore for Flaky {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Box::pin(async { Err(StorageError::Backend("disk full".to_owned())) });
        }
        self.inner.append(entry)
    }
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        self.inner.scan(query)
    }
    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        self.inner.frontier(session)
    }
    fn sessions(
        &self,
        query: SessionQuery,
    ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>> {
        self.inner.sessions(query)
    }
    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        self.inner.parent(session)
    }
    fn claim(&self, root: SessionId) -> BoxFuture<'_, Result<Claim, StorageError>> {
        self.inner.claim(root)
    }
    fn owner(&self, root: SessionId) -> BoxFuture<'_, Result<Option<OwnerToken>, StorageError>> {
        self.inner.owner(root)
    }
    fn renew(&self) -> BoxFuture<'_, Result<Vec<OwnerToken>, StorageError>> {
        self.inner.renew()
    }
    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>> {
        self.inner.retire(session, expected_frontier)
    }
}

#[tokio::test]
async fn input_sent_just_before_a_drive_error_still_gets_a_drive() {
    let fixture = fixture();
    let (provider, permits) = gated(
        ScriptedProvider::new("p")
            .respond(reply("lost"), StopReason::EndTurn, Usage::default())
            .respond(reply("recovered"), StopReason::EndTurn, Usage::default())
            .respond(reply("more done"), StopReason::EndTurn, Usage::default()),
    );
    let store = Arc::new(Flaky {
        inner: Arc::new(InMemoryEventLog::new()),
        fail_next: AtomicBool::new(false),
    });
    let asterpath = Asterpath::builder()
        .store(store.clone())
        .providers(vec![provider.clone() as Arc<dyn Provider>])
        .environment(Arc::new(WorkRoot {
            workspace: workspace(),
            new_session: false,
            pinned: None,
        }))
        .build()
        .await
        .expect("build");
    let desktop = Desktop::new(
        asterpath,
        Arc::new(ApprovalModes::new(ApprovalMode::default())),
        workspace(),
        template(),
        fixture.root.join("projects.json"),
    );
    let id = desktop
        .create_session(&fixture.root, None, None)
        .await
        .unwrap()
        .id;
    let mut view = desktop.watch(id, None).await.unwrap();

    desktop.send(id, "go".to_owned()).await.unwrap();
    until(&mut view, turn_started).await;
    desktop.send(id, "more".to_owned()).await.unwrap();
    // The step's answer cannot be logged: the drive fails with "more" waiting.
    store.fail_next.store(true, Ordering::SeqCst);
    permits.add_permits(3);

    until(&mut view, |event| {
        turn_committed(event) == Some(&TurnEndReason::Completed)
    })
    .await;
    let last = provider.inner.requests().pop().unwrap();
    assert_eq!(user_texts(&last), ["go", "more"]);
}

#[tokio::test]
async fn a_session_created_with_a_chosen_profile_starts_with_its_prompt_and_tools() {
    let fixture = fixture();
    let agents = fixture.root.join(".asterpath/agents");
    std::fs::create_dir_all(&agents).unwrap();
    std::fs::write(
        agents.join("coder.md"),
        "---\ndescription: Writes code\n---\nYou code.\n",
    )
    .unwrap();
    std::fs::write(
        agents.join("reviewer.md"),
        "---\ntools: [read_file]\n---\nYou review.\n",
    )
    .unwrap();
    std::fs::write(agents.join("searcher.md"), "---\ntools: [glob]\n---\n").unwrap();
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        reply("ok"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let desktop = Desktop::new(
        engine(&fixture.db, provider.clone()).await,
        Arc::new(ApprovalModes::new(ApprovalMode::default())),
        Workspace {
            agent: Some("coder".to_owned()),
            ..workspace()
        },
        template(),
        fixture.root.join("projects.json"),
    );

    let choices = desktop.agents(&fixture.root).unwrap();
    assert_eq!(choices.default.as_deref(), Some("coder"));
    let names: Vec<(&str, Option<&str>)> = choices
        .agents
        .iter()
        .map(|agent| (agent.name.as_str(), agent.description.as_deref()))
        .collect();
    assert_eq!(
        names,
        [
            ("coder", Some("Writes code")),
            ("reviewer", None),
            ("searcher", None)
        ]
    );

    let defaulted = desktop
        .create_session(&fixture.root, None, None)
        .await
        .unwrap();
    assert_eq!(defaulted.agent.as_deref(), Some("coder"));
    let created = desktop
        .create_session(&fixture.root, None, Some("reviewer"))
        .await
        .unwrap();
    assert_eq!(created.agent.as_deref(), Some("reviewer"));
    let listed = desktop.sessions(&fixture.root).await.unwrap();
    assert!(listed.contains(&created), "{listed:?}");
    let info = desktop.session_info(created.id).await.unwrap();
    assert_eq!(info.agent.as_deref(), Some("reviewer"));
    let unknown = desktop
        .create_session(&fixture.root, None, Some("missing"))
        .await
        .unwrap_err();
    assert!(unknown.contains("unknown agent missing"), "{unknown}");
    // `glob` is whitelisted but not a tool of this work root: an error, as in the CLI.
    let untooled = desktop
        .create_session(&fixture.root, None, Some("searcher"))
        .await
        .unwrap_err();
    assert!(untooled.contains("unknown tool glob"), "{untooled}");

    let mut view = desktop.watch(created.id, None).await.unwrap();
    desktop.send(created.id, "hi".to_owned()).await.unwrap();
    until(&mut view, |event| turn_committed(event).is_some()).await;
    let request = &provider.requests()[0];
    assert!(
        matches!(&request.system[0], Part::Text { text } if text == "You review."),
        "{:?}",
        request.system
    );
    let tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    assert_eq!(tools, ["read_file"]);
}

#[tokio::test]
async fn without_default_agent_the_picker_preselects_the_default_profile() {
    let fixture = fixture();
    let agents = fixture.root.join(".asterpath/agents");
    std::fs::create_dir_all(&agents).unwrap();
    std::fs::write(agents.join("default.md"), "You help.\n").unwrap();
    let desktop = Desktop::new(
        engine(&fixture.db, Arc::new(ScriptedProvider::new("p"))).await,
        Arc::new(ApprovalModes::new(ApprovalMode::default())),
        workspace(),
        template(),
        fixture.root.join("projects.json"),
    );
    let choices = desktop.agents(&fixture.root).unwrap();
    assert_eq!(choices.default.as_deref(), Some("default"));
}

#[tokio::test]
async fn a_default_agent_without_a_profile_is_an_error_up_front() {
    let fixture = fixture();
    let desktop = Desktop::new(
        engine(&fixture.db, Arc::new(ScriptedProvider::new("p"))).await,
        Arc::new(ApprovalModes::new(ApprovalMode::default())),
        Workspace {
            agent: Some("missing".to_owned()),
            ..workspace()
        },
        template(),
        fixture.root.join("projects.json"),
    );
    let error = desktop.agents(&fixture.root).unwrap_err();
    assert!(error.contains("unknown agent missing"), "{error}");
}
