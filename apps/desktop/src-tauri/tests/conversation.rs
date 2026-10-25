//! New sessions, the background driver and the conversation view (#151).

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use asterpath::Asterpath;
use asterpath_desktop::{Access, Desktop, PermissionMode, Permissions, ViewEvent};
use asterpath_host::{WorkRoot, Workspace};
use asterpath_protocol::{
    AgentEvent, InputId, Item, LogId, ModelRequest, ModelSpec, Part, PartDelta, Provider,
    ProviderCapabilities, ProviderError, ProviderId, Role, SessionEvent, StepId, StopReason,
    StreamEvent, TurnEndReason, TurnId, Usage, UserInput,
};
use asterpath_testing::ScriptedProvider;
use common::template;
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
            new: false,
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
        Arc::new(Permissions::new(PermissionMode::default())),
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
        .create_session(&fixture.root, Some("fix"))
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
        .create_session(&fixture.root, None)
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
        .create_session(&fixture.root, None)
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
    spec.metadata = asterpath_host::metadata(&fixture.root, None);
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
