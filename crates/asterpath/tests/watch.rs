//! Watching a session (ADR-0038): committed records replay from a cursor,
//! a synced marker follows, then new commits; streamed deltas reach every
//! watcher in the owner process. Two `Asterpath`s over one store stand for
//! two processes.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use asterpath::protocol::{
    AgentEvent, Item, JsonString, LogId, ModelId, ModelRequest, ModelSpec, Part, Provider,
    ProviderCapabilities, ProviderError, ProviderId, Role, SessionEvent, SessionSpec, SessionStore,
    StopReason, StreamEvent, ToolName, ToolSelection, Usage, UserInput,
};
use asterpath::{Asterpath, OwnedSession, WatchEvent};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use tokio::sync::Notify;

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: vec![text("be helpful")],
        tools: ToolSelection::All,
        request: Default::default(),
        turn: Default::default(),
        compaction: Default::default(),
        subagents: Default::default(),
        metadata: Default::default(),
    }
}

fn input(id: u8) -> UserInput {
    UserInput {
        id: asterpath::protocol::InputId::from_bytes([id; 16]),
        parts: vec![text("go")],
    }
}

fn assistant(value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }]
}

fn answers(count: usize) -> ScriptedProvider {
    (0..count).fold(ScriptedProvider::new("p"), |provider, _| {
        provider.respond(assistant("done"), StopReason::EndTurn, Usage::default())
    })
}

async fn process(store: Arc<dyn SessionStore>, provider: ScriptedProvider) -> Asterpath {
    Asterpath::builder()
        .store(store)
        .providers(vec![Arc::new(provider) as Arc<dyn Provider>])
        .build()
        .await
        .expect("build")
}

async fn drive_all(owned: &OwnedSession) {
    let mut stream = owned.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
}

/// Watch events up to and including the `turns`-th committed `TurnCommitted`.
async fn until_turn_committed(
    stream: &mut BoxStream<'static, Result<WatchEvent, asterpath::AsterError>>,
    mut turns: usize,
) -> Vec<WatchEvent> {
    let mut events = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = stream.next().await {
            let event = event.expect("watch");
            let done = matches!(
                &event,
                WatchEvent::Record(record) if matches!(record.event, SessionEvent::TurnCommitted { .. })
            );
            events.push(event);
            turns -= usize::from(done);
            if turns == 0 {
                break;
            }
        }
    })
    .await
    .expect("TurnCommitted arrives in time");
    events
}

fn kinds(events: &[WatchEvent]) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|event| match event {
            WatchEvent::Record(record) => Some(record.event.kind()),
            _ => None,
        })
        .collect()
}

fn deltas(events: &[WatchEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, WatchEvent::Live(AgentEvent::ModelDelta { .. })))
        .count()
}

#[tokio::test]
async fn every_watcher_in_the_owner_process_sees_deltas_and_records() {
    let asterpath = process(Arc::new(InMemoryEventLog::new()), answers(1)).await;
    let handle = asterpath.create_session(spec()).await.unwrap();
    let mut first = handle.watch(None);
    let mut second = handle.watch(None);

    handle.submit_input(input(1)).await.unwrap();
    let owned = handle.claim().await.unwrap();
    let (first, second, ()) = tokio::join!(
        until_turn_committed(&mut first, 1),
        until_turn_committed(&mut second, 1),
        drive_all(&owned),
    );

    for events in [&first, &second] {
        assert!(matches!(events[0], WatchEvent::Record(_)));
        assert!(events.contains(&WatchEvent::Synced));
        assert!(deltas(events) > 0, "deltas reach every watcher");
        assert_eq!(
            kinds(events),
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
    }
}

#[tokio::test]
async fn a_watcher_in_another_process_sees_commits_but_no_deltas() {
    let shared = InMemoryEventLog::new();
    let observer = process(Arc::new(shared.other_process()), answers(0)).await;
    let owner = process(Arc::new(shared), answers(1)).await;
    let handle = owner.create_session(spec()).await.unwrap();
    let mut watch = observer
        .open_session(handle.id())
        .await
        .unwrap()
        .watch(None);

    handle.submit_input(input(1)).await.unwrap();
    let owned = handle.claim().await.unwrap();
    let (events, ()) = tokio::join!(until_turn_committed(&mut watch, 1), drive_all(&owned));

    assert_eq!(deltas(&events), 0);
    assert_eq!(kinds(&events).last(), Some(&"TurnCommitted"));
}

#[tokio::test]
async fn watching_from_a_cursor_neither_drops_nor_repeats_records() {
    let asterpath = process(Arc::new(InMemoryEventLog::new()), answers(2)).await;
    let handle = asterpath.create_session(spec()).await.unwrap();
    handle.submit_input(input(1)).await.unwrap();
    let owned = handle.claim().await.unwrap();
    drive_all(&owned).await;
    let committed: Vec<LogId> = handle
        .watch(None)
        .take_while(|event| std::future::ready(!matches!(event, Ok(WatchEvent::Synced))))
        .map(|event| match event.unwrap() {
            WatchEvent::Record(record) => record.log_id,
            other => panic!("only records replay, got {other:?}"),
        })
        .collect()
        .await;
    let cursor = committed[3];

    let mut watch = handle.watch(Some(cursor));
    handle.submit_input(input(2)).await.unwrap();
    // The replayed tail holds the first turn's TurnCommitted.
    let (events, ()) = tokio::join!(until_turn_committed(&mut watch, 2), drive_all(&owned));

    let records: Vec<LogId> = events
        .iter()
        .filter_map(|event| match event {
            WatchEvent::Record(record) => Some(record.log_id),
            _ => None,
        })
        .collect();
    let all = asterpath
        .store()
        .scan(asterpath::protocol::LogQuery {
            session: handle.id(),
            after: Some(cursor),
            limit: None,
        })
        .map(|entry| entry.unwrap().log_id)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        records, all,
        "every record after the cursor, once, in order"
    );
    assert_eq!(
        events.iter().filter(|e| **e == WatchEvent::Synced).count(),
        1
    );
}

/// Holds back model call `gated` until `gate` is notified.
struct Gated {
    inner: ScriptedProvider,
    calls: AtomicUsize,
    gated: usize,
    gate: Arc<Notify>,
}

impl Provider for Gated {
    fn id(&self) -> ProviderId {
        self.inner.id()
    }

    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities {
        self.inner.capabilities(spec)
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) != self.gated {
            return self.inner.stream(request);
        }
        let gate = Arc::clone(&self.gate);
        stream::once(async move { gate.notified().await })
            .flat_map(move |()| self.inner.stream(request.clone()))
            .boxed()
    }
}

#[tokio::test]
async fn a_child_session_can_be_watched_while_its_parent_drives() {
    let gate = Arc::new(Notify::new());
    let provider = Gated {
        inner: ScriptedProvider::new("p")
            .respond(
                vec![Item::ToolCall {
                    call_id: "call-spawn".to_owned(),
                    provider_item_id: None,
                    name: ToolName::new("spawn_agent"),
                    raw_args: JsonString::new(r#"{"task":"look around"}"#),
                    raw: None,
                }],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                assistant("child summary"),
                StopReason::EndTurn,
                Usage::default(),
            )
            .respond(
                assistant("parent done"),
                StopReason::EndTurn,
                Usage::default(),
            ),
        calls: AtomicUsize::new(0),
        gated: 1,
        gate: Arc::clone(&gate),
    };
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![Arc::new(provider) as Arc<dyn Provider>])
        .build()
        .await
        .expect("build");
    let handle = asterpath.create_session(spec()).await.unwrap();
    handle.submit_input(input(1)).await.unwrap();
    let mut parent = handle.watch(None);
    let owned = handle.claim().await.unwrap();

    let watch_child = async {
        let child = loop {
            match parent.next().await.expect("parent watch").expect("watch") {
                WatchEvent::Record(record) => match record.event {
                    SessionEvent::ChildSessionSpawned { child, .. } => break child,
                    _ => continue,
                },
                _ => continue,
            }
        };
        let mut watch = asterpath.open_session(child).await.unwrap().watch(None);
        // Subscribed before the child model streams: its deltas reach us.
        let first = watch.next().await.expect("child watch").expect("watch");
        assert!(matches!(first, WatchEvent::Record(_)));
        gate.notify_one();
        until_turn_committed(&mut watch, 1).await
    };
    let (events, ()) = tokio::join!(watch_child, drive_all(&owned));

    assert!(deltas(&events) > 0, "child deltas are no longer dropped");
    assert_eq!(kinds(&events).last(), Some(&"TurnCommitted"));
}
