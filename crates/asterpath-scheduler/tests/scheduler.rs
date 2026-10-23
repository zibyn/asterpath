//! Scheduler behaviour at its public seam: spawn/materialization, recorded
//! outcomes, depth, outcome mapping, lost children and cancel.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath_core::{
    AwaitSignal, Clock, DedupKey, DurableExecutor, EventLog, ExecutionError, LogEntry, LogId,
    LogQuery, SessionCatalog, SessionId, SignalDelivery, SignalPayload, StorageError, SystemClock,
    Timestamp, ToolCallId, TurnId, WakeCause,
};
use asterpath_protocol::{
    AgentError, AgentEvent, AgentRunner, CancelReason, ChildFailure, ChildOutcome,
    CompactionConfig, ConfigSnapshot, ContextPolicy, Item, ModelAlias, ModelId, ModelSpec,
    Parallelism, Part, Provenance, ProviderId, ReplaySafety, RequestConfig, Role, RunRequest,
    SessionEvent, SideEffect, StopReason, SubagentConfig, Summary, ToolDescriptor, ToolName,
    ToolResultStatus, TurnConfig, TurnEndReason, Usage, dedup, encode_entry,
};
use asterpath_protocol::{
    CancelToken, CancellationRegistry, Tool, ToolArgs, ToolContext, ToolError,
};
use asterpath_scheduler::{
    ChildRef, Scheduler, SpawnError, SpawnRequest, SpawnTool, SubagentSpawner, WaitError,
};
use asterpath_testing::{InMemoryEventLog, ManualClock};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

type Log = Arc<InMemoryEventLog>;

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
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

fn config(max_depth: u8) -> ConfigSnapshot {
    ConfigSnapshot {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: vec![text("inherited system")],
        tools: vec![descriptor("echo"), descriptor("spawn_agent")],
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig {
            max_depth,
            ..SubagentConfig::default()
        },
    }
}

fn parent_ref(session: SessionId) -> asterpath_protocol::ParentRef {
    asterpath_protocol::ParentRef {
        session,
        turn: TurnId::from_bytes([2; 16]),
        tool_call: ToolCallId::from_bytes([3; 16]),
    }
}

async fn append(log: &dyn EventLog, session: SessionId, event: SessionEvent) -> LogId {
    let entry = encode_entry(session, None, &event).expect("encodes");
    log.append(entry).await.expect("appends")
}

fn encoded(
    session: SessionId,
    event: &SessionEvent,
    key: Option<DedupKey>,
) -> asterpath_core::NewEntry {
    encode_entry(session, key, event).expect("encodes")
}

async fn kinds(log: &dyn EventLog, session: SessionId) -> Vec<&'static str> {
    events(log, session)
        .await
        .into_iter()
        .map(|event| event.kind())
        .collect()
}

async fn events(log: &dyn EventLog, session: SessionId) -> Vec<SessionEvent> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut events = Vec::new();
    while let Some(entry) = stream.next().await {
        let entry = entry.expect("scan");
        events.push(asterpath_protocol::decode_entry(&entry).expect("decodes"));
    }
    events
}

/// `None` for the root; every other session has one parent edge.
#[derive(Default)]
struct FakeCatalog {
    parents: Mutex<HashMap<SessionId, SessionId>>,
}

impl FakeCatalog {
    fn with_chain(depth: usize, leaf: SessionId) -> Arc<Self> {
        let catalog = Arc::new(Self::default());
        let mut child = leaf;
        for index in 0..depth {
            let parent = SessionId::from_bytes([0x10 + index as u8; 16]);
            catalog.parents.lock().unwrap().insert(child, parent);
            child = parent;
        }
        catalog
    }
}

impl SessionCatalog for FakeCatalog {
    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        let parent = self.parents.lock().unwrap().get(&session).copied();
        Box::pin(async move { Ok(parent) })
    }

    fn children(&self, _session: SessionId) -> BoxStream<'_, Result<SessionId, StorageError>> {
        stream::empty().boxed()
    }
}

/// The scheduler never needs a real wait in these directed tests; the recorded
/// outcome path returns before `await_wake`.
struct NoopExecutor;

impl DurableExecutor for NoopExecutor {
    fn await_signal(
        &self,
        _req: AwaitSignal,
    ) -> BoxFuture<'_, Result<SignalPayload, ExecutionError>> {
        Box::pin(async { Err(ExecutionError::Backend("unused".to_owned())) })
    }

    fn deliver_signal(
        &self,
        _delivery: SignalDelivery,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async { Ok(()) })
    }

    fn await_wake(
        &self,
        _session: SessionId,
        _cause: WakeCause,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(std::future::pending())
    }

    fn wake(&self, _session: SessionId, _cause: WakeCause) {}
}

fn new_scheduler(
    log: Arc<dyn EventLog>,
    catalog: Arc<dyn SessionCatalog>,
    aliases: BTreeMap<ModelAlias, ModelSpec>,
) -> Arc<Scheduler> {
    Arc::new(Scheduler::new(
        log,
        catalog,
        Arc::new(NoopExecutor),
        aliases,
    ))
}

fn new_scheduler_with(
    log: Arc<dyn EventLog>,
    catalog: Arc<dyn SessionCatalog>,
    executor: Arc<dyn DurableExecutor>,
    clock: Arc<dyn Clock>,
    subagents: SubagentConfig,
) -> Arc<Scheduler> {
    Arc::new(Scheduler::with_runtime(
        log,
        catalog,
        executor,
        BTreeMap::new(),
        clock,
        subagents,
        Arc::new(CancellationRegistry::new()),
    ))
}

/// Seeds a root parent with its `SessionCreated` and the spawn
/// `ToolCallRequested`; returns the parent reference and `caused_by` id.
async fn seed_parent(
    log: &InMemoryEventLog,
    max_depth: u8,
) -> (SessionId, asterpath_protocol::ParentRef, LogId) {
    let parent = SessionId::from_bytes([1; 16]);
    append(
        log,
        parent,
        SessionEvent::SessionCreated {
            config: config(max_depth),
            parent: None,
        },
    )
    .await;
    let reference = parent_ref(parent);
    let caused_by = append(
        log,
        parent,
        SessionEvent::ToolCallRequested {
            turn_id: reference.turn,
            step_id: asterpath_core::StepId::from_bytes([4; 16]),
            tool_call_id: reference.tool_call,
            provider_call_id: "call-1".to_owned(),
            name: ToolName::new("spawn_agent"),
            raw_args: asterpath_protocol::JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
    )
    .await;
    (parent, reference, caused_by)
}

async fn completed_child(
    log: &InMemoryEventLog,
    child: SessionId,
    answer: &str,
    reason: TurnEndReason,
) {
    append(
        log,
        child,
        SessionEvent::AssistantMessage {
            turn_id: TurnId::from_bytes([5; 16]),
            step_id: asterpath_core::StepId::from_bytes([6; 16]),
            items: vec![
                Item::Reasoning {
                    summary: vec![text("hidden reasoning")],
                    binding: asterpath_protocol::ReasoningBinding {
                        provider: ProviderId::new("p"),
                        model: ModelId::new("model-1"),
                        prefix_epoch: 0,
                        tools_hash: "h".to_owned(),
                    },
                    raw: asterpath_protocol::RawProviderData {
                        provider: ProviderId::new("p"),
                        model: None,
                        kind: "reasoning".to_owned(),
                        json: asterpath_protocol::JsonString::new("{}"),
                    },
                },
                Item::Message {
                    role: Role::Assistant,
                    parts: vec![
                        text(answer),
                        Part::Refusal {
                            text: "refusal".to_owned(),
                        },
                    ],
                },
            ],
            raw: None,
            model: config(3).model,
            usage: Usage::default(),
            stop_reason: StopReason::EndTurn,
        },
    )
    .await;
    append(
        log,
        child,
        SessionEvent::TurnCommitted {
            turn_id: TurnId::from_bytes([5; 16]),
            reason,
        },
    )
    .await;
}

#[tokio::test]
async fn spawn_materializes_once_and_reuses_the_child_on_replay() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );

    let mut request = SpawnRequest::new(reference.clone(), caused_by, vec![text("do it")]);
    request.tool_allowlist = Some(vec![ToolName::new("echo")]);
    let child = scheduler.spawn(request.clone()).await.expect("spawn");

    let parent_events = events(&*log, parent).await;
    assert_eq!(
        parent_events
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
            .count(),
        1
    );
    let SessionEvent::ChildSessionSpawned {
        child: recorded,
        config: spawned,
        ..
    } = parent_events
        .iter()
        .find(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
        .expect("spawn recorded")
    else {
        unreachable!()
    };
    assert_eq!(*recorded, child.child);

    let child_events = events(&*log, child.child).await;
    assert_eq!(
        child_events
            .iter()
            .map(SessionEvent::kind)
            .collect::<Vec<_>>(),
        ["SessionCreated", "InputRecorded"]
    );
    let SessionEvent::SessionCreated {
        config: created,
        parent: edge,
    } = &child_events[0]
    else {
        unreachable!()
    };
    assert_eq!(
        created, spawned,
        "ChildSessionSpawned.config is the child snapshot"
    );
    assert_eq!(edge.as_ref().map(|edge| edge.session), Some(parent));
    assert_eq!(
        created.tools.len(),
        1,
        "the allowlist narrows the parent snapshot"
    );
    assert_eq!(created.tools[0].name, ToolName::new("echo"));
    assert_eq!(created.system, config(3).system);
    let SessionEvent::InputRecorded { input } = &child_events[1] else {
        unreachable!()
    };
    assert_eq!(input, &vec![text("do it")]);

    // R1/R2: a replayed spawn returns the same child and writes nothing twice.
    let again = scheduler.spawn(request).await.expect("replay spawn");
    assert_eq!(again.child, child.child);
    assert_eq!(
        events(&*log, parent)
            .await
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
            .count(),
        1
    );
    assert_eq!(
        events(&*log, child.child)
            .await
            .iter()
            .map(SessionEvent::kind)
            .collect::<Vec<_>>(),
        ["SessionCreated", "InputRecorded"]
    );
}

#[tokio::test]
async fn spawn_honours_model_alias_and_rejects_invalid_narrowing() {
    let log = Arc::new(InMemoryEventLog::new());
    let (_, reference, caused_by) = seed_parent(&log, 3).await;
    let mut aliases = BTreeMap::new();
    aliases.insert(
        ModelAlias::new("fast"),
        ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("fast-model"),
            context_window: None,
        },
    );
    aliases.insert(
        ModelAlias::new("other-provider"),
        ModelSpec {
            provider: ProviderId::new("q"),
            model: ModelId::new("m"),
            context_window: None,
        },
    );
    let scheduler = new_scheduler(log.clone(), Arc::new(FakeCatalog::default()), aliases);

    // Each case uses its own tool call so R1 does not mask the validation.
    let mut call = 0u8;
    let mut request =
        |model_alias: Option<ModelAlias>, tool_allowlist: Option<Vec<ToolName>>, context| {
            call += 1;
            let mut parent = reference.clone();
            parent.tool_call = ToolCallId::from_bytes([0x40 + call; 16]);
            let mut request = SpawnRequest::new(parent, caused_by, vec![text("task")]);
            request.model_alias = model_alias;
            request.tool_allowlist = tool_allowlist;
            request.context = context;
            request
        };

    assert!(matches!(
        scheduler
            .spawn(request(
                Some(ModelAlias::new("missing")),
                None,
                ContextPolicy::Fresh
            ))
            .await,
        Err(SpawnError::InvalidArgs { .. })
    ));
    assert!(matches!(
        scheduler
            .spawn(request(
                Some(ModelAlias::new("other-provider")),
                None,
                ContextPolicy::Fresh
            ))
            .await,
        Err(SpawnError::InvalidArgs { .. })
    ));
    assert!(matches!(
        scheduler
            .spawn(request(
                None,
                Some(vec![ToolName::new("not-inherited")]),
                ContextPolicy::Fresh
            ))
            .await,
        Err(SpawnError::InvalidArgs { .. })
    ));
    assert_eq!(
        events(&*log, SessionId::from_bytes([1; 16]))
            .await
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
            .count(),
        0,
        "rejected spawns create no child records"
    );

    let child = scheduler
        .spawn(request(
            Some(ModelAlias::new("fast")),
            Some(vec![ToolName::new("echo")]),
            ContextPolicy::Fresh,
        ))
        .await
        .expect("alias spawn");
    let created = events(&*log, child.child).await;
    let SessionEvent::SessionCreated {
        config: created, ..
    } = &created[0]
    else {
        unreachable!()
    };
    assert_eq!(created.model.model, ModelId::new("fast-model"));
    assert_eq!(created.tools, vec![descriptor("echo")]);
}

#[tokio::test]
async fn depth_exceeded_creates_no_child() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    // Parent already sits at depth 3; a child would be depth 4.
    let catalog = FakeCatalog::with_chain(3, parent);
    let scheduler = new_scheduler(log.clone(), catalog, BTreeMap::new());

    let result = scheduler
        .spawn(SpawnRequest::new(
            reference,
            caused_by,
            vec![text("too deep")],
        ))
        .await;
    assert!(matches!(result, Err(SpawnError::DepthExceeded)));
    assert!(
        !events(&*log, parent)
            .await
            .iter()
            .any(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
    );
}

#[tokio::test]
async fn recorded_outcome_wins_without_reading_the_child() {
    let inner = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&inner, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    let outcome = ChildOutcome::Completed {
        summary: Summary {
            items: vec![text("already done")],
        },
    };
    inner
        .append(encoded(
            parent,
            &SessionEvent::ChildSessionSpawned {
                child,
                task: vec![text("task")],
                config: config(3),
                context: ContextPolicy::Fresh,
                parent_tool_call_id: reference.tool_call,
                caused_by,
                deadline: None,
            },
            Some(dedup::child_session(child)),
        ))
        .await
        .expect("spawn record");
    inner
        .append(encoded(
            parent,
            &SessionEvent::ChildSessionCompleted {
                child,
                outcome: outcome.clone(),
            },
            Some(dedup::child_session(child)),
        ))
        .await
        .expect("completion record");

    // Any child read fails, so a passing result proves R3 short-circuits.
    let log = Arc::new(PoisonLog {
        inner: inner.clone(),
        parent,
    });
    let scheduler = new_scheduler(log, Arc::new(FakeCatalog::default()), BTreeMap::new());
    let child_ref = ChildRef {
        parent: reference,
        child,
    };

    assert_eq!(
        scheduler.await_completion(&child_ref).await.expect("R3"),
        outcome
    );
}

#[tokio::test]
async fn child_terminal_becomes_one_parent_completion() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config(3),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: reference.tool_call,
            caused_by,
            deadline: None,
        },
    )
    .await;
    completed_child(&log, child, "the answer", TurnEndReason::Completed).await;

    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let child_ref = ChildRef {
        parent: reference,
        child,
    };
    let outcome = scheduler
        .await_completion(&child_ref)
        .await
        .expect("outcome");
    let ChildOutcome::Completed { summary } = &outcome else {
        panic!("expected completed, got {outcome:?}");
    };
    assert_eq!(
        summary.items,
        vec![
            text("the answer"),
            Part::Refusal {
                text: "refusal".to_owned()
            }
        ],
        "summary keeps text/refusal parts and drops reasoning"
    );

    // R3: the second call reads the record, never the child.
    assert_eq!(
        scheduler.await_completion(&child_ref).await.expect("R3"),
        outcome
    );
    assert_eq!(
        events(&*log, parent)
            .await
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionCompleted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn child_failure_reasons_map_to_child_failure() {
    let cases = [
        (TurnEndReason::Failed, ChildFailure::TurnFailed),
        (
            TurnEndReason::ContextExhausted,
            ChildFailure::ContextExhausted,
        ),
        (
            TurnEndReason::MaxStepsExceeded,
            ChildFailure::MaxStepsExceeded,
        ),
        (
            TurnEndReason::CompactionFailed,
            ChildFailure::CompactionFailed,
        ),
    ];
    for (reason, expected) in cases {
        let log = Arc::new(InMemoryEventLog::new());
        let (parent, reference, caused_by) = seed_parent(&log, 3).await;
        let child = SessionId::from_bytes([9; 16]);
        append(
            &*log,
            parent,
            SessionEvent::ChildSessionSpawned {
                child,
                task: vec![text("task")],
                config: config(3),
                context: ContextPolicy::Fresh,
                parent_tool_call_id: reference.tool_call,
                caused_by,
                deadline: None,
            },
        )
        .await;
        completed_child(&log, child, "partial", reason.clone()).await;

        let scheduler = new_scheduler(log, Arc::new(FakeCatalog::default()), BTreeMap::new());
        let outcome = scheduler
            .await_completion(&ChildRef {
                parent: reference,
                child,
            })
            .await
            .expect("outcome");
        assert!(
            matches!(outcome, ChildOutcome::Failed { reason, .. } if reason == expected),
            "{reason:?} must map to {expected:?}, got {outcome:?}"
        );
    }

    // Stopped is still a completed task.
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config(3),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: reference.tool_call,
            caused_by,
            deadline: None,
        },
    )
    .await;
    completed_child(
        &log,
        child,
        "stopped but done",
        TurnEndReason::Stopped {
            reason: "enough".to_owned(),
        },
    )
    .await;
    let scheduler = new_scheduler(log, Arc::new(FakeCatalog::default()), BTreeMap::new());
    assert!(matches!(
        scheduler
            .await_completion(&ChildRef {
                parent: reference,
                child,
            })
            .await,
        Ok(ChildOutcome::Completed { .. })
    ));
}

#[tokio::test]
async fn unreadable_child_records_lost_and_unblocks_replay() {
    let inner = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&inner, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*inner,
        parent,
        SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config(3),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: reference.tool_call,
            caused_by,
            deadline: None,
        },
    )
    .await;
    let log = Arc::new(PoisonLog {
        inner: inner.clone(),
        parent,
    });
    let scheduler = new_scheduler(log, Arc::new(FakeCatalog::default()), BTreeMap::new());
    let child_ref = ChildRef {
        parent: reference,
        child,
    };

    let error = scheduler.await_completion(&child_ref).await.unwrap_err();
    assert!(matches!(error, WaitError::Execution(_)));
    assert!(matches!(
        events(&*inner, parent)
            .await
            .iter()
            .find_map(|event| match event {
                SessionEvent::ChildSessionCompleted { outcome, .. } => Some(outcome.clone()),
                _ => None,
            }),
        Some(ChildOutcome::Lost { .. })
    ));
}

#[tokio::test]
async fn cancel_writes_parent_only_once_and_never_overwrites_an_outcome() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config(3),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: reference.tool_call,
            caused_by,
            deadline: None,
        },
    )
    .await;
    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let child_ref = ChildRef {
        parent: reference,
        child,
    };

    scheduler.cancel(&child_ref).await.expect("cancel");
    scheduler
        .cancel(&child_ref)
        .await
        .expect("idempotent cancel");
    let completions: Vec<_> = events(&*log, parent)
        .await
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ChildSessionCompleted { outcome, .. } => Some(outcome),
            _ => None,
        })
        .collect();
    assert_eq!(
        completions,
        vec![ChildOutcome::Cancelled {
            reason: CancelReason::Parent
        }]
    );

    // An already recorded outcome is authoritative.
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config(3),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: reference.tool_call,
            caused_by,
            deadline: None,
        },
    )
    .await;
    completed_child(&log, child, "done", TurnEndReason::Completed).await;
    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let child_ref = ChildRef {
        parent: reference,
        child,
    };
    scheduler
        .await_completion(&child_ref)
        .await
        .expect("record");
    scheduler
        .cancel(&child_ref)
        .await
        .expect("cancel after record");
    assert!(matches!(
        events(&*log, parent)
            .await
            .into_iter()
            .find(|event| matches!(event, SessionEvent::ChildSessionCompleted { .. })),
        Some(SessionEvent::ChildSessionCompleted {
            outcome: ChildOutcome::Completed { .. },
            ..
        })
    ));
}

#[tokio::test]
async fn driving_a_fresh_child_opens_exactly_one_turn() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let stub = Arc::new(StubRunner {
        log: log.clone(),
        runs: Mutex::new(0),
    });
    scheduler.set_runner(stub.clone());

    let child = scheduler
        .spawn(SpawnRequest::new(
            reference.clone(),
            caused_by,
            vec![text("task")],
        ))
        .await
        .expect("spawn");
    let outcome = scheduler.await_completion(&child).await.expect("outcome");
    assert!(matches!(outcome, ChildOutcome::Completed { .. }));

    let child_kinds = kinds(&*log, child.child).await;
    assert_eq!(
        child_kinds,
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "AssistantMessage",
            "StepCommitted",
            "TurnCommitted",
        ],
        "the first drive starts the turn; no StreamResumed is prepended"
    );
    assert_eq!(*stub.runs.lock().unwrap(), 1);
    assert_eq!(
        events(&*log, parent)
            .await
            .iter()
            .filter(|event| matches!(event, SessionEvent::ChildSessionCompleted { .. }))
            .count(),
        1
    );
}

fn tool_context(parent: &asterpath_protocol::ParentRef) -> ToolContext {
    ToolContext::new(
        parent.session,
        parent.turn,
        asterpath_core::StepId::from_bytes([4; 16]),
        parent.tool_call,
        CancelToken::new(),
    )
}

#[tokio::test]
async fn spawn_tool_maps_depth_to_invalid_args_and_rejects_overrides() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, _) = seed_parent(&log, 3).await;
    let catalog = FakeCatalog::with_chain(3, parent);
    let scheduler = new_scheduler(log.clone(), catalog, BTreeMap::new());
    let tool = SpawnTool::new(scheduler);

    let args = ToolArgs::new(r#"{"task":"too deep"}"#);
    assert!(matches!(
        tool.invoke(args, tool_context(&reference)).await,
        Err(ToolError::InvalidArgs { .. })
    ));
    assert!(
        !events(&*log, parent)
            .await
            .iter()
            .any(|event| matches!(event, SessionEvent::ChildSessionSpawned { .. }))
    );

    // A system/approval override cannot be expressed by the model.
    let args = ToolArgs::new(r#"{"task":"x","system":"override"}"#);
    assert!(matches!(
        tool.invoke(args, tool_context(&reference)).await,
        Err(ToolError::InvalidArgs { .. })
    ));
}

#[tokio::test]
async fn spawn_tool_maps_a_recorded_lost_outcome_to_internal() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config(3),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: reference.tool_call,
            caused_by,
            deadline: None,
        },
    )
    .await;
    append(
        &*log,
        parent,
        SessionEvent::ChildSessionCompleted {
            child,
            outcome: ChildOutcome::Lost {
                reason: "unreadable".to_owned(),
            },
        },
    )
    .await;

    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let tool = SpawnTool::new(scheduler);
    let args = ToolArgs::new(r#"{"task":"retry"}"#);
    assert!(matches!(
        tool.invoke(args, tool_context(&reference)).await,
        Err(ToolError::Internal { .. })
    ));
}

/// Fails every scan for a session other than the seeded parent.
struct PoisonLog {
    inner: Arc<InMemoryEventLog>,
    parent: SessionId,
}

impl EventLog for PoisonLog {
    fn append(
        &self,
        entry: asterpath_core::NewEntry,
    ) -> BoxFuture<'_, Result<LogId, StorageError>> {
        self.inner.append(entry)
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        if query.session == self.parent {
            return self.inner.scan(query);
        }
        let session = query.session;
        stream::once(async move {
            Err(StorageError::Corrupt {
                session: Some(session),
                detail: "poisoned child log".to_owned(),
            })
        })
        .boxed()
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        self.inner.frontier(session)
    }
}

/// Minimal runner: opens the turn and commits a completed turn.
struct StubRunner {
    log: Log,
    runs: Mutex<u32>,
}

impl AgentRunner for StubRunner {
    fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>> {
        *self.runs.lock().unwrap() += 1;
        let log = self.log.clone();
        let turn = TurnId::from_bytes([7; 16]);
        let step = asterpath_core::StepId::from_bytes([8; 16]);
        stream::once(async move {
            let events = [
                SessionEvent::TurnStarted { turn_id: turn },
                SessionEvent::AssistantMessage {
                    turn_id: turn,
                    step_id: step,
                    items: vec![Item::Message {
                        role: Role::Assistant,
                        parts: vec![text("child answer")],
                    }],
                    raw: None,
                    model: config(3).model,
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
                SessionEvent::StepCommitted {
                    turn_id: turn,
                    step_id: step,
                },
                SessionEvent::TurnCommitted {
                    turn_id: turn,
                    reason: TurnEndReason::Completed,
                },
            ];
            for event in events {
                let entry = encode_entry(req.session, None, &event).expect("encodes");
                log.append(entry).await.map_err(AgentError::Storage)?;
            }
            Ok(AgentEvent::TurnCommitted {
                turn,
                reason: TurnEndReason::Completed,
            })
        })
        .boxed()
    }

    fn compact_now(
        &self,
        _session: SessionId,
    ) -> BoxFuture<'_, Result<asterpath_protocol::CompactResult, AgentError>> {
        Box::pin(async {
            Err(AgentError::NotFound {
                session: SessionId::from_bytes([0; 16]),
            })
        })
    }
}

// --- #49: fork materialization and replay --------------------------------

const PARENT: SessionId = SessionId::from_bytes([1; 16]);
const CALL_A: ToolCallId = ToolCallId::from_bytes([0x21; 16]);
const CALL_SIBLING: ToolCallId = ToolCallId::from_bytes([0x22; 16]);
const CALL_SPAWN: ToolCallId = ToolCallId::from_bytes([3; 16]);
const FORK_TURN: TurnId = TurnId::from_bytes([2; 16]);
const STEP_1: asterpath_core::StepId = asterpath_core::StepId::from_bytes([0x11; 16]);
const STEP_2: asterpath_core::StepId = asterpath_core::StepId::from_bytes([0x12; 16]);

fn raw(provider: &str, kind: &str) -> asterpath_protocol::RawProviderData {
    asterpath_protocol::RawProviderData {
        provider: ProviderId::new(provider),
        model: Some(ModelId::new("model-1")),
        kind: kind.to_owned(),
        json: asterpath_protocol::JsonString::new("{\"sig\":1}"),
    }
}

fn reasoning(provider: &str) -> Item {
    Item::Reasoning {
        summary: vec![text("thought")],
        binding: asterpath_protocol::ReasoningBinding {
            provider: ProviderId::new(provider),
            model: ModelId::new("model-1"),
            prefix_epoch: 0,
            tools_hash: "hash".to_owned(),
        },
        raw: raw(provider, "thinking"),
    }
}

fn assistant_at(step: asterpath_core::StepId, items: Vec<Item>) -> SessionEvent {
    SessionEvent::AssistantMessage {
        turn_id: FORK_TURN,
        step_id: step,
        items,
        raw: None,
        model: config(3).model,
        usage: Usage::default(),
        stop_reason: StopReason::ToolUse,
    }
}

fn spawn_record(child: SessionId, context: ContextPolicy) -> SessionEvent {
    SessionEvent::ChildSessionSpawned {
        child,
        task: vec![text("fork task")],
        config: config(3),
        context,
        parent_tool_call_id: CALL_SPAWN,
        caused_by: LogId::from_i64(1),
        deadline: None,
    }
}

fn fork_parent_ref() -> asterpath_protocol::ParentRef {
    asterpath_protocol::ParentRef {
        session: PARENT,
        turn: FORK_TURN,
        tool_call: CALL_SPAWN,
    }
}

async fn append_keyed(
    log: &dyn EventLog,
    session: SessionId,
    event: SessionEvent,
    key: DedupKey,
) -> LogId {
    let entry = encode_entry(session, Some(key), &event).expect("encodes");
    log.append(entry).await.expect("appends")
}

/// Parent history before the spawn step, then the spawn step itself
/// (assistant message and sibling tool result) that the fork must not see.
async fn seed_fork_parent(log: &dyn EventLog, subagents: &SubagentConfig) -> (LogId, LogId) {
    append(
        log,
        PARENT,
        SessionEvent::SessionCreated {
            config: config_with(3, subagents.clone()),
            parent: None,
        },
    )
    .await;
    append(
        log,
        PARENT,
        SessionEvent::TurnStarted { turn_id: FORK_TURN },
    )
    .await;
    append(
        log,
        PARENT,
        SessionEvent::StepStarted {
            turn_id: FORK_TURN,
            step_id: STEP_1,
        },
    )
    .await;
    append(
        log,
        PARENT,
        SessionEvent::InputRecorded {
            input: vec![text("earlier request")],
        },
    )
    .await;
    append(
        log,
        PARENT,
        assistant_at(
            STEP_1,
            vec![
                reasoning("p"),
                Item::Message {
                    role: Role::Assistant,
                    parts: vec![text("earlier answer")],
                },
                Item::ToolCall {
                    call_id: "call-A".to_owned(),
                    provider_item_id: Some("item-A".to_owned()),
                    name: ToolName::new("echo"),
                    raw_args: asterpath_protocol::JsonString::new("{}"),
                    raw: Some(raw("p", "tool_use")),
                },
            ],
        ),
    )
    .await;
    append(
        log,
        PARENT,
        SessionEvent::ToolCallRequested {
            turn_id: FORK_TURN,
            step_id: STEP_1,
            tool_call_id: CALL_A,
            provider_call_id: "call-A".to_owned(),
            name: ToolName::new("echo"),
            raw_args: asterpath_protocol::JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
    )
    .await;
    append(
        log,
        PARENT,
        SessionEvent::ToolResult {
            tool_call_id: CALL_A,
            status: asterpath_protocol::ToolResultStatus::Completed,
            output: Some(vec![text("tool output")]),
            error: None,
            truncated: false,
        },
    )
    .await;
    append(
        log,
        PARENT,
        SessionEvent::StepCommitted {
            turn_id: FORK_TURN,
            step_id: STEP_1,
        },
    )
    .await;
    let at = append(
        log,
        PARENT,
        SessionEvent::StepStarted {
            turn_id: FORK_TURN,
            step_id: STEP_2,
        },
    )
    .await;
    append(
        log,
        PARENT,
        assistant_at(
            STEP_2,
            vec![
                Item::Message {
                    role: Role::Assistant,
                    parts: vec![text("spawn now")],
                },
                Item::ToolCall {
                    call_id: "spawn-call".to_owned(),
                    provider_item_id: None,
                    name: ToolName::new("spawn_agent"),
                    raw_args: asterpath_protocol::JsonString::new("{}"),
                    raw: None,
                },
                Item::ToolCall {
                    call_id: "sib".to_owned(),
                    provider_item_id: None,
                    name: ToolName::new("echo"),
                    raw_args: asterpath_protocol::JsonString::new("{}"),
                    raw: None,
                },
            ],
        ),
    )
    .await;
    let caused_by = append(
        log,
        PARENT,
        SessionEvent::ToolCallRequested {
            turn_id: FORK_TURN,
            step_id: STEP_2,
            tool_call_id: CALL_SPAWN,
            provider_call_id: "spawn-call".to_owned(),
            name: ToolName::new("spawn_agent"),
            raw_args: asterpath_protocol::JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
    )
    .await;
    append(
        log,
        PARENT,
        SessionEvent::ToolCallRequested {
            turn_id: FORK_TURN,
            step_id: STEP_2,
            tool_call_id: CALL_SIBLING,
            provider_call_id: "sib".to_owned(),
            name: ToolName::new("echo"),
            raw_args: asterpath_protocol::JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
    )
    .await;
    append(
        log,
        PARENT,
        SessionEvent::ToolResult {
            tool_call_id: CALL_SIBLING,
            status: asterpath_protocol::ToolResultStatus::Completed,
            output: Some(vec![text("sibling result")]),
            error: None,
            truncated: false,
        },
    )
    .await;
    (caused_by, at)
}

fn fork_request(caused_by: LogId, at: LogId) -> SpawnRequest {
    let mut request = SpawnRequest::new(fork_parent_ref(), caused_by, vec![text("fork task")]);
    request.context = ContextPolicy::Fork { at };
    request
}

fn fork_event(events: &[SessionEvent]) -> &SessionEvent {
    events
        .iter()
        .find(|event| matches!(event, SessionEvent::ContextForked { .. }))
        .expect("ContextForked record")
}

#[tokio::test]
async fn fork_materializes_the_parent_projection_at_the_step_watermark() {
    let log = Arc::new(InMemoryEventLog::new());
    let (caused_by, at) = seed_fork_parent(&*log, &SubagentConfig::default()).await;
    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );

    let child = scheduler
        .spawn(fork_request(caused_by, at))
        .await
        .expect("fork spawn");

    let child_events = events(&*log, child.child).await;
    assert_eq!(
        child_events
            .iter()
            .map(SessionEvent::kind)
            .collect::<Vec<_>>(),
        ["SessionCreated", "ContextForked", "InputRecorded"]
    );
    let SessionEvent::ContextForked {
        source,
        source_log,
        items,
    } = fork_event(&child_events)
    else {
        unreachable!()
    };
    assert_eq!(source, &fork_parent_ref());
    assert_eq!(*source_log, at);
    assert_eq!(
        items,
        &vec![
            Item::Message {
                role: Role::User,
                parts: vec![text("earlier request")],
            },
            reasoning("p"),
            Item::Message {
                role: Role::Assistant,
                parts: vec![text("earlier answer")],
            },
            Item::ToolCall {
                call_id: "call-A".to_owned(),
                provider_item_id: Some("item-A".to_owned()),
                name: ToolName::new("echo"),
                raw_args: asterpath_protocol::JsonString::new("{}"),
                raw: Some(raw("p", "tool_use")),
            },
            Item::ToolResult {
                call_id: "call-A".to_owned(),
                provider_item_id: None,
                parts: vec![text("tool output")],
                is_error: false,
                raw: None,
            },
        ],
        "the snapshot stops at the spawn step's StepStarted"
    );
    assert!(
        !items.iter().any(|item| matches!(item, Item::Message { parts, .. }
            if parts.iter().any(|part| matches!(part, Part::Text { text } if text == "spawn now")))),
        "the spawn AssistantMessage is not part of the snapshot"
    );
    assert!(
        !items.iter().any(|item| matches!(item, Item::ToolResult { parts, .. }
            if parts.iter().any(|part| matches!(part, Part::Text { text } if text == "sibling result")))),
        "sibling tool results from the same step are not part of the snapshot"
    );
    let SessionEvent::InputRecorded { input } = &child_events[2] else {
        unreachable!()
    };
    assert_eq!(input, &vec![text("fork task")]);
}

#[tokio::test]
async fn fork_snapshot_respects_compaction_and_is_independent_of_later_changes() {
    let log = Arc::new(InMemoryEventLog::new());
    append(
        &*log,
        PARENT,
        SessionEvent::SessionCreated {
            config: config(3),
            parent: None,
        },
    )
    .await;
    let turn = append(
        &*log,
        PARENT,
        SessionEvent::TurnStarted { turn_id: FORK_TURN },
    )
    .await;
    append(
        &*log,
        PARENT,
        SessionEvent::StepStarted {
            turn_id: FORK_TURN,
            step_id: STEP_1,
        },
    )
    .await;
    append(
        &*log,
        PARENT,
        SessionEvent::InputRecorded {
            input: vec![text("old user")],
        },
    )
    .await;
    let old_answer = append(
        &*log,
        PARENT,
        assistant_at(
            STEP_1,
            vec![Item::Message {
                role: Role::Assistant,
                parts: vec![text("old answer")],
            }],
        ),
    )
    .await;
    append(
        &*log,
        PARENT,
        SessionEvent::StepCommitted {
            turn_id: FORK_TURN,
            step_id: STEP_1,
        },
    )
    .await;
    // The spawn step starts, then a forced in-step compaction rewrites the
    // prefix. `CompactionApplied` folds regardless of the waterline.
    let at = append(
        &*log,
        PARENT,
        SessionEvent::StepStarted {
            turn_id: FORK_TURN,
            step_id: STEP_2,
        },
    )
    .await;
    let key = dedup::compaction(at);
    append_keyed(
        &*log,
        PARENT,
        SessionEvent::CompactionStarted {
            trigger: asterpath_protocol::CompactionTrigger::ProviderError,
            replaces_from: turn,
            caused_by: at,
        },
        key.clone(),
    )
    .await;
    append_keyed(
        &*log,
        PARENT,
        SessionEvent::CompactionApplied {
            first_kept: at,
            summary: vec![text("summary")],
            replaces: asterpath_core::LogIdRange {
                first: turn,
                last: old_answer,
            },
            model: config(3).model,
            usage: Usage::default(),
            raw: None,
        },
        key,
    )
    .await;
    append(
        &*log,
        PARENT,
        assistant_at(
            STEP_2,
            vec![Item::ToolCall {
                call_id: "spawn-call".to_owned(),
                provider_item_id: None,
                name: ToolName::new("spawn_agent"),
                raw_args: asterpath_protocol::JsonString::new("{}"),
                raw: None,
            }],
        ),
    )
    .await;
    let caused_by = append(
        &*log,
        PARENT,
        SessionEvent::ToolCallRequested {
            turn_id: FORK_TURN,
            step_id: STEP_2,
            tool_call_id: CALL_SPAWN,
            provider_call_id: "spawn-call".to_owned(),
            name: ToolName::new("spawn_agent"),
            raw_args: asterpath_protocol::JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
    )
    .await;

    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let request = fork_request(caused_by, at);
    let child = scheduler.spawn(request.clone()).await.expect("fork spawn");

    let snapshot = match fork_event(&events(&*log, child.child).await) {
        SessionEvent::ContextForked { items, .. } => items.clone(),
        _ => unreachable!(),
    };
    assert_eq!(
        snapshot,
        vec![Item::Compaction {
            parts: vec![text("summary")]
        }],
        "the snapshot is the projection after compaction, not the raw records"
    );

    // Parent compaction/extension after the spawn cannot rewrite the child.
    let later = append(
        &*log,
        PARENT,
        SessionEvent::InputRecorded {
            input: vec![text("parent-only later input")],
        },
    )
    .await;
    append_keyed(
        &*log,
        PARENT,
        SessionEvent::CompactionApplied {
            first_kept: later,
            summary: vec![text("later summary")],
            replaces: asterpath_core::LogIdRange {
                first: turn,
                last: later,
            },
            model: config(3).model,
            usage: Usage::default(),
            raw: None,
        },
        dedup::compaction(later),
    )
    .await;

    let again = scheduler.spawn(request).await.expect("replay spawn");
    assert_eq!(again.child, child.child);
    let child_events = events(&*log, again.child).await;
    assert_eq!(
        child_events
            .iter()
            .filter(|event| matches!(event, SessionEvent::ContextForked { .. }))
            .count(),
        1,
        "replay never duplicates ContextForked"
    );
    assert_eq!(
        match fork_event(&child_events) {
            SessionEvent::ContextForked { items, .. } => items,
            _ => unreachable!(),
        },
        &snapshot,
        "the materialized snapshot is independent of later parent changes"
    );
}

#[tokio::test]
async fn fork_drops_foreign_provider_opaque_items_without_translation() {
    let log = Arc::new(InMemoryEventLog::new());
    append(
        &*log,
        PARENT,
        SessionEvent::SessionCreated {
            config: config(3),
            parent: None,
        },
    )
    .await;
    append(
        &*log,
        PARENT,
        SessionEvent::TurnStarted { turn_id: FORK_TURN },
    )
    .await;
    append(
        &*log,
        PARENT,
        SessionEvent::StepStarted {
            turn_id: FORK_TURN,
            step_id: STEP_1,
        },
    )
    .await;
    append(
        &*log,
        PARENT,
        assistant_at(
            STEP_1,
            vec![
                reasoning("q"),
                Item::ProviderOpaque {
                    provider: ProviderId::new("q"),
                    kind: "unknown".to_owned(),
                    raw: raw("q", "unknown"),
                },
                Item::ServerToolCall {
                    provider: ProviderId::new("q"),
                    name: "web".to_owned(),
                    call_id: None,
                    raw: raw("q", "server_tool"),
                },
                Item::Message {
                    role: Role::Assistant,
                    parts: vec![
                        text("kept text"),
                        Part::ProviderOpaque {
                            provider: ProviderId::new("q"),
                            kind: "opaque".to_owned(),
                            raw: raw("q", "opaque"),
                        },
                        Part::ProviderOpaque {
                            provider: ProviderId::new("p"),
                            kind: "opaque".to_owned(),
                            raw: raw("p", "opaque"),
                        },
                    ],
                },
                reasoning("p"),
                Item::ProviderOpaque {
                    provider: ProviderId::new("p"),
                    kind: "known".to_owned(),
                    raw: raw("p", "known"),
                },
            ],
        ),
    )
    .await;
    let at = append(
        &*log,
        PARENT,
        SessionEvent::StepStarted {
            turn_id: FORK_TURN,
            step_id: STEP_2,
        },
    )
    .await;
    let caused_by = append(
        &*log,
        PARENT,
        SessionEvent::ToolCallRequested {
            turn_id: FORK_TURN,
            step_id: STEP_2,
            tool_call_id: CALL_SPAWN,
            provider_call_id: "spawn-call".to_owned(),
            name: ToolName::new("spawn_agent"),
            raw_args: asterpath_protocol::JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
    )
    .await;

    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let child = scheduler
        .spawn(fork_request(caused_by, at))
        .await
        .expect("fork spawn");

    let child_events = events(&*log, child.child).await;
    let SessionEvent::ContextForked { items, .. } = fork_event(&child_events) else {
        unreachable!()
    };
    assert_eq!(
        items,
        &vec![
            Item::Message {
                role: Role::Assistant,
                parts: vec![
                    text("kept text"),
                    Part::ProviderOpaque {
                        provider: ProviderId::new("p"),
                        kind: "opaque".to_owned(),
                        raw: raw("p", "opaque"),
                    },
                ],
            },
            reasoning("p"),
            Item::ProviderOpaque {
                provider: ProviderId::new("p"),
                kind: "known".to_owned(),
                raw: raw("p", "known"),
            },
        ],
        "foreign opaque items are dropped verbatim; same-provider raw is kept"
    );
}

#[tokio::test]
async fn spawn_after_child_spawn_record_fills_the_fork_stream_idempotently() {
    let log = Arc::new(InMemoryEventLog::new());
    let (caused_by, at) = seed_fork_parent(&*log, &SubagentConfig::default()).await;
    let child = SessionId::from_bytes([0x77; 16]);
    append_keyed(
        &*log,
        PARENT,
        spawn_record(child, ContextPolicy::Fork { at }),
        dedup::child_session(child),
    )
    .await;

    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let request = fork_request(caused_by, at);

    // Window: ChildSessionSpawned committed, child stream still empty.
    let spawned = scheduler.spawn(request.clone()).await.expect("R2 fill");
    assert_eq!(spawned.child, child);
    assert_eq!(
        events(&*log, child)
            .await
            .iter()
            .map(SessionEvent::kind)
            .collect::<Vec<_>>(),
        ["SessionCreated", "ContextForked", "InputRecorded"]
    );

    // Window: SessionCreated committed, fork/input records missing.
    let second = SessionId::from_bytes([0x78; 16]);
    let second_call = ToolCallId::from_bytes([0x23; 16]);
    append_keyed(
        &*log,
        PARENT,
        SessionEvent::ChildSessionSpawned {
            child: second,
            task: vec![text("fork task")],
            config: config(3),
            context: ContextPolicy::Fork { at },
            parent_tool_call_id: second_call,
            caused_by: LogId::from_i64(1),
            deadline: None,
        },
        dedup::child_session(second),
    )
    .await;
    append(
        &*log,
        second,
        SessionEvent::SessionCreated {
            config: config(3),
            parent: Some(fork_parent_ref()),
        },
    )
    .await;
    let mut second_request = fork_request(caused_by, at);
    second_request.parent.tool_call = second_call;
    let filled = scheduler.spawn(second_request).await.expect("R2 fill");
    assert_eq!(filled.child, second);
    assert_eq!(
        events(&*log, second)
            .await
            .iter()
            .map(SessionEvent::kind)
            .collect::<Vec<_>>(),
        ["SessionCreated", "ContextForked", "InputRecorded"]
    );

    // A replay writes nothing twice.
    scheduler.spawn(request).await.expect("replay");
    assert_eq!(
        events(&*log, child)
            .await
            .iter()
            .filter(|event| matches!(event, SessionEvent::ContextForked { .. }))
            .count(),
        1
    );
}
// --- #48: admission, recovery, cancellation and timeout -------------------

fn config_with(max_depth: u8, subagents: SubagentConfig) -> ConfigSnapshot {
    let mut config = config(max_depth);
    config.subagents = subagents;
    config
}

fn limit(per_parent: usize) -> SubagentConfig {
    SubagentConfig {
        max_concurrent_children: NonZeroUsize::new(per_parent).expect("non-zero"),
        cancel_grace: Duration::ZERO,
        ..SubagentConfig::default()
    }
}

async fn seed_parent_with(
    log: &InMemoryEventLog,
    subagents: &SubagentConfig,
    calls: &[ToolCallId],
) -> (asterpath_protocol::ParentRef, Vec<LogId>) {
    let parent = SessionId::from_bytes([1; 16]);
    append(
        log,
        parent,
        SessionEvent::SessionCreated {
            config: config_with(3, subagents.clone()),
            parent: None,
        },
    )
    .await;
    let reference = asterpath_protocol::ParentRef {
        session: parent,
        turn: TurnId::from_bytes([2; 16]),
        tool_call: calls[0],
    };
    let mut caused_by = Vec::new();
    for (index, call) in calls.iter().enumerate() {
        caused_by.push(
            append(
                log,
                parent,
                SessionEvent::ToolCallRequested {
                    turn_id: reference.turn,
                    step_id: asterpath_core::StepId::from_bytes([4; 16]),
                    tool_call_id: *call,
                    provider_call_id: format!("call-{index}"),
                    name: ToolName::new("spawn_agent"),
                    raw_args: asterpath_protocol::JsonString::new("{}"),
                    replay: ReplaySafety::Safe,
                    provenance: Provenance::InProcess,
                },
            )
            .await,
        );
    }
    (reference, caused_by)
}

fn request_for(
    reference: &asterpath_protocol::ParentRef,
    call: ToolCallId,
    caused_by: LogId,
) -> SpawnRequest {
    let mut parent = reference.clone();
    parent.tool_call = call;
    SpawnRequest::new(parent, caused_by, vec![text("task")])
}

async fn spawned_calls(log: &dyn EventLog, parent: SessionId) -> Vec<ToolCallId> {
    events(log, parent)
        .await
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

#[tokio::test]
async fn admission_queues_over_the_limit_in_tool_call_seq_order() {
    let log = Arc::new(InMemoryEventLog::new());
    let calls = [
        ToolCallId::from_bytes([0x31; 16]),
        ToolCallId::from_bytes([0x32; 16]),
        ToolCallId::from_bytes([0x33; 16]),
    ];
    let subagents = limit(1);
    let (reference, caused_by) = seed_parent_with(&log, &subagents, &calls).await;
    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        Arc::new(SystemClock),
        subagents.clone(),
    );

    // The first call takes the only slot.
    let first = scheduler
        .spawn(request_for(&reference, calls[0], caused_by[0]))
        .await
        .expect("first admitted");
    assert_eq!(
        spawned_calls(&*log, reference.session).await,
        vec![calls[0]]
    );

    // Enqueue the higher seq first; the lower seq must still go first.
    let queued_high = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        let request = request_for(&reference, calls[2], caused_by[2]);
        async move { scheduler.spawn(request).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let queued_low = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        let request = request_for(&reference, calls[1], caused_by[1]);
        async move { scheduler.spawn(request).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        spawned_calls(&*log, reference.session).await,
        vec![calls[0]],
        "queued spawns are open ToolCallRequested only"
    );

    // Releasing the slot admits the lower seq next, not the earlier arrival.
    scheduler.cancel(&first).await.expect("release slot");
    let second = tokio::time::timeout(Duration::from_secs(5), queued_low)
        .await
        .expect("admitted")
        .expect("task")
        .expect("spawn");
    assert_eq!(second.parent.tool_call, calls[1]);
    assert_eq!(
        spawned_calls(&*log, reference.session).await,
        vec![calls[0], calls[1]]
    );

    scheduler.cancel(&second).await.expect("release slot");
    let third = tokio::time::timeout(Duration::from_secs(5), queued_high)
        .await
        .expect("admitted")
        .expect("task")
        .expect("spawn");
    assert_eq!(third.parent.tool_call, calls[2]);
    assert_eq!(
        spawned_calls(&*log, reference.session).await,
        vec![calls[0], calls[1], calls[2]]
    );
}

#[tokio::test]
async fn a_spawn_cancelled_before_admission_does_not_block_a_later_spawn() {
    let log = Arc::new(InMemoryEventLog::new());
    let calls = [
        ToolCallId::from_bytes([0x61; 16]),
        ToolCallId::from_bytes([0x62; 16]),
    ];
    let subagents = limit(1);
    let (reference, caused_by) = seed_parent_with(&log, &subagents, &calls).await;
    // The first spawn was cancelled while queued: an intent and an
    // `Interrupted` result, but no child and no `ChildSessionSpawned`.
    append(
        &*log,
        reference.session,
        SessionEvent::ToolResult {
            tool_call_id: calls[0],
            status: ToolResultStatus::Interrupted,
            output: None,
            error: None,
            truncated: false,
        },
    )
    .await;

    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        Arc::new(SystemClock),
        subagents.clone(),
    );
    let child = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.spawn(request_for(&reference, calls[1], caused_by[1])),
    )
    .await
    .expect("a closed queue entry must not block the next spawn")
    .expect("spawn admitted");
    assert_eq!(child.parent.tool_call, calls[1]);
    assert_eq!(
        spawned_calls(&*log, reference.session).await,
        vec![calls[1]],
        "the cancelled call creates no child"
    );
}

#[tokio::test]
async fn recovery_counts_admitted_children_and_requeues_only_open_spawns() {
    let log = Arc::new(InMemoryEventLog::new());
    let calls = [
        ToolCallId::from_bytes([0x41; 16]),
        ToolCallId::from_bytes([0x42; 16]),
        ToolCallId::from_bytes([0x43; 16]),
    ];
    let subagents = limit(1);
    let (reference, caused_by) = seed_parent_with(&log, &subagents, &calls).await;
    // call A was admitted before the crash; calls B and C stayed open.
    append(
        &*log,
        reference.session,
        SessionEvent::ChildSessionSpawned {
            child: SessionId::from_bytes([0x50; 16]),
            task: vec![text("task")],
            config: config_with(3, subagents.clone()),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: calls[0],
            caused_by: caused_by[0],
            deadline: None,
        },
    )
    .await;

    // A new process: empty in-memory admission state over the same log.
    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        Arc::new(SystemClock),
        subagents.clone(),
    );

    // R1: replay reuses the admitted child and writes no second record.
    let admitted = scheduler
        .spawn(request_for(&reference, calls[0], caused_by[0]))
        .await
        .expect("replay");
    assert_eq!(admitted.child, SessionId::from_bytes([0x50; 16]));

    let queued_b = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        let request = request_for(&reference, calls[1], caused_by[1]);
        async move { scheduler.spawn(request).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let queued_c = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        let request = request_for(&reference, calls[2], caused_by[2]);
        async move { scheduler.spawn(request).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        spawned_calls(&*log, reference.session).await,
        vec![calls[0]],
        "the admitted child counts as Running and open spawns wait"
    );

    scheduler.cancel(&admitted).await.expect("release slot");
    let b = tokio::time::timeout(Duration::from_secs(5), queued_b)
        .await
        .expect("admitted")
        .expect("task")
        .expect("spawn");
    assert_eq!(b.parent.tool_call, calls[1]);
    scheduler.cancel(&b).await.expect("release slot");
    let c = tokio::time::timeout(Duration::from_secs(5), queued_c)
        .await
        .expect("admitted")
        .expect("task")
        .expect("spawn");
    assert_eq!(c.parent.tool_call, calls[2]);

    assert_eq!(
        spawned_calls(&*log, reference.session).await,
        vec![calls[0], calls[1], calls[2]],
        "no admitted child is recreated"
    );
}

#[tokio::test]
async fn cancel_after_a_child_terminal_still_records_cancelled_parent() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config(3),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: reference.tool_call,
            caused_by,
            deadline: None,
        },
    )
    .await;
    // The child committed its own terminal, but the parent did not record the
    // outcome before entering cancelling.
    completed_child(&log, child, "finished", TurnEndReason::Completed).await;

    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    let child_ref = ChildRef {
        parent: reference,
        child,
    };
    scheduler.cancel(&child_ref).await.expect("cancel");

    assert_eq!(
        events(&*log, parent)
            .await
            .into_iter()
            .find_map(|event| match event {
                SessionEvent::ChildSessionCompleted { outcome, .. } => Some(outcome),
                _ => None,
            }),
        Some(ChildOutcome::Cancelled {
            reason: CancelReason::Parent
        })
    );
}

#[tokio::test]
async fn spawn_uses_the_parent_snapshot_fresh_default_context() {
    let log = Arc::new(InMemoryEventLog::new());
    let subagents = SubagentConfig {
        default_context: ContextPolicy::Fresh,
        ..SubagentConfig::default()
    };
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    // The spawn tool leaves the request at its `Fresh` default.
    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        Arc::new(SystemClock),
        subagents,
    );
    let child = scheduler
        .spawn(SpawnRequest::new(reference, caused_by, vec![text("task")]))
        .await
        .expect("spawn");
    let context = events(&*log, parent)
        .await
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { context, .. } => Some(context),
            _ => None,
        })
        .expect("spawn record");
    assert_eq!(context, ContextPolicy::Fresh);
    assert!(
        !events(&*log, child.child)
            .await
            .iter()
            .any(|event| matches!(event, SessionEvent::ContextForked { .. })),
        "a fresh child never forks the parent"
    );
}

#[tokio::test]
async fn spawn_honours_a_fork_default_context_at_the_step_watermark() {
    let log = Arc::new(InMemoryEventLog::new());
    let subagents = SubagentConfig {
        // The watermark in the default is a placeholder; the spawn step's
        // `StepStarted` is authoritative.
        default_context: ContextPolicy::Fork {
            at: LogId::from_i64(0),
        },
        ..SubagentConfig::default()
    };
    let (caused_by, at) = seed_fork_parent(&*log, &subagents).await;
    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        Arc::new(SystemClock),
        subagents,
    );
    // Same request the spawn tool builds: `Fresh` unless the snapshot says otherwise.
    let child = scheduler
        .spawn(SpawnRequest::new(
            fork_parent_ref(),
            caused_by,
            vec![text("fork task")],
        ))
        .await
        .expect("fork spawn");

    let context = events(&*log, PARENT)
        .await
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { context, .. } => Some(context),
            _ => None,
        })
        .expect("spawn record");
    assert_eq!(context, ContextPolicy::Fork { at });

    let child_events = events(&*log, child.child).await;
    let SessionEvent::ContextForked { source_log, .. } = fork_event(&child_events) else {
        unreachable!()
    };
    assert_eq!(*source_log, at, "the snapshot is taken at the spawn step");
}

/// Appends the child's terminal when driven, as a real agent would when it
/// finishes inside the cancellation grace window.
struct CompletingRunner {
    log: Arc<InMemoryEventLog>,
}

impl AgentRunner for CompletingRunner {
    fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>> {
        let log = Arc::clone(&self.log);
        let session = req.session;
        let turn = TurnId::from_bytes([5; 16]);
        stream::once(async move {
            append(
                &*log,
                session,
                SessionEvent::TurnCommitted {
                    turn_id: turn,
                    reason: TurnEndReason::Completed,
                },
            )
            .await;
            Ok(AgentEvent::TurnCommitted {
                turn,
                reason: TurnEndReason::Completed,
            })
        })
        .boxed()
    }

    fn compact_now(
        &self,
        session: SessionId,
    ) -> BoxFuture<'_, Result<asterpath_protocol::CompactResult, AgentError>> {
        Box::pin(async move { Err(AgentError::NotFound { session }) })
    }
}

#[tokio::test]
async fn a_child_terminal_landing_in_the_timeout_grace_wins() {
    let log = Arc::new(InMemoryEventLog::new());
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
    let subagents = SubagentConfig {
        cancel_grace: Duration::from_millis(50),
        ..SubagentConfig::default()
    };
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        SessionEvent::ChildSessionSpawned {
            child,
            task: vec![text("task")],
            config: config_with(3, subagents.clone()),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: reference.tool_call,
            caused_by,
            deadline: Some(clock.now() - 1),
        },
    )
    .await;

    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        clock,
        subagents,
    );
    let runner: Arc<dyn AgentRunner> = Arc::new(CompletingRunner {
        log: Arc::clone(&log),
    });
    scheduler.set_runner(Arc::clone(&runner));
    let outcome = scheduler
        .await_completion(&ChildRef {
            parent: reference,
            child,
        })
        .await
        .expect("the terminal written during grace wins over the timeout");
    assert!(matches!(outcome, ChildOutcome::Completed { .. }));
    assert_eq!(
        events(&*log, parent)
            .await
            .into_iter()
            .filter_map(|event| match event {
                SessionEvent::ChildSessionCompleted { outcome, .. } => Some(outcome),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![ChildOutcome::Completed {
            summary: Summary { items: Vec::new() }
        }]
    );
}

#[tokio::test]
async fn a_committed_child_terminal_wins_over_an_expired_spawn_deadline() {
    let log = Arc::new(InMemoryEventLog::new());
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        spawn_with_deadline(child, reference.tool_call, caused_by, clock.now() - 1),
    )
    .await;
    // The child finished before the parent noticed the deadline.
    completed_child(&log, child, "finished", TurnEndReason::Completed).await;

    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        clock,
        SubagentConfig::default(),
    );
    let outcome = scheduler
        .await_completion(&ChildRef {
            parent: reference,
            child,
        })
        .await
        .expect("terminal wins");
    assert!(matches!(outcome, ChildOutcome::Completed { .. }));
    assert_eq!(completions(&*log, parent).await.len(), 1);
}

fn spawn_with_deadline(
    child: SessionId,
    tool_call: ToolCallId,
    caused_by: LogId,
    deadline: Timestamp,
) -> SessionEvent {
    SessionEvent::ChildSessionSpawned {
        child,
        task: vec![text("task")],
        config: config_with(
            3,
            SubagentConfig {
                cancel_grace: Duration::from_millis(20),
                ..SubagentConfig::default()
            },
        ),
        context: ContextPolicy::Fresh,
        parent_tool_call_id: tool_call,
        caused_by,
        deadline: Some(deadline),
    }
}

async fn completions(log: &dyn EventLog, parent: SessionId) -> Vec<ChildOutcome> {
    events(log, parent)
        .await
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ChildSessionCompleted { outcome, .. } => Some(outcome),
            _ => None,
        })
        .collect()
}

/// A child that never reaches a terminal, however long it is driven.
struct HangingRunner;

impl AgentRunner for HangingRunner {
    fn run(&self, _req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>> {
        stream::pending().boxed()
    }

    fn compact_now(
        &self,
        session: SessionId,
    ) -> BoxFuture<'_, Result<asterpath_protocol::CompactResult, AgentError>> {
        Box::pin(async move { Err(AgentError::NotFound { session }) })
    }
}

#[tokio::test]
async fn spawn_records_the_timeout_as_a_deadline_on_the_spawn_record() {
    let log = Arc::new(InMemoryEventLog::new());
    let clock = Arc::new(ManualClock::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        clock.clone(),
        SubagentConfig::default(),
    );
    let mut request = SpawnRequest::new(reference, caused_by, vec![text("task")]);
    request.timeout = Some(Duration::from_secs(5));
    scheduler.spawn(request.clone()).await.expect("spawn");
    // A replayed spawn after the clock moved keeps the original deadline.
    clock.advance(60_000);
    scheduler.spawn(request).await.expect("replayed spawn");

    let deadlines = events(&*log, parent)
        .await
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ChildSessionSpawned { deadline, .. } => Some(deadline),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(deadlines, vec![Some(ManualClock::BASE + 5_000)]);
    assert_eq!(
        kinds(&*log, parent).await,
        vec!["SessionCreated", "ToolCallRequested", "ChildSessionSpawned"],
        "the deadline is the only timeout record"
    );
}

#[tokio::test]
async fn a_spawn_without_a_timeout_has_no_deadline() {
    let log = Arc::new(InMemoryEventLog::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let scheduler = new_scheduler(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        BTreeMap::new(),
    );
    scheduler
        .spawn(SpawnRequest::new(reference, caused_by, vec![text("task")]))
        .await
        .expect("spawn");
    assert!(events(&*log, parent).await.iter().any(|event| matches!(
        event,
        SessionEvent::ChildSessionSpawned { deadline: None, .. }
    )));
}

#[tokio::test]
async fn a_spawn_deadline_that_expired_while_stopped_times_out_exactly_once_after_restart() {
    let log = Arc::new(InMemoryEventLog::new());
    let clock = Arc::new(ManualClock::new());
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    // Spawned before the stop; the deadline passed while nothing ran.
    append(
        &*log,
        parent,
        spawn_with_deadline(
            child,
            reference.tool_call,
            caused_by,
            ManualClock::BASE + 1_000,
        ),
    )
    .await;
    clock.advance(2_000);
    let child_ref = ChildRef {
        parent: reference,
        child,
    };

    for _restart in 0..2 {
        let scheduler = new_scheduler_with(
            log.clone(),
            Arc::new(FakeCatalog::default()),
            Arc::new(NoopExecutor),
            clock.clone(),
            SubagentConfig::default(),
        );
        let runner: Arc<dyn AgentRunner> = Arc::new(HangingRunner);
        scheduler.set_runner(Arc::clone(&runner));
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            scheduler.await_completion(&child_ref),
        )
        .await
        .expect("an expired deadline never waits");
        assert!(matches!(result, Err(WaitError::Timeout)), "{result:?}");
    }

    assert_eq!(
        completions(&*log, parent).await,
        vec![ChildOutcome::Cancelled {
            reason: CancelReason::Timeout
        }]
    );
}

#[tokio::test]
async fn a_live_wait_sleeps_until_the_spawn_deadline() {
    let log = Arc::new(InMemoryEventLog::new());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let (parent, reference, caused_by) = seed_parent(&log, 3).await;
    let child = SessionId::from_bytes([9; 16]);
    append(
        &*log,
        parent,
        spawn_with_deadline(child, reference.tool_call, caused_by, clock.now() + 50),
    )
    .await;
    let scheduler = new_scheduler_with(
        log.clone(),
        Arc::new(FakeCatalog::default()),
        Arc::new(NoopExecutor),
        clock,
        SubagentConfig::default(),
    );
    let runner: Arc<dyn AgentRunner> = Arc::new(HangingRunner);
    scheduler.set_runner(Arc::clone(&runner));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.await_completion(&ChildRef {
            parent: reference,
            child,
        }),
    )
    .await
    .expect("the wait ends at the deadline");
    assert!(matches!(result, Err(WaitError::Timeout)), "{result:?}");
    assert_eq!(
        completions(&*log, parent).await,
        vec![ChildOutcome::Cancelled {
            reason: CancelReason::Timeout
        }]
    );
}
