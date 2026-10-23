//! Usage query scenarios U1–U16: buckets, scopes, tree rollup, filtering,
//! corruption and replay-derived totals.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, PoisonError};

use asterpath_core::{
    DedupKey, EventLog, LogEntry, LogId, LogQuery, NewEntry, SessionCatalog, SessionId, StepId,
    StorageError, Timestamp, TurnId,
};
use asterpath_protocol::{
    CompactionTrigger, InputTokens, Item, ModelId, ModelSpec, OutputTokens, Part, ProviderId, Role,
    SessionEvent, StopReason, Usage, UsageBucket, UsageError, UsageFilter, UsageQuery, UsageScope,
    dedup, encode_entry,
};
use asterpath_usage::UsageService;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

const ROOT: SessionId = SessionId::from_bytes([1; 16]);
const CHILD_A: SessionId = SessionId::from_bytes([2; 16]);
const CHILD_B: SessionId = SessionId::from_bytes([3; 16]);
const RETIRED: SessionId = SessionId::from_bytes([4; 16]);
const TURN: TurnId = TurnId::from_bytes([5; 16]);
const STEP_A: StepId = StepId::from_bytes([6; 16]);
const STEP_B: StepId = StepId::from_bytes([7; 16]);

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn assistant_items() -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text("hi")],
    }]
}

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached: input,
            cache_read: 0,
            cache_write: 0,
        },
        output: OutputTokens {
            total: output,
            reasoning: 0,
        },
    }
}

fn model(name: &str) -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new(name),
        context_window: None,
    }
}

fn assistant(model_name: &str, turn: TurnId, step: StepId, usage: Usage) -> SessionEvent {
    SessionEvent::AssistantMessage {
        turn_id: turn,
        step_id: step,
        items: assistant_items(),
        raw: None,
        model: model(model_name),
        usage,
        stop_reason: StopReason::EndTurn,
    }
}

fn aborted(model_name: &str, turn: TurnId, step: StepId, usage: Option<Usage>) -> SessionEvent {
    SessionEvent::ModelCallAborted {
        turn_id: turn,
        step_id: step,
        attempt: 1,
        kind: asterpath_protocol::AbortKind::Transport,
        partial: None,
        model: model(model_name),
        backoff_ms: 0,
        usage,
    }
}

fn started(step: StepId) -> SessionEvent {
    SessionEvent::StepStarted {
        turn_id: TURN,
        step_id: step,
    }
}

fn committed(step: StepId) -> SessionEvent {
    SessionEvent::StepCommitted {
        turn_id: TURN,
        step_id: step,
    }
}

#[derive(Default)]
struct TestLog {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    entries: Vec<LogEntry>,
    next: i64,
    seq: HashMap<SessionId, u64>,
}

impl TestLog {
    fn push(&self, session: SessionId, event: &SessionEvent, key: Option<DedupKey>) -> LogId {
        let at = self.next_time();
        self.push_at(session, event, key, at)
    }

    fn push_at(
        &self,
        session: SessionId,
        event: &SessionEvent,
        key: Option<DedupKey>,
        at: Timestamp,
    ) -> LogId {
        let entry = encode_entry(session, key, event).expect("encode");
        self.insert(entry, at)
    }

    fn push_corrupt(&self, session: SessionId, at: Timestamp) -> LogId {
        self.insert(
            NewEntry {
                session,
                schema_version: asterpath_protocol::LOG_SCHEMA_VERSION,
                dedup_key: None,
                payload: serde_json::json!({"NotAnEvent": {}}),
            },
            at,
        )
    }

    fn insert(&self, entry: NewEntry, at: Timestamp) -> LogId {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.next += 1;
        let log_id = LogId::from_i64(inner.next);
        let seq = inner.seq.entry(entry.session).or_insert(0);
        *seq += 1;
        let seq = *seq;
        inner.entries.push(LogEntry {
            log_id,
            session: entry.session,
            seq,
            recorded_at: at,
            schema_version: entry.schema_version,
            dedup_key: entry.dedup_key,
            payload: entry.payload,
        });
        log_id
    }

    fn next_time(&self) -> Timestamp {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.next * 10
    }
}

impl EventLog for TestLog {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        let at = self.next_time();
        let log_id = self.insert(entry, at);
        Box::pin(async move { Ok(log_id) })
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        let mut entries: Vec<_> = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .iter()
            .filter(|entry| entry.session == query.session)
            .filter(|entry| query.after.is_none_or(|after| entry.log_id > after))
            .cloned()
            .collect();
        entries.sort_by_key(|entry| entry.seq);
        if let Some(limit) = query.limit {
            entries.truncate(limit);
        }
        futures_util::stream::iter(entries.into_iter().map(Ok)).boxed()
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        let frontier = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .iter()
            .filter(|entry| entry.session == session)
            .map(|entry| entry.log_id)
            .max();
        Box::pin(async move { Ok(frontier) })
    }
}

#[derive(Default)]
struct TreeCatalog {
    children: HashMap<SessionId, Vec<SessionId>>,
}

impl TreeCatalog {
    fn parent_of(children: &[SessionId]) -> Self {
        let mut catalog = Self::default();
        catalog.children.insert(ROOT, children.to_vec());
        catalog
    }
}

impl SessionCatalog for TreeCatalog {
    fn parent(
        &self,
        _session: SessionId,
    ) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn children(&self, session: SessionId) -> BoxStream<'_, Result<SessionId, StorageError>> {
        futures_util::stream::iter(
            self.children
                .get(&session)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(Ok),
        )
        .boxed()
    }
}

fn service(log: Arc<TestLog>) -> UsageService {
    UsageService::new(log, Arc::new(TreeCatalog::default()))
}

fn filter(scope: UsageScope) -> UsageFilter {
    UsageFilter {
        scope,
        bucket: None,
        model: None,
        since: None,
        until: None,
        limit: None,
    }
}

async fn records(service: &UsageService, filter: UsageFilter) -> Vec<UsageRecord> {
    service
        .records(filter)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|record| record.expect("record"))
        .collect()
}

use asterpath_protocol::UsageRecord;

async fn totals(service: &UsageService, filter: UsageFilter) -> asterpath_protocol::UsageReport {
    service.totals(filter).await.expect("totals")
}

#[tokio::test]
async fn u1_two_successful_steps_roll_up_by_step_turn_and_session() {
    let log = Arc::new(TestLog::default());
    log.push(ROOT, &SessionEvent::TurnStarted { turn_id: TURN }, None);
    log.push(ROOT, &started(STEP_A), None);
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 2)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    log.push(
        ROOT,
        &committed(STEP_A),
        Some(dedup::step_committed(STEP_A)),
    );
    log.push(ROOT, &started(STEP_B), None);
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_B, usage(20, 3)),
        Some(dedup::assistant_message(TURN, STEP_B)),
    );
    log.push(
        ROOT,
        &committed(STEP_B),
        Some(dedup::step_committed(STEP_B)),
    );
    log.push(
        ROOT,
        &SessionEvent::TurnCommitted {
            turn_id: TURN,
            reason: asterpath_protocol::TurnEndReason::Completed,
        },
        None,
    );
    let service = service(log);

    let session = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(session.own.model_calls, usage(30, 5));
    assert_eq!(session.own.total(), 35);
    assert_eq!(session.own.partial, Usage::default());
    assert_eq!(session.descendants, Default::default());

    let step_b = totals(
        &service,
        filter(UsageScope::Step {
            session: ROOT,
            turn: TURN,
            step: STEP_B,
        }),
    )
    .await;
    assert_eq!(step_b.own.model_calls, usage(20, 3));
    assert_eq!(step_b.own.compaction, Usage::default());

    let turn = totals(
        &service,
        filter(UsageScope::Turn {
            session: ROOT,
            turn: TURN,
        }),
    )
    .await;
    assert_eq!(turn.own.model_calls, usage(30, 5));

    let found = records(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(found.len(), 2);
    assert!(
        found
            .iter()
            .all(|record| record.bucket == UsageBucket::ModelCall)
    );
}

#[tokio::test]
async fn u2_retry_partial_is_diagnostic_and_not_in_total() {
    let log = Arc::new(TestLog::default());
    log.push(ROOT, &aborted("m", TURN, STEP_A, Some(usage(7, 0))), None);
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 2)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    let service = service(log);

    let report = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(report.own.model_calls, usage(10, 2));
    assert_eq!(report.own.partial, usage(7, 0));
    assert_eq!(report.own.total(), 12, "partial must not enter total");

    let found = records(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].bucket, UsageBucket::Partial);
    assert_eq!(found[1].bucket, UsageBucket::ModelCall);
}

#[tokio::test]
async fn u3_abort_without_usage_produces_no_record() {
    let log = Arc::new(TestLog::default());
    log.push(ROOT, &aborted("m", TURN, STEP_A, None), None);
    let service = service(log);

    assert!(
        records(&service, filter(UsageScope::Session(ROOT)))
            .await
            .is_empty()
    );
    let report = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(report.own, Default::default());
}

#[tokio::test]
async fn u4_cancelled_without_assistant_message_only_partial() {
    let log = Arc::new(TestLog::default());
    log.push(
        ROOT,
        &SessionEvent::ModelCallAborted {
            turn_id: TURN,
            step_id: STEP_A,
            attempt: 1,
            kind: asterpath_protocol::AbortKind::Cancelled,
            partial: None,
            model: model("m"),
            backoff_ms: 0,
            usage: Some(usage(4, 0)),
        },
        None,
    );
    let service = service(log);

    let found = records(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].bucket, UsageBucket::Partial);

    let report = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(report.own.model_calls, Usage::default());
    assert_eq!(report.own.total(), 0);
}

#[tokio::test]
async fn u5_cancelled_after_assistant_message_keeps_one_model_call() {
    let log = Arc::new(TestLog::default());
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 2)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    log.push(
        ROOT,
        &SessionEvent::ModelCallAborted {
            turn_id: TURN,
            step_id: STEP_A,
            attempt: 1,
            kind: asterpath_protocol::AbortKind::Cancelled,
            partial: None,
            model: model("m"),
            backoff_ms: 0,
            usage: None,
        },
        None,
    );
    let service = service(log);

    let report = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(report.own.model_calls, usage(10, 2));
    assert_eq!(report.own.total(), 12);
    assert_eq!(
        records(&service, filter(UsageScope::Session(ROOT)))
            .await
            .len(),
        1
    );
}

fn push_compaction(
    log: &TestLog,
    session: SessionId,
    caused_by: LogId,
    turn: Option<TurnId>,
    compaction_usage: Usage,
) -> LogId {
    let key = dedup::compaction(caused_by);
    let mut current_model = model("summary");
    current_model.model = ModelId::new("summary");
    log.push(
        session,
        &SessionEvent::CompactionStarted {
            trigger: CompactionTrigger::Manual,
            replaces_from: caused_by,
            caused_by,
            turn,
        },
        Some(key.clone()),
    );
    log.push(
        session,
        &SessionEvent::CompactionApplied {
            first_kept: caused_by,
            summary: vec![text("sum")],
            replaces: asterpath_core::LogIdRange {
                first: caused_by,
                last: caused_by,
            },
            model: current_model,
            usage: compaction_usage,
            raw: None,
        },
        Some(key),
    )
}

#[tokio::test]
async fn u6_in_turn_compaction_has_turn_and_no_step() {
    let log = Arc::new(TestLog::default());
    log.push(ROOT, &SessionEvent::TurnStarted { turn_id: TURN }, None);
    log.push(ROOT, &started(STEP_A), None);
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 2)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    let frontier = log.push(
        ROOT,
        &committed(STEP_A),
        Some(dedup::step_committed(STEP_A)),
    );
    push_compaction(&log, ROOT, frontier, Some(TURN), usage(50, 5));
    let service = service(log);

    let turn = totals(
        &service,
        filter(UsageScope::Turn {
            session: ROOT,
            turn: TURN,
        }),
    )
    .await;
    assert_eq!(turn.own.model_calls, usage(10, 2));
    assert_eq!(turn.own.compaction, usage(50, 5));
    assert_eq!(turn.own.total(), 67);

    let step = totals(
        &service,
        filter(UsageScope::Step {
            session: ROOT,
            turn: TURN,
            step: STEP_A,
        }),
    )
    .await;
    assert_eq!(
        step.own.compaction,
        Usage::default(),
        "compaction never enters a step"
    );
    assert_eq!(step.own.model_calls, usage(10, 2));

    let found = records(&service, filter(UsageScope::Session(ROOT))).await;
    let compaction = found
        .iter()
        .find(|record| record.bucket == UsageBucket::Compaction)
        .expect("compaction record");
    assert_eq!(compaction.turn, Some(TURN));
    assert_eq!(compaction.step, None);
    assert_eq!(compaction.usage, usage(50, 5));
}

#[tokio::test]
async fn u7_idle_compaction_has_no_turn() {
    let log = Arc::new(TestLog::default());
    log.push(ROOT, &SessionEvent::TurnStarted { turn_id: TURN }, None);
    log.push(ROOT, &started(STEP_A), None);
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 2)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    log.push(
        ROOT,
        &committed(STEP_A),
        Some(dedup::step_committed(STEP_A)),
    );
    let frontier = log.push(
        ROOT,
        &SessionEvent::TurnCommitted {
            turn_id: TURN,
            reason: asterpath_protocol::TurnEndReason::Completed,
        },
        None,
    );
    push_compaction(&log, ROOT, frontier, None, usage(50, 5));
    let service = service(log);

    let found = records(&service, filter(UsageScope::Session(ROOT))).await;
    let compaction = found
        .iter()
        .find(|record| record.bucket == UsageBucket::Compaction)
        .expect("compaction record");
    assert_eq!(compaction.turn, None);
    assert_eq!(compaction.step, None);

    let session = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(session.own.compaction, usage(50, 5));

    let turn = totals(
        &service,
        filter(UsageScope::Turn {
            session: ROOT,
            turn: TURN,
        }),
    )
    .await;
    assert_eq!(turn.own.compaction, Usage::default());
    assert_eq!(turn.own.model_calls, usage(10, 2));
}

#[tokio::test]
async fn u8_tree_own_descendants_and_fork_do_not_inherit() {
    let log = Arc::new(TestLog::default());
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 0)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    // Child logs never copy parent usage; each holds only its own calls.
    log.push(
        CHILD_A,
        &assistant("m", TURN, STEP_A, usage(20, 0)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    log.push(
        CHILD_B,
        &assistant("m", TURN, STEP_A, usage(30, 0)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    let service = UsageService::new(log, Arc::new(TreeCatalog::parent_of(&[CHILD_A, CHILD_B])));

    let own = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(own.own.model_calls, usage(10, 0));

    let subtree = totals(&service, filter(UsageScope::SessionSubtree(ROOT))).await;
    assert_eq!(subtree.own.model_calls, usage(10, 0));
    assert_eq!(subtree.descendants.model_calls, usage(50, 0));
    assert_eq!(
        subtree.own.total() + subtree.descendants.total(),
        60,
        "own + descendants counts each call once"
    );

    let child = totals(&service, filter(UsageScope::Session(CHILD_A))).await;
    assert_eq!(
        child.own.model_calls,
        usage(20, 0),
        "fork child does not inherit"
    );
    assert_eq!(child.descendants, Default::default());
}

#[tokio::test]
async fn u9_crash_after_assistant_message_counts_once() {
    let log = Arc::new(TestLog::default());
    log.push(ROOT, &SessionEvent::TurnStarted { turn_id: TURN }, None);
    log.push(ROOT, &started(STEP_A), None);
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 2)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    // No StepCommitted: the process died at the checkpoint boundary.
    let service = service(log);

    let report = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(report.own.model_calls, usage(10, 2));
    assert_eq!(
        records(&service, filter(UsageScope::Session(ROOT)))
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn u10_crash_before_assistant_message_reports_nothing() {
    let log = Arc::new(TestLog::default());
    log.push(ROOT, &SessionEvent::TurnStarted { turn_id: TURN }, None);
    log.push(ROOT, &started(STEP_A), None);
    // The response never landed; recovery re-calls the model.
    let service = service(log);

    let report = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(report.own, Default::default());
    assert!(
        records(&service, filter(UsageScope::Session(ROOT)))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn u12_totals_reconcile_with_trace_usage_sums() {
    let log = Arc::new(TestLog::default());
    log.push(ROOT, &SessionEvent::TurnStarted { turn_id: TURN }, None);
    log.push(ROOT, &started(STEP_A), None);
    log.push(ROOT, &aborted("m", TURN, STEP_A, Some(usage(999, 0))), None);
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 2)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    let frontier = log.push(
        ROOT,
        &committed(STEP_A),
        Some(dedup::step_committed(STEP_A)),
    );
    push_compaction(&log, ROOT, frontier, Some(TURN), usage(50, 5));

    // An independent fold, as the tracing projection would do it: sum the
    // usage on model_call and compaction spans.
    let entries: Vec<LogEntry> = log
        .scan(LogQuery {
            session: ROOT,
            after: None,
            limit: None,
        })
        .map(|entry| entry.expect("scan"))
        .collect()
        .await;
    let mut trace = 0u64;
    for entry in &entries {
        match asterpath_protocol::decode_entry(entry).expect("decode") {
            SessionEvent::AssistantMessage { usage, .. }
            | SessionEvent::CompactionApplied { usage, .. } => trace += usage.total(),
            _ => {}
        }
    }

    let service = UsageService::new(log, Arc::new(TreeCatalog::default()));
    let report = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(report.own.total(), trace);
    assert_eq!(report.own.partial, usage(999, 0));
}

#[tokio::test]
async fn u13_corrupt_log_fails_the_whole_query() {
    let log = Arc::new(TestLog::default());
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 0)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    log.push_corrupt(ROOT, 5000);
    let service = service(log);

    match service.totals(filter(UsageScope::Session(ROOT))).await {
        Err(UsageError::Corrupt { session, .. }) => assert_eq!(session, ROOT),
        other => panic!("expected Corrupt, got {other:?}"),
    }

    let found: Vec<_> = service
        .records(filter(UsageScope::Session(ROOT)))
        .collect()
        .await;
    assert!(matches!(
        found.last(),
        Some(Err(UsageError::Corrupt { .. }))
    ));
}

#[tokio::test]
async fn u14_filters_combine_and_until_is_exclusive() {
    let log = Arc::new(TestLog::default());
    log.push_at(
        ROOT,
        &assistant("alpha", TURN, STEP_A, usage(10, 0)),
        Some(dedup::assistant_message(TURN, STEP_A)),
        1000,
    );
    log.push_at(
        ROOT,
        &assistant("beta", TURN, STEP_B, usage(20, 0)),
        Some(dedup::assistant_message(TURN, STEP_B)),
        2000,
    );
    log.push_at(
        ROOT,
        &aborted("beta", TURN, STEP_B, Some(usage(3, 0))),
        None,
        3000,
    );
    let service = service(log);

    let mut by_bucket = filter(UsageScope::Session(ROOT));
    by_bucket.bucket = Some(UsageBucket::ModelCall);
    assert_eq!(records(&service, by_bucket).await.len(), 2);

    let mut by_model = filter(UsageScope::Session(ROOT));
    by_model.model = Some(ModelId::new("beta"));
    let beta = records(&service, by_model).await;
    assert_eq!(beta.len(), 2);
    assert!(
        beta.iter()
            .all(|record| record.model.model == ModelId::new("beta"))
    );

    let mut since = filter(UsageScope::Session(ROOT));
    since.since = Some(2000);
    assert_eq!(
        records(&service, since).await.len(),
        2,
        "since is inclusive"
    );

    let mut until = filter(UsageScope::Session(ROOT));
    until.until = Some(2000);
    let before = records(&service, until).await;
    assert_eq!(before.len(), 1, "until is exclusive");
    assert_eq!(before[0].model.model, ModelId::new("alpha"));

    let mut window = filter(UsageScope::Session(ROOT));
    window.since = Some(1000);
    window.until = Some(3000);
    assert_eq!(records(&service, window.clone()).await.len(), 2);

    let mut limited = window.clone();
    limited.limit = Some(1);
    assert_eq!(records(&service, limited).await.len(), 1);
    let limited_totals = totals(&service, window).await;
    assert_eq!(
        limited_totals.own.model_calls,
        usage(30, 0),
        "totals ignores limit"
    );
}

#[tokio::test]
async fn u15_zero_usage_still_produces_a_record() {
    let log = Arc::new(TestLog::default());
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, Usage::default()),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    let service = service(log);

    let found = records(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].usage, Usage::default());
    let report = totals(&service, filter(UsageScope::Session(ROOT))).await;
    assert_eq!(report.own.model_calls, Usage::default());
    assert_eq!(report.own.total(), 0);
}

#[tokio::test]
async fn u16_subtree_sums_only_surviving_logs_and_missing_root_is_not_found() {
    let log = Arc::new(TestLog::default());
    log.push(
        ROOT,
        &assistant("m", TURN, STEP_A, usage(10, 0)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    // CHILD_A has no log at all (archived/deleted); CHILD_B survives.
    log.push(
        CHILD_B,
        &assistant("m", TURN, STEP_A, usage(30, 0)),
        Some(dedup::assistant_message(TURN, STEP_A)),
    );
    let service = UsageService::new(log, Arc::new(TreeCatalog::parent_of(&[CHILD_A, CHILD_B])));

    let subtree = totals(&service, filter(UsageScope::SessionSubtree(ROOT))).await;
    assert_eq!(subtree.own.model_calls, usage(10, 0));
    assert_eq!(subtree.descendants.model_calls, usage(30, 0));

    match service.totals(filter(UsageScope::Session(RETIRED))).await {
        Err(UsageError::NotFound { session }) => assert_eq!(session, RETIRED),
        other => panic!("expected NotFound, got {other:?}"),
    }
    match service
        .totals(filter(UsageScope::SessionSubtree(RETIRED)))
        .await
    {
        Err(UsageError::NotFound { session }) => assert_eq!(session, RETIRED),
        other => panic!("expected NotFound, got {other:?}"),
    }
}
