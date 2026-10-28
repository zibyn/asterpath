//! Backend-neutral `SessionStore` contract suite: the log (G2, G3, G4, G6),
//! the session catalog, retirement (G8, R1-R7) and session-tree ownership
//! across two handles standing for two processes (ADR-0036).
//!
//! The suite only depends on protocol types, so every backend runs the same
//! assertions. G1, `Corrupt` and backend-specific behaviour are not here.
//! Wake discovery is a session-facts fold in the facade (ADR-0023), so it is
//! tested there, not per backend.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::ManualClock;
use asterpath_protocol::{
    CatalogEntry, Clock, DEFAULT_LEASE_TTL, DedupKey, LogEntry, LogId, LogIdRange, LogQuery,
    NewEntry, OwnerToken, ParentFilter, RetirementError, SessionId, SessionQuery, SessionStore,
    StepId, StorageError, ToolCallId, TurnId,
};
use asterpath_protocol::{
    ChildOutcome, CompactionAbortReason, CompactionTrigger, ConfigSnapshot, ContextPolicy,
    JsonString, LOG_SCHEMA_VERSION, ModelId, ModelSpec, ParentRef, Part, Provenance, ProviderId,
    ReplaySafety, SessionEvent, ToolName, ToolResultStatus, Usage, dedup,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

pub trait StoreFactory {
    /// A handle to the backend under test.
    fn open(&self) -> Arc<dyn SessionStore>;
    /// A fresh, empty store that stamps `recorded_at` from `clock`.
    fn open_with_clock(&self, clock: Arc<dyn Clock>) -> Arc<dyn SessionStore>;
    /// Two handles on one fresh, empty store, as two processes sharing it
    /// would open it, both reading time from `clock` and leasing for
    /// `DEFAULT_LEASE_TTL`: claims made through one conflict with the
    /// other's (ADR-0036).
    fn open_processes(&self, clock: Arc<dyn Clock>) -> [Arc<dyn SessionStore>; 2];
}

/// Run the shared store contract against a backend. Retirement cases are
/// skipped when the backend keeps the default `Unsupported`.
pub async fn session_store_contract<F: StoreFactory>(factory: &F) {
    let store = factory.open();
    let store = store.as_ref();
    idempotent_append(store).await;
    shared_dedup_key_pairs(store).await;
    monotonic_seq(store).await;
    config_changes(store).await;
    cursor_scan(store).await;
    independent_streams(store).await;
    hard_errors(store).await;
    let clock = Arc::new(ManualClock::new());
    catalog(&*factory.open_with_clock(clock.clone()), &clock).await;
    metadata_filter(store).await;
    retirement(store).await;
    let clock = Arc::new(ManualClock::new());
    let [here, elsewhere] = factory.open_processes(clock.clone());
    ownership(&*here, &*elsewhere).await;
    let [here, elsewhere] = factory.open_processes(clock.clone());
    process_leases(&*here, &*elsewhere, &clock).await;
    exit_and_kill(|| factory.open_processes(clock.clone()), &clock).await;
}

fn session(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn turn() -> TurnId {
    TurnId::from_bytes([1; 16])
}

fn step() -> StepId {
    StepId::from_bytes([2; 16])
}

fn call(byte: u8) -> ToolCallId {
    ToolCallId::from_bytes([byte; 16])
}

fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("test"),
        model: ModelId::new("model"),
        context_window: Some(1000),
    }
}

fn config() -> ConfigSnapshot {
    ConfigSnapshot {
        system_sections: Vec::new(),
        model: model(),
        system: vec![],
        tools: vec![],
        request: Default::default(),
        turn: Default::default(),
        compaction: Default::default(),
        subagents: Default::default(),
    }
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn tool_call_requested(call_id: ToolCallId) -> SessionEvent {
    SessionEvent::ToolCallRequested {
        turn_id: turn(),
        step_id: step(),
        tool_call_id: call_id,
        provider_call_id: "provider-call".to_owned(),
        name: ToolName::new("echo"),
        raw_args: JsonString::new("{}"),
        replay: ReplaySafety::Safe,
        provenance: Provenance::InProcess,
    }
}

fn tool_result(call_id: ToolCallId) -> SessionEvent {
    SessionEvent::ToolResult {
        tool_call_id: call_id,
        status: ToolResultStatus::Completed,
        output: Some(vec![text("ok")]),
        error: None,
        truncated: false,
    }
}

fn compaction_started() -> SessionEvent {
    SessionEvent::CompactionStarted {
        trigger: CompactionTrigger::Manual,
        replaces_from: LogId::from_i64(1),
        caused_by: LogId::from_i64(1),
        turn: None,
    }
}

fn compaction_applied() -> SessionEvent {
    SessionEvent::CompactionApplied {
        first_kept: LogId::from_i64(2),
        summary: vec![text("summary")],
        replaces: LogIdRange {
            first: LogId::from_i64(1),
            last: LogId::from_i64(1),
        },
        model: model(),
        usage: Usage::default(),
        raw: None,
    }
}

fn compaction_aborted() -> SessionEvent {
    SessionEvent::CompactionAborted {
        reason: CompactionAbortReason::Insufficient,
    }
}

fn child_spawned(child: SessionId) -> SessionEvent {
    SessionEvent::ChildSessionSpawned {
        child,
        task: vec![text("task")],
        config: config(),
        context: ContextPolicy::Fresh,
        parent_tool_call_id: call(1),
        caused_by: LogId::from_i64(1),
        deadline: None,
    }
}

fn child_completed(child: SessionId) -> SessionEvent {
    SessionEvent::ChildSessionCompleted {
        child,
        outcome: ChildOutcome::Cancelled {
            reason: asterpath_protocol::CancelReason::Application,
        },
    }
}

fn approval_decided(call_id: ToolCallId) -> SessionEvent {
    SessionEvent::ApprovalDecided {
        tool_call_id: call_id,
        decision: asterpath_protocol::ApprovalDecision::Allow,
    }
}

fn session_created() -> SessionEvent {
    SessionEvent::SessionCreated {
        config: config(),
        parent: None,
        metadata: BTreeMap::new(),
    }
}

fn context_forked() -> SessionEvent {
    SessionEvent::ContextForked {
        source: asterpath_protocol::ForkSource::Spawn(asterpath_protocol::ParentRef {
            session: session(1),
            turn: turn(),
            tool_call: call(1),
        }),
        source_log: LogId::from_i64(1),
        items: vec![],
    }
}

fn input_recorded() -> SessionEvent {
    SessionEvent::InputRecorded {
        input: vec![text("go")],
    }
}

fn entry_for(
    session: SessionId,
    schema_version: u16,
    event: &SessionEvent,
    dedup_key: Option<DedupKey>,
) -> NewEntry {
    NewEntry {
        session,
        schema_version,
        dedup_key,
        owner: None,
        payload: serde_json::to_value(event).expect("serialize event"),
    }
}

async fn append(
    log: &dyn SessionStore,
    session: SessionId,
    event: &SessionEvent,
    dedup_key: Option<DedupKey>,
) -> LogId {
    append_raw(
        log,
        entry_for(session, LOG_SCHEMA_VERSION, event, dedup_key),
    )
    .await
    .expect("append should succeed")
}

async fn append_raw(log: &dyn SessionStore, entry: NewEntry) -> Result<LogId, StorageError> {
    log.append(entry).await
}

async fn scan(log: &dyn SessionStore, session: SessionId) -> Vec<LogEntry> {
    scan_query(
        log,
        LogQuery {
            session,
            after: None,
            limit: None,
        },
    )
    .await
}

async fn scan_query(log: &dyn SessionStore, query: LogQuery) -> Vec<LogEntry> {
    let results: Vec<Result<LogEntry, StorageError>> = log.scan(query).collect().await;
    results
        .into_iter()
        .map(|result| result.expect("scan should succeed"))
        .collect()
}

async fn frontier(log: &dyn SessionStore, session: SessionId) -> Option<LogId> {
    log.frontier(session)
        .await
        .expect("frontier should succeed")
}

async fn idempotent_append(log: &dyn SessionStore) {
    let session = session(10);
    let call_id = call(10);
    let requested = tool_call_requested(call_id);
    let result = tool_result(call_id);
    let key = dedup::tool_call(call_id);

    let first = append(log, session, &requested, Some(key.clone())).await;
    let second = append(log, session, &requested, Some(key.clone())).await;
    assert_eq!(
        first, second,
        "G2: duplicate append returns the original LogId"
    );
    assert_eq!(
        scan(log, session).await.len(),
        1,
        "G2: duplicate append writes nothing"
    );

    // Same key, different event kind: both records land.
    let result_id = append(log, session, &result, Some(key)).await;
    assert_ne!(first, result_id, "G2: event kind is part of the identity");
    assert_eq!(
        scan(log, session).await.len(),
        2,
        "G2: shared key types coexist"
    );

    // Different key, same event kind: a new record lands.
    let other = append(log, session, &requested, Some(DedupKey::new("other"))).await;
    assert_ne!(other, first);
    assert_eq!(scan(log, session).await.len(), 3);
}

async fn shared_dedup_key_pairs(log: &dyn SessionStore) {
    let call_id = call(20);
    let child = session(21);
    let groups: Vec<(SessionId, DedupKey, Vec<SessionEvent>)> = vec![
        (
            session(20),
            dedup::tool_call(call_id),
            vec![
                tool_call_requested(call_id),
                approval_decided(call_id),
                tool_result(call_id),
            ],
        ),
        (
            session(22),
            dedup::compaction(LogId::from_i64(1)),
            vec![
                compaction_started(),
                compaction_applied(),
                compaction_aborted(),
            ],
        ),
        (
            session(23),
            dedup::child_session(child),
            vec![child_spawned(child), child_completed(child)],
        ),
        (
            session(26),
            dedup::child_session(child),
            vec![session_created(), context_forked(), input_recorded()],
        ),
    ];

    for (session, key, events) in groups {
        let mut first_pass = Vec::new();
        for event in &events {
            first_pass.push(append(log, session, event, Some(key.clone())).await);
        }
        let mut unique = first_pass.clone();
        unique.sort_by_key(|id| id.as_i64());
        unique.dedup();
        assert_eq!(
            unique.len(),
            events.len(),
            "G2/S12: every event kind under one shared key lands"
        );
        assert_eq!(scan(log, session).await.len(), events.len());

        let mut second_pass = Vec::new();
        for event in &events {
            second_pass.push(append(log, session, event, Some(key.clone())).await);
        }
        assert_eq!(
            first_pass, second_pass,
            "G2: replaying a shared-key group is idempotent per event kind"
        );
        assert_eq!(scan(log, session).await.len(), events.len());
    }
}

async fn monotonic_seq(log: &dyn SessionStore) {
    let sid = session(30);
    for index in 0..5 {
        append(
            log,
            sid,
            &input_recorded(),
            Some(DedupKey::new(format!("input-{index}"))),
        )
        .await;
    }
    let entries = scan(log, sid).await;
    let seqs: Vec<u64> = entries.iter().map(|entry| entry.seq).collect();
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4, 5],
        "G3: seq is strictly monotonic without holes"
    );
    assert!(
        entries
            .windows(2)
            .all(|pair| pair[0].log_id < pair[1].log_id),
        "G3: storage assigns increasing log ids"
    );

    let other = session(31);
    append(log, other, &input_recorded(), None).await;
    assert_eq!(
        scan(log, other).await[0].seq,
        1,
        "G3: seq is per session, not global"
    );
}

/// A `ConfigChanged` without an `UpdateId` has no dedup key: every change is
/// its own record and decodes back to the same patch. One with an id is keyed
/// by it, so a retried change is recorded once.
async fn config_changes(log: &dyn SessionStore) {
    let sid = session(35);
    let mut patch = asterpath_protocol::ConfigPatch::default();
    patch.model = Some(ModelSpec {
        model: ModelId::new("model-2"),
        ..model()
    });
    let event = SessionEvent::ConfigChanged { patch };
    let first = append(log, sid, &event, None).await;
    let second = append(log, sid, &event, None).await;
    assert_ne!(
        first, second,
        "a keyless ConfigChanged is never deduplicated"
    );
    let entries = scan(log, sid).await;
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        assert_eq!(
            asterpath_protocol::decode_entry(entry).expect("decode"),
            event
        );
    }

    let keyed = session(36);
    let id = asterpath_protocol::UpdateId::from_bytes([36; 16]);
    let mut patch = asterpath_protocol::ConfigPatch::default();
    patch.id = Some(id);
    patch.append_system = vec![asterpath_protocol::Part::Text {
        text: "more".to_owned(),
    }];
    let event = SessionEvent::ConfigChanged { patch };
    let key = || Some(asterpath_protocol::dedup::config_changed(id));
    let first = append(log, keyed, &event, key()).await;
    let retried = append(log, keyed, &event, key()).await;
    assert_eq!(
        first, retried,
        "a ConfigChanged with an UpdateId is deduplicated"
    );
    let entries = scan(log, keyed).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        asterpath_protocol::decode_entry(&entries[0]).expect("decode"),
        event
    );
}

async fn cursor_scan(log: &dyn SessionStore) {
    let sid = session(40);
    let mut ids = Vec::new();
    for index in 0..4 {
        ids.push(
            append(
                log,
                sid,
                &input_recorded(),
                Some(DedupKey::new(format!("cursor-{index}"))),
            )
            .await,
        );
    }

    let after = scan_query(
        log,
        LogQuery {
            session: sid,
            after: Some(ids[1]),
            limit: None,
        },
    )
    .await;
    assert_eq!(after.len(), 2, "G4: the cursor is exclusive");
    assert_eq!(
        after.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        vec![3, 4],
        "G4: scan is ordered by seq"
    );

    let limited = scan_query(
        log,
        LogQuery {
            session: sid,
            after: None,
            limit: Some(2),
        },
    )
    .await;
    assert_eq!(limited.len(), 2, "G4: limit is bounded");
    assert_eq!(limited[0].seq, 1);

    let empty = scan_query(
        log,
        LogQuery {
            session: sid,
            after: Some(ids[3]),
            limit: None,
        },
    )
    .await;
    assert!(empty.is_empty(), "G4: no records after the last id");

    assert_eq!(
        frontier(log, sid).await,
        Some(ids[3]),
        "G4: frontier is the last LogId"
    );
    assert_eq!(frontier(log, session(99)).await, None);
}

async fn independent_streams(log: &dyn SessionStore) {
    let stream_a = session(0x70);
    let stream_b = session(0x71);
    let turn_a = TurnId::from_bytes([7; 16]);
    let turn_b = TurnId::from_bytes([8; 16]);
    append(log, stream_a, &turn_started(turn_a), None).await;
    append(log, stream_b, &turn_started(turn_b), None).await;
    append(
        log,
        stream_a,
        &turn_committed(turn_a),
        Some(dedup::turn_committed(turn_a)),
    )
    .await;
    let seqs = |entries: Vec<LogEntry>| entries.iter().map(|entry| entry.seq).collect::<Vec<_>>();
    assert_eq!(
        seqs(scan(log, stream_a).await),
        vec![1, 2],
        "G6: another stream does not create holes or delay visibility"
    );
    assert_eq!(
        seqs(scan(log, stream_b).await),
        vec![1],
        "G6: each stream allocates its own seq"
    );
    assert_eq!(
        frontier(log, stream_a).await,
        scan(log, stream_a).await.last().map(|entry| entry.log_id),
        "G6: frontier is per stream"
    );
}

async fn hard_errors(log: &dyn SessionStore) {
    let session = session(50);
    let cases: Vec<(Value, u16)> = vec![
        (json!({"NoSuchEvent": {}}), LOG_SCHEMA_VERSION),
        (json!({"ToolResult": {}}), LOG_SCHEMA_VERSION),
        (json!("not an event"), LOG_SCHEMA_VERSION),
        (
            json!({"InputRecorded": {"input": []}}),
            LOG_SCHEMA_VERSION + 1,
        ),
    ];
    for (payload, schema_version) in cases {
        let error = append_raw(
            log,
            NewEntry {
                session,
                schema_version,
                dedup_key: None,
                owner: None,
                payload,
            },
        )
        .await
        .expect_err("bad payload must be a hard error");
        assert!(
            matches!(error, StorageError::InvalidEntry(_)),
            "expected InvalidEntry, got {error:?}"
        );
    }
    assert_eq!(
        scan(log, session).await.len(),
        0,
        "rejected appends write nothing"
    );

    // Unknown fields are ignored, not rejected.
    let accepted = append_raw(
        log,
        NewEntry {
            session,
            schema_version: LOG_SCHEMA_VERSION,
            dedup_key: None,
            owner: None,
            payload: json!({"InputRecorded": {"input": [], "extra_field": true}}),
        },
    )
    .await;
    assert!(accepted.is_ok(), "unknown fields are ignored");
    assert_eq!(scan(log, session).await.len(), 1);
}

fn turn_started(turn_id: TurnId) -> SessionEvent {
    SessionEvent::TurnStarted { turn_id }
}

fn turn_committed(turn_id: TurnId) -> SessionEvent {
    SessionEvent::TurnCommitted {
        turn_id,
        reason: asterpath_protocol::TurnEndReason::Completed,
    }
}

/// Session metadata of the catalog's root session (ADR-0032).
fn root_metadata() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("cwd".to_owned(), "/work/项目".to_owned()),
        ("name".to_owned(), "a \"quoted\" name".to_owned()),
    ])
}

fn created_under(parent: Option<SessionId>) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: config(),
        parent: parent.map(|session| ParentRef {
            session,
            turn: turn(),
            tool_call: call(1),
        }),
        // Roots carry the application's metadata; children carry none.
        metadata: if parent.is_none() {
            root_metadata()
        } else {
            BTreeMap::new()
        },
    }
}

async fn create(log: &dyn SessionStore, session: SessionId, parent: Option<SessionId>) -> LogId {
    append(
        log,
        session,
        &created_under(parent),
        Some(dedup::session_created(session)),
    )
    .await
}

async fn list(log: &dyn SessionStore, query: SessionQuery) -> Vec<CatalogEntry> {
    log.sessions(query).await.expect("sessions should succeed")
}

fn by_parent(parent: ParentFilter) -> SessionQuery {
    SessionQuery {
        parent,
        ..SessionQuery::default()
    }
}

async fn listed(log: &dyn SessionStore, parent: ParentFilter) -> Vec<SessionId> {
    list(log, by_parent(parent))
        .await
        .iter()
        .map(|entry| entry.session)
        .collect()
}

/// Every page of `parent`, `limit` entries at a time, concatenated.
async fn paged(log: &dyn SessionStore, parent: ParentFilter, limit: usize) -> Vec<CatalogEntry> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = list(
            log,
            SessionQuery {
                parent,
                after,
                limit: Some(limit),
                ..SessionQuery::default()
            },
        )
        .await;
        assert!(page.len() <= limit, "a page respects limit");
        let Some(last) = page.last() else {
            return all;
        };
        after = Some(last.cursor());
        all.extend(page);
    }
}

async fn parent(log: &dyn SessionStore, session: SessionId) -> Option<SessionId> {
    log.parent(session).await.expect("parent should succeed")
}

/// The catalog follows `SessionCreated`: every created session (children
/// included) is listed once, newest `(created_at, session)` first; the tree
/// matches each parent ref; cursor pages neither repeat nor skip (ADR-0028).
async fn catalog(log: &dyn SessionStore, clock: &ManualClock) {
    let root = session(0x81);
    let unknown = session(0x8f);
    create(log, root, None).await;
    clock.advance(1);
    // The clock stands still, so the children share `created_at` and the
    // cursor's session tie-break is exercised on every backend.
    let kids: Vec<SessionId> = (0..3u8)
        .map(|byte| {
            let mut bytes = [0u8; 16];
            bytes[0] = 0x84;
            bytes[1] = byte;
            SessionId::from_bytes(bytes)
        })
        .collect();
    for &child in &kids {
        create(log, child, Some(root)).await;
    }
    let entries = list(log, by_parent(ParentFilter::ChildrenOf(root))).await;
    assert!(
        entries
            .windows(2)
            .all(|pair| pair[0].created_at == pair[1].created_at),
        "children created at one instant tie on created_at"
    );
    // A replayed SessionCreated does not list the session twice.
    create(log, root, None).await;

    let all = list(log, SessionQuery::default()).await;
    for session in kids.iter().chain([&root]) {
        assert_eq!(
            all.iter().filter(|entry| entry.session == *session).count(),
            1,
            "sessions() lists every created session once, children included"
        );
    }
    assert!(!all.iter().any(|entry| entry.session == unknown));
    assert!(
        all.windows(2)
            .all(|pair| pair[0].cursor() > pair[1].cursor()),
        "sessions() is ordered by (created_at, session) descending"
    );
    let root_entry = all.iter().find(|entry| entry.session == root).unwrap();
    assert_eq!(root_entry.parent, None);
    assert_eq!(
        root_entry.metadata,
        root_metadata(),
        "the entry carries SessionCreated's session metadata"
    );
    let first_kid = all.iter().find(|entry| entry.session == kids[0]).unwrap();
    assert_eq!(first_kid.parent, Some(root));
    assert!(first_kid.metadata.is_empty());
    assert!(first_kid.created_at >= root_entry.created_at);

    // Parent filters.
    let mut children = listed(log, ParentFilter::ChildrenOf(root)).await;
    children.sort();
    assert_eq!(children, kids, "ChildrenOf lists exactly the children");
    assert!(
        listed(log, ParentFilter::ChildrenOf(kids[0]))
            .await
            .is_empty()
    );
    let roots = listed(log, ParentFilter::Roots).await;
    assert!(roots.contains(&root));
    assert!(
        !kids.iter().any(|kid| roots.contains(kid)),
        "Roots skips children"
    );

    // Cursor pages, ties included, add up to the unpaged list.
    let unpaged = list(log, by_parent(ParentFilter::ChildrenOf(root))).await;
    assert_eq!(paged(log, ParentFilter::ChildrenOf(root), 1).await, unpaged);
    assert_eq!(paged(log, ParentFilter::Any, 2).await, all);

    // last_activity is the recorded_at of the last committed record.
    let kid = kids[0];
    append(log, kid, &input_recorded(), Some(DedupKey::new("catalog"))).await;
    let last = scan(log, kid).await.last().unwrap().recorded_at;
    let entry = list(log, SessionQuery::default())
        .await
        .into_iter()
        .find(|entry| entry.session == kid)
        .unwrap();
    assert_eq!(entry.last_activity, last, "last_activity follows append");
    assert!(entry.last_activity >= entry.created_at);

    assert_eq!(parent(log, root).await, None);
    assert_eq!(parent(log, kid).await, Some(root));
    assert_eq!(parent(log, unknown).await, None);
}

/// Metadata filters match every pair by equality, ANDed, with parent
/// filters and pages applied to the matches (ADR-0039).
async fn metadata_filter(log: &dyn SessionStore) {
    let tenant = |user: &str, extra: &[(&str, &str)]| {
        let mut metadata = BTreeMap::from([("tenant".to_owned(), user.to_owned())]);
        metadata.extend(
            extra
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        );
        metadata
    };
    let created = |metadata, parent: Option<SessionId>| SessionEvent::SessionCreated {
        config: config(),
        parent: parent.map(|session| ParentRef {
            session,
            turn: turn(),
            tool_call: call(1),
        }),
        metadata,
    };
    let (alice, alice_too, bob, kid) = (session(0xa1), session(0xa2), session(0xa3), session(0xa4));
    let quoted = [("name", "a \"quoted\" 名")];
    for (session, metadata, parent) in [
        (alice, tenant("alice", &quoted), None),
        (alice_too, tenant("alice", &[]), None),
        (bob, tenant("bob", &quoted), None),
        (kid, tenant("alice", &[]), Some(alice)),
    ] {
        append(
            log,
            session,
            &created(metadata, parent),
            Some(dedup::session_created(session)),
        )
        .await;
    }
    let ids = |entries: Vec<CatalogEntry>| {
        let mut ids: Vec<SessionId> = entries.iter().map(|entry| entry.session).collect();
        ids.sort();
        ids
    };
    let query = |metadata, parent| SessionQuery {
        parent,
        metadata,
        ..SessionQuery::default()
    };

    assert_eq!(
        ids(list(log, query(tenant("alice", &[]), ParentFilter::Any)).await),
        vec![alice, alice_too, kid],
        "an equal pair matches; other keys do not matter"
    );
    assert_eq!(
        ids(list(log, query(tenant("bob", &[]), ParentFilter::Any)).await),
        vec![bob]
    );
    assert_eq!(
        ids(list(log, query(tenant("alice", &quoted), ParentFilter::Any)).await),
        vec![alice],
        "every pair must match"
    );
    assert_eq!(
        ids(list(log, query(tenant("alice", &[]), ParentFilter::Roots)).await),
        vec![alice, alice_too],
        "parent and metadata filters combine"
    );
    assert!(
        list(log, query(tenant("carol", &[]), ParentFilter::Any))
            .await
            .is_empty()
    );
    assert!(
        list(log, query(tenant("ali", &[]), ParentFilter::Any))
            .await
            .is_empty(),
        "equality, not prefix"
    );

    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let page = list(
            log,
            SessionQuery {
                metadata: tenant("alice", &[]),
                after,
                limit: Some(1),
                ..SessionQuery::default()
            },
        )
        .await;
        let Some(last) = page.last() else { break };
        after = Some(last.cursor());
        pages.extend(page);
    }
    assert_eq!(
        pages,
        list(log, query(tenant("alice", &[]), ParentFilter::Any)).await,
        "pages count only matching entries"
    );
}

async fn retire(
    log: &dyn SessionStore,
    session: SessionId,
    expected: LogId,
) -> Result<(), RetirementError> {
    log.retire(session, expected).await
}

async fn retirement(log: &dyn SessionStore) {
    // Neither tombstone nor live session: NotFound (or Unsupported).
    match retire(log, session(0x9f), LogId::from_i64(9)).await {
        Err(RetirementError::Unsupported) => return,
        Err(RetirementError::NotFound) => {}
        other => panic!("expected NotFound for an unknown session, got {other:?}"),
    }

    // R2/R3/R7: retiring hides the session everywhere; a retry with the
    // same frontier is a success; a different frontier reports the tombstone.
    let parent_id = session(0x90);
    let target = session(0x91);
    create(log, parent_id, None).await;
    create(log, target, Some(parent_id)).await;
    let captured = frontier(log, target).await.expect("frontier");
    retire(log, target, captured).await.expect("retire");
    assert!(frontier(log, target).await.is_none());
    assert!(scan(log, target).await.is_empty());
    assert!(!listed(log, ParentFilter::Any).await.contains(&target));
    assert_eq!(parent(log, target).await, None);
    assert!(
        listed(log, ParentFilter::ChildrenOf(parent_id))
            .await
            .is_empty()
    );
    // A retired id is never written again: appends fail instead of landing
    // in a log no read can see.
    match append_raw(
        log,
        entry_for(target, LOG_SCHEMA_VERSION, &input_recorded(), None),
    )
    .await
    {
        Err(StorageError::Retired { session }) => assert_eq!(session, target),
        other => panic!("expected Retired for an append after retirement, got {other:?}"),
    }
    assert!(scan(log, target).await.is_empty());
    retire(log, target, captured)
        .await
        .expect("same-frontier retry is idempotent");
    let moved = LogId::from_i64(captured.as_i64() + 1);
    assert!(
        matches!(
            retire(log, target, moved).await,
            Err(RetirementError::FrontierMoved { actual: Some(actual), .. }) if actual == captured
        ),
        "the tombstone outranks the live-session checks"
    );

    // R1: the frontier is a compare-and-set; the late record is not lost.
    let cas = session(0x92);
    create(log, cas, None).await;
    let exported = frontier(log, cas).await.expect("frontier");
    let late = append(log, cas, &input_recorded(), Some(DedupKey::new("late"))).await;
    assert!(
        matches!(
            retire(log, cas, exported).await,
            Err(RetirementError::FrontierMoved { expected, actual: Some(actual) })
                if expected == exported && actual == late
        ),
        "R1: a concurrent append fails the CAS"
    );
    assert_eq!(scan(log, cas).await.len(), 2, "R1: nothing is lost");

    // R4: NotIdle is checked before the frontier, and touches nothing.
    let busy = session(0x93);
    create(log, busy, None).await;
    append(log, busy, &turn_started(turn()), None).await;
    let current = frontier(log, busy).await.expect("frontier");
    assert!(matches!(
        retire(log, busy, LogId::from_i64(current.as_i64() + 100)).await,
        Err(RetirementError::NotIdle)
    ));
    assert_eq!(frontier(log, busy).await, Some(current));

    // An open compaction intent alone still counts as idle.
    let compacting = session(0x94);
    create(log, compacting, None).await;
    append(
        log,
        compacting,
        &compaction_started(),
        Some(dedup::compaction(LogId::from_i64(1))),
    )
    .await;
    let current = frontier(log, compacting).await.expect("frontier");
    retire(log, compacting, current)
        .await
        .expect("a dangling compaction does not block retirement");

    // R5: live children block the parent until retired bottom-up.
    let top = session(0x95);
    let leaf = session(0x96);
    create(log, top, None).await;
    create(log, leaf, Some(top)).await;
    let top_frontier = frontier(log, top).await.expect("frontier");
    let leaf_frontier = frontier(log, leaf).await.expect("frontier");
    assert!(matches!(
        retire(log, top, top_frontier).await,
        Err(RetirementError::HasChildren)
    ));
    retire(log, leaf, leaf_frontier).await.expect("leaf");
    retire(log, top, top_frontier).await.expect("top");

    // R6: a non-idle parent blocks the child; HasChildren outranks it.
    let busy_parent = session(0x97);
    let middle = session(0x98);
    let grandchild = session(0x99);
    create(log, busy_parent, None).await;
    append(log, busy_parent, &turn_started(turn()), None).await;
    create(log, middle, Some(busy_parent)).await;
    create(log, grandchild, Some(middle)).await;
    let middle_frontier = frontier(log, middle).await.expect("frontier");
    assert!(matches!(
        retire(log, middle, middle_frontier).await,
        Err(RetirementError::HasChildren)
    ));
    let grandchild_frontier = frontier(log, grandchild).await.expect("frontier");
    retire(log, grandchild, grandchild_frontier)
        .await
        .expect("an idle grandchild under an idle middle");
    assert!(matches!(
        retire(log, middle, middle_frontier).await,
        Err(RetirementError::ParentNotIdle)
    ));
    assert_eq!(scan(log, middle).await.len(), 1, "the child is untouched");
}

// --- ownership (ADR-0036) ---------------------------------------------------

async fn owned_append(
    log: &dyn SessionStore,
    session: SessionId,
    owner: OwnerToken,
) -> Result<LogId, StorageError> {
    let mut entry = entry_for(session, LOG_SCHEMA_VERSION, &input_recorded(), None);
    entry.owner = Some(owner);
    log.append(entry).await
}

async fn ownership(here: &dyn SessionStore, elsewhere: &dyn SessionStore) {
    let (mine, theirs) = (session(0xA1), session(0xA2));
    create(here, mine, None).await;
    create(here, theirs, None).await;
    assert_eq!(here.owner(mine).await.expect("owner"), None, "unclaimed");

    // Two processes hold different trees side by side.
    let claim = here.claim(mine).await.expect("claim a free tree");
    let token = claim.token();
    assert_eq!(token.root, mine);
    let other = elsewhere.claim(theirs).await.expect("claim another tree");
    owned_append(here, mine, token).await.expect("owner write");
    owned_append(elsewhere, theirs, other.token())
        .await
        .expect("owner write in the other process");

    // A held tree: shared through the same handle, refused elsewhere.
    let again = here.claim(mine).await.expect("re-claim through the holder");
    assert_eq!(again.token(), token, "the holder's claims share one epoch");
    let refused = elsewhere.claim(mine).await.expect_err("held elsewhere");
    assert!(
        matches!(refused, StorageError::OwnedElsewhere { root } if root == mine),
        "{refused:?}"
    );
    assert_eq!(elsewhere.owner(mine).await.expect("owner"), Some(token));
    assert_eq!(here.owner(mine).await.expect("owner"), Some(token));

    // External writes need no claim.
    append(elsewhere, mine, &input_recorded(), None).await;
    assert_eq!(scan(here, mine).await.len(), 3);

    // The tree stays held until its last claim drops.
    drop(claim);
    assert_eq!(elsewhere.owner(mine).await.expect("owner"), Some(token));
    drop(again);
    assert_eq!(elsewhere.owner(mine).await.expect("owner"), None);

    // A new owner fences the old epoch out.
    let taken = elsewhere.claim(mine).await.expect("claim a released tree");
    assert!(taken.token().epoch > token.epoch, "epochs grow per claim");
    let stale = owned_append(here, mine, token)
        .await
        .expect_err("a superseded epoch is rejected");
    assert!(
        matches!(stale, StorageError::OwnedElsewhere { root } if root == mine),
        "{stale:?}"
    );
    owned_append(elsewhere, mine, taken.token())
        .await
        .expect("the new owner writes");
    assert_eq!(
        scan(here, mine).await.len(),
        4,
        "the stale write left nothing"
    );
    drop((taken, other));
}

/// Past a lease written now: `DEFAULT_LEASE_TTL` and one millisecond.
fn lease_expiry() -> i64 {
    DEFAULT_LEASE_TTL.as_millis() as i64 + 1
}

fn owned_elsewhere(error: &StorageError, of: SessionId) -> bool {
    matches!(error, StorageError::OwnedElsewhere { root } if *root == of)
}

/// Liveness is a process lease renewed through the handle; a process that
/// stops renewing loses its trees to the next claimer once the lease
/// expires by the claimer's clock (ADR-0036).
async fn process_leases(
    here: &dyn SessionStore,
    elsewhere: &dyn SessionStore,
    clock: &ManualClock,
) {
    let (kept, released) = (session(0xB1), session(0xB2));
    for root in [kept, released] {
        create(elsewhere, root, None).await;
    }

    // Renewing keeps a holder alive past its first lease.
    let held = here.claim(kept).await.expect("claim");
    clock.advance(lease_expiry() / 2);
    assert_eq!(
        here.renew().await.expect("renew"),
        vec![],
        "nothing was taken"
    );
    clock.advance(lease_expiry() / 2 + 1);
    let refused = elsewhere
        .claim(kept)
        .await
        .expect_err("a renewed lease holds");
    assert!(owned_elsewhere(&refused, kept), "{refused:?}");
    assert_eq!(
        elsewhere.owner(kept).await.expect("owner"),
        Some(held.token())
    );

    // A holder that stops renewing is taken over once its lease expires.
    clock.advance(lease_expiry());
    assert_eq!(
        elsewhere.owner(kept).await.expect("owner"),
        None,
        "an expired lease owns nothing"
    );
    let taken = elsewhere
        .claim(kept)
        .await
        .expect("take over an expired lease");
    assert_eq!(taken.token().epoch, held.token().epoch + 1);
    let stale = owned_append(here, kept, held.token())
        .await
        .expect_err("the old holder's epoch is fenced");
    assert!(owned_elsewhere(&stale, kept), "{stale:?}");
    // Its next renewal reports the loss, and its claims are refused while
    // the new holder lives.
    assert_eq!(here.renew().await.expect("renew"), vec![held.token()]);
    assert_eq!(here.renew().await.expect("renew"), vec![], "reported once");
    let refused = here.claim(kept).await.expect_err("held by the new owner");
    assert!(owned_elsewhere(&refused, kept), "{refused:?}");
    drop(held);
    assert_eq!(
        elsewhere.owner(kept).await.expect("owner"),
        Some(taken.token())
    );
    owned_append(elsewhere, kept, taken.token())
        .await
        .expect("dropping the superseded claim released nothing");

    // A released epoch is fenced even before anyone claims again.
    let claim = elsewhere.claim(released).await.expect("claim");
    let token = claim.token();
    drop(claim);
    let stale = owned_append(elsewhere, released, token)
        .await
        .expect_err("a released claim no longer writes");
    assert!(owned_elsewhere(&stale, released), "{stale:?}");

    drop(taken);
}

/// A process that exits normally frees its trees at once, even with a claim
/// still alive; a killed one holds them until its lease expires.
async fn exit_and_kill(processes: impl Fn() -> [Arc<dyn SessionStore>; 2], clock: &ManualClock) {
    let root = session(0xC1);

    let [exiting, elsewhere] = processes();
    create(&*elsewhere, root, None).await;
    let claim = exiting.claim(root).await.expect("claim");
    drop(exiting);
    let after_exit = elsewhere
        .claim(root)
        .await
        .expect("claim after a normal exit");
    drop((claim, after_exit));

    let [killed, elsewhere] = processes();
    create(&*elsewhere, root, None).await;
    let claim = killed.claim(root).await.expect("claim");
    let token = claim.token();
    // Killed: no exit path runs and nothing renews.
    std::mem::forget((claim, killed));
    let refused = elsewhere.claim(root).await.expect_err("the lease is live");
    assert!(owned_elsewhere(&refused, root), "{refused:?}");
    clock.advance(lease_expiry());
    let taken = elsewhere
        .claim(root)
        .await
        .expect("claim after the lease expires");
    assert_eq!(taken.token().epoch, token.epoch + 1);
}
