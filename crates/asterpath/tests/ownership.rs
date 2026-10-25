//! Session-tree ownership through the facade (ADR-0036): opening is
//! read-only, driving needs a claim, and two `Asterpath`s over one SQLite
//! file behave like two processes sharing it.
#![cfg(feature = "sqlite")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath::protocol::{
    AgentEvent, ApprovalDecision, ApprovalPolicy, ApprovalRequest, ApprovalVerdict, CatalogEntry,
    Claim, CompactionConfig, DEFAULT_LEASE_TTL, Hooks, Item, JsonString, LogEntry, LogId, LogQuery,
    ModelId, ModelSpec, NewEntry, OwnerToken, Parallelism, Part, Provider, ProviderId,
    ReplaySafety, RequestConfig, Role, SessionId, SessionQuery, SessionSpec, SessionStore,
    SideEffect, StopReason, StorageError, SubagentConfig, Tool, ToolDescriptor, ToolName,
    ToolOutput, ToolSelection, TurnConfig, Usage, UserInput, scan_all,
};
use asterpath::{AsterError, Asterpath, OwnedSession};
use asterpath_testing::{InMemoryEventLog, ManualClock, ScriptedProvider, TestTool};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

struct TempDb(PathBuf);

impl TempDb {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "asterpath-ownership-{tag}-{}-{}",
            std::process::id(),
            NEXT_DB.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        Self(dir)
    }

    fn path(&self) -> PathBuf {
        self.0.join("db.sqlite")
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

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
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

fn input(id: u8) -> UserInput {
    UserInput {
        id: asterpath::protocol::InputId::from_bytes([id; 16]),
        parts: vec![text("go")],
    }
}

fn answer() -> ScriptedProvider {
    ScriptedProvider::new("p").respond(
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text("done")],
            raw: None,
        }],
        StopReason::EndTurn,
        Usage::default(),
    )
}

/// One "process": its own engine over the shared database file.
async fn process(db: &Path, provider: ScriptedProvider) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(vec![Arc::new(provider) as Arc<dyn Provider>])
        .build()
        .await
        .expect("build")
}

async fn drive_all(owned: &OwnedSession) -> Vec<AgentEvent> {
    let mut stream = owned.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive"));
    }
    events
}

fn owned_elsewhere(error: AsterError) -> bool {
    matches!(
        error,
        AsterError::Storage(StorageError::OwnedElsewhere { .. })
    )
}

#[tokio::test]
async fn two_processes_drive_different_trees_side_by_side() {
    let db = TempDb::new("parallel");
    let desktop = process(&db.path(), answer()).await;
    let cli = process(&db.path(), answer()).await;
    let first = desktop.create_session(spec()).await.unwrap();
    let second = cli.create_session(spec()).await.unwrap();
    first.submit_input(input(1)).await.unwrap();
    second.submit_input(input(2)).await.unwrap();

    let first = first.claim().await.expect("claim the first tree");
    let second = second.claim().await.expect("claim the second tree");
    let (a, b) = tokio::join!(drive_all(&first), drive_all(&second));
    assert!(matches!(a.last(), Some(AgentEvent::TurnCommitted { .. })));
    assert!(matches!(b.last(), Some(AgentEvent::TurnCommitted { .. })));
}

#[tokio::test]
async fn a_held_tree_is_read_only_elsewhere_until_released() {
    let db = TempDb::new("held");
    let desktop = process(&db.path(), answer()).await;
    let cli = process(&db.path(), answer()).await;
    let session = desktop.create_session(spec()).await.unwrap();
    session.submit_input(input(1)).await.unwrap();
    let owned = session.claim().await.expect("claim");
    let token = owned.owner();
    assert_eq!(token.root, session.id());

    // Same engine: claims share the tree.
    let shared = session.claim().await.expect("re-claim in-process");
    assert_eq!(shared.owner(), token);

    let elsewhere = cli.open_session(session.id()).await.expect("open");
    let refused = elsewhere.claim().await.expect_err("held by the desktop");
    assert!(owned_elsewhere(refused));
    assert_eq!(cli.owner(session.id()).await.unwrap(), Some(token));
    assert!(
        cli.wakeable().await.unwrap().is_empty(),
        "wake discovery skips trees owned elsewhere"
    );
    assert!(
        desktop.wakeable().await.unwrap().is_empty(),
        "and trees this process already owns"
    );
    // Reading still works.
    assert!(elsewhere.facts().await.unwrap().has_pending_input());

    drop((owned, shared));
    assert_eq!(cli.owner(session.id()).await.unwrap(), None);
    assert_eq!(cli.wakeable().await.unwrap(), vec![session.id()]);
    let taken = elsewhere.claim().await.expect("claim after release");
    assert!(taken.owner().epoch > token.epoch);
    let events = drive_all(&taken).await;
    assert!(matches!(
        events.last(),
        Some(AgentEvent::TurnCommitted { .. })
    ));
}

/// Classifies every call as `Ask`.
struct AlwaysAsk;

impl ApprovalPolicy for AlwaysAsk {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        ApprovalVerdict::Ask
    }
}

fn echo() -> Arc<dyn Tool> {
    let descriptor = ToolDescriptor::new(
        ToolName::new("echo"),
        String::new(),
        serde_json::json!({"type": "object"}),
    )
    .with_parallelism(Parallelism::Parallel)
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly);
    TestTool::new(descriptor, Arc::new(Mutex::new(Vec::new())), |_, _| {
        Box::pin(async { Ok(ToolOutput::new(vec![text("echoed")])) })
    })
}

fn calls_echo() -> ScriptedProvider {
    ScriptedProvider::new("p")
        .respond(
            vec![Item::ToolCall {
                call_id: "call-1".to_owned(),
                provider_item_id: None,
                name: ToolName::new("echo"),
                raw_args: JsonString::new("{}"),
                raw: None,
            }],
            StopReason::ToolUse,
            Usage::default(),
        )
        .respond(
            vec![Item::Message {
                role: Role::Assistant,
                parts: vec![text("done")],
                raw: None,
            }],
            StopReason::EndTurn,
            Usage::default(),
        )
}

/// A process whose every tool call asks.
async fn asking_process(db: &Path, provider: ScriptedProvider) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(vec![Arc::new(provider) as Arc<dyn Provider>])
        .tools(vec![echo()])
        .approval(Arc::new(AlwaysAsk))
        .build()
        .await
        .expect("build")
}

type Drive<'a> = BoxStream<'a, Result<AgentEvent, AsterError>>;

async fn until_approval(stream: &mut Drive<'_>) -> asterpath::ToolCallId {
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("approval is requested")
            .expect("event")
            .expect("no error");
        if let AgentEvent::ApprovalRequested(request) = event {
            return request.tool_call_id;
        }
    }
}

async fn last_event(stream: &mut Drive<'_>) -> Option<AgentEvent> {
    let mut last = None;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the drive ends")
    {
        last = Some(event.expect("drive"));
    }
    last
}

#[tokio::test]
async fn an_approval_decided_in_another_process_reaches_the_owner() {
    let db = TempDb::new("approval");
    let desktop = asking_process(&db.path(), calls_echo()).await;
    let cli = process(&db.path(), ScriptedProvider::new("p")).await;

    let session = desktop.create_session(spec()).await.unwrap();
    let _endpoint = desktop.approval_gate().register(session.id());
    session.submit_input(input(1)).await.unwrap();
    let owned = session.claim().await.unwrap();
    let mut stream = owned.drive();
    let call = until_approval(&mut stream).await;

    // The CLI writes the decision without owning the tree; the owner polls.
    let observer = cli.open_session(session.id()).await.unwrap();
    observer
        .approve(call, ApprovalDecision::Allow)
        .await
        .expect("a non-owner records the decision");
    assert!(matches!(
        last_event(&mut stream).await,
        Some(AgentEvent::TurnCommitted { .. })
    ));
}

#[tokio::test]
async fn a_tree_is_claimed_elsewhere_as_soon_as_its_owner_exits() {
    let db = TempDb::new("takeover");
    let desktop = asking_process(&db.path(), calls_echo()).await;
    let session = desktop.create_session(spec()).await.unwrap();
    let id = session.id();
    let endpoint = desktop.approval_gate().register(id);
    session.submit_input(input(1)).await.unwrap();
    let owned = session.claim().await.unwrap();
    let mut stream = owned.drive();
    let call = until_approval(&mut stream).await;
    // The desktop goes away mid-turn, its claim with it.
    drop(stream);
    drop((owned, session, endpoint, desktop));

    let cli = asking_process(&db.path(), answer()).await;
    let _endpoint = cli.approval_gate().register(id);
    assert_eq!(cli.wakeable().await.unwrap(), vec![id]);
    let owned = cli.open_session(id).await.unwrap().claim().await.unwrap();
    let mut stream = owned.drive();
    assert_eq!(
        until_approval(&mut stream).await,
        call,
        "recovery asks again for the open call"
    );
    owned.approve(call, ApprovalDecision::Allow).await.unwrap();
    assert!(matches!(
        last_event(&mut stream).await,
        Some(AgentEvent::TurnCommitted { .. })
    ));
}

/// Records the owner token of every append.
struct Recording {
    inner: Arc<dyn SessionStore>,
    owners: Mutex<Vec<(&'static str, Option<OwnerToken>)>>,
}

impl SessionStore for Recording {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        let kind = asterpath::protocol::decode_payload(&entry.payload, entry.schema_version)
            .expect("decodes")
            .kind();
        self.owners.lock().unwrap().push((kind, entry.owner));
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
}

#[tokio::test]
async fn owner_writes_carry_the_claim_and_external_writes_do_not() {
    let store = Arc::new(Recording {
        inner: Arc::new(InMemoryEventLog::new()),
        owners: Mutex::default(),
    });
    let asterpath = Asterpath::builder()
        .store(store.clone())
        .providers(vec![Arc::new(answer()) as Arc<dyn Provider>])
        .build()
        .await
        .expect("build");
    let session = asterpath.create_session(spec()).await.unwrap();
    session.submit_input(input(1)).await.unwrap();
    let owned = session.claim().await.unwrap();
    drive_all(&owned).await;

    let token = Some(owned.owner());
    let owners = store.owners.lock().unwrap().clone();
    assert_eq!(
        owners[..2],
        [("SessionCreated", None), ("InputRecorded", None)],
        "external writes"
    );
    assert!(owners.len() > 2);
    assert!(
        owners[2..].iter().all(|(_, owner)| *owner == token),
        "every drive write is fenced: {owners:?}"
    );
}

/// Past a lease written now.
const LEASE_EXPIRY: i64 = DEFAULT_LEASE_TTL.as_millis() as i64 + 1;

/// Every owner whose loss a process reported to its application.
#[derive(Default)]
struct LostOwners(Mutex<Vec<OwnerToken>>);

impl Hooks for LostOwners {
    fn on_ownership_lost(&self, owner: OwnerToken) {
        self.0.lock().unwrap().push(owner);
    }
}

/// A process over `db` whose store reads time from `clock`, whose every
/// tool call asks, and that never renews on its own during a test.
async fn clocked_process(
    db: &Path,
    provider: ScriptedProvider,
    clock: Arc<ManualClock>,
    lost: Arc<LostOwners>,
) -> Asterpath {
    let store = asterpath_storage_sqlite::open_with_clock(
        db,
        asterpath_storage_sqlite::OpenOptions::default(),
        clock,
    )
    .expect("open");
    Asterpath::builder()
        .store(store)
        .providers(vec![Arc::new(provider) as Arc<dyn Provider>])
        .tools(vec![echo()])
        .approval(Arc::new(AlwaysAsk))
        .hooks(lost)
        .lease_renew_interval(Duration::from_secs(3600))
        .build()
        .await
        .expect("build")
}

/// Every item of a drive, errors included, until it ends.
async fn drain(stream: &mut Drive<'_>) -> Vec<Result<AgentEvent, AsterError>> {
    let mut items = Vec::new();
    while let Some(item) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the drive ends")
    {
        items.push(item);
    }
    items
}

fn ends_owned_elsewhere(items: Vec<Result<AgentEvent, AsterError>>) -> bool {
    matches!(
        items.into_iter().last(),
        Some(Err(AsterError::Storage(
            StorageError::OwnedElsewhere { .. }
        )))
    )
}

#[tokio::test]
async fn a_killed_owners_tree_is_recovered_elsewhere_once_its_lease_expires() {
    let db = TempDb::new("killed");
    let clock = Arc::new(ManualClock::new());
    let desktop = clocked_process(&db.path(), calls_echo(), clock.clone(), Arc::default()).await;
    let session = desktop.create_session(spec()).await.unwrap();
    let id = session.id();
    let endpoint = desktop.approval_gate().register(id);
    session.submit_input(input(1)).await.unwrap();
    let owned = session.claim().await.unwrap();
    let mut stream = owned.drive();
    let call = until_approval(&mut stream).await;
    // Killed mid-turn: no exit path runs and nothing renews its lease.
    std::mem::forget(stream);
    std::mem::forget((owned, session, endpoint, desktop));

    let cli = clocked_process(&db.path(), answer(), clock.clone(), Arc::default()).await;
    let _endpoint = cli.approval_gate().register(id);
    let elsewhere = cli.open_session(id).await.unwrap();
    assert!(owned_elsewhere(
        elsewhere
            .claim()
            .await
            .expect_err("the dead owner's lease is live")
    ));
    assert!(cli.wakeable().await.unwrap().is_empty());

    clock.advance(LEASE_EXPIRY);
    assert_eq!(cli.wakeable().await.unwrap(), vec![id]);
    let owned = elsewhere
        .claim()
        .await
        .expect("claim after the lease expires");
    let mut stream = owned.drive();
    assert_eq!(
        until_approval(&mut stream).await,
        call,
        "recovery asks again for the open call"
    );
    owned.approve(call, ApprovalDecision::Allow).await.unwrap();
    assert!(matches!(
        last_event(&mut stream).await,
        Some(AgentEvent::TurnCommitted { .. })
    ));
}

#[tokio::test]
async fn an_owner_whose_tree_was_taken_over_stops_and_tells_the_application() {
    let db = TempDb::new("taken-over");
    let clock = Arc::new(ManualClock::new());
    let lost = Arc::new(LostOwners::default());
    let desktop = clocked_process(&db.path(), calls_echo(), clock.clone(), lost.clone()).await;
    let session = desktop.create_session(spec()).await.unwrap();
    let id = session.id();
    let _endpoint = desktop.approval_gate().register(id);
    session.submit_input(input(1)).await.unwrap();
    let owned = session.claim().await.unwrap();
    let mut stream = owned.drive();
    let call = until_approval(&mut stream).await;

    // The desktop stalls past its lease and the CLI takes the tree.
    clock.advance(LEASE_EXPIRY);
    let cli = clocked_process(&db.path(), answer(), clock.clone(), Arc::default()).await;
    let taken = cli.open_session(id).await.unwrap().claim().await.unwrap();
    assert_eq!(taken.owner().epoch, owned.owner().epoch + 1);
    taken.approve(call, ApprovalDecision::Allow).await.unwrap();
    let records = scan_all(&*cli.store(), id).await.unwrap().len();

    // The desktop sees the decision; its next write is refused, and it stops.
    assert!(ends_owned_elsewhere(drain(&mut stream).await));
    assert_eq!(*lost.0.lock().unwrap(), vec![owned.owner()]);
    assert!(ends_owned_elsewhere(drain(&mut owned.drive()).await));
    assert_eq!(
        scan_all(&*cli.store(), id).await.unwrap().len(),
        records,
        "the old owner wrote nothing after the takeover"
    );
}

/// A store whose lease renewal fails once `failing` is set.
struct Unrenewable {
    inner: Arc<dyn SessionStore>,
    failing: AtomicBool,
}

impl SessionStore for Unrenewable {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
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
        if self.failing.load(Ordering::SeqCst) {
            return Box::pin(async { Err(StorageError::Backend("unreachable".to_owned())) });
        }
        self.inner.renew()
    }
}

#[tokio::test]
async fn an_owner_that_cannot_renew_stops_and_tells_the_application() {
    let store = Arc::new(Unrenewable {
        inner: Arc::new(InMemoryEventLog::new()),
        failing: AtomicBool::new(false),
    });
    let lost = Arc::new(LostOwners::default());
    let asterpath = Asterpath::builder()
        .store(store.clone())
        .providers(vec![Arc::new(calls_echo()) as Arc<dyn Provider>])
        .tools(vec![echo()])
        .approval(Arc::new(AlwaysAsk))
        .hooks(lost.clone())
        .lease_renew_interval(Duration::from_millis(10))
        .build()
        .await
        .expect("build");
    let session = asterpath.create_session(spec()).await.unwrap();
    let _endpoint = asterpath.approval_gate().register(session.id());
    session.submit_input(input(1)).await.unwrap();
    let owned = session.claim().await.unwrap();
    let mut stream = owned.drive();
    until_approval(&mut stream).await;
    let records = scan_all(&*store, session.id()).await.unwrap().len();

    store.failing.store(true, Ordering::SeqCst);
    assert!(ends_owned_elsewhere(drain(&mut stream).await));
    assert_eq!(*lost.0.lock().unwrap(), vec![owned.owner()]);
    assert!(ends_owned_elsewhere(drain(&mut owned.drive()).await));
    assert_eq!(
        scan_all(&*store, session.id()).await.unwrap().len(),
        records
    );
}
