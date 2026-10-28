//! Facade wiring for retirement through the one store (retention.md §3, §9).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use asterpath::protocol::{
    AgentError, CompactionConfig, ModelId, ModelSpec, ParentRef, Part, ProviderId, RequestConfig,
    SessionEvent, SessionSpec, SubagentConfig, ToolCallId, ToolSelection, TurnConfig, TurnId,
    UsageError, UsageScope, dedup, encode_entry,
};
use asterpath::protocol::{LogId, RetirementError, SessionStore};
use asterpath::protocol::{LogQuery, SessionId};
use asterpath::{AsterError, Asterpath};
use asterpath_testing::ScriptedProvider;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-retirement-{tag}-{}-{}.db",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_file(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}-wal", path.display()));
    let _ = fs::remove_file(format!("{}-shm", path.display()));
    let _ = fs::remove_file(format!("{}.lock", path.display()));
}

fn spec(provider: &str) -> SessionSpec {
    SessionSpec {
        system_sections: Vec::new(),
        model: ModelSpec {
            provider: ProviderId::new(provider),
            model: ModelId::new("model-1"),
            context_window: Some(2_000),
        },
        system: vec![Part::Text {
            text: "be helpful".to_owned(),
        }],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

async fn scan_len(asterpath: &Asterpath, session: SessionId) -> usize {
    let log = asterpath.store();
    let mut entries = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut count = 0;
    while let Some(entry) = entries.next().await {
        entry.expect("scan succeeds");
        count += 1;
    }
    count
}

#[tokio::test]
async fn sqlite_exposes_retirement_and_hides_the_retired_session() {
    let db = temp_db("sqlite");
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .build()
        .await
        .expect("facade builds");
    let handle = asterpath
        .create_session(spec("p"))
        .await
        .expect("create session");
    let session = handle.id();

    let retirement = asterpath.store();
    let frontier = asterpath
        .store()
        .frontier(session)
        .await
        .expect("frontier")
        .expect("session exists");
    retirement.retire(session, frontier).await.expect("retire");

    assert!(
        asterpath
            .store()
            .frontier(session)
            .await
            .expect("frontier")
            .is_none()
    );
    assert_eq!(scan_len(&asterpath, session).await, 0);
    match asterpath.open_session(session).await {
        Err(AsterError::Agent(AgentError::NotFound { session: found })) => {
            assert_eq!(found, session);
        }
        other => panic!("expected NotFound after retirement, got {other:?}"),
    }

    asterpath.shutdown().await.expect("shutdown");
    cleanup(&db);
}

/// A child of `parent`, recorded as the subagent scheduler records one.
async fn child(asterpath: &Asterpath, parent: SessionId, byte: u8) -> SessionId {
    let child = SessionId::from_bytes([byte; 16]);
    let created = SessionEvent::SessionCreated {
        config: spec("p").snapshot(vec![]),
        parent: Some(ParentRef {
            session: parent,
            turn: TurnId::from_bytes([byte; 16]),
            tool_call: ToolCallId::from_bytes([byte; 16]),
        }),
        metadata: Default::default(),
    };
    let entry = encode_entry(child, Some(dedup::session_created(child)), &created).expect("encode");
    asterpath.store().append(entry).await.expect("append child");
    child
}

async fn assert_gone(asterpath: &Asterpath, session: SessionId) {
    assert!(
        matches!(
            asterpath.open_session(session).await,
            Err(AsterError::Agent(AgentError::NotFound { .. }))
        ),
        "{session:?} still opens"
    );
    assert!(
        !asterpath
            .sessions(Default::default())
            .await
            .expect("catalog")
            .iter()
            .any(|entry| entry.session == session),
        "{session:?} still listed"
    );
    assert!(matches!(
        asterpath.usage(UsageScope::Session(session)).await,
        Err(UsageError::NotFound { .. })
    ));
}

#[tokio::test]
async fn retire_removes_a_whole_idle_tree_deepest_first() {
    let db = temp_db("tree");
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .build()
        .await
        .expect("facade builds");
    let root = asterpath.create_session(spec("p")).await.expect("create");
    let middle = child(&asterpath, root.id(), 0xa1).await;
    let leaf = child(&asterpath, middle, 0xa2).await;
    let sibling = child(&asterpath, root.id(), 0xa3).await;

    // From any session of the tree, the whole tree goes.
    let owned = asterpath
        .open_session(leaf)
        .await
        .expect("open leaf")
        .claim()
        .await
        .expect("claim");
    owned.retire().await.expect("retire tree");

    for session in [root.id(), middle, leaf, sibling] {
        assert_gone(&asterpath, session).await;
    }

    asterpath.shutdown().await.expect("shutdown");
    cleanup(&db);
}

#[tokio::test]
async fn retire_refuses_a_busy_tree_before_retiring_anything() {
    let db = temp_db("busy");
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .build()
        .await
        .expect("facade builds");
    let root = asterpath.create_session(spec("p")).await.expect("create");
    // The deepest session, retired first, is idle; a shallower one is not.
    let idle = child(&asterpath, root.id(), 0xb1).await;
    let deepest = child(&asterpath, idle, 0xb4).await;
    let busy = child(&asterpath, root.id(), 0xb2).await;
    let started = SessionEvent::TurnStarted {
        turn_id: TurnId::from_bytes([0xb3; 16]),
    };
    let entry = encode_entry(busy, None, &started).expect("encode");
    asterpath.store().append(entry).await.expect("append");

    let error = root
        .claim()
        .await
        .expect("claim")
        .retire()
        .await
        .expect_err("a turn is open");
    assert!(matches!(error, RetirementError::NotIdle), "{error:?}");
    for session in [root.id(), idle, deepest, busy] {
        asterpath.open_session(session).await.expect("untouched");
    }

    asterpath.shutdown().await.expect("shutdown");
    cleanup(&db);
}

#[tokio::test]
async fn retire_interrupted_midway_leaves_the_root_whole_and_a_retry_finishes() {
    let store = Arc::new(Retirements::default());
    store.allow(1);
    let asterpath = Asterpath::builder()
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .store(store.clone())
        .build()
        .await
        .expect("facade builds");
    let root = asterpath.create_session(spec("p")).await.expect("create");
    let first = child(&asterpath, root.id(), 0xc1).await;
    let second = child(&asterpath, root.id(), 0xc2).await;

    let owned = root.claim().await.expect("claim");
    owned
        .clone()
        .retire()
        .await
        .expect_err("interrupted after one child");
    let mut left = 0;
    for session in [first, second] {
        left += usize::from(asterpath.open_session(session).await.is_ok());
    }
    assert_eq!(left, 1, "exactly one child retired");
    let reopened = asterpath.open_session(root.id()).await.expect("root opens");
    reopened.facts().await.expect("root reads");

    store.allow(usize::MAX);
    owned.retire().await.expect("the retry finishes");
    for session in [root.id(), first, second] {
        assert_gone(&asterpath, session).await;
    }
}

#[tokio::test]
async fn open_session_reports_unknown_sessions_as_not_found() {
    let db = temp_db("unknown");
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .build()
        .await
        .expect("facade builds");

    let unknown = SessionId::from_bytes([0xee; 16]);
    match asterpath.open_session(unknown).await {
        Err(AsterError::Agent(AgentError::NotFound { session })) => {
            assert_eq!(session, unknown);
        }
        other => panic!("expected NotFound, got {other:?}"),
    }

    asterpath.shutdown().await.expect("shutdown");
    cleanup(&db);
}

/// An in-memory store that retires only as often as allowed, then reports
/// `Unsupported`: the default retirement, or a crash between retirements.
#[derive(Default)]
struct Retirements(asterpath_testing::InMemoryEventLog, AtomicU64);

impl Retirements {
    fn allow(&self, times: usize) {
        self.1.store(times as u64, Ordering::SeqCst);
    }
}

impl SessionStore for Retirements {
    fn append(
        &self,
        entry: asterpath::protocol::NewEntry,
    ) -> futures_core::future::BoxFuture<'_, Result<LogId, asterpath::protocol::StorageError>> {
        self.0.append(entry)
    }

    fn scan(
        &self,
        query: LogQuery,
    ) -> futures_core::stream::BoxStream<
        '_,
        Result<asterpath::protocol::LogEntry, asterpath::protocol::StorageError>,
    > {
        self.0.scan(query)
    }

    fn frontier(
        &self,
        session: SessionId,
    ) -> futures_core::future::BoxFuture<'_, Result<Option<LogId>, asterpath::protocol::StorageError>>
    {
        self.0.frontier(session)
    }

    fn sessions(
        &self,
        query: asterpath::protocol::SessionQuery,
    ) -> futures_core::future::BoxFuture<
        '_,
        Result<Vec<asterpath::protocol::CatalogEntry>, asterpath::protocol::StorageError>,
    > {
        self.0.sessions(query)
    }

    fn parent(
        &self,
        session: SessionId,
    ) -> futures_core::future::BoxFuture<
        '_,
        Result<Option<SessionId>, asterpath::protocol::StorageError>,
    > {
        self.0.parent(session)
    }

    fn claim(
        &self,
        root: SessionId,
    ) -> futures_core::future::BoxFuture<
        '_,
        Result<asterpath::protocol::Claim, asterpath::protocol::StorageError>,
    > {
        self.0.claim(root)
    }

    fn owner(
        &self,
        root: SessionId,
    ) -> futures_core::future::BoxFuture<
        '_,
        Result<Option<asterpath::protocol::OwnerToken>, asterpath::protocol::StorageError>,
    > {
        self.0.owner(root)
    }

    fn renew(
        &self,
    ) -> futures_core::future::BoxFuture<
        '_,
        Result<Vec<asterpath::protocol::OwnerToken>, asterpath::protocol::StorageError>,
    > {
        self.0.renew()
    }

    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> futures_core::future::BoxFuture<'_, Result<(), RetirementError>> {
        let allowed = self
            .1
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if allowed {
            self.0.retire(session, expected_frontier)
        } else {
            Box::pin(async { Err(RetirementError::Unsupported) })
        }
    }
}

#[tokio::test]
async fn injected_store_without_retirement_reports_unsupported() {
    let asterpath = Asterpath::builder()
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .store(Arc::new(Retirements::default()))
        .build()
        .await
        .expect("facade builds with an injected store");
    let handle = asterpath
        .create_session(spec("p"))
        .await
        .expect("create session");
    let session = handle.id();
    let frontier = asterpath
        .store()
        .frontier(session)
        .await
        .expect("frontier")
        .expect("session exists");

    let error = asterpath
        .store()
        .retire(session, frontier)
        .await
        .expect_err("no retirement");
    assert!(
        matches!(error, RetirementError::Unsupported),
        "custom stores may legitimately not implement retirement: {error:?}"
    );
    let error = handle
        .claim()
        .await
        .expect("claim")
        .retire()
        .await
        .expect_err("no retirement through the facade either");
    assert!(matches!(error, RetirementError::Unsupported), "{error:?}");

    asterpath.shutdown().await.expect("shutdown");
}
