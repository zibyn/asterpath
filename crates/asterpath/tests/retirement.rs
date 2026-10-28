//! Facade wiring for retirement through the one store (retention.md §3, §9).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use asterpath::protocol::{
    AgentError, CompactionConfig, ModelId, ModelSpec, Part, ProviderId, RequestConfig, SessionSpec,
    SubagentConfig, ToolSelection, TurnConfig,
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

/// A store with only the log and catalog keeps the default retirement.
struct NoRetirement(asterpath_testing::InMemoryEventLog);

impl SessionStore for NoRetirement {
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
}

#[tokio::test]
async fn injected_store_without_retirement_reports_unsupported() {
    let asterpath = Asterpath::builder()
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .store(Arc::new(NoRetirement(Default::default())))
        .build()
        .await
        .expect("facade builds with an injected store");
    let session = asterpath
        .create_session(spec("p"))
        .await
        .expect("create session")
        .id();
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

    asterpath.shutdown().await.expect("shutdown");
}
