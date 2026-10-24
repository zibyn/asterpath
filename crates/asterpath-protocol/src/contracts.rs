use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;

use crate::agent::ApprovalRequest;
use crate::error::{RetirementError, StorageError};
use crate::ids::{LogId, SessionId};
use crate::time::Timestamp;

/// Key used to make a log append idempotent within one session stream.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct DedupKey(String);

impl DedupKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DedupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DedupKey({:?})", self.0)
    }
}

/// A record submitted to the event log. The storage layer assigns id, seq and time.
#[derive(Clone, Debug, PartialEq)]
pub struct NewEntry {
    pub session: SessionId,
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: serde_json::Value,
}

/// A committed log record as returned by the storage layer.
#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub log_id: LogId,
    pub session: SessionId,
    pub seq: u64,
    /// Diagnostic wall-clock time written by the backend; never used by recovery.
    pub recorded_at: Timestamp,
    pub schema_version: u16,
    pub dedup_key: Option<DedupKey>,
    pub payload: serde_json::Value,
}

/// Cursor-based scan request.
#[derive(Clone, Debug, PartialEq)]
pub struct LogQuery {
    pub session: SessionId,
    /// Exclusive cursor.
    pub after: Option<LogId>,
    pub limit: Option<usize>,
}

/// Which sessions a catalog query lists, by parent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ParentFilter {
    #[default]
    Any,
    Roots,
    ChildrenOf(SessionId),
}

/// Position in the catalog order: the last entry of the previous page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CatalogCursor {
    pub created_at: Timestamp,
    pub session: SessionId,
}

/// A page request over the session catalog (ADR-0028).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionQuery {
    pub parent: ParentFilter,
    /// Exclusive cursor: the page starts after this entry.
    pub after: Option<CatalogCursor>,
    pub limit: Option<usize>,
}

/// One live session in the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CatalogEntry {
    pub session: SessionId,
    pub parent: Option<SessionId>,
    /// `recorded_at` of the session's `SessionCreated`.
    pub created_at: Timestamp,
    /// `recorded_at` of the session's last committed record; display only,
    /// recovery and wake never read it.
    pub last_activity: Timestamp,
    /// The host metadata recorded in `SessionCreated` (ADR-0032).
    pub metadata: BTreeMap<String, String>,
}

impl CatalogEntry {
    /// For store adapters, which build entries outside this crate.
    pub fn new(
        session: SessionId,
        parent: Option<SessionId>,
        created_at: Timestamp,
        last_activity: Timestamp,
        metadata: BTreeMap<String, String>,
    ) -> Self {
        Self {
            session,
            parent,
            created_at,
            last_activity,
            metadata,
        }
    }

    pub fn cursor(&self) -> CatalogCursor {
        CatalogCursor {
            created_at: self.created_at,
            session: self.session,
        }
    }
}

/// The one storage adapter (ADR-0023): the append-only per-session event log
/// plus the session catalog (`sessions` / `parent`) and optional
/// whole-session retirement. Retired sessions are invisible to every read.
pub trait SessionStore: Send + Sync {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;

    /// Sessions that have a `SessionCreated` and are not retired, filtered by
    /// parent, ordered by `(created_at, session)` descending (ADR-0028).
    fn sessions(
        &self,
        query: SessionQuery,
    ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>>;
    /// The parent recorded in `SessionCreated`; `None` for a root or unknown
    /// session.
    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>>;

    /// Write the tombstone and delete the session's log and catalog row.
    /// `expected_frontier` must equal the current last log id. Backends
    /// without retirement keep the default `Unsupported`.
    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>> {
        let _ = (session, expected_frontier);
        Box::pin(async { Err(RetirementError::Unsupported) })
    }
}

/// Every committed record of `session`, in `seq` order: the one whole-session
/// read shared by the engine and its hosts.
pub async fn scan_all(
    log: &dyn SessionStore,
    session: SessionId,
) -> Result<Vec<LogEntry>, StorageError> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        entries.push(entry?);
    }
    Ok(entries)
}

/// Liveness declaration for approval responders, plus the requests waiting
/// for a decision in this process (ADR-0028). Nothing here is durable:
/// recovery re-emits `ApprovalRequested` after a restart.
#[derive(Clone, Debug, Default)]
pub struct ApprovalGate {
    open: Arc<AtomicUsize>,
    pending: Arc<Mutex<PendingRequests>>,
}

#[derive(Debug, Default)]
struct PendingRequests {
    next: u64,
    waiting: BTreeMap<u64, ApprovalRequest>,
}

impl ApprovalGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns an RAII endpoint; dropping it closes that registration.
    pub fn register(&self) -> ApprovalEndpoint {
        self.open.fetch_add(1, Ordering::SeqCst);
        ApprovalEndpoint {
            open: Arc::clone(&self.open),
        }
    }

    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::SeqCst) > 0
    }

    /// Every request still waiting for a decision, across all sessions, in
    /// the order they were requested.
    pub fn pending(&self) -> Vec<ApprovalRequest> {
        self.lock().waiting.values().cloned().collect()
    }

    /// Lists `request` as pending until the returned guard drops, however the
    /// wait ends: a decision, a cancel or a dropped drive stream.
    // Hidden: only the engine's tool pipeline (another crate) lists requests.
    #[doc(hidden)]
    pub fn wait_for(&self, request: ApprovalRequest) -> PendingApproval {
        let mut pending = self.lock();
        let key = pending.next;
        pending.next += 1;
        pending.waiting.insert(key, request);
        PendingApproval {
            pending: Arc::clone(&self.pending),
            key,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PendingRequests> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// RAII listing of one waiting request; see `ApprovalGate::wait_for`.
// Hidden: engine-internal, public only for cross-crate use by the pipeline.
#[doc(hidden)]
#[derive(Debug)]
pub struct PendingApproval {
    pending: Arc<Mutex<PendingRequests>>,
    key: u64,
}

impl Drop for PendingApproval {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .waiting
            .remove(&self.key);
    }
}

#[derive(Debug)]
pub struct ApprovalEndpoint {
    open: Arc<AtomicUsize>,
}

impl Drop for ApprovalEndpoint {
    fn drop(&mut self) {
        self.open.fetch_sub(1, Ordering::SeqCst);
    }
}
