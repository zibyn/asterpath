use std::collections::BTreeMap;
use std::fmt;
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
    /// Set on owner writes (agent loop, compaction, child scheduling): the
    /// store rejects the entry with `OwnedElsewhere` unless `epoch` is the
    /// claim of `root` this handle still holds, checked in the write's own
    /// transaction. External writes (input, approval
    /// decisions, config changes) leave it `None` (ADR-0036).
    pub owner: Option<OwnerToken>,
    pub payload: serde_json::Value,
}

/// How long a process lease lasts unless a backend is configured otherwise:
/// a process killed while holding trees keeps them this long (ADR-0036).
pub const DEFAULT_LEASE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// The credential of one claim of a session tree: the root session id and
/// the epoch the claim was granted, which every new claim of the root
/// increments (ADR-0036).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OwnerToken {
    pub root: SessionId,
    pub epoch: u64,
}

/// Ownership of a session tree held through one store handle; dropping it
/// releases this claim. Claims of one root through one handle share the
/// ownership and its epoch; the tree is released when the last one drops.
pub struct Claim {
    token: OwnerToken,
    _held: Box<dyn std::any::Any + Send + Sync>,
}

impl Claim {
    /// For store adapters: dropping `held` releases the claim.
    pub fn new(token: OwnerToken, held: impl std::any::Any + Send + Sync) -> Self {
        Self {
            token,
            _held: Box::new(held),
        }
    }

    pub fn token(&self) -> OwnerToken {
        self.token
    }
}

impl fmt::Debug for Claim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Claim").field(&self.token).finish()
    }
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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionQuery {
    pub parent: ParentFilter,
    /// Host metadata every listed entry carries: each key equals its value
    /// (ADR-0039). Empty matches all.
    pub metadata: BTreeMap<String, String>,
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
///
/// Ownership is per session tree, keyed by the root session id (ADR-0036):
/// at most one process holds a root at a time. A store handle stands for one
/// process and keeps a process lease in the store; its claims live while the
/// lease does, judged by the reader's clock. Everything is plain reads and
/// conditional writes, no backend-native lock. Dropping the handle (a normal
/// exit) clears its lease and frees its trees.
pub trait SessionStore: Send + Sync {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;

    /// Sessions that have a `SessionCreated` and are not retired, filtered by
    /// parent and metadata, ordered by `(created_at, session)` descending (ADR-0028).
    fn sessions(
        &self,
        query: SessionQuery,
    ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>>;
    /// The parent recorded in `SessionCreated`; `None` for a root or unknown
    /// session.
    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>>;

    /// Take ownership of the tree rooted at `root`, renewing this handle's
    /// lease. Held through this handle already: the same epoch, shared.
    /// Held through another handle whose lease is live: `OwnedElsewhere`.
    /// Otherwise a new, higher epoch. Dropping the last claim of the epoch
    /// releases the tree.
    fn claim(&self, root: SessionId) -> BoxFuture<'_, Result<Claim, StorageError>>;
    /// The live claim of `root` through any handle or process; `None` when
    /// nobody holds it or its holder's lease has expired.
    fn owner(&self, root: SessionId) -> BoxFuture<'_, Result<Option<OwnerToken>, StorageError>>;
    /// Extend this handle's lease to `DEFAULT_LEASE_TTL` (or the backend's
    /// configured TTL) from now. Returns the claims through this handle that
    /// another process has taken over since, each once; they no longer
    /// share with a new claim.
    fn renew(&self) -> BoxFuture<'_, Result<Vec<OwnerToken>, StorageError>>;

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

/// Longest parent chain followed to a root; the scheduler caps nesting lower.
const MAX_PARENT_CHAIN: usize = 64;

/// The root of `session`'s tree: the key it is owned and attended by
/// (ADR-0036, ADR-0039). An unknown session is its own root.
pub async fn root_of(
    log: &dyn SessionStore,
    session: SessionId,
) -> Result<SessionId, StorageError> {
    let mut root = session;
    for _ in 0..MAX_PARENT_CHAIN {
        match log.parent(root).await? {
            Some(parent) => root = parent,
            None => return Ok(root),
        }
    }
    Err(StorageError::Corrupt {
        session: Some(session),
        detail: "session parent chain is cyclic".to_owned(),
    })
}

/// Liveness declarations of approval responders, one per session tree, plus
/// the requests waiting for a decision in this process (ADR-0028, ADR-0039).
/// A tree is keyed by its root session id. Nothing here is durable:
/// recovery re-emits `ApprovalRequested` after a restart.
#[derive(Clone, Debug, Default)]
pub struct ApprovalGate {
    open: Arc<Mutex<BTreeMap<SessionId, usize>>>,
    pending: Arc<Mutex<PendingRequests>>,
}

#[derive(Debug, Default)]
struct PendingRequests {
    next: u64,
    /// Each request with the root of its session's tree.
    waiting: BTreeMap<u64, (SessionId, ApprovalRequest)>,
}

impl ApprovalGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attend the tree rooted at `root` until the returned endpoint drops.
    pub fn register(&self, root: SessionId) -> ApprovalEndpoint {
        *lock(&self.open).entry(root).or_default() += 1;
        ApprovalEndpoint {
            open: Arc::clone(&self.open),
            root,
        }
    }

    /// Whether any endpoint attends the tree rooted at `root`; endpoints of
    /// other trees do not count.
    pub fn is_open(&self, root: SessionId) -> bool {
        lock(&self.open).contains_key(&root)
    }

    /// Every request still waiting for a decision, across all trees, in the
    /// order they were requested.
    pub fn pending(&self) -> Vec<ApprovalRequest> {
        lock(&self.pending)
            .waiting
            .values()
            .map(|(_, request)| request.clone())
            .collect()
    }

    /// The waiting requests of the tree rooted at `root` (its children's
    /// included), in the order they were requested.
    pub fn pending_in(&self, root: SessionId) -> Vec<ApprovalRequest> {
        lock(&self.pending)
            .waiting
            .values()
            .filter(|(tree, _)| *tree == root)
            .map(|(_, request)| request.clone())
            .collect()
    }

    /// Lists `request`, from the tree rooted at `root`, as pending until the
    /// returned guard drops, however the wait ends: a decision, a cancel or
    /// a dropped drive stream.
    // Hidden: only the engine's tool pipeline (another crate) lists requests.
    #[doc(hidden)]
    pub fn wait_for(&self, root: SessionId, request: ApprovalRequest) -> PendingApproval {
        let mut pending = lock(&self.pending);
        let key = pending.next;
        pending.next += 1;
        pending.waiting.insert(key, (root, request));
        PendingApproval {
            pending: Arc::clone(&self.pending),
            key,
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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
        lock(&self.pending).waiting.remove(&self.key);
    }
}

/// One registration on a tree; see [`ApprovalGate::register`].
#[derive(Debug)]
pub struct ApprovalEndpoint {
    open: Arc<Mutex<BTreeMap<SessionId, usize>>>,
    root: SessionId,
}

impl Drop for ApprovalEndpoint {
    fn drop(&mut self) {
        let mut open = lock(&self.open);
        if let Some(count) = open.get_mut(&self.root) {
            *count -= 1;
            if *count == 0 {
                open.remove(&self.root);
            }
        }
    }
}
