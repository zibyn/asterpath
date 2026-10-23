use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;

use crate::error::{RetirementError, StorageError};
use crate::ids::{LogId, SessionId, SignalId};
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

/// Name of a durable signal within a session.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SignalName(String);

impl SignalName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SignalName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SignalName({:?})", self.0)
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

/// The one storage adapter (ADR-0023): the append-only per-session event log
/// plus the session catalog (`sessions` / `parent` / `children`) and optional
/// whole-session retirement. Retired sessions are invisible to every read.
pub trait SessionStore: Send + Sync {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;

    /// Every session that has a `SessionCreated` and is not retired,
    /// children included, ordered by id.
    fn sessions(&self) -> BoxFuture<'_, Result<Vec<SessionId>, StorageError>>;
    /// The parent recorded in `SessionCreated`; `None` for a root or unknown
    /// session.
    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>>;
    /// Live children of `session`, ordered by id.
    fn children(&self, session: SessionId) -> BoxStream<'_, Result<SessionId, StorageError>>;

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

pub type SignalPayload = serde_json::Value;

#[derive(Clone, Debug)]
pub struct SignalDelivery {
    pub session: SessionId,
    pub signal: SignalId,
    pub name: SignalName,
    pub payload: SignalPayload,
}

#[derive(Clone, Debug)]
pub struct AwaitSignal {
    pub session: SessionId,
    pub signal: SignalId,
    pub name: SignalName,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeCause {
    Signal(SignalId),
    ChildCompleted(SessionId),
}

/// Liveness declaration for approval responders: the kernel mechanism only
/// tells the pipeline whether anyone can answer.
#[derive(Clone, Debug, Default)]
pub struct ApprovalGate {
    open: Arc<AtomicUsize>,
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
