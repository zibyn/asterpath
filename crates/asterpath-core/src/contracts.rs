use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;

use crate::error::{ExecutionError, RetirementError, StorageError};
use crate::ids::{LogId, SessionId, SignalId, TimerId};
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

/// Append-only, per-session event log contract.
pub trait EventLog: Send + Sync {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>>;
    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>>;
    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>>;
}

/// A timer that is due at the given time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueTimer {
    pub timer_id: TimerId,
    pub session: SessionId,
    pub deadline: Timestamp,
}

/// Wake discovery contract: read-only, no queue and no lease.
pub trait Storage: Send + Sync {
    /// Sessions that need to be driven: non-compaction open intents, pending
    /// inputs or due timers.
    fn wakeable(&self, now: Timestamp) -> BoxFuture<'_, Result<Vec<SessionId>, StorageError>>;

    fn due_timers(
        &self,
        now: Timestamp,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<DueTimer>, StorageError>>;
}

/// Read-only view of the session tree.
pub trait SessionCatalog: Send + Sync {
    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>>;

    fn children(&self, session: SessionId) -> BoxStream<'_, Result<SessionId, StorageError>>;
}

/// Optional whole-session retirement capability.
pub trait SessionRetirement: Send + Sync {
    /// Write the tombstone and delete the log and every derived row.
    /// `expected_frontier` must equal the current last log id.
    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>>;
}

pub struct TimerRequest {
    pub session: SessionId,
    pub timer: TimerId,
    pub caused_by: LogId,
    pub deadline: Timestamp,
}

pub type SignalPayload = serde_json::Value;

pub struct SignalDelivery {
    pub session: SessionId,
    pub signal: SignalId,
    pub name: SignalName,
    pub payload: SignalPayload,
}

pub struct AwaitSignal {
    pub session: SessionId,
    pub signal: SignalId,
    pub name: SignalName,
    pub caused_by: LogId,
    /// `Record` writes `SignalAwaited`; `AlreadyRecorded` is used by approval,
    /// whose open `ToolCallRequested` already records the intent.
    pub intent: SignalIntent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalIntent {
    Record,
    AlreadyRecorded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeCause {
    Timer(TimerId),
    Signal(SignalId),
    ChildCompleted(SessionId),
    /// Only used by the host's startup recovery loop.
    Startup,
}

/// Durable wait seam: timers, signals and wake notifications.
pub trait DurableExecutor: Send + Sync {
    fn schedule_timer(&self, req: TimerRequest) -> BoxFuture<'_, Result<TimerId, ExecutionError>>;

    fn cancel_timer(
        &self,
        session: SessionId,
        timer: TimerId,
    ) -> BoxFuture<'_, Result<(), ExecutionError>>;

    fn await_signal(
        &self,
        req: AwaitSignal,
    ) -> BoxFuture<'_, Result<SignalPayload, ExecutionError>>;

    fn deliver_signal(&self, delivery: SignalDelivery)
    -> BoxFuture<'_, Result<(), ExecutionError>>;

    fn await_wake(
        &self,
        session: SessionId,
        cause: WakeCause,
    ) -> BoxFuture<'_, Result<(), ExecutionError>>;

    /// In-memory wake only; terminal writers append first, then wake.
    fn wake(&self, session: SessionId, cause: WakeCause);
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
