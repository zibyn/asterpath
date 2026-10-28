//! The single append entry (ADR-0025). Every engine writer (agent, waits,
//! scheduler, facade) appends through this log: after a successful append it
//! shows each new record to the applications' `on_record` hooks, in session order.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{
    AgentError, CatalogEntry, Claim, DedupKey, ExecutionError, LogEntry, LogId, LogQuery, NewEntry,
    OwnerToken, RetirementError, SessionEvent, SessionId, SessionQuery, SessionStore, StorageError,
};
use asterpath_protocol::{CommittedRecord, Hooks, decode_entry, encode_entry};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::lock::Mutex as AsyncMutex;

use crate::error::AsterError;
use crate::owner::Ownership;
use crate::scan::{DecodedLog, ScanError, scan_decoded};
use crate::watch::Watchers;

/// Why a typed append did not commit. An event that does not encode is an
/// engine bug, never the caller's input: it maps to an internal error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AppendError {
    #[error("cannot encode a record of session {session:?}: {detail}")]
    Encode { session: SessionId, detail: String },
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl From<AppendError> for ExecutionError {
    fn from(error: AppendError) -> Self {
        Self::Backend(error.to_string())
    }
}

impl From<AppendError> for AgentError {
    fn from(error: AppendError) -> Self {
        match error {
            AppendError::Storage(error) => Self::Storage(error),
            encode @ AppendError::Encode { .. } => Self::Execution(encode.into()),
        }
    }
}

impl From<AppendError> for AsterError {
    fn from(error: AppendError) -> Self {
        match error {
            AppendError::Storage(error) => Self::Storage(error),
            encode @ AppendError::Encode { .. } => Self::Execution(encode.into()),
        }
    }
}

/// Last record shown to `on_record` for one session.
#[derive(Clone, Copy, Default)]
enum Cursor {
    /// This process has not appended to the session yet.
    #[default]
    Unset,
    /// Records up to this id were shown; `None` when the log was empty.
    At(Option<LogId>),
}

pub(crate) struct CommitLog {
    inner: Arc<dyn SessionStore>,
    hooks: Vec<Arc<dyn Hooks>>,
    // shortcut: one cursor per session touched by this process, never evicted;
    // evict on retirement if long-lived applications touch very many sessions.
    cursors: Mutex<HashMap<SessionId, Arc<AsyncMutex<Cursor>>>>,
    /// Told of every commit, so this process's watchers need not poll.
    watchers: Arc<Watchers>,
    /// Refuses owner writes of a lost claim and learns of claims another
    /// process took over.
    ownership: Arc<Ownership>,
}

impl CommitLog {
    pub(crate) fn new(
        inner: Arc<dyn SessionStore>,
        hooks: Vec<Arc<dyn Hooks>>,
        watchers: Arc<Watchers>,
        ownership: Arc<Ownership>,
    ) -> Self {
        Self {
            inner,
            hooks,
            cursors: Mutex::new(HashMap::new()),
            watchers,
            ownership,
        }
    }

    /// A commit log over `inner` with no hooks, private watchers and an
    /// ownership that holds no claims: what engine parts get in unit tests.
    #[cfg(test)]
    pub(crate) fn for_tests(inner: Arc<dyn SessionStore>) -> Arc<Self> {
        let ownership = Arc::new(Ownership::new(Arc::clone(&inner), Vec::new()));
        Arc::new(Self::new(inner, Vec::new(), Arc::default(), ownership))
    }

    /// Changes on every commit to `session` made through this log. Commits
    /// of other processes never reach it: pair it with
    /// [`POLL_INTERVAL`](crate::watch::POLL_INTERVAL).
    pub(crate) fn commits(&self, session: SessionId) -> tokio::sync::watch::Receiver<()> {
        self.watchers.commits(session)
    }

    /// The typed append: encodes `event`, stamps the owner of the drive or
    /// compaction being polled, shows the record to `on_record` and tells
    /// the watchers. `key` is the caller's dedup key; a replay returns the
    /// committed record's id.
    pub(crate) async fn append_event(
        &self,
        session: SessionId,
        event: SessionEvent,
        key: Option<DedupKey>,
    ) -> Result<LogId, AppendError> {
        let entry = encode_entry(session, key, &event).map_err(|error| AppendError::Encode {
            session,
            detail: error.to_string(),
        })?;
        Ok(self.append(entry).await?)
    }

    /// The whole log of `session`, decoded once.
    pub(crate) async fn read(&self, session: SessionId) -> Result<DecodedLog, ScanError> {
        scan_decoded(self, session).await
    }

    fn cursor(&self, session: SessionId) -> Arc<AsyncMutex<Cursor>> {
        Arc::clone(
            self.cursors
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(session)
                .or_default(),
        )
    }

    /// Appends under the session's cursor lock, so records reach `on_record`
    /// once each and in log order even when writers race. A dedup replay of
    /// a record that was already committed is not shown again.
    async fn append_observed(&self, entry: NewEntry) -> Result<LogId, StorageError> {
        let session = entry.session;
        let cursor = self.cursor(session);
        let mut cursor = cursor.lock().await;
        let after = match *cursor {
            Cursor::At(after) => after,
            // First write this process makes: only records from here on.
            Cursor::Unset => self.inner.frontier(session).await?,
        };
        let log_id = self.inner.append(entry).await?;
        *cursor = Cursor::At(after);
        if after.is_some_and(|after| log_id <= after) {
            return Ok(log_id);
        }

        let mut stream = self.inner.scan(LogQuery {
            session,
            after,
            limit: None,
        });
        while let Some(entry) = stream.next().await {
            // The append already succeeded; an observation failure must not
            // turn it into an error for the writer.
            match entry {
                Ok(entry) => {
                    *cursor = Cursor::At(Some(entry.log_id));
                    self.show(&entry);
                }
                Err(error) => {
                    tracing::warn!(?session, %error, "on_record: scan after append failed");
                    break;
                }
            }
        }
        Ok(log_id)
    }

    fn show(&self, entry: &LogEntry) {
        let event = match decode_entry(entry) {
            Ok(event) => event,
            Err(error) => {
                tracing::warn!(session = ?entry.session, %error, "on_record: undecodable record");
                return;
            }
        };
        let record = CommittedRecord::new(
            entry.session,
            entry.log_id,
            entry.seq,
            entry.recorded_at,
            event,
        );
        for hook in &self.hooks {
            if std::panic::catch_unwind(AssertUnwindSafe(|| hook.on_record(&record))).is_err() {
                tracing::warn!(session = ?record.session, seq = record.seq, "on_record hook panicked");
            }
        }
    }
}

impl SessionStore for CommitLog {
    fn append(&self, mut entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        // A write made while a drive or compaction is polled is an owner
        // write: it carries that claim (ADR-0036).
        entry.owner = entry.owner.or(crate::owner::current());
        let session = entry.session;
        let owner = entry.owner;
        Box::pin(async move {
            if let Some(owner) = owner
                && self.ownership.revoked(owner)
            {
                return Err(StorageError::OwnedElsewhere { root: owner.root });
            }
            // No hooks, no observation work (#74).
            let appended = if self.hooks.is_empty() {
                self.inner.append(entry).await
            } else {
                self.append_observed(entry).await
            };
            if let (Some(owner), Err(StorageError::OwnedElsewhere { .. })) = (owner, &appended) {
                self.ownership.lose(owner);
            }
            let log_id = appended?;
            self.watchers.committed(session);
            Ok(log_id)
        })
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

    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>> {
        self.inner.retire(session, expected_frontier)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use asterpath_protocol::{
        CommittedRecord, Hooks, InputId, SessionEvent, SessionId, SessionStore, StorageError,
        decode_entry, dedup, scan_all,
    };
    use asterpath_testing::InMemoryEventLog;

    use super::{AppendError, CommitLog};
    use crate::owner::Ownership;

    const SESSION: SessionId = SessionId::from_bytes([4; 16]);

    #[derive(Default)]
    struct Seen(Mutex<Vec<SessionEvent>>);

    impl Hooks for Seen {
        fn on_record(&self, record: &CommittedRecord) {
            self.0.lock().unwrap().push(record.event.clone());
        }
    }

    fn input() -> SessionEvent {
        SessionEvent::InputRecorded { input: Vec::new() }
    }

    fn commit_log(inner: &Arc<InMemoryEventLog>, hooks: Vec<Arc<dyn Hooks>>) -> CommitLog {
        let inner: Arc<dyn SessionStore> = inner.clone();
        CommitLog::new(
            Arc::clone(&inner),
            hooks.clone(),
            Arc::default(),
            Arc::new(Ownership::new(inner, hooks)),
        )
    }

    #[tokio::test]
    async fn a_typed_append_is_committed_once_and_shown_to_on_record() {
        let inner = Arc::new(InMemoryEventLog::new());
        let seen = Arc::new(Seen::default());
        let log = commit_log(&inner, vec![seen.clone()]);
        let key = Some(dedup::input_recorded(InputId::from_bytes([1; 16])));

        let first = log
            .append_event(SESSION, input(), key.clone())
            .await
            .expect("append");
        let replay = log
            .append_event(SESSION, input(), key)
            .await
            .expect("replay");

        assert_eq!(first, replay);
        let entries = scan_all(&*inner, SESSION).await.expect("scan");
        assert_eq!(entries.len(), 1);
        assert_eq!(decode_entry(&entries[0]).expect("decodes"), input());
        assert_eq!(*seen.0.lock().unwrap(), vec![input()]);
    }

    #[tokio::test]
    async fn a_store_failure_is_the_storage_arm_of_the_append_error() {
        let inner = Arc::new(InMemoryEventLog::new());
        let log = commit_log(&inner, Vec::new());
        // The store rejects metadata for a session it never saw created.
        let orphan = SessionEvent::MetadataChanged {
            set: Default::default(),
            remove: Vec::new(),
        };

        let error = log.append_event(SESSION, orphan, None).await.unwrap_err();

        assert!(
            matches!(error, AppendError::Storage(StorageError::InvalidEntry(_))),
            "{error:?}"
        );
    }
}
