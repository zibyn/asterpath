//! The single append entry (ADR-0025). Every engine writer (agent, waits,
//! scheduler, facade) appends through this log: after a successful append it
//! shows each new record to the hosts' `on_record` hooks, in session order.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{
    CatalogEntry, Claim, LogEntry, LogId, LogQuery, NewEntry, OwnerToken, RetirementError,
    SessionId, SessionQuery, SessionStore, StorageError,
};
use asterpath_protocol::{CommittedRecord, Hooks, decode_entry};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::lock::Mutex as AsyncMutex;

use crate::owner::Ownership;
use crate::watch::Watchers;

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
    // evict on retirement if long-lived hosts touch very many sessions.
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
