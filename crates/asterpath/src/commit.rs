//! The single append entry (ADR-0025). Every engine writer (agent, runtime,
//! scheduler, facade) appends through this log: after a successful append it
//! shows each new record to the hosts' `on_record` hooks, in session order,
//! then drives the trace tap.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_core::{EventLog, LogEntry, LogId, LogQuery, NewEntry, SessionId, StorageError};
use asterpath_protocol::{CommittedRecord, Hooks, decode_entry};
use asterpath_tracing::TracingTap;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::lock::Mutex as AsyncMutex;

/// Last record shown to `on_record` per session; `None` until first used.
type Cursor = Arc<AsyncMutex<Option<Option<LogId>>>>;

pub(crate) struct CommitLog {
    inner: Arc<dyn EventLog>,
    hooks: Vec<Arc<dyn Hooks>>,
    tap: Arc<TracingTap>,
    // shortcut: one cursor per session touched by this process, never evicted;
    // evict on retirement if long-lived hosts touch very many sessions.
    cursors: Mutex<HashMap<SessionId, Cursor>>,
}

impl CommitLog {
    pub(crate) fn new(
        inner: Arc<dyn EventLog>,
        hooks: Vec<Arc<dyn Hooks>>,
        tap: Arc<TracingTap>,
    ) -> Self {
        Self {
            inner,
            hooks,
            tap,
            cursors: Mutex::new(HashMap::new()),
        }
    }

    fn cursor(&self, session: SessionId) -> Cursor {
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
            Some(after) => after,
            // First write this process makes: only records from here on.
            None => self.inner.frontier(session).await?,
        };
        let log_id = self.inner.append(entry).await?;
        *cursor = Some(after);
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
                    *cursor = Some(Some(entry.log_id));
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

impl EventLog for CommitLog {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        Box::pin(async move {
            let session = entry.session;
            // No hooks, no observation work (#74).
            let log_id = if self.hooks.is_empty() {
                self.inner.append(entry).await?
            } else {
                self.append_observed(entry).await?
            };
            self.tap.record_appended(session).await;
            Ok(log_id)
        })
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        self.inner.scan(query)
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        self.inner.frontier(session)
    }
}
