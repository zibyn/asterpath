//! In-memory `EventLog` and `Storage` backend.
//!
//! **This backend does not satisfy G1.** There is no persistence, no fsync and
//! no crash prefix guarantee; everything is lost with the process. It exists to
//! run the backend-neutral contract suites. The log is also its own
//! `SessionCatalog` (parents come from `SessionCreated`). It does not implement
//! `SessionRetirement`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{
    Clock, EventLog, LogEntry, LogId, LogQuery, NewEntry, SessionCatalog, SessionId, Storage,
    StorageError, SystemClock, Timestamp,
};
use asterpath_protocol::{SessionEvent, SessionFacts, decode_entry, decode_payload};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::stream;

use crate::contract::{BackendFactory, BackendHandle};

#[derive(Default)]
struct State {
    entries: Vec<LogEntry>,
    next_log_id: i64,
    session_seq: HashMap<SessionId, u64>,
    dedup: HashMap<(SessionId, String, Option<String>), LogId>,
}

/// Append-only, per-session in-memory event log with idempotent appends,
/// per-session monotonic `seq` and exclusive-cursor scans.
pub struct InMemoryEventLog {
    state: Mutex<State>,
    clock: SystemClock,
}

impl Default for InMemoryEventLog {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryEventLog {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            clock: SystemClock,
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn snapshot(&self) -> Vec<LogEntry> {
        self.state().entries.clone()
    }

    fn append_sync(&self, entry: NewEntry) -> Result<LogId, StorageError> {
        // Decode before dedup so an unknown variant / bad payload is rejected
        // at append time, exactly like the SQLite write path.
        let event = decode_payload(&entry.payload, entry.schema_version)
            .map_err(|error| StorageError::InvalidEntry(error.to_string()))?;
        let kind = event.kind().to_owned();

        let mut state = self.state();
        let dedup = entry
            .dedup_key
            .as_ref()
            .map(|key| (entry.session, kind, Some(key.as_str().to_owned())));
        if let Some(dedup) = &dedup
            && let Some(log_id) = state.dedup.get(dedup)
        {
            return Ok(*log_id);
        }

        let log_id = LogId::from_i64(state.next_log_id + 1);
        state.next_log_id += 1;
        let seq = state.session_seq.entry(entry.session).or_insert(0);
        *seq += 1;
        let seq = *seq;

        let log_entry = LogEntry {
            log_id,
            session: entry.session,
            seq,
            recorded_at: self.clock.now(),
            schema_version: entry.schema_version,
            dedup_key: entry.dedup_key,
            payload: entry.payload,
        };
        if let Some(dedup) = dedup {
            state.dedup.insert(dedup, log_id);
        }
        state.entries.push(log_entry);
        Ok(log_id)
    }
}

impl EventLog for InMemoryEventLog {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        let result = self.append_sync(entry);
        Box::pin(async move { result })
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        let mut entries: Vec<LogEntry> = self
            .snapshot()
            .into_iter()
            .filter(|entry| entry.session == query.session)
            .filter(|entry| query.after.is_none_or(|after| entry.log_id > after))
            .collect();
        entries.sort_by_key(|entry| entry.seq);
        if let Some(limit) = query.limit {
            entries.truncate(limit);
        }
        stream::iter(entries.into_iter().map(Ok)).boxed()
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        let frontier = self
            .snapshot()
            .into_iter()
            .filter(|entry| entry.session == session)
            .max_by_key(|entry| entry.seq)
            .map(|entry| entry.log_id);
        Box::pin(async move { Ok(frontier) })
    }
}

impl InMemoryEventLog {
    /// The parent recorded in `session`'s `SessionCreated`, if any.
    fn parent_of(&self, session: SessionId) -> Result<Option<SessionId>, StorageError> {
        let Some(first) = self
            .snapshot()
            .into_iter()
            .filter(|entry| entry.session == session)
            .min_by_key(|entry| entry.seq)
        else {
            return Ok(None);
        };
        match decode_entry(&first).map_err(|error| StorageError::Corrupt {
            session: Some(session),
            detail: error.to_string(),
        })? {
            SessionEvent::SessionCreated { parent, .. } => Ok(parent.map(|parent| parent.session)),
            _ => Ok(None),
        }
    }
}

/// Like SQLite's `sessions.parent_id`: the tree comes from `SessionCreated`.
impl SessionCatalog for InMemoryEventLog {
    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        let parent = self.parent_of(session);
        Box::pin(async move { parent })
    }

    fn children(&self, session: SessionId) -> BoxStream<'_, Result<SessionId, StorageError>> {
        let mut sessions: Vec<SessionId> = self
            .snapshot()
            .into_iter()
            .map(|entry| entry.session)
            .collect();
        sessions.sort();
        sessions.dedup();
        let children: Vec<Result<SessionId, StorageError>> = sessions
            .into_iter()
            .filter_map(|candidate| match self.parent_of(candidate) {
                Ok(Some(parent)) if parent == session => Some(Ok(candidate)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect();
        stream::iter(children).boxed()
    }
}

/// Read-only wake discovery derived on demand from the log.
pub struct InMemoryStorage {
    log: Arc<InMemoryEventLog>,
}

impl InMemoryStorage {
    pub fn new(log: Arc<InMemoryEventLog>) -> Self {
        Self { log }
    }
}

impl Storage for InMemoryStorage {
    fn wakeable(&self, now: Timestamp) -> BoxFuture<'_, Result<Vec<SessionId>, StorageError>> {
        let result = self.wakeable_sync(now);
        Box::pin(async move { result })
    }
}

impl InMemoryStorage {
    fn wakeable_sync(&self, _now: Timestamp) -> Result<Vec<SessionId>, StorageError> {
        let mut by_session: HashMap<SessionId, Vec<LogEntry>> = HashMap::new();
        for entry in self.log.snapshot() {
            by_session.entry(entry.session).or_default().push(entry);
        }

        let mut wakeable = Vec::new();
        for (session, mut entries) in by_session {
            entries.sort_by_key(|entry| entry.seq);
            let facts = SessionFacts::fold(&entries).map_err(|error| StorageError::Corrupt {
                session: Some(session),
                detail: error.to_string(),
            })?;
            if !facts.is_idle() || facts.has_pending_input() {
                wakeable.push(session);
            }
        }
        wakeable.sort();
        Ok(wakeable)
    }
}

/// A factory over one shared in-memory log. `open` returns fresh handles to the
/// same backend, mirroring "reopen the same database" in the contract suites.
#[derive(Default)]
pub struct MemoryBackend {
    log: Arc<InMemoryEventLog>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BackendFactory for MemoryBackend {
    fn open(&self) -> BackendHandle {
        BackendHandle {
            log: self.log.clone(),
            storage: Arc::new(InMemoryStorage::new(self.log.clone())),
        }
    }
}
