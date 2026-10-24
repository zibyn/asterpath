//! In-memory `SessionStore` backend.
//!
//! **This backend does not satisfy G1.** There is no persistence, no fsync and
//! no crash prefix guarantee; everything is lost with the process. It exists to
//! run the backend-neutral contract suites. Like SQLite, its session table
//! comes from `SessionCreated`, and retirement writes a tombstone.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{
    CatalogEntry, Clock, LogEntry, LogId, LogQuery, NewEntry, ParentFilter, RetirementError,
    SessionId, SessionQuery, SessionStore, StorageError, SystemClock, Timestamp,
};
use asterpath_protocol::{SessionEvent, SessionFacts, decode_payload};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::stream;

use crate::contract::StoreFactory;

#[derive(Default)]
struct State {
    entries: Vec<LogEntry>,
    next_log_id: i64,
    session_seq: HashMap<SessionId, u64>,
    dedup: HashMap<(SessionId, String, Option<String>), LogId>,
    /// Live sessions, their parents and creation time, like SQLite's
    /// `sessions` table.
    sessions: BTreeMap<SessionId, (Option<SessionId>, Timestamp)>,
    /// Retired sessions and their last log id.
    retired: HashMap<SessionId, LogId>,
}

impl State {
    fn log_of(&self, session: SessionId) -> Vec<LogEntry> {
        if self.retired.contains_key(&session) {
            return Vec::new();
        }
        let mut entries: Vec<LogEntry> = self
            .entries
            .iter()
            .filter(|entry| entry.session == session)
            .cloned()
            .collect();
        entries.sort_by_key(|entry| entry.seq);
        entries
    }

    fn is_idle(&self, session: SessionId) -> Result<bool, StorageError> {
        let facts =
            SessionFacts::fold(&self.log_of(session)).map_err(|error| StorageError::Corrupt {
                session: Some(session),
                detail: error.to_string(),
            })?;
        Ok(facts.is_idle())
    }

    /// The check order is the contract in retention.md §5, as in SQLite.
    fn retire(&mut self, session: SessionId, expected: LogId) -> Result<(), RetirementError> {
        if let Some(last) = self.retired.get(&session) {
            return if *last == expected {
                Ok(())
            } else {
                Err(RetirementError::FrontierMoved {
                    expected,
                    actual: Some(*last),
                })
            };
        }
        let Some((parent, _)) = self.sessions.get(&session).copied() else {
            return Err(RetirementError::NotFound);
        };
        if !self.is_idle(session)? {
            return Err(RetirementError::NotIdle);
        }
        let actual = self.log_of(session).last().map(|entry| entry.log_id);
        if actual != Some(expected) {
            return Err(RetirementError::FrontierMoved { expected, actual });
        }
        if self.sessions.values().any(|(p, _)| *p == Some(session)) {
            return Err(RetirementError::HasChildren);
        }
        if let Some(parent) = parent
            && !self.is_idle(parent)?
        {
            return Err(RetirementError::ParentNotIdle);
        }
        self.retired.insert(session, expected);
        self.sessions.remove(&session);
        self.entries.retain(|entry| entry.session != session);
        Ok(())
    }

    fn catalog(&self, query: SessionQuery) -> Vec<CatalogEntry> {
        let mut entries: Vec<CatalogEntry> = self
            .sessions
            .iter()
            .filter(|(_, (parent, _))| match query.parent {
                ParentFilter::Any => true,
                ParentFilter::Roots => parent.is_none(),
                ParentFilter::ChildrenOf(of) => *parent == Some(of),
            })
            .map(|(session, (parent, created_at))| CatalogEntry {
                session: *session,
                parent: *parent,
                created_at: *created_at,
                last_activity: self
                    .log_of(*session)
                    .last()
                    .map_or(*created_at, |entry| entry.recorded_at),
            })
            .filter(|entry| query.after.is_none_or(|after| entry.cursor() < after))
            .collect();
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.cursor()));
        entries.truncate(query.limit.unwrap_or(usize::MAX));
        entries
    }
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

        let recorded_at = self.clock.now();
        if let SessionEvent::SessionCreated { parent, .. } = &event {
            state.sessions.insert(
                entry.session,
                (parent.as_ref().map(|parent| parent.session), recorded_at),
            );
        }
        let log_entry = LogEntry {
            log_id,
            session: entry.session,
            seq,
            recorded_at,
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

impl SessionStore for InMemoryEventLog {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        let result = self.append_sync(entry);
        Box::pin(async move { result })
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        let mut entries: Vec<LogEntry> = self
            .state()
            .log_of(query.session)
            .into_iter()
            .filter(|entry| query.after.is_none_or(|after| entry.log_id > after))
            .collect();
        if let Some(limit) = query.limit {
            entries.truncate(limit);
        }
        stream::iter(entries.into_iter().map(Ok)).boxed()
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        let frontier = self
            .state()
            .log_of(session)
            .last()
            .map(|entry| entry.log_id);
        Box::pin(async move { Ok(frontier) })
    }

    fn sessions(
        &self,
        query: SessionQuery,
    ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>> {
        let entries = self.state().catalog(query);
        Box::pin(async move { Ok(entries) })
    }

    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        let parent = self
            .state()
            .sessions
            .get(&session)
            .and_then(|(parent, _)| *parent);
        Box::pin(async move { Ok(parent) })
    }

    /// Idle check, frontier CAS, delete and tombstone under one lock.
    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>> {
        let result = self.state().retire(session, expected_frontier);
        Box::pin(async move { result })
    }
}

/// A factory over one shared in-memory store.
#[derive(Default)]
pub struct MemoryBackend {
    store: Arc<InMemoryEventLog>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StoreFactory for MemoryBackend {
    fn open(&self) -> Arc<dyn SessionStore> {
        self.store.clone()
    }
}
