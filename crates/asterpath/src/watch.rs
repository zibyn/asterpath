//! Watching a session without driving it (ADR-0038).
//!
//! Committed records come from the log: replayed from a cursor, then polled,
//! so a watcher in any process sees them. Live events (text deltas and the
//! rest of `AgentEvent`) are never logged; they fan out only to watchers in
//! the process whose drive produced them.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use asterpath_protocol::{
    AgentEvent, CommittedRecord, LogId, LogQuery, SessionId, SessionStore, decode_entry,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use tokio::sync::{broadcast, watch};

use crate::AsterError;

/// Longest a watcher goes without re-reading the log: the delay before it
/// sees a commit made by another process.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Live events a slow watcher may fall behind by before it skips some.
const LIVE_CAPACITY: usize = 1024;

/// One item of [`SessionHandle::watch`](crate::SessionHandle::watch).
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub enum WatchEvent {
    /// A committed record, in log order; its `log_id` is the cursor to
    /// resume after.
    Record(Box<CommittedRecord>),
    /// The replay has caught up with the log; later records are new commits.
    /// Sent once per watch.
    Synced,
    /// A live event of a drive in this process. Never replayed, and skipped
    /// when the watcher falls more than a buffer behind.
    Live(AgentEvent),
}

struct Slot {
    live: broadcast::Sender<AgentEvent>,
    committed: watch::Sender<()>,
}

/// This process's watchers, by session: drives publish their events here and
/// the append entry signals commits, so local watchers need not poll.
#[derive(Default)]
pub(crate) struct Watchers {
    slots: Mutex<HashMap<SessionId, Slot>>,
}

impl Watchers {
    fn subscribe(
        &self,
        session: SessionId,
    ) -> (broadcast::Receiver<AgentEvent>, watch::Receiver<()>) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = slots.entry(session).or_insert_with(|| Slot {
            live: broadcast::channel(LIVE_CAPACITY).0,
            committed: watch::channel(()).0,
        });
        (slot.live.subscribe(), slot.committed.subscribe())
    }

    /// Calls `f` on the session's slot; drops the slot once nobody watches.
    fn with_slot(&self, session: SessionId, f: impl FnOnce(&Slot)) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = slots.get(&session) {
            if slot.live.receiver_count() == 0 {
                slots.remove(&session);
            } else {
                f(slot);
            }
        }
    }

    pub(crate) fn publish(&self, session: SessionId, event: &AgentEvent) {
        self.with_slot(session, |slot| drop(slot.live.send(event.clone())));
    }

    pub(crate) fn committed(&self, session: SessionId) {
        self.with_slot(session, |slot| slot.committed.send_replace(()));
    }
}

struct Watch {
    store: Arc<dyn SessionStore>,
    session: SessionId,
    after: Option<LogId>,
    live: broadcast::Receiver<AgentEvent>,
    committed: watch::Receiver<()>,
    pending: VecDeque<WatchEvent>,
    synced: bool,
}

impl Watch {
    /// Queue every record after the cursor; the cursor moves past each.
    async fn read(&mut self) -> Result<(), AsterError> {
        // Seen before the read, so a commit during it wakes the next wait.
        self.committed.borrow_and_update();
        let mut records = self.store.scan(LogQuery {
            session: self.session,
            after: self.after,
            limit: None,
        });
        while let Some(entry) = records.next().await {
            let entry = entry?;
            let event = decode_entry(&entry).map_err(crate::corrupt(self.session))?;
            self.after = Some(entry.log_id);
            self.pending
                .push_back(WatchEvent::Record(Box::new(CommittedRecord::new(
                    entry.session,
                    entry.log_id,
                    entry.seq,
                    entry.recorded_at,
                    event,
                ))));
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<WatchEvent, AsterError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            if self.synced {
                // Live events first: a drive publishes a delta before it
                // commits the record that ends it.
                tokio::select! {
                    biased;
                    live =self.live.recv() => match live {
                        Ok(event) => return Ok(WatchEvent::Live(event)),
                        // Lagged: the skipped deltas are gone; records are not.
                        Err(_) => continue,
                    },
                    _ = self.committed.changed() => {}
                    () = tokio::time::sleep(POLL_INTERVAL) => {}
                }
            }
            self.read().await?;
            if !self.synced {
                self.synced = true;
                self.pending.push_back(WatchEvent::Synced);
            }
        }
    }
}

/// Subscribes before the replay, so no live event after this call is missed.
pub(crate) fn watch(
    store: Arc<dyn SessionStore>,
    watchers: &Watchers,
    session: SessionId,
    after: Option<LogId>,
) -> BoxStream<'static, Result<WatchEvent, AsterError>> {
    let (live, committed) = watchers.subscribe(session);
    let state = Watch {
        store,
        session,
        after,
        live,
        committed,
        pending: VecDeque::new(),
        synced: false,
    };
    stream::unfold(state, |mut state| async move {
        let event = state.next().await;
        Some((event, state))
    })
    .boxed()
}
