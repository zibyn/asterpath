//! Durable waits: approval signals and child-completion wakes expressed
//! through the event log.
//!
//! An approval wait is recorded by its open `ToolCallRequested`; the answer
//! appends `ApprovalDecided` under the call's dedup key, so a re-delivery
//! after a crash never mints a second record. The waiter registry is process
//! memory only; recovery rebuilds waits by scanning the log.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{
    ApprovalDecision, ExecutionError, LogId, SessionId, SessionStore, ToolCallId,
};
use asterpath_protocol::{SessionEvent, dedup, encode_entry};
use futures_core::future::BoxFuture;

use crate::scan::{backend, scan_decoded};
use tokio::sync::Notify;

/// Longest a wait goes without re-reading the log.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Process-local wait identities. Not persisted.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum WaitKey {
    Approval(ToolCallId),
    Child(SessionId),
}

#[derive(Default)]
struct Waiters {
    slots: Mutex<HashMap<WaitKey, Arc<Notify>>>,
}

impl Waiters {
    fn notifier(&self, key: WaitKey) -> Arc<Notify> {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    fn wake(&self, key: WaitKey) {
        if let Some(notify) = self
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            notify.notify_waiters();
        }
    }
}

/// Approval and child-completion waits backed by the event log.
pub(crate) struct Waits {
    log: Arc<dyn SessionStore>,
    waiters: Waiters,
}

impl Waits {
    pub(crate) fn new(log: Arc<dyn SessionStore>) -> Self {
        Self {
            log,
            waiters: Waiters::default(),
        }
    }

    async fn append(
        &self,
        session: SessionId,
        event: &SessionEvent,
        key: Option<asterpath_protocol::DedupKey>,
    ) -> Result<LogId, ExecutionError> {
        let entry = encode_entry(session, key, event).map_err(backend)?;
        self.log.append(entry).await.map_err(backend)
    }

    pub(crate) async fn find_approval(
        &self,
        session: SessionId,
        call: ToolCallId,
    ) -> Result<Option<ApprovalDecision>, ExecutionError> {
        Ok(scan_decoded(&*self.log, session)
            .await?
            .into_iter()
            .find_map(|(_, event)| match event {
                SessionEvent::ApprovalDecided {
                    tool_call_id,
                    decision,
                } if tool_call_id == call => Some(decision),
                _ => None,
            }))
    }

    /// `Some(())` once the parent log records the child's completion.
    async fn child_completed(
        &self,
        session: SessionId,
        child: SessionId,
    ) -> Result<Option<()>, ExecutionError> {
        let completed = scan_decoded(&*self.log, session)
            .await?
            .iter()
            .any(|(_, event)| {
                matches!(
                    event,
                    SessionEvent::ChildSessionCompleted { child: done, .. } if *done == child
                )
            });
        Ok(completed.then_some(()))
    }

    /// Re-check `ready` on every wake of `key`, and at least every
    /// `POLL_INTERVAL`: another process may write the record, and its wake
    /// never reaches this one (ADR-0036). The notifier is armed before each
    /// check, so a wake between check and wait is not lost.
    async fn wait_until<T, F>(
        &self,
        key: WaitKey,
        mut ready: impl FnMut() -> F,
    ) -> Result<T, ExecutionError>
    where
        F: Future<Output = Result<Option<T>, ExecutionError>>,
    {
        loop {
            let notify = self.waiters.notifier(key);
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(value) = ready().await? {
                return Ok(value);
            }
            let _ = tokio::time::timeout(POLL_INTERVAL, notified).await;
        }
    }
}

impl Waits {
    pub(crate) fn await_approval(
        &self,
        session: SessionId,
        call: ToolCallId,
    ) -> BoxFuture<'_, Result<ApprovalDecision, ExecutionError>> {
        Box::pin(self.wait_until(WaitKey::Approval(call), move || {
            self.find_approval(session, call)
        }))
    }

    pub(crate) fn record_approval(
        &self,
        session: SessionId,
        call: ToolCallId,
        decision: ApprovalDecision,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async move {
            self.append(
                session,
                &SessionEvent::ApprovalDecided {
                    tool_call_id: call,
                    decision,
                },
                Some(dedup::approval_decided(call)),
            )
            .await?;
            self.waiters.wake(WaitKey::Approval(call));
            Ok(())
        })
    }

    /// Resolves once `session`'s log records `child`'s completion.
    pub(crate) fn await_child(
        &self,
        session: SessionId,
        child: SessionId,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(self.wait_until(WaitKey::Child(child), move || {
            self.child_completed(session, child)
        }))
    }

    pub(crate) fn wake_child(&self, child: SessionId) {
        self.waiters.wake(WaitKey::Child(child));
    }
}

#[cfg(test)]
mod tests;
