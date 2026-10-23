//! Durable waits: approval signals and child-completion wakes expressed
//! through the event log.
//!
//! An approval wait is recorded by its open `ToolCallRequested`; delivery
//! appends `SignalReceived` under the approval's dedup key, so a re-delivery
//! after a crash never mints a second record. The waiter registry is process
//! memory only; recovery rebuilds waits by scanning the log.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{
    AwaitSignal, ExecutionError, LogId, SessionId, SessionStore, SignalDelivery, SignalId,
    SignalPayload, WakeCause,
};
use asterpath_protocol::{SessionEvent, dedup, encode_entry};
use futures_core::future::BoxFuture;

use crate::scan::{backend, scan_decoded};
use tokio::sync::Notify;

/// Process-local wait identities. Not persisted.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum WaitKey {
    Signal(SignalId),
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

    pub(crate) async fn find_signal(
        &self,
        session: SessionId,
        signal: SignalId,
    ) -> Result<Option<SignalPayload>, ExecutionError> {
        Ok(scan_decoded(&*self.log, session)
            .await?
            .into_iter()
            .find_map(|(_, event)| match event {
                SessionEvent::SignalReceived {
                    signal_id, payload, ..
                } if signal_id == signal => Some(payload),
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

    /// Re-check `ready` on every wake of `key`. The notifier is armed before
    /// each check, so a wake between check and wait is not lost.
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
            notified.await;
        }
    }
}

impl Waits {
    pub(crate) fn await_signal(
        &self,
        req: AwaitSignal,
    ) -> BoxFuture<'_, Result<SignalPayload, ExecutionError>> {
        Box::pin(self.wait_until(WaitKey::Signal(req.signal), move || {
            self.find_signal(req.session, req.signal)
        }))
    }

    pub(crate) fn deliver_signal(
        &self,
        delivery: SignalDelivery,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async move {
            let signal = delivery.signal;
            self.append(
                delivery.session,
                &SessionEvent::SignalReceived {
                    signal_id: signal,
                    name: delivery.name,
                    payload: delivery.payload,
                },
                Some(dedup::signal(signal)),
            )
            .await?;
            self.waiters.wake(WaitKey::Signal(signal));
            Ok(())
        })
    }

    pub(crate) fn await_wake(
        &self,
        session: SessionId,
        cause: WakeCause,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async move {
            match cause {
                WakeCause::Signal(signal) => self
                    .wait_until(WaitKey::Signal(signal), || {
                        self.find_signal(session, signal)
                    })
                    .await
                    .map(drop),
                WakeCause::ChildCompleted(child) => {
                    self.wait_until(WaitKey::Child(child), || {
                        self.child_completed(session, child)
                    })
                    .await
                }
            }
        })
    }

    pub(crate) fn wake(&self, cause: WakeCause) {
        let key = match cause {
            WakeCause::Signal(signal) => WaitKey::Signal(signal),
            WakeCause::ChildCompleted(child) => WaitKey::Child(child),
        };
        self.waiters.wake(key);
    }
}

#[cfg(test)]
mod tests;
