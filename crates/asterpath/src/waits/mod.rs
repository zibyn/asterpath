//! Durable waits: approval signals and child-completion wakes expressed
//! through the event log.
//!
//! An approval wait is recorded by its open `ToolCallRequested`; delivery
//! appends `SignalReceived` under the approval's dedup key, so a re-delivery
//! after a crash never mints a second record. The waiter registry is process
//! memory only; recovery rebuilds waits by scanning the log.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::scan_all;
use asterpath_protocol::{
    AwaitSignal, ExecutionError, LogId, SessionId, SessionStore, SignalDelivery, SignalId,
    SignalPayload, WakeCause,
};
use asterpath_protocol::{SessionEvent, decode_entry, dedup, encode_entry};
use futures_core::future::BoxFuture;
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

    async fn scan(&self, session: SessionId) -> Result<Vec<SessionEvent>, ExecutionError> {
        let mut events = Vec::new();
        for entry in scan_all(&*self.log, session).await.map_err(backend)? {
            events.push(decode_entry(&entry).map_err(backend)?);
        }
        Ok(events)
    }

    pub(crate) async fn find_signal(
        &self,
        session: SessionId,
        signal: SignalId,
    ) -> Result<Option<SignalPayload>, ExecutionError> {
        Ok(self
            .scan(session)
            .await?
            .into_iter()
            .find_map(|event| match event {
                SessionEvent::SignalReceived {
                    signal_id, payload, ..
                } if signal_id == signal => Some(payload),
                _ => None,
            }))
    }
}

impl Waits {
    pub(crate) fn await_signal(
        &self,
        req: AwaitSignal,
    ) -> BoxFuture<'_, Result<SignalPayload, ExecutionError>> {
        Box::pin(async move {
            let signal = req.signal;
            loop {
                let notify = self.waiters.notifier(WaitKey::Signal(signal));
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(payload) = self.find_signal(req.session, signal).await? {
                    return Ok(payload);
                }
                notified.await;
            }
        })
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
                WakeCause::Signal(signal) => loop {
                    let notify = self.waiters.notifier(WaitKey::Signal(signal));
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if self.find_signal(session, signal).await?.is_some() {
                        return Ok(());
                    }
                    notified.await;
                },
                WakeCause::ChildCompleted(child) => loop {
                    let notify = self.waiters.notifier(WaitKey::Child(child));
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let completed = self.scan(session).await?.iter().any(|event| {
                        matches!(
                            event,
                            SessionEvent::ChildSessionCompleted { child: done, .. }
                                if *done == child
                        )
                    });
                    if completed {
                        return Ok(());
                    }
                    notified.await;
                },
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

fn backend(error: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::Backend(error.to_string())
}

#[cfg(test)]
mod tests;
