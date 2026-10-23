//! Durable executor: approval signals and child-completion wakes expressed
//! through the event log.
//!
//! An approval wait is recorded by its open `ToolCallRequested`; delivery
//! appends `SignalReceived` under the approval's dedup key, so a re-delivery
//! after a crash never mints a second record. The waiter registry is process
//! memory only; recovery rebuilds waits by scanning the log.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_core::{
    AwaitSignal, DURABLE_EXECUTOR_SERVICE, DurableExecutor, EventLog, ExecutionError,
    LifecycleError, LogId, LogQuery, Module, ModuleCtx, ServiceId, SessionId, SignalDelivery,
    SignalId, SignalPayload, WakeCause,
};
use asterpath_protocol::{RecordAppended, SessionEvent, decode_entry, dedup, encode_entry};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
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

/// `DurableExecutor` backed by an event log.
pub struct RuntimeExecutor {
    log: Arc<dyn EventLog>,
    waiters: Waiters,
    /// Set by the module wrapper so committed waits reach the trace tap.
    bus: Mutex<Option<ModuleCtx>>,
}

impl RuntimeExecutor {
    pub fn new(log: Arc<dyn EventLog>) -> Self {
        Self {
            log,
            waiters: Waiters::default(),
            bus: Mutex::new(None),
        }
    }

    pub fn set_event_bus(&self, ctx: ModuleCtx) {
        *self.bus.lock().unwrap_or_else(PoisonError::into_inner) = Some(ctx);
    }

    async fn append(
        &self,
        session: SessionId,
        event: &SessionEvent,
        key: Option<asterpath_core::DedupKey>,
    ) -> Result<LogId, ExecutionError> {
        let entry = encode_entry(session, key, event).map_err(backend)?;
        let log_id = self.log.append(entry).await.map_err(backend)?;
        let bus = self
            .bus
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(bus) = bus {
            bus.emit(RecordAppended { session }).await;
        }
        Ok(log_id)
    }

    async fn scan(&self, session: SessionId) -> Result<Vec<SessionEvent>, ExecutionError> {
        let stream: BoxStream<'_, Result<asterpath_core::LogEntry, asterpath_core::StorageError>> =
            self.log.scan(LogQuery {
                session,
                after: None,
                limit: None,
            });
        let mut events = Vec::new();
        for entry in collect(stream).await? {
            events.push(decode_entry(&entry).map_err(backend)?);
        }
        Ok(events)
    }

    async fn find_signal(
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

impl DurableExecutor for RuntimeExecutor {
    fn await_signal(
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

    fn deliver_signal(
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

    fn await_wake(
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

    fn wake(&self, _session: SessionId, cause: WakeCause) {
        let key = match cause {
            WakeCause::Signal(signal) => WaitKey::Signal(signal),
            WakeCause::ChildCompleted(child) => WaitKey::Child(child),
        };
        self.waiters.wake(key);
    }
}

/// Module wrapper so the composition root can provide the executor service.
pub struct RuntimeModule {
    executor: Arc<RuntimeExecutor>,
}

impl RuntimeModule {
    pub fn new(executor: Arc<RuntimeExecutor>) -> Self {
        Self { executor }
    }
}

impl Module for RuntimeModule {
    fn name(&self) -> &'static str {
        "runtime"
    }

    fn provides(&self) -> &[ServiceId] {
        static IDS: [ServiceId; 1] = [DURABLE_EXECUTOR_SERVICE.id()];
        &IDS
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        let executor: Arc<dyn DurableExecutor> = self.executor.clone();
        self.executor.set_event_bus(ctx.clone());
        let result = ctx.provide(DURABLE_EXECUTOR_SERVICE, executor);
        Box::pin(async move { result.map(|_| ()) })
    }
}

async fn collect<S>(mut stream: S) -> Result<Vec<asterpath_core::LogEntry>, ExecutionError>
where
    S: futures_core::Stream<Item = Result<asterpath_core::LogEntry, asterpath_core::StorageError>>
        + Unpin,
{
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.map_err(backend)?);
    }
    Ok(entries)
}

fn backend(error: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::Backend(error.to_string())
}
