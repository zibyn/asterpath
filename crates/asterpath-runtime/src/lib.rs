//! Durable executor: timers, signals and wake notifications expressed through
//! the event log.
//!
//! Wait ids are derived from the intent record, so re-registering after a crash
//! returns the same id and never mints a second record. The waiter registry is
//! process memory only; recovery rebuilds waits by scanning the log.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use asterpath_core::{
    AwaitSignal, Clock, DURABLE_EXECUTOR_SERVICE, DurableExecutor, EventLog, ExecutionError,
    LifecycleError, LogId, LogQuery, Module, ModuleCtx, ServiceId, SessionId, SignalDelivery,
    SignalId, SignalIntent, SignalName, SignalPayload, Storage, TimerId, TimerRequest, WakeCause,
};
use asterpath_protocol::{
    APPROVAL_SIGNAL_PREFIX, RecordAppended, SessionEvent, decode_entry, dedup, encode_entry,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use tokio::sync::Notify;

/// Process-local wait identities. Not persisted.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum WaitKey {
    Signal(SignalId),
    Timer(TimerId),
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

/// `DurableExecutor` backed by an event log, a storage index and a clock.
pub struct RuntimeExecutor {
    log: Arc<dyn EventLog>,
    _storage: Arc<dyn Storage>,
    clock: Arc<dyn Clock>,
    waiters: Waiters,
    /// Serializes fire/cancel so only one terminal is ever appended.
    timer_lock: tokio::sync::Mutex<()>,
    /// Set by the module wrapper so committed waits reach the trace tap.
    bus: Mutex<Option<ModuleCtx>>,
}

impl RuntimeExecutor {
    pub fn new(log: Arc<dyn EventLog>, storage: Arc<dyn Storage>, clock: Arc<dyn Clock>) -> Self {
        Self {
            log,
            _storage: storage,
            clock,
            waiters: Waiters::default(),
            timer_lock: tokio::sync::Mutex::new(()),
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

    async fn timer_state(
        &self,
        session: SessionId,
        timer: TimerId,
    ) -> Result<TimerState, ExecutionError> {
        let mut deadline = None;
        for event in self.scan(session).await? {
            match event {
                SessionEvent::TimerScheduled {
                    timer_id,
                    deadline: at,
                    ..
                } if timer_id == timer => deadline = Some(at),
                SessionEvent::TimerFired { timer_id } if timer_id == timer => {
                    return Ok(TimerState::Fired);
                }
                SessionEvent::TimerCancelled { timer_id } if timer_id == timer => {
                    return Ok(TimerState::Cancelled);
                }
                _ => {}
            }
        }
        Ok(match deadline {
            Some(deadline) => TimerState::Pending(deadline),
            None => TimerState::Missing,
        })
    }

    async fn append_timer_fired(
        &self,
        session: SessionId,
        timer: TimerId,
    ) -> Result<(), ExecutionError> {
        let _guard = self.timer_lock.lock().await;
        match self.timer_state(session, timer).await? {
            TimerState::Fired => return Ok(()),
            TimerState::Cancelled => return Err(ExecutionError::Cancelled),
            TimerState::Missing => return Err(ExecutionError::NotFound { session }),
            TimerState::Pending(_) => {}
        }
        self.append(
            session,
            &SessionEvent::TimerFired { timer_id: timer },
            Some(dedup::timer(timer)),
        )
        .await?;
        self.waiters.wake(WaitKey::Timer(timer));
        Ok(())
    }

    async fn await_timer(&self, session: SessionId, timer: TimerId) -> Result<(), ExecutionError> {
        loop {
            match self.timer_state(session, timer).await? {
                TimerState::Fired => return Ok(()),
                TimerState::Cancelled => return Err(ExecutionError::Cancelled),
                TimerState::Missing => return Err(ExecutionError::NotFound { session }),
                TimerState::Pending(deadline) => {
                    if deadline <= self.clock.now() {
                        self.append_timer_fired(session, timer).await?;
                        continue;
                    }
                    let notify = self.waiters.notifier(WaitKey::Timer(timer));
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    // Register before re-checking so a fire or cancel in the
                    // window cannot be missed.
                    notified.as_mut().enable();
                    if self.timer_state(session, timer).await?.is_terminal() {
                        continue;
                    }
                    let wait = Duration::from_millis((deadline - self.clock.now()).max(1) as u64);
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep(wait) => {}
                    }
                }
            }
        }
    }
}

enum TimerState {
    Missing,
    Pending(asterpath_core::Timestamp),
    Fired,
    Cancelled,
}

impl TimerState {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Fired | Self::Cancelled)
    }
}

impl DurableExecutor for RuntimeExecutor {
    fn schedule_timer(&self, req: TimerRequest) -> BoxFuture<'_, Result<TimerId, ExecutionError>> {
        Box::pin(async move {
            let timer = derive_timer_id(req.session, req.caused_by, req.timer);
            // A fired or cancelled timer stays terminal; do not resurrect it.
            if self.timer_state(req.session, timer).await?.is_terminal() {
                return Ok(timer);
            }
            self.append(
                req.session,
                &SessionEvent::TimerScheduled {
                    timer_id: timer,
                    deadline: req.deadline,
                    caused_by: req.caused_by,
                },
                Some(dedup::timer(timer)),
            )
            .await?;
            Ok(timer)
        })
    }

    fn cancel_timer(
        &self,
        session: SessionId,
        timer: TimerId,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async move {
            let _guard = self.timer_lock.lock().await;
            match self.timer_state(session, timer).await? {
                TimerState::Fired | TimerState::Cancelled => return Ok(()),
                TimerState::Missing => return Err(ExecutionError::NotFound { session }),
                TimerState::Pending(_) => {}
            }
            self.append(
                session,
                &SessionEvent::TimerCancelled { timer_id: timer },
                Some(dedup::timer(timer)),
            )
            .await?;
            self.waiters.wake(WaitKey::Timer(timer));
            Ok(())
        })
    }

    fn await_signal(
        &self,
        req: AwaitSignal,
    ) -> BoxFuture<'_, Result<SignalPayload, ExecutionError>> {
        Box::pin(async move {
            let signal = signal_id(req.session, &req.name, req.signal);
            if let Some(payload) = self.find_signal(req.session, signal).await? {
                return Ok(payload);
            }
            if req.intent == SignalIntent::Record {
                self.append(
                    req.session,
                    &SessionEvent::SignalAwaited {
                        signal_id: signal,
                        name: req.name.clone(),
                        caused_by: req.caused_by,
                    },
                    Some(dedup::signal(signal)),
                )
                .await?;
            }

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
            let signal = signal_id(delivery.session, &delivery.name, delivery.signal);
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
                WakeCause::Startup => Ok(()),
                WakeCause::Timer(timer) => self.await_timer(session, timer).await,
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
            WakeCause::Timer(timer) => WaitKey::Timer(timer),
            WakeCause::Signal(signal) => WaitKey::Signal(signal),
            WakeCause::ChildCompleted(child) => WaitKey::Child(child),
            WakeCause::Startup => return,
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

/// Approval signals are identified by `tool_call_id`; generic signals by
/// `(session, name)`. The caller's `signal` is therefore only authoritative for
/// approval names.
fn signal_id(session: SessionId, name: &SignalName, supplied: SignalId) -> SignalId {
    if name.as_str().starts_with(APPROVAL_SIGNAL_PREFIX) {
        supplied
    } else {
        let mut hasher = Fnv::new(0xcbf29ce484222325);
        hasher.write(session.as_bytes());
        hasher.write(name.as_str().as_bytes());
        SignalId::from_bytes(hasher.finish_128())
    }
}

/// A timer id is derived from `(session, caused_by, slot)`; the request's
/// `timer` is the caller's slot token.
fn derive_timer_id(session: SessionId, caused_by: LogId, slot: TimerId) -> TimerId {
    let mut hasher = Fnv::new(0xcbf29ce484222325);
    hasher.write(session.as_bytes());
    hasher.write(&caused_by.as_i64().to_be_bytes());
    hasher.write(slot.as_bytes());
    TimerId::from_bytes(hasher.finish_128())
}

/// Deterministic 128-bit FNV-1a; avoids a hashing dependency for stable ids.
struct Fnv(u64);

impl Fnv {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }

    /// Two differently seeded passes; the second consumes the first.
    fn finish_128(&self) -> [u8; 16] {
        let mut second = Fnv::new(self.0 ^ 0x9e3779b97f4a7c15);
        second.write(&self.0.to_be_bytes());
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.0.to_be_bytes());
        bytes[8..].copy_from_slice(&second.0.to_be_bytes());
        bytes
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
