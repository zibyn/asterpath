//! Agent loop: the single-turn / single-step walking skeleton.
//!
//! One driver per session, model-attempt retries inside one step, the
//! `StepStarted.seq` projection waterline, and log-derived state. Tools,
//! steering, cancellation and compaction are deliberately out of scope here.

// The public error enums are fixed by packaging.md §3.4–3.5.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asterpath_core::{DedupKey, EventLog, LogEntry, LogId, LogQuery, SessionId, StepId, TurnId};
use asterpath_protocol::{
    AbortKind, AgentError, AgentEvent, AgentRunner, CompactResult, ConfigSnapshot, Item,
    ModelRequest, OpenIntents, ProviderCatalog, ProviderError, ProviderErrorKind, RunRequest,
    SessionEvent, SessionState, StopReason, StreamEvent, TurnConfig, TurnEndReason, Usage,
    decode_entry, dedup,
};
use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

/// Default turn driver.
pub struct Agent {
    log: Arc<dyn EventLog>,
    providers: Arc<ProviderCatalog>,
    busy: Arc<Mutex<HashSet<SessionId>>>,
}

impl Agent {
    pub fn new(log: Arc<dyn EventLog>, providers: Arc<ProviderCatalog>) -> Self {
        Self {
            log,
            providers,
            busy: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn acquire(&self, session: SessionId) -> bool {
        self.busy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(session)
    }

    async fn scan(&self, session: SessionId) -> Result<Vec<LogEntry>, AgentError> {
        let mut stream = self.log.scan(LogQuery {
            session,
            after: None,
            limit: None,
        });
        let mut entries = Vec::new();
        while let Some(entry) = stream.next().await {
            entries.push(entry?);
        }
        Ok(entries)
    }

    async fn append(
        &self,
        session: SessionId,
        event: SessionEvent,
        key: Option<DedupKey>,
    ) -> Result<LogId, AgentError> {
        let entry = asterpath_protocol::encode_entry(session, key, &event).map_err(|error| {
            AgentError::Corrupt {
                session,
                detail: error.to_string(),
            }
        })?;
        Ok(self.log.append(entry).await?)
    }

    async fn execute(&self, session: SessionId, events: &EventSink) {
        if let Err(error) = self.execute_inner(session, events).await {
            events.push(Err(error));
        }
    }

    async fn execute_inner(
        &self,
        session: SessionId,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let decoded = decode_entries(self.scan(session).await?, session)?;
        let config = decoded
            .iter()
            .find_map(|(_, event)| match event {
                SessionEvent::SessionCreated { config, .. } => Some(config.clone()),
                _ => None,
            })
            .ok_or_else(|| AgentError::Corrupt {
                session,
                detail: "session log has no SessionCreated".to_owned(),
            })?;

        let mut intents = OpenIntents::default();
        for (entry, event) in &decoded {
            intents.apply(event, entry.dedup_key.as_ref());
        }

        match intents.state() {
            SessionState::Idle => {
                if !has_pending_input(&decoded) {
                    events.push(Ok(AgentEvent::Idle { session }));
                    return Ok(());
                }
                let turn = TurnId::from_bytes(fresh_id());
                self.append(session, SessionEvent::TurnStarted { turn_id: turn }, None)
                    .await?;
                events.push(Ok(AgentEvent::TurnStarted { turn }));
                self.run_steps(session, &config, turn, None, events).await
            }
            SessionState::TurnOpen { turn, .. } => {
                // Resume from the log: never repeat TurnStarted.
                let mut committed: HashSet<StepId> = HashSet::new();
                let mut open_step: Option<(StepId, u64)> = None;
                for (entry, event) in &decoded {
                    match event {
                        SessionEvent::StepStarted { turn_id, step_id } if *turn_id == turn => {
                            open_step = Some((*step_id, entry.seq))
                        }
                        SessionEvent::StepCommitted { step_id, .. } => {
                            committed.insert(*step_id);
                            if open_step.is_some_and(|(open, _)| open == *step_id) {
                                open_step = None;
                            }
                        }
                        _ => {}
                    }
                }

                if let Some((step, _)) = open_step
                    && decoded.iter().any(|(_, event)| {
                        matches!(
                            event,
                            SessionEvent::AssistantMessage { turn_id, step_id, .. }
                                if *turn_id == turn && *step_id == step
                        )
                    })
                {
                    // The model call already committed; only the checkpoint is missing.
                    self.append(
                        session,
                        SessionEvent::StepCommitted {
                            turn_id: turn,
                            step_id: step,
                        },
                        Some(dedup::step_committed(step)),
                    )
                    .await?;
                    return self
                        .commit_turn(session, turn, TurnEndReason::Completed, events)
                        .await;
                }

                self.run_steps(session, &config, turn, open_step, events)
                    .await
            }
        }
    }

    async fn run_steps(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        existing_step: Option<(StepId, u64)>,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let mut pending = existing_step;
        let mut steps_started = 0u32;

        loop {
            let (step, watermark) = match pending.take() {
                Some(step) => step,
                None => {
                    if let Some(max) = config.turn.max_steps_per_turn
                        && steps_started >= max
                    {
                        return self
                            .commit_turn(session, turn, TurnEndReason::MaxStepsExceeded, events)
                            .await;
                    }
                    let step = StepId::from_bytes(fresh_id());
                    let log_id = self
                        .append(
                            session,
                            SessionEvent::StepStarted {
                                turn_id: turn,
                                step_id: step,
                            },
                            None,
                        )
                        .await?;
                    let seq = self.seq_of(session, log_id).await?;
                    steps_started += 1;
                    (step, seq)
                }
            };

            match self
                .run_step(session, config, turn, step, watermark, events)
                .await?
            {
                StepOutcome::Continue => {}
                StepOutcome::Turn(reason) => {
                    return self.commit_turn(session, turn, reason, events).await;
                }
            }
        }
    }

    async fn run_step(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        step: StepId,
        watermark: u64,
        events: &EventSink,
    ) -> Result<StepOutcome, AgentError> {
        // The projection is computed once per step; retries reuse it (L12).
        let request = self.build_request(session, config, watermark).await?;
        let max_attempts = config.turn.max_attempts.get();
        let mut attempt = 1u32;

        loop {
            let provider =
                self.providers
                    .get(&config.model.provider)
                    .ok_or_else(|| AgentError::Corrupt {
                        session,
                        detail: format!("provider {} is not registered", config.model.provider),
                    })?;

            let mut stream = provider.stream(request.clone());
            let mut completed: BTreeMap<u32, Item> = BTreeMap::new();
            let mut terminal: Option<(StopReason, Usage)> = None;
            let mut failure: Option<ProviderError> = None;

            while let Some(event) = stream.next().await {
                match event {
                    Ok(StreamEvent::PartDelta { delta, .. }) => {
                        events.push(Ok(AgentEvent::ModelDelta { turn, step, delta }));
                    }
                    Ok(StreamEvent::ItemCompleted {
                        output_index, item, ..
                    }) => {
                        completed.insert(output_index, item);
                    }
                    Ok(StreamEvent::ResponseEnded {
                        stop_reason, usage, ..
                    }) => {
                        terminal = Some((stop_reason, usage));
                        break;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
            drop(stream);

            if let Some((stop_reason, usage)) = terminal {
                let items: Vec<Item> = completed.into_values().collect();
                self.append(
                    session,
                    SessionEvent::AssistantMessage {
                        turn_id: turn,
                        step_id: step,
                        items,
                        raw: None,
                        model: config.model.clone(),
                        usage,
                        stop_reason: stop_reason.clone(),
                    },
                    Some(dedup::assistant_message(turn, step)),
                )
                .await?;
                self.append(
                    session,
                    SessionEvent::StepCommitted {
                        turn_id: turn,
                        step_id: step,
                    },
                    Some(dedup::step_committed(step)),
                )
                .await?;

                return Ok(match stop_reason {
                    StopReason::PauseTurn => StepOutcome::Continue,
                    StopReason::ContextWindowExceeded => {
                        StepOutcome::Turn(TurnEndReason::ContextExhausted)
                    }
                    _ => StepOutcome::Turn(TurnEndReason::Completed),
                });
            }

            // A stream that ends without a terminal event violates S2/S8.
            let error = failure.unwrap_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::StreamProtocol,
                    "model stream ended without ResponseEnded",
                )
            });
            let will_retry = error.retryable && !error.context_exceeded() && attempt < max_attempts;
            let backoff = if will_retry {
                backoff(&config.turn, attempt)
            } else {
                Duration::ZERO
            };
            let partial: Option<Vec<Item>> = if completed.is_empty() {
                None
            } else {
                Some(completed.into_values().collect())
            };

            self.append(
                session,
                SessionEvent::ModelCallAborted {
                    turn_id: turn,
                    step_id: step,
                    attempt,
                    kind: if error.retryable {
                        AbortKind::Transport
                    } else {
                        AbortKind::Rejected
                    },
                    partial,
                    model: config.model.clone(),
                    backoff_ms: backoff.as_millis() as u64,
                    usage: error.partial_usage,
                },
                None,
            )
            .await?;

            if will_retry {
                if !backoff.is_zero() {
                    tokio::time::sleep(backoff).await;
                }
                attempt += 1;
                continue;
            }

            self.append(
                session,
                SessionEvent::StepCommitted {
                    turn_id: turn,
                    step_id: step,
                },
                Some(dedup::step_committed(step)),
            )
            .await?;
            return Ok(StepOutcome::Turn(if error.context_exceeded() {
                TurnEndReason::ContextExhausted
            } else {
                TurnEndReason::Failed
            }));
        }
    }

    /// Project the log at the `StepStarted.seq` waterline. `CompactionApplied`
    /// is a rewrite operator and is folded regardless of the waterline.
    async fn build_request(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        watermark: u64,
    ) -> Result<ModelRequest, AgentError> {
        let entries = self.scan(session).await?;
        let mut visible = Vec::new();
        let mut prefix_epoch = 0u32;
        for entry in entries {
            let event = decode_entry(&entry).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
            match event {
                SessionEvent::CompactionApplied { .. } => {
                    prefix_epoch += 1;
                    visible.push(entry);
                }
                _ if entry.seq < watermark => visible.push(entry),
                _ => {}
            }
        }
        let items = asterpath_protocol::history(&visible).map_err(|error| AgentError::Corrupt {
            session,
            detail: error.to_string(),
        })?;
        Ok(ModelRequest::new(
            config.model.clone(),
            prefix_epoch,
            config.system.clone(),
            items,
            config.tools.clone(),
            &config.request,
        ))
    }

    async fn commit_turn(
        &self,
        session: SessionId,
        turn: TurnId,
        reason: TurnEndReason,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        self.append(
            session,
            SessionEvent::TurnCommitted {
                turn_id: turn,
                reason: reason.clone(),
            },
            Some(dedup::turn_committed(turn)),
        )
        .await?;
        events.push(Ok(AgentEvent::TurnCommitted { turn, reason }));
        Ok(())
    }

    async fn seq_of(&self, session: SessionId, log_id: LogId) -> Result<u64, AgentError> {
        self.scan(session)
            .await?
            .into_iter()
            .find(|entry| entry.log_id == log_id)
            .map(|entry| entry.seq)
            .ok_or_else(|| AgentError::Corrupt {
                session,
                detail: "appended record is missing from the log".to_owned(),
            })
    }
}

impl AgentRunner for Agent {
    fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>> {
        let session = req.session;
        if !self.acquire(session) {
            return stream::once(async move { Err(AgentError::Busy { session }) }).boxed();
        }
        let guard = BusyGuard {
            busy: Arc::clone(&self.busy),
            session,
        };
        let sink = Arc::new(EventSink::default());
        let future_sink = Arc::clone(&sink);
        let future: BoxFuture<'_, ()> = Box::pin(async move {
            self.execute(session, &future_sink).await;
        });

        Box::pin(DriveStream {
            future,
            sink,
            // Dropping the stream releases single-flight; it does not cancel.
            _guard: guard,
            done: false,
        })
    }

    fn compact_now(&self, session: SessionId) -> BoxFuture<'_, Result<CompactResult, AgentError>> {
        Box::pin(async move {
            let entries = decode_entries(self.scan(session).await?, session)?;
            let mut intents = OpenIntents::default();
            for (entry, event) in &entries {
                intents.apply(event, entry.dedup_key.as_ref());
            }
            if !intents.is_idle()
                || self
                    .busy
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(&session)
            {
                return Err(AgentError::Busy { session });
            }
            Ok(CompactResult::NotConfigured)
        })
    }
}

enum StepOutcome {
    /// `pause_turn`: continue the same turn with another step.
    Continue,
    Turn(TurnEndReason),
}

struct BusyGuard {
    busy: Arc<Mutex<HashSet<SessionId>>>,
    session: SessionId,
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.busy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.session);
    }
}

#[derive(Default)]
struct EventSink {
    queue: Mutex<VecDeque<Result<AgentEvent, AgentError>>>,
}

impl EventSink {
    fn push(&self, event: Result<AgentEvent, AgentError>) {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(event);
    }

    fn pop(&self) -> Option<Result<AgentEvent, AgentError>> {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }
}

/// Drives the turn and the stream together: every event is yielded as soon as
/// the loop produces it, without a background task.
struct DriveStream<'a> {
    future: BoxFuture<'a, ()>,
    sink: Arc<EventSink>,
    _guard: BusyGuard,
    done: bool,
}

impl Stream for DriveStream<'_> {
    type Item = Result<AgentEvent, AgentError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(event) = this.sink.pop() {
                return Poll::Ready(Some(event));
            }
            if this.done {
                return Poll::Ready(None);
            }
            match this.future.as_mut().poll(cx) {
                Poll::Ready(()) => this.done = true,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn decode_entries(
    entries: Vec<LogEntry>,
    session: SessionId,
) -> Result<Vec<(LogEntry, SessionEvent)>, AgentError> {
    entries
        .into_iter()
        .map(|entry| {
            let event = decode_entry(&entry).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
            Ok((entry, event))
        })
        .collect()
}

fn has_pending_input(decoded: &[(LogEntry, SessionEvent)]) -> bool {
    let mut pending = false;
    for (_, event) in decoded {
        match event {
            SessionEvent::InputRecorded { .. } => pending = true,
            SessionEvent::StepStarted { .. } => pending = false,
            _ => {}
        }
    }
    pending
}

/// Exponential backoff with jitter, capped at `max_backoff`.
fn backoff(config: &TurnConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10);
    let capped = config
        .initial_backoff
        .saturating_mul(1u32 << exponent)
        .min(config.max_backoff);
    let jitter_max = capped / 4;
    if jitter_max.is_zero() {
        return capped;
    }
    let jitter = Duration::from_nanos(random_u64() % (jitter_max.as_nanos() as u64 + 1));
    (capped + jitter).min(config.max_backoff)
}

fn random_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as u64 ^ elapsed.as_secs())
        .unwrap_or_default()
}

/// Process-local unique id source; session scoping makes collisions moot.
fn fresh_id() -> [u8; 16] {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or_default();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0u8; 16];
    bytes[..8]
        .copy_from_slice(&(nanos ^ (std::process::id() as u64).rotate_left(17)).to_le_bytes());
    bytes[8..].copy_from_slice(&counter.to_le_bytes());
    bytes
}
