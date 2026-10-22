//! Native execution tracing: a deterministic projection of the event log.
//!
//! There is no trace sink and no trace table. [`TracingQuery`] folds the
//! injected [`EventLog`] on demand; [`TracingTap`] pushes the same lifecycle off
//! the typed event bus. `recorded_at` only feeds timings and statuses here, never
//! recovery or projection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_core::{
    EventLog, LifecycleError, LogEntry, LogId, LogIdRange, LogQuery, Module, ModuleCtx, SessionId,
    SignalId, StepId, StorageError, TimerId, Timestamp, ToolCallId, TurnId,
};
use asterpath_protocol::{
    APPROVAL_SIGNAL_PREFIX, AbortKind, AttemptEvent, ChildOutcome, CompactionAttrs, ConfigSnapshot,
    ErrorType, ModelCallAttrs, RecordAppended, ResumeEvent, SessionAttrs, SessionEvent, SideEffect,
    Span, SpanAttributes, SpanFilter, SpanId, SpanKind, SpanStatus, SpanUpdate, SubagentAttrs,
    ToolCallAttrs, ToolErrorKind, ToolResultStatus, TraceError, TraceId, TraceQuery, TraceTap,
    TurnAttrs, TurnEndReason, WaitAttrs, WaitKind, decode_entry,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use tokio::sync::broadcast;

/// Pull seam: folds one session's log per request.
pub struct TracingQuery {
    log: Arc<dyn EventLog>,
}

impl TracingQuery {
    pub fn new(log: Arc<dyn EventLog>) -> Self {
        Self { log }
    }
}

impl TraceQuery for TracingQuery {
    fn spans(&self, filter: SpanFilter) -> BoxStream<'_, Result<Span, TraceError>> {
        let log = Arc::clone(&self.log);
        Box::pin(
            stream::once(async move {
                let Some(session) = filter.session else {
                    // The log has no session enumeration; a trace is per session.
                    return Vec::new();
                };
                match fold_session(log.as_ref(), session).await {
                    Ok(spans) => spans
                        .into_iter()
                        .filter(|span| matches_filter(span, &filter))
                        .take(filter.limit.unwrap_or(usize::MAX))
                        .map(Ok)
                        .collect(),
                    Err(error) => vec![Err(error)],
                }
            })
            .flat_map(stream::iter),
        )
    }
}

fn matches_filter(span: &Span, filter: &SpanFilter) -> bool {
    filter.session.is_none_or(|session| span.session == session)
        && filter.turn.is_none_or(|turn| span.turn == Some(turn))
        && filter.step.is_none_or(|step| span.step == Some(step))
        && filter
            .tool_call
            .is_none_or(|tool_call| span.tool_call == Some(tool_call))
        && filter.kind.is_none_or(|kind| span.kind == kind)
        && filter.status.is_none_or(|status| span.status == status)
        && (!filter.open_only || span.ended_at.is_none())
}

async fn fold_session(log: &dyn EventLog, session: SessionId) -> Result<Vec<Span>, TraceError> {
    let entries = scan(log, session).await?;
    let mut decoded = Vec::with_capacity(entries.len());
    for entry in entries {
        let event = decode_entry(&entry).map_err(|error| TraceError::Corrupt {
            session,
            detail: error.to_string(),
        })?;
        decoded.push((entry, event));
    }

    let created = decoded.iter().find_map(|(entry, event)| match event {
        SessionEvent::SessionCreated { config, parent } => Some((entry, config, parent)),
        _ => None,
    });
    let Some((created_entry, config, parent)) = created else {
        return Err(TraceError::Corrupt {
            session,
            detail: "session log has no SessionCreated".to_owned(),
        });
    };
    let config = config.clone();
    let parent_session = parent.as_ref().map(|parent| parent.session);

    let mut folder = Folder::new(session, &config, created_entry, parent_session);
    for (entry, event) in &decoded {
        folder.apply(entry, event);
    }
    folder.resolve_child_root(log).await;
    Ok(folder.finish())
}

async fn scan(log: &dyn EventLog, session: SessionId) -> Result<Vec<LogEntry>, TraceError> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(entry) => entries.push(entry),
            Err(StorageError::Corrupt { detail, .. }) => {
                return Err(TraceError::Corrupt { session, detail });
            }
            Err(error) => return Err(TraceError::Backend(error.to_string())),
        }
    }
    if entries.is_empty() {
        return Err(TraceError::NotFound { session });
    }
    Ok(entries)
}

#[derive(Clone, Copy)]
struct Opening {
    log_id: LogId,
    seq: u64,
    at: Timestamp,
}

struct Folder {
    session: SessionId,
    trace: TraceId,
    config: ConfigSnapshot,
    spans: Vec<Span>,
    openings: Vec<Opening>,
    session_span: usize,
    parent_session: Option<SessionId>,
    turns: HashMap<TurnId, usize>,
    steps: HashMap<StepId, usize>,
    tools: HashMap<ToolCallId, usize>,
    subagents: HashMap<SessionId, usize>,
    timers: HashMap<TimerId, usize>,
    signals: HashMap<SignalId, usize>,
    compaction_by_key: HashMap<String, usize>,
    open_compactions: Vec<usize>,
    models: HashMap<(TurnId, StepId), usize>,
    approval_waits: HashMap<ToolCallId, usize>,
    model_previous_backoff: HashMap<usize, u64>,
    model_last_abort: HashMap<usize, (LogId, Timestamp, ErrorType)>,
    hosts: HashMap<LogId, usize>,
    stack: Vec<usize>,
    latest_resume_seq: u64,
    last_compaction_seq: Option<u64>,
    turn_last_abort: HashMap<TurnId, ErrorType>,
    turn_end: HashMap<TurnId, TurnEndReason>,
    turn_count: u32,
    step_counts: HashMap<TurnId, u32>,
}

impl Folder {
    fn new(
        session: SessionId,
        config: &ConfigSnapshot,
        created: &LogEntry,
        parent_session: Option<SessionId>,
    ) -> Self {
        let trace = TraceId::from_session(session);
        let opening = Opening {
            log_id: created.log_id,
            seq: created.seq,
            at: created.recorded_at,
        };
        let session_span = Span {
            trace,
            span: span_id(session, created.log_id, SpanKind::Session),
            parent_span: None,
            parent_trace: None,
            child_trace: None,
            kind: SpanKind::Session,
            name: "session".to_owned(),
            session,
            turn: None,
            step: None,
            tool_call: None,
            started_at: created.recorded_at,
            ended_at: None,
            status: SpanStatus::Unset,
            error_type: None,
            log_range: LogIdRange {
                first: created.log_id,
                last: created.log_id,
            },
            replay: false,
            replay_of: None,
            attributes: SpanAttributes::Session(SessionAttrs {
                parent_session,
                fork_source_log: None,
                resumes: Vec::new(),
            }),
        };
        Self {
            session,
            trace,
            config: config.clone(),
            spans: vec![session_span],
            openings: vec![opening],
            session_span: 0,
            parent_session,
            turns: HashMap::new(),
            steps: HashMap::new(),
            tools: HashMap::new(),
            subagents: HashMap::new(),
            timers: HashMap::new(),
            signals: HashMap::new(),
            compaction_by_key: HashMap::new(),
            open_compactions: Vec::new(),
            models: HashMap::new(),
            approval_waits: HashMap::new(),
            model_previous_backoff: HashMap::new(),
            model_last_abort: HashMap::new(),
            hosts: HashMap::new(),
            stack: vec![0],
            latest_resume_seq: 0,
            last_compaction_seq: None,
            turn_last_abort: HashMap::new(),
            turn_end: HashMap::new(),
            turn_count: 0,
            step_counts: HashMap::new(),
        }
    }

    fn push(&mut self, span: Span, opening: Opening) -> usize {
        let index = self.spans.len();
        self.spans.push(span);
        self.openings.push(opening);
        index
    }

    fn deepest_container(&self) -> usize {
        *self.stack.last().unwrap_or(&self.session_span)
    }

    #[allow(clippy::too_many_arguments)]
    fn new_span(
        &self,
        kind: SpanKind,
        name: String,
        opening: Opening,
        parent_span: Option<SpanId>,
        turn: Option<TurnId>,
        step: Option<StepId>,
        tool_call: Option<ToolCallId>,
        attributes: SpanAttributes,
    ) -> Span {
        Span {
            trace: self.trace,
            span: span_id(self.session, opening.log_id, kind),
            parent_span,
            parent_trace: None,
            child_trace: None,
            kind,
            name,
            session: self.session,
            turn,
            step,
            tool_call,
            started_at: opening.at,
            ended_at: None,
            status: SpanStatus::Unset,
            error_type: None,
            log_range: LogIdRange {
                first: opening.log_id,
                last: opening.log_id,
            },
            replay: false,
            replay_of: None,
            attributes,
        }
    }

    fn close(
        &mut self,
        index: usize,
        entry: &LogEntry,
        status: SpanStatus,
        error: Option<ErrorType>,
    ) {
        let span = &mut self.spans[index];
        span.ended_at = Some(entry.recorded_at);
        span.status = status;
        span.error_type = error;
        span.log_range.last = entry.log_id;
        if self.latest_resume_seq > self.openings[index].seq {
            span.replay = true;
        }
    }

    fn apply(&mut self, entry: &LogEntry, event: &SessionEvent) {
        let opening = Opening {
            log_id: entry.log_id,
            seq: entry.seq,
            at: entry.recorded_at,
        };
        match event {
            SessionEvent::SessionCreated { .. } => {}
            SessionEvent::ContextForked { source_log, .. } => {
                if let SpanAttributes::Session(attrs) =
                    &mut self.spans[self.session_span].attributes
                {
                    attrs.fork_source_log = Some(*source_log);
                }
            }
            SessionEvent::StreamResumed { from, reason } => {
                self.latest_resume_seq = entry.seq;
                if let SpanAttributes::Session(attrs) =
                    &mut self.spans[self.session_span].attributes
                {
                    attrs.resumes.push(ResumeEvent {
                        from: *from,
                        reason: reason.clone(),
                        at: entry.recorded_at,
                    });
                }
            }
            SessionEvent::TurnStarted { turn_id } => {
                self.turn_count += 1;
                let parent = self.spans[self.session_span].span;
                let span = self.new_span(
                    SpanKind::Turn,
                    format!("turn {}", self.turn_count),
                    opening,
                    Some(parent),
                    Some(*turn_id),
                    None,
                    None,
                    SpanAttributes::Turn(TurnAttrs {
                        index: self.turn_count,
                    }),
                );
                let index = self.push(span, opening);
                self.turns.insert(*turn_id, index);
                self.stack.push(index);
            }
            SessionEvent::StepStarted { turn_id, step_id } => {
                let turn_span = self.turns.get(turn_id).copied();
                let parent = turn_span.map(|index| self.spans[index].span);
                let step_index = self.step_counts.entry(*turn_id).or_insert(0);
                *step_index += 1;
                let step_name_index = *step_index;
                let span = self.new_span(
                    SpanKind::Step,
                    format!("step {step_name_index}"),
                    opening,
                    parent,
                    Some(*turn_id),
                    Some(*step_id),
                    None,
                    SpanAttributes::Step,
                );
                let index = self.push(span, opening);
                self.steps.insert(*step_id, index);
                self.stack.push(index);

                let request_model = self.config.model.clone();
                let model_span = self.new_span(
                    SpanKind::ModelCall,
                    format!("chat {}", request_model.model.as_str()),
                    opening,
                    Some(self.spans[index].span),
                    Some(*turn_id),
                    Some(*step_id),
                    None,
                    SpanAttributes::ModelCall(ModelCallAttrs {
                        provider: request_model.provider.as_str().to_owned(),
                        request_model,
                        response_model: None,
                        response_id: None,
                        finish_reasons: Vec::new(),
                        usage: None,
                        attempt_count: 1,
                        compacted: false,
                        attempts: Vec::new(),
                    }),
                );
                let model_index = self.push(model_span, opening);
                self.models.insert((*turn_id, *step_id), model_index);
            }
            SessionEvent::AssistantMessage {
                turn_id,
                step_id,
                model,
                usage,
                stop_reason,
                raw,
                ..
            } => {
                let Some(index) = self.models.get(&(*turn_id, *step_id)).copied() else {
                    return;
                };
                if let SpanAttributes::ModelCall(attrs) = &mut self.spans[index].attributes {
                    attrs.request_model = model.clone();
                    attrs.provider = model.provider.as_str().to_owned();
                    attrs.response_model = raw
                        .as_ref()
                        .and_then(|raw| raw.model.as_ref())
                        .map(|model| model.as_str().to_owned());
                    attrs.response_id = raw.as_ref().and_then(response_id);
                    attrs.finish_reasons = vec![stop_reason.clone()];
                    attrs.usage = Some(*usage);
                    attrs.attempt_count = 1 + attrs.attempts.len() as u32;
                    attrs.compacted = self
                        .last_compaction_seq
                        .is_some_and(|sequence| sequence < entry.seq);
                }
                self.close(index, entry, SpanStatus::Ok, None);
            }
            SessionEvent::ModelCallAborted {
                turn_id,
                step_id,
                attempt,
                kind,
                model,
                backoff_ms,
                usage,
                ..
            } => {
                let Some(index) = self.models.get(&(*turn_id, *step_id)).copied() else {
                    return;
                };
                let backoff_before = self
                    .model_previous_backoff
                    .get(&index)
                    .copied()
                    .unwrap_or(0);
                self.model_previous_backoff.insert(index, *backoff_ms);
                let error = abort_error(*kind);
                let replayed = self.latest_resume_seq > self.openings[index].seq;
                if let SpanAttributes::ModelCall(attrs) = &mut self.spans[index].attributes {
                    attrs.attempts.push(AttemptEvent {
                        attempt: *attempt,
                        kind: *kind,
                        error_type: error,
                        backoff_before_ms: backoff_before,
                        partial_usage: *usage,
                        replayed,
                    });
                    attrs.attempt_count = 1 + attrs.attempts.len() as u32;
                    attrs.request_model = model.clone();
                    attrs.provider = model.provider.as_str().to_owned();
                }
                if replayed {
                    self.spans[index].replay = true;
                }
                self.turn_last_abort.insert(*turn_id, error);
                match kind {
                    AbortKind::Rejected => {
                        self.model_last_abort
                            .insert(index, (entry.log_id, entry.recorded_at, error));
                        self.close(index, entry, SpanStatus::Error, Some(error));
                    }
                    AbortKind::Cancelled => {
                        self.close(index, entry, SpanStatus::Cancelled, None);
                    }
                    _ => {
                        // A retryable attempt. `backoff_ms == 0` cannot prove a
                        // retry is over: forced compaction retries too. The span
                        // closes on success or when the step commit shows that
                        // no further attempt will come.
                        self.model_last_abort
                            .insert(index, (entry.log_id, entry.recorded_at, error));
                        if self.latest_resume_seq > self.openings[index].seq {
                            self.spans[index].replay = true;
                        }
                    }
                }
            }
            SessionEvent::ToolCallRequested {
                turn_id,
                step_id,
                tool_call_id,
                name,
                replay,
                provenance,
                ..
            } => {
                let parent = self
                    .steps
                    .get(step_id)
                    .copied()
                    .or_else(|| self.step_for(*turn_id, *step_id))
                    .map(|index| self.spans[index].span);
                let side_effect = self
                    .config
                    .tools
                    .iter()
                    .find(|descriptor| descriptor.name == *name)
                    .map_or(SideEffect::ReadOnly, |descriptor| descriptor.side_effect);
                let span = self.new_span(
                    SpanKind::ToolCall,
                    format!("execute_tool {}", name.as_str()),
                    opening,
                    parent,
                    Some(*turn_id),
                    Some(*step_id),
                    Some(*tool_call_id),
                    SpanAttributes::ToolCall(ToolCallAttrs {
                        name: name.clone(),
                        side_effect,
                        replay: *replay,
                        provenance: *provenance,
                        truncated: false,
                    }),
                );
                let index = self.push(span, opening);
                self.tools.insert(*tool_call_id, index);
                self.stack.push(index);
            }
            SessionEvent::ToolResult {
                tool_call_id,
                status,
                error,
                truncated,
                ..
            } => {
                let Some(index) = self.tools.get(tool_call_id).copied() else {
                    return;
                };
                if let SpanAttributes::ToolCall(attrs) = &mut self.spans[index].attributes {
                    attrs.truncated = *truncated;
                }
                let (span_status, error_type) = tool_status(*status, error.as_ref());
                self.close(index, entry, span_status, error_type);
                self.stack.retain(|open| *open != index);
            }
            SessionEvent::StepCommitted { turn_id, step_id } => {
                if let Some(index) = self.models.get(&(*turn_id, *step_id)).copied()
                    && self.spans[index].ended_at.is_none()
                    && let Some((log_id, at, error)) = self.model_last_abort.get(&index).copied()
                {
                    if let SpanAttributes::ModelCall(attrs) = &mut self.spans[index].attributes {
                        attrs.compacted = self
                            .last_compaction_seq
                            .is_some_and(|sequence| sequence < entry.seq);
                    }
                    self.spans[index].ended_at = Some(at);
                    self.spans[index].status = SpanStatus::Error;
                    self.spans[index].error_type = Some(error);
                    self.spans[index].log_range.last = log_id;
                    if self.latest_resume_seq > self.openings[index].seq {
                        self.spans[index].replay = true;
                    }
                }
                if let Some(index) = self.steps.get(step_id).copied() {
                    self.close(index, entry, SpanStatus::Ok, None);
                    self.stack.retain(|open| *open != index);
                }
            }
            SessionEvent::TurnCommitted { turn_id, reason } => {
                if let Some(index) = self.turns.get(turn_id).copied() {
                    let (status, error) = turn_status(reason, self.turn_last_abort.get(turn_id));
                    self.close(index, entry, status, error);
                    self.stack.retain(|open| *open != index);
                }
                self.turn_end.insert(*turn_id, reason.clone());
            }
            SessionEvent::CompactionStarted {
                trigger,
                replaces_from: _,
                caused_by: _,
            } => {
                let parent = self
                    .stack
                    .iter()
                    .rev()
                    .find(|&&index| {
                        matches!(
                            self.spans[index].kind,
                            SpanKind::Step | SpanKind::Turn | SpanKind::Session
                        )
                    })
                    .copied()
                    .unwrap_or(self.session_span);
                let (turn, step) = (self.spans[parent].turn, self.spans[parent].step);
                let span = self.new_span(
                    SpanKind::Compaction,
                    format!("compact {}", self.config.model.model.as_str()),
                    opening,
                    Some(self.spans[parent].span),
                    turn,
                    step,
                    None,
                    SpanAttributes::Compaction(CompactionAttrs {
                        trigger: *trigger,
                        replaces: None,
                        model: None,
                        usage: None,
                    }),
                );
                let index = self.push(span, opening);
                if let Some(key) = &entry.dedup_key {
                    self.compaction_by_key
                        .insert(key.as_str().to_owned(), index);
                }
                self.open_compactions.push(index);
            }
            SessionEvent::CompactionApplied {
                replaces,
                model,
                usage,
                ..
            } => {
                let Some(index) = self.compaction_span(entry) else {
                    return;
                };
                if let SpanAttributes::Compaction(attrs) = &mut self.spans[index].attributes {
                    attrs.replaces = Some(*replaces);
                    attrs.model = Some(model.clone());
                    attrs.usage = Some(*usage);
                }
                self.spans[index].name = format!("compact {}", model.model.as_str());
                self.close(index, entry, SpanStatus::Ok, None);
                self.last_compaction_seq = Some(entry.seq);
            }
            SessionEvent::CompactionAborted { .. } => {
                let Some(index) = self.compaction_span(entry) else {
                    return;
                };
                self.close(
                    index,
                    entry,
                    SpanStatus::Error,
                    Some(ErrorType::CompactionFailed),
                );
            }
            SessionEvent::ChildSessionSpawned {
                child,
                parent_tool_call_id,
                ..
            } => {
                let parent = self.tools.get(parent_tool_call_id).copied();
                let (turn, step) = parent
                    .map(|index| (self.spans[index].turn, self.spans[index].step))
                    .unwrap_or((None, None));
                let mut span = self.new_span(
                    SpanKind::Subagent,
                    format!("invoke_agent {}", child.to_hex()),
                    opening,
                    parent.map(|index| self.spans[index].span),
                    turn,
                    step,
                    Some(*parent_tool_call_id),
                    SpanAttributes::Subagent(SubagentAttrs {
                        child: *child,
                        outcome: None,
                    }),
                );
                span.child_trace = Some(TraceId::from_session(*child));
                let index = self.push(span, opening);
                self.subagents.insert(*child, index);
                self.stack.push(index);
            }
            SessionEvent::ChildSessionCompleted { child, outcome } => {
                let Some(index) = self.subagents.get(child).copied() else {
                    return;
                };
                if let SpanAttributes::Subagent(attrs) = &mut self.spans[index].attributes {
                    attrs.outcome = Some(outcome.clone());
                }
                self.close(index, entry, child_status(outcome), None);
                self.stack.retain(|open| *open != index);
            }
            SessionEvent::TimerScheduled {
                timer_id,
                deadline,
                caused_by,
            } => {
                self.open_wait(
                    entry,
                    WaitKind::Timer,
                    Some(*deadline),
                    Some(*caused_by),
                    WaitTarget::Timer(*timer_id),
                );
            }
            SessionEvent::TimerFired { timer_id } => {
                if let Some(index) = self.timers.get(timer_id).copied() {
                    self.close(index, entry, SpanStatus::Ok, None);
                }
            }
            SessionEvent::TimerCancelled { timer_id } => {
                if let Some(index) = self.timers.get(timer_id).copied() {
                    self.close(index, entry, SpanStatus::Cancelled, None);
                }
            }
            SessionEvent::SignalAwaited {
                signal_id,
                caused_by,
                ..
            } => {
                self.open_wait(
                    entry,
                    WaitKind::Signal,
                    None,
                    Some(*caused_by),
                    WaitTarget::Signal(*signal_id),
                );
            }
            SessionEvent::SignalReceived {
                signal_id, name, ..
            } => {
                if name.as_str().starts_with(APPROVAL_SIGNAL_PREFIX) {
                    self.close_approval(*signal_id, entry);
                } else if let Some(index) = self.signals.get(signal_id).copied() {
                    self.close(index, entry, SpanStatus::Ok, None);
                }
            }
            _ => {}
        }

        let host = self.deepest_container();
        self.hosts.insert(entry.log_id, host);
        let last = self.spans[self.session_span].log_range.last;
        if entry.log_id > last {
            self.spans[self.session_span].log_range.last = entry.log_id;
        }
    }

    fn step_for(&self, turn: TurnId, step: StepId) -> Option<usize> {
        let index = self.steps.get(&step).copied()?;
        (self.spans[index].turn == Some(turn)).then_some(index)
    }

    fn open_wait(
        &mut self,
        entry: &LogEntry,
        wait: WaitKind,
        deadline: Option<Timestamp>,
        caused_by: Option<LogId>,
        target: WaitTarget,
    ) {
        let opening = Opening {
            log_id: entry.log_id,
            seq: entry.seq,
            at: entry.recorded_at,
        };
        let parent = caused_by.and_then(|log_id| self.hosts.get(&log_id).copied());
        let (turn, step, tool_call) = parent
            .map(|index| {
                (
                    self.spans[index].turn,
                    self.spans[index].step,
                    self.spans[index].tool_call,
                )
            })
            .unwrap_or((None, None, None));
        let span = self.new_span(
            SpanKind::Wait,
            match wait {
                WaitKind::Timer => "wait timer".to_owned(),
                WaitKind::Signal => "wait signal".to_owned(),
                WaitKind::Approval => "wait approval".to_owned(),
            },
            opening,
            parent.map(|index| self.spans[index].span),
            turn,
            step,
            tool_call,
            SpanAttributes::Wait(WaitAttrs {
                wait,
                deadline,
                caused_by,
            }),
        );
        let index = self.push(span, opening);
        match target {
            WaitTarget::Timer(timer) => {
                self.timers.insert(timer, index);
            }
            WaitTarget::Signal(signal) => {
                self.signals.insert(signal, index);
            }
        }
    }

    /// An approval wait is only materialized when its decision was recorded; the
    /// opening record is the tool call, shared with the tool span.
    fn close_approval(&mut self, signal: SignalId, entry: &LogEntry) {
        let tool_call = ToolCallId::from_bytes(*signal.as_bytes());
        let Some(tool_index) = self.tools.get(&tool_call).copied() else {
            return;
        };
        let index = match self.approval_waits.get(&tool_call).copied() {
            Some(index) => index,
            None => {
                let opening = self.openings[tool_index];
                let (turn, step) = (self.spans[tool_index].turn, self.spans[tool_index].step);
                let span = self.new_span(
                    SpanKind::Wait,
                    "wait approval".to_owned(),
                    opening,
                    Some(self.spans[tool_index].span),
                    turn,
                    step,
                    Some(tool_call),
                    SpanAttributes::Wait(WaitAttrs {
                        wait: WaitKind::Approval,
                        deadline: None,
                        caused_by: Some(opening.log_id),
                    }),
                );
                let index = self.push(span, opening);
                self.approval_waits.insert(tool_call, index);
                index
            }
        };
        self.close(index, entry, SpanStatus::Ok, None);
    }

    fn compaction_span(&self, entry: &LogEntry) -> Option<usize> {
        if let Some(key) = &entry.dedup_key
            && let Some(index) = self.compaction_by_key.get(key.as_str())
        {
            return Some(*index);
        }
        self.open_compactions
            .iter()
            .rev()
            .copied()
            .find(|index| self.spans[*index].ended_at.is_none())
    }

    async fn resolve_child_root(&mut self, log: &dyn EventLog) {
        let Some(parent) = self.parent_session else {
            return;
        };
        self.spans[self.session_span].parent_trace = Some(TraceId::from_session(parent));
        let Ok(entries) = scan(log, parent).await else {
            return;
        };
        for entry in entries {
            let Ok(SessionEvent::ChildSessionSpawned { child, .. }) = decode_entry(&entry) else {
                continue;
            };
            if child == self.session {
                self.spans[self.session_span].parent_span =
                    Some(span_id(parent, entry.log_id, SpanKind::Subagent));
                break;
            }
        }
    }

    fn finish(mut self) -> Vec<Span> {
        // A step's status follows the turn: cancellation is not a step error.
        for index in 0..self.spans.len() {
            if self.spans[index].kind != SpanKind::Step || self.spans[index].ended_at.is_none() {
                continue;
            }
            if let Some(turn) = self.spans[index].turn
                && self
                    .turn_end
                    .get(&turn)
                    .is_some_and(|reason| matches!(reason, TurnEndReason::Cancelled))
            {
                self.spans[index].status = SpanStatus::Cancelled;
            }
        }

        let mut order: Vec<usize> = (0..self.spans.len()).collect();
        order.sort_by_key(|&index| {
            let span = &self.spans[index];
            (
                span.started_at,
                span.log_range.first,
                kind_rank(span.kind),
                span.span,
            )
        });
        order
            .into_iter()
            .map(|index| self.spans[index].clone())
            .collect()
    }
}

enum WaitTarget {
    Timer(TimerId),
    Signal(SignalId),
}

fn turn_status(
    reason: &TurnEndReason,
    last_abort: Option<&ErrorType>,
) -> (SpanStatus, Option<ErrorType>) {
    match reason {
        TurnEndReason::Completed | TurnEndReason::Stopped { .. } => (SpanStatus::Ok, None),
        TurnEndReason::Cancelled => (SpanStatus::Cancelled, None),
        TurnEndReason::Failed => (
            SpanStatus::Error,
            Some(last_abort.copied().unwrap_or(ErrorType::Internal)),
        ),
        TurnEndReason::ContextExhausted => (SpanStatus::Error, Some(ErrorType::ContextExhausted)),
        TurnEndReason::CompactionFailed => (SpanStatus::Error, Some(ErrorType::CompactionFailed)),
        TurnEndReason::MaxStepsExceeded => (SpanStatus::Error, Some(ErrorType::MaxSteps)),
        _ => (SpanStatus::Error, Some(ErrorType::Other)),
    }
}

fn tool_status(
    status: ToolResultStatus,
    error: Option<&asterpath_protocol::ToolErrorInfo>,
) -> (SpanStatus, Option<ErrorType>) {
    match status {
        ToolResultStatus::Completed => (SpanStatus::Ok, None),
        ToolResultStatus::Failed => (
            SpanStatus::Error,
            Some(
                error
                    .map(|error| tool_error(error.kind))
                    .unwrap_or(ErrorType::Internal),
            ),
        ),
        ToolResultStatus::Interrupted => (SpanStatus::Interrupted, None),
        _ => (SpanStatus::Error, Some(ErrorType::Other)),
    }
}

fn tool_error(kind: ToolErrorKind) -> ErrorType {
    match kind {
        ToolErrorKind::InvalidArgs => ErrorType::ToolInvalidArgs,
        ToolErrorKind::Denied => ErrorType::ToolDenied,
        ToolErrorKind::Timeout => ErrorType::ToolTimeout,
        ToolErrorKind::Cancelled => ErrorType::ToolCancelled,
        ToolErrorKind::Panic => ErrorType::ToolPanic,
        ToolErrorKind::Transport => ErrorType::Transport,
        ToolErrorKind::NotFound => ErrorType::ToolNotFound,
        ToolErrorKind::Internal => ErrorType::Internal,
        _ => ErrorType::Other,
    }
}

fn abort_error(kind: AbortKind) -> ErrorType {
    match kind {
        AbortKind::Transport => ErrorType::Transport,
        AbortKind::Rejected => ErrorType::ProviderRejected,
        AbortKind::Cancelled => ErrorType::Other,
        AbortKind::Crash => ErrorType::Internal,
        _ => ErrorType::Other,
    }
}

fn child_status(outcome: &ChildOutcome) -> SpanStatus {
    match outcome {
        ChildOutcome::Completed { .. } => SpanStatus::Ok,
        ChildOutcome::Failed { .. } | ChildOutcome::Lost { .. } => SpanStatus::Error,
        ChildOutcome::Cancelled { .. } => SpanStatus::Cancelled,
        _ => SpanStatus::Error,
    }
}

fn response_id(raw: &asterpath_protocol::RawProviderData) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(raw.json.as_str()).ok()?;
    value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn kind_rank(kind: SpanKind) -> u8 {
    match kind {
        SpanKind::Session => 0,
        SpanKind::Turn => 1,
        SpanKind::Step => 2,
        SpanKind::ModelCall => 3,
        SpanKind::ToolCall => 4,
        SpanKind::Subagent => 5,
        SpanKind::Compaction => 6,
        SpanKind::Wait => 7,
        _ => 8,
    }
}

fn kind_tag(kind: SpanKind) -> u8 {
    kind_rank(kind)
}

/// `SpanId = hash(session, opening_log_id, kind)`; the hash is an internal
/// detail fixed for one build.
fn span_id(session: SessionId, log_id: LogId, kind: SpanKind) -> SpanId {
    let mut hash = Fnv::new(0xcbf29ce484222325);
    hash.write(session.as_bytes());
    hash.write(&log_id.as_i64().to_be_bytes());
    hash.write(&[kind_tag(kind)]);
    SpanId::from_bytes(hash.0.to_be_bytes())
}

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
}

/// Real-time seam: diffs the projection after each committed record.
pub struct TracingTap {
    log: Arc<dyn EventLog>,
    updates: broadcast::Sender<Result<SpanUpdate, TraceError>>,
    state: Mutex<HashMap<SessionId, (u64, Vec<Span>)>>,
}

impl TracingTap {
    pub fn new(log: Arc<dyn EventLog>) -> Self {
        let (updates, _) = broadcast::channel(1024);
        Self {
            log,
            updates,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Called from the event bus after a record becomes visible.
    pub async fn record_appended(&self, session: SessionId) {
        let spans = match fold_session(self.log.as_ref(), session).await {
            Ok(spans) => spans,
            Err(TraceError::NotFound { .. }) => return,
            Err(error) => {
                let _ = self.updates.send(Err(error));
                return;
            }
        };
        let sequence = spans
            .iter()
            .map(|span| span.log_range.last.as_i64() as u64)
            .max()
            .unwrap_or(0);

        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state
            .get(&session)
            .is_some_and(|(known, _)| *known > sequence)
        {
            // An out-of-order fold from a concurrent append; the newer one won.
            return;
        }
        let previous = state.get(&session).map(|(_, spans)| spans.clone());
        state.insert(session, (sequence, spans.clone()));
        drop(state);

        let notifications = match &previous {
            None => spans
                .iter()
                .cloned()
                .map(SpanUpdate::Started)
                .collect::<Vec<_>>(),
            Some(previous) => span_updates(previous, &spans),
        };
        for update in notifications {
            let _ = self.updates.send(Ok(update));
        }
    }
}

impl TraceTap for TracingTap {
    fn subscribe(&self) -> BoxStream<'_, Result<SpanUpdate, TraceError>> {
        let receiver = self.updates.subscribe();
        Box::pin(stream::unfold(receiver, |mut receiver| async move {
            loop {
                match receiver.recv().await {
                    Ok(update) => return Some((update, receiver)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        }))
    }
}

fn span_updates(previous: &[Span], current: &[Span]) -> Vec<SpanUpdate> {
    let mut updates = Vec::new();
    for span in current {
        match previous.iter().find(|known| known.span == span.span) {
            None => {
                updates.push(SpanUpdate::Started(span.clone()));
                // A wait materialized only when its decision was recorded can
                // already be terminal in the same fold.
                if span.ended_at.is_some() {
                    updates.push(SpanUpdate::Ended(span.clone()));
                }
            }
            Some(known) if known.ended_at.is_none() && span.ended_at.is_some() => {
                updates.push(SpanUpdate::Ended(span.clone()));
            }
            _ => {}
        }
    }
    updates
}

/// Module wrapper: registers the tap handler and exposes both seams.
pub struct TracingModule {
    query: Arc<TracingQuery>,
    tap: Arc<TracingTap>,
}

impl TracingModule {
    pub fn new(log: Arc<dyn EventLog>) -> Self {
        Self {
            query: Arc::new(TracingQuery::new(Arc::clone(&log))),
            tap: Arc::new(TracingTap::new(log)),
        }
    }

    pub fn query(&self) -> Arc<dyn TraceQuery> {
        Arc::clone(&self.query) as Arc<dyn TraceQuery>
    }

    pub fn tap(&self) -> Arc<dyn TraceTap> {
        Arc::clone(&self.tap) as Arc<dyn TraceTap>
    }
}

impl Module for TracingModule {
    fn name(&self) -> &'static str {
        "tracing"
    }

    fn init<'a>(
        &'a self,
        ctx: &'a ModuleCtx,
    ) -> futures_core::future::BoxFuture<'a, Result<(), LifecycleError>> {
        Box::pin(async move {
            ctx.on(TapHandler(Arc::clone(&self.tap)));
            Ok(())
        })
    }
}

struct TapHandler(Arc<TracingTap>);

impl asterpath_core::Handler<RecordAppended> for TapHandler {
    fn handle<'a>(&'a self, event: &'a RecordAppended) -> futures_core::future::BoxFuture<'a, ()> {
        let tap = Arc::clone(&self.0);
        let session = event.session;
        Box::pin(async move { tap.record_appended(session).await })
    }
}
