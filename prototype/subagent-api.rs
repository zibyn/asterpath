// PROTOTYPE — throwaway code for issue #17. Not production.
//
// Question: what is the minimal API seam for subagent scheduling + recovery?
//
// The event log and scheduler are faked in memory so the two things that are
// hard to reason about on paper can be driven by hand:
//   1. the shape of SubagentSpawner / DurableExecutor,
//   2. which records are written by whom across the crash windows (R1-R4).
//
// Run: rustc --edition 2021 prototype/subagent-api.rs -o /tmp/subagent-api && /tmp/subagent-api

#![allow(dead_code)]

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

type LogId = u64;
// The real seam is Send + Sync; the fake World uses RefCell, so the prototype
// future is not Send. Replace the fake with Arc<dyn ...> impls and add + Send.
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

fn noop_waker() -> Waker {
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    fn wake(_: *const ()) {}
    fn wake_by_ref(_: *const ()) {}
    fn drop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

/// Prototype futures are ready on the first poll: the demo scripts the wake first.
/// A real tokio runtime replaces this.
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = Box::pin(fut);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("prototype: future would block; script the wake first"),
    }
}

// ---------------------------------------------------------------- domain types

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ParentRef {
    session: String,
    turn: u32,
    tool_call: String,
}

#[derive(Clone, Debug, PartialEq)]
enum ContextPolicy {
    Fresh,
    Fork { source_log: LogId },
}

#[derive(Clone, Debug)]
struct SpawnRequest {
    parent: ParentRef,
    caused_by: LogId, // LogId of the spawn ToolCallRequested
    task: String,
    context: ContextPolicy,
    timeout: Option<u64>, // durable timer, not an in-memory timeout
    model_alias: Option<String>,
    tool_allowlist: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
struct ChildRef {
    parent: ParentRef,
    child: String,
}

#[derive(Clone, Debug, PartialEq)]
enum ChildOutcome {
    Completed { summary: String },
    Failed { reason: ChildFailure, summary: String },
    Cancelled { reason: CancelReason },
    Lost { reason: String },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ChildFailure {
    TurnFailed,
    ContextExhausted,
    MaxStepsExceeded,
    CompactionFailed,
}

// Proposed #8 revision: "Cancelled" must say why, or recovery cannot reproduce
// Failed{Timeout} vs Interrupted vs Completed-cancelled deterministically.
#[derive(Clone, Copy, Debug, PartialEq)]
enum CancelReason {
    Parent,
    Host,
    Timeout,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum TurnEndReason {
    Completed,
    Stopped,
    Failed,
    ContextExhausted,
    MaxStepsExceeded,
    CompactionFailed,
    Cancelled,
}

#[derive(Debug, PartialEq)]
enum SpawnError {
    DepthExceeded,
    InvalidArgs(String),
    Storage(String),
}

#[derive(Debug, PartialEq)]
enum WaitError {
    Cancelled, // parent cancel -> ToolResult::Interrupted
    Timeout,   // -> ToolResult::Failed{Timeout}
    Storage(String),
    WouldBlock, // prototype-only: the durable wait would suspend here
}

// ------------------------------------------------------- DurableExecutor seam

#[derive(Clone, Debug)]
struct TimerRequest {
    session: String,
    deadline: u64,
    caused_by: LogId,
    slot: &'static str, // one intent may own several timers; id = (caused_by, slot)
}

#[derive(Clone, Debug)]
struct AwaitSignal {
    session: String,
    signal: String,
    name: String,
    caused_by: LogId,
    intent: SignalIntent,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum SignalIntent {
    Record,          // generic signal: executor writes SignalAwaited
    AlreadyRecorded, // approval: the open ToolCallRequested is the intent record
}

#[derive(Clone, Debug)]
struct SignalDelivery {
    session: String,
    signal: String,
    name: String,
    allow: bool,
}

#[derive(Clone, Debug)]
enum WakeCause {
    ChildCompleted { child: String },
    Timer { timer: String },
    Signal { signal: String },
}

/// Core seam. Wait ids are derived from the intent record, so re-registering
/// after a crash can never mint a second timer/signal/wait.
trait DurableExecutor {
    fn schedule_timer<'a>(&'a self, req: TimerRequest) -> BoxFuture<'a, Result<String, WaitError>>;
    fn cancel_timer<'a>(&'a self, session: &'a str, timer: &'a str)
        -> BoxFuture<'a, Result<(), WaitError>>;
    fn await_signal<'a>(&'a self, req: AwaitSignal) -> BoxFuture<'a, Result<bool, WaitError>>;
    fn deliver_signal<'a>(&'a self, delivery: SignalDelivery)
        -> BoxFuture<'a, Result<(), WaitError>>;
    fn await_wake<'a>(&'a self, session: &'a str, cause: WakeCause)
        -> BoxFuture<'a, Result<(), WaitError>>;
    /// Memory only. The writer appends the terminal record first, then wakes.
    fn wake(&self, session: &str, cause: WakeCause);
}

// -------------------------------------------------------- SubagentSpawner seam

/// spawn = record intent + ensure the child exists/is queued (R1/R2).
/// await_completion = ensure started (R2 again, idempotent) + durable wait + R3.
/// cancel = request cancel, wait cancel_grace, close the wait if needed.
trait SubagentSpawner {
    fn spawn<'a>(&'a self, req: SpawnRequest) -> BoxFuture<'a, Result<ChildRef, SpawnError>>;
    fn await_completion<'a>(&'a self, child: &'a ChildRef)
        -> BoxFuture<'a, Result<ChildOutcome, WaitError>>;
    fn cancel<'a>(&'a self, child: &'a ChildRef) -> BoxFuture<'a, Result<(), WaitError>>;
}

// ------------------------------------------------------------------ fake log

#[derive(Clone, Debug)]
enum Event {
    SessionCreated { parent: Option<ParentRef> },
    ContextForked { source: ParentRef, source_log: LogId },
    InputRecorded { task: String },
    TurnStarted,
    StepStarted,
    StepCommitted,
    TurnCommitted { reason: TurnEndReason },
    ToolCallRequested { turn: u32, step: u32, tool_call: String, name: String },
    ToolResult { tool_call: String, status: &'static str },
    ChildSessionSpawned {
        child: String,
        task: String,
        context: ContextPolicy,
        parent_tool_call: String,
        caused_by: LogId,
    },
    ChildSessionCompleted { child: String, outcome: ChildOutcome },
    TimerScheduled { timer: String, deadline: u64, caused_by: LogId },
    TimerFired { timer: String },
    TimerCancelled { timer: String },
    SignalAwaited { signal: String, name: String },
    SignalReceived { signal: String, name: String, allow: bool },
}

#[derive(Clone, Debug)]
struct Entry {
    seq: u64,
    log_id: LogId,
    dedup: Option<String>,
    event: Event,
}

#[derive(Default)]
struct Log {
    entries: Vec<Entry>,
}

impl Log {
    fn append(&mut self, log_id: LogId, key: Option<String>, event: Event) -> LogId {
        if let Some(key) = &key {
            if let Some(existing) = self.entries.iter().find(|e| e.dedup.as_deref() == Some(key)) {
                return existing.log_id;
            }
        }
        let seq = self.entries.len() as u64;
        self.entries.push(Entry { seq, log_id, dedup: key, event });
        log_id
    }
}

// ---------------------------------------------------------------- the world

struct World {
    logs: RefCell<HashMap<String, Log>>,
    next_log_id: Cell<LogId>,
    next_child: Cell<u32>,
    now: Cell<u64>,
    /// Volatile scheduler state. Cleared by `crash`, rebuilt from logs by `recover`.
    meta: RefCell<HashMap<String, ChildMeta>>,
    timers: RefCell<HashMap<String, TimerMeta>>,
    wakes: RefCell<Vec<(String, WakeCause)>>,
    max_depth: u8,
    limits: Limits,
}

#[derive(Clone)]
struct ChildMeta {
    parent: ParentRef,
    context: ContextPolicy,
    state: ChildState,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ChildState {
    Queued,
    Running,
    Done,
}

struct TimerMeta {
    session: String,
    child: String,
    deadline: u64,
}

#[derive(Clone, Copy)]
struct Limits {
    max_concurrent_children: usize, // 8 per parent
    max_live_children: usize,       // 32 global
}

impl World {
    fn new() -> Self {
        Self {
            logs: RefCell::new(HashMap::new()),
            next_log_id: Cell::new(1),
            next_child: Cell::new(1),
            now: Cell::new(0),
            meta: RefCell::new(HashMap::new()),
            timers: RefCell::new(HashMap::new()),
            wakes: RefCell::new(Vec::new()),
            max_depth: 3,
            // The queue itself is memory-only and deliberately not simulated here:
            // the API surface is the same whether a child runs now or sits in FIFO.
            limits: Limits { max_concurrent_children: 8, max_live_children: 32 },
        }
    }

    fn append(&self, session: &str, key: Option<String>, event: Event) -> LogId {
        let mut logs = self.logs.borrow_mut();
        let log = logs.entry(session.to_string()).or_default();
        let id = self.next_log_id.get();
        let got = log.append(id, key, event);
        if got == id {
            self.next_log_id.set(id + 1);
        }
        got
    }

    fn entries(&self, session: &str) -> Vec<Entry> {
        self.logs
            .borrow()
            .get(session)
            .map(|l| l.entries.clone())
            .unwrap_or_default()
    }

    fn find_in(&self, session: &str, pred: impl Fn(&Event) -> bool) -> Option<Entry> {
        self.entries(session).into_iter().find(|e| pred(&e.event))
    }

    fn has(&self, session: &str, pred: impl Fn(&Event) -> bool) -> bool {
        self.find_in(session, pred).is_some()
    }

    fn find_log_id(&self, session: &str, log_id: LogId) -> Option<Entry> {
        self.entries(session).into_iter().find(|e| e.log_id == log_id)
    }

    fn print_log(&self, session: &str) {
        println!("  log[{session}]:");
        for e in self.entries(session) {
            println!("    seq={:<2} id={:<2} {:?}", e.seq, e.log_id, e.event);
        }
    }

    // ---- recovery -------------------------------------------------------

    /// Kill the process: every in-memory structure disappears; logs survive.
    fn crash(&self) {
        self.meta.borrow_mut().clear();
        self.timers.borrow_mut().clear();
        self.wakes.borrow_mut().clear();
    }

    /// Rebuild volatile state from the logs, in seq order (R1/R2 + queue order).
    fn recover(&self) {
        let mut open: Vec<(u64, LogId, String, LogId, String, ContextPolicy)> = Vec::new();
        let sessions: Vec<String> = {
            let logs = self.logs.borrow();
            let mut v: Vec<_> = logs.keys().cloned().collect();
            v.sort();
            v
        };

        for session in &sessions {
            for e in self.entries(session) {
                match &e.event {
                    Event::ChildSessionSpawned { child, context, parent_tool_call, caused_by, .. } => {
                        let done = self.has(session, |ev| {
                            matches!(ev, Event::ChildSessionCompleted { child: c, .. } if c == child)
                        });
                        if !done {
                            open.push((e.seq, e.log_id, session.clone(), *caused_by, parent_tool_call.clone(), context.clone()));
                        }
                    }
                    Event::TimerScheduled { timer, deadline, caused_by } => {
                        let closed = self.has(session, |ev| {
                            matches!(ev, Event::TimerFired { timer: t } | Event::TimerCancelled { timer: t } if t == timer)
                        });
                        if !closed {
                            if let Some(Entry { event: Event::ChildSessionSpawned { child, .. }, .. }) =
                                self.find_log_id(session, *caused_by)
                            {
                                self.timers.borrow_mut().insert(
                                    timer.clone(),
                                    TimerMeta { session: session.clone(), child, deadline: *deadline },
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Rebuild the queue in the order the intents were recorded.
        open.sort_by_key(|(seq, ..)| *seq);
        for (_, _, session, caused_by, tool_call, context) in open {
            let child = self
                .find_in(&session, |ev| {
                    matches!(ev, Event::ChildSessionSpawned { parent_tool_call, .. }
                        if parent_tool_call == &tool_call)
                })
                .and_then(|e| match e.event {
                    Event::ChildSessionSpawned { child, .. } => Some(child),
                    _ => None,
                });
            let Some(child) = child else { continue };
            let started = self.has(&child, |ev| matches!(ev, Event::TurnStarted));
            let turn = self.turn_of(&session, caused_by);
            self.meta.borrow_mut().insert(
                child,
                ChildMeta {
                    parent: ParentRef { session, turn, tool_call },
                    context,
                    state: if started { ChildState::Running } else { ChildState::Queued },
                },
            );
        }

        println!("  [recover] volatile state rebuilt from logs:");
        for (child, m) in self.meta.borrow().iter() {
            println!(
                "    {child}: {:?}, parent={}, depth={}",
                m.state,
                m.parent.session,
                self.session_depth(&child)
            );
        }
        for (timer, t) in self.timers.borrow().iter() {
            println!("    {timer}: spawn timeout for {} due at {}", t.child, t.deadline);
        }
        println!(
            "    queue limits (memory-only): {} per parent / {} global",
            self.limits.max_concurrent_children, self.limits.max_live_children
        );
    }

    fn session_depth(&self, session: &str) -> u8 {
        let mut depth = 0u8;
        let mut cur = session.to_string();
        for _ in 0..16 {
            match self.find_in(&cur, |e| matches!(e, Event::SessionCreated { .. })) {
                Some(Entry { event: Event::SessionCreated { parent: Some(p), .. }, .. }) => {
                    depth += 1;
                    cur = p.session;
                }
                _ => break,
            }
        }
        depth
    }

    fn turn_of(&self, session: &str, log_id: LogId) -> u32 {
        match self.find_log_id(session, log_id).map(|e| e.event) {
            Some(Event::ToolCallRequested { turn, .. }) => turn,
            _ => 0,
        }
    }

    fn child_ref(&self, child: &str) -> Option<ChildRef> {
        if let Some(m) = self.meta.borrow().get(child) {
            return Some(ChildRef { parent: m.parent.clone(), child: child.to_string() });
        }
        let found = {
            let logs = self.logs.borrow();
            let mut hit = None;
            'outer: for (session, log) in logs.iter() {
                for e in &log.entries {
                    if let Event::ChildSessionSpawned {
                        child: c,
                        parent_tool_call,
                        caused_by,
                        ..
                    } = &e.event
                    {
                        if c == child {
                            hit = Some((session.clone(), *caused_by, parent_tool_call.clone()));
                            break 'outer;
                        }
                    }
                }
            }
            hit
        }?;
        let (session, caused_by, tool_call) = found;
        Some(ChildRef {
            parent: ParentRef { turn: self.turn_of(&session, caused_by), session, tool_call },
            child: child.to_string(),
        })
    }

    // ---- materialization (R2) ------------------------------------------

    fn materialize(&self, cref: &ChildRef) {
        let spawn = self.find_in(&cref.parent.session, |e| {
            matches!(e, Event::ChildSessionSpawned { child, .. } if child == &cref.child)
        });
        let (task, context) = match spawn.map(|e| e.event) {
            Some(Event::ChildSessionSpawned { task, context, .. }) => (task, context),
            _ => ("<missing spawn record>".to_string(), ContextPolicy::Fresh),
        };

        self.append(
            &cref.child,
            Some(format!("SessionCreated:{}", cref.child)),
            Event::SessionCreated { parent: Some(cref.parent.clone()) },
        );
        if let ContextPolicy::Fork { source_log } = &context {
            self.append(
                &cref.child,
                Some(format!("ContextForked:{}", cref.child)),
                Event::ContextForked { source: cref.parent.clone(), source_log: *source_log },
            );
        }
        self.append(
            &cref.child,
            Some(format!("InputRecorded:{}", cref.child)),
            Event::InputRecorded { task },
        );
        if !self.has(&cref.child, |e| matches!(e, Event::TurnStarted)) {
            self.append(&cref.child, None, Event::TurnStarted);
        }
    }

    // ---- child terminal / parent completion ----------------------------

    fn commit_child(&self, child: &str, reason: TurnEndReason) {
        self.append(
            child,
            Some(format!("TurnCommitted:{child}")),
            Event::TurnCommitted { reason },
        );
        println!("  [child {child}] TurnCommitted({reason:?})");
    }

    fn child_terminal(&self, child: &str) -> Option<ChildOutcome> {
        let reason = self.entries(child).into_iter().find_map(|e| match e.event {
            Event::TurnCommitted { reason } => Some(reason),
            _ => None,
        })?;
        let summary = "last committed assistant text".to_string();
        Some(match reason {
            TurnEndReason::Completed | TurnEndReason::Stopped => {
                ChildOutcome::Completed { summary }
            }
            TurnEndReason::Failed => ChildOutcome::Failed {
                reason: ChildFailure::TurnFailed,
                summary,
            },
            TurnEndReason::ContextExhausted => ChildOutcome::Failed {
                reason: ChildFailure::ContextExhausted,
                summary,
            },
            TurnEndReason::MaxStepsExceeded => ChildOutcome::Failed {
                reason: ChildFailure::MaxStepsExceeded,
                summary,
            },
            TurnEndReason::CompactionFailed => ChildOutcome::Failed {
                reason: ChildFailure::CompactionFailed,
                summary,
            },
            TurnEndReason::Cancelled => {
                ChildOutcome::Cancelled { reason: CancelReason::Host }
            }
        })
    }

    fn parent_completion(&self, cref: &ChildRef) -> Option<ChildOutcome> {
        self.entries(&cref.parent.session).into_iter().find_map(|e| match e.event {
            Event::ChildSessionCompleted { child, outcome } if child == cref.child => Some(outcome),
            _ => None,
        })
    }

    /// The scheduler is the unique writer of ChildSessionCompleted.
    fn record_completion(&self, child: &str, outcome: ChildOutcome) {
        let Some(cref) = self.child_ref(child) else { return };
        let id = self.append(
            &cref.parent.session,
            Some(format!("ChildSessionCompleted:{child}")),
            Event::ChildSessionCompleted {
                child: child.to_string(),
                outcome: outcome.clone(),
            },
        );
        if let Some(m) = self.meta.borrow_mut().get_mut(child) {
            m.state = ChildState::Done;
        }
        self.wake(
            &cref.parent.session,
            WakeCause::ChildCompleted { child: child.to_string() },
        );
        println!(
            "  [scheduler] ChildSessionCompleted({child}, {outcome:?}) at log {id}"
        );
    }

    fn finish_child(&self, child: &str, reason: TurnEndReason) {
        self.commit_child(child, reason);
        self.cancel_timers_for(child);
        if let Some(outcome) = self.child_terminal(child) {
            self.record_completion(child, outcome);
        }
    }

    fn fire_due_timers(&self) {
        let due: Vec<(String, String, String)> = self
            .timers
            .borrow()
            .iter()
            .filter(|(_, t)| t.deadline <= self.now.get())
            .map(|(id, t)| (id.clone(), t.session.clone(), t.child.clone()))
            .collect();

        for (timer, session, child) in due {
            if self.has(&session, |e| matches!(e, Event::TimerCancelled { timer: t } if t == &timer)) {
                continue;
            }
            self.append(
                &session,
                Some(format!("TimerFired:{timer}")),
                Event::TimerFired { timer: timer.clone() },
            );
            self.timers.borrow_mut().remove(&timer);
            println!("  [executor] TimerFired({timer})");

            // Timer -> scheduler reaction. Child terminal wins if it already exists.
            let outcome = self.child_terminal(&child).unwrap_or(ChildOutcome::Cancelled {
                reason: CancelReason::Timeout,
            });
            self.record_completion(&child, outcome);
        }
    }

    fn cancel_timers_for(&self, child: &str) {
        let timers: Vec<(String, String)> = self
            .timers
            .borrow()
            .iter()
            .filter(|(_, t)| t.child == child)
            .map(|(id, t)| (id.clone(), t.session.clone()))
            .collect();
        for (timer, session) in timers {
            let _ = block_on(self.cancel_timer(&session, &timer));
        }
    }

    fn received_signal(&self, session: &str, signal: &str) -> Option<bool> {
        self.entries(session).into_iter().find_map(|e| match e.event {
            Event::SignalReceived { signal: s, allow, .. } if s == signal => Some(allow),
            _ => None,
        })
    }
}

fn outcome_to_result(outcome: ChildOutcome) -> Result<ChildOutcome, WaitError> {
    match outcome {
        ChildOutcome::Cancelled { reason: CancelReason::Parent } => Err(WaitError::Cancelled),
        ChildOutcome::Cancelled { reason: CancelReason::Timeout } => Err(WaitError::Timeout),
        other => Ok(other),
    }
}

impl SubagentSpawner for World {
    fn spawn<'a>(&'a self, req: SpawnRequest) -> BoxFuture<'a, Result<ChildRef, SpawnError>> {
        Box::pin(async move {
            // R1: one child per parent tool call, ever.
            if let Some(Entry { event: Event::ChildSessionSpawned { child, context, .. }, .. }) =
                self.find_in(&req.parent.session, |e| {
                    matches!(e, Event::ChildSessionSpawned { parent_tool_call, .. }
                        if parent_tool_call == &req.parent.tool_call)
                })
            {
                self.meta.borrow_mut().entry(child.clone()).or_insert(ChildMeta {
                    parent: req.parent.clone(),
                    context,
                    state: ChildState::Running,
                });
                let cref = ChildRef { parent: req.parent, child };
                println!("  [spawn] R1 hit: tool call already owns {}; reusing", cref.child);
                self.materialize(&cref);
                return Ok(cref);
            }

            let depth = self.session_depth(&req.parent.session) + 1;
            if depth > self.max_depth {
                println!("  [spawn] depth {depth} > max {} -> Failed{{InvalidArgs}}", self.max_depth);
                return Err(SpawnError::DepthExceeded);
            }

            let n = self.next_child.get();
            self.next_child.set(n + 1);
            let child = format!("child-{n}");
            let id = self.append(
                &req.parent.session,
                Some(format!("ChildSessionSpawned:{child}")),
                Event::ChildSessionSpawned {
                    child: child.clone(),
                    task: req.task.clone(),
                    context: req.context.clone(),
                    parent_tool_call: req.parent.tool_call.clone(),
                    caused_by: req.caused_by,
                },
            );
            println!("  [spawn] ChildSessionSpawned({child}) at log {id}");

            self.meta.borrow_mut().insert(
                child.clone(),
                ChildMeta {
                    parent: req.parent.clone(),
                    context: req.context.clone(),
                    state: ChildState::Running,
                },
            );
            let cref = ChildRef { parent: req.parent.clone(), child };
            self.materialize(&cref);

            if let Some(ticks) = req.timeout {
                let timer = block_on(self.schedule_timer(TimerRequest {
                    session: req.parent.session,
                    deadline: self.now.get() + ticks,
                    caused_by: id,
                    slot: "spawn-timeout",
                }))
                .expect("schedule timer");
                println!("  [spawn] durable timeout timer {timer}");
            }
            Ok(cref)
        })
    }

    fn await_completion<'a>(&'a self, cref: &'a ChildRef)
        -> BoxFuture<'a, Result<ChildOutcome, WaitError>> {
        Box::pin(async move {
            // R3: a recorded outcome wins; never query the child again.
            if let Some(outcome) = self.parent_completion(cref) {
                println!("  [await] R3: using recorded ChildSessionCompleted");
                return outcome_to_result(outcome);
            }
            // R2: crash between ChildSessionSpawned and the child's first record.
            self.materialize(cref);
            // Normal runtime: DurableExecutor::await_wake(parent, ChildCompleted(child)).
            if let Some(outcome) = self.child_terminal(&cref.child) {
                self.record_completion(&cref.child, outcome.clone());
                return outcome_to_result(outcome);
            }
            println!("  [await] durable wait: open ChildSessionSpawned, no completion yet");
            Err(WaitError::WouldBlock)
        })
    }

    fn cancel<'a>(&'a self, cref: &'a ChildRef) -> BoxFuture<'a, Result<(), WaitError>> {
        Box::pin(async move {
            if self.parent_completion(cref).is_some() {
                return Ok(());
            }
            // Once the wait is cancelling, it closes as Cancelled{Parent} even if
            // the child happens to finish; only an already-recorded result wins.
            self.cancel_timers_for(&cref.child);
            self.record_completion(&cref.child, ChildOutcome::Cancelled { reason: CancelReason::Parent });
            Ok(())
        })
    }
}

impl DurableExecutor for World {
    fn schedule_timer<'a>(&'a self, req: TimerRequest) -> BoxFuture<'a, Result<String, WaitError>> {
        Box::pin(async move {
            let child = match self.find_log_id(&req.session, req.caused_by).map(|e| e.event) {
                Some(Event::ChildSessionSpawned { child, .. }) => child,
                _ => return Err(WaitError::Storage("caused_by is not ChildSessionSpawned".into())),
            };
            // Deterministic id: crash + re-register cannot mint a second timer.
            let timer = format!("timer:{}:{}", req.caused_by, req.slot);
            self.append(
                &req.session,
                Some(format!("TimerScheduled:{timer}")),
                Event::TimerScheduled { timer: timer.clone(), deadline: req.deadline, caused_by: req.caused_by },
            );
            self.timers.borrow_mut().insert(
                timer.clone(),
                TimerMeta { session: req.session, child, deadline: req.deadline },
            );
            Ok(timer)
        })
    }

    fn cancel_timer<'a>(&'a self, session: &'a str, timer: &'a str)
        -> BoxFuture<'a, Result<(), WaitError>> {
        Box::pin(async move {
            if self.has(session, |e| matches!(e, Event::TimerFired { timer: t } if t == timer)) {
                return Ok(()); // fired wins; never overwrite the recorded terminal
            }
            self.append(
                session,
                Some(format!("TimerCancelled:{timer}")),
                Event::TimerCancelled { timer: timer.to_string() },
            );
            self.timers.borrow_mut().remove(timer);
            Ok(())
        })
    }

    fn await_signal<'a>(&'a self, req: AwaitSignal) -> BoxFuture<'a, Result<bool, WaitError>> {
        Box::pin(async move {
            if let Some(allow) = self.received_signal(&req.session, &req.signal) {
                println!("  [executor] SignalReceived already recorded; resuming");
                return Ok(allow);
            }
            match req.intent {
                SignalIntent::Record => {
                    self.append(
                        &req.session,
                        Some(format!("SignalAwaited:{}", req.signal)),
                        Event::SignalAwaited { signal: req.signal.clone(), name: req.name.clone() },
                    );
                    println!("  [executor] SignalAwaited({}) (generic signal)", req.name);
                }
                SignalIntent::AlreadyRecorded => {
                    println!("  [executor] approval wait: open ToolCallRequested is the intent; no SignalAwaited");
                }
            }
            Err(WaitError::WouldBlock)
        })
    }

    fn deliver_signal<'a>(&'a self, delivery: SignalDelivery)
        -> BoxFuture<'a, Result<(), WaitError>> {
        Box::pin(async move {
            self.append(
                &delivery.session,
                Some(format!("SignalReceived:{}", delivery.signal)),
                Event::SignalReceived {
                    signal: delivery.signal.clone(),
                    name: delivery.name.clone(),
                    allow: delivery.allow,
                },
            );
            println!("  [executor] SignalReceived({}) delivered", delivery.name);
            self.wake(&delivery.session, WakeCause::Signal { signal: delivery.signal });
            Ok(())
        })
    }

    fn await_wake<'a>(&'a self, session: &'a str, cause: WakeCause)
        -> BoxFuture<'a, Result<(), WaitError>> {
        Box::pin(async move {
            let ready = match &cause {
                WakeCause::ChildCompleted { child } => self.has(session, |e| {
                    matches!(e, Event::ChildSessionCompleted { child: c, .. } if c == child)
                }),
                WakeCause::Timer { timer } => self
                    .has(session, |e| matches!(e, Event::TimerFired { timer: t } if t == timer)),
                WakeCause::Signal { signal } => self.has(session, |e| {
                    matches!(e, Event::SignalReceived { signal: s, .. } if s == signal)
                }),
            };
            if ready { Ok(()) } else { Err(WaitError::WouldBlock) }
        })
    }

    fn wake(&self, session: &str, cause: WakeCause) {
        self.wakes.borrow_mut().push((session.to_string(), cause));
    }
}

// -------------------------------------------------------------------- helpers

fn request_spawn_tool(w: &World, session: &str, turn: u32, tool_call: &str) -> LogId {
    w.append(
        session,
        Some(format!("ToolCallRequested:{tool_call}")),
        Event::ToolCallRequested {
            turn,
            step: 1,
            tool_call: tool_call.to_string(),
            name: "spawn_agent".to_string(),
        },
    )
}

fn spawn_request(
    w: &World,
    session: &str,
    turn: u32,
    tool_call: &str,
    task: &str,
    timeout: Option<u64>,
) -> SpawnRequest {
    let caused_by = request_spawn_tool(w, session, turn, tool_call);
    SpawnRequest {
        parent: ParentRef { session: session.to_string(), turn, tool_call: tool_call.to_string() },
        caused_by,
        task: task.to_string(),
        context: ContextPolicy::Fresh,
        timeout,
        model_alias: None,
        tool_allowlist: None,
    }
}

fn child_count(w: &World, session: &str) -> usize {
    w.entries(session)
        .iter()
        .filter(|e| matches!(e.event, Event::ChildSessionSpawned { .. }))
        .count()
}

// -------------------------------------------------------------------- demos

fn example_single() {
    println!("\n=== A. one child, happy path ===");
    let w = World::new();
    let req = spawn_request(&w, "parent", 1, "call-spawn-1", "summarize the research notes", None);
    let child = block_on(w.spawn(req)).expect("spawn");
    println!("  [await before completion] {:?}", block_on(w.await_completion(&child)));
    w.finish_child(&child.child, TurnEndReason::Completed);
    let outcome = block_on(w.await_completion(&child)).expect("completion");
    println!("  -> tool wrapper maps {outcome:?} to ToolResult{{Completed}}");
    w.print_log("parent");
    w.print_log(&child.child);
}

fn example_recovery() {
    println!("\n=== B. crash windows (R1/R2/R3) ===");
    let w = World::new();
    let req = spawn_request(&w, "parent", 1, "call-spawn-2", "write the migration", None);
    let child = block_on(w.spawn(req)).expect("spawn");

    // Window 1: ChildSessionSpawned written, child stream not yet written.
    {
        let mut logs = w.logs.borrow_mut();
        logs.remove(&child.child);
    }
    println!("  [crash] before the child's first record");
    w.crash();
    w.recover();

    let req = spawn_request(&w, "parent", 1, "call-spawn-2", "write the migration", None);
    let child2 = block_on(w.spawn(req)).expect("respawn");
    assert_eq!(child2.child, child.child, "R1 must reuse the same child");
    let _ = block_on(w.await_completion(&child2));
    println!("  [after R2] child records: {:?}", w.entries(&child2.child).len());
    w.finish_child(&child2.child, TurnEndReason::Completed);
    let outcome = block_on(w.await_completion(&child2)).expect("completion");
    println!("  -> outcome {outcome:?}; ChildSessionSpawned count = {}", child_count(&w, "parent"));

    // Window 2: child committed, parent completion missing.
    println!("\n  -- crash after child TurnCommitted, before ChildSessionCompleted --");
    let w = World::new();
    let req = spawn_request(&w, "parent", 1, "call-spawn-3", "second task", None);
    let child = block_on(w.spawn(req)).expect("spawn");
    w.commit_child(&child.child, TurnEndReason::Completed);
    w.crash();
    w.recover();
    let outcome = block_on(w.await_completion(&child)).expect("completion from child terminal");
    println!("  -> recovered outcome {outcome:?}");
    w.crash();
    w.recover();
    let outcome = block_on(w.await_completion(&child)).expect("R3");
    println!("  -> second recovery {outcome:?} (R3, child not queried)");
    w.print_log("parent");

    // Window 3: timeout is a durable timer, not an in-memory deadline.
    println!("\n  -- timeout as a durable timer --");
    let w = World::new();
    let req = spawn_request(&w, "parent", 1, "call-spawn-4", "slow task", Some(10));
    let child = block_on(w.spawn(req)).expect("spawn");
    w.now.set(10);
    w.fire_due_timers();
    let result = block_on(w.await_completion(&child));
    println!("  -> await result {result:?}; tool wrapper writes Failed{{Timeout}}");
}

fn example_fanout() {
    println!("\n=== C. fan-out: three spawns in one AssistantMessage ===");
    let w = World::new();
    let a = spawn_request(&w, "parent", 1, "call-a", "task A", None);
    let b = spawn_request(&w, "parent", 1, "call-b", "task B", None);
    let c = spawn_request(&w, "parent", 1, "call-c", "task C", Some(20));

    // No batch API: the #7 tool pipeline runs these three calls in parallel and
    // the scheduler's FIFO queue (8 per parent / 32 global) caps admission.
    let ca = block_on(w.spawn(a)).expect("spawn a");
    let cb = block_on(w.spawn(b)).expect("spawn b");
    let cc = block_on(w.spawn(c)).expect("spawn c");

    w.finish_child(&ca.child, TurnEndReason::Completed);
    w.finish_child(&cb.child, TurnEndReason::Failed);
    w.now.set(20);
    w.fire_due_timers();

    let oa = block_on(w.await_completion(&ca));
    let ob = block_on(w.await_completion(&cb));
    let oc = block_on(w.await_completion(&cc));
    println!("  -> three independent tool results: {oa:?}, {ob:?}, {oc:?}");
    println!("  -> aggregation is the parent model's next step, not a scheduler API");
    w.print_log("parent");
}

fn example_approval() {
    println!("\n=== D. approval: child's own tool wait, not a new subagent API ===");
    let w = World::new();
    let caused_by = request_spawn_tool(&w, "child-9", 1, "call-9");
    let wait = AwaitSignal {
        session: "child-9".to_string(),
        signal: "call-9".to_string(),
        name: "tool.approval.call-9".to_string(),
        caused_by,
        intent: SignalIntent::AlreadyRecorded,
    };
    println!("  [child pipeline] Ask -> await_signal");
    println!("  [await] {:?}", block_on(w.await_signal(wait.clone())));
    block_on(w.deliver_signal(SignalDelivery {
        session: "child-9".to_string(),
        signal: "call-9".to_string(),
        name: "tool.approval.call-9".to_string(),
        allow: true,
    }))
    .expect("deliver");
    println!("  [await] {:?}", block_on(w.await_signal(wait)));
    w.print_log("child-9");
    println!("  -> no SignalAwaited: the open ToolCallRequested carries the intent");
}

fn main() {
    println!("PROTOTYPE #17 — SubagentSpawner / DurableExecutor seam");
    println!("spawn = record intent + ensure child; await = R2 + durable wait + R3");
    example_single();
    example_recovery();
    example_fanout();
    example_approval();
}
