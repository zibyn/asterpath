//! Crash-point equivalence for one model step (ADR-0017, GLOSSARY 尝试策略).
//!
//! Each scenario runs once live. Then, for every append of that run, a fresh
//! log crashes just before and just after the append (in-process `Panic`
//! failpoint), and a fresh agent recovers it. The recovered end reason,
//! attempt numbers and compaction count must match the live run, except for
//! the accepted differences D1 / D4, which have their own expected outcome,
//! and D3, which has its own test.
//!
//! The provider is deterministic per call: recovery resumes the script at the
//! first call whose outcome is not in the log, so a call in flight at the
//! crash is answered the same way again.

use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::support::{self, SESSION, append, decoded, drive, model, text, tool_table};
use crate::agent::Agent;
use asterpath_protocol::{
    AbortKind, CompactPlan, CompactSummary, CompactionError, CompactionInput, CompactionJob,
    CompactionOutcome, CompactionTrigger, Compactor, ConfigSnapshot, Item, ProjectedOrigin,
    ProviderError, ProviderErrorKind, Role, SessionEvent, StopReason, ToolConfig, TurnConfig,
    TurnEndReason, Usage, dedup,
};
use asterpath_protocol::{InputId, LogId, LogIdRange, SessionStore, StepId, TurnId};
use asterpath_testing::{Failpoint, FailpointEventLog, InMemoryEventLog, Point, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

const MAX_ATTEMPTS: u32 = 3;

fn config(backoff: Duration) -> ConfigSnapshot {
    ConfigSnapshot {
        turn: {
            let mut config = TurnConfig::default();
            config.max_attempts = MAX_ATTEMPTS.try_into().expect("non-zero");
            config.initial_backoff = backoff;
            config.max_backoff = backoff;
            config
        },
        ..support::config(Vec::new())
    }
}

// --- scenario -------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum Reply {
    Done,
    Transport,
    Rejected,
    ContextExceeded,
}

#[derive(Clone, Copy, Debug)]
enum Forced {
    Applied,
    Insufficient,
    Failed,
}

#[derive(Clone, Debug)]
struct Scenario {
    name: &'static str,
    replies: Vec<Reply>,
    forced: Forced,
}

fn scenario(name: &'static str, replies: &[Reply], forced: Forced) -> Scenario {
    Scenario {
        name,
        replies: replies.to_vec(),
        forced,
    }
}

impl Scenario {
    fn provider(&self) -> Arc<ScriptedProvider> {
        let mut provider = ScriptedProvider::new("p");
        for reply in &self.replies {
            provider = match reply {
                Reply::Done => provider.respond(
                    vec![Item::Message {
                        role: Role::Assistant,
                        parts: vec![text("done")],
                        raw: None,
                    }],
                    StopReason::EndTurn,
                    Usage::default(),
                ),
                Reply::Transport => {
                    provider.fail(ProviderError::new(ProviderErrorKind::Transport, "reset"))
                }
                Reply::Rejected => {
                    provider.fail(ProviderError::new(ProviderErrorKind::InvalidRequest, "bad"))
                }
                Reply::ContextExceeded => provider.fail(ProviderError::new(
                    ProviderErrorKind::ContextExceeded,
                    "too long",
                )),
            };
        }
        Arc::new(provider)
    }

    /// The same world with one more failed call in front.
    fn with_failed_first_call(&self) -> Self {
        let mut replies = vec![Reply::Transport];
        replies.extend(&self.replies);
        Self {
            replies,
            ..self.clone()
        }
    }
}

/// Plans only forced (`ProviderError`) compactions; records what it was given.
struct ForcedCompactor {
    result: Forced,
    inputs: Mutex<Vec<CompactionInput>>,
}

impl ForcedCompactor {
    fn new(result: Forced) -> Arc<Self> {
        Arc::new(Self {
            result,
            inputs: Mutex::new(Vec::new()),
        })
    }
}

impl Compactor for ForcedCompactor {
    fn plan(&self, input: &CompactionInput) -> CompactPlan {
        if input.trigger != CompactionTrigger::ProviderError {
            return CompactPlan::NotNeeded;
        }
        self.inputs.lock().unwrap().push(input.clone());
        CompactPlan::Summarize(CompactionJob {
            first_kept: LogId::from_i64(0),
            replaces: LogIdRange {
                first: LogId::from_i64(0),
                last: LogId::from_i64(0),
            },
            model: input.model.clone(),
            summary_input: Vec::new(),
            max_summary_tokens: 128,
            usable_window: 1000,
            retained_tokens: 0,
            turn: input.turn.clone(),
        })
    }

    fn compact<'a>(
        &'a self,
        job: CompactionJob,
    ) -> BoxFuture<'a, Result<CompactSummary, CompactionError>> {
        Box::pin(async move {
            match self.result {
                Forced::Applied => Ok(CompactSummary::Applied(CompactionOutcome {
                    first_kept: job.first_kept,
                    summary: vec![text("summary")],
                    replaces: job.replaces,
                    model: job.model,
                    usage: Usage::default(),
                    raw: None,
                })),
                Forced::Insufficient => Ok(CompactSummary::Insufficient),
                Forced::Failed => Err(CompactionError::Provider(ProviderError::new(
                    ProviderErrorKind::Server,
                    "summary failed",
                ))),
            }
        })
    }
}

// --- harness --------------------------------------------------------------

fn agent(
    log: Arc<dyn SessionStore>,
    provider: Arc<ScriptedProvider>,
    compactor: Arc<ForcedCompactor>,
) -> Agent {
    support::agent(
        log,
        provider,
        tool_table(Vec::new()),
        ToolConfig::default(),
        Vec::new(),
    )
    .with_compactor(Some(compactor))
}

async fn input(log: &InMemoryEventLog, id: u8) -> LogId {
    append(
        log,
        SessionEvent::InputRecorded {
            input: vec![text("go")],
        },
        Some(dedup::input_recorded(InputId::from_bytes([id; 16]))),
    )
    .await
}

/// A session with one pending input: the next drive opens a turn.
async fn seeded(backoff: Duration) -> Arc<InMemoryEventLog> {
    let log = Arc::new(InMemoryEventLog::new());
    append(
        &log,
        SessionEvent::SessionCreated {
            config: config(backoff),
            parent: None,
            metadata: Default::default(),
        },
        Some(dedup::session_created(SESSION)),
    )
    .await;
    input(&log, 4).await;
    log
}

#[derive(Clone, Debug, PartialEq)]
struct Outcome {
    reasons: Vec<TurnEndReason>,
    /// Attempt numbers of every `ModelCallAborted`, in log order.
    attempts: Vec<u32>,
    crash_aborts: usize,
    compactions: usize,
    steps: usize,
}

fn outcome(events: &[SessionEvent]) -> Outcome {
    let mut outcome = Outcome {
        reasons: Vec::new(),
        attempts: Vec::new(),
        crash_aborts: 0,
        compactions: 0,
        steps: 0,
    };
    for event in events {
        match event {
            SessionEvent::TurnCommitted { reason, .. } => outcome.reasons.push(reason.clone()),
            SessionEvent::ModelCallAborted { attempt, kind, .. } => {
                outcome.attempts.push(*attempt);
                if *kind == AbortKind::Crash {
                    outcome.crash_aborts += 1;
                }
            }
            SessionEvent::CompactionApplied { .. } => outcome.compactions += 1,
            SessionEvent::StepStarted { .. } => outcome.steps += 1,
            _ => {}
        }
    }
    outcome
}

/// Model calls whose outcome the log holds, in call order.
fn answered_calls(events: &[SessionEvent]) -> usize {
    events
        .iter()
        .filter(|event| match event {
            SessionEvent::ModelCallAborted { kind, .. } => *kind != AbortKind::Crash,
            SessionEvent::AssistantMessage { .. } => true,
            _ => false,
        })
        .count()
}

async fn live(scenario: &Scenario) -> (Outcome, usize) {
    let log = seeded(Duration::ZERO).await;
    let before = decoded(&log).await.len();
    let agent = agent(
        log.clone(),
        scenario.provider(),
        ForcedCompactor::new(scenario.forced),
    );
    drive(&agent).await;
    let events = decoded(&log).await;
    (outcome(&events), events.len() - before)
}

/// What recovery must produce for a log that crashed with `crashed` on disk.
async fn expected(scenario: &Scenario, live: &Outcome, crashed: &[SessionEvent]) -> Outcome {
    let step_open = crashed
        .iter()
        .any(|event| matches!(event, SessionEvent::StepStarted { .. }))
        && !crashed
            .iter()
            .any(|event| matches!(event, SessionEvent::StepCommitted { .. }));
    let answered = answered_calls(crashed);
    if step_open && answered == 0 {
        // D4: the first attempt was in flight; recovery records Crash{1} and
        // the retry is attempt 2, as if the first call had failed.
        let (mut shifted, _) = Box::pin(live_of(scenario.with_failed_first_call())).await;
        shifted.crash_aborts = 1;
        return shifted;
    }
    let turn_open = !crashed
        .iter()
        .any(|event| matches!(event, SessionEvent::TurnCommitted { .. }));
    let last = crashed.iter().rev().find_map(|event| match event {
        SessionEvent::ModelCallAborted { kind, .. } => Some(Some(*kind)),
        SessionEvent::CompactionStarted { .. } => Some(None),
        _ => None,
    });
    if turn_open
        && last == Some(Some(AbortKind::Rejected))
        && scenario.replies[answered - 1] == Reply::ContextExceeded
    {
        // D1: a recorded `Rejected` cannot say the context was exceeded.
        let mut failed = outcome(crashed);
        failed.reasons = vec![TurnEndReason::Failed];
        return failed;
    }
    live.clone()
}

async fn live_of(scenario: Scenario) -> (Outcome, usize) {
    live(&scenario).await
}

/// Crash at the `occurrence`-th append of the run, then recover on a fresh
/// agent. Returns the log at the crash and the recovered outcome.
async fn crash_then_recover(
    scenario: &Scenario,
    point: Point,
    occurrence: usize,
) -> (Vec<SessionEvent>, Outcome) {
    let log = seeded(Duration::ZERO).await;
    let failing = Arc::new(FailpointEventLog::new(
        log.clone(),
        Failpoint::panic(point).with_occurrence(NonZeroUsize::new(occurrence).expect("one-based")),
    ));
    let first = agent(
        failing,
        scenario.provider(),
        ForcedCompactor::new(scenario.forced),
    );
    let crashed = AssertUnwindSafe(drive(&first)).catch_unwind().await;
    assert!(
        crashed.is_err(),
        "{}: append {occurrence} crashes",
        scenario.name
    );
    drop(first);
    let crashed = decoded(&log).await;

    let recovering = scenario.provider();
    recovering.skip(answered_calls(&crashed));
    let second = agent(
        log.clone(),
        recovering,
        ForcedCompactor::new(scenario.forced),
    );
    drive(&second).await;
    (crashed, outcome(&decoded(&log).await))
}

async fn assert_crash_points_match_live(scenario: Scenario) {
    let (live, appends) = live(&scenario).await;
    for occurrence in 1..=appends {
        for point in [Point::AppendBeforeCommit, Point::AppendAfterCommit] {
            let (crashed, recovered) = crash_then_recover(&scenario, point, occurrence).await;
            let expected = expected(&scenario, &live, &crashed).await;
            assert_eq!(
                recovered,
                expected,
                "{}: crash at {} of append {occurrence}",
                scenario.name,
                point.as_str()
            );
        }
    }
}

// --- equivalence ----------------------------------------------------------

#[tokio::test]
async fn first_attempt_success() {
    assert_crash_points_match_live(scenario("first", &[Reply::Done], Forced::Applied)).await;
}

#[tokio::test]
async fn retryable_failures_then_success() {
    assert_crash_points_match_live(scenario(
        "retry",
        &[Reply::Transport, Reply::Transport, Reply::Done],
        Forced::Applied,
    ))
    .await;
}

#[tokio::test]
async fn retry_budget_exhausted() {
    assert_crash_points_match_live(scenario(
        "exhaust",
        &[
            Reply::Transport,
            Reply::Transport,
            Reply::Transport,
            Reply::Done,
        ],
        Forced::Applied,
    ))
    .await;
}

#[tokio::test]
async fn rejected_call() {
    assert_crash_points_match_live(scenario(
        "reject",
        &[Reply::Rejected, Reply::Done],
        Forced::Applied,
    ))
    .await;
}

#[tokio::test]
async fn forced_compaction_then_success() {
    assert_crash_points_match_live(scenario(
        "c5",
        &[Reply::ContextExceeded, Reply::Done],
        Forced::Applied,
    ))
    .await;
}

/// D2: the C5 retry does not spend the attempt budget.
#[tokio::test]
async fn forced_compaction_on_the_last_budgeted_attempt() {
    assert_crash_points_match_live(scenario(
        "c5-last",
        &[
            Reply::Transport,
            Reply::Transport,
            Reply::ContextExceeded,
            Reply::Done,
        ],
        Forced::Applied,
    ))
    .await;
}

#[tokio::test]
async fn retryable_failures_after_forced_compaction() {
    assert_crash_points_match_live(scenario(
        "c5-then-retry",
        &[
            Reply::ContextExceeded,
            Reply::Transport,
            Reply::Transport,
            Reply::Done,
        ],
        Forced::Applied,
    ))
    .await;
}

#[tokio::test]
async fn context_exceeded_twice() {
    assert_crash_points_match_live(scenario(
        "c5-twice",
        &[Reply::ContextExceeded, Reply::ContextExceeded, Reply::Done],
        Forced::Applied,
    ))
    .await;
}

#[tokio::test]
async fn forced_compaction_insufficient() {
    assert_crash_points_match_live(scenario(
        "c5-insufficient",
        &[Reply::ContextExceeded, Reply::Done],
        Forced::Insufficient,
    ))
    .await;
}

#[tokio::test]
async fn forced_compaction_fails() {
    assert_crash_points_match_live(scenario(
        "c5-failed",
        &[Reply::ContextExceeded, Reply::Done],
        Forced::Failed,
    ))
    .await;
}

// --- accepted differences and D5 ------------------------------------------

const TURN: TurnId = TurnId::from_bytes([1; 16]);
const STEP: StepId = StepId::from_bytes([2; 16]);

async fn open_step(log: &InMemoryEventLog) {
    append(log, SessionEvent::TurnStarted { turn_id: TURN }, None).await;
    append(
        log,
        SessionEvent::StepStarted {
            turn_id: TURN,
            step_id: STEP,
        },
        None,
    )
    .await;
}

async fn aborted(log: &InMemoryEventLog, attempt: u32, kind: AbortKind, backoff_ms: u64) {
    append(
        log,
        SessionEvent::ModelCallAborted {
            turn_id: TURN,
            step_id: STEP,
            attempt,
            kind,
            partial: None,
            model: model(),
            backoff_ms,
            usage: None,
            error: None,
        },
        None,
    )
    .await;
}

/// D3: recovery retries at once; it never reads `recorded_at` to finish a
/// recorded backoff.
#[tokio::test]
async fn recovery_does_not_wait_out_a_recorded_backoff() {
    let hour = Duration::from_secs(3600);
    let log = seeded(hour).await;
    open_step(&log).await;
    aborted(&log, 1, AbortKind::Transport, hour.as_millis() as u64).await;

    let provider = scenario("d3", &[Reply::Done], Forced::Applied).provider();
    let agent = agent(log.clone(), provider, ForcedCompactor::new(Forced::Applied));
    tokio::time::timeout(Duration::from_secs(10), drive(&agent))
        .await
        .expect("no backoff sleep on recovery");

    let outcome = outcome(&decoded(&log).await);
    assert_eq!(outcome.reasons, vec![TurnEndReason::Completed]);
    assert_eq!(outcome.attempts, vec![1]);
}

/// D5: a redone `ProviderError` compaction sees the step's projection, not
/// records appended after `StepStarted`.
#[tokio::test]
async fn redone_forced_compaction_uses_the_step_watermark() {
    let log = seeded(Duration::ZERO).await;
    open_step(&log).await;
    let steering = input(&log, 5).await;
    aborted(&log, 1, AbortKind::Rejected, 0).await;
    append(
        &log,
        SessionEvent::CompactionStarted {
            trigger: CompactionTrigger::ProviderError,
            replaces_from: LogId::from_i64(0),
            caused_by: steering,
            turn: Some(TURN),
        },
        Some(dedup::compaction(steering)),
    )
    .await;

    let compactor = ForcedCompactor::new(Forced::Applied);
    let provider = scenario("d5", &[Reply::Done, Reply::Done], Forced::Applied).provider();
    let agent = agent(log.clone(), provider, Arc::clone(&compactor));
    drive(&agent).await;

    let outcome = outcome(&decoded(&log).await);
    assert_eq!(outcome.compactions, 1);
    assert_eq!(outcome.attempts, vec![1]);
    let inputs = compactor.inputs.lock().unwrap();
    assert_eq!(inputs.len(), 1, "the open compaction is redone once");
    assert!(
        inputs[0]
            .history
            .iter()
            .all(|entry| entry.origin != ProjectedOrigin::Log(steering)),
        "steering input after StepStarted stays out of the summary"
    );
}
