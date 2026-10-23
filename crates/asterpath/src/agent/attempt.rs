//! Attempt policy (GLOSSARY 尝试策略): from a step's recorded attempt history
//! and its latest outcome, decide whether to retry, force a compaction or end
//! the turn. The live loop and crash recovery both ask only this module.
//!
//! - A retryable failure (or a crash) retries while the budget lasts. The
//!   budget is `max_attempts`; the one C5 retry after a forced compaction is
//!   not charged to it.
//! - Context exhaustion forces one compaction per step (C5); a second one ends
//!   the turn `ContextExhausted`.
//! - D1: a recorded `Rejected` abort cannot say the context was exceeded, so
//!   recovery treats it as a plain rejection unless a forced compaction
//!   follows it in the log.

use asterpath_protocol::{AbortKind, CompactionAbortReason, TurnEndReason};

/// How the latest model attempt of a step ended, short of a response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    Retryable,
    Rejected,
    ContextExceeded,
    Cancelled,
    /// The attempt never reached a terminal record (process crash).
    Crash,
}

impl Failure {
    /// What a recorded abort says, as far as the log can tell (D1).
    pub(crate) fn recorded(kind: AbortKind) -> Self {
        match kind {
            AbortKind::Transport => Self::Retryable,
            AbortKind::Cancelled => Self::Cancelled,
            AbortKind::Crash => Self::Crash,
            // Rejected, and any kind a newer writer adds, never retries.
            _ => Self::Rejected,
        }
    }
}

/// How a forced compaction ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Forced {
    Applied,
    /// Nothing usable came out: insufficient, not needed, not configured.
    Exhausted,
    Failed,
}

/// The step's attempt history the decision depends on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Attempts {
    /// The attempt the latest outcome belongs to (one-based).
    pub(crate) attempt: u32,
    /// The step already ran its one forced compaction.
    pub(crate) compacted: bool,
}

impl Attempts {
    pub(crate) const FIRST: Self = Self {
        attempt: 1,
        compacted: false,
    };
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Next {
    /// Call the model again as `attempt`. `backoff` is false for the C5 retry.
    Retry { attempt: u32, backoff: bool },
    /// Run the step's one forced compaction at its watermark.
    ForceCompact,
    /// Commit the step and end the turn.
    End(TurnEndReason),
}

pub(crate) fn after_failure(max_attempts: u32, history: Attempts, failure: Failure) -> Next {
    match failure {
        Failure::Cancelled => Next::End(TurnEndReason::Cancelled),
        Failure::Rejected => Next::End(TurnEndReason::Failed),
        Failure::ContextExceeded if history.compacted => Next::End(TurnEndReason::ContextExhausted),
        Failure::ContextExceeded => Next::ForceCompact,
        Failure::Retryable | Failure::Crash => {
            // The C5 retry is free: it widens the budget by one.
            let budget = max_attempts.saturating_add(u32::from(history.compacted));
            if history.attempt < budget {
                Next::Retry {
                    attempt: history.attempt + 1,
                    backoff: true,
                }
            } else {
                Next::End(TurnEndReason::Failed)
            }
        }
    }
}

pub(crate) fn after_compaction(history: Attempts, forced: Forced) -> Next {
    match forced {
        Forced::Applied => Next::Retry {
            attempt: history.attempt + 1,
            backoff: false,
        },
        Forced::Exhausted => Next::End(TurnEndReason::ContextExhausted),
        Forced::Failed => Next::End(TurnEndReason::CompactionFailed),
    }
}

/// One record of a step's attempt history, in log order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Record {
    Aborted {
        attempt: u32,
        kind: AbortKind,
    },
    /// A `ProviderError` compaction started: the abort before it was C5.
    ForcedStarted,
    ForcedApplied,
    ForcedAborted(CompactionAbortReason),
}

/// Replay a step's recorded history through the same rules. `None` means no
/// attempt reached a terminal record. Returns the history after the last
/// record and the decision the live loop made (or was about to make) there.
pub(crate) fn replay(
    max_attempts: u32,
    records: impl IntoIterator<Item = Record>,
) -> Option<(Attempts, Next)> {
    let mut state: Option<(Attempts, Next)> = None;
    for record in records {
        state = match (record, state) {
            (Record::Aborted { attempt, kind }, previous) => {
                let history = Attempts {
                    attempt,
                    compacted: previous.is_some_and(|(history, _)| history.compacted),
                };
                Some((
                    history,
                    after_failure(max_attempts, history, Failure::recorded(kind)),
                ))
            }
            (Record::ForcedStarted, Some((history, _))) => {
                let history = Attempts {
                    compacted: true,
                    ..history
                };
                Some((history, Next::ForceCompact))
            }
            (Record::ForcedApplied, Some((history, _))) => {
                Some((history, after_compaction(history, Forced::Applied)))
            }
            (Record::ForcedAborted(reason), Some((history, _))) => {
                let forced = match reason {
                    CompactionAbortReason::SummaryFailed => Forced::Failed,
                    _ => Forced::Exhausted,
                };
                Some((history, after_compaction(history, forced)))
            }
            // A compaction record with no abort before it is not this step's.
            (_, None) => None,
        };
    }
    state
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: u32 = 3;

    fn at(attempt: u32, compacted: bool) -> Attempts {
        Attempts { attempt, compacted }
    }

    fn retry(attempt: u32) -> Next {
        Next::Retry {
            attempt,
            backoff: true,
        }
    }

    fn end(reason: TurnEndReason) -> Next {
        Next::End(reason)
    }

    #[test]
    fn after_failure_table() {
        use Failure::*;
        use TurnEndReason::{Cancelled as EndCancelled, ContextExhausted, Failed};
        let table = [
            (at(1, false), Retryable, retry(2)),
            (at(2, false), Retryable, retry(3)),
            (at(3, false), Retryable, end(Failed)),
            (at(1, false), Crash, retry(2)),
            (at(3, false), Crash, end(Failed)),
            // The C5 retry is not charged to the budget.
            (at(2, true), Retryable, retry(3)),
            (at(3, true), Retryable, retry(4)),
            (at(4, true), Retryable, end(Failed)),
            (at(4, true), Crash, end(Failed)),
            (at(1, false), Rejected, end(Failed)),
            (at(2, true), Rejected, end(Failed)),
            (at(1, false), Cancelled, end(EndCancelled)),
            (at(3, true), Cancelled, end(EndCancelled)),
            (at(1, false), ContextExceeded, Next::ForceCompact),
            // D2: the last budgeted attempt still gets its forced compaction.
            (at(3, false), ContextExceeded, Next::ForceCompact),
            (at(2, true), ContextExceeded, end(ContextExhausted)),
            (at(4, true), ContextExceeded, end(ContextExhausted)),
        ];
        for (history, failure, expected) in table {
            assert_eq!(
                after_failure(MAX, history, failure),
                expected,
                "{history:?} {failure:?}"
            );
        }
    }

    #[test]
    fn single_attempt_budget_never_retries() {
        assert_eq!(
            after_failure(1, Attempts::FIRST, Failure::Crash),
            end(TurnEndReason::Failed)
        );
    }

    #[test]
    fn after_compaction_table() {
        let table = [
            (
                at(1, true),
                Forced::Applied,
                Next::Retry {
                    attempt: 2,
                    backoff: false,
                },
            ),
            (
                at(3, true),
                Forced::Applied,
                Next::Retry {
                    attempt: 4,
                    backoff: false,
                },
            ),
            (
                at(1, true),
                Forced::Exhausted,
                end(TurnEndReason::ContextExhausted),
            ),
            (
                at(1, true),
                Forced::Failed,
                end(TurnEndReason::CompactionFailed),
            ),
        ];
        for (history, forced, expected) in table {
            assert_eq!(after_compaction(history, forced), expected, "{forced:?}");
        }
    }

    fn aborted(attempt: u32, kind: AbortKind) -> Record {
        Record::Aborted { attempt, kind }
    }

    #[test]
    fn replay_table() {
        use AbortKind::*;
        type Case = (Vec<Record>, Option<(Attempts, Next)>);
        let cases: Vec<Case> = vec![
            (vec![], None),
            (vec![aborted(1, Crash)], Some((at(1, false), retry(2)))),
            (
                vec![aborted(1, Transport), aborted(2, Transport)],
                Some((at(2, false), retry(3))),
            ),
            (
                vec![aborted(3, Transport)],
                Some((at(3, false), end(TurnEndReason::Failed))),
            ),
            (
                vec![aborted(1, Cancelled)],
                Some((at(1, false), end(TurnEndReason::Cancelled))),
            ),
            // D1: a lone Rejected is a plain rejection.
            (
                vec![aborted(1, Rejected)],
                Some((at(1, false), end(TurnEndReason::Failed))),
            ),
            (
                vec![aborted(1, Rejected), Record::ForcedStarted],
                Some((at(1, true), Next::ForceCompact)),
            ),
            (
                vec![
                    aborted(1, Rejected),
                    Record::ForcedStarted,
                    Record::ForcedApplied,
                ],
                Some((
                    at(1, true),
                    Next::Retry {
                        attempt: 2,
                        backoff: false,
                    },
                )),
            ),
            // D2: C5 on the last budgeted attempt still retries.
            (
                vec![
                    aborted(1, Transport),
                    aborted(2, Transport),
                    aborted(3, Rejected),
                    Record::ForcedStarted,
                    Record::ForcedApplied,
                ],
                Some((
                    at(3, true),
                    Next::Retry {
                        attempt: 4,
                        backoff: false,
                    },
                )),
            ),
            (
                vec![
                    aborted(1, Rejected),
                    Record::ForcedStarted,
                    Record::ForcedApplied,
                    aborted(2, Transport),
                ],
                Some((at(2, true), retry(3))),
            ),
            (
                vec![
                    aborted(1, Rejected),
                    Record::ForcedStarted,
                    Record::ForcedAborted(CompactionAbortReason::Insufficient),
                ],
                Some((at(1, true), end(TurnEndReason::ContextExhausted))),
            ),
            (
                vec![
                    aborted(1, Rejected),
                    Record::ForcedStarted,
                    Record::ForcedAborted(CompactionAbortReason::SummaryFailed),
                ],
                Some((at(1, true), end(TurnEndReason::CompactionFailed))),
            ),
            // D1 again: the second context overflow after C5 reads as Failed.
            (
                vec![
                    aborted(1, Rejected),
                    Record::ForcedStarted,
                    Record::ForcedApplied,
                    aborted(2, Rejected),
                ],
                Some((at(2, true), end(TurnEndReason::Failed))),
            ),
        ];
        for (records, expected) in cases {
            assert_eq!(replay(MAX, records.clone()), expected, "{records:?}");
        }
    }
}
