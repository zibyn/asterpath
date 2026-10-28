//! Durable waits: approval signals and child-completion wakes expressed
//! through the event log.
//!
//! An approval wait is recorded by its open `ToolCallRequested`; the answer
//! appends `ApprovalDecided` under the call's dedup key, so a re-delivery
//! after a crash never mints a second record. The waiter registry is process
//! memory only; recovery rebuilds waits by scanning the log.

use std::sync::Arc;

use asterpath_protocol::{ApprovalDecision, ExecutionError, SessionId, ToolCallId};
use asterpath_protocol::{SessionEvent, dedup};
use futures_core::future::BoxFuture;

use crate::commit::CommitLog;
use crate::watch::POLL_INTERVAL;

/// Approval and child-completion waits backed by the event log.
pub(crate) struct Waits {
    log: Arc<CommitLog>,
}

impl Waits {
    pub(crate) fn new(log: Arc<CommitLog>) -> Self {
        Self { log }
    }

    pub(crate) async fn find_approval(
        &self,
        session: SessionId,
        call: ToolCallId,
    ) -> Result<Option<ApprovalDecision>, ExecutionError> {
        Ok(self
            .log
            .read(session)
            .await?
            .iter()
            .find_map(|(_, event)| match event {
                SessionEvent::ApprovalDecided {
                    tool_call_id,
                    decision,
                } if *tool_call_id == call => Some(decision.clone()),
                _ => None,
            }))
    }

    /// `Some(())` once the parent log records the child's completion.
    async fn child_completed(
        &self,
        session: SessionId,
        child: SessionId,
    ) -> Result<Option<()>, ExecutionError> {
        let completed = self.log.read(session).await?.iter().any(|(_, event)| {
            matches!(
                event,
                SessionEvent::ChildSessionCompleted { child: done, .. } if *done == child
            )
        });
        Ok(completed.then_some(()))
    }

    /// Re-check `ready` on every commit to `session`, and at least every
    /// `POLL_INTERVAL`: another process may write the record, and its commit
    /// signal never reaches this one (ADR-0036). The signal is marked seen
    /// before each check, so a commit between check and wait is not lost.
    async fn wait_until<T, F>(
        &self,
        session: SessionId,
        mut ready: impl FnMut() -> F,
    ) -> Result<T, ExecutionError>
    where
        F: Future<Output = Result<Option<T>, ExecutionError>>,
    {
        let mut commits = self.log.commits(session);
        loop {
            commits.borrow_and_update();
            if let Some(value) = ready().await? {
                return Ok(value);
            }
            let _ = tokio::time::timeout(POLL_INTERVAL, commits.changed()).await;
        }
    }
}

impl Waits {
    pub(crate) fn await_approval(
        &self,
        session: SessionId,
        call: ToolCallId,
    ) -> BoxFuture<'_, Result<ApprovalDecision, ExecutionError>> {
        Box::pin(self.wait_until(session, move || self.find_approval(session, call)))
    }

    pub(crate) fn record_approval(
        &self,
        session: SessionId,
        call: ToolCallId,
        decision: ApprovalDecision,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async move {
            self.log
                .append_event(
                    session,
                    SessionEvent::ApprovalDecided {
                        tool_call_id: call,
                        decision,
                    },
                    Some(dedup::approval_decided(call)),
                )
                .await?;
            Ok(())
        })
    }

    /// Resolves once `session`'s log records `child`'s completion.
    pub(crate) fn await_child(
        &self,
        session: SessionId,
        child: SessionId,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(self.wait_until(session, move || self.child_completed(session, child)))
    }
}

#[cfg(test)]
mod tests;
