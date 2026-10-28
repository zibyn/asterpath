//! Approval and child-completion behaviour through the engine's durable waits.

use std::sync::Arc;
use std::time::Duration;

use crate::commit::CommitLog;
use crate::waits::Waits;
use asterpath_protocol::{
    ApprovalDecision, CancelReason, ChildOutcome, LogQuery, SessionId, SessionStore, ToolCallId,
};
use asterpath_protocol::{SessionEvent, decode_entry, dedup, encode_entry};
use asterpath_testing::InMemoryEventLog;
use futures_util::StreamExt;

const SESSION: SessionId = SessionId::from_bytes([7; 16]);

struct Fixture {
    log: Arc<InMemoryEventLog>,
    commits: Arc<CommitLog>,
    waits: Arc<Waits>,
}

fn fixture() -> Fixture {
    let log = Arc::new(InMemoryEventLog::new());
    let commits = CommitLog::for_tests(log.clone());
    let waits = Arc::new(Waits::new(Arc::clone(&commits)));
    Fixture {
        log,
        commits,
        waits,
    }
}

/// Below the cross-process poll interval: only the commit signal can wake
/// a wait this soon.
const PROMPTLY: Duration = Duration::from_millis(100);

async fn events(log: &InMemoryEventLog) -> Vec<SessionEvent> {
    let mut stream = log.scan(LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan"));
    }
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodes"))
        .collect()
}

fn count(events: &[SessionEvent], kind: &str) -> usize {
    events.iter().filter(|event| event.kind() == kind).count()
}

#[tokio::test]
async fn a_recorded_decision_wakes_the_waiter_and_is_keyed_by_the_call() {
    let fixture = fixture();
    let call = ToolCallId::from_bytes([3; 16]);

    let waiting = Arc::clone(&fixture.waits);
    let handle = tokio::spawn(async move { waiting.await_approval(SESSION, call).await });

    tokio::time::sleep(Duration::from_millis(10)).await;
    fixture
        .waits
        .record_approval(SESSION, call, ApprovalDecision::Allow)
        .await
        .expect("record");

    let decision = handle.await.expect("join").expect("decision");
    assert_eq!(decision, ApprovalDecision::Allow);
    let recorded = events(&fixture.log)
        .await
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ApprovalDecided { tool_call_id, .. } => Some(tool_call_id),
            _ => None,
        })
        .expect("recorded");
    assert_eq!(recorded, call);
}

#[tokio::test]
async fn duplicate_decisions_and_repeat_waits_collapse() {
    let fixture = fixture();
    let call = ToolCallId::from_bytes([0; 16]);
    fixture
        .waits
        .record_approval(SESSION, call, ApprovalDecision::Allow)
        .await
        .expect("record");
    // The second answer must not write a second record.
    fixture
        .waits
        .record_approval(SESSION, call, ApprovalDecision::Deny { reason: None })
        .await
        .expect("duplicate record");

    let first = fixture
        .waits
        .await_approval(SESSION, call)
        .await
        .expect("first wait");
    let second = fixture
        .waits
        .await_approval(SESSION, call)
        .await
        .expect("second wait");

    assert_eq!(first, ApprovalDecision::Allow);
    assert_eq!(second, first);
    let state = events(&fixture.log).await;
    assert_eq!(count(&state, "ApprovalDecided"), 1);
}

#[tokio::test]
async fn a_child_wait_resolves_once_the_completion_is_recorded() {
    let fixture = fixture();
    let child = SessionId::from_bytes([5; 16]);

    let waiting = Arc::clone(&fixture.waits);
    let handle = tokio::spawn(async move { waiting.await_child(SESSION, child).await });

    tokio::time::sleep(Duration::from_millis(10)).await;
    let completed = SessionEvent::ChildSessionCompleted {
        child,
        outcome: ChildOutcome::Cancelled {
            reason: CancelReason::Application,
        },
    };
    fixture
        .commits
        .append(
            encode_entry(SESSION, Some(dedup::child_session(child)), &completed).expect("encode"),
        )
        .await
        .expect("append");

    handle.await.expect("join").expect("child wait");
    // An already recorded completion satisfies a later wait immediately.
    fixture
        .waits
        .await_child(SESSION, child)
        .await
        .expect("later wait");
}

#[tokio::test(start_paused = true)]
async fn an_approval_appended_through_the_store_wakes_the_wait_before_the_poll() {
    let fixture = fixture();
    let call = ToolCallId::from_bytes([4; 16]);
    let waiting = Arc::clone(&fixture.waits);
    let handle = tokio::spawn(async move { waiting.await_approval(SESSION, call).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let started = tokio::time::Instant::now();

    let decided = SessionEvent::ApprovalDecided {
        tool_call_id: call,
        decision: ApprovalDecision::Allow,
    };
    fixture
        .commits
        .append(
            encode_entry(SESSION, Some(dedup::approval_decided(call)), &decided).expect("encode"),
        )
        .await
        .expect("append");

    let decision = handle.await.expect("join").expect("decision");
    assert_eq!(decision, ApprovalDecision::Allow);
    assert!(
        started.elapsed() < PROMPTLY,
        "woke after {:?}",
        started.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn a_child_completion_appended_through_the_store_wakes_the_wait_before_the_poll() {
    let fixture = fixture();
    let child = SessionId::from_bytes([6; 16]);
    let waiting = Arc::clone(&fixture.waits);
    let handle = tokio::spawn(async move { waiting.await_child(SESSION, child).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let started = tokio::time::Instant::now();

    let completed = SessionEvent::ChildSessionCompleted {
        child,
        outcome: ChildOutcome::Cancelled {
            reason: CancelReason::Application,
        },
    };
    fixture
        .commits
        .append(
            encode_entry(SESSION, Some(dedup::child_session(child)), &completed).expect("encode"),
        )
        .await
        .expect("append");

    handle.await.expect("join").expect("child wait");
    assert!(
        started.elapsed() < PROMPTLY,
        "woke after {:?}",
        started.elapsed()
    );
}
