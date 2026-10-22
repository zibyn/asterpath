//! Scenario (a): a real `SIGKILL` mid-turn, then recovery from the same SQLite.
#![cfg(unix)]

mod common;

use std::path::Path;

use asterpath_protocol::{SessionEvent, ToolResultStatus, TurnEndReason};
use common::{Scenario, Testdrive, markers, scan, session_id};

async fn session_entries(
    db: &Path,
    session: asterpath_core::SessionId,
) -> Vec<asterpath_core::LogEntry> {
    scan(db, session).await
}

fn assistant_per_step(events: &[SessionEvent]) -> Vec<(asterpath_core::StepId, usize)> {
    let mut counts = std::collections::BTreeMap::new();
    for event in events {
        if let SessionEvent::AssistantMessage { step_id, .. } = event {
            *counts.entry(*step_id).or_insert(0) += 1;
        }
    }
    counts.into_iter().collect()
}

fn commits_per_step(events: &[SessionEvent]) -> Vec<(asterpath_core::StepId, usize)> {
    let mut counts = std::collections::BTreeMap::new();
    for event in events {
        if let SessionEvent::StepCommitted { step_id, .. } = event {
            *counts.entry(*step_id).or_insert(0) += 1;
        }
    }
    counts.into_iter().collect()
}

fn started_steps(events: &[SessionEvent]) -> Vec<asterpath_core::StepId> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::StepStarted { step_id, .. } => Some(*step_id),
            _ => None,
        })
        .collect()
}

fn assert_one_commit_per_step(events: &[SessionEvent]) {
    let started = started_steps(events);
    let commits = commits_per_step(events);
    assert_eq!(
        commits.len(),
        started.len(),
        "every step has a terminal record: {commits:?} vs {started:?}"
    );
    for (_, count) in commits {
        assert_eq!(count, 1, "a step is committed exactly once");
    }
}

fn assert_one_assistant_per_step(events: &[SessionEvent]) {
    for (step, count) in assistant_per_step(events) {
        assert_eq!(count, 1, "step {step:?} has exactly one AssistantMessage");
    }
}

fn turn_reasons(events: &[SessionEvent]) -> Vec<TurnEndReason> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::TurnCommitted { reason, .. } => Some(reason.clone()),
            _ => None,
        })
        .collect()
}

fn tool_results(events: &[SessionEvent]) -> Vec<(ToolResultStatus, bool, bool)> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                status,
                output,
                error,
                ..
            } => Some((*status, output.is_some(), error.is_some())),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a1_safe_tool_crash_reruns_once_and_completes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("a1.sqlite");
    let effect = dir.path().join("effect.json");

    let mut run = Testdrive::start(Scenario {
        run: true,
        db: &db,
        fixture: "a1_safe.json",
        effect: &effect,
        hold_ms: 30_000,
        failpoints: Vec::new(),
    });
    run.wait_marker("\"event\":\"effect_applied\"");
    let run_lines = run.kill();
    let session = session_id(&run_lines);

    // The side effect is already on disk when the process dies.
    assert_eq!(
        std::fs::read_to_string(&effect).expect("effect file"),
        "{\"k\":\"v\"}",
        "the idempotent marker is written exactly once"
    );

    let resume_lines = Testdrive::start(Scenario {
        run: false,
        db: &db,
        fixture: "a1_safe.json",
        effect: &effect,
        hold_ms: 0,
        failpoints: Vec::new(),
    })
    .wait_success();
    assert_eq!(session_id(&resume_lines), session);

    assert_eq!(
        std::fs::read_to_string(&effect).expect("effect file"),
        "{\"k\":\"v\"}",
        "the safe rerun overwrites, it does not duplicate"
    );

    let entries = session_entries(&db, session).await;
    let events: Vec<SessionEvent> = entries
        .iter()
        .map(|entry| asterpath_protocol::decode_entry(entry).expect("decode"))
        .collect();
    assert_eq!(turn_reasons(&events), vec![TurnEndReason::Completed]);
    assert_eq!(
        tool_results(&events),
        vec![(ToolResultStatus::Completed, true, false)],
        "exactly one completed result"
    );
    assert_eq!(
        common::kind_count(&entries, "StreamResumed"),
        1,
        "recovery is recorded"
    );
    assert_one_assistant_per_step(&events);
    assert_one_commit_per_step(&events);
    // The script's second step is a legitimate second model call; the crashed
    // step never gets a second AssistantMessage (asserted per step above).
    assert_eq!(started_steps(&events).len(), 2);

    // S10 / L11: the committed model call keeps its usage across the crash.
    let usage = events
        .iter()
        .find_map(|event| match event {
            SessionEvent::AssistantMessage { usage, .. } => Some(usage),
            _ => None,
        })
        .expect("committed assistant message");
    assert_eq!(usage.input.uncached, 10);
    assert_eq!(usage.output.total, 5);

    let requests = markers(&resume_lines, "model_request");
    assert_eq!(requests.len(), 1, "the resumed process makes one request");
    assert_eq!(
        requests[0]["tool_results"], 1,
        "the second model request projects exactly one ToolResult"
    );
}

#[tokio::test]
async fn a2_unsafe_tool_crash_records_interrupted_and_never_reruns() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("a2.sqlite");
    let effect = dir.path().join("effect.log");

    let mut run = Testdrive::start(Scenario {
        run: true,
        db: &db,
        fixture: "a2_unsafe.json",
        effect: &effect,
        hold_ms: 30_000,
        failpoints: Vec::new(),
    });
    run.wait_marker("\"event\":\"effect_applied\"");
    let run_lines = run.kill();
    let session = session_id(&run_lines);

    let resume_lines = Testdrive::start(Scenario {
        run: false,
        db: &db,
        fixture: "a2_unsafe.json",
        effect: &effect,
        hold_ms: 0,
        failpoints: Vec::new(),
    })
    .wait_success();
    assert_eq!(session_id(&resume_lines), session);

    assert_eq!(
        common::effect_lines(&effect),
        vec!["line".to_owned()],
        "the unsafe side effect happens exactly once"
    );

    let entries = session_entries(&db, session).await;
    let events: Vec<SessionEvent> = entries
        .iter()
        .map(|entry| asterpath_protocol::decode_entry(entry).expect("decode"))
        .collect();
    assert_eq!(
        tool_results(&events),
        vec![(ToolResultStatus::Interrupted, false, false)],
        "an unsafe in-flight tool is interrupted, never rerun"
    );
    assert_eq!(turn_reasons(&events), vec![TurnEndReason::Completed]);
    assert_eq!(common::kind_count(&entries, "StreamResumed"), 1);
    assert_one_assistant_per_step(&events);
    assert_one_commit_per_step(&events);

    let requests = markers(&resume_lines, "model_request");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]["tool_results"], 1,
        "the interrupted result is model-visible"
    );
}

#[tokio::test]
async fn a3_model_stream_crash_records_crash_abort_and_retries_the_step() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("a3.sqlite");
    let effect = dir.path().join("unused.json");

    let mut run = Testdrive::start(Scenario {
        run: true,
        db: &db,
        fixture: "a3_stream_crash.json",
        effect: &effect,
        hold_ms: 30_000,
        failpoints: Vec::new(),
    });
    run.wait_marker("\"event\":\"stream_open\"");
    let run_lines = run.kill();
    let session = session_id(&run_lines);

    let resume_lines = Testdrive::start(Scenario {
        run: false,
        db: &db,
        fixture: "a3_stream_crash.json",
        effect: &effect,
        hold_ms: 0,
        failpoints: Vec::new(),
    })
    .wait_success();
    assert_eq!(session_id(&resume_lines), session);

    let entries = session_entries(&db, session).await;
    let events: Vec<SessionEvent> = entries
        .iter()
        .map(|entry| asterpath_protocol::decode_entry(entry).expect("decode"))
        .collect();
    let aborts: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ModelCallAborted { kind, .. } => Some(*kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        aborts,
        vec![asterpath_protocol::AbortKind::Crash],
        "the interrupted attempt is recorded as a crash"
    );
    assert_eq!(common::kind_count(&entries, "AssistantMessage"), 1);
    assert_eq!(common::kind_count(&entries, "StepCommitted"), 1);
    assert_eq!(common::kind_count(&entries, "StreamResumed"), 1);
    assert_eq!(turn_reasons(&events), vec![TurnEndReason::Completed]);
    assert_eq!(started_steps(&events).len(), 1, "the same step is retried");
    assert_one_assistant_per_step(&events);
    assert_one_commit_per_step(&events);
}
