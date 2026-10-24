//! v1 acceptance scenarios: a real `SIGKILL`, then recovery from the same SQLite.
#![cfg(unix)]

mod common;
mod m5;
mod m6;

use std::collections::HashSet;
use std::path::Path;

use asterpath::AsterpathBuilder;
use asterpath_protocol::{
    ChildOutcome, InputTokens, Item, OutputTokens, Part, Role, SessionEvent, Span, SpanAttributes,
    SpanKind, ToolResultStatus, TraceId, TurnEndReason, Usage, UsageScope,
};
use asterpath_protocol::{SessionId, ToolCallId};
use common::{Scenario, Testdrive, markers, scan, session_id};
use futures_util::StreamExt;
use serde_json::{Value, json};

async fn session_entries(
    db: &Path,
    session: asterpath_protocol::SessionId,
) -> Vec<asterpath_protocol::LogEntry> {
    scan(db, session).await
}

fn assistant_per_step(events: &[SessionEvent]) -> Vec<(asterpath_protocol::StepId, usize)> {
    let mut counts = std::collections::BTreeMap::new();
    for event in events {
        if let SessionEvent::AssistantMessage { step_id, .. } = event {
            *counts.entry(*step_id).or_insert(0) += 1;
        }
    }
    counts.into_iter().collect()
}

fn commits_per_step(events: &[SessionEvent]) -> Vec<(asterpath_protocol::StepId, usize)> {
    let mut counts = std::collections::BTreeMap::new();
    for event in events {
        if let SessionEvent::StepCommitted { step_id, .. } = event {
            *counts.entry(*step_id).or_insert(0) += 1;
        }
    }
    counts.into_iter().collect()
}

fn started_steps(events: &[SessionEvent]) -> Vec<asterpath_protocol::StepId> {
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

/// Assistant model output across the committed steps, in log order.
fn assistant_items(events: &[SessionEvent]) -> Vec<Item> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::AssistantMessage { items, .. } => Some(items.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn visible_part(part: &Part) -> Option<Value> {
    match part {
        Part::Text { text } => Some(json!({"kind": "text", "text": text})),
        Part::Refusal { text } => Some(json!({"kind": "refusal", "text": text})),
        Part::Image { media_type, .. } => Some(json!({"kind": "image", "media_type": media_type})),
        Part::File { media_type, .. } => Some(json!({"kind": "file", "media_type": media_type})),
        // Provider-local content is not translatable across providers.
        Part::ProviderOpaque { .. } => None,
        _ => unreachable!("fixtures build only known parts"),
    }
}

/// The provider-independent projection: raw payloads (and reasoning replay
/// bindings, which name the producing provider) are provider-local metadata.
fn visible_projection(items: &[Item]) -> Vec<Value> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::Message { role, parts, .. } => Some(json!({
                "kind": "message",
                "role": role,
                "parts": parts.iter().filter_map(visible_part).collect::<Vec<_>>(),
            })),
            Item::Reasoning { summary, .. } => Some(json!({
                "kind": "reasoning",
                "summary": summary.iter().filter_map(visible_part).collect::<Vec<_>>(),
            })),
            Item::ToolCall {
                call_id,
                name,
                raw_args,
                ..
            } => Some(json!({
                "kind": "tool_call",
                "call_id": call_id,
                "name": name.as_str(),
                "args": serde_json::from_str::<Value>(raw_args.as_str()).unwrap_or(Value::Null),
            })),
            Item::ToolResult {
                call_id,
                parts,
                is_error,
                ..
            } => Some(json!({
                "kind": "tool_result",
                "call_id": call_id,
                "parts": parts.iter().filter_map(visible_part).collect::<Vec<_>>(),
                "is_error": is_error,
            })),
            Item::Compaction { parts } => Some(json!({
                "kind": "compaction",
                "parts": parts.iter().filter_map(visible_part).collect::<Vec<_>>(),
            })),
            // Server-tool and opaque items belong to one provider's protocol.
            _ => None,
        })
        .collect()
}

fn effective_tool_calls(items: &[Item]) -> Vec<(String, Value)> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::ToolCall { name, raw_args, .. } => Some((
                name.as_str().to_owned(),
                serde_json::from_str::<Value>(raw_args.as_str()).unwrap_or(Value::Null),
            )),
            _ => None,
        })
        .collect()
}

fn final_assistant_text(items: &[Item]) -> Option<String> {
    items.iter().rev().find_map(|item| match item {
        Item::Message {
            role: Role::Assistant,
            parts,
            ..
        } => Some(
            parts
                .iter()
                .filter_map(|part| match part {
                    Part::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>(),
        ),
        _ => None,
    })
}

fn reasoning_raw(items: &[Item]) -> Vec<(String, String)> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::Reasoning { raw, .. } => Some((raw.kind.clone(), raw.json.as_str().to_owned())),
            _ => None,
        })
        .collect()
}

fn committed_count(lines: &[String], reason: &str) -> usize {
    markers(lines, "turn_committed")
        .iter()
        .filter(|marker| marker["reason"] == reason)
        .count()
}

#[tokio::test]
async fn b1_both_providers_produce_the_same_normalized_history() {
    let fixture_path = common::fixture("b1_equivalent.json");
    let fixture: asterpath_testing::Fixture = serde_json::from_str(
        &std::fs::read_to_string(&fixture_path).expect("read scenario (b) fixture"),
    )
    .expect("parse scenario (b) fixture");

    let anthropic =
        asterpath_testing::http_mock::MockServer::start(common::mock::anthropic_replies(&fixture));
    let openai =
        asterpath_testing::http_mock::MockServer::start(common::mock::openai_replies(&fixture));

    let dir = tempfile::tempdir().expect("temp dir");
    let anthropic_db = dir.path().join("anthropic.sqlite");
    let openai_db = dir.path().join("openai.sqlite");
    let anthropic_effect = dir.path().join("anthropic-effect.json");
    let openai_effect = dir.path().join("openai-effect.json");

    let anthropic_lines = Testdrive::start_provider(
        Scenario {
            run: true,
            db: &anthropic_db,
            fixture: "b1_equivalent.json",
            effect: &anthropic_effect,
            hold_ms: 0,
            failpoints: Vec::new(),
        },
        "anthropic",
        &anthropic.base_url(),
    )
    .wait_success();
    let openai_lines = Testdrive::start_provider(
        Scenario {
            run: true,
            db: &openai_db,
            fixture: "b1_equivalent.json",
            effect: &openai_effect,
            hold_ms: 0,
            failpoints: Vec::new(),
        },
        "openai",
        &format!("{}/v1", openai.base_url()),
    )
    .wait_success();

    // Each side spoke its own wire protocol, one request per scripted step.
    assert_eq!(
        anthropic
            .requests()
            .iter()
            .map(|r| r.path().to_owned())
            .collect::<Vec<_>>(),
        vec!["/v1/messages".to_owned(); 2],
        "Anthropic Messages endpoint"
    );
    assert_eq!(
        openai
            .requests()
            .iter()
            .map(|r| r.path().to_owned())
            .collect::<Vec<_>>(),
        vec!["/v1/responses".to_owned(); 2],
        "OpenAI Responses endpoint"
    );
    assert_eq!(
        std::fs::read_to_string(&anthropic_effect).expect("effect"),
        "{\"k\":\"v\"}"
    );
    assert_eq!(
        std::fs::read_to_string(&openai_effect).expect("effect"),
        "{\"k\":\"v\"}"
    );

    let anthropic_events = common::events(&anthropic_db, session_id(&anthropic_lines)).await;
    let openai_events = common::events(&openai_db, session_id(&openai_lines)).await;

    assert_eq!(committed_count(&anthropic_lines, "Completed"), 1);
    assert_eq!(committed_count(&openai_lines, "Completed"), 1);
    assert_eq!(
        turn_reasons(&anthropic_events),
        vec![TurnEndReason::Completed]
    );
    assert_eq!(turn_reasons(&openai_events), vec![TurnEndReason::Completed]);

    let anthropic_items = assistant_items(&anthropic_events);
    let openai_items = assistant_items(&openai_events);

    assert_eq!(
        visible_projection(&anthropic_items),
        visible_projection(&openai_items),
        "the normalized item/part projection is provider-independent"
    );
    assert_eq!(
        effective_tool_calls(&anthropic_items),
        effective_tool_calls(&openai_items)
    );
    assert_eq!(
        effective_tool_calls(&anthropic_items),
        vec![("set_marker".to_owned(), json!({"key": "k", "value": "v"}))],
        "one tool call with the same effective args on both sides"
    );
    assert_eq!(
        final_assistant_text(&anthropic_items),
        final_assistant_text(&openai_items)
    );
    assert_eq!(
        final_assistant_text(&anthropic_items).as_deref(),
        Some("done")
    );

    // Raw provider data is present on each side, differs, and is never compared.
    let anthropic_raw = reasoning_raw(&anthropic_items);
    let openai_raw = reasoning_raw(&openai_items);
    assert_eq!(anthropic_raw.len(), 1);
    assert_eq!(openai_raw.len(), 1);
    assert_eq!(anthropic_raw[0].0, "anthropic.thinking");
    assert_eq!(openai_raw[0].0, "openai.reasoning");
    assert_ne!(
        anthropic_raw, openai_raw,
        "provider raw payloads are intentionally different"
    );
}

// --- Scenario (c): fan-out, crash while the parent waits, recovery -----------

const TASK_A: &str = "task A: report alpha";
const TASK_B: &str = "task B: report beta";
const SUMMARY_A: &str = "summary A: alpha done";
const SUMMARY_B: &str = "summary B: beta done";
const SECRET_A: &str = "transcript-A-secret";
const SECRET_B: &str = "transcript-B-secret";

/// The loop's lifecycle records for one child turn. Crash bookkeeping
/// (`StreamResumed`, `ModelCallAborted`) is not part of the sequence.
const CHILD_LIFECYCLE: &[&str] = &[
    "SessionCreated",
    "InputRecorded",
    "TurnStarted",
    "StepStarted",
    "AssistantMessage",
    "StepCommitted",
    "TurnCommitted",
];

fn usage(uncached: u64, output: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached,
            cache_read: 0,
            cache_write: 0,
        },
        output: OutputTokens {
            total: output,
            reasoning: 0,
        },
    }
}

/// The child log minus the crash bookkeeping the loop itself writes on
/// recovery; anything else unexpected remains visible to the assertion.
fn lifecycle(events: &[SessionEvent]) -> Vec<&'static str> {
    events
        .iter()
        .filter(|event| {
            !matches!(
                event,
                SessionEvent::StreamResumed { .. }
                    | SessionEvent::ModelCallAborted {
                        kind: asterpath_protocol::AbortKind::Crash,
                        ..
                    }
            )
        })
        .map(|event| event.kind())
        .collect()
}

fn task_text(parts: &[Part]) -> String {
    parts
        .iter()
        .filter_map(|part| match part {
            Part::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// The facade holds the writer lock, so scenario (c) reads through its log
/// instead of opening a second connection.
async fn committed(
    log: &dyn asterpath_protocol::SessionStore,
    session: SessionId,
) -> Vec<asterpath_protocol::LogEntry> {
    let mut stream = log.scan(asterpath_protocol::LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("log entry"));
    }
    entries
}

#[tokio::test]
async fn c1_parallel_children_survive_a_waiting_crash() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("c1.sqlite");
    let effect = dir.path().join("unused");

    let mut run = Testdrive::start(Scenario {
        run: true,
        db: &db,
        fixture: "c1_fanout.json",
        effect: &effect,
        hold_ms: 0,
        failpoints: Vec::new(),
    });
    let waiting = run.wait_marker("\"event\":\"parent_waiting\"");
    assert_eq!(
        serde_json::from_str::<Value>(&waiting).expect("marker json")["children"],
        2,
        "both children have started before the parent crash"
    );
    let run_lines = run.kill();
    let parent = session_id(&run_lines);
    assert!(
        markers(&run_lines, "model_request")
            .iter()
            .all(|request| request["route"] != "parent_final"),
        "the parent is still waiting when it is killed"
    );

    let resume_lines = Testdrive::resume_session(
        Scenario {
            run: false,
            db: &db,
            fixture: "c1_fanout.json",
            effect: &effect,
            hold_ms: 0,
            failpoints: Vec::new(),
        },
        parent,
    )
    .wait_success();
    assert_eq!(session_id(&resume_lines), parent);

    // Everything below reads the same SQLite file; no process memory is reused.
    let asterpath = AsterpathBuilder::new()
        .database(&db)
        .build()
        .await
        .expect("reopen the acceptance database");

    let log = asterpath.store();
    let entries = committed(log.as_ref(), parent).await;
    let events: Vec<SessionEvent> = entries
        .iter()
        .map(|entry| asterpath_protocol::decode_entry(entry).expect("decode"))
        .collect();

    // --- child identity: one spawn and one completion per parent tool call ---

    let spawns: Vec<(ToolCallId, SessionId, String)> = events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ChildSessionSpawned {
                child,
                task,
                parent_tool_call_id,
                ..
            } => Some((*parent_tool_call_id, *child, task_text(task))),
            _ => None,
        })
        .collect();
    assert_eq!(
        spawns.len(),
        2,
        "parent {parent:?} spawned exactly two children: {spawns:?}"
    );
    assert_eq!(
        spawns
            .iter()
            .map(|(call, _, _)| *call)
            .collect::<HashSet<_>>()
            .len(),
        2,
        "the two spawns are distinct tool calls"
    );
    let spawned_children: HashSet<SessionId> = spawns.iter().map(|(_, child, _)| *child).collect();
    assert_eq!(spawned_children.len(), 2, "no duplicate child session");
    let child_for = |needle: &str| {
        spawns
            .iter()
            .find(|(_, _, task)| task.contains(needle))
            .map(|(_, child, _)| *child)
            .expect("routed spawn task")
    };
    let child_a = child_for(TASK_A);
    let child_b = child_for(TASK_B);

    for (call, child, _) in &spawns {
        assert_eq!(
            spawns.iter().filter(|(other, _, _)| other == call).count(),
            1,
            "one ChildSessionSpawned per parent_tool_call_id"
        );
        let completions: Vec<&ChildOutcome> = events
            .iter()
            .filter_map(|event| match event {
                SessionEvent::ChildSessionCompleted {
                    child: done,
                    outcome,
                } if done == child => Some(outcome),
                _ => None,
            })
            .collect();
        assert_eq!(
            completions.len(),
            1,
            "one ChildSessionCompleted for child {child:?}"
        );
        assert!(matches!(completions[0], ChildOutcome::Completed { .. }));
    }
    let completed_children: HashSet<SessionId> = events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ChildSessionCompleted { child, .. } => Some(*child),
            _ => None,
        })
        .collect();
    assert_eq!(
        completed_children, spawned_children,
        "resume completed the same two children; it created no third"
    );
    assert_eq!(common::kind_count(&entries, "ChildSessionSpawned"), 2);
    assert_eq!(common::kind_count(&entries, "ChildSessionCompleted"), 2);

    // --- parent terminal shape ---------------------------------------------

    let spawn_calls: HashSet<ToolCallId> = events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolCallRequested {
                tool_call_id, name, ..
            } if name.as_str() == "spawn_agent" => Some(*tool_call_id),
            _ => None,
        })
        .collect();
    assert_eq!(spawn_calls.len(), 2);
    let results: Vec<(ToolCallId, ToolResultStatus)> = events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                tool_call_id,
                status,
                ..
            } => Some((*tool_call_id, *status)),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 2, "exactly two spawn tool results");
    assert!(
        results
            .iter()
            .all(|(_, status)| *status == ToolResultStatus::Completed)
    );
    assert_eq!(
        results
            .iter()
            .map(|(call, _)| *call)
            .collect::<HashSet<_>>(),
        spawn_calls,
        "both spawn results are Completed"
    );
    assert_eq!(turn_reasons(&events), vec![TurnEndReason::Completed]);
    assert_eq!(
        common::kind_count(&entries, "AssistantMessage"),
        2,
        "one model call per step; the waiting step is not re-requested"
    );
    assert_eq!(common::kind_count(&entries, "StepStarted"), 2);
    assert_eq!(common::kind_count(&entries, "StepCommitted"), 2);
    assert_eq!(common::kind_count(&entries, "StreamResumed"), 1);

    // --- child logs: one clean turn each ----------------------------------

    let catalog = asterpath.store();
    let catalog_children: Vec<SessionId> = catalog
        .sessions(asterpath_protocol::SessionQuery {
            parent: asterpath_protocol::ParentFilter::ChildrenOf(parent),
            ..Default::default()
        })
        .await
        .expect("child sessions")
        .into_iter()
        .map(|entry| entry.session)
        .collect();
    assert_eq!(
        catalog_children.len(),
        2,
        "the session tree has exactly two children"
    );
    assert_eq!(
        catalog_children.into_iter().collect::<HashSet<_>>(),
        spawned_children
    );
    for child in &spawned_children {
        let child_entries = committed(log.as_ref(), *child).await;
        let child_events: Vec<SessionEvent> = child_entries
            .iter()
            .map(|entry| asterpath_protocol::decode_entry(entry).expect("decode"))
            .collect();
        assert_eq!(
            lifecycle(&child_events),
            CHILD_LIFECYCLE.to_vec(),
            "child {child:?} log: {child_events:?}"
        );
        assert_eq!(
            turn_reasons(&child_events),
            vec![TurnEndReason::Completed],
            "child {child:?} completed exactly once"
        );
    }

    // --- the parent's second request sees summaries, not transcripts ------

    let requests = markers(&resume_lines, "model_request");
    let final_request = requests
        .iter()
        .find(|request| request["route"] == "parent_final")
        .expect("the parent's second request happens after recovery");
    assert_eq!(final_request["tool_results"], 2);
    let texts: Vec<&str> = final_request["texts"]
        .as_array()
        .expect("request texts")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let joined = texts.join("\n");
    for summary in [SUMMARY_A, SUMMARY_B] {
        assert!(
            joined.contains(summary),
            "the second request carries {summary:?}: {joined}"
        );
    }
    for secret in [SECRET_A, SECRET_B] {
        assert!(
            !joined.contains(secret),
            "a child transcript leaked into the parent request: {joined}"
        );
    }

    // --- usage attribution -------------------------------------------------

    let own = asterpath
        .usage(UsageScope::Session(parent))
        .await
        .expect("parent usage");
    assert_eq!(
        own.own.model_calls,
        usage(40, 12),
        "parent Session own is only the parent's own calls"
    );
    assert_eq!(own.descendants.model_calls, Usage::default());
    let child_a_usage = asterpath
        .usage(UsageScope::Session(child_a))
        .await
        .expect("child A usage")
        .own;
    let child_b_usage = asterpath
        .usage(UsageScope::Session(child_b))
        .await
        .expect("child B usage")
        .own;
    assert_eq!(child_a_usage.model_calls, usage(11, 3));
    assert_eq!(child_b_usage.model_calls, usage(12, 4));
    let subtree = asterpath
        .usage(UsageScope::SessionSubtree(parent))
        .await
        .expect("subtree usage");
    assert_eq!(subtree.own.model_calls, usage(40, 12));
    assert_eq!(
        subtree.descendants.model_calls,
        usage(23, 7),
        "descendants are exactly the two children's sum"
    );
    assert_eq!(
        subtree.own.total() + subtree.descendants.total(),
        usage(40, 12).total() + usage(23, 7).total(),
        "own + descendants does not double count"
    );

    // --- trace attribution -------------------------------------------------

    let parent_spans = asterpath.trace(parent).await.expect("parent trace");
    let subagents: Vec<&Span> = parent_spans
        .iter()
        .filter(|span| span.kind == SpanKind::Subagent)
        .collect();
    assert_eq!(subagents.len(), 2, "two subagent spans on the parent trace");
    for subagent in &subagents {
        let SpanAttributes::Subagent(attrs) = &subagent.attributes else {
            panic!("subagent span without subagent attributes");
        };
        let call = subagent.tool_call.expect("subagent anchor");
        let tool = parent_spans
            .iter()
            .find(|span| span.kind == SpanKind::ToolCall && span.tool_call == Some(call))
            .expect("the spawn tool span");
        assert_eq!(
            subagent.parent_span,
            Some(tool.span),
            "subagent span hangs off its spawn tool span"
        );
        assert_eq!(
            subagent.child_trace,
            Some(TraceId::from_session(attrs.child))
        );
        assert!(matches!(
            attrs.outcome,
            Some(ChildOutcome::Completed { .. })
        ));
    }
    assert_eq!(
        subagents
            .iter()
            .filter_map(|span| match &span.attributes {
                SpanAttributes::Subagent(attrs) => Some(attrs.child),
                _ => None,
            })
            .collect::<HashSet<_>>(),
        spawned_children
    );
    let spawn_tool_spans = parent_spans
        .iter()
        .filter(|span| {
            matches!(
                (&span.attributes, span.kind),
                (SpanAttributes::ToolCall(attrs), SpanKind::ToolCall)
                    if attrs.name.as_str() == "spawn_agent"
            )
        })
        .count();
    assert_eq!(spawn_tool_spans, 2);
    let unique: HashSet<_> = parent_spans.iter().map(|span| span.span).collect();
    assert_eq!(
        unique.len(),
        parent_spans.len(),
        "recovery produced no duplicate parent spans"
    );
    assert_eq!(
        parent_spans
            .iter()
            .filter(|span| span.kind == SpanKind::Session)
            .count(),
        1
    );
    assert_eq!(
        parent_spans
            .iter()
            .filter(|span| span.kind == SpanKind::Step)
            .count(),
        2
    );
    assert!(
        parent_spans
            .iter()
            .any(|span| span.kind == SpanKind::Step && span.replay),
        "the waiting step closes after resume and carries the replay marker"
    );
    assert!(
        subagents.iter().any(|span| span.replay),
        "the resumed child wait carries a replay marker"
    );

    for child in &spawned_children {
        let spans = asterpath.trace(*child).await.expect("child trace");
        let roots: Vec<&Span> = spans
            .iter()
            .filter(|span| span.kind == SpanKind::Session)
            .collect();
        assert_eq!(roots.len(), 1, "child {child:?} has one session root span");
        let subagent = subagents
            .iter()
            .find(|span| {
                matches!(&span.attributes, SpanAttributes::Subagent(attrs) if attrs.child == *child)
            })
            .expect("the child's subagent span");
        assert_eq!(
            roots[0].parent_trace,
            Some(TraceId::from_session(parent)),
            "child root points back at the parent trace"
        );
        assert_eq!(
            roots[0].parent_span,
            Some(subagent.span),
            "child root points at the spawn subagent span"
        );
        let unique: HashSet<_> = spans.iter().map(|span| span.span).collect();
        assert_eq!(
            unique.len(),
            spans.len(),
            "child {child:?} has no duplicate spans after recovery"
        );
    }
}

/// The loop is one implementation for every provider (ADR 0010): it branches on
/// capabilities, never on a provider identity.
#[test]
fn agent_loop_contains_no_provider_branch() {
    let source = include_str!("../../asterpath/src/agent/mod.rs");
    assert!(!source.contains("anthropic"));
    assert!(!source.contains("openai"));
}
