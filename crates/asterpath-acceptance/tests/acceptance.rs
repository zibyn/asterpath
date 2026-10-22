//! Scenario (a): a real `SIGKILL` mid-turn, then recovery from the same SQLite.
#![cfg(unix)]

mod common;

use std::path::Path;

use asterpath_protocol::{Item, Part, Role, SessionEvent, ToolResultStatus, TurnEndReason};
use common::{Scenario, Testdrive, markers, scan, session_id};
use serde_json::{Value, json};

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
        // Provider-local content is not translatable across providers.
        Part::ProviderOpaque { .. } => None,
    }
}

/// The provider-independent projection: raw payloads (and reasoning replay
/// bindings, which name the producing provider) are provider-local metadata.
fn visible_projection(items: &[Item]) -> Vec<Value> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::Message { role, parts } => Some(json!({
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

    let anthropic = common::mock::MockServer::start(common::mock::anthropic_replies(&fixture));
    let openai = common::mock::MockServer::start(common::mock::openai_replies(&fixture));

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
        anthropic.paths(),
        vec!["/v1/messages".to_owned(); 2],
        "Anthropic Messages endpoint"
    );
    assert_eq!(
        openai.paths(),
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

/// The loop is one implementation for every provider (ADR 0010): it branches on
/// capabilities, never on a provider identity.
#[test]
fn agent_loop_contains_no_provider_branch() {
    let source = include_str!("../../asterpath-agent/src/lib.rs");
    assert!(!source.contains("anthropic"));
    assert!(!source.contains("openai"));
}
