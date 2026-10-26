//! Golden log fixture at the current schema version: it must keep decoding and
//! projecting to its canonical expected result. Historical-version fixtures
//! start with the first release (ADR-0004).

use asterpath_protocol::{CancelReason, DedupKey, LogEntry, LogId, SessionId};
use asterpath_protocol::{LOG_SCHEMA_VERSION, decode_entry, history};
use serde_json::Value;

fn parse_entry(line: &str) -> LogEntry {
    let value: Value = serde_json::from_str(line).expect("fixture line is JSON");
    LogEntry {
        log_id: serde_json::from_value(value["log_id"].clone()).expect("log_id"),
        session: serde_json::from_value::<SessionId>(value["session"].clone()).expect("session"),
        seq: value["seq"].as_u64().expect("seq"),
        recorded_at: value["recorded_at"].as_i64().expect("recorded_at"),
        schema_version: value["schema_version"].as_u64().expect("schema_version") as u16,
        dedup_key: serde_json::from_value::<Option<DedupKey>>(value["dedup_key"].clone())
            .expect("dedup_key"),
        payload: value["payload"].clone(),
    }
}

#[test]
fn fixture_decodes_and_projects_to_its_canonical_expectation() {
    let fixture = include_str!("fixtures/log/v1/basic_turn.jsonl");
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/log/v1/basic_turn.expected.json"))
            .expect("expected projection is JSON");

    let entries: Vec<LogEntry> = fixture
        .lines()
        .filter(|line| !line.is_empty())
        .map(parse_entry)
        .collect();
    assert_eq!(entries.len(), 5);

    for entry in &entries {
        let event = decode_entry(entry).expect("every fixture entry decodes");
        assert_eq!(event.kind(), expected_event_kind(entry));
        assert_eq!(entry.schema_version, LOG_SCHEMA_VERSION);
    }

    let history = history(&entries).expect("projection");
    assert_eq!(serde_json::to_value(&history).unwrap(), expected["items"]);

    let raw_frontier = entries.iter().map(|entry| entry.log_id).max();
    assert_eq!(
        serde_json::to_value(raw_frontier).unwrap(),
        expected["frontier"]
    );
}

fn expected_event_kind(entry: &LogEntry) -> &'static str {
    match entry.seq {
        1 => "InputRecorded",
        2 | 5 => "AssistantMessage",
        3 => "CompactionApplied",
        4 => "StepStarted",
        _ => unreachable!(),
    }
}

#[test]
fn current_version_round_trips_written_entries() {
    // Write with the current version, read back, and keep the same projection.
    let fixture = include_str!("fixtures/log/v1/basic_turn.jsonl");
    let entries: Vec<LogEntry> = fixture
        .lines()
        .filter(|line| !line.is_empty())
        .map(parse_entry)
        .collect();

    let written: Vec<LogEntry> = entries
        .iter()
        .map(|entry| LogEntry {
            schema_version: LOG_SCHEMA_VERSION,
            ..entry.clone()
        })
        .collect();

    let before = history(&entries).expect("before");
    let after = history(&written).expect("after");
    assert_eq!(before, after);
    assert_eq!(
        written.iter().map(|entry| entry.log_id).max(),
        Some(LogId::from_i64(5))
    );
}

#[test]
fn messages_without_raw_decode_with_raw_none() {
    // `Item::Message.raw` is omitted when absent.
    let fixture = include_str!("fixtures/log/v1/basic_turn.jsonl");
    let entries: Vec<LogEntry> = fixture
        .lines()
        .filter(|line| !line.is_empty())
        .map(parse_entry)
        .collect();
    let raws: Vec<_> = history(&entries)
        .expect("projection")
        .into_iter()
        .filter_map(|item| match item {
            asterpath_protocol::Item::Message { raw, .. } => Some(raw),
            _ => None,
        })
        .collect();
    assert!(!raws.is_empty());
    assert!(raws.iter().all(Option::is_none));
}

#[test]
fn application_cancel_reason_keeps_its_logged_name() {
    let logged = serde_json::to_value(CancelReason::Application).unwrap();
    assert_eq!(logged, "Host");
    assert_eq!(
        serde_json::from_value::<CancelReason>(logged).unwrap(),
        CancelReason::Application
    );
}
