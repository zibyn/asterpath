//! Behaviour of the canonical model, log decoding and folds.

use asterpath_protocol::*;
use asterpath_protocol::{
    LogEntry, LogId, LogIdRange, SessionId, SignalId, StepId, ToolCallId, TurnId,
};
use serde_json::{Value, json};

fn entry(seq: u64, schema_version: u16, payload: Value) -> LogEntry {
    LogEntry {
        log_id: LogId::from_i64(seq as i64),
        session: session(1),
        seq,
        recorded_at: 0,
        schema_version,
        dedup_key: None,
        payload,
    }
}

fn session(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn turn(byte: u8) -> TurnId {
    TurnId::from_bytes([byte; 16])
}

fn step(byte: u8) -> StepId {
    StepId::from_bytes([byte; 16])
}

fn call(byte: u8) -> ToolCallId {
    ToolCallId::from_bytes([byte; 16])
}

fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("test"),
        model: ModelId::new("model"),
        context_window: Some(1000),
    }
}

fn config() -> ConfigSnapshot {
    ConfigSnapshot {
        model: model(),
        system: vec![],
        tools: vec![],
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn assistant(text_value: &str) -> SessionEvent {
    SessionEvent::AssistantMessage {
        turn_id: turn(1),
        step_id: step(1),
        items: vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text(text_value)],
        }],
        raw: None,
        model: model(),
        usage: Usage::default(),
        stop_reason: StopReason::EndTurn,
    }
}

#[test]
fn unknown_fields_are_ignored() {
    let payload = json!({"InputRecorded": {"input": [], "future_field": true}});
    let event = decode_entry(&entry(1, LOG_SCHEMA_VERSION, payload)).expect("decode");
    assert!(matches!(event, SessionEvent::InputRecorded { input } if input.is_empty()));
}

#[test]
fn unknown_variant_is_a_hard_error() {
    let payload = json!({"NoSuchEvent": {}});
    let error = decode_entry(&entry(1, LOG_SCHEMA_VERSION, payload)).unwrap_err();
    assert!(matches!(error, LogDecodeError::Decode { .. }), "{error:?}");
}

#[test]
fn missing_required_field_is_a_hard_error() {
    let payload = json!({"ToolResult": {}});
    let error = decode_entry(&entry(1, LOG_SCHEMA_VERSION, payload)).unwrap_err();
    assert!(matches!(error, LogDecodeError::Decode { .. }), "{error:?}");
}

#[test]
fn bad_payload_is_a_hard_error() {
    let error = decode_entry(&entry(1, LOG_SCHEMA_VERSION, json!("not an event"))).unwrap_err();
    assert!(matches!(error, LogDecodeError::Decode { .. }), "{error:?}");
}

#[test]
fn newer_schema_version_is_rejected() {
    let payload = json!({"InputRecorded": {"input": []}});
    let error = decode_entry(&entry(1, LOG_SCHEMA_VERSION + 1, payload)).unwrap_err();
    assert!(
        matches!(
            error,
            LogDecodeError::UnsupportedVersion { found, supported }
                if found == LOG_SCHEMA_VERSION + 1 && supported == LOG_SCHEMA_VERSION
        ),
        "{error:?}"
    );
}

#[test]
fn older_schema_version_goes_through_the_upcast_chain() {
    let payload = json!({"InputRecorded": {"input": []}});
    let error = decode_entry(&entry(1, 0, payload)).unwrap_err();
    assert!(
        matches!(error, LogDecodeError::Upcast { from: 0, to: 1, .. }),
        "{error:?}"
    );
}

#[test]
fn malformed_opaque_ids_are_decode_errors_not_panics() {
    let payload = json!({"TurnStarted": {"turn_id": "éééééééééééééééé"}});
    let error = decode_entry(&entry(1, LOG_SCHEMA_VERSION, payload)).unwrap_err();
    assert!(matches!(error, LogDecodeError::Decode { .. }), "{error:?}");
}

#[test]
fn event_kind_is_the_persistent_variant_name() {
    let event = assistant("hi");
    assert_eq!(event.kind(), "AssistantMessage");
}

#[test]
fn dedup_keys_follow_the_shared_key_table() {
    let tool_call = call(1);

    // ToolCallRequested and ToolResult share the tool call id; the event kind is
    // the discriminator that keeps them apart.
    assert_eq!(dedup::tool_call(tool_call), dedup::tool_result(tool_call));
    assert_eq!(
        dedup::assistant_message(turn(1), step(1)).as_str(),
        dedup::assistant_message(turn(1), step(1)).as_str()
    );
    assert_ne!(
        dedup::assistant_message(turn(1), step(1)),
        dedup::assistant_message(turn(1), step(2))
    );
    assert_eq!(
        dedup::compaction(LogId::from_i64(7)).as_str(),
        "compaction:7"
    );
    assert_eq!(
        dedup::signal(SignalId::from_bytes([3; 16])).as_str(),
        SignalId::from_bytes([3; 16]).to_hex()
    );
}

fn visible_user(value: &str) -> (u64, SessionEvent) {
    (
        0,
        SessionEvent::InputRecorded {
            input: vec![text(value)],
        },
    )
}

#[test]
fn projection_folds_compaction_and_keeps_the_raw_log() {
    let mut entries = vec![
        entry(
            1,
            1,
            json!({"InputRecorded": {"input": [{"Text": {"text": "hi"}}]}}),
        ),
        entry(
            2,
            1,
            json!({"StepStarted": {"turn_id": turn(1), "step_id": step(1)}}),
        ),
        entry(3, 1, serde_json::to_value(assistant("old")).unwrap()),
        entry(
            4,
            1,
            serde_json::to_value(SessionEvent::CompactionApplied {
                first_kept: LogId::from_i64(5),
                summary: vec![text("first summary")],
                replaces: LogIdRange {
                    first: LogId::from_i64(1),
                    last: LogId::from_i64(3),
                },
                model: model(),
                usage: Usage::default(),
                raw: None,
            })
            .unwrap(),
        ),
        entry(
            5,
            1,
            json!({"StepStarted": {"turn_id": turn(1), "step_id": step(2)}}),
        ),
        entry(6, 1, serde_json::to_value(assistant("new")).unwrap()),
        entry(
            7,
            1,
            serde_json::to_value(SessionEvent::CompactionApplied {
                first_kept: LogId::from_i64(8),
                summary: vec![text("second summary")],
                replaces: LogIdRange {
                    first: LogId::from_i64(1),
                    last: LogId::from_i64(6),
                },
                model: model(),
                usage: Usage::default(),
                raw: None,
            })
            .unwrap(),
        ),
        entry(
            8,
            1,
            json!({"StepStarted": {"turn_id": turn(1), "step_id": step(3)}}),
        ),
        entry(9, 1, serde_json::to_value(assistant("latest")).unwrap()),
    ];

    let history = project(&entries).expect("project");

    assert_eq!(history.len(), 2, "{history:?}");
    assert_eq!(
        history[0].origin,
        ProjectedOrigin::Summary(LogIdRange {
            first: LogId::from_i64(1),
            last: LogId::from_i64(6),
        })
    );
    assert_eq!(
        history[0].items,
        vec![Item::Compaction {
            parts: vec![text("second summary")]
        }]
    );
    assert_eq!(history[1].origin, ProjectedOrigin::Log(LogId::from_i64(9)));
    assert_eq!(
        history[1].items,
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text("latest")],
        }]
    );

    // The projection is a read-only fold: the raw log is untouched and still scannable.
    entries.sort_by_key(|entry| entry.seq);
    assert_eq!(entries.len(), 9);
}

#[test]
fn projection_orders_by_seq_not_by_input_order() {
    let mut entries = vec![
        entry(1, 1, serde_json::to_value(assistant("first")).unwrap()),
        entry(2, 1, serde_json::to_value(assistant("second")).unwrap()),
    ];
    entries.reverse();
    let history = project(&entries).expect("project");
    assert_eq!(
        history[0].items,
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text("first")],
        }]
    );
    assert_eq!(
        history[1].items,
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text("second")],
        }]
    );
}

#[test]
fn projection_turns_input_and_tool_results_into_items() {
    let tool_call = call(4);
    let events = [
        SessionEvent::InputRecorded {
            input: vec![text("run it")],
        },
        SessionEvent::ToolCallRequested {
            turn_id: turn(1),
            step_id: step(1),
            tool_call_id: tool_call,
            provider_call_id: "provider-call".to_owned(),
            name: ToolName::new("echo"),
            raw_args: JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
        SessionEvent::ToolResult {
            tool_call_id: tool_call,
            status: ToolResultStatus::Completed,
            output: Some(vec![text("done")]),
            error: None,
            truncated: false,
        },
        SessionEvent::ToolResult {
            tool_call_id: call(5),
            status: ToolResultStatus::Failed,
            output: None,
            error: Some(ToolErrorInfo {
                kind: ToolErrorKind::Denied,
                message: "no".to_owned(),
            }),
            truncated: false,
        },
    ];
    let entries: Vec<LogEntry> = events
        .iter()
        .enumerate()
        .map(|(index, event)| entry(index as u64 + 1, 1, serde_json::to_value(event).unwrap()))
        .collect();

    let error = project(&entries).unwrap_err();
    assert!(matches!(error, ProjectionError::UnknownToolCall { .. }));

    let entries = &entries[..3];
    let history = project(entries).expect("project");
    assert_eq!(
        history[0].items,
        vec![Item::Message {
            role: Role::User,
            parts: vec![text("run it")],
        }]
    );
    assert_eq!(
        history[1].items,
        vec![Item::ToolResult {
            call_id: "provider-call".to_owned(),
            provider_item_id: None,
            parts: vec![text("done")],
            is_error: false,
            raw: None,
        }]
    );
}

#[test]
fn user_input_projection_uses_a_user_message() {
    let (_, event) = visible_user("hello");
    let entries = vec![entry(1, 1, serde_json::to_value(event).unwrap())];
    let history = project(&entries).expect("project");
    assert_eq!(
        history[0].items,
        vec![Item::Message {
            role: Role::User,
            parts: vec![text("hello")],
        }]
    );
}

#[test]
fn config_snapshot_round_trips_through_json() {
    let snapshot = config();
    let json = serde_json::to_value(&snapshot).expect("serialize");
    let back: ConfigSnapshot = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, snapshot);
}

#[test]
fn usage_totals_use_the_exclusive_rule() {
    let usage = Usage {
        input: InputTokens {
            uncached: 10,
            cache_read: 2,
            cache_write: 3,
        },
        output: OutputTokens {
            total: 7,
            reasoning: 4,
        },
    };
    assert_eq!(usage.input_total(), 15);
    assert_eq!(usage.total(), 22);
}
