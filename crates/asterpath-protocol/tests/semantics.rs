//! Behaviour of the canonical model, log decoding and folds.

use asterpath_protocol::*;
use asterpath_protocol::{LogEntry, LogId, LogIdRange, SessionId, StepId, ToolCallId, TurnId};
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
        system_sections: Vec::new(),
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
            raw: None,
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
fn older_schema_version_is_rejected_without_upcasting() {
    let payload = json!({"InputRecorded": {"input": []}});
    let error = decode_entry(&entry(1, 0, payload)).unwrap_err();
    assert!(
        matches!(error, LogDecodeError::UnsupportedVersion { found: 0, .. }),
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
        dedup::approval_decided(ToolCallId::from_bytes([3; 16])),
        dedup::tool_call(ToolCallId::from_bytes([3; 16]))
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
            raw: None,
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
            raw: None,
        }]
    );
    assert_eq!(
        history[1].items,
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text("second")],
            raw: None,
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
            raw: None,
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
            raw: None,
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
fn model_request_copies_stop_sequences_and_provider_options() {
    let mut request = RequestConfig::default();
    request.stop_sequences = vec!["END".to_owned()];
    request.provider_options.insert(
        ProviderId::new("openai"),
        JsonString::new(r#"{"service_tier":"flex"}"#),
    );
    let model_request = ModelRequest::new(model(), 0, vec![], vec![], vec![], vec![], &request);
    assert_eq!(model_request.stop_sequences, request.stop_sequences);
    assert_eq!(model_request.provider_options, request.provider_options);
}

#[test]
fn model_request_copies_output_format() {
    let mut request = RequestConfig::default();
    request.output_format = Some(OutputFormat::new(
        "answer",
        serde_json::json!({"type": "object"}),
    ));
    let model_request = ModelRequest::new(model(), 0, vec![], vec![], vec![], vec![], &request);
    assert_eq!(model_request.output_format, request.output_format);
}

#[test]
fn request_config_recorded_before_output_format_decodes_without_it() {
    let mut json = serde_json::to_value(RequestConfig::default()).expect("serialize");
    json.as_object_mut()
        .expect("object")
        .remove("output_format");
    let back: RequestConfig = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.output_format, None);
}

#[test]
fn model_request_copies_server_tools() {
    let mut request = RequestConfig::default();
    request.server_tools.insert(
        ProviderId::new("anthropic"),
        vec![JsonString::new(
            r#"{"type":"web_search_20250305","name":"web_search"}"#,
        )],
    );
    let model_request = ModelRequest::new(model(), 0, vec![], vec![], vec![], vec![], &request);
    assert_eq!(model_request.server_tools, request.server_tools);
}

#[test]
fn request_config_without_server_tools_still_decodes() {
    let mut json = serde_json::to_value(RequestConfig::default()).expect("serialize");
    json.as_object_mut().expect("object").remove("server_tools");
    let back: RequestConfig = serde_json::from_value(json).expect("deserialize");
    assert!(back.server_tools.is_empty());
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

fn changed(model_name: &str) -> SessionEvent {
    let mut patch = ConfigPatch::default();
    patch.model = Some(ModelSpec {
        model: ModelId::new(model_name),
        ..model()
    });
    SessionEvent::ConfigChanged { patch }
}

fn records(events: Vec<SessionEvent>) -> Vec<(LogEntry, SessionEvent)> {
    events
        .into_iter()
        .zip(1u64..)
        .map(|(event, seq)| (entry(seq, LOG_SCHEMA_VERSION, json!(null)), event))
        .collect()
}

#[test]
fn effective_config_applies_changes_below_the_watermark_only() {
    let records = records(vec![
        SessionEvent::SessionCreated {
            config: config(),
            parent: None,
            metadata: Default::default(),
        },
        changed("second"),
        SessionEvent::StepStarted {
            turn_id: turn(1),
            step_id: step(1),
        },
        changed("third"),
    ]);
    let at = |watermark| {
        effective_config(&records, watermark)
            .expect("has SessionCreated")
            .model
            .model
    };
    assert_eq!(at(1), ModelId::new("model"), "SessionCreated alone");
    assert_eq!(
        at(3),
        ModelId::new("second"),
        "the step at seq 3 sees seq 2"
    );
    assert_eq!(at(u64::MAX), ModelId::new("third"));
}

#[test]
fn a_patch_overrides_only_the_fields_it_carries() {
    let mut request = RequestConfig::default();
    request.stop_sequences = vec!["END".to_owned()];
    let mut patch = ConfigPatch::default();
    patch.request = Some(request.clone());
    let mut snapshot = config();
    snapshot.apply(&patch);
    assert_eq!(snapshot.request, request);
    assert_eq!(snapshot.model, model(), "an absent field keeps its value");
}

#[test]
fn effective_config_needs_session_created() {
    let records = records(vec![changed("second")]);
    assert!(effective_config(&records, u64::MAX).is_none());
}

#[test]
fn config_changed_round_trips_through_the_log_encoding() {
    let event = changed("second");
    assert_eq!(event.kind(), "ConfigChanged");
    let new = encode_entry(session(1), None, &event).expect("encode");
    let stored = entry(1, new.schema_version, new.payload);
    assert_eq!(decode_entry(&stored).expect("decode"), event);
}

fn appended(value: &str) -> SessionEvent {
    let mut patch = ConfigPatch::default();
    patch.append_system = vec![text(value)];
    SessionEvent::ConfigChanged { patch }
}

fn log(events: &[SessionEvent]) -> Vec<LogEntry> {
    events
        .iter()
        .zip(1u64..)
        .map(|(event, seq)| entry(seq, 1, serde_json::to_value(event).unwrap()))
        .collect()
}

fn created_with_system(value: &str) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: ConfigSnapshot {
            system: vec![text(value)],
            ..config()
        },
        parent: None,
        metadata: Default::default(),
    }
}

fn tool_step(call_byte: u8) -> [SessionEvent; 2] {
    [
        SessionEvent::AssistantMessage {
            turn_id: turn(1),
            step_id: step(call_byte),
            items: vec![Item::ToolCall {
                call_id: format!("call-{call_byte}"),
                provider_item_id: None,
                name: ToolName::new("echo"),
                raw_args: JsonString::new("{}"),
                raw: None,
            }],
            raw: None,
            model: model(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        },
        SessionEvent::ToolCallRequested {
            turn_id: turn(1),
            step_id: step(call_byte),
            tool_call_id: call(call_byte),
            provider_call_id: format!("call-{call_byte}"),
            name: ToolName::new("echo"),
            raw_args: JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
    ]
}

fn result(call_byte: u8) -> SessionEvent {
    SessionEvent::ToolResult {
        tool_call_id: call(call_byte),
        status: ToolResultStatus::Completed,
        output: Some(vec![text("done")]),
        error: None,
        truncated: false,
    }
}

fn step_started(byte: u8) -> SessionEvent {
    SessionEvent::StepStarted {
        turn_id: turn(1),
        step_id: step(byte),
    }
}

fn instruction(value: &str) -> Item {
    Item::Instruction {
        key: None,
        parts: vec![text(value)],
    }
}

#[test]
fn effective_config_appends_system_instructions() {
    let records = records(vec![created_with_system("base"), appended("more")]);
    let config = effective_config(&records, u64::MAX).expect("config");
    assert_eq!(config.system, vec![text("base"), text("more")]);
}

#[test]
fn an_appended_instruction_lands_after_the_tool_results_at_the_next_step() {
    let [call_message, requested] = tool_step(1);
    let entries = log(&[
        created_with_system("base"),
        SessionEvent::InputRecorded {
            input: vec![text("hi")],
        },
        step_started(1),
        call_message,
        requested,
        // Appended while the tool runs: between tool_use and tool_result.
        appended("be brief"),
        result(1),
        step_started(2),
        assistant("ok"),
    ]);

    // The request of step 2 sees everything below its StepStarted (seq 8).
    let at_step = request_history(&entries[..7]).expect("project");
    assert_eq!(
        at_step.system,
        vec![text("base")],
        "top-level system is unchanged"
    );
    assert!(matches!(at_step.items[1], Item::ToolCall { .. }));
    assert!(matches!(at_step.items[2], Item::ToolResult { .. }));
    assert_eq!(at_step.items[3], instruction("be brief"));
    assert_eq!(at_step.items.len(), 4);

    // Later requests keep it at the same place, before the step's reply.
    let later = request_history(&entries).expect("project");
    assert_eq!(later.items[..4], at_step.items[..]);
    assert!(matches!(later.items[4], Item::Message { .. }));
}

#[test]
fn compaction_moves_replaced_instructions_into_the_baseline_system() {
    let [call_message, requested] = tool_step(1);
    let entries = log(&[
        created_with_system("base"),
        SessionEvent::InputRecorded {
            input: vec![text("hi")],
        },
        step_started(1),
        call_message,
        requested,
        appended("first"),
        result(1),
        step_started(2),
        assistant("ok"),
        appended("second"),
        SessionEvent::CompactionApplied {
            first_kept: LogId::from_i64(12),
            summary: vec![text("summary")],
            replaces: LogIdRange {
                first: LogId::from_i64(2),
                last: LogId::from_i64(9),
            },
            model: model(),
            usage: Usage::default(),
            raw: None,
        },
        step_started(3),
    ]);

    let projected = request_history(&entries).expect("project");
    assert_eq!(projected.system, vec![text("base"), text("first")]);
    assert_eq!(
        projected.items,
        vec![
            Item::Compaction {
                parts: vec![text("summary")]
            },
            instruction("second"),
        ],
        "an instruction after the replaced range keeps its place"
    );
}

fn tool(name: &str) -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new(name),
        format!("{name} tool"),
        json!({"type": "object"}),
    )
    .with_parallelism(Parallelism::Parallel)
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly)
}

fn created_with_tools(names: &[&str]) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: ConfigSnapshot {
            tools: names.iter().map(|name| tool(name)).collect(),
            ..config()
        },
        parent: None,
        metadata: Default::default(),
    }
}

fn tools_changed(added: &[&str], removed: &[&str]) -> SessionEvent {
    let mut patch = ConfigPatch::default();
    patch.add_tools = added.iter().map(|name| tool(name)).collect();
    patch.remove_tools = removed.iter().map(|name| ToolName::new(*name)).collect();
    SessionEvent::ConfigChanged { patch }
}

fn tools_item(added: &[&str], removed: &[&str]) -> Item {
    Item::ToolsChanged {
        added: added.iter().map(|name| tool(name)).collect(),
        removed: removed.iter().map(|name| ToolName::new(*name)).collect(),
    }
}

fn names(tools: &[ToolDescriptor]) -> Vec<&str> {
    tools.iter().map(|tool| tool.name.as_str()).collect()
}

#[test]
fn effective_config_adds_and_removes_tools() {
    let records = records(vec![
        created_with_tools(&["echo", "read"]),
        tools_changed(&["write"], &["echo"]),
    ]);
    let config = effective_config(&records, u64::MAX).expect("config");
    assert_eq!(names(&config.tools), ["read", "write"]);
}

#[test]
fn a_tool_change_lands_at_the_next_step_and_keeps_the_baseline_tools() {
    let [call_message, requested] = tool_step(1);
    let entries = log(&[
        created_with_tools(&["echo"]),
        SessionEvent::InputRecorded {
            input: vec![text("hi")],
        },
        step_started(1),
        call_message,
        requested,
        tools_changed(&["write"], &["echo"]),
        result(1),
        step_started(2),
    ]);
    let projected = request_history(&entries).expect("project");
    assert_eq!(names(&projected.tools), ["echo"], "baseline is unchanged");
    assert!(matches!(projected.items[2], Item::ToolResult { .. }));
    assert_eq!(projected.items[3], tools_item(&["write"], &["echo"]));
}

#[test]
fn compaction_moves_replaced_tool_changes_into_the_baseline_tools() {
    let entries = log(&[
        created_with_tools(&["echo"]),
        SessionEvent::InputRecorded {
            input: vec![text("hi")],
        },
        tools_changed(&["write"], &[]),
        step_started(1),
        assistant("ok"),
        tools_changed(&[], &["echo"]),
        SessionEvent::CompactionApplied {
            first_kept: LogId::from_i64(7),
            summary: vec![text("summary")],
            replaces: LogIdRange {
                first: LogId::from_i64(2),
                last: LogId::from_i64(5),
            },
            model: model(),
            usage: Usage::default(),
            raw: None,
        },
        step_started(2),
    ]);
    let projected = request_history(&entries).expect("project");
    assert_eq!(names(&projected.tools), ["echo", "write"]);
    assert_eq!(projected.items[1], tools_item(&[], &["echo"]));
}

#[test]
fn a_patch_recorded_before_tool_changes_still_decodes() {
    let patch: ConfigPatch = serde_json::from_value(json!({"model": null})).expect("decode");
    assert!(!patch.changes_tools());
}

fn section(key: &str, value: &str) -> SystemSection {
    SystemSection::new(key, vec![text(value)])
}

fn refreshed(key: &str, value: &str) -> SessionEvent {
    let mut patch = ConfigPatch::default();
    patch.system_sections = vec![section(key, value)];
    SessionEvent::ConfigChanged { patch }
}

fn removed_section(key: &str) -> SessionEvent {
    let mut patch = ConfigPatch::default();
    patch.system_sections = vec![SystemSection::new(key, vec![])];
    SessionEvent::ConfigChanged { patch }
}

fn created_with_sections(sections: Vec<SystemSection>) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: ConfigSnapshot {
            system: vec![text("base")],
            system_sections: sections,
            ..config()
        },
        parent: None,
        metadata: Default::default(),
    }
}

#[test]
fn effective_config_replaces_and_removes_sections_by_key() {
    let records = records(vec![
        created_with_sections(vec![section("agents", "v0"), section("date", "mon")]),
        refreshed("agents", "v1"),
        refreshed("skills", "s1"),
        removed_section("date"),
    ]);
    let config = effective_config(&records, u64::MAX).expect("config");
    assert_eq!(config.system, vec![text("base")]);
    assert_eq!(
        config.system_sections,
        vec![section("agents", "v1"), section("skills", "s1")],
        "a refresh keeps the section's place; a new key goes last"
    );
}

#[test]
fn a_keyed_refresh_is_appended_in_place_and_leaves_the_baseline_alone() {
    let entries = log(&[
        created_with_sections(vec![section("agents", "v0")]),
        SessionEvent::InputRecorded {
            input: vec![text("hi")],
        },
        refreshed("agents", "v1"),
        step_started(1),
    ]);
    let history = request_history(&entries).expect("project");
    assert_eq!(history.system, vec![text("base")]);
    assert_eq!(history.system_sections, vec![section("agents", "v0")]);
    assert_eq!(
        history.items[1],
        Item::Instruction {
            key: Some("agents".to_owned()),
            parts: vec![
                text("Instruction section `agents` (supersedes any earlier text of this section):"),
                text("v1"),
            ],
        }
    );
}

#[test]
fn compaction_keeps_only_the_latest_value_of_a_section_refreshed_three_times() {
    let entries = log(&[
        created_with_sections(vec![section("agents", "v0"), section("date", "mon")]),
        SessionEvent::InputRecorded {
            input: vec![text("hi")],
        },
        refreshed("agents", "v1"),
        step_started(1),
        assistant("ok"),
        refreshed("agents", "v2"),
        step_started(2),
        assistant("ok"),
        refreshed("agents", "v3"),
        removed_section("date"),
        appended("plain"),
        step_started(3),
        assistant("ok"),
        SessionEvent::CompactionApplied {
            first_kept: LogId::from_i64(15),
            summary: vec![text("summary")],
            replaces: LogIdRange {
                first: LogId::from_i64(2),
                last: LogId::from_i64(13),
            },
            model: model(),
            usage: Usage::default(),
            raw: None,
        },
        step_started(4),
    ]);
    let history = request_history(&entries).expect("project");
    assert_eq!(history.system, vec![text("base"), text("plain")]);
    assert_eq!(history.system_sections, vec![section("agents", "v3")]);
    assert_eq!(
        history.items,
        vec![Item::Compaction {
            parts: vec![text("summary")]
        }]
    );
}

#[test]
fn a_removed_section_renders_as_a_removal_notice() {
    let entries = log(&[
        created_with_sections(vec![section("date", "mon")]),
        removed_section("date"),
        step_started(1),
    ]);
    let history = request_history(&entries).expect("project");
    assert_eq!(
        history.items,
        vec![Item::Instruction {
            key: Some("date".to_owned()),
            parts: vec![text(
                "Instruction section `date` removed; disregard its earlier text."
            )],
        }]
    );
}
