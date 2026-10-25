//! Tool contract tests: default approval policy and tool-result projection
//! ordering.

use asterpath_protocol::{
    ApprovalPolicy, ApprovalRequest, ApprovalVerdict, DefaultApprovalPolicy, Item, JsonString,
    Part, Provenance, ReplaySafety, SessionEvent, SideEffect, ToolName, ToolResultStatus,
    TransportKind, encode_entry, history,
};
use asterpath_protocol::{LogEntry, LogId, SessionId, ToolCallId};

fn session() -> SessionId {
    SessionId::from_bytes([1; 16])
}

fn entry(seq: u64, event: SessionEvent) -> LogEntry {
    let payload = encode_entry(session(), None, &event)
        .expect("encodes")
        .payload;
    LogEntry {
        log_id: LogId::from_i64(seq as i64),
        session: session(),
        seq,
        recorded_at: 0,
        schema_version: asterpath_protocol::LOG_SCHEMA_VERSION,
        dedup_key: None,
        payload,
    }
}

fn request(provenance: Provenance) -> ApprovalRequest {
    ApprovalRequest::new(
        session(),
        asterpath_protocol::TurnId::from_bytes([2; 16]),
        asterpath_protocol::StepId::from_bytes([3; 16]),
        ToolCallId::from_bytes([4; 16]),
        ToolName::new("tool"),
        JsonString::new("{}"),
        ReplaySafety::Unsafe,
        SideEffect::Mutating,
        provenance,
    )
}

#[test]
fn default_policy_allows_in_process_and_asks_for_external_calls() {
    let policy = DefaultApprovalPolicy;

    assert_eq!(
        policy.classify(&request(Provenance::InProcess)),
        ApprovalVerdict::Allow
    );
    // The pipeline denies the `Ask` when no endpoint attends the tree.
    assert_eq!(
        policy.classify(&request(Provenance::External {
            transport: TransportKind::Http
        })),
        ApprovalVerdict::Ask
    );
    // `Unknown` is never asked.
    assert!(matches!(
        policy.classify(&request(Provenance::Unknown)),
        ApprovalVerdict::Deny { .. }
    ));
}

#[test]
fn tool_results_project_in_intent_order_not_completion_order() {
    let assistant = SessionEvent::AssistantMessage {
        turn_id: asterpath_protocol::TurnId::from_bytes([2; 16]),
        step_id: asterpath_protocol::StepId::from_bytes([3; 16]),
        items: vec![
            Item::ToolCall {
                call_id: "provider-a".to_owned(),
                provider_item_id: None,
                name: ToolName::new("echo"),
                raw_args: JsonString::new("{}"),
                raw: None,
            },
            Item::ToolCall {
                call_id: "provider-b".to_owned(),
                provider_item_id: None,
                name: ToolName::new("echo"),
                raw_args: JsonString::new("{}"),
                raw: None,
            },
        ],
        raw: None,
        model: asterpath_protocol::ModelSpec {
            provider: asterpath_protocol::ProviderId::new("p"),
            model: asterpath_protocol::ModelId::new("m"),
            context_window: None,
        },
        usage: asterpath_protocol::Usage {
            input: Default::default(),
            output: Default::default(),
        },
        stop_reason: asterpath_protocol::StopReason::ToolUse,
    };
    let intent_a = SessionEvent::ToolCallRequested {
        turn_id: asterpath_protocol::TurnId::from_bytes([2; 16]),
        step_id: asterpath_protocol::StepId::from_bytes([3; 16]),
        tool_call_id: ToolCallId::from_bytes([10; 16]),
        provider_call_id: "provider-a".to_owned(),
        name: ToolName::new("echo"),
        raw_args: JsonString::new("{}"),
        replay: ReplaySafety::Safe,
        provenance: Provenance::InProcess,
    };
    let intent_b = SessionEvent::ToolCallRequested {
        turn_id: asterpath_protocol::TurnId::from_bytes([2; 16]),
        step_id: asterpath_protocol::StepId::from_bytes([3; 16]),
        tool_call_id: ToolCallId::from_bytes([11; 16]),
        provider_call_id: "provider-b".to_owned(),
        name: ToolName::new("echo"),
        raw_args: JsonString::new("{}"),
        replay: ReplaySafety::Safe,
        provenance: Provenance::InProcess,
    };
    let result = |tool_call_id: ToolCallId, text: &str| SessionEvent::ToolResult {
        tool_call_id,
        status: ToolResultStatus::Completed,
        output: Some(vec![Part::Text {
            text: text.to_owned(),
        }]),
        error: None,
        truncated: false,
    };

    let entries = [
        entry(1, assistant),
        entry(2, intent_a),
        entry(3, intent_b),
        // Completion order is reversed relative to intent order.
        entry(4, result(ToolCallId::from_bytes([11; 16]), "result-b")),
        entry(5, result(ToolCallId::from_bytes([10; 16]), "result-a")),
    ];

    let items = history(&entries).expect("projects");
    let call_ids: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            Item::ToolResult { call_id, .. } => Some(call_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(call_ids, ["provider-a", "provider-b"]);

    let texts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            Item::ToolResult { parts, .. } => match parts.first() {
                Some(Part::Text { text }) => Some(text.as_str()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(texts, ["result-a", "result-b"]);
}
