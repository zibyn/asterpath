//! The live `AgentEvent` JSON shape is a Tier 1 promise: tag `type`,
//! snake_case variant names, IDs as hex strings.

use asterpath_protocol::{
    AgentEvent, ApprovalRequest, InputTokens, JsonString, OutputTokens, Part, PartDelta,
    Provenance, ReplaySafety, SessionId, SideEffect, StepId, ToolCallId, ToolErrorInfo,
    ToolErrorKind, ToolName, ToolResultStatus, TurnEndReason, TurnId, Usage,
};

const HEX1: &str = "01010101010101010101010101010101";
const HEX2: &str = "02020202020202020202020202020202";
const HEX3: &str = "03030303030303030303030303030303";

fn json(event: AgentEvent) -> String {
    serde_json::to_string(&event).expect("AgentEvent serializes")
}

fn delta(delta: PartDelta) -> AgentEvent {
    AgentEvent::ModelDelta {
        turn: TurnId::from_bytes([1; 16]),
        step: StepId::from_bytes([2; 16]),
        output_index: 3,
        part_index: 0,
        delta,
    }
}

#[test]
fn idle() {
    assert_eq!(
        json(AgentEvent::Idle {
            session: SessionId::from_bytes([1; 16])
        }),
        format!(r#"{{"type":"idle","session":"{HEX1}"}}"#)
    );
}

#[test]
fn turn_started() {
    assert_eq!(
        json(AgentEvent::TurnStarted {
            turn: TurnId::from_bytes([1; 16])
        }),
        format!(r#"{{"type":"turn_started","turn":"{HEX1}"}}"#)
    );
}

#[test]
fn model_delta_text() {
    assert_eq!(
        json(delta(PartDelta::Text { text: "hi".into() })),
        format!(
            r#"{{"type":"model_delta","turn":"{HEX1}","step":"{HEX2}","output_index":3,"part_index":0,"delta":{{"type":"text","text":"hi"}}}}"#
        )
    );
}

#[test]
fn model_delta_json_args() {
    assert_eq!(
        json(delta(PartDelta::JsonArgs {
            fragment: r#"{"a""#.into()
        })),
        format!(
            r#"{{"type":"model_delta","turn":"{HEX1}","step":"{HEX2}","output_index":3,"part_index":0,"delta":{{"type":"json_args","fragment":"{{\"a\""}}}}"#
        )
    );
}

#[test]
fn model_delta_reasoning_summary() {
    assert_eq!(
        json(delta(PartDelta::ReasoningSummary {
            text: "think".into()
        })),
        format!(
            r#"{{"type":"model_delta","turn":"{HEX1}","step":"{HEX2}","output_index":3,"part_index":0,"delta":{{"type":"reasoning_summary","text":"think"}}}}"#
        )
    );
}

#[test]
fn tool_started() {
    assert_eq!(
        json(AgentEvent::ToolStarted {
            turn: TurnId::from_bytes([1; 16]),
            step: StepId::from_bytes([2; 16]),
            output_index: 1,
            call: ToolCallId::from_bytes([3; 16]),
            tool: ToolName::new("echo"),
            args: JsonString::new(r#"{"q":"hi"}"#),
        }),
        format!(
            r#"{{"type":"tool_started","turn":"{HEX1}","step":"{HEX2}","output_index":1,"call":"{HEX3}","tool":"echo","args":"{{\"q\":\"hi\"}}"}}"#
        )
    );
}

#[test]
fn tool_finished() {
    assert_eq!(
        json(AgentEvent::ToolFinished {
            turn: TurnId::from_bytes([1; 16]),
            step: StepId::from_bytes([2; 16]),
            call: ToolCallId::from_bytes([3; 16]),
            status: ToolResultStatus::Completed,
            output: Some(vec![Part::Text { text: "hi".into() }]),
            error: None,
        }),
        format!(
            r#"{{"type":"tool_finished","turn":"{HEX1}","step":"{HEX2}","call":"{HEX3}","status":"Completed","output":[{{"Text":{{"text":"hi"}}}}],"error":null}}"#
        )
    );
}

#[test]
fn tool_finished_failed() {
    assert_eq!(
        json(AgentEvent::ToolFinished {
            turn: TurnId::from_bytes([1; 16]),
            step: StepId::from_bytes([2; 16]),
            call: ToolCallId::from_bytes([3; 16]),
            status: ToolResultStatus::Failed,
            output: None,
            error: Some(ToolErrorInfo {
                kind: ToolErrorKind::Timeout,
                message: "slow".into(),
            }),
        }),
        format!(
            r#"{{"type":"tool_finished","turn":"{HEX1}","step":"{HEX2}","call":"{HEX3}","status":"Failed","output":null,"error":{{"kind":"Timeout","message":"slow"}}}}"#
        )
    );
}

#[test]
fn approval_requested() {
    let request = ApprovalRequest::new(
        SessionId::from_bytes([1; 16]),
        TurnId::from_bytes([1; 16]),
        StepId::from_bytes([2; 16]),
        ToolCallId::from_bytes([3; 16]),
        ToolName::new("write"),
        JsonString::new(r#"{"path":"x"}"#),
        ReplaySafety::Unsafe,
        SideEffect::Mutating,
        Provenance::InProcess,
    );
    assert_eq!(
        json(AgentEvent::ApprovalRequested(request)),
        format!(
            r#"{{"type":"approval_requested","session":"{HEX1}","root":"{HEX1}","turn":"{HEX1}","step":"{HEX2}","tool_call_id":"{HEX3}","tool":"write","args":"{{\"path\":\"x\"}}","replay":"Unsafe","side_effect":"Mutating","provenance":"InProcess"}}"#
        )
    );
}

#[test]
fn turn_committed() {
    assert_eq!(
        json(AgentEvent::TurnCommitted {
            turn: TurnId::from_bytes([1; 16]),
            reason: TurnEndReason::Stopped {
                reason: "hook".into()
            },
            usage: Usage {
                input: InputTokens {
                    uncached: 1,
                    cache_read: 2,
                    cache_write: 3,
                },
                output: OutputTokens {
                    total: 5,
                    reasoning: 4,
                },
            },
        }),
        format!(
            r#"{{"type":"turn_committed","turn":"{HEX1}","reason":{{"Stopped":{{"reason":"hook"}}}},"usage":{{"input":{{"uncached":1,"cache_read":2,"cache_write":3}},"output":{{"total":5,"reasoning":4}}}}}}"#
        )
    );
}
