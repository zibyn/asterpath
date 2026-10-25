//! CLI test helpers: the binary with a clean environment, and Anthropic SSE
//! bodies for `asterpath_testing::http_mock`.

#![allow(dead_code)]

use assert_cmd::Command;
pub use asterpath_testing::http_mock::{MockServer, Reply};

/// The CLI binary with host environment removed and a dummy key in place.
/// The default config path points nowhere, so the host's file is never read.
pub fn bin() -> Command {
    Command::from(process_bin())
}

/// [`bin`] as a plain process, for tests that talk to it while it runs.
pub fn process_bin() -> std::process::Command {
    let mut command = std::process::Command::new(assert_cmd::cargo::cargo_bin!("asterpath"));
    command.env("XDG_CONFIG_HOME", "/nonexistent/asterpath-test-config");
    for name in [
        "ASTERPATH_CONFIG",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "DEEPSEEK_API_KEY",
    ] {
        command.env_remove(name);
    }
    command.env("ANTHROPIC_API_KEY", "test-key");
    command
}

/// A complete Anthropic text response ending the turn.
pub fn text_sse(text: &str) -> String {
    sse("end_turn", text)
}

/// A response the loop classifies as a non-completed turn.
pub fn context_exceeded_sse(text: &str) -> String {
    sse("model_context_window_exceeded", text)
}

/// A complete Anthropic tool-use response. `args` is raw JSON text.
pub fn tool_use_sse(name: &str, args: &str) -> String {
    let args = serde_json::to_string(args).expect("json string");
    format!(
        concat!(
            "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_1\",",
            "\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-test\",",
            "\"stop_reason\":null,\"usage\":{{\"input_tokens\":5,\"output_tokens\":1}}}}}}\n\n",
            "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,",
            "\"content_block\":{{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"{name}\",\"input\":{{}}}}}}\n\n",
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,",
            "\"delta\":{{\"type\":\"input_json_delta\",\"partial_json\":{args}}}}}\n\n",
            "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n",
            "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"tool_use\",",
            "\"stop_sequence\":null}},\"usage\":{{\"output_tokens\":2}}}}\n\n",
            "event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
        ),
        name = name,
        args = args,
    )
}

fn sse(stop_reason: &str, text: &str) -> String {
    format!(
        concat!(
            "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_1\",",
            "\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-test\",",
            "\"stop_reason\":null,\"usage\":{{\"input_tokens\":5,\"output_tokens\":1}}}}}}\n\n",
            "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,",
            "\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n",
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,",
            "\"delta\":{{\"type\":\"text_delta\",\"text\":\"{text}\"}}}}\n\n",
            "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n",
            "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{stop_reason}\",",
            "\"stop_sequence\":null}},\"usage\":{{\"output_tokens\":2}}}}\n\n",
            "event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
        ),
        text = text,
        stop_reason = stop_reason,
    )
}

/// A complete OpenAI Responses text response.
pub fn openai_text_sse(text: &str) -> String {
    use serde_json::json;
    let item = json!({
        "id": "msg_1",
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    });
    [
        json!({"type": "response.created", "sequence_number": 0,
            "response": {"id": "resp_1", "status": "in_progress", "output": []}}),
        json!({"type": "response.output_item.added", "sequence_number": 1,
            "output_index": 0, "item": {"id": "msg_1", "type": "message",
                "status": "in_progress", "role": "assistant", "content": []}}),
        json!({"type": "response.output_item.done", "sequence_number": 2,
            "output_index": 0, "item": item}),
        json!({"type": "response.completed", "sequence_number": 3,
            "response": {"id": "resp_1", "status": "completed", "output": [item],
                "usage": {"input_tokens": 5, "input_tokens_details": {"cached_tokens": 0},
                    "output_tokens": 2, "output_tokens_details": {"reasoning_tokens": 0},
                    "total_tokens": 7}}}),
    ]
    .iter()
    .map(|data| {
        format!(
            "event: {}\ndata: {data}\n\n",
            data["type"].as_str().unwrap()
        )
    })
    .collect()
}
