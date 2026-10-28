//! Prompt-cache prefix checks on rendered request bodies (#175).
//!
//! [`cache_prefix`] flattens an Anthropic Messages or OpenAI Responses body
//! into the order the provider caches it, one JSON value per line, with the
//! cache breakpoint markers removed. A request reuses the previous request's
//! cache when the previous prefix is a byte prefix of its own, which is what
//! [`assert_cache_prefix`] checks.
//!
//! Lines, in order:
//! - Anthropic: `{"model"}`, each top-level tool, each system block, the
//!   parameters that invalidate the message cache (`tool_choice`, `thinking`,
//!   `output_config`, null when absent), then per message `{"role"}` followed
//!   by its content blocks. Blocks are flattened so a message that only gains
//!   blocks keeps its prefix, as the block-level cache does.
//! - OpenAI: `{"model"}`, `{"instructions"}`, each tool, `{"reasoning"}`, then
//!   each input item.
//!
//! Every other top-level field (`max_tokens`, sampling, `stream`, ...) is left
//! out: changing it does not touch the cached prefix. The parameter lines are
//! the conservative reading of each provider's caching docs; a scenario that
//! changes one asserts the break explicitly.

use serde_json::{Value, json};

/// The cacheable prefix of a rendered request body. The provider is told by
/// the body's shape: `messages` is Anthropic, `input` is OpenAI.
pub fn cache_prefix(body: &str) -> String {
    let body: Value = serde_json::from_str(body).expect("request body is JSON");
    let mut lines = vec![json!({"model": body["model"]})];
    let items = |key: &str| body[key].as_array().cloned().unwrap_or_default();
    if body.get("messages").is_some() {
        lines.extend(items("tools"));
        lines.extend(items("system"));
        for key in ["tool_choice", "thinking", "output_config"] {
            lines.push(json!({ key: body[key] }));
        }
        for message in items("messages") {
            lines.push(json!({"role": message["role"]}));
            match &message["content"] {
                Value::Array(blocks) => lines.extend(blocks.iter().cloned()),
                text => lines.push(json!({"type": "text", "text": text})),
            }
        }
    } else if body.get("input").is_some() {
        lines.push(json!({"instructions": body["instructions"]}));
        lines.extend(items("tools"));
        lines.push(json!({"reasoning": body["reasoning"]}));
        lines.extend(items("input"));
    } else {
        panic!("neither an Anthropic nor an OpenAI request body: {body}");
    }
    let mut out = String::new();
    for mut line in lines {
        // Breakpoints only sit on top-level blocks; nested keys are content.
        if let Value::Object(map) = &mut line {
            map.remove("cache_control");
        }
        out.push_str(&serde_json::to_string(&line).expect("JSON value serializes"));
        out.push('\n');
    }
    out
}

/// The line of `after`'s [`cache_prefix`] where it stops extending
/// `before`'s, or `None` when `before`'s prefix is a prefix of `after`'s.
pub fn prefix_break(before: &str, after: &str) -> Option<String> {
    let after = cache_prefix(after);
    let at = first_difference(&cache_prefix(before), &after)?;
    let start = after[..at].rfind('\n').map_or(0, |newline| newline + 1);
    let end = after[at..]
        .find('\n')
        .map_or(after.len(), |newline| at + newline);
    Some(after[start..end].to_owned())
}

fn first_difference(before: &str, after: &str) -> Option<usize> {
    if after.starts_with(before) {
        return None;
    }
    Some(
        before
            .bytes()
            .zip(after.bytes())
            .position(|(a, b)| a != b)
            .unwrap_or(before.len().min(after.len())),
    )
}

/// Panics unless `before`'s cacheable prefix is a prefix of `after`'s,
/// printing the first differing byte and the lines around it.
#[track_caller]
pub fn assert_cache_prefix(before: &str, after: &str) {
    let (before, after) = (cache_prefix(before), cache_prefix(after));
    if let Some(at) = first_difference(&before, &after) {
        panic!(
            "cache prefix broken at byte {at}\n--- before\n{}\n--- after\n{}",
            context(&before, at),
            context(&after, at)
        );
    }
}

/// The line holding byte `at` and the one before it, with a caret under `at`.
fn context(text: &str, at: usize) -> String {
    let at = at.min(text.len());
    let start = text[..at]
        .rfind('\n')
        .map(|newline| text[..newline].rfind('\n').map_or(0, |prev| prev + 1))
        .unwrap_or(0);
    let line_start = text[..at].rfind('\n').map_or(0, |newline| newline + 1);
    let end = text[at..]
        .find('\n')
        .map_or(text.len(), |newline| at + newline);
    let column = text[line_start..at].chars().count();
    format!("{}\n{}^", &text[start..end], " ".repeat(column))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anthropic(messages: Value, tools: Value) -> String {
        json!({
            "model": "m",
            "max_tokens": 10,
            "system": [{"type": "text", "text": "sys", "cache_control": {"type": "ephemeral"}}],
            "tools": tools,
            "messages": messages,
            "stream": true,
        })
        .to_string()
    }

    #[test]
    fn a_moved_breakpoint_and_appended_blocks_keep_the_prefix() {
        let before = anthropic(
            json!([{"role": "user", "content": [
                {"type": "text", "text": "go", "cache_control": {"type": "ephemeral"}}
            ]}]),
            json!([{"name": "a"}]),
        );
        let after = anthropic(
            json!([
                {"role": "user", "content": [{"type": "text", "text": "go"}]},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "ok", "cache_control": {"type": "ephemeral"}}
                ]},
            ]),
            json!([{"name": "a"}]),
        );
        assert_eq!(prefix_break(&before, &after), None);
        assert_cache_prefix(&before, &after);
    }

    #[test]
    fn an_added_tool_breaks_before_the_messages() {
        let messages = json!([{"role": "user", "content": "go"}]);
        let before = anthropic(messages.clone(), json!([{"name": "a"}]));
        let after = anthropic(messages, json!([{"name": "a"}, {"name": "b"}]));
        assert_eq!(
            prefix_break(&before, &after).as_deref(),
            Some(r#"{"name":"b"}"#)
        );
    }

    #[test]
    fn non_prefix_fields_are_ignored_and_openai_is_detected() {
        let body = |max: u32, effort: &str| {
            json!({
                "model": "m",
                "instructions": "sys",
                "input": [{"type": "message", "role": "user", "content": "go"}],
                "max_output_tokens": max,
                "reasoning": {"effort": effort},
            })
            .to_string()
        };
        assert_eq!(prefix_break(&body(10, "low"), &body(20, "low")), None);
        assert!(prefix_break(&body(10, "low"), &body(10, "high")).is_some());
    }

    #[test]
    #[should_panic(expected = "cache prefix broken at byte")]
    fn a_rewritten_message_fails_with_its_position() {
        let before = anthropic(json!([{"role": "user", "content": "go"}]), json!([]));
        let after = anthropic(json!([{"role": "user", "content": "stop"}]), json!([]));
        assert_cache_prefix(&before, &after);
    }
}
