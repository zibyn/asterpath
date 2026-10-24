//! The tool-set fingerprint is persisted in `ReasoningBinding.tools_hash`; any
//! change to its output silently invalidates recorded reasoning.

use asterpath_protocol::{
    Parallelism, Provenance, ReplaySafety, SideEffect, ToolDescriptor, ToolName, tools_hash,
};
use serde_json::json;

fn descriptor(name: &str, schema: serde_json::Value) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: format!("{name} tool"),
        schema,
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::ReadOnly,
        provenance: Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
        strict: None,
    }
}

#[test]
fn tools_hash_output_is_pinned() {
    // Expected value computed independently: SHA-256 of the name-sorted,
    // key-sorted, compact JSON array of {name, description, schema}.
    let tools = [
        descriptor(
            "search",
            json!({"type": "object", "required": ["q"], "properties": {"q": {"type": "string"}}}),
        ),
        descriptor("echo", json!({"type": "object"})),
    ];
    assert_eq!(
        tools_hash(&tools, &[]),
        "6991b469916c8790be8c4be6cd34b233adda190e81776173a6829c4af1d0a1a1"
    );
}

#[test]
fn tools_hash_ignores_tool_order() {
    let first = descriptor("a", json!({}));
    let second = descriptor("b", json!({}));
    assert_eq!(
        tools_hash(&[first.clone(), second.clone()], &[]),
        tools_hash(&[second, first], &[])
    );
}

#[test]
fn server_tools_wrap_the_fingerprint_in_an_object() {
    // Expected value computed independently: SHA-256 of
    // {"server_tools":[{"name":"web_search","type":"web_search_20250305"}],
    //  "tools":[{"description":"echo tool","name":"echo","schema":{"type":"object"}}]}
    // compact, keys sorted.
    let tools = [descriptor("echo", json!({"type": "object"}))];
    let server_tools = [json!({"type": "web_search_20250305", "name": "web_search"})];
    assert_eq!(
        tools_hash(&tools, &server_tools),
        "40e9bc43200dd74c9a8901bf4d94e2bae8c94a0a04bdd95186fa8b883b12c4f6"
    );
}

#[test]
fn tools_hash_includes_strict_only_when_set() {
    // Expected value computed independently: as above, with `"strict": true`
    // added to the one tool that sets it.
    let mut echo = descriptor("echo", json!({"type": "object"}));
    echo.strict = Some(true);
    let tools = [echo, descriptor("search", json!({"type": "object"}))];
    assert_eq!(
        tools_hash(&tools, &[]),
        "d8c7f8099d9f86f5da7d09792c17b56452247bcc6e9613030be9c74945ad3d58"
    );
}

#[test]
fn strict_and_server_tools_combine_in_the_object_form() {
    // Expected value computed independently (Python json.dumps, sort_keys,
    // compact) as SHA-256 of
    // {"server_tools":[{"name":"web_search","type":"web_search_20250305"}],
    //  "tools":[{"description":"echo tool","name":"echo","schema":{"type":"object"},"strict":false},
    //           {"description":"search tool","name":"search","schema":{"type":"object"}}]}
    let mut echo = descriptor("echo", json!({"type": "object"}));
    echo.strict = Some(false);
    let tools = [descriptor("search", json!({"type": "object"})), echo];
    let server_tools = [json!({"type": "web_search_20250305", "name": "web_search"})];
    assert_eq!(
        tools_hash(&tools, &server_tools),
        "a0149e2316cce3120522322a6de56f901ce8a34acc0bc0ad0eeb08b08d5a2c9d"
    );
}
