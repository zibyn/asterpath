//! Mid-session config changes through real adapters. A model switch (#109):
//! reasoning bound to the old model is not replayed, and is replayed again
//! after switching back; the gating is the adapter's binding check. An
//! appended instruction (#110) renders in place and keeps old thinking. A
//! tool change (#111) renders inline on Anthropic and as the effective
//! top-level `tools` on OpenAI.
#![cfg(unix)]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use asterpath::anthropic::{AnthropicModelProfile, AnthropicProvider};
use asterpath::openai::OpenAiProvider;
use asterpath::{Asterpath, OwnedSession};
use asterpath_protocol::{
    CompactionConfig, ConfigPatch, InputId, ModelId, ModelSpec, Part, Provider, ProviderId,
    ReplaySafety, RequestConfig, SessionSpec, SideEffect, SubagentConfig, Tool, ToolArgs,
    ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput, ToolSelection, TurnConfig,
    UpdateId, UserInput,
};
use asterpath_protocol::{SessionEvent, ToolErrorKind, ToolResultStatus, decode_entry, scan_all};
use asterpath_testing::http_mock::MockServer;
use asterpath_testing::{Fixture, InMemoryEventLog};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use serde_json::{Value, json};

fn model(name: &str) -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("openai"),
        model: ModelId::new(name),
        context_window: None,
    }
}

/// Switches the session to model-2 on its first call and back to model-1 on
/// its second.
struct Switch {
    descriptor: ToolDescriptor,
    handle: OnceLock<OwnedSession>,
    calls: AtomicUsize,
}

impl Tool for Switch {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let target = match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "model-2",
                _ => "model-1",
            };
            let mut update = ConfigPatch::default();
            update.model = Some(model(target));
            self.handle
                .get()
                .expect("handle set")
                .update_config(update)
                .await
                .expect("update accepted");
            Ok(ToolOutput::new(vec![Part::Text {
                text: format!("now {target}"),
            }]))
        })
    }
}

fn switch_call(id: &str) -> Value {
    json!({"ToolCall": {
        "call_id": id,
        "provider_item_id": null,
        "name": "switch",
        "raw_args": "{}",
        "raw": null,
    }})
}

fn step(items: Vec<Value>, stop_reason: &str) -> Value {
    json!({"items": items, "stop_reason": stop_reason})
}

fn reasoning_inputs(body: &Value) -> Vec<Value> {
    body["input"]
        .as_array()
        .expect("input array")
        .iter()
        .filter(|item| item["type"] == "reasoning")
        .cloned()
        .collect()
}

#[tokio::test]
async fn reasoning_is_dropped_after_a_model_switch_and_replayed_after_switching_back() {
    let reasoning = json!({"Reasoning": {
        "summary": [{"Text": {"text": "think"}}],
        "binding": {"provider": "openai", "model": "model-1", "prefix_epoch": 0, "tools_hash": ""},
        "raw": {"provider": "openai", "model": "model-1", "kind": "fixture", "json": "{}"},
    }});
    let done = json!({"Message": {"role": "Assistant", "parts": [{"Text": {"text": "done"}}]}});
    let fixture: Fixture = serde_json::from_value(json!({
        "steps": [
            step(vec![reasoning, switch_call("call-1")], "ToolUse"),
            step(vec![switch_call("call-2")], "ToolUse"),
            step(vec![done], "EndTurn"),
        ],
    }))
    .expect("fixture");
    let server = MockServer::start(common::mock::openai_replies(&fixture));

    let switch = Arc::new(Switch {
        descriptor: ToolDescriptor::new(
            ToolName::new("switch"),
            String::new(),
            json!({"type": "object"}),
        )
        .with_replay(ReplaySafety::Safe),
        handle: OnceLock::new(),
        calls: AtomicUsize::new(0),
    });
    let provider = Arc::new(
        OpenAiProvider::new("test-key").with_base_url(format!("{}/v1", server.base_url())),
    );
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider])
        .tools(vec![switch.clone()])
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(SessionSpec {
            system_sections: Vec::new(),
            model: model("model-1"),
            system: Vec::new(),
            tools: ToolSelection::Names(vec![ToolName::new("switch")]),
            request: RequestConfig::default(),
            turn: {
                let mut config = TurnConfig::default();
                config.initial_backoff = Duration::ZERO;
                config.max_backoff = Duration::ZERO;
                config
            },
            compaction: CompactionConfig::default(),
            subagents: SubagentConfig::default(),
            metadata: Default::default(),
        })
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    switch.handle.set(handle.clone()).expect("set once");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([1; 16]),
            parts: vec![Part::Text {
                text: "go".to_owned(),
            }],
        })
        .await
        .expect("input");
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
    drop(stream);

    let bodies: Vec<Value> = server
        .requests()
        .iter()
        .map(|request| request.json_body())
        .collect();
    let models: Vec<&str> = bodies
        .iter()
        .map(|body| body["model"].as_str().expect("model"))
        .collect();
    assert_eq!(models, ["model-1", "model-2", "model-1"]);
    assert!(
        reasoning_inputs(&bodies[1]).is_empty(),
        "model-1 reasoning is not replayed to model-2"
    );
    assert_eq!(
        reasoning_inputs(&bodies[2]).len(),
        1,
        "switching back replays it again"
    );
}

/// Appends a system instruction to the session on every call.
struct Append {
    descriptor: ToolDescriptor,
    handle: OnceLock<OwnedSession>,
}

impl Tool for Append {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let mut update = ConfigPatch::default();
            update.id = Some(UpdateId::from_bytes([7; 16]));
            update.append_system = vec![Part::Text {
                text: "answer in French".to_owned(),
            }];
            self.handle
                .get()
                .expect("handle set")
                .update_config(update)
                .await
                .expect("update accepted");
            Ok(ToolOutput::new(vec![Part::Text {
                text: "appended".to_owned(),
            }]))
        })
    }
}

/// One turn: step 1 thinks and calls `append`, step 2 answers.
async fn run_append_turn(provider: Arc<dyn Provider>, model: ModelSpec) {
    let append = Arc::new(Append {
        descriptor: ToolDescriptor::new(
            ToolName::new("append"),
            String::new(),
            json!({"type": "object"}),
        )
        .with_replay(ReplaySafety::Safe),
        handle: OnceLock::new(),
    });
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider])
        .tools(vec![append.clone()])
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(SessionSpec {
            system_sections: Vec::new(),
            model,
            system: vec![Part::Text {
                text: "be helpful".to_owned(),
            }],
            tools: ToolSelection::Names(vec![ToolName::new("append")]),
            request: RequestConfig::default(),
            turn: {
                let mut config = TurnConfig::default();
                config.initial_backoff = Duration::ZERO;
                config.max_backoff = Duration::ZERO;
                config
            },
            compaction: CompactionConfig::default(),
            subagents: SubagentConfig::default(),
            metadata: Default::default(),
        })
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    append.handle.set(handle.clone()).expect("set once");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([1; 16]),
            parts: vec![Part::Text {
                text: "go".to_owned(),
            }],
        })
        .await
        .expect("input");
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
}

fn append_fixture(provider: &str, model: &str) -> Fixture {
    let reasoning = json!({"Reasoning": {
        "summary": [{"Text": {"text": "think"}}],
        "binding": {"provider": provider, "model": model, "prefix_epoch": 0, "tools_hash": ""},
        "raw": {"provider": provider, "model": model, "kind": "fixture", "json": "{}"},
    }});
    let call = json!({"ToolCall": {
        "call_id": "call-1",
        "provider_item_id": null,
        "name": "append",
        "raw_args": "{}",
        "raw": null,
    }});
    let done = json!({"Message": {"role": "Assistant", "parts": [{"Text": {"text": "fini"}}]}});
    serde_json::from_value(json!({
        "steps": [step(vec![reasoning, call], "ToolUse"), step(vec![done], "EndTurn")],
    }))
    .expect("fixture")
}

fn anthropic_model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("anthropic"),
        model: ModelId::new("claude-opus-5-5"),
        context_window: None,
    }
}

#[tokio::test]
async fn anthropic_appends_a_system_message_and_replays_the_old_thinking() {
    let server = MockServer::start(common::mock::anthropic_replies(&append_fixture(
        "anthropic",
        "claude-opus-5-5",
    )));
    let provider = Arc::new(AnthropicProvider::new("test-key").with_base_url(server.base_url()));
    run_append_turn(provider, anthropic_model()).await;

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    let after = &requests[1];
    let body = after.json_body();
    assert_eq!(
        body["messages"][1]["content"][0],
        json!({"type": "thinking", "thinking": "think", "signature": "sig-anthropic"}),
        "the thinking from before the change is replayed"
    );
    assert_eq!(
        body["system"],
        json!([{"type": "text", "text": "be helpful", "cache_control": {"type": "ephemeral"}}])
    );
    let messages = body["messages"].as_array().expect("messages");
    let roles: Vec<&str> = messages
        .iter()
        .map(|message| message["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["user", "assistant", "user", "system"]);
    assert_eq!(messages[3]["content"][0]["text"], "answer in French");
}

#[tokio::test]
async fn anthropic_without_inline_changes_rewrites_the_system_and_drops_the_old_thinking() {
    let server = MockServer::start(common::mock::anthropic_replies(&append_fixture(
        "anthropic",
        "claude-opus-5-5",
    )));
    let provider = Arc::new(
        AnthropicProvider::new("test-key")
            .with_base_url(server.base_url())
            .with_model_profile(
                "claude-opus-5-5",
                AnthropicModelProfile::latest().with_inline_changes(false),
            ),
    );
    run_append_turn(provider, anthropic_model()).await;

    let after = &server.requests()[1];
    assert!(!after.body.contains("sig-anthropic"), "{}", after.body);
    let body = after.json_body();
    let system: Vec<&str> = body["system"]
        .as_array()
        .expect("system")
        .iter()
        .map(|block| block["text"].as_str().unwrap())
        .collect();
    assert_eq!(system, ["be helpful", "answer in French"]);
}

#[tokio::test]
async fn openai_appends_a_developer_message_after_the_tool_output() {
    let server = MockServer::start(common::mock::openai_replies(&append_fixture(
        "openai", "model-1",
    )));
    let provider = Arc::new(
        OpenAiProvider::new("test-key").with_base_url(format!("{}/v1", server.base_url())),
    );
    run_append_turn(provider, model("model-1")).await;

    let body = server.requests()[1].json_body();
    assert_eq!(body["instructions"], "be helpful");
    let input = body["input"].as_array().expect("input");
    let last = input.last().expect("items");
    assert_eq!(input[input.len() - 2]["type"], "function_call_output");
    assert_eq!(
        *last,
        json!({
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "answer in French"}]
        })
    );
}

fn plain_tool(name: &str) -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new(name),
        format!("{name} tool"),
        json!({"type": "object"}),
    )
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly)
}

/// Adds `lookup` and removes itself.
struct Equip {
    descriptor: ToolDescriptor,
    handle: OnceLock<OwnedSession>,
}

impl Tool for Equip {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let mut update = ConfigPatch::default();
            update.id = Some(UpdateId::from_bytes([8; 16]));
            update.add_tools = vec![plain_tool("lookup")];
            update.remove_tools = vec![ToolName::new("equip")];
            self.handle
                .get()
                .expect("handle set")
                .update_config(update)
                .await
                .expect("update accepted");
            Ok(ToolOutput::new(vec![Part::Text {
                text: "equipped".to_owned(),
            }]))
        })
    }
}

struct Lookup(ToolDescriptor);

impl Tool for Lookup {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.0
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async {
            Ok(ToolOutput::new(vec![Part::Text {
                text: "found".to_owned(),
            }]))
        })
    }
}

/// One turn: step 1 thinks and calls `equip`, step 2 calls `lookup`, step 3
/// calls the removed `equip`, step 4 answers. Returns the tool result
/// statuses with their error kinds.
async fn run_equip_turn(
    provider: Arc<dyn Provider>,
    model: ModelSpec,
) -> Vec<(ToolResultStatus, Option<ToolErrorKind>)> {
    let equip = Arc::new(Equip {
        descriptor: plain_tool("equip"),
        handle: OnceLock::new(),
    });
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider])
        .tools(vec![equip.clone(), Arc::new(Lookup(plain_tool("lookup")))])
        .build()
        .await
        .expect("build");
    let handle = asterpath
        .create_session(SessionSpec {
            system_sections: Vec::new(),
            model,
            system: vec![Part::Text {
                text: "be helpful".to_owned(),
            }],
            tools: ToolSelection::Names(vec![ToolName::new("equip")]),
            request: RequestConfig::default(),
            turn: {
                let mut config = TurnConfig::default();
                config.initial_backoff = Duration::ZERO;
                config.max_backoff = Duration::ZERO;
                config
            },
            compaction: CompactionConfig::default(),
            subagents: SubagentConfig::default(),
            metadata: Default::default(),
        })
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    equip.handle.set(handle.clone()).expect("set once");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([1; 16]),
            parts: vec![Part::Text {
                text: "go".to_owned(),
            }],
        })
        .await
        .expect("input");
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
    scan_all(&*asterpath.store(), handle.id())
        .await
        .expect("scan")
        .iter()
        .filter_map(|entry| match decode_entry(entry).expect("decode") {
            SessionEvent::ToolResult { status, error, .. } => {
                Some((status, error.map(|error| error.kind)))
            }
            _ => None,
        })
        .collect()
}

fn equip_fixture(provider: &str, model: &str) -> Fixture {
    let reasoning = json!({"Reasoning": {
        "summary": [{"Text": {"text": "think"}}],
        "binding": {"provider": provider, "model": model, "prefix_epoch": 0, "tools_hash": ""},
        "raw": {"provider": provider, "model": model, "kind": "fixture", "json": "{}"},
    }});
    let call = |id: &str, name: &str| {
        json!({"ToolCall": {
            "call_id": id,
            "provider_item_id": null,
            "name": name,
            "raw_args": "{}",
            "raw": null,
        }})
    };
    let done = json!({"Message": {"role": "Assistant", "parts": [{"Text": {"text": "fini"}}]}});
    serde_json::from_value(json!({
        "steps": [
            step(vec![reasoning, call("call-1", "equip")], "ToolUse"),
            step(vec![call("call-2", "lookup")], "ToolUse"),
            step(vec![call("call-3", "equip")], "ToolUse"),
            step(vec![done], "EndTurn"),
        ],
    }))
    .expect("fixture")
}

fn expected_results() -> Vec<(ToolResultStatus, Option<ToolErrorKind>)> {
    vec![
        (ToolResultStatus::Completed, None),
        (ToolResultStatus::Completed, None),
        (ToolResultStatus::Failed, Some(ToolErrorKind::NotFound)),
    ]
}

fn top_level_tools(body: &Value) -> Vec<&str> {
    body["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn anthropic_renders_tool_changes_inline_and_replays_the_old_thinking() {
    let server = MockServer::start(common::mock::anthropic_replies(&equip_fixture(
        "anthropic",
        "claude-opus-5-5",
    )));
    let provider = Arc::new(AnthropicProvider::new("test-key").with_base_url(server.base_url()));
    let results = run_equip_turn(provider, anthropic_model()).await;
    assert_eq!(results, expected_results());

    let requests = server.requests();
    assert_eq!(requests.len(), 4);
    let after = &requests[1];
    assert!(
        after
            .head
            .to_ascii_lowercase()
            .contains("anthropic-beta: inline-tools-2026-09-15"),
        "{}",
        after.head
    );
    let body = after.json_body();
    assert_eq!(
        top_level_tools(&body),
        ["equip"],
        "top-level tools unchanged"
    );
    assert_eq!(
        body["messages"][1]["content"][0],
        json!({"type": "thinking", "thinking": "think", "signature": "sig-anthropic"}),
        "the thinking from before the change is replayed"
    );
    let system = &body["messages"][3];
    assert_eq!(system["role"], "system");
    assert_eq!(system["content"][0]["type"], "tool_addition");
    assert_eq!(system["content"][0]["tool"]["definition"]["name"], "lookup");
    assert_eq!(system["content"][1]["type"], "tool_removal");
    assert_eq!(
        system["content"][1]["tool"],
        json!({"type": "tool_reference", "name": "equip"})
    );
    let last = requests[3].json_body();
    assert_eq!(top_level_tools(&last), ["equip"]);
    assert!(
        requests[3].body.contains("is not registered"),
        "the removed tool's call and result stay in history"
    );
}

#[tokio::test]
async fn anthropic_without_inline_changes_rewrites_the_tools_and_drops_the_old_thinking() {
    let server = MockServer::start(common::mock::anthropic_replies(&equip_fixture(
        "anthropic",
        "claude-opus-5-5",
    )));
    let provider = Arc::new(
        AnthropicProvider::new("test-key")
            .with_base_url(server.base_url())
            .with_model_profile(
                "claude-opus-5-5",
                AnthropicModelProfile::latest().with_inline_changes(false),
            ),
    );
    let results = run_equip_turn(provider, anthropic_model()).await;
    assert_eq!(results, expected_results());

    let after = &server.requests()[1];
    assert!(!after.head.to_ascii_lowercase().contains("anthropic-beta"));
    assert!(!after.body.contains("sig-anthropic"), "{}", after.body);
    let body = after.json_body();
    assert_eq!(top_level_tools(&body), ["lookup"]);
    assert!(
        body["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .all(|message| message["role"] != "system")
    );
}

#[tokio::test]
async fn openai_sends_the_effective_tools_and_replays_reasoning() {
    let server = MockServer::start(common::mock::openai_replies(&equip_fixture(
        "openai", "model-1",
    )));
    let provider = Arc::new(
        OpenAiProvider::new("test-key").with_base_url(format!("{}/v1", server.base_url())),
    );
    let results = run_equip_turn(provider, model("model-1")).await;
    assert_eq!(results, expected_results());

    let requests = server.requests();
    assert_eq!(requests.len(), 4);
    let body = requests[1].json_body();
    assert_eq!(top_level_tools(&body), ["lookup"]);
    assert_eq!(reasoning_inputs(&body).len(), 1, "reasoning is replayed");
    let last = requests[3].json_body();
    assert!(
        requests[3].body.contains("is not registered"),
        "the removed tool's call and result stay in history"
    );
    assert_eq!(top_level_tools(&last), ["lookup"]);
}
