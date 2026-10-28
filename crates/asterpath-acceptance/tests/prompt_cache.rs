//! Prompt-cache regression (#175): adjacent model requests of one session,
//! rendered by the real adapters, keep the previous request's cacheable
//! prefix ([`asterpath_testing::cache_prefix`]). Where a change is known to
//! break the prefix, the break and its first differing line are asserted.
#![cfg(unix)]

mod common;

use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use asterpath::anthropic::{AnthropicModelProfile, AnthropicProvider};
use asterpath::openai::{OpenAiModelProfile, OpenAiProvider};
use asterpath::{Asterpath, DefaultCompactor, OwnedSession};
use asterpath_protocol::{
    CompactResult, CompactionConfig, ConfigPatch, InputId, ModelId, ModelSpec, Part, Provider,
    ProviderCatalog, ProviderId, ReasoningEffort, ReasoningParams, ReplaySafety, RequestConfig,
    SessionSpec, SessionStore, SideEffect, SubagentConfig, SystemSection, Tool, ToolArgs,
    ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput, ToolSelection, TurnConfig,
    UpdateId, UserInput,
};
use asterpath_testing::cache_prefix::{assert_cache_prefix, prefix_break};
use asterpath_testing::http_mock::{MockServer, Reply};
use asterpath_testing::{Fixture, InMemoryEventLog};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use serde_json::{Value, json};

/// `inline_changes` turns Anthropic's inline instructions, tool changes and
/// effort changes on or off together; OpenAI always renders effort changes
/// in place (`configuration_update`).
#[derive(Clone, Copy, Debug)]
enum Wire {
    Anthropic { inline_changes: bool },
    OpenAi,
}

const WIRES: [Wire; 3] = [
    Wire::Anthropic {
        inline_changes: true,
    },
    Wire::Anthropic {
        inline_changes: false,
    },
    Wire::OpenAi,
];

impl Wire {
    fn provider_id(self) -> &'static str {
        match self {
            Self::Anthropic { .. } => "anthropic",
            Self::OpenAi => "openai",
        }
    }

    fn model_id(self) -> &'static str {
        match self {
            Self::Anthropic { .. } => "claude-opus-5-5",
            Self::OpenAi => "model-1",
        }
    }

    fn model(self) -> ModelSpec {
        ModelSpec {
            provider: ProviderId::new(self.provider_id()),
            model: ModelId::new(self.model_id()),
            // Only consulted by compaction.
            context_window: Some(200_000),
        }
    }

    fn serve(self, steps: Vec<Value>) -> MockServer {
        let fixture: Fixture = serde_json::from_value(json!({ "steps": steps })).expect("fixture");
        let replies: Vec<Reply> = match self {
            Self::Anthropic { .. } => common::mock::anthropic_replies(&fixture),
            Self::OpenAi => common::mock::openai_replies(&fixture),
        };
        MockServer::start(replies)
    }

    fn provider(self, server: &MockServer) -> Arc<dyn Provider> {
        match self {
            Self::Anthropic { inline_changes } => Arc::new(
                AnthropicProvider::new("test-key")
                    .with_base_url(server.base_url())
                    .with_model_profile(
                        self.model_id(),
                        AnthropicModelProfile::latest()
                            .with_inline_changes(inline_changes)
                            .with_inline_effort(inline_changes),
                    ),
            ),
            Self::OpenAi => Arc::new(
                OpenAiProvider::new("test-key")
                    .with_base_url(format!("{}/v1", server.base_url()))
                    .with_model_profile(
                        self.model_id(),
                        OpenAiModelProfile::latest().with_inline_effort(true),
                    ),
            ),
        }
    }

    fn reasoning(self) -> Value {
        let (provider, model) = (self.provider_id(), self.model_id());
        json!({"Reasoning": {
            "summary": [{"Text": {"text": "think"}}],
            "binding": {"provider": provider, "model": model, "prefix_epoch": 0, "tools_hash": ""},
            "raw": {"provider": provider, "model": model, "kind": "fixture", "json": "{}"},
        }})
    }
}

fn call(id: &str, name: &str) -> Value {
    json!({"ToolCall": {
        "call_id": id,
        "provider_item_id": null,
        "name": name,
        "raw_args": "{}",
        "raw": null,
    }})
}

fn answer(text: &str) -> Value {
    json!({"Message": {"role": "Assistant", "parts": [{"Text": {"text": text}}]}})
}

fn calls(items: Vec<Value>) -> Value {
    json!({"items": items, "stop_reason": "ToolUse"})
}

fn ends(text: &str) -> Value {
    json!({"items": [answer(text)], "stop_reason": "EndTurn"})
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

/// Applies the next queued patch to the session on every call.
struct Patcher {
    descriptor: ToolDescriptor,
    handle: OnceLock<OwnedSession>,
    patches: Mutex<VecDeque<ConfigPatch>>,
}

impl Tool for Patcher {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let patch = self
                .patches
                .lock()
                .expect("patches")
                .pop_front()
                .expect("a queued patch");
            self.handle
                .get()
                .expect("handle set")
                .update_config(patch)
                .await
                .expect("update accepted");
            Ok(ToolOutput::new(vec![Part::Text {
                text: "patched".to_owned(),
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

struct Setup {
    store: Arc<dyn SessionStore>,
    tools: ToolSelection,
    patches: Vec<ConfigPatch>,
    compactor: bool,
    sections: Vec<SystemSection>,
    request: RequestConfig,
}

impl Setup {
    fn new(tools: &[&str]) -> Self {
        Self {
            store: Arc::new(InMemoryEventLog::new()),
            tools: ToolSelection::Names(tools.iter().map(|name| ToolName::new(*name)).collect()),
            patches: Vec::new(),
            compactor: false,
            request: RequestConfig::default(),
            sections: Vec::new(),
        }
    }
}

/// A facade on `server` whose registry holds `patch` and `lookup`, in the
/// given order.
async fn build(
    wire: Wire,
    server: &MockServer,
    setup: &Setup,
    lookup_first: bool,
) -> (Asterpath, Arc<Patcher>) {
    let provider = wire.provider(server);
    let patcher = Arc::new(Patcher {
        descriptor: plain_tool("patch"),
        handle: OnceLock::new(),
        patches: Mutex::new(setup.patches.clone().into()),
    });
    let lookup: Arc<dyn Tool> = Arc::new(Lookup(plain_tool("lookup")));
    let mut tools: Vec<Arc<dyn Tool>> = vec![patcher.clone(), lookup];
    if lookup_first {
        tools.reverse();
    }
    let mut builder = Asterpath::builder()
        .store(Arc::clone(&setup.store))
        .providers(vec![Arc::clone(&provider)])
        .tools(tools);
    if setup.compactor {
        let catalog = ProviderCatalog::new(vec![provider]).expect("catalog");
        builder = builder.compactor(Arc::new(DefaultCompactor::new(Arc::new(catalog))));
    }
    (builder.build().await.expect("build"), patcher)
}

async fn create(wire: Wire, asterpath: &Asterpath, setup: &Setup) -> OwnedSession {
    asterpath
        .create_session(SessionSpec {
            model: wire.model(),
            system: vec![Part::Text {
                text: "be helpful".to_owned(),
            }],
            system_sections: setup.sections.clone(),
            tools: setup.tools.clone(),
            request: setup.request.clone(),
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
        .expect("claim")
}

async fn turn(handle: &OwnedSession, input: u8, text: &str) {
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([input; 16]),
            parts: vec![Part::Text {
                text: text.to_owned(),
            }],
        })
        .await
        .expect("input");
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
}

fn bodies(server: &MockServer) -> Vec<String> {
    server
        .requests()
        .into_iter()
        .map(|request| request.body)
        .collect()
}

/// One session, one turn per input, every request on `server`.
async fn session(wire: Wire, server: &MockServer, setup: &Setup, inputs: &[&str]) -> Vec<String> {
    let (asterpath, patcher) = build(wire, server, setup, false).await;
    let handle = create(wire, &asterpath, setup).await;
    patcher.handle.set(handle.clone()).expect("set once");
    for (index, text) in inputs.iter().enumerate() {
        turn(&handle, index as u8 + 1, text).await;
    }
    bodies(server)
}

fn assert_each_extends(wire: Wire, bodies: &[String]) {
    for (index, pair) in bodies.windows(2).enumerate() {
        if let Some(line) = prefix_break(&pair[0], &pair[1]) {
            panic!(
                "{wire:?}: request {} breaks request {index}'s prefix at {line}",
                index + 1
            );
        }
    }
}

#[tokio::test]
async fn turns_with_tool_calls_and_replayed_reasoning_extend_the_prefix() {
    for wire in WIRES {
        let server = wire.serve(vec![
            calls(vec![wire.reasoning(), call("call-1", "lookup")]),
            ends("one"),
            calls(vec![call("call-2", "lookup"), call("call-3", "lookup")]),
            ends("two"),
        ]);
        let bodies = session(wire, &server, &Setup::new(&["lookup"]), &["go", "again"]).await;
        assert_eq!(bodies.len(), 4, "{wire:?}");
        assert_each_extends(wire, &bodies);
    }
}

fn append_patch() -> ConfigPatch {
    let mut patch = ConfigPatch::default();
    patch.id = Some(UpdateId::from_bytes([7; 16]));
    patch.append_system = vec![Part::Text {
        text: "answer in French".to_owned(),
    }];
    patch
}

#[tokio::test]
async fn appended_instructions_extend_the_prefix_unless_they_rewrite_the_system() {
    for wire in WIRES {
        let server = wire.serve(vec![
            calls(vec![wire.reasoning(), call("call-1", "patch")]),
            ends("fini"),
        ]);
        let mut setup = Setup::new(&["patch"]);
        setup.patches = vec![append_patch()];
        let bodies = session(wire, &server, &setup, &["go"]).await;
        let broken = prefix_break(&bodies[0], &bodies[1]);
        match wire {
            // Without inline changes the instruction joins the top-level system.
            Wire::Anthropic {
                inline_changes: false,
            } => assert_eq!(
                broken.as_deref(),
                Some(r#"{"text":"answer in French","type":"text"}"#)
            ),
            _ => assert_eq!(broken, None, "{wire:?}"),
        }
    }
}

fn date_section(day: &str) -> SystemSection {
    SystemSection::new(
        "date",
        vec![Part::Text {
            text: format!("Today is {day}."),
        }],
    )
}

#[tokio::test]
async fn a_keyed_refresh_extends_the_prefix_unless_it_rewrites_the_system() {
    for wire in WIRES {
        let server = wire.serve(vec![
            calls(vec![wire.reasoning(), call("call-1", "patch")]),
            calls(vec![call("call-2", "patch")]),
            ends("fini"),
        ]);
        let mut setup = Setup::new(&["patch"]);
        setup.sections = vec![date_section("Monday")];
        setup.patches = [9, 10]
            .into_iter()
            .zip(["Tuesday", "Wednesday"])
            .map(|(id, day)| {
                let mut patch = ConfigPatch::default();
                patch.id = Some(UpdateId::from_bytes([id; 16]));
                patch.system_sections = vec![date_section(day)];
                patch
            })
            .collect();
        let bodies = session(wire, &server, &setup, &["go"]).await;
        assert_eq!(bodies.len(), 3, "{wire:?}");
        assert!(bodies[0].contains("Today is Monday."), "{wire:?}");
        match wire {
            // Without inline changes the refresh joins the top-level system.
            Wire::Anthropic {
                inline_changes: false,
            } => {
                let line = prefix_break(&bodies[0], &bodies[1]).expect("rewrites the system");
                assert!(line.contains("Instruction section `date`"), "{line}");
            }
            _ => assert_each_extends(wire, &bodies),
        }
    }
}

fn equip_patch() -> ConfigPatch {
    let mut patch = ConfigPatch::default();
    patch.id = Some(UpdateId::from_bytes([8; 16]));
    patch.add_tools = vec![plain_tool("lookup")];
    patch.remove_tools = vec![ToolName::new("patch")];
    patch
}

#[tokio::test]
async fn tool_changes_extend_the_prefix_only_when_rendered_inline() {
    for wire in WIRES {
        let server = wire.serve(vec![
            calls(vec![wire.reasoning(), call("call-1", "patch")]),
            calls(vec![call("call-2", "lookup")]),
            ends("fini"),
        ]);
        let mut setup = Setup::new(&["patch"]);
        setup.patches = vec![equip_patch()];
        let bodies = session(wire, &server, &setup, &["go"]).await;
        assert_eq!(bodies.len(), 3, "{wire:?}");
        let broken = prefix_break(&bodies[0], &bodies[1]);
        match wire {
            Wire::Anthropic {
                inline_changes: true,
            } => assert_eq!(broken, None),
            // The effective tools replace the top-level `tools`.
            _ => {
                let line = broken.unwrap_or_else(|| panic!("{wire:?} keeps the prefix"));
                assert!(line.contains(r#""lookup tool""#), "{wire:?}: {line}");
            }
        }
        assert_each_extends(wire, &bodies[1..]);
    }
}

fn with_effort(effort: ReasoningEffort) -> RequestConfig {
    let mut request = RequestConfig::default();
    request.reasoning = Some({
        let mut params = ReasoningParams::default();
        params.effort = Some(effort);
        params
    });
    request
}

#[tokio::test]
async fn effort_changes_extend_the_prefix_only_when_rendered_in_place() {
    for wire in WIRES {
        let server = wire.serve(vec![calls(vec![call("call-1", "patch")]), ends("fini")]);
        let mut setup = Setup::new(&["patch"]);
        setup.request = with_effort(ReasoningEffort::High);
        let mut patch = ConfigPatch::default();
        patch.request = Some(with_effort(ReasoningEffort::Low));
        setup.patches = vec![patch];
        let bodies = session(wire, &server, &setup, &["go"]).await;
        assert_eq!(bodies.len(), 2, "{wire:?}");
        let broken = prefix_break(&bodies[0], &bodies[1]);
        match wire {
            // The effective effort replaces the top-level `output_config`.
            Wire::Anthropic {
                inline_changes: false,
            } => assert_eq!(
                broken.as_deref(),
                Some(r#"{"output_config":{"effort":"low"}}"#)
            ),
            _ => assert_eq!(broken, None, "{wire:?}"),
        }
    }
}

#[tokio::test]
async fn output_limits_keep_the_prefix_and_reasoning_params_break_it() {
    let mut limited = RequestConfig::default();
    limited.max_output_tokens = NonZeroU32::new(1234).expect("non-zero");
    let mut reasoning = limited.clone();
    reasoning.reasoning = Some({
        let mut params = ReasoningParams::default();
        params.effort = Some(ReasoningEffort::High);
        params
    });
    let patches = [limited, reasoning].map(|request| {
        let mut patch = ConfigPatch::default();
        patch.request = Some(request);
        patch
    });
    for wire in WIRES {
        let server = wire.serve(vec![
            calls(vec![call("call-1", "patch")]),
            calls(vec![call("call-2", "patch")]),
            ends("fini"),
        ]);
        let mut setup = Setup::new(&["patch"]);
        setup.patches = patches.to_vec();
        let bodies = session(wire, &server, &setup, &["go"]).await;
        assert_eq!(bodies.len(), 3, "{wire:?}");
        assert_cache_prefix(&bodies[0], &bodies[1]);
        // Adding reasoning has no baseline effort to keep in place.
        let expected = match wire {
            Wire::Anthropic { .. } => r#"{"thinking":{"type":"adaptive"}}"#,
            Wire::OpenAi => r#"{"reasoning":{"effort":"high"}}"#,
        };
        assert_eq!(
            prefix_break(&bodies[1], &bodies[2]).as_deref(),
            Some(expected),
            "{wire:?}"
        );
    }
}

#[tokio::test]
async fn the_second_request_after_compaction_extends_the_first() {
    for wire in WIRES {
        let server = wire.serve(vec![
            calls(vec![wire.reasoning(), call("call-1", "lookup")]),
            ends("one"),
            ends("the summary"),
            calls(vec![call("call-2", "lookup")]),
            ends("two"),
        ]);
        let mut setup = Setup::new(&["lookup"]);
        setup.compactor = true;
        let (asterpath, patcher) = build(wire, &server, &setup, false).await;
        let handle = create(wire, &asterpath, &setup).await;
        patcher.handle.set(handle.clone()).expect("set once");
        turn(&handle, 1, "go").await;
        let result = handle.compact_now().await.expect("compact_now");
        assert!(matches!(result, CompactResult::Applied(_)), "{result:?}");
        turn(&handle, 2, "again").await;

        let bodies = bodies(&server);
        assert_eq!(bodies.len(), 5, "{wire:?}");
        assert!(
            bodies[2].contains("the conversation segment"),
            "summary call"
        );
        assert_cache_prefix(&bodies[0], &bodies[1]);
        assert_cache_prefix(&bodies[3], &bodies[4]);
    }
}

#[tokio::test]
async fn the_first_request_after_resuming_extends_the_last_one_before() {
    for wire in WIRES {
        let mut setup = Setup::new(&[]);
        setup.tools = ToolSelection::All;
        let before = wire.serve(vec![
            calls(vec![wire.reasoning(), call("call-1", "lookup")]),
            ends("one"),
        ]);
        let (asterpath, _) = build(wire, &before, &setup, false).await;
        let handle = create(wire, &asterpath, &setup).await;
        turn(&handle, 1, "go").await;
        let session = handle.id();
        drop(handle);
        asterpath.shutdown().await.expect("shutdown");

        // A new process over the same log, its registry built in the other order.
        let after = wire.serve(vec![ends("two")]);
        let (asterpath, _) = build(wire, &after, &setup, true).await;
        let handle = asterpath
            .open_session(session)
            .await
            .expect("open")
            .claim()
            .await
            .expect("claim");
        turn(&handle, 2, "again").await;

        let last = bodies(&before).pop().expect("requests before");
        let first = bodies(&after).into_iter().next().expect("a request after");
        assert_cache_prefix(&last, &first);
    }
}
