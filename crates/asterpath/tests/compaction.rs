//! End-to-end compaction: automatic threshold, forced retry, manual, mapping
//! and crash recovery (C1–C16, minus fork which belongs to #47).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use asterpath::DefaultCompactor;
use asterpath::protocol::{
    AgentError, AgentEvent, CompactionAbortReason, CompactionConfig, CompactionTrigger, Compactor,
    InputTokens, Item, JsonString, ModelId, ModelSpec, OutputTokens, Parallelism, Part, Provider,
    ProviderCatalog, ProviderError, ProviderErrorKind, ProviderId, ReplaySafety, RequestConfig,
    Role, SessionEvent, SessionSpec, SideEffect, StopReason, SubagentConfig, Tool, ToolArgs,
    ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput, ToolSelection, TurnConfig,
    TurnEndReason, Usage, UserInput, decode_entry, dedup, encode_entry,
};
use asterpath::protocol::{InputId, LogEntry, LogId, LogQuery, SessionId, SessionStore};
use asterpath::{Asterpath, SessionHandle};
use asterpath_testing::ScriptedProvider;
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-compaction-{tag}-{}-{}.db",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_file(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}-wal", path.display()));
    let _ = fs::remove_file(format!("{}-shm", path.display()));
    let _ = fs::remove_file(format!("{}.lock", path.display()));
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn assistant(value: &str) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }
}

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached: input,
            cache_read: 0,
            cache_write: 0,
        },
        output: OutputTokens {
            total: output,
            reasoning: 0,
        },
    }
}

fn tool_call(call: &str, name: &str) -> Item {
    Item::ToolCall {
        call_id: call.to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new("{}"),
        raw: None,
    }
}

fn descriptor(name: &str) -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new(name),
        String::new(),
        serde_json::json!({"type": "object"}),
    )
    .with_parallelism(Parallelism::Parallel)
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly)
}

struct BigTool {
    descriptor: ToolDescriptor,
    output: String,
}

impl BigTool {
    fn new(name: &str, output: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            descriptor: descriptor(name),
            output: output.into(),
        })
    }
}

impl Tool for BigTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        let output = self.output.clone();
        Box::pin(async move { Ok(ToolOutput::new(vec![text(&output)])) })
    }
}

fn turn() -> TurnConfig {
    let mut config = TurnConfig::default();
    config.initial_backoff = Duration::ZERO;
    config.max_backoff = Duration::ZERO;
    config
}

fn spec(window: Option<u32>, compaction: CompactionConfig) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: window,
        },
        system: vec![text("be helpful")],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: turn(),
        compaction,
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

fn compaction_config(max_auto: u32) -> CompactionConfig {
    let mut config = CompactionConfig::default();
    config.max_auto_compactions_per_turn = max_auto;
    config
}

fn compactor(provider: &Arc<ScriptedProvider>) -> Arc<dyn Compactor> {
    let provider = Arc::clone(provider) as Arc<dyn Provider>;
    Arc::new(DefaultCompactor::new(Arc::new(
        ProviderCatalog::new(vec![provider]).expect("catalog"),
    )))
}

async fn facade(
    db: &Path,
    provider: Arc<ScriptedProvider>,
    compactor: Option<Arc<dyn Compactor>>,
) -> Asterpath {
    let mut builder = Asterpath::builder()
        .database(db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![BigTool::new("big", big()) as Arc<dyn Tool>]);
    if let Some(compactor) = compactor {
        builder = builder.compactor(compactor);
    }
    builder.build().await.expect("facade builds")
}

async fn scan(log: Arc<dyn SessionStore>, session: SessionId) -> Vec<LogEntry> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan succeeds"));
    }
    entries
}

fn kinds(entries: &[LogEntry]) -> Vec<&'static str> {
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodable").kind())
        .collect()
}

fn count(entries: &[LogEntry], kind: &str) -> usize {
    kinds(entries)
        .into_iter()
        .filter(|item| *item == kind)
        .count()
}

async fn drive_all(handle: &SessionHandle) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive succeeds"));
    }
    events
}

fn input(id: u8, message: &str) -> UserInput {
    UserInput {
        id: InputId::from_bytes([id; 16]),
        parts: vec![text(message)],
    }
}

fn big() -> String {
    "tool output ".repeat(80)
}

/// Script for a two-step tool turn: ToolUse then EndTurn.
fn two_step_script(provider: ScriptedProvider, call: &str, tool: &str) -> ScriptedProvider {
    provider
        .respond(
            vec![tool_call(call, tool)],
            StopReason::ToolUse,
            usage(30, 5),
        )
        .respond(vec![assistant("done")], StopReason::EndTurn, usage(30, 5))
}

fn image_and_reasoning() -> Vec<Item> {
    vec![
        Item::Reasoning {
            summary: vec![text("hidden reasoning")],
            binding: asterpath::protocol::ReasoningBinding {
                provider: ProviderId::new("p"),
                model: ModelId::new("model-1"),
                prefix_epoch: 0,
                tools_hash: String::new(),
            },
            raw: asterpath::protocol::RawProviderData {
                provider: ProviderId::new("p"),
                model: None,
                kind: "raw".to_owned(),
                json: JsonString::new("{\"raw\":true}"),
            },
        },
        Item::Message {
            role: Role::Assistant,
            parts: vec![
                text("step one"),
                Part::Image {
                    media_type: "image/png".to_owned(),
                    source: asterpath::protocol::ImageSource::Url {
                        url: "https://example.invalid/p.png".to_owned(),
                    },
                },
            ],
            raw: None,
        },
    ]
}

#[tokio::test]
async fn c1_c14_automatic_compaction_projects_a_text_only_summary() {
    let db = temp_db("c1");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .with_capabilities({
                let mut capabilities = asterpath::protocol::ProviderCapabilities::default();
                capabilities.image_input = true;
                capabilities
            })
            .respond(
                {
                    let mut items = image_and_reasoning();
                    items.push(tool_call("c1", "big"));
                    items
                },
                StopReason::ToolUse,
                usage(30, 5),
            )
            .respond(
                vec![tool_call("c2", "big")],
                StopReason::ToolUse,
                usage(800, 5),
            )
            .respond(
                vec![assistant("the summary")],
                StopReason::EndTurn,
                usage(5, 5),
            )
            .respond(vec![assistant("done")], StopReason::EndTurn, usage(5, 5)),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(1)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        }
    )));
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, AgentEvent::Idle { .. }))
    );

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionApplied"), 1);
    assert_eq!(count(&entries, "CompactionAborted"), 0);
    // Raw records survive the projection.
    assert!(count(&entries, "InputRecorded") == 1);
    assert_eq!(count(&entries, "AssistantMessage"), 3);

    // The summary request is the third model call: text-only, no tools, no
    // reasoning and no non-text parts.
    let requests = provider.requests();
    let summary_request = &requests[2];
    assert!(summary_request.tools.is_empty());
    assert_eq!(
        summary_request.tool_choice,
        asterpath::protocol::ToolChoice::None
    );
    assert!(summary_request.reasoning.is_none());
    assert!(summary_request.sampling.is_none());
    assert_eq!(summary_request.max_output_tokens, 2048);
    let rendered = format!("{:?}", summary_request.items);
    assert!(!rendered.contains("hidden reasoning"));
    assert!(!rendered.contains("image/png"));
    assert!(!rendered.contains("raw\":true"));
    assert!(rendered.contains("tool call big"));

    // The next step sees the summary plus what was kept, not the raw prefix.
    let next = &requests[3];
    assert!(
        next.items
            .iter()
            .any(|item| matches!(item, Item::Compaction { parts }
            if matches!(&parts[0], Part::Text { text } if text == "the summary")))
    );
    let next_text = format!("{:?}", next.items);
    assert!(!next_text.contains("step one"));

    cleanup(&db);
}

#[tokio::test]
async fn c2_forced_compaction_with_one_complete_step_has_nothing_to_do() {
    let db = temp_db("c2");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("c1", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .fail(ProviderError::new(
                ProviderErrorKind::ContextExceeded,
                "too long",
            )),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::ContextExhausted,
            ..
        }
    )));
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 0);
    assert_eq!(count(&entries, "CompactionApplied"), 0);
    cleanup(&db);
}

#[tokio::test]
async fn c5_forced_compaction_retries_the_same_step_once() {
    let db = temp_db("c5");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("c1", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .respond(
                vec![tool_call("c2", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .fail(ProviderError::new(
                ProviderErrorKind::ContextExceeded,
                "too long",
            ))
            .respond(
                vec![assistant("the summary")],
                StopReason::EndTurn,
                usage(5, 5),
            )
            .respond(vec![assistant("done")], StopReason::EndTurn, usage(5, 5)),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        }
    )));

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "StepStarted"), 3);
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionApplied"), 1);
    assert_eq!(count(&entries, "CompactionAborted"), 0);
    // One abort, then the compaction, then the retried message: no step was
    // opened or committed in between.
    let sequence = kinds(&entries);
    let start = sequence
        .iter()
        .rposition(|kind| *kind == "StepStarted")
        .expect("third step");
    assert_eq!(
        &sequence[start..],
        [
            "StepStarted",
            "ModelCallAborted",
            "CompactionStarted",
            "CompactionApplied",
            "AssistantMessage",
            "StepCommitted",
            "TurnCommitted",
        ]
    );

    // The retry request contains the applied summary.
    let requests = provider.requests();
    let retry = requests.last().expect("retry request");
    assert!(
        retry
            .items
            .iter()
            .any(|item| matches!(item, Item::Compaction { .. }))
    );
    cleanup(&db);
}

#[tokio::test]
async fn c5_second_context_exhaustion_ends_the_turn() {
    let db = temp_db("c5b");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("c1", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .respond(
                vec![tool_call("c2", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .fail(ProviderError::new(
                ProviderErrorKind::ContextExceeded,
                "too long",
            ))
            .respond(
                vec![assistant("the summary")],
                StopReason::EndTurn,
                usage(5, 5),
            )
            .fail(ProviderError::new(
                ProviderErrorKind::ContextExceeded,
                "still too long",
            )),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::ContextExhausted,
            ..
        }
    )));
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "StepStarted"), 3);
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionApplied"), 1);
    cleanup(&db);
}

#[tokio::test]
async fn c6_stop_reason_exhaustion_does_not_compress() {
    let db = temp_db("c6");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![assistant("partial")],
        StopReason::ContextWindowExceeded,
        usage(90, 5),
    ));
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(1)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::ContextExhausted,
            ..
        }
    )));
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 0);
    assert_eq!(count(&entries, "CompactionApplied"), 0);
    cleanup(&db);
}

#[tokio::test]
async fn c7_manual_compaction_is_idle_only_and_turn_free() {
    let db = temp_db("c7");
    let provider = Arc::new(
        two_step_script(ScriptedProvider::new("p"), "c1", "big").respond(
            vec![assistant("the summary")],
            StopReason::EndTurn,
            usage(5, 5),
        ),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    drive_all(&handle).await;
    let before = scan(asterpath.store(), handle.id()).await;

    let result = handle.compact_now().await.expect("compact_now");
    assert!(matches!(
        result,
        asterpath::protocol::CompactResult::Applied(_)
    ));
    let after = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&after, "CompactionStarted"), 1);
    assert_eq!(count(&after, "CompactionApplied"), 1);
    assert_eq!(count(&after, "TurnStarted"), count(&before, "TurnStarted"));
    assert_eq!(
        count(&after, "TurnCommitted"),
        count(&before, "TurnCommitted")
    );

    // TurnOpen is Busy and leaves the log unchanged.
    let session = handle.id();
    let entry = encode_entry(
        session,
        None,
        &SessionEvent::TurnStarted {
            turn_id: asterpath::protocol::TurnId::from_bytes([9; 16]),
        },
    )
    .expect("encode");
    asterpath.store().append(entry).await.expect("append turn");
    let open = scan(asterpath.store(), handle.id()).await;
    let error = handle.compact_now().await.expect_err("busy");
    assert!(matches!(
        error,
        asterpath::AsterError::Agent(AgentError::Busy { .. })
    ));
    let unchanged = scan(asterpath.store(), handle.id()).await;
    assert_eq!(open.len(), unchanged.len());
    cleanup(&db);
}

#[tokio::test]
async fn c9_at_most_one_automatic_compaction_per_turn() {
    let db = temp_db("c9");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("c1", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .respond(
                vec![tool_call("c2", "big")],
                StopReason::ToolUse,
                usage(800, 5),
            )
            .respond(
                vec![assistant("the summary")],
                StopReason::EndTurn,
                usage(5, 5),
            )
            .respond(
                vec![tool_call("c3", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .respond(vec![assistant("done")], StopReason::EndTurn, usage(5, 5)),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(1)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    drive_all(&handle).await;
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionApplied"), 1);
    cleanup(&db);
}

#[tokio::test]
async fn c13_missing_window_disables_every_path() {
    let db = temp_db("c13");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("c1", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .fail(ProviderError::new(
                ProviderErrorKind::ContextExceeded,
                "too long",
            ))
            .respond(
                vec![assistant("the summary")],
                StopReason::EndTurn,
                usage(5, 5),
            ),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(None, compaction_config(1)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::ContextExhausted,
            ..
        }
    )));

    let result = handle.compact_now().await.expect("manual");
    assert_eq!(result, asterpath::protocol::CompactResult::NotConfigured);
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 0);
    assert_eq!(count(&entries, "CompactionApplied"), 0);
    cleanup(&db);
}

#[tokio::test]
async fn c15_forced_summary_failure_maps_to_compaction_failed() {
    let db = temp_db("c15a");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("c1", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .respond(
                vec![tool_call("c2", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .fail(ProviderError::new(
                ProviderErrorKind::ContextExceeded,
                "too long",
            ))
            .fail(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                "summary rejected",
            )),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::CompactionFailed,
            ..
        }
    )));
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionAborted"), 1);
    let aborted = entries
        .iter()
        .find_map(|entry| match decode_entry(entry).ok()? {
            SessionEvent::CompactionAborted { reason } => Some(reason),
            _ => None,
        });
    assert_eq!(aborted, Some(CompactionAbortReason::SummaryFailed));
    cleanup(&db);
}

#[tokio::test]
async fn c15_forced_insufficient_maps_to_context_exhausted() {
    let db = temp_db("c15b");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("c1", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .respond(
                vec![tool_call("c2", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .fail(ProviderError::new(
                ProviderErrorKind::ContextExceeded,
                "too long",
            ))
            .respond(
                vec![assistant("the summary")],
                StopReason::EndTurn,
                usage(5, 5),
            ),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(90), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::ContextExhausted,
            ..
        }
    )));
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionAborted"), 1);
    let aborted = entries
        .iter()
        .find_map(|entry| match decode_entry(entry).ok()? {
            SessionEvent::CompactionAborted { reason } => Some(reason),
            _ => None,
        });
    assert_eq!(aborted, Some(CompactionAbortReason::Insufficient));
    cleanup(&db);
}

#[tokio::test]
async fn c16_without_a_compactor_nothing_is_written() {
    let db = temp_db("c16");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("c1", "big")],
                StopReason::ToolUse,
                usage(30, 5),
            )
            .fail(ProviderError::new(
                ProviderErrorKind::ContextExceeded,
                "too long",
            )),
    );
    let asterpath = facade(&db, Arc::clone(&provider), None).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(1)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let events = drive_all(&handle).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::ContextExhausted,
            ..
        }
    )));
    let result = handle.compact_now().await.expect("manual");
    assert_eq!(result, asterpath::protocol::CompactResult::NotConfigured);
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 0);
    assert_eq!(count(&entries, "CompactionApplied"), 0);
    cleanup(&db);
}

async fn seed_open_compaction(
    asterpath: &Asterpath,
    session: SessionId,
    trigger: CompactionTrigger,
) -> LogId {
    let entries = scan(asterpath.store(), session).await;
    let frontier = entries.last().expect("frontier").log_id;
    let key = dedup::compaction(frontier);
    let started = SessionEvent::CompactionStarted {
        trigger,
        replaces_from: entries.first().expect("first").log_id,
        caused_by: frontier,
        turn: None,
    };
    asterpath
        .store()
        .append(encode_entry(session, Some(key), &started).expect("encode"))
        .await
        .expect("append started");
    frontier
}

#[tokio::test]
async fn c3_open_compaction_is_redone_on_recovery() {
    let db = temp_db("c3");
    let provider = Arc::new(
        two_step_script(ScriptedProvider::new("p"), "c1", "big").respond(
            vec![assistant("the summary")],
            StopReason::EndTurn,
            usage(5, 5),
        ),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    drive_all(&handle).await;
    seed_open_compaction(&asterpath, handle.id(), CompactionTrigger::Manual).await;

    // A later drive redoes the summary and closes the intent.
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive succeeds");
    }
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionApplied"), 1);
    assert_eq!(count(&entries, "CompactionAborted"), 0);
    assert!(matches!(
        handle.state().await.expect("state"),
        asterpath::protocol::SessionState::Idle
    ));
    cleanup(&db);
}

#[tokio::test]
async fn c3_failed_redo_closes_with_an_abort() {
    let db = temp_db("c3f");
    let provider = Arc::new(
        two_step_script(ScriptedProvider::new("p"), "c1", "big").fail(ProviderError::new(
            ProviderErrorKind::InvalidRequest,
            "summary rejected",
        )),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    drive_all(&handle).await;
    seed_open_compaction(&asterpath, handle.id(), CompactionTrigger::Manual).await;

    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive succeeds");
    }
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionAborted"), 1);
    assert_eq!(count(&entries, "CompactionApplied"), 0);
    assert!(matches!(
        handle.state().await.expect("state"),
        asterpath::protocol::SessionState::Idle
    ));
    cleanup(&db);
}

#[tokio::test]
async fn c3_without_a_compactor_the_intent_stays_open_and_idle() {
    let db = temp_db("c3n");
    let provider = Arc::new(two_step_script(ScriptedProvider::new("p"), "c1", "big"));
    let asterpath = facade(&db, Arc::clone(&provider), None).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    drive_all(&handle).await;
    seed_open_compaction(&asterpath, handle.id(), CompactionTrigger::Manual).await;

    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive succeeds");
    }
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionApplied"), 0);
    assert_eq!(count(&entries, "CompactionAborted"), 0);
    assert!(matches!(
        handle.state().await.expect("state"),
        asterpath::protocol::SessionState::Idle
    ));
    cleanup(&db);
}

#[tokio::test]
async fn c4_committed_compaction_is_not_repeated() {
    let db = temp_db("c4");
    let provider = Arc::new(
        two_step_script(ScriptedProvider::new("p"), "c1", "big").respond(
            vec![assistant("the summary")],
            StopReason::EndTurn,
            usage(5, 5),
        ),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    drive_all(&handle).await;
    handle.compact_now().await.expect("manual compaction");
    let before = provider.requests().len();

    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive succeeds");
    }
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    assert_eq!(count(&entries, "CompactionApplied"), 1);
    assert_eq!(
        provider.requests().len(),
        before,
        "no summary call is replayed"
    );
    cleanup(&db);
}

#[tokio::test]
async fn c10_same_frontier_append_is_idempotent() {
    let db = temp_db("c10");
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = facade(&db, Arc::clone(&provider), None).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    let entries = scan(asterpath.store(), handle.id()).await;
    let frontier = entries.last().expect("frontier").log_id;
    let key = dedup::compaction(frontier);
    let started = SessionEvent::CompactionStarted {
        trigger: CompactionTrigger::Manual,
        replaces_from: entries.first().expect("first").log_id,
        caused_by: frontier,
        turn: None,
    };
    let first = asterpath
        .store()
        .append(encode_entry(handle.id(), Some(key.clone()), &started).expect("encode"))
        .await
        .expect("append");
    let second = asterpath
        .store()
        .append(encode_entry(handle.id(), Some(key), &started).expect("encode"))
        .await
        .expect("append");
    assert_eq!(first, second);
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(count(&entries, "CompactionStarted"), 1);
    cleanup(&db);
}

#[tokio::test]
async fn c11_summary_usage_is_recorded_on_the_applied_event_only() {
    let db = temp_db("c11");
    let provider = Arc::new(
        two_step_script(ScriptedProvider::new("p"), "c1", "big").respond(
            vec![assistant("the summary")],
            StopReason::EndTurn,
            usage(11, 7),
        ),
    );
    let asterpath = facade(&db, Arc::clone(&provider), Some(compactor(&provider))).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    handle.submit_input(input(1, "go")).await.expect("input");
    drive_all(&handle).await;
    handle.compact_now().await.expect("manual");

    let entries = scan(asterpath.store(), handle.id()).await;
    let mut summary_usage = None;
    let mut last_usage = None;
    for entry in &entries {
        match decode_entry(entry).expect("decodable") {
            SessionEvent::CompactionApplied { usage, .. } => summary_usage = Some(usage),
            SessionEvent::AssistantMessage { usage, .. } => last_usage = Some(usage),
            _ => {}
        }
    }
    assert_eq!(summary_usage, Some(usage(11, 7)));
    assert_eq!(last_usage, Some(usage(30, 5)));
    // The start's caused_by anchors turn attribution; it precedes the applied
    // event and sits outside any step's usage.
    assert!(count(&entries, "StepCommitted") >= 2);
    cleanup(&db);
}

#[tokio::test]
async fn manual_states_are_results_not_errors() {
    let db = temp_db("manual-states");
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = facade(&db, Arc::clone(&provider), None).await;
    let handle = asterpath
        .create_session(spec(Some(1000), compaction_config(0)))
        .await
        .expect("session");
    assert_eq!(
        handle.compact_now().await.expect("nothing to do"),
        asterpath::protocol::CompactResult::NotConfigured
    );
    cleanup(&db);
}

#[cfg(feature = "anthropic")]
fn anthropic_sse(blocks: &[&str], deltas: &[(usize, &str)]) -> String {
    let mut body = String::from(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg\",\"usage\":{\"input_tokens\":1}}}\n\n",
    );
    for (index, block) in blocks.iter().enumerate() {
        body.push_str(&format!(
            "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":{index},\"content_block\":{block}}}\n\n"
        ));
        for (_, delta) in deltas.iter().filter(|(at, _)| *at == index) {
            body.push_str(&format!(
                "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":{index},\"delta\":{delta}}}\n\n"
            ));
        }
        body.push_str(&format!(
            "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":{index}}}\n\n"
        ));
    }
    body.push_str("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n");
    body.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
    body
}

/// #104: the summary request has an empty system, so replaying the session's
/// Anthropic thinking into it would be rejected. A tool-free session makes the
/// tools fingerprint match, so only the compactor's text view keeps it out.
#[cfg(feature = "anthropic")]
#[tokio::test]
async fn anthropic_thinking_never_reaches_the_summary_request() {
    use asterpath_testing::http_mock::{MockServer, Reply};

    let thinking = anthropic_sse(
        &[
            r#"{"type":"thinking","thinking":"","signature":""}"#,
            r#"{"type":"redacted_thinking","data":"opaque=="}"#,
            r#"{"type":"text","text":""}"#,
        ],
        &[
            (0, r#"{"type":"thinking_delta","thinking":"why"}"#),
            (0, r#"{"type":"signature_delta","signature":"sig-123"}"#),
            (2, r#"{"type":"text_delta","text":"first answer"}"#),
        ],
    );
    let plain = |reply: &str| {
        anthropic_sse(
            &[r#"{"type":"text","text":""}"#],
            &[(0, &format!(r#"{{"type":"text_delta","text":"{reply}"}}"#))],
        )
    };
    let server = MockServer::start(vec![
        Reply::sse(thinking),
        Reply::sse(plain("second answer")),
        Reply::sse(plain("the summary")),
    ]);
    let provider: Arc<dyn Provider> = Arc::new(
        asterpath::anthropic::AnthropicProvider::new("test-key").with_base_url(server.base_url()),
    );
    let compactor: Arc<dyn Compactor> = Arc::new(DefaultCompactor::new(Arc::new(
        ProviderCatalog::new(vec![Arc::clone(&provider)]).expect("catalog"),
    )));
    let db = temp_db("anthropic-thinking");
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider])
        .compactor(compactor)
        .build()
        .await
        .expect("facade builds");
    let mut spec = spec(Some(200_000), compaction_config(0));
    spec.model.provider = ProviderId::new("anthropic");
    spec.model.model = ModelId::new("claude-test");
    spec.tools = ToolSelection::Names(Vec::new());
    let handle = asterpath.create_session(spec).await.expect("session");
    handle.submit_input(input(1, "first")).await.expect("input");
    drive_all(&handle).await;
    handle
        .submit_input(input(2, "second"))
        .await
        .expect("input");
    drive_all(&handle).await;
    assert!(matches!(
        handle.compact_now().await.expect("compact_now"),
        asterpath::protocol::CompactResult::Applied(_)
    ));

    let block_types = |body: &serde_json::Value| -> Vec<String> {
        body["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .filter_map(|message| message["content"].as_array())
            .flatten()
            .filter_map(|block| block["type"].as_str().map(str::to_owned))
            .collect()
    };
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    // The normal session request still replays both reasoning blocks.
    let session = block_types(&requests[1].json_body());
    assert!(session.iter().any(|kind| kind == "thinking"), "{session:?}");
    assert!(
        session.iter().any(|kind| kind == "redacted_thinking"),
        "{session:?}"
    );
    // The summary request has no system and carries neither block.
    let summary_body = requests[2].json_body();
    assert!(summary_body.get("system").is_none(), "{summary_body}");
    let summary = block_types(&summary_body);
    assert!(
        !summary
            .iter()
            .any(|kind| kind == "thinking" || kind == "redacted_thinking"),
        "{summary:?}"
    );
    cleanup(&db);
}
