//! Summary-call behaviour of the default compactor.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crate::compaction::DefaultCompactor;
use asterpath_protocol::{CacheLifetime, ModelRequest, RequestConfig};
use asterpath_protocol::{
    CompactSummary, CompactionConfig, CompactionJob, Compactor, InputTokens, Item, ModelId,
    ModelSpec, OutputTokens, Part, Provider, ProviderCatalog, ProviderError, ProviderErrorKind,
    ProviderId, Role, StopReason, ToolChoice, TurnConfig, Usage,
};
use asterpath_protocol::{LogId, LogIdRange};
use asterpath_testing::ScriptedProvider;

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn message(value: &str) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }
}

fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new("m"),
        context_window: Some(1000),
    }
}

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached: input,
            cache_read: 0,
            cache_write: 0,
            cache_write_1h: 0,
        },
        output: OutputTokens {
            total: output,
            reasoning: 0,
        },
    }
}

fn turn() -> TurnConfig {
    let mut config = TurnConfig::default();
    config.max_attempts = NonZeroU32::new(2).expect("non-zero");
    config.initial_backoff = Duration::ZERO;
    config.max_backoff = Duration::ZERO;
    config
}

fn job(usable: u32, retained: u64) -> CompactionJob {
    CompactionJob {
        first_kept: LogId::from_i64(5),
        replaces: LogIdRange {
            first: LogId::from_i64(1),
            last: LogId::from_i64(3),
        },
        model: model(),
        summary_input: vec![
            Item::Message {
                role: Role::User,
                parts: vec![text("hello")],
                raw: None,
            },
            Item::ToolCall {
                call_id: "c1".to_owned(),
                provider_item_id: None,
                name: asterpath_protocol::ToolName::new("shell"),
                raw_args: asterpath_protocol::JsonString::new("{}"),
                raw: None,
            },
        ],
        max_summary_tokens: 2048,
        usable_window: usable,
        retained_tokens: retained,
        turn: turn(),
        session: None,
        prefix: None,
    }
}

fn compactor(provider: Arc<ScriptedProvider>) -> DefaultCompactor {
    DefaultCompactor::new(Arc::new(
        ProviderCatalog::new(vec![provider as Arc<dyn Provider>]).expect("catalog"),
    ))
}

#[tokio::test]
async fn summary_request_is_independent_of_the_session_request() {
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![message("the summary")],
        StopReason::EndTurn,
        usage(7, 3),
    ));
    let result = compactor(Arc::clone(&provider))
        .compact(job(1000, 0))
        .await
        .expect("summary succeeds");
    let CompactSummary::Applied(outcome) = result else {
        panic!("expected applied");
    };
    assert_eq!(outcome.first_kept, LogId::from_i64(5));
    assert_eq!(outcome.replaces.last, LogId::from_i64(3));
    assert_eq!(outcome.model, model());
    assert_eq!(outcome.usage, usage(7, 3));
    assert_eq!(outcome.summary, vec![text("the summary")]);
    assert!(outcome.raw.is_none());

    let request = provider.requests().pop().expect("one request");
    assert!(request.tools.is_empty());
    assert_eq!(request.tool_choice, ToolChoice::None);
    assert!(!request.disable_parallel_tool_use);
    assert!(request.sampling.is_none());
    assert!(request.reasoning.is_none());
    assert!(request.system.is_empty());
    assert_eq!(request.max_output_tokens, 2048);
    // Fixed prompt first, then the supplied text view.
    assert!(matches!(
        &request.items[0],
        Item::Message { role: Role::User, parts, .. }
            if matches!(&parts[0], Part::Text { text } if text.contains("Summarize"))
    ));
    assert_eq!(request.items.len(), 3);
}

#[tokio::test]
async fn overlong_summary_is_insufficient() {
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![message(&"x".repeat(300))],
        StopReason::EndTurn,
        usage(1, 100),
    ));
    let result = compactor(provider)
        .compact(job(100, 99))
        .await
        .expect("call succeeds");
    assert_eq!(result, CompactSummary::Insufficient);
}

#[tokio::test]
async fn retryable_failure_is_retried_then_applied() {
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .fail(ProviderError::new(
                ProviderErrorKind::Transport,
                "temporary",
            ))
            .respond(vec![message("ok")], StopReason::EndTurn, usage(1, 1)),
    );
    let result = compactor(Arc::clone(&provider))
        .compact(job(1000, 0))
        .await
        .expect("second attempt succeeds");
    assert!(matches!(result, CompactSummary::Applied(_)));
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn rejected_failure_is_not_retried() {
    let provider = Arc::new(ScriptedProvider::new("p").fail(ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        "bad request",
    )));
    let error = compactor(Arc::clone(&provider))
        .compact(job(1000, 0))
        .await
        .expect_err("failure surfaces");
    assert!(matches!(
        error,
        asterpath_protocol::CompactionError::Provider(error)
            if error.kind == ProviderErrorKind::InvalidRequest
    ));
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn missing_summary_provider_is_an_error_without_fallback() {
    let compactor = DefaultCompactor::new(Arc::new(ProviderCatalog::default()));
    let mut job = job(1000, 0);
    job.model = ModelSpec {
        provider: ProviderId::new("absent"),
        model: ModelId::new("other"),
        context_window: Some(1000),
    };
    let error = compactor.compact(job).await.expect_err("no fallback");
    assert!(matches!(
        error,
        asterpath_protocol::CompactionError::Provider(_)
    ));
}

#[test]
fn compaction_config_defaults_match_the_spec() {
    let config = CompactionConfig::default();
    assert_eq!(config.target_ratio, 0.5);
    assert_eq!(config.max_auto_compactions_per_turn, 1);
    assert_eq!(config.max_summary_tokens, 2048);
    assert!(config.summary_model.is_none());
}

fn session_request() -> ModelRequest {
    let mut config = RequestConfig::default();
    config.cache_ttl.messages = CacheLifetime::OneHour;
    ModelRequest::new(
        model(),
        2,
        vec![text("be helpful")],
        vec![Item::Message {
            role: Role::User,
            parts: vec![text("hello")],
            raw: None,
        }],
        Vec::new(),
        Vec::new(),
        &config,
    )
}

fn summarized(provider: &ScriptedProvider) -> ModelRequest {
    provider.requests().pop().expect("one request")
}

#[tokio::test]
async fn same_model_summary_forks_the_session_request() {
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![message("the summary")],
        StopReason::EndTurn,
        usage(1, 1),
    ));
    let mut job = job(1000, 0);
    job.prefix = Some(session_request());
    compactor(Arc::clone(&provider))
        .compact(job)
        .await
        .expect("summary succeeds");

    let session = session_request();
    let request = summarized(&provider);
    assert_eq!(request.system, session.system);
    assert_eq!(request.prefix_epoch, 2);
    assert_eq!(request.cache_ttl, session.cache_ttl);
    assert_eq!(request.tool_choice, session.tool_choice);
    assert_eq!(request.items[..1], session.items[..]);
    assert_eq!(request.items.len(), 2);
    assert!(matches!(
        &request.items[1],
        Item::Message { role: Role::User, parts, .. }
            if matches!(&parts[0], Part::Text { text } if text.contains("Summarize"))
    ));
}

#[tokio::test]
async fn another_summary_model_falls_back_to_an_independent_request() {
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![message("the summary")],
        StopReason::EndTurn,
        usage(1, 1),
    ));
    let mut job = job(1000, 0);
    job.model.model = ModelId::new("summarizer");
    job.prefix = Some(session_request());
    compactor(Arc::clone(&provider))
        .compact(job)
        .await
        .expect("summary succeeds");

    let request = summarized(&provider);
    assert!(request.system.is_empty());
    assert_eq!(request.cache_ttl, RequestConfig::default().cache_ttl);
    assert_eq!(request.tool_choice, ToolChoice::None);
    assert_eq!(request.items.len(), 3);
}
