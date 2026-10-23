//! Failpoint decorators: canonical names, per-instance occurrence counting,
//! domain validation and full delegation on a miss.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use asterpath_protocol::{EventLog, LogQuery, SessionId};
use asterpath_protocol::{
    JsonString, Parallelism, Part, Provenance, Provider, ReplaySafety, SessionEvent, SideEffect,
    Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput,
};
use asterpath_testing::{
    Failpoint, FailpointEventLog, FailpointTool, Fixture, InMemoryEventLog, Point, ScriptedProvider,
};
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

const SESSION: SessionId = SessionId::from_bytes([1; 16]);

fn point(value: &str) -> Point {
    Point::parse(value).expect("canonical point")
}

#[test]
fn canonical_point_names_round_trip() {
    for (value, expected) in [
        ("append:before_commit", Point::AppendBeforeCommit),
        ("append:after_commit", Point::AppendAfterCommit),
        ("tool:before_effect", Point::ToolBeforeEffect),
        ("tool:after_effect", Point::ToolAfterEffect),
    ] {
        assert_eq!(point(value), expected);
        assert_eq!(expected.as_str(), value);
    }
    assert!(Point::parse("append:nowhere").is_none());
    assert!(Point::parse("clock:advance").is_none(), "not a decorator");
}

async fn append(log: &dyn EventLog, text: &str) {
    let event = SessionEvent::InputRecorded {
        input: vec![Part::Text {
            text: text.to_owned(),
        }],
    };
    let entry = asterpath_protocol::encode_entry(SESSION, None, &event).expect("encode");
    log.append(entry).await.expect("append");
}

#[tokio::test]
#[should_panic(expected = "failpoint append:before_commit")]
async fn event_log_panics_at_the_configured_occurrence() {
    let inner = Arc::new(InMemoryEventLog::new());
    let log = FailpointEventLog::new(
        inner.clone(),
        Failpoint::panic(Point::AppendBeforeCommit)
            .with_occurrence(std::num::NonZeroUsize::new(2).expect("non-zero")),
    );
    append(&log, "one").await;
    assert_eq!(scan(&inner).await.len(), 1);
    append(&log, "two").await;
}

#[tokio::test]
async fn event_log_delegates_everything_until_the_occurrence() {
    let inner = Arc::new(InMemoryEventLog::new());
    let log = FailpointEventLog::new(
        inner.clone(),
        Failpoint::panic(Point::AppendAfterCommit)
            .with_occurrence(std::num::NonZeroUsize::new(3).expect("non-zero")),
    );
    append(&log, "one").await;
    append(&log, "two").await;
    assert_eq!(scan(&inner).await.len(), 2);
    let frontier = log.frontier(SESSION).await.expect("frontier");
    assert_eq!(
        frontier,
        Some(inner.frontier(SESSION).await.expect("inner").unwrap())
    );
}

#[tokio::test]
#[should_panic(expected = "tool:before_effect is not an append failpoint")]
async fn event_log_rejects_a_tool_domain_point() {
    let inner: Arc<dyn EventLog> = Arc::new(InMemoryEventLog::new());
    let _ = FailpointEventLog::new(inner, Failpoint::panic(Point::ToolBeforeEffect));
}

struct CounterTool {
    descriptor: ToolDescriptor,
    calls: Arc<AtomicUsize>,
}

impl CounterTool {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self {
            descriptor: ToolDescriptor {
                name: ToolName::new("counter"),
                description: "counts".to_owned(),
                schema: serde_json::json!({"type": "object"}),
                parallelism: Parallelism::Parallel,
                replay: ReplaySafety::Safe,
                side_effect: SideEffect::ReadOnly,
                provenance: Provenance::InProcess,
                timeout: None,
                max_output_bytes: None,
            },
            calls,
        }
    }
}

impl Tool for CounterTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ToolOutput::new(vec![Part::Text {
                text: "ok".to_owned(),
            }]))
        })
    }
}

fn context() -> ToolContext {
    ToolContext::new(
        SESSION,
        asterpath_protocol::TurnId::from_bytes([2; 16]),
        asterpath_protocol::StepId::from_bytes([3; 16]),
        asterpath_protocol::ToolCallId::from_bytes([4; 16]),
        asterpath_protocol::CancelToken::new(),
    )
}

#[tokio::test]
#[should_panic(expected = "failpoint tool:after_effect")]
async fn tool_failpoint_fires_after_the_effect() {
    let calls = Arc::new(AtomicUsize::new(0));
    let tool = FailpointTool::new(
        Arc::new(CounterTool::new(Arc::clone(&calls))),
        Failpoint::panic(Point::ToolAfterEffect),
    );
    let _ = tool.invoke(JsonString::new("{}"), context()).await;
    unreachable!("the failpoint halts the call");
}

#[tokio::test]
async fn tool_failpoint_delegates_and_passes_the_descriptor_through() {
    let calls = Arc::new(AtomicUsize::new(0));
    let tool = FailpointTool::new(
        Arc::new(CounterTool::new(Arc::clone(&calls))),
        Failpoint::panic(Point::ToolAfterEffect)
            .with_occurrence(std::num::NonZeroUsize::new(2).expect("non-zero")),
    );
    assert_eq!(tool.descriptor().name.as_str(), "counter");
    let result = tool.invoke(JsonString::new("{}"), context()).await;
    assert!(result.is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[should_panic(expected = "append:after_commit is not a tool failpoint")]
async fn tool_rejects_an_append_domain_point() {
    let tool: Arc<dyn Tool> = Arc::new(CounterTool::new(Arc::new(AtomicUsize::new(0))));
    let _ = FailpointTool::new(tool, Failpoint::panic(Point::AppendAfterCommit));
}

#[tokio::test]
#[should_panic(expected = "failpoint append:after_commit")]
async fn nested_decorators_count_per_instance() {
    // The inner decorator is the first hit; the outer one only counts to two.
    let inner = Arc::new(InMemoryEventLog::new());
    let inner = FailpointEventLog::new(inner, Failpoint::panic(Point::AppendAfterCommit));
    let log = FailpointEventLog::new(
        Arc::new(inner),
        Failpoint::panic(Point::AppendAfterCommit)
            .with_occurrence(std::num::NonZeroUsize::new(2).expect("non-zero")),
    );
    append(&log, "one").await;
}

#[test]
fn fixture_parses_and_replays_after_a_committed_prefix() {
    let fixture: Fixture = serde_json::from_str(
        r#"{
            "provider": "p",
            "model": "m",
            "tools": ["echo"],
            "steps": [
                {"items": [{"ToolCall": {"call_id": "c1", "provider_item_id": null,
                    "name": "echo", "raw_args": "{}", "raw": null}}],
                 "stop_reason": "ToolUse"},
                {"items": [{"Message": {"role": "Assistant",
                    "parts": [{"Text": {"text": "done"}}]}}],
                 "stop_reason": "EndTurn"}
            ]
        }"#,
    )
    .expect("fixture");
    assert_eq!(fixture.provider, "p");
    assert_eq!(fixture.tools, vec![ToolName::new("echo")]);

    let provider = ScriptedProvider::from_fixture(&fixture);
    provider.skip(1);
    let request = asterpath_protocol::ModelRequest::new(
        asterpath_protocol::ModelSpec {
            provider: asterpath_protocol::ProviderId::new("p"),
            model: asterpath_protocol::ModelId::new("m"),
            context_window: None,
        },
        0,
        vec![],
        vec![],
        vec![],
        &asterpath_protocol::RequestConfig::default(),
    );
    let events: Vec<_> = futures::executor::block_on(async {
        use futures_util::StreamExt;
        provider.stream(request).collect::<Vec<_>>().await
    });
    assert!(matches!(
        events.last(),
        Some(Ok(asterpath_protocol::StreamEvent::ResponseEnded {
            stop_reason: asterpath_protocol::StopReason::EndTurn,
            ..
        }))
    ));
    assert_eq!(provider.requests().len(), 1);
}

async fn scan(log: &Arc<InMemoryEventLog>) -> Vec<asterpath_protocol::LogEntry> {
    let mut stream = log.scan(LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan"));
    }
    entries
}
