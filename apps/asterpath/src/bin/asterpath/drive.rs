//! Driving a session to the end of its turn, with or without an operator
//! approval prompt, and reading the result back from the log.

use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use asterpath::{Asterpath, OwnedSession, SessionHandle};
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalRequest, Item, LogEntry, Part, ProjectedOrigin,
    Provenance, ProviderErrorInfo, Role, SessionEvent, SessionId, SessionStore, TurnEndReason,
    Usage, decode_entry, project, scan_all,
};
use futures_util::StreamExt;

use crate::{CliError, CliResult, aster_error};

#[derive(Default)]
pub struct DriveOutcome {
    /// Whether anything was driven.
    pub ran: bool,
    pub idle: bool,
    pub reason: Option<TurnEndReason>,
    /// The usage of the last committed turn.
    pub usage: Option<Usage>,
    /// A stream-json input error that stopped serving early; reported after the
    /// result line.
    pub error: Option<CliError>,
}

impl DriveOutcome {
    /// Fold one live event into the outcome.
    pub fn observe(&mut self, event: &AgentEvent) {
        match event {
            AgentEvent::TurnCommitted { reason, usage, .. } => {
                self.reason = Some(reason.clone());
                self.usage = Some(*usage);
            }
            AgentEvent::Idle { .. } => self.idle = true,
            _ => {}
        }
    }
}

/// Who watches a drive and answers its approvals.
pub trait Operator {
    /// A live event to show.
    fn show(&mut self, _event: &AgentEvent) {}

    /// The answer to an approval request; `None` cancels the turn.
    fn decide(
        &mut self,
        _request: &ApprovalRequest,
    ) -> impl Future<Output = Option<ApprovalDecision>> {
        std::future::ready(Some(ApprovalDecision::Deny { reason: None }))
    }

    /// Resolves when the operator interrupts the turn; never by default.
    fn interrupted(&mut self) -> impl Future<Output = ()> {
        std::future::pending()
    }
}

/// No operator: nothing is shown and every approval is denied (with no
/// endpoint registered, the framework denies first).
pub struct Headless;

impl Operator for Headless {}

/// How often a drive looks for child sessions' approval requests.
const CHILD_APPROVAL_POLL: Duration = Duration::from_millis(100);

/// Drive to `TurnCommitted` or `Idle`; an interrupt cancels the turn and
/// the drive runs on to its end. There is no exit code for "stopped at a wait".
/// Child sessions' requests never reach this stream, so the tree's pending
/// list is polled for them.
pub async fn drive(
    asterpath: &Asterpath,
    handle: &OwnedSession,
    operator: &mut impl Operator,
) -> CliResult<DriveOutcome> {
    let mut outcome = DriveOutcome {
        ran: true,
        ..DriveOutcome::default()
    };
    let gate = asterpath.approval_gate();
    let mut asked = HashSet::new();
    let mut poll = tokio::time::interval(CHILD_APPROVAL_POLL);
    let mut stream = handle.drive();
    loop {
        let event = tokio::select! {
            event = stream.next() => event,
            () = operator.interrupted() => {
                handle.cancel().await.map_err(aster_error)?;
                continue;
            }
            _ = poll.tick() => {
                for request in gate.pending_in(handle.owner().root) {
                    if request.session != handle.id()
                        && asked.insert((request.session, request.tool_call_id))
                    {
                        let child = asterpath
                            .open_session(request.session)
                            .await
                            .map_err(aster_error)?;
                        respond(&child, handle, operator, &request).await?;
                    }
                }
                continue;
            }
        };
        let Some(event) = event else { break };
        let event = event.map_err(|error| CliError::run(format!("drive failed: {error}")))?;
        outcome.observe(&event);
        if let AgentEvent::ApprovalRequested(request) = &event {
            respond(handle, handle, operator, request).await?;
        } else {
            operator.show(&event);
        }
    }
    Ok(outcome)
}

/// Ask the operator and record the answer in `session`; no answer cancels
/// the driven turn.
async fn respond(
    session: &SessionHandle,
    driven: &OwnedSession,
    operator: &mut impl Operator,
    request: &ApprovalRequest,
) -> CliResult<()> {
    match operator.decide(request).await {
        Some(decision) => session.approve(request.tool_call_id, decision).await,
        None => driven.cancel().await,
    }
    .map_err(aster_error)
}

/// Print an approval request's fields and the question.
pub fn ask<W: Write>(out: &mut W, request: &ApprovalRequest) {
    let _ = writeln!(out, "tool: {}", request.tool.as_str());
    let _ = writeln!(out, "args: {}", request.args.as_str());
    let _ = writeln!(out, "replay: {:?}", request.replay);
    let _ = writeln!(out, "side_effect: {:?}", request.side_effect);
    let _ = writeln!(out, "provenance: {:?}", request.provenance);
    let _ = write!(out, "allow? [y/N] ");
    let _ = out.flush();
}

/// Anything other than yes (including EOF, `None`) denies.
pub fn answer(line: Option<&str>) -> ApprovalDecision {
    match line.map(|line| line.trim().to_ascii_lowercase()).as_deref() {
        Some("y" | "yes") => ApprovalDecision::Allow,
        _ => ApprovalDecision::Deny { reason: None },
    }
}

/// Map a turn's end to the process result: only `Completed` (or no turn) is 0.
pub fn turn_result(reason: Option<&TurnEndReason>, entries: &[LogEntry]) -> CliResult<()> {
    match reason {
        None | Some(TurnEndReason::Completed) => Ok(()),
        Some(TurnEndReason::Failed) => Err(CliError::run(match last_provider_error(entries) {
            Some(error) => format!("turn ended with Failed: {}", describe(&error)),
            None => "turn ended with Failed".to_owned(),
        })),
        Some(other) => Err(CliError::run(format!("turn ended with {other:?}"))),
    }
}

/// The error of the last model-call abort in the latest turn, if it recorded one.
fn last_provider_error(entries: &[LogEntry]) -> Option<ProviderErrorInfo> {
    entries
        .iter()
        .rev()
        .find_map(|entry| match decode_entry(entry).ok()? {
            SessionEvent::ModelCallAborted { error, .. } => Some(error),
            // Stop at the turn boundary: an older turn's error is not this failure.
            SessionEvent::TurnStarted { .. } => Some(None),
            _ => None,
        })
        .flatten()
}

fn describe(error: &ProviderErrorInfo) -> String {
    match &error.request_id {
        Some(id) => format!("{:?}: {} (request_id {id})", error.kind, error.message),
        None => format!("{:?}: {}", error.kind, error.message),
    }
}

pub async fn scan(
    event_log: Arc<dyn SessionStore>,
    session: SessionId,
) -> Result<Vec<LogEntry>, String> {
    scan_all(&*event_log, session)
        .await
        .map_err(|error| error.to_string())
}

/// The latest turn's last assistant message in the model-visible projection;
/// `None` when that turn wrote no text.
pub fn final_assistant_text(entries: &[LogEntry]) -> Option<String> {
    let turn_start = entries
        .iter()
        .rev()
        .find(|entry| matches!(decode_entry(entry), Ok(SessionEvent::TurnStarted { .. })))?
        .log_id;
    // shortcut: text compacted away within the same turn reads as `None`; read
    // the raw step events if applications need it.
    let items = project(entries)
        .ok()?
        .into_iter()
        .filter(|projected| matches!(projected.origin, ProjectedOrigin::Log(id) if id > turn_start))
        .flat_map(|projected| projected.items);
    for item in items.rev() {
        if let Item::Message {
            role: Role::Assistant,
            parts,
            ..
        } = item
        {
            let text: String = parts
                .iter()
                .filter_map(|part| match part {
                    Part::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

/// The latest turn's structured result from the output tool: the arguments
/// of its first output call (ADR-0034).
pub fn output_result(entries: &[LogEntry]) -> Option<String> {
    let mut first = None;
    for entry in entries.iter().rev() {
        match decode_entry(entry).ok()? {
            SessionEvent::TurnStarted { .. } => break,
            SessionEvent::ToolCallRequested {
                raw_args,
                provenance: Provenance::Output,
                ..
            } => first = Some(raw_args.as_str().to_owned()),
            _ => {}
        }
    }
    first
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use asterpath::{Asterpath, AsterpathBuilder, AsterpathConfig};
    use asterpath_app::approval::ApprovalModes;
    use asterpath_protocol::{InputId, SandboxPolicy, UserInput};
    use std::io::Cursor;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use asterpath_protocol::ToolCallId;
    use asterpath_protocol::{
        ApprovalPolicy, ApprovalRequest, ApprovalVerdict, CompactionConfig, JsonString, ModelId,
        ModelSpec, Parallelism, Provenance, Provider, ProviderId, ReplaySafety, RequestConfig,
        SessionEvent, SessionSpec, SideEffect, StopReason, SubagentConfig, Tool, ToolArgs,
        ToolConfig, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput, ToolSelection,
        TurnConfig, Usage,
    };
    use asterpath_testing::ScriptedProvider;
    use futures_util::future::BoxFuture;

    fn request() -> ApprovalRequest {
        ApprovalRequest::new(
            SessionId::from_bytes([1; 16]),
            asterpath_protocol::TurnId::from_bytes([2; 16]),
            asterpath_protocol::StepId::from_bytes([3; 16]),
            ToolCallId::from_bytes([4; 16]),
            ToolName::new("echo"),
            JsonString::new(r#"{"q":"hi"}"#),
            ReplaySafety::Safe,
            SideEffect::ReadOnly,
            Provenance::InProcess,
        )
    }

    #[test]
    fn prompt_prints_every_approval_field_and_maps_the_answer() {
        let mut rendered = Vec::new();
        ask(&mut rendered, &request());
        let rendered = String::from_utf8(rendered).unwrap();
        for field in [
            "tool: echo",
            r#"args: {"q":"hi"}"#,
            "replay: Safe",
            "side_effect: ReadOnly",
            "provenance: InProcess",
            "allow? [y/N]",
        ] {
            assert!(rendered.contains(field), "missing {field}: {rendered}");
        }
        for (line, expected) in [
            (Some("y"), ApprovalDecision::Allow),
            (Some(" YES "), ApprovalDecision::Allow),
            (Some("n"), ApprovalDecision::Deny { reason: None }),
            (Some(""), ApprovalDecision::Deny { reason: None }),
            (None, ApprovalDecision::Deny { reason: None }),
        ] {
            assert_eq!(answer(line), expected);
        }
    }

    /// Answers approvals from scripted lines and records the questions.
    struct PromptIo {
        reader: Cursor<&'static [u8]>,
        writer: Vec<u8>,
    }

    impl Operator for PromptIo {
        async fn decide(&mut self, request: &ApprovalRequest) -> Option<ApprovalDecision> {
            use std::io::BufRead;
            ask(&mut self.writer, request);
            let mut line = String::new();
            let read = self.reader.read_line(&mut line).is_ok_and(|n| n > 0);
            Some(answer(read.then_some(line.as_str())))
        }
    }

    struct EchoTool {
        descriptor: ToolDescriptor,
        calls: Arc<AtomicUsize>,
    }

    impl Tool for EchoTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }

        fn invoke(
            &self,
            _args: ToolArgs,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(ToolOutput::new(vec![Part::Text { text: "ok".into() }])) })
        }
    }

    fn echo_tool(calls: Arc<AtomicUsize>) -> Arc<dyn Tool> {
        Arc::new(EchoTool {
            descriptor: ToolDescriptor::new(
                ToolName::new("echo"),
                String::new(),
                serde_json::json!({"type": "object"}),
            )
            .with_parallelism(Parallelism::Parallel)
            .with_replay(ReplaySafety::Safe)
            .with_side_effect(SideEffect::ReadOnly),
            calls,
        })
    }

    struct AlwaysAsk;

    impl ApprovalPolicy for AlwaysAsk {
        fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
            ApprovalVerdict::Ask
        }
    }

    fn tool_call() -> Item {
        Item::ToolCall {
            call_id: "call-1".to_owned(),
            provider_item_id: None,
            name: ToolName::new("echo"),
            raw_args: JsonString::new(r#"{"q":"hi"}"#),
            raw: None,
        }
    }

    fn final_message() -> Item {
        Item::Message {
            role: Role::Assistant,
            parts: vec![Part::Text {
                text: "done".into(),
            }],
            raw: None,
        }
    }

    fn spec() -> SessionSpec {
        SessionSpec {
            system_sections: Vec::new(),
            model: ModelSpec {
                provider: ProviderId::new("p"),
                model: ModelId::new("model-1"),
                context_window: None,
            },
            system: Vec::new(),
            tools: ToolSelection::All,
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
        }
    }

    fn temp_db(tag: &str) -> PathBuf {
        use std::sync::atomic::AtomicU64;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "asterpath-app-approval-{tag}-{}-{}.db",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn cleanup(db: &Path) {
        let _ = std::fs::remove_file(db);
        let _ = std::fs::remove_file(format!("{}-wal", db.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db.display()));
        let _ = std::fs::remove_file(format!("{}.lock", db.display()));
    }

    fn script() -> Arc<ScriptedProvider> {
        Arc::new(
            ScriptedProvider::new("p")
                .respond(vec![tool_call()], StopReason::ToolUse, Usage::default())
                .respond(vec![final_message()], StopReason::EndTurn, Usage::default()),
        )
    }

    async fn build(db: &Path, provider: Arc<ScriptedProvider>, tool: Arc<dyn Tool>) -> Asterpath {
        Asterpath::builder()
            .database(db)
            .providers(vec![provider as Arc<dyn Provider>])
            .tools(vec![tool])
            .tool(ToolConfig::default())
            .approval(Arc::new(AlwaysAsk))
            .build()
            .await
            .expect("build")
    }

    #[tokio::test]
    async fn allow_prompt_is_delivered_and_executes_the_tool() {
        let db = temp_db("allow");
        let calls = Arc::new(AtomicUsize::new(0));
        let asterpath = build(&db, script(), echo_tool(Arc::clone(&calls))).await;
        let handle = asterpath
            .create_session(spec())
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        let _endpoint = asterpath.approval_gate().register(handle.id());
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([1; 16]),
                parts: vec![Part::Text { text: "go".into() }],
            })
            .await
            .unwrap();

        let mut io = PromptIo {
            reader: Cursor::new(b"y\n"),
            writer: Vec::new(),
        };
        let outcome = drive(&asterpath, &handle, &mut io).await.expect("drive");

        assert!(matches!(outcome.reason, Some(TurnEndReason::Completed)));
        assert!(!outcome.idle);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8(io.writer).unwrap().contains("tool: echo"));
        assert_eq!(signal_count(&asterpath, handle.id()).await, 1);

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }

    #[tokio::test]
    async fn final_text_is_none_when_the_latest_turn_wrote_no_text() {
        let db = temp_db("final-text");
        let provider = Arc::new(
            ScriptedProvider::new("p")
                .respond(vec![final_message()], StopReason::EndTurn, Usage::default())
                .respond(vec![tool_call()], StopReason::ToolUse, Usage::default()),
        );
        let asterpath = build(&db, provider, echo_tool(Arc::new(AtomicUsize::new(0)))).await;
        let mut spec = spec();
        spec.turn.max_steps_per_turn = Some(1);
        let handle = asterpath
            .create_session(spec)
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        for (byte, text) in [(1, "first"), (2, "second")] {
            handle
                .submit_input(UserInput {
                    id: InputId::from_bytes([byte; 16]),
                    parts: vec![Part::Text { text: text.into() }],
                })
                .await
                .unwrap();
            let outcome = drive(&asterpath, &handle, &mut Headless)
                .await
                .expect("drive");
            let entries = scan(asterpath.store(), handle.id()).await.unwrap();
            if byte == 1 {
                assert_eq!(final_assistant_text(&entries).as_deref(), Some("done"));
            } else {
                assert!(matches!(
                    outcome.reason,
                    Some(TurnEndReason::MaxStepsExceeded)
                ));
                assert_eq!(final_assistant_text(&entries), None);
            }
        }

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }

    #[tokio::test]
    async fn deny_prompt_records_denied_and_skips_the_tool() {
        let db = temp_db("deny");
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(ScriptedProvider::new("p").respond(
            vec![tool_call()],
            StopReason::ToolUse,
            Usage::default(),
        ));
        let asterpath = build(&db, provider, echo_tool(Arc::clone(&calls))).await;
        let handle = asterpath
            .create_session(spec())
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        let _endpoint = asterpath.approval_gate().register(handle.id());
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([1; 16]),
                parts: vec![Part::Text { text: "go".into() }],
            })
            .await
            .unwrap();

        let mut io = PromptIo {
            reader: Cursor::new(b"n\n"),
            writer: Vec::new(),
        };
        let outcome = drive(&asterpath, &handle, &mut io).await.expect("drive");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let values = tool_result_values(&asterpath, handle.id()).await;
        assert_eq!(values.len(), 1);
        assert!(values[0].to_lowercase().contains("denied"), "{values:?}");
        assert!(outcome.reason.is_some() || outcome.idle);

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }

    #[tokio::test]
    async fn recorded_decision_is_reused_on_restart_without_prompting() {
        let db = temp_db("reuse");
        let calls = Arc::new(AtomicUsize::new(0));

        // Phase 1: reach the approval, record Allow, then drop the drive.
        let asterpath = build(&db, script(), echo_tool(Arc::clone(&calls))).await;
        let handle = asterpath
            .create_session(spec())
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        let _endpoint = asterpath.approval_gate().register(handle.id());
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([1; 16]),
                parts: vec![Part::Text { text: "go".into() }],
            })
            .await
            .unwrap();
        let mut stream = handle.drive();
        let request = loop {
            let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .expect("approval requested")
                .expect("event")
                .expect("no error");
            if let AgentEvent::ApprovalRequested(request) = event {
                break request;
            }
        };
        handle
            .approve(request.tool_call_id, ApprovalDecision::Allow)
            .await
            .unwrap();
        drop(stream);
        assert_eq!(calls.load(Ordering::SeqCst), 0, "not executed before crash");
        let session = handle.id();
        drop(handle);
        asterpath.shutdown().await.unwrap();

        // Phase 2: restart with a provider that can only answer the final step.
        let provider = Arc::new(ScriptedProvider::new("p").respond(
            vec![final_message()],
            StopReason::EndTurn,
            Usage::default(),
        ));
        let asterpath = build(&db, provider, echo_tool(Arc::clone(&calls))).await;
        let handle = asterpath
            .open_session(session)
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        let _endpoint = asterpath.approval_gate().register(handle.id());

        let mut io = PromptIo {
            reader: Cursor::new(b"n\n"),
            writer: Vec::new(),
        };
        let outcome = drive(&asterpath, &handle, &mut io).await.expect("resume");

        assert!(matches!(outcome.reason, Some(TurnEndReason::Completed)));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the recorded Allow ran the tool"
        );
        assert!(
            !String::from_utf8(io.writer).unwrap().contains("allow?"),
            "a recorded decision must not re-prompt"
        );
        assert_eq!(signal_count(&asterpath, handle.id()).await, 1);

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }

    /// The CLI `untrusted` policy over a real `bash` tool that would create `ran`.
    async fn ask_bash(db: &Path, root: &Path) -> Asterpath {
        let provider = Arc::new(ScriptedProvider::new("p").respond(
            vec![Item::ToolCall {
                call_id: "call-1".to_owned(),
                provider_item_id: None,
                name: ToolName::new("bash"),
                raw_args: JsonString::new(r#"{"command":"touch ran"}"#),
                raw: None,
            }],
            StopReason::ToolUse,
            Usage::default(),
        ));
        let bash = asterpath_tools::bash(
            root.to_path_buf(),
            None,
            SandboxPolicy::FullAccess,
            asterpath_tools::DEFAULT_BASH_TIMEOUT,
        )
        .expect("absolute root");
        let config = AsterpathConfig::default();
        let policy = Arc::new(ApprovalModes::new(config::ApprovalMode::Untrusted.into()));
        AsterpathBuilder::from_config(config)
            .database(db)
            .providers(vec![provider as Arc<dyn Provider>])
            .tools(vec![bash])
            .approval(policy)
            .build()
            .await
            .expect("build")
    }

    #[tokio::test]
    async fn ask_mode_asks_before_bash_and_closed_gate_denies_without_spawning() {
        for open in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("ask.db");
            let asterpath = ask_bash(&db, dir.path()).await;
            let handle = asterpath
                .create_session(spec())
                .await
                .unwrap()
                .claim()
                .await
                .unwrap();
            let _endpoint = open.then(|| asterpath.approval_gate().register(handle.id()));
            handle
                .submit_input(UserInput {
                    id: InputId::from_bytes([1; 16]),
                    parts: vec![Part::Text { text: "go".into() }],
                })
                .await
                .unwrap();

            let mut io = PromptIo {
                reader: Cursor::new(b"n\n"),
                writer: Vec::new(),
            };
            drive(&asterpath, &handle, &mut io).await.expect("drive");

            let asked = String::from_utf8(io.writer).unwrap().contains("tool: bash");
            assert_eq!(asked, open, "ApprovalRequested only while the gate is open");
            assert!(!dir.path().join("ran").exists(), "bash never started");
            let values = tool_result_values(&asterpath, handle.id()).await;
            assert_eq!(values.len(), 1, "{values:?}");
            let reason = if open {
                "tool call denied"
            } else {
                asterpath_protocol::NO_APPROVAL_ENDPOINT
            };
            assert!(values[0].contains(reason), "{values:?}");
            asterpath.shutdown().await.unwrap();
        }
    }

    /// A child's `Ask` never reaches the parent's stream; the operator is
    /// still asked, and the answer lands in the child (audit §2.3 item 5).
    #[tokio::test]
    async fn a_child_sessions_approval_is_prompted_too() {
        let db = temp_db("child");
        let calls = Arc::new(AtomicUsize::new(0));
        let spawn = Item::ToolCall {
            call_id: "call-spawn".to_owned(),
            provider_item_id: None,
            name: ToolName::new("spawn_agent"),
            raw_args: JsonString::new(r#"{"task":"echo"}"#),
            raw: None,
        };
        let provider = Arc::new(
            ScriptedProvider::new("p")
                .respond(vec![spawn], StopReason::ToolUse, Usage::default())
                .respond(vec![tool_call()], StopReason::ToolUse, Usage::default())
                .respond(vec![final_message()], StopReason::EndTurn, Usage::default())
                .respond(vec![final_message()], StopReason::EndTurn, Usage::default()),
        );
        let asterpath = build(&db, provider, echo_tool(Arc::clone(&calls))).await;
        let handle = asterpath
            .create_session(spec())
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        let _endpoint = asterpath.approval_gate().register(handle.id());
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([1; 16]),
                parts: vec![Part::Text { text: "go".into() }],
            })
            .await
            .unwrap();

        let mut io = PromptIo {
            reader: Cursor::new(b"y\ny\n"),
            writer: Vec::new(),
        };
        let outcome =
            tokio::time::timeout(Duration::from_secs(10), drive(&asterpath, &handle, &mut io))
                .await
                .expect("the child's approval does not hang the drive")
                .expect("drive");

        assert!(matches!(outcome.reason, Some(TurnEndReason::Completed)));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the child ran echo");
        let asked = String::from_utf8(io.writer).unwrap();
        assert!(asked.contains("tool: spawn_agent"), "{asked}");
        assert!(asked.contains("tool: echo"), "{asked}");

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }

    async fn signal_count(asterpath: &Asterpath, session: SessionId) -> usize {
        scan(asterpath.store(), session)
            .await
            .unwrap()
            .iter()
            .filter(|entry| {
                decode_entry(entry).is_ok_and(|event| event.kind() == "ApprovalDecided")
            })
            .count()
    }

    async fn tool_result_values(asterpath: &Asterpath, session: SessionId) -> Vec<String> {
        scan(asterpath.store(), session)
            .await
            .unwrap()
            .iter()
            .filter_map(|entry| match decode_entry(entry).ok()? {
                SessionEvent::ToolResult { status, error, .. } => {
                    Some(format!("{status:?} {:?}", error.map(|error| error.message)))
                }
                _ => None,
            })
            .collect()
    }
}
