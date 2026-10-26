//! Interactive mode (packaging.md §4.5): a line-mode REPL over one session.
//!
//! Input lines and Ctrl-C arrive on channels, so [`repl`] runs without a TTY:
//! a thread feeds stdin lines, a task feeds interrupts. Approval prompts read
//! their answer from the same line channel and are interrupted by Ctrl-C.

use std::io::{self, BufRead, Write};

use asterpath::{Asterpath, OwnedSession, SessionHandle};
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalRequest, InputId, Item, LogEntry, Part, PartDelta, Role,
    SystemClock, UsageScope, UserInput, project, unique_bytes,
};
use asterpath_protocol::{Clock, CompactResult};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::drive::{self, scan, turn_result};
use crate::exec::attachment;
use crate::session::{self, SessionArgs};
use crate::{CliError, CliResult, GlobalArgs, aster_error};

/// How long a tool result may be on screen.
const RESULT_CHARS: usize = 200;

const HELP: &str = "\
/help           this list
/new            start a new session
/model <id>     switch the model (same provider)
/compact        compact the context now
/status         session state and usage
/exit           quit (also Ctrl-D, or Ctrl-C while idle)";

/// What the REPL asks its caller to do when it returns.
#[derive(Debug, PartialEq)]
pub enum Next {
    Exit,
    New,
}

pub async fn interactive(
    global: &GlobalArgs,
    mut args: SessionArgs,
    prompt: Option<String>,
) -> CliResult<()> {
    let mut first: Vec<Part> = prompt.map(|text| Part::Text { text }).into_iter().collect();
    for path in &args.images {
        first.push(attachment(path)?);
    }
    args.config_style = true;
    let mut opened = session::open(global, &args, None).await?;
    // After `open`: the `-r` picker reads stdin itself.
    let mut lines = stdin_lines();
    let mut interrupts = interrupts();
    loop {
        let Some(owned) = &opened.owned else {
            // Another process drives it: show the conversation, change nothing.
            let entries = scan(opened.asterpath.store(), opened.handle.id())
                .await
                .map_err(CliError::run)?;
            transcript(&mut io::stdout(), &entries);
            return opened.shutdown().await;
        };
        // The TTY prompt is this mode's approval channel (§4.4).
        let endpoint = opened
            .asterpath
            .approval_gate()
            .register(owned.owner().root);
        let next = repl(
            &opened.asterpath,
            owned,
            std::mem::take(&mut first),
            &mut lines,
            &mut interrupts,
            io::stdout(),
        )
        .await;
        drop(endpoint);
        // `/new` keeps the model in use, including one chosen with `/model`.
        // shortcut: carries the model id only, so a resumed session on another provider than the config's hands over a foreign id; carry the provider when `-m` can name one
        let model = match &next {
            Ok(Next::New) => session::snapshot(&opened.asterpath, opened.handle.id())
                .await
                .ok()
                .map(|snapshot| snapshot.model.model.as_str().to_owned()),
            _ => None,
        };
        opened.shutdown().await?;
        match next? {
            Next::Exit => return Ok(()),
            Next::New => {
                let args = new_session(&args, model);
                opened = session::open(global, &args, None).await?;
            }
        }
    }
}

/// The flags of a fresh session in the current work root.
fn new_session(args: &SessionArgs, model: Option<String>) -> SessionArgs {
    SessionArgs {
        model: model.or_else(|| args.model.clone()),
        approval: args.approval,
        sandbox: args.sandbox,
        cd: None,
        continue_last: false,
        resume: None,
        fork_session: false,
        name: None,
        agent: args.agent.clone(),
        style: args.style.clone(),
        config_style: args.config_style,
        images: Vec::new(),
    }
}

fn stdin_lines() -> UnboundedReceiver<String> {
    let (tx, rx) = unbounded_channel();
    std::thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn interrupts() -> UnboundedReceiver<()> {
    let (tx, rx) = unbounded_channel();
    tokio::spawn(async move {
        while tokio::signal::ctrl_c().await.is_ok() {
            if tx.send(()).is_err() {
                break;
            }
        }
    });
    rx
}

/// Run the session until EOF, an idle interrupt, `/exit` or `/new`.
/// An open turn or pending input is driven first (crash recovery), then `first`.
pub async fn repl<W: Write>(
    asterpath: &Asterpath,
    handle: &OwnedSession,
    first: Vec<Part>,
    lines: &mut UnboundedReceiver<String>,
    interrupts: &mut UnboundedReceiver<()>,
    mut out: W,
) -> CliResult<Next> {
    if handle.facts().await.map_err(aster_error)?.is_wakeable() {
        turn(asterpath, handle, None, lines, interrupts, &mut out).await?;
    }
    if !first.is_empty() {
        turn(asterpath, handle, Some(first), lines, interrupts, &mut out).await?;
    }
    loop {
        let _ = write!(out, "> ");
        let _ = out.flush();
        let line = tokio::select! {
            line = lines.recv() => line,
            Some(()) = interrupts.recv() => None,
        };
        let Some(line) = line else {
            let _ = writeln!(out);
            return Ok(Next::Exit);
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('/') {
            if let Some(next) = command(asterpath, handle, line, &mut out).await {
                return Ok(next);
            }
            continue;
        }
        let parts = vec![Part::Text {
            text: line.to_owned(),
        }];
        turn(asterpath, handle, Some(parts), lines, interrupts, &mut out).await?;
    }
}

/// Submit `parts` (if any) and drive to the end of the turn; Ctrl-C cancels.
/// A turn that does not complete is reported, not fatal.
async fn turn<W: Write>(
    asterpath: &Asterpath,
    handle: &OwnedSession,
    parts: Option<Vec<Part>>,
    lines: &mut UnboundedReceiver<String>,
    interrupts: &mut UnboundedReceiver<()>,
    out: &mut W,
) -> CliResult<()> {
    if let Some(parts) = parts {
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes(unique_bytes(SystemClock.now())),
                parts,
            })
            .await
            .map_err(aster_error)?;
    }
    let outcome = drive::drive(
        asterpath,
        handle,
        &mut Terminal {
            lines,
            interrupts,
            out: &mut *out,
        },
    )
    .await?;
    let _ = writeln!(out);
    let entries = scan(asterpath.store(), handle.id())
        .await
        .map_err(CliError::run)?;
    if let Err(error) = turn_result(outcome.reason.as_ref(), &entries) {
        let _ = writeln!(out, "{}", error.message);
    }
    Ok(())
}

/// A slash command; `Some` ends the REPL.
async fn command<W: Write>(
    asterpath: &Asterpath,
    handle: &OwnedSession,
    line: &str,
    out: &mut W,
) -> Option<Next> {
    let mut words = line.split_whitespace();
    let name = words.next().unwrap_or_default();
    let result = match name {
        "/exit" => return Some(Next::Exit),
        "/new" => return Some(Next::New),
        "/help" => Ok(HELP.to_owned()),
        "/model" => match words.next() {
            Some(model) => switch_model(asterpath, handle, model).await,
            None => Err(CliError::config("usage: /model <id>")),
        },
        "/compact" => match handle.compact_now().await {
            Ok(CompactResult::Applied(_)) => Ok("compacted".to_owned()),
            Ok(other) => Ok(format!("compact: {other:?}")),
            Err(error) => Err(aster_error(error)),
        },
        "/status" => status(asterpath, handle).await,
        other => Err(CliError::config(format!(
            "unknown command {other}; /help lists them"
        ))),
    };
    let _ = match result {
        Ok(text) => writeln!(out, "{text}"),
        Err(error) => writeln!(out, "{}", error.message),
    };
    None
}

async fn switch_model(
    asterpath: &Asterpath,
    handle: &SessionHandle,
    model: &str,
) -> CliResult<String> {
    let snapshot = session::snapshot(asterpath, handle.id()).await?;
    handle
        .update_config(session::model_patch(&snapshot, model))
        .await
        .map_err(aster_error)?;
    Ok(format!("model: {model}"))
}

async fn status(asterpath: &Asterpath, handle: &SessionHandle) -> CliResult<String> {
    let snapshot = session::snapshot(asterpath, handle.id()).await?;
    let state = handle.state().await.map_err(aster_error)?;
    let usage = asterpath
        .usage(UsageScope::Session(handle.id()))
        .await
        .map_err(|error| CliError::run(error.to_string()))?;
    let own = &usage.own.model_calls;
    Ok(format!(
        "session: {}\nmodel: {}/{}\nstate: {state:?}\nusage: {} input, {} output, {} total tokens",
        handle.id().to_hex(),
        snapshot.model.provider.as_str(),
        snapshot.model.model.as_str(),
        own.input_total(),
        own.output.total,
        usage.own.total(),
    ))
}

/// Show a live event: text deltas as they stream, tool calls and their
/// truncated results. Display redaction would go here (application policy).
/// The conversation's text as the model sees it: `> ` before user lines.
fn transcript<W: Write>(out: &mut W, entries: &[LogEntry]) {
    let Ok(projection) = project(entries) else {
        return;
    };
    for item in projection.into_iter().flat_map(|projected| projected.items) {
        let Item::Message { role, parts, .. } = item else {
            continue;
        };
        let text: String = parts
            .iter()
            .filter_map(|part| match part {
                Part::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        let _ = match role {
            Role::User => writeln!(out, "> {text}"),
            Role::Assistant => writeln!(out, "{text}"),
        };
    }
}

pub fn render<W: Write>(out: &mut W, event: &AgentEvent) {
    let _ = match event {
        AgentEvent::ModelDelta {
            delta: PartDelta::Text { text },
            ..
        } => write!(out, "{text}"),
        AgentEvent::ToolStarted { tool, args, .. } => {
            writeln!(out, "\n[tool] {} {}", tool.as_str(), args.as_str())
        }
        AgentEvent::ToolFinished {
            status,
            output,
            error,
            ..
        } => {
            let text = match (output, error) {
                (_, Some(error)) => error.message.clone(),
                (Some(parts), None) => parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
                (None, None) => String::new(),
            };
            writeln!(out, "[{status:?}] {}", truncate(&text))
        }
        _ => Ok(()),
    };
    let _ = out.flush();
}

fn truncate(text: &str) -> String {
    match text.char_indices().nth(RESULT_CHARS) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text.to_owned(),
    }
}

/// The TTY operator: live events on `out`, approvals answered from the line
/// channel, Ctrl-C cancels the turn (also while an approval is asked).
struct Terminal<'a, W> {
    lines: &'a mut UnboundedReceiver<String>,
    interrupts: &'a mut UnboundedReceiver<()>,
    out: W,
}

impl<W: Write> drive::Operator for Terminal<'_, W> {
    fn show(&mut self, event: &AgentEvent) {
        render(&mut self.out, event);
    }

    async fn decide(&mut self, request: &ApprovalRequest) -> Option<ApprovalDecision> {
        drive::ask(&mut self.out, request);
        tokio::select! {
            line = self.lines.recv() => Some(drive::answer(line.as_deref())),
            Some(()) = self.interrupts.recv() => None,
        }
    }

    async fn interrupted(&mut self) {
        if self.interrupts.recv().await.is_none() {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use asterpath_protocol::{
        Item, JsonString, ModelId, ModelSpec, Provider, ProviderId, Role, SessionSpec, StopReason,
        Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput, Usage,
    };
    use asterpath_testing::ScriptedProvider;
    use futures_util::future::BoxFuture;

    /// Runs until the turn is cancelled.
    struct Hang(ToolDescriptor);

    impl Tool for Hang {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.0
        }

        fn invoke(
            &self,
            _args: ToolArgs,
            ctx: ToolContext,
        ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
            Box::pin(async move {
                ctx.cancel.cancelled().await;
                Ok(ToolOutput::new(Vec::new()))
            })
        }
    }

    fn spec() -> SessionSpec {
        let mut spec = SessionSpec {
            model: ModelSpec {
                provider: ProviderId::new("p"),
                model: ModelId::new("m"),
                context_window: None,
            },
            system: Vec::new(),
            tools: asterpath_protocol::ToolSelection::All,
            request: Default::default(),
            turn: Default::default(),
            compaction: Default::default(),
            subagents: Default::default(),
            metadata: Default::default(),
        };
        spec.turn.initial_backoff = Duration::ZERO;
        spec
    }

    struct AlwaysAsk;

    impl asterpath_protocol::ApprovalPolicy for AlwaysAsk {
        fn classify(&self, _request: &ApprovalRequest) -> asterpath_protocol::ApprovalVerdict {
            asterpath_protocol::ApprovalVerdict::Ask
        }
    }

    async fn setup(dir: &std::path::Path, ask: bool) -> Asterpath {
        let provider = ScriptedProvider::new("p")
            .respond(
                vec![Item::ToolCall {
                    call_id: "call-1".to_owned(),
                    provider_item_id: None,
                    name: ToolName::new("hang"),
                    raw_args: JsonString::new("{}"),
                    raw: None,
                }],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                vec![Item::Message {
                    role: Role::Assistant,
                    parts: vec![Part::Text {
                        text: "after".into(),
                    }],
                    raw: None,
                }],
                StopReason::EndTurn,
                Usage::default(),
            );
        let hang = Hang(ToolDescriptor::new(
            ToolName::new("hang"),
            String::new(),
            serde_json::json!({"type": "object"}),
        ));
        let mut builder = Asterpath::builder()
            .database(dir.join("repl.db"))
            .providers(vec![Arc::new(provider) as Arc<dyn Provider>])
            .tools(vec![Arc::new(hang) as Arc<dyn Tool>]);
        if ask {
            builder = builder.approval(Arc::new(AlwaysAsk));
        }
        builder.build().await.expect("build")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ctrl_c_cancels_the_turn_then_exits_while_idle() {
        let dir = tempfile::tempdir().unwrap();
        let asterpath = setup(dir.path(), false).await;
        let handle = asterpath
            .create_session(spec())
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        let (line_tx, mut lines) = unbounded_channel::<String>();
        let (interrupt_tx, mut interrupts) = unbounded_channel();
        line_tx.send("go".into()).unwrap();
        let interrupter = tokio::spawn(async move {
            // One Ctrl-C mid-turn, one at the next prompt.
            for _ in 0..2 {
                tokio::time::sleep(Duration::from_millis(300)).await;
                interrupt_tx.send(()).unwrap();
            }
            interrupt_tx
        });

        let mut out = Vec::new();
        let next = tokio::time::timeout(
            Duration::from_secs(10),
            repl(
                &asterpath,
                &handle,
                Vec::new(),
                &mut lines,
                &mut interrupts,
                &mut out,
            ),
        )
        .await
        .expect("the REPL returns")
        .expect("no error");

        assert_eq!(next, Next::Exit);
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("[tool] hang {}"), "{out}");
        assert!(out.contains("turn ended with Cancelled"), "{out}");
        assert!(!out.contains("after"), "{out}");
        drop((line_tx, interrupter.await));
        drop(handle);
        asterpath.shutdown().await.unwrap();
    }

    /// Single-threaded: the approval prompt must not block the runtime.
    #[tokio::test]
    async fn ctrl_c_during_an_approval_prompt_cancels_the_turn() {
        let dir = tempfile::tempdir().unwrap();
        let asterpath = setup(dir.path(), true).await;
        let handle = asterpath
            .create_session(spec())
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        let _endpoint = asterpath.approval_gate().register(handle.id());
        let (line_tx, mut lines) = unbounded_channel::<String>();
        let (interrupt_tx, mut interrupts) = unbounded_channel();
        line_tx.send("go".into()).unwrap();
        let interrupter = tokio::spawn(async move {
            // One Ctrl-C at the approval prompt, one at the next prompt.
            for _ in 0..2 {
                tokio::time::sleep(Duration::from_millis(300)).await;
                interrupt_tx.send(()).unwrap();
            }
            interrupt_tx
        });

        let mut out = Vec::new();
        let next = tokio::time::timeout(
            Duration::from_secs(10),
            repl(
                &asterpath,
                &handle,
                Vec::new(),
                &mut lines,
                &mut interrupts,
                &mut out,
            ),
        )
        .await
        .expect("the REPL returns")
        .expect("no error");

        assert_eq!(next, Next::Exit);
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("allow? [y/N]"), "{out}");
        assert!(out.contains("turn ended with Cancelled"), "{out}");
        assert!(!out.contains("after"), "{out}");
        drop((line_tx, interrupter.await));
        drop(handle);
        asterpath.shutdown().await.unwrap();
    }

    #[test]
    fn long_tool_results_are_truncated_on_a_char_boundary() {
        let text = "é".repeat(RESULT_CHARS + 1);
        let shown = truncate(&text);
        assert_eq!(shown.chars().count(), RESULT_CHARS + 1);
        assert!(shown.ends_with('…'));
        assert_eq!(truncate("short"), "short");
    }
}
