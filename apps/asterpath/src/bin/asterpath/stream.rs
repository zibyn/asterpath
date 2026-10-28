//! The `exec` stream protocol (packaging.md §4.3): every `AgentEvent` as one
//! JSON line on stdout and, with `--input-format stream-json`, one command per
//! stdin line. Stdin and stdout are the session driver's operator: approvals
//! of the whole tree, its children's included, go to the application over the
//! stream.

use std::collections::HashMap;
use std::io::Write;

use asterpath::{Asterpath, OwnedSession};
use asterpath_app::driver::{Approvals, Interrupt, Operator, submit};
use asterpath_protocol::{AgentEvent, ApprovalDecision, ApprovalRequest, Part, ToolCallId};
use serde::Deserialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, Lines};

use crate::drive::{self, DriveOutcome};
use crate::{CliError, CliResult, aster_error};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    UserInput {
        parts: Vec<Part>,
    },
    Approval {
        call: ToolCallId,
        decision: ApprovalDecision,
    },
    Cancel,
}

/// Drive (from the start when `start`), printing every event, and serve
/// `commands` until stdin is closed and the session is idle. Stdin closing
/// while an approval waits cancels the turn: nobody is left to answer. A bad
/// command or unreadable stdin cancels the turn and stops serving; that error
/// comes back beside the outcome so the caller still prints the result.
pub async fn run<R: AsyncBufRead + Unpin, W: Write>(
    asterpath: &Asterpath,
    handle: &OwnedSession,
    start: bool,
    commands: Option<R>,
    out: &mut W,
) -> CliResult<(DriveOutcome, Option<CliError>)> {
    let mut app = App {
        handle,
        lines: commands.map(AsyncBufReadExt::lines),
        out,
        outcome: DriveOutcome::default(),
        answers: HashMap::new(),
        error: None,
        failed: None,
    };
    let mut ran = start;
    if start {
        drive::drive(asterpath, handle, &mut app).await?;
    }
    // Between drives only input starts one; no approval is waiting.
    while let Some(command) = app.command().await {
        match command {
            Command::UserInput { parts } => {
                submit(handle, parts).await.map_err(aster_error)?;
                drive::drive(asterpath, handle, &mut app).await?;
                ran = true;
            }
            Command::Approval { .. } => {}
            Command::Cancel => handle.cancel().await.map_err(aster_error)?,
        }
    }
    if let Some(error) = app.failed {
        return Err(error);
    }
    Ok((DriveOutcome { ran, ..app.outcome }, app.error))
}

/// The application on stdin and stdout.
struct App<'a, R, W> {
    handle: &'a OwnedSession,
    /// `None` once stdin is closed or unusable.
    lines: Option<Lines<R>>,
    out: &'a mut W,
    outcome: DriveOutcome,
    /// Decisions read before their request was asked.
    answers: HashMap<ToolCallId, ApprovalDecision>,
    /// The bad command or stdin error that stopped serving.
    error: Option<CliError>,
    /// A stdout or session error that ends the run.
    failed: Option<CliError>,
}

impl<R: AsyncBufRead + Unpin, W: Write> App<'_, R, W> {
    /// The next command; `None` once stdin is closed, or after a bad command
    /// (kept in `error`). Cancel-safe, as `next_line` is.
    async fn command(&mut self) -> Option<Command> {
        loop {
            let command = match self.lines.as_mut()?.next_line().await {
                Ok(Some(line)) if line.trim().is_empty() => continue,
                Ok(Some(line)) => serde_json::from_str(&line)
                    .map_err(|error| CliError::config(format!("bad command {line}: {error}"))),
                Ok(None) => {
                    self.lines = None;
                    return None;
                }
                Err(error) => Err(CliError::run(format!("cannot read stdin: {error}"))),
            };
            match command {
                Ok(command) => return Some(command),
                Err(error) => {
                    self.error = Some(error);
                    self.lines = None;
                    return None;
                }
            }
        }
    }

    fn emit(&mut self, event: &AgentEvent) {
        if self.failed.is_none()
            && let Err(error) = emit(self.out, event)
        {
            self.failed = Some(error);
        }
    }
}

impl<R: AsyncBufRead + Unpin, W: Write> Operator for App<'_, R, W> {
    /// Only an application on stdin can answer; without one the framework
    /// denies every `Ask`.
    fn approvals(&self) -> Approvals {
        if self.lines.is_some() {
            Approvals::Asked
        } else {
            Approvals::Denied
        }
    }

    fn show(&mut self, event: &AgentEvent) {
        self.outcome.observe(event);
        self.emit(event);
    }

    async fn decide(&mut self, request: &ApprovalRequest) -> Option<ApprovalDecision> {
        self.emit(&AgentEvent::ApprovalRequested(request.clone()));
        loop {
            if let Some(decision) = self.answers.remove(&request.tool_call_id) {
                return Some(decision);
            }
            // `None`: nobody is left to answer.
            match self.command().await? {
                Command::Approval { call, decision } => {
                    self.answers.insert(call, decision);
                }
                Command::UserInput { parts } => {
                    if let Err(error) = submit(self.handle, parts).await {
                        self.failed = Some(aster_error(error));
                        return None;
                    }
                }
                Command::Cancel => return None,
            }
        }
    }

    async fn interrupted(&mut self) -> Interrupt {
        if self.lines.is_some() {
            loop {
                match self.command().await {
                    Some(Command::UserInput { parts }) => return Interrupt::Input(parts),
                    Some(Command::Cancel) => return Interrupt::Cancel,
                    Some(Command::Approval { call, decision }) => {
                        self.answers.insert(call, decision);
                    }
                    None if self.error.is_some() => return Interrupt::Cancel,
                    None => break,
                }
            }
        }
        std::future::pending().await
    }
}

fn emit<W: Write>(out: &mut W, event: &AgentEvent) -> CliResult<()> {
    let line = serde_json::to_string(event)
        .map_err(|error| CliError::run(format!("cannot encode event: {error}")))?;
    writeln!(out, "{line}")
        .and_then(|()| out.flush())
        .map_err(|error| CliError::run(format!("cannot write stdout: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_parse_from_their_documented_shapes() {
        let parse = |line: &str| serde_json::from_str::<Command>(line).unwrap();
        assert_eq!(
            parse(r#"{"type":"user_input","parts":[{"Text":{"text":"hi"}}]}"#),
            Command::UserInput {
                parts: vec![Part::Text { text: "hi".into() }]
            }
        );
        assert_eq!(
            parse(
                r#"{"type":"approval","call":"04040404040404040404040404040404","decision":"Allow"}"#
            ),
            Command::Approval {
                call: ToolCallId::from_bytes([4; 16]),
                decision: ApprovalDecision::Allow
            }
        );
        assert_eq!(
            parse(
                r#"{"type":"approval","call":"04040404040404040404040404040404","decision":{"Deny":{"reason":"no"}}}"#
            ),
            Command::Approval {
                call: ToolCallId::from_bytes([4; 16]),
                decision: ApprovalDecision::Deny {
                    reason: Some("no".into())
                }
            }
        );
        assert_eq!(parse(r#"{"type":"cancel"}"#), Command::Cancel);
        assert!(serde_json::from_str::<Command>(r#"{"type":"quit"}"#).is_err());
    }
}
