//! The `exec` stream protocol (packaging.md §4.3): every `AgentEvent` as one
//! JSON line on stdout and, with `--input-format stream-json`, one command per
//! stdin line. Approvals go to the application over the stream.

use std::collections::HashSet;
use std::io::Write;

use asterpath::OwnedSession;
use asterpath_protocol::{AgentEvent, ApprovalDecision, Part, ToolCallId};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use serde::Deserialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use crate::drive::DriveOutcome;
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
/// command or unreadable stdin cancels the turn and stops serving; the error
/// comes back in [`DriveOutcome::error`] so the caller still prints the result.
pub async fn run<R: AsyncBufRead + Unpin, W: Write>(
    handle: &OwnedSession,
    start: bool,
    commands: Option<R>,
    out: &mut W,
) -> CliResult<DriveOutcome> {
    let mut outcome = DriveOutcome {
        ran: start,
        ..DriveOutcome::default()
    };
    let mut lines = commands.map(AsyncBufReadExt::lines);
    let mut stream: Option<BoxStream<'_, _>> = start.then(|| handle.drive());
    let mut pending: HashSet<ToolCallId> = HashSet::new();
    loop {
        // Nobody is left to answer a waiting approval.
        if lines.is_none() && !pending.is_empty() {
            pending.clear();
            handle.cancel().await.map_err(aster_error)?;
        }
        tokio::select! {
            event = async { stream.as_mut()?.next().await }, if stream.is_some() => match event {
                Some(Ok(event)) => {
                    outcome.observe(&event);
                    if let AgentEvent::ApprovalRequested(request) = &event {
                        pending.insert(request.tool_call_id);
                    }
                    emit(out, &event)?;
                }
                Some(Err(error)) => return Err(CliError::run(format!("drive failed: {error}"))),
                // Input that arrived during the drive starts the next one.
                None => {
                    stream = None;
                    pending.clear();
                    if outcome.error.is_none()
                        && handle.facts().await.map_err(aster_error)?.is_wakeable()
                    {
                        stream = Some(handle.drive());
                    }
                }
            },
            line = async { lines.as_mut()?.next_line().await.transpose() }, if lines.is_some() => {
                let command = match line {
                    Some(Ok(line)) if line.trim().is_empty() => continue,
                    Some(Ok(line)) => serde_json::from_str(&line)
                        .map_err(|error| CliError::config(format!("bad command {line}: {error}"))),
                    Some(Err(error)) => Err(CliError::run(format!("cannot read stdin: {error}"))),
                    None => {
                        lines = None;
                        continue;
                    }
                };
                match command {
                    Ok(Command::UserInput { parts }) => {
                        crate::exec::submit(handle, parts).await?;
                        if stream.is_none() {
                            stream = Some(handle.drive());
                            outcome.ran = true;
                        }
                    }
                    Ok(Command::Approval { call, decision }) => {
                        pending.remove(&call);
                        handle.approve(call, decision).await.map_err(aster_error)?;
                    }
                    Ok(Command::Cancel) => handle.cancel().await.map_err(aster_error)?,
                    Err(error) => {
                        outcome.error = Some(error);
                        lines = None;
                        handle.cancel().await.map_err(aster_error)?;
                    }
                }
            },
            else => break,
        }
    }
    Ok(outcome)
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
