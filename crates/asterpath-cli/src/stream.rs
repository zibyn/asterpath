//! The `exec` host protocol (packaging.md §4.3): every `AgentEvent` as one
//! JSON line on stdout and, with `--input-format stream-json`, one command per
//! stdin line. Approvals go to the host over the stream.

use std::io::Write;

use asterpath::SessionHandle;
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
/// `commands` until stdin is closed and the session is idle. Returns the last
/// turn's outcome and whether anything was driven.
pub async fn run<R: AsyncBufRead + Unpin, W: Write>(
    handle: &SessionHandle,
    start: bool,
    commands: Option<R>,
    out: &mut W,
) -> CliResult<(DriveOutcome, bool)> {
    let mut outcome = DriveOutcome::default();
    let mut ran = start;
    let mut lines = commands.map(AsyncBufReadExt::lines);
    let mut stream: Option<BoxStream<'_, _>> = start.then(|| handle.drive());
    loop {
        tokio::select! {
            event = async { stream.as_mut()?.next().await }, if stream.is_some() => match event {
                Some(Ok(event)) => {
                    if let AgentEvent::TurnCommitted { reason, usage, .. } = &event {
                        outcome.reason = Some(reason.clone());
                        outcome.usage = Some(*usage);
                    }
                    emit(out, &event)?;
                }
                Some(Err(error)) => return Err(CliError::run(format!("drive failed: {error}"))),
                // Input that arrived during the drive starts the next one.
                None => {
                    stream = None;
                    if handle.facts().await.map_err(aster_error)?.is_wakeable() {
                        stream = Some(handle.drive());
                    }
                }
            },
            line = async { lines.as_mut()?.next_line().await.transpose() }, if lines.is_some() => match line {
                Some(line) => {
                    let line = line.map_err(|error| CliError::run(format!("cannot read stdin: {error}")))?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let command = serde_json::from_str(&line)
                        .map_err(|error| CliError::config(format!("bad command {line}: {error}")))?;
                    match command {
                        Command::UserInput { parts } => {
                            crate::exec::submit(handle, parts).await?;
                            if stream.is_none() {
                                stream = Some(handle.drive());
                                ran = true;
                            }
                        }
                        Command::Approval { call, decision } => {
                            handle.approve(call, decision).await.map_err(aster_error)?;
                        }
                        Command::Cancel => handle.cancel().await.map_err(aster_error)?,
                    }
                }
                None => lines = None,
            },
            else => break,
        }
    }
    Ok((outcome, ran))
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
