//! `asterpath exec`: one headless turn (packaging.md §4.3). `stream-json`
//! output and input are served by [`crate::stream`].

use std::io::{self, IsTerminal, Read};
use std::path::{Path, PathBuf};

use asterpath_protocol::{Clock, SystemClock, UserInput, unique_bytes};
use asterpath_protocol::{FileSource, ImageSource, InputId, OutputFormat as SchemaFormat, Part};
use clap::{Args, ValueEnum};

use crate::drive::{self, final_assistant_text, scan, turn_result};
use crate::session::{self, SessionArgs};
use crate::stream;
use crate::{CliError, CliResult, GlobalArgs, aster_error};

#[derive(Args)]
pub struct ExecArgs {
    #[command(flatten)]
    session: SessionArgs,
    /// What stdout carries.
    #[arg(long, value_enum, default_value_t)]
    output_format: OutputFormat,
    /// What stdin carries; `stream-json` needs `--output-format stream-json`.
    #[arg(long, value_enum, default_value_t)]
    input_format: InputFormat,
    /// JSON Schema the final reply must follow (structured output).
    #[arg(long, value_name = "FILE")]
    output_schema: Option<PathBuf>,
    /// Input; `-` or none reads stdin. Piped stdin is appended to a prompt.
    prompt: Option<String>,
}

#[derive(Clone, Copy, Default, ValueEnum)]
enum OutputFormat {
    /// The final assistant text only.
    #[default]
    Text,
    /// One result object.
    Json,
    /// One `AgentEvent` per line, then the result object.
    StreamJson,
}

#[derive(Clone, Copy, Default, PartialEq, ValueEnum)]
enum InputFormat {
    /// The prompt as text.
    #[default]
    Text,
    /// One command per line; approvals are asked over the stream.
    StreamJson,
}

pub async fn exec(global: &GlobalArgs, args: ExecArgs) -> CliResult<()> {
    let continuing = args.session.continuing();
    let commands = args.input_format == InputFormat::StreamJson;
    if commands && !matches!(args.output_format, OutputFormat::StreamJson) {
        return Err(CliError::config(
            "--input-format stream-json needs --output-format stream-json",
        ));
    }
    // No per-turn request option exists, and a schema patched into a continued
    // session would outlive this exec.
    if args.output_schema.is_some() && continuing && !args.session.fork_session {
        return Err(CliError::config(
            "--output-schema applies to new or forked sessions only",
        ));
    }
    // Files are read before the work root changes the process directory.
    let schema = args.output_schema.as_deref().map(read_schema).transpose()?;
    let mut parts = Vec::new();
    let text = if commands {
        args.prompt.clone().filter(|prompt| prompt != "-")
    } else {
        input_text(args.prompt.as_deref(), continuing)?
    };
    if let Some(text) = text {
        parts.push(Part::Text { text });
    }
    for path in &args.session.images {
        parts.push(attachment(path)?);
    }
    if parts.is_empty() && !continuing && !commands {
        return Err(CliError::config(
            "no prompt: give PROMPT or pipe it on stdin",
        ));
    }

    let opened = session::open(global, &args.session, schema).await?;
    let handle = &opened.handle;
    // A continued session first finishes what it had open (crash recovery).
    let recover = opened.continued && handle.facts().await.map_err(aster_error)?.is_wakeable();
    let outcome = if let OutputFormat::StreamJson = args.output_format {
        let start = recover || !parts.is_empty();
        if !parts.is_empty() {
            submit(handle, parts).await?;
        }
        // Only a host on stdin can answer; otherwise the framework denies every `Ask`.
        let _endpoint = commands.then(|| opened.asterpath.approval_gate().register());
        let stdin = commands.then(|| tokio::io::BufReader::new(tokio::io::stdin()));
        stream::run(handle, start, stdin, &mut io::stdout()).await?
    } else {
        // No approval channel: no endpoint is registered, so every `Ask` is denied.
        let mut outcome = drive::DriveOutcome::default();
        if recover {
            outcome = drive::drive(handle, &mut drive::Headless).await?;
        }
        if !parts.is_empty() {
            submit(handle, parts).await?;
            outcome = drive::drive(handle, &mut drive::Headless).await?;
        }
        outcome
    };
    let ran = outcome.ran;

    let entries = scan(opened.asterpath.store(), handle.id())
        .await
        .map_err(CliError::run)?;
    let text = ran.then(|| final_assistant_text(&entries)).flatten();
    match args.output_format {
        OutputFormat::Text if !ran => println!("idle"),
        OutputFormat::Text => {
            if let Some(text) = &text {
                println!("{text}");
            }
        }
        OutputFormat::Json | OutputFormat::StreamJson => println!(
            "{}",
            serde_json::json!({
                "session": handle.id().to_hex(),
                "result": text,
                "reason": outcome.reason,
                "usage": outcome.usage,
            })
        ),
    }
    opened.shutdown().await?;
    if let Some(error) = outcome.error {
        return Err(error);
    }
    turn_result(outcome.reason.as_ref(), &entries)
}

pub async fn submit(handle: &asterpath::SessionHandle, parts: Vec<Part>) -> CliResult<()> {
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes(unique_bytes(SystemClock.now())),
            parts,
        })
        .await
        .map(drop)
        .map_err(aster_error)
}

/// The prompt, with piped stdin appended as a `<stdin>` block; `-` or no
/// prompt reads stdin, except that a continued session on a terminal only
/// recovers.
fn input_text(prompt: Option<&str>, continuing: bool) -> CliResult<Option<String>> {
    let piped = !io::stdin().is_terminal();
    let stdin = match prompt {
        Some("-") => Some(read_stdin()?),
        Some(_) | None if piped => Some(read_stdin()?),
        None if !continuing => Some(read_stdin()?),
        _ => None,
    };
    Ok(compose(prompt, stdin))
}

fn compose(prompt: Option<&str>, stdin: Option<String>) -> Option<String> {
    let stdin = stdin.filter(|text| !text.trim().is_empty());
    match (prompt.filter(|prompt| *prompt != "-"), stdin) {
        (Some(prompt), Some(stdin)) => Some(format!("{prompt}\n\n<stdin>\n{stdin}\n</stdin>")),
        (Some(prompt), None) => Some(prompt.to_owned()),
        (None, stdin) => stdin,
    }
}

fn read_stdin() -> CliResult<String> {
    let mut text = String::new();
    io::stdin()
        .read_to_string(&mut text)
        .map_err(|error| CliError::config(format!("cannot read stdin: {error}")))?;
    Ok(text)
}

fn read_schema(path: &Path) -> CliResult<SchemaFormat> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| CliError::config(format!("cannot read {}: {error}", path.display())))?;
    let schema = serde_json::from_str(&text)
        .map_err(|error| CliError::config(format!("{}: {error}", path.display())))?;
    Ok(SchemaFormat::new("output", schema))
}

/// An image or a PDF as an inline base64 part (ADR-0031).
pub fn attachment(path: &Path) -> CliResult<Part> {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    let media_type = match extension.as_deref() {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("pdf") => "application/pdf",
        _ => {
            return Err(CliError::config(format!(
                "{}: attach png, jpeg, gif, webp or pdf",
                path.display()
            )));
        }
    };
    let bytes = std::fs::read(path)
        .map_err(|error| CliError::config(format!("cannot read {}: {error}", path.display())))?;
    let data = base64(&bytes);
    Ok(if media_type == "application/pdf" {
        Part::File {
            media_type: media_type.to_owned(),
            filename: path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned()),
            source: FileSource::Base64 { data },
        }
    } else {
        Part::Image {
            media_type: media_type.to_owned(),
            source: ImageSource::Base64 { data },
        }
    })
}

/// Standard base64 with padding.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, byte)| n | u32::from(*byte) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_rfc_4648_vectors() {
        for (input, expected) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(input.as_bytes()), expected);
        }
    }

    #[test]
    fn stdin_is_the_input_or_a_block_after_the_prompt() {
        assert_eq!(compose(Some("hi"), None).as_deref(), Some("hi"));
        assert_eq!(
            compose(Some("explain"), Some("fn main() {}\n".into())).as_deref(),
            Some("explain\n\n<stdin>\nfn main() {}\n\n</stdin>")
        );
        assert_eq!(
            compose(Some("hi"), Some(" \n".into())).as_deref(),
            Some("hi")
        );
        assert_eq!(
            compose(Some("-"), Some("piped".into())).as_deref(),
            Some("piped")
        );
        assert_eq!(
            compose(None, Some("piped".into())).as_deref(),
            Some("piped")
        );
        assert_eq!(compose(None, Some(String::new())), None);
        assert_eq!(compose(None, None), None);
    }
}
