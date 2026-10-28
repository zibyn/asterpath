//! Reference CLI (packaging.md §4): `exec`, `doctor`, `agents`, `styles` and `debug`, with the
//! interactive mode on the bare command.
//!
//! The binary owns application policy (exit codes, stdout/stderr, approval
//! prompts) and error reporting; it composes the same facade an embedder would.
//!
//! Layout: `config` (from the `asterpath_app` library) resolves the file and
//! `-c`; `session` resolves the flags and opens a new, continued or forked
//! session with the library's work-root assembly (ADR-0049); `drive`
//! runs it with the library's session driver (`driver`); `exec`, `doctor`, `agents`, `styles` and `debug` are the
//! commands; `interactive` is the bare command (`command: None`) and reuses
//! `session::open` and `drive`.

mod agents;
mod debug;
mod doctor;
mod drive;
mod exec;
mod interactive;
mod session;
mod stream;

use std::path::PathBuf;

use asterpath::AsterError;
use asterpath_app::config;
use asterpath_protocol::SessionId;
use clap::parser::ValueSource;
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "asterpath", version, about)]
struct Cli {
    #[command(flatten)]
    global: GlobalArgs,
    #[command(flatten)]
    session: session::SessionArgs,
    /// First input of the interactive session.
    prompt: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run one turn headless and print the result.
    Exec(exec::ExecArgs),
    /// Validate the configuration, report MCP servers and skills, then exit.
    Doctor,
    /// List the agent profiles of the work root and the file each comes from.
    Agents,
    /// List the output styles of the work root and the file each comes from.
    Styles,
    /// Inspect a session's log.
    Debug(debug::DebugArgs),
}

/// Options every command takes.
#[derive(Args)]
pub struct GlobalArgs {
    /// Configuration file (default `$XDG_CONFIG_HOME/asterpath/config.toml`).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// Override any configuration key: dotted path, TOML value (else a string).
    #[arg(
        short = 'c',
        long = "config-override",
        value_name = "KEY=VALUE",
        global = true
    )]
    overrides: Vec<String>,
}

impl GlobalArgs {
    /// The file plus `-c`; validation and flags are the caller's.
    fn settings(&self) -> CliResult<config::Settings> {
        config::load(self.config.clone(), &self.overrides).map_err(CliError::config)
    }
}

#[derive(Debug)]
pub struct CliError {
    code: i32,
    message: String,
}

impl CliError {
    pub fn config(message: impl std::fmt::Display) -> Self {
        Self {
            code: 2,
            message: message.to_string(),
        }
    }

    pub fn run(message: impl std::fmt::Display) -> Self {
        Self {
            code: 1,
            message: message.to_string(),
        }
    }
}

pub type CliResult<T> = Result<T, CliError>;

impl Cli {
    /// Global options may come before a subcommand; the interactive mode's
    /// options and prompt may not, since the subcommand would drop them.
    fn parse_checked() -> Self {
        let matches = Self::command().get_matches();
        if matches.subcommand().is_some()
            // clap derive ids: the `SessionArgs` group and the `prompt` field.
            && ["SessionArgs", "prompt"]
                .into_iter()
                .any(|id| matches.value_source(id) == Some(ValueSource::CommandLine))
        {
            Self::command()
                .error(
                    clap::error::ErrorKind::ArgumentConflict,
                    "only --config and -c may come before a subcommand",
                )
                .exit();
        }
        Self::from_arg_matches(&matches).unwrap_or_else(|error| error.exit())
    }
}

#[tokio::main]
async fn main() {
    let code = match dispatch(Cli::parse_checked()).await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("asterpath: {}", error.message);
            error.code
        }
    };
    std::process::exit(code);
}

async fn dispatch(cli: Cli) -> CliResult<()> {
    match cli.command {
        Some(Command::Exec(args)) => exec::exec(&cli.global, args).await,
        Some(Command::Doctor) => doctor::doctor(&cli.global).await,
        Some(Command::Agents) => agents::agents(&cli.global),
        Some(Command::Styles) => agents::styles(&cli.global),
        Some(Command::Debug(args)) => debug::debug(&cli.global, args).await,
        None => interactive::interactive(&cli.global, cli.session, cli.prompt).await,
    }
}

pub fn parse_session(value: &str) -> CliResult<SessionId> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| CliError::config(format!("invalid session id {value}")))
}

pub fn aster_error(error: AsterError) -> CliError {
    match error {
        AsterError::Config { message } => CliError::config(message),
        other => CliError::run(other.to_string()),
    }
}

pub fn not_found(session: SessionId) -> CliError {
    CliError::run(format!("session {session:?} not found"))
}
