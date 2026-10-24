//! Reference CLI (packaging.md §4): `exec`, `doctor` and `debug`, with the
//! interactive mode on the bare command.
//!
//! The binary owns host policy (exit codes, stdout/stderr, approval prompts) and
//! error reporting; it composes the same facade an embedder would.
//!
//! Layout: `config` resolves the file, `-c` and flags; `session` assembles the
//! composition root and opens a new, continued or forked session; `drive`
//! runs it to the end of a turn; `exec`, `doctor` and `debug` are the
//! commands; `interactive` is the bare command (`command: None`) and reuses
//! `session::open` and `drive`.

mod config;
mod debug;
mod doctor;
mod drive;
mod exec;
mod interactive;
mod session;

use std::path::PathBuf;

use asterpath::AsterError;
use asterpath_protocol::SessionId;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "asterpath",
    version,
    about,
    args_conflicts_with_subcommands = true
)]
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
    pub fn config(message: impl Into<String>) -> Self {
        Self {
            code: 2,
            message: message.into(),
        }
    }

    pub fn run(message: impl Into<String>) -> Self {
        Self {
            code: 1,
            message: message.into(),
        }
    }
}

pub type CliResult<T> = Result<T, CliError>;

#[tokio::main]
async fn main() {
    let code = match dispatch(Cli::parse()).await {
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
