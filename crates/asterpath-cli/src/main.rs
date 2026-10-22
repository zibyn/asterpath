//! Reference CLI: `run`, `inspect` and `config check`.
//!
//! The binary owns host policy (exit codes, stdout/stderr) and error reporting;
//! it composes the same facade an embedder would.

mod config;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use asterpath::{AsterError, Asterpath};
use asterpath_core::{EventLog, InputId, LogEntry, LogQuery, SessionId};
use asterpath_protocol::{
    AgentEvent, Item, OpenIntent, OpenIntents, Part, Role, SessionState, TurnEndReason, UserInput,
    decode_entry, history,
};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;

#[derive(Parser)]
#[command(name = "asterpath", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a session, run one turn, and print the final assistant text.
    Run(RunArgs),
    /// Print a projection summary and the open intents of a session.
    Inspect(InspectArgs),
    /// Configuration commands.
    Config(ConfigArgs),
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    common: Common,
    /// User input for the new session.
    prompt: String,
}

#[derive(Args)]
struct InspectArgs {
    #[command(flatten)]
    common: Common,
    /// Session id in hex.
    session: String,
}

#[derive(Args)]
struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigCommand,
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Parse and validate the configuration without initialising modules.
    Check(CheckArgs),
}

#[derive(Args)]
struct CheckArgs {
    #[command(flatten)]
    common: Common,
}

/// Options shared by every command. Unknown options (notably `--session` on
/// `run`) are usage errors, which clap exits with 2.
#[derive(Args)]
struct Common {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    db: Option<PathBuf>,
    #[arg(long)]
    provider: Option<String>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long, value_enum)]
    approval: Option<config::ApprovalMode>,
    /// Emit machine-readable output where the command supports it.
    #[arg(long)]
    json: bool,
}

impl Common {
    fn overrides(&self) -> config::Overrides {
        config::Overrides {
            config: self.config.clone(),
            db: self.db.clone(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            approval: self.approval,
        }
    }
}

struct CliError {
    code: i32,
    message: String,
}

impl CliError {
    fn config(message: impl Into<String>) -> Self {
        Self {
            code: 2,
            message: message.into(),
        }
    }

    fn run(message: impl Into<String>) -> Self {
        Self {
            code: 1,
            message: message.into(),
        }
    }
}

type CliResult<T> = Result<T, CliError>;

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
        Command::Run(args) => run(args).await,
        Command::Inspect(args) => inspect(args).await,
        Command::Config(args) => match args.command {
            ConfigCommand::Check(args) => config_check(args),
        },
    }
}

fn load(common: &Common) -> CliResult<config::Settings> {
    let settings = config::load(common.overrides()).map_err(CliError::config)?;
    config::validate(&settings).map_err(CliError::config)?;
    Ok(settings)
}

fn config_check(args: CheckArgs) -> CliResult<()> {
    load(&args.common)?;
    println!("config ok");
    Ok(())
}

async fn run(args: RunArgs) -> CliResult<()> {
    let settings = load(&args.common)?;
    let spec = config::session_spec(&settings).map_err(CliError::config)?;
    let providers = config::providers(&settings).map_err(CliError::config)?;
    let asterpath = assemble(&settings, providers).await?;

    let handle = asterpath.create_session(spec).await.map_err(aster_error)?;
    eprintln!("session: {}", handle.id().to_hex());
    handle
        .submit_input(UserInput {
            id: fresh_input_id(),
            parts: vec![Part::Text { text: args.prompt }],
        })
        .await
        .map_err(aster_error)?;

    let mut reason = None;
    {
        let mut stream = handle.drive();
        while let Some(event) = stream.next().await {
            match event {
                Ok(AgentEvent::TurnCommitted { reason: ended, .. }) => reason = Some(ended),
                Ok(_) => {}
                Err(error) => return Err(CliError::run(format!("drive failed: {error}"))),
            }
        }
    }

    let entries = scan(asterpath.event_log(), handle.id())
        .await
        .map_err(CliError::run)?;
    if let Some(text) = final_assistant_text(&entries) {
        println!("{text}");
    }
    asterpath.shutdown().await.map_err(aster_error)?;

    match reason {
        None | Some(TurnEndReason::Completed) => Ok(()),
        Some(other) => Err(CliError::run(format!("turn ended with {other:?}"))),
    }
}

async fn inspect(args: InspectArgs) -> CliResult<()> {
    let settings = load(&args.common)?;
    let session = parse_session(&args.session)?;
    if !settings.db.exists() {
        return Err(CliError::run(format!("session {session:?} not found")));
    }
    let asterpath = Asterpath::builder()
        .database(&settings.db)
        .build()
        .await
        .map_err(aster_error)?;

    let entries = scan(asterpath.event_log(), session)
        .await
        .map_err(CliError::run)?;
    if entries.is_empty() {
        return Err(CliError::run(format!("session {session:?} not found")));
    }

    let (open, state) = fold_open_intents(&entries).map_err(CliError::run)?;
    if args.common.json {
        let items = history(&entries).map_err(|error| CliError::run(error.to_string()))?;
        let json = serde_json::to_string_pretty(&items)
            .map_err(|error| CliError::run(error.to_string()))?;
        println!("{json}");
    } else {
        let items = history(&entries).map_err(|error| CliError::run(error.to_string()))?;
        println!("session: {}", session.to_hex());
        println!("state: {}", state_label(state));
        println!("entries: {}", entries.len());
        println!("projected items: {}", items.len());
        if open.is_empty() {
            println!("open intents: none");
        } else {
            println!("open intents:");
            for intent in open {
                println!("  {intent:?}");
            }
        }
    }

    asterpath.shutdown().await.map_err(aster_error)?;
    Ok(())
}

async fn assemble(
    settings: &config::Settings,
    providers: Vec<Arc<dyn asterpath_protocol::Provider>>,
) -> CliResult<Asterpath> {
    let mut builder = Asterpath::builder()
        .database(&settings.db)
        .providers(providers)
        .tool(config::tool_config(settings))
        .turn(config::turn_config(settings))
        .compaction(config::compaction_config(settings))
        .subagents(config::subagent_config(settings))
        .model_aliases(config::model_aliases(settings));
    if let Some(policy) = config::approval_policy(settings.approval) {
        builder = builder.approval(policy);
    }
    builder.build().await.map_err(aster_error)
}

async fn scan(event_log: Arc<dyn EventLog>, session: SessionId) -> Result<Vec<LogEntry>, String> {
    let mut stream = event_log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.map_err(|error| error.to_string())?);
    }
    Ok(entries)
}

/// Fold the log once; the deltas tell the summary which intents are still open.
fn fold_open_intents(entries: &[LogEntry]) -> Result<(Vec<OpenIntent>, SessionState), String> {
    let mut intents = OpenIntents::default();
    let mut open = BTreeSet::new();
    for entry in entries {
        let event = decode_entry(entry).map_err(|error| error.to_string())?;
        let delta = intents.apply_delta(&event, entry.dedup_key.as_ref());
        for intent in delta.opened {
            open.insert(intent);
        }
        for intent in delta.closed {
            open.remove(&intent);
        }
    }
    Ok((open.into_iter().collect(), intents.state()))
}

fn state_label(state: SessionState) -> String {
    match state {
        SessionState::Idle => "idle".to_owned(),
        SessionState::TurnOpen { turn, .. } => format!("turn-open {}", turn.to_hex()),
    }
}

/// The last assistant message in the model-visible projection.
fn final_assistant_text(entries: &[LogEntry]) -> Option<String> {
    let items = history(entries).ok()?;
    for item in items.iter().rev() {
        if let Item::Message {
            role: Role::Assistant,
            parts,
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

fn parse_session(value: &str) -> CliResult<SessionId> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| CliError::config(format!("invalid session id {value}")))
}

fn aster_error(error: AsterError) -> CliError {
    match error {
        AsterError::Config { message } => CliError::config(message),
        other => CliError::run(other.to_string()),
    }
}

fn fresh_input_id() -> InputId {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or_default();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0u8; 16];
    bytes[..8]
        .copy_from_slice(&(nanos ^ (std::process::id() as u64).rotate_left(17)).to_le_bytes());
    bytes[8..].copy_from_slice(&counter.to_le_bytes());
    InputId::from_bytes(bytes)
}
