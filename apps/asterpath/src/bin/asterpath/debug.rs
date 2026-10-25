//! `asterpath debug`: read-only JSON views of a session's log.

use asterpath_protocol::{Item, SessionId};
use clap::{Args, Subcommand};

use crate::session::bare;
use crate::{CliError, CliResult, GlobalArgs, aster_error, not_found, parse_session};

#[derive(Args)]
pub struct DebugArgs {
    #[command(subcommand)]
    command: DebugCommand,
}

#[derive(Subcommand)]
enum DebugCommand {
    /// Print the projection (model-visible input) and the open intents.
    PromptInput {
        /// Session id in hex.
        session: String,
    },
    /// Print the span tree.
    Trace {
        /// Session id in hex.
        session: String,
    },
}

pub async fn debug(global: &GlobalArgs, args: DebugArgs) -> CliResult<()> {
    let settings = global.settings()?;
    let (DebugCommand::PromptInput { session } | DebugCommand::Trace { session }) = &args.command;
    let session = parse_session(session)?;
    if !settings.db.exists() {
        return Err(not_found(session));
    }
    let asterpath = bare(&settings).await?;
    let json = match args.command {
        DebugCommand::PromptInput { .. } => prompt_input(&asterpath, session).await,
        DebugCommand::Trace { .. } => asterpath
            .trace(session)
            .await
            .map(|spans| serde_json::json!(spans))
            .map_err(|error| CliError::run(error.to_string())),
    };
    let shutdown = asterpath.shutdown().await.map_err(aster_error);
    let json = json?;
    shutdown?;
    let json =
        serde_json::to_string_pretty(&json).map_err(|error| CliError::run(error.to_string()))?;
    println!("{json}");
    Ok(())
}

async fn prompt_input(
    asterpath: &asterpath::Asterpath,
    session: SessionId,
) -> CliResult<serde_json::Value> {
    let entries = crate::drive::scan(asterpath.store(), session)
        .await
        .map_err(CliError::run)?;
    if entries.is_empty() {
        return Err(not_found(session));
    }
    let handle = asterpath.open_session(session).await.map_err(aster_error)?;
    let facts = handle.facts().await.map_err(aster_error)?;
    let items: Vec<Item> = handle
        .projection()
        .await
        .map_err(aster_error)?
        .into_iter()
        .flat_map(|projected| projected.items)
        .collect();
    // shortcut: open intents print as Debug text; serialize them once OpenIntent derives Serialize
    let open: Vec<String> = facts
        .open_intents()
        .iter()
        .map(|intent| format!("{intent:?}"))
        .collect();
    Ok(serde_json::json!({ "items": items, "open_intents": open }))
}
