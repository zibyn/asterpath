//! `asterpath agents` and `asterpath styles`: the agent profiles (ADR-0051)
//! and output styles (ADR-0052) of the current work root, each with the file
//! that wins for its name.

use asterpath_app::workspace::{
    AgentFile, WorkspaceError, agent_profiles, load_agent, load_style, style_files,
};

use crate::session::{builtin_agents, current_root};
use crate::{CliError, CliResult, GlobalArgs, config};

pub fn agents(global: &GlobalArgs) -> CliResult<()> {
    list(global, agent_profiles, |file| {
        Ok(load_agent(file)?.description)
    })
}

pub fn styles(global: &GlobalArgs) -> CliResult<()> {
    list(
        global,
        style_files,
        |file| Ok(load_style(file)?.description),
    )
}

/// One `name\tpath[\tdescription]` line per file, or `\terror: …` when the
/// file does not load.
fn list(
    global: &GlobalArgs,
    files: impl Fn(
        &asterpath_app::workspace::Workspace,
        Option<&std::path::Path>,
        &std::path::Path,
    ) -> Result<Vec<AgentFile>, WorkspaceError>,
    description: impl Fn(&AgentFile) -> Result<Option<String>, WorkspaceError>,
) -> CliResult<()> {
    let settings = global.settings()?;
    config::validate(&settings).map_err(CliError::config)?;
    let root = current_root()?;
    let workspace = settings.workspace().map_err(CliError::config)?;
    for file in files(&workspace, builtin_agents().as_deref(), &root).map_err(CliError::config)? {
        let mut line = format!("{}\t{}", file.name, file.path.display());
        match description(&file) {
            Ok(Some(description)) => line = format!("{line}\t{description}"),
            Ok(None) => {}
            Err(error) => line = format!("{line}\terror: {error}"),
        }
        println!("{line}");
    }
    Ok(())
}
