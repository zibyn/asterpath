//! `asterpath agents`: the agent profiles of the current work root, each
//! with the file that wins for its name (ADR-0051).

use asterpath_app::workspace::{agent_profiles, load_agent};

use crate::session::current_root;
use crate::{CliError, CliResult, GlobalArgs, config};

pub fn agents(global: &GlobalArgs) -> CliResult<()> {
    let settings = global.settings()?;
    config::validate(&settings).map_err(CliError::config)?;
    let root = current_root()?;
    let workspace = settings.workspace().map_err(CliError::config)?;
    // shortcut: no builtin agent profiles until #186 resolves the bundled directory
    for file in agent_profiles(&workspace, None, &root).map_err(CliError::config)? {
        let mut line = format!("{}\t{}", file.name, file.path.display());
        match load_agent(&file) {
            Ok(profile) => {
                if let Some(description) = profile.description {
                    line = format!("{line}\t{description}");
                }
            }
            Err(error) => line = format!("{line}\terror: {error}"),
        }
        println!("{line}");
    }
    Ok(())
}
