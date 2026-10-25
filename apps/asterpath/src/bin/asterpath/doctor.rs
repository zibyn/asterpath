//! `asterpath doctor`: validate the configuration without opening the
//! database or the engine.

use asterpath_skills::{DEFAULT_INDEX_BUDGET, discover, render_index};

use asterpath_app::workspace::{load_mcp, skill_roots, workspace_tools};

use crate::session::{current_root, mcp_status_lines};
use crate::{CliError, CliResult, GlobalArgs, config};

pub async fn doctor(global: &GlobalArgs) -> CliResult<()> {
    let settings = global.settings()?;
    config::validate(&settings).map_err(CliError::config)?;
    // The `enabled` switch only governs new sessions, so diagnostics are
    // always available here.
    let root = current_root()?;
    let workspace = settings.workspace().map_err(CliError::config)?;
    let discovery = discover(&skill_roots(&workspace, &root));
    let (_, budget) = render_index(&discovery.skills, DEFAULT_INDEX_BUDGET);
    for diagnostic in discovery.diagnostics.iter().chain(&budget) {
        println!("skill: {diagnostic}");
    }
    workspace_tools(&workspace, &root, workspace.tools.as_deref()).map_err(CliError::config)?;
    // Loading is the only way to report a server's status; stop them again.
    let mcp = load_mcp(&workspace, &root)
        .await
        .map_err(CliError::config)?;
    for line in mcp_status_lines(&workspace, &mcp) {
        println!("{line}");
    }
    mcp.shutdown().await;
    println!("config ok");
    Ok(())
}
