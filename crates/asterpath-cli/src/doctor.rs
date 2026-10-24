//! `asterpath doctor`: validate the configuration without opening the
//! database or the engine.

use asterpath_skills::{DEFAULT_INDEX_BUDGET, discover, render_index};

use crate::session::{load_mcp, mcp_status_lines, skill_roots, workspace_tools};
use crate::{CliError, CliResult, GlobalArgs, config};

pub async fn doctor(global: &GlobalArgs) -> CliResult<()> {
    let settings = global.settings()?;
    config::validate(&settings).map_err(CliError::config)?;
    // The `enabled` switch only governs new sessions, so diagnostics are
    // always available here.
    let discovery = discover(&skill_roots(&settings));
    let (_, budget) = render_index(&discovery.skills, DEFAULT_INDEX_BUDGET);
    for diagnostic in discovery.diagnostics.iter().chain(&budget) {
        println!("skill: {diagnostic}");
    }
    workspace_tools(&settings, settings.tool_names())?;
    // Loading is the only way to report a server's status; stop them again.
    let mcp = load_mcp(&settings).await?;
    for line in mcp_status_lines(&settings, &mcp)? {
        println!("{line}");
    }
    mcp.shutdown().await;
    println!("config ok");
    Ok(())
}
