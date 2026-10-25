//! `asterpath doctor`: validate the configuration without opening the
//! database or the engine.

use asterpath_skills::{DEFAULT_INDEX_BUDGET, discover, render_index};

use crate::session::{current_root, load_mcp, mcp_status_lines, skill_roots, workspace_tools};
use crate::{CliError, CliResult, GlobalArgs, config};

pub async fn doctor(global: &GlobalArgs) -> CliResult<()> {
    let settings = global.settings()?;
    config::validate(&settings).map_err(CliError::config)?;
    // The `enabled` switch only governs new sessions, so diagnostics are
    // always available here.
    let root = current_root()?;
    let discovery = discover(&skill_roots(&settings, &root));
    let (_, budget) = render_index(&discovery.skills, DEFAULT_INDEX_BUDGET);
    for diagnostic in discovery.diagnostics.iter().chain(&budget) {
        println!("skill: {diagnostic}");
    }
    workspace_tools(&settings, &root, settings.tool_names())?;
    // Loading is the only way to report a server's status; stop them again.
    let mcp = load_mcp(&settings, &root).await?;
    for line in mcp_status_lines(&settings, &mcp)? {
        println!("{line}");
    }
    mcp.shutdown().await;
    println!("config ok");
    Ok(())
}
