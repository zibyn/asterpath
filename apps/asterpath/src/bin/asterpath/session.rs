//! Session assembly (packaging.md §4.6): the flags, the work root and a
//! new, continued (`--continue` / `-r`) or forked session. The work-root
//! assembly and metadata conventions are `asterpath_app::workspace`'s (ADR-0049).

use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use asterpath::{AsterError, Asterpath, OwnedSession, SessionHandle};
use asterpath_app::approval::ApprovalModes;
use asterpath_app::config;
use asterpath_app::workspace::{
    self, AGENT, NAME, STYLE, WorkRoot, Workspace, by_id_or_name, continued_root, in_root,
    load_mcp, metadata, new_session_spec, work_root, workspace_tools,
};
use asterpath_mcp::{McpServers, ServerStatus};
use asterpath_protocol::{
    CatalogEntry, ConfigPatch, ConfigSnapshot, ModelId, OutputFormat, SessionId, StorageError,
    decode_entry,
};
use asterpath_skills::SkillDiagnosticKind;
use clap::{Args, ValueEnum};

use crate::drive::scan;
use crate::{CliError, CliResult, GlobalArgs, aster_error, not_found};

/// How many sessions the `-r` picker lists.
const PICKER_SIZE: usize = 20;

/// `-a`: the library's `config::ApprovalMode`, kept free of clap.
#[derive(Clone, Copy, ValueEnum)]
pub enum ApprovalMode {
    Untrusted,
    Never,
}

impl From<ApprovalMode> for config::ApprovalMode {
    fn from(mode: ApprovalMode) -> Self {
        match mode {
            ApprovalMode::Untrusted => Self::Untrusted,
            ApprovalMode::Never => Self::Never,
        }
    }
}

/// `-s`: the library's `SandboxMode`.
#[derive(Clone, Copy, ValueEnum)]
pub enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
    FullAccess,
}

impl From<SandboxMode> for asterpath_app::workspace::SandboxMode {
    fn from(mode: SandboxMode) -> Self {
        match mode {
            SandboxMode::ReadOnly => Self::ReadOnly,
            SandboxMode::WorkspaceWrite => Self::WorkspaceWrite,
            SandboxMode::FullAccess => Self::FullAccess,
        }
    }
}

/// Options of the session-running commands (interactive and `exec`).
#[derive(Args)]
pub struct SessionArgs {
    /// Model id for a new session; on a continued one, switches the model.
    #[arg(short = 'm', long)]
    pub model: Option<String>,
    /// Approval policy.
    #[arg(short = 'a', long = "ask-for-approval", value_enum)]
    pub approval: Option<ApprovalMode>,
    /// Sandbox mode; overrides `[sandbox] mode`.
    #[arg(short = 's', long, value_enum)]
    pub sandbox: Option<SandboxMode>,
    /// Work root: tool root, writable root, `AGENTS.md` and session `cwd`.
    #[arg(short = 'C', long = "cd", value_name = "DIR")]
    pub cd: Option<PathBuf>,
    /// Continue the last active session of the work root.
    #[arg(long = "continue", conflicts_with = "resume")]
    pub continue_last: bool,
    /// Continue a session by id or name; without a value, pick one.
    #[arg(short = 'r', long, value_name = "ID|NAME", num_args = 0..=1, default_missing_value = "")]
    pub resume: Option<String>,
    /// Fork the continued session instead of continuing it.
    #[arg(long)]
    pub fork_session: bool,
    /// Name of a new session.
    #[arg(short = 'n', long)]
    pub name: Option<String>,
    /// Agent profile of a new session; overrides `default_agent`.
    #[arg(long, value_name = "NAME")]
    pub agent: Option<String>,
    /// Output style of a new session; overrides `output_style`.
    #[arg(long, value_name = "NAME")]
    pub style: Option<String>,
    /// Attach a file to the first input (repeatable).
    #[arg(short = 'i', long = "image", value_name = "FILE")]
    pub images: Vec<PathBuf>,
}

impl SessionArgs {
    pub fn continuing(&self) -> bool {
        self.continue_last || self.resume.is_some()
    }

    /// Flag combinations clap cannot express.
    fn check(&self) -> CliResult<()> {
        if self.fork_session && !self.continuing() {
            return Err(CliError::config("--fork-session needs --continue or -r"));
        }
        if self.name.is_some() && self.continuing() && !self.fork_session {
            return Err(CliError::config(
                "-n names a new session; it cannot rename one",
            ));
        }
        if self.agent.is_some() && self.continuing() {
            return Err(CliError::config(
                "--agent picks the profile of a new session; a continued one keeps its own",
            ));
        }
        if self.style.is_some() && self.continuing() {
            return Err(CliError::config(
                "--style picks the output style of a new session; a continued one keeps its own",
            ));
        }
        Ok(())
    }
}

/// A session ready for input, with the modules it was assembled from.
pub struct Opened {
    pub asterpath: Asterpath,
    pub mcp: McpServers,
    pub handle: SessionHandle,
    /// The claim of the session's tree; `None` when another process drives
    /// the continued session, which is then only viewed (ADR-0036).
    pub owned: Option<OwnedSession>,
    /// Whether the session existed before (continued, not created or forked):
    /// only then can it have work to recover.
    pub continued: bool,
}

impl Opened {
    /// Engine first, then the MCP servers its tools call (ADR-0027).
    pub async fn shutdown(self) -> CliResult<()> {
        drop((self.owned, self.handle));
        let shutdown = self.asterpath.shutdown().await;
        self.mcp.shutdown().await;
        shutdown.map_err(aster_error)
    }
}

/// Which front end opens a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Frontend {
    /// `output_style` does not apply: nobody takes over what a style leaves
    /// to the user.
    Exec,
    Interactive,
}

/// Resolve the work root, assemble the engine and open the session.
/// `output_format` is the `--output-schema` request constraint.
pub async fn open(
    global: &GlobalArgs,
    args: &SessionArgs,
    frontend: Frontend,
    output_format: Option<OutputFormat>,
) -> CliResult<Opened> {
    args.check()?;
    let mut settings = global.settings()?;
    if let Some(mode) = args.approval {
        settings.approval = mode.into();
    }
    if let Some(mode) = args.sandbox {
        settings.set_sandbox(mode.into());
    }
    let explicit_root = args
        .cd
        .as_deref()
        .map(work_root)
        .transpose()
        .map_err(CliError::config)?;

    if !args.continuing() {
        if let Some(model) = &args.model {
            settings.set_model(model.clone());
        }
        if let Some(agent) = &args.agent {
            settings.set_agent(agent.clone());
        }
        if args.style.is_some() || frontend == Frontend::Exec {
            settings.set_style(args.style.clone());
        }
        config::validate(&settings).map_err(CliError::config)?;
        let root = explicit_root.map_or_else(current_root, Ok)?;
        return create(settings, &root, args, output_format).await;
    }

    config::validate(&settings).map_err(CliError::config)?;
    let match_root = explicit_root.clone().map_or_else(current_root, Ok)?;
    let (target, snapshot) = find(&settings, args, &match_root).await?;
    // The session's own directory unless `-C` says otherwise.
    let (root, pinned) =
        continued_root(&target.metadata, explicit_root, match_root).map_err(CliError::config)?;

    // The session's model provider comes from its log; the file cannot move it.
    settings.set_provider(snapshot.model.provider.as_str().to_owned());
    let workspace = settings.workspace().map_err(CliError::config)?;
    let mcp = load_mcp(&workspace, &root)
        .await
        .map_err(CliError::config)?;
    report_mcp_failures(&workspace, &mcp);
    let approval = ApprovalModes::new(settings.approval.into())
        .with_sandbox(&workspace)
        .with_rules(settings.rules.clone());
    let environment = WorkRoot {
        workspace,
        new_session: false,
        pinned,
        builtin_agents: builtin_agents(),
    };
    let asterpath = config::assemble(&settings, mcp.tools(), environment, Arc::new(approval))
        .await
        .map_err(aster_error)?;
    let (handle, continued) = if args.fork_session {
        let mut metadata = metadata(&root, args.name.as_deref());
        // The fork keeps the parent's system, so it keeps its profile and
        // style names.
        for key in [AGENT, STYLE] {
            if let Some(value) = target.metadata.get(key) {
                metadata.insert(key.to_owned(), value.clone());
            }
        }
        let handle = asterpath
            .fork_session(target.session, metadata)
            .await
            .map_err(aster_error)?;
        (handle, false)
    } else {
        let handle = asterpath
            .open_session(target.session)
            .await
            .map_err(aster_error)?;
        (handle, true)
    };
    eprintln!("session: {}", handle.id().to_hex());
    let owned = match handle.claim().await {
        Ok(owned) => Some(owned),
        Err(AsterError::Storage(StorageError::OwnedElsewhere { .. })) if continued => {
            eprintln!("another process is driving this session; it is read-only here");
            None
        }
        Err(error) => return Err(aster_error(error)),
    };

    let mut patch = match &args.model {
        Some(model) => model_patch(&snapshot, model),
        None => ConfigPatch::default(),
    };
    if let Some(format) = output_format {
        let mut request = snapshot.request.clone();
        request.output_format = Some(format);
        patch.request = Some(request);
    }
    // A viewer leaves the owner's config alone.
    if patch != ConfigPatch::default() && owned.is_some() {
        handle.update_config(patch).await.map_err(aster_error)?;
    }
    Ok(Opened {
        asterpath,
        mcp,
        handle,
        owned,
        continued,
    })
}

/// Switch `snapshot`'s model id; the provider stays.
pub fn model_patch(snapshot: &ConfigSnapshot, model: &str) -> ConfigPatch {
    let mut spec = snapshot.model.clone();
    spec.model = ModelId::new(model.to_owned());
    let mut patch = ConfigPatch::default();
    patch.model = Some(spec);
    patch
}

/// A new session: the agent profile, workspace tools, skills, `AGENTS.md`
/// and session metadata.
async fn create(
    mut settings: config::Settings,
    root: &Path,
    args: &SessionArgs,
    output_format: Option<OutputFormat>,
) -> CliResult<Opened> {
    let workspace = settings.workspace().map_err(CliError::config)?;
    // An unknown `[tools] enabled` name fails before anything starts.
    workspace_tools(&workspace, root, workspace.tools.as_deref()).map_err(CliError::config)?;
    let mut spec = config::session_spec(&settings).map_err(CliError::config)?;
    let configured = spec.model.clone();
    let builtin = builtin_agents();
    let diagnostics = new_session_spec(
        &workspace,
        builtin.as_deref(),
        root,
        args.name.as_deref(),
        &mut spec,
    )
    .map_err(CliError::config)?;
    // `-m` wins over the profile's model.
    if args.model.is_some() {
        spec.model = configured;
    }
    // A profile may pin another provider; the engine must have it.
    if spec.model.provider.as_str() != settings.provider().map_err(CliError::config)? {
        settings.set_provider(spec.model.provider.as_str().to_owned());
        config::validate(&settings).map_err(CliError::config)?;
    }
    let settings = &settings;
    for diagnostic in diagnostics
        .skills
        .iter()
        .filter(|diagnostic| diagnostic.kind != SkillDiagnosticKind::AuthorityFieldIgnored)
    {
        eprintln!("skill: {diagnostic}");
    }
    for diagnostic in &diagnostics.agents {
        eprintln!("agent: {diagnostic}; not offered to sub-agents");
    }
    let mcp = load_mcp(&workspace, root).await.map_err(CliError::config)?;
    report_mcp_failures(&workspace, &mcp);
    spec.request.output_format = output_format;
    let approval = ApprovalModes::new(settings.approval.into())
        .with_sandbox(&workspace)
        .with_rules(settings.rules.clone());
    let environment = WorkRoot {
        workspace,
        new_session: true,
        pinned: None,
        builtin_agents: builtin,
    };
    let asterpath = config::assemble(settings, mcp.tools(), environment, Arc::new(approval))
        .await
        .map_err(aster_error)?;
    let handle = asterpath.create_session(spec).await.map_err(aster_error)?;
    eprintln!("session: {}", handle.id().to_hex());
    let owned = Some(handle.claim().await.map_err(aster_error)?);
    Ok(Opened {
        asterpath,
        mcp,
        handle,
        owned,
        continued: false,
    })
}

pub fn current_root() -> CliResult<PathBuf> {
    let cwd = std::env::current_dir()
        .map_err(|error| CliError::config(format!("cannot read working directory: {error}")))?;
    work_root(&cwd).map_err(CliError::config)
}

/// The builtin agent directory: `agents/` beside the binary, else
/// `<prefix>/share/asterpath/agents` for a binary in `<prefix>/bin/`.
pub fn builtin_agents() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok();
    let bin = exe.as_deref().and_then(Path::parent);
    workspace::builtin_agents(bin.into_iter().flat_map(|bin| {
        [bin.join("agents")].into_iter().chain(
            bin.parent()
                .map(|prefix| prefix.join("share/asterpath/agents")),
        )
    }))
}

/// The session `--continue` or `-r` names, and its effective config.
async fn find(
    settings: &config::Settings,
    args: &SessionArgs,
    root: &Path,
) -> CliResult<(CatalogEntry, ConfigSnapshot)> {
    let picker = args.resume.as_deref() == Some("");
    if picker && !(io::stdin().is_terminal() && io::stderr().is_terminal()) {
        return Err(CliError::config(
            "-r without a value picks interactively and needs a terminal",
        ));
    }
    if !settings.db.exists() {
        return Err(CliError::run("no sessions"));
    }
    let asterpath = bare(settings).await?;
    let found = async {
        let mut here = in_root(&asterpath, root).await.map_err(CliError::run)?;
        let target = match args.resume.as_deref() {
            None => here.into_iter().next().ok_or_else(|| {
                CliError::run(format!("no session to continue in {}", root.display()))
            })?,
            Some("") => {
                here.truncate(PICKER_SIZE);
                pick(&here, io::stdin().lock(), io::stderr())?
            }
            Some(value) => by_id_or_name(&asterpath, root, value)
                .await
                .map_err(CliError::run)?,
        };
        let snapshot = snapshot(&asterpath, target.session).await?;
        Ok((target, snapshot))
    }
    .await;
    let shutdown = asterpath.shutdown().await.map_err(aster_error);
    let found = found?;
    shutdown?;
    Ok(found)
}

/// List the candidates and read a number.
fn pick<R: BufRead, W: Write>(
    sessions: &[CatalogEntry],
    mut reader: R,
    mut writer: W,
) -> CliResult<CatalogEntry> {
    if sessions.is_empty() {
        return Err(CliError::run("no session to resume here"));
    }
    for (index, entry) in sessions.iter().enumerate() {
        let name = entry.metadata.get(NAME).map(String::as_str).unwrap_or("");
        let _ = writeln!(
            writer,
            "{:>2}. {} {name}",
            index + 1,
            entry.session.to_hex()
        );
    }
    let _ = write!(writer, "session number: ");
    let _ = writer.flush();
    let mut answer = String::new();
    let _ = reader.read_line(&mut answer);
    answer
        .trim()
        .parse::<usize>()
        .ok()
        .and_then(|number| sessions.get(number.checked_sub(1)?))
        .cloned()
        .ok_or_else(|| CliError::config(format!("no session number {}", answer.trim())))
}

/// The effective config at the end of the log: what the next step runs with.
pub async fn snapshot(asterpath: &Asterpath, session: SessionId) -> CliResult<ConfigSnapshot> {
    let entries = scan(asterpath.store(), session)
        .await
        .map_err(CliError::run)?;
    let decoded: Vec<_> = entries
        .into_iter()
        .filter_map(|entry| {
            let event = decode_entry(&entry).ok()?;
            Some((entry, event))
        })
        .collect();
    asterpath_protocol::effective_config(&decoded, u64::MAX).ok_or_else(|| not_found(session))
}

/// One `mcp: <id> ...` line per configured server, in config order.
pub fn mcp_status_lines(workspace: &Workspace, mcp: &McpServers) -> Vec<String> {
    workspace
        .mcp_servers
        .iter()
        .zip(mcp.statuses())
        .map(|(config, status)| match status {
            ServerStatus::Loaded { tools } => {
                let mut names: Vec<&str> = tools.iter().map(|tool| tool.as_str()).collect();
                names.sort_unstable();
                format!("mcp: {} loaded: {}", config.id, names.join(", "))
            }
            ServerStatus::Failed { stage, message } => {
                format!("mcp: {} failed at {stage:?}: {message}", config.id)
            }
            other => format!("mcp: {} {other:?}", config.id),
        })
        .collect()
}

/// Optional servers that failed are reported and skipped.
fn report_mcp_failures(workspace: &Workspace, mcp: &McpServers) {
    let lines = mcp_status_lines(workspace, mcp);
    for (line, status) in lines.iter().zip(mcp.statuses()) {
        if !matches!(status, ServerStatus::Loaded { .. }) {
            eprintln!("{line}");
        }
    }
}

/// No providers and no drive: enough for the catalog and log-derived views.
pub async fn bare(settings: &config::Settings) -> CliResult<Asterpath> {
    config::create_db_dir(settings).map_err(aster_error)?;
    Asterpath::builder()
        .database(&settings.db)
        .build()
        .await
        .map_err(aster_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use asterpath_protocol::SessionId;

    fn entry(byte: u8, last_activity: i64, metadata: &[(&str, &str)]) -> CatalogEntry {
        let mut entry = CatalogEntry::new(
            SessionId::from_bytes([byte; 16]),
            None,
            0,
            last_activity,
            BTreeMap::new(),
        );
        for (key, value) in metadata {
            entry
                .metadata
                .insert((*key).to_owned(), (*value).to_owned());
        }
        entry
    }

    #[test]
    fn the_picker_lists_numbered_sessions_and_reads_a_choice() {
        let sessions = [entry(1, 10, &[(NAME, "first")]), entry(2, 5, &[])];
        let mut shown = Vec::new();
        let chosen = pick(&sessions, Cursor::new(b"2\n"), &mut shown).unwrap();
        assert_eq!(chosen.session, SessionId::from_bytes([2; 16]));
        let shown = String::from_utf8(shown).unwrap();
        assert!(
            shown.contains(" 1. 01010101010101010101010101010101 first"),
            "{shown}"
        );
        for answer in ["0\n", "3\n", "x\n", ""] {
            assert_eq!(
                pick(&sessions, Cursor::new(answer.as_bytes()), Vec::new())
                    .unwrap_err()
                    .code,
                2
            );
        }
    }
}
