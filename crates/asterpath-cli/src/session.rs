//! Session assembly (packaging.md §4.6): the work root, the composition root,
//! and a new, continued (`--continue` / `-r`) or forked session.

use std::collections::BTreeMap;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use asterpath::{
    AsterError, Asterpath, AsterpathBuilder, AsterpathConfig, DefaultCompactor, OwnedSession,
    SessionHandle,
};
use asterpath_mcp::{McpServers, McpTransport, ServerStatus};
use asterpath_protocol::{
    CatalogEntry, ConfigPatch, ConfigSnapshot, EnvironmentAdapter, EnvironmentError, ModelId,
    OutputFormat, Part, ProviderCatalog, SandboxPolicy, SandboxProvider, SessionEnvironment,
    SessionId, SessionQuery, SpawnEnv, StorageError, Tool, decode_entry,
};
use asterpath_skills::{
    DEFAULT_INDEX_BUDGET, LoadSkillTool, SkillDiagnostic, SkillDiagnosticKind, SkillRoot,
    SkillRootLabel, discover, render_index,
};
use clap::Args;
use futures_util::future::BoxFuture;

use crate::config::{self, ApprovalMode, SandboxMode};
use crate::drive::scan;
use crate::{CliError, CliResult, GlobalArgs, aster_error, not_found, parse_session};

/// Host metadata keys this CLI writes (ADR-0032).
const CWD: &str = "cwd";
const NAME: &str = "name";
/// How many sessions the `-r` picker lists.
const PICKER_SIZE: usize = 20;

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

/// Resolve the work root, assemble the engine and open the session.
/// `output_format` is the `--output-schema` request constraint.
pub async fn open(
    global: &GlobalArgs,
    args: &SessionArgs,
    output_format: Option<OutputFormat>,
) -> CliResult<Opened> {
    args.check()?;
    let mut settings = global.settings()?;
    if let Some(mode) = args.approval {
        settings.approval = mode;
    }
    if let Some(mode) = args.sandbox {
        settings.set_sandbox(mode);
    }
    let explicit_root = args.cd.as_deref().map(work_root).transpose()?;

    if !args.continuing() {
        if let Some(model) = &args.model {
            settings.set_model(model.clone());
        }
        config::validate(&settings).map_err(CliError::config)?;
        let root = explicit_root.map_or_else(current_root, Ok)?;
        return create(&settings, &root, args.name.as_deref(), output_format).await;
    }

    config::validate(&settings).map_err(CliError::config)?;
    let match_root = explicit_root.clone().map_or_else(current_root, Ok)?;
    let (target, snapshot) = find(&settings, args, &match_root).await?;
    // The session's own directory unless `-C` says otherwise.
    let root = match explicit_root {
        Some(root) => root,
        None => match target.metadata.get(CWD) {
            Some(cwd) => work_root(Path::new(cwd))?,
            None => match_root,
        },
    };

    // The session's model provider comes from its log; the file cannot move it.
    settings.set_provider(snapshot.model.provider.as_str().to_owned());
    let mcp = load_mcp(&settings, &root).await?;
    report_mcp_failures(&settings, &mcp)?;
    // `-C`, or the directory matched for a session without `cwd`, wins.
    let pinned =
        (target.metadata.get(CWD) != Some(&root.display().to_string())).then(|| root.clone());
    let environment = WorkRoot {
        settings: settings.clone(),
        new: false,
        pinned,
    };
    let asterpath = assemble(&settings, mcp.tools(), environment).await?;
    let (handle, continued) = if args.fork_session {
        let metadata = metadata(&root, args.name.as_deref());
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

/// A new session: workspace tools, skills, `AGENTS.md` and host metadata.
async fn create(
    settings: &config::Settings,
    root: &Path,
    name: Option<&str>,
    output_format: Option<OutputFormat>,
) -> CliResult<Opened> {
    // An unknown `[tools] enabled` name fails before anything starts.
    workspace_tools(settings, root, settings.tool_names())?;
    let mut skills = skill_setup(settings, root);
    for diagnostic in skills
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.kind != SkillDiagnosticKind::AuthorityFieldIgnored)
    {
        eprintln!("skill: {diagnostic}");
    }
    let mut spec = config::session_spec(settings).map_err(CliError::config)?;
    let mcp = load_mcp(settings, root).await?;
    report_mcp_failures(settings, &mcp)?;
    if let Some(agents) = agents_md(root)? {
        spec.system.push(agents);
    }
    if let Some(index) = skills.index.take() {
        spec.system.push(index);
    }
    spec.request.output_format = output_format;
    spec.metadata = metadata(root, name);
    let environment = WorkRoot {
        settings: settings.clone(),
        new: true,
        pinned: None,
    };
    let asterpath = assemble(settings, mcp.tools(), environment).await?;
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

fn metadata(root: &Path, name: Option<&str>) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::from([(CWD.to_owned(), root.display().to_string())]);
    if let Some(name) = name {
        metadata.insert(NAME.to_owned(), name.to_owned());
    }
    metadata
}

/// `<root>/AGENTS.md` only: no parent directories, no user-level file.
fn agents_md(root: &Path) -> CliResult<Option<Part>> {
    let path = root.join("AGENTS.md");
    if !path.is_file() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|error| CliError::config(format!("cannot read {}: {error}", path.display())))?;
    Ok(Some(Part::Text { text }))
}

/// A directory made absolute and canonical; missing is a configuration error.
fn work_root(dir: &Path) -> CliResult<PathBuf> {
    dir.canonicalize()
        .map_err(|error| CliError::config(format!("work root {}: {error}", dir.display())))
}

pub fn current_root() -> CliResult<PathBuf> {
    let cwd = std::env::current_dir()
        .map_err(|error| CliError::config(format!("cannot read working directory: {error}")))?;
    work_root(&cwd)
}

/// The CLI's session environment (ADR-0037): workspace tools, the skill
/// loader and the sandbox policy rooted at the root session's `cwd`, and
/// the `[sandbox] env` variables for bash.
struct WorkRoot {
    settings: config::Settings,
    /// New sessions get `[tools] enabled` and the skill switch; continued
    /// ones every workspace tool and the skill loader, so whatever their
    /// effective config names still resolves.
    new: bool,
    /// Overrides the `cwd` metadata.
    pinned: Option<PathBuf>,
}

impl WorkRoot {
    fn tools(&self, root: &Path) -> CliResult<Vec<Arc<dyn Tool>>> {
        let selection = if self.new {
            self.settings.tool_names()
        } else {
            None
        };
        let mut tools = workspace_tools(&self.settings, root, selection)?;
        if !self.new || self.settings.skills_enabled() {
            tools.push(Arc::new(LoadSkillTool::new(skill_roots(
                &self.settings,
                root,
            ))));
        }
        Ok(tools)
    }
}

impl EnvironmentAdapter for WorkRoot {
    fn resolve<'a>(
        &'a self,
        metadata: &'a BTreeMap<String, String>,
    ) -> BoxFuture<'a, Result<SessionEnvironment, EnvironmentError>> {
        Box::pin(async move {
            let root = match (&self.pinned, metadata.get(CWD)) {
                (Some(root), _) => root.clone(),
                (None, Some(cwd)) => PathBuf::from(cwd),
                (None, None) => return Err(EnvironmentError::new("the session has no cwd")),
            };
            let tools = self
                .tools(&root)
                .map_err(|error| EnvironmentError::new(error.message))?;
            let mut spawn = SpawnEnv::default();
            spawn.vars = self.settings.spawn_vars();
            Ok(SessionEnvironment::new(tools, spawn))
        })
    }
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
        let sessions = catalog(&asterpath).await?;
        let mut here = in_root(&sessions, root);
        let target = match args.resume.as_deref() {
            None => here.into_iter().next().ok_or_else(|| {
                CliError::run(format!("no session to continue in {}", root.display()))
            })?,
            Some("") => {
                here.truncate(PICKER_SIZE);
                pick(&here, io::stdin().lock(), io::stderr())?
            }
            Some(value) => by_id_or_name(&sessions, root, value)?,
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

/// Every catalog entry.
async fn catalog(asterpath: &Asterpath) -> CliResult<Vec<CatalogEntry>> {
    let mut all = Vec::new();
    let mut query = SessionQuery {
        limit: Some(256),
        ..SessionQuery::default()
    };
    loop {
        let page = asterpath.sessions(query).await.map_err(aster_error)?;
        let Some(last) = page.last() else {
            return Ok(all);
        };
        query.after = Some(last.cursor());
        all.extend(page);
    }
}

/// Sessions created in `root`, last active first.
// shortcut: reads the whole catalog and sorts by last_activity here; move to a store-side filter when catalogs grow large
fn in_root(sessions: &[CatalogEntry], root: &Path) -> Vec<CatalogEntry> {
    let root = root.display().to_string();
    let mut here: Vec<CatalogEntry> = sessions
        .iter()
        .filter(|entry| entry.metadata.get(CWD) == Some(&root))
        .cloned()
        .collect();
    here.sort_by_key(|entry| std::cmp::Reverse(entry.last_activity));
    here
}

/// A hex id (any root), else the last active session of that name in `root`.
fn by_id_or_name(sessions: &[CatalogEntry], root: &Path, value: &str) -> CliResult<CatalogEntry> {
    if let Ok(id) = parse_session(value) {
        return sessions
            .iter()
            .find(|entry| entry.session == id)
            .cloned()
            .ok_or_else(|| not_found(id));
    }
    in_root(sessions, root)
        .into_iter()
        .find(|entry| entry.metadata.get(NAME).map(String::as_str) == Some(value))
        .ok_or_else(|| CliError::run(format!("no session named {value} in {}", root.display())))
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

struct SkillSetup {
    index: Option<Part>,
    diagnostics: Vec<SkillDiagnostic>,
}

/// New-session skill policy: `[skills] enabled`. The loader tool comes from
/// the session environment.
fn skill_setup(settings: &config::Settings, root: &Path) -> SkillSetup {
    if !settings.skills_enabled() {
        return SkillSetup {
            index: None,
            diagnostics: Vec::new(),
        };
    }
    let discovery = discover(&skill_roots(settings, root));
    let (index, budget) = render_index(&discovery.skills, DEFAULT_INDEX_BUDGET);
    SkillSetup {
        index,
        diagnostics: discovery.diagnostics.into_iter().chain(budget).collect(),
    }
}

/// Explicit paths, then the project and user roots in priority order.
pub fn skill_roots(settings: &config::Settings, root: &Path) -> Vec<SkillRoot> {
    let mut roots: Vec<SkillRoot> = settings
        .skill_paths()
        .iter()
        .map(|path| SkillRoot {
            path: path.clone(),
            label: SkillRootLabel::Explicit,
        })
        .collect();
    roots.push(SkillRoot {
        path: root.join(".asterpath/skills"),
        label: SkillRootLabel::ProjectNative,
    });
    roots.push(SkillRoot {
        path: root.join(".agents/skills"),
        label: SkillRootLabel::ProjectAgents,
    });
    if let Some(home) = config::config_home() {
        roots.push(SkillRoot {
            path: home.join("asterpath/skills"),
            label: SkillRootLabel::UserNative,
        });
    }
    if let Some(home) = std::env::var_os("HOME") {
        roots.push(SkillRoot {
            path: PathBuf::from(home).join(".agents/skills"),
            label: SkillRootLabel::UserAgents,
        });
    }
    roots
}

/// `asterpath-tools` rooted at `root` under the `[sandbox]` policy,
/// filtered to `selection` (`None` = all) in the order given.
pub fn workspace_tools(
    settings: &config::Settings,
    root: &Path,
    selection: Option<&[String]>,
) -> CliResult<Vec<Arc<dyn Tool>>> {
    let policy = settings.sandbox_policy(root);
    let all = asterpath_tools::coding(
        root.to_path_buf(),
        sandbox_provider(&policy),
        policy,
        asterpath_tools::DEFAULT_BASH_TIMEOUT,
    )
    .map_err(|error| CliError::config(error.to_string()))?;
    let Some(selection) = selection else {
        return Ok(all);
    };
    selection
        .iter()
        .map(|name| {
            all.iter()
                .find(|tool| tool.descriptor().name.as_str() == name)
                .cloned()
                .ok_or_else(|| CliError::config(format!("unknown tool {name}")))
        })
        .collect()
}

/// The composition root. No approval endpoint is registered here: a command
/// that can ask the operator registers one around its drive.
async fn assemble(
    settings: &config::Settings,
    tools: Vec<Arc<dyn Tool>>,
    environment: WorkRoot,
) -> CliResult<Asterpath> {
    let providers = config::providers(settings).map_err(CliError::config)?;
    // Sessions without a context window still report `NotConfigured`.
    let catalog = ProviderCatalog::new(providers.clone())
        .map_err(|error| CliError::config(error.to_string()))?;
    let config = AsterpathConfig::default();
    let approval = config::approval_policy(settings.approval, Arc::clone(&config.gate));
    AsterpathBuilder::from_config(config)
        .compactor(Arc::new(DefaultCompactor::new(Arc::new(catalog))))
        .database(&settings.db)
        .providers(providers)
        .tools(tools)
        .environment(Arc::new(environment))
        .tool(config::tool_config(settings))
        .max_live_children(config::max_live_children(settings))
        .model_aliases(config::model_aliases(settings))
        .approval(approval)
        .build()
        .await
        .map_err(aster_error)
}

/// Start every configured MCP server under the `[sandbox]` policy; a stdio
/// server without `cwd` starts in `root`. A `required` failure is a
/// configuration error.
pub async fn load_mcp(settings: &config::Settings, root: &Path) -> CliResult<McpServers> {
    let mut configs = settings.mcp_servers().map_err(CliError::config)?;
    for config in &mut configs {
        if let McpTransport::Stdio {
            cwd: cwd @ None, ..
        } = &mut config.transport
        {
            *cwd = Some(root.to_path_buf());
        }
    }
    let policy = settings.sandbox_policy(root);
    McpServers::load(configs, sandbox_provider(&policy), policy)
        .await
        .map_err(|error| CliError::config(error.to_string()))
}

/// bwrap on Linux. Elsewhere a restricted policy has no backend, so stdio
/// servers fail closed at the sandbox stage unless the mode is `full-access`.
fn sandbox_provider(policy: &SandboxPolicy) -> Option<Arc<dyn SandboxProvider>> {
    #[cfg(target_os = "linux")]
    if !matches!(policy, SandboxPolicy::FullAccess) {
        return Some(Arc::new(asterpath_sandbox_linux::LinuxSandbox::new()));
    }
    let _ = policy;
    None
}

/// One `mcp: <id> ...` line per configured server, in config order.
pub fn mcp_status_lines(settings: &config::Settings, mcp: &McpServers) -> CliResult<Vec<String>> {
    let configs = settings.mcp_servers().map_err(CliError::config)?;
    Ok(configs
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
        .collect())
}

/// Optional servers that failed are reported and skipped.
fn report_mcp_failures(settings: &config::Settings, mcp: &McpServers) -> CliResult<()> {
    let lines = mcp_status_lines(settings, mcp)?;
    for (line, status) in lines.iter().zip(mcp.statuses()) {
        if !matches!(status, ServerStatus::Loaded { .. }) {
            eprintln!("{line}");
        }
    }
    Ok(())
}

/// No providers and no drive: enough for the catalog and log-derived views.
pub async fn bare(settings: &config::Settings) -> CliResult<Asterpath> {
    Asterpath::builder()
        .database(&settings.db)
        .build()
        .await
        .map_err(aster_error)
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn in_root_keeps_the_root_and_puts_the_last_active_first() {
        let sessions = [
            entry(1, 10, &[(CWD, "/a")]),
            entry(2, 30, &[(CWD, "/a")]),
            entry(3, 50, &[(CWD, "/b")]),
            entry(4, 60, &[]),
        ];
        let here = in_root(&sessions, Path::new("/a"));
        let ids: Vec<_> = here.iter().map(|entry| entry.session).collect();
        assert_eq!(
            ids,
            [
                SessionId::from_bytes([2; 16]),
                SessionId::from_bytes([1; 16])
            ]
        );
    }

    #[test]
    fn a_name_resolves_to_its_last_active_session_in_the_root_and_an_id_to_itself() {
        let sessions = [
            entry(1, 10, &[(NAME, "fix"), (CWD, "/a")]),
            entry(2, 30, &[(NAME, "fix"), (CWD, "/a")]),
            entry(3, 50, &[(NAME, "other"), (CWD, "/a")]),
            entry(4, 70, &[(NAME, "fix"), (CWD, "/b")]),
        ];
        let root = Path::new("/a");
        assert_eq!(
            by_id_or_name(&sessions, root, "fix").unwrap().session,
            SessionId::from_bytes([2; 16])
        );
        let hex = SessionId::from_bytes([4; 16]).to_hex();
        assert_eq!(
            by_id_or_name(&sessions, root, &hex).unwrap().session,
            SessionId::from_bytes([4; 16]),
            "an id resolves outside the root"
        );
        assert_eq!(by_id_or_name(&sessions, root, "nope").unwrap_err().code, 1);
        assert_eq!(
            by_id_or_name(&sessions, Path::new("/c"), "fix")
                .unwrap_err()
                .code,
            1,
            "a name resolves only in the root"
        );
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
