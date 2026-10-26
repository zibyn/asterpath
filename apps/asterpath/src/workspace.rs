//! Work-root assembly (ADR-0049): how a session is assembled from its work
//! root, and the session metadata conventions that let the CLI and the
//! desktop window continue each other's sessions.
//!
//! The assembly takes plain values and prints nothing; [`crate::config`]
//! fills them from `config.toml`.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use asterpath::{AsterError, Asterpath};
use asterpath_mcp::{McpError, McpServerConfig, McpServers, McpTransport};
use asterpath_protocol::{
    CatalogEntry, EnvironmentAdapter, EnvironmentError, ModelId, ModelSpec, ParentFilter, Part,
    ProviderId, SandboxPolicy, SandboxProvider, SessionEnvironment, SessionId, SessionQuery,
    SessionSpec, SpawnEnv, Tool, ToolName, ToolSelection,
};
use asterpath_skills::{
    DEFAULT_INDEX_BUDGET, FrontmatterValue, LoadSkillTool, SkillDiagnostic, SkillRoot,
    SkillRootLabel, discover, parse_frontmatter, render_index,
};
use futures_util::future::BoxFuture;

/// What the work-root assembly and the configuration fail with; every
/// variant displays as a message ready to show.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    /// `config.toml`, an override, a tool name or another application value
    /// is invalid.
    #[error("{0}")]
    Config(String),
    /// A file or directory the application needs cannot be read.
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    /// No session matches.
    #[error("{0}")]
    NotFound(String),
    /// A `required` MCP server did not load.
    #[error(transparent)]
    Mcp(#[from] McpError),
    /// The engine or its store failed (boxed: `AsterError` is large).
    #[error(transparent)]
    Engine(Box<AsterError>),
}

impl From<AsterError> for WorkspaceError {
    fn from(error: AsterError) -> Self {
        Self::Engine(Box::new(error))
    }
}

/// Session metadata keys (ADR-0032): the work root, the session name and the
/// agent profile it was created with (ADR-0051).
pub const CWD: &str = "cwd";
pub const NAME: &str = "name";
pub const AGENT: &str = "agent";
/// The output style a session was created with (ADR-0052).
pub const STYLE: &str = "style";
/// The agent profile of new sessions when none is configured (ADR-0051).
pub const DEFAULT_AGENT: &str = "default";

/// `[sandbox] mode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    ReadOnly,
    #[default]
    WorkspaceWrite,
    FullAccess,
}

/// What a work root contributes, independent of where the values come from.
#[derive(Clone, Debug)]
pub struct Workspace {
    pub sandbox: SandboxMode,
    /// Workspace tools of new sessions, in order; `None` means all of them.
    pub tools: Option<Vec<String>>,
    /// Skills of new sessions; continued ones always get the loader.
    pub skills_enabled: bool,
    /// Explicit skill roots, highest priority first.
    pub skill_paths: Vec<PathBuf>,
    /// User-level skill roots, searched after the project roots.
    pub user_skill_roots: Vec<SkillRoot>,
    /// Variables passed on to bash (ADR-0037).
    pub spawn_vars: BTreeMap<OsString, OsString>,
    pub mcp_servers: Vec<McpServerConfig>,
    /// The agent profile of new sessions; `None` means no base prompt.
    pub agent: Option<String>,
    /// User-level agent profiles, between the builtin and project ones.
    pub user_agents: Option<PathBuf>,
    /// The output style of new sessions (ADR-0052); `None` adds none.
    pub style: Option<String>,
    /// User-level output styles, between the builtin and project ones.
    pub user_styles: Option<PathBuf>,
}

impl Default for Workspace {
    fn default() -> Self {
        Self {
            sandbox: SandboxMode::default(),
            tools: None,
            skills_enabled: true,
            skill_paths: Vec::new(),
            user_skill_roots: Vec::new(),
            spawn_vars: BTreeMap::new(),
            mcp_servers: Vec::new(),
            agent: None,
            user_agents: None,
            style: None,
            user_styles: None,
        }
    }
}

impl Workspace {
    /// `workspace-write` makes the work root the only writable root.
    pub fn sandbox_policy(&self, root: &Path) -> SandboxPolicy {
        match self.sandbox {
            SandboxMode::ReadOnly => SandboxPolicy::ReadOnly,
            SandboxMode::WorkspaceWrite => SandboxPolicy::WorkspaceWrite {
                writable_roots: vec![root.to_path_buf()],
            },
            SandboxMode::FullAccess => SandboxPolicy::FullAccess,
        }
    }
}

/// The session metadata of a new or forked session.
pub fn metadata(root: &Path, name: Option<&str>) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::from([(CWD.to_owned(), root.display().to_string())]);
    if let Some(name) = name {
        metadata.insert(NAME.to_owned(), name.to_owned());
    }
    metadata
}

/// A new session's spec from its work root: the selected agent profile's
/// prompt, the output style, `AGENTS.md`, then the skill index, appended to
/// `spec.system`; the profile's model and tools; and the session metadata.
/// `builtin_agents` is the bundled profile directory, if any; the bundled
/// styles are its sibling `styles`. Returns the skill diagnostics for
/// the caller to show.
pub fn new_session_spec(
    workspace: &Workspace,
    builtin_agents: Option<&Path>,
    root: &Path,
    name: Option<&str>,
    spec: &mut SessionSpec,
) -> Result<Vec<SkillDiagnostic>, WorkspaceError> {
    let profiles = agent_profiles(workspace, builtin_agents, root)?;
    let agent = match selected_agent(workspace, &profiles)? {
        Some(file) => Some((file.name.clone(), load_agent(file)?)),
        None => None,
    };
    let style = selected_style(workspace, builtin_agents, root)?;
    let (index, diagnostics) = skill_index(workspace, root);
    if let Some((_, profile)) = &agent {
        if !profile.prompt.is_empty() {
            spec.system.push(Part::Text {
                text: profile.prompt.clone(),
            });
        }
        if let Some((provider, model)) = &profile.model {
            // A different model does not inherit the configured context window.
            let same =
                spec.model.provider.as_str() == provider && spec.model.model.as_str() == model;
            spec.model = ModelSpec {
                provider: ProviderId::new(provider.clone()),
                model: ModelId::new(model.clone()),
                context_window: spec.model.context_window.filter(|_| same),
            };
        }
        if let Some(tools) = &profile.tools {
            spec.tools = ToolSelection::Names(tools.iter().cloned().map(ToolName::new).collect());
        }
    }
    if let Some((_, style)) = &style
        && !style.prompt.is_empty()
    {
        spec.system.push(Part::Text {
            text: format!("{STYLE_FRAMING}\n\n{}", style.prompt),
        });
    }
    if let Some(agents) = agents_md(root)? {
        spec.system.push(agents);
    }
    if let Some(index) = index {
        spec.system.push(index);
    }
    spec.metadata = metadata(root, name);
    if let Some((name, _)) = agent {
        spec.metadata.insert(AGENT.to_owned(), name);
    }
    if let Some((name, _)) = style {
        spec.metadata.insert(STYLE.to_owned(), name);
    }
    Ok(diagnostics)
}

/// The profile a new session takes from `profiles`: `workspace.agent`
/// (unknown is an error), else the one named `default` if there is one.
pub fn selected_agent<'a>(
    workspace: &Workspace,
    profiles: &'a [AgentFile],
) -> Result<Option<&'a AgentFile>, WorkspaceError> {
    let find = |name: &str| profiles.iter().find(|file| file.name == name);
    match &workspace.agent {
        Some(name) => find(name)
            .map(Some)
            .ok_or_else(|| WorkspaceError::Config(format!("unknown agent {name}"))),
        None => Ok(find(DEFAULT_AGENT)),
    }
}

/// The framing line before an output style's body (ADR-0052).
const STYLE_FRAMING: &str =
    "The output style below takes precedence over any output requirements above.";

/// `workspace.style`, loaded; an unknown name is an error.
fn selected_style(
    workspace: &Workspace,
    builtin_agents: Option<&Path>,
    root: &Path,
) -> Result<Option<(String, OutputStyle)>, WorkspaceError> {
    let Some(name) = &workspace.style else {
        return Ok(None);
    };
    let files = style_files(workspace, builtin_agents, root)?;
    let file = files
        .iter()
        .find(|file| &file.name == name)
        .ok_or_else(|| WorkspaceError::Config(format!("unknown style {name}")))?;
    Ok(Some((name.clone(), load_style(file)?)))
}

/// The builtin agent directory: the first of the application's `candidates`
/// that is a directory. A debug build tries this crate's source `agents/`
/// first, since the Tauri build copies a snapshot of it beside the binary; a
/// release build never looks at the build machine's path. `None`, with a
/// line on stderr, when there is none.
pub fn builtin_agents(candidates: impl IntoIterator<Item = PathBuf>) -> Option<PathBuf> {
    let source = cfg!(debug_assertions)
        .then(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/agents")));
    let found = source
        .into_iter()
        .chain(candidates)
        .find(|dir| dir.is_dir());
    if found.is_none() {
        eprintln!("asterpath: no builtin agent profiles found; continuing without them");
    }
    found
}

/// An agent profile or output style file (ADR-0051, ADR-0052): its name is
/// the file name without `.md`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentFile {
    pub name: String,
    pub path: PathBuf,
}

/// An agent profile's frontmatter and body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentProfile {
    pub description: Option<String>,
    /// `provider/model`, split.
    pub model: Option<(String, String)>,
    /// Tool whitelist; `None` means every tool.
    pub tools: Option<Vec<String>>,
    /// The base prompt, trimmed.
    pub prompt: String,
}

/// The `*.md` profiles of `builtin_agents`, the user directory and
/// `<root>/.asterpath/agents`, in name order; a later directory's profile
/// replaces an earlier one of the same name. Missing directories are empty.
pub fn agent_profiles(
    workspace: &Workspace,
    builtin_agents: Option<&Path>,
    root: &Path,
) -> Result<Vec<AgentFile>, WorkspaceError> {
    named_files(
        builtin_agents
            .map(Path::to_path_buf)
            .into_iter()
            .chain(workspace.user_agents.clone())
            .chain([root.join(".asterpath/agents")]),
    )
}

/// The `*.md` output styles of the `styles` directory beside
/// `builtin_agents`, the user directory and `<root>/.asterpath/styles`, with
/// the same precedence as [`agent_profiles`].
pub fn style_files(
    workspace: &Workspace,
    builtin_agents: Option<&Path>,
    root: &Path,
) -> Result<Vec<AgentFile>, WorkspaceError> {
    named_files(
        builtin_agents
            .and_then(Path::parent)
            .map(|dir| dir.join("styles"))
            .into_iter()
            .chain(workspace.user_styles.clone())
            .chain([root.join(".asterpath/styles")]),
    )
}

/// The `*.md` files of `dirs` by name; a later directory's file replaces an
/// earlier one of the same name. Missing directories are empty.
fn named_files(dirs: impl Iterator<Item = PathBuf>) -> Result<Vec<AgentFile>, WorkspaceError> {
    let mut found = BTreeMap::new();
    for dir in dirs {
        let io = |source| WorkspaceError::Io {
            context: format!("cannot read {}", dir.display()),
            source,
        };
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io(error)),
        };
        for entry in entries {
            let path = entry.map_err(io)?.path();
            if path.extension().is_some_and(|ext| ext == "md")
                && path.is_file()
                && let Some(name) = path.file_stem().and_then(|stem| stem.to_str())
            {
                found.insert(name.to_owned(), path.clone());
            }
        }
    }
    Ok(found
        .into_iter()
        .map(|(name, path)| AgentFile { name, path })
        .collect())
}

/// Read and parse a profile; an unknown frontmatter field is an error.
pub fn load_agent(file: &AgentFile) -> Result<AgentProfile, WorkspaceError> {
    let invalid = |message: String| {
        WorkspaceError::Config(format!("agent {}: {message}", file.path.display()))
    };
    let (fields, body) = read_frontmatter(file, invalid)?;
    let mut profile = AgentProfile {
        description: None,
        model: None,
        tools: None,
        prompt: body.trim().to_owned(),
    };
    for (key, value) in fields {
        match (key.as_str(), value) {
            ("description", FrontmatterValue::String(text)) => profile.description = Some(text),
            ("model", FrontmatterValue::String(text)) => {
                let (provider, model) = text
                    .split_once('/')
                    .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
                    .ok_or_else(|| {
                        invalid(format!("`model` must be provider/model, not {text}"))
                    })?;
                profile.model = Some((provider.to_owned(), model.to_owned()));
            }
            ("tools", FrontmatterValue::List(names)) => profile.tools = Some(names),
            ("description" | "model", _) => {
                return Err(invalid(format!("`{key}` must be a string")));
            }
            ("tools", _) => return Err(invalid("`tools` must be a list".to_owned())),
            _ => return Err(invalid(format!("unknown field `{key}`"))),
        }
    }
    Ok(profile)
}

/// An output style's description and body (ADR-0052).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputStyle {
    pub description: Option<String>,
    /// Appended after the profile's prompt, trimmed.
    pub prompt: String,
}

/// Read and parse a style; any field but `description` is an error.
pub fn load_style(file: &AgentFile) -> Result<OutputStyle, WorkspaceError> {
    let invalid = |message: String| {
        WorkspaceError::Config(format!("style {}: {message}", file.path.display()))
    };
    let (fields, body) = read_frontmatter(file, invalid)?;
    let mut style = OutputStyle {
        description: None,
        prompt: body.trim().to_owned(),
    };
    for (key, value) in fields {
        match (key.as_str(), value) {
            ("description", FrontmatterValue::String(text)) => style.description = Some(text),
            ("description", _) => {
                return Err(invalid("`description` must be a string".to_owned()));
            }
            _ => return Err(invalid(format!("unknown field `{key}`"))),
        }
    }
    Ok(style)
}

/// A profile or style file's frontmatter fields and body; `invalid` wraps a
/// parse error.
fn read_frontmatter(
    file: &AgentFile,
    invalid: impl Fn(String) -> WorkspaceError,
) -> Result<(BTreeMap<String, FrontmatterValue>, String), WorkspaceError> {
    let content = std::fs::read_to_string(&file.path).map_err(|source| WorkspaceError::Io {
        context: format!("cannot read {}", file.path.display()),
        source,
    })?;
    let (fields, body) = parse_frontmatter(&content).map_err(invalid)?;
    Ok((fields, body.to_owned()))
}

/// New-session skill policy: `skills_enabled`. The loader tool comes from
/// the session environment.
fn skill_index(workspace: &Workspace, root: &Path) -> (Option<Part>, Vec<SkillDiagnostic>) {
    if !workspace.skills_enabled {
        return (None, Vec::new());
    }
    let discovery = discover(&skill_roots(workspace, root));
    let (index, budget) = render_index(&discovery.skills, DEFAULT_INDEX_BUDGET);
    (
        index,
        discovery.diagnostics.into_iter().chain(budget).collect(),
    )
}

/// `<root>/AGENTS.md` only: no parent directories, no user-level file.
pub fn agents_md(root: &Path) -> Result<Option<Part>, WorkspaceError> {
    let path = root.join("AGENTS.md");
    if !path.is_file() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path).map_err(|source| WorkspaceError::Io {
        context: format!("cannot read {}", path.display()),
        source,
    })?;
    Ok(Some(Part::Text { text }))
}

/// A directory made absolute and canonical.
pub fn work_root(dir: &Path) -> Result<PathBuf, WorkspaceError> {
    dir.canonicalize().map_err(|source| WorkspaceError::Io {
        context: format!("work root {}", dir.display()),
        source,
    })
}

/// The work root of a continued session and the root to pin its environment
/// to: `explicit` (`-C`) wins, else the session's `cwd`, else `fallback`
/// (the directory it was matched in). Pinned when it differs from `cwd`.
pub fn continued_root(
    metadata: &BTreeMap<String, String>,
    explicit: Option<PathBuf>,
    fallback: PathBuf,
) -> Result<(PathBuf, Option<PathBuf>), WorkspaceError> {
    let root = match explicit {
        Some(root) => root,
        None => match metadata.get(CWD) {
            Some(cwd) => work_root(Path::new(cwd))?,
            None => fallback,
        },
    };
    let pinned = (metadata.get(CWD) != Some(&root.display().to_string())).then(|| root.clone());
    Ok((root, pinned))
}

/// Every root session whose session metadata has all of `metadata`
/// (ADR-0039), page by page; empty matches every root.
// shortcut: reads every matching entry; page in the caller when lists grow long
pub async fn roots(
    asterpath: &Asterpath,
    metadata: BTreeMap<String, String>,
) -> Result<Vec<CatalogEntry>, WorkspaceError> {
    every(
        asterpath,
        SessionQuery {
            parent: ParentFilter::Roots,
            metadata,
            ..SessionQuery::default()
        },
    )
    .await
}

/// Root sessions created in `root`, last active first.
pub async fn in_root(
    asterpath: &Asterpath,
    root: &Path,
) -> Result<Vec<CatalogEntry>, WorkspaceError> {
    let cwd = BTreeMap::from([(CWD.to_owned(), root.display().to_string())]);
    let mut here = roots(asterpath, cwd).await?;
    here.sort_by_key(|entry| std::cmp::Reverse(entry.last_activity));
    Ok(here)
}

/// A hex id (any session), else the last active session of that name in
/// `root`.
pub async fn by_id_or_name(
    asterpath: &Asterpath,
    root: &Path,
    value: &str,
) -> Result<CatalogEntry, WorkspaceError> {
    if let Ok(id) = serde_json::from_value::<SessionId>(serde_json::Value::String(value.to_owned()))
    {
        // shortcut: the catalog has no lookup by id, so this reads it whole
        return every(asterpath, SessionQuery::default())
            .await?
            .into_iter()
            .find(|entry| entry.session == id)
            .ok_or_else(|| WorkspaceError::NotFound(format!("session {id:?} not found")));
    }
    in_root(asterpath, root)
        .await?
        .into_iter()
        .find(|entry| entry.metadata.get(NAME).map(String::as_str) == Some(value))
        .ok_or_else(|| {
            WorkspaceError::NotFound(format!("no session named {value} in {}", root.display()))
        })
}

/// Every entry `query` matches, 256 a page.
async fn every(
    asterpath: &Asterpath,
    mut query: SessionQuery,
) -> Result<Vec<CatalogEntry>, WorkspaceError> {
    query.limit = Some(256);
    let mut all = Vec::new();
    loop {
        let page = asterpath.sessions(query.clone()).await?;
        let Some(last) = page.last() else {
            return Ok(all);
        };
        query.after = Some(last.cursor());
        all.extend(page);
    }
}

/// The session environment of a work root (ADR-0037): workspace tools, the
/// skill loader and the sandbox policy rooted at the root session's `cwd`,
/// and the spawn variables for bash.
pub struct WorkRoot {
    pub workspace: Workspace,
    /// New sessions get the tool selection and the skill switch; continued
    /// ones every workspace tool and the skill loader, so whatever their
    /// effective config names still resolves.
    pub new_session: bool,
    /// Overrides the `cwd` metadata.
    pub pinned: Option<PathBuf>,
}

impl WorkRoot {
    fn tools(&self, root: &Path) -> Result<Vec<Arc<dyn Tool>>, WorkspaceError> {
        let selection = if self.new_session {
            self.workspace.tools.as_deref()
        } else {
            None
        };
        let mut tools = workspace_tools(&self.workspace, root, selection)?;
        if !self.new_session || self.workspace.skills_enabled {
            tools.push(Arc::new(LoadSkillTool::new(skill_roots(
                &self.workspace,
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
                .map_err(|error| EnvironmentError::new(error.to_string()))?;
            let mut spawn = SpawnEnv::default();
            spawn.vars = self.workspace.spawn_vars.clone();
            Ok(SessionEnvironment::new(tools, spawn))
        })
    }
}

/// Explicit paths, then the project and user roots in priority order.
pub fn skill_roots(workspace: &Workspace, root: &Path) -> Vec<SkillRoot> {
    let mut roots: Vec<SkillRoot> = workspace
        .skill_paths
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
    roots.extend(workspace.user_skill_roots.iter().cloned());
    roots
}

/// `asterpath-tools` rooted at `root` under the sandbox policy, filtered to
/// `selection` (`None` = all) in the order given.
pub fn workspace_tools(
    workspace: &Workspace,
    root: &Path,
    selection: Option<&[String]>,
) -> Result<Vec<Arc<dyn Tool>>, WorkspaceError> {
    let policy = workspace.sandbox_policy(root);
    let all = asterpath_tools::coding(
        root.to_path_buf(),
        sandbox_provider(&policy),
        policy,
        asterpath_tools::DEFAULT_BASH_TIMEOUT,
    )
    .map_err(|error| WorkspaceError::Config(error.to_string()))?;
    let Some(selection) = selection else {
        return Ok(all);
    };
    selection
        .iter()
        .map(|name| {
            all.iter()
                .find(|tool| tool.descriptor().name.as_str() == name)
                .cloned()
                .ok_or_else(|| WorkspaceError::Config(format!("unknown tool {name}")))
        })
        .collect()
}

/// Start every configured MCP server under the sandbox policy; a stdio
/// server without `cwd` starts in `root`. A `required` failure is an error.
pub async fn load_mcp(workspace: &Workspace, root: &Path) -> Result<McpServers, WorkspaceError> {
    let mut configs = workspace.mcp_servers.clone();
    for config in &mut configs {
        if let McpTransport::Stdio {
            cwd: cwd @ None, ..
        } = &mut config.transport
        {
            *cwd = Some(root.to_path_buf());
        }
    }
    let policy = workspace.sandbox_policy(root);
    Ok(McpServers::load(configs, sandbox_provider(&policy), policy).await?)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_continued_root_is_pinned_only_when_it_is_not_the_sessions_cwd() {
        let dir = std::env::temp_dir().canonicalize().unwrap();
        let cwd = BTreeMap::from([(CWD.to_owned(), dir.display().to_string())]);
        let fallback = PathBuf::from("/matched");
        assert_eq!(
            continued_root(&cwd, None, fallback.clone()).unwrap(),
            (dir.clone(), None)
        );
        assert_eq!(
            continued_root(&cwd, Some(fallback.clone()), PathBuf::new()).unwrap(),
            (fallback.clone(), Some(fallback.clone()))
        );
        assert_eq!(
            continued_root(&BTreeMap::new(), None, fallback.clone()).unwrap(),
            (fallback.clone(), Some(fallback))
        );
    }
}
