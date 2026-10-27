//! Desktop command layer (ADR-0035): one resident `Asterpath` for every
//! project, assembled by [`crate::workspace`] from the same `config.toml` and
//! database as the CLI. Each method backs one Tauri command in
//! `bin/asterpath-desktop/main.rs`; this module builds and tests without Tauri.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::config::{self, KeySource, Settings, SetupChoice};
use crate::workspace::{
    AGENT, CWD, NAME, STYLE, WorkRoot, Workspace, agent_profiles, find_style, in_root, load_agent,
    load_style, new_session_spec, roots, selected_agent, style_files, work_root,
};
use asterpath::{AsterError, Asterpath, LogId, OwnedSession, SessionId, WatchEvent};
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalGate, ApprovalPolicy, ApprovalRequest, ApprovalVerdict,
    CatalogEntry, Clock, ConfigPatch, EnvironmentAdapter, InputId, ModelSpec, Part, SessionEvent,
    SessionSpec, SideEffect, StorageError, SystemClock, ToolCallId, ToolName, ToolSelection,
    UsageScope, UserInput, decode_entry, effective_config, root_of, scan_all, unique_bytes,
};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};

/// A session's badge in the sidebar.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// This process drives it.
    Running,
    /// A tool call in its tree waits for a decision here (ADR-0039).
    AwaitingApproval,
    Idle,
    /// Another process drives it; read-only here.
    Elsewhere,
}

/// The status bar of a session.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SessionInfo {
    /// The model its next step uses.
    pub model: ModelSpec,
    /// Committed tokens of the session and its subagents.
    pub tokens: u64,
    /// Why this process last stopped driving it, until the next send.
    pub error: Option<String>,
    /// The agent profile it was created with; fixed for its life (ADR-0051).
    pub agent: Option<String>,
    /// Why spawned commands run without a sandbox here (ADR-0055).
    pub no_sandbox: Option<String>,
}

/// An agent profile or output style the new-session picker offers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Choice {
    pub name: String,
    pub description: Option<String>,
}

/// A project's agent profiles and the one preselected (`default_agent`,
/// else `default`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Agents {
    pub default: Option<String>,
    pub agents: Vec<Choice>,
}

/// A project's output styles (ADR-0052) and the one preselected
/// (`output_style`; `None` is "none").
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Styles {
    pub default: Option<String>,
    pub styles: Vec<Choice>,
}

/// A work root in the sidebar.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Project {
    pub root: String,
    /// The root's last path component.
    pub name: String,
}

impl Project {
    fn new(root: String) -> Self {
        let name = Path::new(&root)
            .file_name()
            .map_or_else(|| root.clone(), |name| name.to_string_lossy().into_owned());
        Self { root, name }
    }
}

/// A root session in a project's list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SessionSummary {
    pub id: SessionId,
    pub name: Option<String>,
    pub created_at: i64,
    pub last_activity: i64,
    pub status: Status,
    pub agent: Option<String>,
    /// The output style it was created with (ADR-0052).
    pub style: Option<String>,
}

impl SessionSummary {
    fn new(entry: CatalogEntry, status: Status) -> Self {
        Self {
            id: entry.session,
            name: entry.metadata.get(NAME).cloned(),
            created_at: entry.created_at,
            last_activity: entry.last_activity,
            status,
            agent: entry.metadata.get(AGENT).cloned(),
            style: entry.metadata.get(STYLE).cloned(),
        }
    }
}

/// What a conversation view renders, in order: the stored records, then
/// `Synced` once the replay caught up, then new records and, while this
/// process drives, the live events between them (ADR-0038).
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum ViewEvent {
    Record { log_id: LogId, event: SessionEvent },
    Synced,
    Live { event: AgentEvent },
}

/// Who may drive a session (ADR-0036).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    /// This process drives it now.
    Here,
    /// Nobody; sending claims it.
    Free,
    /// Another process; read-only here.
    Elsewhere,
}

/// The composer's approval mode: what a session tree's mutating tool calls
/// get from the approval policy. The sandbox is `config.toml`'s in every
/// mode; `Deny` keeps a tree read-only because every in-process tool that
/// writes or runs a process (`bash` included) declares itself mutating.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    /// Mutating tools are denied; external tools still ask.
    Deny,
    /// Mutating and external tools ask (`config.toml`'s `untrusted`).
    #[default]
    Ask,
    /// Nothing asks (`never`).
    Allow,
}

impl From<config::ApprovalMode> for ApprovalMode {
    fn from(mode: config::ApprovalMode) -> Self {
        match mode {
            config::ApprovalMode::Untrusted => Self::Ask,
            config::ApprovalMode::Never => Self::Allow,
        }
    }
}

/// The user's answer on an approval card.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Answer {
    AllowOnce,
    /// Allow, and stop asking for this tool in this session.
    AllowForSession,
    /// The reason is sent to the model.
    Deny {
        reason: Option<String>,
    },
}

#[derive(Default)]
struct TreeApprovals {
    mode: Option<ApprovalMode>,
    allowed: BTreeSet<ToolName>,
}

/// The desktop's approval policy: each session tree's mode and the tools
/// allowed for it, keyed by the root so subagents follow their tree; in
/// memory only (ADR-0035), so a restart asks again.
pub struct ApprovalModes {
    default: ApprovalMode,
    asks_mutating: Arc<dyn ApprovalPolicy>,
    trees: Mutex<HashMap<SessionId, TreeApprovals>>,
}

impl ApprovalModes {
    pub fn new(default: ApprovalMode) -> Self {
        Self {
            default,
            asks_mutating: config::approval_policy(config::ApprovalMode::Untrusted),
            trees: Mutex::default(),
        }
    }

    /// The mode of the tree rooted at `root`.
    pub fn mode(&self, root: SessionId) -> ApprovalMode {
        lock(&self.trees)
            .get(&root)
            .and_then(|tree| tree.mode)
            .unwrap_or(self.default)
    }

    pub fn set_mode(&self, root: SessionId, mode: ApprovalMode) {
        lock(&self.trees).entry(root).or_default().mode = Some(mode);
    }

    /// Stop asking for `tool` anywhere in the tree rooted at `root`.
    pub fn allow(&self, root: SessionId, tool: ToolName) {
        lock(&self.trees)
            .entry(root)
            .or_default()
            .allowed
            .insert(tool);
    }
}

impl ApprovalPolicy for ApprovalModes {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict {
        let allowed = lock(&self.trees)
            .get(&request.root)
            .is_some_and(|tree| tree.allowed.contains(&request.tool));
        match self.mode(request.root) {
            ApprovalMode::Allow => ApprovalVerdict::Allow,
            ApprovalMode::Deny if matches!(request.side_effect, SideEffect::Mutating) => {
                ApprovalVerdict::Deny {
                    reason: Some("the session is read-only".to_owned()),
                }
            }
            _ => match self.asks_mutating.classify(request) {
                ApprovalVerdict::Ask if allowed => ApprovalVerdict::Allow,
                verdict => verdict,
            },
        }
    }
}

/// A provider the settings page offers, with where its key comes from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProviderKey {
    pub id: String,
    /// `anthropic` or `openai`.
    pub protocol: String,
    /// `None`: the adapter's default endpoint.
    pub base_url: Option<String>,
    pub key: KeySource,
}

/// What the settings page shows; never a key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Setup {
    pub path: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    /// `anthropic` and `openai`, then the file's other instances.
    pub providers: Vec<ProviderKey>,
    /// No provider, model or key for that provider: the page must be shown.
    pub needed: bool,
    /// Why the configuration does not load, when it is not just missing setup.
    pub error: Option<String>,
}

/// The settings of `config` (resolved as [`Desktop::open`] does).
pub fn setup(config: Option<PathBuf>) -> Setup {
    let path = config::config_path(config.clone()).map(|path| path.display().to_string());
    let settings = match config::load(config, &[]).and_then(|settings| {
        config::validate(&settings)?;
        Ok(settings)
    }) {
        Ok(settings) => settings,
        Err(error) => {
            return Setup {
                path,
                provider: None,
                model: None,
                providers: Vec::new(),
                needed: false,
                error: Some(error.to_string()),
            };
        }
    };
    let mut ids = vec!["anthropic".to_owned(), "openai".to_owned()];
    for (id, _) in config::key_sources(&settings) {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    let providers = ids
        .into_iter()
        .filter_map(|id| {
            let (protocol, base_url) = config::endpoint(&settings, &id)?;
            Some(ProviderKey {
                protocol: protocol.to_owned(),
                base_url: base_url.map(str::to_owned),
                key: config::key_source(&settings, &id)?,
                id,
            })
        })
        .collect();
    let provider = settings.provider().ok().map(str::to_owned);
    let model = settings.model_id().map(str::to_owned);
    let needed = model.is_none()
        || provider.as_deref().is_none_or(|id| {
            config::key_source(&settings, id).is_none_or(|key| key == KeySource::Missing)
        });
    Setup {
        path,
        provider,
        model,
        providers,
        needed,
        error: None,
    }
}

/// Write the page's values to `config`'s file.
pub fn save_setup(config: Option<PathBuf>, choice: &SetupChoice) -> Result<(), String> {
    let path = config::config_path(config)
        .ok_or("no configuration path: neither XDG_CONFIG_HOME nor HOME is set")?;
    config::save_setup(&path, choice).map_err(text)
}

/// The background task driving a session tree this process claimed.
struct Driver {
    owned: OwnedSession,
    /// Input arrived since the task's last drive started.
    again: bool,
}

type Drivers = Arc<Mutex<HashMap<SessionId, Driver>>>;
/// The last drive error per session.
type Errors = Arc<Mutex<HashMap<SessionId, String>>>;

pub struct Desktop {
    asterpath: Asterpath,
    /// The policy `asterpath` was built with.
    modes: Arc<ApprovalModes>,
    /// New sessions' work-root rules and `config.toml` spec.
    workspace: Workspace,
    template: SessionSpec,
    /// The roots added with the folder picker, a JSON array of paths.
    projects_file: PathBuf,
    /// The model picker: the configured model, then the aliases' models.
    models: Vec<ModelSpec>,
    /// The bundled agent profiles; `None` means no builtin layer.
    builtin_agents: Option<PathBuf>,
    drivers: Drivers,
    errors: Errors,
}

impl Desktop {
    /// `asterpath`'s environment adapter treats every session as continued
    /// (`WorkRoot { new_session: false }`); a new session gets `workspace`'s
    /// new-session tools as its explicit selection instead. `asterpath`'s
    /// approval policy must be `modes`.
    pub fn new(
        asterpath: Asterpath,
        modes: Arc<ApprovalModes>,
        workspace: Workspace,
        template: SessionSpec,
        projects_file: PathBuf,
    ) -> Self {
        Self {
            asterpath,
            modes,
            workspace,
            models: vec![template.model.clone()],
            template,
            projects_file,
            builtin_agents: None,
            drivers: Drivers::default(),
            errors: Errors::default(),
        }
    }

    /// `config` (else `ASTERPATH_CONFIG`, else the default `config.toml`) and
    /// its database; `builtin_agents` is the bundled profile directory.
    pub async fn open(
        config: Option<PathBuf>,
        projects_file: PathBuf,
        builtin_agents: Option<PathBuf>,
    ) -> Result<Self, String> {
        let settings: Settings = config::load(config, &[]).map_err(text)?;
        config::validate(&settings).map_err(text)?;
        let workspace = settings.workspace().map_err(text)?;
        let template = config::session_spec(&settings).map_err(text)?;
        let environment = WorkRoot {
            workspace: workspace.clone(),
            new_session: false,
            pinned: None,
            builtin_agents: builtin_agents.clone(),
        };
        // shortcut: no MCP servers yet; they are loaded per work root, which a multi-project process does not have
        let modes = Arc::new(ApprovalModes::new(settings.approval.into()));
        let asterpath = config::assemble(&settings, Vec::new(), environment, modes.clone())
            .await
            .map_err(text)?;
        let mut desktop = Self::new(asterpath, modes, workspace, template, projects_file);
        desktop.builtin_agents = builtin_agents;
        for model in config::model_aliases(&settings).into_values() {
            if !desktop.models.contains(&model) {
                desktop.models.push(model);
            }
        }
        Ok(desktop)
    }

    /// Added roots and every root a session was created in, by path.
    pub async fn projects(&self) -> Result<Vec<Project>, String> {
        let mut paths: BTreeSet<String> = self.added()?.into_iter().collect();
        paths.extend(
            roots(&self.asterpath, BTreeMap::new())
                .await
                .map_err(text)?
                .into_iter()
                .filter_map(|entry| entry.metadata.get(CWD).cloned()),
        );
        Ok(paths.into_iter().map(Project::new).collect())
    }

    /// Remember `dir` (canonical) as a project.
    pub fn add_project(&self, dir: &Path) -> Result<Project, String> {
        let root = work_root(dir).map_err(text)?.display().to_string();
        let mut added = self.added()?;
        if !added.contains(&root) {
            added.push(root.clone());
            if let Some(parent) = self.projects_file.parent() {
                std::fs::create_dir_all(parent).map_err(text)?;
            }
            let json = serde_json::to_string_pretty(&added).map_err(text)?;
            std::fs::write(&self.projects_file, json).map_err(|error| {
                format!("cannot write {}: {error}", self.projects_file.display())
            })?;
        }
        Ok(Project::new(root))
    }

    /// The root sessions created in `root`, last active first.
    pub async fn sessions(&self, root: &Path) -> Result<Vec<SessionSummary>, String> {
        let entries = in_root(&self.asterpath, root).await.map_err(text)?;
        let mut sessions = Vec::with_capacity(entries.len());
        for entry in entries {
            let status = self.status(entry.session).await?;
            sessions.push(SessionSummary::new(entry, status));
        }
        Ok(sessions)
    }

    /// The agent profiles a new session in `root` may use.
    pub fn agents(&self, root: &Path) -> Result<Agents, String> {
        let root = work_root(root).map_err(text)?;
        let profiles =
            agent_profiles(&self.workspace, self.builtin_agents.as_deref(), &root).map_err(text)?;
        // An unknown `default_agent` fails here, before the user picks anything.
        let default = selected_agent(&self.workspace, &profiles)
            .map_err(text)?
            .map(|file| file.name.clone());
        let agents = profiles
            .into_iter()
            .map(|file| Choice {
                // A broken profile is still listed; creating with it reports why.
                description: load_agent(&file)
                    .ok()
                    .and_then(|profile| profile.description),
                name: file.name,
            })
            .collect();
        Ok(Agents { default, agents })
    }

    /// The output styles a new session in `root` may use.
    pub fn styles(&self, root: &Path) -> Result<Styles, String> {
        let root = work_root(root).map_err(text)?;
        let files =
            style_files(&self.workspace, self.builtin_agents.as_deref(), &root).map_err(text)?;
        let default = self.workspace.style.clone();
        // An unknown `output_style` fails here, before the user picks anything.
        if let Some(name) = &default {
            find_style(&files, name).map_err(text)?;
        }
        let styles = files
            .into_iter()
            .map(|file| Choice {
                // A broken style is still listed; creating with it reports why.
                description: load_style(&file).ok().and_then(|style| style.description),
                name: file.name,
            })
            .collect();
        Ok(Styles { default, styles })
    }

    /// A session in `root` as the CLI creates one: the agent profile
    /// (`agent`, else `default_agent`), the output style (`style` as picked;
    /// the picker preselects `output_style`), `AGENTS.md`, the skill index,
    /// session metadata and the tools a new session gets there.
    pub async fn create_session(
        &self,
        root: &Path,
        name: Option<&str>,
        agent: Option<&str>,
        style: Option<&str>,
    ) -> Result<SessionSummary, String> {
        let root = work_root(root).map_err(text)?;
        let mut spec = self.template.clone();
        let mut workspace = self.workspace.clone();
        if let Some(agent) = agent {
            workspace.agent = Some(agent.to_owned());
        }
        workspace.style = style.map(str::to_owned);
        // shortcut: skill and profile diagnostics are dropped; show them once the view has a place for warnings
        new_session_spec(
            &workspace,
            self.builtin_agents.as_deref(),
            &root,
            name,
            &mut spec,
        )
        .map_err(text)?;
        let fresh = WorkRoot {
            workspace: self.workspace.clone(),
            new_session: true,
            pinned: None,
            builtin_agents: self.builtin_agents.clone(),
        };
        let environment = fresh.resolve(&spec.metadata).await.map_err(text)?;
        // The profile's whitelist, else every tool the environment gives a new session.
        let available: Vec<_> = environment
            .tools
            .iter()
            .map(|tool| tool.descriptor().name.clone())
            .collect();
        if let ToolSelection::Names(names) = &spec.tools {
            // As in the CLI, a whitelisted name the work root lacks is an error.
            if let Some(name) = names.iter().find(|name| !available.contains(name)) {
                return Err(format!("unknown tool {}", name.as_str()));
            }
        } else {
            spec.tools = ToolSelection::Names(available);
        }
        let name = spec.metadata.get(NAME).cloned();
        let agent = spec.metadata.get(AGENT).cloned();
        let style = spec.metadata.get(STYLE).cloned();
        // Providers are built once from config.toml; a profile cannot add one.
        let provider = spec.model.provider.clone();
        let id = self
            .asterpath
            .create_session(spec)
            .await
            .map_err(|error| match &agent {
                Some(agent) => format!("agent {agent} (provider {provider}): {error}"),
                None => error.to_string(),
            })?
            .id();
        // Its only record so far is `SessionCreated`, whose time the catalog shows.
        let created = scan_all(&*self.asterpath.store(), id)
            .await
            .map_err(text)?
            .first()
            .map_or(0, |entry| entry.recorded_at);
        Ok(SessionSummary {
            id,
            name,
            created_at: created,
            last_activity: created,
            status: Status::Idle,
            agent,
            style,
        })
    }

    /// Who may drive `session` now.
    pub async fn access(&self, session: SessionId) -> Result<Access, String> {
        if lock(&self.drivers).contains_key(&session) {
            return Ok(Access::Here);
        }
        Ok(match self.asterpath.owner(session).await.map_err(text)? {
            Some(_) => Access::Elsewhere,
            None => Access::Free,
        })
    }

    /// The sidebar badge of root session `session`.
    async fn status(&self, session: SessionId) -> Result<Status, String> {
        if !self
            .asterpath
            .approval_gate()
            .pending_in(session)
            .is_empty()
        {
            return Ok(Status::AwaitingApproval);
        }
        Ok(match self.access(session).await? {
            Access::Here => Status::Running,
            Access::Free => Status::Idle,
            Access::Elsewhere => Status::Elsewhere,
        })
    }

    /// The models the picker offers.
    pub fn models(&self) -> Vec<ModelSpec> {
        self.models.clone()
    }

    pub async fn session_info(&self, session: SessionId) -> Result<SessionInfo, String> {
        let store = self.asterpath.store();
        let entries = scan_all(&*store, session).await.map_err(text)?;
        let records = entries
            .into_iter()
            .map(|entry| {
                let event = decode_entry(&entry).map_err(text)?;
                Ok((entry, event))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let agent = records.iter().find_map(|(_, event)| match event {
            SessionEvent::SessionCreated { metadata, .. } => metadata.get(AGENT).cloned(),
            _ => None,
        });
        let model = effective_config(&records, u64::MAX)
            .ok_or_else(|| format!("session {} does not exist", session.to_hex()))?
            .model;
        let usage = self
            .asterpath
            .usage(UsageScope::SessionSubtree(session))
            .await
            .map_err(text)?;
        Ok(SessionInfo {
            model,
            tokens: usage.own.total() + usage.descendants.total(),
            error: lock(&self.errors).get(&session).cloned(),
            agent,
            no_sandbox: self
                .workspace
                .sandbox_backend
                .unavailable()
                .map(str::to_owned),
        })
    }

    /// Use `model` from the session's next step on (ADR-0030). An external
    /// write: no claim, so it also works while another process drives.
    pub async fn set_model(&self, session: SessionId, model: ModelSpec) -> Result<(), String> {
        let handle = self.asterpath.open_session(session).await.map_err(text)?;
        let mut patch = ConfigPatch::default();
        patch.model = Some(model);
        handle.update_config(patch).await.map_err(text)
    }

    /// The session's records after `after`, then everything new. Never
    /// ends; drop it to stop.
    pub async fn watch(
        &self,
        session: SessionId,
        after: Option<LogId>,
    ) -> Result<BoxStream<'static, Result<ViewEvent, String>>, String> {
        let handle = self.asterpath.open_session(session).await.map_err(text)?;
        Ok(handle
            .watch(after)
            .filter_map(|event| async move {
                match event {
                    Ok(WatchEvent::Record(record)) => Some(Ok(ViewEvent::Record {
                        log_id: record.log_id,
                        event: record.event,
                    })),
                    Ok(WatchEvent::Synced) => Some(Ok(ViewEvent::Synced)),
                    Ok(WatchEvent::Live(event)) => Some(Ok(ViewEvent::Live { event })),
                    Ok(_) => None,
                    Err(error) => Some(Err(error.to_string())),
                }
            })
            .boxed())
    }

    /// Log `message` as input and drive the session to idle in the
    /// background; during a turn it steers the next step. A session another
    /// process drives is read-only: nothing is logged.
    pub async fn send(&self, session: SessionId, message: String) -> Result<(), String> {
        let owned = self.claim(session).await?;
        lock(&self.errors).remove(&session);
        owned
            .submit_input(UserInput {
                id: InputId::from_bytes(unique_bytes(SystemClock.now())),
                parts: vec![Part::Text { text: message }],
            })
            .await
            .map_err(text)?;
        let mut drivers = lock(&self.drivers);
        if let Some(driver) = drivers.get_mut(&session) {
            driver.again = true;
        } else {
            drivers.insert(
                session,
                Driver {
                    owned: owned.clone(),
                    again: false,
                },
            );
            tokio::spawn(drive(
                Arc::clone(&self.drivers),
                Arc::clone(&self.errors),
                self.asterpath.approval_gate(),
                owned,
            ));
        }
        Ok(())
    }

    /// Cancel the turn this process drives; a no-op otherwise.
    pub async fn cancel(&self, session: SessionId) -> Result<(), String> {
        let owned = lock(&self.drivers)
            .get(&session)
            .map(|driver| driver.owned.clone());
        match owned {
            Some(owned) => owned.cancel().await.map_err(text),
            None => Ok(()),
        }
    }

    /// The approval requests waiting in `session`'s tree, its subagents'
    /// included; each names the session to answer it on.
    pub fn approvals(&self, session: SessionId) -> Vec<ApprovalRequest> {
        self.asterpath.approval_gate().pending_in(session)
    }

    /// Answer the request `call` of `session` (the request's own session).
    pub async fn approve(
        &self,
        session: SessionId,
        call: ToolCallId,
        answer: Answer,
    ) -> Result<(), String> {
        let decision = match answer {
            Answer::AllowOnce => ApprovalDecision::Allow,
            Answer::AllowForSession => {
                let request = self
                    .asterpath
                    .approval_gate()
                    .pending()
                    .into_iter()
                    .find(|request| request.session == session && request.tool_call_id == call)
                    .ok_or("the request is no longer waiting")?;
                self.modes.allow(request.root, request.tool);
                ApprovalDecision::Allow
            }
            Answer::Deny { reason } => ApprovalDecision::Deny { reason },
        };
        let handle = self.asterpath.open_session(session).await.map_err(text)?;
        handle.approve(call, decision).await.map_err(text)
    }

    /// The mode of `session`'s tree.
    pub async fn approval_mode(&self, session: SessionId) -> Result<ApprovalMode, String> {
        Ok(self.modes.mode(self.root(session).await?))
    }

    /// Applies to `session`'s whole tree from its next tool call.
    pub async fn set_approval_mode(
        &self,
        session: SessionId,
        mode: ApprovalMode,
    ) -> Result<(), String> {
        self.modes.set_mode(self.root(session).await?, mode);
        Ok(())
    }

    async fn root(&self, session: SessionId) -> Result<SessionId, String> {
        root_of(&*self.asterpath.store(), session)
            .await
            .map_err(text)
    }

    /// Writer access to `session`; refused while another process drives it.
    async fn claim(&self, session: SessionId) -> Result<OwnedSession, String> {
        let handle = self.asterpath.open_session(session).await.map_err(text)?;
        handle.claim().await.map_err(|error| match error {
            AsterError::Storage(StorageError::OwnedElsewhere { .. }) => {
                "another process is driving this session".to_owned()
            }
            error => error.to_string(),
        })
    }

    fn added(&self) -> Result<Vec<String>, String> {
        match std::fs::read_to_string(&self.projects_file) {
            Ok(json) => serde_json::from_str(&json)
                .map_err(|error| format!("invalid {}: {error}", self.projects_file.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(format!(
                "cannot read {}: {error}",
                self.projects_file.display()
            )),
        }
    }
}

/// Drive until idle with no input left, then release the claim. Checking
/// `again` and removing the driver happen under the lock `send` takes, so
/// input sent while the task stops always gets a drive. The tree's approval
/// endpoint is open meanwhile; the view answers through `approve`.
async fn drive(drivers: Drivers, errors: Errors, gate: Arc<ApprovalGate>, owned: OwnedSession) {
    let session = owned.id();
    let _endpoint = gate.register(owned.owner().root);
    loop {
        let mut stream = owned.drive();
        let mut failed = false;
        while let Some(event) = stream.next().await {
            if let Err(error) = event {
                lock(&errors).insert(session, error.to_string());
                failed = true;
                break;
            }
        }
        drop(stream);
        // After an error only new input retries; old pending input would fail again.
        if !failed
            && owned
                .facts()
                .await
                .is_ok_and(|facts| facts.has_pending_input())
        {
            continue;
        }
        let mut drivers = lock(&drivers);
        let again = drivers
            .get_mut(&session)
            .is_some_and(|driver| std::mem::take(&mut driver.again));
        if !again {
            drivers.remove(&session);
            return;
        }
    }
}

fn lock<T>(map: &Mutex<T>) -> MutexGuard<'_, T> {
    map.lock().unwrap_or_else(PoisonError::into_inner)
}

fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
