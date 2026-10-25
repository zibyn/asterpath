//! Desktop reference host, command layer (ADR-0035): one resident
//! `Asterpath` for every project, assembled by `asterpath-host` from the same
//! `config.toml` and database as the CLI. Each method backs one Tauri command
//! in `main.rs`; this crate builds and tests without Tauri.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use asterpath::{AsterError, Asterpath, LogId, OwnedSession, SessionId, WatchEvent};
use asterpath_host::config::{self, Settings};
use asterpath_host::{CWD, NAME, WorkRoot, Workspace, new_session_spec, work_root};
use asterpath_protocol::{
    AgentEvent, CatalogEntry, Clock, ConfigPatch, EnvironmentAdapter, InputId, ModelSpec,
    ParentFilter, Part, SessionEvent, SessionQuery, SessionSpec, StorageError, SystemClock,
    ToolSelection, UsageScope, UserInput, decode_entry, effective_config, scan_all, unique_bytes,
};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use serde::Serialize;

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
}

impl SessionSummary {
    fn new(entry: CatalogEntry, status: Status) -> Self {
        Self {
            id: entry.session,
            name: entry.metadata.get(NAME).cloned(),
            created_at: entry.created_at,
            last_activity: entry.last_activity,
            status,
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
    /// New sessions' work-root rules and `config.toml` spec.
    workspace: Workspace,
    template: SessionSpec,
    /// The roots added with the folder picker, a JSON array of paths.
    projects_file: PathBuf,
    /// The model picker: the configured model, then the aliases' models.
    models: Vec<ModelSpec>,
    drivers: Drivers,
    errors: Errors,
}

impl Desktop {
    /// `asterpath`'s environment adapter treats every session as continued
    /// (`WorkRoot { new: false }`); a new session gets `workspace`'s
    /// new-session tools as its explicit selection instead.
    pub fn new(
        asterpath: Asterpath,
        workspace: Workspace,
        template: SessionSpec,
        projects_file: PathBuf,
    ) -> Self {
        Self {
            asterpath,
            workspace,
            models: vec![template.model.clone()],
            template,
            projects_file,
            drivers: Drivers::default(),
            errors: Errors::default(),
        }
    }

    /// The default `config.toml` (or `ASTERPATH_CONFIG`) and its database.
    pub async fn open(projects_file: PathBuf) -> Result<Self, String> {
        let settings: Settings = config::load(None, &[])?;
        config::validate(&settings)?;
        let workspace = settings.workspace()?;
        let template = config::session_spec(&settings)?;
        let environment = WorkRoot {
            workspace: workspace.clone(),
            new: false,
            pinned: None,
        };
        // shortcut: no MCP servers yet; they are loaded per work root, which a multi-project process does not have
        let asterpath = config::assemble(&settings, Vec::new(), environment)
            .await
            .map_err(text)?;
        let mut desktop = Self::new(asterpath, workspace, template, projects_file);
        for model in config::model_aliases(&settings).into_values() {
            if !desktop.models.contains(&model) {
                desktop.models.push(model);
            }
        }
        Ok(desktop)
    }

    /// Added roots and every root a session was created in, by path.
    pub async fn projects(&self) -> Result<Vec<Project>, String> {
        let mut roots: BTreeSet<String> = self.added()?.into_iter().collect();
        roots.extend(
            self.roots(BTreeMap::new())
                .await?
                .into_iter()
                .filter_map(|entry| entry.metadata.get(CWD).cloned()),
        );
        Ok(roots.into_iter().map(Project::new).collect())
    }

    /// Remember `dir` (canonical) as a project.
    pub fn add_project(&self, dir: &Path) -> Result<Project, String> {
        let root = work_root(dir)?.display().to_string();
        let mut added = self.added()?;
        if !added.contains(&root) {
            added.push(root.clone());
            if let Some(parent) = self.projects_file.parent() {
                std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
            }
            let json = serde_json::to_string_pretty(&added).map_err(|error| error.to_string())?;
            std::fs::write(&self.projects_file, json).map_err(|error| {
                format!("cannot write {}: {error}", self.projects_file.display())
            })?;
        }
        Ok(Project::new(root))
    }

    /// The root sessions created in `root`, last active first.
    pub async fn sessions(&self, root: &Path) -> Result<Vec<SessionSummary>, String> {
        let query = BTreeMap::from([(CWD.to_owned(), root.display().to_string())]);
        let mut entries = self.roots(query).await?;
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.last_activity));
        let mut sessions = Vec::with_capacity(entries.len());
        for entry in entries {
            let status = self.status(entry.session).await?;
            sessions.push(SessionSummary::new(entry, status));
        }
        Ok(sessions)
    }

    /// A session in `root` as the CLI creates one: `AGENTS.md`, the skill
    /// index, host metadata and the tools a new session gets there.
    pub async fn create_session(
        &self,
        root: &Path,
        name: Option<&str>,
    ) -> Result<SessionSummary, String> {
        let root = work_root(root)?;
        let mut spec = self.template.clone();
        // shortcut: skill diagnostics are dropped; show them once the view has a place for warnings
        new_session_spec(&self.workspace, &root, name, &mut spec)?;
        let fresh = WorkRoot {
            workspace: self.workspace.clone(),
            new: true,
            pinned: None,
        };
        let environment = fresh
            .resolve(&spec.metadata)
            .await
            .map_err(|error| error.to_string())?;
        spec.tools = ToolSelection::Names(
            environment
                .tools
                .iter()
                .map(|tool| tool.descriptor().name.clone())
                .collect(),
        );
        let session = self
            .asterpath
            .create_session(spec)
            .await
            .map_err(text)?
            .id();
        let mut created = self.roots(BTreeMap::new()).await?;
        created.retain(|entry| entry.session == session);
        created
            .pop()
            .map(|entry| SessionSummary::new(entry, Status::Idle))
            .ok_or_else(|| "the new session is not in the catalog".to_owned())
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
        let entries = scan_all(&*store, session)
            .await
            .map_err(|e| e.to_string())?;
        let records = entries
            .into_iter()
            .map(|entry| {
                let event = decode_entry(&entry).map_err(|e| e.to_string())?;
                Ok((entry, event))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let model = effective_config(&records, u64::MAX)
            .ok_or_else(|| format!("session {} does not exist", session.to_hex()))?
            .model;
        let usage = self
            .asterpath
            .usage(UsageScope::SessionSubtree(session))
            .await
            .map_err(|e| e.to_string())?;
        Ok(SessionInfo {
            model,
            tokens: usage.own.total() + usage.descendants.total(),
            error: lock(&self.errors).get(&session).cloned(),
        })
    }

    /// Use `model` from the session's next step on (ADR-0030).
    pub async fn set_model(&self, session: SessionId, model: ModelSpec) -> Result<(), String> {
        let owned = self.claim(session).await?;
        let mut patch = ConfigPatch::default();
        patch.model = Some(model);
        owned.update_config(patch).await.map_err(text)
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

    /// Every root session whose metadata has `metadata`, page by page.
    // shortcut: reads every matching catalog entry per call; page in the UI when lists grow long
    async fn roots(&self, metadata: BTreeMap<String, String>) -> Result<Vec<CatalogEntry>, String> {
        let mut query = SessionQuery {
            parent: ParentFilter::Roots,
            metadata,
            limit: Some(256),
            ..SessionQuery::default()
        };
        let mut all = Vec::new();
        loop {
            let page = self.asterpath.sessions(query.clone()).await.map_err(text)?;
            let Some(last) = page.last() else {
                return Ok(all);
            };
            query.after = Some(last.cursor());
            all.extend(page);
        }
    }
}

/// Drive until idle with no input left, then release the claim. Checking
/// `again` and removing the driver happen under the lock `send` takes, so
/// input sent while the task stops always gets a drive.
async fn drive(drivers: Drivers, errors: Errors, owned: OwnedSession) {
    let session = owned.id();
    loop {
        let mut stream = owned.drive();
        while let Some(event) = stream.next().await {
            if let Err(error) = event {
                lock(&errors).insert(session, error.to_string());
                lock(&drivers).remove(&session);
                return;
            }
        }
        drop(stream);
        let pending = owned
            .facts()
            .await
            .is_ok_and(|facts| facts.has_pending_input());
        if pending {
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

fn text(error: AsterError) -> String {
    error.to_string()
}
