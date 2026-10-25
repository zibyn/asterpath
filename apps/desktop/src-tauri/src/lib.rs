//! Desktop reference host, command layer (ADR-0035): one resident
//! `Asterpath` for every project, assembled by `asterpath-host` from the same
//! `config.toml` and database as the CLI. Each method backs one Tauri command
//! in `main.rs`; this crate builds and tests without Tauri.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use asterpath::Asterpath;
use asterpath_host::config::{self, Settings};
use asterpath_host::{CWD, NAME, WorkRoot, work_root};
use asterpath_protocol::{CatalogEntry, ParentFilter, SessionQuery};
use serde::Serialize;

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
    /// Hex session id.
    pub id: String,
    pub name: Option<String>,
    pub created_at: i64,
    pub last_activity: i64,
}

impl From<CatalogEntry> for SessionSummary {
    fn from(entry: CatalogEntry) -> Self {
        Self {
            id: entry.session.to_hex(),
            name: entry.metadata.get(NAME).cloned(),
            created_at: entry.created_at,
            last_activity: entry.last_activity,
        }
    }
}

pub struct Desktop {
    asterpath: Asterpath,
    /// The roots added with the folder picker, a JSON array of paths.
    projects_file: PathBuf,
}

impl Desktop {
    pub fn new(asterpath: Asterpath, projects_file: PathBuf) -> Self {
        Self {
            asterpath,
            projects_file,
        }
    }

    /// The default `config.toml` (or `ASTERPATH_CONFIG`) and its database.
    pub async fn open(projects_file: PathBuf) -> Result<Self, String> {
        let settings: Settings = config::load(None, &[])?;
        config::validate(&settings)?;
        let environment = WorkRoot {
            workspace: settings.workspace()?,
            // shortcut: one adapter treats every session as continued (all workspace tools); slice ② creates sessions and needs per-session `new`
            new: false,
            pinned: None,
        };
        // shortcut: no MCP servers yet; they are loaded per work root, which a multi-project process does not have
        let asterpath = config::assemble(&settings, Vec::new(), environment)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Self::new(asterpath, projects_file))
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
        let mut sessions = self.roots(query).await?;
        sessions.sort_by_key(|entry| std::cmp::Reverse(entry.last_activity));
        Ok(sessions.into_iter().map(SessionSummary::from).collect())
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
            let page = self
                .asterpath
                .sessions(query.clone())
                .await
                .map_err(|error| error.to_string())?;
            let Some(last) = page.last() else {
                return Ok(all);
            };
            query.after = Some(last.cursor());
            all.extend(page);
        }
    }
}
