//! Sandbox contract: confine a spawn request according to a resolved policy.
//!
//! [`SandboxProvider::confine`] is a pure transform at a process spawn point.
//! Policy resolution, audit and backend construction belong to the host
//! (ADR-0019). The contract lives in protocol so hosts' own spawn-type tools can
//! use it without depending on MCP (ADR-0027).

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{Path, PathBuf};

/// Turns "the command we want to start" into "the command to actually start".
///
/// The provider is stateless: the caller resolves the policy and supplies the
/// complete environment. Callers construct the process themselves.
pub trait SandboxProvider: Send + Sync + 'static {
    fn confine(&self, request: &SpawnRequest<'_>) -> Result<ConfinedSpawn, SandboxError>;
}

/// A spawn point's request. Borrows; the caller keeps ownership.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpawnRequest<'a> {
    pub program: &'a OsStr,
    pub args: &'a [OsString],
    pub cwd: Option<&'a Path>,
    /// Complete environment; inherited values already merged by the caller.
    pub env: &'a BTreeMap<OsString, OsString>,
    pub policy: &'a SandboxPolicy,
}

impl<'a> SpawnRequest<'a> {
    pub fn new(
        program: &'a OsStr,
        args: &'a [OsString],
        cwd: Option<&'a Path>,
        env: &'a BTreeMap<OsString, OsString>,
        policy: &'a SandboxPolicy,
    ) -> Self {
        Self {
            program,
            args,
            cwd,
            env,
            policy,
        }
    }
}

/// What a session's spawned processes start with (ADR-0037). The host's
/// session environment supplies it; tools receive it in `ToolContext`.
/// `Debug` prints variable names only: values may be secrets.
#[non_exhaustive]
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SpawnEnv {
    /// The session's policy; `None` leaves the tool's own.
    pub sandbox: Option<SandboxPolicy>,
    /// Passed on top of [`INHERITED_ENV`]; nothing else of the process
    /// environment reaches a child.
    pub vars: BTreeMap<OsString, OsString>,
}

impl SpawnEnv {
    /// The complete environment of a spawned process: the [`INHERITED_ENV`]
    /// variables this process has, overridden by `vars`.
    pub fn complete(&self) -> BTreeMap<OsString, OsString> {
        let mut env: BTreeMap<OsString, OsString> = INHERITED_ENV
            .iter()
            .filter_map(|name| Some((OsString::from(name), std::env::var_os(name)?)))
            .collect();
        env.extend(self.vars.clone());
        env
    }
}

impl fmt::Debug for SpawnEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnEnv")
            .field("sandbox", &self.sandbox)
            .field("vars", &self.vars.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// The only process variables a spawned child inherits (ADR-0037).
pub const INHERITED_ENV: &[&str] = &[
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR", "TERM",
];

/// The command to actually start. Owns everything the caller needs.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfinedSpawn {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub cwd: Option<PathBuf>,
    /// Complete environment; the provider may remove or append entries.
    pub env: BTreeMap<OsString, OsString>,
    pub enforcement: Enforcement,
}

impl ConfinedSpawn {
    pub fn new(
        program: OsString,
        args: Vec<OsString>,
        cwd: Option<PathBuf>,
        env: BTreeMap<OsString, OsString>,
        enforcement: Enforcement,
    ) -> Self {
        Self {
            program,
            args,
            cwd,
            env,
            enforcement,
        }
    }

    /// The request unchanged with `Enforcement::Full`: the `FullAccess`
    /// pass-through and the no-provider path at a spawn point.
    pub fn unconfined(request: &SpawnRequest<'_>) -> Self {
        Self::new(
            request.program.to_os_string(),
            request.args.to_vec(),
            request.cwd.map(Path::to_path_buf),
            request.env.clone(),
            Enforcement::Full,
        )
    }
}

/// Filesystem policy resolved by the host.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SandboxPolicy {
    /// Whole filesystem readable, nothing writable.
    ReadOnly,
    /// Whole filesystem readable; only these absolute roots writable.
    WorkspaceWrite { writable_roots: Vec<PathBuf> },
    /// No confinement.
    FullAccess,
}

impl SandboxPolicy {
    /// Rejects policies no backend may enforce. Providers call this first: an
    /// empty or relative root set fails closed instead of degrading to
    /// read-only or binding relative paths.
    pub fn validate(&self) -> Result<(), SandboxError> {
        let Self::WorkspaceWrite { writable_roots } = self else {
            return Ok(());
        };
        if writable_roots.is_empty() {
            return Err(SandboxError::Unsupported {
                reason: "workspace-write requires at least one writable root".to_owned(),
            });
        }
        if let Some(root) = writable_roots.iter().find(|root| !root.is_absolute()) {
            return Err(SandboxError::Unsupported {
                reason: format!("writable root must be absolute: {}", root.display()),
            });
        }
        Ok(())
    }
}

/// How completely the backend enforced the requested policy.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Enforcement {
    Full,
    /// At least one requested restriction was not enforced.
    Partial {
        unenforced: Vec<SandboxAspect>,
    },
}

/// A policy dimension that may be left unenforced.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SandboxAspect {
    FilesystemWrite,
}

/// Failure to confine or start a sandboxed process.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SandboxError {
    /// The backend cannot fully enforce the requested policy.
    Unsupported { reason: String },
    /// Policy denies this spawn; no process was started.
    Refused { reason: String },
    /// The backend failed to construct the confinement.
    Setup { message: String },
}

impl fmt::Display for SandboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { reason } => write!(f, "sandbox: unsupported: {reason}"),
            Self::Refused { reason } => write!(f, "sandbox: refused: {reason}"),
            Self::Setup { message } => write!(f, "sandbox: setup failed: {message}"),
        }
    }
}

impl std::error::Error for SandboxError {}
