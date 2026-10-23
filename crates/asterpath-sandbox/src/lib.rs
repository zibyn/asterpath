//! Sandbox contract: confine a spawn request according to a resolved policy.
//!
//! [`SandboxProvider::confine`] is a pure transform at a process spawn point.
//! Policy resolution, audit and backend construction belong to the host; this
//! crate has no dependencies and no features (ADR-0019).

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
