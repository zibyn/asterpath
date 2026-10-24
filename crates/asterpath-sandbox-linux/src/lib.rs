//! Linux bwrap reference backend for the sandbox contract.
//!
//! [`LinuxSandbox`] turns a resolved [`SandboxPolicy`] into a pure command
//! transform (see `docs/design/sandbox.md` §11); it never spawns a process.
//! Callers build the process from the returned [`ConfinedSpawn`].
//!
//! `bwrap` is resolved at confine time. If it cannot be found, confined policies
//! return [`SandboxError::Setup`] and no command is produced; `FullAccess` is a
//! pass-through and does not need bwrap. A bwrap that exists but cannot create
//! namespaces fails when the caller spawns it (exit or spawn error), never by a
//! bare-run fallback.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use asterpath_protocol::{
    ConfinedSpawn, Enforcement, SandboxError, SandboxPolicy, SandboxProvider, SpawnRequest,
};

/// Whole filesystem read-only, with writable roots appended per policy.
const READ_ONLY_FLAGS: [&str; 8] = [
    "--ro-bind",
    "/",
    "/",
    "--dev",
    "/dev",
    "--proc",
    "/proc",
    // Never add `--new-session` without `--unshare-pid`: bash's killpg on the
    // bwrap process group would no longer reach anything inside.
    "--die-with-parent",
];

/// Linux sandbox backend implemented with bubblewrap.
#[derive(Clone, Debug)]
pub struct LinuxSandbox {
    bwrap: PathBuf,
}

impl Default for LinuxSandbox {
    fn default() -> Self {
        Self::new()
    }
}

impl LinuxSandbox {
    /// Use the `bwrap` found on `PATH`.
    pub fn new() -> Self {
        Self {
            bwrap: PathBuf::from("bwrap"),
        }
    }

    /// Use a specific bwrap program, resolved directly when it contains a path
    /// separator and via `PATH` otherwise.
    ///
    /// Primarily for hosts with a nonstandard bwrap location and for tests that
    /// must exercise a missing binary without changing `PATH`.
    pub fn with_bwrap_program(program: impl Into<PathBuf>) -> Self {
        Self {
            bwrap: program.into(),
        }
    }
}

impl SandboxProvider for LinuxSandbox {
    fn confine(&self, request: &SpawnRequest<'_>) -> Result<ConfinedSpawn, SandboxError> {
        request.policy.validate()?;
        if matches!(request.policy, SandboxPolicy::FullAccess) {
            return Ok(ConfinedSpawn::unconfined(request));
        }

        let program = resolve_bwrap(&self.bwrap).ok_or_else(|| SandboxError::Setup {
            message: format!("bwrap not found: {}", self.bwrap.display()),
        })?;

        let mut args: Vec<OsString> = READ_ONLY_FLAGS.iter().map(OsString::from).collect();

        if let SandboxPolicy::WorkspaceWrite { writable_roots } = request.policy {
            for root in writable_roots {
                args.push("--bind".into());
                args.push(root.clone().into_os_string());
                args.push(root.clone().into_os_string());
            }
        }

        if let Some(cwd) = request.cwd {
            args.push("--chdir".into());
            args.push(cwd.as_os_str().to_os_string());
        }

        args.push(request.program.to_os_string());
        args.extend(request.args.iter().cloned());

        Ok(ConfinedSpawn::new(
            program.into_os_string(),
            args,
            // The caller still applies `current_dir`; `--chdir` makes the child
            // itself start there inside the namespace.
            request.cwd.map(Path::to_path_buf),
            request.env.clone(),
            Enforcement::Full,
        ))
    }
}

/// Resolve a bare program name through `PATH`; paths with a separator are used
/// as given. Returns `None` when no regular file exists.
fn resolve_bwrap(program: &Path) -> Option<PathBuf> {
    if program.is_absolute() || program.components().count() > 1 {
        return program.is_file().then(|| program.to_path_buf());
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(program))
        .find(|candidate| candidate.is_file())
}
