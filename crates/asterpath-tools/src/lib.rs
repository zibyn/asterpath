//! Root-bound file and shell tools: `read_file`, `list_dir`, `grep`,
//! `write_file`, `edit_file` and `bash`.
//!
//! Each tool is built for one absolute root directory; every path argument is
//! resolved against it and anything that leaves it is `Denied`. A host serving
//! several projects builds one tool set per project in its session
//! environment (ADR-0037). `bash` gets only the minimal inherited environment
//! plus the session's `ToolContext::spawn` variables, and runs under the
//! session's sandbox policy when it names one.
//!
//! The file tools open each path component by component beneath the root
//! without following symlinks, and read and write through those descriptors,
//! so a path swapped for a symlink mid-call cannot escape. Any symlink on a
//! path argument is therefore `Denied`, even one pointing inside the root;
//! `list_dir` and `grep` show or skip symlinks but never enter them. On
//! non-Unix platforms the file tools always return `Denied`.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use asterpath_protocol::{
    ConfinedSpawn, Parallelism, Part, ReplaySafety, SandboxPolicy, SandboxProvider, SideEffect,
    SpawnRequest, Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput,
};
use futures_core::future::BoxFuture;
use serde::Deserialize;
use serde::de::DeserializeOwned;

#[cfg(unix)]
mod fs;

/// Construction failed because the root is not an absolute path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelativeRoot(pub PathBuf);

impl fmt::Display for RelativeRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tool root must be absolute: {}", self.0.display())
    }
}

impl std::error::Error for RelativeRoot {}

type Tools = Result<Vec<Arc<dyn Tool>>, RelativeRoot>;

/// `read_file`, `list_dir`, `grep`.
pub fn read_only(root: PathBuf) -> Tools {
    Ok(vec![
        read_file(root.clone())?,
        list_dir(root.clone())?,
        grep(root)?,
    ])
}

/// [`read_only`] plus `write_file`, `edit_file` and `bash` (see [`bash`] for `timeout`).
pub fn coding(
    root: PathBuf,
    sandbox: Option<Arc<dyn SandboxProvider>>,
    policy: SandboxPolicy,
    timeout: Duration,
) -> Tools {
    let mut tools = read_only(root.clone())?;
    tools.push(write_file(root.clone())?);
    tools.push(edit_file(root.clone())?);
    tools.push(bash(root, sandbox, policy, timeout)?);
    Ok(tools)
}

pub fn read_file(root: PathBuf) -> Result<Arc<dyn Tool>, RelativeRoot> {
    RootTool::build(
        root,
        Op::Fs(FsOp::Read),
        "read_file",
        "Read a UTF-8 text file. `path` is relative to the project root.",
        serde_json::json!({
            "type": "object",
            "properties": { "path": { "type": "string" } },
            "required": ["path"],
            "additionalProperties": false
        }),
    )
}

pub fn list_dir(root: PathBuf) -> Result<Arc<dyn Tool>, RelativeRoot> {
    RootTool::build(
        root,
        Op::Fs(FsOp::List),
        "list_dir",
        "List a directory relative to the project root (default: the root). Directories end with `/`.",
        serde_json::json!({
            "type": "object",
            "properties": { "path": { "type": "string" } },
            "additionalProperties": false
        }),
    )
}

pub fn grep(root: PathBuf) -> Result<Arc<dyn Tool>, RelativeRoot> {
    RootTool::build(
        root,
        Op::Fs(FsOp::Grep),
        "grep",
        "Find lines containing a literal string in files under `path` (default: the root). Prints `path:line: text`.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string" },
                "path": { "type": "string" }
            },
            "required": ["pattern"],
            "additionalProperties": false
        }),
    )
}

pub fn write_file(root: PathBuf) -> Result<Arc<dyn Tool>, RelativeRoot> {
    RootTool::build(
        root,
        Op::Fs(FsOp::Write),
        "write_file",
        "Create or overwrite a file relative to the project root; parent directories are created.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "content": { "type": "string" }
            },
            "required": ["path", "content"],
            "additionalProperties": false
        }),
    )
}

pub fn edit_file(root: PathBuf) -> Result<Arc<dyn Tool>, RelativeRoot> {
    RootTool::build(
        root,
        Op::Fs(FsOp::Edit),
        "edit_file",
        "Replace `old_string` with `new_string` in a file; `old_string` must occur exactly once.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "old_string": { "type": "string" },
                "new_string": { "type": "string" }
            },
            "required": ["path", "old_string", "new_string"],
            "additionalProperties": false
        }),
    )
}

/// Runs `bash -c <command>` in the root through `sandbox`. Without a provider
/// only `FullAccess` starts a process; any other policy fails closed.
///
/// On Unix the process (the sandbox launcher, if any) leads its own process
/// group. Cancelling sends the group SIGTERM, then SIGKILL after 500ms;
/// dropping the call mid-run SIGKILLs the group. After bash exits the pipes
/// are read for at most 1s more, so a background process holding them cannot
/// hang the call; the output then says reading stopped. After a successful
/// exit, background processes keep running and are no longer subject to
/// cancellation. Elsewhere only bash itself is killed.
///
/// `timeout` becomes the descriptor's deadline: the engine cancels the call
/// when it expires and records a `Timeout` carrying the tail of the output.
/// The model cannot change it.
pub fn bash(
    root: PathBuf,
    sandbox: Option<Arc<dyn SandboxProvider>>,
    policy: SandboxPolicy,
    timeout: Duration,
) -> Result<Arc<dyn Tool>, RelativeRoot> {
    RootTool::build(
        root,
        Op::Bash {
            sandbox,
            policy,
            timeout,
        },
        "bash",
        "Run a bash command with the project root as the working directory. Returns stdout, stderr and the exit status.",
        serde_json::json!({
            "type": "object",
            "properties": { "command": { "type": "string" } },
            "required": ["command"],
            "additionalProperties": false
        }),
    )
}

/// A host default for [`bash`]'s `timeout`.
pub const DEFAULT_BASH_TIMEOUT: Duration = Duration::from_secs(120);

/// Upper bound on what any tool reads into memory or returns; the pipeline
/// truncates further to `ToolConfig::max_output_bytes`.
const MAX_BYTES: usize = 1 << 20;

#[derive(Clone, Copy)]
enum FsOp {
    Read,
    List,
    Grep,
    Write,
    Edit,
}

enum Op {
    Fs(FsOp),
    Bash {
        sandbox: Option<Arc<dyn SandboxProvider>>,
        policy: SandboxPolicy,
        timeout: Duration,
    },
}

struct RootTool {
    root: PathBuf,
    op: Op,
    descriptor: ToolDescriptor,
}

impl RootTool {
    fn build(
        root: PathBuf,
        op: Op,
        name: &str,
        description: &str,
        schema: serde_json::Value,
    ) -> Result<Arc<dyn Tool>, RelativeRoot> {
        if !root.is_absolute() {
            return Err(RelativeRoot(root));
        }
        let read_only = matches!(op, Op::Fs(FsOp::Read | FsOp::List | FsOp::Grep));
        let descriptor = ToolDescriptor::new(ToolName::new(name), description.to_owned(), schema)
            .with_parallelism(if read_only {
                Parallelism::Parallel
            } else {
                Parallelism::Exclusive
            })
            .with_replay(if read_only {
                ReplaySafety::Safe
            } else {
                ReplaySafety::Unsafe
            })
            .with_side_effect(if read_only {
                SideEffect::ReadOnly
            } else {
                SideEffect::Mutating
            })
            .with_timeout(match &op {
                Op::Bash { timeout, .. } => Some(*timeout),
                Op::Fs(_) => None,
            });
        Ok(Arc::new(Self {
            root,
            op,
            descriptor,
        }))
    }

    async fn run(&self, args: ToolArgs, ctx: ToolContext) -> Result<String, ToolError> {
        match &self.op {
            &Op::Fs(op) => {
                let root = self.root.clone();
                tokio::task::spawn_blocking(move || run_fs(&root, op, &args, &ctx))
                    .await
                    .map_err(|error| ToolError::Internal {
                        message: format!("tool task failed: {error}"),
                    })?
            }
            Op::Bash {
                sandbox, policy, ..
            } => {
                let args: BashArgs = parse(&args)?;
                let root = self
                    .root
                    .canonicalize()
                    .map_err(|error| ToolError::Internal {
                        message: format!(
                            "cannot open project root {}: {error}",
                            self.root.display()
                        ),
                    })?;
                run_bash(&root, &args.command, sandbox.as_deref(), policy, &ctx).await
            }
        }
    }
}

#[cfg(unix)]
use fs::run as run_fs;

/// File tools need `openat(O_NOFOLLOW)` to stay beneath the root; without it
/// they fail closed (ADR-0019).
// shortcut: non-Unix hosts get no file tools, add a handle-relative backend when one ships there.
#[cfg(not(unix))]
fn run_fs(_: &Path, _: FsOp, _: &ToolArgs, _: &ToolContext) -> Result<String, ToolError> {
    Err(ToolError::Denied {
        message: "file tools are only supported on Unix".to_owned(),
    })
}

impl Tool for RootTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let text = self.run(args, ctx).await?;
            Ok(ToolOutput::new(vec![Part::Text { text }]))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PathArgs {
    path: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListArgs {
    path: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrepArgs {
    pattern: String,
    path: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    path: String,
    content: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditArgs {
    path: String,
    old_string: String,
    new_string: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BashArgs {
    command: String,
}

fn parse<T: DeserializeOwned>(args: &ToolArgs) -> Result<T, ToolError> {
    serde_json::from_str(args.as_str()).map_err(|error| invalid(error.to_string()))
}

fn invalid(message: impl Into<String>) -> ToolError {
    ToolError::InvalidArgs {
        message: message.into(),
    }
}

fn io(relative: &str, error: std::io::Error) -> ToolError {
    match error.kind() {
        std::io::ErrorKind::NotFound => ToolError::NotFound {
            message: format!("{relative}: {error}"),
        },
        _ => ToolError::Internal {
            message: format!("{relative}: {error}"),
        },
    }
}

async fn run_bash(
    root: &Path,
    command: &str,
    sandbox: Option<&dyn SandboxProvider>,
    policy: &SandboxPolicy,
    ctx: &ToolContext,
) -> Result<String, ToolError> {
    let args = [OsString::from("-c"), OsString::from(command)];
    let env: BTreeMap<OsString, OsString> = ctx.spawn.complete();
    let policy = ctx.spawn.sandbox.as_ref().unwrap_or(policy);
    let request = SpawnRequest::new(OsStr::new("bash"), &args, Some(root), &env, policy);
    let confined = match (sandbox, policy) {
        (Some(sandbox), _) => sandbox
            .confine(&request)
            .map_err(|error| ToolError::Denied {
                message: error.to_string(),
            })?,
        (None, SandboxPolicy::FullAccess) => ConfinedSpawn::unconfined(&request),
        (None, _) => {
            return Err(ToolError::Denied {
                message: "no sandbox backend for the requested policy".to_owned(),
            });
        }
    };
    let mut process = tokio::process::Command::new(&confined.program);
    process
        .args(&confined.args)
        .env_clear()
        .envs(&confined.env)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    // Own process group (the sandbox launcher's, if any), so killpg reaches every descendant.
    #[cfg(unix)]
    process.process_group(0);
    #[cfg(not(unix))]
    process.kill_on_drop(true);
    if let Some(cwd) = &confined.cwd {
        process.current_dir(cwd);
    }
    let mut child = process.spawn().map_err(|error| ToolError::Internal {
        message: format!("cannot start bash: {error}"),
    })?;
    let mut group = GroupGuard(child.id());
    let out_pipe = child.stdout.take().expect("piped stdout");
    let err_pipe = child.stderr.take().expect("piped stderr");
    let (mut stdout, mut stdout_cut, mut stderr, mut stderr_cut) =
        (Vec::new(), false, Vec::new(), false);
    let failed = |error: std::io::Error| ToolError::Internal {
        message: format!("bash failed: {error}"),
    };
    let mut read_done = false;
    let output = {
        let mut reads = std::pin::pin!(async {
            tokio::try_join!(
                drain(out_pipe, &mut stdout, &mut stdout_cut),
                drain(err_pipe, &mut stderr, &mut stderr_cut)
            )
        });
        let output = async {
            let status = tokio::select! {
                read = &mut reads => {
                    read_done = true;
                    read?;
                    child.wait().await?
                }
                status = child.wait() => {
                    group.disarm();
                    let status = status?;
                    // A background process that inherited the pipes may hold them open forever.
                    // shortcut: such a process outlives a successful bash exit and its output
                    // after READ_AFTER_EXIT is lost; kill the group on exit if hosts need that.
                    match tokio::time::timeout(READ_AFTER_EXIT, &mut reads).await {
                        Ok(read) => {
                            read_done = true;
                            read.map(|_| ())?
                        }
                        Err(_) => return Ok((status, false)),
                    }
                    status
                }
            };
            group.disarm();
            Ok::<_, std::io::Error>((status, true))
        };
        tokio::select! {
            output = output => Some(output),
            () = ctx.cancel.cancelled() => {
                if let Some(pid) = group.0.take() {
                    signal_group(pid, Signal::Term, true);
                    let exited = tokio::time::timeout(KILL_WAIT, child.wait()).await.is_ok();
                    // Even if bash exited, a descendant may have ignored SIGTERM. Once bash
                    // is reaped its pid may be reused, so only the group is signalled.
                    signal_group(pid, Signal::Kill, !exited);
                }
                // Keep what was already written: the pipes reach EOF once the group is dead.
                if !read_done {
                    let _ = tokio::time::timeout(KILL_WAIT, &mut reads).await;
                }
                None
            }
        }
    };
    let Some(output) = output else {
        let mut message = String::from("bash stopped before exiting");
        let mut seen = String::from_utf8_lossy(&stdout).into_owned();
        if !stderr.is_empty() {
            seen.push_str("\n[stderr]\n");
            seen.push_str(&String::from_utf8_lossy(&stderr));
        }
        if !seen.is_empty() {
            let mut start = seen.len().saturating_sub(CANCEL_TAIL);
            while !seen.is_char_boundary(start) {
                start += 1;
            }
            message.push_str("; last output:\n");
            message.push_str(&seen[start..]);
        }
        return Err(ToolError::Cancelled { message });
    };
    let (status, drained) = output.map_err(failed)?;
    let mut text = String::from_utf8_lossy(&stdout).into_owned();
    if !drained {
        text.push_str("\n[a background process still holds the output; stopped reading]");
    }
    if stdout_cut {
        text.push_str("\n[stdout truncated]");
    }
    if !stderr.is_empty() {
        text.push_str("\n[stderr]\n");
        text.push_str(&String::from_utf8_lossy(&stderr));
        if stderr_cut {
            text.push_str("\n[stderr truncated]");
        }
    }
    text.push_str(&format!("\n[exit status: {status}]"));
    Ok(text)
}

/// Read `pipe` to the end, keeping the first [`MAX_BYTES`] / 2 bytes; draining
/// the rest keeps the child from blocking on a full pipe.
/// Writes into `kept`/`cut` as it goes, so a read abandoned midway keeps what arrived.
async fn drain(
    mut pipe: impl tokio::io::AsyncRead + Unpin,
    kept: &mut Vec<u8>,
    cut: &mut bool,
) -> std::io::Result<()> {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 8192];
    loop {
        let read = pipe.read(&mut buf).await?;
        if read == 0 {
            return Ok(());
        }
        let room = (MAX_BYTES / 2).saturating_sub(kept.len());
        kept.extend_from_slice(&buf[..read.min(room)]);
        *cut |= read > room;
    }
}

/// Bytes of captured output kept in the error when a run is cancelled.
const CANCEL_TAIL: usize = 4096;
/// How long to keep reading the pipes after bash exits.
const READ_AFTER_EXIT: Duration = Duration::from_secs(1);
/// Grace between SIGTERM and SIGKILL on cancel.
const KILL_WAIT: Duration = Duration::from_millis(500);

/// SIGKILLs bash's process group when dropped while armed (the `invoke`
/// future was dropped mid-run); disarmed once bash has exited.
struct GroupGuard(Option<u32>);

impl GroupGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for GroupGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            signal_group(pid, Signal::Kill, true);
        }
    }
}

enum Signal {
    Term,
    Kill,
}

/// Signals the process group led by `pid`; if the group is already gone and
/// `pid_fallback` (bash is not yet reaped, so `pid` is still ours), signals `pid`.
#[cfg(unix)]
fn signal_group(pid: u32, signal: Signal, pid_fallback: bool) {
    // `kill(0)` would signal our own group; never do that.
    if pid == 0 {
        return;
    }
    let signal = match signal {
        Signal::Term => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
    };
    // SAFETY: kill(2) with our own child's pid and a valid signal.
    if unsafe { libc::kill(-(pid as libc::pid_t), signal) } != 0 && pid_fallback {
        // SAFETY: same child, direct-pid fallback.
        unsafe { libc::kill(pid as libc::pid_t, signal) };
    }
}

/// Without process groups, dropping the child (`kill_on_drop`) kills bash only.
#[cfg(not(unix))]
fn signal_group(_pid: u32, _signal: Signal, _pid_fallback: bool) {}
