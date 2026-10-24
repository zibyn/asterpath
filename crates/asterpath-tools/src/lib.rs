//! Root-bound file and shell tools: `read_file`, `list_dir`, `grep`,
//! `write_file`, `edit_file` and `bash`.
//!
//! Each tool is built for one absolute root directory; every path argument is
//! resolved against it and anything that leaves it is `Denied`. A host serving
//! several projects builds one tool set (and one `Asterpath`) per project.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use asterpath_protocol::{
    ConfinedSpawn, Parallelism, Part, Provenance, ReplaySafety, SandboxPolicy, SandboxProvider,
    SideEffect, SpawnRequest, Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName,
    ToolOutput,
};
use futures_core::future::BoxFuture;
use serde::Deserialize;
use serde::de::DeserializeOwned;

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

/// [`read_only`] plus `write_file`, `edit_file` and `bash`.
pub fn coding(
    root: PathBuf,
    sandbox: Option<Arc<dyn SandboxProvider>>,
    policy: SandboxPolicy,
) -> Tools {
    let mut tools = read_only(root.clone())?;
    tools.push(write_file(root.clone())?);
    tools.push(edit_file(root.clone())?);
    tools.push(bash(root, sandbox, policy)?);
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
pub fn bash(
    root: PathBuf,
    sandbox: Option<Arc<dyn SandboxProvider>>,
    policy: SandboxPolicy,
) -> Result<Arc<dyn Tool>, RelativeRoot> {
    RootTool::build(
        root,
        Op::Bash { sandbox, policy },
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
        let descriptor = ToolDescriptor {
            name: ToolName::new(name),
            description: description.to_owned(),
            schema,
            parallelism: if read_only {
                Parallelism::Parallel
            } else {
                Parallelism::Exclusive
            },
            replay: if read_only {
                ReplaySafety::Safe
            } else {
                ReplaySafety::Unsafe
            },
            side_effect: if read_only {
                SideEffect::ReadOnly
            } else {
                SideEffect::Mutating
            },
            provenance: Provenance::InProcess,
            timeout: None,
            max_output_bytes: None,
        };
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
            Op::Bash { sandbox, policy } => {
                // shortcut: no default timeout, so a command that never exits runs until the host cancels; set descriptor.timeout once a policy for unattended hosts is decided.
                let args: BashArgs = parse(&args)?;
                let root = resolve(&self.root, ".")?;
                run_bash(&root, &args.command, sandbox.as_deref(), policy, &ctx).await
            }
        }
    }
}

/// Resolve `relative` inside the root. The deepest existing ancestor is
/// canonicalized, so `..`, absolute paths and symlinks that leave the root
/// are all caught; the not-yet-existing tail is plain names only.
// shortcut: check-then-use, so a symlink swapped in between can escape; open with O_NOFOLLOW per component if hosts run untrusted concurrent writers.
fn resolve(root: &Path, relative: &str) -> Result<PathBuf, ToolError> {
    let denied = || ToolError::Denied {
        message: format!("path is outside the project root: {relative}"),
    };
    let relative = Path::new(relative);
    if relative
        .components()
        .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
    {
        return Err(denied());
    }
    let root = root.canonicalize().map_err(|error| ToolError::Internal {
        message: format!("cannot open project root {}: {error}", root.display()),
    })?;
    let path = root.join(relative);
    let mut existing = path.as_path();
    while existing.symlink_metadata().is_err() {
        existing = existing.parent().ok_or_else(denied)?;
    }
    // A dangling symlink fails to canonicalize: deny rather than follow it.
    let real = existing.canonicalize().map_err(|_| denied())?;
    if !real.starts_with(&root) {
        return Err(denied());
    }
    let tail = path.strip_prefix(existing).expect("ancestor of path");
    // `join("")` would append a trailing slash and make a file look like a directory.
    Ok(if tail.as_os_str().is_empty() {
        real
    } else {
        real.join(tail)
    })
}

fn run_fs(root: &Path, op: FsOp, args: &ToolArgs, ctx: &ToolContext) -> Result<String, ToolError> {
    match op {
        FsOp::Read => {
            let args: PathArgs = parse(args)?;
            read_capped(&resolve(root, &args.path)?, &args.path)
        }
        FsOp::List => {
            let args: ListArgs = parse(args)?;
            let relative = args.path.as_deref().unwrap_or(".");
            list(&resolve(root, relative)?, relative)
        }
        FsOp::Grep => {
            let args: GrepArgs = parse(args)?;
            if args.pattern.is_empty() {
                return Err(invalid("pattern must not be empty"));
            }
            let relative = args.path.as_deref().unwrap_or(".");
            let start = resolve(root, relative)?;
            let root = resolve(root, ".")?;
            let mut out = String::new();
            search(&root, &start, &args.pattern, ctx, &mut out)?;
            if out.len() >= MAX_BYTES {
                out.push_str("[output truncated]\n");
            }
            Ok(out)
        }
        FsOp::Write => {
            let args: WriteArgs = parse(args)?;
            let path = resolve(root, &args.path)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|error| io(&args.path, error))?;
            }
            replace_file(&path, args.content.as_bytes()).map_err(|error| io(&args.path, error))?;
            Ok(format!(
                "wrote {} bytes to {}",
                args.content.len(),
                args.path
            ))
        }
        FsOp::Edit => {
            let args: EditArgs = parse(args)?;
            if args.old_string.is_empty() {
                return Err(invalid("old_string must not be empty"));
            }
            let path = resolve(root, &args.path)?;
            let content = read_text(&path, &args.path)?;
            match content.matches(&args.old_string).count() {
                1 => {}
                0 => return Err(invalid(format!("old_string not found in {}", args.path))),
                count => {
                    return Err(invalid(format!(
                        "old_string occurs {count} times in {}; it must be unique",
                        args.path
                    )));
                }
            }
            let updated = content.replacen(&args.old_string, &args.new_string, 1);
            replace_file(&path, updated.as_bytes()).map_err(|error| io(&args.path, error))?;
            Ok(format!("edited {}", args.path))
        }
    }
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

fn read_text(path: &Path, relative: &str) -> Result<String, ToolError> {
    let bytes = std::fs::read(path).map_err(|error| io(relative, error))?;
    String::from_utf8(bytes).map_err(|_| invalid(format!("{relative} is not valid UTF-8")))
}

/// The first [`MAX_BYTES`] of a UTF-8 file, with a marker when it is longer.
fn read_capped(path: &Path, relative: &str) -> Result<String, ToolError> {
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(|error| io(relative, error))?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io(relative, error))?;
    let truncated = bytes.len() > MAX_BYTES;
    bytes.truncate(MAX_BYTES);
    let mut text = match String::from_utf8(bytes) {
        Ok(text) => text,
        // Only a character cut by the cap may be incomplete; anything else is not UTF-8.
        Err(error) if truncated && error.utf8_error().error_len().is_none() => {
            let valid = error.utf8_error().valid_up_to();
            let mut bytes = error.into_bytes();
            bytes.truncate(valid);
            String::from_utf8(bytes).expect("valid prefix")
        }
        Err(_) => return Err(invalid(format!("{relative} is not valid UTF-8"))),
    };
    if truncated {
        text.push_str(&format!(
            "\n[truncated: {relative} is larger than {MAX_BYTES} bytes]"
        ));
    }
    Ok(text)
}

/// Write `contents` to a temp file beside `path`, then rename it over `path`,
/// so a crash leaves either the old or the new file. Keeps an existing mode.
fn replace_file(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut name = OsString::from(".");
    name.push(path.file_name().unwrap_or_default());
    name.push(format!(
        ".{}.{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let temp = path.with_file_name(name);
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(contents)?;
        if let Ok(meta) = std::fs::metadata(path) {
            file.set_permissions(meta.permissions())?;
        }
        file.sync_all()?;
        std::fs::rename(&temp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn list(path: &Path, relative: &str) -> Result<String, ToolError> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(path).map_err(|error| io(relative, error))? {
        let entry = entry.map_err(|error| io(relative, error))?;
        let mut name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            name.push('/');
        }
        names.push(name);
    }
    names.sort();
    Ok(names.join("\n"))
}

/// Depth-first literal search. Symlinks are not followed, so the walk never
/// leaves the root; `.git` and files that are not UTF-8 are skipped. Stops
/// once `out` reaches [`MAX_BYTES`].
fn search(
    root: &Path,
    path: &Path,
    pattern: &str,
    ctx: &ToolContext,
    out: &mut String,
) -> Result<(), ToolError> {
    if ctx.cancel.is_cancelled() {
        return Err(ToolError::Cancelled {
            message: "grep cancelled".to_owned(),
        });
    }
    if out.len() >= MAX_BYTES || path.file_name() == Some(OsStr::new(".git")) {
        return Ok(());
    }
    let display = path
        .strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string();
    let kind = path
        .symlink_metadata()
        .map_err(|error| io(&display, error))?
        .file_type();
    if kind.is_dir() {
        let mut children: Vec<PathBuf> = std::fs::read_dir(path)
            .map_err(|error| io(&display, error))?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect();
        children.sort();
        for child in children {
            search(root, &child, pattern, ctx, out)?;
        }
    } else if kind.is_file()
        && let Ok(text) = std::fs::read_to_string(path)
    {
        for (number, line) in text.lines().enumerate() {
            if out.len() >= MAX_BYTES {
                break;
            }
            if line.contains(pattern) {
                out.push_str(&format!("{display}:{}: {line}\n", number + 1));
            }
        }
    }
    Ok(())
}

async fn run_bash(
    root: &Path,
    command: &str,
    sandbox: Option<&dyn SandboxProvider>,
    policy: &SandboxPolicy,
    ctx: &ToolContext,
) -> Result<String, ToolError> {
    let args = [OsString::from("-c"), OsString::from(command)];
    let env: BTreeMap<OsString, OsString> = std::env::vars_os().collect();
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
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    if let Some(cwd) = &confined.cwd {
        process.current_dir(cwd);
    }
    // shortcut: kill_on_drop signals only the direct child; put it in its own process group and killpg if backgrounded grandchildren must die too.
    let mut child = process.spawn().map_err(|error| ToolError::Internal {
        message: format!("cannot start bash: {error}"),
    })?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let output = async { tokio::try_join!(drain(stdout), drain(stderr), child.wait()) };
    let ((stdout, stdout_cut), (stderr, stderr_cut), status) = tokio::select! {
        output = output => output.map_err(|error| ToolError::Internal {
            message: format!("bash failed: {error}"),
        })?,
        () = ctx.cancel.cancelled() => {
            return Err(ToolError::Cancelled { message: "bash cancelled".to_owned() });
        }
    };
    let mut text = String::from_utf8_lossy(&stdout).into_owned();
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
async fn drain(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<(Vec<u8>, bool)> {
    use tokio::io::AsyncReadExt;
    let mut kept = Vec::new();
    let mut cut = false;
    let mut buf = [0u8; 8192];
    loop {
        let read = pipe.read(&mut buf).await?;
        if read == 0 {
            return Ok((kept, cut));
        }
        let room = (MAX_BYTES / 2).saturating_sub(kept.len());
        kept.extend_from_slice(&buf[..read.min(room)]);
        cut |= read > room;
    }
}
