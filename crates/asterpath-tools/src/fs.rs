//! File tool IO, resolved beneath the root by file descriptor.
//!
//! Every call opens the root, then walks the relative path one component at a
//! time with `openat(O_NOFOLLOW)`, and does all reading and writing through the
//! resulting descriptors. Nothing is looked up by full path after the check, so
//! a directory swapped for a symlink mid-call cannot redirect the operation
//! outside the root. Any symlink on the path is `Denied`, even one that points
//! back inside the root.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::path::{Component, Path, PathBuf};

use asterpath_protocol::{ToolArgs, ToolContext, ToolError};
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags};
use rustix::io::Errno;

use crate::{
    EditArgs, FsOp, GrepArgs, ListArgs, MAX_BYTES, PathArgs, WriteArgs, invalid, io, parse,
};

pub(crate) fn run(
    root: &Path,
    op: FsOp,
    args: &ToolArgs,
    ctx: &ToolContext,
) -> Result<String, ToolError> {
    match op {
        FsOp::Read => {
            let args: PathArgs = parse(args)?;
            let (dir, name) = resolve(root, &args.path, false)?;
            read_capped(open_file(&dir, name.as_deref(), &args.path)?, &args.path)
        }
        FsOp::List => {
            let args: ListArgs = parse(args)?;
            let relative = args.path.as_deref().unwrap_or(".");
            let (dir, name) = resolve(root, relative, false)?;
            let (fd, kind) = open_any(&dir, name.as_deref(), relative)?;
            if kind != FileType::Directory {
                return Err(invalid(format!("{relative} is not a directory")));
            }
            list(&fd, relative)
        }
        FsOp::Grep => {
            let args: GrepArgs = parse(args)?;
            if args.pattern.is_empty() {
                return Err(invalid("pattern must not be empty"));
            }
            let relative = args.path.as_deref().unwrap_or(".");
            let (dir, name) = resolve(root, relative, false)?;
            let (fd, kind) = open_any(&dir, name.as_deref(), relative)?;
            let display: PathBuf = normal_parts(relative)?.into_iter().collect();
            let mut out = String::new();
            if display.file_name() != Some(OsStr::new(".git")) {
                search(fd, kind, &display, &args.pattern, ctx, &mut out)?;
            }
            if out.len() >= MAX_BYTES {
                out.push_str("[output truncated]\n");
            }
            Ok(out)
        }
        FsOp::Write => {
            let args: WriteArgs = parse(args)?;
            let (dir, name) = resolve(root, &args.path, true)?;
            let name = name.ok_or_else(|| invalid("path must name a file"))?;
            replace_file(&dir, &name, args.content.as_bytes(), &args.path)?;
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
            let (dir, name) = resolve(root, &args.path, false)?;
            let name = name.ok_or_else(|| invalid("path must name a file"))?;
            let content = read_text(open_file(&dir, Some(&name), &args.path)?, &args.path)?;
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
            replace_file(&dir, &name, updated.as_bytes(), &args.path)?;
            Ok(format!("edited {}", args.path))
        }
    }
}

fn denied(relative: &str) -> ToolError {
    ToolError::Denied {
        message: format!("path is outside the project root or crosses a symlink: {relative}"),
    }
}

/// The plain names in `relative`; `..`, absolute paths and prefixes are denied.
fn normal_parts(relative: &str) -> Result<Vec<&OsStr>, ToolError> {
    Path::new(relative)
        .components()
        .filter(|part| *part != Component::CurDir)
        .map(|part| match part {
            Component::Normal(name) => Ok(name),
            _ => Err(denied(relative)),
        })
        .collect()
}

/// Open the parent directory of `relative` beneath `root`, returning it and the
/// last component (`None` when `relative` names the root itself). With
/// `create`, missing parent directories are made one at a time.
fn resolve(
    root: &Path,
    relative: &str,
    create: bool,
) -> Result<(OwnedFd, Option<OsString>), ToolError> {
    let mut parts = normal_parts(relative)?;
    let last = parts.pop().map(OsStr::to_owned);
    // The root path is host-supplied and trusted, so following symlinks in it is fine.
    let mut dir =
        rustix::fs::open(root, dir_flags() - OFlags::NOFOLLOW, Mode::empty()).map_err(|error| {
            ToolError::Internal {
                message: format!("cannot open project root {}: {error}", root.display()),
            }
        })?;
    for part in parts {
        let mut opened = rustix::fs::openat(&dir, part, dir_flags(), Mode::empty());
        if create && matches!(opened, Err(Errno::NOENT)) {
            match rustix::fs::mkdirat(&dir, part, Mode::from_raw_mode(0o777)) {
                Ok(()) | Err(Errno::EXIST) => {}
                Err(error) => return Err(io(relative, error.into())),
            }
            opened = rustix::fs::openat(&dir, part, dir_flags(), Mode::empty());
        }
        dir = opened.map_err(|error| open_error(&dir, part, error, relative))?;
    }
    Ok((dir, last))
}

fn dir_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

/// A symlink makes `openat(O_NOFOLLOW)` fail (`ELOOP`, or `ENOTDIR` with
/// `O_DIRECTORY`); report that as `Denied`, anything else as IO. A directory
/// seen after `ENOTDIR` means a symlink was there a moment ago.
fn open_error(dir: &OwnedFd, name: &OsStr, error: Errno, relative: &str) -> ToolError {
    let kind = rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)
        .ok()
        .map(|stat| FileType::from_raw_mode(stat.st_mode));
    if error == Errno::LOOP
        || kind == Some(FileType::Symlink)
        || (error == Errno::NOTDIR && kind == Some(FileType::Directory))
    {
        denied(relative)
    } else {
        io(relative, error.into())
    }
}

/// Open `name` in `dir` (or `dir` itself for `None`) without following a
/// symlink, and report what it is. `O_NONBLOCK` keeps a FIFO from hanging.
fn open_any(
    dir: &OwnedFd,
    name: Option<&OsStr>,
    relative: &str,
) -> Result<(OwnedFd, FileType), ToolError> {
    let fd = match name {
        None => dir.try_clone().map_err(|error| io(relative, error))?,
        Some(name) => rustix::fs::openat(
            dir,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|error| open_error(dir, name, error, relative))?,
    };
    let stat = rustix::fs::fstat(&fd).map_err(|error| io(relative, error.into()))?;
    Ok((fd, FileType::from_raw_mode(stat.st_mode)))
}

fn open_file(dir: &OwnedFd, name: Option<&OsStr>, relative: &str) -> Result<File, ToolError> {
    match open_any(dir, name, relative)? {
        (fd, FileType::RegularFile) => Ok(File::from(fd)),
        _ => Err(invalid(format!("{relative} is not a regular file"))),
    }
}

fn read_text(mut file: File, relative: &str) -> Result<String, ToolError> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| io(relative, error))?;
    String::from_utf8(bytes).map_err(|_| invalid(format!("{relative} is not valid UTF-8")))
}

/// The first [`MAX_BYTES`] of a UTF-8 file, with a marker when it is longer.
fn read_capped(file: File, relative: &str) -> Result<String, ToolError> {
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

/// Write `contents` to a temp file in `dir`, then rename it over `name`, so a
/// crash leaves either the old or the new file. Keeps an existing mode; a
/// symlink at `name` is denied rather than replaced.
fn replace_file(
    dir: &OwnedFd,
    name: &OsStr,
    contents: &[u8],
    relative: &str,
) -> Result<(), ToolError> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mode = match rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) if FileType::from_raw_mode(stat.st_mode) == FileType::Symlink => {
            return Err(denied(relative));
        }
        Ok(stat) => Some(Mode::from_raw_mode(stat.st_mode & 0o7777)),
        Err(Errno::NOENT) => None,
        Err(error) => return Err(io(relative, error.into())),
    };
    let mut temp = OsString::from(".");
    temp.push(name);
    temp.push(format!(
        ".{}.{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut created = false;
    let result = (|| -> std::io::Result<()> {
        let fd = rustix::fs::openat(
            dir,
            &temp,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o666),
        )?;
        created = true;
        if let Some(mode) = mode {
            rustix::fs::fchmod(&fd, mode)?;
        }
        let mut file = File::from(fd);
        file.write_all(contents)?;
        file.sync_all()?;
        rustix::fs::renameat(dir, &temp, dir, name)?;
        Ok(())
    })();
    if result.is_err() && created {
        let _ = rustix::fs::unlinkat(dir, &temp, AtFlags::empty());
    }
    result.map_err(|error| io(relative, error))
}

/// The entry names of `dir`, not following symlinks.
fn entries(dir: &OwnedFd) -> std::io::Result<Vec<(OsString, FileType)>> {
    use std::os::unix::ffi::OsStrExt;
    let mut out = Vec::new();
    for entry in Dir::read_from(dir)? {
        let entry = entry?;
        let name = OsStr::from_bytes(entry.file_name().to_bytes());
        if name == "." || name == ".." {
            continue;
        }
        let mut kind = entry.file_type();
        if kind == FileType::Unknown {
            kind = rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)
                .map_or(FileType::Unknown, |stat| {
                    FileType::from_raw_mode(stat.st_mode)
                });
        }
        out.push((name.to_owned(), kind));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn list(dir: &OwnedFd, relative: &str) -> Result<String, ToolError> {
    let names: Vec<String> = entries(dir)
        .map_err(|error| io(relative, error))?
        .into_iter()
        .map(|(name, kind)| {
            let mut name = name.to_string_lossy().into_owned();
            if kind == FileType::Directory {
                name.push('/');
            }
            name
        })
        .collect();
    Ok(names.join("\n"))
}

/// Depth-first literal search through descriptors. Symlinks are not followed,
/// so the walk never leaves the root; `.git`, special files and files that are
/// not UTF-8 are skipped. Stops once `out` reaches [`MAX_BYTES`].
fn search(
    fd: OwnedFd,
    kind: FileType,
    display: &Path,
    pattern: &str,
    ctx: &ToolContext,
    out: &mut String,
) -> Result<(), ToolError> {
    if ctx.cancel.is_cancelled() {
        return Err(ToolError::Cancelled {
            message: "grep cancelled".to_owned(),
        });
    }
    if out.len() >= MAX_BYTES {
        return Ok(());
    }
    match kind {
        FileType::Directory => {
            let shown = display.display().to_string();
            for (name, kind) in entries(&fd).map_err(|error| io(&shown, error))? {
                if name == ".git" || !matches!(kind, FileType::Directory | FileType::RegularFile) {
                    continue;
                }
                // Swapped or vanished since listing: skip, as an unreadable file is.
                if let Ok((child, kind)) = open_any(&fd, Some(&name), &shown) {
                    search(child, kind, &display.join(&name), pattern, ctx, out)?;
                }
            }
        }
        FileType::RegularFile => {
            let Ok(text) = read_text(File::from(fd), "") else {
                return Ok(());
            };
            let shown = display.display();
            for (number, line) in text.lines().enumerate() {
                if out.len() >= MAX_BYTES {
                    break;
                }
                if line.contains(pattern) {
                    out.push_str(&format!("{shown}:{}: {line}\n", number + 1));
                }
            }
        }
        _ => {}
    }
    Ok(())
}
