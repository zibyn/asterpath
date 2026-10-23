//! Public-seam tests for the Linux bwrap backend.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;

use asterpath_sandbox::{
    ConfinedSpawn, Enforcement, SandboxError, SandboxPolicy, SandboxProvider, SpawnRequest,
};
use asterpath_sandbox_linux::LinuxSandbox;

fn env() -> BTreeMap<OsString, OsString> {
    BTreeMap::from([
        (OsString::from("LANG"), OsString::from("C")),
        (OsString::from("PATH"), OsString::from("/usr/bin:/bin")),
    ])
}

fn args(parts: &[&str]) -> Vec<OsString> {
    parts.iter().map(OsString::from).collect()
}

/// Any existing executable stands in for bwrap, so transform tests do not
/// depend on bwrap being installed.
fn stub_provider() -> LinuxSandbox {
    LinuxSandbox::with_bwrap_program(std::env::current_exe().unwrap())
}

fn run(spawn: &ConfinedSpawn) -> std::io::Result<std::process::ExitStatus> {
    let mut command = Command::new(&spawn.program);
    command.args(&spawn.args).env_clear().envs(&spawn.env);
    if let Some(cwd) = &spawn.cwd {
        command.current_dir(cwd);
    }
    command.status()
}

#[test]
fn read_only_wraps_bwrap_read_only() {
    let provider = stub_provider();
    let env = env();
    let policy = SandboxPolicy::ReadOnly;
    let cwd = Path::new("/work/space");
    let request_args = args(&["--version"]);
    let request = SpawnRequest::new(
        OsStr::new("/bin/echo"),
        &request_args,
        Some(cwd),
        &env,
        &policy,
    );

    let spawn = provider.confine(&request).expect("read-only confine");

    assert_eq!(spawn.program, std::env::current_exe().unwrap());
    assert_eq!(
        spawn.args,
        args(&[
            "--ro-bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--proc",
            "/proc",
            "--die-with-parent",
            "--chdir",
            "/work/space",
            "/bin/echo",
            "--version",
        ])
    );
    assert_eq!(spawn.cwd.as_deref(), Some(cwd));
    assert_eq!(spawn.env, env);
    assert_eq!(spawn.enforcement, Enforcement::Full);
    for absent in ["--unshare-net", "--unshare-pid"] {
        assert!(
            !spawn.args.iter().any(|arg| arg == absent),
            "{absent} must not be added"
        );
    }
}

#[test]
fn workspace_write_binds_each_root_in_order() {
    let provider = stub_provider();
    let env = env();
    let policy = SandboxPolicy::WorkspaceWrite {
        writable_roots: vec![PathBuf::from("/work"), PathBuf::from("/var/tmp/scratch")],
    };
    let request_args = args(&["--flag", "value"]);
    let request = SpawnRequest::new(
        OsStr::new("/usr/bin/mcp-server"),
        &request_args,
        None,
        &env,
        &policy,
    );

    let spawn = provider.confine(&request).expect("workspace-write confine");

    assert_eq!(
        spawn.args,
        args(&[
            "--ro-bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--proc",
            "/proc",
            "--die-with-parent",
            "--bind",
            "/work",
            "/work",
            "--bind",
            "/var/tmp/scratch",
            "/var/tmp/scratch",
            "/usr/bin/mcp-server",
            "--flag",
            "value",
        ])
    );
    assert_eq!(spawn.cwd, None);
    assert_eq!(spawn.env, env);
    assert_eq!(spawn.enforcement, Enforcement::Full);
}

#[test]
fn workspace_write_rejects_empty_and_relative_roots() {
    // Validation happens before bwrap resolution, so a missing binary still
    // reports the policy error.
    let provider = LinuxSandbox::with_bwrap_program("/nonexistent/asterpath-bwrap");
    let env = env();
    for policy in [
        SandboxPolicy::WorkspaceWrite {
            writable_roots: Vec::new(),
        },
        SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![PathBuf::from("relative/dir")],
        },
    ] {
        let request = SpawnRequest::new(OsStr::new("/bin/true"), &[], None, &env, &policy);
        match provider.confine(&request) {
            Err(SandboxError::Unsupported { reason }) => {
                assert!(reason.contains("writable root"), "{reason}")
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}

#[test]
fn full_access_is_pass_through_without_bwrap() {
    let provider = LinuxSandbox::with_bwrap_program("/nonexistent/asterpath-bwrap");
    let env = env();
    let policy = SandboxPolicy::FullAccess;
    let cwd = Path::new("/work/space");
    let request_args = args(&["--flag", "value"]);
    let request = SpawnRequest::new(
        OsStr::new("/bin/echo"),
        &request_args,
        Some(cwd),
        &env,
        &policy,
    );

    let spawn = provider.confine(&request).expect("full-access confine");

    assert_eq!(spawn.program, OsString::from("/bin/echo"));
    assert_eq!(spawn.args, request_args);
    assert_eq!(spawn.cwd.as_deref(), Some(cwd));
    assert_eq!(spawn.env, env);
    assert_eq!(spawn.enforcement, Enforcement::Full);
}

#[test]
fn missing_bwrap_is_setup_and_never_runs_bare() {
    let provider = LinuxSandbox::with_bwrap_program("/nonexistent/asterpath-bwrap");
    let env = env();
    let policy = SandboxPolicy::ReadOnly;
    let request = SpawnRequest::new(OsStr::new("/bin/true"), &[], None, &env, &policy);

    match provider.confine(&request) {
        Err(SandboxError::Setup { message }) => assert!(message.contains("bwrap")),
        other => panic!("expected Setup, got {other:?}"),
    }
}

fn bwrap_on_path() -> bool {
    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path)
            .map(|dir| dir.join("bwrap"))
            .any(|candidate| candidate.is_file())
    })
}

/// Namespace setup needs privileges the host may not grant (containers, CI).
/// Only run the real-bwrap assertions when bwrap can actually create one.
fn bwrap_can_sandbox() -> bool {
    Command::new("bwrap")
        .args([
            "--ro-bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--proc",
            "/proc",
            "--die-with-parent",
            "/bin/true",
        ])
        .status()
        .is_ok_and(|status| status.success())
}

#[test]
#[cfg(target_os = "linux")]
fn read_only_runs_confined_true() {
    if !bwrap_on_path() || !bwrap_can_sandbox() {
        return;
    }
    let env = env();
    let policy = SandboxPolicy::ReadOnly;
    let request = SpawnRequest::new(OsStr::new("/bin/true"), &[], None, &env, &policy);

    let spawn = LinuxSandbox::new().confine(&request).expect("confine");
    let status = run(&spawn).expect("spawn bwrap");

    assert!(status.success(), "confined /bin/true failed: {status:?}");
}

#[test]
#[cfg(target_os = "linux")]
fn workspace_write_allows_root_and_denies_outside() {
    if !bwrap_on_path() || !bwrap_can_sandbox() {
        return;
    }
    let base = std::env::temp_dir().join(format!("asterpath-sandbox-{}", std::process::id()));
    let root = base.join("root");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&root).expect("create temp root");
    let inside = root.join("inside.txt");
    let outside = base.join("outside.txt");

    let env = env();
    let policy = SandboxPolicy::WorkspaceWrite {
        writable_roots: vec![root.clone()],
    };
    let script = "printf ok > \"$1\"";

    for (target, expected_success) in [(&inside, true), (&outside, false)] {
        let sh_args = vec![
            OsString::from("-c"),
            OsString::from(script),
            OsString::from("sh"),
            target.as_os_str().to_os_string(),
        ];
        let request = SpawnRequest::new(OsStr::new("/bin/sh"), &sh_args, None, &env, &policy);
        let spawn = LinuxSandbox::new().confine(&request).expect("confine");
        let status = run(&spawn).expect("spawn bwrap");

        assert_eq!(
            status.success(),
            expected_success,
            "writing {target:?} should {} (status {status:?})",
            if expected_success { "succeed" } else { "fail" }
        );
    }

    assert_eq!(std::fs::read_to_string(&inside).unwrap(), "ok");
    assert!(!outside.exists(), "outside write must not create a file");

    let _ = std::fs::remove_dir_all(&base);
}
