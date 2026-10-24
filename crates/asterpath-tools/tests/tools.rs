//! Root-bound tools through the public `Tool` interface.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use asterpath_protocol::{
    CancelToken, JsonString, Part, ReplaySafety, SessionId, SideEffect, StepId, Tool, ToolCallId,
    ToolContext, ToolError, TurnId,
};
use asterpath_tools::{coding, read_only};

fn ctx(cancel: CancelToken) -> ToolContext {
    ToolContext::new(
        SessionId::from_bytes([1; 16]),
        TurnId::from_bytes([2; 16]),
        StepId::from_bytes([3; 16]),
        ToolCallId::from_bytes([4; 16]),
        cancel,
    )
}

fn root() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");
    std::fs::create_dir(&root).expect("root");
    (dir, root)
}

fn tool(tools: &[Arc<dyn Tool>], name: &str) -> Arc<dyn Tool> {
    tools
        .iter()
        .find(|tool| tool.descriptor().name.as_str() == name)
        .cloned()
        .unwrap_or_else(|| panic!("no tool {name}"))
}

async fn call(tool: &Arc<dyn Tool>, args: serde_json::Value) -> Result<String, ToolError> {
    call_with(tool, args, CancelToken::new()).await
}

async fn call_with(
    tool: &Arc<dyn Tool>,
    args: serde_json::Value,
    cancel: CancelToken,
) -> Result<String, ToolError> {
    let output = tool
        .invoke(JsonString::new(args.to_string()), ctx(cancel))
        .await?;
    Ok(output
        .content
        .iter()
        .map(|part| match part {
            Part::Text { text } => text.as_str(),
            other => panic!("unexpected part {other:?}"),
        })
        .collect())
}

fn full_access(root: &Path) -> Vec<Arc<dyn Tool>> {
    coding(
        root.to_path_buf(),
        None,
        asterpath_protocol::SandboxPolicy::FullAccess,
    )
    .expect("absolute root")
}

#[test]
fn a_relative_root_fails_construction() {
    assert!(read_only(PathBuf::from("rel")).is_err());
    assert!(
        coding(
            PathBuf::from("rel"),
            None,
            asterpath_protocol::SandboxPolicy::FullAccess
        )
        .is_err()
    );
}

#[test]
fn presets_carry_the_documented_names_and_effects() {
    let (_dir, root) = root();
    let names = |tools: &[Arc<dyn Tool>]| {
        tools
            .iter()
            .map(|tool| tool.descriptor().name.as_str().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(&read_only(root.clone()).unwrap()),
        ["read_file", "list_dir", "grep"]
    );
    let tools = full_access(&root);
    assert_eq!(
        names(&tools),
        [
            "read_file",
            "list_dir",
            "grep",
            "write_file",
            "edit_file",
            "bash"
        ]
    );
    for tool in &tools {
        let descriptor = tool.descriptor();
        let read_only = ["read_file", "list_dir", "grep"].contains(&descriptor.name.as_str());
        assert_eq!(
            (descriptor.replay, descriptor.side_effect),
            if read_only {
                (ReplaySafety::Safe, SideEffect::ReadOnly)
            } else {
                (ReplaySafety::Unsafe, SideEffect::Mutating)
            },
            "{}",
            descriptor.name.as_str()
        );
    }
}

#[tokio::test]
async fn paths_outside_the_root_are_denied() {
    let (dir, root) = root();
    let outside = dir.path().join("secret.txt");
    std::fs::write(&outside, "secret").unwrap();
    std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
    std::os::unix::fs::symlink(dir.path(), root.join("up")).unwrap();
    std::os::unix::fs::symlink(dir.path().join("missing"), root.join("dangling")).unwrap();
    let tools = full_access(&root);
    let read = tool(&tools, "read_file");
    let write = tool(&tools, "write_file");

    for path in ["../secret.txt", "/etc/passwd", "link", "up/secret.txt"] {
        let error = call(&read, serde_json::json!({ "path": path }))
            .await
            .expect_err(path);
        assert!(
            matches!(error, ToolError::Denied { .. }),
            "{path}: {error:?}"
        );
    }
    for path in ["../x", "up/x", "dangling", "link"] {
        let error = call(&write, serde_json::json!({ "path": path, "content": "x" }))
            .await
            .expect_err(path);
        assert!(
            matches!(error, ToolError::Denied { .. }),
            "{path}: {error:?}"
        );
    }
    assert_eq!(std::fs::read_to_string(&outside).unwrap(), "secret");
    assert!(!dir.path().join("x").exists());
    assert!(!dir.path().join("missing").exists());
}

#[tokio::test]
async fn read_list_grep_and_write_stay_under_the_root() {
    let (_dir, root) = root();
    let tools = full_access(&root);
    call(
        &tool(&tools, "write_file"),
        serde_json::json!({ "path": "src/a.txt", "content": "alpha\nbeta needle\n" }),
    )
    .await
    .unwrap();
    std::fs::write(root.join("b.txt"), "needle\n").unwrap();

    assert_eq!(
        call(
            &tool(&tools, "read_file"),
            serde_json::json!({ "path": "src/a.txt" })
        )
        .await
        .unwrap(),
        "alpha\nbeta needle\n"
    );
    assert_eq!(
        call(&tool(&tools, "list_dir"), serde_json::json!({}))
            .await
            .unwrap(),
        "b.txt\nsrc/"
    );
    assert_eq!(
        call(
            &tool(&tools, "grep"),
            serde_json::json!({ "pattern": "needle" })
        )
        .await
        .unwrap(),
        "b.txt:1: needle\nsrc/a.txt:2: beta needle\n"
    );
    let missing = call(
        &tool(&tools, "read_file"),
        serde_json::json!({ "path": "nope" }),
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, ToolError::NotFound { .. }), "{missing:?}");
}

#[tokio::test]
async fn edit_file_requires_exactly_one_occurrence_and_leaves_the_file_alone_otherwise() {
    let (_dir, root) = root();
    let tools = full_access(&root);
    let edit = tool(&tools, "edit_file");
    std::fs::write(root.join("f.txt"), "one two two").unwrap();

    for old in ["three", "two"] {
        let error = call(
            &edit,
            serde_json::json!({ "path": "f.txt", "old_string": old, "new_string": "x" }),
        )
        .await
        .expect_err(old);
        assert!(
            matches!(error, ToolError::InvalidArgs { .. }),
            "{old}: {error:?}"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("f.txt")).unwrap(),
            "one two two"
        );
    }

    call(
        &edit,
        serde_json::json!({ "path": "f.txt", "old_string": "one", "new_string": "1" }),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("f.txt")).unwrap(),
        "1 two two"
    );
}

#[tokio::test]
async fn bash_spawns_through_the_sandbox_provider_in_the_root() {
    let (_dir, root) = root();
    let sandbox = Arc::new(asterpath_testing::FakeSandboxProvider::full());
    let policy = asterpath_protocol::SandboxPolicy::WorkspaceWrite {
        writable_roots: vec![root.clone()],
    };
    let bash = asterpath_tools::bash(root.clone(), Some(sandbox.clone()), policy.clone()).unwrap();

    let output = call(
        &bash,
        serde_json::json!({ "command": "pwd; echo hi > out.txt" }),
    )
    .await
    .unwrap();

    let real_root = root.canonicalize().unwrap();
    assert!(
        output.starts_with(&format!("{}\n", real_root.display())),
        "{output}"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("out.txt")).unwrap(),
        "hi\n"
    );
    let recorded = sandbox.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].cwd.as_deref(), Some(real_root.as_path()));
    assert_eq!(recorded[0].policy, policy);
}

#[tokio::test]
async fn bash_without_a_provider_fails_closed_unless_full_access() {
    let (_dir, root) = root();
    let bash = asterpath_tools::bash(
        root.clone(),
        None,
        asterpath_protocol::SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![root.clone()],
        },
    )
    .unwrap();

    let error = call(&bash, serde_json::json!({ "command": "touch ran" }))
        .await
        .unwrap_err();
    assert!(matches!(error, ToolError::Denied { .. }), "{error:?}");
    assert!(!root.join("ran").exists(), "no process was started");

    let bash = asterpath_tools::bash(
        root.clone(),
        None,
        asterpath_protocol::SandboxPolicy::FullAccess,
    )
    .unwrap();
    call(&bash, serde_json::json!({ "command": "touch ran" }))
        .await
        .unwrap();
    assert!(root.join("ran").exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn cancelling_bash_kills_the_process() {
    let (_dir, root) = root();
    let bash = tool(&full_access(&root), "bash");
    let cancel = CancelToken::new();
    let running = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            call_with(
                &bash,
                serde_json::json!({ "command": "echo $$ > pid; exec sleep 30" }),
                cancel,
            )
            .await
        }
    });
    let pid_file = root.join("pid");
    let pid = loop {
        if let Ok(pid) = std::fs::read_to_string(&pid_file)
            && !pid.trim().is_empty()
        {
            break pid.trim().to_owned();
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };

    cancel.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), running)
        .await
        .expect("returns promptly")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, ToolError::Cancelled { .. }), "{error:?}");

    // Killed: the process is gone or a zombie awaiting the reaper.
    let status = format!("/proc/{pid}/stat");
    for _ in 0..100 {
        match std::fs::read_to_string(&status) {
            Err(_) => return,
            Ok(stat) if stat.contains(") Z ") => return,
            Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    }
    panic!("process {pid} still running after cancel");
}
