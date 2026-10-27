//! The sandbox backend probed at startup (ADR-0055, #196).

use std::path::Path;
use std::sync::Arc;

use asterpath::protocol::{
    CancelToken, JsonString, SandboxPolicy, SessionId, SpawnEnv, StepId, Tool, ToolCallId,
    ToolContext, ToolError, TurnId,
};
use asterpath_app::workspace::{SandboxBackend, SandboxMode, Workspace, workspace_tools};

fn bash(workspace: &Workspace, root: &Path) -> Arc<dyn Tool> {
    workspace_tools(workspace, root, Some(&["bash".to_owned()]))
        .unwrap()
        .remove(0)
}

async fn run(bash: &Arc<dyn Tool>, command: &str, spawn: SpawnEnv) -> Result<(), ToolError> {
    let ctx = ToolContext::new(
        SessionId::from_bytes([1; 16]),
        TurnId::from_bytes([2; 16]),
        StepId::from_bytes([3; 16]),
        ToolCallId::from_bytes([4; 16]),
        CancelToken::new(),
    )
    .with_spawn(spawn);
    let args = JsonString::new(serde_json::json!({ "command": command }).to_string());
    bash.invoke(args, ctx).await.map(|_| ())
}

#[tokio::test]
async fn without_a_backend_the_effective_policy_is_full_access() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let backend = SandboxBackend::probe_bwrap("/nonexistent/bwrap");
    assert!(backend.unavailable().is_some());
    let workspace = Workspace {
        sandbox: SandboxMode::WorkspaceWrite,
        sandbox_backend: backend,
        ..Workspace::default()
    };

    assert_eq!(workspace.sandbox_policy(&root), SandboxPolicy::FullAccess);
    run(&bash(&workspace, &root), "touch ran", SpawnEnv::default())
        .await
        .unwrap();
    assert!(root.join("ran").exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_usable_backend_confines_calls_even_when_the_mode_is_full_access() {
    let backend = SandboxBackend::probe();
    if let Some(reason) = backend.unavailable() {
        eprintln!("skipping: {reason}");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let workspace = Workspace {
        sandbox: SandboxMode::FullAccess,
        sandbox_backend: backend,
        ..Workspace::default()
    };

    let mut spawn = SpawnEnv::default();
    spawn.sandbox = Some(SandboxPolicy::ReadOnly);
    let bash = bash(&workspace, &root);
    run(&bash, "true", spawn.clone()).await.unwrap();
    let result = run(&bash, "touch ran", spawn).await;
    assert!(!root.join("ran").exists(), "{result:?}");
}
