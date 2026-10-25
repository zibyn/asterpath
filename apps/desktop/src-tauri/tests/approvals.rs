//! Approval cards and the composer's permission mode (#152).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use asterpath::Asterpath;
use asterpath_desktop::{Answer, Desktop, PermissionMode, Permissions};
use asterpath_host::{WorkRoot, Workspace};
use asterpath_protocol::{
    ApprovalRequest, Item, JsonString, ModelRequest, Part, Provider, Role, SessionId, StopReason,
    ToolName, Usage,
};
use asterpath_testing::ScriptedProvider;
use common::template;

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    desktop: Desktop,
    provider: Arc<ScriptedProvider>,
}

/// A desktop whose sessions have `read_file` and `write_file`, the scripted
/// replies on `p`, and `default` as every session's starting mode.
async fn fixture(provider: ScriptedProvider, default: PermissionMode) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let workspace = Workspace {
        tools: Some(vec!["read_file".to_owned(), "write_file".to_owned()]),
        skills_enabled: false,
        ..Workspace::default()
    };
    let provider = Arc::new(provider);
    let permissions = Arc::new(Permissions::new(default));
    let asterpath = Asterpath::builder()
        .database(root.join("asterpath.db"))
        .providers(vec![provider.clone() as Arc<dyn Provider>])
        .environment(Arc::new(WorkRoot {
            workspace: workspace.clone(),
            new: false,
            pinned: None,
        }))
        .approval(permissions.clone())
        .build()
        .await
        .expect("build");
    let desktop = Desktop::new(
        asterpath,
        permissions,
        workspace,
        template(),
        root.join("projects.json"),
    );
    Fixture {
        _dir: dir,
        root,
        desktop,
        provider,
    }
}

fn write(call: &str, path: &str) -> Vec<Item> {
    vec![Item::ToolCall {
        call_id: call.to_owned(),
        provider_item_id: None,
        name: ToolName::new("write_file"),
        raw_args: JsonString::new(serde_json::json!({"path": path, "content": "hi"}).to_string()),
        raw: None,
    }]
}

fn reply(text: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Text {
            text: text.to_owned(),
        }],
        raw: None,
    }]
}

fn tool_use(provider: ScriptedProvider, items: Vec<Item>) -> ScriptedProvider {
    provider.respond(items, StopReason::ToolUse, Usage::default())
}

fn end(provider: ScriptedProvider) -> ScriptedProvider {
    provider.respond(reply("done"), StopReason::EndTurn, Usage::default())
}

/// The session's waiting approvals, once there is one.
async fn asked(desktop: &Desktop, session: SessionId) -> Vec<ApprovalRequest> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pending = desktop.approvals(session);
            if !pending.is_empty() {
                return pending;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("an approval request")
}

/// Wait until the provider got `n` requests and the driver stopped.
async fn settled(fixture: &Fixture, session: SessionId, n: usize) -> Vec<ModelRequest> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while fixture.provider.requests().len() < n
            || fixture.desktop.access(session).await.unwrap() != asterpath_desktop::Access::Free
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("settled");
    fixture.provider.requests()
}

fn tool_results(request: &ModelRequest) -> Vec<(String, bool)> {
    request
        .items
        .iter()
        .filter_map(|item| match item {
            Item::ToolResult {
                parts, is_error, ..
            } => Some((
                parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect(),
                *is_error,
            )),
            _ => None,
        })
        .collect()
}

async fn session(fixture: &Fixture) -> SessionId {
    fixture
        .desktop
        .create_session(&fixture.root, None)
        .await
        .unwrap()
        .id
}

#[tokio::test]
async fn a_file_write_waits_for_the_card_and_runs_once_allowed() {
    let fixture = fixture(
        end(tool_use(ScriptedProvider::new("p"), write("c1", "a.txt"))),
        PermissionMode::WorkspaceWrite,
    )
    .await;
    let id = session(&fixture).await;

    fixture.desktop.send(id, "write".to_owned()).await.unwrap();
    let pending = asked(&fixture.desktop, id).await;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].tool.as_str(), "write_file");
    fixture
        .desktop
        .approve(
            pending[0].session,
            pending[0].tool_call_id,
            Answer::AllowOnce,
        )
        .await
        .unwrap();

    let requests = settled(&fixture, id, 2).await;
    assert_eq!(tool_results(&requests[1]).len(), 1);
    assert!(!tool_results(&requests[1])[0].1, "{:?}", requests[1].items);
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("a.txt")).unwrap(),
        "hi"
    );
    assert!(fixture.desktop.approvals(id).is_empty());
}

#[tokio::test]
async fn a_denial_s_reason_reaches_the_model() {
    let fixture = fixture(
        end(tool_use(ScriptedProvider::new("p"), write("c1", "a.txt"))),
        PermissionMode::WorkspaceWrite,
    )
    .await;
    let id = session(&fixture).await;

    fixture.desktop.send(id, "write".to_owned()).await.unwrap();
    let pending = asked(&fixture.desktop, id).await;
    fixture
        .desktop
        .approve(
            pending[0].session,
            pending[0].tool_call_id,
            Answer::Deny {
                reason: Some("use b.txt instead".to_owned()),
            },
        )
        .await
        .unwrap();

    let requests = settled(&fixture, id, 2).await;
    let results = tool_results(&requests[1]);
    assert!(results[0].1, "{results:?}");
    assert!(results[0].0.contains("use b.txt instead"), "{results:?}");
    assert!(!fixture.root.join("a.txt").exists());
}

#[tokio::test]
async fn allowing_a_tool_for_the_session_stops_asking_for_it() {
    let provider = ScriptedProvider::new("p");
    let provider = tool_use(provider, write("c1", "a.txt"));
    let provider = end(tool_use(provider, write("c2", "b.txt")));
    let fixture = fixture(provider, PermissionMode::WorkspaceWrite).await;
    let id = session(&fixture).await;

    fixture.desktop.send(id, "write".to_owned()).await.unwrap();
    let pending = asked(&fixture.desktop, id).await;
    fixture
        .desktop
        .approve(
            pending[0].session,
            pending[0].tool_call_id,
            Answer::AllowForSession,
        )
        .await
        .unwrap();

    settled(&fixture, id, 3).await;
    assert!(fixture.root.join("b.txt").exists(), "the second write ran");

    // Another session still asks.
    let other = session(&fixture).await;
    assert_eq!(
        fixture.desktop.permission_mode(other),
        PermissionMode::WorkspaceWrite
    );
}

#[tokio::test]
async fn read_only_denies_writes_and_full_trust_runs_them_without_asking() {
    let provider = ScriptedProvider::new("p");
    let provider = end(tool_use(provider, write("c1", "a.txt")));
    let provider = end(tool_use(provider, write("c2", "b.txt")));
    let fixture = fixture(provider, PermissionMode::WorkspaceWrite).await;
    let id = session(&fixture).await;

    fixture
        .desktop
        .set_permission_mode(id, PermissionMode::ReadOnly);
    assert_eq!(
        fixture.desktop.permission_mode(id),
        PermissionMode::ReadOnly
    );
    fixture
        .desktop
        .send(id, "write a".to_owned())
        .await
        .unwrap();
    let requests = settled(&fixture, id, 2).await;
    assert!(tool_results(&requests[1])[0].1, "denied");
    assert!(!fixture.root.join("a.txt").exists());

    fixture
        .desktop
        .set_permission_mode(id, PermissionMode::FullTrust);
    fixture
        .desktop
        .send(id, "write b".to_owned())
        .await
        .unwrap();
    settled(&fixture, id, 4).await;
    assert!(fixture.root.join("b.txt").exists());
}

/// The JSON `src/lib/commands.ts` sends and reads.
#[test]
fn modes_and_answers_have_the_frontend_s_json() {
    assert_eq!(
        serde_json::to_value([
            PermissionMode::ReadOnly,
            PermissionMode::WorkspaceWrite,
            PermissionMode::FullTrust
        ])
        .unwrap(),
        serde_json::json!(["read_only", "workspace_write", "full_trust"])
    );
    let answers: Vec<Answer> = serde_json::from_value(serde_json::json!([
        {"kind": "allow_once"},
        {"kind": "allow_for_session"},
        {"kind": "deny", "reason": "no"},
    ]))
    .unwrap();
    assert_eq!(
        answers,
        [
            Answer::AllowOnce,
            Answer::AllowForSession,
            Answer::Deny {
                reason: Some("no".to_owned())
            }
        ]
    );
}
