//! Approval cards and the composer's approval mode (#152).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use asterpath::Asterpath;
use asterpath_app::desktop::{Answer, ApprovalMode, ApprovalModes, Desktop, PendingApproval};
use asterpath_app::workspace::{SandboxBackend, SandboxMode, WorkRoot, Workspace};
use asterpath_protocol::{
    ApprovalPolicy, ApprovalRequest, ApprovalVerdict, Item, JsonString, ModelRequest, Part,
    Provenance, Provider, ReplaySafety, Role, SessionId, SideEffect, StepId, StopReason,
    ToolCallId, ToolName, TransportKind, TurnId, Usage,
};
use asterpath_testing::ScriptedProvider;
use common::template;

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    desktop: Desktop,
    provider: Arc<ScriptedProvider>,
}

/// A desktop whose sessions have `read_file`, `write_file` and `bash`, the scripted
/// replies on `p`, and `default` as every session's starting mode.
async fn fixture(provider: ScriptedProvider, default: ApprovalMode) -> Fixture {
    fixture_in(provider, default, Workspace::default()).await
}

/// [`fixture`] with `workspace`'s sandbox.
async fn fixture_in(
    provider: ScriptedProvider,
    default: ApprovalMode,
    workspace: Workspace,
) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let workspace = Workspace {
        tools: Some(vec![
            "read_file".to_owned(),
            "write_file".to_owned(),
            "bash".to_owned(),
        ]),
        skills_enabled: false,
        ..workspace
    };
    let provider = Arc::new(provider);
    let modes = Arc::new(ApprovalModes::new(default).with_sandbox(&workspace));
    let asterpath = Asterpath::builder()
        .database(root.join("asterpath.db"))
        .providers(vec![provider.clone() as Arc<dyn Provider>])
        .environment(modes.environment(Arc::new(WorkRoot {
            workspace: workspace.clone(),
            new_session: false,
            pinned: None,
            builtin_agents: None,
        })))
        .approval(modes.clone())
        .build()
        .await
        .expect("build");
    let desktop = Desktop::new(
        asterpath,
        modes,
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
async fn asked(desktop: &Desktop, session: SessionId) -> Vec<PendingApproval> {
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
            || fixture.desktop.access(session).await.unwrap()
                != asterpath_app::desktop::Access::Free
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
        .create_session(&fixture.root, None, None, None)
        .await
        .unwrap()
        .id
}

#[tokio::test]
async fn a_file_write_waits_for_the_card_and_runs_once_allowed() {
    let fixture = fixture(
        end(tool_use(ScriptedProvider::new("p"), write("c1", "a.txt"))),
        ApprovalMode::Ask,
    )
    .await;
    let id = session(&fixture).await;

    fixture.desktop.send(id, "write".to_owned()).await.unwrap();
    let pending = asked(&fixture.desktop, id).await;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].request.tool.as_str(), "write_file");
    fixture
        .desktop
        .approve(
            pending[0].request.session,
            pending[0].request.tool_call_id,
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
        ApprovalMode::Ask,
    )
    .await;
    let id = session(&fixture).await;

    fixture.desktop.send(id, "write".to_owned()).await.unwrap();
    let pending = asked(&fixture.desktop, id).await;
    fixture
        .desktop
        .approve(
            pending[0].request.session,
            pending[0].request.tool_call_id,
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
    let fixture = fixture(provider, ApprovalMode::Ask).await;
    let id = session(&fixture).await;

    fixture.desktop.send(id, "write".to_owned()).await.unwrap();
    let pending = asked(&fixture.desktop, id).await;
    fixture
        .desktop
        .approve(
            pending[0].request.session,
            pending[0].request.tool_call_id,
            Answer::AllowForSession,
        )
        .await
        .unwrap();

    settled(&fixture, id, 3).await;
    assert!(fixture.root.join("b.txt").exists(), "the second write ran");

    // Another session still asks.
    let other = session(&fixture).await;
    assert_eq!(
        fixture.desktop.approval_mode(other).await.unwrap(),
        ApprovalMode::Ask
    );
}

#[tokio::test]
async fn deny_mode_denies_writes_and_allow_mode_runs_them_without_asking() {
    let provider = ScriptedProvider::new("p");
    let provider = end(tool_use(provider, write("c1", "a.txt")));
    let provider = end(tool_use(provider, write("c2", "b.txt")));
    let fixture = fixture(provider, ApprovalMode::Ask).await;
    let id = session(&fixture).await;

    fixture
        .desktop
        .set_approval_mode(id, ApprovalMode::Deny)
        .await
        .unwrap();
    assert_eq!(
        fixture.desktop.approval_mode(id).await.unwrap(),
        ApprovalMode::Deny
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
        .set_approval_mode(id, ApprovalMode::Allow)
        .await
        .unwrap();
    fixture
        .desktop
        .send(id, "write b".to_owned())
        .await
        .unwrap();
    settled(&fixture, id, 4).await;
    assert!(fixture.root.join("b.txt").exists());
}

fn bash(call: &str, command: &str) -> Vec<Item> {
    vec![Item::ToolCall {
        call_id: call.to_owned(),
        provider_item_id: None,
        name: ToolName::new("bash"),
        raw_args: JsonString::new(serde_json::json!({ "command": command }).to_string()),
        raw: None,
    }]
}

/// Allowing a command for the session offers and allows its prefix:
/// `true run test` stops asking for `true run lint`, not for `true x`.
#[tokio::test]
async fn allowing_a_command_for_the_session_stops_asking_for_its_prefix() {
    let provider = ScriptedProvider::new("p");
    let provider = tool_use(provider, bash("c1", "true run test"));
    let provider = tool_use(provider, bash("c2", "true run lint"));
    let provider = end(tool_use(provider, bash("c3", "true x")));
    let fixture = fixture_in(
        provider,
        ApprovalMode::Ask,
        workspace(SandboxMode::WorkspaceWrite, false),
    )
    .await;
    let id = session(&fixture).await;

    fixture.desktop.send(id, "run".to_owned()).await.unwrap();
    let pending = asked(&fixture.desktop, id).await;
    assert_eq!(pending[0].session_rules, Some(vec!["true run".to_owned()]));
    fixture
        .desktop
        .approve(
            pending[0].request.session,
            pending[0].request.tool_call_id,
            Answer::AllowForSession,
        )
        .await
        .unwrap();

    // The next card is the third call's: the second ran without asking.
    let next = loop {
        let pending = asked(&fixture.desktop, id).await;
        if pending[0].request.args.as_str() != bash_args("true run test") {
            break pending;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(next[0].request.args.as_str(), bash_args("true x"));
}

fn bash_args(command: &str) -> String {
    serde_json::json!({ "command": command }).to_string()
}

/// A command with a substitution gives no prefix, so the card offers no
/// "allow for session" and the answer is refused.
#[tokio::test]
async fn a_command_without_a_prefix_cannot_be_allowed_for_the_session() {
    let provider = end(tool_use(
        ScriptedProvider::new("p"),
        bash("c1", "echo $(true)"),
    ));
    let fixture = fixture_in(
        provider,
        ApprovalMode::Ask,
        workspace(SandboxMode::WorkspaceWrite, false),
    )
    .await;
    let id = session(&fixture).await;

    fixture.desktop.send(id, "run".to_owned()).await.unwrap();
    let pending = asked(&fixture.desktop, id).await;
    assert_eq!(pending[0].session_rules, None);
    assert!(
        fixture
            .desktop
            .approve(
                pending[0].request.session,
                pending[0].request.tool_call_id,
                Answer::AllowForSession,
            )
            .await
            .is_err()
    );
}

/// Read-only mode runs bash in a `ReadOnly` sandbox instead of denying it
/// (ADR-0055): reads work, writes fail inside the command.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_only_mode_runs_bash_in_a_read_only_sandbox() {
    if let Some(reason) = asterpath_app::workspace::SandboxBackend::probe().unavailable() {
        eprintln!("skipping: {reason}");
        return;
    }
    let provider = end(tool_use(
        ScriptedProvider::new("p"),
        bash("c1", "ls >/dev/null && echo listed; echo hi > c.txt"),
    ));
    let fixture = fixture(provider, ApprovalMode::Ask).await;
    let id = session(&fixture).await;
    // Switched after the session's environment was resolved: takes effect at once.
    fixture
        .desktop
        .set_approval_mode(id, ApprovalMode::Deny)
        .await
        .unwrap();

    fixture.desktop.send(id, "ls".to_owned()).await.unwrap();
    let requests = settled(&fixture, id, 2).await;
    let results = tool_results(&requests[1]);
    assert!(results[0].0.contains("listed"), "{results:?}");
    assert!(!fixture.root.join("c.txt").exists());
}

fn request(session: SessionId, root: SessionId, tool: &str) -> ApprovalRequest {
    ApprovalRequest::new(
        session,
        TurnId::from_bytes([3; 16]),
        StepId::from_bytes([4; 16]),
        ToolCallId::from_bytes([5; 16]),
        ToolName::new(tool),
        JsonString::new("{}"),
        ReplaySafety::Unsafe,
        SideEffect::Mutating,
        Provenance::InProcess,
    )
    .with_root(root)
}

/// One row of ADR-0055's table: the verdicts of `bash`, `write_file`,
/// `read_file`, an MCP tool with `readOnlyHint` and one without.
fn row(modes: &ApprovalModes, mode: ApprovalMode) -> [&'static str; 5] {
    let root = SessionId::from_bytes([1; 16]);
    modes.set_mode(root, mode);
    let mcp = Provenance::External {
        transport: TransportKind::Stdio,
    };
    [
        ("bash", SideEffect::Mutating, Provenance::InProcess),
        ("write_file", SideEffect::Mutating, Provenance::InProcess),
        ("read_file", SideEffect::ReadOnly, Provenance::InProcess),
        ("fs__read", SideEffect::ReadOnly, mcp),
        ("fs__write", SideEffect::Mutating, mcp),
    ]
    .map(|(tool, side_effect, provenance)| {
        let mut request = request(root, root, tool);
        request.side_effect = side_effect;
        request.provenance = provenance;
        match modes.classify(&request) {
            ApprovalVerdict::Allow => "allow",
            ApprovalVerdict::Ask => "ask",
            ApprovalVerdict::Deny { .. } => "deny",
            other => panic!("{other:?}"),
        }
    })
}

fn workspace(sandbox: SandboxMode, backend: bool) -> Workspace {
    Workspace {
        sandbox,
        sandbox_backend: if backend {
            SandboxBackend::Bwrap("bwrap".into())
        } else {
            SandboxBackend::Unavailable("none".to_owned())
        },
        ..Workspace::default()
    }
}

/// Inside the sandbox bash is allowed in every mode; the mode decides what
/// runs outside it, and read-only mode tightens even `full-access`.
#[test]
fn the_mode_decides_only_calls_outside_the_sandbox() {
    use ApprovalMode::{Allow, Ask, Deny};
    let sandboxed =
        ApprovalModes::new(Ask).with_sandbox(&workspace(SandboxMode::WorkspaceWrite, true));
    assert_eq!(
        row(&sandboxed, Deny),
        ["allow", "deny", "allow", "ask", "deny"]
    );
    assert_eq!(
        row(&sandboxed, Ask),
        ["allow", "ask", "allow", "ask", "ask"]
    );
    assert_eq!(row(&sandboxed, Allow), ["allow"; 5]);

    let full = ApprovalModes::new(Ask).with_sandbox(&workspace(SandboxMode::FullAccess, true));
    assert_eq!(row(&full, Deny)[0], "allow", "tightened to ReadOnly");
    assert_eq!(row(&full, Ask)[0], "ask");
    assert_eq!(row(&full, Allow)[0], "allow");

    let none = ApprovalModes::new(Ask).with_sandbox(&workspace(SandboxMode::WorkspaceWrite, false));
    assert_eq!(row(&none, Deny), ["deny", "deny", "allow", "ask", "deny"]);
    assert_eq!(row(&none, Ask), ["ask", "ask", "allow", "ask", "ask"]);
    assert_eq!(row(&none, Allow), ["allow"; 5]);
}

/// A subagent's calls follow its tree's mode and allowances, not defaults
/// of its own.
#[test]
fn a_subagent_follows_its_tree_s_mode_and_allowances() {
    let root = SessionId::from_bytes([1; 16]);
    let child = SessionId::from_bytes([2; 16]);
    let modes = ApprovalModes::new(ApprovalMode::Ask);

    assert!(modes.allow_for_session(&request(child, root, "write_file")));
    assert_eq!(
        modes.classify(&request(child, root, "write_file")),
        ApprovalVerdict::Allow
    );
    assert_eq!(
        modes.classify(&request(child, root, "bash")),
        ApprovalVerdict::Ask
    );

    modes.set_mode(root, ApprovalMode::Deny);
    assert!(matches!(
        modes.classify(&request(child, root, "write_file")),
        ApprovalVerdict::Deny { .. }
    ));

    modes.set_mode(root, ApprovalMode::Allow);
    assert_eq!(
        modes.classify(&request(child, root, "bash")),
        ApprovalVerdict::Allow
    );
    assert_eq!(
        modes.mode(child),
        ApprovalMode::Ask,
        "keyed by the root only"
    );
}

/// The JSON `src/lib/commands.ts` sends and reads.
#[test]
fn modes_and_answers_have_the_frontend_s_json() {
    assert_eq!(
        serde_json::to_value([ApprovalMode::Deny, ApprovalMode::Ask, ApprovalMode::Allow]).unwrap(),
        serde_json::json!(["deny", "ask", "allow"])
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

/// The verdict of `command` run by `bash` under `rules` in `mode`.
fn command(modes: &ApprovalModes, mode: ApprovalMode, command: &str) -> &'static str {
    let root = SessionId::from_bytes([1; 16]);
    modes.set_mode(root, mode);
    let mut request = request(root, root, "bash");
    request.args = JsonString::new(serde_json::json!({ "command": command }).to_string());
    match modes.classify(&request) {
        ApprovalVerdict::Allow => "allow",
        ApprovalVerdict::Ask => "ask",
        ApprovalVerdict::Deny { .. } => "deny",
        other => panic!("{other:?}"),
    }
}

fn with_rules(backend: bool, allow: &[&str], ask: &[&str], deny: &[&str]) -> ApprovalModes {
    let owned = |list: &[&str]| {
        list.iter()
            .map(|rule| (*rule).to_owned())
            .collect::<Vec<_>>()
    };
    let rules =
        asterpath_app::approval::rules::CommandRules::new(&owned(allow), &owned(ask), &owned(deny))
            .unwrap();
    ApprovalModes::new(ApprovalMode::Ask)
        .with_sandbox(&workspace(SandboxMode::WorkspaceWrite, backend))
        .with_rules(rules)
}

/// Deny and ask rules hold inside the sandbox too.
#[test]
fn deny_and_ask_rules_hold_inside_the_sandbox() {
    use ApprovalMode::{Allow, Ask};
    let deny = with_rules(true, &[], &[], &["bash(git push)"]);
    assert_eq!(command(&deny, Ask, "git push origin main"), "deny");
    assert_eq!(command(&deny, Ask, "ls && git push"), "deny");
    assert_eq!(command(&deny, Allow, "git push"), "deny");

    let ask = with_rules(true, &[], &["bash(git push)"], &[]);
    assert_eq!(command(&ask, Ask, "git push"), "ask");
    assert_eq!(command(&ask, Ask, "git status"), "allow");
}

/// Without a backend, builtin and allowed commands stop asking; the rest
/// fall back to the mode.
#[test]
fn allow_rules_turn_ask_into_allow_outside_the_sandbox() {
    use ApprovalMode::{Ask, Deny};
    let none = with_rules(false, &["bash(cargo test)"], &[], &[]);
    assert_eq!(command(&none, Ask, "ls -la"), "allow");
    assert_eq!(command(&none, Ask, "find . -delete"), "ask");
    assert_eq!(command(&none, Ask, "ls; rm x"), "ask");
    assert_eq!(command(&none, Ask, "echo $(rm x)"), "ask");
    assert_eq!(command(&none, Ask, "git status"), "ask");
    assert_eq!(command(&none, Ask, "cargo test -p foo"), "allow");
    // Allow rules never loosen read-only mode.
    assert_eq!(command(&none, Deny, "cargo test -p foo"), "deny");
    assert_eq!(command(&none, Deny, "ls"), "deny");
}

/// An ask rule beats a tool allowed for the session, and never loosens a deny.
#[test]
fn an_ask_rule_beats_a_session_allowance() {
    let root = SessionId::from_bytes([1; 16]);
    let modes = with_rules(true, &[], &["bash(git push)"], &[]);
    assert!(modes.allow_for_session(&bash_request(root, "git push")));
    assert_eq!(command(&modes, ApprovalMode::Ask, "git push"), "ask");

    let none = with_rules(false, &[], &["bash(git push)"], &[]);
    assert!(none.allow_for_session(&bash_request(root, "git push")));
    assert_eq!(command(&none, ApprovalMode::Deny, "git push"), "deny");
}

fn bash_request(root: SessionId, command: &str) -> ApprovalRequest {
    let mut request = request(root, root, "bash");
    request.args = JsonString::new(serde_json::json!({ "command": command }).to_string());
    request
}

/// What "allow for session" offers: each simple command's words up to its
/// first non-option argument; the tool for other tools; nothing for a
/// command it cannot read.
#[test]
fn allow_for_session_offers_a_prefix_per_simple_command() {
    let root = SessionId::from_bytes([1; 16]);
    let offer = |command: &str| ApprovalModes::session_rules(&bash_request(root, command));
    let words = |list: &[&str]| Some(list.iter().map(|word| (*word).to_owned()).collect());
    assert_eq!(offer("npm run test -- --watch"), words(&["npm run"]));
    assert_eq!(offer("cargo test -p foo"), words(&["cargo test"]));
    assert_eq!(offer("rm -rf build"), words(&["rm -rf build"]));
    assert_eq!(offer("make"), words(&["make"]));
    assert_eq!(
        offer("cargo build && npm run lint | tee; npm run x"),
        words(&["cargo build", "npm run", "tee"])
    );
    assert_eq!(offer("echo $(rm x)"), None);
    assert_eq!(offer("$CMD run"), None);
    assert_eq!(offer("if"), None);
    // No prefix rule could match these again (#198).
    assert_eq!(offer("ls > out.txt"), None);
    assert_eq!(offer("FOO=1 npm test"), None);
    assert_eq!(offer("rm *.o"), None);
    assert_eq!(offer("cargo fmt *.rs"), words(&["cargo fmt"]));
    assert_eq!(
        ApprovalModes::session_rules(&request(root, root, "write_file")),
        words(&["write_file"])
    );
}

/// A command allowed for the session stops asking for its prefix only, in
/// its tree only; an ask rule still wins.
#[test]
fn allowing_a_command_for_the_session_allows_its_prefix() {
    use ApprovalMode::Ask;
    let root = SessionId::from_bytes([1; 16]);
    let modes = with_rules(false, &["bash(cargo test)"], &["bash(npm run deploy)"], &[]);
    assert!(modes.allow_for_session(&bash_request(root, "npm run test")));
    assert_eq!(command(&modes, Ask, "npm run lint"), "allow");
    assert_eq!(command(&modes, Ask, "npm run lint && cargo test"), "allow");
    assert_eq!(command(&modes, Ask, "rm -rf build"), "ask");
    assert_eq!(command(&modes, Ask, "npm run lint; rm x"), "ask");
    assert_eq!(command(&modes, Ask, "npm run deploy"), "ask");

    let other = SessionId::from_bytes([9; 16]);
    assert_eq!(
        modes.classify(&bash_request(other, "npm run lint")),
        ApprovalVerdict::Ask
    );
    assert!(!modes.allow_for_session(&bash_request(root, "echo $(npm run x)")));
}

/// The sandbox policy `bash` gets when `modes`' environment runs call `call`
/// in the tree rooted at `root`.
async fn spawned_with(
    modes: &Arc<ApprovalModes>,
    root: SessionId,
    call: ToolCallId,
) -> Option<asterpath_protocol::SandboxPolicy> {
    use asterpath_protocol::{
        CancelToken, EnvironmentAdapter, EnvironmentError, SessionEnvironment, SpawnEnv, ToolArgs,
        ToolContext, ToolDescriptor, ToolOutput,
    };
    use futures_util::future::BoxFuture;
    use std::sync::Mutex;

    struct Bash(Arc<dyn asterpath_protocol::Tool>);
    impl EnvironmentAdapter for Bash {
        fn resolve<'a>(
            &'a self,
            _: &'a std::collections::BTreeMap<String, String>,
        ) -> BoxFuture<'a, Result<SessionEnvironment, EnvironmentError>> {
            let tool = self.0.clone();
            Box::pin(async move { Ok(SessionEnvironment::new(vec![tool], SpawnEnv::default())) })
        }
    }
    let seen = Arc::new(Mutex::new(None));
    let record = seen.clone();
    let descriptor = ToolDescriptor::new(
        ToolName::new("bash"),
        String::new(),
        serde_json::json!({"type": "object"}),
    );
    let tool = asterpath_testing::TestTool::new(descriptor, Arc::default(), move |_, ctx| {
        *record.lock().unwrap() = ctx.spawn.sandbox;
        Box::pin(async { Ok(ToolOutput::new(Vec::new())) })
    });
    let environment = modes
        .environment(Arc::new(Bash(tool)))
        .resolve(&Default::default())
        .await
        .unwrap();
    let ctx = ToolContext::new(
        root,
        TurnId::from_bytes([3; 16]),
        StepId::from_bytes([4; 16]),
        call,
        CancelToken::new(),
    );
    environment.tools[0]
        .invoke(ToolArgs::new("{}"), ctx)
        .await
        .unwrap();
    let policy = seen.lock().unwrap().clone();
    policy
}

/// A `full-access` bash call allowed only because read-only mode confines
/// it stays confined when the mode is loosened before it spawns (#193).
#[tokio::test]
async fn a_call_allowed_for_the_read_only_cap_stays_confined_after_a_switch() {
    use asterpath_protocol::SandboxPolicy;
    let root = SessionId::from_bytes([1; 16]);
    let modes = Arc::new(
        ApprovalModes::new(ApprovalMode::Ask)
            .with_sandbox(&workspace(SandboxMode::FullAccess, true)),
    );
    modes.set_mode(root, ApprovalMode::Deny);
    let approved = request(root, root, "bash");
    assert_eq!(modes.classify(&approved), ApprovalVerdict::Allow);

    modes.set_mode(root, ApprovalMode::Ask);
    assert_eq!(
        spawned_with(&modes, root, approved.tool_call_id).await,
        Some(SandboxPolicy::ReadOnly)
    );
    // Only that call: the next one runs as the mode now says.
    assert_eq!(
        spawned_with(&modes, root, approved.tool_call_id).await,
        None
    );
}
