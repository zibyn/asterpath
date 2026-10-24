//! Reference CLI skill integration: discovery feeds the new session, the
//! switch suppresses it, `config check` reports diagnostics, and `resume`
//! keeps the loader registered.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use asterpath::Asterpath;
use asterpath_protocol::{
    CompactionConfig, ModelId, ModelSpec, Part, Provider, ProviderId, RequestConfig, SessionSpec,
    SubagentConfig, Tool, ToolSelection, TurnConfig, UserInput,
};
use asterpath_protocol::{InputId, LogQuery, SessionStore};
use asterpath_skills::{LoadSkillTool, SkillRoot, SkillRootLabel};
use asterpath_testing::ScriptedProvider;
use common::{MockServer, Reply, bin};
use futures_util::StreamExt;
use tempfile::TempDir;

struct Project {
    _dir: TempDir,
    root: PathBuf,
    home: PathBuf,
}

impl Project {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("project");
        let home = dir.path().join("home");
        fs::create_dir_all(&root).expect("project");
        fs::create_dir_all(&home).expect("home");
        Self {
            _dir: dir,
            root,
            home,
        }
    }

    fn skill(&self, root: &Path, dir: &str, frontmatter: &str, body: &str) {
        let path = root.join(dir);
        fs::create_dir_all(&path).expect("skill dir");
        fs::write(
            path.join("SKILL.md"),
            format!("---\n{frontmatter}\n---\n{body}"),
        )
        .expect("skill file");
    }

    fn project_skills(&self) -> PathBuf {
        self.root.join(".asterpath/skills")
    }

    fn db(&self) -> PathBuf {
        self.root.join("sessions.db")
    }

    fn config(&self, server: &MockServer, skills: &str) -> PathBuf {
        let path = self.root.join("asterpath.toml");
        fs::write(
            &path,
            format!(
                "[storage]\npath = \"{}\"\n\n[providers.anthropic]\nbase_url = \"{}\"\n\n{skills}",
                self.db().display(),
                server.base_url()
            ),
        )
        .expect("config");
        path
    }

    fn command(&self) -> Command {
        let mut command = bin();
        command
            .current_dir(&self.root)
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join(".config"));
        command
    }
}

#[test]
fn run_appends_the_index_and_registers_load_skill() {
    let project = Project::new();
    project.skill(
        &project.project_skills(),
        "demo",
        "description: Demo skill.",
        "# Demo\n\nBody.\n",
    );
    project.skill(&project.project_skills(), "broken", "name: broken", "body");
    project.skill(
        &project.project_skills(),
        "authority",
        "description: d\nallowed-tools: [x]",
        "body",
    );
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello"))]);
    let config = project.config(&server, "[skills]\n");

    let output = project
        .command()
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--provider",
            "anthropic",
            "--model",
            "claude-test",
            "say hello",
        ])
        .output()
        .expect("run");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("DescriptionMissing"), "{stderr}");
    assert!(
        !stderr.contains("AuthorityFieldIgnored"),
        "the authority-field advisory belongs to `config check`: {stderr}"
    );
    let request = &server.requests()[0];
    assert!(request.body.contains("## Skills"), "{}", request.body);
    assert!(
        request.body.contains("- demo: Demo skill."),
        "{}",
        request.body
    );
    assert!(
        request.body.contains("\"name\":\"load_skill\""),
        "{}",
        request.body
    );
}

#[test]
fn run_no_skills_omits_the_index_and_the_tool() {
    let project = Project::new();
    project.skill(
        &project.project_skills(),
        "demo",
        "description: Demo skill.",
        "# Demo\n\nBody.\n",
    );
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello"))]);
    let config = project.config(&server, "[skills]\n");

    let output = project
        .command()
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--no-skills",
            "--provider",
            "anthropic",
            "--model",
            "claude-test",
            "say hello",
        ])
        .output()
        .expect("run");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = &server.requests()[0];
    assert!(!request.body.contains("## Skills"), "{}", request.body);
    assert!(!request.body.contains("load_skill"), "{}", request.body);
}

#[test]
fn config_check_prints_skill_diagnostics() {
    let project = Project::new();
    project.skill(&project.project_skills(), "broken", "name: broken", "body");
    project.skill(
        &project.project_skills(),
        "authority",
        "description: d\nallowed-tools: [x]",
        "body",
    );
    let server = MockServer::start(vec![Reply::sse(common::text_sse("unused"))]);
    let config = project.config(&server, "[skills]\n");

    let output = project
        .command()
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .output()
        .expect("config check");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("config ok"), "{stdout}");
    assert!(stdout.contains("DescriptionMissing"), "{stdout}");
    assert!(stdout.contains("AuthorityFieldIgnored"), "{stdout}");

    // The switch governs new sessions, not diagnostics.
    let config = project.config(&server, "[skills]\nenabled = false\n");
    let output = project
        .command()
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .output()
        .expect("config check");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");
    assert!(stdout.contains("DescriptionMissing"), "{stdout}");
}

#[tokio::test]
async fn resume_registers_load_skill_even_when_skills_are_disabled() {
    let project = Project::new();
    let skills_root = project.root.join("skills");
    project.skill(
        &skills_root,
        "demo",
        "description: Demo.",
        "# Demo\n\nBody.\n",
    );
    let db = project.db();
    let provider = Arc::new(ScriptedProvider::new("anthropic"));
    let loader = Arc::new(LoadSkillTool::new(vec![SkillRoot {
        path: skills_root.clone(),
        label: SkillRootLabel::Explicit,
    }]));
    let app = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![loader as Arc<dyn Tool>])
        .build()
        .await
        .expect("build");
    let spec = SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("anthropic"),
            model: ModelId::new("claude-test"),
            context_window: None,
        },
        system: vec![Part::Text {
            text: "## Skills\n\n- demo: Demo.".to_owned(),
        }],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: {
            let mut config = TurnConfig::default();
            config.initial_backoff = Duration::ZERO;
            config.max_backoff = Duration::ZERO;
            config
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    };
    let handle = app.create_session(spec).await.expect("create");
    let session = handle.id();
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([7; 16]),
            parts: vec![Part::Text {
                text: "go".to_owned(),
            }],
        })
        .await
        .expect("input");
    drop(handle);
    app.shutdown().await.expect("shutdown");

    let server = MockServer::start(vec![
        Reply::sse(common::tool_use_sse("load_skill", r#"{"name":"demo"}"#)),
        Reply::sse(common::text_sse("done")),
    ]);
    let config = project.config(
        &server,
        &format!(
            "[skills]\nenabled = false\npaths = [\"{}\"]\n",
            skills_root.display()
        ),
    );

    let output = project
        .command()
        .args([
            "resume",
            "--config",
            config.to_str().unwrap(),
            "--approval",
            "allow",
            &session.to_hex(),
        ])
        .output()
        .expect("resume");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "done",
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(
        requests[1].body.contains("Body."),
        "the loader ran on resume: {}",
        requests[1].body
    );
    assert!(
        !requests[1].body.contains("not found"),
        "the loader is registered: {}",
        requests[1].body
    );

    // The executed result is in the log as an ordinary completed tool result.
    let log = app_log(&db, session).await;
    assert!(
        log.iter().any(|(status, output)| {
            status == "Completed" && output.as_deref().is_some_and(|text| text.contains("Body."))
        }),
        "{log:?}"
    );
}

async fn app_log(
    db: &Path,
    session: asterpath_protocol::SessionId,
) -> Vec<(String, Option<String>)> {
    let backend = asterpath_storage_sqlite::open(db).expect("open");
    let mut stream = backend.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut out = Vec::new();
    while let Some(entry) = stream.next().await {
        let entry = entry.expect("entry");
        let Ok(event) = asterpath_protocol::decode_entry(&entry) else {
            continue;
        };
        if let asterpath_protocol::SessionEvent::ToolResult { status, output, .. } = event {
            let text = output.map(|parts| {
                parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect()
            });
            out.push((format!("{status:?}"), text));
        }
    }
    out
}
