//! The sidebar's commands over a database another host writes (ADR-0035).

mod common;

use std::path::Path;
use std::sync::Arc;

use asterpath::Asterpath;
use asterpath_desktop::Desktop;
use asterpath_host::Workspace;
use asterpath_protocol::Provider;
use asterpath_testing::ScriptedProvider;
use common::template;

async fn engine(db: &Path) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(vec![
            Arc::new(ScriptedProvider::new("p")) as Arc<dyn Provider>
        ])
        .build()
        .await
        .expect("build")
}

/// A session as the CLI creates it: the host kit's metadata for its root.
async fn cli_session(asterpath: &Asterpath, root: &Path, name: Option<&str>) -> String {
    let mut spec = template();
    spec.metadata = asterpath_host::metadata(root, name);
    asterpath.create_session(spec).await.unwrap().id().to_hex()
}

#[tokio::test]
async fn a_session_the_cli_created_is_listed_under_its_project() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("asterpath.db");
    let here = asterpath_host::work_root(&dir.path().join("..")).unwrap();
    let there = dir.path().canonicalize().unwrap();

    let cli = engine(&db).await;
    let fix = cli_session(&cli, &here, Some("fix")).await;
    let other = cli_session(&cli, &there, None).await;

    let desktop = Desktop::new(
        engine(&db).await,
        Workspace::default(),
        template(),
        dir.path().join("projects.json"),
    );
    let roots: Vec<String> = desktop
        .projects()
        .await
        .unwrap()
        .into_iter()
        .map(|project| project.root)
        .collect();
    assert!(roots.contains(&here.display().to_string()), "{roots:?}");
    assert!(roots.contains(&there.display().to_string()), "{roots:?}");

    let sessions = desktop.sessions(&here).await.unwrap();
    assert_eq!(sessions.len(), 1, "only the root's sessions");
    assert_eq!(sessions[0].id.to_hex(), fix);
    assert_eq!(sessions[0].name.as_deref(), Some("fix"));
    assert_eq!(
        desktop.sessions(&there).await.unwrap()[0].id.to_hex(),
        other
    );
}

#[tokio::test]
async fn an_added_project_is_listed_without_sessions_and_kept_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("asterpath.db");
    let projects = dir.path().join("state/projects.json");
    let root = dir.path().join("work");
    std::fs::create_dir(&root).unwrap();

    let desktop = Desktop::new(
        engine(&db).await,
        Workspace::default(),
        template(),
        projects.clone(),
    );
    let added = desktop.add_project(&root.join(".")).unwrap();
    assert_eq!(
        added.root,
        root.canonicalize().unwrap().display().to_string()
    );
    assert_eq!(added.name, "work");
    desktop.add_project(&root).unwrap();
    assert!(desktop.add_project(&dir.path().join("missing")).is_err());
    drop(desktop);

    let desktop = Desktop::new(
        engine(&db).await,
        Workspace::default(),
        template(),
        projects,
    );
    assert_eq!(desktop.projects().await.unwrap(), [added]);
}
