//! The desktop's settings page: reading and writing `config.toml` (ADR-0050).

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use asterpath_app::config::KeySource;
use asterpath_app::desktop::{Desktop, save_setup, setup};

/// A config with comments and an instance whose key variable is never set.
fn write_config(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    fs::write(
        &path,
        format!(
            concat!(
                "# keep me\n[providers.deepseek]\nprotocol = \"anthropic\"\n",
                "api_key_env = \"ASTERPATH_SETUP_TEST_UNSET\"\n# and me\n\n",
                "[storage]\npath = \"{}\"\n\n[skills]\nenabled = false\n",
            ),
            dir.join("sessions.db").display()
        ),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    path
}

#[tokio::test]
async fn saved_settings_keep_comments_are_owner_only_and_open_a_backend() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path());

    let before = setup(Some(path.clone()));
    assert!(before.needed, "{before:?}");
    assert!(before.error.is_none(), "{before:?}");
    let deepseek = before
        .providers
        .iter()
        .find(|p| p.id == "deepseek")
        .unwrap();
    assert_eq!(deepseek.key, KeySource::Missing);

    save_setup(Some(path.clone()), "deepseek", "ds-pro", Some("sk-secret")).unwrap();
    let text = fs::read_to_string(&path).unwrap();
    assert!(
        text.contains("# keep me") && text.contains("# and me"),
        "{text}"
    );
    assert!(text.contains("api_key = \"sk-secret\""), "{text}");
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let after = setup(Some(path.clone()));
    assert!(!after.needed, "{after:?}");
    assert_eq!(after.provider.as_deref(), Some("deepseek"));
    assert_eq!(after.model.as_deref(), Some("ds-pro"));
    let deepseek = after.providers.iter().find(|p| p.id == "deepseek").unwrap();
    assert_eq!(deepseek.key, KeySource::File);
    assert!(!serde_json::to_string(&after).unwrap().contains("sk-secret"));

    // An empty key keeps the file's.
    save_setup(Some(path.clone()), "deepseek", "ds-flash", Some("")).unwrap();
    assert!(fs::read_to_string(&path).unwrap().contains("sk-secret"));

    let desktop = Desktop::open(Some(path), dir.path().join("projects.json"))
        .await
        .unwrap();
    desktop.create_session(dir.path(), None).await.unwrap();
}

#[test]
fn an_invalid_choice_leaves_the_file_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path());
    let before = fs::read_to_string(&path).unwrap();
    assert!(save_setup(Some(path.clone()), "gemini", "m", Some("k")).is_err());
    assert!(save_setup(Some(path.clone()), "deepseek", " ", None).is_err());
    assert_eq!(fs::read_to_string(&path).unwrap(), before);
}

#[test]
fn a_missing_file_is_created_owner_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("new/config.toml");
    save_setup(Some(path.clone()), "anthropic", "claude-test", Some("sk")).unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let text = fs::read_to_string(&path).unwrap();
    assert!(!text.contains("[providers]\n"), "{text}");
    assert!(text.contains("[providers.anthropic]"), "{text}");
}
