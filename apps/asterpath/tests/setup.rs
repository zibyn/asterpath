//! The desktop's settings page: reading and writing `config.toml` (ADR-0050).

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use asterpath_app::config::{KeySource, SetupChoice};
use asterpath_app::desktop::{Desktop, save_setup, setup};

fn choice(provider: &str, model: &str, api_key: Option<&str>) -> SetupChoice {
    SetupChoice {
        provider: provider.to_owned(),
        model: model.to_owned(),
        protocol: None,
        base_url: None,
        api_key: api_key.map(str::to_owned),
    }
}

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

    save_setup(
        Some(path.clone()),
        &choice("deepseek", "ds-pro", Some("sk-secret")),
    )
    .unwrap();
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
    save_setup(
        Some(path.clone()),
        &choice("deepseek", "ds-flash", Some("")),
    )
    .unwrap();
    assert!(fs::read_to_string(&path).unwrap().contains("sk-secret"));

    let desktop = Desktop::open(Some(path), dir.path().join("projects.json"), None)
        .await
        .unwrap();
    desktop.create_session(dir.path(), None).await.unwrap();
}

#[test]
fn an_invalid_choice_leaves_the_file_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path());
    let before = fs::read_to_string(&path).unwrap();
    assert!(save_setup(Some(path.clone()), &choice("gemini", "m", Some("k"))).is_err());
    assert!(save_setup(Some(path.clone()), &choice("deepseek", " ", None)).is_err());
    assert_eq!(fs::read_to_string(&path).unwrap(), before);
}

#[test]
fn a_missing_file_is_created_owner_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("new/config.toml");
    save_setup(
        Some(path.clone()),
        &choice("anthropic", "claude-test", Some("sk")),
    )
    .unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let text = fs::read_to_string(&path).unwrap();
    assert!(!text.contains("[providers]\n"), "{text}");
    assert!(text.contains("[providers.anthropic]"), "{text}");
}

#[test]
fn a_new_instance_gets_its_protocol_and_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path());
    let mut custom = choice("relay", "m", Some("sk"));
    custom.protocol = Some("openai".to_owned());
    custom.base_url = Some("http://127.0.0.1:9/v1".to_owned());
    save_setup(Some(path.clone()), &custom).unwrap();

    let relay = setup(Some(path.clone()))
        .providers
        .into_iter()
        .find(|p| p.id == "relay")
        .unwrap();
    assert_eq!(relay.protocol, "openai");
    assert_eq!(relay.base_url.as_deref(), Some("http://127.0.0.1:9/v1"));
    // Its own variable, not OPENAI_API_KEY.
    assert!(
        fs::read_to_string(&path)
            .unwrap()
            .contains("api_key_env = \"RELAY_API_KEY\"")
    );

    // Empty clears it back to the adapter's default; `None` keeps it.
    custom.base_url = Some(String::new());
    save_setup(Some(path.clone()), &custom).unwrap();
    assert!(!fs::read_to_string(&path).unwrap().contains("base_url"));
}
