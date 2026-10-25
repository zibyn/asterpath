//! Reference CLI tool selection: the six `asterpath-tools` tools are on by
//! default, `[tools] enabled` (or `-c tools.enabled=[...]`) picks a subset, an
//! empty list drops them all.

mod common;

use std::path::PathBuf;

use common::{MockServer, Reply, bin};

const ALL: [&str; 6] = [
    "read_file",
    "list_dir",
    "grep",
    "write_file",
    "edit_file",
    "bash",
];

/// Workspace tool names the model saw in the first request, sorted.
fn run(file: &str, flags: &[&str]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello"))]);
    let config: PathBuf = dir.path().join("asterpath.toml");
    std::fs::write(
        &config,
        format!(
            "[storage]\npath = \"{}\"\n\n[providers.anthropic]\nbase_url = \"{}\"\n\n[skills]\nenabled = false\n\n{file}",
            dir.path().join("sessions.db").display(),
            server.base_url()
        ),
    )
    .expect("config");
    let mut args = vec![
        "exec",
        "--config",
        config.to_str().unwrap(),
        "-c",
        "model.provider=anthropic",
        "-m",
        "claude-test",
    ];
    args.extend_from_slice(flags);
    args.push("hi");
    let output = bin()
        .current_dir(dir.path())
        .args(&args)
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = server.requests()[0].json_body();
    let mut names: Vec<String> = body["tools"]
        .as_array()
        .map(|tools| {
            tools
                .iter()
                .map(|tool| tool["name"].as_str().unwrap().to_owned())
                .filter(|name| ALL.contains(&name.as_str()))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn sorted<const N: usize>(names: [&str; N]) -> Vec<String> {
    let mut names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
    names.sort();
    names
}

#[test]
fn all_six_tools_are_registered_by_default() {
    assert_eq!(run("", &[]), sorted(ALL));
}

#[test]
fn an_empty_list_drops_every_tool() {
    assert!(run("", &["-c", "tools.enabled=[]"]).is_empty());
}

#[test]
fn an_override_selects_a_subset() {
    assert_eq!(
        run("", &["-c", r#"tools.enabled=["read_file"]"#]),
        ["read_file"]
    );
    assert_eq!(
        run("", &["-c", r#"tools.enabled=["grep","bash"]"#]),
        ["bash", "grep"]
    );
}

#[test]
fn config_selects_a_subset_and_the_override_wins() {
    let file = "[tools]\nenabled = [\"list_dir\"]\n";
    assert_eq!(run(file, &[]), ["list_dir"]);
    assert_eq!(
        run(file, &["-c", r#"tools.enabled=["read_file"]"#]),
        ["read_file"]
    );
    assert!(run(file, &["-c", "tools.enabled=[]"]).is_empty());
}

#[test]
fn an_unknown_tool_name_is_a_usage_error() {
    let output = bin()
        .args([
            "exec",
            "-c",
            r#"tools.enabled=["nope"]"#,
            "-c",
            "model.provider=anthropic",
            "-m",
            "m",
            "hi",
        ])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("unknown tool nope"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
