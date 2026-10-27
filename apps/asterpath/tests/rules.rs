//! Command rules (#197): the matcher on its own, and loading the rules from `config.toml`.

use asterpath_app::approval::rules::{CommandRules, RuleKind};
use asterpath_app::workspace::WorkspaceError;

fn rules(allow: &[&str], ask: &[&str], deny: &[&str]) -> CommandRules {
    let owned = |list: &[&str]| {
        list.iter()
            .map(|rule| (*rule).to_owned())
            .collect::<Vec<_>>()
    };
    CommandRules::new(&owned(allow), &owned(ask), &owned(deny)).unwrap()
}

fn bash(rules: &CommandRules, command: &str) -> Option<RuleKind> {
    rules.matches(
        "bash",
        &serde_json::json!({ "command": command }).to_string(),
    )
}

#[test]
fn a_deny_rule_matches_any_subcommand_by_token_prefix() {
    let rules = rules(&[], &[], &["bash(git push)"]);
    for command in [
        "git push",
        "git push origin main",
        "ls && git push",
        "ls; git push",
        "cat x | git push",
        "false || git push",
        "(cd sub && git push)",
        "echo $(git push)",
        "/usr/bin/git push",
    ] {
        assert_eq!(bash(&rules, command), Some(RuleKind::Deny), "{command}");
    }
    for command in ["git pushx", "git status", "echo git push", "git 'push x'"] {
        assert_ne!(bash(&rules, command), Some(RuleKind::Deny), "{command}");
    }
}

#[test]
fn deny_beats_ask_beats_allow() {
    let rules = rules(
        &["bash(git)"],
        &["bash(git push)"],
        &["bash(git push --force)"],
    );
    assert_eq!(bash(&rules, "git status"), Some(RuleKind::Allow));
    assert_eq!(bash(&rules, "git push"), Some(RuleKind::Ask));
    assert_eq!(bash(&rules, "git status && git push"), Some(RuleKind::Ask));
    assert_eq!(
        bash(&rules, "git push && git push --force"),
        Some(RuleKind::Deny)
    );
    // The user's ask rule beats the builtin allow.
    let rules = self::rules(&[], &["bash(ls)"], &[]);
    assert_eq!(bash(&rules, "ls -la"), Some(RuleKind::Ask));
}

#[test]
fn every_subcommand_must_be_allowed() {
    let rules = rules(&["bash(cargo test)"], &[], &[]);
    assert_eq!(bash(&rules, "cargo test -p foo"), Some(RuleKind::Allow));
    assert_eq!(bash(&rules, "cargo test && ls -la"), Some(RuleKind::Allow));
    assert_eq!(bash(&rules, "cargo testx"), None);
    assert_eq!(bash(&rules, "cargo test; rm x"), None);
    assert_eq!(bash(&rules, "cargo build"), None);
}

#[test]
fn builtin_read_only_commands_are_allowed() {
    let rules = CommandRules::default();
    for command in [
        "ls -la",
        "cat a.txt | grep foo | wc -l",
        "pwd",
        "head -n 5 a && tail b",
        "rg 'fn main' src",
        "find . -name '*.rs'",
        "grep -r \"x\" . 2>/dev/null",
        "ls ~/notes",
    ] {
        assert_eq!(bash(&rules, command), Some(RuleKind::Allow), "{command}");
    }
}

#[test]
fn risky_forms_of_builtin_commands_are_not_allowed() {
    let rules = CommandRules::default();
    for command in [
        "find . -delete",
        "find . -name x -exec rm {} ;",
        "find . -execdir rm {} +",
        "find . -ok rm {} ;",
        "find . -fprint out",
        "rg --pre ./run x",
        "rg --pre=./run x",
        "rg -z x",
        "ls *",
        "cat ?.txt",
        "ls [ab]",
        "cat {a,b}",
        "ls; rm x",
        "echo $(rm x)",
        "ls `rm x`",
        "PATH=/tmp ls",
        "X=1; ls",
        "cat a > b",
        "ls >> log",
        "ls $DIR",
        "git status",
        "f() { ls; }; f",
    ] {
        assert_eq!(bash(&rules, command), None, "{command}");
    }
    assert_eq!(
        bash(&rules, &format!("ls {}", "a".repeat(5000))),
        None,
        "too long"
    );
}

#[test]
fn an_unparsable_command_matches_no_allow_rule() {
    let rules = rules(&["bash(ls)"], &[], &[]);
    assert_eq!(bash(&rules, "ls 'unterminated"), None);
    assert_eq!(bash(&rules, "ls && && ls"), None);
    assert_eq!(rules.matches("bash", "{}"), None);
    assert_eq!(rules.matches("bash", "not json"), None);
}

#[test]
fn a_tool_rule_matches_by_name_whatever_its_arguments() {
    let rules = rules(&["fs__read"], &["write_file"], &["bash"]);
    assert_eq!(rules.matches("fs__read", "{}"), Some(RuleKind::Allow));
    assert_eq!(rules.matches("write_file", "{}"), Some(RuleKind::Ask));
    assert_eq!(rules.matches("edit_file", "{}"), None);
    assert_eq!(bash(&rules, "ls"), Some(RuleKind::Deny));
    assert_eq!(rules.matches("bash", "not json"), Some(RuleKind::Deny));
}

#[test]
fn only_bash_takes_a_command_prefix() {
    let owned = |rule: &str| vec![rule.to_owned()];
    let error = CommandRules::new(&[], &owned("write_file(x)"), &[]).unwrap_err();
    assert!(error.contains("approval.ask[0]"), "{error}");
    assert!(error.contains("write_file(x)"), "{error}");
    for bad in ["bash()", "bash(ls", "(ls)", ""] {
        assert!(CommandRules::new(&owned(bad), &[], &[]).is_err(), "{bad}");
    }
}

#[test]
fn config_load_reports_the_bad_rule() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        "[approval]\nallow = [\"bash(cargo test)\"]\ndeny = [\"bash(git push)\", \"write_file(x)\"]\n",
    )
    .unwrap();
    let Err(WorkspaceError::Config(message)) = asterpath_app::config::load(Some(config), &[])
    else {
        panic!("expected a config error");
    };
    assert!(message.contains("approval.deny[1]"), "{message}");
    assert!(message.contains("write_file(x)"), "{message}");
}
