//! Skill discovery, index rendering and `load_skill`, exercised through the
//! crate's public interface.

use std::path::{Path, PathBuf};

use asterpath_protocol::{CancelToken, Part, Tool, ToolArgs, ToolContext, ToolError};
use asterpath_protocol::{SessionId, StepId, ToolCallId, TurnId};
use asterpath_skills::{
    DEFAULT_INDEX_BUDGET, Discovery, LoadSkillTool, SkillDiagnosticKind, SkillRoot, SkillRootLabel,
    discover, render_index,
};

fn skill(root: &Path, dir: &str, frontmatter: &str, body: &str) {
    let path = root.join(dir);
    std::fs::create_dir_all(&path).expect("skill dir");
    std::fs::write(
        path.join("SKILL.md"),
        format!("---\n{frontmatter}\n---\n{body}"),
    )
    .expect("write skill");
}

fn root_at(path: &Path, label: SkillRootLabel) -> SkillRoot {
    SkillRoot {
        path: path.to_path_buf(),
        label,
    }
}

fn kinds(discovery: &Discovery) -> Vec<(String, SkillDiagnosticKind)> {
    discovery
        .diagnostics
        .iter()
        .map(|diagnostic| {
            (
                diagnostic.skill.clone().unwrap_or_default(),
                diagnostic.kind,
            )
        })
        .collect()
}

fn names(discovery: &Discovery) -> Vec<&str> {
    discovery
        .skills
        .iter()
        .map(|skill| skill.name.as_str())
        .collect()
}

#[test]
fn s1_higher_priority_layer_shadows_lower_and_reports_it() {
    let dir = tempfile::tempdir().expect("temp");
    let explicit = dir.path().join("explicit");
    let project = dir.path().join("project");
    skill(&explicit, "demo", "description: explicit", "explicit body");
    skill(&project, "demo", "description: project", "project body");
    skill(&project, "other", "description: project only", "body");

    let discovery = discover(&[
        root_at(&explicit, SkillRootLabel::Explicit),
        root_at(&project, SkillRootLabel::ProjectNative),
    ]);

    assert_eq!(names(&discovery), vec!["demo", "other"]);
    assert_eq!(discovery.skills[0].description, "explicit");
    assert_eq!(
        kinds(&discovery),
        vec![("demo".to_owned(), SkillDiagnosticKind::Shadowed)]
    );
}

#[test]
fn s1_duplicate_name_in_one_layer_skips_both() {
    let dir = tempfile::tempdir().expect("temp");
    let first = dir.path().join("first");
    let second = dir.path().join("second");
    skill(&first, "demo", "description: one", "one");
    skill(&second, "demo", "description: two", "two");
    skill(&first, "kept", "description: kept", "kept");

    let discovery = discover(&[
        root_at(&first, SkillRootLabel::Explicit),
        root_at(&second, SkillRootLabel::Explicit),
    ]);

    assert_eq!(names(&discovery), vec!["kept"]);
    let duplicate = kinds(&discovery);
    assert_eq!(duplicate.len(), 2, "{duplicate:?}");
    assert!(
        duplicate
            .iter()
            .all(|(_, kind)| *kind == SkillDiagnosticKind::DuplicateInLayer)
    );
}

#[test]
fn s2_invalid_skills_are_isolated_and_authority_fields_are_reported() {
    let dir = tempfile::tempdir().expect("temp");
    let root = dir.path().join("skills");
    skill(&root, "missing-desc", "name: missing-desc", "body");
    skill(&root, "mismatch", "name: other\ndescription: d", "body");
    skill(&root, "unknown-field", "description: d\nfuture: 1", "body");
    skill(
        &root,
        "authority",
        "description: d\nallowed-tools: [x]",
        "body",
    );
    skill(&root, "valid", "description: fine", "body");
    std::fs::create_dir_all(root.join("too-big")).expect("dir");
    let oversized = format!("---\ndescription: d\n---\n{}", "x".repeat(65 * 1024));
    std::fs::write(root.join("too-big/SKILL.md"), oversized).expect("write");

    let discovery = discover(&[root_at(&root, SkillRootLabel::ProjectNative)]);

    assert_eq!(
        names(&discovery),
        vec!["authority", "unknown-field", "valid"]
    );
    let diagnostics = kinds(&discovery);
    for expected in [
        ("missing-desc", SkillDiagnosticKind::DescriptionMissing),
        ("mismatch", SkillDiagnosticKind::NameMismatch),
        ("too-big", SkillDiagnosticKind::TooLarge),
        ("authority", SkillDiagnosticKind::AuthorityFieldIgnored),
    ] {
        assert!(
            diagnostics
                .iter()
                .any(|(name, kind)| name == expected.0 && *kind == expected.1),
            "missing {expected:?} in {diagnostics:?}"
        );
    }
}

#[test]
fn s2_field_types_limits_and_encoding_are_rejected() {
    let dir = tempfile::tempdir().expect("temp");
    let root = dir.path().join("skills");
    skill(&root, "numeric-desc", "description: 123", "body");
    skill(&root, "bool-name", "name: true\ndescription: d", "body");
    let long = "x".repeat(1025);
    skill(&root, "long-desc", &format!("description: {long}"), "body");
    skill(&root, "Bad_Name", "name: Bad_Name\ndescription: d", "body");
    let pad = "x".repeat(17 * 1024);
    skill(
        &root,
        "big-frontmatter",
        &format!("description: d\npad: \"{pad}\""),
        "body",
    );
    std::fs::create_dir_all(root.join("binary")).expect("dir");
    std::fs::write(root.join("binary/SKILL.md"), [0xff, 0xfe, 0x00]).expect("write");

    let discovery = discover(&[root_at(&root, SkillRootLabel::ProjectNative)]);

    assert!(discovery.skills.is_empty(), "{:?}", discovery.skills);
    let diagnostics = kinds(&discovery);
    for expected in [
        ("numeric-desc", SkillDiagnosticKind::FieldTypeInvalid),
        ("bool-name", SkillDiagnosticKind::FieldTypeInvalid),
        ("long-desc", SkillDiagnosticKind::DescriptionInvalid),
        ("Bad_Name", SkillDiagnosticKind::NameInvalid),
        ("big-frontmatter", SkillDiagnosticKind::TooLarge),
        ("binary", SkillDiagnosticKind::Unreadable),
    ] {
        assert!(
            diagnostics
                .iter()
                .any(|(name, kind)| name == expected.0 && *kind == expected.1),
            "missing {expected:?} in {diagnostics:?}"
        );
    }
}

#[test]
fn s10_name_defaults_to_directory_and_mismatch_is_rejected() {
    let dir = tempfile::tempdir().expect("temp");
    let root = dir.path().join("skills");
    std::fs::create_dir_all(root.join("default-name")).expect("dir");
    std::fs::write(
        root.join("default-name/SKILL.md"),
        "---\ndescription: implied\n---\nbody",
    )
    .expect("write");
    skill(&root, "wrong-name", "name: another\ndescription: d", "body");

    let discovery = discover(&[root_at(&root, SkillRootLabel::ProjectNative)]);

    assert_eq!(names(&discovery), vec!["default-name"]);
    assert_eq!(discovery.skills[0].description, "implied");
    assert_eq!(
        kinds(&discovery),
        vec![("wrong-name".to_owned(), SkillDiagnosticKind::NameMismatch)]
    );
}

#[test]
fn s9_index_budget_omits_everything_after_the_first_overflow() {
    let header = "## Skills\n\nCall `load_skill` with a skill name when the task matches its description.\n\n";
    let first = "- first: one\n";
    let skills: Vec<_> = ["first", "second", "third"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| asterpath_skills::SkillSummary {
            name: name.to_owned(),
            description: ["one", "two", "three"][index].to_owned(),
            root: PathBuf::from("/skills"),
        })
        .collect();

    let budget = header.chars().count() + first.chars().count();
    let (part, diagnostics) = render_index(&skills, budget);

    let Some(Part::Text { text }) = part else {
        panic!("expected a text part");
    };
    assert!(text.contains("- first: one"));
    assert!(!text.contains("- second"));
    assert!(!text.contains("- third"));
    assert_eq!(diagnostics.len(), 2);
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| diagnostic.kind == SkillDiagnosticKind::IndexBudgetExceeded)
    );

    let (all, none_omitted) = render_index(&skills, DEFAULT_INDEX_BUDGET);
    assert!(all.is_some());
    assert!(none_omitted.is_empty());
}

#[test]
fn no_skills_renders_no_index() {
    let (part, diagnostics) = render_index(&[], DEFAULT_INDEX_BUDGET);
    assert!(part.is_none());
    assert!(diagnostics.is_empty());
}

#[test]
fn descriptions_flatten_to_one_line() {
    let skills = vec![asterpath_skills::SkillSummary {
        name: "multi".to_owned(),
        description: "line one\nline\ttwo".to_owned(),
        root: PathBuf::from("/skills"),
    }];
    let (Some(Part::Text { text }), _) = render_index(&skills, DEFAULT_INDEX_BUDGET) else {
        panic!("expected a text part");
    };
    assert!(text.contains("- multi: line one line two"), "{text}");
}

fn context() -> ToolContext {
    ToolContext::new(
        SessionId::from_bytes([1; 16]),
        TurnId::from_bytes([2; 16]),
        StepId::from_bytes([3; 16]),
        ToolCallId::from_bytes([4; 16]),
        CancelToken::new(),
    )
}

#[tokio::test]
async fn load_skill_returns_the_body_and_maps_failures() {
    let dir = tempfile::tempdir().expect("temp");
    let root = dir.path().join("skills");
    skill(&root, "demo", "description: d", "# Demo\n\nDo the thing.\n");
    let tool = LoadSkillTool::new(vec![root_at(&root, SkillRootLabel::ProjectNative)]);

    let output = tool
        .invoke(ToolArgs::new(r#"{"name":"demo"}"#), context())
        .await
        .expect("load");
    assert_eq!(
        output.content,
        vec![Part::Text {
            text: "# Demo\n\nDo the thing.\n".to_owned()
        }]
    );

    let missing = tool
        .invoke(ToolArgs::new(r#"{"name":"absent"}"#), context())
        .await
        .expect_err("missing skill");
    assert!(matches!(missing, ToolError::NotFound { .. }), "{missing:?}");

    for args in [r#"{"name":"../demo"}"#, r#"{"name":"Demo"}"#, r#"{}"#] {
        let error = tool
            .invoke(ToolArgs::new(args), context())
            .await
            .expect_err("invalid arguments");
        assert!(
            matches!(error, ToolError::InvalidArgs { .. }),
            "{args}: {error:?}"
        );
    }

    assert_eq!(tool.descriptor().name.as_str(), "load_skill");
    assert!(tool.descriptor().replay == asterpath_protocol::ReplaySafety::Safe);
    assert!(tool.descriptor().side_effect == asterpath_protocol::SideEffect::ReadOnly);
    assert!(tool.descriptor().parallelism == asterpath_protocol::Parallelism::Parallel);
}
