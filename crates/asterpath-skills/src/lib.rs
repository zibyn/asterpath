//! Application-resolved instruction packages: `SKILL.md` discovery, the session index
//! and the `load_skill` tool.
//!
//! A skill grants nothing. Discovery only reads metadata; the body is read by
//! the model on demand through an ordinary `ReadOnly` + `Safe` tool.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};

use asterpath_protocol::{
    Parallelism, Part, ReplaySafety, SideEffect, Tool, ToolArgs, ToolContext, ToolDescriptor,
    ToolError, ToolName, ToolOutput,
};
use futures_core::future::BoxFuture;
use saphyr::{LoadableYamlNode, Yaml};
use serde::Deserialize;

/// Model-visible name of the on-demand loader.
pub const LOAD_SKILL_TOOL_NAME: &str = "load_skill";
/// Total characters the rendered index may occupy.
pub const DEFAULT_INDEX_BUDGET: usize = 8_000;

const MAX_SKILL_BYTES: usize = 64 * 1024;
const MAX_FRONTMATTER_BYTES: usize = 16 * 1024;
const MAX_DESCRIPTION_CHARS: usize = 1024;
const MAX_NAME_LEN: usize = 64;

/// Fields that exist in other products' skill formats but are never executed.
const AUTHORITY_FIELDS: [&str; 6] = [
    "allowed-tools",
    "disallowed-tools",
    "model",
    "hooks",
    "context",
    "agent",
];

/// One configured search root. Roots sharing a label form one priority layer;
/// roots are scanned in the order given, but the label decides priority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRoot {
    pub path: PathBuf,
    pub label: SkillRootLabel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkillRootLabel {
    Explicit,
    ProjectNative,
    ProjectAgents,
    UserNative,
    UserAgents,
}

impl SkillRootLabel {
    /// Fixed priority order from `docs/design/skills.md` §4; lower wins.
    fn rank(self) -> usize {
        match self {
            Self::Explicit => 0,
            Self::ProjectNative => 1,
            Self::ProjectAgents => 2,
            Self::UserNative => 3,
            Self::UserAgents => 4,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillSummary {
    pub name: String,
    pub description: String,
    /// The root that supplied the skill.
    pub root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillDiagnostic {
    pub root: PathBuf,
    pub skill: Option<String>,
    pub kind: SkillDiagnosticKind,
    pub message: String,
}

impl fmt::Display for SkillDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.root.display())?;
        if let Some(skill) = &self.skill {
            write!(f, " (skill {skill})")?;
        }
        write!(f, ": {:?}: {}", self.kind, self.message)
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkillDiagnosticKind {
    Unreadable,
    FrontmatterMissing,
    FrontmatterInvalid,
    DescriptionMissing,
    DescriptionInvalid,
    FieldTypeInvalid,
    NameInvalid,
    NameMismatch,
    TooLarge,
    DuplicateInLayer,
    Shadowed,
    IndexBudgetExceeded,
    AuthorityFieldIgnored,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Discovery {
    pub skills: Vec<SkillSummary>,
    pub diagnostics: Vec<SkillDiagnostic>,
}

struct Candidate {
    layer: usize,
    name: String,
    description: String,
    root: PathBuf,
    canonical: PathBuf,
    ignored_fields: Vec<String>,
}

/// Scan every root once. A broken skill is skipped without affecting the rest;
/// the same skill found through the same canonical directory is counted once.
pub fn discover(roots: &[SkillRoot]) -> Discovery {
    let mut candidates = Vec::new();
    let mut diagnostics = Vec::new();

    for root in roots {
        scan_root(root, &mut candidates, &mut diagnostics);
    }

    let mut seen_paths = HashSet::new();
    candidates.retain(|candidate| seen_paths.insert(candidate.canonical.clone()));

    let mut by_name: BTreeMap<String, Vec<Candidate>> = BTreeMap::new();
    for candidate in candidates {
        by_name
            .entry(candidate.name.clone())
            .or_default()
            .push(candidate);
    }

    let mut winners: Vec<(usize, SkillSummary)> = Vec::new();
    for (name, group) in by_name {
        let highest = group
            .iter()
            .map(|candidate| candidate.layer)
            .min()
            .expect("group is non-empty");
        let top: Vec<&Candidate> = group
            .iter()
            .filter(|candidate| candidate.layer == highest)
            .collect();
        if top.len() > 1 {
            for candidate in &top {
                diagnostics.push(SkillDiagnostic {
                    root: candidate.root.clone(),
                    skill: Some(name.clone()),
                    kind: SkillDiagnosticKind::DuplicateInLayer,
                    message: format!(
                        "the name `{name}` is defined {} times in one layer; all copies are skipped",
                        top.len()
                    ),
                });
            }
            continue;
        }
        let winner = top[0];
        for candidate in group.iter().filter(|candidate| candidate.layer != highest) {
            diagnostics.push(SkillDiagnostic {
                root: candidate.root.clone(),
                skill: Some(name.clone()),
                kind: SkillDiagnosticKind::Shadowed,
                message: format!(
                    "shadowed by the copy in {}",
                    winner.root.join(&name).display()
                ),
            });
        }
        for field in &winner.ignored_fields {
            diagnostics.push(SkillDiagnostic {
                root: winner.root.clone(),
                skill: Some(name.clone()),
                kind: SkillDiagnosticKind::AuthorityFieldIgnored,
                message: format!("Asterpath does not execute `{field}`"),
            });
        }
        winners.push((
            highest,
            SkillSummary {
                name,
                description: winner.description.clone(),
                root: winner.root.clone(),
            },
        ));
    }
    winners.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.name.cmp(&right.1.name))
    });

    Discovery {
        skills: winners.into_iter().map(|(_, skill)| skill).collect(),
        diagnostics,
    }
}

fn scan_root(
    root: &SkillRoot,
    candidates: &mut Vec<Candidate>,
    diagnostics: &mut Vec<SkillDiagnostic>,
) {
    let layer = root.label.rank();
    let Ok(entries) = std::fs::read_dir(&root.path) else {
        return;
    };
    let mut dirs: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();
    dirs.sort();

    for dir in dirs {
        if !std::fs::metadata(&dir).is_ok_and(|meta| meta.is_dir()) {
            continue;
        }
        let Some(name) = dir.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let file = dir.join("SKILL.md");
        if !file.is_file() {
            continue;
        }
        let diagnose = |kind, message: String| SkillDiagnostic {
            root: root.path.clone(),
            skill: Some(name.to_owned()),
            kind,
            message,
        };
        match read_skill_file(&file) {
            Err(error) => diagnostics.push(diagnose(
                SkillDiagnosticKind::Unreadable,
                format!("cannot read {}: {error}", file.display()),
            )),
            Ok(SkillRead::TooLarge) => diagnostics.push(diagnose(
                SkillDiagnosticKind::TooLarge,
                format!("SKILL.md exceeds {MAX_SKILL_BYTES} bytes"),
            )),
            Ok(SkillRead::NotUtf8) => diagnostics.push(diagnose(
                SkillDiagnosticKind::Unreadable,
                "SKILL.md is not valid UTF-8".to_owned(),
            )),
            Ok(SkillRead::Content(content)) => match parse_skill(name, &content) {
                Ok(parsed) => candidates.push(Candidate {
                    layer,
                    name: parsed.name,
                    description: parsed.description,
                    root: root.path.clone(),
                    canonical: std::fs::canonicalize(&dir).unwrap_or(dir),
                    ignored_fields: parsed.ignored_fields,
                }),
                Err((kind, message)) => diagnostics.push(diagnose(kind, message)),
            },
        }
    }
}

struct ParsedSkill {
    name: String,
    description: String,
    ignored_fields: Vec<String>,
}

enum SkillRead {
    Content(String),
    TooLarge,
    NotUtf8,
}

/// Read at most `MAX_SKILL_BYTES + 1` bytes so a hostile file cannot be
/// allocated in full before it is rejected.
fn read_skill_file(path: &Path) -> std::io::Result<SkillRead> {
    let mut file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take((MAX_SKILL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SKILL_BYTES {
        return Ok(SkillRead::TooLarge);
    }
    Ok(match String::from_utf8(bytes) {
        Ok(content) => SkillRead::Content(content),
        Err(_) => SkillRead::NotUtf8,
    })
}

fn parse_skill(
    dir_name: &str,
    content: &str,
) -> Result<ParsedSkill, (SkillDiagnosticKind, String)> {
    let Some((yaml, _)) = split_frontmatter(content) else {
        return Err((
            SkillDiagnosticKind::FrontmatterMissing,
            "SKILL.md must open with a `---` frontmatter block".to_owned(),
        ));
    };
    if yaml.len() > MAX_FRONTMATTER_BYTES {
        return Err((
            SkillDiagnosticKind::TooLarge,
            format!("frontmatter exceeds {MAX_FRONTMATTER_BYTES} bytes"),
        ));
    }
    let documents = Yaml::load_from_str(yaml).map_err(|error| {
        (
            SkillDiagnosticKind::FrontmatterInvalid,
            format!("invalid YAML frontmatter: {error}"),
        )
    })?;
    let Some(document) = documents.first() else {
        return Err((
            SkillDiagnosticKind::DescriptionMissing,
            "missing `description`".to_owned(),
        ));
    };
    if !document.is_mapping() {
        return Err((
            SkillDiagnosticKind::FrontmatterInvalid,
            "frontmatter must be a mapping".to_owned(),
        ));
    }

    let description = match document.as_mapping_get("description") {
        None => {
            return Err((
                SkillDiagnosticKind::DescriptionMissing,
                "missing `description`".to_owned(),
            ));
        }
        Some(node) => match node.as_str() {
            Some(value) if !value.trim().is_empty() => value.to_owned(),
            Some(_) => {
                return Err((
                    SkillDiagnosticKind::DescriptionMissing,
                    "`description` is empty".to_owned(),
                ));
            }
            None => {
                return Err((
                    SkillDiagnosticKind::FieldTypeInvalid,
                    "`description` must be a string".to_owned(),
                ));
            }
        },
    };
    if description.chars().count() > MAX_DESCRIPTION_CHARS {
        return Err((
            SkillDiagnosticKind::DescriptionInvalid,
            format!("`description` exceeds {MAX_DESCRIPTION_CHARS} characters"),
        ));
    }

    let name_node = document.as_mapping_get("name");
    let name = match name_node {
        None => dir_name.to_owned(),
        Some(node) => match node.as_str() {
            Some(value) => value.to_owned(),
            None => {
                return Err((
                    SkillDiagnosticKind::FieldTypeInvalid,
                    "`name` must be a string".to_owned(),
                ));
            }
        },
    };
    if !valid_name(&name) {
        return Err((
            SkillDiagnosticKind::NameInvalid,
            format!("`{name}` is not a valid skill name"),
        ));
    }
    if name_node.is_some() && name != dir_name {
        return Err((
            SkillDiagnosticKind::NameMismatch,
            format!("`name` is `{name}` but the directory is `{dir_name}`"),
        ));
    }

    let mut ignored_fields = Vec::new();
    for field in AUTHORITY_FIELDS {
        if document.contains_mapping_key(field) {
            ignored_fields.push(field.to_owned());
        }
    }

    Ok(ParsedSkill {
        name,
        description,
        ignored_fields,
    })
}

fn split_frontmatter(content: &str) -> Option<(&str, &str)> {
    let rest = content
        .strip_prefix("---\r\n")
        .or_else(|| content.strip_prefix("---\n"))?;
    let mut offset = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            return Some((&rest[..offset], &rest[offset + line.len()..]));
        }
        offset += line.len();
    }
    None
}

fn valid_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return false;
    }
    name.split('-').all(|segment| {
        !segment.is_empty()
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    })
}

/// Render the progressive-disclosure index. Once one entry does not fit, every
/// later (lower-priority) entry is omitted as well.
pub fn render_index(
    skills: &[SkillSummary],
    budget: usize,
) -> (Option<Part>, Vec<SkillDiagnostic>) {
    let mut text = String::from(
        "## Skills\n\nCall `load_skill` with a skill name when the task matches its description.\n\n",
    );
    let mut omitted = Vec::new();
    let mut fits = true;
    for skill in skills {
        let line = format!("- {}: {}\n", skill.name, flatten(&skill.description));
        if fits && text.chars().count() + line.chars().count() <= budget {
            text.push_str(&line);
        } else {
            fits = false;
            omitted.push(SkillDiagnostic {
                root: skill.root.clone(),
                skill: Some(skill.name.clone()),
                kind: SkillDiagnosticKind::IndexBudgetExceeded,
                message: format!("index budget of {budget} characters exceeded; skill omitted"),
            });
        }
    }
    if omitted.len() == skills.len() {
        return (None, omitted);
    }
    (Some(Part::Text { text }), omitted)
}

fn flatten(text: &str) -> String {
    let mut flat = String::with_capacity(text.len());
    let mut pending_space = false;
    for character in text.chars() {
        if character.is_control() || character.is_whitespace() {
            pending_space = !flat.is_empty();
        } else {
            if pending_space {
                flat.push(' ');
                pending_space = false;
            }
            flat.push(character);
        }
    }
    flat
}

/// Reads `<root>/<name>/SKILL.md` from the current disk at call time. The index
/// is a discovery surface, not an access-control list.
pub struct LoadSkillTool {
    roots: Vec<SkillRoot>,
    descriptor: ToolDescriptor,
}

impl LoadSkillTool {
    pub fn new(roots: Vec<SkillRoot>) -> Self {
        Self {
            roots,
            descriptor: descriptor(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoadArgs {
    name: String,
}

impl Tool for LoadSkillTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let args: LoadArgs =
                serde_json::from_str(args.as_str()).map_err(|error| ToolError::InvalidArgs {
                    message: format!("invalid load_skill arguments: {error}"),
                })?;
            if !valid_name(&args.name) {
                return Err(ToolError::InvalidArgs {
                    message: format!("`{}` is not a valid skill name", args.name),
                });
            }
            for root in &self.roots {
                let path = root.path.join(&args.name).join("SKILL.md");
                let content = match read_skill_file(&path) {
                    Ok(SkillRead::Content(content)) => content,
                    Ok(SkillRead::TooLarge) => {
                        return Err(ToolError::Internal {
                            message: format!("{} exceeds {MAX_SKILL_BYTES} bytes", path.display()),
                        });
                    }
                    Ok(SkillRead::NotUtf8) => {
                        return Err(ToolError::Internal {
                            message: format!("{} is not valid UTF-8", path.display()),
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        return Err(ToolError::Internal {
                            message: format!("cannot read {}: {error}", path.display()),
                        });
                    }
                };
                let body = split_frontmatter(&content)
                    .map(|(_, body)| body)
                    .unwrap_or(&content);
                return Ok(ToolOutput::new(vec![Part::Text {
                    text: body.to_owned(),
                }]));
            }
            Err(ToolError::NotFound {
                message: format!("unknown skill `{}`", args.name),
            })
        })
    }
}

fn descriptor() -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new(LOAD_SKILL_TOOL_NAME),
        "Load the full instructions for a skill by name.",
        serde_json::json!({
            "type": "object",
            "properties": { "name": { "type": "string" } },
            "required": ["name"],
            "additionalProperties": false
        }),
    )
    .with_parallelism(Parallelism::Parallel)
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly)
}
