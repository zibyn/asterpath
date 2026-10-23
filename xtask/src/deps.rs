use std::fmt;
use std::fs;

use cargo_metadata::{DependencyKind, MetadataCommand};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Contract,
    Module,
    App,
    Tool,
}

#[derive(Debug, Deserialize)]
pub struct AllowlistFile {
    #[serde(default)]
    pub package: Vec<AllowedPackage>,
}

#[derive(Debug, Deserialize)]
pub struct AllowedPackage {
    pub name: String,
    pub role: Role,
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Debug)]
pub struct PackageInfo {
    pub name: String,
    pub features: Vec<String>,
    pub normal_workspace_deps: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Violation {
    PackageNotAllowed {
        package: String,
    },
    DependencyNotAllowed {
        package: String,
        dependency: String,
    },
    ModuleDependency {
        from: String,
        to: String,
    },
    ContractFeatures {
        package: String,
        features: Vec<String>,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::PackageNotAllowed { package } => {
                write!(
                    f,
                    "{package}: workspace member is not listed in deps-allowlist.toml"
                )
            }
            Violation::DependencyNotAllowed {
                package,
                dependency,
            } => write!(
                f,
                "{package}: normal dependency on workspace crate {dependency} is not allowed"
            ),
            Violation::ModuleDependency { from, to } => write!(
                f,
                "{from}: module crates must not depend on other module crates ({to})"
            ),
            Violation::ContractFeatures { package, features } => write!(
                f,
                "{package}: contract crates must not declare features ({})",
                features.join(", ")
            ),
        }
    }
}

/// Pure checker: everything is passed in so the rules can be unit-tested.
pub fn check(packages: &[PackageInfo], allow: &[AllowedPackage]) -> Vec<Violation> {
    let mut violations = Vec::new();

    let role_of = |name: &str| allow.iter().find(|a| a.name == name).map(|a| a.role);

    for package in packages {
        let Some(entry) = allow.iter().find(|a| a.name == package.name) else {
            violations.push(Violation::PackageNotAllowed {
                package: package.name.clone(),
            });
            continue;
        };

        if entry.role == Role::Contract && !package.features.is_empty() {
            violations.push(Violation::ContractFeatures {
                package: package.name.clone(),
                features: package.features.clone(),
            });
        }

        for dependency in &package.normal_workspace_deps {
            if entry.role == Role::Module && role_of(dependency) == Some(Role::Module) {
                violations.push(Violation::ModuleDependency {
                    from: package.name.clone(),
                    to: dependency.clone(),
                });
            }
            if !entry.allow.iter().any(|allowed| allowed == "*")
                && !entry.allow.contains(dependency)
            {
                violations.push(Violation::DependencyNotAllowed {
                    package: package.name.clone(),
                    dependency: dependency.clone(),
                });
            }
        }
    }

    violations
}

/// Reads `cargo metadata --no-deps` and the root allowlist, then checks the workspace.
pub fn check_workspace() -> Result<(), Vec<String>> {
    let metadata = MetadataCommand::new()
        .no_deps()
        .exec()
        .map_err(|err| vec![format!("cargo metadata failed: {err}")])?;

    let allowlist_path = metadata.workspace_root.join("deps-allowlist.toml");
    let allowlist: AllowlistFile = fs::read_to_string(&allowlist_path)
        .map_err(|err| vec![format!("cannot read {}: {err}", allowlist_path)])
        .and_then(|text| {
            toml::from_str(&text)
                .map_err(|err| vec![format!("cannot parse {}: {err}", allowlist_path)])
        })?;

    let workspace: Vec<String> = metadata
        .packages
        .iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .map(|package| package.name.to_string())
        .collect();

    let packages: Vec<PackageInfo> = metadata
        .packages
        .iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .map(|package| PackageInfo {
            name: package.name.to_string(),
            features: package.features.keys().cloned().collect(),
            normal_workspace_deps: package
                .dependencies
                .iter()
                .filter(|dependency| {
                    dependency.kind == DependencyKind::Normal && dependency.source.is_none()
                })
                .map(|dependency| dependency.name.to_string())
                .filter(|name| workspace.contains(name))
                .collect(),
        })
        .collect();

    let violations = check(&packages, &allowlist.package);
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations.into_iter().map(|v| v.to_string()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(name: &str, features: &[&str], deps: &[&str]) -> PackageInfo {
        PackageInfo {
            name: name.to_string(),
            features: features.iter().map(|f| f.to_string()).collect(),
            normal_workspace_deps: deps.iter().map(|d| d.to_string()).collect(),
        }
    }

    fn allowed(name: &str, role: Role, allow: &[&str]) -> AllowedPackage {
        AllowedPackage {
            name: name.to_string(),
            role,
            allow: allow.iter().map(|a| a.to_string()).collect(),
        }
    }

    #[test]
    fn accepts_a_workspace_that_follows_the_allowlist() {
        let packages = [
            package("asterpath-core", &[], &[]),
            package("asterpath-protocol", &[], &["asterpath-core"]),
            package(
                "asterpath-agent",
                &[],
                &["asterpath-core", "asterpath-protocol"],
            ),
        ];
        let allow = [
            allowed("asterpath-core", Role::Contract, &[]),
            allowed("asterpath-protocol", Role::Contract, &["asterpath-core"]),
            allowed(
                "asterpath-agent",
                Role::Module,
                &["asterpath-core", "asterpath-protocol"],
            ),
        ];

        assert!(check(&packages, &allow).is_empty());
    }

    #[test]
    fn rejects_a_module_to_module_dependency() {
        let packages = [
            package("asterpath-agent", &[], &["asterpath-tools"]),
            package("asterpath-tools", &[], &[]),
        ];
        let allow = [
            allowed("asterpath-agent", Role::Module, &["*"]),
            allowed("asterpath-tools", Role::Module, &["*"]),
        ];

        assert!(
            check(&packages, &allow).contains(&Violation::ModuleDependency {
                from: "asterpath-agent".to_string(),
                to: "asterpath-tools".to_string(),
            })
        );
    }

    #[test]
    fn rejects_a_workspace_dependency_outside_the_allowlist() {
        let packages = [
            package("asterpath-agent", &[], &["asterpath-tools"]),
            package("asterpath-tools", &[], &[]),
        ];
        let allow = [
            allowed("asterpath-agent", Role::Module, &["asterpath-core"]),
            allowed("asterpath-tools", Role::Module, &[]),
        ];

        assert!(
            check(&packages, &allow).contains(&Violation::DependencyNotAllowed {
                package: "asterpath-agent".to_string(),
                dependency: "asterpath-tools".to_string(),
            })
        );
    }

    #[test]
    fn rejects_features_on_a_contract_crate() {
        let packages = [package("asterpath-core", &["std"], &[])];
        let allow = [allowed("asterpath-core", Role::Contract, &[])];

        assert_eq!(
            check(&packages, &allow),
            [Violation::ContractFeatures {
                package: "asterpath-core".to_string(),
                features: vec!["std".to_string()],
            }]
        );
    }

    #[test]
    fn rejects_a_package_missing_from_the_allowlist() {
        let packages = [package("asterpath-unknown", &[], &[])];

        assert_eq!(
            check(&packages, &[]),
            [Violation::PackageNotAllowed {
                package: "asterpath-unknown".to_string(),
            }]
        );
    }

    #[test]
    fn wildcard_allows_application_crates_to_depend_on_any_workspace_member() {
        let packages = [
            package("asterpath", &[], &["asterpath-agent", "asterpath-tools"]),
            package("asterpath-agent", &[], &[]),
            package("asterpath-tools", &[], &[]),
        ];
        let allow = [
            allowed("asterpath", Role::App, &["*"]),
            allowed("asterpath-agent", Role::Module, &[]),
            allowed("asterpath-tools", Role::Module, &[]),
        ];

        assert!(check(&packages, &allow).is_empty());
    }

    #[test]
    fn repository_allowlist_matches_the_spec_table() {
        let text = include_str!("../../deps-allowlist.toml");
        let file: AllowlistFile = toml::from_str(text).unwrap();

        let roster: Vec<(&str, Role)> = vec![
            ("asterpath-core", Role::Contract),
            ("asterpath-protocol", Role::Contract),
            ("asterpath-sandbox", Role::Contract),
            ("asterpath-sandbox-linux", Role::Module),
            ("asterpath-agent", Role::Module),
            ("asterpath-tools", Role::Module),
            ("asterpath-storage-sqlite", Role::Module),
            ("asterpath-runtime", Role::Module),
            ("asterpath-scheduler", Role::Module),
            ("asterpath-compaction", Role::Module),
            ("asterpath-provider-anthropic", Role::Module),
            ("asterpath-provider-openai", Role::Module),
            ("asterpath-tracing", Role::Module),
            ("asterpath-usage", Role::Module),
            ("asterpath-plugin-host", Role::Module),
            ("asterpath-skills", Role::Module),
            ("asterpath-mcp-fixture", Role::Tool),
            ("asterpath-testing", Role::Module),
            ("asterpath", Role::App),
            ("asterpath-cli", Role::App),
            ("asterpath-acceptance", Role::App),
            ("xtask", Role::Tool),
        ];

        assert_eq!(file.package.len(), roster.len());
        for (name, role) in roster {
            let entry = file
                .package
                .iter()
                .find(|entry| entry.name == name)
                .unwrap_or_else(|| panic!("{name} missing from allowlist"));
            assert_eq!(entry.role, role, "wrong role for {name}");
            match role {
                Role::Contract | Role::Module => assert!(
                    !entry.allow.iter().any(|allowed| allowed == "*"),
                    "{name} must list explicit dependencies"
                ),
                Role::App | Role::Tool => {}
            }
        }
    }
}
