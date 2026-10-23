use std::fmt;
use std::fs;

use cargo_metadata::{DependencyKind, MetadataCommand};
use serde::Deserialize;

/// Role rules (ADR-0026): the protocol crate depends on no member; adapters
/// depend only on protocol and other adapters; the engine only on protocol and
/// the adapters it re-exports or feature-gates. Apps and tools are bounded only
/// by their `allow` list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Protocol,
    Adapter,
    Engine,
    App,
    Tool,
}

impl Role {
    /// Roles a crate of this role may depend on; `None` means any.
    fn may_depend_on(self) -> Option<&'static [Role]> {
        match self {
            Role::Protocol => Some(&[]),
            Role::Adapter | Role::Engine => Some(&[Role::Protocol, Role::Adapter]),
            Role::App | Role::Tool => None,
        }
    }
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
    RoleDependency {
        from: String,
        to: String,
        role: Role,
    },
    ProtocolFeatures {
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
            Violation::RoleDependency { from, to, role } => write!(
                f,
                "{from}: its role must not depend on {to} (role {role:?})"
            ),
            Violation::ProtocolFeatures { package, features } => write!(
                f,
                "{package}: the protocol crate must not declare features ({})",
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

        if entry.role == Role::Protocol && !package.features.is_empty() {
            violations.push(Violation::ProtocolFeatures {
                package: package.name.clone(),
                features: package.features.clone(),
            });
        }

        for dependency in &package.normal_workspace_deps {
            if let (Some(permitted), Some(role)) = (entry.role.may_depend_on(), role_of(dependency))
                && !permitted.contains(&role)
            {
                violations.push(Violation::RoleDependency {
                    from: package.name.clone(),
                    to: dependency.clone(),
                    role,
                });
                continue;
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
    fn accepts_a_workspace_that_follows_the_role_rules() {
        let packages = [
            package("asterpath-protocol", &[], &[]),
            package("asterpath-http", &[], &["asterpath-protocol"]),
            package(
                "asterpath-provider-openai",
                &[],
                &["asterpath-protocol", "asterpath-http"],
            ),
            package(
                "asterpath",
                &["openai"],
                &["asterpath-protocol", "asterpath-provider-openai"],
            ),
            package("asterpath-cli", &[], &["asterpath", "asterpath-protocol"]),
        ];
        let allow = [
            allowed("asterpath-protocol", Role::Protocol, &[]),
            allowed("asterpath-http", Role::Adapter, &["asterpath-protocol"]),
            allowed(
                "asterpath-provider-openai",
                Role::Adapter,
                &["asterpath-protocol", "asterpath-http"],
            ),
            allowed(
                "asterpath",
                Role::Engine,
                &["asterpath-protocol", "asterpath-provider-openai"],
            ),
            allowed("asterpath-cli", Role::App, &["*"]),
        ];

        assert!(check(&packages, &allow).is_empty());
    }

    #[test]
    fn rejects_any_workspace_dependency_of_the_protocol_crate() {
        let packages = [
            package("asterpath-protocol", &[], &["asterpath-http"]),
            package("asterpath-http", &[], &[]),
        ];
        let allow = [
            allowed("asterpath-protocol", Role::Protocol, &["asterpath-http"]),
            allowed("asterpath-http", Role::Adapter, &[]),
        ];

        assert_eq!(
            check(&packages, &allow),
            [Violation::RoleDependency {
                from: "asterpath-protocol".to_string(),
                to: "asterpath-http".to_string(),
                role: Role::Adapter,
            }]
        );
    }

    #[test]
    fn rejects_an_adapter_depending_on_the_engine_or_an_app() {
        let packages = [
            package("asterpath-skills", &[], &["asterpath", "asterpath-cli"]),
            package("asterpath", &[], &[]),
            package("asterpath-cli", &[], &[]),
        ];
        let allow = [
            allowed(
                "asterpath-skills",
                Role::Adapter,
                &["asterpath", "asterpath-cli"],
            ),
            allowed("asterpath", Role::Engine, &[]),
            allowed("asterpath-cli", Role::App, &["*"]),
        ];

        assert_eq!(
            check(&packages, &allow),
            [
                Violation::RoleDependency {
                    from: "asterpath-skills".to_string(),
                    to: "asterpath".to_string(),
                    role: Role::Engine,
                },
                Violation::RoleDependency {
                    from: "asterpath-skills".to_string(),
                    to: "asterpath-cli".to_string(),
                    role: Role::App,
                },
            ]
        );
    }

    #[test]
    fn rejects_an_engine_dependency_on_an_app() {
        let packages = [
            package("asterpath", &[], &["asterpath-cli"]),
            package("asterpath-cli", &[], &[]),
        ];
        let allow = [
            allowed("asterpath", Role::Engine, &["asterpath-cli"]),
            allowed("asterpath-cli", Role::App, &["*"]),
        ];

        assert_eq!(
            check(&packages, &allow),
            [Violation::RoleDependency {
                from: "asterpath".to_string(),
                to: "asterpath-cli".to_string(),
                role: Role::App,
            }]
        );
    }

    #[test]
    fn rejects_a_workspace_dependency_outside_the_allowlist() {
        let packages = [
            package("asterpath-skills", &[], &["asterpath-http"]),
            package("asterpath-http", &[], &[]),
        ];
        let allow = [
            allowed("asterpath-skills", Role::Adapter, &["asterpath-protocol"]),
            allowed("asterpath-http", Role::Adapter, &[]),
        ];

        assert_eq!(
            check(&packages, &allow),
            [Violation::DependencyNotAllowed {
                package: "asterpath-skills".to_string(),
                dependency: "asterpath-http".to_string(),
            }]
        );
    }

    #[test]
    fn rejects_features_on_the_protocol_crate() {
        let packages = [package("asterpath-protocol", &["std"], &[])];
        let allow = [allowed("asterpath-protocol", Role::Protocol, &[])];

        assert_eq!(
            check(&packages, &allow),
            [Violation::ProtocolFeatures {
                package: "asterpath-protocol".to_string(),
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
            package("asterpath-cli", &[], &["asterpath", "asterpath-skills"]),
            package("asterpath", &[], &[]),
            package("asterpath-skills", &[], &[]),
        ];
        let allow = [
            allowed("asterpath-cli", Role::App, &["*"]),
            allowed("asterpath", Role::Engine, &[]),
            allowed("asterpath-skills", Role::Adapter, &[]),
        ];

        assert!(check(&packages, &allow).is_empty());
    }

    #[test]
    fn repository_allowlist_matches_the_spec_table() {
        let text = include_str!("../../deps-allowlist.toml");
        let file: AllowlistFile = toml::from_str(text).unwrap();

        let roster: Vec<(&str, Role)> = vec![
            ("asterpath-protocol", Role::Protocol),
            ("asterpath-http", Role::Adapter),
            ("asterpath-sandbox", Role::Adapter),
            ("asterpath-sandbox-linux", Role::Adapter),
            ("asterpath-storage-sqlite", Role::Adapter),
            ("asterpath-provider-anthropic", Role::Adapter),
            ("asterpath-provider-openai", Role::Adapter),
            ("asterpath-plugin-host", Role::Adapter),
            ("asterpath-skills", Role::Adapter),
            ("asterpath-testing", Role::Adapter),
            ("asterpath", Role::Engine),
            ("asterpath-cli", Role::App),
            ("asterpath-acceptance", Role::App),
            ("asterpath-mcp-fixture", Role::Tool),
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
                Role::Protocol | Role::Adapter | Role::Engine => assert!(
                    !entry.allow.iter().any(|allowed| allowed == "*"),
                    "{name} must list explicit dependencies"
                ),
                Role::App | Role::Tool => {}
            }
        }
    }
}
