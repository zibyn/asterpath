//! Configurable `SandboxProvider` double that records every request it receives.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use asterpath_sandbox::{
    ConfinedSpawn, Enforcement, SandboxAspect, SandboxError, SandboxPolicy, SandboxProvider,
    SpawnRequest,
};

/// Owned snapshot of a received [`SpawnRequest`], kept for assertions.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedSpawnRequest {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<OsString, OsString>,
    pub policy: SandboxPolicy,
}

impl RecordedSpawnRequest {
    fn from_request(request: &SpawnRequest<'_>) -> Self {
        Self {
            program: request.program.to_os_string(),
            args: request.args.to_vec(),
            cwd: request.cwd.map(Path::to_path_buf),
            env: request.env.clone(),
            policy: request.policy.clone(),
        }
    }
}

enum Outcome {
    Full,
    Partial { unenforced: Vec<SandboxAspect> },
    Unsupported { reason: String },
    Refused { reason: String },
    Setup { message: String },
}

/// Returns a configured outcome for every call and records each request.
pub struct FakeSandboxProvider {
    outcome: Outcome,
    recorded: Mutex<Vec<RecordedSpawnRequest>>,
}

impl FakeSandboxProvider {
    pub fn full() -> Self {
        Self::with_outcome(Outcome::Full)
    }

    pub fn partial(unenforced: Vec<SandboxAspect>) -> Self {
        Self::with_outcome(Outcome::Partial { unenforced })
    }

    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::with_outcome(Outcome::Unsupported {
            reason: reason.into(),
        })
    }

    pub fn refused(reason: impl Into<String>) -> Self {
        Self::with_outcome(Outcome::Refused {
            reason: reason.into(),
        })
    }

    pub fn setup(message: impl Into<String>) -> Self {
        Self::with_outcome(Outcome::Setup {
            message: message.into(),
        })
    }

    fn with_outcome(outcome: Outcome) -> Self {
        Self {
            outcome,
            recorded: Mutex::new(Vec::new()),
        }
    }

    /// Every request received so far, in call order.
    pub fn recorded(&self) -> Vec<RecordedSpawnRequest> {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl SandboxProvider for FakeSandboxProvider {
    fn confine(&self, request: &SpawnRequest<'_>) -> Result<ConfinedSpawn, SandboxError> {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(RecordedSpawnRequest::from_request(request));

        match &self.outcome {
            Outcome::Full => Ok(pass_through(request, Enforcement::Full)),
            Outcome::Partial { unenforced } => Ok(pass_through(
                request,
                Enforcement::Partial {
                    unenforced: unenforced.clone(),
                },
            )),
            Outcome::Unsupported { reason } => Err(SandboxError::Unsupported {
                reason: reason.clone(),
            }),
            Outcome::Refused { reason } => Err(SandboxError::Refused {
                reason: reason.clone(),
            }),
            Outcome::Setup { message } => Err(SandboxError::Setup {
                message: message.clone(),
            }),
        }
    }
}

fn pass_through(request: &SpawnRequest<'_>, enforcement: Enforcement) -> ConfinedSpawn {
    ConfinedSpawn::new(
        request.program.to_os_string(),
        request.args.to_vec(),
        request.cwd.map(Path::to_path_buf),
        request.env.clone(),
        enforcement,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::{OsStr, OsString};
    use std::path::{Path, PathBuf};

    use asterpath_sandbox::{
        Enforcement, SandboxAspect, SandboxError, SandboxPolicy, SandboxProvider, SpawnRequest,
    };

    use super::FakeSandboxProvider;

    fn env() -> BTreeMap<OsString, OsString> {
        BTreeMap::from([(OsString::from("A"), OsString::from("1"))])
    }

    fn args() -> Vec<OsString> {
        vec![OsString::from("--flag")]
    }

    fn request<'a>(
        program: &'a OsStr,
        args: &'a [OsString],
        cwd: Option<&'a Path>,
        env: &'a BTreeMap<OsString, OsString>,
        policy: &'a SandboxPolicy,
    ) -> SpawnRequest<'a> {
        SpawnRequest::new(program, args, cwd, env, policy)
    }

    #[test]
    fn full_reports_full_enforcement_and_passes_the_request_through() {
        let env = env();
        let args = args();
        let policy = SandboxPolicy::ReadOnly;
        let provider = FakeSandboxProvider::full();

        let confined = provider
            .confine(&request(
                OsStr::new("/usr/bin/server"),
                &args,
                Some(Path::new("/work")),
                &env,
                &policy,
            ))
            .unwrap();

        assert_eq!(confined.program, OsString::from("/usr/bin/server"));
        assert_eq!(confined.args, vec![OsString::from("--flag")]);
        assert_eq!(confined.cwd, Some(PathBuf::from("/work")));
        assert_eq!(confined.env, env);
        assert_eq!(confined.enforcement, Enforcement::Full);
    }

    #[test]
    fn records_every_request_with_all_fields() {
        let env = env();
        let args = args();
        let read_only = SandboxPolicy::ReadOnly;
        let workspace = SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![PathBuf::from("/w")],
        };
        let provider = FakeSandboxProvider::full();

        provider
            .confine(&request(
                OsStr::new("/usr/bin/server"),
                &args,
                Some(Path::new("/work")),
                &env,
                &read_only,
            ))
            .unwrap();
        provider
            .confine(&request(
                OsStr::new("/usr/bin/other"),
                &args,
                None,
                &env,
                &workspace,
            ))
            .unwrap();

        let recorded = provider.recorded();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].program, OsString::from("/usr/bin/server"));
        assert_eq!(recorded[0].args, vec![OsString::from("--flag")]);
        assert_eq!(recorded[0].cwd, Some(PathBuf::from("/work")));
        assert_eq!(recorded[0].env, env);
        assert_eq!(recorded[0].policy, SandboxPolicy::ReadOnly);
        assert_eq!(recorded[1].program, OsString::from("/usr/bin/other"));
        assert_eq!(recorded[1].cwd, None);
        assert_eq!(recorded[1].policy, workspace);
    }

    #[test]
    fn recorded_requests_are_owned_snapshots() {
        let mut env_map = env();
        let mut args = args();
        let policy = SandboxPolicy::ReadOnly;
        let provider = FakeSandboxProvider::full();

        {
            let request = request(
                OsStr::new("/usr/bin/server"),
                &args,
                Some(Path::new("/work")),
                &env_map,
                &policy,
            );
            provider.confine(&request).unwrap();
        }
        args.push(OsString::from("--later"));
        env_map.insert(OsString::from("B"), OsString::from("2"));

        let recorded = provider.recorded();
        assert_eq!(recorded[0].args, vec![OsString::from("--flag")]);
        assert_eq!(recorded[0].env, env());
    }

    #[test]
    fn s2_unsupported_fails_closed_and_records_the_attempt() {
        let env = env();
        let args = args();
        let policy = SandboxPolicy::ReadOnly;
        let provider = FakeSandboxProvider::unsupported("no backend");

        let error = provider
            .confine(&request(
                OsStr::new("/usr/bin/server"),
                &args,
                None,
                &env,
                &policy,
            ))
            .unwrap_err();

        match &error {
            SandboxError::Unsupported { reason } => assert_eq!(reason, "no backend"),
            other => panic!("expected Unsupported, got {other:?}"),
        }
        assert!(error.to_string().starts_with("sandbox:"));
        assert_eq!(provider.recorded().len(), 1);
    }

    #[test]
    fn s3_partial_reports_unenforced_aspects() {
        let env = env();
        let args = args();
        let policy = SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![PathBuf::from("/w")],
        };
        let provider = FakeSandboxProvider::partial(vec![SandboxAspect::FilesystemWrite]);

        let confined = provider
            .confine(&request(
                OsStr::new("/usr/bin/server"),
                &args,
                None,
                &env,
                &policy,
            ))
            .unwrap();

        assert_eq!(
            confined.enforcement,
            Enforcement::Partial {
                unenforced: vec![SandboxAspect::FilesystemWrite],
            }
        );
        assert_eq!(confined.program, OsString::from("/usr/bin/server"));
    }

    #[test]
    fn s4_refused_returns_a_denial_with_a_sandbox_prefix() {
        let env = env();
        let args = args();
        let policy = SandboxPolicy::ReadOnly;
        let provider = FakeSandboxProvider::refused("policy denies spawn");

        let error = provider
            .confine(&request(
                OsStr::new("/usr/bin/server"),
                &args,
                None,
                &env,
                &policy,
            ))
            .unwrap_err();

        match &error {
            SandboxError::Refused { reason } => assert_eq!(reason, "policy denies spawn"),
            other => panic!("expected Refused, got {other:?}"),
        }
        assert!(error.to_string().starts_with("sandbox:"));
        assert_eq!(provider.recorded().len(), 1);
    }

    #[test]
    fn setup_returns_a_setup_error() {
        let env = env();
        let args = args();
        let policy = SandboxPolicy::FullAccess;
        let provider = FakeSandboxProvider::setup("cannot create namespace");

        let error = provider
            .confine(&request(
                OsStr::new("/usr/bin/server"),
                &args,
                None,
                &env,
                &policy,
            ))
            .unwrap_err();

        match &error {
            SandboxError::Setup { message } => assert_eq!(message, "cannot create namespace"),
            other => panic!("expected Setup, got {other:?}"),
        }
        assert!(error.to_string().starts_with("sandbox:"));
    }
}
