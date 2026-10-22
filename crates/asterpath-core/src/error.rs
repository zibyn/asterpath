use crate::ids::{LogId, SessionId};

/// Failures of module host startup and shutdown.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LifecycleError {
    #[error("module dependency cycle: {path:?}")]
    Cycle { path: Vec<String> },
    #[error("module `{module}` requires missing service `{service}`")]
    MissingDependency { module: String, service: String },
    #[error("service `{service}` is already provided")]
    DuplicateProvider { service: String },
    #[error("module `{module}` failed to initialize: {source}")]
    InitFailed {
        module: String,
        #[source]
        source: Box<LifecycleError>,
    },
}

/// Failures of the event log / storage backends.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    #[error("storage is already locked by another writer")]
    WriterBusy,
    #[error("storage schema version {found} is newer than supported version {supported}")]
    SchemaTooNew { found: i64, supported: i64 },
    #[error("invalid log entry: {0}")]
    InvalidEntry(String),
    #[error("corrupt storage: {detail}")]
    Corrupt {
        session: Option<SessionId>,
        detail: String,
    },
    #[error("storage backend error: {0}")]
    Backend(String),
}

/// Failures of the durable executor seam.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExecutionError {
    #[error("session {session:?} not found")]
    NotFound { session: SessionId },
    #[error("execution backend error: {0}")]
    Backend(String),
    #[error("wait was cancelled")]
    Cancelled,
}

/// Failures of the optional session retirement capability.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RetirementError {
    #[error("session not found")]
    NotFound,
    #[error("session is not idle")]
    NotIdle,
    #[error("frontier moved: expected {expected:?}, actual {actual:?}")]
    FrontierMoved {
        expected: LogId,
        actual: Option<LogId>,
    },
    #[error("session has live children")]
    HasChildren,
    #[error("parent session is not idle")]
    ParentNotIdle,
    #[error(transparent)]
    Storage(#[from] StorageError),
}
