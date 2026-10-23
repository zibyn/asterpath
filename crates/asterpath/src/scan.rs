//! Whole-log reads: scan a session and decode every record, with the error
//! mapping each engine part needs.

use asterpath_protocol::{
    AgentError, ExecutionError, LogEntry, SessionEvent, SessionId, SessionStore, StorageError,
    TraceError, decode_entry, scan_all,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ScanError {
    #[error("{source}")]
    Storage {
        session: SessionId,
        source: StorageError,
    },
    #[error("session {session:?} has an undecodable record: {detail}")]
    Decode { session: SessionId, detail: String },
}

/// Every committed record of `session`, decoded, in log order.
pub(crate) async fn scan_decoded(
    log: &dyn SessionStore,
    session: SessionId,
) -> Result<Vec<(LogEntry, SessionEvent)>, ScanError> {
    scan_all(log, session)
        .await
        .map_err(|source| ScanError::Storage { session, source })?
        .into_iter()
        .map(|entry| {
            let event = decode_entry(&entry).map_err(|error| ScanError::Decode {
                session,
                detail: error.to_string(),
            })?;
            Ok((entry, event))
        })
        .collect()
}

pub(crate) fn backend(error: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::Backend(error.to_string())
}

impl From<ScanError> for AgentError {
    fn from(error: ScanError) -> Self {
        match error {
            ScanError::Storage { source, .. } => Self::Storage(source),
            ScanError::Decode { session, detail } => Self::Corrupt { session, detail },
        }
    }
}

impl From<ScanError> for ExecutionError {
    fn from(error: ScanError) -> Self {
        backend(error)
    }
}

impl From<ScanError> for TraceError {
    fn from(error: ScanError) -> Self {
        match error {
            ScanError::Storage {
                session,
                source: StorageError::Corrupt { detail, .. },
            } => Self::Corrupt { session, detail },
            ScanError::Storage { source, .. } => Self::Backend(source.to_string()),
            ScanError::Decode { session, detail } => Self::Corrupt { session, detail },
        }
    }
}

impl From<ScanError> for crate::scheduler::WaitError {
    fn from(error: ScanError) -> Self {
        Self::Execution(error.into())
    }
}
