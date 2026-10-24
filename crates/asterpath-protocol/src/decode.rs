//! Typed decoding of the opaque core log envelope.
//!
//! Pre-release there are no upcasters: a payload at any other schema version
//! is rejected and the development database is rebuilt (ADR-0004). The log is
//! never rewritten.

use crate::LogEntry;
use serde_json::Value;

use crate::event::SessionEvent;

/// Shape/semantic version of every persisted `SessionEvent` payload graph.
/// Bump for any persisted shape change, including new optional fields.
pub const LOG_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LogDecodeError {
    #[error("log schema version {found} is not the supported version {supported}")]
    UnsupportedVersion { found: u16, supported: u16 },
    #[error("cannot decode log payload: {detail}")]
    Decode { detail: String },
}

/// Decode one committed entry into its typed event.
pub fn decode_entry(entry: &LogEntry) -> Result<SessionEvent, LogDecodeError> {
    decode_payload(&entry.payload, entry.schema_version)
}

/// Decode a payload at a known schema version. Unknown fields are ignored;
/// unknown variants and missing required fields are hard errors.
pub fn decode_payload(
    payload: &Value,
    schema_version: u16,
) -> Result<SessionEvent, LogDecodeError> {
    if schema_version != LOG_SCHEMA_VERSION {
        return Err(LogDecodeError::UnsupportedVersion {
            found: schema_version,
            supported: LOG_SCHEMA_VERSION,
        });
    }

    serde_json::from_value(payload.clone()).map_err(|error| LogDecodeError::Decode {
        detail: error.to_string(),
    })
}
