//! Typed decoding of the opaque core log envelope.
//!
//! Upcasting happens only while reading: the payload is walked up the version
//! chain in memory and then deserialized once. The log is never rewritten.

use crate::LogEntry;
use serde_json::Value;

use crate::event::SessionEvent;

/// Shape/semantic version of every persisted `SessionEvent` payload graph.
/// Bump for any persisted shape change, including new optional fields.
pub const LOG_SCHEMA_VERSION: u16 = 2;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LogDecodeError {
    #[error("log schema version {found} is newer than supported version {supported}")]
    UnsupportedVersion { found: u16, supported: u16 },
    #[error("cannot upcast log payload from version {from} to {to}: {detail}")]
    Upcast { from: u16, to: u16, detail: String },
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
    if schema_version > LOG_SCHEMA_VERSION {
        return Err(LogDecodeError::UnsupportedVersion {
            found: schema_version,
            supported: LOG_SCHEMA_VERSION,
        });
    }

    let mut value = payload.clone();
    let mut version = schema_version;
    while version < LOG_SCHEMA_VERSION {
        value = upcast(version, value)?;
        version += 1;
    }

    serde_json::from_value(value).map_err(|error| LogDecodeError::Decode {
        detail: error.to_string(),
    })
}

/// Pure, stepwise payload upcasters; one arm per version bump.
fn upcast(from: u16, mut payload: Value) -> Result<Value, LogDecodeError> {
    match from {
        // v2 (#107): `ModelCallAborted` gained `error`; v1 never recorded one.
        1 => {
            if let Some(aborted) = payload
                .get_mut("ModelCallAborted")
                .and_then(Value::as_object_mut)
            {
                aborted.insert("error".to_owned(), Value::Null);
            }
            Ok(payload)
        }
        _ => Err(LogDecodeError::Upcast {
            from,
            to: from + 1,
            detail: "no upcaster for this schema version".to_owned(),
        }),
    }
}
