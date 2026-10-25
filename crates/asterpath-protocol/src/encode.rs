//! Typed encoding of `SessionEvent` into the opaque core log envelope.
//!
//! Protocol owns both directions so core and storage stay payload-agnostic.

use crate::{DedupKey, NewEntry, SessionId};

use crate::decode::LOG_SCHEMA_VERSION;
use crate::event::SessionEvent;

pub fn encode_entry(
    session: SessionId,
    dedup_key: Option<DedupKey>,
    event: &SessionEvent,
) -> Result<NewEntry, serde_json::Error> {
    Ok(NewEntry {
        session,
        schema_version: LOG_SCHEMA_VERSION,
        dedup_key,
        owner: None,
        payload: serde_json::to_value(event)?,
    })
}
