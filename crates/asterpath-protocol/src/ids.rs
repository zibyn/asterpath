//! Opaque protocol identifiers and JSON text wrappers.

use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! string_newtype {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({:?})"), self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_newtype!(ProviderId);
string_newtype!(ModelId);
string_newtype!(ModelAlias);
string_newtype!(ToolName);

/// Provider JSON kept as original text; never parsed and re-serialized on replay.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsonString(String);

impl JsonString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for JsonString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JsonString({:?})", self.0)
    }
}

/// Arbitrary JSON value (signal payloads, tool output fields).
pub type JsonValue = serde_json::Value;

/// A JSON Schema document.
pub type JsonSchema = serde_json::Value;

/// Provider-original tool arguments.
pub type ToolArgs = JsonString;
