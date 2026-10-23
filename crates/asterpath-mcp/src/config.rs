//! MCP server configuration (`mcp.md` §2, ADR-0027).
//!
//! The embedder owns the data; this crate only defines the shape. Unknown
//! fields are rejected. Secrets in `env` and HTTP `headers` are redacted from
//! `Debug` output.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use asterpath_protocol::{ToolNameError, validate_server_id};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

pub const DEFAULT_STARTUP: Duration = Duration::from_secs(30);
pub const DEFAULT_CALL: Duration = Duration::from_secs(120);

/// `[a-z0-9-]{1,32}`; deserialization rejects anything else.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ServerId(String);

impl ServerId {
    pub fn new(value: impl Into<String>) -> Result<Self, ToolNameError> {
        let value = value.into();
        validate_server_id(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServerId({:?})", self.0)
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ServerId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

/// Transport configuration. `Debug` never prints env or header values.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum McpTransport {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        cwd: Option<PathBuf>,
    },
    Http {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
}

struct RedactedMap<'a>(&'a BTreeMap<String, String>);

impl fmt::Debug for RedactedMap<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for key in self.0.keys() {
            map.entry(key, &"<redacted>");
        }
        map.finish()
    }
}

impl fmt::Debug for McpTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdio {
                command,
                args,
                env,
                cwd,
            } => f
                .debug_struct("Stdio")
                .field("command", command)
                .field("args", args)
                .field("env", &RedactedMap(env))
                .field("cwd", cwd)
                .finish(),
            Self::Http { url, headers } => f
                .debug_struct("Http")
                .field("url", url)
                .field("headers", &RedactedMap(headers))
                .finish(),
        }
    }
}

/// Timeouts are millisecond fields in the config and `Duration` in Rust.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpTimeouts {
    #[serde(
        default,
        rename = "startup_ms",
        with = "millis",
        skip_serializing_if = "Option::is_none"
    )]
    pub startup: Option<Duration>,
    #[serde(
        default,
        rename = "call_ms",
        with = "millis",
        skip_serializing_if = "Option::is_none"
    )]
    pub call: Option<Duration>,
}

impl McpTimeouts {
    pub fn startup_timeout(&self) -> Duration {
        self.startup.unwrap_or(DEFAULT_STARTUP)
    }

    pub fn call_timeout(&self) -> Duration {
        self.call.unwrap_or(DEFAULT_CALL)
    }
}

mod millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(duration) => serializer.serialize_u64(duration.as_millis() as u64),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        match Option::<i64>::deserialize(deserializer)? {
            None => Ok(None),
            Some(millis) if millis > 0 => Ok(Some(Duration::from_millis(millis as u64))),
            Some(_) => Err(serde::de::Error::custom("timeouts must be positive")),
        }
    }
}

/// One MCP server the embedder wants loaded. Unknown fields are rejected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    pub id: ServerId,
    pub transport: McpTransport,
    #[serde(default)]
    pub required: bool,
    pub protocol_versions: Vec<String>,
    #[serde(default)]
    pub timeouts: McpTimeouts,
}
