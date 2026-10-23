//! Shared tool and MCP server name rules.
//!
//! In-process tools use bare names; external tools are exposed as
//! `<server_id>__<sanitized_tool_name>` and `__` is reserved for that namespace.

use crate::ids::ToolName;

/// Why a tool or MCP server name was rejected.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ToolNameError {
    #[error("tool name must be 1-64 characters of [A-Za-z0-9_-]")]
    InvalidToolName,
    #[error("tool name must not contain `__`")]
    ReservedSeparator,
    #[error("tool name exceeds 64 characters")]
    TooLong,
    #[error("MCP server id must be 1-32 characters of [a-z0-9-]")]
    InvalidServerId,
}

fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

fn is_name_char(ch: char) -> bool {
    ch.is_ascii() && is_name_byte(ch as u8)
}

/// Validates a bare, in-process tool name: `[A-Za-z0-9_-]{1,64}` without `__`.
pub fn validate_tool_name(name: &str) -> Result<(), ToolNameError> {
    if name.is_empty() || name.len() > 64 || !name.bytes().all(is_name_byte) {
        return Err(ToolNameError::InvalidToolName);
    }
    if name.contains("__") {
        return Err(ToolNameError::ReservedSeparator);
    }
    Ok(())
}

/// Validates an externally registered composed name: `[A-Za-z0-9_-]{1,64}`.
/// `__` is allowed because asterpath-mcp composes `<server_id>__<tool>`.
pub fn validate_external_tool_name(name: &str) -> Result<(), ToolNameError> {
    if name.is_empty() || name.len() > 64 || !name.bytes().all(is_name_byte) {
        return Err(ToolNameError::InvalidToolName);
    }
    Ok(())
}

/// Validates an MCP server id: `[a-z0-9-]{1,32}`.
pub fn validate_server_id(id: &str) -> Result<(), ToolNameError> {
    let valid = !id.is_empty()
        && id.len() <= 32
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(ToolNameError::InvalidServerId)
    }
}

/// Replaces every character outside `[A-Za-z0-9_-]` with `_`, keeping case.
pub fn sanitize_tool_name(raw: &str) -> String {
    raw.chars()
        .map(|ch| if is_name_char(ch) { ch } else { '_' })
        .collect()
}

/// Composes the externally visible name `<server_id>__<sanitized raw name>`.
pub fn external_tool_name(server_id: &str, raw: &str) -> Result<ToolName, ToolNameError> {
    validate_server_id(server_id)?;
    let sanitized = sanitize_tool_name(raw);
    if sanitized.is_empty() {
        return Err(ToolNameError::InvalidToolName);
    }
    let name = format!("{server_id}__{sanitized}");
    if name.len() > 64 {
        return Err(ToolNameError::TooLong);
    }
    Ok(ToolName::new(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bare_names() {
        for name in [
            "t",
            "tool",
            "read_file",
            "shell-2",
            "A",
            "x".repeat(64).as_str(),
        ] {
            assert!(validate_tool_name(name).is_ok(), "{name}");
        }
    }

    #[test]
    fn rejects_bad_bare_names() {
        assert_eq!(validate_tool_name(""), Err(ToolNameError::InvalidToolName));
        assert_eq!(
            validate_tool_name("a b"),
            Err(ToolNameError::InvalidToolName)
        );
        assert_eq!(
            validate_tool_name("a.b"),
            Err(ToolNameError::InvalidToolName)
        );
        assert_eq!(validate_tool_name("é"), Err(ToolNameError::InvalidToolName));
        assert_eq!(
            validate_tool_name(&"x".repeat(65)),
            Err(ToolNameError::InvalidToolName)
        );
        assert_eq!(
            validate_tool_name("a__b"),
            Err(ToolNameError::ReservedSeparator)
        );
    }

    #[test]
    fn validates_server_ids() {
        for id in ["a", "weather-2", "0", &"a".repeat(32)] {
            assert!(validate_server_id(id).is_ok(), "{id}");
        }
        for id in [
            "",
            "Weather",
            "under_score",
            "space id",
            &"a".repeat(33),
            "dot.id",
        ] {
            assert_eq!(
                validate_server_id(id),
                Err(ToolNameError::InvalidServerId),
                "{id}"
            );
        }
    }

    #[test]
    fn sanitize_replaces_every_invalid_character() {
        assert_eq!(sanitize_tool_name("a.b/c d"), "a_b_c_d");
        assert_eq!(sanitize_tool_name("A_b-2"), "A_b-2");
        assert_eq!(sanitize_tool_name("é"), "_");
        assert_eq!(sanitize_tool_name(""), "");
    }

    #[test]
    fn composes_external_names() {
        assert_eq!(
            external_tool_name("weather", "get.forecast").unwrap(),
            ToolName::new("weather__get_forecast")
        );
        assert_eq!(
            external_tool_name("weather", "ünicode").unwrap(),
            ToolName::new("weather___nicode")
        );
    }

    #[test]
    fn external_names_allow_the_separator_but_keep_the_charset() {
        assert!(validate_external_tool_name("plugin__tool").is_ok());
        assert!(validate_external_tool_name(&"x".repeat(64)).is_ok());
        for name in ["", "a.b", "a b", "é", "a__b\n", &"x".repeat(65)] {
            assert_eq!(
                validate_external_tool_name(name),
                Err(ToolNameError::InvalidToolName),
                "{name}"
            );
        }
    }

    #[test]
    fn rejects_empty_sanitized_component_and_oversize_names() {
        assert_eq!(
            external_tool_name("weather", ""),
            Err(ToolNameError::InvalidToolName)
        );
        let raw = "x".repeat(59);
        assert_eq!(
            external_tool_name("0123456789", &raw),
            Err(ToolNameError::TooLong)
        );
        assert!(external_tool_name("0123456789", &"x".repeat(49)).is_ok());
    }
}
