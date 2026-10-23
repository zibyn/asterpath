use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

fn parse_hex(value: &str) -> Result<[u8; 16], String> {
    let digits = value.as_bytes();
    if digits.len() != 32 {
        return Err(format!("expected 32 hex characters, got {}", digits.len()));
    }
    let mut bytes = [0u8; 16];
    for (index, pair) in digits.as_chunks::<2>().0.iter().enumerate() {
        let high = hex_digit(pair[0]).ok_or_else(|| format!("invalid hex at byte {index}"))?;
        let low = hex_digit(pair[1]).ok_or_else(|| format!("invalid hex at byte {index}"))?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 16]);

        impl $name {
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            pub fn to_hex(self) -> String {
                self.0.iter().map(|byte| format!("{byte:02x}")).collect()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.to_hex())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.to_hex())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                parse_hex(&value).map(Self).map_err(D::Error::custom)
            }
        }
    };
}

opaque_id!(SessionId);
opaque_id!(TurnId);
opaque_id!(StepId);
opaque_id!(InputId);
opaque_id!(ToolCallId);
opaque_id!(SignalId);

/// Position of a committed log record within its session stream.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogId(i64);

impl LogId {
    pub const fn from_i64(value: i64) -> Self {
        Self(value)
    }

    pub const fn as_i64(self) -> i64 {
        self.0
    }
}

impl fmt::Debug for LogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogId({})", self.0)
    }
}

impl Serialize for LogId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for LogId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        i64::deserialize(deserializer).map(Self)
    }
}

/// Inclusive range of committed log records, used as a compaction origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogIdRange {
    pub first: LogId,
    pub last: LogId,
}
