//! Token usage in the exclusive normalization: cache reads and writes are
//! counted separately from uncached input and never double-counted.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input: InputTokens,
    pub output: OutputTokens,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputTokens {
    pub uncached: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputTokens {
    /// Includes reasoning tokens.
    pub total: u64,
    /// Subset of `total`; never added on top.
    pub reasoning: u64,
}

impl Usage {
    pub fn input_total(&self) -> u64 {
        self.input.uncached + self.input.cache_read + self.input.cache_write
    }

    pub fn total(&self) -> u64 {
        self.input_total() + self.output.total
    }
}
