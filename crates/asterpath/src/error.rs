//! Facade error sum. Each layer keeps its own enum; this only adds them up.

use asterpath_core::{ExecutionError, StorageError};
use asterpath_protocol::{AgentError, ProviderError, ToolError};

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum AsterError {
    #[error("configuration error: {message}")]
    Config { message: String },
    #[error(transparent)]
    Agent(#[from] AgentError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Tool(#[from] ToolError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_errors_are_part_of_the_facade_sum() {
        let error = AsterError::from(ToolError::Timeout {
            message: "slow".to_owned(),
        });
        assert!(matches!(error, AsterError::Tool(ToolError::Timeout { .. })));
        assert_eq!(error.to_string(), "timed out: slow");
    }
}
