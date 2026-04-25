use thiserror::Error;

#[derive(Debug, Error)]
pub enum McpError {
    #[error("transport I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("timeout waiting for response (id={id}, method={method})")]
    Timeout { id: u64, method: String },
    #[error("connection lost")]
    ConnectionLost,
    #[error("server error: code={code} message={message}")]
    ServerError { code: i64, message: String },
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("needs auth")]
    NeedsAuth,
    #[error("cancelled")]
    Cancelled,
    #[error("{0}")]
    Other(String),
}

impl McpError {
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}
