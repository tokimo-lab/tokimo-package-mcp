use async_trait::async_trait;
use serde_json::Value;

use crate::error::McpError;

pub mod http;
pub mod stdio;

/// Wire-level transport: sends/receives already-serialised JSON values.
#[async_trait]
pub trait McpTransport: Send + Sync {
    /// Send a single JSON-RPC message (request / notification).
    async fn send(&self, msg: Value) -> Result<(), McpError>;

    /// Receive the next JSON-RPC message. Returns `None` when the transport
    /// is closed.
    async fn recv(&self) -> Option<Value>;

    /// Close / kill the transport. Safe to call multiple times.
    async fn close(&self);
}
