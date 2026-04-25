//! MCP (Model Context Protocol) client — pure protocol layer, DB-free.
//!
//! Wire types + JSON-RPC 2.0 framing + transports (stdio today) + a
//! high-level [`McpClient`] that does the initialize / list_tools / call_tool
//! dance. No knowledge of Tokimo DB, AppState, or AgentRunner.

pub mod client;
pub mod connection;
pub mod error;
pub mod protocol;
pub mod tool_name;
pub mod transport;
pub mod types;

pub use client::McpClient;
pub use connection::{McpConnection, McpConnectionState};
pub use error::McpError;
pub use types::{
    CallToolResult, ClientInfo, InitializeResult, McpAnnotations, McpContent, McpNotification,
    McpPrompt, McpResource, McpTool, ReadResourceResult,
};
