//! [`McpClient`] — high-level MCP client on top of any [`McpTransport`].
//!
//! Owns:
//! - a JSON-RPC id allocator (`AtomicU64`)
//! - a `HashMap<id, oneshot::Sender>` for pending requests
//! - a broadcast channel for server-initiated notifications
//!
//! One background task reads from the transport, demultiplexes responses
//! back to their `oneshot::Sender`, and forwards notifications.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::timeout;

use crate::error::McpError;
use crate::protocol::{JSON_RPC_VERSION, JsonRpcRequest, JsonRpcResponse, MCP_PROTOCOL_VERSION};
use crate::transport::McpTransport;
use crate::types::{
    CallToolResult, ClientInfo, InitializeResult, ListPromptsResult, ListResourcesResult, ListToolsResult,
    McpNotification, McpPrompt, McpResource, McpTool, ReadResourceResult,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_mins(1);

type PendingMap = HashMap<u64, oneshot::Sender<Result<Value, McpError>>>;

pub struct McpClient {
    transport: Arc<dyn McpTransport>,
    next_id: AtomicU64,
    pending: Arc<Mutex<PendingMap>>,
    notification_tx: broadcast::Sender<McpNotification>,
    log_target: String,
}

impl McpClient {
    pub fn new(transport: Arc<dyn McpTransport>, log_target: impl Into<String>) -> Arc<Self> {
        let (tx, _) = broadcast::channel(64);
        let client = Arc::new(Self {
            transport: transport.clone(),
            next_id: AtomicU64::new(1),
            pending: Arc::new(Mutex::new(HashMap::new())),
            notification_tx: tx,
            log_target: log_target.into(),
        });
        let weak = Arc::downgrade(&client);
        tokio::spawn(async move {
            while let Some(this) = weak.upgrade() {
                let Some(msg) = this.transport.recv().await else {
                    tracing::info!(target: "mcp::client", target_log = %this.log_target, "transport closed");
                    this.fail_all_pending(McpError::ConnectionLost).await;
                    break;
                };
                this.dispatch(msg).await;
            }
        });
        client
    }

    pub fn subscribe_notifications(&self) -> broadcast::Receiver<McpNotification> {
        self.notification_tx.subscribe()
    }

    async fn fail_all_pending(&self, _err: McpError) {
        let mut pending = self.pending.lock().await;
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(McpError::ConnectionLost));
        }
    }

    async fn dispatch(&self, raw: Value) {
        let Ok(msg) = serde_json::from_value::<JsonRpcResponse>(raw.clone()) else {
            tracing::warn!(target: "mcp::client", target_log = %self.log_target, "malformed JSON-RPC frame: {raw}");
            return;
        };

        match (msg.id, msg.result, msg.error, msg.method) {
            // response to our request
            (Some(id), result, error, _) => {
                let mut pending = self.pending.lock().await;
                if let Some(tx) = pending.remove(&id) {
                    let payload = match error {
                        Some(e) => Err(McpError::ServerError {
                            code: e.code,
                            message: e.message,
                        }),
                        None => Ok(result.unwrap_or(Value::Null)),
                    };
                    let _ = tx.send(payload);
                } else {
                    tracing::debug!(target: "mcp::client", target_log = %self.log_target, "orphan response id={id}");
                }
            }
            // server-initiated notification (no id, has method)
            (None, _, _, Some(method)) => {
                let params = msg.params.unwrap_or(Value::Null);
                let notif = match method.as_str() {
                    "notifications/tools/list_changed" => McpNotification::ToolsListChanged,
                    "notifications/resources/list_changed" => McpNotification::ResourcesListChanged,
                    "notifications/prompts/list_changed" => McpNotification::PromptsListChanged,
                    "notifications/progress" => McpNotification::Progress {
                        token: params.get("progressToken").cloned().unwrap_or(Value::Null),
                        progress: params.clone(),
                    },
                    "notifications/message" => McpNotification::LogMessage {
                        level: params
                            .get("level")
                            .and_then(Value::as_str)
                            .unwrap_or("info")
                            .to_string(),
                        data: params.get("data").cloned().unwrap_or(Value::Null),
                    },
                    _ => McpNotification::Other { method, params },
                };
                let _ = self.notification_tx.send(notif);
            }
            _ => {
                tracing::debug!(target: "mcp::client", target_log = %self.log_target, "ignored frame");
            }
        }
    }

    async fn send_request(&self, method: &str, params: Value) -> Result<Value, McpError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let req = JsonRpcRequest {
            jsonrpc: JSON_RPC_VERSION,
            id,
            method,
            params,
        };
        let wire = serde_json::to_value(&req)?;
        if let Err(e) = self.transport.send(wire).await {
            self.pending.lock().await.remove(&id);
            return Err(e);
        }

        match timeout(DEFAULT_TIMEOUT, rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(McpError::ConnectionLost),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(McpError::Timeout {
                    id,
                    method: method.to_string(),
                })
            }
        }
    }

    async fn send_notification(&self, method: &str, params: Value) -> Result<(), McpError> {
        let msg = json!({
            "jsonrpc": JSON_RPC_VERSION,
            "method": method,
            "params": params,
        });
        self.transport.send(msg).await
    }

    // ── High-level API ──────────────────────────────────────────────

    pub async fn initialize(&self, client_info: ClientInfo) -> Result<InitializeResult, McpError> {
        let params = json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {
                "roots": { "listChanged": false },
                "sampling": {}
            },
            "clientInfo": { "name": client_info.name, "version": client_info.version }
        });
        let raw = self.send_request("initialize", params).await?;
        let res: InitializeResult = serde_json::from_value(raw)?;
        // MCP requires `notifications/initialized` after handshake.
        self.send_notification("notifications/initialized", json!({})).await?;
        Ok(res)
    }

    pub async fn list_tools(&self) -> Result<Vec<McpTool>, McpError> {
        let raw = self.send_request("tools/list", json!({})).await?;
        let res: ListToolsResult = serde_json::from_value(raw)?;
        Ok(res.tools)
    }

    pub async fn list_resources(&self) -> Result<Vec<McpResource>, McpError> {
        let raw = self
            .send_request("resources/list", json!({}))
            .await
            .unwrap_or(Value::Null);
        let res: ListResourcesResult =
            serde_json::from_value(raw).unwrap_or(ListResourcesResult { resources: Vec::new() });
        Ok(res.resources)
    }

    pub async fn list_prompts(&self) -> Result<Vec<McpPrompt>, McpError> {
        let raw = self
            .send_request("prompts/list", json!({}))
            .await
            .unwrap_or(Value::Null);
        let res: ListPromptsResult = serde_json::from_value(raw).unwrap_or(ListPromptsResult { prompts: Vec::new() });
        Ok(res.prompts)
    }

    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<CallToolResult, McpError> {
        let params = json!({ "name": name, "arguments": arguments });
        let raw = self.send_request("tools/call", params).await?;
        let res: CallToolResult = serde_json::from_value(raw)?;
        Ok(res)
    }

    pub async fn read_resource(&self, uri: &str) -> Result<ReadResourceResult, McpError> {
        let params = json!({ "uri": uri });
        let raw = self.send_request("resources/read", params).await?;
        let res: ReadResourceResult = serde_json::from_value(raw)?;
        Ok(res)
    }

    pub async fn close(&self) {
        self.transport.close().await;
    }
}
