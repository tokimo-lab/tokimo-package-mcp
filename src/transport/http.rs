//! HTTP transport — MCP "Streamable HTTP" (2025-03-26 spec).
//!
//! Wire model:
//!   - All client→server messages are POSTed to a single URL with
//!     `Content-Type: application/json` and `Accept: application/json, text/event-stream`.
//!   - Server responds with either:
//!     * `202 Accepted` with empty body (for notifications — no reply expected)
//!     * `application/json` body containing exactly one JSON-RPC message
//!     * `text/event-stream` body containing one or more `data:` SSE events,
//!       each a JSON-RPC message
//!   - Server may echo `Mcp-Session-Id` header on the first response; client
//!     must include it in all subsequent requests for that session.
//!
//! This implementation is sufficient for simple request/response MCP servers
//! (e.g. ida-pro-mcp). It does NOT open the optional server→client GET SSE
//! stream — that can be added later if servers in the wild require push
//! notifications outside of a request.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::error::McpError;
use crate::transport::McpTransport;

pub struct HttpTransport {
    inner: Arc<HttpInner>,
}

struct HttpInner {
    client: Client,
    url: String,
    headers: HashMap<String, String>,
    session_id: Mutex<Option<String>>,
    incoming: Mutex<mpsc::Receiver<Value>>,
    incoming_tx: mpsc::Sender<Value>,
    log_target: String,
}

impl HttpTransport {
    pub fn new(server_name: &str, url: String, headers: HashMap<String, String>) -> Result<Self, McpError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            // No overall request timeout; long-lived SSE responses are allowed.
            .pool_idle_timeout(Some(Duration::from_secs(30)))
            .build()
            .map_err(|e| McpError::other(format!("http client build failed: {e}")))?;

        let (tx, rx) = mpsc::channel::<Value>(128);

        Ok(Self {
            inner: Arc::new(HttpInner {
                client,
                url,
                headers,
                session_id: Mutex::new(None),
                incoming: Mutex::new(rx),
                incoming_tx: tx,
                log_target: format!("mcp::{server_name}"),
            }),
        })
    }
}

#[async_trait]
impl McpTransport for HttpTransport {
    async fn send(&self, msg: Value) -> Result<(), McpError> {
        let inner = Arc::clone(&self.inner);

        // Build request synchronously so the caller sees immediate errors
        // (network set-up, invalid URL). Body streaming happens in a spawned
        // task so `send` returns promptly.
        let mut req = inner
            .client
            .post(&inner.url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream");
        for (k, v) in &inner.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(sid) = inner.session_id.lock().await.clone() {
            req = req.header("mcp-session-id", sid);
        }
        let req = req.json(&msg);

        let tx = inner.incoming_tx.clone();
        let session_slot = Arc::clone(&inner);
        let log_target = inner.log_target.clone();

        tokio::spawn(async move {
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(target: "mcp::transport", target_log = %log_target, "http POST failed: {e}");
                    return;
                }
            };

            // Capture Mcp-Session-Id from the first response.
            if let Some(sid) = resp.headers().get("mcp-session-id")
                && let Ok(s) = sid.to_str()
            {
                let mut slot = session_slot.session_id.lock().await;
                if slot.is_none() {
                    *slot = Some(s.to_string());
                }
            }

            let status = resp.status();
            let content_type = resp
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_ascii_lowercase();

            if status == StatusCode::ACCEPTED || status == StatusCode::NO_CONTENT {
                // No body expected (notification ack).
                return;
            }

            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(
                    target: "mcp::transport",
                    target_log = %log_target,
                    "http POST non-2xx: {status} :: {body}",
                );
                return;
            }

            if content_type.contains("text/event-stream") {
                parse_sse_stream(resp, tx, &log_target).await;
            } else {
                match resp.text().await {
                    Ok(body) if !body.trim().is_empty() => match serde_json::from_str::<Value>(body.trim()) {
                        Ok(v) => {
                            let _ = tx.send(v).await;
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "mcp::transport",
                                target_log = %log_target,
                                "invalid JSON body: {e} :: {body}",
                            );
                        }
                    },
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(target: "mcp::transport", target_log = %log_target, "read body failed: {e}");
                    }
                }
            }
        });

        Ok(())
    }

    async fn recv(&self) -> Option<Value> {
        self.inner.incoming.lock().await.recv().await
    }

    async fn close(&self) {
        // Best-effort: DELETE the session to let the server reclaim resources.
        if let Some(sid) = self.inner.session_id.lock().await.clone() {
            let req = self.inner.client.delete(&self.inner.url).header("mcp-session-id", sid);
            let _ = req.send().await;
        }
        tracing::debug!(target: "mcp::transport", target_log = %self.inner.log_target, "http transport closed");
    }
}

/// Parse a `text/event-stream` response body, forwarding each event's `data:`
/// payload (joined across multiple `data:` lines per SSE spec) into `tx` as
/// a JSON value.
async fn parse_sse_stream(resp: reqwest::Response, tx: mpsc::Sender<Value>, log_target: &str) {
    let mut stream = resp.bytes_stream();
    let mut buf = String::new();

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                match std::str::from_utf8(&bytes) {
                    Ok(s) => buf.push_str(s),
                    Err(_) => {
                        // Best-effort: append lossy conversion.
                        buf.push_str(&String::from_utf8_lossy(&bytes));
                    }
                }
                while let Some(idx) = find_event_boundary(&buf) {
                    let event = buf[..idx].to_string();
                    buf.drain(..idx + event_boundary_len(&buf[idx..]));
                    if let Some(data) = extract_sse_data(&event)
                        && !data.is_empty()
                    {
                        match serde_json::from_str::<Value>(&data) {
                            Ok(v) => {
                                if tx.send(v).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "mcp::transport",
                                    target_log = %log_target,
                                    "invalid SSE JSON: {e} :: {data}",
                                );
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(target: "mcp::transport", target_log = %log_target, "SSE stream error: {e}");
                break;
            }
        }
    }

    // Handle any trailing event without a final blank line.
    if !buf.trim().is_empty()
        && let Some(data) = extract_sse_data(&buf)
        && !data.is_empty()
        && let Ok(v) = serde_json::from_str::<Value>(&data)
    {
        let _ = tx.send(v).await;
    }
}

/// Find the position of the first event terminator (`\n\n` or `\r\n\r\n`).
fn find_event_boundary(s: &str) -> Option<usize> {
    let lf = s.find("\n\n");
    let crlf = s.find("\r\n\r\n");
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn event_boundary_len(s: &str) -> usize {
    if s.starts_with("\r\n\r\n") { 4 } else { 2 }
}

fn extract_sse_data(event: &str) -> Option<String> {
    let mut data = String::new();
    for line in event.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if data.is_empty() { None } else { Some(data) }
}
