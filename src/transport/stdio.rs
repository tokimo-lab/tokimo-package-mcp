//! Stdio transport — spawns a child process, writes JSON-per-line to its
//! stdin, parses JSON-per-line from its stdout.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, mpsc};

use crate::error::McpError;
use crate::transport::McpTransport;

pub struct StdioTransport {
    inner: Arc<StdioInner>,
}

struct StdioInner {
    child: Mutex<Option<Child>>,
    stdin: Mutex<Option<ChildStdin>>,
    incoming: Mutex<mpsc::Receiver<Value>>,
    log_target: String,
}

impl StdioTransport {
    pub fn spawn(
        server_name: &str,
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> Result<Self, McpError> {
        let mut cmd = Command::new(command);
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(env)
            .kill_on_drop(true);

        let mut child = cmd.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| McpError::other("failed to capture child stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::other("failed to capture child stdout"))?;
        let stderr = child.stderr.take();

        let (tx, rx) = mpsc::channel::<Value>(128);

        // Stdout reader — parses JSON per line.
        let log_target = format!("mcp::{server_name}");
        let target_read = log_target.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<Value>(trimmed) {
                            Ok(v) => {
                                if tx.send(v).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(target: "mcp::transport", target_log = %target_read, "invalid JSON from stdout: {e} :: {trimmed}");
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(target: "mcp::transport", target_log = %target_read, "stdout read error: {e}");
                        break;
                    }
                }
            }
            tracing::info!(target: "mcp::transport", target_log = %target_read, "stdout reader exited");
        });

        // Stderr forwarder — log only.
        if let Some(stderr) = stderr {
            let target_err = log_target.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    if !line.trim().is_empty() {
                        tracing::debug!(target: "mcp::transport", target_log = %target_err, "stderr: {line}");
                    }
                }
            });
        }

        Ok(Self {
            inner: Arc::new(StdioInner {
                child: Mutex::new(Some(child)),
                stdin: Mutex::new(Some(stdin)),
                incoming: Mutex::new(rx),
                log_target,
            }),
        })
    }
}

#[async_trait]
impl McpTransport for StdioTransport {
    async fn send(&self, msg: Value) -> Result<(), McpError> {
        let mut guard = self.inner.stdin.lock().await;
        let stdin = guard.as_mut().ok_or(McpError::ConnectionLost)?;
        let mut bytes = serde_json::to_vec(&msg)?;
        bytes.push(b'\n');
        stdin.write_all(&bytes).await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn recv(&self) -> Option<Value> {
        let mut rx = self.inner.incoming.lock().await;
        rx.recv().await
    }

    async fn close(&self) {
        if let Some(mut stdin) = self.inner.stdin.lock().await.take() {
            let _ = stdin.shutdown().await;
        }
        if let Some(mut child) = self.inner.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        tracing::debug!(target: "mcp::transport", target_log = %self.inner.log_target, "stdio transport closed");
    }
}
