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
        let resolved_command = resolve_command(command)?;
        // Pass the resolved path (possibly `.cmd` / `.bat`) directly to
        // `Command::new`. Since Rust 1.77 the standard library
        // transparently wraps batch scripts with `cmd.exe /C` using
        // correct argument quoting (CVE-2024-24576 fix). Wrapping again
        // ourselves causes argv to be mangled by cmd.exe's notorious
        // quoting rules and starves the child's stdin / stdout pipes,
        // making `initialize` time out.
        let mut cmd = Command::new(&resolved_command);
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

fn resolve_command(command: &str) -> Result<std::path::PathBuf, McpError> {
    match which::which(command) {
        Ok(path) => Ok(path),
        Err(which::Error::CannotFindBinaryPath) => Err(McpError::other(command_not_found_message(command))),
        Err(err) => Err(McpError::other(format!(
            "failed to resolve MCP server command '{command}' from PATH: {err}"
        ))),
    }
}

#[cfg(target_os = "windows")]
fn command_not_found_message(command: &str) -> String {
    format!(
        "program '{command}' not found in PATH (looked for {command}, {command}.cmd, {command}.bat, {command}.exe on Windows)"
    )
}

#[cfg(not(target_os = "windows"))]
fn command_not_found_message(command: &str) -> String {
    format!("program '{command}' not found in PATH")
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[cfg(target_os = "windows")]
    use tempfile::tempdir;
    use tokio::time::{Duration, timeout};

    /// Minimal Node.js script that reads JSON-per-line from stdin and
    /// echoes each parsed object back with `{ "echo": <orig> }`. Used to
    /// verify that the stdio transport's stdin / stdout pipes survive
    /// however the OS launches the binary (notably Windows `.cmd`
    /// shims).
    const NODE_ECHO: &str = r#"
process.stdin.setEncoding('utf8');
let buf = '';
process.stdin.on('data', (chunk) => {
  buf += chunk;
  let idx;
  while ((idx = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, idx);
    buf = buf.slice(idx + 1);
    if (!line.trim()) continue;
    try {
      const v = JSON.parse(line);
      process.stdout.write(JSON.stringify({ echo: v }) + '\n');
    } catch (e) {
      process.stdout.write(JSON.stringify({ error: String(e) }) + '\n');
    }
  }
});
"#;

    /// Resolve the command name we want to invoke. On Windows we
    /// deliberately use the bare name (`node`) — not `node.exe` —
    /// so the test exercises the PATHEXT-aware resolver. If `node`
    /// is not on PATH we skip the test (CI environments without
    /// Node should not fail).
    fn node_command() -> Option<&'static str> {
        if which::which("node").is_ok() {
            Some("node")
        } else {
            None
        }
    }

    #[tokio::test]
    async fn stdio_roundtrip_through_resolved_command() {
        let Some(cmd) = node_command() else {
            eprintln!("skipping: `node` not on PATH");
            return;
        };

        let transport = StdioTransport::spawn("test-echo", cmd, &["-e".into(), NODE_ECHO.into()], &HashMap::new())
            .expect("spawn echo child");

        // Send a JSON request and expect the echo back within a short
        // window. The whole roundtrip must complete in well under the
        // 60 s production initialize timeout — if pipes are starved
        // (e.g. due to incorrect cmd.exe wrapping on Windows) this
        // assertion fires immediately rather than waiting 60 s.
        transport
            .send(json!({"hello": "world"}))
            .await
            .expect("write to child stdin");

        let reply = timeout(Duration::from_secs(5), transport.recv())
            .await
            .expect("child did not respond within 5 s — stdin/stdout pipe is starved")
            .expect("child closed stdout unexpectedly");

        assert_eq!(reply, json!({"echo": {"hello": "world"}}));

        transport.close().await;
    }

    #[tokio::test]
    async fn resolve_command_reports_not_found_clearly() {
        let err = resolve_command("definitely-not-a-real-binary-xyz").expect_err("missing binary should error");
        let msg = format!("{err}");
        assert!(msg.contains("not found in PATH"), "got: {msg}");
    }

    /// Windows-specific regression test: a `.cmd` shim must forward
    /// stdin / stdout to the underlying program. The previous
    /// implementation wrapped the resolved path with `cmd.exe /C` a
    /// second time, which mangled argv via cmd.exe's quoting rules and
    /// (on some shells) starved the child's stdin pipe so MCP
    /// `initialize` timed out at 60 s. Letting `Command::new` see the
    /// `.cmd` directly lets Rust's built-in shim wrap it correctly.
    #[cfg(target_os = "windows")]
    #[tokio::test]
    async fn stdio_roundtrip_through_cmd_shim() {
        use std::io::Write;

        let Some(node) = node_command() else {
            eprintln!("skipping: `node` not on PATH");
            return;
        };
        // Resolve node to an absolute path so the .cmd shim doesn't
        // depend on PATH inheritance.
        let node_path = which::which(node).expect("resolve node path");

        let tmp = tempdir().expect("create tempdir");
        let js_path = tmp.path().join("echo.js");
        std::fs::write(&js_path, NODE_ECHO).expect("write js");
        let script_path = tmp.path().join("echo_shim.cmd");
        let mut f = std::fs::File::create(&script_path).expect("write shim");
        writeln!(f, "@echo off").unwrap();
        writeln!(f, r#""{}" "{}""#, node_path.display(), js_path.display()).unwrap();
        drop(f);

        let transport = StdioTransport::spawn(
            "test-cmd-shim",
            script_path.to_str().expect("utf-8 path"),
            &[],
            &HashMap::new(),
        )
        .expect("spawn cmd shim");

        transport.send(json!({"via": "cmd"})).await.expect("write");
        let reply = timeout(Duration::from_secs(5), transport.recv())
            .await
            .expect("cmd shim did not respond within 5 s — stdin/stdout pipe is starved")
            .expect("cmd shim closed stdout unexpectedly");
        assert_eq!(reply, json!({"echo": {"via": "cmd"}}));
        transport.close().await;
    }
}
