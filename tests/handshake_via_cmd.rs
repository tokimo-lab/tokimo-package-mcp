//! End-to-end regression test for the full MCP handshake on Windows
//! via a `.cmd` shim — exactly the production path used by `npx
//! chrome-devtools-mcp@latest`.
//!
//! Spawns a tiny Node MCP server stub that implements
//! `initialize` + `tools/list` + `resources/list` + `prompts/list` +
//! `notifications/initialized` per the MCP spec, wires it up via a
//! `.cmd` shim, and asserts that `McpConnection::connect` returns
//! successfully within 5 s.
//!
//! If this hangs we have evidence that the stdio transport (or the
//! client's id allocator) is broken — not the upstream MCP server.

#![cfg(target_os = "windows")]

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use tokimo_package_mcp::client::McpClient;
use tokimo_package_mcp::connection::McpConnection;
use tokimo_package_mcp::transport::stdio::StdioTransport;
use tokimo_package_mcp::types::ClientInfo;

/// Minimal MCP server stub. Speaks JSON-RPC line-per-message on
/// stdin / stdout. Logs to stderr so the parent's stdout reader is
/// not confused.
const MCP_STUB: &str = r#"
const stub = {
  initialize: () => ({
    protocolVersion: '2024-11-05',
    capabilities: { tools: {}, resources: {}, prompts: {} },
    serverInfo: { name: 'stub', version: '0.0.1' },
    instructions: 'stub'
  }),
  'tools/list': () => ({ tools: [{ name: 'echo', description: 'echo', inputSchema: { type: 'object' } }] }),
  'resources/list': () => ({ resources: [] }),
  'prompts/list': () => ({ prompts: [] }),
};
process.stdin.setEncoding('utf8');
let buf = '';
process.stdin.on('data', (chunk) => {
  buf += chunk;
  let idx;
  while ((idx = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, idx);
    buf = buf.slice(idx + 1);
    if (!line.trim()) continue;
    let req;
    try { req = JSON.parse(line); }
    catch (e) { process.stderr.write('parse err: ' + e + '\n'); continue; }
    if (req.id === undefined || req.id === null) {
      process.stderr.write('got notification: ' + req.method + '\n');
      continue;
    }
    const handler = stub[req.method];
    let payload;
    if (handler) {
      payload = { jsonrpc: '2.0', id: req.id, result: handler(req.params || {}) };
    } else {
      payload = { jsonrpc: '2.0', id: req.id, error: { code: -32601, message: 'method not found: ' + req.method } };
    }
    process.stdout.write(JSON.stringify(payload) + '\n');
  }
});
"#;

#[tokio::test]
async fn full_handshake_via_cmd_shim() {
    let Ok(node_path) = which::which("node") else {
        eprintln!("skipping: `node` not on PATH");
        return;
    };

    let tmp = tempfile::tempdir().expect("create tempdir");
    let js_path = tmp.path().join("mcp_stub.js");
    std::fs::write(&js_path, MCP_STUB).expect("write stub");
    let cmd_path = tmp.path().join("mcp_stub.cmd");
    let mut f = std::fs::File::create(&cmd_path).expect("write cmd shim");
    writeln!(f, "@echo off").unwrap();
    writeln!(f, r#""{}" "{}""#, node_path.display(), js_path.display()).unwrap();
    drop(f);

    let transport = StdioTransport::spawn(
        "test-mcp-stub",
        cmd_path.to_str().expect("utf-8 path"),
        &[],
        &HashMap::new(),
    )
    .expect("spawn stub via .cmd");

    let client = McpClient::new(Arc::new(transport), "mcp::test-stub");
    let info = ClientInfo {
        name: "tokimo-test".into(),
        version: "0.0.0".into(),
    };

    // The full McpConnection::connect path: initialize + tools/list +
    // resources/list + prompts/list. This is what `connect_row` in
    // the rust-server does for every MCP server. If anything in the
    // pipe / id-allocator / dispatch is broken on Windows, this
    // either hangs or returns an error well before the 5 s deadline.
    let conn = timeout(Duration::from_secs(5), McpConnection::connect("stub", client, info))
        .await
        .expect("handshake did not complete within 5 s — pipe / dispatch is broken")
        .expect("handshake returned an error");

    assert_eq!(conn.server_name, "stub");
    let tools = conn.tools().await;
    assert_eq!(tools.len(), 1, "expected 1 tool from stub");
    assert_eq!(tools[0].name, "echo");

    conn.close().await;
}
