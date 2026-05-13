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

/// Stub that sends a server-initiated `roots/list` request right
/// after `initialize` (mirrors chrome-devtools-mcp's behaviour) and
/// records the client's reply to stderr — which the test then reads
/// via a side channel by examining how the stub behaves once it has
/// a reply.
///
/// Protocol:
/// 1. Wait for `initialize` request → reply normally.
/// 2. Immediately send `{ id: 100, method: 'roots/list' }` to the
///    client.
/// 3. When the response to id=100 arrives, remember its `result` and
///    reply to ANY subsequent `tools/call` echoing the roots back in
///    the tool result content. The test calls `tools/call` and
///    asserts the roots are `[]` — proving the client both replied
///    and that the reply had the right shape.
const MCP_STUB_WITH_ROOTS_REQUEST: &str = r#"
let rootsFromClient = null;
const stub = {
  initialize: () => ({
    protocolVersion: '2024-11-05',
    capabilities: { tools: {}, resources: {}, prompts: {}, roots: { listChanged: false } },
    serverInfo: { name: 'stub', version: '0.0.1' }
  }),
  'tools/list': () => ({ tools: [{ name: 'whoami', description: 'reports roots', inputSchema: { type: 'object' } }] }),
  'resources/list': () => ({ resources: [] }),
  'prompts/list': () => ({ prompts: [] }),
  'tools/call': () => ({
    content: [{ type: 'text', text: JSON.stringify({ roots: rootsFromClient }) }],
    isError: false
  })
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
    let msg;
    try { msg = JSON.parse(line); }
    catch (e) { process.stderr.write('parse err: ' + e + '\n'); continue; }

    // Reply from client to our server-initiated request
    if (msg.id === 100 && (msg.result !== undefined || msg.error !== undefined)) {
      rootsFromClient = msg.error ? { error: msg.error } : (msg.result && msg.result.roots);
      process.stderr.write('got roots reply: ' + JSON.stringify(rootsFromClient) + '\n');
      continue;
    }
    if (msg.id === undefined || msg.id === null) continue; // notification

    // Normal request from client
    const handler = stub[msg.method];
    let payload;
    if (handler) {
      payload = { jsonrpc: '2.0', id: msg.id, result: handler(msg.params || {}) };
    } else {
      payload = { jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'method not found: ' + msg.method } };
    }
    process.stdout.write(JSON.stringify(payload) + '\n');

    // After initialize, fire our server-initiated roots/list request
    if (msg.method === 'initialize') {
      process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: 100, method: 'roots/list' }) + '\n');
    }
  }
});
"#;

/// Regression test for the original "tool calls hang forever" bug
/// observed with `chrome-devtools-mcp`. The MCP spec lets the server
/// send requests to the client (e.g. `roots/list`). If the client
/// silently drops them, the server's pending future never resolves
/// and any subsequent `tools/call` blocks until the 60 s client
/// timeout fires — visible to the user as "MCP probe is stuck".
///
/// This test asserts that:
/// 1. The handshake completes.
/// 2. A `tools/call` after handshake succeeds quickly.
/// 3. The stub observed our reply to its `roots/list` and the reply
///    had the expected `{ roots: [] }` shape.
#[tokio::test]
async fn server_initiated_roots_list_is_answered() {
    let Ok(node_path) = which::which("node") else {
        eprintln!("skipping: `node` not on PATH");
        return;
    };

    let tmp = tempfile::tempdir().expect("create tempdir");
    let js_path = tmp.path().join("mcp_stub_roots.js");
    std::fs::write(&js_path, MCP_STUB_WITH_ROOTS_REQUEST).expect("write stub");
    let cmd_path = tmp.path().join("mcp_stub_roots.cmd");
    let mut f = std::fs::File::create(&cmd_path).expect("write cmd shim");
    writeln!(f, "@echo off").unwrap();
    writeln!(f, r#""{}" "{}""#, node_path.display(), js_path.display()).unwrap();
    drop(f);

    let transport = StdioTransport::spawn(
        "test-mcp-roots",
        cmd_path.to_str().expect("utf-8 path"),
        &[],
        &HashMap::new(),
    )
    .expect("spawn stub");

    let client = McpClient::new(Arc::new(transport), "mcp::test-roots");
    let info = ClientInfo {
        name: "tokimo-test".into(),
        version: "0.0.0".into(),
    };

    let conn = timeout(
        Duration::from_secs(5),
        McpConnection::connect("stub", client.clone(), info),
    )
    .await
    .expect("handshake did not complete within 5 s")
    .expect("handshake failed");

    // Give the stub a moment to record our reply to its `roots/list`
    // request — it happens off the critical path of our requests.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now call a tool. The stub echoes whatever roots WE replied
    // with. If our reply was missing or malformed, this fails.
    let result = timeout(
        Duration::from_secs(5),
        client.call_tool("whoami", serde_json::json!({})),
    )
    .await
    .expect("tools/call hung — server is still waiting on roots reply")
    .expect("tools/call returned an error");

    let text = result
        .content
        .iter()
        .find_map(|c| match c {
            tokimo_package_mcp::types::McpContent::Text { text } => Some(text.clone()),
            _ => None,
        })
        .expect("expected text content");
    assert!(
        text.contains("\"roots\":[]") || text.contains("\"roots\": []"),
        "stub did not record correct roots reply; got: {text}"
    );

    conn.close().await;
}
