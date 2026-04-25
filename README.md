# tokimo-package-mcp

MCP (Model Context Protocol) client for Rust — protocol types, transports, client connection management.

## Features

- **Full MCP protocol** — types for tools, resources, prompts, sampling, and capabilities
- **Multiple transports**:
  - `StdioTransport` — launch an MCP server as a subprocess, communicate over stdin/stdout
  - `HttpTransport` — connect to an HTTP-based MCP server (SSE + POST)
- **Connection management** — `McpConnection` tracks server state (initializing / ready / error), reconnects
- **Tool name utilities** — build/parse structured MCP tool names with server prefix (`build_mcp_tool_name`)
- **Streaming** — async tool call results via tokio streams

## Usage

```rust
use tokimo_package_mcp::{McpClient, transport::stdio::StdioTransport};

let transport = StdioTransport::spawn("npx", &["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]).await?;
let client = McpClient::new(transport);
client.initialize().await?;

let tools = client.list_tools(None).await?;
for tool in &tools {
    println!("{}: {}", tool.name, tool.description.as_deref().unwrap_or(""));
}

let result = client.call_tool("read_file", serde_json::json!({ "path": "/tmp/test.txt" })).await?;
```

## Cargo

```toml
tokimo-package-mcp = { git = "https://github.com/tokimo-lab/tokimo-package-mcp" }
```

## License

MIT
