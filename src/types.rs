use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

// ── Initialize ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    #[serde(default)]
    pub protocol_version: String,
    #[serde(default)]
    pub capabilities: Value,
    #[serde(default)]
    pub server_info: Value,
    #[serde(default)]
    pub instructions: Option<String>,
}

// ── Tools ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpAnnotations {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub read_only_hint: bool,
    #[serde(default)]
    pub destructive_hint: bool,
    #[serde(default)]
    pub idempotent_hint: bool,
    #[serde(default)]
    pub open_world_hint: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct McpTool {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "inputSchema", default = "empty_schema")]
    pub input_schema: Value,
    #[serde(default)]
    pub annotations: McpAnnotations,
    #[serde(rename = "_meta", default)]
    pub meta: Value,
}

fn empty_schema() -> Value {
    serde_json::json!({ "type": "object" })
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListToolsResult {
    #[serde(default)]
    pub tools: Vec<McpTool>,
}

// ── Call tool ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum McpContent {
    Text {
        text: String,
    },
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
    },
    Resource {
        resource: Value,
    },
    #[serde(rename = "resource_link")]
    ResourceLink {
        uri: String,
        #[serde(default)]
        name: Option<String>,
        #[serde(default, rename = "mimeType")]
        mime_type: Option<String>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallToolResult {
    #[serde(default)]
    pub content: Vec<McpContent>,
    #[serde(default)]
    pub is_error: bool,
    #[serde(default, rename = "_meta")]
    pub meta: Value,
    #[serde(default)]
    pub structured_content: Value,
}

// ── Resources ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpResource {
    pub uri: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListResourcesResult {
    #[serde(default)]
    pub resources: Vec<McpResource>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadResourceResult {
    #[serde(default)]
    pub contents: Vec<Value>,
}

// ── Prompts ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct McpPrompt {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListPromptsResult {
    #[serde(default)]
    pub prompts: Vec<McpPrompt>,
}

// ── Notifications ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum McpNotification {
    ToolsListChanged,
    ResourcesListChanged,
    PromptsListChanged,
    Progress { token: Value, progress: Value },
    LogMessage { level: String, data: Value },
    Other { method: String, params: Value },
}
