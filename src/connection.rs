//! [`McpConnection`] — thin wrapper around [`McpClient`] that tracks
//! connection state + caches tool list after handshake.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::client::McpClient;
use crate::error::McpError;
use crate::types::{ClientInfo, InitializeResult, McpPrompt, McpResource, McpTool};

#[derive(Debug, Clone)]
pub enum McpConnectionState {
    Connecting,
    Connected,
    NeedsAuth,
    Failed(String),
}

pub struct McpConnection {
    pub server_name: String,
    pub client: Arc<McpClient>,
    pub init: InitializeResult,
    tools: RwLock<Vec<McpTool>>,
    resources: RwLock<Vec<McpResource>>,
    prompts: RwLock<Vec<McpPrompt>>,
    state: RwLock<McpConnectionState>,
}

impl McpConnection {
    /// Perform handshake and cache initial tool/resource/prompt lists.
    pub async fn connect(
        server_name: impl Into<String>,
        client: Arc<McpClient>,
        client_info: ClientInfo,
    ) -> Result<Arc<Self>, McpError> {
        let server_name = server_name.into();
        let init = client.initialize(client_info).await?;
        let tools = client.list_tools().await.unwrap_or_default();
        let resources = client.list_resources().await.unwrap_or_default();
        let prompts = client.list_prompts().await.unwrap_or_default();
        Ok(Arc::new(Self {
            server_name,
            client,
            init,
            tools: RwLock::new(tools),
            resources: RwLock::new(resources),
            prompts: RwLock::new(prompts),
            state: RwLock::new(McpConnectionState::Connected),
        }))
    }

    pub async fn tools(&self) -> Vec<McpTool> {
        self.tools.read().await.clone()
    }

    pub async fn resources(&self) -> Vec<McpResource> {
        self.resources.read().await.clone()
    }

    pub async fn prompts(&self) -> Vec<McpPrompt> {
        self.prompts.read().await.clone()
    }

    pub async fn refresh_tools(&self) -> Result<(), McpError> {
        let tools = self.client.list_tools().await?;
        *self.tools.write().await = tools;
        Ok(())
    }

    pub async fn state(&self) -> McpConnectionState {
        self.state.read().await.clone()
    }

    pub async fn set_state(&self, state: McpConnectionState) {
        *self.state.write().await = state;
    }

    pub async fn close(&self) {
        self.client.close().await;
    }
}
