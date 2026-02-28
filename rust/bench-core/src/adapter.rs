use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use crate::metrics::ContainerMetrics;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionParams {
    pub uri: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventData {
    pub stream: String,
    pub event_type: String,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub stream: String,
    #[serde(default)]
    pub from_offset: Option<u64>,
    #[serde(default)]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadEvent {
    pub offset: u64,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub timestamp_ms: u64,
}

#[async_trait]
pub trait EventStoreAdapter: Send + Sync {
    async fn setup(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn teardown(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn connect(&self, params: &ConnectionParams) -> anyhow::Result<()>;

    async fn append(&self, evt: EventData) -> anyhow::Result<()>;

    async fn batch_append(&self, events: Vec<EventData>) -> anyhow::Result<()> {
        for e in events {
            self.append(e).await?;
        }
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> anyhow::Result<Vec<ReadEvent>>;

    async fn ping(&self) -> anyhow::Result<Duration>;

    /// Collect container metrics (image size, CPU, memory).
    /// Returns ContainerMetrics with available data. Adapters that don't use containers
    /// can return default/empty metrics.
    async fn collect_container_metrics(&self) -> anyhow::Result<ContainerMetrics> {
        Ok(ContainerMetrics::default())
    }
}

pub trait AdapterFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn create(&self) -> Box<dyn EventStoreAdapter>;
}
