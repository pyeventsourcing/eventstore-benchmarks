use anyhow::Result;
use async_trait::async_trait;
use axonserver_client::proto::dcb::source_events_response;
use axonserver_client::proto::dcb::{Criterion, Event, Tag, TaggedEvent, TagsAndNamesCriterion};
use axonserver_client::AxonServerClient;
use bench_core::adapter::{
    ConnectionParams, ContainerManager, EventData, EventStoreAdapter, ReadEvent, ReadRequest,
};
use bench_testcontainers::axonserver::{AxonServer, AXONSERVER_GRPC_PORT};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;

// Container manager - handles lifecycle
pub struct AxonServerContainerManager {
    container: Option<ContainerAsync<AxonServer>>,
}

impl AxonServerContainerManager {
    pub fn new() -> Self {
        Self { container: None }
    }
}

#[async_trait]
impl ContainerManager for AxonServerContainerManager {
    async fn start(&mut self) -> Result<ConnectionParams> {
        let container = AxonServer::default().start().await?;
        let host_port = container.get_host_port_ipv4(AXONSERVER_GRPC_PORT).await?;
        let uri = format!("http://localhost:{}", host_port);

        self.container = Some(container);

        // Wait for container to be ready
        for _ in 0..60 {
            if let Ok(mut client) = AxonServerClient::connect(uri.clone()).await {
                if client.get_head().await.is_ok() {
                    return Ok(ConnectionParams {
                        uri,
                        options: Default::default(),
                    });
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("Axon Server container did not become ready within 60s")
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(container) = self.container.take() {
            container.stop().await?;
        }
        Ok(())
    }

    fn container_id(&self) -> Option<String> {
        self.container.as_ref().map(|c| c.id().to_string())
    }
}

// Lightweight adapter - just wraps a client
pub struct AxonServerAdapter {
    client: AxonServerClient,
}

impl AxonServerAdapter {
    pub async fn new(params: &ConnectionParams) -> Result<Self> {
        let uri = if params.uri.is_empty() {
            "http://localhost:8124".to_string()
        } else {
            params.uri.clone()
        };
        let client = AxonServerClient::connect(uri).await?;

        Ok(Self { client })
    }
}

#[async_trait]
impl EventStoreAdapter for AxonServerAdapter {
    async fn append(&self, evt: EventData) -> Result<()> {
        // Note: AxonServerClient requires &mut self for operations,
        // but we need &self for the trait. We'll need to clone the client.
        // This is a limitation of the axonserver_client API design.
        let mut client = self.client.clone();

        let mut tags: Vec<Tag> = evt
            .tags
            .iter()
            .map(|t| Tag {
                key: t.as_bytes().to_vec().into(),
                value: Vec::new().into(),
            })
            .collect();
        // Add a stream tag so we can filter by stream on read.
        tags.push(Tag {
            key: b"stream".to_vec().into(),
            value: evt.stream.as_bytes().to_vec().into(),
        });

        let event = Event {
            identifier: uuid::Uuid::new_v4().to_string(),
            timestamp: now_millis(),
            name: evt.event_type,
            version: String::new(),
            payload: evt.payload.into(),
            metadata: Default::default(),
        };
        let tagged = TaggedEvent {
            event: Some(event),
            tag: tags,
        };
        client.append(vec![tagged]).await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let mut client = self.client.clone();

        let from = req.from_offset.unwrap_or(0) as i64;
        let criterion = Criterion {
            tags_and_names: Some(TagsAndNamesCriterion {
                name: vec![],
                tag: vec![Tag {
                    key: b"stream".to_vec().into(),
                    value: req.stream.as_bytes().to_vec().into(),
                }],
            }),
        };
        let responses = client.source(from, vec![criterion]).await?;

        let mut out = Vec::new();
        for resp in responses {
            if let Some(result) = resp.result {
                match result {
                    source_events_response::Result::Event(seq_evt) => {
                        if let Some(evt) = seq_evt.event {
                            out.push(ReadEvent {
                                offset: seq_evt.sequence as u64,
                                event_type: evt.name,
                                payload: evt.payload.to_vec(),
                                timestamp_ms: evt.timestamp as u64,
                            });
                        }
                        if let Some(lim) = req.limit {
                            if out.len() as u64 >= lim {
                                break;
                            }
                        }
                    }
                    source_events_response::Result::ConsistencyMarker(_) => {}
                }
            }
        }
        Ok(out)
    }

    async fn ping(&self) -> Result<Duration> {
        let mut client = self.client.clone();
        let t0 = std::time::Instant::now();
        client.get_head().await?;
        Ok(t0.elapsed())
    }
}

pub struct AxonServerFactory;

impl bench_core::AdapterFactory for AxonServerFactory {
    fn name(&self) -> &'static str {
        "axonserver"
    }

    fn create(&self, params: &ConnectionParams) -> Result<Box<dyn EventStoreAdapter>> {
        // AxonServerAdapter::new is async, so we need to block
        let adapter = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { AxonServerAdapter::new(params).await })
        })?;
        Ok(Box::new(adapter))
    }

    fn create_container_manager(&self) -> Option<Box<dyn ContainerManager>> {
        Some(Box::new(AxonServerContainerManager::new()))
    }
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
