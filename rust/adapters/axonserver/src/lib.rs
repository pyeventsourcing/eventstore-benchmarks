use anyhow::Result;
use async_trait::async_trait;
use axonserver_client::proto::dcb::{
    Criterion, Event, Tag, TaggedEvent, TagsAndNamesCriterion,
};
use axonserver_client::AxonServerClient;
use bench_core::adapter::{ConnectionParams, EventData, EventStoreAdapter, ReadEvent, ReadRequest};
use bench_testcontainers::axonserver::{AxonServer, AXONSERVER_GRPC_PORT};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::sync::Mutex;
use tokio::time::Duration;

pub struct AxonServerAdapter {
    client: Arc<Mutex<Option<AxonServerClient>>>,
    container: Arc<Mutex<Option<ContainerAsync<AxonServer>>>>,
}

impl AxonServerAdapter {
    pub fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            container: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl EventStoreAdapter for AxonServerAdapter {
    async fn setup(&self) -> Result<()> {
        let container = AxonServer::default().start().await?;
        let host_port = container.get_host_port_ipv4(AXONSERVER_GRPC_PORT).await?;
        let uri = format!("http://localhost:{}", host_port);

        let mut container_guard = self.container.lock().await;
        *container_guard = Some(container);
        drop(container_guard);

        for _ in 0..60 {
            if let Ok(client) = AxonServerClient::connect(uri.clone()).await {
                let mut client_guard = self.client.lock().await;
                *client_guard = Some(client);
                drop(client_guard);

                if self.ping().await.is_ok() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("Axon Server container did not become ready within 60s")
    }

    async fn teardown(&self) -> Result<()> {
        {
            let mut guard = self.client.lock().await;
            *guard = None;
        }
        let container = {
            let mut guard = self.container.lock().await;
            guard.take()
        };
        if let Some(c) = container {
            c.stop().await?;
            drop(c);
        }
        Ok(())
    }

    async fn connect(&self, params: &ConnectionParams) -> Result<()> {
        let uri = if params.uri.is_empty() {
            "http://localhost:8124".to_string()
        } else {
            params.uri.clone()
        };
        let client = AxonServerClient::connect(uri).await?;
        let mut guard = self.client.lock().await;
        *guard = Some(client);
        Ok(())
    }

    async fn append(&self, evt: EventData) -> Result<()> {
        let mut client = {
            let guard = self.client.lock().await;
            guard
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Axon Server client not connected"))?
        };

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

        // Write back in case the inner channel state changed.
        let mut guard = self.client.lock().await;
        *guard = Some(client);
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let mut client = {
            let guard = self.client.lock().await;
            guard
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Axon Server client not connected"))?
        };

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

        let mut guard = self.client.lock().await;
        *guard = Some(client);
        Ok(out)
    }

    async fn ping(&self) -> Result<Duration> {
        let mut client = {
            let guard = self.client.lock().await;
            guard
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Axon Server client not connected"))?
        };
        let t0 = std::time::Instant::now();
        client.get_head().await?;
        let elapsed = t0.elapsed();

        let mut guard = self.client.lock().await;
        *guard = Some(client);
        Ok(elapsed)
    }
}

// Bring the oneof variants into scope for pattern matching.
use axonserver_client::proto::dcb::source_events_response;

pub struct AxonServerFactory;

impl bench_core::AdapterFactory for AxonServerFactory {
    fn name(&self) -> &'static str {
        "axonserver"
    }
    fn create(&self) -> Box<dyn EventStoreAdapter> {
        Box::new(AxonServerAdapter::new())
    }
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
