use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    ConnectionParams, ContainerManager, EventData, EventStoreAdapter, ReadEvent, ReadRequest,
};
use bench_testcontainers::kurrentdb::{KurrentDb, KURRENTDB_PORT};
use kurrentdb::{AppendToStreamOptions, Client, ClientSettings, ReadStreamOptions, StreamPosition};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;
use uuid::Uuid;

// Container manager - handles lifecycle
pub struct KurrentDbContainerManager {
    container: Option<ContainerAsync<KurrentDb>>,
}

impl KurrentDbContainerManager {
    pub fn new() -> Self {
        Self { container: None }
    }
}

#[async_trait]
impl ContainerManager for KurrentDbContainerManager {
    async fn start(&mut self) -> Result<ConnectionParams> {
        let container = KurrentDb::default().start().await?;
        let host_port = container.get_host_port_ipv4(KURRENTDB_PORT).await?;
        let uri = format!("esdb://localhost:{}?tls=false", host_port);

        self.container = Some(container);

        // Wait for container to be ready
        for _ in 0..60 {
            // Recreate the client on each attempt so the gRPC channel
            // doesn't cache a failed connection from before the node is ready.
            if let Ok(settings) = uri.parse::<ClientSettings>() {
                if let Ok(client) = Client::new(settings) {
                    // Test with a ping append
                    let event =
                        kurrentdb::EventData::binary("ping", vec![].into()).id(Uuid::new_v4());
                    let options = AppendToStreamOptions::default();
                    if client
                        .append_to_stream("_ping", &options, event)
                        .await
                        .is_ok()
                    {
                        return Ok(ConnectionParams {
                            uri,
                            options: Default::default(),
                        });
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("KurrentDB container did not become ready within 60s")
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
pub struct KurrentDbAdapter {
    client: Client,
}

impl KurrentDbAdapter {
    pub fn new(params: &ConnectionParams) -> Result<Self> {
        let conn_str = if params.uri.is_empty() {
            "esdb://localhost:2113?tls=false".to_string()
        } else {
            params.uri.clone()
        };
        let settings: ClientSettings = conn_str.parse()?;
        let client = Client::new(settings).map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(Self { client })
    }
}

#[async_trait]
impl EventStoreAdapter for KurrentDbAdapter {
    async fn append(&self, evt: EventData) -> Result<()> {
        let event =
            kurrentdb::EventData::binary(evt.event_type, evt.payload.into()).id(Uuid::new_v4());
        let options = AppendToStreamOptions::default();
        self.client
            .append_to_stream(evt.stream, &options, event)
            .await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let count = req.limit.unwrap_or(4096) as usize;
        let options = ReadStreamOptions::default()
            .position(match req.from_offset {
                Some(off) => StreamPosition::Position(off),
                None => StreamPosition::Start,
            })
            .max_count(count);
        let mut stream = self.client.read_stream(req.stream, &options).await?;
        let mut out = Vec::new();
        while let Some(event) = stream.next().await? {
            let recorded = event.get_original_event();
            out.push(ReadEvent {
                offset: recorded.revision,
                event_type: recorded.event_type.clone(),
                payload: recorded.data.to_vec(),
                timestamp_ms: recorded.created.timestamp_millis() as u64,
            });
            if let Some(lim) = req.limit {
                if out.len() as u64 >= lim {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn ping(&self) -> Result<Duration> {
        let t0 = std::time::Instant::now();
        // Perform a test append to verify the node is leader and accepting writes
        let event = kurrentdb::EventData::binary("ping", vec![].into()).id(Uuid::new_v4());
        let options = AppendToStreamOptions::default();
        self.client
            .append_to_stream("_ping", &options, event)
            .await?;
        Ok(t0.elapsed())
    }
}

pub struct KurrentDbFactory;

impl bench_core::AdapterFactory for KurrentDbFactory {
    fn name(&self) -> &'static str {
        "kurrentdb"
    }

    fn create(&self, params: &ConnectionParams) -> Result<Box<dyn EventStoreAdapter>> {
        Ok(Box::new(KurrentDbAdapter::new(params)?))
    }

    fn create_container_manager(&self) -> Option<Box<dyn ContainerManager>> {
        Some(Box::new(KurrentDbContainerManager::new()))
    }
}
