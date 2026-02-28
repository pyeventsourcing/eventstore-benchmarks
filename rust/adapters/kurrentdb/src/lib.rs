use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{ConnectionParams, EventData, EventStoreAdapter, ReadEvent, ReadRequest};
use bench_testcontainers::kurrentdb::{KurrentDb, KURRENTDB_PORT};
use kurrentdb::{AppendToStreamOptions, Client, ClientSettings, ReadStreamOptions, StreamPosition};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::sync::Mutex;
use tokio::time::Duration;
use uuid::Uuid;

pub struct KurrentDbAdapter {
    client: Arc<Mutex<Option<Client>>>,
    container: Arc<Mutex<Option<ContainerAsync<KurrentDb>>>>,
}

impl KurrentDbAdapter {
    pub fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            container: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl EventStoreAdapter for KurrentDbAdapter {
    async fn setup(&self) -> Result<()> {
        let container = KurrentDb::default().start().await?;
        let host_port = container.get_host_port_ipv4(KURRENTDB_PORT).await?;
        let uri = format!("esdb://localhost:{}?tls=false", host_port);

        let mut container_guard = self.container.lock().await;
        *container_guard = Some(container);
        drop(container_guard);

        for _ in 0..60 {
            // Recreate the client on each attempt so the gRPC channel
            // doesn't cache a failed connection from before the node is ready.
            if let Ok(settings) = uri.parse::<ClientSettings>() {
                if let Ok(client) = Client::new(settings) {
                    let mut client_guard = self.client.lock().await;
                    *client_guard = Some(client);
                    drop(client_guard);

                    if self.ping().await.is_ok() {
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("KurrentDB container did not become ready within 60s")
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
        let conn_str = if params.uri.is_empty() {
            "esdb://localhost:2113?tls=false".to_string()
        } else {
            params.uri.clone()
        };
        let settings: ClientSettings = conn_str.parse()?;
        let client = Client::new(settings).map_err(|e| anyhow::anyhow!("{}", e))?;
        let mut guard = self.client.lock().await;
        *guard = Some(client);
        Ok(())
    }

    async fn append(&self, evt: EventData) -> Result<()> {
        let client = {
            let guard = self.client.lock().await;
            guard
                .clone()
                .ok_or_else(|| anyhow::anyhow!("KurrentDB client not connected"))?
        };
        let event =
            kurrentdb::EventData::binary(evt.event_type, evt.payload.into()).id(Uuid::new_v4());
        let options = AppendToStreamOptions::default();
        client.append_to_stream(evt.stream, &options, event).await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let client = {
            let guard = self.client.lock().await;
            guard
                .clone()
                .ok_or_else(|| anyhow::anyhow!("KurrentDB client not connected"))?
        };
        let count = req.limit.unwrap_or(4096) as usize;
        let options = ReadStreamOptions::default()
            .position(match req.from_offset {
                Some(off) => StreamPosition::Position(off),
                None => StreamPosition::Start,
            })
            .max_count(count);
        let mut stream = client.read_stream(req.stream, &options).await?;
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
        let client = {
            let guard = self.client.lock().await;
            guard
                .clone()
                .ok_or_else(|| anyhow::anyhow!("KurrentDB client not connected"))?
        };
        let t0 = std::time::Instant::now();
        // Perform a test append to verify the node is leader and accepting writes
        let event = kurrentdb::EventData::binary("ping", vec![].into()).id(Uuid::new_v4());
        let options = AppendToStreamOptions::default();
        client.append_to_stream("_ping", &options, event).await?;
        Ok(t0.elapsed())
    }
}

pub struct KurrentDbFactory;

impl bench_core::AdapterFactory for KurrentDbFactory {
    fn name(&self) -> &'static str {
        "kurrentdb"
    }
    fn create(&self) -> Box<dyn EventStoreAdapter> {
        Box::new(KurrentDbAdapter::new())
    }
}
