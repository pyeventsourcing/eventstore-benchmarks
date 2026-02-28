use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{ConnectionParams, EventData, EventStoreAdapter, ReadEvent, ReadRequest};
use bench_testcontainers::eventsourcingdb::{
    EventsourcingDb, EVENTSOURCINGDB_API_TOKEN, EVENTSOURCINGDB_PORT,
};
use eventsourcingdb::client::Client;
use eventsourcingdb::event::EventCandidate;
use futures::StreamExt;
use serde_json::json;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::sync::Mutex;
use tokio::time::Duration;
use url::Url;

pub struct EventsourcingDbAdapter {
    client: Arc<Mutex<Option<Arc<Client>>>>,
    container: Arc<Mutex<Option<ContainerAsync<EventsourcingDb>>>>,
}

impl EventsourcingDbAdapter {
    pub fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            container: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_client(&self) -> Result<Arc<Client>> {
        let guard = self.client.lock().await;
        guard
            .clone()
            .ok_or_else(|| anyhow::anyhow!("EventsourcingDB client not connected"))
    }
}

#[async_trait]
impl EventStoreAdapter for EventsourcingDbAdapter {
    async fn setup(&self) -> Result<()> {
        let container = EventsourcingDb::default().start().await?;
        let host_port = container.get_host_port_ipv4(EVENTSOURCINGDB_PORT).await?;
        let base_url = format!("http://localhost:{}/", host_port);

        let mut container_guard = self.container.lock().await;
        *container_guard = Some(container);
        drop(container_guard);

        for _ in 0..60 {
            let url: Url = base_url.parse()?;
            let client = Client::new(url, EVENTSOURCINGDB_API_TOKEN);
            if client.ping().await.is_ok() {
                let mut client_guard = self.client.lock().await;
                *client_guard = Some(Arc::new(client));
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("EventsourcingDB container did not become ready within 60s")
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
        let base_url = if params.uri.is_empty() {
            "http://localhost:4000".to_string()
        } else {
            params.uri.clone()
        };
        let api_token = params.options.get("api_token").cloned().unwrap_or_default();
        let url: Url = base_url
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid URL: {}", e))?;
        let client = Client::new(url, api_token);
        let mut guard = self.client.lock().await;
        *guard = Some(Arc::new(client));
        Ok(())
    }

    async fn append(&self, evt: EventData) -> Result<()> {
        let client = self.get_client().await?;
        let data: serde_json::Value = serde_json::from_slice(&evt.payload).unwrap_or_else(|_| {
            json!({"raw": serde_json::Value::String(
                String::from_utf8_lossy(&evt.payload).to_string()
            )})
        });
        let event = EventCandidate::builder()
            .source("https://bench.eventsourcingdb.io".to_string())
            .subject(format!("/{}", evt.stream))
            .ty(if evt.event_type.contains('.') {
                evt.event_type
            } else {
                format!("io.eventsourcingdb.bench.{}", evt.event_type)
            })
            .data(data)
            .build();
        client
            .write_events(vec![event], vec![])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let client = self.get_client().await?;
        let subject = format!("/{}", req.stream);
        let mut stream = client
            .read_events(&subject, None)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        let mut out = Vec::new();
        let mut offset: u64 = 0;
        while let Some(result) = stream.next().await {
            let event = result.map_err(|e| anyhow::anyhow!("{}", e))?;
            let current_offset = offset;
            offset += 1;
            if let Some(from) = req.from_offset {
                if current_offset < from {
                    continue;
                }
            }
            let payload = serde_json::to_vec(event.data())?;
            let timestamp_ms = event.time().timestamp_millis() as u64;
            out.push(ReadEvent {
                offset: current_offset,
                event_type: event.ty().to_string(),
                payload,
                timestamp_ms,
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
        let client = self.get_client().await?;
        let t0 = std::time::Instant::now();
        client.ping().await.map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(t0.elapsed())
    }
}

pub struct EventsourcingDbFactory;

impl bench_core::AdapterFactory for EventsourcingDbFactory {
    fn name(&self) -> &'static str {
        "eventsourcingdb"
    }
    fn create(&self) -> Box<dyn EventStoreAdapter> {
        Box::new(EventsourcingDbAdapter::new())
    }
}
