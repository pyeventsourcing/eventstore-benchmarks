use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{ConnectionParams, EventData, EventStoreAdapter, ReadEvent, ReadRequest};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;
use umadb_client::UmaDBClient;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBQuery, DCBQueryItem};
use uuid::Uuid;

pub struct UmaDbAdapter {
    client: Arc<Mutex<Option<Arc<umadb_client::AsyncUmaDBClient>>>>,
}

impl UmaDbAdapter {
    pub fn new() -> Self {
        Self { client: Arc::new(Mutex::new(None)) }
    }
}

#[async_trait]
impl EventStoreAdapter for UmaDbAdapter {
    async fn connect(&self, params: &ConnectionParams) -> Result<()> {
        let mut builder = UmaDBClient::new(params.uri.clone());
        if let Some(v) = params.options.get("api_key") {
            builder = builder.api_key(v.clone());
        }
        if let Some(v) = params.options.get("ca_path") {
            builder = builder.ca_path(v.clone());
        }
        if let Some(v) = params.options.get("batch_size") {
            if let Ok(n) = v.parse::<u32>() {
                builder = builder.batch_size(n);
            }
        }
        let client = builder.connect_async().await?;
        let mut guard = self.client.lock().await;
        *guard = Some(Arc::new(client));
        Ok(())
    }

    async fn append(&self, evt: EventData) -> Result<()> {
        let mut tags = evt.tags.clone();
        tags.push(format!("stream:{}", evt.stream));
        let dcb_evt = DCBEvent {
            event_type: evt.event_type,
            tags,
            data: evt.payload,
            uuid: Some(Uuid::new_v4()),
        };
        let client_arc = {
            let guard = self.client.lock().await;
            guard.clone().ok_or_else(|| anyhow::anyhow!("UmaDB client not connected"))?
        };
        let _pos: u64 = client_arc.append(vec![dcb_evt], None).await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let client_arc = {
            let guard = self.client.lock().await;
            guard.clone().ok_or_else(|| anyhow::anyhow!("UmaDB client not connected"))?
        };
        let query = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![format!("stream:{}", req.stream)],
            }],
        };
        let mut rr = client_arc
            .read(Some(query), req.from_offset, false, req.limit.map(|l| l as u32), false)
            .await?;
        let mut out = Vec::new();
        let mut got: u64 = 0;
        while let Some(item) = rr.next().await {
            match item {
                Ok(se) => {
                    out.push(ReadEvent {
                        offset: se.position,
                        event_type: se.event.event_type.clone(),
                        payload: se.event.data.clone(),
                        timestamp_ms: 0,
                    });
                    got += 1;
                    if let Some(lim) = req.limit { if got >= lim { break; } }
                }
                Err(_status) => break,
            }
        }
        Ok(out)
    }

    async fn ping(&self) -> Result<Duration> {
        let client_arc = {
            let guard = self.client.lock().await;
            guard.clone().ok_or_else(|| anyhow::anyhow!("UmaDB client not connected"))?
        };
        let t0 = std::time::Instant::now();
        let _ = client_arc.head().await?; // Option<u64>
        Ok(t0.elapsed())
    }
}

pub struct UmaDbFactory;
impl bench_core::AdapterFactory for UmaDbFactory {
    fn name(&self) -> &'static str { "umadb" }
    fn create(&self) -> Box<dyn EventStoreAdapter> { Box::new(UmaDbAdapter::new()) }
}
