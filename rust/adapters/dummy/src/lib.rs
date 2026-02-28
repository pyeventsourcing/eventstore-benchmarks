use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{ConnectionParams, EventData, EventStoreAdapter, ReadEvent, ReadRequest};
use std::time::Duration;

pub struct DummyAdapter;

impl DummyAdapter {
    pub fn new(_params: &ConnectionParams) -> Result<Self> {
        Ok(Self)
    }
}

#[async_trait]
impl EventStoreAdapter for DummyAdapter {
    async fn append(&self, _evt: EventData) -> Result<()> {
        tokio::time::sleep(Duration::from_micros(10)).await;
        Ok(())
    }
    async fn read(&self, _req: ReadRequest) -> Result<Vec<ReadEvent>> {
        Ok(vec![])
    }
    async fn ping(&self) -> Result<Duration> {
        Ok(Duration::from_millis(1))
    }
}

pub struct DummyFactory;

impl bench_core::AdapterFactory for DummyFactory {
    fn name(&self) -> &'static str {
        "dummy"
    }
    fn create(&self, params: &ConnectionParams) -> Result<Box<dyn EventStoreAdapter>> {
        Ok(Box::new(DummyAdapter::new(params)?))
    }
    // No container manager - dummy adapter doesn't use containers
}
