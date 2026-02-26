use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bench_core::adapter::{ConnectionParams, EventData, EventStoreAdapter, ReadEvent, ReadRequest};
use bench_core::{run_workload, RunOptions, StreamsConfig, Workload};

struct DummyAdapter;

#[async_trait]
impl EventStoreAdapter for DummyAdapter {
    async fn connect(&self, _params: &ConnectionParams) -> anyhow::Result<()> {
        Ok(())
    }
    async fn append(&self, _evt: EventData) -> anyhow::Result<()> {
        // Simulate very small latency
        tokio::time::sleep(Duration::from_micros(10)).await;
        Ok(())
    }
    async fn read(&self, _req: ReadRequest) -> anyhow::Result<Vec<ReadEvent>> {
        Ok(vec![])
    }
    async fn ping(&self) -> anyhow::Result<Duration> {
        Ok(Duration::from_millis(1))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_workload_smoke() {
    let adapter = Arc::new(DummyAdapter);
    let wl = Workload {
        name: "test".to_string(),
        duration_seconds: 1,
        writers: 2,
        event_size_bytes: 64,
        streams: StreamsConfig { distribution: "uniform".to_string(), unique_streams: 100 },
        conflict_rate: None,
        durability: None,
    };
    let opts = RunOptions {
        adapter_name: "dummy".to_string(),
        conn: ConnectionParams { uri: String::new(), options: Default::default() },
        seed: 42,
    };

    let res = run_workload(adapter, wl, opts).await.expect("run");
    assert!(res.summary.events_written > 0);
    assert!(res.summary.throughput_eps > 0.0);
    assert!(!res.samples.is_empty());
}
