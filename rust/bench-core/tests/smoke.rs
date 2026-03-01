use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bench_core::adapter::{AdapterFactory, ConnectionParams, EventData, EventStoreAdapter, ReadEvent, ReadRequest};
use bench_core::workflows::ConcurrentWritersFactory;
use bench_core::{run_workload, RunOptions, StreamsConfig, Workload, WorkflowFactory};

struct DummyAdapter;

#[async_trait]
impl EventStoreAdapter for DummyAdapter {
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

struct DummyFactory;

impl AdapterFactory for DummyFactory {
    fn name(&self) -> &'static str {
        "dummy"
    }
    fn create(&self, _params: &ConnectionParams) -> anyhow::Result<Box<dyn EventStoreAdapter>> {
        Ok(Box::new(DummyAdapter))
    }
    // No container manager for dummy
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_workload_smoke() {
    let factory: Arc<dyn AdapterFactory> = Arc::new(DummyFactory);
    let wl = Workload {
        name: "test".to_string(),
        duration_seconds: 1,
        writers: 2,
        event_size_bytes: 64,
        streams: StreamsConfig {
            distribution: "uniform".to_string(),
            unique_streams: 100,
        },
        conflict_rate: None,
        durability: None,
    };
    let opts = RunOptions {
        adapter_name: "dummy".to_string(),
        conn: ConnectionParams {
            uri: String::new(),
            options: Default::default(),
        },
        seed: 42,
    };

    let workflow_factory = ConcurrentWritersFactory;
    let workflow = workflow_factory.create(&wl, opts.seed).expect("workflow");

    let res = run_workload(factory, workflow, wl, opts).await.expect("run");
    assert!(res.summary.events_written > 0);
    assert!(res.summary.throughput_eps > 0.0);
    assert!(!res.samples.is_empty());
}
