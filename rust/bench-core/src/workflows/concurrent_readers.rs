use crate::adapter::{EventStoreAdapter, ReadRequest};
use crate::metrics::{now_ms, LatencyRecorder, RawSample};
use crate::workload::Workload;
use crate::workflow_strategy::{WorkflowFactory, WorkflowStrategy};
use anyhow::Result;
use async_trait::async_trait;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

/// Workflow that performs concurrent reads from streams
pub struct ConcurrentReadersWorkflow {
    config: Workload,
    seed: u64,
}

impl ConcurrentReadersWorkflow {
    pub fn new(config: Workload, seed: u64) -> Self {
        Self { config, seed }
    }
}

#[async_trait]
impl WorkflowStrategy for ConcurrentReadersWorkflow {
    async fn execute(
        &self,
        reader_adapters: Vec<Arc<dyn EventStoreAdapter>>,
        _writer_adapters: Vec<Arc<dyn EventStoreAdapter>>,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)> {
        let samples = Arc::new(Mutex::new(Vec::<RawSample>::with_capacity(100_000)));
        let mut set = JoinSet::new();

        // Start one task per reader - each uses its own adapter instance
        for (i, adapter) in reader_adapters.into_iter().enumerate() {
            let samples = samples.clone();
            let wl = self.config.clone();
            let seed = self.seed + (i as u64);

            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = wl.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(wl.streams.unique_streams.max(1));
                let mut rec = LatencyRecorder::new();
                let mut total_events_read = 0u64;

                while Instant::now() < end_at {
                    let stream_idx = if use_heavy_tail && rng.gen_bool(0.2) {
                        // 20% of the time, pick from a small hot set starting at 0
                        rng.gen_range(0..hot_set)
                    } else {
                        rng.gen_range(0..wl.streams.unique_streams)
                    };

                    let req = ReadRequest {
                        stream: format!("stream-{}", stream_idx),
                        from_offset: None,
                        limit: Some(100), // Read up to 100 events per request
                    };

                    let t0 = Instant::now();
                    let result = adapter.read(req).await;
                    let dt = t0.elapsed();
                    let now = Instant::now();

                    let (ok, _events_count) = match result {
                        Ok(events) => {
                            let count = events.len() as u64;
                            total_events_read += count;
                            (true, count)
                        }
                        Err(_) => (false, 0),
                    };

                    // Only record samples during the measurement window (after warmup, before cooldown)
                    if now >= measurement_start && now <= measurement_end {
                        rec.record(dt);
                        let mut s = samples.lock().await;
                        s.push(RawSample {
                            t_ms: now_ms(),
                            op: "read".to_string(),
                            latency_us: dt.as_micros() as u64,
                            ok,
                        });
                    }
                }
                (rec, total_events_read)
            });
        }

        let mut overall = LatencyRecorder::new();
        let mut events_read: u64 = 0;
        while let Some(res) = set.join_next().await {
            let (rec, reader_events_read) = res.expect("join");
            overall.hist.add(&rec.hist).unwrap();
            events_read += reader_events_read;
        }

        let samples_vec = samples.lock().await.clone();
        Ok((overall, 0, events_read, samples_vec))
    }
}

/// Factory for creating ConcurrentReadersWorkflow instances
pub struct ConcurrentReadersFactory;

impl WorkflowFactory for ConcurrentReadersFactory {
    fn name(&self) -> &'static str {
        "concurrent_readers"
    }

    fn create(&self, config: &Workload, seed: u64) -> Result<Box<dyn WorkflowStrategy>> {
        Ok(Box::new(ConcurrentReadersWorkflow::new(
            config.clone(),
            seed,
        )))
    }
}
