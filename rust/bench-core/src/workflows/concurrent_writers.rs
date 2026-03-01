use crate::adapter::{EventData, EventStoreAdapter};
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

/// Workflow that performs concurrent unconditional appends to streams
pub struct ConcurrentWritersWorkflow {
    config: Workload,
    seed: u64,
}

impl ConcurrentWritersWorkflow {
    pub fn new(config: Workload, seed: u64) -> Self {
        Self { config, seed }
    }
}

#[async_trait]
impl WorkflowStrategy for ConcurrentWritersWorkflow {
    async fn execute(
        &self,
        writer_adapters: Vec<Arc<dyn EventStoreAdapter>>,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, Vec<RawSample>)> {
        let samples = Arc::new(Mutex::new(Vec::<RawSample>::with_capacity(100_000)));
        let mut set = JoinSet::new();

        // Start one task per writer - each uses its own adapter instance
        for (i, adapter) in writer_adapters.into_iter().enumerate() {
            let samples = samples.clone();
            let wl = self.config.clone();
            let seed = self.seed + (i as u64);

            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = wl.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(wl.streams.unique_streams.max(1));
                let mut rec = LatencyRecorder::new();
                let size = wl.event_size_bytes;

                while Instant::now() < end_at {
                    let stream_idx = if use_heavy_tail && rng.gen_bool(0.2) {
                        // 20% of the time, pick from a small hot set starting at 0
                        rng.gen_range(0..hot_set)
                    } else {
                        rng.gen_range(0..wl.streams.unique_streams)
                    };

                    let evt = EventData {
                        stream: format!("stream-{}", stream_idx),
                        event_type: "test".to_string(),
                        payload: vec![0u8; size],
                        tags: vec![],
                    };

                    let t0 = Instant::now();
                    let ok = adapter.append(evt).await.is_ok();
                    let dt = t0.elapsed();
                    let now = Instant::now();

                    // Only record samples during the measurement window (after warmup, before cooldown)
                    if now >= measurement_start && now <= measurement_end {
                        rec.record(dt);
                        let mut s = samples.lock().await;
                        s.push(RawSample {
                            t_ms: now_ms(),
                            op: "append".to_string(),
                            latency_us: dt.as_micros() as u64,
                            ok,
                        });
                    }
                }
                rec
            });
        }

        let mut overall = LatencyRecorder::new();
        let mut events_written: u64 = 0;
        while let Some(res) = set.join_next().await {
            let rec = res.expect("join");
            overall.hist.add(&rec.hist).unwrap();
            events_written += rec.hist.len() as u64;
        }

        let samples_vec = samples.lock().await.clone();
        Ok((overall, events_written, samples_vec))
    }
}

/// Factory for creating ConcurrentWritersWorkflow instances
pub struct ConcurrentWritersFactory;

impl WorkflowFactory for ConcurrentWritersFactory {
    fn name(&self) -> &'static str {
        "concurrent_writers"
    }

    fn create(&self, config: &Workload, seed: u64) -> Result<Box<dyn WorkflowStrategy>> {
        Ok(Box::new(ConcurrentWritersWorkflow::new(
            config.clone(),
            seed,
        )))
    }
}
