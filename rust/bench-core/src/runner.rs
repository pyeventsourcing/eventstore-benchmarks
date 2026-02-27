use crate::adapter::{ConnectionParams, EventData, EventStoreAdapter};
use crate::metrics::{now_ms, LatencyRecorder, RawSample, RunMetrics, Summary};
use crate::workload::Workload;
use anyhow::Result;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub adapter_name: String,
    pub conn: ConnectionParams,
    pub seed: u64,
}

pub async fn run_workload(
    adapter: Arc<dyn EventStoreAdapter>,
    wl: Workload,
    opts: RunOptions,
) -> Result<RunMetrics> {
    adapter.connect(&opts.conn).await?;

    let end_at = Instant::now() + Duration::from_secs(wl.duration_seconds);

    let samples = Arc::new(Mutex::new(Vec::<RawSample>::with_capacity(100_000)));
    let mut set = JoinSet::new();

    for i in 0..wl.writers {
        let adapter = adapter.clone();
        let samples = samples.clone();
        let wl = wl.clone();
        let seed = opts.seed + (i as u64);

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
                rec.record(dt);
                let mut s = samples.lock().await;
                s.push(RawSample {
                    t_ms: now_ms(),
                    op: "append".to_string(),
                    latency_us: dt.as_micros() as u64,
                    ok,
                });
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

    let dur_s = wl.duration_seconds as f64;
    let summary = Summary {
        workload: wl.name,
        adapter: opts.adapter_name,
        writers: wl.writers,
        events_written,
        events_read: 0,
        duration_s: dur_s,
        throughput_eps: (events_written as f64) / dur_s.max(0.001),
        latency: overall.to_stats(),
    };

    let samples_vec = samples.lock().await.clone();
    Ok(RunMetrics { summary, samples: samples_vec })
}
