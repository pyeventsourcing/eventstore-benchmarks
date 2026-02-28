use crate::adapter::{AdapterFactory, ConnectionParams, EventData, EventStoreAdapter};
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
    factory: Arc<dyn AdapterFactory>,
    wl: Workload,
    opts: RunOptions,
) -> Result<RunMetrics> {
    // Create a shared adapter for setup/teardown and stats collection
    let shared_adapter: Arc<dyn EventStoreAdapter> = Arc::from(factory.create());

    // Start container.
    println!("Starting {} container...", opts.adapter_name);
    let setup_start = std::time::Instant::now();

    shared_adapter.setup().await?;

    let startup_time_s = setup_start.elapsed().as_secs_f64();
    println!(
        "{} container is ready after {:.2} seconds",
        opts.adapter_name,
        startup_time_s
    );

    // Add 1s warmup + 1s cooldown to the actual run time
    // This prevents startup glitches and incomplete final buckets in plots
    let warmup_duration = Duration::from_secs(1);
    let cooldown_duration = Duration::from_secs(1);
    let total_run_duration = Duration::from_secs(wl.duration_seconds) + warmup_duration + cooldown_duration;

    let start_at = Instant::now();
    let measurement_start = start_at + warmup_duration;
    let measurement_end = measurement_start + Duration::from_secs(wl.duration_seconds);
    let end_at = start_at + total_run_duration;

    let samples = Arc::new(Mutex::new(Vec::<RawSample>::with_capacity(100_000)));
    let mut set = JoinSet::new();

    // Start a background task to periodically collect container stats during the workload
    let adapter_for_stats = shared_adapter.clone();
    let stats_handle = tokio::spawn(async move {
        let mut cpu_samples = Vec::new();
        let mut mem_samples = Vec::new();

        // Start sampling immediately as workload begins
        while Instant::now() < end_at {
            if let Ok(metrics) = adapter_for_stats.collect_container_metrics().await {
                if let Some(cpu) = metrics.avg_cpu_percent {
                    cpu_samples.push(cpu);
                }
                if let Some(mem) = metrics.avg_memory_bytes {
                    mem_samples.push(mem);
                }
            }
            // Sample every 1 second to capture short workloads
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        (cpu_samples, mem_samples)
    });

    // Start one client per writer - each gets its own adapter instance
    for i in 0..wl.writers {
        let factory = factory.clone();
        let samples = samples.clone();
        let wl = wl.clone();
        let opts = opts.clone();
        let seed = opts.seed + (i as u64);

        set.spawn(async move {
            // Create a dedicated adapter instance for this writer
            let adapter: Arc<dyn EventStoreAdapter> = Arc::from(factory.create());

            // Connect the adapter
            if let Err(e) = adapter.connect(&opts.conn).await {
                eprintln!("Writer {} failed to connect: {}", i, e);
                return LatencyRecorder::new();
            }

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

    // Wait for stats collection to finish
    let (cpu_samples, mem_samples) = stats_handle.await.unwrap_or_default();

    // Collect final container metrics (for image size)
    let mut container_metrics = shared_adapter.collect_container_metrics().await.unwrap_or_default();
    container_metrics.startup_time_s = startup_time_s;

    // Compute CPU and memory statistics from samples collected during workload
    // Average gives overall resource usage, peak shows maximum demand
    if !cpu_samples.is_empty() {
        let avg_cpu = cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64;
        let peak_cpu = cpu_samples.iter().copied().fold(0.0f64, f64::max);
        container_metrics.avg_cpu_percent = Some(avg_cpu);
        container_metrics.peak_cpu_percent = Some(peak_cpu);
    }

    if !mem_samples.is_empty() {
        let avg_mem = mem_samples.iter().sum::<u64>() / mem_samples.len() as u64;
        let peak_mem = *mem_samples.iter().max().unwrap_or(&0);
        container_metrics.avg_memory_bytes = Some(avg_mem);
        container_metrics.peak_memory_bytes = Some(peak_mem);
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
        container: container_metrics,
    };

    let samples_vec = samples.lock().await.clone();
    let metrics = RunMetrics {
        summary,
        samples: samples_vec,
    };

    shared_adapter.teardown().await?;

    Ok(metrics)
}
