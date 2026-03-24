use crate::adapter::StoreManager;
use crate::metrics::{RunMetrics, Summary};
use crate::workloads::{Workload, PerformanceWorkload};
use crate::metrics::ContainerMetrics;
use crate::container_stats::ContainerMonitor;
use anyhow::Result;
use std::time::{Instant};

pub async fn execute_run(
    mut store: Box<dyn StoreManager>,
    workload: &Workload,
) -> Result<RunMetrics> {
    // Start store container
    println!("Starting {} container...", store.name());
    let setup_start = Instant::now();

    store.start().await?;

    let startup_time_s = setup_start.elapsed().as_secs_f64();
    println!(
        "{} container is ready after {:.2} seconds",
        store.name(),
        startup_time_s
    );

    // Initialize container monitoring if possible
    let monitor = if let Some(id) = store.container_id() {
        match ContainerMonitor::new(id) {
            Ok(mut m) => {
                m.start().await;
                Some(m)
            }
            Err(e) => {
                eprintln!("Failed to initialize container monitor: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Extract workload details and execute based on type
    let (workload_name, duration_seconds, writers, readers, overall, events_written, events_read, throughput_samples) = match workload {
        Workload::Performance(perf_workload) => {
            execute_performance_workload(store.as_ref(), perf_workload).await?
        }
        Workload::Durability(dur_workload) => {
            anyhow::bail!("Durability workloads not yet implemented: {}", dur_workload.name());
        }
        Workload::Consistency(cons_workload) => {
            anyhow::bail!("Consistency workloads not yet implemented: {}", cons_workload.name());
        }
        Workload::Operational(op_workload) => {
            anyhow::bail!("Operational workloads not yet implemented: {}", op_workload.name());
        }
    };

    let (dur_s, throughput_eps) = if throughput_samples.len() >= 2 {
        let first_sample = throughput_samples.first().unwrap();
        let last_sample = throughput_samples.last().unwrap();
        let duration = last_sample.elapsed_s - first_sample.elapsed_s;
        let count_delta = last_sample.count - first_sample.count;
        let throughput = (count_delta as f64) / duration.max(0.001);
        (duration, throughput)
    } else {
        let total_ops = events_written + events_read;
        (duration_seconds as f64, (total_ops as f64) / (duration_seconds as f64).max(0.001))
    };

    // Collect container metrics
    let mut container_metrics = ContainerMetrics {
        startup_time_s,
        ..Default::default()
    };

    if let Some(m) = monitor {
        match m.get_image_size().await {
            Ok(size) => container_metrics.image_size_bytes = Some(size),
            Err(e) => eprintln!("Failed to get image size: {}", e),
        }

        match m.stop().await {
            Ok((avg_cpu, peak_cpu, avg_mem, peak_mem)) => {
                container_metrics.avg_cpu_percent = avg_cpu;
                container_metrics.peak_cpu_percent = peak_cpu;
                container_metrics.avg_memory_bytes = avg_mem;
                container_metrics.peak_memory_bytes = peak_mem;
            }
            Err(e) => eprintln!("Failed to stop container monitor: {}", e),
        }
    }

    let summary = Summary {
        workload: workload_name,
        adapter: store.name().to_string(),
        writers,
        readers,
        events_written,
        events_read,
        duration_s: dur_s,
        throughput_eps,
        latency: overall.to_stats(),
        container: container_metrics,
    };

    let metrics = RunMetrics {
        summary,
        throughput_samples,
        sample_rate: 100, // 1-in-100 sampling
        latency_histogram: overall,
    };

    // Stop container
    store.stop().await?;

    Ok(metrics)
}

async fn execute_performance_workload(
    store: &dyn StoreManager,
    workload: &PerformanceWorkload,
) -> Result<(String, u64, usize, usize, crate::metrics::LatencyRecorder, u64, u64, Vec<crate::metrics::ThroughputSample>)> {
    // Prepare the workload
    workload.prepare(store).await?;

    // Warmup and cooldown durations
    let duration_seconds = workload.duration_seconds();

    // Execute the workload
    let (overall, events_written, events_read, throughput_samples) = workload
        .execute(store)
        .await?;

    Ok((
        workload.name().to_string(),
        duration_seconds,
        workload.writers(),
        workload.readers(),
        overall,
        events_written,
        events_read,
        throughput_samples,
    ))
}
