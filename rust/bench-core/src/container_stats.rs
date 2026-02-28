use anyhow::Result;
use serde::Deserialize;
use std::process::Command;
use std::thread;
use std::time::Duration as StdDuration;

#[derive(Debug, Deserialize)]
struct DockerImageInspect {
    #[serde(rename = "Size")]
    size: Option<u64>,
}

/// Get the container image size in bytes
pub fn get_container_image_size(container_id: &str) -> Result<u64> {
    // First, get the image ID from the container
    let output = Command::new("docker")
        .args(&["inspect", "--format", "{{.Image}}", container_id])
        .output()?;

    if !output.status.success() {
        anyhow::bail!("docker inspect container failed");
    }

    let image_id = String::from_utf8(output.stdout)?.trim().to_string();

    // Then get the image size
    let output = Command::new("docker")
        .args(&["image", "inspect", &image_id])
        .output()?;

    if !output.status.success() {
        anyhow::bail!("docker image inspect failed");
    }

    let json_str = String::from_utf8(output.stdout)?;
    let inspect: Vec<DockerImageInspect> = serde_json::from_str(&json_str)?;

    if let Some(first) = inspect.first() {
        if let Some(size) = first.size {
            return Ok(size);
        }
    }

    anyhow::bail!("Could not extract image size from docker image inspect")
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerStats {
    #[serde(rename = "CPUPerc")]
    cpu_perc: String,
    mem_usage: String,
}

#[derive(Debug)]
pub struct ContainerStats {
    pub cpu_percent: f64,
    pub memory_bytes: u64,
}

/// Get current container stats using docker stats with JSON format
/// Takes multiple samples to get accurate CPU measurements (Docker calculates CPU as deltas)
pub fn get_container_stats(container_id: &str) -> Result<ContainerStats> {
    const NUM_SAMPLES: usize = 1;
    const SAMPLE_DELAY_MS: u64 = 150;

    let mut cpu_samples = Vec::new();
    let mut mem_samples = Vec::new();

    // Take multiple samples for better accuracy
    for i in 0..NUM_SAMPLES {
        let output = Command::new("docker")
            .args(&[
                "stats",
                "--no-stream",
                "--format",
                "{{json .}}",
                container_id,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!("docker stats failed");
        }

        let json_str = String::from_utf8(output.stdout)?;
        let stats: DockerStats = serde_json::from_str(json_str.trim())?;

        // Parse CPU percentage (format: "1.23%")
        let cpu_str = stats.cpu_perc.trim().trim_end_matches('%');
        if let Ok(cpu) = cpu_str.parse::<f64>() {
            cpu_samples.push(cpu);
        }

        // Parse memory (format: "123.4MiB / 7.775GiB")
        let mem_parts: Vec<&str> = stats.mem_usage.split('/').collect();
        if !mem_parts.is_empty() {
            if let Ok(mem_bytes) = parse_memory_size(mem_parts[0].trim()) {
                mem_samples.push(mem_bytes);
            }
        }

        if i < NUM_SAMPLES - 1 {
            thread::sleep(StdDuration::from_millis(SAMPLE_DELAY_MS));
        }
    }

    // Return average CPU and latest memory
    let cpu_percent = if !cpu_samples.is_empty() {
        cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64
    } else {
        0.0
    };

    let memory_bytes = mem_samples.last().copied().unwrap_or(0);

    Ok(ContainerStats {
        cpu_percent,
        memory_bytes,
    })
}

/// Parse memory size string (e.g., "123.4MiB", "1.5GiB") to bytes
fn parse_memory_size(s: &str) -> Result<u64> {
    let s = s.trim();

    // Extract number and unit
    let (num_str, unit) = if s.ends_with("GiB") {
        (&s[..s.len() - 3], "GiB")
    } else if s.ends_with("MiB") {
        (&s[..s.len() - 3], "MiB")
    } else if s.ends_with("KiB") {
        (&s[..s.len() - 3], "KiB")
    } else if s.ends_with("B") {
        (&s[..s.len() - 1], "B")
    } else {
        anyhow::bail!("unknown memory unit in: {}", s);
    };

    let num = num_str.parse::<f64>()?;

    let bytes = match unit {
        "GiB" => (num * 1024.0 * 1024.0 * 1024.0) as u64,
        "MiB" => (num * 1024.0 * 1024.0) as u64,
        "KiB" => (num * 1024.0) as u64,
        "B" => num as u64,
        _ => anyhow::bail!("unknown memory unit: {}", unit),
    };

    Ok(bytes)
}
