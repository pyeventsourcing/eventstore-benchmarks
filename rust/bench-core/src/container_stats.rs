use anyhow::Result;
use serde::Deserialize;
use std::process::Command;

#[derive(Debug, Deserialize)]
struct DockerInspectSize {
    #[serde(rename = "SizeRootFs")]
    size_root_fs: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DockerInspect {
    #[serde(rename = "Size")]
    size: Option<DockerInspectSize>,
}

/// Get the container image size in bytes using docker inspect
pub fn get_container_image_size(container_id: &str) -> Result<u64> {
    let output = Command::new("docker")
        .args(&["inspect", "--size", container_id])
        .output()?;

    if !output.status.success() {
        anyhow::bail!("docker inspect failed");
    }

    let json_str = String::from_utf8(output.stdout)?;
    let inspect: Vec<DockerInspect> = serde_json::from_str(&json_str)?;

    if let Some(first) = inspect.first() {
        if let Some(size_info) = &first.size {
            if let Some(size) = size_info.size_root_fs {
                return Ok(size);
            }
        }
    }

    anyhow::bail!("Could not extract image size from docker inspect")
}

#[derive(Debug)]
pub struct ContainerStats {
    pub cpu_percent: f64,
    pub memory_bytes: u64,
}

/// Get current container stats using docker stats --no-stream
pub fn get_container_stats(container_id: &str) -> Result<ContainerStats> {
    let output = Command::new("docker")
        .args(&[
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}}|{{.MemUsage}}",
            container_id,
        ])
        .output()?;

    if !output.status.success() {
        anyhow::bail!("docker stats failed");
    }

    let line = String::from_utf8(output.stdout)?;
    let parts: Vec<&str> = line.trim().split('|').collect();

    if parts.len() != 2 {
        anyhow::bail!("unexpected docker stats format");
    }

    // Parse CPU percentage (format: "1.23%")
    let cpu_str = parts[0].trim().trim_end_matches('%');
    let cpu_percent = cpu_str.parse::<f64>()?;

    // Parse memory (format: "123.4MiB / 7.775GiB")
    let mem_parts: Vec<&str> = parts[1].split('/').collect();
    if mem_parts.is_empty() {
        anyhow::bail!("unexpected memory format");
    }

    let memory_bytes = parse_memory_size(mem_parts[0].trim())?;

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
