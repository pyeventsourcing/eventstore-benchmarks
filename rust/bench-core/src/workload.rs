use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsConfig {
    pub distribution: String, // e.g., "zipf", "uniform"
    pub unique_streams: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupConfig {
    /// Number of events to prepopulate during setup phase
    pub events_to_prepopulate: u64,
    /// Number of streams to distribute prepopulated events across
    #[serde(default)]
    pub prepopulate_streams: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workload {
    pub name: String,
    pub duration_seconds: u64,
    #[serde(default)]
    pub writers: usize,
    #[serde(default)]
    pub readers: usize,
    pub event_size_bytes: usize,
    pub streams: StreamsConfig,
    #[serde(default)]
    pub setup: Option<SetupConfig>,
    #[serde(default)]
    pub conflict_rate: Option<f64>,
    #[serde(default)]
    pub durability: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkloadFile;

impl WorkloadFile {
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Workload> {
        let s = std::fs::read_to_string(path)?;
        let wl: Workload = serde_yaml::from_str(&s)?;
        Ok(wl)
    }
}
