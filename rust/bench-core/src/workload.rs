use crate::adapter::StoreManager;
use crate::metrics::{LatencyRecorder, RawSample};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Instant;


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

/// A workload_type defines how to execute a specific benchmark scenario
/// against a store managed by a StoreManager.
#[async_trait]
pub trait Workload: Send + Sync {
    /// Prepare the workload_type.
    async fn prepare(
        &self,
        _store: &dyn StoreManager,
    ) -> Result<()> {
        Ok(())
    }

    /// Execute the workload_type.
    /// Returns a tuple of (LatencyRecorder, events_written, events_read, samples).
    async fn execute(
        &self,
        store: &dyn StoreManager,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)>;

    /// Workload name
    fn name(&self) -> String;

    /// Number of writers used in this workload_type (for reporting)
    fn writers(&self) -> usize;

    /// Number of readers used in this workload_type (for reporting)
    fn readers(&self) -> usize;

    /// Workload duration in seconds
    fn duration_seconds(&self) -> u64;
}

/// Factory for creating workload_type instances from YAML configuration
pub trait WorkloadFactory: Send + Sync {
    /// Name of the workload_type type (used for selection in CLI or automatically detected)
    fn name(&self) -> &'static str;

    /// Create a workload_type instance from the given YAML configuration
    fn create(&self, yaml_config: &str, seed: u64) -> Result<Box<dyn Workload>>;
}
