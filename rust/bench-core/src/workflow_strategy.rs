use crate::adapter::EventStoreAdapter;
use crate::metrics::{LatencyRecorder, RawSample};
use crate::workload::Workload;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Instant;

/// A workflow strategy defines how to execute a specific type of workload
/// against event store adapters. Different strategies can implement different
/// operation patterns (e.g., concurrent writes, read-heavy, transactional).
#[async_trait]
pub trait WorkflowStrategy: Send + Sync {
    /// Execute the workflow using the provided adapters and timing windows.
    /// Returns a tuple of (LatencyRecorder, events_written, samples).
    async fn execute(
        &self,
        writer_adapters: Vec<Arc<dyn EventStoreAdapter>>,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, Vec<RawSample>)>;
}

/// Factory for creating workflow strategy instances from configuration
pub trait WorkflowFactory: Send + Sync {
    /// Name of the workflow (used for selection in CLI)
    fn name(&self) -> &'static str;

    /// Create a workflow strategy instance from the given workload configuration
    fn create(&self, config: &Workload, seed: u64) -> Result<Box<dyn WorkflowStrategy>>;
}
