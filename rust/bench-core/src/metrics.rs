use hdrhistogram::Histogram;
use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize)]
pub struct RawSample {
    pub t_ms: u128,
    pub op: String,
    pub latency_us: u64,
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyStats {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub workload: String,
    pub adapter: String,
    pub writers: usize,
    pub events_written: u64,
    pub events_read: u64,
    pub duration_s: f64,
    pub throughput_eps: f64,
    pub latency: LatencyStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunMetrics {
    pub summary: Summary,
    pub samples: Vec<RawSample>,
}

pub struct LatencyRecorder {
    pub hist: Histogram<u64>,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self { hist: Histogram::new(3).expect("hist")} // 3 sigfigs
    }
    pub fn record(&mut self, dur: Duration) {
        let us = dur.as_micros() as u64;
        let _ = self.hist.record(us.max(1));
    }
    pub fn to_stats(&self) -> LatencyStats {
        LatencyStats {
            p50_ms: self.hist.value_at_quantile(0.50) as f64 / 1000.0,
            p95_ms: self.hist.value_at_quantile(0.95) as f64 / 1000.0,
            p99_ms: self.hist.value_at_quantile(0.99) as f64 / 1000.0,
            p999_ms: self.hist.value_at_quantile(0.999) as f64 / 1000.0,
        }
    }
}

pub fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}
