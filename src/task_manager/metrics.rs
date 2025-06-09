use std::time::{SystemTime, UNIX_EPOCH};

/// Performance metrics for task execution
#[derive(Debug, Clone)]
pub struct TaskMetrics {
    pub execution_duration_ms: u64,
    pub memory_usage_bytes: Option<u64>,
    pub cpu_time_ms: Option<u64>,
    pub restart_count: u32,
    pub error_count: u32,
    pub last_success_time: Option<u64>,
    pub throughput_ops_per_sec: Option<f64>,
}

impl TaskMetrics {
    pub fn new() -> Self {
        Self {
            execution_duration_ms: 0,
            memory_usage_bytes: None,
            cpu_time_ms: None,
            restart_count: 0,
            error_count: 0,
            last_success_time: None,
            throughput_ops_per_sec: None,
        }
    }

    pub fn record_execution(&mut self, duration_ms: u64) {
        self.execution_duration_ms = duration_ms;
        self.last_success_time = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
    }

    pub fn record_restart(&mut self) {
        self.restart_count += 1;
    }
}

impl Default for TaskMetrics {
    fn default() -> Self {
        Self::new()
    }
}
