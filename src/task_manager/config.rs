use crate::task_manager::restart::RestartPolicy;
use crate::task_manager::types::ShutdownPhase;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Configuration for shutdown behavior
#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    /// Timeout for waiting for running tasks to complete gracefully
    pub graceful_timeout: Duration,
    /// Timeout for force termination phase
    pub force_timeout: Duration,
    /// Whether to allow new tasks during shutdown
    pub reject_new_tasks: bool,
    /// Whether to cancel pending restarts during shutdown
    pub cancel_pending_restarts: bool,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            graceful_timeout: Duration::from_secs(30),
            force_timeout: Duration::from_secs(5),
            reject_new_tasks: true,
            cancel_pending_restarts: true,
        }
    }
}

/// Status tracking for shutdown process
#[derive(Debug, Clone)]
pub struct ShutdownStatus {
    /// Current shutdown phase
    pub phase: ShutdownPhase,
    /// Timestamp when shutdown was initiated
    pub shutdown_started_at: u64,
    /// Timestamp when current phase started
    pub phase_started_at: u64,
    /// Number of tasks remaining to complete
    pub tasks_remaining: usize,
    /// Number of tasks that were running when shutdown started
    pub initial_task_count: usize,
    /// Number of tasks cancelled during shutdown
    pub tasks_cancelled: usize,
    /// Whether shutdown completed successfully
    pub completed_gracefully: bool,
}

impl ShutdownStatus {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            phase: ShutdownPhase::Running,
            shutdown_started_at: now,
            phase_started_at: now,
            tasks_remaining: 0,
            initial_task_count: 0,
            tasks_cancelled: 0,
            completed_gracefully: false,
        }
    }

    pub fn shutdown_elapsed(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Duration::from_millis(now.saturating_sub(self.shutdown_started_at))
    }

    pub fn phase_elapsed(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Duration::from_millis(now.saturating_sub(self.phase_started_at))
    }

    pub fn progress(&self) -> f64 {
        if self.initial_task_count == 0 {
            return 1.0;
        }
        let completed = self.initial_task_count.saturating_sub(self.tasks_remaining);
        completed as f64 / self.initial_task_count as f64
    }
}

impl Default for ShutdownStatus {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for task manager behavior
#[derive(Debug, Clone)]
pub struct TaskManagerConfig {
    /// Maximum number of concurrent tasks
    pub max_concurrent_tasks: usize,
    /// Default restart policy
    pub default_restart_policy: RestartPolicy,
    /// Shutdown timeout for graceful termination (deprecated, use shutdown_config)
    pub shutdown_timeout: Duration,
    /// Enhanced shutdown configuration
    pub shutdown_config: ShutdownConfig,
}

impl Default for TaskManagerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 100,
            default_restart_policy: RestartPolicy::default(),
            shutdown_timeout: Duration::from_secs(30),
            shutdown_config: ShutdownConfig::default(),
        }
    }
}
