use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// Unique identifier for tasks
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Get the inner UUID for correlation ID purposes
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Task execution state with atomic operations for thread safety
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::Pending => write!(f, "pending"),
            TaskState::Running => write!(f, "running"),
            TaskState::Completed => write!(f, "completed"),
            TaskState::Failed => write!(f, "failed"),
            TaskState::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Failure classification for restart decision making
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureType {
    /// Transient failures that should be retried
    Transient,
    /// Permanent failures that should not be retried
    Permanent,
    /// Resource-related failures (e.g., out of memory)
    Resource,
    /// Network-related failures
    Network,
    /// Configuration or setup failures
    Configuration,
    /// Unknown failure type (default to transient)
    Unknown,
}

/// Circuit breaker state for failure management
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    /// Circuit is closed, allowing normal operation
    Closed,
    /// Circuit is open, blocking operations due to failures
    Open,
    /// Circuit is half-open, testing if service has recovered
    HalfOpen,
}

/// Shutdown phases for coordinated task termination
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownPhase {
    /// Normal operation, not shutting down
    Running,
    /// Stop accepting new tasks
    StopAccepting,
    /// Waiting for running tasks to complete
    WaitingForTasks,
    /// Force terminating remaining tasks
    ForceTerminating,
    /// Shutdown complete
    Complete,
}

impl std::fmt::Display for ShutdownPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownPhase::Running => write!(f, "running"),
            ShutdownPhase::StopAccepting => write!(f, "stop_accepting"),
            ShutdownPhase::WaitingForTasks => write!(f, "waiting_for_tasks"),
            ShutdownPhase::ForceTerminating => write!(f, "force_terminating"),
            ShutdownPhase::Complete => write!(f, "complete"),
        }
    }
}

/// Type aliases for task functions and results
pub type TaskResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type TaskFn<T> = Box<dyn FnOnce() -> TaskResult<T> + Send + 'static>;
pub type AsyncTaskFn<T> = Box<
    dyn FnOnce() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TaskResult<T>> + Send + 'static>,
        > + Send
        + 'static,
>;

/// Task completion information
#[derive(Debug)]
pub struct TaskCompletion<T> {
    pub task_id: TaskId,
    pub task_name: String,
    pub result: TaskResult<T>,
    pub duration: Duration,
}

/// Pending restart information for delayed task restarts
pub struct PendingRestart<T> {
    pub task_id: TaskId,
    pub task_name: String,
    pub task_fn: Box<
        dyn FnOnce() -> std::pin::Pin<
                Box<dyn std::future::Future<Output = TaskResult<T>> + Send + 'static>,
            > + Send
            + 'static,
    >,
    pub restart_time: u64, // Timestamp when restart should occur
    pub restart_count: u32,
}

impl<T> std::fmt::Debug for PendingRestart<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRestart")
            .field("task_id", &self.task_id)
            .field("task_name", &self.task_name)
            .field("task_fn", &"<function>")
            .field("restart_time", &self.restart_time)
            .field("restart_count", &self.restart_count)
            .finish()
    }
}

/// Statistics for task manager monitoring
#[derive(Debug)]
pub struct TaskManagerStats {
    pub total_tasks: usize,
    pub tasks_by_state: HashMap<TaskState, usize>,
    pub is_at_capacity: bool,
    pub is_shutting_down: bool,
    pub circuit_breaker_state: CircuitBreakerState,
    pub circuit_breaker_failure_count: u32,
    pub pending_restarts: usize,
    pub shutdown_status: Option<super::config::ShutdownStatus>,
}
