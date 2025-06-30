use crate::task_manager::types::TaskId;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{Span, field};
use uuid::Uuid;

/// Correlation ID for tracking operations across task boundaries
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CorrelationId(Uuid);

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl CorrelationId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_task_id(task_id: &TaskId) -> Self {
        // Create a deterministic correlation ID based on the task ID
        // Use the task ID's UUID directly
        Self(task_id.as_uuid())
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Logging context for task operations
#[derive(Debug, Clone)]
pub struct TaskLoggingContext {
    pub correlation_id: CorrelationId,
    pub task_id: TaskId,
    pub task_name: String,
    pub operation: String,
    pub start_time: u64,
}

impl TaskLoggingContext {
    pub fn new(task_id: TaskId, task_name: String, operation: String) -> Self {
        Self {
            correlation_id: CorrelationId::from_task_id(&task_id),
            task_id,
            task_name,
            operation,
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    pub fn elapsed_ms(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now.saturating_sub(self.start_time)
    }

    pub fn create_span(&self) -> Span {
        tracing::info_span!(
            "task_operation",
            correlation_id = %self.correlation_id,
            task_id = %self.task_id,
            task_name = %self.task_name,
            operation = %self.operation,
            start_time = self.start_time,
            elapsed_ms = field::Empty,
            result = field::Empty,
        )
    }
}
