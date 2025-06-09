// Task Manager Module - A comprehensive async task management system
//
// This module provides a robust task management system with the following features:
// - JoinSet-based concurrent task execution
// - Comprehensive error handling and restart policies
// - Circuit breaker pattern for failure isolation
// - Structured logging with correlation IDs
// - Graceful shutdown with multiple phases
// - Task state tracking and metrics collection
// - Registry-based task lookup and management

pub mod types;
pub mod logging;
pub mod metrics;
pub mod config;
pub mod circuit_breaker;
pub mod restart;
pub mod handle;
pub mod registry;
pub mod manager;

// Re-export commonly used types for convenience
pub use types::{
    TaskId, TaskState, TaskResult, TaskCompletion, PendingRestart,
    FailureType, CircuitBreakerState, ShutdownPhase, TaskManagerStats,
    TaskFn, AsyncTaskFn
};

pub use logging::{CorrelationId, TaskLoggingContext};
pub use metrics::TaskMetrics;
pub use config::{TaskManagerConfig, ShutdownConfig, ShutdownStatus};
pub use circuit_breaker::CircuitBreaker;
pub use restart::{RestartPolicy, RestartUtils};
pub use handle::{TaskHandle, TaskError};
pub use registry::TaskRegistry;
pub use manager::TaskManager;

// Convenience type aliases
pub type DefaultTaskManager = TaskManager<()>;
pub type StringTaskManager = TaskManager<String>;

#[cfg(test)]
mod tests; 