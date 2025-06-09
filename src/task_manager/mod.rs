//! Task Manager Module
//!
//! A comprehensive task management system with advanced features including:
//! - Task lifecycle management with spawn, monitor, and cancellation
//! - Configurable restart policies with exponential backoff
//! - Circuit breaker pattern for fault tolerance
//! - Structured logging with correlation IDs
//! - Performance metrics and telemetry
//! - Graceful shutdown with multiple phases
//! - Thread-safe task registry and state management

// Suppress warnings for this comprehensive module since it's designed to be reusable
// and not all features may be used in every application
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]

// Module declarations
mod circuit_breaker;
mod config;
mod handle;
mod logging;
mod manager;
mod metrics;
mod registry;
mod restart;
mod types;

// Re-export public API
pub use circuit_breaker::CircuitBreaker;
pub use config::{ShutdownConfig, ShutdownStatus, TaskManagerConfig};
pub use handle::{TaskError, TaskHandle};
pub use logging::{CorrelationId, TaskLoggingContext};
pub use manager::TaskManager;
pub use metrics::TaskMetrics;
pub use registry::TaskRegistry;
pub use restart::{RestartPolicy, RestartUtils};
pub use types::{
    AsyncTaskFn, CircuitBreakerState, FailureType, IntoTaskResult, PendingRestart, ShutdownPhase, TaskCompletion,
    TaskFn, TaskId, TaskManagerStats, TaskResult, TaskState,
};

// Convenience type aliases for common use cases
pub type DefaultTaskManager = TaskManager<()>;
pub type StringTaskManager = TaskManager<String>;

#[cfg(test)]
mod tests;
