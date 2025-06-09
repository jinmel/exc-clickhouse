use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

use crate::task_manager::types::{TaskId, TaskState, FailureType};
use crate::task_manager::logging::{CorrelationId, TaskLoggingContext};
use crate::task_manager::metrics::TaskMetrics;
use crate::task_manager::restart::RestartUtils;

/// Enhanced error information for failed tasks with context preservation
#[derive(Debug, Clone)]
pub struct TaskError {
    pub message: String,
    pub timestamp: u64,
    pub error_type: String,
    pub correlation_id: CorrelationId,
    pub error_chain: Vec<String>,
    pub context: HashMap<String, String>,
    pub failure_category: FailureType,
}

impl TaskError {
    pub fn new(error: &dyn std::error::Error, correlation_id: CorrelationId) -> Self {
        let mut error_chain = Vec::new();
        let mut current_error = Some(error);
        
        // Build error chain
        while let Some(err) = current_error {
            error_chain.push(err.to_string());
            current_error = err.source();
        }
        
        let failure_category = RestartUtils::classify_failure(error);
        
        Self {
            message: error.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            error_type: std::any::type_name_of_val(error).to_string(),
            correlation_id,
            error_chain,
            context: HashMap::new(),
            failure_category,
        }
    }
    
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.context.insert(key, value);
        self
    }
    
    pub fn log_structured(&self, task_name: &str) {
        tracing::error!(
            correlation_id = %self.correlation_id,
            task_name = task_name,
            error_type = %self.error_type,
            failure_category = ?self.failure_category,
            timestamp = self.timestamp,
            error_chain = ?self.error_chain,
            context = ?self.context,
            "Task failed with error: {}", self.message
        );
    }
}

/// Task handle with comprehensive state management and metadata tracking
#[derive(Debug)]
pub struct TaskHandle<T> {
    pub id: TaskId,
    pub name: String,
    pub correlation_id: CorrelationId,
    state: Arc<AtomicU32>, // Using u32 to represent TaskState enum
    creation_time: u64,
    last_restart_time: Arc<AtomicU64>,
    restart_count: Arc<AtomicU32>,
    error_history: Arc<parking_lot::Mutex<Vec<TaskError>>>,
    metrics: Arc<parking_lot::Mutex<TaskMetrics>>,
    join_handle: Option<JoinHandle<T>>,
}

impl<T> TaskHandle<T> {
    /// Create a new task handle with join handle
    pub fn new(id: TaskId, name: String, join_handle: JoinHandle<T>) -> Self {
        let correlation_id = CorrelationId::from_task_id(&id);
        let creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        tracing::info!(
            correlation_id = %correlation_id,
            task_id = %id,
            task_name = %name,
            creation_time = creation_time,
            "Task handle created"
        );
        
        Self {
            id,
            name,
            correlation_id,
            state: Arc::new(AtomicU32::new(TaskState::Pending as u32)),
            creation_time,
            last_restart_time: Arc::new(AtomicU64::new(0)),
            restart_count: Arc::new(AtomicU32::new(0)),
            error_history: Arc::new(parking_lot::Mutex::new(Vec::new())),
            metrics: Arc::new(parking_lot::Mutex::new(TaskMetrics::new())),
            join_handle: Some(join_handle),
        }
    }
    
    /// Create a new managed task handle without join handle (for JoinSet management)
    pub fn new_managed(id: TaskId, name: String) -> Self {
        let correlation_id = CorrelationId::from_task_id(&id);
        let creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        tracing::info!(
            correlation_id = %correlation_id,
            task_id = %id,
            task_name = %name,
            creation_time = creation_time,
            "Managed task handle created"
        );
        
        Self {
            id,
            name,
            correlation_id,
            state: Arc::new(AtomicU32::new(TaskState::Pending as u32)),
            creation_time,
            last_restart_time: Arc::new(AtomicU64::new(0)),
            restart_count: Arc::new(AtomicU32::new(0)),
            error_history: Arc::new(parking_lot::Mutex::new(Vec::new())),
            metrics: Arc::new(parking_lot::Mutex::new(TaskMetrics::new())),
            join_handle: None,
        }
    }
    
    /// Get current task state
    pub fn state(&self) -> TaskState {
        let state_value = self.state.load(Ordering::SeqCst);
        match state_value {
            0 => TaskState::Pending,
            1 => TaskState::Running,
            2 => TaskState::Completed,
            3 => TaskState::Failed,
            4 => TaskState::Cancelled,
            _ => TaskState::Pending, // Default fallback
        }
    }
    
    /// Set task state with validation
    pub fn set_state(&self, new_state: TaskState) -> bool {
        let current_state = self.state();
        
        // Validate state transitions
        let valid_transition = match (current_state, new_state) {
            // From Pending
            (TaskState::Pending, TaskState::Running) => true,
            (TaskState::Pending, TaskState::Cancelled) => true,
            // From Running
            (TaskState::Running, TaskState::Completed) => true,
            (TaskState::Running, TaskState::Failed) => true,
            (TaskState::Running, TaskState::Cancelled) => true,
            // From Failed
            (TaskState::Failed, TaskState::Running) => true, // Restart
            (TaskState::Failed, TaskState::Cancelled) => true,
            // From Completed
            (TaskState::Completed, TaskState::Cancelled) => true, // Allow cancellation of completed tasks
            // Same state
            (current, new) if current == new => true,
            // All other transitions are invalid
            _ => false,
        };
        
        if valid_transition {
            self.state.store(new_state as u32, Ordering::SeqCst);
            
            tracing::debug!(
                correlation_id = %self.correlation_id,
                task_id = %self.id,
                task_name = %self.name,
                old_state = %current_state,
                new_state = %new_state,
                "Task state transition"
            );
            
            // Update metrics based on state change
            if new_state == TaskState::Completed {
                let mut metrics = self.metrics.lock();
                let duration = (SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64)
                    .saturating_sub(self.creation_time);
                metrics.record_execution(duration);
            }
            
            true
        } else {
            tracing::warn!(
                correlation_id = %self.correlation_id,
                task_id = %self.id,
                task_name = %self.name,
                current_state = %current_state,
                attempted_state = %new_state,
                "Invalid task state transition attempted"
            );
            false
        }
    }
    
    /// Record a task failure with error details
    pub fn record_failure(&self, error: &dyn std::error::Error) {
        let task_error = TaskError::new(error, self.correlation_id.clone())
            .with_context("task_name".to_string(), self.name.clone())
            .with_context("restart_count".to_string(), self.restart_count().to_string());
        
        task_error.log_structured(&self.name);
        
        let mut error_history = self.error_history.lock();
        error_history.push(task_error);
        
        // Keep only the last 10 errors to prevent unbounded growth
        if error_history.len() > 10 {
            error_history.remove(0);
        }
        
        // Update metrics
        let mut metrics = self.metrics.lock();
        metrics.record_error();
        
        self.set_state(TaskState::Failed);
    }
    
    /// Record a task restart
    pub fn record_restart(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        self.last_restart_time.store(now, Ordering::SeqCst);
        self.restart_count.fetch_add(1, Ordering::SeqCst);
        
        // Update metrics
        let mut metrics = self.metrics.lock();
        metrics.record_restart();
        
        tracing::info!(
            correlation_id = %self.correlation_id,
            task_id = %self.id,
            task_name = %self.name,
            restart_count = self.restart_count(),
            restart_time = now,
            "Task restart recorded"
        );
        
        self.set_state(TaskState::Running);
    }
    
    /// Get restart count
    pub fn restart_count(&self) -> u32 {
        self.restart_count.load(Ordering::SeqCst)
    }
    
    /// Get last restart time
    pub fn last_restart_time(&self) -> u64 {
        self.last_restart_time.load(Ordering::SeqCst)
    }
    
    /// Get creation time
    pub fn creation_time(&self) -> u64 {
        self.creation_time
    }
    
    /// Get error history
    pub fn error_history(&self) -> Vec<TaskError> {
        self.error_history.lock().clone()
    }
    
    /// Check if task should be restarted based on policy
    pub fn should_restart(&self, max_restarts: u32, min_time_between_restarts: Duration) -> bool {
        let restart_count = self.restart_count();
        
        // Check restart count limit
        if max_restarts > 0 && restart_count >= max_restarts {
            return false;
        }
        
        // Check minimum time between restarts
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last_restart = self.last_restart_time();
        let time_since_restart = now.saturating_sub(last_restart);
        
        time_since_restart >= min_time_between_restarts.as_millis() as u64
    }
    
    /// Take ownership of the join handle
    pub fn take_join_handle(&mut self) -> Option<JoinHandle<T>> {
        self.join_handle.take()
    }
    
    /// Check if task is running
    pub fn is_running(&self) -> bool {
        self.state() == TaskState::Running
    }
    
    /// Check if task is completed
    pub fn is_completed(&self) -> bool {
        self.state() == TaskState::Completed
    }
    
    /// Check if task is failed
    pub fn is_failed(&self) -> bool {
        self.state() == TaskState::Failed
    }
    
    /// Get metadata summary for debugging
    pub fn metadata_summary(&self) -> String {
        format!(
            "Task[{}] '{}' - State: {}, Restarts: {}, Created: {}ms ago",
            self.id,
            self.name,
            self.state(),
            self.restart_count(),
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64)
                .saturating_sub(self.creation_time)
        )
    }
    
    /// Get current metrics
    pub fn get_metrics(&self) -> TaskMetrics {
        self.metrics.lock().clone()
    }
    
    /// Create logging context for operations
    pub fn create_logging_context(&self, operation: &str) -> TaskLoggingContext {
        TaskLoggingContext::new(self.id.clone(), self.name.clone(), operation.to_string())
    }
} 