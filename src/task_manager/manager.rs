use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{Instrument, field};

use crate::task_manager::circuit_breaker::CircuitBreaker;
use crate::task_manager::config::{ShutdownStatus, TaskManagerConfig};
use crate::task_manager::handle::TaskHandle;
use crate::task_manager::logging::{CorrelationId, TaskLoggingContext};
use crate::task_manager::registry::TaskRegistry;
use crate::task_manager::restart::RestartUtils;
use crate::task_manager::types::ShutdownPhase;
use crate::task_manager::types::{
    PendingRestart, TaskCompletion, TaskId, TaskManagerStats, TaskResult, TaskState,
};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

/// Type alias for a restartable task function
///
/// This represents a function that can be called to create a new instance of a task.
/// The function returns a pinned boxed future that will eventually produce a TaskResult<T>.
/// This allows tasks to be restarted by calling the stored function again.
pub type TaskFunction<T> = Arc<
    dyn Fn() -> Pin<Box<dyn std::future::Future<Output = TaskResult<T>> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

/// Type alias for a restartable task function with cancellation token support
///
/// This represents a function that can be called to create a new instance of a task
/// with access to a cancellation token for graceful shutdown.
pub type CancellableTaskFunction<T> = Arc<
    dyn Fn(CancellationToken) -> Pin<Box<dyn std::future::Future<Output = TaskResult<T>> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

/// Main task manager with JoinSet-based task tracking
pub struct TaskManager<T> {
    /// JoinSet for managing concurrent tasks
    join_set: JoinSet<TaskCompletion<T>>,
    /// Registry for task metadata and handles
    registry: TaskRegistry<TaskCompletion<T>>,
    /// Configuration for task management
    config: TaskManagerConfig,
    /// Shutdown signal sender
    shutdown_tx: watch::Sender<bool>,
    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,
    /// Task counter for generating sequential IDs
    task_counter: Arc<AtomicU64>,
    /// Circuit breaker for handling persistent failures
    circuit_breaker: CircuitBreaker,
    /// Pending restart queue for delayed task restarts
    restart_queue: Arc<parking_lot::Mutex<Vec<PendingRestart<T>>>>,
    /// Shutdown status tracking
    shutdown_status: Arc<parking_lot::Mutex<ShutdownStatus>>,

    /// Storage for task functions to enable task restarts
    task_fns: HashMap<TaskId, TaskFunction<T>>,
    /// Storage for cancellation tokens to enable graceful shutdown
    cancellation_tokens: HashMap<TaskId, CancellationToken>,
}

impl<T: Send + 'static> TaskManager<T> {
    /// Create a new TaskManager with default configuration
    pub fn new() -> Self {
        Self::with_config(TaskManagerConfig::default())
    }

    /// Create a new TaskManager with custom configuration
    pub fn with_config(config: TaskManagerConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let circuit_breaker = CircuitBreaker::new(
            config.default_restart_policy.circuit_breaker_threshold,
            config.default_restart_policy.circuit_breaker_timeout,
        );

        Self {
            join_set: JoinSet::new(),
            registry: TaskRegistry::new(),
            config,
            shutdown_tx,
            shutdown_rx,
            task_counter: Arc::new(AtomicU64::new(0)),
            circuit_breaker,
            restart_queue: Arc::new(parking_lot::Mutex::new(Vec::new())),
            shutdown_status: Arc::new(parking_lot::Mutex::new(ShutdownStatus::new())),
            task_fns: HashMap::new(),
            cancellation_tokens: HashMap::new(),
        }
    }

    /// Create an instrumented task future that produces TaskCompletion<T>
    fn make_task<F>(
        &self,
        task_id: TaskId,
        task_name: String,
        task_fn: F,
    ) -> Pin<Box<dyn std::future::Future<Output = TaskCompletion<T>> + Send + 'static>>
    where
        F: Fn() -> Pin<Box<dyn std::future::Future<Output = TaskResult<T>> + Send + 'static>>
            + Send
            + 'static,
    {
        let correlation_id = CorrelationId::from_task_id(&task_id);
        let task_id_for_async = task_id.clone();
        let task_name_for_async = task_name.clone();
        let task_name_for_completion = task_name.clone();

        Box::pin(async move {
            let span = tracing::info_span!(
                "task_execution",
                correlation_id = %correlation_id,
                task_id = %task_id_for_async,
                task_name = %task_name_for_async,
                operation = "task_execution",
                result = field::Empty,
                duration_ms = field::Empty,
                error_type = field::Empty,
            );

            let span_for_recording = span.clone();

            async move {
                tracing::info!("Starting task execution");

                let start_time = std::time::Instant::now();
                let result = task_fn().await;
                let duration = start_time.elapsed();

                // Record span fields based on result
                span_for_recording.record("duration_ms", duration.as_millis());

                match &result {
                    Ok(_) => {
                        span_for_recording.record("result", "success");
                        tracing::info!(
                            duration_ms = duration.as_millis(),
                            "Task completed successfully"
                        );
                    }
                    Err(e) => {
                        span_for_recording.record("result", "error");
                        span_for_recording
                            .record("error_type", std::any::type_name_of_val(e.as_ref()));
                        tracing::error!(
                            duration_ms = duration.as_millis(),
                            error = %e,
                            error_type = std::any::type_name_of_val(e.as_ref()),
                            "Task failed"
                        );
                    }
                }

                TaskCompletion {
                    task_id: task_id_for_async,
                    task_name: task_name_for_completion,
                    result,
                    duration,
                }
            }
            .instrument(span)
            .await
        })
    }

    /// Spawn a new async task with automatic registration
    pub fn spawn_task<F, N>(&mut self, name: N, task_fn: F) -> TaskId
    where
        F: Fn() -> Pin<Box<dyn std::future::Future<Output = TaskResult<T>> + Send + 'static>>
            + Send
            + Sync
            + Clone
            + 'static,
        N: std::fmt::Display,
    {
        // Check if shutdown is in progress and new tasks should be rejected
        {
            let shutdown_status = self.shutdown_status.lock();
            if shutdown_status.phase != ShutdownPhase::Running
                && self.config.shutdown_config.reject_new_tasks
            {
                tracing::warn!(
                    name = %name,
                    shutdown_phase = %shutdown_status.phase,
                    "Task spawn rejected due to shutdown in progress"
                );
                // Return a dummy task ID for now - in production this should return a Result
                return TaskId::new();
            }
        }

        // Check capacity limits
        if self.is_at_capacity() {
            tracing::warn!(
                name = %name,
                active_tasks = self.active_task_count(),
                max_tasks = self.config.max_concurrent_tasks,
                "Task spawn rejected due to capacity limit"
            );
            // Return a dummy task ID for now - in production this should return a Result
            return TaskId::new();
        }

        let task_id = TaskId::new();
        let name = name.to_string(); // Convert to String once

        // Create logging context
        let _logging_context =
            TaskLoggingContext::new(task_id.clone(), name.clone(), "task_execution".to_string());

        // Store the task function for potential restarts
        self.task_fns
            .insert(task_id.clone(), Arc::new(task_fn.clone()));

        // Create the instrumented task using our new method
        let fut = self.make_task(task_id.clone(), name.clone(), task_fn.clone());

        // Spawn the task
        let _abort_handle = self.join_set.spawn(fut);

        // Create TaskHandle for tracking (JoinSet manages the actual execution)
        let handle = TaskHandle::new_managed(task_id.clone(), name.clone());

        // Set task to running state since it's being spawned
        handle.set_state(TaskState::Running);

        // Register the task
        if let Err(e) = self.registry.register(handle) {
            tracing::warn!(
                task_id = %task_id,
                error = %e,
                "Failed to register task in registry"
            );
        }

        tracing::debug!(
            task_id = %task_id,
            task_name = %name,
            total_tasks = self.registry.len(),
            "Task spawned and registered"
        );

        task_id
    }

    /// Spawn a new async task with cancellation token support for graceful shutdown
    pub fn spawn_cancellable_task<F, N>(&mut self, name: N, task_fn: F) -> TaskId
    where
        F: Fn(CancellationToken) -> Pin<Box<dyn std::future::Future<Output = TaskResult<T>> + Send + 'static>>
            + Send
            + Sync
            + Clone
            + 'static,
        N: std::fmt::Display,
    {
        // Check if shutdown is in progress and new tasks should be rejected
        {
            let shutdown_status = self.shutdown_status.lock();
            if shutdown_status.phase != ShutdownPhase::Running
                && self.config.shutdown_config.reject_new_tasks
            {
                tracing::warn!(
                    name = %name,
                    shutdown_phase = %shutdown_status.phase,
                    "Cancellable task spawn rejected due to shutdown in progress"
                );
                return TaskId::new();
            }
        }

        // Check capacity limits
        if self.is_at_capacity() {
            tracing::warn!(
                name = %name,
                active_tasks = self.active_task_count(),
                max_tasks = self.config.max_concurrent_tasks,
                "Cancellable task spawn rejected due to capacity limit"
            );
            return TaskId::new();
        }

        let task_id = TaskId::new();
        let name = name.to_string();

        // Create and store cancellation token
        let cancellation_token = CancellationToken::new();
        self.cancellation_tokens.insert(task_id.clone(), cancellation_token.clone());

        // Create logging context
        let _logging_context =
            TaskLoggingContext::new(task_id.clone(), name.clone(), "task_execution".to_string());

        // Create a task wrapper that calls the cancellable function with the token
        let task_fn_wrapper = {
            let token = cancellation_token.clone();
            let task_fn = task_fn.clone();
            move || task_fn(token.clone())
        };

        // Store the task function for potential restarts (as a regular TaskFunction)
        self.task_fns.insert(task_id.clone(), Arc::new(move || {
            let token = cancellation_token.clone();
            let task_fn = task_fn.clone();
            task_fn(token)
        }));

        // Create the instrumented task using our new method
        let fut = self.make_task(task_id.clone(), name.clone(), task_fn_wrapper);

        // Spawn the task
        let _abort_handle = self.join_set.spawn(fut);

        // Create TaskHandle for tracking
        let handle = TaskHandle::new_managed(task_id.clone(), name.clone());
        handle.set_state(TaskState::Running);

        // Register the task
        if let Err(e) = self.registry.register(handle) {
            tracing::warn!(
                task_id = %task_id,
                error = %e,
                "Failed to register cancellable task in registry"
            );
        }

        tracing::debug!(
            task_id = %task_id,
            task_name = %name,
            total_tasks = self.registry.len(),
            "Cancellable task spawned and registered"
        );

        task_id
    }

    /// Get task handle by ID
    pub fn get_task(&self, task_id: &TaskId) -> Option<&TaskHandle<TaskCompletion<T>>> {
        self.registry.get(task_id)
    }

    /// Get task handle by name
    pub fn get_task_by_name(&self, name: &str) -> Option<&TaskHandle<TaskCompletion<T>>> {
        self.registry.get_by_name(name)
    }

    /// Get all task IDs
    pub fn get_task_ids(&self) -> Vec<TaskId> {
        self.registry.task_ids()
    }

    /// Get tasks by state
    pub fn get_tasks_by_state(&self, state: TaskState) -> Vec<&TaskHandle<TaskCompletion<T>>> {
        self.registry.tasks_by_state(state)
    }

    /// Get the number of active tasks (excludes completed/failed/cancelled tasks)
    pub fn active_task_count(&self) -> usize {
        self.registry.tasks_by_state(TaskState::Running).len()
            + self.registry.tasks_by_state(TaskState::Pending).len()
    }

    /// Check if the task manager is at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.active_task_count() >= self.config.max_concurrent_tasks
    }

    /// Cancel a specific task by ID
    pub async fn cancel_task(&mut self, task_id: &TaskId) -> Result<(), String> {
        if let Some(handle) = self.registry.get(task_id) {
            // For JoinSet-managed tasks, we abort all tasks and let the JoinSet handle cleanup
            // Individual task cancellation in JoinSet requires tracking abort handles separately
            handle.set_state(TaskState::Cancelled);

            tracing::info!(
                task_id = %task_id,
                task_name = %handle.name,
                "Task marked as cancelled (JoinSet will handle cleanup)"
            );

            Ok(())
        } else {
            Err(format!("Task with ID {task_id} not found"))
        }
    }

    /// Cancel all tasks
    pub async fn cancel_all_tasks(&mut self) {
        let task_ids: Vec<TaskId> = self.registry.task_ids();
        let initial_count = task_ids.len();

        for task_id in task_ids {
            if let Err(e) = self.cancel_task(&task_id).await {
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    "Failed to cancel task"
                );
            }
        }

        // Abort all remaining tasks in JoinSet
        self.join_set.abort_all();

        // Update shutdown status if we're shutting down
        if self.is_shutting_down() {
            let mut status = self.shutdown_status.lock();
            status.tasks_cancelled += initial_count;
        }

        tracing::info!(cancelled_tasks = initial_count, "All tasks cancelled");
    }

    /// Wait for the next task to complete and handle the result
    pub async fn wait_for_next_completion(&mut self) -> Option<TaskCompletion<T>> {
        loop {
            tokio::select! {
                // Check for task completion
                result = self.join_set.join_next() => {
                    if let Some(join_result) = result {
                        match join_result {
                            Ok(task_completion) => {
                                // Update task state based on completion result
                                let task_id = task_completion.task_id.clone();
                                if let Some(handle) = self.registry.get(&task_id) {
                                    match &task_completion.result {
                                        Ok(_) => {
                                            handle.set_state(TaskState::Completed);
                                            self.circuit_breaker.record_success();
                                        }
                                        Err(e) => {
                                            handle.record_failure(&**e);
                                            self.circuit_breaker.record_failure();
                                        }
                                    }

                                    // Clean up cancellation token for completed task
                                    self.cancellation_tokens.remove(&task_completion.task_id);
                                }
                                return Some(task_completion);
                            }
                            Err(join_error) => {
                                tracing::error!(
                                    error = %join_error,
                                    "Task join failed"
                                );
                                return None;
                            }
                        }
                    }
                    else {
                        return None;
                    }
                }

                // Check for shutdown signal
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("Shutdown signal received");
                        return None;
                    }
                    // Continue waiting if it's not a shutdown signal - loop continues
                }
            }
        }
    }

    /// Run the task manager event loop
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Task manager started");

        while let Some(completion) = self.wait_for_next_completion().await {
            let task_id = completion.task_id;
            let task_name = completion.task_name.clone();

            // Handle task completion
            if let Some(handle) = self.registry.get(&task_id) {
                match completion.result {
                    Ok(_) => {
                        handle.set_state(TaskState::Completed);
                        self.circuit_breaker.record_success();

                        tracing::info!(
                            task_id = %task_id,
                            task_name = %task_name,
                            duration_ms = completion.duration.as_millis(),
                            "Task completed successfully"
                        );
                    }
                    Err(e) => {
                        handle.record_failure(&*e);
                        self.circuit_breaker.record_failure();

                        tracing::error!(
                            task_id = %task_id,
                            task_name = %task_name,
                            duration_ms = completion.duration.as_millis(),
                            error = %e,
                            "Task failed"
                        );

                        // Check if task should be restarted using new comprehensive logic
                        let failure_type = RestartUtils::classify_failure(&*e);
                        let should_restart = RestartUtils::should_restart_task(
                            &*e,
                            handle.restart_count(),
                            handle.last_restart_time(),
                            &self.config.default_restart_policy,
                            Some(&self.circuit_breaker),
                        );

                        tracing::debug!(
                            task_id = %task_id,
                            error_type = ?failure_type,
                            restart_count = handle.restart_count(),
                            should_restart = should_restart,
                            error_msg = %e,
                            "Restart decision analysis"
                        );

                        if should_restart {
                            let restart_delay = RestartUtils::calculate_backoff_delay(
                                handle.restart_count(),
                                &self.config.default_restart_policy,
                            );

                            let _restart_time = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64
                                + restart_delay.as_millis() as u64;

                            tracing::info!(
                                task_id = %task_id,
                                task_name = %task_name,
                                restart_count = handle.restart_count(),
                                delay_ms = restart_delay.as_millis(),
                                "Scheduling task restart with exponential backoff"
                            );

                            // Restart the task using the stored function and proper instrumentation
                            handle.record_restart();
                            let task_fn = self.task_fns.get(&task_id).unwrap().clone();
                            let fut =
                                self.make_task(task_id.clone(), task_name.clone(), move || {
                                    task_fn()
                                });
                            self.join_set.spawn(fut);

                            // Don't remove task from registry since we're restarting it
                        } else {
                            tracing::warn!(
                                task_id = %task_id,
                                task_name = %task_name,
                                restart_count = handle.restart_count(),
                                "Task restart denied by restart policy"
                            );
                            // Clean up completed task from registry since not restarting
                            self.registry.remove(&task_id);
                            // Also clean up cancellation token 
                            self.cancellation_tokens.remove(&task_id);
                        }
                    }
                }
            } else {
                // No more tasks or shutdown signal received
                break;
            }
        }

        tracing::info!("Task manager stopped");
        Ok(())
    }

    /// Initiate graceful shutdown of the task manager with enhanced phase tracking
    pub async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Initiating enhanced graceful shutdown");

        // Initialize shutdown status
        let initial_task_count = self.active_task_count();
        {
            let mut status = self.shutdown_status.lock();
            status.phase = ShutdownPhase::StopAccepting;
            status.shutdown_started_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            status.phase_started_at = status.shutdown_started_at;
            status.initial_task_count = initial_task_count;
            status.tasks_remaining = initial_task_count;
        }

        tracing::info!(
            initial_task_count = initial_task_count,
            "Phase 1: Stop accepting new tasks"
        );

        // Send shutdown signal
        if let Err(e) = self.shutdown_tx.send(true) {
            tracing::warn!(error = %e, "Failed to send shutdown signal");
        }

        // Cancel pending restarts if configured
        if self.config.shutdown_config.cancel_pending_restarts {
            let cancelled_restarts = {
                let mut restart_queue = self.restart_queue.lock();
                let count = restart_queue.len();
                restart_queue.clear();
                count
            };
            if cancelled_restarts > 0 {
                tracing::info!(
                    cancelled_restarts = cancelled_restarts,
                    "Cancelled pending restarts during shutdown"
                );
            }
        }

        // Phase 2: Wait for running tasks to complete gracefully
        self.transition_shutdown_phase(ShutdownPhase::WaitingForTasks)
            .await;

        // Trigger all cancellation tokens for graceful shutdown
        let cancellation_count = self.cancellation_tokens.len();
        if cancellation_count > 0 {
            tracing::info!(
                cancellation_tokens = cancellation_count,
                "Triggering cancellation tokens for graceful shutdown"
            );
            for (task_id, token) in &self.cancellation_tokens {
                token.cancel();
                tracing::debug!(
                    task_id = %task_id,
                    "Cancellation token triggered"
                );
            }
        }

        let graceful_timeout = self.config.shutdown_config.graceful_timeout;
        tracing::info!(
            timeout_secs = graceful_timeout.as_secs(),
            "Phase 2: Waiting for tasks to complete gracefully"
        );

        let graceful_result = tokio::select! {
            _ = self.wait_for_all_tasks_completion() => {
                tracing::info!("All tasks completed gracefully");
                true
            }
            _ = tokio::time::sleep(graceful_timeout) => {
                let remaining = self.active_task_count();
                tracing::warn!(
                    remaining_tasks = remaining,
                    timeout_secs = graceful_timeout.as_secs(),
                    "Graceful shutdown timeout reached"
                );
                false
            }
        };

        // Phase 3: Force termination if needed
        if !graceful_result && self.active_task_count() > 0 {
            self.transition_shutdown_phase(ShutdownPhase::ForceTerminating)
                .await;

            let force_timeout = self.config.shutdown_config.force_timeout;
            tracing::warn!(
                remaining_tasks = self.active_task_count(),
                timeout_secs = force_timeout.as_secs(),
                "Phase 3: Force terminating remaining tasks"
            );

            // Cancel all remaining tasks
            self.cancel_all_tasks().await;

            // Wait for force termination to complete or timeout
            tokio::select! {
                _ = self.wait_for_all_tasks_completion() => {
                    tracing::info!("Force termination completed");
                }
                _ = tokio::time::sleep(force_timeout) => {
                    tracing::error!(
                        remaining_tasks = self.active_task_count(),
                        "Force termination timeout reached, some tasks may still be running"
                    );
                }
            }
        }

        // Phase 4: Shutdown complete
        self.transition_shutdown_phase(ShutdownPhase::Complete)
            .await;

        let final_status = {
            let mut status = self.shutdown_status.lock();
            status.completed_gracefully = graceful_result;
            status.tasks_remaining = self.active_task_count();
            status.clone()
        };

        // Clean up all remaining cancellation tokens
        let remaining_tokens = self.cancellation_tokens.len();
        if remaining_tokens > 0 {
            tracing::debug!(
                remaining_tokens = remaining_tokens,
                "Cleaning up remaining cancellation tokens"
            );
            self.cancellation_tokens.clear();
        }

        tracing::info!(
            shutdown_duration_ms = final_status.shutdown_elapsed().as_millis(),
            completed_gracefully = final_status.completed_gracefully,
            tasks_cancelled = final_status.tasks_cancelled,
            remaining_tasks = final_status.tasks_remaining,
            "Shutdown complete"
        );

        Ok(())
    }

    /// Transition to a new shutdown phase with status updates
    async fn transition_shutdown_phase(&mut self, new_phase: ShutdownPhase) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut status = self.shutdown_status.lock();
        let old_phase = status.phase;
        status.phase = new_phase;
        status.phase_started_at = now;
        status.tasks_remaining = self.active_task_count();

        tracing::debug!(
            old_phase = %old_phase,
            new_phase = %new_phase,
            tasks_remaining = status.tasks_remaining,
            "Shutdown phase transition"
        );
    }

    /// Wait for all tasks to complete
    async fn wait_for_all_tasks_completion(&mut self) {
        while !self.join_set.is_empty() {
            if self.wait_for_next_completion().await.is_none() {
                break;
            }
        }
    }

    /// Get shutdown signal receiver for external monitoring
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Check if shutdown has been initiated
    pub fn is_shutting_down(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

    /// Get current shutdown status with detailed information
    pub fn get_shutdown_status(&self) -> ShutdownStatus {
        self.shutdown_status.lock().clone()
    }

    /// Get current shutdown phase
    pub fn get_shutdown_phase(&self) -> ShutdownPhase {
        self.shutdown_status.lock().phase
    }

    /// Check if shutdown is complete
    pub fn is_shutdown_complete(&self) -> bool {
        self.shutdown_status.lock().phase == ShutdownPhase::Complete
    }

    /// Get task manager statistics
    pub fn get_stats(&self) -> TaskManagerStats {
        let tasks_by_state = [
            TaskState::Pending,
            TaskState::Running,
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Cancelled,
        ]
        .iter()
        .map(|&state| (state, self.registry.tasks_by_state(state).len()))
        .collect();

        let shutdown_status = self.shutdown_status.lock().clone();

        TaskManagerStats {
            total_tasks: self.registry.len(),
            tasks_by_state,
            is_at_capacity: self.is_at_capacity(),
            is_shutting_down: self.is_shutting_down(),
            circuit_breaker_state: self.circuit_breaker.state(),
            circuit_breaker_failure_count: self.circuit_breaker.failure_count(),
            pending_restarts: self.restart_queue.lock().len(),
            shutdown_status: Some(shutdown_status),
        }
    }

    /// Get circuit breaker status
    pub fn get_circuit_breaker_status(
        &self,
    ) -> (crate::task_manager::types::CircuitBreakerState, u32) {
        (
            self.circuit_breaker.state(),
            self.circuit_breaker.failure_count(),
        )
    }

    /// Get restart policy configuration
    pub fn get_restart_policy(&self) -> &crate::task_manager::restart::RestartPolicy {
        &self.config.default_restart_policy
    }

    /// Update restart policy configuration
    pub fn update_restart_policy(&mut self, policy: crate::task_manager::restart::RestartPolicy) {
        // Update circuit breaker if needed
        if policy.enable_circuit_breaker {
            self.circuit_breaker = CircuitBreaker::new(
                policy.circuit_breaker_threshold,
                policy.circuit_breaker_timeout,
            );
        }

        self.config.default_restart_policy = policy;
        tracing::info!("Restart policy updated");
    }

    /// Manually reset circuit breaker
    pub fn reset_circuit_breaker(&self) {
        self.circuit_breaker.reset();
        tracing::info!("Circuit breaker manually reset");
    }
}

impl<T> Default for TaskManager<T>
where
    T: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
