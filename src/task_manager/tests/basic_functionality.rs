use super::super::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_task_manager_basic_functionality() {
    let mut manager = TaskManager::new();
    
    // Spawn a simple task
    let task_id = manager.spawn_task("test_task".to_string(), || async {
        sleep(Duration::from_millis(10)).await;
        Ok::<String, Box<dyn std::error::Error + Send + Sync>>("completed".to_string())
    });
    
    // Verify task was registered
    assert!(manager.get_task(&task_id).is_some());
    assert_eq!(manager.active_task_count(), 1);
    
    // Wait for completion
    if let Some(completion) = manager.wait_for_next_completion().await {
        assert_eq!(completion.task_id, task_id);
        assert!(completion.result.is_ok());
    }
}

#[tokio::test]
async fn test_task_manager_blocking_tasks() {
    let mut manager: TaskManager<i32> = TaskManager::new();
    
    // Spawn a blocking task
    let task_id = manager.spawn_blocking_task("blocking_task".to_string(), || {
        std::thread::sleep(Duration::from_millis(10));
        Ok(42)
    });
    
    // Wait for task completion
    if let Some(completion) = manager.wait_for_next_completion().await {
        assert_eq!(completion.task_id, task_id);
        assert_eq!(completion.task_name, "blocking_task");
        assert!(completion.result.is_ok());
        assert_eq!(completion.result.unwrap(), 42);
    } else {
        panic!("Expected task completion");
    }
}

#[tokio::test]
async fn test_task_manager_capacity_limits() {
    let config = TaskManagerConfig {
        max_concurrent_tasks: 2,
        ..Default::default()
    };
    let manager: TaskManager<String> = TaskManager::with_config(config);
    
    // Initially not at capacity
    assert!(!manager.is_at_capacity());
    
    // Note: In a real test, we'd spawn tasks and verify capacity limits
    // For now, we just test the capacity checking logic
    assert_eq!(manager.active_task_count(), 0);
}

#[tokio::test]
async fn test_task_manager_stats() {
    let manager: TaskManager<String> = TaskManager::new();
    
    let stats = manager.get_stats();
    assert_eq!(stats.total_tasks, 0);
    assert!(!stats.is_at_capacity);
    assert!(!stats.is_shutting_down);
    assert_eq!(stats.circuit_breaker_state, CircuitBreakerState::Closed);
    assert_eq!(stats.circuit_breaker_failure_count, 0);
    assert_eq!(stats.pending_restarts, 0);
    
    // Verify all task states are represented
    assert!(stats.tasks_by_state.contains_key(&TaskState::Pending));
    assert!(stats.tasks_by_state.contains_key(&TaskState::Running));
    assert!(stats.tasks_by_state.contains_key(&TaskState::Completed));
    assert!(stats.tasks_by_state.contains_key(&TaskState::Failed));
    assert!(stats.tasks_by_state.contains_key(&TaskState::Cancelled));
}

#[tokio::test]
async fn test_task_registry_operations() {
    let mut registry = TaskRegistry::new();
    let task_id = TaskId::new();
    let join_handle = tokio::spawn(async { Ok::<(), eyre::Error>(()) });
    let handle = TaskHandle::new(task_id.clone(), "test_task".to_string(), join_handle);
    
    // Register task
    assert!(registry.register(handle).is_ok());
    assert_eq!(registry.len(), 1);
    
    // Retrieve task
    assert!(registry.get(&task_id).is_some());
    assert!(registry.get_by_name("test_task").is_some());
    
    // Remove task
    assert!(registry.remove(&task_id).is_some());
    assert_eq!(registry.len(), 0);
}

#[tokio::test]
async fn test_correlation_id_generation() {
    let task_id = TaskId::new();
    let correlation_id = CorrelationId::from_task_id(&task_id);
    
    // Correlation ID should be derived from task ID
    assert_eq!(task_id.to_string(), correlation_id.to_string());
    
    // Different task IDs should generate different correlation IDs
    let task_id2 = TaskId::new();
    let correlation_id2 = CorrelationId::from_task_id(&task_id2);
    assert_ne!(correlation_id.to_string(), correlation_id2.to_string());
}

#[tokio::test]
async fn test_task_logging_context() {
    let task_id = TaskId::new();
    let task_name = "test_task".to_string();
    let operation = "test_operation".to_string();
    
    let context = TaskLoggingContext::new(task_id.clone(), task_name.clone(), operation.clone());
    
    assert_eq!(context.task_id.to_string(), task_id.to_string());
    assert_eq!(context.task_name, task_name);
    assert_eq!(context.operation, operation);
    assert_eq!(context.correlation_id.to_string(), task_id.to_string());
    
    // Test elapsed time calculation
    sleep(Duration::from_millis(10)).await;
    assert!(context.elapsed_ms() >= 10);
    
    // Test span creation
    let span = context.create_span();
    // Note: span might be disabled in test environment, just verify it's created
    // We can't easily test span properties in unit tests, so just verify it doesn't panic
    drop(span);
}

#[tokio::test]
async fn test_task_metrics_tracking() {
    let mut metrics = TaskMetrics::new();
    
    // Initial state
    assert_eq!(metrics.execution_duration_ms, 0);
    assert_eq!(metrics.restart_count, 0);
    assert_eq!(metrics.error_count, 0);
    assert!(metrics.last_success_time.is_none());
    
    // Record execution
    metrics.record_execution(1500);
    assert_eq!(metrics.execution_duration_ms, 1500);
    assert!(metrics.last_success_time.is_some());
    
    // Record error
    metrics.record_error();
    assert_eq!(metrics.error_count, 1);
    
    // Record restart
    metrics.record_restart();
    assert_eq!(metrics.restart_count, 1);
} 