use super::super::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_end_to_end_task_lifecycle() {
    let mut manager = TaskManager::<String>::new();
    
    // Spawn multiple tasks with different outcomes
    let success_task = manager.spawn_task("success_task".to_string(), || async {
        sleep(Duration::from_millis(50)).await;
        Ok("success".to_string())
    });
    
    let failure_task = manager.spawn_task("failure_task".to_string(), || async {
        sleep(Duration::from_millis(30)).await;
        Err("simulated failure".into())
    });
    
    let slow_task = manager.spawn_task("slow_task".to_string(), || async {
        sleep(Duration::from_millis(100)).await;
        Ok("slow_success".to_string())
    });
    
    // Wait for all tasks to complete
    let mut completed_tasks = Vec::new();
    for _ in 0..3 {
        if let Some(completion) = manager.wait_for_next_completion().await {
            completed_tasks.push(completion);
        }
    }
    
    // Verify all tasks completed
    assert_eq!(completed_tasks.len(), 3);
    
    // Check task states in registry
    let success_handle = manager.get_task(&success_task).unwrap();
    let failure_handle = manager.get_task(&failure_task).unwrap();
    let slow_handle = manager.get_task(&slow_task).unwrap();
    
    assert_eq!(success_handle.state(), TaskState::Completed);
    assert_eq!(failure_handle.state(), TaskState::Failed);
    assert_eq!(slow_handle.state(), TaskState::Completed);
    
    // Verify metrics were recorded
    let success_metrics = success_handle.get_metrics();
    let failure_metrics = failure_handle.get_metrics();
    
    assert!(success_metrics.last_success_time.is_some());
    assert_eq!(failure_metrics.error_count, 1);
}

#[tokio::test]
async fn test_resource_limit_boundary_testing() {
    let mut config = TaskManagerConfig::default();
    config.max_concurrent_tasks = 5; // Low limit for testing
    
    let mut manager = TaskManager::<()>::with_config(config);
    
    // Try to spawn more tasks than the limit
    let mut task_ids = Vec::new();
    for i in 0..10 {
        let task_id = manager.spawn_task(format!("limited_task_{}", i), || async {
            sleep(Duration::from_millis(100)).await;
            Ok(())
        });
        task_ids.push(task_id);
    }
    
    // Check that capacity limit is enforced
    assert!(manager.is_at_capacity());
    
    // Only 5 tasks should have been actually spawned and registered
    let stats = manager.get_stats();
    assert_eq!(stats.total_tasks, 5);
    
    // Wait for all spawned tasks to complete
    for _ in 0..5 {
        manager.wait_for_next_completion().await;
    }
    
    // Should no longer be at capacity (0 remaining tasks < 5 limit)
    assert!(!manager.is_at_capacity());
    
    // Verify final stats
    let final_stats = manager.get_stats();
    assert_eq!(final_stats.total_tasks, 5);
    assert_eq!(final_stats.tasks_by_state.get(&TaskState::Completed).unwrap_or(&0), &5);
} 