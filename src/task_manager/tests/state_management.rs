use super::super::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_task_handle_state_management() {
    let task_id = TaskId::new();
    let join_handle = tokio::spawn(async { Ok::<(), eyre::Error>(()) });
    let handle = TaskHandle::new(task_id, "test_task".to_string(), join_handle);

    // Test state transitions
    assert_eq!(handle.state(), TaskState::Pending);
    assert!(handle.set_state(TaskState::Running));
    assert_eq!(handle.state(), TaskState::Running);
    assert!(handle.set_state(TaskState::Completed));
    assert_eq!(handle.state(), TaskState::Completed);

    // Test invalid transition
    assert!(!handle.set_state(TaskState::Running));
    assert_eq!(handle.state(), TaskState::Completed);
}

#[tokio::test]
async fn test_task_handle_restart_logic() {
    let task_id = TaskId::new();
    let join_handle = tokio::spawn(async { Ok::<(), eyre::Error>(()) });
    let handle = TaskHandle::new(task_id, "test_task".to_string(), join_handle);

    // Set to failed state
    handle.set_state(TaskState::Failed);

    // Should be restartable initially (creation time is far enough in the past)
    sleep(Duration::from_millis(150)).await;
    assert!(handle.should_restart(3, Duration::from_millis(100)));

    // Record restart
    handle.record_restart();
    assert_eq!(handle.restart_count(), 1);

    // Should not restart immediately due to time constraint
    assert!(!handle.should_restart(3, Duration::from_millis(100)));

    // Wait and try again
    sleep(Duration::from_millis(150)).await;
    assert!(handle.should_restart(3, Duration::from_millis(100)));

    // Test restart count limit
    handle.record_restart(); // count = 2
    handle.record_restart(); // count = 3
    sleep(Duration::from_millis(150)).await;

    // At this point restart_count = 3, max_restarts = 3, so should_restart should return false
    assert_eq!(handle.restart_count(), 3);
    assert!(!handle.should_restart(3, Duration::from_millis(100))); // Should fail due to max restarts reached
}

#[tokio::test]
async fn test_task_manager_cancellation() {
    let mut manager: TaskManager<String> = TaskManager::new();

    // Spawn a long-running task
    let task_id = manager.spawn_task("long_task".to_string(), || async {
        sleep(Duration::from_secs(10)).await;
        Ok("completed".to_string())
    });

    // Verify task was registered
    assert!(manager.get_task(&task_id).is_some());

    // Cancel the task
    let result = manager.cancel_task(&task_id).await;
    assert!(result.is_ok());

    // Verify task was marked as cancelled (but remains in registry)
    if let Some(task_handle) = manager.get_task(&task_id) {
        assert_eq!(task_handle.state(), TaskState::Cancelled);
    } else {
        panic!("Task should remain in registry after cancellation");
    }
}
