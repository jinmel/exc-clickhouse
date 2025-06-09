use super::super::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_high_concurrency_task_spawning() {
    let mut manager = TaskManager::<u32>::new();
    const TASK_COUNT: u32 = 50; // Reduced for CI/test environments

    // Spawn many tasks concurrently
    let mut task_ids = Vec::new();
    for i in 0..TASK_COUNT {
        let task_id = manager.spawn_task(format!("concurrent_task_{}", i), move || async move {
            // Small random delay to simulate work
            let delay = (i % 10) + 1;
            sleep(Duration::from_millis(delay as u64)).await;
            Ok(i)
        });
        task_ids.push(task_id);
    }

    // Wait for all tasks to complete
    let mut completed_count = 0;
    while completed_count < TASK_COUNT {
        if manager.wait_for_next_completion().await.is_some() {
            completed_count += 1;
        }
    }

    // Verify all tasks completed successfully
    assert_eq!(completed_count, TASK_COUNT);

    // Check final statistics
    let stats = manager.get_stats();
    assert_eq!(stats.total_tasks, TASK_COUNT as usize);

    // Verify all tasks are in completed state
    let completed_tasks = manager.get_tasks_by_state(TaskState::Completed);
    assert_eq!(completed_tasks.len(), TASK_COUNT as usize);
}
