use super::super::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_task_manager_shutdown() {
    let mut manager = TaskManager::new();

    // Spawn a long-running task
    let _task_id = manager.spawn_task("long_task".to_string(), || {
        Box::pin(async {
            sleep(Duration::from_secs(10)).await;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
    });

    // Initiate shutdown
    let shutdown_result =
        tokio::time::timeout(Duration::from_millis(100), manager.shutdown()).await;

    // Should complete within timeout due to force termination
    assert!(shutdown_result.is_ok());
    assert!(manager.is_shutdown_complete());
}

#[tokio::test]
async fn test_enhanced_shutdown_phases() {
    let mut config = TaskManagerConfig::default();
    config.shutdown_config.graceful_timeout = Duration::from_millis(100);
    config.shutdown_config.force_timeout = Duration::from_millis(50);

    let mut manager = TaskManager::<()>::with_config(config);

    // Initial state should be running
    assert_eq!(manager.get_shutdown_phase(), ShutdownPhase::Running);
    assert!(!manager.is_shutting_down());
    assert!(!manager.is_shutdown_complete());

    // Spawn a long-running task
    let _task_id = manager.spawn_task("long_task".to_string(), || {
        Box::pin(async {
            sleep(Duration::from_millis(200)).await;
            Ok(())
        })
    });

    // Start shutdown
    let shutdown_handle = tokio::spawn(async move { manager.shutdown().await });

    // Give shutdown time to progress through phases
    sleep(Duration::from_millis(300)).await;

    // Shutdown should complete
    assert!(shutdown_handle.await.is_ok());
}

#[tokio::test]
async fn test_shutdown_status_tracking() {
    let mut config = TaskManagerConfig::default();
    config.shutdown_config.graceful_timeout = Duration::from_millis(50);
    config.shutdown_config.force_timeout = Duration::from_millis(50);

    let mut manager = TaskManager::<()>::with_config(config);

    // Spawn multiple tasks
    let _task1 = manager.spawn_task("task1".to_string(), || {
        Box::pin(async {
            sleep(Duration::from_millis(100)).await;
            Ok(())
        })
    });
    let _task2 = manager.spawn_task("task2".to_string(), || {
        Box::pin(async {
            sleep(Duration::from_millis(100)).await;
            Ok(())
        })
    });

    // Initial status
    let initial_status = manager.get_shutdown_status();
    assert_eq!(initial_status.phase, ShutdownPhase::Running);
    assert_eq!(initial_status.initial_task_count, 0);

    // Start shutdown
    let shutdown_result = manager.shutdown().await;
    assert!(shutdown_result.is_ok());

    // Check final status
    let final_status = manager.get_shutdown_status();
    assert_eq!(final_status.phase, ShutdownPhase::Complete);
    assert_eq!(final_status.initial_task_count, 2);
    // Check that shutdown_started_at was set (non-zero)
    assert!(final_status.shutdown_started_at > 0);
    assert!(manager.is_shutdown_complete());
}

#[tokio::test]
async fn test_shutdown_during_high_load() {
    let mut config = TaskManagerConfig::default();
    config.shutdown_config.graceful_timeout = Duration::from_millis(100);

    let mut manager = TaskManager::<()>::with_config(config);

    // Spawn many tasks with varying durations
    for i in 0..20 {
        let duration = Duration::from_millis(50 + (i % 10) * 10);
        manager.spawn_task(format!("load_task_{}", i), move || {
            Box::pin(async move {
                sleep(duration).await;
                Ok(())
            })
        });
    }

    // Start shutdown immediately while tasks are running
    let shutdown_start = std::time::Instant::now();
    let shutdown_result = manager.shutdown().await;
    let shutdown_duration = shutdown_start.elapsed();

    // Verify shutdown completed successfully
    assert!(shutdown_result.is_ok());
    assert!(manager.is_shutdown_complete());

    // Shutdown should complete within reasonable time
    assert!(shutdown_duration < Duration::from_millis(300));

    // Verify shutdown status
    let status = manager.get_shutdown_status();
    assert_eq!(status.phase, ShutdownPhase::Complete);
    assert_eq!(status.initial_task_count, 20);
}
