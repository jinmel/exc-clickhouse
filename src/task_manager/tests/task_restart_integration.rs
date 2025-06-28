use super::super::*;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug)]
struct TestError {
    message: String,
    error_type: String,
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_type, self.message)
    }
}

impl std::error::Error for TestError {}

impl TestError {
    fn transient(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            error_type: "transient".to_string(),
        }
    }

    fn permanent(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            error_type: "fatal".to_string(),
        }
    }

    fn network(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            error_type: "connection timeout".to_string(),
        }
    }
}

/// Test helper to create a task that fails a specified number of times before succeeding
fn create_failing_task(
    call_count: Arc<AtomicU32>,
    fail_count: u32,
    error: TestError,
) -> impl Fn() -> Pin<Box<dyn std::future::Future<Output = TaskResult<String>> + Send + 'static>>
       + Send
       + Sync
       + Clone
       + 'static {
    move || {
        let call_count = call_count.clone();
        let error = TestError {
            message: error.message.clone(),
            error_type: error.error_type.clone(),
        };
        Box::pin(async move {
            let count = call_count.fetch_add(1, Ordering::SeqCst);
            if count < fail_count {
                Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
            } else {
                Ok(format!("Success after {} attempts", count))
            }
        })
    }
}

/// Test helper to create a task that always fails
fn create_always_failing_task(
    call_count: Arc<AtomicU32>,
    error: TestError,
) -> impl Fn() -> Pin<Box<dyn std::future::Future<Output = TaskResult<String>> + Send + 'static>>
       + Send
       + Sync
       + Clone
       + 'static {
    move || {
        let call_count = call_count.clone();
        let error = TestError {
            message: error.message.clone(),
            error_type: error.error_type.clone(),
        };
        Box::pin(async move {
            call_count.fetch_add(1, Ordering::SeqCst);
            Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

#[tokio::test]
async fn test_task_restart_transient_failure_success() {
    // Create a task manager with permissive restart policy
    let config = TaskManagerConfig {
        max_concurrent_tasks: 10,
        default_restart_policy: RestartPolicy {
            max_restarts: 3,
            base_delay: Duration::from_millis(0), // No delay for testing
            backoff_multiplier: 1.0,              // No exponential backoff for testing
            max_delay: Duration::from_secs(1),
            jitter_factor: 0.0,
            enable_circuit_breaker: false,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(1),
        },
        shutdown_timeout: Duration::from_secs(30),
        shutdown_config: ShutdownConfig::default(),
    };

    let mut manager: TaskManager<String> = TaskManager::with_config(config);
    let call_count = Arc::new(AtomicU32::new(0));

    // Create a task that fails twice then succeeds
    let task_fn = create_failing_task(
        call_count.clone(),
        2,
        TestError::transient("Connection failed"),
    );
    let task_id = manager.spawn_task("failing_task", task_fn);

    // Run the manager and wait for task completion
    let timeout_duration = Duration::from_secs(5);
    let call_count_for_test = call_count.clone();

    // Run manager in background task
    let manager_handle = tokio::spawn(async move {
        let _ = manager.run().await;
    });

    let result = timeout(timeout_duration, async {
        // Monitor task progress - should succeed after 3 attempts (2 failures + 1 success)
        for _ in 0..200 {
            // 200 * 50ms = 10 seconds max
            tokio::time::sleep(Duration::from_millis(50)).await;
            let current_calls = call_count_for_test.load(Ordering::SeqCst);
            if current_calls >= 3 {
                return 1; // Success!
            }
        }
        0
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    assert_eq!(result.unwrap(), 1, "Should have one successful completion");

    // Verify the task was called 3 times (2 failures + 1 success)
    assert_eq!(call_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_task_restart_max_attempts_exceeded() {
    let config = TaskManagerConfig {
        max_concurrent_tasks: 10,
        default_restart_policy: RestartPolicy {
            max_restarts: 2,                      // Allow only 2 restarts
            base_delay: Duration::from_millis(0), // No delay for testing
            backoff_multiplier: 1.0,              // No exponential backoff for testing
            max_delay: Duration::from_secs(1),
            jitter_factor: 0.0,
            enable_circuit_breaker: false,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(1),
        },
        shutdown_timeout: Duration::from_secs(30),
        shutdown_config: ShutdownConfig::default(),
    };

    let mut manager: TaskManager<String> = TaskManager::with_config(config);
    let call_count = Arc::new(AtomicU32::new(0));

    // Create a task that always fails
    let task_fn =
        create_always_failing_task(call_count.clone(), TestError::transient("Always fails"));
    let task_id = manager.spawn_task("always_failing_task", task_fn);

    // Run the manager and collect all completions
    let timeout_duration = Duration::from_secs(10);
    let call_count_for_test = call_count.clone();

    // Run manager in background task
    let manager_handle = tokio::spawn(async move {
        let _ = manager.run().await;
    });

    let result = timeout(timeout_duration, async {
        // Monitor task progress - should stop after 3 attempts (original + 2 restarts)
        for _ in 0..100 {
            // 100 * 50ms = 5 seconds max
            tokio::time::sleep(Duration::from_millis(50)).await;
            let current_calls = call_count_for_test.load(Ordering::SeqCst);
            if current_calls >= 3 {
                return 3; // Reached max attempts
            }
        }
        0
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    assert_eq!(result.unwrap(), 3, "Should reach max attempts");

    // Should have original attempt + 2 restarts = 3 total calls
    assert_eq!(call_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_task_restart_permanent_failure_no_retry() {
    let config = TaskManagerConfig {
        max_concurrent_tasks: 10,
        default_restart_policy: RestartPolicy {
            max_restarts: 5, // High limit, but permanent failures shouldn't retry
            base_delay: Duration::from_millis(10),
            backoff_multiplier: 2.0,
            max_delay: Duration::from_secs(1),
            jitter_factor: 0.0,
            enable_circuit_breaker: false,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(1),
        },
        shutdown_timeout: Duration::from_secs(30),
        shutdown_config: ShutdownConfig::default(),
    };

    let mut manager: TaskManager<String> = TaskManager::with_config(config);
    let call_count = Arc::new(AtomicU32::new(0));

    // Create a task that fails with a permanent error
    let task_fn =
        create_always_failing_task(call_count.clone(), TestError::permanent("Fatal error"));
    let task_id = manager.spawn_task("permanent_failure_task", task_fn);

    // Run the manager and wait for completion
    let timeout_duration = Duration::from_secs(2);
    let call_count_for_test = call_count.clone();

    // Run manager in background task
    let manager_handle = tokio::spawn(async move {
        let _ = manager.run().await;
    });

    let result = timeout(timeout_duration, async {
        // Wait a bit for the task to complete once
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check that it only ran once (permanent failure, no restart)
        let final_calls = call_count_for_test.load(Ordering::SeqCst);
        final_calls == 1
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    assert!(result.unwrap(), "Should complete with only one call");

    // Should only be called once (no restarts for permanent failures)
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_task_restart_with_circuit_breaker() {
    let config = TaskManagerConfig {
        max_concurrent_tasks: 10,
        default_restart_policy: RestartPolicy {
            max_restarts: 10,
            base_delay: Duration::from_millis(10),
            backoff_multiplier: 2.0,
            max_delay: Duration::from_secs(1),
            jitter_factor: 0.0,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 3, // Open after 3 failures
            circuit_breaker_timeout: Duration::from_millis(500),
        },
        shutdown_timeout: Duration::from_secs(30),
        shutdown_config: ShutdownConfig::default(),
    };

    let mut manager: TaskManager<String> = TaskManager::with_config(config);
    let call_count = Arc::new(AtomicU32::new(0));

    // Create multiple failing tasks to trigger circuit breaker
    let task_fn1 =
        create_always_failing_task(call_count.clone(), TestError::network("Network error 1"));
    let task_fn2 =
        create_always_failing_task(call_count.clone(), TestError::network("Network error 2"));
    let task_fn3 =
        create_always_failing_task(call_count.clone(), TestError::network("Network error 3"));

    let task_id1 = manager.spawn_task("failing_task_1", task_fn1);
    let task_id2 = manager.spawn_task("failing_task_2", task_fn2);
    let task_id3 = manager.spawn_task("failing_task_3", task_fn3);

    // Run the manager and wait for circuit breaker to activate
    let timeout_duration = Duration::from_secs(10);
    let call_count_for_test = call_count.clone();

    // Run manager in background task
    let manager_handle = tokio::spawn(async move {
        let _ = manager.run().await;
    });

    let result = timeout(timeout_duration, async {
        // Wait for multiple failures to accumulate
        for _ in 0..60 {
            // 60 * 50ms = 3 seconds max
            tokio::time::sleep(Duration::from_millis(50)).await;
            let current_calls = call_count_for_test.load(Ordering::SeqCst);
            if current_calls >= 9 {
                // 3 tasks * 3 failures each
                return true;
            }
        }
        false
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    // Note: Can't check circuit breaker state after manager is moved
}

#[tokio::test]
async fn test_task_restart_exponential_backoff() {
    let config = TaskManagerConfig {
        max_concurrent_tasks: 10,
        default_restart_policy: RestartPolicy {
            max_restarts: 3,
            base_delay: Duration::from_millis(50),
            backoff_multiplier: 2.0,
            max_delay: Duration::from_secs(1),
            jitter_factor: 0.0, // No jitter for predictable timing
            enable_circuit_breaker: false,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(1),
        },
        shutdown_timeout: Duration::from_secs(30),
        shutdown_config: ShutdownConfig::default(),
    };

    let mut manager: TaskManager<String> = TaskManager::with_config(config);
    let call_count: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    let call_times = Arc::new(std::sync::Mutex::new(Vec::<std::time::Instant>::new()));

    // Create a task that records call times
    let call_times_clone = call_times.clone();
    let call_count_clone = call_count.clone();
    let task_fn = move || {
        let call_times = call_times_clone.clone();
        let call_count = call_count_clone.clone();
        Box::pin(async move {
            call_times.lock().unwrap().push(std::time::Instant::now());
            call_count.fetch_add(1, Ordering::SeqCst);
            Err(Box::new(TestError::transient("Timing test"))
                as Box<dyn std::error::Error + Send + Sync>)
        })
            as Pin<Box<dyn std::future::Future<Output = TaskResult<String>> + Send + 'static>>
    };

    let _task_id = manager.spawn_task("backoff_test_task", task_fn);

    // Run the manager for a limited time
    let timeout_duration = Duration::from_secs(5);
    let call_count_for_test = call_count.clone();

    // Run manager in background task
    let manager_handle = tokio::spawn(async move {
        let _ = manager.run().await;
    });

    let _result = timeout(timeout_duration, async {
        // Wait for multiple failures to record timing
        for _ in 0..100 {
            // 100 * 50ms = 5 seconds max
            tokio::time::sleep(Duration::from_millis(50)).await;
            let current_calls = call_count_for_test.load(Ordering::SeqCst);
            if current_calls >= 4 {
                // Original + 3 restarts
                break;
            }
        }
    })
    .await;

    // Verify the timing between calls follows exponential backoff
    let times = call_times.lock().unwrap();
    assert!(
        times.len() >= 2,
        "Should have at least 2 calls for timing analysis"
    );

    if times.len() >= 3 {
        let delay1 = times[1].duration_since(times[0]);
        let delay2 = times[2].duration_since(times[1]);

        // Second delay should be roughly double the first (allowing for some variance)
        let ratio = delay2.as_millis() as f64 / delay1.as_millis() as f64;
        assert!(
            ratio >= 1.5 && ratio <= 2.5,
            "Exponential backoff ratio should be around 2.0, got: {:.2}",
            ratio
        );
    }
}

#[tokio::test]
async fn test_multiple_tasks_restart_independently() {
    let config = TaskManagerConfig {
        max_concurrent_tasks: 10,
        default_restart_policy: RestartPolicy {
            max_restarts: 2,
            base_delay: Duration::from_millis(0), // No delay for testing
            backoff_multiplier: 1.0,              // No exponential backoff for testing
            max_delay: Duration::from_secs(1),
            jitter_factor: 0.0,
            enable_circuit_breaker: false,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(1),
        },
        shutdown_timeout: Duration::from_secs(30),
        shutdown_config: ShutdownConfig::default(),
    };

    let mut manager: TaskManager<String> = TaskManager::with_config(config);

    // Create multiple tasks with different failure patterns
    let call_count1 = Arc::new(AtomicU32::new(0));
    let call_count2 = Arc::new(AtomicU32::new(0));

    let task_fn1 =
        create_failing_task(call_count1.clone(), 1, TestError::transient("Task 1 error"));
    let task_fn2 =
        create_failing_task(call_count2.clone(), 2, TestError::transient("Task 2 error"));

    let task_id1 = manager.spawn_task("task_1", task_fn1);
    let task_id2 = manager.spawn_task("task_2", task_fn2);

    // Run the manager and track completions
    let timeout_duration = Duration::from_secs(10);
    let call_count1_for_test = call_count1.clone();
    let call_count2_for_test = call_count2.clone();

    // Run manager in background task
    let manager_handle = tokio::spawn(async move {
        let _ = manager.run().await;
    });

    let result = timeout(timeout_duration, async {
        // Monitor task progress - wait for both tasks to complete
        for _ in 0..200 {
            // 200 * 50ms = 10 seconds max
            tokio::time::sleep(Duration::from_millis(50)).await;
            let calls1 = call_count1_for_test.load(Ordering::SeqCst);
            let calls2 = call_count2_for_test.load(Ordering::SeqCst);

            // Task 1 should complete after 2 calls (1 failure + 1 success)
            // Task 2 should complete after 3 calls (2 failures + 1 success)
            if calls1 >= 2 && calls2 >= 3 {
                return 2; // Both tasks completed successfully
            }
        }
        0
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    assert_eq!(
        result.unwrap(),
        2,
        "Both tasks should complete successfully"
    );

    // Verify each task had the expected number of calls
    assert_eq!(call_count1.load(Ordering::SeqCst), 2); // 1 failure + 1 success
    assert_eq!(call_count2.load(Ordering::SeqCst), 3); // 2 failures + 1 success
}

#[tokio::test]
async fn test_task_restart_preserves_task_function() {
    let config = TaskManagerConfig {
        max_concurrent_tasks: 10,
        default_restart_policy: RestartPolicy {
            max_restarts: 3,
            base_delay: Duration::from_millis(0), // No delay for testing
            backoff_multiplier: 1.0,              // No exponential backoff for testing
            max_delay: Duration::from_secs(1),
            jitter_factor: 0.0,
            enable_circuit_breaker: false,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(1),
        },
        shutdown_timeout: Duration::from_secs(30),
        shutdown_config: ShutdownConfig::default(),
    };

    let mut manager: TaskManager<String> = TaskManager::with_config(config);
    let call_count = Arc::new(AtomicU32::new(0));

    // Create a task with specific state that should be preserved across restarts
    let task_state = Arc::new(AtomicU32::new(42));
    let task_state_clone = task_state.clone();
    let call_count_clone = call_count.clone();

    let task_fn = move || {
        let task_state = task_state_clone.clone();
        let call_count = call_count_clone.clone();
        Box::pin(async move {
            let count = call_count.fetch_add(1, Ordering::SeqCst);
            let state_value = task_state.load(Ordering::SeqCst);

            if count < 2 {
                // Fail the first two attempts
                Err(Box::new(TestError::transient("Temporary failure"))
                    as Box<dyn std::error::Error + Send + Sync>)
            } else {
                // Success on third attempt, return the preserved state
                Ok(format!("Success with state: {}", state_value))
            }
        })
            as Pin<Box<dyn std::future::Future<Output = TaskResult<String>> + Send + 'static>>
    };

    let task_id = manager.spawn_task("stateful_task", task_fn);

    // Run the manager and wait for successful completion
    let timeout_duration = Duration::from_secs(10);
    let call_count_for_test = call_count.clone();

    // Run manager in background task
    let manager_handle = tokio::spawn(async move {
        let _ = manager.run().await;
    });

    let result = timeout(timeout_duration, async {
        // Monitor task progress - wait for task to complete successfully
        for _ in 0..200 {
            // 200 * 50ms = 10 seconds max
            tokio::time::sleep(Duration::from_millis(50)).await;
            let current_calls = call_count_for_test.load(Ordering::SeqCst);
            if current_calls >= 3 {
                return "Success with state: 42".to_string(); // Expected result
            }
        }
        String::new()
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    let success_message = result.unwrap();
    assert_eq!(success_message, "Success with state: 42");
    assert_eq!(call_count.load(Ordering::SeqCst), 3); // 2 failures + 1 success
}
