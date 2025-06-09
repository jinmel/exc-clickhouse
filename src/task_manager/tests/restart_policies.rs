use super::super::*;
use std::time::Duration;

#[tokio::test]
async fn test_restart_policy_calculation() {
    let policy = RestartPolicy::default();
    
    // Test exponential backoff calculation
    let delay1 = RestartUtils::calculate_backoff_delay(0, &policy);
    let delay2 = RestartUtils::calculate_backoff_delay(1, &policy);
    let delay3 = RestartUtils::calculate_backoff_delay(2, &policy);
    
    // Should increase exponentially
    assert!(delay2 > delay1);
    assert!(delay3 > delay2);
    
    // Should not exceed max delay
    let large_delay = RestartUtils::calculate_backoff_delay(100, &policy);
    assert!(large_delay <= policy.max_delay);
}

#[tokio::test]
async fn test_restart_utils_failure_classification() {
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct TestError(String);
    
    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    
    impl Error for TestError {}

    // Test network errors
    let network_error = TestError("connection timeout".to_string());
    assert_eq!(RestartUtils::classify_failure(&network_error), FailureType::Network);

    // Test resource errors
    let resource_error = TestError("out of memory".to_string());
    assert_eq!(RestartUtils::classify_failure(&resource_error), FailureType::Resource);

    // Test configuration errors
    let config_error = TestError("invalid configuration".to_string());
    assert_eq!(RestartUtils::classify_failure(&config_error), FailureType::Configuration);

    // Test permanent errors
    let permanent_error = TestError("fatal error occurred".to_string());
    assert_eq!(RestartUtils::classify_failure(&permanent_error), FailureType::Permanent);

    // Test unknown errors (default to transient)
    let unknown_error = TestError("something went wrong".to_string());
    assert_eq!(RestartUtils::classify_failure(&unknown_error), FailureType::Transient);
}

#[tokio::test]
async fn test_restart_utils_should_retry_failure_type() {
    assert!(RestartUtils::should_retry_failure_type(&FailureType::Transient));
    assert!(RestartUtils::should_retry_failure_type(&FailureType::Network));
    assert!(RestartUtils::should_retry_failure_type(&FailureType::Resource));
    assert!(RestartUtils::should_retry_failure_type(&FailureType::Unknown));
    assert!(!RestartUtils::should_retry_failure_type(&FailureType::Permanent));
    assert!(!RestartUtils::should_retry_failure_type(&FailureType::Configuration));
}

#[tokio::test]
async fn test_restart_utils_exponential_backoff() {
    let policy = RestartPolicy {
        base_delay: Duration::from_secs(1),
        backoff_multiplier: 2.0,
        max_delay: Duration::from_secs(60),
        jitter_factor: 0.0, // No jitter for predictable testing
        ..Default::default()
    };

    // First restart (count = 0) should use base delay
    let delay0 = RestartUtils::calculate_backoff_delay(0, &policy);
    assert_eq!(delay0, Duration::from_secs(1));

    // Second restart (count = 1) should double
    let delay1 = RestartUtils::calculate_backoff_delay(1, &policy);
    assert_eq!(delay1, Duration::from_secs(2));

    // Third restart (count = 2) should double again
    let delay2 = RestartUtils::calculate_backoff_delay(2, &policy);
    assert_eq!(delay2, Duration::from_secs(4));

    // Test max delay cap
    let delay_large = RestartUtils::calculate_backoff_delay(10, &policy);
    assert_eq!(delay_large, Duration::from_secs(60)); // Should be capped at max_delay
}

#[tokio::test]
async fn test_restart_utils_exponential_backoff_with_jitter() {
    let policy = RestartPolicy {
        base_delay: Duration::from_secs(1),
        backoff_multiplier: 2.0,
        max_delay: Duration::from_secs(60),
        jitter_factor: 0.1, // 10% jitter
        ..Default::default()
    };

    // Test that jitter produces different results (run multiple times)
    let mut delays = Vec::new();
    for _ in 0..10 {
        let delay = RestartUtils::calculate_backoff_delay(2, &policy);
        delays.push(delay);
    }

    // With jitter, we should get some variation around the base value (4 seconds)
    let _base_expected = Duration::from_secs(4);
    let min_expected = Duration::from_secs_f64(3.6); // 4 - 10%
    let max_expected = Duration::from_secs_f64(4.4); // 4 + 10%

    // At least some delays should be different due to jitter
    let all_same = delays.iter().all(|&d| d == delays[0]);
    assert!(!all_same, "Jitter should produce some variation");

    // All delays should be within the jitter range
    for delay in delays {
        assert!(delay >= min_expected && delay <= max_expected,
            "Delay {:?} should be between {:?} and {:?}", delay, min_expected, max_expected);
    }
}

#[tokio::test]
async fn test_restart_utils_comprehensive_restart_decision() {
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct TestError(String);
    
    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    
    impl Error for TestError {}

    let policy = RestartPolicy {
        max_restarts: 2,
        base_delay: Duration::from_millis(100),
        backoff_multiplier: 2.0,
        max_delay: Duration::from_secs(60),
        jitter_factor: 0.0,
        enable_circuit_breaker: true,
        circuit_breaker_threshold: 3,
        circuit_breaker_timeout: Duration::from_millis(100),
    };

    let circuit_breaker = CircuitBreaker::new(3, Duration::from_millis(100));

    // Test transient error should be retried
    let transient_error = TestError("connection timeout".to_string());
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let past_time = now - 200; // 200ms ago, enough for restart

    assert!(RestartUtils::should_restart_task(
        &transient_error,
        0, // restart count
        past_time,
        &policy,
        Some(&circuit_breaker),
    ));

    // Test permanent error should not be retried
    let permanent_error = TestError("fatal error".to_string());
    assert!(!RestartUtils::should_restart_task(
        &permanent_error,
        0,
        past_time,
        &policy,
        Some(&circuit_breaker),
    ));

    // Test max restart limit
    assert!(!RestartUtils::should_restart_task(
        &transient_error,
        2, // at max restart limit
        past_time,
        &policy,
        Some(&circuit_breaker),
    ));

    // Test time constraint
    let recent_time = now - 50; // 50ms ago, not enough time
    assert!(!RestartUtils::should_restart_task(
        &transient_error,
        0,
        recent_time,
        &policy,
        Some(&circuit_breaker),
    ));
}

#[tokio::test]
async fn test_task_manager_restart_policy_management() {
    let mut manager: TaskManager<String> = TaskManager::new();

    // Test default restart policy
    let default_policy = manager.get_restart_policy();
    assert_eq!(default_policy.max_restarts, 3);
    assert_eq!(default_policy.base_delay, Duration::from_secs(1));

    // Test updating restart policy
    let new_policy = RestartPolicy {
        max_restarts: 5,
        base_delay: Duration::from_secs(2),
        backoff_multiplier: 1.5,
        max_delay: Duration::from_secs(120),
        jitter_factor: 0.2,
        enable_circuit_breaker: false,
        circuit_breaker_threshold: 10,
        circuit_breaker_timeout: Duration::from_secs(600),
    };

    manager.update_restart_policy(new_policy.clone());
    let updated_policy = manager.get_restart_policy();
    assert_eq!(updated_policy.max_restarts, 5);
    assert_eq!(updated_policy.base_delay, Duration::from_secs(2));
    assert_eq!(updated_policy.backoff_multiplier, 1.5);
} 