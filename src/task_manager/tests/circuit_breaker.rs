use super::super::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_circuit_breaker_functionality() {
    let circuit_breaker = CircuitBreaker::new(3, Duration::from_millis(100));
    
    // Initially closed
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::Closed);
    assert!(circuit_breaker.can_execute());
    
    // Record failures to open circuit
    for _ in 0..3 {
        circuit_breaker.record_failure();
    }
    
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::Open);
    assert!(!circuit_breaker.can_execute());
    
    // Wait for timeout and test half-open state
    sleep(Duration::from_millis(150)).await;
    // can_execute() will transition to HalfOpen state
    assert!(circuit_breaker.can_execute());
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::HalfOpen);
    
    // Record success to close circuit
    circuit_breaker.record_success();
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::Closed);
}

#[tokio::test]
async fn test_circuit_breaker_recovery() {
    let circuit_breaker = CircuitBreaker::new(2, Duration::from_millis(50));

    // Open the circuit
    circuit_breaker.record_failure();
    circuit_breaker.record_failure();
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::Open);
    assert!(!circuit_breaker.can_execute());

    // Wait for timeout
    sleep(Duration::from_millis(60)).await;

    // Should transition to half-open
    assert!(circuit_breaker.can_execute());
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::HalfOpen);

    // Record success to close the circuit
    circuit_breaker.record_success();
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::Closed);
    assert_eq!(circuit_breaker.failure_count(), 0);
}

#[tokio::test]
async fn test_circuit_breaker_edge_cases() {
    let circuit_breaker = CircuitBreaker::new(3, Duration::from_millis(100));
    
    // Test rapid failure/success cycles
    for _ in 0..5 {
        circuit_breaker.record_failure();
        circuit_breaker.record_success();
    }
    
    // Should still be closed due to successes
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::Closed);
    
    // Test threshold boundary
    for _ in 0..3 {
        circuit_breaker.record_failure();
    }
    
    // Should now be open
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::Open);
    assert!(!circuit_breaker.can_execute());
    
    // Test recovery after timeout
    sleep(Duration::from_millis(150)).await;
    
    // Should transition to half-open
    assert!(circuit_breaker.can_execute());
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::HalfOpen);
    
    // Success should close the circuit
    circuit_breaker.record_success();
    assert_eq!(circuit_breaker.state(), CircuitBreakerState::Closed);
}

#[tokio::test]
async fn test_task_manager_circuit_breaker_integration() {
    let manager: TaskManager<String> = TaskManager::new();

    // Initially circuit breaker should be closed
    let (state, count) = manager.get_circuit_breaker_status();
    assert_eq!(state, CircuitBreakerState::Closed);
    assert_eq!(count, 0);

    // Test manual circuit breaker reset
    manager.reset_circuit_breaker();
    let (state, count) = manager.get_circuit_breaker_status();
    assert_eq!(state, CircuitBreakerState::Closed);
    assert_eq!(count, 0);
} 