use std::time::{Duration, SystemTime, UNIX_EPOCH};
use rand::Rng;
use crate::task_manager::types::FailureType;
use crate::task_manager::circuit_breaker::CircuitBreaker;

/// Restart policy configuration for task failure recovery
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    /// Maximum number of restart attempts (0 = unlimited)
    pub max_restarts: u32,
    /// Base delay for exponential backoff
    pub base_delay: Duration,
    /// Multiplier for exponential backoff (e.g., 2.0 for doubling)
    pub backoff_multiplier: f64,
    /// Maximum delay cap for exponential backoff
    pub max_delay: Duration,
    /// Jitter factor to prevent thundering herd (0.0-1.0)
    pub jitter_factor: f64,
    /// Whether to enable circuit breaker pattern
    pub enable_circuit_breaker: bool,
    /// Failure threshold for circuit breaker
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker reset timeout
    pub circuit_breaker_timeout: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 3,
            base_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            max_delay: Duration::from_secs(60),
            jitter_factor: 0.1,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(60),
        }
    }
}

/// Utilities for restart logic and failure classification
pub struct RestartUtils;

impl RestartUtils {
    /// Classify failure type based on error characteristics
    pub fn classify_failure(error: &dyn std::error::Error) -> FailureType {
        let error_msg = error.to_string().to_lowercase();
        let error_type = std::any::type_name_of_val(error);
        
        // Network-related errors
        if error_msg.contains("connection") 
            || error_msg.contains("timeout") 
            || error_msg.contains("network")
            || error_msg.contains("dns")
            || error_msg.contains("socket")
            || error_type.contains("hyper")
            || error_type.contains("reqwest") {
            return FailureType::Network;
        }
        
        // Resource-related errors
        if error_msg.contains("memory") 
            || error_msg.contains("disk")
            || error_msg.contains("space")
            || error_msg.contains("resource")
            || error_msg.contains("limit") {
            return FailureType::Resource;
        }
        
        // Configuration errors
        if error_msg.contains("config") 
            || error_msg.contains("permission")
            || error_msg.contains("auth")
            || error_msg.contains("credential")
            || error_msg.contains("key")
            || error_msg.contains("invalid") {
            return FailureType::Configuration;
        }
        
        // Permanent errors
        if error_msg.contains("parse") 
            || error_msg.contains("format")
            || error_msg.contains("syntax")
            || error_msg.contains("not found")
            || error_msg.contains("does not exist")
            || error_msg.contains("fatal") {
            return FailureType::Permanent;
        }
        
        // Default to transient for unknown errors
        FailureType::Transient
    }
    
    /// Determine if a failure type should be retried
    pub fn should_retry_failure_type(failure_type: &FailureType) -> bool {
        match failure_type {
            FailureType::Transient | FailureType::Network | FailureType::Resource => true,
            FailureType::Permanent | FailureType::Configuration => false,
            FailureType::Unknown => true, // Default to retry for unknown failures
        }
    }
    
    /// Calculate exponential backoff delay with jitter
    pub fn calculate_backoff_delay(
        restart_count: u32,
        policy: &RestartPolicy,
    ) -> Duration {
        if restart_count == 0 {
            return policy.base_delay;
        }
        
        // Calculate exponential backoff: base_delay * multiplier^restart_count
        let base_ms = policy.base_delay.as_millis() as f64;
        let multiplier = policy.backoff_multiplier;
        let exponential_delay_ms = base_ms * multiplier.powi(restart_count as i32);
        
        // Cap at max_delay
        let capped_delay_ms = exponential_delay_ms.min(policy.max_delay.as_millis() as f64);
        
        // Add jitter to prevent thundering herd
        let jitter_range = capped_delay_ms * policy.jitter_factor;
        let mut rng = rand::rng();
        let jitter = rng.random_range(-jitter_range..=jitter_range);
        let final_delay_ms = (capped_delay_ms + jitter).max(0.0);
        
        // Ensure final delay doesn't exceed max_delay
        let max_delay_ms = policy.max_delay.as_millis() as f64;
        let clamped_delay_ms = final_delay_ms.min(max_delay_ms);
        
        Duration::from_millis(clamped_delay_ms as u64)
    }
    
    /// Comprehensive restart decision logic
    pub fn should_restart_task(
        error: &dyn std::error::Error,
        restart_count: u32,
        last_restart_time: u64,
        policy: &RestartPolicy,
        circuit_breaker: Option<&CircuitBreaker>,
    ) -> bool {
        // Check restart count limit
        if policy.max_restarts > 0 && restart_count >= policy.max_restarts {
            return false;
        }
        
        // Classify failure type
        let failure_type = Self::classify_failure(error);
        if !Self::should_retry_failure_type(&failure_type) {
            return false;
        }
        
        // Check circuit breaker if enabled
        if policy.enable_circuit_breaker {
            if let Some(cb) = circuit_breaker {
                if !cb.can_execute() {
                    return false;
                }
            }
        }
        
        // Check minimum time between restarts
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        let required_delay = Self::calculate_backoff_delay(restart_count, policy);
        let time_since_last_restart = now.saturating_sub(last_restart_time);
        
        if time_since_last_restart < required_delay.as_millis() as u64 {
            return false;
        }
        
        true
    }
} 