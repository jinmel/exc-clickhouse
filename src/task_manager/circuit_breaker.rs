use crate::task_manager::types::CircuitBreakerState;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Circuit breaker for handling persistent failures
#[derive(Debug)]
pub struct CircuitBreaker {
    state: Arc<parking_lot::Mutex<CircuitBreakerState>>,
    failure_count: Arc<AtomicU32>,
    last_failure_time: Arc<AtomicU64>,
    threshold: u32,
    timeout: Duration,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, timeout: Duration) -> Self {
        Self {
            state: Arc::new(parking_lot::Mutex::new(CircuitBreakerState::Closed)),
            failure_count: Arc::new(AtomicU32::new(0)),
            last_failure_time: Arc::new(AtomicU64::new(0)),
            threshold,
            timeout,
        }
    }

    pub fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_failure_time.store(now, Ordering::SeqCst);

        if count >= self.threshold {
            let mut state = self.state.lock();
            *state = CircuitBreakerState::Open;
        }
    }

    pub fn record_success(&self) {
        self.failure_count.store(0, Ordering::SeqCst);
        let mut state = self.state.lock();
        *state = CircuitBreakerState::Closed;
    }

    pub fn can_execute(&self) -> bool {
        let state = self.state.lock();
        match *state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let last_failure = self.last_failure_time.load(Ordering::SeqCst);

                if now.saturating_sub(last_failure) >= self.timeout.as_millis() as u64 {
                    drop(state);
                    let mut state = self.state.lock();
                    *state = CircuitBreakerState::HalfOpen;
                    true
                } else {
                    false
                }
            }
            CircuitBreakerState::HalfOpen => true,
        }
    }

    pub fn state(&self) -> CircuitBreakerState {
        *self.state.lock()
    }

    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::SeqCst)
    }

    pub fn reset(&self) {
        self.failure_count.store(0, Ordering::SeqCst);
        let mut state = self.state.lock();
        *state = CircuitBreakerState::Closed;
    }
}
