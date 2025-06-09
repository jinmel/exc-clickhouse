use super::super::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_task_manager_failure_handling() {
    let mut manager: TaskManager<String> = TaskManager::new();
    
    // Test spawning a failing task
    let task_id = manager.spawn_task("failing_task".to_string(), || async {
        sleep(Duration::from_millis(10)).await;
        Err("task failed".into())
    });
    
    // Wait for task completion
    if let Some(completion) = manager.wait_for_next_completion().await {
        assert_eq!(completion.task_id, task_id);
        assert_eq!(completion.task_name, "failing_task");
        assert!(completion.result.is_err());
    } else {
        panic!("Expected task completion");
    }
}

#[tokio::test]
async fn test_enhanced_task_error_context() {
    use std::error::Error;
    use std::fmt;
    
    #[derive(Debug)]
    struct TestError {
        message: String,
        source: Option<Box<dyn Error + Send + Sync>>,
    }
    
    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.message)
        }
    }
    
    impl Error for TestError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.source.as_ref().map(|e| e.as_ref() as &(dyn Error + 'static))
        }
    }
    
    let correlation_id = CorrelationId::new();
    let inner_error = TestError {
        message: "Inner error".to_string(),
        source: None,
    };
    let outer_error = TestError {
        message: "Outer error".to_string(),
        source: Some(Box::new(inner_error)),
    };
    
    let task_error = TaskError::new(&outer_error, correlation_id.clone())
        .with_context("test_key".to_string(), "test_value".to_string());
    
    assert_eq!(task_error.correlation_id.to_string(), correlation_id.to_string());
    assert_eq!(task_error.error_chain.len(), 2);
    assert_eq!(task_error.error_chain[0], "Outer error");
    assert_eq!(task_error.error_chain[1], "Inner error");
    assert_eq!(task_error.context.get("test_key"), Some(&"test_value".to_string()));
    assert_eq!(task_error.failure_category, FailureType::Transient);
} 