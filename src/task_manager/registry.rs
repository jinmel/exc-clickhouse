use crate::task_manager::handle::TaskHandle;
use crate::task_manager::types::{TaskId, TaskState};
use std::collections::HashMap;

/// Registry for efficient task lookups and management
#[derive(Debug)]
pub struct TaskRegistry<T> {
    tasks: HashMap<TaskId, TaskHandle<T>>,
    name_to_id: HashMap<String, TaskId>,
}

impl<T> TaskRegistry<T> {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            name_to_id: HashMap::new(),
        }
    }

    /// Register a new task handle
    pub fn register(&mut self, handle: TaskHandle<T>) -> Result<(), String> {
        // Check for duplicate names
        if self.name_to_id.contains_key(&handle.name) {
            return Err(format!("Task with name '{}' already exists", handle.name));
        }

        let task_id = handle.id.clone();
        let task_name = handle.name.clone();

        self.name_to_id.insert(task_name, task_id.clone());
        self.tasks.insert(task_id, handle);

        Ok(())
    }

    /// Get task by ID
    pub fn get(&self, id: &TaskId) -> Option<&TaskHandle<T>> {
        self.tasks.get(id)
    }

    /// Get mutable task by ID
    pub fn get_mut(&mut self, id: &TaskId) -> Option<&mut TaskHandle<T>> {
        self.tasks.get_mut(id)
    }

    /// Get task by name
    pub fn get_by_name(&self, name: &str) -> Option<&TaskHandle<T>> {
        self.name_to_id.get(name).and_then(|id| self.tasks.get(id))
    }

    /// Remove task from registry
    pub fn remove(&mut self, id: &TaskId) -> Option<TaskHandle<T>> {
        if let Some(handle) = self.tasks.remove(id) {
            self.name_to_id.remove(&handle.name);
            Some(handle)
        } else {
            None
        }
    }

    /// Get all task IDs
    pub fn task_ids(&self) -> Vec<TaskId> {
        self.tasks.keys().cloned().collect()
    }

    /// Get tasks by state
    pub fn tasks_by_state(&self, state: TaskState) -> Vec<&TaskHandle<T>> {
        self.tasks
            .values()
            .filter(|handle| handle.state() == state)
            .collect()
    }

    /// Get total number of tasks
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Get all tasks
    pub fn all_tasks(&self) -> impl Iterator<Item = &TaskHandle<T>> {
        self.tasks.values()
    }

    /// Get all tasks mutably
    pub fn all_tasks_mut(&mut self) -> impl Iterator<Item = &mut TaskHandle<T>> {
        self.tasks.values_mut()
    }
}

impl<T> Default for TaskRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}
