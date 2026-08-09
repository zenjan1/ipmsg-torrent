//! Task chaining - automatically start next task when current task completes
//!
//! Allows users to create chains of tasks that execute in sequence. When one task
//! completes, the next task in the chain is automatically started (moved from Queued
//! to Downloading state).
//!
//! Chains are useful for:
//! - Downloading multi-part files in order
//! - Sequential processing pipelines
//! - Automated workflows

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;

/// Error type for task chain operations
#[derive(Debug, thiserror::Error)]
pub enum TaskChainError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Chain not found: {0}")]
    ChainNotFound(String),
    #[error("Task not found in chain: {0}")]
    TaskNotInChain(String),
    #[error("Chain is full (max {max} tasks)")]
    ChainFull { max: usize },
    #[error("Circular dependency detected")]
    CircularDependency,
}

/// A chain of tasks that execute in sequence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskChain {
    /// Unique chain ID
    pub chain_id: String,
    /// Human-readable name
    pub name: String,
    /// Ordered list of task IDs in the chain
    pub task_ids: Vec<String>,
    /// Whether the chain is enabled
    pub enabled: bool,
    /// When the chain was created
    pub created_at: DateTime<Utc>,
    /// Optional description
    pub description: Option<String>,
    /// Whether to auto-remove completed tasks from the chain
    pub auto_remove_completed: bool,
}

impl TaskChain {
    /// Create a new task chain
    pub fn new(chain_id: String, name: String) -> Self {
        Self {
            chain_id,
            name,
            task_ids: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            description: None,
            auto_remove_completed: false,
        }
    }

    /// Add a task to the end of the chain
    pub fn add_task(&mut self, task_id: String) -> Result<(), TaskChainError> {
        if self.task_ids.contains(&task_id) {
            return Err(TaskChainError::TaskNotInChain(task_id));
        }
        self.task_ids.push(task_id);
        Ok(())
    }

    /// Remove a task from the chain
    pub fn remove_task(&mut self, task_id: &str) -> Result<(), TaskChainError> {
        if let Some(pos) = self.task_ids.iter().position(|id| id == task_id) {
            self.task_ids.remove(pos);
            Ok(())
        } else {
            Err(TaskChainError::TaskNotInChain(task_id.to_string()))
        }
    }

    /// Get the next task to run (first Queued task in the chain)
    pub fn get_next_task(&self) -> Option<&str> {
        self.task_ids.first().map(|id| id.as_str())
    }

    /// Remove the first task from the chain (after completion)
    pub fn pop_completed_task(&mut self) -> Option<String> {
        if self.task_ids.is_empty() {
            None
        } else {
            Some(self.task_ids.remove(0))
        }
    }

    /// Check if the chain is empty
    pub fn is_empty(&self) -> bool {
        self.task_ids.is_empty()
    }

    /// Get the number of tasks in the chain
    pub fn len(&self) -> usize {
        self.task_ids.len()
    }
}

/// Manager for task chains
#[derive(Debug, Clone)]
pub struct TaskChainManager {
    /// Map of chain_id -> TaskChain
    chains: HashMap<String, TaskChain>,
    /// Map of task_id -> chain_id (for quick lookup)
    task_to_chain: HashMap<String, String>,
    /// Path to persist configuration
    config_path: PathBuf,
}

impl TaskChainManager {
    /// Create a new task chain manager
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            chains: HashMap::new(),
            task_to_chain: HashMap::new(),
            config_path,
        }
    }

    /// Create a new chain
    pub fn create_chain(&mut self, chain_id: String, name: String) -> Result<(), TaskChainError> {
        let chain = TaskChain::new(chain_id.clone(), name);
        self.chains.insert(chain_id, chain);
        Ok(())
    }

    /// Delete a chain
    pub fn delete_chain(&mut self, chain_id: &str) -> Result<(), TaskChainError> {
        if let Some(chain) = self.chains.remove(chain_id) {
            // Remove all task mappings
            for task_id in &chain.task_ids {
                self.task_to_chain.remove(task_id);
            }
            Ok(())
        } else {
            Err(TaskChainError::ChainNotFound(chain_id.to_string()))
        }
    }

    /// Add a task to a chain
    pub fn add_task_to_chain(
        &mut self,
        chain_id: &str,
        task_id: String,
    ) -> Result<(), TaskChainError> {
        if let Some(chain) = self.chains.get_mut(chain_id) {
            chain.add_task(task_id.clone())?;
            self.task_to_chain.insert(task_id, chain_id.to_string());
            Ok(())
        } else {
            Err(TaskChainError::ChainNotFound(chain_id.to_string()))
        }
    }

    /// Remove a task from a chain
    pub fn remove_task_from_chain(
        &mut self,
        chain_id: &str,
        task_id: &str,
    ) -> Result<(), TaskChainError> {
        if let Some(chain) = self.chains.get_mut(chain_id) {
            chain.remove_task(task_id)?;
            self.task_to_chain.remove(task_id);
            Ok(())
        } else {
            Err(TaskChainError::ChainNotFound(chain_id.to_string()))
        }
    }

    /// Get the next task to run when a task completes
    pub fn get_next_task_after_completion(
        &self,
        completed_task_id: &str,
    ) -> Option<(String, String)> {
        // Find the chain containing this task
        let chain_id = self.task_to_chain.get(completed_task_id)?;
        let chain = self.chains.get(chain_id)?;

        if !chain.enabled {
            return None;
        }

        // Get the next task in the chain
        let next_task_id = chain.get_next_task()?;
        Some((chain_id.clone(), next_task_id.to_string()))
    }

    /// Mark a task as completed and remove it from the chain
    pub fn mark_task_completed(
        &mut self,
        task_id: &str,
    ) -> Result<Option<(String, String)>, TaskChainError> {
        // Find the chain containing this task
        if let Some(chain_id) = self.task_to_chain.get(task_id).cloned() {
            if let Some(chain) = self.chains.get_mut(&chain_id) {
                if chain.auto_remove_completed {
                    chain.pop_completed_task();
                    self.task_to_chain.remove(task_id);
                }

                // Get the next task
                if let Some(next_task_id) = chain.get_next_task() {
                    return Ok(Some((chain_id, next_task_id.to_string())));
                }
            }
        }
        Ok(None)
    }

    /// List all chains
    pub fn list_chains(&self) -> Vec<&TaskChain> {
        self.chains.values().collect()
    }

    /// Get a chain by ID
    pub fn get_chain(&self, chain_id: &str) -> Option<&TaskChain> {
        self.chains.get(chain_id)
    }

    /// Get the chain containing a task
    pub fn get_chain_for_task(&self, task_id: &str) -> Option<&TaskChain> {
        let chain_id = self.task_to_chain.get(task_id)?;
        self.chains.get(chain_id)
    }

    /// Enable or disable a chain
    pub fn set_chain_enabled(
        &mut self,
        chain_id: &str,
        enabled: bool,
    ) -> Result<(), TaskChainError> {
        if let Some(chain) = self.chains.get_mut(chain_id) {
            chain.enabled = enabled;
            Ok(())
        } else {
            Err(TaskChainError::ChainNotFound(chain_id.to_string()))
        }
    }

    /// Set auto-remove completed tasks for a chain
    pub fn set_auto_remove_completed(
        &mut self,
        chain_id: &str,
        auto_remove: bool,
    ) -> Result<(), TaskChainError> {
        if let Some(chain) = self.chains.get_mut(chain_id) {
            chain.auto_remove_completed = auto_remove;
            Ok(())
        } else {
            Err(TaskChainError::ChainNotFound(chain_id.to_string()))
        }
    }

    /// Save state to disk (atomic write)
    pub async fn save(&self) -> Result<(), TaskChainError> {
        let state = TaskChainState {
            chains: self.chains.clone(),
            task_to_chain: self.task_to_chain.clone(),
        };
        let json = serde_json::to_string_pretty(&state)?;
        let temp_path = self.config_path.with_extension("json.tmp");
        fs::write(&temp_path, json.as_bytes()).await?;
        fs::rename(&temp_path, &self.config_path).await?;
        Ok(())
    }

    /// Load state from disk
    pub async fn load(&mut self) -> Result<(), TaskChainError> {
        if !self.config_path.exists() {
            return Ok(());
        }
        let content = fs::read_to_string(&self.config_path).await?;
        if content.trim().is_empty() {
            return Ok(());
        }
        let state: TaskChainState = serde_json::from_str(&content)?;
        self.chains = state.chains;
        self.task_to_chain = state.task_to_chain;
        Ok(())
    }
}

/// Persisted state for task chains
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskChainState {
    /// All task chains
    pub chains: HashMap<String, TaskChain>,
    /// Mapping of task_id -> chain_id
    pub task_to_chain: HashMap<String, String>,
}

/// Summary of task chains
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskChainSummary {
    /// Total number of chains
    pub total_chains: usize,
    /// Number of enabled chains
    pub enabled_chains: usize,
    /// Total tasks across all chains
    pub total_tasks: usize,
    /// Chains with their basic info
    pub chains: Vec<ChainInfo>,
}

/// Basic info about a chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainInfo {
    /// Chain ID
    pub chain_id: String,
    /// Chain name
    pub name: String,
    /// Number of tasks in the chain
    pub task_count: usize,
    /// Whether the chain is enabled
    pub enabled: bool,
    /// ID of the first task in the chain (if any)
    pub next_task_id: Option<String>,
}

impl TaskChainManager {
    /// Generate a summary of all chains
    pub fn get_summary(&self) -> TaskChainSummary {
        let chains: Vec<ChainInfo> = self
            .chains
            .values()
            .map(|chain| ChainInfo {
                chain_id: chain.chain_id.clone(),
                name: chain.name.clone(),
                task_count: chain.task_ids.len(),
                enabled: chain.enabled,
                next_task_id: chain.get_next_task().map(|s| s.to_string()),
            })
            .collect();

        let total_tasks = chains.iter().map(|c| c.task_count).sum();
        let enabled_chains = chains.iter().filter(|c| c.enabled).count();

        TaskChainSummary {
            total_chains: chains.len(),
            enabled_chains,
            total_tasks,
            chains,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_chain() {
        let mut chain = TaskChain::new("chain-1".to_string(), "Test Chain".to_string());
        assert_eq!(chain.chain_id, "chain-1");
        assert_eq!(chain.name, "Test Chain");
        assert!(chain.task_ids.is_empty());
        assert!(chain.enabled);
    }

    #[test]
    fn test_add_task_to_chain() {
        let mut chain = TaskChain::new("chain-1".to_string(), "Test Chain".to_string());
        chain.add_task("task-1".to_string()).unwrap();
        chain.add_task("task-2".to_string()).unwrap();
        assert_eq!(chain.task_ids.len(), 2);
        assert_eq!(chain.task_ids[0], "task-1");
        assert_eq!(chain.task_ids[1], "task-2");
    }

    #[test]
    fn test_add_duplicate_task() {
        let mut chain = TaskChain::new("chain-1".to_string(), "Test Chain".to_string());
        chain.add_task("task-1".to_string()).unwrap();
        let result = chain.add_task("task-1".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_task_from_chain() {
        let mut chain = TaskChain::new("chain-1".to_string(), "Test Chain".to_string());
        chain.add_task("task-1".to_string()).unwrap();
        chain.add_task("task-2".to_string()).unwrap();
        chain.remove_task("task-1").unwrap();
        assert_eq!(chain.task_ids.len(), 1);
        assert_eq!(chain.task_ids[0], "task-2");
    }

    #[test]
    fn test_remove_nonexistent_task() {
        let mut chain = TaskChain::new("chain-1".to_string(), "Test Chain".to_string());
        let result = chain.remove_task("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_next_task() {
        let mut chain = TaskChain::new("chain-1".to_string(), "Test Chain".to_string());
        assert!(chain.get_next_task().is_none());
        chain.add_task("task-1".to_string()).unwrap();
        chain.add_task("task-2".to_string()).unwrap();
        assert_eq!(chain.get_next_task(), Some("task-1"));
    }

    #[test]
    fn test_pop_completed_task() {
        let mut chain = TaskChain::new("chain-1".to_string(), "Test Chain".to_string());
        chain.add_task("task-1".to_string()).unwrap();
        chain.add_task("task-2".to_string()).unwrap();
        let popped = chain.pop_completed_task();
        assert_eq!(popped, Some("task-1".to_string()));
        assert_eq!(chain.task_ids.len(), 1);
        assert_eq!(chain.task_ids[0], "task-2");
    }

    #[test]
    fn test_chain_manager_create_delete() {
        let mut manager = TaskChainManager::new(PathBuf::from("/tmp/test.json"));
        manager
            .create_chain("chain-1".to_string(), "Test Chain".to_string())
            .unwrap();
        assert!(manager.get_chain("chain-1").is_some());
        manager.delete_chain("chain-1").unwrap();
        assert!(manager.get_chain("chain-1").is_none());
    }

    #[test]
    fn test_chain_manager_add_remove_task() {
        let mut manager = TaskChainManager::new(PathBuf::from("/tmp/test.json"));
        manager
            .create_chain("chain-1".to_string(), "Test Chain".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-1".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-2".to_string())
            .unwrap();

        assert!(manager.get_chain_for_task("task-1").is_some());
        assert!(manager.get_chain_for_task("task-2").is_some());

        manager.remove_task_from_chain("chain-1", "task-1").unwrap();
        assert!(manager.get_chain_for_task("task-1").is_none());
    }

    #[test]
    fn test_chain_manager_get_next_after_completion() {
        let mut manager = TaskChainManager::new(PathBuf::from("/tmp/test.json"));
        manager
            .create_chain("chain-1".to_string(), "Test Chain".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-1".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-2".to_string())
            .unwrap();

        let next = manager.get_next_task_after_completion("task-1");
        assert_eq!(next, Some(("chain-1".to_string(), "task-1".to_string())));
    }

    #[test]
    fn test_chain_manager_mark_completed() {
        let mut manager = TaskChainManager::new(PathBuf::from("/tmp/test.json"));
        manager
            .create_chain("chain-1".to_string(), "Test Chain".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-1".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-2".to_string())
            .unwrap();

        // Enable auto-remove
        manager.set_auto_remove_completed("chain-1", true).unwrap();

        let next = manager.mark_task_completed("task-1").unwrap();
        assert_eq!(next, Some(("chain-1".to_string(), "task-2".to_string())));

        // task-1 should be removed from the chain
        assert!(manager.get_chain_for_task("task-1").is_none());
    }

    #[test]
    fn test_chain_manager_disabled_chain() {
        let mut manager = TaskChainManager::new(PathBuf::from("/tmp/test.json"));
        manager
            .create_chain("chain-1".to_string(), "Test Chain".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-1".to_string())
            .unwrap();
        manager.set_chain_enabled("chain-1", false).unwrap();

        let next = manager.get_next_task_after_completion("task-1");
        assert!(next.is_none());
    }

    #[test]
    fn test_chain_summary() {
        let mut manager = TaskChainManager::new(PathBuf::from("/tmp/test.json"));
        manager
            .create_chain("chain-1".to_string(), "Chain 1".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-1".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-2".to_string())
            .unwrap();

        manager
            .create_chain("chain-2".to_string(), "Chain 2".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-2", "task-3".to_string())
            .unwrap();

        let summary = manager.get_summary();
        assert_eq!(summary.total_chains, 2);
        assert_eq!(summary.enabled_chains, 2);
        assert_eq!(summary.total_tasks, 3);
    }

    #[tokio::test]
    async fn test_chain_manager_save_load() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("chains.json");

        let mut manager = TaskChainManager::new(path.clone());
        manager
            .create_chain("chain-1".to_string(), "Test Chain".to_string())
            .unwrap();
        manager
            .add_task_to_chain("chain-1", "task-1".to_string())
            .unwrap();
        manager.save().await.unwrap();

        let mut loaded = TaskChainManager::new(path);
        loaded.load().await.unwrap();

        assert!(loaded.get_chain("chain-1").is_some());
        assert!(loaded.get_chain_for_task("task-1").is_some());
    }

    #[test]
    fn test_chain_serialization() {
        let mut chain = TaskChain::new("chain-1".to_string(), "Test Chain".to_string());
        chain.add_task("task-1".to_string()).unwrap();
        chain.description = Some("Test description".to_string());

        let json = serde_json::to_string(&chain).unwrap();
        let deserialized: TaskChain = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.chain_id, "chain-1");
        assert_eq!(deserialized.name, "Test Chain");
        assert_eq!(deserialized.task_ids.len(), 1);
        assert_eq!(
            deserialized.description,
            Some("Test description".to_string())
        );
    }
}
