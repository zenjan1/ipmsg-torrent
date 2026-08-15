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
        if let Some(chain_id) = self.task_to_chain.get(task_id).cloned()
            && let Some(chain) = self.chains.get_mut(&chain_id)
        {
            if chain.auto_remove_completed {
                chain.pop_completed_task();
                self.task_to_chain.remove(task_id);
            }

            // Get the next task
            if let Some(next_task_id) = chain.get_next_task() {
                return Ok(Some((chain_id, next_task_id.to_string())));
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

    // ===== TaskChain new() defaults =====

    #[test]
    fn test_new_chain_defaults() {
        let chain = TaskChain::new("c1".into(), "n".into());
        assert!(chain.task_ids.is_empty());
        assert!(chain.enabled);
        assert!(chain.description.is_none());
        assert!(!chain.auto_remove_completed);
    }

    // ===== TaskChain::is_empty / len =====

    #[test]
    fn test_is_empty_and_len() {
        let mut chain = TaskChain::new("c1".into(), "n".into());
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);
        chain.add_task("t1".into()).unwrap();
        assert!(!chain.is_empty());
        assert_eq!(chain.len(), 1);
        chain.add_task("t2".into()).unwrap();
        assert_eq!(chain.len(), 2);
    }

    // ===== pop from empty =====

    #[test]
    fn test_pop_empty_chain() {
        let mut chain = TaskChain::new("c1".into(), "n".into());
        assert!(chain.pop_completed_task().is_none());
    }

    // ===== TaskChain serde =====

    #[test]
    fn test_chain_serde_roundtrip() {
        let mut chain = TaskChain::new("c1".into(), "MyChain".into());
        chain.add_task("t1".into()).unwrap();
        chain.add_task("t2".into()).unwrap();
        chain.description = Some("desc".into());
        chain.auto_remove_completed = true;

        let json = serde_json::to_string(&chain).unwrap();
        let back: TaskChain = serde_json::from_str(&json).unwrap();
        assert_eq!(back.chain_id, "c1");
        assert_eq!(back.name, "MyChain");
        assert_eq!(back.task_ids, vec!["t1", "t2"]);
        assert!(back.auto_remove_completed);
        assert_eq!(back.description, Some("desc".into()));
    }

    #[test]
    fn test_chain_serde_extra_fields_ignored() {
        let json = r#"{"chain_id":"c1","name":"n","task_ids":[],"enabled":true,"created_at":"2026-01-01T00:00:00Z","auto_remove_completed":false,"extra_field":42}"#;
        let chain: TaskChain = serde_json::from_str(json).unwrap();
        assert_eq!(chain.chain_id, "c1");
    }

    #[test]
    fn test_chain_serde_pretty() {
        let chain = TaskChain::new("c1".into(), "n".into());
        let pretty = serde_json::to_string_pretty(&chain).unwrap();
        let back: TaskChain = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.chain_id, chain.chain_id);
    }

    // ===== TaskChain Clone/Debug =====

    #[test]
    fn test_chain_clone_debug() {
        let mut chain = TaskChain::new("c1".into(), "n".into());
        chain.add_task("t1".into()).unwrap();
        let cloned = chain.clone();
        assert_eq!(cloned.chain_id, chain.chain_id);
        assert_eq!(cloned.task_ids, chain.task_ids);
        // Debug works
        let _ = format!("{:?}", chain);
    }

    // ===== TaskChainError Display =====

    #[test]
    fn test_error_display_all_variants() {
        let e1 = TaskChainError::Io(std::io::Error::new(std::io::ErrorKind::Other, "io"));
        assert!(e1.to_string().contains("IO error"));

        let e2 = TaskChainError::Json(serde_json::from_str::<String>("invalid").unwrap_err());
        assert!(e2.to_string().contains("JSON"));

        let e3 = TaskChainError::ChainNotFound("abc".into());
        assert!(e3.to_string().contains("abc"));

        let e4 = TaskChainError::TaskNotInChain("xyz".into());
        assert!(e4.to_string().contains("xyz"));

        let e5 = TaskChainError::ChainFull { max: 10 };
        assert!(e5.to_string().contains("10"));

        let e6 = TaskChainError::CircularDependency;
        assert!(e6.to_string().contains("Circular"));
    }

    #[test]
    fn test_error_debug() {
        let e = TaskChainError::ChainNotFound("x".into());
        let _ = format!("{:?}", e);
    }

    // ===== TaskChainState serde =====

    #[test]
    fn test_state_serde_roundtrip() {
        let mut state = TaskChainState::default();
        let mut chain = TaskChain::new("c1".into(), "n".into());
        chain.add_task("t1".into()).unwrap();
        state.chains.insert("c1".into(), chain);
        state.task_to_chain.insert("t1".into(), "c1".into());

        let json = serde_json::to_string(&state).unwrap();
        let back: TaskChainState = serde_json::from_str(&json).unwrap();
        assert!(back.chains.contains_key("c1"));
        assert_eq!(back.task_to_chain.get("t1"), Some(&"c1".into()));
    }

    #[test]
    fn test_state_default() {
        let state = TaskChainState::default();
        assert!(state.chains.is_empty());
        assert!(state.task_to_chain.is_empty());
    }

    // ===== TaskChainSummary / ChainInfo serde =====

    #[test]
    fn test_summary_serde_roundtrip() {
        let summary = TaskChainSummary {
            total_chains: 2,
            enabled_chains: 1,
            total_tasks: 5,
            chains: vec![ChainInfo {
                chain_id: "c1".into(),
                name: "n".into(),
                task_count: 3,
                enabled: true,
                next_task_id: Some("t1".into()),
            }],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: TaskChainSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_chains, 2);
        assert_eq!(back.enabled_chains, 1);
        assert_eq!(back.total_tasks, 5);
        assert_eq!(back.chains.len(), 1);
    }

    #[test]
    fn test_chain_info_serde() {
        let info = ChainInfo {
            chain_id: "c1".into(),
            name: "n".into(),
            task_count: 0,
            enabled: false,
            next_task_id: None,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: ChainInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.chain_id, "c1");
        assert!(back.next_task_id.is_none());
    }

    #[test]
    fn test_summary_clone_debug() {
        let summary = TaskChainSummary {
            total_chains: 0,
            enabled_chains: 0,
            total_tasks: 0,
            chains: vec![],
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_chains, 0);
        let _ = format!("{:?}", summary);
    }

    // ===== Manager: delete nonexistent =====

    #[test]
    fn test_delete_nonexistent_chain() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        let result = mgr.delete_chain("nope");
        assert!(result.is_err());
    }

    // ===== Manager: add/remove task to nonexistent chain =====

    #[test]
    fn test_add_task_to_nonexistent_chain() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        let result = mgr.add_task_to_chain("nope", "t1".into());
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_task_from_nonexistent_chain() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        let result = mgr.remove_task_from_chain("nope", "t1");
        assert!(result.is_err());
    }

    // ===== Manager: set enabled / auto_remove on nonexistent =====

    #[test]
    fn test_set_enabled_nonexistent() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        assert!(mgr.set_chain_enabled("nope", true).is_err());
    }

    #[test]
    fn test_set_auto_remove_nonexistent() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        assert!(mgr.set_auto_remove_completed("nope", true).is_err());
    }

    // ===== Manager: list/get chains =====

    #[test]
    fn test_list_chains_empty() {
        let mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        assert!(mgr.list_chains().is_empty());
    }

    #[test]
    fn test_list_chains_multiple() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.create_chain("c2".into(), "C2".into()).unwrap();
        assert_eq!(mgr.list_chains().len(), 2);
    }

    #[test]
    fn test_get_chain_nonexistent() {
        let mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        assert!(mgr.get_chain("nope").is_none());
    }

    #[test]
    fn test_get_chain_for_task_nonexistent() {
        let mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        assert!(mgr.get_chain_for_task("nope").is_none());
    }

    // ===== Manager: mark_task_completed without auto_remove =====

    #[test]
    fn test_mark_completed_without_auto_remove() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t2".into()).unwrap();

        // auto_remove is off by default
        let next = mgr.mark_task_completed("t1").unwrap();
        assert_eq!(next, Some(("c1".into(), "t1".into())));
        // t1 still in chain mapping (not auto-removed)
        assert!(mgr.get_chain_for_task("t1").is_some());
    }

    #[test]
    fn test_mark_completed_task_not_in_any_chain() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        let result = mgr.mark_task_completed("orphan");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_mark_completed_last_task_in_chain() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t1".into()).unwrap();
        mgr.set_auto_remove_completed("c1", true).unwrap();

        let next = mgr.mark_task_completed("t1").unwrap();
        assert!(next.is_none()); // no more tasks
        assert!(mgr.get_chain_for_task("t1").is_none()); // removed
    }

    // ===== Manager: get_next_task_after_completion edge cases =====

    #[test]
    fn test_next_after_completion_task_not_in_chain() {
        let mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        assert!(mgr.get_next_task_after_completion("orphan").is_none());
    }

    #[test]
    fn test_next_after_completion_disabled_chain() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t2".into()).unwrap();
        mgr.set_chain_enabled("c1", false).unwrap();

        assert!(mgr.get_next_task_after_completion("t1").is_none());
    }

    // ===== Manager: delete chain cleans up task_to_chain =====

    #[test]
    fn test_delete_chain_cleans_task_mapping() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t2".into()).unwrap();

        assert!(mgr.get_chain_for_task("t1").is_some());
        mgr.delete_chain("c1").unwrap();
        assert!(mgr.get_chain_for_task("t1").is_none());
        assert!(mgr.get_chain_for_task("t2").is_none());
    }

    // ===== Manager: summary details =====

    #[test]
    fn test_summary_enabled_count() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.create_chain("c2".into(), "C2".into()).unwrap();
        mgr.set_chain_enabled("c2", false).unwrap();

        let summary = mgr.get_summary();
        assert_eq!(summary.total_chains, 2);
        assert_eq!(summary.enabled_chains, 1);
    }

    #[test]
    fn test_summary_next_task_id() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t1".into()).unwrap();

        let summary = mgr.get_summary();
        assert_eq!(summary.chains[0].next_task_id, Some("t1".into()));
    }

    #[test]
    fn test_summary_empty_chain_next_task_none() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();

        let summary = mgr.get_summary();
        assert!(summary.chains[0].next_task_id.is_none());
    }

    // ===== Persistence: save creates file =====

    #[tokio::test]
    async fn test_save_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("chains.json");
        let mut mgr = TaskChainManager::new(path.clone());
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.save().await.unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_save_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("chains.json");

        let mut mgr = TaskChainManager::new(path.clone());
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.save().await.unwrap();

        mgr.create_chain("c2".into(), "C2".into()).unwrap();
        mgr.save().await.unwrap();

        let mut loaded = TaskChainManager::new(path);
        loaded.load().await.unwrap();
        assert!(loaded.get_chain("c1").is_some());
        assert!(loaded.get_chain("c2").is_some());
    }

    #[tokio::test]
    async fn test_save_no_tmp_leftover() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("chains.json");
        let mut mgr = TaskChainManager::new(path.clone());
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.save().await.unwrap();

        let tmp_file = path.with_extension("json.tmp");
        assert!(!tmp_file.exists());
    }

    #[tokio::test]
    async fn test_load_missing_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nonexistent.json");
        let mut mgr = TaskChainManager::new(path);
        mgr.load().await.unwrap(); // should not error
        assert!(mgr.list_chains().is_empty());
    }

    #[tokio::test]
    async fn test_load_empty_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("chains.json");
        std::fs::write(&path, "").unwrap();
        let mut mgr = TaskChainManager::new(path);
        mgr.load().await.unwrap();
        assert!(mgr.list_chains().is_empty());
    }

    #[tokio::test]
    async fn test_load_corrupt_json() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("chains.json");
        std::fs::write(&path, "not json").unwrap();
        let mut mgr = TaskChainManager::new(path);
        assert!(mgr.load().await.is_err());
    }

    #[tokio::test]
    async fn test_full_persistence_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("chains.json");

        let mut mgr = TaskChainManager::new(path.clone());
        mgr.create_chain("c1".into(), "Chain Alpha".into()).unwrap();
        mgr.add_task_to_chain("c1", "t1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t2".into()).unwrap();
        mgr.set_auto_remove_completed("c1", true).unwrap();
        mgr.create_chain("c2".into(), "Chain Beta".into()).unwrap();
        mgr.set_chain_enabled("c2", false).unwrap();
        mgr.save().await.unwrap();

        let mut loaded = TaskChainManager::new(path);
        loaded.load().await.unwrap();

        assert!(loaded.get_chain("c1").is_some());
        assert!(loaded.get_chain("c2").is_some());
        assert!(loaded.get_chain_for_task("t1").is_some());
        assert!(loaded.get_chain_for_task("t2").is_some());

        let c1 = loaded.get_chain("c1").unwrap();
        assert!(c1.auto_remove_completed);
        assert_eq!(c1.task_ids.len(), 2);

        let c2 = loaded.get_chain("c2").unwrap();
        assert!(!c2.enabled);
    }

    // ===== Unicode task IDs =====

    #[test]
    fn test_unicode_task_ids() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "链".into()).unwrap();
        mgr.add_task_to_chain("c1", "任务-α".into()).unwrap();
        mgr.add_task_to_chain("c1", "任务-β".into()).unwrap();

        assert!(mgr.get_chain_for_task("任务-α").is_some());
        assert!(mgr.get_chain_for_task("任务-β").is_some());
    }

    #[test]
    fn test_emoji_task_id() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "🔗".into()).unwrap();
        mgr.add_task_to_chain("c1", "🚀".into()).unwrap();
        assert!(mgr.get_chain_for_task("🚀").is_some());
    }

    // ===== Complex workflow =====

    #[test]
    fn test_complete_workflow() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));

        // Create chain
        mgr.create_chain("pipeline".into(), "Build Pipeline".into())
            .unwrap();
        mgr.add_task_to_chain("pipeline", "step-1".into()).unwrap();
        mgr.add_task_to_chain("pipeline", "step-2".into()).unwrap();
        mgr.add_task_to_chain("pipeline", "step-3".into()).unwrap();
        mgr.set_auto_remove_completed("pipeline", true).unwrap();

        // Complete step-1 -> next is step-1 (first in chain before pop)
        let next = mgr.mark_task_completed("step-1").unwrap();
        assert!(next.is_some());
        let (chain_id, next_task) = next.unwrap();
        assert_eq!(chain_id, "pipeline");
        assert_eq!(next_task, "step-2");

        // step-1 removed, step-2 now first
        assert!(mgr.get_chain_for_task("step-1").is_none());

        // Complete step-2
        let next = mgr.mark_task_completed("step-2").unwrap();
        assert_eq!(next.unwrap().1, "step-3");

        // Complete step-3 (last)
        let next = mgr.mark_task_completed("step-3").unwrap();
        assert!(next.is_none()); // no more tasks
    }

    #[test]
    fn test_multiple_chains_independent() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("a".into(), "A".into()).unwrap();
        mgr.create_chain("b".into(), "B".into()).unwrap();
        mgr.add_task_to_chain("a", "t1".into()).unwrap();
        mgr.add_task_to_chain("b", "t2".into()).unwrap();

        // Each task belongs to its own chain
        assert_eq!(mgr.get_chain_for_task("t1").unwrap().chain_id, "a");
        assert_eq!(mgr.get_chain_for_task("t2").unwrap().chain_id, "b");

        // Deleting chain A doesn't affect chain B
        mgr.delete_chain("a").unwrap();
        assert!(mgr.get_chain_for_task("t1").is_none());
        assert!(mgr.get_chain_for_task("t2").is_some());
    }

    // ===== Manager: Clone =====

    #[test]
    fn test_manager_clone() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();
        mgr.add_task_to_chain("c1", "t1".into()).unwrap();

        let cloned = mgr.clone();
        assert!(cloned.get_chain("c1").is_some());
        assert!(cloned.get_chain_for_task("t1").is_some());
    }

    #[test]
    fn test_manager_clone_independence() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();

        let mut cloned = mgr.clone();
        cloned.create_chain("c2".into(), "C2".into()).unwrap();

        // Original not affected
        assert!(mgr.get_chain("c2").is_none());
        assert!(cloned.get_chain("c2").is_some());
    }

    // ===== Manager: Debug =====

    #[test]
    fn test_manager_debug() {
        let mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        let _ = format!("{:?}", mgr);
    }

    // ===== add_task duplicate across different chains =====

    #[test]
    fn test_same_task_added_to_different_chains() {
        // The module allows adding same task_id to different chains
        // (task_to_chain will point to the last one)
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("a".into(), "A".into()).unwrap();
        mgr.create_chain("b".into(), "B".into()).unwrap();
        mgr.add_task_to_chain("a", "t1".into()).unwrap();
        mgr.add_task_to_chain("b", "t1".into()).unwrap();

        // task_to_chain now points to "b"
        assert_eq!(mgr.get_chain_for_task("t1").unwrap().chain_id, "b");
    }

    // ===== set_chain_enabled toggle =====

    #[test]
    fn test_toggle_chain_enabled() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();

        assert!(mgr.get_chain("c1").unwrap().enabled);
        mgr.set_chain_enabled("c1", false).unwrap();
        assert!(!mgr.get_chain("c1").unwrap().enabled);
        mgr.set_chain_enabled("c1", true).unwrap();
        assert!(mgr.get_chain("c1").unwrap().enabled);
    }

    // ===== set_auto_remove_completed toggle =====

    #[test]
    fn test_toggle_auto_remove() {
        let mut mgr = TaskChainManager::new(PathBuf::from("/tmp/x.json"));
        mgr.create_chain("c1".into(), "C1".into()).unwrap();

        assert!(!mgr.get_chain("c1").unwrap().auto_remove_completed);
        mgr.set_auto_remove_completed("c1", true).unwrap();
        assert!(mgr.get_chain("c1").unwrap().auto_remove_completed);
    }

    // ===== get_next_task returns first in order =====

    #[test]
    fn test_get_next_task_order() {
        let mut chain = TaskChain::new("c1".into(), "n".into());
        chain.add_task("first".into()).unwrap();
        chain.add_task("second".into()).unwrap();
        chain.add_task("third".into()).unwrap();
        assert_eq!(chain.get_next_task(), Some("first"));
    }

    // ===== pop order =====

    #[test]
    fn test_pop_order() {
        let mut chain = TaskChain::new("c1".into(), "n".into());
        chain.add_task("a".into()).unwrap();
        chain.add_task("b".into()).unwrap();
        chain.add_task("c".into()).unwrap();

        assert_eq!(chain.pop_completed_task(), Some("a".into()));
        assert_eq!(chain.pop_completed_task(), Some("b".into()));
        assert_eq!(chain.pop_completed_task(), Some("c".into()));
        assert!(chain.pop_completed_task().is_none());
    }

    // ===== Persistence with Unicode =====

    #[tokio::test]
    async fn test_persistence_unicode() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("chains.json");

        let mut mgr = TaskChainManager::new(path.clone());
        mgr.create_chain("链-1".into(), "测试链".into()).unwrap();
        mgr.add_task_to_chain("链-1", "任务-α".into()).unwrap();
        mgr.save().await.unwrap();

        let mut loaded = TaskChainManager::new(path);
        loaded.load().await.unwrap();
        assert!(loaded.get_chain("链-1").is_some());
        assert!(loaded.get_chain_for_task("任务-α").is_some());
    }
}
