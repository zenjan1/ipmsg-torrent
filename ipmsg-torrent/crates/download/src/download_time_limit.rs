//! Per-task download time limit management
//!
//! Automatically pause downloads that exceed configured time limits.
//! Supports both global default limits and per-task overrides.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Download time limit configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTimeLimitConfig {
    /// Enable automatic time limit enforcement
    pub enabled: bool,
    /// Default time limit in seconds (applied to new tasks if they don't have a specific limit)
    pub default_limit_secs: Option<u64>,
    /// Per-task time limit overrides (task_id -> limit_secs)
    pub task_limits: HashMap<String, u64>,
}

impl Default for DownloadTimeLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_limit_secs: None,
            task_limits: HashMap::new(),
        }
    }
}

/// Download time limit manager
#[derive(Debug, Clone)]
pub struct DownloadTimeLimitManager {
    config: DownloadTimeLimitConfig,
}

impl DownloadTimeLimitManager {
    /// Create a new manager with default configuration
    pub fn new() -> Self {
        Self {
            config: DownloadTimeLimitConfig::default(),
        }
    }

    /// Create a new manager from existing configuration
    pub fn from_config(config: DownloadTimeLimitConfig) -> Self {
        Self { config }
    }

    /// Get the current configuration
    pub fn config(&self) -> &DownloadTimeLimitConfig {
        &self.config
    }

    /// Enable or disable time limit enforcement
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    /// Set the default time limit for new tasks
    pub fn set_default_limit(&mut self, limit_secs: Option<u64>) {
        self.config.default_limit_secs = limit_secs;
    }

    /// Set a per-task time limit override
    pub fn set_task_limit(&mut self, task_id: &str, limit_secs: u64) {
        self.config
            .task_limits
            .insert(task_id.to_string(), limit_secs);
    }

    /// Remove a per-task time limit override
    pub fn remove_task_limit(&mut self, task_id: &str) {
        self.config.task_limits.remove(task_id);
    }

    /// Get the effective time limit for a task
    /// Returns None if no limit is set
    pub fn get_effective_limit(&self, task_id: &str, task_limit: Option<u64>) -> Option<u64> {
        // Task-specific limit takes precedence
        if let Some(limit) = task_limit {
            return Some(limit);
        }

        // Check per-task override
        if let Some(&limit) = self.config.task_limits.get(task_id) {
            return Some(limit);
        }

        // Fall back to default
        self.config.default_limit_secs
    }

    /// Check if a task has exceeded its time limit
    /// Returns true if the task should be paused
    pub fn should_pause(
        &self,
        task_id: &str,
        task_limit: Option<u64>,
        active_time_secs: f64,
    ) -> bool {
        if !self.config.enabled {
            return false;
        }

        if let Some(limit) = self.get_effective_limit(task_id, task_limit) {
            active_time_secs >= limit as f64
        } else {
            false
        }
    }

    /// Save configuration to disk
    pub fn save(&self, path: &Path) -> Result<(), DownloadTimeLimitError> {
        let content = serde_json::to_string_pretty(&self.config)
            .map_err(|e| DownloadTimeLimitError::Serialize(e.to_string()))?;

        let temp_path = path.with_extension("tmp");
        std::fs::write(&temp_path, &content)
            .map_err(|e| DownloadTimeLimitError::Io(e.to_string()))?;

        std::fs::rename(&temp_path, path).map_err(|e| DownloadTimeLimitError::Io(e.to_string()))?;

        Ok(())
    }

    /// Load configuration from disk
    pub fn load(path: &Path) -> Result<Self, DownloadTimeLimitError> {
        if !path.exists() {
            return Ok(Self::new());
        }

        let content =
            std::fs::read_to_string(path).map_err(|e| DownloadTimeLimitError::Io(e.to_string()))?;

        let config: DownloadTimeLimitConfig = serde_json::from_str(&content)
            .map_err(|e| DownloadTimeLimitError::Parse(e.to_string()))?;

        Ok(Self::from_config(config))
    }
}

/// Errors that can occur during download time limit operations
#[derive(Debug, thiserror::Error)]
pub enum DownloadTimeLimitError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Serialize error: {0}")]
    Serialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let manager = DownloadTimeLimitManager::new();
        let config = manager.config();

        assert!(!config.enabled);
        assert_eq!(config.default_limit_secs, None);
        assert!(config.task_limits.is_empty());
    }

    #[test]
    fn test_set_enabled() {
        let mut manager = DownloadTimeLimitManager::new();

        manager.set_enabled(true);
        assert!(manager.config().enabled);

        manager.set_enabled(false);
        assert!(!manager.config().enabled);
    }

    #[test]
    fn test_set_default_limit() {
        let mut manager = DownloadTimeLimitManager::new();

        manager.set_default_limit(Some(3600));
        assert_eq!(manager.config().default_limit_secs, Some(3600));

        manager.set_default_limit(None);
        assert_eq!(manager.config().default_limit_secs, None);
    }

    #[test]
    fn test_set_task_limit() {
        let mut manager = DownloadTimeLimitManager::new();

        manager.set_task_limit("task-1", 1800);
        assert_eq!(manager.config().task_limits.get("task-1"), Some(&1800));

        manager.remove_task_limit("task-1");
        assert_eq!(manager.config().task_limits.get("task-1"), None);
    }

    #[test]
    fn test_get_effective_limit() {
        let mut manager = DownloadTimeLimitManager::new();

        // No limits set
        assert_eq!(manager.get_effective_limit("task-1", None), None);

        // Set default
        manager.set_default_limit(Some(3600));
        assert_eq!(manager.get_effective_limit("task-1", None), Some(3600));

        // Task-specific override
        manager.set_task_limit("task-1", 1800);
        assert_eq!(manager.get_effective_limit("task-1", None), Some(1800));

        // Task field takes precedence
        assert_eq!(manager.get_effective_limit("task-1", Some(900)), Some(900));
    }

    #[test]
    fn test_should_pause() {
        let mut manager = DownloadTimeLimitManager::new();

        // Disabled - should not pause
        manager.set_default_limit(Some(3600));
        assert!(!manager.should_pause("task-1", None, 4000.0));

        // Enable
        manager.set_enabled(true);

        // Under limit
        assert!(!manager.should_pause("task-1", None, 3500.0));

        // At limit
        assert!(manager.should_pause("task-1", None, 3600.0));

        // Over limit
        assert!(manager.should_pause("task-1", None, 4000.0));

        // Task-specific limit
        manager.set_task_limit("task-1", 1800);
        assert!(manager.should_pause("task-1", None, 1900.0));
        assert!(!manager.should_pause("task-1", None, 1700.0));

        // Task field override
        assert!(manager.should_pause("task-1", Some(900), 1000.0));
        assert!(!manager.should_pause("task-1", Some(900), 800.0));
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("download_time_limit.json");

        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));
        manager.set_task_limit("task-1", 1800);

        manager.save(&config_path).unwrap();

        // Load into new manager
        let manager2 = DownloadTimeLimitManager::load(&config_path).unwrap();

        assert!(manager2.config().enabled);
        assert_eq!(manager2.config().default_limit_secs, Some(3600));
        assert_eq!(manager2.config().task_limits.get("task-1"), Some(&1800));
    }

    #[test]
    fn test_load_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("nonexistent.json");

        let manager = DownloadTimeLimitManager::load(&config_path).unwrap();
        assert!(!manager.config().enabled);
        assert_eq!(manager.config().default_limit_secs, None);
    }
}
