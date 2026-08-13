//! Per-task download time limit management
//!
//! Automatically pause downloads that exceed configured time limits.
//! Supports both global default limits and per-task overrides.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Download time limit configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloadTimeLimitConfig {
    /// Enable automatic time limit enforcement
    pub enabled: bool,
    /// Default time limit in seconds (applied to new tasks if they don't have a specific limit)
    pub default_limit_secs: Option<u64>,
    /// Per-task time limit overrides (task_id -> limit_secs)
    pub task_limits: HashMap<String, u64>,
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

    // === Serialization tests ===

    #[test]
    fn test_config_serialization_roundtrip() {
        let mut config = DownloadTimeLimitConfig::default();
        config.enabled = true;
        config.default_limit_secs = Some(7200);
        config.task_limits.insert("t1".to_string(), 3600);
        config.task_limits.insert("t2".to_string(), 1800);

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DownloadTimeLimitConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.enabled, true);
        assert_eq!(deserialized.default_limit_secs, Some(7200));
        assert_eq!(deserialized.task_limits.get("t1"), Some(&3600));
        assert_eq!(deserialized.task_limits.get("t2"), Some(&1800));
    }

    #[test]
    fn test_config_serialization_pretty_format() {
        let mut config = DownloadTimeLimitConfig::default();
        config.enabled = true;
        config.default_limit_secs = Some(3600);

        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains("\"enabled\": true"));
        assert!(pretty.contains("\"default_limit_secs\": 3600"));
    }

    #[test]
    fn test_config_deserialize_extra_fields() {
        let json = r#"{
            "enabled": true,
            "default_limit_secs": 3600,
            "task_limits": {},
            "unknown_field": "should be ignored",
            "another_extra": 42
        }"#;

        let config: DownloadTimeLimitConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.default_limit_secs, Some(3600));
        assert!(config.task_limits.is_empty());
    }

    #[test]
    fn test_config_serialize_empty_task_limits() {
        let config = DownloadTimeLimitConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["task_limits"].as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_config_serialize_none_default_limit() {
        let config = DownloadTimeLimitConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed["default_limit_secs"].is_null());
    }

    // === Default value tests ===

    #[test]
    fn test_config_default_values() {
        let config = DownloadTimeLimitConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.default_limit_secs, None);
        assert!(config.task_limits.is_empty());
    }

    #[test]
    fn test_manager_new_equals_default() {
        let manager = DownloadTimeLimitManager::new();
        let config = manager.config();
        assert!(!config.enabled);
        assert_eq!(config.default_limit_secs, None);
        assert!(config.task_limits.is_empty());
    }

    #[test]
    fn test_manager_from_config() {
        let mut config = DownloadTimeLimitConfig::default();
        config.enabled = true;
        config.default_limit_secs = Some(1800);
        config.task_limits.insert("x".to_string(), 600);

        let manager = DownloadTimeLimitManager::from_config(config);
        assert!(manager.config().enabled);
        assert_eq!(manager.config().default_limit_secs, Some(1800));
        assert_eq!(manager.config().task_limits.get("x"), Some(&600));
    }

    // === Constructor tests ===

    #[test]
    fn test_from_config_preserves_all_fields() {
        let mut config = DownloadTimeLimitConfig::default();
        config.enabled = true;
        config.default_limit_secs = Some(9999);
        config.task_limits.insert("a".to_string(), 100);
        config.task_limits.insert("b".to_string(), 200);
        config.task_limits.insert("c".to_string(), 300);

        let manager = DownloadTimeLimitManager::from_config(config);
        assert_eq!(manager.config().task_limits.len(), 3);
        assert_eq!(manager.config().default_limit_secs, Some(9999));
    }

    // === set_enabled tests ===

    #[test]
    fn test_set_enabled_toggle() {
        let mut manager = DownloadTimeLimitManager::new();
        assert!(!manager.config().enabled);

        manager.set_enabled(true);
        assert!(manager.config().enabled);

        manager.set_enabled(true); // idempotent
        assert!(manager.config().enabled);

        manager.set_enabled(false);
        assert!(!manager.config().enabled);
    }

    // === set_default_limit tests ===

    #[test]
    fn test_set_default_limit_zero() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_default_limit(Some(0));
        assert_eq!(manager.config().default_limit_secs, Some(0));
    }

    #[test]
    fn test_set_default_limit_large_value() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_default_limit(Some(u64::MAX));
        assert_eq!(manager.config().default_limit_secs, Some(u64::MAX));
    }

    #[test]
    fn test_set_default_limit_overwrite() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_default_limit(Some(3600));
        assert_eq!(manager.config().default_limit_secs, Some(3600));

        manager.set_default_limit(Some(7200));
        assert_eq!(manager.config().default_limit_secs, Some(7200));

        manager.set_default_limit(None);
        assert_eq!(manager.config().default_limit_secs, None);
    }

    // === set_task_limit / remove_task_limit tests ===

    #[test]
    fn test_set_task_limit_multiple_tasks() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_task_limit("t1", 100);
        manager.set_task_limit("t2", 200);
        manager.set_task_limit("t3", 300);

        assert_eq!(manager.config().task_limits.get("t1"), Some(&100));
        assert_eq!(manager.config().task_limits.get("t2"), Some(&200));
        assert_eq!(manager.config().task_limits.get("t3"), Some(&300));
        assert_eq!(manager.config().task_limits.len(), 3);
    }

    #[test]
    fn test_set_task_limit_overwrite_existing() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_task_limit("t1", 100);
        assert_eq!(manager.config().task_limits.get("t1"), Some(&100));

        manager.set_task_limit("t1", 999);
        assert_eq!(manager.config().task_limits.get("t1"), Some(&999));
    }

    #[test]
    fn test_set_task_limit_empty_id() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_task_limit("", 100);
        assert_eq!(manager.config().task_limits.get(""), Some(&100));
    }

    #[test]
    fn test_set_task_limit_zero() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_task_limit("t1", 0);
        assert_eq!(manager.config().task_limits.get("t1"), Some(&0));
    }

    #[test]
    fn test_remove_task_limit_nonexistent() {
        let mut manager = DownloadTimeLimitManager::new();
        // Should not panic
        manager.remove_task_limit("nonexistent");
        assert!(manager.config().task_limits.is_empty());
    }

    #[test]
    fn test_remove_then_readd_task_limit() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_task_limit("t1", 100);
        manager.remove_task_limit("t1");
        assert_eq!(manager.config().task_limits.get("t1"), None);

        manager.set_task_limit("t1", 500);
        assert_eq!(manager.config().task_limits.get("t1"), Some(&500));
    }

    // === get_effective_limit tests ===

    #[test]
    fn test_effective_limit_no_limits_returns_none() {
        let manager = DownloadTimeLimitManager::new();
        assert_eq!(manager.get_effective_limit("any-task", None), None);
    }

    #[test]
    fn test_effective_limit_default_only() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_default_limit(Some(3600));

        assert_eq!(manager.get_effective_limit("t1", None), Some(3600));
        assert_eq!(manager.get_effective_limit("t2", None), Some(3600));
        assert_eq!(manager.get_effective_limit("unknown", None), Some(3600));
    }

    #[test]
    fn test_effective_limit_per_task_override() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_default_limit(Some(3600));
        manager.set_task_limit("t1", 1800);

        // t1 uses per-task override
        assert_eq!(manager.get_effective_limit("t1", None), Some(1800));
        // t2 falls back to default
        assert_eq!(manager.get_effective_limit("t2", None), Some(3600));
    }

    #[test]
    fn test_effective_limit_task_field_highest_priority() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_default_limit(Some(3600));
        manager.set_task_limit("t1", 1800);

        // Task field (from task info) takes precedence over everything
        assert_eq!(manager.get_effective_limit("t1", Some(900)), Some(900));
        // Even for tasks without per-task override
        assert_eq!(manager.get_effective_limit("t2", Some(500)), Some(500));
    }

    #[test]
    fn test_effective_limit_task_field_zero() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_default_limit(Some(3600));

        // Zero from task field is still Some(0)
        assert_eq!(manager.get_effective_limit("t1", Some(0)), Some(0));
    }

    #[test]
    fn test_effective_limit_precedence_chain() {
        let mut manager = DownloadTimeLimitManager::new();

        // No limits at all
        assert_eq!(manager.get_effective_limit("t1", None), None);
        assert_eq!(manager.get_effective_limit("t1", Some(100)), Some(100));

        // Only default
        manager.set_default_limit(Some(5000));
        assert_eq!(manager.get_effective_limit("t1", None), Some(5000));
        assert_eq!(manager.get_effective_limit("t1", Some(100)), Some(100));

        // Default + per-task override
        manager.set_task_limit("t1", 3000);
        assert_eq!(manager.get_effective_limit("t1", None), Some(3000));
        assert_eq!(manager.get_effective_limit("t1", Some(100)), Some(100));

        // Other tasks still use default
        assert_eq!(manager.get_effective_limit("t2", None), Some(5000));
    }

    // === should_pause tests ===

    #[test]
    fn test_should_pause_disabled_always_false() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_default_limit(Some(100));
        manager.set_enabled(false);

        // Way over limit but disabled
        assert!(!manager.should_pause("t1", None, 99999.0));
    }

    #[test]
    fn test_should_pause_no_limit_set() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);

        // No limit set for this task, no default
        assert!(!manager.should_pause("t1", None, 99999.0));
    }

    #[test]
    fn test_should_pause_zero_time() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));

        assert!(!manager.should_pause("t1", None, 0.0));
    }

    #[test]
    fn test_should_pause_zero_limit() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(0));

        // Zero limit means any time >= 0 triggers pause
        assert!(manager.should_pause("t1", None, 0.0));
        assert!(manager.should_pause("t1", None, 1.0));
    }

    #[test]
    fn test_should_pause_exact_boundary() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));

        // Just under
        assert!(!manager.should_pause("t1", None, 3599.999));
        // Exact
        assert!(manager.should_pause("t1", None, 3600.0));
        // Just over
        assert!(manager.should_pause("t1", None, 3600.001));
    }

    #[test]
    fn test_should_pause_negative_time() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));

        // Negative time should not trigger
        assert!(!manager.should_pause("t1", None, -1.0));
    }

    #[test]
    fn test_should_pause_with_task_field_limit() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));

        // Task field limit overrides
        assert!(manager.should_pause("t1", Some(100), 150.0));
        assert!(!manager.should_pause("t1", Some(100), 50.0));
    }

    #[test]
    fn test_should_pause_with_per_task_override() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));
        manager.set_task_limit("t1", 600);

        // Uses per-task override (600), not default (3600)
        assert!(manager.should_pause("t1", None, 700.0));
        assert!(!manager.should_pause("t1", None, 500.0));

        // Other tasks still use default
        assert!(!manager.should_pause("t2", None, 700.0));
        assert!(manager.should_pause("t2", None, 3700.0));
    }

    #[test]
    fn test_should_pause_task_field_zero_limit() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);

        // Task field limit of 0 means always pause
        assert!(manager.should_pause("t1", Some(0), 0.0));
    }

    // === Persistence tests ===

    #[test]
    fn test_save_creates_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("time_limit.json");

        let manager = DownloadTimeLimitManager::new();
        manager.save(&config_path).unwrap();

        assert!(config_path.exists());
    }

    #[test]
    fn test_save_overwrite_existing() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("time_limit.json");

        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(1000));
        manager.save(&config_path).unwrap();

        // Overwrite with different config
        manager.set_default_limit(Some(2000));
        manager.save(&config_path).unwrap();

        let loaded = DownloadTimeLimitManager::load(&config_path).unwrap();
        assert_eq!(loaded.config().default_limit_secs, Some(2000));
    }

    #[test]
    fn test_save_load_roundtrip_full_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("time_limit.json");

        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(7200));
        manager.set_task_limit("task-a", 3600);
        manager.set_task_limit("task-b", 1800);
        manager.set_task_limit("task-c", 900);

        manager.save(&config_path).unwrap();
        let loaded = DownloadTimeLimitManager::load(&config_path).unwrap();

        assert!(loaded.config().enabled);
        assert_eq!(loaded.config().default_limit_secs, Some(7200));
        assert_eq!(loaded.config().task_limits.len(), 3);
        assert_eq!(loaded.config().task_limits.get("task-a"), Some(&3600));
        assert_eq!(loaded.config().task_limits.get("task-b"), Some(&1800));
        assert_eq!(loaded.config().task_limits.get("task-c"), Some(&900));
    }

    #[test]
    fn test_load_invalid_json() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("bad.json");
        std::fs::write(&config_path, "not valid json{{{").unwrap();

        let result = DownloadTimeLimitManager::load(&config_path);
        assert!(result.is_err());
        match result.unwrap_err() {
            DownloadTimeLimitError::Parse(msg) => {
                assert!(!msg.is_empty());
            }
            other => panic!("Expected Parse error, got: {:?}", other),
        }
    }

    #[test]
    fn test_load_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("empty.json");
        std::fs::write(&config_path, "").unwrap();

        let result = DownloadTimeLimitManager::load(&config_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_save_no_temp_file_left() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("time_limit.json");

        let manager = DownloadTimeLimitManager::new();
        manager.save(&config_path).unwrap();

        // Check no .tmp file left behind
        let tmp_path = config_path.with_extension("tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_persistence_unicode_task_ids() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("time_limit.json");

        let mut manager = DownloadTimeLimitManager::new();
        manager.set_task_limit("任务-中文", 1000);
        manager.set_task_limit("task-🚀-emoji", 2000);
        manager.set_task_limit("task with spaces", 3000);

        manager.save(&config_path).unwrap();
        let loaded = DownloadTimeLimitManager::load(&config_path).unwrap();

        assert_eq!(loaded.config().task_limits.get("任务-中文"), Some(&1000));
        assert_eq!(
            loaded.config().task_limits.get("task-🚀-emoji"),
            Some(&2000)
        );
        assert_eq!(
            loaded.config().task_limits.get("task with spaces"),
            Some(&3000)
        );
    }

    // === Error Display tests ===

    #[test]
    fn test_error_display_io() {
        let err = DownloadTimeLimitError::Io("disk full".to_string());
        assert_eq!(format!("{}", err), "IO error: disk full");
    }

    #[test]
    fn test_error_display_parse() {
        let err = DownloadTimeLimitError::Parse("invalid json".to_string());
        assert_eq!(format!("{}", err), "Parse error: invalid json");
    }

    #[test]
    fn test_error_display_serialize() {
        let err = DownloadTimeLimitError::Serialize("cannot serialize".to_string());
        assert_eq!(format!("{}", err), "Serialize error: cannot serialize");
    }

    #[test]
    fn test_error_debug() {
        let err = DownloadTimeLimitError::Io("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Io"));
        assert!(debug_str.contains("test"));
    }

    // === Clone/Debug trait tests ===

    #[test]
    fn test_config_clone() {
        let mut config = DownloadTimeLimitConfig::default();
        config.enabled = true;
        config.default_limit_secs = Some(3600);
        config.task_limits.insert("t1".to_string(), 100);

        let cloned = config.clone();
        assert_eq!(cloned.enabled, true);
        assert_eq!(cloned.default_limit_secs, Some(3600));
        assert_eq!(cloned.task_limits.get("t1"), Some(&100));
    }

    #[test]
    fn test_config_debug() {
        let config = DownloadTimeLimitConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("DownloadTimeLimitConfig"));
        assert!(debug_str.contains("enabled"));
    }

    #[test]
    fn test_manager_clone() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));

        let cloned = manager.clone();
        assert!(cloned.config().enabled);
        assert_eq!(cloned.config().default_limit_secs, Some(3600));
    }

    #[test]
    fn test_manager_debug() {
        let manager = DownloadTimeLimitManager::new();
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("DownloadTimeLimitManager"));
    }

    // === Edge case / complex scenario tests ===

    #[test]
    fn test_many_task_limits() {
        let mut manager = DownloadTimeLimitManager::new();
        for i in 0..100 {
            manager.set_task_limit(&format!("task-{}", i), i as u64 * 60);
        }
        assert_eq!(manager.config().task_limits.len(), 100);

        for i in 0..100 {
            assert_eq!(
                manager.config().task_limits.get(&format!("task-{}", i)),
                Some(&(i as u64 * 60))
            );
        }
    }

    #[test]
    fn test_disable_does_not_clear_limits() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));
        manager.set_task_limit("t1", 1800);

        manager.set_enabled(false);

        // Limits are still there, just not enforced
        assert_eq!(manager.get_effective_limit("t1", None), Some(1800));
        assert_eq!(manager.get_effective_limit("t2", None), Some(3600));
        assert!(!manager.should_pause("t1", None, 99999.0));
    }

    #[test]
    fn test_reenable_enforces_limits_again() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(100));

        assert!(manager.should_pause("t1", None, 200.0));

        manager.set_enabled(false);
        assert!(!manager.should_pause("t1", None, 200.0));

        manager.set_enabled(true);
        assert!(manager.should_pause("t1", None, 200.0));
    }

    #[test]
    fn test_full_workflow() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("time_limit.json");

        // Create and configure
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(3600));
        manager.set_task_limit("vip-task", 7200);
        manager.set_task_limit("low-priority", 600);

        // Verify behavior
        assert!(!manager.should_pause("normal", None, 3500.0));
        assert!(manager.should_pause("normal", None, 3600.0));
        assert!(!manager.should_pause("vip-task", None, 3600.0));
        assert!(manager.should_pause("vip-task", None, 7200.0));
        assert!(manager.should_pause("low-priority", None, 600.0));

        // Persist
        manager.save(&config_path).unwrap();

        // Reload and verify
        let loaded = DownloadTimeLimitManager::load(&config_path).unwrap();
        assert!(loaded.config().enabled);
        assert!(!loaded.should_pause("normal", None, 3500.0));
        assert!(loaded.should_pause("normal", None, 3600.0));
        assert!(!loaded.should_pause("vip-task", None, 7199.0));
        assert!(loaded.should_pause("low-priority", None, 601.0));
    }

    #[test]
    fn test_config_accessor_returns_reference() {
        let mut manager = DownloadTimeLimitManager::new();
        manager.set_enabled(true);
        manager.set_default_limit(Some(42));

        let config_ref = manager.config();
        assert!(config_ref.enabled);
        assert_eq!(config_ref.default_limit_secs, Some(42));
    }
}
