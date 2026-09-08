//! Per-task proxy override configuration
//!
//! Allows individual download tasks to use a different proxy than the global proxy setting.
//! When a task has a proxy override, it takes precedence over the global proxy.

use crate::proxy::ProxyConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;

/// Per-task proxy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProxyConfig {
    /// Task ID this proxy override applies to
    pub task_id: String,
    /// Proxy configuration for this task
    pub proxy: ProxyConfig,
    /// Whether this override is enabled
    pub enabled: bool,
    /// Optional description/notes
    pub notes: Option<String>,
}

impl TaskProxyConfig {
    /// Create a new task proxy override
    pub fn new(task_id: String, proxy: ProxyConfig) -> Self {
        Self {
            task_id,
            proxy,
            enabled: true,
            notes: None,
        }
    }

    /// Create with optional notes
    pub fn with_notes(task_id: String, proxy: ProxyConfig, notes: Option<String>) -> Self {
        Self {
            task_id,
            proxy,
            enabled: true,
            notes,
        }
    }

    /// Check if this override should be used for the task
    pub fn is_active(&self) -> bool {
        self.enabled
    }
}

/// Manager for per-task proxy overrides
#[derive(Debug, Clone)]
pub struct TaskProxyManager {
    /// Map of task_id -> TaskProxyConfig
    overrides: HashMap<String, TaskProxyConfig>,
    /// Path to persist configuration
    config_path: PathBuf,
}

impl TaskProxyManager {
    /// Create a new manager
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            overrides: HashMap::new(),
            config_path,
        }
    }

    /// Add or update a task proxy override
    pub async fn set_task_proxy(
        &mut self,
        task_id: String,
        proxy: ProxyConfig,
        notes: Option<String>,
    ) -> Result<(), TaskProxyError> {
        let config = TaskProxyConfig::with_notes(task_id.clone(), proxy, notes);
        self.overrides.insert(task_id, config);
        self.save().await
    }

    /// Remove a task proxy override
    pub async fn remove_task_proxy(&mut self, task_id: &str) -> Result<(), TaskProxyError> {
        self.overrides.remove(task_id);
        self.save().await
    }

    /// Get the proxy override for a task (if any and if enabled)
    pub fn get_task_proxy(&self, task_id: &str) -> Option<&TaskProxyConfig> {
        self.overrides.get(task_id).filter(|c| c.is_active())
    }

    /// Get the proxy override config regardless of enabled state
    pub fn get_task_proxy_raw(&self, task_id: &str) -> Option<&TaskProxyConfig> {
        self.overrides.get(task_id)
    }

    /// List all task proxy overrides
    pub fn list_overrides(&self) -> Vec<&TaskProxyConfig> {
        self.overrides.values().collect()
    }

    /// Enable or disable a task proxy override
    pub async fn set_enabled(
        &mut self,
        task_id: &str,
        enabled: bool,
    ) -> Result<(), TaskProxyError> {
        if let Some(config) = self.overrides.get_mut(task_id) {
            config.enabled = enabled;
            self.save().await
        } else {
            Err(TaskProxyError::TaskNotFound(task_id.to_string()))
        }
    }

    /// Update notes for a task proxy override
    pub async fn set_notes(
        &mut self,
        task_id: &str,
        notes: Option<String>,
    ) -> Result<(), TaskProxyError> {
        if let Some(config) = self.overrides.get_mut(task_id) {
            config.notes = notes;
            self.save().await
        } else {
            Err(TaskProxyError::TaskNotFound(task_id.to_string()))
        }
    }

    /// Clear all overrides
    pub async fn clear_all(&mut self) -> Result<(), TaskProxyError> {
        self.overrides.clear();
        self.save().await
    }

    /// Get summary statistics
    pub fn get_summary(&self) -> TaskProxySummary {
        let total = self.overrides.len();
        let enabled = self.overrides.values().filter(|c| c.enabled).count();
        let disabled = total - enabled;

        TaskProxySummary {
            total_overrides: total,
            enabled_overrides: enabled,
            disabled_overrides: disabled,
        }
    }

    /// Save configuration to disk (atomic write)
    async fn save(&self) -> Result<(), TaskProxyError> {
        let config_data: Vec<&TaskProxyConfig> = self.overrides.values().collect();
        let json = serde_json::to_string_pretty(&config_data)
            .map_err(|e| TaskProxyError::Serialization(e.to_string()))?;

        // Atomic write: write to temp file then rename
        let temp_path = self.config_path.with_extension("json.tmp");
        fs::write(&temp_path, &json)
            .await
            .map_err(|e| TaskProxyError::Io(e.to_string()))?;

        fs::rename(&temp_path, &self.config_path)
            .await
            .map_err(|e| TaskProxyError::Io(e.to_string()))?;

        Ok(())
    }

    /// Load configuration from disk
    pub async fn load(config_path: PathBuf) -> Result<Self, TaskProxyError> {
        if !config_path.exists() {
            return Ok(Self::new(config_path));
        }

        let json = fs::read_to_string(&config_path)
            .await
            .map_err(|e| TaskProxyError::Io(e.to_string()))?;

        let configs: Vec<TaskProxyConfig> = serde_json::from_str(&json)
            .map_err(|e| TaskProxyError::Deserialization(e.to_string()))?;

        let mut overrides = HashMap::new();
        for config in configs {
            overrides.insert(config.task_id.clone(), config);
        }

        Ok(Self {
            overrides,
            config_path,
        })
    }
}

/// Summary of task proxy overrides
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProxySummary {
    pub total_overrides: usize,
    pub enabled_overrides: usize,
    pub disabled_overrides: usize,
}

impl TaskProxySummary {
    /// Format summary for display
    pub fn format_summary(&self) -> String {
        format!(
            "Task Proxy Overrides: {} total ({} enabled, {} disabled)",
            self.total_overrides, self.enabled_overrides, self.disabled_overrides
        )
    }
}

/// Errors for task proxy operations
#[derive(Debug, thiserror::Error)]
pub enum TaskProxyError {
    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{ProxyAuth, ProxyConfig, ProxyType};
    use tempfile::tempdir;

    fn create_test_proxy() -> ProxyConfig {
        ProxyConfig::new(ProxyType::Socks5, "127.0.0.1".to_string(), 1080)
    }

    #[tokio::test]
    async fn test_task_proxy_creation() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::new("task-123".to_string(), proxy.clone());

        assert_eq!(config.task_id, "task-123");
        assert_eq!(config.proxy.host, "127.0.0.1");
        assert!(config.enabled);
        assert!(config.notes.is_none());
        assert!(config.is_active());
    }

    #[tokio::test]
    async fn test_task_proxy_with_notes() {
        let proxy = create_test_proxy();
        let notes = Some("Test proxy server".to_string());
        let config = TaskProxyConfig::with_notes("task-456".to_string(), proxy, notes.clone());

        assert_eq!(config.notes, notes);
    }

    #[tokio::test]
    async fn test_manager_set_and_get() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-1".to_string(), proxy, None)
            .await
            .unwrap();

        let config = manager.get_task_proxy("task-1");
        assert!(config.is_some());
        assert_eq!(config.unwrap().task_id, "task-1");
    }

    #[tokio::test]
    async fn test_manager_remove() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-1".to_string(), proxy, None)
            .await
            .unwrap();

        assert!(manager.get_task_proxy("task-1").is_some());

        manager.remove_task_proxy("task-1").await.unwrap();

        assert!(manager.get_task_proxy("task-1").is_none());
    }

    #[tokio::test]
    async fn test_manager_enable_disable() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-1".to_string(), proxy, None)
            .await
            .unwrap();

        // Initially enabled
        assert!(manager.get_task_proxy("task-1").is_some());

        // Disable
        manager.set_enabled("task-1", false).await.unwrap();
        assert!(manager.get_task_proxy("task-1").is_none()); // get_task_proxy filters by enabled

        // But raw get still returns it
        assert!(manager.get_task_proxy_raw("task-1").is_some());

        // Re-enable
        manager.set_enabled("task-1", true).await.unwrap();
        assert!(manager.get_task_proxy("task-1").is_some());
    }

    #[tokio::test]
    async fn test_manager_list_overrides() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy1 = create_test_proxy();
        let proxy2 = ProxyConfig::new(ProxyType::Http, "192.168.1.1".to_string(), 8080);

        manager
            .set_task_proxy("task-1".to_string(), proxy1, None)
            .await
            .unwrap();
        manager
            .set_task_proxy("task-2".to_string(), proxy2, None)
            .await
            .unwrap();

        let overrides = manager.list_overrides();
        assert_eq!(overrides.len(), 2);
    }

    #[tokio::test]
    async fn test_manager_clear_all() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-1".to_string(), proxy.clone(), None)
            .await
            .unwrap();
        manager
            .set_task_proxy("task-2".to_string(), proxy, None)
            .await
            .unwrap();

        assert_eq!(manager.list_overrides().len(), 2);

        manager.clear_all().await.unwrap();

        assert_eq!(manager.list_overrides().len(), 0);
    }

    #[tokio::test]
    async fn test_manager_summary() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-1".to_string(), proxy.clone(), None)
            .await
            .unwrap();
        manager
            .set_task_proxy("task-2".to_string(), proxy, None)
            .await
            .unwrap();

        manager.set_enabled("task-2", false).await.unwrap();

        let summary = manager.get_summary();
        assert_eq!(summary.total_overrides, 2);
        assert_eq!(summary.enabled_overrides, 1);
        assert_eq!(summary.disabled_overrides, 1);
    }

    #[tokio::test]
    async fn test_manager_persistence() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        // Create and populate manager
        {
            let mut manager = TaskProxyManager::new(config_path.clone());
            let proxy = create_test_proxy();
            manager
                .set_task_proxy("task-1".to_string(), proxy, Some("Test notes".to_string()))
                .await
                .unwrap();
        }

        // Load from disk
        let loaded = TaskProxyManager::load(config_path.clone()).await.unwrap();
        let config = loaded.get_task_proxy_raw("task-1");
        assert!(config.is_some());
        assert_eq!(config.unwrap().notes, Some("Test notes".to_string()));
    }

    #[tokio::test]
    async fn test_manager_load_nonexistent() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        let manager = TaskProxyManager::load(config_path).await.unwrap();
        assert_eq!(manager.list_overrides().len(), 0);
    }

    #[tokio::test]
    async fn test_set_notes() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-1".to_string(), proxy, None)
            .await
            .unwrap();

        assert!(
            manager
                .get_task_proxy_raw("task-1")
                .unwrap()
                .notes
                .is_none()
        );

        manager
            .set_notes("task-1", Some("Updated notes".to_string()))
            .await
            .unwrap();

        assert_eq!(
            manager.get_task_proxy_raw("task-1").unwrap().notes,
            Some("Updated notes".to_string())
        );
    }

    #[tokio::test]
    async fn test_set_notes_not_found() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let result = manager
            .set_notes("nonexistent", Some("Notes".to_string()))
            .await;
        assert!(matches!(result, Err(TaskProxyError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn test_enable_not_found() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let result = manager.set_enabled("nonexistent", true).await;
        assert!(matches!(result, Err(TaskProxyError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn test_summary_format() {
        let summary = TaskProxySummary {
            total_overrides: 5,
            enabled_overrides: 3,
            disabled_overrides: 2,
        };

        let formatted = summary.format_summary();
        assert!(formatted.contains("5 total"));
        assert!(formatted.contains("3 enabled"));
        assert!(formatted.contains("2 disabled"));
    }

    // ===== Serialization tests =====

    #[tokio::test]
    async fn test_task_proxy_config_serialization_roundtrip() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::with_notes(
            "task-ser".to_string(),
            proxy,
            Some("roundtrip test".to_string()),
        );
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TaskProxyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task-ser");
        assert_eq!(deserialized.notes, Some("roundtrip test".to_string()));
        assert!(deserialized.enabled);
    }

    #[tokio::test]
    async fn test_task_proxy_config_serialization_without_notes() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::new("task-no-notes".to_string(), proxy);
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TaskProxyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task-no-notes");
        assert!(deserialized.notes.is_none());
    }

    #[tokio::test]
    async fn test_task_proxy_summary_serialization() {
        let summary = TaskProxySummary {
            total_overrides: 10,
            enabled_overrides: 7,
            disabled_overrides: 3,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: TaskProxySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_overrides, 10);
        assert_eq!(deserialized.enabled_overrides, 7);
        assert_eq!(deserialized.disabled_overrides, 3);
    }

    #[tokio::test]
    async fn test_task_proxy_config_json_structure() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::new("task-json".to_string(), proxy);
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains("\"task_id\""));
        assert!(json.contains("\"task-json\""));
        assert!(json.contains("\"enabled\""));
        assert!(json.contains("\"proxy\""));
    }

    // ===== Error Display tests =====

    #[tokio::test]
    async fn test_error_display_task_not_found() {
        let err = TaskProxyError::TaskNotFound("abc-123".to_string());
        assert_eq!(err.to_string(), "Task not found: abc-123");
    }

    #[tokio::test]
    async fn test_error_display_io() {
        let err = TaskProxyError::Io("permission denied".to_string());
        assert_eq!(err.to_string(), "I/O error: permission denied");
    }

    #[tokio::test]
    async fn test_error_display_serialization() {
        let err = TaskProxyError::Serialization("invalid data".to_string());
        assert_eq!(err.to_string(), "Serialization error: invalid data");
    }

    #[tokio::test]
    async fn test_error_display_deserialization() {
        let err = TaskProxyError::Deserialization("corrupt file".to_string());
        assert_eq!(err.to_string(), "Deserialization error: corrupt file");
    }

    // ===== Boundary condition tests =====

    #[tokio::test]
    async fn test_empty_task_id() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("".to_string(), proxy, None)
            .await
            .unwrap();

        assert!(manager.get_task_proxy("").is_some());
    }

    #[tokio::test]
    async fn test_overwrite_existing_proxy() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy1 = create_test_proxy();
        let proxy2 = ProxyConfig::new(ProxyType::Http, "10.0.0.1".to_string(), 3128);

        manager
            .set_task_proxy("task-ow".to_string(), proxy1, Some("first".to_string()))
            .await
            .unwrap();
        manager
            .set_task_proxy("task-ow".to_string(), proxy2, Some("second".to_string()))
            .await
            .unwrap();

        let config = manager.get_task_proxy_raw("task-ow").unwrap();
        assert_eq!(config.proxy.host, "10.0.0.1");
        assert_eq!(config.notes, Some("second".to_string()));
        assert_eq!(manager.list_overrides().len(), 1);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_task() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        // Remove on empty should succeed (no error)
        let result = manager.remove_task_proxy("nonexistent").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_task_proxy_disabled_returns_none() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-dis".to_string(), proxy, None)
            .await
            .unwrap();

        manager.set_enabled("task-dis", false).await.unwrap();

        // get_task_proxy filters by enabled
        assert!(manager.get_task_proxy("task-dis").is_none());
        // get_task_proxy_raw returns regardless of enabled
        assert!(manager.get_task_proxy_raw("task-dis").is_some());
    }

    // ===== Summary edge case tests =====

    #[tokio::test]
    async fn test_summary_empty_manager() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let manager = TaskProxyManager::new(config_path);

        let summary = manager.get_summary();
        assert_eq!(summary.total_overrides, 0);
        assert_eq!(summary.enabled_overrides, 0);
        assert_eq!(summary.disabled_overrides, 0);
    }

    #[tokio::test]
    async fn test_summary_all_enabled() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        for i in 0..5 {
            manager
                .set_task_proxy(format!("task-{}", i), proxy.clone(), None)
                .await
                .unwrap();
        }

        let summary = manager.get_summary();
        assert_eq!(summary.total_overrides, 5);
        assert_eq!(summary.enabled_overrides, 5);
        assert_eq!(summary.disabled_overrides, 0);
    }

    #[tokio::test]
    async fn test_summary_all_disabled() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        for i in 0..3 {
            manager
                .set_task_proxy(format!("task-{}", i), proxy.clone(), None)
                .await
                .unwrap();
            manager
                .set_enabled(&format!("task-{}", i), false)
                .await
                .unwrap();
        }

        let summary = manager.get_summary();
        assert_eq!(summary.total_overrides, 3);
        assert_eq!(summary.enabled_overrides, 0);
        assert_eq!(summary.disabled_overrides, 3);
    }

    #[tokio::test]
    async fn test_summary_format_zero_values() {
        let summary = TaskProxySummary {
            total_overrides: 0,
            enabled_overrides: 0,
            disabled_overrides: 0,
        };
        let formatted = summary.format_summary();
        assert!(formatted.contains("0 total"));
        assert!(formatted.contains("0 enabled"));
        assert!(formatted.contains("0 disabled"));
    }

    // ===== Persistence edge case tests =====

    #[tokio::test]
    async fn test_persistence_multiple_entries() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        {
            let mut manager = TaskProxyManager::new(config_path.clone());
            let proxy1 = create_test_proxy();
            let proxy2 = ProxyConfig::new(ProxyType::Http, "proxy.example.com".to_string(), 8080);
            let proxy3 = ProxyConfig::new(ProxyType::Socks5, "10.10.10.10".to_string(), 9050);

            manager
                .set_task_proxy("task-a".to_string(), proxy1, Some("A".to_string()))
                .await
                .unwrap();
            manager
                .set_task_proxy("task-b".to_string(), proxy2, Some("B".to_string()))
                .await
                .unwrap();
            manager
                .set_task_proxy("task-c".to_string(), proxy3, None)
                .await
                .unwrap();
        }

        let loaded = TaskProxyManager::load(config_path).await.unwrap();
        assert_eq!(loaded.list_overrides().len(), 3);
        assert_eq!(
            loaded.get_task_proxy_raw("task-a").unwrap().proxy.host,
            "127.0.0.1"
        );
        assert_eq!(
            loaded.get_task_proxy_raw("task-b").unwrap().proxy.host,
            "proxy.example.com"
        );
        assert_eq!(loaded.get_task_proxy_raw("task-c").unwrap().notes, None);
    }

    #[tokio::test]
    async fn test_persistence_with_disabled_entries() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        {
            let mut manager = TaskProxyManager::new(config_path.clone());
            let proxy = create_test_proxy();
            manager
                .set_task_proxy("task-en".to_string(), proxy.clone(), None)
                .await
                .unwrap();
            manager
                .set_task_proxy("task-dis".to_string(), proxy, None)
                .await
                .unwrap();
            manager.set_enabled("task-dis", false).await.unwrap();
        }

        let loaded = TaskProxyManager::load(config_path).await.unwrap();
        assert!(loaded.get_task_proxy_raw("task-en").unwrap().enabled);
        assert!(!loaded.get_task_proxy_raw("task-dis").unwrap().enabled);
    }

    #[tokio::test]
    async fn test_load_invalid_json() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        // Write invalid JSON
        tokio::fs::write(&config_path, "not valid json{{{")
            .await
            .unwrap();

        let result = TaskProxyManager::load(config_path).await;
        assert!(matches!(result, Err(TaskProxyError::Deserialization(_))));
    }

    #[tokio::test]
    async fn test_load_empty_file() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        tokio::fs::write(&config_path, "").await.unwrap();

        let result = TaskProxyManager::load(config_path).await;
        assert!(matches!(result, Err(TaskProxyError::Deserialization(_))));
    }

    #[tokio::test]
    async fn test_load_empty_json_array() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        tokio::fs::write(&config_path, "[]").await.unwrap();

        let loaded = TaskProxyManager::load(config_path).await.unwrap();
        assert_eq!(loaded.list_overrides().len(), 0);
    }

    // ===== is_active tests =====

    #[tokio::test]
    async fn test_is_active_enabled() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::new("task-act".to_string(), proxy);
        assert!(config.is_active());
    }

    #[tokio::test]
    async fn test_is_active_disabled() {
        let proxy = create_test_proxy();
        let mut config = TaskProxyConfig::new("task-act2".to_string(), proxy);
        config.enabled = false;
        assert!(!config.is_active());
    }

    // ===== Multiple operations tests =====

    #[tokio::test]
    async fn test_set_remove_set_cycle() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy1 = create_test_proxy();
        let proxy2 = ProxyConfig::new(ProxyType::Http, "new-proxy.example.com".to_string(), 3128);

        // Set
        manager
            .set_task_proxy("task-cycle".to_string(), proxy1, None)
            .await
            .unwrap();
        assert!(manager.get_task_proxy("task-cycle").is_some());

        // Remove
        manager.remove_task_proxy("task-cycle").await.unwrap();
        assert!(manager.get_task_proxy("task-cycle").is_none());

        // Set again with different proxy
        manager
            .set_task_proxy(
                "task-cycle".to_string(),
                proxy2,
                Some("re-added".to_string()),
            )
            .await
            .unwrap();
        let config = manager.get_task_proxy_raw("task-cycle").unwrap();
        assert_eq!(config.proxy.host, "new-proxy.example.com");
        assert_eq!(config.notes, Some("re-added".to_string()));
    }

    #[tokio::test]
    async fn test_set_notes_to_none() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy(
                "task-nn".to_string(),
                proxy,
                Some("initial notes".to_string()),
            )
            .await
            .unwrap();

        assert!(
            manager
                .get_task_proxy_raw("task-nn")
                .unwrap()
                .notes
                .is_some()
        );

        // Set notes to None
        manager.set_notes("task-nn", None).await.unwrap();
        assert!(
            manager
                .get_task_proxy_raw("task-nn")
                .unwrap()
                .notes
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_clear_all_then_re_add() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-ca1".to_string(), proxy.clone(), None)
            .await
            .unwrap();
        manager
            .set_task_proxy("task-ca2".to_string(), proxy, None)
            .await
            .unwrap();

        manager.clear_all().await.unwrap();
        assert_eq!(manager.list_overrides().len(), 0);
        assert_eq!(manager.get_summary().total_overrides, 0);

        // Re-add after clear
        let proxy3 = ProxyConfig::new(ProxyType::Http, "fresh.example.com".to_string(), 80);
        manager
            .set_task_proxy("task-fresh".to_string(), proxy3, None)
            .await
            .unwrap();
        assert_eq!(manager.list_overrides().len(), 1);
    }

    // ===== List overrides content verification =====

    #[tokio::test]
    async fn test_list_overrides_contains_correct_data() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-lc".to_string(), proxy, Some("check me".to_string()))
            .await
            .unwrap();

        let overrides = manager.list_overrides();
        assert_eq!(overrides.len(), 1);
        assert_eq!(overrides[0].task_id, "task-lc");
        assert_eq!(overrides[0].notes, Some("check me".to_string()));
        assert_eq!(overrides[0].proxy.proxy_type, ProxyType::Socks5);
    }

    // ===== Debug trait tests =====

    #[tokio::test]
    async fn test_debug_impl_task_proxy_config() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::new("task-dbg".to_string(), proxy);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("task-dbg"));
    }

    #[tokio::test]
    async fn test_debug_impl_manager() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let manager = TaskProxyManager::new(config_path);
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("TaskProxyManager"));
    }

    #[tokio::test]
    async fn test_debug_impl_error() {
        let err = TaskProxyError::TaskNotFound("xyz".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("TaskNotFound"));
    }

    // ===== Clone trait tests =====

    #[tokio::test]
    async fn test_clone_task_proxy_config() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::with_notes(
            "task-clone".to_string(),
            proxy,
            Some("clone me".to_string()),
        );
        let cloned = config.clone();
        assert_eq!(cloned.task_id, "task-clone");
        assert_eq!(cloned.notes, Some("clone me".to_string()));
        assert!(cloned.enabled);
    }

    #[tokio::test]
    async fn test_clone_manager() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let mut manager = TaskProxyManager::new(config_path);

        let proxy = create_test_proxy();
        manager
            .set_task_proxy("task-cm".to_string(), proxy, None)
            .await
            .unwrap();

        let cloned = manager.clone();
        assert_eq!(cloned.list_overrides().len(), 1);
        assert!(cloned.get_task_proxy("task-cm").is_some());
    }

    // ===== Persistence after disable/re-enable =====

    #[tokio::test]
    async fn test_persistence_after_toggle_enabled() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        {
            let mut manager = TaskProxyManager::new(config_path.clone());
            let proxy = create_test_proxy();
            manager
                .set_task_proxy("task-tog".to_string(), proxy, None)
                .await
                .unwrap();
            manager.set_enabled("task-tog", false).await.unwrap();
            manager.set_enabled("task-tog", true).await.unwrap();
        }

        let loaded = TaskProxyManager::load(config_path).await.unwrap();
        assert!(loaded.get_task_proxy_raw("task-tog").unwrap().enabled);
        assert!(loaded.get_task_proxy("task-tog").is_some());
    }

    // ===== Summary format verification =====

    #[tokio::test]
    async fn test_summary_format_large_numbers() {
        let summary = TaskProxySummary {
            total_overrides: 1000,
            enabled_overrides: 999,
            disabled_overrides: 1,
        };
        let formatted = summary.format_summary();
        assert!(formatted.contains("1000 total"));
        assert!(formatted.contains("999 enabled"));
        assert!(formatted.contains("1 disabled"));
    }

    // ===== Phase 256: Comprehensive Test Coverage =====

    // --- TaskProxyConfig serde ---

    #[tokio::test]
    async fn test_config_serde_roundtrip_full() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig {
            task_id: "full-serde".to_string(),
            proxy,
            enabled: false,
            notes: Some("notes here".to_string()),
        };
        let json = serde_json::to_string(&config).unwrap();
        let de: TaskProxyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.task_id, "full-serde");
        assert!(!de.enabled);
        assert_eq!(de.notes, Some("notes here".to_string()));
    }

    #[tokio::test]
    async fn test_config_serde_extra_fields_ignored() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::new("extra-fields".to_string(), proxy);
        let mut json = serde_json::to_value(&config).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("unknown_field".to_string(), serde_json::json!(42));
        let de: TaskProxyConfig = serde_json::from_value(json).unwrap();
        assert_eq!(de.task_id, "extra-fields");
    }

    #[tokio::test]
    async fn test_config_serde_pretty() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::new("pretty".to_string(), proxy);
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let de: TaskProxyConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(de.task_id, "pretty");
    }

    #[tokio::test]
    async fn test_config_serde_unicode() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::with_notes(
            "中文任务".to_string(),
            proxy,
            Some("日本語のノート".to_string()),
        );
        let json = serde_json::to_string(&config).unwrap();
        let de: TaskProxyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.task_id, "中文任务");
        assert_eq!(de.notes, Some("日本語のノート".to_string()));
    }

    #[tokio::test]
    async fn test_config_serde_emoji() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::with_notes(
            "🚀task".to_string(),
            proxy,
            Some("🔥fast proxy🔥".to_string()),
        );
        let json = serde_json::to_string(&config).unwrap();
        let de: TaskProxyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.task_id, "🚀task");
        assert_eq!(de.notes, Some("🔥fast proxy🔥".to_string()));
    }

    // --- TaskProxyConfig traits ---

    #[tokio::test]
    async fn test_config_clone_independence() {
        let proxy = create_test_proxy();
        let config = TaskProxyConfig::with_notes(
            "orig".to_string(),
            proxy,
            Some("original notes".to_string()),
        );
        let mut cloned = config.clone();
        cloned.task_id = "modified".to_string();
        cloned.notes = Some("changed".to_string());
        assert_eq!(config.task_id, "orig");
        assert_eq!(config.notes, Some("original notes".to_string()));
    }

    // --- TaskProxySummary serde ---

    #[tokio::test]
    async fn test_summary_serde_roundtrip() {
        let summary = TaskProxySummary {
            total_overrides: 42,
            enabled_overrides: 30,
            disabled_overrides: 12,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let de: TaskProxySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(de.total_overrides, 42);
        assert_eq!(de.enabled_overrides, 30);
        assert_eq!(de.disabled_overrides, 12);
    }

    #[tokio::test]
    async fn test_summary_serde_extra_fields_ignored() {
        let summary = TaskProxySummary {
            total_overrides: 1,
            enabled_overrides: 1,
            disabled_overrides: 0,
        };
        let mut json = serde_json::to_value(&summary).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("extra".to_string(), serde_json::json!("ignored"));
        let de: TaskProxySummary = serde_json::from_value(json).unwrap();
        assert_eq!(de.total_overrides, 1);
    }

    #[tokio::test]
    async fn test_summary_clone_independence() {
        let summary = TaskProxySummary {
            total_overrides: 5,
            enabled_overrides: 3,
            disabled_overrides: 2,
        };
        let mut cloned = summary.clone();
        cloned.total_overrides = 999;
        assert_eq!(summary.total_overrides, 5);
    }

    // --- TaskProxyError traits ---

    #[tokio::test]
    async fn test_error_debug_all_variants() {
        let e1 = TaskProxyError::TaskNotFound("nf".to_string());
        let e2 = TaskProxyError::Io("io".to_string());
        let e3 = TaskProxyError::Serialization("ser".to_string());
        let e4 = TaskProxyError::Deserialization("de".to_string());
        let debugs = format!("{:?}\n{:?}\n{:?}\n{:?}", e1, e2, e3, e4);
        assert!(debugs.contains("TaskNotFound"));
        assert!(debugs.contains("Io"));
        assert!(debugs.contains("Serialization"));
        assert!(debugs.contains("Deserialization"));
    }

    #[tokio::test]
    async fn test_error_unicode_message() {
        let err = TaskProxyError::TaskNotFound("任务不存在".to_string());
        assert_eq!(err.to_string(), "Task not found: 任务不存在");
    }

    #[tokio::test]
    async fn test_error_empty_message() {
        let err = TaskProxyError::Io("".to_string());
        assert_eq!(err.to_string(), "I/O error: ");
    }

    #[tokio::test]
    async fn test_error_special_chars() {
        let err = TaskProxyError::Serialization("line1\nline2\ttab\"quote\\backslash".to_string());
        let display = err.to_string();
        assert!(display.contains("line1"));
        assert!(display.contains("quote"));
    }

    // --- TaskProxyManager: new ---

    #[tokio::test]
    async fn test_manager_new_empty() {
        let dir = tempdir().unwrap();
        let manager = TaskProxyManager::new(dir.path().join("tp.json"));
        assert_eq!(manager.list_overrides().len(), 0);
        assert!(manager.get_task_proxy("any").is_none());
        assert!(manager.get_task_proxy_raw("any").is_none());
        assert_eq!(manager.get_summary().total_overrides, 0);
    }

    // --- set_task_proxy ---

    #[tokio::test]
    async fn test_set_task_proxy_unicode_id() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("中文任务ID".to_string(), proxy, None)
            .await
            .unwrap();
        assert!(manager.get_task_proxy("中文任务ID").is_some());
    }

    #[tokio::test]
    async fn test_set_task_proxy_emoji_id() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("🚀🔥".to_string(), proxy, None)
            .await
            .unwrap();
        assert!(manager.get_task_proxy("🚀🔥").is_some());
    }

    #[tokio::test]
    async fn test_set_task_proxy_long_id() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        let long_id = "a".repeat(1000);
        manager
            .set_task_proxy(long_id.clone(), proxy, None)
            .await
            .unwrap();
        assert!(manager.get_task_proxy(&long_id).is_some());
    }

    #[tokio::test]
    async fn test_set_task_proxy_many_entries() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        for i in 0..100 {
            manager
                .set_task_proxy(format!("task-{}", i), proxy.clone(), None)
                .await
                .unwrap();
        }
        assert_eq!(manager.list_overrides().len(), 100);
        assert_eq!(manager.get_summary().total_overrides, 100);
    }

    // --- get_task_proxy vs get_task_proxy_raw ---

    #[tokio::test]
    async fn test_get_proxy_enabled_vs_disabled() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();

        manager
            .set_task_proxy("en".to_string(), proxy.clone(), None)
            .await
            .unwrap();
        manager
            .set_task_proxy("dis".to_string(), proxy, None)
            .await
            .unwrap();
        manager.set_enabled("dis", false).await.unwrap();

        // get_task_proxy only returns enabled
        assert!(manager.get_task_proxy("en").is_some());
        assert!(manager.get_task_proxy("dis").is_none());

        // get_task_proxy_raw returns both
        assert!(manager.get_task_proxy_raw("en").is_some());
        assert!(manager.get_task_proxy_raw("dis").is_some());
    }

    #[tokio::test]
    async fn test_get_proxy_nonexistent() {
        let dir = tempdir().unwrap();
        let manager = TaskProxyManager::new(dir.path().join("tp.json"));
        assert!(manager.get_task_proxy("nope").is_none());
        assert!(manager.get_task_proxy_raw("nope").is_none());
    }

    // --- set_enabled ---

    #[tokio::test]
    async fn test_set_enabled_toggle() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("tog".to_string(), proxy, None)
            .await
            .unwrap();

        // Toggle multiple times
        for _ in 0..5 {
            manager.set_enabled("tog", false).await.unwrap();
            assert!(!manager.get_task_proxy_raw("tog").unwrap().enabled);
            manager.set_enabled("tog", true).await.unwrap();
            assert!(manager.get_task_proxy_raw("tog").unwrap().enabled);
        }
    }

    // --- set_notes ---

    #[tokio::test]
    async fn test_set_notes_various() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("notes-test".to_string(), proxy, None)
            .await
            .unwrap();

        // Set to Some
        manager
            .set_notes("notes-test", Some("first".to_string()))
            .await
            .unwrap();
        assert_eq!(
            manager.get_task_proxy_raw("notes-test").unwrap().notes,
            Some("first".to_string())
        );

        // Overwrite
        manager
            .set_notes("notes-test", Some("second".to_string()))
            .await
            .unwrap();
        assert_eq!(
            manager.get_task_proxy_raw("notes-test").unwrap().notes,
            Some("second".to_string())
        );

        // Clear
        manager.set_notes("notes-test", None).await.unwrap();
        assert!(
            manager
                .get_task_proxy_raw("notes-test")
                .unwrap()
                .notes
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_set_notes_unicode() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("uni-notes".to_string(), proxy, None)
            .await
            .unwrap();
        manager
            .set_notes("uni-notes", Some("🎉庆祝".to_string()))
            .await
            .unwrap();
        assert_eq!(
            manager.get_task_proxy_raw("uni-notes").unwrap().notes,
            Some("🎉庆祝".to_string())
        );
    }

    // --- remove_task_proxy ---

    #[tokio::test]
    async fn test_remove_idempotent() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("rem-idem".to_string(), proxy, None)
            .await
            .unwrap();
        manager.remove_task_proxy("rem-idem").await.unwrap();
        // Second remove should also succeed
        manager.remove_task_proxy("rem-idem").await.unwrap();
        assert!(manager.get_task_proxy_raw("rem-idem").is_none());
    }

    // --- clear_all ---

    #[tokio::test]
    async fn test_clear_all_empty_manager() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        manager.clear_all().await.unwrap();
        assert_eq!(manager.list_overrides().len(), 0);
    }

    #[tokio::test]
    async fn test_clear_all_then_repopulate() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        for i in 0..10 {
            manager
                .set_task_proxy(format!("t{}", i), proxy.clone(), None)
                .await
                .unwrap();
        }
        manager.clear_all().await.unwrap();
        assert_eq!(manager.list_overrides().len(), 0);

        // Repopulate
        for i in 10..20 {
            manager
                .set_task_proxy(format!("t{}", i), proxy.clone(), None)
                .await
                .unwrap();
        }
        assert_eq!(manager.list_overrides().len(), 10);
    }

    // --- Persistence ---

    #[tokio::test]
    async fn test_persistence_no_tmp_leftover() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let tmp_path = dir.path().join("task_proxy.json.tmp");

        let mut manager = TaskProxyManager::new(config_path.clone());
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("no-tmp".to_string(), proxy, None)
            .await
            .unwrap();

        assert!(config_path.exists());
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_persistence_overwrite() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        let proxy1 = create_test_proxy();
        let proxy2 = ProxyConfig::new(ProxyType::Http, "overwrite.com".to_string(), 9999);

        {
            let mut m = TaskProxyManager::new(config_path.clone());
            m.set_task_proxy("ow-task".to_string(), proxy1, None)
                .await
                .unwrap();
        }
        {
            let mut m = TaskProxyManager::new(config_path.clone());
            m.set_task_proxy("ow-task".to_string(), proxy2, None)
                .await
                .unwrap();
        }

        let loaded = TaskProxyManager::load(config_path).await.unwrap();
        assert_eq!(
            loaded.get_task_proxy_raw("ow-task").unwrap().proxy.host,
            "overwrite.com"
        );
    }

    #[tokio::test]
    async fn test_persistence_unicode_path() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("代理_设置.json");

        let mut manager = TaskProxyManager::new(config_path.clone());
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("unicode-path".to_string(), proxy, None)
            .await
            .unwrap();

        assert!(config_path.exists());
        let loaded = TaskProxyManager::load(config_path).await.unwrap();
        assert!(loaded.get_task_proxy_raw("unicode-path").is_some());
    }

    #[tokio::test]
    async fn test_persistence_pretty_json() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        let mut manager = TaskProxyManager::new(config_path.clone());
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("pretty-p".to_string(), proxy, Some("notes".to_string()))
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&config_path).await.unwrap();
        // Pretty JSON should have newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("    "));
    }

    #[tokio::test]
    async fn test_persistence_roundtrip_all_proxy_types() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        let proxy_types = vec![
            ProxyConfig::new(ProxyType::Http, "http.com".to_string(), 80),
            ProxyConfig::new(ProxyType::Http, "https.com".to_string(), 443),
            ProxyConfig::new(ProxyType::Socks5, "socks5.com".to_string(), 1080),
        ];

        {
            let mut m = TaskProxyManager::new(config_path.clone());
            for (i, p) in proxy_types.into_iter().enumerate() {
                m.set_task_proxy(format!("type-{}", i), p, None)
                    .await
                    .unwrap();
            }
        }

        let loaded = TaskProxyManager::load(config_path).await.unwrap();
        assert_eq!(loaded.list_overrides().len(), 3);
    }

    // --- format_summary ---

    #[tokio::test]
    async fn test_format_summary_exact_format() {
        let summary = TaskProxySummary {
            total_overrides: 7,
            enabled_overrides: 5,
            disabled_overrides: 2,
        };
        let formatted = summary.format_summary();
        assert_eq!(
            formatted,
            "Task Proxy Overrides: 7 total (5 enabled, 2 disabled)"
        );
    }

    #[tokio::test]
    async fn test_format_summary_unicode_content() {
        // Summary itself is ASCII format, but verify it works with zero values
        let summary = TaskProxySummary {
            total_overrides: 0,
            enabled_overrides: 0,
            disabled_overrides: 0,
        };
        let formatted = summary.format_summary();
        assert!(formatted.contains("0 total"));
    }

    // --- Complex workflow tests ---

    #[tokio::test]
    async fn test_complete_lifecycle() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        // Create manager and add proxies
        let mut manager = TaskProxyManager::new(config_path.clone());
        let proxy1 = create_test_proxy();
        let proxy2 = ProxyConfig::new(ProxyType::Http, "backup.com".to_string(), 8080);

        manager
            .set_task_proxy(
                "lifecycle-1".to_string(),
                proxy1,
                Some("primary".to_string()),
            )
            .await
            .unwrap();
        manager
            .set_task_proxy(
                "lifecycle-2".to_string(),
                proxy2,
                Some("backup".to_string()),
            )
            .await
            .unwrap();

        // Verify
        assert_eq!(manager.list_overrides().len(), 2);
        assert_eq!(manager.get_summary().enabled_overrides, 2);

        // Disable one
        manager.set_enabled("lifecycle-1", false).await.unwrap();
        assert_eq!(manager.get_summary().disabled_overrides, 1);
        assert!(manager.get_task_proxy("lifecycle-1").is_none());
        assert!(manager.get_task_proxy("lifecycle-2").is_some());

        // Save and reload
        drop(manager);
        let loaded = TaskProxyManager::load(config_path).await.unwrap();
        assert_eq!(loaded.list_overrides().len(), 2);
        assert!(!loaded.get_task_proxy_raw("lifecycle-1").unwrap().enabled);
        assert!(loaded.get_task_proxy_raw("lifecycle-2").unwrap().enabled);

        // Update notes
        let mut loaded = loaded;
        loaded
            .set_notes("lifecycle-1", Some("re-enabled".to_string()))
            .await
            .unwrap();
        loaded.set_enabled("lifecycle-1", true).await.unwrap();
        assert!(loaded.get_task_proxy("lifecycle-1").is_some());

        // Remove one
        loaded.remove_task_proxy("lifecycle-2").await.unwrap();
        assert_eq!(loaded.list_overrides().len(), 1);

        // Clear all
        loaded.clear_all().await.unwrap();
        assert_eq!(loaded.list_overrides().len(), 0);
    }

    #[tokio::test]
    async fn test_multi_task_independent_proxies() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));

        let proxies = vec![
            ProxyConfig::new(ProxyType::Http, "proxy1.com".to_string(), 80),
            ProxyConfig::new(ProxyType::Http, "proxy2.com".to_string(), 443),
            ProxyConfig::new(ProxyType::Socks5, "proxy3.com".to_string(), 1080),
        ];

        for (i, p) in proxies.into_iter().enumerate() {
            manager
                .set_task_proxy(format!("indep-{}", i), p, Some(format!("proxy {}", i)))
                .await
                .unwrap();
        }

        // Verify each has correct proxy
        for i in 0..3 {
            let config = manager.get_task_proxy_raw(&format!("indep-{}", i)).unwrap();
            assert_eq!(config.notes, Some(format!("proxy {}", i)));
        }

        // Disable middle one
        manager.set_enabled("indep-1", false).await.unwrap();
        assert!(manager.get_task_proxy("indep-0").is_some());
        assert!(manager.get_task_proxy("indep-1").is_none());
        assert!(manager.get_task_proxy("indep-2").is_some());
    }

    #[tokio::test]
    async fn test_save_load_save_load_cycle() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");
        let proxy = create_test_proxy();

        for cycle in 0..3 {
            let mut m = TaskProxyManager::new(config_path.clone());
            m.set_task_proxy(
                format!("cycle-{}", cycle),
                proxy.clone(),
                Some(format!("cycle {}", cycle)),
            )
            .await
            .unwrap();
            drop(m);

            let loaded = TaskProxyManager::load(config_path.clone()).await.unwrap();
            assert!(
                loaded
                    .get_task_proxy_raw(&format!("cycle-{}", cycle))
                    .is_some()
            );
        }
    }

    // --- Boundary: proxy with credentials ---

    #[tokio::test]
    async fn test_proxy_with_credentials() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));

        let proxy = ProxyConfig::with_auth(
            ProxyType::Http,
            "auth.proxy.com".to_string(),
            8080,
            "user".to_string(),
            "p@ssw0rd!".to_string(),
        );

        manager
            .set_task_proxy("auth-task".to_string(), proxy, None)
            .await
            .unwrap();

        // Persist and reload
        let loaded = TaskProxyManager::load(dir.path().join("tp.json"))
            .await
            .unwrap();
        let config = loaded.get_task_proxy_raw("auth-task").unwrap();
        assert_eq!(config.proxy.auth.as_ref().unwrap().username, "user");
        assert_eq!(config.proxy.auth.as_ref().unwrap().password, "p@ssw0rd!");
    }

    // --- Summary serde with extra fields ---

    #[tokio::test]
    async fn test_summary_serde_pretty() {
        let summary = TaskProxySummary {
            total_overrides: 3,
            enabled_overrides: 2,
            disabled_overrides: 1,
        };
        let pretty = serde_json::to_string_pretty(&summary).unwrap();
        let de: TaskProxySummary = serde_json::from_str(&pretty).unwrap();
        assert_eq!(de.total_overrides, 3);
    }

    // --- Error std::error::Error trait ---

    #[tokio::test]
    async fn test_error_is_std_error() {
        let err: Box<dyn std::error::Error> =
            Box::new(TaskProxyError::TaskNotFound("x".to_string()));
        assert!(err.to_string().contains("x"));
    }

    // --- Config with_notes None vs Some(None) ---

    #[tokio::test]
    async fn test_with_notes_none_vs_new() {
        let proxy = create_test_proxy();
        let c1 = TaskProxyConfig::new("a".to_string(), proxy.clone());
        let c2 = TaskProxyConfig::with_notes("a".to_string(), proxy, None);
        assert_eq!(c1.enabled, c2.enabled);
        assert_eq!(c1.notes, c2.notes);
        assert_eq!(c1.task_id, c2.task_id);
    }

    // --- Manager Debug with content ---

    #[tokio::test]
    async fn test_debug_manager_with_entries() {
        let dir = tempdir().unwrap();
        let mut manager = TaskProxyManager::new(dir.path().join("tp.json"));
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("dbg-entry".to_string(), proxy, None)
            .await
            .unwrap();
        let debug = format!("{:?}", manager);
        assert!(debug.contains("dbg-entry"));
    }

    // --- Summary serialization in file ---

    #[tokio::test]
    async fn test_persistence_json_structure() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("task_proxy.json");

        let mut manager = TaskProxyManager::new(config_path.clone());
        let proxy = create_test_proxy();
        manager
            .set_task_proxy("struct".to_string(), proxy, None)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&config_path).await.unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].get("task_id").is_some());
        assert!(parsed[0].get("proxy").is_some());
        assert!(parsed[0].get("enabled").is_some());
    }
}
