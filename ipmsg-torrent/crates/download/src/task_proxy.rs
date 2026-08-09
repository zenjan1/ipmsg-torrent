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
    use crate::proxy::{ProxyConfig, ProxyType};
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
}
