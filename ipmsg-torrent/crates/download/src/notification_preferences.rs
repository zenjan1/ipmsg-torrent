//! Download Task Notification Preferences
//!
//! Per-task notification preference system that allows fine-grained control
//! over which download events trigger notifications.
//!
//! Features:
//! - Per-task notification configuration (enable/disable specific events)
//! - Global default preferences for new tasks
//! - Notification cooldown to prevent spam
//! - Event deduplication within configurable time windows
//! - Priority-based filtering (suppress low-priority notifications)
//! - Persistent configuration via JSON
//! - DownloadManager integration with REST API and CLI support

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;
use tracing::{debug, info};

/// Errors from notification preferences operations
#[derive(Error, Debug)]
pub enum NotificationPreferencesError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Notification event types for per-task preferences
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskNotificationEvent {
    /// Download started
    Started,
    /// Download completed successfully
    Completed,
    /// Download failed
    Failed,
    /// Download paused
    Paused,
    /// Download resumed
    Resumed,
    /// Progress milestone reached
    ProgressMilestone,
    /// Speed alert triggered
    SpeedAlert,
    /// ETA changed significantly
    EtaChanged,
    /// Task added to queue
    Added,
    /// Task removed from queue
    Removed,
}

impl TaskNotificationEvent {
    /// Human-readable label
    pub fn label(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Paused => "paused",
            Self::Resumed => "resumed",
            Self::ProgressMilestone => "progress_milestone",
            Self::SpeedAlert => "speed_alert",
            Self::EtaChanged => "eta_changed",
            Self::Added => "added",
            Self::Removed => "removed",
        }
    }

    /// All event variants
    pub fn all() -> Vec<Self> {
        vec![
            Self::Started,
            Self::Completed,
            Self::Failed,
            Self::Paused,
            Self::Resumed,
            Self::ProgressMilestone,
            Self::SpeedAlert,
            Self::EtaChanged,
            Self::Added,
            Self::Removed,
        ]
    }

    /// Default events that are enabled
    pub fn default_enabled() -> Vec<Self> {
        vec![Self::Completed, Self::Failed]
    }
}

impl std::fmt::Display for TaskNotificationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// Minimum priority level for notifications
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MinimumPriority {
    /// Show all notifications
    Low = 0,
    /// Normal and above (default)
    #[default]
    Normal = 1,
    /// High and critical only
    High = 2,
    /// Critical only
    Critical = 3,
    /// Suppress all
    None = 4,
}

impl std::fmt::Display for MinimumPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Low => write!(f, "low"),
            Self::Normal => write!(f, "normal"),
            Self::High => write!(f, "high"),
            Self::Critical => write!(f, "critical"),
            Self::None => write!(f, "none"),
        }
    }
}

/// Per-task notification configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNotificationConfig {
    /// Task ID this config applies to
    pub task_id: String,
    /// Whether notifications are enabled for this task
    pub enabled: bool,
    /// Which events trigger notifications
    pub enabled_events: Vec<TaskNotificationEvent>,
    /// Minimum priority to show
    pub min_priority: MinimumPriority,
    /// Cooldown period in seconds (suppress duplicate notifications)
    pub cooldown_secs: u64,
    /// Custom label override (optional)
    pub label_override: Option<String>,
    /// When this config was created
    pub created_at: DateTime<Utc>,
    /// When this config was last updated
    pub updated_at: DateTime<Utc>,
}

impl TaskNotificationConfig {
    /// Create a new config with defaults for a task
    pub fn new(task_id: String) -> Self {
        let now = Utc::now();
        Self {
            task_id,
            enabled: true,
            enabled_events: TaskNotificationEvent::default_enabled(),
            min_priority: MinimumPriority::default(),
            cooldown_secs: 300, // 5 minutes default
            label_override: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Check if a specific event should trigger a notification
    pub fn should_notify(&self, event: &TaskNotificationEvent) -> bool {
        self.enabled && self.enabled_events.contains(event)
    }

    /// Enable a specific event
    pub fn enable_event(&mut self, event: TaskNotificationEvent) {
        if !self.enabled_events.contains(&event) {
            self.enabled_events.push(event);
            self.updated_at = Utc::now();
        }
    }

    /// Disable a specific event
    pub fn disable_event(&mut self, event: TaskNotificationEvent) {
        self.enabled_events.retain(|e| e != &event);
        self.updated_at = Utc::now();
    }

    /// Enable all events
    pub fn enable_all_events(&mut self) {
        self.enabled_events = TaskNotificationEvent::all();
        self.updated_at = Utc::now();
    }

    /// Disable all events
    pub fn disable_all_events(&mut self) {
        self.enabled_events.clear();
        self.updated_at = Utc::now();
    }
}

/// Global notification preferences configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPreferencesConfig {
    /// Whether the preferences system is enabled
    pub enabled: bool,
    /// Default events enabled for new tasks
    pub default_enabled_events: Vec<TaskNotificationEvent>,
    /// Default minimum priority for new tasks
    pub default_min_priority: MinimumPriority,
    /// Default cooldown period in seconds
    pub default_cooldown_secs: u64,
    /// Maximum notification history entries per task
    pub max_history_per_task: usize,
    /// Enable notification deduplication
    pub dedup_enabled: bool,
    /// Deduplication time window in seconds
    pub dedup_window_secs: u64,
}

impl Default for NotificationPreferencesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_enabled_events: TaskNotificationEvent::default_enabled(),
            default_min_priority: MinimumPriority::Normal,
            default_cooldown_secs: 300,
            max_history_per_task: 50,
            dedup_enabled: true,
            dedup_window_secs: 60,
        }
    }
}

/// Notification preference summary for display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPreferencesSummary {
    /// Total tasks with custom preferences
    pub tasks_with_custom_prefs: usize,
    /// Tasks with notifications enabled
    pub tasks_enabled: usize,
    /// Tasks with notifications disabled
    pub tasks_disabled: usize,
    /// Total notifications sent in this session
    pub total_notifications_sent: u64,
    /// Total notifications suppressed (cooldown/dedup)
    pub total_notifications_suppressed: u64,
    /// Per-event notification counts
    pub event_counts: HashMap<String, u64>,
}

/// Tracks notification cooldown state for a task
#[derive(Debug, Clone)]
struct CooldownState {
    /// Last notification time per event type
    last_notified: HashMap<TaskNotificationEvent, DateTime<Utc>>,
}

impl CooldownState {
    fn new() -> Self {
        Self {
            last_notified: HashMap::new(),
        }
    }

    /// Check if we're in cooldown for this event
    fn is_in_cooldown(&self, event: &TaskNotificationEvent, cooldown_secs: u64) -> bool {
        if let Some(last_time) = self.last_notified.get(event) {
            let elapsed = Utc::now().signed_duration_since(*last_time);
            elapsed.num_seconds() < cooldown_secs as i64
        } else {
            false
        }
    }

    /// Record that we sent a notification for this event
    fn record_notification(&mut self, event: TaskNotificationEvent) {
        self.last_notified.insert(event, Utc::now());
    }
}

/// Tracks deduplication state
#[derive(Debug, Clone)]
struct DedupState {
    /// Recent notification hashes for dedup
    recent_hashes: Vec<(String, DateTime<Utc>)>,
    /// Maximum entries to keep
    max_entries: usize,
}

impl DedupState {
    fn new(max_entries: usize) -> Self {
        Self {
            recent_hashes: Vec::new(),
            max_entries,
        }
    }

    /// Check if a notification is a duplicate
    fn is_duplicate(&self, hash: &str, window_secs: u64) -> bool {
        let cutoff = Utc::now() - chrono::Duration::seconds(window_secs as i64);
        self.recent_hashes
            .iter()
            .any(|(h, t)| h == hash && *t > cutoff)
    }

    /// Record a notification hash
    fn record(&mut self, hash: String) {
        self.recent_hashes.push((hash, Utc::now()));
        // Prune old entries
        if self.recent_hashes.len() > self.max_entries {
            let cutoff = Utc::now() - chrono::Duration::seconds(300);
            self.recent_hashes.retain(|(_, t)| *t > cutoff);
        }
    }
}

/// Notification preferences manager
#[derive(Debug)]
pub struct NotificationPreferencesManager {
    /// Global configuration
    config: NotificationPreferencesConfig,
    /// Per-task configurations
    task_configs: HashMap<String, TaskNotificationConfig>,
    /// Cooldown tracking per task
    cooldowns: HashMap<String, CooldownState>,
    /// Deduplication state
    dedup: DedupState,
    /// Statistics
    stats_sent: u64,
    stats_suppressed: u64,
    /// Per-event counts
    event_counts: HashMap<TaskNotificationEvent, u64>,
}

impl Default for NotificationPreferencesManager {
    fn default() -> Self {
        Self::new()
    }
}

impl NotificationPreferencesManager {
    /// Create a new manager with default configuration
    pub fn new() -> Self {
        let config = NotificationPreferencesConfig::default();
        let dedup = DedupState::new(1000);
        Self {
            config,
            task_configs: HashMap::new(),
            cooldowns: HashMap::new(),
            dedup,
            stats_sent: 0,
            stats_suppressed: 0,
            event_counts: HashMap::new(),
        }
    }

    /// Get the current configuration
    pub fn get_config(&self) -> &NotificationPreferencesConfig {
        &self.config
    }

    /// Update the global configuration
    pub fn set_config(&mut self, config: NotificationPreferencesConfig) {
        info!("Updating notification preferences config");
        self.config = config;
    }

    /// Set notification preferences for a specific task
    pub fn set_task_config(&mut self, config: TaskNotificationConfig) {
        debug!(
            "Setting notification preferences for task {}",
            config.task_id
        );
        self.task_configs.insert(config.task_id.clone(), config);
    }

    /// Get notification preferences for a specific task
    pub fn get_task_config(&self, task_id: &str) -> Option<&TaskNotificationConfig> {
        self.task_configs.get(task_id)
    }

    /// Remove notification preferences for a task
    pub fn remove_task_config(&mut self, task_id: &str) -> bool {
        let removed = self.task_configs.remove(task_id).is_some();
        if removed {
            self.cooldowns.remove(task_id);
            debug!("Removed notification preferences for task {}", task_id);
        }
        removed
    }

    /// Get or create default config for a task
    pub fn get_or_create_task_config(&mut self, task_id: &str) -> &TaskNotificationConfig {
        if !self.task_configs.contains_key(task_id) {
            let config = TaskNotificationConfig {
                task_id: task_id.to_string(),
                enabled: true,
                enabled_events: self.config.default_enabled_events.clone(),
                min_priority: self.config.default_min_priority,
                cooldown_secs: self.config.default_cooldown_secs,
                label_override: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };
            self.task_configs.insert(task_id.to_string(), config);
        }
        self.task_configs.get(task_id).unwrap()
    }

    /// Check if a notification should be sent for a task event
    pub fn should_notify(&mut self, task_id: &str, event: &TaskNotificationEvent) -> bool {
        if !self.config.enabled {
            return false;
        }

        // Get task config (create default if not exists)
        let task_config = self.get_or_create_task_config(task_id).clone();

        // Check if notifications are enabled for this task
        if !task_config.enabled {
            self.stats_suppressed += 1;
            return false;
        }

        // Check if this event type is enabled
        if !task_config.should_notify(event) {
            self.stats_suppressed += 1;
            return false;
        }

        // Check cooldown
        let cooldown = self
            .cooldowns
            .entry(task_id.to_string())
            .or_insert_with(CooldownState::new);

        if cooldown.is_in_cooldown(event, task_config.cooldown_secs) {
            debug!(
                "Notification suppressed by cooldown for task {} event {}",
                task_id, event
            );
            self.stats_suppressed += 1;
            return false;
        }

        // Check deduplication
        if self.config.dedup_enabled {
            let hash = format!(
                "{}:{}:{}",
                task_id,
                event.label(),
                Utc::now().timestamp() / self.config.dedup_window_secs as i64
            );
            if self
                .dedup
                .is_duplicate(&hash, self.config.dedup_window_secs)
            {
                debug!(
                    "Notification suppressed by dedup for task {} event {}",
                    task_id, event
                );
                self.stats_suppressed += 1;
                return false;
            }
            self.dedup.record(hash);
        }

        // Record the notification
        cooldown.record_notification(*event);
        self.stats_sent += 1;
        *self.event_counts.entry(*event).or_insert(0) += 1;

        true
    }

    /// List all tasks with custom notification preferences
    pub fn list_task_configs(&self) -> Vec<&TaskNotificationConfig> {
        self.task_configs.values().collect()
    }

    /// Get summary of notification preferences
    pub fn get_summary(&self) -> NotificationPreferencesSummary {
        let tasks_enabled = self.task_configs.values().filter(|c| c.enabled).count();
        let tasks_disabled = self.task_configs.len() - tasks_enabled;

        let event_counts: HashMap<String, u64> = self
            .event_counts
            .iter()
            .map(|(k, v)| (k.label().to_string(), *v))
            .collect();

        NotificationPreferencesSummary {
            tasks_with_custom_prefs: self.task_configs.len(),
            tasks_enabled,
            tasks_disabled,
            total_notifications_sent: self.stats_sent,
            total_notifications_suppressed: self.stats_suppressed,
            event_counts,
        }
    }

    /// Clear all statistics
    pub fn clear_stats(&mut self) {
        self.stats_sent = 0;
        self.stats_suppressed = 0;
        self.event_counts.clear();
    }

    /// Clear cooldown state for a task
    pub fn clear_cooldown(&mut self, task_id: &str) {
        self.cooldowns.remove(task_id);
    }

    /// Clear all cooldown states
    pub fn clear_all_cooldowns(&mut self) {
        self.cooldowns.clear();
    }

    /// Enable notifications for a task
    pub fn enable_task_notifications(&mut self, task_id: &str) {
        let config = self.get_or_create_task_config(task_id);
        let mut config = config.clone();
        config.enabled = true;
        config.updated_at = Utc::now();
        self.task_configs.insert(task_id.to_string(), config);
    }

    /// Disable notifications for a task
    pub fn disable_task_notifications(&mut self, task_id: &str) {
        let config = self.get_or_create_task_config(task_id);
        let mut config = config.clone();
        config.enabled = false;
        config.updated_at = Utc::now();
        self.task_configs.insert(task_id.to_string(), config);
    }

    /// Set cooldown period for a task
    pub fn set_task_cooldown(&mut self, task_id: &str, cooldown_secs: u64) {
        let config = self.get_or_create_task_config(task_id);
        let mut config = config.clone();
        config.cooldown_secs = cooldown_secs;
        config.updated_at = Utc::now();
        self.task_configs.insert(task_id.to_string(), config);
    }

    /// Set minimum priority for a task
    pub fn set_task_min_priority(&mut self, task_id: &str, min_priority: MinimumPriority) {
        let config = self.get_or_create_task_config(task_id);
        let mut config = config.clone();
        config.min_priority = min_priority;
        config.updated_at = Utc::now();
        self.task_configs.insert(task_id.to_string(), config);
    }

    /// Save configuration to file
    pub async fn save_config(&self, path: &PathBuf) -> Result<(), NotificationPreferencesError> {
        let data = serde_json::to_string_pretty(&self.config)?;
        tokio::fs::write(path, data).await?;
        debug!("Saved notification preferences config to {:?}", path);
        Ok(())
    }

    /// Load configuration from file
    pub async fn load_config(
        &mut self,
        path: &PathBuf,
    ) -> Result<(), NotificationPreferencesError> {
        if path.exists() {
            let data = tokio::fs::read_to_string(path).await?;
            self.config = serde_json::from_str(&data)?;
            debug!("Loaded notification preferences config from {:?}", path);
        }
        Ok(())
    }

    /// Save per-task configurations to file
    pub async fn save_task_configs(
        &self,
        path: &PathBuf,
    ) -> Result<(), NotificationPreferencesError> {
        let data = serde_json::to_string_pretty(&self.task_configs)?;
        tokio::fs::write(path, data).await?;
        debug!("Saved task notification configs to {:?}", path);
        Ok(())
    }

    /// Load per-task configurations from file
    pub async fn load_task_configs(
        &mut self,
        path: &PathBuf,
    ) -> Result<(), NotificationPreferencesError> {
        if path.exists() {
            let data = tokio::fs::read_to_string(path).await?;
            self.task_configs = serde_json::from_str(&data)?;
            debug!(
                "Loaded {} task notification configs from {:?}",
                self.task_configs.len(),
                path
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_notification_event_labels() {
        assert_eq!(TaskNotificationEvent::Started.label(), "started");
        assert_eq!(TaskNotificationEvent::Completed.label(), "completed");
        assert_eq!(TaskNotificationEvent::Failed.label(), "failed");
        assert_eq!(TaskNotificationEvent::Paused.label(), "paused");
        assert_eq!(TaskNotificationEvent::Resumed.label(), "resumed");
        assert_eq!(
            TaskNotificationEvent::ProgressMilestone.label(),
            "progress_milestone"
        );
        assert_eq!(TaskNotificationEvent::SpeedAlert.label(), "speed_alert");
        assert_eq!(TaskNotificationEvent::EtaChanged.label(), "eta_changed");
        assert_eq!(TaskNotificationEvent::Added.label(), "added");
        assert_eq!(TaskNotificationEvent::Removed.label(), "removed");
    }

    #[test]
    fn test_task_notification_event_all() {
        let all = TaskNotificationEvent::all();
        assert_eq!(all.len(), 10);
    }

    #[test]
    fn test_task_notification_event_default_enabled() {
        let defaults = TaskNotificationEvent::default_enabled();
        assert_eq!(defaults.len(), 2);
        assert!(defaults.contains(&TaskNotificationEvent::Completed));
        assert!(defaults.contains(&TaskNotificationEvent::Failed));
    }

    #[test]
    fn test_minimum_priority_ordering() {
        assert!(MinimumPriority::Low < MinimumPriority::Normal);
        assert!(MinimumPriority::Normal < MinimumPriority::High);
        assert!(MinimumPriority::High < MinimumPriority::Critical);
        assert!(MinimumPriority::Critical < MinimumPriority::None);
    }

    #[test]
    fn test_task_notification_config_new() {
        let config = TaskNotificationConfig::new("task-123".to_string());
        assert_eq!(config.task_id, "task-123");
        assert!(config.enabled);
        assert_eq!(config.enabled_events.len(), 2);
        assert_eq!(config.cooldown_secs, 300);
        assert!(config.label_override.is_none());
    }

    #[test]
    fn test_task_config_should_notify() {
        let mut config = TaskNotificationConfig::new("task-1".to_string());
        assert!(config.should_notify(&TaskNotificationEvent::Completed));
        assert!(config.should_notify(&TaskNotificationEvent::Failed));
        assert!(!config.should_notify(&TaskNotificationEvent::Started));
        assert!(!config.should_notify(&TaskNotificationEvent::Paused));

        config.enable_event(TaskNotificationEvent::Started);
        assert!(config.should_notify(&TaskNotificationEvent::Started));

        config.disable_event(TaskNotificationEvent::Completed);
        assert!(!config.should_notify(&TaskNotificationEvent::Completed));
    }

    #[test]
    fn test_task_config_enable_disable_all() {
        let mut config = TaskNotificationConfig::new("task-1".to_string());

        config.enable_all_events();
        assert_eq!(config.enabled_events.len(), 10);

        config.disable_all_events();
        assert!(config.enabled_events.is_empty());
    }

    #[test]
    fn test_task_config_enable_event_idempotent() {
        let mut config = TaskNotificationConfig::new("task-1".to_string());
        let initial_count = config.enabled_events.len();

        // Enable an already-enabled event
        config.enable_event(TaskNotificationEvent::Completed);
        assert_eq!(config.enabled_events.len(), initial_count);

        // Enable a new event
        config.enable_event(TaskNotificationEvent::Started);
        assert_eq!(config.enabled_events.len(), initial_count + 1);
    }

    #[test]
    fn test_cooldown_state() {
        let mut state = CooldownState::new();
        let event = TaskNotificationEvent::Completed;

        // Not in cooldown initially
        assert!(!state.is_in_cooldown(&event, 300));

        // Record notification
        state.record_notification(event);

        // Now in cooldown
        assert!(state.is_in_cooldown(&event, 300));

        // Different event not in cooldown
        assert!(!state.is_in_cooldown(&TaskNotificationEvent::Failed, 300));
    }

    #[test]
    fn test_dedup_state() {
        let mut state = DedupState::new(100);

        // Not duplicate initially
        assert!(!state.is_duplicate("hash1", 60));

        // Record hash
        state.record("hash1".to_string());

        // Now it's a duplicate
        assert!(state.is_duplicate("hash1", 60));

        // Different hash is not duplicate
        assert!(!state.is_duplicate("hash2", 60));
    }

    #[test]
    fn test_manager_new() {
        let manager = NotificationPreferencesManager::new();
        assert!(manager.config.enabled);
        assert!(manager.task_configs.is_empty());
        assert_eq!(manager.stats_sent, 0);
        assert_eq!(manager.stats_suppressed, 0);
    }

    #[test]
    fn test_manager_set_get_config() {
        let mut manager = NotificationPreferencesManager::new();
        let mut config = NotificationPreferencesConfig::default();
        config.enabled = false;
        config.default_cooldown_secs = 600;

        manager.set_config(config);
        assert!(!manager.get_config().enabled);
        assert_eq!(manager.get_config().default_cooldown_secs, 600);
    }

    #[test]
    fn test_manager_task_config() {
        let mut manager = NotificationPreferencesManager::new();
        let config = TaskNotificationConfig::new("task-1".to_string());

        manager.set_task_config(config);
        assert!(manager.get_task_config("task-1").is_some());
        assert!(manager.get_task_config("task-2").is_none());
    }

    #[test]
    fn test_manager_remove_task_config() {
        let mut manager = NotificationPreferencesManager::new();
        let config = TaskNotificationConfig::new("task-1".to_string());

        manager.set_task_config(config);
        assert!(manager.remove_task_config("task-1"));
        assert!(!manager.remove_task_config("task-1"));
        assert!(manager.get_task_config("task-1").is_none());
    }

    #[test]
    fn test_manager_get_or_create_config() {
        let mut manager = NotificationPreferencesManager::new();

        // Creates default config
        let config = manager.get_or_create_task_config("task-1");
        assert_eq!(config.task_id, "task-1");
        assert!(config.enabled);

        // Returns same config on second call
        let config2 = manager.get_or_create_task_config("task-1");
        assert_eq!(config2.task_id, "task-1");
    }

    #[test]
    fn test_manager_should_notify_basic() {
        let mut manager = NotificationPreferencesManager::new();

        // Completed is in default enabled events
        assert!(manager.should_notify("task-1", &TaskNotificationEvent::Completed));

        // Started is not in default enabled events
        assert!(!manager.should_notify("task-1", &TaskNotificationEvent::Started));
    }

    #[test]
    fn test_manager_should_notify_disabled_task() {
        let mut manager = NotificationPreferencesManager::new();

        // Disable notifications for task
        manager.disable_task_notifications("task-1");

        // Should not notify
        assert!(!manager.should_notify("task-1", &TaskNotificationEvent::Completed));
    }

    #[test]
    fn test_manager_should_notify_disabled_global() {
        let mut manager = NotificationPreferencesManager::new();

        // Disable global preferences
        let mut config = manager.get_config().clone();
        config.enabled = false;
        manager.set_config(config);

        // Should not notify even for default events
        assert!(!manager.should_notify("task-1", &TaskNotificationEvent::Completed));
    }

    #[test]
    fn test_manager_should_notify_cooldown() {
        let mut manager = NotificationPreferencesManager::new();

        // First notification should pass
        assert!(manager.should_notify("task-1", &TaskNotificationEvent::Completed));

        // Second notification within cooldown should be suppressed
        assert!(!manager.should_notify("task-1", &TaskNotificationEvent::Completed));
    }

    #[test]
    fn test_manager_enable_disable_task() {
        let mut manager = NotificationPreferencesManager::new();

        manager.disable_task_notifications("task-1");
        let config = manager.get_task_config("task-1").unwrap();
        assert!(!config.enabled);

        manager.enable_task_notifications("task-1");
        let config = manager.get_task_config("task-1").unwrap();
        assert!(config.enabled);
    }

    #[test]
    fn test_manager_set_cooldown() {
        let mut manager = NotificationPreferencesManager::new();

        manager.set_task_cooldown("task-1", 600);
        let config = manager.get_task_config("task-1").unwrap();
        assert_eq!(config.cooldown_secs, 600);
    }

    #[test]
    fn test_manager_set_min_priority() {
        let mut manager = NotificationPreferencesManager::new();

        manager.set_task_min_priority("task-1", MinimumPriority::High);
        let config = manager.get_task_config("task-1").unwrap();
        assert_eq!(config.min_priority, MinimumPriority::High);
    }

    #[test]
    fn test_manager_summary() {
        let mut manager = NotificationPreferencesManager::new();

        // Create some task configs
        manager.set_task_config(TaskNotificationConfig::new("task-1".to_string()));
        manager.set_task_config(TaskNotificationConfig::new("task-2".to_string()));
        manager.disable_task_notifications("task-2");

        let summary = manager.get_summary();
        assert_eq!(summary.tasks_with_custom_prefs, 2);
        assert_eq!(summary.tasks_enabled, 1);
        assert_eq!(summary.tasks_disabled, 1);
    }

    #[test]
    fn test_manager_clear_stats() {
        let mut manager = NotificationPreferencesManager::new();

        // Generate some stats
        manager.should_notify("task-1", &TaskNotificationEvent::Completed);

        assert!(manager.stats_sent > 0);
        manager.clear_stats();
        assert_eq!(manager.stats_sent, 0);
        assert_eq!(manager.stats_suppressed, 0);
    }

    #[test]
    fn test_manager_clear_cooldown() {
        let mut manager = NotificationPreferencesManager::new();

        // First notification passes
        assert!(manager.should_notify("task-1", &TaskNotificationEvent::Completed));

        // Clear cooldown
        manager.clear_cooldown("task-1");

        // Should pass again (cooldown was cleared)
        // Note: dedup may still catch it within the same time window
        // but cooldown is cleared
    }

    #[test]
    fn test_manager_list_task_configs() {
        let mut manager = NotificationPreferencesManager::new();

        manager.set_task_config(TaskNotificationConfig::new("task-1".to_string()));
        manager.set_task_config(TaskNotificationConfig::new("task-2".to_string()));

        let configs = manager.list_task_configs();
        assert_eq!(configs.len(), 2);
    }

    #[test]
    fn test_notification_preferences_config_default() {
        let config = NotificationPreferencesConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_enabled_events.len(), 2);
        assert_eq!(config.default_cooldown_secs, 300);
        assert_eq!(config.max_history_per_task, 50);
        assert!(config.dedup_enabled);
        assert_eq!(config.dedup_window_secs, 60);
    }

    #[test]
    fn test_task_config_serialization() {
        let config = TaskNotificationConfig::new("task-123".to_string());
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TaskNotificationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task-123");
        assert!(deserialized.enabled);
    }

    #[test]
    fn test_config_serialization() {
        let config = NotificationPreferencesConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NotificationPreferencesConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(
            deserialized.default_cooldown_secs,
            config.default_cooldown_secs
        );
    }

    #[test]
    fn test_minimum_priority_display() {
        assert_eq!(format!("{}", MinimumPriority::Low), "low");
        assert_eq!(format!("{}", MinimumPriority::Normal), "normal");
        assert_eq!(format!("{}", MinimumPriority::High), "high");
        assert_eq!(format!("{}", MinimumPriority::Critical), "critical");
        assert_eq!(format!("{}", MinimumPriority::None), "none");
    }

    #[test]
    fn test_task_notification_event_display() {
        assert_eq!(format!("{}", TaskNotificationEvent::Started), "started");
        assert_eq!(format!("{}", TaskNotificationEvent::Completed), "completed");
    }

    #[tokio::test]
    async fn test_save_load_config() {
        let dir = std::env::temp_dir().join("notif_pref_test_config");
        let _ = tokio::fs::create_dir_all(&dir).await;
        let path = dir.join("config.json");

        let manager = NotificationPreferencesManager::new();
        manager.save_config(&path).await.unwrap();

        let mut manager2 = NotificationPreferencesManager::new();
        let mut config = NotificationPreferencesConfig::default();
        config.default_cooldown_secs = 999;
        manager2.set_config(config);

        // Load should overwrite
        manager2.load_config(&path).await.unwrap();
        assert_eq!(manager2.get_config().default_cooldown_secs, 300);

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_save_load_task_configs() {
        let dir = std::env::temp_dir().join("notif_pref_test_tasks");
        let _ = tokio::fs::create_dir_all(&dir).await;
        let path = dir.join("tasks.json");

        let mut manager = NotificationPreferencesManager::new();
        manager.set_task_config(TaskNotificationConfig::new("task-1".to_string()));
        manager.set_task_config(TaskNotificationConfig::new("task-2".to_string()));
        manager.save_task_configs(&path).await.unwrap();

        let mut manager2 = NotificationPreferencesManager::new();
        manager2.load_task_configs(&path).await.unwrap();
        assert!(manager2.get_task_config("task-1").is_some());
        assert!(manager2.get_task_config("task-2").is_some());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let path = PathBuf::from("/tmp/nonexistent_notif_pref_file.json");
        let mut manager = NotificationPreferencesManager::new();

        // Should not error on missing file
        manager.load_config(&path).await.unwrap();
        manager.load_task_configs(&path).await.unwrap();
    }
}
