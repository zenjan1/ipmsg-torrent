//! Download Expiry Manager
//!
//! Manages download task expiry based on configurable policies.
//! Supports absolute expiry dates, relative duration from creation,
//! auto-cleanup of expired tasks, and pre-expiry notifications.
//!
//! Features:
//! - Per-task expiry dates (absolute or relative to creation)
//! - Global default expiry policy
//! - Pre-expiry notifications at configurable thresholds
//! - Auto-cleanup of expired tasks (pause or remove)
//! - Expiry summary with upcoming/active/expired counts
//! - Persistent storage to JSON
//! - Human-readable expiry status formatting

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Configuration for the download expiry manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpiryConfig {
    /// Whether expiry tracking is enabled.
    pub enabled: bool,
    /// Default expiry duration in seconds (None = no default expiry).
    /// Applied to tasks that don't have an explicit expiry set.
    pub default_expiry_secs: Option<u64>,
    /// Pre-expiry notification thresholds in seconds before expiry.
    /// E.g., [3600, 300] means notify at 1 hour and 5 minutes before expiry.
    pub notify_before_secs: Vec<u64>,
    /// What to do when a task expires: Pause or Remove.
    pub expiry_action: ExpiryAction,
    /// Whether to auto-remove expired tasks from the queue.
    pub auto_cleanup: bool,
    /// Maximum number of tasks to track (0 = unlimited).
    pub max_tracked_tasks: usize,
}

impl Default for ExpiryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_expiry_secs: None,
            notify_before_secs: vec![3600, 300],
            expiry_action: ExpiryAction::Pause,
            auto_cleanup: false,
            max_tracked_tasks: 0,
        }
    }
}

/// Action to take when a download expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryAction {
    /// Pause the expired download.
    #[default]
    Pause,
    /// Remove the expired download from the queue.
    Remove,
}

/// The expiry state for a single download task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExpiry {
    /// The task ID.
    pub task_id: String,
    /// Absolute expiry time (UTC).
    pub expires_at: DateTime<Utc>,
    /// When the expiry was set.
    pub set_at: DateTime<Utc>,
    /// Whether this task has already expired.
    pub expired: bool,
    /// Which notification thresholds have already fired.
    pub notified_thresholds: Vec<u64>,
}

impl TaskExpiry {
    /// Create a new task expiry with an absolute expiry time.
    pub fn new(task_id: String, expires_at: DateTime<Utc>) -> Self {
        Self {
            task_id,
            expires_at,
            set_at: Utc::now(),
            expired: false,
            notified_thresholds: Vec::new(),
        }
    }

    /// Create a new task expiry with a relative duration from now.
    pub fn with_duration(task_id: String, duration_secs: u64) -> Self {
        let expires_at = Utc::now() + Duration::seconds(duration_secs as i64);
        Self::new(task_id, expires_at)
    }

    /// Check if this task has expired.
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }

    /// Get remaining seconds until expiry (negative if expired).
    pub fn remaining_secs(&self) -> i64 {
        (self.expires_at - Utc::now()).num_seconds()
    }

    /// Get the urgency level based on remaining time.
    pub fn urgency(&self) -> ExpiryUrgency {
        let remaining = self.remaining_secs();
        if remaining <= 0 {
            ExpiryUrgency::Expired
        } else if remaining <= 300 {
            // 5 minutes
            ExpiryUrgency::Critical
        } else if remaining <= 3600 {
            // 1 hour
            ExpiryUrgency::High
        } else if remaining <= 86400 {
            // 24 hours
            ExpiryUrgency::Medium
        } else {
            ExpiryUrgency::Low
        }
    }

    /// Check if a notification threshold should fire.
    pub fn should_notify(&self, threshold_secs: u64) -> bool {
        let remaining = self.remaining_secs();
        if remaining < 0 {
            return false;
        }
        let threshold = threshold_secs as i64;
        remaining <= threshold && !self.notified_thresholds.contains(&threshold_secs)
    }

    /// Mark a threshold as notified.
    pub fn mark_notified(&mut self, threshold_secs: u64) {
        if !self.notified_thresholds.contains(&threshold_secs) {
            self.notified_thresholds.push(threshold_secs);
        }
    }
}

/// Urgency level for task expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryUrgency {
    /// Not expiring soon (> 24 hours remaining).
    Low,
    /// Expiring within 24 hours.
    Medium,
    /// Expiring within 1 hour.
    High,
    /// Expiring within 5 minutes.
    Critical,
    /// Already expired.
    Expired,
}

impl std::fmt::Display for ExpiryUrgency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpiryUrgency::Low => write!(f, "🟢 Low"),
            ExpiryUrgency::Medium => write!(f, "🟡 Medium"),
            ExpiryUrgency::High => write!(f, "🟠 High"),
            ExpiryUrgency::Critical => write!(f, "🔴 Critical"),
            ExpiryUrgency::Expired => write!(f, "⚫ Expired"),
        }
    }
}

/// Summary of all tracked task expiries.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExpirySummary {
    /// Total number of tracked tasks.
    pub total_tracked: usize,
    /// Number of tasks already expired.
    pub expired_count: usize,
    /// Number of tasks expiring within 5 minutes.
    pub critical_count: usize,
    /// Number of tasks expiring within 1 hour.
    pub high_count: usize,
    /// Number of tasks expiring within 24 hours.
    pub medium_count: usize,
    /// Number of tasks with > 24 hours remaining.
    pub low_count: usize,
    /// Task IDs that need notification (threshold crossed since last check).
    pub pending_notifications: Vec<String>,
    /// Task IDs that have expired since last check.
    pub newly_expired: Vec<String>,
}

impl ExpirySummary {
    /// Format a human-readable summary.
    pub fn format_summary(&self) -> String {
        let mut lines = Vec::new();
        lines.push("📅 Download Expiry Summary".to_string());
        lines.push(format!("  Total tracked: {}", self.total_tracked));
        lines.push(format!("  ⚫ Expired: {}", self.expired_count));
        lines.push(format!("  🔴 Critical (< 5 min): {}", self.critical_count));
        lines.push(format!("  🟠 High (< 1 hour): {}", self.high_count));
        lines.push(format!("  🟡 Medium (< 24 hours): {}", self.medium_count));
        lines.push(format!("  🟢 Low (> 24 hours): {}", self.low_count));
        if !self.pending_notifications.is_empty() {
            lines.push(format!(
                "  🔔 Pending notifications: {}",
                self.pending_notifications.len()
            ));
        }
        if !self.newly_expired.is_empty() {
            lines.push(format!("  ⏰ Newly expired: {}", self.newly_expired.len()));
        }
        lines.join("\n")
    }
}

/// The download expiry manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadExpiryManager {
    /// Manager configuration.
    config: ExpiryConfig,
    /// Tracked task expiries, keyed by task ID.
    tasks: HashMap<String, TaskExpiry>,
}

impl Default for DownloadExpiryManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadExpiryManager {
    /// Create a new expiry manager with default config.
    pub fn new() -> Self {
        Self {
            config: ExpiryConfig::default(),
            tasks: HashMap::new(),
        }
    }

    /// Create a new expiry manager with custom config.
    pub fn with_config(config: ExpiryConfig) -> Self {
        Self {
            config,
            tasks: HashMap::new(),
        }
    }

    /// Set the expiry for a task using an absolute time.
    pub fn set_expiry(&mut self, task_id: &str, expires_at: DateTime<Utc>) {
        if self.config.max_tracked_tasks > 0
            && self.tasks.len() >= self.config.max_tracked_tasks
            && !self.tasks.contains_key(task_id)
        {
            return;
        }
        let mut expiry = TaskExpiry::new(task_id.to_string(), expires_at);
        // Preserve previously notified thresholds if re-setting expiry
        if let Some(existing) = self.tasks.get(task_id) {
            expiry.notified_thresholds = existing.notified_thresholds.clone();
        }
        self.tasks.insert(task_id.to_string(), expiry);
    }

    /// Set the expiry for a task using a relative duration from now.
    pub fn set_expiry_duration(&mut self, task_id: &str, duration_secs: u64) {
        let expires_at = Utc::now() + Duration::seconds(duration_secs as i64);
        self.set_expiry(task_id, expires_at);
    }

    /// Set expiry using the default duration from config (if any).
    pub fn apply_default_expiry(&mut self, task_id: &str) {
        if let Some(secs) = self.config.default_expiry_secs {
            self.set_expiry_duration(task_id, secs);
        }
    }

    /// Remove the expiry for a task.
    pub fn remove_expiry(&mut self, task_id: &str) {
        self.tasks.remove(task_id);
    }

    /// Get the expiry info for a task.
    pub fn get_expiry(&self, task_id: &str) -> Option<&TaskExpiry> {
        self.tasks.get(task_id)
    }

    /// Check if a task has an expiry set.
    pub fn has_expiry(&self, task_id: &str) -> bool {
        self.tasks.contains_key(task_id)
    }

    /// Refresh all task expiry states. Returns list of newly expired task IDs.
    pub fn refresh(&mut self) -> Vec<String> {
        let mut newly_expired = Vec::new();
        for (task_id, expiry) in &mut self.tasks {
            if !expiry.expired && expiry.is_expired() {
                expiry.expired = true;
                newly_expired.push(task_id.clone());
            }
        }
        newly_expired
    }

    /// Check for pending notifications. Returns task IDs that crossed a threshold.
    pub fn check_notifications(&mut self) -> Vec<String> {
        let thresholds = self.config.notify_before_secs.clone();
        let mut pending = Vec::new();

        for (task_id, expiry) in &mut self.tasks {
            if expiry.expired {
                continue;
            }
            for &threshold in &thresholds {
                if expiry.should_notify(threshold) {
                    expiry.mark_notified(threshold);
                    if !pending.contains(task_id) {
                        pending.push(task_id.clone());
                    }
                }
            }
        }
        pending
    }

    /// Get the list of expired task IDs.
    pub fn get_expired_ids(&self) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|(_, e)| e.is_expired())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get a summary of all tracked expiries.
    pub fn get_summary(&self) -> ExpirySummary {
        let mut summary = ExpirySummary {
            total_tracked: self.tasks.len(),
            ..Default::default()
        };

        for expiry in self.tasks.values() {
            match expiry.urgency() {
                ExpiryUrgency::Expired => summary.expired_count += 1,
                ExpiryUrgency::Critical => summary.critical_count += 1,
                ExpiryUrgency::High => summary.high_count += 1,
                ExpiryUrgency::Medium => summary.medium_count += 1,
                ExpiryUrgency::Low => summary.low_count += 1,
            }
        }
        summary
    }

    /// Get tasks sorted by expiry time (earliest first).
    pub fn get_tasks_by_expiry(&self) -> Vec<&TaskExpiry> {
        let mut tasks: Vec<&TaskExpiry> = self.tasks.values().collect();
        tasks.sort_by_key(|t| t.expires_at);
        tasks
    }

    /// Clear all tracked expiries.
    pub fn clear(&mut self) {
        self.tasks.clear();
    }

    /// Remove expired tasks from tracking.
    pub fn cleanup_expired(&mut self) -> usize {
        let expired_ids: Vec<String> = self.get_expired_ids();
        for id in &expired_ids {
            self.tasks.remove(id);
        }
        expired_ids.len()
    }

    /// Get the configuration.
    pub fn config(&self) -> &ExpiryConfig {
        &self.config
    }

    /// Set the configuration.
    pub fn set_config(&mut self, config: ExpiryConfig) {
        self.config = config;
    }

    /// Get the number of tracked tasks.
    pub fn tracked_count(&self) -> usize {
        self.tasks.len()
    }

    /// Format a human-readable list of upcoming expiries.
    pub fn format_upcoming(&self, limit: usize) -> String {
        let tasks = self.get_tasks_by_expiry();
        if tasks.is_empty() {
            return "No tasks with expiry set.".to_string();
        }

        let mut lines = Vec::new();
        lines.push("📅 Upcoming Expiries:".to_string());

        for (i, expiry) in tasks.iter().take(limit).enumerate() {
            let remaining = expiry.remaining_secs();
            let urgency = expiry.urgency();
            let time_str = if remaining <= 0 {
                "EXPIRED".to_string()
            } else {
                format_remaining_time(remaining)
            };
            lines.push(format!(
                "  {}. {} - {} ({})",
                i + 1,
                expiry.task_id,
                time_str,
                urgency
            ));
        }

        if tasks.len() > limit {
            lines.push(format!("  ... and {} more", tasks.len() - limit));
        }

        lines.join("\n")
    }
}

/// Format remaining seconds into a human-readable string.
pub fn format_remaining_time(secs: i64) -> String {
    if secs <= 0 {
        return "expired".to_string();
    }
    let secs = secs as u64;
    if secs >= 86400 {
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        format!("{}d {}h", days, hours)
    } else if secs >= 3600 {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    } else if secs >= 60 {
        let mins = secs / 60;
        let s = secs % 60;
        format!("{}m {}s", mins, s)
    } else {
        format!("{}s", secs)
    }
}

/// Save expiry config to disk.
pub async fn save_expiry_config(
    config: &ExpiryConfig,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string_pretty(config)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Load expiry config from disk.
pub async fn load_expiry_config(
    path: &Path,
) -> Result<ExpiryConfig, Box<dyn std::error::Error + Send + Sync>> {
    let json = tokio::fs::read_to_string(path).await?;
    let config: ExpiryConfig = serde_json::from_str(&json)?;
    Ok(config)
}

/// Save expiry data (manager state) to disk.
pub async fn save_expiry_data(
    manager: &DownloadExpiryManager,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string_pretty(manager)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Load expiry data (manager state) from disk.
pub async fn load_expiry_data(
    path: &Path,
) -> Result<DownloadExpiryManager, Box<dyn std::error::Error + Send + Sync>> {
    let json = tokio::fs::read_to_string(path).await?;
    let manager: DownloadExpiryManager = serde_json::from_str(&json)?;
    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ExpiryConfig::default();
        assert!(config.enabled);
        assert!(config.default_expiry_secs.is_none());
        assert_eq!(config.notify_before_secs, vec![3600, 300]);
        assert_eq!(config.expiry_action, ExpiryAction::Pause);
        assert!(!config.auto_cleanup);
        assert_eq!(config.max_tracked_tasks, 0);
    }

    #[test]
    fn test_task_expiry_new() {
        let expires_at = Utc::now() + Duration::hours(2);
        let expiry = TaskExpiry::new("task-1".to_string(), expires_at);
        assert_eq!(expiry.task_id, "task-1");
        assert!(!expiry.expired);
        assert!(expiry.remaining_secs() > 0);
        assert!(!expiry.is_expired());
    }

    #[test]
    fn test_task_expiry_with_duration() {
        let expiry = TaskExpiry::with_duration("task-2".to_string(), 7200);
        assert_eq!(expiry.task_id, "task-2");
        assert!(!expiry.is_expired());
        let remaining = expiry.remaining_secs();
        assert!(remaining > 7100 && remaining <= 7200);
    }

    #[test]
    fn test_task_expiry_expired() {
        let expires_at = Utc::now() - Duration::hours(1);
        let expiry = TaskExpiry::new("task-3".to_string(), expires_at);
        assert!(expiry.is_expired());
        assert!(expiry.remaining_secs() < 0);
    }

    #[test]
    fn test_expiry_urgency_levels() {
        // Expired
        let expired = TaskExpiry::new("t".to_string(), Utc::now() - Duration::hours(1));
        assert_eq!(expired.urgency(), ExpiryUrgency::Expired);

        // Critical (< 5 min)
        let critical = TaskExpiry::new("t".to_string(), Utc::now() + Duration::minutes(3));
        assert_eq!(critical.urgency(), ExpiryUrgency::Critical);

        // High (< 1 hour)
        let high = TaskExpiry::new("t".to_string(), Utc::now() + Duration::minutes(30));
        assert_eq!(high.urgency(), ExpiryUrgency::High);

        // Medium (< 24 hours)
        let medium = TaskExpiry::new("t".to_string(), Utc::now() + Duration::hours(12));
        assert_eq!(medium.urgency(), ExpiryUrgency::Medium);

        // Low (> 24 hours)
        let low = TaskExpiry::new("t".to_string(), Utc::now() + Duration::days(2));
        assert_eq!(low.urgency(), ExpiryUrgency::Low);
    }

    #[test]
    fn test_should_notify() {
        let mut expiry = TaskExpiry::with_duration("t".to_string(), 1800); // 30 min
        assert!(expiry.should_notify(3600)); // 1 hour threshold (30min <= 1h)
        assert!(!expiry.should_notify(300)); // 5 min threshold (30min > 5min, not yet)
        assert!(expiry.should_notify(7200)); // 2 hour threshold (30min <= 2h, within window)

        expiry.mark_notified(3600);
        assert!(!expiry.should_notify(3600)); // already notified

        // A task expiring in 4 minutes should trigger the 5-min threshold
        let mut expiry2 = TaskExpiry::with_duration("t2".to_string(), 240); // 4 min
        assert!(expiry2.should_notify(300)); // 5 min threshold (4min <= 5min)
        assert!(expiry2.should_notify(3600)); // 1 hour threshold (4min <= 1h)
    }

    #[test]
    fn test_should_not_notify_expired() {
        let mut expiry = TaskExpiry::new("t".to_string(), Utc::now() - Duration::minutes(1));
        assert!(!expiry.should_notify(3600));
        assert!(!expiry.should_notify(300));
    }

    #[test]
    fn test_manager_set_and_get() {
        let mut mgr = DownloadExpiryManager::new();
        let expires_at = Utc::now() + Duration::hours(2);
        mgr.set_expiry("task-1", expires_at);

        assert!(mgr.has_expiry("task-1"));
        assert!(!mgr.has_expiry("task-2"));
        assert_eq!(mgr.tracked_count(), 1);

        let expiry = mgr.get_expiry("task-1").unwrap();
        assert_eq!(expiry.task_id, "task-1");
    }

    #[test]
    fn test_manager_set_duration() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry_duration("task-1", 3600);

        let expiry = mgr.get_expiry("task-1").unwrap();
        assert!(expiry.remaining_secs() > 3500);
    }

    #[test]
    fn test_manager_remove_expiry() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry_duration("task-1", 3600);
        assert!(mgr.has_expiry("task-1"));

        mgr.remove_expiry("task-1");
        assert!(!mgr.has_expiry("task-1"));
        assert_eq!(mgr.tracked_count(), 0);
    }

    #[test]
    fn test_manager_refresh() {
        let mut mgr = DownloadExpiryManager::new();
        // Add an already-expired task
        mgr.set_expiry("task-1", Utc::now() - Duration::hours(1));
        // Add a future task
        mgr.set_expiry_duration("task-2", 7200);

        let newly_expired = mgr.refresh();
        assert_eq!(newly_expired.len(), 1);
        assert!(newly_expired.contains(&"task-1".to_string()));

        // Second refresh should not report again
        let newly_expired2 = mgr.refresh();
        assert!(newly_expired2.is_empty());
    }

    #[test]
    fn test_manager_check_notifications() {
        let config = ExpiryConfig {
            notify_before_secs: vec![3600, 300],
            ..Default::default()
        };
        let mut mgr = DownloadExpiryManager::with_config(config);

        // Task expiring in 30 minutes - should trigger 1h threshold
        mgr.set_expiry_duration("task-1", 1800);

        let pending = mgr.check_notifications();
        assert_eq!(pending.len(), 1);
        assert!(pending.contains(&"task-1".to_string()));

        // Second check should not fire again
        let pending2 = mgr.check_notifications();
        assert!(pending2.is_empty());
    }

    #[test]
    fn test_manager_get_summary() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry("expired", Utc::now() - Duration::hours(1));
        mgr.set_expiry_duration("critical", 180); // 3 min
        mgr.set_expiry_duration("high", 1800); // 30 min
        mgr.set_expiry_duration("medium", 43200); // 12 hours
        mgr.set_expiry_duration("low", 172800); // 2 days

        let summary = mgr.get_summary();
        assert_eq!(summary.total_tracked, 5);
        assert_eq!(summary.expired_count, 1);
        assert_eq!(summary.critical_count, 1);
        assert_eq!(summary.high_count, 1);
        assert_eq!(summary.medium_count, 1);
        assert_eq!(summary.low_count, 1);
    }

    #[test]
    fn test_manager_clear() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry_duration("task-1", 3600);
        mgr.set_expiry_duration("task-2", 7200);
        assert_eq!(mgr.tracked_count(), 2);

        mgr.clear();
        assert_eq!(mgr.tracked_count(), 0);
    }

    #[test]
    fn test_manager_cleanup_expired() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry("expired-1", Utc::now() - Duration::hours(1));
        mgr.set_expiry("expired-2", Utc::now() - Duration::minutes(5));
        mgr.set_expiry_duration("active", 7200);

        let cleaned = mgr.cleanup_expired();
        assert_eq!(cleaned, 2);
        assert_eq!(mgr.tracked_count(), 1);
        assert!(mgr.has_expiry("active"));
    }

    #[test]
    fn test_max_tracked_tasks() {
        let config = ExpiryConfig {
            max_tracked_tasks: 2,
            ..Default::default()
        };
        let mut mgr = DownloadExpiryManager::with_config(config);
        mgr.set_expiry_duration("task-1", 3600);
        mgr.set_expiry_duration("task-2", 7200);
        mgr.set_expiry_duration("task-3", 10800); // should be rejected

        assert_eq!(mgr.tracked_count(), 2);
        assert!(!mgr.has_expiry("task-3"));
    }

    #[test]
    fn test_max_tracked_allows_update() {
        let config = ExpiryConfig {
            max_tracked_tasks: 2,
            ..Default::default()
        };
        let mut mgr = DownloadExpiryManager::with_config(config);
        mgr.set_expiry_duration("task-1", 3600);
        mgr.set_expiry_duration("task-2", 7200);

        // Updating existing task should work
        mgr.set_expiry_duration("task-1", 1800);
        assert_eq!(mgr.tracked_count(), 2);
    }

    #[test]
    fn test_apply_default_expiry() {
        let config = ExpiryConfig {
            default_expiry_secs: Some(86400), // 24 hours
            ..Default::default()
        };
        let mut mgr = DownloadExpiryManager::with_config(config);
        mgr.apply_default_expiry("task-1");

        let expiry = mgr.get_expiry("task-1").unwrap();
        let remaining = expiry.remaining_secs();
        assert!(remaining > 86300 && remaining <= 86400);
    }

    #[test]
    fn test_apply_default_expiry_no_default() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.apply_default_expiry("task-1");
        assert!(!mgr.has_expiry("task-1"));
    }

    #[test]
    fn test_get_tasks_by_expiry() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry_duration("later", 7200);
        mgr.set_expiry_duration("sooner", 1800);
        mgr.set_expiry_duration("earliest", 600);

        let sorted = mgr.get_tasks_by_expiry();
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].task_id, "earliest");
        assert_eq!(sorted[1].task_id, "sooner");
        assert_eq!(sorted[2].task_id, "later");
    }

    #[test]
    fn test_format_remaining_time() {
        assert_eq!(format_remaining_time(30), "30s");
        assert_eq!(format_remaining_time(90), "1m 30s");
        assert_eq!(format_remaining_time(3661), "1h 1m");
        assert_eq!(format_remaining_time(90000), "1d 1h");
        assert_eq!(format_remaining_time(0), "expired");
        assert_eq!(format_remaining_time(-100), "expired");
    }

    #[test]
    fn test_format_upcoming_empty() {
        let mgr = DownloadExpiryManager::new();
        assert_eq!(mgr.format_upcoming(10), "No tasks with expiry set.");
    }

    #[test]
    fn test_format_upcoming() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry_duration("task-1", 3600);
        let output = mgr.format_upcoming(5);
        assert!(output.contains("Upcoming Expiries"));
        assert!(output.contains("task-1"));
    }

    #[test]
    fn test_expiry_urgency_display() {
        assert_eq!(format!("{}", ExpiryUrgency::Low), "🟢 Low");
        assert_eq!(format!("{}", ExpiryUrgency::Medium), "🟡 Medium");
        assert_eq!(format!("{}", ExpiryUrgency::High), "🟠 High");
        assert_eq!(format!("{}", ExpiryUrgency::Critical), "🔴 Critical");
        assert_eq!(format!("{}", ExpiryUrgency::Expired), "⚫ Expired");
    }

    #[test]
    fn test_expiry_action_serialization() {
        let action = ExpiryAction::Pause;
        let json = serde_json::to_string(&action).unwrap();
        assert_eq!(json, "\"pause\"");

        let action: ExpiryAction = serde_json::from_str("\"remove\"").unwrap();
        assert_eq!(action, ExpiryAction::Remove);
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = ExpiryConfig {
            enabled: true,
            default_expiry_secs: Some(86400),
            notify_before_secs: vec![7200, 3600, 600],
            expiry_action: ExpiryAction::Remove,
            auto_cleanup: true,
            max_tracked_tasks: 500,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: ExpiryConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.default_expiry_secs, Some(86400));
        assert_eq!(loaded.notify_before_secs, vec![7200, 3600, 600]);
        assert_eq!(loaded.expiry_action, ExpiryAction::Remove);
        assert!(loaded.auto_cleanup);
        assert_eq!(loaded.max_tracked_tasks, 500);
    }

    #[test]
    fn test_manager_serialization_roundtrip() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry_duration("task-1", 3600);
        mgr.set_expiry_duration("task-2", 7200);

        let json = serde_json::to_string(&mgr).unwrap();
        let loaded: DownloadExpiryManager = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.tracked_count(), 2);
        assert!(loaded.has_expiry("task-1"));
        assert!(loaded.has_expiry("task-2"));
    }

    #[test]
    fn test_summary_format() {
        let summary = ExpirySummary {
            total_tracked: 10,
            expired_count: 2,
            critical_count: 1,
            high_count: 3,
            medium_count: 2,
            low_count: 2,
            pending_notifications: vec!["t1".to_string()],
            newly_expired: vec!["t2".to_string()],
        };
        let formatted = summary.format_summary();
        assert!(formatted.contains("Total tracked: 10"));
        assert!(formatted.contains("Expired: 2"));
        assert!(formatted.contains("Pending notifications: 1"));
        assert!(formatted.contains("Newly expired: 1"));
    }

    #[test]
    fn test_get_expired_ids() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry("expired-1", Utc::now() - Duration::hours(1));
        mgr.set_expiry_duration("active", 7200);
        mgr.set_expiry("expired-2", Utc::now() - Duration::seconds(10));

        let expired = mgr.get_expired_ids();
        assert_eq!(expired.len(), 2);
        assert!(expired.contains(&"expired-1".to_string()));
        assert!(expired.contains(&"expired-2".to_string()));
    }

    #[test]
    fn test_preserve_notified_thresholds_on_update() {
        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry_duration("task-1", 1800); // 30 min

        // Trigger notification
        let pending = mgr.check_notifications();
        assert!(!pending.is_empty());

        // Re-set expiry (extend it)
        mgr.set_expiry_duration("task-1", 7200); // 2 hours now

        // The notified threshold should be preserved
        let expiry = mgr.get_expiry("task-1").unwrap();
        assert!(expiry.notified_thresholds.contains(&3600));
    }

    #[tokio::test]
    async fn test_save_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("expiry_config.json");

        let config = ExpiryConfig {
            default_expiry_secs: Some(43200),
            notify_before_secs: vec![7200, 1800],
            expiry_action: ExpiryAction::Remove,
            ..Default::default()
        };
        save_expiry_config(&config, &path).await.unwrap();

        let loaded = load_expiry_config(&path).await.unwrap();
        assert_eq!(loaded.default_expiry_secs, Some(43200));
        assert_eq!(loaded.expiry_action, ExpiryAction::Remove);
    }

    #[tokio::test]
    async fn test_save_load_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("expiry_data.json");

        let mut mgr = DownloadExpiryManager::new();
        mgr.set_expiry_duration("task-1", 3600);
        mgr.set_expiry_duration("task-2", 7200);
        save_expiry_data(&mgr, &path).await.unwrap();

        let loaded = load_expiry_data(&path).await.unwrap();
        assert_eq!(loaded.tracked_count(), 2);
        assert!(loaded.has_expiry("task-1"));
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let result = load_expiry_config(&path).await;
        assert!(result.is_err());
    }
}
