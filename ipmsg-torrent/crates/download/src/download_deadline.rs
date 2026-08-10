//! Download Deadline Manager (Phase 107)
//!
//! Adds optional deadlines to download tasks with urgency tracking:
//! - Each task can have an optional deadline (absolute time)
//! - Urgency levels: None, Low (>24h), Medium (1-24h), High (1-6h), Critical (<1h)
//! - Deadline-aware scheduler boosts urgent tasks
//! - Persistent configuration to deadline_config.json

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

/// Urgency level for a download task with a deadline
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DeadlineUrgency {
    /// No deadline set
    #[default]
    None,
    /// Deadline > 24 hours away
    Low,
    /// Deadline 1-24 hours away
    Medium,
    /// Deadline 1-6 hours away
    High,
    /// Deadline < 1 hour away (or overdue)
    Critical,
}

impl std::fmt::Display for DeadlineUrgency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeadlineUrgency::None => write!(f, "none"),
            DeadlineUrgency::Low => write!(f, "low"),
            DeadlineUrgency::Medium => write!(f, "medium"),
            DeadlineUrgency::High => write!(f, "high"),
            DeadlineUrgency::Critical => write!(f, "critical"),
        }
    }
}

impl DeadlineUrgency {
    /// Emoji representation
    pub fn emoji(&self) -> &'static str {
        match self {
            DeadlineUrgency::None => "⚪",
            DeadlineUrgency::Low => "🟢",
            DeadlineUrgency::Medium => "🟡",
            DeadlineUrgency::High => "🟠",
            DeadlineUrgency::Critical => "🔴",
        }
    }

    /// Priority boost value for scheduler (higher = more urgent)
    pub fn scheduler_boost(&self) -> i32 {
        match self {
            DeadlineUrgency::None => 0,
            DeadlineUrgency::Low => 1,
            DeadlineUrgency::Medium => 2,
            DeadlineUrgency::High => 3,
            DeadlineUrgency::Critical => 5,
        }
    }
}

/// Deadline data stored per-task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadlineData {
    /// The deadline timestamp (UTC)
    pub deadline: DateTime<Utc>,
    /// Whether this deadline is enabled (can disable without removing)
    pub enabled: bool,
    /// Whether the deadline was missed (task not completed by deadline)
    pub missed: bool,
    /// Computed urgency (cached, updated on refresh)
    pub urgency: DeadlineUrgency,
}

/// Configuration for the deadline system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadlineConfig {
    /// Whether deadline tracking is enabled globally
    pub enabled: bool,
    /// Threshold for Low urgency (hours before deadline)
    pub low_threshold_hours: f64,
    /// Threshold for Medium urgency (hours before deadline)
    pub medium_threshold_hours: f64,
    /// Threshold for High urgency (hours before deadline)
    pub high_threshold_hours: f64,
    /// Whether to auto-boost priority of urgent tasks in scheduler
    pub auto_boost_priority: bool,
    /// Whether to send notifications when deadline is approaching
    pub notify_approaching: bool,
    /// Hours before deadline to send notification
    pub notify_hours_before: f64,
    /// Whether to auto-pause non-urgent tasks when critical tasks exist
    pub auto_pause_non_urgent: bool,
}

impl Default for DeadlineConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            low_threshold_hours: 24.0,
            medium_threshold_hours: 12.0,
            high_threshold_hours: 6.0,
            auto_boost_priority: true,
            notify_approaching: true,
            notify_hours_before: 2.0,
            auto_pause_non_urgent: false,
        }
    }
}

/// Summary of deadline status across all tasks
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeadlineSummary {
    /// Total tasks with deadlines
    pub tasks_with_deadlines: usize,
    /// Tasks at each urgency level
    pub critical_count: usize,
    pub high_count: usize,
    pub medium_count: usize,
    pub low_count: usize,
    /// Tasks that missed their deadline
    pub missed_count: usize,
    /// Tasks completed before deadline
    pub completed_on_time: usize,
}

impl DeadlineSummary {
    pub fn format_summary(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "📅 Deadline Summary: {} tasks with deadlines",
            self.tasks_with_deadlines
        ));
        if self.critical_count > 0 {
            lines.push(format!("  🔴 Critical: {}", self.critical_count));
        }
        if self.high_count > 0 {
            lines.push(format!("  🟠 High:     {}", self.high_count));
        }
        if self.medium_count > 0 {
            lines.push(format!("  🟡 Medium:   {}", self.medium_count));
        }
        if self.low_count > 0 {
            lines.push(format!("  🟢 Low:      {}", self.low_count));
        }
        if self.missed_count > 0 {
            lines.push(format!("  ❌ Missed:   {}", self.missed_count));
        }
        if self.completed_on_time > 0 {
            lines.push(format!("  ✅ On time:  {}", self.completed_on_time));
        }
        lines.join("\n")
    }
}

/// Manager for tracking download deadlines
#[derive(Debug, Clone)]
pub struct DeadlineManager {
    /// Per-task deadline data (task_id -> DeadlineData)
    deadlines: std::collections::HashMap<String, DeadlineData>,
    /// Configuration
    config: DeadlineConfig,
}

impl Default for DeadlineManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DeadlineManager {
    /// Create a new DeadlineManager with default configuration
    pub fn new() -> Self {
        Self {
            deadlines: std::collections::HashMap::new(),
            config: DeadlineConfig::default(),
        }
    }

    /// Create a DeadlineManager with custom configuration
    pub fn with_config(config: DeadlineConfig) -> Self {
        Self {
            deadlines: std::collections::HashMap::new(),
            config,
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> &DeadlineConfig {
        &self.config
    }

    /// Set the configuration
    pub fn set_config(&mut self, config: DeadlineConfig) {
        self.config = config;
    }

    /// Set a deadline for a task
    pub fn set_deadline(&mut self, task_id: &str, deadline: DateTime<Utc>, enabled: bool) {
        let now = Utc::now();
        let urgency = if enabled {
            compute_urgency(&deadline, &now, &self.config)
        } else {
            DeadlineUrgency::None
        };
        let data = DeadlineData {
            deadline,
            enabled,
            missed: false,
            urgency,
        };
        self.deadlines.insert(task_id.to_string(), data);
    }

    /// Remove a deadline for a task
    pub fn remove_deadline(&mut self, task_id: &str) -> bool {
        self.deadlines.remove(task_id).is_some()
    }

    /// Get deadline data for a task
    pub fn get_deadline(&self, task_id: &str) -> Option<&DeadlineData> {
        self.deadlines.get(task_id)
    }

    /// Check if a task has a deadline
    pub fn has_deadline(&self, task_id: &str) -> bool {
        self.deadlines.contains_key(task_id)
    }

    /// Refresh urgency levels for all deadlines
    pub fn refresh_all(&mut self) {
        let now = Utc::now();
        for data in self.deadlines.values_mut() {
            if data.enabled {
                data.urgency = compute_urgency(&data.deadline, &now, &self.config);
                // Mark as missed if overdue
                if data.deadline < now && !data.missed {
                    data.missed = true;
                }
            }
        }
    }

    /// Get a summary of all deadlines
    pub fn summary(&self) -> DeadlineSummary {
        let mut summary = DeadlineSummary::default();
        summary.tasks_with_deadlines = self.deadlines.len();

        for data in self.deadlines.values() {
            if data.missed {
                summary.missed_count += 1;
            }
            match data.urgency {
                DeadlineUrgency::None => {}
                DeadlineUrgency::Low => summary.low_count += 1,
                DeadlineUrgency::Medium => summary.medium_count += 1,
                DeadlineUrgency::High => summary.high_count += 1,
                DeadlineUrgency::Critical => summary.critical_count += 1,
            }
        }

        summary
    }

    /// Get all task IDs with deadlines
    pub fn task_ids_with_deadlines(&self) -> Vec<&str> {
        self.deadlines.keys().map(|s| s.as_str()).collect()
    }

    /// Get all task IDs at a specific urgency level
    pub fn task_ids_at_urgency(&self, urgency: DeadlineUrgency) -> Vec<&str> {
        self.deadlines
            .iter()
            .filter(|(_, data)| data.urgency == urgency)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Clear all deadlines
    pub fn clear_all(&mut self) {
        self.deadlines.clear();
    }

    /// Mark a task as completed on time (removes its deadline)
    pub fn mark_completed(&mut self, task_id: &str) {
        self.deadlines.remove(task_id);
    }
}

/// Compute urgency level for a deadline given current time and config
pub fn compute_urgency(
    deadline: &DateTime<Utc>,
    now: &DateTime<Utc>,
    config: &DeadlineConfig,
) -> DeadlineUrgency {
    let remaining = *deadline - *now;
    let remaining_hours = remaining.num_seconds().max(0) as f64 / 3600.0;

    if remaining.num_seconds() < 0 {
        // Overdue
        DeadlineUrgency::Critical
    } else if remaining_hours <= config.high_threshold_hours {
        // Within high threshold (e.g. <= 6h) → Critical
        DeadlineUrgency::Critical
    } else if remaining_hours <= config.medium_threshold_hours {
        // Between high and medium threshold (e.g. 6h-12h) → High
        DeadlineUrgency::High
    } else if remaining_hours <= config.low_threshold_hours {
        // Between medium and low threshold (e.g. 12h-24h) → Medium
        DeadlineUrgency::Medium
    } else {
        // Beyond low threshold (e.g. > 24h) → Low
        DeadlineUrgency::Low
    }
}

/// Format remaining time as human-readable string
pub fn format_remaining(deadline: &DateTime<Utc>, now: &DateTime<Utc>) -> String {
    let remaining = *deadline - *now;
    let total_secs = remaining.num_seconds();

    if total_secs < 0 {
        let overdue_secs = -total_secs;
        if overdue_secs < 3600 {
            format!("overdue by {}m", overdue_secs / 60)
        } else if overdue_secs < 86400 {
            format!("overdue by {}h", overdue_secs / 3600)
        } else {
            format!("overdue by {}d", overdue_secs / 86400)
        }
    } else if total_secs < 3600 {
        format!("{}m left", total_secs / 60)
    } else if total_secs < 86400 {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        format!("{}h {}m left", hours, mins)
    } else {
        let days = total_secs / 86400;
        let hours = (total_secs % 86400) / 3600;
        format!("{}d {}h left", days, hours)
    }
}

/// Persistence helpers
pub async fn save_deadline_config(config: &DeadlineConfig, path: &Path) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, json.as_bytes()).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

pub async fn load_deadline_config(path: &Path) -> Result<DeadlineConfig, std::io::Error> {
    let content = fs::read_to_string(path).await?;
    serde_json::from_str(&content)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn test_config() -> DeadlineConfig {
        DeadlineConfig::default()
    }

    #[test]
    fn test_urgency_none_when_no_deadline() {
        // DeadlineUrgency::None is the default when no deadline is set
        let urgency = DeadlineUrgency::None;
        assert_eq!(urgency.scheduler_boost(), 0);
        assert_eq!(urgency.emoji(), "⚪");
    }

    #[test]
    fn test_urgency_critical_when_overdue() {
        let config = test_config();
        let now = Utc::now();
        let deadline = now - Duration::hours(1);
        let urgency = compute_urgency(&deadline, &now, &config);
        assert_eq!(urgency, DeadlineUrgency::Critical);
    }

    #[test]
    fn test_urgency_critical_within_high_threshold() {
        let config = test_config();
        let now = Utc::now();
        let deadline = now + Duration::minutes(30);
        let urgency = compute_urgency(&deadline, &now, &config);
        assert_eq!(urgency, DeadlineUrgency::Critical);
    }

    #[test]
    fn test_urgency_high_within_medium_threshold() {
        let config = test_config();
        let now = Utc::now();
        // 8 hours is between high_threshold (6h) and medium_threshold (12h) → High
        let deadline = now + Duration::hours(8);
        let urgency = compute_urgency(&deadline, &now, &config);
        assert_eq!(urgency, DeadlineUrgency::High);
    }

    #[test]
    fn test_urgency_medium_within_low_threshold() {
        let config = test_config();
        let now = Utc::now();
        let deadline = now + Duration::hours(18);
        let urgency = compute_urgency(&deadline, &now, &config);
        assert_eq!(urgency, DeadlineUrgency::Medium);
    }

    #[test]
    fn test_urgency_low_beyond_low_threshold() {
        let config = test_config();
        let now = Utc::now();
        let deadline = now + Duration::hours(48);
        let urgency = compute_urgency(&deadline, &now, &config);
        assert_eq!(urgency, DeadlineUrgency::Low);
    }

    #[test]
    fn test_scheduler_boost_ordering() {
        assert!(
            DeadlineUrgency::Critical.scheduler_boost() > DeadlineUrgency::High.scheduler_boost()
        );
        assert!(
            DeadlineUrgency::High.scheduler_boost() > DeadlineUrgency::Medium.scheduler_boost()
        );
        assert!(DeadlineUrgency::Medium.scheduler_boost() > DeadlineUrgency::Low.scheduler_boost());
        assert!(DeadlineUrgency::Low.scheduler_boost() > DeadlineUrgency::None.scheduler_boost());
    }

    #[test]
    fn test_format_remaining_minutes() {
        let now = Utc::now();
        let deadline = now + Duration::minutes(45);
        let result = format_remaining(&deadline, &now);
        assert_eq!(result, "45m left");
    }

    #[test]
    fn test_format_remaining_hours() {
        let now = Utc::now();
        let deadline = now + Duration::hours(3) + Duration::minutes(15);
        let result = format_remaining(&deadline, &now);
        assert_eq!(result, "3h 15m left");
    }

    #[test]
    fn test_format_remaining_days() {
        let now = Utc::now();
        let deadline = now + Duration::days(2) + Duration::hours(5);
        let result = format_remaining(&deadline, &now);
        assert_eq!(result, "2d 5h left");
    }

    #[test]
    fn test_format_remaining_overdue() {
        let now = Utc::now();
        let deadline = now - Duration::hours(2);
        let result = format_remaining(&deadline, &now);
        assert_eq!(result, "overdue by 2h");
    }

    #[test]
    fn test_format_remaining_overdue_days() {
        let now = Utc::now();
        let deadline = now - Duration::days(3);
        let result = format_remaining(&deadline, &now);
        assert_eq!(result, "overdue by 3d");
    }

    #[test]
    fn test_deadline_data_serialization() {
        let data = DeadlineData {
            deadline: Utc::now() + Duration::hours(5),
            enabled: true,
            missed: false,
            urgency: DeadlineUrgency::High,
        };
        let json = serde_json::to_string(&data).unwrap();
        let deserialized: DeadlineData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, true);
        assert_eq!(deserialized.missed, false);
        assert_eq!(deserialized.urgency, DeadlineUrgency::High);
    }

    #[test]
    fn test_deadline_config_default() {
        let config = DeadlineConfig::default();
        assert!(config.enabled);
        assert!(config.auto_boost_priority);
        assert!(config.notify_approaching);
        assert_eq!(config.low_threshold_hours, 24.0);
        assert_eq!(config.medium_threshold_hours, 12.0);
        assert_eq!(config.high_threshold_hours, 6.0);
        assert!(!config.auto_pause_non_urgent);
    }

    #[test]
    fn test_deadline_config_serialization() {
        let config = DeadlineConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        let deserialized: DeadlineConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.low_threshold_hours, config.low_threshold_hours);
    }

    #[test]
    fn test_deadline_summary_format() {
        let summary = DeadlineSummary {
            tasks_with_deadlines: 5,
            critical_count: 1,
            high_count: 2,
            medium_count: 1,
            low_count: 1,
            missed_count: 0,
            completed_on_time: 3,
        };
        let output = summary.format_summary();
        assert!(output.contains("5 tasks with deadlines"));
        assert!(output.contains("Critical: 1"));
        assert!(output.contains("High:     2"));
        assert!(output.contains("On time:  3"));
    }

    #[test]
    fn test_deadline_summary_empty() {
        let summary = DeadlineSummary::default();
        let output = summary.format_summary();
        assert!(output.contains("0 tasks with deadlines"));
    }

    #[test]
    fn test_urgency_display() {
        assert_eq!(format!("{}", DeadlineUrgency::None), "none");
        assert_eq!(format!("{}", DeadlineUrgency::Low), "low");
        assert_eq!(format!("{}", DeadlineUrgency::Medium), "medium");
        assert_eq!(format!("{}", DeadlineUrgency::High), "high");
        assert_eq!(format!("{}", DeadlineUrgency::Critical), "critical");
    }

    #[test]
    fn test_urgency_emoji() {
        assert_eq!(DeadlineUrgency::None.emoji(), "⚪");
        assert_eq!(DeadlineUrgency::Low.emoji(), "🟢");
        assert_eq!(DeadlineUrgency::Medium.emoji(), "🟡");
        assert_eq!(DeadlineUrgency::High.emoji(), "🟠");
        assert_eq!(DeadlineUrgency::Critical.emoji(), "🔴");
    }

    #[test]
    fn test_custom_thresholds() {
        let config = DeadlineConfig {
            low_threshold_hours: 48.0,
            medium_threshold_hours: 24.0,
            high_threshold_hours: 12.0,
            ..Default::default()
        };
        let now = Utc::now();

        // 36 hours -> Medium (between 24 and 48)
        let deadline = now + Duration::hours(36);
        assert_eq!(
            compute_urgency(&deadline, &now, &config),
            DeadlineUrgency::Medium
        );

        // 18 hours -> High (between 12 and 24)
        let deadline = now + Duration::hours(18);
        assert_eq!(
            compute_urgency(&deadline, &now, &config),
            DeadlineUrgency::High
        );
    }

    #[tokio::test]
    async fn test_save_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deadline_config.json");

        let config = DeadlineConfig {
            enabled: false,
            low_threshold_hours: 36.0,
            ..Default::default()
        };

        save_deadline_config(&config, &path).await.unwrap();
        let loaded = load_deadline_config(&path).await.unwrap();

        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.low_threshold_hours, 36.0);
    }

    #[tokio::test]
    async fn test_load_missing_file() {
        let path = std::path::PathBuf::from("/tmp/nonexistent_deadline_config.json");
        let result = load_deadline_config(&path).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_deadline_data_disabled_deadline() {
        let data = DeadlineData {
            deadline: Utc::now() - Duration::hours(1),
            enabled: false,
            missed: false,
            urgency: DeadlineUrgency::None,
        };
        // Disabled deadline should not be considered urgent
        assert!(!data.enabled);
        assert_eq!(data.urgency, DeadlineUrgency::None);
    }

    #[test]
    fn test_deadline_data_missed_flag() {
        let data = DeadlineData {
            deadline: Utc::now() - Duration::hours(2),
            enabled: true,
            missed: true,
            urgency: DeadlineUrgency::Critical,
        };
        assert!(data.missed);
        assert_eq!(data.urgency, DeadlineUrgency::Critical);
    }

    // ===== DeadlineManager Tests =====

    #[test]
    fn test_manager_new() {
        let mgr = DeadlineManager::new();
        assert_eq!(mgr.config().enabled, true);
        assert_eq!(mgr.task_ids_with_deadlines().len(), 0);
    }

    #[test]
    fn test_manager_with_config() {
        let config = DeadlineConfig {
            enabled: false,
            low_threshold_hours: 48.0,
            ..Default::default()
        };
        let mgr = DeadlineManager::with_config(config);
        assert_eq!(mgr.config().enabled, false);
        assert_eq!(mgr.config().low_threshold_hours, 48.0);
    }

    #[test]
    fn test_manager_set_deadline() {
        let mut mgr = DeadlineManager::new();
        let deadline = Utc::now() + Duration::hours(2);
        mgr.set_deadline("task1", deadline, true);
        assert!(mgr.has_deadline("task1"));
        let data = mgr.get_deadline("task1").unwrap();
        assert_eq!(data.enabled, true);
        assert_eq!(data.urgency, DeadlineUrgency::Critical); // 2h is within high threshold
    }

    #[test]
    fn test_manager_set_deadline_disabled() {
        let mut mgr = DeadlineManager::new();
        let deadline = Utc::now() + Duration::hours(2);
        mgr.set_deadline("task1", deadline, false);
        assert!(mgr.has_deadline("task1"));
        let data = mgr.get_deadline("task1").unwrap();
        assert_eq!(data.enabled, false);
        assert_eq!(data.urgency, DeadlineUrgency::None);
    }

    #[test]
    fn test_manager_remove_deadline() {
        let mut mgr = DeadlineManager::new();
        let deadline = Utc::now() + Duration::hours(2);
        mgr.set_deadline("task1", deadline, true);
        assert!(mgr.has_deadline("task1"));
        assert!(mgr.remove_deadline("task1"));
        assert!(!mgr.has_deadline("task1"));
        assert!(!mgr.remove_deadline("task1")); // Already removed
    }

    #[test]
    fn test_manager_refresh_all() {
        let mut mgr = DeadlineManager::new();
        let deadline = Utc::now() + Duration::hours(2);
        mgr.set_deadline("task1", deadline, true);
        mgr.refresh_all();
        let data = mgr.get_deadline("task1").unwrap();
        assert_eq!(data.urgency, DeadlineUrgency::Critical);
    }

    #[test]
    fn test_manager_refresh_all_marks_missed() {
        let mut mgr = DeadlineManager::new();
        let deadline = Utc::now() - Duration::hours(1);
        mgr.set_deadline("task1", deadline, true);
        mgr.refresh_all();
        let data = mgr.get_deadline("task1").unwrap();
        assert!(data.missed);
    }

    #[test]
    fn test_manager_summary() {
        let mut mgr = DeadlineManager::new();
        mgr.set_deadline("task1", Utc::now() + Duration::hours(2), true); // Critical
        mgr.set_deadline("task2", Utc::now() + Duration::hours(48), true); // Low
        mgr.set_deadline("task3", Utc::now() - Duration::hours(1), true); // Critical + missed
        // Need to refresh to mark overdue tasks as missed
        mgr.refresh_all();
        let summary = mgr.summary();
        assert_eq!(summary.tasks_with_deadlines, 3);
        assert_eq!(summary.critical_count, 2);
        assert_eq!(summary.low_count, 1);
        assert_eq!(summary.missed_count, 1);
    }

    #[test]
    fn test_manager_task_ids_at_urgency() {
        let mut mgr = DeadlineManager::new();
        mgr.set_deadline("task1", Utc::now() + Duration::hours(2), true); // Critical
        mgr.set_deadline("task2", Utc::now() + Duration::hours(48), true); // Low
        let critical_ids = mgr.task_ids_at_urgency(DeadlineUrgency::Critical);
        assert_eq!(critical_ids.len(), 1);
        assert!(critical_ids.contains(&"task1"));
        let low_ids = mgr.task_ids_at_urgency(DeadlineUrgency::Low);
        assert_eq!(low_ids.len(), 1);
        assert!(low_ids.contains(&"task2"));
    }

    #[test]
    fn test_manager_clear_all() {
        let mut mgr = DeadlineManager::new();
        mgr.set_deadline("task1", Utc::now() + Duration::hours(2), true);
        mgr.set_deadline("task2", Utc::now() + Duration::hours(48), true);
        assert_eq!(mgr.task_ids_with_deadlines().len(), 2);
        mgr.clear_all();
        assert_eq!(mgr.task_ids_with_deadlines().len(), 0);
    }

    #[test]
    fn test_manager_mark_completed() {
        let mut mgr = DeadlineManager::new();
        mgr.set_deadline("task1", Utc::now() + Duration::hours(2), true);
        assert!(mgr.has_deadline("task1"));
        mgr.mark_completed("task1");
        assert!(!mgr.has_deadline("task1"));
    }

    #[test]
    fn test_manager_set_config() {
        let mut mgr = DeadlineManager::new();
        let new_config = DeadlineConfig {
            enabled: false,
            low_threshold_hours: 100.0,
            ..Default::default()
        };
        mgr.set_config(new_config);
        assert_eq!(mgr.config().enabled, false);
        assert_eq!(mgr.config().low_threshold_hours, 100.0);
    }
}
