//! Task Scheduler - time-based scheduling with quiet hours and bandwidth-aware timing.
//!
//! Allows users to define schedules for when downloads should run:
//! - Quiet hours: pause all downloads during specific time windows (e.g., 8am-6pm)
//! - Bandwidth-aware timing: prioritize downloads during low-bandwidth periods
//! - Day-of-week filtering: different schedules for weekdays vs weekends
//! - Priority-based scheduling: high-priority tasks bypass quiet hours
//!
//! Schedules persist to disk and are restored on startup.

use chrono::{Datelike, Local, Timelike, Weekday};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

/// Error type for task scheduler operations.
#[derive(Debug, thiserror::Error)]
pub enum TaskSchedulerError {
    #[error("invalid schedule: {0}")]
    InvalidSchedule(String),
    #[error("persistence error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Type of schedule rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleType {
    /// Quiet hours: pause all downloads during this window.
    QuietHours,
    /// Active hours: allow downloads during this window.
    ActiveHours,
    /// High bandwidth period: prioritize downloads.
    HighBandwidth,
    /// Low bandwidth period: limit downloads.
    LowBandwidth,
}

/// A schedule rule defining when downloads should run or be paused.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRule {
    /// Unique rule identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Type of schedule.
    pub schedule_type: ScheduleType,
    /// Start hour (0-23).
    pub start_hour: u32,
    /// Start minute (0-59).
    pub start_minute: u32,
    /// End hour (0-23).
    pub end_hour: u32,
    /// End minute (0-59).
    pub end_minute: u32,
    /// Days of week when this rule applies (empty = every day).
    pub days_of_week: Vec<Weekday>,
    /// Whether this rule is enabled.
    pub enabled: bool,
    /// Priority (higher number = higher priority).
    pub priority: i32,
    /// Optional speed limit in bytes per second (0 = unlimited).
    pub speed_limit_bps: u64,
}

impl ScheduleRule {
    /// Create a new schedule rule.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        schedule_type: ScheduleType,
        start_hour: u32,
        start_minute: u32,
        end_hour: u32,
        end_minute: u32,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            schedule_type,
            start_hour,
            start_minute,
            end_hour,
            end_minute,
            days_of_week: Vec::new(),
            enabled: true,
            priority: 0,
            speed_limit_bps: 0,
        }
    }

    /// Set days of week for this rule.
    pub fn with_days(mut self, days: Vec<Weekday>) -> Self {
        self.days_of_week = days;
        self
    }

    /// Set priority for this rule.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set speed limit for this rule.
    pub fn with_speed_limit(mut self, speed_limit_bps: u64) -> Self {
        self.speed_limit_bps = speed_limit_bps;
        self
    }

    /// Check if this rule applies at the given time.
    pub fn applies_at(&self, time: chrono::DateTime<Local>) -> bool {
        if !self.enabled {
            return false;
        }

        // Check day of week filter.
        if !self.days_of_week.is_empty() && !self.days_of_week.contains(&time.weekday()) {
            return false;
        }

        // Check time window (supports overnight ranges).
        let time_minutes = time.hour() * 60 + time.minute();
        let start_minutes = self.start_hour * 60 + self.start_minute;
        let end_minutes = self.end_hour * 60 + self.end_minute;

        if start_minutes <= end_minutes {
            // Normal range (e.g., 9:00-17:00).
            time_minutes >= start_minutes && time_minutes < end_minutes
        } else {
            // Overnight range (e.g., 22:00-06:00).
            time_minutes >= start_minutes || time_minutes < end_minutes
        }
    }
}

/// Configuration for the task scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSchedulerConfig {
    /// Whether scheduling is enabled.
    pub enabled: bool,
    /// Whether high-priority tasks bypass quiet hours.
    pub high_priority_bypass_quiet: bool,
    /// Default speed limit during quiet hours (0 = pause).
    pub quiet_hours_speed_limit_bps: u64,
    /// Default speed limit during low bandwidth periods (0 = unlimited).
    pub low_bandwidth_speed_limit_bps: u64,
}

impl Default for TaskSchedulerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            high_priority_bypass_quiet: true,
            quiet_hours_speed_limit_bps: 0,
            low_bandwidth_speed_limit_bps: 0,
        }
    }
}

/// Persisted scheduler data (rules + config).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskSchedulerData {
    pub config: TaskSchedulerConfig,
    pub rules: Vec<ScheduleRule>,
}

/// Result of evaluating schedules at a specific time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleEvaluation {
    /// Whether downloads should be paused.
    pub should_pause: bool,
    /// Speed limit to apply (0 = unlimited).
    pub speed_limit_bps: u64,
    /// Active schedule rule IDs.
    pub active_rule_ids: Vec<String>,
    /// Human-readable description of active schedules.
    pub description: String,
}

/// Manager for task scheduling operations.
#[derive(Debug, Clone)]
pub struct TaskSchedulerManager {
    config: TaskSchedulerConfig,
    rules: Vec<ScheduleRule>,
}

impl TaskSchedulerManager {
    /// Create a new manager with default configuration.
    pub fn new() -> Self {
        Self {
            config: TaskSchedulerConfig::default(),
            rules: Vec::new(),
        }
    }

    /// Set configuration.
    pub fn set_config(&mut self, config: TaskSchedulerConfig) {
        self.config = config;
    }

    /// Get current configuration.
    pub fn get_config(&self) -> &TaskSchedulerConfig {
        &self.config
    }

    /// Add a schedule rule.
    pub fn add_rule(&mut self, rule: ScheduleRule) {
        self.rules.push(rule);
    }

    /// Remove a schedule rule by ID.
    pub fn remove_rule(&mut self, rule_id: &str) -> bool {
        let initial_len = self.rules.len();
        self.rules.retain(|r| r.id != rule_id);
        self.rules.len() < initial_len
    }

    /// Get all rules.
    pub fn get_rules(&self) -> &[ScheduleRule] {
        &self.rules
    }

    /// Enable or disable a rule.
    pub fn set_rule_enabled(&mut self, rule_id: &str, enabled: bool) -> bool {
        if let Some(rule) = self.rules.iter_mut().find(|r| r.id == rule_id) {
            rule.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Evaluate schedules at the current time.
    pub fn evaluate_now(&self) -> ScheduleEvaluation {
        self.evaluate_at(Local::now())
    }

    /// Evaluate schedules at a specific time.
    pub fn evaluate_at(&self, time: chrono::DateTime<Local>) -> ScheduleEvaluation {
        if !self.config.enabled {
            return ScheduleEvaluation {
                should_pause: false,
                speed_limit_bps: 0,
                active_rule_ids: Vec::new(),
                description: "Scheduler disabled".to_string(),
            };
        }

        // Find all applicable rules, sorted by priority (highest first).
        let mut applicable: Vec<&ScheduleRule> =
            self.rules.iter().filter(|r| r.applies_at(time)).collect();
        applicable.sort_by(|a, b| b.priority.cmp(&a.priority));

        if applicable.is_empty() {
            return ScheduleEvaluation {
                should_pause: false,
                speed_limit_bps: 0,
                active_rule_ids: Vec::new(),
                description: "No active schedules".to_string(),
            };
        }

        // Use the highest priority rule to determine the primary action.
        // All applicable rules are tracked, but the first (highest priority) wins.
        let primary = applicable[0];
        let mut should_pause = false;
        let mut speed_limit_bps = 0;
        let mut active_rule_ids = Vec::new();
        let mut descriptions = Vec::new();

        for rule in &applicable {
            active_rule_ids.push(rule.id.clone());
            descriptions.push(rule.name.clone());
        }

        match primary.schedule_type {
            ScheduleType::QuietHours => {
                should_pause = true;
                if self.config.quiet_hours_speed_limit_bps > 0 {
                    speed_limit_bps = self.config.quiet_hours_speed_limit_bps;
                }
            }
            ScheduleType::ActiveHours => {
                should_pause = false;
                speed_limit_bps = 0;
            }
            ScheduleType::HighBandwidth => {
                speed_limit_bps = 0; // Unlimited.
            }
            ScheduleType::LowBandwidth => {
                if self.config.low_bandwidth_speed_limit_bps > 0 {
                    speed_limit_bps = self.config.low_bandwidth_speed_limit_bps;
                } else if primary.speed_limit_bps > 0 {
                    speed_limit_bps = primary.speed_limit_bps;
                }
            }
        }

        ScheduleEvaluation {
            should_pause,
            speed_limit_bps,
            active_rule_ids,
            description: descriptions.join(", "),
        }
    }

    /// Save scheduler data to disk.
    pub async fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), TaskSchedulerError> {
        let data = TaskSchedulerData {
            config: self.config.clone(),
            rules: self.rules.clone(),
        };
        let json = serde_json::to_string_pretty(&data)?;
        fs::write(path.as_ref(), json).await?;
        Ok(())
    }

    /// Load scheduler data from disk.
    pub async fn load_from_file(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<(), TaskSchedulerError> {
        let json = fs::read_to_string(path.as_ref()).await?;
        let data: TaskSchedulerData = serde_json::from_str(&json)?;
        self.config = data.config;
        self.rules = data.rules;
        Ok(())
    }
}

impl Default for TaskSchedulerManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_schedule_rule_applies_normal_range() {
        let rule = ScheduleRule::new("r1", "Work Hours", ScheduleType::QuietHours, 9, 0, 17, 0);
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 30, 0).unwrap(); // Monday 10:30.
        assert!(rule.applies_at(time));

        let time_outside = Local.with_ymd_and_hms(2026, 8, 10, 18, 0, 0).unwrap(); // 18:00.
        assert!(!rule.applies_at(time_outside));
    }

    #[test]
    fn test_schedule_rule_applies_overnight() {
        let rule = ScheduleRule::new("r1", "Night", ScheduleType::ActiveHours, 22, 0, 6, 0);
        let time_night = Local.with_ymd_and_hms(2026, 8, 10, 23, 0, 0).unwrap(); // 23:00.
        assert!(rule.applies_at(time_night));

        let time_early = Local.with_ymd_and_hms(2026, 8, 11, 3, 0, 0).unwrap(); // 03:00 next day.
        assert!(rule.applies_at(time_early));

        let time_day = Local.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap(); // 12:00.
        assert!(!rule.applies_at(time_day));
    }

    #[test]
    fn test_schedule_rule_day_filter() {
        let rule = ScheduleRule::new("r1", "Weekend", ScheduleType::HighBandwidth, 0, 0, 24, 0)
            .with_days(vec![Weekday::Sat, Weekday::Sun]);

        let saturday = Local.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap(); // Saturday.
        assert!(rule.applies_at(saturday));

        let monday = Local.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap(); // Monday.
        assert!(!rule.applies_at(monday));
    }

    #[test]
    fn test_schedule_rule_disabled() {
        let mut rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        rule.enabled = false;
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 30, 0).unwrap();
        assert!(!rule.applies_at(time));
    }

    #[test]
    fn test_evaluate_no_rules() {
        let manager = TaskSchedulerManager::new();
        let eval = manager.evaluate_now();
        assert!(!eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 0);
        assert!(eval.active_rule_ids.is_empty());
    }

    #[test]
    fn test_evaluate_quiet_hours() {
        let mut manager = TaskSchedulerManager::new();
        let rule = ScheduleRule::new("r1", "Work", ScheduleType::QuietHours, 9, 0, 17, 0);
        manager.add_rule(rule);

        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 30, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(eval.should_pause);
        assert_eq!(eval.active_rule_ids, vec!["r1"]);
        assert!(eval.description.contains("Work"));
    }

    #[test]
    fn test_evaluate_priority() {
        let mut manager = TaskSchedulerManager::new();
        let rule1 =
            ScheduleRule::new("r1", "Low", ScheduleType::QuietHours, 9, 0, 17, 0).with_priority(1);
        let rule2 = ScheduleRule::new("r2", "High", ScheduleType::ActiveHours, 9, 0, 17, 0)
            .with_priority(10);
        manager.add_rule(rule1);
        manager.add_rule(rule2);

        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 30, 0).unwrap();
        let eval = manager.evaluate_at(time);
        // ActiveHours has higher priority, so should not pause.
        assert!(!eval.should_pause);
        assert_eq!(eval.active_rule_ids, vec!["r2", "r1"]);
    }

    #[test]
    fn test_evaluate_speed_limit() {
        let mut manager = TaskSchedulerManager::new();
        manager.config.low_bandwidth_speed_limit_bps = 1_000_000;
        let rule = ScheduleRule::new("r1", "Slow", ScheduleType::LowBandwidth, 9, 0, 17, 0);
        manager.add_rule(rule);

        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 30, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(!eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 1_000_000);
    }

    #[test]
    fn test_persistence() {
        let mut manager = TaskSchedulerManager::new();
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        manager.add_rule(rule);
        manager.config.high_priority_bypass_quiet = false;

        let temp_path = std::env::temp_dir().join("test_scheduler.json");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            manager.save_to_file(&temp_path).await.unwrap();

            let mut loaded = TaskSchedulerManager::new();
            loaded.load_from_file(&temp_path).await.unwrap();

            assert_eq!(loaded.get_rules().len(), 1);
            assert_eq!(loaded.get_rules()[0].id, "r1");
            assert!(!loaded.get_config().high_priority_bypass_quiet);
        });

        std::fs::remove_file(temp_path).ok();
    }

    #[test]
    fn test_remove_rule() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Test1",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        manager.add_rule(ScheduleRule::new(
            "r2",
            "Test2",
            ScheduleType::QuietHours,
            18,
            0,
            22,
            0,
        ));

        assert_eq!(manager.get_rules().len(), 2);
        assert!(manager.remove_rule("r1"));
        assert_eq!(manager.get_rules().len(), 1);
        assert_eq!(manager.get_rules()[0].id, "r2");
        assert!(!manager.remove_rule("nonexistent"));
    }

    #[test]
    fn test_set_rule_enabled() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Test",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));

        assert!(manager.set_rule_enabled("r1", false));
        assert!(!manager.get_rules()[0].enabled);

        assert!(manager.set_rule_enabled("r1", true));
        assert!(manager.get_rules()[0].enabled);

        assert!(!manager.set_rule_enabled("nonexistent", true));
    }

    #[test]
    fn test_disabled_scheduler() {
        let mut manager = TaskSchedulerManager::new();
        manager.config.enabled = false;
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Test",
            ScheduleType::QuietHours,
            0,
            0,
            24,
            0,
        ));

        let eval = manager.evaluate_now();
        assert!(!eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 0);
        assert!(eval.description.contains("disabled"));
    }
}
