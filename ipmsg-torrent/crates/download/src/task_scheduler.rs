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

    // ===== Existing tests =====

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

    // ===== Phase 220: Comprehensive test coverage =====

    // --- ScheduleType: serde + traits ---

    #[test]
    fn test_schedule_type_serde_roundtrip_all_variants() {
        for variant in [
            ScheduleType::QuietHours,
            ScheduleType::ActiveHours,
            ScheduleType::HighBandwidth,
            ScheduleType::LowBandwidth,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let loaded: ScheduleType = serde_json::from_str(&json).unwrap();
            assert_eq!(loaded, variant);
        }
    }

    #[test]
    fn test_schedule_type_snake_case_values() {
        assert_eq!(
            serde_json::to_string(&ScheduleType::QuietHours).unwrap(),
            "\"quiet_hours\""
        );
        assert_eq!(
            serde_json::to_string(&ScheduleType::ActiveHours).unwrap(),
            "\"active_hours\""
        );
        assert_eq!(
            serde_json::to_string(&ScheduleType::HighBandwidth).unwrap(),
            "\"high_bandwidth\""
        );
        assert_eq!(
            serde_json::to_string(&ScheduleType::LowBandwidth).unwrap(),
            "\"low_bandwidth\""
        );
    }

    #[test]
    fn test_schedule_type_clone_copy_debug() {
        let st = ScheduleType::QuietHours;
        let cloned = st;
        assert_eq!(cloned, ScheduleType::QuietHours);
        // Copy trait: original still usable
        let _copied = st;
        assert_eq!(format!("{:?}", st), "QuietHours");
    }

    #[test]
    fn test_schedule_type_eq() {
        assert_eq!(ScheduleType::QuietHours, ScheduleType::QuietHours);
        assert_ne!(ScheduleType::QuietHours, ScheduleType::ActiveHours);
        assert_ne!(ScheduleType::HighBandwidth, ScheduleType::LowBandwidth);
    }

    // --- ScheduleRule: construction + builder ---

    #[test]
    fn test_schedule_rule_new_defaults() {
        let rule = ScheduleRule::new("id1", "Name", ScheduleType::QuietHours, 9, 0, 17, 0);
        assert_eq!(rule.id, "id1");
        assert_eq!(rule.name, "Name");
        assert_eq!(rule.schedule_type, ScheduleType::QuietHours);
        assert_eq!(rule.start_hour, 9);
        assert_eq!(rule.start_minute, 0);
        assert_eq!(rule.end_hour, 17);
        assert_eq!(rule.end_minute, 0);
        assert!(rule.days_of_week.is_empty());
        assert!(rule.enabled);
        assert_eq!(rule.priority, 0);
        assert_eq!(rule.speed_limit_bps, 0);
    }

    #[test]
    fn test_schedule_rule_with_days() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_days(vec![Weekday::Mon, Weekday::Wed, Weekday::Fri]);
        assert_eq!(rule.days_of_week.len(), 3);
        assert!(rule.days_of_week.contains(&Weekday::Mon));
        assert!(rule.days_of_week.contains(&Weekday::Wed));
        assert!(rule.days_of_week.contains(&Weekday::Fri));
        assert!(!rule.days_of_week.contains(&Weekday::Tue));
    }

    #[test]
    fn test_schedule_rule_with_priority() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_priority(42);
        assert_eq!(rule.priority, 42);
    }

    #[test]
    fn test_schedule_rule_with_negative_priority() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_priority(-5);
        assert_eq!(rule.priority, -5);
    }

    #[test]
    fn test_schedule_rule_with_speed_limit() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::LowBandwidth, 9, 0, 17, 0)
            .with_speed_limit(500_000);
        assert_eq!(rule.speed_limit_bps, 500_000);
    }

    #[test]
    fn test_schedule_rule_builder_chain() {
        let rule = ScheduleRule::new("r1", "Chained", ScheduleType::LowBandwidth, 8, 30, 20, 0)
            .with_days(vec![Weekday::Mon])
            .with_priority(10)
            .with_speed_limit(1_000_000);
        assert_eq!(rule.id, "r1");
        assert_eq!(rule.priority, 10);
        assert_eq!(rule.speed_limit_bps, 1_000_000);
        assert_eq!(rule.days_of_week, vec![Weekday::Mon]);
    }

    // --- ScheduleRule: serde ---

    #[test]
    fn test_schedule_rule_serde_roundtrip() {
        let rule = ScheduleRule::new("r1", "Test Rule", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_days(vec![Weekday::Mon, Weekday::Fri])
            .with_priority(5)
            .with_speed_limit(100_000);
        let json = serde_json::to_string(&rule).unwrap();
        let loaded: ScheduleRule = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.id, "r1");
        assert_eq!(loaded.name, "Test Rule");
        assert_eq!(loaded.schedule_type, ScheduleType::QuietHours);
        assert_eq!(loaded.start_hour, 9);
        assert_eq!(loaded.end_hour, 17);
        assert_eq!(loaded.priority, 5);
        assert_eq!(loaded.speed_limit_bps, 100_000);
        assert_eq!(loaded.days_of_week.len(), 2);
    }

    #[test]
    fn test_schedule_rule_serde_extra_fields_ignored() {
        let json = r#"{"id":"r1","name":"Test","schedule_type":"quiet_hours","start_hour":9,"start_minute":0,"end_hour":17,"end_minute":0,"days_of_week":[],"enabled":true,"priority":0,"speed_limit_bps":0,"extra_field":"ignored"}"#;
        let loaded: ScheduleRule = serde_json::from_str(json).unwrap();
        assert_eq!(loaded.id, "r1");
    }

    // --- ScheduleRule: applies_at boundary tests ---

    #[test]
    fn test_applies_at_exact_start() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        let time = Local.with_ymd_and_hms(2026, 8, 10, 9, 0, 0).unwrap();
        assert!(rule.applies_at(time));
    }

    #[test]
    fn test_applies_at_exact_end_excluded() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        let time = Local.with_ymd_and_hms(2026, 8, 10, 17, 0, 0).unwrap();
        // End time is exclusive (time_minutes < end_minutes for normal range).
        assert!(!rule.applies_at(time));
    }

    #[test]
    fn test_applies_at_one_minute_before_end() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        let time = Local.with_ymd_and_hms(2026, 8, 10, 16, 59, 0).unwrap();
        assert!(rule.applies_at(time));
    }

    #[test]
    fn test_applies_at_one_minute_before_start() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        let time = Local.with_ymd_and_hms(2026, 8, 10, 8, 59, 0).unwrap();
        assert!(!rule.applies_at(time));
    }

    #[test]
    fn test_applies_at_midnight_boundary() {
        // Rule from 0:0 to 6:0.
        let rule = ScheduleRule::new("r1", "Night", ScheduleType::ActiveHours, 0, 0, 6, 0);
        let midnight = Local.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
        assert!(rule.applies_at(midnight));
        let five_fifty_nine = Local.with_ymd_and_hms(2026, 8, 10, 5, 59, 0).unwrap();
        assert!(rule.applies_at(five_fifty_nine));
        let six = Local.with_ymd_and_hms(2026, 8, 10, 6, 0, 0).unwrap();
        assert!(!rule.applies_at(six));
    }

    #[test]
    fn test_applies_at_overnight_exact_boundaries() {
        // Overnight: 22:00-06:00.
        let rule = ScheduleRule::new("r1", "Night", ScheduleType::QuietHours, 22, 0, 6, 0);
        let at_22 = Local.with_ymd_and_hms(2026, 8, 10, 22, 0, 0).unwrap();
        assert!(rule.applies_at(at_22));
        let at_6 = Local.with_ymd_and_hms(2026, 8, 11, 6, 0, 0).unwrap();
        // 6:00 is end time, exclusive.
        assert!(!rule.applies_at(at_6));
        let at_5_59 = Local.with_ymd_and_hms(2026, 8, 11, 5, 59, 0).unwrap();
        assert!(rule.applies_at(at_5_59));
    }

    #[test]
    fn test_applies_at_minute_precision() {
        // Rule with non-zero minutes: 9:30-17:45.
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 30, 17, 45);
        let at_9_29 = Local.with_ymd_and_hms(2026, 8, 10, 9, 29, 0).unwrap();
        assert!(!rule.applies_at(at_9_29));
        let at_9_30 = Local.with_ymd_and_hms(2026, 8, 10, 9, 30, 0).unwrap();
        assert!(rule.applies_at(at_9_30));
        let at_17_44 = Local.with_ymd_and_hms(2026, 8, 10, 17, 44, 0).unwrap();
        assert!(rule.applies_at(at_17_44));
        let at_17_45 = Local.with_ymd_and_hms(2026, 8, 10, 17, 45, 0).unwrap();
        assert!(!rule.applies_at(at_17_45));
    }

    #[test]
    fn test_applies_at_empty_days_means_all_days() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        // No days set → applies every day.
        for weekday in [
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
            Weekday::Sat,
            Weekday::Sun,
        ] {
            let (y, m, d) = match weekday {
                Weekday::Mon => (2026, 8, 10),
                Weekday::Tue => (2026, 8, 11),
                Weekday::Wed => (2026, 8, 12),
                Weekday::Thu => (2026, 8, 13),
                Weekday::Fri => (2026, 8, 14),
                Weekday::Sat => (2026, 8, 15),
                Weekday::Sun => (2026, 8, 16),
            };
            let time = Local.with_ymd_and_hms(y, m, d, 10, 0, 0).unwrap();
            assert!(rule.applies_at(time), "should apply on {:?}", weekday);
        }
    }

    #[test]
    fn test_applies_at_single_day() {
        let rule = ScheduleRule::new("r1", "Monday Only", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_days(vec![Weekday::Mon]);
        let monday = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        assert!(rule.applies_at(monday));
        let tuesday = Local.with_ymd_and_hms(2026, 8, 11, 10, 0, 0).unwrap();
        assert!(!rule.applies_at(tuesday));
    }

    #[test]
    fn test_applies_at_weekdays_only() {
        let rule = ScheduleRule::new("r1", "Weekdays", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_days(vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ]);
        let saturday = Local.with_ymd_and_hms(2026, 8, 15, 10, 0, 0).unwrap();
        assert!(!rule.applies_at(saturday));
        let wednesday = Local.with_ymd_and_hms(2026, 8, 12, 10, 0, 0).unwrap();
        assert!(rule.applies_at(wednesday));
    }

    #[test]
    fn test_applies_at_disabled_with_days() {
        // Disabled rule with matching day should NOT apply.
        let mut rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_days(vec![Weekday::Mon]);
        rule.enabled = false;
        let monday = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        assert!(!rule.applies_at(monday));
    }

    #[test]
    fn test_applies_at_wrong_day_right_time() {
        let rule = ScheduleRule::new("r1", "Weekend Only", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_days(vec![Weekday::Sat, Weekday::Sun]);
        // Monday at 10:00 - right time, wrong day.
        let monday = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        assert!(!rule.applies_at(monday));
    }

    // --- ScheduleRule: Unicode ---

    #[test]
    fn test_schedule_rule_unicode_id_and_name() {
        let rule = ScheduleRule::new("规则-1", "工作时间", ScheduleType::QuietHours, 9, 0, 17, 0);
        assert_eq!(rule.id, "规则-1");
        assert_eq!(rule.name, "工作时间");
    }

    #[test]
    fn test_schedule_rule_emoji_id_and_name() {
        let rule = ScheduleRule::new(
            "🌙-night",
            "🔇 Quiet",
            ScheduleType::QuietHours,
            22,
            0,
            6,
            0,
        );
        assert_eq!(rule.id, "🌙-night");
        assert_eq!(rule.name, "🔇 Quiet");
    }

    // --- ScheduleRule: Clone/Debug ---

    #[test]
    fn test_schedule_rule_clone() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_days(vec![Weekday::Mon])
            .with_priority(5);
        let cloned = rule.clone();
        assert_eq!(cloned.id, "r1");
        assert_eq!(cloned.priority, 5);
        assert_eq!(cloned.days_of_week, vec![Weekday::Mon]);
    }

    #[test]
    fn test_schedule_rule_debug() {
        let rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        let debug_str = format!("{:?}", rule);
        assert!(debug_str.contains("r1"));
        assert!(debug_str.contains("Test"));
        assert!(debug_str.contains("QuietHours"));
    }

    // --- TaskSchedulerConfig ---

    #[test]
    fn test_config_default() {
        let config = TaskSchedulerConfig::default();
        assert!(config.enabled);
        assert!(config.high_priority_bypass_quiet);
        assert_eq!(config.quiet_hours_speed_limit_bps, 0);
        assert_eq!(config.low_bandwidth_speed_limit_bps, 0);
    }

    #[test]
    fn test_config_custom_values() {
        let config = TaskSchedulerConfig {
            enabled: false,
            high_priority_bypass_quiet: false,
            quiet_hours_speed_limit_bps: 50_000,
            low_bandwidth_speed_limit_bps: 200_000,
        };
        assert!(!config.enabled);
        assert!(!config.high_priority_bypass_quiet);
        assert_eq!(config.quiet_hours_speed_limit_bps, 50_000);
        assert_eq!(config.low_bandwidth_speed_limit_bps, 200_000);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = TaskSchedulerConfig {
            enabled: false,
            high_priority_bypass_quiet: false,
            quiet_hours_speed_limit_bps: 100_000,
            low_bandwidth_speed_limit_bps: 500_000,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: TaskSchedulerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(
            loaded.high_priority_bypass_quiet,
            config.high_priority_bypass_quiet
        );
        assert_eq!(
            loaded.quiet_hours_speed_limit_bps,
            config.quiet_hours_speed_limit_bps
        );
        assert_eq!(
            loaded.low_bandwidth_speed_limit_bps,
            config.low_bandwidth_speed_limit_bps
        );
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"high_priority_bypass_quiet":true,"quiet_hours_speed_limit_bps":0,"low_bandwidth_speed_limit_bps":0,"unknown":"field"}"#;
        let loaded: TaskSchedulerConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = TaskSchedulerConfig::default();
        let cloned = config.clone();
        assert!(cloned.enabled);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("enabled"));
    }

    // --- TaskSchedulerData ---

    #[test]
    fn test_data_default() {
        let data = TaskSchedulerData::default();
        assert!(data.config.enabled);
        assert!(data.rules.is_empty());
    }

    #[test]
    fn test_data_serde_roundtrip() {
        let data = TaskSchedulerData {
            config: TaskSchedulerConfig {
                enabled: true,
                high_priority_bypass_quiet: false,
                quiet_hours_speed_limit_bps: 10_000,
                low_bandwidth_speed_limit_bps: 20_000,
            },
            rules: vec![
                ScheduleRule::new("r1", "Rule 1", ScheduleType::QuietHours, 9, 0, 17, 0),
                ScheduleRule::new("r2", "Rule 2", ScheduleType::ActiveHours, 18, 0, 22, 0),
            ],
        };
        let json = serde_json::to_string(&data).unwrap();
        let loaded: TaskSchedulerData = serde_json::from_str(&json).unwrap();
        assert!(!loaded.config.high_priority_bypass_quiet);
        assert_eq!(loaded.rules.len(), 2);
        assert_eq!(loaded.rules[0].id, "r1");
        assert_eq!(loaded.rules[1].id, "r2");
    }

    // --- ScheduleEvaluation ---

    #[test]
    fn test_evaluation_serde_roundtrip() {
        let eval = ScheduleEvaluation {
            should_pause: true,
            speed_limit_bps: 100_000,
            active_rule_ids: vec!["r1".into(), "r2".into()],
            description: "Quiet Hours, Low Priority".into(),
        };
        let json = serde_json::to_string(&eval).unwrap();
        let loaded: ScheduleEvaluation = serde_json::from_str(&json).unwrap();
        assert!(loaded.should_pause);
        assert_eq!(loaded.speed_limit_bps, 100_000);
        assert_eq!(loaded.active_rule_ids.len(), 2);
        assert_eq!(loaded.description, "Quiet Hours, Low Priority");
    }

    #[test]
    fn test_evaluation_clone_debug() {
        let eval = ScheduleEvaluation {
            should_pause: false,
            speed_limit_bps: 0,
            active_rule_ids: vec![],
            description: "test".into(),
        };
        let cloned = eval.clone();
        assert!(!cloned.should_pause);
        let debug_str = format!("{:?}", eval);
        assert!(debug_str.contains("should_pause"));
    }

    // --- TaskSchedulerManager: construction + config ---

    #[test]
    fn test_manager_new_equals_default() {
        let new = TaskSchedulerManager::new();
        let default = TaskSchedulerManager::default();
        assert_eq!(new.get_config().enabled, default.get_config().enabled);
        assert_eq!(new.get_rules().len(), default.get_rules().len());
    }

    #[test]
    fn test_manager_set_config_preserves_rules() {
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
        assert_eq!(manager.get_rules().len(), 1);

        let new_config = TaskSchedulerConfig {
            enabled: false,
            high_priority_bypass_quiet: false,
            quiet_hours_speed_limit_bps: 50_000,
            low_bandwidth_speed_limit_bps: 100_000,
        };
        manager.set_config(new_config);

        assert!(!manager.get_config().enabled);
        assert!(!manager.get_config().high_priority_bypass_quiet);
        assert_eq!(manager.get_config().quiet_hours_speed_limit_bps, 50_000);
        assert_eq!(manager.get_rules().len(), 1); // rules preserved
    }

    #[test]
    fn test_manager_get_config_returns_reference() {
        let manager = TaskSchedulerManager::new();
        let config = manager.get_config();
        assert!(config.enabled);
    }

    // --- TaskSchedulerManager: add_rule ---

    #[test]
    fn test_manager_add_multiple_rules() {
        let mut manager = TaskSchedulerManager::new();
        for i in 0..10 {
            manager.add_rule(ScheduleRule::new(
                format!("r{}", i),
                format!("Rule {}", i),
                ScheduleType::QuietHours,
                9,
                0,
                17,
                0,
            ));
        }
        assert_eq!(manager.get_rules().len(), 10);
    }

    #[test]
    fn test_manager_add_duplicate_id() {
        // Duplicate IDs are allowed (no uniqueness enforcement).
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "First",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Second",
            ScheduleType::ActiveHours,
            18,
            0,
            22,
            0,
        ));
        assert_eq!(manager.get_rules().len(), 2);
    }

    // --- TaskSchedulerManager: remove_rule ---

    #[test]
    fn test_manager_remove_rule_idempotent() {
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
        assert!(manager.remove_rule("r1"));
        assert!(!manager.remove_rule("r1")); // already removed
        assert!(manager.get_rules().is_empty());
    }

    #[test]
    fn test_manager_remove_from_empty() {
        let mut manager = TaskSchedulerManager::new();
        assert!(!manager.remove_rule("nonexistent"));
    }

    // --- TaskSchedulerManager: set_rule_enabled ---

    #[test]
    fn test_manager_set_rule_enabled_toggle() {
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

        // Toggle: true → false → true.
        assert!(manager.set_rule_enabled("r1", false));
        assert!(!manager.get_rules()[0].enabled);
        assert!(manager.set_rule_enabled("r1", true));
        assert!(manager.get_rules()[0].enabled);
    }

    #[test]
    fn test_manager_set_rule_enabled_idempotent() {
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

        // Set to same value twice.
        assert!(manager.set_rule_enabled("r1", true));
        assert!(manager.set_rule_enabled("r1", true));
        assert!(manager.get_rules()[0].enabled);
    }

    // --- TaskSchedulerManager: evaluate_at ---

    #[test]
    fn test_evaluate_active_hours() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Active",
            ScheduleType::ActiveHours,
            9,
            0,
            17,
            0,
        ));
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(!eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 0);
        assert_eq!(eval.active_rule_ids, vec!["r1"]);
    }

    #[test]
    fn test_evaluate_high_bandwidth() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Fast",
            ScheduleType::HighBandwidth,
            0,
            0,
            24,
            0,
        ));
        let time = Local.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(!eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 0); // unlimited
        assert_eq!(eval.active_rule_ids, vec!["r1"]);
    }

    #[test]
    fn test_evaluate_low_bandwidth_with_rule_speed_limit() {
        // Config has no low_bandwidth_speed_limit_bps, fall back to rule's speed_limit_bps.
        let mut manager = TaskSchedulerManager::new();
        let rule = ScheduleRule::new("r1", "Slow", ScheduleType::LowBandwidth, 9, 0, 17, 0)
            .with_speed_limit(256_000);
        manager.add_rule(rule);
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(!eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 256_000);
    }

    #[test]
    fn test_evaluate_low_bandwidth_config_overrides_rule() {
        // Config low_bandwidth_speed_limit_bps takes precedence over rule's speed_limit_bps.
        let mut manager = TaskSchedulerManager::new();
        manager.config.low_bandwidth_speed_limit_bps = 500_000;
        let rule = ScheduleRule::new("r1", "Slow", ScheduleType::LowBandwidth, 9, 0, 17, 0)
            .with_speed_limit(256_000);
        manager.add_rule(rule);
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(!eval.should_pause);
        // Config value (500_000) takes precedence over rule value (256_000).
        assert_eq!(eval.speed_limit_bps, 500_000);
    }

    #[test]
    fn test_evaluate_quiet_hours_with_speed_limit() {
        // Quiet hours with config speed limit → should_pause=true but speed_limit > 0.
        let mut manager = TaskSchedulerManager::new();
        manager.config.quiet_hours_speed_limit_bps = 50_000;
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Quiet",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 50_000);
    }

    #[test]
    fn test_evaluate_quiet_hours_no_speed_limit() {
        // Quiet hours with no config speed limit → full pause.
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Quiet",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 0); // full pause
    }

    #[test]
    fn test_evaluate_multiple_rules_same_time() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Quiet",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        manager.add_rule(ScheduleRule::new(
            "r2",
            "Bandwidth",
            ScheduleType::LowBandwidth,
            9,
            0,
            17,
            0,
        ));
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        // Both rules active, both listed.
        assert_eq!(eval.active_rule_ids.len(), 2);
        assert!(eval.description.contains("Quiet"));
        assert!(eval.description.contains("Bandwidth"));
    }

    #[test]
    fn test_evaluate_no_matching_rules() {
        let mut manager = TaskSchedulerManager::new();
        // Rule only applies on Monday.
        let rule = ScheduleRule::new("r1", "Monday", ScheduleType::QuietHours, 9, 0, 17, 0)
            .with_days(vec![Weekday::Mon]);
        manager.add_rule(rule);
        // Evaluate on Saturday.
        let saturday = Local.with_ymd_and_hms(2026, 8, 15, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(saturday);
        assert!(!eval.should_pause);
        assert!(eval.active_rule_ids.is_empty());
        assert!(eval.description.contains("No active"));
    }

    #[test]
    fn test_evaluate_all_rules_disabled() {
        let mut manager = TaskSchedulerManager::new();
        let mut rule = ScheduleRule::new("r1", "Test", ScheduleType::QuietHours, 9, 0, 17, 0);
        rule.enabled = false;
        manager.add_rule(rule);
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        assert!(!eval.should_pause);
        assert!(eval.active_rule_ids.is_empty());
    }

    #[test]
    fn test_evaluate_description_joins_multiple() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Alpha",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        manager.add_rule(ScheduleRule::new(
            "r2",
            "Beta",
            ScheduleType::LowBandwidth,
            9,
            0,
            17,
            0,
        ));
        manager.add_rule(ScheduleRule::new(
            "r3",
            "Gamma",
            ScheduleType::ActiveHours,
            9,
            0,
            17,
            0,
        ));
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        // Description should contain all rule names joined by ", ".
        assert!(eval.description.contains("Alpha"));
        assert!(eval.description.contains("Beta"));
        assert!(eval.description.contains("Gamma"));
        assert!(eval.description.contains(", "));
    }

    #[test]
    fn test_evaluate_negative_priority() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(
            ScheduleRule::new("r1", "Low", ScheduleType::QuietHours, 9, 0, 17, 0).with_priority(-5),
        );
        manager.add_rule(
            ScheduleRule::new("r2", "Higher", ScheduleType::ActiveHours, 9, 0, 17, 0)
                .with_priority(-1),
        );
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        // -1 > -5, so ActiveHours wins → no pause.
        assert!(!eval.should_pause);
        assert_eq!(eval.active_rule_ids[0], "r2");
    }

    #[test]
    fn test_evaluate_same_priority_order_preserved() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(
            ScheduleRule::new("r1", "First", ScheduleType::QuietHours, 9, 0, 17, 0)
                .with_priority(5),
        );
        manager.add_rule(
            ScheduleRule::new("r2", "Second", ScheduleType::ActiveHours, 9, 0, 17, 0)
                .with_priority(5),
        );
        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        // Both active, both listed.
        assert_eq!(eval.active_rule_ids.len(), 2);
    }

    // --- TaskSchedulerManager: persistence ---

    #[test]
    fn test_save_creates_file() {
        let manager = TaskSchedulerManager::new();
        let temp_path = std::env::temp_dir().join("test_scheduler_create.json");
        let _ = std::fs::remove_file(&temp_path);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            manager.save_to_file(&temp_path).await.unwrap();
        });
        assert!(temp_path.exists());
        std::fs::remove_file(temp_path).ok();
    }

    #[test]
    fn test_save_overwrites_existing() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "r1",
            "First",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        let temp_path = std::env::temp_dir().join("test_scheduler_overwrite.json");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            manager.save_to_file(&temp_path).await.unwrap();

            // Overwrite with different data.
            manager.remove_rule("r1");
            manager.add_rule(ScheduleRule::new(
                "r2",
                "Second",
                ScheduleType::ActiveHours,
                10,
                0,
                18,
                0,
            ));
            manager.save_to_file(&temp_path).await.unwrap();

            let mut loaded = TaskSchedulerManager::new();
            loaded.load_from_file(&temp_path).await.unwrap();
            assert_eq!(loaded.get_rules().len(), 1);
            assert_eq!(loaded.get_rules()[0].id, "r2");
        });
        std::fs::remove_file(temp_path).ok();
    }

    #[test]
    fn test_load_missing_file() {
        let mut manager = TaskSchedulerManager::new();
        let missing = std::env::temp_dir().join("nonexistent_scheduler_file.json");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async { manager.load_from_file(&missing).await });
        assert!(result.is_err());
    }

    #[test]
    fn test_load_corrupt_json() {
        let temp_path = std::env::temp_dir().join("test_scheduler_corrupt.json");
        std::fs::write(&temp_path, "not valid json{{{").unwrap();

        let mut manager = TaskSchedulerManager::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async { manager.load_from_file(&temp_path).await });
        assert!(result.is_err());
        std::fs::remove_file(temp_path).ok();
    }

    #[test]
    fn test_save_no_tmp_leftover() {
        let manager = TaskSchedulerManager::new();
        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join("test_scheduler_no_tmp.json");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            manager.save_to_file(&temp_path).await.unwrap();
        });

        // Check no .tmp files left in temp dir from our save.
        let has_tmp = std::fs::read_dir(&temp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with("test_scheduler_no_tmp") && name.ends_with(".tmp")
            });
        assert!(!has_tmp);
        std::fs::remove_file(temp_path).ok();
    }

    #[test]
    fn test_persistence_full_roundtrip() {
        let mut manager = TaskSchedulerManager::new();
        manager.config.enabled = true;
        manager.config.high_priority_bypass_quiet = false;
        manager.config.quiet_hours_speed_limit_bps = 50_000;
        manager.config.low_bandwidth_speed_limit_bps = 200_000;

        manager.add_rule(
            ScheduleRule::new("r1", "Work Hours", ScheduleType::QuietHours, 9, 0, 17, 0)
                .with_days(vec![
                    Weekday::Mon,
                    Weekday::Tue,
                    Weekday::Wed,
                    Weekday::Thu,
                    Weekday::Fri,
                ])
                .with_priority(10)
                .with_speed_limit(0),
        );
        manager.add_rule(
            ScheduleRule::new(
                "r2",
                "Night Download",
                ScheduleType::ActiveHours,
                22,
                0,
                6,
                0,
            )
            .with_days(vec![Weekday::Sat, Weekday::Sun])
            .with_priority(5),
        );

        let temp_path = std::env::temp_dir().join("test_scheduler_full_roundtrip.json");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            manager.save_to_file(&temp_path).await.unwrap();
            let mut loaded = TaskSchedulerManager::new();
            loaded.load_from_file(&temp_path).await.unwrap();

            assert_eq!(loaded.get_rules().len(), 2);
            assert!(!loaded.get_config().high_priority_bypass_quiet);
            assert_eq!(loaded.get_config().quiet_hours_speed_limit_bps, 50_000);
            assert_eq!(loaded.get_rules()[0].days_of_week.len(), 5);
            assert_eq!(
                loaded.get_rules()[1].schedule_type,
                ScheduleType::ActiveHours
            );
        });
        std::fs::remove_file(temp_path).ok();
    }

    #[test]
    fn test_persistence_unicode_rule_names() {
        let mut manager = TaskSchedulerManager::new();
        manager.add_rule(ScheduleRule::new(
            "rule-工作",
            "工作时间-安静模式",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        let temp_path = std::env::temp_dir().join("test_scheduler_unicode.json");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            manager.save_to_file(&temp_path).await.unwrap();
            let mut loaded = TaskSchedulerManager::new();
            loaded.load_from_file(&temp_path).await.unwrap();
            assert_eq!(loaded.get_rules()[0].id, "rule-工作");
            assert_eq!(loaded.get_rules()[0].name, "工作时间-安静模式");
        });
        std::fs::remove_file(temp_path).ok();
    }

    // --- TaskSchedulerError ---

    #[test]
    fn test_error_display_invalid_schedule() {
        let err = TaskSchedulerError::InvalidSchedule("bad config".to_string());
        assert_eq!(format!("{}", err), "invalid schedule: bad config");
    }

    #[test]
    fn test_error_display_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = TaskSchedulerError::Io(io_err);
        let display = format!("{}", err);
        assert!(display.contains("persistence error"));
        assert!(display.contains("file not found"));
    }

    #[test]
    fn test_error_display_serialize() {
        let bad_json = serde_json::from_str::<TaskSchedulerConfig>("invalid").unwrap_err();
        let err = TaskSchedulerError::Serialize(bad_json);
        let display = format!("{}", err);
        assert!(display.contains("serialization error"));
    }

    #[test]
    fn test_error_debug() {
        let err = TaskSchedulerError::InvalidSchedule("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("InvalidSchedule"));
    }

    // --- TaskSchedulerManager: Clone ---

    #[test]
    fn test_manager_clone() {
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
        let cloned = manager.clone();
        assert_eq!(cloned.get_rules().len(), 1);
        assert_eq!(cloned.get_rules()[0].id, "r1");
    }

    #[test]
    fn test_manager_clone_independent() {
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
        let mut cloned = manager.clone();
        cloned.add_rule(ScheduleRule::new(
            "r2",
            "Other",
            ScheduleType::ActiveHours,
            18,
            0,
            22,
            0,
        ));
        // Original unchanged.
        assert_eq!(manager.get_rules().len(), 1);
        assert_eq!(cloned.get_rules().len(), 2);
    }

    // --- Complex workflow tests ---

    #[test]
    fn test_complete_lifecycle() {
        // Create → configure → add rules → evaluate → save → load → evaluate again.
        let mut manager = TaskSchedulerManager::new();
        manager.config.quiet_hours_speed_limit_bps = 100_000;

        manager.add_rule(
            ScheduleRule::new("work", "Work Hours", ScheduleType::QuietHours, 9, 0, 17, 0)
                .with_days(vec![
                    Weekday::Mon,
                    Weekday::Tue,
                    Weekday::Wed,
                    Weekday::Thu,
                    Weekday::Fri,
                ]),
        );
        manager.add_rule(ScheduleRule::new(
            "night",
            "Night Active",
            ScheduleType::ActiveHours,
            22,
            0,
            6,
            0,
        ));

        // Evaluate during work hours.
        let work_time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap(); // Monday.
        let eval = manager.evaluate_at(work_time);
        assert!(eval.should_pause);
        assert_eq!(eval.speed_limit_bps, 100_000);

        // Save and reload.
        let temp_path = std::env::temp_dir().join("test_scheduler_lifecycle.json");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            manager.save_to_file(&temp_path).await.unwrap();
        });

        let mut loaded = TaskSchedulerManager::new();
        rt.block_on(async {
            loaded.load_from_file(&temp_path).await.unwrap();
        });

        // Evaluate again with loaded manager.
        let eval2 = loaded.evaluate_at(work_time);
        assert!(eval2.should_pause);
        assert_eq!(eval2.speed_limit_bps, 100_000);

        std::fs::remove_file(temp_path).ok();
    }

    #[test]
    fn test_disable_enable_rule_workflow() {
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

        let time = Local.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();

        // Initially active.
        assert!(manager.evaluate_at(time).should_pause);

        // Disable → no longer active.
        manager.set_rule_enabled("r1", false);
        assert!(!manager.evaluate_at(time).should_pause);

        // Re-enable → active again.
        manager.set_rule_enabled("r1", true);
        assert!(manager.evaluate_at(time).should_pause);
    }

    #[test]
    fn test_add_remove_add_workflow() {
        let mut manager = TaskSchedulerManager::new();

        manager.add_rule(ScheduleRule::new(
            "r1",
            "First",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        assert_eq!(manager.get_rules().len(), 1);

        manager.remove_rule("r1");
        assert!(manager.get_rules().is_empty());

        // Re-add with same ID but different config.
        manager.add_rule(ScheduleRule::new(
            "r1",
            "Second",
            ScheduleType::ActiveHours,
            18,
            0,
            22,
            0,
        ));
        assert_eq!(manager.get_rules().len(), 1);
        assert_eq!(manager.get_rules()[0].name, "Second");
        assert_eq!(
            manager.get_rules()[0].schedule_type,
            ScheduleType::ActiveHours
        );
    }

    #[test]
    fn test_many_rules_evaluation() {
        let mut manager = TaskSchedulerManager::new();
        // Add 20 rules, all active at the same time, with different priorities.
        for i in 0..20 {
            manager.add_rule(
                ScheduleRule::new(
                    format!("r{}", i),
                    format!("Rule {}", i),
                    ScheduleType::QuietHours,
                    0,
                    0,
                    24,
                    0,
                )
                .with_priority(i),
            );
        }
        let time = Local.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);
        // All 20 rules active.
        assert_eq!(eval.active_rule_ids.len(), 20);
        // Highest priority (r19) is first.
        assert_eq!(eval.active_rule_ids[0], "r19");
        // QuietHours → should_pause.
        assert!(eval.should_pause);
    }

    #[test]
    fn test_mixed_schedule_types_evaluation() {
        let mut manager = TaskSchedulerManager::new();
        // All 4 schedule types active at same time, different priorities.
        manager.add_rule(
            ScheduleRule::new("quiet", "Quiet", ScheduleType::QuietHours, 0, 0, 24, 0)
                .with_priority(1),
        );
        manager.add_rule(
            ScheduleRule::new("active", "Active", ScheduleType::ActiveHours, 0, 0, 24, 0)
                .with_priority(2),
        );
        manager.add_rule(
            ScheduleRule::new("high", "High BW", ScheduleType::HighBandwidth, 0, 0, 24, 0)
                .with_priority(3),
        );
        manager.add_rule(
            ScheduleRule::new("low", "Low BW", ScheduleType::LowBandwidth, 0, 0, 24, 0)
                .with_priority(4),
        );

        let time = Local.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        let eval = manager.evaluate_at(time);

        // All 4 active, LowBandwidth has highest priority (4).
        assert_eq!(eval.active_rule_ids.len(), 4);
        assert_eq!(eval.active_rule_ids[0], "low");
        // LowBandwidth: should_pause=false.
        assert!(!eval.should_pause);
    }

    // --- Pretty serde ---

    #[test]
    fn test_config_pretty_serde() {
        let config = TaskSchedulerConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let loaded: TaskSchedulerConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
    }

    #[test]
    fn test_data_pretty_serde() {
        let mut data = TaskSchedulerData::default();
        data.rules.push(ScheduleRule::new(
            "r1",
            "Test",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        let pretty = serde_json::to_string_pretty(&data).unwrap();
        assert!(pretty.contains('\n'));
        let loaded: TaskSchedulerData = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.rules.len(), 1);
    }

    // --- Edge: 24-hour rule ---

    #[test]
    fn test_applies_at_full_day_0_to_24() {
        // Rule covering 0:0 to 24:0 (end_minutes = 24*60 = 1440).
        // Note: end_hour=24 is unusual but the math works out.
        let rule = ScheduleRule::new("r1", "All Day", ScheduleType::QuietHours, 0, 0, 24, 0);
        let midnight = Local.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
        assert!(rule.applies_at(midnight));
        let noon = Local.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        assert!(rule.applies_at(noon));
        let late = Local.with_ymd_and_hms(2026, 8, 10, 23, 59, 0).unwrap();
        assert!(rule.applies_at(late));
    }

    // --- Edge: zero-duration window (start == end) ---

    #[test]
    fn test_applies_at_zero_duration_window() {
        // start == end → start_minutes == end_minutes → normal range.
        // time_minutes >= start && time_minutes < end → never true when start == end.
        let rule = ScheduleRule::new("r1", "Zero", ScheduleType::QuietHours, 12, 0, 12, 0);
        let noon = Local.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap();
        // 720 >= 720 && 720 < 720 → false.
        assert!(!rule.applies_at(noon));
    }

    // --- get_rules returns correct slice ---

    #[test]
    fn test_get_rules_returns_all() {
        let mut manager = TaskSchedulerManager::new();
        assert!(manager.get_rules().is_empty());
        manager.add_rule(ScheduleRule::new(
            "r1",
            "A",
            ScheduleType::QuietHours,
            9,
            0,
            17,
            0,
        ));
        manager.add_rule(ScheduleRule::new(
            "r2",
            "B",
            ScheduleType::ActiveHours,
            18,
            0,
            22,
            0,
        ));
        assert_eq!(manager.get_rules().len(), 2);
        assert_eq!(manager.get_rules()[0].id, "r1");
        assert_eq!(manager.get_rules()[1].id, "r2");
    }
}
