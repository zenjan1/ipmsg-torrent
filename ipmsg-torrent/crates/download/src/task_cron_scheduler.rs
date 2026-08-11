//! Per-task cron-based scheduler
//!
//! Allows tasks to be scheduled using cron expressions (e.g., "0 2 * * *" for daily at 2 AM).
//! Tasks are automatically started when the cron schedule triggers and optionally stopped after a duration.

use chrono::{DateTime, Datelike, Local, Timelike};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Error type for task cron scheduler operations.
#[derive(Debug, thiserror::Error)]
pub enum TaskCronSchedulerError {
    #[error("task {0} has no cron schedule")]
    NoSchedule(String),
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),
    #[error("persistence error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Cron schedule configuration for a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCronSchedule {
    /// Unique schedule identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Cron expression (minute hour day_of_month month day_of_week).
    pub cron_expr: String,
    /// Optional duration in seconds after which to stop the task (None = run indefinitely).
    pub duration_secs: Option<u64>,
    /// Whether this schedule is enabled.
    pub enabled: bool,
    /// Last trigger time.
    pub last_trigger: Option<DateTime<Local>>,
    /// Next scheduled trigger time.
    pub next_trigger: Option<DateTime<Local>>,
}

impl TaskCronSchedule {
    /// Create a new cron schedule.
    pub fn new(
        id: String,
        name: String,
        cron_expr: String,
    ) -> Result<Self, TaskCronSchedulerError> {
        // Validate cron expression (basic validation)
        let parts: Vec<&str> = cron_expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(TaskCronSchedulerError::InvalidCron(
                "cron expression must have 5 fields: minute hour day_of_month month day_of_week"
                    .to_string(),
            ));
        }

        let mut schedule = Self {
            id,
            name,
            cron_expr,
            duration_secs: None,
            enabled: true,
            last_trigger: None,
            next_trigger: None,
        };
        schedule.update_next_trigger()?;
        Ok(schedule)
    }

    /// Set the duration after which to stop the task.
    pub fn with_duration(mut self, duration_secs: u64) -> Self {
        self.duration_secs = Some(duration_secs);
        self
    }

    /// Enable or disable the schedule.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Update the next trigger time based on the cron expression.
    pub fn update_next_trigger(&mut self) -> Result<(), TaskCronSchedulerError> {
        self.next_trigger = self.compute_next_trigger()?;
        Ok(())
    }

    /// Compute the next trigger time from now.
    fn compute_next_trigger(&self) -> Result<Option<DateTime<Local>>, TaskCronSchedulerError> {
        let parts: Vec<&str> = self.cron_expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(TaskCronSchedulerError::InvalidCron(
                "invalid cron expression".to_string(),
            ));
        }

        let minute = parse_cron_field(parts[0], 0, 59)?;
        let hour = parse_cron_field(parts[1], 0, 23)?;
        let day_of_month = parse_cron_field(parts[2], 1, 31)?;
        let month = parse_cron_field(parts[3], 1, 12)?;
        let day_of_week = parse_cron_field(parts[4], 0, 6)?;

        let now = Local::now();
        let mut candidate = now + chrono::Duration::minutes(1);
        candidate = candidate
            .with_second(0)
            .ok_or_else(|| TaskCronSchedulerError::InvalidCron("invalid time".to_string()))?;

        // Search for the next matching time (up to 1 year ahead)
        for _ in 0..525_600 {
            // 1 year in minutes
            if matches_field(candidate.minute() as i32, &minute)
                && matches_field(candidate.hour() as i32, &hour)
                && matches_field(candidate.day() as i32, &day_of_month)
                && matches_field(candidate.month() as i32, &month)
                && matches_field(
                    candidate.weekday().num_days_from_sunday() as i32,
                    &day_of_week,
                )
            {
                return Ok(Some(candidate));
            }
            candidate += chrono::Duration::minutes(1);
        }

        Ok(None) // No match found within 1 year
    }

    /// Check if the schedule should trigger at the given time.
    pub fn should_trigger_at(&self, time: DateTime<Local>) -> Result<bool, TaskCronSchedulerError> {
        if !self.enabled {
            return Ok(false);
        }

        let parts: Vec<&str> = self.cron_expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(TaskCronSchedulerError::InvalidCron(
                "invalid cron expression".to_string(),
            ));
        }

        let minute = parse_cron_field(parts[0], 0, 59)?;
        let hour = parse_cron_field(parts[1], 0, 23)?;
        let day_of_month = parse_cron_field(parts[2], 1, 31)?;
        let month = parse_cron_field(parts[3], 1, 12)?;
        let day_of_week = parse_cron_field(parts[4], 0, 6)?;

        Ok(matches_field(time.minute() as i32, &minute)
            && matches_field(time.hour() as i32, &hour)
            && matches_field(time.day() as i32, &day_of_month)
            && matches_field(time.month() as i32, &month)
            && matches_field(time.weekday().num_days_from_sunday() as i32, &day_of_week))
    }

    /// Mark the schedule as triggered.
    pub fn mark_triggered(&mut self) {
        self.last_trigger = Some(Local::now());
        let _ = self.update_next_trigger();
    }
}

/// Parse a cron field into a list of matching values.
fn parse_cron_field(field: &str, min: i32, max: i32) -> Result<Vec<i32>, TaskCronSchedulerError> {
    let mut values = Vec::new();

    for part in field.split(',') {
        if part == "*" {
            return Ok((min..=max).collect());
        } else if part.contains('/') {
            // Step values: */5 or 1-10/2
            let slash_parts: Vec<&str> = part.split('/').collect();
            if slash_parts.len() != 2 {
                return Err(TaskCronSchedulerError::InvalidCron(format!(
                    "invalid step: {}",
                    part
                )));
            }

            let step: i32 = slash_parts[1].parse().map_err(|_| {
                TaskCronSchedulerError::InvalidCron(format!("invalid step: {}", part))
            })?;

            let range = parse_range(slash_parts[0], min, max)?;
            let mut i = range[0];
            while i <= *range.last().unwrap() {
                values.push(i);
                i += step;
            }
        } else if part.contains('-') {
            // Range: 1-5
            let range = parse_range(part, min, max)?;
            values.extend(range);
        } else {
            // Single value
            let val: i32 = part.parse().map_err(|_| {
                TaskCronSchedulerError::InvalidCron(format!("invalid value: {}", part))
            })?;
            if val < min || val > max {
                return Err(TaskCronSchedulerError::InvalidCron(format!(
                    "value {} out of range [{}, {}]",
                    val, min, max
                )));
            }
            values.push(val);
        }
    }

    values.sort();
    values.dedup();
    Ok(values)
}

/// Parse a range field (e.g., "1-5" or "*").
fn parse_range(field: &str, min: i32, max: i32) -> Result<Vec<i32>, TaskCronSchedulerError> {
    if field == "*" {
        return Ok((min..=max).collect());
    }

    let parts: Vec<&str> = field.split('-').collect();
    if parts.len() != 2 {
        return Err(TaskCronSchedulerError::InvalidCron(format!(
            "invalid range: {}",
            field
        )));
    }

    let start: i32 = parts[0].parse().map_err(|_| {
        TaskCronSchedulerError::InvalidCron(format!("invalid range start: {}", field))
    })?;
    let end: i32 = parts[1].parse().map_err(|_| {
        TaskCronSchedulerError::InvalidCron(format!("invalid range end: {}", field))
    })?;

    if start < min || end > max || start > end {
        return Err(TaskCronSchedulerError::InvalidCron(format!(
            "range {}-{} out of bounds [{}, {}]",
            start, end, min, max
        )));
    }

    Ok((start..=end).collect())
}

/// Check if a value matches any of the allowed values.
fn matches_field(value: i32, allowed: &[i32]) -> bool {
    allowed.contains(&value)
}

/// Task cron scheduler configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskCronSchedulerConfig {
    /// Enable cron scheduling.
    pub enabled: bool,
    /// Check interval in seconds (default: 60).
    pub check_interval_secs: u64,
}

/// Task cron scheduler data for persistence.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskCronSchedulerData {
    /// Scheduler configuration.
    pub config: TaskCronSchedulerConfig,
    /// Task schedules (task_id -> schedule).
    pub schedules: HashMap<String, TaskCronSchedule>,
}

/// Task cron scheduler manager.
#[derive(Debug, Clone)]
pub struct TaskCronScheduler {
    config: TaskCronSchedulerConfig,
    schedules: HashMap<String, TaskCronSchedule>,
}

impl TaskCronScheduler {
    /// Create a new scheduler with default configuration.
    pub fn new() -> Self {
        Self {
            config: TaskCronSchedulerConfig {
                enabled: true,
                check_interval_secs: 60,
            },
            schedules: HashMap::new(),
        }
    }

    /// Create a scheduler from persisted data.
    pub fn from_data(data: TaskCronSchedulerData) -> Self {
        Self {
            config: data.config,
            schedules: data.schedules,
        }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &TaskCronSchedulerConfig {
        &self.config
    }

    /// Set the configuration.
    pub fn set_config(&mut self, config: TaskCronSchedulerConfig) {
        self.config = config;
    }

    /// Add a cron schedule for a task.
    pub fn add_schedule(
        &mut self,
        task_id: &str,
        schedule: TaskCronSchedule,
    ) -> Result<(), TaskCronSchedulerError> {
        self.schedules.insert(task_id.to_string(), schedule);
        Ok(())
    }

    /// Remove a cron schedule for a task.
    pub fn remove_schedule(
        &mut self,
        task_id: &str,
    ) -> Result<TaskCronSchedule, TaskCronSchedulerError> {
        self.schedules
            .remove(task_id)
            .ok_or_else(|| TaskCronSchedulerError::NoSchedule(task_id.to_string()))
    }

    /// Get the schedule for a task.
    pub fn get_schedule(&self, task_id: &str) -> Option<&TaskCronSchedule> {
        self.schedules.get(task_id)
    }

    /// Get a mutable reference to the schedule for a task.
    pub fn get_schedule_mut(&mut self, task_id: &str) -> Option<&mut TaskCronSchedule> {
        self.schedules.get_mut(task_id)
    }

    /// List all schedules.
    pub fn list_schedules(&self) -> Vec<(&String, &TaskCronSchedule)> {
        self.schedules.iter().collect()
    }

    /// Enable or disable a schedule.
    pub fn set_schedule_enabled(
        &mut self,
        task_id: &str,
        enabled: bool,
    ) -> Result<(), TaskCronSchedulerError> {
        let schedule = self
            .schedules
            .get_mut(task_id)
            .ok_or_else(|| TaskCronSchedulerError::NoSchedule(task_id.to_string()))?;
        schedule.enabled = enabled;
        Ok(())
    }

    /// Check all schedules and return task IDs that should be triggered.
    pub fn check_schedules(&mut self) -> Result<Vec<String>, TaskCronSchedulerError> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        let now = Local::now();
        let mut triggered = Vec::new();

        for (task_id, schedule) in self.schedules.iter_mut() {
            if !schedule.enabled {
                continue;
            }

            if schedule.should_trigger_at(now)? {
                schedule.mark_triggered();
                triggered.push(task_id.clone());
            }
        }

        Ok(triggered)
    }

    /// Update next trigger times for all schedules.
    pub fn update_all_next_triggers(&mut self) -> Result<(), TaskCronSchedulerError> {
        for schedule in self.schedules.values_mut() {
            schedule.update_next_trigger()?;
        }
        Ok(())
    }

    /// Get a summary of the scheduler.
    pub fn summary(&self) -> TaskCronSchedulerSummary {
        let total_schedules = self.schedules.len();
        let enabled_schedules = self.schedules.values().filter(|s| s.enabled).count();
        let disabled_schedules = total_schedules - enabled_schedules;

        let mut upcoming_triggers: Vec<(&String, &DateTime<Local>)> = self
            .schedules
            .iter()
            .filter_map(|(id, s)| s.next_trigger.as_ref().map(|t| (id, t)))
            .collect();
        upcoming_triggers.sort_by_key(|(_, t)| **t);

        TaskCronSchedulerSummary {
            total_schedules,
            enabled_schedules,
            disabled_schedules,
            upcoming_triggers: upcoming_triggers
                .into_iter()
                .take(10)
                .map(|(id, t)| (id.clone(), *t))
                .collect(),
        }
    }

    /// Convert to persistable data.
    pub fn to_data(&self) -> TaskCronSchedulerData {
        TaskCronSchedulerData {
            config: self.config.clone(),
            schedules: self.schedules.clone(),
        }
    }

    /// Save to disk.
    pub async fn save(&self, dir: &Path) -> Result<(), TaskCronSchedulerError> {
        let data = self.to_data();
        let json = serde_json::to_string_pretty(&data)?;
        let path = dir.join("task_cron_scheduler.json");
        fs::write(&path, json).await?;
        Ok(())
    }

    /// Load from disk.
    pub async fn load(dir: &Path) -> Result<Self, TaskCronSchedulerError> {
        let path = dir.join("task_cron_scheduler.json");
        if !path.exists() {
            return Ok(Self::new());
        }
        let json = fs::read_to_string(&path).await?;
        let data: TaskCronSchedulerData = serde_json::from_str(&json)?;
        Ok(Self::from_data(data))
    }
}

impl Default for TaskCronScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of the task cron scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCronSchedulerSummary {
    /// Total number of schedules.
    pub total_schedules: usize,
    /// Number of enabled schedules.
    pub enabled_schedules: usize,
    /// Number of disabled schedules.
    pub disabled_schedules: usize,
    /// Upcoming triggers (task_id, next_trigger_time), sorted by time.
    pub upcoming_triggers: Vec<(String, DateTime<Local>)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_parse_cron_field_single() {
        let values = parse_cron_field("5", 0, 59).unwrap();
        assert_eq!(values, vec![5]);
    }

    #[test]
    fn test_parse_cron_field_wildcard() {
        let values = parse_cron_field("*", 0, 59).unwrap();
        assert_eq!(values.len(), 60);
        assert_eq!(values[0], 0);
        assert_eq!(values[59], 59);
    }

    #[test]
    fn test_parse_cron_field_range() {
        let values = parse_cron_field("1-5", 0, 59).unwrap();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_parse_cron_field_step() {
        let values = parse_cron_field("*/15", 0, 59).unwrap();
        assert_eq!(values, vec![0, 15, 30, 45]);
    }

    #[test]
    fn test_parse_cron_field_list() {
        let values = parse_cron_field("0,15,30,45", 0, 59).unwrap();
        assert_eq!(values, vec![0, 15, 30, 45]);
    }

    #[test]
    fn test_parse_cron_field_invalid() {
        assert!(parse_cron_field("60", 0, 59).is_err());
        assert!(parse_cron_field("abc", 0, 59).is_err());
    }

    #[test]
    fn test_task_cron_schedule_new() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test Schedule".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        assert_eq!(schedule.cron_expr, "0 2 * * *");
        assert!(schedule.enabled);
        assert!(schedule.next_trigger.is_some());
    }

    #[test]
    fn test_task_cron_schedule_invalid() {
        let result = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "invalid".to_string(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_task_cron_schedule_with_duration() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap()
        .with_duration(3600);
        assert_eq!(schedule.duration_secs, Some(3600));
    }

    #[test]
    fn test_task_cron_schedule_should_trigger() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "30 14 * * *".to_string(),
        )
        .unwrap();

        // Should trigger at 14:30
        let trigger_time = Local.with_ymd_and_hms(2026, 8, 12, 14, 30, 0).unwrap();
        assert!(schedule.should_trigger_at(trigger_time).unwrap());

        // Should not trigger at 14:31
        let no_trigger = Local.with_ymd_and_hms(2026, 8, 12, 14, 31, 0).unwrap();
        assert!(!schedule.should_trigger_at(no_trigger).unwrap());
    }

    #[test]
    fn test_task_cron_schedule_disabled() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "30 14 * * *".to_string(),
        )
        .unwrap()
        .with_enabled(false);

        let trigger_time = Local.with_ymd_and_hms(2026, 8, 12, 14, 30, 0).unwrap();
        assert!(!schedule.should_trigger_at(trigger_time).unwrap());
    }

    #[test]
    fn test_task_cron_scheduler_add_remove() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "sched1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();

        scheduler.add_schedule("task1", schedule).unwrap();
        assert!(scheduler.get_schedule("task1").is_some());

        let removed = scheduler.remove_schedule("task1").unwrap();
        assert_eq!(removed.id, "sched1");
        assert!(scheduler.get_schedule("task1").is_none());
    }

    #[test]
    fn test_task_cron_scheduler_remove_nonexistent() {
        let mut scheduler = TaskCronScheduler::new();
        assert!(scheduler.remove_schedule("nonexistent").is_err());
    }

    #[test]
    fn test_task_cron_scheduler_enable_disable() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "sched1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();

        scheduler.add_schedule("task1", schedule).unwrap();
        scheduler.set_schedule_enabled("task1", false).unwrap();

        let s = scheduler.get_schedule("task1").unwrap();
        assert!(!s.enabled);
    }

    #[test]
    fn test_task_cron_scheduler_summary() {
        let mut scheduler = TaskCronScheduler::new();

        let s1 = TaskCronSchedule::new("s1".to_string(), "S1".to_string(), "0 2 * * *".to_string())
            .unwrap();
        let s2 = TaskCronSchedule::new("s2".to_string(), "S2".to_string(), "0 3 * * *".to_string())
            .unwrap()
            .with_enabled(false);

        scheduler.add_schedule("task1", s1).unwrap();
        scheduler.add_schedule("task2", s2).unwrap();

        let summary = scheduler.summary();
        assert_eq!(summary.total_schedules, 2);
        assert_eq!(summary.enabled_schedules, 1);
        assert_eq!(summary.disabled_schedules, 1);
        assert_eq!(summary.upcoming_triggers.len(), 2);
    }

    #[test]
    fn test_task_cron_scheduler_persistence() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "sched1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        let data = scheduler.to_data();
        let json = serde_json::to_string(&data).unwrap();
        let loaded: TaskCronSchedulerData = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.schedules.len(), 1);
        assert!(loaded.schedules.contains_key("task1"));
    }

    #[test]
    fn test_task_cron_schedule_mark_triggered() {
        let mut schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();

        assert!(schedule.last_trigger.is_none());
        schedule.mark_triggered();
        assert!(schedule.last_trigger.is_some());
        assert!(schedule.next_trigger.is_some());
    }

    #[test]
    fn test_parse_cron_field_range_step() {
        let values = parse_cron_field("0-10/2", 0, 59).unwrap();
        assert_eq!(values, vec![0, 2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_task_cron_scheduler_update_all_next_triggers() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "sched1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        scheduler.update_all_next_triggers().unwrap();
        assert!(
            scheduler
                .get_schedule("task1")
                .unwrap()
                .next_trigger
                .is_some()
        );
    }
}
