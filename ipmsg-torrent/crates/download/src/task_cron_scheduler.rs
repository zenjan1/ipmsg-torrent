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

    // ─── parse_cron_field ───────────────────────────────────────────

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
    fn test_parse_cron_field_range_step() {
        let values = parse_cron_field("0-10/2", 0, 59).unwrap();
        assert_eq!(values, vec![0, 2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_parse_cron_field_zero() {
        let values = parse_cron_field("0", 0, 59).unwrap();
        assert_eq!(values, vec![0]);
    }

    #[test]
    fn test_parse_cron_field_max_value() {
        let values = parse_cron_field("59", 0, 59).unwrap();
        assert_eq!(values, vec![59]);
    }

    #[test]
    fn test_parse_cron_field_out_of_range_high() {
        let result = parse_cron_field("60", 0, 59);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cron_field_out_of_range_negative() {
        let result = parse_cron_field("-1", 0, 59);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cron_field_dedup() {
        let values = parse_cron_field("5,5,5", 0, 59).unwrap();
        assert_eq!(values, vec![5]);
    }

    #[test]
    fn test_parse_cron_field_sorted() {
        let values = parse_cron_field("30,10,50,20", 0, 59).unwrap();
        assert_eq!(values, vec![10, 20, 30, 50]);
    }

    #[test]
    fn test_parse_cron_field_invalid_step_format() {
        // three slashes
        assert!(parse_cron_field("1/2/3", 0, 59).is_err());
    }

    #[test]
    fn test_parse_cron_field_invalid_step_value() {
        assert!(parse_cron_field("*/abc", 0, 59).is_err());
    }

    #[test]
    fn test_parse_cron_field_wildcard_hours() {
        let values = parse_cron_field("*", 0, 23).unwrap();
        assert_eq!(values.len(), 24);
    }

    #[test]
    fn test_parse_cron_field_wildcard_day_of_month() {
        let values = parse_cron_field("*", 1, 31).unwrap();
        assert_eq!(values.len(), 31);
        assert_eq!(values[0], 1);
    }

    #[test]
    fn test_parse_cron_field_wildcard_month() {
        let values = parse_cron_field("*", 1, 12).unwrap();
        assert_eq!(values.len(), 12);
    }

    #[test]
    fn test_parse_cron_field_wildcard_day_of_week() {
        let values = parse_cron_field("*", 0, 6).unwrap();
        assert_eq!(values.len(), 7);
        assert_eq!(values[0], 0);
        assert_eq!(values[6], 6);
    }

    #[test]
    fn test_parse_cron_field_single_value_hour() {
        let values = parse_cron_field("14", 0, 23).unwrap();
        assert_eq!(values, vec![14]);
    }

    #[test]
    fn test_parse_cron_field_range_full() {
        let values = parse_cron_field("0-6", 0, 6).unwrap();
        assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_parse_cron_field_step_10_minutes() {
        let values = parse_cron_field("*/10", 0, 59).unwrap();
        assert_eq!(values, vec![0, 10, 20, 30, 40, 50]);
    }

    // ─── parse_range ────────────────────────────────────────────────

    #[test]
    fn test_parse_range_basic() {
        let values = parse_range("1-5", 0, 59).unwrap();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_parse_range_wildcard() {
        let values = parse_range("*", 0, 59).unwrap();
        assert_eq!(values.len(), 60);
    }

    #[test]
    fn test_parse_range_invalid_format() {
        assert!(parse_range("1-", 0, 59).is_err());
    }

    #[test]
    fn test_parse_range_non_numeric() {
        assert!(parse_range("a-b", 0, 59).is_err());
    }

    #[test]
    fn test_parse_range_start_greater_than_end() {
        assert!(parse_range("10-5", 0, 59).is_err());
    }

    #[test]
    fn test_parse_range_out_of_bounds() {
        assert!(parse_range("0-60", 0, 59).is_err());
    }

    #[test]
    fn test_parse_range_single_element() {
        let values = parse_range("5-5", 0, 59).unwrap();
        assert_eq!(values, vec![5]);
    }

    #[test]
    fn test_parse_range_three_parts() {
        assert!(parse_range("1-2-3", 0, 59).is_err());
    }

    // ─── matches_field ──────────────────────────────────────────────

    #[test]
    fn test_matches_field_found() {
        assert!(matches_field(5, &[1, 3, 5, 7]));
    }

    #[test]
    fn test_matches_field_not_found() {
        assert!(!matches_field(4, &[1, 3, 5, 7]));
    }

    #[test]
    fn test_matches_field_empty() {
        assert!(!matches_field(0, &[]));
    }

    // ─── TaskCronSchedulerError ─────────────────────────────────────

    #[test]
    fn test_error_display_no_schedule() {
        let err = TaskCronSchedulerError::NoSchedule("task-abc".to_string());
        assert_eq!(err.to_string(), "task task-abc has no cron schedule");
    }

    #[test]
    fn test_error_display_invalid_cron() {
        let err = TaskCronSchedulerError::InvalidCron("bad expr".to_string());
        assert_eq!(err.to_string(), "invalid cron expression: bad expr");
    }

    #[test]
    fn test_error_display_io() {
        let err = TaskCronSchedulerError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn test_error_display_serialize() {
        let json_err = serde_json::from_str::<String>("invalid").unwrap_err();
        let err = TaskCronSchedulerError::Serialize(json_err);
        let msg = err.to_string();
        assert!(msg.contains("serialization error"));
    }

    #[test]
    fn test_error_debug() {
        let err = TaskCronSchedulerError::NoSchedule("t1".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("NoSchedule"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: TaskCronSchedulerError = TaskCronSchedulerError::from(io_err);
        assert!(err.to_string().contains("denied"));
    }

    #[test]
    fn test_error_from_serde_json() {
        let json_err = serde_json::from_str::<String>("not json").unwrap_err();
        let err: TaskCronSchedulerError = TaskCronSchedulerError::from(json_err);
        let msg = err.to_string();
        assert!(msg.contains("serialization error"));
    }

    // ─── TaskCronSchedule::new ──────────────────────────────────────

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
    fn test_task_cron_schedule_new_fields() {
        let schedule = TaskCronSchedule::new(
            "id-1".to_string(),
            "My Schedule".to_string(),
            "30 14 * * 1".to_string(),
        )
        .unwrap();
        assert_eq!(schedule.id, "id-1");
        assert_eq!(schedule.name, "My Schedule");
        assert_eq!(schedule.cron_expr, "30 14 * * 1");
        assert!(schedule.duration_secs.is_none());
        assert!(schedule.last_trigger.is_none());
    }

    #[test]
    fn test_task_cron_schedule_invalid_too_few_fields() {
        let result =
            TaskCronSchedule::new("test".to_string(), "Test".to_string(), "0 2 *".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_task_cron_schedule_invalid_too_many_fields() {
        let result = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 2 * * * *".to_string(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_task_cron_schedule_invalid_empty() {
        let result = TaskCronSchedule::new("test".to_string(), "Test".to_string(), "".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_task_cron_schedule_invalid_non_numeric() {
        let result = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "abc def * * *".to_string(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_task_cron_schedule_invalid_minute_out_of_range() {
        let result = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "60 2 * * *".to_string(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_task_cron_schedule_invalid_hour_out_of_range() {
        let result = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 25 * * *".to_string(),
        );
        assert!(result.is_err());
    }

    // ─── TaskCronSchedule builder methods ───────────────────────────

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
    fn test_task_cron_schedule_with_duration_zero() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap()
        .with_duration(0);
        assert_eq!(schedule.duration_secs, Some(0));
    }

    #[test]
    fn test_task_cron_schedule_with_enabled_false() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap()
        .with_enabled(false);
        assert!(!schedule.enabled);
    }

    #[test]
    fn test_task_cron_schedule_with_enabled_true() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap()
        .with_enabled(false)
        .with_enabled(true);
        assert!(schedule.enabled);
    }

    #[test]
    fn test_task_cron_schedule_builder_chain() {
        let schedule = TaskCronSchedule::new(
            "id".to_string(),
            "Name".to_string(),
            "0 3 * * *".to_string(),
        )
        .unwrap()
        .with_duration(7200)
        .with_enabled(false);
        assert_eq!(schedule.duration_secs, Some(7200));
        assert!(!schedule.enabled);
    }

    // ─── TaskCronSchedule::should_trigger_at ────────────────────────

    #[test]
    fn test_task_cron_schedule_should_trigger() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "30 14 * * *".to_string(),
        )
        .unwrap();

        let trigger_time = Local.with_ymd_and_hms(2026, 8, 12, 14, 30, 0).unwrap();
        assert!(schedule.should_trigger_at(trigger_time).unwrap());

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
    fn test_task_cron_schedule_should_trigger_specific_dow() {
        // Monday = 1
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 9 * * 1".to_string(),
        )
        .unwrap();

        // 2026-08-10 is a Monday
        let monday = Local.with_ymd_and_hms(2026, 8, 10, 9, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(monday).unwrap());

        // 2026-08-11 is a Tuesday
        let tuesday = Local.with_ymd_and_hms(2026, 8, 11, 9, 0, 0).unwrap();
        assert!(!schedule.should_trigger_at(tuesday).unwrap());
    }

    #[test]
    fn test_task_cron_schedule_should_trigger_specific_month() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 0 1 6 *".to_string(),
        )
        .unwrap();

        // June 1st
        let june1 = Local.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(june1).unwrap());

        // July 1st
        let july1 = Local.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        assert!(!schedule.should_trigger_at(july1).unwrap());
    }

    #[test]
    fn test_task_cron_schedule_should_trigger_specific_dom() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 12 15 * *".to_string(),
        )
        .unwrap();

        let day15 = Local.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(day15).unwrap());

        let day16 = Local.with_ymd_and_hms(2026, 8, 16, 12, 0, 0).unwrap();
        assert!(!schedule.should_trigger_at(day16).unwrap());
    }

    #[test]
    fn test_task_cron_schedule_wildcard_triggers_every_minute_match() {
        let schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "* * * * *".to_string(),
        )
        .unwrap();

        let any_time = Local.with_ymd_and_hms(2026, 3, 15, 10, 45, 0).unwrap();
        assert!(schedule.should_trigger_at(any_time).unwrap());
    }

    // ─── TaskCronSchedule::mark_triggered ───────────────────────────

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
    fn test_task_cron_schedule_mark_triggered_multiple() {
        let mut schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();

        schedule.mark_triggered();
        let first_trigger = schedule.last_trigger.unwrap();

        schedule.mark_triggered();
        let second_trigger = schedule.last_trigger.unwrap();
        assert!(second_trigger >= first_trigger);
    }

    // ─── TaskCronSchedule::update_next_trigger ──────────────────────

    #[test]
    fn test_update_next_trigger() {
        let mut schedule = TaskCronSchedule::new(
            "test".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();

        let old_next = schedule.next_trigger;
        schedule.update_next_trigger().unwrap();
        assert!(schedule.next_trigger.is_some());
        // next_trigger should be updated (same or later)
        assert!(schedule.next_trigger >= old_next);
    }

    // ─── TaskCronSchedulerConfig ────────────────────────────────────

    #[test]
    fn test_config_default() {
        let config = TaskCronSchedulerConfig::default();
        // Default for bool is false
        assert!(!config.enabled);
        // Default for u64 is 0
        assert_eq!(config.check_interval_secs, 0);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = TaskCronSchedulerConfig {
            enabled: false,
            check_interval_secs: 120,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: TaskCronSchedulerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.check_interval_secs, 120);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = TaskCronSchedulerConfig {
            enabled: true,
            check_interval_secs: 30,
        };
        let cloned = config.clone();
        assert_eq!(cloned.enabled, true);
        assert_eq!(cloned.check_interval_secs, 30);
        let debug = format!("{:?}", config);
        assert!(debug.contains("TaskCronSchedulerConfig"));
    }

    // ─── TaskCronSchedulerData ──────────────────────────────────────

    #[test]
    fn test_data_default() {
        let data = TaskCronSchedulerData::default();
        assert!(!data.config.enabled);
        assert!(data.schedules.is_empty());
    }

    #[test]
    fn test_data_serde_roundtrip() {
        let mut data = TaskCronSchedulerData {
            config: TaskCronSchedulerConfig {
                enabled: true,
                check_interval_secs: 60,
            },
            schedules: HashMap::new(),
        };
        let schedule =
            TaskCronSchedule::new("s1".to_string(), "S1".to_string(), "0 2 * * *".to_string())
                .unwrap();
        data.schedules.insert("task1".to_string(), schedule);

        let json = serde_json::to_string(&data).unwrap();
        let loaded: TaskCronSchedulerData = serde_json::from_str(&json).unwrap();
        assert!(loaded.config.enabled);
        assert_eq!(loaded.schedules.len(), 1);
        assert!(loaded.schedules.contains_key("task1"));
    }

    #[test]
    fn test_data_clone_debug() {
        let data = TaskCronSchedulerData::default();
        let cloned = data.clone();
        assert_eq!(cloned.schedules.len(), 0);
        let debug = format!("{:?}", data);
        assert!(debug.contains("TaskCronSchedulerData"));
    }

    // ─── TaskCronScheduler::new / default ───────────────────────────

    #[test]
    fn test_scheduler_new() {
        let scheduler = TaskCronScheduler::new();
        assert!(scheduler.config().enabled);
        assert_eq!(scheduler.config().check_interval_secs, 60);
        assert!(scheduler.list_schedules().is_empty());
    }

    #[test]
    fn test_scheduler_default_equals_new() {
        let new = TaskCronScheduler::new();
        let default = TaskCronScheduler::default();
        assert_eq!(new.config().enabled, default.config().enabled);
        assert_eq!(
            new.config().check_interval_secs,
            default.config().check_interval_secs
        );
    }

    // ─── TaskCronScheduler::from_data ───────────────────────────────

    #[test]
    fn test_scheduler_from_data() {
        let mut data = TaskCronSchedulerData {
            config: TaskCronSchedulerConfig {
                enabled: false,
                check_interval_secs: 30,
            },
            schedules: HashMap::new(),
        };
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Schedule 1".to_string(),
            "0 5 * * *".to_string(),
        )
        .unwrap();
        data.schedules.insert("task1".to_string(), schedule);

        let scheduler = TaskCronScheduler::from_data(data);
        assert!(!scheduler.config().enabled);
        assert_eq!(scheduler.config().check_interval_secs, 30);
        assert!(scheduler.get_schedule("task1").is_some());
    }

    #[test]
    fn test_scheduler_from_data_empty() {
        let data = TaskCronSchedulerData::default();
        let scheduler = TaskCronScheduler::from_data(data);
        assert!(scheduler.list_schedules().is_empty());
    }

    // ─── TaskCronScheduler::config / set_config ─────────────────────

    #[test]
    fn test_scheduler_config_ref() {
        let scheduler = TaskCronScheduler::new();
        let config = scheduler.config();
        assert!(config.enabled);
    }

    #[test]
    fn test_scheduler_set_config() {
        let mut scheduler = TaskCronScheduler::new();
        scheduler.set_config(TaskCronSchedulerConfig {
            enabled: false,
            check_interval_secs: 120,
        });
        assert!(!scheduler.config().enabled);
        assert_eq!(scheduler.config().check_interval_secs, 120);
    }

    // ─── TaskCronScheduler::add_schedule ────────────────────────────

    #[test]
    fn test_add_schedule() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();
        assert!(scheduler.get_schedule("task1").is_some());
    }

    #[test]
    fn test_add_schedule_overwrite() {
        let mut scheduler = TaskCronScheduler::new();
        let s1 = TaskCronSchedule::new(
            "s1".to_string(),
            "First".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        let s2 = TaskCronSchedule::new(
            "s2".to_string(),
            "Second".to_string(),
            "0 3 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", s1).unwrap();
        scheduler.add_schedule("task1", s2).unwrap();
        assert_eq!(scheduler.get_schedule("task1").unwrap().id, "s2");
    }

    #[test]
    fn test_add_schedule_multiple_tasks() {
        let mut scheduler = TaskCronScheduler::new();
        for i in 0..5 {
            let schedule = TaskCronSchedule::new(
                format!("s{}", i),
                format!("Schedule {}", i),
                "0 2 * * *".to_string(),
            )
            .unwrap();
            scheduler
                .add_schedule(&format!("task{}", i), schedule)
                .unwrap();
        }
        assert_eq!(scheduler.list_schedules().len(), 5);
    }

    // ─── TaskCronScheduler::remove_schedule ─────────────────────────

    #[test]
    fn test_remove_schedule() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();
        let removed = scheduler.remove_schedule("task1").unwrap();
        assert_eq!(removed.id, "s1");
        assert!(scheduler.get_schedule("task1").is_none());
    }

    #[test]
    fn test_remove_schedule_nonexistent() {
        let mut scheduler = TaskCronScheduler::new();
        let result = scheduler.remove_schedule("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_schedule_idempotent() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();
        scheduler.remove_schedule("task1").unwrap();
        assert!(scheduler.remove_schedule("task1").is_err());
    }

    // ─── TaskCronScheduler::get_schedule / get_schedule_mut ─────────

    #[test]
    fn test_get_schedule_exists() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();
        let s = scheduler.get_schedule("task1").unwrap();
        assert_eq!(s.id, "s1");
    }

    #[test]
    fn test_get_schedule_not_exists() {
        let scheduler = TaskCronScheduler::new();
        assert!(scheduler.get_schedule("nope").is_none());
    }

    #[test]
    fn test_get_schedule_mut() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        let s = scheduler.get_schedule_mut("task1").unwrap();
        s.enabled = false;

        assert!(!scheduler.get_schedule("task1").unwrap().enabled);
    }

    #[test]
    fn test_get_schedule_mut_not_exists() {
        let mut scheduler = TaskCronScheduler::new();
        assert!(scheduler.get_schedule_mut("nope").is_none());
    }

    // ─── TaskCronScheduler::list_schedules ──────────────────────────

    #[test]
    fn test_list_schedules_empty() {
        let scheduler = TaskCronScheduler::new();
        assert!(scheduler.list_schedules().is_empty());
    }

    #[test]
    fn test_list_schedules_multiple() {
        let mut scheduler = TaskCronScheduler::new();
        for i in 0..3 {
            let schedule = TaskCronSchedule::new(
                format!("s{}", i),
                format!("S{}", i),
                "0 2 * * *".to_string(),
            )
            .unwrap();
            scheduler
                .add_schedule(&format!("task{}", i), schedule)
                .unwrap();
        }
        assert_eq!(scheduler.list_schedules().len(), 3);
    }

    // ─── TaskCronScheduler::set_schedule_enabled ────────────────────

    #[test]
    fn test_set_schedule_enabled() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();
        scheduler.set_schedule_enabled("task1", false).unwrap();
        assert!(!scheduler.get_schedule("task1").unwrap().enabled);
    }

    #[test]
    fn test_set_schedule_enabled_nonexistent() {
        let mut scheduler = TaskCronScheduler::new();
        assert!(scheduler.set_schedule_enabled("nope", false).is_err());
    }

    #[test]
    fn test_set_schedule_enabled_toggle() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        scheduler.set_schedule_enabled("task1", false).unwrap();
        assert!(!scheduler.get_schedule("task1").unwrap().enabled);

        scheduler.set_schedule_enabled("task1", true).unwrap();
        assert!(scheduler.get_schedule("task1").unwrap().enabled);
    }

    // ─── TaskCronScheduler::check_schedules ─────────────────────────

    #[test]
    fn test_check_schedules_disabled_global() {
        let mut scheduler = TaskCronScheduler::new();
        scheduler.set_config(TaskCronSchedulerConfig {
            enabled: false,
            check_interval_secs: 60,
        });

        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "* * * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        let triggered = scheduler.check_schedules().unwrap();
        assert!(triggered.is_empty());
    }

    #[test]
    fn test_check_schedules_disabled_schedule() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "* * * * *".to_string(),
        )
        .unwrap()
        .with_enabled(false);
        scheduler.add_schedule("task1", schedule).unwrap();

        let triggered = scheduler.check_schedules().unwrap();
        assert!(triggered.is_empty());
    }

    #[test]
    fn test_check_schedules_empty() {
        let mut scheduler = TaskCronScheduler::new();
        let triggered = scheduler.check_schedules().unwrap();
        assert!(triggered.is_empty());
    }

    // ─── TaskCronScheduler::update_all_next_triggers ────────────────

    #[test]
    fn test_update_all_next_triggers() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
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

    #[test]
    fn test_update_all_next_triggers_empty() {
        let mut scheduler = TaskCronScheduler::new();
        scheduler.update_all_next_triggers().unwrap();
    }

    #[test]
    fn test_update_all_next_triggers_multiple() {
        let mut scheduler = TaskCronScheduler::new();
        for i in 0..3 {
            let schedule = TaskCronSchedule::new(
                format!("s{}", i),
                format!("S{}", i),
                format!("{} 2 * * *", i).as_str().to_string(),
            )
            .unwrap();
            scheduler
                .add_schedule(&format!("task{}", i), schedule)
                .unwrap();
        }
        scheduler.update_all_next_triggers().unwrap();
        for i in 0..3 {
            assert!(
                scheduler
                    .get_schedule(&format!("task{}", i))
                    .unwrap()
                    .next_trigger
                    .is_some()
            );
        }
    }

    // ─── TaskCronScheduler::summary ─────────────────────────────────

    #[test]
    fn test_summary_empty() {
        let scheduler = TaskCronScheduler::new();
        let summary = scheduler.summary();
        assert_eq!(summary.total_schedules, 0);
        assert_eq!(summary.enabled_schedules, 0);
        assert_eq!(summary.disabled_schedules, 0);
        assert!(summary.upcoming_triggers.is_empty());
    }

    #[test]
    fn test_summary() {
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
    fn test_summary_upcoming_sorted() {
        let mut scheduler = TaskCronScheduler::new();
        let s1 = TaskCronSchedule::new(
            "s1".to_string(),
            "Early".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        let s2 = TaskCronSchedule::new(
            "s2".to_string(),
            "Late".to_string(),
            "0 5 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", s1).unwrap();
        scheduler.add_schedule("task2", s2).unwrap();

        let summary = scheduler.summary();
        // upcoming_triggers should be sorted by time
        assert!(summary.upcoming_triggers.len() >= 2);
        assert!(summary.upcoming_triggers[0].1 <= summary.upcoming_triggers[1].1);
    }

    #[test]
    fn test_summary_max_10_upcoming() {
        let mut scheduler = TaskCronScheduler::new();
        for i in 0..15 {
            let schedule = TaskCronSchedule::new(
                format!("s{}", i),
                format!("S{}", i),
                format!("{} * * * *", i % 60).as_str().to_string(),
            )
            .unwrap();
            scheduler
                .add_schedule(&format!("task{}", i), schedule)
                .unwrap();
        }
        let summary = scheduler.summary();
        assert!(summary.upcoming_triggers.len() <= 10);
    }

    // ─── TaskCronScheduler::to_data ─────────────────────────────────

    #[test]
    fn test_to_data() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        let data = scheduler.to_data();
        assert!(data.config.enabled);
        assert_eq!(data.schedules.len(), 1);
        assert!(data.schedules.contains_key("task1"));
    }

    #[test]
    fn test_to_data_empty() {
        let scheduler = TaskCronScheduler::new();
        let data = scheduler.to_data();
        assert!(data.schedules.is_empty());
    }

    // ─── TaskCronScheduler::save / load (async) ─────────────────────

    #[tokio::test]
    async fn test_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        scheduler.save(dir.path()).await.unwrap();

        let loaded = TaskCronScheduler::load(dir.path()).await.unwrap();
        assert!(loaded.get_schedule("task1").is_some());
        assert_eq!(loaded.get_schedule("task1").unwrap().id, "s1");
    }

    #[tokio::test]
    async fn test_load_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = TaskCronScheduler::load(dir.path()).await.unwrap();
        assert!(loaded.list_schedules().is_empty());
        assert!(loaded.config().enabled);
    }

    #[tokio::test]
    async fn test_save_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let mut scheduler = TaskCronScheduler::new();

        let s1 = TaskCronSchedule::new(
            "s1".to_string(),
            "First".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", s1).unwrap();
        scheduler.save(dir.path()).await.unwrap();

        // Overwrite with different data
        let s2 = TaskCronSchedule::new(
            "s2".to_string(),
            "Second".to_string(),
            "0 3 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", s2).unwrap();
        scheduler.save(dir.path()).await.unwrap();

        let loaded = TaskCronScheduler::load(dir.path()).await.unwrap();
        assert_eq!(loaded.get_schedule("task1").unwrap().id, "s2");
    }

    #[tokio::test]
    async fn test_load_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("task_cron_scheduler.json");
        tokio::fs::write(&path, "not valid json").await.unwrap();

        let result = TaskCronScheduler::load(dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = TaskCronScheduler::new();
        scheduler.save(dir.path()).await.unwrap();

        let path = dir.path().join("task_cron_scheduler.json");
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_save_empty_schedules() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = TaskCronScheduler::new();
        scheduler.save(dir.path()).await.unwrap();

        let loaded = TaskCronScheduler::load(dir.path()).await.unwrap();
        assert!(loaded.list_schedules().is_empty());
    }

    // ─── TaskCronSchedulerSummary ───────────────────────────────────

    #[test]
    fn test_summary_serde_roundtrip() {
        let summary = TaskCronSchedulerSummary {
            total_schedules: 5,
            enabled_schedules: 3,
            disabled_schedules: 2,
            upcoming_triggers: vec![],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let loaded: TaskCronSchedulerSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_schedules, 5);
        assert_eq!(loaded.enabled_schedules, 3);
        assert_eq!(loaded.disabled_schedules, 2);
    }

    #[test]
    fn test_summary_clone_debug() {
        let summary = TaskCronSchedulerSummary {
            total_schedules: 1,
            enabled_schedules: 1,
            disabled_schedules: 0,
            upcoming_triggers: vec![],
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_schedules, 1);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("TaskCronSchedulerSummary"));
    }

    // ─── TaskCronSchedule serde ─────────────────────────────────────

    #[test]
    fn test_schedule_serde_roundtrip() {
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "My Schedule".to_string(),
            "30 14 * * 1".to_string(),
        )
        .unwrap()
        .with_duration(3600);

        let json = serde_json::to_string(&schedule).unwrap();
        let loaded: TaskCronSchedule = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.id, "s1");
        assert_eq!(loaded.name, "My Schedule");
        assert_eq!(loaded.cron_expr, "30 14 * * 1");
        assert_eq!(loaded.duration_secs, Some(3600));
        assert!(loaded.enabled);
    }

    #[test]
    fn test_schedule_clone() {
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        let cloned = schedule.clone();
        assert_eq!(cloned.id, schedule.id);
        assert_eq!(cloned.cron_expr, schedule.cron_expr);
    }

    #[test]
    fn test_schedule_debug() {
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        let debug = format!("{:?}", schedule);
        assert!(debug.contains("TaskCronSchedule"));
        assert!(debug.contains("s1"));
    }

    // ─── Scheduler Clone / Debug ────────────────────────────────────

    #[test]
    fn test_scheduler_clone() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        let cloned = scheduler.clone();
        assert!(cloned.get_schedule("task1").is_some());
        assert_eq!(cloned.config().enabled, scheduler.config().enabled);
    }

    #[test]
    fn test_scheduler_clone_independence() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("task1", schedule).unwrap();

        let mut cloned = scheduler.clone();
        cloned.remove_schedule("task1").unwrap();

        // Original still has it
        assert!(scheduler.get_schedule("task1").is_some());
        // Clone doesn't
        assert!(cloned.get_schedule("task1").is_none());
    }

    #[test]
    fn test_scheduler_debug() {
        let scheduler = TaskCronScheduler::new();
        let debug = format!("{:?}", scheduler);
        assert!(debug.contains("TaskCronScheduler"));
    }

    // ─── Unicode ────────────────────────────────────────────────────

    #[test]
    fn test_unicode_task_id() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "中文任务".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("任务-α", schedule).unwrap();
        assert!(scheduler.get_schedule("任务-α").is_some());
        assert_eq!(scheduler.get_schedule("任务-α").unwrap().name, "中文任务");
    }

    #[test]
    fn test_unicode_schedule_name() {
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "🌙 夜间下载".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        assert_eq!(schedule.name, "🌙 夜间下载");
    }

    #[test]
    fn test_unicode_persistence() {
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "日本語タスク".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        scheduler.add_schedule("任务-🚀", schedule).unwrap();

        let data = scheduler.to_data();
        let json = serde_json::to_string(&data).unwrap();
        let loaded: TaskCronSchedulerData = serde_json::from_str(&json).unwrap();
        assert!(loaded.schedules.contains_key("任务-🚀"));
        assert_eq!(loaded.schedules["任务-🚀"].name, "日本語タスク");
    }

    // ─── Complex workflows ──────────────────────────────────────────

    #[test]
    fn test_full_lifecycle() {
        let mut scheduler = TaskCronScheduler::new();

        // Add schedules
        let s1 = TaskCronSchedule::new(
            "s1".to_string(),
            "Morning".to_string(),
            "0 8 * * *".to_string(),
        )
        .unwrap()
        .with_duration(3600);
        let s2 = TaskCronSchedule::new(
            "s2".to_string(),
            "Night".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap()
        .with_duration(7200);

        scheduler.add_schedule("task1", s1).unwrap();
        scheduler.add_schedule("task2", s2).unwrap();

        // Verify
        assert_eq!(scheduler.list_schedules().len(), 2);
        assert_eq!(scheduler.summary().total_schedules, 2);

        // Disable one
        scheduler.set_schedule_enabled("task1", false).unwrap();
        assert_eq!(scheduler.summary().enabled_schedules, 1);
        assert_eq!(scheduler.summary().disabled_schedules, 1);

        // Remove one
        scheduler.remove_schedule("task1").unwrap();
        assert_eq!(scheduler.list_schedules().len(), 1);

        // to_data roundtrip
        let data = scheduler.to_data();
        let restored = TaskCronScheduler::from_data(data);
        assert_eq!(restored.list_schedules().len(), 1);
        assert!(restored.get_schedule("task2").is_some());
    }

    #[test]
    fn test_multiple_schedules_independent() {
        let mut scheduler = TaskCronScheduler::new();

        for i in 0..10 {
            let schedule = TaskCronSchedule::new(
                format!("s{}", i),
                format!("Schedule {}", i),
                format!("{} * * * *", i % 60).as_str().to_string(),
            )
            .unwrap();
            scheduler
                .add_schedule(&format!("task{}", i), schedule)
                .unwrap();
        }

        assert_eq!(scheduler.list_schedules().len(), 10);

        // Disable even-numbered tasks
        for i in (0..10).step_by(2) {
            scheduler
                .set_schedule_enabled(&format!("task{}", i), false)
                .unwrap();
        }

        let summary = scheduler.summary();
        assert_eq!(summary.enabled_schedules, 5);
        assert_eq!(summary.disabled_schedules, 5);
    }

    #[tokio::test]
    async fn test_full_persistence_lifecycle() {
        let dir = tempfile::tempdir().unwrap();

        // Create and populate
        let mut scheduler = TaskCronScheduler::new();
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Daily Backup".to_string(),
            "0 3 * * *".to_string(),
        )
        .unwrap()
        .with_duration(1800);
        scheduler.add_schedule("backup-task", schedule).unwrap();

        // Save
        scheduler.save(dir.path()).await.unwrap();

        // Load
        let loaded = TaskCronScheduler::load(dir.path()).await.unwrap();
        assert!(loaded.get_schedule("backup-task").is_some());
        let s = loaded.get_schedule("backup-task").unwrap();
        assert_eq!(s.name, "Daily Backup");
        assert_eq!(s.duration_secs, Some(1800));

        // Modify and save again
        let mut loaded = loaded;
        loaded.set_schedule_enabled("backup-task", false).unwrap();
        loaded.save(dir.path()).await.unwrap();

        // Reload
        let reloaded = TaskCronScheduler::load(dir.path()).await.unwrap();
        assert!(!reloaded.get_schedule("backup-task").unwrap().enabled);
    }

    // ─── Cron expression edge cases ─────────────────────────────────

    #[test]
    fn test_cron_midnight() {
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Midnight".to_string(),
            "0 0 * * *".to_string(),
        )
        .unwrap();

        let midnight = Local.with_ymd_and_hms(2026, 8, 12, 0, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(midnight).unwrap());

        let not_midnight = Local.with_ymd_and_hms(2026, 8, 12, 0, 1, 0).unwrap();
        assert!(!schedule.should_trigger_at(not_midnight).unwrap());
    }

    #[test]
    fn test_cron_end_of_day() {
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "End of day".to_string(),
            "59 23 * * *".to_string(),
        )
        .unwrap();

        let eod = Local.with_ymd_and_hms(2026, 8, 12, 23, 59, 0).unwrap();
        assert!(schedule.should_trigger_at(eod).unwrap());
    }

    #[test]
    fn test_cron_range_dow() {
        // Weekdays only (Mon-Fri = 1-5)
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Weekdays".to_string(),
            "0 9 * * 1-5".to_string(),
        )
        .unwrap();

        // 2026-08-10 is Monday
        let monday = Local.with_ymd_and_hms(2026, 8, 10, 9, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(monday).unwrap());

        // 2026-08-14 is Friday
        let friday = Local.with_ymd_and_hms(2026, 8, 14, 9, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(friday).unwrap());

        // 2026-08-15 is Saturday
        let saturday = Local.with_ymd_and_hms(2026, 8, 15, 9, 0, 0).unwrap();
        assert!(!schedule.should_trigger_at(saturday).unwrap());
    }

    #[test]
    fn test_cron_step_minutes() {
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Every 15 min".to_string(),
            "*/15 * * * *".to_string(),
        )
        .unwrap();

        let at_0 = Local.with_ymd_and_hms(2026, 8, 12, 10, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(at_0).unwrap());

        let at_15 = Local.with_ymd_and_hms(2026, 8, 12, 10, 15, 0).unwrap();
        assert!(schedule.should_trigger_at(at_15).unwrap());

        let at_7 = Local.with_ymd_and_hms(2026, 8, 12, 10, 7, 0).unwrap();
        assert!(!schedule.should_trigger_at(at_7).unwrap());
    }

    #[test]
    fn test_cron_list_values() {
        let schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Specific hours".to_string(),
            "0 8,12,18 * * *".to_string(),
        )
        .unwrap();

        let at_8 = Local.with_ymd_and_hms(2026, 8, 12, 8, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(at_8).unwrap());

        let at_12 = Local.with_ymd_and_hms(2026, 8, 12, 12, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(at_12).unwrap());

        let at_18 = Local.with_ymd_and_hms(2026, 8, 12, 18, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(at_18).unwrap());

        let at_10 = Local.with_ymd_and_hms(2026, 8, 12, 10, 0, 0).unwrap();
        assert!(!schedule.should_trigger_at(at_10).unwrap());
    }

    // ─── Boundary: large number of schedules ────────────────────────

    #[test]
    fn test_large_number_of_schedules() {
        let mut scheduler = TaskCronScheduler::new();
        for i in 0..100 {
            let schedule = TaskCronSchedule::new(
                format!("s{}", i),
                format!("Schedule {}", i),
                "0 2 * * *".to_string(),
            )
            .unwrap();
            scheduler
                .add_schedule(&format!("task{}", i), schedule)
                .unwrap();
        }
        assert_eq!(scheduler.list_schedules().len(), 100);
        assert_eq!(scheduler.summary().total_schedules, 100);
    }

    // ─── Error message quality ──────────────────────────────────────

    #[test]
    fn test_error_message_contains_task_id() {
        let err = TaskCronSchedulerError::NoSchedule("my-task-123".to_string());
        assert!(err.to_string().contains("my-task-123"));
    }

    #[test]
    fn test_error_message_contains_cron_expr() {
        let err = TaskCronSchedulerError::InvalidCron("bad expression".to_string());
        assert!(err.to_string().contains("bad expression"));
    }

    // ─── Data extra fields ignored ──────────────────────────────────

    #[test]
    fn test_config_extra_fields_ignored() {
        let json = r#"{"enabled":true,"check_interval_secs":30,"extra_field":"ignored"}"#;
        let config: TaskCronSchedulerConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.check_interval_secs, 30);
    }

    #[test]
    fn test_data_extra_fields_ignored() {
        let json =
            r#"{"config":{"enabled":true,"check_interval_secs":60},"schedules":{},"extra":42}"#;
        let data: TaskCronSchedulerData = serde_json::from_str(json).unwrap();
        assert!(data.config.enabled);
        assert!(data.schedules.is_empty());
    }

    // ─── Scheduler with custom config ───────────────────────────────

    #[test]
    fn test_scheduler_with_custom_config() {
        let mut scheduler = TaskCronScheduler::new();
        scheduler.set_config(TaskCronSchedulerConfig {
            enabled: true,
            check_interval_secs: 30,
        });
        assert_eq!(scheduler.config().check_interval_secs, 30);
    }

    // ─── should_trigger_at with invalid cron ────────────────────────

    #[test]
    fn test_should_trigger_at_invalid_cron() {
        let mut schedule = TaskCronSchedule::new(
            "s1".to_string(),
            "Test".to_string(),
            "0 2 * * *".to_string(),
        )
        .unwrap();
        // Corrupt the cron expression
        schedule.cron_expr = "invalid".to_string();
        let time = Local.with_ymd_and_hms(2026, 8, 12, 2, 0, 0).unwrap();
        assert!(schedule.should_trigger_at(time).is_err());
    }
}
