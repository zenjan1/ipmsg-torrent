//! Per-Task Download Schedule Windows
//!
//! Allows each download task to have specific time windows when it's allowed to run.
//! For example, large downloads can be scheduled to run only at night (e.g., 22:00-08:00).
//! The scheduler checks these windows and only starts/resumes tasks during their allowed times.
//!
//! Features:
//! - Per-task time windows with start/end times
//! - Day-of-week filtering (e.g., only weekdays, only weekends)
//! - Priority-based bypass (high-priority tasks can ignore schedule windows)
//! - Automatic pause/resume based on schedule
//! - Persistence to disk

use chrono::{DateTime, Datelike, Local, Timelike, Weekday};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Error type for task schedule window operations.
#[derive(Debug, thiserror::Error)]
pub enum TaskScheduleWindowError {
    #[error("task {0} has no schedule window")]
    NoSchedule(String),
    #[error("invalid schedule window: {0}")]
    InvalidSchedule(String),
    #[error("persistence error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Time window for when a task is allowed to download.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleWindow {
    /// Unique window identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Start hour (0-23).
    pub start_hour: u32,
    /// Start minute (0-59).
    pub start_minute: u32,
    /// End hour (0-23).
    pub end_hour: u32,
    /// End minute (0-59).
    pub end_minute: u32,
    /// Days of week when this window applies (empty = every day).
    pub days_of_week: Vec<Weekday>,
    /// Whether this window is enabled.
    pub enabled: bool,
}

impl ScheduleWindow {
    /// Create a new schedule window.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        start_hour: u32,
        start_minute: u32,
        end_hour: u32,
        end_minute: u32,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            start_hour,
            start_minute,
            end_hour,
            end_minute,
            days_of_week: Vec::new(),
            enabled: true,
        }
    }

    /// Set days of week for this window.
    pub fn with_days(mut self, days: Vec<Weekday>) -> Self {
        self.days_of_week = days;
        self
    }

    /// Check if this window applies at the given time.
    pub fn applies_at(&self, time: DateTime<Local>) -> bool {
        if !self.enabled {
            return false;
        }

        // Check day of week filter
        if !self.days_of_week.is_empty() && !self.days_of_week.contains(&time.weekday()) {
            return false;
        }

        // Check time window
        let current_minutes = time.hour() * 60 + time.minute();
        let start_minutes = self.start_hour * 60 + self.start_minute;
        let end_minutes = self.end_hour * 60 + self.end_minute;

        if start_minutes <= end_minutes {
            // Normal window (e.g., 08:00-18:00)
            current_minutes >= start_minutes && current_minutes < end_minutes
        } else {
            // Overnight window (e.g., 22:00-08:00)
            current_minutes >= start_minutes || current_minutes < end_minutes
        }
    }
}

/// Configuration for task schedule windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskScheduleWindowsConfig {
    /// Whether schedule windows are enabled globally.
    pub enabled: bool,
    /// Whether high-priority tasks bypass schedule windows.
    pub priority_bypass: bool,
    /// Schedule windows per task (task_id -> Vec<ScheduleWindow>).
    pub task_windows: HashMap<String, Vec<ScheduleWindow>>,
}

impl Default for TaskScheduleWindowsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            priority_bypass: true,
            task_windows: HashMap::new(),
        }
    }
}

/// Data structure for persisting task schedule windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskScheduleWindowsData {
    /// Task ID.
    pub task_id: String,
    /// Schedule windows for this task.
    pub windows: Vec<ScheduleWindow>,
}

/// Manager for per-task download schedule windows.
#[derive(Debug, Clone)]
pub struct TaskScheduleWindowsManager {
    config: TaskScheduleWindowsConfig,
}

impl TaskScheduleWindowsManager {
    /// Create a new manager with default configuration.
    pub fn new() -> Self {
        Self {
            config: TaskScheduleWindowsConfig::default(),
        }
    }

    /// Create a new manager from existing configuration.
    pub fn from_config(config: TaskScheduleWindowsConfig) -> Self {
        Self { config }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &TaskScheduleWindowsConfig {
        &self.config
    }

    /// Set the configuration.
    pub fn set_config(&mut self, config: TaskScheduleWindowsConfig) {
        self.config = config;
    }

    /// Enable or disable schedule windows globally.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    /// Set whether high-priority tasks bypass schedule windows.
    pub fn set_priority_bypass(&mut self, bypass: bool) {
        self.config.priority_bypass = bypass;
    }

    /// Add a schedule window to a task.
    pub fn add_window(&mut self, task_id: &str, window: ScheduleWindow) {
        self.config
            .task_windows
            .entry(task_id.to_string())
            .or_default()
            .push(window);
    }

    /// Remove a schedule window from a task.
    pub fn remove_window(&mut self, task_id: &str, window_id: &str) -> bool {
        let mut removed = false;
        if let Some(windows) = self.config.task_windows.get_mut(task_id) {
            let initial_len = windows.len();
            windows.retain(|w| w.id != window_id);
            removed = windows.len() < initial_len;
        }
        if removed
            && self
                .config
                .task_windows
                .get(task_id)
                .is_some_and(|w| w.is_empty())
        {
            self.config.task_windows.remove(task_id);
        }
        removed
    }

    /// Get all schedule windows for a task.
    pub fn get_windows(&self, task_id: &str) -> Option<&Vec<ScheduleWindow>> {
        self.config.task_windows.get(task_id)
    }

    /// Get all task schedule windows.
    pub fn get_all_windows(&self) -> &HashMap<String, Vec<ScheduleWindow>> {
        &self.config.task_windows
    }

    /// Clear all schedule windows for a task.
    pub fn clear_task_windows(&mut self, task_id: &str) {
        self.config.task_windows.remove(task_id);
    }

    /// Clear all schedule windows for all tasks.
    pub fn clear_all(&mut self) {
        self.config.task_windows.clear();
    }

    /// Check if a task is allowed to download at the given time.
    /// Returns true if:
    /// - Schedule windows are disabled globally
    /// - Task has no schedule windows configured
    /// - Task has priority and priority_bypass is enabled
    /// - Current time falls within at least one enabled schedule window
    pub fn is_allowed_at(&self, task_id: &str, task_priority: i32, time: DateTime<Local>) -> bool {
        // Global disable check
        if !self.config.enabled {
            return true;
        }

        // Priority bypass
        if self.config.priority_bypass && task_priority > 0 {
            return true;
        }

        // No windows configured = always allowed
        let windows = match self.config.task_windows.get(task_id) {
            Some(w) => w,
            None => return true,
        };

        // Check if any enabled window applies
        windows.iter().any(|w| w.applies_at(time))
    }

    /// Check if a task is allowed to download right now.
    pub fn is_allowed_now(&self, task_id: &str, task_priority: i32) -> bool {
        self.is_allowed_at(task_id, task_priority, Local::now())
    }

    /// Get the next time a task will be allowed to download.
    /// Returns None if the task has no schedule windows or is always allowed.
    pub fn next_allowed_time(&self, task_id: &str, task_priority: i32) -> Option<DateTime<Local>> {
        // If always allowed, return None
        if !self.config.enabled {
            return None;
        }
        if self.config.priority_bypass && task_priority > 0 {
            return None;
        }

        let windows = self.config.task_windows.get(task_id)?;
        if windows.is_empty() {
            return None;
        }

        let now = Local::now();
        let mut earliest: Option<DateTime<Local>> = None;

        // Check each enabled window
        for window in windows.iter().filter(|w| w.enabled) {
            // Check if currently in window
            if window.applies_at(now) {
                return Some(now);
            }

            // Find next occurrence
            match (self.find_next_window_occurrence(window, now), earliest) {
                (Some(next), None) => earliest = Some(next),
                (Some(next), Some(e)) if next < e => earliest = Some(next),
                _ => {}
            }
        }

        earliest
    }

    /// Find the next occurrence of a schedule window.
    fn find_next_window_occurrence(
        &self,
        window: &ScheduleWindow,
        from: DateTime<Local>,
    ) -> Option<DateTime<Local>> {
        // Try next 7 days to find a matching day
        for day_offset in 0..7 {
            let candidate_date = from.date_naive() + chrono::Duration::days(day_offset as i64);
            let candidate = candidate_date
                .and_hms_opt(window.start_hour, window.start_minute, 0)?
                .and_local_timezone(Local)
                .earliest()?;

            // Skip if in the past
            if candidate <= from {
                continue;
            }

            // Check day of week filter
            if !window.days_of_week.is_empty()
                && !window.days_of_week.contains(&candidate.weekday())
            {
                continue;
            }

            return Some(candidate);
        }

        None
    }

    /// Load configuration from a file.
    pub async fn load_from_file(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<(), TaskScheduleWindowError> {
        let content = match fs::read_to_string(path.as_ref()).await {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let config: TaskScheduleWindowsConfig = serde_json::from_str(&content)?;
        self.config = config;
        Ok(())
    }

    /// Save configuration to a file atomically.
    pub async fn save_to_file(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(), TaskScheduleWindowError> {
        let content = serde_json::to_string_pretty(&self.config)?;
        let temp_path = path.as_ref().with_extension("tmp");
        fs::write(&temp_path, &content).await?;
        fs::rename(&temp_path, path.as_ref()).await?;
        Ok(())
    }
}

impl Default for TaskScheduleWindowsManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_time(hour: u32, minute: u32) -> DateTime<Local> {
        Local::now()
            .date_naive()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap()
    }

    fn make_time_on_date(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
    ) -> DateTime<Local> {
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap()
    }

    // ── ScheduleWindow creation ──────────────────────────────────────────

    #[test]
    fn test_schedule_window_creation() {
        let window = ScheduleWindow::new("night", "Night Downloads", 22, 0, 8, 0);
        assert_eq!(window.id, "night");
        assert_eq!(window.name, "Night Downloads");
        assert_eq!(window.start_hour, 22);
        assert_eq!(window.start_minute, 0);
        assert_eq!(window.end_hour, 8);
        assert_eq!(window.end_minute, 0);
        assert!(window.enabled);
        assert!(window.days_of_week.is_empty());
    }

    #[test]
    fn test_schedule_window_with_days() {
        let window = ScheduleWindow::new("weekday", "Weekday Only", 9, 0, 17, 0).with_days(vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
        ]);
        assert_eq!(window.days_of_week.len(), 5);
    }

    #[test]
    fn test_schedule_window_boundary_hours() {
        let window = ScheduleWindow::new("full", "Full Day", 0, 0, 23, 59);
        assert_eq!(window.start_hour, 0);
        assert_eq!(window.start_minute, 0);
        assert_eq!(window.end_hour, 23);
        assert_eq!(window.end_minute, 59);
    }

    #[test]
    fn test_schedule_window_unicode_fields() {
        let window = ScheduleWindow::new("夜間", "中文窗口", 22, 0, 8, 0);
        assert_eq!(window.id, "夜間");
        assert_eq!(window.name, "中文窗口");
    }

    #[test]
    fn test_schedule_window_emoji_fields() {
        let window = ScheduleWindow::new("🌙", "🌃 Night", 22, 0, 8, 0);
        assert_eq!(window.id, "🌙");
        assert_eq!(window.name, "🌃 Night");
    }

    // ── ScheduleWindow Clone/Debug ───────────────────────────────────────

    #[test]
    fn test_schedule_window_clone() {
        let window = ScheduleWindow::new("w1", "Window", 8, 0, 18, 0)
            .with_days(vec![Weekday::Mon, Weekday::Fri]);
        let cloned = window.clone();
        assert_eq!(cloned.id, window.id);
        assert_eq!(cloned.name, window.name);
        assert_eq!(cloned.start_hour, window.start_hour);
        assert_eq!(cloned.end_hour, window.end_hour);
        assert_eq!(cloned.days_of_week, window.days_of_week);
        assert_eq!(cloned.enabled, window.enabled);
    }

    #[test]
    fn test_schedule_window_clone_independence() {
        let mut window = ScheduleWindow::new("w1", "Window", 8, 0, 18, 0);
        let cloned = window.clone();
        window.enabled = false;
        assert!(cloned.enabled);
    }

    #[test]
    fn test_schedule_window_debug() {
        let window = ScheduleWindow::new("w1", "Test Window", 8, 0, 18, 0);
        let debug = format!("{:?}", window);
        assert!(debug.contains("w1"));
        assert!(debug.contains("Test Window"));
    }

    // ── ScheduleWindow serde ─────────────────────────────────────────────

    #[test]
    fn test_schedule_window_serde_roundtrip() {
        let window = ScheduleWindow::new("night", "Night Downloads", 22, 0, 8, 0)
            .with_days(vec![Weekday::Mon, Weekday::Fri]);
        let json = serde_json::to_string(&window).unwrap();
        let deserialized: ScheduleWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, window.id);
        assert_eq!(deserialized.name, window.name);
        assert_eq!(deserialized.start_hour, window.start_hour);
        assert_eq!(deserialized.start_minute, window.start_minute);
        assert_eq!(deserialized.end_hour, window.end_hour);
        assert_eq!(deserialized.end_minute, window.end_minute);
        assert_eq!(deserialized.enabled, window.enabled);
        assert_eq!(deserialized.days_of_week, window.days_of_week);
    }

    #[test]
    fn test_schedule_window_serde_extra_fields_ignored() {
        let json = r#"{"id":"w1","name":"W","start_hour":8,"start_minute":0,"end_hour":18,"end_minute":0,"days_of_week":[],"enabled":true,"extra_field":"ignored"}"#;
        let window: ScheduleWindow = serde_json::from_str(json).unwrap();
        assert_eq!(window.id, "w1");
    }

    #[test]
    fn test_schedule_window_pretty_serde() {
        let window = ScheduleWindow::new("w1", "Window", 8, 0, 18, 0);
        let pretty = serde_json::to_string_pretty(&window).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: ScheduleWindow = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.id, window.id);
    }

    #[test]
    fn test_schedule_window_serde_unicode() {
        let window = ScheduleWindow::new("中文id", "日本語ウィンドウ", 10, 30, 20, 30);
        let json = serde_json::to_string(&window).unwrap();
        let deserialized: ScheduleWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "中文id");
        assert_eq!(deserialized.name, "日本語ウィンドウ");
    }

    #[test]
    fn test_schedule_window_serde_all_weekdays() {
        let all_days = vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
            Weekday::Sat,
            Weekday::Sun,
        ];
        let window =
            ScheduleWindow::new("all", "All Week", 0, 0, 23, 59).with_days(all_days.clone());
        let json = serde_json::to_string(&window).unwrap();
        let deserialized: ScheduleWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.days_of_week.len(), 7);
    }

    // ── ScheduleWindow applies_at ────────────────────────────────────────

    #[test]
    fn test_normal_window_applies() {
        let window = ScheduleWindow::new("day", "Daytime", 8, 0, 18, 0);

        // Before window
        assert!(!window.applies_at(make_time(7, 59)));

        // At start
        assert!(window.applies_at(make_time(8, 0)));

        // During window
        assert!(window.applies_at(make_time(12, 30)));

        // At end
        assert!(!window.applies_at(make_time(18, 0)));

        // After window
        assert!(!window.applies_at(make_time(19, 0)));
    }

    #[test]
    fn test_overnight_window_applies() {
        let window = ScheduleWindow::new("night", "Nighttime", 22, 0, 8, 0);

        // Before window
        assert!(!window.applies_at(make_time(21, 59)));

        // At start
        assert!(window.applies_at(make_time(22, 0)));

        // During window (late night)
        assert!(window.applies_at(make_time(23, 30)));

        // During window (early morning)
        assert!(window.applies_at(make_time(2, 0)));

        // At end
        assert!(!window.applies_at(make_time(8, 0)));

        // After window
        assert!(!window.applies_at(make_time(10, 0)));
    }

    #[test]
    fn test_disabled_window() {
        let mut window = ScheduleWindow::new("day", "Daytime", 8, 0, 18, 0);
        window.enabled = false;

        assert!(!window.applies_at(make_time(12, 0)));
    }

    #[test]
    fn test_window_midnight_boundary() {
        let window = ScheduleWindow::new("midnight", "Midnight", 0, 0, 1, 0);
        assert!(window.applies_at(make_time(0, 0)));
        assert!(window.applies_at(make_time(0, 30)));
        assert!(!window.applies_at(make_time(1, 0)));
        assert!(!window.applies_at(make_time(23, 59)));
    }

    #[test]
    fn test_window_minute_precision() {
        let window = ScheduleWindow::new("precise", "Precise", 10, 15, 14, 45);
        assert!(!window.applies_at(make_time(10, 14)));
        assert!(window.applies_at(make_time(10, 15)));
        assert!(window.applies_at(make_time(12, 30)));
        assert!(window.applies_at(make_time(14, 44)));
        assert!(!window.applies_at(make_time(14, 45)));
    }

    #[test]
    fn test_window_same_start_end() {
        // Zero-length window: start == end → start_minutes == end_minutes
        // Normal path: start <= end → current >= start && current < end
        // Only matches exactly at start_minute
        let window = ScheduleWindow::new("zero", "Zero", 12, 0, 12, 0);
        // start_minutes == end_minutes → normal path: current >= start && current < end
        // 12:00 >= 720 && 12:00 < 720 → false
        assert!(!window.applies_at(make_time(12, 0)));
    }

    #[test]
    fn test_window_overnight_exact_midnight() {
        let window = ScheduleWindow::new("overnight", "Overnight", 23, 0, 1, 0);
        assert!(!window.applies_at(make_time(22, 59)));
        assert!(window.applies_at(make_time(23, 0)));
        assert!(window.applies_at(make_time(0, 0)));
        assert!(window.applies_at(make_time(0, 30)));
        assert!(!window.applies_at(make_time(1, 0)));
    }

    // ── ScheduleWindow applies_at with days_of_week ──────────────────────

    #[test]
    fn test_window_with_matching_day() {
        // 2026-08-16 is a Sunday
        let window =
            ScheduleWindow::new("sun", "Sunday", 8, 0, 18, 0).with_days(vec![Weekday::Sun]);
        let time = make_time_on_date(2026, 8, 16, 12, 0);
        assert!(window.applies_at(time));
    }

    #[test]
    fn test_window_with_non_matching_day() {
        // 2026-08-17 is a Monday
        let window =
            ScheduleWindow::new("sun", "Sunday Only", 8, 0, 18, 0).with_days(vec![Weekday::Sun]);
        let time = make_time_on_date(2026, 8, 17, 12, 0);
        assert!(!window.applies_at(time));
    }

    #[test]
    fn test_window_empty_days_means_all_days() {
        let window = ScheduleWindow::new("all", "All Days", 8, 0, 18, 0);
        // No days_of_week set → applies to all days
        let sunday = make_time_on_date(2026, 8, 16, 12, 0);
        let monday = make_time_on_date(2026, 8, 17, 12, 0);
        assert!(window.applies_at(sunday));
        assert!(window.applies_at(monday));
    }

    #[test]
    fn test_window_weekday_filter() {
        let window = ScheduleWindow::new("weekday", "Weekdays", 8, 0, 18, 0).with_days(vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
        ]);
        // 2026-08-16 is Sunday → should not match
        let sunday = make_time_on_date(2026, 8, 16, 12, 0);
        assert!(!window.applies_at(sunday));
        // 2026-08-17 is Monday → should match
        let monday = make_time_on_date(2026, 8, 17, 12, 0);
        assert!(window.applies_at(monday));
    }

    #[test]
    fn test_window_disabled_with_matching_day() {
        let mut window =
            ScheduleWindow::new("sun", "Sunday", 8, 0, 18, 0).with_days(vec![Weekday::Sun]);
        window.enabled = false;
        let time = make_time_on_date(2026, 8, 16, 12, 0);
        assert!(!window.applies_at(time));
    }

    // ── TaskScheduleWindowsConfig serde ──────────────────────────────────

    #[test]
    fn test_config_serde_roundtrip_default() {
        let config = TaskScheduleWindowsConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TaskScheduleWindowsConfig = serde_json::from_str(&json).unwrap();
        assert!(deserialized.enabled);
        assert!(deserialized.priority_bypass);
        assert!(deserialized.task_windows.is_empty());
    }

    #[test]
    fn test_config_serde_roundtrip_custom() {
        let mut config = TaskScheduleWindowsConfig::default();
        config.enabled = false;
        config.priority_bypass = false;
        let window = ScheduleWindow::new("w1", "W1", 8, 0, 18, 0);
        config
            .task_windows
            .insert("task1".to_string(), vec![window]);
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TaskScheduleWindowsConfig = serde_json::from_str(&json).unwrap();
        assert!(!deserialized.enabled);
        assert!(!deserialized.priority_bypass);
        assert!(deserialized.task_windows.contains_key("task1"));
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"priority_bypass":true,"task_windows":{},"extra":42}"#;
        let config: TaskScheduleWindowsConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = TaskScheduleWindowsConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: TaskScheduleWindowsConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
    }

    // ── TaskScheduleWindowsConfig traits ─────────────────────────────────

    #[test]
    fn test_config_clone() {
        let config = TaskScheduleWindowsConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.priority_bypass, config.priority_bypass);
    }

    #[test]
    fn test_config_clone_independence() {
        let mut config = TaskScheduleWindowsConfig::default();
        let mut cloned = config.clone();
        cloned.enabled = false;
        assert!(config.enabled);
    }

    #[test]
    fn test_config_debug() {
        let config = TaskScheduleWindowsConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("enabled"));
    }

    #[test]
    fn test_config_default_values() {
        let config = TaskScheduleWindowsConfig::default();
        assert!(config.enabled);
        assert!(config.priority_bypass);
        assert!(config.task_windows.is_empty());
    }

    // ── TaskScheduleWindowsData serde ────────────────────────────────────

    #[test]
    fn test_data_serde_roundtrip() {
        let data = TaskScheduleWindowsData {
            task_id: "task1".to_string(),
            windows: vec![
                ScheduleWindow::new("w1", "W1", 8, 0, 18, 0),
                ScheduleWindow::new("w2", "W2", 22, 0, 8, 0),
            ],
        };
        let json = serde_json::to_string(&data).unwrap();
        let deserialized: TaskScheduleWindowsData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task1");
        assert_eq!(deserialized.windows.len(), 2);
    }

    #[test]
    fn test_data_serde_unicode_task_id() {
        let data = TaskScheduleWindowsData {
            task_id: "中文任务".to_string(),
            windows: vec![],
        };
        let json = serde_json::to_string(&data).unwrap();
        let deserialized: TaskScheduleWindowsData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "中文任务");
    }

    #[test]
    fn test_data_clone_debug() {
        let data = TaskScheduleWindowsData {
            task_id: "t1".to_string(),
            windows: vec![ScheduleWindow::new("w", "W", 0, 0, 23, 59)],
        };
        let cloned = data.clone();
        assert_eq!(cloned.task_id, data.task_id);
        let debug = format!("{:?}", data);
        assert!(debug.contains("t1"));
    }

    // ── TaskScheduleWindowError Display ──────────────────────────────────

    #[test]
    fn test_error_no_schedule_display() {
        let err = TaskScheduleWindowError::NoSchedule("task1".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("task1"));
        assert!(msg.contains("no schedule window"));
    }

    #[test]
    fn test_error_invalid_schedule_display() {
        let err = TaskScheduleWindowError::InvalidSchedule("bad time".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("bad time"));
    }

    #[test]
    fn test_error_io_display() {
        let err = TaskScheduleWindowError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("file not found"));
    }

    #[test]
    fn test_error_serialize_display() {
        let bad_json = serde_json::from_str::<TaskScheduleWindowsConfig>("invalid").unwrap_err();
        let err = TaskScheduleWindowError::Serialize(bad_json);
        let msg = format!("{}", err);
        assert!(!msg.is_empty());
    }

    #[test]
    fn test_error_debug() {
        let err = TaskScheduleWindowError::NoSchedule("t1".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("NoSchedule"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: TaskScheduleWindowError = io_err.into();
        let msg = format!("{}", err);
        assert!(msg.contains("denied"));
    }

    #[test]
    fn test_error_from_serde() {
        let serde_err = serde_json::from_str::<TaskScheduleWindowsConfig>("bad").unwrap_err();
        let err: TaskScheduleWindowError = serde_err.into();
        let msg = format!("{}", err);
        assert!(!msg.is_empty());
    }

    // ── Manager construction ─────────────────────────────────────────────

    #[test]
    fn test_manager_new() {
        let manager = TaskScheduleWindowsManager::new();
        assert!(manager.config().enabled);
        assert!(manager.config().priority_bypass);
        assert!(manager.get_all_windows().is_empty());
    }

    #[test]
    fn test_manager_default_equals_new() {
        let new = TaskScheduleWindowsManager::new();
        let default = TaskScheduleWindowsManager::default();
        assert_eq!(new.config().enabled, default.config().enabled);
        assert_eq!(
            new.config().priority_bypass,
            default.config().priority_bypass
        );
    }

    #[test]
    fn test_manager_from_config() {
        let mut config = TaskScheduleWindowsConfig::default();
        config.enabled = false;
        config.priority_bypass = false;
        let manager = TaskScheduleWindowsManager::from_config(config);
        assert!(!manager.config().enabled);
        assert!(!manager.config().priority_bypass);
    }

    #[test]
    fn test_manager_from_config_preserves_windows() {
        let mut config = TaskScheduleWindowsConfig::default();
        let window = ScheduleWindow::new("w1", "W1", 8, 0, 18, 0);
        config
            .task_windows
            .insert("task1".to_string(), vec![window]);
        let manager = TaskScheduleWindowsManager::from_config(config);
        assert_eq!(manager.get_windows("task1").unwrap().len(), 1);
    }

    #[test]
    fn test_manager_set_config() {
        let mut manager = TaskScheduleWindowsManager::new();
        let mut config = TaskScheduleWindowsConfig::default();
        config.enabled = false;
        manager.set_config(config);
        assert!(!manager.config().enabled);
    }

    #[test]
    fn test_manager_set_enabled() {
        let mut manager = TaskScheduleWindowsManager::new();
        assert!(manager.config().enabled);
        manager.set_enabled(false);
        assert!(!manager.config().enabled);
        manager.set_enabled(true);
        assert!(manager.config().enabled);
    }

    #[test]
    fn test_manager_set_priority_bypass() {
        let mut manager = TaskScheduleWindowsManager::new();
        assert!(manager.config().priority_bypass);
        manager.set_priority_bypass(false);
        assert!(!manager.config().priority_bypass);
    }

    #[test]
    fn test_manager_config_reference() {
        let manager = TaskScheduleWindowsManager::new();
        let config_ref = manager.config();
        assert!(config_ref.enabled);
    }

    // ── Manager Clone/Debug ──────────────────────────────────────────────

    #[test]
    fn test_manager_clone() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.add_window("task1", ScheduleWindow::new("w1", "W1", 8, 0, 18, 0));
        let cloned = manager.clone();
        assert!(cloned.get_windows("task1").is_some());
    }

    #[test]
    fn test_manager_clone_independence() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.add_window("task1", ScheduleWindow::new("w1", "W1", 8, 0, 18, 0));
        let mut cloned = manager.clone();
        cloned.clear_all();
        assert!(manager.get_windows("task1").is_some());
    }

    #[test]
    fn test_manager_debug() {
        let manager = TaskScheduleWindowsManager::new();
        let debug = format!("{:?}", manager);
        assert!(debug.contains("TaskScheduleWindowsManager"));
    }

    // ── Manager window operations ────────────────────────────────────────

    #[test]
    fn test_manager_add_remove_windows() {
        let mut manager = TaskScheduleWindowsManager::new();

        let window1 = ScheduleWindow::new("night1", "Night 1", 22, 0, 8, 0);
        let window2 = ScheduleWindow::new("night2", "Night 2", 23, 0, 7, 0);

        manager.add_window("task1", window1.clone());
        manager.add_window("task1", window2.clone());

        assert_eq!(manager.get_windows("task1").unwrap().len(), 2);

        manager.remove_window("task1", "night1");
        assert_eq!(manager.get_windows("task1").unwrap().len(), 1);
        assert_eq!(manager.get_windows("task1").unwrap()[0].id, "night2");

        manager.remove_window("task1", "night2");
        assert!(manager.get_windows("task1").is_none());
    }

    #[test]
    fn test_manager_add_window_multiple_tasks() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.add_window("task1", ScheduleWindow::new("w1", "W1", 8, 0, 18, 0));
        manager.add_window("task2", ScheduleWindow::new("w2", "W2", 22, 0, 8, 0));
        assert!(manager.get_windows("task1").is_some());
        assert!(manager.get_windows("task2").is_some());
        assert_eq!(manager.get_all_windows().len(), 2);
    }

    #[test]
    fn test_manager_remove_window_nonexistent_task() {
        let mut manager = TaskScheduleWindowsManager::new();
        assert!(!manager.remove_window("nonexistent", "w1"));
    }

    #[test]
    fn test_manager_remove_window_nonexistent_id() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.add_window("task1", ScheduleWindow::new("w1", "W1", 8, 0, 18, 0));
        assert!(!manager.remove_window("task1", "nonexistent"));
        assert_eq!(manager.get_windows("task1").unwrap().len(), 1);
    }

    #[test]
    fn test_manager_get_windows_nonexistent() {
        let manager = TaskScheduleWindowsManager::new();
        assert!(manager.get_windows("nonexistent").is_none());
    }

    #[test]
    fn test_manager_get_all_windows() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.add_window("t1", ScheduleWindow::new("w1", "W1", 8, 0, 18, 0));
        manager.add_window("t2", ScheduleWindow::new("w2", "W2", 22, 0, 8, 0));
        let all = manager.get_all_windows();
        assert_eq!(all.len(), 2);
        assert!(all.contains_key("t1"));
        assert!(all.contains_key("t2"));
    }

    #[test]
    fn test_manager_clear_windows() {
        let mut manager = TaskScheduleWindowsManager::new();

        manager.add_window("task1", ScheduleWindow::new("w1", "Window 1", 8, 0, 12, 0));
        manager.add_window("task1", ScheduleWindow::new("w2", "Window 2", 14, 0, 18, 0));
        manager.add_window("task2", ScheduleWindow::new("w3", "Window 3", 20, 0, 22, 0));

        manager.clear_task_windows("task1");
        assert!(manager.get_windows("task1").is_none());
        assert!(manager.get_windows("task2").is_some());

        manager.clear_all();
        assert!(manager.get_windows("task2").is_none());
    }

    #[test]
    fn test_manager_clear_task_windows_nonexistent() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.clear_task_windows("nonexistent"); // should not panic
        assert!(manager.get_all_windows().is_empty());
    }

    #[test]
    fn test_manager_clear_all_empty() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.clear_all(); // should not panic
        assert!(manager.get_all_windows().is_empty());
    }

    // ── Manager is_allowed_at ────────────────────────────────────────────

    #[test]
    fn test_manager_is_allowed_no_windows() {
        let manager = TaskScheduleWindowsManager::new();

        // No windows = always allowed
        assert!(manager.is_allowed_at("task1", 0, make_time(12, 0)));
        assert!(manager.is_allowed_at("task1", 0, make_time(3, 0)));
    }

    #[test]
    fn test_manager_is_allowed_with_windows() {
        let mut manager = TaskScheduleWindowsManager::new();

        let window = ScheduleWindow::new("night", "Night Only", 22, 0, 8, 0);
        manager.add_window("task1", window);

        // During allowed time
        assert!(manager.is_allowed_at("task1", 0, make_time(23, 0)));
        assert!(manager.is_allowed_at("task1", 0, make_time(3, 0)));

        // Outside allowed time
        assert!(!manager.is_allowed_at("task1", 0, make_time(12, 0)));
        assert!(!manager.is_allowed_at("task1", 0, make_time(15, 0)));
    }

    #[test]
    fn test_manager_priority_bypass() {
        let mut manager = TaskScheduleWindowsManager::new();

        let window = ScheduleWindow::new("night", "Night Only", 22, 0, 8, 0);
        manager.add_window("task1", window);

        // High priority task (priority > 0) bypasses windows
        assert!(manager.is_allowed_at("task1", 1, make_time(12, 0)));
        assert!(manager.is_allowed_at("task1", 5, make_time(15, 0)));

        // Normal priority task (priority = 0) respects windows
        assert!(!manager.is_allowed_at("task1", 0, make_time(12, 0)));
    }

    #[test]
    fn test_manager_priority_bypass_disabled() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.set_priority_bypass(false);

        let window = ScheduleWindow::new("night", "Night Only", 22, 0, 8, 0);
        manager.add_window("task1", window);

        // High priority task still respects windows when bypass is disabled
        assert!(!manager.is_allowed_at("task1", 1, make_time(12, 0)));
    }

    #[test]
    fn test_manager_global_disable() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.set_enabled(false);

        let window = ScheduleWindow::new("night", "Night Only", 22, 0, 8, 0);
        manager.add_window("task1", window);

        // All tasks allowed when globally disabled
        assert!(manager.is_allowed_at("task1", 0, make_time(12, 0)));
        assert!(manager.is_allowed_at("task1", 0, make_time(23, 0)));
    }

    #[test]
    fn test_manager_is_allowed_negative_priority() {
        let mut manager = TaskScheduleWindowsManager::new();
        let window = ScheduleWindow::new("night", "Night Only", 22, 0, 8, 0);
        manager.add_window("task1", window);
        // Negative priority should NOT bypass (only > 0 bypasses)
        assert!(!manager.is_allowed_at("task1", -1, make_time(12, 0)));
    }

    #[test]
    fn test_manager_is_allowed_multiple_windows_any_match() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.add_window(
            "task1",
            ScheduleWindow::new("morning", "Morning", 6, 0, 9, 0),
        );
        manager.add_window(
            "task1",
            ScheduleWindow::new("evening", "Evening", 18, 0, 22, 0),
        );

        // In morning window
        assert!(manager.is_allowed_at("task1", 0, make_time(7, 0)));
        // In evening window
        assert!(manager.is_allowed_at("task1", 0, make_time(20, 0)));
        // In neither
        assert!(!manager.is_allowed_at("task1", 0, make_time(12, 0)));
    }

    #[test]
    fn test_manager_is_allowed_disabled_window_in_list() {
        let mut manager = TaskScheduleWindowsManager::new();
        let mut disabled = ScheduleWindow::new("disabled", "Disabled", 8, 0, 18, 0);
        disabled.enabled = false;
        manager.add_window("task1", disabled);
        // Only disabled windows → no match → not allowed
        assert!(!manager.is_allowed_at("task1", 0, make_time(12, 0)));
    }

    #[test]
    fn test_manager_is_allowed_nonexistent_task() {
        let manager = TaskScheduleWindowsManager::new();
        // No windows for task → always allowed
        assert!(manager.is_allowed_at("nonexistent", 0, make_time(12, 0)));
    }

    #[test]
    fn test_manager_is_allowed_unicode_task_id() {
        let mut manager = TaskScheduleWindowsManager::new();
        let window = ScheduleWindow::new("night", "Night", 22, 0, 8, 0);
        manager.add_window("中文任务", window);
        assert!(manager.is_allowed_at("中文任务", 0, make_time(23, 0)));
        assert!(!manager.is_allowed_at("中文任务", 0, make_time(12, 0)));
    }

    #[test]
    fn test_manager_is_allowed_emoji_task_id() {
        let mut manager = TaskScheduleWindowsManager::new();
        let window = ScheduleWindow::new("night", "Night", 22, 0, 8, 0);
        manager.add_window("🚀", window);
        assert!(manager.is_allowed_at("🚀", 0, make_time(23, 0)));
    }

    // ── Manager next_allowed_time ────────────────────────────────────────

    #[test]
    fn test_next_allowed_time_no_windows() {
        let manager = TaskScheduleWindowsManager::new();
        assert!(manager.next_allowed_time("task1", 0).is_none());
    }

    #[test]
    fn test_next_allowed_time_currently_in_window() {
        let mut manager = TaskScheduleWindowsManager::new();

        let window = ScheduleWindow::new("day", "Daytime", 8, 0, 18, 0);
        manager.add_window("task1", window);

        // Currently in window (assume test runs during daytime)
        let now = Local::now();
        if now.hour() >= 8 && now.hour() < 18 {
            let next = manager.next_allowed_time("task1", 0).unwrap();
            assert!(next <= now + chrono::Duration::seconds(1));
        }
    }

    #[test]
    fn test_next_allowed_time_global_disabled() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.set_enabled(false);
        let window = ScheduleWindow::new("night", "Night", 22, 0, 8, 0);
        manager.add_window("task1", window);
        assert!(manager.next_allowed_time("task1", 0).is_none());
    }

    #[test]
    fn test_next_allowed_time_priority_bypass() {
        let mut manager = TaskScheduleWindowsManager::new();
        let window = ScheduleWindow::new("night", "Night", 22, 0, 8, 0);
        manager.add_window("task1", window);
        assert!(manager.next_allowed_time("task1", 1).is_none());
    }

    #[test]
    fn test_next_allowed_time_empty_windows() {
        let mut manager = TaskScheduleWindowsManager::new();
        // Manually insert empty vec
        manager
            .config
            .task_windows
            .insert("task1".to_string(), vec![]);
        assert!(manager.next_allowed_time("task1", 0).is_none());
    }

    #[test]
    fn test_next_allowed_time_nonexistent_task() {
        let manager = TaskScheduleWindowsManager::new();
        assert!(manager.next_allowed_time("nonexistent", 0).is_none());
    }

    // ── Manager persistence ──────────────────────────────────────────────

    #[test]
    fn test_persistence_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = TaskScheduleWindowsManager::new();

            let mut window = ScheduleWindow::new("night", "Night Downloads", 22, 0, 8, 0);
            window.days_of_week = vec![Weekday::Mon, Weekday::Fri];
            manager.add_window("task1", window);
            manager.set_priority_bypass(false);

            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join("test_schedule_windows.json");

            manager.save_to_file(&path).await.unwrap();

            let mut loaded = TaskScheduleWindowsManager::new();
            loaded.load_from_file(&path).await.unwrap();

            assert_eq!(loaded.config().priority_bypass, false);
            assert!(loaded.get_windows("task1").is_some());
            let windows = loaded.get_windows("task1").unwrap();
            assert_eq!(windows.len(), 1);
            assert_eq!(windows[0].id, "night");
            assert_eq!(windows[0].days_of_week.len(), 2);

            // Cleanup
            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_load_missing_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = TaskScheduleWindowsManager::new();
            let path = std::env::temp_dir().join("nonexistent_schedule_windows.json");

            // Should succeed with default config
            assert!(manager.load_from_file(&path).await.is_ok());
            assert!(manager.get_all_windows().is_empty());
        });
    }

    #[test]
    fn test_save_creates_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let manager = TaskScheduleWindowsManager::new();
            let path = std::env::temp_dir().join("test_sw_create.json");
            let _ = tokio::fs::remove_file(&path).await;

            manager.save_to_file(&path).await.unwrap();
            assert!(tokio::fs::metadata(&path).await.is_ok());

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_save_overwrites_existing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = TaskScheduleWindowsManager::new();
            let path = std::env::temp_dir().join("test_sw_overwrite.json");

            // Save first version
            manager.save_to_file(&path).await.unwrap();

            // Add window and save again
            manager.add_window("task1", ScheduleWindow::new("w1", "W1", 8, 0, 18, 0));
            manager.save_to_file(&path).await.unwrap();

            let mut loaded = TaskScheduleWindowsManager::new();
            loaded.load_from_file(&path).await.unwrap();
            assert!(loaded.get_windows("task1").is_some());

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_save_no_tmp_leftover() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let manager = TaskScheduleWindowsManager::new();
            let path = std::env::temp_dir().join("test_sw_no_tmp.json");
            let tmp_path = path.with_extension("tmp");

            manager.save_to_file(&path).await.unwrap();

            assert!(tokio::fs::metadata(&tmp_path).await.is_err());

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_load_corrupt_json() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("test_sw_corrupt.json");
            tokio::fs::write(&path, "not valid json").await.unwrap();

            let mut manager = TaskScheduleWindowsManager::new();
            let result = manager.load_from_file(&path).await;
            assert!(result.is_err());

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_load_empty_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("test_sw_empty.json");
            tokio::fs::write(&path, "").await.unwrap();

            let mut manager = TaskScheduleWindowsManager::new();
            let result = manager.load_from_file(&path).await;
            assert!(result.is_err());

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_persistence_pretty_json() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = TaskScheduleWindowsManager::new();
            manager.add_window("task1", ScheduleWindow::new("w1", "W1", 8, 0, 18, 0));

            let path = std::env::temp_dir().join("test_sw_pretty.json");
            manager.save_to_file(&path).await.unwrap();

            let content = tokio::fs::read_to_string(&path).await.unwrap();
            assert!(content.contains('\n'));

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_persistence_unicode_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = TaskScheduleWindowsManager::new();
            let window = ScheduleWindow::new("夜間", "中文ウィンドウ", 22, 0, 8, 0);
            manager.add_window("🚀任务", window);

            let path = std::env::temp_dir().join("test_sw_unicode.json");
            manager.save_to_file(&path).await.unwrap();

            let mut loaded = TaskScheduleWindowsManager::new();
            loaded.load_from_file(&path).await.unwrap();

            let windows = loaded.get_windows("🚀任务").unwrap();
            assert_eq!(windows[0].id, "夜間");
            assert_eq!(windows[0].name, "中文ウィンドウ");

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_persistence_full_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = TaskScheduleWindowsManager::new();
            manager.set_enabled(false);
            manager.set_priority_bypass(false);

            let mut w1 = ScheduleWindow::new("morning", "朝", 6, 0, 9, 0);
            w1.days_of_week = vec![Weekday::Mon, Weekday::Wed, Weekday::Fri];
            manager.add_window("t1", w1);
            manager.add_window("t1", ScheduleWindow::new("night", "夜", 22, 0, 8, 0));
            manager.add_window("t2", ScheduleWindow::new("allday", "終日", 0, 0, 23, 59));

            let path = std::env::temp_dir().join("test_sw_full.json");
            manager.save_to_file(&path).await.unwrap();

            let mut loaded = TaskScheduleWindowsManager::new();
            loaded.load_from_file(&path).await.unwrap();

            assert!(!loaded.config().enabled);
            assert!(!loaded.config().priority_bypass);
            assert_eq!(loaded.get_windows("t1").unwrap().len(), 2);
            assert_eq!(loaded.get_windows("t2").unwrap().len(), 1);
            assert_eq!(loaded.get_windows("t1").unwrap()[0].days_of_week.len(), 3);

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    // ── Complex workflows ────────────────────────────────────────────────

    #[test]
    fn test_full_lifecycle() {
        let mut manager = TaskScheduleWindowsManager::new();

        // Add windows
        let w1 = ScheduleWindow::new("night", "Night", 22, 0, 8, 0);
        let w2 = ScheduleWindow::new("morning", "Morning", 6, 0, 9, 0);
        manager.add_window("task1", w1);
        manager.add_window("task1", w2);

        // Verify windows exist
        assert_eq!(manager.get_windows("task1").unwrap().len(), 2);

        // Check allowed times
        assert!(manager.is_allowed_at("task1", 0, make_time(23, 0))); // night
        assert!(manager.is_allowed_at("task1", 0, make_time(7, 0))); // morning
        assert!(!manager.is_allowed_at("task1", 0, make_time(12, 0))); // midday

        // Remove one window
        assert!(manager.remove_window("task1", "night"));
        assert_eq!(manager.get_windows("task1").unwrap().len(), 1);

        // Night should no longer be allowed
        assert!(!manager.is_allowed_at("task1", 0, make_time(23, 0)));
        assert!(manager.is_allowed_at("task1", 0, make_time(7, 0)));

        // Clear all
        manager.clear_all();
        assert!(manager.get_all_windows().is_empty());
    }

    #[test]
    fn test_multi_task_independent() {
        let mut manager = TaskScheduleWindowsManager::new();
        manager.add_window("task1", ScheduleWindow::new("day", "Day", 8, 0, 18, 0));
        manager.add_window("task2", ScheduleWindow::new("night", "Night", 22, 0, 8, 0));

        // task1 allowed during day
        assert!(manager.is_allowed_at("task1", 0, make_time(12, 0)));
        assert!(!manager.is_allowed_at("task1", 0, make_time(23, 0)));

        // task2 allowed during night
        assert!(!manager.is_allowed_at("task2", 0, make_time(12, 0)));
        assert!(manager.is_allowed_at("task2", 0, make_time(23, 0)));

        // Clear task1 should not affect task2
        manager.clear_task_windows("task1");
        assert!(manager.get_windows("task1").is_none());
        assert!(manager.get_windows("task2").is_some());
    }

    #[test]
    fn test_toggle_enabled_idempotent() {
        let mut manager = TaskScheduleWindowsManager::new();
        let window = ScheduleWindow::new("night", "Night", 22, 0, 8, 0);
        manager.add_window("task1", window);

        manager.set_enabled(false);
        assert!(manager.is_allowed_at("task1", 0, make_time(12, 0)));

        manager.set_enabled(false); // already disabled
        assert!(manager.is_allowed_at("task1", 0, make_time(12, 0)));

        manager.set_enabled(true);
        assert!(!manager.is_allowed_at("task1", 0, make_time(12, 0)));
    }

    #[test]
    fn test_add_remove_readd_window() {
        let mut manager = TaskScheduleWindowsManager::new();
        let window = ScheduleWindow::new("night", "Night", 22, 0, 8, 0);

        manager.add_window("task1", window.clone());
        assert!(!manager.is_allowed_at("task1", 0, make_time(12, 0)));

        manager.remove_window("task1", "night");
        assert!(manager.is_allowed_at("task1", 0, make_time(12, 0)));

        manager.add_window("task1", window);
        assert!(!manager.is_allowed_at("task1", 0, make_time(12, 0)));
    }

    #[test]
    fn test_many_windows_for_single_task() {
        let mut manager = TaskScheduleWindowsManager::new();
        for i in 0..10 {
            let window = ScheduleWindow::new(
                format!("w{}", i),
                format!("Window {}", i),
                i * 2,
                0,
                i * 2 + 1,
                0,
            );
            manager.add_window("task1", window);
        }
        assert_eq!(manager.get_windows("task1").unwrap().len(), 10);
    }

    #[test]
    fn test_many_tasks() {
        let mut manager = TaskScheduleWindowsManager::new();
        for i in 0..50 {
            let window =
                ScheduleWindow::new(format!("w{}", i), format!("Window {}", i), 8, 0, 18, 0);
            manager.add_window(&format!("task{}", i), window);
        }
        assert_eq!(manager.get_all_windows().len(), 50);
    }
}
