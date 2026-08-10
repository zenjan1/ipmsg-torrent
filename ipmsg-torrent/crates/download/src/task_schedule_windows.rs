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
            .or_insert_with(Vec::new)
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
                .map_or(false, |w| w.is_empty())
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
            if let Some(next) = self.find_next_window_occurrence(window, now) {
                if earliest.is_none() || next < earliest.unwrap() {
                    earliest = Some(next);
                }
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
    use chrono::TimeZone;

    fn make_time(hour: u32, minute: u32) -> DateTime<Local> {
        Local::now()
            .date_naive()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap()
    }

    #[test]
    fn test_schedule_window_creation() {
        let window = ScheduleWindow::new("night", "Night Downloads", 22, 0, 8, 0);
        assert_eq!(window.id, "night");
        assert_eq!(window.start_hour, 22);
        assert_eq!(window.end_hour, 8);
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
}
