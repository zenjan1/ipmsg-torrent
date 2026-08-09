//! Download Task TTL (Time-To-Live) management
//!
//! Automatically pauses download tasks that exceed a configurable maximum lifetime.
//! This prevents stale downloads from occupying queue slots indefinitely.
//!
//! Features:
//! - Global default TTL configuration (persisted to `ttl_config.json`)
//! - Per-task TTL override (None = use global default)
//! - Automatic TTL checking in the scheduler loop
//! - Graceful auto-pause when TTL expires (task state → Paused with TTL reason)
//! - Configurable check interval
//! - Summary and status reporting
//!
//! TTL can be disabled entirely, or set to specific durations per task.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Errors from TTL operations.
#[derive(Error, Debug)]
pub enum TtlError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid TTL duration: {0}")]
    InvalidDuration(String),
}

/// Global TTL configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfig {
    /// Whether TTL enforcement is enabled globally.
    pub enabled: bool,
    /// Default maximum lifetime in seconds for tasks without a per-task override.
    /// `None` means no limit (tasks can run indefinitely).
    pub default_max_lifetime_secs: Option<u64>,
    /// How often to check for TTL expiry (seconds). Default: 60.
    pub check_interval_secs: u64,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_max_lifetime_secs: None,
            check_interval_secs: 60,
        }
    }
}

/// Per-task TTL state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTtlState {
    /// Per-task maximum lifetime override in seconds.
    /// `None` means use the global default from `TtlConfig`.
    pub max_lifetime_secs: Option<u64>,
    /// Timestamp when the task was first started downloading (not created).
    /// Used to calculate actual lifetime spent downloading.
    /// `None` if the task has never been started.
    pub first_download_start: Option<DateTime<Utc>>,
    /// Total accumulated download time for TTL purposes.
    /// This excludes time spent paused/queued — only counts active download time.
    pub accumulated_lifetime_secs: f64,
    /// Whether this task has been auto-paused due to TTL expiry.
    pub ttl_expired: bool,
}

impl Default for TaskTtlState {
    fn default() -> Self {
        Self {
            max_lifetime_secs: None,
            first_download_start: None,
            accumulated_lifetime_secs: 0.0,
            ttl_expired: false,
        }
    }
}

/// Result of a TTL check for a single task.
#[derive(Debug, Clone)]
pub struct TtlCheckResult {
    /// Task ID.
    pub task_id: String,
    /// Whether the task's TTL has expired.
    pub expired: bool,
    /// Remaining lifetime in seconds (negative if expired).
    pub remaining_secs: f64,
    /// Effective max lifetime for this task.
    pub effective_max_secs: Option<u64>,
}

/// Summary of TTL status across all tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlSummary {
    /// Whether TTL is globally enabled.
    pub enabled: bool,
    /// Default max lifetime (seconds).
    pub default_max_lifetime_secs: Option<u64>,
    /// Check interval (seconds).
    pub check_interval_secs: u64,
    /// Total number of tasks tracked.
    pub total_tasks: usize,
    /// Number of tasks with per-task TTL overrides.
    pub tasks_with_override: usize,
    /// Number of tasks that have been auto-paused due to TTL.
    pub tasks_expired: usize,
    /// Number of tasks currently downloading with TTL countdown.
    pub tasks_active: usize,
    /// Per-task details.
    pub task_details: Vec<TaskTtlDetail>,
}

/// Per-task TTL detail for summary reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTtlDetail {
    pub task_id: String,
    pub task_name: String,
    pub effective_max_secs: Option<u64>,
    pub accumulated_lifetime_secs: f64,
    pub remaining_secs: f64,
    pub expired: bool,
    pub is_override: bool,
}

/// Parse a human-friendly duration string into seconds.
///
/// Supported formats:
/// - `"30s"` → 30 seconds
/// - `"5m"` → 300 seconds
/// - `"2h"` → 7200 seconds
/// - `"1d"` → 86400 seconds
/// - `"7d"` → 604800 seconds
/// - `"none"` / `"off"` / `"∞"` → None (no limit)
/// - Plain integer → seconds
pub fn parse_ttl_duration(s: &str) -> Result<Option<u64>, TtlError> {
    let trimmed = s.trim().to_lowercase();

    if trimmed == "none" || trimmed == "off" || trimmed == "∞" || trimmed == "infinity" {
        return Ok(None);
    }

    if trimmed.is_empty() {
        return Err(TtlError::InvalidDuration("empty duration string".into()));
    }

    // Try plain integer (seconds)
    if let Ok(secs) = trimmed.parse::<u64>() {
        return Ok(Some(secs));
    }

    // Parse suffix
    let (num_str, multiplier) = if let Some(n) = trimmed.strip_suffix('s') {
        (n, 1u64)
    } else if let Some(n) = trimmed.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = trimmed.strip_suffix('h') {
        (n, 3600)
    } else if let Some(n) = trimmed.strip_suffix('d') {
        (n, 86400)
    } else {
        return Err(TtlError::InvalidDuration(format!(
            "unrecognized duration format: '{s}'. Use: 30s, 5m, 2h, 1d, none"
        )));
    };

    let num: u64 = num_str.trim().parse().map_err(|_| {
        TtlError::InvalidDuration(format!("invalid number in duration: '{num_str}'"))
    })?;

    Ok(Some(num * multiplier))
}

/// Format seconds into a human-readable duration string.
pub fn format_ttl_duration(secs: Option<u64>) -> String {
    match secs {
        None => "∞ (no limit)".to_string(),
        Some(s) if s == 0 => "0s".to_string(),
        Some(s) => {
            let days = s / 86400;
            let hours = (s % 86400) / 3600;
            let mins = (s % 3600) / 60;
            let secs = s % 60;

            let mut parts = Vec::new();
            if days > 0 {
                parts.push(format!("{days}d"));
            }
            if hours > 0 {
                parts.push(format!("{hours}h"));
            }
            if mins > 0 {
                parts.push(format!("{mins}m"));
            }
            if secs > 0 || parts.is_empty() {
                parts.push(format!("{secs}s"));
            }
            parts.join(" ")
        }
    }
}

/// TTL manager that handles checking and enforcing task lifetimes.
#[derive(Debug, Clone)]
pub struct TtlManager {
    config: TtlConfig,
    /// Per-task TTL states, keyed by task ID.
    task_states: HashMap<String, TaskTtlState>,
}

impl TtlManager {
    /// Create a new TTL manager with default config.
    pub fn new() -> Self {
        Self {
            config: TtlConfig::default(),
            task_states: HashMap::new(),
        }
    }

    /// Create with a specific config.
    pub fn with_config(config: TtlConfig) -> Self {
        Self {
            config,
            task_states: HashMap::new(),
        }
    }

    /// Get current config.
    pub fn config(&self) -> &TtlConfig {
        &self.config
    }

    /// Update config.
    pub fn set_config(&mut self, config: TtlConfig) {
        self.config = config;
    }

    /// Get or initialize TTL state for a task.
    pub fn get_task_state(&self, task_id: &str) -> Option<&TaskTtlState> {
        self.task_states.get(task_id)
    }

    /// Get or create mutable TTL state for a task.
    pub fn ensure_task_state(&mut self, task_id: &str) -> &mut TaskTtlState {
        self.task_states.entry(task_id.to_string()).or_default()
    }

    /// Set per-task TTL override.
    pub fn set_task_ttl(&mut self, task_id: &str, max_lifetime_secs: Option<u64>) {
        let state = self.ensure_task_state(task_id);
        state.max_lifetime_secs = max_lifetime_secs;
    }

    /// Get effective max lifetime for a task (per-task override or global default).
    pub fn effective_max_lifetime(&self, task_id: &str) -> Option<u64> {
        if let Some(state) = self.task_states.get(task_id) {
            if let Some(override_val) = state.max_lifetime_secs {
                return Some(override_val);
            }
        }
        self.config.default_max_lifetime_secs
    }

    /// Record that a task has started downloading (for TTL tracking).
    pub fn record_download_start(&mut self, task_id: &str, now: DateTime<Utc>) {
        let state = self.ensure_task_state(task_id);
        if state.first_download_start.is_none() {
            state.first_download_start = Some(now);
        }
    }

    /// Update accumulated lifetime for a task.
    pub fn update_accumulated_time(&mut self, task_id: &str, additional_secs: f64) {
        let state = self.ensure_task_state(task_id);
        state.accumulated_lifetime_secs += additional_secs;
    }

    /// Check if a task's TTL has expired.
    pub fn check_task_ttl(&self, task_id: &str) -> TtlCheckResult {
        let effective_max = self.effective_max_lifetime(task_id);
        let state = self.task_states.get(task_id);

        let accumulated = state.map_or(0.0, |s| s.accumulated_lifetime_secs);
        let remaining = match effective_max {
            Some(max) => max as f64 - accumulated,
            None => f64::INFINITY,
        };

        TtlCheckResult {
            task_id: task_id.to_string(),
            expired: effective_max.is_some() && remaining <= 0.0,
            remaining_secs: remaining,
            effective_max_secs: effective_max,
        }
    }

    /// Check all tasks and return IDs of those whose TTL has expired.
    pub fn check_all_expired(&self) -> Vec<String> {
        if !self.config.enabled {
            return Vec::new();
        }

        self.task_states
            .iter()
            .filter(|(id, _)| self.check_task_ttl(id).expired)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Mark a task as TTL-expired (auto-paused).
    pub fn mark_expired(&mut self, task_id: &str) {
        if let Some(state) = self.task_states.get_mut(task_id) {
            state.ttl_expired = true;
        }
    }

    /// Reset TTL state for a task (e.g., when user manually resumes).
    pub fn reset_task(&mut self, task_id: &str) {
        if let Some(state) = self.task_states.get_mut(task_id) {
            state.ttl_expired = false;
            state.accumulated_lifetime_secs = 0.0;
            state.first_download_start = None;
        }
    }

    /// Remove task state (when task is deleted).
    pub fn remove_task(&mut self, task_id: &str) {
        self.task_states.remove(task_id);
    }

    /// Generate a summary of TTL status.
    pub fn summary<F>(&self, task_name_fn: F) -> TtlSummary
    where
        F: Fn(&str) -> String,
    {
        let mut details = Vec::new();
        let mut tasks_with_override = 0;
        let mut tasks_expired = 0;
        let mut tasks_active = 0;

        for (task_id, state) in &self.task_states {
            let effective_max = self.effective_max_lifetime(task_id);
            let is_override = state.max_lifetime_secs.is_some();
            let remaining = match effective_max {
                Some(max) => max as f64 - state.accumulated_lifetime_secs,
                None => f64::INFINITY,
            };
            let expired = effective_max.is_some() && remaining <= 0.0;

            if is_override {
                tasks_with_override += 1;
            }
            if expired || state.ttl_expired {
                tasks_expired += 1;
            }
            if !expired && !state.ttl_expired && state.first_download_start.is_some() {
                tasks_active += 1;
            }

            details.push(TaskTtlDetail {
                task_id: task_id.clone(),
                task_name: task_name_fn(task_id),
                effective_max_secs: effective_max,
                accumulated_lifetime_secs: state.accumulated_lifetime_secs,
                remaining_secs: remaining,
                expired,
                is_override,
            });
        }

        // Sort by remaining time ascending (most urgent first)
        details.sort_by(|a, b| {
            a.remaining_secs
                .partial_cmp(&b.remaining_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        TtlSummary {
            enabled: self.config.enabled,
            default_max_lifetime_secs: self.config.default_max_lifetime_secs,
            check_interval_secs: self.config.check_interval_secs,
            total_tasks: self.task_states.len(),
            tasks_with_override,
            tasks_expired,
            tasks_active,
            task_details: details,
        }
    }

    /// Serialize config for persistence.
    pub fn serialize_config(&self) -> Result<String, TtlError> {
        Ok(serde_json::to_string_pretty(&self.config)?)
    }

    /// Load config from persisted JSON.
    pub fn load_config(json: &str) -> Result<TtlConfig, TtlError> {
        Ok(serde_json::from_str(json)?)
    }

    /// Save TTL task states for persistence.
    pub fn serialize_states(&self) -> Result<String, TtlError> {
        Ok(serde_json::to_string_pretty(&self.task_states)?)
    }

    /// Load TTL task states from persisted JSON.
    pub fn load_states(json: &str) -> Result<HashMap<String, TaskTtlState>, TtlError> {
        Ok(serde_json::from_str(json)?)
    }

    /// Restore task states from loaded data.
    pub fn restore_states(&mut self, states: HashMap<String, TaskTtlState>) {
        self.task_states = states;
    }
}

impl Default for TtlManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_parse_ttl_seconds() {
        assert_eq!(parse_ttl_duration("30s").unwrap(), Some(30));
        assert_eq!(parse_ttl_duration("30").unwrap(), Some(30));
    }

    #[test]
    fn test_parse_ttl_minutes() {
        assert_eq!(parse_ttl_duration("5m").unwrap(), Some(300));
        assert_eq!(parse_ttl_duration("10M").unwrap(), Some(600));
    }

    #[test]
    fn test_parse_ttl_hours() {
        assert_eq!(parse_ttl_duration("2h").unwrap(), Some(7200));
        assert_eq!(parse_ttl_duration("24H").unwrap(), Some(86400));
    }

    #[test]
    fn test_parse_ttl_days() {
        assert_eq!(parse_ttl_duration("1d").unwrap(), Some(86400));
        assert_eq!(parse_ttl_duration("7d").unwrap(), Some(604800));
    }

    #[test]
    fn test_parse_ttl_none() {
        assert_eq!(parse_ttl_duration("none").unwrap(), None);
        assert_eq!(parse_ttl_duration("off").unwrap(), None);
        assert_eq!(parse_ttl_duration("∞").unwrap(), None);
        assert_eq!(parse_ttl_duration("infinity").unwrap(), None);
    }

    #[test]
    fn test_parse_ttl_invalid() {
        assert!(parse_ttl_duration("").is_err());
        assert!(parse_ttl_duration("abc").is_err());
        assert!(parse_ttl_duration("5x").is_err());
    }

    #[test]
    fn test_format_ttl() {
        assert_eq!(format_ttl_duration(None), "∞ (no limit)");
        assert_eq!(format_ttl_duration(Some(0)), "0s");
        assert_eq!(format_ttl_duration(Some(30)), "30s");
        assert_eq!(format_ttl_duration(Some(90)), "1m 30s");
        assert_eq!(format_ttl_duration(Some(3661)), "1h 1m 1s");
        assert_eq!(format_ttl_duration(Some(90061)), "1d 1h 1m 1s");
        assert_eq!(format_ttl_duration(Some(86400)), "1d");
        assert_eq!(format_ttl_duration(Some(3600)), "1h");
        assert_eq!(format_ttl_duration(Some(60)), "1m");
    }

    #[test]
    fn test_ttl_config_default() {
        let config = TtlConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.default_max_lifetime_secs, None);
        assert_eq!(config.check_interval_secs, 60);
    }

    #[test]
    fn test_ttl_manager_basic() {
        let mut mgr = TtlManager::new();
        assert!(!mgr.config().enabled);

        mgr.set_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(3600),
            check_interval_secs: 30,
        });

        assert!(mgr.config().enabled);
        assert_eq!(mgr.effective_max_lifetime("any-task"), Some(3600));
    }

    #[test]
    fn test_per_task_ttl_override() {
        let mut mgr = TtlManager::with_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(3600),
            check_interval_secs: 60,
        });

        // Task with no override uses global default
        assert_eq!(mgr.effective_max_lifetime("task1"), Some(3600));

        // Set per-task override
        mgr.set_task_ttl("task1", Some(7200));
        assert_eq!(mgr.effective_max_lifetime("task1"), Some(7200));

        // Override with None still uses global default
        mgr.set_task_ttl("task2", None);
        assert_eq!(mgr.effective_max_lifetime("task2"), Some(3600));
    }

    #[test]
    fn test_ttl_check_not_expired() {
        let mut mgr = TtlManager::with_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(3600),
            check_interval_secs: 60,
        });

        mgr.update_accumulated_time("task1", 1800.0); // 30 minutes used of 1 hour
        let result = mgr.check_task_ttl("task1");
        assert!(!result.expired);
        assert!((result.remaining_secs - 1800.0).abs() < 0.01);
        assert_eq!(result.effective_max_secs, Some(3600));
    }

    #[test]
    fn test_ttl_check_expired() {
        let mut mgr = TtlManager::with_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(3600),
            check_interval_secs: 60,
        });

        mgr.update_accumulated_time("task1", 3700.0); // Exceeded 1 hour
        let result = mgr.check_task_ttl("task1");
        assert!(result.expired);
        assert!(result.remaining_secs < 0.0);
    }

    #[test]
    fn test_ttl_no_limit() {
        let mut mgr = TtlManager::with_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: None, // No limit
            check_interval_secs: 60,
        });

        mgr.update_accumulated_time("task1", 999999.0);
        let result = mgr.check_task_ttl("task1");
        assert!(!result.expired);
        assert!(result.remaining_secs.is_infinite());
    }

    #[test]
    fn test_ttl_disabled_never_expires() {
        let mut mgr = TtlManager::with_config(TtlConfig {
            enabled: false,                     // Disabled
            default_max_lifetime_secs: Some(1), // 1 second limit
            check_interval_secs: 60,
        });

        mgr.update_accumulated_time("task1", 100.0);
        // Even though accumulated > max, check_all_expired returns empty when disabled
        let expired = mgr.check_all_expired();
        assert!(expired.is_empty());
    }

    #[test]
    fn test_check_all_expired() {
        let mut mgr = TtlManager::with_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(100),
            check_interval_secs: 60,
        });

        mgr.update_accumulated_time("task1", 50.0); // OK
        mgr.update_accumulated_time("task2", 150.0); // Expired
        mgr.update_accumulated_time("task3", 200.0); // Expired

        let expired = mgr.check_all_expired();
        assert_eq!(expired.len(), 2);
        assert!(expired.contains(&"task2".to_string()));
        assert!(expired.contains(&"task3".to_string()));
    }

    #[test]
    fn test_mark_expired_and_reset() {
        let mut mgr = TtlManager::new();
        mgr.update_accumulated_time("task1", 100.0);

        mgr.mark_expired("task1");
        assert!(mgr.get_task_state("task1").unwrap().ttl_expired);

        mgr.reset_task("task1");
        assert!(!mgr.get_task_state("task1").unwrap().ttl_expired);
        assert_eq!(
            mgr.get_task_state("task1")
                .unwrap()
                .accumulated_lifetime_secs,
            0.0
        );
        assert!(
            mgr.get_task_state("task1")
                .unwrap()
                .first_download_start
                .is_none()
        );
    }

    #[test]
    fn test_remove_task() {
        let mut mgr = TtlManager::new();
        mgr.update_accumulated_time("task1", 100.0);
        assert!(mgr.get_task_state("task1").is_some());

        mgr.remove_task("task1");
        assert!(mgr.get_task_state("task1").is_none());
    }

    #[test]
    fn test_record_download_start() {
        let mut mgr = TtlManager::new();
        let now = Utc::now();

        mgr.record_download_start("task1", now);
        assert_eq!(
            mgr.get_task_state("task1").unwrap().first_download_start,
            Some(now)
        );

        // Calling again should not overwrite
        let later = now + Duration::seconds(100);
        mgr.record_download_start("task1", later);
        assert_eq!(
            mgr.get_task_state("task1").unwrap().first_download_start,
            Some(now)
        );
    }

    #[test]
    fn test_summary() {
        let mut mgr = TtlManager::with_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(3600),
            check_interval_secs: 60,
        });

        mgr.update_accumulated_time("task1", 1800.0);
        mgr.set_task_ttl("task2", Some(7200));
        mgr.update_accumulated_time("task2", 3600.0);
        mgr.record_download_start("task1", Utc::now());
        mgr.record_download_start("task2", Utc::now());

        let summary = mgr.summary(|id| format!("Task-{id}"));
        assert!(summary.enabled);
        assert_eq!(summary.total_tasks, 2);
        assert_eq!(summary.tasks_with_override, 1);
        assert_eq!(summary.tasks_active, 2);
        assert_eq!(summary.task_details.len(), 2);
        // task1 has less remaining (1800s vs 3600s), so it comes first
        assert_eq!(summary.task_details[0].task_id, "task1");
    }

    #[test]
    fn test_config_serialization() {
        let config = TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(7200),
            check_interval_secs: 30,
        };

        let json = serde_json::to_string(&config).unwrap();
        let loaded: TtlConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(
            loaded.default_max_lifetime_secs,
            config.default_max_lifetime_secs
        );
        assert_eq!(loaded.check_interval_secs, config.check_interval_secs);
    }

    #[test]
    fn test_states_serialization() {
        let mut mgr = TtlManager::new();
        mgr.update_accumulated_time("task1", 100.0);
        mgr.set_task_ttl("task1", Some(3600));
        mgr.record_download_start("task1", Utc::now());

        let json = mgr.serialize_states().unwrap();
        let loaded = TtlManager::load_states(&json).unwrap();
        assert!(loaded.contains_key("task1"));
        assert_eq!(loaded["task1"].max_lifetime_secs, Some(3600));
        assert!((loaded["task1"].accumulated_lifetime_secs - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_restore_states() {
        let mut mgr = TtlManager::new();
        let mut states = HashMap::new();
        states.insert(
            "task1".to_string(),
            TaskTtlState {
                max_lifetime_secs: Some(1800),
                first_download_start: None,
                accumulated_lifetime_secs: 500.0,
                ttl_expired: false,
            },
        );

        mgr.restore_states(states);
        assert_eq!(mgr.effective_max_lifetime("task1"), Some(1800));
        assert!(
            (mgr.get_task_state("task1")
                .unwrap()
                .accumulated_lifetime_secs
                - 500.0)
                .abs()
                < 0.01
        );
    }

    #[test]
    fn test_unknown_task_ttl_check() {
        let mgr = TtlManager::with_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(3600),
            check_interval_secs: 60,
        });

        // Unknown task: accumulated = 0, so not expired
        let result = mgr.check_task_ttl("unknown");
        assert!(!result.expired);
        assert!((result.remaining_secs - 3600.0).abs() < 0.01);
    }

    #[test]
    fn test_per_task_override_in_check() {
        let mut mgr = TtlManager::with_config(TtlConfig {
            enabled: true,
            default_max_lifetime_secs: Some(3600),
            check_interval_secs: 60,
        });

        // Override task2 to have shorter TTL
        mgr.set_task_ttl("task2", Some(100));
        mgr.update_accumulated_time("task2", 150.0);

        let result = mgr.check_task_ttl("task2");
        assert!(result.expired);
        assert_eq!(result.effective_max_secs, Some(100));
    }
}
