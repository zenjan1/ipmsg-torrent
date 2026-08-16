//! Auto-cleanup of completed/failed downloads
//!
//! Automatically remove download tasks that have reached a terminal state
//! (Complete or Error) after a configurable retention period.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Configuration for auto-cleanup behavior
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AutoCleanupConfig {
    /// Whether auto-cleanup is enabled
    pub enabled: bool,
    /// Seconds to retain completed tasks before removal (0 = immediate, None = never)
    pub completed_retention_secs: Option<u64>,
    /// Seconds to retain failed tasks before removal (0 = immediate, None = never)
    pub failed_retention_secs: Option<u64>,
    /// Check interval in seconds (how often to scan for tasks to clean up)
    pub check_interval_secs: u64,
}

impl Default for AutoCleanupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            completed_retention_secs: None,
            failed_retention_secs: None,
            check_interval_secs: 300, // 5 minutes
        }
    }
}

impl AutoCleanupConfig {
    /// Create a disabled config (no auto-cleanup)
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Create a config that immediately removes completed tasks
    pub fn immediate_completed() -> Self {
        Self {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: None,
            check_interval_secs: 60,
        }
    }

    /// Display summary of config
    pub fn display(&self) -> String {
        if !self.enabled {
            return "auto-cleanup: disabled".to_string();
        }
        let completed = match self.completed_retention_secs {
            None => "never".to_string(),
            Some(0) => "immediate".to_string(),
            Some(s) => format_duration(s),
        };
        let failed = match self.failed_retention_secs {
            None => "never".to_string(),
            Some(0) => "immediate".to_string(),
            Some(s) => format_duration(s),
        };
        format!(
            "auto-cleanup: enabled (completed: {}, failed: {}, check every: {})",
            completed,
            failed,
            format_duration(self.check_interval_secs)
        )
    }
}

/// Format seconds into human-readable duration
fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86400)
    }
}

/// Parse duration string to seconds (supports: 30s, 5m, 1h, 7d)
pub fn parse_duration_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Handle "none" / "never" / "off"
    let lower = s.to_lowercase();
    if lower == "none" || lower == "never" || lower == "off" {
        return None;
    }

    // Handle "immediate" / "0"
    if lower == "immediate" || s == "0" {
        return Some(0);
    }

    // Parse number + suffix
    let (num_str, multiplier) = if lower.ends_with('d') {
        (&s[..s.len() - 1], 86400u64)
    } else if lower.ends_with('h') {
        (&s[..s.len() - 1], 3600u64)
    } else if lower.ends_with('m') {
        (&s[..s.len() - 1], 60u64)
    } else if lower.ends_with('s') {
        (&s[..s.len() - 1], 1u64)
    } else {
        // Assume seconds if no suffix
        (s, 1u64)
    };

    num_str.trim().parse::<u64>().ok().map(|n| n * multiplier)
}

/// Data about a task needed for cleanup decisions
#[derive(Debug, Clone)]
pub struct TaskCleanupData {
    pub id: String,
    pub state: TaskCleanupState,
    pub updated_at: DateTime<Utc>,
}

/// Simplified task state for cleanup purposes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCleanupState {
    Complete,
    Error,
    Other,
}

/// Check if a task should be cleaned up based on config and current time
pub fn should_cleanup(
    task: &TaskCleanupData,
    config: &AutoCleanupConfig,
    now: DateTime<Utc>,
) -> bool {
    if !config.enabled {
        return false;
    }

    let retention_secs = match task.state {
        TaskCleanupState::Complete => config.completed_retention_secs,
        TaskCleanupState::Error => config.failed_retention_secs,
        TaskCleanupState::Other => return false,
    };

    let retention_secs = match retention_secs {
        Some(s) => s,
        None => return false, // Never clean up this state
    };

    let age = now.signed_duration_since(task.updated_at);
    age >= Duration::seconds(retention_secs as i64)
}

/// Filter a list of tasks to find those that should be cleaned up
pub fn tasks_to_cleanup(
    tasks: &[TaskCleanupData],
    config: &AutoCleanupConfig,
    now: DateTime<Utc>,
) -> Vec<String> {
    tasks
        .iter()
        .filter(|t| should_cleanup(t, config, now))
        .map(|t| t.id.clone())
        .collect()
}

// --- Persistence ---

const CONFIG_FILENAME: &str = "auto_cleanup_config.json";

/// Persistence error type
#[derive(Debug)]
pub enum AutoCleanupPersistenceError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for AutoCleanupPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Json(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<std::io::Error> for AutoCleanupPersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for AutoCleanupPersistenceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Save auto-cleanup config to disk (atomic write)
pub fn save_auto_cleanup_config(
    data_dir: &Path,
    config: &AutoCleanupConfig,
) -> Result<(), AutoCleanupPersistenceError> {
    let path = data_dir.join(CONFIG_FILENAME);
    let json = serde_json::to_string_pretty(config)?;

    // Atomic write: write to temp file, then rename
    let temp_path = data_dir.join("auto_cleanup_config.json.tmp");
    fs::write(&temp_path, &json)?;
    fs::rename(&temp_path, &path)?;

    Ok(())
}

/// Load auto-cleanup config from disk
pub fn load_auto_cleanup_config(
    data_dir: &Path,
) -> Result<Option<AutoCleanupConfig>, AutoCleanupPersistenceError> {
    let path = data_dir.join(CONFIG_FILENAME);
    if !path.exists() {
        return Ok(None);
    }

    let json = fs::read_to_string(&path)?;
    let config: AutoCleanupConfig = serde_json::from_str(&json)?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn make_task(id: &str, state: TaskCleanupState, age_secs: i64) -> TaskCleanupData {
        TaskCleanupData {
            id: id.to_string(),
            state,
            updated_at: Utc::now() - Duration::seconds(age_secs),
        }
    }

    // ========== AutoCleanupConfig defaults ==========

    #[test]
    fn test_default_config_disabled() {
        let config = AutoCleanupConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.completed_retention_secs, None);
        assert_eq!(config.failed_retention_secs, None);
        assert_eq!(config.check_interval_secs, 300);
    }

    #[test]
    fn test_disabled_equals_default() {
        assert_eq!(AutoCleanupConfig::disabled(), AutoCleanupConfig::default());
    }

    #[test]
    fn test_immediate_completed_fields() {
        let config = AutoCleanupConfig::immediate_completed();
        assert!(config.enabled);
        assert_eq!(config.completed_retention_secs, Some(0));
        assert_eq!(config.failed_retention_secs, None);
        assert_eq!(config.check_interval_secs, 60);
    }

    // ========== AutoCleanupConfig serde ==========

    #[test]
    fn test_config_serde_roundtrip() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: Some(86400),
            check_interval_secs: 120,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: AutoCleanupConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded, config);
    }

    #[test]
    fn test_config_serde_roundtrip_default() {
        let config = AutoCleanupConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: AutoCleanupConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded, config);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(7200),
            failed_retention_secs: None,
            check_interval_secs: 300,
        };
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let loaded: AutoCleanupConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded, config);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"completed_retention_secs":100,"failed_retention_secs":null,"check_interval_secs":60,"extra_field":"ignored","unknown":42}"#;
        let loaded: AutoCleanupConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.completed_retention_secs, Some(100));
    }

    #[test]
    fn test_config_serde_missing_optional_fields() {
        let json = r#"{"enabled":false,"check_interval_secs":300}"#;
        let loaded: AutoCleanupConfig = serde_json::from_str(json).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.completed_retention_secs, None);
        assert_eq!(loaded.failed_retention_secs, None);
        assert_eq!(loaded.check_interval_secs, 300);
    }

    // ========== AutoCleanupConfig traits ==========

    #[test]
    fn test_config_clone() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: Some(1800),
            check_interval_secs: 60,
        };
        let cloned = config.clone();
        assert_eq!(cloned, config);
    }

    #[test]
    fn test_config_clone_independence() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let mut cloned = config.clone();
        cloned.enabled = false;
        assert!(config.enabled);
    }

    #[test]
    fn test_config_debug() {
        let config = AutoCleanupConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("AutoCleanupConfig"));
        assert!(debug.contains("enabled"));
    }

    #[test]
    fn test_config_partial_eq() {
        let a = AutoCleanupConfig::default();
        let b = AutoCleanupConfig::default();
        assert_eq!(a, b);
    }

    // ========== Display ==========

    #[test]
    fn test_display_disabled() {
        let config = AutoCleanupConfig::disabled();
        assert_eq!(config.display(), "auto-cleanup: disabled");
    }

    #[test]
    fn test_display_enabled() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: Some(86400),
            check_interval_secs: 60,
        };
        let display = config.display();
        assert!(display.contains("enabled"));
        assert!(display.contains("1h"));
        assert!(display.contains("1d"));
    }

    #[test]
    fn test_display_immediate() {
        let config = AutoCleanupConfig::immediate_completed();
        let display = config.display();
        assert!(display.contains("immediate"));
    }

    #[test]
    fn test_display_never_retention() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: None,
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let display = config.display();
        assert!(display.contains("never"));
    }

    #[test]
    fn test_display_check_interval() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(60),
            failed_retention_secs: Some(60),
            check_interval_secs: 7200,
        };
        let display = config.display();
        assert!(display.contains("2h"));
    }

    // ========== format_duration ==========

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0), "0s");
    }

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(59), "59s");
    }

    #[test]
    fn test_format_duration_exact_minute() {
        assert_eq!(format_duration(60), "1m");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(300), "5m");
        assert_eq!(format_duration(3599), "59m");
    }

    #[test]
    fn test_format_duration_exact_hour() {
        assert_eq!(format_duration(3600), "1h");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(7200), "2h");
        assert_eq!(format_duration(86399), "23h");
    }

    #[test]
    fn test_format_duration_exact_day() {
        assert_eq!(format_duration(86400), "1d");
    }

    #[test]
    fn test_format_duration_days() {
        assert_eq!(format_duration(172800), "2d");
        assert_eq!(format_duration(864000), "10d");
    }

    #[test]
    fn test_format_duration_large_value() {
        assert_eq!(format_duration(u64::MAX), format!("{}d", u64::MAX / 86400));
    }

    // ========== parse_duration_secs ==========

    #[test]
    fn test_parse_duration_secs_basic() {
        assert_eq!(parse_duration_secs("30s"), Some(30));
        assert_eq!(parse_duration_secs("5m"), Some(300));
        assert_eq!(parse_duration_secs("1h"), Some(3600));
        assert_eq!(parse_duration_secs("7d"), Some(604800));
    }

    #[test]
    fn test_parse_duration_secs_no_suffix() {
        assert_eq!(parse_duration_secs("60"), Some(60));
        assert_eq!(parse_duration_secs("0"), Some(0));
        assert_eq!(parse_duration_secs("3600"), Some(3600));
    }

    #[test]
    fn test_parse_duration_secs_immediate() {
        assert_eq!(parse_duration_secs("immediate"), Some(0));
    }

    #[test]
    fn test_parse_duration_secs_none_values() {
        assert_eq!(parse_duration_secs("none"), None);
        assert_eq!(parse_duration_secs("never"), None);
        assert_eq!(parse_duration_secs("off"), None);
    }

    #[test]
    fn test_parse_duration_secs_empty() {
        assert_eq!(parse_duration_secs(""), None);
    }

    #[test]
    fn test_parse_duration_secs_whitespace() {
        assert_eq!(parse_duration_secs("  30s  "), Some(30));
        assert_eq!(parse_duration_secs(" 5m "), Some(300));
        assert_eq!(parse_duration_secs(" 1h"), Some(3600));
    }

    #[test]
    fn test_parse_duration_secs_case_insensitive() {
        assert_eq!(parse_duration_secs("30S"), Some(30));
        assert_eq!(parse_duration_secs("5M"), Some(300));
        assert_eq!(parse_duration_secs("1H"), Some(3600));
        assert_eq!(parse_duration_secs("7D"), Some(604800));
    }

    #[test]
    fn test_parse_duration_secs_mixed_case() {
        assert_eq!(parse_duration_secs("None"), None);
        assert_eq!(parse_duration_secs("NEVER"), None);
        assert_eq!(parse_duration_secs("Immediate"), Some(0));
        assert_eq!(parse_duration_secs("OFF"), None);
    }

    #[test]
    fn test_parse_duration_secs_invalid() {
        assert_eq!(parse_duration_secs("abc"), None);
        assert_eq!(parse_duration_secs("xyz123"), None);
        assert_eq!(parse_duration_secs("-5s"), None);
    }

    #[test]
    fn test_parse_duration_secs_large_value() {
        assert_eq!(parse_duration_secs("999999s"), Some(999999));
        assert_eq!(parse_duration_secs("10000d"), Some(864000000));
    }

    #[test]
    fn test_parse_duration_secs_whitespace_only() {
        assert_eq!(parse_duration_secs("   "), None);
    }

    // ========== TaskCleanupState traits ==========

    #[test]
    fn test_cleanup_state_clone() {
        let state = TaskCleanupState::Complete;
        assert_eq!(state.clone(), state);
    }

    #[test]
    fn test_cleanup_state_copy() {
        let state = TaskCleanupState::Error;
        let copied = state;
        assert_eq!(state, copied);
    }

    #[test]
    fn test_cleanup_state_debug() {
        assert_eq!(format!("{:?}", TaskCleanupState::Complete), "Complete");
        assert_eq!(format!("{:?}", TaskCleanupState::Error), "Error");
        assert_eq!(format!("{:?}", TaskCleanupState::Other), "Other");
    }

    #[test]
    fn test_cleanup_state_eq() {
        assert_eq!(TaskCleanupState::Complete, TaskCleanupState::Complete);
        assert_ne!(TaskCleanupState::Complete, TaskCleanupState::Error);
        assert_ne!(TaskCleanupState::Error, TaskCleanupState::Other);
    }

    // ========== TaskCleanupData traits ==========

    #[test]
    fn test_cleanup_data_clone() {
        let task = make_task("t1", TaskCleanupState::Complete, 100);
        let cloned = task.clone();
        assert_eq!(cloned.id, task.id);
        assert_eq!(cloned.state, task.state);
    }

    #[test]
    fn test_cleanup_data_debug() {
        let task = make_task("t1", TaskCleanupState::Complete, 0);
        let debug = format!("{:?}", task);
        assert!(debug.contains("TaskCleanupData"));
        assert!(debug.contains("t1"));
    }

    #[test]
    fn test_cleanup_data_unicode_id() {
        let task = make_task("任务-中文", TaskCleanupState::Complete, 0);
        assert_eq!(task.id, "任务-中文");
    }

    #[test]
    fn test_cleanup_data_emoji_id() {
        let task = make_task("task-🚀-001", TaskCleanupState::Error, 0);
        assert_eq!(task.id, "task-🚀-001");
    }

    // ========== should_cleanup ==========

    #[test]
    fn test_should_cleanup_disabled() {
        let config = AutoCleanupConfig::disabled();
        let task = make_task("t1", TaskCleanupState::Complete, 9999);
        assert!(!should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_completed_immediate() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Complete, 0);
        assert!(should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_completed_not_yet() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Complete, 1800);
        assert!(!should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_completed_expired() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Complete, 7200);
        assert!(should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_failed() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: None,
            failed_retention_secs: Some(1800),
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Error, 2400);
        assert!(should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_failed_not_yet() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: None,
            failed_retention_secs: Some(3600),
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Error, 1800);
        assert!(!should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_failed_immediate() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: None,
            failed_retention_secs: Some(0),
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Error, 0);
        assert!(should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_other_state_ignored() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: Some(0),
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Other, 99999);
        assert!(!should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_never_retention() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: None,
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Complete, 99999);
        assert!(!should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_exact_boundary() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        // Exactly at boundary (3600s == 3600s retention)
        let task = make_task("t1", TaskCleanupState::Complete, 3600);
        assert!(should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_just_under_boundary() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        // 1 second before boundary
        let task = make_task("t1", TaskCleanupState::Complete, 3599);
        assert!(!should_cleanup(&task, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_both_retention() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: Some(1800),
            check_interval_secs: 60,
        };
        let now = Utc::now();
        let complete_expired = make_task("c1", TaskCleanupState::Complete, 7200);
        let complete_recent = make_task("c2", TaskCleanupState::Complete, 1800);
        let failed_expired = make_task("f1", TaskCleanupState::Error, 3600);
        let failed_recent = make_task("f2", TaskCleanupState::Error, 600);
        assert!(should_cleanup(&complete_expired, &config, now));
        assert!(!should_cleanup(&complete_recent, &config, now));
        assert!(should_cleanup(&failed_expired, &config, now));
        assert!(!should_cleanup(&failed_recent, &config, now));
    }

    #[test]
    fn test_should_cleanup_completed_only_failed_never() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let complete = make_task("c1", TaskCleanupState::Complete, 0);
        let failed = make_task("f1", TaskCleanupState::Error, 99999);
        assert!(should_cleanup(&complete, &config, Utc::now()));
        assert!(!should_cleanup(&failed, &config, Utc::now()));
    }

    #[test]
    fn test_should_cleanup_unicode_id() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let task = make_task("中文任务-✅", TaskCleanupState::Complete, 0);
        assert!(should_cleanup(&task, &config, Utc::now()));
    }

    // ========== tasks_to_cleanup ==========

    #[test]
    fn test_tasks_to_cleanup_mixed() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: Some(0),
            check_interval_secs: 60,
        };
        let now = Utc::now();
        let tasks = vec![
            make_task("old-complete", TaskCleanupState::Complete, 7200),
            make_task("new-complete", TaskCleanupState::Complete, 1800),
            make_task("old-fail", TaskCleanupState::Error, 10),
            make_task("downloading", TaskCleanupState::Other, 99999),
        ];
        let to_clean = tasks_to_cleanup(&tasks, &config, now);
        assert_eq!(to_clean.len(), 2);
        assert!(to_clean.contains(&"old-complete".to_string()));
        assert!(to_clean.contains(&"old-fail".to_string()));
    }

    #[test]
    fn test_tasks_to_cleanup_empty() {
        let config = AutoCleanupConfig::immediate_completed();
        let tasks: Vec<TaskCleanupData> = vec![];
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert!(to_clean.is_empty());
    }

    #[test]
    fn test_tasks_to_cleanup_all_complete() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let tasks = vec![
            make_task("c1", TaskCleanupState::Complete, 0),
            make_task("c2", TaskCleanupState::Complete, 0),
            make_task("c3", TaskCleanupState::Complete, 0),
        ];
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert_eq!(to_clean.len(), 3);
    }

    #[test]
    fn test_tasks_to_cleanup_all_failed() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: None,
            failed_retention_secs: Some(0),
            check_interval_secs: 60,
        };
        let tasks = vec![
            make_task("f1", TaskCleanupState::Error, 0),
            make_task("f2", TaskCleanupState::Error, 0),
        ];
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert_eq!(to_clean.len(), 2);
    }

    #[test]
    fn test_tasks_to_cleanup_all_other() {
        let config = AutoCleanupConfig::immediate_completed();
        let tasks = vec![
            make_task("o1", TaskCleanupState::Other, 99999),
            make_task("o2", TaskCleanupState::Other, 99999),
        ];
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert!(to_clean.is_empty());
    }

    #[test]
    fn test_tasks_to_cleanup_none_match() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(86400),
            failed_retention_secs: Some(86400),
            check_interval_secs: 60,
        };
        let tasks = vec![
            make_task("c1", TaskCleanupState::Complete, 100),
            make_task("f1", TaskCleanupState::Error, 100),
        ];
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert!(to_clean.is_empty());
    }

    #[test]
    fn test_tasks_to_cleanup_preserves_order() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let tasks = vec![
            make_task("a", TaskCleanupState::Complete, 0),
            make_task("b", TaskCleanupState::Complete, 0),
            make_task("c", TaskCleanupState::Complete, 0),
        ];
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert_eq!(to_clean, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_tasks_to_cleanup_unicode_ids() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let tasks = vec![
            make_task("任务一", TaskCleanupState::Complete, 0),
            make_task("タスク二", TaskCleanupState::Complete, 0),
            make_task("작업-삼", TaskCleanupState::Complete, 0),
        ];
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert_eq!(to_clean.len(), 3);
    }

    // ========== AutoCleanupPersistenceError ==========

    #[test]
    fn test_persistence_error_io_display() {
        let err = AutoCleanupPersistenceError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        let display = format!("{}", err);
        assert!(display.contains("IO error"));
    }

    #[test]
    fn test_persistence_error_json_display() {
        let json_err = serde_json::from_str::<AutoCleanupConfig>("invalid").unwrap_err();
        let err = AutoCleanupPersistenceError::Json(json_err);
        let display = format!("{}", err);
        assert!(display.contains("JSON error"));
    }

    #[test]
    fn test_persistence_error_debug() {
        let err =
            AutoCleanupPersistenceError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        let debug = format!("{:?}", err);
        assert!(debug.contains("Io"));
    }

    #[test]
    fn test_persistence_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "io test");
        let err: AutoCleanupPersistenceError = io_err.into();
        let display = format!("{}", err);
        assert!(display.contains("IO error"));
    }

    #[test]
    fn test_persistence_error_from_json() {
        let json_err = serde_json::from_str::<AutoCleanupConfig>("bad").unwrap_err();
        let err: AutoCleanupPersistenceError = json_err.into();
        let display = format!("{}", err);
        assert!(display.contains("JSON error"));
    }

    // ========== Persistence ==========

    #[test]
    fn test_save_and_load_config() {
        let dir = TempDir::new().unwrap();
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(7200),
            failed_retention_secs: Some(86400),
            check_interval_secs: 120,
        };

        save_auto_cleanup_config(dir.path(), &config).unwrap();
        let loaded = load_auto_cleanup_config(dir.path()).unwrap().unwrap();

        assert_eq!(loaded.enabled, true);
        assert_eq!(loaded.completed_retention_secs, Some(7200));
        assert_eq!(loaded.failed_retention_secs, Some(86400));
        assert_eq!(loaded.check_interval_secs, 120);
    }

    #[test]
    fn test_load_config_missing_file() {
        let dir = TempDir::new().unwrap();
        let loaded = load_auto_cleanup_config(dir.path()).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_config_corrupted_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILENAME);
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(b"not valid json {{{").unwrap();

        let result = load_auto_cleanup_config(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_save_overwrites_existing() {
        let dir = TempDir::new().unwrap();

        let config1 = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        save_auto_cleanup_config(dir.path(), &config1).unwrap();

        let config2 = AutoCleanupConfig {
            enabled: false,
            completed_retention_secs: None,
            failed_retention_secs: Some(1800),
            check_interval_secs: 300,
        };
        save_auto_cleanup_config(dir.path(), &config2).unwrap();

        let loaded = load_auto_cleanup_config(dir.path()).unwrap().unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.completed_retention_secs, None);
        assert_eq!(loaded.failed_retention_secs, Some(1800));
    }

    #[test]
    fn test_save_creates_file() {
        let dir = TempDir::new().unwrap();
        let config = AutoCleanupConfig::default();
        save_auto_cleanup_config(dir.path(), &config).unwrap();
        assert!(dir.path().join(CONFIG_FILENAME).exists());
    }

    #[test]
    fn test_save_no_tmp_leftover() {
        let dir = TempDir::new().unwrap();
        let config = AutoCleanupConfig::default();
        save_auto_cleanup_config(dir.path(), &config).unwrap();
        let tmp_path = dir.path().join("auto_cleanup_config.json.tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_save_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILENAME);
        fs::write(&path, "").unwrap();
        let result = load_auto_cleanup_config(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_persistence_unicode_config() {
        let dir = TempDir::new().unwrap();
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: Some(86400),
            check_interval_secs: 120,
        };
        save_auto_cleanup_config(dir.path(), &config).unwrap();
        let loaded = load_auto_cleanup_config(dir.path()).unwrap().unwrap();
        assert_eq!(loaded, config);
    }

    #[test]
    fn test_pretty_json_roundtrip() {
        let dir = TempDir::new().unwrap();
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(7200),
            failed_retention_secs: Some(86400),
            check_interval_secs: 300,
        };
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let path = dir.path().join(CONFIG_FILENAME);
        fs::write(&path, &pretty).unwrap();
        let loaded = load_auto_cleanup_config(dir.path()).unwrap().unwrap();
        assert_eq!(loaded, config);
    }

    // ========== Complex workflows ==========

    #[test]
    fn test_complete_lifecycle() {
        let dir = TempDir::new().unwrap();

        // Start with no config
        assert!(load_auto_cleanup_config(dir.path()).unwrap().is_none());

        // Create and save config
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: Some(0),
            check_interval_secs: 60,
        };
        save_auto_cleanup_config(dir.path(), &config).unwrap();

        // Load and verify
        let loaded = load_auto_cleanup_config(dir.path()).unwrap().unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.completed_retention_secs, Some(3600));

        // Check cleanup logic
        let now = Utc::now();
        let tasks = vec![
            make_task("old-complete", TaskCleanupState::Complete, 7200),
            make_task("new-complete", TaskCleanupState::Complete, 1800),
            make_task("old-fail", TaskCleanupState::Error, 10),
            make_task("active", TaskCleanupState::Other, 0),
        ];
        let to_clean = tasks_to_cleanup(&tasks, &loaded, now);
        assert_eq!(to_clean.len(), 2);
        assert!(to_clean.contains(&"old-complete".to_string()));
        assert!(to_clean.contains(&"old-fail".to_string()));
    }

    #[test]
    fn test_config_update_cycle() {
        let dir = TempDir::new().unwrap();

        // Save initial config
        let config1 = AutoCleanupConfig::immediate_completed();
        save_auto_cleanup_config(dir.path(), &config1).unwrap();

        // Verify immediate cleanup works
        let tasks = vec![make_task("t1", TaskCleanupState::Complete, 0)];
        let to_clean = tasks_to_cleanup(&tasks, &config1, Utc::now());
        assert_eq!(to_clean.len(), 1);

        // Update to disabled
        let config2 = AutoCleanupConfig::disabled();
        save_auto_cleanup_config(dir.path(), &config2).unwrap();
        let loaded = load_auto_cleanup_config(dir.path()).unwrap().unwrap();
        assert!(!loaded.enabled);

        // Same task should not be cleaned
        let to_clean = tasks_to_cleanup(&tasks, &loaded, Utc::now());
        assert!(to_clean.is_empty());
    }

    #[test]
    fn test_many_tasks_cleanup() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(0),
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let tasks: Vec<TaskCleanupData> = (0..100)
            .map(|i| make_task(&format!("task-{:03}", i), TaskCleanupState::Complete, 0))
            .collect();
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert_eq!(to_clean.len(), 100);
    }

    #[test]
    fn test_mixed_retention_policies() {
        let config = AutoCleanupConfig {
            enabled: true,
            completed_retention_secs: Some(3600),
            failed_retention_secs: None, // Never clean failed
            check_interval_secs: 60,
        };
        let tasks = vec![
            make_task("c1", TaskCleanupState::Complete, 7200), // Clean
            make_task("c2", TaskCleanupState::Complete, 1800), // Too recent
            make_task("f1", TaskCleanupState::Error, 99999),   // Never clean
            make_task("o1", TaskCleanupState::Other, 99999),   // Never clean
        ];
        let to_clean = tasks_to_cleanup(&tasks, &config, Utc::now());
        assert_eq!(to_clean.len(), 1);
        assert_eq!(to_clean[0], "c1");
    }
}
