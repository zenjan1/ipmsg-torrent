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

    #[test]
    fn test_default_config_disabled() {
        let config = AutoCleanupConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.completed_retention_secs, None);
        assert_eq!(config.failed_retention_secs, None);
        assert_eq!(config.check_interval_secs, 300);
    }

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
    fn test_parse_duration_secs() {
        assert_eq!(parse_duration_secs("30s"), Some(30));
        assert_eq!(parse_duration_secs("5m"), Some(300));
        assert_eq!(parse_duration_secs("1h"), Some(3600));
        assert_eq!(parse_duration_secs("7d"), Some(604800));
        assert_eq!(parse_duration_secs("60"), Some(60));
        assert_eq!(parse_duration_secs("0"), Some(0));
        assert_eq!(parse_duration_secs("immediate"), Some(0));
        assert_eq!(parse_duration_secs("none"), None);
        assert_eq!(parse_duration_secs("never"), None);
        assert_eq!(parse_duration_secs(""), None);
    }

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
        // Task completed 30 minutes ago, retention is 1 hour
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
        // Task completed 2 hours ago, retention is 1 hour
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
        // Task failed 40 minutes ago, retention is 30 minutes
        let task = make_task("t1", TaskCleanupState::Error, 2400);
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
            completed_retention_secs: None, // Never
            failed_retention_secs: None,
            check_interval_secs: 60,
        };
        let task = make_task("t1", TaskCleanupState::Complete, 99999);
        assert!(!should_cleanup(&task, &config, Utc::now()));
    }

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
            make_task("old-complete", TaskCleanupState::Complete, 7200), // Should clean
            make_task("new-complete", TaskCleanupState::Complete, 1800), // Too recent
            make_task("old-fail", TaskCleanupState::Error, 10),          // Should clean (immediate)
            make_task("downloading", TaskCleanupState::Other, 99999),    // Never clean
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
    fn test_format_duration() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(300), "5m");
        assert_eq!(format_duration(3600), "1h");
        assert_eq!(format_duration(86400), "1d");
        assert_eq!(format_duration(172800), "2d");
    }
}
