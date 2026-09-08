//! Download Resume Policy - configure how tasks are restored on application startup.
//!
//! When the application restarts, tasks that were in "Downloading" state need to be
//! transitioned to a stable state. This module provides configurable policies for
//! controlling that behavior.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Policy for how to handle tasks that were downloading when the app was last shut down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ResumePolicy {
    /// Automatically resume all previously-downloading tasks (set to Queued).
    AutoResumeAll,
    /// Only auto-resume tasks with High priority (set to Queued); others go to Paused.
    AutoResumeHighPriority,
    /// Keep all previously-downloading tasks paused (safest, current default).
    #[default]
    KeepPaused,
    /// Resume only tasks that were explicitly marked as favorites/pinned.
    AutoResumeFavorites,
}

impl std::fmt::Display for ResumePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AutoResumeAll => write!(f, "auto_resume_all"),
            Self::AutoResumeHighPriority => write!(f, "auto_resume_high_priority"),
            Self::KeepPaused => write!(f, "keep_paused"),
            Self::AutoResumeFavorites => write!(f, "auto_resume_favorites"),
        }
    }
}

impl ResumePolicy {
    /// Parse from string (case-insensitive).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "auto_resume_all" | "all" | "auto" => Some(Self::AutoResumeAll),
            "auto_resume_high_priority" | "high_priority" | "high" => {
                Some(Self::AutoResumeHighPriority)
            }
            "keep_paused" | "paused" | "keep" => Some(Self::KeepPaused),
            "auto_resume_favorites" | "favorites" | "pinned" => Some(Self::AutoResumeFavorites),
            _ => None,
        }
    }
}

/// Configuration for the resume policy system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumePolicyConfig {
    /// The active resume policy.
    pub policy: ResumePolicy,
    /// Whether to also restore Error tasks to Queued (default: false).
    pub auto_retry_errors: bool,
    /// Maximum number of tasks to auto-resume (0 = unlimited).
    /// Only applies to AutoResumeAll and AutoResumeHighPriority policies.
    pub max_auto_resume: usize,
}

impl Default for ResumePolicyConfig {
    fn default() -> Self {
        Self {
            policy: ResumePolicy::KeepPaused,
            auto_retry_errors: false,
            max_auto_resume: 0,
        }
    }
}

/// Result of applying a resume policy to a set of tasks.
#[derive(Debug, Clone, Default)]
pub struct ResumePolicyResult {
    /// Number of tasks set to Queued (will auto-start).
    pub resumed: usize,
    /// Number of tasks set to Paused.
    pub paused: usize,
    /// Number of error tasks set to Queued (if auto_retry_errors enabled).
    pub errors_retried: usize,
    /// Number of tasks skipped (already in terminal or stable state).
    pub skipped: usize,
}

impl ResumePolicyResult {
    /// Format a human-readable summary.
    pub fn format_summary(&self) -> String {
        format!(
            "Resume policy applied: {} resumed, {} paused, {} errors retried, {} skipped",
            self.resumed, self.paused, self.errors_retried, self.skipped
        )
    }
}

/// Task data needed to apply resume policy (extracted from DownloadTask).
#[derive(Debug, Clone)]
pub struct TaskResumeData {
    pub id: String,
    pub state: TaskStateForResume,
    pub priority: TaskPriorityForResume,
    pub is_favorite: bool,
}

/// Simplified state for resume decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStateForResume {
    Downloading,
    Paused,
    Queued,
    Complete,
    Error,
}

/// Simplified priority for resume decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPriorityForResume {
    Low,
    Normal,
    High,
}

/// Apply the resume policy to a list of tasks.
///
/// Returns a vector of (task_id, new_state) pairs for tasks that need state changes.
pub fn apply_resume_policy(
    config: &ResumePolicyConfig,
    tasks: &[TaskResumeData],
) -> Vec<(String, TaskStateForResume)> {
    let mut changes = Vec::new();
    let mut resumed_count = 0usize;

    for task in tasks {
        let should_resume = match config.policy {
            ResumePolicy::AutoResumeAll => {
                if config.max_auto_resume > 0 && resumed_count >= config.max_auto_resume {
                    false
                } else {
                    task.state == TaskStateForResume::Downloading
                }
            }
            ResumePolicy::AutoResumeHighPriority => {
                if config.max_auto_resume > 0 && resumed_count >= config.max_auto_resume {
                    false
                } else {
                    task.state == TaskStateForResume::Downloading
                        && task.priority == TaskPriorityForResume::High
                }
            }
            ResumePolicy::KeepPaused => false,
            ResumePolicy::AutoResumeFavorites => {
                if config.max_auto_resume > 0 && resumed_count >= config.max_auto_resume {
                    false
                } else {
                    task.state == TaskStateForResume::Downloading && task.is_favorite
                }
            }
        };

        if should_resume {
            changes.push((task.id.clone(), TaskStateForResume::Queued));
            resumed_count += 1;
        } else if task.state == TaskStateForResume::Downloading {
            // Policy says don't resume → pause it
            changes.push((task.id.clone(), TaskStateForResume::Paused));
        }

        // Handle error retry
        if config.auto_retry_errors && task.state == TaskStateForResume::Error {
            changes.push((task.id.clone(), TaskStateForResume::Queued));
        }
    }

    changes
}

/// Compute a summary of what the policy would do without actually applying it.
pub fn preview_resume_policy(
    config: &ResumePolicyConfig,
    tasks: &[TaskResumeData],
) -> ResumePolicyResult {
    let mut result = ResumePolicyResult::default();

    let mut would_resume = 0usize;

    for task in tasks {
        match task.state {
            TaskStateForResume::Downloading => {
                let should_resume = match config.policy {
                    ResumePolicy::AutoResumeAll => {
                        config.max_auto_resume == 0 || would_resume < config.max_auto_resume
                    }
                    ResumePolicy::AutoResumeHighPriority => {
                        (config.max_auto_resume == 0 || would_resume < config.max_auto_resume)
                            && task.priority == TaskPriorityForResume::High
                    }
                    ResumePolicy::KeepPaused => false,
                    ResumePolicy::AutoResumeFavorites => {
                        (config.max_auto_resume == 0 || would_resume < config.max_auto_resume)
                            && task.is_favorite
                    }
                };

                if should_resume {
                    result.resumed += 1;
                    would_resume += 1;
                } else {
                    result.paused += 1;
                }
            }
            TaskStateForResume::Error if config.auto_retry_errors => {
                result.errors_retried += 1;
            }
            _ => {
                result.skipped += 1;
            }
        }
    }

    result
}

// --- Persistence ---

const CONFIG_FILENAME: &str = "resume_policy_config.json";

/// Save resume policy config to disk (atomic write).
pub fn save_resume_policy_config(
    data_dir: &Path,
    config: &ResumePolicyConfig,
) -> Result<(), String> {
    let path = data_dir.join(CONFIG_FILENAME);
    let json = serde_json::to_string_pretty(config).map_err(|e| format!("serialize: {e}"))?;

    // Atomic write: write to temp file then rename
    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, &json).map_err(|e| format!("write tmp: {e}"))?;
    std::fs::rename(&tmp_path, &path).map_err(|e| format!("rename: {e}"))?;

    Ok(())
}

/// Load resume policy config from disk.
pub fn load_resume_policy_config(data_dir: &Path) -> Option<ResumePolicyConfig> {
    let path = data_dir.join(CONFIG_FILENAME);
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_task(
        id: &str,
        state: TaskStateForResume,
        priority: TaskPriorityForResume,
        is_favorite: bool,
    ) -> TaskResumeData {
        TaskResumeData {
            id: id.to_string(),
            state,
            priority,
            is_favorite,
        }
    }

    // --- ResumePolicy tests ---

    #[test]
    fn test_default_policy_is_keep_paused() {
        assert_eq!(ResumePolicy::default(), ResumePolicy::KeepPaused);
    }

    #[test]
    fn test_policy_display() {
        assert_eq!(ResumePolicy::AutoResumeAll.to_string(), "auto_resume_all");
        assert_eq!(
            ResumePolicy::AutoResumeHighPriority.to_string(),
            "auto_resume_high_priority"
        );
        assert_eq!(ResumePolicy::KeepPaused.to_string(), "keep_paused");
        assert_eq!(
            ResumePolicy::AutoResumeFavorites.to_string(),
            "auto_resume_favorites"
        );
    }

    #[test]
    fn test_policy_from_str_loose() {
        assert_eq!(
            ResumePolicy::from_str_loose("auto_resume_all"),
            Some(ResumePolicy::AutoResumeAll)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("all"),
            Some(ResumePolicy::AutoResumeAll)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("high"),
            Some(ResumePolicy::AutoResumeHighPriority)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("paused"),
            Some(ResumePolicy::KeepPaused)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("favorites"),
            Some(ResumePolicy::AutoResumeFavorites)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("pinned"),
            Some(ResumePolicy::AutoResumeFavorites)
        );
        assert_eq!(ResumePolicy::from_str_loose("unknown"), None);
    }

    #[test]
    fn test_policy_serde_roundtrip() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            max_auto_resume: 5,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: ResumePolicyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::AutoResumeAll);
        assert!(loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 5);
    }

    // --- apply_resume_policy tests ---

    #[test]
    fn test_keep_paused_policy() {
        let config = ResumePolicyConfig::default(); // KeepPaused
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
            make_task(
                "t3",
                TaskStateForResume::Paused,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t4",
                TaskStateForResume::Complete,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];

        let changes = apply_resume_policy(&config, &tasks);

        // t1 and t2 should be paused (were downloading)
        // t3 and t4 should not change
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0], ("t1".to_string(), TaskStateForResume::Paused));
        assert_eq!(changes[1], ("t2".to_string(), TaskStateForResume::Paused));
    }

    #[test]
    fn test_auto_resume_all_policy() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
            make_task(
                "t3",
                TaskStateForResume::Paused,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];

        let changes = apply_resume_policy(&config, &tasks);

        // t1 and t2 should be queued (auto-resumed)
        // t3 unchanged
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0], ("t1".to_string(), TaskStateForResume::Queued));
        assert_eq!(changes[1], ("t2".to_string(), TaskStateForResume::Queued));
    }

    #[test]
    fn test_auto_resume_high_priority_policy() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
            make_task(
                "t3",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];

        let changes = apply_resume_policy(&config, &tasks);

        // Only t1 (high priority) should be queued
        // t2 and t3 should be paused
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0], ("t1".to_string(), TaskStateForResume::Queued));
        assert_eq!(changes[1], ("t2".to_string(), TaskStateForResume::Paused));
        assert_eq!(changes[2], ("t3".to_string(), TaskStateForResume::Paused));
    }

    #[test]
    fn test_auto_resume_favorites_policy() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeFavorites,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                true,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
        ];

        let changes = apply_resume_policy(&config, &tasks);

        // t1 is favorite → queued; t2 is not → paused
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0], ("t1".to_string(), TaskStateForResume::Queued));
        assert_eq!(changes[1], ("t2".to_string(), TaskStateForResume::Paused));
    }

    #[test]
    fn test_max_auto_resume_limit() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            max_auto_resume: 1,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "t3",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
        ];

        let changes = apply_resume_policy(&config, &tasks);

        // Only 1 task should be resumed, rest paused
        let resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        let paused = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Paused)
            .count();
        assert_eq!(resumed, 1);
        assert_eq!(paused, 2);
    }

    #[test]
    fn test_auto_retry_errors() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::KeepPaused,
            auto_retry_errors: true,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Error,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];

        let changes = apply_resume_policy(&config, &tasks);

        // t1 error → queued (auto retry); t2 downloading → paused (keep paused policy)
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0], ("t1".to_string(), TaskStateForResume::Queued));
        assert_eq!(changes[1], ("t2".to_string(), TaskStateForResume::Paused));
    }

    #[test]
    fn test_no_auto_retry_errors() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::KeepPaused,
            auto_retry_errors: false,
            ..Default::default()
        };
        let tasks = vec![make_task(
            "t1",
            TaskStateForResume::Error,
            TaskPriorityForResume::Normal,
            false,
        )];

        let changes = apply_resume_policy(&config, &tasks);
        assert!(changes.is_empty()); // Error tasks not touched
    }

    #[test]
    fn test_complete_and_queued_untouched() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Complete,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Queued,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];

        let changes = apply_resume_policy(&config, &tasks);
        assert!(changes.is_empty()); // Complete and Queued tasks not modified
    }

    // --- preview_resume_policy tests ---

    #[test]
    fn test_preview_auto_resume_all() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
            make_task(
                "t3",
                TaskStateForResume::Paused,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t4",
                TaskStateForResume::Complete,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t5",
                TaskStateForResume::Error,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];

        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 2); // t1, t2
        assert_eq!(result.paused, 0); // none were downloading and not resumed
        assert_eq!(result.errors_retried, 0); // auto_retry_errors is false
        assert_eq!(result.skipped, 3); // t3, t4, t5
    }

    #[test]
    fn test_preview_with_error_retry() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::KeepPaused,
            auto_retry_errors: true,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Error,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t3",
                TaskStateForResume::Complete,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];

        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 0); // KeepPaused
        assert_eq!(result.paused, 1); // t1
        assert_eq!(result.errors_retried, 1); // t2
        assert_eq!(result.skipped, 1); // t3
    }

    #[test]
    fn test_preview_format_summary() {
        let result = ResumePolicyResult {
            resumed: 3,
            paused: 2,
            errors_retried: 1,
            skipped: 5,
        };
        let summary = result.format_summary();
        assert!(summary.contains("3 resumed"));
        assert!(summary.contains("2 paused"));
        assert!(summary.contains("1 errors retried"));
        assert!(summary.contains("5 skipped"));
    }

    // --- Persistence tests ---

    #[test]
    fn test_save_and_load_config() {
        let tmp = TempDir::new().unwrap();
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            auto_retry_errors: true,
            max_auto_resume: 10,
        };

        save_resume_policy_config(tmp.path(), &config).unwrap();
        let loaded = load_resume_policy_config(tmp.path()).unwrap();

        assert_eq!(loaded.policy, ResumePolicy::AutoResumeHighPriority);
        assert!(loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 10);
    }

    #[test]
    fn test_load_missing_file_returns_none() {
        let tmp = TempDir::new().unwrap();
        assert!(load_resume_policy_config(tmp.path()).is_none());
    }

    #[test]
    fn test_load_corrupted_file_returns_none() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(CONFIG_FILENAME);
        std::fs::write(&path, "not valid json").unwrap();
        assert!(load_resume_policy_config(tmp.path()).is_none());
    }

    #[test]
    fn test_save_overwrites_existing() {
        let tmp = TempDir::new().unwrap();

        let config1 = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        save_resume_policy_config(tmp.path(), &config1).unwrap();

        let config2 = ResumePolicyConfig {
            policy: ResumePolicy::KeepPaused,
            auto_retry_errors: true,
            max_auto_resume: 5,
        };
        save_resume_policy_config(tmp.path(), &config2).unwrap();

        let loaded = load_resume_policy_config(tmp.path()).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::KeepPaused);
        assert!(loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 5);
    }

    #[test]
    fn test_default_config_serde() {
        let config = ResumePolicyConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: ResumePolicyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::KeepPaused);
        assert!(!loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 0);
    }

    #[test]
    fn test_preview_max_auto_resume() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            max_auto_resume: 2,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t3",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];

        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 2); // limited by max_auto_resume
        assert_eq!(result.paused, 1); // t3 gets paused
    }

    #[test]
    fn test_empty_tasks_list() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let tasks: Vec<TaskResumeData> = vec![];

        let changes = apply_resume_policy(&config, &tasks);
        assert!(changes.is_empty());

        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 0);
        assert_eq!(result.paused, 0);
    }

    // --- ResumePolicy serde per-variant ---

    #[test]
    fn test_serde_auto_resume_all() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: false,
            max_auto_resume: 0,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: ResumePolicyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::AutoResumeAll);
    }

    #[test]
    fn test_serde_auto_resume_high_priority() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            auto_retry_errors: true,
            max_auto_resume: 3,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: ResumePolicyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::AutoResumeHighPriority);
        assert!(loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 3);
    }

    #[test]
    fn test_serde_keep_paused() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::KeepPaused,
            auto_retry_errors: false,
            max_auto_resume: 0,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: ResumePolicyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::KeepPaused);
    }

    #[test]
    fn test_serde_auto_resume_favorites() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeFavorites,
            auto_retry_errors: false,
            max_auto_resume: 0,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: ResumePolicyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::AutoResumeFavorites);
    }

    #[test]
    fn test_serde_snake_case_values() {
        // Verify the JSON uses snake_case
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"auto_resume_all\""));

        let config2 = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            ..Default::default()
        };
        let json2 = serde_json::to_string(&config2).unwrap();
        assert!(json2.contains("\"auto_resume_high_priority\""));

        let config3 = ResumePolicyConfig {
            policy: ResumePolicy::KeepPaused,
            ..Default::default()
        };
        let json3 = serde_json::to_string(&config3).unwrap();
        assert!(json3.contains("\"keep_paused\""));

        let config4 = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeFavorites,
            ..Default::default()
        };
        let json4 = serde_json::to_string(&config4).unwrap();
        assert!(json4.contains("\"auto_resume_favorites\""));
    }

    #[test]
    fn test_serde_extra_fields_ignored() {
        let json = r#"{"policy":"keep_paused","auto_retry_errors":false,"max_auto_resume":0,"extra_field":"ignored","unknown":42}"#;
        let loaded: ResumePolicyConfig = serde_json::from_str(json).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::KeepPaused);
        assert!(!loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 0);
    }

    #[test]
    fn test_serde_pretty_json() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            max_auto_resume: 5,
        };
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let loaded: ResumePolicyConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::AutoResumeAll);
        assert!(loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 5);
    }

    // --- ResumePolicy traits ---

    #[test]
    fn test_policy_clone() {
        let policy = ResumePolicy::AutoResumeAll;
        let cloned = policy;
        assert_eq!(policy, cloned);
    }

    #[test]
    fn test_policy_debug() {
        let debug = format!("{:?}", ResumePolicy::AutoResumeAll);
        assert_eq!(debug, "AutoResumeAll");
        let debug2 = format!("{:?}", ResumePolicy::KeepPaused);
        assert_eq!(debug2, "KeepPaused");
        let debug3 = format!("{:?}", ResumePolicy::AutoResumeHighPriority);
        assert_eq!(debug3, "AutoResumeHighPriority");
        let debug4 = format!("{:?}", ResumePolicy::AutoResumeFavorites);
        assert_eq!(debug4, "AutoResumeFavorites");
    }

    #[test]
    fn test_policy_eq() {
        assert_eq!(ResumePolicy::AutoResumeAll, ResumePolicy::AutoResumeAll);
        assert_ne!(ResumePolicy::AutoResumeAll, ResumePolicy::KeepPaused);
        assert_ne!(
            ResumePolicy::AutoResumeHighPriority,
            ResumePolicy::AutoResumeFavorites
        );
    }

    // --- from_str_loose edge cases ---

    #[test]
    fn test_from_str_loose_case_insensitive() {
        assert_eq!(
            ResumePolicy::from_str_loose("AUTO_RESUME_ALL"),
            Some(ResumePolicy::AutoResumeAll)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("Keep_Paused"),
            Some(ResumePolicy::KeepPaused)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("HIGH_PRIORITY"),
            Some(ResumePolicy::AutoResumeHighPriority)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("FAVORITES"),
            Some(ResumePolicy::AutoResumeFavorites)
        );
    }

    #[test]
    fn test_from_str_loose_with_hyphens() {
        assert_eq!(
            ResumePolicy::from_str_loose("auto-resume-all"),
            Some(ResumePolicy::AutoResumeAll)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("auto-resume-high-priority"),
            Some(ResumePolicy::AutoResumeHighPriority)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("keep-paused"),
            Some(ResumePolicy::KeepPaused)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("auto-resume-favorites"),
            Some(ResumePolicy::AutoResumeFavorites)
        );
    }

    #[test]
    fn test_from_str_loose_empty_and_whitespace() {
        assert_eq!(ResumePolicy::from_str_loose(""), None);
        assert_eq!(ResumePolicy::from_str_loose("  "), None);
        assert_eq!(ResumePolicy::from_str_loose("\t"), None);
    }

    #[test]
    fn test_from_str_loose_unicode() {
        assert_eq!(ResumePolicy::from_str_loose("中文"), None);
        assert_eq!(ResumePolicy::from_str_loose("🎉"), None);
    }

    #[test]
    fn test_from_str_loose_all_aliases() {
        // auto_resume_all aliases
        assert_eq!(
            ResumePolicy::from_str_loose("auto_resume_all"),
            Some(ResumePolicy::AutoResumeAll)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("all"),
            Some(ResumePolicy::AutoResumeAll)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("auto"),
            Some(ResumePolicy::AutoResumeAll)
        );

        // auto_resume_high_priority aliases
        assert_eq!(
            ResumePolicy::from_str_loose("auto_resume_high_priority"),
            Some(ResumePolicy::AutoResumeHighPriority)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("high_priority"),
            Some(ResumePolicy::AutoResumeHighPriority)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("high"),
            Some(ResumePolicy::AutoResumeHighPriority)
        );

        // keep_paused aliases
        assert_eq!(
            ResumePolicy::from_str_loose("keep_paused"),
            Some(ResumePolicy::KeepPaused)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("paused"),
            Some(ResumePolicy::KeepPaused)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("keep"),
            Some(ResumePolicy::KeepPaused)
        );

        // auto_resume_favorites aliases
        assert_eq!(
            ResumePolicy::from_str_loose("auto_resume_favorites"),
            Some(ResumePolicy::AutoResumeFavorites)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("favorites"),
            Some(ResumePolicy::AutoResumeFavorites)
        );
        assert_eq!(
            ResumePolicy::from_str_loose("pinned"),
            Some(ResumePolicy::AutoResumeFavorites)
        );
    }

    // --- ResumePolicyConfig traits ---

    #[test]
    fn test_config_clone() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            max_auto_resume: 10,
        };
        let cloned = config.clone();
        assert_eq!(cloned.policy, config.policy);
        assert_eq!(cloned.auto_retry_errors, config.auto_retry_errors);
        assert_eq!(cloned.max_auto_resume, config.max_auto_resume);
    }

    #[test]
    fn test_config_clone_independence() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            max_auto_resume: 10,
        };
        let mut cloned = config.clone();
        cloned.policy = ResumePolicy::KeepPaused;
        cloned.auto_retry_errors = false;
        cloned.max_auto_resume = 0;
        // Original unchanged
        assert_eq!(config.policy, ResumePolicy::AutoResumeAll);
        assert!(config.auto_retry_errors);
        assert_eq!(config.max_auto_resume, 10);
    }

    #[test]
    fn test_config_debug() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: false,
            max_auto_resume: 5,
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("AutoResumeAll"));
        assert!(debug.contains("auto_retry_errors"));
        assert!(debug.contains("max_auto_resume"));
    }

    // --- ResumePolicyResult traits ---

    #[test]
    fn test_result_default() {
        let result = ResumePolicyResult::default();
        assert_eq!(result.resumed, 0);
        assert_eq!(result.paused, 0);
        assert_eq!(result.errors_retried, 0);
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn test_result_clone() {
        let result = ResumePolicyResult {
            resumed: 5,
            paused: 3,
            errors_retried: 1,
            skipped: 2,
        };
        let cloned = result.clone();
        assert_eq!(cloned.resumed, 5);
        assert_eq!(cloned.paused, 3);
        assert_eq!(cloned.errors_retried, 1);
        assert_eq!(cloned.skipped, 2);
    }

    #[test]
    fn test_result_debug() {
        let result = ResumePolicyResult {
            resumed: 1,
            paused: 2,
            errors_retried: 3,
            skipped: 4,
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("resumed: 1"));
        assert!(debug.contains("paused: 2"));
        assert!(debug.contains("errors_retried: 3"));
        assert!(debug.contains("skipped: 4"));
    }

    #[test]
    fn test_format_summary_zero_values() {
        let result = ResumePolicyResult::default();
        let summary = result.format_summary();
        assert!(summary.contains("0 resumed"));
        assert!(summary.contains("0 paused"));
        assert!(summary.contains("0 errors retried"));
        assert!(summary.contains("0 skipped"));
    }

    #[test]
    fn test_format_summary_large_values() {
        let result = ResumePolicyResult {
            resumed: 1000,
            paused: 2000,
            errors_retried: 500,
            skipped: 3000,
        };
        let summary = result.format_summary();
        assert!(summary.contains("1000 resumed"));
        assert!(summary.contains("2000 paused"));
        assert!(summary.contains("500 errors retried"));
        assert!(summary.contains("3000 skipped"));
    }

    // --- TaskStateForResume traits ---

    #[test]
    fn test_task_state_clone_copy() {
        let state = TaskStateForResume::Downloading;
        let cloned = state;
        let copied = state;
        assert_eq!(cloned, copied);
        assert_eq!(state, cloned);
    }

    #[test]
    fn test_task_state_debug() {
        assert_eq!(format!("{:?}", TaskStateForResume::Downloading), "Downloading");
        assert_eq!(format!("{:?}", TaskStateForResume::Paused), "Paused");
        assert_eq!(format!("{:?}", TaskStateForResume::Queued), "Queued");
        assert_eq!(format!("{:?}", TaskStateForResume::Complete), "Complete");
        assert_eq!(format!("{:?}", TaskStateForResume::Error), "Error");
    }

    #[test]
    fn test_task_state_eq() {
        assert_eq!(
            TaskStateForResume::Downloading,
            TaskStateForResume::Downloading
        );
        assert_ne!(
            TaskStateForResume::Downloading,
            TaskStateForResume::Paused
        );
        assert_ne!(
            TaskStateForResume::Complete,
            TaskStateForResume::Error
        );
    }

    #[test]
    fn test_task_state_all_variants_distinct() {
        let states = vec![
            TaskStateForResume::Downloading,
            TaskStateForResume::Paused,
            TaskStateForResume::Queued,
            TaskStateForResume::Complete,
            TaskStateForResume::Error,
        ];
        for i in 0..states.len() {
            for j in (i + 1)..states.len() {
                assert_ne!(states[i], states[j]);
            }
        }
    }

    // --- TaskPriorityForResume traits ---

    #[test]
    fn test_task_priority_clone_copy() {
        let priority = TaskPriorityForResume::High;
        let cloned = priority;
        let copied = priority;
        assert_eq!(cloned, copied);
        assert_eq!(priority, cloned);
    }

    #[test]
    fn test_task_priority_debug() {
        assert_eq!(format!("{:?}", TaskPriorityForResume::Low), "Low");
        assert_eq!(format!("{:?}", TaskPriorityForResume::Normal), "Normal");
        assert_eq!(format!("{:?}", TaskPriorityForResume::High), "High");
    }

    #[test]
    fn test_task_priority_eq() {
        assert_eq!(TaskPriorityForResume::Low, TaskPriorityForResume::Low);
        assert_ne!(TaskPriorityForResume::Low, TaskPriorityForResume::High);
        assert_ne!(
            TaskPriorityForResume::Normal,
            TaskPriorityForResume::High
        );
    }

    #[test]
    fn test_task_priority_all_variants_distinct() {
        let priorities = vec![
            TaskPriorityForResume::Low,
            TaskPriorityForResume::Normal,
            TaskPriorityForResume::High,
        ];
        for i in 0..priorities.len() {
            for j in (i + 1)..priorities.len() {
                assert_ne!(priorities[i], priorities[j]);
            }
        }
    }

    // --- TaskResumeData traits ---

    #[test]
    fn test_task_resume_data_clone() {
        let task = make_task(
            "task-1",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::High,
            true,
        );
        let cloned = task.clone();
        assert_eq!(cloned.id, "task-1");
        assert_eq!(cloned.state, TaskStateForResume::Downloading);
        assert_eq!(cloned.priority, TaskPriorityForResume::High);
        assert!(cloned.is_favorite);
    }

    #[test]
    fn test_task_resume_data_clone_independence() {
        let task = make_task(
            "task-1",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::High,
            true,
        );
        let mut cloned = task.clone();
        cloned.id = "modified".to_string();
        cloned.state = TaskStateForResume::Paused;
        // Original unchanged
        assert_eq!(task.id, "task-1");
        assert_eq!(task.state, TaskStateForResume::Downloading);
    }

    #[test]
    fn test_task_resume_data_debug() {
        let task = make_task(
            "task-1",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::High,
            true,
        );
        let debug = format!("{:?}", task);
        assert!(debug.contains("task-1"));
        assert!(debug.contains("Downloading"));
        assert!(debug.contains("High"));
    }

    #[test]
    fn test_task_resume_data_unicode_id() {
        let task = make_task(
            "任务-中文",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::Normal,
            false,
        );
        assert_eq!(task.id, "任务-中文");
    }

    #[test]
    fn test_task_resume_data_emoji_id() {
        let task = make_task(
            "task-🎉",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::Normal,
            false,
        );
        assert_eq!(task.id, "task-🎉");
    }

    // --- apply_resume_policy additional tests ---

    #[test]
    fn test_apply_single_downloading_task_auto_resume() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let tasks = vec![make_task(
            "t1",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::Normal,
            false,
        )];
        let changes = apply_resume_policy(&config, &tasks);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].1, TaskStateForResume::Queued);
    }

    #[test]
    fn test_apply_single_downloading_task_keep_paused() {
        let config = ResumePolicyConfig::default();
        let tasks = vec![make_task(
            "t1",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::Normal,
            false,
        )];
        let changes = apply_resume_policy(&config, &tasks);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].1, TaskStateForResume::Paused);
    }

    #[test]
    fn test_apply_max_auto_resume_zero_means_unlimited() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            max_auto_resume: 0,
            ..Default::default()
        };
        let tasks: Vec<TaskResumeData> = (0..100)
            .map(|i| {
                make_task(
                    &format!("t{i}"),
                    TaskStateForResume::Downloading,
                    TaskPriorityForResume::Normal,
                    false,
                )
            })
            .collect();
        let changes = apply_resume_policy(&config, &tasks);
        let resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        assert_eq!(resumed, 100);
    }

    #[test]
    fn test_apply_max_auto_resume_exact_boundary() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            max_auto_resume: 3,
            ..Default::default()
        };
        let tasks: Vec<TaskResumeData> = (0..3)
            .map(|i| {
                make_task(
                    &format!("t{i}"),
                    TaskStateForResume::Downloading,
                    TaskPriorityForResume::Normal,
                    false,
                )
            })
            .collect();
        let changes = apply_resume_policy(&config, &tasks);
        // All 3 should be resumed (exactly at limit)
        assert_eq!(changes.len(), 3);
        assert!(changes
            .iter()
            .all(|(_, s)| *s == TaskStateForResume::Queued));
    }

    #[test]
    fn test_apply_max_auto_resume_one_over_boundary() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            max_auto_resume: 3,
            ..Default::default()
        };
        let tasks: Vec<TaskResumeData> = (0..4)
            .map(|i| {
                make_task(
                    &format!("t{i}"),
                    TaskStateForResume::Downloading,
                    TaskPriorityForResume::Normal,
                    false,
                )
            })
            .collect();
        let changes = apply_resume_policy(&config, &tasks);
        let resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        let paused = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Paused)
            .count();
        assert_eq!(resumed, 3);
        assert_eq!(paused, 1);
    }

    #[test]
    fn test_apply_multiple_error_tasks_with_retry() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::KeepPaused,
            auto_retry_errors: true,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "e1",
                TaskStateForResume::Error,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "e2",
                TaskStateForResume::Error,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "e3",
                TaskStateForResume::Error,
                TaskPriorityForResume::Low,
                false,
            ),
        ];
        let changes = apply_resume_policy(&config, &tasks);
        assert_eq!(changes.len(), 3);
        assert!(changes
            .iter()
            .all(|(_, s)| *s == TaskStateForResume::Queued));
    }

    #[test]
    fn test_apply_unicode_task_ids() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "任务-一",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "任务-二",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];
        let changes = apply_resume_policy(&config, &tasks);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].0, "任务-一");
        assert_eq!(changes[1].0, "任务-二");
    }

    #[test]
    fn test_apply_emoji_task_ids() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let tasks = vec![make_task(
            "task-🚀",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::Normal,
            false,
        )];
        let changes = apply_resume_policy(&config, &tasks);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, "task-🚀");
    }

    #[test]
    fn test_apply_empty_task_id() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let tasks = vec![make_task(
            "",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::Normal,
            false,
        )];
        let changes = apply_resume_policy(&config, &tasks);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, "");
    }

    #[test]
    fn test_apply_mixed_states_all_policies() {
        let policies = [
            ResumePolicy::AutoResumeAll,
            ResumePolicy::AutoResumeHighPriority,
            ResumePolicy::KeepPaused,
            ResumePolicy::AutoResumeFavorites,
        ];
        for policy in &policies {
            let config = ResumePolicyConfig {
                policy: *policy,
                ..Default::default()
            };
            let tasks = vec![
                make_task(
                    "complete",
                    TaskStateForResume::Complete,
                    TaskPriorityForResume::Normal,
                    false,
                ),
                make_task(
                    "queued",
                    TaskStateForResume::Queued,
                    TaskPriorityForResume::Normal,
                    false,
                ),
                make_task(
                    "paused",
                    TaskStateForResume::Paused,
                    TaskPriorityForResume::Normal,
                    false,
                ),
            ];
            // Complete, Queued, Paused tasks should never be modified
            let changes = apply_resume_policy(&config, &tasks);
            assert!(changes.is_empty());
        }
    }

    #[test]
    fn test_apply_high_priority_max_resume() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            max_auto_resume: 1,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "h1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "h2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "l1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
        ];
        let changes = apply_resume_policy(&config, &tasks);
        let resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        assert_eq!(resumed, 1); // Only 1 high priority resumed (limit)
    }

    #[test]
    fn test_apply_favorites_max_resume() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeFavorites,
            max_auto_resume: 1,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "f1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                true,
            ),
            make_task(
                "f2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                true,
            ),
        ];
        let changes = apply_resume_policy(&config, &tasks);
        let resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        assert_eq!(resumed, 1); // Only 1 favorite resumed (limit)
    }

    // --- preview_resume_policy additional tests ---

    #[test]
    fn test_preview_empty_tasks() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let result = preview_resume_policy(&config, &[]);
        assert_eq!(result.resumed, 0);
        assert_eq!(result.paused, 0);
        assert_eq!(result.errors_retried, 0);
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn test_preview_only_complete_and_queued() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "c1",
                TaskStateForResume::Complete,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "q1",
                TaskStateForResume::Queued,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];
        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 0);
        assert_eq!(result.paused, 0);
        assert_eq!(result.errors_retried, 0);
        assert_eq!(result.skipped, 2);
    }

    #[test]
    fn test_preview_only_error_tasks_no_retry() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: false,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "e1",
                TaskStateForResume::Error,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "e2",
                TaskStateForResume::Error,
                TaskPriorityForResume::High,
                false,
            ),
        ];
        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 0);
        assert_eq!(result.paused, 0);
        assert_eq!(result.errors_retried, 0);
        assert_eq!(result.skipped, 2);
    }

    #[test]
    fn test_preview_keep_paused_with_downloading() {
        let config = ResumePolicyConfig::default(); // KeepPaused
        let tasks = vec![
            make_task(
                "d1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                true,
            ),
            make_task(
                "d2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
        ];
        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 0);
        assert_eq!(result.paused, 2); // Both paused
    }

    #[test]
    fn test_preview_high_priority_with_max() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            max_auto_resume: 1,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "h1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "h2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "l1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
        ];
        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 1);
        assert_eq!(result.paused, 2); // h2 + l1
    }

    #[test]
    fn test_preview_favorites_with_max() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeFavorites,
            max_auto_resume: 1,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "f1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                true,
            ),
            make_task(
                "f2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                true,
            ),
        ];
        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 1);
        assert_eq!(result.paused, 1);
    }

    #[test]
    fn test_preview_all_states_mixed() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "d1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "p1",
                TaskStateForResume::Paused,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "q1",
                TaskStateForResume::Queued,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "c1",
                TaskStateForResume::Complete,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "e1",
                TaskStateForResume::Error,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];
        let result = preview_resume_policy(&config, &tasks);
        assert_eq!(result.resumed, 1); // d1
        assert_eq!(result.paused, 0);
        assert_eq!(result.errors_retried, 1); // e1
        assert_eq!(result.skipped, 3); // p1, q1, c1
    }

    #[test]
    fn test_preview_consistency_with_apply() {
        // preview and apply should agree on counts
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            auto_retry_errors: true,
            max_auto_resume: 2,
        };
        let tasks = vec![
            make_task(
                "h1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "h2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "l1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
            make_task(
                "e1",
                TaskStateForResume::Error,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "c1",
                TaskStateForResume::Complete,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];
        let preview = preview_resume_policy(&config, &tasks);
        let changes = apply_resume_policy(&config, &tasks);

        let apply_resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        // apply_resume_policy generates Queued changes for both resumed downloads
        // and error retries. preview.resumed counts only downloads resumed,
        // while preview.errors_retried counts error retries. Together they
        // should equal the total Queued changes from apply.
        assert_eq!(preview.resumed + preview.errors_retried, apply_resumed);
    }

    // --- Persistence additional tests ---

    #[test]
    fn test_save_no_tmp_left() {
        let tmp = TempDir::new().unwrap();
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        save_resume_policy_config(tmp.path(), &config).unwrap();
        // No .tmp file should remain
        let tmp_file = tmp.path().join("resume_policy_config.tmp");
        assert!(!tmp_file.exists());
    }

    #[test]
    fn test_save_creates_file() {
        let tmp = TempDir::new().unwrap();
        let config = ResumePolicyConfig::default();
        save_resume_policy_config(tmp.path(), &config).unwrap();
        let path = tmp.path().join(CONFIG_FILENAME);
        assert!(path.exists());
    }

    #[test]
    fn test_load_empty_file_returns_none() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(CONFIG_FILENAME);
        std::fs::write(&path, "").unwrap();
        assert!(load_resume_policy_config(tmp.path()).is_none());
    }

    #[test]
    fn test_save_pretty_json_format() {
        let tmp = TempDir::new().unwrap();
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            max_auto_resume: 5,
        };
        save_resume_policy_config(tmp.path(), &config).unwrap();
        let path = tmp.path().join(CONFIG_FILENAME);
        let content = std::fs::read_to_string(&path).unwrap();
        // Pretty JSON has newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
    }

    #[test]
    fn test_persistence_unicode_path() {
        let tmp = TempDir::new().unwrap();
        let unicode_dir = tmp.path().join("中文目录");
        std::fs::create_dir(&unicode_dir).unwrap();
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeFavorites,
            auto_retry_errors: true,
            max_auto_resume: 7,
        };
        save_resume_policy_config(&unicode_dir, &config).unwrap();
        let loaded = load_resume_policy_config(&unicode_dir).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::AutoResumeFavorites);
        assert!(loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 7);
    }

    #[test]
    fn test_persistence_unicode_values() {
        // Test that config with unicode-adjacent values round-trips
        let tmp = TempDir::new().unwrap();
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            max_auto_resume: 42,
        };
        save_resume_policy_config(tmp.path(), &config).unwrap();
        let loaded = load_resume_policy_config(tmp.path()).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::AutoResumeAll);
        assert!(loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 42);
    }

    #[test]
    fn test_persistence_full_roundtrip_all_policies() {
        let tmp = TempDir::new().unwrap();
        let policies = [
            ResumePolicy::AutoResumeAll,
            ResumePolicy::AutoResumeHighPriority,
            ResumePolicy::KeepPaused,
            ResumePolicy::AutoResumeFavorites,
        ];
        for policy in &policies {
            let config = ResumePolicyConfig {
                policy: *policy,
                auto_retry_errors: true,
                max_auto_resume: 99,
            };
            save_resume_policy_config(tmp.path(), &config).unwrap();
            let loaded = load_resume_policy_config(tmp.path()).unwrap();
            assert_eq!(loaded.policy, *policy);
            assert!(loaded.auto_retry_errors);
            assert_eq!(loaded.max_auto_resume, 99);
        }
    }

    // --- Edge cases ---

    #[test]
    fn test_large_max_auto_resume() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            max_auto_resume: usize::MAX,
            ..Default::default()
        };
        let tasks = vec![make_task(
            "t1",
            TaskStateForResume::Downloading,
            TaskPriorityForResume::Normal,
            false,
        )];
        let changes = apply_resume_policy(&config, &tasks);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].1, TaskStateForResume::Queued);
    }

    #[test]
    fn test_max_auto_resume_one() {
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            max_auto_resume: 1,
            ..Default::default()
        };
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];
        let changes = apply_resume_policy(&config, &tasks);
        let resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        assert_eq!(resumed, 1);
    }

    // --- Complete workflow ---

    #[test]
    fn test_complete_workflow() {
        let tmp = TempDir::new().unwrap();

        // 1. Create config
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            auto_retry_errors: true,
            max_auto_resume: 5,
        };

        // 2. Save config
        save_resume_policy_config(tmp.path(), &config).unwrap();

        // 3. Load config
        let loaded = load_resume_policy_config(tmp.path()).unwrap();
        assert_eq!(loaded.policy, ResumePolicy::AutoResumeAll);
        assert!(loaded.auto_retry_errors);
        assert_eq!(loaded.max_auto_resume, 5);

        // 4. Preview policy
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "t2",
                TaskStateForResume::Error,
                TaskPriorityForResume::Normal,
                false,
            ),
            make_task(
                "t3",
                TaskStateForResume::Complete,
                TaskPriorityForResume::Normal,
                false,
            ),
        ];
        let preview = preview_resume_policy(&loaded, &tasks);
        assert_eq!(preview.resumed, 1);
        assert_eq!(preview.errors_retried, 1);
        assert_eq!(preview.skipped, 1);

        // 5. Apply policy
        let changes = apply_resume_policy(&loaded, &tasks);
        let resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        assert_eq!(resumed, 2); // t1 resumed + t2 error retried

        // 6. Verify summary
        let summary = preview.format_summary();
        assert!(summary.contains("1 resumed"));
        assert!(summary.contains("1 errors retried"));
    }

    #[test]
    fn test_multiple_policies_independent() {
        let tasks = vec![
            make_task(
                "t1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                true,
            ),
            make_task(
                "t2",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
        ];

        // AutoResumeAll
        let config_all = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeAll,
            ..Default::default()
        };
        let changes_all = apply_resume_policy(&config_all, &tasks);
        assert_eq!(changes_all.len(), 2);
        assert!(changes_all
            .iter()
            .all(|(_, s)| *s == TaskStateForResume::Queued));

        // KeepPaused
        let config_keep = ResumePolicyConfig::default();
        let changes_keep = apply_resume_policy(&config_keep, &tasks);
        assert_eq!(changes_keep.len(), 2);
        assert!(changes_keep
            .iter()
            .all(|(_, s)| *s == TaskStateForResume::Paused));

        // AutoResumeHighPriority
        let config_high = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            ..Default::default()
        };
        let changes_high = apply_resume_policy(&config_high, &tasks);
        let resumed = changes_high
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        assert_eq!(resumed, 1); // Only high priority

        // AutoResumeFavorites
        let config_fav = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeFavorites,
            ..Default::default()
        };
        let changes_fav = apply_resume_policy(&config_fav, &tasks);
        let resumed = changes_fav
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        assert_eq!(resumed, 1); // Only favorites
    }

    #[test]
    fn test_save_load_apply_cycle() {
        let tmp = TempDir::new().unwrap();

        // Save config
        let config = ResumePolicyConfig {
            policy: ResumePolicy::AutoResumeHighPriority,
            auto_retry_errors: false,
            max_auto_resume: 0,
        };
        save_resume_policy_config(tmp.path(), &config).unwrap();

        // Load and apply
        let loaded = load_resume_policy_config(tmp.path()).unwrap();
        let tasks = vec![
            make_task(
                "h1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::High,
                false,
            ),
            make_task(
                "l1",
                TaskStateForResume::Downloading,
                TaskPriorityForResume::Low,
                false,
            ),
        ];
        let changes = apply_resume_policy(&loaded, &tasks);
        let resumed = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Queued)
            .count();
        assert_eq!(resumed, 1); // Only high priority
        let paused = changes
            .iter()
            .filter(|(_, s)| *s == TaskStateForResume::Paused)
            .count();
        assert_eq!(paused, 1); // Low priority paused
    }

    #[test]
    fn test_config_filename_constant() {
        assert_eq!(CONFIG_FILENAME, "resume_policy_config.json");
    }
}
