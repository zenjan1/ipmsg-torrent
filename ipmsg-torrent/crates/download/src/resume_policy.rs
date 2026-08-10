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
}
