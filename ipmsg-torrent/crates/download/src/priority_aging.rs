//! Priority Aging - Automatic priority boost for long-waiting tasks.
//!
//! Tasks that have been queued for a long time can be starved by
//! continuously arriving high-priority tasks. Priority aging automatically
//! boosts the priority of tasks that exceed configurable wait-time thresholds.
//!
//! Features:
//! - Configurable aging thresholds per priority level transition
//! - Optional "aging cap" to prevent aging beyond a target priority
//! - Persistence to `priority_aging_config.json` (atomic write)
//! - Integration with the scheduler's task selection logic
//! - CLI and REST API support

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

/// Errors from priority aging operations.
#[derive(Error, Debug)]
pub enum PriorityAgingError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid threshold: {0}")]
    InvalidThreshold(String),
}

/// Priority levels used for aging configuration.
/// Mirrors DownloadPriority but is self-contained for the aging module.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum AgingPriority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
}

impl AgingPriority {
    /// Convert from DownloadPriority.
    pub fn from_download_priority(p: crate::DownloadPriority) -> Self {
        match p {
            crate::DownloadPriority::Low => Self::Low,
            crate::DownloadPriority::Normal => Self::Normal,
            crate::DownloadPriority::High => Self::High,
        }
    }

    /// Convert to DownloadPriority.
    pub fn to_download_priority(self) -> crate::DownloadPriority {
        match self {
            Self::Low => crate::DownloadPriority::Low,
            Self::Normal => crate::DownloadPriority::Normal,
            Self::High => crate::DownloadPriority::High,
        }
    }
}

/// Configuration for priority aging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityAgingConfig {
    /// Enable priority aging globally.
    pub enabled: bool,
    /// Time (seconds) a Low-priority task can wait before being boosted to Normal.
    /// Default: 3600 (1 hour).
    pub low_to_normal_secs: u64,
    /// Time (seconds) a Normal-priority task can wait before being boosted to High.
    /// Default: 7200 (2 hours).
    pub normal_to_high_secs: u64,
    /// Maximum priority a task can be aged to (default: High).
    /// Set to Normal to prevent aging to High.
    pub max_aged_priority: AgingPriority,
    /// How often (seconds) the aging check runs. Default: 60.
    pub check_interval_secs: u64,
}

impl Default for PriorityAgingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            low_to_normal_secs: 3600,
            normal_to_high_secs: 7200,
            max_aged_priority: AgingPriority::High,
            check_interval_secs: 60,
        }
    }
}

/// Input data for a single task needed by the aging algorithm.
#[derive(Debug, Clone)]
pub struct TaskAgingData {
    pub id: String,
    pub priority: AgingPriority,
    pub queued_at: Option<DateTime<Utc>>,
    pub state: crate::DownloadState,
}

/// Result of an aging evaluation for a single task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgingDecision {
    pub task_id: String,
    pub old_priority: AgingPriority,
    pub new_priority: AgingPriority,
    pub wait_secs: u64,
}

/// Evaluate whether a task should be priority-boosted.
///
/// Returns `Some(decision)` if the task should be boosted, `None` otherwise.
pub fn evaluate_task_aging(
    task: &TaskAgingData,
    config: &PriorityAgingConfig,
    now: DateTime<Utc>,
) -> Option<AgingDecision> {
    // Only age tasks that are Queued
    if task.state != crate::DownloadState::Queued {
        return None;
    }

    // Don't age tasks already at or above the max aged priority
    if task.priority >= config.max_aged_priority {
        return None;
    }

    let queued_at = task.queued_at?;
    let wait_duration = now.signed_duration_since(queued_at);
    if wait_duration.num_seconds() < 0 {
        return None; // queued_at is in the future (shouldn't happen)
    }
    let wait_secs = wait_duration.num_seconds() as u64;

    let new_priority = match task.priority {
        AgingPriority::Low => {
            if wait_secs >= config.low_to_normal_secs {
                AgingPriority::Normal.min(config.max_aged_priority)
            } else {
                return None;
            }
        }
        AgingPriority::Normal => {
            if wait_secs >= config.normal_to_high_secs {
                AgingPriority::High.min(config.max_aged_priority)
            } else {
                return None;
            }
        }
        AgingPriority::High => return None, // Already at max
    };

    if new_priority > task.priority {
        Some(AgingDecision {
            task_id: task.id.clone(),
            old_priority: task.priority,
            new_priority,
            wait_secs,
        })
    } else {
        None
    }
}

/// Evaluate aging for a batch of tasks. Returns all tasks that should be boosted.
pub fn evaluate_batch_aging(
    tasks: &[TaskAgingData],
    config: &PriorityAgingConfig,
    now: DateTime<Utc>,
) -> Vec<AgingDecision> {
    if !config.enabled {
        return Vec::new();
    }
    tasks
        .iter()
        .filter_map(|t| evaluate_task_aging(t, config, now))
        .collect()
}

/// Format a wait duration for display.
pub fn format_wait_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{hours}h{mins}m")
    }
}

// --- Persistence ---

const PRIORITY_AGING_CONFIG_FILE: &str = "priority_aging_config.json";

/// Save priority aging config to disk (atomic write).
pub fn save_priority_aging_config(
    config: &PriorityAgingConfig,
    data_dir: &Path,
) -> Result<(), PriorityAgingError> {
    let path = data_dir.join(PRIORITY_AGING_CONFIG_FILE);
    let json = serde_json::to_string_pretty(config)?;
    let tmp_path = data_dir.join("priority_aging_config.json.tmp");
    std::fs::write(&tmp_path, json.as_bytes())?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load priority aging config from disk.
pub fn load_priority_aging_config(
    data_dir: &Path,
) -> Result<PriorityAgingConfig, PriorityAgingError> {
    let path = data_dir.join(PRIORITY_AGING_CONFIG_FILE);
    if !path.exists() {
        return Ok(PriorityAgingConfig::default());
    }
    let json = std::fs::read_to_string(&path)?;
    let config: PriorityAgingConfig = serde_json::from_str(&json)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DownloadState;
    use tempfile::tempdir;

    fn make_task(
        id: &str,
        priority: AgingPriority,
        queued_at: Option<DateTime<Utc>>,
        state: DownloadState,
    ) -> TaskAgingData {
        TaskAgingData {
            id: id.to_string(),
            priority,
            queued_at,
            state,
        }
    }

    #[test]
    fn test_default_config() {
        let config = PriorityAgingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.low_to_normal_secs, 3600);
        assert_eq!(config.normal_to_high_secs, 7200);
        assert_eq!(config.max_aged_priority, AgingPriority::High);
        assert_eq!(config.check_interval_secs, 60);
    }

    #[test]
    fn test_aging_priority_conversion() {
        assert_eq!(
            AgingPriority::from_download_priority(crate::DownloadPriority::Low),
            AgingPriority::Low
        );
        assert_eq!(
            AgingPriority::from_download_priority(crate::DownloadPriority::Normal),
            AgingPriority::Normal
        );
        assert_eq!(
            AgingPriority::from_download_priority(crate::DownloadPriority::High),
            AgingPriority::High
        );
        assert_eq!(
            AgingPriority::Low.to_download_priority(),
            crate::DownloadPriority::Low
        );
        assert_eq!(
            AgingPriority::Normal.to_download_priority(),
            crate::DownloadPriority::Normal
        );
        assert_eq!(
            AgingPriority::High.to_download_priority(),
            crate::DownloadPriority::High
        );
    }

    #[test]
    fn test_no_aging_when_disabled() {
        let config = PriorityAgingConfig {
            enabled: false,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Queued,
        );
        let decisions = evaluate_batch_aging(&[task], &config, now);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_low_to_normal_after_threshold() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 3600,
            ..Default::default()
        };
        let now = Utc::now();
        // Task waiting for 2 hours (7200s > 3600s threshold)
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(7200)),
            DownloadState::Queued,
        );
        let decisions = evaluate_batch_aging(&[task], &config, now);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].old_priority, AgingPriority::Low);
        assert_eq!(decisions[0].new_priority, AgingPriority::Normal);
        assert!(decisions[0].wait_secs >= 7200);
    }

    #[test]
    fn test_no_aging_below_threshold() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 3600,
            ..Default::default()
        };
        let now = Utc::now();
        // Task waiting for 30 minutes (1800s < 3600s threshold)
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(1800)),
            DownloadState::Queued,
        );
        let decisions = evaluate_batch_aging(&[task], &config, now);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_normal_to_high_after_threshold() {
        let config = PriorityAgingConfig {
            enabled: true,
            normal_to_high_secs: 7200,
            ..Default::default()
        };
        let now = Utc::now();
        // Task waiting for 3 hours (10800s > 7200s threshold)
        let task = make_task(
            "t1",
            AgingPriority::Normal,
            Some(now - chrono::Duration::seconds(10800)),
            DownloadState::Queued,
        );
        let decisions = evaluate_batch_aging(&[task], &config, now);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].old_priority, AgingPriority::Normal);
        assert_eq!(decisions[0].new_priority, AgingPriority::High);
    }

    #[test]
    fn test_no_aging_for_high_priority() {
        let config = PriorityAgingConfig {
            enabled: true,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::High,
            Some(now - chrono::Duration::seconds(100000)),
            DownloadState::Queued,
        );
        let decisions = evaluate_batch_aging(&[task], &config, now);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_max_aged_priority_cap() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            normal_to_high_secs: 120,
            max_aged_priority: AgingPriority::Normal, // Cap at Normal
            ..Default::default()
        };
        let now = Utc::now();
        // Low task should age to Normal (within cap)
        let low_task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(300)),
            DownloadState::Queued,
        );
        let decisions = evaluate_batch_aging(&[low_task], &config, now);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].new_priority, AgingPriority::Normal);

        // Normal task should NOT age to High (exceeds cap)
        let normal_task = make_task(
            "t2",
            AgingPriority::Normal,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Queued,
        );
        let decisions = evaluate_batch_aging(&[normal_task], &config, now);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_no_aging_for_non_queued_tasks() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        // Downloading task
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Downloading,
        );
        let decisions = evaluate_batch_aging(&[task], &config, now);
        assert!(decisions.is_empty());

        // Paused task
        let task2 = make_task(
            "t2",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Paused,
        );
        let decisions = evaluate_batch_aging(&[task2], &config, now);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_no_aging_without_queued_at() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task("t1", AgingPriority::Low, None, DownloadState::Queued);
        let decisions = evaluate_batch_aging(&[task], &config, now);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_batch_aging_mixed() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 3600,
            normal_to_high_secs: 7200,
            ..Default::default()
        };
        let now = Utc::now();
        let tasks = vec![
            // Low, waiting 2h -> should age to Normal
            make_task(
                "t1",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(7200)),
                DownloadState::Queued,
            ),
            // Low, waiting 30m -> no aging
            make_task(
                "t2",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(1800)),
                DownloadState::Queued,
            ),
            // Normal, waiting 3h -> should age to High
            make_task(
                "t3",
                AgingPriority::Normal,
                Some(now - chrono::Duration::seconds(10800)),
                DownloadState::Queued,
            ),
            // High, waiting forever -> no aging
            make_task(
                "t4",
                AgingPriority::High,
                Some(now - chrono::Duration::seconds(100000)),
                DownloadState::Queued,
            ),
        ];
        let decisions = evaluate_batch_aging(&tasks, &config, now);
        assert_eq!(decisions.len(), 2);
        assert!(decisions.iter().any(|d| d.task_id == "t1"));
        assert!(decisions.iter().any(|d| d.task_id == "t3"));
    }

    #[test]
    fn test_format_wait_duration() {
        assert_eq!(format_wait_duration(30), "30s");
        assert_eq!(format_wait_duration(90), "1m30s");
        assert_eq!(format_wait_duration(3661), "1h1m");
        assert_eq!(format_wait_duration(7200), "2h0m");
        assert_eq!(format_wait_duration(0), "0s");
    }

    #[test]
    fn test_persistence_save_load() {
        let dir = tempdir().unwrap();
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 1800,
            normal_to_high_secs: 3600,
            max_aged_priority: AgingPriority::Normal,
            check_interval_secs: 30,
        };
        save_priority_aging_config(&config, dir.path()).unwrap();
        let loaded = load_priority_aging_config(dir.path()).unwrap();
        assert_eq!(loaded.enabled, true);
        assert_eq!(loaded.low_to_normal_secs, 1800);
        assert_eq!(loaded.normal_to_high_secs, 3600);
        assert_eq!(loaded.max_aged_priority, AgingPriority::Normal);
        assert_eq!(loaded.check_interval_secs, 30);
    }

    #[test]
    fn test_persistence_load_missing_file() {
        let dir = tempdir().unwrap();
        let config = load_priority_aging_config(dir.path()).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.low_to_normal_secs, 3600);
    }

    #[test]
    fn test_persistence_overwrite() {
        let dir = tempdir().unwrap();
        let config1 = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 100,
            ..Default::default()
        };
        save_priority_aging_config(&config1, dir.path()).unwrap();
        let config2 = PriorityAgingConfig {
            enabled: false,
            low_to_normal_secs: 200,
            ..Default::default()
        };
        save_priority_aging_config(&config2, dir.path()).unwrap();
        let loaded = load_priority_aging_config(dir.path()).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.low_to_normal_secs, 200);
    }

    #[test]
    fn test_evaluate_single_task() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(120)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now);
        assert!(decision.is_some());
        let d = decision.unwrap();
        assert_eq!(d.task_id, "t1");
        assert_eq!(d.old_priority, AgingPriority::Low);
        assert_eq!(d.new_priority, AgingPriority::Normal);
    }

    #[test]
    fn test_evaluate_task_future_queued_at() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        // queued_at in the future
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now + chrono::Duration::seconds(3600)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now);
        assert!(decision.is_none());
    }

    #[test]
    fn test_aging_at_exact_threshold() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 3600,
            ..Default::default()
        };
        let now = Utc::now();
        // Exactly at threshold
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(3600)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now);
        assert!(decision.is_some());
        assert_eq!(decision.unwrap().new_priority, AgingPriority::Normal);
    }

    #[test]
    fn test_aging_one_second_before_threshold() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 3600,
            ..Default::default()
        };
        let now = Utc::now();
        // One second before threshold
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(3599)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now);
        assert!(decision.is_none());
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 1234,
            normal_to_high_secs: 5678,
            max_aged_priority: AgingPriority::Normal,
            check_interval_secs: 42,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PriorityAgingConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.low_to_normal_secs, config.low_to_normal_secs);
        assert_eq!(deserialized.normal_to_high_secs, config.normal_to_high_secs);
        assert_eq!(deserialized.max_aged_priority, config.max_aged_priority);
        assert_eq!(deserialized.check_interval_secs, config.check_interval_secs);
    }
}
