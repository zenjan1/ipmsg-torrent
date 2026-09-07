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

    // ================================================================
    // Comprehensive test coverage (Phase 251)
    // ================================================================

    // --- AgingPriority serde ---

    #[test]
    fn aging_priority_serde_all_variants() {
        for (variant, expected) in [
            (AgingPriority::Low, "\"low\""),
            (AgingPriority::Normal, "\"normal\""),
            (AgingPriority::High, "\"high\""),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected);
            let back: AgingPriority = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn aging_priority_serde_snake_case_values() {
        let low: AgingPriority = serde_json::from_str("\"low\"").unwrap();
        let normal: AgingPriority = serde_json::from_str("\"normal\"").unwrap();
        let high: AgingPriority = serde_json::from_str("\"high\"").unwrap();
        assert_eq!(low, AgingPriority::Low);
        assert_eq!(normal, AgingPriority::Normal);
        assert_eq!(high, AgingPriority::High);
    }

    #[test]
    fn aging_priority_serde_invalid_variant() {
        let result: Result<AgingPriority, _> = serde_json::from_str("\"critical\"");
        assert!(result.is_err());
    }

    // --- AgingPriority traits ---

    #[test]
    fn aging_priority_clone_copy_debug() {
        let p = AgingPriority::High;
        let cloned = p;
        assert_eq!(cloned, AgingPriority::High);
        let debug = format!("{p:?}");
        assert!(debug.contains("High"));
    }

    #[test]
    fn aging_priority_eq_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(AgingPriority::Low);
        set.insert(AgingPriority::Low);
        set.insert(AgingPriority::Normal);
        set.insert(AgingPriority::High);
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn aging_priority_default_is_normal() {
        assert_eq!(AgingPriority::default(), AgingPriority::Normal);
    }

    #[test]
    fn aging_priority_ord() {
        assert!(AgingPriority::Low < AgingPriority::Normal);
        assert!(AgingPriority::Normal < AgingPriority::High);
        assert!(AgingPriority::Low < AgingPriority::High);
    }

    #[test]
    fn aging_priority_partial_ord_consistency() {
        assert_eq!(
            AgingPriority::Low.partial_cmp(&AgingPriority::Normal),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            AgingPriority::High.partial_cmp(&AgingPriority::High),
            Some(std::cmp::Ordering::Equal)
        );
    }

    // --- AgingDecision serde ---

    #[test]
    fn aging_decision_serde_roundtrip() {
        let decision = AgingDecision {
            task_id: "task-123".to_string(),
            old_priority: AgingPriority::Low,
            new_priority: AgingPriority::Normal,
            wait_secs: 7200,
        };
        let json = serde_json::to_string(&decision).unwrap();
        let back: AgingDecision = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, "task-123");
        assert_eq!(back.old_priority, AgingPriority::Low);
        assert_eq!(back.new_priority, AgingPriority::Normal);
        assert_eq!(back.wait_secs, 7200);
    }

    #[test]
    fn aging_decision_serde_extra_fields_ignored() {
        let json = r#"{"task_id":"t1","old_priority":"low","new_priority":"normal","wait_secs":100,"extra":"field"}"#;
        let decision: AgingDecision = serde_json::from_str(json).unwrap();
        assert_eq!(decision.task_id, "t1");
        assert_eq!(decision.wait_secs, 100);
    }

    #[test]
    fn aging_decision_clone_debug_eq() {
        let d = AgingDecision {
            task_id: "t".to_string(),
            old_priority: AgingPriority::Normal,
            new_priority: AgingPriority::High,
            wait_secs: 500,
        };
        let cloned = d.clone();
        assert_eq!(cloned.task_id, d.task_id);
        assert_eq!(cloned, d);
        let debug = format!("{d:?}");
        assert!(debug.contains("Normal"));
        assert!(debug.contains("High"));
    }

    // --- TaskAgingData traits ---

    #[test]
    fn task_aging_data_clone_debug() {
        let now = Utc::now();
        let task = TaskAgingData {
            id: "task-1".to_string(),
            priority: AgingPriority::Low,
            queued_at: Some(now),
            state: DownloadState::Queued,
        };
        let cloned = task.clone();
        assert_eq!(cloned.id, "task-1");
        assert_eq!(cloned.priority, AgingPriority::Low);
        let debug = format!("{task:?}");
        assert!(debug.contains("task-1"));
    }

    #[test]
    fn task_aging_data_unicode_id() {
        let task = TaskAgingData {
            id: "任务-中文".to_string(),
            priority: AgingPriority::Normal,
            queued_at: None,
            state: DownloadState::Queued,
        };
        assert_eq!(task.id, "任务-中文");
    }

    // --- PriorityAgingConfig serde ---

    #[test]
    fn config_serde_roundtrip_default() {
        let config = PriorityAgingConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: PriorityAgingConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.enabled, config.enabled);
        assert_eq!(back.low_to_normal_secs, config.low_to_normal_secs);
        assert_eq!(back.normal_to_high_secs, config.normal_to_high_secs);
        assert_eq!(back.max_aged_priority, config.max_aged_priority);
        assert_eq!(back.check_interval_secs, config.check_interval_secs);
    }

    #[test]
    fn config_serde_snake_case_values() {
        let config = PriorityAgingConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"enabled\""));
        assert!(json.contains("\"low_to_normal_secs\""));
        assert!(json.contains("\"normal_to_high_secs\""));
        assert!(json.contains("\"max_aged_priority\""));
        assert!(json.contains("\"check_interval_secs\""));
    }

    #[test]
    fn config_serde_extra_fields_ignored() {
        let json = r#"{
            "enabled":true,
            "low_to_normal_secs":100,
            "normal_to_high_secs":200,
            "max_aged_priority":"high",
            "check_interval_secs":30,
            "unknown_field":"ignored"
        }"#;
        let config: PriorityAgingConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.low_to_normal_secs, 100);
    }

    #[test]
    fn config_serde_pretty() {
        let config = PriorityAgingConfig {
            enabled: true,
            ..Default::default()
        };
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let back: PriorityAgingConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.enabled, config.enabled);
    }

    #[test]
    fn config_clone_debug() {
        let config = PriorityAgingConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        let debug = format!("{config:?}");
        assert!(debug.contains("PriorityAgingConfig"));
    }

    #[test]
    fn config_custom_values() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 0,
            normal_to_high_secs: u64::MAX,
            max_aged_priority: AgingPriority::Low,
            check_interval_secs: 1,
        };
        assert_eq!(config.low_to_normal_secs, 0);
        assert_eq!(config.normal_to_high_secs, u64::MAX);
        assert_eq!(config.max_aged_priority, AgingPriority::Low);
    }

    // --- PriorityAgingError ---

    #[test]
    fn error_display_all_variants() {
        let io_err = PriorityAgingError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        assert!(io_err.to_string().contains("file missing"));

        let json_err =
            PriorityAgingError::Json(serde_json::from_str::<String>("invalid").unwrap_err());
        assert!(json_err.to_string().contains("JSON"));

        let invalid = PriorityAgingError::InvalidThreshold("bad value".to_string());
        assert!(invalid.to_string().contains("bad value"));
        assert!(invalid.to_string().contains("Invalid threshold"));
    }

    #[test]
    fn error_debug() {
        let err = PriorityAgingError::InvalidThreshold("test".to_string());
        let debug = format!("{err:?}");
        assert!(debug.contains("InvalidThreshold"));
    }

    #[test]
    fn error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: PriorityAgingError = PriorityAgingError::from(io_err);
        assert!(err.to_string().contains("denied"));
    }

    #[test]
    fn error_from_json() {
        let json_err = serde_json::from_str::<PriorityAgingConfig>("{}").unwrap_err();
        let err: PriorityAgingError = PriorityAgingError::from(json_err);
        assert!(err.to_string().contains("JSON"));
    }

    #[test]
    fn error_is_std_error() {
        let err: Box<dyn std::error::Error> =
            Box::new(PriorityAgingError::InvalidThreshold("x".to_string()));
        assert!(err.to_string().contains("x"));
    }

    // --- format_wait_duration ---

    #[test]
    fn format_wait_duration_boundaries() {
        assert_eq!(format_wait_duration(0), "0s");
        assert_eq!(format_wait_duration(1), "1s");
        assert_eq!(format_wait_duration(59), "59s");
        assert_eq!(format_wait_duration(60), "1m0s");
        assert_eq!(format_wait_duration(61), "1m1s");
        assert_eq!(format_wait_duration(3599), "59m59s");
        assert_eq!(format_wait_duration(3600), "1h0m");
        assert_eq!(format_wait_duration(3601), "1h0m");
        assert_eq!(format_wait_duration(7200), "2h0m");
        assert_eq!(format_wait_duration(86400), "24h0m");
    }

    #[test]
    fn format_wait_duration_large_value() {
        assert_eq!(
            format_wait_duration(u64::MAX),
            format!("{}h{}m", u64::MAX / 3600, (u64::MAX % 3600) / 60)
        );
    }

    // --- evaluate_task_aging edge cases ---

    #[test]
    fn evaluate_paused_task_no_aging() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Paused,
        );
        assert!(evaluate_task_aging(&task, &config, now).is_none());
    }

    #[test]
    fn evaluate_completed_task_no_aging() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Complete,
        );
        assert!(evaluate_task_aging(&task, &config, now).is_none());
    }

    #[test]
    fn evaluate_failed_task_no_aging() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Error,
        );
        assert!(evaluate_task_aging(&task, &config, now).is_none());
    }

    #[test]
    fn evaluate_downloading_task_no_aging() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Downloading,
        );
        assert!(evaluate_task_aging(&task, &config, now).is_none());
    }

    #[test]
    fn evaluate_empty_task_id() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(120)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now);
        assert!(decision.is_some());
        assert_eq!(decision.unwrap().task_id, "");
    }

    #[test]
    fn evaluate_unicode_task_id() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "任务-中文-🚀",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(120)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now).unwrap();
        assert_eq!(decision.task_id, "任务-中文-🚀");
    }

    #[test]
    fn evaluate_emoji_task_id() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "🎯🎮🎲",
            AgingPriority::Normal,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now).unwrap();
        assert_eq!(decision.task_id, "🎯🎮🎲");
    }

    #[test]
    fn evaluate_zero_wait_secs() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 0,
            ..Default::default()
        };
        let now = Utc::now();
        // queued_at == now means wait_secs = 0, threshold = 0, so 0 >= 0 is true
        let task = make_task("t1", AgingPriority::Low, Some(now), DownloadState::Queued);
        let decision = evaluate_task_aging(&task, &config, now);
        assert!(decision.is_some());
        assert_eq!(decision.unwrap().wait_secs, 0);
    }

    #[test]
    fn evaluate_normal_to_high_exact_threshold() {
        let config = PriorityAgingConfig {
            enabled: true,
            normal_to_high_secs: 7200,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Normal,
            Some(now - chrono::Duration::seconds(7200)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now);
        assert!(decision.is_some());
        assert_eq!(decision.unwrap().new_priority, AgingPriority::High);
    }

    #[test]
    fn evaluate_normal_to_high_one_second_before() {
        let config = PriorityAgingConfig {
            enabled: true,
            normal_to_high_secs: 7200,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Normal,
            Some(now - chrono::Duration::seconds(7199)),
            DownloadState::Queued,
        );
        assert!(evaluate_task_aging(&task, &config, now).is_none());
    }

    #[test]
    fn evaluate_max_aged_priority_low() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            normal_to_high_secs: 120,
            max_aged_priority: AgingPriority::Low,
            ..Default::default()
        };
        let now = Utc::now();
        // Low task can't age because max_aged_priority is Low (already at or above cap)
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Queued,
        );
        assert!(evaluate_task_aging(&task, &config, now).is_none());
    }

    #[test]
    fn evaluate_decision_wait_secs_accuracy() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let queued_at = now - chrono::Duration::seconds(120);
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(queued_at),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now).unwrap();
        // wait_secs should be approximately 120
        assert!(decision.wait_secs >= 119 && decision.wait_secs <= 121);
    }

    // --- evaluate_batch_aging edge cases ---

    #[test]
    fn batch_empty_tasks() {
        let config = PriorityAgingConfig {
            enabled: true,
            ..Default::default()
        };
        let now = Utc::now();
        let decisions = evaluate_batch_aging(&[], &config, now);
        assert!(decisions.is_empty());
    }

    #[test]
    fn batch_all_same_priority_no_aging() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        // All High priority - none should age
        let tasks: Vec<_> = (0..10)
            .map(|i| {
                make_task(
                    &format!("t{i}"),
                    AgingPriority::High,
                    Some(now - chrono::Duration::seconds(10000)),
                    DownloadState::Queued,
                )
            })
            .collect();
        let decisions = evaluate_batch_aging(&tasks, &config, now);
        assert!(decisions.is_empty());
    }

    #[test]
    fn batch_many_tasks() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let tasks: Vec<_> = (0..100)
            .map(|i| {
                make_task(
                    &format!("task-{i}"),
                    AgingPriority::Low,
                    Some(now - chrono::Duration::seconds(120)),
                    DownloadState::Queued,
                )
            })
            .collect();
        let decisions = evaluate_batch_aging(&tasks, &config, now);
        assert_eq!(decisions.len(), 100);
    }

    #[test]
    fn batch_unicode_task_ids() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let tasks = vec![
            make_task(
                "日本語タスク",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(120)),
                DownloadState::Queued,
            ),
            make_task(
                "한국어작업",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(120)),
                DownloadState::Queued,
            ),
        ];
        let decisions = evaluate_batch_aging(&tasks, &config, now);
        assert_eq!(decisions.len(), 2);
        assert_eq!(decisions[0].task_id, "日本語タスク");
        assert_eq!(decisions[1].task_id, "한국어작업");
    }

    #[test]
    fn batch_preserves_order() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let tasks = vec![
            make_task(
                "a",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(120)),
                DownloadState::Queued,
            ),
            make_task(
                "b",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(120)),
                DownloadState::Queued,
            ),
            make_task(
                "c",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(120)),
                DownloadState::Queued,
            ),
        ];
        let decisions = evaluate_batch_aging(&tasks, &config, now);
        assert_eq!(decisions[0].task_id, "a");
        assert_eq!(decisions[1].task_id, "b");
        assert_eq!(decisions[2].task_id, "c");
    }

    // --- Persistence ---

    #[test]
    fn persistence_save_creates_file() {
        let dir = tempdir().unwrap();
        let config = PriorityAgingConfig::default();
        save_priority_aging_config(&config, dir.path()).unwrap();
        assert!(dir.path().join("priority_aging_config.json").exists());
    }

    #[test]
    fn persistence_no_tmp_leftover() {
        let dir = tempdir().unwrap();
        let config = PriorityAgingConfig::default();
        save_priority_aging_config(&config, dir.path()).unwrap();
        assert!(!dir.path().join("priority_aging_config.json.tmp").exists());
    }

    #[test]
    fn persistence_corrupted_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("priority_aging_config.json");
        std::fs::write(&path, "not valid json{{{").unwrap();
        let result = load_priority_aging_config(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn persistence_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("priority_aging_config.json");
        std::fs::write(&path, "").unwrap();
        let result = load_priority_aging_config(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn persistence_pretty_roundtrip() {
        let dir = tempdir().unwrap();
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 999,
            normal_to_high_secs: 1111,
            max_aged_priority: AgingPriority::Normal,
            check_interval_secs: 15,
        };
        save_priority_aging_config(&config, dir.path()).unwrap();
        let json = std::fs::read_to_string(dir.path().join("priority_aging_config.json")).unwrap();
        // Should be pretty-printed
        assert!(json.contains('\n'));
        let loaded: PriorityAgingConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.low_to_normal_secs, 999);
        assert_eq!(loaded.normal_to_high_secs, 1111);
    }

    #[test]
    fn persistence_unicode_config() {
        let dir = tempdir().unwrap();
        let config = PriorityAgingConfig {
            enabled: true,
            ..Default::default()
        };
        save_priority_aging_config(&config, dir.path()).unwrap();
        let loaded = load_priority_aging_config(dir.path()).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
    }

    #[test]
    fn persistence_save_to_nested_dir() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("sub").join("dir");
        std::fs::create_dir_all(&nested).unwrap();
        let config = PriorityAgingConfig::default();
        save_priority_aging_config(&config, &nested).unwrap();
        assert!(nested.join("priority_aging_config.json").exists());
    }

    #[test]
    fn persistence_atomic_write() {
        let dir = tempdir().unwrap();
        // Save multiple times to verify atomic overwrite
        for i in 0..5 {
            let config = PriorityAgingConfig {
                enabled: i % 2 == 0,
                low_to_normal_secs: i * 100,
                ..Default::default()
            };
            save_priority_aging_config(&config, dir.path()).unwrap();
        }
        let loaded = load_priority_aging_config(dir.path()).unwrap();
        // Last iteration: i=4, 4%2==0 -> enabled=true
        assert!(loaded.enabled);
        assert_eq!(loaded.low_to_normal_secs, 400);
    }

    // --- Complex workflows ---

    #[test]
    fn complex_lifecycle() {
        let dir = tempdir().unwrap();
        // 1. Load default config
        let config = load_priority_aging_config(dir.path()).unwrap();
        assert!(!config.enabled);

        // 2. Create custom config and save
        let custom = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            normal_to_high_secs: 120,
            max_aged_priority: AgingPriority::High,
            check_interval_secs: 30,
        };
        save_priority_aging_config(&custom, dir.path()).unwrap();

        // 3. Load and verify
        let loaded = load_priority_aging_config(dir.path()).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.low_to_normal_secs, 60);

        // 4. Evaluate aging
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(120)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &loaded, now);
        assert!(decision.is_some());
        assert_eq!(decision.unwrap().new_priority, AgingPriority::Normal);
    }

    #[test]
    fn complex_multi_task_workflow() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 3600,
            normal_to_high_secs: 7200,
            max_aged_priority: AgingPriority::High,
            ..Default::default()
        };
        let now = Utc::now();
        let tasks = vec![
            // Low, 0s wait -> no aging
            make_task(
                "fresh",
                AgingPriority::Low,
                Some(now),
                DownloadState::Queued,
            ),
            // Low, 1h wait -> age to Normal
            make_task(
                "aged-low",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(3600)),
                DownloadState::Queued,
            ),
            // Normal, 2h wait -> age to High
            make_task(
                "aged-normal",
                AgingPriority::Normal,
                Some(now - chrono::Duration::seconds(7200)),
                DownloadState::Queued,
            ),
            // High, any wait -> no aging
            make_task(
                "high",
                AgingPriority::High,
                Some(now - chrono::Duration::seconds(100000)),
                DownloadState::Queued,
            ),
            // Low, downloading -> no aging
            make_task(
                "downloading",
                AgingPriority::Low,
                Some(now - chrono::Duration::seconds(100000)),
                DownloadState::Downloading,
            ),
            // Low, no queued_at -> no aging
            make_task(
                "no-queue-time",
                AgingPriority::Low,
                None,
                DownloadState::Queued,
            ),
        ];
        let decisions = evaluate_batch_aging(&tasks, &config, now);
        assert_eq!(decisions.len(), 2);
        let ids: Vec<&str> = decisions.iter().map(|d| d.task_id.as_str()).collect();
        assert!(ids.contains(&"aged-low"));
        assert!(ids.contains(&"aged-normal"));
    }

    #[test]
    fn independent_configs() {
        // evaluate_task_aging doesn't check config.enabled (only batch does)
        // Test with different thresholds instead
        let config1 = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let config2 = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 10000,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(120)),
            DownloadState::Queued,
        );
        // config1 should age (120 >= 60), config2 should not (120 < 10000)
        assert!(evaluate_task_aging(&task, &config1, now).is_some());
        assert!(evaluate_task_aging(&task, &config2, now).is_none());
    }

    #[test]
    fn batch_respects_enabled_flag() {
        let config_enabled = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            ..Default::default()
        };
        let config_disabled = PriorityAgingConfig {
            enabled: false,
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
        let decisions_enabled = evaluate_batch_aging(&[task.clone()], &config_enabled, now);
        assert_eq!(decisions_enabled.len(), 1);
        let decisions_disabled = evaluate_batch_aging(&[task], &config_disabled, now);
        assert!(decisions_disabled.is_empty());
    }

    #[test]
    fn config_with_zero_thresholds() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 0,
            normal_to_high_secs: 0,
            ..Default::default()
        };
        let now = Utc::now();
        // Low task with 0 wait -> should age (0 >= 0)
        let low_task = make_task("t1", AgingPriority::Low, Some(now), DownloadState::Queued);
        let decision = evaluate_task_aging(&low_task, &config, now);
        assert!(decision.is_some());

        // Normal task with 0 wait -> should age (0 >= 0)
        let normal_task = make_task(
            "t2",
            AgingPriority::Normal,
            Some(now),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&normal_task, &config, now);
        assert!(decision.is_some());
    }

    #[test]
    fn aging_decision_fields_correctness() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 100,
            ..Default::default()
        };
        let now = Utc::now();
        let queued_at = now - chrono::Duration::seconds(200);
        let task = make_task(
            "my-task",
            AgingPriority::Low,
            Some(queued_at),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now).unwrap();
        assert_eq!(decision.task_id, "my-task");
        assert_eq!(decision.old_priority, AgingPriority::Low);
        assert_eq!(decision.new_priority, AgingPriority::Normal);
        assert!(decision.wait_secs >= 199 && decision.wait_secs <= 201);
    }

    #[test]
    fn low_ages_to_normal_not_high() {
        // Low should always go to Normal, never skip to High
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 60,
            normal_to_high_secs: 120,
            max_aged_priority: AgingPriority::High,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Low,
            Some(now - chrono::Duration::seconds(10000)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now).unwrap();
        assert_eq!(decision.new_priority, AgingPriority::Normal);
    }

    #[test]
    fn normal_ages_to_high() {
        let config = PriorityAgingConfig {
            enabled: true,
            normal_to_high_secs: 60,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::Normal,
            Some(now - chrono::Duration::seconds(120)),
            DownloadState::Queued,
        );
        let decision = evaluate_task_aging(&task, &config, now).unwrap();
        assert_eq!(decision.new_priority, AgingPriority::High);
    }

    #[test]
    fn high_never_ages() {
        let config = PriorityAgingConfig {
            enabled: true,
            low_to_normal_secs: 0,
            normal_to_high_secs: 0,
            ..Default::default()
        };
        let now = Utc::now();
        let task = make_task(
            "t1",
            AgingPriority::High,
            Some(now - chrono::Duration::seconds(u64::MAX as i64 % 100000)),
            DownloadState::Queued,
        );
        assert!(evaluate_task_aging(&task, &config, now).is_none());
    }
}
