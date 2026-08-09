//! Queue Staleness Detection & Auto-Promotion
//!
//! Detects download tasks that have been waiting in the queue (Queued state) for too long
//! and optionally auto-promotes their priority to prevent indefinite starvation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

/// Configuration for queue staleness detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StalenessConfig {
    /// Enable staleness detection (default: false).
    pub enabled: bool,
    /// Time in seconds a task must be in Queued state before considered stale (default: 3600 = 1h).
    pub stale_threshold_secs: u64,
    /// Auto-promote stale tasks to higher priority (default: false).
    pub auto_promote: bool,
    /// Maximum priority level to promote to (default: High).
    /// Tasks already at this level or above are not promoted.
    pub max_promote_priority: StalePriority,
    /// How many priority levels to bump (default: 1).
    pub promote_levels: u8,
    /// Maximum number of times a task can be auto-promoted (0 = unlimited, default: 3).
    pub max_promotions: u32,
    /// Check interval in seconds for the background scanner (default: 300 = 5min).
    pub check_interval_secs: u64,
}

impl Default for StalenessConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            stale_threshold_secs: 3600,
            auto_promote: false,
            max_promote_priority: StalePriority::High,
            promote_levels: 1,
            max_promotions: 3,
            check_interval_secs: 300,
        }
    }
}

/// Simplified priority levels for staleness promotion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StalePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Urgent = 3,
}

impl std::fmt::Display for StalePriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StalePriority::Low => write!(f, "low"),
            StalePriority::Normal => write!(f, "normal"),
            StalePriority::High => write!(f, "high"),
            StalePriority::Urgent => write!(f, "urgent"),
        }
    }
}

impl std::str::FromStr for StalePriority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "low" => Ok(StalePriority::Low),
            "normal" => Ok(StalePriority::Normal),
            "high" => Ok(StalePriority::High),
            "urgent" => Ok(StalePriority::Urgent),
            _ => Err(format!(
                "invalid priority: {s} (valid: low, normal, high, urgent)"
            )),
        }
    }
}

/// Input data for a single task to evaluate for staleness.
#[derive(Debug, Clone)]
pub struct TaskStalenessData {
    pub id: String,
    pub name: String,
    pub is_queued: bool,
    pub created_at: DateTime<Utc>,
    pub priority: StalePriority,
    pub promotion_count: u32,
}

/// Result of staleness analysis for a single task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleTaskInfo {
    pub id: String,
    pub name: String,
    pub queued_duration_secs: u64,
    pub current_priority: String,
    pub promoted: bool,
    pub new_priority: Option<String>,
    pub promotion_count: u32,
}

/// Summary of the queue staleness analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StalenessSummary {
    pub total_queued: usize,
    pub stale_count: usize,
    pub promoted_count: u32,
    pub skipped_count: u32,
    pub tasks: Vec<StaleTaskInfo>,
    pub config: StalenessConfig,
}

impl StalenessSummary {
    /// Format the summary for display.
    pub fn format_report(&self) -> String {
        let mut out = String::new();
        out.push_str("=== Queue Staleness Report ===\n");
        out.push_str(&format!(
            "Total queued: {} | Stale: {} | Promoted: {} | Skipped: {}\n",
            self.total_queued, self.stale_count, self.promoted_count, self.skipped_count
        ));
        out.push_str(&format!(
            "Config: threshold={}s, auto_promote={}, max_promotions={}\n\n",
            self.config.stale_threshold_secs, self.config.auto_promote, self.config.max_promotions
        ));

        if self.tasks.is_empty() {
            out.push_str("No stale tasks detected.\n");
            return out;
        }

        for task in &self.tasks {
            let status = if task.promoted {
                format!(
                    "PROMOTED: {} -> {}",
                    task.current_priority,
                    task.new_priority.as_deref().unwrap_or("?")
                )
            } else {
                "SKIPPED".to_string()
            };
            out.push_str(&format!(
                "  [{}] {} ({}) - queued for {}s (promotions: {})\n",
                status, task.name, task.id, task.queued_duration_secs, task.promotion_count
            ));
        }

        out
    }
}

/// Evaluate a single task for staleness.
/// Returns `Some(StaleTaskInfo)` if the task is stale, `None` otherwise.
pub fn evaluate_task(
    task: &TaskStalenessData,
    now: DateTime<Utc>,
    config: &StalenessConfig,
) -> Option<StaleTaskInfo> {
    if !task.is_queued {
        return None;
    }

    let queued_duration = now.signed_duration_since(task.created_at);
    let queued_secs = queued_duration.num_seconds() as u64;

    if queued_secs < config.stale_threshold_secs {
        return None;
    }

    let current_priority_str = task.priority.to_string();

    // Check if promotion should happen
    if !config.auto_promote {
        return Some(StaleTaskInfo {
            id: task.id.clone(),
            name: task.name.clone(),
            queued_duration_secs: queued_secs,
            current_priority: current_priority_str,
            promoted: false,
            new_priority: None,
            promotion_count: task.promotion_count,
        });
    }

    // Check if already at max promote priority
    if task.priority >= config.max_promote_priority {
        return Some(StaleTaskInfo {
            id: task.id.clone(),
            name: task.name.clone(),
            queued_duration_secs: queued_secs,
            current_priority: current_priority_str,
            promoted: false,
            new_priority: None,
            promotion_count: task.promotion_count,
        });
    }

    // Check max promotions limit
    if config.max_promotions > 0 && task.promotion_count >= config.max_promotions {
        return Some(StaleTaskInfo {
            id: task.id.clone(),
            name: task.name.clone(),
            queued_duration_secs: queued_secs,
            current_priority: current_priority_str,
            promoted: false,
            new_priority: None,
            promotion_count: task.promotion_count,
        });
    }

    // Calculate new priority
    let new_priority_level = promote_priority(
        task.priority,
        config.promote_levels,
        config.max_promote_priority,
    );
    let new_priority_str = new_priority_level.to_string();

    Some(StaleTaskInfo {
        id: task.id.clone(),
        name: task.name.clone(),
        queued_duration_secs: queued_secs,
        current_priority: current_priority_str,
        promoted: true,
        new_priority: Some(new_priority_str),
        promotion_count: task.promotion_count + 1,
    })
}

/// Promote a priority by `levels` steps, capped at `max_priority`.
pub fn promote_priority(
    current: StalePriority,
    levels: u8,
    max_priority: StalePriority,
) -> StalePriority {
    let current_val = current as u8;
    let max_val = max_priority as u8;
    let promoted = current_val.saturating_add(levels);
    match promoted.min(max_val) {
        0 => StalePriority::Low,
        1 => StalePriority::Normal,
        2 => StalePriority::High,
        _ => StalePriority::Urgent,
    }
}

/// Analyze all tasks for staleness and return a summary.
pub fn analyze_staleness(
    tasks: &[TaskStalenessData],
    now: DateTime<Utc>,
    config: &StalenessConfig,
) -> StalenessSummary {
    let total_queued = tasks.iter().filter(|t| t.is_queued).count();
    let mut promoted_count = 0u32;
    let mut skipped_count = 0u32;
    let mut stale_tasks = Vec::new();

    for task in tasks {
        if let Some(info) = evaluate_task(task, now, config) {
            if info.promoted {
                promoted_count += 1;
            } else {
                skipped_count += 1;
            }
            stale_tasks.push(info);
        }
    }

    // Sort by queued duration descending
    stale_tasks.sort_by(|a, b| b.queued_duration_secs.cmp(&a.queued_duration_secs));

    StalenessSummary {
        total_queued,
        stale_count: stale_tasks.len(),
        promoted_count,
        skipped_count,
        tasks: stale_tasks,
        config: config.clone(),
    }
}

/// Persist staleness config to disk (atomic write).
pub async fn save_staleness_config(
    config: &StalenessConfig,
    data_dir: &Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("staleness_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json.as_bytes()).await?;
    fs::rename(&tmp, &path).await?;
    Ok(())
}

/// Load staleness config from disk.
pub async fn load_staleness_config(data_dir: &Path) -> Option<StalenessConfig> {
    let path = data_dir.join("staleness_config.json");
    let content = fs::read_to_string(&path).await.ok()?;
    serde_json::from_str(&content).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_task(
        id: &str,
        queued_secs: u64,
        priority: StalePriority,
        promo_count: u32,
    ) -> TaskStalenessData {
        TaskStalenessData {
            id: id.to_string(),
            name: format!("task-{id}"),
            is_queued: true,
            created_at: Utc::now() - Duration::seconds(queued_secs as i64),
            priority,
            promotion_count: promo_count,
        }
    }

    fn make_task_non_queued(id: &str) -> TaskStalenessData {
        TaskStalenessData {
            id: id.to_string(),
            name: format!("task-{id}"),
            is_queued: false,
            created_at: Utc::now() - Duration::hours(2),
            priority: StalePriority::Low,
            promotion_count: 0,
        }
    }

    #[test]
    fn test_default_config() {
        let config = StalenessConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.stale_threshold_secs, 3600);
        assert!(!config.auto_promote);
        assert_eq!(config.max_promote_priority, StalePriority::High);
        assert_eq!(config.promote_levels, 1);
        assert_eq!(config.max_promotions, 3);
        assert_eq!(config.check_interval_secs, 300);
    }

    #[test]
    fn test_promote_priority_single_level() {
        assert_eq!(
            promote_priority(StalePriority::Low, 1, StalePriority::Urgent),
            StalePriority::Normal
        );
        assert_eq!(
            promote_priority(StalePriority::Normal, 1, StalePriority::Urgent),
            StalePriority::High
        );
        assert_eq!(
            promote_priority(StalePriority::High, 1, StalePriority::Urgent),
            StalePriority::Urgent
        );
    }

    #[test]
    fn test_promote_priority_capped_at_max() {
        assert_eq!(
            promote_priority(StalePriority::Low, 1, StalePriority::High),
            StalePriority::Normal
        );
        assert_eq!(
            promote_priority(StalePriority::Normal, 5, StalePriority::High),
            StalePriority::High
        );
        assert_eq!(
            promote_priority(StalePriority::High, 10, StalePriority::High),
            StalePriority::High
        );
    }

    #[test]
    fn test_promote_priority_multiple_levels() {
        assert_eq!(
            promote_priority(StalePriority::Low, 2, StalePriority::Urgent),
            StalePriority::High
        );
        assert_eq!(
            promote_priority(StalePriority::Low, 3, StalePriority::Urgent),
            StalePriority::Urgent
        );
    }

    #[test]
    fn test_evaluate_not_queued() {
        let task = make_task_non_queued("1");
        let config = StalenessConfig::default();
        let result = evaluate_task(&task, Utc::now(), &config);
        assert!(result.is_none());
    }

    #[test]
    fn test_evaluate_queued_not_stale() {
        let task = make_task("1", 1800, StalePriority::Low, 0); // 30 min, threshold 1h
        let config = StalenessConfig::default();
        let result = evaluate_task(&task, Utc::now(), &config);
        assert!(result.is_none());
    }

    #[test]
    fn test_evaluate_stale_no_auto_promote() {
        let task = make_task("1", 7200, StalePriority::Low, 0); // 2h stale
        let mut config = StalenessConfig::default();
        config.auto_promote = false;
        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(!result.promoted);
        assert_eq!(result.queued_duration_secs, 7200);
        assert!(result.new_priority.is_none());
    }

    #[test]
    fn test_evaluate_stale_with_promotion() {
        let task = make_task("1", 7200, StalePriority::Low, 0);
        let mut config = StalenessConfig::default();
        config.auto_promote = true;
        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(result.promoted);
        assert_eq!(result.new_priority.as_deref(), Some("normal"));
        assert_eq!(result.promotion_count, 1);
    }

    #[test]
    fn test_evaluate_already_at_max_priority() {
        let task = make_task("1", 7200, StalePriority::High, 0);
        let mut config = StalenessConfig::default();
        config.auto_promote = true;
        config.max_promote_priority = StalePriority::High;
        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(!result.promoted);
    }

    #[test]
    fn test_evaluate_max_promotions_reached() {
        let task = make_task("1", 7200, StalePriority::Low, 3);
        let mut config = StalenessConfig::default();
        config.auto_promote = true;
        config.max_promotions = 3;
        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(!result.promoted);
    }

    #[test]
    fn test_evaluate_unlimited_promotions() {
        let task = make_task("1", 7200, StalePriority::Low, 100);
        let mut config = StalenessConfig::default();
        config.auto_promote = true;
        config.max_promotions = 0; // unlimited
        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(result.promoted);
    }

    #[test]
    fn test_analyze_staleness_mixed() {
        let tasks = vec![
            make_task("1", 7200, StalePriority::Low, 0), // stale, will promote
            make_task("2", 1800, StalePriority::Low, 0), // not stale (30min)
            make_task("3", 10800, StalePriority::Normal, 0), // stale, will promote
            make_task_non_queued("4"),                   // not queued
        ];
        let mut config = StalenessConfig::default();
        config.auto_promote = true;
        let summary = analyze_staleness(&tasks, Utc::now(), &config);
        assert_eq!(summary.total_queued, 3);
        assert_eq!(summary.stale_count, 2);
        assert_eq!(summary.promoted_count, 2);
        assert_eq!(summary.skipped_count, 0);
        // Sorted by duration descending
        assert_eq!(summary.tasks[0].id, "3");
        assert_eq!(summary.tasks[1].id, "1");
    }

    #[test]
    fn test_analyze_staleness_no_stale() {
        let tasks = vec![
            make_task("1", 600, StalePriority::Low, 0),
            make_task("2", 300, StalePriority::Normal, 0),
        ];
        let config = StalenessConfig::default();
        let summary = analyze_staleness(&tasks, Utc::now(), &config);
        assert_eq!(summary.stale_count, 0);
        assert!(summary.tasks.is_empty());
    }

    #[test]
    fn test_format_report_empty() {
        let summary = StalenessSummary {
            total_queued: 0,
            stale_count: 0,
            promoted_count: 0,
            skipped_count: 0,
            tasks: vec![],
            config: StalenessConfig::default(),
        };
        let report = summary.format_report();
        assert!(report.contains("No stale tasks detected"));
    }

    #[test]
    fn test_format_report_with_tasks() {
        let summary = StalenessSummary {
            total_queued: 3,
            stale_count: 2,
            promoted_count: 1,
            skipped_count: 1,
            tasks: vec![
                StaleTaskInfo {
                    id: "t1".to_string(),
                    name: "big-file".to_string(),
                    queued_duration_secs: 7200,
                    current_priority: "low".to_string(),
                    promoted: true,
                    new_priority: Some("normal".to_string()),
                    promotion_count: 1,
                },
                StaleTaskInfo {
                    id: "t2".to_string(),
                    name: "at-max".to_string(),
                    queued_duration_secs: 10800,
                    current_priority: "high".to_string(),
                    promoted: false,
                    new_priority: None,
                    promotion_count: 3,
                },
            ],
            config: StalenessConfig::default(),
        };
        let report = summary.format_report();
        assert!(report.contains("PROMOTED"));
        assert!(report.contains("SKIPPED"));
        assert!(report.contains("big-file"));
        assert!(report.contains("at-max"));
    }

    #[test]
    fn test_stale_priority_ordering() {
        assert!(StalePriority::Low < StalePriority::Normal);
        assert!(StalePriority::Normal < StalePriority::High);
        assert!(StalePriority::High < StalePriority::Urgent);
    }

    #[test]
    fn test_stale_priority_from_str() {
        assert_eq!("low".parse::<StalePriority>().unwrap(), StalePriority::Low);
        assert_eq!(
            "Normal".parse::<StalePriority>().unwrap(),
            StalePriority::Normal
        );
        assert_eq!(
            "HIGH".parse::<StalePriority>().unwrap(),
            StalePriority::High
        );
        assert_eq!(
            "urgent".parse::<StalePriority>().unwrap(),
            StalePriority::Urgent
        );
        assert!("invalid".parse::<StalePriority>().is_err());
    }

    #[test]
    fn test_config_serialization() {
        let config = StalenessConfig {
            enabled: true,
            stale_threshold_secs: 1800,
            auto_promote: true,
            max_promote_priority: StalePriority::Urgent,
            promote_levels: 2,
            max_promotions: 5,
            check_interval_secs: 600,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StalenessConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, true);
        assert_eq!(deserialized.stale_threshold_secs, 1800);
        assert_eq!(deserialized.max_promote_priority, StalePriority::Urgent);
    }

    #[tokio::test]
    async fn test_save_and_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = StalenessConfig {
            enabled: true,
            stale_threshold_secs: 900,
            ..Default::default()
        };
        save_staleness_config(&config, dir.path()).await.unwrap();
        let loaded = load_staleness_config(dir.path()).await.unwrap();
        assert_eq!(loaded.enabled, true);
        assert_eq!(loaded.stale_threshold_secs, 900);
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_staleness_config(dir.path()).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_promotion_count_tracking() {
        // Test that promotion_count is properly incremented
        let mut task = make_task("1", 7200, StalePriority::Low, 0);
        let mut config = StalenessConfig::default();
        config.auto_promote = true;

        // First promotion
        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(result.promoted);
        assert_eq!(result.promotion_count, 1);

        // Update task with new promotion count
        task.promotion_count = result.promotion_count;
        task.created_at = Utc::now() - Duration::seconds(10800); // 3 hours ago

        // Second promotion
        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(result.promoted);
        assert_eq!(result.promotion_count, 2);

        // Update task again
        task.promotion_count = result.promotion_count;
        task.created_at = Utc::now() - Duration::seconds(14400); // 4 hours ago

        // Third promotion
        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(result.promoted);
        assert_eq!(result.promotion_count, 3);
    }

    #[test]
    fn test_promotion_count_respects_max_promotions() {
        let task = make_task("1", 7200, StalePriority::Low, 3);
        let mut config = StalenessConfig::default();
        config.auto_promote = true;
        config.max_promotions = 3;

        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(!result.promoted); // Should not promote because max reached
        assert_eq!(result.promotion_count, 3);
    }

    #[test]
    fn test_promotion_count_unlimited() {
        let task = make_task("1", 7200, StalePriority::Low, 10);
        let mut config = StalenessConfig::default();
        config.auto_promote = true;
        config.max_promotions = 0; // Unlimited

        let result = evaluate_task(&task, Utc::now(), &config).unwrap();
        assert!(result.promoted);
        assert_eq!(result.promotion_count, 11); // Should increment to 11
    }
}
