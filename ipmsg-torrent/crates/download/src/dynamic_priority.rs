//! Dynamic Priority Adjustment (Phase 132)
//!
//! Automatically adjusts task priorities based on multiple factors:
//! - Speed performance (slow tasks get lower priority)
//! - Wait time (long-waiting tasks get boosted)
//! - Progress percentage (near-complete tasks get boosted)
//! - Retry count (frequently failing tasks get lowered)
//! - File size (small files get boosted to clear queue faster)
//!
//! Unlike priority_aging (which only considers wait time), this module
//! combines multiple signals into a composite score for smarter scheduling.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

/// Errors from dynamic priority operations.
#[derive(Error, Debug)]
pub enum DynamicPriorityError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Priority levels for dynamic adjustment.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum DynamicPriority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
}

impl DynamicPriority {
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

/// Weight configuration for each factor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorWeights {
    /// Weight for speed performance factor (0.0 - 5.0)
    #[serde(default = "default_speed_weight")]
    pub speed_weight: f64,
    /// Weight for wait time factor (0.0 - 5.0)
    #[serde(default = "default_wait_time_weight")]
    pub wait_time_weight: f64,
    /// Weight for progress factor (0.0 - 5.0)
    #[serde(default = "default_progress_weight")]
    pub progress_weight: f64,
    /// Weight for retry count factor (0.0 - 5.0)
    #[serde(default = "default_retry_weight")]
    pub retry_weight: f64,
    /// Weight for file size factor (0.0 - 5.0)
    #[serde(default = "default_size_weight")]
    pub size_weight: f64,
}

fn default_speed_weight() -> f64 {
    1.0
}
fn default_wait_time_weight() -> f64 {
    1.5
}
fn default_progress_weight() -> f64 {
    1.0
}
fn default_retry_weight() -> f64 {
    0.8
}
fn default_size_weight() -> f64 {
    0.5
}

impl Default for FactorWeights {
    fn default() -> Self {
        Self {
            speed_weight: default_speed_weight(),
            wait_time_weight: default_wait_time_weight(),
            progress_weight: default_progress_weight(),
            retry_weight: default_retry_weight(),
            size_weight: default_size_weight(),
        }
    }
}

/// Configuration for dynamic priority adjustment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicPriorityConfig {
    /// Whether dynamic priority adjustment is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Factor weights for score calculation.
    #[serde(default)]
    pub weights: FactorWeights,
    /// Minimum seconds a task must wait before being eligible for boost.
    #[serde(default = "default_min_wait_secs")]
    pub min_wait_secs: u64,
    /// Speed threshold in bytes/sec below which a task is considered slow.
    #[serde(default = "default_slow_speed_bps")]
    pub slow_speed_bps: u64,
    /// Progress percentage above which a task gets a boost (0-100).
    #[serde(default = "default_near_complete_pct")]
    pub near_complete_pct: u32,
    /// Max retry count before penalty kicks in.
    #[serde(default = "default_max_retry_threshold")]
    pub max_retry_threshold: u32,
    /// File size threshold in bytes below which tasks get a boost.
    #[serde(default = "default_small_file_bytes")]
    pub small_file_bytes: u64,
    /// How often to run dynamic priority adjustment (seconds).
    #[serde(default = "default_check_interval_secs")]
    pub check_interval_secs: u64,
    /// Maximum priority changes per adjustment cycle.
    #[serde(default = "default_max_changes_per_cycle")]
    pub max_changes_per_cycle: usize,
}

fn default_min_wait_secs() -> u64 {
    300 // 5 minutes
}
fn default_slow_speed_bps() -> u64 {
    50_000 // 50 KB/s
}
fn default_near_complete_pct() -> u32 {
    85
}
fn default_max_retry_threshold() -> u32 {
    3
}
fn default_small_file_bytes() -> u64 {
    10 * 1024 * 1024 // 10 MB
}
fn default_check_interval_secs() -> u64 {
    300 // 5 minutes
}
fn default_max_changes_per_cycle() -> usize {
    5
}

impl Default for DynamicPriorityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            weights: FactorWeights::default(),
            min_wait_secs: default_min_wait_secs(),
            slow_speed_bps: default_slow_speed_bps(),
            near_complete_pct: default_near_complete_pct(),
            max_retry_threshold: default_max_retry_threshold(),
            small_file_bytes: default_small_file_bytes(),
            check_interval_secs: default_check_interval_secs(),
            max_changes_per_cycle: default_max_changes_per_cycle(),
        }
    }
}

/// Input data for a single task used in priority scoring.
#[derive(Debug, Clone)]
pub struct TaskPriorityInput {
    /// Task ID.
    pub task_id: String,
    /// Current priority.
    pub current_priority: DynamicPriority,
    /// Current download speed in bytes/sec (0 if not active).
    pub current_speed_bps: u64,
    /// Progress percentage (0-100).
    pub progress_pct: u32,
    /// Number of retries so far.
    pub retry_count: u32,
    /// File size in bytes (0 if unknown).
    pub file_size_bytes: u64,
    /// When the task was created.
    pub created_at: DateTime<Utc>,
    /// Whether the task is currently queued (not actively downloading).
    pub is_queued: bool,
}

/// Result of a priority adjustment decision for a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityAdjustment {
    /// Task ID.
    pub task_id: String,
    /// Previous priority.
    pub old_priority: DynamicPriority,
    /// New recommended priority.
    pub new_priority: DynamicPriority,
    /// Composite score that determined the adjustment.
    pub score: f64,
    /// Human-readable reason for the change.
    pub reason: String,
}

/// Summary of a dynamic priority adjustment cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicPrioritySummary {
    /// Whether the system is enabled.
    pub enabled: bool,
    /// Number of tasks evaluated.
    pub tasks_evaluated: usize,
    /// Number of tasks whose priority was changed.
    pub tasks_adjusted: usize,
    /// Timestamp of last adjustment.
    pub last_adjustment_at: Option<DateTime<Utc>>,
    /// Factor weights in use.
    pub weights: FactorWeights,
    /// Recent adjustment history.
    pub recent_adjustments: Vec<AdjustmentRecord>,
}

/// Record of a single priority adjustment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdjustmentRecord {
    /// When the adjustment happened.
    pub timestamp: DateTime<Utc>,
    /// Task ID.
    pub task_id: String,
    /// Previous priority.
    pub old_priority: DynamicPriority,
    /// New priority.
    pub new_priority: DynamicPriority,
    /// Composite score.
    pub score: f64,
    /// Reason for the change.
    pub reason: String,
}

/// Maximum number of adjustment records to keep.
const MAX_ADJUSTMENT_RECORDS: usize = 100;

/// Manager for dynamic priority adjustment.
pub struct DynamicPriorityManager {
    config: DynamicPriorityConfig,
    records: Vec<AdjustmentRecord>,
    last_adjustment_at: Option<DateTime<Utc>>,
}

impl DynamicPriorityManager {
    /// Create a new manager with default config.
    pub fn new() -> Self {
        Self {
            config: DynamicPriorityConfig::default(),
            records: Vec::new(),
            last_adjustment_at: None,
        }
    }

    /// Get the current configuration.
    pub fn get_config(&self) -> &DynamicPriorityConfig {
        &self.config
    }

    /// Set the configuration.
    pub fn set_config(&mut self, config: DynamicPriorityConfig) {
        self.config = config;
    }

    /// Check if the manager is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Enable or disable dynamic priority adjustment.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    /// Calculate the composite priority score for a task.
    ///
    /// Score ranges from -10.0 (should be lowered) to +10.0 (should be boosted).
    /// Positive scores suggest boosting, negative scores suggest lowering.
    pub fn calculate_score(&self, input: &TaskPriorityInput) -> f64 {
        let w = &self.config.weights;
        let mut score = 0.0;

        // Factor 1: Speed performance
        // If task is active and slow, penalize. If fast, reward.
        if input.current_speed_bps > 0 {
            let speed_ratio = input.current_speed_bps as f64 / self.config.slow_speed_bps as f64;
            if speed_ratio < 0.5 {
                // Very slow
                score -= w.speed_weight * 2.0;
            } else if speed_ratio < 1.0 {
                // Below threshold
                score -= w.speed_weight;
            } else if speed_ratio > 2.0 {
                // Very fast
                score += w.speed_weight;
            }
        }

        // Factor 2: Wait time (only for queued tasks)
        if input.is_queued {
            let wait_secs = (Utc::now() - input.created_at).num_seconds().max(0) as u64;
            if wait_secs >= self.config.min_wait_secs {
                let wait_ratio = wait_secs as f64 / (self.config.min_wait_secs * 4) as f64;
                let wait_score = wait_ratio.min(1.0) * 2.0;
                score += w.wait_time_weight * wait_score;
            }
        }

        // Factor 3: Progress (near-complete tasks get boosted)
        if input.progress_pct >= self.config.near_complete_pct {
            let progress_bonus = (input.progress_pct - self.config.near_complete_pct) as f64 / 15.0;
            score += w.progress_weight * progress_bonus.min(1.0) * 2.0;
        }

        // Factor 4: Retry count (frequent failures get penalized)
        if input.retry_count >= self.config.max_retry_threshold {
            let retry_penalty = (input.retry_count - self.config.max_retry_threshold) as f64 / 3.0;
            score -= w.retry_weight * retry_penalty.min(2.0);
        }

        // Factor 5: File size (small files get boosted to clear queue)
        if input.file_size_bytes > 0 && input.file_size_bytes < self.config.small_file_bytes {
            let size_ratio =
                1.0 - (input.file_size_bytes as f64 / self.config.small_file_bytes as f64);
            score += w.size_weight * size_ratio;
        }

        // Clamp score to [-10, 10]
        score.clamp(-10.0, 10.0)
    }

    /// Determine the recommended priority based on score.
    pub fn score_to_priority(&self, score: f64, current: DynamicPriority) -> DynamicPriority {
        if score >= 3.0 && current != DynamicPriority::High {
            DynamicPriority::High
        } else if score <= -3.0 && current != DynamicPriority::Low {
            DynamicPriority::Low
        } else {
            current
        }
    }

    /// Generate a human-readable reason for a priority change.
    fn generate_reason(score: f64, input: &TaskPriorityInput) -> String {
        let mut factors = Vec::new();

        if input.current_speed_bps > 0 && input.current_speed_bps < 50_000 {
            factors.push("slow speed");
        }
        if input.is_queued {
            let wait_secs = (Utc::now() - input.created_at).num_seconds().max(0) as u64;
            if wait_secs >= 300 {
                factors.push("long wait time");
            }
        }
        if input.progress_pct >= 85 {
            factors.push("near completion");
        }
        if input.retry_count >= 3 {
            factors.push("frequent retries");
        }
        if input.file_size_bytes > 0 && input.file_size_bytes < 10 * 1024 * 1024 {
            factors.push("small file");
        }

        if factors.is_empty() {
            return format!("score={:.1}", score);
        }
        format!("score={:.1} ({})", score, factors.join(", "))
    }

    /// Evaluate all tasks and return recommended adjustments.
    ///
    /// Returns a list of tasks whose priority should change, limited by
    /// `max_changes_per_cycle`.
    pub fn evaluate(&self, tasks: &[TaskPriorityInput]) -> Vec<PriorityAdjustment> {
        if !self.config.enabled {
            return Vec::new();
        }

        let mut adjustments: Vec<PriorityAdjustment> = Vec::new();

        for task in tasks {
            let score = self.calculate_score(task);
            let new_priority = self.score_to_priority(score, task.current_priority);

            if new_priority != task.current_priority {
                adjustments.push(PriorityAdjustment {
                    task_id: task.task_id.clone(),
                    old_priority: task.current_priority,
                    new_priority,
                    score,
                    reason: Self::generate_reason(score, task),
                });
            }
        }

        // Sort by absolute score descending (most impactful changes first)
        adjustments.sort_by(|a, b| b.score.abs().partial_cmp(&a.score.abs()).unwrap());

        // Limit to max changes per cycle
        adjustments.truncate(self.config.max_changes_per_cycle);

        adjustments
    }

    /// Apply adjustments and record them.
    pub fn record_adjustments(&mut self, adjustments: Vec<PriorityAdjustment>) {
        let now = Utc::now();
        self.last_adjustment_at = Some(now);

        for adj in &adjustments {
            self.records.push(AdjustmentRecord {
                timestamp: now,
                task_id: adj.task_id.clone(),
                old_priority: adj.old_priority,
                new_priority: adj.new_priority,
                score: adj.score,
                reason: adj.reason.clone(),
            });
        }

        // Trim old records
        if self.records.len() > MAX_ADJUSTMENT_RECORDS {
            let drain_count = self.records.len() - MAX_ADJUSTMENT_RECORDS;
            self.records.drain(..drain_count);
        }
    }

    /// Get a summary of the dynamic priority system.
    pub fn get_summary(
        &self,
        tasks_evaluated: usize,
        tasks_adjusted: usize,
    ) -> DynamicPrioritySummary {
        DynamicPrioritySummary {
            enabled: self.config.enabled,
            tasks_evaluated,
            tasks_adjusted,
            last_adjustment_at: self.last_adjustment_at,
            weights: self.config.weights.clone(),
            recent_adjustments: self.records.clone(),
        }
    }

    /// Get adjustment history.
    pub fn get_records(&self) -> &[AdjustmentRecord] {
        &self.records
    }

    /// Clear adjustment history.
    pub fn clear_records(&mut self) {
        self.records.clear();
    }
}

// Persistence functions

/// Save dynamic priority config to disk.
pub fn save_dynamic_priority_config(
    data_dir: &Path,
    config: &DynamicPriorityConfig,
) -> Result<(), DynamicPriorityError> {
    let path = data_dir.join("dynamic_priority_config.json");
    let json = serde_json::to_string_pretty(config)?;
    std::fs::write(&path, json)?;
    Ok(())
}

/// Load dynamic priority config from disk.
pub fn load_dynamic_priority_config(data_dir: &Path) -> Option<DynamicPriorityConfig> {
    let path = data_dir.join("dynamic_priority_config.json");
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_task(
        id: &str,
        priority: DynamicPriority,
        speed: u64,
        progress: u32,
        retries: u32,
        size: u64,
        created_at: DateTime<Utc>,
        is_queued: bool,
    ) -> TaskPriorityInput {
        TaskPriorityInput {
            task_id: id.to_string(),
            current_priority: priority,
            current_speed_bps: speed,
            progress_pct: progress,
            retry_count: retries,
            file_size_bytes: size,
            created_at,
            is_queued,
        }
    }

    #[test]
    fn test_default_config() {
        let config = DynamicPriorityConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.min_wait_secs, 300);
        assert_eq!(config.slow_speed_bps, 50_000);
        assert_eq!(config.near_complete_pct, 85);
        assert_eq!(config.max_retry_threshold, 3);
        assert_eq!(config.small_file_bytes, 10 * 1024 * 1024);
        assert_eq!(config.check_interval_secs, 300);
        assert_eq!(config.max_changes_per_cycle, 5);
    }

    #[test]
    fn test_priority_conversion() {
        assert_eq!(
            DynamicPriority::from_download_priority(crate::DownloadPriority::Low),
            DynamicPriority::Low
        );
        assert_eq!(
            DynamicPriority::from_download_priority(crate::DownloadPriority::Normal),
            DynamicPriority::Normal
        );
        assert_eq!(
            DynamicPriority::from_download_priority(crate::DownloadPriority::High),
            DynamicPriority::High
        );
        assert_eq!(
            DynamicPriority::High.to_download_priority(),
            crate::DownloadPriority::High
        );
    }

    #[test]
    fn test_score_calculation_slow_speed() {
        let mgr = DynamicPriorityManager::new();
        let task = make_task(
            "t1",
            DynamicPriority::Normal,
            10_000, // Very slow (10 KB/s, threshold is 50 KB/s)
            30,
            0,
            100 * 1024 * 1024, // 100 MB (large file)
            Utc::now() - Duration::seconds(60),
            false,
        );
        let score = mgr.calculate_score(&task);
        // Slow speed should produce negative score
        assert!(
            score < 0.0,
            "expected negative score for slow task, got {}",
            score
        );
    }

    #[test]
    fn test_score_calculation_long_wait() {
        let mgr = DynamicPriorityManager::new();
        let task = make_task(
            "t2",
            DynamicPriority::Normal,
            0,
            0,
            0,
            100 * 1024 * 1024,
            Utc::now() - Duration::seconds(3600), // 1 hour ago
            true,
        );
        let score = mgr.calculate_score(&task);
        // Long wait should produce positive score
        assert!(
            score > 0.0,
            "expected positive score for long-waiting task, got {}",
            score
        );
    }

    #[test]
    fn test_score_calculation_near_complete() {
        let mgr = DynamicPriorityManager::new();
        let task = make_task(
            "t3",
            DynamicPriority::Normal,
            0,
            95, // 95% complete (threshold is 85%)
            0,
            100 * 1024 * 1024,
            Utc::now() - Duration::seconds(60),
            false,
        );
        let score = mgr.calculate_score(&task);
        assert!(
            score > 0.0,
            "expected positive score for near-complete task, got {}",
            score
        );
    }

    #[test]
    fn test_score_calculation_frequent_retries() {
        let mgr = DynamicPriorityManager::new();
        let task = make_task(
            "t4",
            DynamicPriority::Normal,
            0,
            20,
            5, // 5 retries (threshold is 3)
            100 * 1024 * 1024,
            Utc::now() - Duration::seconds(60),
            false,
        );
        let score = mgr.calculate_score(&task);
        assert!(
            score < 0.0,
            "expected negative score for frequent retries, got {}",
            score
        );
    }

    #[test]
    fn test_score_calculation_small_file() {
        let mgr = DynamicPriorityManager::new();
        let task = make_task(
            "t5",
            DynamicPriority::Normal,
            0,
            0,
            0,
            5 * 1024 * 1024, // 5 MB (below 10 MB threshold)
            Utc::now() - Duration::seconds(60),
            false,
        );
        let score = mgr.calculate_score(&task);
        assert!(
            score > 0.0,
            "expected positive score for small file, got {}",
            score
        );
    }

    #[test]
    fn test_score_to_priority_boost() {
        let mgr = DynamicPriorityManager::new();
        assert_eq!(
            mgr.score_to_priority(5.0, DynamicPriority::Normal),
            DynamicPriority::High
        );
        assert_eq!(
            mgr.score_to_priority(3.0, DynamicPriority::Low),
            DynamicPriority::High
        );
    }

    #[test]
    fn test_score_to_priority_lower() {
        let mgr = DynamicPriorityManager::new();
        assert_eq!(
            mgr.score_to_priority(-5.0, DynamicPriority::Normal),
            DynamicPriority::Low
        );
        assert_eq!(
            mgr.score_to_priority(-3.0, DynamicPriority::High),
            DynamicPriority::Low
        );
    }

    #[test]
    fn test_score_to_priority_no_change() {
        let mgr = DynamicPriorityManager::new();
        assert_eq!(
            mgr.score_to_priority(1.0, DynamicPriority::Normal),
            DynamicPriority::Normal
        );
        assert_eq!(
            mgr.score_to_priority(-1.0, DynamicPriority::High),
            DynamicPriority::High
        );
        assert_eq!(
            mgr.score_to_priority(0.0, DynamicPriority::Low),
            DynamicPriority::Low
        );
    }

    #[test]
    fn test_score_to_priority_already_max() {
        let mgr = DynamicPriorityManager::new();
        // Already high, even with positive score
        assert_eq!(
            mgr.score_to_priority(5.0, DynamicPriority::High),
            DynamicPriority::High
        );
    }

    #[test]
    fn test_score_to_priority_already_min() {
        let mgr = DynamicPriorityManager::new();
        // Already low, even with negative score
        assert_eq!(
            mgr.score_to_priority(-5.0, DynamicPriority::Low),
            DynamicPriority::Low
        );
    }

    #[test]
    fn test_evaluate_disabled() {
        let mgr = DynamicPriorityManager::new();
        // Not enabled by default
        let tasks = vec![make_task(
            "t1",
            DynamicPriority::Normal,
            10_000,
            0,
            0,
            100 * 1024 * 1024,
            Utc::now() - Duration::seconds(3600),
            true,
        )];
        let adjustments = mgr.evaluate(&tasks);
        assert!(adjustments.is_empty());
    }

    #[test]
    fn test_evaluate_with_changes() {
        let mut mgr = DynamicPriorityManager::new();
        mgr.set_enabled(true);

        let tasks = vec![
            // Long-waiting task should get boosted
            make_task(
                "t1",
                DynamicPriority::Normal,
                0,
                0,
                0,
                100 * 1024 * 1024,
                Utc::now() - Duration::seconds(7200), // 2 hours
                true,
            ),
            // Slow task should get lowered
            make_task(
                "t2",
                DynamicPriority::Normal,
                5_000, // Very slow
                10,
                5, // Many retries
                100 * 1024 * 1024,
                Utc::now() - Duration::seconds(60),
                false,
            ),
        ];

        let adjustments = mgr.evaluate(&tasks);
        // At least one task should have an adjustment
        assert!(!adjustments.is_empty());
    }

    #[test]
    fn test_evaluate_max_changes_limit() {
        let mut mgr = DynamicPriorityManager::new();
        mgr.set_enabled(true);
        mgr.config.max_changes_per_cycle = 1;

        // Create multiple tasks that would all want adjustment
        let tasks = vec![
            make_task(
                "t1",
                DynamicPriority::Normal,
                0,
                0,
                0,
                100 * 1024 * 1024,
                Utc::now() - Duration::seconds(7200),
                true,
            ),
            make_task(
                "t2",
                DynamicPriority::Normal,
                5_000,
                10,
                5,
                100 * 1024 * 1024,
                Utc::now() - Duration::seconds(60),
                false,
            ),
            make_task(
                "t3",
                DynamicPriority::Normal,
                0,
                0,
                0,
                100 * 1024 * 1024,
                Utc::now() - Duration::seconds(5400),
                true,
            ),
        ];

        let adjustments = mgr.evaluate(&tasks);
        assert!(adjustments.len() <= 1);
    }

    #[test]
    fn test_record_adjustments() {
        let mut mgr = DynamicPriorityManager::new();
        let adjustments = vec![PriorityAdjustment {
            task_id: "t1".to_string(),
            old_priority: DynamicPriority::Normal,
            new_priority: DynamicPriority::High,
            score: 5.0,
            reason: "long wait".to_string(),
        }];

        mgr.record_adjustments(adjustments);
        assert_eq!(mgr.get_records().len(), 1);
        assert!(mgr.last_adjustment_at.is_some());
    }

    #[test]
    fn test_record_adjustments_trim() {
        let mut mgr = DynamicPriorityManager::new();

        // Add more than MAX_ADJUSTMENT_RECORDS
        for i in 0..120 {
            mgr.record_adjustments(vec![PriorityAdjustment {
                task_id: format!("t{}", i),
                old_priority: DynamicPriority::Normal,
                new_priority: DynamicPriority::High,
                score: 5.0,
                reason: "test".to_string(),
            }]);
        }

        assert!(mgr.get_records().len() <= MAX_ADJUSTMENT_RECORDS);
    }

    #[test]
    fn test_clear_records() {
        let mut mgr = DynamicPriorityManager::new();
        mgr.record_adjustments(vec![PriorityAdjustment {
            task_id: "t1".to_string(),
            old_priority: DynamicPriority::Normal,
            new_priority: DynamicPriority::High,
            score: 5.0,
            reason: "test".to_string(),
        }]);
        assert!(!mgr.get_records().is_empty());

        mgr.clear_records();
        assert!(mgr.get_records().is_empty());
    }

    #[test]
    fn test_get_summary() {
        let mgr = DynamicPriorityManager::new();
        let summary = mgr.get_summary(10, 2);
        assert!(!summary.enabled);
        assert_eq!(summary.tasks_evaluated, 10);
        assert_eq!(summary.tasks_adjusted, 2);
        assert!(summary.last_adjustment_at.is_none());
    }

    #[test]
    fn test_config_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let config = DynamicPriorityConfig {
            enabled: true,
            min_wait_secs: 600,
            ..Default::default()
        };

        save_dynamic_priority_config(dir.path(), &config).unwrap();
        let loaded = load_dynamic_priority_config(dir.path()).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.min_wait_secs, 600);
    }

    #[test]
    fn test_config_persistence_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_dynamic_priority_config(dir.path());
        assert!(result.is_none());
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = DynamicPriorityConfig {
            enabled: true,
            weights: FactorWeights {
                speed_weight: 2.0,
                wait_time_weight: 3.0,
                progress_weight: 1.5,
                retry_weight: 0.5,
                size_weight: 0.8,
            },
            min_wait_secs: 120,
            slow_speed_bps: 100_000,
            near_complete_pct: 90,
            max_retry_threshold: 5,
            small_file_bytes: 5 * 1024 * 1024,
            check_interval_secs: 60,
            max_changes_per_cycle: 3,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DynamicPriorityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.weights.speed_weight, 2.0);
        assert_eq!(deserialized.weights.wait_time_weight, 3.0);
        assert_eq!(deserialized.min_wait_secs, 120);
    }

    #[test]
    fn test_score_clamping() {
        let mut mgr = DynamicPriorityManager::new();
        // Extreme weights to force clamping
        mgr.config.weights = FactorWeights {
            speed_weight: 5.0,
            wait_time_weight: 5.0,
            progress_weight: 5.0,
            retry_weight: 5.0,
            size_weight: 5.0,
        };

        // Task with all negative factors
        let task = make_task(
            "t1",
            DynamicPriority::Normal,
            1_000, // Extremely slow
            10,
            10,                // Many retries
            100 * 1024 * 1024, // Large file
            Utc::now() - Duration::seconds(60),
            false,
        );
        let score = mgr.calculate_score(&task);
        assert!(
            score >= -10.0,
            "score should not be below -10.0, got {}",
            score
        );

        // Task with all positive factors
        let task2 = make_task(
            "t2",
            DynamicPriority::Normal,
            200_000,                              // Very fast
            99,                                   // Near complete
            0,                                    // No retries
            1 * 1024 * 1024,                      // Small file
            Utc::now() - Duration::seconds(7200), // Long wait
            true,
        );
        let score2 = mgr.calculate_score(&task2);
        assert!(
            score2 <= 10.0,
            "score should not be above 10.0, got {}",
            score2
        );
    }

    #[test]
    fn test_no_change_for_already_high_with_positive_factors() {
        let mgr = DynamicPriorityManager::new();
        let task = make_task(
            "t1",
            DynamicPriority::High, // Already high
            200_000,
            95,
            0,
            1 * 1024 * 1024,
            Utc::now() - Duration::seconds(7200),
            true,
        );
        let score = mgr.calculate_score(&task);
        let new_priority = mgr.score_to_priority(score, DynamicPriority::High);
        assert_eq!(new_priority, DynamicPriority::High);
    }

    #[test]
    fn test_generate_reason_factors() {
        let reason = DynamicPriorityManager::generate_reason(
            5.0,
            &make_task(
                "t1",
                DynamicPriority::Normal,
                10_000,          // slow
                90,              // near complete
                5,               // frequent retries
                5 * 1024 * 1024, // small
                Utc::now() - Duration::seconds(3600),
                true,
            ),
        );
        assert!(reason.contains("slow speed"));
        assert!(reason.contains("long wait time"));
        assert!(reason.contains("near completion"));
        assert!(reason.contains("frequent retries"));
        assert!(reason.contains("small file"));
    }

    #[test]
    fn test_evaluate_empty_tasks() {
        let mut mgr = DynamicPriorityManager::new();
        mgr.set_enabled(true);
        let adjustments = mgr.evaluate(&[]);
        assert!(adjustments.is_empty());
    }

    #[test]
    fn test_set_enabled() {
        let mut mgr = DynamicPriorityManager::new();
        assert!(!mgr.is_enabled());
        mgr.set_enabled(true);
        assert!(mgr.is_enabled());
    }

    #[test]
    fn test_set_config() {
        let mut mgr = DynamicPriorityManager::new();
        let config = DynamicPriorityConfig {
            enabled: true,
            min_wait_secs: 999,
            ..Default::default()
        };
        mgr.set_config(config);
        assert!(mgr.is_enabled());
        assert_eq!(mgr.get_config().min_wait_secs, 999);
    }
}
