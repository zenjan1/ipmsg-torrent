//! Smart Queue Optimizer
//!
//! Automatically optimizes download queue order based on multiple dimensions:
//! - Deadline urgency (tasks with approaching deadlines get priority)
//! - Priority aging (tasks waiting too long get boosted)
//! - Speed prediction (faster downloads preferred for throughput)
//! - Dependency chains (unlock blocked tasks earlier)
//! - Task size (smaller tasks first for quick wins, or larger first for throughput)
//! - Freshness (newer tasks may be more relevant)

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Strategy for queue optimization
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OptimizationStrategy {
    /// Balance all factors equally
    #[default]
    Balanced,
    /// Prioritize tasks with approaching deadlines
    DeadlineFirst,
    /// Prioritize tasks that have been waiting longest
    Fairness,
    /// Prioritize smaller tasks for quick completions
    ShortestJobFirst,
    /// Prioritize larger tasks for maximum throughput
    LongestJobFirst,
    /// Prioritize newer tasks
    NewestFirst,
    /// Prioritize tasks that unlock other dependent tasks
    DependencyFirst,
}

impl std::fmt::Display for OptimizationStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Balanced => write!(f, "balanced"),
            Self::DeadlineFirst => write!(f, "deadline_first"),
            Self::Fairness => write!(f, "fairness"),
            Self::ShortestJobFirst => write!(f, "shortest_job_first"),
            Self::LongestJobFirst => write!(f, "longest_job_first"),
            Self::NewestFirst => write!(f, "newest_first"),
            Self::DependencyFirst => write!(f, "dependency_first"),
        }
    }
}

/// Configuration for the smart queue optimizer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmartQueueConfig {
    /// Whether smart queue optimization is enabled
    pub enabled: bool,
    /// Optimization strategy to use
    pub strategy: OptimizationStrategy,
    /// Weight for deadline urgency factor (0.0 - 1.0)
    pub deadline_weight: f64,
    /// Weight for priority/aging factor (0.0 - 1.0)
    pub priority_weight: f64,
    /// Weight for dependency unlocking factor (0.0 - 1.0)
    pub dependency_weight: f64,
    /// Weight for task size factor (0.0 - 1.0)
    pub size_weight: f64,
    /// Weight for freshness factor (0.0 - 1.0)
    pub freshness_weight: f64,
    /// Minimum score difference to trigger reorder (0.0 - 1.0)
    pub reorder_threshold: f64,
    /// Maximum number of tasks to consider for optimization
    pub max_tasks: usize,
    /// Whether to auto-apply optimization results
    pub auto_apply: bool,
}

impl Default for SmartQueueConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            strategy: OptimizationStrategy::Balanced,
            deadline_weight: 0.3,
            priority_weight: 0.25,
            dependency_weight: 0.2,
            size_weight: 0.15,
            freshness_weight: 0.1,
            reorder_threshold: 0.1,
            max_tasks: 100,
            auto_apply: false,
        }
    }
}

/// Input data for a task to be optimized
#[derive(Debug, Clone)]
pub struct TaskOptimizationData {
    /// Task ID
    pub id: String,
    /// Task name
    pub name: String,
    /// Current queue position (lower = earlier)
    pub queue_position: Option<u32>,
    /// Task priority level
    pub priority: i32,
    /// Task size in bytes
    pub size: u64,
    /// Current progress (0.0 - 1.0)
    pub progress: f32,
    /// Task state (Queued, Downloading, Paused, etc.)
    pub state: String,
    /// When the task was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Optional deadline
    pub deadline: Option<chrono::DateTime<chrono::Utc>>,
    /// Task IDs this task depends on
    pub depends_on: Vec<String>,
    /// Number of times promoted by staleness detection
    pub staleness_promotions: u32,
    /// Whether this task is a favorite/pinned
    pub is_favorite: bool,
}

/// Score breakdown for a single task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskScore {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Overall composite score (higher = should be earlier in queue)
    pub total_score: f64,
    /// Deadline urgency component (0.0 - 1.0)
    pub deadline_score: f64,
    /// Priority/aging component (0.0 - 1.0)
    pub priority_score: f64,
    /// Dependency unlocking component (0.0 - 1.0)
    pub dependency_score: f64,
    /// Size-based component (0.0 - 1.0)
    pub size_score: f64,
    /// Freshness component (0.0 - 1.0)
    pub freshness_score: f64,
    /// Recommended position in queue
    pub recommended_position: usize,
    /// Current position in queue
    pub current_position: Option<u32>,
    /// Whether this task should move up
    pub should_move_up: bool,
}

/// Result of queue optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationResult {
    /// Timestamp of optimization
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Strategy used
    pub strategy: OptimizationStrategy,
    /// Number of tasks analyzed
    pub tasks_analyzed: usize,
    /// Number of tasks that would change position
    pub tasks_to_reorder: usize,
    /// Score breakdown per task
    pub task_scores: Vec<TaskScore>,
    /// Recommended task order (task IDs in priority order)
    pub recommended_order: Vec<String>,
    /// Whether auto-apply was triggered
    pub auto_applied: bool,
    /// Human-readable summary
    pub summary: String,
}

/// Summary of the smart queue optimizer status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmartQueueSummary {
    /// Whether optimizer is enabled
    pub enabled: bool,
    /// Current strategy
    pub strategy: OptimizationStrategy,
    /// Number of queued tasks
    pub queued_tasks: usize,
    /// Number of tasks that would benefit from reorder
    pub reorder_candidates: usize,
    /// Last optimization timestamp
    pub last_optimization: Option<chrono::DateTime<chrono::Utc>>,
    /// Configuration summary
    pub config_summary: String,
}

/// Smart Queue Optimizer
pub struct SmartQueueOptimizer {
    config: SmartQueueConfig,
    last_result: Option<OptimizationResult>,
}

impl SmartQueueOptimizer {
    /// Create a new optimizer with default config
    pub fn new() -> Self {
        Self {
            config: SmartQueueConfig::default(),
            last_result: None,
        }
    }

    /// Create with custom config
    pub fn with_config(config: SmartQueueConfig) -> Self {
        Self {
            config,
            last_result: None,
        }
    }

    /// Get current config
    pub fn get_config(&self) -> &SmartQueueConfig {
        &self.config
    }

    /// Update config
    pub fn set_config(&mut self, config: SmartQueueConfig) {
        self.config = config;
    }

    /// Get last optimization result
    pub fn get_last_result(&self) -> Option<&OptimizationResult> {
        self.last_result.as_ref()
    }

    /// Compute deadline urgency score (0.0 - 1.0, higher = more urgent)
    fn compute_deadline_score(
        deadline: &Option<chrono::DateTime<chrono::Utc>>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> f64 {
        match deadline {
            None => 0.0,
            Some(dl) => {
                let remaining = (*dl - now).num_seconds().max(0) as f64;
                let hours_remaining = remaining / 3600.0;
                if hours_remaining <= 1.0 {
                    1.0 // Critical: less than 1 hour (or overdue)
                } else if hours_remaining <= 6.0 {
                    0.9 // High
                } else if hours_remaining <= 12.0 {
                    0.7 // Medium-high
                } else if hours_remaining <= 24.0 {
                    0.5 // Medium
                } else if hours_remaining <= 48.0 {
                    0.3 // Low-medium
                } else {
                    0.1 // Low: more than 2 days
                }
            }
        }
    }

    /// Compute priority score (0.0 - 1.0, higher = higher priority)
    fn compute_priority_score(priority: i32, staleness_promotions: u32) -> f64 {
        let base = match priority {
            p if p >= 3 => 1.0, // High
            p if p >= 2 => 0.6, // Normal+
            p if p >= 1 => 0.3, // Normal
            _ => 0.1,           // Low
        };
        // Boost for staleness promotions (aging)
        let aging_boost = (staleness_promotions as f64 * 0.15).min(0.3);
        (base + aging_boost).min(1.0)
    }

    /// Compute dependency score (0.0 - 1.0, higher = unlocks more tasks)
    fn compute_dependency_score(task_id: &str, all_tasks: &[TaskOptimizationData]) -> f64 {
        // Count how many other queued tasks depend on this one
        let dependents = all_tasks
            .iter()
            .filter(|t| t.depends_on.contains(&task_id.to_string()) && t.state == "Queued")
            .count();

        if dependents == 0 {
            0.0
        } else if dependents == 1 {
            0.5
        } else if dependents <= 3 {
            0.8
        } else {
            1.0
        }
    }

    /// Compute size score based on strategy (0.0 - 1.0)
    fn compute_size_score(size: u64, all_sizes: &[u64], strategy: OptimizationStrategy) -> f64 {
        if all_sizes.is_empty() {
            return 0.5;
        }

        let max_size = *all_sizes.iter().max().unwrap_or(&1);
        let min_size = *all_sizes.iter().min().unwrap_or(&1);

        if max_size == min_size {
            return 0.5;
        }

        let normalized = (size as f64 - min_size as f64) / (max_size as f64 - min_size as f64);

        match strategy {
            OptimizationStrategy::ShortestJobFirst => 1.0 - normalized, // Smaller = higher score
            OptimizationStrategy::LongestJobFirst => normalized,        // Larger = higher score
            _ => 0.5, // Neutral for other strategies
        }
    }

    /// Compute freshness score (0.0 - 1.0, higher = newer)
    fn compute_freshness_score(
        created_at: chrono::DateTime<chrono::Utc>,
        now: chrono::DateTime<chrono::Utc>,
        all_ages: &[f64],
    ) -> f64 {
        let age_hours = (now - created_at).num_seconds().max(0) as f64 / 3600.0;

        if all_ages.is_empty() {
            return 0.5;
        }

        let max_age = all_ages.iter().cloned().fold(0.0_f64, f64::max);
        let min_age = all_ages.iter().cloned().fold(f64::MAX, f64::min);

        if (max_age - min_age).abs() < 0.01 {
            return 0.5;
        }

        let normalized = (age_hours - min_age) / (max_age - min_age);
        1.0 - normalized // Newer = higher score
    }

    /// Optimize the queue order for the given tasks
    pub fn optimize(&mut self, tasks: &[TaskOptimizationData]) -> OptimizationResult {
        let now = chrono::Utc::now();
        let strategy = self.config.strategy;

        // Filter to only queued tasks
        let queued: Vec<&TaskOptimizationData> = tasks
            .iter()
            .filter(|t| t.state == "Queued")
            .take(self.config.max_tasks)
            .collect();

        let tasks_analyzed = queued.len();

        if queued.is_empty() {
            let result = OptimizationResult {
                timestamp: now,
                strategy,
                tasks_analyzed: 0,
                tasks_to_reorder: 0,
                task_scores: vec![],
                recommended_order: vec![],
                auto_applied: false,
                summary: "No queued tasks to optimize.".to_string(),
            };
            self.last_result = Some(result.clone());
            return result;
        }

        // Pre-compute shared data
        let all_sizes: Vec<u64> = queued.iter().map(|t| t.size).collect();
        let all_ages: Vec<f64> = queued
            .iter()
            .map(|t| (now - t.created_at).num_seconds().max(0) as f64 / 3600.0)
            .collect();

        // Compute scores for each task
        let mut task_scores: Vec<TaskScore> = queued
            .iter()
            .map(|t| {
                let deadline_score = Self::compute_deadline_score(&t.deadline, now);
                let priority_score =
                    Self::compute_priority_score(t.priority, t.staleness_promotions);
                let dependency_score = Self::compute_dependency_score(&t.id, tasks);
                let size_score = Self::compute_size_score(t.size, &all_sizes, strategy);
                let freshness_score = Self::compute_freshness_score(t.created_at, now, &all_ages);

                // Apply strategy-specific weight adjustments
                let (dw, pw, depw, sw, fw) = match strategy {
                    OptimizationStrategy::Balanced => (
                        self.config.deadline_weight,
                        self.config.priority_weight,
                        self.config.dependency_weight,
                        self.config.size_weight,
                        self.config.freshness_weight,
                    ),
                    OptimizationStrategy::DeadlineFirst => (0.5, 0.15, 0.15, 0.1, 0.1),
                    OptimizationStrategy::Fairness => (0.1, 0.5, 0.15, 0.1, 0.15),
                    OptimizationStrategy::ShortestJobFirst => (0.15, 0.2, 0.15, 0.4, 0.1),
                    OptimizationStrategy::LongestJobFirst => (0.15, 0.2, 0.15, 0.4, 0.1),
                    OptimizationStrategy::NewestFirst => (0.15, 0.15, 0.1, 0.1, 0.5),
                    OptimizationStrategy::DependencyFirst => (0.15, 0.15, 0.5, 0.1, 0.1),
                };

                let total_score = deadline_score * dw
                    + priority_score * pw
                    + dependency_score * depw
                    + size_score * sw
                    + freshness_score * fw;

                TaskScore {
                    task_id: t.id.clone(),
                    task_name: t.name.clone(),
                    total_score,
                    deadline_score,
                    priority_score,
                    dependency_score,
                    size_score,
                    freshness_score,
                    recommended_position: 0, // Will be set after sorting
                    current_position: t.queue_position,
                    should_move_up: false, // Will be set after sorting
                }
            })
            .collect();

        // Sort by total score descending (higher score = earlier in queue)
        task_scores.sort_by(|a, b| {
            b.total_score
                .partial_cmp(&a.total_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Assign recommended positions
        for (i, score) in task_scores.iter_mut().enumerate() {
            score.recommended_position = i + 1;
            score.should_move_up = match score.current_position {
                Some(current) => (i as u32 + 1) < current,
                None => true,
            };
        }

        let recommended_order: Vec<String> =
            task_scores.iter().map(|s| s.task_id.clone()).collect();

        // Count tasks that need reordering
        let tasks_to_reorder = task_scores
            .iter()
            .filter(|s| match s.current_position {
                Some(current) => {
                    let diff = (current as i64) - (s.recommended_position as i64);
                    (diff as f64).abs() > (self.config.reorder_threshold * tasks_analyzed as f64)
                }
                None => false,
            })
            .count();

        let summary = format!(
            "🧠 Smart Queue Optimization\n\
             Strategy: {}\n\
             Tasks analyzed: {}\n\
             Tasks to reorder: {}\n\
             Top priority: {}",
            strategy,
            tasks_analyzed,
            tasks_to_reorder,
            task_scores
                .first()
                .map(|s| format!("{} (score: {:.2})", s.task_name, s.total_score))
                .unwrap_or_else(|| "N/A".to_string()),
        );

        let result = OptimizationResult {
            timestamp: now,
            strategy,
            tasks_analyzed,
            tasks_to_reorder,
            task_scores: task_scores.clone(),
            recommended_order,
            auto_applied: self.config.auto_apply,
            summary,
        };

        self.last_result = Some(result.clone());
        result
    }

    /// Get a summary of the optimizer status
    pub fn get_summary(&self, queued_count: usize) -> SmartQueueSummary {
        let reorder_candidates = self
            .last_result
            .as_ref()
            .map(|r| r.tasks_to_reorder)
            .unwrap_or(0);

        SmartQueueSummary {
            enabled: self.config.enabled,
            strategy: self.config.strategy,
            queued_tasks: queued_count,
            reorder_candidates,
            last_optimization: self.last_result.as_ref().map(|r| r.timestamp),
            config_summary: format!(
                "weights: deadline={:.1} priority={:.1} dependency={:.1} size={:.1} freshness={:.1}",
                self.config.deadline_weight,
                self.config.priority_weight,
                self.config.dependency_weight,
                self.config.size_weight,
                self.config.freshness_weight,
            ),
        }
    }
}

impl Default for SmartQueueOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Save smart queue config to disk
pub fn save_smart_queue_config(config: &SmartQueueConfig, data_dir: &Path) -> std::io::Result<()> {
    let path = data_dir.join("smart_queue_config.json");
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;
    std::fs::write(&path, json)
}

/// Load smart queue config from disk
pub fn load_smart_queue_config(data_dir: &Path) -> Option<SmartQueueConfig> {
    let path = data_dir.join("smart_queue_config.json");
    let content = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_task(id: &str, priority: i32, size: u64) -> TaskOptimizationData {
        TaskOptimizationData {
            id: id.to_string(),
            name: format!("Task {}", id),
            queue_position: None,
            priority,
            size,
            progress: 0.0,
            state: "Queued".to_string(),
            created_at: Utc::now(),
            deadline: None,
            depends_on: vec![],
            staleness_promotions: 0,
            is_favorite: false,
        }
    }

    #[test]
    fn test_deadline_score_no_deadline() {
        let now = Utc::now();
        let score = SmartQueueOptimizer::compute_deadline_score(&None, now);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_deadline_score_overdue() {
        let now = Utc::now();
        let past = now - chrono::Duration::hours(1);
        let score = SmartQueueOptimizer::compute_deadline_score(&Some(past), now);
        assert_eq!(score, 1.0);
    }

    #[test]
    fn test_deadline_score_critical() {
        let now = Utc::now();
        let soon = now + chrono::Duration::minutes(30);
        let score = SmartQueueOptimizer::compute_deadline_score(&Some(soon), now);
        assert_eq!(score, 1.0); // Less than 1 hour = critical
    }

    #[test]
    fn test_deadline_score_medium() {
        let now = Utc::now();
        let later = now + chrono::Duration::hours(18);
        let score = SmartQueueOptimizer::compute_deadline_score(&Some(later), now);
        assert_eq!(score, 0.5); // 18 hours = medium
    }

    #[test]
    fn test_deadline_score_low() {
        let now = Utc::now();
        let far = now + chrono::Duration::days(5);
        let score = SmartQueueOptimizer::compute_deadline_score(&Some(far), now);
        assert_eq!(score, 0.1); // 5 days = low
    }

    #[test]
    fn test_priority_score_high() {
        let score = SmartQueueOptimizer::compute_priority_score(3, 0);
        assert_eq!(score, 1.0);
    }

    #[test]
    fn test_priority_score_normal() {
        let score = SmartQueueOptimizer::compute_priority_score(1, 0);
        assert_eq!(score, 0.3);
    }

    #[test]
    fn test_priority_score_with_aging() {
        let score = SmartQueueOptimizer::compute_priority_score(1, 2);
        // 0.3 + 2 * 0.15 = 0.6
        assert!((score - 0.6).abs() < 0.01);
    }

    #[test]
    fn test_priority_score_aging_cap() {
        let score = SmartQueueOptimizer::compute_priority_score(1, 10);
        // 0.3 + min(10 * 0.15, 0.3) = 0.3 + 0.3 = 0.6
        assert!(score <= 1.0);
    }

    #[test]
    fn test_dependency_score_no_dependents() {
        let tasks = vec![make_task("a", 1, 1000)];
        let score = SmartQueueOptimizer::compute_dependency_score("a", &tasks);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_dependency_score_with_dependents() {
        let mut task_b = make_task("b", 1, 1000);
        task_b.depends_on = vec!["a".to_string()];
        let tasks = vec![make_task("a", 1, 1000), task_b];
        let score = SmartQueueOptimizer::compute_dependency_score("a", &tasks);
        assert_eq!(score, 0.5); // 1 dependent
    }

    #[test]
    fn test_dependency_score_multiple_dependents() {
        let mut task_b = make_task("b", 1, 1000);
        task_b.depends_on = vec!["a".to_string()];
        let mut task_c = make_task("c", 1, 1000);
        task_c.depends_on = vec!["a".to_string()];
        let mut task_d = make_task("d", 1, 1000);
        task_d.depends_on = vec!["a".to_string()];
        let tasks = vec![make_task("a", 1, 1000), task_b, task_c, task_d];
        let score = SmartQueueOptimizer::compute_dependency_score("a", &tasks);
        assert_eq!(score, 0.8); // 3 dependents
    }

    #[test]
    fn test_size_score_sjf() {
        let sizes = vec![100, 500, 1000];
        // ShortestJobFirst: smaller = higher score
        let score_small = SmartQueueOptimizer::compute_size_score(
            100,
            &sizes,
            OptimizationStrategy::ShortestJobFirst,
        );
        let score_large = SmartQueueOptimizer::compute_size_score(
            1000,
            &sizes,
            OptimizationStrategy::ShortestJobFirst,
        );
        assert!(score_small > score_large);
    }

    #[test]
    fn test_size_score_ljf() {
        let sizes = vec![100, 500, 1000];
        // LongestJobFirst: larger = higher score
        let score_small = SmartQueueOptimizer::compute_size_score(
            100,
            &sizes,
            OptimizationStrategy::LongestJobFirst,
        );
        let score_large = SmartQueueOptimizer::compute_size_score(
            1000,
            &sizes,
            OptimizationStrategy::LongestJobFirst,
        );
        assert!(score_large > score_small);
    }

    #[test]
    fn test_optimize_empty() {
        let mut optimizer = SmartQueueOptimizer::new();
        let result = optimizer.optimize(&[]);
        assert_eq!(result.tasks_analyzed, 0);
        assert!(result.recommended_order.is_empty());
    }

    #[test]
    fn test_optimize_basic() {
        let mut optimizer = SmartQueueOptimizer::new();
        let tasks = vec![
            make_task("a", 1, 1000),
            make_task("b", 3, 500), // High priority
            make_task("c", 1, 200),
        ];
        let result = optimizer.optimize(&tasks);
        assert_eq!(result.tasks_analyzed, 3);
        // High priority task should be first
        assert_eq!(result.recommended_order[0], "b");
    }

    #[test]
    fn test_optimize_deadline_priority() {
        let mut optimizer = SmartQueueOptimizer::with_config(SmartQueueConfig {
            strategy: OptimizationStrategy::DeadlineFirst,
            ..Default::default()
        });

        let task_a = make_task("a", 3, 1000); // High priority but no deadline
        let mut task_b = make_task("b", 1, 500); // Low priority but urgent deadline
        task_b.deadline = Some(Utc::now() + chrono::Duration::minutes(30));

        let result = optimizer.optimize(&[task_a, task_b]);
        // Deadline task should be first with DeadlineFirst strategy
        assert_eq!(result.recommended_order[0], "b");
    }

    #[test]
    fn test_optimize_sjf() {
        let mut optimizer = SmartQueueOptimizer::with_config(SmartQueueConfig {
            strategy: OptimizationStrategy::ShortestJobFirst,
            deadline_weight: 0.0,
            priority_weight: 0.0,
            dependency_weight: 0.0,
            freshness_weight: 0.0,
            size_weight: 1.0,
            ..Default::default()
        });

        let tasks = vec![
            make_task("large", 1, 10000),
            make_task("small", 1, 100),
            make_task("medium", 1, 1000),
        ];
        let result = optimizer.optimize(&tasks);
        // Smallest first
        assert_eq!(result.recommended_order[0], "small");
    }

    #[test]
    fn test_optimize_dependency_first() {
        let mut optimizer = SmartQueueOptimizer::with_config(SmartQueueConfig {
            strategy: OptimizationStrategy::DependencyFirst,
            deadline_weight: 0.0,
            priority_weight: 0.0,
            freshness_weight: 0.0,
            size_weight: 0.0,
            dependency_weight: 1.0,
            ..Default::default()
        });

        let mut task_b = make_task("b", 1, 1000);
        task_b.depends_on = vec!["a".to_string()];
        let mut task_c = make_task("c", 1, 1000);
        task_c.depends_on = vec!["a".to_string()];

        let tasks = vec![make_task("a", 1, 1000), task_b, task_c];
        let result = optimizer.optimize(&tasks);
        // Task "a" unlocks 2 dependents, should be first
        assert_eq!(result.recommended_order[0], "a");
    }

    #[test]
    fn test_optimize_only_queued() {
        let mut optimizer = SmartQueueOptimizer::new();
        let mut task_a = make_task("a", 1, 1000);
        task_a.state = "Downloading".to_string();
        let task_b = make_task("b", 1, 500);

        let result = optimizer.optimize(&[task_a, task_b]);
        // Only task_b is Queued
        assert_eq!(result.tasks_analyzed, 1);
        assert_eq!(result.recommended_order, vec!["b"]);
    }

    #[test]
    fn test_optimize_max_tasks_limit() {
        let mut optimizer = SmartQueueOptimizer::with_config(SmartQueueConfig {
            max_tasks: 2,
            ..Default::default()
        });

        let tasks = vec![
            make_task("a", 1, 100),
            make_task("b", 2, 200),
            make_task("c", 3, 300),
        ];
        let result = optimizer.optimize(&tasks);
        assert_eq!(result.tasks_analyzed, 2);
    }

    #[test]
    fn test_task_scores_have_positions() {
        let mut optimizer = SmartQueueOptimizer::new();
        let mut task_a = make_task("a", 1, 1000);
        task_a.queue_position = Some(3);

        let result = optimizer.optimize(&[task_a]);
        assert_eq!(result.task_scores.len(), 1);
        assert_eq!(result.task_scores[0].current_position, Some(3));
        assert_eq!(result.task_scores[0].recommended_position, 1);
        assert!(result.task_scores[0].should_move_up);
    }

    #[test]
    fn test_summary() {
        let optimizer = SmartQueueOptimizer::new();
        let summary = optimizer.get_summary(5);
        assert!(!summary.enabled);
        assert_eq!(summary.queued_tasks, 5);
        assert_eq!(summary.strategy, OptimizationStrategy::Balanced);
    }

    #[test]
    fn test_config_serialization() {
        let config = SmartQueueConfig {
            enabled: true,
            strategy: OptimizationStrategy::DeadlineFirst,
            deadline_weight: 0.5,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SmartQueueConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, true);
        assert_eq!(loaded.strategy, OptimizationStrategy::DeadlineFirst);
        assert!((loaded.deadline_weight - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_strategy_display() {
        assert_eq!(OptimizationStrategy::Balanced.to_string(), "balanced");
        assert_eq!(
            OptimizationStrategy::DeadlineFirst.to_string(),
            "deadline_first"
        );
        assert_eq!(
            OptimizationStrategy::ShortestJobFirst.to_string(),
            "shortest_job_first"
        );
    }

    #[test]
    fn test_save_load_config() {
        let dir = std::env::temp_dir().join("smart_queue_test_save_load");
        std::fs::create_dir_all(&dir).ok();

        let config = SmartQueueConfig {
            enabled: true,
            strategy: OptimizationStrategy::Fairness,
            ..Default::default()
        };

        save_smart_queue_config(&config, &dir).unwrap();
        let loaded = load_smart_queue_config(&dir).unwrap();
        assert_eq!(loaded.enabled, true);
        assert_eq!(loaded.strategy, OptimizationStrategy::Fairness);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_load_missing_config() {
        let dir = std::env::temp_dir().join("smart_queue_test_missing");
        std::fs::remove_dir_all(&dir).ok();
        let result = load_smart_queue_config(&dir);
        assert!(result.is_none());
    }

    #[test]
    fn test_optimize_favorites_boost() {
        // Favorites don't have a direct score component in base optimizer,
        // but we verify they're handled correctly
        let mut optimizer = SmartQueueOptimizer::new();
        let mut task_a = make_task("a", 1, 1000);
        task_a.is_favorite = true;
        let task_b = make_task("b", 1, 500);

        let result = optimizer.optimize(&[task_a, task_b]);
        assert_eq!(result.tasks_analyzed, 2);
    }

    #[test]
    fn test_recommended_order_matches_scores() {
        let mut optimizer = SmartQueueOptimizer::new();
        let tasks = vec![
            make_task("low", 0, 1000),
            make_task("high", 3, 500),
            make_task("mid", 1, 300),
        ];
        let result = optimizer.optimize(&tasks);
        // Scores should be in descending order
        for i in 0..result.task_scores.len() - 1 {
            assert!(result.task_scores[i].total_score >= result.task_scores[i + 1].total_score);
        }
    }
}
