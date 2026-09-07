//! Priority Queue for Download Task Scheduling
//!
//! Implements intelligent task prioritization with:
//! - Priority-based scheduling (Urgent > High > Normal > Low)
//! - Dynamic priority adjustment based on performance metrics
//! - Priority inheritance for parent-child task relationships
//! - Queue visualization with ETA predictions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Priority levels for download tasks.
///
/// Tasks with higher priority receive more bandwidth and are scheduled first.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    /// Lowest priority - background tasks, non-urgent downloads
    Low = 0,
    /// Default priority for normal downloads
    #[default]
    Normal = 1,
    /// High priority - user-initiated urgent downloads
    High = 2,
    /// Critical priority - time-sensitive downloads
    Urgent = 3,
}

impl Priority {
    /// Parse priority from string (case-insensitive).
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "low" | "l" | "0" => Some(Self::Low),
            "normal" | "n" | "1" | "default" => Some(Self::Normal),
            "high" | "h" | "2" => Some(Self::High),
            "urgent" | "u" | "3" | "critical" => Some(Self::Urgent),
            _ => None,
        }
    }

    /// Human-readable label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Urgent => "urgent",
        }
    }

    /// Numeric weight for bandwidth allocation.
    pub fn weight(&self) -> f64 {
        match self {
            Self::Low => 0.5,
            Self::Normal => 1.0,
            Self::High => 2.0,
            Self::Urgent => 4.0,
        }
    }

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
            Self::High | Self::Urgent => crate::DownloadPriority::High,
        }
    }
}

/// A task wrapper with priority metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityTask {
    /// Unique task identifier
    pub task_id: String,
    /// Current priority level
    pub priority: Priority,
    /// Original priority (before dynamic adjustment)
    pub original_priority: Priority,
    /// Timestamp when task was added to queue
    pub added_at: DateTime<Utc>,
    /// Timestamp of last priority update
    pub last_updated: DateTime<Utc>,
    /// Download progress (0.0 - 1.0)
    pub progress: f64,
    /// Current download speed in bytes/sec
    pub current_speed: u64,
    /// Parent task ID for priority inheritance
    pub parent_task_id: Option<String>,
    /// Number of priority adjustments
    pub adjustment_count: u32,
    /// Custom priority score (higher = more important)
    pub priority_score: f64,
}

impl PriorityTask {
    /// Create a new priority task.
    pub fn new(task_id: String, priority: Priority) -> Self {
        let now = Utc::now();
        Self {
            task_id,
            priority,
            original_priority: priority,
            added_at: now,
            last_updated: now,
            progress: 0.0,
            current_speed: 0,
            parent_task_id: None,
            adjustment_count: 0,
            priority_score: priority.weight(),
        }
    }

    /// Set parent task for priority inheritance.
    pub fn with_parent(mut self, parent_id: String) -> Self {
        self.parent_task_id = Some(parent_id);
        self
    }

    /// Update progress and speed metrics.
    pub fn update_metrics(&mut self, progress: f64, speed: u64) {
        self.progress = progress.clamp(0.0, 1.0);
        self.current_speed = speed;
        self.last_updated = Utc::now();
    }

    /// Calculate effective priority score considering all factors.
    pub fn calculate_effective_score(&self, config: &PriorityConfig) -> f64 {
        let base_score = self.priority.weight();
        let mut score = base_score;

        // Age factor: older tasks get slight boost
        let age_hours = (Utc::now() - self.added_at).num_seconds() as f64 / 3600.0;
        score += age_hours * config.age_boost_factor;

        // Progress factor: near-complete tasks get boost
        if self.progress > 0.9 {
            score += config.completion_boost;
        }

        // Speed factor: slow tasks may get penalized
        if self.current_speed < config.slow_speed_threshold {
            score -= config.slow_speed_penalty;
        }

        // Priority inheritance from parent
        if self.parent_task_id.is_some() {
            score *= config.inheritance_multiplier;
        }

        score.max(0.1) // Ensure minimum score
    }
}

/// Configuration for priority queue behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityConfig {
    /// Enable automatic priority adjustment
    #[serde(default = "default_true")]
    pub auto_adjust_enabled: bool,
    /// Interval in seconds between auto-adjustments
    #[serde(default = "default_adjust_interval")]
    pub adjust_interval_secs: u64,
    /// Factor for boosting old tasks (per hour)
    #[serde(default = "default_age_boost")]
    pub age_boost_factor: f64,
    /// Boost for tasks near completion (>90%)
    #[serde(default = "default_completion_boost")]
    pub completion_boost: f64,
    /// Penalty for slow downloads
    #[serde(default = "default_slow_penalty")]
    pub slow_speed_penalty: f64,
    /// Speed threshold considered "slow" (bytes/sec)
    #[serde(default = "default_slow_threshold")]
    pub slow_speed_threshold: u64,
    /// Multiplier for child tasks with priority inheritance
    #[serde(default = "default_inheritance_mult")]
    pub inheritance_multiplier: f64,
    /// Maximum priority adjustments per task
    #[serde(default = "default_max_adjustments")]
    pub max_adjustments: u32,
    /// Enable priority inheritance
    #[serde(default = "default_true")]
    pub inheritance_enabled: bool,
}

fn default_true() -> bool {
    true
}

fn default_adjust_interval() -> u64 {
    60
}

fn default_age_boost() -> f64 {
    0.01
}

fn default_completion_boost() -> f64 {
    0.5
}

fn default_slow_penalty() -> f64 {
    0.3
}

fn default_slow_threshold() -> u64 {
    10240 // 10 KB/s
}

fn default_inheritance_mult() -> f64 {
    1.2
}

fn default_max_adjustments() -> u32 {
    10
}

impl Default for PriorityConfig {
    fn default() -> Self {
        Self {
            auto_adjust_enabled: true,
            adjust_interval_secs: 60,
            age_boost_factor: 0.01,
            completion_boost: 0.5,
            slow_speed_penalty: 0.3,
            slow_speed_threshold: 10240,
            inheritance_multiplier: 1.2,
            max_adjustments: 10,
            inheritance_enabled: true,
        }
    }
}

/// Queue status summary for visualization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStatus {
    /// Total number of tasks in queue
    pub total_tasks: usize,
    /// Tasks by priority level
    pub tasks_by_priority: HashMap<String, usize>,
    /// Estimated completion time for all tasks
    pub estimated_completion_secs: u64,
    /// Top 5 tasks in scheduling order
    pub next_tasks: Vec<QueueTaskInfo>,
    /// Average wait time in seconds
    pub avg_wait_secs: u64,
    /// Tasks with adjusted priority
    pub adjusted_count: usize,
}

/// Information about a task in the queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueTaskInfo {
    pub task_id: String,
    pub priority: Priority,
    pub progress: f64,
    pub current_speed: u64,
    pub wait_time_secs: u64,
    pub effective_score: f64,
}

/// Priority queue manager for download task scheduling.
pub struct PriorityQueue {
    /// All tasks in the queue
    tasks: Arc<RwLock<HashMap<String, PriorityTask>>>,
    /// Configuration
    config: Arc<RwLock<PriorityConfig>>,
    /// Last auto-adjustment timestamp
    last_adjust: Arc<RwLock<DateTime<Utc>>>,
}

impl PriorityQueue {
    /// Create a new priority queue.
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(PriorityConfig::default())),
            last_adjust: Arc::new(RwLock::new(Utc::now())),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(config: PriorityConfig) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(config)),
            last_adjust: Arc::new(RwLock::new(Utc::now())),
        }
    }

    /// Add a task to the queue.
    pub async fn add_task(&self, task_id: String, priority: Priority) -> Result<(), QueueError> {
        let mut tasks = self.tasks.write().await;
        if tasks.contains_key(&task_id) {
            return Err(QueueError::TaskAlreadyExists(task_id));
        }
        tasks.insert(task_id.clone(), PriorityTask::new(task_id, priority));
        Ok(())
    }

    /// Add a task with parent for priority inheritance.
    pub async fn add_task_with_parent(
        &self,
        task_id: String,
        priority: Priority,
        parent_id: String,
    ) -> Result<(), QueueError> {
        let config = self.config.read().await;
        if !config.inheritance_enabled {
            return self.add_task(task_id, priority).await;
        }
        drop(config);

        let mut tasks = self.tasks.write().await;
        if tasks.contains_key(&task_id) {
            return Err(QueueError::TaskAlreadyExists(task_id));
        }

        // Check if parent exists and get its priority
        let effective_priority = if let Some(parent) = tasks.get(&parent_id) {
            // Inherit parent's priority if higher
            if parent.priority > priority {
                parent.priority
            } else {
                priority
            }
        } else {
            priority
        };

        let task = PriorityTask::new(task_id.clone(), effective_priority).with_parent(parent_id);
        tasks.insert(task_id, task);
        Ok(())
    }

    /// Remove a task from the queue.
    pub async fn remove_task(&self, task_id: &str) -> Result<(), QueueError> {
        let mut tasks = self.tasks.write().await;
        if tasks.remove(task_id).is_none() {
            return Err(QueueError::TaskNotFound(task_id.to_string()));
        }
        Ok(())
    }

    /// Update task priority.
    pub async fn update_priority(
        &self,
        task_id: &str,
        priority: Priority,
    ) -> Result<(), QueueError> {
        let mut tasks = self.tasks.write().await;
        let task = tasks
            .get_mut(task_id)
            .ok_or_else(|| QueueError::TaskNotFound(task_id.to_string()))?;

        task.priority = priority;
        task.priority_score = priority.weight();
        task.last_updated = Utc::now();
        task.adjustment_count += 1;
        Ok(())
    }

    /// Update task metrics (progress and speed).
    pub async fn update_task_metrics(
        &self,
        task_id: &str,
        progress: f64,
        speed: u64,
    ) -> Result<(), QueueError> {
        let mut tasks = self.tasks.write().await;
        let task = tasks
            .get_mut(task_id)
            .ok_or_else(|| QueueError::TaskNotFound(task_id.to_string()))?;
        task.update_metrics(progress, speed);
        Ok(())
    }

    /// Get the next task to execute based on priority.
    pub async fn get_next_task(&self) -> Option<String> {
        let tasks = self.tasks.read().await;
        let config = self.config.read().await;

        tasks
            .values()
            .max_by(|a, b| {
                let score_a = a.calculate_effective_score(&config);
                let score_b = b.calculate_effective_score(&config);
                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|t| t.task_id.clone())
    }

    /// Get all tasks in scheduling order.
    pub async fn get_scheduled_order(&self) -> Vec<String> {
        let tasks = self.tasks.read().await;
        let config = self.config.read().await;

        let mut task_list: Vec<_> = tasks.values().cloned().collect();
        task_list.sort_by(|a, b| {
            let score_a = a.calculate_effective_score(&config);
            let score_b = b.calculate_effective_score(&config);
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        task_list.into_iter().map(|t| t.task_id).collect()
    }

    /// Get queue status summary.
    pub async fn get_queue_status(&self) -> QueueStatus {
        let tasks = self.tasks.read().await;
        let config = self.config.read().await;

        let mut tasks_by_priority: HashMap<String, usize> = HashMap::new();
        let mut total_wait_secs: u64 = 0;
        let mut adjusted_count = 0;
        let mut task_infos: Vec<QueueTaskInfo> = Vec::new();

        let now = Utc::now();

        for task in tasks.values() {
            // Count by priority
            *tasks_by_priority
                .entry(task.priority.label().to_string())
                .or_insert(0) += 1;

            // Calculate wait time
            let wait_secs = (now - task.added_at).num_seconds() as u64;
            total_wait_secs += wait_secs;

            // Count adjusted tasks
            if task.priority != task.original_priority {
                adjusted_count += 1;
            }

            // Collect task info
            let score = task.calculate_effective_score(&config);
            task_infos.push(QueueTaskInfo {
                task_id: task.task_id.clone(),
                priority: task.priority,
                progress: task.progress,
                current_speed: task.current_speed,
                wait_time_secs: wait_secs,
                effective_score: score,
            });
        }

        // Sort by score and take top 5
        task_infos.sort_by(|a, b| {
            b.effective_score
                .partial_cmp(&a.effective_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let next_tasks = task_infos.into_iter().take(5).collect();

        let total_tasks = tasks.len();
        let avg_wait_secs = if total_tasks > 0 {
            total_wait_secs / total_tasks as u64
        } else {
            0
        };

        // Estimate completion time (rough calculation)
        let estimated_completion_secs = self.estimate_completion_time(&tasks).await;

        QueueStatus {
            total_tasks,
            tasks_by_priority,
            estimated_completion_secs,
            next_tasks,
            avg_wait_secs,
            adjusted_count,
        }
    }

    /// Estimate total completion time for all tasks.
    async fn estimate_completion_time(&self, tasks: &HashMap<String, PriorityTask>) -> u64 {
        let mut total_secs: u64 = 0;

        for task in tasks.values() {
            if task.progress >= 1.0 {
                continue;
            }

            // Assume average file size of 100MB for estimation
            let assumed_size: u64 = 100 * 1024 * 1024;
            let remaining = (assumed_size as f64 * (1.0 - task.progress)) as u64;

            if task.current_speed > 0 {
                total_secs += remaining / task.current_speed;
            } else {
                // Assume minimum 100 KB/s for tasks without speed data
                total_secs += remaining / 102400;
            }
        }

        total_secs
    }

    /// Automatically adjust priorities based on performance metrics.
    pub async fn auto_adjust_priorities(&self) -> Result<usize, QueueError> {
        let config = self.config.read().await;
        if !config.auto_adjust_enabled {
            return Ok(0);
        }

        let mut last_adjust = self.last_adjust.write().await;
        let elapsed = (Utc::now() - *last_adjust).num_seconds() as u64;
        if elapsed < config.adjust_interval_secs {
            return Ok(0);
        }

        let mut tasks = self.tasks.write().await;
        let mut adjusted = 0;

        for task in tasks.values_mut() {
            if task.adjustment_count >= config.max_adjustments {
                continue;
            }

            let old_priority = task.priority;
            let mut new_priority = old_priority;

            // Boost near-complete tasks
            if task.progress > 0.9 && task.priority < Priority::High {
                new_priority = Priority::High;
            }

            // Penalize very slow tasks
            if task.current_speed > 0
                && task.current_speed < config.slow_speed_threshold
                && task.priority > Priority::Low
            {
                new_priority = match task.priority {
                    Priority::Urgent => Priority::High,
                    Priority::High => Priority::Normal,
                    Priority::Normal => Priority::Low,
                    Priority::Low => Priority::Low,
                };
            }

            // Boost long-waiting tasks
            let wait_hours = (Utc::now() - task.added_at).num_hours();
            if wait_hours > 24 && task.priority < Priority::High {
                new_priority = Priority::High;
            }

            if new_priority != old_priority {
                task.priority = new_priority;
                task.priority_score = new_priority.weight();
                task.adjustment_count += 1;
                adjusted += 1;
            }
        }

        *last_adjust = Utc::now();
        Ok(adjusted)
    }

    /// Get configuration.
    pub async fn get_config(&self) -> PriorityConfig {
        self.config.read().await.clone()
    }

    /// Update configuration.
    pub async fn update_config(&self, config: PriorityConfig) {
        *self.config.write().await = config;
    }

    /// Get task by ID.
    pub async fn get_task(&self, task_id: &str) -> Option<PriorityTask> {
        self.tasks.read().await.get(task_id).cloned()
    }

    /// Check if queue contains task.
    pub async fn contains(&self, task_id: &str) -> bool {
        self.tasks.read().await.contains_key(task_id)
    }

    /// Get queue size.
    pub async fn len(&self) -> usize {
        self.tasks.read().await.len()
    }

    /// Check if queue is empty.
    pub async fn is_empty(&self) -> bool {
        self.tasks.read().await.is_empty()
    }

    /// Clear all tasks from queue.
    pub async fn clear(&self) {
        self.tasks.write().await.clear();
    }

    /// Get tasks by priority level.
    pub async fn get_tasks_by_priority(&self, priority: Priority) -> Vec<String> {
        self.tasks
            .read()
            .await
            .values()
            .filter(|t| t.priority == priority)
            .map(|t| t.task_id.clone())
            .collect()
    }

    /// Calculate bandwidth allocation weights for all tasks.
    pub async fn calculate_bandwidth_weights(&self) -> HashMap<String, f64> {
        let tasks = self.tasks.read().await;
        let config = self.config.read().await;

        let mut weights: HashMap<String, f64> = HashMap::new();
        let total_weight: f64 = tasks
            .values()
            .map(|t| t.calculate_effective_score(&config))
            .sum();

        if total_weight > 0.0 {
            for task in tasks.values() {
                let score = task.calculate_effective_score(&config);
                weights.insert(task.task_id.clone(), score / total_weight);
            }
        }

        weights
    }
}

impl Default for PriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors from priority queue operations.
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("task already exists: {0}")]
    TaskAlreadyExists(String),
    #[error("task not found: {0}")]
    TaskNotFound(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Priority enum: ordering ==========

    #[tokio::test]
    async fn test_priority_ordering() {
        assert!(Priority::Urgent > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
    }

    #[tokio::test]
    async fn test_priority_ordering_reflexive() {
        assert_eq!(Priority::Low.cmp(&Priority::Low), std::cmp::Ordering::Equal);
        assert_eq!(
            Priority::Normal.cmp(&Priority::Normal),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            Priority::High.cmp(&Priority::High),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            Priority::Urgent.cmp(&Priority::Urgent),
            std::cmp::Ordering::Equal
        );
    }

    // ========== Priority: from_str_opt ==========

    #[tokio::test]
    async fn test_priority_from_str() {
        assert_eq!(Priority::from_str_opt("low"), Some(Priority::Low));
        assert_eq!(Priority::from_str_opt("HIGH"), Some(Priority::High));
        assert_eq!(Priority::from_str_opt("urgent"), Some(Priority::Urgent));
        assert_eq!(Priority::from_str_opt("normal"), Some(Priority::Normal));
        assert_eq!(Priority::from_str_opt("invalid"), None);
    }

    #[tokio::test]
    async fn test_priority_from_str_all_aliases() {
        // Low aliases
        assert_eq!(Priority::from_str_opt("l"), Some(Priority::Low));
        assert_eq!(Priority::from_str_opt("0"), Some(Priority::Low));
        // Normal aliases
        assert_eq!(Priority::from_str_opt("n"), Some(Priority::Normal));
        assert_eq!(Priority::from_str_opt("1"), Some(Priority::Normal));
        assert_eq!(Priority::from_str_opt("default"), Some(Priority::Normal));
        // High aliases
        assert_eq!(Priority::from_str_opt("h"), Some(Priority::High));
        assert_eq!(Priority::from_str_opt("2"), Some(Priority::High));
        // Urgent aliases
        assert_eq!(Priority::from_str_opt("u"), Some(Priority::Urgent));
        assert_eq!(Priority::from_str_opt("3"), Some(Priority::Urgent));
        assert_eq!(Priority::from_str_opt("critical"), Some(Priority::Urgent));
    }

    #[tokio::test]
    async fn test_priority_from_str_case_insensitive() {
        assert_eq!(Priority::from_str_opt("LOW"), Some(Priority::Low));
        assert_eq!(Priority::from_str_opt("Low"), Some(Priority::Low));
        assert_eq!(Priority::from_str_opt("NORMAL"), Some(Priority::Normal));
        assert_eq!(Priority::from_str_opt("URGENT"), Some(Priority::Urgent));
        assert_eq!(Priority::from_str_opt("High"), Some(Priority::High));
    }

    #[tokio::test]
    async fn test_priority_from_str_empty_whitespace() {
        assert_eq!(Priority::from_str_opt(""), None);
        assert_eq!(Priority::from_str_opt(" "), None);
        assert_eq!(Priority::from_str_opt("\t"), None);
        assert_eq!(Priority::from_str_opt("\n"), None);
    }

    #[tokio::test]
    async fn test_priority_from_str_unicode() {
        assert_eq!(Priority::from_str_opt("中文"), None);
        assert_eq!(Priority::from_str_opt("🔥"), None);
        assert_eq!(Priority::from_str_opt("low🔥"), None);
    }

    // ========== Priority: label ==========

    #[tokio::test]
    async fn test_priority_label_all_variants() {
        assert_eq!(Priority::Low.label(), "low");
        assert_eq!(Priority::Normal.label(), "normal");
        assert_eq!(Priority::High.label(), "high");
        assert_eq!(Priority::Urgent.label(), "urgent");
    }

    // ========== Priority: weight ==========

    #[tokio::test]
    async fn test_priority_weight() {
        assert!(Priority::Urgent.weight() > Priority::High.weight());
        assert!(Priority::High.weight() > Priority::Normal.weight());
        assert!(Priority::Normal.weight() > Priority::Low.weight());
    }

    #[tokio::test]
    async fn test_priority_weight_values() {
        assert_eq!(Priority::Low.weight(), 0.5);
        assert_eq!(Priority::Normal.weight(), 1.0);
        assert_eq!(Priority::High.weight(), 2.0);
        assert_eq!(Priority::Urgent.weight(), 4.0);
    }

    // ========== Priority: serde ==========

    #[tokio::test]
    async fn test_priority_serde_roundtrip_all_variants() {
        for p in [
            Priority::Low,
            Priority::Normal,
            Priority::High,
            Priority::Urgent,
        ] {
            let json = serde_json::to_string(&p).unwrap();
            let deserialized: Priority = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, p);
        }
    }

    #[tokio::test]
    async fn test_priority_serde_snake_case_values() {
        assert_eq!(serde_json::to_string(&Priority::Low).unwrap(), "\"low\"");
        assert_eq!(
            serde_json::to_string(&Priority::Normal).unwrap(),
            "\"normal\""
        );
        assert_eq!(serde_json::to_string(&Priority::High).unwrap(), "\"high\"");
        assert_eq!(
            serde_json::to_string(&Priority::Urgent).unwrap(),
            "\"urgent\""
        );
    }

    // ========== Priority: traits ==========

    #[tokio::test]
    async fn test_priority_clone_copy() {
        let p = Priority::High;
        let p2 = p; // Copy
        let p3 = p.clone();
        assert_eq!(p, p2);
        assert_eq!(p, p3);
    }

    #[tokio::test]
    async fn test_priority_debug() {
        assert_eq!(format!("{:?}", Priority::Low), "Low");
        assert_eq!(format!("{:?}", Priority::Normal), "Normal");
        assert_eq!(format!("{:?}", Priority::High), "High");
        assert_eq!(format!("{:?}", Priority::Urgent), "Urgent");
    }

    #[tokio::test]
    async fn test_priority_eq_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Priority::Low);
        set.insert(Priority::Low); // duplicate
        set.insert(Priority::High);
        assert_eq!(set.len(), 2);
    }

    #[tokio::test]
    async fn test_priority_default() {
        assert_eq!(Priority::default(), Priority::Normal);
    }

    // ========== Priority: from/to_download_priority ==========

    #[tokio::test]
    async fn test_priority_conversion() {
        assert_eq!(
            Priority::from_download_priority(crate::DownloadPriority::Low),
            Priority::Low
        );
        assert_eq!(
            Priority::from_download_priority(crate::DownloadPriority::Normal),
            Priority::Normal
        );
        assert_eq!(
            Priority::from_download_priority(crate::DownloadPriority::High),
            Priority::High
        );

        assert_eq!(
            Priority::Low.to_download_priority(),
            crate::DownloadPriority::Low
        );
        assert_eq!(
            Priority::Normal.to_download_priority(),
            crate::DownloadPriority::Normal
        );
        assert_eq!(
            Priority::High.to_download_priority(),
            crate::DownloadPriority::High
        );
        assert_eq!(
            Priority::Urgent.to_download_priority(),
            crate::DownloadPriority::High
        );
    }

    // ========== PriorityTask: new ==========

    #[tokio::test]
    async fn test_create_priority_task() {
        let task = PriorityTask::new("task1".to_string(), Priority::High);
        assert_eq!(task.task_id, "task1");
        assert_eq!(task.priority, Priority::High);
        assert_eq!(task.original_priority, Priority::High);
        assert_eq!(task.adjustment_count, 0);
    }

    #[tokio::test]
    async fn test_priority_task_new_defaults() {
        let task = PriorityTask::new("t1".to_string(), Priority::Normal);
        assert_eq!(task.progress, 0.0);
        assert_eq!(task.current_speed, 0);
        assert_eq!(task.parent_task_id, None);
        assert_eq!(task.priority_score, Priority::Normal.weight());
        assert!(task.added_at <= Utc::now());
        assert!(task.last_updated <= Utc::now());
    }

    #[tokio::test]
    async fn test_priority_task_new_all_priorities() {
        for p in [
            Priority::Low,
            Priority::Normal,
            Priority::High,
            Priority::Urgent,
        ] {
            let task = PriorityTask::new(format!("task_{:?}", p), p);
            assert_eq!(task.priority, p);
            assert_eq!(task.original_priority, p);
            assert_eq!(task.priority_score, p.weight());
        }
    }

    // ========== PriorityTask: with_parent ==========

    #[tokio::test]
    async fn test_task_with_parent() {
        let task = PriorityTask::new("child".to_string(), Priority::Normal)
            .with_parent("parent".to_string());
        assert_eq!(task.parent_task_id, Some("parent".to_string()));
    }

    #[tokio::test]
    async fn test_task_with_parent_unicode() {
        let task = PriorityTask::new("子任务".to_string(), Priority::Normal)
            .with_parent("父任务".to_string());
        assert_eq!(task.parent_task_id, Some("父任务".to_string()));
    }

    #[tokio::test]
    async fn test_task_with_parent_builder_chain() {
        let task =
            PriorityTask::new("child".to_string(), Priority::Low).with_parent("parent".to_string());
        assert_eq!(task.task_id, "child");
        assert_eq!(task.priority, Priority::Low);
        assert_eq!(task.parent_task_id, Some("parent".to_string()));
    }

    // ========== PriorityTask: update_metrics ==========

    #[tokio::test]
    async fn test_update_task_metrics() {
        let mut task = PriorityTask::new("task1".to_string(), Priority::Normal);
        task.update_metrics(0.5, 102400);
        assert_eq!(task.progress, 0.5);
        assert_eq!(task.current_speed, 102400);
    }

    #[tokio::test]
    async fn test_progress_clamping() {
        let mut task = PriorityTask::new("task1".to_string(), Priority::Normal);
        task.update_metrics(1.5, 100);
        assert_eq!(task.progress, 1.0);
        task.update_metrics(-0.5, 100);
        assert_eq!(task.progress, 0.0);
    }

    #[tokio::test]
    async fn test_progress_clamping_exact_boundaries() {
        let mut task = PriorityTask::new("t".to_string(), Priority::Normal);
        task.update_metrics(0.0, 0);
        assert_eq!(task.progress, 0.0);
        task.update_metrics(1.0, 0);
        assert_eq!(task.progress, 1.0);
    }

    #[tokio::test]
    async fn test_update_metrics_speed_zero() {
        let mut task = PriorityTask::new("t".to_string(), Priority::Normal);
        task.update_metrics(0.5, 0);
        assert_eq!(task.current_speed, 0);
    }

    #[tokio::test]
    async fn test_update_metrics_large_speed() {
        let mut task = PriorityTask::new("t".to_string(), Priority::Normal);
        task.update_metrics(0.5, u64::MAX);
        assert_eq!(task.current_speed, u64::MAX);
    }

    #[tokio::test]
    async fn test_update_metrics_updates_timestamp() {
        let mut task = PriorityTask::new("t".to_string(), Priority::Normal);
        let before = task.last_updated;
        task.update_metrics(0.5, 100);
        assert!(task.last_updated >= before);
    }

    // ========== PriorityTask: calculate_effective_score ==========

    #[tokio::test]
    async fn test_effective_score_basic() {
        let config = PriorityConfig::default();
        let mut task = PriorityTask::new("task1".to_string(), Priority::High);
        task.update_metrics(0.5, 100000); // Above slow threshold
        let score = task.calculate_effective_score(&config);
        // Score should be at least the base weight (no penalty, no boost)
        assert!(score >= Priority::High.weight());
    }

    #[tokio::test]
    async fn test_effective_score_completion_boost() {
        let config = PriorityConfig::default();
        let mut task = PriorityTask::new("task1".to_string(), Priority::High);
        task.update_metrics(0.95, 100000); // Near complete
        let score = task.calculate_effective_score(&config);
        assert!(score > Priority::High.weight()); // Should have completion boost
    }

    #[tokio::test]
    async fn test_effective_score_no_boost_below_90_percent() {
        let config = PriorityConfig::default();
        let mut task = PriorityTask::new("task1".to_string(), Priority::Normal);
        task.update_metrics(0.89, 100000);
        let score = task.calculate_effective_score(&config);
        // No completion boost at 89%
        let base = Priority::Normal.weight();
        assert!(score < base + config.completion_boost + 0.01);
    }

    #[tokio::test]
    async fn test_effective_score_slow_speed_penalty() {
        let config = PriorityConfig {
            slow_speed_threshold: 100000,
            slow_speed_penalty: 0.5,
            ..Default::default()
        };
        let mut task = PriorityTask::new("t".to_string(), Priority::Normal);
        task.update_metrics(0.5, 100); // Very slow
        let score = task.calculate_effective_score(&config);
        // Should have penalty applied
        assert!(score < Priority::Normal.weight());
    }

    #[tokio::test]
    async fn test_effective_score_no_penalty_at_threshold() {
        let config = PriorityConfig {
            slow_speed_threshold: 10000,
            slow_speed_penalty: 0.5,
            ..Default::default()
        };
        let mut task = PriorityTask::new("t".to_string(), Priority::Normal);
        task.update_metrics(0.5, 10000); // At threshold, not below
        let score = task.calculate_effective_score(&config);
        // Speed == threshold, not < threshold, so no penalty
        let base = Priority::Normal.weight();
        assert!((score - base).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_effective_score_inheritance_multiplier() {
        let config = PriorityConfig {
            inheritance_multiplier: 2.0,
            ..Default::default()
        };
        let task_no_parent = PriorityTask::new("a".to_string(), Priority::Normal);
        let task_with_parent =
            PriorityTask::new("b".to_string(), Priority::Normal).with_parent("p".to_string());

        let score_no = task_no_parent.calculate_effective_score(&config);
        let score_with = task_with_parent.calculate_effective_score(&config);
        assert!(score_with > score_no);
    }

    #[tokio::test]
    async fn test_effective_score_minimum_0_1() {
        let config = PriorityConfig {
            slow_speed_penalty: 100.0, // Huge penalty
            ..Default::default()
        };
        let mut task = PriorityTask::new("t".to_string(), Priority::Low);
        task.update_metrics(0.0, 0);
        let score = task.calculate_effective_score(&config);
        assert!(score >= 0.1);
    }

    // ========== PriorityConfig ==========

    #[tokio::test]
    async fn test_config_default_values() {
        let config = PriorityConfig::default();
        assert!(config.auto_adjust_enabled);
        assert_eq!(config.adjust_interval_secs, 60);
        assert_eq!(config.age_boost_factor, 0.01);
        assert_eq!(config.completion_boost, 0.5);
        assert_eq!(config.slow_speed_penalty, 0.3);
        assert_eq!(config.slow_speed_threshold, 10240);
        assert_eq!(config.inheritance_multiplier, 1.2);
        assert_eq!(config.max_adjustments, 10);
        assert!(config.inheritance_enabled);
    }

    #[tokio::test]
    async fn test_config_serde_roundtrip() {
        let config = PriorityConfig {
            auto_adjust_enabled: false,
            adjust_interval_secs: 120,
            age_boost_factor: 0.05,
            completion_boost: 1.0,
            slow_speed_penalty: 0.5,
            slow_speed_threshold: 20480,
            inheritance_multiplier: 1.5,
            max_adjustments: 5,
            inheritance_enabled: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PriorityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.auto_adjust_enabled, config.auto_adjust_enabled);
        assert_eq!(
            deserialized.adjust_interval_secs,
            config.adjust_interval_secs
        );
        assert_eq!(deserialized.age_boost_factor, config.age_boost_factor);
        assert_eq!(deserialized.completion_boost, config.completion_boost);
        assert_eq!(deserialized.slow_speed_penalty, config.slow_speed_penalty);
        assert_eq!(
            deserialized.slow_speed_threshold,
            config.slow_speed_threshold
        );
        assert_eq!(
            deserialized.inheritance_multiplier,
            config.inheritance_multiplier
        );
        assert_eq!(deserialized.max_adjustments, config.max_adjustments);
        assert_eq!(deserialized.inheritance_enabled, config.inheritance_enabled);
    }

    #[tokio::test]
    async fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"auto_adjust_enabled":true,"unknown_field":42,"another":"test"}"#;
        let config: PriorityConfig = serde_json::from_str(json).unwrap();
        assert!(config.auto_adjust_enabled);
    }

    #[tokio::test]
    async fn test_config_serde_pretty() {
        let config = PriorityConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let deserialized: PriorityConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.auto_adjust_enabled, config.auto_adjust_enabled);
        assert_eq!(
            deserialized.adjust_interval_secs,
            config.adjust_interval_secs
        );
    }

    #[tokio::test]
    async fn test_config_clone_debug() {
        let config = PriorityConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.auto_adjust_enabled, config.auto_adjust_enabled);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("PriorityConfig"));
    }

    // ========== QueueStatus / QueueTaskInfo ==========

    #[tokio::test]
    async fn test_queue_status_serde_clone() {
        let status = QueueStatus {
            total_tasks: 5,
            tasks_by_priority: HashMap::new(),
            estimated_completion_secs: 3600,
            next_tasks: vec![],
            avg_wait_secs: 100,
            adjusted_count: 2,
        };
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: QueueStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_tasks, 5);
        assert_eq!(deserialized.estimated_completion_secs, 3600);

        let cloned = status.clone();
        assert_eq!(cloned.total_tasks, status.total_tasks);
    }

    #[tokio::test]
    async fn test_queue_task_info_serde_clone() {
        let info = QueueTaskInfo {
            task_id: "task1".to_string(),
            priority: Priority::High,
            progress: 0.5,
            current_speed: 102400,
            wait_time_secs: 60,
            effective_score: 2.5,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: QueueTaskInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task1");
        assert_eq!(deserialized.priority, Priority::High);

        let cloned = info.clone();
        assert_eq!(cloned.progress, info.progress);
    }

    #[tokio::test]
    async fn test_queue_status_debug() {
        let status = QueueStatus {
            total_tasks: 0,
            tasks_by_priority: HashMap::new(),
            estimated_completion_secs: 0,
            next_tasks: vec![],
            avg_wait_secs: 0,
            adjusted_count: 0,
        };
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("QueueStatus"));
    }

    #[tokio::test]
    async fn test_queue_task_info_debug() {
        let info = QueueTaskInfo {
            task_id: "t".to_string(),
            priority: Priority::Low,
            progress: 0.0,
            current_speed: 0,
            wait_time_secs: 0,
            effective_score: 0.5,
        };
        let debug_str = format!("{:?}", info);
        assert!(debug_str.contains("QueueTaskInfo"));
    }

    // ========== QueueError ==========

    #[tokio::test]
    async fn test_queue_error_display_all_variants() {
        let e1 = QueueError::TaskAlreadyExists("task1".to_string());
        assert_eq!(format!("{}", e1), "task already exists: task1");

        let e2 = QueueError::TaskNotFound("task2".to_string());
        assert_eq!(format!("{}", e2), "task not found: task2");

        let e3 = QueueError::InvalidConfig("bad config".to_string());
        assert_eq!(format!("{}", e3), "invalid configuration: bad config");
    }

    #[tokio::test]
    async fn test_queue_error_debug() {
        let e = QueueError::TaskNotFound("t".to_string());
        let debug = format!("{:?}", e);
        assert!(debug.contains("TaskNotFound"));
    }

    #[tokio::test]
    async fn test_queue_error_unicode() {
        let e = QueueError::TaskAlreadyExists("中文任务".to_string());
        assert!(format!("{}", e).contains("中文任务"));

        let e2 = QueueError::TaskNotFound("🔥task".to_string());
        assert!(format!("{}", e2).contains("🔥task"));
    }

    #[tokio::test]
    async fn test_queue_error_is_error_trait() {
        let e: Box<dyn std::error::Error> = Box::new(QueueError::TaskNotFound("t".to_string()));
        assert!(e.to_string().contains("task not found"));
    }

    // ========== PriorityQueue: new/default/with_config ==========

    #[tokio::test]
    async fn test_queue_new_default_equality() {
        let q1 = PriorityQueue::new();
        let q2 = PriorityQueue::default();
        assert_eq!(q1.len().await, q2.len().await);
        assert_eq!(q1.is_empty().await, q2.is_empty().await);
    }

    #[tokio::test]
    async fn test_queue_with_config() {
        let config = PriorityConfig {
            auto_adjust_enabled: false,
            adjust_interval_secs: 300,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config.clone());
        let loaded = queue.get_config().await;
        assert!(!loaded.auto_adjust_enabled);
        assert_eq!(loaded.adjust_interval_secs, 300);
    }

    // ========== PriorityQueue: add_task ==========

    #[tokio::test]
    async fn test_queue_add_task() {
        let queue = PriorityQueue::new();
        queue
            .add_task("task1".to_string(), Priority::Normal)
            .await
            .unwrap();
        assert!(queue.contains("task1").await);
        assert_eq!(queue.len().await, 1);
    }

    #[tokio::test]
    async fn test_queue_add_duplicate() {
        let queue = PriorityQueue::new();
        queue
            .add_task("task1".to_string(), Priority::Normal)
            .await
            .unwrap();
        let result = queue.add_task("task1".to_string(), Priority::High).await;
        assert!(matches!(result, Err(QueueError::TaskAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_queue_add_unicode_task_id() {
        let queue = PriorityQueue::new();
        queue
            .add_task("中文任务".to_string(), Priority::High)
            .await
            .unwrap();
        assert!(queue.contains("中文任务").await);
    }

    #[tokio::test]
    async fn test_queue_add_emoji_task_id() {
        let queue = PriorityQueue::new();
        queue
            .add_task("🔥download".to_string(), Priority::Normal)
            .await
            .unwrap();
        assert!(queue.contains("🔥download").await);
    }

    #[tokio::test]
    async fn test_queue_add_multiple_tasks() {
        let queue = PriorityQueue::new();
        for i in 0..50 {
            queue
                .add_task(format!("task_{}", i), Priority::Normal)
                .await
                .unwrap();
        }
        assert_eq!(queue.len().await, 50);
    }

    // ========== PriorityQueue: add_task_with_parent ==========

    #[tokio::test]
    async fn test_priority_inheritance() {
        let queue = PriorityQueue::new();
        queue
            .add_task("parent".to_string(), Priority::Urgent)
            .await
            .unwrap();
        queue
            .add_task_with_parent("child".to_string(), Priority::Low, "parent".to_string())
            .await
            .unwrap();

        let child = queue.get_task("child").await.unwrap();
        assert_eq!(child.priority, Priority::Urgent); // Inherited from parent
        assert_eq!(child.parent_task_id, Some("parent".to_string()));
    }

    #[tokio::test]
    async fn test_priority_inheritance_lower_parent() {
        let queue = PriorityQueue::new();
        queue
            .add_task("parent".to_string(), Priority::Low)
            .await
            .unwrap();
        queue
            .add_task_with_parent("child".to_string(), Priority::High, "parent".to_string())
            .await
            .unwrap();

        let child = queue.get_task("child").await.unwrap();
        assert_eq!(child.priority, Priority::High); // Keeps own higher priority
    }

    #[tokio::test]
    async fn test_priority_inheritance_parent_not_found() {
        let queue = PriorityQueue::new();
        queue
            .add_task_with_parent(
                "child".to_string(),
                Priority::Normal,
                "nonexistent".to_string(),
            )
            .await
            .unwrap();

        let child = queue.get_task("child").await.unwrap();
        assert_eq!(child.priority, Priority::Normal); // Keeps own priority
        assert_eq!(child.parent_task_id, Some("nonexistent".to_string()));
    }

    #[tokio::test]
    async fn test_priority_inheritance_disabled() {
        let config = PriorityConfig {
            inheritance_enabled: false,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);
        queue
            .add_task("parent".to_string(), Priority::Urgent)
            .await
            .unwrap();
        queue
            .add_task_with_parent("child".to_string(), Priority::Low, "parent".to_string())
            .await
            .unwrap();

        let child = queue.get_task("child").await.unwrap();
        assert_eq!(child.priority, Priority::Low); // No inheritance
    }

    #[tokio::test]
    async fn test_add_task_with_parent_duplicate() {
        let queue = PriorityQueue::new();
        queue
            .add_task("parent".to_string(), Priority::High)
            .await
            .unwrap();
        queue
            .add_task("child".to_string(), Priority::Normal)
            .await
            .unwrap();
        let result = queue
            .add_task_with_parent("child".to_string(), Priority::Low, "parent".to_string())
            .await;
        assert!(matches!(result, Err(QueueError::TaskAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_add_task_with_parent_equal_priority() {
        let queue = PriorityQueue::new();
        queue
            .add_task("parent".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task_with_parent("child".to_string(), Priority::Normal, "parent".to_string())
            .await
            .unwrap();

        let child = queue.get_task("child").await.unwrap();
        assert_eq!(child.priority, Priority::Normal); // Same as parent
    }

    // ========== PriorityQueue: remove_task ==========

    #[tokio::test]
    async fn test_queue_remove_task() {
        let queue = PriorityQueue::new();
        queue
            .add_task("task1".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue.remove_task("task1").await.unwrap();
        assert!(!queue.contains("task1").await);
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn test_queue_remove_nonexistent() {
        let queue = PriorityQueue::new();
        let result = queue.remove_task("nonexistent").await;
        assert!(matches!(result, Err(QueueError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn test_queue_remove_idempotent_after_removal() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue.remove_task("t").await.unwrap();
        let result = queue.remove_task("t").await;
        assert!(matches!(result, Err(QueueError::TaskNotFound(_))));
    }

    // ========== PriorityQueue: update_priority ==========

    #[tokio::test]
    async fn test_queue_update_priority() {
        let queue = PriorityQueue::new();
        queue
            .add_task("task1".to_string(), Priority::Low)
            .await
            .unwrap();
        queue
            .update_priority("task1", Priority::High)
            .await
            .unwrap();
        let task = queue.get_task("task1").await.unwrap();
        assert_eq!(task.priority, Priority::High);
        assert_eq!(task.adjustment_count, 1);
    }

    #[tokio::test]
    async fn test_queue_update_priority_nonexistent() {
        let queue = PriorityQueue::new();
        let result = queue.update_priority("nope", Priority::High).await;
        assert!(matches!(result, Err(QueueError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn test_queue_update_priority_increments_count() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Low)
            .await
            .unwrap();
        queue.update_priority("t", Priority::Normal).await.unwrap();
        queue.update_priority("t", Priority::High).await.unwrap();
        queue.update_priority("t", Priority::Urgent).await.unwrap();
        let task = queue.get_task("t").await.unwrap();
        assert_eq!(task.adjustment_count, 3);
    }

    #[tokio::test]
    async fn test_queue_update_priority_score_updated() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Low)
            .await
            .unwrap();
        queue.update_priority("t", Priority::Urgent).await.unwrap();
        let task = queue.get_task("t").await.unwrap();
        assert_eq!(task.priority_score, Priority::Urgent.weight());
    }

    // ========== PriorityQueue: update_task_metrics ==========

    #[tokio::test]
    async fn test_queue_update_task_metrics() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue.update_task_metrics("t", 0.75, 50000).await.unwrap();
        let task = queue.get_task("t").await.unwrap();
        assert_eq!(task.progress, 0.75);
        assert_eq!(task.current_speed, 50000);
    }

    #[tokio::test]
    async fn test_queue_update_task_metrics_nonexistent() {
        let queue = PriorityQueue::new();
        let result = queue.update_task_metrics("nope", 0.5, 100).await;
        assert!(matches!(result, Err(QueueError::TaskNotFound(_))));
    }

    // ========== PriorityQueue: get_next_task ==========

    #[tokio::test]
    async fn test_queue_get_next_task() {
        let queue = PriorityQueue::new();
        queue
            .add_task("low".to_string(), Priority::Low)
            .await
            .unwrap();
        queue
            .add_task("high".to_string(), Priority::High)
            .await
            .unwrap();
        queue
            .add_task("normal".to_string(), Priority::Normal)
            .await
            .unwrap();

        let next = queue.get_next_task().await.unwrap();
        assert_eq!(next, "high");
    }

    #[tokio::test]
    async fn test_empty_queue_next_task() {
        let queue = PriorityQueue::new();
        let next = queue.get_next_task().await;
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn test_single_task_next() {
        let queue = PriorityQueue::new();
        queue
            .add_task("only".to_string(), Priority::Low)
            .await
            .unwrap();
        assert_eq!(queue.get_next_task().await.unwrap(), "only");
    }

    #[tokio::test]
    async fn test_urgent_task_always_first() {
        let queue = PriorityQueue::new();
        queue
            .add_task("high".to_string(), Priority::High)
            .await
            .unwrap();
        queue
            .add_task("normal".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("urgent".to_string(), Priority::Urgent)
            .await
            .unwrap();
        queue
            .add_task("low".to_string(), Priority::Low)
            .await
            .unwrap();

        let next = queue.get_next_task().await.unwrap();
        assert_eq!(next, "urgent");
    }

    // ========== PriorityQueue: get_scheduled_order ==========

    #[tokio::test]
    async fn test_queue_scheduled_order() {
        let queue = PriorityQueue::new();
        queue
            .add_task("low".to_string(), Priority::Low)
            .await
            .unwrap();
        queue
            .add_task("urgent".to_string(), Priority::Urgent)
            .await
            .unwrap();
        queue
            .add_task("high".to_string(), Priority::High)
            .await
            .unwrap();

        let order = queue.get_scheduled_order().await;
        assert_eq!(order[0], "urgent");
        assert_eq!(order[1], "high");
        assert_eq!(order[2], "low");
    }

    #[tokio::test]
    async fn test_scheduled_order_empty() {
        let queue = PriorityQueue::new();
        let order = queue.get_scheduled_order().await;
        assert!(order.is_empty());
    }

    #[tokio::test]
    async fn test_scheduled_order_single() {
        let queue = PriorityQueue::new();
        queue
            .add_task("only".to_string(), Priority::Low)
            .await
            .unwrap();
        let order = queue.get_scheduled_order().await;
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], "only");
    }

    #[tokio::test]
    async fn test_scheduled_order_all_same_priority() {
        let queue = PriorityQueue::new();
        queue
            .add_task("a".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("b".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("c".to_string(), Priority::Normal)
            .await
            .unwrap();
        let order = queue.get_scheduled_order().await;
        assert_eq!(order.len(), 3);
    }

    // ========== PriorityQueue: get_queue_status ==========

    #[tokio::test]
    async fn test_queue_status() {
        let queue = PriorityQueue::new();
        queue
            .add_task("task1".to_string(), Priority::High)
            .await
            .unwrap();
        queue
            .add_task("task2".to_string(), Priority::Normal)
            .await
            .unwrap();

        let status = queue.get_queue_status().await;
        assert_eq!(status.total_tasks, 2);
        assert_eq!(status.tasks_by_priority.get("high"), Some(&1));
        assert_eq!(status.tasks_by_priority.get("normal"), Some(&1));
    }

    #[tokio::test]
    async fn test_empty_queue_status() {
        let queue = PriorityQueue::new();
        let status = queue.get_queue_status().await;
        assert_eq!(status.total_tasks, 0);
        assert!(status.next_tasks.is_empty());
        assert_eq!(status.avg_wait_secs, 0);
        assert_eq!(status.adjusted_count, 0);
    }

    #[tokio::test]
    async fn test_queue_status_adjusted_count() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t1".to_string(), Priority::Low)
            .await
            .unwrap();
        queue.update_priority("t1", Priority::High).await.unwrap();
        let status = queue.get_queue_status().await;
        assert_eq!(status.adjusted_count, 1);
    }

    #[tokio::test]
    async fn test_queue_status_next_tasks_max_5() {
        let queue = PriorityQueue::new();
        for i in 0..10 {
            queue
                .add_task(format!("t{}", i), Priority::Normal)
                .await
                .unwrap();
        }
        let status = queue.get_queue_status().await;
        assert!(status.next_tasks.len() <= 5);
    }

    // ========== PriorityQueue: estimate_completion_time ==========

    #[tokio::test]
    async fn test_estimate_completion_completed_task() {
        let queue = PriorityQueue::new();
        queue
            .add_task("done".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .update_task_metrics("done", 1.0, 100000)
            .await
            .unwrap();
        let status = queue.get_queue_status().await;
        // Completed tasks should not contribute
        assert_eq!(status.estimated_completion_secs, 0);
    }

    #[tokio::test]
    async fn test_estimate_completion_zero_speed() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        // Speed is 0, uses assumed 100KB/s
        let status = queue.get_queue_status().await;
        assert!(status.estimated_completion_secs > 0);
    }

    #[tokio::test]
    async fn test_estimate_completion_with_speed() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue.update_task_metrics("t", 0.5, 102400).await.unwrap();
        let status = queue.get_queue_status().await;
        assert!(status.estimated_completion_secs > 0);
    }

    // ========== PriorityQueue: auto_adjust_priorities ==========

    #[tokio::test]
    async fn test_auto_adjust_slow_task() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            slow_speed_threshold: 100000,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("slow".to_string(), Priority::High)
            .await
            .unwrap();
        queue.update_task_metrics("slow", 0.5, 1000).await.unwrap(); // Very slow

        let adjusted = queue.auto_adjust_priorities().await.unwrap();
        assert_eq!(adjusted, 1);

        let task = queue.get_task("slow").await.unwrap();
        assert_eq!(task.priority, Priority::Normal); // Demoted
    }

    #[tokio::test]
    async fn test_auto_adjust_near_complete() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("almost_done".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .update_task_metrics("almost_done", 0.95, 100000)
            .await
            .unwrap(); // 95% complete

        let adjusted = queue.auto_adjust_priorities().await.unwrap();
        assert_eq!(adjusted, 1);

        let task = queue.get_task("almost_done").await.unwrap();
        assert_eq!(task.priority, Priority::High); // Boosted
    }

    #[tokio::test]
    async fn test_disabled_auto_adjust() {
        let config = PriorityConfig {
            auto_adjust_enabled: false,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("task".to_string(), Priority::High)
            .await
            .unwrap();
        queue.update_task_metrics("task", 0.5, 1000).await.unwrap();

        let adjusted = queue.auto_adjust_priorities().await.unwrap();
        assert_eq!(adjusted, 0); // Should not adjust
    }

    #[tokio::test]
    async fn test_auto_adjust_interval_not_elapsed() {
        let config = PriorityConfig {
            adjust_interval_secs: 3600, // 1 hour
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue.update_task_metrics("t", 0.95, 100000).await.unwrap();

        // First call adjusts (elapsed since creation)
        let adjusted = queue.auto_adjust_priorities().await.unwrap();
        // Immediate second call should not adjust (interval not elapsed)
        let adjusted2 = queue.auto_adjust_priorities().await.unwrap();
        assert_eq!(adjusted2, 0);
    }

    #[tokio::test]
    async fn test_max_adjustments_limit() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            max_adjustments: 2,
            slow_speed_threshold: 100000,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("task".to_string(), Priority::High)
            .await
            .unwrap();
        queue.update_task_metrics("task", 0.5, 1000).await.unwrap();

        // First adjustment
        queue.auto_adjust_priorities().await.unwrap();
        // Second adjustment
        queue.auto_adjust_priorities().await.unwrap();
        // Third should be skipped
        queue.auto_adjust_priorities().await.unwrap();

        let task = queue.get_task("task").await.unwrap();
        assert_eq!(task.adjustment_count, 2); // Limited to max
    }

    #[tokio::test]
    async fn test_auto_adjust_slow_urgent_to_high() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            slow_speed_threshold: 100000,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("t".to_string(), Priority::Urgent)
            .await
            .unwrap();
        queue.update_task_metrics("t", 0.5, 100).await.unwrap();

        queue.auto_adjust_priorities().await.unwrap();
        let task = queue.get_task("t").await.unwrap();
        assert_eq!(task.priority, Priority::High); // Urgent → High
    }

    #[tokio::test]
    async fn test_auto_adjust_slow_normal_to_low() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            slow_speed_threshold: 100000,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue.update_task_metrics("t", 0.5, 100).await.unwrap();

        queue.auto_adjust_priorities().await.unwrap();
        let task = queue.get_task("t").await.unwrap();
        assert_eq!(task.priority, Priority::Low); // Normal → Low
    }

    #[tokio::test]
    async fn test_auto_adjust_slow_low_stays_low() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            slow_speed_threshold: 100000,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("t".to_string(), Priority::Low)
            .await
            .unwrap();
        queue.update_task_metrics("t", 0.5, 100).await.unwrap();

        queue.auto_adjust_priorities().await.unwrap();
        let task = queue.get_task("t").await.unwrap();
        assert_eq!(task.priority, Priority::Low); // Low stays Low
    }

    #[tokio::test]
    async fn test_auto_adjust_near_complete_already_high() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("t".to_string(), Priority::High)
            .await
            .unwrap();
        queue.update_task_metrics("t", 0.95, 100000).await.unwrap();

        let adjusted = queue.auto_adjust_priorities().await.unwrap();
        // Already High, condition is priority < High, so no adjustment
        assert_eq!(adjusted, 0);
    }

    #[tokio::test]
    async fn test_auto_adjust_zero_speed_not_penalized() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            slow_speed_threshold: 100000,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("t".to_string(), Priority::High)
            .await
            .unwrap();
        queue.update_task_metrics("t", 0.5, 0).await.unwrap(); // Zero speed

        queue.auto_adjust_priorities().await.unwrap();
        let task = queue.get_task("t").await.unwrap();
        // Zero speed: condition is speed > 0 && speed < threshold, so no penalty
        assert_eq!(task.priority, Priority::High);
    }

    #[tokio::test]
    async fn test_auto_adjust_long_waiting_boost() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        queue
            .add_task("old".to_string(), Priority::Normal)
            .await
            .unwrap();
        // Manually set old timestamp
        {
            let mut tasks = queue.tasks.write().await;
            if let Some(task) = tasks.get_mut("old") {
                task.added_at = Utc::now() - chrono::Duration::hours(48);
            }
        }

        let adjusted = queue.auto_adjust_priorities().await.unwrap();
        assert_eq!(adjusted, 1);
        let task = queue.get_task("old").await.unwrap();
        assert_eq!(task.priority, Priority::High);
    }

    #[tokio::test]
    async fn test_auto_adjust_empty_queue() {
        let config = PriorityConfig {
            adjust_interval_secs: 0,
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);
        let adjusted = queue.auto_adjust_priorities().await.unwrap();
        assert_eq!(adjusted, 0);
    }

    // ========== PriorityQueue: get_config/update_config ==========

    #[tokio::test]
    async fn test_config_update() {
        let queue = PriorityQueue::new();
        let new_config = PriorityConfig {
            auto_adjust_enabled: false,
            ..Default::default()
        };
        queue.update_config(new_config.clone()).await;
        let config = queue.get_config().await;
        assert!(!config.auto_adjust_enabled);
    }

    #[tokio::test]
    async fn test_config_update_all_fields() {
        let queue = PriorityQueue::new();
        let new_config = PriorityConfig {
            auto_adjust_enabled: false,
            adjust_interval_secs: 300,
            age_boost_factor: 0.05,
            completion_boost: 1.0,
            slow_speed_penalty: 0.8,
            slow_speed_threshold: 50000,
            inheritance_multiplier: 2.0,
            max_adjustments: 20,
            inheritance_enabled: false,
        };
        queue.update_config(new_config).await;
        let loaded = queue.get_config().await;
        assert_eq!(loaded.adjust_interval_secs, 300);
        assert_eq!(loaded.age_boost_factor, 0.05);
        assert_eq!(loaded.completion_boost, 1.0);
        assert_eq!(loaded.slow_speed_penalty, 0.8);
        assert_eq!(loaded.slow_speed_threshold, 50000);
        assert_eq!(loaded.inheritance_multiplier, 2.0);
        assert_eq!(loaded.max_adjustments, 20);
        assert!(!loaded.inheritance_enabled);
    }

    // ========== PriorityQueue: get_task/contains/len/is_empty ==========

    #[tokio::test]
    async fn test_get_task_exists() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        let task = queue.get_task("t").await.unwrap();
        assert_eq!(task.task_id, "t");
    }

    #[tokio::test]
    async fn test_get_task_not_exists() {
        let queue = PriorityQueue::new();
        assert!(queue.get_task("nope").await.is_none());
    }

    #[tokio::test]
    async fn test_queue_empty_check() {
        let queue = PriorityQueue::new();
        assert!(queue.is_empty().await);

        queue
            .add_task("task".to_string(), Priority::Normal)
            .await
            .unwrap();
        assert!(!queue.is_empty().await);
    }

    // ========== PriorityQueue: clear ==========

    #[tokio::test]
    async fn test_queue_clear() {
        let queue = PriorityQueue::new();
        queue
            .add_task("task1".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("task2".to_string(), Priority::High)
            .await
            .unwrap();
        queue.clear().await;
        assert!(queue.is_empty().await);
    }

    #[tokio::test]
    async fn test_queue_clear_empty() {
        let queue = PriorityQueue::new();
        queue.clear().await;
        assert!(queue.is_empty().await);
    }

    // ========== PriorityQueue: get_tasks_by_priority ==========

    #[tokio::test]
    async fn test_get_tasks_by_priority() {
        let queue = PriorityQueue::new();
        queue
            .add_task("high1".to_string(), Priority::High)
            .await
            .unwrap();
        queue
            .add_task("high2".to_string(), Priority::High)
            .await
            .unwrap();
        queue
            .add_task("low1".to_string(), Priority::Low)
            .await
            .unwrap();

        let high_tasks = queue.get_tasks_by_priority(Priority::High).await;
        assert_eq!(high_tasks.len(), 2);
    }

    #[tokio::test]
    async fn test_get_tasks_by_priority_empty_result() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        let urgent = queue.get_tasks_by_priority(Priority::Urgent).await;
        assert!(urgent.is_empty());
    }

    #[tokio::test]
    async fn test_get_tasks_by_priority_all_variants() {
        let queue = PriorityQueue::new();
        queue
            .add_task("l".to_string(), Priority::Low)
            .await
            .unwrap();
        queue
            .add_task("n".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("h".to_string(), Priority::High)
            .await
            .unwrap();
        queue
            .add_task("u".to_string(), Priority::Urgent)
            .await
            .unwrap();

        for p in [
            Priority::Low,
            Priority::Normal,
            Priority::High,
            Priority::Urgent,
        ] {
            let tasks = queue.get_tasks_by_priority(p).await;
            assert_eq!(tasks.len(), 1);
        }
    }

    // ========== PriorityQueue: calculate_bandwidth_weights ==========

    #[tokio::test]
    async fn test_bandwidth_weights() {
        let queue = PriorityQueue::new();
        queue
            .add_task("urgent".to_string(), Priority::Urgent)
            .await
            .unwrap();
        queue
            .add_task("low".to_string(), Priority::Low)
            .await
            .unwrap();

        let weights = queue.calculate_bandwidth_weights().await;
        assert!(weights.get("urgent").unwrap() > weights.get("low").unwrap());
    }

    #[tokio::test]
    async fn test_bandwidth_weights_sum_to_one() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t1".to_string(), Priority::High)
            .await
            .unwrap();
        queue
            .add_task("t2".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("t3".to_string(), Priority::Low)
            .await
            .unwrap();

        let weights = queue.calculate_bandwidth_weights().await;
        let sum: f64 = weights.values().sum();
        assert!((sum - 1.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_bandwidth_weights_empty() {
        let queue = PriorityQueue::new();
        let weights = queue.calculate_bandwidth_weights().await;
        assert!(weights.is_empty());
    }

    #[tokio::test]
    async fn test_bandwidth_weights_single_task() {
        let queue = PriorityQueue::new();
        queue
            .add_task("only".to_string(), Priority::Normal)
            .await
            .unwrap();
        let weights = queue.calculate_bandwidth_weights().await;
        assert!((weights.get("only").unwrap() - 1.0).abs() < 0.01);
    }

    // ========== PriorityTask: serde ==========

    #[tokio::test]
    async fn test_priority_task_serde_roundtrip() {
        let task = PriorityTask::new("t1".to_string(), Priority::High);
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: PriorityTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "t1");
        assert_eq!(deserialized.priority, Priority::High);
        assert_eq!(deserialized.original_priority, Priority::High);
    }

    #[tokio::test]
    async fn test_priority_task_serde_unicode() {
        let task = PriorityTask::new("中文任务".to_string(), Priority::Normal);
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: PriorityTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "中文任务");
    }

    // ========== PriorityConfig: default functions ==========

    #[tokio::test]
    async fn test_default_functions() {
        assert!(default_true());
        assert_eq!(default_adjust_interval(), 60);
        assert_eq!(default_age_boost(), 0.01);
        assert_eq!(default_completion_boost(), 0.5);
        assert_eq!(default_slow_penalty(), 0.3);
        assert_eq!(default_slow_threshold(), 10240);
        assert_eq!(default_inheritance_mult(), 1.2);
        assert_eq!(default_max_adjustments(), 10);
    }

    // ========== Concurrent access ==========

    #[tokio::test]
    async fn test_concurrent_task_access() {
        let queue = Arc::new(PriorityQueue::new());
        let mut handles = vec![];

        for i in 0..10 {
            let q = Arc::clone(&queue);
            handles.push(tokio::spawn(async move {
                q.add_task(format!("task{}", i), Priority::Normal).await
            }));
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        assert_eq!(queue.len().await, 10);
    }

    // ========== Task age boost ==========

    #[tokio::test]
    async fn test_task_age_boost() {
        let config = PriorityConfig {
            age_boost_factor: 0.1, // Significant age boost
            ..Default::default()
        };
        let queue = PriorityQueue::with_config(config);

        // Add old task
        queue
            .add_task("old".to_string(), Priority::Normal)
            .await
            .unwrap();
        // Manually set old timestamp
        {
            let mut tasks = queue.tasks.write().await;
            if let Some(task) = tasks.get_mut("old") {
                task.added_at = Utc::now() - chrono::Duration::hours(48);
            }
        }

        // Add new task
        queue
            .add_task("new".to_string(), Priority::Normal)
            .await
            .unwrap();

        let order = queue.get_scheduled_order().await;
        assert_eq!(order[0], "old"); // Old task should be first due to age boost
    }

    // ========== Complex workflows ==========

    #[tokio::test]
    async fn test_complete_lifecycle() {
        let queue = PriorityQueue::new();

        // Add tasks
        queue
            .add_task("t1".to_string(), Priority::Low)
            .await
            .unwrap();
        queue
            .add_task("t2".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("t3".to_string(), Priority::High)
            .await
            .unwrap();

        // Verify order
        let order = queue.get_scheduled_order().await;
        assert_eq!(order[0], "t3");

        // Update metrics
        queue.update_task_metrics("t1", 0.5, 50000).await.unwrap();
        queue.update_task_metrics("t2", 0.3, 100000).await.unwrap();

        // Change priority
        queue.update_priority("t1", Priority::Urgent).await.unwrap();

        // Now t1 should be first
        let next = queue.get_next_task().await.unwrap();
        assert_eq!(next, "t1");

        // Remove and verify
        queue.remove_task("t2").await.unwrap();
        assert_eq!(queue.len().await, 2);

        // Status
        let status = queue.get_queue_status().await;
        assert_eq!(status.total_tasks, 2);
    }

    #[tokio::test]
    async fn test_multiple_inheritance_chains() {
        let queue = PriorityQueue::new();

        queue
            .add_task("parent1".to_string(), Priority::Urgent)
            .await
            .unwrap();
        queue
            .add_task("parent2".to_string(), Priority::Low)
            .await
            .unwrap();
        queue
            .add_task_with_parent("child1".to_string(), Priority::Low, "parent1".to_string())
            .await
            .unwrap();
        queue
            .add_task_with_parent(
                "child2".to_string(),
                Priority::Normal,
                "parent2".to_string(),
            )
            .await
            .unwrap();

        let child1 = queue.get_task("child1").await.unwrap();
        assert_eq!(child1.priority, Priority::Urgent); // Inherited high

        let child2 = queue.get_task("child2").await.unwrap();
        assert_eq!(child2.priority, Priority::Normal); // Keeps own higher
    }

    #[tokio::test]
    async fn test_50_tasks_scheduling() {
        let queue = PriorityQueue::new();
        for i in 0..50 {
            let p = match i % 4 {
                0 => Priority::Low,
                1 => Priority::Normal,
                2 => Priority::High,
                _ => Priority::Urgent,
            };
            queue.add_task(format!("task_{}", i), p).await.unwrap();
        }

        let order = queue.get_scheduled_order().await;
        assert_eq!(order.len(), 50);

        // First tasks should be urgent
        for id in &order[..12] {
            let task = queue.get_task(id).await.unwrap();
            assert!(task.priority == Priority::Urgent || task.priority == Priority::High);
        }
    }

    #[tokio::test]
    async fn test_remove_and_readd() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue.remove_task("t").await.unwrap();
        queue
            .add_task("t".to_string(), Priority::High)
            .await
            .unwrap();
        let task = queue.get_task("t").await.unwrap();
        assert_eq!(task.priority, Priority::High);
    }

    // ========== Edge cases ==========

    #[tokio::test]
    async fn test_empty_task_id() {
        let queue = PriorityQueue::new();
        queue
            .add_task("".to_string(), Priority::Normal)
            .await
            .unwrap();
        assert!(queue.contains("").await);
    }

    #[tokio::test]
    async fn test_progress_exactly_90_percent() {
        let config = PriorityConfig::default();
        let mut task = PriorityTask::new("t".to_string(), Priority::Normal);
        task.update_metrics(0.9, 100000);
        let score = task.calculate_effective_score(&config);
        // At exactly 0.9, no completion boost (condition is > 0.9)
        assert!(score < Priority::Normal.weight() + config.completion_boost);
    }

    #[tokio::test]
    async fn test_progress_just_above_90_percent() {
        let config = PriorityConfig::default();
        let mut task = PriorityTask::new("t".to_string(), Priority::Normal);
        task.update_metrics(0.91, 100000);
        let score = task.calculate_effective_score(&config);
        // At 0.91, should have completion boost
        assert!(score > Priority::Normal.weight());
    }

    #[tokio::test]
    async fn test_queue_status_serde_roundtrip() {
        let queue = PriorityQueue::new();
        queue
            .add_task("t1".to_string(), Priority::High)
            .await
            .unwrap();
        let status = queue.get_queue_status().await;
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: QueueStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_tasks, status.total_tasks);
    }

    #[tokio::test]
    async fn test_bandwidth_weights_all_same_priority() {
        let queue = PriorityQueue::new();
        queue
            .add_task("a".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("b".to_string(), Priority::Normal)
            .await
            .unwrap();
        queue
            .add_task("c".to_string(), Priority::Normal)
            .await
            .unwrap();

        let weights = queue.calculate_bandwidth_weights().await;
        // All should be roughly equal (1/3 each)
        for w in weights.values() {
            assert!((w - 1.0 / 3.0).abs() < 0.1);
        }
    }
}
