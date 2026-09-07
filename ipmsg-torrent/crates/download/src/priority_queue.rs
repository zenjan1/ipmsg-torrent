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
    use std::time::Duration;

    #[tokio::test]
    async fn test_priority_ordering() {
        assert!(Priority::Urgent > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
    }

    #[tokio::test]
    async fn test_priority_from_str() {
        assert_eq!(Priority::from_str_opt("low"), Some(Priority::Low));
        assert_eq!(Priority::from_str_opt("HIGH"), Some(Priority::High));
        assert_eq!(Priority::from_str_opt("urgent"), Some(Priority::Urgent));
        assert_eq!(Priority::from_str_opt("normal"), Some(Priority::Normal));
        assert_eq!(Priority::from_str_opt("invalid"), None);
    }

    #[tokio::test]
    async fn test_priority_weight() {
        assert!(Priority::Urgent.weight() > Priority::High.weight());
        assert!(Priority::High.weight() > Priority::Normal.weight());
        assert!(Priority::Normal.weight() > Priority::Low.weight());
    }

    #[tokio::test]
    async fn test_create_priority_task() {
        let task = PriorityTask::new("task1".to_string(), Priority::High);
        assert_eq!(task.task_id, "task1");
        assert_eq!(task.priority, Priority::High);
        assert_eq!(task.original_priority, Priority::High);
        assert_eq!(task.adjustment_count, 0);
    }

    #[tokio::test]
    async fn test_task_with_parent() {
        let task = PriorityTask::new("child".to_string(), Priority::Normal)
            .with_parent("parent".to_string());
        assert_eq!(task.parent_task_id, Some("parent".to_string()));
    }

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
    async fn test_effective_score_calculation() {
        let config = PriorityConfig::default();
        let mut task = PriorityTask::new("task1".to_string(), Priority::High);
        task.update_metrics(0.95, 100000); // Near complete

        let score = task.calculate_effective_score(&config);
        assert!(score > Priority::High.weight()); // Should have completion boost
    }

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
    async fn test_empty_queue_next_task() {
        let queue = PriorityQueue::new();
        let next = queue.get_next_task().await;
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn test_empty_queue_status() {
        let queue = PriorityQueue::new();
        let status = queue.get_queue_status().await;
        assert_eq!(status.total_tasks, 0);
        assert!(status.next_tasks.is_empty());
    }

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
            Priority::Urgent.to_download_priority(),
            crate::DownloadPriority::High
        );
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
}
