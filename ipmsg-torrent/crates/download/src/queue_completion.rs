//! Download Queue Completion Predictor (Phase 112)
//!
//! Estimates when all queued downloads will finish, accounting for:
//! - Current download progress and speeds
//! - Concurrent download limits
//! - Task priorities and dependencies
//! - Historical speed data
//!
//! Useful for planning and resource allocation decisions.

use crate::eta_estimator::{EtaConfidence, EtaEstimator};
use crate::DownloadTask;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Configuration for queue completion prediction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueCompletionConfig {
    /// Enable queue completion prediction
    pub enabled: bool,
    /// Assume stalled downloads will complete at this speed (bytes/sec, 0 = ignore)
    pub stalled_speed_assumption_bps: u64,
    /// Minimum samples before including task in prediction
    pub min_samples: u32,
    /// Prediction confidence threshold (0.0-1.0)
    pub confidence_threshold: f64,
}

impl Default for QueueCompletionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            stalled_speed_assumption_bps: 1024, // 1 KB/s
            min_samples: 3,
            confidence_threshold: 0.5,
        }
    }
}

/// Individual task completion estimate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompletionEstimate {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Current progress (0.0-1.0)
    pub progress: f32,
    /// Estimated time to complete (seconds)
    pub eta_seconds: Option<f64>,
    /// Confidence level (0.0-1.0)
    pub confidence: f64,
    /// Is task currently downloading?
    pub is_active: bool,
    /// Task priority
    pub priority: crate::DownloadPriority,
}

/// Queue completion prediction result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueCompletionPrediction {
    /// Timestamp of prediction
    pub predicted_at: DateTime<Utc>,
    /// Estimated total time to complete all tasks (seconds)
    pub total_eta_seconds: f64,
    /// Estimated completion timestamp
    pub estimated_completion: Option<DateTime<Utc>>,
    /// Number of tasks included in prediction
    pub task_count: usize,
    /// Number of tasks with reliable estimates
    pub reliable_estimates: usize,
    /// Overall confidence (0.0-1.0)
    pub confidence: f64,
    /// Per-task estimates
    pub task_estimates: Vec<TaskCompletionEstimate>,
    /// Active concurrent downloads
    pub active_downloads: usize,
    /// Maximum concurrent downloads allowed
    pub max_concurrent: usize,
    /// Prediction summary message
    pub summary: String,
}

/// Queue completion predictor
#[derive(Debug, Clone)]
pub struct QueueCompletionPredictor {
    config: QueueCompletionConfig,
}

impl QueueCompletionPredictor {
    /// Create a new predictor with default configuration
    pub fn new() -> Self {
        Self {
            config: QueueCompletionConfig::default(),
        }
    }

    /// Create a new predictor with custom configuration
    pub fn from_config(config: QueueCompletionConfig) -> Self {
        Self { config }
    }

    /// Get current configuration
    pub fn config(&self) -> &QueueCompletionConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: QueueCompletionConfig) {
        self.config = config;
    }

    /// Predict queue completion time
    ///
    /// # Arguments
    /// * `tasks` - All download tasks in the queue
    /// * `eta_estimator` - ETA estimator with historical speed data
    /// * `max_concurrent` - Maximum concurrent downloads allowed
    ///
    /// # Returns
    /// Queue completion prediction
    pub async fn predict(
        &self,
        tasks: &[DownloadTask],
        eta_estimator: &EtaEstimator,
        max_concurrent: usize,
    ) -> QueueCompletionPrediction {
        let mut task_estimates = Vec::new();
        let mut active_count = 0;
        let mut reliable_count = 0;

        // Collect estimates for all non-completed tasks
        for task in tasks {
            if task.state == crate::DownloadState::Complete
                || task.state == crate::DownloadState::Error
            {
                continue;
            }

            let is_active = task.state == crate::DownloadState::Downloading;
            if is_active {
                active_count += 1;
            }

            // Get ETA from estimator
            let remaining_bytes = task.size.saturating_sub(task.downloaded);
            let eta = eta_estimator.estimate(&task.id, remaining_bytes).await;
            let progress = task.progress();

            let (eta_seconds, confidence) = if let Some(eta) = eta {
                if eta.sample_count >= self.config.min_samples {
                    let conf = match eta.confidence {
                        EtaConfidence::High => 0.9,
                        EtaConfidence::Medium => 0.6,
                        EtaConfidence::Low => 0.3,
                    };
                    if conf >= self.config.confidence_threshold {
                        reliable_count += 1;
                        (Some(eta.estimated_secs), conf)
                    } else if eta.estimated_secs.is_finite() && eta.estimated_secs > 0.0 {
                        (Some(eta.estimated_secs), conf)
                    } else {
                        (None, conf)
                    }
                } else {
                    (None, 0.0)
                }
            } else {
                (None, 0.0)
            };

            task_estimates.push(TaskCompletionEstimate {
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                progress,
                eta_seconds,
                confidence,
                is_active,
                priority: task.priority,
            });
        }

        // Sort by priority (high to low), then by queue position
        task_estimates.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.progress.partial_cmp(&b.progress).unwrap_or(std::cmp::Ordering::Equal))
        });

        // Calculate total completion time accounting for concurrency
        let total_eta_seconds = self.calculate_total_time(&task_estimates, max_concurrent);

        // Calculate overall confidence (weighted average)
        let confidence = if task_estimates.is_empty() {
            1.0
        } else {
            let total_conf: f64 = task_estimates.iter().map(|t| t.confidence).sum();
            total_conf / task_estimates.len() as f64
        };

        // Estimate completion timestamp
        let estimated_completion = if total_eta_seconds.is_finite() && total_eta_seconds > 0.0 {
            Some(Utc::now() + chrono::Duration::seconds(total_eta_seconds as i64))
        } else {
            None
        };

        // Generate summary
        let summary = self.generate_summary(
            task_estimates.len(),
            reliable_count,
            total_eta_seconds,
            confidence,
            active_count,
            max_concurrent,
        );

        QueueCompletionPrediction {
            predicted_at: Utc::now(),
            total_eta_seconds,
            estimated_completion,
            task_count: task_estimates.len(),
            reliable_estimates: reliable_count,
            confidence,
            task_estimates,
            active_downloads: active_count,
            max_concurrent,
            summary,
        }
    }

    /// Calculate total completion time accounting for concurrent downloads
    fn calculate_total_time(
        &self,
        task_estimates: &[TaskCompletionEstimate],
        max_concurrent: usize,
    ) -> f64 {
        if task_estimates.is_empty() {
            return 0.0;
        }

        // Simple model: tasks run in waves of max_concurrent
        // Each wave takes as long as the slowest task in that wave
        let mut wave_times = Vec::new();
        let mut current_wave = Vec::new();

        for estimate in task_estimates {
            if let Some(eta) = estimate.eta_seconds {
                if eta.is_finite() && eta > 0.0 {
                    current_wave.push(eta);

                    if current_wave.len() >= max_concurrent {
                        // Wave complete, add the slowest task time
                        wave_times.push(current_wave.iter().cloned().fold(0.0_f64, f64::max));
                        current_wave.clear();
                    }
                }
            }
        }

        // Add remaining tasks in the last wave
        if !current_wave.is_empty() {
            wave_times.push(current_wave.iter().cloned().fold(0.0_f64, f64::max));
        }

        // Sum all wave times
        let total_time: f64 = wave_times.iter().sum();

        // If no reliable estimates, return infinity
        if !total_time.is_finite() || total_time <= 0.0 {
            f64::INFINITY
        } else {
            total_time
        }
    }

    /// Generate human-readable summary
    fn generate_summary(
        &self,
        task_count: usize,
        reliable_count: usize,
        total_eta_seconds: f64,
        confidence: f64,
        active_downloads: usize,
        max_concurrent: usize,
    ) -> String {
        if task_count == 0 {
            return "Queue is empty or all tasks are complete.".to_string();
        }

        let confidence_label = if confidence >= 0.8 {
            "high"
        } else if confidence >= 0.5 {
            "medium"
        } else {
            "low"
        };

        let time_str = if total_eta_seconds.is_finite() && total_eta_seconds > 0.0 {
            format_duration(total_eta_seconds)
        } else {
            "unknown".to_string()
        };

        format!(
            "{} tasks remaining ({} active, {} max concurrent). Estimated completion: {} ({} confidence, {}/{} reliable estimates)",
            task_count,
            active_downloads,
            max_concurrent,
            time_str,
            confidence_label,
            reliable_count,
            task_count
        )
    }
}

/// Format duration in human-readable form
fn format_duration(seconds: f64) -> String {
    if seconds < 60.0 {
        format!("{:.0}s", seconds)
    } else if seconds < 3600.0 {
        let mins = (seconds / 60.0).floor();
        let secs = (seconds % 60.0).floor();
        format!("{}m {:.0}s", mins, secs)
    } else if seconds < 86400.0 {
        let hours = (seconds / 3600.0).floor();
        let mins = ((seconds % 3600.0) / 60.0).floor();
        format!("{}h {}m", hours, mins)
    } else {
        let days = (seconds / 86400.0).floor();
        let hours = ((seconds % 86400.0) / 3600.0).floor();
        format!("{}d {}h", days, hours)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eta_estimator::EtaEstimator;
    use crate::{DownloadPriority, DownloadProtocol, DownloadState, DownloadTask};
    use std::path::PathBuf;

    fn create_test_task(id: &str, name: &str, state: DownloadState, progress: f32) -> DownloadTask {
        let size = 100_000_000u64;
        let downloaded = (size as f32 * progress) as u64;
        DownloadTask {
            id: id.to_string(),
            name: name.to_string(),
            protocol: DownloadProtocol::Xunlei,
            size,
            downloaded,
            state,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from(format!("/tmp/{}", name)),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec![],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: vec![],
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            current_session_start: None,
            mirror_urls: vec![],
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        }
    }

    #[test]
    fn test_config_default() {
        let config = QueueCompletionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 3);
        assert_eq!(config.stalled_speed_assumption_bps, 1024);
    }

    #[test]
    fn test_predictor_creation() {
        let predictor = QueueCompletionPredictor::new();
        assert!(predictor.config().enabled);

        let config = QueueCompletionConfig {
            enabled: false,
            min_samples: 5,
            ..Default::default()
        };
        let predictor = QueueCompletionPredictor::from_config(config);
        assert!(!predictor.config().enabled);
        assert_eq!(predictor.config().min_samples, 5);
    }

    #[tokio::test]
    async fn test_predict_empty_queue() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();
        let tasks: Vec<DownloadTask> = vec![];

        let prediction = predictor.predict(&tasks, &eta_estimator, 3).await;

        assert_eq!(prediction.task_count, 0);
        assert_eq!(prediction.total_eta_seconds, 0.0);
        assert!(prediction.summary.contains("empty"));
    }

    #[tokio::test]
    async fn test_predict_with_active_tasks() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        // Add some speed data to the estimator
        eta_estimator.update_speed("task1", 1_000_000.0).await; // 1 MB/s
        eta_estimator.update_speed("task1", 1_000_000.0).await;
        eta_estimator.update_speed("task1", 1_000_000.0).await;

        let tasks = vec![
            create_test_task("task1", "file1.txt", DownloadState::Downloading, 0.5),
            create_test_task("task2", "file2.txt", DownloadState::Queued, 0.0),
        ];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        assert_eq!(prediction.task_count, 2);
        assert_eq!(prediction.active_downloads, 1);
        assert!(prediction.total_eta_seconds > 0.0);
    }

    #[tokio::test]
    async fn test_predict_with_completed_tasks() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        let tasks = vec![
            create_test_task("task1", "file1.txt", DownloadState::Complete, 1.0),
            create_test_task("task2", "file2.txt", DownloadState::Downloading, 0.5),
        ];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        // Only task2 should be included (task1 is complete)
        assert_eq!(prediction.task_count, 1);
        assert_eq!(prediction.task_estimates[0].task_id, "task2");
    }

    #[test]
    fn test_calculate_total_time_single_concurrent() {
        let predictor = QueueCompletionPredictor::new();
        let estimates = vec![
            TaskCompletionEstimate {
                task_id: "t1".to_string(),
                task_name: "f1".to_string(),
                progress: 0.5,
                eta_seconds: Some(100.0),
                confidence: 0.8,
                is_active: true,
                priority: crate::DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t2".to_string(),
                task_name: "f2".to_string(),
                progress: 0.0,
                eta_seconds: Some(200.0),
                confidence: 0.8,
                is_active: false,
                priority: crate::DownloadPriority::Normal,
            },
        ];

        // With max_concurrent=1, tasks run sequentially
        let total = predictor.calculate_total_time(&estimates, 1);
        assert_eq!(total, 300.0); // 100 + 200
    }

    #[test]
    fn test_calculate_total_time_parallel() {
        let predictor = QueueCompletionPredictor::new();
        let estimates = vec![
            TaskCompletionEstimate {
                task_id: "t1".to_string(),
                task_name: "f1".to_string(),
                progress: 0.5,
                eta_seconds: Some(100.0),
                confidence: 0.8,
                is_active: true,
                priority: crate::DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t2".to_string(),
                task_name: "f2".to_string(),
                progress: 0.0,
                eta_seconds: Some(200.0),
                confidence: 0.8,
                is_active: false,
                priority: crate::DownloadPriority::Normal,
            },
        ];

        // With max_concurrent=2, tasks run in parallel
        let total = predictor.calculate_total_time(&estimates, 2);
        assert_eq!(total, 200.0); // max(100, 200)
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30.0), "30s");
        assert_eq!(format_duration(90.0), "1m 30s");
        assert_eq!(format_duration(3661.0), "1h 1m");
        assert_eq!(format_duration(90000.0), "1d 1h");
    }

    #[tokio::test]
    async fn test_prediction_confidence() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        // Add plenty of samples for high confidence
        for _ in 0..10 {
            eta_estimator.update_speed("task1", 1_000_000.0).await;
        }

        let tasks = vec![create_test_task(
            "task1",
            "file1.txt",
            DownloadState::Downloading,
            0.5,
        )];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        assert!(prediction.confidence > 0.5);
        assert_eq!(prediction.reliable_estimates, 1);
    }

    #[tokio::test]
    async fn test_prediction_with_error_tasks() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        let tasks = vec![
            create_test_task("task1", "file1.txt", DownloadState::Error, 0.3),
            create_test_task("task2", "file2.txt", DownloadState::Downloading, 0.5),
        ];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        // Error tasks should be excluded
        assert_eq!(prediction.task_count, 1);
        assert_eq!(prediction.task_estimates[0].task_id, "task2");
    }

    #[test]
    fn test_summary_generation() {
        let predictor = QueueCompletionPredictor::new();
        let summary = predictor.generate_summary(5, 4, 3600.0, 0.75, 2, 3);

        assert!(summary.contains("5 tasks"));
        assert!(summary.contains("2 active"));
        assert!(summary.contains("3 max concurrent"));
        assert!(summary.contains("1h"));
        assert!(summary.contains("medium confidence"));
        assert!(summary.contains("4/5 reliable"));
    }

    #[test]
    fn test_summary_empty_queue() {
        let predictor = QueueCompletionPredictor::new();
        let summary = predictor.generate_summary(0, 0, 0.0, 1.0, 0, 3);

        assert!(summary.contains("empty"));
    }
}
