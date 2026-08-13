//! Download Queue Completion Predictor (Phase 112)
//!
//! Estimates when all queued downloads will finish, accounting for:
//! - Current download progress and speeds
//! - Concurrent download limits
//! - Task priorities and dependencies
//! - Historical speed data
//!
//! Useful for planning and resource allocation decisions.

use crate::DownloadTask;
use crate::eta_estimator::{EtaConfidence, EtaEstimator};
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
            b.priority.cmp(&a.priority).then_with(|| {
                a.progress
                    .partial_cmp(&b.progress)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
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

    // ========== Phase 209: Comprehensive Test Coverage ==========

    // --- Serialization Tests ---

    #[test]
    fn test_config_serde_roundtrip() {
        let config = QueueCompletionConfig {
            enabled: false,
            stalled_speed_assumption_bps: 2048,
            min_samples: 5,
            confidence_threshold: 0.7,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: QueueCompletionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(
            deserialized.stalled_speed_assumption_bps,
            config.stalled_speed_assumption_bps
        );
        assert_eq!(deserialized.min_samples, config.min_samples);
        assert!((deserialized.confidence_threshold - config.confidence_threshold).abs() < 1e-10);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{
            "enabled": true,
            "stalled_speed_assumption_bps": 1024,
            "min_samples": 3,
            "confidence_threshold": 0.5,
            "unknown_field": "should be ignored"
        }"#;
        let config: QueueCompletionConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 3);
    }

    #[test]
    fn test_task_completion_estimate_serde_roundtrip() {
        let estimate = TaskCompletionEstimate {
            task_id: "task123".to_string(),
            task_name: "test_file.txt".to_string(),
            progress: 0.45,
            eta_seconds: Some(120.5),
            confidence: 0.75,
            is_active: true,
            priority: DownloadPriority::High,
        };
        let json = serde_json::to_string(&estimate).unwrap();
        let deserialized: TaskCompletionEstimate = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, estimate.task_id);
        assert_eq!(deserialized.task_name, estimate.task_name);
        assert!((deserialized.progress - estimate.progress).abs() < 1e-6);
        assert_eq!(deserialized.eta_seconds, estimate.eta_seconds);
        assert!((deserialized.confidence - estimate.confidence).abs() < 1e-10);
        assert_eq!(deserialized.is_active, estimate.is_active);
        assert_eq!(deserialized.priority, estimate.priority);
    }

    #[test]
    fn test_task_completion_estimate_none_eta() {
        let estimate = TaskCompletionEstimate {
            task_id: "task1".to_string(),
            task_name: "file.txt".to_string(),
            progress: 0.0,
            eta_seconds: None,
            confidence: 0.0,
            is_active: false,
            priority: DownloadPriority::Low,
        };
        let json = serde_json::to_string(&estimate).unwrap();
        let deserialized: TaskCompletionEstimate = serde_json::from_str(&json).unwrap();
        assert!(deserialized.eta_seconds.is_none());
        assert_eq!(deserialized.confidence, 0.0);
    }

    #[test]
    fn test_prediction_serde_roundtrip() {
        let prediction = QueueCompletionPrediction {
            predicted_at: Utc::now(),
            total_eta_seconds: 3600.0,
            estimated_completion: Some(Utc::now() + chrono::Duration::hours(1)),
            task_count: 5,
            reliable_estimates: 4,
            confidence: 0.8,
            task_estimates: vec![],
            active_downloads: 2,
            max_concurrent: 3,
            summary: "Test summary".to_string(),
        };
        let json = serde_json::to_string(&prediction).unwrap();
        let deserialized: QueueCompletionPrediction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_count, prediction.task_count);
        assert_eq!(
            deserialized.reliable_estimates,
            prediction.reliable_estimates
        );
        assert!((deserialized.confidence - prediction.confidence).abs() < 1e-10);
        assert_eq!(deserialized.active_downloads, prediction.active_downloads);
        assert_eq!(deserialized.max_concurrent, prediction.max_concurrent);
    }

    // --- Config Tests ---

    #[test]
    fn test_config_custom_values() {
        let config = QueueCompletionConfig {
            enabled: false,
            stalled_speed_assumption_bps: 512,
            min_samples: 10,
            confidence_threshold: 0.9,
        };
        assert!(!config.enabled);
        assert_eq!(config.stalled_speed_assumption_bps, 512);
        assert_eq!(config.min_samples, 10);
        assert!((config.confidence_threshold - 0.9).abs() < 1e-10);
    }

    #[test]
    fn test_predictor_set_config() {
        let mut predictor = QueueCompletionPredictor::new();
        assert!(predictor.config().enabled);

        let new_config = QueueCompletionConfig {
            enabled: false,
            min_samples: 7,
            ..Default::default()
        };
        predictor.set_config(new_config);

        assert!(!predictor.config().enabled);
        assert_eq!(predictor.config().min_samples, 7);
    }

    // --- Clone/Debug Trait Tests ---

    #[test]
    fn test_config_clone_debug() {
        let config = QueueCompletionConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.min_samples, config.min_samples);

        // Debug trait
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("QueueCompletionConfig"));
    }

    #[test]
    fn test_task_completion_estimate_clone_debug() {
        let estimate = TaskCompletionEstimate {
            task_id: "t1".to_string(),
            task_name: "f1".to_string(),
            progress: 0.5,
            eta_seconds: Some(100.0),
            confidence: 0.8,
            is_active: true,
            priority: DownloadPriority::Normal,
        };
        let cloned = estimate.clone();
        assert_eq!(cloned.task_id, estimate.task_id);
        assert_eq!(cloned.progress, estimate.progress);

        let debug_str = format!("{:?}", estimate);
        assert!(debug_str.contains("TaskCompletionEstimate"));
    }

    #[test]
    fn test_prediction_clone_debug() {
        let prediction = QueueCompletionPrediction {
            predicted_at: Utc::now(),
            total_eta_seconds: 100.0,
            estimated_completion: None,
            task_count: 1,
            reliable_estimates: 1,
            confidence: 0.9,
            task_estimates: vec![],
            active_downloads: 1,
            max_concurrent: 2,
            summary: "Test".to_string(),
        };
        let cloned = prediction.clone();
        assert_eq!(cloned.task_count, prediction.task_count);
        assert_eq!(cloned.summary, prediction.summary);

        let debug_str = format!("{:?}", prediction);
        assert!(debug_str.contains("QueueCompletionPrediction"));
    }

    #[test]
    fn test_predictor_clone() {
        let predictor = QueueCompletionPredictor::new();
        let cloned = predictor.clone();
        assert_eq!(cloned.config().enabled, predictor.config().enabled);
        assert_eq!(cloned.config().min_samples, predictor.config().min_samples);
    }

    // --- format_duration Boundary Tests ---

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0.0), "0s");
    }

    #[test]
    fn test_format_duration_exactly_60_seconds() {
        assert_eq!(format_duration(60.0), "1m 0s");
    }

    #[test]
    fn test_format_duration_just_under_60_seconds() {
        assert_eq!(format_duration(59.0), "59s");
    }

    #[test]
    fn test_format_duration_exactly_3600_seconds() {
        assert_eq!(format_duration(3600.0), "1h 0m");
    }

    #[test]
    fn test_format_duration_just_under_3600_seconds() {
        assert_eq!(format_duration(3599.0), "59m 59s");
    }

    #[test]
    fn test_format_duration_exactly_86400_seconds() {
        assert_eq!(format_duration(86400.0), "1d 0h");
    }

    #[test]
    fn test_format_duration_just_under_86400_seconds() {
        assert_eq!(format_duration(86399.0), "23h 59m");
    }

    #[test]
    fn test_format_duration_large_value() {
        assert_eq!(format_duration(172800.0), "2d 0h");
        assert_eq!(format_duration(604800.0), "7d 0h");
    }

    #[test]
    fn test_format_duration_fractional_seconds() {
        assert_eq!(format_duration(30.7), "31s");
        assert_eq!(format_duration(90.3), "1m 30s");
    }

    // --- calculate_total_time Edge Cases ---

    #[test]
    fn test_calculate_total_time_empty_estimates() {
        let predictor = QueueCompletionPredictor::new();
        let estimates: Vec<TaskCompletionEstimate> = vec![];
        let total = predictor.calculate_total_time(&estimates, 3);
        assert_eq!(total, 0.0);
    }

    #[test]
    fn test_calculate_total_time_all_none_eta() {
        let predictor = QueueCompletionPredictor::new();
        let estimates = vec![
            TaskCompletionEstimate {
                task_id: "t1".to_string(),
                task_name: "f1".to_string(),
                progress: 0.5,
                eta_seconds: None,
                confidence: 0.0,
                is_active: true,
                priority: DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t2".to_string(),
                task_name: "f2".to_string(),
                progress: 0.0,
                eta_seconds: None,
                confidence: 0.0,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
        ];
        let total = predictor.calculate_total_time(&estimates, 2);
        assert!(!total.is_finite()); // Should return infinity
    }

    #[test]
    fn test_calculate_total_time_zero_eta() {
        let predictor = QueueCompletionPredictor::new();
        let estimates = vec![TaskCompletionEstimate {
            task_id: "t1".to_string(),
            task_name: "f1".to_string(),
            progress: 1.0,
            eta_seconds: Some(0.0),
            confidence: 0.9,
            is_active: true,
            priority: DownloadPriority::Normal,
        }];
        let total = predictor.calculate_total_time(&estimates, 2);
        assert!(!total.is_finite()); // Zero ETA should result in infinity
    }

    #[test]
    fn test_calculate_total_time_mixed_none_and_some() {
        let predictor = QueueCompletionPredictor::new();
        let estimates = vec![
            TaskCompletionEstimate {
                task_id: "t1".to_string(),
                task_name: "f1".to_string(),
                progress: 0.5,
                eta_seconds: Some(100.0),
                confidence: 0.8,
                is_active: true,
                priority: DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t2".to_string(),
                task_name: "f2".to_string(),
                progress: 0.0,
                eta_seconds: None,
                confidence: 0.0,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
        ];
        let total = predictor.calculate_total_time(&estimates, 2);
        assert_eq!(total, 100.0); // Only counts tasks with Some(eta)
    }

    #[test]
    fn test_calculate_total_time_three_waves() {
        let predictor = QueueCompletionPredictor::new();
        let estimates = vec![
            TaskCompletionEstimate {
                task_id: "t1".to_string(),
                task_name: "f1".to_string(),
                progress: 0.0,
                eta_seconds: Some(100.0),
                confidence: 0.8,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t2".to_string(),
                task_name: "f2".to_string(),
                progress: 0.0,
                eta_seconds: Some(200.0),
                confidence: 0.8,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t3".to_string(),
                task_name: "f3".to_string(),
                progress: 0.0,
                eta_seconds: Some(150.0),
                confidence: 0.8,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t4".to_string(),
                task_name: "f4".to_string(),
                progress: 0.0,
                eta_seconds: Some(300.0),
                confidence: 0.8,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
        ];
        // With max_concurrent=2: wave1=[100,200]=200, wave2=[150,300]=300, total=500
        let total = predictor.calculate_total_time(&estimates, 2);
        assert_eq!(total, 500.0);
    }

    #[test]
    fn test_calculate_total_time_high_concurrency() {
        let predictor = QueueCompletionPredictor::new();
        let estimates = vec![
            TaskCompletionEstimate {
                task_id: "t1".to_string(),
                task_name: "f1".to_string(),
                progress: 0.0,
                eta_seconds: Some(100.0),
                confidence: 0.8,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t2".to_string(),
                task_name: "f2".to_string(),
                progress: 0.0,
                eta_seconds: Some(200.0),
                confidence: 0.8,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
            TaskCompletionEstimate {
                task_id: "t3".to_string(),
                task_name: "f3".to_string(),
                progress: 0.0,
                eta_seconds: Some(150.0),
                confidence: 0.8,
                is_active: false,
                priority: DownloadPriority::Normal,
            },
        ];
        // With max_concurrent=10 (higher than task count), all in one wave
        let total = predictor.calculate_total_time(&estimates, 10);
        assert_eq!(total, 200.0); // max(100, 200, 150)
    }

    // --- Priority Sorting Tests ---

    #[tokio::test]
    async fn test_predict_priority_sorting() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        // Add speed data for all tasks
        for task_id in ["low", "normal", "high"] {
            for _ in 0..5 {
                eta_estimator.update_speed(task_id, 1_000_000.0).await;
            }
        }

        let mut tasks = vec![
            create_test_task("low", "low_priority.txt", DownloadState::Queued, 0.0),
            create_test_task("normal", "normal_priority.txt", DownloadState::Queued, 0.0),
            create_test_task("high", "high_priority.txt", DownloadState::Queued, 0.0),
        ];

        // Set priorities
        tasks[0].priority = DownloadPriority::Low;
        tasks[1].priority = DownloadPriority::Normal;
        tasks[2].priority = DownloadPriority::High;

        let prediction = predictor.predict(&tasks, &eta_estimator, 3).await;

        // High priority should come first
        assert_eq!(
            prediction.task_estimates[0].priority,
            DownloadPriority::High
        );
        assert_eq!(
            prediction.task_estimates[1].priority,
            DownloadPriority::Normal
        );
        assert_eq!(prediction.task_estimates[2].priority, DownloadPriority::Low);
    }

    // --- Confidence Threshold Tests ---

    #[tokio::test]
    async fn test_predict_confidence_threshold_filtering() {
        let config = QueueCompletionConfig {
            confidence_threshold: 0.95, // Very high threshold
            min_samples: 3,
            ..Default::default()
        };
        let predictor = QueueCompletionPredictor::from_config(config);
        let eta_estimator = EtaEstimator::new();

        // Add only 3 samples (should give Low or Medium confidence)
        for _ in 0..3 {
            eta_estimator.update_speed("task1", 1_000_000.0).await;
        }

        let tasks = vec![create_test_task(
            "task1",
            "file1.txt",
            DownloadState::Downloading,
            0.5,
        )];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        // With high threshold, reliable count should be 0
        assert_eq!(prediction.reliable_estimates, 0);
    }

    #[tokio::test]
    async fn test_predict_min_samples_requirement() {
        let config = QueueCompletionConfig {
            min_samples: 10, // Require many samples
            ..Default::default()
        };
        let predictor = QueueCompletionPredictor::from_config(config);
        let eta_estimator = EtaEstimator::new();

        // Add only 3 samples (below threshold)
        for _ in 0..3 {
            eta_estimator.update_speed("task1", 1_000_000.0).await;
        }

        let tasks = vec![create_test_task(
            "task1",
            "file1.txt",
            DownloadState::Downloading,
            0.5,
        )];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        // Should have no reliable estimates due to insufficient samples
        assert_eq!(prediction.reliable_estimates, 0);
        // But should still have an estimate with 0 confidence
        assert_eq!(prediction.task_estimates.len(), 1);
        assert_eq!(prediction.task_estimates[0].confidence, 0.0);
    }

    // --- State Filtering Tests ---

    #[tokio::test]
    async fn test_predict_paused_tasks_included() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        for _ in 0..5 {
            eta_estimator.update_speed("task1", 1_000_000.0).await;
        }

        let tasks = vec![
            create_test_task("task1", "file1.txt", DownloadState::Paused, 0.5),
            create_test_task("task2", "file2.txt", DownloadState::Downloading, 0.3),
        ];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        // Paused tasks should be included (not filtered like Complete/Error)
        assert_eq!(prediction.task_count, 2);
        assert_eq!(prediction.active_downloads, 1); // Only Downloading is active
    }

    #[tokio::test]
    async fn test_predict_all_states_filtering() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        let tasks = vec![
            create_test_task("queued", "queued.txt", DownloadState::Queued, 0.0),
            create_test_task(
                "downloading",
                "downloading.txt",
                DownloadState::Downloading,
                0.3,
            ),
            create_test_task("paused", "paused.txt", DownloadState::Paused, 0.5),
            create_test_task("complete", "complete.txt", DownloadState::Complete, 1.0),
            create_test_task("error", "error.txt", DownloadState::Error, 0.7),
        ];

        let prediction = predictor.predict(&tasks, &eta_estimator, 3).await;

        // Only Queued, Downloading, Paused should be included
        assert_eq!(prediction.task_count, 3);
        assert_eq!(prediction.active_downloads, 1);

        let task_ids: Vec<&str> = prediction
            .task_estimates
            .iter()
            .map(|t| t.task_id.as_str())
            .collect();
        assert!(task_ids.contains(&"queued"));
        assert!(task_ids.contains(&"downloading"));
        assert!(task_ids.contains(&"paused"));
        assert!(!task_ids.contains(&"complete"));
        assert!(!task_ids.contains(&"error"));
    }

    // --- Summary Generation Tests ---

    #[test]
    fn test_summary_high_confidence() {
        let predictor = QueueCompletionPredictor::new();
        let summary = predictor.generate_summary(5, 5, 3600.0, 0.85, 2, 3);
        assert!(summary.contains("high confidence"));
    }

    #[test]
    fn test_summary_medium_confidence() {
        let predictor = QueueCompletionPredictor::new();
        let summary = predictor.generate_summary(5, 4, 3600.0, 0.6, 2, 3);
        assert!(summary.contains("medium confidence"));
    }

    #[test]
    fn test_summary_low_confidence() {
        let predictor = QueueCompletionPredictor::new();
        let summary = predictor.generate_summary(5, 2, 3600.0, 0.3, 2, 3);
        assert!(summary.contains("low confidence"));
    }

    #[test]
    fn test_summary_unknown_time() {
        let predictor = QueueCompletionPredictor::new();
        let summary = predictor.generate_summary(5, 0, f64::INFINITY, 0.3, 0, 3);
        assert!(summary.contains("unknown"));
    }

    #[test]
    fn test_summary_time_formats() {
        let predictor = QueueCompletionPredictor::new();

        let summary_secs = predictor.generate_summary(1, 1, 30.0, 0.9, 1, 1);
        assert!(summary_secs.contains("30s"));

        let summary_mins = predictor.generate_summary(1, 1, 150.0, 0.9, 1, 1);
        assert!(summary_mins.contains("2m"));

        let summary_hours = predictor.generate_summary(1, 1, 7200.0, 0.9, 1, 1);
        assert!(summary_hours.contains("2h"));

        let summary_days = predictor.generate_summary(1, 1, 172800.0, 0.9, 1, 1);
        assert!(summary_days.contains("2d"));
    }

    // --- Prediction Completion Timestamp Tests ---

    #[tokio::test]
    async fn test_predict_estimated_completion_timestamp() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        for _ in 0..5 {
            eta_estimator.update_speed("task1", 1_000_000.0).await;
        }

        let tasks = vec![create_test_task(
            "task1",
            "file1.txt",
            DownloadState::Downloading,
            0.5,
        )];

        let before = Utc::now();
        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;
        let after = Utc::now();

        assert!(prediction.estimated_completion.is_some());
        let completion = prediction.estimated_completion.unwrap();

        // Completion should be in the future
        assert!(completion > after);

        // predicted_at should be between before and after
        assert!(prediction.predicted_at >= before);
        assert!(prediction.predicted_at <= after);
    }

    #[tokio::test]
    async fn test_predict_no_completion_when_no_estimates() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        // No speed data, so no reliable estimates
        let tasks = vec![create_test_task(
            "task1",
            "file1.txt",
            DownloadState::Queued,
            0.0,
        )];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        // Should have no estimated completion
        assert!(prediction.estimated_completion.is_none());
    }

    // --- Active Downloads Counting Tests ---

    #[tokio::test]
    async fn test_predict_active_downloads_counting() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        let tasks = vec![
            create_test_task("t1", "f1.txt", DownloadState::Downloading, 0.3),
            create_test_task("t2", "f2.txt", DownloadState::Downloading, 0.5),
            create_test_task("t3", "f3.txt", DownloadState::Downloading, 0.7),
            create_test_task("t4", "f4.txt", DownloadState::Queued, 0.0),
            create_test_task("t5", "f5.txt", DownloadState::Paused, 0.2),
        ];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        assert_eq!(prediction.active_downloads, 3);
        assert_eq!(prediction.max_concurrent, 2);
    }

    // --- Task Estimate Field Tests ---

    #[tokio::test]
    async fn test_task_estimate_fields_preserved() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        for _ in 0..5 {
            eta_estimator.update_speed("task1", 1_000_000.0).await;
        }

        let mut task = create_test_task("task1", "test_file.txt", DownloadState::Downloading, 0.45);
        task.priority = DownloadPriority::High;

        let prediction = predictor.predict(&[task], &eta_estimator, 2).await;

        assert_eq!(prediction.task_estimates.len(), 1);
        let estimate = &prediction.task_estimates[0];
        assert_eq!(estimate.task_id, "task1");
        assert_eq!(estimate.task_name, "test_file.txt");
        // progress() returns 0-100 percentage, allow some tolerance
        assert!(estimate.progress >= 40.0 && estimate.progress <= 50.0);
        assert!(estimate.eta_seconds.is_some());
        assert!(estimate.confidence > 0.0);
        assert!(estimate.is_active);
        assert_eq!(estimate.priority, DownloadPriority::High);
    }

    // --- Zero Size Task Tests ---

    #[tokio::test]
    async fn test_predict_zero_size_task() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        let mut task = create_test_task("task1", "empty.txt", DownloadState::Downloading, 0.0);
        task.size = 0;
        task.downloaded = 0;

        let prediction = predictor.predict(&[task], &eta_estimator, 2).await;

        // Zero-size task should still be included
        assert_eq!(prediction.task_count, 1);
    }

    // --- Unicode Task Name Tests ---

    #[tokio::test]
    async fn test_predict_unicode_task_names() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        for _ in 0..5 {
            eta_estimator.update_speed("task1", 1_000_000.0).await;
        }

        let tasks = vec![
            create_test_task("task1", "中文文件.txt", DownloadState::Downloading, 0.5),
            create_test_task("task2", "日本語ファイル.txt", DownloadState::Queued, 0.0),
            create_test_task("task3", "🔥emoji🎉.txt", DownloadState::Queued, 0.0),
        ];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        // All 3 tasks should be present (order depends on priority/progress sorting)
        let names: Vec<&str> = prediction
            .task_estimates
            .iter()
            .map(|t| t.task_name.as_str())
            .collect();
        assert!(names.contains(&"中文文件.txt"));
        assert!(names.contains(&"日本語ファイル.txt"));
        assert!(names.contains(&"🔥emoji🎉.txt"));
    }

    // --- Integration Test: Full Workflow ---

    #[tokio::test]
    async fn test_full_prediction_workflow() {
        let predictor = QueueCompletionPredictor::new();
        let eta_estimator = EtaEstimator::new();

        // Simulate speed data for multiple tasks
        for _ in 0..10 {
            eta_estimator.update_speed("task1", 2_000_000.0).await;
            eta_estimator.update_speed("task2", 1_500_000.0).await;
            eta_estimator.update_speed("task3", 500_000.0).await;
        }

        let tasks = vec![
            create_test_task("task1", "fast_file.txt", DownloadState::Downloading, 0.6),
            create_test_task("task2", "medium_file.txt", DownloadState::Downloading, 0.3),
            create_test_task("task3", "slow_file.txt", DownloadState::Queued, 0.0),
            create_test_task("task4", "completed.txt", DownloadState::Complete, 1.0),
        ];

        let prediction = predictor.predict(&tasks, &eta_estimator, 2).await;

        // Verify prediction structure
        assert_eq!(prediction.task_count, 3); // Excludes completed
        assert_eq!(prediction.active_downloads, 2);
        assert_eq!(prediction.max_concurrent, 2);
        assert!(prediction.total_eta_seconds > 0.0);
        assert!(prediction.total_eta_seconds.is_finite());
        assert!(prediction.estimated_completion.is_some());
        assert!(prediction.confidence > 0.0);
        assert!(prediction.reliable_estimates > 0);
        assert!(!prediction.summary.is_empty());

        // Verify task estimates
        assert_eq!(prediction.task_estimates.len(), 3);
        for estimate in &prediction.task_estimates {
            assert!(!estimate.task_id.is_empty());
            assert!(!estimate.task_name.is_empty());
            // progress() returns 0-100 percentage
            assert!(estimate.progress >= 0.0 && estimate.progress <= 100.0);
        }
    }
}
