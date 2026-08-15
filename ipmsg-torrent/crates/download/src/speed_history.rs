//! Per-task speed history tracking
//!
//! Stores speed samples over time for each download task,
//! enabling trend analysis and visualization.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single speed sample for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedSample {
    /// Timestamp when this sample was recorded
    pub timestamp: DateTime<Utc>,
    /// Download speed in bytes per second
    pub speed_bps: f64,
    /// Total bytes downloaded at this point
    pub downloaded: u64,
}

/// Speed history for a single task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpeedHistory {
    /// Task ID
    pub task_id: String,
    /// Speed samples (newest last)
    pub samples: Vec<SpeedSample>,
    /// Maximum number of samples to keep
    pub max_samples: usize,
}

impl TaskSpeedHistory {
    /// Create a new speed history for a task
    pub fn new(task_id: String, max_samples: usize) -> Self {
        Self {
            task_id,
            samples: Vec::new(),
            max_samples,
        }
    }

    /// Add a new speed sample
    pub fn add_sample(&mut self, speed_bps: f64, downloaded: u64) {
        self.samples.push(SpeedSample {
            timestamp: Utc::now(),
            speed_bps,
            downloaded,
        });

        // Keep only the most recent samples
        if self.samples.len() > self.max_samples {
            self.samples.remove(0);
        }
    }

    /// Get the most recent sample
    pub fn latest(&self) -> Option<&SpeedSample> {
        self.samples.last()
    }

    /// Get samples within a time window (seconds from now)
    pub fn samples_in_window(&self, window_seconds: u64) -> Vec<&SpeedSample> {
        let cutoff = Utc::now() - chrono::Duration::seconds(window_seconds as i64);
        self.samples
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .collect()
    }

    /// Calculate average speed over a time window
    pub fn avg_speed_in_window(&self, window_seconds: u64) -> f64 {
        let samples = self.samples_in_window(window_seconds);
        if samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = samples.iter().map(|s| s.speed_bps).sum();
        sum / samples.len() as f64
    }

    /// Calculate peak speed over a time window
    pub fn peak_speed_in_window(&self, window_seconds: u64) -> f64 {
        self.samples_in_window(window_seconds)
            .iter()
            .map(|s| s.speed_bps)
            .fold(0.0f64, f64::max)
    }

    /// Get total number of samples
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Clear all samples
    pub fn clear(&mut self) {
        self.samples.clear();
    }
}

/// Manager for all task speed histories
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpeedHistoryManager {
    /// Per-task speed histories
    pub histories: HashMap<String, TaskSpeedHistory>,
    /// Default maximum samples per task
    pub default_max_samples: usize,
}

impl SpeedHistoryManager {
    /// Create a new speed history manager
    pub fn new(default_max_samples: usize) -> Self {
        Self {
            histories: HashMap::new(),
            default_max_samples,
        }
    }

    /// Get or create history for a task
    pub fn get_or_create(&mut self, task_id: &str) -> &mut TaskSpeedHistory {
        if !self.histories.contains_key(task_id) {
            self.histories.insert(
                task_id.to_string(),
                TaskSpeedHistory::new(task_id.to_string(), self.default_max_samples),
            );
        }
        self.histories.get_mut(task_id).unwrap()
    }

    /// Add a speed sample for a task
    pub fn add_sample(&mut self, task_id: &str, speed_bps: f64, downloaded: u64) {
        self.get_or_create(task_id)
            .add_sample(speed_bps, downloaded);
    }

    /// Get history for a task
    pub fn get(&self, task_id: &str) -> Option<&TaskSpeedHistory> {
        self.histories.get(task_id)
    }

    /// Get mutable history for a task
    pub fn get_mut(&mut self, task_id: &str) -> Option<&mut TaskSpeedHistory> {
        self.histories.get_mut(task_id)
    }

    /// Remove history for a task
    pub fn remove(&mut self, task_id: &str) -> bool {
        self.histories.remove(task_id).is_some()
    }

    /// List all task IDs with speed history
    pub fn list_task_ids(&self) -> Vec<&String> {
        self.histories.keys().collect()
    }

    /// Get summary statistics for a task
    pub fn get_summary(&self, task_id: &str) -> Option<SpeedHistorySummary> {
        self.histories.get(task_id).map(|h| {
            let avg_5min = h.avg_speed_in_window(300);
            let avg_15min = h.avg_speed_in_window(900);
            let avg_1h = h.avg_speed_in_window(3600);
            let peak = h.samples.iter().map(|s| s.speed_bps).fold(0.0f64, f64::max);

            SpeedHistorySummary {
                task_id: task_id.to_string(),
                sample_count: h.sample_count(),
                latest_speed: h.latest().map(|s| s.speed_bps).unwrap_or(0.0),
                avg_5min,
                avg_15min,
                avg_1h,
                peak_speed: peak,
            }
        })
    }
}

/// Summary statistics for a task's speed history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedHistorySummary {
    /// Task ID
    pub task_id: String,
    /// Number of samples recorded
    pub sample_count: usize,
    /// Most recent speed (bytes/sec)
    pub latest_speed: f64,
    /// Average speed over last 5 minutes (bytes/sec)
    pub avg_5min: f64,
    /// Average speed over last 15 minutes (bytes/sec)
    pub avg_15min: f64,
    /// Average speed over last 1 hour (bytes/sec)
    pub avg_1h: f64,
    /// Peak speed ever recorded (bytes/sec)
    pub peak_speed: f64,
}

impl SpeedHistorySummary {
    /// Format as human-readable string
    pub fn format_summary(&self) -> String {
        format!(
            "Speed History: {} samples\n\
             Latest: {:.1} KB/s\n\
             Avg 5min: {:.1} KB/s | Avg 15min: {:.1} KB/s | Avg 1h: {:.1} KB/s\n\
             Peak: {:.1} KB/s",
            self.sample_count,
            self.latest_speed / 1024.0,
            self.avg_5min / 1024.0,
            self.avg_15min / 1024.0,
            self.avg_1h / 1024.0,
            self.peak_speed / 1024.0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_speed_sample_creation() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: 1024.0,
            downloaded: 10000,
        };
        assert_eq!(sample.speed_bps, 1024.0);
        assert_eq!(sample.downloaded, 10000);
    }

    #[test]
    fn test_task_speed_history_new() {
        let history = TaskSpeedHistory::new("task1".to_string(), 100);
        assert_eq!(history.task_id, "task1");
        assert_eq!(history.max_samples, 100);
        assert_eq!(history.sample_count(), 0);
    }

    #[test]
    fn test_task_speed_history_add_sample() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        history.add_sample(2048.0, 2000);

        assert_eq!(history.sample_count(), 2);
        assert_eq!(history.latest().unwrap().speed_bps, 2048.0);
        assert_eq!(history.latest().unwrap().downloaded, 2000);
    }

    #[test]
    fn test_task_speed_history_max_samples() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 5);

        for i in 0..10 {
            history.add_sample(i as f64 * 100.0, i * 1000);
        }

        assert_eq!(history.sample_count(), 5);
        // Should keep the last 5 samples (indices 5-9)
        assert_eq!(history.samples[0].downloaded, 5000);
        assert_eq!(history.samples[4].downloaded, 9000);
    }

    #[test]
    fn test_task_speed_history_clear() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        history.add_sample(2048.0, 2000);

        history.clear();
        assert_eq!(history.sample_count(), 0);
    }

    #[test]
    fn test_task_speed_history_avg_speed() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1000.0, 1000);
        history.add_sample(2000.0, 2000);
        history.add_sample(3000.0, 3000);

        let avg = history.avg_speed_in_window(3600);
        assert!((avg - 2000.0).abs() < 0.01);
    }

    #[test]
    fn test_task_speed_history_peak_speed() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1000.0, 1000);
        history.add_sample(5000.0, 2000);
        history.add_sample(3000.0, 3000);

        let peak = history.peak_speed_in_window(3600);
        assert_eq!(peak, 5000.0);
    }

    #[test]
    fn test_speed_history_manager_new() {
        let manager = SpeedHistoryManager::new(100);
        assert_eq!(manager.default_max_samples, 100);
        assert_eq!(manager.histories.len(), 0);
    }

    #[test]
    fn test_speed_history_manager_add_sample() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        manager.add_sample("task1", 2048.0, 2000);

        let history = manager.get("task1").unwrap();
        assert_eq!(history.sample_count(), 2);
    }

    #[test]
    fn test_speed_history_manager_remove() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);

        assert!(manager.remove("task1"));
        assert!(!manager.remove("task1"));
        assert!(manager.get("task1").is_none());
    }

    #[test]
    fn test_speed_history_manager_list_task_ids() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        manager.add_sample("task2", 2048.0, 2000);
        manager.add_sample("task3", 3072.0, 3000);

        let mut ids = manager.list_task_ids();
        ids.sort();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], "task1");
        assert_eq!(ids[1], "task2");
        assert_eq!(ids[2], "task3");
    }

    #[test]
    fn test_speed_history_summary() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1000.0, 1000);
        manager.add_sample("task1", 2000.0, 2000);
        manager.add_sample("task1", 3000.0, 3000);

        let summary = manager.get_summary("task1").unwrap();
        assert_eq!(summary.sample_count, 3);
        assert_eq!(summary.latest_speed, 3000.0);
        assert_eq!(summary.peak_speed, 3000.0);
        assert!((summary.avg_5min - 2000.0).abs() < 0.01);
    }

    #[test]
    fn test_speed_history_summary_format() {
        let summary = SpeedHistorySummary {
            task_id: "task1".to_string(),
            sample_count: 100,
            latest_speed: 1024.0,
            avg_5min: 2048.0,
            avg_15min: 1536.0,
            avg_1h: 1280.0,
            peak_speed: 5120.0,
        };

        let formatted = summary.format_summary();
        assert!(formatted.contains("100 samples"));
        assert!(formatted.contains("1.0 KB/s"));
        assert!(formatted.contains("5.0 KB/s"));
    }

    #[test]
    fn test_speed_history_manager_get_summary_nonexistent() {
        let manager = SpeedHistoryManager::new(100);
        assert!(manager.get_summary("nonexistent").is_none());
    }

    #[test]
    fn test_task_speed_history_empty_window() {
        let history = TaskSpeedHistory::new("task1".to_string(), 100);
        assert_eq!(history.avg_speed_in_window(3600), 0.0);
        assert_eq!(history.peak_speed_in_window(3600), 0.0);
        assert_eq!(history.samples_in_window(3600).len(), 0);
    }

    // ============================================================================
    // Comprehensive Test Coverage - Phase 232
    // ============================================================================

    // === SpeedSample Serialization ===

    #[test]
    fn test_speed_sample_serde_roundtrip() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: 1024.5,
            downloaded: 10000,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let deserialized: SpeedSample = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.speed_bps, 1024.5);
        assert_eq!(deserialized.downloaded, 10000);
    }

    #[test]
    fn test_speed_sample_serde_zero_values() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: 0.0,
            downloaded: 0,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let deserialized: SpeedSample = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.speed_bps, 0.0);
        assert_eq!(deserialized.downloaded, 0);
    }

    #[test]
    fn test_speed_sample_serde_large_values() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: f64::MAX,
            downloaded: u64::MAX,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let deserialized: SpeedSample = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.speed_bps, f64::MAX);
        assert_eq!(deserialized.downloaded, u64::MAX);
    }

    #[test]
    fn test_speed_sample_serde_extra_fields_ignored() {
        let json = r#"{"timestamp":"2026-08-15T12:00:00Z","speed_bps":1024.0,"downloaded":1000,"extra_field":"ignored"}"#;
        let sample: SpeedSample = serde_json::from_str(json).unwrap();
        assert_eq!(sample.speed_bps, 1024.0);
        assert_eq!(sample.downloaded, 1000);
    }

    #[test]
    fn test_speed_sample_serde_pretty() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: 2048.0,
            downloaded: 5000,
        };
        let pretty = serde_json::to_string_pretty(&sample).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: SpeedSample = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.speed_bps, 2048.0);
    }

    // === SpeedSample Traits ===

    #[test]
    fn test_speed_sample_clone() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: 1024.0,
            downloaded: 1000,
        };
        let cloned = sample.clone();
        assert_eq!(cloned.speed_bps, 1024.0);
        assert_eq!(cloned.downloaded, 1000);
    }

    #[test]
    fn test_speed_sample_debug() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: 1024.0,
            downloaded: 1000,
        };
        let debug = format!("{:?}", sample);
        assert!(debug.contains("SpeedSample"));
        assert!(debug.contains("1024"));
    }

    // === TaskSpeedHistory Serialization ===

    #[test]
    fn test_task_speed_history_serde_roundtrip() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        history.add_sample(2048.0, 2000);

        let json = serde_json::to_string(&history).unwrap();
        let deserialized: TaskSpeedHistory = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task1");
        assert_eq!(deserialized.max_samples, 100);
        assert_eq!(deserialized.sample_count(), 2);
    }

    #[test]
    fn test_task_speed_history_serde_empty() {
        let history = TaskSpeedHistory::new("task1".to_string(), 50);
        let json = serde_json::to_string(&history).unwrap();
        let deserialized: TaskSpeedHistory = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sample_count(), 0);
        assert_eq!(deserialized.max_samples, 50);
    }

    #[test]
    fn test_task_speed_history_serde_extra_fields_ignored() {
        let json = r#"{"task_id":"task1","samples":[],"max_samples":100,"extra":"ignored"}"#;
        let history: TaskSpeedHistory = serde_json::from_str(json).unwrap();
        assert_eq!(history.task_id, "task1");
        assert_eq!(history.max_samples, 100);
    }

    #[test]
    fn test_task_speed_history_serde_pretty() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        let pretty = serde_json::to_string_pretty(&history).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: TaskSpeedHistory = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.sample_count(), 1);
    }

    // === TaskSpeedHistory Traits ===

    #[test]
    fn test_task_speed_history_clone() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);

        let cloned = history.clone();
        assert_eq!(cloned.task_id, "task1");
        assert_eq!(cloned.sample_count(), 1);
        assert_eq!(cloned.max_samples, 100);
    }

    #[test]
    fn test_task_speed_history_clone_independence() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);

        let mut cloned = history.clone();
        cloned.add_sample(2048.0, 2000);

        assert_eq!(history.sample_count(), 1);
        assert_eq!(cloned.sample_count(), 2);
    }

    #[test]
    fn test_task_speed_history_debug() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);

        let debug = format!("{:?}", history);
        assert!(debug.contains("TaskSpeedHistory"));
        assert!(debug.contains("task1"));
        assert!(debug.contains("1024"));
    }

    // === TaskSpeedHistory Boundary Tests ===

    #[test]
    fn test_task_speed_history_new_zero_max_samples() {
        let history = TaskSpeedHistory::new("task1".to_string(), 0);
        assert_eq!(history.max_samples, 0);
        assert_eq!(history.sample_count(), 0);
    }

    #[test]
    fn test_task_speed_history_add_sample_zero_max_samples() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 0);
        history.add_sample(1024.0, 1000);
        // With max_samples=0, samples should be removed immediately
        assert_eq!(history.sample_count(), 0);
    }

    #[test]
    fn test_task_speed_history_add_sample_max_samples_one() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 1);
        history.add_sample(1024.0, 1000);
        assert_eq!(history.sample_count(), 1);
        history.add_sample(2048.0, 2000);
        assert_eq!(history.sample_count(), 1);
        assert_eq!(history.latest().unwrap().speed_bps, 2048.0);
    }

    #[test]
    fn test_task_speed_history_add_sample_zero_speed() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(0.0, 1000);
        assert_eq!(history.sample_count(), 1);
        assert_eq!(history.latest().unwrap().speed_bps, 0.0);
    }

    #[test]
    fn test_task_speed_history_add_sample_negative_speed() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(-100.0, 1000);
        assert_eq!(history.sample_count(), 1);
        assert_eq!(history.latest().unwrap().speed_bps, -100.0);
    }

    #[test]
    fn test_task_speed_history_add_sample_large_values() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(f64::MAX, u64::MAX);
        assert_eq!(history.sample_count(), 1);
        assert_eq!(history.latest().unwrap().speed_bps, f64::MAX);
        assert_eq!(history.latest().unwrap().downloaded, u64::MAX);
    }

    #[test]
    fn test_task_speed_history_latest_empty() {
        let history = TaskSpeedHistory::new("task1".to_string(), 100);
        assert!(history.latest().is_none());
    }

    #[test]
    fn test_task_speed_history_latest_single_sample() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        let latest = history.latest().unwrap();
        assert_eq!(latest.speed_bps, 1024.0);
        assert_eq!(latest.downloaded, 1000);
    }

    #[test]
    fn test_task_speed_history_samples_in_window_zero_window() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        // Zero second window should return empty (samples are in the past)
        let samples = history.samples_in_window(0);
        assert_eq!(samples.len(), 0);
    }

    #[test]
    fn test_task_speed_history_samples_in_window_large_window() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        history.add_sample(2048.0, 2000);
        // Large window should include all samples
        let samples = history.samples_in_window(3600);
        assert_eq!(samples.len(), 2);
    }

    #[test]
    fn test_task_speed_history_avg_speed_empty() {
        let history = TaskSpeedHistory::new("task1".to_string(), 100);
        assert_eq!(history.avg_speed_in_window(3600), 0.0);
    }

    #[test]
    fn test_task_speed_history_avg_speed_single_sample() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        let avg = history.avg_speed_in_window(3600);
        assert_eq!(avg, 1024.0);
    }

    #[test]
    fn test_task_speed_history_avg_speed_zero_speeds() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(0.0, 1000);
        history.add_sample(0.0, 2000);
        let avg = history.avg_speed_in_window(3600);
        assert_eq!(avg, 0.0);
    }

    #[test]
    fn test_task_speed_history_peak_speed_empty() {
        let history = TaskSpeedHistory::new("task1".to_string(), 100);
        assert_eq!(history.peak_speed_in_window(3600), 0.0);
    }

    #[test]
    fn test_task_speed_history_peak_speed_single_sample() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        let peak = history.peak_speed_in_window(3600);
        assert_eq!(peak, 1024.0);
    }

    #[test]
    fn test_task_speed_history_peak_speed_negative_speeds() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.add_sample(-100.0, 1000);
        history.add_sample(-50.0, 2000);
        // Peak of negative values should be 0.0 (from fold initial value)
        let peak = history.peak_speed_in_window(3600);
        assert_eq!(peak, 0.0);
    }

    #[test]
    fn test_task_speed_history_clear_empty() {
        let mut history = TaskSpeedHistory::new("task1".to_string(), 100);
        history.clear();
        assert_eq!(history.sample_count(), 0);
    }

    #[test]
    fn test_task_speed_history_unicode_task_id() {
        let mut history = TaskSpeedHistory::new("任务1".to_string(), 100);
        history.add_sample(1024.0, 1000);
        assert_eq!(history.task_id, "任务1");
        assert_eq!(history.sample_count(), 1);
    }

    #[test]
    fn test_task_speed_history_emoji_task_id() {
        let mut history = TaskSpeedHistory::new("🚀task".to_string(), 100);
        history.add_sample(1024.0, 1000);
        assert_eq!(history.task_id, "🚀task");
    }

    // === SpeedHistoryManager Serialization ===

    #[test]
    fn test_speed_history_manager_serde_roundtrip() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        manager.add_sample("task2", 2048.0, 2000);

        let json = serde_json::to_string(&manager).unwrap();
        let deserialized: SpeedHistoryManager = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.default_max_samples, 100);
        assert_eq!(deserialized.histories.len(), 2);
        assert!(deserialized.get("task1").is_some());
        assert!(deserialized.get("task2").is_some());
    }

    #[test]
    fn test_speed_history_manager_serde_empty() {
        let manager = SpeedHistoryManager::new(50);
        let json = serde_json::to_string(&manager).unwrap();
        let deserialized: SpeedHistoryManager = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.default_max_samples, 50);
        assert_eq!(deserialized.histories.len(), 0);
    }

    #[test]
    fn test_speed_history_manager_serde_extra_fields_ignored() {
        let json = r#"{"histories":{},"default_max_samples":100,"extra":"ignored"}"#;
        let manager: SpeedHistoryManager = serde_json::from_str(json).unwrap();
        assert_eq!(manager.default_max_samples, 100);
    }

    #[test]
    fn test_speed_history_manager_serde_pretty() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        let pretty = serde_json::to_string_pretty(&manager).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: SpeedHistoryManager = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.histories.len(), 1);
    }

    // === SpeedHistoryManager Traits ===

    #[test]
    fn test_speed_history_manager_clone() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);

        let cloned = manager.clone();
        assert_eq!(cloned.default_max_samples, 100);
        assert_eq!(cloned.histories.len(), 1);
    }

    #[test]
    fn test_speed_history_manager_clone_independence() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);

        let mut cloned = manager.clone();
        cloned.add_sample("task2", 2048.0, 2000);

        assert_eq!(manager.histories.len(), 1);
        assert_eq!(cloned.histories.len(), 2);
    }

    #[test]
    fn test_speed_history_manager_debug() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);

        let debug = format!("{:?}", manager);
        assert!(debug.contains("SpeedHistoryManager"));
        assert!(debug.contains("task1"));
    }

    #[test]
    fn test_speed_history_manager_default() {
        let manager = SpeedHistoryManager::default();
        assert_eq!(manager.default_max_samples, 0);
        assert_eq!(manager.histories.len(), 0);
    }

    // === SpeedHistoryManager Boundary Tests ===

    #[test]
    fn test_speed_history_manager_new_zero_max_samples() {
        let manager = SpeedHistoryManager::new(0);
        assert_eq!(manager.default_max_samples, 0);
    }

    #[test]
    fn test_speed_history_manager_get_or_create_new_task() {
        let mut manager = SpeedHistoryManager::new(100);
        let history = manager.get_or_create("task1");
        assert_eq!(history.task_id, "task1");
        assert_eq!(history.max_samples, 100);
        assert_eq!(history.sample_count(), 0);
    }

    #[test]
    fn test_speed_history_manager_get_or_create_existing_task() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);

        let history = manager.get_or_create("task1");
        assert_eq!(history.sample_count(), 1);
    }

    #[test]
    fn test_speed_history_manager_add_sample_creates_history() {
        let mut manager = SpeedHistoryManager::new(100);
        assert!(manager.get("task1").is_none());

        manager.add_sample("task1", 1024.0, 1000);
        assert!(manager.get("task1").is_some());
        assert_eq!(manager.get("task1").unwrap().sample_count(), 1);
    }

    #[test]
    fn test_speed_history_manager_get_exists() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        assert!(manager.get("task1").is_some());
    }

    #[test]
    fn test_speed_history_manager_get_not_exists() {
        let manager = SpeedHistoryManager::new(100);
        assert!(manager.get("task1").is_none());
    }

    #[test]
    fn test_speed_history_manager_get_mut_exists() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        let history = manager.get_mut("task1").unwrap();
        history.add_sample(2048.0, 2000);
        assert_eq!(history.sample_count(), 2);
    }

    #[test]
    fn test_speed_history_manager_get_mut_not_exists() {
        let mut manager = SpeedHistoryManager::new(100);
        assert!(manager.get_mut("task1").is_none());
    }

    #[test]
    fn test_speed_history_manager_remove_exists() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        assert!(manager.remove("task1"));
        assert!(manager.get("task1").is_none());
    }

    #[test]
    fn test_speed_history_manager_remove_not_exists() {
        let mut manager = SpeedHistoryManager::new(100);
        assert!(!manager.remove("task1"));
    }

    #[test]
    fn test_speed_history_manager_remove_idempotent() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        assert!(manager.remove("task1"));
        assert!(!manager.remove("task1"));
    }

    #[test]
    fn test_speed_history_manager_list_task_ids_empty() {
        let manager = SpeedHistoryManager::new(100);
        assert_eq!(manager.list_task_ids().len(), 0);
    }

    #[test]
    fn test_speed_history_manager_list_task_ids_multiple() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        manager.add_sample("task2", 2048.0, 2000);
        manager.add_sample("task3", 3072.0, 3000);

        let ids = manager.list_task_ids();
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn test_speed_history_manager_get_summary_exists() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1000.0, 1000);
        manager.add_sample("task1", 2000.0, 2000);
        manager.add_sample("task1", 3000.0, 3000);

        let summary = manager.get_summary("task1").unwrap();
        assert_eq!(summary.task_id, "task1");
        assert_eq!(summary.sample_count, 3);
        assert_eq!(summary.latest_speed, 3000.0);
        assert_eq!(summary.peak_speed, 3000.0);
    }

    #[test]
    fn test_speed_history_manager_get_summary_not_exists() {
        let manager = SpeedHistoryManager::new(100);
        assert!(manager.get_summary("task1").is_none());
    }

    #[test]
    fn test_speed_history_manager_get_summary_empty_history() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.get_or_create("task1");

        let summary = manager.get_summary("task1").unwrap();
        assert_eq!(summary.sample_count, 0);
        assert_eq!(summary.latest_speed, 0.0);
        assert_eq!(summary.peak_speed, 0.0);
    }

    #[test]
    fn test_speed_history_manager_unicode_task_id() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("任务1", 1024.0, 1000);
        manager.add_sample("🚀task", 2048.0, 2000);

        assert!(manager.get("任务1").is_some());
        assert!(manager.get("🚀task").is_some());
    }

    // === SpeedHistorySummary Serialization ===

    #[test]
    fn test_speed_history_summary_serde_roundtrip() {
        let summary = SpeedHistorySummary {
            task_id: "task1".to_string(),
            sample_count: 100,
            latest_speed: 1024.0,
            avg_5min: 2048.0,
            avg_15min: 1536.0,
            avg_1h: 1280.0,
            peak_speed: 5120.0,
        };

        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: SpeedHistorySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task1");
        assert_eq!(deserialized.sample_count, 100);
        assert_eq!(deserialized.latest_speed, 1024.0);
    }

    #[test]
    fn test_speed_history_summary_serde_zero_values() {
        let summary = SpeedHistorySummary {
            task_id: "task1".to_string(),
            sample_count: 0,
            latest_speed: 0.0,
            avg_5min: 0.0,
            avg_15min: 0.0,
            avg_1h: 0.0,
            peak_speed: 0.0,
        };

        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: SpeedHistorySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sample_count, 0);
    }

    #[test]
    fn test_speed_history_summary_serde_extra_fields_ignored() {
        let json = r#"{"task_id":"task1","sample_count":10,"latest_speed":1024.0,"avg_5min":0.0,"avg_15min":0.0,"avg_1h":0.0,"peak_speed":2048.0,"extra":"ignored"}"#;
        let summary: SpeedHistorySummary = serde_json::from_str(json).unwrap();
        assert_eq!(summary.task_id, "task1");
        assert_eq!(summary.sample_count, 10);
    }

    // === SpeedHistorySummary Traits ===

    #[test]
    fn test_speed_history_summary_clone() {
        let summary = SpeedHistorySummary {
            task_id: "task1".to_string(),
            sample_count: 100,
            latest_speed: 1024.0,
            avg_5min: 2048.0,
            avg_15min: 1536.0,
            avg_1h: 1280.0,
            peak_speed: 5120.0,
        };

        let cloned = summary.clone();
        assert_eq!(cloned.task_id, "task1");
        assert_eq!(cloned.sample_count, 100);
    }

    #[test]
    fn test_speed_history_summary_debug() {
        let summary = SpeedHistorySummary {
            task_id: "task1".to_string(),
            sample_count: 100,
            latest_speed: 1024.0,
            avg_5min: 2048.0,
            avg_15min: 1536.0,
            avg_1h: 1280.0,
            peak_speed: 5120.0,
        };

        let debug = format!("{:?}", summary);
        assert!(debug.contains("SpeedHistorySummary"));
        assert!(debug.contains("task1"));
    }

    // === SpeedHistorySummary format_summary ===

    #[test]
    fn test_speed_history_summary_format_zero_values() {
        let summary = SpeedHistorySummary {
            task_id: "task1".to_string(),
            sample_count: 0,
            latest_speed: 0.0,
            avg_5min: 0.0,
            avg_15min: 0.0,
            avg_1h: 0.0,
            peak_speed: 0.0,
        };

        let formatted = summary.format_summary();
        assert!(formatted.contains("0 samples"));
        assert!(formatted.contains("0.0 KB/s"));
    }

    #[test]
    fn test_speed_history_summary_format_large_values() {
        let summary = SpeedHistorySummary {
            task_id: "task1".to_string(),
            sample_count: 10000,
            latest_speed: 1048576.0, // 1 MB/s
            avg_5min: 2097152.0,     // 2 MB/s
            avg_15min: 1572864.0,    // 1.5 MB/s
            avg_1h: 1048576.0,       // 1 MB/s
            peak_speed: 5242880.0,   // 5 MB/s
        };

        let formatted = summary.format_summary();
        assert!(formatted.contains("10000 samples"));
        assert!(formatted.contains("1024.0 KB/s")); // 1 MB/s in KB/s
    }

    #[test]
    fn test_speed_history_summary_format_all_sections() {
        let summary = SpeedHistorySummary {
            task_id: "task1".to_string(),
            sample_count: 100,
            latest_speed: 1024.0,
            avg_5min: 2048.0,
            avg_15min: 1536.0,
            avg_1h: 1280.0,
            peak_speed: 5120.0,
        };

        let formatted = summary.format_summary();
        assert!(formatted.contains("Speed History:"));
        assert!(formatted.contains("samples"));
        assert!(formatted.contains("Latest:"));
        assert!(formatted.contains("Avg 5min:"));
        assert!(formatted.contains("Avg 15min:"));
        assert!(formatted.contains("Avg 1h:"));
        assert!(formatted.contains("Peak:"));
    }

    #[test]
    fn test_speed_history_summary_format_unicode_task_id() {
        let summary = SpeedHistorySummary {
            task_id: "任务1".to_string(),
            sample_count: 10,
            latest_speed: 1024.0,
            avg_5min: 1024.0,
            avg_15min: 1024.0,
            avg_1h: 1024.0,
            peak_speed: 2048.0,
        };

        let formatted = summary.format_summary();
        // Task ID is not in the format string, but the summary should still work
        assert!(formatted.contains("10 samples"));
    }

    // === Complex Workflow Tests ===

    #[test]
    fn test_speed_history_complete_lifecycle() {
        let mut manager = SpeedHistoryManager::new(100);

        // Add samples for multiple tasks
        manager.add_sample("task1", 1000.0, 1000);
        manager.add_sample("task1", 2000.0, 2000);
        manager.add_sample("task2", 3000.0, 3000);

        // Verify state
        assert_eq!(manager.histories.len(), 2);
        assert_eq!(manager.get("task1").unwrap().sample_count(), 2);
        assert_eq!(manager.get("task2").unwrap().sample_count(), 1);

        // Get summary
        let summary = manager.get_summary("task1").unwrap();
        assert_eq!(summary.sample_count, 2);
        assert_eq!(summary.latest_speed, 2000.0);

        // Remove task
        assert!(manager.remove("task1"));
        assert_eq!(manager.histories.len(), 1);
        assert!(manager.get("task1").is_none());

        // List remaining
        let ids = manager.list_task_ids();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn test_speed_history_multiple_tasks_independent() {
        let mut manager = SpeedHistoryManager::new(100);

        manager.add_sample("task1", 1000.0, 1000);
        manager.add_sample("task2", 2000.0, 2000);
        manager.add_sample("task3", 3000.0, 3000);

        // Each task should have independent history
        assert_eq!(manager.get("task1").unwrap().sample_count(), 1);
        assert_eq!(manager.get("task2").unwrap().sample_count(), 1);
        assert_eq!(manager.get("task3").unwrap().sample_count(), 1);

        // Remove one shouldn't affect others
        manager.remove("task2");
        assert_eq!(manager.histories.len(), 2);
        assert!(manager.get("task1").is_some());
        assert!(manager.get("task2").is_none());
        assert!(manager.get("task3").is_some());
    }

    #[test]
    fn test_speed_history_max_samples_enforcement() {
        let mut manager = SpeedHistoryManager::new(5);

        // Add more samples than max
        for i in 0..10 {
            manager.add_sample("task1", i as f64 * 100.0, i * 1000);
        }

        // Should only keep the last 5
        let history = manager.get("task1").unwrap();
        assert_eq!(history.sample_count(), 5);
        assert_eq!(history.samples[0].downloaded, 5000);
        assert_eq!(history.samples[4].downloaded, 9000);
    }

    #[test]
    fn test_speed_history_persistence_roundtrip() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("task1", 1024.0, 1000);
        manager.add_sample("task1", 2048.0, 2000);
        manager.add_sample("task2", 3072.0, 3000);

        // Serialize
        let json = serde_json::to_string(&manager).unwrap();

        // Deserialize
        let loaded: SpeedHistoryManager = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.default_max_samples, 100);
        assert_eq!(loaded.histories.len(), 2);
        assert_eq!(loaded.get("task1").unwrap().sample_count(), 2);
        assert_eq!(loaded.get("task2").unwrap().sample_count(), 1);
    }

    #[test]
    fn test_speed_history_unicode_persistence() {
        let mut manager = SpeedHistoryManager::new(100);
        manager.add_sample("任务1", 1024.0, 1000);
        manager.add_sample("🚀task", 2048.0, 2000);

        let json = serde_json::to_string(&manager).unwrap();
        let loaded: SpeedHistoryManager = serde_json::from_str(&json).unwrap();

        assert!(loaded.get("任务1").is_some());
        assert!(loaded.get("🚀task").is_some());
    }
}
