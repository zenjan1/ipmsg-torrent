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
}
