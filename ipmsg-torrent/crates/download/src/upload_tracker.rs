//! Upload speed and bytes tracking system (Phase 133)
//!
//! Tracks upload speed per task and globally, with persistent configuration.
//! Upload data is maintained separately from DownloadTask to avoid struct bloat.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the upload tracker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadTrackerConfig {
    /// Whether upload tracking is enabled
    pub enabled: bool,
    /// Speed calculation window in seconds (how far back to average)
    pub speed_window_secs: u64,
    /// Maximum number of tasks to track simultaneously
    pub max_tracked_tasks: usize,
}

impl Default for UploadTrackerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            speed_window_secs: 10,
            max_tracked_tasks: 500,
        }
    }
}

/// A single upload speed sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSample {
    /// Timestamp of the sample
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Cumulative uploaded bytes at this sample time
    pub uploaded_bytes: u64,
}

/// Per-task upload tracking data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskUploadData {
    /// Task ID
    pub task_id: String,
    /// Total bytes uploaded
    pub total_uploaded: u64,
    /// Current upload speed (bytes/sec), smoothed
    pub current_speed_bps: f64,
    /// Recent speed samples for averaging
    #[serde(skip)]
    pub samples: Vec<UploadSample>,
    /// When tracking started for this task
    pub tracking_started_at: chrono::DateTime<chrono::Utc>,
    /// Last time data was recorded
    pub last_recorded_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Summary of upload tracking across all tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadTrackerSummary {
    /// Whether tracking is enabled
    pub enabled: bool,
    /// Number of tasks being tracked
    pub tracked_task_count: usize,
    /// Total bytes uploaded across all tasks
    pub total_uploaded_bytes: u64,
    /// Current aggregate upload speed (bytes/sec)
    pub current_upload_bps: f64,
    /// Top uploaders (task_id, speed_bps)
    pub top_uploaders: Vec<(String, f64)>,
    /// Human-readable summary
    pub formatted: String,
}

/// Manages upload speed and bytes tracking for download tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadTracker {
    /// Configuration
    pub config: UploadTrackerConfig,
    /// Per-task upload data
    pub tasks: HashMap<String, TaskUploadData>,
    /// Global total uploaded bytes (persisted across restarts)
    pub global_uploaded_bytes: u64,
}

impl Default for UploadTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl UploadTracker {
    /// Create a new upload tracker with default config
    pub fn new() -> Self {
        Self {
            config: UploadTrackerConfig::default(),
            tasks: HashMap::new(),
            global_uploaded_bytes: 0,
        }
    }

    /// Set configuration
    pub fn set_config(&mut self, config: UploadTrackerConfig) {
        self.config = config;
    }

    /// Get configuration
    pub fn get_config(&self) -> &UploadTrackerConfig {
        &self.config
    }

    /// Record uploaded bytes for a task
    ///
    /// This should be called periodically (e.g., every 2 seconds during speed tracking)
    /// with the cumulative uploaded bytes for the task.
    pub fn record_upload(&mut self, task_id: &str, uploaded_bytes: u64) {
        if !self.config.enabled {
            return;
        }

        let now = chrono::Utc::now();

        // Enforce max tracked tasks limit
        if !self.tasks.contains_key(task_id) && self.tasks.len() >= self.config.max_tracked_tasks {
            // Remove the oldest tracked task
            if let Some(oldest_id) = self
                .tasks
                .iter()
                .min_by_key(|(_, v)| v.last_recorded_at.unwrap_or(v.tracking_started_at))
                .map(|(k, _)| k.clone())
            {
                self.tasks.remove(&oldest_id);
            }
        }

        let task_data = self
            .tasks
            .entry(task_id.to_string())
            .or_insert_with(|| TaskUploadData {
                task_id: task_id.to_string(),
                total_uploaded: 0,
                current_speed_bps: 0.0,
                samples: Vec::new(),
                tracking_started_at: now,
                last_recorded_at: None,
            });

        let delta = if uploaded_bytes >= task_data.total_uploaded {
            uploaded_bytes - task_data.total_uploaded
        } else {
            // Reset case (task restarted)
            uploaded_bytes
        };

        task_data.total_uploaded = uploaded_bytes;
        task_data.last_recorded_at = Some(now);

        // Add sample
        task_data.samples.push(UploadSample {
            timestamp: now,
            uploaded_bytes,
        });

        // Prune old samples beyond the speed window
        let cutoff = now - chrono::Duration::seconds(self.config.speed_window_secs as i64);
        task_data.samples.retain(|s| s.timestamp >= cutoff);

        // Calculate speed from samples
        if task_data.samples.len() >= 2 {
            let oldest = &task_data.samples[0];
            let newest = &task_data.samples[task_data.samples.len() - 1];
            let elapsed = (newest.timestamp - oldest.timestamp)
                .num_milliseconds()
                .max(1) as f64
                / 1000.0;
            let bytes_diff = newest.uploaded_bytes.saturating_sub(oldest.uploaded_bytes);
            task_data.current_speed_bps = bytes_diff as f64 / elapsed;
        }

        // Update global total
        self.global_uploaded_bytes = self.tasks.values().map(|t| t.total_uploaded).sum();
        let _ = delta; // used conceptually
    }

    /// Get current upload speed for a task
    pub fn get_task_upload_speed(&self, task_id: &str) -> f64 {
        self.tasks
            .get(task_id)
            .map(|t| t.current_speed_bps)
            .unwrap_or(0.0)
    }

    /// Get total uploaded bytes for a task
    pub fn get_task_uploaded(&self, task_id: &str) -> u64 {
        self.tasks
            .get(task_id)
            .map(|t| t.total_uploaded)
            .unwrap_or(0)
    }

    /// Get current aggregate upload speed across all tasks
    pub fn get_total_upload_speed(&self) -> f64 {
        self.tasks.values().map(|t| t.current_speed_bps).sum()
    }

    /// Get total uploaded bytes across all tasks
    pub fn get_total_uploaded(&self) -> u64 {
        self.global_uploaded_bytes
    }

    /// Remove tracking data for a task
    pub fn remove_task(&mut self, task_id: &str) {
        self.tasks.remove(task_id);
        self.global_uploaded_bytes = self.tasks.values().map(|t| t.total_uploaded).sum();
    }

    /// Clear all tracking data
    pub fn clear(&mut self) {
        self.tasks.clear();
        self.global_uploaded_bytes = 0;
    }

    /// Get a summary of upload tracking
    pub fn get_summary(&self) -> UploadTrackerSummary {
        let total_uploaded = self.get_total_uploaded();
        let total_speed = self.get_total_upload_speed();

        let mut top_uploaders: Vec<(String, f64)> = self
            .tasks
            .iter()
            .filter(|(_, t)| t.current_speed_bps > 0.0)
            .map(|(id, t)| (id.clone(), t.current_speed_bps))
            .collect();
        top_uploaders.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        top_uploaders.truncate(5);

        let formatted = format!(
            "📤 Upload Tracker: {} | Tasks: {} | Speed: {} | Total: {}",
            if self.config.enabled {
                "enabled"
            } else {
                "disabled"
            },
            self.tasks.len(),
            format_speed_bps(total_speed),
            format_size(total_uploaded),
        );

        UploadTrackerSummary {
            enabled: self.config.enabled,
            tracked_task_count: self.tasks.len(),
            total_uploaded_bytes: total_uploaded,
            current_upload_bps: total_speed,
            top_uploaders,
            formatted,
        }
    }

    /// Get upload data for a specific task
    pub fn get_task_data(&self, task_id: &str) -> Option<&TaskUploadData> {
        self.tasks.get(task_id)
    }

    /// List all tracked task IDs
    pub fn list_tracked_tasks(&self) -> Vec<String> {
        self.tasks.keys().cloned().collect()
    }
}

/// Format bytes per second into human-readable string
pub fn format_speed_bps(bps: f64) -> String {
    if bps < 1024.0 {
        format!("{:.0} B/s", bps)
    } else if bps < 1024.0 * 1024.0 {
        format!("{:.1} KB/s", bps / 1024.0)
    } else if bps < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} MB/s", bps / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB/s", bps / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Format bytes into human-readable size
pub fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Save upload tracker config to disk
pub async fn save_upload_tracker_config(
    path: &std::path::Path,
    config: &UploadTrackerConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string_pretty(config)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Load upload tracker config from disk
pub async fn load_upload_tracker_config(path: &std::path::Path) -> Option<UploadTrackerConfig> {
    match tokio::fs::read_to_string(path).await {
        Ok(json) => serde_json::from_str(&json).ok(),
        Err(_) => None,
    }
}

/// Save full upload tracker state to disk (for persistence across restarts)
pub async fn save_upload_tracker_state(
    path: &std::path::Path,
    tracker: &UploadTracker,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Save without samples (they are runtime-only)
    let serializable = SerializableUploadTracker {
        config: tracker.config.clone(),
        tasks: tracker
            .tasks
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    TaskUploadData {
                        task_id: v.task_id.clone(),
                        total_uploaded: v.total_uploaded,
                        current_speed_bps: v.current_speed_bps,
                        samples: Vec::new(), // don't persist samples
                        tracking_started_at: v.tracking_started_at,
                        last_recorded_at: v.last_recorded_at,
                    },
                )
            })
            .collect(),
        global_uploaded_bytes: tracker.global_uploaded_bytes,
    };
    let json = serde_json::to_string_pretty(&serializable)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Serializable version of UploadTracker (without runtime-only fields)
#[derive(Serialize, Deserialize)]
struct SerializableUploadTracker {
    config: UploadTrackerConfig,
    tasks: HashMap<String, TaskUploadData>,
    global_uploaded_bytes: u64,
}

/// Load full upload tracker state from disk
pub async fn load_upload_tracker_state(path: &std::path::Path) -> Option<UploadTracker> {
    match tokio::fs::read_to_string(path).await {
        Ok(json) => {
            let state: SerializableUploadTracker = serde_json::from_str(&json).ok()?;
            Some(UploadTracker {
                config: state.config,
                tasks: state.tasks,
                global_uploaded_bytes: state.global_uploaded_bytes,
            })
        }
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== UploadTrackerConfig serde =====

    #[test]
    fn config_serde_roundtrip_default() {
        let config = UploadTrackerConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: UploadTrackerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.speed_window_secs, config.speed_window_secs);
        assert_eq!(loaded.max_tracked_tasks, config.max_tracked_tasks);
    }

    #[test]
    fn config_serde_roundtrip_custom() {
        let config = UploadTrackerConfig {
            enabled: false,
            speed_window_secs: 60,
            max_tracked_tasks: 100,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: UploadTrackerConfig = serde_json::from_str(&json).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.speed_window_secs, 60);
        assert_eq!(loaded.max_tracked_tasks, 100);
    }

    #[test]
    fn config_serde_pretty() {
        let config = UploadTrackerConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let loaded: UploadTrackerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, true);
    }

    #[test]
    fn config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"speed_window_secs":10,"max_tracked_tasks":500,"extra_field":"ignored","another":42}"#;
        let loaded: UploadTrackerConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.speed_window_secs, 10);
        assert_eq!(loaded.max_tracked_tasks, 500);
    }

    #[test]
    fn config_serde_missing_fields_error() {
        // serde requires all non-Option fields, so missing fields cause error
        let json = r#"{"enabled":true}"#;
        let result: Result<UploadTrackerConfig, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    // ===== UploadTrackerConfig traits =====

    #[test]
    fn config_clone() {
        let config = UploadTrackerConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.speed_window_secs, config.speed_window_secs);
        assert_eq!(cloned.max_tracked_tasks, config.max_tracked_tasks);
    }

    #[test]
    fn config_clone_independence() {
        let mut config = UploadTrackerConfig::default();
        let cloned = config.clone();
        config.enabled = false;
        assert!(cloned.enabled);
    }

    #[test]
    fn config_debug() {
        let config = UploadTrackerConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("UploadTrackerConfig"));
        assert!(debug.contains("enabled"));
    }

    #[test]
    fn config_default_values() {
        let config = UploadTrackerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.speed_window_secs, 10);
        assert_eq!(config.max_tracked_tasks, 500);
    }

    // ===== UploadSample traits =====

    #[test]
    fn upload_sample_clone() {
        let sample = UploadSample {
            timestamp: chrono::Utc::now(),
            uploaded_bytes: 1000,
        };
        let cloned = sample.clone();
        assert_eq!(cloned.uploaded_bytes, 1000);
    }

    #[test]
    fn upload_sample_debug() {
        let sample = UploadSample {
            timestamp: chrono::Utc::now(),
            uploaded_bytes: 1000,
        };
        let debug = format!("{:?}", sample);
        assert!(debug.contains("UploadSample"));
        assert!(debug.contains("1000"));
    }

    // ===== TaskUploadData traits =====

    #[test]
    fn task_upload_data_clone() {
        let data = TaskUploadData {
            task_id: "task1".to_string(),
            total_uploaded: 5000,
            current_speed_bps: 100.0,
            samples: vec![],
            tracking_started_at: chrono::Utc::now(),
            last_recorded_at: None,
        };
        let cloned = data.clone();
        assert_eq!(cloned.task_id, "task1");
        assert_eq!(cloned.total_uploaded, 5000);
    }

    #[test]
    fn task_upload_data_debug() {
        let data = TaskUploadData {
            task_id: "task1".to_string(),
            total_uploaded: 5000,
            current_speed_bps: 100.0,
            samples: vec![],
            tracking_started_at: chrono::Utc::now(),
            last_recorded_at: None,
        };
        let debug = format!("{:?}", data);
        assert!(debug.contains("TaskUploadData"));
        assert!(debug.contains("task1"));
    }

    #[test]
    fn task_upload_data_serde_roundtrip() {
        let data = TaskUploadData {
            task_id: "task1".to_string(),
            total_uploaded: 5000,
            current_speed_bps: 100.0,
            samples: vec![], // samples are #[serde(skip)]
            tracking_started_at: chrono::Utc::now(),
            last_recorded_at: Some(chrono::Utc::now()),
        };
        let json = serde_json::to_string(&data).unwrap();
        let loaded: TaskUploadData = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.task_id, "task1");
        assert_eq!(loaded.total_uploaded, 5000);
        assert!(loaded.samples.is_empty()); // skipped
    }

    // ===== UploadTrackerSummary traits =====

    #[test]
    fn summary_clone() {
        let summary = UploadTrackerSummary {
            enabled: true,
            tracked_task_count: 3,
            total_uploaded_bytes: 1000,
            current_upload_bps: 500.0,
            top_uploaders: vec![("task1".to_string(), 300.0)],
            formatted: "test".to_string(),
        };
        let cloned = summary.clone();
        assert_eq!(cloned.tracked_task_count, 3);
        assert_eq!(cloned.total_uploaded_bytes, 1000);
    }

    #[test]
    fn summary_debug() {
        let summary = UploadTrackerSummary {
            enabled: true,
            tracked_task_count: 0,
            total_uploaded_bytes: 0,
            current_upload_bps: 0.0,
            top_uploaders: vec![],
            formatted: String::new(),
        };
        let debug = format!("{:?}", summary);
        assert!(debug.contains("UploadTrackerSummary"));
    }

    #[test]
    fn summary_serde_roundtrip() {
        let summary = UploadTrackerSummary {
            enabled: true,
            tracked_task_count: 2,
            total_uploaded_bytes: 5000,
            current_upload_bps: 250.0,
            top_uploaders: vec![("t1".to_string(), 150.0), ("t2".to_string(), 100.0)],
            formatted: "test summary".to_string(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let loaded: UploadTrackerSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.tracked_task_count, 2);
        assert_eq!(loaded.total_uploaded_bytes, 5000);
        assert_eq!(loaded.top_uploaders.len(), 2);
    }

    // ===== UploadTracker traits =====

    #[test]
    fn tracker_clone() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        let cloned = tracker.clone();
        assert_eq!(cloned.get_task_uploaded("task1"), 1000);
        assert_eq!(cloned.global_uploaded_bytes, tracker.global_uploaded_bytes);
    }

    #[test]
    fn tracker_clone_independence() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        let mut cloned = tracker.clone();
        cloned.record_upload("task2", 500);
        assert!(tracker.tasks.get("task2").is_none());
    }

    #[test]
    fn tracker_debug() {
        let tracker = UploadTracker::new();
        let debug = format!("{:?}", tracker);
        assert!(debug.contains("UploadTracker"));
    }

    #[test]
    fn tracker_default_equals_new() {
        let new = UploadTracker::new();
        let default = UploadTracker::default();
        assert_eq!(new.tasks.len(), default.tasks.len());
        assert_eq!(new.global_uploaded_bytes, default.global_uploaded_bytes);
        assert_eq!(new.config.enabled, default.config.enabled);
    }

    // ===== format_speed_bps =====

    #[test]
    fn format_speed_bps_zero() {
        assert_eq!(format_speed_bps(0.0), "0 B/s");
    }

    #[test]
    fn format_speed_bps_boundary_1023() {
        assert_eq!(format_speed_bps(1023.0), "1023 B/s");
    }

    #[test]
    fn format_speed_bps_boundary_1024() {
        assert_eq!(format_speed_bps(1024.0), "1.0 KB/s");
    }

    #[test]
    fn format_speed_bps_boundary_just_under_mb() {
        assert_eq!(format_speed_bps(1024.0 * 1024.0 - 1.0), "1024.0 KB/s");
    }

    #[test]
    fn format_speed_bps_boundary_1mb() {
        assert_eq!(format_speed_bps(1024.0 * 1024.0), "1.00 MB/s");
    }

    #[test]
    fn format_speed_bps_boundary_just_under_gb() {
        assert_eq!(
            format_speed_bps(1024.0 * 1024.0 * 1024.0 - 1.0),
            "1024.00 MB/s"
        );
    }

    #[test]
    fn format_speed_bps_boundary_1gb() {
        assert_eq!(format_speed_bps(1024.0 * 1024.0 * 1024.0), "1.00 GB/s");
    }

    #[test]
    fn format_speed_bps_negative() {
        // Negative values should not panic
        let result = format_speed_bps(-100.0);
        assert!(result.contains("B/s"));
    }

    #[test]
    fn format_speed_bps_very_large() {
        let result = format_speed_bps(5.0 * 1024.0 * 1024.0 * 1024.0);
        assert!(result.contains("GB/s"));
    }

    // ===== format_size =====

    #[test]
    fn format_size_zero() {
        assert_eq!(format_size(0), "0 B");
    }

    #[test]
    fn format_size_boundary_1023() {
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn format_size_boundary_1024() {
        assert_eq!(format_size(1024), "1.0 KB");
    }

    #[test]
    fn format_size_boundary_just_under_mb() {
        assert_eq!(format_size(1024 * 1024 - 1), "1024.0 KB");
    }

    #[test]
    fn format_size_boundary_1mb() {
        assert_eq!(format_size(1024 * 1024), "1.0 MB");
    }

    #[test]
    fn format_size_boundary_just_under_gb() {
        assert_eq!(format_size(1024 * 1024 * 1024 - 1), "1024.0 MB");
    }

    #[test]
    fn format_size_boundary_1gb() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn format_size_very_large() {
        let result = format_size(5 * 1024 * 1024 * 1024);
        assert!(result.contains("GB"));
    }

    // ===== UploadTracker::new() =====

    #[test]
    fn new_tracker_default_values() {
        let tracker = UploadTracker::new();
        assert!(tracker.config.enabled);
        assert_eq!(tracker.config.speed_window_secs, 10);
        assert_eq!(tracker.config.max_tracked_tasks, 500);
        assert_eq!(tracker.tasks.len(), 0);
        assert_eq!(tracker.global_uploaded_bytes, 0);
    }

    // ===== record_upload =====

    #[test]
    fn record_upload_basic() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        assert_eq!(tracker.get_task_uploaded("task1"), 1000);
        assert_eq!(tracker.get_total_uploaded(), 1000);
    }

    #[test]
    fn record_upload_increment() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task1", 2000);
        assert_eq!(tracker.get_task_uploaded("task1"), 2000);
        assert_eq!(tracker.get_total_uploaded(), 2000);
    }

    #[test]
    fn record_upload_disabled() {
        let mut tracker = UploadTracker::new();
        tracker.config.enabled = false;
        tracker.record_upload("task1", 1000);
        assert_eq!(tracker.get_task_uploaded("task1"), 0);
        assert_eq!(tracker.get_total_uploaded(), 0);
        assert_eq!(tracker.tasks.len(), 0);
    }

    #[test]
    fn record_upload_zero_bytes() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 0);
        assert_eq!(tracker.get_task_uploaded("task1"), 0);
        assert_eq!(tracker.tasks.len(), 1); // task is tracked
    }

    #[test]
    fn record_upload_same_value_no_delta() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 5000);
        tracker.record_upload("task1", 5000); // same value
        assert_eq!(tracker.get_task_uploaded("task1"), 5000);
    }

    #[test]
    fn record_upload_large_value() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", u64::MAX);
        assert_eq!(tracker.get_task_uploaded("task1"), u64::MAX);
    }

    #[test]
    fn record_upload_unicode_task_id() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("任务_中文", 1000);
        assert_eq!(tracker.get_task_uploaded("任务_中文"), 1000);
    }

    #[test]
    fn record_upload_emoji_task_id() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task_🚀", 1000);
        assert_eq!(tracker.get_task_uploaded("task_🚀"), 1000);
    }

    #[test]
    fn record_upload_empty_task_id() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("", 1000);
        assert_eq!(tracker.get_task_uploaded(""), 1000);
    }

    #[test]
    fn record_upload_reset_case() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 5000);
        // Simulate reset (uploaded goes back to 0)
        tracker.record_upload("task1", 100);
        // Should still track the new value
        assert_eq!(tracker.get_task_uploaded("task1"), 100);
    }

    #[test]
    fn record_upload_multiple_tasks() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 500);
        tracker.record_upload("task3", 300);

        assert_eq!(tracker.get_task_uploaded("task1"), 1000);
        assert_eq!(tracker.get_task_uploaded("task2"), 500);
        assert_eq!(tracker.get_task_uploaded("task3"), 300);
        assert_eq!(tracker.get_total_uploaded(), 1800);
    }

    // ===== max_tracked_tasks =====

    #[test]
    fn max_tracked_tasks_eviction() {
        let mut tracker = UploadTracker::new();
        tracker.config.max_tracked_tasks = 3;

        tracker.record_upload("task1", 100);
        tracker.record_upload("task2", 200);
        tracker.record_upload("task3", 300);
        tracker.record_upload("task4", 400);

        // Should have evicted one task
        assert!(tracker.tasks.len() <= 3);
    }

    #[test]
    fn max_tracked_tasks_1() {
        let mut tracker = UploadTracker::new();
        tracker.config.max_tracked_tasks = 1;

        tracker.record_upload("task1", 100);
        tracker.record_upload("task2", 200);

        assert_eq!(tracker.tasks.len(), 1);
    }

    #[test]
    fn max_tracked_tasks_existing_not_evicted() {
        let mut tracker = UploadTracker::new();
        tracker.config.max_tracked_tasks = 2;

        tracker.record_upload("task1", 100);
        tracker.record_upload("task2", 200);
        // Re-record to task1 (existing task should not be evicted)
        tracker.record_upload("task1", 300);

        assert_eq!(tracker.get_task_uploaded("task1"), 300);
        assert_eq!(tracker.tasks.len(), 2);
    }

    // ===== get_task_upload_speed =====

    #[test]
    fn get_task_upload_speed_nonexistent() {
        let tracker = UploadTracker::new();
        assert_eq!(tracker.get_task_upload_speed("nonexistent"), 0.0);
    }

    #[test]
    fn get_task_upload_speed_zero_initial() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        // Speed needs at least 2 samples, so initial speed is 0
        let speed = tracker.get_task_upload_speed("task1");
        // With single sample, speed should be 0
        assert_eq!(speed, 0.0);
    }

    // ===== get_total_upload_speed =====

    #[test]
    fn get_total_upload_speed_empty() {
        let tracker = UploadTracker::new();
        assert_eq!(tracker.get_total_upload_speed(), 0.0);
    }

    #[test]
    fn get_total_upload_speed_multiple() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 500);
        // Speeds are 0 initially (single sample), so total should be 0
        let total = tracker.get_total_upload_speed();
        assert!(total >= 0.0);
    }

    // ===== get_total_uploaded =====

    #[test]
    fn get_total_uploaded_empty() {
        let tracker = UploadTracker::new();
        assert_eq!(tracker.get_total_uploaded(), 0);
    }

    #[test]
    fn get_total_uploaded_aggregate() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 2000);
        assert_eq!(tracker.get_total_uploaded(), 3000);
    }

    // ===== remove_task =====

    #[test]
    fn remove_task_basic() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 500);
        assert_eq!(tracker.get_total_uploaded(), 1500);

        tracker.remove_task("task1");
        assert_eq!(tracker.get_task_uploaded("task1"), 0);
        assert_eq!(tracker.get_total_uploaded(), 500);
    }

    #[test]
    fn remove_task_nonexistent() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.remove_task("nonexistent"); // should not panic
        assert_eq!(tracker.get_total_uploaded(), 1000);
    }

    #[test]
    fn remove_task_idempotent() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.remove_task("task1");
        tracker.remove_task("task1"); // second remove should not panic
        assert_eq!(tracker.tasks.len(), 0);
    }

    // ===== clear =====

    #[test]
    fn clear_basic() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 500);

        tracker.clear();
        assert_eq!(tracker.tasks.len(), 0);
        assert_eq!(tracker.get_total_uploaded(), 0);
    }

    #[test]
    fn clear_empty() {
        let mut tracker = UploadTracker::new();
        tracker.clear(); // clearing empty tracker should not panic
        assert_eq!(tracker.tasks.len(), 0);
        assert_eq!(tracker.get_total_uploaded(), 0);
    }

    // ===== get_summary =====

    #[test]
    fn summary_empty() {
        let tracker = UploadTracker::new();
        let summary = tracker.get_summary();
        assert!(summary.enabled);
        assert_eq!(summary.tracked_task_count, 0);
        assert_eq!(summary.total_uploaded_bytes, 0);
        assert_eq!(summary.current_upload_bps, 0.0);
        assert!(summary.top_uploaders.is_empty());
        assert!(summary.formatted.contains("enabled"));
    }

    #[test]
    fn summary_with_tasks() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 500);

        let summary = tracker.get_summary();
        assert!(summary.enabled);
        assert_eq!(summary.tracked_task_count, 2);
        assert_eq!(summary.total_uploaded_bytes, 1500);
        assert!(!summary.formatted.is_empty());
    }

    #[test]
    fn summary_disabled() {
        let mut tracker = UploadTracker::new();
        tracker.config.enabled = false;
        let summary = tracker.get_summary();
        assert!(!summary.enabled);
        assert!(summary.formatted.contains("disabled"));
    }

    #[test]
    fn summary_formatted_content() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        let summary = tracker.get_summary();
        assert!(summary.formatted.contains("Upload Tracker"));
        assert!(summary.formatted.contains("Tasks: 1"));
    }

    // ===== get_task_data =====

    #[test]
    fn get_task_data_exists() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        let data = tracker.get_task_data("task1");
        assert!(data.is_some());
        let data = data.unwrap();
        assert_eq!(data.task_id, "task1");
        assert_eq!(data.total_uploaded, 1000);
        assert!(data.tracking_started_at <= chrono::Utc::now());
    }

    #[test]
    fn get_task_data_not_exists() {
        let tracker = UploadTracker::new();
        assert!(tracker.get_task_data("nonexistent").is_none());
    }

    #[test]
    fn get_task_data_last_recorded() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        let data = tracker.get_task_data("task1").unwrap();
        assert!(data.last_recorded_at.is_some());
    }

    // ===== list_tracked_tasks =====

    #[test]
    fn list_tracked_tasks_empty() {
        let tracker = UploadTracker::new();
        assert!(tracker.list_tracked_tasks().is_empty());
    }

    #[test]
    fn list_tracked_tasks_multiple() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 100);
        tracker.record_upload("task2", 200);
        tracker.record_upload("task3", 300);

        let mut tasks = tracker.list_tracked_tasks();
        tasks.sort();
        assert_eq!(tasks, vec!["task1", "task2", "task3"]);
    }

    // ===== set_config / get_config =====

    #[test]
    fn set_config_basic() {
        let mut tracker = UploadTracker::new();
        let config = UploadTrackerConfig {
            enabled: false,
            speed_window_secs: 60,
            max_tracked_tasks: 50,
        };
        tracker.set_config(config);
        assert!(!tracker.get_config().enabled);
        assert_eq!(tracker.get_config().speed_window_secs, 60);
        assert_eq!(tracker.get_config().max_tracked_tasks, 50);
    }

    #[test]
    fn set_config_multiple_times() {
        let mut tracker = UploadTracker::new();
        let config1 = UploadTrackerConfig {
            enabled: false,
            speed_window_secs: 30,
            max_tracked_tasks: 100,
        };
        tracker.set_config(config1);
        assert!(!tracker.get_config().enabled);

        let config2 = UploadTrackerConfig {
            enabled: true,
            speed_window_secs: 60,
            max_tracked_tasks: 200,
        };
        tracker.set_config(config2);
        assert!(tracker.get_config().enabled);
        assert_eq!(tracker.get_config().speed_window_secs, 60);
    }

    // ===== get_nonexistent_task =====

    #[test]
    fn get_nonexistent_task_uploaded() {
        let tracker = UploadTracker::new();
        assert_eq!(tracker.get_task_uploaded("nonexistent"), 0);
    }

    #[test]
    fn get_nonexistent_task_speed() {
        let tracker = UploadTracker::new();
        assert_eq!(tracker.get_task_upload_speed("nonexistent"), 0.0);
    }

    #[test]
    fn get_nonexistent_task_data() {
        let tracker = UploadTracker::new();
        assert!(tracker.get_task_data("nonexistent").is_none());
    }

    // ===== Persistence (async) =====

    #[tokio::test]
    async fn save_load_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload_config.json");

        let config = UploadTrackerConfig {
            enabled: false,
            speed_window_secs: 30,
            max_tracked_tasks: 100,
        };
        save_upload_tracker_config(&path, &config).await.unwrap();

        let loaded = load_upload_tracker_config(&path).await.unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.speed_window_secs, 30);
        assert_eq!(loaded.max_tracked_tasks, 100);
    }

    #[tokio::test]
    async fn load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let loaded = load_upload_tracker_config(&path).await;
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn load_config_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.json");
        tokio::fs::write(&path, "not valid json").await.unwrap();
        let loaded = load_upload_tracker_config(&path).await;
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn save_config_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");

        let config1 = UploadTrackerConfig {
            enabled: true,
            speed_window_secs: 10,
            max_tracked_tasks: 500,
        };
        save_upload_tracker_config(&path, &config1).await.unwrap();

        let config2 = UploadTrackerConfig {
            enabled: false,
            speed_window_secs: 20,
            max_tracked_tasks: 100,
        };
        save_upload_tracker_config(&path, &config2).await.unwrap();

        let loaded = load_upload_tracker_config(&path).await.unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.speed_window_secs, 20);
    }

    #[tokio::test]
    async fn save_load_state_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload_state.json");

        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 2000);
        tracker.global_uploaded_bytes = 3000;

        save_upload_tracker_state(&path, &tracker).await.unwrap();

        let loaded = load_upload_tracker_state(&path).await.unwrap();
        assert_eq!(loaded.get_task_uploaded("task1"), 1000);
        assert_eq!(loaded.get_task_uploaded("task2"), 2000);
        assert_eq!(loaded.global_uploaded_bytes, 3000);
        assert_eq!(loaded.tasks.len(), 2);
    }

    #[tokio::test]
    async fn load_state_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent_state.json");
        let loaded = load_upload_tracker_state(&path).await;
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn load_state_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt_state.json");
        tokio::fs::write(&path, "invalid json data").await.unwrap();
        let loaded = load_upload_tracker_state(&path).await;
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn save_state_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new_state.json");

        let tracker = UploadTracker::new();
        save_upload_tracker_state(&path, &tracker).await.unwrap();

        assert!(path.exists());
    }

    #[tokio::test]
    async fn save_state_unicode_task_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unicode_state.json");

        let mut tracker = UploadTracker::new();
        tracker.record_upload("任务_中文", 1000);
        tracker.record_upload("タスク_日本語", 2000);

        save_upload_tracker_state(&path, &tracker).await.unwrap();

        let loaded = load_upload_tracker_state(&path).await.unwrap();
        assert_eq!(loaded.get_task_uploaded("任务_中文"), 1000);
        assert_eq!(loaded.get_task_uploaded("タスク_日本語"), 2000);
    }

    #[tokio::test]
    async fn save_state_creates_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let tracker = UploadTracker::new();
        save_upload_tracker_state(&path, &tracker).await.unwrap();

        // Verify the file is valid JSON
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed.is_object());
        assert!(parsed.get("config").is_some());
        assert!(parsed.get("tasks").is_some());
    }

    // ===== Complex workflows =====

    #[test]
    fn full_lifecycle() {
        let mut tracker = UploadTracker::new();

        // Record uploads for multiple tasks
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 2000);
        tracker.record_upload("task3", 3000);
        assert_eq!(tracker.get_total_uploaded(), 6000);
        assert_eq!(tracker.tasks.len(), 3);

        // Remove one task
        tracker.remove_task("task2");
        assert_eq!(tracker.get_total_uploaded(), 4000);
        assert_eq!(tracker.tasks.len(), 2);

        // Get summary
        let summary = tracker.get_summary();
        assert_eq!(summary.tracked_task_count, 2);
        assert_eq!(summary.total_uploaded_bytes, 4000);

        // Clear all
        tracker.clear();
        assert_eq!(tracker.get_total_uploaded(), 0);
        assert_eq!(tracker.tasks.len(), 0);
    }

    #[test]
    fn multi_task_independent_tracking() {
        let mut tracker = UploadTracker::new();

        tracker.record_upload("task1", 100);
        tracker.record_upload("task1", 200);
        tracker.record_upload("task1", 300);

        tracker.record_upload("task2", 5000);

        assert_eq!(tracker.get_task_uploaded("task1"), 300);
        assert_eq!(tracker.get_task_uploaded("task2"), 5000);
        assert_eq!(tracker.get_total_uploaded(), 5300);
    }

    #[test]
    fn remove_then_readd() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.remove_task("task1");
        assert_eq!(tracker.get_task_uploaded("task1"), 0);

        tracker.record_upload("task1", 500);
        assert_eq!(tracker.get_task_uploaded("task1"), 500);
        assert_eq!(tracker.get_total_uploaded(), 500);
    }

    #[test]
    fn config_change_affects_tracking() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        assert_eq!(tracker.get_task_uploaded("task1"), 1000);

        // Disable tracking
        tracker.config.enabled = false;
        tracker.record_upload("task1", 2000);
        // Value should not change since tracking is disabled
        assert_eq!(tracker.get_task_uploaded("task1"), 1000);

        // Re-enable tracking
        tracker.config.enabled = true;
        tracker.record_upload("task1", 3000);
        assert_eq!(tracker.get_task_uploaded("task1"), 3000);
    }

    #[test]
    fn global_uploaded_consistency() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 100);
        tracker.record_upload("task2", 200);
        tracker.record_upload("task3", 300);

        // Global should be sum of all tasks
        assert_eq!(tracker.get_total_uploaded(), 600);
        assert_eq!(tracker.global_uploaded_bytes, 600);

        // Remove one task
        tracker.remove_task("task2");
        assert_eq!(tracker.get_total_uploaded(), 400);
        assert_eq!(tracker.global_uploaded_bytes, 400);

        // Clear all
        tracker.clear();
        assert_eq!(tracker.get_total_uploaded(), 0);
        assert_eq!(tracker.global_uploaded_bytes, 0);
    }

    #[test]
    fn boundary_u64_max() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", u64::MAX);
        assert_eq!(tracker.get_task_uploaded("task1"), u64::MAX);
        assert_eq!(tracker.get_total_uploaded(), u64::MAX);
    }

    #[test]
    fn boundary_zero_everywhere() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 0);
        tracker.record_upload("task2", 0);
        assert_eq!(tracker.get_total_uploaded(), 0);
        assert_eq!(tracker.get_task_uploaded("task1"), 0);
        assert_eq!(tracker.get_task_uploaded("task2"), 0);
    }

    #[test]
    fn many_tasks_tracked() {
        let mut tracker = UploadTracker::new();
        for i in 0..100 {
            tracker.record_upload(&format!("task_{}", i), (i as u64 + 1) * 100);
        }
        assert_eq!(tracker.tasks.len(), 100);
        let expected_total: u64 = (1..=100).map(|i| i * 100).sum();
        assert_eq!(tracker.get_total_uploaded(), expected_total);
    }
}
