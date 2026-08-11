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

    #[test]
    fn test_default_config() {
        let config = UploadTrackerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.speed_window_secs, 10);
        assert_eq!(config.max_tracked_tasks, 500);
    }

    #[test]
    fn test_new_tracker() {
        let tracker = UploadTracker::new();
        assert!(tracker.config.enabled);
        assert_eq!(tracker.tasks.len(), 0);
        assert_eq!(tracker.global_uploaded_bytes, 0);
    }

    #[test]
    fn test_record_upload() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        assert_eq!(tracker.get_task_uploaded("task1"), 1000);
        assert_eq!(tracker.get_total_uploaded(), 1000);

        tracker.record_upload("task1", 2000);
        assert_eq!(tracker.get_task_uploaded("task1"), 2000);
        assert_eq!(tracker.get_total_uploaded(), 2000);
    }

    #[test]
    fn test_record_multiple_tasks() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 500);
        tracker.record_upload("task3", 300);

        assert_eq!(tracker.get_task_uploaded("task1"), 1000);
        assert_eq!(tracker.get_task_uploaded("task2"), 500);
        assert_eq!(tracker.get_task_uploaded("task3"), 300);
        assert_eq!(tracker.get_total_uploaded(), 1800);
    }

    #[test]
    fn test_disabled_tracking() {
        let mut tracker = UploadTracker::new();
        tracker.config.enabled = false;
        tracker.record_upload("task1", 1000);
        assert_eq!(tracker.get_task_uploaded("task1"), 0);
        assert_eq!(tracker.get_total_uploaded(), 0);
    }

    #[test]
    fn test_remove_task() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 500);
        assert_eq!(tracker.get_total_uploaded(), 1500);

        tracker.remove_task("task1");
        assert_eq!(tracker.get_task_uploaded("task1"), 0);
        assert_eq!(tracker.get_total_uploaded(), 500);
    }

    #[test]
    fn test_clear() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 1000);
        tracker.record_upload("task2", 500);

        tracker.clear();
        assert_eq!(tracker.tasks.len(), 0);
        assert_eq!(tracker.get_total_uploaded(), 0);
    }

    #[test]
    fn test_summary() {
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
    fn test_get_nonexistent_task() {
        let tracker = UploadTracker::new();
        assert_eq!(tracker.get_task_uploaded("nonexistent"), 0);
        assert_eq!(tracker.get_task_upload_speed("nonexistent"), 0.0);
        assert!(tracker.get_task_data("nonexistent").is_none());
    }

    #[test]
    fn test_max_tracked_tasks() {
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
    fn test_list_tracked_tasks() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 100);
        tracker.record_upload("task2", 200);

        let mut tasks = tracker.list_tracked_tasks();
        tasks.sort();
        assert_eq!(tasks, vec!["task1", "task2"]);
    }

    #[test]
    fn test_format_speed_bps() {
        assert_eq!(format_speed_bps(500.0), "500 B/s");
        assert_eq!(format_speed_bps(1500.0), "1.5 KB/s");
        assert_eq!(format_speed_bps(1_500_000.0), "1.46 MB/s");
        assert_eq!(format_speed_bps(1_500_000_000.0), "1.40 GB/s");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1500), "1.5 KB");
        assert_eq!(format_size(1_500_000), "1.4 MB");
        assert_eq!(format_size(1_500_000_000), "1.40 GB");
    }

    #[test]
    fn test_config_serialization() {
        let config = UploadTrackerConfig {
            enabled: false,
            speed_window_secs: 30,
            max_tracked_tasks: 100,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: UploadTrackerConfig = serde_json::from_str(&json).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.speed_window_secs, 30);
        assert_eq!(loaded.max_tracked_tasks, 100);
    }

    #[test]
    fn test_set_config() {
        let mut tracker = UploadTracker::new();
        let config = UploadTrackerConfig {
            enabled: false,
            speed_window_secs: 60,
            max_tracked_tasks: 50,
        };
        tracker.set_config(config);
        assert!(!tracker.get_config().enabled);
        assert_eq!(tracker.get_config().speed_window_secs, 60);
    }

    #[test]
    fn test_reset_case() {
        let mut tracker = UploadTracker::new();
        tracker.record_upload("task1", 5000);
        // Simulate reset (uploaded goes back to 0)
        tracker.record_upload("task1", 100);
        // Should still track the new value
        assert_eq!(tracker.get_task_uploaded("task1"), 100);
    }
}
