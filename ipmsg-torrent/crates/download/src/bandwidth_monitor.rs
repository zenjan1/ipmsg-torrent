//! Bandwidth monitoring with rolling history
//!
//! Tracks download/upload speeds over time for dashboard display.
//! Maintains a rolling window of samples (default 60 minutes at 10s intervals).

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Default number of samples to keep in history
const DEFAULT_MAX_SAMPLES: usize = 360; // 60 minutes at 10s intervals

/// Default sampling interval
const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// A single bandwidth sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthSample {
    /// Timestamp (seconds since epoch)
    pub timestamp: u64,
    /// Download speed in bytes/sec
    pub download_bps: f64,
    /// Upload speed in bytes/sec (reserved for future use)
    pub upload_bps: f64,
}

/// Bandwidth statistics for a time window
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BandwidthStats {
    /// Average download speed in the window (bytes/sec)
    pub avg_download_bps: f64,
    /// Peak download speed in the window (bytes/sec)
    pub peak_download_bps: f64,
    /// Average upload speed in the window (bytes/sec)
    pub avg_upload_bps: f64,
    /// Peak upload speed in the window (bytes/sec)
    pub peak_upload_bps: f64,
    /// Total bytes downloaded in the window
    pub total_downloaded: u64,
    /// Total bytes uploaded in the window
    pub total_uploaded: u64,
    /// Number of samples in the window
    pub sample_count: usize,
    /// Window duration in seconds
    pub window_secs: u64,
}

/// Per-task bandwidth info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskBandwidth {
    pub task_id: String,
    pub task_name: String,
    pub current_bps: f64,
    pub avg_bps: f64,
    pub peak_bps: f64,
    pub total_downloaded: u64,
}

/// Full bandwidth dashboard data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthDashboard {
    /// Current instantaneous speed
    pub current_download_bps: f64,
    pub current_upload_bps: f64,
    /// Stats for different time windows
    pub last_5min: BandwidthStats,
    pub last_15min: BandwidthStats,
    pub last_60min: BandwidthStats,
    /// Per-task breakdown
    pub tasks: Vec<TaskBandwidth>,
    /// Historical samples for charting
    pub history: Vec<BandwidthSample>,
}

/// Monitors bandwidth usage over time with a rolling window
pub struct BandwidthMonitor {
    /// Rolling history of bandwidth samples
    history: Arc<RwLock<VecDeque<BandwidthSample>>>,
    /// Maximum samples to keep
    max_samples: usize,
    /// Sampling interval
    sample_interval: Duration,
    /// Handle for the background sampling task
    _handle: tokio::task::JoinHandle<()>,
    /// Cancellation token
    cancel: tokio_util::sync::CancellationToken,
    /// Current speed tracker callback
    current_speed: Arc<RwLock<CurrentSpeed>>,
}

/// Tracks current instantaneous speed
#[derive(Debug, Clone, Default)]
struct CurrentSpeed {
    download_bps: f64,
    upload_bps: f64,
}

impl BandwidthMonitor {
    /// Create a new bandwidth monitor with default settings
    pub fn new() -> Self {
        Self::with_config(DEFAULT_MAX_SAMPLES, DEFAULT_SAMPLE_INTERVAL)
    }

    /// Create a bandwidth monitor with custom configuration
    pub fn with_config(max_samples: usize, sample_interval: Duration) -> Self {
        let history = Arc::new(RwLock::new(VecDeque::with_capacity(max_samples)));
        let current_speed = Arc::new(RwLock::new(CurrentSpeed::default()));
        let cancel = tokio_util::sync::CancellationToken::new();

        let history_clone = history.clone();
        let current_speed_clone = current_speed.clone();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(sample_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let speed = current_speed_clone.read().await;
                        let sample = BandwidthSample {
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            download_bps: speed.download_bps,
                            upload_bps: speed.upload_bps,
                        };
                        drop(speed);

                        let mut hist = history_clone.write().await;
                        if hist.len() >= max_samples {
                            hist.pop_front();
                        }
                        hist.push_back(sample);
                    }
                    _ = cancel_clone.cancelled() => {
                        break;
                    }
                }
            }
        });

        Self {
            history,
            max_samples,
            sample_interval,
            _handle: handle,
            cancel,
            current_speed,
        }
    }

    /// Update the current instantaneous download speed
    pub async fn update_current_speed(&self, download_bps: f64, upload_bps: f64) {
        let mut speed = self.current_speed.write().await;
        speed.download_bps = download_bps;
        speed.upload_bps = upload_bps;
    }

    /// Get the current instantaneous speed
    pub async fn current_speed(&self) -> (f64, f64) {
        let speed = self.current_speed.read().await;
        (speed.download_bps, speed.upload_bps)
    }

    /// Get bandwidth statistics for a specific time window
    pub async fn stats_for_window(&self, window: Duration) -> BandwidthStats {
        let history = self.history.read().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let cutoff = now.saturating_sub(window.as_secs());

        let samples: Vec<_> = history
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .cloned()
            .collect();

        if samples.is_empty() {
            return BandwidthStats::default();
        }

        let count = samples.len();
        let mut total_dl = 0.0f64;
        let mut total_ul = 0.0f64;
        let mut peak_dl = 0.0f64;
        let mut peak_ul = 0.0f64;

        for s in &samples {
            total_dl += s.download_bps;
            total_ul += s.upload_bps;
            peak_dl = peak_dl.max(s.download_bps);
            peak_ul = peak_ul.max(s.upload_bps);
        }

        // Estimate total bytes: average_bps * window_secs
        let avg_dl = total_dl / count as f64;
        let avg_ul = total_ul / count as f64;
        let actual_window = if count > 1 {
            samples.last().unwrap().timestamp - samples.first().unwrap().timestamp
        } else {
            0
        };

        BandwidthStats {
            avg_download_bps: avg_dl,
            peak_download_bps: peak_dl,
            avg_upload_bps: avg_ul,
            peak_upload_bps: peak_ul,
            total_downloaded: (avg_dl * actual_window as f64 / 8.0) as u64,
            total_uploaded: (avg_ul * actual_window as f64 / 8.0) as u64,
            sample_count: count,
            window_secs: actual_window,
        }
    }

    /// Get the full bandwidth dashboard
    pub async fn dashboard(
        &self,
        task_speeds: Vec<(String, String, f64, u64)>,
    ) -> BandwidthDashboard {
        let (current_dl, current_ul) = self.current_speed().await;
        let last_5min = self.stats_for_window(Duration::from_secs(300)).await;
        let last_15min = self.stats_for_window(Duration::from_secs(900)).await;
        let last_60min = self.stats_for_window(Duration::from_secs(3600)).await;

        // Build per-task bandwidth info
        let tasks: Vec<TaskBandwidth> = task_speeds
            .into_iter()
            .map(|(id, name, current_bps, total_downloaded)| {
                TaskBandwidth {
                    task_id: id,
                    task_name: name,
                    current_bps,
                    avg_bps: current_bps,  // Simplified: use current as average
                    peak_bps: current_bps, // Simplified: use current as peak
                    total_downloaded,
                }
            })
            .collect();

        let history = self.history.read().await;
        let history_vec: Vec<BandwidthSample> = history.iter().cloned().collect();

        BandwidthDashboard {
            current_download_bps: current_dl,
            current_upload_bps: current_ul,
            last_5min,
            last_15min,
            last_60min,
            tasks,
            history: history_vec,
        }
    }

    /// Get raw history samples
    pub async fn history(&self) -> Vec<BandwidthSample> {
        self.history.read().await.iter().cloned().collect()
    }

    /// Clear all history
    pub async fn clear_history(&self) {
        self.history.write().await.clear();
    }

    /// Get the maximum number of samples
    pub fn max_samples(&self) -> usize {
        self.max_samples
    }

    /// Get the sampling interval
    pub fn sample_interval(&self) -> Duration {
        self.sample_interval
    }
}

impl Default for BandwidthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for BandwidthMonitor {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bandwidth_monitor_creation() {
        let monitor = BandwidthMonitor::new();
        assert_eq!(monitor.max_samples(), DEFAULT_MAX_SAMPLES);
        assert_eq!(monitor.sample_interval(), DEFAULT_SAMPLE_INTERVAL);
    }

    #[tokio::test]
    async fn test_update_and_get_current_speed() {
        let monitor = BandwidthMonitor::new();
        monitor.update_current_speed(1024.0, 512.0).await;
        let (dl, ul) = monitor.current_speed().await;
        assert_eq!(dl, 1024.0);
        assert_eq!(ul, 512.0);
    }

    #[tokio::test]
    async fn test_stats_empty_history() {
        let monitor = BandwidthMonitor::new();
        let stats = monitor.stats_for_window(Duration::from_secs(300)).await;
        assert_eq!(stats.sample_count, 0);
        assert_eq!(stats.avg_download_bps, 0.0);
    }

    #[tokio::test]
    async fn test_stats_with_manual_samples() {
        let monitor = BandwidthMonitor::with_config(100, Duration::from_secs(3600));
        // Manually inject samples
        {
            let mut history = monitor.history.write().await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            for i in 0..5 {
                history.push_back(BandwidthSample {
                    timestamp: now - (4 - i) * 10,
                    download_bps: 1000.0 * (i + 1) as f64,
                    upload_bps: 500.0 * (i + 1) as f64,
                });
            }
        }
        let stats = monitor.stats_for_window(Duration::from_secs(300)).await;
        assert_eq!(stats.sample_count, 5);
        // Average of 1000, 2000, 3000, 4000, 5000 = 3000
        assert!((stats.avg_download_bps - 3000.0).abs() < 0.1);
        assert!((stats.peak_download_bps - 5000.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_rolling_window_eviction() {
        // Test that time-window filtering works correctly
        let monitor = BandwidthMonitor::with_config(100, Duration::from_secs(3600));
        let base_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        {
            let mut history = monitor.history.write().await;
            // Insert 10 samples, 2 minutes apart (total 18 minutes span)
            for i in 0..10 {
                history.push_back(BandwidthSample {
                    timestamp: base_time - (9 - i) * 120,
                    download_bps: 100.0,
                    upload_bps: 50.0,
                });
            }
        }

        // 5-min window: should include only recent samples
        let stats_5min = monitor.stats_for_window(Duration::from_secs(300)).await;
        // 10-min window: should include more samples
        let stats_10min = monitor.stats_for_window(Duration::from_secs(600)).await;
        // 20-min window: should include all samples
        let stats_20min = monitor.stats_for_window(Duration::from_secs(1200)).await;

        // Verify that larger windows include more samples
        assert!(stats_10min.sample_count >= stats_5min.sample_count);
        assert!(stats_20min.sample_count >= stats_10min.sample_count);
        assert_eq!(stats_20min.sample_count, 10); // All samples in 20-min window
    }

    #[tokio::test]
    async fn test_clear_history() {
        let monitor = BandwidthMonitor::new();
        {
            let mut history = monitor.history.write().await;
            history.push_back(BandwidthSample {
                timestamp: 1000,
                download_bps: 100.0,
                upload_bps: 50.0,
            });
        }
        assert_eq!(monitor.history().await.len(), 1);
        monitor.clear_history().await;
        assert_eq!(monitor.history().await.len(), 0);
    }

    #[tokio::test]
    async fn test_dashboard_empty() {
        let monitor = BandwidthMonitor::new();
        let dashboard = monitor.dashboard(vec![]).await;
        assert_eq!(dashboard.current_download_bps, 0.0);
        assert_eq!(dashboard.tasks.len(), 0);
        assert_eq!(dashboard.history.len(), 0);
    }

    #[tokio::test]
    async fn test_dashboard_with_tasks() {
        let monitor = BandwidthMonitor::new();
        monitor.update_current_speed(2048.0, 1024.0).await;
        let task_speeds = vec![
            ("task1".to_string(), "file1.txt".to_string(), 1024.0, 50000),
            ("task2".to_string(), "file2.txt".to_string(), 1024.0, 30000),
        ];
        let dashboard = monitor.dashboard(task_speeds).await;
        assert_eq!(dashboard.current_download_bps, 2048.0);
        assert_eq!(dashboard.tasks.len(), 2);
        assert_eq!(dashboard.tasks[0].task_id, "task1");
        assert_eq!(dashboard.tasks[1].total_downloaded, 30000);
    }

    #[tokio::test]
    async fn test_window_filtering() {
        let monitor = BandwidthMonitor::with_config(100, Duration::from_secs(3600));
        {
            let mut history = monitor.history.write().await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            // Old samples (outside 5min window)
            history.push_back(BandwidthSample {
                timestamp: now - 600,
                download_bps: 9999.0,
                upload_bps: 0.0,
            });
            // Recent samples (inside 5min window)
            for i in 0..3 {
                history.push_back(BandwidthSample {
                    timestamp: now - (2 - i) * 10,
                    download_bps: 100.0,
                    upload_bps: 50.0,
                });
            }
        }
        // 5min window should exclude the old sample
        let stats_5min = monitor.stats_for_window(Duration::from_secs(300)).await;
        assert_eq!(stats_5min.sample_count, 3);
        assert!((stats_5min.avg_download_bps - 100.0).abs() < 0.1);

        // 15min window should include all samples
        let stats_15min = monitor.stats_for_window(Duration::from_secs(900)).await;
        assert_eq!(stats_15min.sample_count, 4);
    }

    #[tokio::test]
    async fn test_sample_serialization() {
        let sample = BandwidthSample {
            timestamp: 1700000000,
            download_bps: 1024.5,
            upload_bps: 512.25,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let deserialized: BandwidthSample = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.timestamp, 1700000000);
        assert!((deserialized.download_bps - 1024.5).abs() < 0.01);
    }
}
