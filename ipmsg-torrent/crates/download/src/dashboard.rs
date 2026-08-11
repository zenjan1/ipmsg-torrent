//! Download Dashboard (Phase 113)
//!
//! Provides a unified overview of the download system, aggregating:
//! - Queue status (total, running, paused, completed, error)
//! - Current speeds and bandwidth usage
//! - Queue health score
//! - Queue completion prediction
//! - Top active downloads
//! - Disk space status
//!
//! Useful for a quick glance at the entire download system state.

use crate::DownloadPriority;
use crate::queue_completion::QueueCompletionPrediction;
use crate::queue_health::HealthStatus;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Configuration for dashboard generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardConfig {
    /// Enable dashboard generation
    pub enabled: bool,
    /// Number of top active tasks to include
    pub top_active_count: usize,
    /// Include queue completion prediction
    pub include_prediction: bool,
    /// Include per-protocol breakdown
    pub include_protocol_breakdown: bool,
    /// Include disk space status
    pub include_disk_status: bool,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            top_active_count: 5,
            include_prediction: true,
            include_protocol_breakdown: true,
            include_disk_status: true,
        }
    }
}

/// Queue status summary
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueueStatus {
    /// Total tasks in queue
    pub total: usize,
    /// Tasks currently downloading
    pub running: usize,
    /// Tasks queued waiting to start
    pub queued: usize,
    /// Tasks paused by user
    pub paused: usize,
    /// Tasks completed successfully
    pub completed: usize,
    /// Tasks failed with error
    pub error: usize,
    /// Tasks in recycle bin
    pub recycled: usize,
}

/// Protocol breakdown statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProtocolBreakdown {
    /// HTTP/HTTPS downloads
    pub http_count: usize,
    /// Torrent downloads
    pub torrent_count: usize,
    /// Ed2k downloads
    pub ed2k_count: usize,
    /// P2P downloads
    pub p2p_count: usize,
    /// Magnet links
    pub magnet_count: usize,
}

/// Top active download task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopActiveTask {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Current progress (0.0-1.0)
    pub progress: f32,
    /// Current speed (bytes/sec)
    pub speed_bps: u64,
    /// ETA in seconds (if available)
    pub eta_seconds: Option<f64>,
    /// Total size in bytes
    pub total_size: u64,
    /// Downloaded bytes so far
    pub downloaded: u64,
    /// Task priority
    pub priority: DownloadPriority,
    /// Task protocol
    pub protocol: String,
}

/// Disk space status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskStatus {
    /// Available disk space in bytes
    pub available_bytes: u64,
    /// Total disk space in bytes
    pub total_bytes: u64,
    /// Usage percentage (0.0-1.0)
    pub usage_percent: f64,
    /// Is disk space low?
    pub is_low: bool,
    /// Is disk space critical?
    pub is_critical: bool,
}

impl Default for DiskStatus {
    fn default() -> Self {
        Self {
            available_bytes: 0,
            total_bytes: 0,
            usage_percent: 0.0,
            is_low: false,
            is_critical: false,
        }
    }
}

/// Complete dashboard snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    /// Timestamp of snapshot
    pub snapshot_at: DateTime<Utc>,
    /// Queue status summary
    pub queue_status: QueueStatus,
    /// Current total download speed (bytes/sec)
    pub current_speed_bps: u64,
    /// Current total upload speed (bytes/sec)
    pub current_upload_bps: u64,
    /// Queue health status
    pub health_status: HealthStatus,
    /// Queue health score (0-100)
    pub health_score: u32,
    /// Number of active issues
    pub issue_count: usize,
    /// Queue completion prediction (if enabled)
    pub prediction: Option<QueueCompletionPrediction>,
    /// Top active downloads by speed
    pub top_active: Vec<TopActiveTask>,
    /// Protocol breakdown (if enabled)
    pub protocol_breakdown: Option<ProtocolBreakdown>,
    /// Disk space status (if enabled)
    pub disk_status: Option<DiskStatus>,
    /// Total downloaded bytes (all time)
    pub total_downloaded_bytes: u64,
    /// Total uploaded bytes (all time)
    pub total_uploaded_bytes: u64,
    /// Uptime in seconds
    pub uptime_seconds: u64,
}

impl DashboardSnapshot {
    /// Format dashboard as human-readable string
    pub fn format_summary(&self) -> String {
        let mut out = String::with_capacity(2048);

        out.push_str("=== Download Dashboard ===\n");
        out.push_str(&format!(
            "Snapshot: {}\n\n",
            self.snapshot_at.format("%Y-%m-%d %H:%M:%S UTC")
        ));

        // Queue status
        out.push_str("Queue Status:\n");
        out.push_str(&format!(
            "  Total: {} | Running: {} | Queued: {} | Paused: {} | Completed: {} | Error: {}\n",
            self.queue_status.total,
            self.queue_status.running,
            self.queue_status.queued,
            self.queue_status.paused,
            self.queue_status.completed,
            self.queue_status.error,
        ));
        if self.queue_status.recycled > 0 {
            out.push_str(&format!("  Recycled: {}\n", self.queue_status.recycled));
        }
        out.push('\n');

        // Speed
        out.push_str("Speed:\n");
        out.push_str(&format!(
            "  Download: {} | Upload: {}\n",
            format_speed(self.current_speed_bps),
            format_speed(self.current_upload_bps),
        ));
        out.push_str(&format!(
            "  Total Downloaded: {} | Total Uploaded: {}\n",
            format_size(self.total_downloaded_bytes),
            format_size(self.total_uploaded_bytes),
        ));
        out.push('\n');

        // Health
        out.push_str("Health:\n");
        out.push_str(&format!(
            "  Status: {} | Score: {}/100 | Issues: {}\n",
            self.health_status.label(),
            self.health_score,
            self.issue_count,
        ));
        out.push('\n');

        // Prediction
        if let Some(ref pred) = self.prediction {
            out.push_str("Queue Completion Prediction:\n");
            if let Some(completion) = pred.estimated_completion {
                out.push_str(&format!(
                    "  Estimated completion: {} (in {})\n",
                    completion.format("%Y-%m-%d %H:%M:%S UTC"),
                    format_duration(pred.total_eta_seconds),
                ));
            }
            out.push_str(&format!(
                "  Tasks: {} | Reliable: {} | Confidence: {:.0}%\n",
                pred.task_count,
                pred.reliable_estimates,
                pred.confidence * 100.0,
            ));
            out.push('\n');
        }

        // Top active
        if !self.top_active.is_empty() {
            out.push_str("Top Active Downloads:\n");
            for (i, task) in self.top_active.iter().enumerate() {
                out.push_str(&format!(
                    "  {}. {} - {:.1}% @ {} (ETA: {})\n",
                    i + 1,
                    task.task_name,
                    task.progress * 100.0,
                    format_speed(task.speed_bps),
                    task.eta_seconds
                        .map(format_duration)
                        .unwrap_or_else(|| "N/A".to_string()),
                ));
            }
            out.push('\n');
        }

        // Protocol breakdown
        if let Some(ref proto) = self.protocol_breakdown {
            out.push_str("Protocol Breakdown:\n");
            if proto.http_count > 0 {
                out.push_str(&format!("  HTTP/HTTPS: {}\n", proto.http_count));
            }
            if proto.torrent_count > 0 {
                out.push_str(&format!("  Torrent: {}\n", proto.torrent_count));
            }
            if proto.ed2k_count > 0 {
                out.push_str(&format!("  Ed2k: {}\n", proto.ed2k_count));
            }
            if proto.p2p_count > 0 {
                out.push_str(&format!("  P2P: {}\n", proto.p2p_count));
            }
            if proto.magnet_count > 0 {
                out.push_str(&format!("  Magnet: {}\n", proto.magnet_count));
            }
            out.push('\n');
        }

        // Disk status
        if let Some(ref disk) = self.disk_status {
            out.push_str("Disk Space:\n");
            out.push_str(&format!(
                "  Available: {} / {} ({:.1}% used)\n",
                format_size(disk.available_bytes),
                format_size(disk.total_bytes),
                disk.usage_percent * 100.0,
            ));
            if disk.is_critical {
                out.push_str("  ⚠️ CRITICAL: Disk space critically low!\n");
            } else if disk.is_low {
                out.push_str("  ⚠️ WARNING: Disk space running low\n");
            }
            out.push('\n');
        }

        out.push_str(&format!(
            "Uptime: {}\n",
            format_duration(self.uptime_seconds as f64)
        ));

        out
    }
}

/// Format bytes per second as human-readable speed
fn format_speed(bps: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bps >= GB {
        format!("{:.2} GB/s", bps as f64 / GB as f64)
    } else if bps >= MB {
        format!("{:.2} MB/s", bps as f64 / MB as f64)
    } else if bps >= KB {
        format!("{:.2} KB/s", bps as f64 / KB as f64)
    } else {
        format!("{} B/s", bps)
    }
}

/// Format bytes as human-readable size
fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format duration in seconds as human-readable string
fn format_duration(secs: f64) -> String {
    if secs.is_nan() || secs.is_infinite() || secs < 0.0 {
        return "N/A".to_string();
    }

    let total_secs = secs as u64;
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

/// Dashboard manager for generating snapshots
#[derive(Debug, Clone)]
pub struct DashboardManager {
    config: DashboardConfig,
}

impl Default for DashboardManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DashboardManager {
    /// Create a new dashboard manager with default config
    pub fn new() -> Self {
        Self {
            config: DashboardConfig::default(),
        }
    }

    /// Get current configuration
    pub fn get_config(&self) -> &DashboardConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: DashboardConfig) {
        self.config = config;
    }

    /// Check if dashboard is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dashboard_config_default() {
        let config = DashboardConfig::default();
        assert!(config.enabled);
        assert_eq!(config.top_active_count, 5);
        assert!(config.include_prediction);
        assert!(config.include_protocol_breakdown);
        assert!(config.include_disk_status);
    }

    #[test]
    fn test_dashboard_manager_new() {
        let manager = DashboardManager::new();
        assert!(manager.is_enabled());
        assert_eq!(manager.get_config().top_active_count, 5);
    }

    #[test]
    fn test_dashboard_manager_set_config() {
        let mut manager = DashboardManager::new();
        let mut config = DashboardConfig::default();
        config.top_active_count = 10;
        config.enabled = false;
        manager.set_config(config);
        assert!(!manager.is_enabled());
        assert_eq!(manager.get_config().top_active_count, 10);
    }

    #[test]
    fn test_queue_status_default() {
        let status = QueueStatus::default();
        assert_eq!(status.total, 0);
        assert_eq!(status.running, 0);
        assert_eq!(status.completed, 0);
    }

    #[test]
    fn test_protocol_breakdown_default() {
        let breakdown = ProtocolBreakdown::default();
        assert_eq!(breakdown.http_count, 0);
        assert_eq!(breakdown.torrent_count, 0);
    }

    #[test]
    fn test_disk_status_default() {
        let disk = DiskStatus::default();
        assert_eq!(disk.available_bytes, 0);
        assert!(!disk.is_low);
        assert!(!disk.is_critical);
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(500), "500 B/s");
        assert_eq!(format_speed(1024), "1.00 KB/s");
        assert_eq!(format_speed(1024 * 1024), "1.00 MB/s");
        assert_eq!(format_speed(1024 * 1024 * 1024), "1.00 GB/s");
        assert_eq!(format_speed(1536), "1.50 KB/s");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1024), "1.00 KB");
        assert_eq!(format_size(1024 * 1024), "1.00 MB");
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_size(1024u64 * 1024 * 1024 * 1024), "1.00 TB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30.0), "30s");
        assert_eq!(format_duration(90.0), "1m 30s");
        assert_eq!(format_duration(3661.0), "1h 1m 1s");
        assert_eq!(format_duration(90000.0), "1d 1h 0m");
        assert_eq!(format_duration(f64::NAN), "N/A");
        assert_eq!(format_duration(-1.0), "N/A");
    }

    #[test]
    fn test_dashboard_snapshot_format_summary() {
        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus {
                total: 10,
                running: 3,
                queued: 2,
                paused: 1,
                completed: 3,
                error: 1,
                recycled: 0,
            },
            current_speed_bps: 1024 * 1024,
            current_upload_bps: 512 * 1024,
            health_status: HealthStatus::Healthy,
            health_score: 95,
            issue_count: 0,
            prediction: None,
            top_active: vec![TopActiveTask {
                task_id: "task-1".to_string(),
                task_name: "test-file.zip".to_string(),
                progress: 0.5,
                speed_bps: 1024 * 1024,
                eta_seconds: Some(60.0),
                total_size: 100 * 1024 * 1024,
                downloaded: 50 * 1024 * 1024,
                priority: DownloadPriority::Normal,
                protocol: "HTTP".to_string(),
            }],
            protocol_breakdown: Some(ProtocolBreakdown {
                http_count: 5,
                torrent_count: 3,
                ed2k_count: 1,
                p2p_count: 0,
                magnet_count: 1,
            }),
            disk_status: Some(DiskStatus {
                available_bytes: 50 * 1024 * 1024 * 1024,
                total_bytes: 100 * 1024 * 1024 * 1024,
                usage_percent: 0.5,
                is_low: false,
                is_critical: false,
            }),
            total_downloaded_bytes: 10 * 1024 * 1024 * 1024,
            total_uploaded_bytes: 1 * 1024 * 1024 * 1024,
            uptime_seconds: 3600,
        };

        let summary = snapshot.format_summary();
        assert!(summary.contains("Download Dashboard"));
        assert!(summary.contains("Queue Status"));
        assert!(summary.contains("Total: 10"));
        assert!(summary.contains("Running: 3"));
        assert!(summary.contains("Speed:"));
        assert!(summary.contains("Health:"));
        assert!(summary.contains("Score: 95/100"));
        assert!(summary.contains("Top Active Downloads"));
        assert!(summary.contains("test-file.zip"));
        assert!(summary.contains("Protocol Breakdown"));
        assert!(summary.contains("HTTP/HTTPS: 5"));
        assert!(summary.contains("Disk Space"));
        assert!(summary.contains("Uptime:"));
    }

    #[test]
    fn test_dashboard_snapshot_with_prediction() {
        let prediction = QueueCompletionPrediction {
            predicted_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            total_eta_seconds: 3600.0,
            estimated_completion: Some("2026-08-10T13:00:00Z".parse().unwrap()),
            task_count: 5,
            reliable_estimates: 4,
            confidence: 0.8,
            task_estimates: vec![],
            active_downloads: 3,
            max_concurrent: 5,
            summary: "Queue will complete in ~1h".to_string(),
        };

        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus::default(),
            current_speed_bps: 0,
            current_upload_bps: 0,
            health_status: HealthStatus::Healthy,
            health_score: 100,
            issue_count: 0,
            prediction: Some(prediction),
            top_active: vec![],
            protocol_breakdown: None,
            disk_status: None,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };

        let summary = snapshot.format_summary();
        assert!(summary.contains("Queue Completion Prediction"));
        assert!(summary.contains("Tasks: 5"));
        assert!(summary.contains("Confidence: 80%"));
    }

    #[test]
    fn test_dashboard_snapshot_disk_warning() {
        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus::default(),
            current_speed_bps: 0,
            current_upload_bps: 0,
            health_status: HealthStatus::Healthy,
            health_score: 100,
            issue_count: 0,
            prediction: None,
            top_active: vec![],
            protocol_breakdown: None,
            disk_status: Some(DiskStatus {
                available_bytes: 1 * 1024 * 1024 * 1024,
                total_bytes: 100 * 1024 * 1024 * 1024,
                usage_percent: 0.99,
                is_low: true,
                is_critical: false,
            }),
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };

        let summary = snapshot.format_summary();
        assert!(summary.contains("WARNING"));
        assert!(summary.contains("Disk space Running low"));
    }

    #[test]
    fn test_dashboard_snapshot_disk_critical() {
        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus::default(),
            current_speed_bps: 0,
            current_upload_bps: 0,
            health_status: HealthStatus::Healthy,
            health_score: 100,
            issue_count: 0,
            prediction: None,
            top_active: vec![],
            protocol_breakdown: None,
            disk_status: Some(DiskStatus {
                available_bytes: 100 * 1024 * 1024,
                total_bytes: 100 * 1024 * 1024 * 1024,
                usage_percent: 0.999,
                is_low: true,
                is_critical: true,
            }),
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };

        let summary = snapshot.format_summary();
        assert!(summary.contains("CRITICAL"));
    }

    #[test]
    fn test_dashboard_serialization() {
        let config = DashboardConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DashboardConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.top_active_count, config.top_active_count);
    }
}
