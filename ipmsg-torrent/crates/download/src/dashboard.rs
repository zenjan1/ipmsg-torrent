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
        assert!(summary.contains("Disk space running low"));
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

    // === Serde roundtrip tests ===

    #[test]
    fn test_dashboard_config_serde_roundtrip() {
        let config = DashboardConfig {
            enabled: false,
            top_active_count: 20,
            include_prediction: false,
            include_protocol_breakdown: false,
            include_disk_status: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DashboardConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, false);
        assert_eq!(deserialized.top_active_count, 20);
        assert!(!deserialized.include_prediction);
        assert!(!deserialized.include_protocol_breakdown);
        assert!(!deserialized.include_disk_status);
    }

    #[test]
    fn test_dashboard_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"top_active_count":5,"include_prediction":true,"include_protocol_breakdown":true,"include_disk_status":true,"extra_field":"ignored"}"#;
        let config: DashboardConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.top_active_count, 5);
    }

    #[test]
    fn test_dashboard_config_pretty_serde() {
        let config = DashboardConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: DashboardConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.top_active_count, config.top_active_count);
    }

    #[test]
    fn test_queue_status_serde_roundtrip() {
        let status = QueueStatus {
            total: 100,
            running: 10,
            queued: 20,
            paused: 5,
            completed: 50,
            error: 10,
            recycled: 5,
        };
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: QueueStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total, 100);
        assert_eq!(deserialized.running, 10);
        assert_eq!(deserialized.queued, 20);
        assert_eq!(deserialized.paused, 5);
        assert_eq!(deserialized.completed, 50);
        assert_eq!(deserialized.error, 10);
        assert_eq!(deserialized.recycled, 5);
    }

    #[test]
    fn test_queue_status_serde_extra_fields_ignored() {
        let json = r#"{"total":1,"running":0,"queued":0,"paused":0,"completed":0,"error":0,"recycled":0,"unknown":42}"#;
        let status: QueueStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status.total, 1);
    }

    #[test]
    fn test_protocol_breakdown_serde_roundtrip() {
        let breakdown = ProtocolBreakdown {
            http_count: 10,
            torrent_count: 5,
            ed2k_count: 3,
            p2p_count: 2,
            magnet_count: 1,
        };
        let json = serde_json::to_string(&breakdown).unwrap();
        let deserialized: ProtocolBreakdown = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.http_count, 10);
        assert_eq!(deserialized.torrent_count, 5);
        assert_eq!(deserialized.ed2k_count, 3);
        assert_eq!(deserialized.p2p_count, 2);
        assert_eq!(deserialized.magnet_count, 1);
    }

    #[test]
    fn test_protocol_breakdown_serde_extra_fields_ignored() {
        let json = r#"{"http_count":1,"torrent_count":0,"ed2k_count":0,"p2p_count":0,"magnet_count":0,"extra":true}"#;
        let breakdown: ProtocolBreakdown = serde_json::from_str(json).unwrap();
        assert_eq!(breakdown.http_count, 1);
    }

    #[test]
    fn test_top_active_task_serde_roundtrip() {
        let task = TopActiveTask {
            task_id: "task-42".to_string(),
            task_name: "ubuntu.iso".to_string(),
            progress: 0.75,
            speed_bps: 5 * 1024 * 1024,
            eta_seconds: Some(120.0),
            total_size: 1024 * 1024 * 1024,
            downloaded: 768 * 1024 * 1024,
            priority: DownloadPriority::High,
            protocol: "HTTP".to_string(),
        };
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: TopActiveTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task-42");
        assert_eq!(deserialized.task_name, "ubuntu.iso");
        assert!((deserialized.progress - 0.75).abs() < f32::EPSILON);
        assert_eq!(deserialized.speed_bps, 5 * 1024 * 1024);
        assert_eq!(deserialized.eta_seconds, Some(120.0));
        assert_eq!(deserialized.total_size, 1024 * 1024 * 1024);
        assert_eq!(deserialized.downloaded, 768 * 1024 * 1024);
        assert_eq!(deserialized.protocol, "HTTP");
    }

    #[test]
    fn test_top_active_task_serde_none_eta() {
        let task = TopActiveTask {
            task_id: "t1".to_string(),
            task_name: "file.zip".to_string(),
            progress: 0.0,
            speed_bps: 0,
            eta_seconds: None,
            total_size: 100,
            downloaded: 0,
            priority: DownloadPriority::Normal,
            protocol: "Ed2k".to_string(),
        };
        let json = serde_json::to_string(&task).unwrap();
        assert!(json.contains("null") || json.contains("eta_seconds"));
        let deserialized: TopActiveTask = serde_json::from_str(&json).unwrap();
        assert!(deserialized.eta_seconds.is_none());
    }

    #[test]
    fn test_disk_status_serde_roundtrip() {
        let disk = DiskStatus {
            available_bytes: 50 * 1024 * 1024 * 1024,
            total_bytes: 500 * 1024 * 1024 * 1024,
            usage_percent: 0.9,
            is_low: true,
            is_critical: false,
        };
        let json = serde_json::to_string(&disk).unwrap();
        let deserialized: DiskStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.available_bytes, 50 * 1024 * 1024 * 1024);
        assert_eq!(deserialized.total_bytes, 500 * 1024 * 1024 * 1024);
        assert!((deserialized.usage_percent - 0.9).abs() < f64::EPSILON);
        assert!(deserialized.is_low);
        assert!(!deserialized.is_critical);
    }

    #[test]
    fn test_disk_status_serde_extra_fields_ignored() {
        let json = r#"{"available_bytes":100,"total_bytes":200,"usage_percent":0.5,"is_low":false,"is_critical":false,"temp":"ignore"}"#;
        let disk: DiskStatus = serde_json::from_str(json).unwrap();
        assert_eq!(disk.available_bytes, 100);
        assert_eq!(disk.total_bytes, 200);
    }

    #[test]
    fn test_dashboard_snapshot_serde_roundtrip() {
        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus {
                total: 5,
                running: 2,
                queued: 1,
                paused: 1,
                completed: 1,
                error: 0,
                recycled: 0,
            },
            current_speed_bps: 1024,
            current_upload_bps: 512,
            health_status: HealthStatus::Degraded,
            health_score: 60,
            issue_count: 2,
            prediction: None,
            top_active: vec![],
            protocol_breakdown: None,
            disk_status: None,
            total_downloaded_bytes: 1024 * 1024,
            total_uploaded_bytes: 512 * 1024,
            uptime_seconds: 7200,
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        let deserialized: DashboardSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.queue_status.total, 5);
        assert_eq!(deserialized.current_speed_bps, 1024);
        assert_eq!(deserialized.health_score, 60);
        assert_eq!(deserialized.issue_count, 2);
        assert_eq!(deserialized.total_downloaded_bytes, 1024 * 1024);
        assert_eq!(deserialized.uptime_seconds, 7200);
    }

    // === Clone/Debug trait tests ===

    #[test]
    fn test_dashboard_config_clone_debug() {
        let config = DashboardConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.top_active_count, config.top_active_count);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("DashboardConfig"));
    }

    #[test]
    fn test_queue_status_clone_debug() {
        let status = QueueStatus {
            total: 10,
            running: 3,
            queued: 2,
            paused: 1,
            completed: 3,
            error: 1,
            recycled: 0,
        };
        let cloned = status.clone();
        assert_eq!(cloned.total, 10);
        assert_eq!(cloned.running, 3);
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("QueueStatus"));
    }

    #[test]
    fn test_protocol_breakdown_clone_debug() {
        let breakdown = ProtocolBreakdown {
            http_count: 5,
            torrent_count: 3,
            ed2k_count: 1,
            p2p_count: 0,
            magnet_count: 2,
        };
        let cloned = breakdown.clone();
        assert_eq!(cloned.http_count, 5);
        let debug_str = format!("{:?}", breakdown);
        assert!(debug_str.contains("ProtocolBreakdown"));
    }

    #[test]
    fn test_top_active_task_clone_debug() {
        let task = TopActiveTask {
            task_id: "t1".to_string(),
            task_name: "file.zip".to_string(),
            progress: 0.5,
            speed_bps: 1024,
            eta_seconds: Some(60.0),
            total_size: 2048,
            downloaded: 1024,
            priority: DownloadPriority::Normal,
            protocol: "HTTP".to_string(),
        };
        let cloned = task.clone();
        assert_eq!(cloned.task_id, "t1");
        let debug_str = format!("{:?}", task);
        assert!(debug_str.contains("TopActiveTask"));
    }

    #[test]
    fn test_disk_status_clone_debug() {
        let disk = DiskStatus {
            available_bytes: 1024,
            total_bytes: 2048,
            usage_percent: 0.5,
            is_low: false,
            is_critical: false,
        };
        let cloned = disk.clone();
        assert_eq!(cloned.available_bytes, 1024);
        let debug_str = format!("{:?}", disk);
        assert!(debug_str.contains("DiskStatus"));
    }

    #[test]
    fn test_dashboard_snapshot_clone_debug() {
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
            disk_status: None,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let cloned = snapshot.clone();
        assert_eq!(cloned.health_score, 100);
        let debug_str = format!("{:?}", snapshot);
        assert!(debug_str.contains("DashboardSnapshot"));
    }

    #[test]
    fn test_dashboard_manager_clone_debug() {
        let manager = DashboardManager::new();
        let cloned = manager.clone();
        assert_eq!(cloned.get_config().top_active_count, manager.get_config().top_active_count);
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("DashboardManager"));
    }

    // === format_speed boundary tests ===

    #[test]
    fn test_format_speed_zero() {
        assert_eq!(format_speed(0), "0 B/s");
    }

    #[test]
    fn test_format_speed_exact_kb_boundary() {
        assert_eq!(format_speed(1024), "1.00 KB/s");
    }

    #[test]
    fn test_format_speed_exact_mb_boundary() {
        assert_eq!(format_speed(1024 * 1024), "1.00 MB/s");
    }

    #[test]
    fn test_format_speed_exact_gb_boundary() {
        assert_eq!(format_speed(1024 * 1024 * 1024), "1.00 GB/s");
    }

    #[test]
    fn test_format_speed_below_kb() {
        assert_eq!(format_speed(1023), "1023 B/s");
    }

    #[test]
    fn test_format_speed_below_mb() {
        assert_eq!(format_speed(1024 * 1024 - 1), "1024.00 KB/s");
    }

    #[test]
    fn test_format_speed_below_gb() {
        assert_eq!(format_speed(1024 * 1024 * 1024 - 1), "1024.00 MB/s");
    }

    #[test]
    fn test_format_speed_large_value() {
        // 2.5 GB/s
        let val = (2.5 * 1024.0 * 1024.0 * 1024.0) as u64;
        assert_eq!(format_speed(val), "2.50 GB/s");
    }

    // === format_size boundary tests ===

    #[test]
    fn test_format_size_zero() {
        assert_eq!(format_size(0), "0 B");
    }

    #[test]
    fn test_format_size_exact_kb() {
        assert_eq!(format_size(1024), "1.00 KB");
    }

    #[test]
    fn test_format_size_exact_mb() {
        assert_eq!(format_size(1024 * 1024), "1.00 MB");
    }

    #[test]
    fn test_format_size_exact_gb() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_format_size_exact_tb() {
        assert_eq!(format_size(1024u64 * 1024 * 1024 * 1024), "1.00 TB");
    }

    #[test]
    fn test_format_size_below_kb() {
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn test_format_size_below_mb() {
        assert_eq!(format_size(1024 * 1024 - 1), "1024.00 KB");
    }

    #[test]
    fn test_format_size_large_tb() {
        let val = 5u64 * 1024 * 1024 * 1024 * 1024;
        assert_eq!(format_size(val), "5.00 TB");
    }

    // === format_duration boundary tests ===

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0.0), "0s");
    }

    #[test]
    fn test_format_duration_exact_minute() {
        assert_eq!(format_duration(60.0), "1m 0s");
    }

    #[test]
    fn test_format_duration_exact_hour() {
        assert_eq!(format_duration(3600.0), "1h 0m 0s");
    }

    #[test]
    fn test_format_duration_exact_day() {
        assert_eq!(format_duration(86400.0), "1d 0h 0m");
    }

    #[test]
    fn test_format_duration_infinity() {
        assert_eq!(format_duration(f64::INFINITY), "N/A");
    }

    #[test]
    fn test_format_duration_negative_infinity() {
        assert_eq!(format_duration(f64::NEG_INFINITY), "N/A");
    }

    #[test]
    fn test_format_duration_fractional_seconds() {
        // fractional seconds are truncated
        assert_eq!(format_duration(30.9), "30s");
    }

    #[test]
    fn test_format_duration_large_value() {
        // 3 days, 5 hours, 30 minutes
        let secs = 3.0 * 86400.0 + 5.0 * 3600.0 + 30.0 * 60.0;
        assert_eq!(format_duration(secs), "3d 5h 30m");
    }

    // === DashboardManager tests ===

    #[test]
    fn test_dashboard_manager_default_trait() {
        let manager = DashboardManager::default();
        assert!(manager.is_enabled());
        assert_eq!(manager.get_config().top_active_count, 5);
    }

    #[test]
    fn test_dashboard_manager_is_enabled_default() {
        let manager = DashboardManager::new();
        assert!(manager.is_enabled());
    }

    #[test]
    fn test_dashboard_manager_disable() {
        let mut manager = DashboardManager::new();
        let mut config = DashboardConfig::default();
        config.enabled = false;
        manager.set_config(config);
        assert!(!manager.is_enabled());
    }

    #[test]
    fn test_dashboard_manager_get_config_returns_reference() {
        let manager = DashboardManager::new();
        let config = manager.get_config();
        assert!(config.enabled);
        assert_eq!(config.top_active_count, 5);
        assert!(config.include_prediction);
    }

    #[test]
    fn test_dashboard_manager_set_config_updates_all_fields() {
        let mut manager = DashboardManager::new();
        let config = DashboardConfig {
            enabled: false,
            top_active_count: 100,
            include_prediction: false,
            include_protocol_breakdown: false,
            include_disk_status: false,
        };
        manager.set_config(config);
        let current = manager.get_config();
        assert!(!current.enabled);
        assert_eq!(current.top_active_count, 100);
        assert!(!current.include_prediction);
        assert!(!current.include_protocol_breakdown);
        assert!(!current.include_disk_status);
    }

    // === format_summary edge case tests ===

    #[test]
    fn test_format_summary_empty_queue() {
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
            disk_status: None,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let summary = snapshot.format_summary();
        assert!(summary.contains("Total: 0"));
        assert!(summary.contains("Running: 0"));
        assert!(!summary.contains("Top Active Downloads"));
        assert!(!summary.contains("Protocol Breakdown"));
        assert!(!summary.contains("Disk Space"));
        assert!(!summary.contains("Queue Completion Prediction"));
    }

    #[test]
    fn test_format_summary_with_recycled_tasks() {
        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus {
                total: 10,
                running: 0,
                queued: 0,
                paused: 0,
                completed: 5,
                error: 0,
                recycled: 5,
            },
            current_speed_bps: 0,
            current_upload_bps: 0,
            health_status: HealthStatus::Healthy,
            health_score: 100,
            issue_count: 0,
            prediction: None,
            top_active: vec![],
            protocol_breakdown: None,
            disk_status: None,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let summary = snapshot.format_summary();
        assert!(summary.contains("Recycled: 5"));
    }

    #[test]
    fn test_format_summary_without_recycled_when_zero() {
        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus {
                total: 5,
                running: 2,
                queued: 1,
                paused: 1,
                completed: 1,
                error: 0,
                recycled: 0,
            },
            current_speed_bps: 0,
            current_upload_bps: 0,
            health_status: HealthStatus::Healthy,
            health_score: 100,
            issue_count: 0,
            prediction: None,
            top_active: vec![],
            protocol_breakdown: None,
            disk_status: None,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let summary = snapshot.format_summary();
        assert!(!summary.contains("Recycled:"));
    }

    #[test]
    fn test_format_summary_multiple_top_active() {
        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus::default(),
            current_speed_bps: 10 * 1024 * 1024,
            current_upload_bps: 0,
            health_status: HealthStatus::Healthy,
            health_score: 100,
            issue_count: 0,
            prediction: None,
            top_active: vec![
                TopActiveTask {
                    task_id: "t1".to_string(),
                    task_name: "file1.zip".to_string(),
                    progress: 0.9,
                    speed_bps: 5 * 1024 * 1024,
                    eta_seconds: Some(30.0),
                    total_size: 100 * 1024 * 1024,
                    downloaded: 90 * 1024 * 1024,
                    priority: DownloadPriority::High,
                    protocol: "HTTP".to_string(),
                },
                TopActiveTask {
                    task_id: "t2".to_string(),
                    task_name: "file2.tar.gz".to_string(),
                    progress: 0.3,
                    speed_bps: 3 * 1024 * 1024,
                    eta_seconds: Some(300.0),
                    total_size: 500 * 1024 * 1024,
                    downloaded: 150 * 1024 * 1024,
                    priority: DownloadPriority::Normal,
                    protocol: "Torrent".to_string(),
                },
                TopActiveTask {
                    task_id: "t3".to_string(),
                    task_name: "file3.iso".to_string(),
                    progress: 0.1,
                    speed_bps: 2 * 1024 * 1024,
                    eta_seconds: None,
                    total_size: 4 * 1024 * 1024 * 1024,
                    downloaded: 400 * 1024 * 1024,
                    priority: DownloadPriority::Low,
                    protocol: "Ed2k".to_string(),
                },
            ],
            protocol_breakdown: None,
            disk_status: None,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let summary = snapshot.format_summary();
        assert!(summary.contains("1. file1.zip"));
        assert!(summary.contains("2. file2.tar.gz"));
        assert!(summary.contains("3. file3.iso"));
        assert!(summary.contains("90.0%"));
        assert!(summary.contains("30.0%"));
        assert!(summary.contains("10.0%"));
        assert!(summary.contains("ETA: N/A"));
    }

    #[test]
    fn test_format_summary_all_health_statuses() {
        // HealthStatus::label() returns lowercase strings
        for (status, label) in [
            (HealthStatus::Healthy, "healthy"),
            (HealthStatus::Degraded, "degraded"),
            (HealthStatus::Critical, "critical"),
        ] {
            let snapshot = DashboardSnapshot {
                snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
                queue_status: QueueStatus::default(),
                current_speed_bps: 0,
                current_upload_bps: 0,
                health_status: status.clone(),
                health_score: 50,
                issue_count: 1,
                prediction: None,
                top_active: vec![],
                protocol_breakdown: None,
                disk_status: None,
                total_downloaded_bytes: 0,
                total_uploaded_bytes: 0,
                uptime_seconds: 0,
            };
            let summary = snapshot.format_summary();
            assert!(summary.contains(label), "Missing label {} for {:?}", label, status);
        }
    }

    #[test]
    fn test_format_summary_protocol_breakdown_partial() {
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
            protocol_breakdown: Some(ProtocolBreakdown {
                http_count: 5,
                torrent_count: 0,
                ed2k_count: 0,
                p2p_count: 0,
                magnet_count: 0,
            }),
            disk_status: None,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let summary = snapshot.format_summary();
        assert!(summary.contains("Protocol Breakdown"));
        assert!(summary.contains("HTTP/HTTPS: 5"));
        // Zero-count protocols should not appear
        assert!(!summary.contains("Torrent:"));
        assert!(!summary.contains("Ed2k:"));
    }

    #[test]
    fn test_format_summary_disk_both_warnings() {
        // Test is_low=true, is_critical=false => WARNING
        let snapshot_low = DashboardSnapshot {
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
                available_bytes: 1024,
                total_bytes: 1024 * 1024,
                usage_percent: 0.99,
                is_low: true,
                is_critical: false,
            }),
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let summary_low = snapshot_low.format_summary();
        assert!(summary_low.contains("WARNING"));
        assert!(!summary_low.contains("CRITICAL"));

        // Test is_low=true, is_critical=true => CRITICAL (also contains WARNING text in code path)
        let snapshot_critical = DashboardSnapshot {
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
                available_bytes: 100,
                total_bytes: 1024 * 1024,
                usage_percent: 0.999,
                is_low: true,
                is_critical: true,
            }),
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let summary_critical = snapshot_critical.format_summary();
        assert!(summary_critical.contains("CRITICAL"));
    }

    // === QueueStatus field coverage tests ===

    #[test]
    fn test_queue_status_all_fields_zero() {
        let status = QueueStatus {
            total: 0,
            running: 0,
            queued: 0,
            paused: 0,
            completed: 0,
            error: 0,
            recycled: 0,
        };
        assert_eq!(status.total, 0);
        assert_eq!(status.running, 0);
        assert_eq!(status.queued, 0);
        assert_eq!(status.paused, 0);
        assert_eq!(status.completed, 0);
        assert_eq!(status.error, 0);
        assert_eq!(status.recycled, 0);
    }

    #[test]
    fn test_queue_status_large_values() {
        let status = QueueStatus {
            total: usize::MAX,
            running: usize::MAX,
            queued: usize::MAX,
            paused: usize::MAX,
            completed: usize::MAX,
            error: usize::MAX,
            recycled: usize::MAX,
        };
        assert_eq!(status.total, usize::MAX);
    }

    // === DiskStatus field coverage tests ===

    #[test]
    fn test_disk_status_usage_percent_boundaries() {
        let disk_full = DiskStatus {
            available_bytes: 0,
            total_bytes: 1024,
            usage_percent: 1.0,
            is_low: true,
            is_critical: true,
        };
        assert!((disk_full.usage_percent - 1.0).abs() < f64::EPSILON);
        assert!(disk_full.is_low);
        assert!(disk_full.is_critical);
    }

    #[test]
    fn test_disk_status_zero_usage() {
        let disk = DiskStatus {
            available_bytes: 1024 * 1024 * 1024,
            total_bytes: 1024 * 1024 * 1024,
            usage_percent: 0.0,
            is_low: false,
            is_critical: false,
        };
        assert_eq!(disk.usage_percent, 0.0);
        assert!(!disk.is_low);
        assert!(!disk.is_critical);
    }

    // === TopActiveTask with different priorities ===

    #[test]
    fn test_top_active_task_all_priorities() {
        for priority in [
            DownloadPriority::Low,
            DownloadPriority::Normal,
            DownloadPriority::High,
        ] {
            let task = TopActiveTask {
                task_id: "t1".to_string(),
                task_name: "file.zip".to_string(),
                progress: 0.5,
                speed_bps: 1024,
                eta_seconds: Some(60.0),
                total_size: 2048,
                downloaded: 1024,
                priority: priority.clone(),
                protocol: "HTTP".to_string(),
            };
            assert_eq!(task.priority, priority);
        }
    }

    #[test]
    fn test_top_active_task_unicode_name() {
        let task = TopActiveTask {
            task_id: "t1".to_string(),
            task_name: "中文文件.zip".to_string(),
            progress: 0.5,
            speed_bps: 1024,
            eta_seconds: Some(60.0),
            total_size: 2048,
            downloaded: 1024,
            priority: DownloadPriority::Normal,
            protocol: "HTTP".to_string(),
        };
        assert_eq!(task.task_name, "中文文件.zip");
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: TopActiveTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_name, "中文文件.zip");
    }

    #[test]
    fn test_top_active_task_emoji_name() {
        let task = TopActiveTask {
            task_id: "t1".to_string(),
            task_name: "🎬 movie.mkv".to_string(),
            progress: 0.0,
            speed_bps: 0,
            eta_seconds: None,
            total_size: 0,
            downloaded: 0,
            priority: DownloadPriority::Normal,
            protocol: "HTTP".to_string(),
        };
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: TopActiveTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_name, "🎬 movie.mkv");
    }

    // === Full snapshot serde with all optional fields ===

    #[test]
    fn test_dashboard_snapshot_full_serde() {
        let prediction = QueueCompletionPrediction {
            predicted_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            total_eta_seconds: 3600.0,
            estimated_completion: Some("2026-08-10T13:00:00Z".parse().unwrap()),
            task_count: 10,
            reliable_estimates: 8,
            confidence: 0.85,
            task_estimates: vec![],
            active_downloads: 5,
            max_concurrent: 10,
            summary: "~1h remaining".to_string(),
        };

        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus {
                total: 20,
                running: 5,
                queued: 3,
                paused: 2,
                completed: 8,
                error: 1,
                recycled: 1,
            },
            current_speed_bps: 10 * 1024 * 1024,
            current_upload_bps: 1 * 1024 * 1024,
            health_status: HealthStatus::Degraded,
            health_score: 65,
            issue_count: 3,
            prediction: Some(prediction),
            top_active: vec![TopActiveTask {
                task_id: "t1".to_string(),
                task_name: "test.zip".to_string(),
                progress: 0.5,
                speed_bps: 5 * 1024 * 1024,
                eta_seconds: Some(120.0),
                total_size: 100 * 1024 * 1024,
                downloaded: 50 * 1024 * 1024,
                priority: DownloadPriority::High,
                protocol: "HTTP".to_string(),
            }],
            protocol_breakdown: Some(ProtocolBreakdown {
                http_count: 10,
                torrent_count: 5,
                ed2k_count: 2,
                p2p_count: 1,
                magnet_count: 2,
            }),
            disk_status: Some(DiskStatus {
                available_bytes: 50 * 1024 * 1024 * 1024,
                total_bytes: 500 * 1024 * 1024 * 1024,
                usage_percent: 0.9,
                is_low: true,
                is_critical: false,
            }),
            total_downloaded_bytes: 100 * 1024 * 1024 * 1024,
            total_uploaded_bytes: 10 * 1024 * 1024 * 1024,
            uptime_seconds: 86400,
        };

        let json = serde_json::to_string_pretty(&snapshot).unwrap();
        let deserialized: DashboardSnapshot = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.queue_status.total, 20);
        assert_eq!(deserialized.queue_status.recycled, 1);
        assert_eq!(deserialized.current_speed_bps, 10 * 1024 * 1024);
        assert_eq!(deserialized.health_score, 65);
        assert!(deserialized.prediction.is_some());
        assert_eq!(deserialized.prediction.unwrap().task_count, 10);
        assert_eq!(deserialized.top_active.len(), 1);
        assert!(deserialized.protocol_breakdown.is_some());
        assert!(deserialized.disk_status.is_some());
        assert!(deserialized.disk_status.unwrap().is_low);
        assert_eq!(deserialized.total_downloaded_bytes, 100 * 1024 * 1024 * 1024);
        assert_eq!(deserialized.uptime_seconds, 86400);
    }

    // === format_summary speed and size formatting in context ===

    #[test]
    fn test_format_summary_speed_formatting() {
        let snapshot = DashboardSnapshot {
            snapshot_at: "2026-08-10T12:00:00Z".parse().unwrap(),
            queue_status: QueueStatus::default(),
            current_speed_bps: 1024 * 1024, // 1 MB/s
            current_upload_bps: 512 * 1024,  // 512 KB/s
            health_status: HealthStatus::Healthy,
            health_score: 100,
            issue_count: 0,
            prediction: None,
            top_active: vec![],
            protocol_breakdown: None,
            disk_status: None,
            total_downloaded_bytes: 1024 * 1024 * 1024, // 1 GB
            total_uploaded_bytes: 512 * 1024 * 1024,     // 512 MB
            uptime_seconds: 3600,
        };
        let summary = snapshot.format_summary();
        assert!(summary.contains("1.00 MB/s"));
        assert!(summary.contains("512.00 KB/s"));
        assert!(summary.contains("1.00 GB"));
        assert!(summary.contains("512.00 MB"));
    }

    #[test]
    fn test_format_summary_zero_speed() {
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
            disk_status: None,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
            uptime_seconds: 0,
        };
        let summary = snapshot.format_summary();
        assert!(summary.contains("0 B/s"));
        assert!(summary.contains("0 B"));
    }

    // === DashboardConfig custom values ===

    #[test]
    fn test_dashboard_config_custom_values() {
        let config = DashboardConfig {
            enabled: true,
            top_active_count: 0,
            include_prediction: false,
            include_protocol_breakdown: true,
            include_disk_status: false,
        };
        assert!(config.enabled);
        assert_eq!(config.top_active_count, 0);
        assert!(!config.include_prediction);
        assert!(config.include_protocol_breakdown);
        assert!(!config.include_disk_status);
    }

    #[test]
    fn test_dashboard_config_max_top_active() {
        let config = DashboardConfig {
            enabled: true,
            top_active_count: usize::MAX,
            include_prediction: true,
            include_protocol_breakdown: true,
            include_disk_status: true,
        };
        assert_eq!(config.top_active_count, usize::MAX);
    }
}
