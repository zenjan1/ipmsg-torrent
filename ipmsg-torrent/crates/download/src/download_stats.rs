//! Download Statistics and Analytics
//!
//! Comprehensive statistics tracking for download performance analysis.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistics for a single protocol
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProtocolStats {
    /// Total number of downloads
    pub total: u64,
    /// Number of completed downloads
    pub completed: u64,
    /// Number of failed downloads
    pub failed: u64,
    /// Number of paused downloads
    pub paused: u64,
    /// Number of running downloads
    pub running: u64,
    /// Total bytes downloaded
    pub bytes_downloaded: u64,
    /// Average download speed (bytes/sec)
    pub avg_speed_bps: f64,
    /// Peak download speed (bytes/sec)
    pub peak_speed_bps: f64,
}

impl ProtocolStats {
    pub fn success_rate(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.completed as f64 / self.total as f64) * 100.0
    }

    pub fn failure_rate(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.failed as f64 / self.total as f64) * 100.0
    }
}

/// Overall download statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloadStatistics {
    /// Statistics by protocol
    pub by_protocol: HashMap<String, ProtocolStats>,
    /// Total downloads across all protocols
    pub total_downloads: u64,
    /// Total completed downloads
    pub total_completed: u64,
    /// Total failed downloads
    pub total_failed: u64,
    /// Total bytes downloaded
    pub total_bytes: u64,
    /// Overall average speed (bytes/sec)
    pub overall_avg_speed: f64,
    /// Overall peak speed (bytes/sec)
    pub overall_peak_speed: f64,
    /// Statistics collection start time
    pub since: DateTime<Utc>,
    /// Last update time
    pub last_updated: DateTime<Utc>,
}

impl DownloadStatistics {
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            since: now,
            last_updated: now,
            ..Default::default()
        }
    }

    pub fn overall_success_rate(&self) -> f64 {
        if self.total_downloads == 0 {
            return 0.0;
        }
        (self.total_completed as f64 / self.total_downloads as f64) * 100.0
    }

    pub fn overall_failure_rate(&self) -> f64 {
        if self.total_downloads == 0 {
            return 0.0;
        }
        (self.total_failed as f64 / self.total_downloads as f64) * 100.0
    }

    pub fn format_display(&self) -> String {
        let mut output = String::from("📊 Download Statistics\n");
        output.push_str(&format!(
            "  Period: {} to {}\n",
            self.since.format("%Y-%m-%d %H:%M"),
            self.last_updated.format("%Y-%m-%d %H:%M")
        ));
        output.push_str(&format!("  Total Downloads: {}\n", self.total_downloads));
        output.push_str(&format!(
            "  Completed: {} ({:.1}%)\n",
            self.total_completed,
            self.overall_success_rate()
        ));
        output.push_str(&format!(
            "  Failed: {} ({:.1}%)\n",
            self.total_failed,
            self.overall_failure_rate()
        ));
        output.push_str(&format!(
            "  Total Data: {}\n",
            format_bytes(self.total_bytes)
        ));
        output.push_str(&format!(
            "  Avg Speed: {}/s\n",
            format_bytes(self.overall_avg_speed as u64)
        ));
        output.push_str(&format!(
            "  Peak Speed: {}/s\n",
            format_bytes(self.overall_peak_speed as u64)
        ));

        if !self.by_protocol.is_empty() {
            output.push_str("\n  By Protocol:\n");
            for (protocol, stats) in &self.by_protocol {
                output.push_str(&format!("    {}:\n", protocol.to_uppercase()));
                output.push_str(&format!(
                    "      Downloads: {} ({} completed, {} failed)\n",
                    stats.total, stats.completed, stats.failed
                ));
                output.push_str(&format!(
                    "      Success Rate: {:.1}%\n",
                    stats.success_rate()
                ));
                output.push_str(&format!(
                    "      Data: {}\n",
                    format_bytes(stats.bytes_downloaded)
                ));
                output.push_str(&format!(
                    "      Avg Speed: {}/s\n",
                    format_bytes(stats.avg_speed_bps as u64)
                ));
            }
        }

        output
    }
}

/// Manager for download statistics
#[derive(Debug, Clone)]
pub struct StatsManager {
    stats: DownloadStatistics,
}

impl Default for StatsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsManager {
    pub fn new() -> Self {
        Self {
            stats: DownloadStatistics::new(),
        }
    }

    pub fn get_stats(&self) -> &DownloadStatistics {
        &self.stats
    }

    pub fn record_download_started(&mut self, protocol: &str) {
        let stats = self
            .stats
            .by_protocol
            .entry(protocol.to_string())
            .or_default();
        stats.total += 1;
        stats.running += 1;
        self.stats.total_downloads += 1;
        self.stats.last_updated = Utc::now();
    }

    pub fn record_download_completed(&mut self, protocol: &str, bytes: u64, speed_bps: f64) {
        let stats = self
            .stats
            .by_protocol
            .entry(protocol.to_string())
            .or_default();
        stats.completed += 1;
        stats.running = stats.running.saturating_sub(1);
        stats.bytes_downloaded += bytes;

        // Update average speed (simple moving average)
        if stats.avg_speed_bps == 0.0 {
            stats.avg_speed_bps = speed_bps;
        } else {
            stats.avg_speed_bps = (stats.avg_speed_bps + speed_bps) / 2.0;
        }

        if speed_bps > stats.peak_speed_bps {
            stats.peak_speed_bps = speed_bps;
        }

        self.stats.total_completed += 1;
        self.stats.total_bytes += bytes;

        if speed_bps > self.stats.overall_peak_speed {
            self.stats.overall_peak_speed = speed_bps;
        }

        // Update overall average
        if self.stats.overall_avg_speed == 0.0 {
            self.stats.overall_avg_speed = speed_bps;
        } else {
            self.stats.overall_avg_speed = (self.stats.overall_avg_speed + speed_bps) / 2.0;
        }

        self.stats.last_updated = Utc::now();
    }

    pub fn record_download_failed(&mut self, protocol: &str) {
        let stats = self
            .stats
            .by_protocol
            .entry(protocol.to_string())
            .or_default();
        stats.failed += 1;
        stats.running = stats.running.saturating_sub(1);
        self.stats.total_failed += 1;
        self.stats.last_updated = Utc::now();
    }

    pub fn record_download_paused(&mut self, protocol: &str) {
        let stats = self
            .stats
            .by_protocol
            .entry(protocol.to_string())
            .or_default();
        stats.paused += 1;
        stats.running = stats.running.saturating_sub(1);
        self.stats.last_updated = Utc::now();
    }

    pub fn record_download_resumed(&mut self, protocol: &str) {
        let stats = self
            .stats
            .by_protocol
            .entry(protocol.to_string())
            .or_default();
        stats.paused = stats.paused.saturating_sub(1);
        stats.running += 1;
        self.stats.last_updated = Utc::now();
    }

    pub fn reset(&mut self) {
        self.stats = DownloadStatistics::new();
    }
}

/// Format bytes into human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_stats_success_rate() {
        let mut stats = ProtocolStats::default();
        stats.total = 10;
        stats.completed = 8;
        stats.failed = 2;

        assert_eq!(stats.success_rate(), 80.0);
        assert_eq!(stats.failure_rate(), 20.0);
    }

    #[test]
    fn test_protocol_stats_zero_division() {
        let stats = ProtocolStats::default();
        assert_eq!(stats.success_rate(), 0.0);
        assert_eq!(stats.failure_rate(), 0.0);
    }

    #[test]
    fn test_download_statistics_new() {
        let stats = DownloadStatistics::new();
        assert_eq!(stats.total_downloads, 0);
        assert_eq!(stats.total_completed, 0);
        assert_eq!(stats.total_failed, 0);
        assert_eq!(stats.total_bytes, 0);
    }

    #[test]
    fn test_download_statistics_overall_rates() {
        let mut stats = DownloadStatistics::new();
        stats.total_downloads = 100;
        stats.total_completed = 85;
        stats.total_failed = 15;

        assert_eq!(stats.overall_success_rate(), 85.0);
        assert_eq!(stats.overall_failure_rate(), 15.0);
    }

    #[test]
    fn test_stats_manager_record_started() {
        let mut manager = StatsManager::new();
        manager.record_download_started("http");

        let stats = manager.get_stats();
        assert_eq!(stats.total_downloads, 1);
        assert_eq!(stats.by_protocol["http"].total, 1);
        assert_eq!(stats.by_protocol["http"].running, 1);
    }

    #[test]
    fn test_stats_manager_record_completed() {
        let mut manager = StatsManager::new();
        manager.record_download_started("http");
        manager.record_download_completed("http", 1024 * 1024, 500_000.0);

        let stats = manager.get_stats();
        assert_eq!(stats.total_completed, 1);
        assert_eq!(stats.total_bytes, 1024 * 1024);
        assert_eq!(stats.by_protocol["http"].completed, 1);
        assert_eq!(stats.by_protocol["http"].bytes_downloaded, 1024 * 1024);
        assert_eq!(stats.by_protocol["http"].avg_speed_bps, 500_000.0);
    }

    #[test]
    fn test_stats_manager_record_failed() {
        let mut manager = StatsManager::new();
        manager.record_download_started("torrent");
        manager.record_download_failed("torrent");

        let stats = manager.get_stats();
        assert_eq!(stats.total_failed, 1);
        assert_eq!(stats.by_protocol["torrent"].failed, 1);
        assert_eq!(stats.by_protocol["torrent"].running, 0);
    }

    #[test]
    fn test_stats_manager_record_paused_resumed() {
        let mut manager = StatsManager::new();
        manager.record_download_started("http");
        manager.record_download_paused("http");

        let stats = manager.get_stats();
        assert_eq!(stats.by_protocol["http"].paused, 1);
        assert_eq!(stats.by_protocol["http"].running, 0);

        manager.record_download_resumed("http");
        let stats = manager.get_stats();
        assert_eq!(stats.by_protocol["http"].paused, 0);
        assert_eq!(stats.by_protocol["http"].running, 1);
    }

    #[test]
    fn test_stats_manager_multiple_protocols() {
        let mut manager = StatsManager::new();

        manager.record_download_started("http");
        manager.record_download_started("torrent");
        manager.record_download_started("ed2k");

        manager.record_download_completed("http", 1000, 100.0);
        manager.record_download_completed("torrent", 2000, 200.0);
        manager.record_download_failed("ed2k");

        let stats = manager.get_stats();
        assert_eq!(stats.total_downloads, 3);
        assert_eq!(stats.total_completed, 2);
        assert_eq!(stats.total_failed, 1);
        assert_eq!(stats.by_protocol.len(), 3);
    }

    #[test]
    fn test_stats_manager_reset() {
        let mut manager = StatsManager::new();
        manager.record_download_started("http");
        manager.record_download_completed("http", 1000, 100.0);

        manager.reset();

        let stats = manager.get_stats();
        assert_eq!(stats.total_downloads, 0);
        assert_eq!(stats.total_completed, 0);
        assert_eq!(stats.by_protocol.len(), 0);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(1024u64 * 1024 * 1024 * 1024), "1.00 TB");
    }

    #[test]
    fn test_download_statistics_display() {
        let mut stats = DownloadStatistics::new();
        stats.total_downloads = 10;
        stats.total_completed = 8;
        stats.total_failed = 2;
        stats.total_bytes = 1024 * 1024 * 100; // 100 MB
        stats.overall_avg_speed = 500_000.0;
        stats.overall_peak_speed = 1_000_000.0;

        let display = stats.format_display();
        assert!(display.contains("Download Statistics"));
        assert!(display.contains("Total Downloads: 10"));
        assert!(display.contains("Completed: 8"));
        assert!(display.contains("Failed: 2"));
        assert!(display.contains("100.00 MB"));
    }
}
