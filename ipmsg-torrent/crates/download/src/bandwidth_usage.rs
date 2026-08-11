//! Bandwidth Usage Tracker - Track bandwidth consumption by hour and protocol
//!
//! Provides rolling 24-hour window analysis, peak hour detection,
//! and per-protocol bandwidth usage tracking.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for bandwidth usage tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthUsageConfig {
    /// Enable bandwidth tracking
    pub enabled: bool,
    /// Maximum hourly samples to keep (default: 720 = 30 days)
    pub max_hourly_samples: usize,
    /// Peak hour threshold in bytes (hours exceeding this are flagged)
    pub peak_threshold_bytes: u64,
    /// Track per-protocol breakdown
    pub track_protocol_breakdown: bool,
}

impl Default for BandwidthUsageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_hourly_samples: 720,
            peak_threshold_bytes: 1_073_741_824, // 1 GB/hour
            track_protocol_breakdown: true,
        }
    }
}

/// Per-protocol bandwidth breakdown
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProtocolBreakdown {
    /// HTTP/HTTPS download bytes
    pub http_bytes: u64,
    /// Torrent download bytes
    pub torrent_bytes: u64,
    /// Ed2k download bytes
    pub ed2k_bytes: u64,
    /// P2P download bytes
    pub p2p_bytes: u64,
}

impl ProtocolBreakdown {
    /// Total bytes across all protocols
    pub fn total(&self) -> u64 {
        self.http_bytes + self.torrent_bytes + self.ed2k_bytes + self.p2p_bytes
    }

    /// Add bytes for a specific protocol
    pub fn add(&mut self, protocol: &str, bytes: u64) {
        match protocol {
            "http" | "https" | "xunlei" => self.http_bytes += bytes,
            "torrent" => self.torrent_bytes += bytes,
            "ed2k" => self.ed2k_bytes += bytes,
            "p2p" => self.p2p_bytes += bytes,
            _ => self.http_bytes += bytes,
        }
    }
}

/// Bandwidth sample for a single hour
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlySample {
    /// Unix timestamp of the hour start (truncated to hour boundary)
    pub hour_ts: u64,
    /// Total download bytes in this hour
    pub download_bytes: u64,
    /// Total upload bytes in this hour
    pub upload_bytes: u64,
    /// Per-protocol breakdown
    pub breakdown: ProtocolBreakdown,
    /// Number of active tasks during this hour
    pub active_tasks: u32,
}

/// Rolling 24-hour window summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollingWindowSummary {
    /// Window size in hours
    pub window_hours: usize,
    /// Total download bytes in window
    pub total_download_bytes: u64,
    /// Total upload bytes in window
    pub total_upload_bytes: u64,
    /// Average bytes per hour
    pub avg_bytes_per_hour: u64,
    /// Peak hour bytes
    pub peak_bytes: u64,
    /// Peak hour timestamp
    pub peak_hour_ts: Option<u64>,
    /// Lowest hour bytes (non-zero)
    pub lowest_bytes: u64,
    /// Lowest hour timestamp (non-zero)
    pub lowest_hour_ts: Option<u64>,
    /// Hours with data
    pub hours_with_data: usize,
    /// Average speed in bytes per second (over active hours)
    pub avg_speed_bps: f64,
}

/// Peak hour analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeakHourAnalysis {
    /// Top N peak hours sorted by download volume
    pub top_hours: Vec<PeakHourEntry>,
    /// Average download during peak hours
    pub avg_peak_bytes: u64,
    /// Most common day of week for peak hours (0=Sun, 6=Sat)
    pub common_peak_day: Option<u8>,
    /// Most common hour of day for peaks (0-23)
    pub common_peak_hour: Option<u8>,
}

/// Single peak hour entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeakHourEntry {
    /// Hour timestamp
    pub hour_ts: u64,
    /// Download bytes
    pub download_bytes: u64,
    /// Upload bytes
    pub upload_bytes: u64,
    /// Protocol breakdown
    pub breakdown: ProtocolBreakdown,
    /// Day of week (0=Sun, 6=Sat)
    pub day_of_week: u8,
    /// Hour of day (0-23)
    pub hour_of_day: u8,
}

/// Overall bandwidth usage summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthUsageSummary {
    /// Total download bytes tracked
    pub total_download_bytes: u64,
    /// Total upload bytes tracked
    pub total_upload_bytes: u64,
    /// Number of hourly samples
    pub total_samples: usize,
    /// Rolling 24-hour summary
    pub rolling_24h: RollingWindowSummary,
    /// Peak hour analysis
    pub peak_analysis: PeakHourAnalysis,
    /// Current hour bytes so far
    pub current_hour_bytes: u64,
    /// Hours exceeding peak threshold
    pub peak_hour_count: usize,
}

/// Bandwidth usage tracker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthUsageTracker {
    /// Configuration
    config: BandwidthUsageConfig,
    /// Hourly samples (oldest first)
    samples: VecDeque<HourlySample>,
    /// Current hour accumulator
    current_hour: Option<HourlyAccumulator>,
    /// Current hour boundary timestamp
    current_hour_ts: u64,
}

/// Accumulator for the current (in-progress) hour
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HourlyAccumulator {
    download_bytes: u64,
    upload_bytes: u64,
    breakdown: ProtocolBreakdown,
    active_task_ids: Vec<u64>,
}

impl BandwidthUsageTracker {
    /// Create a new tracker with default config
    pub fn new() -> Self {
        Self::with_config(BandwidthUsageConfig::default())
    }

    /// Create a new tracker with custom config
    pub fn with_config(config: BandwidthUsageConfig) -> Self {
        Self {
            config,
            samples: VecDeque::new(),
            current_hour: Some(HourlyAccumulator::default()),
            current_hour_ts: Self::current_hour_boundary(),
        }
    }

    /// Record download bytes for the current hour
    pub fn record_download(&mut self, bytes: u64, protocol: &str) {
        if !self.config.enabled || bytes == 0 {
            return;
        }
        self.roll_hour_if_needed();
        if let Some(ref mut acc) = self.current_hour {
            acc.download_bytes += bytes;
            acc.breakdown.add(protocol, bytes);
        }
    }

    /// Record upload bytes for the current hour
    pub fn record_upload(&mut self, bytes: u64) {
        if !self.config.enabled || bytes == 0 {
            return;
        }
        self.roll_hour_if_needed();
        if let Some(ref mut acc) = self.current_hour {
            acc.upload_bytes += bytes;
        }
    }

    /// Track an active task ID for the current hour
    pub fn track_active_task(&mut self, task_id: u64) {
        if !self.config.enabled {
            return;
        }
        self.roll_hour_if_needed();
        if let Some(ref mut acc) = self.current_hour {
            if !acc.active_task_ids.contains(&task_id) {
                acc.active_task_ids.push(task_id);
            }
        }
    }

    /// Get the rolling 24-hour window summary
    pub fn rolling_24h_summary(&self) -> RollingWindowSummary {
        self.rolling_window_summary(24)
    }

    /// Get a rolling window summary for N hours
    pub fn rolling_window_summary(&self, hours: usize) -> RollingWindowSummary {
        let cutoff = Self::current_hour_boundary() - (hours as u64 * 3600);
        let recent: Vec<&HourlySample> = self
            .samples
            .iter()
            .filter(|s| s.hour_ts >= cutoff)
            .collect();

        // Include current hour if it has data
        let current_data = self.current_hour.as_ref();
        let current_bytes = current_data.map(|a| a.download_bytes).unwrap_or(0);
        let current_upload = current_data.map(|a| a.upload_bytes).unwrap_or(0);

        let total_download: u64 = recent.iter().map(|s| s.download_bytes).sum::<u64>() + current_bytes;
        let total_upload: u64 = recent.iter().map(|s| s.upload_bytes).sum::<u64>() + current_upload;

        let hours_with_data = recent.len() + if current_bytes > 0 { 1 } else { 0 };

        let avg_bytes = if hours_with_data > 0 {
            total_download / hours_with_data as u64
        } else {
            0
        };

        let mut peak_bytes = current_bytes;
        let mut peak_ts = if current_bytes > 0 {
            Some(self.current_hour_ts)
        } else {
            None
        };
        let mut lowest_bytes = if current_bytes > 0 {
            current_bytes
        } else {
            u64::MAX
        };
        let mut lowest_ts = if current_bytes > 0 {
            Some(self.current_hour_ts)
        } else {
            None
        };

        for sample in &recent {
            if sample.download_bytes > peak_bytes {
                peak_bytes = sample.download_bytes;
                peak_ts = Some(sample.hour_ts);
            }
            if sample.download_bytes > 0 && sample.download_bytes < lowest_bytes {
                lowest_bytes = sample.download_bytes;
                lowest_ts = Some(sample.hour_ts);
            }
        }

        if lowest_bytes == u64::MAX {
            lowest_bytes = 0;
        }

        let avg_speed = if hours_with_data > 0 {
            total_download as f64 / (hours_with_data as f64 * 3600.0)
        } else {
            0.0
        };

        RollingWindowSummary {
            window_hours: hours,
            total_download_bytes: total_download,
            total_upload_bytes: total_upload,
            avg_bytes_per_hour: avg_bytes,
            peak_bytes,
            peak_hour_ts: peak_ts,
            lowest_bytes,
            lowest_hour_ts: lowest_ts,
            hours_with_data,
            avg_speed_bps: avg_speed,
        }
    }

    /// Analyze peak hours (top N by download volume)
    pub fn peak_hour_analysis(&self, top_n: usize) -> PeakHourAnalysis {
        let mut entries: Vec<PeakHourEntry> = self
            .samples
            .iter()
            .map(|s| {
                let dt = Self::ts_to_datetime(s.hour_ts);
                PeakHourEntry {
                    hour_ts: s.hour_ts,
                    download_bytes: s.download_bytes,
                    upload_bytes: s.upload_bytes,
                    breakdown: s.breakdown.clone(),
                    day_of_week: dt.2,
                    hour_of_day: dt.3,
                }
            })
            .collect();

        // Include current hour
        if let Some(ref acc) = self.current_hour {
            if acc.download_bytes > 0 {
                let dt = Self::ts_to_datetime(self.current_hour_ts);
                entries.push(PeakHourEntry {
                    hour_ts: self.current_hour_ts,
                    download_bytes: acc.download_bytes,
                    upload_bytes: acc.upload_bytes,
                    breakdown: acc.breakdown.clone(),
                    day_of_week: dt.2,
                    hour_of_day: dt.3,
                });
            }
        }

        entries.sort_by(|a, b| b.download_bytes.cmp(&a.download_bytes));
        entries.truncate(top_n);

        let avg_peak = if entries.is_empty() {
            0
        } else {
            entries.iter().map(|e| e.download_bytes).sum::<u64>() / entries.len() as u64
        };

        // Most common day/hour
        let mut day_counts = [0u32; 7];
        let mut hour_counts = [0u32; 24];
        for entry in &entries {
            day_counts[entry.day_of_week as usize] += 1;
            hour_counts[entry.hour_of_day as usize] += 1;
        }

        let common_day = day_counts
            .into_iter()
            .enumerate()
            .max_by_key(|(_, c)| *c)
            .filter(|(_, c)| *c > 0)
            .map(|(i, _)| i as u8);

        let common_hour = hour_counts
            .into_iter()
            .enumerate()
            .max_by_key(|(_, c)| *c)
            .filter(|(_, c)| *c > 0)
            .map(|(i, _)| i as u8);

        PeakHourAnalysis {
            top_hours: entries,
            avg_peak_bytes: avg_peak,
            common_peak_day: common_day,
            common_peak_hour: common_hour,
        }
    }

    /// Get overall summary
    pub fn summary(&self) -> BandwidthUsageSummary {
        let total_download: u64 = self.samples.iter().map(|s| s.download_bytes).sum::<u64>()
            + self
                .current_hour
                .as_ref()
                .map(|a| a.download_bytes)
                .unwrap_or(0);
        let total_upload: u64 = self.samples.iter().map(|s| s.upload_bytes).sum::<u64>()
            + self
                .current_hour
                .as_ref()
                .map(|a| a.upload_bytes)
                .unwrap_or(0);

        let rolling = self.rolling_24h_summary();
        let peak = self.peak_hour_analysis(10);
        let current_bytes = self
            .current_hour
            .as_ref()
            .map(|a| a.download_bytes)
            .unwrap_or(0);
        let peak_count = self
            .samples
            .iter()
            .filter(|s| s.download_bytes >= self.config.peak_threshold_bytes)
            .count();

        BandwidthUsageSummary {
            total_download_bytes: total_download,
            total_upload_bytes: total_upload,
            total_samples: self.samples.len(),
            rolling_24h: rolling,
            peak_analysis: peak,
            current_hour_bytes: current_bytes,
            peak_hour_count: peak_count,
        }
    }

    /// Get all hourly samples
    pub fn samples(&self) -> &VecDeque<HourlySample> {
        &self.samples
    }

    /// Get config
    pub fn config(&self) -> &BandwidthUsageConfig {
        &self.config
    }

    /// Set config
    pub fn set_config(&mut self, config: BandwidthUsageConfig) {
        self.config = config;
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.samples.clear();
        self.current_hour = Some(HourlyAccumulator::default());
        self.current_hour_ts = Self::current_hour_boundary();
    }

    /// Prune old samples beyond max_hourly_samples
    pub fn prune(&mut self) {
        while self.samples.len() > self.config.max_hourly_samples {
            self.samples.pop_front();
        }
    }

    /// Format bytes as human-readable string
    pub fn format_bytes(bytes: u64) -> String {
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

    /// Format summary as human-readable string
    pub fn format_summary(&self) -> String {
        let summary = self.summary();
        let mut out = String::new();

        out.push_str("📊 Bandwidth Usage Summary\n");
        out.push_str(&format!(
            "  Total Download: {}\n",
            Self::format_bytes(summary.total_download_bytes)
        ));
        out.push_str(&format!(
            "  Total Upload:   {}\n",
            Self::format_bytes(summary.total_upload_bytes)
        ));
        out.push_str(&format!("  Samples:        {}\n", summary.total_samples));
        out.push_str(&format!(
            "  Current Hour:   {}\n",
            Self::format_bytes(summary.current_hour_bytes)
        ));
        out.push_str(&format!(
            "  Peak Hours:     {} (≥{})\n",
            summary.peak_hour_count,
            Self::format_bytes(self.config.peak_threshold_bytes)
        ));

        out.push_str("\n📈 Rolling 24h:\n");
        let r = &summary.rolling_24h;
        out.push_str(&format!(
            "  Download:       {}\n",
            Self::format_bytes(r.total_download_bytes)
        ));
        out.push_str(&format!(
            "  Avg/Hour:       {}\n",
            Self::format_bytes(r.avg_bytes_per_hour)
        ));
        out.push_str(&format!(
            "  Peak:           {} (avg speed: {}/s)\n",
            Self::format_bytes(r.peak_bytes),
            Self::format_bytes(r.avg_speed_bps as u64)
        ));
        out.push_str(&format!(
            "  Active Hours:   {}/{}\n",
            r.hours_with_data, r.window_hours
        ));

        if !summary.peak_analysis.top_hours.is_empty() {
            out.push_str("\n🔥 Top Peak Hours:\n");
            for (i, entry) in summary.peak_analysis.top_hours.iter().take(5).enumerate() {
                out.push_str(&format!(
                    "  {}. {} - {} (day={}, hour={:02})\n",
                    i + 1,
                    Self::format_bytes(entry.download_bytes),
                    entry.hour_ts,
                    entry.day_of_week,
                    entry.hour_of_day,
                ));
            }
        }

        out
    }

    // --- Internal helpers ---

    fn roll_hour_if_needed(&mut self) {
        let now_hour = Self::current_hour_boundary();
        if now_hour == self.current_hour_ts {
            return;
        }

        // Finalize current hour into samples
        if let Some(acc) = self.current_hour.take() {
            if acc.download_bytes > 0 || acc.upload_bytes > 0 {
                let sample = HourlySample {
                    hour_ts: self.current_hour_ts,
                    download_bytes: acc.download_bytes,
                    upload_bytes: acc.upload_bytes,
                    breakdown: acc.breakdown,
                    active_tasks: acc.active_task_ids.len() as u32,
                };
                self.samples.push_back(sample);
                self.prune();
            }
        }

        // Start new hour
        self.current_hour_ts = now_hour;
        self.current_hour = Some(HourlyAccumulator::default());
    }

    fn current_hour_boundary() -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now - (now % 3600)
    }

    /// Convert unix timestamp to (year, month, day_of_week, hour)
    /// Simplified calculation without external chrono dependency
    fn ts_to_datetime(ts: u64) -> (u32, u32, u8, u8) {
        let secs = ts % 86400;
        let hour = (secs / 3600) as u8;

        // Day of week: Jan 1 1970 was Thursday (4)
        let days_since_epoch = ts / 86400;
        let day_of_week = ((days_since_epoch + 4) % 7) as u8;

        // Approximate year/month (good enough for display)
        let mut year = 1970u32;
        let mut remaining_days = days_since_epoch;

        loop {
            let days_in_year = if Self::is_leap_year(year) {
                366
            } else {
                365
            };
            if remaining_days < days_in_year {
                break;
            }
            remaining_days -= days_in_year;
            year += 1;
        }

        let leap = Self::is_leap_year(year);
        let month_days = [
            31,
            if leap { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut month = 1u32;
        for &md in &month_days {
            if remaining_days < md {
                break;
            }
            remaining_days -= md;
            month += 1;
        }

        (year, month, day_of_week, hour)
    }

    fn is_leap_year(year: u32) -> bool {
        (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
    }
}

impl Default for BandwidthUsageTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = BandwidthUsageConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_hourly_samples, 720);
        assert_eq!(config.peak_threshold_bytes, 1_073_741_824);
        assert!(config.track_protocol_breakdown);
    }

    #[test]
    fn test_protocol_breakdown() {
        let mut bd = ProtocolBreakdown::default();
        bd.add("http", 100);
        bd.add("https", 200);
        bd.add("torrent", 300);
        bd.add("ed2k", 400);
        bd.add("p2p", 500);
        assert_eq!(bd.total(), 1500);
        assert_eq!(bd.http_bytes, 300); // http + https
        assert_eq!(bd.torrent_bytes, 300);
        assert_eq!(bd.ed2k_bytes, 400);
        assert_eq!(bd.p2p_bytes, 500);
    }

    #[test]
    fn test_protocol_breakdown_unknown() {
        let mut bd = ProtocolBreakdown::default();
        bd.add("unknown_protocol", 100);
        assert_eq!(bd.http_bytes, 100); // unknown goes to http
        assert_eq!(bd.total(), 100);
    }

    #[test]
    fn test_tracker_new() {
        let tracker = BandwidthUsageTracker::new();
        assert!(tracker.config().enabled);
        assert!(tracker.samples().is_empty());
    }

    #[test]
    fn test_record_download() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(1000, "http");
        tracker.record_download(2000, "torrent");

        let acc = tracker.current_hour.as_ref().unwrap();
        assert_eq!(acc.download_bytes, 3000);
        assert_eq!(acc.breakdown.http_bytes, 1000);
        assert_eq!(acc.breakdown.torrent_bytes, 2000);
    }

    #[test]
    fn test_record_upload() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_upload(500);
        tracker.record_upload(300);

        let acc = tracker.current_hour.as_ref().unwrap();
        assert_eq!(acc.upload_bytes, 800);
    }

    #[test]
    fn test_record_zero_ignored() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(0, "http");
        tracker.record_upload(0);

        let acc = tracker.current_hour.as_ref().unwrap();
        assert_eq!(acc.download_bytes, 0);
        assert_eq!(acc.upload_bytes, 0);
    }

    #[test]
    fn test_disabled_tracker() {
        let mut tracker = BandwidthUsageTracker::with_config(BandwidthUsageConfig {
            enabled: false,
            ..Default::default()
        });
        tracker.record_download(1000, "http");
        tracker.record_upload(500);

        let acc = tracker.current_hour.as_ref().unwrap();
        assert_eq!(acc.download_bytes, 0);
        assert_eq!(acc.upload_bytes, 0);
    }

    #[test]
    fn test_track_active_task() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.track_active_task(1);
        tracker.track_active_task(2);
        tracker.track_active_task(1); // duplicate

        let acc = tracker.current_hour.as_ref().unwrap();
        assert_eq!(acc.active_task_ids.len(), 2);
    }

    #[test]
    fn test_rolling_summary_empty() {
        let tracker = BandwidthUsageTracker::new();
        let summary = tracker.rolling_24h_summary();
        assert_eq!(summary.total_download_bytes, 0);
        assert_eq!(summary.total_upload_bytes, 0);
        assert_eq!(summary.hours_with_data, 0);
        assert_eq!(summary.avg_bytes_per_hour, 0);
        assert_eq!(summary.peak_bytes, 0);
    }

    #[test]
    fn test_rolling_summary_with_current_hour() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(5000, "http");
        tracker.record_upload(1000);

        let summary = tracker.rolling_24h_summary();
        assert_eq!(summary.total_download_bytes, 5000);
        assert_eq!(summary.total_upload_bytes, 1000);
        assert_eq!(summary.hours_with_data, 1);
        assert_eq!(summary.avg_bytes_per_hour, 5000);
        assert_eq!(summary.peak_bytes, 5000);
    }

    #[test]
    fn test_rolling_window_custom_hours() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(1000, "http");

        let summary = tracker.rolling_window_summary(1);
        assert_eq!(summary.total_download_bytes, 1000);
        assert_eq!(summary.window_hours, 1);
    }

    #[test]
    fn test_peak_analysis_empty() {
        let tracker = BandwidthUsageTracker::new();
        let analysis = tracker.peak_hour_analysis(5);
        assert!(analysis.top_hours.is_empty());
        assert_eq!(analysis.avg_peak_bytes, 0);
        assert!(analysis.common_peak_day.is_none());
        assert!(analysis.common_peak_hour.is_none());
    }

    #[test]
    fn test_peak_analysis_with_current() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(5000, "torrent");

        let analysis = tracker.peak_hour_analysis(5);
        assert_eq!(analysis.top_hours.len(), 1);
        assert_eq!(analysis.top_hours[0].download_bytes, 5000);
        assert_eq!(analysis.avg_peak_bytes, 5000);
    }

    #[test]
    fn test_overall_summary() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(10000, "http");
        tracker.record_upload(2000);

        let summary = tracker.summary();
        assert_eq!(summary.total_download_bytes, 10000);
        assert_eq!(summary.total_upload_bytes, 2000);
        assert_eq!(summary.current_hour_bytes, 10000);
        assert_eq!(summary.total_samples, 0); // no completed hours yet
    }

    #[test]
    fn test_clear() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(1000, "http");
        tracker.clear();

        let acc = tracker.current_hour.as_ref().unwrap();
        assert_eq!(acc.download_bytes, 0);
        assert!(tracker.samples().is_empty());
    }

    #[test]
    fn test_prune() {
        let mut tracker = BandwidthUsageTracker::with_config(BandwidthUsageConfig {
            max_hourly_samples: 3,
            ..Default::default()
        });

        // Manually add samples
        for i in 0..5 {
            tracker.samples.push_back(HourlySample {
                hour_ts: 1000 + i * 3600,
                download_bytes: 100,
                upload_bytes: 0,
                breakdown: ProtocolBreakdown::default(),
                active_tasks: 0,
            });
        }
        assert_eq!(tracker.samples.len(), 5);

        tracker.prune();
        assert_eq!(tracker.samples.len(), 3);
        // Should keep the newest ones
        assert_eq!(tracker.samples[0].hour_ts, 1000 + 2 * 3600);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(BandwidthUsageTracker::format_bytes(0), "0 B");
        assert_eq!(BandwidthUsageTracker::format_bytes(512), "512 B");
        assert_eq!(BandwidthUsageTracker::format_bytes(1024), "1.00 KB");
        assert_eq!(BandwidthUsageTracker::format_bytes(1536), "1.50 KB");
        assert_eq!(BandwidthUsageTracker::format_bytes(1_048_576), "1.00 MB");
        assert_eq!(
            BandwidthUsageTracker::format_bytes(1_073_741_824),
            "1.00 GB"
        );
        assert_eq!(
            BandwidthUsageTracker::format_bytes(1_099_511_627_776),
            "1.00 TB"
        );
    }

    #[test]
    fn test_format_summary() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(1_000_000, "http");
        let output = tracker.format_summary();
        assert!(output.contains("Bandwidth Usage Summary"));
        assert!(output.contains("Rolling 24h"));
        assert!(output.contains("976.56 KB")); // ~1MB
    }

    #[test]
    fn test_config_serialization() {
        let config = BandwidthUsageConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let restored: BandwidthUsageConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.enabled, config.enabled);
        assert_eq!(restored.max_hourly_samples, config.max_hourly_samples);
        assert_eq!(restored.peak_threshold_bytes, config.peak_threshold_bytes);
    }

    #[test]
    fn test_tracker_serialization() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(5000, "torrent");
        tracker.record_upload(1000);

        let json = serde_json::to_string(&tracker).unwrap();
        let restored: BandwidthUsageTracker = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.config().enabled, true);
    }

    #[test]
    fn test_ts_to_datetime() {
        // Unix epoch: 1970-01-01 00:00:00 UTC (Thursday)
        let (year, month, dow, hour) = BandwidthUsageTracker::ts_to_datetime(0);
        assert_eq!(year, 1970);
        assert_eq!(month, 1);
        assert_eq!(dow, 4); // Thursday
        assert_eq!(hour, 0);

        // 2024-01-01 12:00:00 UTC = 1704110400
        let (year, month, _dow, hour) = BandwidthUsageTracker::ts_to_datetime(1704110400);
        assert_eq!(year, 2024);
        assert_eq!(month, 1);
        assert_eq!(hour, 12);
    }

    #[test]
    fn test_hour_boundary() {
        let boundary = BandwidthUsageTracker::current_hour_boundary();
        assert_eq!(boundary % 3600, 0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(boundary <= now);
        assert!(now - boundary < 3600);
    }

    #[test]
    fn test_set_config() {
        let mut tracker = BandwidthUsageTracker::new();
        let new_config = BandwidthUsageConfig {
            enabled: false,
            max_hourly_samples: 100,
            peak_threshold_bytes: 500_000_000,
            track_protocol_breakdown: false,
        };
        tracker.set_config(new_config.clone());
        assert_eq!(tracker.config().max_hourly_samples, 100);
        assert_eq!(tracker.config().peak_threshold_bytes, 500_000_000);
    }

    #[test]
    fn test_peak_count_in_summary() {
        let mut tracker = BandwidthUsageTracker::with_config(BandwidthUsageConfig {
            peak_threshold_bytes: 1000,
            ..Default::default()
        });

        // Add samples above threshold
        tracker.samples.push_back(HourlySample {
            hour_ts: 1000,
            download_bytes: 2000,
            upload_bytes: 0,
            breakdown: ProtocolBreakdown::default(),
            active_tasks: 0,
        });
        tracker.samples.push_back(HourlySample {
            hour_ts: 4600,
            download_bytes: 500,
            upload_bytes: 0,
            breakdown: ProtocolBreakdown::default(),
            active_tasks: 0,
        });
        tracker.samples.push_back(HourlySample {
            hour_ts: 8200,
            download_bytes: 3000,
            upload_bytes: 0,
            breakdown: ProtocolBreakdown::default(),
            active_tasks: 0,
        });

        let summary = tracker.summary();
        assert_eq!(summary.peak_hour_count, 2); // 2000 and 3000 exceed 1000
    }

    #[test]
    fn test_rolling_window_with_samples() {
        let mut tracker = BandwidthUsageTracker::new();
        let now_hour = BandwidthUsageTracker::current_hour_boundary();

        // Add a sample from 2 hours ago
        tracker.samples.push_back(HourlySample {
            hour_ts: now_hour - 7200,
            download_bytes: 5000,
            upload_bytes: 1000,
            breakdown: ProtocolBreakdown::default(),
            active_tasks: 2,
        });

        // Add a sample from 25 hours ago (outside 24h window)
        tracker.samples.push_back(HourlySample {
            hour_ts: now_hour - 90000,
            download_bytes: 9999,
            upload_bytes: 9999,
            breakdown: ProtocolBreakdown::default(),
            active_tasks: 5,
        });

        // Current hour data
        tracker.record_download(1000, "http");

        let summary = tracker.rolling_24h_summary();
        assert_eq!(summary.total_download_bytes, 6000); // 5000 + 1000, not 9999
        assert_eq!(summary.total_upload_bytes, 1000);
        assert_eq!(summary.hours_with_data, 2);
        assert_eq!(summary.peak_bytes, 5000);
    }

    #[test]
    fn test_multiple_protocol_tracking() {
        let mut tracker = BandwidthUsageTracker::new();
        tracker.record_download(100, "http");
        tracker.record_download(200, "https");
        tracker.record_download(300, "xunlei");
        tracker.record_download(400, "torrent");
        tracker.record_download(500, "ed2k");
        tracker.record_download(600, "p2p");

        let acc = tracker.current_hour.as_ref().unwrap();
        assert_eq!(acc.download_bytes, 2100);
        assert_eq!(acc.breakdown.http_bytes, 600); // http + https + xunlei
        assert_eq!(acc.breakdown.torrent_bytes, 400);
        assert_eq!(acc.breakdown.ed2k_bytes, 500);
        assert_eq!(acc.breakdown.p2p_bytes, 600);
    }
}
