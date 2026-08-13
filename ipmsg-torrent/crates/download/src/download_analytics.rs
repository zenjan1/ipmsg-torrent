//! Download Analytics - Historical trend tracking for download metrics
//!
//! Records daily snapshots of download statistics (bytes transferred, task counts,
//! peak speeds, error counts) and provides trend analysis over configurable time
//! windows. Unlike the in-memory StatsManager, this module persists daily records
//! to disk for long-term historical analysis.
//!
//! ## Features
//! - Automatic daily snapshot creation with rollover detection
//! - Configurable retention period (default 90 days)
//! - Trend analysis: compare current period vs previous period
//! - Peak speed tracking per day
//! - Protocol breakdown per day
//! - Summary generation with human-readable format

use chrono::{DateTime, NaiveDate, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Default number of days to retain analytics data
const DEFAULT_RETENTION_DAYS: u32 = 90;

/// Maximum number of daily records to keep in memory
const MAX_RECORDS: usize = 365;

/// Configuration for the analytics system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsConfig {
    /// Whether analytics tracking is enabled
    pub enabled: bool,
    /// Number of days to retain historical data
    pub retention_days: u32,
    /// Whether to track per-protocol breakdowns
    pub track_protocol_breakdown: bool,
    /// Whether to track hourly distribution
    pub track_hourly_distribution: bool,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: DEFAULT_RETENTION_DAYS,
            track_protocol_breakdown: true,
            track_hourly_distribution: true,
        }
    }
}

/// Protocol identifier for breakdown tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnalyticsProtocol {
    Http,
    Torrent,
    Ed2k,
    P2p,
    Other,
}

impl AnalyticsProtocol {
    pub fn from_url(url: &str) -> Self {
        let lower = url.to_lowercase();
        if lower.starts_with("http://") || lower.starts_with("https://") {
            Self::Http
        } else if lower.starts_with("magnet:") || lower.ends_with(".torrent") {
            Self::Torrent
        } else if lower.starts_with("ed2k://") {
            Self::Ed2k
        } else if lower.starts_with("p2p://") {
            Self::P2p
        } else {
            Self::Other
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Http => "HTTP/HTTPS",
            Self::Torrent => "Torrent",
            Self::Ed2k => "eDonkey",
            Self::P2p => "P2P",
            Self::Other => "Other",
        }
    }
}

/// A single day's download metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyMetrics {
    /// The date this record covers
    pub date: NaiveDate,
    /// Total bytes downloaded on this day
    pub bytes_downloaded: u64,
    /// Total bytes uploaded (seeded) on this day
    pub bytes_uploaded: u64,
    /// Number of tasks started on this day
    pub tasks_started: u32,
    /// Number of tasks completed successfully
    pub tasks_completed: u32,
    /// Number of tasks that failed
    pub tasks_failed: u32,
    /// Number of tasks paused by user
    pub tasks_paused: u32,
    /// Peak download speed in bytes/sec
    pub peak_speed_bps: u64,
    /// Average download speed in bytes/sec (computed from samples)
    pub avg_speed_bps: u64,
    /// Number of speed samples recorded
    pub speed_sample_count: u64,
    /// Per-protocol byte breakdown
    pub protocol_bytes: HashMap<AnalyticsProtocol, u64>,
    /// Hourly download distribution (hour 0-23 -> bytes)
    pub hourly_bytes: HashMap<u8, u64>,
    /// Number of errors encountered
    pub error_count: u32,
    /// Number of auto-retry attempts
    pub retry_count: u32,
    /// Total active download time in seconds
    pub active_time_secs: u64,
    /// When this record was last updated
    pub last_updated: DateTime<Utc>,
}

impl DailyMetrics {
    pub fn new(date: NaiveDate) -> Self {
        Self {
            date,
            bytes_downloaded: 0,
            bytes_uploaded: 0,
            tasks_started: 0,
            tasks_completed: 0,
            tasks_failed: 0,
            tasks_paused: 0,
            peak_speed_bps: 0,
            avg_speed_bps: 0,
            speed_sample_count: 0,
            protocol_bytes: HashMap::new(),
            hourly_bytes: HashMap::new(),
            error_count: 0,
            retry_count: 0,
            active_time_secs: 0,
            last_updated: Utc::now(),
        }
    }

    /// Record bytes downloaded
    pub fn record_download(&mut self, bytes: u64, protocol: AnalyticsProtocol, hour: u8) {
        self.bytes_downloaded += bytes;
        self.last_updated = Utc::now();

        if let Some(proto_bytes) = self.protocol_bytes.get_mut(&protocol) {
            *proto_bytes += bytes;
        } else {
            self.protocol_bytes.insert(protocol, bytes);
        }

        if let Some(hourly) = self.hourly_bytes.get_mut(&hour) {
            *hourly += bytes;
        } else {
            self.hourly_bytes.insert(hour, bytes);
        }
    }

    /// Record a speed sample
    pub fn record_speed(&mut self, speed_bps: u64) {
        if speed_bps > self.peak_speed_bps {
            self.peak_speed_bps = speed_bps;
        }
        // Running average
        let total_samples = self.speed_sample_count + 1;
        self.avg_speed_bps =
            (self.avg_speed_bps * self.speed_sample_count + speed_bps) / total_samples;
        self.speed_sample_count = total_samples;
        self.last_updated = Utc::now();
    }

    /// Record a task state transition
    pub fn record_task_event(&mut self, event: TaskAnalyticsEvent) {
        match event {
            TaskAnalyticsEvent::Started => self.tasks_started += 1,
            TaskAnalyticsEvent::Completed => self.tasks_completed += 1,
            TaskAnalyticsEvent::Failed => self.tasks_failed += 1,
            TaskAnalyticsEvent::Paused => self.tasks_paused += 1,
            TaskAnalyticsEvent::Error => self.error_count += 1,
            TaskAnalyticsEvent::Retry => self.retry_count += 1,
        }
        self.last_updated = Utc::now();
    }

    /// Record active download time
    pub fn record_active_time(&mut self, secs: u64) {
        self.active_time_secs += secs;
        self.last_updated = Utc::now();
    }

    /// Get the top protocol by bytes downloaded
    pub fn top_protocol(&self) -> Option<(AnalyticsProtocol, u64)> {
        self.protocol_bytes
            .iter()
            .max_by_key(|(_, bytes)| *bytes)
            .map(|(proto, bytes)| (*proto, *bytes))
    }

    /// Get the peak hour (hour with most downloads)
    pub fn peak_hour(&self) -> Option<(u8, u64)> {
        self.hourly_bytes
            .iter()
            .max_by_key(|(_, bytes)| *bytes)
            .map(|(hour, bytes)| (*hour, *bytes))
    }

    /// Format bytes as human-readable
    fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;
        const GB: u64 = 1024 * MB;

        if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    /// Format speed as human-readable
    fn format_speed(bps: u64) -> String {
        format!("{}/s", Self::format_bytes(bps))
    }
}

/// Events that can be recorded in analytics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAnalyticsEvent {
    Started,
    Completed,
    Failed,
    Paused,
    Error,
    Retry,
}

/// Analytics summary for a time range
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsSummary {
    /// Number of days in this summary
    pub days_covered: u32,
    /// Start date (inclusive)
    pub start_date: NaiveDate,
    /// End date (inclusive)
    pub end_date: NaiveDate,
    /// Total bytes downloaded in the period
    pub total_bytes_downloaded: u64,
    /// Total bytes uploaded in the period
    pub total_bytes_uploaded: u64,
    /// Total tasks started
    pub total_tasks_started: u32,
    /// Total tasks completed
    pub total_tasks_completed: u32,
    /// Total tasks failed
    pub total_tasks_failed: u32,
    /// Overall peak speed across all days
    pub overall_peak_speed_bps: u64,
    /// Average daily download bytes
    pub avg_daily_bytes: u64,
    /// Average daily tasks completed
    pub avg_daily_completions: f32,
    /// Total errors across all days
    pub total_errors: u32,
    /// Total retries across all days
    pub total_retries: u32,
    /// Aggregate protocol breakdown
    pub protocol_totals: HashMap<AnalyticsProtocol, u64>,
    /// Aggregate hourly distribution
    pub hourly_totals: HashMap<u8, u64>,
    /// Day with highest downloads
    pub best_day: Option<(NaiveDate, u64)>,
    /// Day with lowest downloads (excluding zero-download days)
    pub worst_day: Option<(NaiveDate, u64)>,
    /// Total active download time
    pub total_active_time_secs: u64,
    /// Success rate (completed / started)
    pub success_rate: f32,
}

impl AnalyticsSummary {
    /// Format the summary as a human-readable report
    pub fn format_report(&self) -> String {
        let mut report = String::new();
        report.push_str(&format!(
            "📊 Download Analytics: {} to {} ({} days)\n",
            self.start_date, self.end_date, self.days_covered
        ));
        report.push_str("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        report.push_str(&format!(
            "📥 Total Downloaded: {}\n",
            DailyMetrics::format_bytes(self.total_bytes_downloaded)
        ));
        report.push_str(&format!(
            "📤 Total Uploaded:   {}\n",
            DailyMetrics::format_bytes(self.total_bytes_uploaded)
        ));
        report.push_str(&format!(
            "📈 Avg Daily:        {}\n",
            DailyMetrics::format_bytes(self.avg_daily_bytes)
        ));
        report.push_str(&format!(
            "⚡ Peak Speed:       {}\n",
            DailyMetrics::format_speed(self.overall_peak_speed_bps)
        ));
        report.push_str(&format!(
            "✅ Tasks Completed:  {}\n",
            self.total_tasks_completed
        ));
        report.push_str(&format!(
            "❌ Tasks Failed:     {}\n",
            self.total_tasks_failed
        ));
        report.push_str(&format!(
            "🎯 Success Rate:     {:.1}%\n",
            self.success_rate * 100.0
        ));
        report.push_str(&format!(
            "⏱  Active Time:      {}\n",
            format_duration(self.total_active_time_secs)
        ));
        report.push_str(&format!("🔄 Total Errors:     {}\n", self.total_errors));
        report.push_str(&format!("🔁 Total Retries:    {}\n", self.total_retries));

        if let Some((date, bytes)) = &self.best_day {
            report.push_str(&format!(
                "🏆 Best Day:          {} ({})\n",
                date,
                DailyMetrics::format_bytes(*bytes)
            ));
        }
        if let Some((date, bytes)) = &self.worst_day {
            report.push_str(&format!(
                "📉 Worst Day:        {} ({})\n",
                date,
                DailyMetrics::format_bytes(*bytes)
            ));
        }

        // Protocol breakdown
        if !self.protocol_totals.is_empty() {
            report.push_str("\n📡 Protocol Breakdown:\n");
            let mut protos: Vec<_> = self.protocol_totals.iter().collect();
            protos.sort_by(|a, b| b.1.cmp(a.1));
            for (proto, bytes) in protos {
                let pct = if self.total_bytes_downloaded > 0 {
                    (*bytes as f64 / self.total_bytes_downloaded as f64) * 100.0
                } else {
                    0.0
                };
                report.push_str(&format!(
                    "   {:12} {} ({:.1}%)\n",
                    proto.label(),
                    DailyMetrics::format_bytes(*bytes),
                    pct
                ));
            }
        }

        // Peak hours (top 3)
        if !self.hourly_totals.is_empty() {
            report.push_str("\n🕐 Peak Hours:\n");
            let mut hours: Vec<_> = self.hourly_totals.iter().collect();
            hours.sort_by(|a, b| b.1.cmp(a.1));
            for (hour, bytes) in hours.iter().take(3) {
                report.push_str(&format!(
                    "   {:02}:00 - {} \n",
                    hour,
                    DailyMetrics::format_bytes(**bytes)
                ));
            }
        }

        report
    }
}

/// Trend comparison between two periods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendComparison {
    /// Current period summary
    pub current: AnalyticsSummary,
    /// Previous period summary (same length, immediately before current)
    pub previous: AnalyticsSummary,
    /// Percentage change in bytes downloaded (-100 to +inf)
    pub download_change_pct: f64,
    /// Percentage change in tasks completed
    pub completion_change_pct: f64,
    /// Percentage change in peak speed
    pub speed_change_pct: f64,
    /// Percentage change in error count
    pub error_change_pct: f64,
    /// Whether the trend is improving, declining, or stable
    pub trend_direction: TrendDirection,
}

/// Overall trend direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrendDirection {
    Improving,
    Declining,
    Stable,
}

impl TrendComparison {
    /// Format the comparison as a human-readable report
    pub fn format_report(&self) -> String {
        let arrow = |pct: f64| -> &'static str {
            if pct > 5.0 {
                "📈"
            } else if pct < -5.0 {
                "📉"
            } else {
                "➡️"
            }
        };

        format!(
            "📊 Trend Comparison\n\
             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
             {} Downloads: {} → {} ({:+.1}%)\n\
             {} Completions: {} → {} ({:+.1}%)\n\
             {} Peak Speed: {} → {} ({:+.1}%)\n\
             {} Errors: {} → {} ({:+.1}%)\n\
             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
             Overall: {:?}\n",
            arrow(self.download_change_pct),
            DailyMetrics::format_bytes(self.previous.total_bytes_downloaded),
            DailyMetrics::format_bytes(self.current.total_bytes_downloaded),
            self.download_change_pct,
            arrow(self.completion_change_pct),
            self.previous.total_tasks_completed,
            self.current.total_tasks_completed,
            self.completion_change_pct,
            arrow(self.speed_change_pct),
            DailyMetrics::format_speed(self.previous.overall_peak_speed_bps),
            DailyMetrics::format_speed(self.current.overall_peak_speed_bps),
            self.speed_change_pct,
            arrow(self.error_change_pct),
            self.previous.total_errors,
            self.current.total_errors,
            self.error_change_pct,
            self.trend_direction,
        )
    }
}

/// Manager for download analytics
#[derive(Debug, Clone)]
pub struct AnalyticsManager {
    config: AnalyticsConfig,
    /// Daily metrics records indexed by date
    records: HashMap<NaiveDate, DailyMetrics>,
}

impl AnalyticsManager {
    pub fn new() -> Self {
        Self {
            config: AnalyticsConfig::default(),
            records: HashMap::new(),
        }
    }

    pub fn with_config(config: AnalyticsConfig) -> Self {
        Self {
            config,
            records: HashMap::new(),
        }
    }

    /// Get or create today's metrics record
    pub fn today_mut(&mut self) -> &mut DailyMetrics {
        let today = Utc::now().date_naive();
        self.records
            .entry(today)
            .or_insert_with(|| DailyMetrics::new(today))
    }

    /// Get today's metrics (read-only)
    pub fn today(&self) -> Option<&DailyMetrics> {
        let today = Utc::now().date_naive();
        self.records.get(&today)
    }

    /// Record bytes downloaded
    pub fn record_download(&mut self, bytes: u64, url: &str) {
        if !self.config.enabled {
            return;
        }
        let protocol = AnalyticsProtocol::from_url(url);
        let hour = Utc::now().time().hour() as u8;
        self.today_mut().record_download(bytes, protocol, hour);
    }

    /// Record a speed sample
    pub fn record_speed(&mut self, speed_bps: u64) {
        if !self.config.enabled {
            return;
        }
        self.today_mut().record_speed(speed_bps);
    }

    /// Record a task event
    pub fn record_task_event(&mut self, event: TaskAnalyticsEvent) {
        if !self.config.enabled {
            return;
        }
        self.today_mut().record_task_event(event);
    }

    /// Record active download time
    pub fn record_active_time(&mut self, secs: u64) {
        if !self.config.enabled {
            return;
        }
        self.today_mut().record_active_time(secs);
    }

    /// Get metrics for a specific date
    pub fn get_day(&self, date: NaiveDate) -> Option<&DailyMetrics> {
        self.records.get(&date)
    }

    /// Get all records sorted by date (newest first)
    pub fn all_records(&self) -> Vec<&DailyMetrics> {
        let mut records: Vec<_> = self.records.values().collect();
        records.sort_by_key(|r| std::cmp::Reverse(r.date));
        records
    }

    /// Get summary for the last N days
    pub fn summary_last_n_days(&self, n: u32) -> Option<AnalyticsSummary> {
        let today = Utc::now().date_naive();
        let start = today - chrono::Duration::days(n as i64 - 1);
        self.summary_range(start, today)
    }

    /// Get summary for a specific date range
    pub fn summary_range(&self, start: NaiveDate, end: NaiveDate) -> Option<AnalyticsSummary> {
        let relevant: Vec<&DailyMetrics> = self
            .records
            .values()
            .filter(|r| r.date >= start && r.date <= end)
            .collect();

        if relevant.is_empty() {
            return None;
        }

        let days_covered = relevant.len() as u32;
        let mut summary = AnalyticsSummary {
            days_covered,
            start_date: relevant.iter().map(|r| r.date).min().unwrap_or(start),
            end_date: relevant.iter().map(|r| r.date).max().unwrap_or(end),
            total_bytes_downloaded: 0,
            total_bytes_uploaded: 0,
            total_tasks_started: 0,
            total_tasks_completed: 0,
            total_tasks_failed: 0,
            overall_peak_speed_bps: 0,
            avg_daily_bytes: 0,
            avg_daily_completions: 0.0,
            total_errors: 0,
            total_retries: 0,
            protocol_totals: HashMap::new(),
            hourly_totals: HashMap::new(),
            best_day: None,
            worst_day: None,
            total_active_time_secs: 0,
            success_rate: 0.0,
        };

        let mut min_bytes: Option<u64> = None;

        for record in &relevant {
            summary.total_bytes_downloaded += record.bytes_downloaded;
            summary.total_bytes_uploaded += record.bytes_uploaded;
            summary.total_tasks_started += record.tasks_started;
            summary.total_tasks_completed += record.tasks_completed;
            summary.total_tasks_failed += record.tasks_failed;
            summary.total_errors += record.error_count;
            summary.total_retries += record.retry_count;
            summary.total_active_time_secs += record.active_time_secs;

            if record.peak_speed_bps > summary.overall_peak_speed_bps {
                summary.overall_peak_speed_bps = record.peak_speed_bps;
            }

            // Track best/worst days
            match &summary.best_day {
                Some((_, best_bytes)) if record.bytes_downloaded > *best_bytes => {
                    summary.best_day = Some((record.date, record.bytes_downloaded));
                }
                None => {
                    summary.best_day = Some((record.date, record.bytes_downloaded));
                }
                _ => {}
            }

            if record.bytes_downloaded > 0 {
                match min_bytes {
                    Some(min) if record.bytes_downloaded < min => {
                        min_bytes = Some(record.bytes_downloaded);
                        summary.worst_day = Some((record.date, record.bytes_downloaded));
                    }
                    None => {
                        min_bytes = Some(record.bytes_downloaded);
                        summary.worst_day = Some((record.date, record.bytes_downloaded));
                    }
                    _ => {}
                }
            }

            // Aggregate protocol breakdown
            for (proto, bytes) in &record.protocol_bytes {
                *summary.protocol_totals.entry(*proto).or_insert(0) += bytes;
            }

            // Aggregate hourly distribution
            for (hour, bytes) in &record.hourly_bytes {
                *summary.hourly_totals.entry(*hour).or_insert(0) += bytes;
            }
        }

        summary.avg_daily_bytes = summary.total_bytes_downloaded / days_covered as u64;
        summary.avg_daily_completions = summary.total_tasks_completed as f32 / days_covered as f32;
        summary.success_rate = if summary.total_tasks_started > 0 {
            summary.total_tasks_completed as f32 / summary.total_tasks_started as f32
        } else {
            0.0
        };

        Some(summary)
    }

    /// Compare the last N days with the previous N days
    pub fn compare_periods(&self, days: u32) -> Option<TrendComparison> {
        let today = Utc::now().date_naive();
        let current_start = today - chrono::Duration::days(days as i64 - 1);
        let prev_end = current_start - chrono::Duration::days(1);
        let prev_start = prev_end - chrono::Duration::days(days as i64 - 1);

        let current = self.summary_range(current_start, today)?;
        let previous = self.summary_range(prev_start, prev_end)?;

        let download_change_pct = if previous.total_bytes_downloaded > 0 {
            ((current.total_bytes_downloaded as f64 - previous.total_bytes_downloaded as f64)
                / previous.total_bytes_downloaded as f64)
                * 100.0
        } else if current.total_bytes_downloaded > 0 {
            100.0
        } else {
            0.0
        };

        let completion_change_pct = if previous.total_tasks_completed > 0 {
            ((current.total_tasks_completed as f64 - previous.total_tasks_completed as f64)
                / previous.total_tasks_completed as f64)
                * 100.0
        } else if current.total_tasks_completed > 0 {
            100.0
        } else {
            0.0
        };

        let speed_change_pct = if previous.overall_peak_speed_bps > 0 {
            ((current.overall_peak_speed_bps as f64 - previous.overall_peak_speed_bps as f64)
                / previous.overall_peak_speed_bps as f64)
                * 100.0
        } else if current.overall_peak_speed_bps > 0 {
            100.0
        } else {
            0.0
        };

        let error_change_pct = if previous.total_errors > 0 {
            ((current.total_errors as f64 - previous.total_errors as f64)
                / previous.total_errors as f64)
                * 100.0
        } else if current.total_errors > 0 {
            100.0
        } else {
            0.0
        };

        // Determine trend direction based on download change
        let trend_direction = if download_change_pct > 5.0 {
            TrendDirection::Improving
        } else if download_change_pct < -5.0 {
            TrendDirection::Declining
        } else {
            TrendDirection::Stable
        };

        Some(TrendComparison {
            current,
            previous,
            download_change_pct,
            completion_change_pct,
            speed_change_pct,
            error_change_pct,
            trend_direction,
        })
    }

    /// Prune records older than retention period
    pub fn prune_old_records(&mut self) {
        if !self.config.enabled {
            return;
        }
        let cutoff =
            Utc::now().date_naive() - chrono::Duration::days(self.config.retention_days as i64);
        self.records.retain(|date, _| *date >= cutoff);

        // Also enforce max records limit
        if self.records.len() > MAX_RECORDS {
            let to_remove = self.records.len() - MAX_RECORDS;
            let mut dates: Vec<NaiveDate> = self.records.keys().copied().collect();
            dates.sort();
            for date in dates.into_iter().take(to_remove) {
                self.records.remove(&date);
            }
        }
    }

    /// Get/set config
    pub fn config(&self) -> &AnalyticsConfig {
        &self.config
    }

    pub fn set_config(&mut self, config: AnalyticsConfig) {
        self.config = config;
    }

    /// Clear all analytics data
    pub fn clear(&mut self) {
        self.records.clear();
    }

    /// Get total record count
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Get mutable reference to config
    pub fn config_mut(&mut self) -> &mut AnalyticsConfig {
        &mut self.config
    }

    /// Insert a daily metrics record
    pub fn insert_record(&mut self, date: NaiveDate, metrics: DailyMetrics) {
        self.records.insert(date, metrics);
    }

    /// Get mutable reference to records
    pub fn records_mut(&mut self) -> &mut HashMap<NaiveDate, DailyMetrics> {
        &mut self.records
    }
}

impl Default for AnalyticsManager {
    fn default() -> Self {
        Self::new()
    }
}

// Persistence functions for analytics data

/// Save analytics config to disk
pub fn save_analytics_config(
    data_dir: &Path,
    config: &AnalyticsConfig,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("analytics_config.json");
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;
    std::fs::write(&path, json)
}

/// Load analytics config from disk
pub fn load_analytics_config(data_dir: &Path) -> Option<AnalyticsConfig> {
    let path = data_dir.join("analytics_config.json");
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

/// Save all analytics records to disk
pub fn save_analytics_records(
    data_dir: &Path,
    records: &HashMap<NaiveDate, DailyMetrics>,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("analytics_records.json");
    let json = serde_json::to_string_pretty(records).map_err(std::io::Error::other)?;
    std::fs::write(&path, json)
}

/// Load analytics records from disk
pub fn load_analytics_records(
    data_dir: &Path,
) -> Result<HashMap<NaiveDate, DailyMetrics>, std::io::Error> {
    let path = data_dir.join("analytics_records.json");
    let json = std::fs::read_to_string(&path)?;
    serde_json::from_str(&json).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Format duration in seconds to human-readable
fn format_duration(secs: u64) -> String {
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, mins)
    } else if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analytics_protocol_from_url() {
        assert_eq!(
            AnalyticsProtocol::from_url("https://example.com/file.zip"),
            AnalyticsProtocol::Http
        );
        assert_eq!(
            AnalyticsProtocol::from_url("magnet:?xt=urn:btih:abc"),
            AnalyticsProtocol::Torrent
        );
        assert_eq!(
            AnalyticsProtocol::from_url("ed2k://|file|test|100|"),
            AnalyticsProtocol::Ed2k
        );
        assert_eq!(
            AnalyticsProtocol::from_url("p2p://node/file"),
            AnalyticsProtocol::P2p
        );
        assert_eq!(
            AnalyticsProtocol::from_url("ftp://example.com/file"),
            AnalyticsProtocol::Other
        );
    }

    #[test]
    fn test_daily_metrics_record_download() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);

        metrics.record_download(1000, AnalyticsProtocol::Http, 14);
        metrics.record_download(2000, AnalyticsProtocol::Http, 14);
        metrics.record_download(500, AnalyticsProtocol::Torrent, 15);

        assert_eq!(metrics.bytes_downloaded, 3500);
        assert_eq!(metrics.protocol_bytes[&AnalyticsProtocol::Http], 3000);
        assert_eq!(metrics.protocol_bytes[&AnalyticsProtocol::Torrent], 500);
        assert_eq!(metrics.hourly_bytes[&14], 3000);
        assert_eq!(metrics.hourly_bytes[&15], 500);
    }

    #[test]
    fn test_daily_metrics_record_speed() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);

        metrics.record_speed(100_000);
        metrics.record_speed(200_000);
        metrics.record_speed(150_000);

        assert_eq!(metrics.peak_speed_bps, 200_000);
        assert_eq!(metrics.speed_sample_count, 3);
        // Average: (100000 + 200000 + 150000) / 3 = 150000
        assert_eq!(metrics.avg_speed_bps, 150_000);
    }

    #[test]
    fn test_daily_metrics_task_events() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);

        metrics.record_task_event(TaskAnalyticsEvent::Started);
        metrics.record_task_event(TaskAnalyticsEvent::Started);
        metrics.record_task_event(TaskAnalyticsEvent::Completed);
        metrics.record_task_event(TaskAnalyticsEvent::Failed);
        metrics.record_task_event(TaskAnalyticsEvent::Error);
        metrics.record_task_event(TaskAnalyticsEvent::Retry);

        assert_eq!(metrics.tasks_started, 2);
        assert_eq!(metrics.tasks_completed, 1);
        assert_eq!(metrics.tasks_failed, 1);
        assert_eq!(metrics.error_count, 1);
        assert_eq!(metrics.retry_count, 1);
    }

    #[test]
    fn test_daily_metrics_top_protocol() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);

        metrics.record_download(100, AnalyticsProtocol::Http, 10);
        metrics.record_download(500, AnalyticsProtocol::Torrent, 11);
        metrics.record_download(200, AnalyticsProtocol::Ed2k, 12);

        let (proto, bytes) = metrics.top_protocol().unwrap();
        assert_eq!(proto, AnalyticsProtocol::Torrent);
        assert_eq!(bytes, 500);
    }

    #[test]
    fn test_daily_metrics_peak_hour() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);

        metrics.record_download(100, AnalyticsProtocol::Http, 10);
        metrics.record_download(500, AnalyticsProtocol::Http, 14);
        metrics.record_download(200, AnalyticsProtocol::Http, 22);

        let (hour, bytes) = metrics.peak_hour().unwrap();
        assert_eq!(hour, 14);
        assert_eq!(bytes, 500);
    }

    #[test]
    fn test_analytics_manager_basic() {
        let mut manager = AnalyticsManager::new();

        manager.record_download(1000, "https://example.com/file.zip");
        manager.record_speed(500_000);
        manager.record_task_event(TaskAnalyticsEvent::Started);
        manager.record_task_event(TaskAnalyticsEvent::Completed);

        let today = manager.today().unwrap();
        assert_eq!(today.bytes_downloaded, 1000);
        assert_eq!(today.peak_speed_bps, 500_000);
        assert_eq!(today.tasks_started, 1);
        assert_eq!(today.tasks_completed, 1);
    }

    #[test]
    fn test_analytics_manager_disabled() {
        let mut manager = AnalyticsManager::with_config(AnalyticsConfig {
            enabled: false,
            ..Default::default()
        });

        manager.record_download(1000, "https://example.com/file.zip");
        manager.record_speed(500_000);

        assert!(manager.today().is_none());
    }

    #[test]
    fn test_analytics_manager_summary() {
        let mut manager = AnalyticsManager::new();

        // Manually insert records for different dates
        let today = Utc::now().date_naive();
        let yesterday = today - chrono::Duration::days(1);

        let mut day1 = DailyMetrics::new(yesterday);
        day1.bytes_downloaded = 5000;
        day1.tasks_completed = 3;
        day1.peak_speed_bps = 1_000_000;

        let mut day2 = DailyMetrics::new(today);
        day2.bytes_downloaded = 10000;
        day2.tasks_completed = 5;
        day2.peak_speed_bps = 2_000_000;

        manager.records.insert(yesterday, day1);
        manager.records.insert(today, day2);

        let summary = manager.summary_last_n_days(2).unwrap();
        assert_eq!(summary.days_covered, 2);
        assert_eq!(summary.total_bytes_downloaded, 15000);
        assert_eq!(summary.total_tasks_completed, 8);
        assert_eq!(summary.overall_peak_speed_bps, 2_000_000);
        assert_eq!(summary.avg_daily_bytes, 7500);
        assert_eq!(summary.best_day, Some((today, 10000)));
        assert_eq!(summary.worst_day, Some((yesterday, 5000)));
    }

    #[test]
    fn test_analytics_manager_empty_summary() {
        let manager = AnalyticsManager::new();
        assert!(manager.summary_last_n_days(7).is_none());
    }

    #[test]
    fn test_analytics_prune_old_records() {
        let mut manager = AnalyticsManager::with_config(AnalyticsConfig {
            retention_days: 7,
            ..Default::default()
        });

        let today = Utc::now().date_naive();
        let old_date = today - chrono::Duration::days(30);
        let recent_date = today - chrono::Duration::days(3);

        manager
            .records
            .insert(old_date, DailyMetrics::new(old_date));
        manager
            .records
            .insert(recent_date, DailyMetrics::new(recent_date));
        manager.records.insert(today, DailyMetrics::new(today));

        assert_eq!(manager.record_count(), 3);
        manager.prune_old_records();
        assert_eq!(manager.record_count(), 2);
        assert!(manager.get_day(old_date).is_none());
        assert!(manager.get_day(recent_date).is_some());
    }

    #[test]
    fn test_analytics_compare_periods() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        // Insert records for 14 days: 7 "previous" + 7 "current"
        for i in 0..14 {
            let date = today - chrono::Duration::days(i);
            let mut metrics = DailyMetrics::new(date);
            if i < 7 {
                // Current period: higher values
                metrics.bytes_downloaded = 10000;
                metrics.tasks_completed = 5;
                metrics.peak_speed_bps = 2_000_000;
                metrics.tasks_started = 6;
            } else {
                // Previous period: lower values
                metrics.bytes_downloaded = 5000;
                metrics.tasks_completed = 3;
                metrics.peak_speed_bps = 1_000_000;
                metrics.tasks_started = 4;
            }
            manager.records.insert(date, metrics);
        }

        let comparison = manager.compare_periods(7).unwrap();
        assert!(comparison.download_change_pct > 90.0); // ~100% increase
        assert!(comparison.completion_change_pct > 60.0); // ~66% increase
        assert!(comparison.speed_change_pct > 90.0); // ~100% increase
        assert_eq!(comparison.trend_direction, TrendDirection::Improving);
    }

    #[test]
    fn test_trend_comparison_format() {
        let comparison = TrendComparison {
            current: AnalyticsSummary {
                days_covered: 7,
                start_date: NaiveDate::from_ymd_opt(2026, 8, 4).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 8, 10).unwrap(),
                total_bytes_downloaded: 10_000_000,
                total_bytes_uploaded: 1_000_000,
                total_tasks_started: 20,
                total_tasks_completed: 18,
                total_tasks_failed: 2,
                overall_peak_speed_bps: 5_000_000,
                avg_daily_bytes: 1_428_571,
                avg_daily_completions: 2.57,
                total_errors: 3,
                total_retries: 5,
                protocol_totals: HashMap::new(),
                hourly_totals: HashMap::new(),
                best_day: None,
                worst_day: None,
                total_active_time_secs: 3600,
                success_rate: 0.9,
            },
            previous: AnalyticsSummary {
                days_covered: 7,
                start_date: NaiveDate::from_ymd_opt(2026, 7, 28).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 8, 3).unwrap(),
                total_bytes_downloaded: 5_000_000,
                total_bytes_uploaded: 500_000,
                total_tasks_started: 15,
                total_tasks_completed: 10,
                total_tasks_failed: 5,
                overall_peak_speed_bps: 2_500_000,
                avg_daily_bytes: 714_285,
                avg_daily_completions: 1.43,
                total_errors: 8,
                total_retries: 12,
                protocol_totals: HashMap::new(),
                hourly_totals: HashMap::new(),
                best_day: None,
                worst_day: None,
                total_active_time_secs: 1800,
                success_rate: 0.667,
            },
            download_change_pct: 100.0,
            completion_change_pct: 80.0,
            speed_change_pct: 100.0,
            error_change_pct: -62.5,
            trend_direction: TrendDirection::Improving,
        };

        let report = comparison.format_report();
        assert!(report.contains("Trend Comparison"));
        assert!(report.contains("Improving"));
        assert!(report.contains("+100.0%"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(DailyMetrics::format_bytes(500), "500 B");
        assert_eq!(DailyMetrics::format_bytes(1024), "1.00 KB");
        assert_eq!(DailyMetrics::format_bytes(1_500_000), "1.43 MB");
        assert_eq!(DailyMetrics::format_bytes(2_000_000_000), "1.86 GB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30), "0m");
        assert_eq!(format_duration(3661), "1h 1m");
        assert_eq!(format_duration(90061), "1d 1h 1m");
    }

    #[test]
    fn test_analytics_config_serialization() {
        let config = AnalyticsConfig {
            enabled: true,
            retention_days: 30,
            track_protocol_breakdown: true,
            track_hourly_distribution: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AnalyticsConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.retention_days, 30);
        assert!(!deserialized.track_hourly_distribution);
    }

    #[test]
    fn test_daily_metrics_serialization() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_download(1000, AnalyticsProtocol::Http, 14);
        metrics.record_speed(500_000);
        metrics.record_task_event(TaskAnalyticsEvent::Completed);

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: DailyMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.bytes_downloaded, 1000);
        assert_eq!(deserialized.peak_speed_bps, 500_000);
        assert_eq!(deserialized.tasks_completed, 1);
    }

    #[test]
    fn test_summary_report_format() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        let mut metrics = DailyMetrics::new(today);
        metrics.bytes_downloaded = 5_000_000;
        metrics.tasks_completed = 10;
        metrics.tasks_started = 12;
        metrics.peak_speed_bps = 2_000_000;
        metrics
            .protocol_bytes
            .insert(AnalyticsProtocol::Http, 4_000_000);
        metrics
            .protocol_bytes
            .insert(AnalyticsProtocol::Torrent, 1_000_000);
        metrics.hourly_bytes.insert(14, 3_000_000);
        metrics.hourly_bytes.insert(22, 2_000_000);
        manager.records.insert(today, metrics);

        let summary = manager.summary_last_n_days(1).unwrap();
        let report = summary.format_report();

        assert!(report.contains("Download Analytics"));
        assert!(report.contains("Total Downloaded"));
        assert!(report.contains("Protocol Breakdown"));
        assert!(report.contains("HTTP/HTTPS"));
        assert!(report.contains("Peak Hours"));
    }

    #[test]
    fn test_analytics_manager_clear() {
        let mut manager = AnalyticsManager::new();
        manager.record_download(1000, "https://example.com/file.zip");
        assert_eq!(manager.record_count(), 1);

        manager.clear();
        assert_eq!(manager.record_count(), 0);
        assert!(manager.today().is_none());
    }

    #[test]
    fn test_success_rate_calculation() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        let mut metrics = DailyMetrics::new(today);
        metrics.tasks_started = 10;
        metrics.tasks_completed = 8;
        metrics.tasks_failed = 2;
        manager.records.insert(today, metrics);

        let summary = manager.summary_last_n_days(1).unwrap();
        assert!((summary.success_rate - 0.8).abs() < 0.01);
    }

    #[test]
    fn test_all_records_sorted() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        for i in 0..5 {
            let date = today - chrono::Duration::days(i);
            manager.records.insert(date, DailyMetrics::new(date));
        }

        let records = manager.all_records();
        assert_eq!(records.len(), 5);
        // Should be newest first
        for i in 0..records.len() - 1 {
            assert!(records[i].date > records[i + 1].date);
        }
    }

    // ===== AnalyticsProtocol comprehensive tests =====

    #[test]
    fn test_analytics_protocol_labels() {
        assert_eq!(AnalyticsProtocol::Http.label(), "HTTP/HTTPS");
        assert_eq!(AnalyticsProtocol::Torrent.label(), "Torrent");
        assert_eq!(AnalyticsProtocol::Ed2k.label(), "eDonkey");
        assert_eq!(AnalyticsProtocol::P2p.label(), "P2P");
        assert_eq!(AnalyticsProtocol::Other.label(), "Other");
    }

    #[test]
    fn test_analytics_protocol_from_url_empty() {
        assert_eq!(AnalyticsProtocol::from_url(""), AnalyticsProtocol::Other);
    }

    #[test]
    fn test_analytics_protocol_from_url_mixed_case() {
        assert_eq!(
            AnalyticsProtocol::from_url("HTTPS://EXAMPLE.COM/file.zip"),
            AnalyticsProtocol::Http
        );
        assert_eq!(
            AnalyticsProtocol::from_url("HTTP://example.com/file"),
            AnalyticsProtocol::Http
        );
    }

    #[test]
    fn test_analytics_protocol_from_url_torrent_file() {
        assert_eq!(
            AnalyticsProtocol::from_url("file:///path/to/file.torrent"),
            AnalyticsProtocol::Torrent
        );
    }

    #[test]
    fn test_analytics_protocol_from_url_ftp() {
        assert_eq!(
            AnalyticsProtocol::from_url("ftp://ftp.example.com/file.iso"),
            AnalyticsProtocol::Other
        );
    }

    #[test]
    fn test_analytics_protocol_serde_roundtrip() {
        let protos = vec![
            AnalyticsProtocol::Http,
            AnalyticsProtocol::Torrent,
            AnalyticsProtocol::Ed2k,
            AnalyticsProtocol::P2p,
            AnalyticsProtocol::Other,
        ];
        for proto in &protos {
            let json = serde_json::to_string(proto).unwrap();
            let deser: AnalyticsProtocol = serde_json::from_str(&json).unwrap();
            assert_eq!(*proto, deser);
        }
    }

    #[test]
    fn test_analytics_protocol_serde_lowercase() {
        let json = "\"http\"";
        let proto: AnalyticsProtocol = serde_json::from_str(json).unwrap();
        assert_eq!(proto, AnalyticsProtocol::Http);
    }

    #[test]
    fn test_analytics_protocol_clone_copy_debug() {
        let a = AnalyticsProtocol::Http;
        let b = a; // Copy
        assert_eq!(a, b);
        let _ = format!("{:?}", a);
        let c = a.clone();
        assert_eq!(a, c);
    }

    // ===== DailyMetrics comprehensive tests =====

    #[test]
    fn test_daily_metrics_new_zero_state() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        let metrics = DailyMetrics::new(date);
        assert_eq!(metrics.date, date);
        assert_eq!(metrics.bytes_downloaded, 0);
        assert_eq!(metrics.bytes_uploaded, 0);
        assert_eq!(metrics.tasks_started, 0);
        assert_eq!(metrics.tasks_completed, 0);
        assert_eq!(metrics.tasks_failed, 0);
        assert_eq!(metrics.tasks_paused, 0);
        assert_eq!(metrics.peak_speed_bps, 0);
        assert_eq!(metrics.avg_speed_bps, 0);
        assert_eq!(metrics.speed_sample_count, 0);
        assert!(metrics.protocol_bytes.is_empty());
        assert!(metrics.hourly_bytes.is_empty());
        assert_eq!(metrics.error_count, 0);
        assert_eq!(metrics.retry_count, 0);
        assert_eq!(metrics.active_time_secs, 0);
    }

    #[test]
    fn test_daily_metrics_record_download_zero_bytes() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_download(0, AnalyticsProtocol::Http, 10);
        assert_eq!(metrics.bytes_downloaded, 0);
        assert_eq!(metrics.protocol_bytes[&AnalyticsProtocol::Http], 0);
        assert_eq!(metrics.hourly_bytes[&10], 0);
    }

    #[test]
    fn test_daily_metrics_record_download_multiple_protocols_same_hour() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_download(100, AnalyticsProtocol::Http, 14);
        metrics.record_download(200, AnalyticsProtocol::Torrent, 14);
        metrics.record_download(300, AnalyticsProtocol::Ed2k, 14);
        assert_eq!(metrics.bytes_downloaded, 600);
        assert_eq!(metrics.hourly_bytes[&14], 600);
        assert_eq!(metrics.protocol_bytes.len(), 3);
    }

    #[test]
    fn test_daily_metrics_record_download_same_protocol_different_hours() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_download(100, AnalyticsProtocol::Http, 8);
        metrics.record_download(200, AnalyticsProtocol::Http, 14);
        metrics.record_download(300, AnalyticsProtocol::Http, 22);
        assert_eq!(metrics.bytes_downloaded, 600);
        assert_eq!(metrics.protocol_bytes[&AnalyticsProtocol::Http], 600);
        assert_eq!(metrics.hourly_bytes.len(), 3);
    }

    #[test]
    fn test_daily_metrics_record_speed_zero() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_speed(0);
        assert_eq!(metrics.peak_speed_bps, 0);
        assert_eq!(metrics.speed_sample_count, 1);
        assert_eq!(metrics.avg_speed_bps, 0);
    }

    #[test]
    fn test_daily_metrics_record_speed_single_sample() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_speed(500_000);
        assert_eq!(metrics.peak_speed_bps, 500_000);
        assert_eq!(metrics.avg_speed_bps, 500_000);
        assert_eq!(metrics.speed_sample_count, 1);
    }

    #[test]
    fn test_daily_metrics_record_speed_decreasing() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_speed(1_000_000);
        metrics.record_speed(500_000);
        metrics.record_speed(100_000);
        assert_eq!(metrics.peak_speed_bps, 1_000_000);
        assert_eq!(metrics.speed_sample_count, 3);
        // Average: (1000000 + 500000 + 100000) / 3 = 533333
        assert_eq!(metrics.avg_speed_bps, 533_333);
    }

    #[test]
    fn test_daily_metrics_record_task_paused() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_task_event(TaskAnalyticsEvent::Paused);
        metrics.record_task_event(TaskAnalyticsEvent::Paused);
        assert_eq!(metrics.tasks_paused, 2);
    }

    #[test]
    fn test_daily_metrics_record_active_time() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_active_time(60);
        metrics.record_active_time(120);
        assert_eq!(metrics.active_time_secs, 180);
    }

    #[test]
    fn test_daily_metrics_record_active_time_zero() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_active_time(0);
        assert_eq!(metrics.active_time_secs, 0);
    }

    #[test]
    fn test_daily_metrics_top_protocol_empty() {
        let today = Utc::now().date_naive();
        let metrics = DailyMetrics::new(today);
        assert!(metrics.top_protocol().is_none());
    }

    #[test]
    fn test_daily_metrics_top_protocol_single() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_download(1000, AnalyticsProtocol::Http, 10);
        let (proto, bytes) = metrics.top_protocol().unwrap();
        assert_eq!(proto, AnalyticsProtocol::Http);
        assert_eq!(bytes, 1000);
    }

    #[test]
    fn test_daily_metrics_peak_hour_empty() {
        let today = Utc::now().date_naive();
        let metrics = DailyMetrics::new(today);
        assert!(metrics.peak_hour().is_none());
    }

    #[test]
    fn test_daily_metrics_peak_hour_single() {
        let today = Utc::now().date_naive();
        let mut metrics = DailyMetrics::new(today);
        metrics.record_download(500, AnalyticsProtocol::Http, 22);
        let (hour, bytes) = metrics.peak_hour().unwrap();
        assert_eq!(hour, 22);
        assert_eq!(bytes, 500);
    }

    #[test]
    fn test_daily_metrics_format_bytes_zero() {
        assert_eq!(DailyMetrics::format_bytes(0), "0 B");
    }

    #[test]
    fn test_daily_metrics_format_bytes_exact_kb() {
        assert_eq!(DailyMetrics::format_bytes(1024), "1.00 KB");
    }

    #[test]
    fn test_daily_metrics_format_bytes_exact_mb() {
        assert_eq!(DailyMetrics::format_bytes(1_048_576), "1.00 MB");
    }

    #[test]
    fn test_daily_metrics_format_bytes_exact_gb() {
        assert_eq!(DailyMetrics::format_bytes(1_073_741_824), "1.00 GB");
    }

    #[test]
    fn test_daily_metrics_format_bytes_large() {
        assert_eq!(DailyMetrics::format_bytes(5_000_000_000), "4.66 GB");
    }

    #[test]
    fn test_daily_metrics_format_speed() {
        assert_eq!(DailyMetrics::format_speed(0), "0 B/s");
        assert_eq!(DailyMetrics::format_speed(1024), "1.00 KB/s");
        assert_eq!(DailyMetrics::format_speed(1_048_576), "1.00 MB/s");
    }

    // ===== AnalyticsConfig tests =====

    #[test]
    fn test_analytics_config_default() {
        let config = AnalyticsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.retention_days, DEFAULT_RETENTION_DAYS);
        assert!(config.track_protocol_breakdown);
        assert!(config.track_hourly_distribution);
    }

    #[test]
    fn test_analytics_config_custom_values() {
        let config = AnalyticsConfig {
            enabled: false,
            retention_days: 365,
            track_protocol_breakdown: false,
            track_hourly_distribution: false,
        };
        assert!(!config.enabled);
        assert_eq!(config.retention_days, 365);
        assert!(!config.track_protocol_breakdown);
        assert!(!config.track_hourly_distribution);
    }

    #[test]
    fn test_analytics_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"retention_days":30,"track_protocol_breakdown":true,"track_hourly_distribution":true,"unknown_field":"value"}"#;
        let config: AnalyticsConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.retention_days, 30);
    }

    #[test]
    fn test_analytics_config_clone_debug() {
        let config = AnalyticsConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.retention_days, config.retention_days);
        let _ = format!("{:?}", config);
    }

    // ===== AnalyticsSummary tests =====

    #[test]
    fn test_analytics_summary_serde_roundtrip() {
        let summary = AnalyticsSummary {
            days_covered: 7,
            start_date: NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 8, 7).unwrap(),
            total_bytes_downloaded: 10_000_000,
            total_bytes_uploaded: 1_000_000,
            total_tasks_started: 20,
            total_tasks_completed: 18,
            total_tasks_failed: 2,
            overall_peak_speed_bps: 5_000_000,
            avg_daily_bytes: 1_428_571,
            avg_daily_completions: 2.57,
            total_errors: 3,
            total_retries: 5,
            protocol_totals: HashMap::new(),
            hourly_totals: HashMap::new(),
            best_day: Some((NaiveDate::from_ymd_opt(2026, 8, 5).unwrap(), 3_000_000)),
            worst_day: Some((NaiveDate::from_ymd_opt(2026, 8, 2).unwrap(), 500_000)),
            total_active_time_secs: 3600,
            success_rate: 0.9,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deser: AnalyticsSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.days_covered, 7);
        assert_eq!(deser.total_bytes_downloaded, 10_000_000);
        assert_eq!(deser.total_tasks_completed, 18);
    }

    #[test]
    fn test_analytics_summary_format_report_with_best_worst_day() {
        let summary = AnalyticsSummary {
            days_covered: 3,
            start_date: NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 8, 3).unwrap(),
            total_bytes_downloaded: 6_000_000,
            total_bytes_uploaded: 600_000,
            total_tasks_started: 15,
            total_tasks_completed: 12,
            total_tasks_failed: 3,
            overall_peak_speed_bps: 3_000_000,
            avg_daily_bytes: 2_000_000,
            avg_daily_completions: 4.0,
            total_errors: 5,
            total_retries: 8,
            protocol_totals: HashMap::new(),
            hourly_totals: HashMap::new(),
            best_day: Some((NaiveDate::from_ymd_opt(2026, 8, 2).unwrap(), 3_000_000)),
            worst_day: Some((NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(), 1_000_000)),
            total_active_time_secs: 7200,
            success_rate: 0.8,
        };
        let report = summary.format_report();
        assert!(report.contains("Best Day"));
        assert!(report.contains("Worst Day"));
        assert!(report.contains("80.0%"));
        assert!(report.contains("2h 0m"));
    }

    #[test]
    fn test_analytics_summary_format_report_no_best_worst() {
        let summary = AnalyticsSummary {
            days_covered: 0,
            start_date: NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(),
            total_bytes_downloaded: 0,
            total_bytes_uploaded: 0,
            total_tasks_started: 0,
            total_tasks_completed: 0,
            total_tasks_failed: 0,
            overall_peak_speed_bps: 0,
            avg_daily_bytes: 0,
            avg_daily_completions: 0.0,
            total_errors: 0,
            total_retries: 0,
            protocol_totals: HashMap::new(),
            hourly_totals: HashMap::new(),
            best_day: None,
            worst_day: None,
            total_active_time_secs: 0,
            success_rate: 0.0,
        };
        let report = summary.format_report();
        assert!(!report.contains("Best Day"));
        assert!(!report.contains("Worst Day"));
        assert!(report.contains("0.0%"));
    }

    // ===== TrendDirection tests =====

    #[test]
    fn test_trend_direction_serde_roundtrip() {
        let directions = vec![
            TrendDirection::Improving,
            TrendDirection::Declining,
            TrendDirection::Stable,
        ];
        for dir in &directions {
            let json = serde_json::to_string(dir).unwrap();
            let deser: TrendDirection = serde_json::from_str(&json).unwrap();
            assert_eq!(*dir, deser);
        }
    }

    #[test]
    fn test_trend_direction_serde_lowercase() {
        let json = "\"improving\"";
        let dir: TrendDirection = serde_json::from_str(json).unwrap();
        assert_eq!(dir, TrendDirection::Improving);

        let json = "\"declining\"";
        let dir: TrendDirection = serde_json::from_str(json).unwrap();
        assert_eq!(dir, TrendDirection::Declining);

        let json = "\"stable\"";
        let dir: TrendDirection = serde_json::from_str(json).unwrap();
        assert_eq!(dir, TrendDirection::Stable);
    }

    #[test]
    fn test_trend_direction_clone_copy_debug() {
        let d = TrendDirection::Improving;
        let d2 = d; // Copy
        assert_eq!(d, d2);
        let _ = format!("{:?}", d);
        let d3 = d.clone();
        assert_eq!(d, d3);
    }

    // ===== TrendComparison tests =====

    #[test]
    fn test_trend_comparison_serde_roundtrip() {
        let comparison = TrendComparison {
            current: AnalyticsSummary {
                days_covered: 7,
                start_date: NaiveDate::from_ymd_opt(2026, 8, 8).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 8, 14).unwrap(),
                total_bytes_downloaded: 10_000_000,
                total_bytes_uploaded: 0,
                total_tasks_started: 10,
                total_tasks_completed: 8,
                total_tasks_failed: 2,
                overall_peak_speed_bps: 5_000_000,
                avg_daily_bytes: 1_428_571,
                avg_daily_completions: 1.14,
                total_errors: 3,
                total_retries: 5,
                protocol_totals: HashMap::new(),
                hourly_totals: HashMap::new(),
                best_day: None,
                worst_day: None,
                total_active_time_secs: 0,
                success_rate: 0.8,
            },
            previous: AnalyticsSummary {
                days_covered: 7,
                start_date: NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 8, 7).unwrap(),
                total_bytes_downloaded: 5_000_000,
                total_bytes_uploaded: 0,
                total_tasks_started: 8,
                total_tasks_completed: 5,
                total_tasks_failed: 3,
                overall_peak_speed_bps: 2_500_000,
                avg_daily_bytes: 714_285,
                avg_daily_completions: 0.71,
                total_errors: 6,
                total_retries: 10,
                protocol_totals: HashMap::new(),
                hourly_totals: HashMap::new(),
                best_day: None,
                worst_day: None,
                total_active_time_secs: 0,
                success_rate: 0.625,
            },
            download_change_pct: 100.0,
            completion_change_pct: 60.0,
            speed_change_pct: 100.0,
            error_change_pct: -50.0,
            trend_direction: TrendDirection::Improving,
        };
        let json = serde_json::to_string(&comparison).unwrap();
        let deser: TrendComparison = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.trend_direction, TrendDirection::Improving);
        assert!((deser.download_change_pct - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_trend_comparison_format_declining() {
        let comparison = TrendComparison {
            current: AnalyticsSummary {
                days_covered: 7,
                start_date: NaiveDate::from_ymd_opt(2026, 8, 8).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 8, 14).unwrap(),
                total_bytes_downloaded: 2_000_000,
                total_bytes_uploaded: 0,
                total_tasks_started: 5,
                total_tasks_completed: 2,
                total_tasks_failed: 3,
                overall_peak_speed_bps: 1_000_000,
                avg_daily_bytes: 285_714,
                avg_daily_completions: 0.29,
                total_errors: 10,
                total_retries: 15,
                protocol_totals: HashMap::new(),
                hourly_totals: HashMap::new(),
                best_day: None,
                worst_day: None,
                total_active_time_secs: 0,
                success_rate: 0.4,
            },
            previous: AnalyticsSummary {
                days_covered: 7,
                start_date: NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 8, 7).unwrap(),
                total_bytes_downloaded: 10_000_000,
                total_bytes_uploaded: 0,
                total_tasks_started: 20,
                total_tasks_completed: 18,
                total_tasks_failed: 2,
                overall_peak_speed_bps: 5_000_000,
                avg_daily_bytes: 1_428_571,
                avg_daily_completions: 2.57,
                total_errors: 3,
                total_retries: 5,
                protocol_totals: HashMap::new(),
                hourly_totals: HashMap::new(),
                best_day: None,
                worst_day: None,
                total_active_time_secs: 0,
                success_rate: 0.9,
            },
            download_change_pct: -80.0,
            completion_change_pct: -88.9,
            speed_change_pct: -80.0,
            error_change_pct: 233.3,
            trend_direction: TrendDirection::Declining,
        };
        let report = comparison.format_report();
        assert!(report.contains("Declining"));
        assert!(report.contains("-80.0%"));
    }

    #[test]
    fn test_trend_comparison_format_stable() {
        let comparison = TrendComparison {
            current: AnalyticsSummary {
                days_covered: 7,
                start_date: NaiveDate::from_ymd_opt(2026, 8, 8).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 8, 14).unwrap(),
                total_bytes_downloaded: 5_100_000,
                total_bytes_uploaded: 0,
                total_tasks_started: 10,
                total_tasks_completed: 9,
                total_tasks_failed: 1,
                overall_peak_speed_bps: 5_100_000,
                avg_daily_bytes: 728_571,
                avg_daily_completions: 1.29,
                total_errors: 2,
                total_retries: 3,
                protocol_totals: HashMap::new(),
                hourly_totals: HashMap::new(),
                best_day: None,
                worst_day: None,
                total_active_time_secs: 0,
                success_rate: 0.9,
            },
            previous: AnalyticsSummary {
                days_covered: 7,
                start_date: NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 8, 7).unwrap(),
                total_bytes_downloaded: 5_000_000,
                total_bytes_uploaded: 0,
                total_tasks_started: 10,
                total_tasks_completed: 9,
                total_tasks_failed: 1,
                overall_peak_speed_bps: 5_000_000,
                avg_daily_bytes: 714_285,
                avg_daily_completions: 1.29,
                total_errors: 2,
                total_retries: 3,
                protocol_totals: HashMap::new(),
                hourly_totals: HashMap::new(),
                best_day: None,
                worst_day: None,
                total_active_time_secs: 0,
                success_rate: 0.9,
            },
            download_change_pct: 2.0,
            completion_change_pct: 0.0,
            speed_change_pct: 2.0,
            error_change_pct: 0.0,
            trend_direction: TrendDirection::Stable,
        };
        let report = comparison.format_report();
        assert!(report.contains("Stable"));
    }

    // ===== AnalyticsManager comprehensive tests =====

    #[test]
    fn test_analytics_manager_default_trait() {
        let manager = AnalyticsManager::default();
        assert_eq!(manager.record_count(), 0);
        assert!(manager.config().enabled);
    }

    #[test]
    fn test_analytics_manager_with_config() {
        let config = AnalyticsConfig {
            enabled: true,
            retention_days: 30,
            track_protocol_breakdown: false,
            track_hourly_distribution: false,
        };
        let manager = AnalyticsManager::with_config(config);
        assert_eq!(manager.config().retention_days, 30);
        assert!(!manager.config().track_protocol_breakdown);
    }

    #[test]
    fn test_analytics_manager_today_mut_creates_record() {
        let mut manager = AnalyticsManager::new();
        assert!(manager.today().is_none());
        let _ = manager.today_mut();
        assert!(manager.today().is_some());
        assert_eq!(manager.record_count(), 1);
    }

    #[test]
    fn test_analytics_manager_record_download_disabled() {
        let mut manager = AnalyticsManager::with_config(AnalyticsConfig {
            enabled: false,
            ..Default::default()
        });
        manager.record_download(1000, "https://example.com/file.zip");
        assert!(manager.today().is_none());
    }

    #[test]
    fn test_analytics_manager_record_speed_disabled() {
        let mut manager = AnalyticsManager::with_config(AnalyticsConfig {
            enabled: false,
            ..Default::default()
        });
        manager.record_speed(500_000);
        assert!(manager.today().is_none());
    }

    #[test]
    fn test_analytics_manager_record_task_event_disabled() {
        let mut manager = AnalyticsManager::with_config(AnalyticsConfig {
            enabled: false,
            ..Default::default()
        });
        manager.record_task_event(TaskAnalyticsEvent::Started);
        assert!(manager.today().is_none());
    }

    #[test]
    fn test_analytics_manager_record_active_time_disabled() {
        let mut manager = AnalyticsManager::with_config(AnalyticsConfig {
            enabled: false,
            ..Default::default()
        });
        manager.record_active_time(60);
        assert!(manager.today().is_none());
    }

    #[test]
    fn test_analytics_manager_get_day_existing() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();
        manager.records.insert(today, DailyMetrics::new(today));
        assert!(manager.get_day(today).is_some());
    }

    #[test]
    fn test_analytics_manager_get_day_non_existing() {
        let manager = AnalyticsManager::new();
        let date = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        assert!(manager.get_day(date).is_none());
    }

    #[test]
    fn test_analytics_manager_summary_range_exact() {
        let mut manager = AnalyticsManager::new();
        let date1 = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2026, 8, 2).unwrap();
        let date3 = NaiveDate::from_ymd_opt(2026, 8, 3).unwrap();

        let mut m1 = DailyMetrics::new(date1);
        m1.bytes_downloaded = 1000;
        let mut m2 = DailyMetrics::new(date2);
        m2.bytes_downloaded = 2000;
        let mut m3 = DailyMetrics::new(date3);
        m3.bytes_downloaded = 3000;

        manager.records.insert(date1, m1);
        manager.records.insert(date2, m2);
        manager.records.insert(date3, m3);

        let summary = manager.summary_range(date1, date3).unwrap();
        assert_eq!(summary.days_covered, 3);
        assert_eq!(summary.total_bytes_downloaded, 6000);
        assert_eq!(summary.avg_daily_bytes, 2000);
    }

    #[test]
    fn test_analytics_manager_summary_range_partial_overlap() {
        let mut manager = AnalyticsManager::new();
        let date1 = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2026, 8, 2).unwrap();

        let mut m1 = DailyMetrics::new(date1);
        m1.bytes_downloaded = 1000;
        let mut m2 = DailyMetrics::new(date2);
        m2.bytes_downloaded = 2000;

        manager.records.insert(date1, m1);
        manager.records.insert(date2, m2);

        // Range that partially overlaps (date2 to date5)
        let start = NaiveDate::from_ymd_opt(2026, 8, 2).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 8, 5).unwrap();
        let summary = manager.summary_range(start, end).unwrap();
        assert_eq!(summary.days_covered, 1);
        assert_eq!(summary.total_bytes_downloaded, 2000);
    }

    #[test]
    fn test_analytics_manager_summary_range_no_overlap() {
        let mut manager = AnalyticsManager::new();
        let date1 = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();

        let mut m1 = DailyMetrics::new(date1);
        m1.bytes_downloaded = 1000;
        manager.records.insert(date1, m1);

        let start = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 8, 15).unwrap();
        assert!(manager.summary_range(start, end).is_none());
    }

    #[test]
    fn test_analytics_manager_summary_single_day() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        let mut metrics = DailyMetrics::new(today);
        metrics.bytes_downloaded = 5000;
        metrics.tasks_completed = 3;
        metrics.tasks_started = 5;
        metrics.peak_speed_bps = 1_000_000;
        manager.records.insert(today, metrics);

        let summary = manager.summary_last_n_days(1).unwrap();
        assert_eq!(summary.days_covered, 1);
        assert_eq!(summary.total_bytes_downloaded, 5000);
        assert_eq!(summary.total_tasks_completed, 3);
        assert!((summary.success_rate - 0.6).abs() < 0.01);
    }

    #[test]
    fn test_analytics_manager_compare_periods_declining() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        for i in 0..14 {
            let date = today - chrono::Duration::days(i);
            let mut metrics = DailyMetrics::new(date);
            if i < 7 {
                // Current period: lower values
                metrics.bytes_downloaded = 3000;
                metrics.tasks_completed = 2;
                metrics.peak_speed_bps = 500_000;
                metrics.tasks_started = 3;
                metrics.error_count = 5;
            } else {
                // Previous period: higher values
                metrics.bytes_downloaded = 10000;
                metrics.tasks_completed = 8;
                metrics.peak_speed_bps = 2_000_000;
                metrics.tasks_started = 10;
                metrics.error_count = 1;
            }
            manager.records.insert(date, metrics);
        }

        let comparison = manager.compare_periods(7).unwrap();
        assert!(comparison.download_change_pct < -60.0);
        assert_eq!(comparison.trend_direction, TrendDirection::Declining);
    }

    #[test]
    fn test_analytics_manager_compare_periods_stable() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        for i in 0..14 {
            let date = today - chrono::Duration::days(i);
            let mut metrics = DailyMetrics::new(date);
            metrics.bytes_downloaded = 5000;
            metrics.tasks_completed = 4;
            metrics.peak_speed_bps = 1_000_000;
            metrics.tasks_started = 5;
            manager.records.insert(date, metrics);
        }

        let comparison = manager.compare_periods(7).unwrap();
        assert!((comparison.download_change_pct).abs() < 1.0);
        assert_eq!(comparison.trend_direction, TrendDirection::Stable);
    }

    #[test]
    fn test_analytics_manager_compare_periods_missing_previous() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        // Only insert current period (7 days), no previous
        for i in 0..7 {
            let date = today - chrono::Duration::days(i);
            let mut metrics = DailyMetrics::new(date);
            metrics.bytes_downloaded = 5000;
            manager.records.insert(date, metrics);
        }

        // Should return None because previous period has no data
        assert!(manager.compare_periods(7).is_none());
    }

    #[test]
    fn test_analytics_manager_prune_disabled_config() {
        let mut manager = AnalyticsManager::with_config(AnalyticsConfig {
            enabled: false,
            retention_days: 1,
            ..Default::default()
        });

        let today = Utc::now().date_naive();
        let old_date = today - chrono::Duration::days(30);
        manager
            .records
            .insert(old_date, DailyMetrics::new(old_date));
        manager.records.insert(today, DailyMetrics::new(today));

        // Prune should not remove anything when disabled
        manager.prune_old_records();
        assert_eq!(manager.record_count(), 2);
    }

    #[test]
    fn test_analytics_manager_prune_max_records() {
        let mut manager = AnalyticsManager::with_config(AnalyticsConfig {
            retention_days: 3650, // very long retention
            ..Default::default()
        });

        // Insert more than MAX_RECORDS (365)
        let today = Utc::now().date_naive();
        for i in 0..400 {
            let date = today - chrono::Duration::days(i);
            manager.records.insert(date, DailyMetrics::new(date));
        }
        assert_eq!(manager.record_count(), 400);

        manager.prune_old_records();
        assert_eq!(manager.record_count(), MAX_RECORDS);
    }

    #[test]
    fn test_analytics_manager_insert_record() {
        let mut manager = AnalyticsManager::new();
        let date = NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        let mut metrics = DailyMetrics::new(date);
        metrics.bytes_downloaded = 9999;
        manager.insert_record(date, metrics);
        assert_eq!(manager.record_count(), 1);
        assert_eq!(manager.get_day(date).unwrap().bytes_downloaded, 9999);
    }

    #[test]
    fn test_analytics_manager_config_mut() {
        let mut manager = AnalyticsManager::new();
        assert!(manager.config().enabled);
        manager.config_mut().enabled = false;
        assert!(!manager.config().enabled);
    }

    #[test]
    fn test_analytics_manager_set_config() {
        let mut manager = AnalyticsManager::new();
        let config = AnalyticsConfig {
            retention_days: 180,
            ..Default::default()
        };
        manager.set_config(config);
        assert_eq!(manager.config().retention_days, 180);
    }

    #[test]
    fn test_analytics_manager_records_mut() {
        let mut manager = AnalyticsManager::new();
        let date = NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        manager.records_mut().insert(date, DailyMetrics::new(date));
        assert_eq!(manager.record_count(), 1);
    }

    #[test]
    fn test_analytics_manager_all_records_empty() {
        let manager = AnalyticsManager::new();
        assert!(manager.all_records().is_empty());
    }

    // ===== TaskAnalyticsEvent tests =====

    #[test]
    fn test_task_analytics_event_clone_copy_debug() {
        let e = TaskAnalyticsEvent::Started;
        let e2 = e; // Copy
        assert_eq!(e, e2);
        let _ = format!("{:?}", e);
        let e3 = e.clone();
        assert_eq!(e, e3);
    }

    #[test]
    fn test_task_analytics_event_all_variants() {
        let events = vec![
            TaskAnalyticsEvent::Started,
            TaskAnalyticsEvent::Completed,
            TaskAnalyticsEvent::Failed,
            TaskAnalyticsEvent::Paused,
            TaskAnalyticsEvent::Error,
            TaskAnalyticsEvent::Retry,
        ];
        assert_eq!(events.len(), 6);
        // All should be distinct
        for i in 0..events.len() {
            for j in (i + 1)..events.len() {
                assert_ne!(events[i], events[j]);
            }
        }
    }

    // ===== format_duration comprehensive tests =====

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0), "0m");
    }

    #[test]
    fn test_format_duration_minutes_only() {
        assert_eq!(format_duration(59), "0m");
        assert_eq!(format_duration(60), "1m");
        assert_eq!(format_duration(3599), "59m");
    }

    #[test]
    fn test_format_duration_hours_minutes() {
        assert_eq!(format_duration(3600), "1h 0m");
        assert_eq!(format_duration(3660), "1h 1m");
        assert_eq!(format_duration(7200), "2h 0m");
    }

    #[test]
    fn test_format_duration_days() {
        assert_eq!(format_duration(86400), "1d 0h 0m");
        assert_eq!(format_duration(90061), "1d 1h 1m");
    }

    // ===== Persistence tests =====

    #[test]
    fn test_save_load_analytics_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = AnalyticsConfig {
            enabled: true,
            retention_days: 45,
            track_protocol_breakdown: false,
            track_hourly_distribution: true,
        };
        save_analytics_config(dir.path(), &config).unwrap();
        let loaded = load_analytics_config(dir.path()).unwrap();
        assert_eq!(loaded.retention_days, 45);
        assert!(!loaded.track_protocol_breakdown);
        assert!(loaded.track_hourly_distribution);
    }

    #[test]
    fn test_load_analytics_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_analytics_config(dir.path()).is_none());
    }

    #[test]
    fn test_load_analytics_config_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("analytics_config.json");
        std::fs::write(&path, "not valid json").unwrap();
        assert!(load_analytics_config(dir.path()).is_none());
    }

    #[test]
    fn test_save_load_analytics_records_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut records = HashMap::new();
        let date1 = NaiveDate::from_ymd_opt(2026, 8, 13).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();

        let mut m1 = DailyMetrics::new(date1);
        m1.bytes_downloaded = 5000;
        m1.record_download(1000, AnalyticsProtocol::Http, 10);

        let mut m2 = DailyMetrics::new(date2);
        m2.bytes_downloaded = 10000;
        m2.record_speed(2_000_000);

        records.insert(date1, m1);
        records.insert(date2, m2);

        save_analytics_records(dir.path(), &records).unwrap();
        let loaded = load_analytics_records(dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[&date1].bytes_downloaded, 6000); // 5000 + 1000
        assert_eq!(loaded[&date2].peak_speed_bps, 2_000_000);
    }

    #[test]
    fn test_load_analytics_records_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_analytics_records(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_analytics_records_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("analytics_records.json");
        std::fs::write(&path, "{invalid json}").unwrap();
        let result = load_analytics_records(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_save_analytics_config_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let config1 = AnalyticsConfig {
            retention_days: 30,
            ..Default::default()
        };
        save_analytics_config(dir.path(), &config1).unwrap();

        let config2 = AnalyticsConfig {
            retention_days: 90,
            ..Default::default()
        };
        save_analytics_config(dir.path(), &config2).unwrap();

        let loaded = load_analytics_config(dir.path()).unwrap();
        assert_eq!(loaded.retention_days, 90);
    }

    // ===== Summary best/worst day edge cases =====

    #[test]
    fn test_summary_best_worst_day_excludes_zero() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();
        let d1 = today - chrono::Duration::days(2);
        let d2 = today - chrono::Duration::days(1);

        let mut m1 = DailyMetrics::new(d1);
        m1.bytes_downloaded = 0; // zero-download day
        let mut m2 = DailyMetrics::new(d2);
        m2.bytes_downloaded = 5000;
        let mut m3 = DailyMetrics::new(today);
        m3.bytes_downloaded = 2000;

        manager.records.insert(d1, m1);
        manager.records.insert(d2, m2);
        manager.records.insert(today, m3);

        let summary = manager.summary_last_n_days(3).unwrap();
        // Best day should be d2 (5000)
        assert_eq!(summary.best_day, Some((d2, 5000)));
        // Worst day should be today (2000), not d1 (0 bytes excluded)
        assert_eq!(summary.worst_day, Some((today, 2000)));
    }

    #[test]
    fn test_summary_all_zero_downloads_no_worst_day() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();
        let d1 = today - chrono::Duration::days(1);

        let m1 = DailyMetrics::new(d1); // 0 bytes
        let m2 = DailyMetrics::new(today); // 0 bytes
        manager.records.insert(d1, m1);
        manager.records.insert(today, m2);

        let summary = manager.summary_last_n_days(2).unwrap();
        assert!(summary.worst_day.is_none()); // all zero, no worst
        assert_eq!(summary.best_day, Some((today, 0))); // best is still set
    }

    // ===== Compare periods edge cases =====

    #[test]
    fn test_compare_periods_both_zero_downloads() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        for i in 0..14 {
            let date = today - chrono::Duration::days(i);
            let metrics = DailyMetrics::new(date); // all zeros
            manager.records.insert(date, metrics);
        }

        let comparison = manager.compare_periods(7).unwrap();
        assert!((comparison.download_change_pct).abs() < 0.01);
        assert!((completion_change_pct(&comparison)).abs() < 0.01);
        assert_eq!(comparison.trend_direction, TrendDirection::Stable);
    }

    fn completion_change_pct(c: &TrendComparison) -> f64 {
        c.completion_change_pct
    }

    #[test]
    fn test_compare_periods_previous_zero_current_nonzero() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        for i in 0..14 {
            let date = today - chrono::Duration::days(i);
            let mut metrics = DailyMetrics::new(date);
            if i < 7 {
                metrics.bytes_downloaded = 5000;
                metrics.tasks_completed = 3;
                metrics.tasks_started = 4;
            }
            manager.records.insert(date, metrics);
        }

        let comparison = manager.compare_periods(7).unwrap();
        assert!((comparison.download_change_pct - 100.0).abs() < 0.01);
        assert!((comparison.completion_change_pct - 100.0).abs() < 0.01);
    }

    // ===== Protocol and hourly aggregation in summary =====

    #[test]
    fn test_summary_protocol_aggregation() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();
        let d1 = today - chrono::Duration::days(1);

        let mut m1 = DailyMetrics::new(d1);
        m1.protocol_bytes.insert(AnalyticsProtocol::Http, 3000);
        m1.protocol_bytes.insert(AnalyticsProtocol::Torrent, 1000);

        let mut m2 = DailyMetrics::new(today);
        m2.protocol_bytes.insert(AnalyticsProtocol::Http, 2000);
        m2.protocol_bytes.insert(AnalyticsProtocol::Ed2k, 500);

        manager.records.insert(d1, m1);
        manager.records.insert(today, m2);

        let summary = manager.summary_last_n_days(2).unwrap();
        assert_eq!(summary.protocol_totals[&AnalyticsProtocol::Http], 5000);
        assert_eq!(summary.protocol_totals[&AnalyticsProtocol::Torrent], 1000);
        assert_eq!(summary.protocol_totals[&AnalyticsProtocol::Ed2k], 500);
    }

    #[test]
    fn test_summary_hourly_aggregation() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();
        let d1 = today - chrono::Duration::days(1);

        let mut m1 = DailyMetrics::new(d1);
        m1.hourly_bytes.insert(10, 1000);
        m1.hourly_bytes.insert(14, 3000);

        let mut m2 = DailyMetrics::new(today);
        m2.hourly_bytes.insert(10, 2000);
        m2.hourly_bytes.insert(22, 500);

        manager.records.insert(d1, m1);
        manager.records.insert(today, m2);

        let summary = manager.summary_last_n_days(2).unwrap();
        assert_eq!(summary.hourly_totals[&10], 3000);
        assert_eq!(summary.hourly_totals[&14], 3000);
        assert_eq!(summary.hourly_totals[&22], 500);
    }

    // ===== Success rate edge cases =====

    #[test]
    fn test_success_rate_zero_started() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        let mut metrics = DailyMetrics::new(today);
        metrics.tasks_started = 0;
        metrics.tasks_completed = 0;
        manager.records.insert(today, metrics);

        let summary = manager.summary_last_n_days(1).unwrap();
        assert!((summary.success_rate).abs() < 0.01);
    }

    #[test]
    fn test_success_rate_100_percent() {
        let mut manager = AnalyticsManager::new();
        let today = Utc::now().date_naive();

        let mut metrics = DailyMetrics::new(today);
        metrics.tasks_started = 10;
        metrics.tasks_completed = 10;
        manager.records.insert(today, metrics);

        let summary = manager.summary_last_n_days(1).unwrap();
        assert!((summary.success_rate - 1.0).abs() < 0.01);
    }

    // ===== Constants validation =====

    #[test]
    fn test_default_retention_days() {
        assert_eq!(DEFAULT_RETENTION_DAYS, 90);
    }

    #[test]
    fn test_max_records_constant() {
        assert_eq!(MAX_RECORDS, 365);
    }
}
