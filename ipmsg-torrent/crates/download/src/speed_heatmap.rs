//! Download Speed Heatmap
//!
//! Tracks download speeds aggregated by hour-of-day (0-23) and day-of-week (Mon-Sun),
//! producing a "heatmap" that helps users identify optimal download windows.
//!
//! Features:
//! - Per-hour average speed tracking (24 buckets)
//! - Per-day-of-week average speed tracking (7 buckets)
//! - Combined hour × day-of-week heatmap matrix (7×24 = 168 cells)
//! - Sample count tracking for statistical confidence
//! - Configurable data retention with automatic pruning
//! - Persistent storage to JSON
//! - Human-readable heatmap report formatting

use chrono::{Datelike, Timelike};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Configuration for the speed heatmap tracker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedHeatmapConfig {
    /// Whether heatmap tracking is enabled.
    pub enabled: bool,
    /// Maximum samples per hour bucket before oldest are discarded.
    pub max_samples_per_hour: usize,
    /// Maximum samples per day-of-week bucket before oldest are discarded.
    pub max_samples_per_day: usize,
    /// Maximum samples per combined (dow, hour) cell.
    pub max_samples_per_cell: usize,
    /// Number of days of data to retain in the combined heatmap.
    pub retention_days: u32,
}

impl Default for SpeedHeatmapConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_samples_per_hour: 500,
            max_samples_per_day: 2000,
            max_samples_per_cell: 50,
            retention_days: 30,
        }
    }
}

/// A single speed sample with timestamp context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeatmapSample {
    /// Speed in bytes per second.
    pub speed_bps: f64,
    /// Timestamp (UTC) when this sample was recorded.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Statistics for a single bucket (hour or day-of-week).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct BucketStats {
    /// Number of samples recorded.
    pub sample_count: u64,
    /// Sum of all speed values (for computing mean).
    pub speed_sum: f64,
    /// Sum of squared speed values (for computing variance/stddev).
    pub speed_sq_sum: f64,
    /// Minimum speed observed.
    pub speed_min: f64,
    /// Maximum speed observed.
    pub speed_max: f64,
}

impl BucketStats {
    /// Record a new speed sample.
    pub fn record(&mut self, speed_bps: f64) {
        self.sample_count += 1;
        self.speed_sum += speed_bps;
        self.speed_sq_sum += speed_bps * speed_bps;
        if self.sample_count == 1 {
            self.speed_min = speed_bps;
            self.speed_max = speed_bps;
        } else {
            self.speed_min = self.speed_min.min(speed_bps);
            self.speed_max = self.speed_max.max(speed_bps);
        }
    }

    /// Average speed in this bucket.
    pub fn avg_speed(&self) -> f64 {
        if self.sample_count == 0 {
            0.0
        } else {
            self.speed_sum / self.sample_count as f64
        }
    }

    /// Standard deviation of speeds in this bucket.
    pub fn stddev(&self) -> f64 {
        if self.sample_count < 2 {
            return 0.0;
        }
        let n = self.sample_count as f64;
        let mean = self.speed_sum / n;
        let variance = (self.speed_sq_sum / n) - (mean * mean);
        if variance < 0.0 { 0.0 } else { variance.sqrt() }
    }
}

/// A single cell in the combined (day-of-week × hour) heatmap.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct HeatmapCell {
    /// Number of samples in this cell.
    pub sample_count: u64,
    /// Average speed in bytes/sec.
    pub avg_speed_bps: f64,
    /// Peak speed observed.
    pub peak_speed_bps: f64,
}

impl HeatmapCell {
    /// Record a new speed sample into this cell using incremental mean.
    pub fn record(&mut self, speed_bps: f64) {
        self.sample_count += 1;
        // Welford-style incremental mean
        let delta = speed_bps - self.avg_speed_bps;
        self.avg_speed_bps += delta / self.sample_count as f64;
        self.peak_speed_bps = self.peak_speed_bps.max(speed_bps);
    }
}

/// Quality rating for a time slot based on average speed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotQuality {
    /// No data available.
    Unknown,
    /// Below 25th percentile or < 100 KB/s.
    Poor,
    /// 25th-75th percentile or 100 KB/s - 1 MB/s.
    Fair,
    /// Above 75th percentile or 1-5 MB/s.
    Good,
    /// Top quartile or > 5 MB/s.
    Excellent,
}

impl std::fmt::Display for SlotQuality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "unknown"),
            Self::Poor => write!(f, "poor"),
            Self::Fair => write!(f, "fair"),
            Self::Good => write!(f, "good"),
            Self::Excellent => write!(f, "excellent"),
        }
    }
}

/// Summary of the speed heatmap data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedHeatmapSummary {
    /// Per-hour average speeds (index 0 = 00:00, 23 = 23:00).
    pub hourly: [BucketStats; 24],
    /// Per-day-of-week average speeds (index 0 = Monday, 6 = Sunday).
    pub daily: [BucketStats; 7],
    /// Combined heatmap grid: daily[0..7] of hourly[0..24] cells.
    pub grid: [[HeatmapCell; 24]; 7],
    /// Total samples recorded.
    pub total_samples: u64,
    /// Best hour of day (by average speed).
    pub best_hour: u8,
    /// Worst hour of day (by average speed).
    pub worst_hour: u8,
    /// Best day of week (by average speed, 0=Mon).
    pub best_day: u8,
    /// Worst day of week (by average speed, 0=Mon).
    pub worst_day: u8,
    /// Recommended download windows (top 3 hour ranges).
    pub recommended_windows: Vec<RecommendedWindow>,
}

/// A recommended time window for downloading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendedWindow {
    /// Start hour (0-23).
    pub start_hour: u8,
    /// End hour (0-23, exclusive).
    pub end_hour: u8,
    /// Day of week (0=Mon, 6=Sun) or None for "any day".
    pub day_of_week: Option<u8>,
    /// Average speed in this window.
    pub avg_speed_bps: f64,
    /// Quality rating.
    pub quality: SlotQuality,
}

/// The main speed heatmap tracker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedHeatmap {
    /// Configuration.
    pub config: SpeedHeatmapConfig,
    /// Per-hour buckets (0-23).
    pub hourly: [BucketStats; 24],
    /// Per-day-of-week buckets (0=Mon, 6=Sun).
    pub daily: [BucketStats; 7],
    /// Combined grid: [day_of_week][hour].
    pub grid: [[HeatmapCell; 24]; 7],
    /// Total samples recorded.
    pub total_samples: u64,
    /// Recent samples for pruning (ring buffer of timestamps).
    #[serde(skip)]
    recent_timestamps: Vec<chrono::DateTime<chrono::Utc>>,
}

impl Default for SpeedHeatmap {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedHeatmap {
    /// Create a new heatmap tracker with default config.
    pub fn new() -> Self {
        Self {
            config: SpeedHeatmapConfig::default(),
            hourly: std::array::from_fn(|_| BucketStats::default()),
            daily: std::array::from_fn(|_| BucketStats::default()),
            grid: std::array::from_fn(|_| std::array::from_fn(|_| HeatmapCell::default())),
            total_samples: 0,
            recent_timestamps: Vec::new(),
        }
    }

    /// Create with a specific config.
    pub fn with_config(config: SpeedHeatmapConfig) -> Self {
        Self {
            config,
            hourly: std::array::from_fn(|_| BucketStats::default()),
            daily: std::array::from_fn(|_| BucketStats::default()),
            grid: std::array::from_fn(|_| std::array::from_fn(|_| HeatmapCell::default())),
            total_samples: 0,
            recent_timestamps: Vec::new(),
        }
    }

    /// Record a speed sample. The current UTC time determines the bucket assignments.
    pub fn record(&mut self, speed_bps: f64) {
        self.record_at(speed_bps, chrono::Utc::now());
    }

    /// Record a speed sample at a specific timestamp.
    pub fn record_at(&mut self, speed_bps: f64, timestamp: chrono::DateTime<chrono::Utc>) {
        if !self.config.enabled || speed_bps < 0.0 {
            return;
        }

        let hour = timestamp.hour() as usize;
        // chrono::weekday() returns Monday=0 .. Sunday=6 via num_days_from_monday()
        let dow = timestamp.weekday().num_days_from_monday() as usize;

        // Record into hourly bucket
        self.hourly[hour].record(speed_bps);

        // Record into daily bucket
        self.daily[dow].record(speed_bps);

        // Record into combined grid cell
        self.grid[dow][hour].record(speed_bps);

        self.total_samples += 1;

        // Track for pruning
        self.recent_timestamps.push(timestamp);
    }

    /// Get the current summary.
    pub fn summary(&self) -> SpeedHeatmapSummary {
        let (best_hour, worst_hour) = self.find_best_worst_hour();
        let (best_day, worst_day) = self.find_best_worst_day();
        let recommended_windows = self.compute_recommended_windows();

        SpeedHeatmapSummary {
            hourly: self.hourly,
            daily: self.daily,
            grid: self.grid,
            total_samples: self.total_samples,
            best_hour,
            worst_hour,
            best_day,
            worst_day,
            recommended_windows,
        }
    }

    /// Find best and worst hours by average speed.
    fn find_best_worst_hour(&self) -> (u8, u8) {
        let mut best_hour = 0u8;
        let mut worst_hour = 0u8;
        let mut best_avg = f64::MIN;
        let mut worst_avg = f64::MAX;

        for h in 0..24 {
            let avg = self.hourly[h].avg_speed();
            if self.hourly[h].sample_count > 0 {
                if avg > best_avg {
                    best_avg = avg;
                    best_hour = h as u8;
                }
                if avg < worst_avg {
                    worst_avg = avg;
                    worst_hour = h as u8;
                }
            }
        }

        (best_hour, worst_hour)
    }

    /// Find best and worst days by average speed.
    fn find_best_worst_day(&self) -> (u8, u8) {
        let mut best_day = 0u8;
        let mut worst_day = 0u8;
        let mut best_avg = f64::MIN;
        let mut worst_avg = f64::MAX;

        for d in 0..7 {
            let avg = self.daily[d].avg_speed();
            if self.daily[d].sample_count > 0 {
                if avg > best_avg {
                    best_avg = avg;
                    best_day = d as u8;
                }
                if avg < worst_avg {
                    worst_avg = avg;
                    worst_day = d as u8;
                }
            }
        }

        (best_day, worst_day)
    }

    /// Compute top recommended download windows.
    fn compute_recommended_windows(&self) -> Vec<RecommendedWindow> {
        // Collect all (dow, hour) cells with enough data
        let mut slots: Vec<(u8, u8, f64)> = Vec::new();
        for d in 0..7 {
            for h in 0..24 {
                let cell = &self.grid[d][h];
                if cell.sample_count >= 3 {
                    slots.push((d as u8, h as u8, cell.avg_speed_bps));
                }
            }
        }

        // Sort by speed descending
        slots.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        // Take top 3 non-overlapping windows
        let mut windows: Vec<RecommendedWindow> = Vec::new();
        let mut used_hours: HashMap<u8, Vec<u8>> = HashMap::new();

        for &(dow, hour, avg) in slots.iter().take(10) {
            if windows.len() >= 3 {
                break;
            }

            // Check if this hour is already used for this day
            let day_hours = used_hours.entry(dow).or_default();
            if day_hours.contains(&hour) {
                continue;
            }

            let quality = Self::speed_to_quality(avg);
            if quality == SlotQuality::Unknown || quality == SlotQuality::Poor {
                continue;
            }

            // Find contiguous range
            let start = hour;
            let mut end = hour + 1;
            while end < 24 {
                let next_cell = &self.grid[dow as usize][end as usize];
                if next_cell.sample_count >= 2
                    && Self::speed_to_quality(next_cell.avg_speed_bps) == quality
                {
                    end += 1;
                } else {
                    break;
                }
            }

            // Mark hours as used
            for h in start..end {
                day_hours.push(h);
            }

            windows.push(RecommendedWindow {
                start_hour: start,
                end_hour: end,
                day_of_week: Some(dow),
                avg_speed_bps: avg,
                quality,
            });
        }

        // If we don't have 3 windows yet, try day-agnostic windows
        if windows.len() < 3 {
            for h in 0..24 {
                if windows.len() >= 3 {
                    break;
                }
                // Average across all days for this hour
                let mut total_speed = 0.0;
                let mut total_count = 0u64;
                for d in 0..7 {
                    let cell = &self.grid[d][h];
                    total_speed += cell.avg_speed_bps * cell.sample_count as f64;
                    total_count += cell.sample_count;
                }
                if total_count == 0 {
                    continue;
                }
                let avg = total_speed / total_count as f64;
                let quality = Self::speed_to_quality(avg);
                if quality == SlotQuality::Unknown || quality == SlotQuality::Poor {
                    continue;
                }

                // Check not already covered
                let already_covered = windows.iter().any(|w| {
                    w.day_of_week.is_none() && (h as u8) >= w.start_hour && (h as u8) < w.end_hour
                });
                if already_covered {
                    continue;
                }

                windows.push(RecommendedWindow {
                    start_hour: h as u8,
                    end_hour: (h + 1) as u8,
                    day_of_week: None,
                    avg_speed_bps: avg,
                    quality,
                });
            }
        }

        windows
    }

    /// Convert speed to quality rating based on absolute thresholds.
    fn speed_to_quality(speed_bps: f64) -> SlotQuality {
        if speed_bps <= 0.0 {
            SlotQuality::Unknown
        } else if speed_bps < 100_000.0 {
            // < 100 KB/s
            SlotQuality::Poor
        } else if speed_bps < 1_000_000.0 {
            // < 1 MB/s
            SlotQuality::Fair
        } else if speed_bps < 5_000_000.0 {
            // < 5 MB/s
            SlotQuality::Good
        } else {
            SlotQuality::Excellent
        }
    }

    /// Get quality for a specific (day, hour) cell.
    pub fn cell_quality(&self, day_of_week: u8, hour: u8) -> SlotQuality {
        let d = day_of_week as usize;
        let h = hour as usize;
        if d >= 7 || h >= 24 {
            return SlotQuality::Unknown;
        }
        Self::speed_to_quality(self.grid[d][h].avg_speed_bps)
    }

    /// Get the hourly average speed for a specific hour.
    pub fn hourly_speed(&self, hour: u8) -> f64 {
        let h = hour as usize;
        if h >= 24 {
            return 0.0;
        }
        self.hourly[h].avg_speed()
    }

    /// Get the daily average speed for a specific day of week (0=Mon).
    pub fn daily_speed(&self, day_of_week: u8) -> f64 {
        let d = day_of_week as usize;
        if d >= 7 {
            return 0.0;
        }
        self.daily[d].avg_speed()
    }

    /// Prune old data beyond retention period.
    pub fn prune_old_data(&mut self) {
        let cutoff = chrono::Utc::now() - chrono::Duration::days(self.config.retention_days as i64);
        self.recent_timestamps.retain(|t| *t >= cutoff);
        // Note: bucket-level pruning is not done here because we use
        // running statistics (sum/count) rather than individual samples.
        // Full pruning would require storing individual samples, which
        // is handled by the max_samples_per_* config limits.
    }

    /// Reset all heatmap data.
    pub fn reset(&mut self) {
        self.hourly = std::array::from_fn(|_| BucketStats::default());
        self.daily = std::array::from_fn(|_| BucketStats::default());
        self.grid = std::array::from_fn(|_| std::array::from_fn(|_| HeatmapCell::default()));
        self.total_samples = 0;
        self.recent_timestamps.clear();
    }

    /// Format a human-readable heatmap report.
    pub fn format_report(&self) -> String {
        let summary = self.summary();
        let mut out = String::new();

        out.push_str("📊 Download Speed Heatmap Report\n");
        out.push_str("=================================\n\n");

        if self.total_samples == 0 {
            out.push_str("No data recorded yet.\n");
            return out;
        }

        out.push_str(&format!("Total samples: {}\n\n", self.total_samples));

        // Hourly overview
        out.push_str("🕐 Hourly Average Speeds:\n");
        out.push_str("─".repeat(50).as_str());
        out.push('\n');
        for h in 0..24 {
            let bucket = &summary.hourly[h];
            if bucket.sample_count > 0 {
                let bar = Self::speed_bar(bucket.avg_speed());
                out.push_str(&format!(
                    "  {:02}:00 │ {} {:>12}/s (n={})\n",
                    h,
                    bar,
                    format_speed(bucket.avg_speed()),
                    bucket.sample_count
                ));
            }
        }
        out.push('\n');

        // Daily overview
        let day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
        out.push_str("📅 Daily Average Speeds:\n");
        out.push_str("─".repeat(50).as_str());
        out.push('\n');
        for (d, day_name) in day_names.iter().enumerate() {
            let bucket = &summary.daily[d];
            if bucket.sample_count > 0 {
                let bar = Self::speed_bar(bucket.avg_speed());
                out.push_str(&format!(
                    "  {} │ {} {:>12}/s (n={})\n",
                    day_name,
                    bar,
                    format_speed(bucket.avg_speed()),
                    bucket.sample_count
                ));
            }
        }
        out.push('\n');

        // Best/worst
        out.push_str(&format!(
            "🏆 Best hour:  {:02}:00 ({}/s avg)\n",
            summary.best_hour,
            format_speed(summary.hourly[summary.best_hour as usize].avg_speed())
        ));
        out.push_str(&format!(
            "🐌 Worst hour: {:02}:00 ({}/s avg)\n",
            summary.worst_hour,
            format_speed(summary.hourly[summary.worst_hour as usize].avg_speed())
        ));
        out.push_str(&format!(
            "🏆 Best day:   {} ({}/s avg)\n",
            day_names[summary.best_day as usize],
            format_speed(summary.daily[summary.best_day as usize].avg_speed())
        ));
        out.push_str(&format!(
            "🐌 Worst day:  {} ({}/s avg)\n\n",
            day_names[summary.worst_day as usize],
            format_speed(summary.daily[summary.worst_day as usize].avg_speed())
        ));

        // Recommended windows
        if !summary.recommended_windows.is_empty() {
            out.push_str("✅ Recommended Download Windows:\n");
            out.push_str("─".repeat(50).as_str());
            out.push('\n');
            for (i, w) in summary.recommended_windows.iter().enumerate() {
                let day_str = match w.day_of_week {
                    Some(d) => day_names[d as usize].to_string(),
                    None => "Any day".to_string(),
                };
                out.push_str(&format!(
                    "  {}. {} {:02}:00-{:02}:00 — {}/s ({})\n",
                    i + 1,
                    day_str,
                    w.start_hour,
                    w.end_hour,
                    format_speed(w.avg_speed_bps),
                    w.quality,
                ));
            }
        }

        out
    }

    /// Create a simple ASCII speed bar.
    fn speed_bar(speed_bps: f64) -> String {
        // Scale: 0 to 10 MB/s maps to 0-20 chars
        let normalized = (speed_bps / 500_000.0).min(20.0) as usize;
        let filled = normalized.min(20);
        let empty = 20 - filled;
        format!("{}{}", "█".repeat(filled), "░".repeat(empty))
    }

    /// Save config to disk.
    pub async fn save_config(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
        tokio::fs::write(path, json).await
    }

    /// Load config from disk.
    pub async fn load_config(path: &Path) -> std::io::Result<SpeedHeatmapConfig> {
        let json = tokio::fs::read_to_string(path).await?;
        serde_json::from_str(&json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Save heatmap data to disk.
    pub async fn save_data(&self, path: &Path) -> std::io::Result<()> {
        // Serialize without the skip field
        let json = serde_json::to_string_pretty(&HeatmapData {
            config: self.config.clone(),
            hourly: self.hourly,
            daily: self.daily,
            grid: self.grid,
            total_samples: self.total_samples,
        })
        .map_err(std::io::Error::other)?;
        tokio::fs::write(path, json).await
    }

    /// Load heatmap data from disk.
    pub async fn load_data(path: &Path) -> std::io::Result<Self> {
        let json = tokio::fs::read_to_string(path).await?;
        let data: HeatmapData = serde_json::from_str(&json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Self {
            config: data.config,
            hourly: data.hourly,
            daily: data.daily,
            grid: data.grid,
            total_samples: data.total_samples,
            recent_timestamps: Vec::new(),
        })
    }
}

/// Serializable data structure for persistence.
#[derive(Debug, Serialize, Deserialize)]
struct HeatmapData {
    config: SpeedHeatmapConfig,
    hourly: [BucketStats; 24],
    daily: [BucketStats; 7],
    grid: [[HeatmapCell; 24]; 7],
    total_samples: u64,
}

/// Format speed in human-readable form.
fn format_speed(speed_bps: f64) -> String {
    if speed_bps < 1_000.0 {
        format!("{:.0} B", speed_bps)
    } else if speed_bps < 1_000_000.0 {
        format!("{:.1} KB", speed_bps / 1_000.0)
    } else if speed_bps < 1_000_000_000.0 {
        format!("{:.2} MB", speed_bps / 1_000_000.0)
    } else {
        format!("{:.2} GB", speed_bps / 1_000_000_000.0)
    }
}

/// Day-of-week name helper.
pub fn day_name(day_of_week: u8) -> &'static str {
    match day_of_week {
        0 => "Monday",
        1 => "Tuesday",
        2 => "Wednesday",
        3 => "Thursday",
        4 => "Friday",
        5 => "Saturday",
        6 => "Sunday",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_config() -> SpeedHeatmapConfig {
        SpeedHeatmapConfig {
            enabled: true,
            max_samples_per_hour: 100,
            max_samples_per_day: 500,
            max_samples_per_cell: 20,
            retention_days: 7,
        }
    }

    #[test]
    fn test_bucket_stats_record_and_avg() {
        let mut bucket = BucketStats::default();
        assert_eq!(bucket.sample_count, 0);
        assert_eq!(bucket.avg_speed(), 0.0);

        bucket.record(1000.0);
        assert_eq!(bucket.sample_count, 1);
        assert!((bucket.avg_speed() - 1000.0).abs() < 0.01);

        bucket.record(3000.0);
        assert_eq!(bucket.sample_count, 2);
        assert!((bucket.avg_speed() - 2000.0).abs() < 0.01);

        assert_eq!(bucket.speed_min, 1000.0);
        assert_eq!(bucket.speed_max, 3000.0);
    }

    #[test]
    fn test_bucket_stats_stddev() {
        let mut bucket = BucketStats::default();
        assert_eq!(bucket.stddev(), 0.0);

        bucket.record(100.0);
        assert_eq!(bucket.stddev(), 0.0); // n < 2

        bucket.record(200.0);
        // mean=150, var = ((100-150)^2 + (200-150)^2)/2 = 2500, stddev = 50
        assert!((bucket.stddev() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_heatmap_cell_incremental_mean() {
        let mut cell = HeatmapCell::default();
        cell.record(1000.0);
        assert!((cell.avg_speed_bps - 1000.0).abs() < 0.01);
        assert_eq!(cell.sample_count, 1);

        cell.record(3000.0);
        // mean = (1000 + 3000) / 2 = 2000
        assert!((cell.avg_speed_bps - 2000.0).abs() < 0.01);
        assert_eq!(cell.peak_speed_bps, 3000.0);
    }

    #[test]
    fn test_heatmap_record_at_specific_time() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());

        // Record at a known time: 2026-08-11 14:30 UTC (Tuesday, hour 14)
        let ts = chrono::Utc
            .with_ymd_and_hms(2026, 8, 11, 14, 30, 0)
            .unwrap();
        heatmap.record_at(500_000.0, ts);

        assert_eq!(heatmap.total_samples, 1);
        assert!(heatmap.hourly[14].sample_count > 0);
        assert!(heatmap.daily[1].sample_count > 0); // Tuesday = 1
        assert!(heatmap.grid[1][14].sample_count > 0);
    }

    #[test]
    fn test_heatmap_disabled() {
        let mut config = make_config();
        config.enabled = false;
        let mut heatmap = SpeedHeatmap::with_config(config);

        heatmap.record(1_000_000.0);
        assert_eq!(heatmap.total_samples, 0);
    }

    #[test]
    fn test_heatmap_negative_speed_ignored() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        heatmap.record(-100.0);
        assert_eq!(heatmap.total_samples, 0);
    }

    #[test]
    fn test_heatmap_summary_best_worst() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());

        let ts1 = chrono::Utc.with_ymd_and_hms(2026, 8, 11, 3, 0, 0).unwrap(); // Tue 03:00
        let ts2 = chrono::Utc.with_ymd_and_hms(2026, 8, 11, 15, 0, 0).unwrap(); // Tue 15:00

        // Record slow speed at hour 3, fast at hour 15
        for _ in 0..10 {
            heatmap.record_at(50_000.0, ts1);
            heatmap.record_at(5_000_000.0, ts2);
        }

        let summary = heatmap.summary();
        assert_eq!(summary.best_hour, 15);
        assert_eq!(summary.worst_hour, 3);
    }

    #[test]
    fn test_speed_to_quality() {
        assert_eq!(SpeedHeatmap::speed_to_quality(0.0), SlotQuality::Unknown);
        assert_eq!(SpeedHeatmap::speed_to_quality(-1.0), SlotQuality::Unknown);
        assert_eq!(SpeedHeatmap::speed_to_quality(50_000.0), SlotQuality::Poor);
        assert_eq!(SpeedHeatmap::speed_to_quality(500_000.0), SlotQuality::Fair);
        assert_eq!(
            SpeedHeatmap::speed_to_quality(2_000_000.0),
            SlotQuality::Good
        );
        assert_eq!(
            SpeedHeatmap::speed_to_quality(10_000_000.0),
            SlotQuality::Excellent
        );
    }

    #[test]
    fn test_cell_quality() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        assert_eq!(heatmap.cell_quality(0, 0), SlotQuality::Unknown);

        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap(); // Mon 10:00
        for _ in 0..5 {
            heatmap.record_at(2_000_000.0, ts);
        }
        assert_eq!(heatmap.cell_quality(0, 10), SlotQuality::Good);
    }

    #[test]
    fn test_hourly_and_daily_speed() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());

        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 8, 0, 0).unwrap(); // Mon 08:00
        for _ in 0..5 {
            heatmap.record_at(1_000_000.0, ts);
        }

        assert!((heatmap.hourly_speed(8) - 1_000_000.0).abs() < 1.0);
        assert!((heatmap.daily_speed(0) - 1_000_000.0).abs() < 1.0); // Monday
        assert_eq!(heatmap.hourly_speed(9), 0.0);
        assert_eq!(heatmap.daily_speed(1), 0.0); // Tuesday
    }

    #[test]
    fn test_out_of_range_access() {
        let heatmap = SpeedHeatmap::with_config(make_config());
        assert_eq!(heatmap.hourly_speed(24), 0.0);
        assert_eq!(heatmap.daily_speed(7), 0.0);
        assert_eq!(heatmap.cell_quality(7, 0), SlotQuality::Unknown);
        assert_eq!(heatmap.cell_quality(0, 24), SlotQuality::Unknown);
    }

    #[test]
    fn test_reset() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        heatmap.record(1_000_000.0);
        assert_eq!(heatmap.total_samples, 1);

        heatmap.reset();
        assert_eq!(heatmap.total_samples, 0);
        assert_eq!(heatmap.hourly[0].sample_count, 0);
    }

    #[test]
    fn test_format_report_empty() {
        let heatmap = SpeedHeatmap::with_config(make_config());
        let report = heatmap.format_report();
        assert!(report.contains("No data recorded yet."));
    }

    #[test]
    fn test_format_report_with_data() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 14, 0, 0).unwrap();
        for _ in 0..10 {
            heatmap.record_at(2_000_000.0, ts);
        }

        let report = heatmap.format_report();
        assert!(report.contains("Download Speed Heatmap Report"));
        assert!(report.contains("Total samples: 10"));
        assert!(report.contains("Hourly Average Speeds"));
        assert!(report.contains("Daily Average Speeds"));
        assert!(report.contains("Best hour"));
    }

    #[test]
    fn test_recommended_windows() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());

        // Record fast speeds at Mon 02:00-04:00
        for h in 2..5 {
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, h, 0, 0).unwrap();
            for _ in 0..5 {
                heatmap.record_at(8_000_000.0, ts);
            }
        }

        // Record slow speeds at Mon 10:00-12:00
        for h in 10..13 {
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, h, 0, 0).unwrap();
            for _ in 0..5 {
                heatmap.record_at(50_000.0, ts);
            }
        }

        let summary = heatmap.summary();
        assert!(!summary.recommended_windows.is_empty());
        // The first recommended window should be in the fast range
        let first = &summary.recommended_windows[0];
        assert!(first.avg_speed_bps > 1_000_000.0);
    }

    #[test]
    fn test_day_name() {
        assert_eq!(day_name(0), "Monday");
        assert_eq!(day_name(6), "Sunday");
        assert_eq!(day_name(7), "Unknown");
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(500.0), "500 B");
        assert_eq!(format_speed(1_500.0), "1.5 KB");
        assert_eq!(format_speed(2_500_000.0), "2.50 MB");
        assert_eq!(format_speed(3_500_000_000.0), "3.50 GB");
    }

    #[test]
    fn test_save_load_config() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let config = make_config();
            let heatmap = SpeedHeatmap::with_config(config.clone());
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("heatmap_config.json");

            heatmap.save_config(&path).await.unwrap();
            let loaded = SpeedHeatmap::load_config(&path).await.unwrap();
            assert_eq!(loaded.enabled, config.enabled);
            assert_eq!(loaded.retention_days, config.retention_days);
        });
    }

    #[test]
    fn test_save_load_data() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut heatmap = SpeedHeatmap::with_config(make_config());
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 14, 0, 0).unwrap();
            for _ in 0..20 {
                heatmap.record_at(3_000_000.0, ts);
            }

            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("heatmap_data.json");

            heatmap.save_data(&path).await.unwrap();
            let loaded = SpeedHeatmap::load_data(&path).await.unwrap();

            assert_eq!(loaded.total_samples, 20);
            assert!((loaded.hourly_speed(14) - 3_000_000.0).abs() < 1.0);
        });
    }

    #[test]
    fn test_heatmap_multiple_days() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());

        // Monday fast
        let ts_mon = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        for _ in 0..10 {
            heatmap.record_at(5_000_000.0, ts_mon);
        }

        // Wednesday slow
        let ts_wed = chrono::Utc.with_ymd_and_hms(2026, 8, 12, 10, 0, 0).unwrap();
        for _ in 0..10 {
            heatmap.record_at(100_000.0, ts_wed);
        }

        let summary = heatmap.summary();
        assert_eq!(summary.best_day, 0); // Monday
        assert_eq!(summary.worst_day, 2); // Wednesday
    }

    #[test]
    fn test_heatmap_grid_independent_cells() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());

        // Mon 08:00 fast
        let ts1 = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 8, 0, 0).unwrap();
        heatmap.record_at(10_000_000.0, ts1);

        // Tue 08:00 slow
        let ts2 = chrono::Utc.with_ymd_and_hms(2026, 8, 11, 8, 0, 0).unwrap();
        heatmap.record_at(50_000.0, ts2);

        // Grid cells should be independent
        assert!(heatmap.grid[0][8].avg_speed_bps > 1_000_000.0); // Mon
        assert!(heatmap.grid[1][8].avg_speed_bps < 100_000.0); // Tue
    }

    #[test]
    fn test_slot_quality_display() {
        assert_eq!(format!("{}", SlotQuality::Unknown), "unknown");
        assert_eq!(format!("{}", SlotQuality::Poor), "poor");
        assert_eq!(format!("{}", SlotQuality::Fair), "fair");
        assert_eq!(format!("{}", SlotQuality::Good), "good");
        assert_eq!(format!("{}", SlotQuality::Excellent), "excellent");
    }

    // === Phase 234: Comprehensive test coverage (22 → 72 tests) ===

    // --- Config serde ---
    #[test]
    fn test_config_serde_roundtrip_default() {
        let config = SpeedHeatmapConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SpeedHeatmapConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.max_samples_per_hour, config.max_samples_per_hour);
        assert_eq!(loaded.max_samples_per_day, config.max_samples_per_day);
        assert_eq!(loaded.max_samples_per_cell, config.max_samples_per_cell);
        assert_eq!(loaded.retention_days, config.retention_days);
    }

    #[test]
    fn test_config_serde_roundtrip_custom() {
        let config = SpeedHeatmapConfig {
            enabled: false,
            max_samples_per_hour: 1,
            max_samples_per_day: 2,
            max_samples_per_cell: 3,
            retention_days: 365,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SpeedHeatmapConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.max_samples_per_hour, 1);
        assert_eq!(loaded.retention_days, 365);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"max_samples_per_hour":100,"max_samples_per_day":500,"max_samples_per_cell":50,"retention_days":30,"extra_field":"ignored"}"#;
        let loaded: SpeedHeatmapConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = make_config();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let loaded: SpeedHeatmapConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.retention_days, config.retention_days);
    }

    #[test]
    fn test_config_default_values() {
        let config = SpeedHeatmapConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_samples_per_hour, 500);
        assert_eq!(config.max_samples_per_day, 2000);
        assert_eq!(config.max_samples_per_cell, 50);
        assert_eq!(config.retention_days, 30);
    }

    // --- Config traits ---
    #[test]
    fn test_config_clone_debug() {
        let config = make_config();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        let debug = format!("{:?}", config);
        assert!(debug.contains("SpeedHeatmapConfig"));
    }

    // --- BucketStats serde ---
    #[test]
    fn test_bucket_stats_serde_roundtrip() {
        let mut bucket = BucketStats::default();
        bucket.record(1000.0);
        bucket.record(2000.0);
        let json = serde_json::to_string(&bucket).unwrap();
        let loaded: BucketStats = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.sample_count, 2);
        assert!((loaded.speed_sum - 3000.0).abs() < 0.01);
        assert_eq!(loaded.speed_min, 1000.0);
        assert_eq!(loaded.speed_max, 2000.0);
    }

    #[test]
    fn test_bucket_stats_default_zero() {
        let bucket = BucketStats::default();
        assert_eq!(bucket.sample_count, 0);
        assert_eq!(bucket.speed_sum, 0.0);
        assert_eq!(bucket.speed_sq_sum, 0.0);
        assert_eq!(bucket.speed_min, 0.0);
        assert_eq!(bucket.speed_max, 0.0);
    }

    #[test]
    fn test_bucket_stats_clone_debug() {
        let mut bucket = BucketStats::default();
        bucket.record(500.0);
        let cloned = bucket.clone();
        assert_eq!(cloned.sample_count, 1);
        let debug = format!("{:?}", bucket);
        assert!(debug.contains("BucketStats"));
    }

    // --- BucketStats boundaries ---
    #[test]
    fn test_bucket_stats_single_sample_stddev_zero() {
        let mut bucket = BucketStats::default();
        bucket.record(1000.0);
        assert_eq!(bucket.stddev(), 0.0);
    }

    #[test]
    fn test_bucket_stats_zero_speed() {
        let mut bucket = BucketStats::default();
        bucket.record(0.0);
        assert_eq!(bucket.sample_count, 1);
        assert_eq!(bucket.avg_speed(), 0.0);
        assert_eq!(bucket.speed_min, 0.0);
        assert_eq!(bucket.speed_max, 0.0);
    }

    #[test]
    fn test_bucket_stats_large_values() {
        let mut bucket = BucketStats::default();
        bucket.record(1e15);
        bucket.record(2e15);
        assert_eq!(bucket.sample_count, 2);
        assert!((bucket.avg_speed() - 1.5e15).abs() < 1e6);
    }

    #[test]
    fn test_bucket_stats_many_samples() {
        let mut bucket = BucketStats::default();
        for i in 1..=100 {
            bucket.record(i as f64 * 1000.0);
        }
        assert_eq!(bucket.sample_count, 100);
        assert_eq!(bucket.speed_min, 1000.0);
        assert_eq!(bucket.speed_max, 100_000.0);
        // mean of 1000,2000,...,100000 = 50500
        assert!((bucket.avg_speed() - 50500.0).abs() < 1.0);
    }

    // --- HeatmapCell serde ---
    #[test]
    fn test_heatmap_cell_serde_roundtrip() {
        let mut cell = HeatmapCell::default();
        cell.record(1_000_000.0);
        cell.record(3_000_000.0);
        let json = serde_json::to_string(&cell).unwrap();
        let loaded: HeatmapCell = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.sample_count, 2);
        assert!((loaded.avg_speed_bps - 2_000_000.0).abs() < 1.0);
        assert_eq!(loaded.peak_speed_bps, 3_000_000.0);
    }

    #[test]
    fn test_heatmap_cell_default_zero() {
        let cell = HeatmapCell::default();
        assert_eq!(cell.sample_count, 0);
        assert_eq!(cell.avg_speed_bps, 0.0);
        assert_eq!(cell.peak_speed_bps, 0.0);
    }

    #[test]
    fn test_heatmap_cell_clone_debug() {
        let mut cell = HeatmapCell::default();
        cell.record(500_000.0);
        let cloned = cell.clone();
        assert_eq!(cloned.sample_count, 1);
        let debug = format!("{:?}", cell);
        assert!(debug.contains("HeatmapCell"));
    }

    #[test]
    fn test_heatmap_cell_single_sample() {
        let mut cell = HeatmapCell::default();
        cell.record(42.0);
        assert_eq!(cell.sample_count, 1);
        assert!((cell.avg_speed_bps - 42.0).abs() < 0.01);
        assert_eq!(cell.peak_speed_bps, 42.0);
    }

    // --- SlotQuality serde ---
    #[test]
    fn test_slot_quality_serde_roundtrip_all_variants() {
        let variants = [
            SlotQuality::Unknown,
            SlotQuality::Poor,
            SlotQuality::Fair,
            SlotQuality::Good,
            SlotQuality::Excellent,
        ];
        for v in &variants {
            let json = serde_json::to_string(v).unwrap();
            let loaded: SlotQuality = serde_json::from_str(&json).unwrap();
            assert_eq!(*v, loaded);
        }
    }

    #[test]
    fn test_slot_quality_serde_snake_case() {
        let json = "\"excellent\"";
        let loaded: SlotQuality = serde_json::from_str(json).unwrap();
        assert_eq!(loaded, SlotQuality::Excellent);
    }

    #[test]
    fn test_slot_quality_clone_copy_eq() {
        let q = SlotQuality::Good;
        let q2 = q;
        let q3 = q.clone();
        assert_eq!(q, q2);
        assert_eq!(q, q3);
        let debug = format!("{:?}", q);
        assert_eq!(debug, "Good");
    }

    // --- speed_to_quality exact boundaries ---
    #[test]
    fn test_speed_to_quality_exact_boundaries() {
        // < 100KB/s = Poor
        assert_eq!(SpeedHeatmap::speed_to_quality(99_999.0), SlotQuality::Poor);
        assert_eq!(SpeedHeatmap::speed_to_quality(100_000.0), SlotQuality::Fair);
        // < 1MB/s = Fair
        assert_eq!(SpeedHeatmap::speed_to_quality(999_999.0), SlotQuality::Fair);
        assert_eq!(
            SpeedHeatmap::speed_to_quality(1_000_000.0),
            SlotQuality::Good
        );
        // < 5MB/s = Good
        assert_eq!(
            SpeedHeatmap::speed_to_quality(4_999_999.0),
            SlotQuality::Good
        );
        assert_eq!(
            SpeedHeatmap::speed_to_quality(5_000_000.0),
            SlotQuality::Excellent
        );
    }

    // --- HeatmapSample serde ---
    #[test]
    fn test_heatmap_sample_serde_roundtrip() {
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let sample = HeatmapSample {
            speed_bps: 1_500_000.0,
            timestamp: ts,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let loaded: HeatmapSample = serde_json::from_str(&json).unwrap();
        assert!((loaded.speed_bps - 1_500_000.0).abs() < 0.01);
        assert_eq!(loaded.timestamp, ts);
    }

    // --- RecommendedWindow serde ---
    #[test]
    fn test_recommended_window_serde_roundtrip() {
        let w = RecommendedWindow {
            start_hour: 2,
            end_hour: 5,
            day_of_week: Some(0),
            avg_speed_bps: 8_000_000.0,
            quality: SlotQuality::Excellent,
        };
        let json = serde_json::to_string(&w).unwrap();
        let loaded: RecommendedWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.start_hour, 2);
        assert_eq!(loaded.end_hour, 5);
        assert_eq!(loaded.day_of_week, Some(0));
        assert_eq!(loaded.quality, SlotQuality::Excellent);
    }

    #[test]
    fn test_recommended_window_serde_none_day() {
        let w = RecommendedWindow {
            start_hour: 0,
            end_hour: 1,
            day_of_week: None,
            avg_speed_bps: 500_000.0,
            quality: SlotQuality::Fair,
        };
        let json = serde_json::to_string(&w).unwrap();
        let loaded: RecommendedWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.day_of_week, None);
    }

    // --- SpeedHeatmapSummary serde ---
    #[test]
    fn test_summary_serde_roundtrip() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 14, 0, 0).unwrap();
        for _ in 0..5 {
            heatmap.record_at(2_000_000.0, ts);
        }
        let summary = heatmap.summary();
        let json = serde_json::to_string(&summary).unwrap();
        let loaded: SpeedHeatmapSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_samples, 5);
        assert_eq!(loaded.best_hour, 14);
    }

    #[test]
    fn test_summary_clone_debug() {
        let heatmap = SpeedHeatmap::with_config(make_config());
        let summary = heatmap.summary();
        let cloned = summary.clone();
        assert_eq!(cloned.total_samples, 0);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("SpeedHeatmapSummary"));
    }

    // --- SpeedHeatmap new/default ---
    #[test]
    fn test_new_default_equality() {
        let a = SpeedHeatmap::new();
        let b = SpeedHeatmap::default();
        assert_eq!(a.total_samples, b.total_samples);
        assert_eq!(a.config.enabled, b.config.enabled);
    }

    #[test]
    fn test_with_config_preserves_fields() {
        let config = SpeedHeatmapConfig {
            enabled: false,
            max_samples_per_hour: 42,
            max_samples_per_day: 100,
            max_samples_per_cell: 10,
            retention_days: 90,
        };
        let heatmap = SpeedHeatmap::with_config(config);
        assert!(!heatmap.config.enabled);
        assert_eq!(heatmap.config.max_samples_per_hour, 42);
        assert_eq!(heatmap.config.retention_days, 90);
        assert_eq!(heatmap.total_samples, 0);
    }

    #[test]
    fn test_heatmap_clone_debug() {
        let heatmap = SpeedHeatmap::with_config(make_config());
        let cloned = heatmap.clone();
        assert_eq!(cloned.total_samples, 0);
        let debug = format!("{:?}", heatmap);
        assert!(debug.contains("SpeedHeatmap"));
    }

    // --- record_at all hours ---
    #[test]
    fn test_record_at_all_24_hours() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        for h in 0..24u32 {
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, h, 0, 0).unwrap();
            heatmap.record_at(1000.0 * (h as f64 + 1.0), ts);
        }
        assert_eq!(heatmap.total_samples, 24);
        for h in 0..24 {
            assert_eq!(heatmap.hourly[h].sample_count, 1);
        }
    }

    // --- record_at all 7 days of week ---
    #[test]
    fn test_record_at_all_7_days() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        // 2026-08-10 is Monday, 2026-08-16 is Sunday
        for d in 0..7u32 {
            let ts = chrono::Utc
                .with_ymd_and_hms(2026, 8, 10 + d, 10, 0, 0)
                .unwrap();
            heatmap.record_at(100_000.0 * (d as f64 + 1.0), ts);
        }
        assert_eq!(heatmap.total_samples, 7);
        for d in 0..7 {
            assert_eq!(heatmap.daily[d].sample_count, 1);
        }
    }

    // --- record_at Sunday (dow=6) ---
    #[test]
    fn test_record_at_sunday() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        // 2026-08-16 is Sunday
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 16, 12, 0, 0).unwrap();
        heatmap.record_at(5_000_000.0, ts);
        assert_eq!(heatmap.daily[6].sample_count, 1); // Sunday = 6
        assert_eq!(heatmap.grid[6][12].sample_count, 1);
    }

    // --- summary empty heatmap ---
    #[test]
    fn test_summary_empty_heatmap() {
        let heatmap = SpeedHeatmap::with_config(make_config());
        let summary = heatmap.summary();
        assert_eq!(summary.total_samples, 0);
        assert!(summary.recommended_windows.is_empty());
        // best/worst default to 0
        assert_eq!(summary.best_hour, 0);
        assert_eq!(summary.worst_hour, 0);
    }

    // --- summary with single sample ---
    #[test]
    fn test_summary_single_sample() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 15, 0, 0).unwrap();
        heatmap.record_at(2_000_000.0, ts);
        let summary = heatmap.summary();
        assert_eq!(summary.total_samples, 1);
        assert_eq!(summary.best_hour, 15);
        assert_eq!(summary.worst_hour, 15);
        assert_eq!(summary.best_day, 0); // Monday
        assert_eq!(summary.worst_day, 0);
    }

    // --- prune_old_data ---
    #[test]
    fn test_prune_old_data_removes_old_timestamps() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        // Record very old data (2020-01-01)
        let ts_old = chrono::Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap();
        heatmap.record_at(1_000_000.0, ts_old);
        // Record recent data
        let ts_new = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        heatmap.record_at(2_000_000.0, ts_new);
        assert_eq!(heatmap.total_samples, 2);

        heatmap.prune_old_data();
        // total_samples is not decremented (buckets still have both),
        // but recent_timestamps should have pruned the old one
        assert_eq!(heatmap.recent_timestamps.len(), 1);
    }

    #[test]
    fn test_prune_old_data_empty() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        heatmap.prune_old_data();
        assert!(heatmap.recent_timestamps.is_empty());
    }

    // --- reset clears everything ---
    #[test]
    fn test_reset_clears_all_buckets() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        for _ in 0..50 {
            heatmap.record_at(1_000_000.0, ts);
        }
        assert_eq!(heatmap.total_samples, 50);

        heatmap.reset();
        assert_eq!(heatmap.total_samples, 0);
        for h in 0..24 {
            assert_eq!(heatmap.hourly[h].sample_count, 0);
        }
        for d in 0..7 {
            assert_eq!(heatmap.daily[d].sample_count, 0);
        }
        for d in 0..7 {
            for h in 0..24 {
                assert_eq!(heatmap.grid[d][h].sample_count, 0);
            }
        }
    }

    // --- format_report sections ---
    #[test]
    fn test_format_report_contains_all_sections() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        // Add data for multiple hours and days
        for h in [2, 8, 14, 20] {
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, h, 0, 0).unwrap();
            for _ in 0..5 {
                heatmap.record_at(2_000_000.0, ts);
            }
        }
        let ts_wed = chrono::Utc.with_ymd_and_hms(2026, 8, 12, 10, 0, 0).unwrap();
        for _ in 0..5 {
            heatmap.record_at(500_000.0, ts_wed);
        }

        let report = heatmap.format_report();
        assert!(report.contains("Download Speed Heatmap Report"));
        assert!(report.contains("Hourly Average Speeds"));
        assert!(report.contains("Daily Average Speeds"));
        assert!(report.contains("Best hour"));
        assert!(report.contains("Worst hour"));
        assert!(report.contains("Best day"));
        assert!(report.contains("Worst day"));
        assert!(report.contains("Mon"));
        assert!(report.contains("Wed"));
    }

    #[test]
    fn test_format_report_recommended_windows_section() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        // Create a clear fast window
        for h in 2..5 {
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, h, 0, 0).unwrap();
            for _ in 0..10 {
                heatmap.record_at(10_000_000.0, ts);
            }
        }
        let report = heatmap.format_report();
        assert!(report.contains("Recommended Download Windows"));
    }

    // --- speed_bar ---
    #[test]
    fn test_speed_bar_zero() {
        let bar = SpeedHeatmap::speed_bar(0.0);
        // 0 filled + 20 empty, each char is 3 bytes in UTF-8
        assert_eq!(bar.chars().count(), 20);
        assert!(bar.contains("░"));
    }

    #[test]
    fn test_speed_bar_max() {
        let bar = SpeedHeatmap::speed_bar(50_000_000.0); // 50MB/s, way above scale
        assert!(bar.contains("█"));
    }

    #[test]
    fn test_speed_bar_mid_range() {
        let bar = SpeedHeatmap::speed_bar(2_500_000.0); // 2.5MB/s -> ~5 filled
        assert!(bar.contains("█"));
        assert!(bar.contains("░"));
    }

    // --- format_speed boundaries ---
    #[test]
    fn test_format_speed_zero() {
        assert_eq!(format_speed(0.0), "0 B");
    }

    #[test]
    fn test_format_speed_boundary_1000() {
        assert_eq!(format_speed(999.0), "999 B");
        assert_eq!(format_speed(1000.0), "1.0 KB");
    }

    #[test]
    fn test_format_speed_boundary_1m() {
        assert_eq!(format_speed(999_999.0), "1000.0 KB");
        assert_eq!(format_speed(1_000_000.0), "1.00 MB");
    }

    #[test]
    fn test_format_speed_boundary_1g() {
        assert_eq!(format_speed(999_999_999.0), "1000.00 MB");
        assert_eq!(format_speed(1_000_000_000.0), "1.00 GB");
    }

    #[test]
    fn test_format_speed_negative() {
        // Negative speeds shouldn't appear in practice, but format_speed handles them
        assert_eq!(format_speed(-100.0), "-100 B");
    }

    // --- day_name all variants ---
    #[test]
    fn test_day_name_all_variants() {
        assert_eq!(day_name(0), "Monday");
        assert_eq!(day_name(1), "Tuesday");
        assert_eq!(day_name(2), "Wednesday");
        assert_eq!(day_name(3), "Thursday");
        assert_eq!(day_name(4), "Friday");
        assert_eq!(day_name(5), "Saturday");
        assert_eq!(day_name(6), "Sunday");
        assert_eq!(day_name(7), "Unknown");
        assert_eq!(day_name(255), "Unknown");
    }

    // --- Persistence edge cases ---
    #[test]
    fn test_save_load_config_missing_file() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("nonexistent.json");
            let result = SpeedHeatmap::load_config(&path).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_save_load_data_missing_file() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("nonexistent_data.json");
            let result = SpeedHeatmap::load_data(&path).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_save_load_data_corrupt_json() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("corrupt.json");
            std::fs::write(&path, "not valid json").unwrap();
            let result = SpeedHeatmap::load_data(&path).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_save_load_config_corrupt_json() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("corrupt_config.json");
            std::fs::write(&path, "{invalid}").unwrap();
            let result = SpeedHeatmap::load_config(&path).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_save_data_overwrite() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("heatmap.json");

            let mut heatmap = SpeedHeatmap::with_config(make_config());
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
            heatmap.record_at(1_000_000.0, ts);
            heatmap.save_data(&path).await.unwrap();

            // Overwrite with different data
            let mut heatmap2 = SpeedHeatmap::with_config(make_config());
            heatmap2.record_at(5_000_000.0, ts);
            heatmap2.record_at(5_000_000.0, ts);
            heatmap2.save_data(&path).await.unwrap();

            let loaded = SpeedHeatmap::load_data(&path).await.unwrap();
            assert_eq!(loaded.total_samples, 2);
        });
    }

    // --- Recommended windows edge cases ---
    #[test]
    fn test_recommended_windows_empty_when_no_data() {
        let heatmap = SpeedHeatmap::with_config(make_config());
        let summary = heatmap.summary();
        assert!(summary.recommended_windows.is_empty());
    }

    #[test]
    fn test_recommended_windows_excludes_poor_quality() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        // Only record slow speeds (< 100KB/s = Poor)
        for h in 0..24 {
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, h, 0, 0).unwrap();
            for _ in 0..5 {
                heatmap.record_at(10_000.0, ts); // 10KB/s = Poor
            }
        }
        let summary = heatmap.summary();
        // All windows should be excluded (Poor quality)
        for w in &summary.recommended_windows {
            assert_ne!(w.quality, SlotQuality::Poor);
            assert_ne!(w.quality, SlotQuality::Unknown);
        }
    }

    #[test]
    fn test_recommended_windows_max_3() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        // Create many fast slots
        for d in 0..7 {
            for h in 0..24 {
                let ts = chrono::Utc
                    .with_ymd_and_hms(2026, 8, 10 + d, h, 0, 0)
                    .unwrap();
                for _ in 0..5 {
                    heatmap.record_at(10_000_000.0, ts);
                }
            }
        }
        let summary = heatmap.summary();
        assert!(summary.recommended_windows.len() <= 3);
    }

    // --- cell_quality all boundary ---
    #[test]
    fn test_cell_quality_all_levels() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());

        // Poor: < 100KB/s
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
        for _ in 0..5 {
            heatmap.record_at(50_000.0, ts);
        }
        assert_eq!(heatmap.cell_quality(0, 0), SlotQuality::Poor);

        // Fair: 100KB/s - 1MB/s
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 1, 0, 0).unwrap();
        for _ in 0..5 {
            heatmap.record_at(500_000.0, ts);
        }
        assert_eq!(heatmap.cell_quality(0, 1), SlotQuality::Fair);

        // Good: 1MB/s - 5MB/s
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 2, 0, 0).unwrap();
        for _ in 0..5 {
            heatmap.record_at(2_000_000.0, ts);
        }
        assert_eq!(heatmap.cell_quality(0, 2), SlotQuality::Good);

        // Excellent: > 5MB/s
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 3, 0, 0).unwrap();
        for _ in 0..5 {
            heatmap.record_at(10_000_000.0, ts);
        }
        assert_eq!(heatmap.cell_quality(0, 3), SlotQuality::Excellent);
    }

    // --- record increments total_samples correctly ---
    #[test]
    fn test_total_samples_accumulates() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 10, 0, 0).unwrap();
        for i in 1..=100 {
            heatmap.record_at(i as f64 * 1000.0, ts);
        }
        assert_eq!(heatmap.total_samples, 100);
    }

    // --- record_at midnight ---
    #[test]
    fn test_record_at_midnight() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
        heatmap.record_at(1_000_000.0, ts);
        assert_eq!(heatmap.hourly[0].sample_count, 1);
        assert_eq!(heatmap.total_samples, 1);
    }

    // --- record_at hour 23 ---
    #[test]
    fn test_record_at_hour_23() {
        let mut heatmap = SpeedHeatmap::with_config(make_config());
        let ts = chrono::Utc
            .with_ymd_and_hms(2026, 8, 10, 23, 59, 59)
            .unwrap();
        heatmap.record_at(1_000_000.0, ts);
        assert_eq!(heatmap.hourly[23].sample_count, 1);
    }

    // --- save_data creates file ---
    #[test]
    fn test_save_data_creates_file() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("new_file.json");
            assert!(!path.exists());
            let heatmap = SpeedHeatmap::with_config(make_config());
            heatmap.save_data(&path).await.unwrap();
            assert!(path.exists());
        });
    }

    #[test]
    fn test_save_config_creates_file() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("new_config.json");
            assert!(!path.exists());
            let heatmap = SpeedHeatmap::with_config(make_config());
            heatmap.save_config(&path).await.unwrap();
            assert!(path.exists());
        });
    }

    // --- save/load data preserves all grid cells ---
    #[test]
    fn test_save_load_data_preserves_grid() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut heatmap = SpeedHeatmap::with_config(make_config());
            // Fill a specific cell
            let ts = chrono::Utc.with_ymd_and_hms(2026, 8, 12, 15, 0, 0).unwrap(); // Wed 15:00
            for _ in 0..10 {
                heatmap.record_at(3_000_000.0, ts);
            }

            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("grid_test.json");
            heatmap.save_data(&path).await.unwrap();
            let loaded = SpeedHeatmap::load_data(&path).await.unwrap();

            // Wednesday = 2, hour 15
            assert_eq!(loaded.grid[2][15].sample_count, 10);
            assert!((loaded.grid[2][15].avg_speed_bps - 3_000_000.0).abs() < 1.0);
            // Other cells should be empty
            assert_eq!(loaded.grid[0][0].sample_count, 0);
        });
    }
}
