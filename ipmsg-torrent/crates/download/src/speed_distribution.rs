//! Download Speed Distribution Analysis System
//!
//! Tracks download speed distributions across multiple dimensions:
//! - Per-domain speed statistics (mean, median, p95, p99, min, max, stddev)
//! - Per-protocol speed statistics (HTTP/Torrent/Ed2k/P2P)
//! - Hourly speed distribution (24 buckets)
//! - Speed bucket histogram (categorizes speeds into ranges)
//! - Percentile calculations for capacity planning
//! - Anomaly detection based on distribution deviation

use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;

/// Number of speed histogram buckets
const NUM_HISTOGRAM_BUCKETS: usize = 10;

/// Default maximum samples per domain
const DEFAULT_MAX_DOMAIN_SAMPLES: usize = 1000;

/// Default maximum samples per hour bucket
const DEFAULT_MAX_HOURLY_SAMPLES: usize = 500;

/// Speed bucket boundaries (bytes per second) for histogram
const SPEED_BUCKET_BOUNDARIES: [u64; NUM_HISTOGRAM_BUCKETS + 1] = [
    0,
    10 * 1024,         // 10 KB/s
    50 * 1024,         // 50 KB/s
    100 * 1024,        // 100 KB/s
    500 * 1024,        // 500 KB/s
    1024 * 1024,       // 1 MB/s
    5 * 1024 * 1024,   // 5 MB/s
    10 * 1024 * 1024,  // 10 MB/s
    50 * 1024 * 1024,  // 50 MB/s
    100 * 1024 * 1024, // 100 MB/s
    u64::MAX,
];

/// Labels for speed histogram buckets
const SPEED_BUCKET_LABELS: [&str; NUM_HISTOGRAM_BUCKETS] = [
    "<10KB/s",
    "10-50KB/s",
    "50-100KB/s",
    "100-500KB/s",
    "500KB-1MB/s",
    "1-5MB/s",
    "5-10MB/s",
    "10-50MB/s",
    "50-100MB/s",
    ">100MB/s",
];

/// Protocol classification for speed tracking
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeedProtocol {
    #[default]
    Http,
    Torrent,
    Ed2k,
    P2p,
    Unknown,
}

impl std::fmt::Display for SpeedProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpeedProtocol::Http => write!(f, "HTTP"),
            SpeedProtocol::Torrent => write!(f, "Torrent"),
            SpeedProtocol::Ed2k => write!(f, "Ed2k"),
            SpeedProtocol::P2p => write!(f, "P2P"),
            SpeedProtocol::Unknown => write!(f, "Unknown"),
        }
    }
}

impl SpeedProtocol {
    /// Parse protocol from a protocol string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "http" | "https" | "ftp" => SpeedProtocol::Http,
            "torrent" | "bittorrent" | "bt" => SpeedProtocol::Torrent,
            "ed2k" | "edonkey" => SpeedProtocol::Ed2k,
            "p2p" => SpeedProtocol::P2p,
            _ => SpeedProtocol::Unknown,
        }
    }
}

/// Statistical summary for a set of speed samples
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpeedStats {
    /// Number of samples recorded
    pub sample_count: usize,
    /// Minimum speed observed (bytes/sec)
    pub min_bps: f64,
    /// Maximum speed observed (bytes/sec)
    pub max_bps: f64,
    /// Mean speed (bytes/sec)
    pub mean_bps: f64,
    /// Median speed (bytes/sec)
    pub median_bps: f64,
    /// 95th percentile speed (bytes/sec)
    pub p95_bps: f64,
    /// 99th percentile speed (bytes/sec)
    pub p99_bps: f64,
    /// Standard deviation of speed
    pub stddev_bps: f64,
    /// Coefficient of variation (stddev/mean, lower = more stable)
    pub stability: f64,
    /// Total bytes transferred across all samples
    pub total_bytes: u64,
}

impl SpeedStats {
    /// Compute stats from a sorted list of speed values
    fn from_sorted_speedes(speeds: &[f64], total_bytes: u64) -> Self {
        if speeds.is_empty() {
            return Self::default();
        }
        let n = speeds.len();
        let sum: f64 = speeds.iter().sum();
        let mean = sum / n as f64;

        let variance = if n > 1 {
            speeds.iter().map(|s| (*s - mean).powi(2)).sum::<f64>() / (n - 1) as f64
        } else {
            0.0
        };
        let stddev = variance.sqrt();
        let stability = if mean > 0.0 {
            (1.0 - (stddev / mean).min(1.0)).max(0.0)
        } else {
            0.0
        };

        Self {
            sample_count: n,
            min_bps: speeds[0],
            max_bps: speeds[n - 1],
            mean_bps: mean,
            median_bps: percentile_from_sorted(speeds, 0.50),
            p95_bps: percentile_from_sorted(speeds, 0.95),
            p99_bps: percentile_from_sorted(speeds, 0.99),
            stddev_bps: stddev,
            stability,
            total_bytes,
        }
    }

    /// Format mean speed as human-readable string
    pub fn format_mean(&self) -> String {
        format_speed_bps(self.mean_bps)
    }

    /// Format median speed as human-readable string
    pub fn format_median(&self) -> String {
        format_speed_bps(self.median_bps)
    }
}

/// Extract a percentile value from a sorted slice
fn percentile_from_sorted(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let idx = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Per-domain speed tracking data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSpeedData {
    /// Domain name (normalized, lowercase)
    pub domain: String,
    /// All speed samples (bytes/sec), kept sorted for percentile calc
    pub speeds: Vec<f64>,
    /// Maximum samples to retain
    pub max_samples: usize,
    /// Total bytes downloaded from this domain
    pub total_bytes: u64,
    /// Number of downloads from this domain
    pub download_count: u32,
    /// First time we recorded speed for this domain
    pub first_seen: DateTime<Utc>,
    /// Last time we recorded speed for this domain
    pub last_seen: DateTime<Utc>,
}

impl DomainSpeedData {
    fn new(domain: String, max_samples: usize) -> Self {
        Self {
            domain,
            speeds: Vec::new(),
            max_samples,
            total_bytes: 0,
            download_count: 0,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
        }
    }

    /// Record a speed sample for this domain
    fn record_speed(&mut self, speed_bps: f64, bytes: u64) {
        // Insert in sorted position
        let pos = self.speeds.partition_point(|&x| x < speed_bps);
        self.speeds.insert(pos, speed_bps);

        // Trim oldest if over limit
        if self.speeds.len() > self.max_samples {
            // Remove from the front (oldest/smallest is not necessarily oldest,
            // but we trim from front to keep recent distribution)
            self.speeds.remove(0);
        }

        self.total_bytes += bytes;
        self.download_count += 1;
        self.last_seen = Utc::now();
    }

    /// Compute statistics for this domain
    fn stats(&self) -> SpeedStats {
        SpeedStats::from_sorted_speedes(&self.speeds, self.total_bytes)
    }
}

/// Per-protocol speed tracking data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolSpeedData {
    /// Protocol type
    pub protocol: SpeedProtocol,
    /// All speed samples (bytes/sec), kept sorted
    pub speeds: Vec<f64>,
    /// Maximum samples to retain
    pub max_samples: usize,
    /// Total bytes downloaded via this protocol
    pub total_bytes: u64,
    /// Number of downloads via this protocol
    pub download_count: u32,
}

impl ProtocolSpeedData {
    fn new(protocol: SpeedProtocol, max_samples: usize) -> Self {
        Self {
            protocol,
            speeds: Vec::new(),
            max_samples,
            total_bytes: 0,
            download_count: 0,
        }
    }

    fn record_speed(&mut self, speed_bps: f64, bytes: u64) {
        let pos = self.speeds.partition_point(|&x| x < speed_bps);
        self.speeds.insert(pos, speed_bps);
        if self.speeds.len() > self.max_samples {
            self.speeds.remove(0);
        }
        self.total_bytes += bytes;
        self.download_count += 1;
    }

    fn stats(&self) -> SpeedStats {
        SpeedStats::from_sorted_speedes(&self.speeds, self.total_bytes)
    }
}

/// Hourly speed distribution bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlySpeedBucket {
    /// Hour of day (0-23)
    pub hour: u8,
    /// Speed samples for this hour, kept sorted
    pub speeds: Vec<f64>,
    /// Maximum samples to retain
    pub max_samples: usize,
    /// Total bytes downloaded during this hour
    pub total_bytes: u64,
    /// Number of samples recorded
    pub sample_count: usize,
}

impl HourlySpeedBucket {
    fn new(hour: u8, max_samples: usize) -> Self {
        Self {
            hour,
            speeds: Vec::new(),
            max_samples,
            total_bytes: 0,
            sample_count: 0,
        }
    }

    fn record_speed(&mut self, speed_bps: f64, bytes: u64) {
        let pos = self.speeds.partition_point(|&x| x < speed_bps);
        self.speeds.insert(pos, speed_bps);
        if self.speeds.len() > self.max_samples {
            self.speeds.remove(0);
        }
        self.total_bytes += bytes;
        self.sample_count += 1;
    }

    fn stats(&self) -> SpeedStats {
        SpeedStats::from_sorted_speedes(&self.speeds, self.total_bytes)
    }
}

/// Speed histogram bucket counts
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpeedHistogram {
    /// Count of samples in each bucket
    pub bucket_counts: [u64; NUM_HISTOGRAM_BUCKETS],
    /// Total samples recorded
    pub total_samples: u64,
}

impl SpeedHistogram {
    fn record_speed(&mut self, speed_bps: f64) {
        let speed = speed_bps as u64;
        for i in 0..NUM_HISTOGRAM_BUCKETS {
            if speed >= SPEED_BUCKET_BOUNDARIES[i] && speed < SPEED_BUCKET_BOUNDARIES[i + 1] {
                self.bucket_counts[i] += 1;
                break;
            }
        }
        self.total_samples += 1;
    }

    /// Get the bucket index for a given speed
    pub fn bucket_index(speed_bps: f64) -> usize {
        let speed = speed_bps as u64;
        for i in 0..NUM_HISTOGRAM_BUCKETS {
            if speed >= SPEED_BUCKET_BOUNDARIES[i] && speed < SPEED_BUCKET_BOUNDARIES[i + 1] {
                return i;
            }
        }
        NUM_HISTOGRAM_BUCKETS - 1
    }

    /// Get bucket labels and counts as pairs
    pub fn labeled_counts(&self) -> Vec<(&'static str, u64)> {
        SPEED_BUCKET_LABELS
            .iter()
            .zip(self.bucket_counts.iter())
            .map(|(label, count)| (*label, *count))
            .collect()
    }

    /// Get the most common speed range
    pub fn modal_bucket(&self) -> Option<&'static str> {
        if self.total_samples == 0 {
            return None;
        }
        let max_idx = self
            .bucket_counts
            .iter()
            .enumerate()
            .max_by_key(|(_, c)| *c)
            .map(|(i, _)| i)?;
        Some(SPEED_BUCKET_LABELS[max_idx])
    }
}

/// Configuration for the speed distribution system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedDistributionConfig {
    /// Whether the system is globally enabled
    pub enabled: bool,
    /// Maximum speed samples per domain
    #[serde(default = "default_max_domain_samples")]
    pub max_domain_samples: usize,
    /// Maximum speed samples per hourly bucket
    #[serde(default = "default_max_hourly_samples")]
    pub max_hourly_samples: usize,
    /// Maximum number of domains to track
    pub max_tracked_domains: usize,
    /// Whether to track per-protocol statistics
    #[serde(default = "default_true")]
    pub track_protocol_stats: bool,
    /// Whether to track hourly distribution
    #[serde(default = "default_true")]
    pub track_hourly_distribution: bool,
    /// Whether to maintain global histogram
    #[serde(default = "default_true")]
    pub track_histogram: bool,
}

fn default_max_domain_samples() -> usize {
    DEFAULT_MAX_DOMAIN_SAMPLES
}
fn default_max_hourly_samples() -> usize {
    DEFAULT_MAX_HOURLY_SAMPLES
}
fn default_true() -> bool {
    true
}

impl Default for SpeedDistributionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_domain_samples: DEFAULT_MAX_DOMAIN_SAMPLES,
            max_hourly_samples: DEFAULT_MAX_HOURLY_SAMPLES,
            max_tracked_domains: 200,
            track_protocol_stats: true,
            track_hourly_distribution: true,
            track_histogram: true,
        }
    }
}

/// Summary of speed distribution analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedDistributionSummary {
    /// Global speed statistics
    pub global_stats: SpeedStats,
    /// Number of tracked domains
    pub tracked_domains: usize,
    /// Top domains by download count
    pub top_domains: Vec<DomainSpeedSummary>,
    /// Per-protocol statistics
    pub protocol_stats: Vec<ProtocolSpeedSummary>,
    /// Best hour (highest median speed)
    pub best_hour: Option<HourlySpeedSummary>,
    /// Worst hour (lowest median speed, among hours with data)
    pub worst_hour: Option<HourlySpeedSummary>,
    /// Speed histogram
    pub histogram: HistogramSummary,
    /// Total samples recorded
    pub total_samples: usize,
    /// Overall speed stability score (0.0 - 1.0)
    pub overall_stability: f64,
}

/// Summary for a single domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSpeedSummary {
    /// Domain name
    pub domain: String,
    /// Number of speed samples
    pub sample_count: usize,
    /// Mean speed
    pub mean_bps: f64,
    /// Median speed
    pub median_bps: f64,
    /// P95 speed
    pub p95_bps: f64,
    /// Min speed
    pub min_bps: f64,
    /// Max speed
    pub max_bps: f64,
    /// Stability score (0.0 - 1.0)
    pub stability: f64,
    /// Total bytes downloaded
    pub total_bytes: u64,
    /// Number of downloads
    pub download_count: u32,
}

/// Summary for a single protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolSpeedSummary {
    /// Protocol name
    pub protocol: String,
    /// Number of speed samples
    pub sample_count: usize,
    /// Mean speed
    pub mean_bps: f64,
    /// Median speed
    pub median_bps: f64,
    /// P95 speed
    pub p95_bps: f64,
    /// Stability score
    pub stability: f64,
    /// Total bytes
    pub total_bytes: u64,
}

/// Summary for an hourly bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlySpeedSummary {
    /// Hour (0-23)
    pub hour: u8,
    /// Mean speed for this hour
    pub mean_bps: f64,
    /// Median speed for this hour
    pub median_bps: f64,
    /// Number of samples
    pub sample_count: usize,
    /// Formatted hour string
    pub hour_label: String,
}

/// Histogram summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramSummary {
    /// Bucket labels and counts
    pub buckets: Vec<HistogramBucket>,
    /// Most common speed range
    pub modal_range: Option<String>,
    /// Total samples
    pub total_samples: u64,
}

/// A single histogram bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Range label (e.g., "100-500KB/s")
    pub label: String,
    /// Number of samples in this range
    pub count: u64,
    /// Percentage of total samples
    pub percentage: f64,
}

/// The main speed distribution manager
pub struct SpeedDistributionManager {
    config: SpeedDistributionConfig,
    /// Per-domain speed data
    domains: HashMap<String, DomainSpeedData>,
    /// Per-protocol speed data
    protocols: HashMap<SpeedProtocol, ProtocolSpeedData>,
    /// Hourly speed buckets (0-23)
    hourly: Vec<HourlySpeedBucket>,
    /// Global speed histogram
    histogram: SpeedHistogram,
    /// Global sorted speed samples for overall stats
    global_speeds: Vec<f64>,
    /// Global max samples
    global_max_samples: usize,
    /// Total bytes across all samples
    global_total_bytes: u64,
    /// Data directory for persistence
    data_dir: PathBuf,
}

impl SpeedDistributionManager {
    /// Create a new speed distribution manager
    pub fn new(data_dir: PathBuf) -> Self {
        let config = SpeedDistributionConfig::default();
        let hourly = (0..24)
            .map(|h| HourlySpeedBucket::new(h, config.max_hourly_samples))
            .collect();
        Self {
            global_max_samples: config.max_domain_samples * 10,
            config,
            domains: HashMap::new(),
            protocols: HashMap::new(),
            hourly,
            histogram: SpeedHistogram::default(),
            global_speeds: Vec::new(),
            global_total_bytes: 0,
            data_dir,
        }
    }

    /// Create with a specific config
    pub fn with_config(data_dir: PathBuf, config: SpeedDistributionConfig) -> Self {
        let hourly = (0..24)
            .map(|h| HourlySpeedBucket::new(h, config.max_hourly_samples))
            .collect();
        Self {
            global_max_samples: config.max_domain_samples * 10,
            config,
            domains: HashMap::new(),
            protocols: HashMap::new(),
            hourly,
            histogram: SpeedHistogram::default(),
            global_speeds: Vec::new(),
            global_total_bytes: 0,
            data_dir,
        }
    }

    /// Get current config
    pub fn get_config(&self) -> &SpeedDistributionConfig {
        &self.config
    }

    /// Set config
    pub async fn set_config(&mut self, config: SpeedDistributionConfig) -> std::io::Result<()> {
        self.config = config.clone();
        self.global_max_samples = config.max_domain_samples * 10;
        // Rebuild hourly buckets with new max_samples
        self.hourly = (0..24)
            .map(|h| HourlySpeedBucket::new(h, config.max_hourly_samples))
            .collect();
        self.save_config().await?;
        self.save_data().await
    }

    /// Record a speed sample
    pub async fn record_speed(
        &mut self,
        domain: &str,
        protocol: SpeedProtocol,
        speed_bps: f64,
        bytes: u64,
    ) {
        if !self.config.enabled || speed_bps <= 0.0 {
            return;
        }

        let now = Utc::now();

        // Record global
        let pos = self.global_speeds.partition_point(|&x| x < speed_bps);
        self.global_speeds.insert(pos, speed_bps);
        if self.global_speeds.len() > self.global_max_samples {
            self.global_speeds.remove(0);
        }
        self.global_total_bytes += bytes;

        // Record per-domain
        let normalized_domain = normalize_domain(domain);
        if !self.domains.contains_key(&normalized_domain) {
            if self.domains.len() >= self.config.max_tracked_domains {
                // Evict least recently seen domain
                self.evict_least_recent_domain();
            }
            self.domains.insert(
                normalized_domain.clone(),
                DomainSpeedData::new(normalized_domain.clone(), self.config.max_domain_samples),
            );
        }
        if let Some(dd) = self.domains.get_mut(&normalized_domain) {
            dd.record_speed(speed_bps, bytes);
        }

        // Record per-protocol
        if self.config.track_protocol_stats {
            let proto_data = self.protocols.entry(protocol).or_insert_with(|| {
                ProtocolSpeedData::new(protocol, self.config.max_domain_samples)
            });
            proto_data.record_speed(speed_bps, bytes);
        }

        // Record hourly
        if self.config.track_hourly_distribution {
            let hour = now.hour() as u8;
            if let Some(bucket) = self.hourly.get_mut(hour as usize) {
                bucket.record_speed(speed_bps, bytes);
            }
        }

        // Record histogram
        if self.config.track_histogram {
            self.histogram.record_speed(speed_bps);
        }
    }

    /// Evict the least recently seen domain
    fn evict_least_recent_domain(&mut self) {
        if let Some(oldest_key) = self
            .domains
            .iter()
            .min_by_key(|(_, v)| v.last_seen)
            .map(|(k, _)| k.clone())
        {
            self.domains.remove(&oldest_key);
        }
    }

    /// Get global speed statistics
    pub fn global_stats(&self) -> SpeedStats {
        SpeedStats::from_sorted_speedes(&self.global_speeds, self.global_total_bytes)
    }

    /// Get statistics for a specific domain
    pub fn domain_stats(&self, domain: &str) -> Option<SpeedStats> {
        let normalized = normalize_domain(domain);
        self.domains.get(&normalized).map(|d| d.stats())
    }

    /// Get statistics for a specific protocol
    pub fn protocol_stats(&self, protocol: SpeedProtocol) -> Option<SpeedStats> {
        self.protocols.get(&protocol).map(|p| p.stats())
    }

    /// Get hourly statistics for a specific hour
    pub fn hourly_stats(&self, hour: u8) -> Option<SpeedStats> {
        self.hourly.get(hour as usize).map(|h| h.stats())
    }

    /// Get the full summary
    pub fn get_summary(&self) -> SpeedDistributionSummary {
        let global_stats = self.global_stats();

        // Top domains by download count
        let mut domain_summaries: Vec<DomainSpeedSummary> = self
            .domains
            .values()
            .map(|d| {
                let stats = d.stats();
                DomainSpeedSummary {
                    domain: d.domain.clone(),
                    sample_count: stats.sample_count,
                    mean_bps: stats.mean_bps,
                    median_bps: stats.median_bps,
                    p95_bps: stats.p95_bps,
                    min_bps: stats.min_bps,
                    max_bps: stats.max_bps,
                    stability: stats.stability,
                    total_bytes: d.total_bytes,
                    download_count: d.download_count,
                }
            })
            .collect();
        domain_summaries.sort_by_key(|a| std::cmp::Reverse(a.download_count));
        domain_summaries.truncate(10);

        // Protocol summaries
        let protocol_summaries: Vec<ProtocolSpeedSummary> = self
            .protocols
            .values()
            .map(|p| {
                let stats = p.stats();
                ProtocolSpeedSummary {
                    protocol: p.protocol.to_string(),
                    sample_count: stats.sample_count,
                    mean_bps: stats.mean_bps,
                    median_bps: stats.median_bps,
                    p95_bps: stats.p95_bps,
                    stability: stats.stability,
                    total_bytes: p.total_bytes,
                }
            })
            .collect();

        // Best and worst hours
        let hours_with_data: Vec<(u8, SpeedStats)> = self
            .hourly
            .iter()
            .filter(|h| h.sample_count > 0)
            .map(|h| (h.hour, h.stats()))
            .collect();

        let best_hour = hours_with_data
            .iter()
            .max_by(|a, b| {
                a.1.median_bps
                    .partial_cmp(&b.1.median_bps)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(hour, stats)| HourlySpeedSummary {
                hour: *hour,
                mean_bps: stats.mean_bps,
                median_bps: stats.median_bps,
                sample_count: stats.sample_count,
                hour_label: format!("{:02}:00", hour),
            });

        let worst_hour = hours_with_data
            .iter()
            .min_by(|a, b| {
                a.1.median_bps
                    .partial_cmp(&b.1.median_bps)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(hour, stats)| HourlySpeedSummary {
                hour: *hour,
                mean_bps: stats.mean_bps,
                median_bps: stats.median_bps,
                sample_count: stats.sample_count,
                hour_label: format!("{:02}:00", hour),
            });

        // Histogram summary
        let histogram = {
            let labeled = self.histogram.labeled_counts();
            let total = self.histogram.total_samples;
            HistogramSummary {
                buckets: labeled
                    .into_iter()
                    .map(|(label, count)| HistogramBucket {
                        label: label.to_string(),
                        count,
                        percentage: if total > 0 {
                            (count as f64 / total as f64) * 100.0
                        } else {
                            0.0
                        },
                    })
                    .collect(),
                modal_range: self.histogram.modal_bucket().map(|s| s.to_string()),
                total_samples: total,
            }
        };

        let total_samples = self.global_speeds.len();
        let overall_stability = global_stats.stability;

        SpeedDistributionSummary {
            global_stats,
            tracked_domains: self.domains.len(),
            top_domains: domain_summaries,
            protocol_stats: protocol_summaries,
            best_hour,
            worst_hour,
            histogram,
            total_samples,
            overall_stability,
        }
    }

    /// Get all tracked domain names
    pub fn tracked_domains(&self) -> Vec<String> {
        let mut domains: Vec<String> = self.domains.keys().cloned().collect();
        domains.sort();
        domains
    }

    /// Remove a domain from tracking
    pub fn remove_domain(&mut self, domain: &str) -> bool {
        let normalized = normalize_domain(domain);
        self.domains.remove(&normalized).is_some()
    }

    /// Clear all speed distribution data
    pub async fn clear(&mut self) -> std::io::Result<()> {
        self.domains.clear();
        self.protocols.clear();
        for bucket in &mut self.hourly {
            bucket.speeds.clear();
            bucket.total_bytes = 0;
            bucket.sample_count = 0;
        }
        self.histogram = SpeedHistogram::default();
        self.global_speeds.clear();
        self.global_total_bytes = 0;
        self.save_data().await
    }

    /// Generate a formatted report
    pub fn format_report(&self) -> String {
        let summary = self.get_summary();
        let mut report = String::new();

        report.push_str("╔══════════════════════════════════════════════╗\n");
        report.push_str("║     Download Speed Distribution Report       ║\n");
        report.push_str("╚══════════════════════════════════════════════╝\n\n");

        // Global stats
        report.push_str(&format!(
            "📊 Global Statistics:\n\
             ├─ Samples: {}\n\
             ├─ Mean: {}\n\
             ├─ Median: {}\n\
             ├─ P95: {}\n\
             ├─ Min: {}\n\
             ├─ Max: {}\n\
             ├─ Stability: {:.1}%\n\
             └─ Total: {}\n\n",
            summary.global_stats.sample_count,
            format_speed_bps(summary.global_stats.mean_bps),
            format_speed_bps(summary.global_stats.median_bps),
            format_speed_bps(summary.global_stats.p95_bps),
            format_speed_bps(summary.global_stats.min_bps),
            format_speed_bps(summary.global_stats.max_bps),
            summary.global_stats.stability * 100.0,
            format_bytes(summary.global_stats.total_bytes),
        ));

        // Top domains
        if !summary.top_domains.is_empty() {
            report.push_str(&format!("🌐 Top Domains ({}):\n", summary.tracked_domains));
            for (i, d) in summary.top_domains.iter().enumerate() {
                report.push_str(&format!(
                    "  {}. {} - avg {} median {} ({} downloads, {:.0}% stable)\n",
                    i + 1,
                    d.domain,
                    format_speed_bps(d.mean_bps),
                    format_speed_bps(d.median_bps),
                    d.download_count,
                    d.stability * 100.0,
                ));
            }
            report.push('\n');
        }

        // Protocol stats
        if !summary.protocol_stats.is_empty() {
            report.push_str("📡 Protocol Distribution:\n");
            for p in &summary.protocol_stats {
                report.push_str(&format!(
                    "  {} - avg {} median {} ({} samples)\n",
                    p.protocol,
                    format_speed_bps(p.mean_bps),
                    format_speed_bps(p.median_bps),
                    p.sample_count,
                ));
            }
            report.push('\n');
        }

        // Best/worst hours
        if let Some(ref best) = summary.best_hour {
            report.push_str(&format!(
                "⏰ Best Hour: {} (median {})\n",
                best.hour_label,
                format_speed_bps(best.median_bps),
            ));
        }
        if let Some(ref worst) = summary.worst_hour {
            report.push_str(&format!(
                "⏰ Worst Hour: {} (median {})\n",
                worst.hour_label,
                format_speed_bps(worst.median_bps),
            ));
        }
        report.push('\n');

        // Histogram
        if summary.histogram.total_samples > 0 {
            report.push_str("📈 Speed Distribution:\n");
            let max_count = summary
                .histogram
                .buckets
                .iter()
                .map(|b| b.count)
                .max()
                .unwrap_or(1);
            for bucket in &summary.histogram.buckets {
                if bucket.count > 0 {
                    let bar_len = (bucket.count as f64 / max_count as f64 * 20.0).round() as usize;
                    let bar = "█".repeat(bar_len.max(1));
                    report.push_str(&format!(
                        "  {:>12} │ {:<20} {:>5} ({:.1}%)\n",
                        bucket.label, bar, bucket.count, bucket.percentage,
                    ));
                }
            }
            if let Some(ref modal) = summary.histogram.modal_range {
                report.push_str(&format!("\n  Most common range: {}\n", modal));
            }
        }

        report
    }

    /// Save config to disk
    async fn save_config(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("speed_distribution_config.json");
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
        fs::write(&path, json).await
    }

    /// Save data to disk
    async fn save_data(&self) -> std::io::Result<()> {
        let data = PersistedData {
            domains: self.domains.clone(),
            protocols: self.protocols.clone(),
            hourly: self.hourly.clone(),
            histogram: self.histogram.clone(),
            global_speeds: self.global_speeds.clone(),
            global_total_bytes: self.global_total_bytes,
        };
        let path = self.data_dir.join("speed_distribution_data.json");
        let json = serde_json::to_string_pretty(&data).map_err(std::io::Error::other)?;
        fs::write(&path, json).await
    }

    /// Load data from disk
    pub async fn load(&mut self) -> std::io::Result<()> {
        // Load config
        let config_path = self.data_dir.join("speed_distribution_config.json");
        if let Ok(json) = fs::read_to_string(&config_path).await
            && let Ok(config) = serde_json::from_str::<SpeedDistributionConfig>(&json)
        {
            self.config = config;
            self.global_max_samples = self.config.max_domain_samples * 10;
        }

        // Load data
        let data_path = self.data_dir.join("speed_distribution_data.json");
        if let Ok(json) = fs::read_to_string(&data_path).await
            && let Ok(data) = serde_json::from_str::<PersistedData>(&json)
        {
            self.domains = data.domains;
            self.protocols = data.protocols;
            self.hourly = data.hourly;
            self.histogram = data.histogram;
            self.global_speeds = data.global_speeds;
            self.global_total_bytes = data.global_total_bytes;
        }

        Ok(())
    }
}

/// Persisted data format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedData {
    domains: HashMap<String, DomainSpeedData>,
    protocols: HashMap<SpeedProtocol, ProtocolSpeedData>,
    hourly: Vec<HourlySpeedBucket>,
    histogram: SpeedHistogram,
    global_speeds: Vec<f64>,
    global_total_bytes: u64,
}

/// Normalize a domain name (lowercase, strip port and www prefix)
fn normalize_domain(domain: &str) -> String {
    let d = domain.to_lowercase();
    // Strip port
    let d = d.split(':').next().unwrap_or(&d).to_string();
    // Strip www.
    d.strip_prefix("www.").unwrap_or(&d).to_string()
}

/// Format bytes per second as human-readable string
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

/// Format bytes as human-readable string
fn format_bytes(bytes: u64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_normalize_domain() {
        assert_eq!(normalize_domain("Example.COM"), "example.com");
        assert_eq!(normalize_domain("www.example.com"), "example.com");
        assert_eq!(normalize_domain("Example.COM:8080"), "example.com");
        assert_eq!(normalize_domain("www.example.com:443"), "example.com");
        assert_eq!(normalize_domain("sub.example.com"), "sub.example.com");
    }

    #[test]
    fn test_speed_protocol_from_str() {
        assert_eq!(SpeedProtocol::from_str("http"), SpeedProtocol::Http);
        assert_eq!(SpeedProtocol::from_str("HTTPS"), SpeedProtocol::Http);
        assert_eq!(SpeedProtocol::from_str("torrent"), SpeedProtocol::Torrent);
        assert_eq!(SpeedProtocol::from_str("ed2k"), SpeedProtocol::Ed2k);
        assert_eq!(SpeedProtocol::from_str("p2p"), SpeedProtocol::P2p);
        assert_eq!(SpeedProtocol::from_str("unknown"), SpeedProtocol::Unknown);
    }

    #[test]
    fn test_format_speed_bps() {
        assert_eq!(format_speed_bps(500.0), "500 B/s");
        assert_eq!(format_speed_bps(1024.0), "1.0 KB/s");
        assert_eq!(format_speed_bps(1024.0 * 1024.0), "1.00 MB/s");
        assert_eq!(format_speed_bps(1024.0 * 1024.0 * 1024.0), "1.00 GB/s");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_percentile_from_sorted() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert_eq!(percentile_from_sorted(&data, 0.50), 6.0);
        assert_eq!(percentile_from_sorted(&data, 0.0), 1.0);
        assert_eq!(percentile_from_sorted(&data, 1.0), 10.0);
        assert_eq!(percentile_from_sorted(&data, 0.95), 10.0);
    }

    #[test]
    fn test_percentile_empty() {
        assert_eq!(percentile_from_sorted(&[], 0.5), 0.0);
        assert_eq!(percentile_from_sorted(&[42.0], 0.5), 42.0);
    }

    #[test]
    fn test_speed_stats_from_sorted() {
        let speeds = vec![100.0, 200.0, 300.0, 400.0, 500.0];
        let stats = SpeedStats::from_sorted_speedes(&speeds, 1500);
        assert_eq!(stats.sample_count, 5);
        assert_eq!(stats.min_bps, 100.0);
        assert_eq!(stats.max_bps, 500.0);
        assert!((stats.mean_bps - 300.0).abs() < 0.01);
        assert_eq!(stats.median_bps, 300.0);
        assert_eq!(stats.total_bytes, 1500);
        assert!(stats.stability > 0.0);
    }

    #[test]
    fn test_speed_stats_empty() {
        let stats = SpeedStats::from_sorted_speedes(&[], 0);
        assert_eq!(stats.sample_count, 0);
        assert_eq!(stats.min_bps, 0.0);
        assert_eq!(stats.max_bps, 0.0);
    }

    #[test]
    fn test_histogram_bucket_index() {
        assert_eq!(SpeedHistogram::bucket_index(5000.0), 0); // <10KB/s
        assert_eq!(SpeedHistogram::bucket_index(20.0 * 1024.0), 1); // 10-50KB/s
        assert_eq!(SpeedHistogram::bucket_index(200.0 * 1024.0), 3); // 100-500KB/s
        assert_eq!(SpeedHistogram::bucket_index(2.0 * 1024.0 * 1024.0), 5); // 1-5MB/s
    }

    #[test]
    fn test_histogram_record_and_modal() {
        let mut hist = SpeedHistogram::default();
        // Record many slow speeds
        for _ in 0..10 {
            hist.record_speed(50.0 * 1024.0); // 50-100KB/s bucket
        }
        // Record a few fast speeds
        for _ in 0..3 {
            hist.record_speed(5.0 * 1024.0 * 1024.0); // 1-5MB/s bucket
        }
        assert_eq!(hist.total_samples, 13);
        assert_eq!(hist.modal_bucket(), Some("50-100KB/s"));
    }

    #[test]
    fn test_histogram_empty_modal() {
        let hist = SpeedHistogram::default();
        assert_eq!(hist.modal_bucket(), None);
    }

    #[test]
    fn test_domain_speed_data() {
        let mut dsd = DomainSpeedData::new("example.com".to_string(), 100);
        dsd.record_speed(100.0 * 1024.0, 1000);
        dsd.record_speed(200.0 * 1024.0, 2000);
        dsd.record_speed(300.0 * 1024.0, 3000);

        assert_eq!(dsd.download_count, 3);
        assert_eq!(dsd.total_bytes, 6000);
        let stats = dsd.stats();
        assert_eq!(stats.sample_count, 3);
        assert_eq!(stats.min_bps, 100.0 * 1024.0);
        assert_eq!(stats.max_bps, 300.0 * 1024.0);
    }

    #[test]
    fn test_domain_speed_data_max_samples() {
        let mut dsd = DomainSpeedData::new("test.com".to_string(), 5);
        for i in 0..10 {
            dsd.record_speed((i as f64 + 1.0) * 1000.0, 100);
        }
        assert!(dsd.speeds.len() <= 5);
    }

    #[test]
    fn test_protocol_speed_data() {
        let mut psd = ProtocolSpeedData::new(SpeedProtocol::Http, 100);
        psd.record_speed(1.0 * 1024.0 * 1024.0, 1_000_000);
        psd.record_speed(2.0 * 1024.0 * 1024.0, 2_000_000);

        assert_eq!(psd.download_count, 2);
        let stats = psd.stats();
        assert_eq!(stats.sample_count, 2);
        assert!((stats.mean_bps - 1.5 * 1024.0 * 1024.0).abs() < 1.0);
    }

    #[test]
    fn test_hourly_bucket() {
        let mut bucket = HourlySpeedBucket::new(14, 100);
        bucket.record_speed(500.0 * 1024.0, 5000);
        bucket.record_speed(1000.0 * 1024.0, 10000);

        assert_eq!(bucket.sample_count, 2);
        assert_eq!(bucket.total_bytes, 15000);
        let stats = bucket.stats();
        assert_eq!(stats.sample_count, 2);
    }

    #[tokio::test]
    async fn test_manager_new() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());
        assert!(mgr.domains.is_empty());
        assert!(mgr.protocols.is_empty());
        assert_eq!(mgr.global_speeds.len(), 0);
    }

    #[tokio::test]
    async fn test_manager_record_speed() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed(
            "example.com",
            SpeedProtocol::Http,
            1.0 * 1024.0 * 1024.0,
            1_000_000,
        )
        .await;
        mgr.record_speed(
            "example.com",
            SpeedProtocol::Http,
            2.0 * 1024.0 * 1024.0,
            2_000_000,
        )
        .await;
        mgr.record_speed("other.com", SpeedProtocol::Torrent, 500.0 * 1024.0, 500_000)
            .await;

        assert_eq!(mgr.domains.len(), 2);
        assert!(mgr.protocols.contains_key(&SpeedProtocol::Http));
        assert!(mgr.protocols.contains_key(&SpeedProtocol::Torrent));
        assert_eq!(mgr.global_speeds.len(), 3);
    }

    #[tokio::test]
    async fn test_manager_domain_stats() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        mgr.record_speed("example.com", SpeedProtocol::Http, 200.0 * 1024.0, 2000)
            .await;

        let stats = mgr.domain_stats("example.com").unwrap();
        assert_eq!(stats.sample_count, 2);
        assert_eq!(stats.min_bps, 100.0 * 1024.0);
        assert_eq!(stats.max_bps, 200.0 * 1024.0);
    }

    #[tokio::test]
    async fn test_manager_domain_not_found() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());
        assert!(mgr.domain_stats("nonexistent.com").is_none());
    }

    #[tokio::test]
    async fn test_manager_remove_domain() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        assert!(mgr.remove_domain("example.com"));
        assert!(!mgr.remove_domain("example.com"));
        assert!(mgr.domains.is_empty());
    }

    #[tokio::test]
    async fn test_manager_clear() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        mgr.clear().await.unwrap();

        assert!(mgr.domains.is_empty());
        assert!(mgr.global_speeds.is_empty());
        assert_eq!(mgr.global_total_bytes, 0);
    }

    #[tokio::test]
    async fn test_manager_summary() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed(
            "example.com",
            SpeedProtocol::Http,
            1.0 * 1024.0 * 1024.0,
            1_000_000,
        )
        .await;
        mgr.record_speed("other.com", SpeedProtocol::Torrent, 500.0 * 1024.0, 500_000)
            .await;

        let summary = mgr.get_summary();
        assert_eq!(summary.tracked_domains, 2);
        assert_eq!(summary.total_samples, 2);
        assert!(!summary.top_domains.is_empty());
        assert!(!summary.protocol_stats.is_empty());
    }

    #[tokio::test]
    async fn test_manager_persistence() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        // Create and populate
        {
            let mut mgr = SpeedDistributionManager::new(dir.clone());
            mgr.record_speed(
                "example.com",
                SpeedProtocol::Http,
                1.0 * 1024.0 * 1024.0,
                1_000_000,
            )
            .await;
            mgr.record_speed(
                "example.com",
                SpeedProtocol::Http,
                2.0 * 1024.0 * 1024.0,
                2_000_000,
            )
            .await;
            // Save data
            mgr.set_config(mgr.config.clone()).await.unwrap();
        }

        // Reload and verify
        {
            let mut mgr = SpeedDistributionManager::new(dir);
            mgr.load().await.unwrap();
            assert_eq!(mgr.domains.len(), 1);
            assert_eq!(mgr.global_speeds.len(), 2);
            let stats = mgr.domain_stats("example.com").unwrap();
            assert_eq!(stats.sample_count, 2);
        }
    }

    #[tokio::test]
    async fn test_manager_disabled() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());
        mgr.config.enabled = false;

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;

        assert!(mgr.domains.is_empty());
        assert!(mgr.global_speeds.is_empty());
    }

    #[tokio::test]
    async fn test_manager_zero_speed_ignored() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 0.0, 1000)
            .await;
        mgr.record_speed("example.com", SpeedProtocol::Http, -1.0, 1000)
            .await;

        assert!(mgr.domains.is_empty());
    }

    #[tokio::test]
    async fn test_manager_max_domains_eviction() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());
        mgr.config.max_tracked_domains = 3;

        for i in 0..5 {
            mgr.record_speed(
                &format!("domain{}.com", i),
                SpeedProtocol::Http,
                100.0 * 1024.0,
                1000,
            )
            .await;
        }

        assert!(mgr.domains.len() <= 3);
    }

    #[test]
    fn test_speed_stats_format() {
        let stats = SpeedStats {
            sample_count: 10,
            min_bps: 100.0 * 1024.0,
            max_bps: 5.0 * 1024.0 * 1024.0,
            mean_bps: 1.5 * 1024.0 * 1024.0,
            median_bps: 1.0 * 1024.0 * 1024.0,
            p95_bps: 4.0 * 1024.0 * 1024.0,
            p99_bps: 4.5 * 1024.0 * 1024.0,
            stddev_bps: 500.0 * 1024.0,
            stability: 0.85,
            total_bytes: 100_000_000,
        };
        assert_eq!(stats.format_mean(), "1.50 MB/s");
        assert_eq!(stats.format_median(), "1.00 MB/s");
    }

    #[tokio::test]
    async fn test_format_report() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed(
            "example.com",
            SpeedProtocol::Http,
            1.0 * 1024.0 * 1024.0,
            1_000_000,
        )
        .await;
        mgr.record_speed(
            "example.com",
            SpeedProtocol::Http,
            2.0 * 1024.0 * 1024.0,
            2_000_000,
        )
        .await;

        let report = mgr.format_report();
        assert!(report.contains("Speed Distribution Report"));
        assert!(report.contains("Global Statistics"));
        assert!(report.contains("Top Domains"));
        assert!(report.contains("example.com"));
    }

    #[test]
    fn test_config_default() {
        let config = SpeedDistributionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_domain_samples, DEFAULT_MAX_DOMAIN_SAMPLES);
        assert_eq!(config.max_hourly_samples, DEFAULT_MAX_HOURLY_SAMPLES);
        assert_eq!(config.max_tracked_domains, 200);
        assert!(config.track_protocol_stats);
        assert!(config.track_hourly_distribution);
        assert!(config.track_histogram);
    }

    #[test]
    fn test_config_serialization() {
        let config = SpeedDistributionConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SpeedDistributionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.max_domain_samples, config.max_domain_samples);
    }

    #[test]
    fn test_histogram_labeled_counts() {
        let mut hist = SpeedHistogram::default();
        hist.record_speed(5000.0); // <10KB/s
        hist.record_speed(5000.0); // <10KB/s
        hist.record_speed(30.0 * 1024.0); // 10-50KB/s

        let counts = hist.labeled_counts();
        assert_eq!(counts.len(), NUM_HISTOGRAM_BUCKETS);
        assert_eq!(counts[0].1, 2); // <10KB/s has 2
        assert_eq!(counts[1].1, 1); // 10-50KB/s has 1
    }

    #[tokio::test]
    async fn test_set_config() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        let mut new_config = SpeedDistributionConfig::default();
        new_config.max_tracked_domains = 50;
        new_config.enabled = false;

        mgr.set_config(new_config.clone()).await.unwrap();
        assert_eq!(mgr.config.max_tracked_domains, 50);
        assert!(!mgr.config.enabled);
    }

    #[test]
    fn test_tracked_domains_list() {
        // This is a sync test on the internal structure
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());
        // Use tokio runtime for async record
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            mgr.record_speed("beta.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
                .await;
            mgr.record_speed("alpha.com", SpeedProtocol::Http, 200.0 * 1024.0, 2000)
                .await;
        });
        let domains = mgr.tracked_domains();
        assert_eq!(domains.len(), 2);
        assert_eq!(domains[0], "alpha.com");
        assert_eq!(domains[1], "beta.com");
    }

    // ===== Phase 215: Comprehensive Test Coverage =====

    // --- SpeedProtocol tests ---

    #[test]
    fn test_speed_protocol_display_all_variants() {
        assert_eq!(SpeedProtocol::Http.to_string(), "HTTP");
        assert_eq!(SpeedProtocol::Torrent.to_string(), "Torrent");
        assert_eq!(SpeedProtocol::Ed2k.to_string(), "Ed2k");
        assert_eq!(SpeedProtocol::P2p.to_string(), "P2P");
        assert_eq!(SpeedProtocol::Unknown.to_string(), "Unknown");
    }

    #[test]
    fn test_speed_protocol_serde_roundtrip() {
        for proto in [
            SpeedProtocol::Http,
            SpeedProtocol::Torrent,
            SpeedProtocol::Ed2k,
            SpeedProtocol::P2p,
            SpeedProtocol::Unknown,
        ] {
            let json = serde_json::to_string(&proto).unwrap();
            let deserialized: SpeedProtocol = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, proto);
        }
    }

    #[test]
    fn test_speed_protocol_serde_snake_case() {
        let json = serde_json::to_string(&SpeedProtocol::Http).unwrap();
        assert_eq!(json, "\"http\"");
        let json = serde_json::to_string(&SpeedProtocol::Unknown).unwrap();
        assert_eq!(json, "\"unknown\"");
    }

    #[test]
    fn test_speed_protocol_clone_copy_debug() {
        let proto = SpeedProtocol::Http;
        let cloned = proto.clone();
        assert_eq!(cloned, SpeedProtocol::Http);
        // Copy trait
        let copied = proto;
        assert_eq!(copied, SpeedProtocol::Http);
        // Debug trait
        let debug_str = format!("{:?}", proto);
        assert!(debug_str.contains("Http"));
    }

    #[test]
    fn test_speed_protocol_default() {
        let proto: SpeedProtocol = Default::default();
        assert_eq!(proto, SpeedProtocol::Http);
    }

    #[test]
    fn test_speed_protocol_from_str_edge_cases() {
        assert_eq!(SpeedProtocol::from_str(""), SpeedProtocol::Unknown);
        assert_eq!(SpeedProtocol::from_str("ftp"), SpeedProtocol::Http);
        assert_eq!(SpeedProtocol::from_str("HTTPS"), SpeedProtocol::Http);
        assert_eq!(
            SpeedProtocol::from_str("BitTorrent"),
            SpeedProtocol::Torrent
        );
        assert_eq!(SpeedProtocol::from_str("bt"), SpeedProtocol::Torrent);
        assert_eq!(SpeedProtocol::from_str("edonkey"), SpeedProtocol::Ed2k);
        assert_eq!(
            SpeedProtocol::from_str("random_string"),
            SpeedProtocol::Unknown
        );
    }

    // --- SpeedStats tests ---

    #[test]
    fn test_speed_stats_serde_roundtrip() {
        let stats = SpeedStats {
            sample_count: 100,
            min_bps: 1000.0,
            max_bps: 50000.0,
            mean_bps: 25000.0,
            median_bps: 20000.0,
            p95_bps: 45000.0,
            p99_bps: 48000.0,
            stddev_bps: 5000.0,
            stability: 0.85,
            total_bytes: 1_000_000,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: SpeedStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sample_count, stats.sample_count);
        assert!((deserialized.mean_bps - stats.mean_bps).abs() < 0.01);
    }

    #[test]
    fn test_speed_stats_clone_debug() {
        let stats = SpeedStats::default();
        let cloned = stats.clone();
        assert_eq!(cloned.sample_count, 0);
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("SpeedStats"));
    }

    #[test]
    fn test_speed_stats_single_sample() {
        let stats = SpeedStats::from_sorted_speedes(&[1000.0], 1000);
        assert_eq!(stats.sample_count, 1);
        assert_eq!(stats.min_bps, 1000.0);
        assert_eq!(stats.max_bps, 1000.0);
        assert_eq!(stats.mean_bps, 1000.0);
        assert_eq!(stats.median_bps, 1000.0);
        assert_eq!(stats.stddev_bps, 0.0);
        assert_eq!(stats.stability, 1.0);
    }

    #[test]
    fn test_speed_stats_stability_calculation() {
        // All same values = perfect stability
        let stats = SpeedStats::from_sorted_speedes(&[1000.0, 1000.0, 1000.0], 3000);
        assert_eq!(stats.stability, 1.0);

        // Moderate variation = lower stability
        let stats = SpeedStats::from_sorted_speedes(&[800.0, 1000.0, 1200.0], 3000);
        assert!(stats.stability < 1.0);
        assert!(stats.stability > 0.0);

        // Extreme variation = stability clamped to 0
        let stats = SpeedStats::from_sorted_speedes(&[100.0, 1000.0, 10000.0], 11100);
        assert_eq!(stats.stability, 0.0);
    }

    #[test]
    fn test_speed_stats_zero_mean() {
        let stats = SpeedStats::from_sorted_speedes(&[0.0, 0.0, 0.0], 0);
        assert_eq!(stats.stability, 0.0);
    }

    #[test]
    fn test_speed_stats_format_mean_median() {
        let stats = SpeedStats {
            sample_count: 1,
            min_bps: 1024.0,
            max_bps: 1024.0,
            mean_bps: 1024.0,
            median_bps: 1024.0,
            p95_bps: 1024.0,
            p99_bps: 1024.0,
            stddev_bps: 0.0,
            stability: 1.0,
            total_bytes: 1024,
        };
        assert_eq!(stats.format_mean(), "1.0 KB/s");
        assert_eq!(stats.format_median(), "1.0 KB/s");
    }

    // --- DomainSpeedData tests ---

    #[test]
    fn test_domain_speed_data_serde_roundtrip() {
        let mut dsd = DomainSpeedData::new("test.com".to_string(), 100);
        dsd.record_speed(1000.0, 100);
        dsd.record_speed(2000.0, 200);

        let json = serde_json::to_string(&dsd).unwrap();
        let deserialized: DomainSpeedData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.domain, "test.com");
        assert_eq!(deserialized.speeds.len(), 2);
        assert_eq!(deserialized.total_bytes, 300);
        assert_eq!(deserialized.download_count, 2);
    }

    #[test]
    fn test_domain_speed_data_clone_debug() {
        let dsd = DomainSpeedData::new("test.com".to_string(), 100);
        let cloned = dsd.clone();
        assert_eq!(cloned.domain, "test.com");
        let debug_str = format!("{:?}", dsd);
        assert!(debug_str.contains("DomainSpeedData"));
    }

    #[test]
    fn test_domain_speed_data_sorted_insertion() {
        let mut dsd = DomainSpeedData::new("test.com".to_string(), 100);
        dsd.record_speed(500.0, 100);
        dsd.record_speed(100.0, 100);
        dsd.record_speed(300.0, 100);
        dsd.record_speed(200.0, 100);

        // Speeds should be sorted
        assert_eq!(dsd.speeds, vec![100.0, 200.0, 300.0, 500.0]);
    }

    #[test]
    fn test_domain_speed_data_max_samples_trimming() {
        let mut dsd = DomainSpeedData::new("test.com".to_string(), 5);
        for i in 0..10 {
            dsd.record_speed((i as f64 + 1.0) * 100.0, 100);
        }
        assert!(dsd.speeds.len() <= 5);
        assert_eq!(dsd.download_count, 10);
    }

    // --- ProtocolSpeedData tests ---

    #[test]
    fn test_protocol_speed_data_serde_roundtrip() {
        let mut psd = ProtocolSpeedData::new(SpeedProtocol::Torrent, 100);
        psd.record_speed(1_000_000.0, 1_000_000);
        psd.record_speed(2_000_000.0, 2_000_000);

        let json = serde_json::to_string(&psd).unwrap();
        let deserialized: ProtocolSpeedData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.protocol, SpeedProtocol::Torrent);
        assert_eq!(deserialized.speeds.len(), 2);
    }

    #[test]
    fn test_protocol_speed_data_clone_debug() {
        let psd = ProtocolSpeedData::new(SpeedProtocol::Http, 100);
        let cloned = psd.clone();
        assert_eq!(cloned.protocol, SpeedProtocol::Http);
        let debug_str = format!("{:?}", psd);
        assert!(debug_str.contains("ProtocolSpeedData"));
    }

    // --- HourlySpeedBucket tests ---

    #[test]
    fn test_hourly_speed_bucket_serde_roundtrip() {
        let mut bucket = HourlySpeedBucket::new(14, 100);
        bucket.record_speed(1000.0, 100);
        bucket.record_speed(2000.0, 200);

        let json = serde_json::to_string(&bucket).unwrap();
        let deserialized: HourlySpeedBucket = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.hour, 14);
        assert_eq!(deserialized.sample_count, 2);
    }

    #[test]
    fn test_hourly_speed_bucket_clone_debug() {
        let bucket = HourlySpeedBucket::new(0, 100);
        let cloned = bucket.clone();
        assert_eq!(cloned.hour, 0);
        let debug_str = format!("{:?}", bucket);
        assert!(debug_str.contains("HourlySpeedBucket"));
    }

    #[test]
    fn test_hourly_speed_bucket_max_samples() {
        let mut bucket = HourlySpeedBucket::new(12, 5);
        for i in 0..10 {
            bucket.record_speed((i as f64 + 1.0) * 100.0, 100);
        }
        assert!(bucket.speeds.len() <= 5);
        assert_eq!(bucket.sample_count, 10);
    }

    // --- SpeedHistogram tests ---

    #[test]
    fn test_speed_histogram_serde_roundtrip() {
        let mut hist = SpeedHistogram::default();
        hist.record_speed(5000.0);
        hist.record_speed(50_000.0);

        let json = serde_json::to_string(&hist).unwrap();
        let deserialized: SpeedHistogram = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_samples, 2);
        assert_eq!(deserialized.bucket_counts[0], 1);
    }

    #[test]
    fn test_speed_histogram_clone_debug() {
        let hist = SpeedHistogram::default();
        let cloned = hist.clone();
        assert_eq!(cloned.total_samples, 0);
        let debug_str = format!("{:?}", hist);
        assert!(debug_str.contains("SpeedHistogram"));
    }

    #[test]
    fn test_speed_histogram_bucket_index_boundaries() {
        // Test exact boundaries
        assert_eq!(SpeedHistogram::bucket_index(0.0), 0);
        assert_eq!(SpeedHistogram::bucket_index(10.0 * 1024.0 - 1.0), 0);
        assert_eq!(SpeedHistogram::bucket_index(10.0 * 1024.0), 1);
        assert_eq!(SpeedHistogram::bucket_index(50.0 * 1024.0 - 1.0), 1);
        assert_eq!(SpeedHistogram::bucket_index(50.0 * 1024.0), 2);
        assert_eq!(SpeedHistogram::bucket_index(100.0 * 1024.0), 3);
        assert_eq!(SpeedHistogram::bucket_index(500.0 * 1024.0), 4);
        assert_eq!(SpeedHistogram::bucket_index(1024.0 * 1024.0), 5);
        assert_eq!(SpeedHistogram::bucket_index(5.0 * 1024.0 * 1024.0), 6);
        assert_eq!(SpeedHistogram::bucket_index(10.0 * 1024.0 * 1024.0), 7);
        assert_eq!(SpeedHistogram::bucket_index(50.0 * 1024.0 * 1024.0), 8);
        assert_eq!(SpeedHistogram::bucket_index(100.0 * 1024.0 * 1024.0), 9);
    }

    #[test]
    fn test_speed_histogram_all_buckets() {
        let mut hist = SpeedHistogram::default();
        // Record one sample in each bucket
        hist.record_speed(5.0 * 1024.0); // <10KB/s
        hist.record_speed(20.0 * 1024.0); // 10-50KB/s
        hist.record_speed(75.0 * 1024.0); // 50-100KB/s
        hist.record_speed(200.0 * 1024.0); // 100-500KB/s
        hist.record_speed(750.0 * 1024.0); // 500KB-1MB/s
        hist.record_speed(2.0 * 1024.0 * 1024.0); // 1-5MB/s
        hist.record_speed(7.0 * 1024.0 * 1024.0); // 5-10MB/s
        hist.record_speed(20.0 * 1024.0 * 1024.0); // 10-50MB/s
        hist.record_speed(75.0 * 1024.0 * 1024.0); // 50-100MB/s
        hist.record_speed(150.0 * 1024.0 * 1024.0); // >100MB/s

        assert_eq!(hist.total_samples, 10);
        for i in 0..NUM_HISTOGRAM_BUCKETS {
            assert_eq!(
                hist.bucket_counts[i], 1,
                "bucket {} should have 1 sample",
                i
            );
        }
    }

    // --- Config tests ---

    #[test]
    fn test_config_serde_roundtrip() {
        let config = SpeedDistributionConfig {
            enabled: false,
            max_domain_samples: 500,
            max_hourly_samples: 250,
            max_tracked_domains: 100,
            track_protocol_stats: false,
            track_hourly_distribution: false,
            track_histogram: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SpeedDistributionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, false);
        assert_eq!(deserialized.max_domain_samples, 500);
        assert_eq!(deserialized.max_hourly_samples, 250);
        assert_eq!(deserialized.max_tracked_domains, 100);
        assert_eq!(deserialized.track_protocol_stats, false);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{
            "enabled": true,
            "max_domain_samples": 1000,
            "max_hourly_samples": 500,
            "max_tracked_domains": 200,
            "track_protocol_stats": true,
            "track_hourly_distribution": true,
            "track_histogram": true,
            "unknown_field": "should be ignored"
        }"#;
        let config: SpeedDistributionConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = SpeedDistributionConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("SpeedDistributionConfig"));
    }

    // --- Summary struct tests ---

    #[test]
    fn test_speed_distribution_summary_serde_roundtrip() {
        let summary = SpeedDistributionSummary {
            global_stats: SpeedStats::default(),
            tracked_domains: 5,
            top_domains: vec![],
            protocol_stats: vec![],
            best_hour: None,
            worst_hour: None,
            histogram: HistogramSummary {
                buckets: vec![],
                modal_range: None,
                total_samples: 0,
            },
            total_samples: 100,
            overall_stability: 0.9,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: SpeedDistributionSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tracked_domains, 5);
        assert_eq!(deserialized.total_samples, 100);
    }

    #[test]
    fn test_domain_speed_summary_clone_debug() {
        let summary = DomainSpeedSummary {
            domain: "test.com".to_string(),
            sample_count: 10,
            mean_bps: 1000.0,
            median_bps: 900.0,
            p95_bps: 1500.0,
            min_bps: 100.0,
            max_bps: 2000.0,
            stability: 0.8,
            total_bytes: 10000,
            download_count: 5,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.domain, "test.com");
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("DomainSpeedSummary"));
    }

    #[test]
    fn test_hourly_speed_summary_serde() {
        let summary = HourlySpeedSummary {
            hour: 14,
            mean_bps: 1_000_000.0,
            median_bps: 800_000.0,
            sample_count: 50,
            hour_label: "14:00".to_string(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: HourlySpeedSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.hour, 14);
        assert_eq!(deserialized.hour_label, "14:00");
    }

    #[test]
    fn test_histogram_summary_serde() {
        let summary = HistogramSummary {
            buckets: vec![HistogramBucket {
                label: "<10KB/s".to_string(),
                count: 100,
                percentage: 50.0,
            }],
            modal_range: Some("<10KB/s".to_string()),
            total_samples: 200,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: HistogramSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.buckets.len(), 1);
        assert_eq!(deserialized.total_samples, 200);
    }

    // --- Manager edge case tests ---

    #[tokio::test]
    async fn test_manager_with_config() {
        let tmp = TempDir::new().unwrap();
        let config = SpeedDistributionConfig {
            max_domain_samples: 100,
            max_hourly_samples: 50,
            max_tracked_domains: 10,
            ..Default::default()
        };
        let mgr = SpeedDistributionManager::with_config(tmp.path().to_path_buf(), config);
        assert_eq!(mgr.config.max_domain_samples, 100);
        assert_eq!(mgr.config.max_hourly_samples, 50);
        assert_eq!(mgr.config.max_tracked_domains, 10);
    }

    #[tokio::test]
    async fn test_manager_get_config() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());
        let config = mgr.get_config();
        assert!(config.enabled);
        assert_eq!(config.max_domain_samples, DEFAULT_MAX_DOMAIN_SAMPLES);
    }

    #[tokio::test]
    async fn test_manager_protocol_stats() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        mgr.record_speed("example.com", SpeedProtocol::Http, 200.0 * 1024.0, 2000)
            .await;
        mgr.record_speed("other.com", SpeedProtocol::Torrent, 500.0 * 1024.0, 5000)
            .await;

        let http_stats = mgr.protocol_stats(SpeedProtocol::Http).unwrap();
        assert_eq!(http_stats.sample_count, 2);

        let torrent_stats = mgr.protocol_stats(SpeedProtocol::Torrent).unwrap();
        assert_eq!(torrent_stats.sample_count, 1);

        let ed2k_stats = mgr.protocol_stats(SpeedProtocol::Ed2k);
        assert!(ed2k_stats.is_none());
    }

    #[tokio::test]
    async fn test_manager_hourly_stats() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        // Record some speeds (will go to current hour)
        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;

        // Get current hour
        let current_hour = chrono::Utc::now().hour() as u8;
        let stats = mgr.hourly_stats(current_hour);
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.sample_count, 1);

        // Empty hour should return empty stats
        let empty_hour = (current_hour + 12) % 24;
        let empty_stats = mgr.hourly_stats(empty_hour);
        assert!(empty_stats.is_some());
        assert_eq!(empty_stats.unwrap().sample_count, 0);
    }

    #[tokio::test]
    async fn test_manager_global_stats() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        mgr.record_speed("example.com", SpeedProtocol::Http, 200.0 * 1024.0, 2000)
            .await;
        mgr.record_speed("other.com", SpeedProtocol::Torrent, 300.0 * 1024.0, 3000)
            .await;

        let global = mgr.global_stats();
        assert_eq!(global.sample_count, 3);
        assert_eq!(global.min_bps, 100.0 * 1024.0);
        assert_eq!(global.max_bps, 300.0 * 1024.0);
        assert_eq!(global.total_bytes, 6000);
    }

    #[tokio::test]
    async fn test_manager_unicode_domain() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("中文域名.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        mgr.record_speed("日本語.jp", SpeedProtocol::Http, 200.0 * 1024.0, 2000)
            .await;

        assert_eq!(mgr.domains.len(), 2);
        let domains = mgr.tracked_domains();
        assert!(domains.contains(&"中文域名.com".to_string()));
        assert!(domains.contains(&"日本語.jp".to_string()));
    }

    #[tokio::test]
    async fn test_manager_domain_normalization() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        // These should all normalize to the same domain
        mgr.record_speed("Example.COM", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        mgr.record_speed(
            "example.com:8080",
            SpeedProtocol::Http,
            200.0 * 1024.0,
            2000,
        )
        .await;
        mgr.record_speed("www.example.com", SpeedProtocol::Http, 300.0 * 1024.0, 3000)
            .await;

        // Should all be merged into "example.com"
        assert_eq!(mgr.domains.len(), 1);
        let stats = mgr.domain_stats("example.com").unwrap();
        assert_eq!(stats.sample_count, 3);
    }

    #[tokio::test]
    async fn test_manager_remove_domain_not_found() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());
        assert!(!mgr.remove_domain("nonexistent.com"));
    }

    #[tokio::test]
    async fn test_manager_summary_empty() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        let summary = mgr.get_summary();
        assert_eq!(summary.tracked_domains, 0);
        assert_eq!(summary.total_samples, 0);
        assert!(summary.top_domains.is_empty());
        assert!(summary.protocol_stats.is_empty());
        assert!(summary.best_hour.is_none());
        assert!(summary.worst_hour.is_none());
    }

    #[tokio::test]
    async fn test_manager_summary_with_data() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        for i in 0..10 {
            mgr.record_speed(
                &format!("domain{}.com", i),
                SpeedProtocol::Http,
                (i as f64 + 1.0) * 100.0 * 1024.0,
                (i as u64 + 1) * 1000,
            )
            .await;
        }

        let summary = mgr.get_summary();
        assert_eq!(summary.tracked_domains, 10);
        assert_eq!(summary.total_samples, 10);
        assert!(!summary.top_domains.is_empty());
        assert!(summary.top_domains.len() <= 10);
    }

    #[tokio::test]
    async fn test_manager_persistence_missing_files() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        // Load from non-existent files should not error
        let result = mgr.load().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_manager_persistence_corrupt_json() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        // Write corrupt JSON
        fs::write(dir.join("speed_distribution_config.json"), "not json")
            .await
            .unwrap();
        fs::write(dir.join("speed_distribution_data.json"), "not json")
            .await
            .unwrap();

        let mut mgr = SpeedDistributionManager::new(dir);
        // Load should succeed but not load any data
        let result = mgr.load().await;
        assert!(result.is_ok());
        assert!(mgr.domains.is_empty());
    }

    #[tokio::test]
    async fn test_manager_persistence_overwrite() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        // First save
        {
            let mut mgr = SpeedDistributionManager::new(dir.clone());
            mgr.record_speed("first.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
                .await;
            mgr.set_config(mgr.config.clone()).await.unwrap();
        }

        // Second save (overwrite)
        {
            let mut mgr = SpeedDistributionManager::new(dir.clone());
            mgr.record_speed("second.com", SpeedProtocol::Http, 200.0 * 1024.0, 2000)
                .await;
            mgr.set_config(mgr.config.clone()).await.unwrap();
        }

        // Load and verify only second data exists
        {
            let mut mgr = SpeedDistributionManager::new(dir);
            mgr.load().await.unwrap();
            assert_eq!(mgr.domains.len(), 1);
            assert!(mgr.domains.contains_key("second.com"));
        }
    }

    #[tokio::test]
    async fn test_manager_clear_resets_histogram() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        assert_eq!(mgr.histogram.total_samples, 1);

        mgr.clear().await.unwrap();
        assert_eq!(mgr.histogram.total_samples, 0);
    }

    #[tokio::test]
    async fn test_manager_clear_resets_hourly() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;

        let current_hour = chrono::Utc::now().hour() as usize;
        assert!(mgr.hourly[current_hour].sample_count > 0);

        mgr.clear().await.unwrap();
        for bucket in &mgr.hourly {
            assert_eq!(bucket.sample_count, 0);
        }
    }

    // --- Format function edge cases ---

    #[test]
    fn test_format_speed_bps_edge_cases() {
        assert_eq!(format_speed_bps(0.0), "0 B/s");
        assert_eq!(format_speed_bps(0.5), "0 B/s"); // rounds to 0
        assert_eq!(format_speed_bps(1023.0), "1023 B/s");
        assert_eq!(format_speed_bps(1024.0 * 1024.0 - 1.0), "1024.0 KB/s");
    }

    #[test]
    fn test_format_bytes_edge_cases() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024 * 1024 - 1), "1024.0 KB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 - 1), "1024.0 MB");
    }

    // --- Constants tests ---

    #[test]
    fn test_constants() {
        assert_eq!(NUM_HISTOGRAM_BUCKETS, 10);
        assert_eq!(DEFAULT_MAX_DOMAIN_SAMPLES, 1000);
        assert_eq!(DEFAULT_MAX_HOURLY_SAMPLES, 500);
    }

    #[test]
    fn test_speed_bucket_boundaries_length() {
        assert_eq!(SPEED_BUCKET_BOUNDARIES.len(), NUM_HISTOGRAM_BUCKETS + 1);
        assert_eq!(SPEED_BUCKET_LABELS.len(), NUM_HISTOGRAM_BUCKETS);
    }

    #[test]
    fn test_speed_bucket_boundaries_sorted() {
        for i in 0..NUM_HISTOGRAM_BUCKETS {
            assert!(SPEED_BUCKET_BOUNDARIES[i] < SPEED_BUCKET_BOUNDARIES[i + 1]);
        }
    }

    // --- Report format tests ---

    #[tokio::test]
    async fn test_format_report_empty() {
        let tmp = TempDir::new().unwrap();
        let mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());
        let report = mgr.format_report();
        assert!(report.contains("Speed Distribution Report"));
        assert!(report.contains("Global Statistics"));
    }

    #[tokio::test]
    async fn test_format_report_with_protocol_stats() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        mgr.record_speed("example.com", SpeedProtocol::Http, 100.0 * 1024.0, 1000)
            .await;
        mgr.record_speed("other.com", SpeedProtocol::Torrent, 200.0 * 1024.0, 2000)
            .await;

        let report = mgr.format_report();
        assert!(report.contains("Protocol Distribution"));
        assert!(report.contains("HTTP"));
        assert!(report.contains("Torrent"));
    }

    #[tokio::test]
    async fn test_format_report_with_histogram() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = SpeedDistributionManager::new(tmp.path().to_path_buf());

        for i in 0..20 {
            mgr.record_speed(
                "example.com",
                SpeedProtocol::Http,
                (i as f64 + 1.0) * 10.0 * 1024.0,
                1000,
            )
            .await;
        }

        let report = mgr.format_report();
        assert!(report.contains("Speed Distribution"));
    }

    // --- PersistedData tests ---

    #[test]
    fn test_persisted_data_serde() {
        let data = PersistedData {
            domains: HashMap::new(),
            protocols: HashMap::new(),
            hourly: vec![],
            histogram: SpeedHistogram::default(),
            global_speeds: vec![100.0, 200.0, 300.0],
            global_total_bytes: 600,
        };
        let json = serde_json::to_string(&data).unwrap();
        let deserialized: PersistedData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.global_speeds.len(), 3);
        assert_eq!(deserialized.global_total_bytes, 600);
    }

    #[test]
    fn test_persisted_data_clone_debug() {
        let data = PersistedData {
            domains: HashMap::new(),
            protocols: HashMap::new(),
            hourly: vec![],
            histogram: SpeedHistogram::default(),
            global_speeds: vec![],
            global_total_bytes: 0,
        };
        let cloned = data.clone();
        assert_eq!(cloned.global_total_bytes, 0);
        let debug_str = format!("{:?}", data);
        assert!(debug_str.contains("PersistedData"));
    }
}
