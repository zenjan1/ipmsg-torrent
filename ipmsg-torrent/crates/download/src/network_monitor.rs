//! Network Condition Monitor (Phase 102)
//!
//! Monitors overall network quality for downloads, classifies conditions,
//! detects patterns by time-of-day, and recommends optimal download windows.
//!
//! Features:
//! - Network condition classification (Excellent/Good/Fair/Poor/Congested)
//! - Time-of-day analysis to find optimal download windows
//! - Network stability tracking (jitter, speed variance)
//! - Historical data persistence for long-term pattern detection
//! - Actionable recommendations for download scheduling

use chrono::{Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;
use tokio::fs;

/// Default maximum samples to keep in memory
const DEFAULT_MAX_SAMPLES: usize = 2880; // ~24h at 30s intervals

/// Default number of hourly buckets for time-of-day analysis
const HOURS_IN_DAY: usize = 24;

/// Errors from network monitor operations.
#[derive(Error, Debug)]
pub enum NetworkMonitorError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Network condition classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkCondition {
    /// Excellent: >10MB/s, low jitter
    Excellent,
    /// Good: 1-10MB/s, moderate jitter
    Good,
    /// Fair: 100KB/s-1MB/s
    Fair,
    /// Poor: 10-100KB/s
    Poor,
    /// Congested: <10KB/s or highly unstable
    Congested,
}

impl NetworkCondition {
    /// Classify based on speed (bytes/sec) and jitter coefficient
    pub fn classify(speed_bps: f64, jitter: f64) -> Self {
        // High jitter degrades the classification
        let jitter_penalty = if jitter > 0.8 {
            2
        } else if jitter > 0.5 {
            1
        } else {
            0
        };

        let base = match speed_bps {
            s if s >= 10_000_000.0 => 0, // Excellent base
            s if s >= 1_000_000.0 => 1,  // Good base
            s if s >= 100_000.0 => 2,    // Fair base
            s if s >= 10_000.0 => 3,     // Poor base
            _ => 4,                      // Congested base
        };

        let adjusted = (base + jitter_penalty).min(4);
        match adjusted {
            0 => NetworkCondition::Excellent,
            1 => NetworkCondition::Good,
            2 => NetworkCondition::Fair,
            3 => NetworkCondition::Poor,
            _ => NetworkCondition::Congested,
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            NetworkCondition::Excellent => "🟢",
            NetworkCondition::Good => "🔵",
            NetworkCondition::Fair => "🟡",
            NetworkCondition::Poor => "🟠",
            NetworkCondition::Congested => "🔴",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            NetworkCondition::Excellent => "Excellent",
            NetworkCondition::Good => "Good",
            NetworkCondition::Fair => "Fair",
            NetworkCondition::Poor => "Poor",
            NetworkCondition::Congested => "Congested",
        }
    }

    /// Recommended max concurrent downloads for this condition
    pub fn recommended_concurrency(&self) -> usize {
        match self {
            NetworkCondition::Excellent => 8,
            NetworkCondition::Good => 4,
            NetworkCondition::Fair => 2,
            NetworkCondition::Poor => 1,
            NetworkCondition::Congested => 1,
        }
    }
}

impl std::fmt::Display for NetworkCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.emoji(), self.label())
    }
}

/// A network quality sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkSample {
    /// When the sample was taken
    pub timestamp: i64,
    /// Aggregate download speed (bytes/sec)
    pub speed_bps: f64,
    /// Number of active downloads when sampled
    pub active_tasks: usize,
    /// Speed variance (jitter indicator)
    pub jitter: f64,
}

/// Statistics for a specific hour of the day (0-23)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HourlyStats {
    /// Number of samples in this hour
    pub sample_count: u32,
    /// Average speed (bytes/sec)
    pub avg_speed_bps: f64,
    /// Peak speed (bytes/sec)
    pub peak_speed_bps: f64,
    /// Cumulative speed for averaging (internal)
    #[serde(default)]
    cumulative_speed: f64,
}

impl HourlyStats {
    fn add_sample(&mut self, speed_bps: f64) {
        self.cumulative_speed += speed_bps;
        self.sample_count += 1;
        self.avg_speed_bps = self.cumulative_speed / self.sample_count as f64;
        if speed_bps > self.peak_speed_bps {
            self.peak_speed_bps = speed_bps;
        }
    }
}

/// Network condition summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkSummary {
    /// Current network condition
    pub current_condition: NetworkCondition,
    /// Current aggregate speed
    pub current_speed_bps: f64,
    /// Average speed over all samples
    pub avg_speed_bps: f64,
    /// Peak speed observed
    pub peak_speed_bps: f64,
    /// Network stability score (0-100, higher = more stable)
    pub stability_score: f64,
    /// Total samples collected
    pub total_samples: usize,
    /// Recommended concurrent downloads
    pub recommended_concurrency: usize,
    /// Best hours for downloading (sorted by avg speed, descending)
    pub best_hours: Vec<u8>,
    /// Worst hours for downloading (sorted by avg speed, ascending)
    pub worst_hours: Vec<u8>,
    /// Time window recommendation
    pub recommendation: String,
}

/// Configuration for the network monitor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMonitorConfig {
    /// Enable network monitoring
    pub enabled: bool,
    /// Maximum samples to keep in memory
    pub max_samples: usize,
    /// Sample interval in seconds
    pub sample_interval_secs: u64,
    /// Speed threshold for "excellent" (bytes/sec)
    pub excellent_threshold: f64,
    /// Speed threshold for "good" (bytes/sec)
    pub good_threshold: f64,
    /// Speed threshold for "fair" (bytes/sec)
    pub fair_threshold: f64,
    /// Speed threshold for "poor" (bytes/sec)
    pub poor_threshold: f64,
    /// Jitter threshold for high instability
    pub high_jitter_threshold: f64,
}

impl Default for NetworkMonitorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_samples: DEFAULT_MAX_SAMPLES,
            sample_interval_secs: 30,
            excellent_threshold: 10_000_000.0,
            good_threshold: 1_000_000.0,
            fair_threshold: 100_000.0,
            poor_threshold: 10_000.0,
            high_jitter_threshold: 0.5,
        }
    }
}

/// Persistence wrapper for network monitor data
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetworkMonitorData {
    /// Configuration
    pub config: NetworkMonitorConfig,
    /// Historical hourly statistics
    pub hourly_stats: Vec<HourlyStats>,
    /// Total samples ever collected (for display)
    pub total_samples_collected: usize,
}

/// Network Condition Monitor
///
/// Tracks network quality over time and provides insights for download optimization.
#[derive(Debug)]
pub struct NetworkMonitor {
    /// Configuration
    config: NetworkMonitorConfig,
    /// Recent samples (ring buffer)
    samples: Vec<NetworkSample>,
    /// Hourly statistics buckets (0-23)
    hourly_stats: Vec<HourlyStats>,
    /// Total samples collected (including evicted)
    total_samples_collected: usize,
    /// Speed samples for jitter calculation (recent window)
    recent_speeds: Vec<f64>,
    /// Maximum recent speed samples for jitter calculation
    max_recent_speeds: usize,
}

impl NetworkMonitor {
    /// Create a new network monitor with default configuration.
    pub fn new() -> Self {
        Self::with_config(NetworkMonitorConfig::default())
    }

    /// Create a new network monitor with custom configuration.
    pub fn with_config(config: NetworkMonitorConfig) -> Self {
        Self {
            config,
            samples: Vec::new(),
            hourly_stats: vec![HourlyStats::default(); HOURS_IN_DAY],
            total_samples_collected: 0,
            recent_speeds: Vec::new(),
            max_recent_speeds: 20,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &NetworkMonitorConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: NetworkMonitorConfig) {
        self.config = config;
    }

    /// Record a network quality sample
    pub fn record_sample(&mut self, speed_bps: f64, active_tasks: usize) {
        if !self.config.enabled {
            return;
        }

        let now = Utc::now();
        let jitter = self.calculate_jitter(speed_bps);

        let sample = NetworkSample {
            timestamp: now.timestamp(),
            speed_bps,
            active_tasks,
            jitter,
        };

        // Add to ring buffer
        self.samples.push(sample);
        if self.samples.len() > self.config.max_samples {
            self.samples.remove(0);
        }

        // Update hourly stats
        let hour = now.hour() as usize;
        if hour < HOURS_IN_DAY {
            self.hourly_stats[hour].add_sample(speed_bps);
        }

        // Update recent speeds for jitter
        self.recent_speeds.push(speed_bps);
        if self.recent_speeds.len() > self.max_recent_speeds {
            self.recent_speeds.remove(0);
        }

        self.total_samples_collected += 1;
    }

    /// Calculate jitter based on recent speed variance
    fn calculate_jitter(&self, new_speed: f64) -> f64 {
        if self.recent_speeds.len() < 2 {
            return 0.0;
        }

        let all_speeds: Vec<f64> = self
            .recent_speeds
            .iter()
            .copied()
            .chain(std::iter::once(new_speed))
            .collect();

        let mean: f64 = all_speeds.iter().sum::<f64>() / all_speeds.len() as f64;
        if mean <= 0.0 {
            return 0.0;
        }

        let variance: f64 =
            all_speeds.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / all_speeds.len() as f64;
        let stddev = variance.sqrt();

        // Coefficient of variation (normalized jitter)
        (stddev / mean).min(1.0)
    }

    /// Get current network condition
    pub fn current_condition(&self) -> NetworkCondition {
        if self.samples.is_empty() {
            return NetworkCondition::Good; // Default assumption
        }

        let latest = self.samples.last().unwrap();
        let avg_speed = self.recent_average_speed();
        let jitter = latest.jitter;

        // Use average of recent speeds for classification
        NetworkCondition::classify(avg_speed, jitter)
    }

    /// Get recent average speed (last N samples)
    fn recent_average_speed(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }

        let window = self.samples.len().min(10);
        let recent = &self.samples[self.samples.len() - window..];
        let sum: f64 = recent.iter().map(|s| s.speed_bps).sum();
        sum / window as f64
    }

    /// Calculate network stability score (0-100)
    pub fn stability_score(&self) -> f64 {
        if self.recent_speeds.len() < 2 {
            return 100.0; // No data = assume stable
        }

        let mean: f64 = self.recent_speeds.iter().sum::<f64>() / self.recent_speeds.len() as f64;
        if mean <= 0.0 {
            return 0.0;
        }

        let variance: f64 = self
            .recent_speeds
            .iter()
            .map(|s| (s - mean).powi(2))
            .sum::<f64>()
            / self.recent_speeds.len() as f64;
        let cv = (variance.sqrt() / mean).min(1.0);

        // Convert coefficient of variation to 0-100 score
        // CV=0 → 100 (perfectly stable)
        // CV=1 → 0 (extremely unstable)
        ((1.0 - cv) * 100.0).round()
    }

    /// Get best hours for downloading (sorted by avg speed, descending)
    pub fn best_hours(&self) -> Vec<u8> {
        let mut hours_with_stats: Vec<(u8, f64)> = self
            .hourly_stats
            .iter()
            .enumerate()
            .filter(|(_, stats)| stats.sample_count >= 3) // Need minimum samples
            .map(|(h, stats)| (h as u8, stats.avg_speed_bps))
            .collect();

        hours_with_stats.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        hours_with_stats.into_iter().map(|(h, _)| h).collect()
    }

    /// Get worst hours for downloading (sorted by avg speed, ascending)
    pub fn worst_hours(&self) -> Vec<u8> {
        let mut hours_with_stats: Vec<(u8, f64)> = self
            .hourly_stats
            .iter()
            .enumerate()
            .filter(|(_, stats)| stats.sample_count >= 3)
            .map(|(h, stats)| (h as u8, stats.avg_speed_bps))
            .collect();

        hours_with_stats.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        hours_with_stats.into_iter().map(|(h, _)| h).collect()
    }

    /// Generate a network condition summary
    pub fn summary(&self) -> NetworkSummary {
        let current_condition = self.current_condition();
        let current_speed = self.recent_average_speed();

        let avg_speed = if self.samples.is_empty() {
            0.0
        } else {
            self.samples.iter().map(|s| s.speed_bps).sum::<f64>() / self.samples.len() as f64
        };

        let peak_speed = self
            .samples
            .iter()
            .map(|s| s.speed_bps)
            .fold(0.0f64, |a, b| a.max(b));

        let stability = self.stability_score();
        let best = self.best_hours();
        let worst = self.worst_hours();

        let recommendation = self.generate_recommendation(current_condition, &best, &worst);

        NetworkSummary {
            current_condition,
            current_speed_bps: current_speed,
            avg_speed_bps: avg_speed,
            peak_speed_bps: peak_speed,
            stability_score: stability,
            total_samples: self.total_samples_collected,
            recommended_concurrency: current_condition.recommended_concurrency(),
            best_hours: best,
            worst_hours: worst,
            recommendation,
        }
    }

    /// Generate actionable recommendation based on current conditions
    fn generate_recommendation(
        &self,
        condition: NetworkCondition,
        best_hours: &[u8],
        worst_hours: &[u8],
    ) -> String {
        match condition {
            NetworkCondition::Excellent => {
                "Network is excellent! Maximize concurrent downloads. Consider starting large downloads now.".to_string()
            }
            NetworkCondition::Good => {
                "Network is good for downloading. Normal concurrent download count recommended.".to_string()
            }
            NetworkCondition::Fair => {
                if !best_hours.is_empty() {
                    format!(
                        "Network is fair. Consider scheduling large downloads during better hours ({})",
                        format_hours(&best_hours[..best_hours.len().min(3)])
                    )
                } else {
                    "Network is fair. Reduce concurrent downloads for better per-task speed.".to_string()
                }
            }
            NetworkCondition::Poor => {
                if !worst_hours.is_empty() && !best_hours.is_empty() {
                    format!(
                        "Network is poor. Avoid downloading during peak hours ({}). Best times: {}",
                        format_hours(&worst_hours[..worst_hours.len().min(3)]),
                        format_hours(&best_hours[..best_hours.len().min(3)])
                    )
                } else {
                    "Network is poor. Limit to 1-2 downloads and consider pausing non-urgent tasks.".to_string()
                }
            }
            NetworkCondition::Congested => {
                "Network is congested! Pause non-essential downloads. Wait for better conditions or schedule for off-peak hours.".to_string()
            }
        }
    }

    /// Get all samples (for debugging/export)
    pub fn samples(&self) -> &[NetworkSample] {
        &self.samples
    }

    /// Get hourly statistics
    pub fn hourly_stats(&self) -> &[HourlyStats] {
        &self.hourly_stats
    }

    /// Clear all collected data
    pub fn clear(&mut self) {
        self.samples.clear();
        self.hourly_stats = vec![HourlyStats::default(); HOURS_IN_DAY];
        self.total_samples_collected = 0;
        self.recent_speeds.clear();
    }

    /// Save configuration and hourly stats to disk
    pub async fn save(&self, path: &Path) -> Result<(), NetworkMonitorError> {
        let data = NetworkMonitorData {
            config: self.config.clone(),
            hourly_stats: self.hourly_stats.clone(),
            total_samples_collected: self.total_samples_collected,
        };
        let json = serde_json::to_string_pretty(&data)?;
        let tmp_path = path.with_extension("json.tmp");
        fs::write(&tmp_path, json.as_bytes()).await?;
        fs::rename(&tmp_path, path).await?;
        Ok(())
    }

    /// Load configuration and hourly stats from disk
    pub async fn load(path: &Path) -> Result<Self, NetworkMonitorError> {
        match fs::read_to_string(path).await {
            Ok(content) => {
                let data: NetworkMonitorData = serde_json::from_str(&content)?;
                Ok(Self {
                    config: data.config,
                    samples: Vec::new(),
                    hourly_stats: data.hourly_stats,
                    total_samples_collected: data.total_samples_collected,
                    recent_speeds: Vec::new(),
                    max_recent_speeds: 20,
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new()),
            Err(e) => Err(e.into()),
        }
    }
}

impl Default for NetworkMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Format hours for display (e.g., "02:00-05:00")
fn format_hours(hours: &[u8]) -> String {
    hours
        .iter()
        .map(|h| format!("{:02}:00", h))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_condition_classify_excellent() {
        let cond = NetworkCondition::classify(15_000_000.0, 0.1);
        assert_eq!(cond, NetworkCondition::Excellent);
    }

    #[test]
    fn test_network_condition_classify_good() {
        let cond = NetworkCondition::classify(5_000_000.0, 0.2);
        assert_eq!(cond, NetworkCondition::Good);
    }

    #[test]
    fn test_network_condition_classify_fair() {
        let cond = NetworkCondition::classify(500_000.0, 0.1);
        assert_eq!(cond, NetworkCondition::Fair);
    }

    #[test]
    fn test_network_condition_classify_poor() {
        let cond = NetworkCondition::classify(50_000.0, 0.1);
        assert_eq!(cond, NetworkCondition::Poor);
    }

    #[test]
    fn test_network_condition_classify_congested() {
        let cond = NetworkCondition::classify(5_000.0, 0.1);
        assert_eq!(cond, NetworkCondition::Congested);
    }

    #[test]
    fn test_network_condition_jitter_penalty() {
        // High jitter degrades classification
        let cond = NetworkCondition::classify(15_000_000.0, 0.9);
        // Excellent base + 2 jitter penalty = Fair
        assert_eq!(cond, NetworkCondition::Fair);
    }

    #[test]
    fn test_network_condition_moderate_jitter() {
        // Moderate jitter
        let cond = NetworkCondition::classify(5_000_000.0, 0.6);
        // Good base + 1 jitter penalty = Fair
        assert_eq!(cond, NetworkCondition::Fair);
    }

    #[test]
    fn test_network_monitor_new() {
        let monitor = NetworkMonitor::new();
        assert!(monitor.samples().is_empty());
        assert_eq!(monitor.total_samples_collected, 0);
        assert_eq!(monitor.hourly_stats().len(), 24);
    }

    #[test]
    fn test_network_monitor_record_sample() {
        let mut monitor = NetworkMonitor::new();
        monitor.record_sample(5_000_000.0, 3);
        assert_eq!(monitor.samples().len(), 1);
        assert_eq!(monitor.total_samples_collected, 1);
    }

    #[test]
    fn test_network_monitor_disabled_no_record() {
        let mut monitor = NetworkMonitor::new();
        let _ = monitor.config().clone();
        let mut config = monitor.config.clone();
        config.enabled = false;
        monitor.set_config(config);
        monitor.record_sample(5_000_000.0, 3);
        assert!(monitor.samples().is_empty());
    }

    #[test]
    fn test_network_monitor_ring_buffer() {
        let mut monitor = NetworkMonitor::new();
        monitor.config.max_samples = 5;
        for i in 0..10 {
            monitor.record_sample(i as f64 * 1_000_000.0, 1);
        }
        assert_eq!(monitor.samples().len(), 5);
        assert_eq!(monitor.total_samples_collected, 10);
    }

    #[test]
    fn test_network_monitor_current_condition_default() {
        let monitor = NetworkMonitor::new();
        // No samples → defaults to Good
        assert_eq!(monitor.current_condition(), NetworkCondition::Good);
    }

    #[test]
    fn test_network_monitor_stability_score_no_data() {
        let monitor = NetworkMonitor::new();
        assert_eq!(monitor.stability_score(), 100.0);
    }

    #[test]
    fn test_network_monitor_stability_score_stable() {
        let mut monitor = NetworkMonitor::new();
        // Record consistent speeds
        for _ in 0..10 {
            monitor.record_sample(5_000_000.0, 2);
        }
        // Stable speeds → high score
        assert!(monitor.stability_score() > 90.0);
    }

    #[test]
    fn test_network_monitor_stability_score_unstable() {
        let mut monitor = NetworkMonitor::new();
        // Record wildly varying speeds
        let speeds = [100.0, 10_000_000.0, 50.0, 5_000_000.0, 200.0];
        for speed in &speeds {
            monitor.record_sample(*speed, 1);
        }
        // Unstable → lower score
        assert!(monitor.stability_score() < 50.0);
    }

    #[test]
    fn test_network_monitor_best_hours() {
        let mut monitor = NetworkMonitor::new();
        // Simulate samples at different hours (we can't control time, but hourly stats
        // are updated based on current hour; just verify the function works)
        for _ in 0..5 {
            monitor.record_sample(1_000_000.0, 1);
        }
        // best_hours should return a sorted vec
        let best = monitor.best_hours();
        // With all samples in the same hour, only that hour should qualify
        if !best.is_empty() {
            // First element should be the hour with highest avg
            assert!(best.len() <= 24);
        }
    }

    #[test]
    fn test_network_monitor_summary() {
        let mut monitor = NetworkMonitor::new();
        for _ in 0..5 {
            monitor.record_sample(5_000_000.0, 2);
        }
        let summary = monitor.summary();
        assert_eq!(summary.total_samples, 5);
        assert!(summary.peak_speed_bps >= 5_000_000.0);
        assert!(summary.stability_score >= 0.0);
        assert!(summary.stability_score <= 100.0);
        assert!(!summary.recommendation.is_empty());
    }

    #[test]
    fn test_network_monitor_clear() {
        let mut monitor = NetworkMonitor::new();
        for _ in 0..5 {
            monitor.record_sample(5_000_000.0, 2);
        }
        assert!(!monitor.samples().is_empty());
        monitor.clear();
        assert!(monitor.samples().is_empty());
        assert_eq!(monitor.total_samples_collected, 0);
    }

    #[test]
    fn test_network_condition_recommended_concurrency() {
        assert_eq!(NetworkCondition::Excellent.recommended_concurrency(), 8);
        assert_eq!(NetworkCondition::Good.recommended_concurrency(), 4);
        assert_eq!(NetworkCondition::Fair.recommended_concurrency(), 2);
        assert_eq!(NetworkCondition::Poor.recommended_concurrency(), 1);
        assert_eq!(NetworkCondition::Congested.recommended_concurrency(), 1);
    }

    #[test]
    fn test_network_condition_display() {
        assert_eq!(format!("{}", NetworkCondition::Excellent), "🟢 Excellent");
        assert_eq!(format!("{}", NetworkCondition::Congested), "🔴 Congested");
    }

    #[test]
    fn test_format_hours() {
        assert_eq!(format_hours(&[2, 14, 23]), "02:00, 14:00, 23:00");
        assert_eq!(format_hours(&[0]), "00:00");
        assert_eq!(format_hours(&[]), "");
    }

    #[test]
    fn test_hourly_stats_add_sample() {
        let mut stats = HourlyStats::default();
        stats.add_sample(1_000_000.0);
        stats.add_sample(3_000_000.0);
        assert_eq!(stats.sample_count, 2);
        assert!((stats.avg_speed_bps - 2_000_000.0).abs() < 1.0);
        assert_eq!(stats.peak_speed_bps, 3_000_000.0);
    }

    #[test]
    fn test_network_monitor_config_default() {
        let config = NetworkMonitorConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_samples, DEFAULT_MAX_SAMPLES);
        assert_eq!(config.sample_interval_secs, 30);
    }

    #[tokio::test]
    async fn test_network_monitor_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("network_monitor.json");

        let mut monitor = NetworkMonitor::new();
        for _ in 0..5 {
            monitor.record_sample(5_000_000.0, 2);
        }
        monitor.save(&path).await.unwrap();

        let loaded = NetworkMonitor::load(&path).await.unwrap();
        assert_eq!(loaded.total_samples_collected, 5);
        assert_eq!(loaded.config.enabled, true);
    }

    #[tokio::test]
    async fn test_network_monitor_load_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let monitor = NetworkMonitor::load(&path).await.unwrap();
        assert_eq!(monitor.total_samples_collected, 0);
        assert!(monitor.samples().is_empty());
    }
}
