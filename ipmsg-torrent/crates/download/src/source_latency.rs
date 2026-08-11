//! Download Source Latency Monitor
//!
//! Tracks connection latency to download domains for smarter source selection.
//! Measures TCP connect time and HTTP HEAD request latency per domain.
//!
//! Features:
//! - Per-domain latency tracking with EMA smoothing
//! - Latency percentile estimation (p50, p90, p99)
//! - Domain health classification based on latency thresholds
//! - Automatic latency decay over time (staleness penalty)
//! - Configurable sample window and decay rate
//! - Integration with DownloadManager for source selection
//! - Persistent storage to JSON

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Configuration for source latency monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceLatencyConfig {
    /// Enable latency monitoring.
    pub enabled: bool,
    /// Maximum samples to keep per domain.
    pub max_samples_per_domain: usize,
    /// EMA smoothing factor (0.0-1.0), higher = more weight on recent.
    pub ema_alpha: f64,
    /// Latency thresholds in milliseconds for health classification.
    pub excellent_threshold_ms: f64,
    pub good_threshold_ms: f64,
    pub fair_threshold_ms: f64,
    pub poor_threshold_ms: f64,
    /// Decay factor per hour (0.0-1.0), applied when no new samples.
    pub hourly_decay_factor: f64,
    /// Domains to ignore (e.g., localhost).
    pub ignored_domains: Vec<String>,
    /// Enable percentile estimation.
    pub enable_percentiles: bool,
}

impl Default for SourceLatencyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_samples_per_domain: 50,
            ema_alpha: 0.3,
            excellent_threshold_ms: 50.0,
            good_threshold_ms: 150.0,
            fair_threshold_ms: 500.0,
            poor_threshold_ms: 1000.0,
            hourly_decay_factor: 0.95,
            ignored_domains: vec!["localhost".to_string()],
            enable_percentiles: true,
        }
    }
}

/// Health classification based on latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyHealth {
    /// < excellent_threshold_ms
    Excellent,
    /// < good_threshold_ms
    Good,
    /// < fair_threshold_ms
    Fair,
    /// < poor_threshold_ms
    Poor,
    /// >= poor_threshold_ms
    Unreachable,
}

impl LatencyHealth {
    pub fn emoji(&self) -> &'static str {
        match self {
            Self::Excellent => "🟢",
            Self::Good => "🟡",
            Self::Fair => "🟠",
            Self::Poor => "🔴",
            Self::Unreachable => "⚫",
        }
    }
}

impl std::fmt::Display for LatencyHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Excellent => write!(f, "Excellent"),
            Self::Good => write!(f, "Good"),
            Self::Fair => write!(f, "Fair"),
            Self::Poor => write!(f, "Poor"),
            Self::Unreachable => write!(f, "Unreachable"),
        }
    }
}

/// A single latency sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySample {
    /// When the sample was taken.
    pub timestamp: DateTime<Utc>,
    /// Latency in milliseconds.
    pub latency_ms: f64,
    /// Whether the connection succeeded.
    pub success: bool,
    /// Optional error message if failed.
    pub error: Option<String>,
}

/// Per-domain latency statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainLatencyStats {
    /// Domain name.
    pub domain: String,
    /// Exponential moving average latency in ms.
    pub ema_latency_ms: f64,
    /// Minimum observed latency in ms.
    pub min_latency_ms: f64,
    /// Maximum observed latency in ms.
    pub max_latency_ms: f64,
    /// Total samples collected.
    pub total_samples: u64,
    /// Successful connections.
    pub successful_connections: u64,
    /// Failed connections.
    pub failed_connections: u64,
    /// Consecutive failures.
    pub consecutive_failures: u32,
    /// Last successful connection time.
    pub last_success_at: Option<DateTime<Utc>>,
    /// Last sample time.
    pub last_sample_at: Option<DateTime<Utc>>,
    /// Current health classification.
    pub health: LatencyHealth,
    /// Recent latency samples (for percentile calculation).
    pub recent_samples: Vec<f64>,
    /// Estimated p50 latency (median).
    pub p50_ms: Option<f64>,
    /// Estimated p90 latency.
    pub p90_ms: Option<f64>,
    /// Estimated p99 latency.
    pub p99_ms: Option<f64>,
}

impl DomainLatencyStats {
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            ema_latency_ms: 0.0,
            min_latency_ms: f64::MAX,
            max_latency_ms: 0.0,
            total_samples: 0,
            successful_connections: 0,
            failed_connections: 0,
            consecutive_failures: 0,
            last_success_at: None,
            last_sample_at: None,
            health: LatencyHealth::Excellent,
            recent_samples: Vec::new(),
            p50_ms: None,
            p90_ms: None,
            p99_ms: None,
        }
    }

    /// Add a latency sample and update statistics.
    pub fn add_sample(&mut self, sample: LatencySample, config: &SourceLatencyConfig) {
        self.total_samples += 1;
        self.last_sample_at = Some(sample.timestamp);

        if sample.success {
            self.successful_connections += 1;
            self.consecutive_failures = 0;
            self.last_success_at = Some(sample.timestamp);

            let latency = sample.latency_ms;

            // Update EMA
            if self.ema_latency_ms == 0.0 {
                self.ema_latency_ms = latency;
            } else {
                self.ema_latency_ms =
                    config.ema_alpha * latency + (1.0 - config.ema_alpha) * self.ema_latency_ms;
            }

            // Update min/max
            self.min_latency_ms = self.min_latency_ms.min(latency);
            self.max_latency_ms = self.max_latency_ms.max(latency);

            // Track recent samples for percentile estimation
            self.recent_samples.push(latency);
            if self.recent_samples.len() > config.max_samples_per_domain {
                self.recent_samples.remove(0);
            }

            // Update percentiles if enabled
            if config.enable_percentiles && self.recent_samples.len() >= 3 {
                self.update_percentiles();
            }
        } else {
            self.failed_connections += 1;
            self.consecutive_failures += 1;
        }

        // Update health classification
        self.update_health(config);
    }

    /// Update health classification based on current EMA latency.
    fn update_health(&mut self, config: &SourceLatencyConfig) {
        if self.consecutive_failures >= 3 {
            self.health = LatencyHealth::Unreachable;
        } else if self.ema_latency_ms < config.excellent_threshold_ms {
            self.health = LatencyHealth::Excellent;
        } else if self.ema_latency_ms < config.good_threshold_ms {
            self.health = LatencyHealth::Good;
        } else if self.ema_latency_ms < config.fair_threshold_ms {
            self.health = LatencyHealth::Fair;
        } else if self.ema_latency_ms < config.poor_threshold_ms {
            self.health = LatencyHealth::Poor;
        } else {
            self.health = LatencyHealth::Unreachable;
        }
    }

    /// Update percentile estimates from recent samples.
    fn update_percentiles(&mut self) {
        if self.recent_samples.is_empty() {
            return;
        }

        let mut sorted = self.recent_samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let len = sorted.len();
        self.p50_ms = Some(sorted[len / 2]);
        self.p90_ms = Some(sorted[(len as f64 * 0.9) as usize].min(sorted[len - 1]));
        self.p99_ms = Some(sorted[(len as f64 * 0.99) as usize].min(sorted[len - 1]));
    }

    /// Apply time-based decay to EMA latency (called periodically).
    pub fn apply_decay(&mut self, hours_elapsed: f64, decay_factor: f64) {
        if hours_elapsed > 0.0 && self.ema_latency_ms > 0.0 {
            // Decay EMA towards a baseline (assume 1000ms if no recent data)
            let baseline = 1000.0;
            let decay = decay_factor.powf(hours_elapsed);
            self.ema_latency_ms = baseline + (self.ema_latency_ms - baseline) * decay;
        }
    }
}

/// Summary of source latency monitoring across all domains.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceLatencySummary {
    /// Total domains tracked.
    pub total_domains: usize,
    /// Domains by health classification.
    pub excellent_count: usize,
    pub good_count: usize,
    pub fair_count: usize,
    pub poor_count: usize,
    pub unreachable_count: usize,
    /// Overall average latency across all domains.
    pub overall_avg_latency_ms: f64,
    /// Top 3 fastest domains.
    pub fastest_domains: Vec<(String, f64)>,
    /// Top 3 slowest domains.
    pub slowest_domains: Vec<(String, f64)>,
    /// Total samples collected.
    pub total_samples: u64,
}

/// Source latency monitor.
pub struct SourceLatencyMonitor {
    config: SourceLatencyConfig,
    domain_stats: HashMap<String, DomainLatencyStats>,
    last_decay_check: DateTime<Utc>,
}

impl SourceLatencyMonitor {
    pub fn new() -> Self {
        Self {
            config: SourceLatencyConfig::default(),
            domain_stats: HashMap::new(),
            last_decay_check: Utc::now(),
        }
    }

    pub fn with_config(config: SourceLatencyConfig) -> Self {
        Self {
            config,
            domain_stats: HashMap::new(),
            last_decay_check: Utc::now(),
        }
    }

    /// Get current configuration.
    pub fn config(&self) -> &SourceLatencyConfig {
        &self.config
    }

    /// Update configuration.
    pub fn set_config(&mut self, config: SourceLatencyConfig) {
        self.config = config;
    }

    /// Record a latency sample for a domain.
    pub fn record_latency(
        &mut self,
        domain: &str,
        latency_ms: f64,
        success: bool,
        error: Option<String>,
    ) {
        if !self.config.enabled {
            return;
        }

        // Skip ignored domains
        if self.config.ignored_domains.iter().any(|d| d == domain) {
            return;
        }

        let sample = LatencySample {
            timestamp: Utc::now(),
            latency_ms,
            success,
            error,
        };

        let stats = self
            .domain_stats
            .entry(domain.to_string())
            .or_insert_with(|| DomainLatencyStats::new(domain.to_string()));

        stats.add_sample(sample, &self.config);
    }

    /// Record a successful connection.
    pub fn record_success(&mut self, domain: &str, latency_ms: f64) {
        self.record_latency(domain, latency_ms, true, None);
    }

    /// Record a failed connection.
    pub fn record_failure(&mut self, domain: &str, error: String) {
        self.record_latency(domain, 0.0, false, Some(error));
    }

    /// Get statistics for a specific domain.
    pub fn get_domain_stats(&self, domain: &str) -> Option<&DomainLatencyStats> {
        self.domain_stats.get(domain)
    }

    /// Get statistics for all domains.
    pub fn get_all_stats(&self) -> &HashMap<String, DomainLatencyStats> {
        &self.domain_stats
    }

    /// Get a summary of source latency monitoring.
    pub fn get_summary(&self) -> SourceLatencySummary {
        let mut excellent = 0;
        let mut good = 0;
        let mut fair = 0;
        let mut poor = 0;
        let mut unreachable = 0;
        let mut total_latency = 0.0;
        let mut total_samples = 0u64;
        let mut domain_latencies: Vec<(String, f64)> = Vec::new();

        for stats in self.domain_stats.values() {
            match stats.health {
                LatencyHealth::Excellent => excellent += 1,
                LatencyHealth::Good => good += 1,
                LatencyHealth::Fair => fair += 1,
                LatencyHealth::Poor => poor += 1,
                LatencyHealth::Unreachable => unreachable += 1,
            }

            if stats.ema_latency_ms > 0.0 {
                total_latency += stats.ema_latency_ms;
                domain_latencies.push((stats.domain.clone(), stats.ema_latency_ms));
            }

            total_samples += stats.total_samples;
        }

        // Sort by latency for fastest/slowest
        domain_latencies.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let fastest = domain_latencies.iter().take(3).cloned().collect();
        let slowest = domain_latencies.iter().rev().take(3).cloned().collect();

        let overall_avg = if !domain_latencies.is_empty() {
            total_latency / domain_latencies.len() as f64
        } else {
            0.0
        };

        SourceLatencySummary {
            total_domains: self.domain_stats.len(),
            excellent_count: excellent,
            good_count: good,
            fair_count: fair,
            poor_count: poor,
            unreachable_count: unreachable,
            overall_avg_latency_ms: overall_avg,
            fastest_domains: fastest,
            slowest_domains: slowest,
            total_samples,
        }
    }

    /// Apply time-based decay to all domains (call periodically).
    pub fn apply_periodic_decay(&mut self) {
        let now = Utc::now();
        let hours_elapsed = (now - self.last_decay_check).num_seconds() as f64 / 3600.0;

        if hours_elapsed >= 1.0 {
            let decay_factor = self.config.hourly_decay_factor;
            for stats in self.domain_stats.values_mut() {
                stats.apply_decay(hours_elapsed, decay_factor);
                stats.update_health(&self.config);
            }
            self.last_decay_check = now;
        }
    }

    /// Clear statistics for a specific domain.
    pub fn clear_domain(&mut self, domain: &str) {
        self.domain_stats.remove(domain);
    }

    /// Clear all statistics.
    pub fn clear_all(&mut self) {
        self.domain_stats.clear();
        self.last_decay_check = Utc::now();
    }

    /// Get the best domain for a new download (lowest latency).
    pub fn get_best_domain(&self) -> Option<&str> {
        self.domain_stats
            .values()
            .filter(|s| s.health != LatencyHealth::Unreachable && s.ema_latency_ms > 0.0)
            .min_by(|a, b| {
                a.ema_latency_ms
                    .partial_cmp(&b.ema_latency_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|s| s.domain.as_str())
    }

    /// Rank domains by latency (best to worst).
    pub fn rank_domains(&self) -> Vec<(&str, f64, LatencyHealth)> {
        let mut ranked: Vec<_> = self
            .domain_stats
            .values()
            .filter(|s| s.ema_latency_ms > 0.0)
            .map(|s| (s.domain.as_str(), s.ema_latency_ms, s.health))
            .collect();

        ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked
    }

    /// Save configuration to disk.
    pub async fn save_config(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self.config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        tokio::fs::write(path, json).await
    }

    /// Load configuration from disk.
    pub async fn load_config(path: &Path) -> std::io::Result<SourceLatencyConfig> {
        let json = tokio::fs::read_to_string(path).await?;
        serde_json::from_str(&json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Save domain statistics to disk.
    pub async fn save_stats(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self.domain_stats)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        tokio::fs::write(path, json).await
    }

    /// Load domain statistics from disk.
    pub async fn load_stats(path: &Path) -> std::io::Result<HashMap<String, DomainLatencyStats>> {
        let json = tokio::fs::read_to_string(path).await?;
        serde_json::from_str(&json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Format a human-readable summary.
    pub fn format_summary(&self, summary: &SourceLatencySummary) -> String {
        let mut output = String::new();
        output.push_str("📡 Source Latency Monitor Summary\n");
        output.push_str(&format!("  Total Domains: {}\n", summary.total_domains));
        output.push_str(&format!(
            "  Health: {} Excellent, {} Good, {} Fair, {} Poor, {} Unreachable\n",
            summary.excellent_count,
            summary.good_count,
            summary.fair_count,
            summary.poor_count,
            summary.unreachable_count
        ));
        output.push_str(&format!(
            "  Overall Avg Latency: {:.1} ms\n",
            summary.overall_avg_latency_ms
        ));
        output.push_str(&format!("  Total Samples: {}\n", summary.total_samples));

        if !summary.fastest_domains.is_empty() {
            output.push_str("\n  🏆 Fastest Domains:\n");
            for (domain, latency) in &summary.fastest_domains {
                output.push_str(&format!("    {} {:.1} ms\n", domain, latency));
            }
        }

        if !summary.slowest_domains.is_empty() {
            output.push_str("\n  🐌 Slowest Domains:\n");
            for (domain, latency) in &summary.slowest_domains {
                output.push_str(&format!("    {} {:.1} ms\n", domain, latency));
            }
        }

        output
    }
}

impl Default for SourceLatencyMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Type alias for the shared monitor.
pub type SourceLatencyMonitorRef = Arc<Mutex<SourceLatencyMonitor>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = SourceLatencyConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_samples_per_domain, 50);
        assert_eq!(config.excellent_threshold_ms, 50.0);
    }

    #[test]
    fn test_latency_health_display() {
        assert_eq!(format!("{}", LatencyHealth::Excellent), "Excellent");
        assert_eq!(LatencyHealth::Good.emoji(), "🟡");
    }

    #[test]
    fn test_domain_stats_new() {
        let stats = DomainLatencyStats::new("example.com".to_string());
        assert_eq!(stats.domain, "example.com");
        assert_eq!(stats.total_samples, 0);
        assert_eq!(stats.ema_latency_ms, 0.0);
    }

    #[test]
    fn test_add_sample_success() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        let sample = LatencySample {
            timestamp: Utc::now(),
            latency_ms: 100.0,
            success: true,
            error: None,
        };

        stats.add_sample(sample, &config);
        assert_eq!(stats.total_samples, 1);
        assert_eq!(stats.successful_connections, 1);
        assert_eq!(stats.failed_connections, 0);
        assert_eq!(stats.ema_latency_ms, 100.0);
        assert_eq!(stats.min_latency_ms, 100.0);
        assert_eq!(stats.max_latency_ms, 100.0);
    }

    #[test]
    fn test_add_sample_failure() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        let sample = LatencySample {
            timestamp: Utc::now(),
            latency_ms: 0.0,
            success: false,
            error: Some("Connection timeout".to_string()),
        };

        stats.add_sample(sample, &config);
        assert_eq!(stats.total_samples, 1);
        assert_eq!(stats.successful_connections, 0);
        assert_eq!(stats.failed_connections, 1);
        assert_eq!(stats.consecutive_failures, 1);
    }

    #[test]
    fn test_ema_calculation() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig {
            ema_alpha: 0.5,
            ..Default::default()
        };

        // First sample
        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 100.0,
                success: true,
                error: None,
            },
            &config,
        );
        assert_eq!(stats.ema_latency_ms, 100.0);

        // Second sample (EMA = 0.5 * 200 + 0.5 * 100 = 150)
        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 200.0,
                success: true,
                error: None,
            },
            &config,
        );
        assert_eq!(stats.ema_latency_ms, 150.0);
    }

    #[test]
    fn test_health_classification() {
        let config = SourceLatencyConfig::default();

        let mut stats = DomainLatencyStats::new("example.com".to_string());
        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 30.0,
                success: true,
                error: None,
            },
            &config,
        );
        assert_eq!(stats.health, LatencyHealth::Excellent);

        stats.ema_latency_ms = 100.0;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Good);

        stats.ema_latency_ms = 300.0;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Fair);

        stats.ema_latency_ms = 800.0;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Poor);

        stats.ema_latency_ms = 1500.0;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Unreachable);
    }

    #[test]
    fn test_consecutive_failures_unreachable() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        for _ in 0..3 {
            stats.add_sample(
                LatencySample {
                    timestamp: Utc::now(),
                    latency_ms: 0.0,
                    success: false,
                    error: Some("Timeout".to_string()),
                },
                &config,
            );
        }

        assert_eq!(stats.health, LatencyHealth::Unreachable);
        assert_eq!(stats.consecutive_failures, 3);
    }

    #[test]
    fn test_percentile_calculation() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        for latency in [50.0, 100.0, 150.0, 200.0, 250.0] {
            stats.add_sample(
                LatencySample {
                    timestamp: Utc::now(),
                    latency_ms: latency,
                    success: true,
                    error: None,
                },
                &config,
            );
        }

        assert!(stats.p50_ms.is_some());
        assert!(stats.p90_ms.is_some());
        assert!(stats.p99_ms.is_some());
        assert_eq!(stats.p50_ms.unwrap(), 150.0);
    }

    #[test]
    fn test_monitor_record_success() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.total_samples, 1);
        assert_eq!(stats.successful_connections, 1);
    }

    #[test]
    fn test_monitor_record_failure() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_failure("example.com", "Timeout".to_string());

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.failed_connections, 1);
        assert_eq!(stats.consecutive_failures, 1);
    }

    #[test]
    fn test_monitor_ignored_domains() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("localhost", 10.0);

        assert!(monitor.get_domain_stats("localhost").is_none());
    }

    #[test]
    fn test_monitor_disabled() {
        let mut monitor = SourceLatencyMonitor::with_config(SourceLatencyConfig {
            enabled: false,
            ..Default::default()
        });

        monitor.record_success("example.com", 100.0);
        assert!(monitor.get_domain_stats("example.com").is_none());
    }

    #[test]
    fn test_get_summary() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("fast.com", 30.0);
        monitor.record_success("medium.com", 200.0);
        monitor.record_success("slow.com", 800.0);

        let summary = monitor.get_summary();
        assert_eq!(summary.total_domains, 3);
        assert_eq!(summary.excellent_count, 1);
        assert_eq!(summary.good_count, 1);
        assert_eq!(summary.poor_count, 1);
        assert_eq!(summary.total_samples, 3);
    }

    #[test]
    fn test_get_best_domain() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("slow.com", 500.0);
        monitor.record_success("fast.com", 50.0);
        monitor.record_success("medium.com", 200.0);

        let best = monitor.get_best_domain();
        assert_eq!(best, Some("fast.com"));
    }

    #[test]
    fn test_rank_domains() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("slow.com", 500.0);
        monitor.record_success("fast.com", 50.0);
        monitor.record_success("medium.com", 200.0);

        let ranked = monitor.rank_domains();
        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].0, "fast.com");
        assert_eq!(ranked[1].0, "medium.com");
        assert_eq!(ranked[2].0, "slow.com");
    }

    #[test]
    fn test_clear_domain() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);
        assert!(monitor.get_domain_stats("example.com").is_some());

        monitor.clear_domain("example.com");
        assert!(monitor.get_domain_stats("example.com").is_none());
    }

    #[test]
    fn test_clear_all() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("a.com", 100.0);
        monitor.record_success("b.com", 200.0);

        monitor.clear_all();
        assert_eq!(monitor.get_all_stats().len(), 0);
    }

    #[test]
    fn test_decay() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        stats.ema_latency_ms = 200.0;

        // Apply 1 hour of decay with factor 0.95
        stats.apply_decay(1.0, 0.95);

        // EMA should move towards baseline (1000ms)
        // New EMA = 1000 + (200 - 1000) * 0.95 = 1000 - 760 * 0.95 = 1000 - 722 = 278
        assert!(stats.ema_latency_ms > 200.0);
        assert!(stats.ema_latency_ms < 1000.0);
    }

    #[test]
    fn test_format_summary() {
        let monitor = SourceLatencyMonitor::new();
        let summary = monitor.get_summary();
        let formatted = monitor.format_summary(&summary);

        assert!(formatted.contains("Source Latency Monitor Summary"));
        assert!(formatted.contains("Total Domains"));
    }

    #[tokio::test]
    async fn test_save_load_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("latency_config.json");

        let monitor = SourceLatencyMonitor::new();
        monitor.save_config(&config_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_config(&config_path)
            .await
            .unwrap();
        assert_eq!(loaded.enabled, monitor.config().enabled);
    }

    #[tokio::test]
    async fn test_save_load_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("latency_stats.json");

        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);
        monitor.save_stats(&stats_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_stats(&stats_path).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains_key("example.com"));
    }
}
