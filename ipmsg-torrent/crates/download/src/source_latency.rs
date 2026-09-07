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

    /// Get mutable statistics for all domains (for loading from disk).
    pub fn get_all_stats_mut(&mut self) -> &mut HashMap<String, DomainLatencyStats> {
        &mut self.domain_stats
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
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
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
        let json =
            serde_json::to_string_pretty(&self.domain_stats).map_err(std::io::Error::other)?;
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

    // ========== SourceLatencyConfig serde ==========

    #[test]
    fn config_serde_roundtrip_default() {
        let config = SourceLatencyConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SourceLatencyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.max_samples_per_domain, config.max_samples_per_domain);
        assert!((loaded.ema_alpha - config.ema_alpha).abs() < 1e-10);
    }

    #[test]
    fn config_serde_roundtrip_custom() {
        let config = SourceLatencyConfig {
            enabled: false,
            max_samples_per_domain: 100,
            ema_alpha: 0.7,
            excellent_threshold_ms: 25.0,
            good_threshold_ms: 75.0,
            fair_threshold_ms: 250.0,
            poor_threshold_ms: 500.0,
            hourly_decay_factor: 0.9,
            ignored_domains: vec!["localhost".into(), "127.0.0.1".into()],
            enable_percentiles: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SourceLatencyConfig = serde_json::from_str(&json).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.max_samples_per_domain, 100);
        assert_eq!(loaded.ignored_domains.len(), 2);
        assert!(!loaded.enable_percentiles);
    }

    #[test]
    fn config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"max_samples_per_domain":50,"ema_alpha":0.3,"excellent_threshold_ms":50.0,"good_threshold_ms":150.0,"fair_threshold_ms":500.0,"poor_threshold_ms":1000.0,"hourly_decay_factor":0.95,"ignored_domains":[],"enable_percentiles":true,"extra_field":"ignored"}"#;
        let loaded: SourceLatencyConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
    }

    #[test]
    fn config_serde_pretty_json() {
        let config = SourceLatencyConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let loaded: SourceLatencyConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
    }

    #[test]
    fn config_default_values() {
        let config = SourceLatencyConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_samples_per_domain, 50);
        assert!((config.ema_alpha - 0.3).abs() < 1e-10);
        assert!((config.excellent_threshold_ms - 50.0).abs() < 1e-10);
        assert!((config.good_threshold_ms - 150.0).abs() < 1e-10);
        assert!((config.fair_threshold_ms - 500.0).abs() < 1e-10);
        assert!((config.poor_threshold_ms - 1000.0).abs() < 1e-10);
        assert!((config.hourly_decay_factor - 0.95).abs() < 1e-10);
        assert_eq!(config.ignored_domains, vec!["localhost"]);
        assert!(config.enable_percentiles);
    }

    // ========== SourceLatencyConfig traits ==========

    #[test]
    fn config_clone() {
        let config = SourceLatencyConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.max_samples_per_domain, config.max_samples_per_domain);
    }

    #[test]
    fn config_clone_independence() {
        let mut config = SourceLatencyConfig::default();
        let cloned = config.clone();
        config.enabled = false;
        assert!(cloned.enabled);
    }

    #[test]
    fn config_debug() {
        let config = SourceLatencyConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("SourceLatencyConfig"));
        assert!(debug.contains("enabled"));
    }

    // ========== LatencyHealth ==========

    #[test]
    fn health_display_all_variants() {
        assert_eq!(format!("{}", LatencyHealth::Excellent), "Excellent");
        assert_eq!(format!("{}", LatencyHealth::Good), "Good");
        assert_eq!(format!("{}", LatencyHealth::Fair), "Fair");
        assert_eq!(format!("{}", LatencyHealth::Poor), "Poor");
        assert_eq!(format!("{}", LatencyHealth::Unreachable), "Unreachable");
    }

    #[test]
    fn health_emoji_all_variants() {
        assert_eq!(LatencyHealth::Excellent.emoji(), "🟢");
        assert_eq!(LatencyHealth::Good.emoji(), "🟡");
        assert_eq!(LatencyHealth::Fair.emoji(), "🟠");
        assert_eq!(LatencyHealth::Poor.emoji(), "🔴");
        assert_eq!(LatencyHealth::Unreachable.emoji(), "⚫");
    }

    #[test]
    fn health_serde_roundtrip_all_variants() {
        for health in [
            LatencyHealth::Excellent,
            LatencyHealth::Good,
            LatencyHealth::Fair,
            LatencyHealth::Poor,
            LatencyHealth::Unreachable,
        ] {
            let json = serde_json::to_string(&health).unwrap();
            let loaded: LatencyHealth = serde_json::from_str(&json).unwrap();
            assert_eq!(loaded, health);
        }
    }

    #[test]
    fn health_serde_snake_case_values() {
        let json = serde_json::to_string(&LatencyHealth::Excellent).unwrap();
        assert_eq!(json, "\"excellent\"");
        let json = serde_json::to_string(&LatencyHealth::Unreachable).unwrap();
        assert_eq!(json, "\"unreachable\"");
    }

    #[test]
    fn health_traits_clone_copy_debug_eq() {
        let h = LatencyHealth::Excellent;
        let h2 = h; // Copy
        let h3 = h.clone();
        assert_eq!(h, h2);
        assert_eq!(h, h3);
        let _ = format!("{:?}", h);
    }

    // ========== LatencySample ==========

    #[test]
    fn sample_serde_roundtrip_success() {
        let sample = LatencySample {
            timestamp: Utc::now(),
            latency_ms: 123.45,
            success: true,
            error: None,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let loaded: LatencySample = serde_json::from_str(&json).unwrap();
        assert!((loaded.latency_ms - 123.45).abs() < 1e-10);
        assert!(loaded.success);
        assert!(loaded.error.is_none());
    }

    #[test]
    fn sample_serde_roundtrip_failure() {
        let sample = LatencySample {
            timestamp: Utc::now(),
            latency_ms: 0.0,
            success: false,
            error: Some("Connection refused".to_string()),
        };
        let json = serde_json::to_string(&sample).unwrap();
        let loaded: LatencySample = serde_json::from_str(&json).unwrap();
        assert!(!loaded.success);
        assert_eq!(loaded.error.as_deref(), Some("Connection refused"));
    }

    #[test]
    fn sample_serde_unicode() {
        let sample = LatencySample {
            timestamp: Utc::now(),
            latency_ms: 100.0,
            success: false,
            error: Some("连接超时".to_string()),
        };
        let json = serde_json::to_string(&sample).unwrap();
        let loaded: LatencySample = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.error.as_deref(), Some("连接超时"));
    }

    #[test]
    fn sample_clone_debug() {
        let sample = LatencySample {
            timestamp: Utc::now(),
            latency_ms: 50.0,
            success: true,
            error: None,
        };
        let cloned = sample.clone();
        assert!((cloned.latency_ms - 50.0).abs() < 1e-10);
        let _ = format!("{:?}", sample);
    }

    // ========== DomainLatencyStats ==========

    #[test]
    fn stats_new_fields() {
        let stats = DomainLatencyStats::new("example.com".to_string());
        assert_eq!(stats.domain, "example.com");
        assert_eq!(stats.total_samples, 0);
        assert_eq!(stats.successful_connections, 0);
        assert_eq!(stats.failed_connections, 0);
        assert_eq!(stats.consecutive_failures, 0);
        assert_eq!(stats.ema_latency_ms, 0.0);
        assert_eq!(stats.min_latency_ms, f64::MAX);
        assert_eq!(stats.max_latency_ms, 0.0);
        assert!(stats.last_success_at.is_none());
        assert!(stats.last_sample_at.is_none());
        assert_eq!(stats.health, LatencyHealth::Excellent);
        assert!(stats.recent_samples.is_empty());
        assert!(stats.p50_ms.is_none());
        assert!(stats.p90_ms.is_none());
        assert!(stats.p99_ms.is_none());
    }

    #[test]
    fn stats_serde_roundtrip() {
        let mut stats = DomainLatencyStats::new("test.com".to_string());
        let config = SourceLatencyConfig::default();
        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 100.0,
                success: true,
                error: None,
            },
            &config,
        );
        let json = serde_json::to_string(&stats).unwrap();
        let loaded: DomainLatencyStats = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.domain, "test.com");
        assert_eq!(loaded.total_samples, 1);
    }

    #[test]
    fn stats_clone_debug() {
        let stats = DomainLatencyStats::new("example.com".to_string());
        let cloned = stats.clone();
        assert_eq!(cloned.domain, stats.domain);
        let _ = format!("{:?}", stats);
    }

    #[test]
    fn stats_add_sample_success_updates_all() {
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
        assert!(stats.last_success_at.is_some());
        assert!(stats.last_sample_at.is_some());
        assert_eq!(stats.consecutive_failures, 0);
    }

    #[test]
    fn stats_add_sample_failure() {
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
        assert!(stats.last_success_at.is_none());
    }

    #[test]
    fn stats_ema_calculation() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig {
            ema_alpha: 0.5,
            ..Default::default()
        };

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

        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 200.0,
                success: true,
                error: None,
            },
            &config,
        );
        // EMA = 0.5 * 200 + 0.5 * 100 = 150
        assert_eq!(stats.ema_latency_ms, 150.0);
    }

    #[test]
    fn stats_ema_first_sample_sets_directly() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig {
            ema_alpha: 0.1,
            ..Default::default()
        };

        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 500.0,
                success: true,
                error: None,
            },
            &config,
        );
        // First sample: EMA = latency directly
        assert_eq!(stats.ema_latency_ms, 500.0);
    }

    #[test]
    fn stats_min_max_tracking() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        for latency in [100.0, 50.0, 200.0, 30.0, 150.0] {
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

        assert_eq!(stats.min_latency_ms, 30.0);
        assert_eq!(stats.max_latency_ms, 200.0);
    }

    #[test]
    fn stats_consecutive_failures_reset_on_success() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        for _ in 0..2 {
            stats.add_sample(
                LatencySample {
                    timestamp: Utc::now(),
                    latency_ms: 0.0,
                    success: false,
                    error: Some("err".to_string()),
                },
                &config,
            );
        }
        assert_eq!(stats.consecutive_failures, 2);

        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 100.0,
                success: true,
                error: None,
            },
            &config,
        );
        assert_eq!(stats.consecutive_failures, 0);
    }

    #[test]
    fn stats_health_classification_boundaries() {
        let config = SourceLatencyConfig::default();

        // Excellent: < 50ms
        let mut stats = DomainLatencyStats::new("x".to_string());
        stats.ema_latency_ms = 49.9;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Excellent);

        // Good: < 150ms
        stats.ema_latency_ms = 50.0;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Good);

        stats.ema_latency_ms = 149.9;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Good);

        // Fair: < 500ms
        stats.ema_latency_ms = 150.0;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Fair);

        stats.ema_latency_ms = 499.9;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Fair);

        // Poor: < 1000ms
        stats.ema_latency_ms = 500.0;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Poor);

        stats.ema_latency_ms = 999.9;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Poor);

        // Unreachable: >= 1000ms
        stats.ema_latency_ms = 1000.0;
        stats.update_health(&config);
        assert_eq!(stats.health, LatencyHealth::Unreachable);
    }

    #[test]
    fn stats_consecutive_failures_unreachable() {
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
    fn stats_consecutive_failures_2_not_unreachable() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        for _ in 0..2 {
            stats.add_sample(
                LatencySample {
                    timestamp: Utc::now(),
                    latency_ms: 0.0,
                    success: false,
                    error: Some("err".to_string()),
                },
                &config,
            );
        }

        assert_ne!(stats.health, LatencyHealth::Unreachable);
    }

    #[test]
    fn stats_percentile_calculation_3_samples() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        for latency in [100.0, 200.0, 300.0] {
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
        // sorted: [100, 200, 300], len=3, p50 index=1 => 200
        assert_eq!(stats.p50_ms.unwrap(), 200.0);
    }

    #[test]
    fn stats_percentile_calculation_5_samples() {
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

        assert_eq!(stats.p50_ms.unwrap(), 150.0);
    }

    #[test]
    fn stats_percentile_not_updated_when_disabled() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig {
            enable_percentiles: false,
            ..Default::default()
        };

        for latency in [100.0, 200.0, 300.0] {
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

        assert!(stats.p50_ms.is_none());
    }

    #[test]
    fn stats_percentile_requires_3_samples() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig::default();

        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 100.0,
                success: true,
                error: None,
            },
            &config,
        );
        assert!(stats.p50_ms.is_none());

        stats.add_sample(
            LatencySample {
                timestamp: Utc::now(),
                latency_ms: 200.0,
                success: true,
                error: None,
            },
            &config,
        );
        assert!(stats.p50_ms.is_none());
    }

    #[test]
    fn stats_recent_samples_capped() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        let config = SourceLatencyConfig {
            max_samples_per_domain: 5,
            ..Default::default()
        };

        for i in 0..10 {
            stats.add_sample(
                LatencySample {
                    timestamp: Utc::now(),
                    latency_ms: (i as f64) * 10.0,
                    success: true,
                    error: None,
                },
                &config,
            );
        }

        assert!(stats.recent_samples.len() <= 5);
    }

    #[test]
    fn stats_apply_decay_moves_towards_baseline() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        stats.ema_latency_ms = 200.0;

        stats.apply_decay(1.0, 0.95);
        // EMA should move towards baseline (1000ms)
        // 1000 + (200 - 1000) * 0.95 = 1000 - 722 = 278
        assert!(stats.ema_latency_ms > 200.0);
        assert!(stats.ema_latency_ms < 1000.0);
    }

    #[test]
    fn stats_apply_decay_zero_hours_no_change() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        stats.ema_latency_ms = 200.0;

        stats.apply_decay(0.0, 0.95);
        assert_eq!(stats.ema_latency_ms, 200.0);
    }

    #[test]
    fn stats_apply_decay_zero_ema_no_change() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        stats.ema_latency_ms = 0.0;

        stats.apply_decay(1.0, 0.95);
        assert_eq!(stats.ema_latency_ms, 0.0);
    }

    #[test]
    fn stats_apply_decay_above_baseline_moves_down() {
        let mut stats = DomainLatencyStats::new("example.com".to_string());
        stats.ema_latency_ms = 2000.0;

        stats.apply_decay(1.0, 0.95);
        // 1000 + (2000 - 1000) * 0.95 = 1000 + 950 = 1950
        assert!(stats.ema_latency_ms < 2000.0);
        assert!(stats.ema_latency_ms > 1000.0);
    }

    // ========== SourceLatencyMonitor ==========

    #[test]
    fn monitor_new_default_equal() {
        let m1 = SourceLatencyMonitor::new();
        let m2 = SourceLatencyMonitor::default();
        assert_eq!(m1.get_all_stats().len(), m2.get_all_stats().len());
        assert_eq!(m1.config().enabled, m2.config().enabled);
    }

    #[test]
    fn monitor_with_config() {
        let config = SourceLatencyConfig {
            max_samples_per_domain: 100,
            ..Default::default()
        };
        let monitor = SourceLatencyMonitor::with_config(config);
        assert_eq!(monitor.config().max_samples_per_domain, 100);
    }

    #[test]
    fn monitor_set_config() {
        let mut monitor = SourceLatencyMonitor::new();
        let new_config = SourceLatencyConfig {
            max_samples_per_domain: 200,
            ..Default::default()
        };
        monitor.set_config(new_config);
        assert_eq!(monitor.config().max_samples_per_domain, 200);
    }

    #[test]
    fn monitor_record_success() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.total_samples, 1);
        assert_eq!(stats.successful_connections, 1);
    }

    #[test]
    fn monitor_record_failure() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_failure("example.com", "Timeout".to_string());

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.failed_connections, 1);
        assert_eq!(stats.consecutive_failures, 1);
    }

    #[test]
    fn monitor_ignored_domains() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("localhost", 10.0);

        assert!(monitor.get_domain_stats("localhost").is_none());
    }

    #[test]
    fn monitor_disabled_no_records() {
        let mut monitor = SourceLatencyMonitor::with_config(SourceLatencyConfig {
            enabled: false,
            ..Default::default()
        });

        monitor.record_success("example.com", 100.0);
        assert!(monitor.get_domain_stats("example.com").is_none());
    }

    #[test]
    fn monitor_multiple_domains() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("a.com", 50.0);
        monitor.record_success("b.com", 100.0);
        monitor.record_success("c.com", 200.0);

        assert_eq!(monitor.get_all_stats().len(), 3);
        assert!(monitor.get_domain_stats("a.com").is_some());
        assert!(monitor.get_domain_stats("b.com").is_some());
        assert!(monitor.get_domain_stats("c.com").is_some());
    }

    #[test]
    fn monitor_nonexistent_domain() {
        let monitor = SourceLatencyMonitor::new();
        assert!(monitor.get_domain_stats("nonexistent.com").is_none());
    }

    #[test]
    fn monitor_get_all_stats_mut() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);

        let all = monitor.get_all_stats_mut();
        assert_eq!(all.len(), 1);
        assert!(all.contains_key("example.com"));
    }

    #[test]
    fn monitor_get_summary_empty() {
        let monitor = SourceLatencyMonitor::new();
        let summary = monitor.get_summary();
        assert_eq!(summary.total_domains, 0);
        assert_eq!(summary.excellent_count, 0);
        assert_eq!(summary.good_count, 0);
        assert_eq!(summary.fair_count, 0);
        assert_eq!(summary.poor_count, 0);
        assert_eq!(summary.unreachable_count, 0);
        assert_eq!(summary.overall_avg_latency_ms, 0.0);
        assert!(summary.fastest_domains.is_empty());
        assert!(summary.slowest_domains.is_empty());
        assert_eq!(summary.total_samples, 0);
    }

    #[test]
    fn monitor_get_summary_health_counts() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("fast.com", 30.0); // Excellent
        monitor.record_success("medium.com", 200.0); // Fair
        monitor.record_success("slow.com", 800.0); // Poor

        let summary = monitor.get_summary();
        assert_eq!(summary.total_domains, 3);
        assert_eq!(summary.excellent_count, 1);
        assert_eq!(summary.fair_count, 1);
        assert_eq!(summary.poor_count, 1);
        assert_eq!(summary.total_samples, 3);
    }

    #[test]
    fn monitor_get_summary_overall_avg() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("a.com", 100.0);
        monitor.record_success("b.com", 200.0);

        let summary = monitor.get_summary();
        // avg of 100 and 200 = 150
        assert!((summary.overall_avg_latency_ms - 150.0).abs() < 1.0);
    }

    #[test]
    fn monitor_get_summary_fastest_slowest() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("slow.com", 500.0);
        monitor.record_success("fast.com", 50.0);
        monitor.record_success("medium.com", 200.0);

        let summary = monitor.get_summary();
        assert_eq!(summary.fastest_domains.len(), 3);
        assert_eq!(summary.fastest_domains[0].0, "fast.com");
        assert_eq!(summary.slowest_domains[0].0, "slow.com");
    }

    #[test]
    fn monitor_get_summary_top_3_cap() {
        let mut monitor = SourceLatencyMonitor::new();
        for i in 0..10 {
            monitor.record_success(&format!("domain{}.com", i), (i as f64 + 1.0) * 10.0);
        }

        let summary = monitor.get_summary();
        assert_eq!(summary.fastest_domains.len(), 3);
        assert_eq!(summary.slowest_domains.len(), 3);
    }

    #[test]
    fn monitor_get_best_domain() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("slow.com", 500.0);
        monitor.record_success("fast.com", 50.0);
        monitor.record_success("medium.com", 200.0);

        let best = monitor.get_best_domain();
        assert_eq!(best, Some("fast.com"));
    }

    #[test]
    fn monitor_get_best_domain_empty() {
        let monitor = SourceLatencyMonitor::new();
        assert!(monitor.get_best_domain().is_none());
    }

    #[test]
    fn monitor_get_best_domain_excludes_unreachable() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("good.com", 100.0);

        // Make bad.com unreachable (3 consecutive failures)
        for _ in 0..3 {
            monitor.record_failure("bad.com", "err".to_string());
        }

        let best = monitor.get_best_domain();
        assert_eq!(best, Some("good.com"));
    }

    #[test]
    fn monitor_rank_domains() {
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
    fn monitor_rank_domains_empty() {
        let monitor = SourceLatencyMonitor::new();
        let ranked = monitor.rank_domains();
        assert!(ranked.is_empty());
    }

    #[test]
    fn monitor_clear_domain() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);
        assert!(monitor.get_domain_stats("example.com").is_some());

        monitor.clear_domain("example.com");
        assert!(monitor.get_domain_stats("example.com").is_none());
    }

    #[test]
    fn monitor_clear_domain_nonexistent() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.clear_domain("nonexistent.com"); // should not panic
    }

    #[test]
    fn monitor_clear_all() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("a.com", 100.0);
        monitor.record_success("b.com", 200.0);

        monitor.clear_all();
        assert_eq!(monitor.get_all_stats().len(), 0);
    }

    #[test]
    fn monitor_clear_all_empty() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.clear_all(); // should not panic
        assert_eq!(monitor.get_all_stats().len(), 0);
    }

    #[test]
    fn monitor_apply_periodic_decay() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);

        // This should not panic even if < 1 hour
        monitor.apply_periodic_decay();
    }

    #[test]
    fn monitor_unicode_domain() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("中文域名.com", 100.0);

        let stats = monitor.get_domain_stats("中文域名.com").unwrap();
        assert_eq!(stats.domain, "中文域名.com");
    }

    #[test]
    fn monitor_emoji_domain() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("🚀.com", 100.0);

        let stats = monitor.get_domain_stats("🚀.com").unwrap();
        assert_eq!(stats.domain, "🚀.com");
    }

    #[test]
    fn monitor_same_domain_multiple_samples() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);
        monitor.record_success("example.com", 200.0);
        monitor.record_success("example.com", 300.0);

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.total_samples, 3);
        assert_eq!(stats.successful_connections, 3);
    }

    #[test]
    fn monitor_mixed_success_failure() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);
        monitor.record_failure("example.com", "err".to_string());
        monitor.record_success("example.com", 200.0);

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.total_samples, 3);
        assert_eq!(stats.successful_connections, 2);
        assert_eq!(stats.failed_connections, 1);
        assert_eq!(stats.consecutive_failures, 0);
    }

    // ========== SourceLatencySummary ==========

    #[test]
    fn summary_serde_roundtrip() {
        let summary = SourceLatencySummary {
            total_domains: 5,
            excellent_count: 1,
            good_count: 2,
            fair_count: 1,
            poor_count: 1,
            unreachable_count: 0,
            overall_avg_latency_ms: 150.0,
            fastest_domains: vec![("fast.com".to_string(), 30.0)],
            slowest_domains: vec![("slow.com".to_string(), 500.0)],
            total_samples: 100,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let loaded: SourceLatencySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_domains, 5);
        assert_eq!(loaded.total_samples, 100);
    }

    #[test]
    fn summary_clone_debug() {
        let summary = SourceLatencySummary {
            total_domains: 0,
            excellent_count: 0,
            good_count: 0,
            fair_count: 0,
            poor_count: 0,
            unreachable_count: 0,
            overall_avg_latency_ms: 0.0,
            fastest_domains: vec![],
            slowest_domains: vec![],
            total_samples: 0,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_domains, summary.total_domains);
        let _ = format!("{:?}", summary);
    }

    // ========== format_summary ==========

    #[test]
    fn format_summary_empty() {
        let monitor = SourceLatencyMonitor::new();
        let summary = monitor.get_summary();
        let formatted = monitor.format_summary(&summary);

        assert!(formatted.contains("Source Latency Monitor Summary"));
        assert!(formatted.contains("Total Domains: 0"));
        assert!(formatted.contains("Overall Avg Latency: 0.0 ms"));
    }

    #[test]
    fn format_summary_with_domains() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("fast.com", 30.0);
        monitor.record_success("slow.com", 800.0);

        let summary = monitor.get_summary();
        let formatted = monitor.format_summary(&summary);

        assert!(formatted.contains("Total Domains: 2"));
        assert!(formatted.contains("Fastest Domains"));
        assert!(formatted.contains("Slowest Domains"));
        assert!(formatted.contains("fast.com"));
        assert!(formatted.contains("slow.com"));
    }

    #[test]
    fn format_summary_health_counts() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("a.com", 30.0);
        monitor.record_success("b.com", 200.0);

        let summary = monitor.get_summary();
        let formatted = monitor.format_summary(&summary);

        assert!(formatted.contains("Excellent"));
        assert!(formatted.contains("Fair"));
    }

    #[test]
    fn format_summary_unicode() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("中文.com", 100.0);

        let summary = monitor.get_summary();
        let formatted = monitor.format_summary(&summary);

        assert!(formatted.contains("中文.com"));
    }

    // ========== Persistence ==========

    #[tokio::test]
    async fn save_config_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("latency_config.json");

        let monitor = SourceLatencyMonitor::new();
        monitor.save_config(&config_path).await.unwrap();

        assert!(config_path.exists());
    }

    #[tokio::test]
    async fn save_config_no_tmp_residual() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("latency_config.json");

        let monitor = SourceLatencyMonitor::new();
        monitor.save_config(&config_path).await.unwrap();

        let entries: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn save_config_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("latency_config.json");

        let monitor = SourceLatencyMonitor::new();
        monitor.save_config(&config_path).await.unwrap();

        let monitor2 = SourceLatencyMonitor::with_config(SourceLatencyConfig {
            max_samples_per_domain: 999,
            ..Default::default()
        });
        monitor2.save_config(&config_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_config(&config_path)
            .await
            .unwrap();
        assert_eq!(loaded.max_samples_per_domain, 999);
    }

    #[tokio::test]
    async fn load_config_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("nonexistent.json");

        let result = SourceLatencyMonitor::load_config(&config_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_config_corrupted_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("bad_config.json");
        std::fs::write(&config_path, "not json").unwrap();

        let result = SourceLatencyMonitor::load_config(&config_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn save_load_config_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("latency_config.json");

        let config = SourceLatencyConfig {
            enabled: false,
            max_samples_per_domain: 123,
            ema_alpha: 0.7,
            ..Default::default()
        };
        let monitor = SourceLatencyMonitor::with_config(config);
        monitor.save_config(&config_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_config(&config_path)
            .await
            .unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.max_samples_per_domain, 123);
        assert!((loaded.ema_alpha - 0.7).abs() < 1e-10);
    }

    #[tokio::test]
    async fn save_load_config_unicode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("latency_config_unicode.json");

        let config = SourceLatencyConfig {
            ignored_domains: vec!["localhost".into(), "中文域名.com".into()],
            ..Default::default()
        };
        let monitor = SourceLatencyMonitor::with_config(config);
        monitor.save_config(&config_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_config(&config_path)
            .await
            .unwrap();
        assert_eq!(loaded.ignored_domains, vec!["localhost", "中文域名.com"]);
    }

    #[tokio::test]
    async fn save_stats_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("latency_stats.json");

        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);
        monitor.save_stats(&stats_path).await.unwrap();

        assert!(stats_path.exists());
    }

    #[tokio::test]
    async fn save_stats_no_tmp_residual() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("latency_stats.json");

        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);
        monitor.save_stats(&stats_path).await.unwrap();

        let entries: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn save_load_stats_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("latency_stats.json");

        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 100.0);
        monitor.record_failure("example.com", "err".to_string());
        monitor.record_success("example.com", 200.0);
        monitor.save_stats(&stats_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_stats(&stats_path).await.unwrap();
        assert_eq!(loaded.len(), 1);
        let stats = loaded.get("example.com").unwrap();
        assert_eq!(stats.total_samples, 3);
        assert_eq!(stats.successful_connections, 2);
        assert_eq!(stats.failed_connections, 1);
    }

    #[tokio::test]
    async fn load_stats_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("nonexistent_stats.json");

        let result = SourceLatencyMonitor::load_stats(&stats_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_stats_corrupted_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("bad_stats.json");
        std::fs::write(&stats_path, "{bad json").unwrap();

        let result = SourceLatencyMonitor::load_stats(&stats_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn save_stats_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("latency_stats.json");

        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("a.com", 100.0);
        monitor.save_stats(&stats_path).await.unwrap();

        monitor.clear_all();
        monitor.record_success("b.com", 200.0);
        monitor.save_stats(&stats_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_stats(&stats_path).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains_key("b.com"));
        assert!(!loaded.contains_key("a.com"));
    }

    #[tokio::test]
    async fn save_load_stats_unicode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("latency_stats_unicode.json");

        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("中文域名.com", 100.0);
        monitor.record_success("🚀.com", 50.0);
        monitor.save_stats(&stats_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_stats(&stats_path).await.unwrap();
        assert!(loaded.contains_key("中文域名.com"));
        assert!(loaded.contains_key("🚀.com"));
    }

    #[tokio::test]
    async fn save_stats_empty_monitor() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_path = temp_dir.path().join("empty_stats.json");

        let monitor = SourceLatencyMonitor::new();
        monitor.save_stats(&stats_path).await.unwrap();

        let loaded = SourceLatencyMonitor::load_stats(&stats_path).await.unwrap();
        assert_eq!(loaded.len(), 0);
    }

    // ========== Complex workflows ==========

    #[test]
    fn workflow_complete_lifecycle() {
        let mut monitor = SourceLatencyMonitor::new();

        // Record multiple domains
        monitor.record_success("fast.com", 30.0);
        monitor.record_success("medium.com", 200.0);
        monitor.record_success("slow.com", 800.0);

        // Check summary
        let summary = monitor.get_summary();
        assert_eq!(summary.total_domains, 3);

        // Check ranking
        let ranked = monitor.rank_domains();
        assert_eq!(ranked[0].0, "fast.com");

        // Check best
        assert_eq!(monitor.get_best_domain(), Some("fast.com"));

        // Clear one
        monitor.clear_domain("slow.com");
        assert_eq!(monitor.get_all_stats().len(), 2);
        assert!(monitor.get_best_domain() == Some("fast.com"));

        // Clear all
        monitor.clear_all();
        assert_eq!(monitor.get_all_stats().len(), 0);
        assert!(monitor.get_best_domain().is_none());
    }

    #[test]
    fn workflow_multi_domain_independent() {
        let mut monitor = SourceLatencyMonitor::new();

        for i in 0..10 {
            monitor.record_success(&format!("domain{}.com", i), (i as f64 + 1.0) * 50.0);
        }

        assert_eq!(monitor.get_all_stats().len(), 10);

        let summary = monitor.get_summary();
        assert_eq!(summary.total_domains, 10);
        assert_eq!(summary.total_samples, 10);

        let ranked = monitor.rank_domains();
        assert_eq!(ranked.len(), 10);
        // Lowest latency first
        assert_eq!(ranked[0].0, "domain0.com");
    }

    #[test]
    fn workflow_failure_then_recovery() {
        let mut monitor = SourceLatencyMonitor::new();

        // 2 failures - not yet unreachable
        monitor.record_failure("example.com", "err1".to_string());
        monitor.record_failure("example.com", "err2".to_string());

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_ne!(stats.health, LatencyHealth::Unreachable);

        // 3rd failure - unreachable
        monitor.record_failure("example.com", "err3".to_string());
        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.health, LatencyHealth::Unreachable);

        // Recovery: success resets consecutive failures
        monitor.record_success("example.com", 100.0);
        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.consecutive_failures, 0);
        assert_eq!(stats.successful_connections, 1);
    }

    #[test]
    fn workflow_config_change_takes_effect() {
        let mut monitor = SourceLatencyMonitor::new();

        // Record with default config
        monitor.record_success("example.com", 100.0);
        assert!(monitor.get_domain_stats("example.com").is_some());

        // Change ignored domains
        let mut new_config = SourceLatencyConfig::default();
        new_config.ignored_domains.push("example.com".to_string());
        monitor.set_config(new_config);

        // New records for example.com should be ignored
        monitor.record_success("example.com", 200.0);
        let stats = monitor.get_domain_stats("example.com").unwrap();
        // Still 1 sample (the second was ignored)
        assert_eq!(stats.total_samples, 1);
    }

    #[test]
    fn workflow_zero_latency() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", 0.0);

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.total_samples, 1);
        assert_eq!(stats.ema_latency_ms, 0.0);
        assert_eq!(stats.min_latency_ms, 0.0);
    }

    #[test]
    fn workflow_very_large_latency() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("example.com", f64::MAX / 2.0);

        let stats = monitor.get_domain_stats("example.com").unwrap();
        assert_eq!(stats.total_samples, 1);
        assert!(stats.ema_latency_ms > 0.0);
    }

    #[test]
    fn rank_domains_includes_health() {
        let mut monitor = SourceLatencyMonitor::new();
        monitor.record_success("fast.com", 30.0);
        monitor.record_success("slow.com", 800.0);

        let ranked = monitor.rank_domains();
        assert_eq!(ranked[0].2, LatencyHealth::Excellent); // 30ms
        assert_eq!(ranked[1].2, LatencyHealth::Poor); // 800ms
    }

    #[test]
    fn monitor_independent_instances() {
        let mut m1 = SourceLatencyMonitor::new();
        let mut m2 = SourceLatencyMonitor::new();

        m1.record_success("a.com", 100.0);
        m2.record_success("b.com", 200.0);

        assert_eq!(m1.get_all_stats().len(), 1);
        assert_eq!(m2.get_all_stats().len(), 1);
        assert!(m1.get_domain_stats("b.com").is_none());
        assert!(m2.get_domain_stats("a.com").is_none());
    }
}
