//! Download Speed Trend Analysis System
//!
//! Aggregates per-domain speed data over configurable time windows,
//! detects degradation trends, and provides actionable insights
//! for download performance optimization.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Time window for trend analysis
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TrendWindow {
    /// Last 5 minutes
    Minutes5,
    /// Last 15 minutes
    #[default]
    Minutes15,
    /// Last 30 minutes
    Minutes30,
    /// Last 1 hour
    Hour1,
    /// Last 6 hours
    Hours6,
    /// Last 24 hours
    Hours24,
}

impl TrendWindow {
    /// Get window duration in seconds
    pub fn as_secs(&self) -> u64 {
        match self {
            TrendWindow::Minutes5 => 5 * 60,
            TrendWindow::Minutes15 => 15 * 60,
            TrendWindow::Minutes30 => 30 * 60,
            TrendWindow::Hour1 => 60 * 60,
            TrendWindow::Hours6 => 6 * 60 * 60,
            TrendWindow::Hours24 => 24 * 60 * 60,
        }
    }
}

/// Trend direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrendDirection {
    /// Speed is stable (variance < threshold)
    Stable,
    /// Speed is improving (positive slope)
    Improving,
    /// Speed is degrading (negative slope)
    Degrading,
    /// Not enough data to determine
    Unknown,
}

impl std::fmt::Display for TrendDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrendDirection::Stable => write!(f, "Stable"),
            TrendDirection::Improving => write!(f, "Improving"),
            TrendDirection::Degrading => write!(f, "Degrading"),
            TrendDirection::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Trend severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrendSeverity {
    /// Minor fluctuation (< 10% change)
    Minor,
    /// Noticeable change (10-30% change)
    Moderate,
    /// Significant change (30-50% change)
    Significant,
    /// Critical change (> 50% change)
    Critical,
}

impl std::fmt::Display for TrendSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrendSeverity::Minor => write!(f, "Minor"),
            TrendSeverity::Moderate => write!(f, "Moderate"),
            TrendSeverity::Significant => write!(f, "Significant"),
            TrendSeverity::Critical => write!(f, "Critical"),
        }
    }
}

/// A speed sample with timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTrendSample {
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Speed in bytes per second
    pub speed_bps: f64,
    /// Domain name
    pub domain: String,
}

/// Trend analysis result for a domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainTrend {
    /// Domain name
    pub domain: String,
    /// Current speed (latest sample)
    pub current_speed_bps: f64,
    /// Average speed in window
    pub avg_speed_bps: f64,
    /// Minimum speed in window
    pub min_speed_bps: f64,
    /// Maximum speed in window
    pub max_speed_bps: f64,
    /// Trend direction
    pub direction: TrendDirection,
    /// Trend severity (for degrading trends)
    pub severity: TrendSeverity,
    /// Percentage change from start to end
    pub change_percent: f64,
    /// Number of samples in window
    pub sample_count: usize,
    /// Trend detected timestamp
    pub detected_at: DateTime<Utc>,
}

/// Configuration for speed trend analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTrendConfig {
    /// Enable trend tracking
    pub enabled: bool,
    /// Default analysis window
    pub default_window: TrendWindow,
    /// Minimum samples required for trend detection
    pub min_samples: usize,
    /// Threshold for trend detection (percentage change, default: 10%)
    pub trend_threshold_percent: f64,
    /// Maximum samples per domain
    pub max_samples_per_domain: usize,
    /// Domains to ignore
    pub ignored_domains: Vec<String>,
}

impl Default for SpeedTrendConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_window: TrendWindow::Minutes15,
            min_samples: 5,
            trend_threshold_percent: 10.0,
            max_samples_per_domain: 200,
            ignored_domains: Vec::new(),
        }
    }
}

/// Speed trend summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTrendSummary {
    /// Total domains tracked
    pub total_domains: usize,
    /// Domains with degrading trends
    pub degrading_domains: usize,
    /// Domains with improving trends
    pub improving_domains: usize,
    /// Domains with stable trends
    pub stable_domains: usize,
    /// Overall average speed
    pub overall_avg_speed_bps: f64,
    /// Worst performing domain
    pub worst_domain: Option<String>,
    /// Best performing domain
    pub best_domain: Option<String>,
    /// Analysis timestamp
    pub timestamp: DateTime<Utc>,
}

/// Speed trend manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTrendManager {
    /// Configuration
    config: SpeedTrendConfig,
    /// Per-domain samples
    domain_samples: HashMap<String, Vec<SpeedTrendSample>>,
    /// Cached trends (domain -> trend)
    cached_trends: HashMap<String, DomainTrend>,
}

impl Default for SpeedTrendManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedTrendManager {
    /// Create a new speed trend manager
    pub fn new() -> Self {
        Self {
            config: SpeedTrendConfig::default(),
            domain_samples: HashMap::new(),
            cached_trends: HashMap::new(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: SpeedTrendConfig) -> Self {
        Self {
            config,
            domain_samples: HashMap::new(),
            cached_trends: HashMap::new(),
        }
    }

    /// Get configuration
    pub fn get_config(&self) -> &SpeedTrendConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: SpeedTrendConfig) {
        self.config = config;
        self.cached_trends.clear();
    }

    /// Add a speed sample for a domain
    pub fn add_sample(&mut self, domain: &str, speed_bps: f64) {
        if !self.config.enabled {
            return;
        }

        if self.config.ignored_domains.contains(&domain.to_string()) {
            return;
        }

        let samples = self.domain_samples.entry(domain.to_string()).or_default();

        samples.push(SpeedTrendSample {
            timestamp: Utc::now(),
            speed_bps,
            domain: domain.to_string(),
        });

        // Keep only max_samples
        if samples.len() > self.config.max_samples_per_domain {
            samples.remove(0);
        }

        // Invalidate cached trend
        self.cached_trends.remove(domain);
    }

    /// Analyze trend for a domain
    pub fn analyze_domain(&self, domain: &str, window: Option<TrendWindow>) -> Option<DomainTrend> {
        let samples = self.domain_samples.get(domain)?;
        let window = window.unwrap_or(self.config.default_window);

        if samples.len() < self.config.min_samples {
            return None;
        }

        // Filter samples within window
        let now = Utc::now();
        let window_start = now - chrono::Duration::seconds(window.as_secs() as i64);
        let window_samples: Vec<_> = samples
            .iter()
            .filter(|s| s.timestamp >= window_start)
            .collect();

        if window_samples.len() < self.config.min_samples {
            return None;
        }

        let speeds: Vec<f64> = window_samples.iter().map(|s| s.speed_bps).collect();
        let current_speed = speeds.last().copied().unwrap_or(0.0);
        let avg_speed = speeds.iter().sum::<f64>() / speeds.len() as f64;
        let min_speed = speeds.iter().copied().fold(f64::INFINITY, f64::min);
        let max_speed = speeds.iter().copied().fold(f64::NEG_INFINITY, f64::max);

        // Calculate percentage change
        let first_speed = speeds.first().copied().unwrap_or(0.0);
        let change_percent = if first_speed > 0.0 {
            ((current_speed - first_speed) / first_speed) * 100.0
        } else {
            0.0
        };

        // Determine trend direction
        let direction = if change_percent.abs() < self.config.trend_threshold_percent {
            TrendDirection::Stable
        } else if change_percent > 0.0 {
            TrendDirection::Improving
        } else {
            TrendDirection::Degrading
        };

        // Determine severity
        let severity = match change_percent.abs() {
            p if p < 10.0 => TrendSeverity::Minor,
            p if p < 30.0 => TrendSeverity::Moderate,
            p if p < 50.0 => TrendSeverity::Significant,
            _ => TrendSeverity::Critical,
        };

        Some(DomainTrend {
            domain: domain.to_string(),
            current_speed_bps: current_speed,
            avg_speed_bps: avg_speed,
            min_speed_bps: min_speed,
            max_speed_bps: max_speed,
            direction,
            severity,
            change_percent,
            sample_count: window_samples.len(),
            detected_at: now,
        })
    }

    /// Get trend summary for all domains
    pub fn get_summary(&self) -> SpeedTrendSummary {
        let mut degrading = 0;
        let mut improving = 0;
        let mut stable = 0;
        let mut total_speed = 0.0;
        let mut worst_domain: Option<(String, f64)> = None;
        let mut best_domain: Option<(String, f64)> = None;

        for domain in self.domain_samples.keys() {
            if let Some(trend) = self.analyze_domain(domain, None) {
                match trend.direction {
                    TrendDirection::Degrading => degrading += 1,
                    TrendDirection::Improving => improving += 1,
                    TrendDirection::Stable => stable += 1,
                    TrendDirection::Unknown => {}
                }

                total_speed += trend.avg_speed_bps;

                match &worst_domain {
                    None => worst_domain = Some((domain.clone(), trend.avg_speed_bps)),
                    Some((_, spd)) if trend.avg_speed_bps < *spd => {
                        worst_domain = Some((domain.clone(), trend.avg_speed_bps));
                    }
                    _ => {}
                }

                match &best_domain {
                    None => best_domain = Some((domain.clone(), trend.avg_speed_bps)),
                    Some((_, spd)) if trend.avg_speed_bps > *spd => {
                        best_domain = Some((domain.clone(), trend.avg_speed_bps));
                    }
                    _ => {}
                }
            }
        }

        let total_domains = self.domain_samples.len();
        let overall_avg = if total_domains > 0 {
            total_speed / total_domains as f64
        } else {
            0.0
        };

        SpeedTrendSummary {
            total_domains,
            degrading_domains: degrading,
            improving_domains: improving,
            stable_domains: stable,
            overall_avg_speed_bps: overall_avg,
            worst_domain: worst_domain.map(|(d, _)| d),
            best_domain: best_domain.map(|(d, _)| d),
            timestamp: Utc::now(),
        }
    }

    /// Get all domain trends
    pub fn get_all_trends(&self) -> Vec<DomainTrend> {
        let mut trends = Vec::new();
        for domain in self.domain_samples.keys() {
            if let Some(trend) = self.analyze_domain(domain, None) {
                trends.push(trend);
            }
        }
        trends.sort_by(|a, b| a.avg_speed_bps.partial_cmp(&b.avg_speed_bps).unwrap());
        trends
    }

    /// Get domains with degrading trends
    pub fn get_degrading_domains(&self) -> Vec<DomainTrend> {
        self.get_all_trends()
            .into_iter()
            .filter(|t| t.direction == TrendDirection::Degrading)
            .collect()
    }

    /// Get domains with improving trends
    pub fn get_improving_domains(&self) -> Vec<DomainTrend> {
        self.get_all_trends()
            .into_iter()
            .filter(|t| t.direction == TrendDirection::Improving)
            .collect()
    }

    /// Clear all data for a domain
    pub fn clear_domain(&mut self, domain: &str) {
        self.domain_samples.remove(domain);
        self.cached_trends.remove(domain);
    }

    /// Clear all data
    pub fn clear_all(&mut self) {
        self.domain_samples.clear();
        self.cached_trends.clear();
    }

    /// Get list of tracked domains
    pub fn get_domains(&self) -> Vec<&String> {
        self.domain_samples.keys().collect()
    }

    /// Format trend as human-readable string
    pub fn format_trend(&self, trend: &DomainTrend) -> String {
        let direction_icon = match trend.direction {
            TrendDirection::Stable => "➡️",
            TrendDirection::Improving => "📈",
            TrendDirection::Degrading => "📉",
            TrendDirection::Unknown => "❓",
        };

        format!(
            "{} {} | Current: {} | Avg: {} | Change: {:.1}% | Samples: {}",
            direction_icon,
            trend.domain,
            format_speed(trend.current_speed_bps),
            format_speed(trend.avg_speed_bps),
            trend.change_percent,
            trend.sample_count
        )
    }

    /// Format summary as human-readable string
    pub fn format_summary(&self, summary: &SpeedTrendSummary) -> String {
        let mut output = String::new();
        output.push_str("📊 Speed Trend Summary\n");
        output.push_str(&format!("  Domains tracked: {}\n", summary.total_domains));
        output.push_str(&format!("  📈 Improving: {}\n", summary.improving_domains));
        output.push_str(&format!("  ➡️ Stable: {}\n", summary.stable_domains));
        output.push_str(&format!("  📉 Degrading: {}\n", summary.degrading_domains));
        output.push_str(&format!(
            "  Overall avg: {}\n",
            format_speed(summary.overall_avg_speed_bps)
        ));

        if let Some(ref worst) = summary.worst_domain {
            output.push_str(&format!("  ⚠️  Worst: {}\n", worst));
        }
        if let Some(ref best) = summary.best_domain {
            output.push_str(&format!("  ✅ Best: {}\n", best));
        }

        output
    }

    /// Save configuration to file
    pub fn save_config(&self, path: &str) -> std::io::Result<()> {
        let json =
            serde_json::to_string_pretty(&self.config).map_err(|e| std::io::Error::other(e))?;
        std::fs::write(path, json)
    }

    /// Load configuration from file
    pub fn load_config(path: &str) -> std::io::Result<SpeedTrendConfig> {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(|e| std::io::Error::other(e))
    }

    /// Save data to file
    pub fn save_data(&self, path: &str) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self.domain_samples)
            .map_err(|e| std::io::Error::other(e))?;
        std::fs::write(path, json)
    }

    /// Load data from file
    pub fn load_data(&mut self, path: &str) -> std::io::Result<()> {
        let json = std::fs::read_to_string(path)?;
        self.domain_samples = serde_json::from_str(&json).map_err(|e| std::io::Error::other(e))?;
        Ok(())
    }
}

/// Format speed in human-readable format
fn format_speed(bps: f64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trend_window_as_secs() {
        assert_eq!(TrendWindow::Minutes5.as_secs(), 300);
        assert_eq!(TrendWindow::Minutes15.as_secs(), 900);
        assert_eq!(TrendWindow::Hour1.as_secs(), 3600);
    }

    #[test]
    fn test_trend_direction_display() {
        assert_eq!(format!("{}", TrendDirection::Stable), "Stable");
        assert_eq!(format!("{}", TrendDirection::Improving), "Improving");
        assert_eq!(format!("{}", TrendDirection::Degrading), "Degrading");
    }

    #[test]
    fn test_trend_severity_display() {
        assert_eq!(format!("{}", TrendSeverity::Minor), "Minor");
        assert_eq!(format!("{}", TrendSeverity::Moderate), "Moderate");
        assert_eq!(format!("{}", TrendSeverity::Critical), "Critical");
    }

    #[test]
    fn test_config_default() {
        let config = SpeedTrendConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_window, TrendWindow::Minutes15);
        assert_eq!(config.min_samples, 5);
        assert_eq!(config.trend_threshold_percent, 10.0);
    }

    #[test]
    fn test_manager_new() {
        let manager = SpeedTrendManager::new();
        assert!(manager.get_config().enabled);
        assert!(manager.get_domains().is_empty());
    }

    #[test]
    fn test_add_sample() {
        let mut manager = SpeedTrendManager::new();
        manager.add_sample("example.com", 1024.0 * 1024.0);
        assert_eq!(manager.get_domains().len(), 1);
    }

    #[test]
    fn test_add_sample_disabled() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.enabled = false;
        manager.set_config(config);

        manager.add_sample("example.com", 1024.0);
        assert!(manager.get_domains().is_empty());
    }

    #[test]
    fn test_add_sample_ignored_domain() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.ignored_domains = vec!["internal.cdn".to_string()];
        manager.set_config(config);

        manager.add_sample("internal.cdn", 1024.0);
        assert!(manager.get_domains().is_empty());
    }

    #[test]
    fn test_analyze_domain_insufficient_samples() {
        let mut manager = SpeedTrendManager::new();
        manager.add_sample("example.com", 1024.0);
        manager.add_sample("example.com", 2048.0);

        let trend = manager.analyze_domain("example.com", None);
        assert!(trend.is_none()); // Less than min_samples (5)
    }

    #[test]
    fn test_analyze_domain_stable() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        // Add samples with similar speeds
        for _ in 0..5 {
            manager.add_sample("example.com", 1000.0);
        }

        let trend = manager.analyze_domain("example.com", None).unwrap();
        assert_eq!(trend.direction, TrendDirection::Stable);
        assert_eq!(trend.domain, "example.com");
        assert_eq!(trend.sample_count, 5);
    }

    #[test]
    fn test_analyze_domain_improving() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        // Add samples with increasing speeds
        manager.add_sample("example.com", 100.0);
        manager.add_sample("example.com", 200.0);
        manager.add_sample("example.com", 400.0);
        manager.add_sample("example.com", 800.0);
        manager.add_sample("example.com", 1600.0);

        let trend = manager.analyze_domain("example.com", None).unwrap();
        assert_eq!(trend.direction, TrendDirection::Improving);
        assert!(trend.change_percent > 0.0);
    }

    #[test]
    fn test_analyze_domain_degrading() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        // Add samples with decreasing speeds
        manager.add_sample("example.com", 1600.0);
        manager.add_sample("example.com", 800.0);
        manager.add_sample("example.com", 400.0);
        manager.add_sample("example.com", 200.0);
        manager.add_sample("example.com", 100.0);

        let trend = manager.analyze_domain("example.com", None).unwrap();
        assert_eq!(trend.direction, TrendDirection::Degrading);
        assert!(trend.change_percent < 0.0);
    }

    #[test]
    fn test_severity_minor() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        // Small change (< 10%)
        manager.add_sample("example.com", 100.0);
        manager.add_sample("example.com", 105.0);
        manager.add_sample("example.com", 108.0);

        let trend = manager.analyze_domain("example.com", None).unwrap();
        assert_eq!(trend.severity, TrendSeverity::Minor);
    }

    #[test]
    fn test_severity_critical() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        // Large change (> 50%)
        manager.add_sample("example.com", 1000.0);
        manager.add_sample("example.com", 500.0);
        manager.add_sample("example.com", 200.0);

        let trend = manager.analyze_domain("example.com", None).unwrap();
        assert_eq!(trend.severity, TrendSeverity::Critical);
    }

    #[test]
    fn test_get_summary_empty() {
        let manager = SpeedTrendManager::new();
        let summary = manager.get_summary();
        assert_eq!(summary.total_domains, 0);
        assert_eq!(summary.degrading_domains, 0);
        assert_eq!(summary.improving_domains, 0);
        assert_eq!(summary.stable_domains, 0);
    }

    #[test]
    fn test_get_summary_with_data() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        // Add improving domain
        for i in 0..5 {
            manager.add_sample("fast.com", (100.0 * (i + 1) as f64));
        }

        // Add degrading domain
        for i in 0..5 {
            manager.add_sample("slow.com", (1000.0 - 200.0 * i as f64));
        }

        let summary = manager.get_summary();
        assert_eq!(summary.total_domains, 2);
        assert!(summary.improving_domains > 0 || summary.degrading_domains > 0);
    }

    #[test]
    fn test_get_all_trends() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        manager.add_sample("a.com", 100.0);
        manager.add_sample("a.com", 200.0);
        manager.add_sample("a.com", 300.0);

        manager.add_sample("b.com", 1000.0);
        manager.add_sample("b.com", 2000.0);
        manager.add_sample("b.com", 3000.0);

        let trends = manager.get_all_trends();
        assert_eq!(trends.len(), 2);
        // Should be sorted by avg speed (ascending)
        assert!(trends[0].avg_speed_bps < trends[1].avg_speed_bps);
    }

    #[test]
    fn test_get_degrading_domains() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        // Add degrading domain
        manager.add_sample("bad.com", 1000.0);
        manager.add_sample("bad.com", 500.0);
        manager.add_sample("bad.com", 200.0);

        // Add improving domain
        manager.add_sample("good.com", 100.0);
        manager.add_sample("good.com", 500.0);
        manager.add_sample("good.com", 1000.0);

        let degrading = manager.get_degrading_domains();
        assert_eq!(degrading.len(), 1);
        assert_eq!(degrading[0].domain, "bad.com");
    }

    #[test]
    fn test_get_improving_domains() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        // Add improving domain
        manager.add_sample("good.com", 100.0);
        manager.add_sample("good.com", 500.0);
        manager.add_sample("good.com", 1000.0);

        let improving = manager.get_improving_domains();
        assert_eq!(improving.len(), 1);
        assert_eq!(improving[0].domain, "good.com");
    }

    #[test]
    fn test_clear_domain() {
        let mut manager = SpeedTrendManager::new();
        manager.add_sample("example.com", 1024.0);
        assert_eq!(manager.get_domains().len(), 1);

        manager.clear_domain("example.com");
        assert!(manager.get_domains().is_empty());
    }

    #[test]
    fn test_clear_all() {
        let mut manager = SpeedTrendManager::new();
        manager.add_sample("a.com", 100.0);
        manager.add_sample("b.com", 200.0);
        assert_eq!(manager.get_domains().len(), 2);

        manager.clear_all();
        assert!(manager.get_domains().is_empty());
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(500.0), "500 B/s");
        assert_eq!(format_speed(1024.0), "1.0 KB/s");
        assert_eq!(format_speed(1024.0 * 1024.0), "1.00 MB/s");
        assert_eq!(format_speed(1024.0 * 1024.0 * 1024.0), "1.00 GB/s");
    }

    #[test]
    fn test_format_trend() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        manager.add_sample("example.com", 1024.0 * 1024.0);
        manager.add_sample("example.com", 2048.0 * 1024.0);
        manager.add_sample("example.com", 4096.0 * 1024.0);

        let trend = manager.analyze_domain("example.com", None).unwrap();
        let formatted = manager.format_trend(&trend);
        assert!(formatted.contains("example.com"));
        assert!(formatted.contains("KB/s") || formatted.contains("MB/s"));
    }

    #[test]
    fn test_format_summary() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 3;
        manager.set_config(config);

        manager.add_sample("example.com", 1024.0);
        manager.add_sample("example.com", 2048.0);
        manager.add_sample("example.com", 4096.0);

        let summary = manager.get_summary();
        let formatted = manager.format_summary(&summary);
        assert!(formatted.contains("Speed Trend Summary"));
        assert!(formatted.contains("Domains tracked"));
    }

    #[test]
    fn test_save_load_config() {
        let manager = SpeedTrendManager::new();
        let path = "/tmp/test_speed_trend_config.json";

        manager.save_config(path).unwrap();
        let loaded = SpeedTrendManager::load_config(path).unwrap();
        assert_eq!(loaded.enabled, true);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_save_load_data() {
        let mut manager = SpeedTrendManager::new();
        manager.add_sample("example.com", 1024.0);
        manager.add_sample("example.com", 2048.0);

        let path = "/tmp/test_speed_trend_data.json";
        manager.save_data(path).unwrap();

        let mut loaded = SpeedTrendManager::new();
        loaded.load_data(path).unwrap();
        assert_eq!(loaded.get_domains().len(), 1);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_max_samples_limit() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.max_samples_per_domain = 10;
        manager.set_config(config);

        // Add more than max samples
        for i in 0..20 {
            manager.add_sample("example.com", i as f64 * 100.0);
        }

        let samples = manager.domain_samples.get("example.com").unwrap();
        assert_eq!(samples.len(), 10);
    }

    #[test]
    fn test_window_filtering() {
        let mut manager = SpeedTrendManager::new();
        let mut config = SpeedTrendConfig::default();
        config.min_samples = 2;
        manager.set_config(config);

        // Add samples
        manager.add_sample("example.com", 100.0);
        manager.add_sample("example.com", 200.0);
        manager.add_sample("example.com", 300.0);

        // Analyze with different windows
        let trend_5min = manager.analyze_domain("example.com", Some(TrendWindow::Minutes5));
        let trend_1hour = manager.analyze_domain("example.com", Some(TrendWindow::Hour1));

        // Both should have data (samples are recent)
        assert!(trend_5min.is_some());
        assert!(trend_1hour.is_some());
    }

    #[test]
    fn test_nonexistent_domain() {
        let manager = SpeedTrendManager::new();
        let trend = manager.analyze_domain("nonexistent.com", None);
        assert!(trend.is_none());
    }
}
