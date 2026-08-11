//! Download Bandwidth Forecasting System
//!
//! Predict future download speeds based on historical data, enabling
//! accurate ETA estimates and bandwidth planning. Uses weighted moving
//! averages with time-of-day awareness.
//!
//! Features:
//! - Historical speed analysis with weighted moving average
//! - Time-of-day pattern detection (morning/afternoon/evening/night)
//! - Confidence scoring based on sample count and variance
//! - Per-domain speed forecasting
//! - ETA prediction with confidence intervals
//! - Persistence to `bandwidth_forecast_config.json`

use chrono::{DateTime, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tracing::debug;

/// Configuration for bandwidth forecasting
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForecastConfig {
    /// Enable forecasting
    pub enabled: bool,
    /// Minimum samples required for reliable forecast
    pub min_samples: usize,
    /// Maximum samples to retain per domain
    pub max_samples: usize,
    /// Window for recent trend (seconds)
    pub trend_window_secs: u64,
    /// Confidence threshold for high-confidence forecasts (0.0-1.0)
    pub high_confidence_threshold: f64,
    /// Confidence threshold for medium-confidence forecasts (0.0-1.0)
    pub medium_confidence_threshold: f64,
}

impl Default for ForecastConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_samples: 5,
            max_samples: 200,
            trend_window_secs: 300, // 5 minutes
            high_confidence_threshold: 0.7,
            medium_confidence_threshold: 0.4,
        }
    }
}

/// A speed sample for forecasting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForecastSample {
    /// Timestamp when recorded
    pub timestamp: DateTime<Utc>,
    /// Speed in bytes per second
    pub speed_bps: f64,
    /// Bytes downloaded at this point
    pub bytes_downloaded: u64,
}

/// Time-of-day bucket for pattern detection
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TimeOfDay {
    /// 00:00-06:00
    Night,
    /// 06:00-12:00
    Morning,
    /// 12:00-18:00
    Afternoon,
    /// 18:00-24:00
    Evening,
}

impl TimeOfDay {
    /// Get time bucket from hour (0-23)
    pub fn from_hour(hour: u32) -> Self {
        match hour {
            0..=5 => TimeOfDay::Night,
            6..=11 => TimeOfDay::Morning,
            12..=17 => TimeOfDay::Afternoon,
            _ => TimeOfDay::Evening,
        }
    }

    /// Display name
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeOfDay::Night => "night (00-06)",
            TimeOfDay::Morning => "morning (06-12)",
            TimeOfDay::Afternoon => "afternoon (12-18)",
            TimeOfDay::Evening => "evening (18-24)",
        }
    }
}

/// Forecast confidence level
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ForecastConfidence {
    /// High confidence (>70% threshold)
    High,
    /// Medium confidence (40-70%)
    Medium,
    /// Low confidence (<40%)
    Low,
    /// No data available
    None,
}

impl ForecastConfidence {
    pub fn as_str(&self) -> &'static str {
        match self {
            ForecastConfidence::High => "high",
            ForecastConfidence::Medium => "medium",
            ForecastConfidence::Low => "low",
            ForecastConfidence::None => "none",
        }
    }
}

/// Forecast result for a domain or task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthForecast {
    /// Domain or task identifier
    pub key: String,
    /// Predicted speed (bytes/sec)
    pub predicted_speed_bps: f64,
    /// Minimum expected speed (bytes/sec)
    pub min_speed_bps: f64,
    /// Maximum expected speed (bytes/sec)
    pub max_speed_bps: f64,
    /// Confidence level
    pub confidence: ForecastConfidence,
    /// Confidence score (0.0-1.0)
    pub confidence_score: f64,
    /// Number of samples used
    pub sample_count: usize,
    /// Time-of-day pattern (if available)
    pub time_pattern: Option<TimeOfDay>,
    /// When forecast was generated
    pub generated_at: DateTime<Utc>,
    /// Trend direction
    pub trend: ForecastTrend,
}

/// Speed trend direction
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ForecastTrend {
    /// Speed increasing
    Increasing,
    /// Speed stable
    Stable,
    /// Speed decreasing
    Decreasing,
    /// Not enough data
    Unknown,
}

impl ForecastTrend {
    pub fn as_str(&self) -> &'static str {
        match self {
            ForecastTrend::Increasing => "increasing",
            ForecastTrend::Stable => "stable",
            ForecastTrend::Decreasing => "decreasing",
            ForecastTrend::Unknown => "unknown",
        }
    }
}

/// Per-domain speed history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSpeedHistory {
    /// Domain name
    pub domain: String,
    /// Speed samples
    pub samples: Vec<ForecastSample>,
    /// Time-of-day average speeds
    pub tod_averages: HashMap<String, f64>,
    /// Time-of-day sample counts
    pub tod_counts: HashMap<String, usize>,
}

impl DomainSpeedHistory {
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            samples: Vec::new(),
            tod_averages: HashMap::new(),
            tod_counts: HashMap::new(),
        }
    }

    /// Add a speed sample
    pub fn add_sample(&mut self, sample: ForecastSample, max_samples: usize) {
        self.samples.push(sample);
        if self.samples.len() > max_samples {
            self.samples.remove(0);
        }
        self.recalculate_tod_averages();
    }

    /// Recalculate time-of-day averages
    fn recalculate_tod_averages(&mut self) {
        let mut tod_sums: HashMap<String, f64> = HashMap::new();
        let mut tod_counts: HashMap<String, usize> = HashMap::new();

        for sample in &self.samples {
            let tod = TimeOfDay::from_hour(sample.timestamp.hour());
            let key = tod.as_str().to_string();
            *tod_sums.entry(key.clone()).or_insert(0.0) += sample.speed_bps;
            *tod_counts.entry(key).or_insert(0) += 1;
        }

        self.tod_averages = tod_sums
            .into_iter()
            .map(|(k, v)| {
                let count = tod_counts.get(&k).copied().unwrap_or(1);
                (k, v / count as f64)
            })
            .collect();
        self.tod_counts = tod_counts;
    }

    /// Get average speed for current time of day
    pub fn get_tod_average(&self, tod: TimeOfDay) -> Option<f64> {
        self.tod_averages.get(tod.as_str()).copied()
    }

    /// Get overall average speed
    pub fn overall_average(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.samples.iter().map(|s| s.speed_bps).sum();
        sum / self.samples.len() as f64
    }

    /// Get recent average speed (within window)
    pub fn recent_average(&self, window_secs: u64) -> f64 {
        let cutoff = Utc::now() - Duration::seconds(window_secs as i64);
        let recent: Vec<&ForecastSample> = self
            .samples
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .collect();
        if recent.is_empty() {
            return self.overall_average();
        }
        let sum: f64 = recent.iter().map(|s| s.speed_bps).sum();
        sum / recent.len() as f64
    }

    /// Calculate speed variance
    pub fn variance(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        let avg = self.overall_average();
        let sum_sq: f64 = self
            .samples
            .iter()
            .map(|s| (s.speed_bps - avg).powi(2))
            .sum();
        sum_sq / (self.samples.len() - 1) as f64
    }
}

/// Bandwidth forecast manager
#[derive(Debug, Default)]
pub struct BandwidthForecastManager {
    /// Per-domain speed histories
    pub histories: HashMap<String, DomainSpeedHistory>,
    /// Configuration
    pub config: ForecastConfig,
}

impl BandwidthForecastManager {
    /// Create a new manager with config
    pub fn new(config: ForecastConfig) -> Self {
        Self {
            histories: HashMap::new(),
            config,
        }
    }

    /// Record a speed sample for a domain
    pub fn record_sample(&mut self, domain: &str, speed_bps: f64, bytes_downloaded: u64) {
        if !self.config.enabled {
            return;
        }

        let history = self
            .histories
            .entry(domain.to_string())
            .or_insert_with(|| DomainSpeedHistory::new(domain.to_string()));

        let sample = ForecastSample {
            timestamp: Utc::now(),
            speed_bps,
            bytes_downloaded,
        };

        history.add_sample(sample, self.config.max_samples);
        debug!("Recorded speed sample for {}: {:.0} B/s", domain, speed_bps);
    }

    /// Generate forecast for a domain
    pub fn forecast(&self, domain: &str) -> BandwidthForecast {
        let now = Utc::now();

        let history = match self.histories.get(domain) {
            Some(h) => h,
            None => {
                return BandwidthForecast {
                    key: domain.to_string(),
                    predicted_speed_bps: 0.0,
                    min_speed_bps: 0.0,
                    max_speed_bps: 0.0,
                    confidence: ForecastConfidence::None,
                    confidence_score: 0.0,
                    sample_count: 0,
                    time_pattern: None,
                    generated_at: now,
                    trend: ForecastTrend::Unknown,
                };
            }
        };

        let sample_count = history.samples.len();

        // Not enough data
        if sample_count < self.config.min_samples {
            let avg = history.overall_average();
            return BandwidthForecast {
                key: domain.to_string(),
                predicted_speed_bps: avg,
                min_speed_bps: avg * 0.5,
                max_speed_bps: avg * 1.5,
                confidence: ForecastConfidence::Low,
                confidence_score: sample_count as f64 / self.config.min_samples as f64,
                sample_count,
                time_pattern: None,
                generated_at: now,
                trend: ForecastTrend::Unknown,
            };
        }

        // Weighted average: 60% recent trend, 40% time-of-day pattern
        let recent_avg = history.recent_average(self.config.trend_window_secs);
        let overall_avg = history.overall_average();
        let current_tod = TimeOfDay::from_hour(now.hour());
        let tod_avg = history.get_tod_average(current_tod);

        let predicted_speed = match tod_avg {
            Some(tod) => recent_avg * 0.6 + tod * 0.4,
            None => recent_avg * 0.7 + overall_avg * 0.3,
        };

        // Calculate min/max from recent samples
        let cutoff = now - Duration::seconds(self.config.trend_window_secs as i64);
        let recent: Vec<&ForecastSample> = history
            .samples
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .collect();
        let min_speed = recent
            .iter()
            .map(|s| s.speed_bps)
            .fold(f64::INFINITY, f64::min);
        let max_speed = recent
            .iter()
            .map(|s| s.speed_bps)
            .fold(f64::NEG_INFINITY, f64::max);

        // Confidence based on sample count and variance
        let variance = history.variance();
        let cv = if overall_avg > 0.0 {
            (variance.sqrt() / overall_avg).min(1.0)
        } else {
            1.0
        };
        let sample_confidence = (sample_count as f64 / (self.config.max_samples as f64 * 0.5))
            .min(1.0)
            .max(0.0);
        let stability_confidence = 1.0 - cv;
        let confidence_score = (sample_confidence * 0.5 + stability_confidence * 0.5).max(0.0);

        let confidence = if confidence_score >= self.config.high_confidence_threshold {
            ForecastConfidence::High
        } else if confidence_score >= self.config.medium_confidence_threshold {
            ForecastConfidence::Medium
        } else {
            ForecastConfidence::Low
        };

        // Detect trend
        let trend = self.detect_trend(history);

        BandwidthForecast {
            key: domain.to_string(),
            predicted_speed_bps: predicted_speed.max(0.0),
            min_speed_bps: min_speed.max(0.0),
            max_speed_bps: max_speed.max(0.0),
            confidence,
            confidence_score,
            sample_count,
            time_pattern: Some(current_tod),
            generated_at: now,
            trend,
        }
    }

    /// Detect speed trend from recent samples
    fn detect_trend(&self, history: &DomainSpeedHistory) -> ForecastTrend {
        if history.samples.len() < 3 {
            return ForecastTrend::Unknown;
        }

        let window = (history.samples.len() / 3).max(1);
        let recent: Vec<f64> = history
            .samples
            .iter()
            .rev()
            .take(window)
            .map(|s| s.speed_bps)
            .collect();
        let older: Vec<f64> = history
            .samples
            .iter()
            .rev()
            .skip(window)
            .take(window)
            .map(|s| s.speed_bps)
            .collect();

        if older.is_empty() {
            return ForecastTrend::Unknown;
        }

        let recent_avg: f64 = recent.iter().sum::<f64>() / recent.len() as f64;
        let older_avg: f64 = older.iter().sum::<f64>() / older.len() as f64;

        if older_avg == 0.0 {
            return ForecastTrend::Unknown;
        }

        let change_ratio = (recent_avg - older_avg) / older_avg;

        if change_ratio > 0.1 {
            ForecastTrend::Increasing
        } else if change_ratio < -0.1 {
            ForecastTrend::Decreasing
        } else {
            ForecastTrend::Stable
        }
    }

    /// Estimate time to complete download (seconds)
    pub fn estimate_eta(&self, domain: &str, remaining_bytes: u64) -> Option<u64> {
        let forecast = self.forecast(domain);
        if forecast.predicted_speed_bps <= 0.0 {
            return None;
        }
        let seconds = remaining_bytes as f64 / forecast.predicted_speed_bps;
        Some(seconds.ceil() as u64)
    }

    /// Get forecast summary for all domains
    pub fn get_summary(&self) -> ForecastSummary {
        let mut forecasts = Vec::new();
        for domain in self.histories.keys() {
            forecasts.push(self.forecast(domain));
        }
        forecasts.sort_by(|a, b| b.confidence_score.partial_cmp(&a.confidence_score).unwrap());

        let total_domains = forecasts.len();
        let high_confidence = forecasts
            .iter()
            .filter(|f| f.confidence == ForecastConfidence::High)
            .count();
        let avg_predicted_speed = if forecasts.is_empty() {
            0.0
        } else {
            forecasts.iter().map(|f| f.predicted_speed_bps).sum::<f64>() / forecasts.len() as f64
        };

        ForecastSummary {
            total_domains,
            high_confidence_count: high_confidence,
            avg_predicted_speed_bps: avg_predicted_speed,
            forecasts,
        }
    }

    /// Clear history for a domain
    pub fn clear_domain(&mut self, domain: &str) {
        self.histories.remove(domain);
    }

    /// Clear all histories
    pub fn clear_all(&mut self) {
        self.histories.clear();
    }

    /// Save config to disk
    pub fn save_config(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self.config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(path, json)
    }

    /// Load config from disk
    pub fn load_config(path: &Path) -> std::io::Result<ForecastConfig> {
        let json = fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// Save histories to disk
    pub fn save_histories(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self.histories)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(path, json)
    }

    /// Load histories from disk
    pub fn load_histories(path: &Path) -> std::io::Result<HashMap<String, DomainSpeedHistory>> {
        let json = fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}

/// Summary of all forecasts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForecastSummary {
    /// Number of domains tracked
    pub total_domains: usize,
    /// Number of high-confidence forecasts
    pub high_confidence_count: usize,
    /// Average predicted speed across all domains
    pub avg_predicted_speed_bps: f64,
    /// Individual forecasts
    pub forecasts: Vec<BandwidthForecast>,
}

impl ForecastSummary {
    /// Format summary for display
    pub fn display(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "📊 Bandwidth Forecast Summary\n  Domains: {} | High confidence: {}\n  Avg predicted speed: {:.1} KB/s\n\n",
            self.total_domains,
            self.high_confidence_count,
            self.avg_predicted_speed_bps / 1024.0
        ));
        for f in &self.forecasts {
            out.push_str(&format!(
                "  {} → {:.1} KB/s ({}, {} samples, trend: {})\n",
                f.key,
                f.predicted_speed_bps / 1024.0,
                f.confidence.as_str(),
                f.sample_count,
                f.trend.as_str()
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ForecastConfig {
        ForecastConfig {
            enabled: true,
            min_samples: 3,
            max_samples: 50,
            trend_window_secs: 60,
            high_confidence_threshold: 0.7,
            medium_confidence_threshold: 0.4,
        }
    }

    #[test]
    fn test_time_of_day_from_hour() {
        assert_eq!(TimeOfDay::from_hour(0), TimeOfDay::Night);
        assert_eq!(TimeOfDay::from_hour(5), TimeOfDay::Night);
        assert_eq!(TimeOfDay::from_hour(6), TimeOfDay::Morning);
        assert_eq!(TimeOfDay::from_hour(11), TimeOfDay::Morning);
        assert_eq!(TimeOfDay::from_hour(12), TimeOfDay::Afternoon);
        assert_eq!(TimeOfDay::from_hour(17), TimeOfDay::Afternoon);
        assert_eq!(TimeOfDay::from_hour(18), TimeOfDay::Evening);
        assert_eq!(TimeOfDay::from_hour(23), TimeOfDay::Evening);
    }

    #[test]
    fn test_domain_speed_history_basic() {
        let mut history = DomainSpeedHistory::new("example.com".to_string());
        assert_eq!(history.overall_average(), 0.0);

        let sample = ForecastSample {
            timestamp: Utc::now(),
            speed_bps: 1_000_000.0,
            bytes_downloaded: 1000,
        };
        history.add_sample(sample, 50);

        assert!((history.overall_average() - 1_000_000.0).abs() < 1.0);
    }

    #[test]
    fn test_domain_history_max_samples() {
        let mut history = DomainSpeedHistory::new("test.com".to_string());
        for i in 0..10 {
            let sample = ForecastSample {
                timestamp: Utc::now(),
                speed_bps: (i * 100_000) as f64,
                bytes_downloaded: 0,
            };
            history.add_sample(sample, 5);
        }
        assert_eq!(history.samples.len(), 5);
    }

    #[test]
    fn test_forecast_no_data() {
        let manager = BandwidthForecastManager::new(test_config());
        let forecast = manager.forecast("unknown.com");
        assert_eq!(forecast.confidence, ForecastConfidence::None);
        assert_eq!(forecast.sample_count, 0);
    }

    #[test]
    fn test_forecast_insufficient_samples() {
        let mut manager = BandwidthForecastManager::new(test_config());
        manager.record_sample("slow.com", 500_000.0, 1000);
        manager.record_sample("slow.com", 600_000.0, 2000);

        let forecast = manager.forecast("slow.com");
        assert_eq!(forecast.confidence, ForecastConfidence::Low);
        assert_eq!(forecast.sample_count, 2);
    }

    #[test]
    fn test_forecast_with_sufficient_data() {
        let mut manager = BandwidthForecastManager::new(test_config());
        for i in 0..10 {
            let speed = 1_000_000.0 + (i as f64 * 10_000.0);
            manager.record_sample("fast.com", speed, (i * 1000) as u64);
        }

        let forecast = manager.forecast("fast.com");
        assert!(forecast.predicted_speed_bps > 0.0);
        assert!(forecast.sample_count >= 3);
        assert_ne!(forecast.confidence, ForecastConfidence::None);
    }

    #[test]
    fn test_estimate_eta() {
        let mut manager = BandwidthForecastManager::new(test_config());
        for _ in 0..5 {
            manager.record_sample("cdn.com", 1_000_000.0, 0);
        }

        let eta = manager.estimate_eta("cdn.com", 5_000_000);
        assert!(eta.is_some());
        assert!(eta.unwrap() > 0);
    }

    #[test]
    fn test_estimate_eta_no_data() {
        let manager = BandwidthForecastManager::new(test_config());
        let eta = manager.estimate_eta("unknown.com", 1_000_000);
        assert!(eta.is_none());
    }

    #[test]
    fn test_clear_domain() {
        let mut manager = BandwidthForecastManager::new(test_config());
        manager.record_sample("a.com", 100.0, 0);
        manager.record_sample("b.com", 200.0, 0);
        assert_eq!(manager.histories.len(), 2);

        manager.clear_domain("a.com");
        assert_eq!(manager.histories.len(), 1);
        assert!(manager.histories.contains_key("b.com"));
    }

    #[test]
    fn test_clear_all() {
        let mut manager = BandwidthForecastManager::new(test_config());
        manager.record_sample("a.com", 100.0, 0);
        manager.record_sample("b.com", 200.0, 0);
        manager.clear_all();
        assert!(manager.histories.is_empty());
    }

    #[test]
    fn test_config_persistence() {
        let config = test_config();
        let path = std::env::temp_dir().join("test_forecast_config.json");

        let manager = BandwidthForecastManager::new(config.clone());
        manager.save_config(&path).unwrap();

        let loaded = BandwidthForecastManager::load_config(&path).unwrap();
        assert_eq!(loaded, config);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_history_persistence() {
        let mut manager = BandwidthForecastManager::new(test_config());
        manager.record_sample("persist.com", 500_000.0, 1000);
        manager.record_sample("persist.com", 600_000.0, 2000);

        let path = std::env::temp_dir().join("test_forecast_hist.json");
        manager.save_histories(&path).unwrap();

        let loaded = BandwidthForecastManager::load_histories(&path).unwrap();
        assert!(loaded.contains_key("persist.com"));
        assert_eq!(loaded["persist.com"].samples.len(), 2);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_forecast_summary() {
        let mut manager = BandwidthForecastManager::new(test_config());
        for _ in 0..5 {
            manager.record_sample("a.com", 1_000_000.0, 0);
            manager.record_sample("b.com", 2_000_000.0, 0);
        }

        let summary = manager.get_summary();
        assert_eq!(summary.total_domains, 2);
        assert!(summary.avg_predicted_speed_bps > 0.0);
    }

    #[test]
    fn test_disabled_forecast() {
        let config = ForecastConfig {
            enabled: false,
            ..test_config()
        };
        let mut manager = BandwidthForecastManager::new(config);
        manager.record_sample("test.com", 1_000_000.0, 0);
        assert!(manager.histories.is_empty());
    }

    #[test]
    fn test_trend_detection() {
        let mut manager = BandwidthForecastManager::new(test_config());
        // Increasing trend
        for i in 0..6 {
            let speed = 500_000.0 + (i as f64 * 100_000.0);
            manager.record_sample("up.com", speed, 0);
        }
        let forecast = manager.forecast("up.com");
        assert_eq!(forecast.trend, ForecastTrend::Increasing);
    }

    #[test]
    fn test_tod_average() {
        let mut history = DomainSpeedHistory::new("tod.com".to_string());
        let now = Utc::now();
        let current_tod = TimeOfDay::from_hour(now.hour());

        for i in 0..5 {
            let sample = ForecastSample {
                timestamp: now,
                speed_bps: 1_000_000.0 + (i as f64 * 100_000.0),
                bytes_downloaded: 0,
            };
            history.add_sample(sample, 50);
        }

        let avg = history.get_tod_average(current_tod);
        assert!(avg.is_some());
        assert!(avg.unwrap() > 0.0);
    }

    #[test]
    fn test_summary_display() {
        let summary = ForecastSummary {
            total_domains: 2,
            high_confidence_count: 1,
            avg_predicted_speed_bps: 1_500_000.0,
            forecasts: vec![BandwidthForecast {
                key: "test.com".to_string(),
                predicted_speed_bps: 1_500_000.0,
                min_speed_bps: 1_000_000.0,
                max_speed_bps: 2_000_000.0,
                confidence: ForecastConfidence::High,
                confidence_score: 0.8,
                sample_count: 10,
                time_pattern: Some(TimeOfDay::Afternoon),
                generated_at: Utc::now(),
                trend: ForecastTrend::Stable,
            }],
        };

        let display = summary.display();
        assert!(display.contains("Bandwidth Forecast Summary"));
        assert!(display.contains("test.com"));
    }
}
