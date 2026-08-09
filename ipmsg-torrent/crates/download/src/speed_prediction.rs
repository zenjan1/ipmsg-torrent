//! Download speed prediction and optimal window recommendation
//!
//! Analyzes historical speed data per domain to predict completion times
//! and recommend optimal download windows based on time-of-day patterns.

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Speed sample for a specific domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSpeedSample {
    /// Timestamp when this sample was recorded
    pub timestamp: DateTime<Utc>,
    /// Download speed in bytes per second
    pub speed_bps: f64,
    /// Hour of day (0-23) for pattern analysis
    pub hour: u8,
    /// Day of week (0=Monday, 6=Sunday)
    pub day_of_week: u8,
}

/// Speed statistics for a specific hour
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HourlyStats {
    /// Number of samples for this hour
    pub sample_count: u32,
    /// Average speed in bytes per second
    pub avg_speed_bps: f64,
    /// Minimum speed observed
    pub min_speed_bps: f64,
    /// Maximum speed observed
    pub max_speed_bps: f64,
    /// Sum of speeds for running average calculation
    speed_sum: f64,
    /// Sum of squared speeds for variance calculation
    speed_squared_sum: f64,
}

impl HourlyStats {
    /// Add a new speed sample to the statistics
    pub fn add_sample(&mut self, speed_bps: f64) {
        self.sample_count += 1;
        self.speed_sum += speed_bps;
        self.speed_squared_sum += speed_bps * speed_bps;
        self.avg_speed_bps = self.speed_sum / self.sample_count as f64;

        if self.sample_count == 1 {
            self.min_speed_bps = speed_bps;
            self.max_speed_bps = speed_bps;
        } else {
            self.min_speed_bps = self.min_speed_bps.min(speed_bps);
            self.max_speed_bps = self.max_speed_bps.max(speed_bps);
        }
    }

    /// Calculate standard deviation of speed
    pub fn speed_stddev(&self) -> f64 {
        if self.sample_count < 2 {
            return 0.0;
        }
        let variance = (self.speed_squared_sum / self.sample_count as f64)
            - (self.avg_speed_bps * self.avg_speed_bps);
        variance.sqrt()
    }

    /// Calculate coefficient of variation (stability indicator)
    /// Lower is more stable. < 0.3 is considered stable.
    pub fn coefficient_of_variation(&self) -> f64 {
        if self.avg_speed_bps <= 0.0 {
            return f64::MAX;
        }
        self.speed_stddev() / self.avg_speed_bps
    }
}

/// Speed profile for a specific domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSpeedProfile {
    /// Domain name (e.g., "example.com")
    pub domain: String,
    /// Hourly speed statistics (0-23)
    pub hourly_stats: Vec<HourlyStats>,
    /// Total number of samples recorded
    pub total_samples: u64,
    /// Last time this profile was updated
    pub last_updated: DateTime<Utc>,
    /// Overall average speed across all hours
    pub overall_avg_speed: f64,
    /// Overall sum for running average
    overall_speed_sum: f64,
}

impl DomainSpeedProfile {
    /// Create a new empty profile for a domain
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            hourly_stats: (0..24).map(|_| HourlyStats::default()).collect(),
            total_samples: 0,
            last_updated: Utc::now(),
            overall_avg_speed: 0.0,
            overall_speed_sum: 0.0,
        }
    }

    /// Add a speed sample to the profile
    pub fn add_sample(&mut self, speed_bps: f64, timestamp: DateTime<Utc>) {
        let hour = timestamp.hour() as usize;
        let day_of_week = timestamp.weekday().num_days_from_monday() as u8;

        self.hourly_stats[hour].add_sample(speed_bps);
        self.total_samples += 1;
        self.last_updated = timestamp;
        self.overall_speed_sum += speed_bps;
        self.overall_avg_speed = self.overall_speed_sum / self.total_samples as f64;

        // Store day_of_week for potential future use
        let _ = day_of_week;
    }

    /// Get predicted speed for a specific hour
    pub fn predicted_speed_for_hour(&self, hour: u8) -> f64 {
        self.hourly_stats[hour as usize].avg_speed_bps
    }

    /// Get the best hours for downloading (top N hours by average speed)
    pub fn best_hours(&self, count: usize) -> Vec<(u8, f64)> {
        let mut hours_with_speed: Vec<(u8, f64)> = self
            .hourly_stats
            .iter()
            .enumerate()
            .filter(|(_, stats)| stats.sample_count >= 3) // Need minimum samples
            .map(|(hour, stats)| (hour as u8, stats.avg_speed_bps))
            .collect();

        hours_with_speed.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        hours_with_speed.truncate(count);
        hours_with_speed
    }

    /// Get the worst hours for downloading (bottom N hours by average speed)
    pub fn worst_hours(&self, count: usize) -> Vec<(u8, f64)> {
        let mut hours_with_speed: Vec<(u8, f64)> = self
            .hourly_stats
            .iter()
            .enumerate()
            .filter(|(_, stats)| stats.sample_count >= 3)
            .map(|(hour, stats)| (hour as u8, stats.avg_speed_bps))
            .collect();

        hours_with_speed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        hours_with_speed.truncate(count);
        hours_with_speed
    }

    /// Check if a specific hour is good for downloading
    /// Returns: "excellent", "good", "fair", "poor", or "unknown"
    pub fn hour_quality(&self, hour: u8) -> &'static str {
        let stats = &self.hourly_stats[hour as usize];
        if stats.sample_count < 3 {
            return "unknown";
        }

        let overall = self.overall_avg_speed;
        if overall <= 0.0 {
            return "unknown";
        }

        let ratio = stats.avg_speed_bps / overall;
        match ratio {
            r if r >= 1.3 => "excellent",
            r if r >= 1.0 => "good",
            r if r >= 0.7 => "fair",
            _ => "poor",
        }
    }
}

/// Prediction result for a download task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedPrediction {
    /// Task ID
    pub task_id: String,
    /// Domain being downloaded from
    pub domain: String,
    /// Current speed in bytes per second
    pub current_speed_bps: f64,
    /// Predicted average speed based on historical data
    pub predicted_avg_speed_bps: f64,
    /// Predicted speed for current hour
    pub predicted_current_hour_speed_bps: f64,
    /// Remaining bytes to download
    pub remaining_bytes: u64,
    /// Predicted time to completion at current speed (seconds)
    pub eta_current_speed_secs: u64,
    /// Predicted time to completion at historical average (seconds)
    pub eta_historical_avg_secs: u64,
    /// Predicted time to completion at current hour's typical speed (seconds)
    pub eta_current_hour_secs: u64,
    /// Confidence level: "high", "medium", "low", "none"
    pub confidence: &'static str,
    /// Recommended action
    pub recommendation: PredictionRecommendation,
}

/// Recommendation based on speed prediction
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PredictionRecommendation {
    /// Good time to download, proceed normally
    Proceed,
    /// Better to wait for a specific hour
    WaitUntil {
        /// Recommended hour to start (0-23)
        hour: u8,
        /// Expected speed improvement factor
        speedup_factor: f64,
    },
    /// Speed is significantly below normal, consider alternatives
    ConsiderAlternatives {
        /// Suggested alternative domains
        alternatives: Vec<String>,
    },
    /// Not enough data to make a recommendation
    InsufficientData,
}

/// Configuration for speed prediction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedPredictionConfig {
    /// Minimum samples needed before making predictions
    pub min_samples_for_prediction: u32,
    /// Maximum age of samples to consider (hours)
    pub sample_retention_hours: u64,
    /// Speed ratio threshold for "wait" recommendation (current/predicted < threshold = wait)
    pub wait_threshold_ratio: f64,
    /// Enable speed prediction feature
    pub enabled: bool,
}

impl Default for SpeedPredictionConfig {
    fn default() -> Self {
        Self {
            min_samples_for_prediction: 10,
            sample_retention_hours: 168, // 7 days
            wait_threshold_ratio: 0.5,
            enabled: true,
        }
    }
}

/// Speed prediction manager
#[derive(Debug, Default)]
pub struct SpeedPredictionManager {
    /// Per-domain speed profiles
    profiles: HashMap<String, DomainSpeedProfile>,
    /// Configuration
    config: SpeedPredictionConfig,
}

impl SpeedPredictionManager {
    /// Create a new speed prediction manager
    pub fn new(config: SpeedPredictionConfig) -> Self {
        Self {
            profiles: HashMap::new(),
            config,
        }
    }

    /// Record a speed sample for a domain
    pub fn record_speed(&mut self, domain: &str, speed_bps: f64) {
        let profile = self
            .profiles
            .entry(domain.to_string())
            .or_insert_with(|| DomainSpeedProfile::new(domain.to_string()));
        profile.add_sample(speed_bps, Utc::now());
    }

    /// Record a speed sample with a specific timestamp
    pub fn record_speed_at(&mut self, domain: &str, speed_bps: f64, timestamp: DateTime<Utc>) {
        let profile = self
            .profiles
            .entry(domain.to_string())
            .or_insert_with(|| DomainSpeedProfile::new(domain.to_string()));
        profile.add_sample(speed_bps, timestamp);
    }

    /// Get the speed profile for a domain
    pub fn get_profile(&self, domain: &str) -> Option<&DomainSpeedProfile> {
        self.profiles.get(domain)
    }

    /// Get all tracked domains
    pub fn tracked_domains(&self) -> Vec<&str> {
        self.profiles.keys().map(|s| s.as_str()).collect()
    }

    /// Predict completion time for a download
    pub fn predict(
        &self,
        task_id: &str,
        domain: &str,
        current_speed_bps: f64,
        remaining_bytes: u64,
    ) -> SpeedPrediction {
        let profile = self.profiles.get(domain);

        // Calculate ETA at current speed
        let eta_current = if current_speed_bps > 0.0 {
            (remaining_bytes as f64 / current_speed_bps) as u64
        } else {
            u64::MAX
        };

        // If no profile or insufficient data
        let (profile, has_data) = match profile {
            Some(p) if p.total_samples >= self.config.min_samples_for_prediction as u64 => {
                (p, true)
            }
            Some(p) => (p, false),
            None => {
                return SpeedPrediction {
                    task_id: task_id.to_string(),
                    domain: domain.to_string(),
                    current_speed_bps,
                    predicted_avg_speed_bps: 0.0,
                    predicted_current_hour_speed_bps: 0.0,
                    remaining_bytes,
                    eta_current_speed_secs: eta_current,
                    eta_historical_avg_secs: u64::MAX,
                    eta_current_hour_secs: u64::MAX,
                    confidence: "none",
                    recommendation: PredictionRecommendation::InsufficientData,
                };
            }
        };

        let current_hour = Utc::now().hour() as u8;
        let predicted_avg = profile.overall_avg_speed;
        let predicted_hour = profile.predicted_speed_for_hour(current_hour);

        // Calculate ETAs
        let eta_historical = if predicted_avg > 0.0 {
            (remaining_bytes as f64 / predicted_avg) as u64
        } else {
            u64::MAX
        };

        let eta_hour = if predicted_hour > 0.0 {
            (remaining_bytes as f64 / predicted_hour) as u64
        } else {
            u64::MAX
        };

        // Determine confidence based on sample count
        let confidence = if profile.total_samples >= 100 {
            "high"
        } else if profile.total_samples >= 30 {
            "medium"
        } else if has_data {
            "low"
        } else {
            "none"
        };

        // Generate recommendation
        let recommendation = if !has_data {
            PredictionRecommendation::InsufficientData
        } else {
            let hour_stats = &profile.hourly_stats[current_hour as usize];
            let hour_cv = hour_stats.coefficient_of_variation();

            // If current hour is stable and good, proceed
            if hour_cv < 0.5 && profile.hour_quality(current_hour) != "poor" {
                PredictionRecommendation::Proceed
            } else {
                // Check if there's a significantly better hour
                let best_hours = profile.best_hours(3);
                if let Some((best_hour, best_speed)) = best_hours.first() {
                    let current_hour_speed = profile.predicted_speed_for_hour(current_hour);
                    if current_hour_speed > 0.0 && *best_speed / current_hour_speed > 1.5 {
                        PredictionRecommendation::WaitUntil {
                            hour: *best_hour,
                            speedup_factor: *best_speed / current_hour_speed,
                        }
                    } else {
                        PredictionRecommendation::Proceed
                    }
                } else {
                    PredictionRecommendation::Proceed
                }
            }
        };

        SpeedPrediction {
            task_id: task_id.to_string(),
            domain: domain.to_string(),
            current_speed_bps,
            predicted_avg_speed_bps: predicted_avg,
            predicted_current_hour_speed_bps: predicted_hour,
            remaining_bytes,
            eta_current_speed_secs: eta_current,
            eta_historical_avg_secs: eta_historical,
            eta_current_hour_secs: eta_hour,
            confidence,
            recommendation,
        }
    }

    /// Get optimal download windows for a domain
    pub fn get_optimal_windows(&self, domain: &str, top_n: usize) -> Vec<OptimalWindow> {
        let Some(profile) = self.profiles.get(domain) else {
            return Vec::new();
        };

        let best_hours = profile.best_hours(top_n);
        best_hours
            .into_iter()
            .map(|(hour, speed)| OptimalWindow {
                start_hour: hour,
                end_hour: (hour + 1) % 24,
                predicted_speed_bps: speed,
                quality: profile.hour_quality(hour).to_string(),
                sample_count: profile.hourly_stats[hour as usize].sample_count,
            })
            .collect()
    }

    /// Get a summary of all tracked domains
    pub fn get_summary(&self) -> SpeedPredictionSummary {
        let mut domain_summaries: Vec<DomainSummary> = self
            .profiles
            .values()
            .map(|p| DomainSummary {
                domain: p.domain.clone(),
                total_samples: p.total_samples,
                overall_avg_speed: p.overall_avg_speed,
                best_hour: p.best_hours(1).first().map(|(h, s)| (*h, *s)),
                worst_hour: p.worst_hours(1).first().map(|(h, s)| (*h, *s)),
                last_updated: p.last_updated,
            })
            .collect();

        domain_summaries.sort_by_key(|d| std::cmp::Reverse(d.total_samples));

        SpeedPredictionSummary {
            tracked_domains: self.profiles.len(),
            domain_summaries,
            config_enabled: self.config.enabled,
        }
    }

    /// Remove a domain profile
    pub fn remove_domain(&mut self, domain: &str) -> bool {
        self.profiles.remove(domain).is_some()
    }

    /// Clear all profiles
    pub fn clear_all(&mut self) {
        self.profiles.clear();
    }

    /// Get configuration
    pub fn config(&self) -> &SpeedPredictionConfig {
        &self.config
    }

    /// Set configuration
    pub fn set_config(&mut self, config: SpeedPredictionConfig) {
        self.config = config;
    }

    /// Clean up old samples beyond retention period
    pub fn cleanup_old_samples(&mut self) {
        let cutoff = Utc::now() - Duration::hours(self.config.sample_retention_hours as i64);
        for profile in self.profiles.values_mut() {
            // Rebuild hourly stats from scratch would require storing individual samples
            // For now, just update the last_updated check
            if profile.last_updated < cutoff {
                // Profile is too old, could mark for removal
                // But we keep it for historical reference
            }
        }
    }
}

/// Optimal download window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimalWindow {
    /// Start hour (0-23)
    pub start_hour: u8,
    /// End hour (0-23, exclusive)
    pub end_hour: u8,
    /// Predicted average speed during this window
    pub predicted_speed_bps: f64,
    /// Quality rating: "excellent", "good", "fair", "poor"
    pub quality: String,
    /// Number of samples this prediction is based on
    pub sample_count: u32,
}

/// Summary for a single domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSummary {
    /// Domain name
    pub domain: String,
    /// Total samples recorded
    pub total_samples: u64,
    /// Overall average speed
    pub overall_avg_speed: f64,
    /// Best hour and predicted speed
    pub best_hour: Option<(u8, f64)>,
    /// Worst hour and predicted speed
    pub worst_hour: Option<(u8, f64)>,
    /// Last update timestamp
    pub last_updated: DateTime<Utc>,
}

/// Summary of speed prediction for all domains
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedPredictionSummary {
    /// Number of tracked domains
    pub tracked_domains: usize,
    /// Per-domain summaries
    pub domain_summaries: Vec<DomainSummary>,
    /// Whether prediction is enabled
    pub config_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_timestamp(hour: u8) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 10, hour as u32, 0, 0)
            .unwrap()
    }

    #[test]
    fn test_hourly_stats_basic() {
        let mut stats = HourlyStats::default();
        stats.add_sample(1000.0);
        stats.add_sample(2000.0);
        stats.add_sample(3000.0);

        assert_eq!(stats.sample_count, 3);
        assert!((stats.avg_speed_bps - 2000.0).abs() < 0.01);
        assert!((stats.min_speed_bps - 1000.0).abs() < 0.01);
        assert!((stats.max_speed_bps - 3000.0).abs() < 0.01);
    }

    #[test]
    fn test_hourly_stats_stddev() {
        let mut stats = HourlyStats::default();
        // All same value = zero stddev
        stats.add_sample(1000.0);
        stats.add_sample(1000.0);
        stats.add_sample(1000.0);
        assert!(stats.speed_stddev() < 0.01);

        // Different values
        let mut stats2 = HourlyStats::default();
        stats2.add_sample(100.0);
        stats2.add_sample(200.0);
        stats2.add_sample(300.0);
        assert!(stats2.speed_stddev() > 0.0);
    }

    #[test]
    fn test_domain_profile_creation() {
        let profile = DomainSpeedProfile::new("example.com".to_string());
        assert_eq!(profile.domain, "example.com");
        assert_eq!(profile.total_samples, 0);
        assert_eq!(profile.hourly_stats.len(), 24);
    }

    #[test]
    fn test_domain_profile_add_samples() {
        let mut profile = DomainSpeedProfile::new("example.com".to_string());

        // Add samples at different hours
        profile.add_sample(1000.0, make_timestamp(10));
        profile.add_sample(2000.0, make_timestamp(10));
        profile.add_sample(3000.0, make_timestamp(14));
        profile.add_sample(4000.0, make_timestamp(14));

        assert_eq!(profile.total_samples, 4);
        assert!((profile.overall_avg_speed - 2500.0).abs() < 0.01);
        assert!((profile.predicted_speed_for_hour(10) - 1500.0).abs() < 0.01);
        assert!((profile.predicted_speed_for_hour(14) - 3500.0).abs() < 0.01);
    }

    #[test]
    fn test_best_worst_hours() {
        let mut profile = DomainSpeedProfile::new("example.com".to_string());

        // Create a pattern: fast in morning, slow in afternoon
        for _ in 0..10 {
            profile.add_sample(5000.0, make_timestamp(8)); // Morning: fast
        }
        for _ in 0..10 {
            profile.add_sample(1000.0, make_timestamp(14)); // Afternoon: slow
        }
        for _ in 0..10 {
            profile.add_sample(3000.0, make_timestamp(20)); // Evening: medium
        }

        let best = profile.best_hours(2);
        assert_eq!(best.len(), 2);
        assert_eq!(best[0].0, 8); // Morning is best
        assert!(best[0].1 > best[1].1);

        let worst = profile.worst_hours(1);
        assert_eq!(worst.len(), 1);
        assert_eq!(worst[0].0, 14); // Afternoon is worst
    }

    #[test]
    fn test_hour_quality() {
        let mut profile = DomainSpeedProfile::new("example.com".to_string());

        // Add samples to establish overall average (2 samples per hour)
        for h in 0..24 {
            if h == 10 || h == 14 {
                continue; // Skip these, will add more below
            }
            for _ in 0..2 {
                profile.add_sample(2000.0, make_timestamp(h));
            }
        }

        // Make hour 10 excellent (much faster than average)
        for _ in 0..10 {
            profile.add_sample(4000.0, make_timestamp(10));
        }

        // Make hour 14 poor (much slower than average)
        for _ in 0..10 {
            profile.add_sample(500.0, make_timestamp(14));
        }

        assert_eq!(profile.hour_quality(10), "excellent");
        assert_eq!(profile.hour_quality(14), "poor");
        assert_eq!(profile.hour_quality(3), "unknown"); // Not enough samples (only 2)
    }

    #[test]
    fn test_speed_prediction_manager_basic() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig::default());

        // Record some speeds
        manager.record_speed_at("example.com", 1000.0, make_timestamp(10));
        manager.record_speed_at("example.com", 2000.0, make_timestamp(10));
        manager.record_speed_at("example.com", 1500.0, make_timestamp(14));

        let profile = manager.get_profile("example.com");
        assert!(profile.is_some());
        assert_eq!(profile.unwrap().total_samples, 3);
    }

    #[test]
    fn test_prediction_insufficient_data() {
        let manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 10,
            ..Default::default()
        });

        // No data at all
        let prediction = manager.predict("task1", "unknown.com", 1000.0, 1_000_000);
        assert_eq!(prediction.confidence, "none");
        assert_eq!(
            prediction.recommendation,
            PredictionRecommendation::InsufficientData
        );
    }

    #[test]
    fn test_prediction_with_data() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 5,
            ..Default::default()
        });

        // Add enough samples
        for _ in 0..20 {
            manager.record_speed_at("example.com", 2000.0, make_timestamp(10));
        }

        let prediction = manager.predict("task1", "example.com", 2000.0, 10_000_000);
        assert_ne!(prediction.confidence, "none");
        assert!(prediction.predicted_avg_speed_bps > 0.0);
        assert!(prediction.eta_historical_avg_secs > 0);
    }

    #[test]
    fn test_optimal_windows() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig::default());

        // Create a clear pattern
        for _ in 0..10 {
            manager.record_speed_at("example.com", 5000.0, make_timestamp(3));
        }
        for _ in 0..10 {
            manager.record_speed_at("example.com", 1000.0, make_timestamp(15));
        }

        let windows = manager.get_optimal_windows("example.com", 2);
        assert!(!windows.is_empty());
        assert_eq!(windows[0].start_hour, 3);
        assert_eq!(windows[0].quality, "excellent");
    }

    #[test]
    fn test_summary() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig::default());

        manager.record_speed("example.com", 1000.0);
        manager.record_speed("other.com", 2000.0);

        let summary = manager.get_summary();
        assert_eq!(summary.tracked_domains, 2);
        assert_eq!(summary.domain_summaries.len(), 2);
        assert!(summary.config_enabled);
    }

    #[test]
    fn test_remove_domain() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig::default());

        manager.record_speed("example.com", 1000.0);
        assert!(manager.get_profile("example.com").is_some());

        assert!(manager.remove_domain("example.com"));
        assert!(manager.get_profile("example.com").is_none());

        assert!(!manager.remove_domain("nonexistent.com"));
    }

    #[test]
    fn test_clear_all() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig::default());

        manager.record_speed("a.com", 1000.0);
        manager.record_speed("b.com", 2000.0);
        assert_eq!(manager.tracked_domains().len(), 2);

        manager.clear_all();
        assert_eq!(manager.tracked_domains().len(), 0);
    }

    #[test]
    fn test_config() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig::default());

        assert!(manager.config().enabled);
        assert_eq!(manager.config().min_samples_for_prediction, 10);

        manager.set_config(SpeedPredictionConfig {
            enabled: false,
            min_samples_for_prediction: 50,
            ..Default::default()
        });

        assert!(!manager.config().enabled);
        assert_eq!(manager.config().min_samples_for_prediction, 50);
    }

    #[test]
    fn test_coefficient_of_variation() {
        let mut stats = HourlyStats::default();
        // All same value = CV of 0
        stats.add_sample(1000.0);
        stats.add_sample(1000.0);
        stats.add_sample(1000.0);
        assert!(stats.coefficient_of_variation() < 0.01);

        // High variance = high CV
        let mut stats2 = HourlyStats::default();
        stats2.add_sample(100.0);
        stats2.add_sample(10000.0);
        stats2.add_sample(100.0);
        assert!(stats2.coefficient_of_variation() > 1.0);
    }

    #[test]
    fn test_tracked_domains() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig::default());

        manager.record_speed("alpha.com", 1000.0);
        manager.record_speed("beta.com", 2000.0);
        manager.record_speed("gamma.com", 3000.0);

        let mut domains = manager.tracked_domains();
        domains.sort();
        assert_eq!(domains, vec!["alpha.com", "beta.com", "gamma.com"]);
    }

    #[test]
    fn test_prediction_eta_calculation() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 5,
            ..Default::default()
        });

        // Add samples at known speed (1000 B/s)
        for _ in 0..10 {
            manager.record_speed_at("example.com", 1000.0, make_timestamp(10));
        }

        let prediction = manager.predict("task1", "example.com", 1000.0, 10_000);
        // At 1000 B/s, 10000 bytes should take 10 seconds
        assert!(prediction.eta_current_speed_secs <= 11);
        assert!(prediction.eta_historical_avg_secs <= 11);
    }

    #[test]
    fn test_recommendation_proceed_stable() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 5,
            ..Default::default()
        });

        // Add stable samples at current hour
        let current_hour = Utc::now().hour() as u8;
        for _ in 0..20 {
            manager.record_speed_at("example.com", 2000.0, make_timestamp(current_hour));
        }

        let prediction = manager.predict("task1", "example.com", 2000.0, 100_000);
        // Stable speed should recommend Proceed
        assert_eq!(prediction.recommendation, PredictionRecommendation::Proceed);
    }
}
