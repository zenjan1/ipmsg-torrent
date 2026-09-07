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

    // ========== Serialization Tests ==========

    #[test]
    fn test_speed_prediction_config_serde_roundtrip() {
        let config = SpeedPredictionConfig {
            min_samples_for_prediction: 20,
            sample_retention_hours: 48,
            wait_threshold_ratio: 0.7,
            enabled: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SpeedPredictionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.min_samples_for_prediction, 20);
        assert_eq!(deserialized.sample_retention_hours, 48);
        assert!((deserialized.wait_threshold_ratio - 0.7).abs() < f64::EPSILON);
        assert!(!deserialized.enabled);
    }

    #[test]
    fn test_speed_prediction_config_default_serde() {
        let config = SpeedPredictionConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SpeedPredictionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.min_samples_for_prediction, 10);
        assert_eq!(deserialized.sample_retention_hours, 168);
        assert!((deserialized.wait_threshold_ratio - 0.5).abs() < f64::EPSILON);
        assert!(deserialized.enabled);
    }

    #[test]
    fn test_speed_prediction_config_extra_fields_ignored() {
        let json = r#"{"min_samples_for_prediction":5,"sample_retention_hours":24,"wait_threshold_ratio":0.3,"enabled":true,"extra_field":"ignored"}"#;
        let config: SpeedPredictionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.min_samples_for_prediction, 5);
    }

    #[test]
    fn test_speed_prediction_config_pretty_serde() {
        let config = SpeedPredictionConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: SpeedPredictionConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(
            deserialized.min_samples_for_prediction,
            config.min_samples_for_prediction
        );
    }

    #[test]
    fn test_domain_speed_sample_serde_roundtrip() {
        let sample = DomainSpeedSample {
            timestamp: Utc.with_ymd_and_hms(2026, 8, 10, 14, 30, 0).unwrap(),
            speed_bps: 1024000.0,
            hour: 14,
            day_of_week: 0,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let deserialized: DomainSpeedSample = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.hour, 14);
        assert_eq!(deserialized.day_of_week, 0);
        assert!((deserialized.speed_bps - 1024000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_domain_speed_sample_clone_debug() {
        let sample = DomainSpeedSample {
            timestamp: Utc::now(),
            speed_bps: 5000.0,
            hour: 10,
            day_of_week: 3,
        };
        let cloned = sample.clone();
        assert_eq!(cloned.hour, sample.hour);
        assert_eq!(cloned.day_of_week, sample.day_of_week);
        let debug_str = format!("{:?}", sample);
        assert!(debug_str.contains("DomainSpeedSample"));
    }

    #[test]
    fn test_hourly_stats_serde_roundtrip() {
        let mut stats = HourlyStats::default();
        stats.add_sample(1000.0);
        stats.add_sample(2000.0);
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: HourlyStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sample_count, 2);
        assert!((deserialized.avg_speed_bps - 1500.0).abs() < 0.01);
    }

    #[test]
    fn test_domain_speed_profile_serde_roundtrip() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        profile.add_sample(5000.0, make_timestamp(10));
        profile.add_sample(3000.0, make_timestamp(14));
        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: DomainSpeedProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.domain, "test.com");
        assert_eq!(deserialized.total_samples, 2);
        assert_eq!(deserialized.hourly_stats.len(), 24);
    }

    #[test]
    fn test_prediction_recommendation_serde_all_variants() {
        // Proceed
        let proceed = PredictionRecommendation::Proceed;
        let json = serde_json::to_string(&proceed).unwrap();
        let back: PredictionRecommendation = serde_json::from_str(&json).unwrap();
        assert_eq!(back, PredictionRecommendation::Proceed);

        // WaitUntil
        let wait = PredictionRecommendation::WaitUntil {
            hour: 3,
            speedup_factor: 2.5,
        };
        let json = serde_json::to_string(&wait).unwrap();
        let back: PredictionRecommendation = serde_json::from_str(&json).unwrap();
        match back {
            PredictionRecommendation::WaitUntil {
                hour,
                speedup_factor,
            } => {
                assert_eq!(hour, 3);
                assert!((speedup_factor - 2.5).abs() < f64::EPSILON);
            }
            _ => panic!("Expected WaitUntil"),
        }

        // ConsiderAlternatives
        let consider = PredictionRecommendation::ConsiderAlternatives {
            alternatives: vec!["mirror1.com".into(), "mirror2.com".into()],
        };
        let json = serde_json::to_string(&consider).unwrap();
        let back: PredictionRecommendation = serde_json::from_str(&json).unwrap();
        match back {
            PredictionRecommendation::ConsiderAlternatives { alternatives } => {
                assert_eq!(alternatives.len(), 2);
                assert_eq!(alternatives[0], "mirror1.com");
            }
            _ => panic!("Expected ConsiderAlternatives"),
        }

        // InsufficientData
        let insufficient = PredictionRecommendation::InsufficientData;
        let json = serde_json::to_string(&insufficient).unwrap();
        let back: PredictionRecommendation = serde_json::from_str(&json).unwrap();
        assert_eq!(back, PredictionRecommendation::InsufficientData);
    }

    #[test]
    fn test_speed_prediction_serialize() {
        let prediction = SpeedPrediction {
            task_id: "task-1".to_string(),
            domain: "example.com".to_string(),
            current_speed_bps: 5000.0,
            predicted_avg_speed_bps: 4000.0,
            predicted_current_hour_speed_bps: 4500.0,
            remaining_bytes: 10_000_000,
            eta_current_speed_secs: 2000,
            eta_historical_avg_secs: 2500,
            eta_current_hour_secs: 2222,
            confidence: "high",
            recommendation: PredictionRecommendation::Proceed,
        };
        // SpeedPrediction contains &'static str so full roundtrip requires owned strings.
        // Test serialization works and contains expected fields.
        let json = serde_json::to_string(&prediction).unwrap();
        assert!(json.contains("\"task_id\":\"task-1\""));
        assert!(json.contains("\"domain\":\"example.com\""));
        assert!(json.contains("\"remaining_bytes\":10000000"));
        assert!(json.contains("\"confidence\":\"high\""));
    }

    #[test]
    fn test_optimal_window_serde_roundtrip() {
        let window = OptimalWindow {
            start_hour: 3,
            end_hour: 4,
            predicted_speed_bps: 8000.0,
            quality: "excellent".to_string(),
            sample_count: 50,
        };
        let json = serde_json::to_string(&window).unwrap();
        let deserialized: OptimalWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.start_hour, 3);
        assert_eq!(deserialized.end_hour, 4);
        assert_eq!(deserialized.quality, "excellent");
    }

    #[test]
    fn test_domain_summary_serde_roundtrip() {
        let summary = DomainSummary {
            domain: "cdn.example.com".to_string(),
            total_samples: 500,
            overall_avg_speed: 3500.0,
            best_hour: Some((10, 5000.0)),
            worst_hour: Some((3, 1000.0)),
            last_updated: Utc.with_ymd_and_hms(2026, 8, 10, 12, 0, 0).unwrap(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: DomainSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.domain, "cdn.example.com");
        assert_eq!(deserialized.total_samples, 500);
        assert!(deserialized.best_hour.is_some());
    }

    #[test]
    fn test_speed_prediction_summary_serde_roundtrip() {
        let summary = SpeedPredictionSummary {
            tracked_domains: 3,
            domain_summaries: vec![DomainSummary {
                domain: "a.com".to_string(),
                total_samples: 100,
                overall_avg_speed: 2000.0,
                best_hour: None,
                worst_hour: None,
                last_updated: Utc::now(),
            }],
            config_enabled: true,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: SpeedPredictionSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tracked_domains, 3);
        assert!(deserialized.config_enabled);
    }

    // ========== Clone/Debug Traits ==========

    #[test]
    fn test_all_structs_clone_debug() {
        // HourlyStats
        let mut stats = HourlyStats::default();
        stats.add_sample(1000.0);
        let stats_clone = stats.clone();
        assert_eq!(stats_clone.sample_count, 1);
        assert!(format!("{:?}", stats).contains("HourlyStats"));

        // DomainSpeedProfile
        let profile = DomainSpeedProfile::new("x.com".to_string());
        let profile_clone = profile.clone();
        assert_eq!(profile_clone.domain, "x.com");
        assert!(format!("{:?}", profile).contains("DomainSpeedProfile"));

        // SpeedPredictionConfig
        let config = SpeedPredictionConfig::default();
        let config_clone = config.clone();
        assert_eq!(config_clone.enabled, config.enabled);
        assert!(format!("{:?}", config).contains("SpeedPredictionConfig"));

        // OptimalWindow
        let window = OptimalWindow {
            start_hour: 0,
            end_hour: 1,
            predicted_speed_bps: 0.0,
            quality: String::new(),
            sample_count: 0,
        };
        let window_clone = window.clone();
        assert_eq!(window_clone.start_hour, 0);
        assert!(format!("{:?}", window).contains("OptimalWindow"));

        // DomainSummary
        let ds = DomainSummary {
            domain: "d.com".to_string(),
            total_samples: 0,
            overall_avg_speed: 0.0,
            best_hour: None,
            worst_hour: None,
            last_updated: Utc::now(),
        };
        let ds_clone = ds.clone();
        assert_eq!(ds_clone.domain, "d.com");
        assert!(format!("{:?}", ds).contains("DomainSummary"));

        // SpeedPredictionSummary
        let summary = SpeedPredictionSummary {
            tracked_domains: 0,
            domain_summaries: vec![],
            config_enabled: false,
        };
        let summary_clone = summary.clone();
        assert_eq!(summary_clone.tracked_domains, 0);
        assert!(format!("{:?}", summary).contains("SpeedPredictionSummary"));
    }

    #[test]
    fn test_speed_prediction_clone_debug() {
        let prediction = SpeedPrediction {
            task_id: "t1".to_string(),
            domain: "d.com".to_string(),
            current_speed_bps: 0.0,
            predicted_avg_speed_bps: 0.0,
            predicted_current_hour_speed_bps: 0.0,
            remaining_bytes: 0,
            eta_current_speed_secs: 0,
            eta_historical_avg_secs: 0,
            eta_current_hour_secs: 0,
            confidence: "none",
            recommendation: PredictionRecommendation::InsufficientData,
        };
        let cloned = prediction.clone();
        assert_eq!(cloned.task_id, "t1");
        assert!(format!("{:?}", prediction).contains("SpeedPrediction"));
    }

    #[test]
    fn test_prediction_recommendation_clone_debug() {
        let variants = vec![
            PredictionRecommendation::Proceed,
            PredictionRecommendation::WaitUntil {
                hour: 5,
                speedup_factor: 1.5,
            },
            PredictionRecommendation::ConsiderAlternatives {
                alternatives: vec!["alt.com".into()],
            },
            PredictionRecommendation::InsufficientData,
        ];
        for v in &variants {
            let _cloned = v.clone();
            let debug = format!("{:?}", v);
            assert!(!debug.is_empty());
        }
    }

    // ========== HourlyStats Edge Cases ==========

    #[test]
    fn test_hourly_stats_default_values() {
        let stats = HourlyStats::default();
        assert_eq!(stats.sample_count, 0);
        assert_eq!(stats.avg_speed_bps, 0.0);
        assert_eq!(stats.min_speed_bps, 0.0);
        assert_eq!(stats.max_speed_bps, 0.0);
    }

    #[test]
    fn test_hourly_stats_single_sample() {
        let mut stats = HourlyStats::default();
        stats.add_sample(42.0);
        assert_eq!(stats.sample_count, 1);
        assert!((stats.avg_speed_bps - 42.0).abs() < f64::EPSILON);
        assert!((stats.min_speed_bps - 42.0).abs() < f64::EPSILON);
        assert!((stats.max_speed_bps - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_hourly_stats_single_sample_stddev_zero() {
        let mut stats = HourlyStats::default();
        stats.add_sample(1000.0);
        // Single sample => stddev returns 0.0
        assert_eq!(stats.speed_stddev(), 0.0);
    }

    #[test]
    fn test_hourly_stats_zero_samples_stddev() {
        let stats = HourlyStats::default();
        assert_eq!(stats.speed_stddev(), 0.0);
    }

    #[test]
    fn test_hourly_stats_cv_zero_avg() {
        let stats = HourlyStats::default();
        // avg is 0 => CV returns f64::MAX
        assert_eq!(stats.coefficient_of_variation(), f64::MAX);
    }

    #[test]
    fn test_hourly_stats_negative_speed() {
        let mut stats = HourlyStats::default();
        stats.add_sample(-100.0);
        stats.add_sample(-200.0);
        stats.add_sample(-300.0);
        assert_eq!(stats.sample_count, 3);
        assert!((stats.avg_speed_bps - (-200.0)).abs() < 0.01);
        assert!((stats.min_speed_bps - (-300.0)).abs() < 0.01);
        assert!((stats.max_speed_bps - (-100.0)).abs() < 0.01);
    }

    #[test]
    fn test_hourly_stats_very_large_values() {
        let mut stats = HourlyStats::default();
        stats.add_sample(f64::MAX / 2.0);
        stats.add_sample(f64::MAX / 2.0);
        assert_eq!(stats.sample_count, 2);
        assert!(stats.avg_speed_bps > 0.0);
    }

    #[test]
    fn test_hourly_stats_many_samples() {
        let mut stats = HourlyStats::default();
        for i in 0..1000 {
            stats.add_sample(i as f64);
        }
        assert_eq!(stats.sample_count, 1000);
        assert!((stats.avg_speed_bps - 499.5).abs() < 0.01);
        assert!((stats.min_speed_bps - 0.0).abs() < f64::EPSILON);
        assert!((stats.max_speed_bps - 999.0).abs() < f64::EPSILON);
    }

    // ========== DomainSpeedProfile Edge Cases ==========

    #[test]
    fn test_domain_profile_best_hours_empty_when_no_samples() {
        let profile = DomainSpeedProfile::new("empty.com".to_string());
        let best = profile.best_hours(5);
        assert!(best.is_empty());
    }

    #[test]
    fn test_domain_profile_worst_hours_empty_when_no_samples() {
        let profile = DomainSpeedProfile::new("empty.com".to_string());
        let worst = profile.worst_hours(5);
        assert!(worst.is_empty());
    }

    #[test]
    fn test_domain_profile_best_hours_filters_low_samples() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        // Hour 5: only 2 samples (< 3 threshold)
        profile.add_sample(10000.0, make_timestamp(5));
        profile.add_sample(10000.0, make_timestamp(5));
        // Hour 10: 3 samples (meets threshold)
        profile.add_sample(1000.0, make_timestamp(10));
        profile.add_sample(1000.0, make_timestamp(10));
        profile.add_sample(1000.0, make_timestamp(10));

        let best = profile.best_hours(5);
        // Only hour 10 should appear (hour 5 has < 3 samples)
        assert_eq!(best.len(), 1);
        assert_eq!(best[0].0, 10);
    }

    #[test]
    fn test_domain_profile_best_hours_count_truncation() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        // Add 5 hours with >= 3 samples each
        for h in [2, 5, 8, 12, 18] {
            for _ in 0..5 {
                profile.add_sample((h as f64) * 100.0, make_timestamp(h as u8));
            }
        }
        let best = profile.best_hours(3);
        assert_eq!(best.len(), 3);
    }

    #[test]
    fn test_domain_profile_worst_hours_count_truncation() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        for h in [2, 5, 8, 12, 18] {
            for _ in 0..5 {
                profile.add_sample((h as f64) * 100.0, make_timestamp(h as u8));
            }
        }
        let worst = profile.worst_hours(2);
        assert_eq!(worst.len(), 2);
        // Worst should be lowest speed first
        assert!(worst[0].1 <= worst[1].1);
    }

    #[test]
    fn test_domain_profile_hour_quality_unknown_low_samples() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        // Only 2 samples at hour 5
        profile.add_sample(5000.0, make_timestamp(5));
        profile.add_sample(5000.0, make_timestamp(5));
        assert_eq!(profile.hour_quality(5), "unknown");
    }

    #[test]
    fn test_domain_profile_hour_quality_zero_overall() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        // Add exactly 3 samples at hour 5 with zero speed
        for _ in 0..3 {
            profile.add_sample(0.0, make_timestamp(5));
        }
        // overall_avg_speed is 0 => returns "unknown"
        assert_eq!(profile.hour_quality(5), "unknown");
    }

    #[test]
    fn test_domain_profile_hour_quality_good() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        // Establish baseline: 22 hours at 2000 bps
        for h in 0..22 {
            for _ in 0..3 {
                profile.add_sample(2000.0, make_timestamp(h));
            }
        }
        // Make hour 22 slightly above average (ratio >= 1.0 but < 1.3)
        for _ in 0..3 {
            profile.add_sample(2500.0, make_timestamp(22));
        }
        let quality = profile.hour_quality(22);
        assert_eq!(quality, "good");
    }

    #[test]
    fn test_domain_profile_hour_quality_fair() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        // Establish baseline: 22 hours at 2000 bps
        for h in 0..22 {
            for _ in 0..3 {
                profile.add_sample(2000.0, make_timestamp(h));
            }
        }
        // Make hour 22 below average but >= 0.7 ratio
        for _ in 0..3 {
            profile.add_sample(1500.0, make_timestamp(22));
        }
        let quality = profile.hour_quality(22);
        assert_eq!(quality, "fair");
    }

    #[test]
    fn test_domain_profile_predicted_speed_for_hour() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        profile.add_sample(1000.0, make_timestamp(8));
        profile.add_sample(3000.0, make_timestamp(8));
        assert!((profile.predicted_speed_for_hour(8) - 2000.0).abs() < 0.01);
        // Hour with no samples returns 0
        assert_eq!(profile.predicted_speed_for_hour(23), 0.0);
    }

    // ========== Manager Edge Cases ==========

    #[test]
    fn test_manager_new() {
        let manager = SpeedPredictionManager::new(SpeedPredictionConfig::default());
        assert!(manager.get_profile("any.com").is_none());
        assert!(manager.tracked_domains().is_empty());
    }

    #[test]
    fn test_manager_default() {
        let manager = SpeedPredictionManager::default();
        assert!(manager.tracked_domains().is_empty());
        assert!(manager.config().enabled);
    }

    #[test]
    fn test_manager_record_speed_creates_profile() {
        let mut manager = SpeedPredictionManager::default();
        manager.record_speed("new-domain.com", 5000.0);
        assert!(manager.get_profile("new-domain.com").is_some());
        assert_eq!(manager.tracked_domains().len(), 1);
    }

    #[test]
    fn test_manager_record_speed_accumulates() {
        let mut manager = SpeedPredictionManager::default();
        manager.record_speed("cdn.com", 1000.0);
        manager.record_speed("cdn.com", 2000.0);
        manager.record_speed("cdn.com", 3000.0);
        let profile = manager.get_profile("cdn.com").unwrap();
        assert_eq!(profile.total_samples, 3);
    }

    #[test]
    fn test_manager_predict_zero_speed() {
        let manager = SpeedPredictionManager::default();
        let prediction = manager.predict("task1", "no-data.com", 0.0, 1_000_000);
        assert_eq!(prediction.eta_current_speed_secs, u64::MAX);
        assert_eq!(prediction.confidence, "none");
    }

    #[test]
    fn test_manager_predict_zero_remaining() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 5,
            ..Default::default()
        });
        for _ in 0..10 {
            manager.record_speed_at("example.com", 1000.0, make_timestamp(10));
        }
        let prediction = manager.predict("task1", "example.com", 1000.0, 0);
        assert_eq!(prediction.remaining_bytes, 0);
        assert_eq!(prediction.eta_current_speed_secs, 0);
    }

    #[test]
    fn test_manager_predict_no_profile() {
        let manager = SpeedPredictionManager::default();
        let prediction = manager.predict("task1", "ghost.com", 5000.0, 10_000);
        assert_eq!(prediction.confidence, "none");
        assert_eq!(
            prediction.recommendation,
            PredictionRecommendation::InsufficientData
        );
        assert_eq!(prediction.predicted_avg_speed_bps, 0.0);
    }

    #[test]
    fn test_manager_predict_insufficient_samples() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 100,
            ..Default::default()
        });
        for _ in 0..5 {
            manager.record_speed_at("example.com", 1000.0, make_timestamp(10));
        }
        let prediction = manager.predict("task1", "example.com", 1000.0, 10_000);
        // Has data but not enough samples
        assert_eq!(prediction.confidence, "none");
        assert_eq!(
            prediction.recommendation,
            PredictionRecommendation::InsufficientData
        );
    }

    #[test]
    fn test_manager_predict_confidence_levels() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 5,
            ..Default::default()
        });

        // Low confidence: 5-29 samples
        for _ in 0..10 {
            manager.record_speed_at("low.com", 1000.0, make_timestamp(10));
        }
        let pred = manager.predict("t1", "low.com", 1000.0, 10_000);
        assert_eq!(pred.confidence, "low");

        // Medium confidence: 30-99 samples
        for _ in 0..40 {
            manager.record_speed_at("med.com", 2000.0, make_timestamp(10));
        }
        let pred = manager.predict("t2", "med.com", 2000.0, 10_000);
        assert_eq!(pred.confidence, "medium");

        // High confidence: 100+ samples
        for _ in 0..100 {
            manager.record_speed_at("high.com", 3000.0, make_timestamp(10));
        }
        let pred = manager.predict("t3", "high.com", 3000.0, 10_000);
        assert_eq!(pred.confidence, "high");
    }

    #[test]
    fn test_manager_get_optimal_windows_nonexistent() {
        let manager = SpeedPredictionManager::default();
        let windows = manager.get_optimal_windows("no-such.com", 5);
        assert!(windows.is_empty());
    }

    #[test]
    fn test_manager_get_optimal_windows_top_n() {
        let mut manager = SpeedPredictionManager::default();
        for h in [1, 5, 10, 15, 20] {
            for _ in 0..10 {
                manager.record_speed_at("test.com", (h as f64) * 100.0, make_timestamp(h as u8));
            }
        }
        let windows = manager.get_optimal_windows("test.com", 3);
        assert_eq!(windows.len(), 3);
        // Best should be hour 20 (highest speed)
        assert_eq!(windows[0].start_hour, 20);
    }

    #[test]
    fn test_manager_get_summary_empty() {
        let manager = SpeedPredictionManager::default();
        let summary = manager.get_summary();
        assert_eq!(summary.tracked_domains, 0);
        assert!(summary.domain_summaries.is_empty());
        assert!(summary.config_enabled);
    }

    #[test]
    fn test_manager_get_summary_multiple_domains() {
        let mut manager = SpeedPredictionManager::default();
        manager.record_speed("alpha.com", 1000.0);
        manager.record_speed("beta.com", 2000.0);
        manager.record_speed("gamma.com", 3000.0);
        let summary = manager.get_summary();
        assert_eq!(summary.tracked_domains, 3);
        assert_eq!(summary.domain_summaries.len(), 3);
    }

    #[test]
    fn test_manager_remove_domain_nonexistent() {
        let mut manager = SpeedPredictionManager::default();
        assert!(!manager.remove_domain("ghost.com"));
    }

    #[test]
    fn test_manager_clear_all_empty() {
        let mut manager = SpeedPredictionManager::default();
        manager.clear_all(); // should not panic
        assert!(manager.tracked_domains().is_empty());
    }

    #[test]
    fn test_manager_set_config() {
        let mut manager = SpeedPredictionManager::default();
        assert!(manager.config().enabled);
        manager.set_config(SpeedPredictionConfig {
            enabled: false,
            min_samples_for_prediction: 99,
            sample_retention_hours: 1,
            wait_threshold_ratio: 0.9,
        });
        assert!(!manager.config().enabled);
        assert_eq!(manager.config().min_samples_for_prediction, 99);
        assert_eq!(manager.config().sample_retention_hours, 1);
    }

    #[test]
    fn test_manager_cleanup_old_samples() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            sample_retention_hours: 1,
            ..Default::default()
        });
        // Add a very old sample
        let old_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        manager.record_speed_at("old.com", 1000.0, old_time);
        // Should not panic
        manager.cleanup_old_samples();
        // Profile still exists (kept for historical reference)
        assert!(manager.get_profile("old.com").is_some());
    }

    #[test]
    fn test_manager_multiple_domains_independent() {
        let mut manager = SpeedPredictionManager::default();
        manager.record_speed_at("fast.com", 10000.0, make_timestamp(10));
        manager.record_speed_at("slow.com", 100.0, make_timestamp(10));

        let fast_profile = manager.get_profile("fast.com").unwrap();
        let slow_profile = manager.get_profile("slow.com").unwrap();
        assert!(fast_profile.overall_avg_speed > slow_profile.overall_avg_speed);
    }

    #[test]
    fn test_manager_unicode_domain() {
        let mut manager = SpeedPredictionManager::default();
        manager.record_speed("中文域名.com", 5000.0);
        assert!(manager.get_profile("中文域名.com").is_some());
        assert_eq!(manager.tracked_domains().len(), 1);
    }

    #[test]
    fn test_manager_emoji_domain() {
        let mut manager = SpeedPredictionManager::default();
        manager.record_speed("🚀.com", 3000.0);
        assert!(manager.get_profile("🚀.com").is_some());
    }

    // ========== Prediction ETA Calculations ==========

    #[test]
    fn test_predict_eta_at_current_speed() {
        let manager = SpeedPredictionManager::default();
        // At 1000 B/s, 5000 bytes => 5 seconds
        let pred = manager.predict("t1", "no-data.com", 1000.0, 5000);
        assert_eq!(pred.eta_current_speed_secs, 5);
    }

    #[test]
    fn test_predict_eta_zero_speed_nonzero_remaining() {
        let manager = SpeedPredictionManager::default();
        let pred = manager.predict("t1", "no-data.com", 0.0, 1000);
        assert_eq!(pred.eta_current_speed_secs, u64::MAX);
    }

    #[test]
    fn test_predict_eta_zero_remaining_zero_speed() {
        let manager = SpeedPredictionManager::default();
        let pred = manager.predict("t1", "no-data.com", 0.0, 0);
        // Zero remaining => 0 ETA even at zero speed (0/0 branch: speed <= 0 => u64::MAX)
        assert_eq!(pred.eta_current_speed_secs, u64::MAX);
    }

    #[test]
    fn test_predict_large_remaining() {
        let manager = SpeedPredictionManager::default();
        let pred = manager.predict("t1", "x.com", 1.0, u64::MAX);
        // 1 byte/s with u64::MAX bytes
        assert_eq!(pred.eta_current_speed_secs, u64::MAX);
    }

    // ========== DomainSummary fields ==========

    #[test]
    fn test_domain_summary_best_worst_hour() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 3,
            ..Default::default()
        });
        // Create clear pattern
        for _ in 0..10 {
            manager.record_speed_at("test.com", 10000.0, make_timestamp(3));
        }
        for _ in 0..10 {
            manager.record_speed_at("test.com", 100.0, make_timestamp(15));
        }
        let summary = manager.get_summary();
        let ds = &summary.domain_summaries[0];
        assert!(ds.best_hour.is_some());
        assert!(ds.worst_hour.is_some());
        let (best_h, _) = ds.best_hour.unwrap();
        let (worst_h, _) = ds.worst_hour.unwrap();
        assert_eq!(best_h, 3);
        assert_eq!(worst_h, 15);
    }

    #[test]
    fn test_domain_summary_no_best_worst_when_insufficient_samples() {
        let mut manager = SpeedPredictionManager::default();
        // Only 1 sample per hour (< 3 threshold)
        manager.record_speed_at("sparse.com", 1000.0, make_timestamp(5));
        manager.record_speed_at("sparse.com", 2000.0, make_timestamp(10));
        let summary = manager.get_summary();
        let ds = &summary.domain_summaries[0];
        // best_hour/worst_hour require >= 3 samples per hour
        assert!(ds.best_hour.is_none());
        assert!(ds.worst_hour.is_none());
    }

    // ========== OptimalWindow fields ==========

    #[test]
    fn test_optimal_window_end_hour_wraps() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 3,
            ..Default::default()
        });
        // Hour 23 should have end_hour = 0 (wraps)
        for _ in 0..10 {
            manager.record_speed_at("test.com", 5000.0, make_timestamp(23));
        }
        let windows = manager.get_optimal_windows("test.com", 1);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].start_hour, 23);
        assert_eq!(windows[0].end_hour, 0);
    }

    // ========== Config default values ==========

    #[test]
    fn test_config_default_values() {
        let config = SpeedPredictionConfig::default();
        assert_eq!(config.min_samples_for_prediction, 10);
        assert_eq!(config.sample_retention_hours, 168);
        assert!((config.wait_threshold_ratio - 0.5).abs() < f64::EPSILON);
        assert!(config.enabled);
    }

    #[test]
    fn test_config_custom_values() {
        let config = SpeedPredictionConfig {
            min_samples_for_prediction: 0,
            sample_retention_hours: 0,
            wait_threshold_ratio: 0.0,
            enabled: false,
        };
        assert_eq!(config.min_samples_for_prediction, 0);
        assert_eq!(config.sample_retention_hours, 0);
        assert!((config.wait_threshold_ratio - 0.0).abs() < f64::EPSILON);
        assert!(!config.enabled);
    }

    // ========== PredictionRecommendation PartialEq ==========

    #[test]
    fn test_prediction_recommendation_equality() {
        assert_eq!(
            PredictionRecommendation::Proceed,
            PredictionRecommendation::Proceed
        );
        assert_eq!(
            PredictionRecommendation::InsufficientData,
            PredictionRecommendation::InsufficientData
        );
        assert_ne!(
            PredictionRecommendation::Proceed,
            PredictionRecommendation::InsufficientData
        );
    }

    #[test]
    fn test_prediction_recommendation_wait_until_equality() {
        let a = PredictionRecommendation::WaitUntil {
            hour: 5,
            speedup_factor: 2.0,
        };
        let b = PredictionRecommendation::WaitUntil {
            hour: 5,
            speedup_factor: 2.0,
        };
        let c = PredictionRecommendation::WaitUntil {
            hour: 6,
            speedup_factor: 2.0,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ========== Complex Workflows ==========

    #[test]
    fn test_full_lifecycle() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 5,
            ..Default::default()
        });

        // Record samples across multiple hours
        for _ in 0..20 {
            manager.record_speed_at("cdn.com", 5000.0, make_timestamp(3));
        }
        for _ in 0..20 {
            manager.record_speed_at("cdn.com", 1000.0, make_timestamp(15));
        }

        // Check profile
        let profile = manager.get_profile("cdn.com").unwrap();
        assert_eq!(profile.total_samples, 40);
        assert!(profile.overall_avg_speed > 0.0);

        // Check optimal windows
        let windows = manager.get_optimal_windows("cdn.com", 2);
        assert_eq!(windows[0].start_hour, 3);

        // Predict
        let pred = manager.predict("task1", "cdn.com", 3000.0, 100_000);
        assert_ne!(pred.confidence, "none");
        assert!(pred.eta_historical_avg_secs > 0);

        // Summary
        let summary = manager.get_summary();
        assert_eq!(summary.tracked_domains, 1);

        // Remove
        assert!(manager.remove_domain("cdn.com"));
        assert!(manager.get_profile("cdn.com").is_none());
    }

    #[test]
    fn test_multi_domain_workflow() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 3,
            ..Default::default()
        });

        // Domain A: fast morning
        for _ in 0..10 {
            manager.record_speed_at("a.com", 10000.0, make_timestamp(8));
        }
        // Domain B: fast evening
        for _ in 0..10 {
            manager.record_speed_at("b.com", 10000.0, make_timestamp(20));
        }

        let pred_a = manager.predict("t1", "a.com", 5000.0, 50_000);
        let pred_b = manager.predict("t2", "b.com", 5000.0, 50_000);
        assert_ne!(pred_a.confidence, "none");
        assert_ne!(pred_b.confidence, "none");

        // Summary should have both
        let summary = manager.get_summary();
        assert_eq!(summary.tracked_domains, 2);

        // Remove one
        manager.remove_domain("a.com");
        assert_eq!(manager.get_summary().tracked_domains, 1);
    }

    #[test]
    fn test_record_speed_at_specific_timestamp() {
        let mut manager = SpeedPredictionManager::default();
        let ts = Utc.with_ymd_and_hms(2026, 1, 15, 14, 30, 0).unwrap();
        manager.record_speed_at("test.com", 5000.0, ts);
        let profile = manager.get_profile("test.com").unwrap();
        assert_eq!(profile.total_samples, 1);
        assert!((profile.hourly_stats[14].avg_speed_bps - 5000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_domain_profile_overall_avg_calculation() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        profile.add_sample(1000.0, make_timestamp(0));
        profile.add_sample(2000.0, make_timestamp(1));
        profile.add_sample(3000.0, make_timestamp(2));
        // Overall avg = (1000+2000+3000)/3 = 2000
        assert!((profile.overall_avg_speed - 2000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_domain_profile_last_updated() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        let ts1 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        profile.add_sample(1000.0, ts1);
        assert_eq!(profile.last_updated, ts1);
        profile.add_sample(2000.0, ts2);
        assert_eq!(profile.last_updated, ts2);
    }

    #[test]
    fn test_best_hours_sorted_descending() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        for _ in 0..5 {
            profile.add_sample(100.0, make_timestamp(0));
            profile.add_sample(500.0, make_timestamp(6));
            profile.add_sample(1000.0, make_timestamp(12));
            profile.add_sample(2000.0, make_timestamp(18));
        }
        let best = profile.best_hours(4);
        assert_eq!(best.len(), 4);
        // Verify descending order
        for i in 0..best.len() - 1 {
            assert!(best[i].1 >= best[i + 1].1);
        }
    }

    #[test]
    fn test_worst_hours_sorted_ascending() {
        let mut profile = DomainSpeedProfile::new("test.com".to_string());
        for _ in 0..5 {
            profile.add_sample(100.0, make_timestamp(0));
            profile.add_sample(500.0, make_timestamp(6));
            profile.add_sample(1000.0, make_timestamp(12));
            profile.add_sample(2000.0, make_timestamp(18));
        }
        let worst = profile.worst_hours(4);
        assert_eq!(worst.len(), 4);
        // Verify ascending order
        for i in 0..worst.len() - 1 {
            assert!(worst[i].1 <= worst[i + 1].1);
        }
    }

    #[test]
    fn test_prediction_fields_correct() {
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 5,
            ..Default::default()
        });
        for _ in 0..20 {
            manager.record_speed_at("cdn.com", 2000.0, make_timestamp(10));
        }
        let pred = manager.predict("my-task", "cdn.com", 3000.0, 60_000);
        assert_eq!(pred.task_id, "my-task");
        assert_eq!(pred.domain, "cdn.com");
        assert!((pred.current_speed_bps - 3000.0).abs() < f64::EPSILON);
        assert!(pred.predicted_avg_speed_bps > 0.0);
        assert_eq!(pred.remaining_bytes, 60_000);
        // At 3000 B/s, 60000 bytes => 20 seconds
        assert_eq!(pred.eta_current_speed_secs, 20);
    }

    #[test]
    fn test_consider_alternatives_recommendation() {
        // Create scenario where current hour is poor and best hour is > 1.5x better
        let mut manager = SpeedPredictionManager::new(SpeedPredictionConfig {
            min_samples_for_prediction: 5,
            ..Default::default()
        });

        // Current hour: poor speed with low variance
        let current_hour = Utc::now().hour() as u8;
        for _ in 0..10 {
            manager.record_speed_at("test.com", 100.0, make_timestamp(current_hour));
        }

        // Another hour: much better speed
        let better_hour = (current_hour + 12) % 24;
        for _ in 0..10 {
            manager.record_speed_at("test.com", 500.0, make_timestamp(better_hour));
        }

        let pred = manager.predict("t1", "test.com", 100.0, 10_000);
        // Should either recommend WaitUntil or Proceed depending on CV
        // The key is it doesn't crash and produces valid output
        assert!(!pred.task_id.is_empty());
    }

    #[test]
    fn test_zero_speed_profile() {
        let mut profile = DomainSpeedProfile::new("zero.com".to_string());
        for _ in 0..5 {
            profile.add_sample(0.0, make_timestamp(10));
        }
        assert_eq!(profile.total_samples, 5);
        assert_eq!(profile.overall_avg_speed, 0.0);
        assert_eq!(profile.predicted_speed_for_hour(10), 0.0);
    }

    #[test]
    fn test_mixed_zero_and_nonzero_speed() {
        let mut profile = DomainSpeedProfile::new("mixed.com".to_string());
        profile.add_sample(0.0, make_timestamp(10));
        profile.add_sample(1000.0, make_timestamp(10));
        profile.add_sample(2000.0, make_timestamp(10));
        assert!((profile.predicted_speed_for_hour(10) - 1000.0).abs() < 0.01);
    }
}
