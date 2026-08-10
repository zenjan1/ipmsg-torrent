//! Download speed anomaly detection
//!
//! Learns normal speed patterns for each task and detects when speed drops
//! abnormally low. Triggers alerts when anomalies are detected.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for anomaly detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyConfig {
    /// Enable anomaly detection
    pub enabled: bool,
    /// Minimum samples before detection (need enough data to learn)
    pub min_samples: usize,
    /// Standard deviation multiplier for anomaly threshold
    /// Speed below (mean - threshold * stddev) is considered anomalous
    pub threshold_multiplier: f64,
    /// Minimum speed in bytes/sec to consider (below this is always anomalous)
    pub min_speed_bps: f64,
    /// Maximum anomalies to track per task
    pub max_anomalies_per_task: usize,
    /// Cooldown period in seconds before detecting another anomaly for same task
    pub cooldown_secs: u64,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_samples: 10,
            threshold_multiplier: 2.0,
            min_speed_bps: 1024.0, // 1 KB/s
            max_anomalies_per_task: 50,
            cooldown_secs: 300, // 5 minutes
        }
    }
}

/// A detected speed anomaly
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedAnomaly {
    /// Task ID
    pub task_id: String,
    /// When the anomaly was detected
    pub detected_at: DateTime<Utc>,
    /// Current speed when anomaly detected (bytes/sec)
    pub current_speed_bps: f64,
    /// Expected speed (mean of historical data)
    pub expected_speed_bps: f64,
    /// Standard deviation of historical speeds
    pub stddev_bps: f64,
    /// Severity level
    pub severity: AnomalySeverity,
}

/// Severity of a speed anomaly
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnomalySeverity {
    /// Slight drop, might be temporary
    Mild,
    /// Significant drop, likely a problem
    Moderate,
    /// Severe drop, action needed
    Severe,
}

impl AnomalySeverity {
    /// Human-readable label
    pub fn label(&self) -> &'static str {
        match self {
            Self::Mild => "mild",
            Self::Moderate => "moderate",
            Self::Severe => "severe",
        }
    }

    /// Emoji indicator
    pub fn emoji(&self) -> &'static str {
        match self {
            Self::Mild => "⚠️",
            Self::Moderate => "🔶",
            Self::Severe => "🔴",
        }
    }
}

/// Historical speed data for a single task
#[derive(Debug, Clone, Default)]
struct TaskSpeedProfile {
    /// Historical speed samples (bytes/sec)
    samples: Vec<f64>,
    /// Running mean
    mean: f64,
    /// Running sum of squared differences (for variance)
    sum_sq_diff: f64,
    /// Last anomaly detection time
    last_detection: Option<DateTime<Utc>>,
}

impl TaskSpeedProfile {
    /// Add a speed sample and update statistics
    fn add_sample(&mut self, speed_bps: f64) {
        self.samples.push(speed_bps);

        // Update running mean
        let n = self.samples.len() as f64;
        let old_mean = self.mean;
        self.mean = old_mean + (speed_bps - old_mean) / n;

        // Update sum of squared differences (Welford's algorithm)
        self.sum_sq_diff += (speed_bps - old_mean) * (speed_bps - self.mean);
    }

    /// Get standard deviation
    fn stddev(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        let variance = self.sum_sq_diff / (self.samples.len() - 1) as f64;
        variance.sqrt()
    }

    /// Check if we have enough samples
    fn has_enough_samples(&self, min_samples: usize) -> bool {
        self.samples.len() >= min_samples
    }

    /// Detect if current speed is anomalous
    fn detect_anomaly(
        &self,
        current_speed: f64,
        config: &AnomalyConfig,
        now: DateTime<Utc>,
    ) -> Option<(f64, f64)> {
        // Check cooldown
        if let Some(last) = self.last_detection {
            let elapsed = (now - last).num_seconds() as u64;
            if elapsed < config.cooldown_secs {
                return None;
            }
        }

        // Need enough samples
        if !self.has_enough_samples(config.min_samples) {
            return None;
        }

        // Skip detection for inherently slow tasks (mean speed below minimum threshold)
        if self.mean < config.min_speed_bps {
            return None;
        }

        let stddev = self.stddev();
        let threshold = self.mean - config.threshold_multiplier * stddev;

        // Check if current speed is significantly below expected
        if current_speed < threshold {
            Some((self.mean, stddev))
        } else {
            None
        }
    }

    /// Update last detection time
    fn mark_detected(&mut self, when: DateTime<Utc>) {
        self.last_detection = Some(when);
    }
}

/// Speed anomaly detector
#[derive(Debug, Default)]
pub struct SpeedAnomalyDetector {
    /// Configuration
    config: AnomalyConfig,
    /// Per-task speed profiles
    profiles: HashMap<String, TaskSpeedProfile>,
    /// Detected anomalies
    anomalies: Vec<SpeedAnomaly>,
}

/// Summary of speed anomaly detection status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedAnomalySummary {
    /// Whether anomaly detection is enabled
    pub enabled: bool,
    /// Total number of detected anomalies
    pub total_anomalies: usize,
    /// Number of severe anomalies
    pub severe_count: usize,
    /// Number of moderate anomalies
    pub moderate_count: usize,
    /// Number of mild anomalies
    pub mild_count: usize,
    /// Number of tasks being tracked
    pub tracked_tasks: usize,
}

impl SpeedAnomalyDetector {
    /// Create new detector with config
    pub fn new(config: AnomalyConfig) -> Self {
        Self {
            config,
            profiles: HashMap::new(),
            anomalies: Vec::new(),
        }
    }

    /// Record a speed sample for a task
    pub fn record_speed(&mut self, task_id: &str, speed_bps: f64) {
        let profile = self.profiles.entry(task_id.to_string()).or_default();
        profile.add_sample(speed_bps);
    }

    /// Check for anomalies and return any detected
    pub fn check_for_anomalies(
        &mut self,
        task_id: &str,
        current_speed: f64,
    ) -> Option<SpeedAnomaly> {
        if !self.config.enabled {
            return None;
        }

        let now = Utc::now();
        let profile = self.profiles.get(task_id)?;

        // Detect anomaly
        if let Some((mean, stddev)) = profile.detect_anomaly(current_speed, &self.config, now) {
            // Calculate severity
            let severity = if current_speed < mean * 0.3 {
                AnomalySeverity::Severe
            } else if current_speed < mean * 0.6 {
                AnomalySeverity::Moderate
            } else {
                AnomalySeverity::Mild
            };

            let anomaly = SpeedAnomaly {
                task_id: task_id.to_string(),
                detected_at: now,
                current_speed_bps: current_speed,
                expected_speed_bps: mean,
                stddev_bps: stddev,
                severity,
            };

            // Store anomaly
            self.anomalies.push(anomaly.clone());

            // Trim old anomalies if needed
            if self.anomalies.len() > self.config.max_anomalies_per_task * 100 {
                let keep = self.config.max_anomalies_per_task * 50;
                self.anomalies.drain(..self.anomalies.len() - keep);
            }

            // Update detection time
            if let Some(profile) = self.profiles.get_mut(task_id) {
                profile.mark_detected(now);
            }

            Some(anomaly)
        } else {
            None
        }
    }

    /// Get all anomalies for a task
    pub fn get_anomalies(&self, task_id: &str) -> Vec<&SpeedAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| a.task_id == task_id)
            .collect()
    }

    /// Get all anomalies
    pub fn get_all_anomalies(&self) -> &[SpeedAnomaly] {
        &self.anomalies
    }

    /// Clear anomalies for a task
    pub fn clear_anomalies(&mut self, task_id: &str) {
        self.anomalies.retain(|a| a.task_id != task_id);
    }

    /// Clear all anomalies
    pub fn clear_all_anomalies(&mut self) {
        self.anomalies.clear();
    }

    /// Remove task profile
    pub fn remove_task(&mut self, task_id: &str) {
        self.profiles.remove(task_id);
        self.clear_anomalies(task_id);
    }

    /// Get configuration
    pub fn config(&self) -> &AnomalyConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: AnomalyConfig) {
        self.config = config;
    }

    /// Get number of tracked tasks
    pub fn tracked_task_count(&self) -> usize {
        self.profiles.len()
    }

    /// Get total anomaly count
    pub fn anomaly_count(&self) -> usize {
        self.anomalies.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AnomalyConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 10);
        assert_eq!(config.threshold_multiplier, 2.0);
    }

    #[test]
    fn test_task_speed_profile_add_sample() {
        let mut profile = TaskSpeedProfile::default();

        profile.add_sample(1000.0);
        assert_eq!(profile.samples.len(), 1);
        assert_eq!(profile.mean, 1000.0);

        profile.add_sample(2000.0);
        assert_eq!(profile.samples.len(), 2);
        assert_eq!(profile.mean, 1500.0);
    }

    #[test]
    fn test_task_speed_profile_stddev() {
        let mut profile = TaskSpeedProfile::default();

        // Add samples with known variance
        for _ in 0..10 {
            profile.add_sample(1000.0);
        }
        for _ in 0..10 {
            profile.add_sample(2000.0);
        }

        let stddev = profile.stddev();
        // Mean is 1500, samples are 1000 or 2000
        // Variance = sum((x-mean)^2) / (n-1)
        // = (10 * 500^2 + 10 * 500^2) / 19
        // = 5000000 / 19 ≈ 263157
        // stddev ≈ 513
        assert!(stddev > 500.0 && stddev < 520.0);
    }

    #[test]
    fn test_detector_record_speed() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());

        detector.record_speed("task1", 1000.0);
        detector.record_speed("task1", 1500.0);

        assert_eq!(detector.tracked_task_count(), 1);
    }

    #[test]
    fn test_detector_not_enough_samples() {
        let config = AnomalyConfig {
            min_samples: 10,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        // Add only 5 samples
        for _ in 0..5 {
            detector.record_speed("task1", 1000.0);
        }

        // Should not detect anomaly (not enough data)
        let result = detector.check_for_anomalies("task1", 100.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_detector_detects_anomaly() {
        let config = AnomalyConfig {
            min_samples: 10,
            threshold_multiplier: 2.0,
            min_speed_bps: 100.0,
            cooldown_secs: 0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        // Add normal speed samples
        for _ in 0..20 {
            detector.record_speed("task1", 1000.0);
        }

        // Now speed drops dramatically
        let anomaly = detector.check_for_anomalies("task1", 100.0);

        assert!(anomaly.is_some());
        let a = anomaly.unwrap();
        assert_eq!(a.task_id, "task1");
        assert_eq!(a.current_speed_bps, 100.0);
        assert!(a.expected_speed_bps > 900.0);
        assert_eq!(a.severity, AnomalySeverity::Severe);
    }

    #[test]
    fn test_detector_no_anomaly_when_normal() {
        let config = AnomalyConfig {
            min_samples: 10,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        // Add samples
        for _ in 0..20 {
            detector.record_speed("task1", 1000.0);
        }

        // Speed is still normal
        let result = detector.check_for_anomalies("task1", 950.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_detector_cooldown() {
        let config = AnomalyConfig {
            min_samples: 10,
            cooldown_secs: 300,
            min_speed_bps: 100.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        // Add samples
        for _ in 0..20 {
            detector.record_speed("task1", 1000.0);
        }

        // First anomaly detected
        let first = detector.check_for_anomalies("task1", 100.0);
        assert!(first.is_some());

        // Second anomaly blocked by cooldown
        let second = detector.check_for_anomalies("task1", 100.0);
        assert!(second.is_none());
    }

    #[test]
    fn test_detector_disabled() {
        let config = AnomalyConfig {
            enabled: false,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        for _ in 0..20 {
            detector.record_speed("task1", 1000.0);
        }

        let result = detector.check_for_anomalies("task1", 100.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_severity_levels() {
        let config = AnomalyConfig {
            min_samples: 10,
            threshold_multiplier: 2.0,
            min_speed_bps: 100.0,
            cooldown_secs: 0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        // Add samples at 1000 bytes/sec
        for _ in 0..20 {
            detector.record_speed("task1", 1000.0);
        }

        // Severe: < 30% of mean (300)
        let severe = detector.check_for_anomalies("task1", 200.0);
        assert_eq!(severe.unwrap().severity, AnomalySeverity::Severe);

        // Wait for cooldown
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Moderate: 30-60% of mean (300-600)
        let moderate = detector.check_for_anomalies("task1", 500.0);
        assert_eq!(moderate.unwrap().severity, AnomalySeverity::Moderate);

        // Wait for cooldown
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Mild: 60-100% of mean (600-1000)
        let mild = detector.check_for_anomalies("task1", 700.0);
        assert_eq!(mild.unwrap().severity, AnomalySeverity::Mild);
    }

    #[test]
    fn test_get_anomalies() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        // Add samples for two tasks
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
            detector.record_speed("task2", 2000.0);
        }

        // Trigger anomalies
        detector.check_for_anomalies("task1", 100.0);
        detector.check_for_anomalies("task2", 200.0);

        assert_eq!(detector.get_anomalies("task1").len(), 1);
        assert_eq!(detector.get_anomalies("task2").len(), 1);
        assert_eq!(detector.get_all_anomalies().len(), 2);
    }

    #[test]
    fn test_clear_anomalies() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
        }

        detector.check_for_anomalies("task1", 100.0);
        assert_eq!(detector.anomaly_count(), 1);

        detector.clear_anomalies("task1");
        assert_eq!(detector.anomaly_count(), 0);
    }

    #[test]
    fn test_remove_task() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());

        detector.record_speed("task1", 1000.0);
        detector.record_speed("task2", 2000.0);

        assert_eq!(detector.tracked_task_count(), 2);

        detector.remove_task("task1");

        assert_eq!(detector.tracked_task_count(), 1);
    }

    #[test]
    fn test_severity_labels() {
        assert_eq!(AnomalySeverity::Mild.label(), "mild");
        assert_eq!(AnomalySeverity::Moderate.label(), "moderate");
        assert_eq!(AnomalySeverity::Severe.label(), "severe");

        assert_eq!(AnomalySeverity::Mild.emoji(), "⚠️");
        assert_eq!(AnomalySeverity::Moderate.emoji(), "🔶");
        assert_eq!(AnomalySeverity::Severe.emoji(), "🔴");
    }
}
