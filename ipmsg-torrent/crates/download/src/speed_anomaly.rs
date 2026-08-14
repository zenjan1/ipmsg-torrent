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

    // ==================== AnomalyConfig serde ====================

    #[test]
    fn test_config_serde_roundtrip() {
        let config = AnomalyConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AnomalyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.min_samples, config.min_samples);
        assert_eq!(
            deserialized.threshold_multiplier,
            config.threshold_multiplier
        );
        assert_eq!(deserialized.min_speed_bps, config.min_speed_bps);
        assert_eq!(
            deserialized.max_anomalies_per_task,
            config.max_anomalies_per_task
        );
        assert_eq!(deserialized.cooldown_secs, config.cooldown_secs);
    }

    #[test]
    fn test_config_serde_custom_values() {
        let config = AnomalyConfig {
            enabled: false,
            min_samples: 5,
            threshold_multiplier: 3.0,
            min_speed_bps: 2048.0,
            max_anomalies_per_task: 100,
            cooldown_secs: 600,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AnomalyConfig = serde_json::from_str(&json).unwrap();
        assert!(!deserialized.enabled);
        assert_eq!(deserialized.min_samples, 5);
        assert_eq!(deserialized.threshold_multiplier, 3.0);
        assert_eq!(deserialized.min_speed_bps, 2048.0);
        assert_eq!(deserialized.max_anomalies_per_task, 100);
        assert_eq!(deserialized.cooldown_secs, 600);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"min_samples":10,"threshold_multiplier":2.0,"min_speed_bps":1024.0,"max_anomalies_per_task":50,"cooldown_secs":300,"extra_field":"ignored"}"#;
        let config: AnomalyConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 10);
    }

    #[test]
    fn test_config_serde_missing_field_fails() {
        // AnomalyConfig doesn't use #[serde(default)], so missing fields cause error
        let json = r#"{"enabled":true}"#;
        let result: Result<AnomalyConfig, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_pretty_serde() {
        let config = AnomalyConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: AnomalyConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
    }

    // ==================== AnomalyConfig Clone/Debug ====================

    #[test]
    fn test_config_clone() {
        let config = AnomalyConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.min_samples, config.min_samples);
        assert_eq!(cloned.threshold_multiplier, config.threshold_multiplier);
    }

    #[test]
    fn test_config_debug() {
        let config = AnomalyConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("AnomalyConfig"));
        assert!(debug_str.contains("enabled"));
        assert!(debug_str.contains("min_samples"));
    }

    // ==================== AnomalySeverity serde ====================

    #[test]
    fn test_severity_serde_roundtrip_all_variants() {
        for severity in [
            AnomalySeverity::Mild,
            AnomalySeverity::Moderate,
            AnomalySeverity::Severe,
        ] {
            let json = serde_json::to_string(&severity).unwrap();
            let deserialized: AnomalySeverity = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, severity);
        }
    }

    #[test]
    fn test_severity_serde_values() {
        assert_eq!(
            serde_json::to_string(&AnomalySeverity::Mild).unwrap(),
            r#""Mild""#
        );
        assert_eq!(
            serde_json::to_string(&AnomalySeverity::Moderate).unwrap(),
            r#""Moderate""#
        );
        assert_eq!(
            serde_json::to_string(&AnomalySeverity::Severe).unwrap(),
            r#""Severe""#
        );
    }

    #[test]
    fn test_severity_clone() {
        let s = AnomalySeverity::Moderate;
        let cloned = s;
        assert_eq!(cloned, AnomalySeverity::Moderate);
    }

    #[test]
    fn test_severity_debug() {
        let debug_str = format!("{:?}", AnomalySeverity::Severe);
        assert!(debug_str.contains("Severe"));
    }

    #[test]
    fn test_severity_eq() {
        assert_eq!(AnomalySeverity::Mild, AnomalySeverity::Mild);
        assert_ne!(AnomalySeverity::Mild, AnomalySeverity::Moderate);
        assert_ne!(AnomalySeverity::Moderate, AnomalySeverity::Severe);
    }

    // ==================== SpeedAnomaly serde ====================

    #[test]
    fn test_anomaly_serde_roundtrip() {
        let anomaly = SpeedAnomaly {
            task_id: "task-1".to_string(),
            detected_at: Utc::now(),
            current_speed_bps: 500.0,
            expected_speed_bps: 1000.0,
            stddev_bps: 100.0,
            severity: AnomalySeverity::Moderate,
        };
        let json = serde_json::to_string(&anomaly).unwrap();
        let deserialized: SpeedAnomaly = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, anomaly.task_id);
        assert_eq!(deserialized.current_speed_bps, anomaly.current_speed_bps);
        assert_eq!(deserialized.expected_speed_bps, anomaly.expected_speed_bps);
        assert_eq!(deserialized.stddev_bps, anomaly.stddev_bps);
        assert_eq!(deserialized.severity, anomaly.severity);
    }

    #[test]
    fn test_anomaly_serde_unicode_task_id() {
        let anomaly = SpeedAnomaly {
            task_id: "任务-中文-🚀".to_string(),
            detected_at: Utc::now(),
            current_speed_bps: 100.0,
            expected_speed_bps: 1000.0,
            stddev_bps: 50.0,
            severity: AnomalySeverity::Severe,
        };
        let json = serde_json::to_string(&anomaly).unwrap();
        let deserialized: SpeedAnomaly = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, anomaly.task_id);
    }

    #[test]
    fn test_anomaly_clone() {
        let anomaly = SpeedAnomaly {
            task_id: "t1".to_string(),
            detected_at: Utc::now(),
            current_speed_bps: 200.0,
            expected_speed_bps: 1000.0,
            stddev_bps: 50.0,
            severity: AnomalySeverity::Mild,
        };
        let cloned = anomaly.clone();
        assert_eq!(cloned.task_id, anomaly.task_id);
        assert_eq!(cloned.current_speed_bps, anomaly.current_speed_bps);
    }

    #[test]
    fn test_anomaly_debug() {
        let anomaly = SpeedAnomaly {
            task_id: "t1".to_string(),
            detected_at: Utc::now(),
            current_speed_bps: 200.0,
            expected_speed_bps: 1000.0,
            stddev_bps: 50.0,
            severity: AnomalySeverity::Mild,
        };
        let debug_str = format!("{:?}", anomaly);
        assert!(debug_str.contains("SpeedAnomaly"));
        assert!(debug_str.contains("task_id"));
    }

    // ==================== SpeedAnomalySummary serde ====================

    #[test]
    fn test_summary_serde_roundtrip() {
        let summary = SpeedAnomalySummary {
            enabled: true,
            total_anomalies: 10,
            severe_count: 2,
            moderate_count: 3,
            mild_count: 5,
            tracked_tasks: 8,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: SpeedAnomalySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, summary.enabled);
        assert_eq!(deserialized.total_anomalies, summary.total_anomalies);
        assert_eq!(deserialized.severe_count, summary.severe_count);
        assert_eq!(deserialized.moderate_count, summary.moderate_count);
        assert_eq!(deserialized.mild_count, summary.mild_count);
        assert_eq!(deserialized.tracked_tasks, summary.tracked_tasks);
    }

    #[test]
    fn test_summary_serde_zero_values() {
        let summary = SpeedAnomalySummary {
            enabled: false,
            total_anomalies: 0,
            severe_count: 0,
            moderate_count: 0,
            mild_count: 0,
            tracked_tasks: 0,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: SpeedAnomalySummary = serde_json::from_str(&json).unwrap();
        assert!(!deserialized.enabled);
        assert_eq!(deserialized.total_anomalies, 0);
    }

    #[test]
    fn test_summary_clone() {
        let summary = SpeedAnomalySummary {
            enabled: true,
            total_anomalies: 5,
            severe_count: 1,
            moderate_count: 2,
            mild_count: 2,
            tracked_tasks: 3,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_anomalies, summary.total_anomalies);
        assert_eq!(cloned.severe_count, summary.severe_count);
    }

    #[test]
    fn test_summary_debug() {
        let summary = SpeedAnomalySummary {
            enabled: true,
            total_anomalies: 1,
            severe_count: 0,
            moderate_count: 1,
            mild_count: 0,
            tracked_tasks: 2,
        };
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("SpeedAnomalySummary"));
    }

    // ==================== TaskSpeedProfile internals ====================

    #[test]
    fn test_profile_has_enough_samples() {
        let mut profile = TaskSpeedProfile::default();
        assert!(!profile.has_enough_samples(1));

        profile.add_sample(100.0);
        assert!(profile.has_enough_samples(1));
        assert!(!profile.has_enough_samples(2));

        profile.add_sample(200.0);
        assert!(profile.has_enough_samples(2));
    }

    #[test]
    fn test_profile_stddev_single_sample() {
        let mut profile = TaskSpeedProfile::default();
        profile.add_sample(1000.0);
        assert_eq!(profile.stddev(), 0.0);
    }

    #[test]
    fn test_profile_stddev_two_samples() {
        let mut profile = TaskSpeedProfile::default();
        profile.add_sample(100.0);
        profile.add_sample(200.0);
        // mean = 150, variance = ((100-150)^2 + (200-150)^2) / (2-1) = 5000
        // stddev = sqrt(5000) ≈ 70.71
        let stddev = profile.stddev();
        assert!((stddev - 70.71).abs() < 0.1);
    }

    #[test]
    fn test_profile_mean_running() {
        let mut profile = TaskSpeedProfile::default();
        profile.add_sample(100.0);
        assert_eq!(profile.mean, 100.0);
        profile.add_sample(200.0);
        assert_eq!(profile.mean, 150.0);
        profile.add_sample(300.0);
        assert_eq!(profile.mean, 200.0);
    }

    #[test]
    fn test_profile_detect_anomaly_not_enough_samples() {
        let profile = TaskSpeedProfile::default();
        let config = AnomalyConfig {
            min_samples: 10,
            ..Default::default()
        };
        assert!(profile.detect_anomaly(100.0, &config, Utc::now()).is_none());
    }

    #[test]
    fn test_profile_detect_anomaly_slow_task() {
        let mut profile = TaskSpeedProfile::default();
        for _ in 0..20 {
            profile.add_sample(100.0); // below min_speed_bps default 1024
        }
        let config = AnomalyConfig {
            min_samples: 10,
            min_speed_bps: 1024.0,
            ..Default::default()
        };
        // Mean is 100 which is below min_speed_bps, so no anomaly
        assert!(profile.detect_anomaly(10.0, &config, Utc::now()).is_none());
    }

    #[test]
    fn test_profile_detect_anomaly_within_threshold() {
        let mut profile = TaskSpeedProfile::default();
        for _ in 0..20 {
            profile.add_sample(1000.0);
        }
        let config = AnomalyConfig {
            min_samples: 10,
            threshold_multiplier: 2.0,
            min_speed_bps: 100.0,
            cooldown_secs: 0,
            ..Default::default()
        };
        // stddev is 0, threshold = 1000 - 2*0 = 1000, 999 < 1000 → anomaly
        // But with zero stddev, threshold = mean = 1000, so 999 < 1000 is anomaly
        let result = profile.detect_anomaly(999.0, &config, Utc::now());
        assert!(result.is_some());
    }

    #[test]
    fn test_profile_detect_anomaly_above_mean() {
        let mut profile = TaskSpeedProfile::default();
        for _ in 0..20 {
            profile.add_sample(1000.0);
        }
        let config = AnomalyConfig {
            min_samples: 10,
            threshold_multiplier: 2.0,
            min_speed_bps: 100.0,
            cooldown_secs: 0,
            ..Default::default()
        };
        // Speed above mean should never be anomalous
        assert!(
            profile
                .detect_anomaly(1500.0, &config, Utc::now())
                .is_none()
        );
    }

    #[test]
    fn test_profile_cooldown_blocks_detection() {
        let mut profile = TaskSpeedProfile::default();
        for _ in 0..20 {
            profile.add_sample(1000.0);
        }
        let config = AnomalyConfig {
            min_samples: 10,
            cooldown_secs: 300,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let now = Utc::now();
        profile.mark_detected(now);
        // Should be blocked by cooldown immediately after detection
        assert!(profile.detect_anomaly(100.0, &config, now).is_none());
    }

    #[test]
    fn test_profile_cooldown_expired_allows_detection() {
        let mut profile = TaskSpeedProfile::default();
        for _ in 0..20 {
            profile.add_sample(1000.0);
        }
        let config = AnomalyConfig {
            min_samples: 10,
            cooldown_secs: 60,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let past = Utc::now() - chrono::Duration::seconds(120);
        profile.mark_detected(past);
        // Cooldown expired, should allow detection
        let result = profile.detect_anomaly(100.0, &config, Utc::now());
        assert!(result.is_some());
    }

    #[test]
    fn test_profile_zero_speed_samples() {
        let mut profile = TaskSpeedProfile::default();
        for _ in 0..10 {
            profile.add_sample(0.0);
        }
        assert_eq!(profile.mean, 0.0);
        assert_eq!(profile.stddev(), 0.0);
    }

    #[test]
    fn test_profile_large_speed_values() {
        let mut profile = TaskSpeedProfile::default();
        profile.add_sample(1e12);
        profile.add_sample(1e12 + 1000.0);
        assert!(profile.mean > 1e12);
        assert!(profile.stddev() > 0.0);
    }

    #[test]
    fn test_profile_negative_speed() {
        let mut profile = TaskSpeedProfile::default();
        profile.add_sample(-100.0);
        profile.add_sample(-200.0);
        assert!(profile.mean < 0.0);
    }

    // ==================== SpeedAnomalyDetector ====================

    #[test]
    fn test_detector_default() {
        let detector = SpeedAnomalyDetector::default();
        assert!(detector.config().enabled);
        assert_eq!(detector.tracked_task_count(), 0);
        assert_eq!(detector.anomaly_count(), 0);
    }

    #[test]
    fn test_detector_new() {
        let config = AnomalyConfig {
            enabled: false,
            min_samples: 5,
            ..Default::default()
        };
        let detector = SpeedAnomalyDetector::new(config);
        assert!(!detector.config().enabled);
        assert_eq!(detector.config().min_samples, 5);
    }

    #[test]
    fn test_detector_record_speed_multiple_tasks() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        detector.record_speed("task1", 1000.0);
        detector.record_speed("task2", 2000.0);
        detector.record_speed("task3", 3000.0);
        assert_eq!(detector.tracked_task_count(), 3);
    }

    #[test]
    fn test_detector_record_speed_same_task_accumulates() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        detector.record_speed("task1", 1000.0);
        detector.record_speed("task1", 2000.0);
        detector.record_speed("task1", 3000.0);
        assert_eq!(detector.tracked_task_count(), 1);
    }

    #[test]
    fn test_detector_check_nonexistent_task() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        let result = detector.check_for_anomalies("nonexistent", 100.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_detector_check_disabled() {
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
    fn test_detector_severity_boundary_severe() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
        }
        // < 30% of mean (300) → Severe
        let anomaly = detector.check_for_anomalies("task1", 299.0).unwrap();
        assert_eq!(anomaly.severity, AnomalySeverity::Severe);
    }

    #[test]
    fn test_detector_severity_boundary_moderate() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
        }
        // 30%-60% of mean (300-600) → Moderate
        let anomaly = detector.check_for_anomalies("task1", 400.0).unwrap();
        assert_eq!(anomaly.severity, AnomalySeverity::Moderate);
    }

    #[test]
    fn test_detector_severity_boundary_mild() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
        }
        // 60%-100% of mean (600-1000) → Mild
        let anomaly = detector.check_for_anomalies("task1", 800.0).unwrap();
        assert_eq!(anomaly.severity, AnomalySeverity::Mild);
    }

    #[test]
    fn test_detector_clear_all_anomalies() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
            detector.record_speed("task2", 2000.0);
        }
        detector.check_for_anomalies("task1", 100.0);
        detector.check_for_anomalies("task2", 200.0);
        assert_eq!(detector.anomaly_count(), 2);
        detector.clear_all_anomalies();
        assert_eq!(detector.anomaly_count(), 0);
    }

    #[test]
    fn test_detector_remove_task_clears_anomalies() {
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
        detector.remove_task("task1");
        assert_eq!(detector.anomaly_count(), 0);
        assert_eq!(detector.tracked_task_count(), 0);
    }

    #[test]
    fn test_detector_remove_nonexistent_task() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        detector.record_speed("task1", 1000.0);
        detector.remove_task("nonexistent"); // should not panic
        assert_eq!(detector.tracked_task_count(), 1);
    }

    #[test]
    fn test_detector_set_config() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        assert!(detector.config().enabled);
        let new_config = AnomalyConfig {
            enabled: false,
            min_samples: 99,
            ..Default::default()
        };
        detector.set_config(new_config);
        assert!(!detector.config().enabled);
        assert_eq!(detector.config().min_samples, 99);
    }

    #[test]
    fn test_detector_config_reference() {
        let config = AnomalyConfig {
            enabled: false,
            min_samples: 7,
            ..Default::default()
        };
        let detector = SpeedAnomalyDetector::new(config);
        let config_ref = detector.config();
        assert!(!config_ref.enabled);
        assert_eq!(config_ref.min_samples, 7);
    }

    #[test]
    fn test_detector_get_all_anomalies() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
            detector.record_speed("task2", 2000.0);
        }
        detector.check_for_anomalies("task1", 100.0);
        detector.check_for_anomalies("task2", 200.0);
        let all = detector.get_all_anomalies();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_detector_get_anomalies_empty() {
        let detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        assert!(detector.get_anomalies("nonexistent").is_empty());
    }

    #[test]
    fn test_detector_clear_anomalies_nonexistent_task() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        detector.clear_anomalies("nonexistent"); // should not panic
        assert_eq!(detector.anomaly_count(), 0);
    }

    #[test]
    fn test_detector_unicode_task_ids() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        detector.record_speed("任务-中文", 1000.0);
        detector.record_speed("タスク-日本語", 2000.0);
        detector.record_speed("🚀-emoji", 3000.0);
        assert_eq!(detector.tracked_task_count(), 3);
        assert!(detector.get_anomalies("任务-中文").is_empty());
    }

    #[test]
    fn test_detector_empty_task_id() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        detector.record_speed("", 1000.0);
        assert_eq!(detector.tracked_task_count(), 1);
    }

    #[test]
    fn test_detector_zero_speed() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        detector.record_speed("task1", 0.0);
        detector.record_speed("task1", 0.0);
        assert_eq!(detector.tracked_task_count(), 1);
    }

    #[test]
    fn test_detector_negative_speed() {
        let mut detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        detector.record_speed("task1", -100.0);
        assert_eq!(detector.tracked_task_count(), 1);
    }

    #[test]
    fn test_detector_debug() {
        let detector = SpeedAnomalyDetector::new(AnomalyConfig::default());
        let debug_str = format!("{:?}", detector);
        assert!(debug_str.contains("SpeedAnomalyDetector"));
    }

    #[test]
    fn test_detector_independent_tasks() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        // task1: mean ~1000
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
        }
        // task2: mean ~5000
        for _ in 0..10 {
            detector.record_speed("task2", 5000.0);
        }
        // task1 at 100 should be anomalous (< 30% of 1000)
        let a1 = detector.check_for_anomalies("task1", 100.0);
        assert!(a1.is_some());
        // task2 at 5000 should NOT be anomalous (equal to mean)
        let a2 = detector.check_for_anomalies("task2", 5000.0);
        assert!(a2.is_none());
    }

    #[test]
    fn test_detector_multiple_anomalies_same_task() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
        }
        // Trigger multiple anomalies
        detector.check_for_anomalies("task1", 100.0);
        detector.check_for_anomalies("task1", 200.0);
        detector.check_for_anomalies("task1", 150.0);
        assert_eq!(detector.get_anomalies("task1").len(), 3);
    }

    #[test]
    fn test_detector_anomaly_stores_correct_fields() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
        }
        let anomaly = detector.check_for_anomalies("task1", 100.0).unwrap();
        assert_eq!(anomaly.task_id, "task1");
        assert_eq!(anomaly.current_speed_bps, 100.0);
        assert!(anomaly.expected_speed_bps > 900.0);
        assert!(anomaly.stddev_bps >= 0.0);
        assert_eq!(anomaly.severity, AnomalySeverity::Severe);
    }

    #[test]
    fn test_detector_trimming_on_overflow() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            max_anomalies_per_task: 2,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);
        for _ in 0..10 {
            detector.record_speed("task1", 1000.0);
        }
        // max_anomalies_per_task * 100 = 200, so trimming at >200
        // Generate >200 anomalies
        for _ in 0..210 {
            detector.check_for_anomalies("task1", 100.0);
        }
        // After trimming, should be <= 200
        assert!(detector.anomaly_count() <= 200);
    }

    // ==================== Complete workflow ====================

    #[test]
    fn test_complete_lifecycle() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        // Phase 1: Record normal speeds
        for _ in 0..20 {
            detector.record_speed("task1", 1000.0);
        }
        assert_eq!(detector.tracked_task_count(), 1);
        assert_eq!(detector.anomaly_count(), 0);

        // Phase 2: Speed drops → anomaly detected
        let anomaly = detector.check_for_anomalies("task1", 100.0);
        assert!(anomaly.is_some());
        assert_eq!(anomaly.unwrap().severity, AnomalySeverity::Severe);
        assert_eq!(detector.anomaly_count(), 1);

        // Phase 3: Speed above mean → no anomaly
        let no_anomaly = detector.check_for_anomalies("task1", 1050.0);
        assert!(no_anomaly.is_none());

        // Phase 4: Clear and verify
        detector.clear_anomalies("task1");
        assert_eq!(detector.anomaly_count(), 0);

        // Phase 5: Remove task
        detector.remove_task("task1");
        assert_eq!(detector.tracked_task_count(), 0);
    }

    #[test]
    fn test_multi_task_lifecycle() {
        let config = AnomalyConfig {
            min_samples: 5,
            cooldown_secs: 0,
            min_speed_bps: 100.0,
            threshold_multiplier: 2.0,
            ..Default::default()
        };
        let mut detector = SpeedAnomalyDetector::new(config);

        // Record for 3 tasks
        for _ in 0..10 {
            detector.record_speed("fast", 10000.0);
            detector.record_speed("medium", 1000.0);
            detector.record_speed("slow", 100.0);
        }
        assert_eq!(detector.tracked_task_count(), 3);

        // fast and medium drop to 10 → anomalous
        detector.check_for_anomalies("fast", 10.0);
        detector.check_for_anomalies("medium", 10.0);
        // slow has mean ~100, which equals min_speed_bps (100.0), so detection proceeds
        // and 10 < 100 threshold → also anomalous
        detector.check_for_anomalies("slow", 10.0);

        assert_eq!(detector.get_anomalies("fast").len(), 1);
        assert_eq!(detector.get_anomalies("medium").len(), 1);
        assert_eq!(detector.get_anomalies("slow").len(), 1);
        assert_eq!(detector.anomaly_count(), 3);
    }
}
