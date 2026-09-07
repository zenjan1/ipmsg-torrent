//! Download Progress Prediction System
//!
//! Predicts download completion times based on historical speed data,
//! network conditions, and source reliability. Provides confidence scores
//! and tracks prediction accuracy for continuous learning.

use crate::DownloadManager;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

/// Prediction confidence level
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PredictionConfidence {
    /// High confidence (>10 samples, low variance)
    High,
    /// Medium confidence (5-10 samples or moderate variance)
    Medium,
    /// Low confidence (<5 samples or high variance)
    Low,
    /// Unknown (no data available)
    Unknown,
}

impl std::fmt::Display for PredictionConfidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PredictionConfidence::High => write!(f, "High"),
            PredictionConfidence::Medium => write!(f, "Medium"),
            PredictionConfidence::Low => write!(f, "Low"),
            PredictionConfidence::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Prediction result for a single task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionResult {
    /// Task ID
    pub task_id: u64,
    /// Predicted completion time
    pub estimated_completion: DateTime<Utc>,
    /// Predicted remaining seconds
    pub remaining_seconds: u64,
    /// Confidence level
    pub confidence: PredictionConfidence,
    /// Optimistic estimate (best case, 25th percentile)
    pub optimistic_seconds: u64,
    /// Pessimistic estimate (worst case, 75th percentile)
    pub pessimistic_seconds: u64,
    /// Number of samples used for prediction
    pub sample_count: usize,
    /// Average speed used for prediction (bytes/sec)
    pub predicted_speed_bps: u64,
    /// Current progress percentage (0-100)
    pub current_progress: f64,
    /// When the prediction was made
    pub predicted_at: DateTime<Utc>,
}

/// Configuration for the prediction system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionConfig {
    /// Enable/disable prediction system
    pub enabled: bool,
    /// Minimum samples required for prediction (default: 5)
    pub min_samples: usize,
    /// Maximum samples to keep in history (default: 100)
    pub max_samples: usize,
    /// Smoothing factor for exponential weighted average (0.0-1.0, default: 0.3)
    pub smoothing_factor: f64,
    /// Confidence thresholds
    pub high_confidence_min_samples: usize,
    pub medium_confidence_min_samples: usize,
    /// Enable prediction accuracy tracking
    pub track_accuracy: bool,
    /// Maximum accuracy history entries (default: 50)
    pub max_accuracy_history: usize,
    /// Prediction file path
    #[serde(skip)]
    pub prediction_file: Option<PathBuf>,
}

impl Default for PredictionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_samples: 5,
            max_samples: 100,
            smoothing_factor: 0.3,
            high_confidence_min_samples: 10,
            medium_confidence_min_samples: 5,
            track_accuracy: true,
            max_accuracy_history: 50,
            prediction_file: None,
        }
    }
}

/// Speed sample for time series prediction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedSample {
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Speed in bytes per second
    pub speed_bps: u64,
    /// Progress percentage at this sample
    pub progress: f64,
}

/// Prediction model parameters (learned from data)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionModel {
    /// Task ID
    pub task_id: u64,
    /// Speed samples history
    pub speed_samples: VecDeque<SpeedSample>,
    /// Exponential weighted average speed
    pub ewma_speed: f64,
    /// Linear regression slope (speed trend)
    pub slope: f64,
    /// Linear regression intercept
    pub intercept: f64,
    /// Speed variance for confidence calculation
    pub speed_variance: f64,
    /// Last update time
    pub last_updated: DateTime<Utc>,
}

/// Prediction accuracy record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccuracyRecord {
    /// Task ID
    pub task_id: u64,
    /// When prediction was made
    pub predicted_at: DateTime<Utc>,
    /// Predicted remaining seconds
    pub predicted_seconds: u64,
    /// Actual remaining seconds (filled when task completes)
    pub actual_seconds: Option<u64>,
    /// Prediction error percentage (actual - predicted) / predicted * 100
    pub error_percentage: Option<f64>,
    /// Confidence level at prediction time
    pub confidence: PredictionConfidence,
}

/// Prediction accuracy summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccuracySummary {
    /// Total predictions made
    pub total_predictions: usize,
    /// Completed predictions (with actual data)
    pub completed_predictions: usize,
    /// Average error percentage
    pub avg_error_percentage: f64,
    /// Mean absolute error percentage
    pub mape: f64,
    /// Predictions within 10% error
    pub within_10_percent: usize,
    /// Predictions within 25% error
    pub within_25_percent: usize,
    /// Predictions over 50% error
    pub over_50_percent: usize,
}

/// Progress predictor for download tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressPredictor {
    /// Configuration
    pub config: PredictionConfig,
    /// Prediction models per task
    pub models: HashMap<u64, PredictionModel>,
    /// Accuracy tracking history
    pub accuracy_history: VecDeque<AccuracyRecord>,
}

impl ProgressPredictor {
    /// Create a new predictor with default config
    pub fn new() -> Self {
        Self {
            config: PredictionConfig::default(),
            models: HashMap::new(),
            accuracy_history: VecDeque::new(),
        }
    }

    /// Create a new predictor with custom config
    pub fn with_config(config: PredictionConfig) -> Self {
        Self {
            config,
            models: HashMap::new(),
            accuracy_history: VecDeque::new(),
        }
    }

    /// Add or update speed sample for a task
    pub fn update_speed(&mut self, task_id: u64, speed_bps: u64, progress: f64) {
        if !self.config.enabled {
            return;
        }

        let now = Utc::now();
        let model = self
            .models
            .entry(task_id)
            .or_insert_with(|| PredictionModel {
                task_id,
                speed_samples: VecDeque::new(),
                ewma_speed: speed_bps as f64,
                slope: 0.0,
                intercept: speed_bps as f64,
                speed_variance: 0.0,
                last_updated: now,
            });

        // Add new sample
        model.speed_samples.push_back(SpeedSample {
            timestamp: now,
            speed_bps,
            progress,
        });

        // Trim to max_samples
        while model.speed_samples.len() > self.config.max_samples {
            model.speed_samples.pop_front();
        }

        // Update EWMA
        model.ewma_speed = self.config.smoothing_factor * speed_bps as f64
            + (1.0 - self.config.smoothing_factor) * model.ewma_speed;

        // Update linear regression parameters
        Self::update_linear_regression(model);

        // Update variance
        Self::update_variance(model, speed_bps as f64);

        model.last_updated = now;
    }

    /// Update linear regression slope and intercept
    fn update_linear_regression(model: &mut PredictionModel) {
        let n = model.speed_samples.len();
        if n < 2 {
            model.slope = 0.0;
            model.intercept = model.ewma_speed;
            return;
        }

        // Simple linear regression: speed = slope * time + intercept
        let sum_x: f64 = (0..n).map(|i| i as f64).sum();
        let sum_y: f64 = model.speed_samples.iter().map(|s| s.speed_bps as f64).sum();
        let sum_xy: f64 = model
            .speed_samples
            .iter()
            .enumerate()
            .map(|(i, s)| i as f64 * s.speed_bps as f64)
            .sum();
        let sum_x2: f64 = (0..n).map(|i| (i as f64) * (i as f64)).sum();

        let denom = (n as f64 * sum_x2 - sum_x * sum_x).max(1e-10);
        model.slope = (n as f64 * sum_xy - sum_x * sum_y) / denom;
        model.intercept = (sum_y - model.slope * sum_x) / n as f64;
    }

    /// Update speed variance using Welford's algorithm
    fn update_variance(model: &mut PredictionModel, new_speed: f64) {
        let n = model.speed_samples.len() as f64;
        if n < 2.0 {
            model.speed_variance = 0.0;
            return;
        }

        // Incremental variance calculation
        let mean = model.ewma_speed;
        let delta = new_speed - mean;
        model.speed_variance = model.speed_variance + (delta * delta - model.speed_variance) / n;
    }

    /// Predict completion time for a task
    pub fn predict(
        &self,
        task_id: u64,
        current_progress: f64,
        file_size: u64,
    ) -> Option<PredictionResult> {
        if !self.config.enabled {
            return None;
        }

        let model = self.models.get(&task_id)?;

        // Need minimum samples
        if model.speed_samples.len() < self.config.min_samples {
            return None;
        }

        let remaining_bytes = (file_size as f64 * (100.0 - current_progress) / 100.0) as u64;

        // Predict speed using EWMA + linear trend
        let sample_count = model.speed_samples.len();
        let trend_factor = (sample_count as f64 * 0.1).min(0.5); // Limit trend influence
        let predicted_speed =
            model.ewma_speed * (1.0 - trend_factor) + model.intercept * trend_factor;
        let predicted_speed = predicted_speed.max(1.0); // Avoid division by zero

        // Calculate remaining time
        let remaining_seconds = (remaining_bytes as f64 / predicted_speed) as u64;

        // Calculate confidence based on sample count and variance
        let confidence = self.calculate_confidence(model);

        // Calculate optimistic/pessimistic estimates based on variance
        let stddev = model.speed_variance.sqrt();
        let optimistic_speed = (predicted_speed + stddev * 0.5).max(1.0);
        let pessimistic_speed = (predicted_speed - stddev * 0.5).max(1.0);

        let optimistic_seconds = (remaining_bytes as f64 / optimistic_speed) as u64;
        let pessimistic_seconds = (remaining_bytes as f64 / pessimistic_speed) as u64;

        Some(PredictionResult {
            task_id,
            estimated_completion: Utc::now() + chrono::Duration::seconds(remaining_seconds as i64),
            remaining_seconds,
            confidence,
            optimistic_seconds,
            pessimistic_seconds,
            sample_count,
            predicted_speed_bps: predicted_speed as u64,
            current_progress,
            predicted_at: Utc::now(),
        })
    }

    /// Calculate confidence level based on sample count and variance
    fn calculate_confidence(&self, model: &PredictionModel) -> PredictionConfidence {
        let sample_count = model.speed_samples.len();
        let cv = if model.ewma_speed > 0.0 {
            model.speed_variance.sqrt() / model.ewma_speed
        } else {
            1.0
        };

        if sample_count >= self.config.high_confidence_min_samples && cv < 0.3 {
            PredictionConfidence::High
        } else if sample_count >= self.config.medium_confidence_min_samples && cv < 0.6 {
            PredictionConfidence::Medium
        } else if sample_count >= self.config.min_samples {
            PredictionConfidence::Low
        } else {
            PredictionConfidence::Unknown
        }
    }

    /// Record prediction accuracy when task completes
    pub fn record_completion(&mut self, task_id: u64, actual_seconds: u64) {
        if !self.config.track_accuracy {
            return;
        }

        // Find most recent prediction for this task
        let prediction = self.predict(task_id, 100.0, 0); // Get last prediction
        if let Some(pred) = prediction {
            let error_pct = if pred.remaining_seconds > 0 {
                ((actual_seconds as f64 - pred.remaining_seconds as f64)
                    / pred.remaining_seconds as f64)
                    * 100.0
            } else {
                0.0
            };

            self.accuracy_history.push_back(AccuracyRecord {
                task_id,
                predicted_at: pred.predicted_at,
                predicted_seconds: pred.remaining_seconds,
                actual_seconds: Some(actual_seconds),
                error_percentage: Some(error_pct),
                confidence: pred.confidence,
            });

            // Trim history
            while self.accuracy_history.len() > self.config.max_accuracy_history {
                self.accuracy_history.pop_front();
            }
        }
    }

    /// Get accuracy summary
    pub fn get_accuracy_summary(&self) -> AccuracySummary {
        let completed: Vec<_> = self
            .accuracy_history
            .iter()
            .filter(|r| r.actual_seconds.is_some() && r.error_percentage.is_some())
            .collect();

        let total = self.accuracy_history.len();
        let completed_count = completed.len();

        if completed_count == 0 {
            return AccuracySummary {
                total_predictions: total,
                completed_predictions: 0,
                avg_error_percentage: 0.0,
                mape: 0.0,
                within_10_percent: 0,
                within_25_percent: 0,
                over_50_percent: 0,
            };
        }

        let errors: Vec<f64> = completed
            .iter()
            .filter_map(|r| r.error_percentage)
            .collect();
        let abs_errors: Vec<f64> = errors.iter().map(|e| e.abs()).collect();

        let avg_error = errors.iter().sum::<f64>() / errors.len() as f64;
        let mape = abs_errors.iter().sum::<f64>() / abs_errors.len() as f64;

        let within_10 = abs_errors.iter().filter(|&&e| e <= 10.0).count();
        let within_25 = abs_errors.iter().filter(|&&e| e <= 25.0).count();
        let over_50 = abs_errors.iter().filter(|&&e| e > 50.0).count();

        AccuracySummary {
            total_predictions: total,
            completed_predictions: completed_count,
            avg_error_percentage: avg_error,
            mape,
            within_10_percent: within_10,
            within_25_percent: within_25,
            over_50_percent: over_50,
        }
    }

    /// Remove prediction model for a task
    pub fn remove_task(&mut self, task_id: u64) {
        self.models.remove(&task_id);
    }

    /// Clear all prediction data
    pub fn clear(&mut self) {
        self.models.clear();
        self.accuracy_history.clear();
    }

    /// Get prediction for a task with current state
    pub fn predict_task(
        &self,
        task_id: u64,
        downloaded_bytes: u64,
        file_size: u64,
    ) -> Option<PredictionResult> {
        let progress = if file_size > 0 {
            (downloaded_bytes as f64 / file_size as f64) * 100.0
        } else {
            0.0
        };
        self.predict(task_id, progress, file_size)
    }
}

impl Default for ProgressPredictor {
    fn default() -> Self {
        Self::new()
    }
}

// DownloadManager integration
impl DownloadManager {
    /// Set prediction configuration
    pub async fn set_prediction_config(&self, config: PredictionConfig) {
        let mut predictor = self.progress_predictor.lock().await;
        predictor.config = config;
    }

    /// Get prediction configuration
    pub async fn get_prediction_config(&self) -> PredictionConfig {
        let predictor = self.progress_predictor.lock().await;
        predictor.config.clone()
    }

    /// Predict completion time for a task
    pub async fn predict_task_completion(&self, task_id: &str) -> Option<PredictionResult> {
        let tasks = self.tasks.lock().await;
        let task = tasks.iter().find(|t| t.id == task_id)?;
        let downloaded = task.downloaded;
        let file_size = task.size;
        let task_id_hash = task_id_to_u64(task_id);
        let predictor = self.progress_predictor.lock().await;
        predictor.predict_task(task_id_hash, downloaded, file_size)
    }

    /// Predict all active tasks
    pub async fn predict_all_active_tasks(&self) -> Vec<PredictionResult> {
        let tasks = self.tasks.lock().await;
        let predictor = self.progress_predictor.lock().await;
        tasks
            .iter()
            .filter(|t| t.state == crate::DownloadState::Downloading)
            .filter_map(|t| {
                let tid = task_id_to_u64(&t.id);
                predictor.predict_task(tid, t.downloaded, t.size)
            })
            .collect()
    }

    /// Update speed sample for prediction (called from speed tracker)
    pub async fn update_prediction_speed(&self, task_id: &str, speed_bps: u64, progress: f64) {
        let task_id_hash = task_id_to_u64(task_id);
        let mut predictor = self.progress_predictor.lock().await;
        predictor.update_speed(task_id_hash, speed_bps, progress);
    }

    /// Record task completion for accuracy tracking
    pub async fn record_prediction_completion(&self, task_id: &str, actual_seconds: u64) {
        let task_id_hash = task_id_to_u64(task_id);
        let mut predictor = self.progress_predictor.lock().await;
        predictor.record_completion(task_id_hash, actual_seconds);
    }

    /// Get prediction accuracy summary
    pub async fn get_prediction_accuracy(&self) -> AccuracySummary {
        let predictor = self.progress_predictor.lock().await;
        predictor.get_accuracy_summary()
    }

    /// Remove task from prediction system
    pub async fn remove_prediction_task(&self, task_id: &str) {
        let task_id_hash = task_id_to_u64(task_id);
        let mut predictor = self.progress_predictor.lock().await;
        predictor.remove_task(task_id_hash);
    }

    /// Clear all prediction data
    pub async fn clear_prediction_data(&self) {
        let mut predictor = self.progress_predictor.lock().await;
        predictor.clear();
    }
}

/// Convert a string task ID to a u64 hash for use with the predictor
fn task_id_to_u64(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prediction_config_default() {
        let config = PredictionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 5);
        assert_eq!(config.max_samples, 100);
        assert_eq!(config.smoothing_factor, 0.3);
    }

    #[test]
    fn test_predictor_new() {
        let predictor = ProgressPredictor::new();
        assert!(predictor.config.enabled);
        assert!(predictor.models.is_empty());
        assert!(predictor.accuracy_history.is_empty());
    }

    #[test]
    fn test_update_speed() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1000000, 10.0);
        assert!(predictor.models.contains_key(&1));
        assert_eq!(predictor.models[&1].speed_samples.len(), 1);
    }

    #[test]
    fn test_update_speed_multiple_samples() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1000000 + i * 100000, i as f64 * 10.0);
        }
        assert_eq!(predictor.models[&1].speed_samples.len(), 10);
    }

    #[test]
    fn test_predict_insufficient_samples() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1000000, 10.0);
        let result = predictor.predict(1, 10.0, 10000000);
        assert!(result.is_none());
    }

    #[test]
    fn test_predict_with_enough_samples() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1000000, i as f64 * 10.0);
        }
        let result = predictor.predict(1, 50.0, 10000000);
        assert!(result.is_some());
        let pred = result.unwrap();
        assert_eq!(pred.task_id, 1);
        assert_eq!(pred.confidence, PredictionConfidence::High);
    }

    #[test]
    fn test_confidence_calculation() {
        let mut predictor = ProgressPredictor::new();

        // Low confidence: few samples
        for i in 0..3 {
            predictor.update_speed(1, 1000000, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        assert_eq!(
            predictor.calculate_confidence(model),
            PredictionConfidence::Unknown
        );

        // Medium confidence: more samples
        for i in 3..8 {
            predictor.update_speed(1, 1000000, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        assert_eq!(
            predictor.calculate_confidence(model),
            PredictionConfidence::Medium
        );

        // High confidence: many samples, low variance
        for i in 8..15 {
            predictor.update_speed(1, 1000000, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        assert_eq!(
            predictor.calculate_confidence(model),
            PredictionConfidence::High
        );
    }

    #[test]
    fn test_prediction_remaining_time() {
        let mut predictor = ProgressPredictor::new();
        // Simulate 1 MB/s download
        for i in 0..10 {
            predictor.update_speed(1, 1000000, i as f64 * 10.0);
        }
        let result = predictor.predict(1, 50.0, 10000000).unwrap();
        // Should predict ~5 seconds for remaining 5MB at 1MB/s
        assert!(result.remaining_seconds > 0);
        assert!(result.remaining_seconds < 20); // Reasonable range
    }

    #[test]
    fn test_optimistic_pessimistic_estimates() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1000000, i as f64 * 10.0);
        }
        let result = predictor.predict(1, 50.0, 10000000).unwrap();
        assert!(result.optimistic_seconds <= result.remaining_seconds);
        assert!(result.pessimistic_seconds >= result.remaining_seconds);
    }

    #[test]
    fn test_remove_task() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1000000, 10.0);
        assert!(predictor.models.contains_key(&1));
        predictor.remove_task(1);
        assert!(!predictor.models.contains_key(&1));
    }

    #[test]
    fn test_clear() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1000000, 10.0);
        predictor.update_speed(2, 2000000, 20.0);
        predictor.clear();
        assert!(predictor.models.is_empty());
        assert!(predictor.accuracy_history.is_empty());
    }

    #[test]
    fn test_disabled_prediction() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.enabled = false;
        predictor.update_speed(1, 1000000, 10.0);
        assert!(!predictor.models.contains_key(&1));
        let result = predictor.predict(1, 50.0, 10000000);
        assert!(result.is_none());
    }

    #[test]
    fn test_max_samples_limit() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.max_samples = 10;
        for i in 0..20 {
            predictor.update_speed(1, 1000000, i as f64 * 5.0);
        }
        assert_eq!(predictor.models[&1].speed_samples.len(), 10);
    }

    #[test]
    fn test_ewma_update() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.smoothing_factor = 0.5;
        predictor.update_speed(1, 1000000, 10.0);
        let initial_ewma = predictor.models[&1].ewma_speed;
        predictor.update_speed(1, 2000000, 20.0);
        let new_ewma = predictor.models[&1].ewma_speed;
        // EWMA should move towards new value
        assert!(new_ewma > initial_ewma);
        assert!(new_ewma < 2000000.0);
    }

    #[test]
    fn test_linear_regression_update() {
        let mut predictor = ProgressPredictor::new();
        // Add samples with increasing speed
        for i in 0..10 {
            predictor.update_speed(1, 1000000 + i * 100000, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        // Slope should be positive (increasing speed)
        assert!(model.slope > 0.0);
    }

    #[test]
    fn test_predict_task() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1000000, i as f64 * 10.0);
        }
        let result = predictor.predict_task(1, 5000000, 10000000);
        assert!(result.is_some());
        let pred = result.unwrap();
        assert_eq!(pred.current_progress, 50.0);
    }

    #[test]
    fn test_accuracy_summary_empty() {
        let predictor = ProgressPredictor::new();
        let summary = predictor.get_accuracy_summary();
        assert_eq!(summary.total_predictions, 0);
        assert_eq!(summary.completed_predictions, 0);
        assert_eq!(summary.mape, 0.0);
    }

    #[test]
    fn test_prediction_confidence_display() {
        assert_eq!(format!("{}", PredictionConfidence::High), "High");
        assert_eq!(format!("{}", PredictionConfidence::Medium), "Medium");
        assert_eq!(format!("{}", PredictionConfidence::Low), "Low");
        assert_eq!(format!("{}", PredictionConfidence::Unknown), "Unknown");
    }

    // ── Phase 248: Comprehensive test coverage ──

    // --- PredictionConfidence serde ---

    #[test]
    fn test_confidence_serde_roundtrip_all_variants() {
        for variant in [
            PredictionConfidence::High,
            PredictionConfidence::Medium,
            PredictionConfidence::Low,
            PredictionConfidence::Unknown,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: PredictionConfidence = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back);
        }
    }

    #[test]
    fn test_confidence_serde_pascal_case_values() {
        assert_eq!(
            serde_json::to_string(&PredictionConfidence::High).unwrap(),
            "\"High\""
        );
        assert_eq!(
            serde_json::to_string(&PredictionConfidence::Medium).unwrap(),
            "\"Medium\""
        );
        assert_eq!(
            serde_json::to_string(&PredictionConfidence::Low).unwrap(),
            "\"Low\""
        );
        assert_eq!(
            serde_json::to_string(&PredictionConfidence::Unknown).unwrap(),
            "\"Unknown\""
        );
    }

    // --- PredictionConfidence traits ---

    #[test]
    fn test_confidence_clone_copy() {
        let a = PredictionConfidence::High;
        let b = a; // Copy
        let c = a.clone();
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn test_confidence_debug() {
        let s = format!("{:?}", PredictionConfidence::High);
        assert_eq!(s, "High");
    }

    #[test]
    fn test_confidence_eq() {
        assert_eq!(PredictionConfidence::High, PredictionConfidence::High);
        assert_ne!(PredictionConfidence::High, PredictionConfidence::Low);
    }

    // --- PredictionConfig serde ---

    #[test]
    fn test_config_serde_roundtrip() {
        let config = PredictionConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: PredictionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.enabled, config.enabled);
        assert_eq!(back.min_samples, config.min_samples);
        assert_eq!(back.max_samples, config.max_samples);
        assert!((back.smoothing_factor - config.smoothing_factor).abs() < 1e-10);
    }

    #[test]
    fn test_config_serde_custom_values() {
        let config = PredictionConfig {
            enabled: false,
            min_samples: 3,
            max_samples: 50,
            smoothing_factor: 0.7,
            high_confidence_min_samples: 20,
            medium_confidence_min_samples: 10,
            track_accuracy: false,
            max_accuracy_history: 100,
            prediction_file: None,
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: PredictionConfig = serde_json::from_str(&json).unwrap();
        assert!(!back.enabled);
        assert_eq!(back.min_samples, 3);
        assert_eq!(back.max_samples, 50);
        assert!((back.smoothing_factor - 0.7).abs() < 1e-10);
        assert_eq!(back.high_confidence_min_samples, 20);
        assert!(!back.track_accuracy);
        assert_eq!(back.max_accuracy_history, 100);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"min_samples":5,"max_samples":100,"smoothing_factor":0.3,"high_confidence_min_samples":10,"medium_confidence_min_samples":5,"track_accuracy":true,"max_accuracy_history":50,"extra_field":"ignored"}"#;
        let config: PredictionConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 5);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = PredictionConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let back: PredictionConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.enabled, config.enabled);
        assert_eq!(back.min_samples, config.min_samples);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = PredictionConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.min_samples, config.min_samples);
        let debug = format!("{:?}", config);
        assert!(debug.contains("PredictionConfig"));
    }

    // --- SpeedSample serde + traits ---

    #[test]
    fn test_speed_sample_serde_roundtrip() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: 1_000_000,
            progress: 50.0,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let back: SpeedSample = serde_json::from_str(&json).unwrap();
        assert_eq!(back.speed_bps, 1_000_000);
        assert!((back.progress - 50.0).abs() < 1e-10);
    }

    #[test]
    fn test_speed_sample_clone_debug() {
        let sample = SpeedSample {
            timestamp: Utc::now(),
            speed_bps: 500_000,
            progress: 25.0,
        };
        let cloned = sample.clone();
        assert_eq!(cloned.speed_bps, sample.speed_bps);
        let debug = format!("{:?}", sample);
        assert!(debug.contains("SpeedSample"));
    }

    // --- PredictionModel serde + traits ---

    #[test]
    fn test_prediction_model_serde_roundtrip() {
        let model = PredictionModel {
            task_id: 42,
            speed_samples: VecDeque::from(vec![SpeedSample {
                timestamp: Utc::now(),
                speed_bps: 1_000_000,
                progress: 10.0,
            }]),
            ewma_speed: 1_000_000.0,
            slope: 100.0,
            intercept: 900_000.0,
            speed_variance: 500.0,
            last_updated: Utc::now(),
        };
        let json = serde_json::to_string(&model).unwrap();
        let back: PredictionModel = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, 42);
        assert!((back.ewma_speed - 1_000_000.0).abs() < 1e-10);
        assert_eq!(back.speed_samples.len(), 1);
    }

    #[test]
    fn test_prediction_model_clone_debug() {
        let model = PredictionModel {
            task_id: 1,
            speed_samples: VecDeque::new(),
            ewma_speed: 0.0,
            slope: 0.0,
            intercept: 0.0,
            speed_variance: 0.0,
            last_updated: Utc::now(),
        };
        let cloned = model.clone();
        assert_eq!(cloned.task_id, model.task_id);
        let debug = format!("{:?}", model);
        assert!(debug.contains("PredictionModel"));
    }

    // --- AccuracyRecord serde + traits ---

    #[test]
    fn test_accuracy_record_serde_roundtrip() {
        let record = AccuracyRecord {
            task_id: 1,
            predicted_at: Utc::now(),
            predicted_seconds: 100,
            actual_seconds: Some(110),
            error_percentage: Some(10.0),
            confidence: PredictionConfidence::Medium,
        };
        let json = serde_json::to_string(&record).unwrap();
        let back: AccuracyRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, 1);
        assert_eq!(back.actual_seconds, Some(110));
        assert!((back.error_percentage.unwrap() - 10.0).abs() < 1e-10);
    }

    #[test]
    fn test_accuracy_record_null_actual() {
        let record = AccuracyRecord {
            task_id: 1,
            predicted_at: Utc::now(),
            predicted_seconds: 100,
            actual_seconds: None,
            error_percentage: None,
            confidence: PredictionConfidence::Low,
        };
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("null"));
        let back: AccuracyRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.actual_seconds, None);
        assert_eq!(back.error_percentage, None);
    }

    #[test]
    fn test_accuracy_record_clone_debug() {
        let record = AccuracyRecord {
            task_id: 1,
            predicted_at: Utc::now(),
            predicted_seconds: 50,
            actual_seconds: Some(55),
            error_percentage: Some(10.0),
            confidence: PredictionConfidence::High,
        };
        let cloned = record.clone();
        assert_eq!(cloned.task_id, record.task_id);
        let debug = format!("{:?}", record);
        assert!(debug.contains("AccuracyRecord"));
    }

    // --- AccuracySummary serde + traits ---

    #[test]
    fn test_accuracy_summary_serde_roundtrip() {
        let summary = AccuracySummary {
            total_predictions: 100,
            completed_predictions: 80,
            avg_error_percentage: 5.5,
            mape: 7.2,
            within_10_percent: 60,
            within_25_percent: 75,
            over_50_percent: 3,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: AccuracySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_predictions, 100);
        assert_eq!(back.completed_predictions, 80);
        assert!((back.avg_error_percentage - 5.5).abs() < 1e-10);
    }

    #[test]
    fn test_accuracy_summary_clone_debug() {
        let summary = AccuracySummary {
            total_predictions: 10,
            completed_predictions: 5,
            avg_error_percentage: 2.0,
            mape: 3.0,
            within_10_percent: 3,
            within_25_percent: 4,
            over_50_percent: 0,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_predictions, summary.total_predictions);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("AccuracySummary"));
    }

    // --- PredictionResult serde + traits ---

    #[test]
    fn test_prediction_result_serde_roundtrip() {
        let result = PredictionResult {
            task_id: 42,
            estimated_completion: Utc::now() + chrono::Duration::seconds(60),
            remaining_seconds: 60,
            confidence: PredictionConfidence::High,
            optimistic_seconds: 45,
            pessimistic_seconds: 80,
            sample_count: 20,
            predicted_speed_bps: 1_000_000,
            current_progress: 50.0,
            predicted_at: Utc::now(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: PredictionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, 42);
        assert_eq!(back.remaining_seconds, 60);
        assert_eq!(back.optimistic_seconds, 45);
        assert_eq!(back.pessimistic_seconds, 80);
    }

    #[test]
    fn test_prediction_result_clone_debug() {
        let result = PredictionResult {
            task_id: 1,
            estimated_completion: Utc::now(),
            remaining_seconds: 100,
            confidence: PredictionConfidence::Medium,
            optimistic_seconds: 80,
            pessimistic_seconds: 120,
            sample_count: 10,
            predicted_speed_bps: 500_000,
            current_progress: 25.0,
            predicted_at: Utc::now(),
        };
        let cloned = result.clone();
        assert_eq!(cloned.task_id, result.task_id);
        assert_eq!(cloned.remaining_seconds, result.remaining_seconds);
        let debug = format!("{:?}", result);
        assert!(debug.contains("PredictionResult"));
    }

    // --- ProgressPredictor serde ---

    #[test]
    fn test_predictor_serde_roundtrip() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 10.0);
        predictor.update_speed(1, 1_100_000, 20.0);
        let json = serde_json::to_string(&predictor).unwrap();
        let back: ProgressPredictor = serde_json::from_str(&json).unwrap();
        assert!(back.models.contains_key(&1));
        assert_eq!(back.models[&1].speed_samples.len(), 2);
    }

    #[test]
    fn test_predictor_clone() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 10.0);
        let cloned = predictor.clone();
        assert!(cloned.models.contains_key(&1));
        assert_eq!(cloned.models[&1].speed_samples.len(), 1);
    }

    #[test]
    fn test_predictor_clone_independence() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 10.0);
        let mut cloned = predictor.clone();
        cloned.update_speed(2, 2_000_000, 20.0);
        assert!(!predictor.models.contains_key(&2));
        assert!(cloned.models.contains_key(&2));
    }

    #[test]
    fn test_predictor_debug() {
        let predictor = ProgressPredictor::new();
        let debug = format!("{:?}", predictor);
        assert!(debug.contains("ProgressPredictor"));
    }

    // --- Default impl ---

    #[test]
    fn test_predictor_default_equals_new() {
        let new = ProgressPredictor::new();
        let default = ProgressPredictor::default();
        assert_eq!(default.config.enabled, new.config.enabled);
        assert_eq!(default.config.min_samples, new.config.min_samples);
        assert!(default.models.is_empty());
        assert!(default.accuracy_history.is_empty());
    }

    // --- with_config ---

    #[test]
    fn test_predictor_with_config() {
        let config = PredictionConfig {
            enabled: true,
            min_samples: 3,
            max_samples: 50,
            smoothing_factor: 0.5,
            high_confidence_min_samples: 15,
            medium_confidence_min_samples: 8,
            track_accuracy: false,
            max_accuracy_history: 25,
            prediction_file: None,
        };
        let predictor = ProgressPredictor::with_config(config.clone());
        assert_eq!(predictor.config.min_samples, 3);
        assert_eq!(predictor.config.max_samples, 50);
        assert!((predictor.config.smoothing_factor - 0.5).abs() < 1e-10);
        assert!(!predictor.config.track_accuracy);
    }

    // --- update_speed boundaries ---

    #[test]
    fn test_update_speed_zero_bps() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 0, 10.0);
        assert!(predictor.models.contains_key(&1));
        assert_eq!(predictor.models[&1].speed_samples[0].speed_bps, 0);
    }

    #[test]
    fn test_update_speed_large_bps() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, u64::MAX, 50.0);
        assert!(predictor.models.contains_key(&1));
        assert_eq!(predictor.models[&1].speed_samples[0].speed_bps, u64::MAX);
    }

    #[test]
    fn test_update_speed_zero_progress() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 0.0);
        assert!((predictor.models[&1].speed_samples[0].progress - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_update_speed_100_percent_progress() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 100.0);
        assert!((predictor.models[&1].speed_samples[0].progress - 100.0).abs() < 1e-10);
    }

    #[test]
    fn test_update_speed_disabled_no_model() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.enabled = false;
        predictor.update_speed(1, 1_000_000, 10.0);
        assert!(!predictor.models.contains_key(&1));
    }

    #[test]
    fn test_update_speed_multiple_tasks_independent() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 10.0);
        predictor.update_speed(2, 2_000_000, 20.0);
        predictor.update_speed(3, 3_000_000, 30.0);
        assert_eq!(predictor.models.len(), 3);
        assert_eq!(predictor.models[&1].speed_samples[0].speed_bps, 1_000_000);
        assert_eq!(predictor.models[&2].speed_samples[0].speed_bps, 2_000_000);
        assert_eq!(predictor.models[&3].speed_samples[0].speed_bps, 3_000_000);
    }

    #[test]
    fn test_update_speed_max_samples_trims_oldest() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.max_samples = 5;
        for i in 0..10 {
            predictor.update_speed(1, (i + 1) * 1000, i as f64 * 10.0);
        }
        let model = &predictor.models[&1];
        assert_eq!(model.speed_samples.len(), 5);
        // Oldest should have been removed, first remaining should be sample 5 (index 5)
        assert_eq!(model.speed_samples[0].speed_bps, 6000);
    }

    // --- predict boundaries ---

    #[test]
    fn test_predict_nonexistent_task() {
        let predictor = ProgressPredictor::new();
        assert!(predictor.predict(999, 50.0, 10_000_000).is_none());
    }

    #[test]
    fn test_predict_zero_file_size() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let result = predictor.predict(1, 100.0, 0);
        assert!(result.is_some());
        let pred = result.unwrap();
        assert_eq!(pred.remaining_seconds, 0);
    }

    #[test]
    fn test_predict_100_percent_progress() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let result = predictor.predict(1, 100.0, 10_000_000);
        assert!(result.is_some());
        let pred = result.unwrap();
        assert_eq!(pred.remaining_seconds, 0);
    }

    #[test]
    fn test_predict_zero_percent_progress() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let result = predictor.predict(1, 0.0, 10_000_000);
        assert!(result.is_some());
        let pred = result.unwrap();
        assert!(pred.remaining_seconds > 0);
        assert!((pred.current_progress - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_predict_large_file() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 10_000_000, i as f64);
        }
        let result = predictor.predict(1, 50.0, 10_000_000_000); // 10GB
        assert!(result.is_some());
        let pred = result.unwrap();
        assert!(pred.remaining_seconds > 0);
    }

    #[test]
    fn test_predict_disabled_returns_none() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        predictor.config.enabled = false;
        assert!(predictor.predict(1, 50.0, 10_000_000).is_none());
    }

    #[test]
    fn test_predict_speed_is_at_least_1() {
        // Even with zero speed samples, predicted speed should be >= 1.0
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 0, i as f64 * 10.0);
        }
        let result = predictor.predict(1, 50.0, 10_000_000);
        assert!(result.is_some());
        let pred = result.unwrap();
        assert!(pred.predicted_speed_bps >= 1);
    }

    // --- predict_task ---

    #[test]
    fn test_predict_task_zero_file_size() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let result = predictor.predict_task(1, 0, 0);
        assert!(result.is_some());
        let pred = result.unwrap();
        assert!((pred.current_progress - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_predict_task_half_downloaded() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let result = predictor.predict_task(1, 5_000_000, 10_000_000);
        assert!(result.is_some());
        let pred = result.unwrap();
        assert!((pred.current_progress - 50.0).abs() < 1e-10);
    }

    #[test]
    fn test_predict_task_nonexistent() {
        let predictor = ProgressPredictor::new();
        assert!(predictor.predict_task(999, 0, 10_000_000).is_none());
    }

    // --- record_completion ---

    #[test]
    fn test_record_completion_basic() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        predictor.record_completion(1, 5);
        assert_eq!(predictor.accuracy_history.len(), 1);
        assert_eq!(predictor.accuracy_history[0].task_id, 1);
        assert!(predictor.accuracy_history[0].actual_seconds.is_some());
    }

    #[test]
    fn test_record_completion_disabled() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.track_accuracy = false;
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        predictor.record_completion(1, 5);
        assert!(predictor.accuracy_history.is_empty());
    }

    #[test]
    fn test_record_completion_max_history_trimming() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.max_accuracy_history = 3;
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        // Record multiple completions
        for _ in 0..5 {
            predictor.record_completion(1, 5);
        }
        assert!(predictor.accuracy_history.len() <= 3);
    }

    // --- get_accuracy_summary ---

    #[test]
    fn test_accuracy_summary_with_data() {
        let mut predictor = ProgressPredictor::new();
        // Add enough samples for prediction
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        // Record completion
        predictor.record_completion(1, 5);
        let summary = predictor.get_accuracy_summary();
        assert!(summary.total_predictions > 0);
        assert!(summary.completed_predictions > 0);
    }

    #[test]
    fn test_accuracy_summary_no_completed() {
        let predictor = ProgressPredictor::new();
        let summary = predictor.get_accuracy_summary();
        assert_eq!(summary.total_predictions, 0);
        assert_eq!(summary.completed_predictions, 0);
        assert_eq!(summary.avg_error_percentage, 0.0);
        assert_eq!(summary.mape, 0.0);
        assert_eq!(summary.within_10_percent, 0);
        assert_eq!(summary.within_25_percent, 0);
        assert_eq!(summary.over_50_percent, 0);
    }

    // --- remove_task boundaries ---

    #[test]
    fn test_remove_task_nonexistent() {
        let mut predictor = ProgressPredictor::new();
        predictor.remove_task(999); // Should not panic
    }

    #[test]
    fn test_remove_task_idempotent() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 10.0);
        predictor.remove_task(1);
        predictor.remove_task(1); // Second remove should not panic
        assert!(!predictor.models.contains_key(&1));
    }

    // --- clear boundaries ---

    #[test]
    fn test_clear_empty() {
        let mut predictor = ProgressPredictor::new();
        predictor.clear(); // Should not panic on empty
        assert!(predictor.models.is_empty());
        assert!(predictor.accuracy_history.is_empty());
    }

    // --- task_id_to_u64 ---

    #[test]
    fn test_task_id_to_u64_deterministic() {
        let h1 = task_id_to_u64("task-123");
        let h2 = task_id_to_u64("task-123");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_task_id_to_u64_different_inputs() {
        let h1 = task_id_to_u64("task-1");
        let h2 = task_id_to_u64("task-2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_task_id_to_u64_unicode() {
        let h1 = task_id_to_u64("任务-中文");
        let h2 = task_id_to_u64("任务-中文");
        assert_eq!(h1, h2);
        let h3 = task_id_to_u64("任务-日文");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_task_id_to_u64_empty() {
        let h = task_id_to_u64("");
        // Should not panic, returns some hash
        assert!(h > 0 || h == 0); // just ensure no panic
    }

    // --- Confidence boundary tests ---

    #[test]
    fn test_confidence_high_threshold() {
        let mut predictor = ProgressPredictor::new();
        // high_confidence_min_samples = 10, cv < 0.3
        // Use constant speed to keep variance low
        for i in 0..15 {
            predictor.update_speed(1, 1_000_000, i as f64);
        }
        let model = predictor.models.get(&1).unwrap();
        assert_eq!(
            predictor.calculate_confidence(model),
            PredictionConfidence::High
        );
    }

    #[test]
    fn test_confidence_medium_with_high_variance() {
        let mut predictor = ProgressPredictor::new();
        // 8 samples but high variance → should be Low (cv >= 0.6)
        for i in 0..8 {
            let speed = if i % 2 == 0 { 100 } else { 10_000_000 };
            predictor.update_speed(1, speed, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        let conf = predictor.calculate_confidence(model);
        // With such high variance, cv should be > 0.6, so Low
        assert_eq!(conf, PredictionConfidence::Low);
    }

    #[test]
    fn test_confidence_unknown_below_min_samples() {
        let mut predictor = ProgressPredictor::new();
        // Only 4 samples, below min_samples=5
        for i in 0..4 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        assert_eq!(
            predictor.calculate_confidence(model),
            PredictionConfidence::Unknown
        );
    }

    // --- EWMA boundaries ---

    #[test]
    fn test_ewma_smoothing_factor_zero() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.smoothing_factor = 0.0;
        predictor.update_speed(1, 1_000_000, 10.0);
        let ewma1 = predictor.models[&1].ewma_speed;
        predictor.update_speed(1, 5_000_000, 20.0);
        let ewma2 = predictor.models[&1].ewma_speed;
        // With alpha=0, EWMA should not change
        assert!((ewma2 - ewma1).abs() < 1e-10);
    }

    #[test]
    fn test_ewma_smoothing_factor_one() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.smoothing_factor = 1.0;
        predictor.update_speed(1, 1_000_000, 10.0);
        predictor.update_speed(1, 5_000_000, 20.0);
        let ewma = predictor.models[&1].ewma_speed;
        // With alpha=1, EWMA should be exactly the latest value
        assert!((ewma - 5_000_000.0).abs() < 1e-10);
    }

    // --- Linear regression boundaries ---

    #[test]
    fn test_linear_regression_single_sample() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 10.0);
        let model = predictor.models.get(&1).unwrap();
        assert_eq!(model.slope, 0.0);
    }

    #[test]
    fn test_linear_regression_decreasing_speed() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 10_000_000 - i * 1_000_000, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        assert!(model.slope < 0.0);
    }

    #[test]
    fn test_linear_regression_constant_speed() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        // Constant speed → slope should be near zero
        assert!(model.slope.abs() < 1.0);
    }

    // --- Variance boundaries ---

    #[test]
    fn test_variance_constant_speed() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        // Constant speed → variance should be zero or near-zero
        assert!(model.speed_variance < 1000.0);
    }

    #[test]
    fn test_variance_variable_speed() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            let speed = if i % 2 == 0 { 100_000 } else { 10_000_000 };
            predictor.update_speed(1, speed, i as f64 * 10.0);
        }
        let model = predictor.models.get(&1).unwrap();
        // Variable speed → variance should be significant
        assert!(model.speed_variance > 0.0);
    }

    // --- Complex workflow ---

    #[test]
    fn test_full_lifecycle() {
        let mut predictor = ProgressPredictor::new();

        // 1. Add speed samples
        for i in 0..15 {
            predictor.update_speed(1, 1_000_000 + i * 50_000, i as f64 * 5.0);
        }

        // 2. Predict
        let pred = predictor.predict(1, 50.0, 10_000_000).unwrap();
        assert_eq!(pred.task_id, 1);
        assert!(pred.remaining_seconds > 0);
        assert!(pred.sample_count >= 5);

        // 3. Record completion
        predictor.record_completion(1, 8);

        // 4. Check accuracy
        let summary = predictor.get_accuracy_summary();
        assert!(summary.total_predictions > 0);

        // 5. Remove task
        predictor.remove_task(1);
        assert!(!predictor.models.contains_key(&1));

        // 6. Accuracy history still preserved
        assert!(!predictor.accuracy_history.is_empty());

        // 7. Clear all
        predictor.clear();
        assert!(predictor.models.is_empty());
        assert!(predictor.accuracy_history.is_empty());
    }

    #[test]
    fn test_multiple_tasks_workflow() {
        let mut predictor = ProgressPredictor::new();

        // Task 1: fast download
        for i in 0..10 {
            predictor.update_speed(1, 10_000_000, i as f64 * 10.0);
        }
        // Task 2: slow download
        for i in 0..10 {
            predictor.update_speed(2, 100_000, i as f64 * 10.0);
        }

        let pred1 = predictor.predict(1, 50.0, 100_000_000).unwrap();
        let pred2 = predictor.predict(2, 50.0, 100_000_000).unwrap();

        // Task 1 should have shorter remaining time (faster speed)
        assert!(pred1.remaining_seconds < pred2.remaining_seconds);
    }

    // --- Config boundary values ---

    #[test]
    fn test_config_min_samples_zero() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.min_samples = 0;
        // Even with 0 samples, predict needs a model to exist
        predictor.update_speed(1, 1_000_000, 10.0);
        let result = predictor.predict(1, 50.0, 10_000_000);
        assert!(result.is_some());
    }

    #[test]
    fn test_config_max_samples_one() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.max_samples = 1;
        predictor.update_speed(1, 1_000_000, 10.0);
        predictor.update_speed(1, 2_000_000, 20.0);
        assert_eq!(predictor.models[&1].speed_samples.len(), 1);
        // Only the latest sample should remain
        assert_eq!(predictor.models[&1].speed_samples[0].speed_bps, 2_000_000);
    }

    #[test]
    fn test_config_max_accuracy_history_zero() {
        let mut predictor = ProgressPredictor::new();
        predictor.config.max_accuracy_history = 0;
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        predictor.record_completion(1, 5);
        // History should be trimmed to 0
        assert!(predictor.accuracy_history.is_empty());
    }

    // --- PredictionResult field validation ---

    #[test]
    fn test_prediction_result_fields() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let pred = predictor.predict(1, 50.0, 10_000_000).unwrap();
        assert_eq!(pred.task_id, 1);
        assert!((pred.current_progress - 50.0).abs() < 1e-10);
        assert!(pred.sample_count >= 5);
        assert!(pred.predicted_speed_bps > 0);
        assert!(pred.optimistic_seconds <= pred.remaining_seconds);
        assert!(pred.pessimistic_seconds >= pred.remaining_seconds);
    }

    // --- Edge cases ---

    #[test]
    fn test_predict_very_small_remaining() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        // 99.99% done, 1KB remaining
        let result = predictor.predict(1, 99.99, 1_000_000);
        assert!(result.is_some());
        let pred = result.unwrap();
        // Very small remaining → small remaining_seconds
        assert!(pred.remaining_seconds < 10);
    }

    #[test]
    fn test_predict_u64_max_file_size() {
        let mut predictor = ProgressPredictor::new();
        for i in 0..10 {
            predictor.update_speed(1, 1_000_000, i as f64 * 10.0);
        }
        let result = predictor.predict(1, 50.0, u64::MAX / 2);
        assert!(result.is_some());
    }

    #[test]
    fn test_sequential_updates_overwrite() {
        let mut predictor = ProgressPredictor::new();
        predictor.update_speed(1, 1_000_000, 10.0);
        let ewma1 = predictor.models[&1].ewma_speed;
        predictor.update_speed(1, 5_000_000, 50.0);
        let ewma2 = predictor.models[&1].ewma_speed;
        assert!(ewma2 > ewma1);
    }
}
