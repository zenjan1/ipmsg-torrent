//! Download ETA estimation with EMA-based speed prediction
//!
//! Uses Exponential Moving Average (EMA) to smooth noisy speed samples,
//! then estimates remaining time with confidence intervals.
//!
//! # Algorithm
//!
//! 1. Each task maintains an EMA of observed speeds: `ema = α * sample + (1 - α) * prev`
//! 2. Variance is tracked similarly: `var = α * (sample - ema)² + (1 - α) * prev_var`
//! 3. ETA = remaining_bytes / ema_speed
//! 4. Confidence intervals derived from standard deviation (√var)
//! 5. Stability metric: coefficient of variation = stddev / mean

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Default EMA smoothing factor (α).
/// Higher = more responsive to recent changes; lower = smoother.
const DEFAULT_ALPHA: f64 = 0.3;

/// Minimum samples before we report an ETA (avoid wild guesses)
const MIN_SAMPLES_FOR_ETA: u32 = 3;

/// Speed below which we consider the task stalled (bytes/sec)
const STALL_THRESHOLD_BPS: f64 = 1.0;

/// Per-task speed tracking state
#[derive(Debug, Clone)]
struct TaskSpeedState {
    /// Exponential moving average of speed (bytes/sec)
    ema: f64,
    /// Exponential moving variance of speed
    emvar: f64,
    /// Number of samples observed
    sample_count: u32,
    /// Last observed raw speed
    last_raw_bps: f64,
}

impl TaskSpeedState {
    fn new() -> Self {
        Self {
            ema: 0.0,
            emvar: 0.0,
            sample_count: 0,
            last_raw_bps: 0.0,
        }
    }

    /// Update with a new speed sample using Welford-style EMA variance
    fn update(&mut self, speed_bps: f64, alpha: f64) {
        self.sample_count += 1;
        self.last_raw_bps = speed_bps;

        if self.sample_count == 1 {
            // First sample: seed the EMA
            self.ema = speed_bps;
            self.emvar = 0.0;
        } else {
            let diff = speed_bps - self.ema;
            self.ema += alpha * diff;
            // EMA variance: exponential moving of squared deviation
            self.emvar = (1.0 - alpha) * (self.emvar + alpha * diff * diff);
        }
    }

    fn stddev(&self) -> f64 {
        self.emvar.sqrt()
    }

    /// Coefficient of variation (normalized volatility)
    fn coefficient_of_variation(&self) -> f64 {
        if self.ema <= 0.0 {
            return f64::MAX;
        }
        self.stddev() / self.ema
    }
}

/// Confidence level for an ETA estimate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EtaConfidence {
    /// Very few samples or highly volatile speed — ETA is a rough guess
    Low,
    /// Moderate samples with reasonable speed stability
    Medium,
    /// Many samples with stable speed — ETA should be fairly accurate
    High,
}

impl EtaConfidence {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Result of an ETA estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtaEstimate {
    /// Estimated seconds remaining (point estimate)
    pub estimated_secs: f64,
    /// Lower bound (optimistic, ~1σ fast)
    pub optimistic_secs: f64,
    /// Upper bound (pessimistic, ~1σ slow)
    pub pessimistic_secs: f64,
    /// Confidence level
    pub confidence: EtaConfidence,
    /// Current EMA-smoothed speed (bytes/sec)
    pub smoothed_speed_bps: f64,
    /// Last raw observed speed (bytes/sec)
    pub raw_speed_bps: f64,
    /// Number of speed samples collected for this task
    pub sample_count: u32,
    /// Speed stability: coefficient of variation (lower = more stable)
    pub speed_stability: f64,
}

impl EtaEstimate {
    /// Format the ETA as a human-readable string
    pub fn format_eta(&self) -> String {
        if self.estimated_secs.is_infinite() || self.estimated_secs.is_nan() {
            return "unknown".to_string();
        }
        let secs = self.estimated_secs as u64;
        if secs < 60 {
            format!("{}s", secs)
        } else if secs < 3600 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            let hours = secs / 3600;
            let mins = (secs % 3600) / 60;
            format!("{}h {}m", hours, mins)
        }
    }

    /// Format as a range string: "5m 30s (3m–8m)"
    pub fn format_range(&self) -> String {
        let eta = self.format_eta();
        let opt = format_secs(self.optimistic_secs);
        let pess = format_secs(self.pessimistic_secs);
        format!("{} ({}–{})", eta, opt, pess)
    }
}

fn format_secs(s: f64) -> String {
    if s.is_infinite() || s.is_nan() {
        return "?".to_string();
    }
    let secs = s as u64;
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    }
}

/// Global ETA estimator that tracks speed history for all active downloads
#[derive(Debug)]
pub struct EtaEstimator {
    /// Per-task speed tracking
    tasks: Arc<RwLock<HashMap<String, TaskSpeedState>>>,
    /// EMA smoothing factor
    alpha: f64,
}

impl Default for EtaEstimator {
    fn default() -> Self {
        Self::new()
    }
}

impl EtaEstimator {
    /// Create a new ETA estimator with default alpha
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            alpha: DEFAULT_ALPHA,
        }
    }

    /// Create with a custom alpha (smoothing factor, 0 < alpha <= 1)
    pub fn with_alpha(alpha: f64) -> Self {
        let alpha = alpha.clamp(0.01, 1.0);
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            alpha,
        }
    }

    /// Update the speed for a task. Call this whenever speed_bps is refreshed.
    pub async fn update_speed(&self, task_id: &str, speed_bps: f64) {
        let mut tasks = self.tasks.write().await;
        let state = tasks
            .entry(task_id.to_string())
            .or_insert_with(TaskSpeedState::new);
        state.update(speed_bps.max(0.0), self.alpha);
    }

    /// Estimate ETA for a task given its remaining bytes
    pub async fn estimate(&self, task_id: &str, remaining_bytes: u64) -> Option<EtaEstimate> {
        let tasks = self.tasks.read().await;
        let state = tasks.get(task_id)?;

        if state.sample_count < MIN_SAMPLES_FOR_ETA {
            return None;
        }

        if state.ema <= STALL_THRESHOLD_BPS {
            // Speed too low to estimate
            return None;
        }

        let remaining = remaining_bytes as f64;
        let estimated_secs = remaining / state.ema;

        // Confidence intervals based on speed variance
        let stddev = state.stddev();
        let cv = state.coefficient_of_variation();

        // Optimistic: speed is 1σ above mean
        let optimistic_speed = (state.ema + stddev).max(STALL_THRESHOLD_BPS);
        let optimistic_secs = remaining / optimistic_speed;

        // Pessimistic: speed is 1σ below mean (but at least stall threshold)
        let pessimistic_speed = (state.ema - stddev).max(STALL_THRESHOLD_BPS);
        let pessimistic_secs = remaining / pessimistic_speed;

        // Cap pessimistic at 10x estimate to avoid absurd numbers
        let pessimistic_secs = pessimistic_secs.min(estimated_secs * 10.0);

        // Determine confidence based on sample count and stability
        let confidence = if state.sample_count >= 20 && cv < 0.3 {
            EtaConfidence::High
        } else if state.sample_count >= 5 && cv < 0.8 {
            EtaConfidence::Medium
        } else {
            EtaConfidence::Low
        };

        Some(EtaEstimate {
            estimated_secs,
            optimistic_secs,
            pessimistic_secs,
            confidence,
            smoothed_speed_bps: state.ema,
            raw_speed_bps: state.last_raw_bps,
            sample_count: state.sample_count,
            speed_stability: cv,
        })
    }

    /// Remove tracking for a completed/removed task
    pub async fn remove_task(&self, task_id: &str) {
        let mut tasks = self.tasks.write().await;
        tasks.remove(task_id);
    }

    /// Clear all tracking data
    pub async fn clear(&self) {
        let mut tasks = self.tasks.write().await;
        tasks.clear();
    }

    /// Get the number of tracked tasks
    pub async fn tracked_count(&self) -> usize {
        let tasks = self.tasks.read().await;
        tasks.len()
    }

    /// Get per-task EMA speed (for debugging / display)
    pub async fn smoothed_speed(&self, task_id: &str) -> Option<f64> {
        let tasks = self.tasks.read().await;
        tasks.get(task_id).map(|s| s.ema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_speed_state_first_sample() {
        let mut state = TaskSpeedState::new();
        state.update(1000.0, 0.3);
        assert_eq!(state.ema, 1000.0);
        assert_eq!(state.emvar, 0.0);
        assert_eq!(state.sample_count, 1);
    }

    #[test]
    fn test_task_speed_state_ema_convergence() {
        let mut state = TaskSpeedState::new();
        // Feed constant speed — EMA should converge immediately
        for _ in 0..10 {
            state.update(500.0, 0.3);
        }
        assert!((state.ema - 500.0).abs() < 1.0);
        assert!(state.emvar < 1.0); // variance should be near zero
    }

    #[test]
    fn test_task_speed_state_responds_to_change() {
        let mut state = TaskSpeedState::new();
        // Seed with 1000
        for _ in 0..5 {
            state.update(1000.0, 0.3);
        }
        let ema_before = state.ema;

        // Drop to 100
        state.update(100.0, 0.3);
        // EMA should have dropped significantly
        assert!(state.ema < ema_before);
        assert!(state.ema > 100.0); // but not all the way (it's smoothed)
    }

    #[test]
    fn test_task_speed_state_variance_grows_with_volatility() {
        let mut stable = TaskSpeedState::new();
        let mut volatile = TaskSpeedState::new();

        for i in 0..20 {
            stable.update(500.0, 0.3);
            volatile.update(if i % 2 == 0 { 100.0 } else { 900.0 }, 0.3);
        }

        assert!(volatile.emvar > stable.emvar * 10.0);
    }

    #[test]
    fn test_coefficient_of_variation() {
        let mut state = TaskSpeedState::new();
        for _ in 0..10 {
            state.update(500.0, 0.3);
        }
        // Constant speed → CV should be ~0
        assert!(state.coefficient_of_variation() < 0.01);
    }

    #[test]
    fn test_coefficient_of_variation_zero_speed() {
        let state = TaskSpeedState::new();
        assert_eq!(state.coefficient_of_variation(), f64::MAX);
    }

    #[tokio::test]
    async fn test_estimator_too_few_samples() {
        let est = EtaEstimator::new();
        est.update_speed("t1", 1000.0).await;
        est.update_speed("t1", 1200.0).await;
        // Only 2 samples, below MIN_SAMPLES_FOR_ETA
        assert!(est.estimate("t1", 10_000).await.is_none());
    }

    #[tokio::test]
    async fn test_estimator_basic_eta() {
        let est = EtaEstimator::new();
        // Feed constant 1000 B/s for enough samples
        for _ in 0..5 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 5000).await.unwrap();
        // ETA should be ~5 seconds
        assert!((result.estimated_secs - 5.0).abs() < 1.0);
        assert_eq!(result.confidence, EtaConfidence::Medium);
    }

    #[tokio::test]
    async fn test_estimator_stall_detection() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("t1", 0.5).await; // below stall threshold
        }
        assert!(est.estimate("t1", 5000).await.is_none());
    }

    #[tokio::test]
    async fn test_estimator_confidence_intervals() {
        let est = EtaEstimator::new();
        for _ in 0..10 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 10_000).await.unwrap();
        // With stable speed, optimistic and pessimistic should be close
        assert!(result.optimistic_secs <= result.estimated_secs);
        assert!(result.pessimistic_secs >= result.estimated_secs);
        assert!(
            result.confidence == EtaConfidence::Medium || result.confidence == EtaConfidence::High
        );
    }

    #[tokio::test]
    async fn test_estimator_volatile_speed_low_confidence() {
        let est = EtaEstimator::new();
        for i in 0..10 {
            let speed = if i % 2 == 0 { 100.0 } else { 10000.0 };
            est.update_speed("t1", speed).await;
        }
        let result = est.estimate("t1", 10_000).await.unwrap();
        // Volatile speed → low confidence
        assert_eq!(result.confidence, EtaConfidence::Low);
        // Wide interval
        assert!(result.pessimistic_secs > result.optimistic_secs * 2.0);
    }

    #[tokio::test]
    async fn test_estimator_high_confidence() {
        let est = EtaEstimator::new();
        // 25 stable samples
        for _ in 0..25 {
            est.update_speed("t1", 5000.0).await;
        }
        let result = est.estimate("t1", 50_000).await.unwrap();
        assert_eq!(result.confidence, EtaConfidence::High);
        assert!(result.speed_stability < 0.3);
    }

    #[tokio::test]
    async fn test_estimator_remove_task() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("t1", 1000.0).await;
        }
        assert_eq!(est.tracked_count().await, 1);
        est.remove_task("t1").await;
        assert_eq!(est.tracked_count().await, 0);
        assert!(est.estimate("t1", 5000).await.is_none());
    }

    #[tokio::test]
    async fn test_estimator_clear() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("t1", 1000.0).await;
            est.update_speed("t2", 2000.0).await;
        }
        assert_eq!(est.tracked_count().await, 2);
        est.clear().await;
        assert_eq!(est.tracked_count().await, 0);
    }

    #[tokio::test]
    async fn test_estimator_unknown_task() {
        let est = EtaEstimator::new();
        assert!(est.estimate("nonexistent", 5000).await.is_none());
    }

    #[tokio::test]
    async fn test_estimator_negative_speed_clamped() {
        let est = EtaEstimator::new();
        est.update_speed("t1", -100.0).await;
        let speed = est.smoothed_speed("t1").await.unwrap();
        assert!(speed >= 0.0);
    }

    #[tokio::test]
    async fn test_estimator_zero_remaining() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 0).await.unwrap();
        assert!(result.estimated_secs < 1.0);
    }

    #[test]
    fn test_format_eta_seconds() {
        let e = EtaEstimate {
            estimated_secs: 45.0,
            optimistic_secs: 30.0,
            pessimistic_secs: 60.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 1000.0,
            raw_speed_bps: 1100.0,
            sample_count: 10,
            speed_stability: 0.1,
        };
        assert_eq!(e.format_eta(), "45s");
    }

    #[test]
    fn test_format_eta_minutes() {
        let e = EtaEstimate {
            estimated_secs: 330.0,
            optimistic_secs: 200.0,
            pessimistic_secs: 500.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 500.0,
            raw_speed_bps: 550.0,
            sample_count: 10,
            speed_stability: 0.2,
        };
        assert_eq!(e.format_eta(), "5m 30s");
    }

    #[test]
    fn test_format_eta_hours() {
        let e = EtaEstimate {
            estimated_secs: 7260.0,
            optimistic_secs: 3600.0,
            pessimistic_secs: 14400.0,
            confidence: EtaConfidence::Low,
            smoothed_speed_bps: 100.0,
            raw_speed_bps: 120.0,
            sample_count: 5,
            speed_stability: 0.5,
        };
        assert_eq!(e.format_eta(), "2h 1m");
    }

    #[test]
    fn test_format_range_string() {
        let e = EtaEstimate {
            estimated_secs: 330.0,
            optimistic_secs: 200.0,
            pessimistic_secs: 500.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 500.0,
            raw_speed_bps: 550.0,
            sample_count: 10,
            speed_stability: 0.2,
        };
        assert_eq!(e.format_range(), "5m 30s (3m 20s–8m 20s)");
    }

    #[test]
    fn test_format_eta_unknown() {
        let e = EtaEstimate {
            estimated_secs: f64::INFINITY,
            optimistic_secs: 0.0,
            pessimistic_secs: 0.0,
            confidence: EtaConfidence::Low,
            smoothed_speed_bps: 0.0,
            raw_speed_bps: 0.0,
            sample_count: 1,
            speed_stability: f64::MAX,
        };
        assert_eq!(e.format_eta(), "unknown");
    }

    #[test]
    fn test_eta_confidence_labels() {
        assert_eq!(EtaConfidence::Low.label(), "low");
        assert_eq!(EtaConfidence::Medium.label(), "medium");
        assert_eq!(EtaConfidence::High.label(), "high");
    }

    #[test]
    fn test_eta_estimate_serialization() {
        let e = EtaEstimate {
            estimated_secs: 100.0,
            optimistic_secs: 80.0,
            pessimistic_secs: 150.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 500.0,
            raw_speed_bps: 550.0,
            sample_count: 10,
            speed_stability: 0.15,
        };
        let json = serde_json::to_string(&e).unwrap();
        let parsed: EtaEstimate = serde_json::from_str(&json).unwrap();
        assert!((parsed.estimated_secs - 100.0).abs() < 0.01);
        assert_eq!(parsed.confidence, EtaConfidence::Medium);
    }

    #[tokio::test]
    async fn test_estimator_with_custom_alpha() {
        let est = EtaEstimator::with_alpha(0.8);
        // High alpha → very responsive
        for _ in 0..5 {
            est.update_speed("t1", 1000.0).await;
        }
        // Suddenly jump to 5000
        est.update_speed("t1", 5000.0).await;
        let speed = est.smoothed_speed("t1").await.unwrap();
        // With alpha=0.8, EMA should jump significantly toward 5000
        assert!(speed > 3000.0);
    }

    #[tokio::test]
    async fn test_estimator_alpha_clamped() {
        // Alpha > 1.0 should be clamped
        let est = EtaEstimator::with_alpha(5.0);
        assert!((est.alpha - 1.0).abs() < 0.001);

        // Alpha < 0.01 should be clamped
        let est = EtaEstimator::with_alpha(-1.0);
        assert!((est.alpha - 0.01).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_estimator_pessimistic_cap() {
        let est = EtaEstimator::new();
        // Create a scenario with extreme variance
        for i in 0..10 {
            let speed = if i % 2 == 0 { 1.0 } else { 100000.0 };
            est.update_speed("t1", speed).await;
        }
        let result = est.estimate("t1", 10_000).await.unwrap();
        // Pessimistic should be capped at 10x estimate
        assert!(result.pessimistic_secs <= result.estimated_secs * 10.0 + 1.0);
    }

    #[tokio::test]
    async fn test_estimator_multiple_tasks_independent() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("fast", 10000.0).await;
            est.update_speed("slow", 100.0).await;
        }
        let fast = est.estimate("fast", 50_000).await.unwrap();
        let slow = est.estimate("slow", 50_000).await.unwrap();
        // Fast task should have much lower ETA
        assert!(fast.estimated_secs < slow.estimated_secs / 10.0);
    }

    // ============================================================
    // Phase 230: Comprehensive Test Coverage
    // ============================================================

    // --- TaskSpeedState internals ---

    #[test]
    fn task_speed_state_new_defaults() {
        let state = TaskSpeedState::new();
        assert_eq!(state.ema, 0.0);
        assert_eq!(state.emvar, 0.0);
        assert_eq!(state.sample_count, 0);
        assert_eq!(state.last_raw_bps, 0.0);
    }

    #[test]
    fn task_speed_state_second_sample() {
        let mut state = TaskSpeedState::new();
        state.update(1000.0, 0.3);
        state.update(2000.0, 0.3);
        assert_eq!(state.sample_count, 2);
        assert_eq!(state.last_raw_bps, 2000.0);
        // EMA: 1000 + 0.3 * (2000 - 1000) = 1300
        assert!((state.ema - 1300.0).abs() < 0.01);
    }

    #[test]
    fn task_speed_state_zero_speed_update() {
        let mut state = TaskSpeedState::new();
        state.update(0.0, 0.3);
        assert_eq!(state.ema, 0.0);
        assert_eq!(state.sample_count, 1);
        state.update(0.0, 0.3);
        assert_eq!(state.sample_count, 2);
    }

    #[test]
    fn task_speed_state_large_speed() {
        let mut state = TaskSpeedState::new();
        state.update(1e15, 0.3);
        assert_eq!(state.ema, 1e15);
        assert_eq!(state.sample_count, 1);
    }

    #[test]
    fn task_speed_state_stddev_zero_for_constant() {
        let mut state = TaskSpeedState::new();
        for _ in 0..20 {
            state.update(500.0, 0.3);
        }
        assert!(state.stddev() < 0.001);
    }

    #[test]
    fn task_speed_state_stddev_positive_for_varying() {
        let mut state = TaskSpeedState::new();
        state.update(100.0, 0.3);
        state.update(900.0, 0.3);
        state.update(100.0, 0.3);
        assert!(state.stddev() > 0.0);
    }

    #[test]
    fn task_speed_state_cv_zero_ema() {
        let state = TaskSpeedState {
            ema: 0.0,
            emvar: 10.0,
            sample_count: 5,
            last_raw_bps: 0.0,
        };
        assert_eq!(state.coefficient_of_variation(), f64::MAX);
    }

    #[test]
    fn task_speed_state_negative_ema_cv() {
        let state = TaskSpeedState {
            ema: -1.0,
            emvar: 1.0,
            sample_count: 5,
            last_raw_bps: 0.0,
        };
        // Negative EMA → returns MAX
        assert_eq!(state.coefficient_of_variation(), f64::MAX);
    }

    #[test]
    fn task_speed_state_clone() {
        let mut state = TaskSpeedState::new();
        state.update(1000.0, 0.3);
        let cloned = state.clone();
        assert_eq!(cloned.ema, state.ema);
        assert_eq!(cloned.sample_count, state.sample_count);
    }

    #[test]
    fn task_speed_state_debug() {
        let state = TaskSpeedState::new();
        let debug = format!("{:?}", state);
        assert!(debug.contains("TaskSpeedState"));
        assert!(debug.contains("ema"));
    }

    // --- EtaConfidence ---

    #[test]
    fn eta_confidence_serde_roundtrip() {
        for c in [
            EtaConfidence::Low,
            EtaConfidence::Medium,
            EtaConfidence::High,
        ] {
            let json = serde_json::to_string(&c).unwrap();
            let parsed: EtaConfidence = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, c);
        }
    }

    #[test]
    fn eta_confidence_snake_case_values() {
        assert_eq!(
            serde_json::to_string(&EtaConfidence::Low).unwrap(),
            "\"low\""
        );
        assert_eq!(
            serde_json::to_string(&EtaConfidence::Medium).unwrap(),
            "\"medium\""
        );
        assert_eq!(
            serde_json::to_string(&EtaConfidence::High).unwrap(),
            "\"high\""
        );
    }

    #[test]
    fn eta_confidence_clone_copy() {
        let c = EtaConfidence::High;
        let c2 = c;
        let c3 = c.clone();
        assert_eq!(c2, c3);
    }

    #[test]
    fn eta_confidence_debug() {
        let debug = format!("{:?}", EtaConfidence::Medium);
        assert!(debug.contains("Medium"));
    }

    #[test]
    fn eta_confidence_eq() {
        assert_eq!(EtaConfidence::Low, EtaConfidence::Low);
        assert_ne!(EtaConfidence::Low, EtaConfidence::High);
        assert_ne!(EtaConfidence::Medium, EtaConfidence::High);
    }

    #[test]
    fn eta_confidence_label_all() {
        assert_eq!(EtaConfidence::Low.label(), "low");
        assert_eq!(EtaConfidence::Medium.label(), "medium");
        assert_eq!(EtaConfidence::High.label(), "high");
    }

    // --- EtaEstimate ---

    #[test]
    fn eta_estimate_serde_roundtrip() {
        let e = EtaEstimate {
            estimated_secs: 120.0,
            optimistic_secs: 90.0,
            pessimistic_secs: 200.0,
            confidence: EtaConfidence::High,
            smoothed_speed_bps: 800.0,
            raw_speed_bps: 850.0,
            sample_count: 25,
            speed_stability: 0.1,
        };
        let json = serde_json::to_string(&e).unwrap();
        let parsed: EtaEstimate = serde_json::from_str(&json).unwrap();
        assert!((parsed.estimated_secs - 120.0).abs() < 0.01);
        assert_eq!(parsed.confidence, EtaConfidence::High);
        assert_eq!(parsed.sample_count, 25);
    }

    #[test]
    fn eta_estimate_serde_extra_fields_ignored() {
        let json = r#"{"estimated_secs":10,"optimistic_secs":5,"pessimistic_secs":20,"confidence":"low","smoothed_speed_bps":100,"raw_speed_bps":120,"sample_count":3,"speed_stability":0.5,"extra_field":"ignored"}"#;
        let parsed: EtaEstimate = serde_json::from_str(json).unwrap();
        assert!((parsed.estimated_secs - 10.0).abs() < 0.01);
    }

    #[test]
    fn eta_estimate_clone() {
        let e = EtaEstimate {
            estimated_secs: 50.0,
            optimistic_secs: 40.0,
            pessimistic_secs: 70.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 500.0,
            raw_speed_bps: 550.0,
            sample_count: 10,
            speed_stability: 0.2,
        };
        let cloned = e.clone();
        assert_eq!(cloned.estimated_secs, e.estimated_secs);
        assert_eq!(cloned.confidence, e.confidence);
    }

    #[test]
    fn eta_estimate_debug() {
        let e = EtaEstimate {
            estimated_secs: 50.0,
            optimistic_secs: 40.0,
            pessimistic_secs: 70.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 500.0,
            raw_speed_bps: 550.0,
            sample_count: 10,
            speed_stability: 0.2,
        };
        let debug = format!("{:?}", e);
        assert!(debug.contains("EtaEstimate"));
    }

    #[test]
    fn format_eta_zero() {
        let e = EtaEstimate {
            estimated_secs: 0.0,
            optimistic_secs: 0.0,
            pessimistic_secs: 0.0,
            confidence: EtaConfidence::High,
            smoothed_speed_bps: 1000.0,
            raw_speed_bps: 1000.0,
            sample_count: 20,
            speed_stability: 0.05,
        };
        assert_eq!(e.format_eta(), "0s");
    }

    #[test]
    fn format_eta_nan() {
        let e = EtaEstimate {
            estimated_secs: f64::NAN,
            optimistic_secs: 0.0,
            pessimistic_secs: 0.0,
            confidence: EtaConfidence::Low,
            smoothed_speed_bps: 0.0,
            raw_speed_bps: 0.0,
            sample_count: 1,
            speed_stability: f64::MAX,
        };
        assert_eq!(e.format_eta(), "unknown");
    }

    #[test]
    fn format_eta_exactly_60() {
        let e = EtaEstimate {
            estimated_secs: 60.0,
            optimistic_secs: 50.0,
            pessimistic_secs: 80.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 100.0,
            raw_speed_bps: 110.0,
            sample_count: 10,
            speed_stability: 0.1,
        };
        assert_eq!(e.format_eta(), "1m 0s");
    }

    #[test]
    fn format_eta_exactly_3600() {
        let e = EtaEstimate {
            estimated_secs: 3600.0,
            optimistic_secs: 3000.0,
            pessimistic_secs: 5000.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 100.0,
            raw_speed_bps: 110.0,
            sample_count: 10,
            speed_stability: 0.1,
        };
        assert_eq!(e.format_eta(), "1h 0m");
    }

    #[test]
    fn format_range_with_range() {
        let e = EtaEstimate {
            estimated_secs: 120.0,
            optimistic_secs: 60.0,
            pessimistic_secs: 240.0,
            confidence: EtaConfidence::Medium,
            smoothed_speed_bps: 500.0,
            raw_speed_bps: 550.0,
            sample_count: 10,
            speed_stability: 0.2,
        };
        assert_eq!(e.format_range(), "2m 0s (1m 0s–4m 0s)");
    }

    #[test]
    fn format_range_infinite_bounds() {
        let e = EtaEstimate {
            estimated_secs: 100.0,
            optimistic_secs: f64::INFINITY,
            pessimistic_secs: f64::NAN,
            confidence: EtaConfidence::Low,
            smoothed_speed_bps: 100.0,
            raw_speed_bps: 100.0,
            sample_count: 5,
            speed_stability: 0.5,
        };
        assert_eq!(e.format_range(), "1m 40s (?–?)");
    }

    // --- format_secs helper ---

    #[test]
    fn format_secs_zero() {
        assert_eq!(format_secs(0.0), "0s");
    }

    #[test]
    fn format_secs_small() {
        assert_eq!(format_secs(45.0), "45s");
    }

    #[test]
    fn format_secs_minutes() {
        assert_eq!(format_secs(150.0), "2m 30s");
    }

    #[test]
    fn format_secs_hours() {
        assert_eq!(format_secs(7200.0), "2h 0m");
    }

    #[test]
    fn format_secs_infinity() {
        assert_eq!(format_secs(f64::INFINITY), "?");
    }

    #[test]
    fn format_secs_nan() {
        assert_eq!(format_secs(f64::NAN), "?");
    }

    // --- EtaEstimator ---

    #[test]
    fn estimator_new_defaults() {
        let est = EtaEstimator::new();
        assert!((est.alpha - DEFAULT_ALPHA).abs() < 0.001);
    }

    #[test]
    fn estimator_default_equals_new() {
        let est = EtaEstimator::default();
        assert!((est.alpha - DEFAULT_ALPHA).abs() < 0.001);
    }

    #[test]
    fn estimator_debug() {
        let est = EtaEstimator::new();
        let debug = format!("{:?}", est);
        assert!(debug.contains("EtaEstimator"));
    }

    #[tokio::test]
    async fn estimator_tracked_count_empty() {
        let est = EtaEstimator::new();
        assert_eq!(est.tracked_count().await, 0);
    }

    #[tokio::test]
    async fn estimator_tracked_count_after_updates() {
        let est = EtaEstimator::new();
        est.update_speed("t1", 100.0).await;
        est.update_speed("t2", 200.0).await;
        assert_eq!(est.tracked_count().await, 2);
    }

    #[tokio::test]
    async fn estimator_smoothed_speed_unknown() {
        let est = EtaEstimator::new();
        assert!(est.smoothed_speed("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn estimator_smoothed_speed_after_update() {
        let est = EtaEstimator::new();
        est.update_speed("t1", 1000.0).await;
        let speed = est.smoothed_speed("t1").await.unwrap();
        assert_eq!(speed, 1000.0);
    }

    #[tokio::test]
    async fn estimator_remove_nonexistent_no_panic() {
        let est = EtaEstimator::new();
        est.remove_task("nonexistent").await;
        assert_eq!(est.tracked_count().await, 0);
    }

    #[tokio::test]
    async fn estimator_clear_empty() {
        let est = EtaEstimator::new();
        est.clear().await;
        assert_eq!(est.tracked_count().await, 0);
    }

    #[tokio::test]
    async fn estimator_estimate_exactly_min_samples() {
        let est = EtaEstimator::new();
        for _ in 0..3 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 3000).await;
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.sample_count, 3);
        assert!((r.estimated_secs - 3.0).abs() < 0.5);
    }

    #[tokio::test]
    async fn estimator_estimate_one_below_min() {
        let est = EtaEstimator::new();
        for _ in 0..2 {
            est.update_speed("t1", 1000.0).await;
        }
        assert!(est.estimate("t1", 3000).await.is_none());
    }

    #[tokio::test]
    async fn estimator_stall_exactly_at_threshold() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("t1", 1.0).await; // exactly at threshold
        }
        // EMA should be ~1.0, which is <= STALL_THRESHOLD_BPS
        assert!(est.estimate("t1", 5000).await.is_none());
    }

    #[tokio::test]
    async fn estimator_just_above_stall() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("t1", 1.5).await;
        }
        let result = est.estimate("t1", 15).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn estimator_unicode_task_id() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("任务-α", 1000.0).await;
        }
        let result = est.estimate("任务-α", 5000).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn estimator_emoji_task_id() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("🚀fast", 2000.0).await;
        }
        let result = est.estimate("🚀fast", 10000).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn estimator_empty_task_id() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("", 1000.0).await;
        }
        let result = est.estimate("", 5000).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn estimator_large_remaining() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", u64::MAX).await.unwrap();
        assert!(result.estimated_secs.is_infinite() || result.estimated_secs > 1e15);
    }

    #[tokio::test]
    async fn estimator_zero_speed_update() {
        let est = EtaEstimator::new();
        est.update_speed("t1", 0.0).await;
        let speed = est.smoothed_speed("t1").await.unwrap();
        assert_eq!(speed, 0.0);
    }

    #[tokio::test]
    async fn estimator_remove_then_readd() {
        let est = EtaEstimator::new();
        for _ in 0..5 {
            est.update_speed("t1", 1000.0).await;
        }
        est.remove_task("t1").await;
        assert!(est.smoothed_speed("t1").await.is_none());

        // Re-add
        est.update_speed("t1", 5000.0).await;
        let speed = est.smoothed_speed("t1").await.unwrap();
        assert_eq!(speed, 5000.0); // fresh start
    }

    #[tokio::test]
    async fn estimator_many_tasks() {
        let est = EtaEstimator::new();
        for i in 0..100 {
            est.update_speed(&format!("task_{}", i), 1000.0 * (i as f64 + 1.0))
                .await;
        }
        assert_eq!(est.tracked_count().await, 100);
    }

    #[tokio::test]
    async fn estimator_confidence_low_samples() {
        let est = EtaEstimator::new();
        // 3 samples = just enough for ETA, but low confidence
        for _ in 0..3 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 5000).await.unwrap();
        assert_eq!(result.confidence, EtaConfidence::Low);
    }

    #[tokio::test]
    async fn estimator_confidence_medium_boundary() {
        let est = EtaEstimator::new();
        // 5 samples with stable speed
        for _ in 0..5 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 5000).await.unwrap();
        // 5 samples + low CV → Medium
        assert_eq!(result.confidence, EtaConfidence::Medium);
    }

    #[tokio::test]
    async fn estimator_confidence_high_boundary() {
        let est = EtaEstimator::new();
        for _ in 0..20 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 5000).await.unwrap();
        assert_eq!(result.confidence, EtaConfidence::High);
    }

    #[tokio::test]
    async fn estimator_confidence_high_cv_too_high() {
        let est = EtaEstimator::new();
        // 25 samples but volatile → not High
        for i in 0..25 {
            let speed = if i % 2 == 0 { 100.0 } else { 10000.0 };
            est.update_speed("t1", speed).await;
        }
        let result = est.estimate("t1", 5000).await.unwrap();
        assert_ne!(result.confidence, EtaConfidence::High);
    }

    #[tokio::test]
    async fn estimator_optimistic_lte_estimated() {
        let est = EtaEstimator::new();
        for _ in 0..10 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 10000).await.unwrap();
        assert!(result.optimistic_secs <= result.estimated_secs + 0.001);
    }

    #[tokio::test]
    async fn estimator_pessimistic_gte_estimated() {
        let est = EtaEstimator::new();
        for _ in 0..10 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 10000).await.unwrap();
        assert!(result.pessimistic_secs >= result.estimated_secs - 0.001);
    }

    #[tokio::test]
    async fn estimator_speed_stability_non_negative() {
        let est = EtaEstimator::new();
        for _ in 0..10 {
            est.update_speed("t1", 1000.0).await;
        }
        let result = est.estimate("t1", 5000).await.unwrap();
        assert!(result.speed_stability >= 0.0);
    }

    #[tokio::test]
    async fn estimator_raw_speed_matches_last() {
        let est = EtaEstimator::new();
        est.update_speed("t1", 1000.0).await;
        est.update_speed("t1", 3000.0).await;
        est.update_speed("t1", 5000.0).await;
        let result = est.estimate("t1", 5000).await.unwrap();
        assert_eq!(result.raw_speed_bps, 5000.0);
    }

    #[tokio::test]
    async fn estimator_with_alpha_boundary_low() {
        let est = EtaEstimator::with_alpha(0.01);
        assert!((est.alpha - 0.01).abs() < 0.001);
    }

    #[tokio::test]
    async fn estimator_with_alpha_boundary_high() {
        let est = EtaEstimator::with_alpha(1.0);
        assert!((est.alpha - 1.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn estimator_with_alpha_zero_clamped() {
        let est = EtaEstimator::with_alpha(0.0);
        assert!((est.alpha - 0.01).abs() < 0.001);
    }

    #[tokio::test]
    async fn estimator_smoothed_speed_monotonic_increase() {
        let est = EtaEstimator::new();
        // Feed increasing speeds
        for i in 1..=10 {
            est.update_speed("t1", (i * 100) as f64).await;
        }
        let speed = est.smoothed_speed("t1").await.unwrap();
        // EMA should be somewhere in the middle-high range
        assert!(speed > 500.0);
        assert!(speed < 1000.0);
    }

    // --- Full lifecycle ---

    #[tokio::test]
    async fn full_lifecycle() {
        let est = EtaEstimator::new();

        // Phase 1: Add tasks
        for _ in 0..5 {
            est.update_speed("download_1", 5000.0).await;
            est.update_speed("download_2", 1000.0).await;
        }
        assert_eq!(est.tracked_count().await, 2);

        // Phase 2: Estimate
        let eta1 = est.estimate("download_1", 50_000).await.unwrap();
        let eta2 = est.estimate("download_2", 50_000).await.unwrap();
        assert!(eta1.estimated_secs < eta2.estimated_secs);

        // Phase 3: Remove one
        est.remove_task("download_1").await;
        assert_eq!(est.tracked_count().await, 1);
        assert!(est.estimate("download_1", 50_000).await.is_none());

        // Phase 4: Clear
        est.clear().await;
        assert_eq!(est.tracked_count().await, 0);
        assert!(est.estimate("download_2", 50_000).await.is_none());
    }

    #[tokio::test]
    async fn lifecycle_update_estimate_remove_readd() {
        let est = EtaEstimator::new();

        // Update
        for _ in 0..5 {
            est.update_speed("t1", 2000.0).await;
        }
        let eta = est.estimate("t1", 10_000).await.unwrap();
        assert!((eta.estimated_secs - 5.0).abs() < 1.0);

        // Remove
        est.remove_task("t1").await;
        assert!(est.estimate("t1", 10_000).await.is_none());

        // Re-add with different speed
        for _ in 0..5 {
            est.update_speed("t1", 10_000.0).await;
        }
        let eta2 = est.estimate("t1", 10_000).await.unwrap();
        assert!((eta2.estimated_secs - 1.0).abs() < 0.5);
    }

    #[tokio::test]
    async fn independent_estimators() {
        let est1 = EtaEstimator::new();
        let est2 = EtaEstimator::new();

        for _ in 0..5 {
            est1.update_speed("t1", 1000.0).await;
        }

        assert!(est1.smoothed_speed("t1").await.is_some());
        assert!(est2.smoothed_speed("t1").await.is_none());
    }

    #[tokio::test]
    async fn estimator_same_task_different_speeds_converge() {
        let est = EtaEstimator::new();
        // Feed constant 2000 B/s
        for _ in 0..50 {
            est.update_speed("t1", 2000.0).await;
        }
        let speed = est.smoothed_speed("t1").await.unwrap();
        assert!((speed - 2000.0).abs() < 1.0);

        let result = est.estimate("t1", 20_000).await.unwrap();
        assert!((result.estimated_secs - 10.0).abs() < 1.0);
        assert_eq!(result.confidence, EtaConfidence::High);
    }

    #[test]
    fn constants_are_reasonable() {
        assert!(DEFAULT_ALPHA > 0.0 && DEFAULT_ALPHA <= 1.0);
        assert!(MIN_SAMPLES_FOR_ETA > 0);
        assert!(STALL_THRESHOLD_BPS > 0.0);
    }
}
