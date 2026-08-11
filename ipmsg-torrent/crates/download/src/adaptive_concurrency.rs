//! Adaptive download concurrency optimization
//!
//! Automatically adjusts the number of concurrent connections per download task
//! based on server response time (RTT) and error rate, optimizing download speed
//! while avoiding server-side rate limiting or connection rejection.
//!
//! ## Performance optimizations
//! - EWMA (Exponentially Weighted Moving Average) for RTT smoothing
//! - BBR-inspired bandwidth estimation for optimal concurrency
//! - Per-domain concurrency limits to avoid single-domain overload
//! - Time-decay weighted samples for more responsive adaptation
//! - Hysteresis to prevent oscillation between states

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Configuration for adaptive concurrency
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AdaptiveConcurrencyConfig {
    /// Enable adaptive concurrency
    pub enabled: bool,
    /// Minimum concurrent connections per task
    pub min_connections: u32,
    /// Maximum concurrent connections per task
    pub max_connections: u32,
    /// Initial connections for new tasks
    pub initial_connections: u32,
    /// Target response time in milliseconds (below this = increase connections)
    pub target_response_ms: u64,
    /// Response time threshold for decreasing connections (ms)
    pub high_latency_threshold_ms: u64,
    /// Error rate threshold (0.0-1.0) for decreasing connections
    pub error_rate_threshold: f64,
    /// Number of samples to collect before making adjustment decisions
    pub sample_window: u32,
    /// Minimum time between adjustments
    pub adjustment_cooldown_secs: u64,
    /// Factor to increase connections by (multiplicative)
    pub increase_factor: f64,
    /// Factor to decrease connections by (multiplicative)
    pub decrease_factor: f64,
}

impl Default for AdaptiveConcurrencyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_connections: 1,
            max_connections: 16,
            initial_connections: 4,
            target_response_ms: 200,
            high_latency_threshold_ms: 1000,
            error_rate_threshold: 0.1,
            sample_window: 10,
            adjustment_cooldown_secs: 30,
            increase_factor: 1.5,
            decrease_factor: 0.7,
        }
    }
}

/// A response time sample with bandwidth estimation
#[derive(Debug, Clone)]
pub struct ResponseSample {
    pub timestamp: Instant,
    pub response_time_ms: f64,
    pub bytes_transferred: u64,
    pub success: bool,
}

impl ResponseSample {
    /// Calculate instantaneous throughput in bytes/sec
    pub fn throughput_bps(&self) -> f64 {
        if self.response_time_ms > 0.0 {
            (self.bytes_transferred as f64 * 1000.0) / self.response_time_ms
        } else {
            0.0
        }
    }
}

/// Current concurrency state for a task
#[derive(Debug, Clone)]
pub struct ConcurrencyState {
    pub current_connections: u32,
    pub samples: Vec<ResponseSample>,
    pub last_adjustment: Option<Instant>,
    pub last_adjustment_direction: AdjustmentDirection,
    pub total_adjustments: u32,
    /// Smoothed RTT using EWMA (Exponentially Weighted Moving Average)
    pub smoothed_rtt_ms: f64,
    /// RTT variance for confidence estimation
    pub rtt_variance_ms: f64,
    /// Estimated bandwidth in bytes/sec (from best recent samples)
    pub estimated_bandwidth_bps: f64,
    /// Domain for per-domain limiting
    pub domain: Option<String>,
    /// Minimum RTT observed (baseline for BBR-style estimation)
    pub min_rtt_ms: f64,
    /// Count of consecutive increases (for exponential growth phase)
    pub consecutive_increases: u32,
    /// Count of consecutive decreases (for backoff stabilization)
    pub consecutive_decreases: u32,
}

/// EWMA smoothing factor for RTT (alpha = 0.125, like TCP RTT estimation)
const EWMA_ALPHA: f64 = 0.125;
/// Variance smoothing factor (beta = 0.25, like TCP RTTVAR)
const EWMA_BETA: f64 = 0.25;

/// Direction of the last adjustment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdjustmentDirection {
    None,
    Increased,
    Decreased,
}

/// Decision made by the adaptive concurrency algorithm
#[derive(Debug, Clone, PartialEq)]
pub enum ConcurrencyDecision {
    /// No change needed
    Hold,
    /// Increase connections
    Increase { from: u32, to: u32, reason: String },
    /// Decrease connections
    Decrease { from: u32, to: u32, reason: String },
}

/// Summary of adaptive concurrency for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConcurrencySummary {
    pub task_id: String,
    pub current_connections: u32,
    pub avg_response_ms: f64,
    pub error_rate: f64,
    pub total_adjustments: u32,
    pub last_direction: AdjustmentDirection,
}

/// Overall adaptive concurrency summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConcurrencySummary {
    pub enabled: bool,
    pub config: AdaptiveConcurrencyConfig,
    pub task_count: usize,
    pub tasks: Vec<TaskConcurrencySummary>,
    pub total_adjustments: u32,
}

/// Per-domain concurrency tracking
#[derive(Debug)]
struct DomainState {
    /// Total active connections across all tasks using this domain
    active_connections: u32,
    /// Maximum allowed connections for this domain
    max_connections: u32,
    /// Last time this domain was rate-limited
    last_throttle: Option<Instant>,
}

/// Manages adaptive concurrency for all download tasks
#[derive(Debug)]
pub struct AdaptiveConcurrencyManager {
    config: AdaptiveConcurrencyConfig,
    states: HashMap<String, ConcurrencyState>,
    /// Per-domain connection tracking
    domain_states: HashMap<String, DomainState>,
    /// Default per-domain connection limit
    default_domain_limit: u32,
}

impl AdaptiveConcurrencyManager {
    /// Create a new manager with default config
    pub fn new() -> Self {
        Self {
            config: AdaptiveConcurrencyConfig::default(),
            states: HashMap::new(),
            domain_states: HashMap::new(),
            default_domain_limit: 16,
        }
    }

    /// Create a new manager with custom config
    pub fn with_config(config: AdaptiveConcurrencyConfig) -> Self {
        Self {
            config,
            states: HashMap::new(),
            domain_states: HashMap::new(),
            default_domain_limit: 16,
        }
    }

    /// Set the default per-domain connection limit
    pub fn set_domain_limit(&mut self, limit: u32) {
        self.default_domain_limit = limit;
    }

    /// Set per-domain connection limit for a specific domain
    pub fn set_domain_specific_limit(&mut self, domain: &str, limit: u32) {
        self.domain_states
            .entry(domain.to_string())
            .and_modify(|s| s.max_connections = limit)
            .or_insert(DomainState {
                active_connections: 0,
                max_connections: limit,
                last_throttle: None,
            });
    }

    /// Get the recommended connection count considering per-domain limits
    pub fn get_connections_for_domain(&self, task_id: &str, domain: &str) -> u32 {
        let task_conn = self.get_connections(task_id);

        // Check per-domain limit
        if let Some(domain_state) = self.domain_states.get(domain) {
            let remaining = domain_state
                .max_connections
                .saturating_sub(domain_state.active_connections);
            return task_conn.min(remaining).max(self.config.min_connections);
        }

        task_conn.min(self.default_domain_limit)
    }

    /// Register a connection as active for domain tracking
    pub fn register_active_connection(&mut self, domain: &str) {
        self.domain_states
            .entry(domain.to_string())
            .and_modify(|s| s.active_connections += 1)
            .or_insert(DomainState {
                active_connections: 1,
                max_connections: self.default_domain_limit,
                last_throttle: None,
            });
    }

    /// Unregister a connection from domain tracking
    pub fn unregister_active_connection(&mut self, domain: &str) {
        if let Some(state) = self.domain_states.get_mut(domain) {
            state.active_connections = state.active_connections.saturating_sub(1);
        }
    }

    /// Get current config
    pub fn get_config(&self) -> &AdaptiveConcurrencyConfig {
        &self.config
    }

    /// Update config
    pub fn set_config(&mut self, config: AdaptiveConcurrencyConfig) {
        self.config = config;
    }

    /// Register a new task for adaptive concurrency tracking
    pub fn register_task(&mut self, task_id: &str) {
        if !self.states.contains_key(task_id) {
            self.states.insert(
                task_id.to_string(),
                ConcurrencyState {
                    current_connections: self.config.initial_connections,
                    samples: Vec::new(),
                    last_adjustment: None,
                    last_adjustment_direction: AdjustmentDirection::None,
                    total_adjustments: 0,
                    smoothed_rtt_ms: 0.0,
                    rtt_variance_ms: 0.0,
                    estimated_bandwidth_bps: 0.0,
                    domain: None,
                    min_rtt_ms: f64::MAX,
                    consecutive_increases: 0,
                    consecutive_decreases: 0,
                },
            );
        }
    }

    /// Register a task with domain association for per-domain limiting
    pub fn register_task_with_domain(&mut self, task_id: &str, domain: &str) {
        self.register_task(task_id);
        if let Some(state) = self.states.get_mut(task_id) {
            state.domain = Some(domain.to_string());
        }
        // Ensure domain state exists
        self.domain_states
            .entry(domain.to_string())
            .or_insert(DomainState {
                active_connections: 0,
                max_connections: self.default_domain_limit,
                last_throttle: None,
            });
    }

    /// Unregister a task
    pub fn unregister_task(&mut self, task_id: &str) {
        self.states.remove(task_id);
    }

    /// Record a response sample for a task
    pub fn record_sample(&mut self, task_id: &str, response_time_ms: f64, success: bool) {
        self.record_sample_with_bytes(task_id, response_time_ms, 0, success);
    }

    /// Record a response sample with bytes transferred for bandwidth estimation
    pub fn record_sample_with_bytes(
        &mut self,
        task_id: &str,
        response_time_ms: f64,
        bytes_transferred: u64,
        success: bool,
    ) {
        if let Some(state) = self.states.get_mut(task_id) {
            let sample = ResponseSample {
                timestamp: Instant::now(),
                response_time_ms,
                bytes_transferred,
                success,
            };
            // Compute throughput before moving sample into the vec
            let throughput = if bytes_transferred > 0 && response_time_ms > 0.0 {
                Some(sample.throughput_bps())
            } else {
                None
            };
            state.samples.push(sample);

            // Update EWMA smoothed RTT (like TCP RTT estimation)
            if success {
                if state.smoothed_rtt_ms == 0.0 {
                    // First sample - initialize directly
                    state.smoothed_rtt_ms = response_time_ms;
                    state.rtt_variance_ms = response_time_ms / 2.0;
                    state.min_rtt_ms = response_time_ms;
                } else {
                    // EWMA update: SRTT = (1 - alpha) * SRTT + alpha * RTT
                    let diff = (state.smoothed_rtt_ms - response_time_ms).abs();
                    state.smoothed_rtt_ms =
                        EWMA_ALPHA * response_time_ms + (1.0 - EWMA_ALPHA) * state.smoothed_rtt_ms;
                    // Variance update: RTTVAR = (1 - beta) * RTTVAR + beta * |SRTT - RTT|
                    state.rtt_variance_ms =
                        EWMA_BETA * diff + (1.0 - EWMA_BETA) * state.rtt_variance_ms;
                    // Track minimum RTT (baseline for BBR-style estimation)
                    state.min_rtt_ms = state.min_rtt_ms.min(response_time_ms);
                }

                // Update bandwidth estimate from throughput samples
                if let Some(throughput) = throughput {
                    // Use EWMA for bandwidth, but keep track of best recent throughput
                    if state.estimated_bandwidth_bps == 0.0 {
                        state.estimated_bandwidth_bps = throughput;
                    } else {
                        // Weighted update: take the better of current and new, with decay
                        state.estimated_bandwidth_bps =
                            0.9 * state.estimated_bandwidth_bps + 0.1 * throughput;
                    }
                }
            }

            // Keep only the most recent samples
            let max_samples = self.config.sample_window as usize * 2;
            if state.samples.len() > max_samples {
                state.samples.drain(..state.samples.len() - max_samples);
            }
        }
    }

    /// Get the current recommended connection count for a task
    pub fn get_connections(&self, task_id: &str) -> u32 {
        self.states
            .get(task_id)
            .map(|s| s.current_connections)
            .unwrap_or(self.config.initial_connections)
    }

    /// Evaluate and potentially adjust concurrency for a task
    pub fn evaluate(&mut self, task_id: &str) -> ConcurrencyDecision {
        if !self.config.enabled {
            return ConcurrencyDecision::Hold;
        }

        let state = match self.states.get(task_id) {
            Some(s) => s,
            None => return ConcurrencyDecision::Hold,
        };

        // Check cooldown
        if let Some(last) = state.last_adjustment
            && last.elapsed() < Duration::from_secs(self.config.adjustment_cooldown_secs)
        {
            return ConcurrencyDecision::Hold;
        }

        // Need enough samples
        let recent: Vec<&ResponseSample> = state
            .samples
            .iter()
            .rev()
            .take(self.config.sample_window as usize)
            .collect();

        if recent.len() < self.config.sample_window as usize {
            return ConcurrencyDecision::Hold;
        }

        // Calculate metrics
        let avg_response_ms: f64 =
            recent.iter().map(|s| s.response_time_ms).sum::<f64>() / recent.len() as f64;

        let error_count = recent.iter().filter(|s| !s.success).count();
        let error_rate = error_count as f64 / recent.len() as f64;

        let current = state.current_connections;

        // Get smoothed RTT and variance for better decisions
        let smoothed_rtt = state.smoothed_rtt_ms;
        let rtt_var = state.rtt_variance_ms;
        let min_rtt = state.min_rtt_ms;

        // Calculate BBR-inspired delivery rate threshold
        // If smoothed RTT is significantly above min RTT, we're queueing
        let queueing_ratio = if min_rtt > 0.0 {
            smoothed_rtt / min_rtt
        } else {
            1.0
        };

        // Decision logic with hysteresis
        // Priority 1: High error rate -> multiplicative decrease
        if error_rate > self.config.error_rate_threshold {
            let new_conn =
                self.clamp_connections((current as f64 * self.config.decrease_factor) as u32);
            if new_conn < current {
                return self.make_adjustment(
                    task_id,
                    current,
                    new_conn,
                    format!(
                        "High error rate ({:.1}%) exceeds threshold ({:.1}%)",
                        error_rate * 100.0,
                        self.config.error_rate_threshold * 100.0
                    ),
                );
            }
        }

        // Priority 2: Queueing detected (BBR-style) -> decrease
        // When RTT is 2x+ the minimum, we're adding latency without throughput gain
        if queueing_ratio > 2.0 && current > self.config.min_connections {
            let new_conn =
                self.clamp_connections((current as f64 * self.config.decrease_factor) as u32);
            if new_conn < current {
                return self.make_adjustment(
                    task_id,
                    current,
                    new_conn,
                    format!(
                        "Queueing detected (RTT {:.0}ms vs min {:.0}ms, ratio {:.1}x)",
                        smoothed_rtt, min_rtt, queueing_ratio
                    ),
                );
            }
        }

        // Priority 3: Very high latency -> decrease
        if avg_response_ms > self.config.high_latency_threshold_ms as f64 {
            let new_conn =
                self.clamp_connections((current as f64 * self.config.decrease_factor) as u32);
            if new_conn < current {
                return self.make_adjustment(
                    task_id,
                    current,
                    new_conn,
                    format!(
                        "High latency ({:.0}ms) exceeds threshold ({}ms)",
                        avg_response_ms, self.config.high_latency_threshold_ms
                    ),
                );
            }
        }

        // Priority 4: Good performance and below target -> increase
        // Use additive increase when close to target, multiplicative when well below
        if avg_response_ms < self.config.target_response_ms as f64
            && error_rate < self.config.error_rate_threshold * 0.5
            && queueing_ratio < 1.5
        {
            // Check if we're in slow-start phase (early connection count)
            let increase_factor = if state.consecutive_increases < 3 {
                // Slow start: exponential growth
                self.config.increase_factor * 1.2
            } else {
                // Congestion avoidance: linear growth
                1.0 + (self.config.increase_factor - 1.0) / current as f64
            };

            let new_conn = self.clamp_connections((current as f64 * increase_factor).ceil() as u32);
            if new_conn > current {
                return self.make_adjustment(
                    task_id,
                    current,
                    new_conn,
                    format!(
                        "Good performance ({:.0}ms avg, {:.1}% errors, RTT ratio {:.1}x) - scaling up",
                        avg_response_ms,
                        error_rate * 100.0,
                        queueing_ratio
                    ),
                );
            }
        }

        // Priority 5: RTT variance too high -> hold (connection unstable)
        if rtt_var > smoothed_rtt * 0.5 && smoothed_rtt > 0.0 {
            // High variance means unstable connection, don't increase
            return ConcurrencyDecision::Hold;
        }

        ConcurrencyDecision::Hold
    }

    /// Evaluate all tasks and return decisions
    pub fn evaluate_all(&mut self) -> Vec<(String, ConcurrencyDecision)> {
        let task_ids: Vec<String> = self.states.keys().cloned().collect();
        let mut results = Vec::new();
        for task_id in task_ids {
            let decision = self.evaluate(&task_id);
            if !matches!(decision, ConcurrencyDecision::Hold) {
                results.push((task_id, decision));
            }
        }
        results
    }

    /// Get summary for a single task
    pub fn get_task_summary(&self, task_id: &str) -> Option<TaskConcurrencySummary> {
        let state = self.states.get(task_id)?;
        let recent: Vec<&ResponseSample> = state
            .samples
            .iter()
            .rev()
            .take(self.config.sample_window as usize)
            .collect();

        let avg_response_ms = if recent.is_empty() {
            0.0
        } else {
            recent.iter().map(|s| s.response_time_ms).sum::<f64>() / recent.len() as f64
        };

        let error_rate = if recent.is_empty() {
            0.0
        } else {
            recent.iter().filter(|s| !s.success).count() as f64 / recent.len() as f64
        };

        Some(TaskConcurrencySummary {
            task_id: task_id.to_string(),
            current_connections: state.current_connections,
            avg_response_ms,
            error_rate,
            total_adjustments: state.total_adjustments,
            last_direction: state.last_adjustment_direction,
        })
    }

    /// Get overall summary
    pub fn get_summary(&self) -> AdaptiveConcurrencySummary {
        let tasks: Vec<TaskConcurrencySummary> = self
            .states
            .keys()
            .filter_map(|id| self.get_task_summary(id))
            .collect();

        let total_adjustments: u32 = self.states.values().map(|s| s.total_adjustments).sum();

        AdaptiveConcurrencySummary {
            enabled: self.config.enabled,
            config: self.config,
            task_count: self.states.len(),
            tasks,
            total_adjustments,
        }
    }

    /// Clear all state
    pub fn clear(&mut self) {
        self.states.clear();
    }

    /// Save config to JSON string
    pub fn save_config(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.config)
    }

    /// Load config from JSON string
    pub fn load_config(json: &str) -> Result<AdaptiveConcurrencyConfig, serde_json::Error> {
        serde_json::from_str(json)
    }

    // --- Private helpers ---

    fn clamp_connections(&self, value: u32) -> u32 {
        value.clamp(self.config.min_connections, self.config.max_connections)
    }

    /// Get the smoothed RTT estimate for a task
    pub fn get_smoothed_rtt(&self, task_id: &str) -> Option<f64> {
        self.states.get(task_id).and_then(|s: &ConcurrencyState| {
            if s.smoothed_rtt_ms > 0.0 {
                Some(s.smoothed_rtt_ms)
            } else {
                None
            }
        })
    }

    /// Get the estimated bandwidth for a task
    pub fn get_estimated_bandwidth(&self, task_id: &str) -> Option<f64> {
        self.states.get(task_id).and_then(|s: &ConcurrencyState| {
            if s.estimated_bandwidth_bps > 0.0 {
                Some(s.estimated_bandwidth_bps)
            } else {
                None
            }
        })
    }

    fn make_adjustment(
        &mut self,
        task_id: &str,
        from: u32,
        to: u32,
        reason: String,
    ) -> ConcurrencyDecision {
        if let Some(state) = self.states.get_mut(task_id) {
            state.current_connections = to;
            state.last_adjustment = Some(Instant::now());
            state.total_adjustments += 1;

            if to > from {
                state.last_adjustment_direction = AdjustmentDirection::Increased;
                state.consecutive_increases += 1;
                state.consecutive_decreases = 0;
            } else {
                state.last_adjustment_direction = AdjustmentDirection::Decreased;
                state.consecutive_decreases += 1;
                state.consecutive_increases = 0;
            }
        }

        if to > from {
            ConcurrencyDecision::Increase { from, to, reason }
        } else {
            ConcurrencyDecision::Decrease { from, to, reason }
        }
    }
}

impl Default for AdaptiveConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> AdaptiveConcurrencyConfig {
        AdaptiveConcurrencyConfig {
            enabled: true,
            min_connections: 1,
            max_connections: 16,
            initial_connections: 4,
            target_response_ms: 200,
            high_latency_threshold_ms: 1000,
            error_rate_threshold: 0.1,
            sample_window: 5,
            adjustment_cooldown_secs: 0, // no cooldown for tests
            increase_factor: 1.5,
            decrease_factor: 0.7,
        }
    }

    #[test]
    fn test_default_config() {
        let config = AdaptiveConcurrencyConfig::default();
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.max_connections, 16);
        assert_eq!(config.initial_connections, 4);
        assert!(config.enabled);
    }

    #[test]
    fn test_register_and_get_connections() {
        let mut mgr = AdaptiveConcurrencyManager::with_config(make_config());
        mgr.register_task("task1");
        assert_eq!(mgr.get_connections("task1"), 4); // initial
        assert_eq!(mgr.get_connections("unknown"), 4); // default
    }

    #[test]
    fn test_unregister_task() {
        let mut mgr = AdaptiveConcurrencyManager::with_config(make_config());
        mgr.register_task("task1");
        mgr.unregister_task("task1");
        assert_eq!(mgr.get_connections("task1"), 4); // back to default
    }

    #[test]
    fn test_hold_when_not_enough_samples() {
        let mut mgr = AdaptiveConcurrencyManager::with_config(make_config());
        mgr.register_task("task1");

        // Only 3 samples, need 5
        for i in 0..3 {
            mgr.record_sample("task1", 100.0 + i as f64, true);
        }

        let decision = mgr.evaluate("task1");
        assert_eq!(decision, ConcurrencyDecision::Hold);
    }

    #[test]
    fn test_increase_on_good_performance() {
        let config = make_config();
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        // 5 fast, successful samples
        for _ in 0..5 {
            mgr.record_sample("task1", 50.0, true); // well under 200ms target
        }

        let decision = mgr.evaluate("task1");
        match decision {
            ConcurrencyDecision::Increase { from, to, .. } => {
                assert_eq!(from, 4);
                assert_eq!(to, 6); // ceil(4 * 1.5)
            }
            _ => panic!("Expected Increase, got {:?}", decision),
        }
    }

    #[test]
    fn test_decrease_on_high_latency() {
        let config = make_config();
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        // 5 slow samples
        for _ in 0..5 {
            mgr.record_sample("task1", 2000.0, true); // well over 1000ms threshold
        }

        let decision = mgr.evaluate("task1");
        match decision {
            ConcurrencyDecision::Decrease { from, to, .. } => {
                assert_eq!(from, 4);
                assert_eq!(to, 2); // (4 * 0.7) = 2
            }
            _ => panic!("Expected Decrease, got {:?}", decision),
        }
    }

    #[test]
    fn test_decrease_on_high_error_rate() {
        let config = make_config();
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        // 5 samples with 40% error rate (threshold is 10%)
        mgr.record_sample("task1", 100.0, true);
        mgr.record_sample("task1", 100.0, true);
        mgr.record_sample("task1", 100.0, true);
        mgr.record_sample("task1", 100.0, false);
        mgr.record_sample("task1", 100.0, false);

        let decision = mgr.evaluate("task1");
        match decision {
            ConcurrencyDecision::Decrease { from, to, .. } => {
                assert_eq!(from, 4);
                assert_eq!(to, 2);
            }
            _ => panic!("Expected Decrease, got {:?}", decision),
        }
    }

    #[test]
    fn test_respect_min_connections() {
        let config = AdaptiveConcurrencyConfig {
            min_connections: 2,
            max_connections: 16,
            initial_connections: 2,
            decrease_factor: 0.5,
            sample_window: 3,
            adjustment_cooldown_secs: 0,
            ..make_config()
        };
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        // All failures
        for _ in 0..3 {
            mgr.record_sample("task1", 100.0, false);
        }

        let decision = mgr.evaluate("task1");
        match decision {
            ConcurrencyDecision::Decrease { to, .. } => {
                assert_eq!(to, 2); // can't go below min
            }
            _ => {
                // At min already, could be Hold
            }
        }
    }

    #[test]
    fn test_respect_max_connections() {
        let config = AdaptiveConcurrencyConfig {
            min_connections: 1,
            max_connections: 8,
            initial_connections: 7,
            increase_factor: 2.0,
            sample_window: 3,
            adjustment_cooldown_secs: 0,
            ..make_config()
        };
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        // All fast and successful
        for _ in 0..3 {
            mgr.record_sample("task1", 10.0, true);
        }

        let decision = mgr.evaluate("task1");
        match decision {
            ConcurrencyDecision::Increase { to, .. } => {
                assert_eq!(to, 8); // capped at max
            }
            _ => panic!("Expected Increase, got {:?}", decision),
        }
    }

    #[test]
    fn test_cooldown_prevents_rapid_adjustments() {
        let config = AdaptiveConcurrencyConfig {
            adjustment_cooldown_secs: 60,
            sample_window: 3,
            ..make_config()
        };
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        // First evaluation: fast samples -> increase
        for _ in 0..3 {
            mgr.record_sample("task1", 10.0, true);
        }
        let d1 = mgr.evaluate("task1");
        assert!(matches!(d1, ConcurrencyDecision::Increase { .. }));

        // Second evaluation immediately: should hold due to cooldown
        for _ in 0..3 {
            mgr.record_sample("task1", 10.0, true);
        }
        let d2 = mgr.evaluate("task1");
        assert_eq!(d2, ConcurrencyDecision::Hold);
    }

    #[test]
    fn test_disabled_returns_hold() {
        let config = AdaptiveConcurrencyConfig {
            enabled: false,
            ..make_config()
        };
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        for _ in 0..5 {
            mgr.record_sample("task1", 10.0, true);
        }

        let decision = mgr.evaluate("task1");
        assert_eq!(decision, ConcurrencyDecision::Hold);
    }

    #[test]
    fn test_evaluate_all() {
        let mut mgr = AdaptiveConcurrencyManager::with_config(make_config());
        mgr.register_task("fast");
        mgr.register_task("slow");

        // Fast task: should increase
        for _ in 0..5 {
            mgr.record_sample("fast", 50.0, true);
        }

        // Slow task: should decrease
        for _ in 0..5 {
            mgr.record_sample("slow", 2000.0, true);
        }

        let results = mgr.evaluate_all();
        assert_eq!(results.len(), 2);

        let fast_decision = results.iter().find(|(id, _)| id == "fast").unwrap();
        assert!(matches!(
            fast_decision.1,
            ConcurrencyDecision::Increase { .. }
        ));

        let slow_decision = results.iter().find(|(id, _)| id == "slow").unwrap();
        assert!(matches!(
            slow_decision.1,
            ConcurrencyDecision::Decrease { .. }
        ));
    }

    #[test]
    fn test_get_summary() {
        let mut mgr = AdaptiveConcurrencyManager::with_config(make_config());
        mgr.register_task("task1");

        for _ in 0..5 {
            mgr.record_sample("task1", 100.0, true);
        }
        mgr.evaluate("task1");

        let summary = mgr.get_summary();
        assert!(summary.enabled);
        assert_eq!(summary.task_count, 1);
        assert_eq!(summary.tasks.len(), 1);
        assert!(summary.total_adjustments > 0);
    }

    #[test]
    fn test_get_task_summary() {
        let mut mgr = AdaptiveConcurrencyManager::with_config(make_config());
        mgr.register_task("task1");

        for _ in 0..5 {
            mgr.record_sample("task1", 150.0, true);
        }

        let summary = mgr.get_task_summary("task1").unwrap();
        assert_eq!(summary.task_id, "task1");
        assert_eq!(summary.current_connections, 4);
        assert!((summary.avg_response_ms - 150.0).abs() < 1.0);
        assert_eq!(summary.error_rate, 0.0);
    }

    #[test]
    fn test_task_summary_unknown_task() {
        let mgr = AdaptiveConcurrencyManager::new();
        assert!(mgr.get_task_summary("unknown").is_none());
    }

    #[test]
    fn test_clear() {
        let mut mgr = AdaptiveConcurrencyManager::new();
        mgr.register_task("task1");
        mgr.register_task("task2");
        mgr.clear();
        assert_eq!(mgr.get_summary().task_count, 0);
    }

    #[test]
    fn test_save_load_config() {
        let mgr = AdaptiveConcurrencyManager::new();
        let json = mgr.save_config().unwrap();
        let loaded = AdaptiveConcurrencyManager::load_config(&json).unwrap();
        assert_eq!(loaded.enabled, true);
        assert_eq!(loaded.min_connections, 1);
        assert_eq!(loaded.max_connections, 16);
    }

    #[test]
    fn test_config_serialization() {
        let config = AdaptiveConcurrencyConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AdaptiveConcurrencyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.min_connections, config.min_connections);
        assert_eq!(deserialized.max_connections, config.max_connections);
    }

    #[test]
    fn test_sample_overflow_trims_old() {
        let config = AdaptiveConcurrencyConfig {
            sample_window: 3,
            ..make_config()
        };
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        // Record many samples
        for i in 0..20 {
            mgr.record_sample("task1", i as f64, true);
        }

        let state = mgr.states.get("task1").unwrap();
        // Should have trimmed to at most sample_window * 2 = 6
        assert!(state.samples.len() <= 6);
    }

    #[test]
    fn test_adjustment_direction_tracking() {
        let mut mgr = AdaptiveConcurrencyManager::with_config(make_config());
        mgr.register_task("task1");

        // Initially None
        let state = mgr.states.get("task1").unwrap();
        assert_eq!(state.last_adjustment_direction, AdjustmentDirection::None);

        // After increase
        for _ in 0..5 {
            mgr.record_sample("task1", 10.0, true);
        }
        mgr.evaluate("task1");

        let state = mgr.states.get("task1").unwrap();
        assert_eq!(
            state.last_adjustment_direction,
            AdjustmentDirection::Increased
        );
        assert_eq!(state.total_adjustments, 1);
    }

    #[test]
    fn test_unknown_task_evaluate_returns_hold() {
        let mut mgr = AdaptiveConcurrencyManager::new();
        let decision = mgr.evaluate("nonexistent");
        assert_eq!(decision, ConcurrencyDecision::Hold);
    }

    #[test]
    fn test_mixed_error_and_latency() {
        // Error rate takes priority over latency
        let config = make_config();
        let mut mgr = AdaptiveConcurrencyManager::with_config(config);
        mgr.register_task("task1");

        // Mix of high latency and errors - error rate should dominate
        mgr.record_sample("task1", 500.0, true);
        mgr.record_sample("task1", 500.0, true);
        mgr.record_sample("task1", 500.0, true);
        mgr.record_sample("task1", 500.0, false);
        mgr.record_sample("task1", 500.0, false);

        let decision = mgr.evaluate("task1");
        // 40% error rate > 10% threshold -> decrease
        assert!(matches!(decision, ConcurrencyDecision::Decrease { .. }));
    }
}
