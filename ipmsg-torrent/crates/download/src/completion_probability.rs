//! Download Completion Probability Estimator (Phase 162)
//!
//! Predicts the probability (0-100%) that a download task will complete successfully
//! by combining multiple signal sources:
//!
//! - Source reliability: domain success rate and tier
//! - Network status: connectivity and quality
//! - Historical success rate: per-protocol and overall
//! - Task state: progress, retries, errors, stall count
//! - Disk space: available vs required
//! - Error recovery: recent error frequency and category
//!
//! The estimator produces a weighted composite score and a confidence level.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Default weight for source reliability factor
const DEFAULT_WEIGHT_RELIABILITY: f64 = 0.25;
/// Default weight for network status factor
const DEFAULT_WEIGHT_NETWORK: f64 = 0.20;
/// Default weight for historical success rate factor
const DEFAULT_WEIGHT_HISTORY: f64 = 0.15;
/// Default weight for task state factor
const DEFAULT_WEIGHT_TASK_STATE: f64 = 0.20;
/// Default weight for disk space factor
const DEFAULT_WEIGHT_DISK: f64 = 0.10;
/// Default weight for error frequency factor
const DEFAULT_WEIGHT_ERROR: f64 = 0.10;

/// Confidence level for a probability estimate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfidenceLevel {
    /// Very few signals available, estimate is rough
    Low,
    /// Moderate signals, estimate is reasonable
    Medium,
    /// Strong signals from multiple sources, estimate is reliable
    High,
}

impl ConfidenceLevel {
    /// Numeric multiplier for weighting
    pub fn as_multiplier(&self) -> f64 {
        match self {
            ConfidenceLevel::Low => 0.7,
            ConfidenceLevel::Medium => 0.85,
            ConfidenceLevel::High => 1.0,
        }
    }
}

impl std::fmt::Display for ConfidenceLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfidenceLevel::Low => write!(f, "Low"),
            ConfidenceLevel::Medium => write!(f, "Medium"),
            ConfidenceLevel::High => write!(f, "High"),
        }
    }
}

/// Probability category for human-readable display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbabilityCategory {
    /// 0-20%: very unlikely to succeed
    VeryLow,
    /// 20-40%: unlikely but possible
    Low,
    /// 40-60%: uncertain
    Moderate,
    /// 60-80%: likely to succeed
    High,
    /// 80-100%: very likely to succeed
    VeryHigh,
}

impl ProbabilityCategory {
    /// Categorize from a probability value (0-100)
    pub fn from_probability(prob: f64) -> Self {
        if prob < 20.0 {
            Self::VeryLow
        } else if prob < 40.0 {
            Self::Low
        } else if prob < 60.0 {
            Self::Moderate
        } else if prob < 80.0 {
            Self::High
        } else {
            Self::VeryHigh
        }
    }

    /// Human-readable label
    pub fn label(&self) -> &'static str {
        match self {
            Self::VeryLow => "Very Low",
            Self::Low => "Low",
            Self::Moderate => "Moderate",
            Self::High => "High",
            Self::VeryHigh => "Very High",
        }
    }

    /// Emoji indicator
    pub fn emoji(&self) -> &'static str {
        match self {
            Self::VeryLow => "🔴",
            Self::Low => "🟠",
            Self::Moderate => "🟡",
            Self::High => "🟢",
            Self::VeryHigh => "✅",
        }
    }
}

impl std::fmt::Display for ProbabilityCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.emoji(), self.label())
    }
}

/// Input signals for a single task's probability estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProbabilityInput {
    /// Task identifier
    pub task_id: String,
    /// Source domain (for reliability lookup)
    pub source_domain: Option<String>,
    /// Protocol (http/torrent/ed2k/p2p)
    pub protocol: String,
    /// Current progress (0.0 - 1.0)
    pub progress: f64,
    /// Number of retries so far
    pub retry_count: u32,
    /// Number of errors encountered
    pub error_count: u32,
    /// Number of times the task has stalled (speed dropped to 0)
    pub stall_count: u32,
    /// Whether the task is currently paused
    pub is_paused: bool,
    /// File size in bytes (for disk space check)
    pub file_size_bytes: u64,
}

impl TaskProbabilityInput {
    /// Create a new task probability input with defaults
    pub fn new(task_id: String, protocol: String) -> Self {
        Self {
            task_id,
            source_domain: None,
            protocol,
            progress: 0.0,
            retry_count: 0,
            error_count: 0,
            stall_count: 0,
            is_paused: false,
            file_size_bytes: 0,
        }
    }
}

/// Individual factor score (0.0 - 1.0, where 1.0 = best)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct FactorScore {
    /// The raw score (0.0 - 1.0)
    pub score: f64,
    /// The weight applied to this factor
    pub weight: f64,
    /// Whether this factor had data available
    pub has_data: bool,
}

impl FactorScore {
    /// Weighted contribution to the final score
    pub fn weighted_score(&self) -> f64 {
        if !self.has_data {
            return 0.0;
        }
        self.score * self.weight
    }
}

/// The result of a completion probability estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionProbability {
    /// Task ID
    pub task_id: String,
    /// Overall probability (0.0 - 100.0)
    pub probability: f64,
    /// Confidence level
    pub confidence: ConfidenceLevel,
    /// Probability category
    pub category: ProbabilityCategory,
    /// When the estimate was made
    pub estimated_at: DateTime<Utc>,
    /// Individual factor scores
    pub factors: ProbabilityFactors,
    /// Human-readable summary
    pub summary: String,
}

impl CompletionProbability {
    /// Generate a human-readable summary
    pub fn generate_summary(&mut self) {
        self.category = ProbabilityCategory::from_probability(self.probability);
        self.summary = format!(
            "{} {:.1}% completion probability ({}) [confidence: {}]",
            self.category.emoji(),
            self.probability,
            self.category.label(),
            self.confidence,
        );
    }
}

/// All factor scores for a probability estimation
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProbabilityFactors {
    /// Source reliability factor
    pub source_reliability: FactorScore,
    /// Network connectivity factor
    pub network: FactorScore,
    /// Historical success rate factor
    pub history: FactorScore,
    /// Task state factor (progress, retries, stalls)
    pub task_state: FactorScore,
    /// Disk space factor
    pub disk_space: FactorScore,
    /// Error frequency factor
    pub error_frequency: FactorScore,
}

/// Configuration for the completion probability estimator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionProbabilityConfig {
    /// Whether estimation is enabled
    pub enabled: bool,
    /// Weight for source reliability (0.0 - 1.0)
    pub weight_reliability: f64,
    /// Weight for network status (0.0 - 1.0)
    pub weight_network: f64,
    /// Weight for historical success rate (0.0 - 1.0)
    pub weight_history: f64,
    /// Weight for task state (0.0 - 1.0)
    pub weight_task_state: f64,
    /// Weight for disk space (0.0 - 1.0)
    pub weight_disk: f64,
    /// Weight for error frequency (0.0 - 1.0)
    pub weight_error: f64,
    /// Minimum probability to recommend starting a task (0.0 - 100.0)
    pub min_recommended_probability: f64,
    /// Maximum number of estimates to cache
    pub max_cache_size: usize,
}

impl Default for CompletionProbabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            weight_reliability: DEFAULT_WEIGHT_RELIABILITY,
            weight_network: DEFAULT_WEIGHT_NETWORK,
            weight_history: DEFAULT_WEIGHT_HISTORY,
            weight_task_state: DEFAULT_WEIGHT_TASK_STATE,
            weight_disk: DEFAULT_WEIGHT_DISK,
            weight_error: DEFAULT_WEIGHT_ERROR,
            min_recommended_probability: 40.0,
            max_cache_size: 200,
        }
    }
}

impl CompletionProbabilityConfig {
    /// Validate that weights sum to approximately 1.0
    pub fn weights_sum(&self) -> f64 {
        self.weight_reliability
            + self.weight_network
            + self.weight_history
            + self.weight_task_state
            + self.weight_disk
            + self.weight_error
    }

    /// Check if weights are valid (sum to ~1.0 within tolerance)
    pub fn weights_valid(&self) -> bool {
        (self.weights_sum() - 1.0).abs() < 0.01
    }
}

/// External signal data fed into the estimator
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EstimatorSignals {
    /// Source reliability score for the domain (0.0 - 1.0)
    pub domain_reliability_score: Option<f64>,
    /// Whether the network is connected
    pub network_connected: Option<bool>,
    /// Network quality score (0.0 - 1.0)
    pub network_quality: Option<f64>,
    /// Overall historical success rate (0.0 - 1.0)
    pub overall_success_rate: Option<f64>,
    /// Per-protocol historical success rate (0.0 - 1.0)
    pub protocol_success_rate: Option<f64>,
    /// Available disk space in bytes
    pub available_disk_bytes: Option<u64>,
    /// Recent error count in the last hour
    pub recent_error_count: Option<u32>,
}

/// Summary of the estimator state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EstimatorSummary {
    /// Number of cached estimates
    pub cached_estimates: usize,
    /// Average probability across all cached estimates
    pub average_probability: f64,
    /// Number of tasks with high probability (>80%)
    pub high_probability_count: usize,
    /// Number of tasks with low probability (<40%)
    pub low_probability_count: usize,
    /// Number of tasks with moderate probability (40-80%)
    pub moderate_probability_count: usize,
    /// Tasks sorted by probability (ascending)
    pub tasks_by_probability: Vec<TaskProbabilityEntry>,
}

/// A task's probability entry for summary display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProbabilityEntry {
    /// Task ID
    pub task_id: String,
    /// Probability (0-100)
    pub probability: f64,
    /// Category
    pub category: ProbabilityCategory,
    /// Confidence
    pub confidence: ConfidenceLevel,
}

/// Errors from completion probability operations
#[derive(Debug, thiserror::Error)]
pub enum CompletionProbabilityError {
    /// I/O error during persistence
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Serialization error
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

/// The main estimator that computes completion probabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionProbabilityEstimator {
    /// Configuration
    config: CompletionProbabilityConfig,
    /// Cached estimates (task_id -> probability)
    cache: HashMap<String, CompletionProbability>,
}

impl CompletionProbabilityEstimator {
    /// Create a new estimator with default config
    pub fn new() -> Self {
        Self {
            config: CompletionProbabilityConfig::default(),
            cache: HashMap::new(),
        }
    }

    /// Create a new estimator with custom config
    pub fn with_config(config: CompletionProbabilityConfig) -> Self {
        Self {
            config,
            cache: HashMap::new(),
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> &CompletionProbabilityConfig {
        &self.config
    }

    /// Update the configuration
    pub fn set_config(&mut self, config: CompletionProbabilityConfig) {
        self.config = config;
    }

    /// Estimate the completion probability for a task
    pub fn estimate(
        &mut self,
        input: &TaskProbabilityInput,
        signals: &EstimatorSignals,
    ) -> CompletionProbability {
        let factors = self.compute_factors(input, signals);
        let (probability, confidence) = self.compute_probability(&factors);

        let mut result = CompletionProbability {
            task_id: input.task_id.clone(),
            probability,
            confidence,
            category: ProbabilityCategory::from_probability(probability),
            estimated_at: Utc::now(),
            factors,
            summary: String::new(),
        };
        result.generate_summary();

        // Cache the result
        if self.cache.len() >= self.config.max_cache_size {
            // Remove oldest entry
            if let Some(oldest_key) = self
                .cache
                .iter()
                .min_by_key(|(_, v)| v.estimated_at)
                .map(|(k, _)| k.clone())
            {
                self.cache.remove(&oldest_key);
            }
        }
        self.cache.insert(input.task_id.clone(), result.clone());

        result
    }

    /// Compute individual factor scores
    fn compute_factors(
        &self,
        input: &TaskProbabilityInput,
        signals: &EstimatorSignals,
    ) -> ProbabilityFactors {
        ProbabilityFactors {
            source_reliability: self.compute_reliability_factor(signals, &self.config),
            network: self.compute_network_factor(signals, &self.config),
            history: self.compute_history_factor(signals, &self.config),
            task_state: self.compute_task_state_factor(input, &self.config),
            disk_space: self.compute_disk_space_factor(input, signals, &self.config),
            error_frequency: self.compute_error_factor(input, signals, &self.config),
        }
    }

    /// Source reliability factor: higher domain reliability → higher score
    fn compute_reliability_factor(
        &self,
        signals: &EstimatorSignals,
        config: &CompletionProbabilityConfig,
    ) -> FactorScore {
        match signals.domain_reliability_score {
            Some(score) => FactorScore {
                score: score.clamp(0.0, 1.0),
                weight: config.weight_reliability,
                has_data: true,
            },
            None => FactorScore {
                score: 0.5, // neutral default
                weight: config.weight_reliability,
                has_data: false,
            },
        }
    }

    /// Network factor: disconnected → 0, connected with quality → quality score
    fn compute_network_factor(
        &self,
        signals: &EstimatorSignals,
        config: &CompletionProbabilityConfig,
    ) -> FactorScore {
        match (signals.network_connected, signals.network_quality) {
            (Some(false), _) => FactorScore {
                score: 0.0,
                weight: config.weight_network,
                has_data: true,
            },
            (Some(true), Some(quality)) => FactorScore {
                score: quality.clamp(0.0, 1.0),
                weight: config.weight_network,
                has_data: true,
            },
            (Some(true), None) => FactorScore {
                score: 0.7, // assume decent if connected but no quality data
                weight: config.weight_network,
                has_data: true,
            },
            _ => FactorScore {
                score: 0.5,
                weight: config.weight_network,
                has_data: false,
            },
        }
    }

    /// History factor: based on overall and protocol-specific success rates
    fn compute_history_factor(
        &self,
        signals: &EstimatorSignals,
        config: &CompletionProbabilityConfig,
    ) -> FactorScore {
        // Blend overall and protocol-specific rates
        let score = match (signals.overall_success_rate, signals.protocol_success_rate) {
            (Some(overall), Some(protocol)) => overall * 0.4 + protocol * 0.6,
            (Some(overall), None) => overall,
            (None, Some(protocol)) => protocol,
            (None, None) => 0.5,
        };

        let has_data =
            signals.overall_success_rate.is_some() || signals.protocol_success_rate.is_some();

        FactorScore {
            score: score.clamp(0.0, 1.0),
            weight: config.weight_history,
            has_data,
        }
    }

    /// Task state factor: considers progress, retries, stalls, and paused state
    fn compute_task_state_factor(
        &self,
        input: &TaskProbabilityInput,
        config: &CompletionProbabilityConfig,
    ) -> FactorScore {
        // Start with progress as base (already-completed work is likely to stay completed)
        let base = input.progress;

        // Penalize for retries (each retry reduces confidence)
        let retry_penalty = (input.retry_count as f64 * 0.05).min(0.3);

        // Penalize for stalls
        let stall_penalty = (input.stall_count as f64 * 0.03).min(0.2);

        // Penalize if paused
        let pause_penalty = if input.is_paused { 0.1 } else { 0.0 };

        let score = (base - retry_penalty - stall_penalty - pause_penalty).clamp(0.0, 1.0);

        FactorScore {
            score,
            weight: config.weight_task_state,
            has_data: true,
        }
    }

    /// Disk space factor: insufficient space → 0, ample space → 1.0
    fn compute_disk_space_factor(
        &self,
        input: &TaskProbabilityInput,
        signals: &EstimatorSignals,
        config: &CompletionProbabilityConfig,
    ) -> FactorScore {
        match signals.available_disk_bytes {
            Some(available) => {
                if input.file_size_bytes == 0 {
                    // Unknown file size, assume OK
                    FactorScore {
                        score: 0.8,
                        weight: config.weight_disk,
                        has_data: false,
                    }
                } else if available >= input.file_size_bytes {
                    // Enough space; score based on headroom ratio
                    let ratio = available as f64 / input.file_size_bytes as f64;
                    let score = if ratio >= 3.0 {
                        1.0
                    } else if ratio >= 2.0 {
                        0.95
                    } else if ratio >= 1.5 {
                        0.85
                    } else {
                        0.7
                    };
                    FactorScore {
                        score,
                        weight: config.weight_disk,
                        has_data: true,
                    }
                } else {
                    // Not enough space
                    FactorScore {
                        score: 0.0,
                        weight: config.weight_disk,
                        has_data: true,
                    }
                }
            }
            None => FactorScore {
                score: 0.5,
                weight: config.weight_disk,
                has_data: false,
            },
        }
    }

    /// Error frequency factor: recent errors reduce probability
    fn compute_error_factor(
        &self,
        input: &TaskProbabilityInput,
        signals: &EstimatorSignals,
        config: &CompletionProbabilityConfig,
    ) -> FactorScore {
        let recent_errors = signals.recent_error_count.unwrap_or(0);
        let total_errors = input.error_count;

        // Combine recent errors (last hour) with task-level error count
        let penalty = (recent_errors as f64 * 0.08 + total_errors as f64 * 0.05).min(1.0);
        let score = (1.0 - penalty).clamp(0.0, 1.0);

        // Always has data: zero errors is meaningful information
        FactorScore {
            score,
            weight: config.weight_error,
            has_data: true,
        }
    }

    /// Compute the final probability from factor scores
    fn compute_probability(&self, factors: &ProbabilityFactors) -> (f64, ConfidenceLevel) {
        let weighted_sum = factors.source_reliability.weighted_score()
            + factors.network.weighted_score()
            + factors.history.weighted_score()
            + factors.task_state.weighted_score()
            + factors.disk_space.weighted_score()
            + factors.error_frequency.weighted_score();

        // Count how many factors have data
        let data_count = [
            factors.source_reliability.has_data,
            factors.network.has_data,
            factors.history.has_data,
            factors.task_state.has_data,
            factors.disk_space.has_data,
            factors.error_frequency.has_data,
        ]
        .iter()
        .filter(|&&d| d)
        .count();

        // Compute confidence based on data availability
        let confidence = if data_count >= 5 {
            ConfidenceLevel::High
        } else if data_count >= 3 {
            ConfidenceLevel::Medium
        } else {
            ConfidenceLevel::Low
        };

        // Apply confidence multiplier: low confidence pulls toward 50%
        let raw = weighted_sum * 100.0;
        let adjusted = 50.0 + (raw - 50.0) * confidence.as_multiplier();

        (adjusted.clamp(0.0, 100.0), confidence)
    }

    /// Get a cached estimate for a task
    pub fn get_cached(&self, task_id: &str) -> Option<&CompletionProbability> {
        self.cache.get(task_id)
    }

    /// Remove a cached estimate
    pub fn remove_cached(&mut self, task_id: &str) -> bool {
        self.cache.remove(task_id).is_some()
    }

    /// Clear all cached estimates
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Get summary of all cached estimates
    pub fn summary(&self) -> EstimatorSummary {
        let mut entries: Vec<TaskProbabilityEntry> = self
            .cache
            .values()
            .map(|p| TaskProbabilityEntry {
                task_id: p.task_id.clone(),
                probability: p.probability,
                category: p.category,
                confidence: p.confidence,
            })
            .collect();

        entries.sort_by(|a, b| a.probability.partial_cmp(&b.probability).unwrap());

        let avg = if entries.is_empty() {
            0.0
        } else {
            entries.iter().map(|e| e.probability).sum::<f64>() / entries.len() as f64
        };

        EstimatorSummary {
            cached_estimates: entries.len(),
            average_probability: avg,
            high_probability_count: entries.iter().filter(|e| e.probability >= 80.0).count(),
            low_probability_count: entries.iter().filter(|e| e.probability < 40.0).count(),
            moderate_probability_count: entries
                .iter()
                .filter(|e| e.probability >= 40.0 && e.probability < 80.0)
                .count(),
            tasks_by_probability: entries,
        }
    }

    /// Format a human-readable report for a single task
    pub fn format_report(probability: &CompletionProbability) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "Completion Probability for task '{}': {}",
            probability.task_id, probability.summary
        ));
        lines.push(String::new());
        lines.push("Factor Breakdown:".to_string());

        let factors = &probability.factors;
        Self::format_factor(
            &mut lines,
            "Source Reliability",
            &factors.source_reliability,
        );
        Self::format_factor(&mut lines, "Network", &factors.network);
        Self::format_factor(&mut lines, "History", &factors.history);
        Self::format_factor(&mut lines, "Task State", &factors.task_state);
        Self::format_factor(&mut lines, "Disk Space", &factors.disk_space);
        Self::format_factor(&mut lines, "Error Frequency", &factors.error_frequency);

        lines.push(String::new());
        lines.push(format!(
            "Confidence: {} ({})",
            probability.confidence,
            match probability.confidence {
                ConfidenceLevel::High => "strong signals from multiple sources",
                ConfidenceLevel::Medium => "moderate data available",
                ConfidenceLevel::Low => "limited data, estimate may be inaccurate",
            }
        ));

        lines.join("\n")
    }

    fn format_factor(lines: &mut Vec<String>, name: &str, factor: &FactorScore) {
        let data_indicator = if factor.has_data { "📊" } else { "⚪" };
        lines.push(format!(
            "  {} {:20}: {:.0}% (weight: {:.0}%, data: {})",
            data_indicator,
            name,
            factor.score * 100.0,
            factor.weight * 100.0,
            if factor.has_data { "yes" } else { "no" },
        ));
    }

    /// Save configuration to disk
    pub async fn save_config(&self, path: &Path) -> Result<(), CompletionProbabilityError> {
        let json = serde_json::to_string_pretty(&self.config)?;
        fs::write(path, json).await?;
        Ok(())
    }

    /// Load configuration from disk
    pub async fn load_config(
        path: &Path,
    ) -> Result<CompletionProbabilityConfig, CompletionProbabilityError> {
        let json = fs::read_to_string(path).await?;
        let config: CompletionProbabilityConfig = serde_json::from_str(&json)?;
        Ok(config)
    }
}

impl Default for CompletionProbabilityEstimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_input(task_id: &str) -> TaskProbabilityInput {
        TaskProbabilityInput::new(task_id.to_string(), "http".to_string())
    }

    fn make_signals() -> EstimatorSignals {
        EstimatorSignals {
            domain_reliability_score: Some(0.85),
            network_connected: Some(true),
            network_quality: Some(0.9),
            overall_success_rate: Some(0.75),
            protocol_success_rate: Some(0.80),
            available_disk_bytes: Some(10_000_000_000),
            recent_error_count: Some(0),
        }
    }

    #[test]
    fn test_confidence_level_display() {
        assert_eq!(ConfidenceLevel::Low.to_string(), "Low");
        assert_eq!(ConfidenceLevel::Medium.to_string(), "Medium");
        assert_eq!(ConfidenceLevel::High.to_string(), "High");
    }

    #[test]
    fn test_confidence_level_multiplier() {
        assert_eq!(ConfidenceLevel::Low.as_multiplier(), 0.7);
        assert_eq!(ConfidenceLevel::Medium.as_multiplier(), 0.85);
        assert_eq!(ConfidenceLevel::High.as_multiplier(), 1.0);
    }

    #[test]
    fn test_probability_category_from_value() {
        assert_eq!(
            ProbabilityCategory::from_probability(10.0),
            ProbabilityCategory::VeryLow
        );
        assert_eq!(
            ProbabilityCategory::from_probability(30.0),
            ProbabilityCategory::Low
        );
        assert_eq!(
            ProbabilityCategory::from_probability(50.0),
            ProbabilityCategory::Moderate
        );
        assert_eq!(
            ProbabilityCategory::from_probability(70.0),
            ProbabilityCategory::High
        );
        assert_eq!(
            ProbabilityCategory::from_probability(90.0),
            ProbabilityCategory::VeryHigh
        );
    }

    #[test]
    fn test_probability_category_emoji() {
        assert_eq!(ProbabilityCategory::VeryLow.emoji(), "🔴");
        assert_eq!(ProbabilityCategory::Low.emoji(), "🟠");
        assert_eq!(ProbabilityCategory::Moderate.emoji(), "🟡");
        assert_eq!(ProbabilityCategory::High.emoji(), "🟢");
        assert_eq!(ProbabilityCategory::VeryHigh.emoji(), "✅");
    }

    #[test]
    fn test_config_default_weights_valid() {
        let config = CompletionProbabilityConfig::default();
        assert!(config.weights_valid());
        assert!((config.weights_sum() - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_config_invalid_weights() {
        let config = CompletionProbabilityConfig {
            weight_reliability: 0.5,
            weight_network: 0.5,
            weight_history: 0.5,
            weight_task_state: 0.5,
            weight_disk: 0.5,
            weight_error: 0.5,
            ..Default::default()
        };
        assert!(!config.weights_valid());
    }

    #[test]
    fn test_factor_score_weighted() {
        let factor = FactorScore {
            score: 0.8,
            weight: 0.25,
            has_data: true,
        };
        assert!((factor.weighted_score() - 0.2).abs() < 0.001);
    }

    #[test]
    fn test_factor_score_no_data() {
        let factor = FactorScore {
            score: 0.8,
            weight: 0.25,
            has_data: false,
        };
        assert_eq!(factor.weighted_score(), 0.0);
    }

    #[test]
    fn test_estimate_high_probability() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let mut input = make_input("task-1");
        input.progress = 0.5;
        input.retry_count = 0;
        input.error_count = 0;
        input.stall_count = 0;

        let signals = make_signals();
        let result = estimator.estimate(&input, &signals);

        // With good signals and good task state, probability should be reasonably high
        assert!(
            result.probability > 50.0,
            "expected >50%, got {:.1}%",
            result.probability
        );
        assert_eq!(result.confidence, ConfidenceLevel::High);
    }

    #[test]
    fn test_estimate_low_probability_disconnected() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let input = make_input("task-2");

        let signals = EstimatorSignals {
            domain_reliability_score: Some(0.2),
            network_connected: Some(false),
            network_quality: Some(0.0),
            overall_success_rate: Some(0.3),
            protocol_success_rate: Some(0.25),
            available_disk_bytes: Some(10_000_000_000),
            recent_error_count: Some(10),
        };

        let result = estimator.estimate(&input, &signals);
        assert!(
            result.probability < 40.0,
            "expected <40%, got {:.1}%",
            result.probability
        );
    }

    #[test]
    fn test_estimate_insufficient_disk() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let mut input = make_input("task-3");
        input.file_size_bytes = 50_000_000_000; // 50 GB

        let signals = EstimatorSignals {
            domain_reliability_score: Some(0.9),
            network_connected: Some(true),
            network_quality: Some(0.9),
            overall_success_rate: Some(0.8),
            protocol_success_rate: Some(0.8),
            available_disk_bytes: Some(1_000_000_000), // only 1 GB
            recent_error_count: Some(0),
        };

        let result = estimator.estimate(&input, &signals);
        // Disk factor should be 0, pulling probability down
        assert_eq!(result.factors.disk_space.score, 0.0);
    }

    #[test]
    fn test_estimate_no_signals() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let input = make_input("task-4");
        let signals = EstimatorSignals::default();

        let result = estimator.estimate(&input, &signals);
        // With minimal data, probability should be moderate
        assert!(
            result.probability >= 20.0 && result.probability <= 80.0,
            "expected moderate probability, got {:.1}%",
            result.probability
        );
        // Should have low confidence due to limited data
        assert!(
            result.confidence == ConfidenceLevel::Low
                || result.confidence == ConfidenceLevel::Medium
        );
    }

    #[test]
    fn test_estimate_retry_penalty() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let mut input = make_input("task-5");
        input.progress = 0.5;
        input.retry_count = 5;
        input.stall_count = 3;
        input.is_paused = true;

        let signals = make_signals();
        let result = estimator.estimate(&input, &signals);

        // Task state factor should be penalized
        assert!(
            result.factors.task_state.score < 0.5,
            "task state score should be low, got {:.2}",
            result.factors.task_state.score
        );
    }

    #[test]
    fn test_estimate_error_penalty() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let mut input = make_input("task-6");
        input.error_count = 10;

        let signals = EstimatorSignals {
            recent_error_count: Some(5),
            ..make_signals()
        };

        let result = estimator.estimate(&input, &signals);
        assert!(
            result.factors.error_frequency.score < 0.5,
            "error factor should be low, got {:.2}",
            result.factors.error_frequency.score
        );
    }

    #[test]
    fn test_cache_operations() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let input = make_input("task-cache");
        let signals = make_signals();

        estimator.estimate(&input, &signals);
        assert!(estimator.get_cached("task-cache").is_some());

        estimator.remove_cached("task-cache");
        assert!(estimator.get_cached("task-cache").is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let mut estimator = CompletionProbabilityEstimator::new();
        estimator.config.max_cache_size = 3;
        let signals = make_signals();

        for i in 0..5 {
            let input = make_input(&format!("task-{}", i));
            estimator.estimate(&input, &signals);
        }

        // Should have at most 3 entries
        assert!(estimator.cache.len() <= 3);
    }

    #[test]
    fn test_clear_cache() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let signals = make_signals();

        for i in 0..3 {
            let input = make_input(&format!("task-{}", i));
            estimator.estimate(&input, &signals);
        }

        estimator.clear_cache();
        assert_eq!(estimator.cache.len(), 0);
    }

    #[test]
    fn test_summary() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let signals = make_signals();

        // Add a high-probability task
        let mut input1 = make_input("task-high");
        input1.progress = 0.9;
        input1.file_size_bytes = 1_000_000; // 1MB, so disk_space has data
        estimator.estimate(&input1, &signals);

        // Add a low-probability task
        let mut input2 = make_input("task-low");
        input2.file_size_bytes = 1_000_000;
        let bad_signals = EstimatorSignals {
            network_connected: Some(false),
            domain_reliability_score: Some(0.1),
            ..EstimatorSignals::default()
        };
        estimator.estimate(&input2, &bad_signals);

        let summary = estimator.summary();
        assert_eq!(summary.cached_estimates, 2);
        assert!(
            summary.high_probability_count >= 1,
            "expected at least 1 high, got {} (probs: {:?})",
            summary.high_probability_count,
            summary
                .tasks_by_probability
                .iter()
                .map(|t| (t.task_id.as_str(), t.probability))
                .collect::<Vec<_>>()
        );
        assert!(summary.low_probability_count >= 1);
    }

    #[test]
    fn test_format_report() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let input = make_input("task-report");
        let signals = make_signals();
        let result = estimator.estimate(&input, &signals);

        let report = CompletionProbabilityEstimator::format_report(&result);
        assert!(report.contains("task-report"));
        assert!(report.contains("Factor Breakdown"));
        assert!(report.contains("Source Reliability"));
        assert!(report.contains("Network"));
        assert!(report.contains("Confidence"));
    }

    #[test]
    fn test_disk_space_scoring() {
        let estimator = CompletionProbabilityEstimator::new();
        let config = CompletionProbabilityConfig::default();

        // Plenty of space (3x)
        let mut input = make_input("t1");
        input.file_size_bytes = 1_000_000;
        let signals = EstimatorSignals {
            available_disk_bytes: Some(3_000_000),
            ..Default::default()
        };
        let factor = estimator.compute_disk_space_factor(&input, &signals, &config);
        assert_eq!(factor.score, 1.0);

        // Tight space (1.2x)
        let signals2 = EstimatorSignals {
            available_disk_bytes: Some(1_200_000),
            ..Default::default()
        };
        let factor2 = estimator.compute_disk_space_factor(&input, &signals2, &config);
        assert_eq!(factor2.score, 0.7);

        // Not enough space
        let signals3 = EstimatorSignals {
            available_disk_bytes: Some(500_000),
            ..Default::default()
        };
        let factor3 = estimator.compute_disk_space_factor(&input, &signals3, &config);
        assert_eq!(factor3.score, 0.0);
    }

    #[test]
    fn test_network_factor_disconnected() {
        let estimator = CompletionProbabilityEstimator::new();
        let config = CompletionProbabilityConfig::default();

        let signals = EstimatorSignals {
            network_connected: Some(false),
            ..Default::default()
        };
        let factor = estimator.compute_network_factor(&signals, &config);
        assert_eq!(factor.score, 0.0);
        assert!(factor.has_data);
    }

    #[test]
    fn test_network_factor_connected_no_quality() {
        let estimator = CompletionProbabilityEstimator::new();
        let config = CompletionProbabilityConfig::default();

        let signals = EstimatorSignals {
            network_connected: Some(true),
            ..Default::default()
        };
        let factor = estimator.compute_network_factor(&signals, &config);
        assert_eq!(factor.score, 0.7);
        assert!(factor.has_data);
    }

    #[test]
    fn test_history_factor_blend() {
        let estimator = CompletionProbabilityEstimator::new();
        let config = CompletionProbabilityConfig::default();

        let signals = EstimatorSignals {
            overall_success_rate: Some(0.6),
            protocol_success_rate: Some(0.9),
            ..Default::default()
        };
        let factor = estimator.compute_history_factor(&signals, &config);
        // 0.6 * 0.4 + 0.9 * 0.6 = 0.24 + 0.54 = 0.78
        assert!((factor.score - 0.78).abs() < 0.01);
    }

    #[test]
    fn test_task_state_factor_progress_only() {
        let estimator = CompletionProbabilityEstimator::new();
        let config = CompletionProbabilityConfig::default();

        let mut input = make_input("t");
        input.progress = 0.7;
        let factor = estimator.compute_task_state_factor(&input, &config);
        assert!((factor.score - 0.7).abs() < 0.01);
    }

    #[test]
    fn test_probability_boundaries() {
        let mut estimator = CompletionProbabilityEstimator::new();

        // Perfect scenario
        let mut input = make_input("perfect");
        input.progress = 1.0;
        let perfect_signals = EstimatorSignals {
            domain_reliability_score: Some(1.0),
            network_connected: Some(true),
            network_quality: Some(1.0),
            overall_success_rate: Some(1.0),
            protocol_success_rate: Some(1.0),
            available_disk_bytes: Some(100_000_000_000),
            recent_error_count: Some(0),
        };
        let result = estimator.estimate(&input, &perfect_signals);
        assert!(
            result.probability >= 90.0,
            "perfect scenario should be >=90%, got {:.1}%",
            result.probability
        );

        // Worst scenario
        let mut input2 = make_input("worst");
        input2.progress = 0.0;
        input2.retry_count = 20;
        input2.error_count = 20;
        input2.stall_count = 10;
        input2.is_paused = true;
        input2.file_size_bytes = 100_000_000_000;
        let worst_signals = EstimatorSignals {
            domain_reliability_score: Some(0.0),
            network_connected: Some(false),
            network_quality: Some(0.0),
            overall_success_rate: Some(0.0),
            protocol_success_rate: Some(0.0),
            available_disk_bytes: Some(0),
            recent_error_count: Some(20),
        };
        let result2 = estimator.estimate(&input2, &worst_signals);
        assert!(
            result2.probability <= 10.0,
            "worst scenario should be <=10%, got {:.1}%",
            result2.probability
        );
    }

    #[test]
    fn test_summary_sorted() {
        let mut estimator = CompletionProbabilityEstimator::new();
        let signals = make_signals();

        let mut input_a = make_input("a-high");
        input_a.progress = 0.9;
        estimator.estimate(&input_a, &signals);

        let mut input_b = make_input("b-low");
        input_b.progress = 0.1;
        input_b.retry_count = 10;
        estimator.estimate(&input_b, &signals);

        let summary = estimator.summary();
        assert_eq!(summary.tasks_by_probability.len(), 2);
        // Should be sorted ascending
        assert!(
            summary.tasks_by_probability[0].probability
                <= summary.tasks_by_probability[1].probability
        );
    }
}
