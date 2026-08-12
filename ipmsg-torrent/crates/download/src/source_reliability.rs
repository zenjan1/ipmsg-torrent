//! Download Source Reliability Tracker
//!
//! Tracks per-domain download reliability metrics (success rate, average speed,
//! failure count) and uses scores to inform source selection.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Reliability score thresholds
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ReliabilityTier {
    /// Score >= 0.8: highly reliable source
    Excellent,
    /// Score >= 0.6: generally reliable
    Good,
    /// Score >= 0.4: moderate reliability
    Fair,
    /// Score >= 0.2: low reliability, prefer alternatives
    Poor,
    /// Score < 0.2: avoid if possible
    Unreliable,
}

impl std::fmt::Display for ReliabilityTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReliabilityTier::Excellent => write!(f, "Excellent"),
            ReliabilityTier::Good => write!(f, "Good"),
            ReliabilityTier::Fair => write!(f, "Fair"),
            ReliabilityTier::Poor => write!(f, "Poor"),
            ReliabilityTier::Unreliable => write!(f, "Unreliable"),
        }
    }
}

/// Configuration for source reliability tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceReliabilityConfig {
    /// Enable reliability tracking
    pub enabled: bool,
    /// Number of recent samples to keep per domain (default: 100)
    pub max_samples_per_domain: usize,
    /// Minimum attempts before computing reliability (default: 3)
    pub min_attempts: u32,
    /// Decay factor for older samples (0.0-1.0, default: 0.95)
    /// Lower = older samples fade faster
    pub decay_factor: f64,
    /// Speed weight in reliability score (default: 0.3)
    pub speed_weight: f64,
    /// Success rate weight in reliability score (default: 0.5)
    pub success_weight: f64,
    /// Failure penalty weight (default: 0.2)
    pub failure_weight: f64,
    /// Domains to ignore (e.g., internal CDNs)
    pub ignored_domains: Vec<String>,
}

impl Default for SourceReliabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_samples_per_domain: 100,
            min_attempts: 3,
            decay_factor: 0.95,
            speed_weight: 0.3,
            success_weight: 0.5,
            failure_weight: 0.2,
            ignored_domains: Vec::new(),
        }
    }
}

/// A single download sample from a domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSample {
    /// Timestamp (unix epoch seconds)
    pub timestamp: u64,
    /// Download speed in bytes/sec (0 if failed)
    pub speed_bps: u64,
    /// Whether the download succeeded
    pub success: bool,
    /// File size in bytes (0 if unknown)
    pub file_size: u64,
    /// Error message if failed
    pub error: Option<String>,
}

/// Reliability metrics for a single domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainReliability {
    /// Domain name (e.g., "example.com")
    pub domain: String,
    /// Recent download samples
    pub samples: Vec<DownloadSample>,
    /// Total successful downloads (all time)
    pub total_successes: u32,
    /// Total failed downloads (all time)
    pub total_failures: u32,
    /// Average download speed (bytes/sec, EMA)
    pub avg_speed_bps: f64,
    /// Peak download speed (bytes/sec)
    pub peak_speed_bps: u64,
    /// Last download timestamp
    pub last_download_at: u64,
    /// Computed reliability score (0.0-1.0)
    pub reliability_score: f64,
    /// Number of consecutive failures (for circuit breaker)
    pub consecutive_failures: u32,
}

impl DomainReliability {
    fn new(domain: String) -> Self {
        Self {
            domain,
            samples: Vec::new(),
            total_successes: 0,
            total_failures: 0,
            avg_speed_bps: 0.0,
            peak_speed_bps: 0,
            last_download_at: 0,
            reliability_score: 0.5, // neutral starting point
            consecutive_failures: 0,
        }
    }

    /// Get the reliability tier based on current score
    pub fn tier(&self) -> ReliabilityTier {
        match self.reliability_score {
            s if s >= 0.8 => ReliabilityTier::Excellent,
            s if s >= 0.6 => ReliabilityTier::Good,
            s if s >= 0.4 => ReliabilityTier::Fair,
            s if s >= 0.2 => ReliabilityTier::Poor,
            _ => ReliabilityTier::Unreliable,
        }
    }

    /// Get success rate as a percentage (0.0-1.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.total_successes + self.total_failures;
        if total == 0 {
            0.5 // neutral
        } else {
            self.total_successes as f64 / total as f64
        }
    }
}

/// Summary of source reliability across all tracked domains
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReliabilitySummary {
    /// Total domains tracked
    pub total_domains: usize,
    /// Domains by tier
    pub excellent_count: usize,
    pub good_count: usize,
    pub fair_count: usize,
    pub poor_count: usize,
    pub unreliable_count: usize,
    /// Top 5 most reliable domains
    pub top_domains: Vec<(String, f64)>,
    /// Bottom 5 least reliable domains
    pub bottom_domains: Vec<(String, f64)>,
    /// Average reliability across all domains
    pub avg_reliability: f64,
    /// Total samples recorded
    pub total_samples: usize,
}

/// Manages source reliability tracking and scoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceReliabilityTracker {
    /// Configuration
    pub config: SourceReliabilityConfig,
    /// Per-domain reliability data
    pub domains: HashMap<String, DomainReliability>,
}

impl Default for SourceReliabilityTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceReliabilityTracker {
    /// Create a new tracker with default configuration
    pub fn new() -> Self {
        Self {
            config: SourceReliabilityConfig::default(),
            domains: HashMap::new(),
        }
    }

    /// Create a new tracker with custom configuration
    pub fn with_config(config: SourceReliabilityConfig) -> Self {
        Self {
            config,
            domains: HashMap::new(),
        }
    }

    /// Record a successful download from a domain
    pub fn record_success(&mut self, domain: &str, speed_bps: u64, file_size: u64) {
        if !self.config.enabled || self.is_ignored(domain) {
            return;
        }

        let now = current_timestamp();
        let domain_data = self
            .domains
            .entry(domain.to_string())
            .or_insert_with(|| DomainReliability::new(domain.to_string()));

        let sample = DownloadSample {
            timestamp: now,
            speed_bps,
            success: true,
            file_size,
            error: None,
        };

        domain_data.samples.push(sample);
        domain_data.total_successes += 1;
        domain_data.last_download_at = now;
        domain_data.consecutive_failures = 0;

        // Update peak speed
        if speed_bps > domain_data.peak_speed_bps {
            domain_data.peak_speed_bps = speed_bps;
        }

        // Update EMA average speed
        if domain_data.avg_speed_bps == 0.0 {
            domain_data.avg_speed_bps = speed_bps as f64;
        } else {
            let alpha = 0.3; // EMA smoothing factor
            domain_data.avg_speed_bps =
                alpha * speed_bps as f64 + (1.0 - alpha) * domain_data.avg_speed_bps;
        }

        // Trim samples to max
        if domain_data.samples.len() > self.config.max_samples_per_domain {
            let excess = domain_data.samples.len() - self.config.max_samples_per_domain;
            domain_data.samples.drain(0..excess);
        }

        // Recompute reliability score
        self.recompute_score(domain);
    }

    /// Record a failed download from a domain
    pub fn record_failure(&mut self, domain: &str, error: &str) {
        if !self.config.enabled || self.is_ignored(domain) {
            return;
        }

        let now = current_timestamp();
        let domain_data = self
            .domains
            .entry(domain.to_string())
            .or_insert_with(|| DomainReliability::new(domain.to_string()));

        let sample = DownloadSample {
            timestamp: now,
            speed_bps: 0,
            success: false,
            file_size: 0,
            error: Some(error.to_string()),
        };

        domain_data.samples.push(sample);
        domain_data.total_failures += 1;
        domain_data.last_download_at = now;
        domain_data.consecutive_failures += 1;

        // Trim samples to max
        if domain_data.samples.len() > self.config.max_samples_per_domain {
            let excess = domain_data.samples.len() - self.config.max_samples_per_domain;
            domain_data.samples.drain(0..excess);
        }

        // Recompute reliability score
        self.recompute_score(domain);
    }

    /// Check if a domain should be ignored
    fn is_ignored(&self, domain: &str) -> bool {
        self.config
            .ignored_domains
            .iter()
            .any(|d| d.to_lowercase() == domain.to_lowercase())
    }

    /// Recompute the reliability score for a domain
    fn recompute_score(&mut self, domain: &str) {
        if let Some(domain_data) = self.domains.get_mut(domain) {
            let total_attempts = domain_data.total_successes + domain_data.total_failures;

            // Need minimum attempts for meaningful score
            if total_attempts < self.config.min_attempts {
                domain_data.reliability_score = 0.5; // neutral
                return;
            }

            // Success rate component (0.0-1.0)
            let success_rate = domain_data.total_successes as f64 / total_attempts as f64;

            // Speed component (normalized to 0.0-1.0)
            // Assume 10 MB/s is excellent, 0 is terrible
            let speed_score = (domain_data.avg_speed_bps / (10.0 * 1024.0 * 1024.0)).min(1.0);

            // Failure penalty (consecutive failures reduce score)
            let failure_penalty = if domain_data.consecutive_failures > 0 {
                let penalty = 0.1 * domain_data.consecutive_failures.min(10) as f64;
                penalty.min(1.0)
            } else {
                0.0
            };

            // Weighted combination
            let raw_score = self.config.success_weight * success_rate
                + self.config.speed_weight * speed_score
                - self.config.failure_weight * failure_penalty;

            // Clamp to 0.0-1.0
            domain_data.reliability_score = raw_score.max(0.0).min(1.0);
        }
    }

    /// Get reliability data for a specific domain
    pub fn get_domain(&self, domain: &str) -> Option<&DomainReliability> {
        self.domains.get(domain)
    }

    /// Get the reliability score for a domain (0.0-1.0, or 0.5 if unknown)
    pub fn get_score(&self, domain: &str) -> f64 {
        self.domains
            .get(domain)
            .map(|d| d.reliability_score)
            .unwrap_or(0.5)
    }

    /// Get the reliability tier for a domain
    pub fn get_tier(&self, domain: &str) -> ReliabilityTier {
        self.domains
            .get(domain)
            .map(|d| d.tier())
            .unwrap_or(ReliabilityTier::Good) // assume good for unknown
    }

    /// Get all tracked domains sorted by reliability (best first)
    pub fn get_domains_by_reliability(&self) -> Vec<&DomainReliability> {
        let mut domains: Vec<_> = self.domains.values().collect();
        domains.sort_by(|a, b| {
            b.reliability_score
                .partial_cmp(&a.reliability_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        domains
    }

    /// Get domains that should be avoided (Poor or Unreliable tier)
    pub fn get_avoid_domains(&self) -> Vec<(String, f64)> {
        self.domains
            .values()
            .filter(|d| {
                d.tier() == ReliabilityTier::Poor || d.tier() == ReliabilityTier::Unreliable
            })
            .map(|d| (d.domain.clone(), d.reliability_score))
            .collect()
    }

    /// Generate a summary of source reliability
    pub fn get_summary(&self) -> ReliabilitySummary {
        let mut excellent_count = 0;
        let mut good_count = 0;
        let mut fair_count = 0;
        let mut poor_count = 0;
        let mut unreliable_count = 0;
        let mut total_samples = 0;
        let mut total_reliability = 0.0;

        for domain in self.domains.values() {
            match domain.tier() {
                ReliabilityTier::Excellent => excellent_count += 1,
                ReliabilityTier::Good => good_count += 1,
                ReliabilityTier::Fair => fair_count += 1,
                ReliabilityTier::Poor => poor_count += 1,
                ReliabilityTier::Unreliable => unreliable_count += 1,
            }
            total_samples += domain.samples.len();
            total_reliability += domain.reliability_score;
        }

        let total_domains = self.domains.len();
        let avg_reliability = if total_domains > 0 {
            total_reliability / total_domains as f64
        } else {
            0.5
        };

        // Top 5 domains
        let mut sorted: Vec<_> = self
            .domains
            .values()
            .map(|d| (d.domain.clone(), d.reliability_score))
            .collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let top_domains: Vec<_> = sorted.iter().take(5).cloned().collect();
        let bottom_domains: Vec<_> = sorted.iter().rev().take(5).cloned().collect();

        ReliabilitySummary {
            total_domains,
            excellent_count,
            good_count,
            fair_count,
            poor_count,
            unreliable_count,
            top_domains,
            bottom_domains,
            avg_reliability,
            total_samples,
        }
    }

    /// Clear all tracked data
    pub fn clear(&mut self) {
        self.domains.clear();
    }

    /// Clear data for a specific domain
    pub fn clear_domain(&mut self, domain: &str) {
        self.domains.remove(domain);
    }

    /// Remove old samples older than the given timestamp
    pub fn prune_old_samples(&mut self, before_timestamp: u64) {
        for domain_data in self.domains.values_mut() {
            domain_data
                .samples
                .retain(|s| s.timestamp >= before_timestamp);
        }
    }

    /// Format a human-readable summary
    pub fn format_summary(&self) -> String {
        let summary = self.get_summary();
        let mut output = String::new();

        output.push_str("📊 Source Reliability Summary\n");
        output.push_str(&format!(
            "  Total domains tracked: {}\n",
            summary.total_domains
        ));
        output.push_str(&format!(
            "  Average reliability: {:.1}%\n",
            summary.avg_reliability * 100.0
        ));
        output.push_str(&format!("  Total samples: {}\n", summary.total_samples));
        output.push('\n');

        output.push_str("  Tier distribution:\n");
        output.push_str(&format!("    🟢 Excellent: {}\n", summary.excellent_count));
        output.push_str(&format!("    🔵 Good: {}\n", summary.good_count));
        output.push_str(&format!("    🟡 Fair: {}\n", summary.fair_count));
        output.push_str(&format!("    🟠 Poor: {}\n", summary.poor_count));
        output.push_str(&format!(
            "    🔴 Unreliable: {}\n",
            summary.unreliable_count
        ));

        if !summary.top_domains.is_empty() {
            output.push('\n');
            output.push_str("  Top reliable domains:\n");
            for (domain, score) in &summary.top_domains {
                output.push_str(&format!("    {} ({:.0}%)\n", domain, score * 100.0));
            }
        }

        if !summary.bottom_domains.is_empty() {
            output.push('\n');
            output.push_str("  Least reliable domains:\n");
            for (domain, score) in &summary.bottom_domains {
                output.push_str(&format!("    {} ({:.0}%)\n", domain, score * 100.0));
            }
        }

        output
    }
}

/// Get current timestamp in seconds since UNIX epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SourceReliabilityConfig {
        SourceReliabilityConfig {
            enabled: true,
            max_samples_per_domain: 10,
            min_attempts: 3,
            decay_factor: 0.95,
            speed_weight: 0.3,
            success_weight: 0.5,
            failure_weight: 0.2,
            ignored_domains: vec!["ignored.com".to_string()],
        }
    }

    #[test]
    fn test_new_tracker() {
        let tracker = SourceReliabilityTracker::new();
        assert!(tracker.config.enabled);
        assert_eq!(tracker.domains.len(), 0);
    }

    #[test]
    fn test_record_success() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());
        tracker.record_success("example.com", 1_000_000, 10_000_000);

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.total_successes, 1);
        assert_eq!(domain.total_failures, 0);
        assert_eq!(domain.samples.len(), 1);
        assert!(domain.samples[0].success);
        assert_eq!(domain.samples[0].speed_bps, 1_000_000);
    }

    #[test]
    fn test_record_failure() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());
        tracker.record_failure("example.com", "Connection timeout");

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.total_successes, 0);
        assert_eq!(domain.total_failures, 1);
        assert_eq!(domain.consecutive_failures, 1);
        assert!(!domain.samples[0].success);
        assert_eq!(
            domain.samples[0].error.as_deref(),
            Some("Connection timeout")
        );
    }

    #[test]
    fn test_reliability_score_excellent() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        // Record many successes with good speed
        for _ in 0..10 {
            tracker.record_success("fast.com", 5_000_000, 50_000_000);
        }

        let domain = tracker.get_domain("fast.com").unwrap();
        assert!(
            domain.reliability_score > 0.6,
            "score was {}",
            domain.reliability_score
        );
        assert!(
            domain.tier() == ReliabilityTier::Excellent || domain.tier() == ReliabilityTier::Good
        );
    }

    #[test]
    fn test_reliability_score_poor() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        // Record mostly failures
        for _ in 0..8 {
            tracker.record_failure("slow.com", "Timeout");
        }
        for _ in 0..2 {
            tracker.record_success("slow.com", 100_000, 1_000_000);
        }

        let domain = tracker.get_domain("slow.com").unwrap();
        assert!(domain.reliability_score < 0.4);
    }

    #[test]
    fn test_consecutive_failures_penalty() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        // Mix of success and failure, but end with consecutive failures
        for _ in 0..5 {
            tracker.record_success("flaky.com", 1_000_000, 10_000_000);
        }
        for _ in 0..5 {
            tracker.record_failure("flaky.com", "Error");
        }

        let domain = tracker.get_domain("flaky.com").unwrap();
        assert_eq!(domain.consecutive_failures, 5);
        assert!(domain.reliability_score < 0.5);
    }

    #[test]
    fn test_success_resets_consecutive_failures() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        tracker.record_failure("example.com", "Error 1");
        tracker.record_failure("example.com", "Error 2");
        assert_eq!(
            tracker
                .get_domain("example.com")
                .unwrap()
                .consecutive_failures,
            2
        );

        tracker.record_success("example.com", 1_000_000, 10_000_000);
        assert_eq!(
            tracker
                .get_domain("example.com")
                .unwrap()
                .consecutive_failures,
            0
        );
    }

    #[test]
    fn test_ignored_domain() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        tracker.record_success("ignored.com", 1_000_000, 10_000_000);
        assert!(tracker.get_domain("ignored.com").is_none());

        tracker.record_failure("ignored.com", "Error");
        assert!(tracker.get_domain("ignored.com").is_none());
    }

    #[test]
    fn test_disabled_tracking() {
        let config = SourceReliabilityConfig {
            enabled: false,
            ..test_config()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        tracker.record_success("example.com", 1_000_000, 10_000_000);
        assert!(tracker.get_domain("example.com").is_none());
    }

    #[test]
    fn test_min_attempts_threshold() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        // Only 2 attempts, below min_attempts of 3
        tracker.record_success("new.com", 1_000_000, 10_000_000);
        tracker.record_success("new.com", 1_000_000, 10_000_000);

        let domain = tracker.get_domain("new.com").unwrap();
        // Should be neutral 0.5 since below min_attempts
        assert_eq!(domain.reliability_score, 0.5);

        // Third attempt should trigger real scoring
        tracker.record_success("new.com", 5_000_000, 50_000_000);
        let domain = tracker.get_domain("new.com").unwrap();
        assert_ne!(domain.reliability_score, 0.5);
    }

    #[test]
    fn test_sample_trimming() {
        let config = SourceReliabilityConfig {
            max_samples_per_domain: 5,
            ..test_config()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        for _ in 0..10 {
            tracker.record_success("example.com", 1_000_000, 10_000_000);
        }

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.samples.len(), 5);
    }

    #[test]
    fn test_get_score_unknown_domain() {
        let tracker = SourceReliabilityTracker::new();
        assert_eq!(tracker.get_score("unknown.com"), 0.5);
    }

    #[test]
    fn test_get_tier_unknown_domain() {
        let tracker = SourceReliabilityTracker::new();
        assert_eq!(tracker.get_tier("unknown.com"), ReliabilityTier::Good);
    }

    #[test]
    fn test_get_domains_by_reliability() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        // Create domains with different reliability
        for _ in 0..10 {
            tracker.record_success("good.com", 5_000_000, 50_000_000);
        }
        for _ in 0..8 {
            tracker.record_failure("bad.com", "Error");
        }
        for _ in 0..2 {
            tracker.record_success("bad.com", 100_000, 1_000_000);
        }

        let sorted = tracker.get_domains_by_reliability();
        assert_eq!(sorted.len(), 2);
        assert!(sorted[0].reliability_score >= sorted[1].reliability_score);
    }

    #[test]
    fn test_get_avoid_domains() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        // Create a poor domain
        for _ in 0..9 {
            tracker.record_failure("terrible.com", "Error");
        }
        tracker.record_success("terrible.com", 100, 1000);

        let avoid = tracker.get_avoid_domains();
        assert!(avoid.iter().any(|(d, _)| d == "terrible.com"));
    }

    #[test]
    fn test_summary() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        for _ in 0..10 {
            tracker.record_success("good.com", 5_000_000, 50_000_000);
        }
        for _ in 0..10 {
            tracker.record_success("also-good.com", 3_000_000, 30_000_000);
        }

        let summary = tracker.get_summary();
        assert_eq!(summary.total_domains, 2);
        assert_eq!(summary.total_samples, 20);
        assert!(summary.avg_reliability > 0.5);
    }

    #[test]
    fn test_clear() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);
        assert_eq!(tracker.domains.len(), 1);

        tracker.clear();
        assert_eq!(tracker.domains.len(), 0);
    }

    #[test]
    fn test_clear_domain() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("a.com", 1_000_000, 10_000_000);
        tracker.record_success("b.com", 1_000_000, 10_000_000);
        assert_eq!(tracker.domains.len(), 2);

        tracker.clear_domain("a.com");
        assert_eq!(tracker.domains.len(), 1);
        assert!(tracker.get_domain("a.com").is_none());
        assert!(tracker.get_domain("b.com").is_some());
    }

    #[test]
    fn test_prune_old_samples() {
        let mut tracker = SourceReliabilityTracker::new();

        // Record some samples
        tracker.record_success("example.com", 1_000_000, 10_000_000);
        tracker.record_success("example.com", 1_000_000, 10_000_000);

        // Prune with current timestamp + 1000 (should remove all)
        let future_ts = current_timestamp() + 1000;
        tracker.prune_old_samples(future_ts);

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.samples.len(), 0);
    }

    #[test]
    fn test_format_summary() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());

        for _ in 0..5 {
            tracker.record_success("fast.com", 5_000_000, 50_000_000);
        }

        let output = tracker.format_summary();
        assert!(output.contains("Source Reliability Summary"));
        assert!(output.contains("Total domains tracked: 1"));
        assert!(output.contains("fast.com"));
    }

    #[test]
    fn test_reliability_tier_display() {
        assert_eq!(format!("{}", ReliabilityTier::Excellent), "Excellent");
        assert_eq!(format!("{}", ReliabilityTier::Good), "Good");
        assert_eq!(format!("{}", ReliabilityTier::Fair), "Fair");
        assert_eq!(format!("{}", ReliabilityTier::Poor), "Poor");
        assert_eq!(format!("{}", ReliabilityTier::Unreliable), "Unreliable");
    }

    #[test]
    fn test_success_rate() {
        let mut domain = DomainReliability::new("test.com".to_string());
        domain.total_successes = 7;
        domain.total_failures = 3;

        let rate = domain.success_rate();
        assert!((rate - 0.7).abs() < 0.001);
    }

    #[test]
    fn test_success_rate_no_attempts() {
        let domain = DomainReliability::new("test.com".to_string());
        assert_eq!(domain.success_rate(), 0.5); // neutral
    }

    #[test]
    fn test_peak_speed_tracking() {
        let mut tracker = SourceReliabilityTracker::new();

        tracker.record_success("example.com", 1_000_000, 10_000_000);
        assert_eq!(
            tracker.get_domain("example.com").unwrap().peak_speed_bps,
            1_000_000
        );

        tracker.record_success("example.com", 5_000_000, 50_000_000);
        assert_eq!(
            tracker.get_domain("example.com").unwrap().peak_speed_bps,
            5_000_000
        );

        tracker.record_success("example.com", 2_000_000, 20_000_000);
        assert_eq!(
            tracker.get_domain("example.com").unwrap().peak_speed_bps,
            5_000_000
        ); // still peak
    }

    #[test]
    fn test_ema_speed_average() {
        let mut tracker = SourceReliabilityTracker::new();

        tracker.record_success("example.com", 1_000_000, 10_000_000);
        let avg1 = tracker.get_domain("example.com").unwrap().avg_speed_bps;
        assert_eq!(avg1, 1_000_000.0); // first sample sets the EMA

        tracker.record_success("example.com", 2_000_000, 20_000_000);
        let avg2 = tracker.get_domain("example.com").unwrap().avg_speed_bps;
        // EMA: 0.3 * 2M + 0.7 * 1M = 1.3M
        assert!((avg2 - 1_300_000.0).abs() < 1.0);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut tracker = SourceReliabilityTracker::with_config(test_config());
        tracker.record_success("example.com", 1_000_000, 10_000_000);
        tracker.record_failure("example.com", "Timeout");

        let json = serde_json::to_string(&tracker).unwrap();
        let restored: SourceReliabilityTracker = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.domains.len(), 1);
        let domain = restored.get_domain("example.com").unwrap();
        assert_eq!(domain.total_successes, 1);
        assert_eq!(domain.total_failures, 1);
    }

    #[test]
    fn test_config_serialization() {
        let config = test_config();
        let json = serde_json::to_string(&config).unwrap();
        let restored: SourceReliabilityConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.enabled, config.enabled);
        assert_eq!(
            restored.max_samples_per_domain,
            config.max_samples_per_domain
        );
        assert_eq!(restored.min_attempts, config.min_attempts);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_domain_reliability_display() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);
        tracker.record_success("example.com", 2_000_000, 20_000_000);

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(format!("{}", domain.tier()), "Excellent");
    }

    #[test]
    fn test_reliability_summary_structure() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("good.com", 5_000_000, 50_000_000);
        tracker.record_failure("bad.com", "Connection timeout");

        let summary = tracker.get_summary();
        assert_eq!(summary.total_domains, 2);
        assert!(summary.total_samples >= 2);
        assert!(!summary.top_domains.is_empty() || !summary.bottom_domains.is_empty());
    }

    #[test]
    fn test_format_summary_output() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);

        let report = tracker.format_summary();
        assert!(report.contains("Source Reliability Summary"));
        assert!(report.contains("Total domains tracked"));
        assert!(report.contains("Tier distribution"));
    }

    #[test]
    fn test_clear_domain_specific() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("keep.com", 1_000_000, 10_000_000);
        tracker.record_success("remove.com", 2_000_000, 20_000_000);

        tracker.clear_domain("remove.com");
        assert!(tracker.get_domain("keep.com").is_some());
        assert!(tracker.get_domain("remove.com").is_none());
    }

    #[test]
    fn test_prune_old_samples() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        tracker.prune_old_samples(now + 1000); // prune future timestamp
        let domain = tracker.get_domain("example.com").unwrap();
        assert!(domain.samples.is_empty());
    }
}
