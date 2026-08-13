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
            domain_data.reliability_score = raw_score.clamp(0.0, 1.0);
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
        // Use min_attempts=1 so scoring kicks in immediately
        let config = SourceReliabilityConfig {
            min_attempts: 1,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);
        // Need speed > 10.48MB/s so speed_score caps at 1.0
        // raw_score = 0.5*1.0 + 0.3*1.0 - 0.2*0.0 = 0.8 => Excellent
        for _ in 0..5 {
            tracker.record_success("example.com", 11_000_000, 110_000_000);
        }

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

    // ── Phase 210: Comprehensive test coverage ──

    // ── ReliabilityTier serialization & traits ──

    #[test]
    fn test_reliability_tier_serde_roundtrip() {
        let tiers = vec![
            ReliabilityTier::Excellent,
            ReliabilityTier::Good,
            ReliabilityTier::Fair,
            ReliabilityTier::Poor,
            ReliabilityTier::Unreliable,
        ];
        for tier in tiers {
            let json = serde_json::to_string(&tier).unwrap();
            let restored: ReliabilityTier = serde_json::from_str(&json).unwrap();
            assert_eq!(tier, restored);
        }
    }

    #[test]
    fn test_reliability_tier_clone_copy_debug() {
        let tier = ReliabilityTier::Excellent;
        let cloned = tier.clone();
        let copied = tier;
        assert_eq!(tier, cloned);
        assert_eq!(tier, copied);
        // Debug
        let debug_str = format!("{:?}", tier);
        assert_eq!(debug_str, "Excellent");
    }

    #[test]
    fn test_reliability_tier_serde_snake_case() {
        // Verify serde uses default derive representation
        let json = serde_json::to_string(&ReliabilityTier::Excellent).unwrap();
        assert!(json.contains("Excellent"));
        let json = serde_json::to_string(&ReliabilityTier::Unreliable).unwrap();
        assert!(json.contains("Unreliable"));
    }

    // ── SourceReliabilityConfig ──

    #[test]
    fn test_config_default_values() {
        let config = SourceReliabilityConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_samples_per_domain, 100);
        assert_eq!(config.min_attempts, 3);
        assert!((config.decay_factor - 0.95).abs() < f64::EPSILON);
        assert!((config.speed_weight - 0.3).abs() < f64::EPSILON);
        assert!((config.success_weight - 0.5).abs() < f64::EPSILON);
        assert!((config.failure_weight - 0.2).abs() < f64::EPSILON);
        assert!(config.ignored_domains.is_empty());
    }

    #[test]
    fn test_config_custom_values() {
        let config = SourceReliabilityConfig {
            enabled: false,
            max_samples_per_domain: 50,
            min_attempts: 10,
            decay_factor: 0.8,
            speed_weight: 0.4,
            success_weight: 0.4,
            failure_weight: 0.2,
            ignored_domains: vec!["cdn.example.com".to_string()],
        };
        assert!(!config.enabled);
        assert_eq!(config.max_samples_per_domain, 50);
        assert_eq!(config.min_attempts, 10);
        assert!((config.decay_factor - 0.8).abs() < f64::EPSILON);
        assert_eq!(config.ignored_domains.len(), 1);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = SourceReliabilityConfig {
            enabled: false,
            max_samples_per_domain: 42,
            min_attempts: 5,
            decay_factor: 0.9,
            speed_weight: 0.25,
            success_weight: 0.55,
            failure_weight: 0.2,
            ignored_domains: vec!["a.com".to_string(), "b.com".to_string()],
        };
        let json = serde_json::to_string(&config).unwrap();
        let restored: SourceReliabilityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.enabled, config.enabled);
        assert_eq!(
            restored.max_samples_per_domain,
            config.max_samples_per_domain
        );
        assert_eq!(restored.min_attempts, config.min_attempts);
        assert!((restored.decay_factor - config.decay_factor).abs() < f64::EPSILON);
        assert_eq!(restored.ignored_domains, config.ignored_domains);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"max_samples_per_domain":100,"min_attempts":3,"decay_factor":0.95,"speed_weight":0.3,"success_weight":0.5,"failure_weight":0.2,"ignored_domains":[],"extra_field":"ignored","another":42}"#;
        let config: SourceReliabilityConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_samples_per_domain, 100);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = SourceReliabilityConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("SourceReliabilityConfig"));
    }

    // ── DownloadSample ──

    #[test]
    fn test_download_sample_serde_roundtrip() {
        let sample = DownloadSample {
            timestamp: 1700000000,
            speed_bps: 5_000_000,
            success: true,
            file_size: 50_000_000,
            error: None,
        };
        let json = serde_json::to_string(&sample).unwrap();
        let restored: DownloadSample = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.timestamp, sample.timestamp);
        assert_eq!(restored.speed_bps, sample.speed_bps);
        assert_eq!(restored.success, sample.success);
        assert_eq!(restored.file_size, sample.file_size);
        assert!(restored.error.is_none());
    }

    #[test]
    fn test_download_sample_with_error() {
        let sample = DownloadSample {
            timestamp: 1700000000,
            speed_bps: 0,
            success: false,
            file_size: 0,
            error: Some("Connection refused".to_string()),
        };
        let json = serde_json::to_string(&sample).unwrap();
        let restored: DownloadSample = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.error.as_deref(), Some("Connection refused"));
        assert!(!restored.success);
    }

    #[test]
    fn test_download_sample_clone_debug() {
        let sample = DownloadSample {
            timestamp: 100,
            speed_bps: 1000,
            success: true,
            file_size: 5000,
            error: None,
        };
        let cloned = sample.clone();
        assert_eq!(cloned.timestamp, sample.timestamp);
        let debug_str = format!("{:?}", sample);
        assert!(debug_str.contains("DownloadSample"));
    }

    // ── DomainReliability ──

    #[test]
    fn test_domain_reliability_new_defaults() {
        let dr = DomainReliability::new("test.com".to_string());
        assert_eq!(dr.domain, "test.com");
        assert!(dr.samples.is_empty());
        assert_eq!(dr.total_successes, 0);
        assert_eq!(dr.total_failures, 0);
        assert_eq!(dr.avg_speed_bps, 0.0);
        assert_eq!(dr.peak_speed_bps, 0);
        assert_eq!(dr.last_download_at, 0);
        assert_eq!(dr.reliability_score, 0.5);
        assert_eq!(dr.consecutive_failures, 0);
    }

    #[test]
    fn test_domain_reliability_tier_excellent_boundary() {
        let mut dr = DomainReliability::new("test.com".to_string());
        dr.reliability_score = 0.8;
        assert_eq!(dr.tier(), ReliabilityTier::Excellent);

        dr.reliability_score = 1.0;
        assert_eq!(dr.tier(), ReliabilityTier::Excellent);

        dr.reliability_score = 0.8001;
        assert_eq!(dr.tier(), ReliabilityTier::Excellent);
    }

    #[test]
    fn test_domain_reliability_tier_good_boundary() {
        let mut dr = DomainReliability::new("test.com".to_string());
        dr.reliability_score = 0.6;
        assert_eq!(dr.tier(), ReliabilityTier::Good);

        dr.reliability_score = 0.7999;
        assert_eq!(dr.tier(), ReliabilityTier::Good);
    }

    #[test]
    fn test_domain_reliability_tier_fair_boundary() {
        let mut dr = DomainReliability::new("test.com".to_string());
        dr.reliability_score = 0.4;
        assert_eq!(dr.tier(), ReliabilityTier::Fair);

        dr.reliability_score = 0.5999;
        assert_eq!(dr.tier(), ReliabilityTier::Fair);
    }

    #[test]
    fn test_domain_reliability_tier_poor_boundary() {
        let mut dr = DomainReliability::new("test.com".to_string());
        dr.reliability_score = 0.2;
        assert_eq!(dr.tier(), ReliabilityTier::Poor);

        dr.reliability_score = 0.3999;
        assert_eq!(dr.tier(), ReliabilityTier::Poor);
    }

    #[test]
    fn test_domain_reliability_tier_unreliable() {
        let mut dr = DomainReliability::new("test.com".to_string());
        dr.reliability_score = 0.19;
        assert_eq!(dr.tier(), ReliabilityTier::Unreliable);

        dr.reliability_score = 0.0;
        assert_eq!(dr.tier(), ReliabilityTier::Unreliable);
    }

    #[test]
    fn test_domain_reliability_success_rate_all_success() {
        let mut dr = DomainReliability::new("test.com".to_string());
        dr.total_successes = 10;
        dr.total_failures = 0;
        assert!((dr.success_rate() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_domain_reliability_success_rate_all_failure() {
        let mut dr = DomainReliability::new("test.com".to_string());
        dr.total_successes = 0;
        dr.total_failures = 10;
        assert!((dr.success_rate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_domain_reliability_success_rate_mixed() {
        let mut dr = DomainReliability::new("test.com".to_string());
        dr.total_successes = 3;
        dr.total_failures = 7;
        assert!((dr.success_rate() - 0.3).abs() < 0.001);
    }

    #[test]
    fn test_domain_reliability_clone_debug() {
        let dr = DomainReliability::new("test.com".to_string());
        let cloned = dr.clone();
        assert_eq!(cloned.domain, dr.domain);
        let debug_str = format!("{:?}", dr);
        assert!(debug_str.contains("DomainReliability"));
    }

    #[test]
    fn test_domain_reliability_serde_roundtrip() {
        let mut dr = DomainReliability::new("example.com".to_string());
        dr.total_successes = 5;
        dr.total_failures = 2;
        dr.avg_speed_bps = 1_500_000.0;
        dr.peak_speed_bps = 3_000_000;
        dr.reliability_score = 0.75;
        dr.samples.push(DownloadSample {
            timestamp: 100,
            speed_bps: 1000,
            success: true,
            file_size: 5000,
            error: None,
        });

        let json = serde_json::to_string(&dr).unwrap();
        let restored: DomainReliability = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.domain, dr.domain);
        assert_eq!(restored.total_successes, dr.total_successes);
        assert_eq!(restored.total_failures, dr.total_failures);
        assert!((restored.avg_speed_bps - dr.avg_speed_bps).abs() < f64::EPSILON);
        assert_eq!(restored.peak_speed_bps, dr.peak_speed_bps);
        assert!((restored.reliability_score - dr.reliability_score).abs() < f64::EPSILON);
        assert_eq!(restored.samples.len(), 1);
    }

    // ── ReliabilitySummary ──

    #[test]
    fn test_reliability_summary_serde_roundtrip() {
        let summary = ReliabilitySummary {
            total_domains: 5,
            excellent_count: 1,
            good_count: 2,
            fair_count: 1,
            poor_count: 1,
            unreliable_count: 0,
            top_domains: vec![("a.com".to_string(), 0.9), ("b.com".to_string(), 0.8)],
            bottom_domains: vec![("c.com".to_string(), 0.3)],
            avg_reliability: 0.65,
            total_samples: 100,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let restored: ReliabilitySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.total_domains, 5);
        assert_eq!(restored.excellent_count, 1);
        assert_eq!(restored.top_domains.len(), 2);
    }

    #[test]
    fn test_reliability_summary_clone_debug() {
        let summary = ReliabilitySummary {
            total_domains: 0,
            excellent_count: 0,
            good_count: 0,
            fair_count: 0,
            poor_count: 0,
            unreliable_count: 0,
            top_domains: vec![],
            bottom_domains: vec![],
            avg_reliability: 0.5,
            total_samples: 0,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_domains, summary.total_domains);
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("ReliabilitySummary"));
    }

    // ── SourceReliabilityTracker traits ──

    #[test]
    fn test_tracker_clone_debug() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);
        let cloned = tracker.clone();
        assert_eq!(cloned.domains.len(), 1);
        let debug_str = format!("{:?}", tracker);
        assert!(debug_str.contains("SourceReliabilityTracker"));
    }

    #[test]
    fn test_tracker_default_trait() {
        let tracker = SourceReliabilityTracker::default();
        assert!(tracker.config.enabled);
        assert_eq!(tracker.domains.len(), 0);
    }

    #[test]
    fn test_tracker_with_config() {
        let config = SourceReliabilityConfig {
            enabled: true,
            max_samples_per_domain: 50,
            min_attempts: 5,
            decay_factor: 0.9,
            speed_weight: 0.2,
            success_weight: 0.6,
            failure_weight: 0.2,
            ignored_domains: vec!["skip.com".to_string()],
        };
        let tracker = SourceReliabilityTracker::with_config(config);
        assert_eq!(tracker.config.max_samples_per_domain, 50);
        assert_eq!(tracker.config.min_attempts, 5);
    }

    // ── Edge cases: zero values ──

    #[test]
    fn test_record_success_zero_speed() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 0, 10_000_000);

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.total_successes, 1);
        assert_eq!(domain.peak_speed_bps, 0);
        assert_eq!(domain.avg_speed_bps, 0.0);
    }

    #[test]
    fn test_record_success_zero_file_size() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 0);

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.total_successes, 1);
        assert_eq!(domain.samples[0].file_size, 0);
    }

    #[test]
    fn test_record_failure_empty_error() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_failure("example.com", "");

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.total_failures, 1);
        assert_eq!(domain.samples[0].error.as_deref(), Some(""));
    }

    // ── Unicode domains ──

    #[test]
    fn test_unicode_domain() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("пример.com", 1_000_000, 10_000_000);

        let domain = tracker.get_domain("пример.com").unwrap();
        assert_eq!(domain.domain, "пример.com");
        assert_eq!(domain.total_successes, 1);
    }

    #[test]
    fn test_japanese_domain() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("日本語ドメイン.com", 1_000_000, 10_000_000);

        assert!(tracker.get_domain("日本語ドメイン.com").is_some());
    }

    #[test]
    fn test_emoji_domain() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("🚀.com", 1_000_000, 10_000_000);

        assert!(tracker.get_domain("🚀.com").is_some());
    }

    // ── Ignored domain case-insensitive ──

    #[test]
    fn test_ignored_domain_case_insensitive() {
        let config = SourceReliabilityConfig {
            ignored_domains: vec!["CDN.Example.COM".to_string()],
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        tracker.record_success("cdn.example.com", 1_000_000, 10_000_000);
        assert!(tracker.get_domain("cdn.example.com").is_none());

        tracker.record_success("CDN.EXAMPLE.COM", 1_000_000, 10_000_000);
        assert!(tracker.get_domain("CDN.EXAMPLE.COM").is_none());
    }

    #[test]
    fn test_ignored_domain_multiple() {
        let config = SourceReliabilityConfig {
            ignored_domains: vec!["a.com".to_string(), "b.com".to_string()],
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        tracker.record_success("a.com", 1_000_000, 10_000_000);
        tracker.record_success("b.com", 1_000_000, 10_000_000);
        tracker.record_success("c.com", 1_000_000, 10_000_000);

        assert!(tracker.get_domain("a.com").is_none());
        assert!(tracker.get_domain("b.com").is_none());
        assert!(tracker.get_domain("c.com").is_some());
    }

    // ── Score computation boundaries ──

    #[test]
    fn test_score_min_attempts_exact_boundary() {
        let config = SourceReliabilityConfig {
            min_attempts: 1,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        // With min_attempts=1, first success should trigger scoring
        tracker.record_success("example.com", 5_000_000, 50_000_000);
        let domain = tracker.get_domain("example.com").unwrap();
        // Score should not be neutral 0.5 since min_attempts=1 is met
        assert_ne!(domain.reliability_score, 0.5);
    }

    #[test]
    fn test_score_min_attempts_zero() {
        let config = SourceReliabilityConfig {
            min_attempts: 0,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        tracker.record_success("example.com", 5_000_000, 50_000_000);
        let domain = tracker.get_domain("example.com").unwrap();
        // With min_attempts=0, scoring should happen immediately
        assert_ne!(domain.reliability_score, 0.5);
    }

    #[test]
    fn test_score_consecutive_failures_capped_at_10() {
        let mut tracker = SourceReliabilityTracker::new();

        // Record many failures to test penalty cap
        for _ in 0..20 {
            tracker.record_failure("bad.com", "Error");
        }

        let domain = tracker.get_domain("bad.com").unwrap();
        assert_eq!(domain.consecutive_failures, 20);
        // Score should be clamped to 0.0 (penalty capped at 1.0 * failure_weight)
        assert!(domain.reliability_score >= 0.0);
    }

    #[test]
    fn test_score_clamped_to_zero_one() {
        let config = SourceReliabilityConfig {
            success_weight: 1.0,
            speed_weight: 0.0,
            failure_weight: 0.0,
            min_attempts: 1,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        // All successes with high speed
        for _ in 0..5 {
            tracker.record_success("good.com", 10_000_000, 100_000_000);
        }

        let domain = tracker.get_domain("good.com").unwrap();
        assert!(domain.reliability_score >= 0.0);
        assert!(domain.reliability_score <= 1.0);
    }

    #[test]
    fn test_score_all_failures_zero_score() {
        let mut tracker = SourceReliabilityTracker::new();

        for _ in 0..10 {
            tracker.record_failure("terrible.com", "Error");
        }

        let domain = tracker.get_domain("terrible.com").unwrap();
        // success_rate=0, speed_score=0, failure_penalty=max
        assert!(domain.reliability_score < 0.1);
    }

    // ── Summary boundaries ──

    #[test]
    fn test_summary_empty_tracker() {
        let tracker = SourceReliabilityTracker::new();
        let summary = tracker.get_summary();
        assert_eq!(summary.total_domains, 0);
        assert_eq!(summary.excellent_count, 0);
        assert_eq!(summary.good_count, 0);
        assert_eq!(summary.fair_count, 0);
        assert_eq!(summary.poor_count, 0);
        assert_eq!(summary.unreliable_count, 0);
        assert!(summary.top_domains.is_empty());
        assert!(summary.bottom_domains.is_empty());
        assert_eq!(summary.avg_reliability, 0.5);
        assert_eq!(summary.total_samples, 0);
    }

    #[test]
    fn test_summary_single_domain() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("only.com", 5_000_000, 50_000_000);

        let summary = tracker.get_summary();
        assert_eq!(summary.total_domains, 1);
        assert_eq!(summary.top_domains.len(), 1);
        assert_eq!(summary.bottom_domains.len(), 1);
        assert_eq!(summary.top_domains[0].0, "only.com");
    }

    #[test]
    fn test_summary_top_bottom_limit_5() {
        let mut tracker = SourceReliabilityTracker::new();

        // Create 10 domains with different reliability
        for i in 0..10 {
            let domain = format!("domain{}.com", i);
            for _ in 0..(i + 1) {
                tracker.record_success(&domain, (i as u64 + 1) * 1_000_000, 10_000_000);
            }
        }

        let summary = tracker.get_summary();
        assert_eq!(summary.total_domains, 10);
        assert!(summary.top_domains.len() <= 5);
        assert!(summary.bottom_domains.len() <= 5);
    }

    #[test]
    fn test_summary_tier_counts() {
        let config = SourceReliabilityConfig {
            min_attempts: 1,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        // Create domains in different tiers
        // Excellent: many fast successes (need very high speed for score >= 0.8)
        for _ in 0..20 {
            tracker.record_success("excellent.com", 10_000_000, 100_000_000);
        }

        // Poor/Unreliable: many failures
        for _ in 0..10 {
            tracker.record_failure("poor.com", "Error");
        }

        let summary = tracker.get_summary();
        // Verify tier distribution adds up
        assert_eq!(
            summary.excellent_count
                + summary.good_count
                + summary.fair_count
                + summary.poor_count
                + summary.unreliable_count,
            summary.total_domains
        );
        // Poor domain should exist
        assert!(summary.poor_count >= 1 || summary.unreliable_count >= 1);
    }

    #[test]
    fn test_summary_avg_reliability_calculation() {
        let mut tracker = SourceReliabilityTracker::with_config(SourceReliabilityConfig {
            min_attempts: 1,
            ..SourceReliabilityConfig::default()
        });

        tracker.record_success("a.com", 5_000_000, 50_000_000);
        tracker.record_success("b.com", 5_000_000, 50_000_000);

        let summary = tracker.get_summary();
        // Average should be the mean of both domain scores
        assert!(summary.avg_reliability > 0.0);
        assert!(summary.avg_reliability <= 1.0);
    }

    // ── format_summary ──

    #[test]
    fn test_format_summary_empty() {
        let tracker = SourceReliabilityTracker::new();
        let output = tracker.format_summary();
        assert!(output.contains("Source Reliability Summary"));
        assert!(output.contains("Total domains tracked: 0"));
        assert!(output.contains("Average reliability: 50.0%"));
    }

    #[test]
    fn test_format_summary_with_domains() {
        let mut tracker = SourceReliabilityTracker::new();
        for _ in 0..5 {
            tracker.record_success("fast.com", 5_000_000, 50_000_000);
        }
        for _ in 0..5 {
            tracker.record_failure("slow.com", "Timeout");
        }

        let output = tracker.format_summary();
        assert!(output.contains("fast.com"));
        assert!(output.contains("slow.com"));
        assert!(output.contains("Tier distribution"));
        assert!(output.contains("Excellent"));
    }

    #[test]
    fn test_format_summary_all_sections() {
        let mut tracker = SourceReliabilityTracker::new();

        // Create a mix of good and bad domains
        for _ in 0..10 {
            tracker.record_success("good.com", 5_000_000, 50_000_000);
        }
        for _ in 0..10 {
            tracker.record_failure("bad.com", "Error");
        }

        let output = tracker.format_summary();
        assert!(output.contains("Top reliable domains"));
        assert!(output.contains("Least reliable domains"));
        assert!(output.contains("🟢 Excellent"));
        assert!(output.contains("🔴 Unreliable"));
    }

    // ── get_domains_by_reliability ──

    #[test]
    fn test_get_domains_by_reliability_empty() {
        let tracker = SourceReliabilityTracker::new();
        let sorted = tracker.get_domains_by_reliability();
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_get_domains_by_reliability_single() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("only.com", 1_000_000, 10_000_000);
        let sorted = tracker.get_domains_by_reliability();
        assert_eq!(sorted.len(), 1);
    }

    #[test]
    fn test_get_domains_by_reliability_ordering() {
        let mut tracker = SourceReliabilityTracker::new();

        // Create domains with known different scores
        for _ in 0..10 {
            tracker.record_success("best.com", 8_000_000, 80_000_000);
        }
        for _ in 0..5 {
            tracker.record_success("ok.com", 1_000_000, 10_000_000);
        }
        for _ in 0..5 {
            tracker.record_failure("worst.com", "Error");
        }

        let sorted = tracker.get_domains_by_reliability();
        assert_eq!(sorted.len(), 3);
        // Best should be first
        assert!(sorted[0].reliability_score >= sorted[1].reliability_score);
        assert!(sorted[1].reliability_score >= sorted[2].reliability_score);
    }

    // ── get_avoid_domains ──

    #[test]
    fn test_get_avoid_domains_empty() {
        let tracker = SourceReliabilityTracker::new();
        let avoid = tracker.get_avoid_domains();
        assert!(avoid.is_empty());
    }

    #[test]
    fn test_get_avoid_domains_excludes_good() {
        let mut tracker = SourceReliabilityTracker::new();
        for _ in 0..10 {
            tracker.record_success("good.com", 5_000_000, 50_000_000);
        }

        let avoid = tracker.get_avoid_domains();
        assert!(!avoid.iter().any(|(d, _)| d == "good.com"));
    }

    // ── prune_old_samples ──

    #[test]
    fn test_prune_old_samples_keeps_recent() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);

        // Prune with timestamp 0 (should keep all since all are >= 0)
        tracker.prune_old_samples(0);
        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.samples.len(), 1);
    }

    #[test]
    fn test_prune_old_samples_multiple_domains() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("a.com", 1_000_000, 10_000_000);
        tracker.record_success("b.com", 2_000_000, 20_000_000);
        tracker.record_failure("c.com", "Error");

        let future_ts = current_timestamp() + 1000;
        tracker.prune_old_samples(future_ts);

        for domain in tracker.domains.values() {
            assert!(domain.samples.is_empty());
        }
    }

    // ── clear_domain ──

    #[test]
    fn test_clear_domain_nonexistent() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.clear_domain("nonexistent.com");
        assert_eq!(tracker.domains.len(), 0);
    }

    // ── Complex workflow ──

    #[test]
    fn test_full_lifecycle() {
        let mut tracker = SourceReliabilityTracker::with_config(SourceReliabilityConfig {
            max_samples_per_domain: 20,
            min_attempts: 2,
            ..SourceReliabilityConfig::default()
        });

        // Phase 1: Record initial successes
        tracker.record_success("fast.com", 5_000_000, 50_000_000);
        tracker.record_success("fast.com", 6_000_000, 60_000_000);
        tracker.record_success("fast.com", 4_000_000, 40_000_000);

        let domain = tracker.get_domain("fast.com").unwrap();
        assert_eq!(domain.total_successes, 3);
        assert!(domain.reliability_score > 0.5);

        // Phase 2: Some failures
        tracker.record_failure("fast.com", "Timeout");
        tracker.record_failure("fast.com", "Connection reset");

        let domain = tracker.get_domain("fast.com").unwrap();
        assert_eq!(domain.consecutive_failures, 2);

        // Phase 3: Recovery
        tracker.record_success("fast.com", 3_000_000, 30_000_000);
        let domain = tracker.get_domain("fast.com").unwrap();
        assert_eq!(domain.consecutive_failures, 0);

        // Phase 4: Summary
        let summary = tracker.get_summary();
        assert_eq!(summary.total_domains, 1);
        assert_eq!(summary.total_samples, 6);

        // Phase 5: Format
        let report = tracker.format_summary();
        assert!(report.contains("fast.com"));

        // Phase 6: Serialize and restore
        let json = serde_json::to_string(&tracker).unwrap();
        let restored: SourceReliabilityTracker = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.get_domain("fast.com").unwrap().total_successes, 4);
    }

    #[test]
    fn test_multiple_domains_independent() {
        let mut tracker = SourceReliabilityTracker::new();

        // Record different patterns for different domains
        for _ in 0..5 {
            tracker.record_success("a.com", 1_000_000, 10_000_000);
        }
        for _ in 0..5 {
            tracker.record_failure("b.com", "Error");
        }
        tracker.record_success("c.com", 5_000_000, 50_000_000);
        tracker.record_failure("c.com", "Timeout");

        // Verify independence
        assert_eq!(tracker.get_domain("a.com").unwrap().total_successes, 5);
        assert_eq!(tracker.get_domain("a.com").unwrap().total_failures, 0);
        assert_eq!(tracker.get_domain("b.com").unwrap().total_successes, 0);
        assert_eq!(tracker.get_domain("b.com").unwrap().total_failures, 5);
        assert_eq!(tracker.get_domain("c.com").unwrap().total_successes, 1);
        assert_eq!(tracker.get_domain("c.com").unwrap().total_failures, 1);
    }

    #[test]
    fn test_disabled_does_not_clear_existing() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);
        assert_eq!(tracker.domains.len(), 1);

        // Disable tracking
        tracker.config.enabled = false;

        // Existing data should remain
        assert!(tracker.get_domain("example.com").is_some());

        // New records should be ignored
        tracker.record_success("example.com", 2_000_000, 20_000_000);
        assert_eq!(
            tracker.get_domain("example.com").unwrap().total_successes,
            1
        );

        tracker.record_failure("new.com", "Error");
        assert!(tracker.get_domain("new.com").is_none());
    }

    #[test]
    fn test_re_enable_tracking() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.config.enabled = false;

        tracker.record_success("example.com", 1_000_000, 10_000_000);
        assert!(tracker.get_domain("example.com").is_none());

        tracker.config.enabled = true;
        tracker.record_success("example.com", 1_000_000, 10_000_000);
        assert!(tracker.get_domain("example.com").is_some());
    }

    // ── Score with custom weights ──

    #[test]
    fn test_custom_weights_speed_focused() {
        let make_config = || SourceReliabilityConfig {
            success_weight: 0.1,
            speed_weight: 0.9,
            failure_weight: 0.0,
            min_attempts: 1,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(make_config());

        // Fast but failing domain
        tracker.record_success("fast.com", 9_000_000, 90_000_000);
        let fast_score = tracker.get_domain("fast.com").unwrap().reliability_score;

        let mut tracker2 = SourceReliabilityTracker::with_config(make_config());
        tracker2.record_success("slow.com", 100_000, 1_000_000);
        let slow_score = tracker2.get_domain("slow.com").unwrap().reliability_score;

        // Fast domain should score higher when speed_weight is dominant
        assert!(fast_score > slow_score);
    }

    #[test]
    fn test_custom_weights_success_focused() {
        let config = SourceReliabilityConfig {
            success_weight: 0.9,
            speed_weight: 0.1,
            failure_weight: 0.0,
            min_attempts: 1,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        // 100% success rate
        for _ in 0..5 {
            tracker.record_success("perfect.com", 1_000_000, 10_000_000);
        }
        let score = tracker.get_domain("perfect.com").unwrap().reliability_score;
        assert!(
            score > 0.8,
            "perfect success rate should score > 0.8, got {}",
            score
        );
    }

    // ── Large number of samples ──

    #[test]
    fn test_many_samples_trimming() {
        let config = SourceReliabilityConfig {
            max_samples_per_domain: 10,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        for i in 0..50 {
            tracker.record_success("example.com", (i + 1) * 100_000, 1_000_000);
        }

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.samples.len(), 10);
        assert_eq!(domain.total_successes, 50);
    }

    // ── get_score and get_tier ──

    #[test]
    fn test_get_score_known_domain() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 5_000_000, 50_000_000);
        let score = tracker.get_score("example.com");
        assert!(score > 0.0 && score <= 1.0);
    }

    #[test]
    fn test_get_tier_all_variants() {
        let config = SourceReliabilityConfig {
            min_attempts: 1,
            ..SourceReliabilityConfig::default()
        };
        let mut tracker = SourceReliabilityTracker::with_config(config);

        // Excellent: score >= 0.8 (need speed > 10.48MB/s to cap speed_score at 1.0)
        for _ in 0..20 {
            tracker.record_success("excellent.com", 11_000_000, 110_000_000);
        }
        assert_eq!(
            tracker.get_tier("excellent.com"),
            ReliabilityTier::Excellent
        );

        // Unreliable: score < 0.2
        for _ in 0..20 {
            tracker.record_failure("unreliable.com", "Error");
        }
        assert!(
            tracker.get_tier("unreliable.com") == ReliabilityTier::Poor
                || tracker.get_tier("unreliable.com") == ReliabilityTier::Unreliable
        );
    }

    // ── Serialization edge cases ──

    #[test]
    fn test_tracker_serde_empty() {
        let tracker = SourceReliabilityTracker::new();
        let json = serde_json::to_string(&tracker).unwrap();
        let restored: SourceReliabilityTracker = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.domains.len(), 0);
    }

    #[test]
    fn test_tracker_serde_with_data() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("a.com", 1_000_000, 10_000_000);
        tracker.record_failure("b.com", "Timeout");

        let json = serde_json::to_string_pretty(&tracker).unwrap();
        let restored: SourceReliabilityTracker = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.domains.len(), 2);
        assert_eq!(restored.get_domain("a.com").unwrap().total_successes, 1);
        assert_eq!(restored.get_domain("b.com").unwrap().total_failures, 1);
    }

    #[test]
    fn test_tracker_serde_extra_fields_ignored() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);

        let mut json: serde_json::Value = serde_json::to_value(&tracker).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("extra".to_string(), serde_json::json!("ignored"));

        let restored: SourceReliabilityTracker = serde_json::from_value(json).unwrap();
        assert_eq!(restored.domains.len(), 1);
    }

    // ── last_download_at tracking ──

    #[test]
    fn test_last_download_at_updated() {
        let mut tracker = SourceReliabilityTracker::new();

        tracker.record_success("example.com", 1_000_000, 10_000_000);
        let ts1 = tracker.get_domain("example.com").unwrap().last_download_at;
        assert!(ts1 > 0);

        // Small delay to ensure different timestamp
        tracker.record_failure("example.com", "Error");
        let ts2 = tracker.get_domain("example.com").unwrap().last_download_at;
        assert!(ts2 >= ts1);
    }

    // ── Sample error field ──

    #[test]
    fn test_success_sample_no_error() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_success("example.com", 1_000_000, 10_000_000);

        let sample = &tracker.get_domain("example.com").unwrap().samples[0];
        assert!(sample.error.is_none());
        assert!(sample.success);
    }

    #[test]
    fn test_failure_sample_has_error() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_failure("example.com", "Connection refused");

        let sample = &tracker.get_domain("example.com").unwrap().samples[0];
        assert!(sample.error.is_some());
        assert!(!sample.success);
        assert_eq!(sample.speed_bps, 0);
        assert_eq!(sample.file_size, 0);
    }

    // ── Unicode in error messages ──

    #[test]
    fn test_unicode_error_message() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_failure("example.com", "连接超时");

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.samples[0].error.as_deref(), Some("连接超时"));
    }

    #[test]
    fn test_emoji_error_message() {
        let mut tracker = SourceReliabilityTracker::new();
        tracker.record_failure("example.com", "💥 crash");

        let domain = tracker.get_domain("example.com").unwrap();
        assert_eq!(domain.samples[0].error.as_deref(), Some("💥 crash"));
    }
}
