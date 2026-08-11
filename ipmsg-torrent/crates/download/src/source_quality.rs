//! Download Source Quality Scoring System
//!
//! Tracks long-term quality metrics for download domains/sources:
//! - Success/failure counts
//! - Average speed
//! - Reliability score (0-100)
//! - Last used timestamp
//! - Recommendation engine for source selection

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;

/// Quality score for a download source/domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceQuality {
    /// Domain or source identifier
    pub source_id: String,
    /// Total successful downloads from this source
    pub success_count: u32,
    /// Total failed downloads from this source
    pub failure_count: u32,
    /// Total bytes downloaded
    pub total_bytes: u64,
    /// Average download speed in bytes/sec (exponential moving average)
    pub avg_speed_bps: f64,
    /// Reliability score (0-100)
    pub reliability_score: f64,
    /// Quality tier based on score
    pub tier: SourceTier,
    /// Last time this source was used
    pub last_used_at: u64,
    /// First time this source was seen
    pub first_seen_at: u64,
    /// Number of times this source was used
    pub use_count: u32,
    /// Consecutive failures (for cooldown)
    pub consecutive_failures: u32,
    /// Whether this source is temporarily blocked due to failures
    pub is_blocked: bool,
    /// Block expiry timestamp (unix epoch seconds)
    pub block_until: Option<u64>,
}

/// Quality tier classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceTier {
    /// Score >= 80: Excellent reliability and speed
    Excellent,
    /// Score >= 60: Good reliability
    Good,
    /// Score >= 40: Average reliability
    Average,
    /// Score >= 20: Below average, prefer alternatives
    Poor,
    /// Score < 20: Unreliable, avoid if possible
    Unreliable,
}

/// Configuration for source quality tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceQualityConfig {
    /// Enable source quality tracking
    pub enabled: bool,
    /// Minimum samples before calculating reliability (default: 3)
    pub min_samples: u32,
    /// Weight for success rate in score (0.0-1.0, default: 0.5)
    pub success_weight: f64,
    /// Weight for speed in score (0.0-1.0, default: 0.3)
    pub speed_weight: f64,
    /// Weight for consistency in score (0.0-1.0, default: 0.2)
    pub consistency_weight: f64,
    /// Consecutive failures before temporary block (default: 5)
    pub block_threshold: u32,
    /// Block duration in seconds (default: 3600 = 1 hour)
    pub block_duration_secs: u64,
    /// Maximum sources to track (default: 1000)
    pub max_sources: usize,
    /// Decay factor for old data (0.0-1.0, default: 0.95)
    /// Applied on each update to gradually forget old failures
    pub decay_factor: f64,
    /// Speed EMA alpha (0.0-1.0, default: 0.3)
    /// Higher = more weight to recent samples
    pub speed_ema_alpha: f64,
}

impl Default for SourceQualityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_samples: 3,
            success_weight: 0.5,
            speed_weight: 0.3,
            consistency_weight: 0.2,
            block_threshold: 5,
            block_duration_secs: 3600,
            max_sources: 1000,
            decay_factor: 0.95,
            speed_ema_alpha: 0.3,
        }
    }
}

/// Summary of source quality statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceQualitySummary {
    /// Total tracked sources
    pub total_sources: usize,
    /// Sources by tier
    pub excellent_count: usize,
    pub good_count: usize,
    pub average_count: usize,
    pub poor_count: usize,
    pub unreliable_count: usize,
    /// Currently blocked sources
    pub blocked_count: usize,
    /// Top 5 sources by score
    pub top_sources: Vec<SourceQuality>,
    /// Bottom 5 sources by score
    pub bottom_sources: Vec<SourceQuality>,
    /// Average reliability across all sources
    pub avg_reliability: f64,
}

/// Manager for source quality tracking
#[derive(Debug)]
pub struct SourceQualityManager {
    config: SourceQualityConfig,
    sources: HashMap<String, SourceQuality>,
    data_dir: PathBuf,
}

impl SourceQualityManager {
    /// Create a new source quality manager
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            config: SourceQualityConfig::default(),
            sources: HashMap::new(),
            data_dir,
        }
    }

    /// Load configuration from disk
    pub async fn load_config(&mut self) -> Result<(), std::io::Error> {
        let config_path = self.data_dir.join("source_quality_config.json");
        if config_path.exists() {
            let data = fs::read_to_string(&config_path).await?;
            self.config = serde_json::from_str(&data).unwrap_or_default();
        }
        Ok(())
    }

    /// Load tracked sources from disk
    pub async fn load_sources(&mut self) -> Result<(), std::io::Error> {
        let sources_path = self.data_dir.join("source_quality_data.json");
        if sources_path.exists() {
            let data = fs::read_to_string(&sources_path).await?;
            if let Ok(sources) = serde_json::from_str::<HashMap<String, SourceQuality>>(&data) {
                self.sources = sources;
            }
        }
        Ok(())
    }

    /// Save configuration to disk
    pub async fn save_config(&self) -> Result<(), std::io::Error> {
        let config_path = self.data_dir.join("source_quality_config.json");
        let data = serde_json::to_string_pretty(&self.config)?;
        atomic_write(&config_path, data.as_bytes()).await
    }

    /// Save tracked sources to disk
    pub async fn save_sources(&self) -> Result<(), std::io::Error> {
        let sources_path = self.data_dir.join("source_quality_data.json");
        let data = serde_json::to_string_pretty(&self.sources)?;
        atomic_write(&sources_path, data.as_bytes()).await
    }

    /// Set configuration
    pub fn set_config(&mut self, config: SourceQualityConfig) {
        self.config = config;
    }

    /// Get configuration
    pub fn get_config(&self) -> &SourceQualityConfig {
        &self.config
    }

    /// Record a successful download from a source
    pub fn record_success(&mut self, source_id: &str, bytes: u64, speed_bps: f64) {
        if !self.config.enabled {
            return;
        }

        let now = current_timestamp();
        let source = self
            .sources
            .entry(source_id.to_string())
            .or_insert_with(|| {
                SourceQuality {
                    source_id: source_id.to_string(),
                    success_count: 0,
                    failure_count: 0,
                    total_bytes: 0,
                    avg_speed_bps: 0.0,
                    reliability_score: 50.0, // Start neutral
                    tier: SourceTier::Average,
                    last_used_at: now,
                    first_seen_at: now,
                    use_count: 0,
                    consecutive_failures: 0,
                    is_blocked: false,
                    block_until: None,
                }
            });

        // Update metrics
        source.success_count += 1;
        source.total_bytes += bytes;
        source.use_count += 1;
        source.last_used_at = now;
        source.consecutive_failures = 0;
        source.is_blocked = false;
        source.block_until = None;

        // Update speed EMA
        let alpha = self.config.speed_ema_alpha;
        if source.avg_speed_bps == 0.0 {
            source.avg_speed_bps = speed_bps;
        } else {
            source.avg_speed_bps = alpha * speed_bps + (1.0 - alpha) * source.avg_speed_bps;
        }

        // Apply decay to failure count (gradually forget old failures)
        source.failure_count = (source.failure_count as f64 * self.config.decay_factor) as u32;

        // Recalculate reliability (inline to avoid borrow checker issues)
        Self::calculate_reliability_score(source, &self.config);
        Self::update_tier_static(source);

        // Enforce max sources limit
        self.enforce_max_sources();
    }

    /// Record a failed download from a source
    pub fn record_failure(&mut self, source_id: &str) {
        if !self.config.enabled {
            return;
        }

        let now = current_timestamp();
        let source = self
            .sources
            .entry(source_id.to_string())
            .or_insert_with(|| SourceQuality {
                source_id: source_id.to_string(),
                success_count: 0,
                failure_count: 0,
                total_bytes: 0,
                avg_speed_bps: 0.0,
                reliability_score: 50.0,
                tier: SourceTier::Average,
                last_used_at: now,
                first_seen_at: now,
                use_count: 0,
                consecutive_failures: 0,
                is_blocked: false,
                block_until: None,
            });

        source.failure_count += 1;
        source.use_count += 1;
        source.last_used_at = now;
        source.consecutive_failures += 1;

        // Check if should block
        if source.consecutive_failures >= self.config.block_threshold {
            source.is_blocked = true;
            source.block_until = Some(now + self.config.block_duration_secs);
        }

        // Recalculate reliability (inline to avoid borrow checker issues)
        Self::calculate_reliability_score(source, &self.config);
        Self::update_tier_static(source);
    }

    /// Update reliability score based on weighted factors (static version)
    fn calculate_reliability_score(source: &mut SourceQuality, config: &SourceQualityConfig) {
        let total = source.success_count + source.failure_count;
        if total < config.min_samples {
            // Not enough data, keep neutral score
            source.reliability_score = 50.0;
            return;
        }

        // Success rate factor (0-100)
        let success_rate = (source.success_count as f64 / total as f64) * 100.0;

        // Speed factor (0-100) - normalized against typical speeds
        // Assume 10 MB/s (10,485,760 B/s) is excellent
        let speed_factor = (source.avg_speed_bps / 10_485_760.0).min(1.0) * 100.0;

        // Consistency factor (0-100) - based on consecutive failures
        let consistency_factor = if source.consecutive_failures == 0 {
            100.0
        } else {
            (100.0 / (source.consecutive_failures as f64 + 1.0)).max(0.0)
        };

        // Weighted score
        source.reliability_score = (success_rate * config.success_weight
            + speed_factor * config.speed_weight
            + consistency_factor * config.consistency_weight)
            .clamp(0.0, 100.0);
    }

    /// Update tier based on reliability score (static version)
    fn update_tier_static(source: &mut SourceQuality) {
        source.tier = match source.reliability_score {
            s if s >= 80.0 => SourceTier::Excellent,
            s if s >= 60.0 => SourceTier::Good,
            s if s >= 40.0 => SourceTier::Average,
            s if s >= 20.0 => SourceTier::Poor,
            _ => SourceTier::Unreliable,
        };
    }

    /// Enforce maximum sources limit by removing lowest-scoring sources
    fn enforce_max_sources(&mut self) {
        if self.sources.len() <= self.config.max_sources {
            return;
        }

        // Sort by reliability score, keep highest
        let mut sources_vec: Vec<_> = self.sources.drain().collect();
        sources_vec.sort_by(|a, b| {
            b.1.reliability_score
                .partial_cmp(&a.1.reliability_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Keep top N
        for (key, value) in sources_vec.into_iter().take(self.config.max_sources) {
            self.sources.insert(key, value);
        }
    }

    /// Get quality info for a specific source
    pub fn get_source(&self, source_id: &str) -> Option<&SourceQuality> {
        self.sources.get(source_id)
    }

    /// Get all tracked sources
    pub fn get_all_sources(&self) -> &HashMap<String, SourceQuality> {
        &self.sources
    }

    /// Check if a source is currently blocked
    pub fn is_blocked(&self, source_id: &str) -> bool {
        if let Some(source) = self.sources.get(source_id) {
            if source.is_blocked {
                if let Some(block_until) = source.block_until {
                    return current_timestamp() < block_until;
                }
            }
        }
        false
    }

    /// Unblock a source manually
    pub fn unblock_source(&mut self, source_id: &str) -> bool {
        if let Some(source) = self.sources.get_mut(source_id) {
            source.is_blocked = false;
            source.block_until = None;
            source.consecutive_failures = 0;
            true
        } else {
            false
        }
    }

    /// Remove a source from tracking
    pub fn remove_source(&mut self, source_id: &str) -> bool {
        self.sources.remove(source_id).is_some()
    }

    /// Clear all tracked sources
    pub fn clear_all(&mut self) {
        self.sources.clear();
    }

    /// Get summary statistics
    pub fn get_summary(&self) -> SourceQualitySummary {
        let mut summary = SourceQualitySummary {
            total_sources: self.sources.len(),
            excellent_count: 0,
            good_count: 0,
            average_count: 0,
            poor_count: 0,
            unreliable_count: 0,
            blocked_count: 0,
            top_sources: Vec::new(),
            bottom_sources: Vec::new(),
            avg_reliability: 0.0,
        };

        if self.sources.is_empty() {
            return summary;
        }

        let mut total_reliability = 0.0;
        let mut sources_vec: Vec<_> = self.sources.values().cloned().collect();

        for source in &sources_vec {
            match source.tier {
                SourceTier::Excellent => summary.excellent_count += 1,
                SourceTier::Good => summary.good_count += 1,
                SourceTier::Average => summary.average_count += 1,
                SourceTier::Poor => summary.poor_count += 1,
                SourceTier::Unreliable => summary.unreliable_count += 1,
            }
            if self.is_blocked(&source.source_id) {
                summary.blocked_count += 1;
            }
            total_reliability += source.reliability_score;
        }

        summary.avg_reliability = total_reliability / sources_vec.len() as f64;

        // Sort for top/bottom
        sources_vec.sort_by(|a, b| {
            b.reliability_score
                .partial_cmp(&a.reliability_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        summary.top_sources = sources_vec.iter().take(5).cloned().collect();
        summary.bottom_sources = sources_vec.iter().rev().take(5).cloned().collect();

        summary
    }

    /// Recommend the best source from a list of candidates
    pub fn recommend_source(&self, candidates: &[String]) -> Option<String> {
        if candidates.is_empty() {
            return None;
        }

        // Filter out blocked sources
        let available: Vec<_> = candidates.iter().filter(|c| !self.is_blocked(c)).collect();

        if available.is_empty() {
            // All blocked, return first candidate
            return candidates.first().cloned();
        }

        // Find highest scoring available source
        available
            .into_iter()
            .max_by(|a, b| {
                let score_a = self
                    .sources
                    .get(*a)
                    .map(|s| s.reliability_score)
                    .unwrap_or(50.0);
                let score_b = self
                    .sources
                    .get(*b)
                    .map(|s| s.reliability_score)
                    .unwrap_or(50.0);
                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }

    /// Format summary for display
    pub fn format_summary(&self, summary: &SourceQualitySummary) -> String {
        let mut output = String::new();
        output.push_str("📊 Source Quality Summary\n");
        output.push_str(&format!("  Total sources: {}\n", summary.total_sources));
        output.push_str(&format!(
            "  Tiers: {} excellent, {} good, {} average, {} poor, {} unreliable\n",
            summary.excellent_count,
            summary.good_count,
            summary.average_count,
            summary.poor_count,
            summary.unreliable_count
        ));
        output.push_str(&format!("  Blocked: {}\n", summary.blocked_count));
        output.push_str(&format!(
            "  Avg reliability: {:.1}/100\n",
            summary.avg_reliability
        ));

        if !summary.top_sources.is_empty() {
            output.push_str("\n🏆 Top sources:\n");
            for (i, source) in summary.top_sources.iter().enumerate() {
                output.push_str(&format!(
                    "  {}. {} ({:.1}/100, {} success, {} fail)\n",
                    i + 1,
                    source.source_id,
                    source.reliability_score,
                    source.success_count,
                    source.failure_count
                ));
            }
        }

        if !summary.bottom_sources.is_empty() && summary.bottom_sources[0].reliability_score < 80.0
        {
            output.push_str("\n⚠️  Bottom sources:\n");
            for (i, source) in summary.bottom_sources.iter().enumerate() {
                if source.reliability_score >= 80.0 {
                    break;
                }
                output.push_str(&format!(
                    "  {}. {} ({:.1}/100, {} success, {} fail)\n",
                    i + 1,
                    source.source_id,
                    source.reliability_score,
                    source.success_count,
                    source.failure_count
                ));
            }
        }

        output
    }
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

/// Atomic file write helper
async fn atomic_write(path: &Path, data: &[u8]) -> Result<(), std::io::Error> {
    let temp_path = path.with_extension("tmp");
    fs::write(&temp_path, data).await?;
    fs::rename(&temp_path, path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_source_quality_config_default() {
        let config = SourceQualityConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 3);
        assert_eq!(config.block_threshold, 5);
        assert_eq!(config.max_sources, 1000);
    }

    #[test]
    fn test_source_tier_classification() {
        let mut source = SourceQuality {
            source_id: "test.com".to_string(),
            success_count: 0,
            failure_count: 0,
            total_bytes: 0,
            avg_speed_bps: 0.0,
            reliability_score: 85.0,
            tier: SourceTier::Excellent,
            last_used_at: 0,
            first_seen_at: 0,
            use_count: 0,
            consecutive_failures: 0,
            is_blocked: false,
            block_until: None,
        };

        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        source.reliability_score = 85.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Excellent);

        source.reliability_score = 65.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Good);

        source.reliability_score = 45.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Average);

        source.reliability_score = 25.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Poor);

        source.reliability_score = 15.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Unreliable);
    }

    #[test]
    fn test_record_success() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        manager.record_success("example.com", 1_000_000, 5_000_000.0);

        let source = manager.get_source("example.com").unwrap();
        assert_eq!(source.success_count, 1);
        assert_eq!(source.total_bytes, 1_000_000);
        assert!(source.avg_speed_bps > 0.0);
        assert_eq!(source.consecutive_failures, 0);
    }

    #[test]
    fn test_record_failure() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        manager.record_failure("example.com");

        let source = manager.get_source("example.com").unwrap();
        assert_eq!(source.failure_count, 1);
        assert_eq!(source.consecutive_failures, 1);
        assert!(!source.is_blocked);
    }

    #[test]
    fn test_consecutive_failures_block() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        for _ in 0..5 {
            manager.record_failure("bad-source.com");
        }

        let source = manager.get_source("bad-source.com").unwrap();
        assert!(source.is_blocked);
        assert!(source.block_until.is_some());
        assert!(manager.is_blocked("bad-source.com"));
    }

    #[test]
    fn test_unblock_source() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        for _ in 0..5 {
            manager.record_failure("blocked.com");
        }

        assert!(manager.is_blocked("blocked.com"));
        assert!(manager.unblock_source("blocked.com"));
        assert!(!manager.is_blocked("blocked.com"));

        let source = manager.get_source("blocked.com").unwrap();
        assert_eq!(source.consecutive_failures, 0);
    }

    #[test]
    fn test_recommend_source() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        // Record some successes to build reputation
        for _ in 0..10 {
            manager.record_success("good.com", 1_000_000, 10_000_000.0);
        }
        for _ in 0..5 {
            manager.record_failure("bad.com");
        }

        let candidates = vec!["bad.com".to_string(), "good.com".to_string()];
        let recommended = manager.recommend_source(&candidates);
        assert_eq!(recommended, Some("good.com".to_string()));
    }

    #[test]
    fn test_recommend_source_all_blocked() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        for _ in 0..5 {
            manager.record_failure("blocked1.com");
            manager.record_failure("blocked2.com");
        }

        let candidates = vec!["blocked1.com".to_string(), "blocked2.com".to_string()];
        let recommended = manager.recommend_source(&candidates);
        // Should return first candidate even if all blocked
        assert_eq!(recommended, Some("blocked1.com".to_string()));
    }

    #[test]
    fn test_recommend_source_empty() {
        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        let candidates: Vec<String> = vec![];
        assert_eq!(manager.recommend_source(&candidates), None);
    }

    #[test]
    fn test_summary_statistics() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        // Create sources in different tiers
        for _ in 0..10 {
            manager.record_success("excellent.com", 1_000_000, 10_000_000.0);
        }
        for _ in 0..5 {
            manager.record_failure("poor.com");
        }

        let summary = manager.get_summary();
        assert_eq!(summary.total_sources, 2);
        assert!(summary.excellent_count > 0 || summary.good_count > 0);
        assert!(summary.poor_count > 0 || summary.unreliable_count > 0);
    }

    #[test]
    fn test_remove_source() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        manager.record_success("test.com", 1000, 1000.0);
        assert!(manager.get_source("test.com").is_some());

        assert!(manager.remove_source("test.com"));
        assert!(manager.get_source("test.com").is_none());
    }

    #[test]
    fn test_clear_all() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        manager.record_success("test1.com", 1000, 1000.0);
        manager.record_success("test2.com", 1000, 1000.0);

        manager.clear_all();
        assert_eq!(manager.get_all_sources().len(), 0);
    }

    #[test]
    fn test_max_sources_enforcement() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.max_sources = 5;

        // Add 10 sources
        for i in 0..10 {
            manager.record_success(&format!("source{}.com", i), 1000, 1000.0);
        }

        // Should only keep 5
        assert_eq!(manager.get_all_sources().len(), 5);
    }

    #[test]
    fn test_disabled_tracking() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.enabled = false;

        manager.record_success("test.com", 1000, 1000.0);
        assert!(manager.get_source("test.com").is_none());
    }

    #[test]
    fn test_speed_ema() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.speed_ema_alpha = 0.5;

        manager.record_success("test.com", 1_000_000, 10_000_000.0);
        let source = manager.get_source("test.com").unwrap();
        let first_speed = source.avg_speed_bps;

        manager.record_success("test.com", 1_000_000, 20_000_000.0);
        let source = manager.get_source("test.com").unwrap();

        // EMA should be between old and new
        assert!(source.avg_speed_bps > first_speed);
        assert!(source.avg_speed_bps < 20_000_000.0);
    }

    #[test]
    fn test_format_summary() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        for _ in 0..10 {
            manager.record_success("good.com", 1_000_000, 10_000_000.0);
        }

        let summary = manager.get_summary();
        let formatted = manager.format_summary(&summary);

        assert!(formatted.contains("Source Quality Summary"));
        assert!(formatted.contains("good.com"));
    }

    #[tokio::test]
    async fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let mut manager = SourceQualityManager::new(data_dir.clone());
        manager.record_success("test.com", 1_000_000, 5_000_000.0);
        manager.save_sources().await.unwrap();
        manager.save_config().await.unwrap();

        let mut loaded = SourceQualityManager::new(data_dir);
        loaded.load_sources().await.unwrap();
        loaded.load_config().await.unwrap();

        assert!(loaded.get_source("test.com").is_some());
        assert_eq!(loaded.get_config().min_samples, 3);
    }
}
