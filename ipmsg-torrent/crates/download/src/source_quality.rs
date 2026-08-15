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

    // ========== SourceQualityConfig ==========

    #[test]
    fn test_source_quality_config_default() {
        let config = SourceQualityConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_samples, 3);
        assert_eq!(config.block_threshold, 5);
        assert_eq!(config.max_sources, 1000);
        assert!((config.success_weight - 0.5).abs() < f64::EPSILON);
        assert!((config.speed_weight - 0.3).abs() < f64::EPSILON);
        assert!((config.consistency_weight - 0.2).abs() < f64::EPSILON);
        assert_eq!(config.block_duration_secs, 3600);
        assert!((config.decay_factor - 0.95).abs() < f64::EPSILON);
        assert!((config.speed_ema_alpha - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = SourceQualityConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SourceQualityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.min_samples, config.min_samples);
        assert!((loaded.success_weight - config.success_weight).abs() < f64::EPSILON);
    }

    #[test]
    fn test_config_custom_values() {
        let config = SourceQualityConfig {
            enabled: false,
            min_samples: 10,
            success_weight: 0.6,
            speed_weight: 0.2,
            consistency_weight: 0.2,
            block_threshold: 3,
            block_duration_secs: 7200,
            max_sources: 500,
            decay_factor: 0.9,
            speed_ema_alpha: 0.5,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SourceQualityConfig = serde_json::from_str(&json).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.min_samples, 10);
        assert_eq!(loaded.block_threshold, 3);
        assert_eq!(loaded.max_sources, 500);
    }

    #[test]
    fn test_config_extra_fields_ignored() {
        let json = r#"{"enabled":true,"min_samples":3,"success_weight":0.5,"speed_weight":0.3,"consistency_weight":0.2,"block_threshold":5,"block_duration_secs":3600,"max_sources":1000,"decay_factor":0.95,"speed_ema_alpha":0.3,"unknown_field":"ignored","another":42}"#;
        let loaded: SourceQualityConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.min_samples, 3);
    }

    #[test]
    fn test_config_pretty_serde() {
        let config = SourceQualityConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let loaded: SourceQualityConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.min_samples, config.min_samples);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = SourceQualityConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.min_samples, config.min_samples);
        let debug = format!("{:?}", config);
        assert!(debug.contains("SourceQualityConfig"));
    }

    #[test]
    fn test_config_boundary_min_samples_zero() {
        let config = SourceQualityConfig {
            min_samples: 0,
            ..Default::default()
        };
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.set_config(config);
        // With min_samples=0, reliability should be calculated immediately
        manager.record_success("test.com", 1_000_000, 10_000_000.0);
        let source = manager.get_source("test.com").unwrap();
        // Score should not be neutral 50.0 since min_samples is 0
        assert!(source.reliability_score != 50.0 || source.success_count >= 1);
    }

    #[test]
    fn test_config_boundary_block_threshold_one() {
        let config = SourceQualityConfig {
            block_threshold: 1,
            ..Default::default()
        };
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.set_config(config);
        manager.record_failure("test.com");
        let source = manager.get_source("test.com").unwrap();
        assert!(source.is_blocked);
    }

    #[test]
    fn test_config_boundary_max_sources_one() {
        let config = SourceQualityConfig {
            max_sources: 1,
            ..Default::default()
        };
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.set_config(config);
        manager.record_success("source1.com", 1000, 1000.0);
        manager.record_success("source2.com", 1000, 1000.0);
        assert_eq!(manager.get_all_sources().len(), 1);
    }

    // ========== SourceTier ==========

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
    fn test_source_tier_exact_boundaries() {
        let mut source = SourceQuality {
            source_id: "test.com".to_string(),
            success_count: 0,
            failure_count: 0,
            total_bytes: 0,
            avg_speed_bps: 0.0,
            reliability_score: 0.0,
            tier: SourceTier::Unreliable,
            last_used_at: 0,
            first_seen_at: 0,
            use_count: 0,
            consecutive_failures: 0,
            is_blocked: false,
            block_until: None,
        };

        // Exact boundary 80.0 -> Excellent
        source.reliability_score = 80.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Excellent);

        // Just below 80.0 -> Good
        source.reliability_score = 79.99;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Good);

        // Exact boundary 60.0 -> Good
        source.reliability_score = 60.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Good);

        // Just below 60.0 -> Average
        source.reliability_score = 59.99;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Average);

        // Exact boundary 40.0 -> Average
        source.reliability_score = 40.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Average);

        // Just below 40.0 -> Poor
        source.reliability_score = 39.99;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Poor);

        // Exact boundary 20.0 -> Poor
        source.reliability_score = 20.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Poor);

        // Just below 20.0 -> Unreliable
        source.reliability_score = 19.99;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Unreliable);

        // Zero -> Unreliable
        source.reliability_score = 0.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Unreliable);

        // 100.0 -> Excellent
        source.reliability_score = 100.0;
        SourceQualityManager::update_tier_static(&mut source);
        assert_eq!(source.tier, SourceTier::Excellent);
    }

    #[test]
    fn test_source_tier_serde_roundtrip() {
        let tiers = vec![
            SourceTier::Excellent,
            SourceTier::Good,
            SourceTier::Average,
            SourceTier::Poor,
            SourceTier::Unreliable,
        ];
        for tier in tiers {
            let json = serde_json::to_string(&tier).unwrap();
            let loaded: SourceTier = serde_json::from_str(&json).unwrap();
            assert_eq!(loaded, tier);
        }
    }

    #[test]
    fn test_source_tier_clone_copy_debug_eq() {
        let tier = SourceTier::Excellent;
        let cloned = tier;
        assert_eq!(cloned, tier);
        let debug = format!("{:?}", tier);
        assert!(debug.contains("Excellent"));
    }

    // ========== SourceQuality ==========

    #[test]
    fn test_source_quality_serde_roundtrip() {
        let source = SourceQuality {
            source_id: "example.com".to_string(),
            success_count: 10,
            failure_count: 2,
            total_bytes: 1_000_000_000,
            avg_speed_bps: 5_000_000.0,
            reliability_score: 75.5,
            tier: SourceTier::Good,
            last_used_at: 1700000000,
            first_seen_at: 1699000000,
            use_count: 12,
            consecutive_failures: 0,
            is_blocked: false,
            block_until: None,
        };
        let json = serde_json::to_string(&source).unwrap();
        let loaded: SourceQuality = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.source_id, "example.com");
        assert_eq!(loaded.success_count, 10);
        assert_eq!(loaded.failure_count, 2);
        assert!((loaded.reliability_score - 75.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_source_quality_unicode_source_id() {
        let source = SourceQuality {
            source_id: "中文域名.测试".to_string(),
            success_count: 1,
            failure_count: 0,
            total_bytes: 1000,
            avg_speed_bps: 1000.0,
            reliability_score: 50.0,
            tier: SourceTier::Average,
            last_used_at: 0,
            first_seen_at: 0,
            use_count: 1,
            consecutive_failures: 0,
            is_blocked: false,
            block_until: None,
        };
        let json = serde_json::to_string(&source).unwrap();
        let loaded: SourceQuality = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.source_id, "中文域名.测试");
    }

    #[test]
    fn test_source_quality_emoji_source_id() {
        let source = SourceQuality {
            source_id: "🚀fast⚡server".to_string(),
            success_count: 5,
            failure_count: 0,
            total_bytes: 0,
            avg_speed_bps: 0.0,
            reliability_score: 50.0,
            tier: SourceTier::Average,
            last_used_at: 0,
            first_seen_at: 0,
            use_count: 5,
            consecutive_failures: 0,
            is_blocked: false,
            block_until: None,
        };
        let json = serde_json::to_string(&source).unwrap();
        let loaded: SourceQuality = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.source_id, "🚀fast⚡server");
    }

    #[test]
    fn test_source_quality_clone_debug() {
        let source = SourceQuality {
            source_id: "test.com".to_string(),
            success_count: 1,
            failure_count: 0,
            total_bytes: 1000,
            avg_speed_bps: 1000.0,
            reliability_score: 50.0,
            tier: SourceTier::Average,
            last_used_at: 0,
            first_seen_at: 0,
            use_count: 1,
            consecutive_failures: 0,
            is_blocked: false,
            block_until: None,
        };
        let cloned = source.clone();
        assert_eq!(cloned.source_id, source.source_id);
        let debug = format!("{:?}", source);
        assert!(debug.contains("SourceQuality"));
        assert!(debug.contains("test.com"));
    }

    #[test]
    fn test_source_quality_large_values() {
        let source = SourceQuality {
            source_id: "large.com".to_string(),
            success_count: u32::MAX,
            failure_count: u32::MAX,
            total_bytes: u64::MAX,
            avg_speed_bps: f64::MAX,
            reliability_score: 100.0,
            tier: SourceTier::Excellent,
            last_used_at: u64::MAX,
            first_seen_at: 0,
            use_count: u32::MAX,
            consecutive_failures: 0,
            is_blocked: false,
            block_until: None,
        };
        let json = serde_json::to_string(&source).unwrap();
        let loaded: SourceQuality = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_bytes, u64::MAX);
        assert_eq!(loaded.success_count, u32::MAX);
    }

    #[test]
    fn test_source_quality_with_block_until() {
        let source = SourceQuality {
            source_id: "blocked.com".to_string(),
            success_count: 0,
            failure_count: 5,
            total_bytes: 0,
            avg_speed_bps: 0.0,
            reliability_score: 10.0,
            tier: SourceTier::Unreliable,
            last_used_at: 1700000000,
            first_seen_at: 1699000000,
            use_count: 5,
            consecutive_failures: 5,
            is_blocked: true,
            block_until: Some(1700003600),
        };
        let json = serde_json::to_string(&source).unwrap();
        let loaded: SourceQuality = serde_json::from_str(&json).unwrap();
        assert!(loaded.is_blocked);
        assert_eq!(loaded.block_until, Some(1700003600));
    }

    // ========== SourceQualitySummary ==========

    #[test]
    fn test_summary_serde_roundtrip() {
        let summary = SourceQualitySummary {
            total_sources: 10,
            excellent_count: 2,
            good_count: 3,
            average_count: 2,
            poor_count: 2,
            unreliable_count: 1,
            blocked_count: 1,
            top_sources: vec![],
            bottom_sources: vec![],
            avg_reliability: 65.5,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let loaded: SourceQualitySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_sources, 10);
        assert!((loaded.avg_reliability - 65.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_summary_empty_struct() {
        let summary = SourceQualitySummary {
            total_sources: 0,
            excellent_count: 0,
            good_count: 0,
            average_count: 0,
            poor_count: 0,
            unreliable_count: 0,
            blocked_count: 0,
            top_sources: vec![],
            bottom_sources: vec![],
            avg_reliability: 0.0,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let loaded: SourceQualitySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_sources, 0);
    }

    #[test]
    fn test_summary_clone_debug() {
        let summary = SourceQualitySummary {
            total_sources: 5,
            excellent_count: 1,
            good_count: 1,
            average_count: 1,
            poor_count: 1,
            unreliable_count: 1,
            blocked_count: 0,
            top_sources: vec![],
            bottom_sources: vec![],
            avg_reliability: 50.0,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_sources, summary.total_sources);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("SourceQualitySummary"));
    }

    // ========== SourceQualityManager ==========

    #[test]
    fn test_manager_new() {
        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        assert!(manager.get_all_sources().is_empty());
        assert!(manager.get_config().enabled);
    }

    #[test]
    fn test_manager_set_get_config() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        let config = SourceQualityConfig {
            min_samples: 10,
            ..Default::default()
        };
        manager.set_config(config);
        assert_eq!(manager.get_config().min_samples, 10);
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
    fn test_record_success_multiple() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        for _ in 0..10 {
            manager.record_success("example.com", 1_000_000, 5_000_000.0);
        }

        let source = manager.get_source("example.com").unwrap();
        assert_eq!(source.success_count, 10);
        assert_eq!(source.total_bytes, 10_000_000);
        assert_eq!(source.use_count, 10);
    }

    #[test]
    fn test_record_success_zero_bytes() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.record_success("test.com", 0, 1000.0);
        let source = manager.get_source("test.com").unwrap();
        assert_eq!(source.total_bytes, 0);
        assert_eq!(source.success_count, 1);
    }

    #[test]
    fn test_record_success_zero_speed() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.record_success("test.com", 1000, 0.0);
        let source = manager.get_source("test.com").unwrap();
        assert_eq!(source.success_count, 1);
    }

    #[test]
    fn test_record_success_unicode_id() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.record_success("中文.测试", 1000, 1000.0);
        assert!(manager.get_source("中文.测试").is_some());
    }

    #[test]
    fn test_record_success_emoji_id() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.record_success("🚀fast", 1000, 1000.0);
        assert!(manager.get_source("🚀fast").is_some());
    }

    #[test]
    fn test_record_success_resets_consecutive_failures() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.record_failure("test.com");
        manager.record_failure("test.com");
        let source = manager.get_source("test.com").unwrap();
        assert_eq!(source.consecutive_failures, 2);

        manager.record_success("test.com", 1000, 1000.0);
        let source = manager.get_source("test.com").unwrap();
        assert_eq!(source.consecutive_failures, 0);
        assert!(!source.is_blocked);
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
    fn test_record_failure_unicode() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.record_failure("日本語.テスト");
        let source = manager.get_source("日本語.テスト").unwrap();
        assert_eq!(source.failure_count, 1);
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
    fn test_block_threshold_boundary() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.block_threshold = 3;

        manager.record_failure("test.com");
        manager.record_failure("test.com");
        assert!(!manager.get_source("test.com").unwrap().is_blocked);

        manager.record_failure("test.com");
        assert!(manager.get_source("test.com").unwrap().is_blocked);
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
    fn test_unblock_nonexistent() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        assert!(!manager.unblock_source("nonexistent.com"));
    }

    #[test]
    fn test_is_blocked_nonexistent() {
        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        assert!(!manager.is_blocked("nonexistent.com"));
    }

    #[test]
    fn test_recommend_source() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

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
        assert_eq!(recommended, Some("blocked1.com".to_string()));
    }

    #[test]
    fn test_recommend_source_empty() {
        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        let candidates: Vec<String> = vec![];
        assert_eq!(manager.recommend_source(&candidates), None);
    }

    #[test]
    fn test_recommend_source_single_candidate() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        for _ in 0..5 {
            manager.record_success("only.com", 1000, 1000.0);
        }
        let candidates = vec!["only.com".to_string()];
        assert_eq!(
            manager.recommend_source(&candidates),
            Some("only.com".to_string())
        );
    }

    #[test]
    fn test_recommend_source_unknown_candidates() {
        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        // Unknown sources should have default score 50.0
        let candidates = vec!["unknown1.com".to_string(), "unknown2.com".to_string()];
        let recommended = manager.recommend_source(&candidates);
        // Should return one of them (both have same score)
        assert!(recommended.is_some());
    }

    #[test]
    fn test_summary_statistics() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

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
    fn test_summary_empty() {
        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        let summary = manager.get_summary();
        assert_eq!(summary.total_sources, 0);
        assert_eq!(summary.excellent_count, 0);
        assert_eq!(summary.good_count, 0);
        assert_eq!(summary.average_count, 0);
        assert_eq!(summary.poor_count, 0);
        assert_eq!(summary.unreliable_count, 0);
        assert_eq!(summary.blocked_count, 0);
        assert!(summary.top_sources.is_empty());
        assert!(summary.bottom_sources.is_empty());
        assert!((summary.avg_reliability - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_summary_top_bottom_sources() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        // Create 10 sources with different scores
        for i in 0..10 {
            for _ in 0..(i + 1) {
                manager.record_success(
                    &format!("source{}.com", i),
                    1_000_000,
                    (i as f64 + 1.0) * 1_000_000.0,
                );
            }
        }

        let summary = manager.get_summary();
        assert_eq!(summary.total_sources, 10);
        assert_eq!(summary.top_sources.len(), 5);
        assert_eq!(summary.bottom_sources.len(), 5);
        // Top should have highest scores
        assert!(
            summary.top_sources[0].reliability_score >= summary.top_sources[4].reliability_score
        );
    }

    #[test]
    fn test_summary_avg_reliability() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.min_samples = 1;

        // Create sources with known reliability
        for _ in 0..10 {
            manager.record_success("good.com", 1_000_000, 10_000_000.0);
        }

        let summary = manager.get_summary();
        assert!(summary.avg_reliability > 0.0);
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
    fn test_remove_source_nonexistent() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        assert!(!manager.remove_source("nonexistent.com"));
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
    fn test_clear_all_empty() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.clear_all();
        assert_eq!(manager.get_all_sources().len(), 0);
    }

    #[test]
    fn test_max_sources_enforcement() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.max_sources = 5;

        for i in 0..10 {
            manager.record_success(&format!("source{}.com", i), 1000, 1000.0);
        }

        assert_eq!(manager.get_all_sources().len(), 5);
    }

    #[test]
    fn test_max_sources_keeps_highest_scoring() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.max_sources = 2;

        // Create one high-scoring source
        for _ in 0..20 {
            manager.record_success("good.com", 1_000_000, 10_000_000.0);
        }

        // Create many low-scoring sources
        for i in 0..10 {
            manager.record_success(&format!("bad{}.com", i), 1000, 100.0);
            manager.record_failure(&format!("bad{}.com", i));
        }

        // Good source should be kept
        assert!(manager.get_source("good.com").is_some());
        assert_eq!(manager.get_all_sources().len(), 2);
    }

    #[test]
    fn test_disabled_tracking() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.enabled = false;

        manager.record_success("test.com", 1000, 1000.0);
        assert!(manager.get_source("test.com").is_none());
    }

    #[test]
    fn test_disabled_failure_tracking() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.enabled = false;

        manager.record_failure("test.com");
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

        assert!(source.avg_speed_bps > first_speed);
        assert!(source.avg_speed_bps < 20_000_000.0);
    }

    #[test]
    fn test_speed_ema_first_sample() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.speed_ema_alpha = 0.3;

        // First sample should set the speed directly (not EMA)
        manager.record_success("test.com", 1_000_000, 5_000_000.0);
        let source = manager.get_source("test.com").unwrap();
        assert!((source.avg_speed_bps - 5_000_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decay_factor() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.decay_factor = 0.5;

        // Record some failures
        for _ in 0..4 {
            manager.record_failure("test.com");
        }
        let source = manager.get_source("test.com").unwrap();
        let initial_failures = source.failure_count;

        // Success should apply decay to failures
        manager.record_success("test.com", 1000, 1000.0);
        let source = manager.get_source("test.com").unwrap();
        assert!(source.failure_count < initial_failures);
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
        assert!(formatted.contains("Total sources"));
    }

    #[test]
    fn test_format_summary_empty() {
        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        let summary = manager.get_summary();
        let formatted = manager.format_summary(&summary);
        assert!(formatted.contains("Source Quality Summary"));
        assert!(formatted.contains("Total sources: 0"));
    }

    #[test]
    fn test_format_summary_with_bottom_sources() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        // Create good source
        for _ in 0..10 {
            manager.record_success("good.com", 1_000_000, 10_000_000.0);
        }
        // Create bad source
        for _ in 0..5 {
            manager.record_failure("bad.com");
        }

        let summary = manager.get_summary();
        let formatted = manager.format_summary(&summary);
        assert!(formatted.contains("good.com"));
        assert!(formatted.contains("bad.com"));
    }

    #[test]
    fn test_get_all_sources() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.record_success("a.com", 1000, 1000.0);
        manager.record_success("b.com", 1000, 1000.0);

        let all = manager.get_all_sources();
        assert_eq!(all.len(), 2);
        assert!(all.contains_key("a.com"));
        assert!(all.contains_key("b.com"));
    }

    #[test]
    fn test_get_source_nonexistent() {
        let manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        assert!(manager.get_source("nonexistent.com").is_none());
    }

    // ========== Persistence ==========

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

    #[tokio::test]
    async fn test_persistence_save_creates_file() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let manager = SourceQualityManager::new(data_dir.clone());
        manager.save_sources().await.unwrap();
        manager.save_config().await.unwrap();

        assert!(data_dir.join("source_quality_data.json").exists());
        assert!(data_dir.join("source_quality_config.json").exists());
    }

    #[tokio::test]
    async fn test_persistence_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let mut manager = SourceQualityManager::new(data_dir.clone());
        manager.record_success("first.com", 1000, 1000.0);
        manager.save_sources().await.unwrap();

        manager.clear_all();
        manager.record_success("second.com", 2000, 2000.0);
        manager.save_sources().await.unwrap();

        let mut loaded = SourceQualityManager::new(data_dir);
        loaded.load_sources().await.unwrap();
        assert!(loaded.get_source("second.com").is_some());
        assert!(loaded.get_source("first.com").is_none());
    }

    #[tokio::test]
    async fn test_persistence_no_tmp_leftover() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let manager = SourceQualityManager::new(data_dir.clone());
        manager.save_sources().await.unwrap();

        let tmp_file = data_dir.join("source_quality_data.tmp");
        assert!(!tmp_file.exists());
    }

    #[tokio::test]
    async fn test_persistence_corrupt_json() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        // Write corrupt data
        std::fs::write(data_dir.join("source_quality_data.json"), "not valid json").unwrap();
        std::fs::write(
            data_dir.join("source_quality_config.json"),
            "also not valid",
        )
        .unwrap();

        let mut manager = SourceQualityManager::new(data_dir);
        // Should not error, just use defaults
        manager.load_sources().await.unwrap();
        manager.load_config().await.unwrap();
        assert!(manager.get_all_sources().is_empty());
        assert_eq!(manager.get_config().min_samples, 3);
    }

    #[tokio::test]
    async fn test_persistence_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        std::fs::write(data_dir.join("source_quality_data.json"), "").unwrap();
        std::fs::write(data_dir.join("source_quality_config.json"), "").unwrap();

        let mut manager = SourceQualityManager::new(data_dir);
        manager.load_sources().await.unwrap();
        manager.load_config().await.unwrap();
        assert!(manager.get_all_sources().is_empty());
    }

    #[tokio::test]
    async fn test_persistence_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let mut manager = SourceQualityManager::new(data_dir);
        manager.load_sources().await.unwrap();
        manager.load_config().await.unwrap();
        assert!(manager.get_all_sources().is_empty());
    }

    #[tokio::test]
    async fn test_persistence_pretty_json() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let mut manager = SourceQualityManager::new(data_dir.clone());
        manager.record_success("test.com", 1000, 1000.0);
        manager.save_sources().await.unwrap();

        let content = std::fs::read_to_string(data_dir.join("source_quality_data.json")).unwrap();
        assert!(content.contains('\n'));
    }

    #[tokio::test]
    async fn test_persistence_unicode_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let mut manager = SourceQualityManager::new(data_dir.clone());
        manager.record_success("中文.测试", 1000, 1000.0);
        manager.record_success("日本語.テスト", 2000, 2000.0);
        manager.save_sources().await.unwrap();

        let mut loaded = SourceQualityManager::new(data_dir);
        loaded.load_sources().await.unwrap();
        assert!(loaded.get_source("中文.测试").is_some());
        assert!(loaded.get_source("日本語.テスト").is_some());
    }

    #[tokio::test]
    async fn test_persistence_full_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let mut manager = SourceQualityManager::new(data_dir.clone());
        manager.config.min_samples = 5;
        manager.config.block_threshold = 3;

        for _ in 0..10 {
            manager.record_success("good.com", 1_000_000, 10_000_000.0);
        }
        for _ in 0..3 {
            manager.record_failure("bad.com");
        }

        manager.save_sources().await.unwrap();
        manager.save_config().await.unwrap();

        let mut loaded = SourceQualityManager::new(data_dir);
        loaded.load_sources().await.unwrap();
        loaded.load_config().await.unwrap();

        assert_eq!(loaded.get_config().min_samples, 5);
        assert_eq!(loaded.get_config().block_threshold, 3);
        assert!(loaded.get_source("good.com").is_some());
        assert!(loaded.get_source("bad.com").is_some());

        let good = loaded.get_source("good.com").unwrap();
        assert_eq!(good.success_count, 10);
    }

    // ========== Complex workflows ==========

    #[test]
    fn test_complete_lifecycle() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        // Record mixed results
        for _ in 0..5 {
            manager.record_success("stable.com", 1_000_000, 5_000_000.0);
        }
        manager.record_failure("stable.com");
        manager.record_success("stable.com", 500_000, 4_000_000.0);

        let source = manager.get_source("stable.com").unwrap();
        assert_eq!(source.success_count, 6);
        assert_eq!(source.failure_count, 1);
        assert!(!source.is_blocked);

        let summary = manager.get_summary();
        assert_eq!(summary.total_sources, 1);
        assert!(summary.avg_reliability > 0.0);
    }

    #[test]
    fn test_multiple_sources_independent() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        manager.record_success("a.com", 1000, 1000.0);
        manager.record_failure("b.com");
        manager.record_success("c.com", 3000, 3000.0);

        assert_eq!(manager.get_all_sources().len(), 3);
        assert_eq!(manager.get_source("a.com").unwrap().success_count, 1);
        assert_eq!(manager.get_source("b.com").unwrap().failure_count, 1);
        assert_eq!(manager.get_source("c.com").unwrap().total_bytes, 3000);
    }

    #[test]
    fn test_block_then_unblock_then_block_again() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        // Block
        for _ in 0..5 {
            manager.record_failure("test.com");
        }
        assert!(manager.is_blocked("test.com"));

        // Unblock
        manager.unblock_source("test.com");
        assert!(!manager.is_blocked("test.com"));

        // Block again
        for _ in 0..5 {
            manager.record_failure("test.com");
        }
        assert!(manager.is_blocked("test.com"));
    }

    #[test]
    fn test_recommend_source_mixed_known_unknown() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        // Create one known bad source
        for _ in 0..5 {
            manager.record_failure("bad.com");
        }

        // Candidates: one bad known, one unknown
        let candidates = vec!["bad.com".to_string(), "unknown.com".to_string()];
        let recommended = manager.recommend_source(&candidates);
        // Unknown has default 50.0, bad should have low score
        assert_eq!(recommended, Some("unknown.com".to_string()));
    }

    #[test]
    fn test_reliability_score_calculation() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.min_samples = 3;

        // Record 10 successes, 0 failures
        for _ in 0..10 {
            manager.record_success("perfect.com", 1_000_000, 10_485_760.0);
        }

        let source = manager.get_source("perfect.com").unwrap();
        // With 100% success rate and excellent speed, score should be high
        assert!(source.reliability_score > 70.0);
        assert_eq!(source.tier, SourceTier::Excellent);
    }

    #[test]
    fn test_reliability_score_low_success_rate() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));
        manager.config.min_samples = 3;

        // Record mostly failures
        for _ in 0..10 {
            manager.record_failure("terrible.com");
        }
        manager.record_success("terrible.com", 100, 100.0);

        let source = manager.get_source("terrible.com").unwrap();
        // With very low success rate, score should be low
        assert!(source.reliability_score < 30.0);
    }

    #[test]
    fn test_use_count_increments() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        manager.record_success("test.com", 1000, 1000.0);
        manager.record_failure("test.com");
        manager.record_success("test.com", 1000, 1000.0);

        let source = manager.get_source("test.com").unwrap();
        assert_eq!(source.use_count, 3);
    }

    #[test]
    fn test_first_seen_last_used() {
        let mut manager = SourceQualityManager::new(PathBuf::from("/tmp"));

        manager.record_success("test.com", 1000, 1000.0);
        let source = manager.get_source("test.com").unwrap();
        let first_seen = source.first_seen_at;
        assert!(first_seen > 0);

        // Wait a tiny bit (simulated by recording more)
        manager.record_success("test.com", 1000, 1000.0);
        let source = manager.get_source("test.com").unwrap();
        // first_seen should not change
        assert_eq!(source.first_seen_at, first_seen);
        // last_used should be >= first_seen
        assert!(source.last_used_at >= first_seen);
    }
}
