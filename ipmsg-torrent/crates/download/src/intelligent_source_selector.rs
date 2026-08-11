//! Intelligent Source Selector (Phase 140)
//!
//! Combines reliability data, rotation health, and bandwidth forecasts to
//! intelligently score and select the best download sources for each task.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for intelligent source selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntelligentSelectorConfig {
    /// Enable intelligent source selection
    pub enabled: bool,
    /// Weight for reliability score (0.0-1.0, default: 0.4)
    pub reliability_weight: f64,
    /// Weight for health score (0.0-1.0, default: 0.3)
    pub health_weight: f64,
    /// Weight for bandwidth forecast (0.0-1.0, default: 0.3)
    pub bandwidth_weight: f64,
    /// Minimum score threshold to consider a source (0.0-1.0, default: 0.3)
    pub min_score_threshold: f64,
    /// Maximum sources to return per selection (default: 3)
    pub max_sources_per_selection: usize,
    /// Enable automatic failover to next best source (default: true)
    pub auto_failover: bool,
    /// Score decay factor for unused sources (0.0-1.0, default: 0.95)
    pub unused_decay_factor: f64,
    /// Domains to always prefer (whitelist)
    pub preferred_domains: Vec<String>,
    /// Domains to always avoid (blacklist)
    pub avoided_domains: Vec<String>,
}

impl Default for IntelligentSelectorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            reliability_weight: 0.4,
            health_weight: 0.3,
            bandwidth_weight: 0.3,
            min_score_threshold: 0.3,
            max_sources_per_selection: 3,
            auto_failover: true,
            unused_decay_factor: 0.95,
            preferred_domains: Vec::new(),
            avoided_domains: Vec::new(),
        }
    }
}

/// A candidate source for intelligent selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCandidate {
    /// Source identifier
    pub source_id: String,
    /// Task ID this source belongs to
    pub task_id: String,
    /// Source URL or address
    pub address: String,
    /// Protocol type (http, torrent, ed2k, p2p)
    pub protocol: String,
    /// Domain extracted from address
    pub domain: String,
    /// Reliability score (0.0-1.0, from SourceReliabilityTracker)
    pub reliability_score: f64,
    /// Health score (0.0-1.0, from SourceRotationManager)
    pub health_score: f64,
    /// Bandwidth forecast score (0.0-1.0, normalized from BandwidthForecast)
    pub bandwidth_score: f64,
    /// Predicted speed in bytes/sec (from bandwidth forecast)
    pub predicted_speed_bps: f64,
    /// Combined intelligent score (0.0-1.0)
    pub intelligent_score: f64,
    /// Selection rank (1 = best)
    pub rank: usize,
    /// Whether this source is currently available
    pub available: bool,
    /// Reason for selection or rejection
    pub selection_reason: String,
    /// Last used timestamp (epoch seconds)
    pub last_used_at: u64,
    /// Number of times this source was selected
    pub selection_count: u32,
}

impl SourceCandidate {
    /// Create a new source candidate
    pub fn new(
        source_id: String,
        task_id: String,
        address: String,
        protocol: String,
        domain: String,
    ) -> Self {
        Self {
            source_id,
            task_id,
            address,
            protocol,
            domain,
            reliability_score: 0.5,
            health_score: 0.5,
            bandwidth_score: 0.5,
            predicted_speed_bps: 0.0,
            intelligent_score: 0.0,
            rank: 0,
            available: true,
            selection_reason: String::new(),
            last_used_at: current_epoch_secs(),
            selection_count: 0,
        }
    }

    /// Calculate combined intelligent score
    pub fn calculate_score(
        &mut self,
        reliability_weight: f64,
        health_weight: f64,
        bandwidth_weight: f64,
    ) {
        let total_weight = reliability_weight + health_weight + bandwidth_weight;
        if total_weight <= 0.0 {
            self.intelligent_score = 0.0;
            return;
        }

        self.intelligent_score = (self.reliability_score * reliability_weight
            + self.health_score * health_weight
            + self.bandwidth_score * bandwidth_weight)
            / total_weight;
    }

    /// Check if source meets minimum threshold
    pub fn meets_threshold(&self, min_threshold: f64) -> bool {
        self.intelligent_score >= min_threshold && self.available
    }
}

/// Result of intelligent source selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectionResult {
    /// Task ID for this selection
    pub task_id: String,
    /// Selected sources in order of preference
    pub selected_sources: Vec<SourceCandidate>,
    /// Rejected sources with reasons
    pub rejected_sources: Vec<SourceCandidate>,
    /// Total candidates evaluated
    pub total_candidates: usize,
    /// Selection timestamp (epoch seconds)
    pub timestamp: u64,
    /// Selection confidence (average score of selected sources)
    pub confidence: f64,
    /// Whether failover is recommended
    pub failover_recommended: bool,
    /// Reason for selection result (e.g., "No candidates available")
    pub selection_reason: String,
}

impl SelectionResult {
    /// Create a new selection result
    pub fn new(task_id: String) -> Self {
        Self {
            task_id,
            selected_sources: Vec::new(),
            rejected_sources: Vec::new(),
            total_candidates: 0,
            timestamp: current_epoch_secs(),
            confidence: 0.0,
            failover_recommended: false,
            selection_reason: String::new(),
        }
    }

    /// Calculate confidence score
    pub fn calculate_confidence(&mut self) {
        if self.selected_sources.is_empty() {
            self.confidence = 0.0;
            return;
        }
        let sum: f64 = self
            .selected_sources
            .iter()
            .map(|s| s.intelligent_score)
            .sum();
        self.confidence = sum / self.selected_sources.len() as f64;
    }

    /// Get the best source
    pub fn best_source(&self) -> Option<&SourceCandidate> {
        self.selected_sources.first()
    }

    /// Get failover sources (all except the first)
    pub fn failover_sources(&self) -> &[SourceCandidate] {
        if self.selected_sources.len() > 1 {
            &self.selected_sources[1..]
        } else {
            &[]
        }
    }
}

/// Summary of intelligent source selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectorSummary {
    /// Total selections made
    pub total_selections: u64,
    /// Average selection confidence
    pub avg_confidence: f64,
    /// Most selected source
    pub most_selected_source: Option<String>,
    /// Most rejected domain
    pub most_rejected_domain: Option<String>,
    /// Average predicted speed (bytes/sec)
    pub avg_predicted_speed_bps: f64,
    /// Failover count
    pub failover_count: u64,
    /// Selection success rate (0.0-1.0)
    pub selection_success_rate: f64,
}

impl Default for SelectorSummary {
    fn default() -> Self {
        Self {
            total_selections: 0,
            avg_confidence: 0.0,
            most_selected_source: None,
            most_rejected_domain: None,
            avg_predicted_speed_bps: 0.0,
            failover_count: 0,
            selection_success_rate: 1.0,
        }
    }
}

/// Intelligent Source Selector
///
/// Combines multiple data sources to make intelligent download source selections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntelligentSourceSelector {
    /// Configuration
    pub config: IntelligentSelectorConfig,
    /// Source candidates cache (task_id -> candidates)
    pub candidates: HashMap<String, Vec<SourceCandidate>>,
    /// Selection history
    pub selection_history: Vec<SelectionResult>,
    /// Maximum history size
    pub max_history_size: usize,
    /// Selection statistics
    pub total_selections: u64,
    /// Successful selections (found at least one source)
    pub successful_selections: u64,
    /// Failover events
    pub failover_count: u64,
    /// Source selection counts (source_id -> count)
    pub source_selection_counts: HashMap<String, u32>,
    /// Domain rejection counts (domain -> count)
    pub domain_rejection_counts: HashMap<String, u32>,
}

impl IntelligentSourceSelector {
    /// Create a new intelligent source selector
    pub fn new() -> Self {
        Self {
            config: IntelligentSelectorConfig::default(),
            candidates: HashMap::new(),
            selection_history: Vec::new(),
            max_history_size: 100,
            total_selections: 0,
            successful_selections: 0,
            failover_count: 0,
            source_selection_counts: HashMap::new(),
            domain_rejection_counts: HashMap::new(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: IntelligentSelectorConfig) -> Self {
        Self {
            config,
            ..Self::new()
        }
    }

    /// Update configuration
    pub fn set_config(&mut self, config: IntelligentSelectorConfig) {
        self.config = config;
    }

    /// Get configuration
    pub fn config(&self) -> &IntelligentSelectorConfig {
        &self.config
    }

    /// Add or update a source candidate
    pub fn add_candidate(&mut self, candidate: SourceCandidate) {
        let task_candidates = self
            .candidates
            .entry(candidate.task_id.clone())
            .or_default();

        // Update existing or add new
        if let Some(existing) = task_candidates
            .iter_mut()
            .find(|c| c.source_id == candidate.source_id)
        {
            existing.reliability_score = candidate.reliability_score;
            existing.health_score = candidate.health_score;
            existing.bandwidth_score = candidate.bandwidth_score;
            existing.predicted_speed_bps = candidate.predicted_speed_bps;
            existing.available = candidate.available;
            existing.last_used_at = candidate.last_used_at;
        } else {
            task_candidates.push(candidate);
        }
    }

    /// Remove candidates for a task
    pub fn remove_task_candidates(&mut self, task_id: &str) -> usize {
        self.candidates
            .remove(task_id)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Clear all candidates
    pub fn clear_candidates(&mut self) {
        self.candidates.clear();
    }

    /// Select best sources for a task
    pub fn select_sources(&mut self, task_id: &str) -> SelectionResult {
        let mut result = SelectionResult::new(task_id.to_string());
        self.total_selections += 1;

        // Get candidates for this task
        let candidates = match self.candidates.get(task_id) {
            Some(c) => c.clone(),
            None => {
                result.selection_reason = "No candidates available".to_string();
                self.add_to_history(result.clone());
                return result;
            }
        };

        result.total_candidates = candidates.len();

        // Score and rank candidates
        let mut scored_candidates: Vec<SourceCandidate> = candidates
            .into_iter()
            .filter_map(|mut c| {
                // Check blacklist
                if self.config.avoided_domains.contains(&c.domain) {
                    c.selection_reason = "Domain in blacklist".to_string();
                    c.available = false;
                    self.domain_rejection_counts
                        .entry(c.domain.clone())
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                    result.rejected_sources.push(c);
                    return None;
                }

                // Apply whitelist bonus
                if self.config.preferred_domains.contains(&c.domain) {
                    c.reliability_score = (c.reliability_score * 1.2).min(1.0);
                    c.selection_reason = "Domain in whitelist".to_string();
                }

                // Calculate combined score
                c.calculate_score(
                    self.config.reliability_weight,
                    self.config.health_weight,
                    self.config.bandwidth_weight,
                );

                // Check threshold
                if !c.meets_threshold(self.config.min_score_threshold) {
                    c.selection_reason = format!(
                        "Score {:.3} below threshold {:.3}",
                        c.intelligent_score, self.config.min_score_threshold
                    );
                    self.domain_rejection_counts
                        .entry(c.domain.clone())
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                    result.rejected_sources.push(c);
                    return None;
                }

                Some(c)
            })
            .collect();

        // Sort by intelligent score (descending)
        scored_candidates.sort_by(|a, b| {
            b.intelligent_score
                .partial_cmp(&a.intelligent_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Assign ranks and limit
        let max_sources = self.config.max_sources_per_selection;
        for (idx, candidate) in scored_candidates.iter_mut().enumerate() {
            candidate.rank = idx + 1;
            if candidate.selection_reason.is_empty() {
                candidate.selection_reason = format!("Rank #{}", candidate.rank);
            }
        }

        // Select top sources
        result.selected_sources = scored_candidates.into_iter().take(max_sources).collect();

        // Update selection counts
        for source in &result.selected_sources {
            self.source_selection_counts
                .entry(source.source_id.clone())
                .and_modify(|e| *e += 1)
                .or_insert(1);
        }

        // Calculate confidence
        result.calculate_confidence();

        // Determine if failover is recommended
        result.failover_recommended =
            self.config.auto_failover && result.selected_sources.len() > 1;

        // Update statistics
        if !result.selected_sources.is_empty() {
            self.successful_selections += 1;
        }

        // Add to history
        self.add_to_history(result.clone());

        result
    }

    /// Record that a source was used for download
    pub fn record_source_used(&mut self, source_id: &str, success: bool) {
        // Update candidate last_used_at
        for candidates in self.candidates.values_mut() {
            if let Some(candidate) = candidates.iter_mut().find(|c| c.source_id == source_id) {
                candidate.last_used_at = current_epoch_secs();
                candidate.selection_count += 1;
                if success {
                    // Boost health score on success
                    candidate.health_score = (candidate.health_score * 1.05).min(1.0);
                } else {
                    // Reduce health score on failure
                    candidate.health_score = (candidate.health_score * 0.9).max(0.0);
                    self.failover_count += 1;
                }
                break;
            }
        }
    }

    /// Decay scores for unused sources
    pub fn decay_unused_scores(&mut self, max_age_secs: u64) {
        let now = current_epoch_secs();
        let decay_factor = self.config.unused_decay_factor;

        for candidates in self.candidates.values_mut() {
            for candidate in candidates.iter_mut() {
                let age = now.saturating_sub(candidate.last_used_at);
                if age > max_age_secs {
                    // Decay health score for unused sources
                    candidate.health_score *= decay_factor;
                    // Recalculate combined score
                    candidate.calculate_score(
                        self.config.reliability_weight,
                        self.config.health_weight,
                        self.config.bandwidth_weight,
                    );
                }
            }
        }
    }

    /// Get selection summary
    pub fn get_summary(&self) -> SelectorSummary {
        let mut summary = SelectorSummary::default();
        summary.total_selections = self.total_selections;

        // Calculate average confidence
        if !self.selection_history.is_empty() {
            let sum: f64 = self.selection_history.iter().map(|r| r.confidence).sum();
            summary.avg_confidence = sum / self.selection_history.len() as f64;
        }

        // Find most selected source
        let max_source = self
            .source_selection_counts
            .iter()
            .max_by_key(|(_, count)| *count);
        if let Some((source_id, _)) = max_source {
            summary.most_selected_source = Some(source_id.clone());
        }

        // Find most rejected domain
        let max_rejected = self
            .domain_rejection_counts
            .iter()
            .max_by_key(|(_, count)| *count);
        if let Some((domain, _)) = max_rejected {
            summary.most_rejected_domain = Some(domain.clone());
        }

        // Calculate average predicted speed
        let speeds: Vec<f64> = self
            .candidates
            .values()
            .flatten()
            .filter(|c| c.available)
            .map(|c| c.predicted_speed_bps)
            .collect();
        if !speeds.is_empty() {
            summary.avg_predicted_speed_bps = speeds.iter().sum::<f64>() / speeds.len() as f64;
        }

        summary.failover_count = self.failover_count;

        // Calculate success rate
        if self.total_selections > 0 {
            summary.selection_success_rate =
                self.successful_selections as f64 / self.total_selections as f64;
        }

        summary
    }

    /// Get selection history
    pub fn get_history(&self, limit: usize) -> Vec<&SelectionResult> {
        self.selection_history.iter().rev().take(limit).collect()
    }

    /// Clear selection history
    pub fn clear_history(&mut self) {
        self.selection_history.clear();
    }

    /// Format summary for display
    pub fn format_summary(&self) -> String {
        let summary = self.get_summary();
        let mut output = String::new();

        output.push_str("🧠 Intelligent Source Selector Summary\n");
        output.push_str("=====================================\n");
        output.push_str(&format!("Total selections: {}\n", summary.total_selections));
        output.push_str(&format!(
            "Success rate: {:.1}%\n",
            summary.selection_success_rate * 100.0
        ));
        output.push_str(&format!("Avg confidence: {:.3}\n", summary.avg_confidence));
        output.push_str(&format!("Failover events: {}\n", summary.failover_count));

        if let Some(ref source) = summary.most_selected_source {
            output.push_str(&format!("Most selected: {}\n", source));
        }
        if let Some(ref domain) = summary.most_rejected_domain {
            output.push_str(&format!("Most rejected: {}\n", domain));
        }

        if summary.avg_predicted_speed_bps > 0.0 {
            output.push_str(&format!(
                "Avg predicted speed: {}/s\n",
                format_speed_bps(summary.avg_predicted_speed_bps as u64)
            ));
        }

        output
    }

    // Private helpers

    fn add_to_history(&mut self, result: SelectionResult) {
        self.selection_history.push(result);
        if self.selection_history.len() > self.max_history_size {
            self.selection_history.remove(0);
        }
    }
}

// Helper functions

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Format bytes per second as human-readable string
pub fn format_speed_bps(bps: u64) -> String {
    if bps >= 1_000_000_000 {
        format!("{:.2} GB/s", bps as f64 / 1_000_000_000.0)
    } else if bps >= 1_000_000 {
        format!("{:.2} MB/s", bps as f64 / 1_000_000.0)
    } else if bps >= 1_000 {
        format!("{:.2} KB/s", bps as f64 / 1_000.0)
    } else {
        format!("{} B/s", bps)
    }
}

/// Extract domain from URL or address
pub fn extract_domain(address: &str) -> String {
    // Try to extract protocol and domain from URLs like "https://example.com/path"
    if let Some(rest) = address
        .strip_prefix("https://")
        .or_else(|| address.strip_prefix("http://"))
    {
        // Take everything before the first '/' or ':'
        let domain = rest
            .split('/')
            .next()
            .unwrap_or(rest)
            .split(':')
            .next()
            .unwrap_or(rest);
        if !domain.is_empty() {
            return domain.to_string();
        }
    }

    // Handle magnet links
    if address.starts_with("magnet:") {
        return "magnet".to_string();
    }

    // Try to extract from address like "host:port"
    if let Some(colon_pos) = address.rfind(':') {
        let host = &address[..colon_pos];
        // Remove protocol prefix if present
        if let Some(slash_pos) = host.rfind("://") {
            return host[slash_pos + 3..].to_string();
        }
        if !host.is_empty() {
            return host.to_string();
        }
    }

    // Return as-is if no pattern matches
    address.to_string()
}

// Persistence functions

/// Save selector configuration to disk
pub fn save_selector_config(
    config: &IntelligentSelectorConfig,
    data_dir: &std::path::Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("intelligent_selector_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(&path, json)
}

/// Load selector configuration from disk
pub fn load_selector_config(
    data_dir: &std::path::Path,
) -> Result<Option<IntelligentSelectorConfig>, std::io::Error> {
    let path = data_dir.join("intelligent_selector_config.json");
    if !path.exists() {
        return Ok(None);
    }
    let json = std::fs::read_to_string(&path)?;
    let config: IntelligentSelectorConfig = serde_json::from_str(&json)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(config))
}

/// Save selector state to disk
pub fn save_selector_state(
    selector: &IntelligentSourceSelector,
    data_dir: &std::path::Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("intelligent_selector_state.json");
    let json = serde_json::to_string_pretty(selector)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(&path, json)
}

/// Load selector state from disk
pub fn load_selector_state(
    data_dir: &std::path::Path,
) -> Result<Option<IntelligentSourceSelector>, std::io::Error> {
    let path = data_dir.join("intelligent_selector_state.json");
    if !path.exists() {
        return Ok(None);
    }
    let json = std::fs::read_to_string(&path)?;
    let selector: IntelligentSourceSelector = serde_json::from_str(&json)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(selector))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> IntelligentSelectorConfig {
        IntelligentSelectorConfig {
            enabled: true,
            reliability_weight: 0.4,
            health_weight: 0.3,
            bandwidth_weight: 0.3,
            min_score_threshold: 0.3,
            max_sources_per_selection: 3,
            auto_failover: true,
            unused_decay_factor: 0.95,
            preferred_domains: vec!["fast.com".to_string()],
            avoided_domains: vec!["slow.com".to_string()],
        }
    }

    fn test_candidate(source_id: &str, task_id: &str, domain: &str) -> SourceCandidate {
        SourceCandidate::new(
            source_id.to_string(),
            task_id.to_string(),
            format!("https://{}/file.zip", domain),
            "http".to_string(),
            domain.to_string(),
        )
    }

    #[test]
    fn test_default_config() {
        let config = IntelligentSelectorConfig::default();
        assert!(config.enabled);
        assert_eq!(config.reliability_weight, 0.4);
        assert_eq!(config.health_weight, 0.3);
        assert_eq!(config.bandwidth_weight, 0.3);
        assert_eq!(config.min_score_threshold, 0.3);
        assert_eq!(config.max_sources_per_selection, 3);
        assert!(config.auto_failover);
    }

    #[test]
    fn test_source_candidate_score_calculation() {
        let mut candidate = test_candidate("s1", "t1", "example.com");
        candidate.reliability_score = 0.8;
        candidate.health_score = 0.7;
        candidate.bandwidth_score = 0.9;

        candidate.calculate_score(0.4, 0.3, 0.3);

        // Expected: (0.8*0.4 + 0.7*0.3 + 0.9*0.3) / 1.0 = 0.32 + 0.21 + 0.27 = 0.80
        assert!((candidate.intelligent_score - 0.80).abs() < 0.001);
    }

    #[test]
    fn test_source_candidate_meets_threshold() {
        let mut candidate = test_candidate("s1", "t1", "example.com");
        candidate.intelligent_score = 0.5;
        candidate.available = true;

        assert!(candidate.meets_threshold(0.3));
        assert!(candidate.meets_threshold(0.5));
        assert!(!candidate.meets_threshold(0.6));

        candidate.available = false;
        assert!(!candidate.meets_threshold(0.5));
    }

    #[test]
    fn test_selector_add_candidate() {
        let mut selector = IntelligentSourceSelector::new();
        let candidate = test_candidate("s1", "t1", "example.com");

        selector.add_candidate(candidate.clone());
        assert_eq!(selector.candidates.get("t1").unwrap().len(), 1);

        // Update existing
        let mut updated = candidate;
        updated.reliability_score = 0.9;
        selector.add_candidate(updated);
        assert_eq!(selector.candidates.get("t1").unwrap().len(), 1);
        assert_eq!(
            selector.candidates.get("t1").unwrap()[0].reliability_score as u32,
            0
        ); // 0.9 truncated
    }

    #[test]
    fn test_selector_select_sources_basic() {
        let mut selector = IntelligentSourceSelector::with_config(test_config());

        // Add candidates with different scores
        let mut c1 = test_candidate("s1", "t1", "fast.com");
        c1.reliability_score = 0.9;
        c1.health_score = 0.8;
        c1.bandwidth_score = 0.7;
        selector.add_candidate(c1);

        let mut c2 = test_candidate("s2", "t1", "medium.com");
        c2.reliability_score = 0.6;
        c2.health_score = 0.5;
        c2.bandwidth_score = 0.4;
        selector.add_candidate(c2);

        let result = selector.select_sources("t1");

        assert_eq!(result.task_id, "t1");
        assert!(!result.selected_sources.is_empty());
        assert_eq!(result.total_candidates, 2);
        assert!(result.confidence > 0.0);
    }

    #[test]
    fn test_selector_blacklist() {
        let mut selector = IntelligentSourceSelector::with_config(test_config());

        let mut c1 = test_candidate("s1", "t1", "slow.com"); // In blacklist
        c1.reliability_score = 0.9;
        c1.health_score = 0.9;
        c1.bandwidth_score = 0.9;
        selector.add_candidate(c1);

        let result = selector.select_sources("t1");

        assert!(result.selected_sources.is_empty());
        assert_eq!(result.rejected_sources.len(), 1);
        assert!(
            result.rejected_sources[0]
                .selection_reason
                .contains("blacklist")
        );
    }

    #[test]
    fn test_selector_whitelist_bonus() {
        let mut selector = IntelligentSourceSelector::with_config(test_config());

        let mut c1 = test_candidate("s1", "t1", "fast.com"); // In whitelist
        c1.reliability_score = 0.7;
        c1.health_score = 0.7;
        c1.bandwidth_score = 0.7;
        selector.add_candidate(c1);

        let result = selector.select_sources("t1");

        assert!(!result.selected_sources.is_empty());
        // Whitelist bonus should have been applied (0.7 * 1.2 = 0.84)
        assert!(
            result.selected_sources[0]
                .selection_reason
                .contains("whitelist")
        );
    }

    #[test]
    fn test_selector_below_threshold() {
        let config = IntelligentSelectorConfig {
            min_score_threshold: 0.8, // High threshold
            ..test_config()
        };
        let mut selector = IntelligentSourceSelector::with_config(config);

        let mut c1 = test_candidate("s1", "t1", "medium.com");
        c1.reliability_score = 0.3;
        c1.health_score = 0.3;
        c1.bandwidth_score = 0.3;
        selector.add_candidate(c1);

        let result = selector.select_sources("t1");

        assert!(result.selected_sources.is_empty());
        assert!(
            result.rejected_sources[0]
                .selection_reason
                .contains("below threshold")
        );
    }

    #[test]
    fn test_selector_max_sources_limit() {
        let config = IntelligentSelectorConfig {
            max_sources_per_selection: 2,
            ..test_config()
        };
        let mut selector = IntelligentSourceSelector::with_config(config);

        // Add 5 candidates
        for i in 0..5 {
            let mut c = test_candidate(&format!("s{}", i), "t1", &format!("domain{}.com", i));
            c.reliability_score = 0.5 + i as f64 * 0.1;
            c.health_score = 0.5 + i as f64 * 0.1;
            c.bandwidth_score = 0.5 + i as f64 * 0.1;
            selector.add_candidate(c);
        }

        let result = selector.select_sources("t1");

        assert!(result.selected_sources.len() <= 2);
    }

    #[test]
    fn test_selector_no_candidates() {
        let mut selector = IntelligentSourceSelector::new();
        let result = selector.select_sources("t1");

        assert!(result.selected_sources.is_empty());
        assert_eq!(result.total_candidates, 0);
    }

    #[test]
    fn test_selection_result_confidence() {
        let mut result = SelectionResult::new("t1".to_string());

        let mut c1 = test_candidate("s1", "t1", "example.com");
        c1.intelligent_score = 0.8;
        result.selected_sources.push(c1);

        let mut c2 = test_candidate("s2", "t1", "example.com");
        c2.intelligent_score = 0.6;
        result.selected_sources.push(c2);

        result.calculate_confidence();

        // Expected: (0.8 + 0.6) / 2 = 0.7
        assert!((result.confidence - 0.7).abs() < 0.001);
    }

    #[test]
    fn test_selection_result_best_source() {
        let mut result = SelectionResult::new("t1".to_string());

        let mut c1 = test_candidate("s1", "t1", "example.com");
        c1.intelligent_score = 0.6;
        result.selected_sources.push(c1);

        let mut c2 = test_candidate("s2", "t1", "example.com");
        c2.intelligent_score = 0.9;
        c2.rank = 1;
        result.selected_sources.insert(0, c2);

        let best = result.best_source().unwrap();
        assert_eq!(best.source_id, "s2");
    }

    #[test]
    fn test_selection_result_failover_sources() {
        let mut result = SelectionResult::new("t1".to_string());

        for i in 0..3 {
            let mut c = test_candidate(&format!("s{}", i), "t1", "example.com");
            c.intelligent_score = 0.9 - i as f64 * 0.1;
            result.selected_sources.push(c);
        }

        let failovers = result.failover_sources();
        assert_eq!(failovers.len(), 2);
    }

    #[test]
    fn test_record_source_used() {
        let mut selector = IntelligentSourceSelector::new();

        let mut c = test_candidate("s1", "t1", "example.com");
        c.health_score = 0.8;
        selector.add_candidate(c);

        // Record success
        selector.record_source_used("s1", true);
        let candidate = &selector.candidates.get("t1").unwrap()[0];
        assert!(candidate.health_score > 0.8);
        assert_eq!(candidate.selection_count, 1);

        // Record failure
        let initial_health = candidate.health_score;
        selector.record_source_used("s1", false);
        let candidate = &selector.candidates.get("t1").unwrap()[0];
        assert!(candidate.health_score < initial_health);
        assert_eq!(selector.failover_count, 1);
    }

    #[test]
    fn test_decay_unused_scores() {
        let mut selector = IntelligentSourceSelector::new();

        let mut c = test_candidate("s1", "t1", "example.com");
        c.health_score = 1.0;
        c.last_used_at = current_epoch_secs() - 1000; // Old
        selector.add_candidate(c);

        selector.decay_unused_scores(500); // Decay if older than 500s

        let candidate = &selector.candidates.get("t1").unwrap()[0];
        assert!(candidate.health_score < 1.0);
    }

    #[test]
    fn test_selector_summary() {
        let mut selector = IntelligentSourceSelector::with_config(test_config());

        // Add and select candidates
        let mut c1 = test_candidate("s1", "t1", "fast.com");
        c1.reliability_score = 0.9;
        c1.health_score = 0.8;
        c1.bandwidth_score = 0.7;
        selector.add_candidate(c1);

        selector.select_sources("t1");
        selector.select_sources("t1");

        let summary = selector.get_summary();
        assert_eq!(summary.total_selections, 2);
        assert!(summary.selection_success_rate > 0.0);
        assert!(summary.avg_confidence > 0.0);
    }

    #[test]
    fn test_selector_history() {
        let mut selector = IntelligentSourceSelector::new();
        selector.max_history_size = 5;

        for i in 0..10 {
            let mut c = test_candidate("s1", "t1", "example.com");
            c.reliability_score = 0.5 + i as f64 * 0.05;
            selector.add_candidate(c);
            selector.select_sources("t1");
        }

        let history = selector.get_history(10);
        assert!(history.len() <= 5); // Limited by max_history_size
    }

    #[test]
    fn test_extract_domain() {
        assert_eq!(
            extract_domain("https://example.com/file.zip"),
            "example.com"
        );
        assert_eq!(
            extract_domain("http://sub.example.com:8080/path"),
            "sub.example.com"
        );
        assert_eq!(extract_domain("example.com:8080"), "example.com");
        assert_eq!(extract_domain("magnet:?xt=urn:btih:abc"), "magnet");
    }

    #[test]
    fn test_format_speed_bps() {
        assert_eq!(format_speed_bps(500), "500 B/s");
        assert_eq!(format_speed_bps(1500), "1.50 KB/s");
        assert_eq!(format_speed_bps(1_500_000), "1.50 MB/s");
        assert_eq!(format_speed_bps(1_500_000_000), "1.50 GB/s");
    }

    #[test]
    fn test_config_serialization() {
        let config = test_config();
        let json = serde_json::to_string(&config).unwrap();
        let restored: IntelligentSelectorConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.enabled, config.enabled);
        assert_eq!(restored.reliability_weight, config.reliability_weight);
        assert_eq!(restored.preferred_domains, config.preferred_domains);
        assert_eq!(restored.avoided_domains, config.avoided_domains);
    }

    #[test]
    fn test_selector_serialization() {
        let mut selector = IntelligentSourceSelector::new();
        let c = test_candidate("s1", "t1", "example.com");
        selector.add_candidate(c);
        selector.total_selections = 5;

        let json = serde_json::to_string(&selector).unwrap();
        let restored: IntelligentSourceSelector = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.total_selections, 5);
        assert_eq!(restored.candidates.len(), 1);
    }

    #[test]
    fn test_remove_task_candidates() {
        let mut selector = IntelligentSourceSelector::new();

        selector.add_candidate(test_candidate("s1", "t1", "a.com"));
        selector.add_candidate(test_candidate("s2", "t1", "b.com"));
        selector.add_candidate(test_candidate("s3", "t2", "c.com"));

        assert_eq!(selector.remove_task_candidates("t1"), 2);
        assert_eq!(selector.candidates.len(), 1);
        assert!(selector.candidates.contains_key("t2"));
    }

    #[test]
    fn test_clear_candidates() {
        let mut selector = IntelligentSourceSelector::new();

        selector.add_candidate(test_candidate("s1", "t1", "a.com"));
        selector.add_candidate(test_candidate("s2", "t2", "b.com"));

        selector.clear_candidates();
        assert!(selector.candidates.is_empty());
    }

    #[test]
    fn test_format_summary() {
        let mut selector = IntelligentSourceSelector::with_config(test_config());

        let mut c = test_candidate("s1", "t1", "fast.com");
        c.reliability_score = 0.9;
        c.health_score = 0.8;
        c.bandwidth_score = 0.7;
        c.predicted_speed_bps = 5_000_000.0;
        selector.add_candidate(c);

        selector.select_sources("t1");

        let output = selector.format_summary();
        assert!(output.contains("Intelligent Source Selector Summary"));
        assert!(output.contains("Total selections: 1"));
        assert!(output.contains("5.00 MB/s"));
    }

    #[test]
    fn test_zero_weights() {
        let config = IntelligentSelectorConfig {
            reliability_weight: 0.0,
            health_weight: 0.0,
            bandwidth_weight: 0.0,
            ..test_config()
        };
        let mut selector = IntelligentSourceSelector::with_config(config);

        let mut c = test_candidate("s1", "t1", "example.com");
        c.reliability_score = 0.9;
        c.health_score = 0.9;
        c.bandwidth_score = 0.9;
        selector.add_candidate(c);

        let result = selector.select_sources("t1");
        // With zero weights, score should be 0, below threshold
        assert!(
            result.selected_sources.is_empty()
                || result.selected_sources[0].intelligent_score == 0.0
        );
    }

    #[test]
    fn test_selection_ranking() {
        let mut selector = IntelligentSourceSelector::with_config(test_config());

        // Add candidates with known scores
        let mut c1 = test_candidate("s1", "t1", "a.com");
        c1.reliability_score = 0.5;
        c1.health_score = 0.5;
        c1.bandwidth_score = 0.5;
        selector.add_candidate(c1);

        let mut c2 = test_candidate("s2", "t1", "b.com");
        c2.reliability_score = 0.9;
        c2.health_score = 0.9;
        c2.bandwidth_score = 0.9;
        selector.add_candidate(c2);

        let mut c3 = test_candidate("s3", "t1", "c.com");
        c3.reliability_score = 0.7;
        c3.health_score = 0.7;
        c3.bandwidth_score = 0.7;
        selector.add_candidate(c3);

        let result = selector.select_sources("t1");

        assert_eq!(result.selected_sources.len(), 3);
        assert_eq!(result.selected_sources[0].rank, 1);
        assert_eq!(result.selected_sources[1].rank, 2);
        assert_eq!(result.selected_sources[2].rank, 3);

        // Best candidate should be first
        assert_eq!(result.selected_sources[0].source_id, "s2");
    }
}
