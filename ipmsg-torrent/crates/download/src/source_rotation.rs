//! Download Source Rotation (Phase 95)
//!
//! When a download source (connection/peer/mirror) becomes unhealthy,
//! automatically try alternative sources and switch if they perform better.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of download for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSource {
    /// Unique source identifier
    pub source_id: String,
    /// Task ID this source belongs to
    pub task_id: String,
    /// Source URL or peer address
    pub address: String,
    /// Protocol type (http, torrent, ed2k, p2p)
    pub protocol: String,
    /// Whether this source is currently active
    pub active: bool,
    /// Whether this source is a backup/alternative
    pub is_backup: bool,
    /// Priority (lower = preferred)
    pub priority: u32,
    /// Health score (0.0 = terrible, 1.0 = perfect)
    pub health_score: f64,
    /// Total bytes downloaded from this source
    pub bytes_downloaded: u64,
    /// Number of times this source was tried
    pub attempt_count: u32,
    /// Number of successful connections
    pub success_count: u32,
    /// Number of failed connections
    pub failure_count: u32,
    /// Last used timestamp (epoch seconds)
    pub last_used_at: u64,
    /// Added timestamp (epoch seconds)
    pub added_at: u64,
    /// Cooldown until this source can be retried (epoch seconds)
    pub cooldown_until: u64,
    /// Tags for categorization
    pub tags: Vec<String>,
}

impl DownloadSource {
    /// Create a new download source
    pub fn new(source_id: String, task_id: String, address: String, protocol: String) -> Self {
        let now = current_epoch_secs();
        Self {
            source_id,
            task_id,
            address,
            protocol,
            active: true,
            is_backup: false,
            priority: 100,
            health_score: 1.0,
            bytes_downloaded: 0,
            attempt_count: 0,
            success_count: 0,
            failure_count: 0,
            last_used_at: now,
            added_at: now,
            cooldown_until: 0,
            tags: Vec::new(),
        }
    }

    /// Check if source is on cooldown
    pub fn is_on_cooldown(&self) -> bool {
        current_epoch_secs() < self.cooldown_until
    }

    /// Get success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.attempt_count == 0 {
            return 1.0;
        }
        self.success_count as f64 / self.attempt_count as f64
    }

    /// Record a successful download attempt
    pub fn record_success(&mut self, bytes: u64) {
        self.attempt_count += 1;
        self.success_count += 1;
        self.bytes_downloaded += bytes;
        self.last_used_at = current_epoch_secs();
        // Clear cooldown on success
        self.cooldown_until = 0;
    }

    /// Record a failed download attempt
    pub fn record_failure(&mut self, cooldown_secs: u64) {
        self.attempt_count += 1;
        self.failure_count += 1;
        self.last_used_at = current_epoch_secs();
        self.cooldown_until = current_epoch_secs() + cooldown_secs;
        // Reduce health score on failure
        self.health_score = (self.health_score * 0.7).max(0.0);
    }

    /// Update health score based on recent performance
    pub fn update_health_score(&mut self) {
        let success_rate = self.success_rate();
        // EMA blend: 60% historical, 40% current success rate
        self.health_score = self.health_score * 0.6 + success_rate * 0.4;
        self.health_score = self.health_score.clamp(0.0, 1.0);
    }
}

/// Configuration for source rotation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRotationConfig {
    /// Enable automatic source rotation
    pub enabled: bool,
    /// Health score below which a source is considered unhealthy
    pub unhealthy_threshold: f64,
    /// Health score above which a source is considered good
    pub healthy_threshold: f64,
    /// Maximum sources per task
    pub max_sources_per_task: usize,
    /// Cooldown time (seconds) after a failure before retrying the source
    pub failure_cooldown_secs: u64,
    /// Exponential backoff multiplier for consecutive failures
    pub backoff_multiplier: f64,
    /// Maximum cooldown time (seconds)
    pub max_cooldown_secs: u64,
    /// Minimum sources to keep active per task
    pub min_active_sources: usize,
    /// Auto-promote backup sources when primary fails
    pub auto_promote_backups: bool,
    /// Maximum sources to try in parallel
    pub max_parallel_sources: usize,
}

impl Default for SourceRotationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            unhealthy_threshold: 0.3,
            healthy_threshold: 0.7,
            max_sources_per_task: 20,
            failure_cooldown_secs: 30,
            backoff_multiplier: 2.0,
            max_cooldown_secs: 3600,
            min_active_sources: 1,
            auto_promote_backups: true,
            max_parallel_sources: 3,
        }
    }
}

/// Result of a source rotation decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotationDecision {
    /// Sources to activate
    pub sources_to_activate: Vec<String>,
    /// Sources to deactivate
    pub sources_to_deactivate: Vec<String>,
    /// Sources to put on cooldown
    pub sources_to_cooldown: Vec<String>,
    /// Reason for the decision
    pub reason: String,
}

/// Summary of source rotation for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRotationSummary {
    pub task_id: String,
    pub total_sources: usize,
    pub active_sources: usize,
    pub backup_sources: usize,
    pub unhealthy_sources: usize,
    pub sources_on_cooldown: usize,
    pub best_source: Option<String>,
    pub worst_source: Option<String>,
    pub total_bytes_downloaded: u64,
    pub overall_health: f64,
}

/// Manager for download source rotation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRotationManager {
    config: SourceRotationConfig,
    /// source_id -> DownloadSource
    sources: HashMap<String, DownloadSource>,
}

impl Default for SourceRotationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceRotationManager {
    /// Create a new manager with default config
    pub fn new() -> Self {
        Self {
            config: SourceRotationConfig::default(),
            sources: HashMap::new(),
        }
    }

    /// Create with custom config
    pub fn with_config(config: SourceRotationConfig) -> Self {
        Self {
            config,
            sources: HashMap::new(),
        }
    }

    /// Get current config
    pub fn config(&self) -> &SourceRotationConfig {
        &self.config
    }

    /// Update config
    pub fn set_config(&mut self, config: SourceRotationConfig) {
        self.config = config;
    }

    /// Add a new download source
    pub fn add_source(&mut self, source: DownloadSource) -> bool {
        if !self.config.enabled {
            return false;
        }

        let task_count = self
            .sources
            .values()
            .filter(|s| s.task_id == source.task_id)
            .count();
        if task_count >= self.config.max_sources_per_task {
            return false;
        }

        // Check for duplicate address within same task
        let duplicate = self
            .sources
            .values()
            .any(|s| s.task_id == source.task_id && s.address == source.address);
        if duplicate {
            return false;
        }

        let source_id = source.source_id.clone();
        self.sources.insert(source_id, source);
        true
    }

    /// Remove a source
    pub fn remove_source(&mut self, source_id: &str) -> Option<DownloadSource> {
        self.sources.remove(source_id)
    }

    /// Get a source by ID
    pub fn get_source(&self, source_id: &str) -> Option<&DownloadSource> {
        self.sources.get(source_id)
    }

    /// Record a successful download from a source
    pub fn record_source_success(&mut self, source_id: &str, bytes: u64) {
        if let Some(source) = self.sources.get_mut(source_id) {
            source.record_success(bytes);
            // Boost health score on success
            source.health_score = (source.health_score + 0.1).min(1.0);
        }
    }

    /// Record a failed download from a source
    pub fn record_source_failure(&mut self, source_id: &str) {
        if let Some(source) = self.sources.get_mut(source_id) {
            // Calculate cooldown with exponential backoff
            let consecutive_failures = source.failure_count;
            let cooldown = calculate_cooldown(
                consecutive_failures,
                self.config.failure_cooldown_secs,
                self.config.backoff_multiplier,
                self.config.max_cooldown_secs,
            );
            source.record_failure(cooldown);
        }
    }

    /// Get available sources for a task (not on cooldown, active)
    pub fn get_available_sources(&self, task_id: &str) -> Vec<&DownloadSource> {
        let mut available: Vec<&DownloadSource> = self
            .sources
            .values()
            .filter(|s| s.task_id == task_id && s.active && !s.is_on_cooldown())
            .collect();
        // Sort by health score descending, then priority ascending
        available.sort_by(|a, b| {
            b.health_score
                .partial_cmp(&a.health_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.priority.cmp(&b.priority))
        });
        available
    }

    /// Get the best available source for a task
    pub fn get_best_source(&self, task_id: &str) -> Option<&DownloadSource> {
        self.get_available_sources(task_id).first().copied()
    }

    /// Get all sources for a task
    pub fn get_task_sources(&self, task_id: &str) -> Vec<&DownloadSource> {
        let mut sources: Vec<&DownloadSource> = self
            .sources
            .values()
            .filter(|s| s.task_id == task_id)
            .collect();
        sources.sort_by_key(|a| a.priority);
        sources
    }

    /// Make a rotation decision for a task
    pub fn decide_rotation(&self, task_id: &str) -> RotationDecision {
        let task_sources = self.get_task_sources(task_id);
        let mut activate = Vec::new();
        let mut deactivate = Vec::new();
        let mut cooldown = Vec::new();
        let mut reasons = Vec::new();

        let active_count = task_sources.iter().filter(|s| s.active).count();
        let _unhealthy_count = task_sources
            .iter()
            .filter(|s| s.health_score < self.config.unhealthy_threshold)
            .count();

        // Deactivate unhealthy sources
        for source in &task_sources {
            if source.active && source.health_score < self.config.unhealthy_threshold {
                deactivate.push(source.source_id.clone());
                reasons.push(format!(
                    "Source {} health {:.2} below threshold {:.2}",
                    source.source_id, source.health_score, self.config.unhealthy_threshold
                ));
            }
        }

        // Activate healthy backups if we don't have enough active sources
        let remaining_active = active_count.saturating_sub(deactivate.len());
        if remaining_active < self.config.min_active_sources && self.config.auto_promote_backups {
            for source in &task_sources {
                if source.is_backup
                    && !source.active
                    && source.health_score >= self.config.healthy_threshold
                    && !source.is_on_cooldown()
                {
                    activate.push(source.source_id.clone());
                    reasons.push(format!(
                        "Promoting backup source {} (health {:.2})",
                        source.source_id, source.health_score
                    ));
                    if remaining_active + activate.len() >= self.config.min_active_sources {
                        break;
                    }
                }
            }
        }

        // Put sources on cooldown if they have too many failures
        for source in &task_sources {
            if source.failure_count > 0
                && source.success_rate() < 0.2
                && source.attempt_count >= 3
                && !source.is_on_cooldown()
            {
                cooldown.push(source.source_id.clone());
                reasons.push(format!(
                    "Source {} success rate {:.1}% too low, cooling down",
                    source.source_id,
                    source.success_rate() * 100.0
                ));
            }
        }

        let reason = if reasons.is_empty() {
            "No rotation needed".to_string()
        } else {
            reasons.join("; ")
        };

        RotationDecision {
            sources_to_activate: activate,
            sources_to_deactivate: deactivate,
            sources_to_cooldown: cooldown,
            reason,
        }
    }

    /// Apply a rotation decision
    pub fn apply_rotation(&mut self, decision: &RotationDecision) {
        for source_id in &decision.sources_to_activate {
            if let Some(source) = self.sources.get_mut(source_id) {
                source.active = true;
                source.is_backup = false;
            }
        }
        for source_id in &decision.sources_to_deactivate {
            if let Some(source) = self.sources.get_mut(source_id) {
                source.active = false;
            }
        }
        for source_id in &decision.sources_to_cooldown {
            let (failure_count, config_snapshot) = {
                if let Some(source) = self.sources.get(source_id) {
                    (
                        source.failure_count,
                        (
                            self.config.failure_cooldown_secs,
                            self.config.backoff_multiplier,
                            self.config.max_cooldown_secs,
                        ),
                    )
                } else {
                    continue;
                }
            };
            let cooldown = calculate_cooldown(
                failure_count,
                config_snapshot.0,
                config_snapshot.1,
                config_snapshot.2,
            );
            if let Some(source) = self.sources.get_mut(source_id) {
                source.cooldown_until = current_epoch_secs() + cooldown;
            }
        }
    }

    /// Remove all sources for a task
    pub fn remove_task_sources(&mut self, task_id: &str) -> usize {
        let ids: Vec<String> = self
            .sources
            .iter()
            .filter(|(_, s)| s.task_id == task_id)
            .map(|(id, _)| id.clone())
            .collect();
        let count = ids.len();
        for id in ids {
            self.sources.remove(&id);
        }
        count
    }

    /// Generate summary for a task
    pub fn get_task_summary(&self, task_id: &str) -> SourceRotationSummary {
        let task_sources = self.get_task_sources(task_id);
        let total = task_sources.len();
        let active = task_sources.iter().filter(|s| s.active).count();
        let backup = task_sources.iter().filter(|s| s.is_backup).count();
        let unhealthy = task_sources
            .iter()
            .filter(|s| s.health_score < self.config.unhealthy_threshold)
            .count();
        let on_cooldown = task_sources.iter().filter(|s| s.is_on_cooldown()).count();
        let total_bytes: u64 = task_sources.iter().map(|s| s.bytes_downloaded).sum();
        let overall_health = if total > 0 {
            task_sources.iter().map(|s| s.health_score).sum::<f64>() / total as f64
        } else {
            0.0
        };

        let best = task_sources
            .iter()
            .max_by(|a, b| {
                a.health_score
                    .partial_cmp(&b.health_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|s| s.source_id.clone());
        let worst = task_sources
            .iter()
            .min_by(|a, b| {
                a.health_score
                    .partial_cmp(&b.health_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|s| s.source_id.clone());

        SourceRotationSummary {
            task_id: task_id.to_string(),
            total_sources: total,
            active_sources: active,
            backup_sources: backup,
            unhealthy_sources: unhealthy,
            sources_on_cooldown: on_cooldown,
            best_source: best,
            worst_source: worst,
            total_bytes_downloaded: total_bytes,
            overall_health,
        }
    }

    /// Get overall summary across all tasks
    pub fn get_overall_summary(&self) -> HashMap<String, SourceRotationSummary> {
        let task_ids: Vec<String> = self
            .sources
            .values()
            .map(|s| s.task_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        task_ids
            .into_iter()
            .map(|tid| (tid.clone(), self.get_task_summary(&tid)))
            .collect()
    }

    /// Get source count
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Update health scores for all sources
    pub fn refresh_health_scores(&mut self) {
        for source in self.sources.values_mut() {
            source.update_health_score();
        }
    }
}

/// Calculate cooldown with exponential backoff
fn calculate_cooldown(
    consecutive_failures: u32,
    base_cooldown_secs: u64,
    backoff_multiplier: f64,
    max_cooldown_secs: u64,
) -> u64 {
    let multiplier = backoff_multiplier.powi(consecutive_failures as i32);
    let cooldown = (base_cooldown_secs as f64 * multiplier) as u64;
    cooldown.min(max_cooldown_secs)
}

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Persistence error type
#[derive(Debug)]
pub enum SourceRotationPersistenceError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for SourceRotationPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Json(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<std::io::Error> for SourceRotationPersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for SourceRotationPersistenceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Save source rotation config to disk (atomic write)
pub fn save_source_rotation_config(
    config: &SourceRotationConfig,
    data_dir: &std::path::Path,
) -> Result<(), SourceRotationPersistenceError> {
    let path = data_dir.join("source_rotation_config.json");
    let json = serde_json::to_string_pretty(config)?;
    let tmp_path = data_dir.join("source_rotation_config.json.tmp");
    std::fs::write(&tmp_path, json.as_bytes())?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load source rotation config from disk
pub fn load_source_rotation_config(
    data_dir: &std::path::Path,
) -> Result<Option<SourceRotationConfig>, SourceRotationPersistenceError> {
    let path = data_dir.join("source_rotation_config.json");
    if !path.exists() {
        return Ok(None);
    }
    let data = std::fs::read_to_string(&path)?;
    let config: SourceRotationConfig = serde_json::from_str(&data)?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_new() {
        let s = DownloadSource::new(
            "s1".into(),
            "t1".into(),
            "http://mirror1.com/file".into(),
            "http".into(),
        );
        assert_eq!(s.source_id, "s1");
        assert_eq!(s.task_id, "t1");
        assert!(s.active);
        assert!(!s.is_backup);
        assert_eq!(s.health_score, 1.0);
        assert_eq!(s.attempt_count, 0);
        assert_eq!(s.success_rate(), 1.0);
    }

    #[test]
    fn test_source_record_success() {
        let mut s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        s.record_success(1000);
        s.record_success(2000);
        assert_eq!(s.attempt_count, 2);
        assert_eq!(s.success_count, 2);
        assert_eq!(s.bytes_downloaded, 3000);
        assert_eq!(s.success_rate(), 1.0);
    }

    #[test]
    fn test_source_record_failure() {
        let mut s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        s.record_failure(30);
        assert_eq!(s.attempt_count, 1);
        assert_eq!(s.failure_count, 1);
        assert!(s.is_on_cooldown());
        assert!(s.health_score < 1.0);
    }

    #[test]
    fn test_source_success_rate() {
        let mut s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        s.record_success(100);
        s.record_failure(0);
        s.record_success(100);
        s.record_failure(0);
        assert_eq!(s.attempt_count, 4);
        assert_eq!(s.success_count, 2);
        assert!((s.success_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_source_cooldown() {
        let mut s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        assert!(!s.is_on_cooldown());
        s.record_failure(60);
        assert!(s.is_on_cooldown());
        // Clear cooldown on success
        s.record_success(100);
        assert!(!s.is_on_cooldown());
    }

    #[test]
    fn test_config_default() {
        let config = SourceRotationConfig::default();
        assert!(config.enabled);
        assert!((config.unhealthy_threshold - 0.3).abs() < 0.01);
        assert_eq!(config.max_sources_per_task, 20);
        assert_eq!(config.failure_cooldown_secs, 30);
        assert!(config.auto_promote_backups);
    }

    #[test]
    fn test_manager_add_remove() {
        let mut mgr = SourceRotationManager::new();
        let s = DownloadSource::new("s1".into(), "t1".into(), "addr1".into(), "http".into());
        assert!(mgr.add_source(s));
        assert_eq!(mgr.source_count(), 1);

        let removed = mgr.remove_source("s1");
        assert!(removed.is_some());
        assert_eq!(mgr.source_count(), 0);
    }

    #[test]
    fn test_manager_disabled() {
        let mut mgr = SourceRotationManager::new();
        mgr.set_config(SourceRotationConfig {
            enabled: false,
            ..Default::default()
        });
        let s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        assert!(!mgr.add_source(s));
    }

    #[test]
    fn test_manager_max_sources_per_task() {
        let mut mgr = SourceRotationManager::new();
        mgr.set_config(SourceRotationConfig {
            max_sources_per_task: 2,
            ..Default::default()
        });
        mgr.add_source(DownloadSource::new(
            "s1".into(),
            "t1".into(),
            "a1".into(),
            "http".into(),
        ));
        mgr.add_source(DownloadSource::new(
            "s2".into(),
            "t1".into(),
            "a2".into(),
            "http".into(),
        ));
        assert!(!mgr.add_source(DownloadSource::new(
            "s3".into(),
            "t1".into(),
            "a3".into(),
            "http".into()
        )));
        // Different task should work
        assert!(mgr.add_source(DownloadSource::new(
            "s4".into(),
            "t2".into(),
            "a4".into(),
            "http".into()
        )));
    }

    #[test]
    fn test_manager_duplicate_address() {
        let mut mgr = SourceRotationManager::new();
        mgr.add_source(DownloadSource::new(
            "s1".into(),
            "t1".into(),
            "same_addr".into(),
            "http".into(),
        ));
        assert!(!mgr.add_source(DownloadSource::new(
            "s2".into(),
            "t1".into(),
            "same_addr".into(),
            "http".into()
        )));
    }

    #[test]
    fn test_get_available_sources_sorted() {
        let mut mgr = SourceRotationManager::new();
        let mut s1 = DownloadSource::new("s1".into(), "t1".into(), "a1".into(), "http".into());
        s1.health_score = 0.5;
        let mut s2 = DownloadSource::new("s2".into(), "t1".into(), "a2".into(), "http".into());
        s2.health_score = 0.9;
        let mut s3 = DownloadSource::new("s3".into(), "t1".into(), "a3".into(), "http".into());
        s3.health_score = 0.7;
        mgr.add_source(s1);
        mgr.add_source(s2);
        mgr.add_source(s3);

        let available = mgr.get_available_sources("t1");
        assert_eq!(available.len(), 3);
        assert_eq!(available[0].source_id, "s2"); // Highest health
        assert_eq!(available[1].source_id, "s3");
        assert_eq!(available[2].source_id, "s1"); // Lowest health
    }

    #[test]
    fn test_get_best_source() {
        let mut mgr = SourceRotationManager::new();
        let mut s1 = DownloadSource::new("s1".into(), "t1".into(), "a1".into(), "http".into());
        s1.health_score = 0.5;
        let mut s2 = DownloadSource::new("s2".into(), "t1".into(), "a2".into(), "http".into());
        s2.health_score = 0.9;
        mgr.add_source(s1);
        mgr.add_source(s2);

        let best = mgr.get_best_source("t1").unwrap();
        assert_eq!(best.source_id, "s2");
    }

    #[test]
    fn test_cooldown_excludes_from_available() {
        let mut mgr = SourceRotationManager::new();
        let mut s1 = DownloadSource::new("s1".into(), "t1".into(), "a1".into(), "http".into());
        s1.cooldown_until = current_epoch_secs() + 3600; // On cooldown
        let s2 = DownloadSource::new("s2".into(), "t1".into(), "a2".into(), "http".into());
        mgr.add_source(s1);
        mgr.add_source(s2);

        let available = mgr.get_available_sources("t1");
        assert_eq!(available.len(), 1);
        assert_eq!(available[0].source_id, "s2");
    }

    #[test]
    fn test_rotation_deactivate_unhealthy() {
        let mut mgr = SourceRotationManager::new();
        let mut s1 = DownloadSource::new("s1".into(), "t1".into(), "a1".into(), "http".into());
        s1.health_score = 0.1; // Very unhealthy
        let s2 = DownloadSource::new("s2".into(), "t1".into(), "a2".into(), "http".into());
        mgr.add_source(s1);
        mgr.add_source(s2);

        let decision = mgr.decide_rotation("t1");
        assert!(decision.sources_to_deactivate.contains(&"s1".to_string()));
    }

    #[test]
    fn test_rotation_promote_backup() {
        let mut mgr = SourceRotationManager::new();
        let mut s1 = DownloadSource::new("s1".into(), "t1".into(), "a1".into(), "http".into());
        s1.health_score = 0.1; // Unhealthy primary
        let mut s2 = DownloadSource::new("s2".into(), "t1".into(), "a2".into(), "http".into());
        s2.is_backup = true;
        s2.active = false;
        s2.health_score = 0.9; // Healthy backup
        mgr.add_source(s1);
        mgr.add_source(s2);

        let decision = mgr.decide_rotation("t1");
        assert!(decision.sources_to_deactivate.contains(&"s1".to_string()));
        assert!(decision.sources_to_activate.contains(&"s2".to_string()));
    }

    #[test]
    fn test_rotation_cooldown_low_success_rate() {
        let mut mgr = SourceRotationManager::new();
        let mut s1 = DownloadSource::new("s1".into(), "t1".into(), "a1".into(), "http".into());
        // Simulate low success rate (below 20%)
        s1.attempt_count = 6;
        s1.success_count = 1;
        s1.failure_count = 5;
        s1.health_score = 0.5; // Not unhealthy enough to deactivate
        mgr.add_source(s1);

        let decision = mgr.decide_rotation("t1");
        assert!(decision.sources_to_cooldown.contains(&"s1".to_string()));
    }

    #[test]
    fn test_apply_rotation() {
        let mut mgr = SourceRotationManager::new();
        let mut s1 = DownloadSource::new("s1".into(), "t1".into(), "a1".into(), "http".into());
        s1.active = true;
        let mut s2 = DownloadSource::new("s2".into(), "t1".into(), "a2".into(), "http".into());
        s2.active = false;
        s2.is_backup = true;
        mgr.add_source(s1);
        mgr.add_source(s2);

        let decision = RotationDecision {
            sources_to_activate: vec!["s2".to_string()],
            sources_to_deactivate: vec!["s1".to_string()],
            sources_to_cooldown: vec![],
            reason: "test".to_string(),
        };
        mgr.apply_rotation(&decision);

        assert!(!mgr.get_source("s1").unwrap().active);
        assert!(mgr.get_source("s2").unwrap().active);
        assert!(!mgr.get_source("s2").unwrap().is_backup);
    }

    #[test]
    fn test_task_summary() {
        let mut mgr = SourceRotationManager::new();
        let mut s1 = DownloadSource::new("s1".into(), "t1".into(), "a1".into(), "http".into());
        s1.health_score = 0.9;
        s1.bytes_downloaded = 5000;
        let mut s2 = DownloadSource::new("s2".into(), "t1".into(), "a2".into(), "http".into());
        s2.health_score = 0.2;
        s2.is_backup = true;
        s2.bytes_downloaded = 1000;
        mgr.add_source(s1);
        mgr.add_source(s2);

        let summary = mgr.get_task_summary("t1");
        assert_eq!(summary.total_sources, 2);
        assert_eq!(summary.active_sources, 2);
        assert_eq!(summary.backup_sources, 1);
        assert_eq!(summary.unhealthy_sources, 1);
        assert_eq!(summary.total_bytes_downloaded, 6000);
        assert_eq!(summary.best_source.as_deref(), Some("s1"));
        assert_eq!(summary.worst_source.as_deref(), Some("s2"));
    }

    #[test]
    fn test_remove_task_sources() {
        let mut mgr = SourceRotationManager::new();
        mgr.add_source(DownloadSource::new(
            "s1".into(),
            "t1".into(),
            "a1".into(),
            "http".into(),
        ));
        mgr.add_source(DownloadSource::new(
            "s2".into(),
            "t1".into(),
            "a2".into(),
            "http".into(),
        ));
        mgr.add_source(DownloadSource::new(
            "s3".into(),
            "t2".into(),
            "a3".into(),
            "http".into(),
        ));
        let removed = mgr.remove_task_sources("t1");
        assert_eq!(removed, 2);
        assert_eq!(mgr.source_count(), 1);
    }

    #[test]
    fn test_calculate_cooldown_backoff() {
        let config = SourceRotationConfig::default();
        let cd0 = calculate_cooldown(
            0,
            config.failure_cooldown_secs,
            config.backoff_multiplier,
            config.max_cooldown_secs,
        );
        let cd1 = calculate_cooldown(
            1,
            config.failure_cooldown_secs,
            config.backoff_multiplier,
            config.max_cooldown_secs,
        );
        let cd2 = calculate_cooldown(
            2,
            config.failure_cooldown_secs,
            config.backoff_multiplier,
            config.max_cooldown_secs,
        );
        assert_eq!(cd0, 30); // base
        assert_eq!(cd1, 60); // 30 * 2^1
        assert_eq!(cd2, 120); // 30 * 2^2
    }

    #[test]
    fn test_calculate_cooldown_max() {
        let config = SourceRotationConfig::default();
        let cd = calculate_cooldown(
            20,
            config.failure_cooldown_secs,
            config.backoff_multiplier,
            config.max_cooldown_secs,
        ); // Very high failure count
        assert_eq!(cd, config.max_cooldown_secs); // Capped at max
    }

    #[test]
    fn test_config_save_load() {
        let config = SourceRotationConfig::default();
        let dir = std::env::temp_dir().join("test_source_rotation_config");
        let _ = std::fs::create_dir_all(&dir);
        save_source_rotation_config(&config, &dir).unwrap();
        let loaded = load_source_rotation_config(&dir).unwrap().unwrap();
        assert_eq!(loaded.max_sources_per_task, config.max_sources_per_task);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_config_load_missing() {
        let dir = std::env::temp_dir().join("test_source_rotation_nonexistent");
        let result = load_source_rotation_config(&dir).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut mgr = SourceRotationManager::new();
        mgr.add_source(DownloadSource::new(
            "s1".into(),
            "t1".into(),
            "addr".into(),
            "http".into(),
        ));
        let json = serde_json::to_string(&mgr).unwrap();
        let mgr2: SourceRotationManager = serde_json::from_str(&json).unwrap();
        assert_eq!(mgr2.source_count(), 1);
    }

    #[test]
    fn test_refresh_health_scores() {
        let mut mgr = SourceRotationManager::new();
        let mut s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        s.attempt_count = 4;
        s.success_count = 2; // 50% success rate
        s.health_score = 1.0;
        mgr.add_source(s);
        mgr.refresh_health_scores();
        let source = mgr.get_source("s1").unwrap();
        // Should be blend of old (1.0 * 0.6) and new (0.5 * 0.4) = 0.8
        assert!((source.health_score - 0.8).abs() < 0.01);
    }

    #[test]
    fn test_no_rotation_needed() {
        let mut mgr = SourceRotationManager::new();
        let mut s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        s.health_score = 0.9;
        mgr.add_source(s);
        let decision = mgr.decide_rotation("t1");
        assert!(decision.sources_to_activate.is_empty());
        assert!(decision.sources_to_deactivate.is_empty());
        assert!(decision.sources_to_cooldown.is_empty());
    }

    #[test]
    fn test_record_source_success_boosts_health() {
        let mut mgr = SourceRotationManager::new();
        let mut s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        s.health_score = 0.5;
        mgr.add_source(s);
        mgr.record_source_success("s1", 1000);
        let source = mgr.get_source("s1").unwrap();
        assert!(source.health_score > 0.5);
    }

    #[test]
    fn test_record_source_failure_reduces_health() {
        let mut mgr = SourceRotationManager::new();
        let mut s = DownloadSource::new("s1".into(), "t1".into(), "addr".into(), "http".into());
        s.health_score = 0.8;
        mgr.add_source(s);
        mgr.record_source_failure("s1");
        let source = mgr.get_source("s1").unwrap();
        assert!(source.health_score < 0.8);
    }
}
