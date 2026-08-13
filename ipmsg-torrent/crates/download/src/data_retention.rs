//! Data retention policy for automatic lifecycle management of completed downloads
//!
//! Automatically manage disk space by removing old completed downloads based on
//! configurable retention rules. Supports per-tag/group rules, disk space pressure
//! detection, and smart cleanup strategies.
//!
//! Features:
//! - Time-based retention (e.g., keep completed downloads for 7 days)
//! - Disk space pressure detection (auto-cleanup when disk is low)
//! - Per-tag/group retention rules
//! - Size-based retention (keep only last N GB)
//! - Smart cleanup (oldest/smallest/largest first)
//! - Persistence to `data_retention_config.json`

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Errors from data retention operations.
#[derive(Error, Debug)]
pub enum DataRetentionError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid retention rule: {0}")]
    InvalidRule(String),
}

/// Cleanup strategy when disk space is low.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CleanupStrategy {
    /// Remove oldest completed downloads first
    OldestFirst,
    /// Remove smallest files first (free up fewer files but more items)
    SmallestFirst,
    /// Remove largest files first (free up more space with fewer deletions)
    LargestFirst,
    /// Remove by tag priority (low-priority tags first)
    TagPriority,
}

impl Default for CleanupStrategy {
    fn default() -> Self {
        Self::OldestFirst
    }
}

/// A retention rule for a specific tag or group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetentionRule {
    /// Rule ID
    pub id: String,
    /// Tag or group name this rule applies to (None = default rule)
    pub tag_or_group: Option<String>,
    /// Whether this is a tag (true) or group (false) rule
    pub is_tag: bool,
    /// Retention period in days (None = keep forever)
    pub retention_days: Option<u32>,
    /// Maximum total size in MB for this tag/group (None = unlimited)
    pub max_size_mb: Option<u64>,
    /// Cleanup strategy when limits are exceeded
    pub cleanup_strategy: CleanupStrategy,
    /// Whether this rule is enabled
    pub enabled: bool,
    /// Priority (higher = processed first)
    pub priority: i32,
}

impl RetentionRule {
    /// Create a new retention rule.
    pub fn new(
        id: String,
        tag_or_group: Option<String>,
        is_tag: bool,
        retention_days: Option<u32>,
        max_size_mb: Option<u64>,
    ) -> Self {
        Self {
            id,
            tag_or_group,
            is_tag,
            retention_days,
            max_size_mb,
            cleanup_strategy: CleanupStrategy::default(),
            enabled: true,
            priority: 0,
        }
    }

    /// Check if a task matches this rule.
    pub fn matches(&self, tags: &[String], group: &Option<String>) -> bool {
        if let Some(ref tag_or_group) = self.tag_or_group {
            if self.is_tag {
                tags.contains(tag_or_group)
            } else {
                group.as_ref() == Some(tag_or_group)
            }
        } else {
            true // Default rule matches everything
        }
    }
}

/// Configuration for data retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRetentionConfig {
    /// Whether data retention is enabled globally
    pub enabled: bool,
    /// Default retention period in days (applies to all tasks without specific rules)
    pub default_retention_days: Option<u32>,
    /// Disk space pressure threshold (percentage, 0-100)
    pub disk_pressure_threshold: Option<f32>,
    /// Minimum free disk space in MB before auto-cleanup triggers
    pub min_free_space_mb: Option<u64>,
    /// Whether to auto-delete when disk pressure is detected
    pub auto_cleanup_on_pressure: bool,
    /// Maximum total size for all completed downloads in MB (None = unlimited)
    pub max_total_size_mb: Option<u64>,
    /// Retention rules for specific tags/groups
    pub rules: Vec<RetentionRule>,
    /// Whether to preserve tasks marked as favorites
    pub preserve_favorites: bool,
    /// Whether to preserve tasks with specific tags (e.g., "important", "keep")
    pub preserve_tags: Vec<String>,
}

impl Default for DataRetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_retention_days: None,
            disk_pressure_threshold: Some(90.0),
            min_free_space_mb: Some(1024), // 1 GB
            auto_cleanup_on_pressure: false,
            max_total_size_mb: None,
            rules: Vec::new(),
            preserve_favorites: true,
            preserve_tags: vec!["important".to_string(), "keep".to_string()],
        }
    }
}

/// Information about a completed download for retention tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedDownload {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub name: String,
    /// File path
    pub file_path: PathBuf,
    /// File size in bytes
    pub size_bytes: u64,
    /// Completion timestamp
    pub completed_at: DateTime<Utc>,
    /// Tags
    pub tags: Vec<String>,
    /// Group
    pub group: Option<String>,
    /// Whether this task is marked as favorite
    pub is_favorite: bool,
    /// Last accessed time
    pub last_accessed: Option<DateTime<Utc>>,
}

/// Result of a retention cleanup operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupResult {
    /// Number of files deleted
    pub files_deleted: usize,
    /// Total bytes freed
    pub bytes_freed: u64,
    /// List of deleted task IDs
    pub deleted_task_ids: Vec<String>,
    /// Reason for cleanup
    pub reason: CleanupReason,
    /// Timestamp of cleanup
    pub cleaned_at: DateTime<Utc>,
}

/// Reason for cleanup operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CleanupReason {
    /// Manual cleanup triggered by user
    Manual,
    /// Retention period expired
    RetentionExpired,
    /// Disk space pressure detected
    DiskPressure,
    /// Total size limit exceeded
    SizeLimitExceeded,
    /// Rule-based cleanup
    RuleBased(String),
}

/// Summary of data retention status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRetentionSummary {
    /// Total completed downloads
    pub total_completed: usize,
    /// Total size of completed downloads in bytes
    pub total_size_bytes: u64,
    /// Oldest completed download age in days
    pub oldest_completion_days: Option<f64>,
    /// Number of retention rules
    pub rule_count: usize,
    /// Estimated cleanup candidates (files that would be deleted)
    pub estimated_cleanup_count: usize,
    /// Estimated bytes that would be freed
    pub estimated_cleanup_bytes: u64,
    /// Last cleanup timestamp
    pub last_cleanup: Option<DateTime<Utc>>,
    /// Cleanup history (last 10 operations)
    pub cleanup_history: Vec<CleanupResult>,
}

/// Data retention policy manager.
pub struct DataRetentionManager {
    config: DataRetentionConfig,
    completed_downloads: HashMap<String, CompletedDownload>,
    cleanup_history: Vec<CleanupResult>,
    data_dir: PathBuf,
}

impl DataRetentionManager {
    /// Create a new data retention manager.
    pub fn new(data_dir: PathBuf) -> Self {
        let config_path = data_dir.join("data_retention_config.json");
        let config = if config_path.exists() {
            match fs::read_to_string(&config_path) {
                Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
                Err(_) => DataRetentionConfig::default(),
            }
        } else {
            DataRetentionConfig::default()
        };

        Self {
            config,
            completed_downloads: HashMap::new(),
            cleanup_history: Vec::new(),
            data_dir,
        }
    }

    /// Get current configuration.
    pub fn get_config(&self) -> &DataRetentionConfig {
        &self.config
    }

    /// Update configuration.
    pub fn set_config(&mut self, config: DataRetentionConfig) -> Result<(), DataRetentionError> {
        self.config = config;
        self.save_config()
    }

    /// Save configuration to disk.
    fn save_config(&self) -> Result<(), DataRetentionError> {
        let config_path = self.data_dir.join("data_retention_config.json");
        let content = serde_json::to_string_pretty(&self.config)?;
        fs::write(config_path, content)?;
        debug!("Data retention config saved");
        Ok(())
    }

    /// Register a completed download.
    pub fn register_completed(&mut self, download: CompletedDownload) {
        debug!(
            task_id = %download.task_id,
            size = download.size_bytes,
            "Registered completed download for retention tracking"
        );
        self.completed_downloads
            .insert(download.task_id.clone(), download);
    }

    /// Remove a completed download from tracking (e.g., if manually deleted).
    pub fn unregister_completed(&mut self, task_id: &str) {
        self.completed_downloads.remove(task_id);
    }

    /// Add a retention rule.
    pub fn add_rule(&mut self, rule: RetentionRule) -> Result<(), DataRetentionError> {
        if rule.retention_days.is_none() && rule.max_size_mb.is_none() {
            return Err(DataRetentionError::InvalidRule(
                "Rule must have either retention_days or max_size_mb set".to_string(),
            ));
        }

        self.config.rules.push(rule);
        self.config
            .rules
            .sort_by_key(|r| std::cmp::Reverse(r.priority));
        self.save_config()
    }

    /// Remove a retention rule.
    pub fn remove_rule(&mut self, rule_id: &str) -> bool {
        let initial_len = self.config.rules.len();
        self.config.rules.retain(|r| r.id != rule_id);
        if self.config.rules.len() < initial_len {
            let _ = self.save_config();
            true
        } else {
            false
        }
    }

    /// List all retention rules.
    pub fn list_rules(&self) -> &[RetentionRule] {
        &self.config.rules
    }

    /// Check if a task should be preserved (favorites, protected tags).
    fn should_preserve(&self, download: &CompletedDownload) -> bool {
        // Preserve favorites if configured
        if self.config.preserve_favorites && download.is_favorite {
            return true;
        }

        // Preserve tasks with protected tags
        for tag in &download.tags {
            if self.config.preserve_tags.contains(tag) {
                return true;
            }
        }

        false
    }

    /// Find cleanup candidates based on retention rules.
    pub fn find_cleanup_candidates(&self, reason: &CleanupReason) -> Vec<CompletedDownload> {
        let now = Utc::now();
        let mut candidates: Vec<CompletedDownload> = self
            .completed_downloads
            .values()
            .filter(|dl| !self.should_preserve(dl))
            .cloned()
            .collect();

        match reason {
            CleanupReason::RetentionExpired => {
                if let Some(days) = self.config.default_retention_days {
                    let cutoff = now - Duration::days(days as i64);
                    candidates.retain(|dl| dl.completed_at < cutoff);
                } else {
                    candidates.clear();
                }
            }
            CleanupReason::DiskPressure => {
                // Sort by strategy
                candidates.sort_by(|a, b| {
                    match &self.config.rules.first().map(|r| &r.cleanup_strategy) {
                        Some(CleanupStrategy::OldestFirst) | None => {
                            a.completed_at.cmp(&b.completed_at)
                        }
                        Some(CleanupStrategy::SmallestFirst) => a.size_bytes.cmp(&b.size_bytes),
                        Some(CleanupStrategy::LargestFirst) => b.size_bytes.cmp(&a.size_bytes),
                        Some(CleanupStrategy::TagPriority) => {
                            // Lower priority tags first (simplified)
                            a.tags.len().cmp(&b.tags.len())
                        }
                    }
                });
            }
            CleanupReason::SizeLimitExceeded => {
                // Sort oldest first
                candidates.sort_by_key(|a| a.completed_at);
            }
            CleanupReason::RuleBased(rule_id) => {
                if let Some(rule) = self.config.rules.iter().find(|r| r.id == *rule_id) {
                    candidates.retain(|dl| rule.matches(&dl.tags, &dl.group));

                    // Apply rule-specific filters
                    if let Some(days) = rule.retention_days {
                        let cutoff = now - Duration::days(days as i64);
                        candidates.retain(|dl| dl.completed_at < cutoff);
                    }
                }
            }
            CleanupReason::Manual => {
                // Return all non-preserved candidates
            }
        }

        candidates
    }

    /// Execute cleanup and return results.
    pub fn execute_cleanup(
        &mut self,
        candidates: Vec<CompletedDownload>,
        reason: CleanupReason,
    ) -> Result<CleanupResult, DataRetentionError> {
        let mut bytes_freed = 0u64;
        let mut deleted_task_ids = Vec::new();

        for candidate in &candidates {
            // Delete the file
            if candidate.file_path.exists() {
                match fs::remove_file(&candidate.file_path) {
                    Ok(_) => {
                        bytes_freed += candidate.size_bytes;
                        deleted_task_ids.push(candidate.task_id.clone());
                        self.completed_downloads.remove(&candidate.task_id);
                        info!(
                            task_id = %candidate.task_id,
                            size = candidate.size_bytes,
                            "Deleted completed download"
                        );
                    }
                    Err(e) => {
                        warn!(
                            task_id = %candidate.task_id,
                            error = %e,
                            "Failed to delete file"
                        );
                    }
                }
            } else {
                // File already gone, just remove from tracking
                deleted_task_ids.push(candidate.task_id.clone());
                self.completed_downloads.remove(&candidate.task_id);
            }
        }

        let result = CleanupResult {
            files_deleted: deleted_task_ids.len(),
            bytes_freed,
            deleted_task_ids: deleted_task_ids.clone(),
            reason: reason.clone(),
            cleaned_at: Utc::now(),
        };

        // Add to history (keep last 10)
        self.cleanup_history.push(result.clone());
        if self.cleanup_history.len() > 10 {
            self.cleanup_history.remove(0);
        }

        info!(
            files_deleted = result.files_deleted,
            bytes_freed = result.bytes_freed,
            "Cleanup completed"
        );

        Ok(result)
    }

    /// Get summary of data retention status.
    pub fn get_summary(&self) -> DataRetentionSummary {
        let now = Utc::now();
        let total_completed = self.completed_downloads.len();
        let total_size_bytes: u64 = self
            .completed_downloads
            .values()
            .map(|dl| dl.size_bytes)
            .sum();

        let oldest_completion_days = self
            .completed_downloads
            .values()
            .map(|dl| (now - dl.completed_at).num_days() as f64)
            .fold(None, |acc: Option<f64>, x| {
                Some(acc.map_or(x, |a| a.max(x)))
            });

        // Estimate cleanup candidates
        let estimated_candidates = self.find_cleanup_candidates(&CleanupReason::RetentionExpired);
        let estimated_cleanup_count = estimated_candidates.len();
        let estimated_cleanup_bytes: u64 =
            estimated_candidates.iter().map(|dl| dl.size_bytes).sum();

        let last_cleanup = self.cleanup_history.last().map(|r| r.cleaned_at);

        DataRetentionSummary {
            total_completed,
            total_size_bytes,
            oldest_completion_days,
            rule_count: self.config.rules.len(),
            estimated_cleanup_count,
            estimated_cleanup_bytes,
            last_cleanup,
            cleanup_history: self.cleanup_history.clone(),
        }
    }

    /// Get cleanup history.
    pub fn get_cleanup_history(&self) -> &[CleanupResult] {
        &self.cleanup_history
    }

    /// Clear cleanup history.
    pub fn clear_history(&mut self) {
        self.cleanup_history.clear();
    }

    /// Check if disk pressure cleanup should trigger.
    pub fn check_disk_pressure(&self, free_space_mb: u64, total_space_mb: u64) -> bool {
        if !self.config.enabled || !self.config.auto_cleanup_on_pressure {
            return false;
        }

        let free_percentage = (free_space_mb as f64 / total_space_mb as f64) * 100.0;

        if let Some(threshold) = self.config.disk_pressure_threshold {
            if free_percentage < (100.0 - threshold as f64) {
                return true;
            }
        }

        if let Some(min_free) = self.config.min_free_space_mb {
            return free_space_mb < min_free;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_default_config() {
        let config = DataRetentionConfig::default();
        assert!(!config.enabled);
        assert!(config.preserve_favorites);
        assert_eq!(config.preserve_tags.len(), 2);
    }

    #[test]
    fn test_retention_rule_matching() {
        let rule = RetentionRule::new(
            "rule1".to_string(),
            Some("videos".to_string()),
            true,
            Some(7),
            None,
        );

        let tags = vec!["videos".to_string(), "entertainment".to_string()];
        assert!(rule.matches(&tags, &None));

        let tags2 = vec!["music".to_string()];
        assert!(!rule.matches(&tags2, &None));
    }

    #[test]
    fn test_retention_rule_group_matching() {
        let rule = RetentionRule::new(
            "rule2".to_string(),
            Some("work".to_string()),
            false,
            Some(30),
            None,
        );

        let group = Some("work".to_string());
        assert!(rule.matches(&[], &group));

        let group2 = Some("personal".to_string());
        assert!(!rule.matches(&[], &group2));
    }

    #[test]
    fn test_add_invalid_rule() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let rule = RetentionRule::new(
            "invalid".to_string(),
            None,
            true,
            None,
            None, // Neither retention_days nor max_size_mb
        );

        assert!(manager.add_rule(rule).is_err());
    }

    #[test]
    fn test_add_valid_rule() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let rule = RetentionRule::new(
            "valid".to_string(),
            Some("test".to_string()),
            true,
            Some(7),
            None,
        );

        assert!(manager.add_rule(rule).is_ok());
        assert_eq!(manager.list_rules().len(), 1);
    }

    #[test]
    fn test_remove_rule() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let rule = RetentionRule::new("to_remove".to_string(), None, true, Some(7), None);

        manager.add_rule(rule).unwrap();
        assert!(manager.remove_rule("to_remove"));
        assert!(!manager.remove_rule("nonexistent"));
    }

    #[test]
    fn test_should_preserve_favorites() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.preserve_favorites = true;

        let download = CompletedDownload {
            task_id: "fav".to_string(),
            name: "favorite.txt".to_string(),
            file_path: PathBuf::from("/tmp/fav.txt"),
            size_bytes: 1024,
            completed_at: Utc::now(),
            tags: vec![],
            group: None,
            is_favorite: true,
            last_accessed: None,
        };

        assert!(manager.should_preserve(&download));
    }

    #[test]
    fn test_should_preserve_protected_tags() {
        let temp_dir = tempdir().unwrap();
        let manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let download = CompletedDownload {
            task_id: "protected".to_string(),
            name: "important.txt".to_string(),
            file_path: PathBuf::from("/tmp/imp.txt"),
            size_bytes: 1024,
            completed_at: Utc::now(),
            tags: vec!["important".to_string()],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };

        assert!(manager.should_preserve(&download));
    }

    #[test]
    fn test_register_and_unregister() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let download = CompletedDownload {
            task_id: "test".to_string(),
            name: "test.txt".to_string(),
            file_path: PathBuf::from("/tmp/test.txt"),
            size_bytes: 1024,
            completed_at: Utc::now(),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };

        manager.register_completed(download);
        assert_eq!(manager.completed_downloads.len(), 1);

        manager.unregister_completed("test");
        assert_eq!(manager.completed_downloads.len(), 0);
    }

    #[test]
    fn test_cleanup_summary() {
        let temp_dir = tempdir().unwrap();
        let manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let summary = manager.get_summary();
        assert_eq!(summary.total_completed, 0);
        assert_eq!(summary.total_size_bytes, 0);
    }

    #[test]
    fn test_config_persistence_round_trip() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let mut config = DataRetentionConfig::default();
        config.enabled = true;
        config.default_retention_days = Some(14);
        config.disk_pressure_threshold = Some(85.0);
        config.min_free_space_mb = Some(2048);
        config.auto_cleanup_on_pressure = true;
        config.max_total_size_mb = Some(10240);
        config.preserve_favorites = false;
        config.preserve_tags = vec!["critical".to_string()];

        manager.set_config(config).unwrap();

        // Reload from disk
        let manager2 = DataRetentionManager::new(temp_dir.path().to_path_buf());
        let loaded = manager2.get_config();
        assert!(loaded.enabled);
        assert_eq!(loaded.default_retention_days, Some(14));
        assert_eq!(loaded.disk_pressure_threshold, Some(85.0));
        assert_eq!(loaded.min_free_space_mb, Some(2048));
        assert!(loaded.auto_cleanup_on_pressure);
        assert_eq!(loaded.max_total_size_mb, Some(10240));
        assert!(!loaded.preserve_favorites);
        assert_eq!(loaded.preserve_tags, vec!["critical".to_string()]);
    }

    #[test]
    fn test_config_missing_file_uses_default() {
        let temp_dir = tempdir().unwrap();
        let manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        let config = manager.get_config();
        assert!(!config.enabled);
        assert_eq!(config.default_retention_days, None);
        assert!(config.preserve_favorites);
    }

    #[test]
    fn test_config_corrupt_file_uses_default() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("data_retention_config.json");
        fs::write(&config_path, "not valid json {{{").unwrap();
        let manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        let config = manager.get_config();
        assert!(!config.enabled);
        assert!(config.preserve_favorites);
    }

    #[test]
    fn test_rule_priority_sorting() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let low = RetentionRule {
            id: "low".to_string(),
            tag_or_group: Some("a".to_string()),
            is_tag: true,
            retention_days: Some(7),
            max_size_mb: None,
            cleanup_strategy: CleanupStrategy::default(),
            enabled: true,
            priority: 1,
        };
        let high = RetentionRule {
            id: "high".to_string(),
            tag_or_group: Some("b".to_string()),
            is_tag: true,
            retention_days: Some(30),
            max_size_mb: None,
            cleanup_strategy: CleanupStrategy::default(),
            enabled: true,
            priority: 10,
        };
        let mid = RetentionRule {
            id: "mid".to_string(),
            tag_or_group: Some("c".to_string()),
            is_tag: true,
            retention_days: Some(14),
            max_size_mb: None,
            cleanup_strategy: CleanupStrategy::default(),
            enabled: true,
            priority: 5,
        };

        manager.add_rule(low).unwrap();
        manager.add_rule(high).unwrap();
        manager.add_rule(mid).unwrap();

        let rules = manager.list_rules();
        assert_eq!(rules[0].id, "high");
        assert_eq!(rules[1].id, "mid");
        assert_eq!(rules[2].id, "low");
    }

    #[test]
    fn test_default_rule_matches_everything() {
        let rule = RetentionRule::new(
            "default".to_string(),
            None, // no tag_or_group => matches all
            true,
            Some(7),
            None,
        );
        assert!(rule.matches(&[], &None));
        assert!(rule.matches(&["anything".to_string()], &Some("grp".to_string())));
    }

    #[test]
    fn test_find_candidates_retention_expired() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.default_retention_days = Some(7);

        let old = CompletedDownload {
            task_id: "old".to_string(),
            name: "old.txt".to_string(),
            file_path: PathBuf::from("/tmp/old.txt"),
            size_bytes: 100,
            completed_at: Utc::now() - Duration::days(10),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        let recent = CompletedDownload {
            task_id: "recent".to_string(),
            name: "recent.txt".to_string(),
            file_path: PathBuf::from("/tmp/recent.txt"),
            size_bytes: 200,
            completed_at: Utc::now() - Duration::days(2),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(old);
        manager.register_completed(recent);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::RetentionExpired);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].task_id, "old");
    }

    #[test]
    fn test_find_candidates_no_retention_days_returns_empty() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        // default_retention_days is None

        let dl = CompletedDownload {
            task_id: "x".to_string(),
            name: "x.txt".to_string(),
            file_path: PathBuf::from("/tmp/x.txt"),
            size_bytes: 100,
            completed_at: Utc::now() - Duration::days(365),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(dl);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::RetentionExpired);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_find_candidates_preserves_favorites() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.default_retention_days = Some(7);
        manager.config.preserve_favorites = true;

        let fav = CompletedDownload {
            task_id: "fav".to_string(),
            name: "fav.txt".to_string(),
            file_path: PathBuf::from("/tmp/fav.txt"),
            size_bytes: 100,
            completed_at: Utc::now() - Duration::days(30),
            tags: vec![],
            group: None,
            is_favorite: true,
            last_accessed: None,
        };
        let normal = CompletedDownload {
            task_id: "normal".to_string(),
            name: "normal.txt".to_string(),
            file_path: PathBuf::from("/tmp/normal.txt"),
            size_bytes: 100,
            completed_at: Utc::now() - Duration::days(30),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(fav);
        manager.register_completed(normal);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::RetentionExpired);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].task_id, "normal");
    }

    #[test]
    fn test_find_candidates_preserves_protected_tags() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.default_retention_days = Some(7);
        manager.config.preserve_tags = vec!["keep".to_string(), "important".to_string()];

        let protected = CompletedDownload {
            task_id: "prot".to_string(),
            name: "prot.txt".to_string(),
            file_path: PathBuf::from("/tmp/prot.txt"),
            size_bytes: 100,
            completed_at: Utc::now() - Duration::days(30),
            tags: vec!["keep".to_string()],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(protected);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::RetentionExpired);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_find_candidates_rule_based() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let rule = RetentionRule::new(
            "r1".to_string(),
            Some("videos".to_string()),
            true,
            Some(3),
            None,
        );
        manager.add_rule(rule).unwrap();

        let video = CompletedDownload {
            task_id: "vid".to_string(),
            name: "video.mp4".to_string(),
            file_path: PathBuf::from("/tmp/video.mp4"),
            size_bytes: 500,
            completed_at: Utc::now() - Duration::days(5),
            tags: vec!["videos".to_string()],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        let music = CompletedDownload {
            task_id: "mus".to_string(),
            name: "song.mp3".to_string(),
            file_path: PathBuf::from("/tmp/song.mp3"),
            size_bytes: 10,
            completed_at: Utc::now() - Duration::days(5),
            tags: vec!["music".to_string()],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(video);
        manager.register_completed(music);

        let candidates =
            manager.find_cleanup_candidates(&CleanupReason::RuleBased("r1".to_string()));
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].task_id, "vid");
    }

    #[test]
    fn test_find_candidates_disk_pressure_sorting_oldest_first() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.rules.push(RetentionRule {
            id: "default".to_string(),
            tag_or_group: None,
            is_tag: false,
            retention_days: None,
            max_size_mb: None,
            cleanup_strategy: CleanupStrategy::OldestFirst,
            enabled: true,
            priority: 0,
        });

        let a = CompletedDownload {
            task_id: "a".to_string(),
            name: "a.txt".to_string(),
            file_path: PathBuf::from("/tmp/a.txt"),
            size_bytes: 100,
            completed_at: Utc::now() - Duration::days(2),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        let b = CompletedDownload {
            task_id: "b".to_string(),
            name: "b.txt".to_string(),
            file_path: PathBuf::from("/tmp/b.txt"),
            size_bytes: 200,
            completed_at: Utc::now() - Duration::days(10),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(a);
        manager.register_completed(b);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::DiskPressure);
        assert_eq!(candidates[0].task_id, "b"); // oldest first
    }

    #[test]
    fn test_find_candidates_disk_pressure_sorting_largest_first() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.rules.push(RetentionRule {
            id: "default".to_string(),
            tag_or_group: None,
            is_tag: false,
            retention_days: None,
            max_size_mb: None,
            cleanup_strategy: CleanupStrategy::LargestFirst,
            enabled: true,
            priority: 0,
        });

        let small = CompletedDownload {
            task_id: "small".to_string(),
            name: "small.txt".to_string(),
            file_path: PathBuf::from("/tmp/small.txt"),
            size_bytes: 10,
            completed_at: Utc::now(),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        let big = CompletedDownload {
            task_id: "big".to_string(),
            name: "big.txt".to_string(),
            file_path: PathBuf::from("/tmp/big.txt"),
            size_bytes: 9999,
            completed_at: Utc::now(),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(small);
        manager.register_completed(big);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::DiskPressure);
        assert_eq!(candidates[0].task_id, "big");
    }

    #[test]
    fn test_execute_cleanup_with_real_files() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        // Create actual files
        let file1 = temp_dir.path().join("file1.txt");
        let file2 = temp_dir.path().join("file2.txt");
        fs::write(&file1, "hello").unwrap();
        fs::write(&file2, "world!").unwrap();

        let dl1 = CompletedDownload {
            task_id: "t1".to_string(),
            name: "file1.txt".to_string(),
            file_path: file1.clone(),
            size_bytes: 5,
            completed_at: Utc::now(),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        let dl2 = CompletedDownload {
            task_id: "t2".to_string(),
            name: "file2.txt".to_string(),
            file_path: file2.clone(),
            size_bytes: 6,
            completed_at: Utc::now(),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(dl1);
        manager.register_completed(dl2);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::Manual);
        assert_eq!(candidates.len(), 2);

        let result = manager
            .execute_cleanup(candidates, CleanupReason::Manual)
            .unwrap();
        assert_eq!(result.files_deleted, 2);
        assert_eq!(result.bytes_freed, 11);
        assert!(!file1.exists());
        assert!(!file2.exists());
        assert_eq!(manager.completed_downloads.len(), 0);
    }

    #[test]
    fn test_execute_cleanup_missing_file() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let dl = CompletedDownload {
            task_id: "gone".to_string(),
            name: "gone.txt".to_string(),
            file_path: PathBuf::from("/nonexistent/path/gone.txt"),
            size_bytes: 100,
            completed_at: Utc::now(),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(dl);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::Manual);
        let result = manager
            .execute_cleanup(candidates, CleanupReason::Manual)
            .unwrap();
        // File doesn't exist, but task is still removed from tracking
        assert_eq!(result.files_deleted, 1);
        assert_eq!(result.bytes_freed, 0);
        assert_eq!(manager.completed_downloads.len(), 0);
    }

    #[test]
    fn test_cleanup_history_capped_at_10() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        for i in 0..15 {
            let result = CleanupResult {
                files_deleted: 1,
                bytes_freed: 100,
                deleted_task_ids: vec![format!("t{}", i)],
                reason: CleanupReason::Manual,
                cleaned_at: Utc::now(),
            };
            manager.cleanup_history.push(result);
        }

        // Manually cap (simulate what execute_cleanup does)
        while manager.cleanup_history.len() > 10 {
            manager.cleanup_history.remove(0);
        }
        assert_eq!(manager.cleanup_history.len(), 10);
    }

    #[test]
    fn test_clear_history() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        manager.cleanup_history.push(CleanupResult {
            files_deleted: 0,
            bytes_freed: 0,
            deleted_task_ids: vec![],
            reason: CleanupReason::Manual,
            cleaned_at: Utc::now(),
        });
        assert_eq!(manager.get_cleanup_history().len(), 1);
        manager.clear_history();
        assert_eq!(manager.get_cleanup_history().len(), 0);
    }

    #[test]
    fn test_check_disk_pressure_disabled() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.enabled = false;
        manager.config.auto_cleanup_on_pressure = true;

        assert!(!manager.check_disk_pressure(100, 10000));
    }

    #[test]
    fn test_check_disk_pressure_auto_cleanup_disabled() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.enabled = true;
        manager.config.auto_cleanup_on_pressure = false;

        assert!(!manager.check_disk_pressure(100, 10000));
    }

    #[test]
    fn test_check_disk_pressure_percentage_threshold() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.enabled = true;
        manager.config.auto_cleanup_on_pressure = true;
        manager.config.disk_pressure_threshold = Some(90.0);
        manager.config.min_free_space_mb = None;

        // 5% free < 10% threshold (100 - 90)
        assert!(manager.check_disk_pressure(500, 10000));
        // 15% free >= 10% threshold
        assert!(!manager.check_disk_pressure(1500, 10000));
    }

    #[test]
    fn test_check_disk_pressure_min_free_space() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.enabled = true;
        manager.config.auto_cleanup_on_pressure = true;
        manager.config.disk_pressure_threshold = None;
        manager.config.min_free_space_mb = Some(1024);

        assert!(manager.check_disk_pressure(512, 100000)); // 512 < 1024
        assert!(!manager.check_disk_pressure(2048, 100000)); // 2048 >= 1024
    }

    #[test]
    fn test_summary_with_populated_data() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.default_retention_days = Some(7);

        let dl1 = CompletedDownload {
            task_id: "s1".to_string(),
            name: "s1.txt".to_string(),
            file_path: PathBuf::from("/tmp/s1.txt"),
            size_bytes: 1000,
            completed_at: Utc::now() - Duration::days(30),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        let dl2 = CompletedDownload {
            task_id: "s2".to_string(),
            name: "s2.txt".to_string(),
            file_path: PathBuf::from("/tmp/s2.txt"),
            size_bytes: 2000,
            completed_at: Utc::now() - Duration::days(2),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(dl1);
        manager.register_completed(dl2);

        let summary = manager.get_summary();
        assert_eq!(summary.total_completed, 2);
        assert_eq!(summary.total_size_bytes, 3000);
        assert!(summary.oldest_completion_days.is_some());
        assert!(summary.oldest_completion_days.unwrap() >= 29.0);
        assert_eq!(summary.estimated_cleanup_count, 1); // only the 30-day-old one
        assert_eq!(summary.estimated_cleanup_bytes, 1000);
        assert!(summary.last_cleanup.is_none());
    }

    #[test]
    fn test_cleanup_strategy_serialization() {
        let strategies = vec![
            CleanupStrategy::OldestFirst,
            CleanupStrategy::SmallestFirst,
            CleanupStrategy::LargestFirst,
            CleanupStrategy::TagPriority,
        ];
        for s in strategies {
            let json = serde_json::to_string(&s).unwrap();
            let parsed: CleanupStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, s);
        }
    }

    #[test]
    fn test_retention_rule_serialization() {
        let rule = RetentionRule::new(
            "r1".to_string(),
            Some("videos".to_string()),
            true,
            Some(7),
            Some(5120),
        );
        let json = serde_json::to_string(&rule).unwrap();
        let parsed: RetentionRule = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, rule.id);
        assert_eq!(parsed.tag_or_group, rule.tag_or_group);
        assert_eq!(parsed.retention_days, rule.retention_days);
        assert_eq!(parsed.max_size_mb, rule.max_size_mb);
    }

    #[test]
    fn test_completed_download_serialization() {
        let dl = CompletedDownload {
            task_id: "ser".to_string(),
            name: "test.bin".to_string(),
            file_path: PathBuf::from("/tmp/test.bin"),
            size_bytes: 42,
            completed_at: Utc::now(),
            tags: vec!["a".to_string(), "b".to_string()],
            group: Some("grp".to_string()),
            is_favorite: true,
            last_accessed: Some(Utc::now()),
        };
        let json = serde_json::to_string(&dl).unwrap();
        let parsed: CompletedDownload = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.task_id, "ser");
        assert_eq!(parsed.size_bytes, 42);
        assert!(parsed.is_favorite);
        assert_eq!(parsed.tags.len(), 2);
        assert_eq!(parsed.group, Some("grp".to_string()));
    }

    #[test]
    fn test_cleanup_reason_serialization() {
        let reasons = vec![
            CleanupReason::Manual,
            CleanupReason::RetentionExpired,
            CleanupReason::DiskPressure,
            CleanupReason::SizeLimitExceeded,
            CleanupReason::RuleBased("rule42".to_string()),
        ];
        for r in reasons {
            let json = serde_json::to_string(&r).unwrap();
            let parsed: CleanupReason = serde_json::from_str(&json).unwrap();
            // Compare by re-serializing (enum with data)
            assert_eq!(
                serde_json::to_string(&parsed).unwrap(),
                serde_json::to_string(&r).unwrap()
            );
        }
    }

    #[test]
    fn test_add_rule_with_max_size_mb() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let rule = RetentionRule::new(
            "size_limit".to_string(),
            Some("cache".to_string()),
            true,
            None,
            Some(1024), // 1 GB max
        );
        assert!(manager.add_rule(rule).is_ok());
        assert_eq!(manager.list_rules().len(), 1);
    }

    #[test]
    fn test_find_candidates_size_limit_exceeded() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let old = CompletedDownload {
            task_id: "old_sz".to_string(),
            name: "old.bin".to_string(),
            file_path: PathBuf::from("/tmp/old.bin"),
            size_bytes: 100,
            completed_at: Utc::now() - Duration::days(60),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        let recent = CompletedDownload {
            task_id: "new_sz".to_string(),
            name: "new.bin".to_string(),
            file_path: PathBuf::from("/tmp/new.bin"),
            size_bytes: 200,
            completed_at: Utc::now() - Duration::days(1),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(old);
        manager.register_completed(recent);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::SizeLimitExceeded);
        // Sorted oldest first
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].task_id, "old_sz");
    }

    #[test]
    fn test_execute_cleanup_records_history() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let dl = CompletedDownload {
            task_id: "hist".to_string(),
            name: "hist.txt".to_string(),
            file_path: PathBuf::from("/nonexistent/hist.txt"),
            size_bytes: 50,
            completed_at: Utc::now(),
            tags: vec![],
            group: None,
            is_favorite: false,
            last_accessed: None,
        };
        manager.register_completed(dl);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::Manual);
        let result = manager
            .execute_cleanup(candidates, CleanupReason::Manual)
            .unwrap();

        assert_eq!(manager.get_cleanup_history().len(), 1);
        assert_eq!(manager.get_cleanup_history()[0].files_deleted, 1);
        assert!(manager.get_summary().last_cleanup.is_some());
    }

    #[test]
    fn test_preserve_favorites_disabled() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.config.preserve_favorites = false;
        manager.config.default_retention_days = Some(7);

        let fav = CompletedDownload {
            task_id: "fav_no_preserve".to_string(),
            name: "fav.txt".to_string(),
            file_path: PathBuf::from("/tmp/fav.txt"),
            size_bytes: 100,
            completed_at: Utc::now() - Duration::days(30),
            tags: vec![],
            group: None,
            is_favorite: true,
            last_accessed: None,
        };
        manager.register_completed(fav);

        let candidates = manager.find_cleanup_candidates(&CleanupReason::RetentionExpired);
        assert_eq!(candidates.len(), 1); // favorite not preserved when config is false
    }

    #[test]
    fn test_unregister_nonexistent_is_noop() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());
        manager.unregister_completed("does_not_exist");
        assert_eq!(manager.completed_downloads.len(), 0);
    }

    #[test]
    fn test_remove_rule_persists() {
        let temp_dir = tempdir().unwrap();
        let mut manager = DataRetentionManager::new(temp_dir.path().to_path_buf());

        let rule = RetentionRule::new("persist_me".to_string(), None, true, Some(7), None);
        manager.add_rule(rule).unwrap();
        assert_eq!(manager.list_rules().len(), 1);

        manager.remove_rule("persist_me");

        // Reload from disk
        let manager2 = DataRetentionManager::new(temp_dir.path().to_path_buf());
        assert_eq!(manager2.list_rules().len(), 0);
    }
}
