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
}
