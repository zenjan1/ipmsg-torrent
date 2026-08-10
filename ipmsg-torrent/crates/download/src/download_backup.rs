//! Download Data Export/Backup System (Phase 123)
//!
//! Comprehensive backup system that exports the entire DownloadManager state
//! including all subsystem configurations to a single portable file.
//! Supports atomic backup creation and selective restore.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Backup format version for forward compatibility
const BACKUP_VERSION: u32 = 1;

/// Complete backup of DownloadManager state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadBackup {
    /// Format version
    pub version: u32,
    /// Backup timestamp
    pub created_at: DateTime<Utc>,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
    /// Source application identifier
    #[serde(default)]
    pub source: String,
    /// Task queue data
    #[serde(default)]
    pub tasks: BackupTasks,
    /// Subsystem configurations
    #[serde(default)]
    pub configs: BackupConfigs,
}

/// Task queue backup data
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BackupTasks {
    /// Exported tasks
    pub tasks: Vec<crate::task_export::ExportedTask>,
    /// Task generation counters
    #[serde(default)]
    pub generations: HashMap<String, u64>,
}

/// All subsystem configurations backup
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BackupConfigs {
    /// Auto-cleanup configuration
    #[serde(default)]
    pub auto_cleanup: Option<crate::auto_cleanup::AutoCleanupConfig>,
    /// Auto-pause configuration
    #[serde(default)]
    pub auto_pause: Option<crate::auto_pause::AutoPauseConfig>,
    /// Automation rules
    #[serde(default)]
    pub automation_rules: Option<crate::automation_rules::AutomationConfig>,
    /// Bandwidth allocation
    #[serde(default)]
    pub bandwidth_allocation: Option<crate::bandwidth_allocation::AllocationConfig>,
    /// Bandwidth schedule rules
    #[serde(default)]
    pub bandwidth_schedule: Option<Vec<crate::bandwidth_schedule::BandwidthScheduleRule>>,
    /// Categorize rules
    #[serde(default)]
    pub categorize_rules: Option<Vec<crate::auto_categorize::CategorizeRule>>,
    /// Conflict detection strategy
    #[serde(default)]
    pub conflict_strategy: Option<crate::conflict_detection::ConflictStrategy>,
    /// Cooldown configuration
    #[serde(default)]
    pub cooldown: Option<crate::download_cooldown::CooldownConfig>,
    /// Data cap configuration
    #[serde(default)]
    pub data_cap: Option<crate::data_cap::DataCapConfig>,
    /// Dependency graph configuration
    #[serde(default)]
    pub dependency_graph: Option<crate::dependency_graph::DependencyGraphConfig>,
    /// Disk monitor configuration
    #[serde(default)]
    pub disk_monitor: Option<crate::disk_monitor::DiskMonitorConfig>,
    /// Domain limit configuration
    #[serde(default)]
    pub domain_limit: Option<crate::domain_limit::DomainLimitConfig>,
    /// Download analytics configuration
    #[serde(default)]
    pub download_analytics: Option<crate::download_analytics::AnalyticsConfig>,
    /// Download budget configuration
    #[serde(default)]
    pub download_budget: Option<crate::download_budget::BudgetConfig>,
    /// Download deadline configuration
    #[serde(default)]
    pub download_deadline: Option<crate::download_deadline::DeadlineConfig>,
    /// Download presets
    #[serde(default)]
    pub download_presets: Option<Vec<crate::download_presets::DownloadPreset>>,
    /// Download quota rules
    #[serde(default)]
    pub download_quota: Option<crate::download_quota::QuotaSystemConfig>,
    /// Download time limit configuration
    #[serde(default)]
    pub download_time_limit: Option<crate::download_time_limit::DownloadTimeLimitConfig>,
    /// Duplicate detection configuration
    #[serde(default)]
    pub duplicate_detection: Option<crate::duplicate_detection::DuplicateDetectionConfig>,
    /// Error recovery configuration
    #[serde(default)]
    pub error_recovery: Option<crate::error_recovery::ErrorRecoveryConfig>,
    /// Global budget configuration
    #[serde(default)]
    pub global_budget: Option<crate::global_budget::GlobalBudgetConfig>,
    /// Integrity verification configuration
    #[serde(default)]
    pub integrity: Option<crate::integrity_verification::IntegrityConfig>,
    /// Network aware configuration
    #[serde(default)]
    pub network_aware: Option<crate::network_aware::NetworkAwareConfig>,
    /// Path rules
    #[serde(default)]
    pub path_rules: Option<Vec<crate::path_rules::PathRule>>,
    /// Path template configuration
    #[serde(default)]
    pub path_template: Option<crate::path_template::PathTemplateConfig>,
    /// Priority aging configuration
    #[serde(default)]
    pub priority_aging: Option<crate::priority_aging::PriorityAgingConfig>,
    /// Protocol limits configuration
    #[serde(default)]
    pub protocol_limits: Option<crate::protocol_limits::ProtocolLimitsConfig>,
    /// Queue completion configuration
    #[serde(default)]
    pub queue_completion: Option<crate::queue_completion::QueueCompletionConfig>,
    /// Queue staleness configuration
    #[serde(default)]
    pub queue_staleness: Option<crate::queue_staleness::StalenessConfig>,
    /// Recycle bin configuration
    #[serde(default)]
    pub recycle_bin: Option<crate::recycle_bin::RecycleBinConfig>,
    /// Resume policy configuration
    #[serde(default)]
    pub resume_policy: Option<crate::resume_policy::ResumePolicyConfig>,
    /// Save path configuration
    #[serde(default)]
    pub save_path: Option<crate::save_path_manager::SavePathConfig>,
    /// Speed alert configuration
    #[serde(default)]
    pub speed_alert: Option<crate::speed_alert::SpeedAlertConfig>,
    /// Speed profiles
    #[serde(default)]
    pub speed_profiles: Option<Vec<crate::speed_profiles::SpeedProfile>>,
    /// Task chain data
    #[serde(default)]
    pub task_chains: Option<Vec<crate::task_chain::TaskChain>>,
    /// Task schedule windows
    #[serde(default)]
    pub task_schedule_windows: Option<Vec<crate::task_schedule_windows::ScheduleWindow>>,
    /// URL allowlist configuration
    #[serde(default)]
    pub url_allowlist: Option<crate::url_allowlist::AllowlistConfig>,
    /// URL bookmarks
    #[serde(default)]
    pub url_bookmarks: Option<Vec<crate::url_bookmarks::UrlBookmark>>,
    /// URL dedup configuration
    #[serde(default)]
    pub url_dedup: Option<crate::url_dedup::DedupConfig>,
    /// URL normalizer configuration
    #[serde(default)]
    pub url_normalizer: Option<crate::url_normalizer::UrlNormalizerConfig>,
    /// URL rewrite rules
    #[serde(default)]
    pub url_rewrite: Option<Vec<crate::url_rewrite::UrlRewriteRule>>,
    /// Watch folder configuration
    #[serde(default)]
    pub watch_folder: Option<crate::watch_folder::WatchFolderAutoScanConfig>,
}

impl BackupConfigs {
    /// Count how many configuration sections are present
    pub fn count_some(&self) -> usize {
        let mut count = 0;
        if self.auto_cleanup.is_some() {
            count += 1;
        }
        if self.auto_pause.is_some() {
            count += 1;
        }
        if self.automation_rules.is_some() {
            count += 1;
        }
        if self.bandwidth_allocation.is_some() {
            count += 1;
        }
        if self.bandwidth_schedule.is_some() {
            count += 1;
        }
        if self.categorize_rules.is_some() {
            count += 1;
        }
        if self.conflict_strategy.is_some() {
            count += 1;
        }
        if self.cooldown.is_some() {
            count += 1;
        }
        if self.data_cap.is_some() {
            count += 1;
        }
        if self.dependency_graph.is_some() {
            count += 1;
        }
        if self.disk_monitor.is_some() {
            count += 1;
        }
        if self.domain_limit.is_some() {
            count += 1;
        }
        if self.download_analytics.is_some() {
            count += 1;
        }
        if self.download_budget.is_some() {
            count += 1;
        }
        if self.download_deadline.is_some() {
            count += 1;
        }
        if self.download_presets.is_some() {
            count += 1;
        }
        if self.download_quota.is_some() {
            count += 1;
        }
        if self.download_time_limit.is_some() {
            count += 1;
        }
        if self.duplicate_detection.is_some() {
            count += 1;
        }
        if self.error_recovery.is_some() {
            count += 1;
        }
        if self.global_budget.is_some() {
            count += 1;
        }
        if self.integrity.is_some() {
            count += 1;
        }
        if self.network_aware.is_some() {
            count += 1;
        }
        if self.path_rules.is_some() {
            count += 1;
        }
        if self.path_template.is_some() {
            count += 1;
        }
        if self.priority_aging.is_some() {
            count += 1;
        }
        if self.protocol_limits.is_some() {
            count += 1;
        }
        if self.queue_completion.is_some() {
            count += 1;
        }
        if self.queue_staleness.is_some() {
            count += 1;
        }
        if self.recycle_bin.is_some() {
            count += 1;
        }
        if self.resume_policy.is_some() {
            count += 1;
        }
        if self.save_path.is_some() {
            count += 1;
        }
        if self.speed_alert.is_some() {
            count += 1;
        }
        if self.speed_profiles.is_some() {
            count += 1;
        }
        if self.task_chains.is_some() {
            count += 1;
        }
        if self.task_schedule_windows.is_some() {
            count += 1;
        }
        if self.url_allowlist.is_some() {
            count += 1;
        }
        if self.url_bookmarks.is_some() {
            count += 1;
        }
        if self.url_dedup.is_some() {
            count += 1;
        }
        if self.url_normalizer.is_some() {
            count += 1;
        }
        if self.url_rewrite.is_some() {
            count += 1;
        }
        if self.watch_folder.is_some() {
            count += 1;
        }
        count
    }
}

/// Errors during backup/restore operations
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize error: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("unsupported backup version: {0}")]
    UnsupportedVersion(u32),
    #[error("invalid backup file: {0}")]
    InvalidFile(String),
    #[error("backup already exists: {0}")]
    AlreadyExists(PathBuf),
}

/// Backup manager for creating and restoring backups
pub struct BackupManager {
    data_dir: PathBuf,
}

impl BackupManager {
    /// Create a new backup manager
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Create a comprehensive backup of all DownloadManager state
    pub fn create_backup(
        &self,
        description: Option<String>,
        tasks: Vec<crate::task_export::ExportedTask>,
        generations: HashMap<String, u64>,
        configs: BackupConfigs,
    ) -> Result<PathBuf, BackupError> {
        let backup = DownloadBackup {
            version: BACKUP_VERSION,
            created_at: Utc::now(),
            description,
            source: "ipmsg-torrent".to_string(),
            tasks: BackupTasks { tasks, generations },
            configs,
        };

        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("backup_{}.json", timestamp);
        let backup_path = self.data_dir.join(&filename);

        // Check if file already exists
        if backup_path.exists() {
            return Err(BackupError::AlreadyExists(backup_path));
        }

        let json = serde_json::to_string_pretty(&backup)?;

        // Atomic write
        let tmp_path = backup_path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &json)?;
        std::fs::rename(&tmp_path, &backup_path)?;

        Ok(backup_path)
    }

    /// Load a backup from file
    pub fn load_backup(&self, backup_path: &Path) -> Result<DownloadBackup, BackupError> {
        let json = std::fs::read_to_string(backup_path)?;
        let backup: DownloadBackup = serde_json::from_str(&json)?;

        if backup.version == 0 || backup.version > BACKUP_VERSION {
            return Err(BackupError::UnsupportedVersion(backup.version));
        }

        Ok(backup)
    }

    /// List all available backups in the data directory
    pub fn list_backups(&self) -> Result<Vec<BackupInfo>, BackupError> {
        let mut backups = Vec::new();

        if !self.data_dir.exists() {
            return Ok(backups);
        }

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json")
                && path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| s.starts_with("backup_"))
            {
                if let Ok(backup) = self.load_backup(&path) {
                    backups.push(BackupInfo {
                        path,
                        created_at: backup.created_at,
                        description: backup.description,
                        task_count: backup.tasks.tasks.len(),
                        config_count: backup.configs.count_some(),
                    });
                }
            }
        }

        // Sort by creation time, newest first
        backups.sort_by_key(|b| std::cmp::Reverse(b.created_at));

        Ok(backups)
    }

    /// Delete a backup file
    pub fn delete_backup(&self, backup_path: &Path) -> Result<(), BackupError> {
        if !backup_path.exists() {
            return Err(BackupError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "backup file not found",
            )));
        }

        std::fs::remove_file(backup_path)?;
        Ok(())
    }
}

/// Information about a backup file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    pub path: PathBuf,
    pub created_at: DateTime<Utc>,
    pub description: Option<String>,
    pub task_count: usize,
    pub config_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DownloadPriority, DownloadProtocol, DownloadState, DownloadTask};
    use chrono::Utc;
    use std::path::PathBuf;

    fn make_test_task(id: &str, name: &str) -> DownloadTask {
        DownloadTask {
            id: id.to_string(),
            name: name.to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 1024,
            downloaded: 512,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 100.0,
            save_path: PathBuf::from("/tmp/downloads"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["test".to_string()],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            current_session_start: None,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        }
    }

    #[test]
    fn test_create_and_load_backup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.mp4"),
        ];
        let exported: Vec<crate::task_export::ExportedTask> = tasks
            .into_iter()
            .map(crate::task_export::ExportedTask::from)
            .collect();

        let mut generations = HashMap::new();
        generations.insert("test".to_string(), 42);

        let configs = BackupConfigs {
            auto_cleanup: Some(crate::auto_cleanup::AutoCleanupConfig::default()),
            ..Default::default()
        };

        let backup_path = manager
            .create_backup(
                Some("test backup".to_string()),
                exported,
                generations,
                configs,
            )
            .unwrap();

        assert!(backup_path.exists());
        assert!(
            backup_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("backup_")
        );

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert_eq!(loaded.version, BACKUP_VERSION);
        assert_eq!(loaded.tasks.tasks.len(), 2);
        assert_eq!(loaded.tasks.generations.get("test"), Some(&42));
        assert!(loaded.configs.auto_cleanup.is_some());
        assert_eq!(loaded.description, Some("test backup".to_string()));
    }

    #[test]
    fn test_list_backups() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        // Initially empty
        let backups = manager.list_backups().unwrap();
        assert!(backups.is_empty());

        // Create a backup
        let configs = BackupConfigs::default();
        manager
            .create_backup(None, Vec::new(), HashMap::new(), configs)
            .unwrap();

        // Should list one backup
        let backups = manager.list_backups().unwrap();
        assert_eq!(backups.len(), 1);
        assert_eq!(backups[0].task_count, 0);
        assert_eq!(backups[0].config_count, 0);
    }

    #[test]
    fn test_delete_backup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, Vec::new(), HashMap::new(), configs)
            .unwrap();

        assert!(backup_path.exists());

        manager.delete_backup(&backup_path).unwrap();
        assert!(!backup_path.exists());
    }

    #[test]
    fn test_delete_nonexistent_backup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let fake_path = temp_dir.path().join("nonexistent.json");
        let result = manager.delete_backup(&fake_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_backup_already_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, Vec::new(), HashMap::new(), configs.clone())
            .unwrap();

        // Try to create another backup at the same timestamp (should fail)
        // Note: This test might be flaky if it runs across a second boundary
        let result = manager.create_backup(None, Vec::new(), HashMap::new(), configs);
        // Either it succeeds (different timestamp) or fails with AlreadyExists
        match result {
            Ok(_) => {}                              // Different timestamp, OK
            Err(BackupError::AlreadyExists(_)) => {} // Same timestamp, expected
            Err(e) => panic!("Unexpected error: {:?}", e),
        }

        // Clean up
        let _ = manager.delete_backup(&backup_path);
    }

    #[test]
    fn test_load_invalid_backup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let invalid_path = temp_dir.path().join("invalid.json");
        std::fs::write(&invalid_path, "not json").unwrap();

        let result = manager.load_backup(&invalid_path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BackupError::Serialize(_)));
    }

    #[test]
    fn test_load_unsupported_version() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let future_path = temp_dir.path().join("future.json");
        let content = r#"{"version":999,"created_at":"2026-01-01T00:00:00Z","source":"test","tasks":{"tasks":[],"generations":{}},"configs":{}}"#;
        std::fs::write(&future_path, content).unwrap();

        let result = manager.load_backup(&future_path);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BackupError::UnsupportedVersion(999)
        ));
    }

    #[test]
    fn test_count_configs() {
        let mut configs = BackupConfigs::default();
        assert_eq!(configs.count_some(), 0);

        configs.auto_cleanup = Some(crate::auto_cleanup::AutoCleanupConfig::default());
        assert_eq!(configs.count_some(), 1);

        configs.auto_pause = Some(crate::auto_pause::AutoPauseConfig::default());
        assert_eq!(configs.count_some(), 2);
    }

    #[test]
    fn test_backup_with_all_configs() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs {
            auto_cleanup: Some(crate::auto_cleanup::AutoCleanupConfig::default()),
            auto_pause: Some(crate::auto_pause::AutoPauseConfig::default()),
            bandwidth_allocation: Some(crate::bandwidth_allocation::AllocationConfig::default()),
            cooldown: Some(crate::download_cooldown::CooldownConfig::default()),
            data_cap: Some(crate::data_cap::DataCapConfig::default()),
            domain_limit: Some(crate::domain_limit::DomainLimitConfig::default()),
            error_recovery: Some(crate::error_recovery::ErrorRecoveryConfig::default()),
            network_aware: Some(crate::network_aware::NetworkAwareConfig::default()),
            priority_aging: Some(crate::priority_aging::PriorityAgingConfig::default()),
            queue_completion: Some(crate::queue_completion::QueueCompletionConfig::default()),
            recycle_bin: Some(crate::recycle_bin::RecycleBinConfig::default()),
            resume_policy: Some(crate::resume_policy::ResumePolicyConfig::default()),
            speed_alert: Some(crate::speed_alert::SpeedAlertConfig::default()),
            url_dedup: Some(crate::url_dedup::DedupConfig::default()),
            ..Default::default()
        };

        let backup_path = manager
            .create_backup(None, Vec::new(), HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert!(loaded.configs.auto_cleanup.is_some());
        assert!(loaded.configs.auto_pause.is_some());
        assert!(loaded.configs.bandwidth_allocation.is_some());
        assert!(loaded.configs.cooldown.is_some());
        assert!(loaded.configs.data_cap.is_some());
        assert!(loaded.configs.domain_limit.is_some());
        assert!(loaded.configs.error_recovery.is_some());
        assert!(loaded.configs.network_aware.is_some());
        assert!(loaded.configs.priority_aging.is_some());
        assert!(loaded.configs.queue_completion.is_some());
        assert!(loaded.configs.recycle_bin.is_some());
        assert!(loaded.configs.resume_policy.is_some());
        assert!(loaded.configs.speed_alert.is_some());
        assert!(loaded.configs.url_dedup.is_some());
    }

    #[test]
    fn test_backup_atomic_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, Vec::new(), HashMap::new(), configs)
            .unwrap();

        // Verify no .tmp file left behind
        let tmp_path = backup_path.with_extension("json.tmp");
        assert!(!tmp_path.exists());

        // Verify file is valid JSON
        let content = std::fs::read_to_string(&backup_path).unwrap();
        let _: DownloadBackup = serde_json::from_str(&content).unwrap();
    }

    #[test]
    fn test_backup_with_newly_integrated_configs() {
        // Test that all the previously-None config fields can be serialized/deserialized
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs {
            automation_rules: Some(crate::automation_rules::AutomationConfig::default()),
            disk_monitor: Some(crate::disk_monitor::DiskMonitorConfig::default()),
            download_analytics: Some(crate::download_analytics::AnalyticsConfig::default()),
            download_budget: Some(crate::download_budget::BudgetConfig::default()),
            download_deadline: Some(crate::download_deadline::DeadlineConfig::default()),
            download_presets: Some(vec![]),
            download_quota: Some(crate::download_quota::QuotaSystemConfig::default()),
            download_time_limit: Some(
                crate::download_time_limit::DownloadTimeLimitConfig::default(),
            ),
            duplicate_detection: Some(
                crate::duplicate_detection::DuplicateDetectionConfig::default(),
            ),
            global_budget: Some(crate::global_budget::GlobalBudgetConfig::default()),
            integrity: Some(crate::integrity_verification::IntegrityConfig::default()),
            path_rules: Some(vec![]),
            path_template: Some(crate::path_template::PathTemplateConfig::default()),
            protocol_limits: Some(crate::protocol_limits::ProtocolLimitsConfig::new()),
            save_path: Some(crate::save_path_manager::SavePathConfig::default()),
            speed_profiles: Some(vec![]),
            task_chains: Some(vec![]),
            task_schedule_windows: Some(vec![]),
            url_bookmarks: Some(vec![]),
            url_normalizer: Some(crate::url_normalizer::UrlNormalizerConfig::default()),
            url_rewrite: Some(vec![]),
            watch_folder: Some(crate::watch_folder::WatchFolderAutoScanConfig::default()),
            categorize_rules: Some(vec![]),
            conflict_strategy: Some(crate::conflict_detection::ConflictStrategy::default()),
            bandwidth_schedule: Some(vec![]),
            ..Default::default()
        };

        let backup_path = manager
            .create_backup(None, Vec::new(), HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();

        // Verify all newly integrated configs are present
        assert!(loaded.configs.automation_rules.is_some());
        assert!(loaded.configs.disk_monitor.is_some());
        assert!(loaded.configs.download_analytics.is_some());
        assert!(loaded.configs.download_budget.is_some());
        assert!(loaded.configs.download_deadline.is_some());
        assert!(loaded.configs.download_presets.is_some());
        assert!(loaded.configs.download_quota.is_some());
        assert!(loaded.configs.download_time_limit.is_some());
        assert!(loaded.configs.duplicate_detection.is_some());
        assert!(loaded.configs.global_budget.is_some());
        assert!(loaded.configs.integrity.is_some());
        assert!(loaded.configs.path_rules.is_some());
        assert!(loaded.configs.path_template.is_some());
        assert!(loaded.configs.protocol_limits.is_some());
        assert!(loaded.configs.save_path.is_some());
        assert!(loaded.configs.speed_profiles.is_some());
        assert!(loaded.configs.task_chains.is_some());
        assert!(loaded.configs.task_schedule_windows.is_some());
        assert!(loaded.configs.url_bookmarks.is_some());
        assert!(loaded.configs.url_normalizer.is_some());
        assert!(loaded.configs.url_rewrite.is_some());
        assert!(loaded.configs.watch_folder.is_some());
        assert!(loaded.configs.categorize_rules.is_some());
        assert!(loaded.configs.conflict_strategy.is_some());
        assert!(loaded.configs.bandwidth_schedule.is_some());

        // Verify count_some() includes all fields
        assert!(loaded.configs.count_some() >= 25);
    }

    #[test]
    fn test_backup_config_roundtrip_values() {
        // Test that specific config values survive the backup/restore cycle
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let mut disk_config = crate::disk_monitor::DiskMonitorConfig::default();
        disk_config.safety_margin_bytes = 500_000_000; // 500MB
        disk_config.check_interval_secs = 60;

        let mut normalizer_config = crate::url_normalizer::UrlNormalizerConfig::default();
        normalizer_config.remove_www = false;

        let conflict = crate::conflict_detection::ConflictStrategy::Rename;

        let configs = BackupConfigs {
            disk_monitor: Some(disk_config.clone()),
            url_normalizer: Some(normalizer_config.clone()),
            conflict_strategy: Some(conflict),
            ..Default::default()
        };

        let backup_path = manager
            .create_backup(None, Vec::new(), HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();

        // Verify values survived roundtrip
        let loaded_disk = loaded.configs.disk_monitor.unwrap();
        assert_eq!(loaded_disk.safety_margin_bytes, 500_000_000);
        assert_eq!(loaded_disk.check_interval_secs, 60);

        let loaded_normalizer = loaded.configs.url_normalizer.unwrap();
        assert!(!loaded_normalizer.remove_www);

        let loaded_conflict = loaded.configs.conflict_strategy.unwrap();
        assert_eq!(
            loaded_conflict,
            crate::conflict_detection::ConflictStrategy::Rename
        );
    }
}
