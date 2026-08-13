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

    // ==================== Phase 207: Comprehensive Test Coverage ====================

    // --- Constants ---

    #[test]
    fn test_backup_version_value() {
        assert_eq!(BACKUP_VERSION, 1);
    }

    // --- DownloadBackup serialization ---

    #[test]
    fn test_download_backup_serde_roundtrip() {
        let backup = DownloadBackup {
            version: 1,
            created_at: Utc::now(),
            description: Some("test".to_string()),
            source: "ipmsg-torrent".to_string(),
            tasks: BackupTasks::default(),
            configs: BackupConfigs::default(),
        };
        let json = serde_json::to_string(&backup).unwrap();
        let loaded: DownloadBackup = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.source, "ipmsg-torrent");
        assert_eq!(loaded.description, Some("test".to_string()));
    }

    #[test]
    fn test_download_backup_pretty_json() {
        let backup = DownloadBackup {
            version: 1,
            created_at: Utc::now(),
            description: None,
            source: "ipmsg-torrent".to_string(),
            tasks: BackupTasks::default(),
            configs: BackupConfigs::default(),
        };
        let pretty = serde_json::to_string_pretty(&backup).unwrap();
        assert!(pretty.contains('\n'));
        assert!(pretty.contains("\"version\": 1"));
    }

    #[test]
    fn test_download_backup_extra_fields_ignored() {
        let json = r#"{
            "version": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "source": "test",
            "tasks": {"tasks": [], "generations": {}},
            "configs": {},
            "unknown_field": "should be ignored",
            "another_extra": 42
        }"#;
        let backup: DownloadBackup = serde_json::from_str(json).unwrap();
        assert_eq!(backup.version, 1);
        assert_eq!(backup.source, "test");
    }

    #[test]
    fn test_download_backup_optional_description() {
        // With description
        let json_with = r#"{
            "version": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "description": "my backup",
            "source": "test",
            "tasks": {"tasks": [], "generations": {}},
            "configs": {}
        }"#;
        let b1: DownloadBackup = serde_json::from_str(json_with).unwrap();
        assert_eq!(b1.description, Some("my backup".to_string()));

        // Without description (missing field)
        let json_without = r#"{
            "version": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "source": "test",
            "tasks": {"tasks": [], "generations": {}},
            "configs": {}
        }"#;
        let b2: DownloadBackup = serde_json::from_str(json_without).unwrap();
        assert_eq!(b2.description, None);
    }

    #[test]
    fn test_download_backup_unicode_description() {
        let backup = DownloadBackup {
            version: 1,
            created_at: Utc::now(),
            description: Some("备份文件 🗂️ 中文".to_string()),
            source: "ipmsg-torrent".to_string(),
            tasks: BackupTasks::default(),
            configs: BackupConfigs::default(),
        };
        let json = serde_json::to_string(&backup).unwrap();
        let loaded: DownloadBackup = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.description, Some("备份文件 🗂️ 中文".to_string()));
    }

    // --- BackupTasks ---

    #[test]
    fn test_backup_tasks_default() {
        let bt = BackupTasks::default();
        assert!(bt.tasks.is_empty());
        assert!(bt.generations.is_empty());
    }

    #[test]
    fn test_backup_tasks_serde_roundtrip() {
        let mut generations = HashMap::new();
        generations.insert("gen1".to_string(), 10);
        generations.insert("gen2".to_string(), 20);
        let bt = BackupTasks {
            tasks: vec![],
            generations,
        };
        let json = serde_json::to_string(&bt).unwrap();
        let loaded: BackupTasks = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.generations.get("gen1"), Some(&10));
        assert_eq!(loaded.generations.get("gen2"), Some(&20));
    }

    #[test]
    fn test_backup_tasks_with_exported_tasks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let tasks = vec![
            make_test_task("t1", "file1.txt"),
            make_test_task("t2", "file2.mp4"),
            make_test_task("t3", "file3.zip"),
        ];
        let exported: Vec<crate::task_export::ExportedTask> = tasks
            .into_iter()
            .map(crate::task_export::ExportedTask::from)
            .collect();

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, exported, HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert_eq!(loaded.tasks.tasks.len(), 3);
    }

    #[test]
    fn test_backup_tasks_generations_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let mut generations = HashMap::new();
        generations.insert("queue".to_string(), 42);
        generations.insert("archive".to_string(), 7);

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, vec![], generations, configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert_eq!(loaded.tasks.generations.get("queue"), Some(&42));
        assert_eq!(loaded.tasks.generations.get("archive"), Some(&7));
    }

    // --- BackupConfigs ---

    #[test]
    fn test_backup_configs_default_all_none() {
        let configs = BackupConfigs::default();
        assert_eq!(configs.count_some(), 0);
        assert!(configs.auto_cleanup.is_none());
        assert!(configs.auto_pause.is_none());
        assert!(configs.automation_rules.is_none());
        assert!(configs.bandwidth_allocation.is_none());
        assert!(configs.bandwidth_schedule.is_none());
        assert!(configs.categorize_rules.is_none());
        assert!(configs.conflict_strategy.is_none());
        assert!(configs.cooldown.is_none());
        assert!(configs.data_cap.is_none());
        assert!(configs.dependency_graph.is_none());
        assert!(configs.disk_monitor.is_none());
        assert!(configs.domain_limit.is_none());
        assert!(configs.download_analytics.is_none());
        assert!(configs.download_budget.is_none());
        assert!(configs.download_deadline.is_none());
        assert!(configs.download_presets.is_none());
        assert!(configs.download_quota.is_none());
        assert!(configs.download_time_limit.is_none());
        assert!(configs.duplicate_detection.is_none());
        assert!(configs.error_recovery.is_none());
        assert!(configs.global_budget.is_none());
        assert!(configs.integrity.is_none());
        assert!(configs.network_aware.is_none());
        assert!(configs.path_rules.is_none());
        assert!(configs.path_template.is_none());
        assert!(configs.priority_aging.is_none());
        assert!(configs.protocol_limits.is_none());
        assert!(configs.queue_completion.is_none());
        assert!(configs.queue_staleness.is_none());
        assert!(configs.recycle_bin.is_none());
        assert!(configs.resume_policy.is_none());
        assert!(configs.save_path.is_none());
        assert!(configs.speed_alert.is_none());
        assert!(configs.speed_profiles.is_none());
        assert!(configs.task_chains.is_none());
        assert!(configs.task_schedule_windows.is_none());
        assert!(configs.url_allowlist.is_none());
        assert!(configs.url_bookmarks.is_none());
        assert!(configs.url_dedup.is_none());
        assert!(configs.url_normalizer.is_none());
        assert!(configs.url_rewrite.is_none());
        assert!(configs.watch_folder.is_none());
    }

    #[test]
    fn test_backup_configs_count_some_all_fields() {
        let configs = BackupConfigs {
            auto_cleanup: Some(crate::auto_cleanup::AutoCleanupConfig::default()),
            auto_pause: Some(crate::auto_pause::AutoPauseConfig::default()),
            automation_rules: Some(crate::automation_rules::AutomationConfig::default()),
            bandwidth_allocation: Some(crate::bandwidth_allocation::AllocationConfig::default()),
            bandwidth_schedule: Some(vec![]),
            categorize_rules: Some(vec![]),
            conflict_strategy: Some(crate::conflict_detection::ConflictStrategy::default()),
            cooldown: Some(crate::download_cooldown::CooldownConfig::default()),
            data_cap: Some(crate::data_cap::DataCapConfig::default()),
            dependency_graph: Some(crate::dependency_graph::DependencyGraphConfig::default()),
            disk_monitor: Some(crate::disk_monitor::DiskMonitorConfig::default()),
            domain_limit: Some(crate::domain_limit::DomainLimitConfig::default()),
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
            error_recovery: Some(crate::error_recovery::ErrorRecoveryConfig::default()),
            global_budget: Some(crate::global_budget::GlobalBudgetConfig::default()),
            integrity: Some(crate::integrity_verification::IntegrityConfig::default()),
            network_aware: Some(crate::network_aware::NetworkAwareConfig::default()),
            path_rules: Some(vec![]),
            path_template: Some(crate::path_template::PathTemplateConfig::default()),
            priority_aging: Some(crate::priority_aging::PriorityAgingConfig::default()),
            protocol_limits: Some(crate::protocol_limits::ProtocolLimitsConfig::new()),
            queue_completion: Some(crate::queue_completion::QueueCompletionConfig::default()),
            queue_staleness: Some(crate::queue_staleness::StalenessConfig::default()),
            recycle_bin: Some(crate::recycle_bin::RecycleBinConfig::default()),
            resume_policy: Some(crate::resume_policy::ResumePolicyConfig::default()),
            save_path: Some(crate::save_path_manager::SavePathConfig::default()),
            speed_alert: Some(crate::speed_alert::SpeedAlertConfig::default()),
            speed_profiles: Some(vec![]),
            task_chains: Some(vec![]),
            task_schedule_windows: Some(vec![]),
            url_allowlist: Some(crate::url_allowlist::AllowlistConfig::default()),
            url_bookmarks: Some(vec![]),
            url_dedup: Some(crate::url_dedup::DedupConfig::default()),
            url_normalizer: Some(crate::url_normalizer::UrlNormalizerConfig::default()),
            url_rewrite: Some(vec![]),
            watch_folder: Some(crate::watch_folder::WatchFolderAutoScanConfig::default()),
        };
        // All 42 fields should be Some
        assert_eq!(configs.count_some(), 42);
    }

    #[test]
    fn test_backup_configs_serde_roundtrip() {
        let mut configs = BackupConfigs::default();
        configs.auto_cleanup = Some(crate::auto_cleanup::AutoCleanupConfig::default());
        configs.domain_limit = Some(crate::domain_limit::DomainLimitConfig::default());

        let json = serde_json::to_string(&configs).unwrap();
        let loaded: BackupConfigs = serde_json::from_str(&json).unwrap();
        assert!(loaded.auto_cleanup.is_some());
        assert!(loaded.domain_limit.is_some());
        assert!(loaded.auto_pause.is_none());
    }

    #[test]
    fn test_backup_configs_extra_fields_ignored() {
        let json = r#"{"future_config": 42, "another": true}"#;
        let configs: BackupConfigs = serde_json::from_str(json).unwrap();
        assert_eq!(configs.count_some(), 0);
    }

    // --- BackupError ---

    #[test]
    fn test_backup_error_io_display() {
        let err = BackupError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("IO error"));
    }

    #[test]
    fn test_backup_error_serialize_display() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err = BackupError::Serialize(json_err);
        let msg = format!("{}", err);
        assert!(msg.contains("serialize"));
    }

    #[test]
    fn test_backup_error_unsupported_version_display() {
        let err = BackupError::UnsupportedVersion(99);
        let msg = format!("{}", err);
        assert!(msg.contains("99"));
        assert!(msg.contains("unsupported"));
    }

    #[test]
    fn test_backup_error_invalid_file_display() {
        let err = BackupError::InvalidFile("corrupt data".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("corrupt data"));
    }

    #[test]
    fn test_backup_error_already_exists_display() {
        let path = PathBuf::from("/tmp/backup.json");
        let err = BackupError::AlreadyExists(path);
        let msg = format!("{}", err);
        assert!(msg.contains("already exists"));
    }

    #[test]
    fn test_backup_error_io_debug() {
        let err = BackupError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "no access",
        ));
        let debug = format!("{:?}", err);
        assert!(debug.contains("Io"));
    }

    #[test]
    fn test_backup_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err: BackupError = BackupError::from(io_err);
        assert!(matches!(err, BackupError::Io(_)));
    }

    #[test]
    fn test_backup_error_from_serde() {
        let serde_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let err: BackupError = BackupError::from(serde_err);
        assert!(matches!(err, BackupError::Serialize(_)));
    }

    // --- BackupManager: create_backup ---

    #[test]
    fn test_create_backup_no_description() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, vec![], HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert_eq!(loaded.description, None);
        assert_eq!(loaded.source, "ipmsg-torrent");
    }

    #[test]
    fn test_create_backup_with_description() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(
                Some("weekly backup".to_string()),
                vec![],
                HashMap::new(),
                configs,
            )
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert_eq!(loaded.description, Some("weekly backup".to_string()));
    }

    #[test]
    fn test_create_backup_filename_format() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, vec![], HashMap::new(), configs)
            .unwrap();

        let filename = backup_path.file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with("backup_"));
        assert!(filename.ends_with(".json"));
        // Should contain timestamp like backup_20260814_034300.json
        assert!(filename.len() > "backup_.json".len());
    }

    #[test]
    fn test_create_backup_empty_tasks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, vec![], HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert!(loaded.tasks.tasks.is_empty());
        assert!(loaded.tasks.generations.is_empty());
    }

    #[test]
    fn test_create_backup_preserves_task_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let task = make_test_task("abc-123", "测试文件.mp4");
        let exported = crate::task_export::ExportedTask::from(task);

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, vec![exported], HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert_eq!(loaded.tasks.tasks.len(), 1);
        assert_eq!(loaded.tasks.tasks[0].id, "abc-123");
        assert_eq!(loaded.tasks.tasks[0].name, "测试文件.mp4");
    }

    // --- BackupManager: load_backup ---

    #[test]
    fn test_load_backup_file_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let fake_path = temp_dir.path().join("does_not_exist.json");
        let result = manager.load_backup(&fake_path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BackupError::Io(_)));
    }

    #[test]
    fn test_load_backup_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let empty_path = temp_dir.path().join("empty.json");
        std::fs::write(&empty_path, "").unwrap();

        let result = manager.load_backup(&empty_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_backup_version_zero() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let path = temp_dir.path().join("v0.json");
        let content = r#"{"version":0,"created_at":"2026-01-01T00:00:00Z","source":"test","tasks":{"tasks":[],"generations":{}},"configs":{}}"#;
        std::fs::write(&path, content).unwrap();

        let result = manager.load_backup(&path);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BackupError::UnsupportedVersion(0)
        ));
    }

    #[test]
    fn test_load_backup_valid_version_1() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let path = temp_dir.path().join("v1.json");
        let content = r#"{"version":1,"created_at":"2026-01-01T00:00:00Z","source":"test","tasks":{"tasks":[],"generations":{}},"configs":{}}"#;
        std::fs::write(&path, content).unwrap();

        let backup = manager.load_backup(&path).unwrap();
        assert_eq!(backup.version, 1);
    }

    // --- BackupManager: list_backups ---

    #[test]
    fn test_list_backups_nonexistent_dir() {
        let manager = BackupManager::new(PathBuf::from("/nonexistent/path/that/does/not/exist"));
        let backups = manager.list_backups().unwrap();
        assert!(backups.is_empty());
    }

    #[test]
    fn test_list_backups_ignores_non_backup_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        // Create non-backup JSON files
        std::fs::write(temp_dir.path().join("config.json"), r#"{"key": "value"}"#).unwrap();
        std::fs::write(temp_dir.path().join("data.json"), r#"[1,2,3]"#).unwrap();

        let backups = manager.list_backups().unwrap();
        assert!(backups.is_empty());
    }

    #[test]
    fn test_list_backups_sorted_newest_first() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        // Create first backup
        let configs = BackupConfigs::default();
        manager
            .create_backup(
                Some("first".to_string()),
                vec![],
                HashMap::new(),
                configs.clone(),
            )
            .unwrap();

        // Wait a second to ensure different timestamp
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Create second backup
        manager
            .create_backup(Some("second".to_string()), vec![], HashMap::new(), configs)
            .unwrap();

        let backups = manager.list_backups().unwrap();
        assert_eq!(backups.len(), 2);
        // Newest first
        assert!(backups[0].created_at >= backups[1].created_at);
    }

    #[test]
    fn test_list_backups_info_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let task = make_test_task("t1", "file1.txt");
        let exported = crate::task_export::ExportedTask::from(task);

        let mut configs = BackupConfigs::default();
        configs.auto_cleanup = Some(crate::auto_cleanup::AutoCleanupConfig::default());

        let backup_path = manager
            .create_backup(
                Some("info test".to_string()),
                vec![exported],
                HashMap::new(),
                configs,
            )
            .unwrap();

        let backups = manager.list_backups().unwrap();
        assert_eq!(backups.len(), 1);
        assert_eq!(backups[0].path, backup_path);
        assert_eq!(backups[0].task_count, 1);
        assert_eq!(backups[0].config_count, 1);
        assert_eq!(backups[0].description, Some("info test".to_string()));
    }

    #[test]
    fn test_list_backups_skips_corrupt_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        // Create a corrupt backup file
        let corrupt_path = temp_dir.path().join("backup_corrupt.json");
        std::fs::write(&corrupt_path, "not valid json").unwrap();

        // Create a valid backup
        let configs = BackupConfigs::default();
        manager
            .create_backup(None, vec![], HashMap::new(), configs)
            .unwrap();

        // Should only list the valid one
        let backups = manager.list_backups().unwrap();
        assert_eq!(backups.len(), 1);
    }

    // --- BackupManager: delete_backup ---

    #[test]
    fn test_delete_backup_removes_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, vec![], HashMap::new(), configs)
            .unwrap();

        assert!(backup_path.exists());
        manager.delete_backup(&backup_path).unwrap();
        assert!(!backup_path.exists());
    }

    #[test]
    fn test_delete_backup_twice_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, vec![], HashMap::new(), configs)
            .unwrap();

        manager.delete_backup(&backup_path).unwrap();
        let result = manager.delete_backup(&backup_path);
        assert!(result.is_err());
    }

    // --- BackupConfigs: individual field tests ---

    #[test]
    fn test_backup_configs_each_field_individually() {
        // Test that each config field can be set and counted independently
        let fields: Vec<(&str, Box<dyn Fn(&mut BackupConfigs)>)> = vec![
            (
                "auto_cleanup",
                Box::new(|c| {
                    c.auto_cleanup = Some(crate::auto_cleanup::AutoCleanupConfig::default())
                }),
            ),
            (
                "auto_pause",
                Box::new(|c| c.auto_pause = Some(crate::auto_pause::AutoPauseConfig::default())),
            ),
            (
                "cooldown",
                Box::new(|c| {
                    c.cooldown = Some(crate::download_cooldown::CooldownConfig::default())
                }),
            ),
            (
                "data_cap",
                Box::new(|c| c.data_cap = Some(crate::data_cap::DataCapConfig::default())),
            ),
            (
                "domain_limit",
                Box::new(|c| {
                    c.domain_limit = Some(crate::domain_limit::DomainLimitConfig::default())
                }),
            ),
            (
                "error_recovery",
                Box::new(|c| {
                    c.error_recovery = Some(crate::error_recovery::ErrorRecoveryConfig::default())
                }),
            ),
            (
                "network_aware",
                Box::new(|c| {
                    c.network_aware = Some(crate::network_aware::NetworkAwareConfig::default())
                }),
            ),
            (
                "priority_aging",
                Box::new(|c| {
                    c.priority_aging = Some(crate::priority_aging::PriorityAgingConfig::default())
                }),
            ),
            (
                "queue_completion",
                Box::new(|c| {
                    c.queue_completion =
                        Some(crate::queue_completion::QueueCompletionConfig::default())
                }),
            ),
            (
                "recycle_bin",
                Box::new(|c| c.recycle_bin = Some(crate::recycle_bin::RecycleBinConfig::default())),
            ),
            (
                "resume_policy",
                Box::new(|c| {
                    c.resume_policy = Some(crate::resume_policy::ResumePolicyConfig::default())
                }),
            ),
            (
                "speed_alert",
                Box::new(|c| c.speed_alert = Some(crate::speed_alert::SpeedAlertConfig::default())),
            ),
            (
                "url_dedup",
                Box::new(|c| c.url_dedup = Some(crate::url_dedup::DedupConfig::default())),
            ),
        ];

        for (name, setter) in &fields {
            let mut configs = BackupConfigs::default();
            setter(&mut configs);
            assert_eq!(
                configs.count_some(),
                1,
                "Field '{}' should make count_some() == 1",
                name
            );
        }
    }

    // --- BackupInfo serialization ---

    #[test]
    fn test_backup_info_serde_roundtrip() {
        let info = BackupInfo {
            path: PathBuf::from("/tmp/backup_20260101.json"),
            created_at: Utc::now(),
            description: Some("test info".to_string()),
            task_count: 5,
            config_count: 10,
        };
        let json = serde_json::to_string(&info).unwrap();
        let loaded: BackupInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.path, PathBuf::from("/tmp/backup_20260101.json"));
        assert_eq!(loaded.task_count, 5);
        assert_eq!(loaded.config_count, 10);
        assert_eq!(loaded.description, Some("test info".to_string()));
    }

    // --- Clone/Debug traits ---

    #[test]
    fn test_download_backup_clone() {
        let backup = DownloadBackup {
            version: 1,
            created_at: Utc::now(),
            description: Some("cloneable".to_string()),
            source: "test".to_string(),
            tasks: BackupTasks::default(),
            configs: BackupConfigs::default(),
        };
        let cloned = backup.clone();
        assert_eq!(cloned.version, 1);
        assert_eq!(cloned.source, "test");
        assert_eq!(cloned.description, Some("cloneable".to_string()));
    }

    #[test]
    fn test_download_backup_debug() {
        let backup = DownloadBackup {
            version: 1,
            created_at: Utc::now(),
            description: None,
            source: "test".to_string(),
            tasks: BackupTasks::default(),
            configs: BackupConfigs::default(),
        };
        let debug = format!("{:?}", backup);
        assert!(debug.contains("DownloadBackup"));
        assert!(debug.contains("version: 1"));
    }

    #[test]
    fn test_backup_tasks_clone() {
        let mut bt = BackupTasks::default();
        bt.generations.insert("g1".to_string(), 5);
        let cloned = bt.clone();
        assert_eq!(cloned.generations.get("g1"), Some(&5));
    }

    #[test]
    fn test_backup_configs_clone() {
        let mut configs = BackupConfigs::default();
        configs.auto_cleanup = Some(crate::auto_cleanup::AutoCleanupConfig::default());
        let cloned = configs.clone();
        assert!(cloned.auto_cleanup.is_some());
    }

    #[test]
    fn test_backup_info_clone() {
        let info = BackupInfo {
            path: PathBuf::from("/test/path.json"),
            created_at: Utc::now(),
            description: None,
            task_count: 3,
            config_count: 7,
        };
        let cloned = info.clone();
        assert_eq!(cloned.task_count, 3);
        assert_eq!(cloned.config_count, 7);
    }

    // --- Edge cases ---

    #[test]
    fn test_backup_many_tasks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let tasks: Vec<crate::task_export::ExportedTask> = (0..50)
            .map(|i| make_test_task(&format!("task-{}", i), &format!("file{}.txt", i)))
            .map(crate::task_export::ExportedTask::from)
            .collect();

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, tasks, HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert_eq!(loaded.tasks.tasks.len(), 50);
    }

    #[test]
    fn test_backup_overwrite_after_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(None, vec![], HashMap::new(), configs.clone())
            .unwrap();

        // Delete and recreate
        manager.delete_backup(&backup_path).unwrap();

        // Wait for different timestamp
        std::thread::sleep(std::time::Duration::from_secs(2));

        let new_path = manager
            .create_backup(
                Some("new version".to_string()),
                vec![],
                HashMap::new(),
                configs,
            )
            .unwrap();

        let loaded = manager.load_backup(&new_path).unwrap();
        assert_eq!(loaded.description, Some("new version".to_string()));
    }

    #[test]
    fn test_backup_empty_json_object() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        // Minimal valid JSON that can deserialize as DownloadBackup
        let path = temp_dir.path().join("minimal.json");
        let content = r#"{"version":1,"created_at":"2026-01-01T00:00:00Z","source":"test","tasks":{"tasks":[],"generations":{}},"configs":{}}"#;
        std::fs::write(&path, content).unwrap();

        let backup = manager.load_backup(&path).unwrap();
        assert_eq!(backup.version, 1);
        assert!(backup.tasks.tasks.is_empty());
        assert_eq!(backup.configs.count_some(), 0);
    }

    #[test]
    fn test_backup_special_characters_in_description() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let desc = "Special chars: \"quotes\" \\slashes\" \n\ttabs 日本語 한국어 العربية";
        let configs = BackupConfigs::default();
        let backup_path = manager
            .create_backup(Some(desc.to_string()), vec![], HashMap::new(), configs)
            .unwrap();

        let loaded = manager.load_backup(&backup_path).unwrap();
        assert_eq!(loaded.description, Some(desc.to_string()));
    }

    #[test]
    fn test_backup_manager_new() {
        let manager = BackupManager::new(PathBuf::from("/test/dir"));
        assert_eq!(manager.data_dir, PathBuf::from("/test/dir"));
    }

    #[test]
    fn test_list_backups_empty_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let backups = manager.list_backups().unwrap();
        assert!(backups.is_empty());
    }

    #[test]
    fn test_backup_no_tmp_leftover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = BackupManager::new(temp_dir.path().to_path_buf());

        let configs = BackupConfigs::default();
        let _backup_path = manager
            .create_backup(None, vec![], HashMap::new(), configs)
            .unwrap();

        // Check no .tmp files in directory
        for entry in std::fs::read_dir(temp_dir.path()).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name_str = name.to_str().unwrap();
            assert!(
                !name_str.ends_with(".tmp"),
                "Found leftover temp file: {}",
                name_str
            );
        }
    }

    #[test]
    fn test_backup_info_debug() {
        let info = BackupInfo {
            path: PathBuf::from("/test.json"),
            created_at: Utc::now(),
            description: None,
            task_count: 0,
            config_count: 0,
        };
        let debug = format!("{:?}", info);
        assert!(debug.contains("BackupInfo"));
    }

    #[test]
    fn test_backup_tasks_debug() {
        let bt = BackupTasks::default();
        let debug = format!("{:?}", bt);
        assert!(debug.contains("BackupTasks"));
    }

    #[test]
    fn test_backup_configs_debug() {
        let configs = BackupConfigs::default();
        let debug = format!("{:?}", configs);
        assert!(debug.contains("BackupConfigs"));
    }
}
