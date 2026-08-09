//! Recycle Bin (Soft Delete) for download tasks (Phase 88)
//!
//! Tasks moved to the recycle bin are preserved with metadata and can be restored.
//! Supports configurable auto-purge of old entries.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::{DownloadProtocol, DownloadState, DownloadTask};

/// A task in the recycle bin
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecycledTask {
    /// Original task data (preserved for restoration)
    pub task: RecycledTaskData,
    /// When the task was moved to the recycle bin
    pub deleted_at: DateTime<Utc>,
    /// Reason for deletion (optional user note)
    pub reason: Option<String>,
}

/// Serializable task data stored in the recycle bin
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecycledTaskData {
    pub id: String,
    pub name: String,
    pub protocol: String,
    pub size: u64,
    pub downloaded: u64,
    pub state: String,
    pub error: Option<String>,
    pub save_path: PathBuf,
    pub created_at: DateTime<Utc>,
    pub tags: Vec<String>,
    pub priority: String,
    pub bandwidth_weight: u8,
    pub queue_position: Option<u32>,
    pub depends_on: Vec<String>,
    pub notes: Option<String>,
    pub group: Option<String>,
    pub speed_limit_bps: Option<u64>,
    pub source_url: Option<String>,
    pub expected_checksum: Option<String>,
    pub checksum_algorithm: Option<String>,
    pub active_time_seconds: f64,
    pub mirror_urls: Vec<String>,
    pub sequential_mode: bool,
}

impl RecycledTaskData {
    /// Create from a DownloadTask
    pub fn from_task(task: &DownloadTask) -> Self {
        Self {
            id: task.id.clone(),
            name: task.name.clone(),
            protocol: format!("{:?}", task.protocol),
            size: task.size,
            downloaded: task.downloaded,
            state: format!("{:?}", task.state),
            error: task.error.clone(),
            save_path: task.save_path.clone(),
            created_at: task.created_at,
            tags: task.tags.clone(),
            priority: format!("{:?}", task.priority),
            bandwidth_weight: task.bandwidth_weight,
            queue_position: task.queue_position,
            depends_on: task.depends_on.clone(),
            notes: task.notes.clone(),
            group: task.group.clone(),
            speed_limit_bps: task.speed_limit_bps,
            source_url: task.source_url.clone(),
            expected_checksum: task.expected_checksum.clone(),
            checksum_algorithm: task.checksum_algorithm.as_ref().map(|a| format!("{:?}", a)),
            active_time_seconds: task.active_time_seconds,
            mirror_urls: task.mirror_urls.clone(),
            sequential_mode: task.sequential_mode,
        }
    }

    /// Restore to a DownloadTask
    pub fn to_task(&self) -> DownloadTask {
        let protocol = parse_protocol(&self.protocol);
        let state = parse_state(&self.state);
        let priority = parse_priority(&self.priority);
        let checksum_algorithm = self.checksum_algorithm.as_ref().map(|a| match a.as_str() {
            "Md5" => crate::checksum::ChecksumAlgorithm::Md5,
            "Sha1" => crate::checksum::ChecksumAlgorithm::Sha1,
            "Sha256" => crate::checksum::ChecksumAlgorithm::Sha256,
            "Ed2k" => crate::checksum::ChecksumAlgorithm::Ed2k,
            _ => crate::checksum::ChecksumAlgorithm::Sha256,
        });

        DownloadTask {
            id: self.id.clone(),
            name: self.name.clone(),
            protocol,
            size: self.size,
            downloaded: self.downloaded,
            state,
            error: self.error.clone(),
            speed_bps: 0.0,
            save_path: self.save_path.clone(),
            created_at: self.created_at,
            updated_at: Utc::now(),
            tags: self.tags.clone(),
            priority,
            schedule: None,
            bandwidth_weight: self.bandwidth_weight,
            queue_position: self.queue_position,
            depends_on: self.depends_on.clone(),
            notes: self.notes.clone(),
            group: self.group.clone(),
            speed_limit_bps: self.speed_limit_bps,
            auto_retry_count: 0,
            retry_after: None,
            source_url: self.source_url.clone(),
            expected_checksum: self.expected_checksum.clone(),
            checksum_algorithm,
            active_time_seconds: self.active_time_seconds,
            current_session_start: None,
            mirror_urls: self.mirror_urls.clone(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: self.sequential_mode,
            max_download_time_secs: None,
        }
    }
}

fn parse_protocol(s: &str) -> DownloadProtocol {
    match s {
        "Http" | "Xunlei" => DownloadProtocol::Xunlei,
        "Torrent" => DownloadProtocol::Torrent,
        "Ed2k" => DownloadProtocol::Ed2k,
        "P2P" => DownloadProtocol::P2P,
        _ => DownloadProtocol::Xunlei,
    }
}

fn parse_state(s: &str) -> DownloadState {
    match s {
        "Queued" => DownloadState::Queued,
        "Downloading" => DownloadState::Downloading,
        "Paused" => DownloadState::Paused,
        "Complete" => DownloadState::Complete,
        "Error" => DownloadState::Error,
        _ => DownloadState::Paused,
    }
}

fn parse_priority(s: &str) -> crate::DownloadPriority {
    match s {
        "Low" => crate::DownloadPriority::Low,
        "High" => crate::DownloadPriority::High,
        _ => crate::DownloadPriority::Normal,
    }
}

/// Recycle bin configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecycleBinConfig {
    /// Enable recycle bin (soft delete). When false, remove_task deletes permanently.
    pub enabled: bool,
    /// Auto-purge entries older than this many seconds (0 = never purge)
    pub auto_purge_after_secs: u64,
    /// Maximum number of recycled tasks (0 = unlimited)
    pub max_entries: usize,
}

impl Default for RecycleBinConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_purge_after_secs: 7 * 24 * 3600, // 7 days
            max_entries: 500,
        }
    }
}

/// Recycle bin state (persisted)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecycleBinState {
    pub config: RecycleBinConfig,
    pub entries: Vec<RecycledTask>,
}

/// Recycle bin manager
#[derive(Default)]
pub struct RecycleBinManager {
    config: RecycleBinConfig,
    entries: Vec<RecycledTask>,
}

impl RecycleBinManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Move a task to the recycle bin
    pub fn recycle(&mut self, task: &DownloadTask, reason: Option<String>) -> RecycledTask {
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(task),
            deleted_at: Utc::now(),
            reason,
        };
        self.entries.push(recycled.clone());
        self.enforce_limits();
        recycled
    }

    /// Restore a task from the recycle bin by ID
    pub fn restore(&mut self, task_id: &str) -> Option<DownloadTask> {
        if let Some(pos) = self.entries.iter().position(|r| r.task.id == task_id) {
            let recycled = self.entries.remove(pos);
            Some(recycled.task.to_task())
        } else {
            None
        }
    }

    /// Permanently delete a recycled task
    pub fn purge_one(&mut self, task_id: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|r| r.task.id != task_id);
        self.entries.len() < before
    }

    /// List all recycled tasks
    pub fn list(&self) -> &[RecycledTask] {
        &self.entries
    }

    /// Get a specific recycled task
    pub fn get(&self, task_id: &str) -> Option<&RecycledTask> {
        self.entries.iter().find(|r| r.task.id == task_id)
    }

    /// Count of recycled tasks
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Auto-purge entries older than configured threshold
    pub fn auto_purge(&mut self) -> usize {
        if self.config.auto_purge_after_secs == 0 {
            return 0;
        }
        let cutoff =
            Utc::now() - chrono::Duration::seconds(self.config.auto_purge_after_secs as i64);
        let before = self.entries.len();
        self.entries.retain(|r| r.deleted_at > cutoff);
        before - self.entries.len()
    }

    /// Empty the entire recycle bin
    pub fn empty(&mut self) -> usize {
        let count = self.entries.len();
        self.entries.clear();
        count
    }

    /// Get/set config
    pub fn config(&self) -> &RecycleBinConfig {
        &self.config
    }

    pub fn set_config(&mut self, config: RecycleBinConfig) {
        self.config = config;
        self.enforce_limits();
    }

    /// Enforce max_entries limit by removing oldest entries
    fn enforce_limits(&mut self) {
        if self.config.max_entries > 0 && self.entries.len() > self.config.max_entries {
            // Sort by deleted_at ascending, keep newest max_entries
            self.entries.sort_by_key(|a| a.deleted_at);
            let excess = self.entries.len() - self.config.max_entries;
            self.entries.drain(..excess);
        }
    }

    /// Load state from disk
    pub fn load_state(&mut self, data_dir: &Path) {
        match load_recycle_bin(data_dir) {
            Ok(state) => {
                self.config = state.config;
                self.entries = state.entries;
            }
            Err(_) => {
                // Use defaults on load failure
            }
        }
    }

    /// Save state to disk
    pub async fn save_state(&self, data_dir: &Path) -> Result<(), RecycleBinError> {
        let state = RecycleBinState {
            config: self.config.clone(),
            entries: self.entries.clone(),
        };
        save_recycle_bin(data_dir, &state)
    }

    /// Get summary statistics
    pub fn summary(&self) -> RecycleBinSummary {
        let total_size: u64 = self.entries.iter().map(|r| r.task.size).sum();
        let total_downloaded: u64 = self.entries.iter().map(|r| r.task.downloaded).sum();
        let oldest = self.entries.iter().map(|r| r.deleted_at).min();
        let newest = self.entries.iter().map(|r| r.deleted_at).max();

        let mut by_protocol: HashMap<String, usize> = HashMap::new();
        for entry in &self.entries {
            *by_protocol.entry(entry.task.protocol.clone()).or_default() += 1;
        }

        RecycleBinSummary {
            total_entries: self.entries.len(),
            total_size,
            total_downloaded,
            oldest_entry: oldest,
            newest_entry: newest,
            by_protocol,
            config_enabled: self.config.enabled,
            auto_purge_after_secs: self.config.auto_purge_after_secs,
            max_entries: self.config.max_entries,
        }
    }
}

/// Summary statistics for the recycle bin
#[derive(Debug, Clone)]
pub struct RecycleBinSummary {
    pub total_entries: usize,
    pub total_size: u64,
    pub total_downloaded: u64,
    pub oldest_entry: Option<DateTime<Utc>>,
    pub newest_entry: Option<DateTime<Utc>>,
    pub by_protocol: HashMap<String, usize>,
    pub config_enabled: bool,
    pub auto_purge_after_secs: u64,
    pub max_entries: usize,
}

/// Persistence errors
#[derive(Debug)]
pub enum RecycleBinError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for RecycleBinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Json(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<std::io::Error> for RecycleBinError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for RecycleBinError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Save recycle bin state to disk (atomic write)
pub fn save_recycle_bin(data_dir: &Path, state: &RecycleBinState) -> Result<(), RecycleBinError> {
    let path = data_dir.join("recycle_bin.json");
    let json = serde_json::to_string_pretty(state)?;
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, json.as_bytes())?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load recycle bin state from disk
pub fn load_recycle_bin(data_dir: &Path) -> Result<RecycleBinState, RecycleBinError> {
    let path = data_dir.join("recycle_bin.json");
    if !path.exists() {
        return Ok(RecycleBinState::default());
    }
    let json = std::fs::read_to_string(&path)?;
    let state: RecycleBinState = serde_json::from_str(&json)?;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DownloadPriority, DownloadProtocol, DownloadState, DownloadTask};
    use std::path::PathBuf;

    fn make_test_task(id: &str, name: &str) -> DownloadTask {
        DownloadTask {
            id: id.to_string(),
            name: name.to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 1_000_000,
            downloaded: 500_000,
            state: DownloadState::Paused,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp/test"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["test".to_string()],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: vec![],
            notes: Some("test notes".to_string()),
            group: Some("test-group".to_string()),
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some("http://example.com/file.zip".to_string()),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 120.0,
            current_session_start: None,
            mirror_urls: vec!["http://mirror.example.com/file.zip".to_string()],
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
        }
    }

    #[test]
    fn test_recycle_and_restore() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "Test File");

        mgr.recycle(&task, Some("no longer needed".to_string()));
        assert_eq!(mgr.len(), 1);
        assert!(mgr.get("t1").is_some());

        let restored = mgr.restore("t1").unwrap();
        assert_eq!(restored.id, "t1");
        assert_eq!(restored.name, "Test File");
        assert_eq!(restored.tags, vec!["test"]);
        assert_eq!(restored.notes, Some("test notes".to_string()));
        assert_eq!(restored.group, Some("test-group".to_string()));
        assert_eq!(restored.mirror_urls.len(), 1);
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn test_recycle_multiple() {
        let mut mgr = RecycleBinManager::new();
        let t1 = make_test_task("t1", "File 1");
        let t2 = make_test_task("t2", "File 2");
        let t3 = make_test_task("t3", "File 3");

        mgr.recycle(&t1, None);
        mgr.recycle(&t2, None);
        mgr.recycle(&t3, None);
        assert_eq!(mgr.len(), 3);

        let list = mgr.list();
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_restore_nonexistent() {
        let mut mgr = RecycleBinManager::new();
        assert!(mgr.restore("nonexistent").is_none());
    }

    #[test]
    fn test_purge_one() {
        let mut mgr = RecycleBinManager::new();
        let t1 = make_test_task("t1", "File 1");
        let t2 = make_test_task("t2", "File 2");

        mgr.recycle(&t1, None);
        mgr.recycle(&t2, None);
        assert_eq!(mgr.len(), 2);

        assert!(mgr.purge_one("t1"));
        assert_eq!(mgr.len(), 1);
        assert!(mgr.get("t1").is_none());
        assert!(mgr.get("t2").is_some());
    }

    #[test]
    fn test_purge_nonexistent() {
        let mut mgr = RecycleBinManager::new();
        assert!(!mgr.purge_one("nonexistent"));
    }

    #[test]
    fn test_empty() {
        let mut mgr = RecycleBinManager::new();
        let t1 = make_test_task("t1", "File 1");
        let t2 = make_test_task("t2", "File 2");

        mgr.recycle(&t1, None);
        mgr.recycle(&t2, None);
        assert_eq!(mgr.len(), 2);

        let count = mgr.empty();
        assert_eq!(count, 2);
        assert!(mgr.is_empty());
    }

    #[test]
    fn test_max_entries_enforcement() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0,
            max_entries: 3,
        });

        for i in 0..5 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            mgr.recycle(&task, None);
        }

        // Should only keep 3 newest
        assert_eq!(mgr.len(), 3);
        // Oldest (t0, t1) should have been removed
        assert!(mgr.get("t0").is_none());
        assert!(mgr.get("t1").is_none());
        assert!(mgr.get("t2").is_some());
        assert!(mgr.get("t3").is_some());
        assert!(mgr.get("t4").is_some());
    }

    #[test]
    fn test_auto_purge() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 1, // 1 second
            max_entries: 0,
        });

        let task = make_test_task("t1", "File 1");
        let mut recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now() - chrono::Duration::seconds(10),
            reason: None,
        };
        mgr.entries.push(recycled);

        // Add a recent one too
        let task2 = make_test_task("t2", "File 2");
        mgr.recycle(&task2, None);

        assert_eq!(mgr.len(), 2);
        let purged = mgr.auto_purge();
        assert_eq!(purged, 1);
        assert_eq!(mgr.len(), 1);
        assert!(mgr.get("t1").is_none());
        assert!(mgr.get("t2").is_some());
    }

    #[test]
    fn test_auto_purge_disabled_when_zero() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0, // disabled
            max_entries: 0,
        });

        let task = make_test_task("t1", "File 1");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now() - chrono::Duration::days(365),
            reason: None,
        };
        mgr.entries.push(recycled);

        let purged = mgr.auto_purge();
        assert_eq!(purged, 0);
        assert_eq!(mgr.len(), 1);
    }

    #[test]
    fn test_config_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let state = RecycleBinState {
            config: RecycleBinConfig {
                enabled: true,
                auto_purge_after_secs: 3600,
                max_entries: 100,
            },
            entries: vec![],
        };

        save_recycle_bin(dir.path(), &state).unwrap();
        let loaded = load_recycle_bin(dir.path()).unwrap();
        assert_eq!(loaded.config.auto_purge_after_secs, 3600);
        assert_eq!(loaded.config.max_entries, 100);
    }

    #[test]
    fn test_entry_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: Some("test reason".to_string()),
        };

        let state = RecycleBinState {
            config: RecycleBinConfig::default(),
            entries: vec![recycled],
        };

        save_recycle_bin(dir.path(), &state).unwrap();
        let loaded = load_recycle_bin(dir.path()).unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].task.id, "t1");
        assert_eq!(loaded.entries[0].task.name, "Test File");
        assert_eq!(loaded.entries[0].reason, Some("test reason".to_string()));
    }

    #[test]
    fn test_load_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = load_recycle_bin(dir.path()).unwrap();
        assert!(loaded.entries.is_empty());
        assert_eq!(
            loaded.config.max_entries,
            RecycleBinConfig::default().max_entries
        );
    }

    #[test]
    fn test_summary() {
        let mut mgr = RecycleBinManager::new();
        let t1 = make_test_task("t1", "File 1");
        let t2 = make_test_task("t2", "File 2");
        mgr.recycle(&t1, None);
        mgr.recycle(&t2, None);

        let summary = mgr.summary();
        assert_eq!(summary.total_entries, 2);
        assert_eq!(summary.total_size, 2_000_000);
        assert_eq!(summary.total_downloaded, 1_000_000);
        assert!(summary.oldest_entry.is_some());
        assert!(summary.newest_entry.is_some());
        assert!(summary.by_protocol.contains_key("Xunlei"));
        assert_eq!(summary.by_protocol["Xunlei"], 2);
    }

    #[test]
    fn test_roundtrip_task_data() {
        let task = make_test_task("t1", "Test File");
        let data = RecycledTaskData::from_task(&task);
        let restored = data.to_task();

        assert_eq!(restored.id, task.id);
        assert_eq!(restored.name, task.name);
        assert_eq!(restored.size, task.size);
        assert_eq!(restored.downloaded, task.downloaded);
        assert_eq!(restored.tags, task.tags);
        assert_eq!(restored.notes, task.notes);
        assert_eq!(restored.group, task.group);
        assert_eq!(restored.source_url, task.source_url);
        assert_eq!(restored.mirror_urls, task.mirror_urls);
        assert_eq!(restored.active_time_seconds, task.active_time_seconds);
        assert_eq!(restored.bandwidth_weight, task.bandwidth_weight);
        // Restored tasks have reset fields
        assert_eq!(restored.speed_bps, 0.0);
        assert_eq!(restored.auto_retry_count, 0);
        assert!(restored.retry_after.is_none());
        assert!(restored.current_session_start.is_none());
        assert!(restored.cooldown.is_none());
    }

    #[test]
    fn test_set_config_enforces_limits() {
        let mut mgr = RecycleBinManager::new();
        // Add 5 entries
        for i in 0..5 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            mgr.recycle(&task, None);
        }
        assert_eq!(mgr.len(), 5);

        // Now set max to 3
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0,
            max_entries: 3,
        });

        assert_eq!(mgr.len(), 3);
    }

    #[test]
    fn test_unlimited_entries() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0,
            max_entries: 0, // unlimited
        });

        for i in 0..100 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            mgr.recycle(&task, None);
        }
        assert_eq!(mgr.len(), 100);
    }

    #[test]
    fn test_default_config() {
        let config = RecycleBinConfig::default();
        assert!(config.enabled);
        assert_eq!(config.auto_purge_after_secs, 7 * 24 * 3600);
        assert_eq!(config.max_entries, 500);
    }

    #[test]
    fn test_load_state() {
        let dir = tempfile::tempdir().unwrap();
        let state = RecycleBinState {
            config: RecycleBinConfig {
                enabled: false,
                auto_purge_after_secs: 100,
                max_entries: 10,
            },
            entries: vec![],
        };
        save_recycle_bin(dir.path(), &state).unwrap();

        let mut mgr = RecycleBinManager::new();
        mgr.load_state(dir.path());
        assert!(!mgr.config().enabled);
        assert_eq!(mgr.config().auto_purge_after_secs, 100);
        assert_eq!(mgr.config().max_entries, 10);
    }
}
