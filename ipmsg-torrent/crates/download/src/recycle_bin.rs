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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
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

    // ── Phase 241: Comprehensive test coverage ──

    // ── RecycledTask serde ──

    #[test]
    fn test_recycled_task_serde_roundtrip() {
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: Some("test reason".to_string()),
        };
        let json = serde_json::to_string(&recycled).unwrap();
        let deserialized: RecycledTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task.id, "t1");
        assert_eq!(deserialized.reason, Some("test reason".to_string()));
    }

    #[test]
    fn test_recycled_task_serde_null_reason() {
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: None,
        };
        let json = serde_json::to_string(&recycled).unwrap();
        assert!(json.contains("\"reason\":null"));
        let deserialized: RecycledTask = serde_json::from_str(&json).unwrap();
        assert!(deserialized.reason.is_none());
    }

    #[test]
    fn test_recycled_task_serde_extra_fields_ignored() {
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: None,
        };
        let mut json = serde_json::to_value(&recycled).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("extra_field".to_string(), serde_json::Value::Bool(true));
        let deserialized: RecycledTask = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.task.id, "t1");
    }

    #[test]
    fn test_recycled_task_pretty_serde() {
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: Some("pretty".to_string()),
        };
        let pretty = serde_json::to_string_pretty(&recycled).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: RecycledTask = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.task.id, "t1");
    }

    // ── RecycledTask traits ──

    #[test]
    fn test_recycled_task_clone() {
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: Some("clone me".to_string()),
        };
        let cloned = recycled.clone();
        assert_eq!(cloned.task.id, recycled.task.id);
        assert_eq!(cloned.reason, recycled.reason);
    }

    #[test]
    fn test_recycled_task_clone_independence() {
        let task = make_test_task("t1", "Test File");
        let mut recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: Some("original".to_string()),
        };
        let cloned = recycled.clone();
        recycled.reason = Some("modified".to_string());
        assert_eq!(cloned.reason, Some("original".to_string()));
    }

    #[test]
    fn test_recycled_task_debug() {
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: None,
        };
        let debug = format!("{:?}", recycled);
        assert!(debug.contains("RecycledTask"));
        assert!(debug.contains("t1"));
    }

    // ── RecycledTaskData serde ──

    #[test]
    fn test_recycled_task_data_serde_roundtrip() {
        let task = make_test_task("t1", "Test File");
        let data = RecycledTaskData::from_task(&task);
        let json = serde_json::to_string(&data).unwrap();
        let deserialized: RecycledTaskData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "t1");
        assert_eq!(deserialized.name, "Test File");
        assert_eq!(deserialized.size, 1_000_000);
        assert_eq!(deserialized.downloaded, 500_000);
        assert_eq!(deserialized.tags, vec!["test"]);
    }

    #[test]
    fn test_recycled_task_data_serde_extra_fields_ignored() {
        let task = make_test_task("t1", "Test File");
        let data = RecycledTaskData::from_task(&task);
        let mut json = serde_json::to_value(&data).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("future_field".to_string(), serde_json::json!("value"));
        let deserialized: RecycledTaskData = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.id, "t1");
    }

    // ── RecycledTaskData traits ──

    #[test]
    fn test_recycled_task_data_clone() {
        let task = make_test_task("t1", "Test File");
        let data = RecycledTaskData::from_task(&task);
        let cloned = data.clone();
        assert_eq!(cloned.id, data.id);
        assert_eq!(cloned.name, data.name);
        assert_eq!(cloned.size, data.size);
    }

    #[test]
    fn test_recycled_task_data_clone_independence() {
        let task = make_test_task("t1", "Test File");
        let mut data = RecycledTaskData::from_task(&task);
        let cloned = data.clone();
        data.name = "modified".to_string();
        assert_eq!(cloned.name, "Test File");
    }

    #[test]
    fn test_recycled_task_data_debug() {
        let task = make_test_task("t1", "Test File");
        let data = RecycledTaskData::from_task(&task);
        let debug = format!("{:?}", data);
        assert!(debug.contains("RecycledTaskData"));
        assert!(debug.contains("t1"));
    }

    // ── parse_protocol ──

    #[test]
    fn test_parse_protocol_http() {
        assert_eq!(parse_protocol("Http"), DownloadProtocol::Xunlei);
    }

    #[test]
    fn test_parse_protocol_xunlei() {
        assert_eq!(parse_protocol("Xunlei"), DownloadProtocol::Xunlei);
    }

    #[test]
    fn test_parse_protocol_torrent() {
        assert_eq!(parse_protocol("Torrent"), DownloadProtocol::Torrent);
    }

    #[test]
    fn test_parse_protocol_ed2k() {
        assert_eq!(parse_protocol("Ed2k"), DownloadProtocol::Ed2k);
    }

    #[test]
    fn test_parse_protocol_p2p() {
        assert_eq!(parse_protocol("P2P"), DownloadProtocol::P2P);
    }

    #[test]
    fn test_parse_protocol_unknown_defaults_to_xunlei() {
        assert_eq!(parse_protocol("Unknown"), DownloadProtocol::Xunlei);
        assert_eq!(parse_protocol(""), DownloadProtocol::Xunlei);
        assert_eq!(parse_protocol("ftp"), DownloadProtocol::Xunlei);
    }

    #[test]
    fn test_parse_protocol_case_sensitive() {
        // Only exact matches work
        assert_eq!(parse_protocol("http"), DownloadProtocol::Xunlei); // falls through to default
        assert_eq!(parse_protocol("TORRENT"), DownloadProtocol::Xunlei); // falls through
    }

    // ── parse_state ──

    #[test]
    fn test_parse_state_queued() {
        assert_eq!(parse_state("Queued"), DownloadState::Queued);
    }

    #[test]
    fn test_parse_state_downloading() {
        assert_eq!(parse_state("Downloading"), DownloadState::Downloading);
    }

    #[test]
    fn test_parse_state_paused() {
        assert_eq!(parse_state("Paused"), DownloadState::Paused);
    }

    #[test]
    fn test_parse_state_complete() {
        assert_eq!(parse_state("Complete"), DownloadState::Complete);
    }

    #[test]
    fn test_parse_state_error() {
        assert_eq!(parse_state("Error"), DownloadState::Error);
    }

    #[test]
    fn test_parse_state_unknown_defaults_to_paused() {
        assert_eq!(parse_state("Unknown"), DownloadState::Paused);
        assert_eq!(parse_state(""), DownloadState::Paused);
    }

    // ── parse_priority ──

    #[test]
    fn test_parse_priority_low() {
        assert_eq!(parse_priority("Low"), crate::DownloadPriority::Low);
    }

    #[test]
    fn test_parse_priority_high() {
        assert_eq!(parse_priority("High"), crate::DownloadPriority::High);
    }

    #[test]
    fn test_parse_priority_normal_default() {
        assert_eq!(parse_priority("Normal"), crate::DownloadPriority::Normal);
        assert_eq!(parse_priority("Unknown"), crate::DownloadPriority::Normal);
        assert_eq!(parse_priority(""), crate::DownloadPriority::Normal);
    }

    // ── RecycleBinConfig serde ──

    #[test]
    fn test_config_serde_roundtrip() {
        let config = RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 86400,
            max_entries: 100,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RecycleBinConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, true);
        assert_eq!(deserialized.auto_purge_after_secs, 86400);
        assert_eq!(deserialized.max_entries, 100);
    }

    #[test]
    fn test_config_serde_default_values() {
        let config = RecycleBinConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RecycleBinConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, RecycleBinConfig::default().enabled);
        assert_eq!(
            deserialized.auto_purge_after_secs,
            RecycleBinConfig::default().auto_purge_after_secs
        );
        assert_eq!(
            deserialized.max_entries,
            RecycleBinConfig::default().max_entries
        );
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let config = RecycleBinConfig::default();
        let mut json = serde_json::to_value(&config).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("extra".to_string(), serde_json::json!(42));
        let deserialized: RecycleBinConfig = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.enabled, RecycleBinConfig::default().enabled);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = RecycleBinConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: RecycleBinConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
    }

    // ── RecycleBinConfig traits ──

    #[test]
    fn test_config_clone() {
        let config = RecycleBinConfig {
            enabled: false,
            auto_purge_after_secs: 100,
            max_entries: 10,
        };
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.auto_purge_after_secs, config.auto_purge_after_secs);
        assert_eq!(cloned.max_entries, config.max_entries);
    }

    #[test]
    fn test_config_debug() {
        let config = RecycleBinConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("RecycleBinConfig"));
        assert!(debug.contains("enabled"));
    }

    #[test]
    fn test_config_default_values() {
        let config = RecycleBinConfig::default();
        assert!(config.enabled);
        assert_eq!(config.auto_purge_after_secs, 7 * 24 * 3600);
        assert_eq!(config.max_entries, 500);
    }

    // ── RecycleBinState serde ──

    #[test]
    fn test_state_serde_roundtrip() {
        let state = RecycleBinState {
            config: RecycleBinConfig {
                enabled: true,
                auto_purge_after_secs: 3600,
                max_entries: 50,
            },
            entries: vec![],
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: RecycleBinState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.config.enabled, true);
        assert_eq!(deserialized.config.auto_purge_after_secs, 3600);
        assert!(deserialized.entries.is_empty());
    }

    #[test]
    fn test_state_serde_with_entries() {
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: None,
        };
        let state = RecycleBinState {
            config: RecycleBinConfig::default(),
            entries: vec![recycled],
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: RecycleBinState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.entries.len(), 1);
        assert_eq!(deserialized.entries[0].task.id, "t1");
    }

    #[test]
    fn test_state_default() {
        let state = RecycleBinState::default();
        assert!(state.entries.is_empty());
        assert!(state.config.enabled); // default config
    }

    // ── RecycleBinManager basics ──

    #[test]
    fn test_manager_default() {
        let mgr = RecycleBinManager::default();
        assert!(mgr.is_empty());
        assert_eq!(mgr.len(), 0);
        assert!(mgr.config().enabled);
    }

    #[test]
    fn test_manager_new_equals_default() {
        let new = RecycleBinManager::new();
        let default = RecycleBinManager::default();
        assert_eq!(new.len(), default.len());
        assert_eq!(new.config().enabled, default.config().enabled);
        assert_eq!(
            new.config().auto_purge_after_secs,
            default.config().auto_purge_after_secs
        );
    }

    // ── recycle ──

    #[test]
    fn test_recycle_with_none_reason() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "Test File");
        let recycled = mgr.recycle(&task, None);
        assert!(recycled.reason.is_none());
        assert_eq!(recycled.task.id, "t1");
        assert_eq!(mgr.len(), 1);
    }

    #[test]
    fn test_recycle_with_empty_reason() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "Test File");
        let recycled = mgr.recycle(&task, Some("".to_string()));
        assert_eq!(recycled.reason, Some("".to_string()));
    }

    #[test]
    fn test_recycle_preserves_all_fields() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "Test File");
        let recycled = mgr.recycle(&task, None);
        assert_eq!(recycled.task.name, "Test File");
        assert_eq!(recycled.task.size, 1_000_000);
        assert_eq!(recycled.task.downloaded, 500_000);
        assert_eq!(recycled.task.protocol, "Xunlei");
        assert_eq!(recycled.task.state, "Paused");
        assert_eq!(recycled.task.tags, vec!["test"]);
        assert_eq!(recycled.task.notes, Some("test notes".to_string()));
        assert_eq!(recycled.task.group, Some("test-group".to_string()));
        assert_eq!(recycled.task.bandwidth_weight, 1);
        assert_eq!(recycled.task.active_time_seconds, 120.0);
        assert_eq!(recycled.task.mirror_urls.len(), 1);
        assert_eq!(recycled.task.sequential_mode, false);
    }

    #[test]
    fn test_recycle_deleted_at_is_recent() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "Test File");
        let before = Utc::now();
        let recycled = mgr.recycle(&task, None);
        let after = Utc::now();
        assert!(recycled.deleted_at >= before - chrono::Duration::seconds(1));
        assert!(recycled.deleted_at <= after + chrono::Duration::seconds(1));
    }

    // ── restore ──

    #[test]
    fn test_restore_removes_from_bin() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "Test File");
        mgr.recycle(&task, None);
        assert_eq!(mgr.len(), 1);

        let restored = mgr.restore("t1");
        assert!(restored.is_some());
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn test_restore_preserves_data() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "Test File");
        mgr.recycle(&task, Some("reason".to_string()));

        let restored = mgr.restore("t1").unwrap();
        assert_eq!(restored.id, "t1");
        assert_eq!(restored.name, "Test File");
        assert_eq!(restored.size, 1_000_000);
        assert_eq!(restored.downloaded, 500_000);
        assert_eq!(restored.tags, vec!["test"]);
    }

    #[test]
    fn test_restore_resets_runtime_fields() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "Test File");
        mgr.recycle(&task, None);

        let restored = mgr.restore("t1").unwrap();
        assert_eq!(restored.speed_bps, 0.0);
        assert_eq!(restored.auto_retry_count, 0);
        assert!(restored.retry_after.is_none());
        assert!(restored.current_session_start.is_none());
        assert!(restored.cooldown.is_none());
        assert!(restored.schedule.is_none());
        assert!(restored.retry_policy.is_none());
        assert!(restored.proxy_override.is_none());
        assert!(restored.deadline.is_none());
        assert_eq!(restored.staleness_promotion_count, 0);
    }

    // ── purge_one ──

    #[test]
    fn test_purge_one_removes_correct_entry() {
        let mut mgr = RecycleBinManager::new();
        let t1 = make_test_task("t1", "File 1");
        let t2 = make_test_task("t2", "File 2");
        let t3 = make_test_task("t3", "File 3");
        mgr.recycle(&t1, None);
        mgr.recycle(&t2, None);
        mgr.recycle(&t3, None);

        assert!(mgr.purge_one("t2"));
        assert_eq!(mgr.len(), 2);
        assert!(mgr.get("t2").is_none());
        assert!(mgr.get("t1").is_some());
        assert!(mgr.get("t3").is_some());
    }

    #[test]
    fn test_purge_one_idempotent() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "File 1");
        mgr.recycle(&task, None);

        assert!(mgr.purge_one("t1"));
        assert!(!mgr.purge_one("t1")); // second call returns false
        assert_eq!(mgr.len(), 0);
    }

    // ── list / get ──

    #[test]
    fn test_list_returns_slice() {
        let mut mgr = RecycleBinManager::new();
        let t1 = make_test_task("t1", "File 1");
        let t2 = make_test_task("t2", "File 2");
        mgr.recycle(&t1, None);
        mgr.recycle(&t2, None);

        let list = mgr.list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].task.id, "t1");
        assert_eq!(list[1].task.id, "t2");
    }

    #[test]
    fn test_list_empty() {
        let mgr = RecycleBinManager::new();
        assert!(mgr.list().is_empty());
    }

    #[test]
    fn test_get_returns_reference() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "File 1");
        mgr.recycle(&task, Some("reason".to_string()));

        let entry = mgr.get("t1").unwrap();
        assert_eq!(entry.task.id, "t1");
        assert_eq!(entry.reason, Some("reason".to_string()));
    }

    #[test]
    fn test_get_not_found() {
        let mgr = RecycleBinManager::new();
        assert!(mgr.get("nonexistent").is_none());
    }

    // ── len / is_empty ──

    #[test]
    fn test_len_after_operations() {
        let mut mgr = RecycleBinManager::new();
        assert_eq!(mgr.len(), 0);

        let task = make_test_task("t1", "File 1");
        mgr.recycle(&task, None);
        assert_eq!(mgr.len(), 1);

        mgr.restore("t1");
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn test_is_empty_boundaries() {
        let mut mgr = RecycleBinManager::new();
        assert!(mgr.is_empty());

        let task = make_test_task("t1", "File 1");
        mgr.recycle(&task, None);
        assert!(!mgr.is_empty());

        mgr.empty();
        assert!(mgr.is_empty());
    }

    // ── auto_purge ──

    #[test]
    fn test_auto_purge_removes_old_entries() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 60, // 60 seconds
            max_entries: 0,
        });

        let task = make_test_task("t1", "File 1");
        let old_recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now() - chrono::Duration::seconds(120),
            reason: None,
        };
        mgr.entries.push(old_recycled);

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
    fn test_auto_purge_all_old() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 1,
            max_entries: 0,
        });

        for i in 0..5 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            let recycled = RecycledTask {
                task: RecycledTaskData::from_task(&task),
                deleted_at: Utc::now() - chrono::Duration::seconds(10),
                reason: None,
            };
            mgr.entries.push(recycled);
        }

        let purged = mgr.auto_purge();
        assert_eq!(purged, 5);
        assert!(mgr.is_empty());
    }

    #[test]
    fn test_auto_purge_none_old() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 3600,
            max_entries: 0,
        });

        let task = make_test_task("t1", "File 1");
        mgr.recycle(&task, None);

        let purged = mgr.auto_purge();
        assert_eq!(purged, 0);
        assert_eq!(mgr.len(), 1);
    }

    // ── empty ──

    #[test]
    fn test_empty_already_empty() {
        let mut mgr = RecycleBinManager::new();
        let count = mgr.empty();
        assert_eq!(count, 0);
        assert!(mgr.is_empty());
    }

    #[test]
    fn test_empty_idempotent() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "File 1");
        mgr.recycle(&task, None);

        mgr.empty();
        mgr.empty();
        assert!(mgr.is_empty());
    }

    // ── set_config ──

    #[test]
    fn test_set_config_updates_config() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: false,
            auto_purge_after_secs: 100,
            max_entries: 10,
        });
        assert!(!mgr.config().enabled);
        assert_eq!(mgr.config().auto_purge_after_secs, 100);
        assert_eq!(mgr.config().max_entries, 10);
    }

    #[test]
    fn test_set_config_multiple_times() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: false,
            auto_purge_after_secs: 0,
            max_entries: 5,
        });
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 3600,
            max_entries: 100,
        });
        assert!(mgr.config().enabled);
        assert_eq!(mgr.config().auto_purge_after_secs, 3600);
        assert_eq!(mgr.config().max_entries, 100);
    }

    // ── enforce_limits ──

    #[test]
    fn test_enforce_limits_removes_oldest_first() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0,
            max_entries: 2,
        });

        for i in 0..5 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            mgr.recycle(&task, None);
        }

        assert_eq!(mgr.len(), 2);
        // Should keep t3 and t4 (newest)
        assert!(mgr.get("t3").is_some());
        assert!(mgr.get("t4").is_some());
    }

    #[test]
    fn test_enforce_limits_max_entries_zero_unlimited() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0,
            max_entries: 0,
        });

        for i in 0..50 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            mgr.recycle(&task, None);
        }
        assert_eq!(mgr.len(), 50);
    }

    #[test]
    fn test_enforce_limits_max_entries_one() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0,
            max_entries: 1,
        });

        for i in 0..5 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            mgr.recycle(&task, None);
        }
        assert_eq!(mgr.len(), 1);
        assert!(mgr.get("t4").is_some());
    }

    // ── summary ──

    #[test]
    fn test_summary_empty() {
        let mgr = RecycleBinManager::new();
        let summary = mgr.summary();
        assert_eq!(summary.total_entries, 0);
        assert_eq!(summary.total_size, 0);
        assert_eq!(summary.total_downloaded, 0);
        assert!(summary.oldest_entry.is_none());
        assert!(summary.newest_entry.is_none());
        assert!(summary.by_protocol.is_empty());
        assert!(summary.config_enabled);
    }

    #[test]
    fn test_summary_with_entries() {
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
        assert!(summary.oldest_entry.unwrap() <= summary.newest_entry.unwrap());
    }

    #[test]
    fn test_summary_by_protocol_counts() {
        let mut mgr = RecycleBinManager::new();
        let t1 = make_test_task("t1", "File 1"); // Xunlei
        let mut t2 = make_test_task("t2", "File 2");
        t2.protocol = DownloadProtocol::Torrent;
        let mut t3 = make_test_task("t3", "File 3");
        t3.protocol = DownloadProtocol::Torrent;

        mgr.recycle(&t1, None);
        mgr.recycle(&t2, None);
        mgr.recycle(&t3, None);

        let summary = mgr.summary();
        assert_eq!(summary.by_protocol.get("Xunlei"), Some(&1));
        assert_eq!(summary.by_protocol.get("Torrent"), Some(&2));
    }

    #[test]
    fn test_summary_config_fields() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: false,
            auto_purge_after_secs: 7200,
            max_entries: 200,
        });
        let summary = mgr.summary();
        assert!(!summary.config_enabled);
        assert_eq!(summary.auto_purge_after_secs, 7200);
        assert_eq!(summary.max_entries, 200);
    }

    // ── RecycleBinSummary traits ──

    #[test]
    fn test_summary_clone() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "File 1");
        mgr.recycle(&task, None);
        let summary = mgr.summary();
        let cloned = summary.clone();
        assert_eq!(cloned.total_entries, summary.total_entries);
        assert_eq!(cloned.total_size, summary.total_size);
    }

    #[test]
    fn test_summary_debug() {
        let mgr = RecycleBinManager::new();
        let summary = mgr.summary();
        let debug = format!("{:?}", summary);
        assert!(debug.contains("RecycleBinSummary"));
        assert!(debug.contains("total_entries"));
    }

    // ── RecycleBinError ──

    #[test]
    fn test_error_io_display() {
        let err = RecycleBinError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        let display = format!("{}", err);
        assert!(display.contains("IO error"));
        assert!(display.contains("file not found"));
    }

    #[test]
    fn test_error_json_display() {
        let json_err = serde_json::from_str::<RecycleBinConfig>("invalid json").unwrap_err();
        let err = RecycleBinError::Json(json_err);
        let display = format!("{}", err);
        assert!(display.contains("JSON error"));
    }

    #[test]
    fn test_error_io_debug() {
        let err = RecycleBinError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        let debug = format!("{:?}", err);
        assert!(debug.contains("Io"));
    }

    #[test]
    fn test_error_json_debug() {
        let json_err = serde_json::from_str::<RecycleBinConfig>("invalid").unwrap_err();
        let err = RecycleBinError::Json(json_err);
        let debug = format!("{:?}", err);
        assert!(debug.contains("Json"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "test");
        let err: RecycleBinError = io_err.into();
        match err {
            RecycleBinError::Io(_) => {}
            _ => panic!("expected Io variant"),
        }
    }

    #[test]
    fn test_error_from_json() {
        let json_err = serde_json::from_str::<RecycleBinConfig>("bad").unwrap_err();
        let err: RecycleBinError = json_err.into();
        match err {
            RecycleBinError::Json(_) => {}
            _ => panic!("expected Json variant"),
        }
    }

    // ── Persistence ──

    #[test]
    fn test_save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let state = RecycleBinState::default();
        save_recycle_bin(dir.path(), &state).unwrap();
        assert!(dir.path().join("recycle_bin.json").exists());
    }

    #[test]
    fn test_save_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let state1 = RecycleBinState {
            config: RecycleBinConfig {
                enabled: true,
                auto_purge_after_secs: 100,
                max_entries: 10,
            },
            entries: vec![],
        };
        save_recycle_bin(dir.path(), &state1).unwrap();

        let state2 = RecycleBinState {
            config: RecycleBinConfig {
                enabled: false,
                auto_purge_after_secs: 200,
                max_entries: 20,
            },
            entries: vec![],
        };
        save_recycle_bin(dir.path(), &state2).unwrap();

        let loaded = load_recycle_bin(dir.path()).unwrap();
        assert!(!loaded.config.enabled);
        assert_eq!(loaded.config.auto_purge_after_secs, 200);
        assert_eq!(loaded.config.max_entries, 20);
    }

    #[test]
    fn test_save_no_tmp_leftover() {
        let dir = tempfile::tempdir().unwrap();
        let state = RecycleBinState::default();
        save_recycle_bin(dir.path(), &state).unwrap();
        let tmp_path = dir.path().join("recycle_bin.json.tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_load_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recycle_bin.json");
        std::fs::write(&path, "not valid json {{{").unwrap();
        let result = load_recycle_bin(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recycle_bin.json");
        std::fs::write(&path, "").unwrap();
        let result = load_recycle_bin(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_persistence_pretty_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let task = make_test_task("t1", "Test File");
        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: Some("pretty test".to_string()),
        };
        let state = RecycleBinState {
            config: RecycleBinConfig {
                enabled: true,
                auto_purge_after_secs: 600,
                max_entries: 50,
            },
            entries: vec![recycled],
        };
        save_recycle_bin(dir.path(), &state).unwrap();

        // Verify file is pretty-printed
        let content = std::fs::read_to_string(dir.path().join("recycle_bin.json")).unwrap();
        assert!(content.contains('\n'));

        let loaded = load_recycle_bin(dir.path()).unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].task.id, "t1");
        assert_eq!(loaded.config.auto_purge_after_secs, 600);
    }

    #[test]
    fn test_save_state_async() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let mut mgr = RecycleBinManager::new();
            let task = make_test_task("t1", "Test File");
            mgr.recycle(&task, Some("async save".to_string()));

            mgr.save_state(dir.path()).await.unwrap();

            let loaded = load_recycle_bin(dir.path()).unwrap();
            assert_eq!(loaded.entries.len(), 1);
            assert_eq!(loaded.entries[0].task.id, "t1");
        });
    }

    #[test]
    fn test_load_state_missing_dir() {
        let mut mgr = RecycleBinManager::new();
        let dir = tempfile::tempdir().unwrap();
        let nonexistent = dir.path().join("nonexistent_subdir");
        mgr.load_state(&nonexistent);
        // Should use defaults, not panic
        assert!(mgr.is_empty());
    }

    // ── Unicode ──

    #[test]
    fn test_unicode_task_name() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "中文文件名");
        mgr.recycle(&task, None);

        let entry = mgr.get("t1").unwrap();
        assert_eq!(entry.task.name, "中文文件名");
    }

    #[test]
    fn test_unicode_reason() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "File");
        mgr.recycle(&task, Some("不再需要了".to_string()));

        let entry = mgr.get("t1").unwrap();
        assert_eq!(entry.reason, Some("不再需要了".to_string()));
    }

    #[test]
    fn test_unicode_tags() {
        let mut mgr = RecycleBinManager::new();
        let mut task = make_test_task("t1", "File");
        task.tags = vec!["标签一".to_string(), "标签二".to_string()];
        mgr.recycle(&task, None);

        let restored = mgr.restore("t1").unwrap();
        assert_eq!(restored.tags, vec!["标签一", "标签二"]);
    }

    #[test]
    fn test_emoji_task_name() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "🎉🎊party");
        mgr.recycle(&task, None);

        let entry = mgr.get("t1").unwrap();
        assert_eq!(entry.task.name, "🎉🎊party");
    }

    #[test]
    fn test_unicode_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut task = make_test_task("t1", "日本語ファイル");
        task.tags = vec!["中文".to_string()];
        task.notes = Some("emoji 🚀".to_string());

        let recycled = RecycledTask {
            task: RecycledTaskData::from_task(&task),
            deleted_at: Utc::now(),
            reason: Some("理由".to_string()),
        };
        let state = RecycleBinState {
            config: RecycleBinConfig::default(),
            entries: vec![recycled],
        };
        save_recycle_bin(dir.path(), &state).unwrap();
        let loaded = load_recycle_bin(dir.path()).unwrap();
        assert_eq!(loaded.entries[0].task.name, "日本語ファイル");
        assert_eq!(loaded.entries[0].task.tags, vec!["中文"]);
        assert_eq!(loaded.entries[0].reason, Some("理由".to_string()));
    }

    // ── from_task / to_task roundtrip ──

    #[test]
    fn test_from_task_captures_all_fields() {
        let task = make_test_task("t1", "Test File");
        let data = RecycledTaskData::from_task(&task);
        assert_eq!(data.id, "t1");
        assert_eq!(data.name, "Test File");
        assert_eq!(data.protocol, "Xunlei");
        assert_eq!(data.size, 1_000_000);
        assert_eq!(data.downloaded, 500_000);
        assert_eq!(data.state, "Paused");
        assert_eq!(data.priority, "Normal");
        assert_eq!(data.bandwidth_weight, 1);
        assert_eq!(data.active_time_seconds, 120.0);
        assert_eq!(data.sequential_mode, false);
    }

    #[test]
    fn test_to_task_restores_protocol() {
        let mut task = make_test_task("t1", "Test");
        task.protocol = DownloadProtocol::Torrent;
        let data = RecycledTaskData::from_task(&task);
        let restored = data.to_task();
        assert!(matches!(restored.protocol, DownloadProtocol::Torrent));
    }

    #[test]
    fn test_to_task_restores_state() {
        let mut task = make_test_task("t1", "Test");
        task.state = DownloadState::Complete;
        let data = RecycledTaskData::from_task(&task);
        let restored = data.to_task();
        assert!(matches!(restored.state, DownloadState::Complete));
    }

    #[test]
    fn test_to_task_restores_priority() {
        let mut task = make_test_task("t1", "Test");
        task.priority = DownloadPriority::High;
        let data = RecycledTaskData::from_task(&task);
        let restored = data.to_task();
        assert!(matches!(restored.priority, crate::DownloadPriority::High));
    }

    #[test]
    fn test_to_task_with_checksum() {
        let mut task = make_test_task("t1", "Test");
        task.expected_checksum = Some("abc123".to_string());
        task.checksum_algorithm = Some(crate::checksum::ChecksumAlgorithm::Sha256);
        let data = RecycledTaskData::from_task(&task);
        let restored = data.to_task();
        assert_eq!(restored.expected_checksum, Some("abc123".to_string()));
        assert!(restored.checksum_algorithm.is_some());
    }

    #[test]
    fn test_to_task_with_unknown_checksum_algorithm() {
        let mut task = make_test_task("t1", "Test");
        task.checksum_algorithm = Some(crate::checksum::ChecksumAlgorithm::Md5);
        let data = RecycledTaskData::from_task(&task);
        let mut data_mut = data;
        data_mut.checksum_algorithm = Some("UnknownAlgo".to_string());
        let restored = data_mut.to_task();
        // Unknown algorithm defaults to Sha256
        assert!(matches!(
            restored.checksum_algorithm,
            Some(crate::checksum::ChecksumAlgorithm::Sha256)
        ));
    }

    // ── Complex workflows ──

    #[test]
    fn test_full_lifecycle() {
        let mut mgr = RecycleBinManager::new();
        let t1 = make_test_task("t1", "File 1");
        let t2 = make_test_task("t2", "File 2");
        let t3 = make_test_task("t3", "File 3");

        // Recycle all three
        mgr.recycle(&t1, Some("no longer needed".to_string()));
        mgr.recycle(&t2, None);
        mgr.recycle(&t3, Some("duplicate".to_string()));
        assert_eq!(mgr.len(), 3);

        // Check summary
        let summary = mgr.summary();
        assert_eq!(summary.total_entries, 3);
        assert_eq!(summary.total_size, 3_000_000);

        // Restore one
        let restored = mgr.restore("t2").unwrap();
        assert_eq!(restored.name, "File 2");
        assert_eq!(mgr.len(), 2);

        // Purge one
        assert!(mgr.purge_one("t1"));
        assert_eq!(mgr.len(), 1);
        assert!(mgr.get("t3").is_some());

        // Save and reload
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            mgr.save_state(dir.path()).await.unwrap();
        });

        let mut mgr2 = RecycleBinManager::new();
        mgr2.load_state(dir.path());
        assert_eq!(mgr2.len(), 1);
        assert!(mgr2.get("t3").is_some());

        // Empty
        let count = mgr2.empty();
        assert_eq!(count, 1);
        assert!(mgr2.is_empty());
    }

    #[test]
    fn test_recycle_restore_recycle_again() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "File 1");

        // First recycle
        mgr.recycle(&task, Some("first".to_string()));
        assert_eq!(mgr.len(), 1);

        // Restore
        let restored = mgr.restore("t1").unwrap();
        assert_eq!(mgr.len(), 0);

        // Recycle again
        mgr.recycle(&restored, Some("second".to_string()));
        assert_eq!(mgr.len(), 1);
        let entry = mgr.get("t1").unwrap();
        assert_eq!(entry.reason, Some("second".to_string()));
    }

    #[test]
    fn test_multiple_managers_independent() {
        let mut mgr1 = RecycleBinManager::new();
        let mut mgr2 = RecycleBinManager::new();

        let task = make_test_task("t1", "File 1");
        mgr1.recycle(&task, None);
        assert_eq!(mgr1.len(), 1);
        assert_eq!(mgr2.len(), 0);
    }

    #[test]
    fn test_config_change_affects_subsequent_operations() {
        let mut mgr = RecycleBinManager::new();

        // Add entries with no limit
        for i in 0..10 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            mgr.recycle(&task, None);
        }
        assert_eq!(mgr.len(), 10);

        // Now set limit
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0,
            max_entries: 5,
        });
        assert_eq!(mgr.len(), 5);

        // Adding more should enforce limit
        let task = make_test_task("new1", "New File");
        mgr.recycle(&task, None);
        assert_eq!(mgr.len(), 5); // still 5, oldest removed
    }

    // ── Boundary conditions ──

    #[test]
    fn test_zero_size_task() {
        let mut mgr = RecycleBinManager::new();
        let mut task = make_test_task("t1", "Empty File");
        task.size = 0;
        task.downloaded = 0;
        mgr.recycle(&task, None);

        let summary = mgr.summary();
        assert_eq!(summary.total_size, 0);
        assert_eq!(summary.total_downloaded, 0);
    }

    #[test]
    fn test_large_task_values() {
        let mut mgr = RecycleBinManager::new();
        let mut task = make_test_task("t1", "Huge File");
        task.size = u64::MAX;
        task.downloaded = u64::MAX;
        mgr.recycle(&task, None);

        let summary = mgr.summary();
        assert_eq!(summary.total_size, u64::MAX);
        assert_eq!(summary.total_downloaded, u64::MAX);
    }

    #[test]
    fn test_empty_task_id() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("", "No ID File");
        mgr.recycle(&task, None);
        assert_eq!(mgr.len(), 1);
        assert!(mgr.get("").is_some());
    }

    #[test]
    fn test_special_characters_in_task_name() {
        let mut mgr = RecycleBinManager::new();
        let task = make_test_task("t1", "file/with\\special\nchars\t\"quotes\"");
        mgr.recycle(&task, None);
        let entry = mgr.get("t1").unwrap();
        assert_eq!(entry.task.name, "file/with\\special\nchars\t\"quotes\"");
    }

    #[test]
    fn test_many_entries() {
        let mut mgr = RecycleBinManager::new();
        mgr.set_config(RecycleBinConfig {
            enabled: true,
            auto_purge_after_secs: 0,
            max_entries: 0,
        });

        for i in 0..500 {
            let task = make_test_task(&format!("t{}", i), &format!("File {}", i));
            mgr.recycle(&task, None);
        }
        assert_eq!(mgr.len(), 500);

        let summary = mgr.summary();
        assert_eq!(summary.total_entries, 500);
    }
}
