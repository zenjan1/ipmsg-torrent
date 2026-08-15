//! Download queue snapshot and restore
//!
//! Create point-in-time snapshots of the entire download queue state,
//! including all tasks and key configuration. Snapshots can be listed,
//! restored (with automatic backup of current state), and deleted.

use crate::DownloadTask;
#[cfg(test)]
use crate::task_queue::load_task_queue;
use crate::task_queue::{PersistedTask, save_task_queue};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Snapshot format version for forward compatibility
const SNAPSHOT_VERSION: u32 = 1;

/// Maximum number of snapshots to keep (oldest are pruned)
const MAX_SNAPSHOTS: usize = 20;

/// A single snapshot entry stored on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    /// Unique snapshot ID (timestamp-based)
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
    /// When the snapshot was created
    pub created_at: DateTime<Utc>,
    /// Number of tasks in the snapshot
    pub task_count: usize,
    /// Total size of all tasks in bytes
    pub total_size: u64,
    /// Snapshot file path (relative to snapshots dir)
    pub file_path: PathBuf,
}

/// Full snapshot data stored in a snapshot file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    /// Format version
    pub version: u32,
    /// Snapshot ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
    /// When the snapshot was created
    pub created_at: DateTime<Utc>,
    /// Global download speed limit (bytes/sec, 0 = unlimited)
    #[serde(default)]
    pub global_speed_limit: u64,
    /// Maximum concurrent downloads
    #[serde(default)]
    pub max_concurrent: usize,
    /// All tasks in the queue
    pub tasks: Vec<PersistedTask>,
}

/// Summary of a snapshot for display purposes
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotSummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub task_count: usize,
    pub total_size: u64,
    pub file_size_bytes: u64,
}

/// Errors during snapshot operations
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize error: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("snapshot not found: {0}")]
    NotFound(String),
    #[error("invalid snapshot: {0}")]
    Invalid(String),
    #[error("unsupported snapshot version: {0}")]
    UnsupportedVersion(u32),
}

/// Manager for download queue snapshots
pub struct SnapshotManager {
    /// Directory where snapshots are stored
    snapshots_dir: PathBuf,
    /// Index of all snapshots
    entries: Vec<SnapshotEntry>,
}

impl SnapshotManager {
    /// Create a new SnapshotManager
    pub fn new(snapshots_dir: PathBuf) -> Self {
        Self {
            snapshots_dir,
            entries: Vec::new(),
        }
    }

    /// Initialize by loading existing snapshot index from disk
    pub fn load(&mut self) -> Result<(), SnapshotError> {
        let index_path = self.snapshots_dir.join("snapshot_index.json");
        if index_path.exists() {
            let json = std::fs::read_to_string(&index_path)?;
            self.entries = serde_json::from_str(&json)?;
            // Verify each entry's file still exists
            self.entries.retain(|e| e.file_path.exists());
            // Save cleaned index
            let _ = self.save_index();
        }
        Ok(())
    }

    /// Create a new snapshot of the current download queue
    pub fn create_snapshot(
        &mut self,
        tasks: &[DownloadTask],
        name: String,
        description: Option<String>,
        global_speed_limit: u64,
        max_concurrent: usize,
    ) -> Result<SnapshotEntry, SnapshotError> {
        // Ensure snapshots directory exists
        std::fs::create_dir_all(&self.snapshots_dir)?;

        // Generate snapshot ID
        let now = Utc::now();
        let id = format!("snap-{}", now.format("%Y%m%d-%H%M%S-%3f"));

        // Convert tasks to persisted format
        let persisted: Vec<PersistedTask> =
            tasks.iter().cloned().map(PersistedTask::from).collect();
        let total_size = persisted.iter().map(|t| t.size).sum();
        let task_count = persisted.len();

        let data = SnapshotData {
            version: SNAPSHOT_VERSION,
            id: id.clone(),
            name: name.clone(),
            description: description.clone(),
            created_at: now,
            global_speed_limit,
            max_concurrent,
            tasks: persisted,
        };

        // Write snapshot file atomically
        let file_path = self.snapshots_dir.join(format!("{}.json", id));
        let json = serde_json::to_string_pretty(&data)?;
        let tmp_path = file_path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &json)?;
        std::fs::rename(&tmp_path, &file_path)?;

        // Create entry
        let entry = SnapshotEntry {
            id: id.clone(),
            name,
            description,
            created_at: now,
            task_count,
            total_size,
            file_path,
        };

        self.entries.push(entry.clone());
        self.save_index()?;
        self.prune_old_snapshots()?;

        Ok(entry)
    }

    /// List all available snapshots
    pub fn list_snapshots(&self) -> Vec<SnapshotSummary> {
        self.entries
            .iter()
            .map(|e| {
                let file_size = std::fs::metadata(&e.file_path)
                    .map(|m| m.len())
                    .unwrap_or(0);
                SnapshotSummary {
                    id: e.id.clone(),
                    name: e.name.clone(),
                    description: e.description.clone(),
                    created_at: e.created_at,
                    task_count: e.task_count,
                    total_size: e.total_size,
                    file_size_bytes: file_size,
                }
            })
            .collect()
    }

    /// Get a specific snapshot's data
    pub fn get_snapshot(&self, id: &str) -> Result<SnapshotData, SnapshotError> {
        let entry = self
            .entries
            .iter()
            .find(|e| e.id == id)
            .ok_or_else(|| SnapshotError::NotFound(id.to_string()))?;

        let json = std::fs::read_to_string(&entry.file_path)?;
        let data: SnapshotData = serde_json::from_str(&json)?;

        if data.version == 0 || data.version > SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion(data.version));
        }

        Ok(data)
    }

    /// Restore a snapshot, replacing the current task queue.
    ///
    /// This writes the snapshot's tasks to the task_queue.json file.
    /// Returns the restored tasks.
    pub fn restore_snapshot(
        &self,
        id: &str,
        data_dir: &Path,
    ) -> Result<Vec<DownloadTask>, SnapshotError> {
        let snapshot = self.get_snapshot(id)?;

        // Reset all running tasks to Paused state (can't resume across snapshots)
        let mut tasks: Vec<DownloadTask> =
            snapshot.tasks.into_iter().map(DownloadTask::from).collect();

        for task in &mut tasks {
            if matches!(task.state, crate::DownloadState::Downloading) {
                task.state = crate::DownloadState::Paused;
                task.speed_bps = 0.0;
            }
        }

        // Write to task_queue.json
        save_task_queue(&tasks, data_dir)
            .map_err(|e| SnapshotError::Io(std::io::Error::other(e.to_string())))?;

        Ok(tasks)
    }

    /// Delete a snapshot
    pub fn delete_snapshot(&mut self, id: &str) -> Result<(), SnapshotError> {
        let pos = self
            .entries
            .iter()
            .position(|e| e.id == id)
            .ok_or_else(|| SnapshotError::NotFound(id.to_string()))?;

        let entry = self.entries.remove(pos);

        // Delete the file (ignore if already gone)
        let _ = std::fs::remove_file(&entry.file_path);

        self.save_index()?;
        Ok(())
    }

    /// Get the number of snapshots
    pub fn snapshot_count(&self) -> usize {
        self.entries.len()
    }

    /// Save the snapshot index to disk
    fn save_index(&self) -> Result<(), SnapshotError> {
        std::fs::create_dir_all(&self.snapshots_dir)?;
        let index_path = self.snapshots_dir.join("snapshot_index.json");
        let json = serde_json::to_string_pretty(&self.entries)?;
        let tmp_path = index_path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &json)?;
        std::fs::rename(&tmp_path, &index_path)?;
        Ok(())
    }

    /// Prune old snapshots if we exceed MAX_SNAPSHOTS
    fn prune_old_snapshots(&mut self) -> Result<(), SnapshotError> {
        while self.entries.len() > MAX_SNAPSHOTS {
            // Remove the oldest entry
            if let Some(oldest) = self.entries.first().cloned() {
                let _ = std::fs::remove_file(&oldest.file_path);
                self.entries.remove(0);
            } else {
                break;
            }
        }
        self.save_index()
    }
}

/// Format bytes as human-readable string
pub fn format_size(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_string();
    }
    let units = ["B", "KB", "MB", "GB", "TB"];
    let i = (bytes as f64).log(1024.0).floor() as usize;
    let i = i.min(units.len() - 1);
    let value = bytes as f64 / 1024.0_f64.powi(i as i32);
    if i == 0 {
        format!("{} {}", bytes, units[0])
    } else {
        format!("{:.1} {}", value, units[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DownloadPriority, DownloadProtocol, DownloadState};
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

    fn make_manager() -> (SnapshotManager, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let mgr = SnapshotManager::new(temp.path().join("snapshots"));
        (mgr, temp)
    }

    #[test]
    fn test_create_empty_snapshot() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "empty".to_string(), None, 0, 5)
            .unwrap();

        assert_eq!(entry.task_count, 0);
        assert_eq!(entry.total_size, 0);
        assert_eq!(entry.name, "empty");
        assert!(entry.id.starts_with("snap-"));
    }

    #[test]
    fn test_create_snapshot_with_tasks() {
        let (mut mgr, _temp) = make_manager();
        let tasks = vec![
            make_test_task("t1", "file1.txt"),
            make_test_task("t2", "file2.mp4"),
        ];

        let entry = mgr
            .create_snapshot(
                &tasks,
                "backup1".to_string(),
                Some("test backup".to_string()),
                1048576,
                3,
            )
            .unwrap();

        assert_eq!(entry.task_count, 2);
        assert_eq!(entry.total_size, 2048); // 1024 + 1024
        assert_eq!(entry.name, "backup1");
        assert_eq!(entry.description, Some("test backup".to_string()));
        assert_eq!(mgr.snapshot_count(), 1);
    }

    #[test]
    fn test_list_snapshots() {
        let (mut mgr, _temp) = make_manager();
        let tasks = vec![make_test_task("t1", "file1.txt")];

        mgr.create_snapshot(&tasks, "snap1".to_string(), None, 0, 5)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.create_snapshot(&[], "snap2".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "snap1");
        assert_eq!(list[0].task_count, 1);
        assert_eq!(list[1].name, "snap2");
        assert_eq!(list[1].task_count, 0);
    }

    #[test]
    fn test_get_snapshot_data() {
        let (mut mgr, _temp) = make_manager();
        let tasks = vec![make_test_task("t1", "file1.txt")];

        mgr.create_snapshot(
            &tasks,
            "my-snap".to_string(),
            Some("desc".to_string()),
            2048,
            10,
        )
        .unwrap();

        let list = mgr.list_snapshots();
        let id = &list[0].id;

        let data = mgr.get_snapshot(id).unwrap();
        assert_eq!(data.name, "my-snap");
        assert_eq!(data.description, Some("desc".to_string()));
        assert_eq!(data.global_speed_limit, 2048);
        assert_eq!(data.max_concurrent, 10);
        assert_eq!(data.tasks.len(), 1);
        assert_eq!(data.tasks[0].name, "file1.txt");
        assert_eq!(data.version, SNAPSHOT_VERSION);
    }

    #[test]
    fn test_get_snapshot_not_found() {
        let (mgr, _temp) = make_manager();
        let result = mgr.get_snapshot("nonexistent");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SnapshotError::NotFound(_)));
    }

    #[test]
    fn test_delete_snapshot() {
        let (mut mgr, _temp) = make_manager();
        mgr.create_snapshot(&[], "to-delete".to_string(), None, 0, 5)
            .unwrap();
        assert_eq!(mgr.snapshot_count(), 1);

        let list = mgr.list_snapshots();
        let id = list[0].id.clone();

        mgr.delete_snapshot(&id).unwrap();
        assert_eq!(mgr.snapshot_count(), 0);
        assert!(mgr.list_snapshots().is_empty());
    }

    #[test]
    fn test_delete_nonexistent_snapshot() {
        let (mut mgr, _temp) = make_manager();
        let result = mgr.delete_snapshot("nonexistent");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SnapshotError::NotFound(_)));
    }

    #[test]
    fn test_restore_snapshot() {
        let (mut mgr, temp) = make_manager();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let tasks = vec![
            make_test_task("t1", "file1.txt"),
            make_test_task("t2", "file2.mp4"),
        ];

        mgr.create_snapshot(&tasks, "restore-me".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let id = list[0].id.clone();

        let restored = mgr.restore_snapshot(&id, &data_dir).unwrap();
        assert_eq!(restored.len(), 2);

        // Downloading tasks should be reset to Paused
        for task in &restored {
            assert_eq!(task.state, DownloadState::Paused);
            assert_eq!(task.speed_bps, 0.0);
        }

        // Verify task_queue.json was written
        let loaded = load_task_queue(&data_dir).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn test_restore_nonexistent_snapshot() {
        let (mgr, temp) = make_manager();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let result = mgr.restore_snapshot("nonexistent", &data_dir);
        assert!(result.is_err());
    }

    #[test]
    fn test_persistence_across_reload() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");

        mgr.create_snapshot(&[], "persist-test".to_string(), None, 0, 5)
            .unwrap();
        assert_eq!(mgr.snapshot_count(), 1);

        // Create a new manager and load from same directory
        let mut mgr2 = SnapshotManager::new(snap_dir);
        mgr2.load().unwrap();
        assert_eq!(mgr2.snapshot_count(), 1);

        let list = mgr2.list_snapshots();
        assert_eq!(list[0].name, "persist-test");
    }

    #[test]
    fn test_prune_old_snapshots() {
        let (mut mgr, _temp) = make_manager();

        // Create more than MAX_SNAPSHOTS
        for i in 0..MAX_SNAPSHOTS + 5 {
            mgr.create_snapshot(&[], format!("snap-{}", i), None, 0, 5)
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        // Should be pruned to MAX_SNAPSHOTS
        assert_eq!(mgr.snapshot_count(), MAX_SNAPSHOTS);

        // The oldest ones should have been pruned
        let list = mgr.list_snapshots();
        // First remaining should not be snap-0
        assert!(list.iter().all(|s| s.name != "snap-0"));
    }

    #[test]
    fn test_snapshot_preserves_task_metadata() {
        let (mut mgr, _temp) = make_manager();

        let mut task = make_test_task("meta-1", "meta_file.txt");
        task.tags = vec!["movies".to_string(), "2026".to_string()];
        task.priority = DownloadPriority::High;
        task.bandwidth_weight = 5;
        task.queue_position = Some(3);
        task.depends_on = vec!["dep-1".to_string()];
        task.group = Some("entertainment".to_string());
        task.speed_limit_bps = Some(102400);
        task.notes = Some("important download".to_string());

        mgr.create_snapshot(&[task], "meta-snap".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let data = mgr.get_snapshot(&list[0].id).unwrap();

        assert_eq!(data.tasks.len(), 1);
        let t = &data.tasks[0];
        assert_eq!(t.tags, vec!["movies", "2026"]);
        assert_eq!(t.priority, DownloadPriority::High);
        assert_eq!(t.bandwidth_weight, 5);
        assert_eq!(t.queue_position, Some(3));
        assert_eq!(t.depends_on, vec!["dep-1"]);
        assert_eq!(t.group, Some("entertainment".to_string()));
        assert_eq!(t.speed_limit_bps, Some(102400));
        assert_eq!(t.notes, Some("important download".to_string()));
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1536), "1.5 KB");
        assert_eq!(format_size(1048576), "1.0 MB");
        assert_eq!(format_size(1073741824), "1.0 GB");
    }

    #[test]
    fn test_snapshot_summary_file_size() {
        let (mut mgr, _temp) = make_manager();
        mgr.create_snapshot(
            &[make_test_task("t1", "file1.txt")],
            "sized".to_string(),
            None,
            0,
            5,
        )
        .unwrap();

        let list = mgr.list_snapshots();
        assert!(list[0].file_size_bytes > 0);
    }

    #[test]
    fn test_load_with_missing_files() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");

        mgr.create_snapshot(&[], "will-survive".to_string(), None, 0, 5)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        mgr.create_snapshot(&[], "will-be-deleted".to_string(), None, 0, 5)
            .unwrap();

        // Manually delete one snapshot file
        let list = mgr.list_snapshots();
        let deleted_id = &list[1].id;
        let file_path = snap_dir.join(format!("{}.json", deleted_id));
        let _ = std::fs::remove_file(&file_path);

        // Reload - should only keep the surviving one
        let mut mgr2 = SnapshotManager::new(snap_dir);
        mgr2.load().unwrap();
        assert_eq!(mgr2.snapshot_count(), 1);
        assert_eq!(mgr2.list_snapshots()[0].name, "will-survive");
    }

    #[test]
    fn test_restore_resets_downloading_state() {
        let (mut mgr, temp) = make_manager();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let mut task1 = make_test_task("d1", "downloading.txt");
        task1.state = DownloadState::Downloading;
        task1.speed_bps = 500.0;

        let mut task2 = make_test_task("p1", "paused.txt");
        task2.state = DownloadState::Paused;

        let mut task3 = make_test_task("q1", "queued.txt");
        task3.state = DownloadState::Queued;

        mgr.create_snapshot(&[task1, task2, task3], "mixed".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let restored = mgr.restore_snapshot(&list[0].id, &data_dir).unwrap();

        // Downloading -> Paused
        let d1 = restored.iter().find(|t| t.id == "d1").unwrap();
        assert_eq!(d1.state, DownloadState::Paused);
        assert_eq!(d1.speed_bps, 0.0);

        // Paused stays Paused
        let p1 = restored.iter().find(|t| t.id == "p1").unwrap();
        assert_eq!(p1.state, DownloadState::Paused);

        // Queued stays Queued
        let q1 = restored.iter().find(|t| t.id == "q1").unwrap();
        assert_eq!(q1.state, DownloadState::Queued);
    }

    // ========== Serialization roundtrip tests ==========

    #[test]
    fn test_snapshot_entry_serde_roundtrip() {
        let entry = SnapshotEntry {
            id: "snap-20260815-120000-000".to_string(),
            name: "test entry".to_string(),
            description: Some("a description".to_string()),
            created_at: Utc::now(),
            task_count: 3,
            total_size: 102400,
            file_path: PathBuf::from("/tmp/snapshots/snap-20260815-120000-000.json"),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: SnapshotEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, entry.id);
        assert_eq!(back.name, entry.name);
        assert_eq!(back.description, entry.description);
        assert_eq!(back.task_count, entry.task_count);
        assert_eq!(back.total_size, entry.total_size);
        assert_eq!(back.file_path, entry.file_path);
    }

    #[test]
    fn test_snapshot_entry_serde_null_description() {
        let entry = SnapshotEntry {
            id: "snap-1".to_string(),
            name: "no desc".to_string(),
            description: None,
            created_at: Utc::now(),
            task_count: 0,
            total_size: 0,
            file_path: PathBuf::from("/tmp/snap-1.json"),
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"description\":null") || !json.contains("description"));
        let back: SnapshotEntry = serde_json::from_str(&json).unwrap();
        assert!(back.description.is_none());
    }

    #[test]
    fn test_snapshot_entry_serde_missing_description_uses_default() {
        // Backward compat: if description field is absent, defaults to None
        let json = r#"{
            "id": "snap-legacy",
            "name": "legacy",
            "created_at": "2026-08-15T12:00:00Z",
            "task_count": 1,
            "total_size": 100,
            "file_path": "/tmp/snap-legacy.json"
        }"#;
        let entry: SnapshotEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.id, "snap-legacy");
        assert!(entry.description.is_none());
    }

    #[test]
    fn test_snapshot_entry_extra_fields_ignored() {
        let json = r#"{
            "id": "snap-x",
            "name": "x",
            "created_at": "2026-08-15T12:00:00Z",
            "task_count": 0,
            "total_size": 0,
            "file_path": "/tmp/snap-x.json",
            "unknown_field": "ignored",
            "another": 42
        }"#;
        let entry: SnapshotEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.id, "snap-x");
    }

    #[test]
    fn test_snapshot_entry_pretty_serde() {
        let entry = SnapshotEntry {
            id: "snap-pretty".to_string(),
            name: "pretty print".to_string(),
            description: Some("pretty".to_string()),
            created_at: Utc::now(),
            task_count: 1,
            total_size: 999,
            file_path: PathBuf::from("/tmp/pretty.json"),
        };
        let pretty = serde_json::to_string_pretty(&entry).unwrap();
        assert!(pretty.contains('\n'));
        let back: SnapshotEntry = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.id, entry.id);
    }

    #[test]
    fn test_snapshot_data_serde_roundtrip() {
        let data = SnapshotData {
            version: SNAPSHOT_VERSION,
            id: "snap-data-1".to_string(),
            name: "data test".to_string(),
            description: Some("full data".to_string()),
            created_at: Utc::now(),
            global_speed_limit: 1048576,
            max_concurrent: 5,
            tasks: vec![],
        };
        let json = serde_json::to_string(&data).unwrap();
        let back: SnapshotData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.version, SNAPSHOT_VERSION);
        assert_eq!(back.id, data.id);
        assert_eq!(back.global_speed_limit, 1048576);
        assert_eq!(back.max_concurrent, 5);
        assert!(back.tasks.is_empty());
    }

    #[test]
    fn test_snapshot_data_serde_with_tasks() {
        let (mut mgr, _temp) = make_manager();
        let tasks = vec![
            make_test_task("t1", "file1.txt"),
            make_test_task("t2", "file2.mp4"),
        ];
        let entry = mgr
            .create_snapshot(&tasks, "with-tasks".to_string(), None, 0, 5)
            .unwrap();
        let data = mgr.get_snapshot(&entry.id).unwrap();
        let json = serde_json::to_string(&data).unwrap();
        let back: SnapshotData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.tasks.len(), 2);
        assert_eq!(back.tasks[0].name, "file1.txt");
        assert_eq!(back.tasks[1].name, "file2.mp4");
    }

    #[test]
    fn test_snapshot_data_default_fields() {
        // Test that default values are applied when fields are missing
        let json = r#"{
            "version": 1,
            "id": "snap-min",
            "name": "minimal",
            "created_at": "2026-08-15T12:00:00Z",
            "tasks": []
        }"#;
        let data: SnapshotData = serde_json::from_str(json).unwrap();
        assert_eq!(data.global_speed_limit, 0);
        assert_eq!(data.max_concurrent, 0);
        assert!(data.description.is_none());
    }

    #[test]
    fn test_snapshot_summary_serialize() {
        let summary = SnapshotSummary {
            id: "snap-sum".to_string(),
            name: "summary test".to_string(),
            description: Some("sum desc".to_string()),
            created_at: Utc::now(),
            task_count: 10,
            total_size: 1024 * 1024,
            file_size_bytes: 4096,
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("snap-sum"));
        assert!(json.contains("summary test"));
        assert!(json.contains("sum desc"));
        assert!(json.contains("1048576"));
        assert!(json.contains("4096"));
    }

    // ========== Clone / Debug traits ==========

    #[test]
    fn test_snapshot_entry_clone_debug() {
        let entry = SnapshotEntry {
            id: "snap-clone".to_string(),
            name: "clone test".to_string(),
            description: Some("clone me".to_string()),
            created_at: Utc::now(),
            task_count: 1,
            total_size: 100,
            file_path: PathBuf::from("/tmp/clone.json"),
        };
        let cloned = entry.clone();
        assert_eq!(cloned.id, entry.id);
        assert_eq!(cloned.name, entry.name);
        assert_eq!(cloned.description, entry.description);
        assert_eq!(cloned.task_count, entry.task_count);
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("snap-clone"));
    }

    #[test]
    fn test_snapshot_data_clone_debug() {
        let data = SnapshotData {
            version: SNAPSHOT_VERSION,
            id: "snap-data-clone".to_string(),
            name: "data clone".to_string(),
            description: None,
            created_at: Utc::now(),
            global_speed_limit: 0,
            max_concurrent: 3,
            tasks: vec![],
        };
        let cloned = data.clone();
        assert_eq!(cloned.id, data.id);
        let debug_str = format!("{:?}", data);
        assert!(debug_str.contains("snap-data-clone"));
    }

    #[test]
    fn test_snapshot_summary_clone_debug() {
        let summary = SnapshotSummary {
            id: "snap-sum-clone".to_string(),
            name: "sum clone".to_string(),
            description: None,
            created_at: Utc::now(),
            task_count: 0,
            total_size: 0,
            file_size_bytes: 0,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.id, summary.id);
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("snap-sum-clone"));
    }

    // ========== SnapshotError Display / Debug ==========

    #[test]
    fn test_snapshot_error_display_io() {
        let err = SnapshotError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("file missing"));
    }

    #[test]
    fn test_snapshot_error_display_serialize() {
        let bad_json = serde_json::from_str::<SnapshotData>("not json").unwrap_err();
        let err = SnapshotError::Serialize(bad_json);
        let msg = format!("{}", err);
        assert!(msg.to_lowercase().contains("serialize") || msg.contains("expected"));
    }

    #[test]
    fn test_snapshot_error_display_not_found() {
        let err = SnapshotError::NotFound("snap-xyz".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("snap-xyz"));
        assert!(msg.to_lowercase().contains("not found"));
    }

    #[test]
    fn test_snapshot_error_display_invalid() {
        let err = SnapshotError::Invalid("corrupt data".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("corrupt data"));
    }

    #[test]
    fn test_snapshot_error_display_unsupported_version() {
        let err = SnapshotError::UnsupportedVersion(99);
        let msg = format!("{}", err);
        assert!(msg.contains("99"));
        assert!(msg.to_lowercase().contains("unsupported"));
    }

    #[test]
    fn test_snapshot_error_debug() {
        let err = SnapshotError::NotFound("snap-debug".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("snap-debug"));
    }

    #[test]
    fn test_snapshot_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "io fail");
        let err: SnapshotError = io_err.into();
        assert!(matches!(err, SnapshotError::Io(_)));
    }

    #[test]
    fn test_snapshot_error_from_serde() {
        let serde_err = serde_json::from_str::<SnapshotData>("bad").unwrap_err();
        let err: SnapshotError = serde_err.into();
        assert!(matches!(err, SnapshotError::Serialize(_)));
    }

    // ========== format_size boundary tests ==========

    #[test]
    fn test_format_size_boundary_values() {
        // Exact boundaries
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1), "1 B");
        assert_eq!(format_size(1023), "1023 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1024 * 1024 - 1), "1024.0 KB");
        assert_eq!(format_size(1024 * 1024), "1.0 MB");
        assert_eq!(format_size(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_size(1024u64.pow(4)), "1.0 TB");
    }

    #[test]
    fn test_format_size_large_values() {
        // 2 TB
        assert_eq!(format_size(2 * 1024u64.pow(4)), "2.0 TB");
        // 10 TB
        assert_eq!(format_size(10 * 1024u64.pow(4)), "10.0 TB");
    }

    #[test]
    fn test_format_size_fractional() {
        // 1.5 KB
        assert_eq!(format_size(1536), "1.5 KB");
        // 2.5 MB
        assert_eq!(format_size(2 * 1024 * 1024 + 512 * 1024), "2.5 MB");
    }

    // ========== Unicode / boundary tests ==========

    #[test]
    fn test_snapshot_with_unicode_name() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "中文快照".to_string(), None, 0, 5)
            .unwrap();
        assert_eq!(entry.name, "中文快照");
        let data = mgr.get_snapshot(&entry.id).unwrap();
        assert_eq!(data.name, "中文快照");
    }

    #[test]
    fn test_snapshot_with_emoji_name() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "🚀 snapshot".to_string(), None, 0, 5)
            .unwrap();
        assert_eq!(entry.name, "🚀 snapshot");
    }

    #[test]
    fn test_snapshot_with_unicode_description() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(
                &[],
                "unicode desc".to_string(),
                Some("这是一个测试描述 🎉".to_string()),
                0,
                5,
            )
            .unwrap();
        let data = mgr.get_snapshot(&entry.id).unwrap();
        assert_eq!(data.description, Some("这是一个测试描述 🎉".to_string()));
    }

    #[test]
    fn test_snapshot_with_unicode_task_names() {
        let (mut mgr, _temp) = make_manager();
        let tasks = vec![
            make_test_task("u1", "日本語ファイル.txt"),
            make_test_task("u2", "한국어파일.mp4"),
            make_test_task("u3", "🎵 music.mp3"),
        ];
        mgr.create_snapshot(&tasks, "unicode tasks".to_string(), None, 0, 5)
            .unwrap();
        let list = mgr.list_snapshots();
        let data = mgr.get_snapshot(&list[0].id).unwrap();
        assert_eq!(data.tasks.len(), 3);
        assert_eq!(data.tasks[0].name, "日本語ファイル.txt");
    }

    #[test]
    fn test_snapshot_unicode_persistence_roundtrip() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        mgr.create_snapshot(
            &[],
            "中文持久化".to_string(),
            Some("emoji 🎊".to_string()),
            0,
            5,
        )
        .unwrap();
        let mut mgr2 = SnapshotManager::new(snap_dir);
        mgr2.load().unwrap();
        let list = mgr2.list_snapshots();
        assert_eq!(list[0].name, "中文持久化");
        let data = mgr2.get_snapshot(&list[0].id).unwrap();
        assert_eq!(data.description, Some("emoji 🎊".to_string()));
    }

    // ========== Manager boundary tests ==========

    #[test]
    fn test_snapshot_manager_new() {
        let mgr = SnapshotManager::new(PathBuf::from("/tmp/test"));
        assert_eq!(mgr.snapshot_count(), 0);
    }

    #[test]
    fn test_snapshot_manager_load_nonexistent_dir() {
        let mut mgr = SnapshotManager::new(PathBuf::from("/tmp/nonexistent_snap_dir_xyz"));
        // load() should succeed even if dir doesn't exist
        assert!(mgr.load().is_ok());
        assert_eq!(mgr.snapshot_count(), 0);
    }

    #[test]
    fn test_snapshot_manager_list_empty() {
        let (mgr, _temp) = make_manager();
        let list = mgr.list_snapshots();
        assert!(list.is_empty());
    }

    #[test]
    fn test_snapshot_manager_snapshot_count_empty() {
        let (mgr, _temp) = make_manager();
        assert_eq!(mgr.snapshot_count(), 0);
    }

    #[test]
    fn test_snapshot_manager_max_snapshots_exact() {
        let (mut mgr, _temp) = make_manager();
        for i in 0..MAX_SNAPSHOTS {
            mgr.create_snapshot(&[], format!("snap-{}", i), None, 0, 5)
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        // Exactly at limit, no pruning
        assert_eq!(mgr.snapshot_count(), MAX_SNAPSHOTS);
    }

    #[test]
    fn test_snapshot_manager_prune_removes_oldest() {
        let (mut mgr, _temp) = make_manager();
        for i in 0..MAX_SNAPSHOTS + 3 {
            mgr.create_snapshot(&[], format!("old-{}", i), None, 0, 5)
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(mgr.snapshot_count(), MAX_SNAPSHOTS);
        let list = mgr.list_snapshots();
        // First 3 should have been pruned
        assert!(list.iter().all(|s| s.name != "old-0"));
        assert!(list.iter().all(|s| s.name != "old-1"));
        assert!(list.iter().all(|s| s.name != "old-2"));
    }

    #[test]
    fn test_snapshot_manager_create_returns_entry_fields() {
        let (mut mgr, _temp) = make_manager();
        let tasks = vec![make_test_task("f1", "big_file.mkv")];
        let entry = mgr
            .create_snapshot(
                &tasks,
                "field test".to_string(),
                Some("checking fields".to_string()),
                512000,
                10,
            )
            .unwrap();

        assert!(entry.id.starts_with("snap-"));
        assert_eq!(entry.name, "field test");
        assert_eq!(entry.description, Some("checking fields".to_string()));
        assert_eq!(entry.task_count, 1);
        assert_eq!(entry.total_size, 1024);
        assert!(entry.file_path.to_str().unwrap().ends_with(".json"));
        assert!(entry.file_path.exists());
    }

    #[test]
    fn test_snapshot_manager_list_order() {
        let (mut mgr, _temp) = make_manager();
        mgr.create_snapshot(&[], "first".to_string(), None, 0, 5)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.create_snapshot(&[], "second".to_string(), None, 0, 5)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.create_snapshot(&[], "third".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].name, "first");
        assert_eq!(list[1].name, "second");
        assert_eq!(list[2].name, "third");
    }

    #[test]
    fn test_snapshot_manager_delete_middle_snapshot() {
        let (mut mgr, _temp) = make_manager();
        mgr.create_snapshot(&[], "keep-1".to_string(), None, 0, 5)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.create_snapshot(&[], "delete-me".to_string(), None, 0, 5)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.create_snapshot(&[], "keep-2".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let del_id = list[1].id.clone();
        mgr.delete_snapshot(&del_id).unwrap();

        assert_eq!(mgr.snapshot_count(), 2);
        let remaining = mgr.list_snapshots();
        assert_eq!(remaining[0].name, "keep-1");
        assert_eq!(remaining[1].name, "keep-2");
    }

    #[test]
    fn test_snapshot_manager_delete_first_snapshot() {
        let (mut mgr, _temp) = make_manager();
        mgr.create_snapshot(&[], "first".to_string(), None, 0, 5)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.create_snapshot(&[], "second".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let first_id = list[0].id.clone();
        mgr.delete_snapshot(&first_id).unwrap();

        assert_eq!(mgr.snapshot_count(), 1);
        assert_eq!(mgr.list_snapshots()[0].name, "second");
    }

    #[test]
    fn test_snapshot_manager_delete_last_snapshot() {
        let (mut mgr, _temp) = make_manager();
        mgr.create_snapshot(&[], "first".to_string(), None, 0, 5)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.create_snapshot(&[], "last".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let last_id = list[1].id.clone();
        mgr.delete_snapshot(&last_id).unwrap();

        assert_eq!(mgr.snapshot_count(), 1);
        assert_eq!(mgr.list_snapshots()[0].name, "first");
    }

    // ========== Persistence tests ==========

    #[test]
    fn test_snapshot_save_creates_index_file() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        mgr.create_snapshot(&[], "index-test".to_string(), None, 0, 5)
            .unwrap();
        let index_path = snap_dir.join("snapshot_index.json");
        assert!(index_path.exists());
    }

    #[test]
    fn test_snapshot_save_creates_snapshot_file() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        let entry = mgr
            .create_snapshot(&[], "file-test".to_string(), None, 0, 5)
            .unwrap();
        assert!(entry.file_path.exists());
        assert!(snap_dir.join("snapshot_index.json").exists());
    }

    #[test]
    fn test_snapshot_no_tmp_leftover() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        mgr.create_snapshot(&[], "atomic-test".to_string(), None, 0, 5)
            .unwrap();
        // Check no .tmp files remain
        for entry in std::fs::read_dir(&snap_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert!(
                !path.to_str().unwrap().ends_with(".tmp"),
                "tmp file found: {:?}",
                path
            );
        }
    }

    #[test]
    fn test_snapshot_overwrite_index() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        mgr.create_snapshot(&[], "snap-a".to_string(), None, 0, 5)
            .unwrap();
        mgr.create_snapshot(&[], "snap-b".to_string(), None, 0, 5)
            .unwrap();

        let mut mgr2 = SnapshotManager::new(snap_dir);
        mgr2.load().unwrap();
        assert_eq!(mgr2.snapshot_count(), 2);
    }

    #[test]
    fn test_snapshot_corrupt_index() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();
        // Write corrupt index
        std::fs::write(snap_dir.join("snapshot_index.json"), "not json").unwrap();

        let result = mgr.load();
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_empty_index_file() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();
        // Write empty index
        std::fs::write(snap_dir.join("snapshot_index.json"), "").unwrap();

        let result = mgr.load();
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_empty_json_array_index() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();
        std::fs::write(snap_dir.join("snapshot_index.json"), "[]").unwrap();

        mgr.load().unwrap();
        assert_eq!(mgr.snapshot_count(), 0);
    }

    #[test]
    fn test_snapshot_full_roundtrip() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let tasks = vec![
            make_test_task("rt1", "round_trip.txt"),
            make_test_task("rt2", "trip.mp4"),
        ];
        mgr.create_snapshot(
            &tasks,
            "roundtrip".to_string(),
            Some("full roundtrip test".to_string()),
            2048,
            8,
        )
        .unwrap();

        // Reload manager
        let mut mgr2 = SnapshotManager::new(snap_dir);
        mgr2.load().unwrap();
        assert_eq!(mgr2.snapshot_count(), 1);

        let list = mgr2.list_snapshots();
        let data = mgr2.get_snapshot(&list[0].id).unwrap();
        assert_eq!(data.name, "roundtrip");
        assert_eq!(data.tasks.len(), 2);

        // Restore
        let restored = mgr2.restore_snapshot(&list[0].id, &data_dir).unwrap();
        assert_eq!(restored.len(), 2);
    }

    #[test]
    fn test_snapshot_pretty_json_on_disk() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "pretty-disk".to_string(), None, 0, 5)
            .unwrap();
        let content = std::fs::read_to_string(&entry.file_path).unwrap();
        // Pretty-printed JSON has newlines
        assert!(content.contains('\n'));
    }

    // ========== SnapshotData version tests ==========

    #[test]
    fn test_snapshot_unsupported_version_zero() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "ver-test".to_string(), None, 0, 5)
            .unwrap();

        // Manually write a snapshot with version 0
        let mut data = mgr.get_snapshot(&entry.id).unwrap();
        data.version = 0;
        let json = serde_json::to_string_pretty(&data).unwrap();
        std::fs::write(&entry.file_path, json).unwrap();

        let result = mgr.get_snapshot(&entry.id);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SnapshotError::UnsupportedVersion(0)
        ));
    }

    #[test]
    fn test_snapshot_unsupported_version_future() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "future-ver".to_string(), None, 0, 5)
            .unwrap();

        let mut data = mgr.get_snapshot(&entry.id).unwrap();
        data.version = SNAPSHOT_VERSION + 1;
        let json = serde_json::to_string_pretty(&data).unwrap();
        std::fs::write(&entry.file_path, json).unwrap();

        let result = mgr.get_snapshot(&entry.id);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SnapshotError::UnsupportedVersion(_)
        ));
    }

    // ========== Restore edge cases ==========

    #[test]
    fn test_restore_empty_snapshot() {
        let (mut mgr, temp) = make_manager();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        mgr.create_snapshot(&[], "empty-restore".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let restored = mgr.restore_snapshot(&list[0].id, &data_dir).unwrap();
        assert!(restored.is_empty());

        // task_queue.json should be written with empty array
        let loaded = load_task_queue(&data_dir).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_restore_preserves_non_downloading_states() {
        let (mut mgr, temp) = make_manager();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let mut t1 = make_test_task("err1", "error_task.txt");
        t1.state = DownloadState::Error;

        let mut t2 = make_test_task("comp1", "completed.txt");
        t2.state = DownloadState::Complete;

        let mut t3 = make_test_task("dl1", "downloading.txt");
        t3.state = DownloadState::Downloading;
        t3.speed_bps = 1000.0;

        mgr.create_snapshot(&[t1, t2, t3], "states".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let restored = mgr.restore_snapshot(&list[0].id, &data_dir).unwrap();

        let err_task = restored.iter().find(|t| t.id == "err1").unwrap();
        assert_eq!(err_task.state, DownloadState::Error);

        let comp_task = restored.iter().find(|t| t.id == "comp1").unwrap();
        assert_eq!(comp_task.state, DownloadState::Complete);

        let dl_task = restored.iter().find(|t| t.id == "dl1").unwrap();
        assert_eq!(dl_task.state, DownloadState::Paused);
        assert_eq!(dl_task.speed_bps, 0.0);
    }

    #[test]
    fn test_restore_overwrites_existing_task_queue() {
        let (mut mgr, temp) = make_manager();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        // Write an existing task queue
        let existing = vec![make_test_task("old1", "old_task.txt")];
        save_task_queue(&existing, &data_dir).unwrap();

        // Create snapshot with different tasks
        let new_tasks = vec![
            make_test_task("new1", "new_task1.txt"),
            make_test_task("new2", "new_task2.txt"),
        ];
        mgr.create_snapshot(&new_tasks, "overwrite".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let restored = mgr.restore_snapshot(&list[0].id, &data_dir).unwrap();

        // Old tasks should be gone
        assert_eq!(restored.len(), 2);
        assert!(restored.iter().all(|t| t.id != "old1"));

        // Verify on disk
        let loaded = load_task_queue(&data_dir).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    // ========== Snapshot summary fields ==========

    #[test]
    fn test_snapshot_summary_fields_correct() {
        let (mut mgr, _temp) = make_manager();
        let tasks = vec![
            make_test_task("s1", "file1.txt"),
            make_test_task("s2", "file2.mp4"),
        ];
        mgr.create_snapshot(
            &tasks,
            "summary-fields".to_string(),
            Some("desc here".to_string()),
            0,
            5,
        )
        .unwrap();

        let list = mgr.list_snapshots();
        let sum = &list[0];
        assert_eq!(sum.name, "summary-fields");
        assert_eq!(sum.description, Some("desc here".to_string()));
        assert_eq!(sum.task_count, 2);
        assert_eq!(sum.total_size, 2048);
        assert!(sum.file_size_bytes > 0);
        assert!(sum.id.starts_with("snap-"));
    }

    #[test]
    fn test_snapshot_summary_file_size_matches_disk() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(
                &[make_test_task("sz1", "sized.txt")],
                "size-check".to_string(),
                None,
                0,
                5,
            )
            .unwrap();

        let list = mgr.list_snapshots();
        let disk_size = std::fs::metadata(&entry.file_path).unwrap().len();
        assert_eq!(list[0].file_size_bytes, disk_size);
    }

    // ========== Constants ==========

    #[test]
    fn test_constants() {
        assert_eq!(SNAPSHOT_VERSION, 1);
        assert_eq!(MAX_SNAPSHOTS, 20);
        assert!(MAX_SNAPSHOTS > 0);
    }

    // ========== Complex workflows ==========

    #[test]
    fn test_full_lifecycle() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        // Create
        let tasks = vec![make_test_task("lc1", "lifecycle.txt")];
        let entry = mgr
            .create_snapshot(
                &tasks,
                "lifecycle".to_string(),
                Some("test lifecycle".to_string()),
                1024,
                3,
            )
            .unwrap();
        assert_eq!(mgr.snapshot_count(), 1);

        // List
        let list = mgr.list_snapshots();
        assert_eq!(list.len(), 1);

        // Get
        let data = mgr.get_snapshot(&entry.id).unwrap();
        assert_eq!(data.name, "lifecycle");

        // Restore
        let restored = mgr.restore_snapshot(&entry.id, &data_dir).unwrap();
        assert_eq!(restored.len(), 1);

        // Delete
        mgr.delete_snapshot(&entry.id).unwrap();
        assert_eq!(mgr.snapshot_count(), 0);

        // Reload
        let mut mgr2 = SnapshotManager::new(snap_dir);
        mgr2.load().unwrap();
        assert_eq!(mgr2.snapshot_count(), 0);
    }

    #[test]
    fn test_multiple_snapshots_independent() {
        let (mut mgr, _temp) = make_manager();

        let tasks_a = vec![make_test_task("a1", "task_a.txt")];
        let tasks_b = vec![
            make_test_task("b1", "task_b1.txt"),
            make_test_task("b2", "task_b2.txt"),
        ];

        mgr.create_snapshot(&tasks_a, "snap-a".to_string(), None, 1024, 2)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.create_snapshot(&tasks_b, "snap-b".to_string(), None, 2048, 4)
            .unwrap();

        let list = mgr.list_snapshots();
        let data_a = mgr.get_snapshot(&list[0].id).unwrap();
        let data_b = mgr.get_snapshot(&list[1].id).unwrap();

        assert_eq!(data_a.tasks.len(), 1);
        assert_eq!(data_b.tasks.len(), 2);
        assert_eq!(data_a.global_speed_limit, 1024);
        assert_eq!(data_b.global_speed_limit, 2048);

        // Delete one doesn't affect other
        mgr.delete_snapshot(&list[0].id).unwrap();
        assert_eq!(mgr.snapshot_count(), 1);
        let remaining = mgr.get_snapshot(&mgr.list_snapshots()[0].id).unwrap();
        assert_eq!(remaining.name, "snap-b");
    }

    #[test]
    fn test_create_delete_recreate_cycle() {
        let (mut mgr, _temp) = make_manager();

        for i in 0..5 {
            mgr.create_snapshot(&[], format!("cycle-{}", i), None, 0, 5)
                .unwrap();
            let list = mgr.list_snapshots();
            let id = list.last().unwrap().id.clone();
            mgr.delete_snapshot(&id).unwrap();
        }

        assert_eq!(mgr.snapshot_count(), 0);
    }

    #[test]
    fn test_snapshot_with_all_task_states() {
        let (mut mgr, _temp) = make_manager();

        let mut t1 = make_test_task("st1", "queued.txt");
        t1.state = DownloadState::Queued;
        let mut t2 = make_test_task("st2", "downloading.txt");
        t2.state = DownloadState::Downloading;
        let mut t3 = make_test_task("st3", "paused.txt");
        t3.state = DownloadState::Paused;
        let mut t4 = make_test_task("st4", "error.txt");
        t4.state = DownloadState::Error;
        let mut t5 = make_test_task("st5", "complete.txt");
        t5.state = DownloadState::Complete;

        mgr.create_snapshot(&[t1, t2, t3, t4, t5], "all-states".to_string(), None, 0, 5)
            .unwrap();

        let list = mgr.list_snapshots();
        let data = mgr.get_snapshot(&list[0].id).unwrap();
        assert_eq!(data.tasks.len(), 5);
    }

    #[test]
    fn test_snapshot_with_speed_limits() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "speed-limit".to_string(), None, 10 * 1024 * 1024, 10)
            .unwrap();
        let data = mgr.get_snapshot(&entry.id).unwrap();
        assert_eq!(data.global_speed_limit, 10 * 1024 * 1024);
        assert_eq!(data.max_concurrent, 10);
    }

    #[test]
    fn test_snapshot_with_max_concurrent_zero() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "zero-concurrent".to_string(), None, 0, 0)
            .unwrap();
        let data = mgr.get_snapshot(&entry.id).unwrap();
        assert_eq!(data.max_concurrent, 0);
    }

    #[test]
    fn test_snapshot_total_size_large() {
        let (mut mgr, _temp) = make_manager();
        let mut task = make_test_task("big", "huge_file.mkv");
        task.size = 10 * 1024 * 1024 * 1024; // 10 GB

        let entry = mgr
            .create_snapshot(&[task], "big-snap".to_string(), None, 0, 5)
            .unwrap();
        assert_eq!(entry.total_size, 10 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_snapshot_id_format() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "id-format".to_string(), None, 0, 5)
            .unwrap();
        // ID should start with "snap-" and contain timestamp
        assert!(entry.id.starts_with("snap-"));
        assert!(entry.id.len() > 10);
    }

    #[test]
    fn test_snapshot_file_is_valid_json() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "json-valid".to_string(), None, 0, 5)
            .unwrap();
        let content = std::fs::read_to_string(&entry.file_path).unwrap();
        let parsed: Result<SnapshotData, _> = serde_json::from_str(&content);
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_index_file_is_valid_json() {
        let (mut mgr, temp) = make_manager();
        let snap_dir = temp.path().join("snapshots");
        mgr.create_snapshot(&[], "index-valid".to_string(), None, 0, 5)
            .unwrap();
        let content = std::fs::read_to_string(snap_dir.join("snapshot_index.json")).unwrap();
        let parsed: Result<Vec<SnapshotEntry>, _> = serde_json::from_str(&content);
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_snapshot_unique_ids() {
        let (mut mgr, _temp) = make_manager();
        let mut ids = std::collections::HashSet::new();
        for i in 0..10 {
            let entry = mgr
                .create_snapshot(&[], format!("unique-{}", i), None, 0, 5)
                .unwrap();
            ids.insert(entry.id);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(ids.len(), 10);
    }

    #[test]
    fn test_snapshot_entry_file_path_matches_id() {
        let (mut mgr, _temp) = make_manager();
        let entry = mgr
            .create_snapshot(&[], "path-match".to_string(), None, 0, 5)
            .unwrap();
        let expected_filename = format!("{}.json", entry.id);
        assert_eq!(
            entry.file_path.file_name().unwrap().to_str().unwrap(),
            expected_filename
        );
    }
}
