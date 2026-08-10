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
}
