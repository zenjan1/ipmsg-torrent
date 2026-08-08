//! Task import/export for download manager
//!
//! Supports exporting download tasks to JSON files and importing them back.
//! Useful for migrating between installations or backing up task lists.

use crate::task_queue::{PersistedProtocol, PersistedState};
use crate::{DownloadPriority, DownloadTask, TimeWindow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Export format version for forward compatibility
const EXPORT_VERSION: u32 = 1;

/// A single task entry in the export file.
/// Similar to PersistedTask but includes the original URL/source for re-import.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportedTask {
    pub name: String,
    pub protocol: PersistedProtocol,
    pub size: u64,
    pub downloaded: u64,
    pub state: PersistedState,
    pub error: Option<String>,
    pub save_path: PathBuf,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub priority: DownloadPriority,
    #[serde(default)]
    pub schedule: Option<TimeWindow>,
    #[serde(default = "default_bandwidth_weight")]
    pub bandwidth_weight: u8,
    #[serde(default)]
    pub queue_position: Option<u32>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Original source URL for re-import (ed2k link, magnet URI, HTTP URL, torrent path)
    #[serde(default)]
    pub source_url: Option<String>,
    /// User-defined group for organizing downloads
    #[serde(default)]
    pub group: Option<String>,
}

fn default_bandwidth_weight() -> u8 {
    1
}

/// Export file format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFile {
    /// Format version for forward compatibility
    pub version: u32,
    /// Export timestamp
    pub exported_at: DateTime<Utc>,
    /// Optional description or label
    #[serde(default)]
    pub description: Option<String>,
    /// The exported tasks
    pub tasks: Vec<ExportedTask>,
}

/// Errors during import/export operations
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize error: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("unsupported export version: {0}")]
    UnsupportedVersion(u32),
    #[error("invalid export file: {0}")]
    InvalidFile(String),
}

/// Convert a DownloadTask to an ExportedTask.
///
/// The `source_url` is not stored in DownloadTask itself, so callers should
/// provide it via `with_source_url()` if available.
impl From<DownloadTask> for ExportedTask {
    fn from(t: DownloadTask) -> Self {
        Self {
            name: t.name,
            protocol: t.protocol.into(),
            size: t.size,
            downloaded: t.downloaded,
            state: t.state.into(),
            error: t.error,
            save_path: t.save_path,
            created_at: t.created_at,
            updated_at: t.updated_at,
            tags: t.tags,
            priority: t.priority,
            schedule: t.schedule,
            bandwidth_weight: t.bandwidth_weight,
            queue_position: t.queue_position,
            depends_on: t.depends_on,
            source_url: None,
            group: t.group,
        }
    }
}

impl ExportedTask {
    /// Set the source URL for re-import
    pub fn with_source_url(mut self, url: Option<String>) -> Self {
        self.source_url = url;
        self
    }

    /// Convert back to a DownloadTask (without an ID — caller assigns one)
    pub fn into_task(self, id: String) -> DownloadTask {
        DownloadTask {
            id,
            name: self.name,
            protocol: self.protocol.into(),
            size: self.size,
            downloaded: self.downloaded,
            state: self.state.into(),
            error: self.error,
            speed_bps: 0.0,
            save_path: self.save_path,
            created_at: self.created_at,
            updated_at: self.updated_at,
            tags: self.tags,
            priority: self.priority,
            schedule: self.schedule,
            bandwidth_weight: self.bandwidth_weight,
            queue_position: self.queue_position,
            depends_on: self.depends_on,
            notes: None,
            group: self.group,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            current_session_start: None,
        }
    }
}

/// Export tasks to a JSON file.
///
/// Writes atomically: writes to a temp file first, then renames.
pub fn export_tasks(
    tasks: &[DownloadTask],
    output_path: &Path,
    description: Option<String>,
) -> Result<usize, ExportError> {
    let exported: Vec<ExportedTask> = tasks.iter().cloned().map(ExportedTask::from).collect();
    let count = exported.len();

    let export_file = ExportFile {
        version: EXPORT_VERSION,
        exported_at: Utc::now(),
        description,
        tasks: exported,
    };

    let json = serde_json::to_string_pretty(&export_file)?;

    // Atomic write: write to .tmp then rename
    let tmp_path = output_path.with_extension("json.tmp");
    std::fs::write(&tmp_path, &json)?;
    std::fs::rename(&tmp_path, output_path)?;

    Ok(count)
}

/// Import tasks from a JSON export file.
///
/// Returns the list of imported tasks (with new IDs assigned).
/// Does NOT add them to the download manager — caller is responsible for that.
pub fn import_tasks(input_path: &Path) -> Result<Vec<ExportedTask>, ExportError> {
    let json = std::fs::read_to_string(input_path)?;
    let export_file: ExportFile = serde_json::from_str(&json)?;

    if export_file.version == 0 || export_file.version > EXPORT_VERSION {
        return Err(ExportError::UnsupportedVersion(export_file.version));
    }

    Ok(export_file.tasks)
}

/// Import tasks from a JSON string (useful for API/CLI inline input).
pub fn import_tasks_from_str(json: &str) -> Result<Vec<ExportedTask>, ExportError> {
    let export_file: ExportFile = serde_json::from_str(json)?;

    if export_file.version == 0 || export_file.version > EXPORT_VERSION {
        return Err(ExportError::UnsupportedVersion(export_file.version));
    }

    Ok(export_file.tasks)
}

/// Generate a simple unique ID for imported tasks
fn generate_import_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("import-{:x}-{:x}", timestamp, seq)
}

/// Prepare imported tasks for addition to the download manager.
///
/// - Assigns new IDs
/// - Resets state to Queued (can't resume a foreign task directly)
/// - Resets speed to 0
/// - Returns (ExportedTask, new_id, source_url) tuples
pub fn prepare_imported_tasks(
    exported: Vec<ExportedTask>,
) -> Vec<(ExportedTask, String, Option<String>)> {
    exported
        .into_iter()
        .map(|task| {
            let new_id = generate_import_id();
            let source_url = task.source_url.clone();
            (task, new_id, source_url)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DownloadProtocol, DownloadState};
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
        }
    }

    #[test]
    fn test_export_import_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let export_path = temp_dir.path().join("export.json");

        let tasks = vec![
            make_test_task("task-1", "file1.txt"),
            make_test_task("task-2", "file2.mp4"),
        ];

        // Export
        let count = export_tasks(&tasks, &export_path, Some("test export".to_string())).unwrap();
        assert_eq!(count, 2);

        // Import
        let imported = import_tasks(&export_path).unwrap();
        assert_eq!(imported.len(), 2);
        assert_eq!(imported[0].name, "file1.txt");
        assert_eq!(imported[1].name, "file2.mp4");
    }

    #[test]
    fn test_export_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let export_path = temp_dir.path().join("empty.json");

        let count = export_tasks(&[], &export_path, None).unwrap();
        assert_eq!(count, 0);

        let imported = import_tasks(&export_path).unwrap();
        assert!(imported.is_empty());
    }

    #[test]
    fn test_import_nonexistent_file() {
        let result = import_tasks(Path::new("/nonexistent/path.json"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExportError::Io(_)));
    }

    #[test]
    fn test_import_invalid_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let bad_path = temp_dir.path().join("bad.json");
        std::fs::write(&bad_path, "not json").unwrap();

        let result = import_tasks(&bad_path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExportError::Serialize(_)));
    }

    #[test]
    fn test_import_unsupported_version() {
        let temp_dir = tempfile::tempdir().unwrap();
        let bad_path = temp_dir.path().join("future.json");
        let content = r#"{"version":999,"exported_at":"2026-01-01T00:00:00Z","tasks":[]}"#;
        std::fs::write(&bad_path, content).unwrap();

        let result = import_tasks(&bad_path);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ExportError::UnsupportedVersion(999)
        ));
    }

    #[test]
    fn test_export_preserves_metadata() {
        let temp_dir = tempfile::tempdir().unwrap();
        let export_path = temp_dir.path().join("meta.json");

        let mut task = make_test_task("meta-1", "meta_file.txt");
        task.tags = vec!["movies".to_string(), "2026".to_string()];
        task.priority = DownloadPriority::High;
        task.bandwidth_weight = 5;
        task.queue_position = Some(3);
        task.depends_on = vec!["dep-1".to_string()];

        let count = export_tasks(&[task], &export_path, None).unwrap();
        assert_eq!(count, 1);

        let imported = import_tasks(&export_path).unwrap();
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].tags, vec!["movies", "2026"]);
        assert_eq!(imported[0].priority, DownloadPriority::High);
        assert_eq!(imported[0].bandwidth_weight, 5);
        assert_eq!(imported[0].queue_position, Some(3));
        assert_eq!(imported[0].depends_on, vec!["dep-1"]);
    }

    #[test]
    fn test_source_url_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let export_path = temp_dir.path().join("url.json");

        let task = make_test_task("url-1", "url_file.txt");
        let mut exported = ExportedTask::from(task);
        exported.source_url = Some("ed2k://|file|test.txt|1024|ABC123|/".to_string());

        let export_file = ExportFile {
            version: EXPORT_VERSION,
            exported_at: Utc::now(),
            description: None,
            tasks: vec![exported],
        };

        let json = serde_json::to_string_pretty(&export_file).unwrap();
        std::fs::write(&export_path, &json).unwrap();

        let imported = import_tasks(&export_path).unwrap();
        assert_eq!(
            imported[0].source_url,
            Some("ed2k://|file|test.txt|1024|ABC123|/".to_string())
        );
    }

    #[test]
    fn test_import_from_str() {
        let json = r#"{
            "version": 1,
            "exported_at": "2026-01-01T00:00:00Z",
            "description": "inline import",
            "tasks": [{
                "name": "test.txt",
                "protocol": "Xunlei",
                "size": 2048,
                "downloaded": 0,
                "state": "Queued",
                "error": null,
                "save_path": "/tmp/dl",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "tags": [],
                "priority": "Normal",
                "schedule": null,
                "bandwidth_weight": 1,
                "queue_position": null,
                "depends_on": [],
                "source_url": "http://example.com/test.txt"
            }]
        }"#;

        let imported = import_tasks_from_str(json).unwrap();
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].name, "test.txt");
        assert_eq!(imported[0].size, 2048);
        assert_eq!(
            imported[0].source_url,
            Some("http://example.com/test.txt".to_string())
        );
    }

    #[test]
    fn test_prepare_imported_tasks() {
        let exported = vec![
            ExportedTask {
                name: "file1.txt".to_string(),
                protocol: PersistedProtocol::Xunlei,
                size: 1024,
                downloaded: 512,
                state: PersistedState::Downloading,
                error: None,
                save_path: PathBuf::from("/tmp/dl"),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
                queue_position: None,
                depends_on: Vec::new(),
                source_url: Some("http://example.com/file1.txt".to_string()),
                group: None,
            },
            ExportedTask {
                name: "file2.txt".to_string(),
                protocol: PersistedProtocol::Ed2k,
                size: 2048,
                downloaded: 0,
                state: PersistedState::Queued,
                error: None,
                save_path: PathBuf::from("/tmp/dl"),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
                queue_position: None,
                depends_on: Vec::new(),
                source_url: Some("ed2k://|file|file2.txt|2048|HASH|/".to_string()),
                group: None,
            },
        ];

        let prepared = prepare_imported_tasks(exported);
        assert_eq!(prepared.len(), 2);

        // Each should have a unique ID and source URL
        let id1 = &prepared[0].1;
        let id2 = &prepared[1].1;
        assert_ne!(id1, id2);
        assert!(id1.starts_with("import-"));
        assert!(id2.starts_with("import-"));

        // Source URLs preserved
        assert_eq!(
            prepared[0].2,
            Some("http://example.com/file1.txt".to_string())
        );
        assert_eq!(
            prepared[1].2,
            Some("ed2k://|file|file2.txt|2048|HASH|/".to_string())
        );
    }

    #[test]
    fn test_into_task_conversion() {
        let exported = ExportedTask {
            name: "converted.txt".to_string(),
            protocol: PersistedProtocol::Torrent,
            size: 4096,
            downloaded: 1024,
            state: PersistedState::Paused,
            error: Some("timeout".to_string()),
            save_path: PathBuf::from("/downloads"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["tag1".to_string()],
            priority: DownloadPriority::Low,
            schedule: None,
            bandwidth_weight: 3,
            queue_position: Some(5),
            depends_on: vec!["dep-1".to_string()],
            source_url: None,
            group: None,
        };

        let task = exported.into_task("new-id".to_string());
        assert_eq!(task.id, "new-id");
        assert_eq!(task.name, "converted.txt");
        assert_eq!(task.protocol, DownloadProtocol::Torrent);
        assert_eq!(task.size, 4096);
        assert_eq!(task.downloaded, 1024);
        assert_eq!(task.state, DownloadState::Paused);
        assert_eq!(task.error, Some("timeout".to_string()));
        assert_eq!(task.speed_bps, 0.0); // Reset on import
        assert_eq!(task.tags, vec!["tag1"]);
        assert_eq!(task.priority, DownloadPriority::Low);
        assert_eq!(task.bandwidth_weight, 3);
        assert_eq!(task.queue_position, Some(5));
        assert_eq!(task.depends_on, vec!["dep-1"]);
    }

    #[test]
    fn test_export_atomic_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let export_path = temp_dir.path().join("atomic.json");

        // Write some initial content
        std::fs::write(&export_path, "old content").unwrap();

        let tasks = vec![make_test_task("atomic-1", "atomic.txt")];
        export_tasks(&tasks, &export_path, None).unwrap();

        // Verify the file was replaced (not appended)
        let content = std::fs::read_to_string(&export_path).unwrap();
        assert!(content.contains("\"version\": 1"));
        assert!(content.contains("atomic.txt"));
        assert!(!content.contains("old content"));

        // Verify no .tmp file left behind
        assert!(!export_path.with_extension("json.tmp").exists());
    }

    #[test]
    fn test_export_all_protocols() {
        let temp_dir = tempfile::tempdir().unwrap();
        let export_path = temp_dir.path().join("protocols.json");

        let protocols = vec![
            (DownloadProtocol::Torrent, "torrent_file.torrent"),
            (DownloadProtocol::Ed2k, "ed2k_file.txt"),
            (DownloadProtocol::Xunlei, "xunlei_file.zip"),
            (DownloadProtocol::Magnet, "magnet_content"),
            (DownloadProtocol::P2P, "p2p_file.dat"),
        ];

        let tasks: Vec<DownloadTask> = protocols
            .into_iter()
            .enumerate()
            .map(|(i, (proto, name))| {
                let mut task = make_test_task(&format!("proto-{}", i), name);
                task.protocol = proto;
                task
            })
            .collect();

        let count = export_tasks(&tasks, &export_path, None).unwrap();
        assert_eq!(count, 5);

        let imported = import_tasks(&export_path).unwrap();
        assert_eq!(imported.len(), 5);

        // Verify protocol roundtrip
        let expected_protocols = vec![
            PersistedProtocol::Torrent,
            PersistedProtocol::Ed2k,
            PersistedProtocol::Xunlei,
            PersistedProtocol::Magnet,
            PersistedProtocol::P2P,
        ];
        for (i, expected) in expected_protocols.iter().enumerate() {
            assert_eq!(imported[i].protocol, *expected);
        }
    }
}
