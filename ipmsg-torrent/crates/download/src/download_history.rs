//! Download history persistence
//!
//! Records completed and failed downloads so users can review past activity
//! even after tasks are removed from the active queue.

use crate::{DownloadProtocol, DownloadState, DownloadTask};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// A single history entry for a completed or failed download.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Original task ID
    pub task_id: String,
    /// File name
    pub name: String,
    /// Protocol used
    pub protocol: HistoryProtocol,
    /// Final outcome
    pub outcome: HistoryOutcome,
    /// File size in bytes
    pub size: u64,
    /// Bytes actually downloaded
    pub downloaded: u64,
    /// Where the file was saved (for completed downloads)
    pub save_path: PathBuf,
    /// Error message (for failed downloads)
    pub error: Option<String>,
    /// When the task was created
    pub created_at: DateTime<Utc>,
    /// When the download finished or failed
    pub finished_at: DateTime<Utc>,
    /// User-defined tags
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum HistoryProtocol {
    Torrent,
    Ed2k,
    Xunlei,
    Magnet,
    P2P,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum HistoryOutcome {
    Completed,
    Failed,
}

impl From<DownloadProtocol> for HistoryProtocol {
    fn from(p: DownloadProtocol) -> Self {
        match p {
            DownloadProtocol::Torrent => Self::Torrent,
            DownloadProtocol::Ed2k => Self::Ed2k,
            DownloadProtocol::Xunlei => Self::Xunlei,
            DownloadProtocol::Magnet => Self::Magnet,
            DownloadProtocol::P2P => Self::P2P,
        }
    }
}

impl HistoryEntry {
    /// Create a history entry from a completed or failed task.
    pub fn from_task(task: &DownloadTask) -> Option<Self> {
        let outcome = match task.state {
            DownloadState::Complete => HistoryOutcome::Completed,
            DownloadState::Error => HistoryOutcome::Failed,
            _ => return None,
        };

        Some(Self {
            task_id: task.id.clone(),
            name: task.name.clone(),
            protocol: task.protocol.into(),
            outcome,
            size: task.size,
            downloaded: task.downloaded,
            save_path: task.save_path.clone(),
            error: task.error.clone(),
            created_at: task.created_at,
            finished_at: task.updated_at,
            tags: task.tags.clone(),
        })
    }

    /// Human-readable summary
    pub fn summary(&self) -> String {
        let status = match self.outcome {
            HistoryOutcome::Completed => "✓",
            HistoryOutcome::Failed => "✗",
        };
        let size_str = format_size(self.size);
        let time_str = self
            .finished_at
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%d %H:%M")
            .to_string();
        format!(
            "{} {} [{}] {} ({}) - {}",
            status,
            self.name,
            format_protocol(self.protocol),
            size_str,
            time_str,
            self.task_id
        )
    }
}

fn format_protocol(p: HistoryProtocol) -> &'static str {
    match p {
        HistoryProtocol::Torrent => "Torrent",
        HistoryProtocol::Ed2k => "Ed2k",
        HistoryProtocol::Xunlei => "Xunlei",
        HistoryProtocol::Magnet => "Magnet",
        HistoryProtocol::P2P => "P2P",
    }
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Maximum number of history entries to keep.
const MAX_HISTORY_ENTRIES: usize = 1000;

/// Save history entries to disk (atomic write via temp file).
pub fn save_history(entries: &[HistoryEntry], data_dir: &Path) -> Result<(), HistoryError> {
    let history_path = data_dir.join("download_history.json");
    let tmp_path = data_dir.join("download_history.json.tmp");

    let json = serde_json::to_string_pretty(entries)
        .map_err(|e| HistoryError::Serialize(e.to_string()))?;

    std::fs::write(&tmp_path, &json).map_err(|e| HistoryError::Io(e.to_string()))?;

    std::fs::rename(&tmp_path, &history_path).map_err(|e| HistoryError::Io(e.to_string()))?;

    Ok(())
}

/// Load history entries from disk.
pub fn load_history(data_dir: &Path) -> Result<Vec<HistoryEntry>, HistoryError> {
    let history_path = data_dir.join("download_history.json");

    if !history_path.exists() {
        return Ok(Vec::new());
    }

    let json =
        std::fs::read_to_string(&history_path).map_err(|e| HistoryError::Io(e.to_string()))?;

    let entries: Vec<HistoryEntry> =
        serde_json::from_str(&json).map_err(|e| HistoryError::Deserialize(e.to_string()))?;

    Ok(entries)
}

/// Append a single entry, enforcing the max-size cap (oldest entries evicted first).
pub fn append_entry(data_dir: &Path, entry: HistoryEntry) -> Result<(), HistoryError> {
    let mut entries = load_history(data_dir)?;
    entries.push(entry);

    // Evict oldest if over capacity
    if entries.len() > MAX_HISTORY_ENTRIES {
        let excess = entries.len() - MAX_HISTORY_ENTRIES;
        entries.drain(..excess);
    }

    save_history(&entries, data_dir)
}

/// Remove entries matching a task ID (used when user explicitly clears history).
pub fn remove_entry(data_dir: &Path, task_id: &str) -> Result<bool, HistoryError> {
    let mut entries = load_history(data_dir)?;
    let before = entries.len();
    entries.retain(|e| e.task_id != task_id);

    if entries.len() < before {
        save_history(&entries, data_dir)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Clear all history entries.
pub fn clear_history(data_dir: &Path) -> Result<(), HistoryError> {
    save_history(&[], data_dir)
}

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("deserialize error: {0}")]
    Deserialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DownloadPriority;

    fn make_task(state: DownloadState, error: Option<String>) -> DownloadTask {
        DownloadTask {
            id: "hist-001".into(),
            name: "ubuntu.iso".into(),
            protocol: DownloadProtocol::Torrent,
            size: 1024 * 1024 * 100,
            downloaded: 1024 * 1024 * 100,
            state,
            error,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp/downloads"),
            created_at: Utc::now() - chrono::Duration::minutes(30),
            updated_at: Utc::now(),
            tags: vec!["linux".into()],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
        }
    }

    #[test]
    fn test_from_task_completed() {
        let task = make_task(DownloadState::Complete, None);
        let entry = HistoryEntry::from_task(&task).unwrap();

        assert_eq!(entry.task_id, "hist-001");
        assert_eq!(entry.name, "ubuntu.iso");
        assert_eq!(entry.outcome, HistoryOutcome::Completed);
        assert_eq!(entry.protocol, HistoryProtocol::Torrent);
        assert_eq!(entry.size, 1024 * 1024 * 100);
        assert_eq!(entry.downloaded, 1024 * 1024 * 100);
        assert!(entry.error.is_none());
        assert_eq!(entry.tags, vec!["linux"]);
    }

    #[test]
    fn test_from_task_failed() {
        let task = make_task(DownloadState::Error, Some("timeout".into()));
        let entry = HistoryEntry::from_task(&task).unwrap();

        assert_eq!(entry.outcome, HistoryOutcome::Failed);
        assert_eq!(entry.error, Some("timeout".into()));
    }

    #[test]
    fn test_from_task_running_returns_none() {
        let task = make_task(DownloadState::Downloading, None);
        assert!(HistoryEntry::from_task(&task).is_none());
    }

    #[test]
    fn test_from_task_paused_returns_none() {
        let task = make_task(DownloadState::Paused, None);
        assert!(HistoryEntry::from_task(&task).is_none());
    }

    #[test]
    fn test_save_load_empty() {
        let dir = tempfile::tempdir().unwrap();
        let entries = load_history(dir.path()).unwrap();
        assert!(entries.is_empty());

        save_history(&[], dir.path()).unwrap();
        let entries = load_history(dir.path()).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_save_load_entries() {
        let dir = tempfile::tempdir().unwrap();

        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        save_history(&[entry.clone()], dir.path()).unwrap();

        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].task_id, entry.task_id);
        assert_eq!(loaded[0].name, entry.name);
        assert_eq!(loaded[0].outcome, HistoryOutcome::Completed);
    }

    #[test]
    fn test_append_entry() {
        let dir = tempfile::tempdir().unwrap();

        let e1 = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        append_entry(dir.path(), e1).unwrap();

        let mut e2 =
            HistoryEntry::from_task(&make_task(DownloadState::Error, Some("err".into()))).unwrap();
        e2.task_id = "hist-002".into();
        append_entry(dir.path(), e2).unwrap();

        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].task_id, "hist-001");
        assert_eq!(loaded[1].task_id, "hist-002");
    }

    #[test]
    fn test_remove_entry() {
        let dir = tempfile::tempdir().unwrap();

        let e1 = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        append_entry(dir.path(), e1).unwrap();

        let found = remove_entry(dir.path(), "hist-001").unwrap();
        assert!(found);

        let loaded = load_history(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_remove_nonexistent() {
        let dir = tempfile::tempdir().unwrap();

        let found = remove_entry(dir.path(), "nonexistent").unwrap();
        assert!(!found);
    }

    #[test]
    fn test_clear_history() {
        let dir = tempfile::tempdir().unwrap();

        let e = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        append_entry(dir.path(), e).unwrap();

        clear_history(dir.path()).unwrap();
        let loaded = load_history(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_max_history_eviction() {
        let dir = tempfile::tempdir().unwrap();

        // Insert MAX_HISTORY_ENTRIES + 10 entries
        for i in 0..(MAX_HISTORY_ENTRIES + 10) {
            let mut entry =
                HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
            entry.task_id = format!("task-{}", i);
            append_entry(dir.path(), entry).unwrap();
        }

        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), MAX_HISTORY_ENTRIES);
        // Oldest 10 should have been evicted
        assert_eq!(loaded[0].task_id, "task-10");
        assert_eq!(
            loaded[MAX_HISTORY_ENTRIES - 1].task_id,
            format!("task-{}", MAX_HISTORY_ENTRIES + 9)
        );
    }

    #[test]
    fn test_summary_completed() {
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let summary = entry.summary();
        assert!(summary.contains("✓"));
        assert!(summary.contains("ubuntu.iso"));
        assert!(summary.contains("Torrent"));
    }

    #[test]
    fn test_summary_failed() {
        let entry =
            HistoryEntry::from_task(&make_task(DownloadState::Error, Some("net err".into())))
                .unwrap();
        let summary = entry.summary();
        assert!(summary.contains("✗"));
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1536), "1.5 KB");
        assert_eq!(format_size(1048576), "1.0 MB");
        assert_eq!(format_size(1073741824), "1.0 GB");
        assert_eq!(format_size(1099511627776), "1.0 TB");
    }

    #[test]
    fn test_protocol_roundtrip() {
        let protocols = vec![
            DownloadProtocol::Torrent,
            DownloadProtocol::Ed2k,
            DownloadProtocol::Xunlei,
            DownloadProtocol::Magnet,
            DownloadProtocol::P2P,
        ];
        for p in protocols {
            let hp: HistoryProtocol = p.into();
            let _ = format_protocol(hp); // ensure it doesn't panic
        }
    }

    #[test]
    fn test_corrupted_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("download_history.json");
        std::fs::write(&path, "not valid json{{{").unwrap();

        let result = load_history(dir.path());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HistoryError::Deserialize(_)));
    }
}
