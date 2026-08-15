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

/// Summary statistics for download history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistorySummary {
    /// Total number of history entries
    pub total_entries: usize,
    /// Number of successfully completed downloads
    pub completed_count: usize,
    /// Number of failed downloads
    pub failed_count: usize,
    /// Total bytes of completed downloads
    pub total_completed_bytes: u64,
    /// Total bytes of failed downloads (partially downloaded)
    pub total_failed_bytes: u64,
    /// Count by protocol
    pub by_protocol: Vec<ProtocolCount>,
}

/// Count of downloads for a specific protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolCount {
    pub protocol: String,
    pub count: usize,
}

impl HistorySummary {
    /// Build summary from a list of history entries
    pub fn from_entries(entries: &[HistoryEntry]) -> Self {
        let completed_count = entries
            .iter()
            .filter(|e| e.outcome == HistoryOutcome::Completed)
            .count();
        let failed_count = entries
            .iter()
            .filter(|e| e.outcome == HistoryOutcome::Failed)
            .count();
        let total_completed_bytes: u64 = entries
            .iter()
            .filter(|e| e.outcome == HistoryOutcome::Completed)
            .map(|e| e.size)
            .sum();
        let total_failed_bytes: u64 = entries
            .iter()
            .filter(|e| e.outcome == HistoryOutcome::Failed)
            .map(|e| e.downloaded)
            .sum();

        // Count by protocol
        let mut protocol_counts = std::collections::HashMap::<String, usize>::new();
        for e in entries {
            *protocol_counts
                .entry(format_protocol(e.protocol).to_string())
                .or_default() += 1;
        }
        let mut by_protocol: Vec<ProtocolCount> = protocol_counts
            .into_iter()
            .map(|(protocol, count)| ProtocolCount { protocol, count })
            .collect();
        by_protocol.sort_by(|a, b| b.count.cmp(&a.count));

        Self {
            total_entries: entries.len(),
            completed_count,
            failed_count,
            total_completed_bytes,
            total_failed_bytes,
            by_protocol,
        }
    }

    /// Format summary as human-readable string
    pub fn format(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("📜 Download History Summary"));
        lines.push(format!("  Total entries: {}", self.total_entries));
        lines.push(format!("  ✓ Completed: {}", self.completed_count));
        lines.push(format!("  ✗ Failed: {}", self.failed_count));
        lines.push(format!(
            "  Total completed: {}",
            format_size(self.total_completed_bytes)
        ));
        if self.total_failed_bytes > 0 {
            lines.push(format!(
                "  Wasted (failed): {}",
                format_size(self.total_failed_bytes)
            ));
        }
        if !self.by_protocol.is_empty() {
            lines.push("  By protocol:".to_string());
            for pc in &self.by_protocol {
                lines.push(format!("    {}: {}", pc.protocol, pc.count));
            }
        }
        lines.join("\n")
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

    // ========== Serialization ==========

    #[test]
    fn test_history_entry_serde_roundtrip() {
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let json = serde_json::to_string(&entry).unwrap();
        let deser: HistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.task_id, entry.task_id);
        assert_eq!(deser.name, entry.name);
        assert_eq!(deser.size, entry.size);
        assert_eq!(deser.downloaded, entry.downloaded);
        assert_eq!(deser.outcome, entry.outcome);
    }

    #[test]
    fn test_history_entry_serde_pretty() {
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let json = serde_json::to_string_pretty(&entry).unwrap();
        let deser: HistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.task_id, entry.task_id);
    }

    #[test]
    fn test_history_entry_serde_with_error() {
        let entry =
            HistoryEntry::from_task(&make_task(DownloadState::Error, Some("timeout".into())))
                .unwrap();
        let json = serde_json::to_string(&entry).unwrap();
        let deser: HistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.error, Some("timeout".into()));
    }

    #[test]
    fn test_history_entry_serde_extra_fields_ignored() {
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let mut json = serde_json::to_value(&entry).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("extra_field".into(), serde_json::json!("ignored"));
        let deser: HistoryEntry = serde_json::from_value(json).unwrap();
        assert_eq!(deser.task_id, entry.task_id);
    }

    #[test]
    fn test_history_entry_serde_missing_optional_error() {
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let mut json = serde_json::to_value(&entry).unwrap();
        json.as_object_mut().unwrap().remove("error");
        let deser: HistoryEntry = serde_json::from_value(json).unwrap();
        assert!(deser.error.is_none());
    }

    #[test]
    fn test_history_entry_serde_missing_tags_uses_default() {
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let mut json = serde_json::to_value(&entry).unwrap();
        json.as_object_mut().unwrap().remove("tags");
        let deser: HistoryEntry = serde_json::from_value(json).unwrap();
        assert!(deser.tags.is_empty());
    }

    #[test]
    fn test_history_protocol_serde_roundtrip() {
        let protocols = [
            HistoryProtocol::Torrent,
            HistoryProtocol::Ed2k,
            HistoryProtocol::Xunlei,
            HistoryProtocol::Magnet,
            HistoryProtocol::P2P,
        ];
        for p in &protocols {
            let json = serde_json::to_string(p).unwrap();
            let deser: HistoryProtocol = serde_json::from_str(&json).unwrap();
            assert_eq!(*p, deser);
        }
    }

    #[test]
    fn test_history_outcome_serde_roundtrip() {
        let outcomes = [HistoryOutcome::Completed, HistoryOutcome::Failed];
        for o in &outcomes {
            let json = serde_json::to_string(o).unwrap();
            let deser: HistoryOutcome = serde_json::from_str(&json).unwrap();
            assert_eq!(*o, deser);
        }
    }

    #[test]
    fn test_history_summary_serde_roundtrip() {
        let entries = vec![
            HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap(),
            HistoryEntry::from_task(&make_task(DownloadState::Error, Some("err".into()))).unwrap(),
        ];
        let summary = HistorySummary::from_entries(&entries);
        let json = serde_json::to_string(&summary).unwrap();
        let deser: HistorySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.total_entries, summary.total_entries);
        assert_eq!(deser.completed_count, summary.completed_count);
        assert_eq!(deser.failed_count, summary.failed_count);
    }

    #[test]
    fn test_history_summary_serde_extra_fields_ignored() {
        let summary = HistorySummary::from_entries(&[]);
        let mut json = serde_json::to_value(&summary).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("extra".into(), serde_json::json!(42));
        let deser: HistorySummary = serde_json::from_value(json).unwrap();
        assert_eq!(deser.total_entries, 0);
    }

    // ========== Clone/Debug traits ==========

    #[test]
    fn test_history_entry_clone_debug() {
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let cloned = entry.clone();
        assert_eq!(cloned.task_id, entry.task_id);
        let debug = format!("{:?}", entry);
        assert!(debug.contains("hist-001"));
        assert!(debug.contains("ubuntu.iso"));
    }

    #[test]
    fn test_history_entry_clone_independence() {
        let mut entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let cloned = entry.clone();
        entry.task_id = "changed".into();
        assert_eq!(cloned.task_id, "hist-001");
    }

    #[test]
    fn test_history_protocol_clone_copy_debug_eq() {
        let p = HistoryProtocol::Torrent;
        let p2 = p;
        assert_eq!(p, p2);
        let debug = format!("{:?}", p);
        assert_eq!(debug, "Torrent");
    }

    #[test]
    fn test_history_outcome_clone_copy_debug_eq() {
        let o = HistoryOutcome::Completed;
        let o2 = o;
        assert_eq!(o, o2);
        let debug = format!("{:?}", o);
        assert_eq!(debug, "Completed");
    }

    #[test]
    fn test_history_summary_clone_debug() {
        let summary = HistorySummary::from_entries(&[]);
        let cloned = summary.clone();
        assert_eq!(cloned.total_entries, 0);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("total_entries"));
    }

    #[test]
    fn test_protocol_count_clone_debug() {
        let pc = ProtocolCount {
            protocol: "Torrent".into(),
            count: 5,
        };
        let cloned = pc.clone();
        assert_eq!(cloned.protocol, "Torrent");
        assert_eq!(cloned.count, 5);
        let debug = format!("{:?}", pc);
        assert!(debug.contains("Torrent"));
    }

    // ========== HistoryError Display ==========

    #[test]
    fn test_history_error_display_io() {
        let e = HistoryError::Io("disk full".into());
        assert_eq!(format!("{}", e), "IO error: disk full");
    }

    #[test]
    fn test_history_error_display_serialize() {
        let e = HistoryError::Serialize("bad data".into());
        assert_eq!(format!("{}", e), "serialize error: bad data");
    }

    #[test]
    fn test_history_error_display_deserialize() {
        let e = HistoryError::Deserialize("unexpected token".into());
        assert_eq!(format!("{}", e), "deserialize error: unexpected token");
    }

    #[test]
    fn test_history_error_debug() {
        let e = HistoryError::Io("disk full".into());
        let debug = format!("{:?}", e);
        assert!(debug.contains("Io"));
        assert!(debug.contains("disk full"));
    }

    #[test]
    fn test_history_error_unicode_message() {
        let e = HistoryError::Io("磁盘已满".into());
        let msg = format!("{}", e);
        assert!(msg.contains("磁盘已满"));
    }

    // ========== format_protocol ==========

    #[test]
    fn test_format_protocol_all_variants() {
        assert_eq!(format_protocol(HistoryProtocol::Torrent), "Torrent");
        assert_eq!(format_protocol(HistoryProtocol::Ed2k), "Ed2k");
        assert_eq!(format_protocol(HistoryProtocol::Xunlei), "Xunlei");
        assert_eq!(format_protocol(HistoryProtocol::Magnet), "Magnet");
        assert_eq!(format_protocol(HistoryProtocol::P2P), "P2P");
    }

    // ========== From<DownloadProtocol> ==========

    #[test]
    fn test_from_download_protocol_all_variants() {
        assert_eq!(
            HistoryProtocol::from(DownloadProtocol::Torrent),
            HistoryProtocol::Torrent
        );
        assert_eq!(
            HistoryProtocol::from(DownloadProtocol::Ed2k),
            HistoryProtocol::Ed2k
        );
        assert_eq!(
            HistoryProtocol::from(DownloadProtocol::Xunlei),
            HistoryProtocol::Xunlei
        );
        assert_eq!(
            HistoryProtocol::from(DownloadProtocol::Magnet),
            HistoryProtocol::Magnet
        );
        assert_eq!(
            HistoryProtocol::from(DownloadProtocol::P2P),
            HistoryProtocol::P2P
        );
    }

    // ========== format_size boundaries ==========

    #[test]
    fn test_format_size_zero() {
        assert_eq!(format_size(0), "0 B");
    }

    #[test]
    fn test_format_size_exact_kb() {
        assert_eq!(format_size(1024), "1.0 KB");
    }

    #[test]
    fn test_format_size_below_kb() {
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn test_format_size_exact_mb() {
        assert_eq!(format_size(1048576), "1.0 MB");
    }

    #[test]
    fn test_format_size_exact_gb() {
        assert_eq!(format_size(1073741824), "1.0 GB");
    }

    #[test]
    fn test_format_size_exact_tb() {
        assert_eq!(format_size(1099511627776), "1.0 TB");
    }

    #[test]
    fn test_format_size_large_tb() {
        assert_eq!(format_size(5 * 1099511627776), "5.0 TB");
    }

    #[test]
    fn test_format_size_u64_max() {
        let result = format_size(u64::MAX);
        assert!(result.contains("TB"));
    }

    // ========== HistorySummary::from_entries ==========

    #[test]
    fn test_summary_from_entries_empty() {
        let summary = HistorySummary::from_entries(&[]);
        assert_eq!(summary.total_entries, 0);
        assert_eq!(summary.completed_count, 0);
        assert_eq!(summary.failed_count, 0);
        assert_eq!(summary.total_completed_bytes, 0);
        assert_eq!(summary.total_failed_bytes, 0);
        assert!(summary.by_protocol.is_empty());
    }

    #[test]
    fn test_summary_from_entries_all_completed() {
        let entries = vec![
            HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap(),
            HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap(),
        ];
        let summary = HistorySummary::from_entries(&entries);
        assert_eq!(summary.total_entries, 2);
        assert_eq!(summary.completed_count, 2);
        assert_eq!(summary.failed_count, 0);
        assert_eq!(summary.total_completed_bytes, 2 * 1024 * 1024 * 100);
        assert_eq!(summary.total_failed_bytes, 0);
    }

    #[test]
    fn test_summary_from_entries_all_failed() {
        let mut task = make_task(DownloadState::Error, Some("err".into()));
        task.downloaded = 500;
        let e = HistoryEntry::from_task(&task).unwrap();
        let summary = HistorySummary::from_entries(&[e]);
        assert_eq!(summary.failed_count, 1);
        assert_eq!(summary.completed_count, 0);
        assert_eq!(summary.total_completed_bytes, 0);
        assert_eq!(summary.total_failed_bytes, 500);
    }

    #[test]
    fn test_summary_from_entries_mixed() {
        let mut failed_task = make_task(DownloadState::Error, Some("err".into()));
        failed_task.downloaded = 200;
        let entries = vec![
            HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap(),
            HistoryEntry::from_task(&failed_task).unwrap(),
        ];
        let summary = HistorySummary::from_entries(&entries);
        assert_eq!(summary.total_entries, 2);
        assert_eq!(summary.completed_count, 1);
        assert_eq!(summary.failed_count, 1);
        assert_eq!(summary.total_completed_bytes, 1024 * 1024 * 100);
        assert_eq!(summary.total_failed_bytes, 200);
    }

    #[test]
    fn test_summary_by_protocol_counts() {
        let mut torrent_task = make_task(DownloadState::Complete, None);
        torrent_task.protocol = DownloadProtocol::Torrent;
        let mut ed2k_task = make_task(DownloadState::Complete, None);
        ed2k_task.protocol = DownloadProtocol::Ed2k;
        ed2k_task.id = "ed2k-1".into();
        let mut magnet_task = make_task(DownloadState::Complete, None);
        magnet_task.protocol = DownloadProtocol::Magnet;
        magnet_task.id = "magnet-1".into();

        let entries = vec![
            HistoryEntry::from_task(&torrent_task).unwrap(),
            HistoryEntry::from_task(&ed2k_task).unwrap(),
            HistoryEntry::from_task(&magnet_task).unwrap(),
        ];
        let summary = HistorySummary::from_entries(&entries);
        assert_eq!(summary.by_protocol.len(), 3);
        // All have count 1, so sorted by count desc (all equal)
        for pc in &summary.by_protocol {
            assert_eq!(pc.count, 1);
        }
    }

    #[test]
    fn test_summary_by_protocol_sorted_desc() {
        let mut entries = Vec::new();
        // 3 Torrent, 1 Ed2k
        for i in 0..3 {
            let mut t = make_task(DownloadState::Complete, None);
            t.id = format!("t-{}", i);
            t.protocol = DownloadProtocol::Torrent;
            entries.push(HistoryEntry::from_task(&t).unwrap());
        }
        let mut t = make_task(DownloadState::Complete, None);
        t.id = "ed2k-1".into();
        t.protocol = DownloadProtocol::Ed2k;
        entries.push(HistoryEntry::from_task(&t).unwrap());

        let summary = HistorySummary::from_entries(&entries);
        assert_eq!(summary.by_protocol[0].protocol, "Torrent");
        assert_eq!(summary.by_protocol[0].count, 3);
        assert_eq!(summary.by_protocol[1].protocol, "Ed2k");
        assert_eq!(summary.by_protocol[1].count, 1);
    }

    // ========== HistorySummary::format ==========

    #[test]
    fn test_summary_format_empty() {
        let summary = HistorySummary::from_entries(&[]);
        let report = summary.format();
        assert!(report.contains("Download History Summary"));
        assert!(report.contains("Total entries: 0"));
        assert!(report.contains("Completed: 0"));
        assert!(report.contains("Failed: 0"));
    }

    #[test]
    fn test_summary_format_with_data() {
        let entries =
            vec![HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap()];
        let summary = HistorySummary::from_entries(&entries);
        let report = summary.format();
        assert!(report.contains("✓ Completed: 1"));
        assert!(report.contains("100.0 MB"));
    }

    #[test]
    fn test_summary_format_shows_failed_bytes() {
        let mut task = make_task(DownloadState::Error, Some("err".into()));
        task.downloaded = 1024 * 1024 * 5;
        let entries = vec![HistoryEntry::from_task(&task).unwrap()];
        let summary = HistorySummary::from_entries(&entries);
        let report = summary.format();
        assert!(report.contains("Wasted (failed)"));
        assert!(report.contains("5.0 MB"));
    }

    #[test]
    fn test_summary_format_no_wasted_when_zero_failed_bytes() {
        let entries =
            vec![HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap()];
        let summary = HistorySummary::from_entries(&entries);
        let report = summary.format();
        assert!(!report.contains("Wasted"));
    }

    #[test]
    fn test_summary_format_includes_protocol_breakdown() {
        let entries =
            vec![HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap()];
        let summary = HistorySummary::from_entries(&entries);
        let report = summary.format();
        assert!(report.contains("By protocol:"));
        assert!(report.contains("Torrent: 1"));
    }

    // ========== HistoryEntry::summary ==========

    #[test]
    fn test_entry_summary_contains_all_fields() {
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let s = entry.summary();
        assert!(s.contains("✓"));
        assert!(s.contains("ubuntu.iso"));
        assert!(s.contains("Torrent"));
        assert!(s.contains("100.0 MB"));
        assert!(s.contains("hist-001"));
    }

    #[test]
    fn test_entry_summary_failed_format() {
        let entry =
            HistoryEntry::from_task(&make_task(DownloadState::Error, Some("net err".into())))
                .unwrap();
        let s = entry.summary();
        assert!(s.contains("✗"));
        assert!(s.contains("ubuntu.iso"));
    }

    // ========== Persistence edge cases ==========

    #[test]
    fn test_save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        save_history(&[], dir.path()).unwrap();
        assert!(dir.path().join("download_history.json").exists());
    }

    #[test]
    fn test_save_no_tmp_leftover() {
        let dir = tempfile::tempdir().unwrap();
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        save_history(&[entry], dir.path()).unwrap();
        assert!(!dir.path().join("download_history.json.tmp").exists());
    }

    #[test]
    fn test_save_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let e1 = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        save_history(&[e1], dir.path()).unwrap();

        let e2 =
            HistoryEntry::from_task(&make_task(DownloadState::Error, Some("err".into()))).unwrap();
        save_history(&[e2], dir.path()).unwrap();

        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].outcome, HistoryOutcome::Failed);
    }

    #[test]
    fn test_save_pretty_json() {
        let dir = tempfile::tempdir().unwrap();
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        save_history(&[entry], dir.path()).unwrap();
        let content = std::fs::read_to_string(dir.path().join("download_history.json")).unwrap();
        assert!(content.contains("\n"));
    }

    #[test]
    fn test_load_nonexistent_dir() {
        let result = load_history(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_empty_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("download_history.json");
        std::fs::write(&path, "").unwrap();
        let result = load_history(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_unicode_task_id_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let mut entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        entry.task_id = "任务-中文-🎉".into();
        save_history(&[entry], dir.path()).unwrap();
        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded[0].task_id, "任务-中文-🎉");
    }

    #[test]
    fn test_unicode_name_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let mut entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        entry.name = "日本語ファイル名🎵".into();
        save_history(&[entry], dir.path()).unwrap();
        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded[0].name, "日本語ファイル名🎵");
    }

    #[test]
    fn test_unicode_tags_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let mut entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        entry.tags = vec!["中文标签".into(), "タグ".into(), "🏷️".into()];
        save_history(&[entry], dir.path()).unwrap();
        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded[0].tags, vec!["中文标签", "タグ", "🏷️"]);
    }

    // ========== Boundary conditions ==========

    #[test]
    fn test_zero_size_entry() {
        let mut task = make_task(DownloadState::Complete, None);
        task.size = 0;
        task.downloaded = 0;
        let entry = HistoryEntry::from_task(&task).unwrap();
        assert_eq!(entry.size, 0);
        assert_eq!(entry.downloaded, 0);
        let s = entry.summary();
        assert!(s.contains("0 B"));
    }

    #[test]
    fn test_u64_max_size() {
        let mut task = make_task(DownloadState::Complete, None);
        task.size = u64::MAX;
        task.downloaded = u64::MAX;
        let entry = HistoryEntry::from_task(&task).unwrap();
        assert_eq!(entry.size, u64::MAX);
        let s = entry.summary();
        assert!(s.contains("TB"));
    }

    #[test]
    fn test_empty_task_id() {
        let mut task = make_task(DownloadState::Complete, None);
        task.id = String::new();
        let entry = HistoryEntry::from_task(&task).unwrap();
        assert_eq!(entry.task_id, "");
    }

    #[test]
    fn test_emoji_task_id() {
        let mut task = make_task(DownloadState::Complete, None);
        task.id = "🎉🎊".into();
        let entry = HistoryEntry::from_task(&task).unwrap();
        assert_eq!(entry.task_id, "🎉🎊");
    }

    #[test]
    fn test_empty_tags() {
        let mut task = make_task(DownloadState::Complete, None);
        task.tags = vec![];
        let entry = HistoryEntry::from_task(&task).unwrap();
        assert!(entry.tags.is_empty());
    }

    #[test]
    fn test_many_tags() {
        let mut task = make_task(DownloadState::Complete, None);
        task.tags = (0..50).map(|i| format!("tag-{}", i)).collect();
        let entry = HistoryEntry::from_task(&task).unwrap();
        assert_eq!(entry.tags.len(), 50);
    }

    // ========== remove_entry edge cases ==========

    #[test]
    fn test_remove_entry_preserves_others() {
        let dir = tempfile::tempdir().unwrap();
        let e1 = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        let mut e2 = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        e2.task_id = "hist-002".into();
        append_entry(dir.path(), e1).unwrap();
        append_entry(dir.path(), e2).unwrap();

        remove_entry(dir.path(), "hist-001").unwrap();
        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].task_id, "hist-002");
    }

    #[test]
    fn test_remove_entry_empty_history() {
        let dir = tempfile::tempdir().unwrap();
        let found = remove_entry(dir.path(), "nonexistent").unwrap();
        assert!(!found);
    }

    #[test]
    fn test_remove_entry_unicode_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        entry.task_id = "中文任务".into();
        append_entry(dir.path(), entry).unwrap();

        let found = remove_entry(dir.path(), "中文任务").unwrap();
        assert!(found);
        let loaded = load_history(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    // ========== clear_history edge cases ==========

    #[test]
    fn test_clear_history_already_empty() {
        let dir = tempfile::tempdir().unwrap();
        clear_history(dir.path()).unwrap();
        let loaded = load_history(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_clear_history_then_append() {
        let dir = tempfile::tempdir().unwrap();
        let entry = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        append_entry(dir.path(), entry).unwrap();
        clear_history(dir.path()).unwrap();

        let mut e2 = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        e2.task_id = "new-task".into();
        append_entry(dir.path(), e2).unwrap();

        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].task_id, "new-task");
    }

    // ========== MAX_HISTORY_ENTRIES constant ==========

    #[test]
    fn test_max_history_entries_constant() {
        assert_eq!(MAX_HISTORY_ENTRIES, 1000);
    }

    #[test]
    fn test_exactly_max_entries_no_eviction() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..MAX_HISTORY_ENTRIES {
            let mut entry =
                HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
            entry.task_id = format!("task-{}", i);
            append_entry(dir.path(), entry).unwrap();
        }
        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), MAX_HISTORY_ENTRIES);
        assert_eq!(loaded[0].task_id, "task-0");
    }

    // ========== Full workflow ==========

    #[test]
    fn test_full_lifecycle() {
        let dir = tempfile::tempdir().unwrap();

        // Start empty
        let loaded = load_history(dir.path()).unwrap();
        assert!(loaded.is_empty());

        // Add completed download
        let e1 = HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap();
        append_entry(dir.path(), e1).unwrap();

        // Add failed download
        let mut e2 =
            HistoryEntry::from_task(&make_task(DownloadState::Error, Some("timeout".into())))
                .unwrap();
        e2.task_id = "hist-002".into();
        append_entry(dir.path(), e2).unwrap();

        // Verify both present
        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);

        // Check summary
        let summary = HistorySummary::from_entries(&loaded);
        assert_eq!(summary.total_entries, 2);
        assert_eq!(summary.completed_count, 1);
        assert_eq!(summary.failed_count, 1);

        // Remove one
        remove_entry(dir.path(), "hist-001").unwrap();
        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].task_id, "hist-002");

        // Clear all
        clear_history(dir.path()).unwrap();
        let loaded = load_history(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_multiple_protocols_in_history() {
        let dir = tempfile::tempdir().unwrap();
        let protocols = [
            DownloadProtocol::Torrent,
            DownloadProtocol::Ed2k,
            DownloadProtocol::Xunlei,
            DownloadProtocol::Magnet,
            DownloadProtocol::P2P,
        ];
        for (i, p) in protocols.iter().enumerate() {
            let mut task = make_task(DownloadState::Complete, None);
            task.id = format!("proto-{}", i);
            task.protocol = *p;
            let entry = HistoryEntry::from_task(&task).unwrap();
            append_entry(dir.path(), entry).unwrap();
        }
        let loaded = load_history(dir.path()).unwrap();
        assert_eq!(loaded.len(), 5);
        let summary = HistorySummary::from_entries(&loaded);
        assert_eq!(summary.by_protocol.len(), 5);
    }

    // ========== serde vector roundtrip ==========

    #[test]
    fn test_vec_history_entry_serde_roundtrip() {
        let entries = vec![
            HistoryEntry::from_task(&make_task(DownloadState::Complete, None)).unwrap(),
            HistoryEntry::from_task(&make_task(DownloadState::Error, Some("err".into()))).unwrap(),
        ];
        let json = serde_json::to_string(&entries).unwrap();
        let deser: Vec<HistoryEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.len(), 2);
    }

    #[test]
    fn test_empty_vec_serde_roundtrip() {
        let entries: Vec<HistoryEntry> = vec![];
        let json = serde_json::to_string(&entries).unwrap();
        assert_eq!(json, "[]");
        let deser: Vec<HistoryEntry> = serde_json::from_str(&json).unwrap();
        assert!(deser.is_empty());
    }
}
