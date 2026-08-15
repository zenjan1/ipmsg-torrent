//! Task archive module - archive completed/failed tasks instead of deleting them
//!
//! Provides the ability to archive tasks for later review instead of permanently
//! deleting them. Archived tasks preserve all metadata and can be restored if needed.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

use crate::DownloadState;

/// Error type for task archive operations
#[derive(Debug, thiserror::Error)]
pub enum TaskArchiveError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Task not found: {0}")]
    TaskNotFound(String),
    #[error("Archive is full (max {max} tasks)")]
    ArchiveFull { max: usize },
}

/// A task that has been archived (preserved for later review)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedTask {
    /// Original task ID
    pub id: String,
    /// Task name
    pub name: String,
    /// Download protocol used
    pub protocol: String,
    /// Total file size in bytes
    pub size: u64,
    /// Bytes downloaded
    pub downloaded: u64,
    /// Final state when archived (Complete, Failed, etc.)
    pub final_state: String,
    /// Error message if failed
    pub error: Option<String>,
    /// Save path where file was stored
    pub save_path: std::path::PathBuf,
    /// When the task was originally created
    pub created_at: DateTime<Utc>,
    /// When the task was originally last updated
    pub updated_at: DateTime<Utc>,
    /// User-defined tags
    pub tags: Vec<String>,
    /// User-defined group
    pub group: Option<String>,
    /// User-defined notes
    pub notes: Option<String>,
    /// Original source URL
    pub source_url: Option<String>,
    /// Total active download time in seconds
    pub active_time_seconds: f64,
    /// When this task was archived
    pub archived_at: DateTime<Utc>,
    /// User-provided reason for archiving (optional)
    pub archive_reason: Option<String>,
}

/// Configuration for task archive
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    /// Whether archiving is enabled
    pub enabled: bool,
    /// Maximum number of archived tasks to keep
    pub max_archived: usize,
    /// Auto-archive completed tasks after this many seconds (None = manual only)
    pub auto_archive_completed_after_secs: Option<u64>,
    /// Auto-archive failed tasks after this many seconds (None = manual only)
    pub auto_archive_failed_after_secs: Option<u64>,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: None,
            auto_archive_failed_after_secs: None,
        }
    }
}

/// State for archive persistence
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArchiveState {
    /// Archive configuration
    pub config: ArchiveConfig,
    /// Archived tasks (newest first)
    pub archived: Vec<ArchivedTask>,
}

/// Save archive state to disk (atomic write)
pub async fn save_archive_state(path: &Path, state: &ArchiveState) -> Result<(), TaskArchiveError> {
    let json = serde_json::to_string_pretty(state)?;
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, json.as_bytes()).await?;
    fs::rename(&temp_path, path).await?;
    Ok(())
}

/// Load archive state from disk
pub async fn load_archive_state(path: &Path) -> Result<ArchiveState, TaskArchiveError> {
    if !path.exists() {
        return Ok(ArchiveState::default());
    }
    let content = fs::read_to_string(path).await?;
    if content.trim().is_empty() {
        return Ok(ArchiveState::default());
    }
    let state: ArchiveState = serde_json::from_str(&content)?;
    Ok(state)
}

/// Create an ArchivedTask from task data
pub fn create_archived_task(
    id: String,
    name: String,
    protocol: &str,
    size: u64,
    downloaded: u64,
    final_state: &str,
    error: Option<String>,
    save_path: std::path::PathBuf,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    tags: Vec<String>,
    group: Option<String>,
    notes: Option<String>,
    source_url: Option<String>,
    active_time_seconds: f64,
    archive_reason: Option<String>,
) -> ArchivedTask {
    ArchivedTask {
        id,
        name,
        protocol: protocol.to_string(),
        size,
        downloaded,
        final_state: final_state.to_string(),
        error,
        save_path,
        created_at,
        updated_at,
        tags,
        group,
        notes,
        source_url,
        active_time_seconds,
        archived_at: Utc::now(),
        archive_reason,
    }
}

/// Check if a task should be auto-archived based on config
pub fn should_auto_archive(
    state: &DownloadState,
    updated_at: DateTime<Utc>,
    config: &ArchiveConfig,
    now: DateTime<Utc>,
) -> bool {
    if !config.enabled {
        return false;
    }

    match state {
        DownloadState::Complete => {
            if let Some(secs) = config.auto_archive_completed_after_secs {
                let elapsed = (now - updated_at).num_seconds() as u64;
                elapsed >= secs
            } else {
                false
            }
        }
        DownloadState::Error => {
            if let Some(secs) = config.auto_archive_failed_after_secs {
                let elapsed = (now - updated_at).num_seconds() as u64;
                elapsed >= secs
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Summary of archive contents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveSummary {
    /// Total number of archived tasks
    pub total_archived: usize,
    /// Number of completed tasks in archive
    pub completed_count: usize,
    /// Number of failed tasks in archive
    pub failed_count: usize,
    /// Number of other tasks in archive
    pub other_count: usize,
    /// Total bytes of archived downloads
    pub total_bytes: u64,
    /// Oldest archive date
    pub oldest_archive: Option<DateTime<Utc>>,
    /// Newest archive date
    pub newest_archive: Option<DateTime<Utc>>,
}

impl ArchiveState {
    /// Generate a summary of the archive
    pub fn summary(&self) -> ArchiveSummary {
        let mut completed = 0;
        let mut failed = 0;
        let mut other = 0;
        let mut total_bytes = 0u64;
        let mut oldest: Option<DateTime<Utc>> = None;
        let mut newest: Option<DateTime<Utc>> = None;

        for task in &self.archived {
            match task.final_state.as_str() {
                "Complete" => completed += 1,
                "Error" => failed += 1,
                _ => other += 1,
            }
            total_bytes += task.downloaded;

            if oldest.is_none() || task.archived_at < oldest.unwrap() {
                oldest = Some(task.archived_at);
            }
            if newest.is_none() || task.archived_at > newest.unwrap() {
                newest = Some(task.archived_at);
            }
        }

        ArchiveSummary {
            total_archived: self.archived.len(),
            completed_count: completed,
            failed_count: failed,
            other_count: other,
            total_bytes,
            oldest_archive: oldest,
            newest_archive: newest,
        }
    }

    /// Add a task to the archive, enforcing max limit
    pub fn archive_task(&mut self, task: ArchivedTask) -> Result<(), TaskArchiveError> {
        if self.archived.len() >= self.config.max_archived {
            return Err(TaskArchiveError::ArchiveFull {
                max: self.config.max_archived,
            });
        }
        // Insert at beginning (newest first)
        self.archived.insert(0, task);
        Ok(())
    }

    /// Remove a task from archive by ID
    pub fn unarchive_task(&mut self, id: &str) -> Option<ArchivedTask> {
        if let Some(pos) = self.archived.iter().position(|t| t.id == id) {
            Some(self.archived.remove(pos))
        } else {
            None
        }
    }

    /// Clear all archived tasks
    pub fn clear_archive(&mut self) {
        self.archived.clear();
    }

    /// Find an archived task by ID
    pub fn find_archived(&self, id: &str) -> Option<&ArchivedTask> {
        self.archived.iter().find(|t| t.id == id)
    }

    /// List archived tasks with optional filtering
    pub fn list_archived(
        &self,
        state_filter: Option<&str>,
        protocol_filter: Option<&str>,
        tag_filter: Option<&str>,
    ) -> Vec<&ArchivedTask> {
        self.archived
            .iter()
            .filter(|t| {
                if let Some(state) = state_filter
                    && t.final_state != state
                {
                    return false;
                }
                if let Some(protocol) = protocol_filter
                    && t.protocol != protocol
                {
                    return false;
                }
                if let Some(tag) = tag_filter
                    && !t.tags.contains(&tag.to_string())
                {
                    return false;
                }
                true
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_test_task(id: &str, state: &str) -> ArchivedTask {
        create_archived_task(
            id.to_string(),
            format!("Task {}", id),
            "Xunlei",
            1000,
            1000,
            state,
            None,
            PathBuf::from("/tmp/test"),
            Utc::now(),
            Utc::now(),
            vec!["test".to_string()],
            None,
            None,
            Some("http://example.com/file".to_string()),
            10.0,
            None,
        )
    }

    #[test]
    fn test_archive_config_default() {
        let config = ArchiveConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_archived, 500);
        assert!(config.auto_archive_completed_after_secs.is_none());
        assert!(config.auto_archive_failed_after_secs.is_none());
    }

    #[test]
    fn test_archive_state_default() {
        let state = ArchiveState::default();
        assert!(state.archived.is_empty());
        assert!(state.config.enabled);
    }

    #[test]
    fn test_archive_task() {
        let mut state = ArchiveState::default();
        let task = make_test_task("task1", "Complete");
        state.archive_task(task).unwrap();
        assert_eq!(state.archived.len(), 1);
        assert_eq!(state.archived[0].id, "task1");
    }

    #[test]
    fn test_archive_task_newest_first() {
        let mut state = ArchiveState::default();
        let task1 = make_test_task("task1", "Complete");
        let task2 = make_test_task("task2", "Complete");
        state.archive_task(task1).unwrap();
        state.archive_task(task2).unwrap();
        assert_eq!(state.archived[0].id, "task2");
        assert_eq!(state.archived[1].id, "task1");
    }

    #[test]
    fn test_archive_full() {
        let mut state = ArchiveState::default();
        state.config.max_archived = 2;
        state
            .archive_task(make_test_task("task1", "Complete"))
            .unwrap();
        state
            .archive_task(make_test_task("task2", "Complete"))
            .unwrap();
        let result = state.archive_task(make_test_task("task3", "Complete"));
        assert!(matches!(
            result,
            Err(TaskArchiveError::ArchiveFull { max: 2 })
        ));
    }

    #[test]
    fn test_unarchive_task() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("task1", "Complete"))
            .unwrap();
        state
            .archive_task(make_test_task("task2", "Complete"))
            .unwrap();
        let removed = state.unarchive_task("task1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, "task1");
        assert_eq!(state.archived.len(), 1);
    }

    #[test]
    fn test_unarchive_nonexistent() {
        let mut state = ArchiveState::default();
        let removed = state.unarchive_task("nonexistent");
        assert!(removed.is_none());
    }

    #[test]
    fn test_clear_archive() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("task1", "Complete"))
            .unwrap();
        state
            .archive_task(make_test_task("task2", "Complete"))
            .unwrap();
        state.clear_archive();
        assert!(state.archived.is_empty());
    }

    #[test]
    fn test_find_archived() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("task1", "Complete"))
            .unwrap();
        assert!(state.find_archived("task1").is_some());
        assert!(state.find_archived("nonexistent").is_none());
    }

    #[test]
    fn test_list_archived_no_filter() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("task1", "Complete"))
            .unwrap();
        state
            .archive_task(make_test_task("task2", "Error"))
            .unwrap();
        let all = state.list_archived(None, None, None);
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_list_archived_by_state() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("task1", "Complete"))
            .unwrap();
        state
            .archive_task(make_test_task("task2", "Error"))
            .unwrap();
        let completed = state.list_archived(Some("Complete"), None, None);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].id, "task1");
    }

    #[test]
    fn test_list_archived_by_tag() {
        let mut state = ArchiveState::default();
        let mut task1 = make_test_task("task1", "Complete");
        task1.tags = vec!["movies".to_string()];
        let mut task2 = make_test_task("task2", "Complete");
        task2.tags = vec!["music".to_string()];
        state.archive_task(task1).unwrap();
        state.archive_task(task2).unwrap();
        let movies = state.list_archived(None, None, Some("movies"));
        assert_eq!(movies.len(), 1);
        assert_eq!(movies[0].id, "task1");
    }

    #[test]
    fn test_archive_summary() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("task1", "Complete"))
            .unwrap();
        state
            .archive_task(make_test_task("task2", "Error"))
            .unwrap();
        state
            .archive_task(make_test_task("task3", "Paused"))
            .unwrap();
        let summary = state.summary();
        assert_eq!(summary.total_archived, 3);
        assert_eq!(summary.completed_count, 1);
        assert_eq!(summary.failed_count, 1);
        assert_eq!(summary.other_count, 1);
        assert!(summary.oldest_archive.is_some());
        assert!(summary.newest_archive.is_some());
    }

    #[test]
    fn test_should_auto_archive_completed() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: Some(3600),
            auto_archive_failed_after_secs: None,
        };
        let now = Utc::now();
        let old_time = now - chrono::Duration::seconds(7200);
        let recent_time = now - chrono::Duration::seconds(1800);

        assert!(should_auto_archive(
            &DownloadState::Complete,
            old_time,
            &config,
            now
        ));
        assert!(!should_auto_archive(
            &DownloadState::Complete,
            recent_time,
            &config,
            now
        ));
    }

    #[test]
    fn test_should_auto_archive_failed() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: None,
            auto_archive_failed_after_secs: Some(86400),
        };
        let now = Utc::now();
        let old_time = now - chrono::Duration::seconds(100000);

        assert!(should_auto_archive(
            &DownloadState::Error,
            old_time,
            &config,
            now
        ));
        assert!(!should_auto_archive(
            &DownloadState::Complete,
            old_time,
            &config,
            now
        ));
    }

    #[test]
    fn test_should_auto_archive_disabled() {
        let config = ArchiveConfig {
            enabled: false,
            max_archived: 500,
            auto_archive_completed_after_secs: Some(0),
            auto_archive_failed_after_secs: None,
        };
        let now = Utc::now();
        let old_time = now - chrono::Duration::seconds(100000);

        assert!(!should_auto_archive(
            &DownloadState::Complete,
            old_time,
            &config,
            now
        ));
    }

    #[tokio::test]
    async fn test_save_and_load_archive_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");

        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("task1", "Complete"))
            .unwrap();
        state.config.max_archived = 100;

        save_archive_state(&path, &state).await.unwrap();

        let loaded = load_archive_state(&path).await.unwrap();
        assert_eq!(loaded.archived.len(), 1);
        assert_eq!(loaded.archived[0].id, "task1");
        assert_eq!(loaded.config.max_archived, 100);
    }

    #[tokio::test]
    async fn test_load_archive_state_missing() {
        let path = PathBuf::from("/nonexistent/path/archive.json");
        let state = load_archive_state(&path).await.unwrap();
        assert!(state.archived.is_empty());
    }

    #[tokio::test]
    async fn test_load_archive_state_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        fs::write(&path, "").await.unwrap();

        let state = load_archive_state(&path).await.unwrap();
        assert!(state.archived.is_empty());
    }

    #[test]
    fn test_archived_task_serialization() {
        let task = create_archived_task(
            "test-id".to_string(),
            "Test Task".to_string(),
            "Xunlei",
            1000,
            500,
            "Complete",
            None,
            PathBuf::from("/tmp/test"),
            Utc::now(),
            Utc::now(),
            vec!["tag1".to_string()],
            Some("group1".to_string()),
            Some("notes".to_string()),
            Some("http://example.com".to_string()),
            30.0,
            Some("manual".to_string()),
        );
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: ArchivedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "test-id");
        assert_eq!(deserialized.name, "Test Task");
        assert_eq!(deserialized.archive_reason, Some("manual".to_string()));
    }

    // === Comprehensive Test Coverage (Phase 235) ===

    // --- Serialization roundtrips ---

    #[test]
    fn test_archived_task_serde_roundtrip() {
        let now = Utc::now();
        let task = create_archived_task(
            "id-1".to_string(),
            "My Task".to_string(),
            "HTTP",
            2048,
            1024,
            "Complete",
            Some("timeout".to_string()),
            PathBuf::from("/home/user/downloads"),
            now,
            now,
            vec!["tag1".to_string(), "tag2".to_string()],
            Some("group-a".to_string()),
            Some("a note".to_string()),
            Some("http://example.com/file.zip".to_string()),
            120.5,
            Some("user request".to_string()),
        );
        let json = serde_json::to_string(&task).unwrap();
        let back: ArchivedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "id-1");
        assert_eq!(back.name, "My Task");
        assert_eq!(back.protocol, "HTTP");
        assert_eq!(back.size, 2048);
        assert_eq!(back.downloaded, 1024);
        assert_eq!(back.final_state, "Complete");
        assert_eq!(back.error, Some("timeout".to_string()));
        assert_eq!(back.save_path, PathBuf::from("/home/user/downloads"));
        assert_eq!(back.tags, vec!["tag1", "tag2"]);
        assert_eq!(back.group, Some("group-a".to_string()));
        assert_eq!(back.notes, Some("a note".to_string()));
        assert_eq!(
            back.source_url,
            Some("http://example.com/file.zip".to_string())
        );
        assert!((back.active_time_seconds - 120.5).abs() < f64::EPSILON);
        assert_eq!(back.archive_reason, Some("user request".to_string()));
    }

    #[test]
    fn test_archived_task_serde_extra_fields_ignored() {
        let mut task = make_test_task("t1", "Complete");
        task.error = None;
        task.group = None;
        task.notes = None;
        task.source_url = None;
        task.archive_reason = None;
        let json = serde_json::to_string(&task).unwrap();
        // Add extra field
        let mut obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        obj["extra_field"] = serde_json::json!("ignored");
        let back: ArchivedTask = serde_json::from_value(obj).unwrap();
        assert_eq!(back.id, "t1");
    }

    #[test]
    fn test_archive_config_serde_roundtrip() {
        let config = ArchiveConfig {
            enabled: false,
            max_archived: 100,
            auto_archive_completed_after_secs: Some(3600),
            auto_archive_failed_after_secs: Some(7200),
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: ArchiveConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.enabled, false);
        assert_eq!(back.max_archived, 100);
        assert_eq!(back.auto_archive_completed_after_secs, Some(3600));
        assert_eq!(back.auto_archive_failed_after_secs, Some(7200));
    }

    #[test]
    fn test_archive_config_serde_extra_fields_ignored() {
        let config = ArchiveConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let mut obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        obj["unknown_key"] = serde_json::json!(42);
        let back: ArchiveConfig = serde_json::from_value(obj).unwrap();
        assert_eq!(back.enabled, true);
        assert_eq!(back.max_archived, 500);
    }

    #[test]
    fn test_archive_state_serde_roundtrip() {
        let mut state = ArchiveState::default();
        state.config.max_archived = 200;
        state.archive_task(make_test_task("a", "Complete")).unwrap();
        state.archive_task(make_test_task("b", "Error")).unwrap();
        let json = serde_json::to_string(&state).unwrap();
        let back: ArchiveState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.archived.len(), 2);
        assert_eq!(back.config.max_archived, 200);
    }

    #[test]
    fn test_archive_state_serde_extra_fields_ignored() {
        let state = ArchiveState::default();
        let json = serde_json::to_string(&state).unwrap();
        let mut obj: serde_json::Value = serde_json::from_str(&json).unwrap();
        obj["extra"] = serde_json::json!(true);
        let back: ArchiveState = serde_json::from_value(obj).unwrap();
        assert!(back.archived.is_empty());
    }

    #[test]
    fn test_archive_summary_serde_roundtrip() {
        let summary = ArchiveSummary {
            total_archived: 10,
            completed_count: 5,
            failed_count: 3,
            other_count: 2,
            total_bytes: 1_000_000,
            oldest_archive: Some(Utc::now()),
            newest_archive: Some(Utc::now()),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: ArchiveSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_archived, 10);
        assert_eq!(back.completed_count, 5);
        assert_eq!(back.failed_count, 3);
        assert_eq!(back.other_count, 2);
        assert_eq!(back.total_bytes, 1_000_000);
    }

    #[test]
    fn test_archive_summary_serde_none_dates() {
        let summary = ArchiveSummary {
            total_archived: 0,
            completed_count: 0,
            failed_count: 0,
            other_count: 0,
            total_bytes: 0,
            oldest_archive: None,
            newest_archive: None,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: ArchiveSummary = serde_json::from_str(&json).unwrap();
        assert!(back.oldest_archive.is_none());
        assert!(back.newest_archive.is_none());
    }

    // --- Clone/Debug traits ---

    #[test]
    fn test_archived_task_clone() {
        let task = make_test_task("clone-me", "Complete");
        let cloned = task.clone();
        assert_eq!(cloned.id, task.id);
        assert_eq!(cloned.name, task.name);
        assert_eq!(cloned.protocol, task.protocol);
    }

    #[test]
    fn test_archived_task_clone_independence() {
        let mut task = make_test_task("orig", "Complete");
        let mut cloned = task.clone();
        cloned.name = "modified".to_string();
        assert_ne!(task.name, cloned.name);
    }

    #[test]
    fn test_archive_config_clone() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 42,
            auto_archive_completed_after_secs: Some(100),
            auto_archive_failed_after_secs: Some(200),
        };
        let cloned = config.clone();
        assert_eq!(cloned.enabled, true);
        assert_eq!(cloned.max_archived, 42);
        assert_eq!(cloned.auto_archive_completed_after_secs, Some(100));
        assert_eq!(cloned.auto_archive_failed_after_secs, Some(200));
    }

    #[test]
    fn test_archive_state_clone() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("s1", "Complete"))
            .unwrap();
        let cloned = state.clone();
        assert_eq!(cloned.archived.len(), 1);
        assert_eq!(cloned.archived[0].id, "s1");
    }

    #[test]
    fn test_archive_summary_clone() {
        let summary = ArchiveSummary {
            total_archived: 5,
            completed_count: 3,
            failed_count: 1,
            other_count: 1,
            total_bytes: 999,
            oldest_archive: None,
            newest_archive: None,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_archived, 5);
        assert_eq!(cloned.total_bytes, 999);
    }

    #[test]
    fn test_archived_task_debug() {
        let task = make_test_task("dbg", "Complete");
        let debug_str = format!("{:?}", task);
        assert!(debug_str.contains("dbg"));
        assert!(debug_str.contains("Complete"));
    }

    #[test]
    fn test_archive_config_debug() {
        let config = ArchiveConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("enabled"));
        assert!(debug_str.contains("max_archived"));
    }

    #[test]
    fn test_archive_state_debug() {
        let state = ArchiveState::default();
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("config"));
        assert!(debug_str.contains("archived"));
    }

    #[test]
    fn test_archive_summary_debug() {
        let summary = ArchiveSummary {
            total_archived: 0,
            completed_count: 0,
            failed_count: 0,
            other_count: 0,
            total_bytes: 0,
            oldest_archive: None,
            newest_archive: None,
        };
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("total_archived"));
    }

    // --- Error Display ---

    #[test]
    fn test_task_archive_error_io_display() {
        let err = TaskArchiveError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("file not found"));
    }

    #[test]
    fn test_task_archive_error_json_display() {
        let bad_json = "{invalid json";
        let serde_err = serde_json::from_str::<ArchiveState>(bad_json).unwrap_err();
        let err = TaskArchiveError::Json(serde_err);
        let msg = format!("{}", err);
        assert!(msg.contains("JSON"));
    }

    #[test]
    fn test_task_archive_error_task_not_found_display() {
        let err = TaskArchiveError::TaskNotFound("missing-id".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("missing-id"));
    }

    #[test]
    fn test_task_archive_error_archive_full_display() {
        let err = TaskArchiveError::ArchiveFull { max: 100 };
        let msg = format!("{}", err);
        assert!(msg.contains("100"));
    }

    #[test]
    fn test_task_archive_error_debug() {
        let err = TaskArchiveError::ArchiveFull { max: 5 };
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("ArchiveFull"));
    }

    #[test]
    fn test_task_archive_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "no access");
        let err: TaskArchiveError = TaskArchiveError::from(io_err);
        let msg = format!("{}", err);
        assert!(msg.contains("no access"));
    }

    #[test]
    fn test_task_archive_error_from_serde() {
        let serde_err = serde_json::from_str::<ArchiveState>("not json").unwrap_err();
        let err: TaskArchiveError = TaskArchiveError::from(serde_err);
        let msg = format!("{}", err);
        assert!(msg.contains("JSON"));
    }

    // --- create_archived_task ---

    #[test]
    fn test_create_archived_task_all_fields() {
        let now = Utc::now();
        let later = now + chrono::Duration::seconds(60);
        let task = create_archived_task(
            "full-id".to_string(),
            "Full Task".to_string(),
            "FTP",
            9999,
            5555,
            "Error",
            Some("connection reset".to_string()),
            PathBuf::from("/data/files"),
            now,
            later,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            Some("vip".to_string()),
            Some("important note".to_string()),
            Some("ftp://server.com/bigfile.iso".to_string()),
            300.0,
            Some("auto-archive".to_string()),
        );
        assert_eq!(task.id, "full-id");
        assert_eq!(task.name, "Full Task");
        assert_eq!(task.protocol, "FTP");
        assert_eq!(task.size, 9999);
        assert_eq!(task.downloaded, 5555);
        assert_eq!(task.final_state, "Error");
        assert_eq!(task.error, Some("connection reset".to_string()));
        assert_eq!(task.save_path, PathBuf::from("/data/files"));
        assert_eq!(task.tags.len(), 3);
        assert_eq!(task.group, Some("vip".to_string()));
        assert_eq!(task.notes, Some("important note".to_string()));
        assert_eq!(
            task.source_url,
            Some("ftp://server.com/bigfile.iso".to_string())
        );
        assert!((task.active_time_seconds - 300.0).abs() < f64::EPSILON);
        assert_eq!(task.archive_reason, Some("auto-archive".to_string()));
        assert!(task.archived_at >= now);
    }

    #[test]
    fn test_create_archived_task_minimal_fields() {
        let now = Utc::now();
        let task = create_archived_task(
            "min-id".to_string(),
            "Min".to_string(),
            "HTTP",
            0,
            0,
            "Complete",
            None,
            PathBuf::from("/tmp"),
            now,
            now,
            vec![],
            None,
            None,
            None,
            0.0,
            None,
        );
        assert_eq!(task.id, "min-id");
        assert!(task.error.is_none());
        assert!(task.group.is_none());
        assert!(task.notes.is_none());
        assert!(task.source_url.is_none());
        assert!(task.archive_reason.is_none());
        assert!(task.tags.is_empty());
        assert_eq!(task.size, 0);
        assert_eq!(task.downloaded, 0);
    }

    // --- should_auto_archive boundary conditions ---

    #[test]
    fn test_should_auto_archive_completed_zero_secs() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: Some(0),
            auto_archive_failed_after_secs: None,
        };
        let now = Utc::now();
        // Zero seconds means immediately archive
        assert!(should_auto_archive(
            &DownloadState::Complete,
            now,
            &config,
            now
        ));
    }

    #[test]
    fn test_should_auto_archive_failed_zero_secs() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: None,
            auto_archive_failed_after_secs: Some(0),
        };
        let now = Utc::now();
        assert!(should_auto_archive(
            &DownloadState::Error,
            now,
            &config,
            now
        ));
    }

    #[test]
    fn test_should_auto_archive_not_for_paused() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: Some(0),
            auto_archive_failed_after_secs: Some(0),
        };
        let now = Utc::now();
        let old_time = now - chrono::Duration::seconds(999999);
        assert!(!should_auto_archive(
            &DownloadState::Paused,
            old_time,
            &config,
            now
        ));
    }

    #[test]
    fn test_should_auto_archive_not_for_downloading() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: Some(0),
            auto_archive_failed_after_secs: Some(0),
        };
        let now = Utc::now();
        let old_time = now - chrono::Duration::seconds(999999);
        assert!(!should_auto_archive(
            &DownloadState::Downloading,
            old_time,
            &config,
            now
        ));
    }

    #[test]
    fn test_should_auto_archive_not_for_queued() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: Some(0),
            auto_archive_failed_after_secs: Some(0),
        };
        let now = Utc::now();
        let old_time = now - chrono::Duration::seconds(999999);
        assert!(!should_auto_archive(
            &DownloadState::Queued,
            old_time,
            &config,
            now
        ));
    }

    #[test]
    fn test_should_auto_archive_exact_boundary() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: Some(100),
            auto_archive_failed_after_secs: None,
        };
        let now = Utc::now();
        let updated = now - chrono::Duration::seconds(100);
        // Exactly at boundary
        assert!(should_auto_archive(
            &DownloadState::Complete,
            updated,
            &config,
            now
        ));
        // One second before boundary
        let updated_before = now - chrono::Duration::seconds(99);
        assert!(!should_auto_archive(
            &DownloadState::Complete,
            updated_before,
            &config,
            now
        ));
    }

    #[test]
    fn test_should_auto_archive_failed_exact_boundary() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 500,
            auto_archive_completed_after_secs: None,
            auto_archive_failed_after_secs: Some(3600),
        };
        let now = Utc::now();
        let updated = now - chrono::Duration::seconds(3600);
        assert!(should_auto_archive(
            &DownloadState::Error,
            updated,
            &config,
            now
        ));
        let updated_before = now - chrono::Duration::seconds(3599);
        assert!(!should_auto_archive(
            &DownloadState::Error,
            updated_before,
            &config,
            now
        ));
    }

    // --- Summary edge cases ---

    #[test]
    fn test_summary_empty_archive() {
        let state = ArchiveState::default();
        let summary = state.summary();
        assert_eq!(summary.total_archived, 0);
        assert_eq!(summary.completed_count, 0);
        assert_eq!(summary.failed_count, 0);
        assert_eq!(summary.other_count, 0);
        assert_eq!(summary.total_bytes, 0);
        assert!(summary.oldest_archive.is_none());
        assert!(summary.newest_archive.is_none());
    }

    #[test]
    fn test_summary_all_completed() {
        let mut state = ArchiveState::default();
        for i in 0..5 {
            state
                .archive_task(make_test_task(&format!("c{}", i), "Complete"))
                .unwrap();
        }
        let summary = state.summary();
        assert_eq!(summary.total_archived, 5);
        assert_eq!(summary.completed_count, 5);
        assert_eq!(summary.failed_count, 0);
        assert_eq!(summary.other_count, 0);
    }

    #[test]
    fn test_summary_all_failed() {
        let mut state = ArchiveState::default();
        for i in 0..3 {
            state
                .archive_task(make_test_task(&format!("f{}", i), "Error"))
                .unwrap();
        }
        let summary = state.summary();
        assert_eq!(summary.total_archived, 3);
        assert_eq!(summary.completed_count, 0);
        assert_eq!(summary.failed_count, 3);
        assert_eq!(summary.other_count, 0);
    }

    #[test]
    fn test_summary_all_other_states() {
        let mut state = ArchiveState::default();
        state.archive_task(make_test_task("p1", "Paused")).unwrap();
        state
            .archive_task(make_test_task("d1", "Downloading"))
            .unwrap();
        state.archive_task(make_test_task("q1", "Queued")).unwrap();
        let summary = state.summary();
        assert_eq!(summary.other_count, 3);
        assert_eq!(summary.completed_count, 0);
        assert_eq!(summary.failed_count, 0);
    }

    #[test]
    fn test_summary_total_bytes_accumulation() {
        let mut state = ArchiveState::default();
        let mut t1 = make_test_task("b1", "Complete");
        t1.downloaded = 100;
        let mut t2 = make_test_task("b2", "Complete");
        t2.downloaded = 200;
        let mut t3 = make_test_task("b3", "Error");
        t3.downloaded = 300;
        state.archive_task(t1).unwrap();
        state.archive_task(t2).unwrap();
        state.archive_task(t3).unwrap();
        let summary = state.summary();
        assert_eq!(summary.total_bytes, 600);
    }

    #[test]
    fn test_summary_single_task_oldest_newest_same() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("solo", "Complete"))
            .unwrap();
        let summary = state.summary();
        assert!(summary.oldest_archive.is_some());
        assert!(summary.newest_archive.is_some());
        // With single task, oldest == newest
        assert_eq!(
            summary.oldest_archive.unwrap(),
            summary.newest_archive.unwrap()
        );
    }

    // --- List filtering ---

    #[test]
    fn test_list_archived_by_protocol() {
        let mut state = ArchiveState::default();
        let mut t1 = make_test_task("h1", "Complete");
        t1.protocol = "HTTP".to_string();
        let mut t2 = make_test_task("h2", "Complete");
        t2.protocol = "FTP".to_string();
        let mut t3 = make_test_task("h3", "Complete");
        t3.protocol = "HTTP".to_string();
        state.archive_task(t1).unwrap();
        state.archive_task(t2).unwrap();
        state.archive_task(t3).unwrap();
        let http = state.list_archived(None, Some("HTTP"), None);
        assert_eq!(http.len(), 2);
        let ftp = state.list_archived(None, Some("FTP"), None);
        assert_eq!(ftp.len(), 1);
    }

    #[test]
    fn test_list_archived_combined_filters() {
        let mut state = ArchiveState::default();
        let mut t1 = make_test_task("cf1", "Complete");
        t1.protocol = "HTTP".to_string();
        t1.tags = vec!["movies".to_string()];
        let mut t2 = make_test_task("cf2", "Complete");
        t2.protocol = "HTTP".to_string();
        t2.tags = vec!["music".to_string()];
        let mut t3 = make_test_task("cf3", "Error");
        t3.protocol = "FTP".to_string();
        t3.tags = vec!["movies".to_string()];
        state.archive_task(t1).unwrap();
        state.archive_task(t2).unwrap();
        state.archive_task(t3).unwrap();
        // State + protocol
        let complete_http = state.list_archived(Some("Complete"), Some("HTTP"), None);
        assert_eq!(complete_http.len(), 2);
        // State + tag
        let complete_movies = state.list_archived(Some("Complete"), None, Some("movies"));
        assert_eq!(complete_movies.len(), 1);
        assert_eq!(complete_movies[0].id, "cf1");
        // All three filters
        let all_filters = state.list_archived(Some("Complete"), Some("HTTP"), Some("music"));
        assert_eq!(all_filters.len(), 1);
        assert_eq!(all_filters[0].id, "cf2");
    }

    #[test]
    fn test_list_archived_no_matches() {
        let mut state = ArchiveState::default();
        state
            .archive_task(make_test_task("nm1", "Complete"))
            .unwrap();
        let result = state.list_archived(Some("Error"), None, None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_list_archived_empty_archive() {
        let state = ArchiveState::default();
        let result = state.list_archived(None, None, None);
        assert!(result.is_empty());
    }

    // --- Persistence ---

    #[tokio::test]
    async fn test_save_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        assert!(!path.exists());
        let state = ArchiveState::default();
        save_archive_state(&path, &state).await.unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_save_overwrites_existing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        let mut state1 = ArchiveState::default();
        state1
            .archive_task(make_test_task("first", "Complete"))
            .unwrap();
        save_archive_state(&path, &state1).await.unwrap();

        let mut state2 = ArchiveState::default();
        state2
            .archive_task(make_test_task("second", "Error"))
            .unwrap();
        state2
            .archive_task(make_test_task("third", "Complete"))
            .unwrap();
        save_archive_state(&path, &state2).await.unwrap();

        let loaded = load_archive_state(&path).await.unwrap();
        assert_eq!(loaded.archived.len(), 2);
        assert_eq!(loaded.archived[0].id, "third");
        assert_eq!(loaded.archived[1].id, "second");
    }

    #[tokio::test]
    async fn test_save_no_tmp_leftover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        let state = ArchiveState::default();
        save_archive_state(&path, &state).await.unwrap();
        let tmp_path = path.with_extension("json.tmp");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_load_corrupt_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        fs::write(&path, "{not valid json").await.unwrap();
        let result = load_archive_state(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_empty_file_returns_default() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        fs::write(&path, "   ").await.unwrap();
        let state = load_archive_state(&path).await.unwrap();
        assert!(state.archived.is_empty());
        assert!(state.config.enabled);
    }

    #[tokio::test]
    async fn test_pretty_json_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        let mut state = ArchiveState::default();
        state.config.max_archived = 42;
        state
            .archive_task(make_test_task("pretty", "Complete"))
            .unwrap();
        save_archive_state(&path, &state).await.unwrap();

        let content = fs::read_to_string(&path).await.unwrap();
        // Verify it's pretty-printed (contains newlines and indentation)
        assert!(content.contains('\n'));
        assert!(content.contains("  "));

        let loaded = load_archive_state(&path).await.unwrap();
        assert_eq!(loaded.archived.len(), 1);
        assert_eq!(loaded.config.max_archived, 42);
    }

    #[tokio::test]
    async fn test_persistence_unicode_names() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        let mut state = ArchiveState::default();
        let mut task = make_test_task("uni1", "Complete");
        task.name = "中文任务名称".to_string();
        task.tags = vec!["标签".to_string(), "emoji🎉".to_string()];
        state.archive_task(task).unwrap();
        save_archive_state(&path, &state).await.unwrap();

        let loaded = load_archive_state(&path).await.unwrap();
        assert_eq!(loaded.archived[0].name, "中文任务名称");
        assert_eq!(loaded.archived[0].tags, vec!["标签", "emoji🎉"]);
    }

    #[tokio::test]
    async fn test_full_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("archive.json");
        let mut state = ArchiveState::default();
        state.config.enabled = true;
        state.config.max_archived = 1000;
        state.config.auto_archive_completed_after_secs = Some(7200);
        state.config.auto_archive_failed_after_secs = Some(14400);

        for i in 0..10 {
            let state_str = if i % 3 == 0 {
                "Complete"
            } else if i % 3 == 1 {
                "Error"
            } else {
                "Paused"
            };
            state
                .archive_task(make_test_task(&format!("task-{}", i), state_str))
                .unwrap();
        }

        save_archive_state(&path, &state).await.unwrap();
        let loaded = load_archive_state(&path).await.unwrap();

        assert_eq!(loaded.archived.len(), 10);
        assert_eq!(loaded.config.max_archived, 1000);
        assert_eq!(loaded.config.auto_archive_completed_after_secs, Some(7200));
        assert_eq!(loaded.config.auto_archive_failed_after_secs, Some(14400));

        let summary = loaded.summary();
        assert_eq!(summary.total_archived, 10);
    }

    // --- Unicode ---

    #[test]
    fn test_archived_task_unicode_fields() {
        let task = create_archived_task(
            "unicode-id-日本語".to_string(),
            "任务名中文".to_string(),
            "HTTP",
            1000,
            500,
            "Complete",
            Some("错误信息".to_string()),
            PathBuf::from("/tmp/中文路径"),
            Utc::now(),
            Utc::now(),
            vec!["标签1".to_string(), "タグ".to_string()],
            Some("グループ".to_string()),
            Some("メモ".to_string()),
            Some("http://example.com/文件.zip".to_string()),
            60.0,
            Some("理由".to_string()),
        );
        let json = serde_json::to_string(&task).unwrap();
        let back: ArchivedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "unicode-id-日本語");
        assert_eq!(back.name, "任务名中文");
        assert_eq!(back.error, Some("错误信息".to_string()));
        assert_eq!(back.tags, vec!["标签1", "タグ"]);
    }

    #[test]
    fn test_archived_task_emoji_fields() {
        let task = create_archived_task(
            "🎉task".to_string(),
            "🚀 Download".to_string(),
            "HTTP",
            100,
            100,
            "Complete",
            None,
            PathBuf::from("/tmp"),
            Utc::now(),
            Utc::now(),
            vec!["⭐".to_string()],
            Some("🏆".to_string()),
            Some("📝 note".to_string()),
            None,
            0.0,
            None,
        );
        let json = serde_json::to_string(&task).unwrap();
        let back: ArchivedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "🎉task");
        assert_eq!(back.name, "🚀 Download");
    }

    // --- Boundary conditions ---

    #[test]
    fn test_archive_max_zero() {
        let mut state = ArchiveState::default();
        state.config.max_archived = 0;
        let result = state.archive_task(make_test_task("zero", "Complete"));
        assert!(matches!(
            result,
            Err(TaskArchiveError::ArchiveFull { max: 0 })
        ));
    }

    #[test]
    fn test_archive_max_one() {
        let mut state = ArchiveState::default();
        state.config.max_archived = 1;
        state
            .archive_task(make_test_task("first", "Complete"))
            .unwrap();
        let result = state.archive_task(make_test_task("second", "Complete"));
        assert!(matches!(
            result,
            Err(TaskArchiveError::ArchiveFull { max: 1 })
        ));
    }

    #[test]
    fn test_unarchive_from_empty() {
        let mut state = ArchiveState::default();
        assert!(state.unarchive_task("anything").is_none());
    }

    #[test]
    fn test_clear_empty_archive() {
        let mut state = ArchiveState::default();
        state.clear_archive(); // should not panic
        assert!(state.archived.is_empty());
    }

    #[test]
    fn test_find_in_empty_archive() {
        let state = ArchiveState::default();
        assert!(state.find_archived("any").is_none());
    }

    #[test]
    fn test_archive_large_size_values() {
        let mut task = make_test_task("big", "Complete");
        task.size = u64::MAX;
        task.downloaded = u64::MAX;
        let mut state = ArchiveState::default();
        state.archive_task(task).unwrap();
        let summary = state.summary();
        assert_eq!(summary.total_bytes, u64::MAX);
    }

    #[test]
    fn test_archive_zero_active_time() {
        let mut task = make_test_task("zero-time", "Complete");
        task.active_time_seconds = 0.0;
        let mut state = ArchiveState::default();
        state.archive_task(task).unwrap();
        assert!((state.archived[0].active_time_seconds - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_archive_negative_active_time() {
        // Negative active time is technically allowed (no validation)
        let mut task = make_test_task("neg-time", "Complete");
        task.active_time_seconds = -1.0;
        let mut state = ArchiveState::default();
        state.archive_task(task).unwrap();
        assert!((state.archived[0].active_time_seconds - (-1.0)).abs() < f64::EPSILON);
    }

    // --- Complete workflow ---

    #[test]
    fn test_complete_workflow() {
        let mut state = ArchiveState::default();

        // Archive multiple tasks
        state
            .archive_task(make_test_task("w1", "Complete"))
            .unwrap();
        state.archive_task(make_test_task("w2", "Error")).unwrap();
        state
            .archive_task(make_test_task("w3", "Complete"))
            .unwrap();
        assert_eq!(state.archived.len(), 3);

        // Find one
        assert!(state.find_archived("w2").is_some());

        // List filtered
        let completed = state.list_archived(Some("Complete"), None, None);
        assert_eq!(completed.len(), 2);

        // Unarchive one
        let removed = state.unarchive_task("w2");
        assert!(removed.is_some());
        assert_eq!(state.archived.len(), 2);

        // Summary
        let summary = state.summary();
        assert_eq!(summary.total_archived, 2);
        assert_eq!(summary.completed_count, 2);
        assert_eq!(summary.failed_count, 0);

        // Clear
        state.clear_archive();
        assert!(state.archived.is_empty());
        let summary2 = state.summary();
        assert_eq!(summary2.total_archived, 0);
    }

    #[test]
    fn test_archive_unarchive_rearchive() {
        let mut state = ArchiveState::default();
        let task = make_test_task("cycle", "Complete");
        state.archive_task(task.clone()).unwrap();
        let removed = state.unarchive_task("cycle").unwrap();
        assert_eq!(removed.id, "cycle");
        assert!(state.archived.is_empty());

        // Re-archive
        state.archive_task(removed).unwrap();
        assert_eq!(state.archived.len(), 1);
        assert_eq!(state.archived[0].id, "cycle");
    }

    // --- Config custom values ---

    #[test]
    fn test_archive_config_custom_values() {
        let config = ArchiveConfig {
            enabled: false,
            max_archived: 1,
            auto_archive_completed_after_secs: Some(0),
            auto_archive_failed_after_secs: Some(u64::MAX),
        };
        assert!(!config.enabled);
        assert_eq!(config.max_archived, 1);
        assert_eq!(config.auto_archive_completed_after_secs, Some(0));
        assert_eq!(config.auto_archive_failed_after_secs, Some(u64::MAX));
    }

    #[test]
    fn test_archive_config_pretty_serde() {
        let config = ArchiveConfig {
            enabled: true,
            max_archived: 42,
            auto_archive_completed_after_secs: Some(3600),
            auto_archive_failed_after_secs: None,
        };
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let back: ArchiveConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.max_archived, 42);
    }
}
