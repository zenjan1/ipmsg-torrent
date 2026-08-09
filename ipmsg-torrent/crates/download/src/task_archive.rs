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
                if let Some(state) = state_filter {
                    if t.final_state != state {
                        return false;
                    }
                }
                if let Some(protocol) = protocol_filter {
                    if t.protocol != protocol {
                        return false;
                    }
                }
                if let Some(tag) = tag_filter {
                    if !t.tags.contains(&tag.to_string()) {
                        return false;
                    }
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
}
