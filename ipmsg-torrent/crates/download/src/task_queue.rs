//! Task queue persistence for download manager
//!
//! Saves and loads download task state to survive application restarts.

use crate::{DownloadPriority, DownloadProtocol, DownloadState, DownloadTask, TimeWindow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Serializable task state for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedTask {
    pub id: String,
    pub name: String,
    pub protocol: PersistedProtocol,
    pub size: u64,
    pub downloaded: u64,
    pub state: PersistedState,
    pub error: Option<String>,
    pub speed_bps: f64,
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
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub speed_limit_bps: Option<u64>,
    #[serde(default)]
    pub auto_retry_count: u32,
    #[serde(default)]
    pub retry_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_url: Option<String>,
    #[serde(default)]
    pub expected_checksum: Option<String>,
    #[serde(default)]
    pub checksum_algorithm: Option<crate::checksum::ChecksumAlgorithm>,
    #[serde(default)]
    pub mirror_urls: Vec<String>,
    #[serde(default)]
    pub retry_policy: Option<crate::RetryPolicy>,
    #[serde(default)]
    pub sequential_mode: bool,
    #[serde(default)]
    pub notes: Option<String>,
    /// Maximum download time in seconds (auto-pause when exceeded, None = no limit)
    #[serde(default)]
    pub max_download_time_secs: Option<u64>,
}

fn default_bandwidth_weight() -> u8 {
    1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedProtocol {
    Torrent,
    Ed2k,
    Xunlei,
    Magnet,
    P2P,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedState {
    Queued,
    Downloading,
    Paused,
    Complete,
    Error,
}

impl From<DownloadProtocol> for PersistedProtocol {
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

impl From<PersistedProtocol> for DownloadProtocol {
    fn from(p: PersistedProtocol) -> Self {
        match p {
            PersistedProtocol::Torrent => Self::Torrent,
            PersistedProtocol::Ed2k => Self::Ed2k,
            PersistedProtocol::Xunlei => Self::Xunlei,
            PersistedProtocol::Magnet => Self::Magnet,
            PersistedProtocol::P2P => Self::P2P,
        }
    }
}

impl From<DownloadState> for PersistedState {
    fn from(s: DownloadState) -> Self {
        match s {
            DownloadState::Queued => Self::Queued,
            DownloadState::Downloading => Self::Downloading,
            DownloadState::Paused => Self::Paused,
            DownloadState::Complete => Self::Complete,
            DownloadState::Error => Self::Error,
        }
    }
}

impl From<PersistedState> for DownloadState {
    fn from(s: PersistedState) -> Self {
        match s {
            PersistedState::Queued => Self::Queued,
            PersistedState::Downloading => Self::Downloading,
            PersistedState::Paused => Self::Paused,
            PersistedState::Complete => Self::Complete,
            PersistedState::Error => Self::Error,
        }
    }
}

impl From<DownloadTask> for PersistedTask {
    fn from(t: DownloadTask) -> Self {
        Self {
            id: t.id,
            name: t.name,
            protocol: t.protocol.into(),
            size: t.size,
            downloaded: t.downloaded,
            state: t.state.into(),
            error: t.error,
            speed_bps: t.speed_bps,
            save_path: t.save_path,
            created_at: t.created_at,
            updated_at: t.updated_at,
            tags: t.tags,
            priority: t.priority,
            schedule: t.schedule,
            bandwidth_weight: t.bandwidth_weight,
            queue_position: t.queue_position,
            depends_on: t.depends_on,
            group: t.group,
            speed_limit_bps: t.speed_limit_bps,
            auto_retry_count: t.auto_retry_count,
            retry_after: t.retry_after,
            source_url: t.source_url,
            expected_checksum: t.expected_checksum,
            checksum_algorithm: t.checksum_algorithm,
            mirror_urls: t.mirror_urls.clone(),
            retry_policy: t.retry_policy,
            sequential_mode: t.sequential_mode,
            notes: t.notes,
            max_download_time_secs: t.max_download_time_secs,
        }
    }
}

impl From<PersistedTask> for DownloadTask {
    fn from(t: PersistedTask) -> Self {
        Self {
            id: t.id,
            name: t.name,
            protocol: t.protocol.into(),
            size: t.size,
            downloaded: t.downloaded,
            state: t.state.into(),
            error: t.error,
            speed_bps: t.speed_bps,
            save_path: t.save_path,
            created_at: t.created_at,
            updated_at: t.updated_at,
            tags: t.tags,
            priority: t.priority,
            schedule: t.schedule,
            bandwidth_weight: t.bandwidth_weight,
            queue_position: t.queue_position,
            depends_on: t.depends_on,
            group: t.group,
            speed_limit_bps: t.speed_limit_bps,
            auto_retry_count: t.auto_retry_count,
            retry_after: t.retry_after,
            source_url: t.source_url,
            expected_checksum: t.expected_checksum,
            checksum_algorithm: t.checksum_algorithm,
            mirror_urls: t.mirror_urls.clone(),
            active_time_seconds: 0.0,
            current_session_start: None,
            retry_policy: t.retry_policy,
            cooldown: None,
            sequential_mode: t.sequential_mode,
            notes: t.notes,
            max_download_time_secs: t.max_download_time_secs,
        }
    }
}

/// Save task queue to disk
pub fn save_task_queue(tasks: &[DownloadTask], data_dir: &Path) -> Result<(), TaskQueueError> {
    let queue_path = data_dir.join("task_queue.json");
    let persisted: Vec<PersistedTask> = tasks.iter().cloned().map(PersistedTask::from).collect();

    let json = serde_json::to_string_pretty(&persisted)
        .map_err(|e| TaskQueueError::Serialize(e.to_string()))?;

    std::fs::write(&queue_path, json).map_err(|e| TaskQueueError::Io(e.to_string()))?;

    Ok(())
}

/// Load task queue from disk
pub fn load_task_queue(data_dir: &Path) -> Result<Vec<DownloadTask>, TaskQueueError> {
    let queue_path = data_dir.join("task_queue.json");

    if !queue_path.exists() {
        return Ok(Vec::new());
    }

    let json =
        std::fs::read_to_string(&queue_path).map_err(|e| TaskQueueError::Io(e.to_string()))?;

    let persisted: Vec<PersistedTask> =
        serde_json::from_str(&json).map_err(|e| TaskQueueError::Deserialize(e.to_string()))?;

    Ok(persisted.into_iter().map(DownloadTask::from).collect())
}

#[derive(Debug, thiserror::Error)]
pub enum TaskQueueError {
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
    use std::path::PathBuf;

    #[test]
    fn test_protocol_conversion() {
        let protocols = vec![
            DownloadProtocol::Torrent,
            DownloadProtocol::Ed2k,
            DownloadProtocol::Xunlei,
            DownloadProtocol::Magnet,
            DownloadProtocol::P2P,
        ];

        for p in protocols {
            let persisted: PersistedProtocol = p.into();
            let back: DownloadProtocol = persisted.into();
            assert_eq!(p, back);
        }
    }

    #[test]
    fn test_state_conversion() {
        let states = vec![
            DownloadState::Queued,
            DownloadState::Downloading,
            DownloadState::Paused,
            DownloadState::Complete,
            DownloadState::Error,
        ];

        for s in states {
            let persisted: PersistedState = s.into();
            let back: DownloadState = persisted.into();
            assert_eq!(s, back);
        }
    }

    #[test]
    fn test_task_conversion() {
        let task = DownloadTask {
            id: "test-123".to_string(),
            name: "test_file.txt".to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 1024,
            downloaded: 512,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 100.0,
            save_path: PathBuf::from("/tmp/downloads"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: crate::DownloadPriority::Normal,
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
        };

        let persisted: PersistedTask = task.clone().into();
        let back: DownloadTask = persisted.into();

        assert_eq!(task.id, back.id);
        assert_eq!(task.name, back.name);
        assert_eq!(task.protocol, back.protocol);
        assert_eq!(task.size, back.size);
        assert_eq!(task.downloaded, back.downloaded);
        assert_eq!(task.state, back.state);
        assert_eq!(task.speed_bps, back.speed_bps);
        assert_eq!(task.save_path, back.save_path);
    }

    #[test]
    fn test_save_load_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        // Load from non-existent file should return empty
        let tasks = load_task_queue(data_dir).unwrap();
        assert!(tasks.is_empty());

        // Save empty list
        save_task_queue(&[], data_dir).unwrap();

        // Load should still be empty
        let tasks = load_task_queue(data_dir).unwrap();
        assert!(tasks.is_empty());
    }

    #[test]
    fn test_save_load_tasks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let tasks = vec![
            DownloadTask {
                id: "task-1".to_string(),
                name: "file1.txt".to_string(),
                protocol: DownloadProtocol::Torrent,
                size: 2048,
                downloaded: 1024,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 50.0,
                save_path: PathBuf::from("/tmp/dl"),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                tags: Vec::new(),
                priority: crate::DownloadPriority::Normal,
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
            },
            DownloadTask {
                id: "task-2".to_string(),
                name: "file2.mp4".to_string(),
                protocol: DownloadProtocol::Ed2k,
                size: 1048576,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: PathBuf::from("/tmp/dl"),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                tags: Vec::new(),
                priority: crate::DownloadPriority::Normal,
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
            },
        ];

        // Save tasks
        save_task_queue(&tasks, data_dir).unwrap();

        // Load and verify
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded.len(), 2);

        assert_eq!(loaded[0].id, "task-1");
        assert_eq!(loaded[0].name, "file1.txt");
        assert_eq!(loaded[0].protocol, DownloadProtocol::Torrent);
        assert_eq!(loaded[0].size, 2048);
        assert_eq!(loaded[0].downloaded, 1024);
        assert_eq!(loaded[0].state, DownloadState::Downloading);

        assert_eq!(loaded[1].id, "task-2");
        assert_eq!(loaded[1].name, "file2.mp4");
        assert_eq!(loaded[1].protocol, DownloadProtocol::Ed2k);
        assert_eq!(loaded[1].size, 1048576);
        assert_eq!(loaded[1].downloaded, 0);
        assert_eq!(loaded[1].state, DownloadState::Queued);
    }

    #[test]
    fn test_task_with_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "err-task".to_string(),
            name: "failed.txt".to_string(),
            protocol: DownloadProtocol::Magnet,
            size: 512,
            downloaded: 256,
            state: DownloadState::Error,
            error: Some("Connection timeout".to_string()),
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp/dl"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: crate::DownloadPriority::Normal,
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].state, DownloadState::Error);
        assert_eq!(loaded[0].error, Some("Connection timeout".to_string()));
    }

    #[test]
    fn test_notes_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "notes-task".to_string(),
            name: "noted_file.txt".to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 1024,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp/dl"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: crate::DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: Some("This is a test note with 中文 characters".to_string()),
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].notes,
            Some("This is a test note with 中文 characters".to_string())
        );

        // Test None notes
        let task2 = DownloadTask {
            id: "no-notes-task".to_string(),
            name: "no_notes.txt".to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 512,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp/dl"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: crate::DownloadPriority::Normal,
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
        };

        save_task_queue(&[task2], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        // Find the task with no notes
        let loaded_task2 = loaded.iter().find(|t| t.id == "no-notes-task").unwrap();
        assert_eq!(loaded_task2.notes, None);
    }
}
