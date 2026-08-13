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
    /// Number of times this task has been auto-promoted by queue staleness detection
    #[serde(default)]
    pub staleness_promotion_count: u32,
    /// Optional deadline (UTC timestamp)
    #[serde(default)]
    pub deadline: Option<chrono::DateTime<chrono::Utc>>,
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
            staleness_promotion_count: t.staleness_promotion_count,
            deadline: t.deadline,
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
            proxy_override: None,
            staleness_promotion_count: t.staleness_promotion_count,
            deadline: t.deadline,
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
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
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
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
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        };

        save_task_queue(&[task2], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        // Find the task with no notes
        let loaded_task2 = loaded.iter().find(|t| t.id == "no-notes-task").unwrap();
        assert_eq!(loaded_task2.notes, None);
    }

    #[test]
    fn test_persisted_protocol_all_variants() {
        // Test all protocol variants
        let variants = vec![
            (DownloadProtocol::Torrent, PersistedProtocol::Torrent),
            (DownloadProtocol::Ed2k, PersistedProtocol::Ed2k),
            (DownloadProtocol::Xunlei, PersistedProtocol::Xunlei),
            (DownloadProtocol::Magnet, PersistedProtocol::Magnet),
            (DownloadProtocol::P2P, PersistedProtocol::P2P),
        ];

        for (proto, expected_persisted) in variants {
            let persisted: PersistedProtocol = proto.into();
            assert_eq!(persisted, expected_persisted);
            let back: DownloadProtocol = persisted.into();
            assert_eq!(back, proto);
        }
    }

    #[test]
    fn test_persisted_state_all_variants() {
        let variants = vec![
            (DownloadState::Queued, PersistedState::Queued),
            (DownloadState::Downloading, PersistedState::Downloading),
            (DownloadState::Paused, PersistedState::Paused),
            (DownloadState::Complete, PersistedState::Complete),
            (DownloadState::Error, PersistedState::Error),
        ];

        for (state, expected_persisted) in variants {
            let persisted: PersistedState = state.into();
            assert_eq!(persisted, expected_persisted);
            let back: DownloadState = persisted.into();
            assert_eq!(back, state);
        }
    }

    #[test]
    fn test_persisted_task_serialization() {
        let task = DownloadTask {
            id: "serialize-test".to_string(),
            name: "test.zip".to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 10240,
            downloaded: 5120,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 1024.5,
            save_path: PathBuf::from("/downloads"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["test".to_string(), "archive".to_string()],
            priority: crate::DownloadPriority::High,
            schedule: None,
            bandwidth_weight: 5,
            queue_position: Some(3),
            depends_on: vec!["dep1".to_string()],
            notes: Some("Important file".to_string()),
            group: Some("downloads".to_string()),
            speed_limit_bps: Some(2048),
            auto_retry_count: 2,
            retry_after: Some(Utc::now()),
            source_url: Some("http://example.com/file.zip".to_string()),
            expected_checksum: Some("abc123".to_string()),
            checksum_algorithm: None,
            active_time_seconds: 120.0,
            current_session_start: None,
            mirror_urls: vec!["http://mirror.com/file.zip".to_string()],
            retry_policy: None,
            cooldown: None,
            sequential_mode: true,
            max_download_time_secs: Some(3600),
            proxy_override: None,
            staleness_promotion_count: 1,
            deadline: Some(Utc::now()),
        };

        let persisted: PersistedTask = task.clone().into();
        let json = serde_json::to_string(&persisted).unwrap();
        let deserialized: PersistedTask = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, "serialize-test");
        assert_eq!(deserialized.name, "test.zip");
        assert_eq!(deserialized.size, 10240);
        assert_eq!(deserialized.downloaded, 5120);
        assert_eq!(deserialized.speed_bps, 1024.5);
        assert_eq!(deserialized.tags.len(), 2);
        assert_eq!(deserialized.bandwidth_weight, 5);
        assert_eq!(deserialized.queue_position, Some(3));
        assert_eq!(deserialized.depends_on.len(), 1);
        assert_eq!(deserialized.notes, Some("Important file".to_string()));
        assert_eq!(deserialized.group, Some("downloads".to_string()));
        assert_eq!(deserialized.speed_limit_bps, Some(2048));
        assert_eq!(deserialized.auto_retry_count, 2);
        assert!(deserialized.retry_after.is_some());
        assert_eq!(
            deserialized.source_url,
            Some("http://example.com/file.zip".to_string())
        );
        assert_eq!(deserialized.expected_checksum, Some("abc123".to_string()));
        assert_eq!(deserialized.mirror_urls.len(), 1);
        assert_eq!(deserialized.sequential_mode, true);
        assert_eq!(deserialized.max_download_time_secs, Some(3600));
        assert_eq!(deserialized.staleness_promotion_count, 1);
        assert!(deserialized.deadline.is_some());
    }

    #[test]
    fn test_persisted_task_with_defaults() {
        // Test that default values are properly handled
        let task = DownloadTask {
            id: "defaults-test".to_string(),
            name: "defaults.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 1000,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: crate::DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1, // default
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
        };

        let persisted: PersistedTask = task.into();
        let json = serde_json::to_string(&persisted).unwrap();

        // Verify JSON contains expected structure
        assert!(json.contains("\"bandwidth_weight\":1"));
        assert!(json.contains("\"sequential_mode\":false"));
        assert!(json.contains("\"staleness_promotion_count\":0"));

        let deserialized: PersistedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.bandwidth_weight, 1);
        assert_eq!(deserialized.sequential_mode, false);
        assert_eq!(deserialized.staleness_promotion_count, 0);
    }

    #[test]
    fn test_persisted_task_missing_fields_use_defaults() {
        // Test backward compatibility: missing fields should use defaults
        let json = r#"{
            "id": "backward-compat",
            "name": "old_task.txt",
            "protocol": "Torrent",
            "size": 5000,
            "downloaded": 1000,
            "state": "Paused",
            "error": null,
            "speed_bps": 0.0,
            "save_path": "/downloads",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }"#;

        let persisted: PersistedTask = serde_json::from_str(json).unwrap();
        assert_eq!(persisted.id, "backward-compat");
        assert_eq!(persisted.bandwidth_weight, 1); // default
        assert_eq!(persisted.sequential_mode, false); // default
        assert_eq!(persisted.staleness_promotion_count, 0); // default
        assert_eq!(persisted.auto_retry_count, 0); // default
        assert!(persisted.tags.is_empty()); // default
        assert!(persisted.depends_on.is_empty()); // default
        assert!(persisted.mirror_urls.is_empty()); // default
    }

    #[test]
    fn test_task_conversion_preserves_all_fields() {
        let original = DownloadTask {
            id: "full-conversion".to_string(),
            name: "complete.mp4".to_string(),
            protocol: DownloadProtocol::Magnet,
            size: 1048576,
            downloaded: 524288,
            state: DownloadState::Downloading,
            error: Some("Partial error".to_string()),
            speed_bps: 2048.0,
            save_path: PathBuf::from("/videos"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["video".to_string(), "large".to_string()],
            priority: crate::DownloadPriority::High,
            schedule: None,
            bandwidth_weight: 8,
            queue_position: Some(5),
            depends_on: vec!["task-1".to_string(), "task-2".to_string()],
            notes: Some("Large video file".to_string()),
            group: Some("media".to_string()),
            speed_limit_bps: Some(4096),
            auto_retry_count: 3,
            retry_after: Some(Utc::now()),
            source_url: Some("magnet:?xt=urn:btih:abc123".to_string()),
            expected_checksum: Some("sha256:xyz789".to_string()),
            checksum_algorithm: None,
            active_time_seconds: 300.5,
            current_session_start: None,
            mirror_urls: vec!["http://backup.com/video.mp4".to_string()],
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: Some(7200),
            proxy_override: None,
            staleness_promotion_count: 2,
            deadline: Some(Utc::now()),
        };

        let persisted: PersistedTask = original.clone().into();
        let restored: DownloadTask = persisted.into();

        assert_eq!(restored.id, original.id);
        assert_eq!(restored.name, original.name);
        assert_eq!(restored.protocol, original.protocol);
        assert_eq!(restored.size, original.size);
        assert_eq!(restored.downloaded, original.downloaded);
        assert_eq!(restored.state, original.state);
        assert_eq!(restored.error, original.error);
        assert_eq!(restored.speed_bps, original.speed_bps);
        assert_eq!(restored.save_path, original.save_path);
        assert_eq!(restored.tags, original.tags);
        assert_eq!(restored.priority, original.priority);
        assert_eq!(restored.bandwidth_weight, original.bandwidth_weight);
        assert_eq!(restored.queue_position, original.queue_position);
        assert_eq!(restored.depends_on, original.depends_on);
        assert_eq!(restored.notes, original.notes);
        assert_eq!(restored.group, original.group);
        assert_eq!(restored.speed_limit_bps, original.speed_limit_bps);
        assert_eq!(restored.auto_retry_count, original.auto_retry_count);
        assert_eq!(restored.mirror_urls, original.mirror_urls);
        assert_eq!(restored.sequential_mode, original.sequential_mode);
        assert_eq!(
            restored.max_download_time_secs,
            original.max_download_time_secs
        );
        assert_eq!(
            restored.staleness_promotion_count,
            original.staleness_promotion_count
        );
    }

    #[test]
    fn test_task_conversion_resets_session_fields() {
        let task = DownloadTask {
            id: "session-reset".to_string(),
            name: "test.txt".to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 1000,
            downloaded: 500,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 100.0,
            save_path: PathBuf::from("/tmp"),
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
            active_time_seconds: 123.45,             // Should be reset
            current_session_start: Some(Utc::now()), // Should be reset
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        };

        let persisted: PersistedTask = task.into();
        let restored: DownloadTask = persisted.into();

        // Session-specific fields should be reset
        assert_eq!(restored.active_time_seconds, 0.0);
        assert!(restored.current_session_start.is_none());
    }

    #[test]
    fn test_save_overwrites_existing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        // Save first batch
        let tasks1 = vec![DownloadTask {
            id: "batch1".to_string(),
            name: "file1.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        }];
        save_task_queue(&tasks1, data_dir).unwrap();

        // Save second batch (should overwrite)
        let tasks2 = vec![DownloadTask {
            id: "batch2".to_string(),
            name: "file2.txt".to_string(),
            protocol: DownloadProtocol::Ed2k,
            size: 200,
            downloaded: 50,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 10.0,
            save_path: PathBuf::from("/tmp"),
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        }];
        save_task_queue(&tasks2, data_dir).unwrap();

        // Load and verify only second batch exists
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "batch2");
        assert_eq!(loaded[0].name, "file2.txt");
    }

    #[test]
    fn test_load_with_invalid_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();
        let queue_path = data_dir.join("task_queue.json");

        // Write invalid JSON
        std::fs::write(&queue_path, "not valid json {{{").unwrap();

        // Should return error
        let result = load_task_queue(data_dir);
        assert!(result.is_err());
        match result {
            Err(TaskQueueError::Deserialize(_)) => (),
            _ => panic!("Expected Deserialize error"),
        }
    }

    #[test]
    fn test_load_with_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();
        let queue_path = data_dir.join("task_queue.json");

        // Write empty file
        std::fs::write(&queue_path, "").unwrap();

        // Should return error
        let result = load_task_queue(data_dir);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_with_empty_array() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();
        let queue_path = data_dir.join("task_queue.json");

        // Write empty array
        std::fs::write(&queue_path, "[]").unwrap();

        // Should load successfully with 0 tasks
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded.len(), 0);
    }

    #[test]
    fn test_save_with_special_characters_in_name() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "special-chars".to_string(),
            name: "file with spaces & 中文 & emoji 🎉.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "file with spaces & 中文 & emoji 🎉.txt");
    }

    #[test]
    fn test_save_with_unicode_tags() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "unicode-tags".to_string(),
            name: "test.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["标签1".to_string(), "タグ2".to_string(), "тег3".to_string()],
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
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        assert_eq!(loaded[0].tags.len(), 3);
        assert_eq!(loaded[0].tags[0], "标签1");
        assert_eq!(loaded[0].tags[1], "タグ2");
        assert_eq!(loaded[0].tags[2], "тег3");
    }

    #[test]
    fn test_multiple_tasks_preserve_order() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let tasks: Vec<DownloadTask> = (0..10)
            .map(|i| DownloadTask {
                id: format!("task-{}", i),
                name: format!("file{}.txt", i),
                protocol: DownloadProtocol::Torrent,
                size: (i + 1) * 100,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: PathBuf::from("/tmp"),
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
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
            })
            .collect();

        save_task_queue(&tasks, data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        assert_eq!(loaded.len(), 10);
        for i in 0..10 {
            assert_eq!(loaded[i].id, format!("task-{}", i));
            assert_eq!(loaded[i].size, ((i + 1) * 100) as u64);
        }
    }

    #[test]
    fn test_task_error_display() {
        let io_error = TaskQueueError::Io("Permission denied".to_string());
        assert_eq!(format!("{}", io_error), "IO error: Permission denied");

        let serialize_error = TaskQueueError::Serialize("Invalid data".to_string());
        assert_eq!(
            format!("{}", serialize_error),
            "serialize error: Invalid data"
        );

        let deserialize_error = TaskQueueError::Deserialize("Missing field".to_string());
        assert_eq!(
            format!("{}", deserialize_error),
            "deserialize error: Missing field"
        );
    }

    #[test]
    fn test_task_with_all_optional_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "all-optionals".to_string(),
            name: "complete.txt".to_string(),
            protocol: DownloadProtocol::Xunlei,
            size: 10000,
            downloaded: 5000,
            state: DownloadState::Downloading,
            error: Some("Network error".to_string()),
            speed_bps: 512.0,
            save_path: PathBuf::from("/downloads"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["important".to_string()],
            priority: crate::DownloadPriority::High,
            schedule: None,
            bandwidth_weight: 10,
            queue_position: Some(1),
            depends_on: vec!["dep1".to_string(), "dep2".to_string()],
            notes: Some("Critical download".to_string()),
            group: Some("work".to_string()),
            speed_limit_bps: Some(1024),
            auto_retry_count: 5,
            retry_after: Some(Utc::now()),
            source_url: Some("http://example.com/file.txt".to_string()),
            expected_checksum: Some("md5:abc123def456".to_string()),
            checksum_algorithm: None,
            active_time_seconds: 600.0,
            current_session_start: None,
            mirror_urls: vec![
                "http://mirror1.com/file.txt".to_string(),
                "http://mirror2.com/file.txt".to_string(),
            ],
            retry_policy: None,
            cooldown: None,
            sequential_mode: true,
            max_download_time_secs: Some(1800),
            proxy_override: None,
            staleness_promotion_count: 3,
            deadline: Some(Utc::now()),
        };

        save_task_queue(&[task.clone()], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        assert_eq!(loaded.len(), 1);
        let loaded_task = &loaded[0];

        // Verify all optional fields preserved
        assert_eq!(loaded_task.error, Some("Network error".to_string()));
        assert_eq!(loaded_task.tags, vec!["important".to_string()]);
        assert_eq!(loaded_task.priority, crate::DownloadPriority::High);
        assert_eq!(loaded_task.bandwidth_weight, 10);
        assert_eq!(loaded_task.queue_position, Some(1));
        assert_eq!(loaded_task.depends_on.len(), 2);
        assert_eq!(loaded_task.notes, Some("Critical download".to_string()));
        assert_eq!(loaded_task.group, Some("work".to_string()));
        assert_eq!(loaded_task.speed_limit_bps, Some(1024));
        assert_eq!(loaded_task.auto_retry_count, 5);
        assert!(loaded_task.retry_after.is_some());
        assert_eq!(
            loaded_task.source_url,
            Some("http://example.com/file.txt".to_string())
        );
        assert_eq!(
            loaded_task.expected_checksum,
            Some("md5:abc123def456".to_string())
        );
        assert_eq!(loaded_task.mirror_urls.len(), 2);
        assert_eq!(loaded_task.sequential_mode, true);
        assert_eq!(loaded_task.max_download_time_secs, Some(1800));
        assert_eq!(loaded_task.staleness_promotion_count, 3);
        assert!(loaded_task.deadline.is_some());
    }

    #[test]
    fn test_different_protocols_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let protocols = vec![
            DownloadProtocol::Torrent,
            DownloadProtocol::Ed2k,
            DownloadProtocol::Xunlei,
            DownloadProtocol::Magnet,
            DownloadProtocol::P2P,
        ];

        let tasks: Vec<DownloadTask> = protocols
            .iter()
            .enumerate()
            .map(|(i, proto)| DownloadTask {
                id: format!("proto-{}", i),
                name: format!("file{}.ext", i),
                protocol: *proto,
                size: 100,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: PathBuf::from("/tmp"),
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
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
            })
            .collect();

        save_task_queue(&tasks, data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        assert_eq!(loaded.len(), 5);
        for (i, task) in loaded.iter().enumerate() {
            assert_eq!(task.protocol, protocols[i]);
        }
    }

    #[test]
    fn test_different_states_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let states = vec![
            DownloadState::Queued,
            DownloadState::Downloading,
            DownloadState::Paused,
            DownloadState::Complete,
            DownloadState::Error,
        ];

        let tasks: Vec<DownloadTask> = states
            .iter()
            .enumerate()
            .map(|(i, state)| DownloadTask {
                id: format!("state-{}", i),
                name: format!("file{}.ext", i),
                protocol: DownloadProtocol::Torrent,
                size: 100,
                downloaded: if *state == DownloadState::Complete {
                    100
                } else {
                    i as u64 * 20
                },
                state: *state,
                error: if *state == DownloadState::Error {
                    Some("Error occurred".to_string())
                } else {
                    None
                },
                speed_bps: 0.0,
                save_path: PathBuf::from("/tmp"),
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
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
            })
            .collect();

        save_task_queue(&tasks, data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();

        assert_eq!(loaded.len(), 5);
        for (i, task) in loaded.iter().enumerate() {
            assert_eq!(task.state, states[i]);
        }
    }

    #[test]
    fn test_default_bandwidth_weight() {
        // Test that default_bandwidth_weight function returns 1
        assert_eq!(default_bandwidth_weight(), 1);
    }
}
