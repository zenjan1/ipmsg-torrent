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

    // ========== Phase 255: Comprehensive Test Coverage ==========

    // ========== PersistedProtocol serde ==========

    #[test]
    fn test_persisted_protocol_serde_roundtrip_all_variants() {
        for p in [
            PersistedProtocol::Torrent,
            PersistedProtocol::Ed2k,
            PersistedProtocol::Xunlei,
            PersistedProtocol::Magnet,
            PersistedProtocol::P2P,
        ] {
            let json = serde_json::to_string(&p).unwrap();
            let deserialized: PersistedProtocol = serde_json::from_str(&json).unwrap();
            assert_eq!(p, deserialized);
        }
    }

    #[test]
    fn test_persisted_protocol_serde_values() {
        assert_eq!(
            serde_json::to_string(&PersistedProtocol::Torrent).unwrap(),
            "\"Torrent\""
        );
        assert_eq!(
            serde_json::to_string(&PersistedProtocol::Ed2k).unwrap(),
            "\"Ed2k\""
        );
        assert_eq!(
            serde_json::to_string(&PersistedProtocol::Xunlei).unwrap(),
            "\"Xunlei\""
        );
        assert_eq!(
            serde_json::to_string(&PersistedProtocol::Magnet).unwrap(),
            "\"Magnet\""
        );
        assert_eq!(
            serde_json::to_string(&PersistedProtocol::P2P).unwrap(),
            "\"P2P\""
        );
    }

    // ========== PersistedProtocol traits ==========

    #[test]
    fn test_persisted_protocol_clone_copy() {
        let p = PersistedProtocol::Torrent;
        let p2 = p; // Copy
        let p3 = p.clone();
        assert_eq!(p, p2);
        assert_eq!(p, p3);
    }

    #[test]
    fn test_persisted_protocol_debug() {
        assert_eq!(format!("{:?}", PersistedProtocol::Torrent), "Torrent");
        assert_eq!(format!("{:?}", PersistedProtocol::Ed2k), "Ed2k");
        assert_eq!(format!("{:?}", PersistedProtocol::Xunlei), "Xunlei");
        assert_eq!(format!("{:?}", PersistedProtocol::Magnet), "Magnet");
        assert_eq!(format!("{:?}", PersistedProtocol::P2P), "P2P");
    }

    #[test]
    fn test_persisted_protocol_eq() {
        assert_eq!(PersistedProtocol::Torrent, PersistedProtocol::Torrent);
        assert_ne!(PersistedProtocol::Torrent, PersistedProtocol::Ed2k);
        assert_ne!(PersistedProtocol::Xunlei, PersistedProtocol::Magnet);
    }

    // ========== PersistedState serde ==========

    #[test]
    fn test_persisted_state_serde_roundtrip_all_variants() {
        for s in [
            PersistedState::Queued,
            PersistedState::Downloading,
            PersistedState::Paused,
            PersistedState::Complete,
            PersistedState::Error,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let deserialized: PersistedState = serde_json::from_str(&json).unwrap();
            assert_eq!(s, deserialized);
        }
    }

    #[test]
    fn test_persisted_state_serde_values() {
        assert_eq!(
            serde_json::to_string(&PersistedState::Queued).unwrap(),
            "\"Queued\""
        );
        assert_eq!(
            serde_json::to_string(&PersistedState::Downloading).unwrap(),
            "\"Downloading\""
        );
        assert_eq!(
            serde_json::to_string(&PersistedState::Paused).unwrap(),
            "\"Paused\""
        );
        assert_eq!(
            serde_json::to_string(&PersistedState::Complete).unwrap(),
            "\"Complete\""
        );
        assert_eq!(
            serde_json::to_string(&PersistedState::Error).unwrap(),
            "\"Error\""
        );
    }

    // ========== PersistedState traits ==========

    #[test]
    fn test_persisted_state_clone_copy() {
        let s = PersistedState::Downloading;
        let s2 = s; // Copy
        let s3 = s.clone();
        assert_eq!(s, s2);
        assert_eq!(s, s3);
    }

    #[test]
    fn test_persisted_state_debug() {
        assert_eq!(format!("{:?}", PersistedState::Queued), "Queued");
        assert_eq!(format!("{:?}", PersistedState::Downloading), "Downloading");
        assert_eq!(format!("{:?}", PersistedState::Paused), "Paused");
        assert_eq!(format!("{:?}", PersistedState::Complete), "Complete");
        assert_eq!(format!("{:?}", PersistedState::Error), "Error");
    }

    #[test]
    fn test_persisted_state_eq() {
        assert_eq!(PersistedState::Queued, PersistedState::Queued);
        assert_ne!(PersistedState::Queued, PersistedState::Downloading);
        assert_ne!(PersistedState::Complete, PersistedState::Error);
    }

    // ========== PersistedTask serde ==========

    #[test]
    fn test_persisted_task_serde_roundtrip() {
        let task = PersistedTask {
            id: "test-id".to_string(),
            name: "test.txt".to_string(),
            protocol: PersistedProtocol::Torrent,
            size: 1024,
            downloaded: 512,
            state: PersistedState::Downloading,
            error: None,
            speed_bps: 100.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["tag1".to_string()],
            priority: DownloadPriority::High,
            schedule: None,
            bandwidth_weight: 5,
            queue_position: Some(1),
            depends_on: vec!["dep1".to_string()],
            group: Some("group1".to_string()),
            speed_limit_bps: Some(1024),
            auto_retry_count: 2,
            retry_after: None,
            source_url: Some("http://example.com".to_string()),
            expected_checksum: None,
            checksum_algorithm: None,
            mirror_urls: vec!["http://mirror.com".to_string()],
            retry_policy: None,
            sequential_mode: true,
            notes: Some("note".to_string()),
            max_download_time_secs: Some(3600),
            staleness_promotion_count: 1,
            deadline: None,
        };
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: PersistedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, task.id);
        assert_eq!(deserialized.name, task.name);
        assert_eq!(deserialized.size, task.size);
        assert_eq!(deserialized.bandwidth_weight, task.bandwidth_weight);
    }

    #[test]
    fn test_persisted_task_serde_extra_fields_ignored() {
        let json = r#"{
            "id": "test",
            "name": "file.txt",
            "protocol": "Torrent",
            "size": 100,
            "downloaded": 0,
            "state": "Queued",
            "error": null,
            "speed_bps": 0.0,
            "save_path": "/tmp",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
            "unknown_field": "should be ignored",
            "another_extra": 42
        }"#;
        let task: PersistedTask = serde_json::from_str(json).unwrap();
        assert_eq!(task.id, "test");
    }

    #[test]
    fn test_persisted_task_serde_pretty() {
        let task = PersistedTask {
            id: "pretty-test".to_string(),
            name: "pretty.txt".to_string(),
            protocol: PersistedProtocol::Ed2k,
            size: 2048,
            downloaded: 0,
            state: PersistedState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/downloads"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            mirror_urls: Vec::new(),
            retry_policy: None,
            sequential_mode: false,
            notes: None,
            max_download_time_secs: None,
            staleness_promotion_count: 0,
            deadline: None,
        };
        let pretty = serde_json::to_string_pretty(&task).unwrap();
        let deserialized: PersistedTask = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.id, task.id);
        assert!(pretty.contains('\n')); // Pretty format has newlines
    }

    // ========== PersistedTask traits ==========

    #[test]
    fn test_persisted_task_clone() {
        let task = PersistedTask {
            id: "clone-test".to_string(),
            name: "clone.txt".to_string(),
            protocol: PersistedProtocol::Xunlei,
            size: 500,
            downloaded: 100,
            state: PersistedState::Downloading,
            error: Some("error".to_string()),
            speed_bps: 50.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["tag".to_string()],
            priority: DownloadPriority::Low,
            schedule: None,
            bandwidth_weight: 2,
            queue_position: Some(3),
            depends_on: vec!["dep".to_string()],
            group: Some("grp".to_string()),
            speed_limit_bps: Some(512),
            auto_retry_count: 1,
            retry_after: None,
            source_url: Some("http://src.com".to_string()),
            expected_checksum: None,
            checksum_algorithm: None,
            mirror_urls: vec!["http://m.com".to_string()],
            retry_policy: None,
            sequential_mode: true,
            notes: Some("notes".to_string()),
            max_download_time_secs: Some(1800),
            staleness_promotion_count: 0,
            deadline: None,
        };
        let cloned = task.clone();
        assert_eq!(cloned.id, task.id);
        assert_eq!(cloned.name, task.name);
        assert_eq!(cloned.size, task.size);
        assert_eq!(cloned.error, task.error);
    }

    #[test]
    fn test_persisted_task_clone_independence() {
        let mut task = PersistedTask {
            id: "independence".to_string(),
            name: "orig.txt".to_string(),
            protocol: PersistedProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: PersistedState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            mirror_urls: Vec::new(),
            retry_policy: None,
            sequential_mode: false,
            notes: None,
            max_download_time_secs: None,
            staleness_promotion_count: 0,
            deadline: None,
        };
        let cloned = task.clone();
        task.name = "modified.txt".to_string();
        task.size = 999;
        assert_eq!(cloned.name, "orig.txt");
        assert_eq!(cloned.size, 100);
    }

    #[test]
    fn test_persisted_task_debug() {
        let task = PersistedTask {
            id: "debug-test".to_string(),
            name: "debug.txt".to_string(),
            protocol: PersistedProtocol::Magnet,
            size: 100,
            downloaded: 0,
            state: PersistedState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            mirror_urls: Vec::new(),
            retry_policy: None,
            sequential_mode: false,
            notes: None,
            max_download_time_secs: None,
            staleness_promotion_count: 0,
            deadline: None,
        };
        let debug_str = format!("{:?}", task);
        assert!(debug_str.contains("PersistedTask"));
        assert!(debug_str.contains("debug-test"));
    }

    // ========== TaskQueueError additional tests ==========

    #[test]
    fn test_task_queue_error_debug() {
        let e = TaskQueueError::Io("disk full".to_string());
        let debug = format!("{:?}", e);
        assert!(debug.contains("Io"));

        let e2 = TaskQueueError::Serialize("invalid".to_string());
        let debug2 = format!("{:?}", e2);
        assert!(debug2.contains("Serialize"));

        let e3 = TaskQueueError::Deserialize("corrupt".to_string());
        let debug3 = format!("{:?}", e3);
        assert!(debug3.contains("Deserialize"));
    }

    #[test]
    fn test_task_queue_error_unicode() {
        let e = TaskQueueError::Io("权限被拒绝".to_string());
        assert!(format!("{}", e).contains("权限被拒绝"));

        let e2 = TaskQueueError::Serialize("無効なデータ".to_string());
        assert!(format!("{}", e2).contains("無効なデータ"));

        let e3 = TaskQueueError::Deserialize("🔥错误🔥".to_string());
        assert!(format!("{}", e3).contains("🔥错误🔥"));
    }

    #[test]
    fn test_task_queue_error_is_error_trait() {
        let e: Box<dyn std::error::Error> =
            Box::new(TaskQueueError::Io("permission denied".to_string()));
        assert!(e.to_string().contains("IO error"));
    }

    // ========== Boundary value tests ==========

    #[test]
    fn test_task_with_u64_max_size() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "max-size".to_string(),
            name: "huge.bin".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: u64::MAX,
            downloaded: u64::MAX / 2,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded[0].size, u64::MAX);
        assert_eq!(loaded[0].downloaded, u64::MAX / 2);
    }

    #[test]
    fn test_task_with_zero_size() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "zero-size".to_string(),
            name: "empty.bin".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 0,
            downloaded: 0,
            state: DownloadState::Complete,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded[0].size, 0);
        assert_eq!(loaded[0].downloaded, 0);
    }

    #[test]
    fn test_task_with_max_bandwidth_weight() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "max-weight".to_string(),
            name: "weighted.txt".to_string(),
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
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: u8::MAX,
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
        assert_eq!(loaded[0].bandwidth_weight, u8::MAX);
    }

    #[test]
    fn test_task_with_many_tags() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let tags: Vec<String> = (0..100).map(|i| format!("tag_{}", i)).collect();
        let task = DownloadTask {
            id: "many-tags".to_string(),
            name: "tagged.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: tags.clone(),
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded[0].tags.len(), 100);
        assert_eq!(loaded[0].tags[99], "tag_99");
    }

    #[test]
    fn test_task_with_many_dependencies() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let deps: Vec<String> = (0..50).map(|i| format!("dep_{}", i)).collect();
        let task = DownloadTask {
            id: "many-deps".to_string(),
            name: "dependent.txt".to_string(),
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
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: deps.clone(),
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
        assert_eq!(loaded[0].depends_on.len(), 50);
        assert_eq!(loaded[0].depends_on[49], "dep_49");
    }

    #[test]
    fn test_task_with_many_mirror_urls() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let mirrors: Vec<String> = (0..30)
            .map(|i| format!("http://mirror{}.com/file", i))
            .collect();
        let task = DownloadTask {
            id: "many-mirrors".to_string(),
            name: "mirrored.txt".to_string(),
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
            mirror_urls: mirrors.clone(),
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
        assert_eq!(loaded[0].mirror_urls.len(), 30);
    }

    // ========== Unicode edge cases ==========

    #[test]
    fn test_unicode_task_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "中文任务ID_日本語_한국어_🔥🎉".to_string(),
            name: "unicode.txt".to_string(),
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded[0].id, "中文任务ID_日本語_한국어_🔥🎉");
    }

    #[test]
    fn test_unicode_save_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "unicode-path".to_string(),
            name: "file.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: PathBuf::from("/下载/中文路径/🔥"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded[0].save_path, PathBuf::from("/下载/中文路径/🔥"));
    }

    #[test]
    fn test_unicode_group() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "unicode-group".to_string(),
            name: "file.txt".to_string(),
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
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: Some("分组_グループ_그룹🎯".to_string()),
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
        assert_eq!(loaded[0].group, Some("分组_グループ_그룹🎯".to_string()));
    }

    // ========== Complex workflow tests ==========

    #[test]
    fn test_complete_lifecycle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        // Create tasks in various states
        let tasks = vec![
            DownloadTask {
                id: "lifecycle-1".to_string(),
                name: "queued.txt".to_string(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: PathBuf::from("/tmp"),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                tags: vec!["batch1".to_string()],
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
                queue_position: Some(1),
                depends_on: Vec::new(),
                notes: None,
                group: Some("test-group".to_string()),
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: Some("http://example.com/1".to_string()),
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
                id: "lifecycle-2".to_string(),
                name: "downloading.txt".to_string(),
                protocol: DownloadProtocol::Ed2k,
                size: 2000,
                downloaded: 500,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 100.0,
                save_path: PathBuf::from("/tmp"),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                tags: vec!["batch1".to_string(), "active".to_string()],
                priority: DownloadPriority::High,
                schedule: None,
                bandwidth_weight: 5,
                queue_position: Some(2),
                depends_on: vec!["lifecycle-1".to_string()],
                notes: Some("Important download".to_string()),
                group: Some("test-group".to_string()),
                speed_limit_bps: Some(1024),
                auto_retry_count: 1,
                retry_after: None,
                source_url: Some("ed2k://...".to_string()),
                expected_checksum: Some("md5:abc".to_string()),
                checksum_algorithm: None,
                active_time_seconds: 60.0,
                current_session_start: None,
                mirror_urls: vec!["http://mirror.com".to_string()],
                retry_policy: None,
                cooldown: None,
                sequential_mode: true,
                max_download_time_secs: Some(3600),
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: Some(Utc::now()),
            },
            DownloadTask {
                id: "lifecycle-3".to_string(),
                name: "completed.txt".to_string(),
                protocol: DownloadProtocol::Xunlei,
                size: 3000,
                downloaded: 3000,
                state: DownloadState::Complete,
                error: None,
                speed_bps: 0.0,
                save_path: PathBuf::from("/tmp"),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                tags: vec!["batch1".to_string(), "done".to_string()],
                priority: DownloadPriority::Low,
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
                active_time_seconds: 300.0,
                current_session_start: None,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 2,
                deadline: None,
            },
        ];

        // Save
        save_task_queue(&tasks, data_dir).unwrap();

        // Load and verify all fields
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded.len(), 3);

        // Verify first task
        assert_eq!(loaded[0].id, "lifecycle-1");
        assert_eq!(loaded[0].state, DownloadState::Queued);
        assert_eq!(loaded[0].tags, vec!["batch1".to_string()]);
        assert_eq!(loaded[0].queue_position, Some(1));

        // Verify second task
        assert_eq!(loaded[1].id, "lifecycle-2");
        assert_eq!(loaded[1].state, DownloadState::Downloading);
        assert_eq!(loaded[1].downloaded, 500);
        assert_eq!(loaded[1].depends_on, vec!["lifecycle-1".to_string()]);
        assert_eq!(loaded[1].sequential_mode, true);
        assert_eq!(loaded[1].max_download_time_secs, Some(3600));

        // Verify third task
        assert_eq!(loaded[2].id, "lifecycle-3");
        assert_eq!(loaded[2].state, DownloadState::Complete);
        assert_eq!(loaded[2].staleness_promotion_count, 2);
    }

    #[test]
    fn test_save_load_multiple_times() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        // First save
        let task1 = DownloadTask {
            id: "multi-save-1".to_string(),
            name: "first.txt".to_string(),
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
        };
        save_task_queue(&[task1], data_dir).unwrap();

        // Load, modify, save again
        let mut loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded.len(), 1);
        loaded[0].downloaded = 50;
        loaded[0].state = DownloadState::Downloading;
        save_task_queue(&loaded, data_dir).unwrap();

        // Load again and verify
        let loaded2 = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded2.len(), 1);
        assert_eq!(loaded2[0].downloaded, 50);
        assert_eq!(loaded2[0].state, DownloadState::Downloading);
    }

    #[test]
    fn test_all_priorities_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let priorities = vec![
            DownloadPriority::Low,
            DownloadPriority::Normal,
            DownloadPriority::High,
        ];

        let tasks: Vec<DownloadTask> = priorities
            .iter()
            .enumerate()
            .map(|(i, p)| DownloadTask {
                id: format!("priority-{}", i),
                name: format!("file{}.txt", i),
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
                priority: *p,
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

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].priority, DownloadPriority::Low);
        assert_eq!(loaded[1].priority, DownloadPriority::Normal);
        assert_eq!(loaded[2].priority, DownloadPriority::High);
    }

    #[test]
    fn test_long_task_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let long_id = "a".repeat(1000);
        let task = DownloadTask {
            id: long_id.clone(),
            name: "long-id.txt".to_string(),
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded[0].id.len(), 1000);
        assert_eq!(loaded[0].id, long_id);
    }

    #[test]
    fn test_special_characters_in_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "special-error".to_string(),
            name: "file.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Error,
            error: Some(
                "Error: \"connection\" failed\nRetry with \\backslash\\ and /slash/ and 中文"
                    .to_string(),
            ),
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
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
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();
        assert!(loaded[0].error.as_ref().unwrap().contains('"'));
        assert!(loaded[0].error.as_ref().unwrap().contains('\n'));
        assert!(loaded[0].error.as_ref().unwrap().contains('\\'));
        assert!(loaded[0].error.as_ref().unwrap().contains("中文"));
    }

    #[test]
    fn test_speed_bps_boundary_values() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "speed-boundary".to_string(),
            name: "speedy.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 1000,
            downloaded: 500,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: f64::MAX,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: Some(u64::MAX),
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
        assert_eq!(loaded[0].speed_bps, f64::MAX);
        assert_eq!(loaded[0].speed_limit_bps, Some(u64::MAX));
    }

    #[test]
    fn test_auto_retry_count_max() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "max-retry".to_string(),
            name: "retry.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Error,
            error: Some("Failed".to_string()),
            speed_bps: 0.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: u32::MAX,
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
        assert_eq!(loaded[0].auto_retry_count, u32::MAX);
    }

    #[test]
    fn test_staleness_promotion_count_max() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "max-staleness".to_string(),
            name: "stale.txt".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 100.0,
            save_path: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::High,
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
            staleness_promotion_count: u32::MAX,
            deadline: None,
        };

        save_task_queue(&[task], data_dir).unwrap();
        let loaded = load_task_queue(data_dir).unwrap();
        assert_eq!(loaded[0].staleness_promotion_count, u32::MAX);
    }

    #[test]
    fn test_empty_strings_in_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let task = DownloadTask {
            id: "".to_string(),
            name: "".to_string(),
            protocol: DownloadProtocol::Torrent,
            size: 100,
            downloaded: 0,
            state: DownloadState::Queued,
            error: Some("".to_string()),
            speed_bps: 0.0,
            save_path: PathBuf::from(""),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["".to_string()],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: vec!["".to_string()],
            notes: Some("".to_string()),
            group: Some("".to_string()),
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some("".to_string()),
            expected_checksum: Some("".to_string()),
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            current_session_start: None,
            mirror_urls: vec!["".to_string()],
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
        assert_eq!(loaded[0].id, "");
        assert_eq!(loaded[0].name, "");
        assert_eq!(loaded[0].error, Some("".to_string()));
        assert_eq!(loaded[0].tags, vec!["".to_string()]);
        assert_eq!(loaded[0].notes, Some("".to_string()));
        assert_eq!(loaded[0].group, Some("".to_string()));
    }
}
