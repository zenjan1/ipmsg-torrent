//! Integration tests for download engines

use ipmsg_download::ed2k::Ed2kFileHash;
use ipmsg_download::{DownloadManager, DownloadProtocol, DownloadState};
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
fn test_bencode_integer() {
    let data = b"i42e";
    let result = ipmsg_download::torrent::bencode::decode(data).unwrap();
    assert_eq!(
        result,
        ipmsg_download::torrent::bencode::Bencode::Integer(42)
    );
}

#[test]
fn test_bencode_bytes() {
    let data = b"5:hello";
    let result = ipmsg_download::torrent::bencode::decode(data).unwrap();
    assert_eq!(
        result,
        ipmsg_download::torrent::bencode::Bencode::Bytes(b"hello".to_vec())
    );
}

#[test]
fn test_bencode_list() {
    let data = b"li1ei2ei3ee";
    let result = ipmsg_download::torrent::bencode::decode(data).unwrap();
    match result {
        ipmsg_download::torrent::bencode::Bencode::List(items) => {
            assert_eq!(items.len(), 3);
        }
        _ => panic!("Expected list"),
    }
}

#[test]
fn test_bencode_dict() {
    let data = b"d3:cow3:moo4:spam3:egge";
    let result = ipmsg_download::torrent::bencode::decode(data).unwrap();
    match result {
        ipmsg_download::torrent::bencode::Bencode::Dict(map) => {
            assert_eq!(map.get("cow").unwrap().as_string().unwrap(), "moo");
            assert_eq!(map.get("spam").unwrap().as_string().unwrap(), "egg");
        }
        _ => panic!("Expected dict"),
    }
}

#[test]
fn test_ed2k_file_hash() {
    let hex = "31d6cfe0d16ae931b73c59d7e0c089c0";
    let hash = Ed2kFileHash::from_hex(hex).unwrap();
    assert_eq!(hash.to_hex(), hex);
}

#[test]
fn test_ed2k_file_hash_invalid() {
    let result = Ed2kFileHash::from_hex("invalid");
    assert!(result.is_err());
}

#[tokio::test]
async fn test_download_manager_creation() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());
    let tasks = manager.list_tasks().await;
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn test_download_manager_add_xunlei() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    let sources = vec![ipmsg_download::xunlei::XunleiSource::Http {
        url: "http://example.com/test.txt".to_string(),
        cookies: None,
        referer: None,
    }];

    let result = manager
        .add_xunlei("test.txt".to_string(), 1024, sources)
        .await;
    assert!(result.is_ok());

    let task_id = result.unwrap();
    let tasks = manager.list_tasks().await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id, task_id);
    assert_eq!(tasks[0].name, "test.txt");
    assert_eq!(tasks[0].protocol, DownloadProtocol::Xunlei);
}

#[tokio::test]
async fn test_download_manager_pause_resume() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    let sources = vec![ipmsg_download::xunlei::XunleiSource::Http {
        url: "http://example.com/test.txt".to_string(),
        cookies: None,
        referer: None,
    }];

    let task_id = manager
        .add_xunlei("test.txt".to_string(), 1024, sources)
        .await
        .unwrap();

    // Wait for spawned task to transition to Downloading
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Pause
    let paused = manager.pause_task(&task_id).await;
    assert!(paused);

    let task = manager.get_task(&task_id).await.unwrap();
    assert_eq!(task.state, DownloadState::Paused);

    // Resume
    let resumed = manager.resume_task(&task_id).await;
    assert!(resumed);

    let task = manager.get_task(&task_id).await.unwrap();
    assert_eq!(task.state, DownloadState::Downloading);
}

#[tokio::test]
async fn test_download_manager_remove_task() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    let sources = vec![ipmsg_download::xunlei::XunleiSource::Http {
        url: "http://example.com/test.txt".to_string(),
        cookies: None,
        referer: None,
    }];

    let task_id = manager
        .add_xunlei("test.txt".to_string(), 1024, sources)
        .await
        .unwrap();

    let removed = manager.remove_task(&task_id).await;
    assert!(removed);

    let tasks = manager.list_tasks().await;
    assert!(tasks.is_empty());
}

#[test]
fn test_download_task_progress() {
    use ipmsg_download::{DownloadPriority, DownloadTask};

    let task = DownloadTask {
        id: "test-id".to_string(),
        name: "test.txt".to_string(),
        protocol: DownloadProtocol::Xunlei,
        size: 1000,
        downloaded: 500,
        state: DownloadState::Downloading,
        error: None,
        speed_bps: 0.0,
        save_path: PathBuf::from("/tmp"),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
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

    assert_eq!(task.progress(), 50.0);
}

#[test]
fn test_download_task_progress_zero_size() {
    use ipmsg_download::{DownloadPriority, DownloadTask};

    let task = DownloadTask {
        id: "test-id".to_string(),
        name: "test.txt".to_string(),
        protocol: DownloadProtocol::Xunlei,
        size: 0,
        downloaded: 0,
        state: DownloadState::Queued,
        error: None,
        speed_bps: 0.0,
        save_path: PathBuf::from("/tmp"),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
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

    assert_eq!(task.progress(), 0.0);
}

// ========== Phase 148: Task Activity REST API Tests ==========

#[tokio::test]
async fn test_task_activity_get_all_summaries() {
    let tmp = TempDir::new().unwrap();
    let dm = DownloadManager::new(tmp.path().to_path_buf());

    // Initially empty
    let summaries = dm.get_all_activity_summaries().await;
    assert!(summaries.is_empty(), "Should start with no activity logs");
}

#[tokio::test]
async fn test_task_activity_log_and_retrieve() {
    let tmp = TempDir::new().unwrap();
    let dm = DownloadManager::new(tmp.path().to_path_buf());

    let task_id = "test-task-123";
    let task_name = "Test Task";

    // Log some activity
    dm.log_task_activity(
        task_id,
        task_name,
        ipmsg_download::task_activity::ActivityEventType::Created,
        "Task created",
    )
    .await;

    dm.log_task_activity(
        task_id,
        task_name,
        ipmsg_download::task_activity::ActivityEventType::Started,
        "Task started",
    )
    .await;

    // Retrieve activity log
    let log = dm.get_task_activity(task_id).await;
    assert!(log.is_some(), "Should find activity log");
    let log = log.unwrap();
    assert_eq!(log.task_id, task_id);
    assert_eq!(log.task_name, task_name);

    // Check summaries
    let summaries = dm.get_all_activity_summaries().await;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].task_id, task_id);
    assert_eq!(summaries[0].total_events, 2);
}

#[tokio::test]
async fn test_task_activity_clear() {
    let tmp = TempDir::new().unwrap();
    let dm = DownloadManager::new(tmp.path().to_path_buf());

    let task_id = "test-task-456";

    // Log activity
    dm.log_task_activity(
        task_id,
        "Test Task",
        ipmsg_download::task_activity::ActivityEventType::Created,
        "Created",
    )
    .await;

    // Clear activity
    dm.clear_task_activity(task_id).await;

    // Should be empty
    let log = dm.get_task_activity(task_id).await;
    assert!(log.is_none() || log.unwrap().events().count() == 0);
}

#[tokio::test]
async fn test_task_activity_with_value() {
    let tmp = TempDir::new().unwrap();
    let dm = DownloadManager::new(tmp.path().to_path_buf());

    let task_id = "test-task-789";

    dm.log_task_activity_with_value(
        task_id,
        "Test Task",
        ipmsg_download::task_activity::ActivityEventType::ProgressMilestone,
        "Progress: 50%",
        50.0,
    )
    .await;

    let log = dm.get_task_activity(task_id).await.unwrap();
    let events: Vec<_> = log.events().collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].numeric_value, Some(50.0));
}

#[tokio::test]
async fn test_task_activity_remove() {
    let tmp = TempDir::new().unwrap();
    let dm = DownloadManager::new(tmp.path().to_path_buf());

    let task_id = "test-task-remove";

    dm.log_task_activity(
        task_id,
        "Test Task",
        ipmsg_download::task_activity::ActivityEventType::Created,
        "Created",
    )
    .await;

    dm.remove_task_activity(task_id).await;

    let log = dm.get_task_activity(task_id).await;
    assert!(log.is_none());
}
