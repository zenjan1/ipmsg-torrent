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
    };

    assert_eq!(task.progress(), 0.0);
}
