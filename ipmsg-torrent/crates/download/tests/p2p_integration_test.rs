//! Integration tests for P2P download management

use ipmsg_download::{DownloadManager, DownloadProtocol, DownloadState};
use tempfile::TempDir;

#[tokio::test]
async fn test_add_p2p_download() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    let task_id = manager
        .add_p2p(
            "abc123hash".to_string(),
            "video.mp4".to_string(),
            1024 * 1024,
            "peer_abc".to_string(),
        )
        .await
        .unwrap();

    let tasks = manager.list_tasks().await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id, task_id);
    assert_eq!(tasks[0].name, "video.mp4");
    assert_eq!(tasks[0].protocol, DownloadProtocol::P2P);
    assert_eq!(tasks[0].size, 1024 * 1024);
    assert_eq!(tasks[0].state, DownloadState::Downloading);
}

#[tokio::test]
async fn test_update_p2p_progress() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    let task_id = manager
        .add_p2p(
            "hash123".to_string(),
            "file.txt".to_string(),
            1000,
            "peer1".to_string(),
        )
        .await
        .unwrap();

    // Update progress
    let updated = manager.update_p2p_progress(&task_id, 500, 1024.0).await;
    assert!(updated);

    let task = manager.get_task(&task_id).await.unwrap();
    assert_eq!(task.downloaded, 500);
    assert_eq!(task.speed_bps, 1024.0);
}

#[tokio::test]
async fn test_update_p2p_progress_nonexistent_task() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    let updated = manager
        .update_p2p_progress("nonexistent", 500, 1024.0)
        .await;
    assert!(!updated);
}

#[tokio::test]
async fn test_complete_p2p_download() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    let task_id = manager
        .add_p2p(
            "hash456".to_string(),
            "document.pdf".to_string(),
            2048,
            "peer_xyz".to_string(),
        )
        .await
        .unwrap();

    // Simulate receiving all data
    let data = vec![0xAB; 2048];
    let file_path = manager
        .complete_p2p_download(&task_id, data.clone())
        .await
        .unwrap();

    // Verify file was written
    assert!(file_path.exists());
    let written_data = tokio::fs::read(&file_path).await.unwrap();
    assert_eq!(written_data, data);

    // Verify task state
    let task = manager.get_task(&task_id).await.unwrap();
    assert_eq!(task.state, DownloadState::Complete);
    assert_eq!(task.downloaded, 2048);
    assert_eq!(task.speed_bps, 0.0);
}

#[tokio::test]
async fn test_complete_p2p_download_nonexistent_task() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    let data = vec![0xAB; 1024];
    let result = manager.complete_p2p_download("nonexistent", data).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_p2p_download_in_task_list() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    // Add multiple P2P downloads
    let task1 = manager
        .add_p2p(
            "hash1".to_string(),
            "file1.txt".to_string(),
            100,
            "peer1".to_string(),
        )
        .await
        .unwrap();
    let task2 = manager
        .add_p2p(
            "hash2".to_string(),
            "file2.txt".to_string(),
            200,
            "peer2".to_string(),
        )
        .await
        .unwrap();

    let tasks = manager.list_tasks().await;
    assert_eq!(tasks.len(), 2);
    assert!(tasks.iter().any(|t| t.id == task1));
    assert!(tasks.iter().any(|t| t.id == task2));
    assert!(tasks.iter().all(|t| t.protocol == DownloadProtocol::P2P));
}

#[tokio::test]
async fn test_p2p_download_progress_calculation() {
    use ipmsg_download::{DownloadPriority, DownloadTask};

    let task = DownloadTask {
        id: "test-id".to_string(),
        name: "test.txt".to_string(),
        protocol: DownloadProtocol::P2P,
        size: 1000,
        downloaded: 750,
        state: DownloadState::Downloading,
        error: None,
        speed_bps: 512.0,
        save_path: std::path::PathBuf::from("/tmp"),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        tags: Vec::new(),
        priority: DownloadPriority::Normal,
    };

    assert_eq!(task.progress(), 75.0);
}
