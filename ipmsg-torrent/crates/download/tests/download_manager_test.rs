//! Integration tests for download manager lifecycle

use ipmsg_download::{DownloadManager, DownloadState};
use tempfile::TempDir;

#[tokio::test]
async fn test_download_manager_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    // Initially empty
    let tasks = manager.list_tasks().await;
    assert!(tasks.is_empty());

    // Add a URL download
    let task_id = manager
        .add_url("https://httpbin.org/robots.txt")
        .await
        .unwrap();
    assert!(!task_id.is_empty());

    // Should have one task
    let tasks = manager.list_tasks().await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id, task_id);
    assert_eq!(tasks[0].state, DownloadState::Queued);

    // Pause the task
    let paused = manager.pause_task(&task_id).await;
    assert!(paused);

    let tasks = manager.list_tasks().await;
    assert_eq!(tasks[0].state, DownloadState::Paused);

    // Resume the task
    let resumed = manager.resume_task(&task_id).await;
    assert!(resumed);

    let tasks = manager.list_tasks().await;
    assert!(matches!(
        tasks[0].state,
        DownloadState::Queued | DownloadState::Downloading
    ));

    // Remove the task
    let removed = manager.remove_task(&task_id).await;
    assert!(removed);

    let tasks = manager.list_tasks().await;
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn test_download_manager_multiple_tasks() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    // Add multiple tasks
    let id1 = manager
        .add_url("https://httpbin.org/robots.txt")
        .await
        .unwrap();
    let id2 = manager
        .add_url("https://httpbin.org/headers")
        .await
        .unwrap();
    let id3 = manager.add_url("https://httpbin.org/ip").await.unwrap();

    let tasks = manager.list_tasks().await;
    assert_eq!(tasks.len(), 3);

    // Pause one
    manager.pause_task(&id2).await;

    let tasks = manager.list_tasks().await;
    let paused_count = tasks
        .iter()
        .filter(|t| t.state == DownloadState::Paused)
        .count();
    assert_eq!(paused_count, 1);

    // Remove all
    manager.remove_task(&id1).await;
    manager.remove_task(&id2).await;
    manager.remove_task(&id3).await;

    let tasks = manager.list_tasks().await;
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn test_download_manager_speed_limit() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    // Set global speed limit
    manager.set_global_speed_limit(1_000_000).await; // 1 MB/s

    // Add a task
    let task_id = manager
        .add_url("https://httpbin.org/robots.txt")
        .await
        .unwrap();

    // Set per-task speed limit
    manager.set_task_speed_limit(500_000).await; // 500 KB/s

    let tasks = manager.list_tasks().await;
    assert_eq!(tasks.len(), 1);
}

#[tokio::test]
async fn test_download_manager_dashboard() {
    let temp_dir = TempDir::new().unwrap();
    let manager = DownloadManager::new(temp_dir.path().to_path_buf());

    // Add some tasks
    manager
        .add_url("https://httpbin.org/robots.txt")
        .await
        .unwrap();
    manager
        .add_url("https://httpbin.org/headers")
        .await
        .unwrap();

    // Generate dashboard
    let dashboard = manager.generate_dashboard().await;
    assert_eq!(dashboard.queue_status.total, 2);
    assert_eq!(dashboard.queue_status.queued, 2);
}
