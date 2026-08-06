//! End-to-end test: download a small file via HTTP using Xunlei engine

use ipmsg_download::xunlei::{XunleiEngine, XunleiSource};

#[tokio::test]
async fn test_http_download_small_file() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let download_dir = tmp_dir.path().to_path_buf();

    // Use httpbin to serve a known response
    let url = "https://httpbin.org/robots.txt";
    let file_name = "robots.txt".to_string();

    let sources = vec![XunleiSource::Http {
        url: url.to_string(),
        cookies: None,
        referer: None,
    }];

    // First, get the actual content to know expected size
    let expected = reqwest::get(url).await.unwrap().text().await.unwrap();
    let file_size = expected.len() as u64;

    let mut engine = XunleiEngine::new(file_name.clone(), file_size, sources, download_dir.clone());
    let result = engine.download(None).await;

    assert!(result.is_ok(), "Download failed: {:?}", result.err());

    // Verify file exists and has correct content
    let output_path = download_dir.join(&file_name);
    assert!(
        output_path.exists(),
        "Output file not found: {}",
        output_path.display()
    );

    let actual = tokio::fs::read_to_string(&output_path).await.unwrap();
    assert_eq!(actual, expected, "File content mismatch");

    println!(
        "✓ Downloaded {} bytes to {}",
        file_size,
        output_path.display()
    );
}

#[tokio::test]
async fn test_download_manager_add_url() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();

    let manager = ipmsg_download::DownloadManager::new(data_dir.clone());

    // Test add_url with a known URL
    let url = "https://httpbin.org/robots.txt";
    let task_id = manager.add_url(url).await.unwrap();

    // Wait for download to complete (with timeout)
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > std::time::Duration::from_secs(30) {
            panic!("Download timed out");
        }

        let tasks = manager.list_tasks().await;
        let task = tasks.iter().find(|t| t.id == task_id).unwrap();

        match task.state {
            ipmsg_download::DownloadState::Complete => break,
            ipmsg_download::DownloadState::Error => {
                panic!(
                    "Download failed: {}",
                    task.error.as_deref().unwrap_or("unknown")
                );
            }
            _ => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }

    // Verify file
    let output_path = data_dir.join("downloads").join("robots.txt");
    assert!(output_path.exists(), "Downloaded file not found");

    let content = tokio::fs::read_to_string(&output_path).await.unwrap();
    assert!(
        content.contains("User-agent"),
        "Unexpected content: {}",
        content
    );

    println!("✓ DownloadManager::add_url succeeded, task state: complete");
}

#[tokio::test]
async fn test_download_manager_pause_resume() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();

    let manager = ipmsg_download::DownloadManager::new(data_dir.clone());

    // Use a slow/large URL to give us time to pause
    let url = "https://speed.cloudflare.com/__down?bytes=1000000";
    let task_id = manager.add_url(url).await.unwrap();

    // Wait a bit for download to start
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Check if already completed (file too small/fast)
    let tasks = manager.list_tasks().await;
    let task = tasks.iter().find(|t| t.id == task_id).unwrap();
    if task.state == ipmsg_download::DownloadState::Complete {
        println!("  (file downloaded before pause - too fast to pause)");
        return;
    }

    // Pause
    let paused = manager.pause_task(&task_id).await;
    assert!(paused, "Failed to pause task");

    let tasks = manager.list_tasks().await;
    let task = tasks.iter().find(|t| t.id == task_id).unwrap();
    assert_eq!(task.state, ipmsg_download::DownloadState::Paused);

    // Resume
    let resumed = manager.resume_task(&task_id).await;
    assert!(resumed, "Failed to resume task");

    // Wait for completion
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > std::time::Duration::from_secs(60) {
            panic!("Resume + download timed out");
        }

        let tasks = manager.list_tasks().await;
        let task = tasks.iter().find(|t| t.id == task_id).unwrap();

        match task.state {
            ipmsg_download::DownloadState::Complete => break,
            ipmsg_download::DownloadState::Error => {
                panic!(
                    "Download failed after resume: {}",
                    task.error.as_deref().unwrap_or("unknown")
                );
            }
            _ => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }

    println!("✓ Pause/resume cycle succeeded");
}
