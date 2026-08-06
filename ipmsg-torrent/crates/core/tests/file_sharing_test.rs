//! Tests for P2P file sharing functionality
//! Tests the file sharing protocol handling including announcements, searches, and responses

use ipmsg_core::file_sharing::FileSharingManager;
use ipmsg_protocol::message::{FileRef, FileShareInfo};
use std::path::PathBuf;
use tempfile::TempDir;

fn make_file_ref(name: &str, size: u64, chunk_size: u32) -> FileRef {
    let chunks = ((size as f64) / (chunk_size as f64)).ceil() as u32;
    FileRef {
        hash: format!("hash_{}", name),
        name: name.to_string(),
        size,
        mime_type: "application/octet-stream".to_string(),
        chunks,
        chunk_size,
        thumbnail: None,
    }
}

fn make_file_share_info(name: &str, owner: &str, tags: Vec<&str>) -> FileShareInfo {
    let file_ref = make_file_ref(name, 1024, 256);
    FileShareInfo {
        file_ref,
        owner: owner.to_string(),
        tags: tags.into_iter().map(|s| s.to_string()).collect(),
        description: Some(format!("Test file: {}", name)),
        created_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn test_file_sharing_manager_creation() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());
    assert_eq!(manager.shared_count().await, 0);
    assert_eq!(manager.discovered_count().await, 0);
}

#[tokio::test]
async fn test_process_announce_stores_discovered_files() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());

    let info1 = make_file_share_info("file1.txt", "peer1", vec!["test"]);
    let info2 = make_file_share_info("file2.txt", "peer2", vec!["test"]);

    manager
        .process_announce(&[info1.clone(), info2.clone()])
        .await;

    assert_eq!(manager.discovered_count().await, 2);
    let discovered = manager.list_discovered_files().await;
    assert!(discovered.iter().any(|f| f.file_ref.name == "file1.txt"));
    assert!(discovered.iter().any(|f| f.file_ref.name == "file2.txt"));
}

#[tokio::test]
async fn test_search_discovered_files() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());

    let info1 = FileShareInfo {
        file_ref: make_file_ref("video.mp4", 1024 * 1024, 64 * 1024),
        owner: "peer1".to_string(),
        tags: vec!["video".to_string(), "mp4".to_string()],
        description: Some("A test video".to_string()),
        created_at: chrono::Utc::now(),
    };

    let info2 = FileShareInfo {
        file_ref: make_file_ref("document.pdf", 512 * 1024, 64 * 1024),
        owner: "peer2".to_string(),
        tags: vec!["document".to_string(), "pdf".to_string()],
        description: Some("A test document".to_string()),
        created_at: chrono::Utc::now(),
    };

    manager
        .process_announce(&[info1.clone(), info2.clone()])
        .await;

    // Search by name
    let results = manager.search("video", &[]).await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].file_ref.name, "video.mp4");

    // Search by tag
    let results = manager.search("", &["pdf".to_string()]).await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].file_ref.name, "document.pdf");

    // Search by description (both have "test" in description)
    let results = manager.search("test", &[]).await;
    assert_eq!(results.len(), 2);

    // Search with no match
    let results = manager.search("nonexistent", &[]).await;
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_search_shared_files() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());

    // Create a test file to share
    let test_file = temp_dir.path().join("test_shared.txt");
    std::fs::write(&test_file, b"Hello, world!").unwrap();

    let info = manager
        .share_file(
            &test_file,
            vec!["text".to_string()],
            Some("A shared text file".to_string()),
            "local_peer".to_string(),
        )
        .await
        .unwrap();

    assert_eq!(manager.shared_count().await, 1);

    // Search for the shared file
    let results = manager.search("shared", &[]).await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].file_ref.name, "test_shared.txt");
}

#[tokio::test]
async fn test_unshare_file() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());

    let test_file = temp_dir.path().join("to_unshare.txt");
    std::fs::write(&test_file, b"temp content").unwrap();

    let info = manager
        .share_file(&test_file, vec![], None, "local_peer".to_string())
        .await
        .unwrap();

    assert_eq!(manager.shared_count().await, 1);

    let removed = manager.unshare_file(&info.file_ref.hash).await;
    assert!(removed);
    assert_eq!(manager.shared_count().await, 0);
}

#[tokio::test]
async fn test_read_chunk() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());

    // Create a test file with known content
    let test_file = temp_dir.path().join("chunked.bin");
    let content: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
    std::fs::write(&test_file, &content).unwrap();

    let info = manager
        .share_file(&test_file, vec![], None, "local_peer".to_string())
        .await
        .unwrap();

    // Read first chunk (chunk index 0)
    let chunk0 = manager.read_chunk(&info.file_ref.hash, 0).await.unwrap();
    assert!(!chunk0.is_empty());
    // File is 1000 bytes, chunk_size is 256KB (from FileRef::new), so chunk 0 = entire file
    assert_eq!(chunk0.len(), content.len());
    assert_eq!(&chunk0[..10], &content[..10]);
}

#[tokio::test]
async fn test_process_announce_updates_existing() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());

    let mut info = make_file_share_info("file.txt", "peer1", vec!["test"]);
    manager.process_announce(&[info.clone()]).await;
    assert_eq!(manager.discovered_count().await, 1);

    // Update the same file (same hash)
    info.description = Some("Updated description".to_string());
    manager.process_announce(&[info.clone()]).await;

    // Should still be 1 file, but with updated info
    assert_eq!(manager.discovered_count().await, 1);
    let discovered = manager.list_discovered_files().await;
    assert_eq!(
        discovered[0].description.as_ref().unwrap(),
        "Updated description"
    );
}

#[tokio::test]
async fn test_search_empty_query_matches_all() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());

    let info1 = make_file_share_info("file1.txt", "peer1", vec!["tag1"]);
    let info2 = make_file_share_info("file2.txt", "peer2", vec!["tag2"]);

    manager.process_announce(&[info1, info2]).await;

    // Empty query with empty tags should match all
    let results = manager.search("", &[]).await;
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_search_case_insensitive() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileSharingManager::new(temp_dir.path().to_path_buf());

    let info = FileShareInfo {
        file_ref: make_file_ref("MyDocument.TXT", 1024, 256),
        owner: "peer1".to_string(),
        tags: vec!["Document".to_string()],
        description: Some("Important File".to_string()),
        created_at: chrono::Utc::now(),
    };

    manager.process_announce(&[info]).await;

    // Case-insensitive name search
    let results = manager.search("mydocument", &[]).await;
    assert_eq!(results.len(), 1);

    // Case-insensitive tag search
    let results = manager.search("", &["document".to_string()]).await;
    assert_eq!(results.len(), 1);

    // Case-insensitive description search
    let results = manager.search("important", &[]).await;
    assert_eq!(results.len(), 1);
}
