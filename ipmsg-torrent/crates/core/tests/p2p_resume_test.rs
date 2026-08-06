//! Tests for P2P file download progress persistence and resume

use ipmsg_core::file_sharing::FileSharingManager;
use ipmsg_core::file_transfer::FileTransferManager;
use ipmsg_core::p2p_progress::{self, P2pDownloadSnapshot};
use ipmsg_protocol::message::FileRef;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Mutex;

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

fn make_file_sharing(dir: PathBuf) -> Arc<Mutex<FileSharingManager>> {
    Arc::new(Mutex::new(FileSharingManager::new(dir)))
}

#[tokio::test]
async fn test_p2p_download_resume_after_restart() {
    let temp_dir = TempDir::new().unwrap();
    let progress_dir = temp_dir.path().join("p2p_progress");
    let sharing_dir = temp_dir.path().join("shared");

    let file_ref = make_file_ref("resume_test.bin", 1024, 256); // 4 chunks
    let file_hash = file_ref.hash.clone();

    // Phase 1: Start download, receive 2 of 4 chunks, then "crash"
    {
        let sharing = make_file_sharing(sharing_dir.clone());
        let manager = FileTransferManager::new(sharing, progress_dir.clone());

        let hash = manager
            .start_download(file_ref.clone(), "peer1".to_string())
            .await;
        assert_eq!(hash, file_hash);

        // Receive chunk 0 and chunk 2
        let chunk0 = vec![0xAA; 256];
        let chunk2 = vec![0xCC; 256];
        let complete = manager.record_chunk(&file_hash, 0, chunk0.clone()).await;
        assert!(!complete);
        let complete = manager.record_chunk(&file_hash, 2, chunk2.clone()).await;
        assert!(!complete);

        // Verify progress
        let progress = manager.get_progress(&file_hash).await.unwrap();
        assert!(progress > 40.0 && progress < 60.0); // ~50%
    }

    // Phase 2: "Restart" — create new manager, restore from disk
    {
        let sharing = make_file_sharing(sharing_dir);
        let manager = FileTransferManager::new(sharing, progress_dir);

        let hash = manager
            .start_download(file_ref.clone(), "peer1".to_string())
            .await;
        assert_eq!(hash, file_hash);

        // Should have restored progress (~50%)
        let progress = manager.get_progress(&file_hash).await.unwrap();
        assert!(
            progress > 40.0,
            "Restored progress should be > 40%, got {}",
            progress
        );

        // Receive remaining chunks (1 and 3)
        let chunk1 = vec![0xBB; 256];
        let chunk3 = vec![0xDD; 256];
        let complete = manager.record_chunk(&file_hash, 1, chunk1).await;
        assert!(!complete);
        let complete = manager.record_chunk(&file_hash, 3, chunk3).await;
        assert!(complete, "Download should be complete after all chunks");

        // Should be able to assemble
        let data = manager.try_assemble(&file_hash).await;
        assert!(data.is_some());
        let data = data.unwrap();
        assert_eq!(data.len(), 1024);

        // Verify chunk data integrity
        assert_eq!(&data[0..256], &[0xAA; 256]);
        assert_eq!(&data[256..512], &[0xBB; 256]);
        assert_eq!(&data[512..768], &[0xCC; 256]);
        assert_eq!(&data[768..1024], &[0xDD; 256]);
    }
}

#[tokio::test]
async fn test_p2p_progress_cleanup_on_finish() {
    let temp_dir = TempDir::new().unwrap();
    let progress_dir = temp_dir.path().join("p2p_progress");
    let sharing_dir = temp_dir.path().join("shared");

    let file_ref = make_file_ref("cleanup_test.bin", 512, 256); // 2 chunks
    let file_hash = file_ref.hash.clone();

    let sharing = make_file_sharing(sharing_dir);
    let manager = FileTransferManager::new(sharing, progress_dir.clone());

    manager
        .start_download(file_ref.clone(), "peer1".to_string())
        .await;

    // Receive all chunks
    manager.record_chunk(&file_hash, 0, vec![0xAA; 256]).await;
    let complete = manager.record_chunk(&file_hash, 1, vec![0xBB; 256]).await;
    assert!(complete);

    // Progress file should exist before finish
    let progress_files = p2p_progress::list_progress(&progress_dir);
    assert_eq!(progress_files.len(), 1, "One progress file should exist before finish");

    // Finish download
    let result = manager.finish_download(&file_hash).await;
    assert!(result.is_some());

    // Progress file should be cleaned up
    let progress_files = p2p_progress::list_progress(&progress_dir);
    assert_eq!(progress_files.len(), 0, "Progress file should be removed after finish");
}

#[tokio::test]
async fn test_p2p_fresh_download_no_resume() {
    let temp_dir = TempDir::new().unwrap();
    let progress_dir = temp_dir.path().join("p2p_progress");
    let sharing_dir = temp_dir.path().join("shared");

    let file_ref = make_file_ref("fresh_test.bin", 256, 256); // 1 chunk
    let file_hash = file_ref.hash.clone();

    let sharing = make_file_sharing(sharing_dir);
    let manager = FileTransferManager::new(sharing, progress_dir);

    // No saved progress, should start from 0
    manager
        .start_download(file_ref.clone(), "peer1".to_string())
        .await;
    let progress = manager.get_progress(&file_hash).await.unwrap();
    assert_eq!(progress, 0.0);
}

#[test]
fn test_p2p_progress_snapshot_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let progress_dir = temp_dir.path();

    let mut snapshot = P2pDownloadSnapshot::new(
        "test_hash_123".to_string(),
        "test_file.bin".to_string(),
        4096,
        1024,
        4,
        "peer_abc".to_string(),
    );

    snapshot.mark_received(0, 1024);
    snapshot.mark_received(2, 1024);
    assert_eq!(snapshot.progress(), 50.0);
    assert!(!snapshot.is_complete());

    // Save
    p2p_progress::save_progress(progress_dir, &snapshot).unwrap();

    // Load
    let loaded = p2p_progress::load_progress(progress_dir, "test_hash_123").unwrap();
    assert_eq!(loaded.file_hash, "test_hash_123");
    assert_eq!(loaded.file_name, "test_file.bin");
    assert_eq!(loaded.file_size, 4096);
    assert_eq!(loaded.total_chunks, 4);
    assert_eq!(loaded.received_chunks.len(), 2);
    assert!(loaded.received_chunks.contains(&0));
    assert!(loaded.received_chunks.contains(&2));
    assert_eq!(loaded.bytes_received, 2048);
    assert_eq!(loaded.owner, "peer_abc");
}
