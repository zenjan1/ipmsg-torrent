//! Test ed2k download resume (progress persistence)

use ipmsg_download::ed2k::{Ed2kEngine, Ed2kFileHash};
use ipmsg_download::progress::{self, ProgressSnapshot};
use std::net::SocketAddr;

/// Helper: create an ed2k engine with no servers (won't actually connect)
fn make_engine(hash: Ed2kFileHash, size: u64, name: &str, dir: &std::path::Path) -> Ed2kEngine {
    let servers: Vec<SocketAddr> = vec![];
    Ed2kEngine::new(hash, size, name.to_string(), dir.to_path_buf(), servers)
}

/// Build a 20-byte progress hash from a 16-byte MD4 ed2k hash
fn progress_hash(md4: &Ed2kFileHash) -> [u8; 20] {
    let mut h = [0u8; 20];
    h[..16].copy_from_slice(&md4.0);
    h
}

#[test]
fn test_ed2k_new_engine_no_progress() {
    let tmp = tempfile::tempdir().unwrap();
    let hash = Ed2kFileHash([0xAA; 16]);
    let size = 20_000_000; // ~2 chunks

    let engine = make_engine(hash.clone(), size, "ed2k_test.bin", tmp.path());

    // No progress file should exist yet
    let pp = progress::progress_path(tmp.path(), "ed2k_test.bin");
    assert!(!pp.exists());

    // Engine should report 0 downloaded chunks
    assert_eq!(engine.downloaded_chunks_count(), 0);
    assert_eq!(engine.downloaded_bytes(), 0);
}

#[test]
fn test_ed2k_resume_from_saved_progress() {
    let tmp = tempfile::tempdir().unwrap();
    let hash = Ed2kFileHash([0xBB; 16]);
    let size = 20_000_000; // 2 chunks: 9_728_000 + 10_272_000
    let ph = progress_hash(&hash);

    // Save a progress snapshot marking chunk 0 as complete
    let mut snap = ProgressSnapshot::new(ph, size, 9_728_000, 2);
    snap.mark_complete(0);
    snap.downloaded = 9_728_000;
    progress::save_progress(tmp.path(), "ed2k_resume.bin", &snap).unwrap();

    // Create engine - should load the saved progress
    let engine = make_engine(hash, size, "ed2k_resume.bin", tmp.path());

    assert_eq!(
        engine.downloaded_chunks_count(),
        1,
        "Should resume with 1 chunk done"
    );
    assert_eq!(
        engine.downloaded_bytes(),
        9_728_000,
        "Should reflect downloaded bytes"
    );
}

#[test]
fn test_ed2k_resume_all_chunks_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let hash = Ed2kFileHash([0xCC; 16]);
    let size = 9_728_000; // exactly 1 chunk
    let ph = progress_hash(&hash);

    let mut snap = ProgressSnapshot::new(ph, size, 9_728_000, 1);
    snap.mark_complete(0);
    snap.downloaded = 9_728_000;
    progress::save_progress(tmp.path(), "ed2k_full.bin", &snap).unwrap();

    let engine = make_engine(hash, size, "ed2k_full.bin", tmp.path());
    assert_eq!(engine.downloaded_chunks_count(), 1);
    assert!(
        engine.is_download_complete(),
        "Engine with all chunks should report complete"
    );
}

#[test]
fn test_ed2k_progress_hash_mismatch_ignores_saved() {
    let tmp = tempfile::tempdir().unwrap();
    let hash_a = Ed2kFileHash([0xAA; 16]);
    let hash_b = Ed2kFileHash([0xBB; 16]);
    let size = 5_000_000;

    // Save progress for hash_a
    let ph_a = progress_hash(&hash_a);
    let mut snap = ProgressSnapshot::new(ph_a, size, 9_728_000, 1);
    snap.mark_complete(0);
    progress::save_progress(tmp.path(), "ed2k_mismatch.bin", &snap).unwrap();

    // Create engine with hash_b - should NOT load the saved progress
    let engine = make_engine(hash_b, size, "ed2k_mismatch.bin", tmp.path());
    assert_eq!(
        engine.downloaded_chunks_count(),
        0,
        "Hash mismatch should ignore saved progress"
    );
}

#[test]
fn test_ed2k_progress_size_mismatch_ignores_saved() {
    let tmp = tempfile::tempdir().unwrap();
    let hash = Ed2kFileHash([0xDD; 16]);
    let ph = progress_hash(&hash);

    // Save progress with size 10_000_000
    let mut snap = ProgressSnapshot::new(ph, 10_000_000, 9_728_000, 2);
    snap.mark_complete(0);
    progress::save_progress(tmp.path(), "ed2k_size.bin", &snap).unwrap();

    // Create engine with different size
    let engine = make_engine(hash, 5_000_000, "ed2k_size.bin", tmp.path());
    assert_eq!(
        engine.downloaded_chunks_count(),
        0,
        "Size mismatch should ignore saved progress"
    );
}

#[test]
fn test_ed2k_small_file_single_chunk() {
    let tmp = tempfile::tempdir().unwrap();
    let hash = Ed2kFileHash([0x11; 16]);
    let size = 1_000_000; // < 1 chunk
    let ph = progress_hash(&hash);

    let mut snap = ProgressSnapshot::new(ph, size, 9_728_000, 1);
    snap.mark_complete(0);
    snap.downloaded = 1_000_000;
    progress::save_progress(tmp.path(), "ed2k_small.bin", &snap).unwrap();

    let engine = make_engine(hash, size, "ed2k_small.bin", tmp.path());
    assert_eq!(engine.downloaded_chunks_count(), 1);
    assert_eq!(engine.downloaded_bytes(), 1_000_000);
    assert!(engine.is_download_complete());
}

#[test]
fn test_ed2k_progress_roundtrip_via_snapshot() {
    // Verify the progress snapshot roundtrip works for ed2k-style parameters
    let hash = Ed2kFileHash([0xEE; 16]);
    let ph = progress_hash(&hash);
    let file_size = 3 * 9_728_000; // 3 chunks

    let mut snap = ProgressSnapshot::new(ph, file_size, 9_728_000, 3);
    snap.mark_complete(0);
    snap.mark_complete(2);
    snap.downloaded = 2 * 9_728_000;

    let bytes = snap.to_bytes();
    let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();

    assert_eq!(loaded.total_pieces, 3);
    assert!(loaded.is_complete(0));
    assert!(!loaded.is_complete(1));
    assert!(loaded.is_complete(2));
    assert_eq!(loaded.downloaded, 2 * 9_728_000);
}
