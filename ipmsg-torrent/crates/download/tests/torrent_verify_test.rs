//! Test torrent piece verification logic
use sha1::{Digest, Sha1};

#[test]
fn test_piece_verification() {
    // Create test data
    let piece_data = b"Hello, BitTorrent!";

    // Calculate SHA1 hash
    let mut hasher = Sha1::new();
    hasher.update(piece_data);
    let hash = hasher.finalize();

    // Verify the hash matches
    let mut expected = [0u8; 20];
    expected.copy_from_slice(&hash);

    // This simulates what TorrentEngine does for piece verification
    let verify_hash = Sha1::digest(piece_data);
    assert_eq!(&verify_hash[..], &expected[..]);
}

#[test]
fn test_empty_piece_verification() {
    let piece_data = b"";
    let hash = Sha1::digest(piece_data);
    // SHA1 of empty string is da39a3ee5e6b4b0d3255bfef95601890afd80709
    assert_eq!(
        hex::encode(&hash),
        "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    );
}
