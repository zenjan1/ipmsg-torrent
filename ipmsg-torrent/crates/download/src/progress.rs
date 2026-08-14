//! Unified download progress persistence
//!
//! Saves and loads a bitmap of completed pieces/chunks so downloads
//! can resume after restart without re-downloading completed blocks.
//!
//! ## Binary format (v1)
//!
//! | Field          | Size        | Description                        |
//! |----------------|-------------|------------------------------------|
//! | magic          | 4 bytes     | `b"IPMP"`                          |
//! | version        | 1 byte      | Format version (currently 1)       |
//! | file_hash      | 20 bytes    | SHA-1 / MD4 / zero-padded hash     |
//! | file_size      | 8 bytes     | Total file size in bytes (u64 LE)  |
//! | piece_size     | 8 bytes     | Piece / chunk size (u64 LE)        |
//! | total_pieces   | 4 bytes     | Number of pieces (u32 LE)          |
//! | bitmap_len     | 4 bytes     | Bitmap byte length (u32 LE)        |
//! | bitmap         | N bytes     | 1 bit per piece, MSB first         |
//! | downloaded     | 8 bytes     | Bytes downloaded so far (u64 LE)   |

use std::path::{Path, PathBuf};

/// Errors from progress persistence operations.
#[derive(Debug, thiserror::Error)]
pub enum ProgressError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid format: {0}")]
    Format(String),
    #[error("file size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("hash mismatch")]
    HashMismatch,
}

const MAGIC: &[u8; 4] = b"IPMP";
const VERSION: u8 = 1;
const HASH_LEN: usize = 20;

/// Snapshot of download progress that can be serialized to / from disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressSnapshot {
    /// File identifier hash (SHA-1 for torrent, MD4 for ed2k, etc.)
    pub file_hash: [u8; HASH_LEN],
    /// Total file size in bytes.
    pub file_size: u64,
    /// Piece / chunk size in bytes.
    pub piece_size: u64,
    /// Total number of pieces.
    pub total_pieces: u32,
    /// Set of completed piece indices.
    pub completed_pieces: Vec<u32>,
    /// Total bytes downloaded so far.
    pub downloaded: u64,
}

impl ProgressSnapshot {
    /// Create a new snapshot.
    pub fn new(
        file_hash: [u8; HASH_LEN],
        file_size: u64,
        piece_size: u64,
        total_pieces: u32,
    ) -> Self {
        Self {
            file_hash,
            file_size,
            piece_size,
            total_pieces,
            completed_pieces: Vec::new(),
            downloaded: 0,
        }
    }

    /// Mark a piece as completed.
    pub fn mark_complete(&mut self, piece_index: u32) {
        if !self.completed_pieces.contains(&piece_index) {
            self.completed_pieces.push(piece_index);
        }
    }

    /// Check whether a piece is marked complete.
    pub fn is_complete(&self, piece_index: u32) -> bool {
        self.completed_pieces.contains(&piece_index)
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let bitmap_len = (self.total_pieces as usize).div_ceil(8);
        let mut bitmap = vec![0u8; bitmap_len];

        for &idx in &self.completed_pieces {
            if (idx as usize) < self.total_pieces as usize {
                let byte_idx = (idx as usize) / 8;
                let bit_idx = 7 - ((idx as usize) % 8); // MSB first
                bitmap[byte_idx] |= 1 << bit_idx;
            }
        }

        let mut buf = Vec::with_capacity(4 + 1 + HASH_LEN + 8 + 8 + 4 + 4 + bitmap_len + 8);
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.extend_from_slice(&self.file_hash);
        buf.extend_from_slice(&self.file_size.to_le_bytes());
        buf.extend_from_slice(&self.piece_size.to_le_bytes());
        buf.extend_from_slice(&self.total_pieces.to_le_bytes());
        buf.extend_from_slice(&(bitmap_len as u32).to_le_bytes());
        buf.extend_from_slice(&bitmap);
        buf.extend_from_slice(&self.downloaded.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, ProgressError> {
        let min_len = 4 + 1 + HASH_LEN + 8 + 8 + 4 + 4 + 8;
        if data.len() < min_len {
            return Err(ProgressError::Format("data too short".into()));
        }

        let mut pos = 0;

        // Magic
        if &data[pos..pos + 4] != MAGIC {
            return Err(ProgressError::Format("bad magic".into()));
        }
        pos += 4;

        // Version
        let version = data[pos];
        pos += 1;
        if version != VERSION {
            return Err(ProgressError::Format(format!(
                "unsupported version {version}"
            )));
        }

        // File hash
        let mut file_hash = [0u8; HASH_LEN];
        file_hash.copy_from_slice(&data[pos..pos + HASH_LEN]);
        pos += HASH_LEN;

        // File size
        let file_size = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| ProgressError::Format("bad file_size".into()))?,
        );
        pos += 8;

        // Piece size
        let piece_size = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| ProgressError::Format("bad piece_size".into()))?,
        );
        pos += 8;

        // Total pieces
        let total_pieces = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| ProgressError::Format("bad total_pieces".into()))?,
        );
        pos += 4;

        // Bitmap length
        let bitmap_len = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| ProgressError::Format("bad bitmap_len".into()))?,
        ) as usize;
        pos += 4;

        if data.len() < pos + bitmap_len + 8 {
            return Err(ProgressError::Format("bitmap truncated".into()));
        }

        // Bitmap
        let bitmap = &data[pos..pos + bitmap_len];
        let mut completed_pieces = Vec::new();
        for (byte_idx, &byte) in bitmap.iter().enumerate() {
            for bit in 0..8 {
                let piece_idx = (byte_idx * 8 + bit) as u32;
                if piece_idx >= total_pieces {
                    break;
                }
                if (byte & (1 << (7 - bit))) != 0 {
                    completed_pieces.push(piece_idx);
                }
            }
        }
        pos += bitmap_len;

        // Downloaded
        let downloaded = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| ProgressError::Format("bad downloaded".into()))?,
        );

        Ok(Self {
            file_hash,
            file_size,
            piece_size,
            total_pieces,
            completed_pieces,
            downloaded,
        })
    }
}

/// Compute the progress file path for a given file name.
pub fn progress_path(download_dir: &Path, file_name: &str) -> PathBuf {
    download_dir.join(format!(".{file_name}.progress"))
}

/// Save a progress snapshot to disk.
pub fn save_progress(
    download_dir: &Path,
    file_name: &str,
    snapshot: &ProgressSnapshot,
) -> Result<(), ProgressError> {
    let path = progress_path(download_dir, file_name);
    let data = snapshot.to_bytes();
    std::fs::write(&path, data)?;
    tracing::debug!(path = %path.display(), bytes = snapshot.to_bytes().len(), "Progress saved");
    Ok(())
}

/// Load a progress snapshot from disk.
///
/// Validates that the file hash and file size match the expected values.
pub fn load_progress(
    download_dir: &Path,
    file_name: &str,
    expected_hash: &[u8; HASH_LEN],
    expected_size: u64,
) -> Result<ProgressSnapshot, ProgressError> {
    let path = progress_path(download_dir, file_name);
    let data = std::fs::read(&path)?;
    let snapshot = ProgressSnapshot::from_bytes(&data)?;

    if snapshot.file_hash != *expected_hash {
        return Err(ProgressError::HashMismatch);
    }
    if snapshot.file_size != expected_size {
        return Err(ProgressError::SizeMismatch {
            expected: expected_size,
            actual: snapshot.file_size,
        });
    }

    tracing::debug!(
        path = %path.display(),
        completed = snapshot.completed_pieces.len(),
        total = snapshot.total_pieces,
        "Progress loaded"
    );
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Constants ──────────────────────────────────────────────────────

    #[test]
    fn test_magic_constant() {
        assert_eq!(MAGIC, b"IPMP");
        assert_eq!(MAGIC.len(), 4);
    }

    #[test]
    fn test_version_constant() {
        assert_eq!(VERSION, 1);
    }

    #[test]
    fn test_hash_len_constant() {
        assert_eq!(HASH_LEN, 20);
    }

    // ── ProgressError Display ──────────────────────────────────────────

    #[test]
    fn test_error_display_io() {
        let err = ProgressError::Io(std::io::Error::other("disk full"));
        assert_eq!(err.to_string(), "IO error: disk full");
    }

    #[test]
    fn test_error_display_format() {
        let err = ProgressError::Format("bad magic".into());
        assert_eq!(err.to_string(), "invalid format: bad magic");
    }

    #[test]
    fn test_error_display_size_mismatch() {
        let err = ProgressError::SizeMismatch {
            expected: 1000,
            actual: 500,
        };
        assert_eq!(
            err.to_string(),
            "file size mismatch: expected 1000, got 500"
        );
    }

    #[test]
    fn test_error_display_hash_mismatch() {
        let err = ProgressError::HashMismatch;
        assert_eq!(err.to_string(), "hash mismatch");
    }

    #[test]
    fn test_error_debug() {
        let err = ProgressError::HashMismatch;
        let debug = format!("{err:?}");
        assert!(debug.contains("HashMismatch"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::other("test");
        let err: ProgressError = io_err.into();
        assert!(matches!(err, ProgressError::Io(_)));
        assert!(err.to_string().contains("test"));
    }

    // ── ProgressSnapshot::new ──────────────────────────────────────────

    #[test]
    fn test_new_default_values() {
        let snap = ProgressSnapshot::new([0xAB; 20], 1_000_000, 256_000, 4);
        assert_eq!(snap.file_hash, [0xAB; 20]);
        assert_eq!(snap.file_size, 1_000_000);
        assert_eq!(snap.piece_size, 256_000);
        assert_eq!(snap.total_pieces, 4);
        assert!(snap.completed_pieces.is_empty());
        assert_eq!(snap.downloaded, 0);
    }

    #[test]
    fn test_new_zero_pieces() {
        let snap = ProgressSnapshot::new([0; 20], 0, 0, 0);
        assert_eq!(snap.total_pieces, 0);
        assert!(snap.completed_pieces.is_empty());
    }

    #[test]
    fn test_new_zero_file_size() {
        let snap = ProgressSnapshot::new([0; 20], 0, 1024, 0);
        assert_eq!(snap.file_size, 0);
    }

    // ── ProgressSnapshot fields ────────────────────────────────────────

    #[test]
    fn test_fields_accessible() {
        let mut snap = ProgressSnapshot::new([0x11; 20], 4096, 1024, 4);
        snap.downloaded = 2048;
        snap.completed_pieces.push(0);
        assert_eq!(snap.file_hash, [0x11; 20]);
        assert_eq!(snap.file_size, 4096);
        assert_eq!(snap.piece_size, 1024);
        assert_eq!(snap.total_pieces, 4);
        assert_eq!(snap.downloaded, 2048);
        assert_eq!(snap.completed_pieces, vec![0]);
    }

    // ── mark_complete ──────────────────────────────────────────────────

    #[test]
    fn test_mark_complete_idempotent() {
        let mut snap = ProgressSnapshot::new([0; 20], 1024, 256, 4);
        snap.mark_complete(2);
        snap.mark_complete(2);
        snap.mark_complete(2);
        assert_eq!(snap.completed_pieces, vec![2]);
    }

    #[test]
    fn test_mark_complete_multiple() {
        let mut snap = ProgressSnapshot::new([0; 20], 4096, 1024, 4);
        snap.mark_complete(0);
        snap.mark_complete(1);
        snap.mark_complete(2);
        snap.mark_complete(3);
        assert_eq!(snap.completed_pieces, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_mark_complete_out_of_bounds_ignored() {
        let mut snap = ProgressSnapshot::new([0; 20], 4096, 1024, 4);
        snap.mark_complete(4); // >= total_pieces
        snap.mark_complete(100);
        // Pieces are stored but won't be encoded in bitmap
        assert_eq!(snap.completed_pieces, vec![4, 100]);
    }

    #[test]
    fn test_mark_complete_preserves_order() {
        let mut snap = ProgressSnapshot::new([0; 20], 8192, 1024, 8);
        snap.mark_complete(7);
        snap.mark_complete(0);
        snap.mark_complete(3);
        assert_eq!(snap.completed_pieces, vec![7, 0, 3]);
    }

    // ── is_complete ────────────────────────────────────────────────────

    #[test]
    fn test_is_complete_true() {
        let mut snap = ProgressSnapshot::new([0; 20], 4096, 1024, 4);
        snap.mark_complete(2);
        assert!(snap.is_complete(2));
    }

    #[test]
    fn test_is_complete_false() {
        let snap = ProgressSnapshot::new([0; 20], 4096, 1024, 4);
        assert!(!snap.is_complete(0));
        assert!(!snap.is_complete(3));
    }

    #[test]
    fn test_is_complete_out_of_bounds() {
        let snap = ProgressSnapshot::new([0; 20], 4096, 1024, 4);
        assert!(!snap.is_complete(99));
    }

    // ── to_bytes / from_bytes roundtrip ────────────────────────────────

    #[test]
    fn test_roundtrip_empty() {
        let snap = ProgressSnapshot::new([0xAB; 20], 1_000_000, 256_000, 4);
        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(snap, loaded);
        assert!(loaded.completed_pieces.is_empty());
    }

    #[test]
    fn test_roundtrip_with_pieces() {
        let mut snap = ProgressSnapshot::new([0x42; 20], 10 * 1024 * 1024, 256 * 1024, 40);
        snap.mark_complete(0);
        snap.mark_complete(5);
        snap.mark_complete(39);
        snap.downloaded = 3 * 256 * 1024;

        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();

        assert_eq!(loaded.total_pieces, 40);
        assert!(loaded.is_complete(0));
        assert!(loaded.is_complete(5));
        assert!(loaded.is_complete(39));
        assert!(!loaded.is_complete(1));
        assert_eq!(loaded.downloaded, 3 * 256 * 1024);
    }

    #[test]
    fn test_bitmap_encoding_msb_first() {
        // 10 pieces → 2 bytes; pieces 0 and 9 complete
        let mut snap = ProgressSnapshot::new([0; 20], 1024, 100, 10);
        snap.mark_complete(0);
        snap.mark_complete(9);

        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();

        assert!(loaded.is_complete(0));
        assert!(loaded.is_complete(9));
        assert!(!loaded.is_complete(1));
        assert!(!loaded.is_complete(8));
    }

    #[test]
    fn test_roundtrip_all_pieces_complete() {
        let mut snap = ProgressSnapshot::new([0x55; 20], 8192, 1024, 8);
        for i in 0..8 {
            snap.mark_complete(i);
        }
        snap.downloaded = 8192;

        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();

        assert_eq!(loaded.completed_pieces.len(), 8);
        for i in 0..8 {
            assert!(loaded.is_complete(i));
        }
        assert_eq!(loaded.downloaded, 8192);
    }

    #[test]
    fn test_roundtrip_single_piece() {
        let mut snap = ProgressSnapshot::new([0; 20], 512, 512, 1);
        snap.mark_complete(0);
        snap.downloaded = 512;

        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();

        assert_eq!(loaded.total_pieces, 1);
        assert!(loaded.is_complete(0));
        assert_eq!(loaded.downloaded, 512);
    }

    #[test]
    fn test_roundtrip_large_piece_count() {
        let mut snap = ProgressSnapshot::new([0; 20], 1_000_000_000, 16_384, 61_036);
        // Mark every 100th piece
        for i in (0..61_036).step_by(100) {
            snap.mark_complete(i);
        }

        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();

        for i in (0..61_036).step_by(100) {
            assert!(loaded.is_complete(i), "piece {i} should be complete");
        }
        assert!(!loaded.is_complete(1));
        assert!(!loaded.is_complete(50));
    }

    #[test]
    fn test_roundtrip_zero_pieces() {
        let snap = ProgressSnapshot::new([0; 20], 0, 1024, 0);
        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(loaded.total_pieces, 0);
        assert!(loaded.completed_pieces.is_empty());
    }

    #[test]
    fn test_roundtrip_max_downloaded() {
        let mut snap = ProgressSnapshot::new([0; 20], u64::MAX, 1024, 1);
        snap.downloaded = u64::MAX;
        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(loaded.downloaded, u64::MAX);
    }

    #[test]
    fn test_roundtrip_all_hash_bytes() {
        // All 0xFF hash
        let snap = ProgressSnapshot::new([0xFF; 20], 1024, 512, 2);
        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(loaded.file_hash, [0xFF; 20]);
    }

    #[test]
    fn test_roundtrip_mixed_hash() {
        let mut hash = [0u8; 20];
        for (i, b) in hash.iter_mut().enumerate() {
            *b = i as u8;
        }
        let snap = ProgressSnapshot::new(hash, 1024, 512, 2);
        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(loaded.file_hash, hash);
    }

    #[test]
    fn test_bitmap_non_aligned_pieces() {
        // 13 pieces → 2 bytes (16 bits), bits 13-15 should be unused
        let mut snap = ProgressSnapshot::new([0; 20], 13 * 1024, 1024, 13);
        snap.mark_complete(0);
        snap.mark_complete(7);
        snap.mark_complete(12); // last piece

        let bytes = snap.to_bytes();
        let loaded = ProgressSnapshot::from_bytes(&bytes).unwrap();

        assert!(loaded.is_complete(0));
        assert!(loaded.is_complete(7));
        assert!(loaded.is_complete(12));
        assert!(!loaded.is_complete(1));
        assert!(!loaded.is_complete(6));
        assert!(!loaded.is_complete(11));
        assert_eq!(loaded.completed_pieces.len(), 3);
    }

    #[test]
    fn test_to_bytes_length() {
        let snap = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        let bytes = snap.to_bytes();
        // 4 (magic) + 1 (version) + 20 (hash) + 8 (size) + 8 (piece_size) + 4 (total_pieces) + 4 (bitmap_len) + 1 (bitmap) + 8 (downloaded) = 58
        assert_eq!(bytes.len(), 58);
    }

    #[test]
    fn test_to_bytes_length_8_pieces() {
        let snap = ProgressSnapshot::new([0; 20], 8192, 1024, 8);
        let bytes = snap.to_bytes();
        // bitmap = 1 byte for 8 pieces
        assert_eq!(bytes.len(), 58);
    }

    #[test]
    fn test_to_bytes_length_9_pieces() {
        let snap = ProgressSnapshot::new([0; 20], 9216, 1024, 9);
        let bytes = snap.to_bytes();
        // bitmap = 2 bytes for 9 pieces
        assert_eq!(bytes.len(), 59);
    }

    // ── from_bytes error cases ─────────────────────────────────────────

    #[test]
    fn test_bad_magic() {
        let mut data = ProgressSnapshot::new([0; 20], 100, 50, 2).to_bytes();
        data[0] = b'X';
        let err = ProgressSnapshot::from_bytes(&data).unwrap_err();
        assert!(matches!(err, ProgressError::Format(_)));
        assert!(err.to_string().contains("bad magic"));
    }

    #[test]
    fn test_bad_version() {
        let mut data = ProgressSnapshot::new([0; 20], 100, 50, 2).to_bytes();
        data[4] = 99; // version byte
        let err = ProgressSnapshot::from_bytes(&data).unwrap_err();
        assert!(matches!(err, ProgressError::Format(_)));
        assert!(err.to_string().contains("unsupported version"));
    }

    #[test]
    fn test_truncated_data() {
        let data = ProgressSnapshot::new([0; 20], 100, 50, 2).to_bytes();
        assert!(ProgressSnapshot::from_bytes(&data[..10]).is_err());
    }

    #[test]
    fn test_truncated_at_header() {
        // Just magic + version, no hash
        let data = vec![b'I', b'P', b'M', b'P', 1];
        let err = ProgressSnapshot::from_bytes(&data).unwrap_err();
        assert!(matches!(err, ProgressError::Format(_)));
    }

    #[test]
    fn test_empty_data() {
        let err = ProgressSnapshot::from_bytes(&[]).unwrap_err();
        assert!(matches!(err, ProgressError::Format(_)));
    }

    #[test]
    fn test_bitmap_truncated() {
        let mut data = ProgressSnapshot::new([0; 20], 8192, 1024, 8).to_bytes();
        // Remove the last byte (downloaded field) and bitmap
        let header_len = 4 + 1 + 20 + 8 + 8 + 4 + 4;
        data.truncate(header_len + 1); // keep bitmap but cut downloaded
        let err = ProgressSnapshot::from_bytes(&data).unwrap_err();
        assert!(matches!(err, ProgressError::Format(_)));
    }

    // ── Clone / Debug / PartialEq ──────────────────────────────────────

    #[test]
    fn test_clone() {
        let mut snap = ProgressSnapshot::new([0xAA; 20], 4096, 1024, 4);
        snap.mark_complete(1);
        snap.downloaded = 1024;
        let cloned = snap.clone();
        assert_eq!(snap, cloned);
    }

    #[test]
    fn test_clone_independence() {
        let mut snap = ProgressSnapshot::new([0; 20], 4096, 1024, 4);
        snap.mark_complete(0);
        let mut cloned = snap.clone();
        cloned.mark_complete(1);
        assert!(!snap.is_complete(1));
        assert!(cloned.is_complete(1));
    }

    #[test]
    fn test_debug() {
        let snap = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        let debug = format!("{snap:?}");
        assert!(debug.contains("ProgressSnapshot"));
        assert!(debug.contains("file_size"));
        assert!(debug.contains("total_pieces"));
    }

    #[test]
    fn test_partial_eq() {
        let a = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        let b = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        assert_eq!(a, b);
    }

    #[test]
    fn test_partial_eq_different_hash() {
        let a = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        let b = ProgressSnapshot::new([1; 20], 1024, 512, 2);
        assert_ne!(a, b);
    }

    #[test]
    fn test_partial_eq_different_pieces() {
        let mut a = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        a.mark_complete(0);
        let b = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        assert_ne!(a, b);
    }

    #[test]
    fn test_partial_eq_different_downloaded() {
        let mut a = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        a.downloaded = 100;
        let mut b = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        b.downloaded = 200;
        assert_ne!(a, b);
    }

    // ── progress_path ──────────────────────────────────────────────────

    #[test]
    fn test_progress_path_basic() {
        let path = progress_path(Path::new("/tmp/dl"), "file.bin");
        assert_eq!(path, PathBuf::from("/tmp/dl/.file.bin.progress"));
    }

    #[test]
    fn test_progress_path_hidden_file() {
        let path = progress_path(Path::new("/tmp"), ".hidden");
        assert_eq!(path, PathBuf::from("/tmp/..hidden.progress"));
    }

    #[test]
    fn test_progress_path_unicode() {
        let path = progress_path(Path::new("/downloads"), "文件.mp4");
        assert_eq!(path, PathBuf::from("/downloads/.文件.mp4.progress"));
    }

    #[test]
    fn test_progress_path_emoji() {
        let path = progress_path(Path::new("/tmp"), "🎬.mkv");
        assert_eq!(path, PathBuf::from("/tmp/.🎬.mkv.progress"));
    }

    #[test]
    fn test_progress_path_empty_name() {
        let path = progress_path(Path::new("/tmp"), "");
        assert_eq!(path, PathBuf::from("/tmp/..progress"));
    }

    #[test]
    fn test_progress_path_nested_dir() {
        let path = progress_path(Path::new("/a/b/c"), "data.zip");
        assert_eq!(path, PathBuf::from("/a/b/c/.data.zip.progress"));
    }

    // ── save_progress ──────────────────────────────────────────────────

    #[test]
    fn test_save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let snap = ProgressSnapshot::new([0; 20], 1024, 512, 2);
        save_progress(dir.path(), "test.bin", &snap).unwrap();

        let path = dir.path().join(".test.bin.progress");
        assert!(path.exists());
    }

    #[test]
    fn test_save_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let snap1 = ProgressSnapshot::new([0xAA; 20], 1024, 512, 2);
        save_progress(dir.path(), "test.bin", &snap1).unwrap();

        let mut snap2 = ProgressSnapshot::new([0xBB; 20], 2048, 1024, 2);
        snap2.mark_complete(0);
        save_progress(dir.path(), "test.bin", &snap2).unwrap();

        let loaded = load_progress(dir.path(), "test.bin", &[0xBB; 20], 2048).unwrap();
        assert!(loaded.is_complete(0));
        assert_eq!(loaded.file_size, 2048);
    }

    #[test]
    fn test_save_load_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut snap = ProgressSnapshot::new([0xFF; 20], 4096, 1024, 4);
        snap.mark_complete(1);
        snap.mark_complete(3);
        snap.downloaded = 2048;

        save_progress(dir.path(), "test.bin", &snap).unwrap();

        let loaded = load_progress(dir.path(), "test.bin", &[0xFF; 20], 4096).unwrap();
        assert_eq!(loaded.completed_pieces, snap.completed_pieces);
        assert_eq!(loaded.downloaded, 2048);
    }

    // ── load_progress ──────────────────────────────────────────────────

    #[test]
    fn test_load_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let err = load_progress(dir.path(), "nonexistent.bin", &[0; 20], 100).unwrap_err();
        assert!(matches!(err, ProgressError::Io(_)));
    }

    #[test]
    fn test_load_hash_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let snap = ProgressSnapshot::new([0xAA; 20], 100, 50, 2);
        save_progress(dir.path(), "test.bin", &snap).unwrap();

        let err = load_progress(dir.path(), "test.bin", &[0xBB; 20], 100).unwrap_err();
        assert!(matches!(err, ProgressError::HashMismatch));
    }

    #[test]
    fn test_load_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let snap = ProgressSnapshot::new([0xAA; 20], 100, 50, 2);
        save_progress(dir.path(), "test.bin", &snap).unwrap();

        let err = load_progress(dir.path(), "test.bin", &[0xAA; 20], 999).unwrap_err();
        assert!(matches!(err, ProgressError::SizeMismatch { .. }));
    }

    #[test]
    fn test_load_size_mismatch_values() {
        let dir = tempfile::tempdir().unwrap();
        let snap = ProgressSnapshot::new([0; 20], 500, 100, 5);
        save_progress(dir.path(), "f.bin", &snap).unwrap();

        let err = load_progress(dir.path(), "f.bin", &[0; 20], 1000).unwrap_err();
        match err {
            ProgressError::SizeMismatch { expected, actual } => {
                assert_eq!(expected, 1000);
                assert_eq!(actual, 500);
            }
            _ => panic!("expected SizeMismatch"),
        }
    }

    #[test]
    fn test_load_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".corrupt.bin.progress");
        std::fs::write(&path, b"not valid data").unwrap();

        let err = load_progress(dir.path(), "corrupt.bin", &[0; 20], 100).unwrap_err();
        assert!(matches!(err, ProgressError::Format(_)));
    }

    #[test]
    fn test_load_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".empty.bin.progress");
        std::fs::write(&path, b"").unwrap();

        let err = load_progress(dir.path(), "empty.bin", &[0; 20], 100).unwrap_err();
        assert!(matches!(err, ProgressError::Format(_)));
    }

    // ── Complex workflows ──────────────────────────────────────────────

    #[test]
    fn test_full_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let hash = [0x42; 20];
        let file_size = 10 * 1024 * 1024u64;
        let piece_size = 256 * 1024u64;
        let total_pieces = 40;

        // Create initial snapshot
        let mut snap = ProgressSnapshot::new(hash, file_size, piece_size, total_pieces);

        // Simulate downloading pieces
        for i in [0, 1, 2, 5, 10, 20, 39] {
            snap.mark_complete(i);
        }
        snap.downloaded = 7 * piece_size;

        // Save
        save_progress(dir.path(), "big_file.bin", &snap).unwrap();

        // Load and verify
        let loaded = load_progress(dir.path(), "big_file.bin", &hash, file_size).unwrap();
        assert_eq!(loaded.total_pieces, total_pieces);
        assert_eq!(loaded.completed_pieces.len(), 7);
        for i in [0, 1, 2, 5, 10, 20, 39] {
            assert!(loaded.is_complete(i), "piece {i} should be complete");
        }
        assert!(!loaded.is_complete(3));
        assert!(!loaded.is_complete(38));
        assert_eq!(loaded.downloaded, 7 * piece_size);
    }

    #[test]
    fn test_save_load_multiple_files() {
        let dir = tempfile::tempdir().unwrap();

        let hash1 = [0x11; 20];
        let hash2 = [0x22; 20];

        let mut snap1 = ProgressSnapshot::new(hash1, 1024, 512, 2);
        snap1.mark_complete(0);
        snap1.downloaded = 512;

        let mut snap2 = ProgressSnapshot::new(hash2, 2048, 1024, 2);
        snap2.mark_complete(1);
        snap2.downloaded = 1024;

        save_progress(dir.path(), "file1.bin", &snap1).unwrap();
        save_progress(dir.path(), "file2.bin", &snap2).unwrap();

        let loaded1 = load_progress(dir.path(), "file1.bin", &hash1, 1024).unwrap();
        let loaded2 = load_progress(dir.path(), "file2.bin", &hash2, 2048).unwrap();

        assert!(loaded1.is_complete(0));
        assert!(!loaded1.is_complete(1));
        assert!(!loaded2.is_complete(0));
        assert!(loaded2.is_complete(1));
    }

    #[test]
    fn test_re_save_after_progress() {
        let dir = tempfile::tempdir().unwrap();
        let hash = [0x33; 20];

        // Initial save with 2 pieces
        let mut snap = ProgressSnapshot::new(hash, 4096, 1024, 4);
        snap.mark_complete(0);
        snap.mark_complete(1);
        snap.downloaded = 2048;
        save_progress(dir.path(), "resume.bin", &snap).unwrap();

        // Load, add more pieces, re-save
        let mut loaded = load_progress(dir.path(), "resume.bin", &hash, 4096).unwrap();
        loaded.mark_complete(2);
        loaded.downloaded = 3072;
        save_progress(dir.path(), "resume.bin", &loaded).unwrap();

        // Final load
        let final_loaded = load_progress(dir.path(), "resume.bin", &hash, 4096).unwrap();
        assert_eq!(final_loaded.completed_pieces.len(), 3);
        assert!(final_loaded.is_complete(0));
        assert!(final_loaded.is_complete(1));
        assert!(final_loaded.is_complete(2));
        assert!(!final_loaded.is_complete(3));
        assert_eq!(final_loaded.downloaded, 3072);
    }
}
