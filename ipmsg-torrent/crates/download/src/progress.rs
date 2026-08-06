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
    fn test_bad_magic() {
        let mut data = ProgressSnapshot::new([0; 20], 100, 50, 2).to_bytes();
        data[0] = b'X';
        assert!(ProgressSnapshot::from_bytes(&data).is_err());
    }

    #[test]
    fn test_truncated_data() {
        let data = ProgressSnapshot::new([0; 20], 100, 50, 2).to_bytes();
        assert!(ProgressSnapshot::from_bytes(&data[..10]).is_err());
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
    fn test_mark_complete_idempotent() {
        let mut snap = ProgressSnapshot::new([0; 20], 1024, 256, 4);
        snap.mark_complete(2);
        snap.mark_complete(2);
        snap.mark_complete(2);
        assert_eq!(snap.completed_pieces, vec![2]);
    }
}
