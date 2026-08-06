//! P2P file download progress persistence
//!
//! Saves and restores FileDownload state so P2P transfers can resume
//! after process restart without re-downloading completed chunks.
//!
//! ## Binary format (v1)
//!
//! | Field          | Size        | Description                        |
//! |----------------|-------------|------------------------------------|
//! | magic          | 4 bytes     | `b"P2PD"`                          |
//! | version        | 1 byte      | Format version (currently 1)       |
//! | file_hash_len  | 4 bytes     | Hash string length (u32 LE)        |
//! | file_hash      | N bytes     | File hash (UTF-8 string)           |
//! | file_name_len  | 4 bytes     | File name length (u32 LE)          |
//! | file_name      | N bytes     | File name (UTF-8 string)           |
//! | file_size      | 8 bytes     | Total file size (u64 LE)           |
//! | chunk_size     | 4 bytes     | Chunk size (u32 LE)                |
//! | total_chunks   | 4 bytes     | Total chunks (u32 LE)              |
//! | received_count | 4 bytes     | Number of received chunks (u32 LE) |
//! | chunks         | Variable    | Received chunk indices (u32 LE each)|
//! | bytes_received | 8 bytes     | Bytes downloaded so far (u64 LE)   |
//! | owner_len      | 4 bytes     | Owner peer ID length (u32 LE)      |
//! | owner          | N bytes     | Owner peer ID (UTF-8 string)       |

use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"P2PD";
const VERSION: u8 = 1;

/// Errors from P2P progress persistence operations
#[derive(Debug, thiserror::Error)]
pub enum P2pProgressError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid format: {0}")]
    Format(String),
    #[error("file hash mismatch")]
    HashMismatch,
    #[error("file size mismatch")]
    SizeMismatch,
}

/// Snapshot of P2P download progress that can be serialized to/from disk
#[derive(Debug, Clone)]
pub struct P2pDownloadSnapshot {
    /// File hash identifier
    pub file_hash: String,
    /// File name
    pub file_name: String,
    /// Total file size in bytes
    pub file_size: u64,
    /// Chunk size in bytes
    pub chunk_size: u32,
    /// Total number of chunks
    pub total_chunks: u32,
    /// Received chunk indices
    pub received_chunks: Vec<u32>,
    /// Bytes downloaded so far
    pub bytes_received: u64,
    /// Owner peer ID
    pub owner: String,
    /// Last activity timestamp
    pub last_activity: DateTime<Utc>,
}

impl P2pDownloadSnapshot {
    /// Create a new snapshot
    pub fn new(
        file_hash: String,
        file_name: String,
        file_size: u64,
        chunk_size: u32,
        total_chunks: u32,
        owner: String,
    ) -> Self {
        Self {
            file_hash,
            file_name,
            file_size,
            chunk_size,
            total_chunks,
            received_chunks: Vec::new(),
            bytes_received: 0,
            owner,
            last_activity: Utc::now(),
        }
    }

    /// Mark a chunk as received
    pub fn mark_received(&mut self, chunk_index: u32, size: u64) {
        if !self.received_chunks.contains(&chunk_index) {
            self.received_chunks.push(chunk_index);
            self.bytes_received += size;
            self.last_activity = Utc::now();
        }
    }

    /// Get missing chunk indices
    pub fn missing_chunks(&self) -> Vec<u32> {
        (0..self.total_chunks)
            .filter(|i| !self.received_chunks.contains(i))
            .collect()
    }

    /// Check if download is complete
    pub fn is_complete(&self) -> bool {
        self.received_chunks.len() == self.total_chunks as usize
    }

    /// Calculate progress percentage
    pub fn progress(&self) -> f32 {
        if self.total_chunks == 0 {
            0.0
        } else {
            (self.received_chunks.len() as f32 / self.total_chunks as f32) * 100.0
        }
    }
}

/// Save P2P download progress to disk
pub fn save_progress(
    progress_dir: &Path,
    snapshot: &P2pDownloadSnapshot,
) -> Result<(), P2pProgressError> {
    std::fs::create_dir_all(progress_dir)?;

    let file_path = progress_path(progress_dir, &snapshot.file_hash);

    let mut data = Vec::new();

    // Magic
    data.extend_from_slice(MAGIC);

    // Version
    data.push(VERSION);

    // File hash
    let hash_bytes = snapshot.file_hash.as_bytes();
    data.extend_from_slice(&(hash_bytes.len() as u32).to_le_bytes());
    data.extend_from_slice(hash_bytes);

    // File name
    let name_bytes = snapshot.file_name.as_bytes();
    data.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    data.extend_from_slice(name_bytes);

    // File size
    data.extend_from_slice(&snapshot.file_size.to_le_bytes());

    // Chunk size
    data.extend_from_slice(&snapshot.chunk_size.to_le_bytes());

    // Total chunks
    data.extend_from_slice(&snapshot.total_chunks.to_le_bytes());

    // Received chunks
    data.extend_from_slice(&(snapshot.received_chunks.len() as u32).to_le_bytes());
    for &chunk_idx in &snapshot.received_chunks {
        data.extend_from_slice(&chunk_idx.to_le_bytes());
    }

    // Bytes received
    data.extend_from_slice(&snapshot.bytes_received.to_le_bytes());

    // Owner
    let owner_bytes = snapshot.owner.as_bytes();
    data.extend_from_slice(&(owner_bytes.len() as u32).to_le_bytes());
    data.extend_from_slice(owner_bytes);

    std::fs::write(&file_path, &data)?;

    tracing::debug!(
        path = %file_path.display(),
        hash = %snapshot.file_hash,
        progress = %format!("{:.1}%", snapshot.progress()),
        "P2P download progress saved"
    );

    Ok(())
}

/// Load P2P download progress from disk
pub fn load_progress(
    progress_dir: &Path,
    file_hash: &str,
) -> Result<P2pDownloadSnapshot, P2pProgressError> {
    let file_path = progress_path(progress_dir, file_hash);

    if !file_path.exists() {
        return Err(P2pProgressError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No progress file found",
        )));
    }

    let data = std::fs::read(&file_path)?;

    if data.len() < 5 {
        return Err(P2pProgressError::Format("File too short".to_string()));
    }

    let mut pos = 0;

    // Magic
    if &data[pos..pos + 4] != MAGIC {
        return Err(P2pProgressError::Format("Invalid magic".to_string()));
    }
    pos += 4;

    // Version
    let version = data[pos];
    if version != VERSION {
        return Err(P2pProgressError::Format(format!(
            "Unsupported version: {}",
            version
        )));
    }
    pos += 1;

    // File hash
    if pos + 4 > data.len() {
        return Err(P2pProgressError::Format(
            "Truncated hash length".to_string(),
        ));
    }
    let hash_len =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;

    if pos + hash_len > data.len() {
        return Err(P2pProgressError::Format("Truncated hash".to_string()));
    }
    let file_hash = String::from_utf8(data[pos..pos + hash_len].to_vec())
        .map_err(|e| P2pProgressError::Format(format!("Invalid hash encoding: {}", e)))?;
    pos += hash_len;

    // File name
    if pos + 4 > data.len() {
        return Err(P2pProgressError::Format(
            "Truncated name length".to_string(),
        ));
    }
    let name_len =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;

    if pos + name_len > data.len() {
        return Err(P2pProgressError::Format("Truncated name".to_string()));
    }
    let file_name = String::from_utf8(data[pos..pos + name_len].to_vec())
        .map_err(|e| P2pProgressError::Format(format!("Invalid name encoding: {}", e)))?;
    pos += name_len;

    // File size
    if pos + 8 > data.len() {
        return Err(P2pProgressError::Format("Truncated file size".to_string()));
    }
    let file_size = u64::from_le_bytes([
        data[pos],
        data[pos + 1],
        data[pos + 2],
        data[pos + 3],
        data[pos + 4],
        data[pos + 5],
        data[pos + 6],
        data[pos + 7],
    ]);
    pos += 8;

    // Chunk size
    if pos + 4 > data.len() {
        return Err(P2pProgressError::Format("Truncated chunk size".to_string()));
    }
    let chunk_size = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    pos += 4;

    // Total chunks
    if pos + 4 > data.len() {
        return Err(P2pProgressError::Format(
            "Truncated total chunks".to_string(),
        ));
    }
    let total_chunks = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    pos += 4;

    // Received chunks
    if pos + 4 > data.len() {
        return Err(P2pProgressError::Format(
            "Truncated chunk count".to_string(),
        ));
    }
    let received_count =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;

    let mut received_chunks = Vec::with_capacity(received_count);
    for _ in 0..received_count {
        if pos + 4 > data.len() {
            return Err(P2pProgressError::Format(
                "Truncated chunk index".to_string(),
            ));
        }
        let chunk_idx =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        received_chunks.push(chunk_idx);
        pos += 4;
    }

    // Bytes received
    if pos + 8 > data.len() {
        return Err(P2pProgressError::Format(
            "Truncated bytes received".to_string(),
        ));
    }
    let bytes_received = u64::from_le_bytes([
        data[pos],
        data[pos + 1],
        data[pos + 2],
        data[pos + 3],
        data[pos + 4],
        data[pos + 5],
        data[pos + 6],
        data[pos + 7],
    ]);
    pos += 8;

    // Owner
    if pos + 4 > data.len() {
        return Err(P2pProgressError::Format(
            "Truncated owner length".to_string(),
        ));
    }
    let owner_len =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;

    if pos + owner_len > data.len() {
        return Err(P2pProgressError::Format("Truncated owner".to_string()));
    }
    let owner = String::from_utf8(data[pos..pos + owner_len].to_vec())
        .map_err(|e| P2pProgressError::Format(format!("Invalid owner encoding: {}", e)))?;

    let snapshot = P2pDownloadSnapshot {
        file_hash,
        file_name,
        file_size,
        chunk_size,
        total_chunks,
        received_chunks,
        bytes_received,
        owner,
        last_activity: Utc::now(),
    };

    tracing::info!(
        hash = %snapshot.file_hash,
        name = %snapshot.file_name,
        progress = %format!("{:.1}%", snapshot.progress()),
        "P2P download progress loaded"
    );

    Ok(snapshot)
}

/// Remove progress file for a completed download
pub fn remove_progress(progress_dir: &Path, file_hash: &str) -> Result<(), P2pProgressError> {
    let file_path = progress_path(progress_dir, file_hash);
    if file_path.exists() {
        std::fs::remove_file(&file_path)?;
        tracing::debug!(hash = %file_hash, "P2P progress file removed");
    }
    Ok(())
}

/// List all saved progress files
pub fn list_progress(progress_dir: &Path) -> Vec<String> {
    let mut hashes = Vec::new();
    if let Ok(entries) = std::fs::read_dir(progress_dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str()
                && name.ends_with(".p2p")
            {
                // Extract hash from filename: <hash>.p2p
                let hash = name.trim_end_matches(".p2p");
                hashes.push(hash.to_string());
            }
        }
    }
    hashes
}

/// Get the progress file path for a given file hash
fn progress_path(progress_dir: &Path, file_hash: &str) -> PathBuf {
    // Sanitize hash for use as filename
    let safe_hash: String = file_hash
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    progress_dir.join(format!("{}.p2p", safe_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_progress() {
        let mut snapshot = P2pDownloadSnapshot::new(
            "abc123".to_string(),
            "test.txt".to_string(),
            1000,
            250,
            4,
            "peer1".to_string(),
        );

        assert_eq!(snapshot.progress(), 0.0);
        assert!(!snapshot.is_complete());
        assert_eq!(snapshot.missing_chunks(), vec![0, 1, 2, 3]);

        snapshot.mark_received(0, 250);
        assert_eq!(snapshot.progress(), 25.0);
        assert_eq!(snapshot.missing_chunks(), vec![1, 2, 3]);

        snapshot.mark_received(1, 250);
        snapshot.mark_received(2, 250);
        snapshot.mark_received(3, 250);
        assert_eq!(snapshot.progress(), 100.0);
        assert!(snapshot.is_complete());
        assert!(snapshot.missing_chunks().is_empty());
    }

    #[test]
    fn test_save_and_load_progress() {
        let temp_dir = TempDir::new().unwrap();
        let progress_dir = temp_dir.path();

        let mut snapshot = P2pDownloadSnapshot::new(
            "hash123".to_string(),
            "video.mp4".to_string(),
            1024 * 1024,
            64 * 1024,
            16,
            "peer_abc".to_string(),
        );

        snapshot.mark_received(0, 64 * 1024);
        snapshot.mark_received(5, 64 * 1024);
        snapshot.mark_received(10, 64 * 1024);

        // Save
        save_progress(progress_dir, &snapshot).unwrap();

        // Load
        let loaded = load_progress(progress_dir, "hash123").unwrap();

        assert_eq!(loaded.file_hash, "hash123");
        assert_eq!(loaded.file_name, "video.mp4");
        assert_eq!(loaded.file_size, 1024 * 1024);
        assert_eq!(loaded.chunk_size, 64 * 1024);
        assert_eq!(loaded.total_chunks, 16);
        assert_eq!(loaded.received_chunks.len(), 3);
        assert!(loaded.received_chunks.contains(&0));
        assert!(loaded.received_chunks.contains(&5));
        assert!(loaded.received_chunks.contains(&10));
        assert_eq!(loaded.bytes_received, 3 * 64 * 1024);
        assert_eq!(loaded.owner, "peer_abc");
    }

    #[test]
    fn test_load_nonexistent_progress() {
        let temp_dir = TempDir::new().unwrap();
        let result = load_progress(temp_dir.path(), "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_progress() {
        let temp_dir = TempDir::new().unwrap();
        let progress_dir = temp_dir.path();

        let snapshot = P2pDownloadSnapshot::new(
            "hash456".to_string(),
            "test.txt".to_string(),
            1000,
            250,
            4,
            "peer1".to_string(),
        );

        save_progress(progress_dir, &snapshot).unwrap();
        assert!(progress_path(progress_dir, "hash456").exists());

        remove_progress(progress_dir, "hash456").unwrap();
        assert!(!progress_path(progress_dir, "hash456").exists());
    }

    #[test]
    fn test_list_progress() {
        let temp_dir = TempDir::new().unwrap();
        let progress_dir = temp_dir.path();

        let snapshot1 = P2pDownloadSnapshot::new(
            "hash1".to_string(),
            "file1.txt".to_string(),
            1000,
            250,
            4,
            "peer1".to_string(),
        );
        let snapshot2 = P2pDownloadSnapshot::new(
            "hash2".to_string(),
            "file2.txt".to_string(),
            2000,
            500,
            4,
            "peer2".to_string(),
        );

        save_progress(progress_dir, &snapshot1).unwrap();
        save_progress(progress_dir, &snapshot2).unwrap();

        let mut hashes = list_progress(progress_dir);
        hashes.sort();
        assert_eq!(hashes, vec!["hash1", "hash2"]);
    }

    #[test]
    fn test_mark_received_idempotent() {
        let mut snapshot = P2pDownloadSnapshot::new(
            "hash".to_string(),
            "test.txt".to_string(),
            1000,
            250,
            4,
            "peer1".to_string(),
        );

        snapshot.mark_received(0, 250);
        snapshot.mark_received(0, 250); // Duplicate
        assert_eq!(snapshot.bytes_received, 250); // Should not double-count
        assert_eq!(snapshot.received_chunks.len(), 1);
    }
}
