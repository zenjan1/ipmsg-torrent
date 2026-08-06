//! Ed2k peer cache - persist discovered peers to disk
//!
//! Format: simple binary format
//! - Magic: 4 bytes "EPCC" (Ed2k Peer Cache)
//! - Version: 1 byte (u8)
//! - File hash: 16 bytes (MD4)
//! - Peer count: 2 bytes (u16 LE)
//! - Peers: count * (4 bytes IP + 2 bytes port)

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"EPCC";
const VERSION: u8 = 1;

/// Get the peer cache file path for a given file hash
pub fn peer_cache_path(download_dir: &Path, file_hash: &[u8; 16]) -> PathBuf {
    let hash_hex = hex::encode(file_hash);
    download_dir.join(format!(".peers-{}", hash_hex))
}

/// Save discovered peers to disk
pub fn save_peers(
    download_dir: &Path,
    file_hash: &[u8; 16],
    peers: &[SocketAddr],
) -> Result<(), PeerCacheError> {
    let path = peer_cache_path(download_dir, file_hash);

    let mut data = Vec::with_capacity(4 + 1 + 16 + 2 + peers.len() * 6);
    data.extend_from_slice(MAGIC);
    data.push(VERSION);
    data.extend_from_slice(file_hash);

    // Filter to IPv4 only (ed2k is IPv4)
    let ipv4_peers: Vec<_> = peers
        .iter()
        .filter_map(|addr| match addr {
            SocketAddr::V4(v4) => Some(v4),
            _ => None,
        })
        .collect();

    let count = ipv4_peers.len().min(u16::MAX as usize) as u16;
    data.extend_from_slice(&count.to_le_bytes());

    for peer in &ipv4_peers[..count as usize] {
        data.extend_from_slice(&peer.ip().octets());
        data.extend_from_slice(&peer.port().to_le_bytes());
    }

    std::fs::write(&path, &data).map_err(|e| PeerCacheError::Io(e.to_string()))?;
    tracing::debug!(path = %path.display(), count = ipv4_peers.len(), "Saved ed2k peers");
    Ok(())
}

/// Load cached peers from disk
pub fn load_peers(
    download_dir: &Path,
    file_hash: &[u8; 16],
) -> Result<Vec<SocketAddr>, PeerCacheError> {
    let path = peer_cache_path(download_dir, file_hash);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let data = std::fs::read(&path).map_err(|e| PeerCacheError::Io(e.to_string()))?;

    // Validate minimum size
    if data.len() < 4 + 1 + 16 + 2 {
        tracing::warn!(path = %path.display(), "Peer cache too small, ignoring");
        return Ok(Vec::new());
    }

    // Check magic
    if &data[0..4] != MAGIC {
        tracing::warn!(path = %path.display(), "Invalid peer cache magic, ignoring");
        return Ok(Vec::new());
    }

    // Check version
    if data[4] != VERSION {
        tracing::warn!(
            version = data[4],
            "Unsupported peer cache version, ignoring"
        );
        return Ok(Vec::new());
    }

    // Check file hash matches
    let cached_hash = &data[5..21];
    if cached_hash != file_hash {
        tracing::warn!("Peer cache hash mismatch, ignoring");
        return Ok(Vec::new());
    }

    // Read peer count
    let count = u16::from_le_bytes([data[21], data[22]]) as usize;

    // Validate data length
    let expected_len = 4 + 1 + 16 + 2 + count * 6;
    if data.len() < expected_len {
        tracing::warn!(
            expected = expected_len,
            actual = data.len(),
            "Peer cache truncated"
        );
        return Ok(Vec::new());
    }

    // Parse peers
    let mut peers = Vec::with_capacity(count);
    let mut offset = 23;
    for _ in 0..count {
        let ip = Ipv4Addr::new(
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        );
        let port = u16::from_le_bytes([data[offset + 4], data[offset + 5]]);
        peers.push(SocketAddr::V4(SocketAddrV4::new(ip, port)));
        offset += 6;
    }

    tracing::debug!(path = %path.display(), count = peers.len(), "Loaded cached ed2k peers");
    Ok(peers)
}

/// Remove peer cache file
pub fn remove_peer_cache(download_dir: &Path, file_hash: &[u8; 16]) -> Result<(), PeerCacheError> {
    let path = peer_cache_path(download_dir, file_hash);
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| PeerCacheError::Io(e.to_string()))?;
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum PeerCacheError {
    #[error("IO error: {0}")]
    Io(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use tempfile::TempDir;

    fn test_hash() -> [u8; 16] {
        [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ]
    }

    fn test_peers() -> Vec<SocketAddr> {
        vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 4662)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 4672)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(172, 16, 0, 1), 4662)),
        ]
    }

    #[test]
    fn test_save_and_load_peers() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0], peers[0]);
        assert_eq!(loaded[1], peers[1]);
        assert_eq!(loaded[2], peers[2]);
    }

    #[test]
    fn test_load_nonexistent() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();

        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_hash_mismatch() {
        let dir = TempDir::new().unwrap();
        let hash1 = test_hash();
        let hash2 = [0u8; 16];
        let peers = test_peers();

        save_peers(dir.path(), &hash1, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash2).unwrap();

        assert!(loaded.is_empty());
    }

    #[test]
    fn test_remove_cache() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();

        save_peers(dir.path(), &hash, &peers).unwrap();
        assert!(peer_cache_path(dir.path(), &hash).exists());

        remove_peer_cache(dir.path(), &hash).unwrap();
        assert!(!peer_cache_path(dir.path(), &hash).exists());
    }

    #[test]
    fn test_empty_peers() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers: Vec<SocketAddr> = vec![];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();

        assert!(loaded.is_empty());
    }

    #[test]
    fn test_ipv6_filtered() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 4662)),
            "[::1]:4662".parse().unwrap(), // IPv6 - should be filtered out
        ];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], peers[0]);
    }

    #[test]
    fn test_corrupt_file() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        // Write garbage
        std::fs::write(&path, b"not a valid cache").unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();

        assert!(loaded.is_empty());
    }
}
