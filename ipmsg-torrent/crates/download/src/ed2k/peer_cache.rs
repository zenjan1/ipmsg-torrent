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

    // ===== Phase 250: Comprehensive Test Coverage =====

    // --- Constants verification ---
    #[test]
    fn test_magic_constant() {
        assert_eq!(MAGIC, b"EPCC");
        assert_eq!(MAGIC.len(), 4);
    }

    #[test]
    fn test_version_constant() {
        assert_eq!(VERSION, 1);
    }

    // --- peer_cache_path ---
    #[test]
    fn test_peer_cache_path_format() {
        let dir = TempDir::new().unwrap();
        let hash = [0xAB; 16];
        let path = peer_cache_path(dir.path(), &hash);
        let expected = dir.path().join(".peers-abababababababababababababababab");
        assert_eq!(path, expected);
    }

    #[test]
    fn test_peer_cache_path_all_zeros() {
        let dir = TempDir::new().unwrap();
        let hash = [0x00; 16];
        let path = peer_cache_path(dir.path(), &hash);
        assert!(
            path.to_str()
                .unwrap()
                .contains(".peers-00000000000000000000000000000000")
        );
    }

    #[test]
    fn test_peer_cache_path_all_ff() {
        let dir = TempDir::new().unwrap();
        let hash = [0xFF; 16];
        let path = peer_cache_path(dir.path(), &hash);
        assert!(
            path.to_str()
                .unwrap()
                .contains(".peers-ffffffffffffffffffffffffffffffff")
        );
    }

    #[test]
    fn test_peer_cache_path_hidden_file() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);
        let filename = path.file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with('.'));
    }

    #[test]
    fn test_peer_cache_path_different_hashes() {
        let dir = TempDir::new().unwrap();
        let hash1 = [0x11; 16];
        let hash2 = [0x22; 16];
        let path1 = peer_cache_path(dir.path(), &hash1);
        let path2 = peer_cache_path(dir.path(), &hash2);
        assert_ne!(path1, path2);
    }

    // --- save_peers ---
    #[test]
    fn test_save_creates_file() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();
        let path = peer_cache_path(dir.path(), &hash);

        assert!(!path.exists());
        save_peers(dir.path(), &hash, &peers).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_save_overwrites_existing() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers1 = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(1, 2, 3, 4),
            1000,
        ))];
        let peers2 = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 2000)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(9, 10, 11, 12), 3000)),
        ];

        save_peers(dir.path(), &hash, &peers1).unwrap();
        let loaded1 = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded1.len(), 1);

        save_peers(dir.path(), &hash, &peers2).unwrap();
        let loaded2 = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded2.len(), 2);
    }

    #[test]
    fn test_save_all_ipv6_filtered() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![
            "[::1]:4662".parse().unwrap(),
            "[fe80::1]:4662".parse().unwrap(),
            "[2001:db8::1]:8080".parse().unwrap(),
        ];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_save_port_zero() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 1, 1),
            0,
        ))];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].port(), 0);
    }

    #[test]
    fn test_save_port_max() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 1, 1),
            65535,
        ))];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].port(), 65535);
    }

    #[test]
    fn test_save_ip_boundaries() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 1000)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(255, 255, 255, 255), 2000)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3000)),
        ];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(
            loaded[0],
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 1000))
        );
        assert_eq!(
            loaded[1],
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(255, 255, 255, 255), 2000))
        );
        assert_eq!(
            loaded[2],
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3000))
        );
    }

    #[test]
    fn test_save_many_peers() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers: Vec<SocketAddr> = (0..100)
            .map(|i| {
                SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(192, 168, (i / 256) as u8, (i % 256) as u8),
                    4662,
                ))
            })
            .collect();

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded.len(), 100);
    }

    // --- Binary format verification ---
    #[test]
    fn test_binary_format_magic() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();
        let path = peer_cache_path(dir.path(), &hash);

        save_peers(dir.path(), &hash, &peers).unwrap();
        let data = std::fs::read(&path).unwrap();

        assert_eq!(&data[0..4], b"EPCC");
    }

    #[test]
    fn test_binary_format_version() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();
        let path = peer_cache_path(dir.path(), &hash);

        save_peers(dir.path(), &hash, &peers).unwrap();
        let data = std::fs::read(&path).unwrap();

        assert_eq!(data[4], 1);
    }

    #[test]
    fn test_binary_format_hash() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();
        let path = peer_cache_path(dir.path(), &hash);

        save_peers(dir.path(), &hash, &peers).unwrap();
        let data = std::fs::read(&path).unwrap();

        assert_eq!(&data[5..21], &hash);
    }

    #[test]
    fn test_binary_format_peer_count() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();
        let path = peer_cache_path(dir.path(), &hash);

        save_peers(dir.path(), &hash, &peers).unwrap();
        let data = std::fs::read(&path).unwrap();

        let count = u16::from_le_bytes([data[21], data[22]]);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_binary_format_peer_data() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 1, 100),
            4662,
        ))];
        let path = peer_cache_path(dir.path(), &hash);

        save_peers(dir.path(), &hash, &peers).unwrap();
        let data = std::fs::read(&path).unwrap();

        // Peer data starts at offset 23
        assert_eq!(data[23], 192);
        assert_eq!(data[24], 168);
        assert_eq!(data[25], 1);
        assert_eq!(data[26], 100);
        let port = u16::from_le_bytes([data[27], data[28]]);
        assert_eq!(port, 4662);
    }

    #[test]
    fn test_binary_format_total_size() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 1000)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 2000)),
        ];
        let path = peer_cache_path(dir.path(), &hash);

        save_peers(dir.path(), &hash, &peers).unwrap();
        let data = std::fs::read(&path).unwrap();

        // 4 (magic) + 1 (version) + 16 (hash) + 2 (count) + 2*6 (peers) = 35
        assert_eq!(data.len(), 35);
    }

    // --- load_peers edge cases ---
    #[test]
    fn test_load_too_small_3_bytes() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        std::fs::write(&path, &[0x45, 0x50, 0x43]).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_load_too_small_header_only() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        // Exactly 23 bytes (header size) but no peers
        let mut data = Vec::new();
        data.extend_from_slice(b"EPCC");
        data.push(1);
        data.extend_from_slice(&hash);
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_load_wrong_magic() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        let mut data = Vec::new();
        data.extend_from_slice(b"XXXX");
        data.push(1);
        data.extend_from_slice(&hash);
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_load_wrong_version() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        let mut data = Vec::new();
        data.extend_from_slice(b"EPCC");
        data.push(99); // Wrong version
        data.extend_from_slice(&hash);
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_load_truncated_peers() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        let mut data = Vec::new();
        data.extend_from_slice(b"EPCC");
        data.push(1);
        data.extend_from_slice(&hash);
        data.extend_from_slice(&5u16.to_le_bytes()); // Claims 5 peers
        data.extend_from_slice(&[192, 168, 1, 1, 0x12, 0x34]); // But only 1 peer's data
        std::fs::write(&path, &data).unwrap();

        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_load_empty_file() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        std::fs::write(&path, b"").unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_load_zero_peers_count() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        let mut data = Vec::new();
        data.extend_from_slice(b"EPCC");
        data.push(1);
        data.extend_from_slice(&hash);
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    // --- remove_peer_cache ---
    #[test]
    fn test_remove_nonexistent_no_error() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();

        let result = remove_peer_cache(dir.path(), &hash);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_idempotent() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();

        save_peers(dir.path(), &hash, &peers).unwrap();
        remove_peer_cache(dir.path(), &hash).unwrap();
        remove_peer_cache(dir.path(), &hash).unwrap();

        assert!(!peer_cache_path(dir.path(), &hash).exists());
    }

    #[test]
    fn test_remove_after_load() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = test_peers();

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded.len(), 3);

        remove_peer_cache(dir.path(), &hash).unwrap();
        let loaded2 = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded2.is_empty());
    }

    // --- PeerCacheError ---
    #[test]
    fn test_error_display() {
        let err = PeerCacheError::Io("disk full".to_string());
        assert_eq!(format!("{}", err), "IO error: disk full");
    }

    #[test]
    fn test_error_debug() {
        let err = PeerCacheError::Io("test error".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Io"));
        assert!(debug.contains("test error"));
    }

    #[test]
    fn test_error_unicode_message() {
        let err = PeerCacheError::Io("磁盘已满".to_string());
        assert_eq!(format!("{}", err), "IO error: 磁盘已满");
    }

    #[test]
    fn test_error_empty_message() {
        let err = PeerCacheError::Io(String::new());
        assert_eq!(format!("{}", err), "IO error: ");
    }

    // --- Peer ordering preservation ---
    #[test]
    fn test_peer_order_preserved() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 1000)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 2000)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 3), 3000)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 4), 4000)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 5), 5000)),
        ];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();

        for (i, peer) in peers.iter().enumerate() {
            assert_eq!(loaded[i], *peer, "Peer order mismatch at index {}", i);
        }
    }

    // --- Mixed IPv4/IPv6 ---
    #[test]
    fn test_mixed_ipv4_ipv6_order() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 1000)),
            "[::1]:2000".parse().unwrap(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 3000)),
            "[fe80::1]:4000".parse().unwrap(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(9, 10, 11, 12), 5000)),
        ];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(
            loaded[0],
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 1000))
        );
        assert_eq!(
            loaded[1],
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 3000))
        );
        assert_eq!(
            loaded[2],
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(9, 10, 11, 12), 5000))
        );
    }

    // --- Hash with all byte values ---
    #[test]
    fn test_hash_all_byte_values() {
        let dir = TempDir::new().unwrap();
        let hash: [u8; 16] = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ];
        let peers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            8080,
        ))];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded.len(), 1);
    }

    // --- Complete workflow ---
    #[test]
    fn test_complete_lifecycle() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();

        // Initially empty
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());

        // Save peers
        let peers = test_peers();
        save_peers(dir.path(), &hash, &peers).unwrap();

        // Load and verify
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded.len(), 3);

        // Remove cache
        remove_peer_cache(dir.path(), &hash).unwrap();

        // Verify empty again
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_multiple_hashes_independent() {
        let dir = TempDir::new().unwrap();
        let hash1 = [0x11; 16];
        let hash2 = [0x22; 16];

        let peers1 = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(1, 1, 1, 1),
            1111,
        ))];
        let peers2 = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), 2222)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(3, 3, 3, 3), 3333)),
        ];

        save_peers(dir.path(), &hash1, &peers1).unwrap();
        save_peers(dir.path(), &hash2, &peers2).unwrap();

        let loaded1 = load_peers(dir.path(), &hash1).unwrap();
        let loaded2 = load_peers(dir.path(), &hash2).unwrap();

        assert_eq!(loaded1.len(), 1);
        assert_eq!(loaded2.len(), 2);

        // Remove one doesn't affect the other
        remove_peer_cache(dir.path(), &hash1).unwrap();
        let loaded1_after = load_peers(dir.path(), &hash1).unwrap();
        let loaded2_after = load_peers(dir.path(), &hash2).unwrap();
        assert!(loaded1_after.is_empty());
        assert_eq!(loaded2_after.len(), 2);
    }

    // --- Standard ed2k ports ---
    #[test]
    fn test_standard_ed2k_ports() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let peers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 4662)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 2), 4672)),
        ];

        save_peers(dir.path(), &hash, &peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();
        assert_eq!(loaded[0].port(), 4662);
        assert_eq!(loaded[1].port(), 4672);
    }

    // --- File size verification ---
    #[test]
    fn test_file_size_empty_peers() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        save_peers(dir.path(), &hash, &[]).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        // 4 (magic) + 1 (version) + 16 (hash) + 2 (count) = 23 bytes
        assert_eq!(metadata.len(), 23);
    }

    #[test]
    fn test_file_size_single_peer() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let path = peer_cache_path(dir.path(), &hash);

        let peers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(1, 2, 3, 4),
            5000,
        ))];
        save_peers(dir.path(), &hash, &peers).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        // 23 + 6 = 29 bytes
        assert_eq!(metadata.len(), 29);
    }

    // --- Roundtrip integrity ---
    #[test]
    fn test_roundtrip_exact_values() {
        let dir = TempDir::new().unwrap();
        let hash = test_hash();
        let original_peers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 0, 1), 4662)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 20, 30, 40), 8080)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(255, 255, 255, 0), 65535)),
        ];

        save_peers(dir.path(), &hash, &original_peers).unwrap();
        let loaded = load_peers(dir.path(), &hash).unwrap();

        assert_eq!(loaded.len(), original_peers.len());
        for (orig, loaded_peer) in original_peers.iter().zip(loaded.iter()) {
            assert_eq!(orig.ip(), loaded_peer.ip());
            assert_eq!(orig.port(), loaded_peer.port());
        }
    }
}
