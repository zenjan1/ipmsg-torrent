//! Ed2k server cache - persist known servers to disk
//!
//! Format: simple binary format
//! - Magic: 4 bytes "ESCC" (Ed2k Server Cache)
//! - Version: 1 byte (u8)
//! - Server count: 2 bytes (u16 LE)
//! - Servers: count * (4 bytes IP + 2 bytes port)

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"ESCC";
const VERSION: u8 = 1;

/// Get the server cache file path
pub fn server_cache_path(download_dir: &Path) -> PathBuf {
    download_dir.join(".ed2k_servers")
}

/// Save server list to disk
pub fn save_servers(download_dir: &Path, servers: &[SocketAddr]) -> Result<(), ServerCacheError> {
    let path = server_cache_path(download_dir);

    let mut data = Vec::with_capacity(4 + 1 + 2 + servers.len() * 6);
    data.extend_from_slice(MAGIC);
    data.push(VERSION);

    // Filter to IPv4 only (ed2k is IPv4)
    let ipv4_servers: Vec<_> = servers
        .iter()
        .filter_map(|addr| match addr {
            SocketAddr::V4(v4) => Some(v4),
            _ => None,
        })
        .collect();

    let count = ipv4_servers.len().min(u16::MAX as usize) as u16;
    data.extend_from_slice(&count.to_le_bytes());

    for server in &ipv4_servers[..count as usize] {
        data.extend_from_slice(&server.ip().octets());
        data.extend_from_slice(&server.port().to_le_bytes());
    }

    std::fs::write(&path, &data).map_err(|e| ServerCacheError::Io(e.to_string()))?;
    tracing::debug!(path = %path.display(), count = ipv4_servers.len(), "Saved ed2k servers");
    Ok(())
}

/// Load cached servers from disk
pub fn load_servers(download_dir: &Path) -> Result<Vec<SocketAddr>, ServerCacheError> {
    let path = server_cache_path(download_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let data = std::fs::read(&path).map_err(|e| ServerCacheError::Io(e.to_string()))?;

    // Validate minimum size
    if data.len() < 4 + 1 + 2 {
        tracing::warn!(path = %path.display(), "Server cache too small, ignoring");
        return Ok(Vec::new());
    }

    // Check magic
    if &data[0..4] != MAGIC {
        tracing::warn!(path = %path.display(), "Invalid server cache magic, ignoring");
        return Ok(Vec::new());
    }

    // Check version
    if data[4] != VERSION {
        tracing::warn!(
            version = data[4],
            "Unsupported server cache version, ignoring"
        );
        return Ok(Vec::new());
    }

    // Read server count
    let count = u16::from_le_bytes([data[5], data[6]]) as usize;

    // Validate data length
    let expected_len = 4 + 1 + 2 + count * 6;
    if data.len() < expected_len {
        tracing::warn!(
            expected = expected_len,
            actual = data.len(),
            "Server cache truncated"
        );
        return Ok(Vec::new());
    }

    // Parse servers
    let mut servers = Vec::with_capacity(count);
    let mut offset = 7;
    for _ in 0..count {
        let ip = Ipv4Addr::new(
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        );
        let port = u16::from_le_bytes([data[offset + 4], data[offset + 5]]);
        servers.push(SocketAddr::V4(SocketAddrV4::new(ip, port)));
        offset += 6;
    }

    tracing::debug!(path = %path.display(), count = servers.len(), "Loaded cached ed2k servers");
    Ok(servers)
}

/// Remove server cache file
pub fn remove_server_cache(download_dir: &Path) -> Result<(), ServerCacheError> {
    let path = server_cache_path(download_dir);
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| ServerCacheError::Io(e.to_string()))?;
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ServerCacheError {
    #[error("IO error: {0}")]
    Io(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use tempfile::TempDir;

    fn test_servers() -> Vec<SocketAddr> {
        vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 4242)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 4242)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(172, 16, 0, 1), 4242)),
        ]
    }

    #[test]
    fn test_save_and_load_servers() {
        let dir = TempDir::new().unwrap();
        let servers = test_servers();

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0], servers[0]);
        assert_eq!(loaded[1], servers[1]);
        assert_eq!(loaded[2], servers[2]);
    }

    #[test]
    fn test_load_nonexistent() {
        let dir = TempDir::new().unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_remove_cache() {
        let dir = TempDir::new().unwrap();
        let servers = test_servers();

        save_servers(dir.path(), &servers).unwrap();
        assert!(server_cache_path(dir.path()).exists());

        remove_server_cache(dir.path()).unwrap();
        assert!(!server_cache_path(dir.path()).exists());
    }

    #[test]
    fn test_empty_servers() {
        let dir = TempDir::new().unwrap();
        let servers: Vec<SocketAddr> = vec![];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert!(loaded.is_empty());
    }

    #[test]
    fn test_ipv6_filtered() {
        let dir = TempDir::new().unwrap();
        let servers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 4242)),
            "[::1]:4242".parse().unwrap(), // IPv6 - should be filtered out
        ];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], servers[0]);
    }

    #[test]
    fn test_corrupt_file() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Write garbage
        std::fs::write(&path, b"not a valid cache").unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert!(loaded.is_empty());
    }
}
