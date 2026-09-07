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
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
    use tempfile::TempDir;

    fn test_servers() -> Vec<SocketAddr> {
        vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 4242)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 4242)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(172, 16, 0, 1), 4242)),
        ]
    }

    // ── Basic functionality tests ──

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

    // ── Constants tests ──

    #[test]
    fn test_magic_constant() {
        assert_eq!(MAGIC, b"ESCC");
        assert_eq!(MAGIC.len(), 4);
    }

    #[test]
    fn test_version_constant() {
        assert_eq!(VERSION, 1);
    }

    // ── server_cache_path tests ──

    #[test]
    fn test_server_cache_path_basic() {
        let path = server_cache_path(Path::new("/tmp/downloads"));
        assert_eq!(path, PathBuf::from("/tmp/downloads/.ed2k_servers"));
    }

    #[test]
    fn test_server_cache_path_relative() {
        let path = server_cache_path(Path::new("downloads"));
        assert_eq!(path, PathBuf::from("downloads/.ed2k_servers"));
    }

    #[test]
    fn test_server_cache_path_hidden_file() {
        let path = server_cache_path(Path::new("/home/user"));
        assert!(path.ends_with(".ed2k_servers"));
        assert!(path.file_name().unwrap().to_str().unwrap().starts_with('.'));
    }

    #[test]
    fn test_server_cache_path_nested() {
        let path = server_cache_path(Path::new("/a/b/c/d"));
        assert_eq!(path, PathBuf::from("/a/b/c/d/.ed2k_servers"));
    }

    // ── Error Display tests ──

    #[test]
    fn test_error_display_io() {
        let err = ServerCacheError::Io("permission denied".to_string());
        assert_eq!(err.to_string(), "IO error: permission denied");
    }

    #[test]
    fn test_error_display_empty_message() {
        let err = ServerCacheError::Io("".to_string());
        assert_eq!(err.to_string(), "IO error: ");
    }

    #[test]
    fn test_error_display_unicode() {
        let err = ServerCacheError::Io("文件不存在".to_string());
        assert_eq!(err.to_string(), "IO error: 文件不存在");
    }

    #[test]
    fn test_error_debug() {
        let err = ServerCacheError::Io("test error".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Io"));
        assert!(debug_str.contains("test error"));
    }

    // ── Port boundary tests ──

    #[test]
    fn test_port_zero() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 1, 1),
            0,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].port(), 0);
    }

    #[test]
    fn test_port_max() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 1, 1),
            u16::MAX,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].port(), u16::MAX);
    }

    #[test]
    fn test_port_typical_ed2k() {
        let dir = TempDir::new().unwrap();
        // Typical ed2k server ports
        let servers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 4242)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 4661)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(172, 16, 0, 1), 4662)),
        ];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].port(), 4242);
        assert_eq!(loaded[1].port(), 4661);
        assert_eq!(loaded[2].port(), 4662);
    }

    // ── IP address boundary tests ──

    #[test]
    fn test_ip_all_zeros() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(0, 0, 0, 0),
            4242,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].ip(), IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
    }

    #[test]
    fn test_ip_all_255() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(255, 255, 255, 255),
            4242,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].ip(),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255))
        );
    }

    #[test]
    fn test_ip_loopback() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            4242,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    }

    #[test]
    fn test_ip_private_class_a() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(10, 255, 255, 255),
            4242,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded[0].ip(), IpAddr::V4(Ipv4Addr::new(10, 255, 255, 255)));
    }

    #[test]
    fn test_ip_private_class_b() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(172, 31, 255, 255),
            4242,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded[0].ip(), IpAddr::V4(Ipv4Addr::new(172, 31, 255, 255)));
    }

    #[test]
    fn test_ip_private_class_c() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 255, 255),
            4242,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(
            loaded[0].ip(),
            IpAddr::V4(Ipv4Addr::new(192, 168, 255, 255))
        );
    }

    // ── Truncated data tests ──

    #[test]
    fn test_truncated_header_too_short() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Only 3 bytes (need at least 7 for header)
        std::fs::write(&path, b"ESC").unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_truncated_header_4_bytes() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Only magic, no version or count
        std::fs::write(&path, MAGIC).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_truncated_header_5_bytes() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Magic + version, no count
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        std::fs::write(&path, &data).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_truncated_header_6_bytes() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Magic + version + 1 byte of count
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.push(1); // only 1 byte of count
        std::fs::write(&path, &data).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_truncated_server_data() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Valid header claiming 1 server but no server data
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&1u16.to_le_bytes()); // count = 1
        // No server data follows
        std::fs::write(&path, &data).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_truncated_server_data_partial() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Valid header claiming 2 servers but only 1 server's data
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&2u16.to_le_bytes()); // count = 2
        // Only 1 server (6 bytes)
        data.extend_from_slice(&[192, 168, 1, 1]);
        data.extend_from_slice(&4242u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    // ── Wrong magic tests ──

    #[test]
    fn test_wrong_magic_all_zeros() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        let mut data = Vec::new();
        data.extend_from_slice(&[0, 0, 0, 0]);
        data.push(VERSION);
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_wrong_magic_similar() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Similar but wrong magic
        std::fs::write(&path, b"ESCD").unwrap(); // D instead of C
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_wrong_magic_lowercase() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        std::fs::write(&path, b"escc").unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    // ── Wrong version tests ──

    #[test]
    fn test_wrong_version_zero() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(0); // version 0
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_wrong_version_future() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(2); // future version
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_wrong_version_max() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(u8::MAX);
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    // ── Multiple save/load cycle tests ──

    #[test]
    fn test_overwrite_existing_cache() {
        let dir = TempDir::new().unwrap();

        // First save
        let servers1 = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 1, 1),
            4242,
        ))];
        save_servers(dir.path(), &servers1).unwrap();

        // Second save (overwrite)
        let servers2 = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 4242)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 4242)),
        ];
        save_servers(dir.path(), &servers2).unwrap();

        let loaded = load_servers(dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(loaded[1].ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
    }

    #[test]
    fn test_save_load_save_load_cycle() {
        let dir = TempDir::new().unwrap();

        for i in 0..5 {
            let servers = vec![SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(192, 168, 1, i as u8),
                4242 + i as u16,
            ))];
            save_servers(dir.path(), &servers).unwrap();
            let loaded = load_servers(dir.path()).unwrap();
            assert_eq!(loaded.len(), 1);
            assert_eq!(
                loaded[0].ip(),
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, i as u8))
            );
        }
    }

    // ── Remove edge cases ──

    #[test]
    fn test_remove_nonexistent_cache() {
        let dir = TempDir::new().unwrap();
        // Should not error when file doesn't exist
        let result = remove_server_cache(dir.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_twice() {
        let dir = TempDir::new().unwrap();
        let servers = test_servers();

        save_servers(dir.path(), &servers).unwrap();
        remove_server_cache(dir.path()).unwrap();
        // Second remove should also succeed
        remove_server_cache(dir.path()).unwrap();
    }

    // ── Many servers tests ──

    #[test]
    fn test_many_servers() {
        let dir = TempDir::new().unwrap();
        let servers: Vec<SocketAddr> = (0..100)
            .map(|i| {
                SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(192, 168, (i / 256) as u8, (i % 256) as u8),
                    4242,
                ))
            })
            .collect();

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 100);
    }

    #[test]
    fn test_250_servers() {
        let dir = TempDir::new().unwrap();
        let servers: Vec<SocketAddr> = (0..250)
            .map(|i| {
                SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(10, 0, (i / 256) as u8, (i % 256) as u8),
                    4661,
                ))
            })
            .collect();

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 250);
    }

    // ── IPv6 filtering tests ──

    #[test]
    fn test_all_ipv6_filtered() {
        let dir = TempDir::new().unwrap();
        let servers = vec![
            "[::1]:4242".parse().unwrap(),
            "[::2]:4242".parse().unwrap(),
            "[fe80::1]:4242".parse().unwrap(),
        ];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert!(loaded.is_empty());
    }

    #[test]
    fn test_mixed_ipv4_ipv6_order_preserved() {
        let dir = TempDir::new().unwrap();
        let servers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 1)),
            "[::1]:4242".parse().unwrap(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), 2)),
            "[fe80::1]:8080".parse().unwrap(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(3, 3, 3, 3), 3)),
        ];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].ip(), IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
        assert_eq!(loaded[1].ip(), IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)));
        assert_eq!(loaded[2].ip(), IpAddr::V4(Ipv4Addr::new(3, 3, 3, 3)));
    }

    // ── Empty file test ──

    #[test]
    fn test_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        std::fs::write(&path, b"").unwrap();
        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    // ── File size boundary tests ──

    #[test]
    fn test_exactly_header_size() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Exactly 7 bytes (header size) with 0 servers
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_header_plus_one_server() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Exactly 13 bytes (header + 1 server)
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&[192, 168, 1, 1]);
        data.extend_from_slice(&4242u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let loaded = load_servers(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
    }

    // ── Extra data tests ──

    #[test]
    fn test_extra_data_ignored() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        // Valid data + extra garbage at end
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&[192, 168, 1, 1]);
        data.extend_from_slice(&4242u16.to_le_bytes());
        data.extend_from_slice(b"extra garbage data");
        std::fs::write(&path, &data).unwrap();

        let loaded = load_servers(dir.path()).unwrap();
        // Should still load the valid server
        assert_eq!(loaded.len(), 1);
    }

    // ── Unicode path tests ──

    #[test]
    fn test_unicode_directory_path() {
        let dir = TempDir::new().unwrap();
        let unicode_dir = dir.path().join("下载目录");
        std::fs::create_dir(&unicode_dir).unwrap();

        let servers = test_servers();
        save_servers(&unicode_dir, &servers).unwrap();
        let loaded = load_servers(&unicode_dir).unwrap();

        assert_eq!(loaded.len(), 3);
    }

    #[test]
    fn test_emoji_directory_path() {
        let dir = TempDir::new().unwrap();
        let emoji_dir = dir.path().join("📥downloads");
        std::fs::create_dir(&emoji_dir).unwrap();

        let servers = test_servers();
        save_servers(&emoji_dir, &servers).unwrap();
        let loaded = load_servers(&emoji_dir).unwrap();

        assert_eq!(loaded.len(), 3);
    }

    // ── Binary format verification ──

    #[test]
    fn test_binary_format_structure() {
        let dir = TempDir::new().unwrap();
        let servers = vec![SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 1, 1),
            4242,
        ))];

        save_servers(dir.path(), &servers).unwrap();
        let path = server_cache_path(dir.path());
        let data = std::fs::read(&path).unwrap();

        // Verify structure
        assert_eq!(&data[0..4], MAGIC);
        assert_eq!(data[4], VERSION);
        let count = u16::from_le_bytes([data[5], data[6]]);
        assert_eq!(count, 1);

        // Verify IP bytes
        assert_eq!(data[7], 192);
        assert_eq!(data[8], 168);
        assert_eq!(data[9], 1);
        assert_eq!(data[10], 1);

        // Verify port (LE)
        let port = u16::from_le_bytes([data[11], data[12]]);
        assert_eq!(port, 4242);
    }

    #[test]
    fn test_binary_format_empty() {
        let dir = TempDir::new().unwrap();
        let servers: Vec<SocketAddr> = vec![];

        save_servers(dir.path(), &servers).unwrap();
        let path = server_cache_path(dir.path());
        let data = std::fs::read(&path).unwrap();

        // Should be exactly header size
        assert_eq!(data.len(), 7);
        assert_eq!(&data[0..4], MAGIC);
        assert_eq!(data[4], VERSION);
        let count = u16::from_le_bytes([data[5], data[6]]);
        assert_eq!(count, 0);
    }

    // ── Count field boundary tests ──

    #[test]
    fn test_count_field_zero() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&0u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let loaded = load_servers(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_count_field_one() {
        let dir = TempDir::new().unwrap();
        let path = server_cache_path(dir.path());

        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&[10, 0, 0, 1]);
        data.extend_from_slice(&8080u16.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let loaded = load_servers(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
    }

    // ── Roundtrip integrity tests ──

    #[test]
    fn test_roundtrip_preserves_order() {
        let dir = TempDir::new().unwrap();
        let servers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 1111)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 2222)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(9, 10, 11, 12), 3333)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(13, 14, 15, 16), 4444)),
        ];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        for (orig, load) in servers.iter().zip(loaded.iter()) {
            assert_eq!(orig, load);
        }
    }

    #[test]
    fn test_roundtrip_all_ip_octets() {
        let dir = TempDir::new().unwrap();
        // Test various IP patterns
        let servers = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 1)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 0, 0, 0), 2)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 1, 0, 0), 3)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 1, 0), 4)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 1), 5)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(255, 128, 64, 32), 6)),
        ];

        save_servers(dir.path(), &servers).unwrap();
        let loaded = load_servers(dir.path()).unwrap();

        assert_eq!(loaded.len(), 6);
        for (orig, load) in servers.iter().zip(loaded.iter()) {
            assert_eq!(orig.ip(), load.ip());
            assert_eq!(orig.port(), load.port());
        }
    }

    // ── Persistence path tests ──

    #[test]
    fn test_persistence_file_created() {
        let dir = TempDir::new().unwrap();
        let servers = test_servers();

        save_servers(dir.path(), &servers).unwrap();

        let path = server_cache_path(dir.path());
        assert!(path.exists());
    }

    #[test]
    fn test_persistence_no_tmp_leftover() {
        let dir = TempDir::new().unwrap();
        let servers = test_servers();

        save_servers(dir.path(), &servers).unwrap();

        // Check no .tmp file left
        let tmp_path = dir.path().join(".ed2k_servers.tmp");
        assert!(!tmp_path.exists());
    }
}
