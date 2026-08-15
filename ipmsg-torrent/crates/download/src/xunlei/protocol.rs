//! Xunlei P2SP protocol types

use std::net::SocketAddr;

/// P2SP source types
#[derive(Debug, Clone)]
pub enum XunleiSource {
    /// HTTP/FTP server source
    Http {
        url: String,
        cookies: Option<String>,
        referer: Option<String>,
    },
    /// P2P peer source
    Peer { addr: SocketAddr, peer_id: [u8; 20] },
    /// Xunlei CDN source
    Cdn { url: String, token: Option<String> },
}

/// P2SP block state
#[derive(Debug, Clone)]
pub struct P2spBlock {
    pub offset: u64,
    pub size: u64,
    pub source: usize, // Index into sources list
    pub downloaded: bool,
    pub data: Option<Vec<u8>>,
}

/// Download progress
#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub total_size: u64,
    pub downloaded: u64,
    pub speed: f64, // bytes per second
    pub sources_count: usize,
    pub completed_blocks: usize,
    pub total_blocks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_xunlei_source_http_basic() {
        let source = XunleiSource::Http {
            url: "http://example.com/file.txt".to_string(),
            cookies: None,
            referer: None,
        };

        match source {
            XunleiSource::Http {
                url,
                cookies,
                referer,
            } => {
                assert_eq!(url, "http://example.com/file.txt");
                assert!(cookies.is_none());
                assert!(referer.is_none());
            }
            _ => panic!("Expected Http variant"),
        }
    }

    #[test]
    fn test_xunlei_source_http_with_cookies() {
        let source = XunleiSource::Http {
            url: "https://example.com/file.txt".to_string(),
            cookies: Some("session=abc123".to_string()),
            referer: Some("https://example.com".to_string()),
        };

        match source {
            XunleiSource::Http {
                url,
                cookies,
                referer,
            } => {
                assert_eq!(url, "https://example.com/file.txt");
                assert_eq!(cookies, Some("session=abc123".to_string()));
                assert_eq!(referer, Some("https://example.com".to_string()));
            }
            _ => panic!("Expected Http variant"),
        }
    }

    #[test]
    fn test_xunlei_source_peer() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
        let peer_id = [42u8; 20];
        let source = XunleiSource::Peer { addr, peer_id };

        match source {
            XunleiSource::Peer {
                addr: a,
                peer_id: p,
            } => {
                assert_eq!(a, addr);
                assert_eq!(p, peer_id);
            }
            _ => panic!("Expected Peer variant"),
        }
    }

    #[test]
    fn test_xunlei_source_peer_zero_id() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);
        let peer_id = [0u8; 20];
        let source = XunleiSource::Peer { addr, peer_id };

        match source {
            XunleiSource::Peer {
                addr: a,
                peer_id: p,
            } => {
                assert_eq!(a, addr);
                assert_eq!(p, [0u8; 20]);
            }
            _ => panic!("Expected Peer variant"),
        }
    }

    #[test]
    fn test_xunlei_source_cdn_basic() {
        let source = XunleiSource::Cdn {
            url: "https://cdn.example.com/file.txt".to_string(),
            token: None,
        };

        match source {
            XunleiSource::Cdn { url, token } => {
                assert_eq!(url, "https://cdn.example.com/file.txt");
                assert!(token.is_none());
            }
            _ => panic!("Expected Cdn variant"),
        }
    }

    #[test]
    fn test_xunlei_source_cdn_with_token() {
        let source = XunleiSource::Cdn {
            url: "https://cdn.example.com/file.txt".to_string(),
            token: Some("secret_token_123".to_string()),
        };

        match source {
            XunleiSource::Cdn { url, token } => {
                assert_eq!(url, "https://cdn.example.com/file.txt");
                assert_eq!(token, Some("secret_token_123".to_string()));
            }
            _ => panic!("Expected Cdn variant"),
        }
    }

    #[test]
    fn test_xunlei_source_clone_http() {
        let source = XunleiSource::Http {
            url: "http://example.com/file.txt".to_string(),
            cookies: Some("session=abc".to_string()),
            referer: None,
        };

        let cloned = source.clone();
        match cloned {
            XunleiSource::Http { url, cookies, .. } => {
                assert_eq!(url, "http://example.com/file.txt");
                assert_eq!(cookies, Some("session=abc".to_string()));
            }
            _ => panic!("Clone failed"),
        }
    }

    #[test]
    fn test_xunlei_source_clone_peer() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080);
        let peer_id = [1u8; 20];
        let source = XunleiSource::Peer { addr, peer_id };

        let cloned = source.clone();
        match cloned {
            XunleiSource::Peer {
                addr: a,
                peer_id: p,
            } => {
                assert_eq!(a, addr);
                assert_eq!(p, peer_id);
            }
            _ => panic!("Clone failed"),
        }
    }

    #[test]
    fn test_xunlei_source_clone_cdn() {
        let source = XunleiSource::Cdn {
            url: "https://cdn.example.com/file.txt".to_string(),
            token: Some("token".to_string()),
        };

        let cloned = source.clone();
        match cloned {
            XunleiSource::Cdn { url, token } => {
                assert_eq!(url, "https://cdn.example.com/file.txt");
                assert_eq!(token, Some("token".to_string()));
            }
            _ => panic!("Clone failed"),
        }
    }

    #[test]
    fn test_xunlei_source_debug() {
        let source = XunleiSource::Http {
            url: "http://example.com/file.txt".to_string(),
            cookies: None,
            referer: None,
        };

        let debug_str = format!("{:?}", source);
        assert!(debug_str.contains("Http"));
        assert!(debug_str.contains("http://example.com/file.txt"));
    }

    #[test]
    fn test_p2sp_block_basic() {
        let block = P2spBlock {
            offset: 0,
            size: 1024,
            source: 0,
            downloaded: false,
            data: None,
        };

        assert_eq!(block.offset, 0);
        assert_eq!(block.size, 1024);
        assert_eq!(block.source, 0);
        assert!(!block.downloaded);
        assert!(block.data.is_none());
    }

    #[test]
    fn test_p2sp_block_downloaded() {
        let block = P2spBlock {
            offset: 1024,
            size: 1024,
            source: 1,
            downloaded: true,
            data: Some(vec![0xAB; 1024]),
        };

        assert_eq!(block.offset, 1024);
        assert_eq!(block.size, 1024);
        assert_eq!(block.source, 1);
        assert!(block.downloaded);
        assert!(block.data.is_some());
        assert_eq!(block.data.as_ref().unwrap().len(), 1024);
    }

    #[test]
    fn test_p2sp_block_large_offset() {
        let block = P2spBlock {
            offset: u64::MAX,
            size: 1,
            source: 0,
            downloaded: false,
            data: None,
        };

        assert_eq!(block.offset, u64::MAX);
        assert_eq!(block.size, 1);
    }

    #[test]
    fn test_p2sp_block_clone() {
        let block = P2spBlock {
            offset: 512,
            size: 256,
            source: 2,
            downloaded: true,
            data: Some(vec![0xCD; 256]),
        };

        let cloned = block.clone();
        assert_eq!(cloned.offset, 512);
        assert_eq!(cloned.size, 256);
        assert_eq!(cloned.source, 2);
        assert!(cloned.downloaded);
        assert_eq!(cloned.data.as_ref().unwrap().len(), 256);
    }

    #[test]
    fn test_p2sp_block_debug() {
        let block = P2spBlock {
            offset: 0,
            size: 1024,
            source: 0,
            downloaded: false,
            data: None,
        };

        let debug_str = format!("{:?}", block);
        assert!(debug_str.contains("P2spBlock"));
        assert!(debug_str.contains("offset"));
        assert!(debug_str.contains("size"));
    }

    #[test]
    fn test_download_progress_basic() {
        let progress = DownloadProgress {
            total_size: 1024 * 1024,
            downloaded: 512 * 1024,
            speed: 1024.0,
            sources_count: 3,
            completed_blocks: 5,
            total_blocks: 10,
        };

        assert_eq!(progress.total_size, 1024 * 1024);
        assert_eq!(progress.downloaded, 512 * 1024);
        assert_eq!(progress.speed, 1024.0);
        assert_eq!(progress.sources_count, 3);
        assert_eq!(progress.completed_blocks, 5);
        assert_eq!(progress.total_blocks, 10);
    }

    #[test]
    fn test_download_progress_zero() {
        let progress = DownloadProgress {
            total_size: 0,
            downloaded: 0,
            speed: 0.0,
            sources_count: 0,
            completed_blocks: 0,
            total_blocks: 0,
        };

        assert_eq!(progress.total_size, 0);
        assert_eq!(progress.downloaded, 0);
        assert_eq!(progress.speed, 0.0);
        assert_eq!(progress.sources_count, 0);
        assert_eq!(progress.completed_blocks, 0);
        assert_eq!(progress.total_blocks, 0);
    }

    #[test]
    fn test_download_progress_complete() {
        let progress = DownloadProgress {
            total_size: 1024,
            downloaded: 1024,
            speed: 512.0,
            sources_count: 2,
            completed_blocks: 10,
            total_blocks: 10,
        };

        assert_eq!(progress.downloaded, progress.total_size);
        assert_eq!(progress.completed_blocks, progress.total_blocks);
    }

    #[test]
    fn test_download_progress_large_file() {
        let progress = DownloadProgress {
            total_size: 10 * 1024 * 1024 * 1024, // 10 GB
            downloaded: 5 * 1024 * 1024 * 1024,  // 5 GB
            speed: 10_000_000.0,                 // 10 MB/s
            sources_count: 5,
            completed_blocks: 500,
            total_blocks: 1000,
        };

        assert_eq!(progress.total_size, 10 * 1024 * 1024 * 1024);
        assert_eq!(progress.downloaded, 5 * 1024 * 1024 * 1024);
        assert_eq!(progress.speed, 10_000_000.0);
    }

    #[test]
    fn test_download_progress_clone() {
        let progress = DownloadProgress {
            total_size: 1024,
            downloaded: 512,
            speed: 100.0,
            sources_count: 2,
            completed_blocks: 5,
            total_blocks: 10,
        };

        let cloned = progress.clone();
        assert_eq!(cloned.total_size, 1024);
        assert_eq!(cloned.downloaded, 512);
        assert_eq!(cloned.speed, 100.0);
        assert_eq!(cloned.sources_count, 2);
        assert_eq!(cloned.completed_blocks, 5);
        assert_eq!(cloned.total_blocks, 10);
    }

    #[test]
    fn test_download_progress_debug() {
        let progress = DownloadProgress {
            total_size: 1024,
            downloaded: 512,
            speed: 100.0,
            sources_count: 2,
            completed_blocks: 5,
            total_blocks: 10,
        };

        let debug_str = format!("{:?}", progress);
        assert!(debug_str.contains("DownloadProgress"));
        assert!(debug_str.contains("total_size"));
        assert!(debug_str.contains("downloaded"));
        assert!(debug_str.contains("speed"));
    }

    #[test]
    fn test_download_progress_speed_calculation() {
        // Test that speed field can represent various speeds
        let speeds = vec![
            0.0,           // No speed
            1.0,           // 1 B/s
            1024.0,        // 1 KB/s
            1_048_576.0,   // 1 MB/s
            10_485_760.0,  // 10 MB/s
            100_000_000.0, // 100 MB/s
        ];

        for speed in speeds {
            let progress = DownloadProgress {
                total_size: 1024,
                downloaded: 512,
                speed,
                sources_count: 1,
                completed_blocks: 5,
                total_blocks: 10,
            };
            assert_eq!(progress.speed, speed);
        }
    }

    #[test]
    fn test_p2sp_block_multiple_sources() {
        // Test blocks assigned to different sources
        let blocks = vec![
            P2spBlock {
                offset: 0,
                size: 1024,
                source: 0,
                downloaded: false,
                data: None,
            },
            P2spBlock {
                offset: 1024,
                size: 1024,
                source: 1,
                downloaded: false,
                data: None,
            },
            P2spBlock {
                offset: 2048,
                size: 1024,
                source: 2,
                downloaded: false,
                data: None,
            },
        ];

        assert_eq!(blocks[0].source, 0);
        assert_eq!(blocks[1].source, 1);
        assert_eq!(blocks[2].source, 2);
    }

    #[test]
    fn test_xunlei_source_http_unicode() {
        let source = XunleiSource::Http {
            url: "https://example.com/文件.txt".to_string(),
            cookies: None,
            referer: None,
        };

        match source {
            XunleiSource::Http { url, .. } => {
                assert_eq!(url, "https://example.com/文件.txt");
            }
            _ => panic!("Expected Http variant"),
        }
    }

    #[test]
    fn test_xunlei_source_cdn_unicode() {
        let source = XunleiSource::Cdn {
            url: "https://cdn.example.com/文件.txt".to_string(),
            token: Some("令牌".to_string()),
        };

        match source {
            XunleiSource::Cdn { url, token } => {
                assert_eq!(url, "https://cdn.example.com/文件.txt");
                assert_eq!(token, Some("令牌".to_string()));
            }
            _ => panic!("Expected Cdn variant"),
        }
    }
}
