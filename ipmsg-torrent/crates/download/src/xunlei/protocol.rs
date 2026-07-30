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
