//! eDonkey download engine

use super::client::Ed2kClient;
use super::protocol::{ED2K_BLOCK_SIZE, ED2K_CHUNK_SIZE, Ed2kFileHash};
use md4::{Digest, Md4};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::time::{Duration, interval};

/// ed2k chunk state
#[derive(Debug, Clone)]
struct ChunkState {
    index: u32,
    size: u64,
    hash: [u8; 16], // MD4 hash (16 bytes)
    blocks_received: HashMap<u64, Vec<u8>>,
    blocks_total: u64,
    complete: bool,
}

impl ChunkState {
    fn new(index: u32, size: u64, hash: [u8; 16]) -> Self {
        let blocks_total = (size as f64 / ED2K_BLOCK_SIZE as f64).ceil() as u64;
        Self {
            index,
            size,
            hash,
            blocks_received: HashMap::new(),
            blocks_total,
            complete: false,
        }
    }

    fn add_block(&mut self, offset: u64, data: Vec<u8>) {
        self.blocks_received.insert(offset, data);
        if self.blocks_received.len() as u64 >= self.blocks_total {
            self.complete = true;
        }
    }

    fn assemble(&self) -> Option<Vec<u8>> {
        if !self.complete {
            return None;
        }
        let mut data = Vec::with_capacity(self.size as usize);
        for i in 0..self.blocks_total {
            let offset = i * ED2K_BLOCK_SIZE;
            let block = self.blocks_received.get(&offset)?;
            data.extend_from_slice(block);
        }
        Some(data)
    }

    fn verify(&self, data: &[u8]) -> bool {
        let hash = Md4::digest(data);
        &hash[..] == self.hash
    }

    fn next_block_needed(&self) -> Option<u64> {
        for i in 0..self.blocks_total {
            let offset = i * ED2K_BLOCK_SIZE;
            if !self.blocks_received.contains_key(&offset) {
                return Some(offset);
            }
        }
        None
    }
}

/// ed2k download engine
pub struct Ed2kEngine {
    file_hash: Ed2kFileHash,
    file_size: u64,
    file_name: String,
    download_dir: PathBuf,
    chunks: Vec<ChunkState>,
    servers: Vec<SocketAddr>,
    peers: HashMap<SocketAddr, Ed2kClient>,
    downloaded_chunks: HashSet<u32>,
    downloaded: u64,
}

impl Ed2kEngine {
    pub fn new(
        file_hash: Ed2kFileHash,
        file_size: u64,
        file_name: String,
        download_dir: PathBuf,
        servers: Vec<SocketAddr>,
    ) -> Self {
        // Initialize chunk states
        let mut chunks = Vec::new();
        let chunk_count = (file_size as f64 / ED2K_CHUNK_SIZE as f64).ceil() as u32;

        for i in 0..chunk_count {
            let chunk_start = i as u64 * ED2K_CHUNK_SIZE;
            let chunk_size = std::cmp::min(ED2K_CHUNK_SIZE, file_size - chunk_start);
            // Chunk hashes are obtained from peers via HashSet message
            // Initially unknown, will be updated when peer sends hash set
            let chunk_hash = [0u8; 16];
            chunks.push(ChunkState::new(i, chunk_size, chunk_hash));
        }

        Self {
            file_hash,
            file_size,
            file_name,
            download_dir,
            chunks,
            servers,
            peers: HashMap::new(),
            downloaded_chunks: HashSet::new(),
            downloaded: 0,
        }
    }

    /// Update chunk hash from peer's HashSet message
    pub fn update_chunk_hash(&mut self, chunk_idx: u32, hash: [u8; 16]) {
        if let Some(chunk) = self.chunks.get_mut(chunk_idx as usize) {
            chunk.hash = hash;
        }
    }

    /// Start the download process
    pub async fn download(&mut self) -> Result<(), Ed2kDownloadError> {
        tracing::info!(
            name = %self.file_name,
            size = self.file_size,
            chunks = self.chunks.len(),
            "Starting ed2k download"
        );

        // Connect to servers and request sources
        let servers = self.servers.clone();
        for server_addr in servers {
            if let Err(e) = self.connect_server(server_addr).await {
                tracing::warn!(addr = %server_addr, error = %e, "Failed to connect to server");
            }
        }

        // Main download loop
        let mut tick = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    // Check if download is complete
                    if self.is_complete() {
                        tracing::info!("Download complete!");
                        break;
                    }

                    // Request blocks from peers
                    self.request_blocks().await;

                    // Handle peer messages
                    self.handle_peer_messages().await;
                }
            }
        }

        // Save file
        self.save_file().await?;

        Ok(())
    }

    async fn connect_server(&mut self, addr: SocketAddr) -> Result<(), Ed2kDownloadError> {
        let mut client = Ed2kClient::connect(addr)
            .await
            .map_err(|e| Ed2kDownloadError::Server(e.to_string()))?;

        // Login to server
        let client_id = rand::random();
        let port = 6882;
        client
            .login(client_id, port)
            .await
            .map_err(|e| Ed2kDownloadError::Server(e.to_string()))?;

        // Request sources for our file
        client
            .request_sources(&self.file_hash)
            .await
            .map_err(|e| Ed2kDownloadError::Server(e.to_string()))?;

        tracing::info!(addr = %addr, "Connected to ed2k server");

        // Parse server responses to get peer list
        self.parse_server_responses(&mut client).await?;

        Ok(())
    }

    async fn parse_server_responses(
        &mut self,
        client: &mut Ed2kClient,
    ) -> Result<(), Ed2kDownloadError> {
        use tokio::time::Duration;

        // Read responses with timeout
        for _ in 0..10 {
            match tokio::time::timeout(Duration::from_secs(2), client.recv()).await {
                Ok(Ok((protocol, payload))) => {
                    match protocol {
                        0x20 => {
                            // LoginAnswer - server accepted our login
                            tracing::debug!("Server login accepted");
                        }
                        0x42 => {
                            // OP_SERVER_ANSWER - contains peer list for our file
                            // Format: [1 byte count][count * (4 bytes IP + 2 bytes port)]
                            if payload.len() >= 1 {
                                let peer_count = payload[0] as usize;
                                let mut offset = 1;

                                for _ in 0..peer_count {
                                    if offset + 6 > payload.len() {
                                        break;
                                    }

                                    let ip = std::net::Ipv4Addr::new(
                                        payload[offset],
                                        payload[offset + 1],
                                        payload[offset + 2],
                                        payload[offset + 3],
                                    );
                                    let port = u16::from_le_bytes([
                                        payload[offset + 4],
                                        payload[offset + 5],
                                    ]);

                                    let peer_addr =
                                        std::net::SocketAddr::new(std::net::IpAddr::V4(ip), port);

                                    tracing::info!(peer = %peer_addr, "Discovered peer from server");

                                    // Connect to peer
                                    if let Err(e) = self.add_peer(peer_addr).await {
                                        tracing::warn!(peer = %peer_addr, error = %e, "Failed to connect to peer");
                                    }

                                    offset += 6;
                                }
                            }
                        }
                        0x34 => {
                            // ServerStatus - server stats
                            if payload.len() >= 8 {
                                let user_count = u32::from_le_bytes([
                                    payload[0], payload[1], payload[2], payload[3],
                                ]);
                                let file_count = u32::from_le_bytes([
                                    payload[4], payload[5], payload[6], payload[7],
                                ]);
                                tracing::debug!(
                                    users = user_count,
                                    files = file_count,
                                    "Server status"
                                );
                            }
                        }
                        _ => {
                            tracing::debug!(
                                protocol = format!("0x{:02x}", protocol),
                                "Server message"
                            );
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Server connection error");
                    break;
                }
                Err(_) => {
                    // Timeout - no more responses
                    break;
                }
            }
        }

        Ok(())
    }

    /// Add a peer to the download
    pub async fn add_peer(&mut self, addr: SocketAddr) -> Result<(), Ed2kDownloadError> {
        if self.peers.contains_key(&addr) {
            return Ok(());
        }

        let client = Ed2kClient::connect(addr)
            .await
            .map_err(|e| Ed2kDownloadError::Peer(e.to_string()))?;

        self.peers.insert(addr, client);
        tracing::info!(addr = %addr, "Connected to ed2k peer");

        Ok(())
    }

    async fn request_blocks(&mut self) {
        // Find chunks we need
        let needed_chunks: Vec<u32> = self
            .chunks
            .iter()
            .filter(|c| !c.complete && !self.downloaded_chunks.contains(&c.index))
            .map(|c| c.index)
            .collect();

        if needed_chunks.is_empty() {
            return;
        }

        // Request blocks from peers - each peer gets different blocks
        let mut peer_iter = self.peers.iter_mut();
        for &chunk_idx in &needed_chunks {
            let chunk = &self.chunks[chunk_idx as usize];

            // Find next block needed in this chunk
            if let Some(offset) = chunk.next_block_needed() {
                let size = std::cmp::min(ED2K_BLOCK_SIZE, chunk.size - offset);

                // Request from next available peer
                if let Some((addr, peer)) = peer_iter.next() {
                    if let Err(e) = peer.request_block(&self.file_hash, offset, size).await {
                        tracing::warn!(addr = %addr, error = %e, "Failed to request block");
                    }
                } else {
                    break; // No more peers
                }
            }
        }
    }

    async fn handle_peer_messages(&mut self) {
        let mut disconnected = Vec::new();

        for (addr, peer) in &mut self.peers {
            // Try to receive data (non-blocking)
            match tokio::time::timeout(Duration::from_millis(100), peer.receive_block()).await {
                Ok(Ok((_hash, offset, data))) => {
                    let block_size = data.len() as u64;
                    // Determine which chunk this block belongs to
                    let chunk_idx = (offset / ED2K_CHUNK_SIZE) as u32;
                    let chunk_offset_in_chunk = offset % ED2K_CHUNK_SIZE;

                    if (chunk_idx as usize) < self.chunks.len() {
                        let chunk = &mut self.chunks[chunk_idx as usize];
                        chunk.add_block(chunk_offset_in_chunk, data);

                        if chunk.complete {
                            if let Some(assembled) = chunk.assemble() {
                                if chunk.verify(&assembled) {
                                    tracing::info!(chunk = chunk_idx, "Chunk verified");
                                    self.downloaded_chunks.insert(chunk_idx);
                                } else {
                                    tracing::warn!(chunk = chunk_idx, "Chunk verification failed");
                                    // Reset chunk state
                                    let hash = chunk.hash;
                                    let size = chunk.size;
                                    *chunk = ChunkState::new(chunk_idx, size, hash);
                                }
                            }
                        }
                    }
                    self.downloaded += block_size;
                    tracing::debug!(addr = %addr, offset = offset, size = block_size, "Received block");
                }
                Ok(Err(e)) => {
                    tracing::warn!(addr = %addr, error = %e, "Peer error");
                    disconnected.push(*addr);
                }
                Err(_) => {} // Timeout, no data
            }
        }

        // Remove disconnected peers
        for addr in disconnected {
            self.peers.remove(&addr);
        }
    }

    fn is_complete(&self) -> bool {
        self.downloaded_chunks.len() == self.chunks.len()
    }

    async fn save_file(&self) -> Result<(), Ed2kDownloadError> {
        let mut file_data = Vec::with_capacity(self.file_size as usize);

        // Assemble all chunks in order
        for chunk in &self.chunks {
            if let Some(data) = chunk.assemble() {
                file_data.extend_from_slice(&data);
            } else {
                return Err(Ed2kDownloadError::Io("incomplete chunk".to_string()));
            }
        }

        // Write to file
        let output_path = self.download_dir.join(&self.file_name);
        tokio::fs::create_dir_all(&self.download_dir)
            .await
            .map_err(|e| Ed2kDownloadError::Io(e.to_string()))?;

        tokio::fs::write(&output_path, &file_data)
            .await
            .map_err(|e| Ed2kDownloadError::Io(e.to_string()))?;

        tracing::info!(path = %output_path.display(), "File saved");

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Ed2kDownloadError {
    #[error("server error: {0}")]
    Server(String),
    #[error("peer error: {0}")]
    Peer(String),
    #[error("IO error: {0}")]
    Io(String),
}
