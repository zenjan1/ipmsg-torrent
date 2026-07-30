//! eDonkey download engine

use super::client::Ed2kClient;
use super::protocol::{ED2K_BLOCK_SIZE, ED2K_CHUNK_SIZE, Ed2kFileHash, Ed2kPeer};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
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
        // Note: Real ed2k uses MD4, but we're using SHA-256 for simplicity
        // TODO: Implement MD4 or use md-5 crate
        let hash = Sha256::digest(data);
        &hash[..16] == self.hash
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
            // TODO: Get chunk hash from file metadata
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

        // TODO: Parse server response to get peer list
        // For now, we'll just keep the connection open

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

        // Request blocks from peers
        for (addr, peer) in &mut self.peers {
            for &chunk_idx in &needed_chunks {
                let chunk = &self.chunks[chunk_idx as usize];
                for block_idx in 0..chunk.blocks_total {
                    let offset = block_idx * ED2K_BLOCK_SIZE;
                    let size = std::cmp::min(ED2K_BLOCK_SIZE, chunk.size - offset);

                    if let Err(e) = peer.request_block(&self.file_hash, offset, size).await {
                        tracing::warn!(addr = %addr, error = %e, "Failed to request block");
                        break;
                    }
                }
                break;
            }
        }
    }

    async fn handle_peer_messages(&mut self) {
        let mut disconnected = Vec::new();

        for (addr, peer) in &mut self.peers {
            // Try to receive data (non-blocking)
            match tokio::time::timeout(Duration::from_millis(100), peer.receive_block()).await {
                Ok(Ok(data)) => {
                    // TODO: Parse which chunk/block this data belongs to
                    // For now, just track downloaded bytes
                    self.downloaded += data.len() as u64;
                    tracing::debug!(addr = %addr, size = data.len(), "Received block");
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
