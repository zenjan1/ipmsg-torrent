//! eDonkey download engine

use super::client::Ed2kClient;
use super::peer_cache;
use super::protocol::{ED2K_BLOCK_SIZE, ED2K_CHUNK_SIZE, Ed2kFileHash};
use super::server_cache;
use crate::progress::{self, ProgressSnapshot};
use crate::proxy::ProxyConfig;
use crate::rate_limiter::RateLimiter;
use md4::{Digest, Md4};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;

/// Maximum server connection retries
const MAX_SERVER_RETRIES: u32 = 2;
/// Base delay for server retry backoff
const SERVER_RETRY_BASE_MS: u64 = 3000;
/// Interval between peer cache saves (seconds)
const PEER_CACHE_SAVE_INTERVAL_SECS: u64 = 60;
/// Maximum peers to exchange with each peer per round
const MAX_PEER_EXCHANGE_PER_PEER: usize = 5;

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
        hash[..] == self.hash
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
    /// Progress snapshot for resume support
    progress: ProgressSnapshot,
    /// Cached peers loaded from disk
    cached_peers: Vec<SocketAddr>,
    /// Cached servers loaded from disk
    cached_servers: Vec<SocketAddr>,
    /// Optional rate limiter for speed control
    rate_limiter: Option<RateLimiter>,
    /// Optional proxy configuration for server/peer connections
    proxy_config: Option<ProxyConfig>,
    /// Known peer addresses discovered through peer exchange
    discovered_peers: HashSet<SocketAddr>,
    /// Last time peer cache was saved to disk
    last_peer_cache_save: Option<tokio::time::Instant>,
    /// Server connection failure counts
    server_failures: HashMap<SocketAddr, u32>,
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

        // Build progress hash: pad MD4 (16 bytes) to 20 bytes with zeros
        let mut progress_hash = [0u8; 20];
        progress_hash[..16].copy_from_slice(&file_hash.0);

        // Build initial progress snapshot
        let mut progress =
            ProgressSnapshot::new(progress_hash, file_size, ED2K_CHUNK_SIZE, chunk_count);

        // Try to load existing progress for resume
        if let Ok(saved) =
            progress::load_progress(&download_dir, &file_name, &progress_hash, file_size)
        {
            tracing::info!(
                completed = saved.completed_pieces.len(),
                total = saved.total_pieces,
                "Resuming ed2k download from saved progress"
            );
            progress = saved;
        }

        // Restore downloaded_chunks set and compute downloaded bytes
        let downloaded_chunks: HashSet<u32> = progress.completed_pieces.iter().copied().collect();
        let downloaded: u64 = chunks
            .iter()
            .filter(|c| downloaded_chunks.contains(&c.index))
            .map(|c| c.size)
            .sum();

        // Mark chunk states as already complete for resumed chunks
        for &idx in &downloaded_chunks {
            if let Some(chunk) = chunks.get_mut(idx as usize) {
                // Fill with dummy data so assemble() works
                for b in 0..chunk.blocks_total {
                    let offset = b * ED2K_BLOCK_SIZE;
                    let block_len = std::cmp::min(ED2K_BLOCK_SIZE, chunk.size - offset) as usize;
                    chunk.blocks_received.insert(offset, vec![0u8; block_len]);
                }
                chunk.complete = true;
            }
        }

        // Load cached peers from disk
        let cached_peers = peer_cache::load_peers(&download_dir, &file_hash.0).unwrap_or_default();
        if !cached_peers.is_empty() {
            tracing::info!(count = cached_peers.len(), "Loaded cached ed2k peers");
        }

        // Load cached servers from disk
        let cached_servers = server_cache::load_servers(&download_dir).unwrap_or_default();
        if !cached_servers.is_empty() {
            tracing::info!(count = cached_servers.len(), "Loaded cached ed2k servers");
        }

        Self {
            file_hash,
            file_size,
            file_name,
            download_dir,
            chunks,
            servers,
            peers: HashMap::new(),
            downloaded_chunks,
            downloaded,
            progress,
            cached_peers,
            cached_servers,
            rate_limiter: None,
            proxy_config: None,
            discovered_peers: HashSet::new(),
            last_peer_cache_save: None,
            server_failures: HashMap::new(),
        }
    }

    /// Number of downloaded (completed) chunks
    pub fn downloaded_chunks_count(&self) -> usize {
        self.downloaded_chunks.len()
    }

    /// Total bytes downloaded so far
    pub fn downloaded_bytes(&self) -> u64 {
        self.downloaded
    }

    /// Whether all chunks have been downloaded
    pub fn is_download_complete(&self) -> bool {
        self.is_complete()
    }

    /// Set rate limiter for speed control
    pub fn set_rate_limiter(&mut self, limiter: RateLimiter) {
        self.rate_limiter = Some(limiter);
    }

    /// Set proxy configuration for server and peer connections
    pub fn set_proxy_config(&mut self, config: Option<ProxyConfig>) {
        self.proxy_config = config;
    }

    /// Update chunk hash from peer's HashSet message
    pub fn update_chunk_hash(&mut self, chunk_idx: u32, hash: [u8; 16]) {
        if let Some(chunk) = self.chunks.get_mut(chunk_idx as usize) {
            chunk.hash = hash;
        }
    }

    /// Start the download process
    pub async fn download(
        &mut self,
        cancel: Option<CancellationToken>,
    ) -> Result<(), Ed2kDownloadError> {
        tracing::info!(
            name = %self.file_name,
            size = self.file_size,
            chunks = self.chunks.len(),
            "Starting ed2k download"
        );

        // Try cached peers first
        let cached = std::mem::take(&mut self.cached_peers);
        for peer_addr in cached {
            if let Err(e) = self.add_peer(peer_addr).await {
                tracing::debug!(addr = %peer_addr, error = %e, "Cached peer unavailable");
            }
        }

        // Combine configured servers with cached servers
        let mut all_servers = self.servers.clone();
        let cached_servers = std::mem::take(&mut self.cached_servers);
        for server in cached_servers {
            if !all_servers.contains(&server) {
                all_servers.push(server);
            }
        }

        // Connect to servers and request sources (with retry)
        for server_addr in all_servers {
            if let Err(e) = self.connect_server_with_retry(server_addr).await {
                tracing::warn!(addr = %server_addr, error = %e, "Failed to connect to server");
                *self.server_failures.entry(server_addr).or_insert(0) += 1;
            }
        }

        // Save server list to cache
        if let Err(e) = server_cache::save_servers(&self.download_dir, &self.servers) {
            tracing::warn!(error = %e, "Failed to save server cache");
        }

        // Main download loop
        let mut tick = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    // Check if cancelled
                    if let Some(ref cancel) = cancel
                        && cancel.is_cancelled() {
                            tracing::info!("Download cancelled");
                            return Err(Ed2kDownloadError::Io("cancelled".to_string()));
                        }

                    // Check if download is complete
                    if self.is_complete() {
                        tracing::info!("Download complete!");
                        break;
                    }

                    // Request blocks from peers
                    self.request_blocks().await;

                    // Handle peer messages (includes HashSet for chunk verification)
                    self.handle_peer_messages().await;

                    // Periodic peer exchange and cache save
                    self.maybe_save_peer_cache();

                    // Request peer sources from connected peers (P2P source exchange)
                    self.request_peer_sources().await;
                }
            }
        }

        // Save file
        self.save_file().await?;

        // Final peer cache save
        self.save_peer_cache();

        Ok(())
    }

    /// Connect to a server with retry logic
    async fn connect_server_with_retry(
        &mut self,
        addr: SocketAddr,
    ) -> Result<(), Ed2kDownloadError> {
        let failures = self.server_failures.get(&addr).copied().unwrap_or(0);
        let max_retries = MAX_SERVER_RETRIES.saturating_sub(failures);

        for attempt in 0..=max_retries {
            match self.connect_server(addr).await {
                Ok(()) => {
                    // Reset failure count on success
                    self.server_failures.remove(&addr);
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        addr = %addr,
                        attempt = attempt + 1,
                        max = max_retries + 1,
                        error = %e,
                        "Server connection attempt failed"
                    );
                    if attempt < max_retries {
                        let delay = SERVER_RETRY_BASE_MS * 2u64.pow(attempt);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                    if attempt == max_retries {
                        return Err(e);
                    }
                }
            }
        }
        Err(Ed2kDownloadError::Server("max retries exceeded".to_string()))
    }

    /// Request peer sources from connected peers (P2P source exchange).
    /// Each peer may share additional peer addresses that have the file.
    async fn request_peer_sources(&mut self) {
        let peer_addrs: Vec<SocketAddr> = self.peers.keys().copied().collect();
        let file_hash = self.file_hash.clone();

        for addr in peer_addrs {
            if let Some(peer) = self.peers.get_mut(&addr) {
                // Send GetSources request to peer (opcode 0x19)
                if let Err(e) = peer.request_sources(&file_hash).await {
                    tracing::debug!(addr = %addr, error = %e, "Failed to request sources from peer");
                }
            }
        }
    }

    /// Periodically save peer cache to disk
    fn maybe_save_peer_cache(&mut self) {
        let should_save = match self.last_peer_cache_save {
            None => true,
            Some(last) => last.elapsed() > Duration::from_secs(PEER_CACHE_SAVE_INTERVAL_SECS),
        };

        if should_save {
            self.save_peer_cache();
            self.last_peer_cache_save = Some(tokio::time::Instant::now());
        }
    }

    async fn connect_server(&mut self, addr: SocketAddr) -> Result<(), Ed2kDownloadError> {
        let mut client = if let Some(ref proxy) = self.proxy_config {
            Ed2kClient::connect_with_proxy(addr, proxy)
                .await
                .map_err(|e| Ed2kDownloadError::Server(e.to_string()))?
        } else {
            Ed2kClient::connect(addr)
                .await
                .map_err(|e| Ed2kDownloadError::Server(e.to_string()))?
        };

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
                            if !payload.is_empty() {
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

                                    // Save to peer cache
                                    self.save_peer_cache();

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

        let client = if let Some(ref proxy) = self.proxy_config {
            Ed2kClient::connect_with_proxy(addr, proxy)
                .await
                .map_err(|e| Ed2kDownloadError::Peer(e.to_string()))?
        } else {
            Ed2kClient::connect(addr)
                .await
                .map_err(|e| Ed2kDownloadError::Peer(e.to_string()))?
        };

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
        let mut new_peers: Vec<SocketAddr> = Vec::new();

        // Collect peer addresses first to avoid borrow issues
        let peer_addrs: Vec<SocketAddr> = self.peers.keys().copied().collect();

        for addr in peer_addrs {
            if let Some(peer) = self.peers.get_mut(&addr) {
                // Try to receive data (non-blocking)
                match tokio::time::timeout(Duration::from_millis(100), peer.receive_block()).await {
                    Ok(Ok((hash, offset, data))) => {
                        let block_size = data.len() as u64;

                        // Apply rate limiting before processing
                        if let Some(ref limiter) = self.rate_limiter {
                            limiter.acquire(block_size).await;
                        }

                        // Verify the block hash matches the file hash
                        if hash != self.file_hash.0 {
                            tracing::warn!(
                                addr = %addr,
                                "Block received with mismatched file hash, discarding"
                            );
                            continue;
                        }

                        // Determine which chunk this block belongs to
                        let chunk_idx = (offset / ED2K_CHUNK_SIZE) as u32;
                        let chunk_offset_in_chunk = offset % ED2K_CHUNK_SIZE;

                        if (chunk_idx as usize) < self.chunks.len() {
                            let chunk = &mut self.chunks[chunk_idx as usize];
                            chunk.add_block(chunk_offset_in_chunk, data);

                            if chunk.complete
                                && let Some(assembled) = chunk.assemble()
                            {
                                if chunk.verify(&assembled) {
                                    tracing::info!(chunk = chunk_idx, "Chunk verified (MD4)");
                                    self.downloaded_chunks.insert(chunk_idx);
                                    self.progress.mark_complete(chunk_idx);
                                    self.progress.downloaded = self.downloaded;
                                    // Persist progress after each chunk
                                    if let Err(e) = progress::save_progress(
                                        &self.download_dir,
                                        &self.file_name,
                                        &self.progress,
                                    ) {
                                        tracing::warn!(error = %e, "Failed to save ed2k progress");
                                    }
                                } else {
                                    tracing::warn!(chunk = chunk_idx, "Chunk verification failed, resetting");
                                    // Reset chunk state
                                    let hash = chunk.hash;
                                    let size = chunk.size;
                                    *chunk = ChunkState::new(chunk_idx, size, hash);
                                }
                            }
                        }
                        self.downloaded += block_size;
                        tracing::debug!(addr = %addr, offset = offset, size = block_size, "Received block");
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(addr = %addr, error = %e, "Peer error");
                        disconnected.push(addr);
                    }
                    Err(_) => {} // Timeout, no data
                }

                // Try to receive HashSet messages (chunk hashes for verification)
                // and peer source exchange messages
                if let Some(peer) = self.peers.get_mut(&addr) {
                    match tokio::time::timeout(Duration::from_millis(50), peer.recv()).await {
                        Ok(Ok((protocol, payload))) => {
                            match protocol {
                                0x51 => {
                                    // HashSet: chunk hashes for file verification
                                    self.handle_hash_set(addr, &payload);
                                }
                                0x42 => {
                                    // Peer source exchange: other peers that have this file
                                    self.handle_peer_sources(&payload, &mut new_peers);
                                }
                                _ => {
                                    tracing::trace!(
                                        addr = %addr,
                                        protocol = format!("0x{:02x}", protocol),
                                        "Unhandled peer message"
                                    );
                                }
                            }
                        }
                        Ok(Err(_)) => {
                            // Peer disconnected or error
                        }
                        Err(_) => {
                            // Timeout, no control messages
                        }
                    }
                }
            }
        }

        // Process newly discovered peers
        for peer_addr in new_peers {
            if !self.peers.contains_key(&peer_addr)
                && !self.discovered_peers.contains(&peer_addr)
            {
                self.discovered_peers.insert(peer_addr);
                if let Err(e) = self.add_peer(peer_addr).await {
                    tracing::debug!(addr = %peer_addr, error = %e, "Discovered peer unavailable");
                } else {
                    tracing::info!(addr = %peer_addr, "Connected to discovered peer");
                }
            }
        }

        // Remove disconnected peers
        for addr in disconnected {
            self.peers.remove(&addr);
        }
    }

    /// Handle HashSet message from peer (chunk hashes for verification).
    /// Format: [16 bytes file hash][2 bytes chunk count][count * 16 bytes MD4 hashes]
    fn handle_hash_set(&mut self, addr: SocketAddr, payload: &[u8]) {
        if payload.len() < 18 {
            tracing::debug!(addr = %addr, "HashSet too short");
            return;
        }

        // Verify file hash matches
        let received_hash = &payload[..16];
        if received_hash != self.file_hash.0 {
            tracing::warn!(addr = %addr, "HashSet for wrong file");
            return;
        }

        let chunk_count = u16::from_le_bytes([payload[16], payload[17]]) as usize;
        let mut offset = 18;

        for chunk_idx in 0..chunk_count {
            if offset + 16 > payload.len() {
                break;
            }
            let mut chunk_hash = [0u8; 16];
            chunk_hash.copy_from_slice(&payload[offset..offset + 16]);
            self.update_chunk_hash(chunk_idx as u32, chunk_hash);
            offset += 16;
        }

        tracing::debug!(
            addr = %addr,
            chunks = chunk_count,
            "Received chunk hashes from peer"
        );
    }

    /// Handle peer source exchange message.
    /// Format: [1 byte count][count * (4 bytes IP + 2 bytes port)]
    fn handle_peer_sources(&self, payload: &[u8], new_peers: &mut Vec<SocketAddr>) {
        if payload.is_empty() {
            return;
        }

        let peer_count = payload[0] as usize;
        let mut offset = 1;

        for _ in 0..peer_count.min(MAX_PEER_EXCHANGE_PER_PEER) {
            if offset + 6 > payload.len() {
                break;
            }

            let ip = std::net::Ipv4Addr::new(
                payload[offset],
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
            );
            let port = u16::from_le_bytes([payload[offset + 4], payload[offset + 5]]);
            let peer_addr = SocketAddr::new(std::net::IpAddr::V4(ip), port);

            // Skip private/loopback addresses from peer exchange
            if !ip.is_loopback() && !ip.is_private() {
                new_peers.push(peer_addr);
            }

            offset += 6;
        }
    }

    /// Save current peer list to cache
    fn save_peer_cache(&self) {
        let peers: Vec<SocketAddr> = self.peers.keys().copied().collect();
        if let Err(e) = peer_cache::save_peers(&self.download_dir, &self.file_hash.0, &peers) {
            tracing::warn!(error = %e, "Failed to save peer cache");
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

        // Remove progress file and peer cache after successful completion
        let progress_path = progress::progress_path(&self.download_dir, &self.file_name);
        if progress_path.exists()
            && let Err(e) = std::fs::remove_file(&progress_path)
        {
            tracing::warn!(error = %e, "Failed to remove progress file");
        }
        if let Err(e) = peer_cache::remove_peer_cache(&self.download_dir, &self.file_hash.0) {
            tracing::warn!(error = %e, "Failed to remove peer cache");
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{ProxyConfig, ProxyType};

    #[test]
    fn test_set_proxy_config_socks5() {
        let hash = Ed2kFileHash([0u8; 16]);
        let mut engine = Ed2kEngine::new(
            hash,
            1024,
            "test.txt".to_string(),
            std::env::temp_dir(),
            vec![],
        );

        assert!(engine.proxy_config.is_none());

        let proxy = ProxyConfig::new(ProxyType::Socks5, "127.0.0.1".into(), 1080);
        engine.set_proxy_config(Some(proxy.clone()));

        assert!(engine.proxy_config.is_some());
        let saved = engine.proxy_config.unwrap();
        assert_eq!(saved.proxy_type, ProxyType::Socks5);
        assert_eq!(saved.host, "127.0.0.1");
        assert_eq!(saved.port, 1080);
    }

    #[test]
    fn test_set_proxy_config_http() {
        let hash = Ed2kFileHash([0u8; 16]);
        let mut engine = Ed2kEngine::new(
            hash,
            1024,
            "test.txt".to_string(),
            std::env::temp_dir(),
            vec![],
        );

        let proxy = ProxyConfig::new(ProxyType::Http, "proxy.example.com".into(), 8080);
        engine.set_proxy_config(Some(proxy));

        assert!(engine.proxy_config.is_some());
    }

    #[test]
    fn test_set_proxy_config_none() {
        let hash = Ed2kFileHash([0u8; 16]);
        let mut engine = Ed2kEngine::new(
            hash,
            1024,
            "test.txt".to_string(),
            std::env::temp_dir(),
            vec![],
        );

        // Set then clear
        let proxy = ProxyConfig::new(ProxyType::Socks5, "127.0.0.1".into(), 1080);
        engine.set_proxy_config(Some(proxy));
        assert!(engine.proxy_config.is_some());

        engine.set_proxy_config(None);
        assert!(engine.proxy_config.is_none());
    }
}
