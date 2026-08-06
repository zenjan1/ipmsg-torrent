//! Torrent download engine - coordinates tracker, peers, and piece management

use super::meta::TorrentMeta;
use super::peer::{PeerConnection, PeerMessage};
use super::tracker::{AnnounceEvent, HttpTracker};
use crate::progress::{self, ProgressSnapshot};
use crate::rate_limiter::RateLimiter;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;

/// Block size for requests (16KB standard)
const BLOCK_SIZE: u32 = 16 * 1024;

/// Download state for a single piece
#[derive(Debug, Clone)]
struct PieceState {
    index: u32,
    length: u64,
    hash: [u8; 20],
    blocks_received: HashMap<u32, Vec<u8>>,
    blocks_total: u32,
    complete: bool,
}

impl PieceState {
    fn new(index: u32, length: u64, hash: [u8; 20]) -> Self {
        let blocks_total = ((length as f64) / (BLOCK_SIZE as f64)).ceil() as u32;
        Self {
            index,
            length,
            hash,
            blocks_received: HashMap::new(),
            blocks_total,
            complete: false,
        }
    }

    fn add_block(&mut self, offset: u32, data: Vec<u8>) {
        self.blocks_received.insert(offset, data);
        if self.blocks_received.len() == self.blocks_total as usize {
            self.complete = true;
        }
    }

    fn assemble(&self) -> Option<Vec<u8>> {
        if !self.complete {
            return None;
        }
        let mut data = Vec::with_capacity(self.length as usize);
        for i in 0..self.blocks_total {
            let offset = i * BLOCK_SIZE;
            let block = self.blocks_received.get(&offset)?;
            data.extend_from_slice(block);
        }
        Some(data)
    }

    fn verify(&self, data: &[u8]) -> bool {
        let hash = Sha1::digest(data);
        hash[..] == self.hash
    }
}

/// Peer state tracking
#[allow(dead_code)]
struct PeerState {
    connection: PeerConnection,
    available_pieces: HashSet<u32>,
    requests_sent: HashMap<(u32, u32), tokio::time::Instant>, // (piece, block) -> time

    // Peer scoring
    score: f64,
    downloaded_bytes: u64,
    uploaded_bytes: u64,
    response_times: Vec<Duration>,
    last_activity: tokio::time::Instant,

    // Choking state
    am_choking: bool,
    peer_choking: bool,
    am_interested: bool,
    peer_interested: bool,
}

impl PeerState {
    fn new(connection: PeerConnection) -> Self {
        Self {
            connection,
            available_pieces: HashSet::new(),
            requests_sent: HashMap::new(),
            score: 1.0,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            response_times: Vec::new(),
            last_activity: tokio::time::Instant::now(),
            am_choking: true,
            peer_choking: true,
            am_interested: false,
            peer_interested: false,
        }
    }

    /// Update peer score based on performance
    fn update_score(&mut self) {
        // Calculate average response time
        let avg_response = if self.response_times.is_empty() {
            Duration::from_secs(1)
        } else {
            let total: Duration = self.response_times.iter().sum();
            total / self.response_times.len() as u32
        };

        // Score based on:
        // 1. Download speed (bytes per second)
        // 2. Response time (faster is better)
        // 3. Reliability (fewer timeouts)

        let elapsed = self.last_activity.elapsed().as_secs_f64();
        let download_speed = if elapsed > 0.0 {
            self.downloaded_bytes as f64 / elapsed
        } else {
            0.0
        };

        // Response time factor (1.0 for <100ms, 0.1 for >10s)
        let response_factor = (1.0 / (avg_response.as_secs_f64() + 0.1)).min(1.0);

        // Speed factor (logarithmic scale)
        let speed_factor = (download_speed / 1024.0 + 1.0).ln();

        self.score = speed_factor * response_factor;

        // Keep only last 10 response times
        if self.response_times.len() > 10 {
            self.response_times.remove(0);
        }
    }

    /// Record a response time
    fn record_response(&mut self, duration: Duration) {
        self.response_times.push(duration);
        self.last_activity = tokio::time::Instant::now();
    }
}

/// Torrent download engine
pub struct TorrentEngine {
    meta: Arc<TorrentMeta>,
    peer_id: [u8; 20],
    download_dir: PathBuf,
    pieces: Vec<PieceState>,
    peers: HashMap<SocketAddr, PeerState>,
    downloaded_pieces: HashSet<u32>,
    tracker: HttpTracker,
    downloaded: u64,
    uploaded: u64,
    /// Progress snapshot for resume support
    progress: ProgressSnapshot,
    /// Optional rate limiter for speed control
    rate_limiter: Option<RateLimiter>,
}

impl TorrentEngine {
    pub fn new(meta: TorrentMeta, download_dir: PathBuf) -> Self {
        let peer_id = Self::generate_peer_id();
        let total_size = meta.total_size();
        let piece_length = meta.info.piece_length;

        // Initialize piece states
        let mut pieces = Vec::new();
        for (i, hash) in meta.info.pieces.iter().enumerate() {
            let piece_start = i as u64 * piece_length;
            let piece_length = std::cmp::min(piece_length, total_size - piece_start);
            pieces.push(PieceState::new(i as u32, piece_length, *hash));
        }

        let tracker = HttpTracker::new(peer_id, 6881);

        // Build initial progress snapshot
        let mut progress = ProgressSnapshot::new(
            meta.info_hash,
            total_size,
            piece_length,
            pieces.len() as u32,
        );

        // Try to load existing progress
        if let Ok(saved) =
            progress::load_progress(&download_dir, &meta.info.name, &meta.info_hash, total_size)
        {
            tracing::info!(
                completed = saved.completed_pieces.len(),
                total = saved.total_pieces,
                "Resuming torrent download from saved progress"
            );
            progress = saved;
        }

        // Restore downloaded_pieces set and compute downloaded bytes
        let downloaded_pieces: HashSet<u32> = progress.completed_pieces.iter().copied().collect();
        let downloaded: u64 = pieces
            .iter()
            .filter(|p| downloaded_pieces.contains(&p.index))
            .map(|p| p.length)
            .sum();

        // Mark piece states as already complete
        for &idx in &downloaded_pieces {
            if let Some(piece) = pieces.get_mut(idx as usize) {
                // Fill with dummy data so assemble() works
                for b in 0..piece.blocks_total {
                    let offset = b * BLOCK_SIZE;
                    // Last block may be shorter than BLOCK_SIZE
                    let block_len =
                        std::cmp::min(BLOCK_SIZE as u64, piece.length - offset as u64) as usize;
                    piece.blocks_received.insert(offset, vec![0u8; block_len]);
                }
                piece.complete = true;
            }
        }

        Self {
            meta: Arc::new(meta),
            peer_id,
            download_dir,
            pieces,
            peers: HashMap::new(),
            downloaded_pieces,
            tracker,
            downloaded,
            uploaded: 0,
            progress,
            rate_limiter: None,
        }
    }

    /// Set rate limiter for speed control
    pub fn set_rate_limiter(&mut self, limiter: RateLimiter) {
        self.rate_limiter = Some(limiter);
    }

    fn generate_peer_id() -> [u8; 20] {
        let mut id = [0u8; 20];
        // Use "-IP0001-" prefix (IPMsg-Torrent version 0001)
        id[0..8].copy_from_slice(b"-IP0001-");
        // Fill rest with random bytes
        for byte in id.iter_mut().skip(8) {
            *byte = rand::random();
        }
        id
    }

    /// Start the download process
    pub async fn download(
        &mut self,
        cancel: Option<CancellationToken>,
    ) -> Result<(), DownloadError> {
        tracing::info!(
            name = %self.meta.info.name,
            pieces = self.pieces.len(),
            size = self.meta.total_size(),
            "Starting torrent download"
        );

        // Announce to tracker
        let response = self
            .tracker
            .announce(&self.meta, AnnounceEvent::Started)
            .await
            .map_err(|e| DownloadError::Tracker(e.to_string()))?;

        tracing::info!(
            peers = response.peers.len(),
            interval = response.interval,
            "Tracker response received"
        );

        // Connect to peers
        for peer in response.peers {
            let addr = SocketAddr::new(peer.ip, peer.port);
            if let Err(e) = self.connect_peer(addr).await {
                tracing::warn!(addr = %addr, error = %e, "Failed to connect to peer");
            }
        }

        // Main download loop
        let mut tick = interval(Duration::from_secs(1));
        let mut last_announce = tokio::time::Instant::now();
        let announce_interval = Duration::from_secs(response.interval);

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    // Check if cancelled
                    if let Some(ref cancel) = cancel
                        && cancel.is_cancelled() {
                            tracing::info!("Download cancelled");
                            return Err(DownloadError::Io("cancelled".to_string()));
                        }

                    // Check if download is complete
                    if self.is_complete() {
                        tracing::info!("Download complete!");
                        break;
                    }

                    // Re-announce if needed
                    if last_announce.elapsed() > announce_interval
                        && let Ok(resp) = self.tracker.announce(&self.meta, AnnounceEvent::None).await {
                            for peer in resp.peers {
                                let addr = SocketAddr::new(peer.ip, peer.port);
                                let _ = self.connect_peer(addr).await;
                            }
                            last_announce = tokio::time::Instant::now();
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

        // Final announce
        let _ = self
            .tracker
            .announce(&self.meta, AnnounceEvent::Completed)
            .await;

        Ok(())
    }

    async fn connect_peer(&mut self, addr: SocketAddr) -> Result<(), DownloadError> {
        if self.peers.contains_key(&addr) {
            return Ok(());
        }

        let conn = PeerConnection::connect(addr, self.meta.info_hash, self.peer_id)
            .await
            .map_err(|e| DownloadError::Peer(e.to_string()))?;

        // Send bitfield (we have nothing initially)
        let mut conn = conn;
        let _ = conn.send(PeerMessage::Bitfield(vec![])).await;

        // Send interested
        let _ = conn.send(PeerMessage::Interested).await;

        let peer_state = PeerState::new(conn);

        self.peers.insert(addr, peer_state);
        tracing::info!(addr = %addr, "Connected to peer");

        Ok(())
    }

    async fn request_blocks(&mut self) {
        // Find pieces we need
        let needed_pieces: Vec<u32> = self
            .pieces
            .iter()
            .filter(|p| !p.complete && !self.downloaded_pieces.contains(&p.index))
            .map(|p| p.index)
            .collect();

        if needed_pieces.is_empty() {
            return;
        }

        // Check if we're in endgame mode (less than 5% pieces remaining)
        let endgame_mode = needed_pieces.len() < (self.pieces.len() / 20).max(1);

        // Calculate piece rarity (how many peers have each piece)
        let mut piece_counts: HashMap<u32, usize> = HashMap::new();
        for &piece_idx in &needed_pieces {
            let count = self
                .peers
                .values()
                .filter(|p| p.available_pieces.contains(&piece_idx))
                .count();
            piece_counts.insert(piece_idx, count);
        }

        // Sort by rarity (fewest peers first)
        let mut rarest_pieces: Vec<_> = piece_counts.into_iter().collect();
        rarest_pieces.sort_by_key(|(_, count)| *count);

        // Sort peers by score (highest first)
        let mut peer_scores: Vec<_> = self
            .peers
            .iter()
            .map(|(addr, state)| (*addr, state.score))
            .collect();
        peer_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Request blocks from best peers first
        for (addr, _score) in &peer_scores {
            let peer_state = match self.peers.get_mut(addr) {
                Some(state) => state,
                None => continue,
            };

            if peer_state.connection.is_choking() {
                continue;
            }

            // Limit in-flight requests per peer
            const MAX_IN_FLIGHT: usize = 10;
            if peer_state.requests_sent.len() >= MAX_IN_FLIGHT {
                continue;
            }

            // Find a piece this peer has that we need (rarest first)
            for (piece_idx, _) in &rarest_pieces {
                if !peer_state.available_pieces.contains(piece_idx) {
                    continue;
                }

                let piece = &self.pieces[*piece_idx as usize];

                // In endgame mode, request from multiple peers
                // In normal mode, only request if not already requested
                let mut requested = false;
                for block_idx in 0..piece.blocks_total {
                    let offset = block_idx * BLOCK_SIZE;

                    // Skip if already requested (unless in endgame mode)
                    if !endgame_mode && peer_state.requests_sent.contains_key(&(*piece_idx, offset))
                    {
                        continue;
                    }

                    let length = std::cmp::min(BLOCK_SIZE, (piece.length - offset as u64) as u32);
                    let request = PeerMessage::Request {
                        index: *piece_idx,
                        begin: offset,
                        length,
                    };

                    if let Err(e) = peer_state.connection.send(request).await {
                        tracing::warn!(addr = %addr, error = %e, "Failed to send request");
                        break;
                    }

                    peer_state
                        .requests_sent
                        .insert((*piece_idx, offset), tokio::time::Instant::now());
                    requested = true;

                    // Stop if we hit the limit
                    if peer_state.requests_sent.len() >= MAX_IN_FLIGHT {
                        break;
                    }
                }

                // Only request from one piece per peer per iteration (unless endgame)
                if requested && !endgame_mode {
                    break;
                }
            }
        }
    }

    async fn handle_peer_messages(&mut self) {
        let mut disconnected = Vec::new();
        let mut received_msgs: Vec<(SocketAddr, PeerMessage)> = Vec::new();

        for (addr, peer_state) in &mut self.peers {
            // Try to receive messages (non-blocking)
            loop {
                match tokio::time::timeout(Duration::from_millis(100), peer_state.connection.recv())
                    .await
                {
                    Ok(Ok(msg)) => {
                        peer_state.connection.update_state(&msg);
                        received_msgs.push((*addr, msg));
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(addr = %addr, error = %e, "Peer error");
                        disconnected.push(*addr);
                        break;
                    }
                    Err(_) => break, // Timeout, no more messages
                }
            }
        }

        // Process collected messages (no borrow conflict)
        for (addr, msg) in received_msgs {
            self.handle_message(addr, &msg).await;
        }

        // Remove disconnected peers
        for addr in disconnected {
            self.peers.remove(&addr);
        }
    }

    async fn handle_message(&mut self, addr: SocketAddr, msg: &PeerMessage) {
        match msg {
            PeerMessage::Bitfield(bitfield) => {
                // Parse which pieces this peer has
                if let Some(peer_state) = self.peers.get_mut(&addr) {
                    for (byte_idx, &byte) in bitfield.iter().enumerate() {
                        for bit in 0..8 {
                            let piece_idx = (byte_idx * 8 + bit) as u32;
                            if piece_idx >= self.pieces.len() as u32 {
                                break;
                            }
                            if (byte & (1 << (7 - bit))) != 0 {
                                peer_state.available_pieces.insert(piece_idx);
                            }
                        }
                    }
                }
            }
            PeerMessage::Have { piece_index } => {
                if let Some(peer_state) = self.peers.get_mut(&addr) {
                    peer_state.available_pieces.insert(*piece_index);
                }
            }
            PeerMessage::Piece { index, begin, data } => {
                // Apply rate limiting before processing
                if let Some(ref limiter) = self.rate_limiter {
                    limiter.acquire(data.len() as u64).await;
                }

                tracing::debug!(
                    piece = index,
                    offset = begin,
                    size = data.len(),
                    "Received block"
                );

                // Record response time and update peer score
                let _response_time = if let Some(peer_state) = self.peers.get_mut(&addr) {
                    if let Some(request_time) = peer_state.requests_sent.remove(&(*index, *begin)) {
                        let duration = request_time.elapsed();
                        peer_state.record_response(duration);
                        peer_state.downloaded_bytes += data.len() as u64;
                        duration
                    } else {
                        Duration::from_millis(100)
                    }
                } else {
                    Duration::from_millis(100)
                };

                let piece = &mut self.pieces[*index as usize];
                piece.add_block(*begin, data.clone());
                self.downloaded += data.len() as u64;

                // Update peer score
                if let Some(peer_state) = self.peers.get_mut(&addr) {
                    peer_state.update_score();
                }

                // Update tracker stats
                let left = self.meta.total_size() - self.downloaded;
                self.tracker
                    .update_stats(self.uploaded, self.downloaded, left);

                // Check if piece is complete
                if piece.complete
                    && let Some(data) = piece.assemble()
                {
                    if piece.verify(&data) {
                        tracing::info!(piece = index, "Piece verified");
                        self.downloaded_pieces.insert(*index);
                        self.progress.mark_complete(*index);
                        self.progress.downloaded = self.downloaded;
                        // Persist progress after each piece
                        if let Err(e) = progress::save_progress(
                            &self.download_dir,
                            &self.meta.info.name,
                            &self.progress,
                        ) {
                            tracing::warn!(error = %e, "Failed to save progress");
                        }
                    } else {
                        tracing::warn!(piece = index, "Piece verification failed");
                        // Reset piece state
                        let hash = piece.hash;
                        *piece = PieceState::new(*index, piece.length, hash);
                    }
                }
            }
            _ => {}
        }
    }

    fn is_complete(&self) -> bool {
        self.downloaded_pieces.len() == self.pieces.len()
    }

    pub fn progress(&self) -> (usize, usize) {
        (self.downloaded_pieces.len(), self.pieces.len())
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    async fn save_file(&self) -> Result<(), DownloadError> {
        let output_path = self.download_dir.join(&self.meta.info.name);
        tokio::fs::create_dir_all(&self.download_dir)
            .await
            .map_err(|e| DownloadError::Io(e.to_string()))?;

        // Create output file
        let mut file = tokio::fs::File::create(&output_path)
            .await
            .map_err(|e| DownloadError::Io(e.to_string()))?;

        // Stream pieces to file instead of accumulating in memory
        use tokio::io::AsyncWriteExt;
        for piece in &self.pieces {
            if let Some(data) = piece.assemble() {
                file.write_all(&data)
                    .await
                    .map_err(|e| DownloadError::Io(e.to_string()))?;
            } else {
                return Err(DownloadError::Io("incomplete piece".to_string()));
            }
        }

        file.flush()
            .await
            .map_err(|e| DownloadError::Io(e.to_string()))?;

        tracing::info!(path = %output_path.display(), "File saved");

        // Remove progress file after successful completion
        let progress_path = progress::progress_path(&self.download_dir, &self.meta.info.name);
        if progress_path.exists()
            && let Err(e) = std::fs::remove_file(&progress_path)
        {
            tracing::warn!(error = %e, "Failed to remove progress file");
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("tracker error: {0}")]
    Tracker(String),
    #[error("peer error: {0}")]
    Peer(String),
    #[error("IO error: {0}")]
    Io(String),
}
