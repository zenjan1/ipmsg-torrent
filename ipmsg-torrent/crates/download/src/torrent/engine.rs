//! Torrent download engine - coordinates tracker, peers, and piece management

use super::meta::TorrentMeta;
use super::peer::{PeerConnection, PeerMessage};
use super::tracker::{AnnounceEvent, HttpTracker};
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
struct PeerState {
    connection: PeerConnection,
    available_pieces: HashSet<u32>,
    requests_sent: HashMap<(u32, u32), tokio::time::Instant>, // (piece, block) -> time
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

        Self {
            meta: Arc::new(meta),
            peer_id,
            download_dir,
            pieces,
            peers: HashMap::new(),
            downloaded_pieces: HashSet::new(),
            tracker,
            downloaded: 0,
            uploaded: 0,
        }
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

        let peer_state = PeerState {
            connection: conn,
            available_pieces: HashSet::new(),
            requests_sent: HashMap::new(),
        };

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

        // Request blocks from available peers
        for (addr, peer_state) in &mut self.peers {
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

                // Find blocks we haven't requested yet
                for block_idx in 0..piece.blocks_total {
                    let offset = block_idx * BLOCK_SIZE;

                    // Skip if already requested
                    if peer_state.requests_sent.contains_key(&(*piece_idx, offset)) {
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

                    // Stop if we hit the limit
                    if peer_state.requests_sent.len() >= MAX_IN_FLIGHT {
                        break;
                    }
                }

                // Only request from one piece per peer per iteration
                break;
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
                tracing::debug!(
                    piece = index,
                    offset = begin,
                    size = data.len(),
                    "Received block"
                );

                // Add block to piece state
                if let Some(peer_state) = self.peers.get_mut(&addr) {
                    peer_state.requests_sent.remove(&(*index, *begin));
                }

                let piece = &mut self.pieces[*index as usize];
                piece.add_block(*begin, data.clone());
                self.downloaded += data.len() as u64;

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
        let mut file_data = Vec::with_capacity(self.meta.total_size() as usize);

        // Assemble all pieces in order
        for piece in &self.pieces {
            if let Some(data) = piece.assemble() {
                file_data.extend_from_slice(&data);
            } else {
                return Err(DownloadError::Io("incomplete piece".to_string()));
            }
        }

        // Write to file
        let output_path = self.download_dir.join(&self.meta.info.name);
        tokio::fs::create_dir_all(&self.download_dir)
            .await
            .map_err(|e| DownloadError::Io(e.to_string()))?;

        tokio::fs::write(&output_path, &file_data)
            .await
            .map_err(|e| DownloadError::Io(e.to_string()))?;

        tracing::info!(path = %output_path.display(), "File saved");

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
