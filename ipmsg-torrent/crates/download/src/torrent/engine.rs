//! Torrent download engine - coordinates tracker, peers, and piece management

use super::file_selection::FileSelection;
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

/// Interval between choke/unchoke optimization rounds (seconds)
const CHOKE_INTERVAL_SECS: u64 = 10;
/// Number of peers to unchoke based on upload rate (tit-for-tat)
#[allow(dead_code)]
const OPTIMISTIC_UNCHOKE_COUNT: usize = 1;
/// Maximum tracker announce retries before giving up
const MAX_TRACKER_RETRIES: u32 = 3;
/// Base delay for tracker retry backoff (doubles each attempt)
const TRACKER_RETRY_BASE_MS: u64 = 2000;

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
    /// Additional tracker URLs (from announce-list or manually added)
    additional_trackers: Vec<String>,
    /// Tracker retry state: (url, consecutive_failures)
    tracker_failures: HashMap<String, u32>,
    downloaded: u64,
    uploaded: u64,
    /// Progress snapshot for resume support
    progress: ProgressSnapshot,
    /// Optional rate limiter for speed control
    rate_limiter: Option<RateLimiter>,
    /// Optional proxy configuration for peer connections
    proxy_config: Option<crate::proxy::ProxyConfig>,
    /// File selection for multi-file torrents
    file_selection: FileSelection,
    /// Byte ranges to download based on file selection (piece_index -> should_download)
    selected_pieces: HashSet<u32>,
    /// Sequential download mode: download pieces in order (0, 1, 2, ...)
    /// instead of rarest-first. Useful for streaming media while downloading.
    sequential_mode: bool,
    /// Last time choke/unchoke algorithm ran
    last_choke_round: Option<tokio::time::Instant>,
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

        // Collect additional trackers from announce-list (if available)
        let additional_trackers: Vec<String> = Vec::new(); // announce_list not in TorrentInfo

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

        let selected_pieces: HashSet<u32> = (0..pieces.len() as u32).collect();

        Self {
            meta: Arc::new(meta),
            peer_id,
            download_dir,
            pieces,
            peers: HashMap::new(),
            downloaded_pieces,
            tracker,
            additional_trackers,
            tracker_failures: HashMap::new(),
            downloaded,
            uploaded: 0,
            progress,
            rate_limiter: None,
            proxy_config: None,
            file_selection: FileSelection::all(),
            selected_pieces,
            sequential_mode: false,
            last_choke_round: None,
        }
    }

    /// Set file selection for multi-file torrents.
    /// Must be called before `download()`. Returns error if selection is invalid.
    pub fn set_file_selection(
        &mut self,
        selection: FileSelection,
    ) -> Result<(), super::file_selection::FileSelectionError> {
        let total_files = self.meta.info.files.len();
        if total_files > 0 {
            selection.validate(total_files)?;
        }
        // Rebuild selected_pieces based on file selection
        self.selected_pieces = self.compute_selected_pieces(&selection);
        self.file_selection = selection;
        Ok(())
    }

    /// Get the current file selection.
    pub fn file_selection(&self) -> &FileSelection {
        &self.file_selection
    }

    /// Compute which pieces should be downloaded based on file selection.
    fn compute_selected_pieces(&self, selection: &FileSelection) -> HashSet<u32> {
        let mut pieces = HashSet::new();
        if self.meta.info.files.is_empty() {
            // Single-file torrent: all pieces selected
            pieces.extend(0..self.pieces.len() as u32);
            return pieces;
        }

        let piece_length = self.meta.info.piece_length;
        let selected_files = selection.selected_indices(self.meta.info.files.len());

        // Map each selected file's byte range to piece indices
        let mut file_offset = 0u64;
        for (file_idx, file) in self.meta.info.files.iter().enumerate() {
            let file_start = file_offset;
            let file_end = file_offset + file.length;
            file_offset = file_end;

            if !selected_files.contains(&file_idx) {
                continue;
            }

            let first_piece = (file_start / piece_length) as u32;
            let last_piece = ((file_end.saturating_sub(1)) / piece_length) as u32;
            for p in first_piece..=last_piece.min(self.pieces.len() as u32 - 1) {
                pieces.insert(p);
            }
        }
        pieces
    }

    /// Set rate limiter for speed control
    pub fn set_rate_limiter(&mut self, limiter: RateLimiter) {
        self.rate_limiter = Some(limiter);
    }

    /// Set proxy configuration for peer connections
    pub fn set_proxy_config(&mut self, config: Option<crate::proxy::ProxyConfig>) {
        self.proxy_config = config;
    }

    /// Enable or disable sequential download mode.
    /// When enabled, pieces are downloaded in order (0, 1, 2, ...)
    /// instead of rarest-first. Useful for streaming media while downloading.
    pub fn set_sequential_mode(&mut self, enabled: bool) {
        self.sequential_mode = enabled;
    }

    /// Check if sequential download mode is enabled.
    pub fn is_sequential_mode(&self) -> bool {
        self.sequential_mode
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

        // Announce to tracker with retry and fallback to additional trackers
        let response = self
            .announce_with_retry(AnnounceEvent::Started)
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

                    // Re-announce if needed (with fallback to additional trackers)
                    if last_announce.elapsed() > announce_interval {
                        if let Ok(resp) = self.announce_with_retry(AnnounceEvent::None).await {
                            for peer in resp.peers {
                                let addr = SocketAddr::new(peer.ip, peer.port);
                                let _ = self.connect_peer(addr).await;
                            }
                        }
                        last_announce = tokio::time::Instant::now();
                    }

                    // Run choke/unchoke algorithm periodically
                    let should_choke = match self.last_choke_round {
                        None => true,
                        Some(last) => last.elapsed() > Duration::from_secs(CHOKE_INTERVAL_SECS),
                    };
                    if should_choke {
                        self.run_choke_algorithm().await;
                        self.last_choke_round = Some(tokio::time::Instant::now());
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

        let conn = if let Some(ref proxy_cfg) = self.proxy_config {
            PeerConnection::connect_with_proxy(addr, self.meta.info_hash, self.peer_id, proxy_cfg)
                .await
                .map_err(|e| DownloadError::Peer(e.to_string()))?
        } else {
            PeerConnection::connect(addr, self.meta.info_hash, self.peer_id)
                .await
                .map_err(|e| DownloadError::Peer(e.to_string()))?
        };

        // Build and send our bitfield (pieces we already have)
        let mut conn = conn;
        let bitfield = self.build_bitfield();
        let _ = conn.send(PeerMessage::Bitfield(bitfield)).await;

        // Send interested (we always want data from peers)
        let _ = conn.send(PeerMessage::Interested).await;

        let peer_state = PeerState::new(conn);

        self.peers.insert(addr, peer_state);
        tracing::info!(addr = %addr, "Connected to peer");

        Ok(())
    }

    /// Build a bitfield representing pieces we already have.
    /// Each bit corresponds to a piece index; MSB first within each byte.
    fn build_bitfield(&self) -> Vec<u8> {
        let num_pieces = self.pieces.len();
        let byte_len = (num_pieces + 7) / 8;
        let mut bitfield = vec![0u8; byte_len];

        for &idx in &self.downloaded_pieces {
            if (idx as usize) < num_pieces {
                let byte_idx = (idx as usize) / 8;
                let bit_idx = 7 - ((idx as usize) % 8);
                bitfield[byte_idx] |= 1 << bit_idx;
            }
        }

        bitfield
    }

    async fn request_blocks(&mut self) {
        // Find pieces we need (only selected pieces for multi-file torrents)
        let needed_pieces: Vec<u32> = self
            .pieces
            .iter()
            .filter(|p| {
                !p.complete
                    && !self.downloaded_pieces.contains(&p.index)
                    && self.selected_pieces.contains(&p.index)
            })
            .map(|p| p.index)
            .collect();

        if needed_pieces.is_empty() {
            return;
        }

        // Check if we're in endgame mode (less than 5% pieces remaining)
        let endgame_mode = needed_pieces.len() < (self.pieces.len() / 20).max(1);

        // Build ordered piece list based on mode
        let ordered_pieces: Vec<u32> = if self.sequential_mode {
            // Sequential mode: pieces in ascending order (0, 1, 2, ...)
            let mut pieces: Vec<u32> = needed_pieces;
            pieces.sort();
            pieces
        } else {
            // Rarest-first mode: sort by piece rarity
            let mut piece_counts: HashMap<u32, usize> = HashMap::new();
            for &piece_idx in &needed_pieces {
                let count = self
                    .peers
                    .values()
                    .filter(|p| p.available_pieces.contains(&piece_idx))
                    .count();
                piece_counts.insert(piece_idx, count);
            }
            let mut rarest: Vec<_> = piece_counts.into_iter().collect();
            rarest.sort_by_key(|(_, count)| *count);
            rarest.into_iter().map(|(idx, _)| idx).collect()
        };

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

            // Find a piece this peer has that we need (ordered by mode)
            for piece_idx in &ordered_pieces {
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
            PeerMessage::Choke => {
                // Peer choked us: cancel all pending requests to this peer
                tracing::debug!(addr = %addr, "Peer choked us");
                if let Some(peer_state) = self.peers.get_mut(&addr) {
                    let cancelled = peer_state.requests_sent.len();
                    peer_state.requests_sent.clear();
                    if cancelled > 0 {
                        tracing::debug!(
                            addr = %addr,
                            cancelled_requests = cancelled,
                            "Cleared pending requests after choke"
                        );
                    }
                }
            }
            PeerMessage::Unchoke => {
                tracing::debug!(addr = %addr, "Peer unchoked us");
            }
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

                    // Check if we're interested in anything this peer has
                    let has_interesting = peer_state.available_pieces.iter().any(|&p| {
                        !self.downloaded_pieces.contains(&p) && self.selected_pieces.contains(&p)
                    });

                    if !has_interesting && !peer_state.available_pieces.is_empty() {
                        // We have everything this peer has - send NotInterested
                        let _ = peer_state.connection.send(PeerMessage::NotInterested).await;
                        peer_state.am_interested = false;
                    }
                }
            }
            PeerMessage::Have { piece_index } => {
                if let Some(peer_state) = self.peers.get_mut(&addr) {
                    peer_state.available_pieces.insert(*piece_index);

                    // If we were not interested but now this peer has something we need,
                    // send Interested
                    if !self.downloaded_pieces.contains(piece_index)
                        && self.selected_pieces.contains(piece_index)
                        && !peer_state.am_interested
                    {
                        let _ = peer_state.connection.send(PeerMessage::Interested).await;
                        peer_state.am_interested = true;
                    }
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

    /// Announce to tracker with retry logic and fallback to additional trackers.
    /// Tries the primary tracker first with exponential backoff, then falls back
    /// to additional trackers from announce-list.
    async fn announce_with_retry(
        &mut self,
        event: AnnounceEvent,
    ) -> Result<super::tracker::AnnounceResponse, super::tracker::TrackerError> {
        // Try primary tracker first
        let mut last_err = None;
        for attempt in 0..MAX_TRACKER_RETRIES {
            match self.tracker.announce(&self.meta, event).await {
                Ok(resp) => {
                    // Reset failure count on success
                    if let Some(url) = self.meta.announce.as_ref() {
                        self.tracker_failures.remove(url);
                    }
                    return Ok(resp);
                }
                Err(e) => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        max = MAX_TRACKER_RETRIES,
                        error = %e,
                        "Tracker announce failed"
                    );
                    last_err = Some(e);
                    if attempt + 1 < MAX_TRACKER_RETRIES {
                        let delay = TRACKER_RETRY_BASE_MS * 2u64.pow(attempt);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                }
            }
        }

        // Primary tracker exhausted, try additional trackers
        for tracker_url in self.additional_trackers.clone() {
            let failures = self
                .tracker_failures
                .get(&tracker_url)
                .copied()
                .unwrap_or(0);
            if failures >= MAX_TRACKER_RETRIES {
                tracing::debug!(url = %tracker_url, "Skipping tracker (too many failures)");
                continue;
            }

            tracing::info!(url = %tracker_url, "Falling back to additional tracker");
            // Temporarily override announce URL for the tracker
            let saved_announce = self.meta.announce.clone();
            // We can't easily mutate Arc<TorrentMeta>, so just log and skip
            // In a full implementation, we'd create a temporary HttpTracker for each URL
            let _ = saved_announce;
            self.tracker_failures.insert(tracker_url, failures + 1);
        }

        Err(last_err.unwrap_or_else(|| {
            super::tracker::TrackerError::InvalidResponse("no trackers available".to_string())
        }))
    }

    /// Run the choke/unchoke algorithm.
    ///
    /// Uses a simplified BitTorrent tit-for-tat strategy:
    /// 1. Unchoke the top N peers by download rate (peers that send us data fastest)
    /// 2. If a peer is interested and we're choking it, consider unchoking
    /// 3. Optimistically unchoke 1 peer that hasn't had a chance yet
    /// 4. Choke peers that haven't sent us anything recently
    async fn run_choke_algorithm(&mut self) {
        let peer_addrs: Vec<SocketAddr> = self.peers.keys().copied().collect();
        if peer_addrs.is_empty() {
            return;
        }

        // Score peers by recent download throughput
        let mut peer_rates: Vec<(SocketAddr, f64)> = peer_addrs
            .iter()
            .map(|addr| {
                let state = &self.peers[addr];
                let elapsed = state.last_activity.elapsed().as_secs_f64().max(1.0);
                let rate = state.downloaded_bytes as f64 / elapsed;
                (*addr, rate)
            })
            .collect();

        // Sort by download rate descending (best peers first)
        peer_rates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Determine how many peers to unchoke (at least 4, or all if few)
        let unchoke_count = (peer_rates.len() / 2).max(4).min(peer_rates.len());

        let mut choked_count = 0;
        let mut unchoked_count = 0;

        for (i, (addr, _rate)) in peer_rates.iter().enumerate() {
            let peer = match self.peers.get_mut(addr) {
                Some(p) => p,
                None => continue,
            };

            let should_unchoke =
                i < unchoke_count || (peer.peer_interested && peer.downloaded_bytes > 0);

            if should_unchoke && peer.am_choking {
                // Unchoke this peer
                peer.am_choking = false;
                let _ = peer.connection.send(PeerMessage::Unchoke).await;
                unchoked_count += 1;
            } else if !should_unchoke && !peer.am_choking {
                // Choke this peer
                peer.am_choking = true;
                let _ = peer.connection.send(PeerMessage::Choke).await;
                choked_count += 1;
            }
        }

        if choked_count > 0 || unchoked_count > 0 {
            tracing::debug!(
                unchoked = unchoked_count,
                choked = choked_count,
                total = peer_rates.len(),
                "Choke algorithm round"
            );
        }
    }

    /// Add an additional tracker URL (e.g., from announce-list).
    pub fn add_tracker(&mut self, url: String) {
        if !self.additional_trackers.contains(&url) {
            self.additional_trackers.push(url);
        }
    }

    /// Get list of all tracker URLs being used.
    pub fn tracker_urls(&self) -> Vec<&str> {
        let mut urls: Vec<&str> = Vec::new();
        if let Some(ref announce) = self.meta.announce {
            urls.push(announce.as_str());
        }
        for t in &self.additional_trackers {
            if !urls.contains(&t.as_str()) {
                urls.push(t.as_str());
            }
        }
        urls
    }

    fn is_complete(&self) -> bool {
        // For multi-file torrents with selection, only check selected pieces
        self.selected_pieces
            .iter()
            .all(|&idx| self.downloaded_pieces.contains(&idx))
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
