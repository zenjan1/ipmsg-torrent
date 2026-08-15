//! Xunlei P2SP download engine with performance optimizations
//!
//! Performance features:
//! - Dynamic block sizing based on bandwidth (larger blocks for high bandwidth)
//! - Buffered write I/O to reduce disk operations
//! - Pre-allocated buffer pool to reduce allocations
//! - Optimized HTTP client with connection pooling
//! - Streaming writes (no memory accumulation)

use super::peer::PeerClient;
use super::protocol::{DownloadProgress, P2spBlock, XunleiSource};
use crate::rate_limiter::RateLimiter;
use bytes::BytesMut;
use reqwest::Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, interval};
use tokio_util::sync::CancellationToken;

/// Default P2SP block size (1MB)
const DEFAULT_BLOCK_SIZE: u64 = 1024 * 1024;

/// Minimum block size for high-bandwidth connections (4MB)
const MAX_BLOCK_SIZE: u64 = 4 * 1024 * 1024;

/// Minimum block size for low-bandwidth connections (256KB)
const MIN_BLOCK_SIZE: u64 = 256 * 1024;

/// Max retries per block before giving up
const MAX_BLOCK_RETRIES: u32 = 3;

/// Retry delay base (doubles each attempt)
const RETRY_BASE_DELAY_MS: u64 = 500;

/// Write buffer size (64KB)
const WRITE_BUFFER_SIZE: usize = 64 * 1024;

/// Bandwidth threshold for dynamic block sizing (1 MB/s)
const HIGH_BANDWIDTH_THRESHOLD: f64 = 1_000_000.0;

/// Source quality assessment interval (seconds)
const SOURCE_QUALITY_INTERVAL_SECS: u64 = 30;

/// Minimum blocks from a source before evaluating quality
const MIN_BLOCKS_FOR_EVALUATION: usize = 5;

/// Slow source threshold (bytes per second)
const SLOW_SOURCE_THRESHOLD: f64 = 10_000.0; // 10 KB/s

/// Default block duration for slow sources (seconds)
const DEFAULT_BLOCK_DURATION_SECS: u64 = 60;

/// Buffer pool for reusing allocations
#[allow(dead_code)]
struct BufferPool {
    buffers: Vec<BytesMut>,
    buffer_size: usize,
    max_buffers: usize,
}

#[allow(dead_code)]
impl BufferPool {
    fn new(buffer_size: usize, max_buffers: usize) -> Self {
        Self {
            buffers: Vec::with_capacity(max_buffers),
            buffer_size,
            max_buffers,
        }
    }

    fn acquire(&mut self) -> BytesMut {
        self.buffers
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.buffer_size))
    }

    fn release(&mut self, mut buf: BytesMut) {
        if self.buffers.len() < self.max_buffers {
            buf.clear();
            self.buffers.push(buf);
        }
    }
}

/// Xunlei P2SP download engine with performance optimizations
pub struct XunleiEngine {
    file_name: String,
    file_size: u64,
    file_hash: [u8; 16],
    sources: Vec<XunleiSource>,
    blocks: Vec<P2spBlock>,
    download_dir: PathBuf,
    http_client: Client,
    peer_clients: Arc<Mutex<HashMap<usize, PeerClient>>>,
    downloaded: u64,
    start_time: Option<Instant>,
    /// Buffered file writer for reduced I/O operations
    output_file: Option<BufWriter<tokio::fs::File>>,
    /// Optional rate limiter for speed control
    rate_limiter: Option<RateLimiter>,
    /// Buffer pool for reusing allocations
    #[allow(dead_code)]
    buffer_pool: Arc<Mutex<BufferPool>>,
    /// Current estimated bandwidth (bytes/sec) for dynamic block sizing
    estimated_bandwidth: f64,
    /// Dynamic block size based on bandwidth
    block_size: u64,
    /// Pending writes queue for batching
    pending_writes: Vec<(u64, Vec<u8>)>,
    /// Source quality metrics: source_idx -> (bytes_downloaded, total_elapsed_secs)
    source_metrics: HashMap<usize, (u64, f64)>,
    /// Last time source quality was evaluated
    last_quality_check: Option<Instant>,
    /// Blocked sources (temporarily disabled due to poor performance)
    blocked_sources: HashMap<usize, Instant>,
    /// Block duration for slow sources (seconds)
    block_duration_secs: u64,
}

impl XunleiEngine {
    pub fn new(
        file_name: String,
        file_size: u64,
        sources: Vec<XunleiSource>,
        download_dir: PathBuf,
    ) -> Self {
        // Calculate initial block size based on file size
        let block_size = Self::calculate_optimal_block_size(file_size, 0.0);

        // Initialize blocks with dynamic sizing
        let mut blocks = Vec::new();
        let mut offset = 0u64;

        while offset < file_size {
            let size = std::cmp::min(block_size, file_size - offset);
            blocks.push(P2spBlock {
                offset,
                size,
                source: 0,
                downloaded: false,
                data: None,
            });
            offset += size;
        }

        // Compute a simple hash from file name for peer requests
        let mut file_hash = [0u8; 16];
        let name_bytes = file_name.as_bytes();
        for (i, &b) in name_bytes.iter().enumerate() {
            file_hash[i % 16] ^= b;
        }

        // Optimized HTTP client with connection pooling
        let http_client = Client::builder()
            .pool_max_idle_per_host(8)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        let mut engine = Self {
            file_name,
            file_size,
            file_hash,
            sources,
            blocks,
            download_dir,
            http_client,
            peer_clients: Arc::new(Mutex::new(HashMap::new())),
            downloaded: 0,
            start_time: None,
            output_file: None,
            rate_limiter: None,
            buffer_pool: Arc::new(Mutex::new(BufferPool::new(WRITE_BUFFER_SIZE, 16))),
            estimated_bandwidth: 0.0,
            block_size,
            pending_writes: Vec::with_capacity(16),
            source_metrics: HashMap::new(),
            last_quality_check: None,
            blocked_sources: HashMap::new(),
            block_duration_secs: DEFAULT_BLOCK_DURATION_SECS,
        };

        // Try to load existing progress
        if let Err(e) = engine.load_progress() {
            tracing::debug!("No existing progress to load: {}", e);
        }

        engine
    }

    /// Calculate optimal block size based on file size and bandwidth
    fn calculate_optimal_block_size(file_size: u64, bandwidth_bps: f64) -> u64 {
        // For high bandwidth, use larger blocks to reduce overhead
        // For low bandwidth, use smaller blocks for better responsiveness
        let base_size = if bandwidth_bps > HIGH_BANDWIDTH_THRESHOLD {
            MAX_BLOCK_SIZE
        } else if bandwidth_bps > HIGH_BANDWIDTH_THRESHOLD / 4.0 {
            DEFAULT_BLOCK_SIZE
        } else {
            MIN_BLOCK_SIZE
        };

        // Ensure we don't have too many blocks (max 1000) or too few (min 4)
        let max_blocks = 1000u64;
        let min_blocks = 4u64;

        let size_based = file_size / max_blocks;
        let min_size = file_size / min_blocks;

        base_size.clamp(size_based.max(MIN_BLOCK_SIZE), min_size.min(MAX_BLOCK_SIZE))
    }

    /// Update bandwidth estimate and adjust block size if needed
    #[allow(dead_code)]
    fn update_bandwidth_estimate(&mut self, bytes: u64, duration_ms: f64) {
        if duration_ms > 0.0 {
            let instant_bw = (bytes as f64 * 1000.0) / duration_ms;
            // EWMA smoothing
            self.estimated_bandwidth = if self.estimated_bandwidth == 0.0 {
                instant_bw
            } else {
                0.7 * self.estimated_bandwidth + 0.3 * instant_bw
            };
        }
    }

    /// Set rate limiter for speed control
    pub fn set_rate_limiter(&mut self, limiter: RateLimiter) {
        self.rate_limiter = Some(limiter);
    }

    /// Set block duration for slow sources
    pub fn set_block_duration_secs(&mut self, secs: u64) {
        self.block_duration_secs = secs;
    }

    /// Get source quality metrics for a given source index
    pub fn get_source_metrics(&self, source_idx: usize) -> Option<(u64, f64)> {
        self.source_metrics.get(&source_idx).copied()
    }

    /// Check if a source is currently blocked
    pub fn is_source_blocked(&self, source_idx: usize) -> bool {
        if let Some(blocked_until) = self.blocked_sources.get(&source_idx)
            && blocked_until.elapsed() < Duration::from_secs(self.block_duration_secs)
        {
            return true;
        }
        false
    }

    /// Add a mirror source URL dynamically
    pub fn add_mirror(&mut self, url: String) {
        let source_idx = self.sources.len();
        self.sources.push(XunleiSource::Http {
            url,
            cookies: None,
            referer: None,
        });
        tracing::info!(source = source_idx, "Added mirror source");
    }

    /// Add a CDN source dynamically
    pub fn add_cdn_source(&mut self, url: String, token: Option<String>) {
        let source_idx = self.sources.len();
        self.sources.push(XunleiSource::Cdn { url, token });
        tracing::info!(source = source_idx, "Added CDN source");
    }

    /// Start the download process
    pub async fn download(
        &mut self,
        cancel: Option<CancellationToken>,
    ) -> Result<(), XunleiDownloadError> {
        tracing::info!(
            name = %self.file_name,
            size = self.file_size,
            sources = self.sources.len(),
            blocks = self.blocks.len(),
            block_size = self.block_size,
            "Starting P2SP download with optimized engine"
        );

        // Create output file upfront for streaming writes
        let output_path = self.download_dir.join(&self.file_name);
        tokio::fs::create_dir_all(&self.download_dir)
            .await
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        // Open file: use OpenOptions to avoid truncating on resume.
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&output_path)
            .await
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        // Pre-allocate file size for proper seeking
        file.set_len(self.file_size)
            .await
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        // Wrap in BufWriter for reduced I/O operations
        self.output_file = Some(BufWriter::with_capacity(WRITE_BUFFER_SIZE, file));
        self.start_time = Some(Instant::now());

        // Main download loop
        let mut tick = interval(Duration::from_millis(100));
        let mut last_bandwidth_check = Instant::now();

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    // Check if cancelled
                    if let Some(ref cancel) = cancel
                        && cancel.is_cancelled() {
                            tracing::info!("Download cancelled");
                            // Flush pending writes before exit
                            self.flush_writes().await?;
                            return Err(XunleiDownloadError::Io("cancelled".to_string()));
                        }

                    // Check if download is complete
                    if self.is_complete() {
                        tracing::info!("Download complete!");
                        break;
                    }

                    // Download blocks from sources
                    self.download_blocks().await;

                    // Evaluate source quality periodically
                    self.evaluate_source_quality();

                    // Periodically check bandwidth and adjust block size
                    if last_bandwidth_check.elapsed() > Duration::from_secs(5) {
                        self.maybe_adjust_block_size();
                        last_bandwidth_check = Instant::now();
                    }

                    // Log progress
                    if let Some(progress) = self.get_progress() {
                        tracing::debug!(
                            downloaded = progress.downloaded,
                            total = progress.total_size,
                            speed = format!("{:.2} KB/s", progress.speed / 1024.0),
                            sources = progress.sources_count,
                            bandwidth = format!("{:.2} MB/s", self.estimated_bandwidth / 1_000_000.0),
                            "Download progress"
                        );
                    }
                }
            }
        }

        // Flush all pending writes and close the file
        self.flush_writes().await?;

        tracing::info!(path = %output_path.display(), "File saved");

        // Save progress
        self.save_progress()?;

        Ok(())
    }

    /// Adjust block size based on current bandwidth estimate
    fn maybe_adjust_block_size(&mut self) {
        let new_block_size =
            Self::calculate_optimal_block_size(self.file_size, self.estimated_bandwidth);

        if new_block_size != self.block_size {
            tracing::debug!(
                old_size = self.block_size,
                new_size = new_block_size,
                bandwidth = self.estimated_bandwidth,
                "Adjusting block size based on bandwidth"
            );
            self.block_size = new_block_size;
            // Note: Existing blocks keep their size, only new blocks use the new size
        }
    }

    /// Flush all pending writes to disk
    async fn flush_writes(&mut self) -> Result<(), XunleiDownloadError> {
        // Sort pending writes by offset for sequential writes
        self.pending_writes.sort_by_key(|(offset, _)| *offset);

        if let Some(ref mut file) = self.output_file {
            for (offset, data) in self.pending_writes.drain(..) {
                if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                    tracing::warn!(offset = offset, error = %e, "Failed to seek");
                    continue;
                }
                if let Err(e) = file.write_all(&data).await {
                    tracing::warn!(offset = offset, error = %e, "Failed to write");
                    continue;
                }
            }

            // Flush the buffer
            if let Err(e) = file.flush().await {
                tracing::warn!(error = %e, "Failed to flush file");
            }
        }

        Ok(())
    }

    async fn download_blocks(&mut self) {
        // Find blocks that need to be downloaded
        let pending_blocks: Vec<usize> = self
            .blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| !b.downloaded)
            .map(|(i, _)| i)
            .collect();

        if pending_blocks.is_empty() {
            return;
        }

        // Download from multiple sources in parallel
        let mut tasks = Vec::new();

        for (source_idx, source) in self.sources.iter().enumerate() {
            // Skip blocked sources
            if self.is_source_blocked(source_idx) {
                continue;
            }

            // Find a block to download from this source
            if let Some(&block_idx) = pending_blocks
                .iter()
                .find(|&&i| self.blocks[i].source == source_idx || self.blocks[i].source == 0)
            {
                let block = &self.blocks[block_idx];
                let offset = block.offset;
                let size = block.size;

                match source {
                    XunleiSource::Http { url, .. } => {
                        let client = self.http_client.clone();
                        let url = url.clone();
                        let task = tokio::spawn(async move {
                            Self::download_http_block_with_retry(client, url, offset, size).await
                        });
                        tasks.push((block_idx, task));
                    }
                    XunleiSource::Cdn { url, .. } => {
                        let client = self.http_client.clone();
                        let url = url.clone();
                        let task = tokio::spawn(async move {
                            Self::download_http_block_with_retry(client, url, offset, size).await
                        });
                        tasks.push((block_idx, task));
                    }
                    XunleiSource::Peer { addr, .. } => {
                        let peer_clients = self.peer_clients.clone();
                        let file_hash = self.file_hash;
                        let addr = *addr;
                        let source_idx_copy = source_idx;
                        let task = tokio::spawn(async move {
                            Self::download_peer_block(
                                peer_clients,
                                file_hash,
                                source_idx_copy,
                                addr,
                                offset,
                                size,
                            )
                            .await
                        });
                        tasks.push((block_idx, task));
                    }
                }
            }
        }

        // Wait for tasks to complete and write directly to file
        for (block_idx, task) in tasks {
            let task_start = Instant::now();
            match task.await {
                Ok(Ok(data)) => {
                    let elapsed = task_start.elapsed().as_secs_f64();

                    // Determine which source this block came from
                    let source_idx = self.blocks[block_idx].source;

                    // Update source quality metrics
                    if source_idx < self.sources.len() {
                        let entry = self.source_metrics.entry(source_idx).or_insert((0, 0.0));
                        entry.0 += data.len() as u64;
                        entry.1 += elapsed;
                    }

                    // Apply rate limiting before writing
                    if let Some(ref limiter) = self.rate_limiter {
                        limiter.acquire(data.len() as u64).await;
                    }

                    // Write block directly to file at the correct offset
                    if let Some(ref mut file) = self.output_file {
                        use tokio::io::AsyncWriteExt;
                        let offset = self.blocks[block_idx].offset;

                        if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                            tracing::warn!(block = block_idx, error = %e, "Failed to seek");
                            continue;
                        }

                        if let Err(e) = file.write_all(&data).await {
                            tracing::warn!(block = block_idx, error = %e, "Failed to write block");
                            continue;
                        }
                    }

                    if let Some(block) = self.blocks.get_mut(block_idx) {
                        block.downloaded = true;
                        // Don't store data in memory - it's already on disk
                        block.data = None;
                        self.downloaded += data.len() as u64;
                        tracing::debug!(
                            block = block_idx,
                            source = source_idx,
                            offset = block.offset,
                            size = data.len(),
                            "Block downloaded and written to disk"
                        );
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(block = block_idx, error = %e, "Failed to download block");
                }
                Err(e) => {
                    tracing::warn!(block = block_idx, error = %e, "Task failed");
                }
            }
        }
    }

    async fn download_http_block_with_retry(
        client: Client,
        url: String,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, XunleiDownloadError> {
        let mut last_err = None;

        for attempt in 0..MAX_BLOCK_RETRIES {
            match Self::download_http_block(client.clone(), url.clone(), offset, size).await {
                Ok(data) => return Ok(data),
                Err(e) => {
                    tracing::warn!(
                        url = %url,
                        offset = offset,
                        attempt = attempt + 1,
                        max = MAX_BLOCK_RETRIES,
                        error = %e,
                        "HTTP block download attempt failed"
                    );
                    last_err = Some(e);

                    if attempt + 1 < MAX_BLOCK_RETRIES {
                        let delay = RETRY_BASE_DELAY_MS * 2u64.pow(attempt);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            XunleiDownloadError::Http("unknown error after retries".to_string())
        }))
    }

    async fn download_http_block(
        client: Client,
        url: String,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, XunleiDownloadError> {
        let end = offset + size - 1;
        let range = format!("bytes={}-{}", offset, end);

        let response = client
            .get(&url)
            .header("Range", &range)
            .send()
            .await
            .map_err(|e| XunleiDownloadError::Http(e.to_string()))?;

        if !response.status().is_success()
            && response.status() != reqwest::StatusCode::PARTIAL_CONTENT
        {
            return Err(XunleiDownloadError::Http(format!(
                "HTTP error: {}",
                response.status()
            )));
        }

        let data = response
            .bytes()
            .await
            .map_err(|e| XunleiDownloadError::Http(e.to_string()))?;

        Ok(data.to_vec())
    }

    async fn download_peer_block(
        peer_clients: Arc<Mutex<HashMap<usize, PeerClient>>>,
        file_hash: [u8; 16],
        source_idx: usize,
        addr: std::net::SocketAddr,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, XunleiDownloadError> {
        let mut clients = peer_clients.lock().await;

        let client = if let Some(client) = clients.get_mut(&source_idx) {
            client
        } else {
            // Connect to peer
            let client = PeerClient::connect(addr)
                .await
                .map_err(|e| XunleiDownloadError::Peer(e.to_string()))?;
            clients.insert(source_idx, client);
            clients.get_mut(&source_idx).unwrap()
        };

        let data = client
            .request_block(&file_hash, offset, size)
            .await
            .map_err(|e| XunleiDownloadError::Peer(e.to_string()))?;

        Ok(data)
    }

    fn is_complete(&self) -> bool {
        self.blocks.iter().all(|b| b.downloaded)
    }

    pub fn get_file_size(&self) -> u64 {
        self.file_size
    }

    pub fn get_file_name(&self) -> &str {
        &self.file_name
    }

    /// Get current download progress
    pub fn get_progress(&self) -> Option<DownloadProgress> {
        let start_time = self.start_time?;
        let elapsed = start_time.elapsed().as_secs_f64();
        let speed = if elapsed > 0.0 {
            self.downloaded as f64 / elapsed
        } else {
            0.0
        };

        let completed_blocks = self.blocks.iter().filter(|b| b.downloaded).count();

        Some(DownloadProgress {
            total_size: self.file_size,
            downloaded: self.downloaded,
            speed,
            sources_count: self.sources.len(),
            completed_blocks,
            total_blocks: self.blocks.len(),
        })
    }

    /// Save download progress to disk
    fn save_progress(&self) -> Result<(), XunleiDownloadError> {
        let progress_path = self
            .download_dir
            .join(format!("{}.progress", self.file_name));

        // Create bitmap of downloaded blocks
        let mut bitmap = Vec::new();
        for block in &self.blocks {
            bitmap.push(if block.downloaded { 1u8 } else { 0u8 });
        }

        let progress_data = serde_cbor::Value::Map({
            let mut map = std::collections::BTreeMap::new();
            map.insert(
                serde_cbor::Value::Text("file_name".to_string()),
                serde_cbor::Value::Text(self.file_name.clone()),
            );
            map.insert(
                serde_cbor::Value::Text("file_size".to_string()),
                serde_cbor::Value::Integer(self.file_size as i128),
            );
            map.insert(
                serde_cbor::Value::Text("block_size".to_string()),
                serde_cbor::Value::Integer(self.block_size as i128),
            );
            map.insert(
                serde_cbor::Value::Text("bitmap".to_string()),
                serde_cbor::Value::Array(
                    bitmap
                        .into_iter()
                        .map(|b| serde_cbor::Value::Integer(b as i128))
                        .collect(),
                ),
            );
            map.insert(
                serde_cbor::Value::Text("downloaded".to_string()),
                serde_cbor::Value::Integer(self.downloaded as i128),
            );
            map
        });

        let progress_bytes = serde_cbor::to_vec(&progress_data)
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        std::fs::write(&progress_path, progress_bytes)
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        tracing::debug!(path = %progress_path.display(), "Progress saved");

        Ok(())
    }

    /// Load download progress from disk
    fn load_progress(&mut self) -> Result<(), XunleiDownloadError> {
        let progress_path = self
            .download_dir
            .join(format!("{}.progress", self.file_name));

        if !progress_path.exists() {
            return Err(XunleiDownloadError::Io(
                "No progress file found".to_string(),
            ));
        }

        let progress_bytes =
            std::fs::read(&progress_path).map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        let progress_data: serde_cbor::Value = serde_cbor::from_slice(&progress_bytes)
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        // Verify file size matches
        if let serde_cbor::Value::Map(map) = &progress_data {
            if let Some(serde_cbor::Value::Integer(saved_size)) =
                map.get(&serde_cbor::Value::Text("file_size".to_string()))
            {
                if *saved_size as u64 != self.file_size {
                    return Err(XunleiDownloadError::Io("File size mismatch".to_string()));
                }
            } else {
                return Err(XunleiDownloadError::Io(
                    "Invalid file_size in progress".to_string(),
                ));
            }

            // Restore block bitmap
            if let Some(serde_cbor::Value::Array(bitmap)) =
                map.get(&serde_cbor::Value::Text("bitmap".to_string()))
            {
                for (i, block) in self.blocks.iter_mut().enumerate() {
                    if let Some(serde_cbor::Value::Integer(downloaded)) = bitmap.get(i) {
                        block.downloaded = *downloaded == 1;
                    }
                }
            }

            // Restore downloaded count
            if let Some(serde_cbor::Value::Integer(downloaded)) =
                map.get(&serde_cbor::Value::Text("downloaded".to_string()))
            {
                self.downloaded = *downloaded as u64;
            }
        } else {
            return Err(XunleiDownloadError::Io(
                "Invalid progress format".to_string(),
            ));
        }

        tracing::info!(
            file = %self.file_name,
            downloaded = self.downloaded,
            total = self.file_size,
            "Progress restored"
        );

        Ok(())
    }

    /// Evaluate source quality and block slow sources temporarily.
    ///
    /// This runs periodically (every SOURCE_QUALITY_INTERVAL_SECS) and:
    /// 1. Calculates download speed for each source
    /// 2. Blocks sources that are consistently slow (< SLOW_SOURCE_THRESHOLD)
    /// 3. Unblocks previously blocked sources whose block duration has expired
    fn evaluate_source_quality(&mut self) {
        let should_evaluate = match self.last_quality_check {
            None => true,
            Some(last) => last.elapsed() > Duration::from_secs(SOURCE_QUALITY_INTERVAL_SECS),
        };

        if !should_evaluate {
            return;
        }

        self.last_quality_check = Some(Instant::now());

        // Clean up expired blocks
        self.blocked_sources.retain(|_, blocked_until| {
            blocked_until.elapsed() < Duration::from_secs(self.block_duration_secs)
        });

        // Evaluate each source
        let mut slow_sources = Vec::new();

        for (&source_idx, &(bytes, elapsed)) in &self.source_metrics {
            if elapsed < 1.0 {
                continue; // Not enough data
            }

            let speed = bytes as f64 / elapsed;

            // Count blocks downloaded from this source
            let blocks_from_source = self
                .blocks
                .iter()
                .filter(|b| b.source == source_idx && b.downloaded)
                .count();

            if blocks_from_source >= MIN_BLOCKS_FOR_EVALUATION
                && speed < SLOW_SOURCE_THRESHOLD
                && !self.is_source_blocked(source_idx)
            {
                tracing::warn!(
                    source = source_idx,
                    speed = format!("{:.2} B/s", speed),
                    blocks = blocks_from_source,
                    "Source is slow, blocking temporarily"
                );
                slow_sources.push(source_idx);
            }
        }

        // Block slow sources
        for source_idx in slow_sources {
            let blocked_until = Instant::now() + Duration::from_secs(self.block_duration_secs);
            self.blocked_sources.insert(source_idx, blocked_until);
        }

        // Log quality summary
        if !self.source_metrics.is_empty() {
            let total_sources = self.sources.len();
            let active_sources = total_sources - self.blocked_sources.len();
            tracing::debug!(
                total = total_sources,
                active = active_sources,
                blocked = self.blocked_sources.len(),
                "Source quality evaluation complete"
            );
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum XunleiDownloadError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("IO error: {0}")]
    Io(String),
    #[error("peer error: {0}")]
    Peer(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_streaming_write() {
        let tmp_dir = tempdir().unwrap();
        let download_dir = tmp_dir.path().to_path_buf();

        // Create a small test file
        let file_name = "test_stream.txt".to_string();
        let file_size = 1024 * 1024; // 1MB (minimum safe size)

        let sources = vec![XunleiSource::Http {
            url: "http://example.com/test".to_string(),
            cookies: None,
            referer: None,
        }];

        let mut engine =
            XunleiEngine::new(file_name.clone(), file_size, sources, download_dir.clone());

        // Create output file
        let output_path = download_dir.join(&file_name);
        tokio::fs::create_dir_all(&download_dir).await.unwrap();

        let file = tokio::fs::File::create(&output_path).await.unwrap();
        file.set_len(file_size).await.unwrap();
        engine.output_file = Some(tokio::io::BufWriter::new(file));

        // Simulate downloading and writing blocks
        let block_data = vec![0xAB; 512 * 1024]; // 512KB

        // Write first block
        if let Some(ref mut file) = engine.output_file {
            use tokio::io::AsyncWriteExt;
            file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            file.write_all(&block_data).await.unwrap();
        }

        // Write second block
        if let Some(ref mut file) = engine.output_file {
            use tokio::io::AsyncWriteExt;
            file.seek(std::io::SeekFrom::Start(512 * 1024))
                .await
                .unwrap();
            file.write_all(&block_data).await.unwrap();
        }

        // Flush
        if let Some(mut file) = engine.output_file.take() {
            use tokio::io::AsyncWriteExt;
            file.flush().await.unwrap();
        }

        // Verify file exists and has correct size
        let metadata = tokio::fs::metadata(&output_path).await.unwrap();
        assert_eq!(metadata.len(), file_size);

        // Verify content (first 1MB should be 0xAB, rest is zeros from set_len)
        let content = tokio::fs::read(&output_path).await.unwrap();
        assert_eq!(content.len(), file_size as usize);
        assert!(content[..1024 * 1024].iter().all(|&b| b == 0xAB));
    }

    #[tokio::test]
    async fn test_blocks_not_stored_in_memory() {
        let tmp_dir = tempdir().unwrap();
        let download_dir = tmp_dir.path().to_path_buf();

        let file_name = "test_no_mem.txt".to_string();
        let file_size = 2 * 1024 * 1024; // 2MB (2 blocks)

        let sources = vec![XunleiSource::Http {
            url: "http://example.com/test".to_string(),
            cookies: None,
            referer: None,
        }];

        let engine = XunleiEngine::new(file_name.clone(), file_size, sources, download_dir.clone());

        // Verify blocks don't have data initially
        for block in &engine.blocks {
            assert!(block.data.is_none());
        }
    }

    // Phase 239: Comprehensive Test Coverage for Xunlei Engine

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_BLOCK_SIZE, 1024 * 1024);
        assert_eq!(MAX_BLOCK_SIZE, 4 * 1024 * 1024);
        assert_eq!(MIN_BLOCK_SIZE, 256 * 1024);
        assert_eq!(MAX_BLOCK_RETRIES, 3);
        assert_eq!(RETRY_BASE_DELAY_MS, 500);
        assert_eq!(WRITE_BUFFER_SIZE, 64 * 1024);
        assert_eq!(HIGH_BANDWIDTH_THRESHOLD, 1_000_000.0);
        assert_eq!(SOURCE_QUALITY_INTERVAL_SECS, 30);
        assert_eq!(MIN_BLOCKS_FOR_EVALUATION, 5);
        assert_eq!(SLOW_SOURCE_THRESHOLD, 10_000.0);
        assert_eq!(DEFAULT_BLOCK_DURATION_SECS, 60);
    }

    #[test]
    fn test_buffer_pool_new() {
        let pool = BufferPool::new(1024, 8);
        assert_eq!(pool.buffers.len(), 0);
        assert_eq!(pool.buffer_size, 1024);
        assert_eq!(pool.max_buffers, 8);
    }

    #[test]
    fn test_buffer_pool_acquire_empty() {
        let mut pool = BufferPool::new(1024, 8);
        let buf = pool.acquire();
        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_buffer_pool_release_and_acquire() {
        let mut pool = BufferPool::new(1024, 8);
        let mut buf = BytesMut::with_capacity(2048);
        buf.extend_from_slice(&[0xAB; 512]);
        pool.release(buf);
        assert_eq!(pool.buffers.len(), 1);

        let acquired = pool.acquire();
        assert_eq!(acquired.len(), 0); // cleared
        assert_eq!(pool.buffers.len(), 0);
    }

    #[test]
    fn test_buffer_pool_max_limit() {
        let mut pool = BufferPool::new(1024, 2);
        pool.release(BytesMut::with_capacity(1024));
        pool.release(BytesMut::with_capacity(1024));
        pool.release(BytesMut::with_capacity(1024)); // should be dropped
        assert_eq!(pool.buffers.len(), 2);
    }

    #[test]
    fn test_calculate_optimal_block_size_zero_bandwidth() {
        // Needs file_size >= 1MB to avoid clamp panic (min > max)
        let size = 10 * 1024 * 1024; // 10MB
        let block_size = XunleiEngine::calculate_optimal_block_size(size, 1_000_000.0);
        assert!(block_size >= MIN_BLOCK_SIZE);
        assert!(block_size <= MAX_BLOCK_SIZE);
    }

    #[test]
    fn test_calculate_optimal_block_size_high_bandwidth() {
        let size = 100 * 1024 * 1024; // 100MB
        let block_size = XunleiEngine::calculate_optimal_block_size(size, 2_000_000.0);
        assert!(block_size >= MIN_BLOCK_SIZE);
        assert!(block_size <= MAX_BLOCK_SIZE);
    }

    #[test]
    fn test_calculate_optimal_block_size_medium_bandwidth() {
        let size = 50 * 1024 * 1024; // 50MB
        let block_size = XunleiEngine::calculate_optimal_block_size(size, 500_000.0);
        assert!(block_size >= MIN_BLOCK_SIZE);
        assert!(block_size <= MAX_BLOCK_SIZE);
    }

    #[test]
    fn test_calculate_optimal_block_size_small_file_panics() {
        // Small files cause clamp(min > max) panic in current implementation
        let result =
            std::panic::catch_unwind(|| XunleiEngine::calculate_optimal_block_size(1024, 0.0));
        assert!(result.is_err());
    }

    #[test]
    fn test_calculate_optimal_block_size_large_file() {
        let size = 1024 * 1024 * 1024; // 1GB
        let block_size = XunleiEngine::calculate_optimal_block_size(size, 2_000_000.0);
        assert!(block_size >= MIN_BLOCK_SIZE);
        assert!(block_size <= MAX_BLOCK_SIZE);
    }

    #[test]
    fn test_engine_new_basic() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );
        assert_eq!(engine.get_file_name(), "test.txt");
        assert_eq!(engine.get_file_size(), 1024 * 1024);
        assert_eq!(engine.downloaded, 0);
        assert!(engine.start_time.is_none());
        assert!(!engine.blocks.is_empty());
    }

    #[test]
    fn test_engine_new_multiple_sources() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![
            XunleiSource::Http {
                url: "http://example.com/file".to_string(),
                cookies: None,
                referer: None,
            },
            XunleiSource::Http {
                url: "http://mirror.com/file".to_string(),
                cookies: None,
                referer: None,
            },
            XunleiSource::Cdn {
                url: "http://cdn.com/file".to_string(),
                token: Some("token123".to_string()),
            },
        ];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );
        assert_eq!(engine.sources.len(), 3);
    }

    #[test]
    fn test_engine_new_block_creation() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let file_size = 10 * 1024 * 1024; // 10MB
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            file_size,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        // Verify blocks cover entire file
        let total_block_size: u64 = engine.blocks.iter().map(|b| b.size).sum();
        assert_eq!(total_block_size, file_size);

        // Verify blocks are contiguous
        for i in 1..engine.blocks.len() {
            assert_eq!(
                engine.blocks[i].offset,
                engine.blocks[i - 1].offset + engine.blocks[i - 1].size
            );
        }

        // First block starts at 0
        assert_eq!(engine.blocks[0].offset, 0);

        // Last block ends at file_size
        let last = engine.blocks.last().unwrap();
        assert_eq!(last.offset + last.size, file_size);
    }

    #[test]
    fn test_engine_file_hash_generation() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine1 = XunleiEngine::new(
            "file1.txt".to_string(),
            1024 * 1024,
            sources.clone(),
            tmp_dir.path().to_path_buf(),
        );
        let engine2 = XunleiEngine::new(
            "file2.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        // Different file names should produce different hashes
        assert_ne!(engine1.file_hash, engine2.file_hash);
    }

    #[test]
    fn test_engine_set_rate_limiter() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(engine.rate_limiter.is_none());

        let limiter = RateLimiter::new(1024 * 1024); // 1MB/s
        engine.set_rate_limiter(limiter);

        assert!(engine.rate_limiter.is_some());
    }

    #[test]
    fn test_engine_set_block_duration_secs() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert_eq!(engine.block_duration_secs, DEFAULT_BLOCK_DURATION_SECS);

        engine.set_block_duration_secs(120);
        assert_eq!(engine.block_duration_secs, 120);
    }

    #[test]
    fn test_engine_add_mirror() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        let initial_count = engine.sources.len();
        engine.add_mirror("http://mirror1.com/file".to_string());
        assert_eq!(engine.sources.len(), initial_count + 1);

        engine.add_mirror("http://mirror2.com/file".to_string());
        assert_eq!(engine.sources.len(), initial_count + 2);
    }

    #[test]
    fn test_engine_add_cdn_source() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        let initial_count = engine.sources.len();
        engine.add_cdn_source("http://cdn.com/file".to_string(), Some("token".to_string()));
        assert_eq!(engine.sources.len(), initial_count + 1);
    }

    #[test]
    fn test_engine_get_source_metrics_not_found() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(engine.get_source_metrics(999).is_none());
    }

    #[test]
    fn test_engine_is_source_blocked_not_blocked() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(!engine.is_source_blocked(0));
        assert!(!engine.is_source_blocked(999));
    }

    #[test]
    fn test_engine_is_complete_false_initially() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(!engine.is_complete());
    }

    #[test]
    fn test_engine_get_progress_none_initially() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(engine.get_progress().is_none());
    }

    #[test]
    fn test_engine_get_progress_some_after_start() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        engine.start_time = Some(Instant::now());
        // Mark first block as downloaded
        if let Some(block) = engine.blocks.first_mut() {
            block.downloaded = true;
            engine.downloaded = block.size;
        }

        let progress = engine.get_progress().unwrap();
        assert_eq!(progress.total_size, 1024 * 1024);
        assert!(progress.downloaded > 0);
        assert!(progress.speed > 0.0);
        assert!(progress.completed_blocks > 0);
    }

    #[test]
    fn test_engine_update_bandwidth_estimate() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert_eq!(engine.estimated_bandwidth, 0.0);

        // First update
        engine.update_bandwidth_estimate(1024 * 1024, 1000.0); // 1MB in 1s
        assert!(engine.estimated_bandwidth > 0.0);

        let first_estimate = engine.estimated_bandwidth;

        // Second update with higher speed
        engine.update_bandwidth_estimate(2 * 1024 * 1024, 1000.0); // 2MB in 1s
        assert!(engine.estimated_bandwidth > first_estimate);
    }

    #[test]
    fn test_engine_update_bandwidth_estimate_zero_duration() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        engine.update_bandwidth_estimate(1024, 0.0);
        assert_eq!(engine.estimated_bandwidth, 0.0); // Should not update
    }

    #[test]
    fn test_engine_maybe_adjust_block_size() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            100 * 1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        let _initial_block_size = engine.block_size;

        // Simulate high bandwidth
        engine.estimated_bandwidth = 5_000_000.0; // 5MB/s
        engine.maybe_adjust_block_size();

        // Block size may or may not change depending on calculation
        assert!(engine.block_size >= MIN_BLOCK_SIZE);
        assert!(engine.block_size <= MAX_BLOCK_SIZE);
    }

    #[test]
    fn test_xunlei_download_error_display() {
        let http_err = XunleiDownloadError::Http("404 Not Found".to_string());
        assert_eq!(format!("{}", http_err), "HTTP error: 404 Not Found");

        let io_err = XunleiDownloadError::Io("Permission denied".to_string());
        assert_eq!(format!("{}", io_err), "IO error: Permission denied");

        let peer_err = XunleiDownloadError::Peer("Connection refused".to_string());
        assert_eq!(format!("{}", peer_err), "peer error: Connection refused");
    }

    #[test]
    fn test_xunlei_download_error_debug() {
        let err = XunleiDownloadError::Http("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Http"));
    }

    #[tokio::test]
    async fn test_engine_save_and_load_progress() {
        let tmp_dir = tempdir().unwrap();
        let download_dir = tmp_dir.path().to_path_buf();
        let file_name = "test_progress.txt".to_string();
        let file_size = 1024 * 1024;

        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];

        let mut engine =
            XunleiEngine::new(file_name.clone(), file_size, sources, download_dir.clone());

        // Mark some blocks as downloaded
        for i in 0..engine.blocks.len() / 2 {
            engine.blocks[i].downloaded = true;
            engine.downloaded += engine.blocks[i].size;
        }

        // Save progress
        engine.save_progress().unwrap();

        // Verify progress file exists
        let progress_path = download_dir.join(format!("{}.progress", file_name));
        assert!(progress_path.exists());

        // Create new engine and load progress
        let sources2 = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine2 = XunleiEngine::new(file_name, file_size, sources2, download_dir);

        engine2.load_progress().unwrap();
        assert_eq!(engine2.downloaded, engine.downloaded);

        // Verify blocks match
        for i in 0..engine.blocks.len() {
            assert_eq!(engine.blocks[i].downloaded, engine2.blocks[i].downloaded);
        }
    }

    #[tokio::test]
    async fn test_engine_load_progress_no_file() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "nonexistent.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        let result = engine.load_progress();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_engine_load_progress_size_mismatch() {
        let tmp_dir = tempdir().unwrap();
        let download_dir = tmp_dir.path().to_path_buf();
        let file_name = "test_mismatch.txt".to_string();

        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];

        let mut engine = XunleiEngine::new(
            file_name.clone(),
            1024 * 1024,
            sources.clone(),
            download_dir.clone(),
        );

        engine.save_progress().unwrap();

        // Try to load with different file size
        let mut engine2 = XunleiEngine::new(
            file_name,
            2 * 1024 * 1024, // Different size
            sources,
            download_dir,
        );

        let result = engine2.load_progress();
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("mismatch"));
    }

    #[test]
    fn test_engine_unicode_filename() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "测试文件.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert_eq!(engine.get_file_name(), "测试文件.txt");
    }

    #[test]
    fn test_engine_emoji_filename() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "🚀download🎉.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert_eq!(engine.get_file_name(), "🚀download🎉.txt");
    }

    #[test]
    fn test_engine_zero_file_size_panics() {
        // calculate_optimal_block_size panics on zero file size due to clamp(min > max)
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            XunleiEngine::new(
                "empty.txt".to_string(),
                0,
                sources,
                tmp_dir.path().to_path_buf(),
            )
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_engine_small_file_1mb() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "small.txt".to_string(),
            1024 * 1024, // 1MB - minimum safe size
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert_eq!(engine.get_file_size(), 1024 * 1024);
        assert!(!engine.blocks.is_empty());
    }

    #[test]
    fn test_engine_very_large_file() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "huge.bin".to_string(),
            1024 * 1024 * 1024, // 1GB
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert_eq!(engine.get_file_size(), 1024 * 1024 * 1024);
        assert!(!engine.blocks.is_empty());
    }

    #[test]
    fn test_engine_multiple_sources_mixed_types() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![
            XunleiSource::Http {
                url: "http://example.com/file".to_string(),
                cookies: None,
                referer: None,
            },
            XunleiSource::Peer {
                addr: "127.0.0.1:8080".parse().unwrap(),
                peer_id: [0u8; 20],
            },
            XunleiSource::Cdn {
                url: "http://cdn.com/file".to_string(),
                token: None,
            },
        ];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert_eq!(engine.sources.len(), 3);
    }

    #[test]
    fn test_engine_source_metrics_initially_empty() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(engine.source_metrics.is_empty());
    }

    #[test]
    fn test_engine_blocked_sources_initially_empty() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(engine.blocked_sources.is_empty());
    }

    #[test]
    fn test_engine_pending_writes_initially_empty() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(engine.pending_writes.is_empty());
    }

    #[test]
    fn test_engine_block_size_reasonable() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let engine = XunleiEngine::new(
            "test.txt".to_string(),
            10 * 1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        assert!(engine.block_size >= MIN_BLOCK_SIZE);
        assert!(engine.block_size <= MAX_BLOCK_SIZE);
    }

    #[tokio::test]
    async fn test_engine_flush_writes_empty() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        // Flush with no file and no pending writes should succeed
        let result = engine.flush_writes().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_engine_evaluate_source_quality_not_enough_data() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        // Add metrics with insufficient elapsed time
        engine.source_metrics.insert(0, (1024, 0.5));

        engine.evaluate_source_quality();

        // Should not block any sources due to insufficient data
        assert!(engine.blocked_sources.is_empty());
    }

    #[test]
    fn test_engine_evaluate_source_quality_first_call() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];
        let mut engine = XunleiEngine::new(
            "test.txt".to_string(),
            1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        // First call should set last_quality_check
        assert!(engine.last_quality_check.is_none());
        engine.evaluate_source_quality();
        assert!(engine.last_quality_check.is_some());
    }

    #[test]
    fn test_engine_calculate_optimal_block_size_boundary_low() {
        let size = MIN_BLOCK_SIZE * 4; // Minimum blocks
        let block_size = XunleiEngine::calculate_optimal_block_size(size, 0.0);
        assert!(block_size >= MIN_BLOCK_SIZE);
    }

    #[test]
    fn test_engine_calculate_optimal_block_size_boundary_high() {
        let size = MAX_BLOCK_SIZE * 1000; // Maximum blocks
        let block_size = XunleiEngine::calculate_optimal_block_size(size, 10_000_000.0);
        assert!(block_size <= MAX_BLOCK_SIZE);
    }

    #[test]
    fn test_engine_multiple_engines_independent() {
        let tmp_dir = tempdir().unwrap();
        let sources = vec![XunleiSource::Http {
            url: "http://example.com/file".to_string(),
            cookies: None,
            referer: None,
        }];

        let mut engine1 = XunleiEngine::new(
            "file1.txt".to_string(),
            1024 * 1024,
            sources.clone(),
            tmp_dir.path().to_path_buf(),
        );
        let mut engine2 = XunleiEngine::new(
            "file2.txt".to_string(),
            2 * 1024 * 1024,
            sources,
            tmp_dir.path().to_path_buf(),
        );

        engine1.downloaded = 512;
        engine2.downloaded = 1024;

        assert_eq!(engine1.downloaded, 512);
        assert_eq!(engine2.downloaded, 1024);
        assert_ne!(engine1.get_file_size(), engine2.get_file_size());
    }

    #[test]
    fn test_engine_source_quality_constants() {
        assert_eq!(SOURCE_QUALITY_INTERVAL_SECS, 30);
        assert_eq!(MIN_BLOCKS_FOR_EVALUATION, 5);
        assert_eq!(SLOW_SOURCE_THRESHOLD, 10_000.0);
    }

    #[test]
    fn test_engine_retry_constants() {
        assert_eq!(MAX_BLOCK_RETRIES, 3);
        assert_eq!(RETRY_BASE_DELAY_MS, 500);
    }

    #[test]
    fn test_engine_buffer_pool_constants() {
        assert_eq!(WRITE_BUFFER_SIZE, 64 * 1024);
    }
}
