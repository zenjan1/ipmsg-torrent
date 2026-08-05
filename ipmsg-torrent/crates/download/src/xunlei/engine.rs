//! Xunlei P2SP download engine

use super::peer::PeerClient;
use super::protocol::{DownloadProgress, P2spBlock, XunleiSource};
use reqwest::Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;

/// P2SP block size (1MB)
const BLOCK_SIZE: u64 = 1024 * 1024;

/// Max retries per block before giving up
const MAX_BLOCK_RETRIES: u32 = 3;

/// Retry delay base (doubles each attempt)
const RETRY_BASE_DELAY_MS: u64 = 500;

/// Xunlei P2SP download engine
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
    start_time: Option<std::time::Instant>,
}

impl XunleiEngine {
    pub fn new(
        file_name: String,
        file_size: u64,
        sources: Vec<XunleiSource>,
        download_dir: PathBuf,
    ) -> Self {
        // Initialize blocks
        let mut blocks = Vec::new();
        let mut offset = 0u64;

        while offset < file_size {
            let size = std::cmp::min(BLOCK_SIZE, file_size - offset);
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

        let http_client = Client::builder()
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
        };

        // Try to load existing progress
        if let Err(e) = engine.load_progress() {
            tracing::debug!("No existing progress to load: {}", e);
        }

        engine
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
            "Starting P2SP download"
        );

        self.start_time = Some(std::time::Instant::now());

        // Main download loop
        let mut tick = interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    // Check if cancelled
                    if let Some(ref cancel) = cancel
                        && cancel.is_cancelled() {
                            tracing::info!("Download cancelled");
                            return Err(XunleiDownloadError::Io("cancelled".to_string()));
                        }

                    // Check if download is complete
                    if self.is_complete() {
                        tracing::info!("Download complete!");
                        break;
                    }

                    // Download blocks from sources
                    self.download_blocks().await;

                    // Log progress
                    if let Some(progress) = self.get_progress() {
                        tracing::debug!(
                            downloaded = progress.downloaded,
                            total = progress.total_size,
                            speed = format!("{:.2} KB/s", progress.speed / 1024.0),
                            sources = progress.sources_count,
                            "Download progress"
                        );
                    }
                }
            }
        }

        // Save file
        self.save_file().await?;

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

        // Wait for tasks to complete
        for (block_idx, task) in tasks {
            match task.await {
                Ok(Ok(data)) => {
                    if let Some(block) = self.blocks.get_mut(block_idx) {
                        block.downloaded = true;
                        block.data = Some(data.clone());
                        self.downloaded += data.len() as u64;
                        tracing::debug!(
                            block = block_idx,
                            offset = block.offset,
                            size = data.len(),
                            "Block downloaded"
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

    async fn save_file(&self) -> Result<(), XunleiDownloadError> {
        let mut file_data = Vec::with_capacity(self.file_size as usize);

        // Assemble blocks in order
        for block in &self.blocks {
            if let Some(data) = &block.data {
                file_data.extend_from_slice(data);
            } else {
                return Err(XunleiDownloadError::Io("missing block data".to_string()));
            }
        }

        // Write to file
        let output_path = self.download_dir.join(&self.file_name);
        tokio::fs::create_dir_all(&self.download_dir)
            .await
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        tokio::fs::write(&output_path, &file_data)
            .await
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        tracing::info!(path = %output_path.display(), "File saved");

        // Save progress
        self.save_progress()?;

        Ok(())
    }

    /// Save download progress to disk
    fn save_progress(&self) -> Result<(), XunleiDownloadError> {
        let progress_path = self.download_dir.join(format!("{}.progress", self.file_name));
        
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
                serde_cbor::Value::Integer(BLOCK_SIZE as i128),
            );
            map.insert(
                serde_cbor::Value::Text("bitmap".to_string()),
                serde_cbor::Value::Array(bitmap.into_iter().map(|b| serde_cbor::Value::Integer(b as i128)).collect()),
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
        let progress_path = self.download_dir.join(format!("{}.progress", self.file_name));

        if !progress_path.exists() {
            return Err(XunleiDownloadError::Io("No progress file found".to_string()));
        }

        let progress_bytes = std::fs::read(&progress_path)
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        let progress_data: serde_cbor::Value = serde_cbor::from_slice(&progress_bytes)
            .map_err(|e| XunleiDownloadError::Io(e.to_string()))?;

        // Verify file size matches
        if let serde_cbor::Value::Map(map) = &progress_data {
            if let Some(serde_cbor::Value::Integer(saved_size)) = map.get(&serde_cbor::Value::Text("file_size".to_string())) {
                if *saved_size as u64 != self.file_size {
                    return Err(XunleiDownloadError::Io("File size mismatch".to_string()));
                }
            } else {
                return Err(XunleiDownloadError::Io("Invalid file_size in progress".to_string()));
            }

            // Restore block bitmap
            if let Some(serde_cbor::Value::Array(bitmap)) = map.get(&serde_cbor::Value::Text("bitmap".to_string())) {
                for (i, block) in self.blocks.iter_mut().enumerate() {
                    if let Some(serde_cbor::Value::Integer(downloaded)) = bitmap.get(i) {
                        block.downloaded = *downloaded == 1;
                    }
                }
            }

            // Restore downloaded count
            if let Some(serde_cbor::Value::Integer(downloaded)) = map.get(&serde_cbor::Value::Text("downloaded".to_string())) {
                self.downloaded = *downloaded as u64;
            }
        } else {
            return Err(XunleiDownloadError::Io("Invalid progress format".to_string()));
        }

        tracing::info!(
            file = %self.file_name,
            downloaded = self.downloaded,
            total = self.file_size,
            "Progress restored"
        );

        Ok(())
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
