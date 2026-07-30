//! Xunlei P2SP download engine

use super::protocol::{DownloadProgress, P2spBlock, XunleiSource};
use reqwest::Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, interval};

/// P2SP block size (1MB)
const BLOCK_SIZE: u64 = 1024 * 1024;

/// Xunlei P2SP download engine
pub struct XunleiEngine {
    file_name: String,
    file_size: u64,
    sources: Vec<XunleiSource>,
    blocks: Vec<P2spBlock>,
    download_dir: PathBuf,
    http_client: Client,
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
        let mut block_idx = 0;

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
            block_idx += 1;
        }

        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            file_name,
            file_size,
            sources,
            blocks,
            download_dir,
            http_client,
            downloaded: 0,
            start_time: None,
        }
    }

    /// Start the download process
    pub async fn download(&mut self) -> Result<(), XunleiDownloadError> {
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
                            Self::download_http_block(client, url, offset, size).await
                        });
                        tasks.push((block_idx, task));
                    }
                    XunleiSource::Cdn { url, .. } => {
                        let client = self.http_client.clone();
                        let url = url.clone();
                        let task = tokio::spawn(async move {
                            Self::download_http_block(client, url, offset, size).await
                        });
                        tasks.push((block_idx, task));
                    }
                    XunleiSource::Peer { .. } => {
                        // TODO: Implement P2P peer download
                        tracing::debug!("P2P peer download not yet implemented");
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

    fn is_complete(&self) -> bool {
        self.blocks.iter().all(|b| b.downloaded)
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
