//! File transfer protocol handler using libp2p request-response
//! Implements torrent-style chunked file transfer

use crate::file_sharing::FileSharingManager;
use crate::p2p_progress::{self, P2pDownloadSnapshot};
use ipmsg_protocol::message::FileRef;
use libp2p::StreamProtocol;
use libp2p::request_response::{self, Codec, ProtocolSupport};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// File transfer protocol name
pub const FILE_TRANSFER_PROTOCOL: &str = "/ipmsg/file-transfer/1.0.0";

/// Codec for file transfer messages
#[derive(Clone, Default)]
pub struct FileTransferCodec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileTransferRequest {
    /// Request file metadata
    GetInfo { file_hash: String },
    /// Request a specific chunk
    GetChunk { file_hash: String, chunk_index: u32 },
    /// Request multiple chunks (batch)
    GetChunks {
        file_hash: String,
        chunk_indices: Vec<u32>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileTransferResponse {
    /// File metadata response
    Info { file_ref: FileRef, available: bool },
    /// Single chunk data response
    Chunk {
        file_hash: String,
        chunk_index: u32,
        data: Vec<u8>,
    },
    /// Multiple chunks response (from batch GetChunks request)
    Chunks {
        file_hash: String,
        /// Each entry is (chunk_index, chunk_data)
        chunks: Vec<(u32, Vec<u8>)>,
    },
    /// Error response
    Error { message: String },
}

impl Codec for FileTransferCodec {
    type Protocol = StreamProtocol;
    type Request = FileTransferRequest;
    type Response = FileTransferResponse;

    fn read_request<'life0, 'life1, 'life2, 'async_trait, T>(
        &'life0 mut self,
        _protocol: &'life1 Self::Protocol,
        io: &'life2 mut T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<Self::Request>> + Send + 'async_trait>,
    >
    where
        T: futures::io::AsyncRead + Unpin + Send,
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
    {
        Box::pin(async move {
            use futures::AsyncReadExt;
            let mut buf = Vec::new();
            io.read_to_end(&mut buf).await?;
            serde_cbor::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })
    }

    fn read_response<'life0, 'life1, 'life2, 'async_trait, T>(
        &'life0 mut self,
        _protocol: &'life1 Self::Protocol,
        io: &'life2 mut T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<Self::Response>> + Send + 'async_trait>,
    >
    where
        T: futures::io::AsyncRead + Unpin + Send,
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
    {
        Box::pin(async move {
            use futures::AsyncReadExt;
            let mut buf = Vec::new();
            io.read_to_end(&mut buf).await?;
            serde_cbor::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })
    }

    fn write_request<'life0, 'life1, 'life2, 'async_trait, T>(
        &'life0 mut self,
        _protocol: &'life1 Self::Protocol,
        io: &'life2 mut T,
        req: Self::Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'async_trait>>
    where
        T: futures::io::AsyncWrite + Unpin + Send,
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
    {
        Box::pin(async move {
            use futures::AsyncWriteExt;
            let data = serde_cbor::to_vec(&req)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            io.write_all(&data).await?;
            io.close().await?;
            Ok(())
        })
    }

    fn write_response<'life0, 'life1, 'life2, 'async_trait, T>(
        &'life0 mut self,
        _protocol: &'life1 Self::Protocol,
        io: &'life2 mut T,
        resp: Self::Response,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'async_trait>>
    where
        T: futures::io::AsyncWrite + Unpin + Send,
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
    {
        Box::pin(async move {
            use futures::AsyncWriteExt;
            let data = serde_cbor::to_vec(&resp)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            io.write_all(&data).await?;
            io.close().await?;
            Ok(())
        })
    }
}

/// Tracks an ongoing file download
#[derive(Debug, Clone)]
pub struct FileDownload {
    pub file_ref: FileRef,
    pub received_chunks: HashMap<u32, Vec<u8>>,
    pub missing_chunks: Vec<u32>,
    pub owner: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// Bytes received so far (for progress tracking)
    pub bytes_received: u64,
    /// Whether this download can be resumed across restarts
    pub resumable: bool,
    /// Last activity timestamp (for timeout detection)
    pub last_activity: chrono::DateTime<chrono::Utc>,
}

impl FileDownload {
    pub fn new(file_ref: FileRef, owner: String) -> Self {
        let missing_chunks = (0..file_ref.chunks).collect();
        let now = chrono::Utc::now();
        Self {
            file_ref,
            received_chunks: HashMap::new(),
            missing_chunks,
            owner,
            started_at: now,
            bytes_received: 0,
            resumable: true,
            last_activity: now,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.missing_chunks.is_empty()
    }

    pub fn progress(&self) -> f32 {
        let total = self.file_ref.chunks as f32;
        let received = (total - self.missing_chunks.len() as f32) / total;
        received * 100.0
    }

    pub fn bytes_total(&self) -> u64 {
        self.file_ref.size
    }

    pub fn bytes_downloaded(&self) -> u64 {
        self.bytes_received
    }

    pub fn download_speed_bps(&self) -> f64 {
        let elapsed = chrono::Utc::now()
            .signed_duration_since(self.started_at)
            .num_seconds() as f64;
        if elapsed > 0.0 {
            self.bytes_received as f64 / elapsed
        } else {
            0.0
        }
    }

    pub fn reassemble(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }
        let mut data = Vec::with_capacity(self.file_ref.size as usize);
        for i in 0..self.file_ref.chunks {
            {
                let chunk = self.received_chunks.get(&i)?;
                data.extend_from_slice(chunk);
            }
        }
        Some(data)
    }

    /// Get the next batch of missing chunk indices for parallel downloading
    pub fn next_missing_chunks(&self, batch_size: usize) -> Vec<u32> {
        self.missing_chunks
            .iter()
            .take(batch_size)
            .copied()
            .collect()
    }

    /// Check if download is stalled (no activity for given duration)
    pub fn is_stalled(&self, timeout: std::time::Duration) -> bool {
        chrono::Utc::now()
            .signed_duration_since(self.last_activity)
            .to_std()
            .unwrap_or_default()
            > timeout
    }

    /// Convert current download state to a snapshot for persistence
    pub fn to_snapshot(&self) -> P2pDownloadSnapshot {
        let mut snapshot = P2pDownloadSnapshot::new(
            self.file_ref.hash.clone(),
            self.file_ref.name.clone(),
            self.file_ref.size,
            self.file_ref.chunk_size,
            self.file_ref.chunks,
            self.owner.clone(),
        );
        snapshot.received_chunks = self.received_chunks.keys().copied().collect();
        snapshot.bytes_received = self.bytes_received;
        snapshot.last_activity = self.last_activity;
        snapshot
    }

    /// Restore download state from a persisted snapshot
    /// Chunk data is loaded from disk files
    pub fn from_snapshot(
        file_ref: FileRef,
        owner: String,
        snapshot: P2pDownloadSnapshot,
        progress_dir: &std::path::Path,
    ) -> Self {
        // Load chunk data from persisted files
        let mut received_chunks: HashMap<u32, Vec<u8>> = HashMap::new();
        for &chunk_idx in &snapshot.received_chunks {
            let chunk_path = chunk_data_path(progress_dir, &snapshot.file_hash, chunk_idx);
            if let Ok(data) = std::fs::read(&chunk_path) {
                received_chunks.insert(chunk_idx, data);
            } else {
                tracing::warn!(
                    hash = %snapshot.file_hash,
                    chunk = %chunk_idx,
                    "Chunk data file missing, will re-download"
                );
            }
        }

        let received_indices: std::collections::HashSet<u32> =
            received_chunks.keys().copied().collect();
        let missing_chunks: Vec<u32> = (0..file_ref.chunks)
            .filter(|i| !received_indices.contains(i))
            .collect();

        Self {
            file_ref,
            received_chunks,
            missing_chunks,
            owner,
            started_at: chrono::Utc::now(),
            bytes_received: snapshot.bytes_received,
            resumable: true,
            last_activity: snapshot.last_activity,
        }
    }
}

/// Get the file path for a specific chunk's persisted data
fn chunk_data_path(
    progress_dir: &std::path::Path,
    file_hash: &str,
    chunk_index: u32,
) -> std::path::PathBuf {
    let safe_hash: String = file_hash
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    progress_dir
        .join("chunks")
        .join(format!("{}_{}", safe_hash, chunk_index))
}

/// File transfer manager handles download/upload state
pub struct FileTransferManager {
    /// Active downloads (file_hash -> FileDownload)
    downloads: Arc<Mutex<HashMap<String, FileDownload>>>,
    /// File sharing manager for serving requests
    file_sharing: Arc<Mutex<FileSharingManager>>,
    /// Pending requests to send
    pending_requests: Arc<Mutex<Vec<(String, FileTransferRequest)>>>,
    /// Directory for persisting download progress
    progress_dir: PathBuf,
}

impl FileTransferManager {
    pub fn new(file_sharing: Arc<Mutex<FileSharingManager>>, progress_dir: PathBuf) -> Self {
        Self {
            downloads: Arc::new(Mutex::new(HashMap::new())),
            file_sharing,
            pending_requests: Arc::new(Mutex::new(Vec::new())),
            progress_dir,
        }
    }

    /// Start downloading a file from a peer
    /// Tries to restore progress from disk first (断点续传)
    pub async fn start_download(&self, file_ref: FileRef, owner: String) -> String {
        let file_hash = file_ref.hash.clone();

        // Try to restore from persisted progress
        let download = match p2p_progress::load_progress(&self.progress_dir, &file_hash) {
            Ok(snapshot) => {
                tracing::info!(
                    hash = %file_hash,
                    progress = %format!("{:.1}%", snapshot.progress()),
                    "Restored P2P download progress"
                );
                FileDownload::from_snapshot(file_ref, owner, snapshot, &self.progress_dir)
            }
            Err(_) => {
                // No saved progress, start fresh
                FileDownload::new(file_ref, owner)
            }
        };

        let mut downloads = self.downloads.lock().await;
        downloads.insert(file_hash.clone(), download);
        file_hash
    }

    /// Record a received chunk with progress tracking and persistence
    pub async fn record_chunk(&self, file_hash: &str, chunk_index: u32, data: Vec<u8>) -> bool {
        let mut downloads = self.downloads.lock().await;
        if let Some(download) = downloads.get_mut(file_hash) {
            let chunk_size = data.len() as u64;

            // Persist chunk data to disk for resume support
            let chunk_path = chunk_data_path(&self.progress_dir, file_hash, chunk_index);
            if let Some(parent) = chunk_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = std::fs::write(&chunk_path, &data) {
                tracing::warn!(
                    hash = %file_hash,
                    chunk = %chunk_index,
                    error = %e,
                    "Failed to persist chunk data"
                );
            }

            download.received_chunks.insert(chunk_index, data);
            download.missing_chunks.retain(|&i| i != chunk_index);
            download.bytes_received += chunk_size;
            download.last_activity = chrono::Utc::now();

            // Log progress
            let progress = download.progress();
            let speed = download.download_speed_bps();
            tracing::info!(
                file_hash = %file_hash,
                chunk = %chunk_index,
                progress = %format!("{:.1}%", progress),
                speed = %format!("{:.1} KB/s", speed / 1024.0),
                "Chunk received"
            );

            let is_complete = download.is_complete();

            // Persist progress metadata to disk (even if complete, so we can track it)
            let snapshot = download.to_snapshot();
            drop(downloads); // Release lock before I/O
            if let Err(e) = p2p_progress::save_progress(&self.progress_dir, &snapshot) {
                tracing::warn!(hash = %file_hash, error = %e, "Failed to save P2P progress");
            }

            return is_complete;
        }
        false
    }

    /// Resume a previously started download (断点续传)
    pub async fn resume_download(&self, file_hash: &str) -> Option<Vec<u32>> {
        let downloads = self.downloads.lock().await;
        if let Some(download) = downloads.get(file_hash) {
            if !download.is_complete() && download.resumable {
                // Return missing chunks for re-request
                Some(download.next_missing_chunks(10))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get missing chunks for a download
    pub async fn get_missing_chunks(&self, file_hash: &str, batch_size: usize) -> Vec<u32> {
        let downloads = self.downloads.lock().await;
        if let Some(download) = downloads.get(file_hash) {
            download
                .missing_chunks
                .iter()
                .take(batch_size)
                .copied()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get download progress
    pub async fn get_progress(&self, file_hash: &str) -> Option<f32> {
        let downloads = self.downloads.lock().await;
        downloads.get(file_hash).map(|d| d.progress())
    }

    /// Check if download is complete and get assembled data
    pub async fn try_assemble(&self, file_hash: &str) -> Option<Vec<u8>> {
        let downloads = self.downloads.lock().await;
        if let Some(download) = downloads.get(file_hash)
            && download.is_complete()
        {
            return download.reassemble();
        }
        None
    }

    /// Remove a completed download and clean up progress file
    pub async fn finish_download(&self, file_hash: &str) -> Option<FileDownload> {
        let mut downloads = self.downloads.lock().await;
        let result = downloads.remove(file_hash);
        // Clean up progress file for completed download
        if result.is_some()
            && let Err(e) = p2p_progress::remove_progress(&self.progress_dir, file_hash)
        {
            tracing::warn!(hash = %file_hash, error = %e, "Failed to remove P2P progress file");
        }
        result
    }

    /// Handle an incoming file transfer request
    pub async fn handle_request(&self, req: FileTransferRequest) -> FileTransferResponse {
        let file_sharing = self.file_sharing.lock().await;

        match req {
            FileTransferRequest::GetInfo { file_hash } => {
                if let Some(info) = file_sharing.get_shared_file(&file_hash).await {
                    FileTransferResponse::Info {
                        file_ref: info.file_ref,
                        available: true,
                    }
                } else {
                    FileTransferResponse::Info {
                        file_ref: FileRef {
                            hash: file_hash,
                            name: String::new(),
                            size: 0,
                            mime_type: String::new(),
                            chunks: 0,
                            chunk_size: 0,
                            thumbnail: None,
                        },
                        available: false,
                    }
                }
            }
            FileTransferRequest::GetChunk {
                file_hash,
                chunk_index,
            } => match file_sharing.read_chunk(&file_hash, chunk_index).await {
                Ok(data) => FileTransferResponse::Chunk {
                    file_hash,
                    chunk_index,
                    data,
                },
                Err(e) => FileTransferResponse::Error {
                    message: format!("Failed to read chunk: {}", e),
                },
            },
            FileTransferRequest::GetChunks {
                file_hash,
                chunk_indices,
            } => {
                // Batch request: return all requested chunks as structured data
                let mut chunks = Vec::with_capacity(chunk_indices.len());
                let mut errors = Vec::new();

                for &chunk_index in &chunk_indices {
                    match file_sharing.read_chunk(&file_hash, chunk_index).await {
                        Ok(data) => {
                            chunks.push((chunk_index, data));
                        }
                        Err(e) => {
                            errors.push(format!("chunk {}: {}", chunk_index, e));
                        }
                    }
                }

                if chunks.is_empty() && !errors.is_empty() {
                    FileTransferResponse::Error {
                        message: format!("Failed to read chunks: {}", errors.join("; ")),
                    }
                } else {
                    FileTransferResponse::Chunks { file_hash, chunks }
                }
            }
        }
    }

    /// Queue a request to be sent
    pub async fn queue_request(&self, peer_id: String, req: FileTransferRequest) {
        let mut pending = self.pending_requests.lock().await;
        pending.push((peer_id, req));
    }

    /// Get and clear pending requests
    pub async fn take_pending_requests(&self) -> Vec<(String, FileTransferRequest)> {
        let mut pending = self.pending_requests.lock().await;
        std::mem::take(&mut *pending)
    }
}

/// Create request-response behaviour for file transfer
pub fn create_file_transfer_behaviour() -> request_response::Behaviour<FileTransferCodec> {
    request_response::Behaviour::new(
        [(
            StreamProtocol::new(FILE_TRANSFER_PROTOCOL),
            ProtocolSupport::Full,
        )],
        request_response::Config::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ipmsg_protocol::message::FileRef;
    use tempfile::TempDir;

    #[test]
    fn test_file_download_progress() {
        let file_ref = FileRef {
            hash: "test".to_string(),
            name: "test.txt".to_string(),
            size: 1000,
            mime_type: "text/plain".to_string(),
            chunks: 4,
            chunk_size: 250,
            thumbnail: None,
        };
        let mut download = FileDownload::new(file_ref, "peer1".to_string());

        assert_eq!(download.progress(), 0.0);
        assert!(!download.is_complete());

        download.received_chunks.insert(0, vec![0; 250]);
        download.missing_chunks.retain(|&i| i != 0);
        assert_eq!(download.progress(), 25.0);

        download.received_chunks.insert(1, vec![0; 250]);
        download.missing_chunks.retain(|&i| i != 1);
        download.received_chunks.insert(2, vec![0; 250]);
        download.missing_chunks.retain(|&i| i != 2);
        download.received_chunks.insert(3, vec![0; 250]);
        download.missing_chunks.retain(|&i| i != 3);

        assert_eq!(download.progress(), 100.0);
        assert!(download.is_complete());
        assert!(download.reassemble().is_some());
    }

    #[test]
    fn test_file_download_next_missing_chunks() {
        let file_ref = FileRef {
            hash: "test".to_string(),
            name: "test.txt".to_string(),
            size: 1000,
            mime_type: "text/plain".to_string(),
            chunks: 4,
            chunk_size: 250,
            thumbnail: None,
        };
        let mut download = FileDownload::new(file_ref, "peer1".to_string());

        // Initially all chunks are missing
        let batch = download.next_missing_chunks(2);
        assert_eq!(batch, vec![0, 1]);

        // Receive chunk 0
        download.received_chunks.insert(0, vec![0; 250]);
        download.missing_chunks.retain(|&i| i != 0);

        // Next batch should skip chunk 0
        let batch = download.next_missing_chunks(2);
        assert_eq!(batch, vec![1, 2]);

        // Request more than available
        download.received_chunks.insert(1, vec![0; 250]);
        download.missing_chunks.retain(|&i| i != 1);
        download.received_chunks.insert(2, vec![0; 250]);
        download.missing_chunks.retain(|&i| i != 2);

        let batch = download.next_missing_chunks(10);
        assert_eq!(batch, vec![3]);
    }

    #[test]
    fn test_file_download_to_snapshot() {
        let file_ref = FileRef {
            hash: "hash123".to_string(),
            name: "video.mp4".to_string(),
            size: 1024,
            mime_type: "video/mp4".to_string(),
            chunks: 4,
            chunk_size: 256,
            thumbnail: None,
        };
        let mut download = FileDownload::new(file_ref, "peer_abc".to_string());

        // Receive some chunks
        download.received_chunks.insert(0, vec![0xAA; 256]);
        download.missing_chunks.retain(|&i| i != 0);
        download.bytes_received += 256;
        download.received_chunks.insert(2, vec![0xCC; 256]);
        download.missing_chunks.retain(|&i| i != 2);
        download.bytes_received += 256;

        let snapshot = download.to_snapshot();
        assert_eq!(snapshot.file_hash, "hash123");
        assert_eq!(snapshot.file_name, "video.mp4");
        assert_eq!(snapshot.file_size, 1024);
        assert_eq!(snapshot.chunk_size, 256);
        assert_eq!(snapshot.total_chunks, 4);
        assert_eq!(snapshot.received_chunks.len(), 2);
        assert!(snapshot.received_chunks.contains(&0));
        assert!(snapshot.received_chunks.contains(&2));
        assert_eq!(snapshot.bytes_received, 512);
        assert_eq!(snapshot.owner, "peer_abc");
    }

    #[tokio::test]
    async fn test_file_transfer_manager_batch_record() {
        let temp_dir = TempDir::new().unwrap();
        let progress_dir = temp_dir.path().join("p2p_progress");
        let sharing_dir = temp_dir.path().join("shared");

        let file_ref = FileRef {
            hash: "batch_test".to_string(),
            name: "batch.bin".to_string(),
            size: 1024,
            mime_type: "application/octet-stream".to_string(),
            chunks: 4,
            chunk_size: 256,
            thumbnail: None,
        };
        let file_hash = file_ref.hash.clone();

        let sharing = Arc::new(Mutex::new(FileSharingManager::new(sharing_dir)));
        let manager = FileTransferManager::new(sharing, progress_dir);

        manager
            .start_download(file_ref.clone(), "peer1".to_string())
            .await;

        // Record chunks in batch (simulating batch response)
        let chunk0 = vec![0xAA; 256];
        let chunk1 = vec![0xBB; 256];
        let complete = manager.record_chunk(&file_hash, 0, chunk0).await;
        assert!(!complete);
        let complete = manager.record_chunk(&file_hash, 1, chunk1).await;
        assert!(!complete);

        // Check progress
        let progress = manager.get_progress(&file_hash).await.unwrap();
        assert!(progress > 40.0 && progress < 60.0); // ~50%

        // Get missing chunks
        let missing = manager.get_missing_chunks(&file_hash, 10).await;
        assert_eq!(missing.len(), 2);
        assert!(missing.contains(&2));
        assert!(missing.contains(&3));
    }
}
