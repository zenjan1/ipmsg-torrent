use crate::{P2PError, P2PEvent};
use chrono::Utc;
use ipmsg_protocol::message::{FileRef, FileShareInfo};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// File sharing manager - tracks shared files and handles search queries
#[derive(Clone)]
pub struct FileSharingManager {
    /// Files we are sharing (hash -> FileShareInfo)
    shared_files: Arc<Mutex<HashMap<String, FileShareInfo>>>,
    /// Files discovered from other peers (hash -> FileShareInfo)
    discovered_files: Arc<Mutex<HashMap<String, FileShareInfo>>>,
    /// Directory for storing shared files
    files_dir: PathBuf,
    /// Event sender for notifying about discovered files
    event_tx: Option<tokio::sync::mpsc::UnboundedSender<P2PEvent>>,
}

impl FileSharingManager {
    pub fn new(files_dir: PathBuf) -> Self {
        Self {
            shared_files: Arc::new(Mutex::new(HashMap::new())),
            discovered_files: Arc::new(Mutex::new(HashMap::new())),
            files_dir,
            event_tx: None,
        }
    }

    pub fn set_event_sender(&mut self, tx: tokio::sync::mpsc::UnboundedSender<P2PEvent>) {
        self.event_tx = Some(tx);
    }

    /// Add a file to share
    pub async fn share_file(
        &self,
        path: &Path,
        tags: Vec<String>,
        description: Option<String>,
        owner: String,
    ) -> Result<FileShareInfo, P2PError> {
        let data = tokio::fs::read(path)
            .await
            .map_err(|e| P2PError::Transport(format!("Failed to read file: {}", e)))?;

        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let size = data.len() as u64;
        let hash = format!("{:x}", Sha256::digest(&data));
        let mime_type = mime_guess::from_path(path)
            .first_or_octet_stream()
            .to_string();

        let file_ref = FileRef::new(name, size, mime_type, &data);

        let info = FileShareInfo {
            file_ref,
            owner,
            tags,
            description,
            created_at: Utc::now(),
        };

        let mut shared = self.shared_files.lock().unwrap();
        shared.insert(hash.clone(), info.clone());

        tracing::info!(hash = %hash, name = %info.file_ref.name, "File shared");
        Ok(info)
    }

    /// Remove a file from sharing
    pub async fn unshare_file(&self, hash: &str) -> bool {
        let mut shared = self.shared_files.lock().unwrap();
        shared.remove(hash).is_some()
    }

    /// List files we are sharing
    pub async fn list_shared_files(&self) -> Vec<FileShareInfo> {
        let shared = self.shared_files.lock().unwrap();
        shared.values().cloned().collect()
    }

    /// Get a shared file by hash
    pub async fn get_shared_file(&self, hash: &str) -> Option<FileShareInfo> {
        let shared = self.shared_files.lock().unwrap();
        shared.get(hash).cloned()
    }

    /// Read file chunk by hash and index
    pub async fn read_chunk(&self, hash: &str, index: u32) -> Result<Vec<u8>, P2PError> {
        // Extract file info under lock, then release lock before async I/O
        let (file_path, start, end) = {
            let shared = self.shared_files.lock().unwrap();
            let info = shared
                .get(hash)
                .ok_or_else(|| P2PError::Transport(format!("File not found: {}", hash)))?;

            let start = (index as u64) * (info.file_ref.chunk_size as u64);
            let end = std::cmp::min(start + info.file_ref.chunk_size as u64, info.file_ref.size);
            let file_path = self.files_dir.join(&info.file_ref.name);
            (file_path, start, end)
        };

        // Read the actual file from disk (async, no lock held)
        let data = tokio::fs::read(&file_path)
            .await
            .map_err(|e| P2PError::Transport(format!("Failed to read file: {}", e)))?;

        let chunk = data[start as usize..end as usize].to_vec();
        Ok(chunk)
    }

    /// Process incoming file share announcements from other peers
    pub async fn process_announce(&self, shares: &[FileShareInfo]) {
        let mut discovered = self.discovered_files.lock().unwrap();
        for share in shares {
            discovered.insert(share.file_ref.hash.clone(), share.clone());
            tracing::info!(
                hash = %share.file_ref.hash,
                name = %share.file_ref.name,
                owner = %share.owner,
                "Discovered shared file"
            );
        }
    }

    /// Search for files (both shared and discovered)
    pub async fn search(&self, query: &str, tags: &[String]) -> Vec<FileShareInfo> {
        let mut results = Vec::new();
        let query_lower = query.to_lowercase();

        // Search shared files
        {
            let shared = self.shared_files.lock().unwrap();
            for info in shared.values() {
                if self.matches_query(info, &query_lower, tags) {
                    results.push(info.clone());
                }
            }
        }

        // Search discovered files
        {
            let discovered = self.discovered_files.lock().unwrap();
            for info in discovered.values() {
                if self.matches_query(info, &query_lower, tags) {
                    results.push(info.clone());
                }
            }
        }

        results
    }

    fn matches_query(&self, info: &FileShareInfo, query_lower: &str, tags: &[String]) -> bool {
        // Match by filename
        if info.file_ref.name.to_lowercase().contains(query_lower) {
            return true;
        }

        // Match by description
        if let Some(desc) = &info.description
            && desc.to_lowercase().contains(query_lower)
        {
            return true;
        }

        // Match by tags
        if !tags.is_empty() {
            for tag in tags {
                if info
                    .tags
                    .iter()
                    .any(|t| t.to_lowercase() == tag.to_lowercase())
                {
                    return true;
                }
            }
        }

        // If query is empty and tags is empty, match everything
        if query_lower.is_empty() && tags.is_empty() {
            return true;
        }

        false
    }

    /// List all discovered files from other peers
    pub async fn list_discovered_files(&self) -> Vec<FileShareInfo> {
        let discovered = self.discovered_files.lock().unwrap();
        discovered.values().cloned().collect()
    }

    /// Get files directory
    pub fn files_dir(&self) -> &Path {
        &self.files_dir
    }

    /// Count of shared files
    pub async fn shared_count(&self) -> usize {
        let shared = self.shared_files.lock().unwrap();
        shared.len()
    }

    /// Count of discovered files
    pub async fn discovered_count(&self) -> usize {
        let discovered = self.discovered_files.lock().unwrap();
        discovered.len()
    }
}
