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
    /// Evicts oldest entries if capacity exceeds MAX_DISCOVERED_FILES
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

        // Evict oldest entries if over capacity (prevent memory leak)
        if discovered.len() > crate::MAX_DISCOVERED_FILES {
            let evict_count = discovered.len() - crate::MAX_DISCOVERED_FILES;
            // Sort by created_at and remove oldest
            let mut entries: Vec<_> = discovered
                .iter()
                .map(|(k, v)| (k.clone(), v.created_at))
                .collect();
            entries.sort_by_key(|(_, ts)| *ts);
            for (hash, _) in entries.into_iter().take(evict_count) {
                discovered.remove(&hash);
            }
            tracing::warn!(
                evicted = evict_count,
                "Discovered files capacity reached, evicted oldest"
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
        let query_matches = if query_lower.is_empty() {
            true
        } else {
            // Match by filename
            info.file_ref.name.to_lowercase().contains(query_lower)
                // Match by description
                || info
                    .description
                    .as_ref()
                    .map(|desc| desc.to_lowercase().contains(query_lower))
                    .unwrap_or(false)
        };

        let tags_match = if tags.is_empty() {
            true
        } else {
            // Match by tags (case-insensitive)
            tags.iter().any(|tag| {
                info.tags
                    .iter()
                    .any(|t| t.to_lowercase() == tag.to_lowercase())
            })
        };

        // If both query and tags are provided, both must match (AND logic)
        // If only one is provided, that one must match
        // If neither is provided, match everything
        query_matches && tags_match
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn make_test_file(dir: &TempDir, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content).unwrap();
        path
    }

    async fn make_manager(dir: &TempDir) -> FileSharingManager {
        let files_dir = dir.path().join("shared");
        std::fs::create_dir_all(&files_dir).unwrap();
        FileSharingManager::new(files_dir)
    }

    #[tokio::test]
    async fn test_new_manager_empty() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        assert_eq!(mgr.shared_count().await, 0);
        assert_eq!(mgr.discovered_count().await, 0);
        assert!(mgr.list_shared_files().await.is_empty());
        assert!(mgr.list_discovered_files().await.is_empty());
    }

    #[tokio::test]
    async fn test_share_file_basic() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let path = make_test_file(&dir, "hello.txt", b"hello world");
        let info = mgr
            .share_file(&path, vec!["test".into()], None, "peer1".into())
            .await
            .unwrap();
        assert_eq!(info.file_ref.name, "hello.txt");
        assert_eq!(info.file_ref.size, 11);
        assert_eq!(info.owner, "peer1");
        assert_eq!(info.tags, vec!["test"]);
        assert_eq!(mgr.shared_count().await, 1);
    }

    #[tokio::test]
    async fn test_share_file_with_description() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let path = make_test_file(&dir, "doc.pdf", b"pdf content");
        let info = mgr
            .share_file(&path, vec![], Some("A document".into()), "peer2".into())
            .await
            .unwrap();
        assert_eq!(info.description, Some("A document".to_string()));
    }

    #[tokio::test]
    async fn test_share_file_not_found() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let result = mgr
            .share_file(
                Path::new("/nonexistent/file.txt"),
                vec![],
                None,
                "peer".into(),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unshare_file() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let path = make_test_file(&dir, "a.txt", b"aaa");
        let info = mgr
            .share_file(&path, vec![], None, "peer".into())
            .await
            .unwrap();
        assert_eq!(mgr.shared_count().await, 1);
        assert!(mgr.unshare_file(&info.file_ref.hash).await);
        assert_eq!(mgr.shared_count().await, 0);
    }

    #[tokio::test]
    async fn test_unshare_nonexistent() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        assert!(!mgr.unshare_file("nonexistent_hash").await);
    }

    #[tokio::test]
    async fn test_get_shared_file() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let path = make_test_file(&dir, "b.txt", b"bbb");
        let info = mgr
            .share_file(&path, vec![], None, "peer".into())
            .await
            .unwrap();
        let found = mgr.get_shared_file(&info.file_ref.hash).await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().file_ref.name, "b.txt");
        assert!(mgr.get_shared_file("bad_hash").await.is_none());
    }

    #[tokio::test]
    async fn test_list_shared_files() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let p1 = make_test_file(&dir, "f1.txt", b"1");
        let p2 = make_test_file(&dir, "f2.txt", b"2");
        mgr.share_file(&p1, vec![], None, "peer".into())
            .await
            .unwrap();
        mgr.share_file(&p2, vec![], None, "peer".into())
            .await
            .unwrap();
        let list = mgr.list_shared_files().await;
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_read_chunk() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let content = b"0123456789abcdef";
        // Create file in manager's files_dir, not in dir.path()
        let files_dir = dir.path().join("shared");
        let path = files_dir.join("chunk.dat");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content).unwrap();
        let info = mgr
            .share_file(&path, vec![], None, "peer".into())
            .await
            .unwrap();
        // chunk_size is 256KB, so one chunk for 16 bytes
        let chunk = mgr.read_chunk(&info.file_ref.hash, 0).await.unwrap();
        assert_eq!(chunk, content);
    }

    #[tokio::test]
    async fn test_read_chunk_not_found() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let result = mgr.read_chunk("bad_hash", 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_announce() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let data = b"announce data";
        let file_ref = FileRef::new(
            "ann.txt".into(),
            data.len() as u64,
            "text/plain".into(),
            data,
        );
        let info = FileShareInfo {
            file_ref: file_ref.clone(),
            owner: "remote_peer".into(),
            tags: vec!["tag1".into()],
            description: None,
            created_at: Utc::now(),
        };
        mgr.process_announce(&[info.clone()]).await;
        assert_eq!(mgr.discovered_count().await, 1);
        let discovered = mgr.list_discovered_files().await;
        assert_eq!(discovered[0].file_ref.hash, file_ref.hash);
    }

    #[tokio::test]
    async fn test_search_by_filename() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let p = make_test_file(&dir, "rust_book.pdf", b"content");
        mgr.share_file(&p, vec![], None, "peer".into())
            .await
            .unwrap();
        let results = mgr.search("rust", &[]).await;
        assert_eq!(results.len(), 1);
        let empty = mgr.search("python", &[]).await;
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_search_by_description() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let p = make_test_file(&dir, "x.bin", b"data");
        mgr.share_file(
            &p,
            vec![],
            Some("important binary file".into()),
            "peer".into(),
        )
        .await
        .unwrap();
        let results = mgr.search("important", &[]).await;
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_search_by_tags() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let p = make_test_file(&dir, "song.mp3", b"music");
        mgr.share_file(&p, vec!["music".into(), "rock".into()], None, "peer".into())
            .await
            .unwrap();
        let results = mgr.search("", &["music".into()]).await;
        assert_eq!(results.len(), 1);
        let results2 = mgr.search("", &["jazz".into()]).await;
        assert!(results2.is_empty());
    }

    #[tokio::test]
    async fn test_search_empty_query_matches_all() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let p1 = make_test_file(&dir, "a.txt", b"a");
        let p2 = make_test_file(&dir, "b.txt", b"b");
        mgr.share_file(&p1, vec![], None, "peer".into())
            .await
            .unwrap();
        mgr.share_file(&p2, vec![], None, "peer".into())
            .await
            .unwrap();
        let results = mgr.search("", &[]).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_search_case_insensitive() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let p = make_test_file(&dir, "README.md", b"readme");
        mgr.share_file(&p, vec![], None, "peer".into())
            .await
            .unwrap();
        let results = mgr.search("readme", &[]).await;
        assert_eq!(results.len(), 1);
        let results2 = mgr.search("README", &[]).await;
        assert_eq!(results2.len(), 1);
    }

    #[tokio::test]
    async fn test_search_includes_discovered() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let data = b"discovered content";
        let file_ref = FileRef::new(
            "remote_file.txt".into(),
            data.len() as u64,
            "text/plain".into(),
            data,
        );
        let info = FileShareInfo {
            file_ref,
            owner: "remote".into(),
            tags: vec![],
            description: None,
            created_at: Utc::now(),
        };
        mgr.process_announce(&[info]).await;
        let results = mgr.search("remote_file", &[]).await;
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_files_dir() {
        let dir = TempDir::new().unwrap();
        let files_dir = dir.path().join("my_files");
        let mgr = FileSharingManager::new(files_dir.clone());
        assert_eq!(mgr.files_dir(), files_dir.as_path());
    }

    #[tokio::test]
    async fn test_set_event_sender() {
        let dir = TempDir::new().unwrap();
        let mut mgr = make_manager(&dir).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        mgr.set_event_sender(tx);
        // Just verify it doesn't panic
    }

    #[tokio::test]
    async fn test_share_empty_file() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;
        let path = make_test_file(&dir, "empty.dat", b"");
        let info = mgr
            .share_file(&path, vec![], None, "peer".into())
            .await
            .unwrap();
        assert_eq!(info.file_ref.size, 0);
        assert_eq!(info.file_ref.chunks, 0);
    }

    #[tokio::test]
    async fn test_share_multiple_files_same_name() {
        let dir = TempDir::new().unwrap();
        let mgr = make_manager(&dir).await;

        // Create two files with the same name but in different directories
        let dir1 = dir.path().join("dir1");
        let dir2 = dir.path().join("dir2");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();

        let p1 = dir1.join("same.txt");
        let p2 = dir2.join("same.txt");

        std::fs::write(&p1, b"content1").unwrap();
        std::fs::write(&p2, b"content2").unwrap();

        let i1 = mgr
            .share_file(&p1, vec![], None, "peer".into())
            .await
            .unwrap();
        let i2 = mgr
            .share_file(&p2, vec![], None, "peer".into())
            .await
            .unwrap();

        // Different content -> different hashes
        assert_ne!(i1.file_ref.hash, i2.file_ref.hash);
        assert_eq!(mgr.shared_count().await, 2);
    }
}
