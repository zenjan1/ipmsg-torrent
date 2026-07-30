//! Multi-protocol download engine for IPMsg-Torrent
//!
//! Supports:
//! - BitTorrent (.torrent files)
//! - eDonkey/eMule (ed2k links)
//! - Xunlei P2SP (HTTP/FTP + P2P hybrid)

pub mod ed2k;
pub mod torrent;
pub mod xunlei;

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Unified download task
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub id: String,
    pub name: String,
    pub protocol: DownloadProtocol,
    pub size: u64,
    pub downloaded: u64,
    pub state: DownloadState,
    pub save_path: PathBuf,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadProtocol {
    Torrent,
    Ed2k,
    Xunlei,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadState {
    Queued,
    Downloading,
    Paused,
    Complete,
    Error,
}

impl DownloadTask {
    pub fn progress(&self) -> f32 {
        if self.size == 0 {
            return 0.0;
        }
        (self.downloaded as f32 / self.size as f32) * 100.0
    }
}

/// Unified download manager
pub struct DownloadManager {
    tasks: Arc<Mutex<Vec<DownloadTask>>>,
    data_dir: PathBuf,
}

impl DownloadManager {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(Vec::new())),
            data_dir,
        }
    }

    /// Add a torrent download task
    pub async fn add_torrent(&self, torrent_path: PathBuf) -> Result<String, DownloadManagerError> {
        let data = tokio::fs::read(&torrent_path)
            .await
            .map_err(|e| DownloadManagerError::Io(e.to_string()))?;

        let meta = torrent::TorrentMeta::from_bytes(&data)
            .map_err(|e| DownloadManagerError::Protocol(e.to_string()))?;

        let task_id = uuid::Uuid::new_v4().to_string();
        let task = DownloadTask {
            id: task_id.clone(),
            name: meta.info.name.clone(),
            protocol: DownloadProtocol::Torrent,
            size: meta.total_size(),
            downloaded: 0,
            state: DownloadState::Queued,
            save_path: self.data_dir.join("downloads"),
            created_at: chrono::Utc::now(),
        };

        self.tasks.lock().await.push(task);

        // Spawn download task
        let meta_clone = meta;
        let download_dir = self.data_dir.join("downloads");
        let tasks = self.tasks.clone();
        let task_id_clone = task_id.clone();

        tokio::spawn(async move {
            // Update state to downloading
            {
                let mut tasks = tasks.lock().await;
                if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                    t.state = DownloadState::Downloading;
                }
            }

            let mut engine = torrent::TorrentEngine::new(meta_clone, download_dir);
            match engine.download().await {
                Ok(()) => {
                    let mut tasks = tasks.lock().await;
                    if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                        t.state = DownloadState::Complete;
                        t.downloaded = t.size;
                    }
                }
                Err(e) => {
                    tracing::error!(task_id = %task_id_clone, error = %e, "Torrent download failed");
                    let mut tasks = tasks.lock().await;
                    if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                        t.state = DownloadState::Error;
                    }
                }
            }
        });

        Ok(task_id)
    }

    /// Add an ed2k download task
    pub async fn add_ed2k(
        &self,
        file_hash: ed2k::Ed2kFileHash,
        file_size: u64,
        file_name: String,
        servers: Vec<std::net::SocketAddr>,
    ) -> Result<String, DownloadManagerError> {
        let task_id = uuid::Uuid::new_v4().to_string();
        let task = DownloadTask {
            id: task_id.clone(),
            name: file_name.clone(),
            protocol: DownloadProtocol::Ed2k,
            size: file_size,
            downloaded: 0,
            state: DownloadState::Queued,
            save_path: self.data_dir.join("downloads"),
            created_at: chrono::Utc::now(),
        };

        self.tasks.lock().await.push(task);

        // Spawn download task
        let download_dir = self.data_dir.join("downloads");
        let tasks = self.tasks.clone();
        let task_id_clone = task_id.clone();
        let file_name_clone = file_name.clone();

        tokio::spawn(async move {
            {
                let mut tasks = tasks.lock().await;
                if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                    t.state = DownloadState::Downloading;
                }
            }

            let mut engine =
                ed2k::Ed2kEngine::new(file_hash, file_size, file_name_clone, download_dir, servers);
            match engine.download().await {
                Ok(()) => {
                    let mut tasks = tasks.lock().await;
                    if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                        t.state = DownloadState::Complete;
                        t.downloaded = t.size;
                    }
                }
                Err(e) => {
                    tracing::error!(task_id = %task_id_clone, error = %e, "ed2k download failed");
                    let mut tasks = tasks.lock().await;
                    if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                        t.state = DownloadState::Error;
                    }
                }
            }
        });

        Ok(task_id)
    }

    /// Add a Xunlei P2SP download task
    pub async fn add_xunlei(
        &self,
        file_name: String,
        file_size: u64,
        sources: Vec<xunlei::XunleiSource>,
    ) -> Result<String, DownloadManagerError> {
        let task_id = uuid::Uuid::new_v4().to_string();
        let task = DownloadTask {
            id: task_id.clone(),
            name: file_name.clone(),
            protocol: DownloadProtocol::Xunlei,
            size: file_size,
            downloaded: 0,
            state: DownloadState::Queued,
            save_path: self.data_dir.join("downloads"),
            created_at: chrono::Utc::now(),
        };

        self.tasks.lock().await.push(task);

        // Spawn download task
        let download_dir = self.data_dir.join("downloads");
        let tasks = self.tasks.clone();
        let task_id_clone = task_id.clone();
        let file_name_clone = file_name.clone();

        tokio::spawn(async move {
            {
                let mut tasks = tasks.lock().await;
                if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                    t.state = DownloadState::Downloading;
                }
            }

            let mut engine =
                xunlei::XunleiEngine::new(file_name_clone, file_size, sources, download_dir);
            match engine.download().await {
                Ok(()) => {
                    let mut tasks = tasks.lock().await;
                    if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                        t.state = DownloadState::Complete;
                        t.downloaded = t.size;
                    }
                }
                Err(e) => {
                    tracing::error!(task_id = %task_id_clone, error = %e, "Xunlei download failed");
                    let mut tasks = tasks.lock().await;
                    if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id_clone) {
                        t.state = DownloadState::Error;
                    }
                }
            }
        });

        Ok(task_id)
    }

    /// List all download tasks
    pub async fn list_tasks(&self) -> Vec<DownloadTask> {
        self.tasks.lock().await.clone()
    }

    /// Get task by ID
    pub async fn get_task(&self, task_id: &str) -> Option<DownloadTask> {
        self.tasks
            .lock()
            .await
            .iter()
            .find(|t| t.id == task_id)
            .cloned()
    }

    /// Pause a download task
    pub async fn pause_task(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            if task.state == DownloadState::Downloading {
                task.state = DownloadState::Paused;
                return true;
            }
        }
        false
    }

    /// Resume a paused task
    pub async fn resume_task(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            if task.state == DownloadState::Paused {
                task.state = DownloadState::Downloading;
                return true;
            }
        }
        false
    }

    /// Remove a task
    pub async fn remove_task(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.lock().await;
        let len_before = tasks.len();
        tasks.retain(|t| t.id != task_id);
        tasks.len() < len_before
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DownloadManagerError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("task not found: {0}")]
    TaskNotFound(String),
}
