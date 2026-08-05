//! Multi-protocol download engine for IPMsg-Torrent
//!
//! Supports:
//! - BitTorrent (.torrent files)
//! - eDonkey/eMule (ed2k links)
//! - Xunlei P2SP (HTTP/FTP + P2P hybrid)

pub mod ed2k;
pub mod magnet;
pub mod torrent;
pub mod xunlei;
pub mod dht;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Unified download task
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub id: String,
    pub name: String,
    pub protocol: DownloadProtocol,
    pub size: u64,
    pub downloaded: u64,
    pub state: DownloadState,
    pub error: Option<String>,
    pub speed_bps: f64,
    pub save_path: PathBuf,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadProtocol {
    Torrent,
    Ed2k,
    Xunlei,
    Magnet,
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

    pub fn eta_seconds(&self) -> Option<f64> {
        if self.speed_bps <= 0.0 || self.size == 0 {
            return None;
        }
        let remaining = self.size.saturating_sub(self.downloaded) as f64;
        // Add 10% safety margin for network fluctuations
        let eta = (remaining / self.speed_bps) * 1.1;
        Some(eta)
    }

    pub fn state_label(&self) -> &'static str {
        match self.state {
            DownloadState::Queued => "queued",
            DownloadState::Downloading => "downloading",
            DownloadState::Paused => "paused",
            DownloadState::Complete => "complete",
            DownloadState::Error => "error",
        }
    }
}

/// Parameters needed to restart a paused/failed task
#[derive(Debug, Clone)]
enum TaskParams {
    Torrent { torrent_path: PathBuf },
    Ed2k {
        file_hash: ed2k::Ed2kFileHash,
        file_size: u64,
        file_name: String,
        servers: Vec<std::net::SocketAddr>,
    },
    Xunlei {
        file_name: String,
        file_size: u64,
        sources: Vec<xunlei::XunleiSource>,
    },
    Magnet {
        #[allow(dead_code)]
        info_hash: [u8; 20],
        #[allow(dead_code)]
        display_name: Option<String>,
        #[allow(dead_code)]
        trackers: Vec<String>,
    },
}

/// Stored task info for resume
#[derive(Debug, Clone)]
struct TaskInfo {
    params: TaskParams,
}

/// Internal handle for a running task
struct RunningTask {
    cancel_token: CancellationToken,
    #[allow(dead_code)]
    params: TaskParams,
    #[allow(dead_code)]
    started_at: std::time::Instant,
    last_downloaded: u64,
    generation: u64,
    // Speed tracking with moving average
    speed_samples: Vec<f64>,
    last_sample_time: std::time::Instant,
}

/// Unified download manager
pub struct DownloadManager {
    tasks: Arc<Mutex<Vec<DownloadTask>>>,
    running: Arc<Mutex<HashMap<String, RunningTask>>>,
    task_info: Arc<Mutex<HashMap<String, TaskInfo>>>,
    task_generation: Arc<Mutex<HashMap<String, u64>>>,
    data_dir: PathBuf,
    dht: Arc<dht::DhtManager>,
}

impl DownloadManager {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(Mutex::new(HashMap::new())),
            task_info: Arc::new(Mutex::new(HashMap::new())),
            task_generation: Arc::new(Mutex::new(HashMap::new())),
            data_dir,
            dht: Arc::new(dht::DhtManager::new()),
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
        let now = chrono::Utc::now();

        let task = DownloadTask {
            id: task_id.clone(),
            name: meta.info.name.clone(),
            protocol: DownloadProtocol::Torrent,
            size: meta.total_size(),
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: self.data_dir.join("downloads"),
            created_at: now,
            updated_at: now,
        };

        self.tasks.lock().await.push(task);

        let params = TaskParams::Torrent {
            torrent_path: torrent_path.clone(),
        };

        self.spawn_task(task_id.clone(), params).await;

        Ok(task_id)
    }

    /// Add a magnet link download task
    pub async fn add_magnet(&self, magnet_uri: &str) -> Result<String, DownloadManagerError> {
        use magnet::MagnetLink;

        let magnet = MagnetLink::parse(magnet_uri)
            .map_err(|e| DownloadManagerError::Protocol(e.to_string()))?;

        let task_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();

        let name = magnet.display_name.clone()
            .unwrap_or_else(|| format!("magnet-{}", hex::encode(&magnet.info_hash[..8])));

        let task = DownloadTask {
            id: task_id.clone(),
            name: name.clone(),
            protocol: DownloadProtocol::Magnet,
            size: 0, // Unknown until metadata is fetched
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: self.data_dir.join("downloads"),
            created_at: now,
            updated_at: now,
        };

        self.tasks.lock().await.push(task);

        let params = TaskParams::Magnet {
            info_hash: magnet.info_hash.try_into().map_err(|_| {
                DownloadManagerError::Protocol("Invalid info hash length".to_string())
            })?,
            display_name: magnet.display_name,
            trackers: magnet.trackers,
        };

        self.spawn_task(task_id.clone(), params).await;

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
        let now = chrono::Utc::now();

        let task = DownloadTask {
            id: task_id.clone(),
            name: file_name.clone(),
            protocol: DownloadProtocol::Ed2k,
            size: file_size,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: self.data_dir.join("downloads"),
            created_at: now,
            updated_at: now,
        };

        self.tasks.lock().await.push(task);

        let params = TaskParams::Ed2k {
            file_hash,
            file_size,
            file_name,
            servers,
        };

        self.spawn_task(task_id.clone(), params).await;

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
        let now = chrono::Utc::now();

        let task = DownloadTask {
            id: task_id.clone(),
            name: file_name.clone(),
            protocol: DownloadProtocol::Xunlei,
            size: file_size,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: self.data_dir.join("downloads"),
            created_at: now,
            updated_at: now,
        };

        self.tasks.lock().await.push(task);

        let params = TaskParams::Xunlei {
            file_name,
            file_size,
            sources,
        };

        self.spawn_task(task_id.clone(), params).await;

        Ok(task_id)
    }

    /// Add an HTTP/FTP URL download (auto-detects file size via HEAD)
    pub async fn add_url(&self, url: &str) -> Result<String, DownloadManagerError> {
        // HEAD request to get file size and name
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|e| DownloadManagerError::Io(e.to_string()))?;

        let head_resp = client
            .head(url)
            .send()
            .await
            .map_err(|e| DownloadManagerError::Io(format!("HEAD request failed: {e}")))?;

        if !head_resp.status().is_success() {
            return Err(DownloadManagerError::Io(format!(
                "HEAD returned {}",
                head_resp.status()
            )));
        }

        let content_length = head_resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        // Extract filename from URL
        let file_name = url
            .split('/')
            .next_back()
            .unwrap_or("download")
            .split('?')
            .next()
            .unwrap_or("download")
            .to_string();
        let file_name = if file_name.is_empty() {
            "download".to_string()
        } else {
            file_name
        };

        let sources = vec![xunlei::XunleiSource::Http {
            url: url.to_string(),
            cookies: None,
            referer: None,
        }];

        self.add_xunlei(file_name, content_length, sources).await
    }

    /// Spawn the actual download task
    async fn spawn_task(&self, task_id: String, params: TaskParams) {
        // Increment generation to invalidate any old spawned tasks
        let generation = {
            let mut gen_map = self.task_generation.lock().await;
            let g = gen_map.entry(task_id.clone()).or_insert(0);
            *g += 1;
            *g
        };
        
        let cancel_token = CancellationToken::new();
        let tasks = self.tasks.clone();
        let running = self.running.clone();
        let data_dir = self.data_dir.clone();
        let dht = self.dht.clone();
        let task_id_clone = task_id.clone();
        let cancel_clone = cancel_token.clone();
        let task_generation = self.task_generation.clone();

        // Store task info for resume
        {
            let mut info = self.task_info.lock().await;
            info.insert(task_id.clone(), TaskInfo { params: params.clone() });
        }

        // Register running task
        {
            let mut r = running.lock().await;
            r.insert(
                task_id.clone(),
                RunningTask {
                    cancel_token: cancel_token.clone(),
                    params: params.clone(),
                    started_at: std::time::Instant::now(),
                    last_downloaded: 0,
                    generation,
                    speed_samples: Vec::new(),
                    last_sample_time: std::time::Instant::now(),
                },
            );
        }

        // Mark as downloading
        {
            let mut t = tasks.lock().await;
            if let Some(task) = t.iter_mut().find(|t| t.id == task_id) {
                task.state = DownloadState::Downloading;
                task.updated_at = chrono::Utc::now();
            }
        }

        tokio::spawn(async move {
            let result: Result<(), String> = match params {
                TaskParams::Torrent { torrent_path } => {
                    match tokio::fs::read(&torrent_path).await {
                        Ok(data) => {
                            match torrent::TorrentMeta::from_bytes(&data) {
                                Ok(meta) => {
                                    let download_dir = data_dir.join("downloads");
                                    let mut engine =
                                        torrent::TorrentEngine::new(meta, download_dir);
                                    engine.download(Some(cancel_clone)).await.map_err(|e| e.to_string())
                                }
                                Err(e) => Err(e.to_string()),
                            }
                        }
                        Err(e) => Err(e.to_string()),
                    }
                }
                TaskParams::Ed2k {
                    file_hash,
                    file_size,
                    file_name,
                    servers,
                } => {
                    let download_dir = data_dir.join("downloads");
                    let mut engine = ed2k::Ed2kEngine::new(
                        file_hash,
                        file_size,
                        file_name,
                        download_dir,
                        servers,
                    );
                    engine.download(Some(cancel_clone)).await.map_err(|e| e.to_string())
                }
                TaskParams::Xunlei {
                    file_name,
                    file_size,
                    sources,
                } => {
                    let download_dir = data_dir.join("downloads");
                    let mut engine =
                        xunlei::XunleiEngine::new(file_name, file_size, sources, download_dir);
                    engine.download(Some(cancel_clone)).await.map_err(|e| e.to_string())
                }
                TaskParams::Magnet {
                    info_hash,
                    display_name: _,
                    trackers: _,
                } => {
                    // Magnet link handling: fetch metadata first, then download as torrent
                    let download_dir = data_dir.join("downloads");
                    
                    // Step 1: Use DHT to find peers
                    let peers = dht.find_peers(info_hash).await.map_err(|e| e.to_string())?;
                    
                    if peers.is_empty() {
                        return Err("No peers found via DHT".to_string());
                    }
                    
                    // Step 2: Try to fetch metadata from peers (BEP 0009)
                    match dht.fetch_metadata(info_hash).await {
                        Ok(metadata_bytes) => {
                            // Parse metadata as torrent
                            match torrent::TorrentMeta::from_bytes(&metadata_bytes) {
                                Ok(meta) => {
                                    // Update task with actual file info
                                    {
                                        let mut t = tasks.lock().await;
                                        if let Some(task) = t.iter_mut().find(|t| t.id == task_id_clone) {
                                            task.name = meta.info.name.clone();
                                            task.size = meta.total_size();
                                        }
                                    }
                                    
                                    // Start torrent download
                                    let mut engine = torrent::TorrentEngine::new(meta, download_dir);
                                    engine.download(Some(cancel_clone)).await.map_err(|e| e.to_string())
                                }
                                Err(e) => Err(format!("Failed to parse metadata: {}", e)),
                            }
                        }
                        Err(dht::DhtError::NotImplemented) => {
                            // Metadata exchange not yet implemented
                            Err("Magnet link metadata exchange not yet implemented. Use .torrent files instead.".to_string())
                        }
                        Err(e) => Err(format!("Failed to fetch metadata: {}", e)),
                    }
                }
            };

            // Update task state only if we're still the active task (same generation)
            let my_generation = {
                let gen_map = task_generation.lock().await;
                gen_map.get(&task_id_clone).copied().unwrap_or(0)
            };
            let is_still_active = {
                let r = running.lock().await;
                r.get(&task_id_clone)
                    .map(|rt| rt.generation == my_generation)
                    .unwrap_or(false)
            };

            let mut t = tasks.lock().await;
            if let Some(task) = t.iter_mut().find(|t| t.id == task_id_clone) {
                match result {
                    Ok(()) => {
                        task.state = DownloadState::Complete;
                        task.downloaded = task.size;
                        task.speed_bps = 0.0;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if err_str == "cancelled" {
                            // Only set Paused if we're still the active task
                            // (not if we were replaced by a resume)
                            if is_still_active {
                                task.state = DownloadState::Paused;
                            }
                        } else {
                            task.state = DownloadState::Error;
                            task.error = Some(err_str);
                        }
                        task.speed_bps = 0.0;
                    }
                }
                task.updated_at = chrono::Utc::now();
            }

            // Remove from running only if we're still the active task
            if is_still_active {
                running.lock().await.remove(&task_id_clone);
            }
            
            Ok(())
        });

        // Spawn speed tracker
        self.spawn_speed_tracker(task_id);
    }

    /// Periodically update speed for a running task using moving average
    fn spawn_speed_tracker(&self, task_id: String) {
        let tasks = self.tasks.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                interval.tick().await;

                let should_stop = {
                    let r = running.lock().await;
                    match r.get(&task_id) {
                        None => true, // task no longer running
                        Some(rt) => rt.cancel_token.is_cancelled(),
                    }
                };

                if should_stop {
                    break;
                }

                // Calculate speed with moving average
                let mut r = running.lock().await;
                if let Some(rt) = r.get_mut(&task_id) {
                    let now = std::time::Instant::now();
                    let dt = now.duration_since(rt.last_sample_time).as_secs_f64();
                    
                    if dt < 0.5 {
                        continue;
                    }

                    // Read current downloaded from tasks
                    let current_downloaded = {
                        let t = tasks.lock().await;
                        t.iter()
                            .find(|t| t.id == task_id)
                            .map(|t| t.downloaded)
                            .unwrap_or(0)
                    };

                    let dd = current_downloaded.saturating_sub(rt.last_downloaded) as f64;
                    let instant_speed = dd / dt;

                    // Add to samples (keep last 10)
                    rt.speed_samples.push(instant_speed);
                    if rt.speed_samples.len() > 10 {
                        rt.speed_samples.remove(0);
                    }

                    // Calculate weighted moving average (recent samples have more weight)
                    let avg_speed = if rt.speed_samples.is_empty() {
                        0.0
                    } else {
                        let weights: Vec<f64> = (1..=rt.speed_samples.len())
                            .map(|i| i as f64)
                            .collect();
                        let total_weight: f64 = weights.iter().sum();
                        let weighted_sum: f64 = rt.speed_samples
                            .iter()
                            .zip(weights.iter())
                            .map(|(speed, weight)| speed * weight)
                            .sum();
                        weighted_sum / total_weight
                    };

                    // Update task speed
                    let mut t = tasks.lock().await;
                    if let Some(task) = t.iter_mut().find(|t| t.id == task_id) {
                        task.speed_bps = avg_speed;
                        task.updated_at = chrono::Utc::now();
                    }

                    rt.last_downloaded = current_downloaded;
                    rt.last_sample_time = now;
                } else {
                    break;
                }
            }
        });
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
        // Cancel and remove from running immediately
        {
            let mut r = self.running.lock().await;
            if let Some(rt) = r.remove(task_id) {
                rt.cancel_token.cancel();
            }
        }

        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            if task.state == DownloadState::Downloading || task.state == DownloadState::Queued {
                task.state = DownloadState::Paused;
                task.speed_bps = 0.0;
                task.updated_at = chrono::Utc::now();
                return true;
            }
        }
        false
    }

    /// Check if a task can be resumed (has stored params)
    pub async fn can_resume(&self, task_id: &str) -> bool {
        let info = self.task_info.lock().await;
        info.contains_key(task_id)
    }

    /// Resume a paused or failed task by re-spawning the engine
    pub async fn resume_task(&self, task_id: &str) -> bool {
        // Get the task and its params
        let params = {
            let tasks = self.tasks.lock().await;
            let Some(task) = tasks.iter().find(|t| t.id == task_id) else {
                tracing::debug!(task_id, "resume_task: task not found");
                return false;
            };

            if task.state != DownloadState::Paused && task.state != DownloadState::Error {
                tracing::debug!(task_id, state = ?task.state, "resume_task: wrong state");
                return false;
            }

            // Check if already running (shouldn't be, but guard)
            let r = self.running.lock().await;
            let already_running = r.contains_key(task_id);
            drop(r);

            if already_running {
                // Wait a bit for the old task to clean up
                drop(tasks);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let r = self.running.lock().await;
                if r.contains_key(task_id) {
                    tracing::debug!(task_id, "resume_task: still running after wait");
                    return false;
                }
                drop(r);
                // Re-acquire tasks lock
                let tasks = self.tasks.lock().await;
                let Some(task) = tasks.iter().find(|t| t.id == task_id) else {
                    return false;
                };
                if task.state != DownloadState::Paused && task.state != DownloadState::Error {
                    return false;
                }
            }

            // Get stored params
            let info = self.task_info.lock().await;
            let Some(task_info) = info.get(task_id) else {
                tracing::debug!(task_id, "resume_task: task_info not found");
                return false;
            };
            task_info.params.clone()
        };

        // Reset state
        {
            let mut tasks = self.tasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
                task.state = DownloadState::Queued;
                task.error = None;
                task.speed_bps = 0.0;
                task.updated_at = chrono::Utc::now();
            }
        }
        self.spawn_task(task_id.to_string(), params).await;
        true
    }

    /// Remove a task (cancels if running)
    pub async fn remove_task(&self, task_id: &str) -> bool {
        // Cancel if running
        {
            let r = self.running.lock().await;
            if let Some(rt) = r.get(task_id) {
                rt.cancel_token.cancel();
            }
        }

        self.running.lock().await.remove(task_id);
        self.task_info.lock().await.remove(task_id);

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
