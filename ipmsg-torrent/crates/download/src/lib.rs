//! Multi-protocol download engine for IPMsg-Torrent
//!
//! Supports:
//! - BitTorrent (.torrent files)
//! - eDonkey/eMule (ed2k links)
//! - Xunlei P2SP (HTTP/FTP + P2P hybrid)

pub mod bandwidth_monitor;
pub mod checksum;
pub mod connection_pool;
pub mod dht;
pub mod disk_monitor;
pub mod download_history;
pub mod ed2k;
pub mod magnet;
pub mod metadata_cache;
pub mod notification;
pub mod progress;
pub mod rate_limiter;
pub mod task_queue;
pub mod torrent;
pub mod web;
pub mod xunlei;

use chrono::Timelike;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use task_queue::{load_task_queue, save_task_queue};
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

use bandwidth_monitor::BandwidthMonitor;
use download_history::{HistoryEntry, append_entry};
use notification::{NotificationContext, NotificationDispatcher};

pub use bandwidth_monitor::{
    BandwidthDashboard, BandwidthMonitor as BandwidthMonitorType, BandwidthSample, BandwidthStats,
    TaskBandwidth,
};
pub use notification::{
    NotificationChannel, NotificationConfig, NotificationError, NotificationEvent,
};
pub use rate_limiter::{DownloadRateController, RateLimiter};

/// Download statistics snapshot.
/// Aggregated from all tasks for dashboard display.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct DownloadStats {
    /// Total number of tasks
    pub total_tasks: usize,
    /// Number of tasks in each state
    pub running: usize,
    pub paused: usize,
    pub completed: usize,
    pub queued: usize,
    pub errored: usize,
    /// Total bytes downloaded across all tasks
    pub total_downloaded: u64,
    /// Total bytes expected (sum of all task sizes)
    pub total_size: u64,
    /// Aggregate download speed (bytes/sec)
    pub total_speed_bps: f64,
    /// Number of tasks per protocol
    pub by_protocol: ProtocolStats,
}

/// Per-protocol task counts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ProtocolStats {
    pub torrent: usize,
    pub ed2k: usize,
    pub xunlei: usize,
    pub magnet: usize,
    pub p2p: usize,
}

/// Events emitted when download tasks change state.
/// WebSocket clients receive these for real-time UI updates.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type")]
pub enum TaskEvent {
    /// A new task was added
    #[serde(rename = "task_added")]
    Added { task: TaskInfoEvent },
    /// A task's progress, speed, or state changed
    #[serde(rename = "task_updated")]
    Updated { task: TaskInfoEvent },
    /// A task was removed
    #[serde(rename = "task_removed")]
    Removed { task_id: String },
    /// Global status update (aggregated stats)
    #[serde(rename = "status")]
    Status {
        total_tasks: usize,
        running_tasks: usize,
        total_speed_bps: f64,
    },
}

/// Snapshot of a task sent over WebSocket events.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TaskInfoEvent {
    pub id: String,
    pub name: String,
    pub protocol: String,
    pub size: u64,
    pub downloaded: u64,
    pub progress: f32,
    pub speed_bps: f64,
    pub state: String,
    pub error: Option<String>,
    pub tags: Vec<String>,
    pub priority: String,
    pub bandwidth_weight: u8,
}

impl TaskInfoEvent {
    fn from_task(task: &DownloadTask) -> Self {
        Self {
            id: task.id.clone(),
            name: task.name.clone(),
            protocol: format!("{:?}", task.protocol),
            size: task.size,
            downloaded: task.downloaded,
            progress: task.progress(),
            speed_bps: task.speed_bps,
            state: task.state_label().to_string(),
            error: task.error.clone(),
            tags: task.tags.clone(),
            priority: task.priority.label().to_string(),
            bandwidth_weight: task.bandwidth_weight,
        }
    }
}

/// Time window for scheduled downloads
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct TimeWindow {
    /// Start hour (0-23)
    pub start_hour: u8,
    /// Start minute (0-59)
    pub start_minute: u8,
    /// End hour (0-23)
    pub end_hour: u8,
    /// End minute (0-59)
    pub end_minute: u8,
}

impl TimeWindow {
    /// Create a new time window
    pub fn new(start_hour: u8, start_minute: u8, end_hour: u8, end_minute: u8) -> Option<Self> {
        if start_hour > 23 || end_hour > 23 || start_minute > 59 || end_minute > 59 {
            return None;
        }
        Some(Self {
            start_hour,
            start_minute,
            end_hour,
            end_minute,
        })
    }

    /// Check if current time is within the window
    pub fn is_active_now(&self) -> bool {
        self.is_active_at(chrono::Local::now())
    }

    /// Check if given time is within the window
    pub fn is_active_at(&self, dt: chrono::DateTime<chrono::Local>) -> bool {
        let current_minutes = dt.hour() as u16 * 60 + dt.minute() as u16;
        let start_minutes = self.start_hour as u16 * 60 + self.start_minute as u16;
        let end_minutes = self.end_hour as u16 * 60 + self.end_minute as u16;

        if start_minutes <= end_minutes {
            // Normal window (e.g., 09:00 - 17:00)
            current_minutes >= start_minutes && current_minutes < end_minutes
        } else {
            // Overnight window (e.g., 22:00 - 06:00)
            current_minutes >= start_minutes || current_minutes < end_minutes
        }
    }

    /// Format as human-readable string
    pub fn format(&self) -> String {
        format!(
            "{:02}:{:02}-{:02}:{:02}",
            self.start_hour, self.start_minute, self.end_hour, self.end_minute
        )
    }
}

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
    /// User-defined tags for organizing downloads (e.g., "movies", "work", "linux")
    pub tags: Vec<String>,
    /// Download priority (higher priority tasks are spawned first)
    pub priority: DownloadPriority,
    /// Optional time window for scheduled downloading
    pub schedule: Option<TimeWindow>,
    /// Bandwidth weight for proportional allocation (1-10, default 1).
    /// Higher weights get proportionally more bandwidth when global limit is active.
    pub bandwidth_weight: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadProtocol {
    Torrent,
    Ed2k,
    Xunlei,
    Magnet,
    P2P,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadState {
    Queued,
    Downloading,
    Paused,
    Complete,
    Error,
}

/// Priority level for download tasks.
/// Higher priority tasks are spawned first when concurrent limits are active.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Default,
)]
pub enum DownloadPriority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
}

impl DownloadPriority {
    /// Parse from string (case-insensitive).
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "high" | "h" | "2" => Some(Self::High),
            "normal" | "n" | "1" | "default" => Some(Self::Normal),
            "low" | "l" | "0" => Some(Self::Low),
            _ => None,
        }
    }

    /// Human-readable label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
        }
    }
}

/// Filter criteria for listing download tasks.
/// All fields are AND-combined; None fields are ignored.
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    /// Substring search in task name (case-insensitive)
    pub query: Option<String>,
    /// Filter by download state
    pub state: Option<DownloadState>,
    /// Filter by protocol
    pub protocol: Option<DownloadProtocol>,
    /// Filter by tag (exact match)
    pub tag: Option<String>,
}

/// Sort criteria for listing download tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSortBy {
    /// Sort by creation time (newest first)
    CreatedDesc,
    /// Sort by creation time (oldest first)
    CreatedAsc,
    /// Sort by name (A-Z)
    NameAsc,
    /// Sort by name (Z-A)
    NameDesc,
    /// Sort by file size (largest first)
    SizeDesc,
    /// Sort by file size (smallest first)
    SizeAsc,
    /// Sort by progress (highest first)
    ProgressDesc,
    /// Sort by progress (lowest first)
    ProgressAsc,
    /// Sort by speed (fastest first)
    SpeedDesc,
}

impl TaskFilter {
    /// Returns true if the task matches all filter criteria.
    pub fn matches(&self, task: &DownloadTask) -> bool {
        if let Some(ref query) = self.query
            && !task.name.to_lowercase().contains(&query.to_lowercase())
        {
            return false;
        }
        if let Some(state) = self.state
            && task.state != state
        {
            return false;
        }
        if let Some(protocol) = self.protocol
            && task.protocol != protocol
        {
            return false;
        }
        if let Some(ref tag) = self.tag
            && !task.tags.iter().any(|t| t == tag)
        {
            return false;
        }
        true
    }
}

/// Apply sort to a list of tasks.
pub fn sort_tasks(tasks: &mut [DownloadTask], sort_by: TaskSortBy) {
    match sort_by {
        TaskSortBy::CreatedDesc => tasks.sort_by_key(|b| std::cmp::Reverse(b.created_at)),
        TaskSortBy::CreatedAsc => tasks.sort_by_key(|a| a.created_at),
        TaskSortBy::NameAsc => tasks.sort_by_key(|a| a.name.to_lowercase()),
        TaskSortBy::NameDesc => tasks.sort_by_key(|b| std::cmp::Reverse(b.name.to_lowercase())),
        TaskSortBy::SizeDesc => tasks.sort_by_key(|b| std::cmp::Reverse(b.size)),
        TaskSortBy::SizeAsc => tasks.sort_by_key(|a| a.size),
        TaskSortBy::ProgressDesc => tasks.sort_by(|a, b| {
            b.progress()
                .partial_cmp(&a.progress())
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        TaskSortBy::ProgressAsc => tasks.sort_by(|a, b| {
            a.progress()
                .partial_cmp(&b.progress())
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        TaskSortBy::SpeedDesc => tasks.sort_by(|a, b| {
            b.speed_bps
                .partial_cmp(&a.speed_bps)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
    }
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
    Torrent {
        torrent_path: PathBuf,
    },
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
    #[allow(dead_code)]
    P2P {
        file_hash: String,
        file_name: String,
        file_size: u64,
        from_peer: String,
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
    // Timeout tracking
    last_progress_time: std::time::Instant,
    retry_count: u32,
}

/// Unified download manager
pub struct DownloadManager {
    tasks: Arc<Mutex<Vec<DownloadTask>>>,
    running: Arc<Mutex<HashMap<String, RunningTask>>>,
    task_info: Arc<Mutex<HashMap<String, TaskInfo>>>,
    task_generation: Arc<Mutex<HashMap<String, u64>>>,
    data_dir: PathBuf,
    dht: Arc<dht::DhtManager>,
    /// Maximum concurrent downloads (0 = unlimited)
    max_concurrent: usize,
    /// Global download rate limiter
    rate_limiter: Arc<DownloadRateController>,
    /// Timeout in seconds for stalled downloads (0 = disabled)
    timeout_secs: Arc<AtomicU64>,
    /// Maximum retry attempts for timed-out downloads
    max_retries: Arc<AtomicU32>,
    /// Broadcast channel for task change events (WebSocket push)
    event_tx: broadcast::Sender<TaskEvent>,
    /// Notification channel for task completion (triggers scheduler)
    task_complete_notify: Arc<tokio::sync::Notify>,
    /// Notification dispatcher for download completion/failure events
    notifier: Arc<NotificationDispatcher>,
    /// Bandwidth monitor for dashboard
    bandwidth_monitor: Arc<BandwidthMonitor>,
}

impl DownloadManager {
    pub fn new(data_dir: PathBuf) -> Self {
        let dm = Self {
            tasks: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(Mutex::new(HashMap::new())),
            task_info: Arc::new(Mutex::new(HashMap::new())),
            task_generation: Arc::new(Mutex::new(HashMap::new())),
            data_dir,
            dht: Arc::new(dht::DhtManager::new()),
            max_concurrent: 0, // 0 = unlimited
            rate_limiter: Arc::new(DownloadRateController::new(0, 0)),
            timeout_secs: Arc::new(AtomicU64::new(0)),
            max_retries: Arc::new(AtomicU32::new(3)),
            event_tx: broadcast::channel(128).0,
            task_complete_notify: Arc::new(tokio::sync::Notify::new()),
            notifier: Arc::new(NotificationDispatcher::new(NotificationConfig::disabled())),
            bandwidth_monitor: Arc::new(BandwidthMonitor::new()),
        };
        dm.start_scheduler();
        dm
    }

    /// Subscribe to task change events for real-time updates.
    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.event_tx.subscribe()
    }

    /// Get the bandwidth monitor handle for dashboard/statistics queries.
    pub fn bandwidth_monitor(&self) -> &Arc<BandwidthMonitor> {
        &self.bandwidth_monitor
    }

    /// Broadcast a task event to all subscribers (best-effort).
    fn emit_event(&self, event: TaskEvent) {
        // Ignore send errors (no active receivers)
        let _ = self.event_tx.send(event);
    }

    /// Record a completed or failed task to download history and send notifications.
    fn record_task_history(
        task: &DownloadTask,
        data_dir: &std::path::Path,
        notifier: Option<&Arc<NotificationDispatcher>>,
    ) {
        if let Some(entry) = HistoryEntry::from_task(task) {
            let data_dir = data_dir.to_path_buf();
            // Spawn async to avoid blocking the caller
            tokio::spawn(async move {
                if let Err(e) = append_entry(&data_dir, entry) {
                    tracing::warn!(error = %e, "Failed to record download history");
                }
            });
        }

        // Send notification
        if let Some(notifier) = notifier {
            let event = match task.state {
                DownloadState::Complete => NotificationEvent::DownloadComplete,
                DownloadState::Error => NotificationEvent::DownloadFailed,
                _ => return,
            };
            let ctx = NotificationContext {
                task_id: task.id.clone(),
                name: task.name.clone(),
                size: task.size,
                downloaded: task.downloaded,
                protocol: format!("{:?}", task.protocol),
                save_path: task.save_path.display().to_string(),
                error: task.error.clone(),
                event,
            };
            let notifier = notifier.clone();
            tokio::spawn(async move {
                if let Err(e) = notifier.dispatch(&ctx).await {
                    tracing::warn!(error = %e, "Failed to send download notification");
                }
            });
        }
    }

    /// Create a DownloadManager and restore tasks from disk
    pub async fn new_with_restore(data_dir: PathBuf) -> Self {
        let tasks = load_task_queue(&data_dir).unwrap_or_default();
        // Reset any "downloading" tasks to "paused" since they aren't actually running
        let mut tasks = tasks;
        for task in &mut tasks {
            if task.state == DownloadState::Downloading {
                task.state = DownloadState::Paused;
                task.speed_bps = 0.0;
            }
        }

        let dm = Self {
            tasks: Arc::new(Mutex::new(tasks)),
            running: Arc::new(Mutex::new(HashMap::new())),
            task_info: Arc::new(Mutex::new(HashMap::new())),
            task_generation: Arc::new(Mutex::new(HashMap::new())),
            data_dir,
            dht: Arc::new(dht::DhtManager::new()),
            max_concurrent: 0,
            rate_limiter: Arc::new(DownloadRateController::new(0, 0)),
            timeout_secs: Arc::new(AtomicU64::new(0)),
            max_retries: Arc::new(AtomicU32::new(3)),
            event_tx: broadcast::channel(128).0,
            task_complete_notify: Arc::new(tokio::sync::Notify::new()),
            notifier: Arc::new(NotificationDispatcher::new(NotificationConfig::disabled())),
            bandwidth_monitor: Arc::new(BandwidthMonitor::new()),
        };
        dm.start_scheduler();
        dm
    }

    /// Start the background scheduler that listens for task completion signals
    /// and spawns the next queued task if a slot is available.
    fn start_scheduler(&self) {
        let notify = self.task_complete_notify.clone();
        let tasks = self.tasks.clone();
        let running = self.running.clone();
        let task_info = self.task_info.clone();
        let task_generation = self.task_generation.clone();
        let data_dir = self.data_dir.clone();
        let dht = self.dht.clone();
        let rate_limiter = self.rate_limiter.clone();
        let max_concurrent = self.max_concurrent;
        let notifier = self.notifier.clone();

        // Spawn schedule checker (runs every 60 seconds)
        let schedule_check_tasks = self.tasks.clone();
        let schedule_check_notify = self.task_complete_notify.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let now = chrono::Local::now();
                let mut changes = 0;
                let mut tasks = schedule_check_tasks.lock().await;

                for task in tasks.iter_mut() {
                    if let Some(window) = task.schedule {
                        let in_window = window.is_active_at(now);

                        match task.state {
                            // Task is downloading but outside its time window — pause it
                            DownloadState::Downloading if !in_window => {
                                task.state = DownloadState::Paused;
                                task.speed_bps = 0.0;
                                task.updated_at = chrono::Utc::now();
                                changes += 1;
                            }
                            // Task is paused but inside its time window — resume it
                            DownloadState::Paused if in_window => {
                                task.state = DownloadState::Queued;
                                task.updated_at = chrono::Utc::now();
                                changes += 1;
                            }
                            _ => {}
                        }
                    }
                }

                if changes > 0 {
                    drop(tasks);
                    schedule_check_notify.notify_one();
                }
            }
        });

        tokio::spawn(async move {
            loop {
                notify.notified().await;

                // Check if we can start a new task
                let can_start = if max_concurrent == 0 {
                    true
                } else {
                    running.lock().await.len() < max_concurrent
                };

                if !can_start {
                    continue;
                }

                // Find the highest-priority queued task
                let next_task_id = {
                    let tasks_lock = tasks.lock().await;
                    tasks_lock
                        .iter()
                        .filter(|t| t.state == DownloadState::Queued)
                        .max_by_key(|t| t.priority)
                        .map(|t| t.id.clone())
                };

                if let Some(task_id) = next_task_id {
                    // Get stored params
                    let info = task_info.lock().await;
                    if let Some(task_info) = info.get(&task_id) {
                        let params = task_info.params.clone();
                        drop(info);

                        // Increment generation
                        let generation = {
                            let mut gen_map = task_generation.lock().await;
                            let g = gen_map.entry(task_id.clone()).or_insert(0);
                            *g += 1;
                            *g
                        };

                        let cancel_token = CancellationToken::new();

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
                                    last_progress_time: std::time::Instant::now(),
                                    retry_count: 0,
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

                        let cancel_clone = cancel_token.clone();
                        let tasks_clone = tasks.clone();
                        let running_clone = running.clone();
                        let task_generation_clone = task_generation.clone();
                        let data_dir_clone = data_dir.clone();
                        let dht_clone = dht.clone();
                        let rate_limiter_clone = rate_limiter.clone();
                        let notify_clone = notify.clone();
                        let task_id_clone = task_id.clone();
                        let notifier_clone = notifier.clone();

                        tokio::spawn(async move {
                            let result: Result<(), String> = match params {
                                TaskParams::Torrent { torrent_path } => {
                                    match tokio::fs::read(&torrent_path).await {
                                        Ok(data) => match torrent::TorrentMeta::from_bytes(&data) {
                                            Ok(meta) => {
                                                let download_dir = data_dir_clone.join("downloads");
                                                let mut engine =
                                                    torrent::TorrentEngine::new(meta, download_dir);
                                                engine.set_rate_limiter(
                                                    rate_limiter_clone.per_task().clone(),
                                                );
                                                engine
                                                    .download(Some(cancel_clone))
                                                    .await
                                                    .map_err(|e| e.to_string())
                                            }
                                            Err(e) => Err(e.to_string()),
                                        },
                                        Err(e) => Err(e.to_string()),
                                    }
                                }
                                TaskParams::Ed2k {
                                    file_hash,
                                    file_size,
                                    file_name,
                                    servers,
                                } => {
                                    let download_dir = data_dir_clone.join("downloads");
                                    let mut engine = ed2k::Ed2kEngine::new(
                                        file_hash,
                                        file_size,
                                        file_name,
                                        download_dir,
                                        servers,
                                    );
                                    engine.set_rate_limiter(rate_limiter_clone.per_task().clone());
                                    engine
                                        .download(Some(cancel_clone))
                                        .await
                                        .map_err(|e| e.to_string())
                                }
                                TaskParams::Xunlei {
                                    file_name,
                                    file_size,
                                    sources,
                                } => {
                                    let download_dir = data_dir_clone.join("downloads");
                                    let mut engine = xunlei::XunleiEngine::new(
                                        file_name,
                                        file_size,
                                        sources,
                                        download_dir,
                                    );
                                    engine.set_rate_limiter(rate_limiter_clone.per_task().clone());
                                    engine
                                        .download(Some(cancel_clone))
                                        .await
                                        .map_err(|e| e.to_string())
                                }
                                TaskParams::Magnet {
                                    info_hash,
                                    display_name,
                                    trackers,
                                } => {
                                    let download_dir = data_dir_clone.join("downloads");
                                    let cache = metadata_cache::cache_dir();
                                    let metadata_bytes = match metadata_cache::load_metadata(
                                        &cache, &info_hash,
                                    ) {
                                        Ok(cached) => cached,
                                        Err(metadata_cache::CacheError::NotFound) => {
                                            let peers = dht_clone
                                                .find_peers(info_hash)
                                                .await
                                                .map_err(|e| e.to_string())?;
                                            if peers.is_empty() {
                                                return Err("No peers found via DHT".to_string());
                                            }
                                            let bytes = match dht_clone
                                                .fetch_metadata(info_hash)
                                                .await
                                            {
                                                Ok(b) => b,
                                                Err(dht::DhtError::NotImplemented) => {
                                                    return Err("Magnet link metadata exchange not yet implemented".to_string());
                                                }
                                                Err(e) => {
                                                    return Err(format!(
                                                        "Failed to fetch metadata: {}",
                                                        e
                                                    ));
                                                }
                                            };
                                            if let Err(e) = metadata_cache::save_metadata(
                                                &cache,
                                                &info_hash,
                                                &bytes,
                                                display_name.as_deref(),
                                                &trackers,
                                            ) {
                                                tracing::warn!(error = %e, "Failed to cache metadata");
                                            }
                                            bytes
                                        }
                                        Err(e) => return Err(format!("Cache error: {e}")),
                                    };
                                    match torrent::TorrentMeta::from_bytes(&metadata_bytes) {
                                        Ok(meta) => {
                                            {
                                                let mut t = tasks_clone.lock().await;
                                                if let Some(task) =
                                                    t.iter_mut().find(|t| t.id == task_id_clone)
                                                {
                                                    task.name = meta.info.name.clone();
                                                    task.size = meta.total_size();
                                                }
                                            }
                                            let mut engine =
                                                torrent::TorrentEngine::new(meta, download_dir);
                                            engine.set_rate_limiter(
                                                rate_limiter_clone.per_task().clone(),
                                            );
                                            engine
                                                .download(Some(cancel_clone))
                                                .await
                                                .map_err(|e| e.to_string())
                                        }
                                        Err(e) => Err(format!("Failed to parse metadata: {}", e)),
                                    }
                                }
                                TaskParams::P2P { .. } => {
                                    Err("P2P resume not yet supported".to_string())
                                }
                            };

                            // Update task state
                            let my_generation = {
                                let gen_map = task_generation_clone.lock().await;
                                gen_map.get(&task_id_clone).copied().unwrap_or(0)
                            };
                            let is_still_active = {
                                let r = running_clone.lock().await;
                                r.get(&task_id_clone)
                                    .map(|rt| rt.generation == my_generation)
                                    .unwrap_or(false)
                            };

                            let mut t = tasks_clone.lock().await;
                            if let Some(task) = t.iter_mut().find(|t| t.id == task_id_clone) {
                                match result {
                                    Ok(()) => {
                                        task.state = DownloadState::Complete;
                                        task.downloaded = task.size;
                                        task.speed_bps = 0.0;
                                        Self::record_task_history(
                                            task,
                                            &data_dir_clone,
                                            Some(&notifier_clone),
                                        );
                                    }
                                    Err(e) => {
                                        let err_str = e.to_string();
                                        if err_str == "cancelled" {
                                            if is_still_active {
                                                task.state = DownloadState::Paused;
                                            }
                                        } else {
                                            task.state = DownloadState::Error;
                                            task.error = Some(err_str);
                                            Self::record_task_history(
                                                task,
                                                &data_dir_clone,
                                                Some(&notifier_clone),
                                            );
                                        }
                                        task.speed_bps = 0.0;
                                    }
                                }
                                task.updated_at = chrono::Utc::now();
                            }

                            if is_still_active {
                                running_clone.lock().await.remove(&task_id_clone);
                            }

                            // Notify scheduler that a slot freed up
                            notify_clone.notify_one();

                            Ok(())
                        });
                    }
                }
            }
        });
    }

    /// Set maximum concurrent downloads (0 = unlimited)
    pub fn set_max_concurrent(&mut self, max: usize) {
        self.max_concurrent = max;
    }

    /// Set global download speed limit in bytes/sec (0 = unlimited).
    /// Shared across all download tasks.
    pub async fn set_global_speed_limit(&self, bytes_per_sec: u64) {
        self.rate_limiter.set_global_limit(bytes_per_sec).await;
    }

    /// Set per-task download speed limit in bytes/sec (0 = unlimited).
    pub async fn set_task_speed_limit(&self, bytes_per_sec: u64) {
        self.rate_limiter.set_task_limit(bytes_per_sec).await;
    }

    /// Get the rate controller handle.
    pub fn rate_limiter(&self) -> &Arc<DownloadRateController> {
        &self.rate_limiter
    }

    /// Set download timeout in seconds (0 = disabled).
    /// When a download makes no progress for this duration, it will be retried.
    pub fn set_timeout_secs(&self, secs: u64) {
        self.timeout_secs.store(secs, Ordering::Relaxed);
    }

    /// Set maximum retry attempts for timed-out downloads.
    pub fn set_max_retries(&self, retries: u32) {
        self.max_retries.store(retries, Ordering::Relaxed);
    }

    /// Set notification configuration for download completion/failure events.
    pub fn set_notification_config(&self, config: NotificationConfig) {
        self.notifier.update_config(config);
    }

    /// Set a time window schedule for a download task.
    /// The task will only download during the specified time window.
    /// Pass None to remove the schedule and allow continuous downloading.
    pub async fn set_schedule(&self, task_id: &str, schedule: Option<TimeWindow>) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.schedule = schedule;
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });
            true
        } else {
            false
        }
    }

    /// Get the schedule for a download task.
    pub async fn get_schedule(&self, task_id: &str) -> Option<Option<TimeWindow>> {
        let tasks = self.tasks.lock().await;
        tasks.iter().find(|t| t.id == task_id).map(|t| t.schedule)
    }

    /// Set bandwidth weight for a download task (1-10, default 1).
    /// Higher weights get proportionally more bandwidth when global limit is active.
    /// Returns true if the task was found and updated.
    pub async fn set_bandwidth_weight(&self, task_id: &str, weight: u8) -> bool {
        let weight = weight.clamp(1, 10);
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.bandwidth_weight = weight;
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });
            true
        } else {
            false
        }
    }

    /// Get the bandwidth weight for a download task.
    pub async fn get_bandwidth_weight(&self, task_id: &str) -> Option<u8> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.id == task_id)
            .map(|t| t.bandwidth_weight)
    }

    /// Calculate proportional bandwidth limits for all running tasks.
    /// Distributes the global limit proportionally based on task weights.
    /// Returns a map of task_id -> bytes_per_sec.
    pub async fn calculate_bandwidth_allocation(&self) -> HashMap<String, u64> {
        let global_limit = self.rate_limiter.global_limit().await;
        if global_limit == 0 {
            // No global limit, no allocation needed
            return HashMap::new();
        }

        let tasks = self.tasks.lock().await;
        let running: Vec<_> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Downloading)
            .collect();

        if running.is_empty() {
            return HashMap::new();
        }

        let total_weight: u64 = running.iter().map(|t| t.bandwidth_weight as u64).sum();
        if total_weight == 0 {
            return HashMap::new();
        }

        running
            .iter()
            .map(|t| {
                let proportion = t.bandwidth_weight as f64 / total_weight as f64;
                let allocated = (global_limit as f64 * proportion) as u64;
                (t.id.clone(), allocated.max(1024)) // At least 1KB/s
            })
            .collect()
    }

    /// Check all tasks with schedules and pause/resume as needed.
    /// Returns the number of tasks that were paused or resumed.
    pub async fn check_schedules(&self) -> usize {
        let now = chrono::Local::now();
        let mut changes = 0;
        let mut tasks = self.tasks.lock().await;

        for task in tasks.iter_mut() {
            if let Some(window) = task.schedule {
                let in_window = window.is_active_at(now);

                match task.state {
                    // Task is downloading but outside its time window — pause it
                    DownloadState::Downloading if !in_window => {
                        task.state = DownloadState::Paused;
                        task.speed_bps = 0.0;
                        task.updated_at = chrono::Utc::now();
                        changes += 1;
                    }
                    // Task is paused but inside its time window — resume it
                    DownloadState::Paused if in_window => {
                        task.state = DownloadState::Queued;
                        task.updated_at = chrono::Utc::now();
                        changes += 1;
                    }
                    _ => {}
                }
            }
        }

        if changes > 0 {
            // Notify scheduler that new tasks may be ready
            drop(tasks);
            self.task_complete_notify.notify_one();
        }

        changes
    }

    /// Get current running task count
    pub async fn running_count(&self) -> usize {
        self.running.lock().await.len()
    }

    /// Check if we can start a new task
    pub async fn can_start_task(&self) -> bool {
        if self.max_concurrent == 0 {
            return true;
        }
        self.running_count().await < self.max_concurrent
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
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        self.tasks.lock().await.push(task.clone());
        self.persist_tasks().await;
        self.emit_event(TaskEvent::Added {
            task: TaskInfoEvent::from_task(&task),
        });

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

        let name = magnet
            .display_name
            .clone()
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
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        self.tasks.lock().await.push(task.clone());
        self.persist_tasks().await;
        self.emit_event(TaskEvent::Added {
            task: TaskInfoEvent::from_task(&task),
        });

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
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        self.tasks.lock().await.push(task.clone());
        self.persist_tasks().await;
        self.emit_event(TaskEvent::Added {
            task: TaskInfoEvent::from_task(&task),
        });

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
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        self.tasks.lock().await.push(task.clone());
        self.persist_tasks().await;
        self.emit_event(TaskEvent::Added {
            task: TaskInfoEvent::from_task(&task),
        });

        let params = TaskParams::Xunlei {
            file_name,
            file_size,
            sources,
        };

        self.spawn_task(task_id.clone(), params).await;

        Ok(task_id)
    }

    /// Add a P2P file download task (downloaded from another peer)
    pub async fn add_p2p(
        &self,
        file_hash: String,
        file_name: String,
        file_size: u64,
        from_peer: String,
    ) -> Result<String, DownloadManagerError> {
        let task_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();

        let task = DownloadTask {
            id: task_id.clone(),
            name: file_name.clone(),
            protocol: DownloadProtocol::P2P,
            size: file_size,
            downloaded: 0,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 0.0,
            save_path: self.data_dir.join("downloads"),
            created_at: now,
            updated_at: now,
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        self.tasks.lock().await.push(task.clone());
        self.persist_tasks().await;
        self.emit_event(TaskEvent::Added {
            task: TaskInfoEvent::from_task(&task),
        });

        // Store task params for potential resume
        {
            let mut info = self.task_info.lock().await;
            info.insert(
                task_id.clone(),
                TaskInfo {
                    params: TaskParams::P2P {
                        file_hash,
                        file_name,
                        file_size,
                        from_peer,
                    },
                },
            );
        }

        // Register as running (P2P downloads are managed externally via FileTransferManager)
        {
            let mut r = self.running.lock().await;
            let generation = {
                let mut gen_map = self.task_generation.lock().await;
                let g = gen_map.entry(task_id.clone()).or_insert(0);
                *g += 1;
                *g
            };
            r.insert(
                task_id.clone(),
                RunningTask {
                    cancel_token: CancellationToken::new(),
                    params: TaskParams::P2P {
                        file_hash: String::new(),
                        file_name: String::new(),
                        file_size: 0,
                        from_peer: String::new(),
                    },
                    started_at: std::time::Instant::now(),
                    last_downloaded: 0,
                    generation,
                    speed_samples: Vec::new(),
                    last_sample_time: std::time::Instant::now(),
                    last_progress_time: std::time::Instant::now(),
                    retry_count: 0,
                },
            );
        }

        Ok(task_id)
    }

    /// Update P2P download progress (called by P2P engine when chunks arrive)
    pub async fn update_p2p_progress(
        &self,
        task_id: &str,
        downloaded: u64,
        speed_bps: f64,
    ) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.downloaded = downloaded;
            task.speed_bps = speed_bps;
            task.updated_at = chrono::Utc::now();
            true
        } else {
            false
        }
    }

    /// Mark a P2P download as complete and save the assembled file
    pub async fn complete_p2p_download(
        &self,
        task_id: &str,
        data: Vec<u8>,
    ) -> Result<PathBuf, DownloadManagerError> {
        let save_path = {
            let tasks = self.tasks.lock().await;
            tasks
                .iter()
                .find(|t| t.id == task_id)
                .map(|t| t.save_path.clone())
                .ok_or_else(|| DownloadManagerError::TaskNotFound(task_id.to_string()))?
        };

        // Ensure save directory exists
        tokio::fs::create_dir_all(&save_path)
            .await
            .map_err(|e| DownloadManagerError::Io(e.to_string()))?;

        // Get file name from task
        let file_name = {
            let tasks = self.tasks.lock().await;
            tasks
                .iter()
                .find(|t| t.id == task_id)
                .map(|t| t.name.clone())
                .ok_or_else(|| DownloadManagerError::TaskNotFound(task_id.to_string()))?
        };

        let file_path = save_path.join(&file_name);

        // Write assembled file
        tokio::fs::write(&file_path, &data)
            .await
            .map_err(|e| DownloadManagerError::Io(e.to_string()))?;

        // Update task state
        {
            let mut tasks = self.tasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
                task.state = DownloadState::Complete;
                task.downloaded = task.size;
                task.speed_bps = 0.0;
                task.updated_at = chrono::Utc::now();
                Self::record_task_history(task, &self.data_dir, Some(&self.notifier));
            }
        }

        // Remove from running
        self.running.lock().await.remove(task_id);

        tracing::info!(
            task_id = %task_id,
            path = %file_path.display(),
            size = %data.len(),
            "P2P download complete, file saved"
        );

        Ok(file_path)
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
        // Check if we can start a new task
        if !self.can_start_task().await {
            // Mark as queued
            let mut tasks = self.tasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
                task.state = DownloadState::Queued;
                task.updated_at = chrono::Utc::now();
            }
            return;
        }

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
        let rate_limiter = Some(self.rate_limiter.clone());
        let task_complete_notify = self.task_complete_notify.clone();
        let notifier = self.notifier.clone();

        // Store task info for resume
        {
            let mut info = self.task_info.lock().await;
            info.insert(
                task_id.clone(),
                TaskInfo {
                    params: params.clone(),
                },
            );
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
                    last_progress_time: std::time::Instant::now(),
                    retry_count: 0,
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
                        Ok(data) => match torrent::TorrentMeta::from_bytes(&data) {
                            Ok(meta) => {
                                let download_dir = data_dir.join("downloads");
                                let mut engine = torrent::TorrentEngine::new(meta, download_dir);
                                // Apply rate limiting
                                if let Some(ref limiter) = rate_limiter {
                                    engine.set_rate_limiter(limiter.per_task().clone());
                                }
                                engine
                                    .download(Some(cancel_clone))
                                    .await
                                    .map_err(|e| e.to_string())
                            }
                            Err(e) => Err(e.to_string()),
                        },
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
                    // Apply rate limiting
                    if let Some(ref limiter) = rate_limiter {
                        engine.set_rate_limiter(limiter.per_task().clone());
                    }
                    engine
                        .download(Some(cancel_clone))
                        .await
                        .map_err(|e| e.to_string())
                }
                TaskParams::Xunlei {
                    file_name,
                    file_size,
                    sources,
                } => {
                    let download_dir = data_dir.join("downloads");
                    let mut engine =
                        xunlei::XunleiEngine::new(file_name, file_size, sources, download_dir);
                    // Apply rate limiting
                    if let Some(ref limiter) = rate_limiter {
                        engine.set_rate_limiter(limiter.per_task().clone());
                    }
                    engine
                        .download(Some(cancel_clone))
                        .await
                        .map_err(|e| e.to_string())
                }
                TaskParams::Magnet {
                    info_hash,
                    display_name,
                    trackers,
                } => {
                    // Magnet link handling: fetch metadata first, then download as torrent
                    let download_dir = data_dir.join("downloads");

                    // Step 1: Check metadata cache
                    let cache = metadata_cache::cache_dir();
                    let metadata_bytes = match metadata_cache::load_metadata(&cache, &info_hash) {
                        Ok(cached) => {
                            tracing::info!("Using cached metadata for magnet link");
                            cached
                        }
                        Err(metadata_cache::CacheError::NotFound) => {
                            // Step 2: Cache miss — fetch from peers via DHT
                            let peers =
                                dht.find_peers(info_hash).await.map_err(|e| e.to_string())?;

                            if peers.is_empty() {
                                return Err("No peers found via DHT".to_string());
                            }

                            // Step 3: Try to fetch metadata from peers (BEP 0009)
                            let bytes = match dht.fetch_metadata(info_hash).await {
                                Ok(b) => b,
                                Err(dht::DhtError::NotImplemented) => {
                                    return Err("Magnet link metadata exchange not yet implemented. Use .torrent files instead.".to_string());
                                }
                                Err(e) => return Err(format!("Failed to fetch metadata: {}", e)),
                            };

                            // Step 4: Cache the fetched metadata
                            if let Err(e) = metadata_cache::save_metadata(
                                &cache,
                                &info_hash,
                                &bytes,
                                display_name.as_deref(),
                                &trackers,
                            ) {
                                tracing::warn!(error = %e, "Failed to cache metadata");
                            }

                            bytes
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Failed to load cached metadata");
                            return Err(format!("Cache error: {e}"));
                        }
                    };

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
                            engine
                                .download(Some(cancel_clone))
                                .await
                                .map_err(|e| e.to_string())
                        }
                        Err(e) => Err(format!("Failed to parse metadata: {}", e)),
                    }
                }
                TaskParams::P2P { .. } => {
                    // P2P downloads are managed externally by FileTransferManager.
                    // This branch is only reached if resume_task is called on a P2P task,
                    // which is not yet supported (P2P resume requires peer reconnection).
                    Err("P2P resume not yet supported via DownloadManager".to_string())
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
                        Self::record_task_history(task, &data_dir, Some(&notifier));
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
                            Self::record_task_history(task, &data_dir, Some(&notifier));
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

            // Notify scheduler that a slot may have freed up
            task_complete_notify.notify_one();

            Ok(())
        });

        // Spawn speed tracker
        self.spawn_speed_tracker(task_id);
    }

    /// Periodically update speed for a running task using moving average
    fn spawn_speed_tracker(&self, task_id: String) {
        let tasks = self.tasks.clone();
        let running = self.running.clone();
        let task_info = self.task_info.clone();
        let task_generation = self.task_generation.clone();
        let data_dir = self.data_dir.clone();
        let dht = self.dht.clone();
        let rate_limiter = self.rate_limiter.clone();
        let timeout_secs = self.timeout_secs.load(Ordering::Relaxed);
        let max_retries = self.max_retries.load(Ordering::Relaxed);
        let notifier = self.notifier.clone();
        let bandwidth_monitor = self.bandwidth_monitor.clone();

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

                    // Track progress for timeout detection
                    if dd > 0.0 {
                        rt.last_progress_time = now;
                    }

                    // Add to samples (keep last 10)
                    rt.speed_samples.push(instant_speed);
                    if rt.speed_samples.len() > 10 {
                        rt.speed_samples.remove(0);
                    }

                    // Calculate weighted moving average (recent samples have more weight)
                    let avg_speed = if rt.speed_samples.is_empty() {
                        0.0
                    } else {
                        let weights: Vec<f64> =
                            (1..=rt.speed_samples.len()).map(|i| i as f64).collect();
                        let total_weight: f64 = weights.iter().sum();
                        let weighted_sum: f64 = rt
                            .speed_samples
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

                    // Update bandwidth monitor with aggregate speed
                    {
                        let all_tasks = tasks.lock().await;
                        let total_speed: f64 = all_tasks
                            .iter()
                            .filter(|t| t.state == DownloadState::Downloading)
                            .map(|t| t.speed_bps)
                            .sum();
                        bandwidth_monitor
                            .update_current_speed(total_speed, 0.0)
                            .await;
                    }

                    rt.last_downloaded = current_downloaded;
                    rt.last_sample_time = now;

                    // Check for timeout (only if enabled and task has started downloading)
                    if timeout_secs > 0 && current_downloaded > 0 {
                        let elapsed = now.duration_since(rt.last_progress_time).as_secs();
                        if elapsed >= timeout_secs && rt.retry_count < max_retries {
                            tracing::warn!(
                                task_id = %task_id,
                                elapsed_secs = elapsed,
                                retry_count = rt.retry_count,
                                "Download stalled, retrying..."
                            );

                            // Cancel current task
                            rt.cancel_token.cancel();
                            rt.retry_count += 1;

                            // Mark task as paused
                            if let Some(task) =
                                tasks.lock().await.iter_mut().find(|t| t.id == task_id)
                            {
                                task.state = DownloadState::Paused;
                                task.speed_bps = 0.0;
                                task.error =
                                    Some(format!("Timeout: no progress for {} seconds", elapsed));
                                task.updated_at = chrono::Utc::now();
                            }

                            // Save retry count before dropping the lock
                            let current_retry_count = rt.retry_count;

                            // Remove from running
                            drop(r);
                            running.lock().await.remove(&task_id);

                            // Re-spawn task after a short delay
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                            // Get stored params
                            let params = {
                                let info = task_info.lock().await;
                                info.get(&task_id).map(|ti| ti.params.clone())
                            };

                            if let Some(params) = params {
                                // Increment generation
                                let generation = {
                                    let mut gen_map = task_generation.lock().await;
                                    let g = gen_map.entry(task_id.clone()).or_insert(0);
                                    *g += 1;
                                    *g
                                };

                                let cancel_token = CancellationToken::new();
                                let cancel_clone = cancel_token.clone();
                                let notifier_clone = notifier.clone();

                                // Register new running task
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
                                            last_progress_time: std::time::Instant::now(),
                                            retry_count: current_retry_count,
                                        },
                                    );
                                }

                                // Mark as downloading
                                {
                                    let mut t = tasks.lock().await;
                                    if let Some(task) = t.iter_mut().find(|t| t.id == task_id) {
                                        task.state = DownloadState::Downloading;
                                        task.error = None;
                                        task.updated_at = chrono::Utc::now();
                                    }
                                }

                                // Spawn the actual download
                                let tasks_clone = tasks.clone();
                                let running_clone = running.clone();
                                let task_generation_clone = task_generation.clone();
                                let data_dir_clone = data_dir.clone();
                                let dht_clone = dht.clone();
                                let rate_limiter_clone = rate_limiter.clone();
                                let task_id_clone = task_id.clone();

                                tokio::spawn(async move {
                                    let result: Result<(), String> = match params {
                                        TaskParams::Torrent { torrent_path } => {
                                            match tokio::fs::read(&torrent_path).await {
                                                Ok(data) => {
                                                    match torrent::TorrentMeta::from_bytes(&data) {
                                                        Ok(meta) => {
                                                            let download_dir =
                                                                data_dir_clone.join("downloads");
                                                            let mut engine =
                                                                torrent::TorrentEngine::new(
                                                                    meta,
                                                                    download_dir,
                                                                );
                                                            engine.set_rate_limiter(
                                                                rate_limiter_clone
                                                                    .per_task()
                                                                    .clone(),
                                                            );
                                                            engine
                                                                .download(Some(cancel_clone))
                                                                .await
                                                                .map_err(|e| e.to_string())
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
                                            let download_dir = data_dir_clone.join("downloads");
                                            let mut engine = ed2k::Ed2kEngine::new(
                                                file_hash,
                                                file_size,
                                                file_name,
                                                download_dir,
                                                servers,
                                            );
                                            engine.set_rate_limiter(
                                                rate_limiter_clone.per_task().clone(),
                                            );
                                            engine
                                                .download(Some(cancel_clone))
                                                .await
                                                .map_err(|e| e.to_string())
                                        }
                                        TaskParams::Xunlei {
                                            file_name,
                                            file_size,
                                            sources,
                                        } => {
                                            let download_dir = data_dir_clone.join("downloads");
                                            let mut engine = xunlei::XunleiEngine::new(
                                                file_name,
                                                file_size,
                                                sources,
                                                download_dir,
                                            );
                                            engine.set_rate_limiter(
                                                rate_limiter_clone.per_task().clone(),
                                            );
                                            engine
                                                .download(Some(cancel_clone))
                                                .await
                                                .map_err(|e| e.to_string())
                                        }
                                        TaskParams::Magnet {
                                            info_hash,
                                            display_name,
                                            trackers,
                                        } => {
                                            let download_dir = data_dir_clone.join("downloads");
                                            let cache = metadata_cache::cache_dir();
                                            let metadata_bytes = match metadata_cache::load_metadata(
                                                &cache, &info_hash,
                                            ) {
                                                Ok(cached) => cached,
                                                Err(metadata_cache::CacheError::NotFound) => {
                                                    let peers = dht_clone
                                                        .find_peers(info_hash)
                                                        .await
                                                        .map_err(|e| e.to_string())?;
                                                    if peers.is_empty() {
                                                        return Err(
                                                            "No peers found via DHT".to_string()
                                                        );
                                                    }
                                                    let bytes = match dht_clone
                                                        .fetch_metadata(info_hash)
                                                        .await
                                                    {
                                                        Ok(b) => b,
                                                        Err(dht::DhtError::NotImplemented) => {
                                                            return Err("Magnet link metadata exchange not yet implemented".to_string());
                                                        }
                                                        Err(e) => {
                                                            return Err(format!(
                                                                "Failed to fetch metadata: {}",
                                                                e
                                                            ));
                                                        }
                                                    };
                                                    if let Err(e) = metadata_cache::save_metadata(
                                                        &cache,
                                                        &info_hash,
                                                        &bytes,
                                                        display_name.as_deref(),
                                                        &trackers,
                                                    ) {
                                                        tracing::warn!(error = %e, "Failed to cache metadata");
                                                    }
                                                    bytes
                                                }
                                                Err(e) => return Err(format!("Cache error: {e}")),
                                            };
                                            match torrent::TorrentMeta::from_bytes(&metadata_bytes)
                                            {
                                                Ok(meta) => {
                                                    {
                                                        let mut t = tasks_clone.lock().await;
                                                        if let Some(task) = t
                                                            .iter_mut()
                                                            .find(|t| t.id == task_id_clone)
                                                        {
                                                            task.name = meta.info.name.clone();
                                                            task.size = meta.total_size();
                                                        }
                                                    }
                                                    let mut engine = torrent::TorrentEngine::new(
                                                        meta,
                                                        download_dir,
                                                    );
                                                    engine.set_rate_limiter(
                                                        rate_limiter_clone.per_task().clone(),
                                                    );
                                                    engine
                                                        .download(Some(cancel_clone))
                                                        .await
                                                        .map_err(|e| e.to_string())
                                                }
                                                Err(e) => {
                                                    Err(format!("Failed to parse metadata: {}", e))
                                                }
                                            }
                                        }
                                        TaskParams::P2P { .. } => {
                                            Err("P2P resume not yet supported".to_string())
                                        }
                                    };

                                    // Update task state
                                    let my_generation = {
                                        let gen_map = task_generation_clone.lock().await;
                                        gen_map.get(&task_id_clone).copied().unwrap_or(0)
                                    };
                                    let is_still_active = {
                                        let r = running_clone.lock().await;
                                        r.get(&task_id_clone)
                                            .map(|rt| rt.generation == my_generation)
                                            .unwrap_or(false)
                                    };

                                    let mut t = tasks_clone.lock().await;
                                    if let Some(task) = t.iter_mut().find(|t| t.id == task_id_clone)
                                    {
                                        match result {
                                            Ok(()) => {
                                                task.state = DownloadState::Complete;
                                                task.downloaded = task.size;
                                                task.speed_bps = 0.0;
                                                Self::record_task_history(
                                                    task,
                                                    &data_dir_clone,
                                                    Some(&notifier_clone),
                                                );
                                            }
                                            Err(e) => {
                                                let err_str = e.to_string();
                                                if err_str == "cancelled" {
                                                    if is_still_active {
                                                        task.state = DownloadState::Paused;
                                                    }
                                                } else {
                                                    task.state = DownloadState::Error;
                                                    task.error = Some(err_str);
                                                    Self::record_task_history(
                                                        task,
                                                        &data_dir_clone,
                                                        Some(&notifier_clone),
                                                    );
                                                }
                                                task.speed_bps = 0.0;
                                            }
                                        }
                                        task.updated_at = chrono::Utc::now();
                                    }

                                    if is_still_active {
                                        running_clone.lock().await.remove(&task_id_clone);
                                    }

                                    Ok(())
                                });
                            }

                            break; // Exit the speed tracker loop
                        }
                    }
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

    /// List tasks with optional filtering and sorting.
    pub async fn list_tasks_filtered(
        &self,
        filter: TaskFilter,
        sort_by: Option<TaskSortBy>,
    ) -> Vec<DownloadTask> {
        let mut tasks: Vec<DownloadTask> = self
            .tasks
            .lock()
            .await
            .iter()
            .filter(|t| filter.matches(t))
            .cloned()
            .collect();
        if let Some(sort_by) = sort_by {
            sort_tasks(&mut tasks, sort_by);
        }
        tasks
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
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id)
            && (task.state == DownloadState::Downloading || task.state == DownloadState::Queued)
        {
            task.state = DownloadState::Paused;
            task.speed_bps = 0.0;
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });
            return true;
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
                self.emit_event(TaskEvent::Updated {
                    task: TaskInfoEvent::from_task(task),
                });
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
        let removed = tasks.len() < len_before;
        drop(tasks);
        if removed {
            self.persist_tasks().await;
            self.emit_event(TaskEvent::Removed {
                task_id: task_id.to_string(),
            });
        }
        removed
    }

    /// Emit a task event (public for testing only).
    #[cfg(test)]
    pub(crate) fn emit_event_for_test(&self, event: TaskEvent) {
        self.emit_event(event);
    }

    /// Get aggregated download statistics.
    pub async fn get_stats(&self) -> DownloadStats {
        let tasks = self.tasks.lock().await;
        let mut stats = DownloadStats {
            total_tasks: tasks.len(),
            ..Default::default()
        };

        for task in tasks.iter() {
            stats.total_downloaded += task.downloaded;
            stats.total_size += task.size;
            stats.total_speed_bps += task.speed_bps;

            match task.state {
                DownloadState::Downloading => stats.running += 1,
                DownloadState::Paused => stats.paused += 1,
                DownloadState::Complete => stats.completed += 1,
                DownloadState::Queued => stats.queued += 1,
                DownloadState::Error => stats.errored += 1,
            }

            match task.protocol {
                DownloadProtocol::Torrent => stats.by_protocol.torrent += 1,
                DownloadProtocol::Ed2k => stats.by_protocol.ed2k += 1,
                DownloadProtocol::Xunlei => stats.by_protocol.xunlei += 1,
                DownloadProtocol::Magnet => stats.by_protocol.magnet += 1,
                DownloadProtocol::P2P => stats.by_protocol.p2p += 1,
            }
        }

        stats
    }

    /// Pause all running downloads.
    pub async fn pause_all(&self) -> usize {
        let tasks = self.tasks.lock().await;
        let mut running_ids: Vec<String> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Downloading)
            .map(|t| t.id.clone())
            .collect();
        drop(tasks);

        let mut count = 0;
        for id in running_ids.drain(..) {
            if self.pause_task(&id).await {
                count += 1;
            }
        }
        count
    }

    /// Resume all paused downloads.
    pub async fn resume_all(&self) -> usize {
        let tasks = self.tasks.lock().await;
        let paused_ids: Vec<String> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Paused)
            .map(|t| t.id.clone())
            .collect();
        drop(tasks);

        let mut count = 0;
        for id in paused_ids {
            if self.resume_task(&id).await {
                count += 1;
            }
        }
        count
    }

    /// Remove all completed downloads.
    pub async fn remove_completed(&self) -> usize {
        let tasks = self.tasks.lock().await;
        let completed_ids: Vec<String> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Complete)
            .map(|t| t.id.clone())
            .collect();
        drop(tasks);

        let mut count = 0;
        for id in completed_ids {
            if self.remove_task(&id).await {
                count += 1;
            }
        }
        count
    }

    /// Remove all failed downloads.
    pub async fn remove_failed(&self) -> usize {
        let tasks = self.tasks.lock().await;
        let failed_ids: Vec<String> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Error)
            .map(|t| t.id.clone())
            .collect();
        drop(tasks);

        let mut count = 0;
        for id in failed_ids {
            if self.remove_task(&id).await {
                count += 1;
            }
        }
        count
    }

    /// Add tags to a download task
    pub async fn add_tags(&self, task_id: &str, tags: Vec<String>) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            for tag in tags {
                let tag = tag.trim().to_lowercase();
                if !tag.is_empty() && !task.tags.contains(&tag) {
                    task.tags.push(tag);
                }
            }
            task.tags.sort();
            task.tags.dedup();
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });
            drop(tasks);
            self.persist_tasks().await;
            true
        } else {
            false
        }
    }

    /// Remove tags from a download task
    pub async fn remove_tags(&self, task_id: &str, tags: Vec<String>) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            let tags_to_remove: Vec<String> =
                tags.iter().map(|t| t.trim().to_lowercase()).collect();
            task.tags.retain(|t| !tags_to_remove.contains(t));
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });
            drop(tasks);
            self.persist_tasks().await;
            true
        } else {
            false
        }
    }

    /// List all tasks with a specific tag
    pub async fn list_tasks_by_tag(&self, tag: &str) -> Vec<DownloadTask> {
        let tag_lower = tag.to_lowercase();
        self.tasks
            .lock()
            .await
            .iter()
            .filter(|t| t.tags.contains(&tag_lower))
            .cloned()
            .collect()
    }

    /// Get all unique tags across all tasks
    pub async fn list_all_tags(&self) -> Vec<String> {
        let tasks = self.tasks.lock().await;
        let mut tags: Vec<String> = tasks.iter().flat_map(|t| t.tags.iter().cloned()).collect();
        tags.sort();
        tags.dedup();
        tags
    }

    /// Set the priority of a download task.
    /// Higher priority tasks are spawned first when concurrent limits are active.
    pub async fn set_priority(&self, task_id: &str, priority: DownloadPriority) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.priority = priority;
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });
            drop(tasks);
            self.persist_tasks().await;
            true
        } else {
            false
        }
    }

    /// Try to start the next queued task if a slot is available.
    /// Picks the highest-priority queued task (FIFO within same priority).
    pub async fn try_start_next_queued(&self) -> Option<String> {
        if !self.can_start_task().await {
            return None;
        }

        // Find the highest-priority queued task
        let tasks = self.tasks.lock().await;
        let next = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Queued)
            .max_by_key(|t| t.priority)
            .map(|t| t.id.clone());
        drop(tasks);

        if let Some(ref task_id) = next {
            // Get stored params
            let info = self.task_info.lock().await;
            if let Some(task_info) = info.get(task_id) {
                let params = task_info.params.clone();
                drop(info);
                self.spawn_task(task_id.clone(), params).await;
                return Some(task_id.clone());
            }
        }
        None
    }

    /// Persist current task list to disk (fire-and-forget)
    async fn persist_tasks(&self) {
        let tasks = self.tasks.lock().await.clone();
        let data_dir = self.data_dir.clone();
        // Spawn to avoid blocking the caller on disk I/O
        tokio::spawn(async move {
            if let Err(e) = save_task_queue(&tasks, &data_dir) {
                tracing::warn!(error = %e, "Failed to persist task queue");
            }
        });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_timeout_default_disabled() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test"));
        assert_eq!(dm.timeout_secs.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_set_timeout_secs() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test"));
        dm.set_timeout_secs(30);
        assert_eq!(dm.timeout_secs.load(Ordering::Relaxed), 30);
    }

    #[tokio::test]
    async fn test_set_max_retries() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test"));
        assert_eq!(dm.max_retries.load(Ordering::Relaxed), 3);
        dm.set_max_retries(5);
        assert_eq!(dm.max_retries.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn test_running_task_has_timeout_fields() {
        let rt = RunningTask {
            cancel_token: CancellationToken::new(),
            params: TaskParams::P2P {
                file_hash: String::new(),
                file_name: String::new(),
                file_size: 0,
                from_peer: String::new(),
            },
            started_at: std::time::Instant::now(),
            last_downloaded: 0,
            generation: 1,
            speed_samples: Vec::new(),
            last_sample_time: std::time::Instant::now(),
            last_progress_time: std::time::Instant::now(),
            retry_count: 0,
        };
        assert_eq!(rt.retry_count, 0);
    }

    #[tokio::test]
    async fn test_get_stats_empty() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_stats"));
        let stats = dm.get_stats().await;
        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.running, 0);
        assert_eq!(stats.paused, 0);
        assert_eq!(stats.completed, 0);
        assert_eq!(stats.queued, 0);
        assert_eq!(stats.errored, 0);
        assert_eq!(stats.total_downloaded, 0);
        assert_eq!(stats.total_size, 0);
        assert_eq!(stats.total_speed_bps, 0.0);
    }

    #[tokio::test]
    async fn test_get_stats_with_tasks() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_stats2"));

        // Manually add some tasks to test stats aggregation
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "t1".into(),
                name: "file1.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 500,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 100.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t2".into(),
                name: "file2.txt".into(),
                protocol: DownloadProtocol::Ed2k,
                size: 2000,
                downloaded: 2000,
                state: DownloadState::Complete,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t3".into(),
                name: "file3.txt".into(),
                protocol: DownloadProtocol::Xunlei,
                size: 500,
                downloaded: 100,
                state: DownloadState::Paused,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        let stats = dm.get_stats().await;
        assert_eq!(stats.total_tasks, 3);
        assert_eq!(stats.running, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.paused, 1);
        assert_eq!(stats.total_downloaded, 2600);
        assert_eq!(stats.total_size, 3500);
        assert_eq!(stats.total_speed_bps, 100.0);
        assert_eq!(stats.by_protocol.torrent, 1);
        assert_eq!(stats.by_protocol.ed2k, 1);
        assert_eq!(stats.by_protocol.xunlei, 1);
    }

    #[tokio::test]
    async fn test_pause_all_no_running() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_pause_all"));
        let count = dm.pause_all().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_resume_all_no_paused() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_resume_all"));
        let count = dm.resume_all().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_remove_completed_empty() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_rm_completed"));
        let count = dm.remove_completed().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_remove_failed_empty() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_rm_failed"));
        let count = dm.remove_failed().await;
        assert_eq!(count, 0);
    }

    #[test]
    fn test_download_stats_default() {
        let stats = DownloadStats::default();
        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.running, 0);
        assert_eq!(stats.paused, 0);
        assert_eq!(stats.completed, 0);
        assert_eq!(stats.queued, 0);
        assert_eq!(stats.errored, 0);
        assert_eq!(stats.total_downloaded, 0);
        assert_eq!(stats.total_size, 0);
        assert_eq!(stats.total_speed_bps, 0.0);
    }

    #[test]
    fn test_download_stats_serialization() {
        let stats = DownloadStats {
            total_tasks: 5,
            running: 2,
            paused: 1,
            completed: 1,
            queued: 0,
            errored: 1,
            total_downloaded: 1024,
            total_size: 2048,
            total_speed_bps: 500.0,
            by_protocol: ProtocolStats {
                torrent: 2,
                ed2k: 1,
                xunlei: 1,
                magnet: 0,
                p2p: 1,
            },
        };
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: DownloadStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_tasks, 5);
        assert_eq!(deserialized.running, 2);
        assert_eq!(deserialized.by_protocol.torrent, 2);
        assert_eq!(deserialized.by_protocol.p2p, 1);
    }

    #[tokio::test]
    async fn test_add_tags_to_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_add_tags"));

        // Add a task manually
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "tag-test-1".into(),
                name: "file.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        // Add tags
        let success = dm
            .add_tags("tag-test-1", vec!["movies".into(), "action".into()])
            .await;
        assert!(success);

        // Verify tags were added
        let task = dm.get_task("tag-test-1").await.unwrap();
        assert_eq!(task.tags, vec!["action", "movies"]); // sorted
    }

    #[tokio::test]
    async fn test_add_tags_nonexistent_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_add_tags_nonexist"));
        let success = dm.add_tags("nonexistent", vec!["tag".into()]).await;
        assert!(!success);
    }

    #[tokio::test]
    async fn test_remove_tags_from_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_remove_tags"));

        // Add a task with tags
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "tag-test-2".into(),
                name: "file.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: vec!["movies".into(), "action".into(), "drama".into()],
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        // Remove some tags
        let success = dm
            .remove_tags("tag-test-2", vec!["action".into(), "drama".into()])
            .await;
        assert!(success);

        // Verify tags were removed
        let task = dm.get_task("tag-test-2").await.unwrap();
        assert_eq!(task.tags, vec!["movies"]);
    }

    #[tokio::test]
    async fn test_list_tasks_by_tag() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_list_by_tag"));

        // Add tasks with different tags
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "t1".into(),
                name: "file1.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: vec!["movies".into(), "action".into()],
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t2".into(),
                name: "file2.txt".into(),
                protocol: DownloadProtocol::Ed2k,
                size: 2000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: vec!["movies".into(), "drama".into()],
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t3".into(),
                name: "file3.txt".into(),
                protocol: DownloadProtocol::Xunlei,
                size: 500,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: vec!["work".into()],
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        // Filter by "movies" tag
        let movies = dm.list_tasks_by_tag("movies").await;
        assert_eq!(movies.len(), 2);

        // Filter by "work" tag
        let work = dm.list_tasks_by_tag("work").await;
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].id, "t3");

        // Filter by non-existent tag
        let none = dm.list_tasks_by_tag("nonexistent").await;
        assert_eq!(none.len(), 0);
    }

    #[tokio::test]
    async fn test_list_all_tags() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_all_tags"));

        // Add tasks with tags
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "t1".into(),
                name: "file1.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: vec!["movies".into(), "action".into()],
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t2".into(),
                name: "file2.txt".into(),
                protocol: DownloadProtocol::Ed2k,
                size: 2000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: vec!["movies".into(), "drama".into()],
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        let tags = dm.list_all_tags().await;
        assert_eq!(tags, vec!["action", "drama", "movies"]); // sorted, deduped
    }

    #[tokio::test]
    async fn test_add_duplicate_tags() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_dup_tags"));

        // Add a task
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "dup-tag-1".into(),
                name: "file.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: vec!["movies".into()],
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        // Add duplicate tag
        let success = dm.add_tags("dup-tag-1", vec!["movies".into()]).await;
        assert!(success);

        // Verify no duplicate was added
        let task = dm.get_task("dup-tag-1").await.unwrap();
        assert_eq!(task.tags, vec!["movies"]);
    }

    #[test]
    fn test_task_with_tags_default_empty() {
        let task = DownloadTask {
            id: "test".into(),
            name: "file.txt".into(),
            protocol: DownloadProtocol::Torrent,
            size: 1000,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: std::path::PathBuf::from("/tmp"),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };
        assert!(task.tags.is_empty());
    }

    // ── Phase 15: TaskFilter & TaskSortBy tests ──

    #[test]
    fn test_task_filter_matches_query() {
        let task = DownloadTask {
            id: "f1".into(),
            name: "Ubuntu 24.04.iso".into(),
            protocol: DownloadProtocol::Torrent,
            size: 4_000_000_000,
            downloaded: 1_000_000_000,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 500_000.0,
            save_path: std::path::PathBuf::from("/tmp"),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: vec!["linux".into()],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        // Query match (case-insensitive)
        let filter = TaskFilter {
            query: Some("ubuntu".into()),
            ..Default::default()
        };
        assert!(filter.matches(&task));

        let filter = TaskFilter {
            query: Some("UBUNTU".into()),
            ..Default::default()
        };
        assert!(filter.matches(&task));

        // Query no match
        let filter = TaskFilter {
            query: Some("debian".into()),
            ..Default::default()
        };
        assert!(!filter.matches(&task));
    }

    #[test]
    fn test_task_filter_matches_state() {
        let task = DownloadTask {
            id: "f2".into(),
            name: "file.txt".into(),
            protocol: DownloadProtocol::Ed2k,
            size: 1000,
            downloaded: 500,
            state: DownloadState::Paused,
            error: None,
            speed_bps: 0.0,
            save_path: std::path::PathBuf::from("/tmp"),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        let filter = TaskFilter {
            state: Some(DownloadState::Paused),
            ..Default::default()
        };
        assert!(filter.matches(&task));

        let filter = TaskFilter {
            state: Some(DownloadState::Downloading),
            ..Default::default()
        };
        assert!(!filter.matches(&task));
    }

    #[test]
    fn test_task_filter_matches_protocol() {
        let task = DownloadTask {
            id: "f3".into(),
            name: "file.txt".into(),
            protocol: DownloadProtocol::Xunlei,
            size: 1000,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: std::path::PathBuf::from("/tmp"),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        let filter = TaskFilter {
            protocol: Some(DownloadProtocol::Xunlei),
            ..Default::default()
        };
        assert!(filter.matches(&task));

        let filter = TaskFilter {
            protocol: Some(DownloadProtocol::Torrent),
            ..Default::default()
        };
        assert!(!filter.matches(&task));
    }

    #[test]
    fn test_task_filter_matches_tag() {
        let task = DownloadTask {
            id: "f4".into(),
            name: "movie.mkv".into(),
            protocol: DownloadProtocol::Torrent,
            size: 2_000_000_000,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: std::path::PathBuf::from("/tmp"),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: vec!["movies".into(), "action".into()],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        let filter = TaskFilter {
            tag: Some("movies".into()),
            ..Default::default()
        };
        assert!(filter.matches(&task));

        let filter = TaskFilter {
            tag: Some("work".into()),
            ..Default::default()
        };
        assert!(!filter.matches(&task));
    }

    #[test]
    fn test_task_filter_combined() {
        let task = DownloadTask {
            id: "f5".into(),
            name: "linux.iso".into(),
            protocol: DownloadProtocol::Torrent,
            size: 1000,
            downloaded: 500,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 100.0,
            save_path: std::path::PathBuf::from("/tmp"),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: vec!["linux".into()],
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
        };

        // All criteria match
        let filter = TaskFilter {
            query: Some("linux".into()),
            state: Some(DownloadState::Downloading),
            protocol: Some(DownloadProtocol::Torrent),
            tag: Some("linux".into()),
        };
        assert!(filter.matches(&task));

        // Query matches but state doesn't
        let filter = TaskFilter {
            query: Some("linux".into()),
            state: Some(DownloadState::Paused),
            protocol: Some(DownloadProtocol::Torrent),
            tag: Some("linux".into()),
        };
        assert!(!filter.matches(&task));
    }

    #[test]
    fn test_sort_tasks_by_name() {
        let mut tasks = vec![
            DownloadTask {
                id: "s1".into(),
                name: "Zebra.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 100,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
            DownloadTask {
                id: "s2".into(),
                name: "apple.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 200,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
            DownloadTask {
                id: "s3".into(),
                name: "Mango.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 300,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
        ];

        sort_tasks(&mut tasks, TaskSortBy::NameAsc);
        assert_eq!(tasks[0].name, "apple.txt");
        assert_eq!(tasks[1].name, "Mango.txt");
        assert_eq!(tasks[2].name, "Zebra.txt");

        sort_tasks(&mut tasks, TaskSortBy::NameDesc);
        assert_eq!(tasks[0].name, "Zebra.txt");
        assert_eq!(tasks[2].name, "apple.txt");
    }

    #[test]
    fn test_sort_tasks_by_size() {
        let mut tasks = vec![
            DownloadTask {
                id: "s1".into(),
                name: "small".into(),
                protocol: DownloadProtocol::Torrent,
                size: 100,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
            DownloadTask {
                id: "s2".into(),
                name: "big".into(),
                protocol: DownloadProtocol::Torrent,
                size: 9000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
            DownloadTask {
                id: "s3".into(),
                name: "medium".into(),
                protocol: DownloadProtocol::Torrent,
                size: 500,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
        ];

        sort_tasks(&mut tasks, TaskSortBy::SizeDesc);
        assert_eq!(tasks[0].size, 9000);
        assert_eq!(tasks[1].size, 500);
        assert_eq!(tasks[2].size, 100);
    }

    #[test]
    fn test_sort_tasks_by_progress() {
        let mut tasks = vec![
            DownloadTask {
                id: "p1".into(),
                name: "a".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 100, // 10%
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
            DownloadTask {
                id: "p2".into(),
                name: "b".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 900, // 90%
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
            DownloadTask {
                id: "p3".into(),
                name: "c".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 500, // 50%
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            },
        ];

        sort_tasks(&mut tasks, TaskSortBy::ProgressDesc);
        assert_eq!(tasks[0].id, "p2");
        assert_eq!(tasks[1].id, "p3");
        assert_eq!(tasks[2].id, "p1");
    }

    #[tokio::test]
    async fn test_list_tasks_filtered_no_filter() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_filtered_empty"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "t1".into(),
                name: "file1.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t2".into(),
                name: "file2.txt".into(),
                protocol: DownloadProtocol::Ed2k,
                size: 2000,
                downloaded: 1000,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 100.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        let all = dm.list_tasks_filtered(TaskFilter::default(), None).await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_list_tasks_filtered_by_query() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_filtered_query"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "t1".into(),
                name: "Ubuntu.iso".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t2".into(),
                name: "Fedora.iso".into(),
                protocol: DownloadProtocol::Torrent,
                size: 2000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t3".into(),
                name: "Debian.iso".into(),
                protocol: DownloadProtocol::Torrent,
                size: 500,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        let filter = TaskFilter {
            query: Some("ubuntu".into()),
            ..Default::default()
        };
        let result = dm.list_tasks_filtered(filter, None).await;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Ubuntu.iso");
    }

    #[tokio::test]
    async fn test_list_tasks_filtered_by_state_and_sorted() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_filtered_state"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "t1".into(),
                name: "big.iso".into(),
                protocol: DownloadProtocol::Torrent,
                size: 5000,
                downloaded: 0,
                state: DownloadState::Paused,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t2".into(),
                name: "small.iso".into(),
                protocol: DownloadProtocol::Ed2k,
                size: 100,
                downloaded: 100,
                state: DownloadState::Complete,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "t3".into(),
                name: "medium.iso".into(),
                protocol: DownloadProtocol::Xunlei,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Paused,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        // Filter by Paused, sort by size descending
        let filter = TaskFilter {
            state: Some(DownloadState::Paused),
            ..Default::default()
        };
        let result = dm
            .list_tasks_filtered(filter, Some(TaskSortBy::SizeDesc))
            .await;
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "big.iso");
        assert_eq!(result[1].name, "medium.iso");
    }

    #[test]
    fn test_download_priority_ordering() {
        assert!(DownloadPriority::High > DownloadPriority::Normal);
        assert!(DownloadPriority::Normal > DownloadPriority::Low);
        assert!(DownloadPriority::High > DownloadPriority::Low);
    }

    #[test]
    fn test_download_priority_from_str() {
        assert_eq!(
            DownloadPriority::from_str_opt("high"),
            Some(DownloadPriority::High)
        );
        assert_eq!(
            DownloadPriority::from_str_opt("HIGH"),
            Some(DownloadPriority::High)
        );
        assert_eq!(
            DownloadPriority::from_str_opt("h"),
            Some(DownloadPriority::High)
        );
        assert_eq!(
            DownloadPriority::from_str_opt("2"),
            Some(DownloadPriority::High)
        );

        assert_eq!(
            DownloadPriority::from_str_opt("normal"),
            Some(DownloadPriority::Normal)
        );
        assert_eq!(
            DownloadPriority::from_str_opt("n"),
            Some(DownloadPriority::Normal)
        );
        assert_eq!(
            DownloadPriority::from_str_opt("1"),
            Some(DownloadPriority::Normal)
        );
        assert_eq!(
            DownloadPriority::from_str_opt("default"),
            Some(DownloadPriority::Normal)
        );

        assert_eq!(
            DownloadPriority::from_str_opt("low"),
            Some(DownloadPriority::Low)
        );
        assert_eq!(
            DownloadPriority::from_str_opt("l"),
            Some(DownloadPriority::Low)
        );
        assert_eq!(
            DownloadPriority::from_str_opt("0"),
            Some(DownloadPriority::Low)
        );

        assert_eq!(DownloadPriority::from_str_opt("invalid"), None);
    }

    #[test]
    fn test_download_priority_label() {
        assert_eq!(DownloadPriority::High.label(), "high");
        assert_eq!(DownloadPriority::Normal.label(), "normal");
        assert_eq!(DownloadPriority::Low.label(), "low");
    }

    #[test]
    fn test_download_priority_default() {
        let priority = DownloadPriority::default();
        assert_eq!(priority, DownloadPriority::Normal);
    }

    #[tokio::test]
    async fn test_set_priority() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_priority"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "t1".into(),
                name: "file.iso".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        // Set priority to High
        assert!(dm.set_priority("t1", DownloadPriority::High).await);

        let task = dm.get_task("t1").await.unwrap();
        assert_eq!(task.priority, DownloadPriority::High);

        // Set priority to Low
        assert!(dm.set_priority("t1", DownloadPriority::Low).await);

        let task = dm.get_task("t1").await.unwrap();
        assert_eq!(task.priority, DownloadPriority::Low);

        // Non-existent task
        assert!(!dm.set_priority("nonexistent", DownloadPriority::High).await);
    }

    #[tokio::test]
    async fn test_priority_in_task_info_event() {
        let task = DownloadTask {
            id: "t1".into(),
            name: "file.iso".into(),
            protocol: DownloadProtocol::Torrent,
            size: 1000,
            downloaded: 500,
            state: DownloadState::Downloading,
            error: None,
            speed_bps: 100.0,
            save_path: std::path::PathBuf::from("/tmp"),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: vec!["test".into()],
            priority: DownloadPriority::High,
            schedule: None,
            bandwidth_weight: 1,
        };

        let event = TaskInfoEvent::from_task(&task);
        assert_eq!(event.priority, "high");

        let task_low = DownloadTask {
            priority: DownloadPriority::Low,
            ..task
        };
        let event_low = TaskInfoEvent::from_task(&task_low);
        assert_eq!(event_low.priority, "low");
    }

    #[test]
    fn test_time_window_new_valid() {
        let window = TimeWindow::new(9, 0, 17, 30);
        assert!(window.is_some());
        let window = window.unwrap();
        assert_eq!(window.start_hour, 9);
        assert_eq!(window.start_minute, 0);
        assert_eq!(window.end_hour, 17);
        assert_eq!(window.end_minute, 30);
    }

    #[test]
    fn test_time_window_new_invalid() {
        // Invalid hour
        assert!(TimeWindow::new(24, 0, 17, 0).is_none());
        assert!(TimeWindow::new(9, 0, 25, 0).is_none());
        // Invalid minute
        assert!(TimeWindow::new(9, 60, 17, 0).is_none());
        assert!(TimeWindow::new(9, 0, 17, 60).is_none());
    }

    #[test]
    fn test_time_window_format() {
        let window = TimeWindow::new(9, 0, 17, 30).unwrap();
        assert_eq!(window.format(), "09:00-17:30");

        let window = TimeWindow::new(22, 15, 6, 45).unwrap();
        assert_eq!(window.format(), "22:15-06:45");
    }

    #[test]
    fn test_time_window_is_active_normal() {
        // Normal window: 09:00 - 17:00
        let window = TimeWindow::new(9, 0, 17, 0).unwrap();

        // Before window
        let before = chrono::Local::now()
            .date_naive()
            .and_hms_opt(8, 30, 0)
            .unwrap()
            .and_local_timezone(chrono::Local)
            .unwrap();
        assert!(!window.is_active_at(before));

        // Inside window
        let inside = chrono::Local::now()
            .date_naive()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_local_timezone(chrono::Local)
            .unwrap();
        assert!(window.is_active_at(inside));

        // After window
        let after = chrono::Local::now()
            .date_naive()
            .and_hms_opt(18, 0, 0)
            .unwrap()
            .and_local_timezone(chrono::Local)
            .unwrap();
        assert!(!window.is_active_at(after));
    }

    #[test]
    fn test_time_window_is_active_overnight() {
        // Overnight window: 22:00 - 06:00
        let window = TimeWindow::new(22, 0, 6, 0).unwrap();

        // Inside window (late night)
        let late_night = chrono::Local::now()
            .date_naive()
            .and_hms_opt(23, 30, 0)
            .unwrap()
            .and_local_timezone(chrono::Local)
            .unwrap();
        assert!(window.is_active_at(late_night));

        // Inside window (early morning)
        let early_morning = chrono::Local::now()
            .date_naive()
            .and_hms_opt(3, 0, 0)
            .unwrap()
            .and_local_timezone(chrono::Local)
            .unwrap();
        assert!(window.is_active_at(early_morning));

        // Outside window (afternoon)
        let afternoon = chrono::Local::now()
            .date_naive()
            .and_hms_opt(14, 0, 0)
            .unwrap()
            .and_local_timezone(chrono::Local)
            .unwrap();
        assert!(!window.is_active_at(afternoon));
    }

    #[tokio::test]
    async fn test_set_bandwidth_weight() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_bandwidth"));

        // Add a task manually
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "bw-test-1".into(),
                name: "file.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        // Test setting valid weight
        let ok = dm.set_bandwidth_weight("bw-test-1", 5).await;
        assert!(ok);

        // Verify weight was set
        let weight = dm.get_bandwidth_weight("bw-test-1").await;
        assert_eq!(weight, Some(5));

        // Test clamping: weight > 10 should be clamped to 10
        let ok = dm.set_bandwidth_weight("bw-test-1", 15).await;
        assert!(ok);
        let weight = dm.get_bandwidth_weight("bw-test-1").await;
        assert_eq!(weight, Some(10));

        // Test clamping: weight < 1 should be clamped to 1
        let ok = dm.set_bandwidth_weight("bw-test-1", 0).await;
        assert!(ok);
        let weight = dm.get_bandwidth_weight("bw-test-1").await;
        assert_eq!(weight, Some(1));

        // Test non-existent task
        let ok = dm.set_bandwidth_weight("non-existent", 5).await;
        assert!(!ok);
    }

    #[tokio::test]
    async fn test_bandwidth_allocation_single_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_bandwidth_alloc"));

        // Set global limit to 1000 bytes/sec
        dm.set_global_speed_limit(1000).await;

        // Add a single downloading task with weight 1
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "alloc-test-1".into(),
                name: "file1.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        let allocation = dm.calculate_bandwidth_allocation().await;
        assert_eq!(allocation.len(), 1);
        // Single task gets all bandwidth (at least 1000)
        assert!(allocation.get("alloc-test-1").unwrap() >= &1000);
    }

    #[tokio::test]
    async fn test_bandwidth_allocation_proportional() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_bandwidth_prop"));

        // Set global limit to 10000 bytes/sec
        dm.set_global_speed_limit(10000).await;

        // Add three downloading tasks with different weights
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "prop-1".into(),
                name: "file1.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
            tasks.push(DownloadTask {
                id: "prop-2".into(),
                name: "file2.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 2,
            });
            tasks.push(DownloadTask {
                id: "prop-3".into(),
                name: "file3.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 3,
            });
        }

        let allocation = dm.calculate_bandwidth_allocation().await;
        assert_eq!(allocation.len(), 3);

        // Total weight is 1+2+3 = 6
        // prop-1: 1/6 * 10000 = 1666
        // prop-2: 2/6 * 10000 = 3333
        // prop-3: 3/6 * 10000 = 5000
        let alloc1 = *allocation.get("prop-1").unwrap();
        let alloc2 = *allocation.get("prop-2").unwrap();
        let alloc3 = *allocation.get("prop-3").unwrap();

        // Verify proportional distribution (with some tolerance)
        assert!(
            alloc1 >= 1000 && alloc1 <= 2000,
            "alloc1={} expected ~1666",
            alloc1
        );
        assert!(
            alloc2 >= 3000 && alloc2 <= 4000,
            "alloc2={} expected ~3333",
            alloc2
        );
        assert!(
            alloc3 >= 4500 && alloc3 <= 5500,
            "alloc3={} expected ~5000",
            alloc3
        );

        // Verify prop-3 gets more than prop-2, which gets more than prop-1
        assert!(alloc3 > alloc2);
        assert!(alloc2 > alloc1);
    }

    #[tokio::test]
    async fn test_bandwidth_allocation_no_global_limit() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_bandwidth_nolimit"));

        // No global limit set (default 0)

        // Add a downloading task
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "nolimit-1".into(),
                name: "file1.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Downloading,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        let allocation = dm.calculate_bandwidth_allocation().await;
        // No global limit means no allocation needed
        assert!(allocation.is_empty());
    }

    #[tokio::test]
    async fn test_bandwidth_allocation_no_downloading_tasks() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_bandwidth_empty"));

        // Set global limit
        dm.set_global_speed_limit(10000).await;

        // Add only queued/paused tasks (no downloading)
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "queued-1".into(),
                name: "file1.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        let allocation = dm.calculate_bandwidth_allocation().await;
        // No downloading tasks means no allocation
        assert!(allocation.is_empty());
    }

    #[tokio::test]
    async fn test_set_schedule() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_schedule"));

        // Add a task manually
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "schedule-test-1".into(),
                name: "file.txt".into(),
                protocol: DownloadProtocol::Torrent,
                size: 1000,
                downloaded: 0,
                state: DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from("/tmp"),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: Vec::new(),
                priority: DownloadPriority::Normal,
                schedule: None,
                bandwidth_weight: 1,
            });
        }

        // Set a schedule
        let window = TimeWindow::new(9, 0, 17, 0).unwrap();
        let success = dm.set_schedule("schedule-test-1", Some(window)).await;
        assert!(success);

        // Verify schedule was set
        let schedule = dm.get_schedule("schedule-test-1").await;
        assert!(schedule.is_some());
        let schedule = schedule.unwrap();
        assert!(schedule.is_some());
        let window = schedule.unwrap();
        assert_eq!(window.start_hour, 9);
        assert_eq!(window.end_hour, 17);

        // Remove schedule
        let success = dm.set_schedule("schedule-test-1", None).await;
        assert!(success);

        // Verify schedule was removed
        let schedule = dm.get_schedule("schedule-test-1").await;
        assert!(schedule.is_some());
        assert!(schedule.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_set_schedule_nonexistent_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_schedule_nonexist"));
        let window = TimeWindow::new(9, 0, 17, 0).unwrap();
        let success = dm.set_schedule("nonexistent", Some(window)).await;
        assert!(!success);
    }

    #[tokio::test]
    async fn test_get_schedule_nonexistent_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_get_schedule_nonexist"));
        let schedule = dm.get_schedule("nonexistent").await;
        assert!(schedule.is_none());
    }
}
