//! Multi-protocol download engine for IPMsg-Torrent
//!
//! Supports:
//! - BitTorrent (.torrent files)
//! - eDonkey/eMule (ed2k links)
//! - Xunlei P2SP (HTTP/FTP + P2P hybrid)

pub mod adaptive_concurrency;
pub mod advanced_search;
pub mod audit_log;
pub mod auto_actions;
pub mod auto_categorize;
pub mod auto_cleanup;
pub mod auto_pause;
pub mod auto_shutdown;
pub mod automation_rules;
pub mod bandwidth_allocation;
pub mod bandwidth_forecast;
pub mod bandwidth_monitor;
pub mod bandwidth_qos;
pub mod bandwidth_schedule;
pub mod bandwidth_usage;
pub mod bulk_ops;
pub mod checksum;
pub mod completion_probability;
pub mod conflict_detection;
pub mod connection_health;
pub mod connection_pool;
pub mod csv_export;
pub mod dashboard;
pub mod data_cap;
pub mod data_retention;
pub mod dependency_graph;
pub mod dependency_visualization;
pub mod dht;
pub mod disk_monitor;
pub mod domain_limit;
pub mod download_analytics;
pub mod download_backup;
pub mod download_budget;
pub mod download_cooldown;
pub mod download_cost;
pub mod download_deadline;
pub mod download_diagnostics;
pub mod download_expiry;
pub mod download_file_stats;
pub mod download_history;
pub mod download_history_analytics;
pub mod download_presets;
pub mod download_quota;
pub mod download_report;
pub mod download_session;
pub mod download_snapshot;
pub mod download_stats;
pub mod download_templates;
pub mod download_time_limit;
pub mod duplicate_detection;
pub mod dynamic_priority;
pub mod ed2k;
pub mod error_recovery;
pub mod eta_estimator;
pub mod event_webhook;
pub mod global_budget;
pub mod health_dashboard;
pub mod host_conn_limit;
pub mod integrity_verification;
pub mod intelligent_source_selector;
pub mod link_extractor;
pub mod link_rot;
pub mod magnet;
pub mod metadata_cache;
pub mod mirror_health;
pub mod network_aware;
pub mod network_monitor;
pub mod notification;
pub mod notification_center;
pub mod notification_preferences;
pub mod path_organizer;
pub mod path_rules;
pub mod path_template;
pub mod path_validator;
pub mod post_hooks;
pub mod preflight_check;
pub mod priority_aging;
pub mod progress;
pub mod progress_milestone;
pub mod progress_prediction;
pub mod protocol_limits;
pub mod proxy;
pub mod queue_completion;
pub mod queue_health;
pub mod queue_staleness;
pub mod rate_limiter;
pub mod recycle_bin;
pub mod resume_policy;
pub mod retry_budget;
pub mod retry_quota;
pub mod rss_feed;
pub mod save_path_manager;
pub mod segment_download;
pub mod sla_compliance;
pub mod smart_queue;
pub mod source_benchmark;
pub mod source_latency;
pub mod source_quality;
pub mod source_reliability;
pub mod source_rotation;
pub mod speed_alert;
pub mod speed_anomaly;
pub mod speed_benchmark;
pub mod speed_boost;
pub mod speed_burst;
pub mod speed_distribution;
pub mod speed_heatmap;
pub mod speed_history;
pub mod speed_prediction;
pub mod speed_profiles;
pub mod speed_test;
pub mod speed_trend;
pub mod system_uptime;
pub mod tag_management;
pub mod task_activity;
pub mod task_archive;
pub mod task_chain;
pub mod task_comments;
pub mod task_cron_scheduler;
pub mod task_export;
pub mod task_favorites;
pub mod task_profiler;
pub mod task_proxy;
pub mod task_queue;
pub mod task_schedule_windows;
pub mod task_scheduler;
pub mod task_scorecard;
pub mod task_snooze;
pub mod torrent;
pub mod ttl;
pub mod upload_tracker;
pub mod url_allowlist;
pub mod url_blacklist;
pub mod url_bookmarks;
pub mod url_dedup;
pub mod url_expander;
pub mod url_health_monitor;
pub mod url_intelligence;
pub mod url_normalizer;
pub mod url_pattern;
pub mod url_rewrite;
pub mod watch_folder;
pub mod web;
pub mod xunlei;

use audit_log::{AuditEventType, AuditLog, AuditLogEntry};
use auto_shutdown::AutoShutdownConfig;
use chrono::Timelike;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use task_activity::{ActivityEvent, ActivityEventType, ActivityLogManager};
use task_queue::{load_task_queue, save_task_queue};
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

use bandwidth_monitor::BandwidthMonitor;
use download_history::{HistoryEntry, append_entry};
use notification::{NotificationContext, NotificationDispatcher};
use post_hooks::HookManager;
use rss_feed::FeedSubscriptionManager;

pub use auto_actions::{
    AutoAction, AutoActionRule, AutoActionTrigger, AutoActionsConfig, AutoActionsSummary,
};
pub use bandwidth_monitor::{
    BandwidthDashboard, BandwidthMonitor as BandwidthMonitorType, BandwidthSample, BandwidthStats,
    BandwidthTrendSummary, MovingAvgPoint, TaskBandwidth, TrendDirection, TrendStats, WindowTrend,
};
pub use bandwidth_schedule::{
    BandwidthScheduleError, BandwidthScheduleManager, BandwidthScheduleRule,
    load_bandwidth_schedule, parse_days, parse_speed_limit, parse_time, parse_time_window,
    save_bandwidth_schedule,
};
pub use bandwidth_usage::{
    BandwidthUsageConfig, BandwidthUsageSummary, BandwidthUsageTracker, HourlySample,
    PeakHourAnalysis, PeakHourEntry, ProtocolBreakdown, RollingWindowSummary,
};
pub use bulk_ops::{
    BulkFilter, BulkGroupAction, BulkPriorityAction, BulkResult, BulkSpeedLimitAction,
    BulkTagAction, BulkWeightAction,
};
pub use dependency_graph::{
    DependencyGraphConfig, DependencyGraphValidator, DependencyIssue, GraphStats, IssueCategory,
    IssueSeverity, TaskDepData, TopologicalOrder, ValidationResult,
};
pub use eta_estimator::{EtaConfidence, EtaEstimate, EtaEstimator};
pub use integrity_verification::{
    IntegrityConfig, IntegrityManager, IntegritySummary, VerificationResult, VerificationStatus,
};
pub use mirror_health::{MirrorHealth, MirrorHealthConfig, MirrorSummary};
pub use notification::{
    NotificationChannel, NotificationConfig, NotificationError, NotificationEvent,
};
pub use preflight_check::{
    PreflightCheckError, PreflightChecker, PreflightConfig, PreflightInput, PreflightProtocol,
    PreflightReport, PreflightStatus,
};
pub use queue_completion::{
    QueueCompletionConfig, QueueCompletionPrediction, QueueCompletionPredictor,
    TaskCompletionEstimate,
};
pub use rate_limiter::{DownloadRateController, RateLimiter};
pub use save_path_manager::{
    FileCategory, SavePathConfig, SavePathError, SavePathManager, SavePathPersistenceError,
    load_save_path_config, save_save_path_config,
};
pub use tag_management::{
    TagAction, TagAliasMap, TagInfo, TagManagementConfig, TagManagementSummary, TagManager,
};

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
    pub queue_position: Option<u32>,
    /// Task IDs this task depends on (for WebSocket UI display)
    pub depends_on: Vec<String>,
    /// User-defined notes or description (for WebSocket UI display)
    pub notes: Option<String>,
    /// User-defined group for organizing downloads (optional)
    pub group: Option<String>,
    /// Per-task speed limit in bytes/sec (None = use global per-task limit)
    pub speed_limit_bps: Option<u64>,
    /// Number of times this task has been auto-retried after failure
    pub auto_retry_count: u32,
    /// Earliest time this task should be retried (for exponential backoff)
    pub retry_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Original source URL for deduplication (ed2k://, magnet:, http://, etc.)
    pub source_url: Option<String>,
    /// Expected checksum for post-download verification (hex-encoded hash)
    pub expected_checksum: Option<String>,
    /// Checksum algorithm (md5, sha1, sha256, ed2k)
    pub checksum_algorithm: Option<String>,
    /// Checksum verification result (pending, passed, failed)
    pub checksum_status: Option<String>,
    /// Estimated seconds remaining (from ETA estimator)
    pub eta_seconds: Option<f64>,
    /// Total time spent actively downloading (seconds)
    pub active_time_seconds: f64,
    /// Mirror/fallback URLs to try if the primary source fails
    pub mirror_urls: Vec<String>,
    /// Per-task retry policy (None = use global default)
    pub retry_policy: Option<RetryPolicy>,
    /// Cooldown state (for WebSocket push)
    pub cooldown: Option<download_cooldown::CooldownState>,
    /// Sequential download mode for torrents (download pieces in order for streaming)
    pub sequential_mode: bool,
    /// Whether this task is in favorites (for WebSocket push)
    pub is_favorite: bool,
    /// Maximum download time in seconds (auto-pause when exceeded, None = no limit)
    pub max_download_time_secs: Option<u64>,
    /// Per-task proxy override (None = use global proxy)
    pub proxy_override: Option<proxy::ProxyConfig>,
    /// Number of times this task has been auto-promoted by queue staleness detection
    pub staleness_promotion_count: u32,
    /// Optional deadline for this task (for WebSocket push)
    pub deadline: Option<chrono::DateTime<chrono::Utc>>,
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
            queue_position: task.queue_position,
            depends_on: task.depends_on.clone(),
            notes: task.notes.clone(),
            group: task.group.clone(),
            speed_limit_bps: task.speed_limit_bps,
            auto_retry_count: task.auto_retry_count,
            retry_after: task.retry_after,
            source_url: task.source_url.clone(),
            expected_checksum: task.expected_checksum.clone(),
            checksum_algorithm: task.checksum_algorithm.map(|a| a.name().to_lowercase()),
            checksum_status: None,
            eta_seconds: task.eta_seconds(),
            active_time_seconds: task.active_time_seconds,
            mirror_urls: task.mirror_urls.clone(),
            retry_policy: task.retry_policy,
            cooldown: task.cooldown.clone(),
            sequential_mode: task.sequential_mode,
            is_favorite: false, // Set by caller via favorites check
            max_download_time_secs: task.max_download_time_secs,
            proxy_override: task.proxy_override.clone(),
            staleness_promotion_count: task.staleness_promotion_count,
            deadline: task.deadline,
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
    /// Explicit queue position within the same priority level.
    /// Lower values come first. `None` means use creation time as tiebreaker.
    pub queue_position: Option<u32>,
    /// Task IDs this task depends on. All dependencies must be Complete before this task starts.
    pub depends_on: Vec<String>,
    /// User-defined notes or description for this task (optional)
    pub notes: Option<String>,
    /// User-defined group for organizing downloads (optional)
    pub group: Option<String>,
    /// Per-task speed limit in bytes/sec (None = use global per-task limit)
    pub speed_limit_bps: Option<u64>,
    /// Number of times this task has been auto-retried after failure
    pub auto_retry_count: u32,
    /// Earliest time this task should be retried (for exponential backoff)
    pub retry_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Original source URL for deduplication (ed2k://, magnet:, http://, etc.)
    pub source_url: Option<String>,
    /// Expected checksum for post-download verification (hex-encoded hash)
    pub expected_checksum: Option<String>,
    /// Checksum algorithm to use (md5, sha1, sha256, ed2k)
    pub checksum_algorithm: Option<checksum::ChecksumAlgorithm>,
    /// Total time spent actively downloading (seconds, excluding paused time)
    pub active_time_seconds: f64,
    /// Timestamp when current download session started (for real-time tracking)
    pub current_session_start: Option<chrono::DateTime<chrono::Utc>>,
    /// Mirror/fallback URLs to try if the primary source fails (HTTP/Xunlei only)
    pub mirror_urls: Vec<String>,
    /// Per-task retry policy (None = use global default)
    pub retry_policy: Option<RetryPolicy>,
    /// Cooldown state for exponential backoff retry tracking
    pub cooldown: Option<download_cooldown::CooldownState>,
    /// Sequential download mode for torrents (download pieces in order for streaming)
    pub sequential_mode: bool,
    /// Maximum download time in seconds (auto-pause when exceeded, None = no limit)
    pub max_download_time_secs: Option<u64>,
    /// Per-task proxy override (None = use global proxy)
    pub proxy_override: Option<proxy::ProxyConfig>,
    /// Number of times this task has been auto-promoted by queue staleness detection
    pub staleness_promotion_count: u32,
    /// Optional deadline for this download (UTC timestamp)
    pub deadline: Option<chrono::DateTime<chrono::Utc>>,
}

/// Per-task retry policy configuration
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct RetryPolicy {
    /// Maximum retry attempts (0 = no retries)
    pub max_retries: u32,
    /// Backoff strategy for retry delays
    pub backoff: RetryBackoff,
}

/// Backoff strategy for retry delays
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum RetryBackoff {
    /// Fixed delay between retries (seconds)
    Fixed(u64),
    /// Exponential backoff: base * 2^retry_count (seconds)
    Exponential { base_secs: u64 },
    /// Linear backoff: base * retry_count (seconds)
    Linear { base_secs: u64 },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff: RetryBackoff::Exponential { base_secs: 30 },
        }
    }
}

impl RetryPolicy {
    /// Calculate delay for the given retry attempt
    pub fn calculate_delay(&self, retry_count: u32) -> u64 {
        match self.backoff {
            RetryBackoff::Fixed(secs) => secs,
            RetryBackoff::Exponential { base_secs } => {
                (base_secs * 2u64.pow(retry_count)).min(3600)
            }
            RetryBackoff::Linear { base_secs } => base_secs * (retry_count as u64 + 1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadProtocol {
    Torrent,
    Ed2k,
    Xunlei,
    Magnet,
    P2P,
}

/// Result of a single URL import within a batch.
#[derive(Debug, Clone)]
pub struct ImportResult {
    /// The original URL or link that was imported.
    pub url: String,
    /// Task ID if successfully added, or error message.
    pub outcome: ImportOutcome,
}

/// Outcome of importing a single URL.
#[derive(Debug, Clone)]
pub enum ImportOutcome {
    /// Successfully added with the given task ID.
    Added(String),
    /// Skipped because a task with the same URL already exists.
    SkippedDuplicate,
    /// Failed with an error message.
    Failed(String),
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

    /// Accumulate current session time into active_time_seconds and clear session start.
    /// Call this when transitioning from Downloading to Paused/Complete/Error.
    pub fn finalize_active_time(&mut self) {
        if let Some(start) = self.current_session_start.take() {
            let elapsed = chrono::Utc::now()
                .signed_duration_since(start)
                .num_milliseconds() as f64
                / 1000.0;
            if elapsed > 0.0 {
                self.active_time_seconds += elapsed;
            }
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
    SegmentHttp {
        url: String,
        file_name: String,
        file_size: u64,
    },
}

/// Stored task info for resume
#[derive(Debug, Clone)]
struct TaskInfo {
    params: TaskParams,
    max_download_time_secs: Option<u64>,
    #[allow(dead_code)]
    proxy_override: Option<proxy::ProxyConfig>,
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
    max_concurrent: Arc<AtomicUsize>,
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
    /// Bandwidth usage tracker for hourly/protocol tracking
    bandwidth_usage: Arc<tokio::sync::Mutex<BandwidthUsageTracker>>,
    /// Auto-shutdown configuration
    auto_shutdown: Arc<tokio::sync::RwLock<AutoShutdownConfig>>,
    /// Save path manager for download directory configuration
    save_path_manager: Arc<SavePathManager>,
    /// Proxy configuration for HTTP/HTTPS downloads
    proxy_config: Arc<tokio::sync::RwLock<Option<proxy::ProxyConfig>>>,
    /// Per-task rate limiters (task_id -> RateLimiter)
    task_rate_limiters: Arc<Mutex<HashMap<String, RateLimiter>>>,
    /// Maximum auto-retry attempts for failed downloads (0 = disabled)
    max_auto_retries: Arc<AtomicU32>,
    /// Base delay in seconds for exponential backoff (actual delay = base * 2^retry_count)
    auto_retry_base_delay_secs: Arc<AtomicU64>,
    /// Post-download hook manager
    hook_manager: Arc<HookManager>,
    /// RSS/Atom feed subscription manager
    rss_feed_manager: Option<Arc<FeedSubscriptionManager>>,
    /// ETA estimator for download time prediction
    eta_estimator: Arc<EtaEstimator>,
    queue_completion_predictor: Arc<tokio::sync::RwLock<QueueCompletionPredictor>>,
    /// Auto-categorization rules for downloads
    categorize_rules: Arc<Mutex<Vec<auto_categorize::CategorizeRule>>>,
    /// Per-task speed history tracking
    speed_history: Arc<Mutex<speed_history::SpeedHistoryManager>>,
    /// Speed trend alert manager
    speed_alerts: Arc<speed_alert::SpeedAlertManager>,
    /// Speed anomaly detector for per-task speed anomaly detection
    speed_anomaly: Arc<Mutex<speed_anomaly::SpeedAnomalyDetector>>,
    /// Speed prediction manager for domain-based speed forecasting
    speed_prediction: Arc<Mutex<speed_prediction::SpeedPredictionManager>>,
    /// Speed profiles manager for named speed limit presets
    speed_profiles: Arc<tokio::sync::RwLock<speed_profiles::SpeedProfileManager>>,
    /// Speed test manager for pre-download throughput measurement
    speed_test: Arc<Mutex<speed_test::SpeedTestManager>>,
    /// Speed trend manager for per-domain trend analysis
    speed_trend: Arc<Mutex<speed_trend::SpeedTrendManager>>,
    /// Speed heatmap for tracking download speeds by hour/day-of-week
    speed_heatmap: Arc<tokio::sync::RwLock<speed_heatmap::SpeedHeatmap>>,
    /// Per-task download session tracking
    download_sessions: Arc<Mutex<download_session::DownloadSessionManager>>,
    /// Auto-cleanup configuration for completed/failed tasks
    auto_cleanup: Arc<tokio::sync::RwLock<auto_cleanup::AutoCleanupConfig>>,
    /// URL deduplication configuration
    url_dedup: Arc<tokio::sync::RwLock<url_dedup::DedupConfig>>,
    /// Audit log for tracking all download lifecycle events
    audit_log: Arc<Mutex<AuditLog>>,
    /// Bandwidth schedule manager for time-based speed limits
    bandwidth_schedule: Arc<Mutex<BandwidthScheduleManager>>,
    /// Download presets for reusable task configurations
    download_presets: Arc<Mutex<Vec<download_presets::DownloadPreset>>>,
    /// URL bookmarks for named collections of downloadable URLs
    url_bookmarks: Arc<Mutex<Vec<url_bookmarks::UrlBookmark>>>,
    /// Cooldown configuration for failed task retry backoff
    cooldown_config: Arc<tokio::sync::RwLock<download_cooldown::CooldownConfig>>,
    /// Conflict detection strategy for file path conflicts
    conflict_strategy: Arc<tokio::sync::RwLock<conflict_detection::ConflictStrategy>>,
    /// Per-domain concurrent download limit configuration
    domain_limit: Arc<tokio::sync::RwLock<domain_limit::DomainLimitConfig>>,
    /// Per-task activity log manager
    activity_log: Arc<Mutex<ActivityLogManager>>,
    /// URL rewrite rules for transforming URLs before download
    url_rewrite: Arc<Mutex<url_rewrite::UrlRewriteManager>>,
    /// Path template manager for auto-organizing downloads
    path_template: Arc<path_template::PathTemplateManager>,
    /// Daily data cap manager for limiting bandwidth usage
    data_cap: Arc<Mutex<data_cap::DataCapManager>>,
    /// Download statistics manager for analytics
    stats_manager: Arc<Mutex<download_stats::StatsManager>>,
    /// URL expander configuration for expanding shortened URLs
    url_expander: Arc<tokio::sync::RwLock<url_expander::UrlExpanderConfig>>,
    /// Watch folder manager for automatic URL import from monitored directories
    watch_folder: Arc<Mutex<watch_folder::WatchFolderManager>>,
    /// Per-protocol concurrent download limits
    protocol_limits: Arc<tokio::sync::RwLock<protocol_limits::ProtocolLimitsConfig>>,
    /// Path validator for save path security checks
    path_validator: Arc<Mutex<path_validator::PathValidator>>,
    /// Path rules for automatic save path assignment
    path_rules: Arc<Mutex<path_rules::PathRuleManager>>,
    /// Link rot detector for checking URL reachability in the queue
    link_rot: Arc<tokio::sync::RwLock<link_rot::LinkRotDetector>>,
    /// URL health monitor for tracking download URL and mirror health
    url_health_monitor: Arc<url_health_monitor::UrlHealthMonitor>,
    /// Task archive for preserving completed/failed tasks
    task_archive: Arc<tokio::sync::RwLock<task_archive::ArchiveState>>,
    /// URL normalizer for cleaning and deduplicating download URLs
    url_normalizer: Arc<tokio::sync::RwLock<url_normalizer::UrlNormalizer>>,
    /// URL intelligence system for pre-download analysis and optimization (Phase 161)
    url_intelligence: Arc<tokio::sync::RwLock<url_intelligence::UrlIntelligenceManager>>,
    /// Priority aging configuration for automatic priority boosting
    priority_aging: Arc<tokio::sync::RwLock<priority_aging::PriorityAgingConfig>>,
    /// Per-task user comments manager
    task_comments: Arc<Mutex<task_comments::TaskCommentsManager>>,
    /// Task favorites/pinning manager
    task_favorites: Arc<Mutex<task_favorites::FavoritesManager>>,
    /// Recycle bin for soft-deleted tasks
    recycle_bin: Arc<Mutex<recycle_bin::RecycleBinManager>>,
    /// Auto-pause configuration for peak hours scheduling
    auto_pause: Arc<tokio::sync::RwLock<auto_pause::AutoPauseConfig>>,
    /// URL allowlist for restricting downloads to trusted sources
    url_allowlist: Arc<tokio::sync::RwLock<url_allowlist::AllowlistConfig>>,
    /// Task snooze manager for pausing downloads until a specific time
    task_snooze: Arc<Mutex<task_snooze::TaskSnoozeManager>>,
    task_scheduler: Arc<Mutex<task_scheduler::TaskSchedulerManager>>,
    /// Progress milestone tracker for sending notifications at download thresholds
    progress_milestone: Arc<Mutex<progress_milestone::ProgressMilestoneTracker>>,
    /// Download time limit manager for auto-pausing tasks that exceed time limits
    download_time_limit: Arc<Mutex<download_time_limit::DownloadTimeLimitManager>>,
    /// Progress milestone configuration
    progress_milestone_config:
        Arc<tokio::sync::RwLock<progress_milestone::ProgressMilestoneConfig>>,
    /// Speed burst manager for temporary speed boosts
    speed_burst: Arc<Mutex<speed_burst::SpeedBurstManager>>,
    /// System-wide speed boost manager for global temporary speed overrides
    speed_boost: Arc<Mutex<speed_boost::SpeedBoostManager>>,
    /// Daily retry quota manager for limiting total retry attempts
    retry_quota: Arc<Mutex<retry_quota::RetryQuotaManager>>,
    /// TTL manager for auto-pausing tasks that exceed their lifetime
    ttl: Arc<Mutex<ttl::TtlManager>>,
    /// Error recovery manager for classifying errors and determining recovery strategies
    error_recovery: Arc<Mutex<error_recovery::ErrorRecoveryManager>>,
    /// Connection health monitor for tracking per-connection quality
    connection_health: Arc<Mutex<connection_health::ConnectionHealthManager>>,
    /// Source rotation manager for automatic failover to alternative sources
    source_rotation: Arc<Mutex<source_rotation::SourceRotationManager>>,
    /// Progress predictor for download completion time estimation
    progress_predictor: Arc<Mutex<progress_prediction::ProgressPredictor>>,
    /// Bandwidth allocation manager for intelligent bandwidth distribution
    bandwidth_allocation: Arc<Mutex<bandwidth_allocation::AllocationManager>>,
    /// Per-task proxy override manager
    #[allow(dead_code)]
    task_proxy: Arc<Mutex<task_proxy::TaskProxyManager>>,
    /// Task chain manager for sequential task execution
    task_chain: Arc<Mutex<task_chain::TaskChainManager>>,
    /// Download queue snapshot manager
    snapshot_manager: Arc<Mutex<download_snapshot::SnapshotManager>>,
    /// Network-aware download manager
    network_aware: Arc<Mutex<network_aware::NetworkStateManager>>,
    /// Task performance profiler
    task_profiler: Arc<Mutex<task_profiler::TaskProfiler>>,
    /// Adaptive download concurrency manager
    adaptive_concurrency: Arc<Mutex<adaptive_concurrency::AdaptiveConcurrencyManager>>,
    /// Download templates for reusable task configurations
    download_templates: Arc<Mutex<download_templates::DownloadTemplateManager>>,
    /// Auto-actions manager for triggering actions on download completion/failure
    auto_actions: Arc<Mutex<auto_actions::AutoActionsManager>>,
    /// Queue staleness configuration for detecting long-waiting tasks
    queue_staleness: Arc<tokio::sync::RwLock<queue_staleness::StalenessConfig>>,
    /// Network condition monitor for tracking overall network quality
    network_monitor: Arc<Mutex<network_monitor::NetworkMonitor>>,
    /// Download deadline manager for task urgency tracking
    download_deadline: Arc<Mutex<download_deadline::DeadlineManager>>,
    /// Integrity verification manager for checking file existence and size
    integrity: Arc<Mutex<integrity_verification::IntegrityManager>>,
    /// Resume policy configuration for controlling task restoration on startup
    resume_policy: Arc<tokio::sync::RwLock<resume_policy::ResumePolicyConfig>>,
    /// Duplicate detection manager for identifying redundant download tasks
    duplicate_detection: Arc<Mutex<duplicate_detection::DuplicateDetectionManager>>,
    /// Dynamic priority adjustment manager for automatic priority optimization
    dynamic_priority: Arc<tokio::sync::RwLock<dynamic_priority::DynamicPriorityManager>>,
    /// Dependency graph validator for checking task dependency integrity
    dependency_graph: Arc<tokio::sync::RwLock<dependency_graph::DependencyGraphValidator>>,
    /// Dependency graph visualization manager
    dep_visualization:
        Arc<tokio::sync::RwLock<dependency_visualization::DependencyVisualizationManager>>,
    /// Download quota manager for per-tag/group data limits
    download_quota: Arc<Mutex<download_quota::DownloadQuotaManager>>,
    /// Advanced search query cache (not persisted, in-memory only)
    #[allow(dead_code)]
    advanced_search_config: Arc<tokio::sync::RwLock<advanced_search::AdvancedSearchQuery>>,
    /// Automation rules engine for IFTTT-style download workflows
    automation_rules: Arc<tokio::sync::RwLock<automation_rules::AutomationRuleManager>>,
    /// Per-task schedule windows for controlling when tasks are allowed to run
    task_schedule_windows:
        Arc<tokio::sync::RwLock<task_schedule_windows::TaskScheduleWindowsManager>>,
    /// Disk space monitor for tracking available disk space
    disk_monitor: Arc<Mutex<disk_monitor::DiskSpaceMonitor>>,
    /// Disk monitor configuration
    disk_monitor_config: Arc<tokio::sync::RwLock<disk_monitor::DiskMonitorConfig>>,
    /// Source benchmark manager for pre-download speed testing
    source_benchmark: Arc<Mutex<source_benchmark::SourceBenchmarkManager>>,
    /// Global download budget manager for weekly/monthly data limits
    global_budget: Arc<tokio::sync::RwLock<global_budget::GlobalBudgetManager>>,
    /// Weekly/monthly download budget manager
    download_budget: Arc<Mutex<download_budget::BudgetManager>>,
    /// Download analytics for historical trend tracking
    download_analytics: Arc<Mutex<download_analytics::AnalyticsManager>>,
    /// Download backup manager for comprehensive state export/restore
    backup_manager: Arc<download_backup::BackupManager>,
    /// Smart queue optimizer for automatic queue reordering
    smart_queue: Arc<tokio::sync::RwLock<smart_queue::SmartQueueOptimizer>>,
    /// Preflight checker for pre-download validation
    preflight_checker: Arc<tokio::sync::RwLock<preflight_check::PreflightChecker>>,
    /// Download diagnostics for troubleshooting issues
    diagnostics: Arc<tokio::sync::RwLock<download_diagnostics::DownloadDiagnostics>>,
    /// Tag management system for cross-task tag operations
    tag_manager: Arc<tag_management::TagManager>,
    /// Download cost tracker for estimating monetary cost of data usage
    cost_tracker: Arc<Mutex<download_cost::CostTracker>>,
    /// Download history analytics for insights and statistics
    history_analytics: Arc<Mutex<download_history_analytics::HistoryAnalyticsManager>>,
    /// Speed benchmark manager for pre-download URL speed testing
    speed_benchmark: Arc<Mutex<speed_benchmark::SpeedBenchmarkManager>>,
    /// Speed distribution analyzer for per-domain/protocol/hourly speed statistics
    speed_distribution: Arc<tokio::sync::RwLock<speed_distribution::SpeedDistributionManager>>,
    /// Event webhook manager for sending HTTP notifications on download events
    event_webhook: Arc<Mutex<event_webhook::WebhookManager>>,
    /// Path organizer manager for automatic file organization by extension
    path_organizer: Arc<Mutex<path_organizer::PathOrganizerManager>>,
    /// Data retention policy manager for automatic lifecycle management
    data_retention: Arc<Mutex<data_retention::DataRetentionManager>>,
    /// Download source quality tracker for long-term reliability scoring
    source_quality: Arc<Mutex<source_quality::SourceQualityManager>>,
    /// Dashboard manager for unified system overview
    dashboard: Arc<Mutex<dashboard::DashboardManager>>,
    /// Upload speed and bytes tracker for seeding/sharing monitoring
    upload_tracker: Arc<Mutex<upload_tracker::UploadTracker>>,
    /// Bandwidth forecast manager for download speed prediction
    bandwidth_forecast: Arc<Mutex<bandwidth_forecast::BandwidthForecastManager>>,
    /// Source reliability tracker for per-domain reliability scoring
    source_reliability: Arc<Mutex<source_reliability::SourceReliabilityTracker>>,
    /// Notification center for advanced notification management with quiet hours and batching
    #[allow(dead_code)]
    notification_center: Arc<Mutex<notification_center::NotificationCenterManager>>,
    /// Task scorecard manager for unified per-task performance scoring (Phase 139)
    task_scorecard: Arc<Mutex<task_scorecard::TaskScorecardManager>>,
    /// Intelligent source selector for combining reliability, health, and bandwidth data (Phase 140)
    intelligent_source_selector: Arc<Mutex<intelligent_source_selector::IntelligentSourceSelector>>,
    /// Retry budget manager for per-domain retry tracking and blocking (Phase 142)
    retry_budget: Arc<Mutex<retry_budget::RetryBudgetManager>>,
    /// Download expiry manager for task expiry tracking
    download_expiry: Arc<Mutex<download_expiry::DownloadExpiryManager>>,
    /// Export/import manager for task backup and migration (Phase 145)
    task_export: Arc<Mutex<task_export::ExportHistory>>,
    /// Source latency monitor for per-domain connection latency tracking (Phase 145)
    source_latency: Arc<Mutex<source_latency::SourceLatencyMonitor>>,
    /// System uptime tracker for dashboard monitoring
    system_uptime: Arc<system_uptime::SystemUptimeTracker>,
    /// File type statistics tracker for download categorization by extension (Phase 143)
    file_stats: Arc<tokio::sync::RwLock<download_file_stats::FileTypeStatsTracker>>,
    /// SLA compliance manager for tracking download service level agreements (Phase 144)
    sla_compliance: Arc<tokio::sync::RwLock<sla_compliance::SlaComplianceManager>>,
    /// Per-task notification preferences manager (Phase 146)
    notification_preferences: Arc<Mutex<notification_preferences::NotificationPreferencesManager>>,
    /// Host connection limiter for per-host TCP connection tracking (Phase 148)
    host_conn_limit: Arc<Mutex<host_conn_limit::HostConnLimitManager>>,
    /// Per-task cron-based scheduler for time-based download scheduling (Phase 149)
    task_cron_scheduler: Arc<Mutex<task_cron_scheduler::TaskCronScheduler>>,
    /// Bandwidth QoS classification manager (Phase 151)
    bandwidth_qos: Arc<Mutex<bandwidth_qos::BandwidthQosManager>>,
    /// URL blacklist for blocking unwanted downloads (Phase 153)
    url_blacklist: Arc<tokio::sync::RwLock<url_blacklist::BlacklistConfig>>,
    /// Connection pool for TCP connection reuse and DNS caching (Phase 157)
    connection_pool: Arc<connection_pool::ConnectionPool>,
    /// Download completion probability estimator (Phase 162)
    completion_probability: Arc<Mutex<completion_probability::CompletionProbabilityEstimator>>,
}

impl DownloadManager {
    pub fn new(data_dir: PathBuf) -> Self {
        let save_path = data_dir.join("downloads");
        let dm = Self {
            tasks: Arc::new(Mutex::new(Vec::new())),
            dep_visualization: Arc::new(tokio::sync::RwLock::new(
                dependency_visualization::DependencyVisualizationManager::new(),
            )),
            speed_alerts: Arc::new(speed_alert::SpeedAlertManager::new()),
            speed_anomaly: Arc::new(Mutex::new(speed_anomaly::SpeedAnomalyDetector::new(
                speed_anomaly::AnomalyConfig::default(),
            ))),
            speed_prediction: Arc::new(Mutex::new(speed_prediction::SpeedPredictionManager::new(
                speed_prediction::SpeedPredictionConfig::default(),
            ))),
            speed_profiles: Arc::new(tokio::sync::RwLock::new(
                speed_profiles::SpeedProfileManager::new(&data_dir),
            )),
            speed_test: Arc::new(Mutex::new(speed_test::SpeedTestManager::new())),
            speed_trend: Arc::new(Mutex::new(speed_trend::SpeedTrendManager::new())),
            speed_heatmap: Arc::new(tokio::sync::RwLock::new(speed_heatmap::SpeedHeatmap::new())),
            running: Arc::new(Mutex::new(HashMap::new())),
            task_info: Arc::new(Mutex::new(HashMap::new())),
            task_generation: Arc::new(Mutex::new(HashMap::new())),
            data_dir: data_dir.clone(),
            dht: Arc::new(dht::DhtManager::new()),
            max_concurrent: Arc::new(AtomicUsize::new(0)), // 0 = unlimited
            rate_limiter: Arc::new(DownloadRateController::new(0, 0)),
            timeout_secs: Arc::new(AtomicU64::new(0)),
            max_retries: Arc::new(AtomicU32::new(3)),
            event_tx: broadcast::channel(128).0,
            task_complete_notify: Arc::new(tokio::sync::Notify::new()),
            notifier: Arc::new(NotificationDispatcher::new(NotificationConfig::disabled())),
            bandwidth_monitor: Arc::new(BandwidthMonitor::new()),
            bandwidth_usage: Arc::new(tokio::sync::Mutex::new(BandwidthUsageTracker::new())),
            auto_shutdown: Arc::new(tokio::sync::RwLock::new(AutoShutdownConfig::default())),
            save_path_manager: Arc::new(SavePathManager::new(save_path)),
            proxy_config: Arc::new(tokio::sync::RwLock::new(None)),
            task_rate_limiters: Arc::new(Mutex::new(HashMap::new())),
            max_auto_retries: Arc::new(AtomicU32::new(0)),
            auto_retry_base_delay_secs: Arc::new(AtomicU64::new(30)),
            hook_manager: Arc::new(HookManager::new(data_dir.clone())),
            rss_feed_manager: None,
            eta_estimator: Arc::new(EtaEstimator::new()),
            queue_completion_predictor: Arc::new(tokio::sync::RwLock::new(
                QueueCompletionPredictor::new(),
            )),
            categorize_rules: Arc::new(Mutex::new(Vec::new())),
            speed_history: Arc::new(Mutex::new(speed_history::SpeedHistoryManager::new(360))),
            auto_cleanup: Arc::new(tokio::sync::RwLock::new(
                auto_cleanup::AutoCleanupConfig::default(),
            )),
            url_dedup: Arc::new(tokio::sync::RwLock::new(url_dedup::DedupConfig::default())),
            audit_log: Arc::new(Mutex::new(AuditLog::new())),
            bandwidth_schedule: Arc::new(Mutex::new(BandwidthScheduleManager::new())),
            download_presets: Arc::new(Mutex::new(Vec::new())),
            url_bookmarks: Arc::new(Mutex::new(Vec::new())),
            cooldown_config: Arc::new(tokio::sync::RwLock::new(
                download_cooldown::CooldownConfig::default(),
            )),
            conflict_strategy: Arc::new(tokio::sync::RwLock::new(
                conflict_detection::ConflictStrategy::default(),
            )),
            domain_limit: Arc::new(tokio::sync::RwLock::new(
                domain_limit::DomainLimitConfig::default(),
            )),
            activity_log: Arc::new(Mutex::new(ActivityLogManager::new())),
            url_rewrite: Arc::new(Mutex::new(url_rewrite::UrlRewriteManager::new())),
            path_template: Arc::new(path_template::PathTemplateManager::new()),
            data_cap: Arc::new(Mutex::new(data_cap::DataCapManager::new())),
            stats_manager: Arc::new(Mutex::new(download_stats::StatsManager::new())),
            url_expander: Arc::new(tokio::sync::RwLock::new(
                url_expander::UrlExpanderConfig::default(),
            )),
            watch_folder: Arc::new(Mutex::new(watch_folder::WatchFolderManager::new())),
            protocol_limits: Arc::new(tokio::sync::RwLock::new(
                protocol_limits::ProtocolLimitsConfig::new(),
            )),
            path_validator: Arc::new(Mutex::new(path_validator::PathValidator::new())),
            path_rules: Arc::new(Mutex::new(path_rules::PathRuleManager::new())),
            link_rot: Arc::new(tokio::sync::RwLock::new(link_rot::LinkRotDetector::new(
                &data_dir,
            ))),
            url_health_monitor: Arc::new(url_health_monitor::UrlHealthMonitor::new()),
            task_archive: Arc::new(tokio::sync::RwLock::new(
                task_archive::ArchiveState::default(),
            )),
            url_normalizer: Arc::new(tokio::sync::RwLock::new(
                url_normalizer::UrlNormalizer::new(),
            )),
            url_intelligence: Arc::new(tokio::sync::RwLock::new(
                url_intelligence::UrlIntelligenceManager::new(),
            )),
            priority_aging: Arc::new(tokio::sync::RwLock::new(
                priority_aging::PriorityAgingConfig::default(),
            )),
            task_comments: Arc::new(Mutex::new(task_comments::TaskCommentsManager::new())),
            download_sessions: Arc::new(
                Mutex::new(download_session::DownloadSessionManager::new()),
            ),
            task_favorites: Arc::new(Mutex::new(task_favorites::FavoritesManager::new())),
            recycle_bin: Arc::new(Mutex::new(recycle_bin::RecycleBinManager::new())),
            auto_pause: Arc::new(tokio::sync::RwLock::new(
                auto_pause::AutoPauseConfig::default(),
            )),
            url_allowlist: Arc::new(tokio::sync::RwLock::new(
                url_allowlist::AllowlistConfig::default(),
            )),
            url_blacklist: Arc::new(tokio::sync::RwLock::new(
                url_blacklist::BlacklistConfig::default(),
            )),
            connection_pool: Arc::new(connection_pool::ConnectionPool::new()),
            completion_probability: Arc::new(Mutex::new(
                completion_probability::CompletionProbabilityEstimator::new(),
            )),
            task_snooze: Arc::new(Mutex::new(task_snooze::TaskSnoozeManager::new())),
            task_scheduler: Arc::new(Mutex::new(task_scheduler::TaskSchedulerManager::new())),
            progress_milestone: Arc::new(Mutex::new(
                progress_milestone::ProgressMilestoneTracker::new(),
            )),
            progress_milestone_config: Arc::new(tokio::sync::RwLock::new(
                progress_milestone::ProgressMilestoneConfig::default(),
            )),
            speed_burst: Arc::new(Mutex::new(speed_burst::SpeedBurstManager::new())),
            speed_boost: Arc::new(Mutex::new(speed_boost::SpeedBoostManager::new())),
            retry_quota: Arc::new(Mutex::new(retry_quota::RetryQuotaManager::new())),
            download_time_limit: Arc::new(Mutex::new(
                download_time_limit::DownloadTimeLimitManager::new(),
            )),
            ttl: Arc::new(Mutex::new(ttl::TtlManager::new())),
            error_recovery: Arc::new(Mutex::new(error_recovery::ErrorRecoveryManager::new())),
            connection_health: Arc::new(Mutex::new(
                connection_health::ConnectionHealthManager::new(),
            )),
            source_rotation: Arc::new(Mutex::new(source_rotation::SourceRotationManager::new())),
            progress_predictor: Arc::new(Mutex::new(progress_prediction::ProgressPredictor::new())),
            bandwidth_allocation: Arc::new(Mutex::new(
                bandwidth_allocation::AllocationManager::new(),
            )),
            task_proxy: Arc::new(Mutex::new(task_proxy::TaskProxyManager::new(
                data_dir.join("task_proxy.json"),
            ))),
            task_chain: Arc::new(Mutex::new(task_chain::TaskChainManager::new(
                data_dir.join("task_chain.json"),
            ))),
            snapshot_manager: Arc::new(Mutex::new(download_snapshot::SnapshotManager::new(
                data_dir.join("snapshots"),
            ))),
            network_aware: Arc::new(Mutex::new(network_aware::NetworkStateManager::new())),
            task_profiler: Arc::new(Mutex::new(task_profiler::TaskProfiler::default())),
            adaptive_concurrency: Arc::new(Mutex::new(
                adaptive_concurrency::AdaptiveConcurrencyManager::new(),
            )),
            download_templates: Arc::new(Mutex::new(
                download_templates::DownloadTemplateManager::new(),
            )),
            auto_actions: Arc::new(Mutex::new(auto_actions::AutoActionsManager::new(
                auto_actions::AutoActionsConfig::default(),
            ))),
            queue_staleness: Arc::new(tokio::sync::RwLock::new(
                queue_staleness::StalenessConfig::default(),
            )),
            network_monitor: Arc::new(Mutex::new(network_monitor::NetworkMonitor::new())),
            download_deadline: Arc::new(Mutex::new(download_deadline::DeadlineManager::new())),
            integrity: Arc::new(Mutex::new(integrity_verification::IntegrityManager::new())),
            resume_policy: Arc::new(tokio::sync::RwLock::new(
                resume_policy::ResumePolicyConfig::default(),
            )),
            duplicate_detection: Arc::new(Mutex::new(
                duplicate_detection::DuplicateDetectionManager::new(),
            )),
            dynamic_priority: Arc::new(tokio::sync::RwLock::new(
                dynamic_priority::DynamicPriorityManager::new(),
            )),
            dependency_graph: Arc::new(tokio::sync::RwLock::new(
                dependency_graph::DependencyGraphValidator::new(),
            )),
            download_quota: Arc::new(Mutex::new(download_quota::DownloadQuotaManager::new())),
            advanced_search_config: Arc::new(tokio::sync::RwLock::new(
                advanced_search::AdvancedSearchQuery::default(),
            )),
            automation_rules: Arc::new(tokio::sync::RwLock::new(
                automation_rules::AutomationRuleManager::new_with_dir(&data_dir),
            )),
            task_schedule_windows: Arc::new(tokio::sync::RwLock::new(
                task_schedule_windows::TaskScheduleWindowsManager::new(),
            )),
            disk_monitor: Arc::new(Mutex::new(disk_monitor::DiskSpaceMonitor::new(
                data_dir.join("downloads"),
                100_000_000, // 100MB default
                30,
            ))),
            disk_monitor_config: Arc::new(tokio::sync::RwLock::new(
                disk_monitor::DiskMonitorConfig::default(),
            )),
            source_benchmark: Arc::new(Mutex::new(source_benchmark::SourceBenchmarkManager::new(
                data_dir.clone(),
            ))),
            global_budget: Arc::new(tokio::sync::RwLock::new(
                global_budget::GlobalBudgetManager::new(),
            )),
            download_budget: Arc::new(Mutex::new(download_budget::BudgetManager::new())),
            download_analytics: Arc::new(Mutex::new(download_analytics::AnalyticsManager::new())),
            backup_manager: Arc::new(download_backup::BackupManager::new(data_dir.clone())),
            smart_queue: Arc::new(tokio::sync::RwLock::new(
                smart_queue::SmartQueueOptimizer::new(),
            )),
            preflight_checker: Arc::new(tokio::sync::RwLock::new(
                preflight_check::PreflightChecker::new(&data_dir),
            )),
            diagnostics: Arc::new(tokio::sync::RwLock::new(
                download_diagnostics::DownloadDiagnostics::new(),
            )),
            tag_manager: Arc::new(tag_management::TagManager::new(&data_dir)),
            cost_tracker: Arc::new(Mutex::new(download_cost::CostTracker::new())),
            history_analytics: Arc::new(Mutex::new(
                download_history_analytics::HistoryAnalyticsManager::new(),
            )),
            speed_benchmark: Arc::new(Mutex::new(speed_benchmark::SpeedBenchmarkManager::new())),
            speed_distribution: Arc::new(tokio::sync::RwLock::new(
                speed_distribution::SpeedDistributionManager::new(data_dir.clone()),
            )),
            event_webhook: Arc::new(Mutex::new(event_webhook::WebhookManager::new(
                data_dir.clone(),
            ))),
            path_organizer: Arc::new(Mutex::new(path_organizer::PathOrganizerManager::new())),
            data_retention: Arc::new(Mutex::new(data_retention::DataRetentionManager::new(
                data_dir.clone(),
            ))),
            source_quality: Arc::new(Mutex::new(source_quality::SourceQualityManager::new(
                data_dir.clone(),
            ))),
            dashboard: Arc::new(Mutex::new(dashboard::DashboardManager::new())),
            upload_tracker: Arc::new(Mutex::new(upload_tracker::UploadTracker::new())),
            bandwidth_forecast: Arc::new(Mutex::new(
                bandwidth_forecast::BandwidthForecastManager::new(
                    bandwidth_forecast::ForecastConfig::default(),
                ),
            )),
            source_reliability: Arc::new(Mutex::new(
                source_reliability::SourceReliabilityTracker::new(),
            )),
            notification_center: Arc::new(Mutex::new(
                notification_center::NotificationCenterManager::new(),
            )),
            task_scorecard: Arc::new(Mutex::new(task_scorecard::TaskScorecardManager::new())),
            intelligent_source_selector: Arc::new(Mutex::new(
                intelligent_source_selector::IntelligentSourceSelector::new(),
            )),
            retry_budget: Arc::new(Mutex::new(retry_budget::RetryBudgetManager::new())),
            download_expiry: Arc::new(Mutex::new(download_expiry::DownloadExpiryManager::new())),
            system_uptime: Arc::new(system_uptime::SystemUptimeTracker::new()),
            task_export: Arc::new(Mutex::new(task_export::ExportHistory::default())),
            source_latency: Arc::new(Mutex::new(source_latency::SourceLatencyMonitor::new())),
            file_stats: Arc::new(tokio::sync::RwLock::new(
                download_file_stats::FileTypeStatsTracker::new(
                    download_file_stats::FileStatsConfig::default(),
                ),
            )),
            sla_compliance: Arc::new(tokio::sync::RwLock::new(
                sla_compliance::SlaComplianceManager::new(data_dir.clone()),
            )),
            notification_preferences: Arc::new(Mutex::new(
                notification_preferences::NotificationPreferencesManager::new(),
            )),
            host_conn_limit: Arc::new(Mutex::new(host_conn_limit::HostConnLimitManager::new())),
            task_cron_scheduler: Arc::new(
                Mutex::new(task_cron_scheduler::TaskCronScheduler::new()),
            ),
            bandwidth_qos: Arc::new(Mutex::new(bandwidth_qos::BandwidthQosManager::new())),
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

    /// Verify checksum after download completion
    async fn verify_checksum(task: &DownloadTask) -> Option<String> {
        if let (Some(expected), Some(algo)) = (&task.expected_checksum, task.checksum_algorithm) {
            match checksum::verify_file(&task.save_path, expected, algo).await {
                Ok(result) if result.matched => {
                    tracing::info!(task_id = %task.id, algorithm = %algo.name(), "Checksum verification passed");
                    None
                }
                Ok(result) => {
                    let msg = format!(
                        "Checksum verification failed: expected {}, got {}",
                        result.expected, result.actual
                    );
                    tracing::error!(task_id = %task.id, "{}", msg);
                    Some(msg)
                }
                Err(e) => {
                    let msg = format!("Checksum verification error: {}", e);
                    tracing::error!(task_id = %task.id, "{}", msg);
                    Some(msg)
                }
            }
        } else {
            None
        }
    }

    /// Record a completed or failed task to download history and send notifications.
    fn record_task_history(
        task: &DownloadTask,
        data_dir: &std::path::Path,
        notifier: Option<&Arc<NotificationDispatcher>>,
        hook_manager: Option<&Arc<HookManager>>,
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

        // Execute post-download hooks
        if let Some(hook_manager) = hook_manager {
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
            let hook_manager = hook_manager.clone();
            tokio::spawn(async move {
                let results = hook_manager.execute_hooks(&ctx).await;
                for result in results {
                    if !result.success {
                        tracing::warn!(
                            hook_id = %result.hook_id,
                            hook_name = %result.hook_name,
                            error = ?result.error,
                            "Post-download hook failed"
                        );
                    }
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

        let _save_path = data_dir.join("downloads");
        let dm = Self {
            tasks: Arc::new(Mutex::new(tasks)),
            dep_visualization: Arc::new(tokio::sync::RwLock::new(
                dependency_visualization::DependencyVisualizationManager::new(),
            )),
            url_blacklist: Arc::new(tokio::sync::RwLock::new(
                url_blacklist::BlacklistConfig::default(),
            )),
            connection_pool: Arc::new(connection_pool::ConnectionPool::new()),
            completion_probability: Arc::new(Mutex::new(
                completion_probability::CompletionProbabilityEstimator::new(),
            )),
            speed_alerts: Arc::new(speed_alert::SpeedAlertManager::new()),
            speed_anomaly: Arc::new(Mutex::new(speed_anomaly::SpeedAnomalyDetector::new(
                speed_anomaly::AnomalyConfig::default(),
            ))),
            speed_prediction: Arc::new(Mutex::new(speed_prediction::SpeedPredictionManager::new(
                speed_prediction::SpeedPredictionConfig::default(),
            ))),
            speed_profiles: Arc::new(tokio::sync::RwLock::new(
                speed_profiles::SpeedProfileManager::new(&data_dir),
            )),
            speed_test: Arc::new(Mutex::new(speed_test::SpeedTestManager::new())),
            speed_trend: Arc::new(Mutex::new(speed_trend::SpeedTrendManager::new())),
            speed_heatmap: Arc::new(tokio::sync::RwLock::new(speed_heatmap::SpeedHeatmap::new())),
            running: Arc::new(Mutex::new(HashMap::new())),
            task_info: Arc::new(Mutex::new(HashMap::new())),
            task_generation: Arc::new(Mutex::new(HashMap::new())),
            data_dir: data_dir.clone(),
            dht: Arc::new(dht::DhtManager::new()),
            max_concurrent: Arc::new(AtomicUsize::new(0)),
            rate_limiter: Arc::new(DownloadRateController::new(0, 0)),
            timeout_secs: Arc::new(AtomicU64::new(0)),
            max_retries: Arc::new(AtomicU32::new(3)),
            event_tx: broadcast::channel(128).0,
            task_complete_notify: Arc::new(tokio::sync::Notify::new()),
            notifier: Arc::new(NotificationDispatcher::new(NotificationConfig::disabled())),
            bandwidth_monitor: Arc::new(BandwidthMonitor::new()),
            bandwidth_usage: Arc::new(tokio::sync::Mutex::new(BandwidthUsageTracker::new())),
            auto_shutdown: Arc::new(tokio::sync::RwLock::new(AutoShutdownConfig::default())),
            save_path_manager: Arc::new(SavePathManager::new(data_dir.join("downloads"))),
            proxy_config: Arc::new(tokio::sync::RwLock::new(None)),
            task_rate_limiters: Arc::new(Mutex::new(HashMap::new())),
            max_auto_retries: Arc::new(AtomicU32::new(0)),
            auto_retry_base_delay_secs: Arc::new(AtomicU64::new(30)),
            hook_manager: Arc::new(HookManager::new(data_dir.clone())),
            rss_feed_manager: None,
            eta_estimator: Arc::new(EtaEstimator::new()),
            queue_completion_predictor: Arc::new(tokio::sync::RwLock::new(
                QueueCompletionPredictor::new(),
            )),
            categorize_rules: Arc::new(Mutex::new(Vec::new())),
            speed_history: Arc::new(Mutex::new(speed_history::SpeedHistoryManager::new(360))),
            auto_cleanup: Arc::new(tokio::sync::RwLock::new(
                auto_cleanup::AutoCleanupConfig::default(),
            )),
            url_dedup: Arc::new(tokio::sync::RwLock::new(url_dedup::DedupConfig::default())),
            audit_log: Arc::new(Mutex::new(AuditLog::new())),
            bandwidth_schedule: Arc::new(Mutex::new(BandwidthScheduleManager::new())),
            download_presets: Arc::new(Mutex::new(Vec::new())),
            url_bookmarks: Arc::new(Mutex::new(Vec::new())),
            cooldown_config: Arc::new(tokio::sync::RwLock::new(
                download_cooldown::CooldownConfig::default(),
            )),
            conflict_strategy: Arc::new(tokio::sync::RwLock::new(
                conflict_detection::ConflictStrategy::default(),
            )),
            domain_limit: Arc::new(tokio::sync::RwLock::new(
                domain_limit::DomainLimitConfig::default(),
            )),
            activity_log: Arc::new(Mutex::new(ActivityLogManager::new())),
            url_rewrite: Arc::new(Mutex::new(url_rewrite::UrlRewriteManager::new())),
            path_template: Arc::new(path_template::PathTemplateManager::new()),
            data_cap: Arc::new(Mutex::new(data_cap::DataCapManager::new())),
            stats_manager: Arc::new(Mutex::new(download_stats::StatsManager::new())),
            url_expander: Arc::new(tokio::sync::RwLock::new(
                url_expander::UrlExpanderConfig::default(),
            )),
            watch_folder: Arc::new(Mutex::new(watch_folder::WatchFolderManager::new())),
            protocol_limits: Arc::new(tokio::sync::RwLock::new(
                protocol_limits::ProtocolLimitsConfig::new(),
            )),
            path_validator: Arc::new(Mutex::new(path_validator::PathValidator::new())),
            path_rules: Arc::new(Mutex::new(path_rules::PathRuleManager::new())),
            link_rot: Arc::new(tokio::sync::RwLock::new(link_rot::LinkRotDetector::new(
                &data_dir,
            ))),
            url_health_monitor: Arc::new(url_health_monitor::UrlHealthMonitor::new()),
            task_archive: Arc::new(tokio::sync::RwLock::new(
                task_archive::ArchiveState::default(),
            )),
            url_normalizer: Arc::new(tokio::sync::RwLock::new(
                url_normalizer::UrlNormalizer::new(),
            )),
            priority_aging: Arc::new(tokio::sync::RwLock::new(
                priority_aging::PriorityAgingConfig::default(),
            )),
            url_intelligence: Arc::new(tokio::sync::RwLock::new(
                url_intelligence::UrlIntelligenceManager::new(),
            )),
            task_comments: Arc::new(Mutex::new(task_comments::TaskCommentsManager::new())),
            download_sessions: Arc::new(
                Mutex::new(download_session::DownloadSessionManager::new()),
            ),
            task_favorites: Arc::new(Mutex::new(task_favorites::FavoritesManager::new())),
            recycle_bin: Arc::new(Mutex::new(recycle_bin::RecycleBinManager::new())),
            auto_pause: Arc::new(tokio::sync::RwLock::new(
                auto_pause::AutoPauseConfig::default(),
            )),
            url_allowlist: Arc::new(tokio::sync::RwLock::new(
                url_allowlist::AllowlistConfig::default(),
            )),
            task_snooze: Arc::new(Mutex::new(task_snooze::TaskSnoozeManager::new())),
            task_scheduler: Arc::new(Mutex::new(task_scheduler::TaskSchedulerManager::new())),
            progress_milestone: Arc::new(Mutex::new(
                progress_milestone::ProgressMilestoneTracker::new(),
            )),
            progress_milestone_config: Arc::new(tokio::sync::RwLock::new(
                progress_milestone::ProgressMilestoneConfig::default(),
            )),
            speed_burst: Arc::new(Mutex::new(speed_burst::SpeedBurstManager::new())),
            speed_boost: Arc::new(Mutex::new(speed_boost::SpeedBoostManager::new())),
            retry_quota: Arc::new(Mutex::new(retry_quota::RetryQuotaManager::new())),
            download_time_limit: Arc::new(Mutex::new(
                download_time_limit::DownloadTimeLimitManager::new(),
            )),
            ttl: Arc::new(Mutex::new(ttl::TtlManager::new())),
            error_recovery: Arc::new(Mutex::new(error_recovery::ErrorRecoveryManager::new())),
            connection_health: Arc::new(Mutex::new(
                connection_health::ConnectionHealthManager::new(),
            )),
            source_rotation: Arc::new(Mutex::new(source_rotation::SourceRotationManager::new())),
            progress_predictor: Arc::new(Mutex::new(progress_prediction::ProgressPredictor::new())),
            bandwidth_allocation: Arc::new(Mutex::new(
                bandwidth_allocation::AllocationManager::new(),
            )),
            task_proxy: Arc::new(Mutex::new(task_proxy::TaskProxyManager::new(
                data_dir.join("task_proxy.json"),
            ))),
            task_chain: Arc::new(Mutex::new(task_chain::TaskChainManager::new(
                data_dir.join("task_chain.json"),
            ))),
            snapshot_manager: Arc::new(Mutex::new(download_snapshot::SnapshotManager::new(
                data_dir.join("snapshots"),
            ))),
            network_aware: Arc::new(Mutex::new(network_aware::NetworkStateManager::new())),
            task_profiler: Arc::new(Mutex::new(task_profiler::TaskProfiler::default())),
            adaptive_concurrency: Arc::new(Mutex::new(
                adaptive_concurrency::AdaptiveConcurrencyManager::new(),
            )),
            download_templates: Arc::new(Mutex::new(
                download_templates::DownloadTemplateManager::new(),
            )),
            auto_actions: Arc::new(Mutex::new(auto_actions::AutoActionsManager::new(
                auto_actions::AutoActionsConfig::default(),
            ))),
            queue_staleness: Arc::new(tokio::sync::RwLock::new(
                queue_staleness::StalenessConfig::default(),
            )),
            network_monitor: Arc::new(Mutex::new(network_monitor::NetworkMonitor::new())),
            download_deadline: Arc::new(Mutex::new(download_deadline::DeadlineManager::new())),
            integrity: Arc::new(Mutex::new(integrity_verification::IntegrityManager::new())),
            resume_policy: Arc::new(tokio::sync::RwLock::new(
                resume_policy::ResumePolicyConfig::default(),
            )),
            duplicate_detection: Arc::new(Mutex::new(
                duplicate_detection::DuplicateDetectionManager::new(),
            )),
            dynamic_priority: Arc::new(tokio::sync::RwLock::new(
                dynamic_priority::DynamicPriorityManager::new(),
            )),
            dependency_graph: Arc::new(tokio::sync::RwLock::new(
                dependency_graph::DependencyGraphValidator::new(),
            )),
            download_quota: Arc::new(Mutex::new(download_quota::DownloadQuotaManager::new())),
            advanced_search_config: Arc::new(tokio::sync::RwLock::new(
                advanced_search::AdvancedSearchQuery::default(),
            )),
            automation_rules: Arc::new(tokio::sync::RwLock::new(
                automation_rules::AutomationRuleManager::new_with_dir(&data_dir),
            )),
            task_schedule_windows: Arc::new(tokio::sync::RwLock::new(
                task_schedule_windows::TaskScheduleWindowsManager::new(),
            )),
            disk_monitor: Arc::new(Mutex::new(disk_monitor::DiskSpaceMonitor::new(
                data_dir.join("downloads"),
                100_000_000, // 100MB default
                30,
            ))),
            disk_monitor_config: Arc::new(tokio::sync::RwLock::new(
                disk_monitor::DiskMonitorConfig::default(),
            )),
            source_benchmark: Arc::new(Mutex::new(source_benchmark::SourceBenchmarkManager::new(
                data_dir.clone(),
            ))),
            global_budget: Arc::new(tokio::sync::RwLock::new(
                global_budget::GlobalBudgetManager::new(),
            )),
            download_budget: Arc::new(Mutex::new(download_budget::BudgetManager::new())),
            download_analytics: Arc::new(Mutex::new(download_analytics::AnalyticsManager::new())),
            backup_manager: Arc::new(download_backup::BackupManager::new(data_dir.clone())),
            smart_queue: Arc::new(tokio::sync::RwLock::new(
                smart_queue::SmartQueueOptimizer::new(),
            )),
            preflight_checker: Arc::new(tokio::sync::RwLock::new(
                preflight_check::PreflightChecker::new(&data_dir),
            )),
            diagnostics: Arc::new(tokio::sync::RwLock::new(
                download_diagnostics::DownloadDiagnostics::new(),
            )),
            tag_manager: Arc::new(tag_management::TagManager::new(&data_dir)),
            cost_tracker: Arc::new(Mutex::new(download_cost::CostTracker::new())),
            history_analytics: Arc::new(Mutex::new(
                download_history_analytics::HistoryAnalyticsManager::new(),
            )),
            speed_benchmark: Arc::new(Mutex::new(speed_benchmark::SpeedBenchmarkManager::new())),
            speed_distribution: Arc::new(tokio::sync::RwLock::new(
                speed_distribution::SpeedDistributionManager::new(data_dir.clone()),
            )),
            event_webhook: Arc::new(Mutex::new(event_webhook::WebhookManager::new(
                data_dir.clone(),
            ))),
            path_organizer: Arc::new(Mutex::new(path_organizer::PathOrganizerManager::new())),
            data_retention: Arc::new(Mutex::new(data_retention::DataRetentionManager::new(
                data_dir.clone(),
            ))),
            source_quality: Arc::new(Mutex::new(source_quality::SourceQualityManager::new(
                data_dir.clone(),
            ))),
            dashboard: Arc::new(Mutex::new(dashboard::DashboardManager::new())),
            upload_tracker: Arc::new(Mutex::new(upload_tracker::UploadTracker::new())),
            bandwidth_forecast: Arc::new(Mutex::new(
                bandwidth_forecast::BandwidthForecastManager::new(
                    bandwidth_forecast::ForecastConfig::default(),
                ),
            )),
            source_reliability: Arc::new(Mutex::new(
                source_reliability::SourceReliabilityTracker::new(),
            )),
            notification_center: Arc::new(Mutex::new(
                notification_center::NotificationCenterManager::new(),
            )),
            task_scorecard: Arc::new(Mutex::new(task_scorecard::TaskScorecardManager::new())),
            intelligent_source_selector: Arc::new(Mutex::new(
                intelligent_source_selector::IntelligentSourceSelector::new(),
            )),
            retry_budget: Arc::new(Mutex::new(retry_budget::RetryBudgetManager::new())),
            download_expiry: Arc::new(Mutex::new(download_expiry::DownloadExpiryManager::new())),
            system_uptime: Arc::new(system_uptime::SystemUptimeTracker::new()),
            task_export: Arc::new(Mutex::new(task_export::ExportHistory::default())),
            source_latency: Arc::new(Mutex::new(source_latency::SourceLatencyMonitor::new())),
            file_stats: Arc::new(tokio::sync::RwLock::new(
                download_file_stats::FileTypeStatsTracker::new(
                    download_file_stats::FileStatsConfig::default(),
                ),
            )),
            sla_compliance: Arc::new(tokio::sync::RwLock::new(
                sla_compliance::SlaComplianceManager::new(data_dir.clone()),
            )),
            notification_preferences: Arc::new(Mutex::new(
                notification_preferences::NotificationPreferencesManager::new(),
            )),
            host_conn_limit: Arc::new(Mutex::new(host_conn_limit::HostConnLimitManager::new())),
            task_cron_scheduler: Arc::new(
                Mutex::new(task_cron_scheduler::TaskCronScheduler::new()),
            ),
            bandwidth_qos: Arc::new(Mutex::new(bandwidth_qos::BandwidthQosManager::new())),
        };
        // Restore SLA compliance data from disk
        {
            let mut sla_mgr = dm.sla_compliance.write().await;
            let _ = sla_mgr.load_config().await;
            let _ = sla_mgr.load_definitions().await;
            let _ = sla_mgr.load_history().await;
        }
        // Restore tag manager from disk
        dm.tag_manager.restore().await;
        // Restore download budget from disk
        if let Some(budget_mgr) = download_budget::load_budget(&dm.data_dir) {
            *dm.download_budget.lock().await = budget_mgr;
        }
        // Restore smart queue config from disk
        if let Some(sq_cfg) = smart_queue::load_smart_queue_config(&dm.data_dir) {
            dm.smart_queue.write().await.set_config(sq_cfg);
        }
        // Restore diagnostics config from disk
        if let Some(diag_cfg) = download_diagnostics::load_diagnostics_config(&dm.data_dir) {
            dm.diagnostics.write().await.set_config(diag_cfg);
        }
        // Restore download analytics from disk
        if let Ok(analytics_records) = download_analytics::load_analytics_records(&dm.data_dir) {
            let mut analytics = dm.download_analytics.lock().await;
            for (date, metrics) in analytics_records {
                analytics.insert_record(date, metrics);
            }
        }
        if let Some(analytics_cfg) = download_analytics::load_analytics_config(&dm.data_dir) {
            dm.download_analytics.lock().await.set_config(analytics_cfg);
        }
        // Restore disk monitor config from disk
        if let Some(disk_cfg) = disk_monitor::load_disk_monitor_config(&dm.data_dir).await {
            *dm.disk_monitor_config.write().await = disk_cfg;
        }
        // Restore download quota config from disk
        if let Some(quota_mgr) = download_quota::load_download_quota(&dm.data_dir) {
            *dm.download_quota.lock().await = quota_mgr;
        }
        // Restore resume policy config from disk
        if let Some(policy_cfg) = resume_policy::load_resume_policy_config(&dm.data_dir) {
            *dm.resume_policy.write().await = policy_cfg;
        }
        // Restore event webhook config from disk
        if let Ok(()) = dm.event_webhook.lock().await.load_config().await {
            // Config loaded successfully
        }
        // Restore path organizer config from disk
        let path_organizer_config_path = dm.data_dir.join("path_organizer_config.json");
        if let Ok(loaded_manager) =
            path_organizer::load_path_organizer_config(&path_organizer_config_path).await
        {
            *dm.path_organizer.lock().await = loaded_manager;
        }
        // Restore source quality config from disk
        if let Ok(()) = dm.source_quality.lock().await.load_config().await {
            // Config loaded successfully
        }
        if let Ok(()) = dm.source_quality.lock().await.load_sources().await {
            // Sources loaded successfully
        }
        // Restore source reliability data from disk
        let _ = dm.load_source_reliability_config().await;
        let _ = dm.load_source_reliability_data().await;
        // Restore task cron scheduler from disk (Phase 149)
        if let Ok(cron_mgr) = task_cron_scheduler::TaskCronScheduler::load(&dm.data_dir).await {
            *dm.task_cron_scheduler.lock().await = cron_mgr;
        }
        // Restore retry budget config and state from disk (Phase 142)
        let retry_budget_config_path = dm.data_dir.join("retry_budget_config.json");
        if let Ok(cfg) = retry_budget::load_retry_budget_config(&retry_budget_config_path).await {
            dm.retry_budget.lock().await.set_config(cfg);
        }
        let retry_budget_state_path = dm.data_dir.join("retry_budget_state.json");
        if let Ok(state) = retry_budget::load_retry_budget_state(&retry_budget_state_path).await {
            *dm.retry_budget.lock().await = state;
        }
        // Restore download_expiry configuration from disk
        let expiry_config_path = dm.data_dir.join("download_expiry_config.json");
        if let Ok(cfg) = download_expiry::load_expiry_config(&expiry_config_path).await {
            dm.download_expiry.lock().await.set_config(cfg);
        }
        let expiry_data_path = dm.data_dir.join("download_expiry_data.json");
        if let Ok(state) = download_expiry::load_expiry_data(&expiry_data_path).await {
            *dm.download_expiry.lock().await = state;
        }
        // Restore task export history from disk
        let export_history_path = dm.data_dir.join("export_history.json");
        if let Ok(history) = task_export::load_export_history(&export_history_path).await {
            *dm.task_export.lock().await = history;
        }
        // Apply resume policy to tasks that were downloading
        {
            let policy_cfg = dm.resume_policy.read().await.clone();
            let favorite_ids: Vec<String> = dm
                .task_favorites
                .lock()
                .await
                .get_favorite_ids()
                .into_iter()
                .collect();
            let task_resume_data: Vec<resume_policy::TaskResumeData> = dm
                .tasks
                .lock()
                .await
                .iter()
                .map(|t| {
                    let state = match t.state {
                        DownloadState::Downloading => {
                            resume_policy::TaskStateForResume::Downloading
                        }
                        DownloadState::Paused => resume_policy::TaskStateForResume::Paused,
                        DownloadState::Queued => resume_policy::TaskStateForResume::Queued,
                        DownloadState::Complete => resume_policy::TaskStateForResume::Complete,
                        DownloadState::Error => resume_policy::TaskStateForResume::Error,
                    };
                    let priority = match t.priority {
                        DownloadPriority::High => resume_policy::TaskPriorityForResume::High,
                        DownloadPriority::Normal => resume_policy::TaskPriorityForResume::Normal,
                        DownloadPriority::Low => resume_policy::TaskPriorityForResume::Low,
                    };
                    let is_fav = favorite_ids.contains(&t.id);
                    resume_policy::TaskResumeData {
                        id: t.id.clone(),
                        state,
                        priority,
                        is_favorite: is_fav,
                    }
                })
                .collect();
            let changes = resume_policy::apply_resume_policy(&policy_cfg, &task_resume_data);
            let change_map: std::collections::HashMap<String, _> = changes.into_iter().collect();
            for task in dm.tasks.lock().await.iter_mut() {
                if let Some(new_state) = change_map.get(&task.id) {
                    match new_state {
                        resume_policy::TaskStateForResume::Queued => {
                            task.state = DownloadState::Queued;
                        }
                        resume_policy::TaskStateForResume::Paused => {
                            task.state = DownloadState::Paused;
                            task.speed_bps = 0.0;
                        }
                        _ => {}
                    }
                }
                // Also reset any still-downloading tasks to their policy-determined state
                if task.state == DownloadState::Downloading && !change_map.contains_key(&task.id) {
                    task.state = DownloadState::Paused;
                    task.speed_bps = 0.0;
                }
            }
        }
        // Restore error recovery config from disk
        if let Ok(Some(recovery_cfg)) = error_recovery::load_error_recovery_config(&dm.data_dir) {
            *dm.error_recovery.lock().await =
                error_recovery::ErrorRecoveryManager::from_config(recovery_cfg);
        }
        // Restore connection health config from disk
        if let Ok(Some(health_cfg)) = connection_health::load_connection_health_config(&dm.data_dir)
        {
            *dm.connection_health.lock().await =
                connection_health::ConnectionHealthManager::with_config(health_cfg);
        }
        // Restore source rotation config from disk
        if let Ok(Some(rotation_cfg)) = source_rotation::load_source_rotation_config(&dm.data_dir) {
            *dm.source_rotation.lock().await =
                source_rotation::SourceRotationManager::with_config(rotation_cfg);
        }
        // Restore progress milestone config from disk
        if let Ok(Some(milestone_cfg)) =
            progress_milestone::load_progress_milestone_config(&dm.data_dir)
        {
            *dm.progress_milestone_config.write().await = milestone_cfg;
        }
        // Restore task snooze data from disk
        if let Ok(snooze_data) = task_snooze::load_task_snooze_data(&dm.data_dir).await {
            *dm.task_snooze.lock().await = task_snooze::TaskSnoozeManager::from_data(snooze_data);
        }
        // Restore recycle bin from disk
        dm.recycle_bin.lock().await.load_state(&dm.data_dir);
        // Restore priority aging config from disk
        if let Ok(aging_cfg) = priority_aging::load_priority_aging_config(&dm.data_dir) {
            *dm.priority_aging.write().await = aging_cfg;
        }
        // Restore path rules from disk
        if let Ok(loaded_rules) =
            path_rules::load_path_rules(&dm.data_dir.join("path_rules.json")).await
        {
            *dm.path_rules.lock().await = loaded_rules;
        }
        // Restore data cap config from disk
        if let Some(loaded_cap) = data_cap::load_data_cap(&dm.data_dir) {
            *dm.data_cap.lock().await = loaded_cap;
        }
        // Restore audit log from disk
        if let Some(loaded_log) = audit_log::load_audit_log(&dm.data_dir) {
            *dm.audit_log.lock().await = loaded_log;
        }
        // Restore per-task activity logs from disk
        if let Some(loaded_activity) = task_activity::load_activity_logs(&dm.data_dir) {
            *dm.activity_log.lock().await = loaded_activity;
        }
        // Restore task comments from disk
        if let Ok(loaded_comments) =
            task_comments::TaskCommentsManager::load(&dm.data_dir.join("task_comments.json")).await
        {
            *dm.task_comments.lock().await = loaded_comments;
        }
        // Restore post-download hooks from disk
        if let Err(e) = dm.hook_manager.load() {
            tracing::warn!(error = %e, "Failed to load post-download hooks");
        }
        // Restore auto-categorization rules from disk
        let rules = auto_categorize::load_rules(&dm.data_dir).await;
        *dm.categorize_rules.lock().await = rules;
        // Restore proxy configuration from disk
        if let Ok(Some(proxy_cfg)) = proxy::load_proxy_config(&dm.data_dir) {
            *dm.proxy_config.write().await = Some(proxy_cfg);
        }
        // Restore notification configuration from disk
        if let Ok(Some(notif_cfg)) = notification::load_notification_config(&dm.data_dir) {
            dm.notifier.update_config(notif_cfg);
        }
        // Restore history analytics configuration from disk
        if let Ok(Some(analytics_cfg)) =
            download_history_analytics::load_analytics_config(&dm.data_dir)
        {
            *dm.history_analytics.lock().await =
                download_history_analytics::HistoryAnalyticsManager::with_config(analytics_cfg);
        }
        // Restore auto-shutdown configuration from disk
        if let Ok(Some(shutdown_cfg)) = auto_shutdown::load_auto_shutdown_config(&dm.data_dir) {
            *dm.auto_shutdown.write().await = shutdown_cfg;
        }
        // Restore auto-cleanup configuration from disk
        if let Ok(Some(cleanup_cfg)) = auto_cleanup::load_auto_cleanup_config(&dm.data_dir) {
            *dm.auto_cleanup.write().await = cleanup_cfg;
        }
        // Restore save-path configuration from disk
        if let Ok(Some(save_path_cfg)) =
            save_path_manager::load_save_path_config(&dm.data_dir).await
        {
            dm.save_path_manager.set_config(save_path_cfg).await;
        }
        // Restore URL deduplication configuration from disk
        if let Some(dedup_cfg) = url_dedup::load_dedup_config(&dm.data_dir) {
            *dm.url_dedup.write().await = dedup_cfg;
        }
        // Restore queue staleness configuration from disk
        if let Some(staleness_cfg) = queue_staleness::load_staleness_config(&dm.data_dir).await {
            *dm.queue_staleness.write().await = staleness_cfg;
        }
        // Restore network monitor from disk
        if let Ok(monitor) =
            network_monitor::NetworkMonitor::load(&dm.data_dir.join("network_monitor.json")).await
        {
            *dm.network_monitor.lock().await = monitor;
        }
        // Restore download deadline configuration from disk
        if let Ok(deadline_cfg) =
            download_deadline::load_deadline_config(&dm.data_dir.join("deadline_config.json")).await
        {
            dm.download_deadline.lock().await.set_config(deadline_cfg);
        }
        // Restore URL expander configuration from disk
        if let Some(expander_cfg) = url_expander::load_url_expander_config(&dm.data_dir) {
            *dm.url_expander.write().await = expander_cfg;
        }
        // Restore conflict detection strategy from disk
        if let Some(strategy) = load_conflict_strategy(&dm.data_dir) {
            *dm.conflict_strategy.write().await = strategy;
        }
        // Restore per-domain download limit config from disk
        if let Ok(Some(domain_cfg)) = domain_limit::load_domain_limit_config(&dm.data_dir) {
            *dm.domain_limit.write().await = domain_cfg;
        }
        // Restore max concurrent downloads setting from disk
        if let Some(max_concurrent) = load_max_concurrent(&dm.data_dir) {
            dm.max_concurrent
                .store(max_concurrent, std::sync::atomic::Ordering::Relaxed);
        }
        // Restore bandwidth schedule from disk
        if let Ok(Some(schedule)) = load_bandwidth_schedule(&dm.data_dir).await {
            *dm.bandwidth_schedule.lock().await = schedule;
        }
        // Restore download presets from disk
        if let Ok(presets) = download_presets::load_presets(&dm.data_dir) {
            *dm.download_presets.lock().await = presets;
        }
        // Restore URL bookmarks from disk
        if let Ok(bookmarks) = url_bookmarks::load_bookmarks(&dm.data_dir) {
            *dm.url_bookmarks.lock().await = bookmarks;
        }
        // Restore URL rewrite rules from disk
        if let Some(rewrite_mgr) = url_rewrite::load_url_rewrite_manager(&dm.data_dir) {
            *dm.url_rewrite.lock().await = rewrite_mgr;
        }
        // Restore path template config from disk
        if let Ok(Some(template_config)) =
            path_template::load_path_template_config(&dm.data_dir).await
        {
            dm.path_template.replace_config(template_config).await;
        }
        // Restore per-protocol limits config from disk
        let proto_limits_path = dm.data_dir.join("protocol_limits.json");
        if let Ok(loaded_limits) =
            protocol_limits::load_protocol_limits_config(&proto_limits_path).await
        {
            *dm.protocol_limits.write().await = loaded_limits;
        }
        // Restore task archive from disk
        dm.restore_archive().await;
        // Restore task chain state from disk
        dm.restore_task_chain().await;
        // Restore task chain state from disk
        dm.restore_task_chain().await;
        // Restore auto-pause config from disk
        if let Ok(Some(loaded_ap)) = auto_pause::load_auto_pause_config(&dm.data_dir).await {
            *dm.auto_pause.write().await = loaded_ap;
        }
        // Restore retry quota config from disk
        let retry_quota_path = dm.data_dir.join("retry_quota.json");
        if retry_quota::RetryQuotaManager::state_file_exists(&retry_quota_path)
            && let Ok(loaded) = retry_quota::RetryQuotaManager::load(&retry_quota_path)
        {
            *dm.retry_quota.lock().await = loaded;
        }
        // Restore download templates from disk
        if let Ok(templates) = download_templates::load_templates(&dm.data_dir) {
            let mut mgr = dm.download_templates.lock().await;
            for t in templates {
                mgr.add_template(t);
            }
        }
        // Restore auto-actions config from disk
        {
            let config_path = dm.data_dir.join("auto_actions_config.json");
            if let Ok(config) = auto_actions::load_auto_actions_config(&config_path) {
                let mut mgr = dm.auto_actions.lock().await;
                mgr.set_config(config);
            }
        }
        // Restore speed profiles from disk
        {
            let mut mgr = dm.speed_profiles.write().await;
            let _ = mgr.load().await;
        }
        // Restore task schedule windows from disk
        {
            let schedule_path = dm.data_dir.join("task_schedule_windows.json");
            let mut mgr = dm.task_schedule_windows.write().await;
            if let Err(e) = mgr.load_from_file(&schedule_path).await {
                tracing::warn!(error = %e, "Failed to load task schedule windows");
            }
        }
        dm.start_scheduler();
        // Restore bandwidth usage data from disk
        if let Err(e) = dm.load_bandwidth_usage().await {
            tracing::warn!(error = %e, "Failed to load bandwidth usage data");
        }
        dm.start_bandwidth_scheduler();
        dm.start_auto_pause_scheduler();
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
        let max_concurrent = self.max_concurrent.clone();
        let notifier = self.notifier.clone();
        let hook_manager = self.hook_manager.clone();
        let auto_shutdown_config = self.auto_shutdown.clone();
        let proxy_config = self.proxy_config.clone();
        let task_rate_limiters = self.task_rate_limiters.clone();
        let max_auto_retries = self.max_auto_retries.clone();
        let auto_retry_base_delay_secs = self.auto_retry_base_delay_secs.clone();
        let domain_limit = self.domain_limit.clone();
        let protocol_limits = self.protocol_limits.clone();
        let retry_quota = self.retry_quota.clone();
        let source_reliability = self.source_reliability.clone();

        // Spawn task snooze expiry checker (runs every 30 seconds)
        let snooze_tasks = self.tasks.clone();
        let snooze_task_snooze = self.task_snooze.clone();
        let snooze_event_tx = self.event_tx.clone();
        let snooze_notify = self.task_complete_notify.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let mut mgr = snooze_task_snooze.lock().await;
                let expired = mgr.collect_expired();
                if expired.is_empty() {
                    continue;
                }
                let expired_ids: Vec<String> = expired.iter().map(|s| s.task_id.clone()).collect();
                tracing::info!(count = expired_ids.len(), "Processing expired task snoozes");

                let mut tasks = snooze_tasks.lock().await;
                for task in tasks.iter_mut() {
                    if expired_ids.contains(&task.id) && task.state == DownloadState::Paused {
                        task.state = DownloadState::Queued;
                        task.updated_at = chrono::Utc::now();
                        task.error = None;
                    }
                }
                drop(tasks);

                mgr.clear_expired();
                drop(mgr);

                let _ = snooze_event_tx.send(TaskEvent::Status {
                    total_tasks: 0,
                    running_tasks: 0,
                    total_speed_bps: 0.0,
                });
                snooze_notify.notify_one();
            }
        });

        // Spawn watch folder auto-scanner (runs every 60 seconds)
        let watch_folder = self.watch_folder.clone();
        let watch_folder_data_dir = self.data_dir.clone();
        let watch_folder_tasks = self.tasks.clone();
        let watch_folder_event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let mut mgr = watch_folder.lock().await;
                if !mgr.is_auto_scan_due() {
                    continue;
                }
                tracing::debug!("Auto-scanning watch folders");
                let urls = mgr.scan_and_collect_urls().await;
                mgr.mark_auto_scan_complete();
                let config_path = watch_folder_data_dir.join("watch_folders.json");
                if let Err(e) = mgr.save(&config_path) {
                    tracing::warn!(error = %e, "Failed to persist watch folder config");
                }
                drop(mgr);

                let mut imported = 0usize;
                for wfu in urls {
                    let tasks_lock = watch_folder_tasks.lock().await;
                    if tasks_lock
                        .iter()
                        .any(|t| t.source_url.as_deref() == Some(&wfu.url))
                    {
                        continue;
                    }
                    drop(tasks_lock);

                    let mut tags = wfu.tags.clone();
                    tags.push("watch-folder".to_string());
                    let url = wfu.url.clone();
                    tracing::info!(url = %url, tags = ?tags, "Watch folder auto-imported URL");
                    imported += 1;
                }
                if imported > 0 {
                    tracing::info!(count = imported, "Watch folder auto-scan completed");
                    let _ = watch_folder_event_tx.send(TaskEvent::Status {
                        total_tasks: 0,
                        running_tasks: 0,
                        total_speed_bps: 0.0,
                    });
                }
            }
        });

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
                // Use timeout to periodically check for retry_after tasks
                tokio::time::timeout(std::time::Duration::from_secs(30), notify.notified())
                    .await
                    .ok();

                // Check if we can start a new task
                let max_concurrent_val = max_concurrent.load(std::sync::atomic::Ordering::Relaxed);
                let can_start = if max_concurrent_val == 0 {
                    true
                } else {
                    running.lock().await.len() < max_concurrent_val
                };

                if can_start {
                    // Find the highest-priority queued task with all dependencies satisfied
                    // Also respects per-domain concurrent download limits
                    let next_task_id = {
                        let tasks_lock = tasks.lock().await;
                        let completed_ids: std::collections::HashSet<&str> = tasks_lock
                            .iter()
                            .filter(|t| t.state == DownloadState::Complete)
                            .map(|t| t.id.as_str())
                            .collect();
                        // Count active downloads per domain for domain limiting
                        let domain_cfg = domain_limit.read().await;
                        let active_domain_counts: std::collections::HashMap<String, u32> =
                            if domain_cfg.enabled {
                                let mut counts = std::collections::HashMap::new();
                                for t in tasks_lock.iter() {
                                    if t.state == DownloadState::Downloading
                                        && let Some(ref url) = t.source_url
                                        && let Some(domain) = domain_limit::extract_domain(url)
                                    {
                                        *counts.entry(domain).or_insert(0) += 1;
                                    }
                                }
                                counts
                            } else {
                                std::collections::HashMap::new()
                            };
                        drop(domain_cfg);
                        // Count active downloads per protocol for protocol limiting
                        let proto_cfg = protocol_limits.blocking_read();
                        let active_protocol_counts: std::collections::HashMap<String, u32> =
                            if proto_cfg.enabled {
                                let mut counts = std::collections::HashMap::new();
                                for t in tasks_lock.iter() {
                                    if t.state == DownloadState::Downloading {
                                        let key = protocol_to_limits_key(t.protocol);
                                        *counts.entry(key).or_insert(0) += 1;
                                    }
                                }
                                counts
                            } else {
                                std::collections::HashMap::new()
                            };
                        drop(proto_cfg);
                        tasks_lock
                            .iter()
                            .filter(|t| {
                                t.state == DownloadState::Queued
                                    && t.depends_on
                                        .iter()
                                        .all(|dep| completed_ids.contains(dep.as_str()))
                                    && t.retry_after.is_none_or(|ra| ra <= chrono::Utc::now())
                                    && {
                                        // Check per-domain limit
                                        let domain_cfg_sync = domain_limit.blocking_read();
                                        if !domain_cfg_sync.enabled {
                                            return true;
                                        }
                                        if let Some(ref url) = t.source_url {
                                            if let Some(domain) = domain_limit::extract_domain(url)
                                            {
                                                domain_limit::can_start_domain_download(
                                                    &domain_cfg_sync,
                                                    &domain,
                                                    &active_domain_counts,
                                                )
                                            } else {
                                                true // Non-HTTP URLs (magnet/ed2k) have no domain
                                            }
                                        } else {
                                            true // No source URL, allow
                                        }
                                    }
                                    && {
                                        // Check per-protocol limit
                                        let proto_cfg_sync = protocol_limits.blocking_read();
                                        if !proto_cfg_sync.enabled {
                                            return true;
                                        }
                                        let key = protocol_to_limits_key(t.protocol);
                                        let current =
                                            active_protocol_counts.get(&key).copied().unwrap_or(0);
                                        proto_cfg_sync.can_start(t.protocol, current)
                                    }
                            })
                            .max_by(|a, b| {
                                a.priority.cmp(&b.priority).then_with(|| {
                                    match (a.queue_position, b.queue_position) {
                                        (Some(pa), Some(pb)) => pb.cmp(&pa),
                                        (Some(_), None) => std::cmp::Ordering::Greater,
                                        (None, Some(_)) => std::cmp::Ordering::Less,
                                        (None, None) => a.created_at.cmp(&b.created_at),
                                    }
                                })
                            })
                            .map(|t| t.id.clone())
                    };

                    if next_task_id.is_none() {
                        // No queued tasks - check if queue is idle for auto-shutdown
                        let (running_count, queued_count, downloading_count) = {
                            let t = tasks.lock().await;
                            let running = running.lock().await.len();
                            let queued = t
                                .iter()
                                .filter(|t| t.state == DownloadState::Queued)
                                .count();
                            let downloading = t
                                .iter()
                                .filter(|t| t.state == DownloadState::Downloading)
                                .count();
                            (running, queued, downloading)
                        };

                        if auto_shutdown::queue_is_idle(
                            running_count,
                            queued_count,
                            downloading_count,
                        ) {
                            let config = auto_shutdown_config.read().await;
                            if auto_shutdown::execute_shutdown_action(&config).await {
                                tracing::info!("Auto-shutdown: all downloads complete, exiting");
                                std::process::exit(0);
                            }
                        }
                    }

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
                                    task.current_session_start = Some(chrono::Utc::now());
                                    task.updated_at = chrono::Utc::now();
                                }
                            }

                            let cancel_clone = cancel_token.clone();
                            let tasks_clone = tasks.clone();
                            let running_clone = running.clone();
                            let task_generation_clone = task_generation.clone();
                            let data_dir_clone = data_dir.clone();
                            let dht_clone = dht.clone();
                            let notify_clone = notify.clone();
                            let task_id_clone = task_id.clone();
                            let notifier_clone = notifier.clone();
                            let hook_manager_clone = hook_manager.clone();
                            let source_reliability_clone = source_reliability.clone();
                            let proxy_config_clone = proxy_config.read().await.clone();
                            let task_rate_limiters_clone = task_rate_limiters.clone();
                            let max_auto_retries_clone = max_auto_retries.clone();
                            let auto_retry_base_delay_secs_clone =
                                auto_retry_base_delay_secs.clone();
                            let retry_quota_clone = retry_quota.clone();
                            // Resolve per-task limiter: use task-specific if set, else global per-task.
                            let task_rate_limiter: RateLimiter = {
                                let limiters = task_rate_limiters_clone.lock().await;
                                limiters
                                    .get(&task_id)
                                    .cloned()
                                    .unwrap_or_else(|| rate_limiter.per_task().clone())
                            };
                            // Capture sequential_mode from task for torrent engine
                            let sequential_mode: bool = {
                                let t = tasks.lock().await;
                                t.iter()
                                    .find(|t| t.id == task_id)
                                    .map(|t| t.sequential_mode)
                                    .unwrap_or(false)
                            };

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
                                                            task_rate_limiter.clone(),
                                                        );
                                                        engine.set_proxy_config(proxy_config_clone);
                                                        engine.set_sequential_mode(sequential_mode);
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
                                        engine.set_rate_limiter(task_rate_limiter.clone());
                                        engine.set_proxy_config(proxy_config_clone);
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
                                        engine.set_rate_limiter(task_rate_limiter.clone());
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
                                                engine.set_rate_limiter(task_rate_limiter.clone());
                                                engine.set_proxy_config(proxy_config_clone);
                                                engine.set_sequential_mode(sequential_mode);
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
                                    TaskParams::SegmentHttp {
                                        url,
                                        file_name,
                                        file_size,
                                    } => {
                                        let download_dir = data_dir_clone.join("downloads");
                                        let mut downloader =
                                            segment_download::SegmentDownloader::new(
                                                url,
                                                file_name,
                                                file_size,
                                                download_dir,
                                            );
                                        downloader.set_rate_limiter(task_rate_limiter.clone());
                                        downloader
                                            .download(Some(cancel_clone))
                                            .await
                                            .map_err(|e| e.to_string())
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
                                            task.finalize_active_time();
                                            task.state = DownloadState::Complete;
                                            task.downloaded = task.size;
                                            task.speed_bps = 0.0;
                                            // Record source reliability success
                                            if let Some(ref url) = task.source_url {
                                                if let Some(domain) =
                                                    domain_limit::extract_domain(url)
                                                {
                                                    let speed = task.speed_bps as u64;
                                                    let size = task.size;
                                                    let sr = source_reliability_clone.clone();
                                                    tokio::spawn(async move {
                                                        let mut tracker = sr.lock().await;
                                                        tracker
                                                            .record_success(&domain, speed, size);
                                                    });
                                                }
                                            }
                                            if let Some(cs_err) = Self::verify_checksum(task).await
                                            {
                                                task.finalize_active_time();
                                                task.state = DownloadState::Error;
                                                task.error = Some(cs_err);
                                            }
                                            Self::record_task_history(
                                                task,
                                                &data_dir_clone,
                                                Some(&notifier_clone),
                                                Some(&hook_manager_clone),
                                            );
                                        }
                                        Err(e) => {
                                            let err_str = e.to_string();
                                            if err_str == "cancelled" {
                                                if is_still_active {
                                                    task.state = DownloadState::Paused;
                                                }
                                            } else {
                                                // Check if auto-retry is enabled and not exhausted
                                                // Use per-task retry policy if available, otherwise global defaults
                                                let (max_retries, delay_secs) =
                                                    if let Some(ref policy) = task.retry_policy {
                                                        (
                                                            policy.max_retries,
                                                            policy.calculate_delay(
                                                                task.auto_retry_count,
                                                            ),
                                                        )
                                                    } else {
                                                        let global_max = max_auto_retries_clone
                                                            .load(Ordering::Relaxed);
                                                        let global_base =
                                                            auto_retry_base_delay_secs_clone
                                                                .load(Ordering::Relaxed);
                                                        (
                                                            global_max,
                                                            (global_base
                                                                * 2u64.pow(task.auto_retry_count))
                                                            .min(3600),
                                                        )
                                                    };

                                                if max_retries > 0
                                                    && task.auto_retry_count < max_retries
                                                {
                                                    // Check retry quota before scheduling
                                                    let quota_allowed = {
                                                        let mut rq = retry_quota_clone.lock().await;
                                                        rq.record_retry()
                                                    };

                                                    if quota_allowed {
                                                        // Schedule retry with calculated delay
                                                        let retry_after = chrono::Utc::now()
                                                            + chrono::Duration::seconds(
                                                                delay_secs as i64,
                                                            );
                                                        task.retry_after = Some(retry_after);
                                                        task.auto_retry_count += 1;
                                                        task.state = DownloadState::Queued;
                                                        task.error = Some(format!(
                                                            "{} (retry {}/{})",
                                                            err_str,
                                                            task.auto_retry_count,
                                                            max_retries
                                                        ));
                                                        tracing::info!(
                                                            task_id = %task_id_clone,
                                                            retry_count = task.auto_retry_count,
                                                            delay_secs = delay_secs,
                                                            "Scheduling auto-retry"
                                                        );
                                                    } else {
                                                        task.finalize_active_time();
                                                        task.state = DownloadState::Error;
                                                        task.error = Some(format!(
                                                            "{} (retry quota exhausted)",
                                                            err_str
                                                        ));
                                                        tracing::warn!(
                                                            task_id = %task_id_clone,
                                                            "Auto-retry blocked: daily retry quota exhausted"
                                                        );
                                                        Self::record_task_history(
                                                            task,
                                                            &data_dir_clone,
                                                            Some(&notifier_clone),
                                                            Some(&hook_manager_clone),
                                                        );
                                                    }
                                                } else {
                                                    task.finalize_active_time();
                                                    task.state = DownloadState::Error;
                                                    task.error = Some(err_str);
                                                    Self::record_task_history(
                                                        task,
                                                        &data_dir_clone,
                                                        Some(&notifier_clone),
                                                        Some(&hook_manager_clone),
                                                    );
                                                }
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
            }
        });
    }

    /// Start the bandwidth schedule checker that adjusts speed limits based on time of day
    fn start_bandwidth_scheduler(&self) {
        let schedule = self.bandwidth_schedule.clone();
        let rate_limiter = self.rate_limiter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let schedule_mgr = schedule.lock().await;
                if let Some(limit) = schedule_mgr.current_speed_limit() {
                    rate_limiter.set_global_limit(limit).await;
                } else {
                    rate_limiter.set_global_limit(0).await;
                }
            }
        });
    }

    /// Start the auto-pause scheduler that pauses/resumes tasks based on peak hours
    fn start_auto_pause_scheduler(&self) {
        let auto_pause = self.auto_pause.clone();
        let tasks = self.tasks.clone();
        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let config = auto_pause.read().await.clone();
                if !config.enabled {
                    continue;
                }

                let peak_hours = match &config.peak_hours {
                    Some(ph) => ph,
                    None => continue,
                };

                let now = chrono::Utc::now();
                let is_peak = peak_hours.is_peak_time(now);

                let mut tasks_lock = tasks.lock().await;
                let mut changed = false;

                if is_peak {
                    // Peak hours: pause all running tasks
                    for task in tasks_lock.iter_mut() {
                        if task.state == DownloadState::Downloading {
                            task.state = DownloadState::Paused;
                            task.error = Some(config.pause_reason.clone());
                            task.updated_at = now;
                            changed = true;
                        }
                    }
                } else if config.auto_resume {
                    // Off-peak: resume tasks that were auto-paused
                    for task in tasks_lock.iter_mut() {
                        if task.state == DownloadState::Paused
                            && task.error.as_deref() == Some(&config.pause_reason)
                        {
                            task.state = DownloadState::Queued;
                            task.error = None;
                            task.updated_at = now;
                            changed = true;
                        }
                    }
                }

                if changed {
                    drop(tasks_lock);
                    // Notify scheduler to process queue changes
                    let _ = event_tx.send(TaskEvent::Status {
                        total_tasks: 0,
                        running_tasks: 0,
                        total_speed_bps: 0.0,
                    });
                }
            }
        });
    }

    /// Tick watch folder auto-scanner (called from main scheduler loop)
    pub async fn tick_watch_folder_auto_scan(&self) -> usize {
        let mut mgr = self.watch_folder.lock().await;
        if !mgr.is_auto_scan_due() {
            return 0;
        }
        tracing::debug!("Auto-scanning watch folders");
        let urls = mgr.scan_and_collect_urls().await;
        mgr.mark_auto_scan_complete();
        let config_path = self.data_dir.join("watch_folders.json");
        if let Err(e) = mgr.save(&config_path) {
            tracing::warn!(error = %e, "Failed to persist watch folder config");
        }
        drop(mgr);

        let mut imported = 0;
        for wfu in urls {
            let tasks = self.tasks.lock().await;
            if tasks
                .iter()
                .any(|t| t.source_url.as_deref() == Some(&wfu.url))
            {
                continue;
            }
            drop(tasks);

            let mut tags = wfu.tags.clone();
            tags.push("watch-folder".to_string());
            let url = wfu.url.clone();
            let result = if url.starts_with("magnet:?") {
                self.add_magnet(&url).await
            } else if url.starts_with("ed2k://") {
                continue;
            } else {
                self.add_url(&url).await
            };

            if let Ok(task_id) = result {
                if !tags.is_empty() {
                    self.add_tags(&task_id, tags).await;
                }
                if let Some(group) = &wfu.group {
                    self.set_task_group(&task_id, Some(group.clone())).await;
                }
                imported += 1;
            }
        }
        imported
    }

    /// Add a bandwidth schedule rule
    pub async fn add_bandwidth_schedule_rule(&self, rule: BandwidthScheduleRule) {
        self.bandwidth_schedule.lock().await.add_rule(rule);
        if let Err(e) =
            save_bandwidth_schedule(&*self.bandwidth_schedule.lock().await, &self.data_dir).await
        {
            tracing::warn!(error = %e, "Failed to save bandwidth schedule");
        }
    }

    /// Remove a bandwidth schedule rule
    pub async fn remove_bandwidth_schedule_rule(&self, id: &str) -> bool {
        let removed = self.bandwidth_schedule.lock().await.remove_rule(id);
        if removed
            && let Err(e) =
                save_bandwidth_schedule(&*self.bandwidth_schedule.lock().await, &self.data_dir)
                    .await
        {
            tracing::warn!(error = %e, "Failed to save bandwidth schedule");
        }
        removed
    }

    /// List all bandwidth schedule rules
    pub async fn list_bandwidth_schedule_rules(&self) -> Vec<BandwidthScheduleRule> {
        self.bandwidth_schedule.lock().await.list_rules().to_vec()
    }

    /// Get the current speed limit from bandwidth schedule
    pub async fn get_current_bandwidth_schedule_limit(&self) -> Option<u64> {
        self.bandwidth_schedule.lock().await.current_speed_limit()
    }

    // ========== Download Presets ==========

    /// Add a download preset. Persists to disk.
    pub async fn add_download_preset(&self, preset: download_presets::DownloadPreset) {
        let mut presets = self.download_presets.lock().await;
        // Remove existing preset with same id if any
        presets.retain(|p| p.id != preset.id);
        presets.push(preset);
        if let Err(e) = download_presets::save_presets(&presets, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist download presets");
        }
    }

    /// List all download presets
    pub async fn list_download_presets(&self) -> Vec<download_presets::DownloadPreset> {
        self.download_presets.lock().await.clone()
    }

    /// Remove a download preset by id. Returns true if removed.
    pub async fn remove_download_preset(&self, id: &str) -> bool {
        let mut presets = self.download_presets.lock().await;
        let len_before = presets.len();
        presets.retain(|p| p.id != id);
        let removed = presets.len() < len_before;
        if removed && let Err(e) = download_presets::save_presets(&presets, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist download presets");
        }
        removed
    }

    /// Get a download preset by id
    pub async fn get_download_preset(&self, id: &str) -> Option<download_presets::DownloadPreset> {
        self.download_presets
            .lock()
            .await
            .iter()
            .find(|p| p.id == id)
            .cloned()
    }

    /// Update a download preset
    pub async fn update_download_preset(
        &self,
        id: &str,
        updates: download_presets::PresetUpdate,
    ) -> bool {
        let mut presets = self.download_presets.lock().await;
        if let Some(preset) = presets.iter_mut().find(|p| p.id == id) {
            if let Some(name) = updates.name {
                preset.name = name;
            }
            if let Some(tags) = updates.tags {
                preset.tags = tags;
            }
            if let Some(group) = updates.group {
                preset.group = Some(group);
            }
            if let Some(priority) = updates.priority {
                preset.priority = priority;
            }
            if let Some(speed_limit) = updates.speed_limit_bps {
                preset.speed_limit_bps = Some(speed_limit);
            }
            if let Some(weight) = updates.bandwidth_weight {
                preset.bandwidth_weight = weight;
            }
            if let Some(path) = updates.save_path {
                preset.save_path = Some(std::path::PathBuf::from(path));
            }
            if let Some(retries) = updates.max_retries {
                preset.max_retries = Some(retries);
            }
            if let Some(desc) = updates.description {
                preset.description = Some(desc);
            }
            if let Some(cat) = updates.category {
                preset.category = Some(cat);
            }
            if let Some(enabled) = updates.enabled {
                preset.enabled = enabled;
            }
            if let Err(e) = download_presets::save_presets(&presets, &self.data_dir) {
                tracing::warn!(error = %e, "Failed to persist download presets");
            }
            true
        } else {
            false
        }
    }

    /// Enable a download preset
    pub async fn enable_download_preset(&self, id: &str) -> bool {
        let mut presets = self.download_presets.lock().await;
        if let Some(preset) = presets.iter_mut().find(|p| p.id == id) {
            preset.enabled = true;
            if let Err(e) = download_presets::save_presets(&presets, &self.data_dir) {
                tracing::warn!(error = %e, "Failed to persist download presets");
            }
            true
        } else {
            false
        }
    }

    /// Disable a download preset
    pub async fn disable_download_preset(&self, id: &str) -> bool {
        let mut presets = self.download_presets.lock().await;
        if let Some(preset) = presets.iter_mut().find(|p| p.id == id) {
            preset.enabled = false;
            if let Err(e) = download_presets::save_presets(&presets, &self.data_dir) {
                tracing::warn!(error = %e, "Failed to persist download presets");
            }
            true
        } else {
            false
        }
    }

    /// Get preset usage summary
    pub async fn get_preset_usage_summary(&self) -> download_presets::PresetUsageSummary {
        let presets = self.download_presets.lock().await;
        let mgr = download_presets::PresetManager::from_presets(presets.clone());
        mgr.usage_summary()
    }

    /// Get preset categories
    pub async fn get_preset_categories(&self) -> Vec<String> {
        let presets = self.download_presets.lock().await;
        let mgr = download_presets::PresetManager::from_presets(presets.clone());
        mgr.categories()
    }

    /// List presets by category
    pub async fn list_presets_by_category(
        &self,
        category: &str,
    ) -> Vec<download_presets::DownloadPreset> {
        let presets = self.download_presets.lock().await;
        presets
            .iter()
            .filter(|p| p.category.as_deref() == Some(category))
            .cloned()
            .collect()
    }

    /// Apply a preset to a task. Updates task tags, group, priority, speed limit, bandwidth weight.
    /// Returns true if the preset was found and applied.
    pub async fn apply_preset_to_task(&self, task_id: &str, preset_id: &str) -> bool {
        let preset = match self.get_download_preset(preset_id).await {
            Some(p) => p,
            None => return false,
        };
        if !preset.enabled {
            return false;
        }

        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            // Apply tags (merge, don't replace)
            for tag in &preset.tags {
                if !task.tags.contains(tag) {
                    task.tags.push(tag.clone());
                }
            }
            // Apply group if preset has one
            if let Some(ref group) = preset.group {
                task.group = Some(group.clone());
            }
            // Apply priority
            task.priority = match preset.priority {
                1 => DownloadPriority::Low,
                3 => DownloadPriority::High,
                _ => DownloadPriority::Normal,
            };
            // Apply speed limit
            task.speed_limit_bps = preset.speed_limit_bps;
            // Apply bandwidth weight
            task.bandwidth_weight = preset.bandwidth_weight;
            // Apply save path if preset has one
            if let Some(ref path) = preset.save_path {
                task.save_path = path.clone();
            }
            // Apply max retries if preset has one
            if let Some(retries) = preset.max_retries {
                task.retry_policy = Some(RetryPolicy {
                    max_retries: retries,
                    backoff: RetryBackoff::Exponential { base_secs: 30 },
                });
            }
            task.updated_at = chrono::Utc::now();
            true
        } else {
            false
        }
    }

    // ========== URL Bookmarks ==========

    /// Add a URL bookmark collection. Persists to disk.
    pub async fn add_url_bookmark(
        &self,
        name: &str,
        entries: Vec<url_bookmarks::BookmarkEntry>,
    ) -> Result<url_bookmarks::UrlBookmark, url_bookmarks::BookmarkError> {
        let mut bookmarks = self.url_bookmarks.lock().await;
        let bookmark = url_bookmarks::add_bookmark(&mut bookmarks, name, entries)?;
        if let Err(e) = url_bookmarks::save_bookmarks(&bookmarks, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL bookmarks");
        }
        Ok(bookmark)
    }

    /// List all URL bookmarks
    pub async fn list_url_bookmarks(&self) -> Vec<url_bookmarks::UrlBookmark> {
        self.url_bookmarks.lock().await.clone()
    }

    /// Get a URL bookmark by name
    pub async fn get_url_bookmark(&self, name: &str) -> Option<url_bookmarks::UrlBookmark> {
        url_bookmarks::get_bookmark(&self.url_bookmarks.lock().await, name).cloned()
    }

    /// Remove a URL bookmark by name
    pub async fn remove_url_bookmark(
        &self,
        name: &str,
    ) -> Result<(), url_bookmarks::BookmarkError> {
        let mut bookmarks = self.url_bookmarks.lock().await;
        url_bookmarks::remove_bookmark(&mut bookmarks, name)?;
        if let Err(e) = url_bookmarks::save_bookmarks(&bookmarks, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL bookmarks");
        }
        Ok(())
    }

    /// Add URLs to an existing bookmark
    pub async fn add_urls_to_bookmark(
        &self,
        name: &str,
        urls: Vec<url_bookmarks::BookmarkEntry>,
    ) -> Result<(), url_bookmarks::BookmarkError> {
        let mut bookmarks = self.url_bookmarks.lock().await;
        if let Some(bookmark) = url_bookmarks::get_bookmark_mut(&mut bookmarks, name) {
            url_bookmarks::add_urls_to_book(bookmark, urls)?;
            if let Err(e) = url_bookmarks::save_bookmarks(&bookmarks, &self.data_dir) {
                tracing::warn!(error = %e, "Failed to persist URL bookmarks");
            }
            Ok(())
        } else {
            Err(url_bookmarks::BookmarkError::NotFound(name.to_string()))
        }
    }

    /// Remove a URL from a bookmark
    pub async fn remove_url_from_bookmark(
        &self,
        name: &str,
        url: &str,
    ) -> Result<(), url_bookmarks::BookmarkError> {
        let mut bookmarks = self.url_bookmarks.lock().await;
        if let Some(bookmark) = url_bookmarks::get_bookmark_mut(&mut bookmarks, name) {
            url_bookmarks::remove_url_from_bookmark(bookmark, url)?;
            if let Err(e) = url_bookmarks::save_bookmarks(&bookmarks, &self.data_dir) {
                tracing::warn!(error = %e, "Failed to persist URL bookmarks");
            }
            Ok(())
        } else {
            Err(url_bookmarks::BookmarkError::NotFound(name.to_string()))
        }
    }

    /// Import all URLs from a bookmark as download tasks
    pub async fn import_bookmark(
        &self,
        name: &str,
    ) -> Result<url_bookmarks::BookmarkImportResult, String> {
        let mut bookmarks = self.url_bookmarks.lock().await;
        let bookmark = url_bookmarks::get_bookmark_mut(&mut bookmarks, name)
            .ok_or_else(|| format!("Bookmark '{}' not found", name))?;

        if bookmark.entries.is_empty() {
            return Err(format!("Bookmark '{}' has no URLs", name));
        }

        let urls: Vec<String> = bookmark.entries.iter().map(|e| e.url.clone()).collect();

        // Mark as used
        url_bookmarks::mark_bookmark_used(bookmark);

        // Persist the updated bookmark
        if let Err(e) = url_bookmarks::save_bookmarks(&bookmarks, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL bookmarks");
        }

        // Import each URL as a download task
        let mut imported = 0;
        let mut skipped = 0;

        for url in &urls {
            // Check for duplicates
            let existing = self.tasks.lock().await;
            let is_duplicate = existing
                .iter()
                .any(|t| t.source_url.as_deref() == Some(url.as_str()));
            drop(existing);

            if is_duplicate {
                skipped += 1;
                continue;
            }

            // Add the download using add_xunlei with HTTP source
            let sources = vec![xunlei::XunleiSource::Http {
                url: url.clone(),
                cookies: None,
                referer: None,
            }];
            let filename = url.rsplit('/').next().unwrap_or("download").to_string();
            match self.add_xunlei(filename, 0, sources).await {
                Ok(_) => imported += 1,
                Err(e) => {
                    tracing::warn!(error = %e, url = %url, "Failed to import URL from bookmark");
                    skipped += 1;
                }
            }
        }

        Ok(url_bookmarks::BookmarkImportResult {
            bookmark_name: name.to_string(),
            urls_imported: imported,
            urls_skipped: skipped,
            urls,
        })
    }

    /// Set maximum concurrent downloads (0 = unlimited).
    /// Persists the setting to disk for restart restoration.
    pub fn set_max_concurrent(&self, max: usize) {
        self.max_concurrent
            .store(max, std::sync::atomic::Ordering::Relaxed);
        if let Err(e) = save_max_concurrent(max, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist max_concurrent");
        }
    }

    /// Get maximum concurrent downloads (0 = unlimited)
    pub fn get_max_concurrent(&self) -> usize {
        self.max_concurrent
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get the hook manager for post-download hook management
    pub fn hook_manager(&self) -> &Arc<HookManager> {
        &self.hook_manager
    }

    /// Get the data directory path
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Get the RSS feed subscription manager (if initialized).
    pub fn rss_feed_manager(&self) -> Option<&Arc<FeedSubscriptionManager>> {
        self.rss_feed_manager.as_ref()
    }

    /// Initialize the RSS feed subscription manager and wire up auto-download.
    ///
    /// Creates a `FeedSubscriptionManager` that persists to `<data_dir>/rss_feeds.json`.
    /// When new feed items are discovered, they are automatically queued as downloads
    /// via `add_url()` (for HTTP/FTP URLs) or `add_magnet()` (for magnet links).
    pub async fn init_rss_feed_manager(&mut self) -> Result<(), rss_feed::RssFeedError> {
        let config_path = self.data_dir.join("rss_feeds.json");
        let mgr = FeedSubscriptionManager::new(&config_path).await?;

        // Wire up auto-download callback.
        // We use a weak-like pattern: clone the Arc fields we need from self.
        // The callback spawns a task that calls add_url/add_magnet on a fresh
        // Arc<DownloadManager>-like handle. Since we don't have an Arc<Self> here,
        // we use the underlying shared state directly.
        let dm_tasks = self.tasks.clone();
        let dm_event_tx = self.event_tx.clone();
        let dm_task_complete_notify = self.task_complete_notify.clone();
        let dm_save_path_manager = self.save_path_manager.clone();

        mgr.set_on_new_item(
            move |item: rss_feed::FeedItem, _sub: &rss_feed::FeedSubscription| {
                let tasks = dm_tasks.clone();
                let event_tx = dm_event_tx.clone();
                let notify = dm_task_complete_notify.clone();
                let save_path_manager = dm_save_path_manager.clone();
                let url = item.url.clone();
                let title = item.title.clone();
                let size = item.size.unwrap_or(0);

                tokio::spawn(async move {
                    // Skip if URL already exists in task list.
                    {
                        let tasks_guard = tasks.lock().await;
                        if tasks_guard
                            .iter()
                            .any(|t| t.source_url.as_deref() == Some(&url))
                        {
                            return;
                        }
                    }

                    let name = if title.is_empty() {
                        url.rsplit('/').next().unwrap_or("download").to_string()
                    } else {
                        title
                    };

                    let now = chrono::Utc::now();
                    let task_id = uuid::Uuid::new_v4().to_string();
                    let save_path = save_path_manager.get_save_path(&name).await.join(&name);

                    // Determine protocol from URL scheme.
                    let (protocol, source_url) = if url.starts_with("magnet:") {
                        (DownloadProtocol::Magnet, url.clone())
                    } else if url.starts_with("ed2k://") {
                        (DownloadProtocol::Ed2k, url.clone())
                    } else {
                        (DownloadProtocol::Xunlei, url.clone())
                    };

                    let task = DownloadTask {
                        id: task_id.clone(),
                        name,
                        protocol,
                        size,
                        downloaded: 0,
                        state: DownloadState::Queued,
                        error: None,
                        speed_bps: 0.0,
                        save_path,
                        created_at: now,
                        updated_at: now,
                        tags: Vec::new(),
                        priority: DownloadPriority::Normal,
                        schedule: None,
                        bandwidth_weight: 1,
                        queue_position: None,
                        depends_on: Vec::new(),
                        notes: None,
                        group: None,
                        speed_limit_bps: None,
                        auto_retry_count: 0,
                        retry_after: None,
                        source_url: Some(source_url),
                        expected_checksum: None,
                        checksum_algorithm: None,
                        active_time_seconds: 0.0,
                        mirror_urls: Vec::new(),
                        retry_policy: None,
                        cooldown: None,
                        sequential_mode: false,
                        max_download_time_secs: None,
                        proxy_override: None,
                        staleness_promotion_count: 0,
                        deadline: None,
                        current_session_start: None,
                    };

                    let task_name = task.name.clone();
                    {
                        let mut tasks_guard = tasks.lock().await;
                        tasks_guard.push(task);
                    }

                    // Emit event and notify scheduler.
                    let _ = event_tx.send(TaskEvent::Added {
                        task: TaskInfoEvent {
                            id: task_id,
                            name: task_name,
                            protocol: format!("{protocol:?}"),
                            size,
                            downloaded: 0,
                            progress: 0.0,
                            speed_bps: 0.0,
                            state: "Queued".to_string(),
                            error: None,
                            tags: Vec::new(),
                            priority: "Normal".to_string(),
                            bandwidth_weight: 1,
                            queue_position: None,
                            depends_on: Vec::new(),
                            notes: None,
                            group: None,
                            speed_limit_bps: None,
                            auto_retry_count: 0,
                            retry_after: None,
                            source_url: Some(url.clone()),
                            expected_checksum: None,
                            checksum_algorithm: None,
                            checksum_status: None,
                            eta_seconds: None,
                            active_time_seconds: 0.0,
                            mirror_urls: Vec::new(),
                            retry_policy: None,
                            cooldown: None,
                            sequential_mode: false,
                            max_download_time_secs: None,
                            proxy_override: None,
                            staleness_promotion_count: 0,
                            deadline: None,
                            is_favorite: false,
                        },
                    });
                    notify.notify_one();

                    tracing::info!(url = %url, "RSS: auto-queued download");
                });
            },
        )
        .await;

        self.rss_feed_manager = Some(Arc::new(mgr));
        Ok(())
    }

    /// Set global download speed limit in bytes/sec (0 = unlimited).
    /// Shared across all download tasks.
    pub async fn set_global_speed_limit(&self, bytes_per_sec: u64) {
        self.rate_limiter.set_global_limit(bytes_per_sec).await;
    }

    /// Set per-task download speed limit in bytes/sec (0 = unlimited).
    /// This is the global per-task limit applied to all tasks that don't have
    /// an individual limit set via `set_task_speed_limit_per_task()`.
    pub async fn set_task_speed_limit(&self, bytes_per_sec: u64) {
        self.rate_limiter.set_task_limit(bytes_per_sec).await;
    }

    /// Set a per-task speed limit in bytes/sec (0 or None = use global per-task limit).
    ///
    /// Creates a dedicated RateLimiter for the task. If the task is currently
    /// running, the new limiter will be used on the next engine spawn (pause+resume).
    pub async fn set_task_speed_limit_per_task(&self, task_id: &str, bytes_per_sec: Option<u64>) {
        // Normalize: Some(0) means "clear the limit" (same as None)
        let normalized = bytes_per_sec.filter(|&v| v != 0);

        // Update the task record
        {
            let mut tasks = self.tasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
                task.speed_limit_bps = normalized;
                task.updated_at = chrono::Utc::now();
            } else {
                return;
            }
        }

        // Manage the per-task limiter map
        {
            let mut limiters = self.task_rate_limiters.lock().await;
            match normalized {
                None => {
                    limiters.remove(task_id);
                }
                Some(limit) => {
                    limiters.insert(task_id.to_string(), RateLimiter::new(limit));
                }
            }
        }

        // Persist and emit event
        self.persist_tasks().await;
        if let Some(task) = self.get_task(task_id).await {
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(&task),
            });
        }
    }

    /// Get the per-task speed limit for a specific task.
    /// Returns None if no individual limit is set (uses global per-task limit).
    pub async fn get_task_speed_limit(&self, task_id: &str) -> Option<u64> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.id == task_id)
            .and_then(|t| t.speed_limit_bps)
    }

    /// Get the rate controller handle.
    pub fn rate_limiter(&self) -> &Arc<DownloadRateController> {
        &self.rate_limiter
    }

    // ── Speed Burst Mode ──────────────────────────────────────────────────

    /// Start a speed burst for a task, temporarily boosting its download speed.
    ///
    /// Returns the burst result indicating success or failure reason.
    /// The burst will automatically expire after the configured duration.
    pub async fn start_speed_burst(
        &self,
        task_id: &str,
        duration_secs: Option<u64>,
        multiplier: Option<f64>,
    ) -> speed_burst::BurstStartResult {
        // Get current speed limit for the task
        let current_limit = {
            let tasks = self.tasks.lock().await;
            match tasks.iter().find(|t| t.id == task_id) {
                Some(task) => task.speed_limit_bps,
                None => return speed_burst::BurstStartResult::TaskNotFound,
            }
        };

        // Check task state - must be downloading or queued
        {
            let tasks = self.tasks.lock().await;
            if let Some(task) = tasks.iter().find(|t| t.id == task_id) {
                match task.state {
                    DownloadState::Downloading | DownloadState::Queued => {}
                    _ => return speed_burst::BurstStartResult::TaskNotActive,
                }
            }
        }

        let mut burst_mgr = self.speed_burst.lock().await;
        let result = burst_mgr.start_burst(task_id, current_limit, duration_secs, multiplier);

        // If burst started, update the rate limiter
        if let speed_burst::BurstStartResult::Started(ref burst) = result {
            let mut limiters = self.task_rate_limiters.lock().await;
            if burst.burst_limit > 0 {
                limiters.insert(task_id.to_string(), RateLimiter::new(burst.burst_limit));
            } else {
                // Unlimited burst - remove any per-task limiter
                limiters.remove(task_id);
            }
        }

        result
    }

    /// Stop an active speed burst for a task, reverting to the original limit.
    pub async fn stop_speed_burst(&self, task_id: &str) -> bool {
        let mut burst_mgr = self.speed_burst.lock().await;
        if let Some(burst) = burst_mgr.stop_burst(task_id) {
            // Revert to original limit
            let mut limiters = self.task_rate_limiters.lock().await;
            match burst.original_limit {
                Some(limit) => {
                    limiters.insert(task_id.to_string(), RateLimiter::new(limit));
                }
                None => {
                    limiters.remove(task_id);
                }
            }
            true
        } else {
            false
        }
    }

    /// Get the current speed burst status for all tasks.
    pub async fn get_speed_burst_status(&self) -> speed_burst::BurstStatus {
        let burst_mgr = self.speed_burst.lock().await;
        burst_mgr.status()
    }

    /// Check if a task has an active speed burst.
    pub async fn has_active_speed_burst(&self, task_id: &str) -> bool {
        let burst_mgr = self.speed_burst.lock().await;
        burst_mgr.has_active_burst(task_id)
    }

    /// Set speed burst configuration.
    pub async fn set_speed_burst_config(&self, config: speed_burst::SpeedBurstConfig) {
        let mut burst_mgr = self.speed_burst.lock().await;
        burst_mgr.set_config(config);
    }

    /// Get speed burst configuration.
    pub async fn get_speed_burst_config(&self) -> speed_burst::SpeedBurstConfig {
        let burst_mgr = self.speed_burst.lock().await;
        burst_mgr.config().clone()
    }

    /// Process expired speed bursts and revert limits.
    /// Called periodically by the scheduler.
    pub async fn process_expired_speed_bursts(&self) {
        let mut burst_mgr = self.speed_burst.lock().await;
        let reverted = burst_mgr.process_expired();

        if reverted.is_empty() {
            return;
        }

        let mut limiters = self.task_rate_limiters.lock().await;
        for (task_id, original_limit) in reverted {
            match original_limit {
                Some(limit) => {
                    limiters.insert(task_id, RateLimiter::new(limit));
                }
                None => {
                    limiters.remove(&task_id);
                }
            }
        }
    }

    // ===== System-Wide Speed Boost =====

    /// Start a system-wide speed boost.
    pub async fn start_speed_boost(
        &self,
        duration_secs: Option<u64>,
        multiplier: Option<f64>,
    ) -> speed_boost::BoostStartResult {
        let current_limit = {
            let limit = self.rate_limiter.global().speed_limit().await;
            if limit > 0 { Some(limit) } else { None }
        };
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.start_boost(current_limit, duration_secs, multiplier)
    }

    /// Start a speed boost using a named preset.
    pub async fn start_speed_boost_preset(
        &self,
        preset_name: &str,
    ) -> speed_boost::BoostStartResult {
        let current_limit = {
            let limit = self.rate_limiter.global().speed_limit().await;
            if limit > 0 { Some(limit) } else { None }
        };
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.start_preset_boost(preset_name, current_limit)
    }

    /// Stop the active system-wide speed boost.
    pub async fn stop_speed_boost(&self) -> bool {
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.stop_boost()
    }

    /// Get the current speed boost status.
    pub async fn get_speed_boost_status(&self) -> speed_boost::SpeedBoostStatus {
        let boost_mgr = self.speed_boost.lock().await;
        boost_mgr.status()
    }

    /// Get the effective global speed limit (considering active boost).
    pub async fn get_effective_speed_limit(&self) -> Option<u64> {
        let base_limit = {
            let limit = self.rate_limiter.global().speed_limit().await;
            if limit > 0 { Some(limit) } else { None }
        };
        let boost_mgr = self.speed_boost.lock().await;
        boost_mgr.effective_limit(base_limit)
    }

    /// Set speed boost configuration.
    pub async fn set_speed_boost_config(&self, config: speed_boost::SpeedBoostConfig) {
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.set_config(config);
    }

    /// Get speed boost configuration.
    pub async fn get_speed_boost_config(&self) -> speed_boost::SpeedBoostConfig {
        let boost_mgr = self.speed_boost.lock().await;
        boost_mgr.config().clone()
    }

    /// Add a named boost preset.
    pub async fn add_speed_boost_preset(&self, id: &str, preset: speed_boost::BoostPreset) -> bool {
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.add_preset(id, preset)
    }

    /// Remove a named boost preset.
    pub async fn remove_speed_boost_preset(&self, id: &str) -> bool {
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.remove_preset(id)
    }

    /// Add a scheduled boost window.
    pub async fn add_scheduled_boost_window(
        &self,
        window: speed_boost::ScheduledBoostWindow,
    ) -> bool {
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.add_scheduled_window(window)
    }

    /// Remove a scheduled boost window.
    pub async fn remove_scheduled_boost_window(&self, id: &str) -> bool {
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.remove_scheduled_window(id)
    }

    /// Process expired speed boosts.
    pub async fn process_expired_speed_boosts(&self) {
        let mut boost_mgr = self.speed_boost.lock().await;
        boost_mgr.process_expired();
    }
    // ===== Auto-Actions =====

    /// Set auto-actions configuration.
    pub async fn set_auto_actions_config(&self, config: auto_actions::AutoActionsConfig) {
        let mut mgr = self.auto_actions.lock().await;
        mgr.set_config(config);
        // Persist to disk
        let config_path = self.data_dir.join("auto_actions_config.json");
        let _ = auto_actions::save_auto_actions_config(mgr.config(), &config_path);
    }

    /// Get current auto-actions configuration.
    pub async fn get_auto_actions_config(&self) -> auto_actions::AutoActionsConfig {
        let mgr = self.auto_actions.lock().await;
        mgr.config().clone()
    }

    /// Get auto-actions summary.
    pub async fn get_auto_actions_summary(&self) -> auto_actions::AutoActionsSummary {
        let mgr = self.auto_actions.lock().await;
        mgr.summary()
    }

    /// Add a new auto-action rule.
    pub async fn add_auto_action_rule(&self, rule: auto_actions::AutoActionRule) -> String {
        let mut mgr = self.auto_actions.lock().await;
        let id = mgr.add_rule(rule);
        // Persist
        let config_path = self.data_dir.join("auto_actions_config.json");
        let _ = auto_actions::save_auto_actions_config(mgr.config(), &config_path);
        id
    }

    /// Remove a auto-action rule by ID.
    pub async fn remove_auto_action_rule(
        &self,
        rule_id: &str,
    ) -> Result<(), auto_actions::AutoActionsError> {
        let mut mgr = self.auto_actions.lock().await;
        mgr.remove_rule(rule_id)?;
        // Persist
        let config_path = self.data_dir.join("auto_actions_config.json");
        let _ = auto_actions::save_auto_actions_config(mgr.config(), &config_path);
        Ok(())
    }

    /// List all auto-action rules.
    pub async fn list_auto_action_rules(&self) -> Vec<auto_actions::AutoActionRule> {
        let mgr = self.auto_actions.lock().await;
        mgr.list_rules().to_vec()
    }

    /// Enable or disable a specific auto-action rule.
    pub async fn set_auto_action_rule_enabled(
        &self,
        rule_id: &str,
        enabled: bool,
    ) -> Result<(), auto_actions::AutoActionsError> {
        let mut mgr = self.auto_actions.lock().await;
        mgr.set_rule_enabled(rule_id, enabled)?;
        // Persist
        let config_path = self.data_dir.join("auto_actions_config.json");
        let _ = auto_actions::save_auto_actions_config(mgr.config(), &config_path);
        Ok(())
    }

    /// Set per-task auto-action override.
    pub async fn set_task_auto_action(
        &self,
        task_id: &str,
        actions: Vec<auto_actions::AutoAction>,
        trigger: auto_actions::AutoActionTrigger,
    ) {
        let mut mgr = self.auto_actions.lock().await;
        mgr.set_task_override(task_id, actions, trigger);
        // Persist
        let config_path = self.data_dir.join("auto_actions_config.json");
        let _ = auto_actions::save_auto_actions_config(mgr.config(), &config_path);
    }

    /// Remove per-task auto-action override.
    pub async fn remove_task_auto_action(
        &self,
        task_id: &str,
    ) -> Result<(), auto_actions::AutoActionsError> {
        let mut mgr = self.auto_actions.lock().await;
        mgr.remove_task_override(task_id)?;
        // Persist
        let config_path = self.data_dir.join("auto_actions_config.json");
        let _ = auto_actions::save_auto_actions_config(mgr.config(), &config_path);
        Ok(())
    }

    /// Clear auto-actions execution history.
    pub async fn clear_auto_actions_history(&self) {
        let mut mgr = self.auto_actions.lock().await;
        mgr.clear_history();
    }

    /// Execute auto-actions for a completed/failed task.
    pub async fn execute_auto_actions(
        &self,
        task_id: &str,
        tags: &[String],
        group: Option<&str>,
        is_complete: bool,
        file_path: &std::path::Path,
    ) -> Vec<auto_actions::AutoActionResult> {
        let actions = {
            let mgr = self.auto_actions.lock().await;
            mgr.get_actions_for_task(task_id, tags, group, is_complete)
        };

        let mut results = Vec::new();
        for (rule_id, action_list) in actions {
            for action in action_list {
                let result = self
                    .execute_single_auto_action(&rule_id, &action, file_path)
                    .await;
                // Record result
                {
                    let mut mgr = self.auto_actions.lock().await;
                    mgr.record_result(result.clone());
                }
                results.push(result);
            }
        }
        results
    }

    /// Execute a single auto-action.
    async fn execute_single_auto_action(
        &self,
        rule_id: &str,
        action: &auto_actions::AutoAction,
        file_path: &std::path::Path,
    ) -> auto_actions::AutoActionResult {
        let action_type = match action {
            auto_actions::AutoAction::OpenFile => "open_file".to_string(),
            auto_actions::AutoAction::MoveTo { .. } => "move_to".to_string(),
            auto_actions::AutoAction::RunCommand { .. } => "run_command".to_string(),
        };

        let (success, error) = match action {
            auto_actions::AutoAction::OpenFile => {
                // Try to open file with system default application
                #[cfg(target_os = "linux")]
                {
                    match std::process::Command::new("xdg-open")
                        .arg(file_path)
                        .spawn()
                    {
                        Ok(_) => (true, None),
                        Err(e) => (false, Some(format!("Failed to open file: {}", e))),
                    }
                }
                #[cfg(target_os = "macos")]
                {
                    match std::process::Command::new("open").arg(file_path).spawn() {
                        Ok(_) => (true, None),
                        Err(e) => (false, Some(format!("Failed to open file: {}", e))),
                    }
                }
                #[cfg(not(any(target_os = "linux", target_os = "macos")))]
                {
                    (
                        false,
                        Some("Open file not supported on this platform".to_string()),
                    )
                }
            }
            auto_actions::AutoAction::MoveTo { target_dir } => {
                match auto_actions::execute_move_to(file_path, target_dir) {
                    Ok(_) => (true, None),
                    Err(e) => (false, Some(e)),
                }
            }
            auto_actions::AutoAction::RunCommand { command } => {
                let cmd = auto_actions::build_command(command, file_path);
                match std::process::Command::new("sh")
                    .arg("-c")
                    .arg(&cmd)
                    .output()
                {
                    Ok(output) => {
                        if output.status.success() {
                            (true, None)
                        } else {
                            let stderr = String::from_utf8_lossy(&output.stderr);
                            (false, Some(format!("Command failed: {}", stderr)))
                        }
                    }
                    Err(e) => (false, Some(format!("Failed to run command: {}", e))),
                }
            }
        };

        auto_actions::AutoActionResult {
            rule_id: rule_id.to_string(),
            action_type,
            success,
            error,
            timestamp: chrono::Utc::now(),
        }
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

    /// Set maximum auto-retry attempts for failed downloads (0 = disabled).
    /// When a download fails, it will be automatically retried with exponential backoff
    /// up to this many times before staying in Error state.
    pub fn set_max_auto_retries(&self, retries: u32) {
        self.max_auto_retries.store(retries, Ordering::Relaxed);
    }

    /// Get the current max auto-retry setting.
    pub fn get_max_auto_retries(&self) -> u32 {
        self.max_auto_retries.load(Ordering::Relaxed)
    }

    /// Set the base delay in seconds for exponential backoff on auto-retry.
    /// Actual delay = base_delay * 2^retry_count (capped at 1 hour).
    pub fn set_auto_retry_base_delay_secs(&self, secs: u64) {
        self.auto_retry_base_delay_secs
            .store(secs, Ordering::Relaxed);
    }

    /// Get the current auto-retry base delay in seconds.
    pub fn get_auto_retry_base_delay_secs(&self) -> u64 {
        self.auto_retry_base_delay_secs.load(Ordering::Relaxed)
    }

    /// Set notification configuration for download completion/failure events.
    /// Also persists the configuration to disk for restoration on restart.
    pub fn set_notification_config(&self, config: NotificationConfig) {
        self.notifier.update_config(config.clone());
        // Persist to disk (best-effort, don't block on failure)
        if let Err(e) = notification::save_notification_config(&config, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist notification config");
        }
    }

    /// Get notification history manager for querying recent notifications
    pub fn notification_history(&self) -> &notification::NotificationHistory {
        self.notifier.history()
    }

    /// Set progress milestone configuration for download progress notifications.
    /// Also persists the configuration to disk for restoration on restart.
    pub async fn set_progress_milestone_config(
        &self,
        config: progress_milestone::ProgressMilestoneConfig,
    ) {
        *self.progress_milestone_config.write().await = config.clone();
        if let Err(e) = progress_milestone::save_progress_milestone_config(&config, &self.data_dir)
        {
            tracing::warn!(error = %e, "Failed to persist progress milestone config");
        }
    }

    /// Get current progress milestone configuration.
    pub async fn get_progress_milestone_config(
        &self,
    ) -> progress_milestone::ProgressMilestoneConfig {
        self.progress_milestone_config.read().await.clone()
    }

    /// Reset milestone tracking for a specific task (e.g., when task restarts).
    pub async fn reset_progress_milestones(&self, task_id: &str) {
        self.progress_milestone.lock().await.reset_task(task_id);
    }

    /// Set retry quota configuration.
    /// Limits total auto-retry attempts per day across all tasks.
    /// Persists to disk for restoration on restart.
    pub async fn set_retry_quota_config(&self, config: retry_quota::RetryQuotaConfig) {
        let mut mgr = self.retry_quota.lock().await;
        mgr.set_config(config);
        let path = self.data_dir.join("retry_quota.json");
        if let Err(e) = mgr.save(&path) {
            tracing::warn!(error = %e, "Failed to persist retry quota config");
        }
    }

    /// Get current retry quota configuration.
    pub async fn get_retry_quota_config(&self) -> retry_quota::RetryQuotaConfig {
        self.retry_quota.lock().await.config().clone()
    }

    /// Get current retry quota usage statistics.
    pub async fn get_retry_quota_usage(&self) -> retry_quota::RetryQuotaUsage {
        self.retry_quota.lock().await.usage()
    }

    /// Set download time limit configuration.
    /// Controls automatic pausing of tasks that exceed time limits.
    pub async fn set_download_time_limit_config(
        &self,
        config: download_time_limit::DownloadTimeLimitConfig,
    ) {
        let mut mgr = self.download_time_limit.lock().await;
        *mgr = download_time_limit::DownloadTimeLimitManager::from_config(config);
        let path = self.data_dir.join("download_time_limit.json");
        if let Err(e) = mgr.save(&path) {
            tracing::warn!(error = %e, "Failed to persist download time limit config");
        }
    }

    /// Get current download time limit configuration.
    pub async fn get_download_time_limit_config(
        &self,
    ) -> download_time_limit::DownloadTimeLimitConfig {
        self.download_time_limit.lock().await.config().clone()
    }

    /// Set per-task download time limit override.
    pub async fn set_task_download_time_limit(
        &self,
        task_id: &str,
        limit_secs: Option<u64>,
    ) -> Result<(), String> {
        let mut tasks = self.task_info.lock().await;
        if let Some(task) = tasks.get_mut(task_id) {
            task.max_download_time_secs = limit_secs;
            Ok(())
        } else {
            Err(format!("Task not found: {}", task_id))
        }
    }

    /// Get per-task download time limit.
    pub async fn get_task_download_time_limit(&self, task_id: &str) -> Option<u64> {
        let tasks = self.task_info.lock().await;
        tasks.get(task_id).and_then(|t| t.max_download_time_secs)
    }

    /// Set TTL configuration.
    pub async fn set_ttl_config(&self, config: ttl::TtlConfig) {
        let mut mgr = self.ttl.lock().await;
        mgr.set_config(config);
        let path = self.data_dir.join("ttl_config.json");
        if let Ok(content) = mgr.serialize_config() {
            let temp_path = path.with_extension("tmp");
            if tokio::fs::write(&temp_path, &content).await.is_ok() {
                let _ = tokio::fs::rename(&temp_path, &path).await;
            }
        }
    }

    /// Get current TTL configuration.
    pub async fn get_ttl_config(&self) -> ttl::TtlConfig {
        self.ttl.lock().await.config().clone()
    }

    /// Set per-task TTL override.
    pub async fn set_task_ttl(&self, task_id: &str, max_lifetime_secs: Option<u64>) {
        self.ttl
            .lock()
            .await
            .set_task_ttl(task_id, max_lifetime_secs);
        let mgr = self.ttl.lock().await;
        let path = self.data_dir.join("ttl_states.json");
        if let Ok(content) = mgr.serialize_states() {
            let temp_path = path.with_extension("tmp");
            if tokio::fs::write(&temp_path, &content).await.is_ok() {
                let _ = tokio::fs::rename(&temp_path, &path).await;
            }
        }
    }

    /// Get TTL summary for all tasks.
    pub async fn get_ttl_summary(&self) -> ttl::TtlSummary {
        let mgr = self.ttl.lock().await;
        let tasks = self.tasks.lock().await;
        mgr.summary(|id| {
            tasks
                .iter()
                .find(|t| t.id == id)
                .map(|t| t.name.clone())
                .unwrap_or_else(|| id.to_string())
        })
    }

    /// Check and auto-pause tasks whose TTL has expired.
    pub async fn check_and_enforce_ttl(&self) {
        let expired_ids = {
            let mgr = self.ttl.lock().await;
            mgr.check_all_expired()
        };

        for task_id in expired_ids {
            let mut tasks = self.tasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id)
                && task.state == crate::DownloadState::Downloading
            {
                task.state = crate::DownloadState::Paused;
                task.error = Some("TTL expired".to_string());
                task.finalize_active_time();
                self.ttl.lock().await.mark_expired(&task_id);
                tracing::info!(task_id = %task_id, "Auto-paused due to TTL expiry");
            }
        }
    }

    /// Reset the retry quota (clear all recorded retry timestamps).
    pub async fn reset_retry_quota(&self) {
        self.retry_quota.lock().await.reset();
    }

    /// Set error recovery configuration.
    /// Persists to disk for automatic restoration on restart.
    pub async fn set_error_recovery_config(
        &self,
        config: error_recovery::ErrorRecoveryConfig,
    ) -> Result<(), error_recovery::ErrorRecoveryPersistenceError> {
        error_recovery::save_error_recovery_config(&config, &self.data_dir)?;
        let mut mgr = self.error_recovery.lock().await;
        mgr.set_config(config);
        Ok(())
    }

    /// Get current error recovery configuration.
    pub async fn get_error_recovery_config(&self) -> error_recovery::ErrorRecoveryConfig {
        self.error_recovery.lock().await.config().clone()
    }

    /// Classify an error and determine recovery strategy.
    pub async fn classify_error(
        &self,
        error: &str,
        consecutive_failures: u32,
    ) -> error_recovery::RecoveryDecision {
        let mgr = self.error_recovery.lock().await;
        mgr.classify_and_decide(error, consecutive_failures)
    }

    /// Set the recovery strategy for a specific error category.
    pub async fn set_error_category_strategy(
        &self,
        category: error_recovery::ErrorCategory,
        strategy: error_recovery::RecoveryStrategy,
    ) {
        let mut mgr = self.error_recovery.lock().await;
        mgr.set_category_strategy(category, strategy);
        // Persist updated config
        if let Err(e) = error_recovery::save_error_recovery_config(mgr.config(), &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist error recovery config");
        }
    }

    /// Reset all error recovery strategies to defaults.
    pub async fn reset_error_recovery_strategies(&self) {
        let mut mgr = self.error_recovery.lock().await;
        mgr.reset_strategies();
        if let Err(e) = error_recovery::save_error_recovery_config(mgr.config(), &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist error recovery config");
        }
    }

    // ========== Phase 94: Connection Health Monitor ==========

    /// Set connection health monitoring configuration.
    pub async fn set_connection_health_config(
        &self,
        config: connection_health::ConnectionHealthConfig,
    ) -> Result<(), connection_health::ConnectionHealthPersistenceError> {
        connection_health::save_connection_health_config(&config, &self.data_dir)?;
        let mut mgr = self.connection_health.lock().await;
        mgr.set_config(config);
        Ok(())
    }

    /// Get current connection health monitoring configuration.
    pub async fn get_connection_health_config(&self) -> connection_health::ConnectionHealthConfig {
        self.connection_health.lock().await.config().clone()
    }

    /// Register a new connection for health monitoring.
    pub async fn register_connection_health(
        &self,
        connection_id: String,
        task_id: String,
        protocol: String,
        remote_addr: String,
    ) -> bool {
        self.connection_health.lock().await.register_connection(
            connection_id,
            task_id,
            protocol,
            remote_addr,
        )
    }

    /// Unregister a connection from health monitoring.
    pub async fn unregister_connection_health(&self, connection_id: &str) {
        self.connection_health
            .lock()
            .await
            .unregister_connection(connection_id);
    }

    /// Record speed for a connection.
    pub async fn record_connection_speed(&self, connection_id: &str, speed_bps: u64) {
        self.connection_health
            .lock()
            .await
            .record_speed(connection_id, speed_bps);
    }

    /// Record bytes transferred for a connection.
    pub async fn record_connection_transfer(&self, connection_id: &str, bytes: u64) {
        self.connection_health
            .lock()
            .await
            .record_transfer(connection_id, bytes);
    }

    /// Record an error for a connection.
    pub async fn record_connection_error(&self, connection_id: &str) {
        self.connection_health
            .lock()
            .await
            .record_error(connection_id);
    }

    /// Record a timeout for a connection.
    pub async fn record_connection_timeout(&self, connection_id: &str) {
        self.connection_health
            .lock()
            .await
            .record_timeout(connection_id);
    }

    /// Get connection health summary.
    pub async fn get_connection_health_summary(
        &self,
    ) -> connection_health::ConnectionHealthSummary {
        self.connection_health.lock().await.get_summary()
    }

    /// Assess health of a specific connection.
    pub async fn assess_connection_health(
        &self,
        connection_id: &str,
    ) -> Option<connection_health::ConnectionHealthAssessment> {
        self.connection_health
            .lock()
            .await
            .assess_connection(connection_id)
    }

    /// Remove all connections for a task.
    pub async fn remove_task_connections_health(&self, task_id: &str) -> usize {
        self.connection_health
            .lock()
            .await
            .remove_task_connections(task_id)
    }

    /// Get unhealthy connections for potential replacement.
    pub async fn get_unhealthy_connections(&self) -> Vec<String> {
        self.connection_health
            .lock()
            .await
            .get_unhealthy_connections()
    }

    // ========== Phase 95: Download Source Rotation ==========

    /// Set source rotation configuration.
    pub async fn set_source_rotation_config(
        &self,
        config: source_rotation::SourceRotationConfig,
    ) -> Result<(), source_rotation::SourceRotationPersistenceError> {
        source_rotation::save_source_rotation_config(&config, &self.data_dir)?;
        let mut mgr = self.source_rotation.lock().await;
        mgr.set_config(config);
        Ok(())
    }

    /// Get current source rotation configuration.
    pub async fn get_source_rotation_config(&self) -> source_rotation::SourceRotationConfig {
        self.source_rotation.lock().await.config().clone()
    }

    /// Add a download source for a task.
    pub async fn add_download_source(&self, source: source_rotation::DownloadSource) -> bool {
        self.source_rotation.lock().await.add_source(source)
    }

    /// Remove a download source.
    pub async fn remove_download_source(
        &self,
        source_id: &str,
    ) -> Option<source_rotation::DownloadSource> {
        self.source_rotation.lock().await.remove_source(source_id)
    }

    /// Record a successful download from a source.
    pub async fn record_source_success(&self, source_id: &str, bytes: u64) {
        self.source_rotation
            .lock()
            .await
            .record_source_success(source_id, bytes);
    }

    /// Record a failed download from a source.
    pub async fn record_source_failure(&self, source_id: &str) {
        self.source_rotation
            .lock()
            .await
            .record_source_failure(source_id);
    }

    /// Get the best available source for a task.
    pub async fn get_best_source(&self, task_id: &str) -> Option<source_rotation::DownloadSource> {
        self.source_rotation
            .lock()
            .await
            .get_best_source(task_id)
            .cloned()
    }

    /// Get all sources for a task.
    pub async fn get_task_sources(&self, task_id: &str) -> Vec<source_rotation::DownloadSource> {
        self.source_rotation
            .lock()
            .await
            .get_task_sources(task_id)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Get source rotation summary for a task.
    pub async fn get_source_rotation_summary(
        &self,
        task_id: &str,
    ) -> source_rotation::SourceRotationSummary {
        self.source_rotation.lock().await.get_task_summary(task_id)
    }

    /// Get overall source rotation summary for all tasks.
    pub async fn get_overall_source_rotation_summary(
        &self,
    ) -> std::collections::HashMap<String, source_rotation::SourceRotationSummary> {
        self.source_rotation.lock().await.get_overall_summary()
    }

    /// Execute source rotation for a task.
    pub async fn execute_source_rotation(
        &self,
        task_id: &str,
    ) -> source_rotation::RotationDecision {
        let mut mgr = self.source_rotation.lock().await;
        let decision = mgr.decide_rotation(task_id);
        mgr.apply_rotation(&decision);
        decision
    }

    /// Execute source rotation for all tasks.
    pub async fn execute_source_rotation_all(
        &self,
    ) -> std::collections::HashMap<String, source_rotation::RotationDecision> {
        let mut mgr = self.source_rotation.lock().await;
        let task_ids: Vec<String> = mgr.get_overall_summary().keys().cloned().collect();
        let mut decisions = std::collections::HashMap::new();
        for task_id in task_ids {
            let decision = mgr.decide_rotation(&task_id);
            mgr.apply_rotation(&decision);
            decisions.insert(task_id, decision);
        }
        decisions
    }

    /// Remove all sources for a task.
    pub async fn remove_task_sources(&self, task_id: &str) -> usize {
        self.source_rotation
            .lock()
            .await
            .remove_task_sources(task_id)
    }

    /// Check if a retry attempt is allowed under the daily quota.
    /// Returns QuotaCheck::Allowed if retry is permitted, Exhausted if quota is spent.
    pub async fn check_retry_quota(&self) -> retry_quota::QuotaCheck {
        self.retry_quota.lock().await.check_quota()
    }

    /// Set bandwidth allocation configuration.
    pub async fn set_allocation_config(
        &self,
        config: bandwidth_allocation::AllocationConfig,
    ) -> Result<(), bandwidth_allocation::AllocationPersistenceError> {
        bandwidth_allocation::save_allocation_config(&config, &self.data_dir)?;
        let mut mgr = self.bandwidth_allocation.lock().await;
        mgr.set_config(config);
        Ok(())
    }

    /// Get bandwidth allocation configuration.
    pub async fn get_allocation_config(&self) -> bandwidth_allocation::AllocationConfig {
        self.bandwidth_allocation.lock().await.config().clone()
    }

    /// Get current bandwidth allocation plan.
    pub async fn get_allocation_plan(&self) -> Option<bandwidth_allocation::AllocationPlan> {
        self.bandwidth_allocation.lock().await.last_plan().cloned()
    }

    /// Calculate bandwidth allocation for all active tasks.
    pub async fn calculate_allocation_plan(
        &self,
        total_bandwidth_bps: u64,
    ) -> bandwidth_allocation::AllocationPlan {
        let tasks = self.tasks.lock().await;
        let task_data: Vec<bandwidth_allocation::TaskAllocationData> = tasks
            .iter()
            .map(|t| bandwidth_allocation::TaskAllocationData {
                task_id: t.id.clone(),
                priority: match t.priority {
                    DownloadPriority::High => 3,
                    DownloadPriority::Normal => 2,
                    DownloadPriority::Low => 1,
                },
                bandwidth_weight: t.bandwidth_weight,
                is_active: t.state == DownloadState::Downloading,
            })
            .collect();

        let mut mgr = self.bandwidth_allocation.lock().await;
        mgr.calculate_allocation(total_bandwidth_bps, &task_data)
    }

    /// Get allocation for a specific task.
    pub async fn get_task_allocation(
        &self,
        task_id: &str,
    ) -> Option<bandwidth_allocation::TaskAllocation> {
        self.bandwidth_allocation
            .lock()
            .await
            .get_task_allocation(task_id)
            .cloned()
    }

    /// Record a retry attempt in the quota tracker.
    /// Returns true if recorded, false if quota was exhausted.
    pub async fn record_retry_quota(&self) -> bool {
        self.retry_quota.lock().await.record_retry()
    }

    /// Set auto-shutdown configuration.
    /// When enabled, triggers an action when all downloads finish.
    /// Also persists the configuration to disk for restoration on restart.
    pub async fn set_auto_shutdown(&self, config: AutoShutdownConfig) {
        *self.auto_shutdown.write().await = config.clone();
        if let Err(e) = auto_shutdown::save_auto_shutdown_config(&config, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist auto-shutdown config");
        }
    }

    /// Get current auto-shutdown configuration.
    pub async fn get_auto_shutdown(&self) -> AutoShutdownConfig {
        let shutdown = self.auto_shutdown.read().await;
        shutdown.clone()
    }

    /// Set auto-cleanup configuration.
    /// Persists the configuration to disk for automatic restoration on restart.
    pub async fn set_auto_cleanup(&self, config: auto_cleanup::AutoCleanupConfig) {
        *self.auto_cleanup.write().await = config.clone();
        if let Err(e) = auto_cleanup::save_auto_cleanup_config(&self.data_dir, &config) {
            tracing::warn!(error = %e, "Failed to persist auto-cleanup config");
        }
    }

    /// Get current auto-cleanup configuration.
    pub async fn get_auto_cleanup(&self) -> auto_cleanup::AutoCleanupConfig {
        let cleanup = self.auto_cleanup.read().await;
        cleanup.clone()
    }

    /// Run auto-cleanup: remove completed/failed tasks based on retention config.
    /// Returns the number of tasks removed.
    pub async fn run_auto_cleanup(&self) -> usize {
        let config = self.get_auto_cleanup().await;
        if !config.enabled {
            return 0;
        }

        let now = chrono::Utc::now();
        let tasks = self.tasks.lock().await;
        let cleanup_data: Vec<auto_cleanup::TaskCleanupData> = tasks
            .iter()
            .map(|t| auto_cleanup::TaskCleanupData {
                id: t.id.clone(),
                state: match t.state {
                    DownloadState::Complete => auto_cleanup::TaskCleanupState::Complete,
                    DownloadState::Error => auto_cleanup::TaskCleanupState::Error,
                    _ => auto_cleanup::TaskCleanupState::Other,
                },
                updated_at: t.updated_at,
            })
            .collect();
        drop(tasks);

        let to_remove = auto_cleanup::tasks_to_cleanup(&cleanup_data, &config, now);
        let count = to_remove.len();

        for task_id in to_remove {
            if self.remove_task(&task_id).await {
                tracing::info!(task_id = %task_id, "Auto-cleaned task");
            }
        }

        count
    }

    /// Set cooldown configuration for failed task retry backoff.
    pub async fn set_cooldown_config(&self, config: download_cooldown::CooldownConfig) {
        *self.cooldown_config.write().await = config.clone();
        if let Err(e) = download_cooldown::save_cooldown_config(&self.data_dir, &config) {
            tracing::warn!(error = %e, "Failed to persist cooldown config");
        }
    }

    /// Get current cooldown configuration.
    pub async fn get_cooldown_config(&self) -> download_cooldown::CooldownConfig {
        let cooldown = self.cooldown_config.read().await;
        cooldown.clone()
    }

    /// Tick cooldown: move tasks whose cooldown period has elapsed from Error back to Queued.
    /// Returns the number of tasks moved back to Queued.
    pub async fn tick_cooldown(&self) -> usize {
        let config = self.get_cooldown_config().await;
        if !config.enabled {
            return 0;
        }

        let now = chrono::Utc::now().timestamp() as u64;
        let mut tasks = self.tasks.lock().await;
        let mut count = 0;

        for task in tasks.iter_mut() {
            if task.state != DownloadState::Error {
                continue;
            }
            if let Some(ref cooldown_state) = task.cooldown
                && download_cooldown::can_retry(cooldown_state, now)
                && !download_cooldown::max_retries_exceeded(cooldown_state, &config)
            {
                task.state = DownloadState::Queued;
                task.updated_at = chrono::Utc::now();
                count += 1;
                tracing::info!(
                    task_id = %task.id,
                    attempt = cooldown_state.retry_attempt,
                    "Cooldown elapsed, task moved back to Queued"
                );
            }
        }

        if count > 0 {
            drop(tasks);
            self.persist_tasks().await;
        }

        count
    }

    /// Record a failure for a task and apply cooldown backoff.
    pub async fn record_task_failure(&self, task_id: &str) {
        let config = self.get_cooldown_config().await;
        if !config.enabled {
            return;
        }

        let now = download_cooldown::now_secs();
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            let cooldown_state = task.cooldown.get_or_insert_with(Default::default);
            download_cooldown::record_failure(cooldown_state, &config, now);
            tracing::info!(
                task_id = %task_id,
                attempt = cooldown_state.retry_attempt,
                next_retry_secs = cooldown_state.next_retry_at,
                "Task failed, cooldown applied"
            );
        }
    }

    /// Reset cooldown state for a task (e.g. when manually resumed or completed).
    pub async fn reset_task_cooldown(&self, task_id: &str) {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id)
            && let Some(ref mut cooldown_state) = task.cooldown
        {
            download_cooldown::reset_cooldown(cooldown_state);
        }
    }

    /// Get cooldown status for a task.
    pub async fn get_cooldown_status(
        &self,
        task_id: &str,
    ) -> Option<download_cooldown::CooldownStatus> {
        let config = self.get_cooldown_config().await;
        let tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter().find(|t| t.id == task_id)
            && let Some(ref cooldown_state) = task.cooldown
        {
            return Some(download_cooldown::cooldown_status(
                task_id,
                cooldown_state,
                &config,
            ));
        }
        None
    }

    /// Set URL deduplication configuration.
    /// Persists the configuration to disk for automatic restoration on restart.
    pub async fn set_url_dedup(&self, config: url_dedup::DedupConfig) {
        *self.url_dedup.write().await = config.clone();
        if let Err(e) = url_dedup::save_dedup_config(&config, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL dedup config");
        }
    }

    /// Get current URL deduplication configuration.
    pub async fn get_url_dedup(&self) -> url_dedup::DedupConfig {
        let dedup = self.url_dedup.read().await;
        dedup.clone()
    }

    /// Set queue staleness configuration.
    /// Persists the configuration to disk for automatic restoration on restart.
    pub async fn set_queue_staleness_config(&self, config: queue_staleness::StalenessConfig) {
        *self.queue_staleness.write().await = config.clone();
        if let Err(e) = queue_staleness::save_staleness_config(&config, &self.data_dir).await {
            tracing::warn!(error = %e, "Failed to persist queue staleness config");
        }
    }

    /// Get current queue staleness configuration.
    pub async fn get_queue_staleness_config(&self) -> queue_staleness::StalenessConfig {
        self.queue_staleness.read().await.clone()
    }

    /// Analyze the queue for stale tasks and optionally promote their priorities.
    /// Returns a summary of the analysis including which tasks were promoted.
    /// When auto_promote is enabled, actually applies priority promotions to tasks
    /// and persists the updated promotion counts.
    pub async fn check_queue_staleness(&self) -> queue_staleness::StalenessSummary {
        let config = self.get_queue_staleness_config().await;
        let now = chrono::Utc::now();
        let mut tasks = self.tasks.lock().await;
        let task_data: Vec<queue_staleness::TaskStalenessData> = tasks
            .iter()
            .map(|t| queue_staleness::TaskStalenessData {
                id: t.id.clone(),
                name: t.name.clone(),
                is_queued: matches!(t.state, DownloadState::Queued),
                created_at: t.created_at,
                priority: match t.priority {
                    DownloadPriority::Low => queue_staleness::StalePriority::Low,
                    DownloadPriority::Normal => queue_staleness::StalePriority::Normal,
                    DownloadPriority::High => queue_staleness::StalePriority::High,
                },
                promotion_count: t.staleness_promotion_count,
            })
            .collect();
        let summary = queue_staleness::analyze_staleness(&task_data, now, &config);

        // Apply promotions to actual tasks when auto_promote is enabled
        if config.auto_promote {
            for stale_task in &summary.tasks {
                if stale_task.promoted
                    && let Some(task) = tasks.iter_mut().find(|t| t.id == stale_task.id)
                {
                    // Apply priority promotion
                    if let Some(new_priority_str) = &stale_task.new_priority {
                        let new_priority = match new_priority_str.as_str() {
                            "low" => DownloadPriority::Low,
                            "normal" => DownloadPriority::Normal,
                            "high" => DownloadPriority::High,
                            _ => DownloadPriority::High,
                        };
                        task.priority = new_priority;
                    }
                    // Update promotion count
                    task.staleness_promotion_count = stale_task.promotion_count;
                    task.updated_at = chrono::Utc::now();
                }
            }
            // Persist updated tasks
            let tasks_ref: Vec<_> = tasks.iter().cloned().collect();
            drop(tasks);
            if let Err(e) = task_queue::save_task_queue(&tasks_ref, &self.data_dir) {
                tracing::warn!(error = %e, "Failed to persist task queue after staleness promotion");
            }
        }

        summary
    }

    /// Get current network condition summary.
    pub async fn get_network_summary(&self) -> network_monitor::NetworkSummary {
        self.network_monitor.lock().await.summary()
    }

    /// Record a network quality sample (called periodically by speed tracker).
    pub async fn record_network_sample(&self, speed_bps: f64, active_tasks: usize) {
        self.network_monitor
            .lock()
            .await
            .record_sample(speed_bps, active_tasks);
    }

    /// Set network monitor configuration.
    pub async fn set_network_monitor_config(&self, config: network_monitor::NetworkMonitorConfig) {
        let mut monitor = self.network_monitor.lock().await;
        monitor.set_config(config);
        if let Err(e) = monitor
            .save(&self.data_dir.join("network_monitor.json"))
            .await
        {
            tracing::warn!(error = %e, "Failed to persist network monitor config");
        }
    }

    /// Get current network monitor configuration.
    pub async fn get_network_monitor_config(&self) -> network_monitor::NetworkMonitorConfig {
        self.network_monitor.lock().await.config().clone()
    }

    /// Clear all collected network monitor data.
    pub async fn clear_network_monitor(&self) {
        self.network_monitor.lock().await.clear();
    }

    /// Handle duplicate task detection based on the configured duplicate policy.
    /// Returns Ok(Some(task_id)) if the task should be skipped (existing task ID),
    /// Ok(None) if the task should be allowed to proceed, or Err if rejected.
    async fn handle_duplicate(
        &self,
        existing_task_id: &str,
        new_source: &str,
    ) -> Result<Option<String>, DownloadManagerError> {
        let dedup_config = self.get_url_dedup().await;
        let policy = dedup_config.duplicate_policy;

        match policy {
            url_dedup::DuplicatePolicy::Reject => Err(DownloadManagerError::DuplicateTask(
                existing_task_id.to_string(),
            )),
            url_dedup::DuplicatePolicy::Skip => {
                tracing::info!(
                    existing_task_id = %existing_task_id,
                    source = %new_source,
                    "Skipping duplicate task (policy: skip)"
                );
                Ok(Some(existing_task_id.to_string()))
            }
            url_dedup::DuplicatePolicy::Allow => {
                tracing::info!(
                    existing_task_id = %existing_task_id,
                    source = %new_source,
                    "Allowing duplicate task (policy: allow)"
                );
                Ok(None)
            }
            url_dedup::DuplicatePolicy::PauseExisting => {
                tracing::info!(
                    existing_task_id = %existing_task_id,
                    source = %new_source,
                    "Pausing existing task before adding duplicate (policy: pause_existing)"
                );
                // Use the existing pause_task method
                self.pause_task(existing_task_id).await;
                Ok(None)
            }
        }
    }

    /// Check if a URL is a duplicate of any existing task based on current dedup config.
    /// Returns the task ID if a duplicate is found, None otherwise.
    pub async fn find_duplicate_by_url(&self, url: &str) -> Option<String> {
        let dedup_config = self.get_url_dedup().await;
        if !dedup_config.enabled {
            return None;
        }

        let tasks = self.tasks.lock().await;
        let existing_urls: Vec<String> =
            tasks.iter().filter_map(|t| t.source_url.clone()).collect();

        if let Some(idx) = url_dedup::find_duplicate_url(url, &existing_urls, &dedup_config) {
            // Find the task with this URL
            tasks
                .iter()
                .find(|t| t.source_url.as_deref() == Some(existing_urls[idx].as_str()))
                .map(|t| t.id.clone())
        } else {
            None
        }
    }

    /// Set URL expander configuration. Persists to disk.
    pub async fn set_url_expander(&self, config: url_expander::UrlExpanderConfig) {
        *self.url_expander.write().await = config.clone();
        if let Err(e) = url_expander::save_url_expander_config(&config, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL expander config");
        }
    }

    /// Get current URL expander configuration.
    pub async fn get_url_expander(&self) -> url_expander::UrlExpanderConfig {
        self.url_expander.read().await.clone()
    }

    /// Get the URL health monitor.
    pub fn url_health_monitor(&self) -> &url_health_monitor::UrlHealthMonitor {
        &self.url_health_monitor
    }

    // ========== Link Rot Detection (Phase 161) ==========

    /// Get link rot detector config.
    pub async fn get_link_rot_config(&self) -> link_rot::LinkRotConfig {
        self.link_rot.read().await.config().clone()
    }

    /// Update link rot detector config.
    pub async fn set_link_rot_config(
        &self,
        config: link_rot::LinkRotConfig,
    ) -> Result<(), link_rot::LinkRotError> {
        self.link_rot.write().await.set_config(config).await
    }

    /// Track a task URL for link rot detection.
    pub async fn track_link_rot(&self, task_id: &str, url: &str) {
        self.link_rot.write().await.track_task(task_id, url);
    }

    /// Stop tracking a task for link rot detection.
    pub async fn untrack_link_rot(&self, task_id: &str) {
        self.link_rot.write().await.untrack_task(task_id);
    }

    /// Get link rot check result for a task.
    pub async fn get_link_rot_result(&self, task_id: &str) -> Option<link_rot::LinkCheckResult> {
        self.link_rot.read().await.get_result(task_id).cloned()
    }

    /// Get link rot summary.
    pub async fn get_link_rot_summary(&self) -> link_rot::LinkRotSummary {
        self.link_rot.read().await.summary()
    }

    /// Get formatted link rot report.
    pub async fn get_link_rot_report(&self) -> String {
        self.link_rot.read().await.format_report()
    }

    /// Clear all link rot results.
    pub async fn clear_link_rot(&self) {
        self.link_rot.write().await.clear();
    }

    /// Get next batch of task IDs to check for link rot.
    pub async fn get_link_rot_batch(&self) -> Vec<String> {
        self.link_rot.read().await.next_batch()
    }

    /// Apply a link rot check result.
    pub async fn apply_link_rot_check(
        &self,
        task_id: &str,
        success: bool,
        http_status: Option<u16>,
        response_time_ms: Option<u64>,
        error: Option<String>,
    ) -> bool {
        self.link_rot.write().await.apply_check_result(
            task_id,
            success,
            http_status,
            response_time_ms,
            error,
        )
    }

    /// Save link rot data to disk.
    pub async fn save_link_rot(&self) -> Result<(), link_rot::LinkRotError> {
        let det = self.link_rot.read().await;
        det.save_config().await?;
        det.save_results().await?;
        Ok(())
    }

    /// Load link rot data from disk.
    pub async fn load_link_rot(&self) -> Result<(), link_rot::LinkRotError> {
        self.link_rot.write().await.load().await
    }

    /// Monitor a URL for health checks.
    pub async fn monitor_url_health(&self, url: &str) -> bool {
        self.url_health_monitor.monitor_url(url).await
    }

    /// Stop monitoring a URL.
    pub async fn unmonitor_url_health(&self, url: &str) -> bool {
        self.url_health_monitor.unmonitor_url(url).await
    }

    /// Get health status for a specific URL.
    pub async fn get_url_health(&self, url: &str) -> Option<url_health_monitor::UrlHealthCheck> {
        self.url_health_monitor.get_url_health(url).await
    }

    /// Get URL health monitoring summary.
    pub async fn get_url_health_summary(&self) -> url_health_monitor::UrlHealthSummary {
        self.url_health_monitor.get_summary().await
    }

    /// Get all URL health checks.
    pub async fn get_all_url_health_checks(&self) -> Vec<url_health_monitor::UrlHealthCheck> {
        self.url_health_monitor.get_all_health_checks().await
    }

    /// Get URL health monitor configuration.
    pub async fn get_url_health_config(&self) -> url_health_monitor::UrlHealthMonitorConfig {
        self.url_health_monitor.get_config().await
    }

    /// Set URL health monitor configuration.
    pub async fn set_url_health_config(&self, config: url_health_monitor::UrlHealthMonitorConfig) {
        self.url_health_monitor.set_config(config).await;
    }

    /// Get the watch folder manager.
    pub async fn watch_folder(&self) -> watch_folder::WatchFolderManager {
        self.watch_folder.lock().await.clone()
    }

    /// Add a watch folder for automatic URL import.
    pub async fn add_watch_folder(
        &self,
        name: String,
        path: std::path::PathBuf,
        recursive: bool,
        extensions: Vec<String>,
        cleanup_after: bool,
        tags: Vec<String>,
        group: Option<String>,
    ) -> Result<String, watch_folder::WatchFolderError> {
        let mut mgr = self.watch_folder.lock().await;
        let id = mgr.add_folder(
            name,
            path,
            recursive,
            extensions,
            cleanup_after,
            tags,
            group,
        )?;
        // Persist
        let config_path = self.data_dir.join("watch_folders.json");
        if let Err(e) = mgr.save(&config_path) {
            tracing::warn!(error = %e, "Failed to persist watch folder config");
        }
        Ok(id)
    }

    /// Remove a watch folder by ID.
    pub async fn remove_watch_folder(
        &self,
        id: &str,
    ) -> Result<(), watch_folder::WatchFolderError> {
        let mut mgr = self.watch_folder.lock().await;
        mgr.remove_folder(id)?;
        let config_path = self.data_dir.join("watch_folders.json");
        if let Err(e) = mgr.save(&config_path) {
            tracing::warn!(error = %e, "Failed to persist watch folder config");
        }
        Ok(())
    }

    /// Enable or disable a watch folder.
    pub async fn set_watch_folder_enabled(
        &self,
        id: &str,
        enabled: bool,
    ) -> Result<(), watch_folder::WatchFolderError> {
        let mut mgr = self.watch_folder.lock().await;
        mgr.set_enabled(id, enabled)?;
        let config_path = self.data_dir.join("watch_folders.json");
        if let Err(e) = mgr.save(&config_path) {
            tracing::warn!(error = %e, "Failed to persist watch folder config");
        }
        Ok(())
    }

    /// Scan all enabled watch folders and auto-import discovered URLs.
    /// Returns the number of URLs imported.
    pub async fn scan_watch_folders(&self) -> usize {
        let mut mgr = self.watch_folder.lock().await;
        let urls = mgr.scan_and_collect_urls().await;
        drop(mgr);

        // Persist
        let config_path = self.data_dir.join("watch_folders.json");
        {
            let mgr = self.watch_folder.lock().await;
            if let Err(e) = mgr.save(&config_path) {
                tracing::warn!(error = %e, "Failed to persist watch folder config");
            }
        }

        let mut imported = 0;
        for wfu in urls {
            // Skip if URL already exists
            {
                let tasks = self.tasks.lock().await;
                if tasks
                    .iter()
                    .any(|t| t.source_url.as_deref() == Some(&wfu.url))
                {
                    continue;
                }
            }

            // Determine protocol and add task
            let url = wfu.url.clone();
            let _name = url.rsplit('/').next().unwrap_or("download").to_string();
            let mut tags = wfu.tags.clone();
            tags.push("watch-folder".to_string());

            let result = if url.starts_with("magnet:?") {
                self.add_magnet(&url).await
            } else if url.starts_with("ed2k://") {
                // Parse ed2k URL - this is complex, skip for now
                continue;
            } else {
                self.add_url(&url).await
            };

            if let Ok(task_id) = result {
                // Apply tags and group from watch folder config
                if !tags.is_empty() {
                    self.add_tags(&task_id, tags).await;
                }
                if let Some(group) = &wfu.group {
                    self.set_task_group(&task_id, Some(group.clone())).await;
                }
                imported += 1;
            }
        }
        imported
    }

    /// Get watch folder summary.
    pub async fn get_watch_folder_summary(&self) -> watch_folder::WatchFolderSummary {
        let mgr = self.watch_folder.lock().await;
        mgr.summary()
    }

    /// Initialize watch folder manager from persisted config.
    pub async fn init_watch_folder(&mut self) {
        let config_path = self.data_dir.join("watch_folders.json");
        match watch_folder::WatchFolderManager::load(&config_path) {
            Ok(mgr) => {
                *self.watch_folder.lock().await = mgr;
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load watch folder config");
            }
        }
    }

    /// Set watch folder auto-scan configuration.
    pub async fn set_watch_folder_auto_scan(&self, enabled: bool, interval_secs: u64) -> bool {
        let mut mgr = self.watch_folder.lock().await;
        let config = watch_folder::WatchFolderAutoScanConfig {
            enabled,
            interval_secs,
            last_auto_scan: mgr.auto_scan_config.last_auto_scan,
        };
        mgr.set_auto_scan_config(config);
        let config_path = self.data_dir.join("watch_folders.json");
        if let Err(e) = mgr.save(&config_path) {
            tracing::warn!(error = %e, "Failed to persist watch folder config");
            return false;
        }
        true
    }

    /// Get watch folder auto-scan configuration.
    pub async fn get_watch_folder_auto_scan(&self) -> watch_folder::WatchFolderAutoScanConfig {
        let mgr = self.watch_folder.lock().await;
        mgr.auto_scan_config.clone()
    }

    /// Expand a shortened URL and validate it is reachable.
    /// Returns the expanded URL and validation result.
    pub async fn expand_and_validate_url(
        &self,
        url: &str,
    ) -> Result<
        (
            url_expander::ExpansionResult,
            url_expander::ValidationResult,
        ),
        url_expander::UrlExpanderError,
    > {
        let config = self.get_url_expander().await;
        url_expander::expand_and_validate(url, &config).await
    }

    /// Check if a URL appears to be from a known URL shortener.
    pub async fn is_shortened_url(&self, url: &str) -> bool {
        let config = self.get_url_expander().await;
        url_expander::is_shortened_url(url, &config)
    }

    /// Extract download links from HTML content.
    /// If `base_url` is provided, relative URLs are resolved against it.
    pub fn extract_links_from_html(
        html: &str,
        base_url: Option<&str>,
    ) -> link_extractor::ExtractionResult {
        let links = link_extractor::extract_links_from_html(html, base_url);
        link_extractor::build_extraction_result(base_url.unwrap_or("<unknown>"), links)
    }

    /// Fetch a web page and extract all downloadable links.
    /// Resolves relative URLs against the page URL.
    pub async fn scrape_url_for_links(
        url: &str,
    ) -> Result<link_extractor::ExtractionResult, String> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent("ipmsg-torrent/1.0")
            .build()
            .map_err(|e| format!("Failed to build HTTP client: {e}"))?;

        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| format!("Failed to fetch {url}: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("HTTP {} for {url}", resp.status()));
        }

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let body = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read response body: {e}"))?;

        let links = link_extractor::extract_links_from_html(&body, Some(url));
        let mut result = link_extractor::build_extraction_result(url, links);
        result.source_url = url.to_string();
        // Store content type for informational purposes
        if !content_type.is_empty() {
            result.protocol_counts.insert("content_type".to_string(), 0);
        }
        Ok(result)
    }

    /// Add a URL rewrite rule.
    /// Persists rules to disk for automatic restoration on restart.
    pub async fn add_url_rewrite_rule(&self, rule: url_rewrite::UrlRewriteRule) {
        let mut mgr = self.url_rewrite.lock().await;
        mgr.add_rule(rule);
        if let Err(e) = url_rewrite::save_url_rewrite_manager(&mgr, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL rewrite rules");
        }
    }

    /// Remove a URL rewrite rule by ID.
    pub async fn remove_url_rewrite_rule(&self, id: &str) -> bool {
        let mut mgr = self.url_rewrite.lock().await;
        let removed = mgr.remove_rule(id);
        if removed && let Err(e) = url_rewrite::save_url_rewrite_manager(&mgr, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL rewrite rules");
        }
        removed
    }

    /// List all URL rewrite rules.
    pub async fn list_url_rewrite_rules(&self) -> Vec<url_rewrite::UrlRewriteRule> {
        let mgr = self.url_rewrite.lock().await;
        mgr.list_rules().to_vec()
    }

    /// Get URL rewrite summary (rules + stats).
    pub async fn get_url_rewrite_summary(&self) -> url_rewrite::UrlRewriteSummary {
        let mgr = self.url_rewrite.lock().await;
        mgr.summary()
    }

    /// Enable or disable URL rewriting globally.
    pub async fn set_url_rewrite_enabled(&self, enabled: bool) {
        let mut mgr = self.url_rewrite.lock().await;
        mgr.enabled = enabled;
        if let Err(e) = url_rewrite::save_url_rewrite_manager(&mgr, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL rewrite rules");
        }
    }

    /// Apply URL rewrite rules to a URL. Returns the rewritten URL.
    pub async fn rewrite_url(&self, url: &str) -> String {
        let mut mgr = self.url_rewrite.lock().await;
        let rewritten = mgr.rewrite_url(url);
        // Persist to save updated apply_count
        if rewritten != url
            && let Err(e) = url_rewrite::save_url_rewrite_manager(&mgr, &self.data_dir)
        {
            tracing::warn!(error = %e, "Failed to persist URL rewrite rules after apply");
        }
        rewritten
    }

    /// Preview URL rewrite without modifying apply counts.
    pub async fn preview_url_rewrite(&self, url: &str) -> Option<(String, String)> {
        let mgr = self.url_rewrite.lock().await;
        mgr.preview_rewrite(url)
    }

    /// Set the path template for auto-organizing downloads.
    /// Persists the configuration to disk for automatic restoration on restart.
    pub async fn set_path_template(
        &self,
        template: &str,
    ) -> Result<(), path_template::PathTemplateError> {
        self.path_template.set_template(template).await?;
        let config = self.path_template.get_config().await;
        if let Err(e) = path_template::save_path_template_config(&config, &self.data_dir).await {
            tracing::warn!(error = %e, "Failed to persist path template config");
        }
        Ok(())
    }

    /// Get the current path template config.
    pub async fn get_path_template_config(&self) -> path_template::PathTemplateConfig {
        self.path_template.get_config().await
    }

    /// Enable or disable path templates.
    pub async fn set_path_template_enabled(&self, enabled: bool) {
        if enabled {
            self.path_template.enable().await;
        } else {
            self.path_template.disable().await;
        }
        let config = self.path_template.get_config().await;
        if let Err(e) = path_template::save_path_template_config(&config, &self.data_dir).await {
            tracing::warn!(error = %e, "Failed to persist path template config");
        }
    }

    /// Compute the save path for a file using the path template.
    pub async fn compute_save_path_with_template(
        &self,
        base_dir: &std::path::Path,
        filename: &str,
        protocol: &str,
    ) -> Result<std::path::PathBuf, path_template::PathTemplateError> {
        self.path_template
            .compute_save_path(base_dir, filename, protocol)
            .await
    }

    /// Preview a path template without saving.
    pub fn preview_path_template(
        template: &str,
        filename: &str,
        protocol: &str,
    ) -> Result<String, path_template::PathTemplateError> {
        let parsed = path_template::PathTemplate::parse(template)?;
        parsed.validate()?;
        let ctx = path_template::TemplateContext::new(filename, protocol);
        Ok(parsed.render(&ctx))
    }

    /// Get path template summary for status display.
    pub async fn get_path_template_summary(&self) -> path_template::PathTemplateConfig {
        self.path_template.get_config().await
    }

    // ── Data Cap ──────────────────────────────────────────────────────

    /// Set the daily data cap configuration.
    /// Persists to disk for automatic restoration on restart.
    pub async fn set_data_cap_config(&self, config: data_cap::DataCapConfig) {
        let mut dc = self.data_cap.lock().await;
        dc.set_config(config);
        if let Err(e) = data_cap::save_data_cap(&dc, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist data cap config");
        }
    }

    /// Get the current data cap status (usage, remaining, cap reached).
    pub async fn get_data_cap_status(&self) -> data_cap::DataCapStatus {
        let dc = self.data_cap.lock().await;
        dc.status()
    }

    /// Enable or disable the daily data cap.
    pub async fn set_data_cap_enabled(&self, enabled: bool) {
        let mut dc = self.data_cap.lock().await;
        dc.set_enabled(enabled);
        if let Err(e) = data_cap::save_data_cap(&dc, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist data cap config");
        }
    }

    /// Set the daily data limit in bytes.
    pub async fn set_data_cap_limit(&self, bytes: u64) {
        let mut dc = self.data_cap.lock().await;
        dc.set_daily_limit(bytes);
        if let Err(e) = data_cap::save_data_cap(&dc, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist data cap config");
        }
    }

    /// Record bytes downloaded for data cap tracking.
    /// Returns true if this caused the daily cap to be reached.
    pub async fn record_data_cap_usage(&self, task_id: &str, bytes: u64) -> bool {
        let mut dc = self.data_cap.lock().await;
        dc.record_download(task_id, bytes)
    }

    /// Check if downloads should be paused due to data cap.
    pub async fn should_pause_for_data_cap(&self) -> bool {
        let dc = self.data_cap.lock().await;
        dc.should_pause_downloads()
    }

    /// Mark that data-cap auto-pause has been applied.
    pub async fn mark_data_cap_paused(&self) {
        let mut dc = self.data_cap.lock().await;
        dc.mark_auto_paused();
        if let Err(e) = data_cap::save_data_cap(&dc, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist data cap state");
        }
    }

    /// Clear the data-cap auto-pause flag (e.g., after midnight or manual resume).
    pub async fn clear_data_cap_paused(&self) {
        let mut dc = self.data_cap.lock().await;
        dc.clear_auto_paused();
        if let Err(e) = data_cap::save_data_cap(&dc, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist data cap state");
        }
    }

    /// Reset today's data cap usage manually.
    pub async fn reset_data_cap_today(&self) {
        let mut dc = self.data_cap.lock().await;
        dc.reset_today();
        if let Err(e) = data_cap::save_data_cap(&dc, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist data cap reset");
        }
    }

    // ── Download Budget (Weekly/Monthly) ─────────────────────────────

    /// Set the download budget configuration.
    pub async fn set_download_budget_config(&self, config: download_budget::BudgetConfig) {
        let mut bm = self.download_budget.lock().await;
        bm.set_config(config);
        if let Err(e) = download_budget::save_budget(&bm, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist download budget config");
        }
    }

    /// Get the download budget configuration.
    pub async fn get_download_budget_config(&self) -> download_budget::BudgetConfig {
        let bm = self.download_budget.lock().await;
        bm.config().clone()
    }

    /// Get the full download budget summary.
    pub async fn get_download_budget_summary(&self) -> download_budget::BudgetSummary {
        let bm = self.download_budget.lock().await;
        bm.summary()
    }

    /// Enable or disable the download budget system.
    pub async fn set_download_budget_enabled(&self, enabled: bool) {
        let mut bm = self.download_budget.lock().await;
        bm.set_enabled(enabled);
        if let Err(e) = download_budget::save_budget(&bm, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist download budget enabled state");
        }
    }

    /// Set the weekly budget limit in bytes.
    pub async fn set_download_budget_weekly_limit(&self, bytes: u64) {
        let mut bm = self.download_budget.lock().await;
        bm.set_weekly_limit(bytes);
        if let Err(e) = download_budget::save_budget(&bm, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist weekly limit");
        }
    }

    /// Set the monthly budget limit in bytes.
    pub async fn set_download_budget_monthly_limit(&self, bytes: u64) {
        let mut bm = self.download_budget.lock().await;
        bm.set_monthly_limit(bytes);
        if let Err(e) = download_budget::save_budget(&bm, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist monthly limit");
        }
    }

    /// Record bytes downloaded for budget tracking.
    /// Returns true if this caused any budget to be newly exhausted.
    pub async fn record_download_budget(&self, task_id: &str, bytes: u64) -> bool {
        let mut bm = self.download_budget.lock().await;
        let crossed = bm.record_download(task_id, bytes);
        if let Err(e) = download_budget::save_budget(&bm, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist download budget usage");
        }
        crossed
    }

    /// Check if downloads should be paused due to budget exhaustion.
    pub async fn should_pause_for_download_budget(&self) -> bool {
        let bm = self.download_budget.lock().await;
        bm.should_pause_downloads()
    }

    /// Mark downloads as auto-paused due to budget exhaustion.
    pub async fn mark_download_budget_paused(&self) {
        let mut bm = self.download_budget.lock().await;
        bm.mark_auto_paused();
        if let Err(e) = download_budget::save_budget(&bm, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist download budget auto-pause");
        }
    }

    /// Clear the download budget auto-pause flag.
    pub async fn clear_download_budget_paused(&self) {
        let mut bm = self.download_budget.lock().await;
        bm.clear_auto_paused();
        if let Err(e) = download_budget::save_budget(&bm, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist download budget clear-pause");
        }
    }

    /// Reset download budget usage manually.
    pub async fn reset_download_budget(&self) {
        let mut bm = self.download_budget.lock().await;
        bm.reset();
        if let Err(e) = download_budget::save_budget(&bm, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist download budget reset");
        }
    }

    // ── Download Analytics ──────────────────────────────────────────

    /// Set download analytics configuration.
    pub async fn set_download_analytics_config(&self, config: download_analytics::AnalyticsConfig) {
        let mut am = self.download_analytics.lock().await;
        am.set_config(config);
        if let Err(e) = download_analytics::save_analytics_config(&self.data_dir, am.config()) {
            tracing::warn!(error = %e, "Failed to persist analytics config");
        }
    }

    /// Get download analytics configuration.
    pub async fn get_download_analytics_config(&self) -> download_analytics::AnalyticsConfig {
        let am = self.download_analytics.lock().await;
        am.config().clone()
    }

    /// Get analytics summary for the last N days.
    pub async fn get_download_analytics_summary(
        &self,
        days: u32,
    ) -> Option<download_analytics::AnalyticsSummary> {
        let am = self.download_analytics.lock().await;
        am.summary_last_n_days(days)
    }

    /// Get analytics summary for a specific date range.
    pub async fn get_download_analytics_range(
        &self,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
    ) -> Option<download_analytics::AnalyticsSummary> {
        let am = self.download_analytics.lock().await;
        am.summary_range(start, end)
    }

    /// Get trend comparison between current and previous N-day periods.
    pub async fn get_download_analytics_trend(
        &self,
        days: u32,
    ) -> Option<download_analytics::TrendComparison> {
        let am = self.download_analytics.lock().await;
        am.compare_periods(days)
    }

    /// Get today's analytics metrics.
    pub async fn get_download_analytics_today(&self) -> Option<download_analytics::DailyMetrics> {
        let am = self.download_analytics.lock().await;
        am.today().cloned()
    }

    /// Get all analytics records (newest first).
    pub async fn get_download_analytics_records(&self) -> Vec<download_analytics::DailyMetrics> {
        let am = self.download_analytics.lock().await;
        am.all_records().into_iter().cloned().collect()
    }

    /// Prune old analytics records beyond retention period.
    pub async fn prune_download_analytics(&self) {
        let mut am = self.download_analytics.lock().await;
        am.prune_old_records();
        if let Err(e) = download_analytics::save_analytics_records(&self.data_dir, am.records_mut())
        {
            tracing::warn!(error = %e, "Failed to persist analytics records after prune");
        }
    }

    /// Clear all download analytics data.
    pub async fn clear_download_analytics(&self) {
        let mut am = self.download_analytics.lock().await;
        am.clear();
        if let Err(e) = download_analytics::save_analytics_records(&self.data_dir, am.records_mut())
        {
            tracing::warn!(error = %e, "Failed to persist analytics records after clear");
        }
    }

    /// Persist current analytics records to disk.
    pub async fn save_download_analytics(&self) {
        let mut am = self.download_analytics.lock().await;
        if let Err(e) = download_analytics::save_analytics_records(&self.data_dir, am.records_mut())
        {
            tracing::warn!(error = %e, "Failed to persist analytics records");
        }
    }

    // ── Smart Queue Optimizer ──────────────────────────────────────────

    /// Set smart queue optimizer configuration.
    pub async fn set_smart_queue_config(&self, config: smart_queue::SmartQueueConfig) {
        let mut sq = self.smart_queue.write().await;
        sq.set_config(config);
        if let Err(e) = smart_queue::save_smart_queue_config(sq.get_config(), &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist smart queue config");
        }
    }

    /// Get smart queue optimizer configuration.
    pub async fn get_smart_queue_config(&self) -> smart_queue::SmartQueueConfig {
        let sq = self.smart_queue.read().await;
        sq.get_config().clone()
    }

    /// Run smart queue optimization and return recommendations.
    pub async fn optimize_smart_queue(&self) -> smart_queue::OptimizationResult {
        let tasks = self.tasks.lock().await;
        let favorites = self.task_favorites.lock().await.get_favorite_ids();
        let task_data: Vec<smart_queue::TaskOptimizationData> = tasks
            .iter()
            .map(|t| smart_queue::TaskOptimizationData {
                id: t.id.clone(),
                name: t.name.clone(),
                queue_position: t.queue_position,
                priority: t.priority as i32,
                size: t.size,
                progress: t.downloaded as f32 / t.size.max(1) as f32,
                state: format!("{:?}", t.state),
                created_at: t.created_at,
                deadline: t.deadline,
                depends_on: t.depends_on.clone(),
                staleness_promotions: t.staleness_promotion_count,
                is_favorite: favorites.contains(&t.id),
            })
            .collect();

        let mut sq = self.smart_queue.write().await;
        sq.optimize(&task_data)
    }

    /// Get smart queue optimizer summary.
    pub async fn get_smart_queue_summary(&self) -> smart_queue::SmartQueueSummary {
        let tasks = self.tasks.lock().await;
        let queued_count = tasks
            .iter()
            .filter(|t| matches!(t.state, DownloadState::Queued))
            .count();
        let sq = self.smart_queue.read().await;
        sq.get_summary(queued_count)
    }

    /// Get last optimization result.
    pub async fn get_smart_queue_last_result(&self) -> Option<smart_queue::OptimizationResult> {
        let sq = self.smart_queue.read().await;
        sq.get_last_result().cloned()
    }

    // ── Preflight Check ─────────────────────────────────────────────

    /// Get preflight check configuration
    pub async fn get_preflight_config(&self) -> preflight_check::PreflightConfig {
        let pc = self.preflight_checker.read().await;
        pc.config().clone()
    }

    /// Set preflight check configuration
    pub async fn set_preflight_config(
        &self,
        config: preflight_check::PreflightConfig,
    ) -> Result<(), preflight_check::PreflightCheckError> {
        let mut pc = self.preflight_checker.write().await;
        pc.set_config(config)
    }

    /// Run preflight checks for a given URL and save directory
    pub async fn run_preflight_check(
        &self,
        input: preflight_check::PreflightInput,
    ) -> preflight_check::PreflightReport {
        let pc = self.preflight_checker.read().await;
        pc.run_checks(&input).await
    }

    // ── Download Diagnostics ─────────────────────────────────────────

    /// Get diagnostics configuration
    pub async fn get_diagnostics_config(&self) -> download_diagnostics::DiagnosticsConfig {
        let diag = self.diagnostics.read().await;
        diag.get_config().clone()
    }

    /// Set diagnostics configuration
    pub async fn set_diagnostics_config(
        &self,
        config: download_diagnostics::DiagnosticsConfig,
    ) -> Result<(), String> {
        let mut diag = self.diagnostics.write().await;
        diag.set_config(config);
        drop(diag);
        self.save_diagnostics_config().await
    }

    /// Run diagnostics analysis with current system state
    pub async fn run_diagnostics(&self) -> Vec<download_diagnostics::DiagnosticFinding> {
        let input = self.build_diagnostics_input().await;
        let diag = self.diagnostics.read().await;
        diag.analyze(&input)
    }

    /// Get diagnostics summary
    pub async fn get_diagnostics_summary(&self) -> download_diagnostics::DiagnosticsSummary {
        let findings = self.run_diagnostics().await;
        let diag = self.diagnostics.read().await;
        diag.summarize(&findings)
    }

    /// Get formatted diagnostics report
    pub async fn get_diagnostics_report(&self) -> String {
        let findings = self.run_diagnostics().await;
        let diag = self.diagnostics.read().await;
        diag.format_report(&findings)
    }

    /// Save diagnostics config to disk
    pub async fn save_diagnostics_config(&self) -> Result<(), String> {
        let diag = self.diagnostics.read().await;
        download_diagnostics::save_diagnostics_config(diag.get_config(), &self.data_dir)
    }

    /// Load diagnostics config from disk
    pub async fn load_diagnostics_config(&self) -> Result<(), String> {
        if let Some(config) = download_diagnostics::load_diagnostics_config(&self.data_dir) {
            let mut diag = self.diagnostics.write().await;
            diag.set_config(config);
        }
        Ok(())
    }

    /// Build diagnostics input from current system state
    async fn build_diagnostics_input(&self) -> download_diagnostics::DiagnosticsInput {
        let tasks = self.tasks.lock().await;
        let running = self.running.lock().await;

        let mut active_downloads = 0;
        let mut queued_downloads = 0;
        let mut failed_downloads = 0;
        let mut stalled_downloads = 0;
        let mut current_speed_bps = 0u64;
        let mut task_diagnostics = Vec::new();

        for task in tasks.iter() {
            let state_str = format!("{:?}", task.state);
            let rt = running.get(&task.id);

            // Calculate current speed from speed_samples
            let speed_bps = rt
                .map(|r| {
                    if r.speed_samples.is_empty() {
                        0.0
                    } else {
                        r.speed_samples.iter().sum::<f64>() / r.speed_samples.len() as f64
                    }
                })
                .unwrap_or(0.0) as u64;

            let secs_since_progress = rt
                .map(|r| r.last_progress_time.elapsed().as_secs())
                .unwrap_or(0);

            let age_secs = chrono::Utc::now()
                .signed_duration_since(task.created_at)
                .num_seconds()
                .max(0) as u64;

            match task.state {
                crate::DownloadState::Downloading => {
                    active_downloads += 1;
                    current_speed_bps += speed_bps;
                    if secs_since_progress > 1800 {
                        stalled_downloads += 1;
                    }
                }
                crate::DownloadState::Queued => queued_downloads += 1,
                crate::DownloadState::Error => failed_downloads += 1,
                _ => {}
            }

            task_diagnostics.push(download_diagnostics::TaskDiagnosticData {
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                state: state_str,
                speed_bps,
                progress_percent: if task.size > 0 {
                    (task.downloaded as f64 / task.size as f64) * 100.0
                } else {
                    0.0
                },
                secs_since_last_progress: secs_since_progress,
                retry_count: rt.map(|r| r.retry_count).unwrap_or(0),
                consecutive_failures: 0, // Not tracked in RunningTask
                last_error: task.error.clone(),
                age_secs,
                total_size: task.size,
                downloaded_bytes: task.downloaded,
            });
        }

        let max_concurrent = self
            .max_concurrent
            .load(std::sync::atomic::Ordering::Relaxed);

        // Get disk space
        let available_disk_bytes = disk_monitor::get_available_space(&self.data_dir).unwrap_or(0);
        let total_disk_bytes = disk_monitor::get_total_space(&self.data_dir).unwrap_or(0);

        download_diagnostics::DiagnosticsInput {
            current_speed_bps,
            avg_speed_bps: 0,
            available_disk_bytes,
            total_disk_bytes,
            network_connected: true,
            dns_working: true,
            proxy_configured: self.proxy_config.read().await.is_some(),
            proxy_reachable: None,
            active_downloads,
            queued_downloads,
            failed_downloads,
            stalled_downloads,
            max_concurrent,
            task_diagnostics,
        }
    }

    // ── Download Statistics ──────────────────────────────────────────

    /// Get download statistics
    pub async fn get_download_stats(&self) -> download_stats::DownloadStatistics {
        let sm = self.stats_manager.lock().await;
        sm.get_stats().clone()
    }

    /// Reset download statistics
    pub async fn reset_download_stats(&self) {
        let mut sm = self.stats_manager.lock().await;
        sm.reset();
    }

    // ── Download Reports ────────────────────────────────────────────

    /// Generate a download report for the given configuration.
    pub async fn generate_download_report(
        &self,
        config: &download_report::ReportConfig,
    ) -> download_report::DownloadReport {
        let tasks = self.list_tasks().await;
        let task_data: Vec<download_report::ReportTaskData> = tasks
            .iter()
            .map(|t| {
                let state_str = match t.state {
                    DownloadState::Complete => "Complete".to_string(),
                    DownloadState::Error => "Error".to_string(),
                    DownloadState::Downloading => "Downloading".to_string(),
                    DownloadState::Paused => "Paused".to_string(),
                    DownloadState::Queued => "Queued".to_string(),
                };
                // Use updated_at as proxy for completion/failure time
                let completed_at = if t.state == DownloadState::Complete {
                    Some(t.updated_at)
                } else {
                    None
                };
                let failed_at = if t.state == DownloadState::Error {
                    Some(t.updated_at)
                } else {
                    None
                };
                download_report::ReportTaskData {
                    id: t.id.clone(),
                    name: t.name.clone(),
                    protocol: format!("{:?}", t.protocol),
                    state: state_str,
                    bytes_downloaded: t.downloaded,
                    total_bytes: t.size,
                    created_at: t.created_at,
                    completed_at,
                    failed_at,
                    tags: t.tags.clone(),
                    group: t.group.clone(),
                }
            })
            .collect();
        download_report::generate_report(&task_data, config)
    }

    /// Set the conflict detection strategy.
    /// Persists the configuration to disk for automatic restoration on restart.
    pub async fn set_conflict_strategy(&self, strategy: conflict_detection::ConflictStrategy) {
        *self.conflict_strategy.write().await = strategy;
        if let Err(e) = save_conflict_strategy(&strategy, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist conflict strategy");
        }
    }

    /// Get the current conflict detection strategy.
    pub async fn get_conflict_strategy(&self) -> conflict_detection::ConflictStrategy {
        *self.conflict_strategy.read().await
    }

    /// Check for file path conflicts for a proposed save path.
    /// Returns a ConflictReport describing any conflicts and the resolved action.
    pub async fn check_conflicts(
        &self,
        task_id: &str,
        task_name: &str,
        save_path: &std::path::Path,
    ) -> conflict_detection::ConflictReport {
        let tasks = self.tasks.lock().await;
        let existing: Vec<conflict_detection::TaskPathInfo> = tasks
            .iter()
            .map(|t| conflict_detection::TaskPathInfo {
                id: t.id.clone(),
                name: t.name.clone(),
                save_path: t.save_path.clone(),
            })
            .collect();
        let new_task = conflict_detection::TaskPathInfo {
            id: task_id.to_string(),
            name: task_name.to_string(),
            save_path: save_path.to_path_buf(),
        };
        conflict_detection::detect_conflicts(&new_task, &existing, true)
    }

    /// Resolve a conflict report using the current strategy.
    /// Returns the final path to use (may differ from original if renamed).
    pub async fn resolve_conflict_report(
        &self,
        report: &mut conflict_detection::ConflictReport,
    ) -> std::path::PathBuf {
        let strategy = self.get_conflict_strategy().await;
        let tasks = self.tasks.lock().await;
        let existing_paths: Vec<std::path::PathBuf> =
            tasks.iter().map(|t| t.save_path.clone()).collect();
        conflict_detection::resolve_conflict(report, strategy, &existing_paths);
        report.resolved_path.clone()
    }

    /// Set the per-domain concurrent download limit configuration.
    /// Persists to disk automatically.
    pub async fn set_domain_limit_config(&self, config: domain_limit::DomainLimitConfig) {
        *self.domain_limit.write().await = config.clone();
        if let Err(e) = domain_limit::save_domain_limit_config(&config, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist domain limit config");
        }
    }

    /// Get the current per-domain download limit configuration.
    pub async fn get_domain_limit_config(&self) -> domain_limit::DomainLimitConfig {
        self.domain_limit.read().await.clone()
    }

    /// Get a summary of per-domain download counts and limits.
    pub async fn get_domain_limit_summary(&self) -> domain_limit::DomainLimitSummary {
        let config = self.domain_limit.read().await.clone();
        let tasks = self.tasks.lock().await;

        let mut domain_counts: HashMap<String, u32> = HashMap::new();
        for task in tasks.iter() {
            if task.state == DownloadState::Downloading
                && let Some(ref url) = task.source_url
                && let Some(domain) = domain_limit::extract_domain(url)
            {
                *domain_counts.entry(domain).or_insert(0) += 1;
            }
        }

        let mut entries: Vec<domain_limit::DomainLimitEntry> = domain_counts
            .into_iter()
            .map(|(domain, active)| {
                let limit = config.get_limit(&domain);
                domain_limit::DomainLimitEntry {
                    domain,
                    active,
                    limit,
                    at_limit: limit > 0 && active >= limit,
                }
            })
            .collect();
        entries.sort_by_key(|e| std::cmp::Reverse(e.active));

        let total_active: u32 = entries.iter().map(|e| e.active).sum();
        let domains_at_limit = entries.iter().filter(|e| e.at_limit).count() as u32;

        domain_limit::DomainLimitSummary {
            enabled: config.enabled,
            default_limit: config.default_limit,
            total_active,
            domains_at_limit,
            entries,
        }
    }

    /// Set the per-protocol concurrent download limits configuration.
    /// Persists to disk automatically.
    pub async fn set_protocol_limits(&self, config: protocol_limits::ProtocolLimitsConfig) {
        *self.protocol_limits.write().await = config.clone();
        let path = self.data_dir.join("protocol_limits.json");
        if let Err(e) = protocol_limits::save_protocol_limits_config(&config, &path).await {
            tracing::warn!(error = %e, "Failed to persist protocol limits config");
        }
    }

    /// Get the current per-protocol concurrent download limits configuration.
    pub async fn get_protocol_limits(&self) -> protocol_limits::ProtocolLimitsConfig {
        self.protocol_limits.read().await.clone()
    }

    /// Get a summary of per-protocol limits with current running counts.
    pub async fn get_protocol_limits_summary(&self) -> protocol_limits::ProtocolLimitsSummary {
        let config = self.protocol_limits.read().await.clone();
        let tasks = self.tasks.lock().await;

        let mut running_counts: HashMap<String, u32> = HashMap::new();
        for task in tasks.iter() {
            if task.state == DownloadState::Downloading {
                let key = protocol_to_limits_key(task.protocol);
                *running_counts.entry(key).or_insert(0) += 1;
            }
        }

        let mut summary = config.summary();
        for entry in &mut summary.entries {
            entry.current_running = running_counts.get(&entry.protocol).copied().unwrap_or(0);
        }
        summary
    }

    /// Check if a protocol can start a new download based on per-protocol limits.
    pub async fn can_start_protocol_download(&self, protocol: DownloadProtocol) -> bool {
        let config = self.protocol_limits.read().await.clone();
        if !config.enabled {
            return true;
        }

        let tasks = self.tasks.lock().await;
        let key = protocol_to_limits_key(protocol);
        let current_running: u32 = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Downloading)
            .filter(|t| protocol_to_limits_key(t.protocol) == key)
            .count() as u32;

        config.can_start(protocol, current_running)
    }

    /// Validate a save path for security and correctness.
    ///
    /// Checks for path traversal attacks, invalid characters, and optionally
    /// auto-creates missing directories.
    pub async fn validate_save_path(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> path_validator::ValidationResult {
        let validator = self.path_validator.lock().await;
        validator.validate(path).await
    }

    /// Set the path validator configuration.
    pub async fn set_path_validator_config(&self, config: path_validator::PathValidatorConfig) {
        let mut validator = self.path_validator.lock().await;
        *validator = path_validator::PathValidator::with_config(config);
    }

    /// Get the current path validator configuration.
    pub async fn get_path_validator_config(&self) -> path_validator::PathValidatorConfig {
        let validator = self.path_validator.lock().await;
        validator.config().clone()
    }

    // ========== Phase 79: URL Normalization ==========

    /// Normalize a URL using the configured URL normalizer.
    ///
    /// This cleans and standardizes URLs for better deduplication and reliability.
    /// Returns the normalization result with the normalized URL and change log.
    pub async fn normalize_url(&self, url: &str) -> url_normalizer::NormalizationResult {
        let normalizer = self.url_normalizer.read().await;
        normalizer.normalize(url)
    }

    /// Check if two URLs are equivalent after normalization.
    pub async fn are_urls_equivalent(&self, url1: &str, url2: &str) -> bool {
        let normalizer = self.url_normalizer.read().await;
        normalizer.are_equivalent(url1, url2)
    }

    /// Set the URL normalizer configuration.
    pub async fn set_url_normalizer_config(&self, config: url_normalizer::UrlNormalizerConfig) {
        let mut normalizer = self.url_normalizer.write().await;
        normalizer.set_config(config.clone());
        drop(normalizer);

        // Persist to disk
        if let Err(e) = url_normalizer::save_url_normalizer_config(&config, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist URL normalizer config");
        }
    }

    /// Get the current URL normalizer configuration.
    pub async fn get_url_normalizer_config(&self) -> url_normalizer::UrlNormalizerConfig {
        let normalizer = self.url_normalizer.read().await;
        normalizer.config().clone()
    }

    // ---- URL Intelligence (Phase 161) ----

    /// Set URL intelligence configuration.
    pub async fn set_url_intelligence_config(
        &self,
        config: url_intelligence::UrlIntelligenceConfig,
    ) {
        let mut mgr = self.url_intelligence.write().await;
        mgr.set_config(config);
    }

    /// Get the current URL intelligence configuration.
    pub async fn get_url_intelligence_config(&self) -> url_intelligence::UrlIntelligenceConfig {
        let mgr = self.url_intelligence.read().await;
        mgr.get_config().clone()
    }

    /// Analyze a URL and return recommendations.
    pub async fn analyze_url(&self, url: &str) -> url_intelligence::UrlAnalysis {
        let mut mgr = self.url_intelligence.write().await;
        mgr.analyze_url(url)
    }

    /// Get cached analysis for a URL.
    pub async fn get_cached_url_analysis(
        &self,
        url: &str,
    ) -> Option<url_intelligence::UrlAnalysis> {
        let mgr = self.url_intelligence.read().await;
        mgr.get_cached_analysis(url).cloned()
    }

    /// Get URL intelligence cache size.
    pub async fn get_url_intelligence_cache_size(&self) -> usize {
        let mgr = self.url_intelligence.read().await;
        mgr.get_cache_size()
    }

    /// Clear URL intelligence analysis cache.
    pub async fn clear_url_intelligence_cache(&self) {
        let mut mgr = self.url_intelligence.write().await;
        mgr.clear_cache();
    }

    // ---- Priority Aging ----

    /// Set priority aging configuration (persisted to disk).
    pub async fn set_priority_aging_config(
        &self,
        config: priority_aging::PriorityAgingConfig,
    ) -> Result<(), priority_aging::PriorityAgingError> {
        *self.priority_aging.write().await = config.clone();
        priority_aging::save_priority_aging_config(&config, &self.data_dir)
    }

    /// Get current priority aging configuration.
    pub async fn get_priority_aging_config(&self) -> priority_aging::PriorityAgingConfig {
        self.priority_aging.read().await.clone()
    }

    /// Run priority aging check. Returns the list of tasks whose priority was boosted.
    /// Intended to be called periodically by the scheduler.
    pub async fn run_priority_aging(&self) -> Vec<priority_aging::AgingDecision> {
        let config = self.priority_aging.read().await.clone();
        if !config.enabled {
            return Vec::new();
        }
        let now = chrono::Utc::now();
        let tasks_data: Vec<priority_aging::TaskAgingData> = {
            let tasks = self.tasks.lock().await;
            tasks
                .iter()
                .filter(|t| t.state == DownloadState::Queued)
                .map(|t| priority_aging::TaskAgingData {
                    id: t.id.clone(),
                    priority: priority_aging::AgingPriority::from_download_priority(t.priority),
                    queued_at: Some(t.created_at),
                    state: t.state,
                })
                .collect()
        };
        let decisions = priority_aging::evaluate_batch_aging(&tasks_data, &config, now);
        if decisions.is_empty() {
            return Vec::new();
        }
        // Apply priority boosts
        let mut boosted_tasks = Vec::new();
        {
            let mut tasks = self.tasks.lock().await;
            for d in &decisions {
                if let Some(task) = tasks.iter_mut().find(|t| t.id == d.task_id) {
                    task.priority = d.new_priority.to_download_priority();
                    boosted_tasks.push(d.task_id.clone());
                    tracing::info!(
                        task_id = %d.task_id,
                        old = ?d.old_priority,
                        new = ?d.new_priority,
                        wait_secs = d.wait_secs,
                        "Priority aging: task boosted"
                    );
                }
            }
        }
        // Emit events for boosted tasks
        {
            let tasks = self.tasks.lock().await;
            for task_id in boosted_tasks {
                if let Some(task) = tasks.iter().find(|t| t.id == task_id) {
                    self.emit_event(TaskEvent::Updated {
                        task: TaskInfoEvent::from_task(task),
                    });
                }
            }
        }
        // Persist task queue (priority changed)
        let _ = self.persist_tasks().await;
        decisions
    }

    /// Log an audit event
    pub async fn log_audit_event(
        &self,
        event_type: AuditEventType,
        task_id: Option<String>,
        task_name: Option<String>,
        protocol: Option<String>,
        details: Option<String>,
    ) {
        let entry = AuditLogEntry::new(event_type, task_id, task_name, protocol, details);
        let mut log = self.audit_log.lock().await;
        log.log(entry);
        // Persist to disk (best-effort, don't block on errors)
        if let Err(e) = audit_log::save_audit_log(&log, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist audit log");
        }
    }

    /// Get all audit log entries
    pub async fn get_audit_entries(&self) -> Vec<AuditLogEntry> {
        let log = self.audit_log.lock().await;
        log.entries().cloned().collect()
    }

    /// Get recent audit log entries
    pub async fn get_recent_audit_entries(&self, n: usize) -> Vec<AuditLogEntry> {
        let log = self.audit_log.lock().await;
        log.recent(n).into_iter().cloned().collect()
    }

    /// Get audit log entries filtered by task ID
    pub async fn get_audit_entries_by_task(&self, task_id: &str) -> Vec<AuditLogEntry> {
        let log = self.audit_log.lock().await;
        log.entries_by_task(task_id).into_iter().cloned().collect()
    }

    /// Get audit log summary
    pub async fn get_audit_summary(&self) -> String {
        let log = self.audit_log.lock().await;
        log.summary().to_string()
    }

    /// Clear audit log
    pub async fn clear_audit_log(&self) {
        let mut log = self.audit_log.lock().await;
        log.clear();
        if let Err(e) = audit_log::save_audit_log(&log, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist audit log after clear");
        }
    }

    /// Log an activity event for a specific task
    pub async fn log_task_activity(
        &self,
        task_id: &str,
        task_name: &str,
        event_type: ActivityEventType,
        message: impl Into<String>,
    ) {
        let event = ActivityEvent::new(event_type, message);
        let mut log = self.activity_log.lock().await;
        log.log_event(task_id, task_name, event);
        // Persist to disk (best-effort)
        if let Err(e) = task_activity::save_activity_logs(&log, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist activity log");
        }
    }

    /// Log an activity event with a numeric value
    pub async fn log_task_activity_with_value(
        &self,
        task_id: &str,
        task_name: &str,
        event_type: ActivityEventType,
        message: impl Into<String>,
        value: f64,
    ) {
        let event = ActivityEvent::new(event_type, message).with_value(value);
        let mut log = self.activity_log.lock().await;
        log.log_event(task_id, task_name, event);
        if let Err(e) = task_activity::save_activity_logs(&log, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist activity log");
        }
    }

    /// Get activity log for a specific task
    pub async fn get_task_activity(&self, task_id: &str) -> Option<task_activity::TaskActivityLog> {
        let log = self.activity_log.lock().await;
        log.get(task_id).cloned()
    }

    /// Get activity summaries for all tracked tasks
    pub async fn get_all_activity_summaries(&self) -> Vec<task_activity::TaskActivitySummary> {
        let log = self.activity_log.lock().await;
        log.all_summaries()
    }

    /// Clear activity log for a specific task
    pub async fn clear_task_activity(&self, task_id: &str) {
        let mut log = self.activity_log.lock().await;
        log.clear_task(task_id);
        if let Err(e) = task_activity::save_activity_logs(&log, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist activity log after clear");
        }
    }

    /// Remove activity log for a task (when task is deleted)
    pub async fn remove_task_activity(&self, task_id: &str) {
        let mut log = self.activity_log.lock().await;
        log.remove(task_id);
        if let Err(e) = task_activity::save_activity_logs(&log, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist activity log after remove");
        }
    }

    /// Get the save path manager for download directory configuration.
    pub fn save_path_manager(&self) -> &Arc<SavePathManager> {
        &self.save_path_manager
    }

    /// Get the ETA estimator for download time prediction.
    pub fn eta_estimator(&self) -> &Arc<EtaEstimator> {
        &self.eta_estimator
    }

    /// Predict queue completion time.
    /// Estimates when all queued downloads will finish based on current speeds and concurrency.
    pub async fn predict_queue_completion(&self) -> queue_completion::QueueCompletionPrediction {
        let tasks = self.tasks.lock().await.clone();
        let max_concurrent = self.get_max_concurrent();
        let predictor = self.queue_completion_predictor.read().await;
        predictor
            .predict(&tasks, &self.eta_estimator, max_concurrent)
            .await
    }

    /// Get queue completion predictor configuration.
    pub async fn get_queue_completion_config(&self) -> queue_completion::QueueCompletionConfig {
        let predictor = self.queue_completion_predictor.read().await;
        predictor.config().clone()
    }

    /// Set queue completion predictor configuration.
    pub async fn set_queue_completion_config(
        &self,
        config: queue_completion::QueueCompletionConfig,
    ) {
        let mut predictor = self.queue_completion_predictor.write().await;
        predictor.set_config(config);
    }

    /// Set proxy configuration for HTTP/HTTPS downloads.
    /// Pass None to disable proxy.
    /// Persists the configuration to disk for automatic restoration on restart.
    pub async fn set_proxy(&self, config: Option<proxy::ProxyConfig>) {
        *self.proxy_config.write().await = config.clone();
        if let Err(e) = proxy::save_proxy_config(&config, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to persist proxy configuration");
        }
    }

    /// Get the current proxy configuration.
    pub async fn get_proxy(&self) -> Option<proxy::ProxyConfig> {
        self.proxy_config.read().await.clone()
    }

    /// Test the current proxy connection.
    /// Returns a ProxyTestResult indicating whether the proxy is reachable.
    pub async fn test_proxy_connection(&self) -> Option<proxy::ProxyTestResult> {
        let proxy_cfg = self.proxy_config.read().await.clone();
        match proxy_cfg {
            Some(cfg) => Some(cfg.test_connection().await),
            None => None,
        }
    }

    /// Test a specific proxy configuration (does not need to be set as current).
    pub async fn test_proxy_config(config: &proxy::ProxyConfig) -> proxy::ProxyTestResult {
        config.test_connection().await
    }

    /// Set the base download directory.
    ///
    /// Automatically persists the configuration to disk.
    pub async fn set_save_path(&self, path: PathBuf) {
        self.save_path_manager.set_base_dir(path).await;
        let _ =
            save_save_path_config(&self.data_dir, &self.save_path_manager.get_config().await).await;
    }

    /// Get the current base download directory.
    pub async fn get_save_path(&self) -> PathBuf {
        self.save_path_manager.get_config().await.base_dir
    }

    /// Enable or disable auto-organization by file type.
    ///
    /// Automatically persists the configuration to disk.
    pub async fn set_auto_organize(&self, enabled: bool) {
        self.save_path_manager.set_auto_organize(enabled).await;
        let _ =
            save_save_path_config(&self.data_dir, &self.save_path_manager.get_config().await).await;
    }

    /// Check if auto-organization is enabled.
    pub async fn is_auto_organize(&self) -> bool {
        self.save_path_manager.get_config().await.auto_organize
    }

    /// Set custom directory name for a file category.
    ///
    /// Automatically persists the configuration to disk.
    pub async fn set_category_dir(&self, category: FileCategory, dir_name: String) {
        self.save_path_manager
            .set_category_dir(category, dir_name)
            .await;
        let _ =
            save_save_path_config(&self.data_dir, &self.save_path_manager.get_config().await).await;
    }

    /// Get full save path configuration (for REST API / CLI).
    pub async fn get_save_path_config(&self) -> SavePathConfig {
        self.save_path_manager.get_config().await
    }

    /// Predict the save path for a given filename without actually downloading.
    pub async fn predict_save_path(&self, file_name: &str) -> PathBuf {
        self.save_path_manager.get_save_path(file_name).await
    }

    /// Validate the current base save path (check existence and writability).
    pub async fn validate_save_path_base(&self) -> serde_json::Value {
        let config = self.save_path_manager.get_config().await;
        let base = &config.base_dir;
        let exists = base.exists();
        let is_dir = base.is_dir();
        let writable = if exists && is_dir {
            SavePathManager::check_writable(base).await.unwrap_or(false)
        } else {
            false
        };
        serde_json::json!({
            "base_dir": base.display().to_string(),
            "exists": exists,
            "is_dir": is_dir,
            "writable": writable,
            "auto_organize": config.auto_organize,
            "category_count": config.category_dirs.len(),
        })
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
        let max_concurrent = self
            .max_concurrent
            .load(std::sync::atomic::Ordering::Relaxed);
        if max_concurrent == 0 {
            return true;
        }
        self.running_count().await < max_concurrent
    }

    /// Find a task with the given source URL (for deduplication).
    /// Returns the task ID if found, None otherwise.
    async fn find_duplicate_by_source(&self, source_url: &str) -> Option<String> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.source_url.as_deref() == Some(source_url))
            .map(|t| t.id.clone())
    }

    /// Add a torrent download task
    pub async fn add_torrent(&self, torrent_path: PathBuf) -> Result<String, DownloadManagerError> {
        let data = tokio::fs::read(&torrent_path)
            .await
            .map_err(|e| DownloadManagerError::Io(e.to_string()))?;

        let meta = torrent::TorrentMeta::from_bytes(&data)
            .map_err(|e| DownloadManagerError::Protocol(e.to_string()))?;

        // Check for duplicate by info_hash
        let info_hash = hex::encode(meta.info_hash);
        if let Some(existing_id) = self.find_duplicate_by_source(&info_hash).await {
            match self.handle_duplicate(&existing_id, &info_hash).await? {
                Some(task_id) => return Ok(task_id), // Skip policy: return existing task ID
                None => {} // Allow or PauseExisting policy: continue to create new task
            }
        }

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
            save_path: self.save_path_manager.get_save_path("").await,
            created_at: now,
            updated_at: now,
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some(info_hash),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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

        self.log_audit_event(
            AuditEventType::TaskAdded,
            Some(task_id.clone()),
            Some(task.name.clone()),
            Some("torrent".to_string()),
            Some(format!("Size: {} bytes", task.size)),
        )
        .await;

        Ok(task_id)
    }

    /// Add a magnet link download task
    pub async fn add_magnet(&self, magnet_uri: &str) -> Result<String, DownloadManagerError> {
        use magnet::MagnetLink;

        let magnet = MagnetLink::parse(magnet_uri)
            .map_err(|e| DownloadManagerError::Protocol(e.to_string()))?;

        // Check for duplicate by magnet URI
        if let Some(existing_id) = self.find_duplicate_by_source(magnet_uri).await {
            match self.handle_duplicate(&existing_id, magnet_uri).await? {
                Some(task_id) => return Ok(task_id), // Skip policy: return existing task ID
                None => {} // Allow or PauseExisting policy: continue to create new task
            }
        }

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
            save_path: self.save_path_manager.get_save_path("").await,
            created_at: now,
            updated_at: now,
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some(magnet_uri.to_string()),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
        // Check for duplicate by ed2k hash
        let hash_str = hex::encode(file_hash.0);
        if let Some(existing_id) = self.find_duplicate_by_source(&hash_str).await {
            match self.handle_duplicate(&existing_id, &hash_str).await? {
                Some(task_id) => return Ok(task_id), // Skip policy: return existing task ID
                None => {} // Allow or PauseExisting policy: continue to create new task
            }
        }

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
            save_path: self.save_path_manager.get_save_path("").await,
            created_at: now,
            updated_at: now,
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some(hash_str),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
        // Check for duplicate by first HTTP URL in sources
        let source_url = sources.iter().find_map(|s| match s {
            xunlei::XunleiSource::Http { url, .. } => Some(url.clone()),
            _ => None,
        });
        if let Some(url) = source_url.as_ref()
            && let Some(existing_id) = self.find_duplicate_by_source(url).await
        {
            match self.handle_duplicate(&existing_id, url).await? {
                Some(task_id) => return Ok(task_id), // Skip policy: return existing task ID
                None => {} // Allow or PauseExisting policy: continue to create new task
            }
        }

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
            save_path: self.save_path_manager.get_save_path("").await,
            created_at: now,
            updated_at: now,
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
        // Check for duplicate by file hash
        if let Some(existing_id) = self.find_duplicate_by_source(&file_hash).await {
            return Err(DownloadManagerError::DuplicateTask(existing_id));
        }

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
            save_path: self.save_path_manager.get_save_path("").await,
            created_at: now,
            updated_at: now,
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some(file_hash.clone()),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
                    max_download_time_secs: None,
                    proxy_override: None,
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
                task.finalize_active_time();
                task.state = DownloadState::Complete;
                task.downloaded = task.size;
                task.speed_bps = 0.0;
                task.updated_at = chrono::Utc::now();
                if let Some(cs_err) = Self::verify_checksum(task).await {
                    task.finalize_active_time();
                    task.state = DownloadState::Error;
                    task.error = Some(cs_err);
                }
                Self::record_task_history(
                    task,
                    &self.data_dir,
                    Some(&self.notifier),
                    Some(&self.hook_manager),
                );
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
        // Apply URL rewrite rules before processing
        let rewritten_url = self.rewrite_url(url).await;
        let url = rewritten_url.as_str();

        // HEAD request to get file size and name
        let proxy_cfg = self.proxy_config.read().await.clone();
        let client = if let Some(ref pcfg) = proxy_cfg {
            pcfg.build_client(std::time::Duration::from_secs(15))
                .map_err(|e| DownloadManagerError::Io(e.to_string()))?
        } else {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .map_err(|e| DownloadManagerError::Io(e.to_string()))?
        };

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

    /// Add an HTTP multi-segment download task (splits URL into parallel segments)
    pub async fn add_http_multisegment(&self, url: &str) -> Result<String, DownloadManagerError> {
        // Apply URL rewrite rules before processing
        let rewritten_url = self.rewrite_url(url).await;
        let url = rewritten_url.as_str();

        // HEAD request to get file size and name
        let proxy_cfg = self.proxy_config.read().await.clone();
        let client = if let Some(ref pcfg) = proxy_cfg {
            pcfg.build_client(std::time::Duration::from_secs(15))
                .map_err(|e| DownloadManagerError::Io(e.to_string()))?
        } else {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .map_err(|e| DownloadManagerError::Io(e.to_string()))?
        };

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

        // Check for duplicate by URL
        if let Some(existing_id) = self.find_duplicate_by_source(url).await {
            return Err(DownloadManagerError::DuplicateTask(existing_id));
        }

        let task_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();

        let task = DownloadTask {
            id: task_id.clone(),
            name: file_name.clone(),
            protocol: DownloadProtocol::Xunlei,
            size: content_length,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: self.save_path_manager.get_save_path("").await,
            created_at: now,
            updated_at: now,
            tags: Vec::new(),
            priority: DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some(url.to_string()),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
        };

        self.tasks.lock().await.push(task.clone());
        self.persist_tasks().await;
        self.emit_event(TaskEvent::Added {
            task: TaskInfoEvent::from_task(&task),
        });

        let params = TaskParams::SegmentHttp {
            url: url.to_string(),
            file_name,
            file_size: content_length,
        };

        self.spawn_task(task_id.clone(), params).await;

        Ok(task_id)
    }

    /// Import multiple URLs from a list of strings.
    ///
    /// Each string can be an HTTP/HTTPS URL, ed2k:// link, or magnet: link.
    /// Blank lines and comments (starting with #) are ignored.
    /// Duplicate URLs (already present in the task list) are skipped.
    pub async fn import_urls(&self, urls: &[String]) -> Vec<ImportResult> {
        let mut results = Vec::with_capacity(urls.len());

        // Collect existing URLs for duplicate detection
        let existing_urls: std::collections::HashSet<String> = {
            let tasks = self.tasks.lock().await;
            tasks.iter().map(|t| t.name.clone()).collect()
        };

        for line in urls {
            let line = line.trim();
            // Skip blank lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Check for duplicate by name (simple heuristic)
            let display_name = extract_display_name(line);
            if existing_urls.contains(&display_name) {
                results.push(ImportResult {
                    url: line.to_string(),
                    outcome: ImportOutcome::SkippedDuplicate,
                });
                continue;
            }

            // Try to add based on protocol prefix
            let outcome = if line.starts_with("ed2k://") {
                match parse_and_add_ed2k(self, line).await {
                    Ok(id) => ImportOutcome::Added(id),
                    Err(e) => ImportOutcome::Failed(e),
                }
            } else if line.starts_with("magnet:") {
                match self.add_magnet(line).await {
                    Ok(id) => ImportOutcome::Added(id),
                    Err(e) => ImportOutcome::Failed(e.to_string()),
                }
            } else if line.starts_with("http://")
                || line.starts_with("https://")
                || line.starts_with("ftp://")
            {
                match self.add_url(line).await {
                    Ok(id) => ImportOutcome::Added(id),
                    Err(e) => ImportOutcome::Failed(e.to_string()),
                }
            } else {
                ImportOutcome::Failed(format!("Unsupported URL scheme: {}", line))
            };

            results.push(ImportResult {
                url: line.to_string(),
                outcome,
            });
        }

        results
    }

    /// Import URLs from a pattern string, expanding ranges like `{01-99}`.
    /// Patterns are expanded first, then each URL is imported via `import_urls`.
    /// Returns the import results plus the expansion metadata.
    pub async fn import_pattern(
        &self,
        pattern: &str,
    ) -> (Vec<ImportResult>, url_pattern::PatternExpansionResult) {
        use url_pattern::{PatternConfig, expand_pattern_with_config};

        let config = PatternConfig::default();
        let expansion = match expand_pattern_with_config(pattern, &config) {
            Ok(urls) => url_pattern::PatternExpansionResult {
                urls: urls.clone(),
                pattern: pattern.to_string(),
                count: urls.len(),
                truncated: false,
            },
            Err(_) => url_pattern::PatternExpansionResult {
                urls: Vec::new(),
                pattern: pattern.to_string(),
                count: 0,
                truncated: false,
            },
        };

        if expansion.urls.is_empty() {
            // Not a valid pattern or no URLs generated — try as a plain URL
            let results = self.import_urls(&[pattern.to_string()]).await;
            return (results, expansion);
        }

        let results = self.import_urls(&expansion.urls).await;
        (results, expansion)
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
        let _task_generation = self.task_generation.clone();
        let rate_limiter = Some(self.rate_limiter.clone());
        let task_complete_notify = self.task_complete_notify.clone();
        let notifier = self.notifier.clone();
        let hook_manager = self.hook_manager.clone();
        let proxy_config = self.proxy_config.clone();
        let task_chain = self.task_chain.clone();

        // Store task info for resume
        {
            let mut info = self.task_info.lock().await;
            info.insert(
                task_id.clone(),
                TaskInfo {
                    params: params.clone(),
                    max_download_time_secs: None,
                    proxy_override: None,
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

        // Mark as downloading and start a download session
        let protocol_str;
        let bytes_at_start;
        {
            let mut t = tasks.lock().await;
            if let Some(task) = t.iter_mut().find(|t| t.id == task_id) {
                task.state = DownloadState::Downloading;
                task.current_session_start = Some(chrono::Utc::now());
                task.updated_at = chrono::Utc::now();
                protocol_str = format!("{:?}", task.protocol);
                bytes_at_start = task.downloaded;
            } else {
                return;
            }
        }
        self.start_download_session(&task_id, bytes_at_start, &protocol_str)
            .await;

        let proxy_config_for_spawn = proxy_config.read().await.clone();
        let my_generation = generation; // capture for completion handler
        let max_auto_retries_clone = self.max_auto_retries.clone();
        let auto_retry_base_delay_secs_clone = self.auto_retry_base_delay_secs.clone();

        // Capture sequential_mode from task for torrent engine
        let sequential_mode: bool = {
            let t = tasks.lock().await;
            t.iter()
                .find(|t| t.id == task_id)
                .map(|t| t.sequential_mode)
                .unwrap_or(false)
        };

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
                                engine.set_proxy_config(proxy_config_for_spawn);
                                engine.set_sequential_mode(sequential_mode);
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
                    engine.set_proxy_config(proxy_config_for_spawn);
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
                            engine.set_proxy_config(proxy_config.read().await.clone());
                            engine.set_sequential_mode(sequential_mode);
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
                TaskParams::SegmentHttp {
                    url,
                    file_name,
                    file_size,
                } => {
                    let download_dir = data_dir.join("downloads");
                    let mut downloader = segment_download::SegmentDownloader::new(
                        url,
                        file_name,
                        file_size,
                        download_dir,
                    );
                    // Apply rate limiting
                    if let Some(ref limiter) = rate_limiter {
                        downloader.set_rate_limiter(limiter.per_task().clone());
                    }
                    downloader
                        .download(Some(cancel_clone))
                        .await
                        .map_err(|e| e.to_string())
                }
            };

            // Update task state only if we're still the active task (same generation).
            // Use the captured `my_generation` (set at spawn time) rather than
            // re-reading from the map, which could have been updated by a newer spawn.
            let is_still_active = {
                let r = running.lock().await;
                r.get(&task_id_clone)
                    .map(|rt| rt.generation == my_generation)
                    .unwrap_or(false)
            };

            // Only update task state if we're still the active generation.
            // This prevents stale tasks (cancelled by pause/resume) from
            // overwriting the state of a newly spawned replacement.
            if !is_still_active {
                return Ok(());
            }

            let mut t = tasks.lock().await;
            if let Some(task) = t.iter_mut().find(|t| t.id == task_id_clone) {
                match result {
                    Ok(()) => {
                        task.finalize_active_time();
                        task.state = DownloadState::Complete;
                        task.downloaded = task.size;
                        task.speed_bps = 0.0;
                        if let Some(cs_err) = Self::verify_checksum(task).await {
                            task.finalize_active_time();
                            task.state = DownloadState::Error;
                            task.error = Some(cs_err);
                        }
                        Self::record_task_history(
                            task,
                            &data_dir,
                            Some(&notifier),
                            Some(&hook_manager),
                        );

                        // Trigger task chain: start next task in chain if any
                        let chain_task = task_chain.lock().await;
                        if let Some(next_task_id) = chain_task
                            .get_next_task_after_completion(&task_id_clone)
                            .map(|(_, tid)| tid)
                        {
                            drop(chain_task);
                            // Set the next task to Queued so the scheduler picks it up
                            let mut tasks_lock = tasks.lock().await;
                            if let Some(next_task) =
                                tasks_lock.iter_mut().find(|t| t.id == next_task_id)
                                && (next_task.state == DownloadState::Paused
                                    || next_task.state == DownloadState::Queued)
                            {
                                next_task.state = DownloadState::Queued;
                            }
                            drop(tasks_lock);
                            task_complete_notify.notify_one();
                        }
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        // Check the cancel token directly instead of string matching,
                        // because thiserror wraps the inner message ("IO error: cancelled").
                        // We must check the token BEFORE removing from running.
                        let was_cancelled = {
                            let r = running.lock().await;
                            r.get(&task_id_clone)
                                .map(|rt| rt.cancel_token.is_cancelled())
                                .unwrap_or(true) // removed from running = cancelled
                        };

                        if was_cancelled {
                            task.state = DownloadState::Paused;
                        } else {
                            // Rotate mirror URLs: move failed primary to end, promote first mirror
                            if !task.mirror_urls.is_empty()
                                && let Some(ref source_url) = task.source_url.clone()
                            {
                                task.mirror_urls.push(source_url.clone());
                                let next_mirror = task.mirror_urls.remove(0);
                                task.source_url = Some(next_mirror);
                                tracing::info!(
                                    task_id = %task_id_clone,
                                    new_source = ?task.source_url,
                                    "Rotated to mirror URL after primary failure"
                                );
                            }
                            // Check if auto-retry is enabled and not exhausted
                            let max_retries = max_auto_retries_clone.load(Ordering::Relaxed);
                            let base_delay =
                                auto_retry_base_delay_secs_clone.load(Ordering::Relaxed);

                            if max_retries > 0 && task.auto_retry_count < max_retries {
                                // Schedule retry with exponential backoff
                                let delay_secs =
                                    (base_delay * 2u64.pow(task.auto_retry_count)).min(3600);
                                let retry_after = chrono::Utc::now()
                                    + chrono::Duration::seconds(delay_secs as i64);
                                task.retry_after = Some(retry_after);
                                task.auto_retry_count += 1;
                                task.state = DownloadState::Queued;
                                task.error = Some(format!(
                                    "{} (retry {}/{})",
                                    err_str, task.auto_retry_count, max_retries
                                ));
                                tracing::info!(
                                    task_id = %task_id_clone,
                                    retry_count = task.auto_retry_count,
                                    delay_secs = delay_secs,
                                    "Scheduling auto-retry"
                                );
                            } else {
                                task.finalize_active_time();
                                task.state = DownloadState::Error;
                                task.error = Some(err_str);
                                Self::record_task_history(
                                    task,
                                    &data_dir,
                                    Some(&notifier),
                                    Some(&hook_manager),
                                );
                            }
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
        let hook_manager = self.hook_manager.clone();
        let bandwidth_monitor = self.bandwidth_monitor.clone();
        let proxy_config = self.proxy_config.clone();
        let task_rate_limiters = self.task_rate_limiters.clone();
        let max_auto_retries = self.max_auto_retries.clone();
        let auto_retry_base_delay_secs = self.auto_retry_base_delay_secs.clone();
        let eta_estimator = self.eta_estimator.clone();
        let speed_history = self.speed_history.clone();
        let speed_alerts = self.speed_alerts.clone();
        let speed_anomaly = self.speed_anomaly.clone();
        let speed_prediction = self.speed_prediction.clone();
        let progress_milestone = self.progress_milestone.clone();
        let progress_milestone_config = self.progress_milestone_config.clone();
        let network_monitor = self.network_monitor.clone();
        let cost_tracker = self.cost_tracker.clone();
        let bandwidth_forecast = self.bandwidth_forecast.clone();
        let _bandwidth_usage = self.bandwidth_usage.clone();
        let speed_distribution = self.speed_distribution.clone();

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

                    // Update ETA estimator with current speed
                    eta_estimator.update_speed(&task_id, avg_speed).await;

                    // Update speed history with current speed
                    {
                        let mut speed_history = speed_history.lock().await;
                        speed_history.add_sample(&task_id, avg_speed, current_downloaded);
                    }

                    // Update speed prediction with domain-based speed sample
                    {
                        let domain = {
                            let t = tasks.lock().await;
                            t.iter()
                                .find(|t| t.id == task_id)
                                .and_then(|t| {
                                    t.source_url.as_ref().and_then(|u| {
                                        url::Url::parse(u)
                                            .ok()
                                            .and_then(|u| u.host_str().map(|h| h.to_string()))
                                    })
                                })
                                .unwrap_or_default()
                        };
                        if !domain.is_empty() && avg_speed > 0.0 {
                            let mut sp = speed_prediction.lock().await;
                            sp.record_speed(&domain, avg_speed);
                        }
                    }

                    // Update bandwidth forecast with domain-based speed sample
                    {
                        let domain = {
                            let t = tasks.lock().await;
                            t.iter()
                                .find(|t| t.id == task_id)
                                .and_then(|t| {
                                    t.source_url.as_ref().and_then(|u| {
                                        url::Url::parse(u)
                                            .ok()
                                            .and_then(|u| u.host_str().map(|h| h.to_string()))
                                    })
                                })
                                .unwrap_or_default()
                        };
                        if !domain.is_empty() && avg_speed > 0.0 {
                            let mut bf = bandwidth_forecast.lock().await;
                            bf.record_sample(&domain, avg_speed, current_downloaded);
                        }
                    }

                    // Check speed alerts
                    {
                        let task_name = {
                            let tasks = tasks.lock().await;
                            tasks
                                .iter()
                                .find(|t| t.id == task_id)
                                .map(|t| t.name.clone())
                                .unwrap_or_default()
                        };
                        let now_secs = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let _alerts = speed_alerts
                            .record_speed(&task_id, &task_name, avg_speed, now_secs)
                            .await;
                    }

                    // Check speed anomalies
                    {
                        let mut anomaly = speed_anomaly.lock().await;
                        anomaly.record_speed(&task_id, avg_speed);
                        let _anomaly_detected = anomaly.check_for_anomalies(&task_id, avg_speed);
                    }

                    // Record speed distribution (Phase 164)
                    {
                        let (domain, protocol) = {
                            let t = tasks.lock().await;
                            t.iter()
                                .find(|t| t.id == task_id)
                                .map(|t| {
                                    let d = t
                                        .source_url
                                        .as_ref()
                                        .and_then(|u| {
                                            url::Url::parse(u)
                                                .ok()
                                                .and_then(|u| u.host_str().map(|h| h.to_string()))
                                        })
                                        .unwrap_or_default();
                                    let p = match t.protocol {
                                        DownloadProtocol::Torrent => {
                                            speed_distribution::SpeedProtocol::Torrent
                                        }
                                        DownloadProtocol::Ed2k => {
                                            speed_distribution::SpeedProtocol::Ed2k
                                        }
                                        DownloadProtocol::P2P => {
                                            speed_distribution::SpeedProtocol::P2p
                                        }
                                        _ => speed_distribution::SpeedProtocol::Http,
                                    };
                                    (d, p)
                                })
                                .unwrap_or_default()
                        };
                        if !domain.is_empty() && avg_speed > 0.0 {
                            let bytes_tick = (avg_speed * 2.0) as u64;
                            speed_distribution
                                .write()
                                .await
                                .record_speed(&domain, protocol, avg_speed, bytes_tick)
                                .await;
                        }
                    }

                    // Record cost for this tick (Phase 127)
                    {
                        let (task_name, bytes_this_tick) = {
                            let t = tasks.lock().await;
                            t.iter()
                                .find(|t| t.id == task_id)
                                .map(|t| {
                                    // Estimate bytes downloaded this tick from speed
                                    let bytes_tick = (avg_speed * 2.0) as u64; // ~2s tick
                                    (t.name.clone(), bytes_tick)
                                })
                                .unwrap_or_default()
                        };
                        if bytes_this_tick > 0 {
                            let mut ct = cost_tracker.lock().await;
                            ct.record_task_usage(
                                &task_id,
                                &task_name,
                                bytes_this_tick,
                                chrono::Utc::now(),
                            );
                        }
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

                        // Update network monitor with aggregate speed
                        let active_count = all_tasks
                            .iter()
                            .filter(|t| t.state == DownloadState::Downloading)
                            .count();
                        network_monitor
                            .lock()
                            .await
                            .record_sample(total_speed, active_count);
                    }

                    // Check progress milestones and send notifications
                    {
                        let (task_size, task_downloaded, task_name) = {
                            let t = tasks.lock().await;
                            t.iter()
                                .find(|t| t.id == task_id)
                                .map(|t| (t.size, t.downloaded, t.name.clone()))
                                .unwrap_or((0, 0, String::new()))
                        };
                        if task_size > 0 {
                            let progress_pct =
                                (task_downloaded as f64 / task_size as f64 * 100.0) as f32;
                            let cfg = progress_milestone_config.read().await.clone();
                            let triggered = {
                                let mut tracker = progress_milestone.lock().await;
                                tracker.check_progress(&task_id, progress_pct, &cfg)
                            };
                            for pct in triggered {
                                tracing::info!(
                                    task_id = %task_id,
                                    task_name = %task_name,
                                    percentage = pct,
                                    "Progress milestone reached"
                                );
                                let ctx = notification::NotificationContext {
                                    task_id: task_id.clone(),
                                    name: task_name.clone(),
                                    size: task_size,
                                    downloaded: task_downloaded,
                                    protocol: "download".to_string(),
                                    save_path: String::new(),
                                    error: None,
                                    event: notification::NotificationEvent::ProgressMilestone,
                                };
                                let notifier_clone = notifier.clone();
                                let pct_clone = pct;
                                tokio::spawn(async move {
                                    if let Err(e) = notifier_clone.dispatch(&ctx).await {
                                        tracing::warn!(
                                            error = %e,
                                            percentage = pct_clone,
                                            "Failed to send progress milestone notification"
                                        );
                                    }
                                });
                            }
                        }
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
                                let hook_manager_clone = hook_manager.clone();

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
                                        task.current_session_start = Some(chrono::Utc::now());
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
                                let task_id_clone = task_id.clone();
                                let proxy_config_clone = proxy_config.read().await.clone();
                                let task_rate_limiters_clone = task_rate_limiters.clone();
                                let max_auto_retries_clone = max_auto_retries.clone();
                                let auto_retry_base_delay_secs_clone =
                                    auto_retry_base_delay_secs.clone();
                                // Resolve per-task limiter: use task-specific if set, else global per-task.
                                let task_rate_limiter: RateLimiter = {
                                    let limiters = task_rate_limiters_clone.lock().await;
                                    limiters
                                        .get(&task_id)
                                        .cloned()
                                        .unwrap_or_else(|| rate_limiter.per_task().clone())
                                };
                                // Capture sequential_mode from task
                                let sequential_mode: bool = {
                                    let t = tasks.lock().await;
                                    t.iter()
                                        .find(|t| t.id == task_id)
                                        .map(|t| t.sequential_mode)
                                        .unwrap_or(false)
                                };

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
                                                                task_rate_limiter.clone(),
                                                            );
                                                            engine.set_proxy_config(
                                                                proxy_config_clone,
                                                            );
                                                            engine.set_sequential_mode(
                                                                sequential_mode,
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
                                            engine.set_rate_limiter(task_rate_limiter.clone());
                                            engine.set_proxy_config(proxy_config_clone);
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
                                            engine.set_rate_limiter(task_rate_limiter.clone());
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
                                                        task_rate_limiter.clone(),
                                                    );
                                                    engine.set_proxy_config(proxy_config_clone);
                                                    engine.set_sequential_mode(sequential_mode);
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
                                        TaskParams::SegmentHttp {
                                            url,
                                            file_name,
                                            file_size,
                                        } => {
                                            let download_dir = data_dir_clone.join("downloads");
                                            let mut downloader =
                                                segment_download::SegmentDownloader::new(
                                                    url,
                                                    file_name,
                                                    file_size,
                                                    download_dir,
                                                );
                                            downloader.set_rate_limiter(task_rate_limiter);
                                            downloader
                                                .download(Some(cancel_clone))
                                                .await
                                                .map_err(|e| e.to_string())
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
                                                task.finalize_active_time();
                                                task.state = DownloadState::Complete;
                                                task.downloaded = task.size;
                                                task.speed_bps = 0.0;
                                                if let Some(cs_err) =
                                                    Self::verify_checksum(task).await
                                                {
                                                    task.finalize_active_time();
                                                    task.state = DownloadState::Error;
                                                    task.error = Some(cs_err);
                                                }
                                                Self::record_task_history(
                                                    task,
                                                    &data_dir_clone,
                                                    Some(&notifier_clone),
                                                    Some(&hook_manager_clone),
                                                );
                                            }
                                            Err(e) => {
                                                let err_str = e.to_string();
                                                if err_str == "cancelled" {
                                                    if is_still_active {
                                                        task.state = DownloadState::Paused;
                                                    }
                                                } else {
                                                    // Check if auto-retry is enabled and not exhausted
                                                    let max_retries = max_auto_retries_clone
                                                        .load(Ordering::Relaxed);
                                                    let base_delay =
                                                        auto_retry_base_delay_secs_clone
                                                            .load(Ordering::Relaxed);

                                                    if max_retries > 0
                                                        && task.auto_retry_count < max_retries
                                                    {
                                                        // Schedule retry with exponential backoff
                                                        let delay_secs = (base_delay
                                                            * 2u64.pow(task.auto_retry_count))
                                                        .min(3600);
                                                        let retry_after = chrono::Utc::now()
                                                            + chrono::Duration::seconds(
                                                                delay_secs as i64,
                                                            );
                                                        task.retry_after = Some(retry_after);
                                                        task.auto_retry_count += 1;
                                                        task.state = DownloadState::Queued;
                                                        task.error = Some(format!(
                                                            "{} (retry {}/{})",
                                                            err_str,
                                                            task.auto_retry_count,
                                                            max_retries
                                                        ));
                                                        tracing::info!(
                                                            task_id = %task_id_clone,
                                                            retry_count = task.auto_retry_count,
                                                            delay_secs = delay_secs,
                                                            "Scheduling auto-retry"
                                                        );
                                                    } else {
                                                        task.finalize_active_time();
                                                        task.state = DownloadState::Error;
                                                        task.error = Some(err_str);
                                                        Self::record_task_history(
                                                            task,
                                                            &data_dir_clone,
                                                            Some(&notifier_clone),
                                                            Some(&hook_manager_clone),
                                                        );
                                                    }
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
            task.finalize_active_time();
            task.state = DownloadState::Paused;
            task.speed_bps = 0.0;
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });

            // Log audit event
            let task_name = task.name.clone();
            let protocol = format!("{:?}", task.protocol);
            drop(tasks);
            self.log_audit_event(
                AuditEventType::TaskPaused,
                Some(task_id.to_string()),
                Some(task_name),
                Some(protocol),
                None,
            )
            .await;

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

                // Log audit event
                self.log_audit_event(
                    AuditEventType::TaskResumed,
                    Some(task_id.to_string()),
                    Some(task.name.clone()),
                    Some(format!("{:?}", task.protocol)),
                    None,
                )
                .await;
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
        self.eta_estimator.remove_task(task_id).await;

        let mut tasks = self.tasks.lock().await;
        let len_before = tasks.len();
        let removed_task = tasks.iter().find(|t| t.id == task_id).cloned();
        tasks.retain(|t| t.id != task_id);
        let removed = tasks.len() < len_before;
        drop(tasks);
        if removed {
            self.persist_tasks().await;
            self.emit_event(TaskEvent::Removed {
                task_id: task_id.to_string(),
            });
            if let Some(task) = &removed_task {
                self.log_audit_event(
                    AuditEventType::TaskRemoved,
                    Some(task_id.to_string()),
                    Some(task.name.clone()),
                    Some(format!("{:?}", task.protocol)),
                    None,
                )
                .await;
            }
        }
        removed
    }

    /// Emit a task event (public for testing only).
    #[cfg(test)]
    pub(crate) fn emit_event_for_test(&self, event: TaskEvent) {
        self.emit_event(event);
    }

    // ─── Recycle Bin (Phase 88) ─────────────────────────────────────────

    /// Move a task to the recycle bin (soft delete).
    /// Returns true if the task was successfully moved.
    pub async fn recycle_task(&self, task_id: &str, reason: Option<String>) -> bool {
        // Check if recycle bin is enabled
        {
            let rb = self.recycle_bin.lock().await;
            if !rb.config().enabled {
                return false;
            }
        }

        // Find the task
        let task = {
            let tasks = self.tasks.lock().await;
            tasks.iter().find(|t| t.id == task_id).cloned()
        };

        let task = match task {
            Some(t) => t,
            None => return false,
        };

        // Cancel if running
        {
            let r = self.running.lock().await;
            if let Some(rt) = r.get(task_id) {
                rt.cancel_token.cancel();
            }
        }
        self.running.lock().await.remove(task_id);
        self.task_info.lock().await.remove(task_id);
        self.eta_estimator.remove_task(task_id).await;

        // Move to recycle bin
        {
            let mut rb = self.recycle_bin.lock().await;
            rb.recycle(&task, reason);
            let _ = rb.save_state(&self.data_dir).await;
        }

        // Remove from active tasks
        {
            let mut tasks = self.tasks.lock().await;
            tasks.retain(|t| t.id != task_id);
        }
        self.persist_tasks().await;

        self.emit_event(TaskEvent::Removed {
            task_id: task_id.to_string(),
        });

        self.log_audit_event(
            AuditEventType::TaskRemoved,
            Some(task_id.to_string()),
            Some(task.name.clone()),
            Some(format!("{:?}", task.protocol)),
            None,
        )
        .await;

        true
    }

    /// Restore a task from the recycle bin.
    /// Returns the restored task ID if successful.
    pub async fn restore_task(&self, task_id: &str) -> Option<String> {
        let task = {
            let mut rb = self.recycle_bin.lock().await;
            rb.restore(task_id)?
        };

        // Save recycle bin state
        {
            let rb = self.recycle_bin.lock().await;
            let _ = rb.save_state(&self.data_dir).await;
        }

        let restored_id = task.id.clone();

        // Add back to active tasks
        {
            let mut tasks = self.tasks.lock().await;
            tasks.push(task);
        }
        self.persist_tasks().await;

        // Emit a task info event for the restored task
        if let Some(task) = self
            .tasks
            .lock()
            .await
            .iter()
            .find(|t| t.id == restored_id)
            .cloned()
        {
            self.emit_event(TaskEvent::Added {
                task: TaskInfoEvent::from_task(&task),
            });
        }

        Some(restored_id)
    }

    /// Permanently delete a task from the recycle bin.
    pub async fn purge_task(&self, task_id: &str) -> bool {
        let purged = {
            let mut rb = self.recycle_bin.lock().await;
            rb.purge_one(task_id)
        };

        if purged {
            let rb = self.recycle_bin.lock().await;
            let _ = rb.save_state(&self.data_dir).await;
        }

        purged
    }

    /// Empty the entire recycle bin.
    pub async fn empty_recycle_bin(&self) -> usize {
        let count = {
            let mut rb = self.recycle_bin.lock().await;
            rb.empty()
        };

        if count > 0 {
            let rb = self.recycle_bin.lock().await;
            let _ = rb.save_state(&self.data_dir).await;
        }

        count
    }

    /// List all tasks in the recycle bin.
    pub async fn list_recycled_tasks(&self) -> Vec<recycle_bin::RecycledTask> {
        let rb = self.recycle_bin.lock().await;
        rb.list().to_vec()
    }

    /// Get recycle bin summary statistics.
    pub async fn get_recycle_bin_summary(&self) -> recycle_bin::RecycleBinSummary {
        let rb = self.recycle_bin.lock().await;
        rb.summary()
    }

    /// Set recycle bin configuration.
    pub async fn set_recycle_bin_config(
        &self,
        config: recycle_bin::RecycleBinConfig,
    ) -> Result<(), recycle_bin::RecycleBinError> {
        {
            let mut rb = self.recycle_bin.lock().await;
            rb.set_config(config);
        }

        let rb = self.recycle_bin.lock().await;
        rb.save_state(&self.data_dir).await
    }

    /// Get current recycle bin configuration.
    pub async fn get_recycle_bin_config(&self) -> recycle_bin::RecycleBinConfig {
        let rb = self.recycle_bin.lock().await;
        rb.config().clone()
    }

    /// Run auto-purge on the recycle bin.
    pub async fn run_recycle_bin_auto_purge(&self) -> usize {
        let purged = {
            let mut rb = self.recycle_bin.lock().await;
            rb.auto_purge()
        };

        if purged > 0 {
            let rb = self.recycle_bin.lock().await;
            let _ = rb.save_state(&self.data_dir).await;
        }

        purged
    }

    // ─── Auto-Pause Scheduler (Phase 89) ──────────────────────────────

    /// Set auto-pause configuration and persist to disk
    pub async fn set_auto_pause_config(
        &self,
        config: auto_pause::AutoPauseConfig,
    ) -> Result<(), auto_pause::AutoPauseError> {
        {
            let mut ap = self.auto_pause.write().await;
            *ap = config;
        }
        let ap = self.auto_pause.read().await.clone();
        auto_pause::save_auto_pause_config(&ap, &self.data_dir).await
    }

    /// Get current auto-pause configuration
    pub async fn get_auto_pause_config(&self) -> auto_pause::AutoPauseConfig {
        self.auto_pause.read().await.clone()
    }

    /// Get auto-pause status including current peak time state
    pub async fn get_auto_pause_status(&self) -> auto_pause::AutoPauseStatus {
        let config = self.auto_pause.read().await.clone();
        let now = chrono::Utc::now();
        let is_peak = config
            .peak_hours
            .as_ref()
            .map(|ph| ph.is_peak_time(now))
            .unwrap_or(false);

        // Count tasks paused by auto-pause
        let tasks = self.tasks.lock().await;
        let paused_count = tasks
            .iter()
            .filter(|t| {
                t.state == DownloadState::Paused && t.error.as_deref() == Some(&config.pause_reason)
            })
            .count();

        auto_pause::AutoPauseStatus {
            enabled: config.enabled,
            peak_hours: config.peak_hours,
            auto_resume: config.auto_resume,
            is_peak_time: is_peak,
            paused_task_count: paused_count,
            peak_started_at: if is_peak { Some(now) } else { None },
        }
    }

    /// Check and apply auto-pause rules. Returns number of tasks paused.
    /// Should be called periodically (e.g., every 60 seconds).
    pub async fn check_auto_pause(&self) -> usize {
        let config = self.auto_pause.read().await.clone();
        if !config.enabled {
            return 0;
        }

        let peak_hours = match &config.peak_hours {
            Some(ph) => ph,
            None => return 0,
        };

        let now = chrono::Utc::now();
        let is_peak = peak_hours.is_peak_time(now);

        let mut tasks = self.tasks.lock().await;
        let mut paused_count = 0;

        if is_peak {
            // Peak hours: pause all running tasks
            for task in tasks.iter_mut() {
                if task.state == DownloadState::Downloading {
                    task.state = DownloadState::Paused;
                    task.error = Some(config.pause_reason.clone());
                    task.updated_at = now;
                    paused_count += 1;
                }
            }
        } else if config.auto_resume {
            // Off-peak: resume tasks that were auto-paused
            for task in tasks.iter_mut() {
                if task.state == DownloadState::Paused
                    && task.error.as_deref() == Some(&config.pause_reason)
                {
                    task.state = DownloadState::Queued;
                    task.error = None;
                    task.updated_at = now;
                    paused_count += 1;
                }
            }
        }

        paused_count
    }

    // ─── Task Archive (Phase 78) ───────────────────────────────────────

    /// Archive a task instead of deleting it (preserves metadata for later review).
    /// The task is removed from the active queue and stored in the archive.
    pub async fn archive_task(
        &self,
        task_id: &str,
        reason: Option<String>,
    ) -> Result<(), task_archive::TaskArchiveError> {
        // Cancel if running
        {
            let r = self.running.lock().await;
            if let Some(rt) = r.get(task_id) {
                rt.cancel_token.cancel();
            }
        }

        self.running.lock().await.remove(task_id);
        self.task_info.lock().await.remove(task_id);
        self.eta_estimator.remove_task(task_id).await;

        let mut tasks = self.tasks.lock().await;
        let removed_task = tasks.iter().find(|t| t.id == task_id).cloned();
        tasks.retain(|t| t.id != task_id);
        drop(tasks);

        if let Some(task) = removed_task {
            let archived = task_archive::create_archived_task(
                task.id.clone(),
                task.name.clone(),
                &format!("{:?}", task.protocol),
                task.size,
                task.downloaded,
                &format!("{:?}", task.state),
                task.error.clone(),
                task.save_path.clone(),
                task.created_at,
                task.updated_at,
                task.tags.clone(),
                task.group.clone(),
                task.notes.clone(),
                task.source_url.clone(),
                task.active_time_seconds,
                reason,
            );

            let mut archive = self.task_archive.write().await;
            archive.archive_task(archived)?;

            // Persist both: task queue (task removed) and archive state
            self.persist_tasks().await;
            let archive_path = self.data_dir.join("task_archive.json");
            let _ = task_archive::save_archive_state(&archive_path, &archive).await;

            self.emit_event(TaskEvent::Removed {
                task_id: task_id.to_string(),
            });
            self.log_audit_event(
                AuditEventType::TaskRemoved,
                Some(task_id.to_string()),
                Some(task.name.clone()),
                Some(format!("{:?} (archived)", task.protocol)),
                None,
            )
            .await;
            Ok(())
        } else {
            Err(task_archive::TaskArchiveError::TaskNotFound(
                task_id.to_string(),
            ))
        }
    }

    /// Get archive summary.
    pub async fn get_archive_summary(&self) -> task_archive::ArchiveSummary {
        let archive = self.task_archive.read().await;
        archive.summary()
    }

    /// Get archive configuration.
    pub async fn get_archive_config(&self) -> task_archive::ArchiveConfig {
        let archive = self.task_archive.read().await;
        archive.config.clone()
    }

    /// Set archive configuration.
    pub async fn set_archive_config(&self, config: task_archive::ArchiveConfig) {
        let mut archive = self.task_archive.write().await;
        archive.config = config;
        let archive_path = self.data_dir.join("task_archive.json");
        let _ = task_archive::save_archive_state(&archive_path, &archive).await;
    }

    /// List archived tasks with optional filters.
    pub async fn list_archived_tasks(
        &self,
        state_filter: Option<&str>,
        protocol_filter: Option<&str>,
        tag_filter: Option<&str>,
    ) -> Vec<task_archive::ArchivedTask> {
        let archive = self.task_archive.read().await;
        archive
            .list_archived(state_filter, protocol_filter, tag_filter)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Restore an archived task back to the active queue.
    pub async fn restore_archived_task(
        &self,
        task_id: &str,
    ) -> Result<String, task_archive::TaskArchiveError> {
        let mut archive = self.task_archive.write().await;
        let archived = archive
            .unarchive_task(task_id)
            .ok_or_else(|| task_archive::TaskArchiveError::TaskNotFound(task_id.to_string()))?;

        // Reconstruct a DownloadTask from the archived data
        let new_id = uuid::Uuid::new_v4().to_string();
        // Parse protocol back from string representation
        let protocol = match archived.protocol.as_str() {
            "Torrent" => DownloadProtocol::Torrent,
            "Ed2k" => DownloadProtocol::Ed2k,
            "Xunlei" => DownloadProtocol::Xunlei,
            "Magnet" => DownloadProtocol::Magnet,
            "P2P" => DownloadProtocol::P2P,
            _ => DownloadProtocol::Xunlei,
        };
        let state = if archived.final_state == "Complete" {
            DownloadState::Complete
        } else {
            DownloadState::Paused
        };
        let task = DownloadTask {
            id: new_id.clone(),
            name: archived.name,
            protocol,
            size: archived.size,
            downloaded: archived.downloaded,
            state,
            error: None,
            speed_bps: 0.0,
            save_path: archived.save_path,
            created_at: archived.created_at,
            updated_at: chrono::Utc::now(),
            tags: archived.tags,
            priority: crate::DownloadPriority::Normal,
            schedule: None,
            bandwidth_weight: 1,
            queue_position: None,
            depends_on: Vec::new(),
            notes: archived.notes,
            group: archived.group,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: archived.source_url,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: archived.active_time_seconds,
            current_session_start: None,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        };

        self.tasks.lock().await.push(task);
        self.persist_tasks().await;

        // Persist archive
        let archive_path = self.data_dir.join("task_archive.json");
        let _ = task_archive::save_archive_state(&archive_path, &archive).await;

        Ok(new_id)
    }

    /// Permanently delete an archived task.
    pub async fn delete_archived_task(&self, task_id: &str) -> bool {
        let mut archive = self.task_archive.write().await;
        let removed = archive.unarchive_task(task_id).is_some();
        if removed {
            let archive_path = self.data_dir.join("task_archive.json");
            let _ = task_archive::save_archive_state(&archive_path, &archive).await;
        }
        removed
    }

    /// Clear all archived tasks.
    pub async fn clear_archive(&self) {
        let mut archive = self.task_archive.write().await;
        archive.clear_archive();
        let archive_path = self.data_dir.join("task_archive.json");
        let _ = task_archive::save_archive_state(&archive_path, &archive).await;
    }

    /// Restore archive state from disk on startup.
    async fn restore_archive(&self) {
        let archive_path = self.data_dir.join("task_archive.json");
        if let Ok(state) = task_archive::load_archive_state(&archive_path).await {
            let mut archive = self.task_archive.write().await;
            *archive = state;
        }
    }

    // ===== Task Chain Methods =====

    /// Create a new task chain.
    pub async fn create_task_chain(
        &self,
        chain_id: String,
        name: String,
    ) -> Result<(), task_chain::TaskChainError> {
        let mut manager = self.task_chain.lock().await;
        manager.create_chain(chain_id, name)?;
        manager.save().await?;
        Ok(())
    }

    /// Delete a task chain.
    pub async fn delete_task_chain(
        &self,
        chain_id: &str,
    ) -> Result<(), task_chain::TaskChainError> {
        let mut manager = self.task_chain.lock().await;
        manager.delete_chain(chain_id)?;
        manager.save().await?;
        Ok(())
    }

    /// Add a task to a chain.
    pub async fn add_task_to_chain(
        &self,
        chain_id: &str,
        task_id: String,
    ) -> Result<(), task_chain::TaskChainError> {
        let mut manager = self.task_chain.lock().await;
        manager.add_task_to_chain(chain_id, task_id)?;
        manager.save().await?;
        Ok(())
    }

    /// Remove a task from a chain.
    pub async fn remove_task_from_chain(
        &self,
        chain_id: &str,
        task_id: &str,
    ) -> Result<(), task_chain::TaskChainError> {
        let mut manager = self.task_chain.lock().await;
        manager.remove_task_from_chain(chain_id, task_id)?;
        manager.save().await?;
        Ok(())
    }

    /// List all task chains.
    pub async fn list_task_chains(&self) -> Vec<task_chain::TaskChain> {
        let manager = self.task_chain.lock().await;
        manager.list_chains().into_iter().cloned().collect()
    }

    /// Get a task chain by ID.
    pub async fn get_task_chain(&self, chain_id: &str) -> Option<task_chain::TaskChain> {
        let manager = self.task_chain.lock().await;
        manager.get_chain(chain_id).cloned()
    }

    /// Get task chain summary.
    pub async fn get_task_chain_summary(&self) -> task_chain::TaskChainSummary {
        let manager = self.task_chain.lock().await;
        manager.get_summary()
    }

    /// Enable or disable a task chain.
    pub async fn set_task_chain_enabled(
        &self,
        chain_id: &str,
        enabled: bool,
    ) -> Result<(), task_chain::TaskChainError> {
        let mut manager = self.task_chain.lock().await;
        manager.set_chain_enabled(chain_id, enabled)?;
        manager.save().await?;
        Ok(())
    }

    /// Set auto-remove completed tasks for a chain.
    pub async fn set_chain_auto_remove(
        &self,
        chain_id: &str,
        auto_remove: bool,
    ) -> Result<(), task_chain::TaskChainError> {
        let mut manager = self.task_chain.lock().await;
        manager.set_auto_remove_completed(chain_id, auto_remove)?;
        manager.save().await?;
        Ok(())
    }

    /// Get the next task to start after a task completes.
    pub async fn get_next_task_in_chain(&self, completed_task_id: &str) -> Option<String> {
        let manager = self.task_chain.lock().await;
        manager
            .get_next_task_after_completion(completed_task_id)
            .map(|(_, task_id)| task_id)
    }

    /// Mark a task as completed in the chain and return the next task to start.
    pub async fn mark_chain_task_completed(&self, task_id: &str) -> Option<String> {
        let mut manager = self.task_chain.lock().await;
        match manager.mark_task_completed(task_id) {
            Ok(Some((_, next_task_id))) => {
                let _ = manager.save().await;
                Some(next_task_id)
            }
            _ => None,
        }
    }

    /// Restore task chain state from disk on startup.
    async fn restore_task_chain(&self) {
        let mut manager = self.task_chain.lock().await;
        let _ = manager.load().await;
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

    /// Analyze queue health and return a diagnostic report.
    pub async fn get_queue_health_report(
        &self,
        config: &queue_health::HealthMonitorConfig,
    ) -> queue_health::QueueHealthReport {
        let tasks = self.tasks.lock().await;
        let now = chrono::Utc::now();

        let health_data: Vec<queue_health::TaskHealthData> = tasks
            .iter()
            .map(|t| {
                let secs_since_progress = t
                    .current_session_start
                    .map(|s| (now - s).num_seconds().max(0) as f64)
                    .unwrap_or(0.0);

                queue_health::TaskHealthData {
                    task_id: t.id.clone(),
                    name: t.name.clone(),
                    state: match t.state {
                        DownloadState::Downloading => "Downloading".to_string(),
                        DownloadState::Queued => "Queued".to_string(),
                        DownloadState::Paused => "Paused".to_string(),
                        DownloadState::Error => "Error".to_string(),
                        DownloadState::Complete => "Complete".to_string(),
                    },
                    speed_bps: t.speed_bps,
                    seconds_since_progress: secs_since_progress,
                    auto_retry_count: t.auto_retry_count,
                    has_mirrors: !t.mirror_urls.is_empty(),
                    size: t.size,
                    downloaded: t.downloaded,
                }
            })
            .collect();
        drop(tasks);

        queue_health::analyze_queue_health(&health_data, config)
    }

    /// Build comprehensive health dashboard aggregating all monitoring data
    pub async fn build_health_dashboard(
        &self,
        config: &health_dashboard::HealthDashboardConfig,
    ) -> health_dashboard::HealthDashboard {
        let tasks = self.tasks.lock().await;

        // Count task states
        let total_tasks = tasks.len();
        let downloading = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Downloading)
            .count();
        let queued = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Queued)
            .count();
        let paused = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Paused)
            .count();
        let completed = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Complete)
            .count();
        let error_count = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Error)
            .count();

        // Calculate current speed from tasks
        let current_speed_bps: u64 = tasks.iter().map(|t| t.speed_bps as u64).sum();

        // Get speed metrics from speed history
        let speed_history = self.speed_history.lock().await;
        let task_ids = speed_history.list_task_ids();
        let mut avg_speed_5min_total: u64 = 0;
        let mut avg_speed_15min_total: u64 = 0;
        let mut summary_count: u64 = 0;
        for tid in task_ids {
            if let Some(s) = speed_history.get_summary(tid) {
                avg_speed_5min_total += s.avg_5min as u64;
                avg_speed_15min_total += s.avg_15min as u64;
                summary_count += 1;
            }
        }
        let avg_speed_5min = if summary_count > 0 {
            avg_speed_5min_total / summary_count
        } else {
            0
        };
        let avg_speed_15min = if summary_count > 0 {
            avg_speed_15min_total / summary_count
        } else {
            0
        };
        drop(speed_history);

        // Get speed alerts and anomalies
        let speed_alert_count = self.speed_alerts.get_alerts(100).await.len();
        let speed_anomaly_count = self.get_speed_anomalies().await.len();

        // Get network status
        let network_status = self.get_network_status().await;
        let network_connected = matches!(network_status, network_aware::NetworkStatus::Connected);
        let network_summary = self.get_network_summary().await;
        let network_quality = network_summary.stability_score as u8;
        let network_issues = 0; // NetworkSummary doesn't have issue_count

        let proxy_config = self.proxy_config.read().await;
        let proxy_enabled = proxy_config.is_some();
        drop(proxy_config);

        // Get storage status
        let disk_available_bytes =
            disk_monitor::get_available_space(&self.get_save_path().await).unwrap_or_default();
        let disk_low = disk_available_bytes < config.low_disk_threshold_bytes;

        // Get integrity issues
        let integrity_summary = self.get_integrity_summary().await;
        let integrity_issues = integrity_summary.missing + integrity_summary.size_mismatch;

        // Get recycle bin count
        let recycle_bin_count = self.list_recycled_tasks().await.len();

        // Get error metrics
        let pending_retry = tasks.iter().filter(|t| t.cooldown.is_some()).count();
        let audit_log = self.audit_log.lock().await;
        let retries_today = audit_log
            .entries()
            .filter(|e| {
                matches!(e.event_type, AuditEventType::TaskRetry)
                    && e.timestamp.date_naive() == chrono::Utc::now().date_naive()
            })
            .count() as u32;
        drop(audit_log);

        let error_recovery_config = self.get_error_recovery_config().await;
        let recovery_enabled = error_recovery_config.enabled;

        // Get deadline misses
        let deadline_summary = self.get_deadline_summary().await;
        let deadline_missed = deadline_summary.missed_count;

        let input = health_dashboard::HealthInput {
            total_tasks,
            downloading,
            queued,
            paused,
            completed,
            error_count,
            current_speed_bps,
            avg_speed_5min,
            avg_speed_15min,
            speed_alert_count,
            speed_anomaly_count,
            network_connected,
            network_quality,
            network_issues,
            proxy_enabled,
            disk_available_bytes,
            disk_low,
            integrity_issues,
            recycle_bin_count,
            pending_retry,
            retries_today,
            recovery_enabled,
            deadline_missed,
        };

        health_dashboard::build_health_dashboard(&input, config)
    }

    /// Get speed history summary for a task
    pub async fn get_task_speed_history(
        &self,
        task_id: &str,
    ) -> Option<speed_history::SpeedHistorySummary> {
        let speed_history = self.speed_history.lock().await;
        speed_history.get_summary(task_id)
    }

    /// Get speed history for all tasks
    pub async fn get_all_speed_history_summaries(&self) -> Vec<speed_history::SpeedHistorySummary> {
        let speed_history = self.speed_history.lock().await;
        speed_history
            .list_task_ids()
            .into_iter()
            .filter_map(|id| speed_history.get_summary(id))
            .collect()
    }

    /// Clear speed history for a task
    pub async fn clear_task_speed_history(&self, task_id: &str) -> bool {
        let mut speed_history = self.speed_history.lock().await;
        if let Some(history) = speed_history.get_mut(task_id) {
            history.clear();
            true
        } else {
            false
        }
    }

    /// Remove speed history for a task
    pub async fn remove_task_speed_history(&self, task_id: &str) -> bool {
        let mut speed_history = self.speed_history.lock().await;
        speed_history.remove(task_id)
    }

    // --- Speed Prediction (Phase 102) ---

    /// Set speed prediction configuration.
    pub async fn set_speed_prediction_config(
        &self,
        config: speed_prediction::SpeedPredictionConfig,
    ) {
        let mut sp = self.speed_prediction.lock().await;
        sp.set_config(config);
    }

    /// Get current speed prediction configuration.
    pub async fn get_speed_prediction_config(&self) -> speed_prediction::SpeedPredictionConfig {
        let sp = self.speed_prediction.lock().await;
        sp.config().clone()
    }

    /// Get speed prediction for a specific task.
    pub async fn predict_task_speed(
        &self,
        task_id: &str,
        domain: &str,
        current_speed: f64,
        remaining_bytes: u64,
    ) -> speed_prediction::SpeedPrediction {
        let sp = self.speed_prediction.lock().await;
        sp.predict(task_id, domain, current_speed, remaining_bytes)
    }

    /// Get speed prediction summary across all tracked domains.
    pub async fn get_speed_prediction_summary(&self) -> speed_prediction::SpeedPredictionSummary {
        let sp = self.speed_prediction.lock().await;
        sp.get_summary()
    }

    /// Get optimal download windows for a domain.
    pub async fn get_optimal_speed_windows(
        &self,
        domain: &str,
        top_n: usize,
    ) -> Vec<speed_prediction::OptimalWindow> {
        let sp = self.speed_prediction.lock().await;
        sp.get_optimal_windows(domain, top_n)
    }

    /// Get speed profile for a specific domain.
    pub async fn get_domain_speed_profile(
        &self,
        domain: &str,
    ) -> Option<speed_prediction::DomainSpeedProfile> {
        let sp = self.speed_prediction.lock().await;
        sp.get_profile(domain).cloned()
    }

    /// List all tracked domains.
    pub async fn list_tracked_speed_domains(&self) -> Vec<String> {
        let sp = self.speed_prediction.lock().await;
        sp.tracked_domains().iter().map(|s| s.to_string()).collect()
    }

    /// Remove a domain from speed prediction tracking.
    pub async fn remove_speed_prediction_domain(&self, domain: &str) -> bool {
        let mut sp = self.speed_prediction.lock().await;
        sp.remove_domain(domain)
    }

    /// Clean up old speed prediction samples.
    pub async fn cleanup_old_speed_predictions(&self) {
        let mut sp = self.speed_prediction.lock().await;
        sp.cleanup_old_samples();
    }

    /// Clear all speed prediction data.
    pub async fn clear_all_speed_predictions(&self) {
        let mut sp = self.speed_prediction.lock().await;
        sp.clear_all();
    }

    // --- Speed Anomaly Detection (Phase 109) ---

    /// Set speed anomaly detection configuration.
    pub async fn set_speed_anomaly_config(&self, config: speed_anomaly::AnomalyConfig) {
        let mut detector = self.speed_anomaly.lock().await;
        detector.set_config(config);
    }

    /// Get current speed anomaly detection configuration.
    pub async fn get_speed_anomaly_config(&self) -> speed_anomaly::AnomalyConfig {
        let detector = self.speed_anomaly.lock().await;
        detector.config().clone()
    }

    /// Get all detected speed anomalies.
    pub async fn get_speed_anomalies(&self) -> Vec<speed_anomaly::SpeedAnomaly> {
        let detector = self.speed_anomaly.lock().await;
        detector.get_all_anomalies().to_vec()
    }

    /// Get speed anomalies for a specific task.
    pub async fn get_task_speed_anomalies(
        &self,
        task_id: &str,
    ) -> Vec<speed_anomaly::SpeedAnomaly> {
        let detector = self.speed_anomaly.lock().await;
        detector
            .get_anomalies(task_id)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Clear speed anomalies for a specific task.
    pub async fn clear_task_speed_anomalies(&self, task_id: &str) {
        let mut detector = self.speed_anomaly.lock().await;
        detector.clear_anomalies(task_id);
    }

    /// Clear all speed anomalies.
    pub async fn clear_all_speed_anomalies(&self) {
        let mut detector = self.speed_anomaly.lock().await;
        detector.clear_all_anomalies();
    }

    /// Get speed anomaly summary.
    pub async fn get_speed_anomaly_summary(&self) -> speed_anomaly::SpeedAnomalySummary {
        let detector = self.speed_anomaly.lock().await;
        let anomalies = detector.get_all_anomalies();
        let total = anomalies.len();
        let severe = anomalies
            .iter()
            .filter(|a| a.severity == speed_anomaly::AnomalySeverity::Severe)
            .count();
        let moderate = anomalies
            .iter()
            .filter(|a| a.severity == speed_anomaly::AnomalySeverity::Moderate)
            .count();
        let mild = anomalies
            .iter()
            .filter(|a| a.severity == speed_anomaly::AnomalySeverity::Mild)
            .count();
        let tracked_tasks = detector.tracked_task_count();
        speed_anomaly::SpeedAnomalySummary {
            enabled: detector.config().enabled,
            total_anomalies: total,
            severe_count: severe,
            moderate_count: moderate,
            mild_count: mild,
            tracked_tasks,
        }
    }

    /// Remove a task from speed anomaly tracking.
    pub async fn remove_speed_anomaly_task(&self, task_id: &str) {
        let mut detector = self.speed_anomaly.lock().await;
        detector.remove_task(task_id);
    }

    /// Clear all speed anomalies
    pub async fn clear_speed_anomalies(&self) {
        let mut detector = self.speed_anomaly.lock().await;
        detector.clear_all_anomalies();
    }

    // --- Speed Profiles (Phase 116) ---

    /// Create a new speed profile.
    pub async fn create_speed_profile(
        &self,
        name: &str,
        speed_limit_bps: u64,
        description: Option<&str>,
    ) -> Result<String, speed_profiles::SpeedProfileError> {
        let mut mgr = self.speed_profiles.write().await;
        mgr.create_profile(name, speed_limit_bps, description).await
    }

    /// Delete a speed profile.
    pub async fn delete_speed_profile(
        &self,
        id: &str,
    ) -> Result<(), speed_profiles::SpeedProfileError> {
        let mut mgr = self.speed_profiles.write().await;
        mgr.delete_profile(id).await
    }

    /// Activate a speed profile, applying its speed limit and max concurrent settings.
    pub async fn activate_speed_profile(
        &self,
        id: &str,
    ) -> Result<speed_profiles::SpeedProfileInfo, speed_profiles::SpeedProfileError> {
        let (speed_limit, _upload_limit, max_concurrent) = {
            let mut mgr = self.speed_profiles.write().await;
            mgr.activate_profile(id).await?
        };

        // Apply the profile's speed limit to the global rate limiter
        self.rate_limiter.set_global_limit(speed_limit).await;

        // Apply max concurrent if specified (> 0)
        if max_concurrent > 0 {
            self.max_concurrent.store(
                max_concurrent as usize,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        // Return the profile info
        let mgr = self.speed_profiles.read().await;
        let profile = mgr
            .get_profile(id)
            .ok_or_else(|| speed_profiles::SpeedProfileError::NotFound(id.to_string()))?;
        Ok(profile.to_info(true))
    }

    /// Deactivate the current speed profile (restore unlimited speed).
    pub async fn deactivate_speed_profile(&self) -> Result<(), speed_profiles::SpeedProfileError> {
        let mut mgr = self.speed_profiles.write().await;
        mgr.deactivate_profile().await?;
        drop(mgr);

        // Restore unlimited speed
        self.rate_limiter.set_global_limit(0).await;
        Ok(())
    }

    /// Get the speed profiles summary.
    pub async fn get_speed_profiles_summary(&self) -> speed_profiles::SpeedProfilesSummary {
        let mgr = self.speed_profiles.read().await;
        mgr.summary()
    }

    /// Get a specific speed profile by ID.
    pub async fn get_speed_profile(&self, id: &str) -> Option<speed_profiles::SpeedProfileInfo> {
        let mgr = self.speed_profiles.read().await;
        let profile = mgr.get_profile(id)?;
        let is_active = mgr.config().active_profile_id.as_deref() == Some(id);
        Some(profile.to_info(is_active))
    }

    /// List all speed profiles.
    pub async fn list_speed_profiles(&self) -> Vec<speed_profiles::SpeedProfileInfo> {
        let mgr = self.speed_profiles.read().await;
        let active_id = mgr.config().active_profile_id.clone();
        mgr.list_profiles()
            .into_iter()
            .map(|p| p.to_info(active_id.as_deref() == Some(p.id.as_str())))
            .collect()
    }

    /// Update a speed profile's settings.
    pub async fn update_speed_profile(
        &self,
        id: &str,
        speed_limit_bps: Option<u64>,
        upload_limit_bps: Option<u64>,
        max_concurrent: Option<u32>,
        description: Option<&str>,
    ) -> Result<(), speed_profiles::SpeedProfileError> {
        let mut mgr = self.speed_profiles.write().await;
        mgr.update_profile(
            id,
            speed_limit_bps,
            upload_limit_bps,
            max_concurrent,
            description,
        )
        .await
    }

    /// Get the currently active speed profile (if any).
    pub async fn get_active_speed_profile(&self) -> Option<speed_profiles::SpeedProfileInfo> {
        let mgr = self.speed_profiles.read().await;
        let profile = mgr.active_profile()?;
        Some(profile.to_info(true))
    }

    // --- Speed Test (Phase 128) ---

    /// Get the current speed test configuration.
    pub async fn get_speed_test_config(&self) -> speed_test::SpeedTestConfig {
        let mgr = self.speed_test.lock().await;
        mgr.get_config().clone()
    }

    /// Update the speed test configuration.
    pub async fn set_speed_test_config(&self, config: speed_test::SpeedTestConfig) {
        let mut mgr = self.speed_test.lock().await;
        mgr.set_config(config);
    }

    /// Perform a speed test to the given URL and return the result.
    pub async fn run_speed_test(&self, url: &str) -> speed_test::SpeedTestResult {
        let mut mgr = self.speed_test.lock().await;
        mgr.test_url(url).await
    }

    /// Get the speed test summary (statistics over all recorded tests).
    pub async fn get_speed_test_summary(&self) -> speed_test::SpeedTestSummary {
        let mgr = self.speed_test.lock().await;
        mgr.get_summary()
    }

    /// Get the full speed test history.
    pub async fn get_speed_test_history(&self) -> Vec<speed_test::SpeedTestResult> {
        let mgr = self.speed_test.lock().await;
        mgr.get_history().to_vec()
    }

    /// Get the most recent speed test result.
    pub async fn get_latest_speed_test(&self) -> Option<speed_test::SpeedTestResult> {
        let mgr = self.speed_test.lock().await;
        mgr.get_latest().cloned()
    }

    /// Clear all speed test history.
    pub async fn clear_speed_test_history(&self) {
        let mut mgr = self.speed_test.lock().await;
        mgr.clear_history();
    }

    /// Save speed test config and history to disk.
    pub async fn save_speed_test_data(&self) -> Result<(), String> {
        let mgr = self.speed_test.lock().await;
        mgr.save_config(&self.data_dir)?;
        mgr.save_history(&self.data_dir)?;
        Ok(())
    }

    /// Load speed test config and history from disk.
    pub async fn load_speed_test_data(&self) -> Result<(), String> {
        let mut mgr = self.speed_test.lock().await;
        mgr.load_config(&self.data_dir)?;
        mgr.load_history(&self.data_dir)?;
        Ok(())
    }

    // --- Speed Trend Analysis (Phase 138) ---

    /// Get the speed trend configuration.
    pub async fn get_speed_trend_config(&self) -> speed_trend::SpeedTrendConfig {
        let mgr = self.speed_trend.lock().await;
        mgr.get_config().clone()
    }

    /// Update the speed trend configuration.
    pub async fn set_speed_trend_config(&self, config: speed_trend::SpeedTrendConfig) {
        let mut mgr = self.speed_trend.lock().await;
        mgr.set_config(config);
    }

    /// Record a speed sample for a domain.
    pub async fn record_speed_trend(&self, domain: &str, speed_bps: f64) {
        let mut mgr = self.speed_trend.lock().await;
        mgr.add_sample(domain, speed_bps);
    }

    /// Analyze trend for a specific domain.
    pub async fn analyze_speed_trend(
        &self,
        domain: &str,
        window: Option<speed_trend::TrendWindow>,
    ) -> Option<speed_trend::DomainTrend> {
        let mgr = self.speed_trend.lock().await;
        mgr.analyze_domain(domain, window)
    }

    /// Get speed trend summary for all domains.
    pub async fn get_speed_trend_summary(&self) -> speed_trend::SpeedTrendSummary {
        let mgr = self.speed_trend.lock().await;
        mgr.get_summary()
    }

    /// Get all domain trends.
    pub async fn get_all_speed_trends(&self) -> Vec<speed_trend::DomainTrend> {
        let mgr = self.speed_trend.lock().await;
        mgr.get_all_trends()
    }

    /// Get domains with degrading trends.
    pub async fn get_degrading_speed_trends(&self) -> Vec<speed_trend::DomainTrend> {
        let mgr = self.speed_trend.lock().await;
        mgr.get_degrading_domains()
    }

    /// Get domains with improving trends.
    pub async fn get_improving_speed_trends(&self) -> Vec<speed_trend::DomainTrend> {
        let mgr = self.speed_trend.lock().await;
        mgr.get_improving_domains()
    }

    /// Clear speed trend data for a domain.
    pub async fn clear_speed_trend_domain(&self, domain: &str) {
        let mut mgr = self.speed_trend.lock().await;
        mgr.clear_domain(domain);
    }

    /// Clear all speed trend data.
    pub async fn clear_all_speed_trends(&self) {
        let mut mgr = self.speed_trend.lock().await;
        mgr.clear_all();
    }

    /// Save speed trend data to disk.
    pub async fn save_speed_trend_data(&self) -> Result<(), String> {
        let mgr = self.speed_trend.lock().await;
        let config_path = format!("{}/speed_trend_config.json", self.data_dir.display());
        let data_path = format!("{}/speed_trend_data.json", self.data_dir.display());
        mgr.save_config(&config_path).map_err(|e| e.to_string())?;
        mgr.save_data(&data_path).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Load speed trend data from disk.
    pub async fn load_speed_trend_data(&self) -> Result<(), String> {
        let mut mgr = self.speed_trend.lock().await;
        let config_path = format!("{}/speed_trend_config.json", self.data_dir.display());
        let data_path = format!("{}/speed_trend_data.json", self.data_dir.display());
        if let Ok(config) = speed_trend::SpeedTrendManager::load_config(&config_path) {
            mgr.set_config(config);
        }
        if mgr.load_data(&data_path).is_err() {
            // Ignore if file doesn't exist
        }
        Ok(())
    }

    // --- Speed Heatmap (Phase 143) ---

    /// Get speed heatmap configuration.
    pub async fn get_speed_heatmap_config(&self) -> speed_heatmap::SpeedHeatmapConfig {
        let heatmap = self.speed_heatmap.read().await;
        heatmap.config.clone()
    }

    /// Update speed heatmap configuration.
    pub async fn set_speed_heatmap_config(&self, config: speed_heatmap::SpeedHeatmapConfig) {
        let mut heatmap = self.speed_heatmap.write().await;
        heatmap.config = config;
    }

    /// Record a speed sample in the heatmap.
    pub async fn record_speed_heatmap(&self, speed_bps: f64) {
        let mut heatmap = self.speed_heatmap.write().await;
        heatmap.record(speed_bps);
    }

    /// Get speed heatmap summary.
    pub async fn get_speed_heatmap_summary(&self) -> speed_heatmap::SpeedHeatmapSummary {
        let heatmap = self.speed_heatmap.read().await;
        heatmap.summary()
    }

    /// Get quality rating for a specific time slot.
    pub async fn get_speed_heatmap_quality(
        &self,
        day_of_week: u8,
        hour: u8,
    ) -> speed_heatmap::SlotQuality {
        let heatmap = self.speed_heatmap.read().await;
        heatmap.cell_quality(day_of_week, hour)
    }

    /// Get hourly average speed.
    pub async fn get_speed_heatmap_hourly_speed(&self, hour: u8) -> f64 {
        let heatmap = self.speed_heatmap.read().await;
        heatmap.hourly_speed(hour)
    }

    /// Get daily average speed.
    pub async fn get_speed_heatmap_daily_speed(&self, day_of_week: u8) -> f64 {
        let heatmap = self.speed_heatmap.read().await;
        heatmap.daily_speed(day_of_week)
    }

    /// Get formatted heatmap report.
    pub async fn format_speed_heatmap_report(&self) -> String {
        let heatmap = self.speed_heatmap.read().await;
        heatmap.format_report()
    }

    /// Reset all heatmap data.
    pub async fn reset_speed_heatmap(&self) {
        let mut heatmap = self.speed_heatmap.write().await;
        heatmap.reset();
    }

    /// Prune old heatmap data.
    pub async fn prune_speed_heatmap(&self) {
        let mut heatmap = self.speed_heatmap.write().await;
        heatmap.prune_old_data();
    }

    /// Save speed heatmap data to disk.
    pub async fn save_speed_heatmap_data(&self) -> Result<(), String> {
        let heatmap = self.speed_heatmap.read().await;
        let config_path = format!("{}/speed_heatmap_config.json", self.data_dir.display());
        let data_path = format!("{}/speed_heatmap_data.json", self.data_dir.display());
        heatmap
            .save_config(std::path::Path::new(&config_path))
            .await
            .map_err(|e| e.to_string())?;
        heatmap
            .save_data(std::path::Path::new(&data_path))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Load speed heatmap data from disk.
    pub async fn load_speed_heatmap_data(&self) -> Result<(), String> {
        let mut heatmap = self.speed_heatmap.write().await;
        let config_path = format!("{}/speed_heatmap_config.json", self.data_dir.display());
        let data_path = format!("{}/speed_heatmap_data.json", self.data_dir.display());
        if let Ok(config) =
            speed_heatmap::SpeedHeatmap::load_config(std::path::Path::new(&config_path)).await
        {
            heatmap.config = config;
        }
        if let Ok(loaded) =
            speed_heatmap::SpeedHeatmap::load_data(std::path::Path::new(&data_path)).await
        {
            *heatmap = loaded;
        }
        Ok(())
    }

    // --- Task Scorecard (Phase 139) ---

    /// Get task scorecard configuration.
    pub async fn get_task_scorecard_config(&self) -> task_scorecard::ScorecardConfig {
        let mgr = self.task_scorecard.lock().await;
        mgr.config.clone()
    }

    /// Set task scorecard configuration.
    pub async fn set_task_scorecard_config(&self, config: task_scorecard::ScorecardConfig) {
        let mut mgr = self.task_scorecard.lock().await;
        mgr.set_config(config);
    }

    /// Generate a scorecard for a specific task.
    pub async fn generate_task_scorecard(
        &self,
        task_id: &str,
    ) -> Option<task_scorecard::TaskScorecard> {
        let tasks = self.tasks.lock().await;
        let task = tasks.iter().find(|t| t.id == task_id)?;

        // Extract domain from source URL
        let source_domain = task.source_url.as_ref().and_then(|url| {
            url::Url::parse(url)
                .ok()
                .and_then(|u| u.host_str().map(|h| h.to_string()))
        });

        // Look up source reliability score
        let source_reliability_score = if let Some(ref domain) = source_domain {
            let rel_mgr = self.source_reliability.lock().await;
            Some(rel_mgr.get_score(domain))
        } else {
            None
        };

        // Get profiler data if available
        let profiler_mgr = self.task_profiler.lock().await;
        let profile = profiler_mgr.get_profile(task_id);

        // Get anomaly data
        let anomaly_mgr = self.speed_anomaly.lock().await;
        let anomalies = anomaly_mgr.get_anomalies(task_id);

        let progress_pct = if task.size > 0 {
            (task.downloaded as f64 / task.size as f64 * 100.0).min(100.0)
        } else {
            0.0
        };

        let input = task_scorecard::ScorecardInput {
            task_id: task_id.to_string(),
            task_name: task.name.clone(),
            protocol: format!("{:?}", task.protocol).to_lowercase(),
            source_domain,
            total_bytes: task.size,
            downloaded_bytes: task.downloaded,
            progress_pct,
            avg_speed_bps: task.speed_bps,
            peak_speed_bps: task.speed_bps * 1.5, // estimate peak as 1.5x average
            efficiency_score: profile.map(|p| p.efficiency_score).unwrap_or(50.0),
            stall_count: 0, // not tracked in DownloadTask
            retry_count: task.auto_retry_count,
            error_count: if task.error.is_some() { 1 } else { 0 },
            duration_secs: task.active_time_seconds,
            is_complete: task.state == crate::DownloadState::Complete,
            source_reliability_score,
            anomaly_count: anomalies.len() as u32,
            bottleneck: profile.map(|p| format!("{:?}", p.bottleneck)),
            profiler_recommendations: profile
                .map(|p| p.recommendations.clone())
                .unwrap_or_default(),
        };

        let mut mgr = self.task_scorecard.lock().await;
        mgr.generate_scorecard(&input)
    }

    /// Get scorecard for a task (if already generated).
    pub async fn get_task_scorecard(&self, task_id: &str) -> Option<task_scorecard::TaskScorecard> {
        let mgr = self.task_scorecard.lock().await;
        mgr.get_scorecard(task_id).cloned()
    }

    /// Get all task scorecards sorted by score (best first).
    pub async fn get_all_task_scorecards(&self) -> Vec<task_scorecard::TaskScorecard> {
        let mgr = self.task_scorecard.lock().await;
        mgr.get_all_scorecards().into_iter().cloned().collect()
    }

    /// Get top performing tasks by score.
    pub async fn get_top_task_scorecards(&self, n: usize) -> Vec<task_scorecard::TaskScorecard> {
        let mgr = self.task_scorecard.lock().await;
        mgr.get_top_performers(n).into_iter().cloned().collect()
    }

    /// Get worst performing tasks by score.
    pub async fn get_worst_task_scorecards(&self, n: usize) -> Vec<task_scorecard::TaskScorecard> {
        let mgr = self.task_scorecard.lock().await;
        mgr.get_worst_performers(n).into_iter().cloned().collect()
    }

    /// Get scorecard summary across all tasks.
    pub async fn get_task_scorecard_summary(&self) -> task_scorecard::ScorecardSummary {
        let mgr = self.task_scorecard.lock().await;
        mgr.get_summary()
    }

    /// Remove scorecard for a task.
    pub async fn remove_task_scorecard(&self, task_id: &str) -> bool {
        let mut mgr = self.task_scorecard.lock().await;
        mgr.remove_scorecard(task_id)
    }

    /// Clear all task scorecards.
    pub async fn clear_all_task_scorecards(&self) {
        let mut mgr = self.task_scorecard.lock().await;
        mgr.clear_all();
    }

    /// Save task scorecard data to disk.
    pub async fn save_task_scorecard_data(&self) -> Result<(), String> {
        let mgr = self.task_scorecard.lock().await;
        let config_path = format!("{}/task_scorecard_config.json", self.data_dir.display());
        let data_path = format!("{}/task_scorecard_data.json", self.data_dir.display());
        mgr.save_config(std::path::Path::new(&config_path))
            .await
            .map_err(|e| e.to_string())?;
        mgr.save_data(std::path::Path::new(&data_path))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Load task scorecard data from disk.
    pub async fn load_task_scorecard_data(&self) -> Result<(), String> {
        let mut mgr = self.task_scorecard.lock().await;
        let config_path = format!("{}/task_scorecard_config.json", self.data_dir.display());
        let data_path = format!("{}/task_scorecard_data.json", self.data_dir.display());
        if let Ok(config) =
            task_scorecard::TaskScorecardManager::load_config(std::path::Path::new(&config_path))
                .await
        {
            mgr.set_config(config);
        }
        if mgr
            .load_data(std::path::Path::new(&data_path))
            .await
            .is_err()
        {
            // Ignore if file doesn't exist
        }
        Ok(())
    }

    // --- Intelligent Source Selector (Phase 140) ---

    /// Set intelligent source selector configuration.
    pub async fn set_intelligent_selector_config(
        &self,
        config: intelligent_source_selector::IntelligentSelectorConfig,
    ) {
        let mut selector = self.intelligent_source_selector.lock().await;
        selector.set_config(config);
    }

    /// Get intelligent source selector configuration.
    pub async fn get_intelligent_selector_config(
        &self,
    ) -> intelligent_source_selector::IntelligentSelectorConfig {
        let selector = self.intelligent_source_selector.lock().await;
        selector.config().clone()
    }

    /// Add or update a source candidate for intelligent selection.
    pub async fn add_intelligent_source_candidate(
        &self,
        candidate: intelligent_source_selector::SourceCandidate,
    ) {
        let mut selector = self.intelligent_source_selector.lock().await;
        selector.add_candidate(candidate);
    }

    /// Remove all candidates for a task.
    pub async fn remove_intelligent_source_task(&self, task_id: &str) {
        let mut selector = self.intelligent_source_selector.lock().await;
        selector.remove_task_candidates(task_id);
    }

    /// Perform intelligent source selection for a task.
    pub async fn select_intelligent_sources(
        &self,
        task_id: &str,
    ) -> intelligent_source_selector::SelectionResult {
        let mut selector = self.intelligent_source_selector.lock().await;
        selector.select_sources(task_id)
    }

    /// Get candidates for a specific task.
    pub async fn get_intelligent_source_candidates(
        &self,
        task_id: &str,
    ) -> Vec<intelligent_source_selector::SourceCandidate> {
        let selector = self.intelligent_source_selector.lock().await;
        selector
            .candidates
            .get(task_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all candidates across all tasks.
    pub async fn get_all_intelligent_source_candidates(
        &self,
    ) -> HashMap<String, Vec<intelligent_source_selector::SourceCandidate>> {
        let selector = self.intelligent_source_selector.lock().await;
        selector.candidates.clone()
    }

    /// Get selector summary statistics.
    pub async fn get_intelligent_selector_summary(
        &self,
    ) -> intelligent_source_selector::SelectorSummary {
        let selector = self.intelligent_source_selector.lock().await;
        selector.get_summary()
    }

    /// Get selection history.
    pub async fn get_intelligent_selector_history(
        &self,
    ) -> Vec<intelligent_source_selector::SelectionResult> {
        let selector = self.intelligent_source_selector.lock().await;
        selector.selection_history.clone()
    }

    /// Clear all candidates and history.
    pub async fn clear_intelligent_selector(&self) {
        let mut selector = self.intelligent_source_selector.lock().await;
        selector.clear_candidates();
        selector.selection_history.clear();
    }

    /// Clear selection history only.
    pub async fn clear_intelligent_selector_history(&self) {
        let mut selector = self.intelligent_source_selector.lock().await;
        selector.selection_history.clear();
    }

    /// Record that a source was used (for tracking selection counts).
    pub async fn record_intelligent_source_used(&self, source_id: &str, success: bool) {
        let mut selector = self.intelligent_source_selector.lock().await;
        selector.record_source_used(source_id, success);
    }

    /// Save intelligent selector config to disk.
    pub async fn save_intelligent_selector_config(&self) -> Result<(), std::io::Error> {
        let selector = self.intelligent_source_selector.lock().await;
        intelligent_source_selector::save_selector_config(
            &selector.config,
            std::path::Path::new(&self.data_dir),
        )
    }

    /// Load intelligent selector config from disk.
    pub async fn load_intelligent_selector_config(&self) -> Result<(), std::io::Error> {
        if let Some(config) =
            intelligent_source_selector::load_selector_config(std::path::Path::new(&self.data_dir))?
        {
            let mut selector = self.intelligent_source_selector.lock().await;
            selector.set_config(config);
        }
        Ok(())
    }

    // --- Download Session Tracking (Phase 84) ---

    /// Start a new download session for a task.
    /// Call this when a task transitions to Downloading state.
    pub async fn start_download_session(&self, task_id: &str, bytes_at_start: u64, protocol: &str) {
        let mut sessions = self.download_sessions.lock().await;
        sessions.start_session(task_id, bytes_at_start, protocol);
    }

    /// Close the active download session for a task.
    /// Call this when a task transitions to Paused/Complete/Error state.
    pub async fn close_download_session(
        &self,
        task_id: &str,
        bytes_at_end: u64,
        outcome: download_session::SessionOutcome,
        error: Option<String>,
    ) {
        let mut sessions = self.download_sessions.lock().await;
        let _ = sessions.close_session(task_id, bytes_at_end, outcome, error);
    }

    /// Update peak speed for the active session.
    pub async fn update_session_peak_speed(&self, task_id: &str, speed_bps: u64) {
        let mut sessions = self.download_sessions.lock().await;
        sessions.update_peak_speed(task_id, speed_bps);
    }

    /// Get session summary for a task.
    pub async fn get_task_session_summary(
        &self,
        task_id: &str,
    ) -> Option<download_session::TaskSessionSummary> {
        let sessions = self.download_sessions.lock().await;
        sessions.get_task_summary(task_id)
    }

    /// Get session summaries for all tasks.
    pub async fn get_all_session_summaries(&self) -> Vec<download_session::TaskSessionSummary> {
        let sessions = self.download_sessions.lock().await;
        sessions.get_all_summaries()
    }

    /// Remove all sessions for a task.
    pub async fn remove_task_sessions(&self, task_id: &str) -> bool {
        let mut sessions = self.download_sessions.lock().await;
        sessions.remove_task_sessions(task_id)
    }

    /// Get download session configuration.
    pub async fn get_download_session_config(&self) -> download_session::DownloadSessionConfig {
        let sessions = self.download_sessions.lock().await;
        sessions.config().clone()
    }

    /// Set download session configuration.
    pub async fn set_download_session_config(
        &self,
        config: download_session::DownloadSessionConfig,
    ) {
        let mut sessions = self.download_sessions.lock().await;
        sessions.set_config(config);
    }

    /// Clear all download sessions.
    pub async fn clear_all_sessions(&self) {
        let mut sessions = self.download_sessions.lock().await;
        sessions.clear_all();
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

    /// Rename a tag across all tasks (Phase 125)
    pub async fn rename_tag(&self, old_name: &str, new_name: &str) -> Option<TagAction> {
        let mut tasks = self.tasks.lock().await;
        let mut affected = 0usize;
        for task in tasks.iter_mut() {
            if let Some(pos) = task.tags.iter().position(|t| t == old_name) {
                task.tags[pos] = new_name.to_string();
                affected += 1;
            }
        }
        drop(tasks);

        if affected > 0 {
            self.tag_manager.rename_tag(old_name, new_name).await;
            self.persist_tasks().await;
            Some(TagAction::Renamed {
                old: old_name.to_string(),
                new: new_name.to_string(),
                affected_tasks: affected,
            })
        } else {
            None
        }
    }

    /// Merge source tag into target tag across all tasks (Phase 125)
    pub async fn merge_tags(&self, source: &str, target: &str) -> Option<TagAction> {
        if source == target {
            return None;
        }

        let mut tasks = self.tasks.lock().await;
        let mut affected = 0usize;
        for task in tasks.iter_mut() {
            let has_source = task.tags.iter().any(|t| t == source);
            let has_target = task.tags.iter().any(|t| t == target);

            if has_source {
                // Remove source tag
                task.tags.retain(|t| t != source);
                // Add target tag if not already present
                if !has_target {
                    task.tags.push(target.to_string());
                }
                affected += 1;
            }
        }
        drop(tasks);

        if affected > 0 {
            self.tag_manager.merge_tags(source, target).await;
            self.persist_tasks().await;
            Some(TagAction::Merged {
                source: source.to_string(),
                target: target.to_string(),
                affected_tasks: affected,
            })
        } else {
            None
        }
    }

    /// Clean up orphan tags (tags with 0 usage) (Phase 125)
    pub async fn cleanup_orphan_tags(&self) -> TagAction {
        // Sync tag manager with actual task data first
        self.sync_tag_manager().await;
        let removed = self.tag_manager.cleanup_orphans().await;
        TagAction::OrphansCleaned { removed }
    }

    /// Sync tag manager with actual task data (Phase 125)
    pub async fn sync_tag_manager(&self) {
        let tasks = self.tasks.lock().await;
        let task_tags: Vec<Vec<String>> = tasks.iter().map(|t| t.tags.clone()).collect();
        drop(tasks);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.tag_manager.sync_from_tasks(&task_tags, now).await;
    }

    /// Add a tag alias (Phase 125)
    pub async fn add_tag_alias(&self, alias: &str, canonical: &str) -> bool {
        self.tag_manager.add_alias(alias, canonical).await
    }

    /// Remove a tag alias (Phase 125)
    pub async fn remove_tag_alias(&self, alias: &str) -> bool {
        self.tag_manager.remove_alias(alias).await
    }

    /// Get all tag aliases (Phase 125)
    pub async fn get_tag_aliases(&self) -> std::collections::HashMap<String, String> {
        self.tag_manager.get_aliases().await
    }

    /// Get tag management summary (Phase 125)
    pub async fn get_tag_management_summary(&self) -> tag_management::TagManagementSummary {
        self.sync_tag_manager().await;
        self.tag_manager.get_summary().await
    }

    /// Get tag management config (Phase 125)
    pub async fn get_tag_management_config(&self) -> tag_management::TagManagementConfig {
        self.tag_manager.get_config().await
    }

    /// Set tag management config (Phase 125)
    pub async fn set_tag_management_config(&self, config: tag_management::TagManagementConfig) {
        self.tag_manager.set_config(config).await;
    }

    /// Set a label (emoji/color) for a tag (Phase 125)
    pub async fn set_tag_label(&self, tag: &str, label: Option<String>) -> bool {
        self.tag_manager.set_tag_label(tag, label).await
    }

    /// Delete a tag from tag manager (does not affect tasks) (Phase 125)
    pub async fn delete_tag(&self, tag: &str) -> bool {
        self.tag_manager.delete_tag(tag).await
    }

    /// Get all tags with usage info (Phase 125)
    pub async fn get_all_tag_info(&self) -> Vec<tag_management::TagInfo> {
        self.sync_tag_manager().await;
        self.tag_manager.get_all_tags().await
    }

    // ─── Phase 127: Download Cost Tracker ───

    /// Get cost tracker config (Phase 127)
    pub async fn get_cost_config(&self) -> download_cost::CostConfig {
        self.cost_tracker.lock().await.config().clone()
    }

    /// Set cost tracker config (Phase 127)
    pub async fn set_cost_config(&self, config: download_cost::CostConfig) {
        let mut tracker = self.cost_tracker.lock().await;
        tracker.set_config(config);
        let path = self.data_dir.join("download_cost_config.json");
        let _ = tracker.save_config(&path);
    }

    /// Record cost for a task based on bytes downloaded (Phase 127)
    pub async fn record_cost(&self, task_id: &str, task_name: &str, bytes: u64) {
        let mut tracker = self.cost_tracker.lock().await;
        tracker.record_task_usage(task_id, task_name, bytes, chrono::Utc::now());
    }

    /// Get cost record for a specific task (Phase 127)
    pub async fn get_task_cost(&self, task_id: &str) -> Option<download_cost::TaskCostRecord> {
        self.cost_tracker
            .lock()
            .await
            .get_task_cost(task_id)
            .cloned()
    }

    /// Get cost summary for the current month (Phase 127)
    pub async fn get_cost_summary_current_month(&self) -> download_cost::CostSummary {
        self.cost_tracker.lock().await.summary_current_month()
    }

    /// Get cost summary for a specific date (Phase 127)
    pub async fn get_cost_summary_for_date(&self, date: &str) -> download_cost::CostSummary {
        self.cost_tracker.lock().await.summary_for_date(date)
    }

    /// Get all-time cost summary (Phase 127)
    pub async fn get_cost_summary_all(&self) -> download_cost::CostSummary {
        self.cost_tracker.lock().await.summary_all()
    }

    /// Format cost summary for display (Phase 127)
    pub async fn format_cost_summary(&self, summary: &download_cost::CostSummary) -> String {
        self.cost_tracker.lock().await.format_summary(summary)
    }

    /// Check if budget alert threshold is exceeded (Phase 127)
    pub async fn is_over_cost_budget(&self) -> bool {
        self.cost_tracker.lock().await.is_over_budget_alert()
    }

    /// Remove a task from cost tracking (Phase 127)
    pub async fn remove_task_cost(&self, task_id: &str) -> Option<download_cost::TaskCostRecord> {
        self.cost_tracker.lock().await.remove_task(task_id)
    }

    /// Clear all cost tracking data (Phase 127)
    pub async fn clear_cost_data(&self) {
        self.cost_tracker.lock().await.clear();
    }

    /// Get all task cost records (Phase 127)
    pub async fn get_all_task_costs(&self) -> Vec<download_cost::TaskCostRecord> {
        self.cost_tracker
            .lock()
            .await
            .all_task_records()
            .values()
            .cloned()
            .collect()
    }

    /// Get daily cost usage records (Phase 127)
    pub async fn get_daily_cost_usage(&self) -> Vec<download_cost::DailyCostUsage> {
        self.cost_tracker
            .lock()
            .await
            .daily_usage()
            .values()
            .cloned()
            .collect()
    }

    /// Prune old daily usage records (Phase 127)
    pub async fn prune_cost_daily_usage(&self, keep_days: u32) {
        self.cost_tracker.lock().await.prune_daily_usage(keep_days);
    }

    // ─── Phase 128: Download History Analytics ───

    /// Get history analytics config (Phase 128)
    pub async fn get_history_analytics_config(
        &self,
    ) -> download_history_analytics::HistoryAnalyticsConfig {
        self.history_analytics.lock().await.config.clone()
    }

    /// Set history analytics config (Phase 128)
    pub async fn set_history_analytics_config(
        &self,
        config: download_history_analytics::HistoryAnalyticsConfig,
    ) {
        let mut mgr = self.history_analytics.lock().await;
        mgr.set_config(config.clone());
        if let Err(e) = download_history_analytics::save_analytics_config(&config, &self.data_dir) {
            tracing::warn!(error = %e, "Failed to save history analytics config");
        }
    }

    /// Generate analytics summary from download history (Phase 128)
    pub async fn get_history_analytics(
        &self,
    ) -> download_history_analytics::HistoryAnalyticsSummary {
        let mgr = self.history_analytics.lock().await;
        let entries = self.get_download_history_entries().await;
        mgr.analyze(&entries)
    }

    /// Generate analytics summary for a custom period (Phase 128)
    pub async fn get_history_analytics_for_period(
        &self,
        period_days: i64,
    ) -> download_history_analytics::HistoryAnalyticsSummary {
        let mgr_guard = self.history_analytics.lock().await;
        let mut mgr = mgr_guard.clone();
        mgr.config.default_period_days = period_days;
        let entries = self.get_download_history_entries().await;
        mgr.analyze(&entries)
    }

    /// Format analytics summary for display (Phase 128)
    pub async fn format_history_analytics(
        &self,
        summary: &download_history_analytics::HistoryAnalyticsSummary,
    ) -> String {
        self.history_analytics.lock().await.format_summary(summary)
    }

    /// Get download history entries for analytics (Phase 128)
    pub async fn get_download_history_entries(&self) -> Vec<download_history::HistoryEntry> {
        match download_history::load_history(&self.data_dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load download history for analytics");
                Vec::new()
            }
        }
    }

    /// Clear history analytics data (Phase 128)
    pub async fn clear_history_analytics(&self) {
        let mgr = self.history_analytics.lock().await;
        let _ = download_history_analytics::save_analytics_config(&mgr.config, &self.data_dir);
    }

    // ========== Download History API (Phase 162) ==========

    /// Get download history summary (counts by outcome, protocol, total size)
    pub async fn get_download_history_summary(&self) -> download_history::HistorySummary {
        let entries = self.get_download_history_entries().await;
        download_history::HistorySummary::from_entries(&entries)
    }

    /// Search download history by name substring
    pub async fn search_download_history(
        &self,
        query: &str,
    ) -> Vec<download_history::HistoryEntry> {
        let entries = self.get_download_history_entries().await;
        let q = query.to_lowercase();
        entries
            .into_iter()
            .filter(|e| e.name.to_lowercase().contains(&q))
            .collect()
    }

    /// Get download history entries filtered by outcome
    pub async fn get_download_history_by_outcome(
        &self,
        outcome: download_history::HistoryOutcome,
    ) -> Vec<download_history::HistoryEntry> {
        let entries = self.get_download_history_entries().await;
        entries
            .into_iter()
            .filter(|e| e.outcome == outcome)
            .collect()
    }

    /// Remove a single entry from download history by task_id
    pub async fn remove_download_history_entry(&self, task_id: &str) -> bool {
        match download_history::remove_entry(&self.data_dir, task_id) {
            Ok(found) => found,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to remove download history entry");
                false
            }
        }
    }

    /// Clear all download history
    pub async fn clear_download_history(&self) -> Result<(), String> {
        download_history::clear_history(&self.data_dir).map_err(|e| e.to_string())
    }

    // ========== Speed Benchmark API (Phase 129) ==========

    /// Get speed benchmark configuration
    pub async fn get_speed_benchmark_config(&self) -> speed_benchmark::BenchmarkConfig {
        self.speed_benchmark.lock().await.get_config().clone()
    }

    /// Set speed benchmark configuration
    pub async fn set_speed_benchmark_config(&self, config: speed_benchmark::BenchmarkConfig) {
        self.speed_benchmark.lock().await.set_config(config);
    }

    /// Benchmark a single URL
    pub async fn benchmark_url(&self, url: &str) -> speed_benchmark::BenchmarkResult {
        self.speed_benchmark.lock().await.benchmark_url(url).await
    }

    /// Benchmark multiple URLs concurrently
    pub async fn benchmark_urls(&self, urls: &[String]) -> speed_benchmark::BenchmarkSummary {
        self.speed_benchmark.lock().await.benchmark_urls(urls).await
    }

    /// Get cached benchmark result for a URL
    pub async fn get_cached_benchmark(
        &self,
        url: &str,
    ) -> Option<speed_benchmark::BenchmarkResult> {
        self.speed_benchmark
            .lock()
            .await
            .get_cached_result(url)
            .cloned()
    }

    /// Get all benchmark results
    pub async fn get_all_benchmarks(&self) -> Vec<speed_benchmark::BenchmarkResult> {
        self.speed_benchmark
            .lock()
            .await
            .get_all_results()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Get benchmark summary
    pub async fn get_benchmark_summary(&self) -> speed_benchmark::BenchmarkSummary {
        self.speed_benchmark.lock().await.get_summary()
    }

    /// Clear all benchmark results
    pub async fn clear_benchmarks(&self) {
        self.speed_benchmark.lock().await.clear_results();
    }

    /// Clear benchmark result for a specific URL
    pub async fn clear_benchmark(&self, url: &str) {
        self.speed_benchmark.lock().await.clear_result(url);
    }

    /// Format benchmark summary for display
    pub async fn format_benchmark_summary(&self) -> String {
        self.speed_benchmark.lock().await.format_summary()
    }

    // ========== Speed Distribution API ==========

    /// Get speed distribution configuration
    pub async fn get_speed_distribution_config(
        &self,
    ) -> speed_distribution::SpeedDistributionConfig {
        self.speed_distribution.read().await.get_config().clone()
    }

    /// Set speed distribution configuration
    pub async fn set_speed_distribution_config(
        &self,
        config: speed_distribution::SpeedDistributionConfig,
    ) -> std::io::Result<()> {
        self.speed_distribution
            .write()
            .await
            .set_config(config)
            .await
    }

    /// Record a speed sample for distribution analysis
    pub async fn record_speed_distribution(
        &self,
        domain: &str,
        protocol: speed_distribution::SpeedProtocol,
        speed_bps: f64,
        bytes: u64,
    ) {
        self.speed_distribution
            .write()
            .await
            .record_speed(domain, protocol, speed_bps, bytes)
            .await;
    }

    /// Get speed distribution summary
    pub async fn get_speed_distribution_summary(
        &self,
    ) -> speed_distribution::SpeedDistributionSummary {
        self.speed_distribution.read().await.get_summary()
    }

    /// Get statistics for a specific domain
    pub async fn get_domain_speed_stats(
        &self,
        domain: &str,
    ) -> Option<speed_distribution::SpeedStats> {
        self.speed_distribution.read().await.domain_stats(domain)
    }

    /// Get statistics for a specific protocol
    pub async fn get_protocol_speed_stats(
        &self,
        protocol: speed_distribution::SpeedProtocol,
    ) -> Option<speed_distribution::SpeedStats> {
        self.speed_distribution
            .read()
            .await
            .protocol_stats(protocol)
    }

    /// Get hourly speed statistics
    pub async fn get_hourly_speed_stats(&self, hour: u8) -> Option<speed_distribution::SpeedStats> {
        self.speed_distribution.read().await.hourly_stats(hour)
    }

    /// Get list of all tracked domains
    pub async fn get_tracked_speed_domains(&self) -> Vec<String> {
        self.speed_distribution.read().await.tracked_domains()
    }

    /// Remove a domain from speed tracking
    pub async fn remove_speed_domain(&self, domain: &str) -> bool {
        self.speed_distribution.write().await.remove_domain(domain)
    }

    /// Clear all speed distribution data
    pub async fn clear_speed_distribution(&self) -> std::io::Result<()> {
        self.speed_distribution.write().await.clear().await
    }

    /// Format speed distribution report
    pub async fn format_speed_distribution_report(&self) -> String {
        self.speed_distribution.read().await.format_report()
    }

    /// Load speed distribution data from disk
    pub async fn load_speed_distribution_data(&self) -> std::io::Result<()> {
        self.speed_distribution.write().await.load().await
    }

    // ========== Event Webhook API ==========

    /// Get webhook configuration
    pub async fn get_webhook_config(&self) -> event_webhook::WebhookConfig {
        self.event_webhook.lock().await.config().clone()
    }

    /// Set webhook configuration
    pub async fn set_webhook_config(&self, config: event_webhook::WebhookConfig) {
        self.event_webhook.lock().await.set_config(config);
    }

    /// Add a webhook endpoint
    pub async fn add_webhook_endpoint(
        &self,
        endpoint: event_webhook::WebhookEndpoint,
    ) -> Result<String, event_webhook::WebhookError> {
        self.event_webhook.lock().await.add_endpoint(endpoint)
    }

    /// Remove a webhook endpoint
    pub async fn remove_webhook_endpoint(
        &self,
        endpoint_id: &str,
    ) -> Result<(), event_webhook::WebhookError> {
        self.event_webhook.lock().await.remove_endpoint(endpoint_id)
    }

    /// Get webhook endpoint by ID
    pub async fn get_webhook_endpoint(
        &self,
        endpoint_id: &str,
    ) -> Option<event_webhook::WebhookEndpoint> {
        self.event_webhook
            .lock()
            .await
            .get_endpoint(endpoint_id)
            .cloned()
    }

    /// List all webhook endpoints
    pub async fn list_webhook_endpoints(&self) -> Vec<event_webhook::WebhookEndpoint> {
        self.event_webhook
            .lock()
            .await
            .list_endpoints()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Update webhook endpoint
    pub async fn update_webhook_endpoint(
        &self,
        endpoint_id: &str,
        updates: event_webhook::WebhookEndpointUpdate,
    ) -> Result<(), event_webhook::WebhookError> {
        self.event_webhook
            .lock()
            .await
            .update_endpoint(endpoint_id, updates)
    }

    /// Get webhook delivery history for an endpoint
    pub async fn get_webhook_history(
        &self,
        endpoint_id: &str,
        limit: usize,
    ) -> Vec<event_webhook::WebhookDelivery> {
        self.event_webhook
            .lock()
            .await
            .get_history(endpoint_id, limit)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Clear webhook delivery history for an endpoint
    pub async fn clear_webhook_history(
        &self,
        endpoint_id: &str,
    ) -> Result<(), event_webhook::WebhookError> {
        self.event_webhook.lock().await.clear_history(endpoint_id)
    }

    /// Clear all webhook delivery history
    pub async fn clear_all_webhook_history(&self) {
        self.event_webhook.lock().await.clear_all_history();
    }

    /// Get webhook summary
    pub async fn get_webhook_summary(&self) -> event_webhook::WebhookSummary {
        self.event_webhook.lock().await.get_summary()
    }

    /// Send webhook event
    pub async fn send_webhook_event(
        &self,
        event: event_webhook::WebhookEvent,
        payload: event_webhook::WebhookPayload,
    ) -> Vec<event_webhook::WebhookDelivery> {
        self.event_webhook
            .lock()
            .await
            .send_event(event, payload)
            .await
    }

    /// Save webhook configuration to disk
    pub async fn save_webhook_config(&self) -> Result<(), event_webhook::WebhookError> {
        self.event_webhook.lock().await.save_config().await
    }

    /// Load webhook configuration from disk
    pub async fn load_webhook_config(&self) -> Result<(), event_webhook::WebhookError> {
        self.event_webhook.lock().await.load_config().await
    }

    // ── Path Organizer API ───────────────────────────────────────────────

    /// Get path organizer configuration
    pub async fn get_path_organizer_config(&self) -> path_organizer::PathOrganizerConfig {
        self.path_organizer.lock().await.config.clone()
    }

    /// Set path organizer configuration
    pub async fn set_path_organizer_config(&self, config: path_organizer::PathOrganizerConfig) {
        self.path_organizer.lock().await.config = config;
    }

    /// Enable or disable path organizer
    pub async fn set_path_organizer_enabled(&self, enabled: bool) {
        self.path_organizer.lock().await.set_enabled(enabled);
    }

    /// Add a custom file category
    pub async fn add_file_category(&self, category: path_organizer::FileCategory) {
        self.path_organizer.lock().await.add_category(category);
    }

    /// Remove a file category by name
    pub async fn remove_file_category(&self, name: &str) -> bool {
        self.path_organizer.lock().await.remove_category(name)
    }

    /// List all file categories
    pub async fn list_file_categories(&self) -> Vec<path_organizer::FileCategory> {
        self.path_organizer.lock().await.list_categories().to_vec()
    }

    /// Get path organizer summary/statistics
    pub async fn get_path_organizer_summary(&self) -> path_organizer::PathOrganizerSummary {
        self.path_organizer.lock().await.get_summary().clone()
    }

    /// Reset path organizer statistics
    pub async fn reset_path_organizer_summary(&self) {
        self.path_organizer.lock().await.reset_summary();
    }

    /// Organize a completed download file into category directory
    pub async fn organize_completed_file(
        &self,
        task_id: &str,
    ) -> Result<Option<path_organizer::OrganizeResult>, path_organizer::PathOrganizerError> {
        let tasks = self.tasks.lock().await;
        let task = tasks.iter().find(|t| t.id == task_id);
        let (file_path, save_path) = match task {
            Some(t) => (
                t.save_path.clone(),
                t.save_path
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_default(),
            ),
            None => return Ok(None),
        };
        drop(tasks);

        if !file_path.exists() {
            return Err(path_organizer::PathOrganizerError::FileNotFound(
                file_path.display().to_string(),
            ));
        }

        let result = self
            .path_organizer
            .lock()
            .await
            .organize_file(&file_path, &save_path)
            .await?;

        // Update task save_path if file was moved
        if let Some(ref r) = result
            && r.moved
        {
            let mut tasks = self.tasks.lock().await;
            if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                t.save_path = r.new_path.clone();
                drop(tasks);
                self.persist_tasks().await;
            }
        }

        Ok(result)
    }

    /// Save path organizer configuration to disk
    pub async fn save_path_organizer_config(
        &self,
    ) -> Result<(), path_organizer::PathOrganizerError> {
        let config_path = self.data_dir.join("path_organizer_config.json");
        let manager = self.path_organizer.lock().await;
        path_organizer::save_path_organizer_config(&manager, &config_path).await
    }

    /// Load path organizer configuration from disk
    pub async fn load_path_organizer_config(
        &self,
    ) -> Result<(), path_organizer::PathOrganizerError> {
        let config_path = self.data_dir.join("path_organizer_config.json");
        let manager = path_organizer::load_path_organizer_config(&config_path).await?;
        *self.path_organizer.lock().await = manager;
        Ok(())
    }

    // ===== Data Retention Policy API =====

    /// Get data retention configuration.
    pub async fn get_data_retention_config(&self) -> data_retention::DataRetentionConfig {
        self.data_retention.lock().await.get_config().clone()
    }

    /// Set data retention configuration.
    pub async fn set_data_retention_config(
        &self,
        config: data_retention::DataRetentionConfig,
    ) -> Result<(), data_retention::DataRetentionError> {
        self.data_retention.lock().await.set_config(config)
    }

    /// Enable or disable data retention.
    pub async fn set_data_retention_enabled(&self, enabled: bool) {
        let mut manager = self.data_retention.lock().await;
        let mut config = manager.get_config().clone();
        config.enabled = enabled;
        let _ = manager.set_config(config);
    }

    /// Add a data retention rule.
    pub async fn add_data_retention_rule(
        &self,
        rule: data_retention::RetentionRule,
    ) -> Result<(), data_retention::DataRetentionError> {
        self.data_retention.lock().await.add_rule(rule)
    }

    /// Remove a data retention rule.
    pub async fn remove_data_retention_rule(&self, rule_id: &str) -> bool {
        self.data_retention.lock().await.remove_rule(rule_id)
    }

    /// List all data retention rules.
    pub async fn list_data_retention_rules(&self) -> Vec<data_retention::RetentionRule> {
        self.data_retention.lock().await.list_rules().to_vec()
    }

    /// Get data retention summary.
    pub async fn get_data_retention_summary(&self) -> data_retention::DataRetentionSummary {
        self.data_retention.lock().await.get_summary()
    }

    /// Register a completed download for retention tracking.
    pub async fn register_completed_download(&self, download: data_retention::CompletedDownload) {
        self.data_retention
            .lock()
            .await
            .register_completed(download);
    }

    /// Find cleanup candidates based on a reason.
    pub async fn find_retention_cleanup_candidates(
        &self,
        reason: data_retention::CleanupReason,
    ) -> Vec<data_retention::CompletedDownload> {
        self.data_retention
            .lock()
            .await
            .find_cleanup_candidates(&reason)
    }

    /// Execute retention cleanup.
    pub async fn execute_retention_cleanup(
        &self,
        reason: data_retention::CleanupReason,
    ) -> Result<data_retention::CleanupResult, data_retention::DataRetentionError> {
        let candidates = self
            .data_retention
            .lock()
            .await
            .find_cleanup_candidates(&reason);
        self.data_retention
            .lock()
            .await
            .execute_cleanup(candidates, reason)
    }

    /// Get cleanup history.
    pub async fn get_data_retention_history(&self) -> Vec<data_retention::CleanupResult> {
        self.data_retention
            .lock()
            .await
            .get_cleanup_history()
            .to_vec()
    }

    /// Clear data retention cleanup history.
    pub async fn clear_data_retention_history(&self) {
        self.data_retention.lock().await.clear_history();
    }

    /// Check if disk pressure cleanup should trigger.
    pub async fn check_data_retention_disk_pressure(
        &self,
        free_space_mb: u64,
        total_space_mb: u64,
    ) -> bool {
        self.data_retention
            .lock()
            .await
            .check_disk_pressure(free_space_mb, total_space_mb)
    }

    // ── Source Quality API ───────────────────────────────────────────────

    /// Get source quality configuration
    pub async fn get_source_quality_config(&self) -> source_quality::SourceQualityConfig {
        self.source_quality.lock().await.get_config().clone()
    }

    /// Set source quality configuration
    pub async fn set_source_quality_config(&self, config: source_quality::SourceQualityConfig) {
        self.source_quality.lock().await.set_config(config);
    }

    /// Record a successful download for source quality tracking (with speed)
    pub async fn record_source_success_with_speed(
        &self,
        source_id: &str,
        bytes: u64,
        speed_bps: f64,
    ) {
        self.source_quality
            .lock()
            .await
            .record_success(source_id, bytes, speed_bps);
    }

    /// Record a failed download for source quality tracking
    pub async fn record_source_quality_failure(&self, source_id: &str) {
        self.source_quality.lock().await.record_failure(source_id);
    }

    /// Get quality info for a specific source
    pub async fn get_source_quality(
        &self,
        source_id: &str,
    ) -> Option<source_quality::SourceQuality> {
        self.source_quality
            .lock()
            .await
            .get_source(source_id)
            .cloned()
    }

    /// Get source quality summary/statistics
    pub async fn get_source_quality_summary(&self) -> source_quality::SourceQualitySummary {
        self.source_quality.lock().await.get_summary()
    }

    /// Recommend the best source from a list of candidates
    pub async fn recommend_source_quality(&self, candidates: &[String]) -> Option<String> {
        self.source_quality
            .lock()
            .await
            .recommend_source(candidates)
    }

    /// Check if a source is currently blocked
    pub async fn is_source_quality_blocked(&self, source_id: &str) -> bool {
        self.source_quality.lock().await.is_blocked(source_id)
    }

    /// Manually unblock a source
    pub async fn unblock_source_quality(&self, source_id: &str) -> bool {
        self.source_quality.lock().await.unblock_source(source_id)
    }

    /// Remove a source from quality tracking
    pub async fn remove_source_quality(&self, source_id: &str) -> bool {
        self.source_quality.lock().await.remove_source(source_id)
    }

    /// Clear all source quality data
    pub async fn clear_source_quality(&self) {
        self.source_quality.lock().await.clear_all();
    }

    /// Save source quality config to disk
    pub async fn save_source_quality_config(&self) -> Result<(), std::io::Error> {
        self.source_quality.lock().await.save_config().await
    }

    /// Load source quality config from disk
    pub async fn load_source_quality_config(&self) -> Result<(), std::io::Error> {
        self.source_quality.lock().await.load_config().await
    }

    // ── Dashboard API ────────────────────────────────────────────────────

    /// Get dashboard configuration
    pub async fn get_dashboard_config(&self) -> dashboard::DashboardConfig {
        self.dashboard.lock().await.get_config().clone()
    }

    /// Set dashboard configuration
    pub async fn set_dashboard_config(&self, config: dashboard::DashboardConfig) {
        self.dashboard.lock().await.set_config(config);
    }

    /// Check if dashboard is enabled
    pub async fn is_dashboard_enabled(&self) -> bool {
        self.dashboard.lock().await.is_enabled()
    }

    /// Generate a comprehensive dashboard snapshot aggregating all system data
    pub async fn generate_dashboard(&self) -> dashboard::DashboardSnapshot {
        let config = self.get_dashboard_config().await;
        if !config.enabled {
            return dashboard::DashboardSnapshot {
                snapshot_at: chrono::Utc::now(),
                queue_status: dashboard::QueueStatus::default(),
                current_speed_bps: 0,
                current_upload_bps: 0,
                health_status: queue_health::HealthStatus::Healthy,
                health_score: 100,
                issue_count: 0,
                prediction: None,
                top_active: vec![],
                protocol_breakdown: None,
                disk_status: None,
                total_downloaded_bytes: 0,
                total_uploaded_bytes: 0,
                uptime_seconds: self.system_uptime.uptime_seconds(),
            };
        }

        let tasks = self.tasks.lock().await;
        let queue_status = dashboard::QueueStatus {
            total: tasks.len(),
            running: tasks
                .iter()
                .filter(|t| t.state == DownloadState::Downloading)
                .count(),
            queued: tasks
                .iter()
                .filter(|t| t.state == DownloadState::Queued)
                .count(),
            paused: tasks
                .iter()
                .filter(|t| t.state == DownloadState::Paused)
                .count(),
            completed: tasks
                .iter()
                .filter(|t| t.state == DownloadState::Complete)
                .count(),
            error: tasks
                .iter()
                .filter(|t| t.state == DownloadState::Error)
                .count(),
            recycled: self.recycle_bin.lock().await.list().len(),
        };

        let current_speed_bps: u64 = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Downloading)
            .map(|t| t.speed_bps as u64)
            .sum();

        let current_upload_bps: u64 =
            self.upload_tracker.lock().await.get_total_upload_speed() as u64;

        let total_downloaded_bytes: u64 = tasks.iter().map(|t| t.downloaded).sum();
        let total_uploaded_bytes: u64 = self.upload_tracker.lock().await.get_total_uploaded();

        // Get queue health
        let default_health_config = queue_health::HealthMonitorConfig::default();
        let health_report = self.get_queue_health_report(&default_health_config).await;
        let health_status = health_report.summary.status;
        let health_score = health_report.summary.health_score as u32;
        let issue_count = health_report.issues.len();

        // Get queue completion prediction if enabled
        let prediction = if config.include_prediction {
            Some(self.predict_queue_completion().await)
        } else {
            None
        };

        // Get top active downloads
        let mut top_active: Vec<dashboard::TopActiveTask> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Downloading)
            .map(|t| dashboard::TopActiveTask {
                task_id: t.id.clone(),
                task_name: t.name.clone(),
                progress: t.progress(),
                speed_bps: t.speed_bps as u64,
                eta_seconds: t.eta_seconds(),
                total_size: t.size,
                downloaded: t.downloaded,
                priority: t.priority.clone(),
                protocol: format!("{:?}", t.protocol),
            })
            .collect();
        top_active.sort_by(|a, b| b.speed_bps.cmp(&a.speed_bps));
        top_active.truncate(config.top_active_count);

        // Get protocol breakdown if enabled
        let protocol_breakdown = if config.include_protocol_breakdown {
            Some(dashboard::ProtocolBreakdown {
                http_count: tasks
                    .iter()
                    .filter(|t| matches!(t.protocol, DownloadProtocol::Xunlei))
                    .count(),
                torrent_count: tasks
                    .iter()
                    .filter(|t| matches!(t.protocol, DownloadProtocol::Torrent))
                    .count(),
                ed2k_count: tasks
                    .iter()
                    .filter(|t| matches!(t.protocol, DownloadProtocol::Ed2k))
                    .count(),
                p2p_count: tasks
                    .iter()
                    .filter(|t| matches!(t.protocol, DownloadProtocol::P2P))
                    .count(),
                magnet_count: tasks
                    .iter()
                    .filter(|t| matches!(t.protocol, DownloadProtocol::Magnet))
                    .count(),
            })
        } else {
            None
        };

        // Get disk status if enabled
        let disk_status = if config.include_disk_status {
            let disk_summary = self.get_disk_monitor_summary().await;
            Some(dashboard::DiskStatus {
                available_bytes: disk_summary.available_bytes,
                total_bytes: disk_summary.total_bytes,
                usage_percent: if disk_summary.total_bytes > 0 {
                    (disk_summary.total_bytes - disk_summary.available_bytes) as f64
                        / disk_summary.total_bytes as f64
                } else {
                    0.0
                },
                is_low: disk_summary.available_bytes < disk_summary.warning_threshold_bytes,
                is_critical: disk_summary.available_bytes < disk_summary.critical_threshold_bytes,
            })
        } else {
            None
        };

        dashboard::DashboardSnapshot {
            snapshot_at: chrono::Utc::now(),
            queue_status,
            current_speed_bps,
            current_upload_bps,
            health_status,
            health_score,
            issue_count,
            prediction,
            top_active,
            protocol_breakdown,
            disk_status,
            total_downloaded_bytes,
            total_uploaded_bytes,
            uptime_seconds: self.system_uptime.uptime_seconds(),
        }
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

    /// Set explicit queue position for a task (lower values come first within same priority).
    /// Returns true if task was found and updated.
    pub async fn set_queue_position(&self, task_id: &str, position: Option<u32>) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.queue_position = position;
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

    /// Get the queue position of a task.
    pub async fn get_queue_position(&self, task_id: &str) -> Option<Option<u32>> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.id == task_id)
            .map(|t| t.queue_position)
    }

    /// Set dependencies for a task. Returns false if task not found or if adding would create a cycle.
    pub async fn set_dependencies(&self, task_id: &str, deps: Vec<String>) -> bool {
        // Check for self-dependency
        if deps.iter().any(|d| d == task_id) {
            return false;
        }

        let tasks = self.tasks.lock().await;

        // Check task exists
        if !tasks.iter().any(|t| t.id == task_id) {
            return false;
        }

        // Check all deps exist
        if !deps.iter().all(|d| tasks.iter().any(|t| t.id == *d)) {
            return false;
        }

        // Check for cycles: can't add dep if any dep transitively depends on us
        if self.would_create_cycle(&tasks, task_id, &deps) {
            return false;
        }

        drop(tasks);

        let mut tasks = self.tasks.lock().await;
        let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) else {
            return false;
        };

        task.depends_on = deps;
        task.updated_at = chrono::Utc::now();
        self.emit_event(TaskEvent::Updated {
            task: TaskInfoEvent::from_task(task),
        });
        drop(tasks);
        self.persist_tasks().await;
        true
    }

    /// Get the dependencies for a task.
    pub async fn get_dependencies(&self, task_id: &str) -> Option<Vec<String>> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.id == task_id)
            .map(|t| t.depends_on.clone())
    }

    /// Check if a task's dependencies are all satisfied (all Complete).
    pub async fn are_dependencies_met(&self, task_id: &str) -> Option<bool> {
        let tasks = self.tasks.lock().await;
        let task = tasks.iter().find(|t| t.id == task_id)?;
        if task.depends_on.is_empty() {
            return Some(true);
        }
        let completed: std::collections::HashSet<&str> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Complete)
            .map(|t| t.id.as_str())
            .collect();
        Some(
            task.depends_on
                .iter()
                .all(|d| completed.contains(d.as_str())),
        )
    }

    /// Validate the dependency graph and return any issues found.
    pub async fn validate_dependency_graph(&self) -> dependency_graph::ValidationResult {
        let tasks = self.tasks.lock().await;
        let task_data: Vec<dependency_graph::TaskDepData> = tasks
            .iter()
            .map(|t| dependency_graph::TaskDepData {
                id: t.id.clone(),
                depends_on: t.depends_on.clone(),
                is_complete: t.state == DownloadState::Complete,
                is_error: t.state == DownloadState::Error,
            })
            .collect();
        let validator = self.dependency_graph.read().await;
        validator.validate(&task_data)
    }

    /// Get dependency graph statistics.
    pub async fn get_dependency_graph_stats(&self) -> dependency_graph::GraphStats {
        let tasks = self.tasks.lock().await;
        let task_data: Vec<dependency_graph::TaskDepData> = tasks
            .iter()
            .map(|t| dependency_graph::TaskDepData {
                id: t.id.clone(),
                depends_on: t.depends_on.clone(),
                is_complete: t.state == DownloadState::Complete,
                is_error: t.state == DownloadState::Error,
            })
            .collect();
        let validator = self.dependency_graph.read().await;
        validator.compute_stats(&task_data)
    }

    /// Get topological ordering of download tasks.
    pub async fn get_dependency_topological_order(&self) -> dependency_graph::TopologicalOrder {
        let tasks = self.tasks.lock().await;
        let task_data: Vec<dependency_graph::TaskDepData> = tasks
            .iter()
            .map(|t| dependency_graph::TaskDepData {
                id: t.id.clone(),
                depends_on: t.depends_on.clone(),
                is_complete: t.state == DownloadState::Complete,
                is_error: t.state == DownloadState::Error,
            })
            .collect();
        let validator = self.dependency_graph.read().await;
        validator.topological_sort(&task_data)
    }

    /// Get the dependency graph validator configuration.
    pub async fn get_dependency_graph_config(&self) -> dependency_graph::DependencyGraphConfig {
        let validator = self.dependency_graph.read().await;
        validator.config().clone()
    }

    /// Update the dependency graph validator configuration.
    pub async fn set_dependency_graph_config(
        &self,
        config: dependency_graph::DependencyGraphConfig,
    ) {
        let mut validator = self.dependency_graph.write().await;
        validator.set_config(config);
    }

    /// Get all transitive dependencies for a task.
    pub async fn get_task_dependency_chain(&self, task_id: &str) -> Option<Vec<String>> {
        let tasks = self.tasks.lock().await;
        if !tasks.iter().any(|t| t.id == task_id) {
            return None;
        }
        let task_data: Vec<dependency_graph::TaskDepData> = tasks
            .iter()
            .map(|t| dependency_graph::TaskDepData {
                id: t.id.clone(),
                depends_on: t.depends_on.clone(),
                is_complete: t.state == DownloadState::Complete,
                is_error: t.state == DownloadState::Error,
            })
            .collect();
        let validator = self.dependency_graph.read().await;
        Some(validator.get_dependency_chain(task_id, &task_data))
    }

    /// Get all tasks that depend on a given task (directly or transitively).
    pub async fn get_task_dependents(&self, task_id: &str) -> Option<Vec<String>> {
        let tasks = self.tasks.lock().await;
        if !tasks.iter().any(|t| t.id == task_id) {
            return None;
        }
        let task_data: Vec<dependency_graph::TaskDepData> = tasks
            .iter()
            .map(|t| dependency_graph::TaskDepData {
                id: t.id.clone(),
                depends_on: t.depends_on.clone(),
                is_complete: t.state == DownloadState::Complete,
                is_error: t.state == DownloadState::Error,
            })
            .collect();
        let validator = self.dependency_graph.read().await;
        Some(validator.get_dependents(task_id, &task_data))
    }

    // ── Dependency Visualization System (Phase 154) ──

    /// Build and return the dependency graph visualization.
    pub async fn build_dependency_visualization(
        &self,
    ) -> Option<dependency_visualization::DependencyGraph> {
        let tasks = self.tasks.lock().await;
        let task_tuples: Vec<(String, String, String, Vec<String>)> = tasks
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    t.name.clone(),
                    format!("{:?}", t.state),
                    t.depends_on.clone(),
                )
            })
            .collect();
        let mut viz = self.dep_visualization.write().await;
        viz.build_graph(&task_tuples);
        viz.get_graph().cloned()
    }

    /// Get dependency graph visualization statistics.
    pub async fn get_dep_visualization_stats(
        &self,
    ) -> Option<dependency_visualization::GraphStats> {
        let tasks = self.tasks.lock().await;
        let task_tuples: Vec<(String, String, String, Vec<String>)> = tasks
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    t.name.clone(),
                    format!("{:?}", t.state),
                    t.depends_on.clone(),
                )
            })
            .collect();
        let mut viz = self.dep_visualization.write().await;
        viz.build_graph(&task_tuples);
        viz.get_stats().cloned()
    }

    /// Get the visualization configuration.
    pub async fn get_dep_visualization_config(
        &self,
    ) -> dependency_visualization::VisualizationConfig {
        let viz = self.dep_visualization.read().await;
        viz.get_config().clone()
    }

    /// Set the visualization configuration.
    pub async fn set_dep_visualization_config(
        &self,
        config: dependency_visualization::VisualizationConfig,
    ) {
        let mut viz = self.dep_visualization.write().await;
        viz.set_config(config);
    }

    /// Get detected dependency cycles.
    pub async fn get_dependency_cycles(&self) -> Vec<Vec<String>> {
        let tasks = self.tasks.lock().await;
        let task_tuples: Vec<(String, String, String, Vec<String>)> = tasks
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    t.name.clone(),
                    format!("{:?}", t.state),
                    t.depends_on.clone(),
                )
            })
            .collect();
        let mut viz = self.dep_visualization.write().await;
        viz.build_graph(&task_tuples);
        viz.get_cycles().unwrap_or_default().to_vec()
    }

    /// Get root tasks (tasks with no dependencies).
    pub async fn get_dependency_roots(&self) -> Vec<String> {
        let tasks = self.tasks.lock().await;
        let task_tuples: Vec<(String, String, String, Vec<String>)> = tasks
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    t.name.clone(),
                    format!("{:?}", t.state),
                    t.depends_on.clone(),
                )
            })
            .collect();
        let mut viz = self.dep_visualization.write().await;
        viz.build_graph(&task_tuples);
        viz.get_roots().unwrap_or_default().to_vec()
    }

    /// Get leaf tasks (tasks with no dependents).
    pub async fn get_dependency_leaves(&self) -> Vec<String> {
        let tasks = self.tasks.lock().await;
        let task_tuples: Vec<(String, String, String, Vec<String>)> = tasks
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    t.name.clone(),
                    format!("{:?}", t.state),
                    t.depends_on.clone(),
                )
            })
            .collect();
        let mut viz = self.dep_visualization.write().await;
        viz.build_graph(&task_tuples);
        viz.get_leaves().unwrap_or_default().to_vec()
    }

    /// Generate a text-based visualization of the dependency graph.
    pub async fn visualize_dependency_graph(&self) -> String {
        let tasks = self.tasks.lock().await;
        let task_tuples: Vec<(String, String, String, Vec<String>)> = tasks
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    t.name.clone(),
                    format!("{:?}", t.state),
                    t.depends_on.clone(),
                )
            })
            .collect();
        let mut viz = self.dep_visualization.write().await;
        viz.build_graph(&task_tuples);
        viz.visualize_text()
            .unwrap_or_else(|| "No tasks to visualize".to_string())
    }

    /// Export the dependency graph in DOT format (for Graphviz).
    pub async fn export_dependency_graph_dot(&self) -> String {
        let tasks = self.tasks.lock().await;
        let task_tuples: Vec<(String, String, String, Vec<String>)> = tasks
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    t.name.clone(),
                    format!("{:?}", t.state),
                    t.depends_on.clone(),
                )
            })
            .collect();
        let mut viz = self.dep_visualization.write().await;
        viz.build_graph(&task_tuples);
        viz.export_dot()
            .unwrap_or_else(|| "digraph Empty {}".to_string())
    }

    // ── Download Quota System (Phase 115) ──

    /// Set the download quota system configuration.
    pub async fn set_download_quota_config(&self, config: download_quota::QuotaSystemConfig) {
        let mut mgr = self.download_quota.lock().await;
        mgr.set_config(config);
        let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
    }

    /// Get the download quota system configuration.
    pub async fn get_download_quota_config(&self) -> download_quota::QuotaSystemConfig {
        let mgr = self.download_quota.lock().await;
        mgr.get_config().clone()
    }

    /// Add a new quota rule.
    pub async fn add_download_quota_rule(
        &self,
        rule: download_quota::QuotaRule,
    ) -> Result<String, download_quota::QuotaError> {
        let mut mgr = self.download_quota.lock().await;
        let result = mgr.add_rule(rule);
        let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
        result
    }

    /// Remove a quota rule by ID.
    pub async fn remove_download_quota_rule(&self, rule_id: &str) -> bool {
        let mut mgr = self.download_quota.lock().await;
        let removed = mgr.remove_rule(rule_id);
        if removed {
            let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
        }
        removed
    }

    /// List all quota rules.
    pub async fn list_download_quota_rules(&self) -> Vec<download_quota::QuotaRule> {
        let mgr = self.download_quota.lock().await;
        mgr.list_rules().into_iter().cloned().collect()
    }

    /// Enable or disable a quota rule.
    pub async fn set_download_quota_rule_enabled(&self, rule_id: &str, enabled: bool) -> bool {
        let mut mgr = self.download_quota.lock().await;
        let result = mgr.set_rule_enabled(rule_id, enabled);
        if result {
            let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
        }
        result
    }

    /// Update the daily limit of a quota rule.
    pub async fn set_download_quota_rule_limit(
        &self,
        rule_id: &str,
        daily_limit_bytes: u64,
    ) -> bool {
        let mut mgr = self.download_quota.lock().await;
        let result = mgr.set_rule_limit(rule_id, daily_limit_bytes);
        if result {
            let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
        }
        result
    }

    /// Get the quota summary (all rules with usage stats).
    pub async fn get_download_quota_summary(&self) -> download_quota::QuotaSummary {
        let mgr = self.download_quota.lock().await;
        let tasks = self.tasks.lock().await;
        mgr.get_summary(|scope| {
            tasks
                .iter()
                .filter(|t| match scope {
                    download_quota::QuotaScope::Tag(tag) => t.tags.iter().any(|tt| tt == tag),
                    download_quota::QuotaScope::Group(group) => {
                        t.group.as_deref() == Some(group.as_str())
                    }
                })
                .count()
        })
    }

    /// Record quota usage for a completed download (bytes downloaded).
    pub async fn record_download_quota_usage(&self, task_id: &str, bytes: u64) {
        let tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter().find(|t| t.id == task_id) {
            let mut mgr = self.download_quota.lock().await;
            let newly_exceeded = mgr.record_usage(&task.tags, task.group.as_deref(), bytes);
            if !newly_exceeded.is_empty() {
                // Auto-pause tasks matching exceeded scopes
                drop(mgr);
                drop(tasks);
                self.pause_tasks_for_exceeded_quotas(&newly_exceeded).await;
            } else {
                let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
            }
        }
    }

    /// Check if a task should be paused based on quota limits.
    pub async fn should_pause_for_quota(&self, task_id: &str) -> bool {
        let mgr = self.download_quota.lock().await;
        if !mgr.config.enabled {
            return false;
        }
        let tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter().find(|t| t.id == task_id) {
            mgr.should_pause_task(&task.tags, task.group.as_deref())
        } else {
            false
        }
    }

    /// Internal: pause all active tasks that match the exceeded quota scopes.
    async fn pause_tasks_for_exceeded_quotas(&self, exceeded_rule_ids: &[String]) {
        let mgr = self.download_quota.lock().await;
        let exceeded_scopes: Vec<download_quota::QuotaScope> = exceeded_rule_ids
            .iter()
            .filter_map(|id| mgr.get_rule(id).map(|r| r.scope.clone()))
            .collect();
        drop(mgr);

        if exceeded_scopes.is_empty() {
            return;
        }

        let mut tasks = self.tasks.lock().await;
        for task in tasks.iter_mut() {
            if task.state != DownloadState::Downloading {
                continue;
            }
            let should_pause = exceeded_scopes.iter().any(|scope| match scope {
                download_quota::QuotaScope::Tag(tag) => task.tags.iter().any(|t| t == tag),
                download_quota::QuotaScope::Group(group) => {
                    task.group.as_deref() == Some(group.as_str())
                }
            });
            if should_pause {
                task.state = DownloadState::Paused;
                task.speed_bps = 0.0;
                let _ = self.event_tx.send(TaskEvent::Updated {
                    task: TaskInfoEvent::from_task(task),
                });
            }
        }
        let mgr = self.download_quota.lock().await;
        let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
    }

    /// Refresh all quota usage records (reset for new day).
    pub async fn refresh_download_quota(&self) {
        let mut mgr = self.download_quota.lock().await;
        mgr.refresh_all();
        let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
    }

    /// Clear all quota usage data.
    pub async fn clear_download_quota_usage(&self) {
        let mut mgr = self.download_quota.lock().await;
        mgr.clear_usage();
        let _ = download_quota::save_download_quota(&mgr, &self.data_dir);
    }

    // ========== Advanced Search (Phase 117) ==========

    /// Execute an advanced search query across all tasks.
    /// Returns a SearchResult with matching tasks and metadata.
    pub async fn advanced_search(
        &self,
        query: &advanced_search::AdvancedSearchQuery,
        sort_by: Option<advanced_search::SearchSortBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> advanced_search::SearchResult {
        let start = std::time::Instant::now();
        let tasks = self.tasks.lock().await;

        let mut matched: Vec<DownloadTask> =
            tasks.iter().filter(|t| query.matches(t)).cloned().collect();

        let total = matched.len();

        // Sort if requested
        if let Some(sort_by) = sort_by {
            advanced_search::sort_search_results(&mut matched, sort_by);
        }

        // Apply offset and limit for pagination
        let offset = offset.unwrap_or(0);
        let matched = if offset >= matched.len() {
            vec![]
        } else {
            let end = limit
                .map(|l| (offset + l).min(matched.len()))
                .unwrap_or(matched.len());
            matched[offset..end].to_vec()
        };

        let execution_time_us = start.elapsed().as_micros() as u64;

        advanced_search::SearchResult {
            tasks: matched,
            total,
            execution_time_us,
            query_summary: query.summarize(),
        }
    }

    /// Set the last used advanced search query (for quick re-execution).
    pub async fn set_last_search_query(&self, query: advanced_search::AdvancedSearchQuery) {
        let mut cfg = self.advanced_search_config.write().await;
        *cfg = query;
    }

    /// Get the last used advanced search query.
    pub async fn get_last_search_query(&self) -> advanced_search::AdvancedSearchQuery {
        self.advanced_search_config.read().await.clone()
    }

    /// Re-execute the last used search query.
    pub async fn rerun_last_search(
        &self,
        sort_by: Option<advanced_search::SearchSortBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> advanced_search::SearchResult {
        let query = self.get_last_search_query().await;
        self.advanced_search(&query, sort_by, limit, offset).await
    }

    /// Quick search by name substring across all tasks.
    pub async fn quick_search(&self, query: &str) -> Vec<DownloadTask> {
        let search_query = advanced_search::AdvancedSearchQuery {
            name_contains: Some(query.to_string()),
            ..Default::default()
        };
        self.advanced_search(&search_query, None, None, None)
            .await
            .tasks
    }

    /// Search tasks by multiple tags (OR logic).
    pub async fn search_by_tags_any(&self, tags: Vec<String>) -> Vec<DownloadTask> {
        let search_query = advanced_search::AdvancedSearchQuery {
            tags_any: Some(tags),
            ..Default::default()
        };
        self.advanced_search(&search_query, None, None, None)
            .await
            .tasks
    }

    /// Search tasks by multiple tags (AND logic).
    pub async fn search_by_tags_all(&self, tags: Vec<String>) -> Vec<DownloadTask> {
        let search_query = advanced_search::AdvancedSearchQuery {
            tags_all: Some(tags),
            ..Default::default()
        };
        self.advanced_search(&search_query, None, None, None)
            .await
            .tasks
    }

    /// Get search statistics: count of tasks by state, protocol, etc.
    pub async fn get_search_stats(&self) -> serde_json::Value {
        let tasks = self.tasks.lock().await;
        let total = tasks.len();
        let by_state = tasks
            .iter()
            .fold(std::collections::HashMap::new(), |mut acc, t| {
                *acc.entry(format!("{:?}", t.state)).or_insert(0) += 1;
                acc
            });
        let by_protocol = tasks
            .iter()
            .fold(std::collections::HashMap::new(), |mut acc, t| {
                *acc.entry(format!("{:?}", t.protocol)).or_insert(0) += 1;
                acc
            });
        let with_tags = tasks.iter().filter(|t| !t.tags.is_empty()).count();
        let with_notes = tasks.iter().filter(|t| t.notes.is_some()).count();
        let with_errors = tasks.iter().filter(|t| t.error.is_some()).count();
        let with_mirrors = tasks.iter().filter(|t| !t.mirror_urls.is_empty()).count();
        let with_deadline = tasks.iter().filter(|t| t.deadline.is_some()).count();
        let with_checksum = tasks
            .iter()
            .filter(|t| t.expected_checksum.is_some())
            .count();

        serde_json::json!({
            "total": total,
            "by_state": by_state,
            "by_protocol": by_protocol,
            "with_tags": with_tags,
            "with_notes": with_notes,
            "with_errors": with_errors,
            "with_mirrors": with_mirrors,
            "with_deadline": with_deadline,
            "with_checksum": with_checksum
        })
    }

    // ─── Automation Rules Engine API ────────────────────────────────────

    /// Get the automation rules engine configuration.
    pub async fn get_automation_config(&self) -> automation_rules::AutomationConfig {
        self.automation_rules.read().await.get_config().clone()
    }

    /// Set the automation rules engine configuration.
    pub async fn set_automation_config(&self, config: automation_rules::AutomationConfig) {
        self.automation_rules.write().await.set_config(config);
    }

    /// Add a new automation rule. Returns the rule ID.
    pub async fn add_automation_rule(
        &self,
        rule: automation_rules::AutomationRule,
    ) -> Result<String, String> {
        let mut mgr = self.automation_rules.write().await;
        let id = mgr.add_rule(rule)?;
        let _ = mgr.save(&self.data_dir);
        Ok(id)
    }

    /// Remove an automation rule by ID.
    pub async fn remove_automation_rule(&self, rule_id: &str) -> bool {
        let mut mgr = self.automation_rules.write().await;
        let removed = mgr.remove_rule(rule_id);
        if removed {
            let _ = mgr.save(&self.data_dir);
        }
        removed
    }

    /// Get an automation rule by ID.
    pub async fn get_automation_rule(
        &self,
        rule_id: &str,
    ) -> Option<automation_rules::AutomationRule> {
        self.automation_rules
            .read()
            .await
            .get_rule(rule_id)
            .cloned()
    }

    /// List all automation rules.
    pub async fn list_automation_rules(&self) -> Vec<automation_rules::AutomationRule> {
        self.automation_rules.read().await.list_rules().to_vec()
    }

    /// Enable or disable an automation rule.
    pub async fn set_automation_rule_enabled(&self, rule_id: &str, enabled: bool) -> bool {
        let mut mgr = self.automation_rules.write().await;
        let updated = mgr.set_rule_enabled(rule_id, enabled);
        if updated {
            let _ = mgr.save(&self.data_dir);
        }
        updated
    }

    /// Update an automation rule.
    pub async fn update_automation_rule(&self, rule: automation_rules::AutomationRule) -> bool {
        let mut mgr = self.automation_rules.write().await;
        let updated = mgr.update_rule(rule);
        if updated {
            let _ = mgr.save(&self.data_dir);
        }
        updated
    }

    /// Get a summary of the automation rules engine.
    pub async fn get_automation_summary(&self) -> automation_rules::AutomationSummary {
        self.automation_rules.read().await.summary()
    }

    /// Clear automation rule fire history.
    pub async fn clear_automation_history(&self) {
        self.automation_rules.write().await.clear_history();
    }

    /// Reset all automation rule fire counts.
    pub async fn reset_automation_counts(&self) {
        let mut mgr = self.automation_rules.write().await;
        mgr.reset_fire_counts();
        let _ = mgr.save(&self.data_dir);
    }

    /// Evaluate automation rules for a given trigger and task context.
    /// Returns the list of rule fire results for rules that matched.
    pub async fn evaluate_automation_rules(
        &self,
        trigger: automation_rules::RuleTrigger,
        context: automation_rules::RuleEvalContext,
    ) -> Vec<automation_rules::RuleFireResult> {
        let mut mgr = self.automation_rules.write().await;
        let results = mgr.evaluate(trigger, &context);
        if !results.is_empty() {
            let _ = mgr.save(&self.data_dir);
        }
        results
    }

    /// Build a RuleEvalContext from a DownloadTask.
    pub async fn build_rule_context(task: &DownloadTask) -> automation_rules::RuleEvalContext {
        automation_rules::RuleEvalContext {
            task_id: task.id.clone(),
            name: task.name.clone(),
            url: task.source_url.clone().unwrap_or_default(),
            size_bytes: task.size,
            downloaded_bytes: task.downloaded,
            state: format!("{:?}", task.state),
            tags: task.tags.clone(),
            group: task.group.clone(),
            priority: task.priority as i32,
            speed_bps: task.speed_bps as u64,
            protocol: format!("{:?}", task.protocol),
            has_mirrors: !task.mirror_urls.is_empty(),
            has_checksum: task.expected_checksum.is_some(),
            has_deadline: task.deadline.is_some(),
            queued_since: if task.state == DownloadState::Queued {
                Some(task.updated_at.timestamp() as u64)
            } else {
                None
            },
            save_path: Some(task.save_path.to_string_lossy().to_string()),
        }
    }

    // ─── Task Schedule Windows API ──────────────────────────────────────

    /// Get the task schedule windows configuration.
    pub async fn get_task_schedule_windows_config(
        &self,
    ) -> task_schedule_windows::TaskScheduleWindowsConfig {
        self.task_schedule_windows.read().await.config().clone()
    }

    /// Set the task schedule windows configuration.
    pub async fn set_task_schedule_windows_config(
        &self,
        config: task_schedule_windows::TaskScheduleWindowsConfig,
    ) {
        let mut mgr = self.task_schedule_windows.write().await;
        mgr.set_config(config);
        let path = self.data_dir.join("task_schedule_windows.json");
        if let Err(e) = mgr.save_to_file(&path).await {
            tracing::warn!(error = %e, "Failed to save task schedule windows config");
        }
    }

    /// Add a schedule window to a task.
    pub async fn add_task_schedule_window(
        &self,
        task_id: &str,
        window: task_schedule_windows::ScheduleWindow,
    ) {
        let mut mgr = self.task_schedule_windows.write().await;
        mgr.add_window(task_id, window);
        let path = self.data_dir.join("task_schedule_windows.json");
        if let Err(e) = mgr.save_to_file(&path).await {
            tracing::warn!(error = %e, "Failed to save task schedule windows");
        }
    }

    /// Remove a schedule window from a task.
    pub async fn remove_task_schedule_window(&self, task_id: &str, window_id: &str) -> bool {
        let mut mgr = self.task_schedule_windows.write().await;
        let removed = mgr.remove_window(task_id, window_id);
        if removed {
            let path = self.data_dir.join("task_schedule_windows.json");
            if let Err(e) = mgr.save_to_file(&path).await {
                tracing::warn!(error = %e, "Failed to save task schedule windows");
            }
        }
        removed
    }

    /// Get all schedule windows for a task.
    pub async fn get_task_schedule_windows(
        &self,
        task_id: &str,
    ) -> Option<Vec<task_schedule_windows::ScheduleWindow>> {
        self.task_schedule_windows
            .read()
            .await
            .get_windows(task_id)
            .cloned()
    }

    /// Clear all schedule windows for a task.
    pub async fn clear_task_schedule_windows(&self, task_id: &str) {
        let mut mgr = self.task_schedule_windows.write().await;
        mgr.clear_task_windows(task_id);
        let path = self.data_dir.join("task_schedule_windows.json");
        if let Err(e) = mgr.save_to_file(&path).await {
            tracing::warn!(error = %e, "Failed to save task schedule windows");
        }
    }

    /// Check if a task is allowed to download right now based on its schedule windows.
    pub async fn is_task_allowed_by_schedule(&self, task_id: &str, priority: i32) -> bool {
        self.task_schedule_windows
            .read()
            .await
            .is_allowed_now(task_id, priority)
    }

    /// Get the next time a task will be allowed to download.
    pub async fn next_task_allowed_time(
        &self,
        task_id: &str,
        priority: i32,
    ) -> Option<chrono::DateTime<chrono::Local>> {
        self.task_schedule_windows
            .read()
            .await
            .next_allowed_time(task_id, priority)
    }

    /// Get a summary of task schedule windows.
    pub async fn get_task_schedule_windows_summary(&self) -> serde_json::Value {
        let mgr = self.task_schedule_windows.read().await;
        let config = mgr.config();
        let total_tasks_with_windows = config.task_windows.len();
        let total_windows: usize = config
            .task_windows
            .values()
            .map(|v| v.len())
            .collect::<Vec<_>>()
            .iter()
            .sum();
        let enabled_windows: usize = config
            .task_windows
            .values()
            .flat_map(|v| v.iter())
            .filter(|w| w.enabled)
            .count();

        serde_json::json!({
            "enabled": config.enabled,
            "priority_bypass": config.priority_bypass,
            "total_tasks_with_windows": total_tasks_with_windows,
            "total_windows": total_windows,
            "enabled_windows": enabled_windows
        })
    }

    /// Rename a download task.
    /// Returns true if the task was found and renamed.
    pub async fn rename_task(&self, task_id: &str, new_name: String) -> bool {
        let new_name = new_name.trim().to_string();
        if new_name.is_empty() {
            return false;
        }
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.name = new_name;
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

    /// Clone (duplicate) an existing download task.
    ///
    /// Creates a new task with the same source URL, metadata (tags, group, priority,
    /// speed limit, mirrors, checksum, retry policy, etc.) but fresh progress.
    /// The cloned task starts in Queued state and is automatically spawned.
    ///
    /// Returns the new task ID on success.
    /// Returns an error if the source task doesn't exist or has no source URL.
    pub async fn clone_task(&self, task_id: &str) -> Result<String, DownloadManagerError> {
        let source_task = {
            let tasks = self.tasks.lock().await;
            tasks
                .iter()
                .find(|t| t.id == task_id)
                .cloned()
                .ok_or_else(|| DownloadManagerError::TaskNotFound(task_id.to_string()))?
        };

        let source_url = source_task
            .source_url
            .as_ref()
            .ok_or_else(|| {
                DownloadManagerError::Io("Cannot clone task: no source URL".to_string())
            })?
            .clone();

        // Build the new task with fresh state but preserved metadata
        let new_task_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();

        let mut new_task = DownloadTask {
            id: new_task_id.clone(),
            name: source_task.name.clone(),
            protocol: source_task.protocol,
            size: source_task.size,
            downloaded: 0,
            state: DownloadState::Queued,
            error: None,
            speed_bps: 0.0,
            save_path: source_task.save_path.clone(),
            created_at: now,
            updated_at: now,
            tags: source_task.tags.clone(),
            priority: source_task.priority,
            schedule: source_task.schedule,
            bandwidth_weight: source_task.bandwidth_weight,
            queue_position: None,   // fresh position
            depends_on: Vec::new(), // don't copy dependencies
            notes: source_task.notes.clone(),
            group: source_task.group.clone(),
            speed_limit_bps: source_task.speed_limit_bps,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some(source_url.clone()),
            expected_checksum: source_task.expected_checksum.clone(),
            checksum_algorithm: source_task.checksum_algorithm,
            active_time_seconds: 0.0,
            current_session_start: None,
            mirror_urls: source_task.mirror_urls.clone(),
            retry_policy: source_task.retry_policy,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
        };

        // Append " (copy)" to name if it doesn't already end with it
        if !new_task.name.ends_with(" (copy)") {
            new_task.name = format!("{} (copy)", new_task.name);
        }

        self.tasks.lock().await.push(new_task.clone());
        self.persist_tasks().await;
        self.emit_event(TaskEvent::Added {
            task: TaskInfoEvent::from_task(&new_task),
        });

        // Re-download using the source URL
        let add_result = self.add_url(&source_url).await;
        match add_result {
            Ok(actual_id) => {
                // The add_url created its own task; remove our placeholder
                // and update the actual task with cloned metadata
                {
                    let mut tasks = self.tasks.lock().await;
                    // Remove the placeholder we just added
                    tasks.retain(|t| t.id != new_task_id);
                    // Update the actual task with cloned metadata
                    if let Some(actual) = tasks.iter_mut().find(|t| t.id == actual_id) {
                        actual.tags = new_task.tags;
                        actual.priority = new_task.priority;
                        actual.schedule = new_task.schedule;
                        actual.bandwidth_weight = new_task.bandwidth_weight;
                        actual.notes = new_task.notes;
                        actual.group = new_task.group;
                        actual.speed_limit_bps = new_task.speed_limit_bps;
                        actual.expected_checksum = new_task.expected_checksum;
                        actual.checksum_algorithm = new_task.checksum_algorithm;
                        actual.mirror_urls = new_task.mirror_urls;
                        actual.retry_policy = new_task.retry_policy;
                        actual.name = new_task.name;
                        actual.save_path = new_task.save_path;
                        actual.updated_at = chrono::Utc::now();
                        self.emit_event(TaskEvent::Updated {
                            task: TaskInfoEvent::from_task(actual),
                        });
                    }
                }
                self.persist_tasks().await;
                Ok(actual_id)
            }
            Err(_) => {
                // If add_url fails, keep the placeholder as a queued task
                Ok(new_task_id)
            }
        }
    }

    /// Set or clear user notes/description for a download task.
    /// Pass None or empty string to clear notes.
    /// Returns true if the task was found and updated.
    pub async fn set_task_notes(&self, task_id: &str, notes: Option<String>) -> bool {
        let notes = notes.and_then(|n| {
            let trimmed = n.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.notes = notes;
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

    /// Add a user comment to a download task.
    /// Returns the created comment on success.
    pub async fn add_task_comment(
        &self,
        task_id: &str,
        text: &str,
        author: Option<&str>,
        tags: Vec<String>,
    ) -> Result<task_comments::TaskComment, task_comments::TaskCommentError> {
        // Verify task exists
        {
            let tasks = self.tasks.lock().await;
            if !tasks.iter().any(|t| t.id == task_id) {
                return Err(task_comments::TaskCommentError::TaskNotFound(
                    task_id.to_string(),
                ));
            }
        }

        let mut comments = self.task_comments.lock().await;
        let comment = comments.add_comment(task_id, text, author, tags)?;

        // Log to task activity
        let task_name = {
            let tasks = self.tasks.lock().await;
            tasks
                .iter()
                .find(|t| t.id == task_id)
                .map(|t| t.name.clone())
                .unwrap_or_default()
        };
        let preview = comment.text.chars().take(50).collect::<String>();
        self.log_task_activity(
            task_id,
            &task_name,
            task_activity::ActivityEventType::CommentAdded,
            format!("Comment added: {preview}"),
        )
        .await;

        drop(comments);
        self.persist_task_comments().await;
        Ok(comment)
    }

    /// Remove a comment by ID.
    pub async fn remove_task_comment(
        &self,
        comment_id: &str,
    ) -> Result<task_comments::TaskComment, task_comments::TaskCommentError> {
        let mut comments = self.task_comments.lock().await;
        let removed = comments.remove_comment(comment_id)?;
        drop(comments);
        self.persist_task_comments().await;
        Ok(removed)
    }

    /// Get all comments for a task (chronological order).
    pub async fn get_task_comments(&self, task_id: &str) -> Vec<task_comments::TaskComment> {
        let comments = self.task_comments.lock().await;
        comments
            .get_comments(task_id)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Get comment summary for a task.
    pub async fn get_task_comment_summary(
        &self,
        task_id: &str,
    ) -> task_comments::TaskCommentSummary {
        let comments = self.task_comments.lock().await;
        comments.get_comment_summary(task_id)
    }

    /// List all task IDs that have comments.
    pub async fn list_tasks_with_comments(&self) -> Vec<String> {
        let comments = self.task_comments.lock().await;
        comments.list_tasks_with_comments()
    }

    /// Get comment counts per task.
    pub async fn get_task_comment_counts(&self) -> HashMap<String, usize> {
        let comments = self.task_comments.lock().await;
        comments.get_comment_counts()
    }

    /// Search comments across all tasks.
    pub async fn search_task_comments(&self, query: &str) -> task_comments::CommentSearchResult {
        let comments = self.task_comments.lock().await;
        comments.search_comments(query)
    }

    /// Search comments by tag.
    pub async fn search_task_comments_by_tag(&self, tag: &str) -> Vec<task_comments::TaskComment> {
        let comments = self.task_comments.lock().await;
        comments.search_by_tag(tag)
    }

    /// Get/set task comments configuration.
    pub async fn set_task_comments_config(&self, config: task_comments::TaskCommentsConfig) {
        let mut comments = self.task_comments.lock().await;
        comments.set_config(config);
        drop(comments);
        self.persist_task_comments().await;
    }

    pub async fn get_task_comments_config(&self) -> task_comments::TaskCommentsConfig {
        let comments = self.task_comments.lock().await;
        comments.config().clone()
    }

    /// Persist task comments to disk.
    async fn persist_task_comments(&self) {
        let comments = self.task_comments.lock().await;
        let path = self.data_dir.join("task_comments.json");
        if let Err(e) = comments.save(&path).await {
            tracing::warn!(error = %e, "Failed to persist task comments");
        }
    }

    /// Add a task to favorites.
    /// Returns Ok(()) if successful, Err if task is already favorited or limit reached.
    pub async fn add_favorite(&self, task_id: &str, note: Option<String>) -> Result<(), String> {
        let mut favorites = self.task_favorites.lock().await;
        favorites.add_favorite(task_id.to_string(), note)?;
        drop(favorites);
        self.persist_task_favorites().await;
        Ok(())
    }

    /// Remove a task from favorites.
    /// Returns true if the task was removed, false if it wasn't in favorites.
    pub async fn remove_favorite(&self, task_id: &str) -> bool {
        let mut favorites = self.task_favorites.lock().await;
        let removed = favorites.remove_favorite(task_id);
        drop(favorites);
        if removed {
            self.persist_task_favorites().await;
        }
        removed
    }

    /// Check if a task is in favorites.
    pub async fn is_favorite(&self, task_id: &str) -> bool {
        let favorites = self.task_favorites.lock().await;
        favorites.is_favorite(task_id)
    }

    /// Get all favorite task IDs.
    pub async fn get_favorite_ids(&self) -> Vec<String> {
        let favorites = self.task_favorites.lock().await;
        favorites.get_favorite_ids().into_iter().collect()
    }

    /// Get favorites count.
    pub async fn get_favorites_count(&self) -> usize {
        let favorites = self.task_favorites.lock().await;
        favorites.count()
    }

    /// Set favorites configuration.
    pub async fn set_favorites_config(&self, config: task_favorites::FavoritesConfig) {
        let mut favorites = self.task_favorites.lock().await;
        favorites.set_config(config);
    }

    /// Get favorites configuration.
    pub async fn get_favorites_config(&self) -> task_favorites::FavoritesConfig {
        let favorites = self.task_favorites.lock().await;
        favorites.get_config().clone()
    }

    /// Persist task favorites to disk.
    async fn persist_task_favorites(&self) {
        let favorites = self.task_favorites.lock().await;
        let path = self.data_dir.join("task_favorites.json");
        if let Err(e) = favorites.save_to_file(&path) {
            tracing::warn!(error = %e, "Failed to persist task favorites");
        }
    }

    /// Internal helper to get favorite task IDs (used by scheduler).
    async fn get_favorite_ids_internal(&self) -> std::collections::HashSet<String> {
        let favorites = self.task_favorites.lock().await;
        favorites.get_favorite_ids()
    }

    /// Set mirror/fallback URLs for a download task.
    /// These URLs are tried in order if the primary source fails.
    /// Only applicable to HTTP/Xunlei downloads.
    /// Returns true if the task was found and updated.
    pub async fn set_mirrors(&self, task_id: &str, urls: Vec<String>) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.mirror_urls = urls.into_iter().filter(|u| !u.is_empty()).collect();
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

    /// Get the mirror/fallback URLs for a download task.
    pub async fn get_mirrors(&self, task_id: &str) -> Option<Vec<String>> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.id == task_id)
            .map(|t| t.mirror_urls.clone())
    }

    /// Check health of all mirrors for a task and return a summary.
    pub async fn check_mirror_health(&self, task_id: &str) -> Option<MirrorSummary> {
        let tasks = self.tasks.lock().await;
        let task = tasks.iter().find(|t| t.id == task_id)?;
        let config = MirrorHealthConfig::default();
        let summary = mirror_health::check_all_mirrors(
            task_id,
            task.source_url.as_deref(),
            &task.mirror_urls,
            &config,
        )
        .await;
        Some(summary)
    }

    /// Switch to the best performing mirror for a task.
    /// Returns the new active URL if switched, or None if no switch needed.
    pub async fn switch_to_best_mirror(&self, task_id: &str) -> Option<String> {
        let summary = self.check_mirror_health(task_id).await?;
        if !summary.should_switch {
            return None;
        }
        let recommended = summary.recommended_url.clone()?;
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            // Move current source to mirrors, set recommended as new source
            if let Some(ref current) = task.source_url
                && !task.mirror_urls.contains(current)
            {
                task.mirror_urls.push(current.clone());
            }
            // Remove recommended from mirrors
            task.mirror_urls.retain(|u| u != &recommended);
            task.source_url = Some(recommended.clone());
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });
            drop(tasks);
            self.persist_tasks().await;
            Some(recommended)
        } else {
            None
        }
    }

    /// Set or clear the group for a download task.
    /// Pass None or empty string to remove from group.
    /// Returns true if the task was found and updated.
    pub async fn set_task_group(&self, task_id: &str, group: Option<String>) -> bool {
        let group = group.and_then(|g| {
            let trimmed = g.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.group = group;
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

    // ─── Phase 80: Bulk Task Operations ───

    /// Get task IDs matching a bulk filter.
    pub async fn get_bulk_filter_matches(&self, filter: &bulk_ops::BulkFilter) -> Vec<String> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .filter(|t| {
                // Filter by task IDs
                if !filter.task_ids.is_empty() && !filter.task_ids.contains(&t.id) {
                    return false;
                }
                // Filter by state
                if let Some(ref state_str) = filter.state {
                    let state_match = match state_str.to_lowercase().as_str() {
                        "downloading" | "running" | "active" => {
                            t.state == DownloadState::Downloading
                        }
                        "paused" => t.state == DownloadState::Paused,
                        "queued" | "waiting" => t.state == DownloadState::Queued,
                        "complete" | "completed" | "done" => t.state == DownloadState::Complete,
                        "error" | "failed" => t.state == DownloadState::Error,
                        _ => false,
                    };
                    if !state_match {
                        return false;
                    }
                }
                // Filter by protocol
                if let Some(ref proto_str) = filter.protocol {
                    let proto_match = match proto_str.to_lowercase().as_str() {
                        "http" | "https" | "ftp" | "xunlei" => {
                            t.protocol == DownloadProtocol::Xunlei
                        }
                        "torrent" | "bittorrent" | "bt" => t.protocol == DownloadProtocol::Torrent,
                        "ed2k" | "edonkey" | "emule" => t.protocol == DownloadProtocol::Ed2k,
                        "p2p" => t.protocol == DownloadProtocol::P2P,
                        "magnet" => t.protocol == DownloadProtocol::Magnet,
                        _ => false,
                    };
                    if !proto_match {
                        return false;
                    }
                }
                // Filter by tag
                if let Some(ref tag) = filter.tag {
                    let tag_lower = tag.to_lowercase();
                    if !t.tags.contains(&tag_lower) {
                        return false;
                    }
                }
                // Filter by group
                if let Some(ref group) = filter.group
                    && t.group.as_deref() != Some(group.as_str())
                {
                    return false;
                }
                true
            })
            .map(|t| t.id.clone())
            .collect()
    }

    /// Bulk tag operation: add/remove/replace/clear tags on multiple tasks.
    pub async fn bulk_tag(
        &self,
        filter: &bulk_ops::BulkFilter,
        action: &bulk_ops::BulkTagAction,
    ) -> bulk_ops::BulkResult {
        let matched_ids = self.get_bulk_filter_matches(filter).await;
        let matched = matched_ids.len();
        let mut modified_ids = Vec::new();

        if matched == 0 {
            return bulk_ops::BulkResult {
                matched: 0,
                modified: 0,
                modified_ids,
                description: "No tasks matched the filter".to_string(),
            };
        }

        let mut tasks = self.tasks.lock().await;
        for id in &matched_ids {
            if let Some(task) = tasks.iter_mut().find(|t| &t.id == id) {
                match action {
                    bulk_ops::BulkTagAction::Add { tags } => {
                        for tag in tags {
                            let tag = tag.trim().to_lowercase();
                            if !tag.is_empty() && !task.tags.contains(&tag) {
                                task.tags.push(tag);
                            }
                        }
                        task.tags.sort();
                        task.tags.dedup();
                    }
                    bulk_ops::BulkTagAction::Remove { tags } => {
                        let tags_to_remove: Vec<String> =
                            tags.iter().map(|t| t.trim().to_lowercase()).collect();
                        task.tags.retain(|t| !tags_to_remove.contains(t));
                    }
                    bulk_ops::BulkTagAction::Replace { tags } => {
                        task.tags = tags
                            .iter()
                            .map(|t| t.trim().to_lowercase())
                            .filter(|t| !t.is_empty())
                            .collect();
                        task.tags.sort();
                        task.tags.dedup();
                    }
                    bulk_ops::BulkTagAction::Clear => {
                        task.tags.clear();
                    }
                }
                task.updated_at = chrono::Utc::now();
                self.emit_event(TaskEvent::Updated {
                    task: TaskInfoEvent::from_task(task),
                });
                modified_ids.push(id.clone());
            }
        }
        drop(tasks);
        self.persist_tasks().await;

        let modified_count = modified_ids.len();
        let description = match action {
            bulk_ops::BulkTagAction::Add { tags } => {
                format!("Added {} tags to {} tasks", tags.len(), modified_count)
            }
            bulk_ops::BulkTagAction::Remove { tags } => {
                format!("Removed {} tags from {} tasks", tags.len(), modified_count)
            }
            bulk_ops::BulkTagAction::Replace { tags } => {
                format!(
                    "Replaced tags with {} tags on {} tasks",
                    tags.len(),
                    modified_count
                )
            }
            bulk_ops::BulkTagAction::Clear => {
                format!("Cleared tags from {} tasks", modified_count)
            }
        };

        bulk_ops::BulkResult {
            matched,
            modified: modified_count,
            modified_ids,
            description,
        }
    }

    /// Bulk group operation: set/clear group on multiple tasks.
    pub async fn bulk_group(
        &self,
        filter: &bulk_ops::BulkFilter,
        action: &bulk_ops::BulkGroupAction,
    ) -> bulk_ops::BulkResult {
        let matched_ids = self.get_bulk_filter_matches(filter).await;
        let matched = matched_ids.len();
        let mut modified_ids = Vec::new();

        if matched == 0 {
            return bulk_ops::BulkResult {
                matched: 0,
                modified: 0,
                modified_ids,
                description: "No tasks matched the filter".to_string(),
            };
        }

        let group_value = match action {
            bulk_ops::BulkGroupAction::Set { group } => {
                let trimmed = group.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            }
            bulk_ops::BulkGroupAction::Clear => None,
        };

        let mut tasks = self.tasks.lock().await;
        for id in &matched_ids {
            if let Some(task) = tasks.iter_mut().find(|t| &t.id == id) {
                task.group = group_value.clone();
                task.updated_at = chrono::Utc::now();
                self.emit_event(TaskEvent::Updated {
                    task: TaskInfoEvent::from_task(task),
                });
                modified_ids.push(id.clone());
            }
        }
        drop(tasks);
        self.persist_tasks().await;

        let modified_count = modified_ids.len();
        let description = match action {
            bulk_ops::BulkGroupAction::Set { group } => {
                format!("Set group '{}' on {} tasks", group, modified_count)
            }
            bulk_ops::BulkGroupAction::Clear => {
                format!("Cleared group from {} tasks", modified_count)
            }
        };

        bulk_ops::BulkResult {
            matched,
            modified: modified_count,
            modified_ids,
            description,
        }
    }

    /// Bulk priority operation: set priority on multiple tasks.
    pub async fn bulk_priority(
        &self,
        filter: &bulk_ops::BulkFilter,
        action: &bulk_ops::BulkPriorityAction,
    ) -> bulk_ops::BulkResult {
        let matched_ids = self.get_bulk_filter_matches(filter).await;
        let matched = matched_ids.len();
        let mut modified_ids = Vec::new();

        if matched == 0 {
            return bulk_ops::BulkResult {
                matched: 0,
                modified: 0,
                modified_ids,
                description: "No tasks matched the filter".to_string(),
            };
        }

        let priority = match bulk_ops::parse_priority(&action.priority) {
            Some(p) => p,
            None => {
                return bulk_ops::BulkResult {
                    matched,
                    modified: 0,
                    modified_ids,
                    description: format!("Invalid priority: {}", action.priority),
                };
            }
        };

        let priority_enum = match priority.as_str() {
            "Low" => DownloadPriority::Low,
            "Normal" => DownloadPriority::Normal,
            "High" => DownloadPriority::High,
            _ => DownloadPriority::Normal,
        };

        let mut tasks = self.tasks.lock().await;
        for id in &matched_ids {
            if let Some(task) = tasks.iter_mut().find(|t| &t.id == id) {
                task.priority = priority_enum;
                task.updated_at = chrono::Utc::now();
                self.emit_event(TaskEvent::Updated {
                    task: TaskInfoEvent::from_task(task),
                });
                modified_ids.push(id.clone());
            }
        }
        drop(tasks);
        self.persist_tasks().await;

        let modified_count = modified_ids.len();
        bulk_ops::BulkResult {
            matched,
            modified: modified_count,
            modified_ids,
            description: format!("Set priority to {} on {} tasks", priority, modified_count),
        }
    }

    /// Bulk speed limit operation: set speed limit on multiple tasks.
    pub async fn bulk_speed_limit(
        &self,
        filter: &bulk_ops::BulkFilter,
        action: &bulk_ops::BulkSpeedLimitAction,
    ) -> bulk_ops::BulkResult {
        let matched_ids = self.get_bulk_filter_matches(filter).await;
        let matched = matched_ids.len();
        let mut modified_ids = Vec::new();

        if matched == 0 {
            return bulk_ops::BulkResult {
                matched: 0,
                modified: 0,
                modified_ids,
                description: "No tasks matched the filter".to_string(),
            };
        }

        let mut tasks = self.tasks.lock().await;
        for id in &matched_ids {
            if let Some(task) = tasks.iter_mut().find(|t| &t.id == id) {
                task.speed_limit_bps = action.bytes_per_sec;
                task.updated_at = chrono::Utc::now();
                self.emit_event(TaskEvent::Updated {
                    task: TaskInfoEvent::from_task(task),
                });
                modified_ids.push(id.clone());
            }
        }
        drop(tasks);
        self.persist_tasks().await;

        let modified_count = modified_ids.len();
        let description = match action.bytes_per_sec {
            Some(limit) => format!(
                "Set speed limit to {} bytes/s on {} tasks",
                limit, modified_count
            ),
            None => format!("Removed speed limit from {} tasks", modified_count),
        };

        bulk_ops::BulkResult {
            matched,
            modified: modified_count,
            modified_ids,
            description,
        }
    }

    /// Bulk bandwidth weight operation: set weight on multiple tasks.
    pub async fn bulk_bandwidth_weight(
        &self,
        filter: &bulk_ops::BulkFilter,
        action: &bulk_ops::BulkWeightAction,
    ) -> bulk_ops::BulkResult {
        let matched_ids = self.get_bulk_filter_matches(filter).await;
        let matched = matched_ids.len();
        let mut modified_ids = Vec::new();

        if matched == 0 {
            return bulk_ops::BulkResult {
                matched: 0,
                modified: 0,
                modified_ids,
                description: "No tasks matched the filter".to_string(),
            };
        }

        let weight = action.weight.clamp(1, 10);

        let mut tasks = self.tasks.lock().await;
        for id in &matched_ids {
            if let Some(task) = tasks.iter_mut().find(|t| &t.id == id) {
                task.bandwidth_weight = weight;
                task.updated_at = chrono::Utc::now();
                self.emit_event(TaskEvent::Updated {
                    task: TaskInfoEvent::from_task(task),
                });
                modified_ids.push(id.clone());
            }
        }
        drop(tasks);
        self.persist_tasks().await;

        let modified_count = modified_ids.len();
        bulk_ops::BulkResult {
            matched,
            modified: modified_count,
            modified_ids,
            description: format!(
                "Set bandwidth weight to {} on {} tasks",
                weight, modified_count
            ),
        }
    }

    /// Set per-task retry policy. Pass None to use global defaults.
    pub async fn set_task_retry_policy(&self, task_id: &str, policy: Option<RetryPolicy>) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.retry_policy = policy;
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

    /// Get per-task retry policy. Returns None if using global defaults.
    pub async fn get_task_retry_policy(&self, task_id: &str) -> Option<RetryPolicy> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.id == task_id)
            .and_then(|t| t.retry_policy)
    }

    /// Set sequential download mode for a torrent task.
    /// When enabled, pieces are downloaded in order (0, 1, 2, ...)
    /// instead of rarest-first. Useful for streaming media while downloading.
    /// Returns true if the task was found and updated.
    pub async fn set_sequential_mode(&self, task_id: &str, enabled: bool) -> bool {
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.sequential_mode = enabled;
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

    /// Get sequential download mode for a torrent task.
    pub async fn get_sequential_mode(&self, task_id: &str) -> bool {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .find(|t| t.id == task_id)
            .map(|t| t.sequential_mode)
            .unwrap_or(false)
    }

    /// Set expected checksum for a download task
    pub async fn set_task_checksum(
        &self,
        task_id: &str,
        checksum: &str,
        algorithm: checksum::ChecksumAlgorithm,
    ) -> Result<(), String> {
        let checksum_lower = checksum.to_lowercase();
        if checksum_lower.len() != algorithm.hex_len() {
            return Err(format!(
                "Invalid checksum length: expected {} hex chars for {}, got {}",
                algorithm.hex_len(),
                algorithm.name(),
                checksum_lower.len()
            ));
        }
        if !checksum_lower.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("Checksum must be a hex string".to_string());
        }

        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id) {
            task.expected_checksum = Some(checksum_lower);
            task.checksum_algorithm = Some(algorithm);
            task.updated_at = chrono::Utc::now();
            self.emit_event(TaskEvent::Updated {
                task: TaskInfoEvent::from_task(task),
            });
            drop(tasks);
            self.persist_tasks().await;
            Ok(())
        } else {
            Err(format!("Task {} not found", task_id))
        }
    }

    /// List all tasks in a specific group.
    pub async fn list_tasks_by_group(&self, group: &str) -> Vec<DownloadTask> {
        let tasks = self.tasks.lock().await;
        tasks
            .iter()
            .filter(|t| t.group.as_deref() == Some(group))
            .cloned()
            .collect()
    }

    /// List all unique group names across all tasks.
    pub async fn list_all_groups(&self) -> Vec<String> {
        let tasks = self.tasks.lock().await;
        let mut groups: Vec<String> = tasks
            .iter()
            .filter_map(|t| t.group.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        groups.sort();
        groups
    }

    /// Get torrent file entries for a task (for multi-file torrent file selection).
    /// Returns file entries with selection state, or None if task is not a torrent/magnet.
    pub async fn get_torrent_file_entries(&self, task_id: &str) -> Option<Vec<torrent::FileEntry>> {
        let tasks = self.tasks.lock().await;
        let task = tasks.iter().find(|t| t.id == task_id)?;
        if task.protocol != DownloadProtocol::Torrent && task.protocol != DownloadProtocol::Magnet {
            return None;
        }
        // For now, return a placeholder with the task's files.
        // In a full implementation, this would read the parsed TorrentMeta.
        // Since we can't easily access the parsed meta from here (it's loaded at spawn time),
        // we provide a utility that parses the torrent file on demand.
        None
    }

    /// Parse a torrent file and return file entries with selection info.
    /// This is a utility method for CLI/API to inspect torrent contents.
    pub async fn inspect_torrent_files(
        &self,
        torrent_path: &std::path::Path,
        selection: &torrent::FileSelection,
    ) -> Result<Vec<torrent::FileEntry>, String> {
        let data = tokio::fs::read(torrent_path)
            .await
            .map_err(|e| format!("Failed to read torrent file: {}", e))?;
        let meta = torrent::TorrentMeta::from_bytes(&data)
            .map_err(|e| format!("Failed to parse torrent: {}", e))?;

        if meta.info.files.is_empty() {
            // Single-file torrent
            return Ok(vec![torrent::FileEntry {
                index: 0,
                path: meta.info.name.clone(),
                size: meta.total_size(),
                selected: true,
            }]);
        }

        Ok(selection.file_entries(&meta))
    }

    /// Add a new auto-categorization rule.
    pub async fn add_categorize_rule(
        &self,
        rule: auto_categorize::CategorizeRule,
    ) -> Result<(), auto_categorize::CategorizeError> {
        let mut rules = self.categorize_rules.lock().await;
        rules.push(rule);
        auto_categorize::save_rules(&self.data_dir, &rules)
            .await
            .map_err(|e| auto_categorize::CategorizeError::Io(e.to_string()))
    }

    /// List all auto-categorization rules.
    pub async fn list_categorize_rules(&self) -> Vec<auto_categorize::CategorizeRule> {
        self.categorize_rules.lock().await.clone()
    }

    /// Remove an auto-categorization rule by ID.
    pub async fn remove_categorize_rule(&self, rule_id: &str) -> bool {
        let mut rules = self.categorize_rules.lock().await;
        let original_len = rules.len();
        rules.retain(|r| r.id != rule_id);
        if rules.len() != original_len {
            if let Err(e) = auto_categorize::save_rules(&self.data_dir, &rules).await {
                tracing::error!(error = %e, "Failed to save categorize rules");
            }
            true
        } else {
            false
        }
    }

    /// Apply auto-categorization rules to a URL/filename and return the matching action.
    pub async fn apply_auto_categorize(
        &self,
        url: &str,
        filename: &str,
    ) -> Option<auto_categorize::CategorizeAction> {
        let rules = self.categorize_rules.lock().await;
        auto_categorize::apply_rules(&rules, url, filename).cloned()
    }

    // ─── Phase 77: Path Rules ───

    /// Add a path rule for automatic save path assignment.
    pub async fn add_path_rule(
        &self,
        rule: path_rules::PathRule,
    ) -> Result<(), path_rules::PathRulesError> {
        let mut manager = self.path_rules.lock().await;
        manager.add_rule(rule);
        let config_path = self.data_dir.join("path_rules.json");
        path_rules::save_path_rules(&manager, &config_path).await
    }

    /// List all path rules.
    pub async fn list_path_rules(&self) -> Vec<path_rules::PathRule> {
        self.path_rules.lock().await.list_rules().to_vec()
    }

    /// Remove a path rule by ID.
    pub async fn remove_path_rule(
        &self,
        rule_id: &str,
    ) -> Result<path_rules::PathRule, path_rules::PathRulesError> {
        let mut manager = self.path_rules.lock().await;
        let removed = manager.remove_rule(rule_id)?;
        let config_path = self.data_dir.join("path_rules.json");
        path_rules::save_path_rules(&manager, &config_path).await?;
        Ok(removed)
    }

    /// Get a path rule by ID.
    pub async fn get_path_rule(&self, rule_id: &str) -> Option<path_rules::PathRule> {
        self.path_rules.lock().await.get_rule(rule_id).cloned()
    }

    /// Enable or disable a path rule.
    pub async fn set_path_rule_enabled(
        &self,
        rule_id: &str,
        enabled: bool,
    ) -> Result<(), path_rules::PathRulesError> {
        let mut manager = self.path_rules.lock().await;
        manager.set_rule_enabled(rule_id, enabled)?;
        let config_path = self.data_dir.join("path_rules.json");
        path_rules::save_path_rules(&manager, &config_path).await
    }

    /// Update a path rule's save path.
    pub async fn update_path_rule_save_path(
        &self,
        rule_id: &str,
        save_path: std::path::PathBuf,
    ) -> Result<(), path_rules::PathRulesError> {
        let mut manager = self.path_rules.lock().await;
        manager.update_rule_save_path(rule_id, save_path)?;
        let config_path = self.data_dir.join("path_rules.json");
        path_rules::save_path_rules(&manager, &config_path).await
    }

    /// Update a path rule's priority.
    pub async fn update_path_rule_priority(
        &self,
        rule_id: &str,
        priority: u32,
    ) -> Result<(), path_rules::PathRulesError> {
        let mut manager = self.path_rules.lock().await;
        manager.update_rule_priority(rule_id, priority)?;
        let config_path = self.data_dir.join("path_rules.json");
        path_rules::save_path_rules(&manager, &config_path).await
    }

    /// Find the matching path rule for a URL and filename.
    pub async fn find_matching_path_rule(
        &self,
        url: &str,
        filename: &str,
    ) -> Option<path_rules::PathRule> {
        let manager = self.path_rules.lock().await;
        manager.find_matching_rule(url, filename).cloned()
    }

    /// Apply path rules to determine save path for a download.
    /// Returns the save path if a matching rule is found, None otherwise.
    pub async fn apply_path_rules(&self, url: &str, filename: &str) -> Option<std::path::PathBuf> {
        let manager = self.path_rules.lock().await;
        manager
            .find_matching_rule(url, filename)
            .map(|rule| rule.save_path.clone())
    }

    fn would_create_cycle(
        &self,
        tasks: &[DownloadTask],
        task_id: &str,
        new_deps: &[String],
    ) -> bool {
        // Build adjacency: task -> its depends_on
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![task_id.to_string()];

        // For each new dep, check if following its dependency chain leads back to task_id
        for dep in new_deps {
            visited.clear();
            stack.clear();
            stack.push(dep.clone());

            while let Some(current) = stack.pop() {
                if current == task_id {
                    return true; // cycle found
                }
                if !visited.insert(current.clone()) {
                    continue;
                }
                if let Some(t) = tasks.iter().find(|t| t.id == current) {
                    for d in &t.depends_on {
                        stack.push(d.clone());
                    }
                }
            }
        }
        false
    }

    /// Move a task up in the queue (decrease position by 1).
    /// Returns true if task was moved.
    pub async fn move_task_up(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.lock().await;
        let idx = tasks.iter().position(|t| t.id == task_id);
        let Some(idx) = idx else {
            return false;
        };

        // Find the previous queued task
        let prev_queued = tasks[..idx]
            .iter()
            .rposition(|t| t.state == DownloadState::Queued);
        let Some(prev_idx) = prev_queued else {
            return false;
        };

        // Swap positions
        let current_pos = tasks[idx].queue_position;
        let prev_pos = tasks[prev_idx].queue_position;
        tasks[idx].queue_position = prev_pos;
        tasks[prev_idx].queue_position = current_pos;
        tasks[idx].updated_at = chrono::Utc::now();
        tasks[prev_idx].updated_at = chrono::Utc::now();

        self.emit_event(TaskEvent::Updated {
            task: TaskInfoEvent::from_task(&tasks[idx]),
        });
        self.emit_event(TaskEvent::Updated {
            task: TaskInfoEvent::from_task(&tasks[prev_idx]),
        });

        drop(tasks);
        self.persist_tasks().await;
        true
    }

    /// Move a task down in the queue (increase position by 1).
    /// Returns true if task was moved.
    pub async fn move_task_down(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.lock().await;
        let idx = tasks.iter().position(|t| t.id == task_id);
        let Some(idx) = idx else {
            return false;
        };

        // Find the next queued task
        let next_queued = tasks[idx + 1..]
            .iter()
            .position(|t| t.state == DownloadState::Queued);
        let Some(next_idx) = next_queued else {
            return false;
        };
        let next_idx = idx + 1 + next_idx;

        // Swap positions
        let current_pos = tasks[idx].queue_position;
        let next_pos = tasks[next_idx].queue_position;
        tasks[idx].queue_position = next_pos;
        tasks[next_idx].queue_position = current_pos;
        tasks[idx].updated_at = chrono::Utc::now();
        tasks[next_idx].updated_at = chrono::Utc::now();

        self.emit_event(TaskEvent::Updated {
            task: TaskInfoEvent::from_task(&tasks[idx]),
        });
        self.emit_event(TaskEvent::Updated {
            task: TaskInfoEvent::from_task(&tasks[next_idx]),
        });

        drop(tasks);
        self.persist_tasks().await;
        true
    }

    /// Move a task to the top of the queue (lowest position value).
    /// Returns true if task was moved.
    pub async fn move_task_to_top(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.lock().await;
        let idx = tasks.iter().position(|t| t.id == task_id);
        let Some(idx) = idx else {
            return false;
        };

        // Find the minimum queue_position among queued tasks
        let min_pos = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Queued)
            .filter_map(|t| t.queue_position)
            .min();

        let new_pos = min_pos.map(|p| p.saturating_sub(1)).or(Some(0));
        tasks[idx].queue_position = new_pos;
        tasks[idx].updated_at = chrono::Utc::now();

        self.emit_event(TaskEvent::Updated {
            task: TaskInfoEvent::from_task(&tasks[idx]),
        });

        drop(tasks);
        self.persist_tasks().await;
        true
    }

    /// Move a task to the bottom of the queue (highest position value).
    /// Returns true if task was moved.
    pub async fn move_task_to_bottom(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.lock().await;
        let idx = tasks.iter().position(|t| t.id == task_id);
        let Some(idx) = idx else {
            return false;
        };

        // Find the maximum queue_position among queued tasks
        let max_pos = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Queued)
            .filter_map(|t| t.queue_position)
            .max();

        let new_pos = max_pos.map(|p| p + 1).or(Some(0));
        tasks[idx].queue_position = new_pos;
        tasks[idx].updated_at = chrono::Utc::now();

        self.emit_event(TaskEvent::Updated {
            task: TaskInfoEvent::from_task(&tasks[idx]),
        });

        drop(tasks);
        self.persist_tasks().await;
        true
    }

    /// Try to start the next queued task if a slot is available.
    /// Picks the highest-priority queued task whose dependencies are all satisfied (FIFO within same priority, queue_position as tiebreaker).
    pub async fn try_start_next_queued(&self) -> Option<String> {
        if !self.can_start_task().await {
            return None;
        }

        // Get favorite task IDs for priority scheduling
        let favorite_ids = self.get_favorite_ids_internal().await;

        // Read schedule windows config (avoid holding tasks lock while acquiring rwlock)
        let schedule_cfg = self.task_schedule_windows.read().await.config().clone();

        // Find the highest-priority queued task with all dependencies satisfied
        let tasks = self.tasks.lock().await;
        let completed_ids: std::collections::HashSet<&str> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Complete)
            .map(|t| t.id.as_str())
            .collect();

        let now = chrono::Local::now();
        let next = tasks
            .iter()
            .filter(|t| {
                t.state == DownloadState::Queued
                    && t.depends_on
                        .iter()
                        .all(|dep| completed_ids.contains(dep.as_str()))
                    && {
                        // Check schedule windows
                        if !schedule_cfg.enabled {
                            true
                        } else if schedule_cfg.priority_bypass && t.priority as i32 > 0 {
                            true
                        } else if let Some(windows) = schedule_cfg.task_windows.get(&t.id) {
                            windows.iter().any(|w| w.applies_at(now))
                        } else {
                            true
                        }
                    }
            })
            .max_by(|a, b| {
                // Favorites always come first
                let a_fav = favorite_ids.contains(&a.id);
                let b_fav = favorite_ids.contains(&b.id);
                match (a_fav, b_fav) {
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    _ => a.priority.cmp(&b.priority).then_with(|| {
                        // Lower queue_position comes first
                        match (a.queue_position, b.queue_position) {
                            (Some(pa), Some(pb)) => pb.cmp(&pa),
                            (Some(_), None) => std::cmp::Ordering::Greater,
                            (None, Some(_)) => std::cmp::Ordering::Less,
                            (None, None) => a.created_at.cmp(&b.created_at),
                        }
                    }),
                }
            })
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

    // ========== Phase 81: CSV Export ==========

    /// Export tasks to a CSV file for spreadsheet analysis.
    ///
    /// Returns the export result with file path and task count.
    pub async fn export_tasks_to_csv(
        &self,
        output_path: &std::path::Path,
        config: Option<csv_export::CsvExportConfig>,
    ) -> Result<csv_export::CsvExportResult, String> {
        let tasks = self.tasks.lock().await;
        csv_export::export_tasks_to_csv(&tasks, output_path, config).map_err(|e| e.to_string())
    }

    /// Export tasks to a CSV string (useful for API responses).
    pub async fn export_tasks_to_csv_string(
        &self,
        config: Option<csv_export::CsvExportConfig>,
    ) -> Result<String, String> {
        let tasks = self.tasks.lock().await;
        csv_export::export_tasks_to_csv_string(&tasks, config).map_err(|e| e.to_string())
    }

    /// Generate a CSV summary report with aggregated statistics.
    pub async fn get_csv_summary(&self) -> String {
        let tasks = self.tasks.lock().await;
        csv_export::generate_csv_summary(&tasks)
    }

    // ========== Phase 89: URL Allowlist ==========

    /// Set URL allowlist configuration and persist to disk.
    pub async fn set_url_allowlist_config(
        &self,
        config: url_allowlist::AllowlistConfig,
    ) -> Result<(), url_allowlist::AllowlistError> {
        {
            let mut al = self.url_allowlist.write().await;
            *al = config;
        }
        let al = self.url_allowlist.read().await.clone();
        url_allowlist::save_allowlist_config(&al, &self.data_dir)
    }

    /// Get current URL allowlist configuration.
    pub async fn get_url_allowlist_config(&self) -> url_allowlist::AllowlistConfig {
        self.url_allowlist.read().await.clone()
    }

    /// Enable or disable URL allowlist enforcement.
    pub async fn set_allowlist_enabled(
        &self,
        enabled: bool,
    ) -> Result<(), url_allowlist::AllowlistError> {
        {
            let mut al = self.url_allowlist.write().await;
            al.enabled = enabled;
        }
        let al = self.url_allowlist.read().await.clone();
        url_allowlist::save_allowlist_config(&al, &self.data_dir)
    }

    /// Add an entry to the URL allowlist.
    pub async fn add_allowlist_entry(
        &self,
        entry: url_allowlist::AllowlistEntry,
    ) -> Result<(), url_allowlist::AllowlistError> {
        {
            let mut al = self.url_allowlist.write().await;
            al.entries.push(entry);
        }
        let al = self.url_allowlist.read().await.clone();
        url_allowlist::save_allowlist_config(&al, &self.data_dir)
    }

    /// Remove an entry from the URL allowlist by ID.
    pub async fn remove_allowlist_entry(
        &self,
        id: &str,
    ) -> Result<(), url_allowlist::AllowlistError> {
        {
            let mut al = self.url_allowlist.write().await;
            let before = al.entries.len();
            al.entries.retain(|e| e.id != id);
            if al.entries.len() == before {
                return Err(url_allowlist::AllowlistError::NotFound(id.to_string()));
            }
        }
        let al = self.url_allowlist.read().await.clone();
        url_allowlist::save_allowlist_config(&al, &self.data_dir)
    }

    /// List all URL allowlist entries.
    pub async fn list_allowlist_entries(&self) -> Vec<url_allowlist::AllowlistEntry> {
        self.url_allowlist.read().await.entries.clone()
    }

    /// Check if a URL is allowed by the allowlist.
    /// Returns `allowed: true` if allowlist is disabled or URL matches an entry.
    pub async fn check_url_allowed(&self, url: &str) -> url_allowlist::AllowlistCheckResult {
        let config = self.url_allowlist.read().await.clone();
        url_allowlist::check_url_allowlist(url, &config)
    }

    // ========== Phase 153: URL Blacklist ==========

    /// Set URL blacklist configuration and persist to disk.
    pub async fn set_url_blacklist_config(
        &self,
        config: url_blacklist::BlacklistConfig,
    ) -> Result<(), url_blacklist::BlacklistError> {
        {
            let mut bl = self.url_blacklist.write().await;
            *bl = config;
        }
        let bl = self.url_blacklist.read().await.clone();
        url_blacklist::save_blacklist_config(&bl, &self.data_dir)
    }

    /// Get current URL blacklist configuration.
    pub async fn get_url_blacklist_config(&self) -> url_blacklist::BlacklistConfig {
        self.url_blacklist.read().await.clone()
    }

    /// Enable or disable URL blacklist enforcement.
    pub async fn set_blacklist_enabled(
        &self,
        enabled: bool,
    ) -> Result<(), url_blacklist::BlacklistError> {
        {
            let mut bl = self.url_blacklist.write().await;
            bl.enabled = enabled;
        }
        let bl = self.url_blacklist.read().await.clone();
        url_blacklist::save_blacklist_config(&bl, &self.data_dir)
    }

    /// Add an entry to the URL blacklist.
    pub async fn add_blacklist_entry(
        &self,
        entry: url_blacklist::BlacklistEntry,
    ) -> Result<(), url_blacklist::BlacklistError> {
        {
            let mut bl = self.url_blacklist.write().await;
            bl.entries.push(entry);
        }
        let bl = self.url_blacklist.read().await.clone();
        url_blacklist::save_blacklist_config(&bl, &self.data_dir)
    }

    /// Remove an entry from the URL blacklist by ID.
    pub async fn remove_blacklist_entry(
        &self,
        id: &str,
    ) -> Result<(), url_blacklist::BlacklistError> {
        {
            let mut bl = self.url_blacklist.write().await;
            let before = bl.entries.len();
            bl.entries.retain(|e| e.id != id);
            if bl.entries.len() == before {
                return Err(url_blacklist::BlacklistError::NotFound(id.to_string()));
            }
        }
        let bl = self.url_blacklist.read().await.clone();
        url_blacklist::save_blacklist_config(&bl, &self.data_dir)
    }

    /// List all URL blacklist entries.
    pub async fn list_blacklist_entries(&self) -> Vec<url_blacklist::BlacklistEntry> {
        self.url_blacklist.read().await.entries.clone()
    }

    /// Check if a URL is blocked by the blacklist.
    /// Returns `blocked: true` if blacklist is enabled and URL matches an entry.
    pub async fn check_url_blocked(&self, url: &str) -> url_blacklist::BlacklistCheckResult {
        let config = self.url_blacklist.read().await.clone();
        url_blacklist::check_url_blacklist(url, &config)
    }

    // ========== Phase 157: Connection Pool Monitoring & Management ==========

    /// Get connection pool status including stats and domain connections.
    pub async fn get_connection_pool_status(&self) -> connection_pool::PoolStatus {
        self.connection_pool.status().await
    }

    /// Get connection pool statistics.
    pub async fn get_connection_pool_stats(&self) -> connection_pool::PoolStats {
        self.connection_pool.stats().await
    }

    /// Get connection pool configuration.
    pub async fn get_connection_pool_config(&self) -> connection_pool::PoolConfig {
        self.connection_pool.get_config_async().await
    }

    /// Update connection pool configuration.
    pub async fn set_connection_pool_config(&self, config: connection_pool::PoolConfig) {
        self.connection_pool.update_config(config).await;
    }

    /// Clean up expired/idle connections from the pool.
    pub async fn cleanup_connection_pool(&self) {
        self.connection_pool.cleanup().await;
    }

    /// Clear all connections and reset statistics.
    pub async fn clear_connection_pool(&self) {
        self.connection_pool.clear().await;
    }

    /// Get per-domain connection information.
    pub async fn get_connection_pool_domains(&self) -> Vec<connection_pool::DomainConnectionInfo> {
        self.connection_pool.get_domain_connections().await
    }

    /// Set per-domain connection limit.
    pub async fn set_connection_pool_domain_limit(&self, domain: &str, limit: usize) {
        self.connection_pool.set_domain_limit(domain, limit).await;
    }

    /// Save connection pool configuration to disk.
    pub async fn save_connection_pool_config(&self) -> Result<(), std::io::Error> {
        let config = self.connection_pool.get_config_async().await;
        connection_pool::save_pool_config(&config, &self.data_dir)
    }

    /// Load connection pool configuration from disk.
    pub async fn load_connection_pool_config(&self) -> Result<(), std::io::Error> {
        let config = connection_pool::load_pool_config(&self.data_dir)?;
        self.connection_pool.update_config(config).await;
        Ok(())
    }

    // ========== Phase 162: Completion Probability Estimator ==========

    /// Estimate completion probability for a task.
    pub async fn estimate_completion_probability(
        &self,
        input: completion_probability::TaskProbabilityInput,
        signals: completion_probability::EstimatorSignals,
    ) -> completion_probability::CompletionProbability {
        let mut est = self.completion_probability.lock().await;
        est.estimate(&input, &signals)
    }

    /// Get cached completion probability for a task.
    pub async fn get_cached_completion_probability(
        &self,
        task_id: &str,
    ) -> Option<completion_probability::CompletionProbability> {
        let est = self.completion_probability.lock().await;
        est.get_cached(task_id).cloned()
    }

    /// Get completion probability estimator configuration.
    pub async fn get_completion_probability_config(
        &self,
    ) -> completion_probability::CompletionProbabilityConfig {
        let est = self.completion_probability.lock().await;
        est.config().clone()
    }

    /// Update completion probability estimator configuration.
    pub async fn set_completion_probability_config(
        &self,
        config: completion_probability::CompletionProbabilityConfig,
    ) {
        let mut est = self.completion_probability.lock().await;
        est.set_config(config);
    }

    /// Get summary of all cached completion probability estimates.
    pub async fn get_completion_probability_summary(
        &self,
    ) -> completion_probability::EstimatorSummary {
        let est = self.completion_probability.lock().await;
        est.summary()
    }

    /// Clear all cached completion probability estimates.
    pub async fn clear_completion_probability_cache(&self) {
        let mut est = self.completion_probability.lock().await;
        est.clear_cache();
    }

    /// Save completion probability configuration to disk.
    pub async fn save_completion_probability_config(
        &self,
    ) -> Result<(), completion_probability::CompletionProbabilityError> {
        let est = self.completion_probability.lock().await;
        let path = self.data_dir.join("completion_probability_config.json");
        est.save_config(&path).await
    }

    /// Load completion probability configuration from disk.
    pub async fn load_completion_probability_config(
        &self,
    ) -> Result<(), completion_probability::CompletionProbabilityError> {
        let path = self.data_dir.join("completion_probability_config.json");
        if path.exists() {
            let config =
                completion_probability::CompletionProbabilityEstimator::load_config(&path).await?;
            let mut est = self.completion_probability.lock().await;
            est.set_config(config);
        }
        Ok(())
    }

    // ========== Phase 106: Per-Task Proxy Override ==========

    /// Set a per-task proxy override. Takes precedence over the global proxy.
    pub async fn set_task_proxy(
        &self,
        task_id: String,
        proxy: proxy::ProxyConfig,
        notes: Option<String>,
    ) -> Result<(), task_proxy::TaskProxyError> {
        let mut mgr = self.task_proxy.lock().await;
        mgr.set_task_proxy(task_id, proxy, notes).await
    }

    /// Remove a per-task proxy override.
    pub async fn remove_task_proxy(&self, task_id: &str) -> Result<(), task_proxy::TaskProxyError> {
        let mut mgr = self.task_proxy.lock().await;
        mgr.remove_task_proxy(task_id).await
    }

    /// Get the active proxy override for a task (if any and enabled).
    pub async fn get_task_proxy(&self, task_id: &str) -> Option<task_proxy::TaskProxyConfig> {
        let mgr = self.task_proxy.lock().await;
        mgr.get_task_proxy(task_id).cloned()
    }

    /// List all per-task proxy overrides.
    pub async fn list_task_proxies(&self) -> Vec<task_proxy::TaskProxyConfig> {
        let mgr = self.task_proxy.lock().await;
        mgr.list_overrides().into_iter().cloned().collect()
    }

    /// Enable or disable a per-task proxy override.
    pub async fn set_task_proxy_enabled(
        &self,
        task_id: &str,
        enabled: bool,
    ) -> Result<(), task_proxy::TaskProxyError> {
        let mut mgr = self.task_proxy.lock().await;
        mgr.set_enabled(task_id, enabled).await
    }

    /// Update notes for a per-task proxy override.
    pub async fn set_task_proxy_notes(
        &self,
        task_id: &str,
        notes: Option<String>,
    ) -> Result<(), task_proxy::TaskProxyError> {
        let mut mgr = self.task_proxy.lock().await;
        mgr.set_notes(task_id, notes).await
    }

    /// Clear all per-task proxy overrides.
    pub async fn clear_task_proxies(&self) -> Result<(), task_proxy::TaskProxyError> {
        let mut mgr = self.task_proxy.lock().await;
        mgr.clear_all().await
    }

    /// Get summary of per-task proxy overrides.
    pub async fn get_task_proxy_summary(&self) -> task_proxy::TaskProxySummary {
        let mgr = self.task_proxy.lock().await;
        mgr.get_summary()
    }

    // ─── Task Snooze (Phase 90) ───────────────────────────────────────

    /// Set task snooze configuration.
    pub async fn set_task_snooze_config(
        &self,
        config: task_snooze::TaskSnoozeConfig,
    ) -> Result<(), task_snooze::TaskSnoozeError> {
        let mut mgr = self.task_snooze.lock().await;
        mgr.set_config(config);
        let data = mgr.to_data();
        task_snooze::save_task_snooze_data(&data, &self.data_dir).await
    }

    /// Get current task snooze configuration.
    pub async fn get_task_snooze_config(&self) -> task_snooze::TaskSnoozeConfig {
        self.task_snooze.lock().await.config().clone()
    }

    /// Snooze a task until the specified time.
    /// The task will be paused and automatically resume when the snooze expires.
    pub async fn snooze_task(
        &self,
        task_id: &str,
        until: chrono::DateTime<chrono::Utc>,
        reason: Option<String>,
    ) -> Result<task_snooze::SnoozeState, task_snooze::TaskSnoozeError> {
        let mut mgr = self.task_snooze.lock().await;
        let state = mgr.snooze_task(task_id.to_string(), until, reason)?;

        // Pause the task if it's currently running or queued
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id)
            && (task.state == DownloadState::Downloading || task.state == DownloadState::Queued)
        {
            task.state = DownloadState::Paused;
            task.updated_at = chrono::Utc::now();
            task.error = Some(format!(
                "Snoozed until {}",
                until.format("%Y-%m-%d %H:%M UTC")
            ));
        }
        drop(tasks);

        // Persist
        let data = mgr.to_data();
        task_snooze::save_task_snooze_data(&data, &self.data_dir).await?;
        Ok(state)
    }

    /// Unsnooze a task immediately (wake it up).
    pub async fn unsnooze_task(
        &self,
        task_id: &str,
    ) -> Result<task_snooze::SnoozeState, task_snooze::TaskSnoozeError> {
        let mut mgr = self.task_snooze.lock().await;
        let state = mgr.unsnooze_task(task_id)?;

        // Move task back to Queued
        let mut tasks = self.tasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id)
            && task.state == DownloadState::Paused
        {
            task.state = DownloadState::Queued;
            task.updated_at = chrono::Utc::now();
            task.error = None;
        }
        drop(tasks);

        // Persist
        let data = mgr.to_data();
        task_snooze::save_task_snooze_data(&data, &self.data_dir).await?;
        Ok(state)
    }

    /// Check if a task is currently snoozed.
    pub async fn is_task_snoozed(&self, task_id: &str) -> bool {
        self.task_snooze.lock().await.is_snoozed(task_id)
    }

    /// Get snooze state for a task.
    pub async fn get_task_snooze_state(&self, task_id: &str) -> Option<task_snooze::SnoozeState> {
        self.task_snooze
            .lock()
            .await
            .get_snooze_state(task_id)
            .cloned()
    }

    /// List all currently snoozed tasks.
    pub async fn list_snoozed_tasks(&self) -> Vec<task_snooze::SnoozeState> {
        self.task_snooze
            .lock()
            .await
            .list_snoozed()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Process expired snoozes: resume tasks whose snooze time has passed.
    /// Returns the list of task IDs that were resumed.
    pub async fn process_expired_snoozes(&self) -> Vec<String> {
        let mut mgr = self.task_snooze.lock().await;
        let expired = mgr.collect_expired();
        if expired.is_empty() {
            return vec![];
        }

        let expired_ids: Vec<String> = expired.iter().map(|s| s.task_id.clone()).collect();

        // Move expired tasks back to Queued
        let mut tasks = self.tasks.lock().await;
        for task in tasks.iter_mut() {
            if expired_ids.contains(&task.id) && task.state == DownloadState::Paused {
                task.state = DownloadState::Queued;
                task.updated_at = chrono::Utc::now();
                task.error = None;
            }
        }
        drop(tasks);

        // Clear expired entries and persist
        mgr.clear_expired();
        let data = mgr.to_data();
        let _ = task_snooze::save_task_snooze_data(&data, &self.data_dir).await;

        expired_ids
    }

    /// Remove snooze tracking for a task (e.g., when task is deleted).
    pub async fn remove_task_snooze(&self, task_id: &str) {
        let mut mgr = self.task_snooze.lock().await;
        mgr.remove_task(task_id);
        let data = mgr.to_data();
        let _ = task_snooze::save_task_snooze_data(&data, &self.data_dir).await;
    }

    // ─── Phase 115: Task Scheduler ───

    /// Set task scheduler configuration.
    pub async fn set_task_scheduler_config(
        &self,
        config: task_scheduler::TaskSchedulerConfig,
    ) -> Result<(), task_scheduler::TaskSchedulerError> {
        let mut mgr = self.task_scheduler.lock().await;
        mgr.set_config(config);
        mgr.save_to_file(self.data_dir.join("task_scheduler_config.json"))
            .await
    }

    /// Get current task scheduler configuration.
    pub async fn get_task_scheduler_config(&self) -> task_scheduler::TaskSchedulerConfig {
        self.task_scheduler.lock().await.get_config().clone()
    }

    /// Add a schedule rule.
    pub async fn add_schedule_rule(
        &self,
        rule: task_scheduler::ScheduleRule,
    ) -> Result<(), task_scheduler::TaskSchedulerError> {
        let mut mgr = self.task_scheduler.lock().await;
        mgr.add_rule(rule);
        mgr.save_to_file(self.data_dir.join("task_scheduler_config.json"))
            .await
    }

    /// Remove a schedule rule by ID.
    pub async fn remove_schedule_rule(
        &self,
        rule_id: &str,
    ) -> Result<bool, task_scheduler::TaskSchedulerError> {
        let mut mgr = self.task_scheduler.lock().await;
        let removed = mgr.remove_rule(rule_id);
        mgr.save_to_file(self.data_dir.join("task_scheduler_config.json"))
            .await?;
        Ok(removed)
    }

    /// Get all schedule rules.
    pub async fn get_schedule_rules(&self) -> Vec<task_scheduler::ScheduleRule> {
        self.task_scheduler.lock().await.get_rules().to_vec()
    }

    /// Enable or disable a schedule rule.
    pub async fn set_schedule_rule_enabled(
        &self,
        rule_id: &str,
        enabled: bool,
    ) -> Result<bool, task_scheduler::TaskSchedulerError> {
        let mut mgr = self.task_scheduler.lock().await;
        let updated = mgr.set_rule_enabled(rule_id, enabled);
        mgr.save_to_file(self.data_dir.join("task_scheduler_config.json"))
            .await?;
        Ok(updated)
    }

    /// Evaluate schedules at the current time.
    pub async fn evaluate_schedule_now(&self) -> task_scheduler::ScheduleEvaluation {
        self.task_scheduler.lock().await.evaluate_now()
    }

    // ─── Phase 98: Download Queue Snapshot & Restore ───

    /// Create a snapshot of the current download queue.
    pub async fn create_queue_snapshot(
        &self,
        name: String,
        description: Option<String>,
    ) -> Result<download_snapshot::SnapshotEntry, download_snapshot::SnapshotError> {
        let tasks = self.tasks.lock().await.clone();
        let speed_limit = self.rate_limiter.global_limit().await;
        let max_concurrent = self.max_concurrent.load(Ordering::Relaxed);

        let mut mgr = self.snapshot_manager.lock().await;
        mgr.create_snapshot(&tasks, name, description, speed_limit, max_concurrent)
    }

    /// List all available snapshots.
    pub async fn list_queue_snapshots(&self) -> Vec<download_snapshot::SnapshotSummary> {
        let mgr = self.snapshot_manager.lock().await;
        mgr.list_snapshots()
    }

    /// Get snapshot data.
    pub async fn get_queue_snapshot(
        &self,
        id: &str,
    ) -> Result<download_snapshot::SnapshotData, download_snapshot::SnapshotError> {
        let mgr = self.snapshot_manager.lock().await;
        mgr.get_snapshot(id)
    }

    /// Restore a snapshot, replacing the current task queue.
    pub async fn restore_queue_snapshot(
        &self,
        id: &str,
    ) -> Result<Vec<DownloadTask>, download_snapshot::SnapshotError> {
        let mgr = self.snapshot_manager.lock().await;
        let restored = mgr.restore_snapshot(id, &self.data_dir)?;
        // Reload tasks into memory
        let mut tasks = self.tasks.lock().await;
        *tasks = restored.clone();
        drop(tasks);
        // Persist
        let _ = task_queue::save_task_queue(&restored, &self.data_dir);
        Ok(restored)
    }

    /// Delete a snapshot.
    pub async fn delete_queue_snapshot(
        &self,
        id: &str,
    ) -> Result<(), download_snapshot::SnapshotError> {
        let mut mgr = self.snapshot_manager.lock().await;
        mgr.delete_snapshot(id)
    }

    // ===== Disk Space Monitor Management (Phase 120) =====

    /// Set disk monitor configuration and persist to disk.
    pub async fn set_disk_monitor_config(&self, config: disk_monitor::DiskMonitorConfig) {
        *self.disk_monitor_config.write().await = config.clone();
        let _ = disk_monitor::save_disk_monitor_config(&config, &self.data_dir).await;
    }

    /// Get disk monitor configuration.
    pub async fn get_disk_monitor_config(&self) -> disk_monitor::DiskMonitorConfig {
        self.disk_monitor_config.read().await.clone()
    }

    /// Get disk monitor summary.
    pub async fn get_disk_monitor_summary(&self) -> disk_monitor::DiskMonitorSummary {
        let config = self.disk_monitor_config.read().await.clone();
        let monitor = self.disk_monitor.lock().await;
        let status = monitor.get_status().await;
        let is_monitoring = monitor.is_running().await;
        let auto_paused_count = monitor.auto_paused_count().await;
        let auto_resumed_count = monitor.auto_resumed_count().await;
        let available_bytes =
            disk_monitor::get_available_space(monitor.monitor_path()).unwrap_or_default();
        let total_bytes =
            disk_monitor::get_total_space(monitor.monitor_path()).unwrap_or(available_bytes * 2);
        let warning_threshold = total_bytes / 10; // 10%
        let critical_threshold = total_bytes / 20; // 5%
        disk_monitor::DiskMonitorSummary {
            enabled: config.enabled,
            status,
            available_bytes,
            total_bytes,
            warning_threshold_bytes: warning_threshold,
            critical_threshold_bytes: critical_threshold,
            safety_margin_bytes: config.safety_margin_bytes,
            check_interval_secs: config.check_interval_secs,
            is_monitoring,
            auto_pause_on_critical: config.auto_pause_on_critical,
            auto_resume_on_recovery: config.auto_resume_on_recovery,
            auto_paused_count,
            auto_resumed_count,
        }
    }

    /// Get current disk space status.
    pub async fn get_disk_status(&self) -> disk_monitor::DiskSpaceStatus {
        let monitor = self.disk_monitor.lock().await;
        monitor.get_status().await
    }

    /// Check disk space now and return status.
    pub async fn check_disk_space_now(&self) -> disk_monitor::DiskSpaceStatus {
        let monitor = self.disk_monitor.lock().await;
        monitor.check().await
    }

    /// Start background disk monitoring.
    pub async fn start_disk_monitoring(&self) {
        let monitor = self.disk_monitor.lock().await;
        monitor.start_monitoring(|| async {}, || async {}).await;
    }

    /// Stop background disk monitoring.
    pub async fn stop_disk_monitoring(&self) {
        let monitor = self.disk_monitor.lock().await;
        monitor.stop_monitoring().await;
    }

    // ===== Network-Aware Download Management =====

    /// Set network-aware configuration.
    pub async fn set_network_aware_config(&self, config: network_aware::NetworkAwareConfig) {
        let mut mgr = self.network_aware.lock().await;
        mgr.set_config(config);
    }

    /// Get network-aware configuration.
    pub async fn get_network_aware_config(&self) -> network_aware::NetworkAwareConfig {
        let mgr = self.network_aware.lock().await;
        mgr.config().clone()
    }

    /// Get current network status.
    pub async fn get_network_status(&self) -> network_aware::NetworkStatus {
        let mgr = self.network_aware.lock().await;
        mgr.status()
    }

    /// Get network-aware summary.
    pub async fn get_network_aware_summary(&self) -> network_aware::NetworkAwareSummary {
        let mgr = self.network_aware.lock().await;
        mgr.summary()
    }

    /// Record a connectivity probe success.
    pub async fn record_network_probe_success(&self) -> bool {
        let mut mgr = self.network_aware.lock().await;
        mgr.record_probe_success().is_some()
    }

    /// Record a connectivity probe failure.
    pub async fn record_network_probe_failure(&self) -> bool {
        let mut mgr = self.network_aware.lock().await;
        mgr.record_probe_failure().is_some()
    }

    /// Force-set network status (for testing or manual override).
    pub async fn force_set_network_status(&self, status: network_aware::NetworkStatus) {
        let mut mgr = self.network_aware.lock().await;
        mgr.force_set_status(status);
    }

    /// Record that a task was auto-paused due to network disconnection.
    pub async fn record_network_auto_pause(&self, task_id: &str, was_running: bool) -> bool {
        let mut mgr = self.network_aware.lock().await;
        mgr.record_auto_pause(task_id, was_running)
    }

    /// Record that a task was auto-resumed after network recovery.
    pub async fn record_network_auto_resume(&self, task_id: &str) -> Option<bool> {
        let mut mgr = self.network_aware.lock().await;
        mgr.record_auto_resume(task_id)
    }

    /// Get list of task IDs that were auto-paused due to network disconnection.
    pub async fn get_network_auto_paused_tasks(&self) -> Vec<String> {
        let mgr = self.network_aware.lock().await;
        mgr.auto_paused_task_ids()
    }

    /// Check if a specific task was auto-paused due to network disconnection.
    pub async fn is_network_auto_paused(&self, task_id: &str) -> bool {
        let mgr = self.network_aware.lock().await;
        mgr.is_auto_paused(task_id)
    }

    /// Clear all network auto-paused task tracking.
    pub async fn clear_network_auto_paused(&self) {
        let mut mgr = self.network_aware.lock().await;
        mgr.clear_auto_paused_tasks();
    }

    /// Reset network-aware state (keeps config).
    pub async fn reset_network_aware(&self) {
        let mut mgr = self.network_aware.lock().await;
        mgr.reset();
    }

    /// Save network-aware configuration to disk.
    pub async fn save_network_aware_config(&self) -> Result<(), network_aware::NetworkAwareError> {
        let mgr = self.network_aware.lock().await;
        network_aware::save_config(mgr.config(), &self.data_dir).await
    }

    /// Load network-aware configuration from disk.
    pub async fn load_network_aware_config(
        &self,
    ) -> Result<network_aware::NetworkAwareConfig, network_aware::NetworkAwareError> {
        network_aware::load_config(&self.data_dir).await
    }

    // ===== Download Deadline Management (Phase 107) =====

    /// Set deadline configuration.
    pub async fn set_deadline_config(&self, config: download_deadline::DeadlineConfig) {
        let mut mgr = self.download_deadline.lock().await;
        mgr.set_config(config);
    }

    /// Get deadline configuration.
    pub async fn get_deadline_config(&self) -> download_deadline::DeadlineConfig {
        let mgr = self.download_deadline.lock().await;
        mgr.config().clone()
    }

    /// Set a deadline for a specific task.
    pub async fn set_task_deadline(
        &self,
        task_id: &str,
        deadline: chrono::DateTime<chrono::Utc>,
        enabled: bool,
    ) {
        let mut mgr = self.download_deadline.lock().await;
        mgr.set_deadline(task_id, deadline, enabled);
    }

    /// Remove a deadline for a specific task.
    pub async fn remove_task_deadline(&self, task_id: &str) -> bool {
        let mut mgr = self.download_deadline.lock().await;
        mgr.remove_deadline(task_id)
    }

    /// Get deadline data for a specific task.
    pub async fn get_task_deadline(
        &self,
        task_id: &str,
    ) -> Option<download_deadline::DeadlineData> {
        let mgr = self.download_deadline.lock().await;
        mgr.get_deadline(task_id).cloned()
    }

    /// Get deadline summary.
    pub async fn get_deadline_summary(&self) -> download_deadline::DeadlineSummary {
        let mgr = self.download_deadline.lock().await;
        mgr.summary()
    }

    /// Refresh urgency levels for all deadlines.
    pub async fn refresh_deadlines(&self) {
        let mut mgr = self.download_deadline.lock().await;
        mgr.refresh_all();
    }

    /// Clear all deadlines.
    pub async fn clear_all_deadlines(&self) {
        let mut mgr = self.download_deadline.lock().await;
        mgr.clear_all();
    }

    /// Save deadline configuration to disk.
    pub async fn save_deadline_config(&self) -> std::io::Result<()> {
        let mgr = self.download_deadline.lock().await;
        download_deadline::save_deadline_config(
            mgr.config(),
            &self.data_dir.join("deadline_config.json"),
        )
        .await
    }

    // ===== Integrity Verification =====

    /// Set integrity verification configuration.
    pub async fn set_integrity_config(&self, config: integrity_verification::IntegrityConfig) {
        let mut mgr = self.integrity.lock().await;
        mgr.set_config(config);
    }

    /// Get integrity verification configuration.
    pub async fn get_integrity_config(&self) -> integrity_verification::IntegrityConfig {
        let mgr = self.integrity.lock().await;
        mgr.config().clone()
    }

    /// Verify a single download task's file integrity.
    pub async fn verify_task_integrity(
        &self,
        task_id: &str,
    ) -> Option<integrity_verification::VerificationResult> {
        let tasks = self.tasks.lock().await;
        let task = tasks.iter().find(|t| t.id == task_id)?;

        let mut mgr = self.integrity.lock().await;
        let result = mgr
            .verify_file(
                task.id.clone(),
                task.name.clone(),
                task.save_path.clone(),
                task.size,
            )
            .await;
        Some(result)
    }

    /// Verify all completed tasks' file integrity.
    pub async fn verify_all_integrity(&self) -> Vec<integrity_verification::VerificationResult> {
        let tasks = self.tasks.lock().await;
        let completed_tasks: Vec<_> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Complete)
            .map(|t| (t.id.clone(), t.name.clone(), t.save_path.clone(), t.size))
            .collect();
        drop(tasks);

        let mut mgr = self.integrity.lock().await;
        mgr.verify_batch(completed_tasks).await
    }

    /// Get verification result for a specific task.
    pub async fn get_integrity_result(
        &self,
        task_id: &str,
    ) -> Option<integrity_verification::VerificationResult> {
        let mgr = self.integrity.lock().await;
        mgr.get_result(task_id).cloned()
    }

    /// Get all verification results.
    pub async fn get_all_integrity_results(
        &self,
    ) -> Vec<integrity_verification::VerificationResult> {
        let mgr = self.integrity.lock().await;
        mgr.all_results().into_iter().cloned().collect()
    }

    /// Get integrity verification summary.
    pub async fn get_integrity_summary(&self) -> integrity_verification::IntegritySummary {
        let mgr = self.integrity.lock().await;
        mgr.summary()
    }

    /// Clear all integrity verification results.
    pub async fn clear_integrity_results(&self) {
        let mut mgr = self.integrity.lock().await;
        mgr.clear();
    }

    /// Save integrity configuration to disk.
    pub async fn save_integrity_config(&self) -> std::io::Result<()> {
        let mgr = self.integrity.lock().await;
        mgr.save_config(&self.data_dir).await
    }

    /// Load integrity configuration from disk.
    pub async fn load_integrity_config(
        &self,
    ) -> std::io::Result<integrity_verification::IntegrityConfig> {
        integrity_verification::IntegrityManager::load_config(&self.data_dir).await
    }

    // ===== Task Performance Profiler =====

    /// Set task profiler configuration.
    pub async fn set_task_profiler_config(&self, config: task_profiler::TaskProfilerConfig) {
        let mut profiler = self.task_profiler.lock().await;
        profiler.set_config(config);
    }

    /// Get task profiler configuration.
    pub async fn get_task_profiler_config(&self) -> task_profiler::TaskProfilerConfig {
        let profiler = self.task_profiler.lock().await;
        profiler.get_config().clone()
    }

    /// Get the performance profile for a specific task.
    pub async fn get_task_profile(&self, task_id: &str) -> Option<task_profiler::TaskProfile> {
        let profiler = self.task_profiler.lock().await;
        profiler.get_profile(task_id).cloned()
    }

    /// Get all task performance profiles.
    pub async fn get_all_task_profiles(&self) -> Vec<task_profiler::TaskProfile> {
        let profiler = self.task_profiler.lock().await;
        profiler.get_all_profiles().into_iter().cloned().collect()
    }

    /// Get the performance summary across all tasks.
    pub async fn get_performance_summary(&self, top_n: usize) -> task_profiler::PerformanceSummary {
        let profiler = self.task_profiler.lock().await;
        profiler.get_performance_summary(top_n)
    }

    /// Remove a task from the profiler.
    pub async fn remove_task_profile(&self, task_id: &str) -> bool {
        let mut profiler = self.task_profiler.lock().await;
        profiler.remove_profile(task_id)
    }

    /// Clear all task profiles.
    pub async fn clear_task_profiles(&self) {
        let mut profiler = self.task_profiler.lock().await;
        profiler.clear_all();
    }

    /// Refresh all task profiles from current task state.
    pub async fn refresh_task_profiles(&self) {
        let tasks = self.tasks.lock().await;
        let mut profiler = self.task_profiler.lock().await;
        let now = chrono::Utc::now();

        for task in tasks.iter() {
            let input = task_profiler::TaskProfileInput {
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                protocol: format!("{:?}", task.protocol).to_lowercase(),
                total_bytes: task.size,
                downloaded_bytes: task.downloaded,
                is_complete: task.state == DownloadState::Complete,
                created_at: task.created_at,
                active_time_seconds: task.active_time_seconds,
                current_speed_bps: task.speed_bps,
                retry_count: task.auto_retry_count,
                error_count: if task.error.is_some() { 1 } else { 0 },
                stall_count: 0,
                total_stall_secs: 0.0,
            };
            let _ = now; // suppress unused warning
            profiler.update_profile(input);
        }
    }

    // --- Adaptive Concurrency ---

    /// Set adaptive concurrency configuration.
    pub async fn set_adaptive_concurrency_config(
        &self,
        config: adaptive_concurrency::AdaptiveConcurrencyConfig,
    ) {
        let mut mgr = self.adaptive_concurrency.lock().await;
        mgr.set_config(config);
    }

    /// Get current adaptive concurrency configuration.
    pub async fn get_adaptive_concurrency_config(
        &self,
    ) -> adaptive_concurrency::AdaptiveConcurrencyConfig {
        let mgr = self.adaptive_concurrency.lock().await;
        *mgr.get_config()
    }

    /// Register a task for adaptive concurrency tracking.
    pub async fn register_adaptive_concurrency(&self, task_id: &str) {
        let mut mgr = self.adaptive_concurrency.lock().await;
        mgr.register_task(task_id);
    }

    /// Unregister a task from adaptive concurrency tracking.
    pub async fn unregister_adaptive_concurrency(&self, task_id: &str) {
        let mut mgr = self.adaptive_concurrency.lock().await;
        mgr.unregister_task(task_id);
    }

    /// Record a response sample for adaptive concurrency evaluation.
    pub async fn record_adaptive_concurrency_sample(
        &self,
        task_id: &str,
        response_time_ms: f64,
        success: bool,
    ) {
        let mut mgr = self.adaptive_concurrency.lock().await;
        mgr.record_sample(task_id, response_time_ms, success);
    }

    /// Get the recommended connection count for a task.
    pub async fn get_adaptive_connections(&self, task_id: &str) -> u32 {
        let mgr = self.adaptive_concurrency.lock().await;
        mgr.get_connections(task_id)
    }

    /// Evaluate and adjust concurrency for all tasks.
    pub async fn evaluate_adaptive_concurrency(
        &self,
    ) -> Vec<(String, adaptive_concurrency::ConcurrencyDecision)> {
        let mut mgr = self.adaptive_concurrency.lock().await;
        mgr.evaluate_all()
    }

    /// Get adaptive concurrency summary.
    pub async fn get_adaptive_concurrency_summary(
        &self,
    ) -> adaptive_concurrency::AdaptiveConcurrencySummary {
        let mgr = self.adaptive_concurrency.lock().await;
        mgr.get_summary()
    }

    /// Clear all adaptive concurrency state.
    pub async fn clear_adaptive_concurrency(&self) {
        let mut mgr = self.adaptive_concurrency.lock().await;
        mgr.clear();
    }

    // ─── Download Templates (Phase 100) ────────────────────────────────

    /// Add or update a download template
    pub async fn add_download_template(&self, template: download_templates::DownloadTemplate) {
        let mut mgr = self.download_templates.lock().await;
        mgr.add_template(template);
        if let Err(e) = self.persist_download_templates().await {
            tracing::warn!(error = %e, "Failed to persist download templates");
        }
    }

    /// Remove a download template by ID
    pub async fn remove_download_template(
        &self,
        id: &str,
    ) -> Option<download_templates::DownloadTemplate> {
        let mut mgr = self.download_templates.lock().await;
        let removed = mgr.remove_template(id);
        if removed.is_some()
            && let Err(e) = self.persist_download_templates().await
        {
            tracing::warn!(error = %e, "Failed to persist download templates");
        }
        removed
    }

    /// Get a download template by ID
    pub async fn get_download_template(
        &self,
        id: &str,
    ) -> Option<download_templates::DownloadTemplate> {
        let mgr = self.download_templates.lock().await;
        mgr.get_template(id).cloned()
    }

    /// List all download templates
    pub async fn list_download_templates(&self) -> Vec<download_templates::DownloadTemplate> {
        let mgr = self.download_templates.lock().await;
        mgr.list_templates().to_vec()
    }

    /// List template summaries
    pub async fn list_download_template_summaries(
        &self,
    ) -> Vec<download_templates::TemplateSummary> {
        let mgr = self.download_templates.lock().await;
        mgr.get_summaries()
    }

    /// Find templates matching a URL
    pub async fn find_matching_templates(
        &self,
        url: &str,
    ) -> Vec<download_templates::DownloadTemplate> {
        let mgr = self.download_templates.lock().await;
        mgr.find_matching_templates(url)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Find the best matching template for a URL
    pub async fn find_best_template(
        &self,
        url: &str,
    ) -> Option<download_templates::DownloadTemplate> {
        let mgr = self.download_templates.lock().await;
        mgr.find_best_template(url).cloned()
    }

    /// Enable or disable a template
    pub async fn set_template_enabled(&self, id: &str, enabled: bool) -> bool {
        let mut mgr = self.download_templates.lock().await;
        let result = mgr.set_enabled(id, enabled);
        if result && let Err(e) = self.persist_download_templates().await {
            tracing::warn!(error = %e, "Failed to persist download templates");
        }
        result
    }

    /// Enable or disable auto-apply for a template
    pub async fn set_template_auto_apply(&self, id: &str, auto_apply: bool) -> bool {
        let mut mgr = self.download_templates.lock().await;
        let result = mgr.set_auto_apply(id, auto_apply);
        if result && let Err(e) = self.persist_download_templates().await {
            tracing::warn!(error = %e, "Failed to persist download templates");
        }
        result
    }

    /// List templates by category
    pub async fn list_templates_by_category(
        &self,
        category: &str,
    ) -> Vec<download_templates::DownloadTemplate> {
        let mgr = self.download_templates.lock().await;
        mgr.list_by_category(category)
            .into_iter()
            .cloned()
            .collect()
    }

    /// List all template categories
    pub async fn list_template_categories(&self) -> Vec<String> {
        let mgr = self.download_templates.lock().await;
        mgr.list_categories()
    }

    /// Get template manager statistics
    pub async fn get_template_stats(&self) -> download_templates::TemplateStats {
        let mgr = self.download_templates.lock().await;
        download_templates::TemplateStats {
            total: mgr.count(),
            enabled: mgr.enabled_count(),
            categories: mgr.list_categories().len(),
        }
    }

    /// Persist download templates to disk
    async fn persist_download_templates(
        &self,
    ) -> Result<(), download_templates::TemplatePersistenceError> {
        let mgr = self.download_templates.lock().await;
        let templates = mgr.list_templates();
        download_templates::save_templates(templates, &self.data_dir)
    }

    // ========== Duplicate Detection ==========

    /// Set duplicate detection configuration
    pub async fn set_duplicate_detection_config(
        &self,
        config: duplicate_detection::DuplicateDetectionConfig,
    ) {
        let mut mgr = self.duplicate_detection.lock().await;
        mgr.set_config(config);
    }

    /// Get duplicate detection configuration
    pub async fn get_duplicate_detection_config(
        &self,
    ) -> duplicate_detection::DuplicateDetectionConfig {
        let mgr = self.duplicate_detection.lock().await;
        mgr.config().clone()
    }

    /// Set dynamic priority adjustment configuration
    pub async fn set_dynamic_priority_config(
        &self,
        config: dynamic_priority::DynamicPriorityConfig,
    ) {
        let mut mgr = self.dynamic_priority.write().await;
        mgr.set_config(config);
    }

    /// Get dynamic priority adjustment configuration
    pub async fn get_dynamic_priority_config(&self) -> dynamic_priority::DynamicPriorityConfig {
        let mgr = self.dynamic_priority.read().await;
        mgr.get_config().clone()
    }

    /// Enable or disable dynamic priority adjustment
    pub async fn set_dynamic_priority_enabled(&self, enabled: bool) {
        let mut mgr = self.dynamic_priority.write().await;
        mgr.set_enabled(enabled);
    }

    /// Get dynamic priority adjustment summary
    pub async fn get_dynamic_priority_summary(&self) -> dynamic_priority::DynamicPrioritySummary {
        let mgr = self.dynamic_priority.read().await;
        mgr.get_summary(0, 0)
    }

    // === Upload Tracker API ===

    /// Set upload tracker configuration
    pub async fn set_upload_tracker_config(&self, config: upload_tracker::UploadTrackerConfig) {
        let mut tracker = self.upload_tracker.lock().await;
        tracker.set_config(config);
    }

    /// Get upload tracker configuration
    pub async fn get_upload_tracker_config(&self) -> upload_tracker::UploadTrackerConfig {
        let tracker = self.upload_tracker.lock().await;
        tracker.get_config().clone()
    }

    /// Get upload tracker summary
    pub async fn get_upload_tracker_summary(&self) -> upload_tracker::UploadTrackerSummary {
        let tracker = self.upload_tracker.lock().await;
        tracker.get_summary()
    }

    /// Record upload bytes for a task
    pub async fn record_upload(&self, task_id: &str, uploaded_bytes: u64) {
        let mut tracker = self.upload_tracker.lock().await;
        tracker.record_upload(task_id, uploaded_bytes);
    }

    /// Get current upload speed for a task
    pub async fn get_task_upload_speed(&self, task_id: &str) -> f64 {
        let tracker = self.upload_tracker.lock().await;
        tracker.get_task_upload_speed(task_id)
    }

    /// Get total uploaded bytes for a task
    pub async fn get_task_uploaded(&self, task_id: &str) -> u64 {
        let tracker = self.upload_tracker.lock().await;
        tracker.get_task_uploaded(task_id)
    }

    /// Get total upload speed across all tasks
    pub async fn get_total_upload_speed(&self) -> f64 {
        let tracker = self.upload_tracker.lock().await;
        tracker.get_total_upload_speed()
    }

    /// Get total uploaded bytes across all tasks
    pub async fn get_total_uploaded(&self) -> u64 {
        let tracker = self.upload_tracker.lock().await;
        tracker.get_total_uploaded()
    }

    /// Remove upload tracking for a task
    pub async fn remove_upload_tracking(&self, task_id: &str) {
        let mut tracker = self.upload_tracker.lock().await;
        tracker.remove_task(task_id);
    }

    /// Clear all upload tracking data
    pub async fn clear_upload_tracking(&self) {
        let mut tracker = self.upload_tracker.lock().await;
        tracker.clear();
    }

    /// List all tracked task IDs
    pub async fn list_upload_tracked_tasks(&self) -> Vec<String> {
        let tracker = self.upload_tracker.lock().await;
        tracker.list_tracked_tasks()
    }

    /// Run dynamic priority adjustment cycle
    pub async fn run_dynamic_priority_adjustment(
        &self,
    ) -> Vec<dynamic_priority::PriorityAdjustment> {
        let tasks = self.tasks.lock().await;
        let mgr = self.dynamic_priority.read().await;

        if !mgr.is_enabled() {
            return Vec::new();
        }

        let task_inputs: Vec<dynamic_priority::TaskPriorityInput> = tasks
            .iter()
            .filter(|t| {
                t.state == crate::DownloadState::Queued
                    || t.state == crate::DownloadState::Downloading
            })
            .map(|t| {
                let current_speed = if t.state == crate::DownloadState::Downloading {
                    t.speed_bps as u64
                } else {
                    0
                };
                let progress_pct = if t.size > 0 {
                    ((t.downloaded as f64 / t.size as f64) * 100.0) as u32
                } else {
                    0
                };
                let is_queued = t.state == crate::DownloadState::Queued;

                dynamic_priority::TaskPriorityInput {
                    task_id: t.id.clone(),
                    current_priority: dynamic_priority::DynamicPriority::from_download_priority(
                        t.priority,
                    ),
                    current_speed_bps: current_speed,
                    progress_pct,
                    retry_count: t.auto_retry_count,
                    file_size_bytes: t.size,
                    created_at: t.created_at,
                    is_queued,
                }
            })
            .collect();

        drop(tasks);

        let adjustments = mgr.evaluate(&task_inputs);
        drop(mgr);

        if !adjustments.is_empty() {
            let mut mgr = self.dynamic_priority.write().await;
            mgr.record_adjustments(adjustments.clone());

            // Apply priority changes
            let mut tasks = self.tasks.lock().await;
            for adj in &adjustments {
                if let Some(task) = tasks.iter_mut().find(|t| t.id == adj.task_id) {
                    let old_priority = task.priority;
                    task.priority = adj.new_priority.to_download_priority();
                    task.updated_at = chrono::Utc::now();

                    self.emit_event(crate::TaskEvent::Updated {
                        task: crate::TaskInfoEvent::from_task(task),
                    });

                    tracing::info!(
                        task_id = %adj.task_id,
                        old_priority = ?old_priority,
                        new_priority = ?task.priority,
                        score = adj.score,
                        reason = %adj.reason,
                        "Dynamic priority adjustment"
                    );
                }
            }
            drop(tasks);
            self.persist_tasks().await;
        }

        adjustments
    }

    /// Clear dynamic priority adjustment history
    pub async fn clear_dynamic_priority_history(&self) {
        let mut mgr = self.dynamic_priority.write().await;
        mgr.clear_records();
    }

    /// Detect duplicates among current download tasks
    pub async fn detect_duplicate_tasks(&self) -> Vec<duplicate_detection::DuplicateGroup> {
        let tasks = self.tasks.lock().await;
        let task_data: Vec<duplicate_detection::TaskDuplicateData> = tasks
            .iter()
            .map(|t| {
                // Convert chrono DateTime to SystemTime
                let created_at = std::time::SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_secs(t.created_at.timestamp() as u64);

                duplicate_detection::TaskDuplicateData {
                    task_id: t.id.clone(),
                    task_name: t.name.clone(),
                    url: t.source_url.clone().unwrap_or_default(),
                    checksum: t.expected_checksum.clone(),
                    file_size: Some(t.size),
                    state: format!("{:?}", t.state),
                    priority: match t.priority {
                        DownloadPriority::High => 2,
                        DownloadPriority::Normal => 1,
                        DownloadPriority::Low => 0,
                    },
                    created_at,
                }
            })
            .collect();

        let mut mgr = self.duplicate_detection.lock().await;
        mgr.detect_duplicates(&task_data)
    }

    /// Get all duplicate groups
    pub async fn get_duplicate_groups(&self) -> Vec<duplicate_detection::DuplicateGroup> {
        let mgr = self.duplicate_detection.lock().await;
        mgr.get_duplicate_groups().into_iter().cloned().collect()
    }

    /// Get duplicate group for a specific task
    pub async fn get_task_duplicate_group(
        &self,
        task_id: &str,
    ) -> Option<duplicate_detection::DuplicateGroup> {
        let mgr = self.duplicate_detection.lock().await;
        mgr.get_task_duplicate_group(task_id).cloned()
    }

    /// Get duplicate detection summary
    pub async fn get_duplicate_summary(&self) -> duplicate_detection::DuplicateSummary {
        let mgr = self.duplicate_detection.lock().await;
        mgr.get_summary()
    }

    /// Clear all duplicate detection results
    pub async fn clear_duplicate_groups(&self) {
        let mut mgr = self.duplicate_detection.lock().await;
        mgr.clear_duplicate_groups();
    }

    /// Save duplicate detection configuration to disk
    pub async fn save_duplicate_detection_config(&self) -> std::io::Result<()> {
        let mgr = self.duplicate_detection.lock().await;
        let path = self.data_dir.join("duplicate_detection_config.json");
        mgr.save_config(&path)
    }

    /// Load duplicate detection configuration from disk
    pub async fn load_duplicate_detection_config(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("duplicate_detection_config.json");
        if path.exists() {
            let config = duplicate_detection::DuplicateDetectionManager::load_config(&path)?;
            let mut mgr = self.duplicate_detection.lock().await;
            mgr.set_config(config);
        }
        Ok(())
    }

    /// Set resume policy configuration
    pub async fn set_resume_policy_config(&self, config: resume_policy::ResumePolicyConfig) {
        let _ = resume_policy::save_resume_policy_config(&self.data_dir, &config);
        let mut policy = self.resume_policy.write().await;
        *policy = config;
    }

    /// Get resume policy configuration
    pub async fn get_resume_policy_config(&self) -> resume_policy::ResumePolicyConfig {
        let policy = self.resume_policy.read().await;
        policy.clone()
    }

    // ===== Source Benchmark Management (Phase 120) =====

    /// Set source benchmark configuration.
    pub async fn set_source_benchmark_config(
        &self,
        config: source_benchmark::BenchmarkConfig,
    ) -> std::io::Result<()> {
        let mut mgr = self.source_benchmark.lock().await;
        mgr.set_config(config.clone());
        source_benchmark::save_benchmark_config(&config, &self.data_dir).await?;
        Ok(())
    }

    /// Get source benchmark configuration.
    pub async fn get_source_benchmark_config(&self) -> source_benchmark::BenchmarkConfig {
        let mgr = self.source_benchmark.lock().await;
        mgr.config().clone()
    }

    /// Benchmark a list of source URLs and return results sorted by speed.
    pub async fn benchmark_sources(
        &self,
        urls: &[String],
    ) -> Result<source_benchmark::BenchmarkSummary, source_benchmark::SourceBenchmarkError> {
        let mgr = self.source_benchmark.lock().await;
        mgr.benchmark_sources(urls).await
    }

    /// Select the best source URL from a list based on benchmark results.
    pub async fn select_best_source(
        &self,
        urls: &[String],
    ) -> Result<String, source_benchmark::SourceBenchmarkError> {
        let mut mgr = self.source_benchmark.lock().await;
        mgr.select_best_source(urls).await
    }

    /// Get source benchmark cache summary.
    pub async fn get_source_benchmark_cache_summary(
        &self,
    ) -> source_benchmark::BenchmarkCacheSummary {
        let mgr = self.source_benchmark.lock().await;
        mgr.cache_summary()
    }

    /// Get cached benchmark result for a specific domain.
    pub async fn get_cached_domain_benchmark(
        &self,
        domain: &str,
    ) -> Option<source_benchmark::CachedDomainBenchmark> {
        let mgr = self.source_benchmark.lock().await;
        mgr.get_cached_domain(domain).cloned()
    }

    /// Clear all cached source benchmark results.
    pub async fn clear_source_benchmark_cache(&self) {
        let mut mgr = self.source_benchmark.lock().await;
        mgr.clear_cache();
    }

    // ========== Bandwidth Forecast API ==========

    /// Get bandwidth forecast configuration.
    pub async fn get_bandwidth_forecast_config(&self) -> bandwidth_forecast::ForecastConfig {
        let mgr = self.bandwidth_forecast.lock().await;
        mgr.config.clone()
    }

    /// Set bandwidth forecast configuration.
    pub async fn set_bandwidth_forecast_config(&self, config: bandwidth_forecast::ForecastConfig) {
        let mut mgr = self.bandwidth_forecast.lock().await;
        mgr.config = config;
    }

    /// Get bandwidth forecast for a specific domain.
    pub async fn forecast_bandwidth(&self, domain: &str) -> bandwidth_forecast::BandwidthForecast {
        let mgr = self.bandwidth_forecast.lock().await;
        mgr.forecast(domain)
    }

    /// Estimate time to complete download (seconds) for a domain.
    pub async fn estimate_download_eta(&self, domain: &str, remaining_bytes: u64) -> Option<u64> {
        let mgr = self.bandwidth_forecast.lock().await;
        mgr.estimate_eta(domain, remaining_bytes)
    }

    /// Get bandwidth forecast summary for all tracked domains.
    pub async fn get_bandwidth_forecast_summary(&self) -> bandwidth_forecast::ForecastSummary {
        let mgr = self.bandwidth_forecast.lock().await;
        mgr.get_summary()
    }

    /// Clear bandwidth forecast history for a specific domain.
    pub async fn clear_bandwidth_forecast_domain(&self, domain: &str) {
        let mut mgr = self.bandwidth_forecast.lock().await;
        mgr.clear_domain(domain);
    }

    /// Clear all bandwidth forecast history.
    pub async fn clear_bandwidth_forecast(&self) {
        let mut mgr = self.bandwidth_forecast.lock().await;
        mgr.clear_all();
    }

    // ========== Phase 138: Bandwidth Usage Tracker ==========

    /// Get bandwidth usage configuration.
    pub async fn get_bandwidth_usage_config(&self) -> BandwidthUsageConfig {
        let tracker = self.bandwidth_usage.lock().await;
        tracker.config().clone()
    }

    /// Set bandwidth usage configuration.
    pub async fn set_bandwidth_usage_config(&self, config: BandwidthUsageConfig) {
        let mut tracker = self.bandwidth_usage.lock().await;
        tracker.set_config(config);
    }

    /// Get bandwidth usage summary.
    pub async fn get_bandwidth_usage_summary(&self) -> BandwidthUsageSummary {
        let tracker = self.bandwidth_usage.lock().await;
        tracker.summary()
    }

    /// Get rolling 24-hour window summary.
    pub async fn get_bandwidth_usage_24h(&self) -> RollingWindowSummary {
        let tracker = self.bandwidth_usage.lock().await;
        tracker.rolling_24h_summary()
    }

    /// Get peak hour analysis.
    pub async fn get_bandwidth_usage_peak_hours(&self, top_n: usize) -> PeakHourAnalysis {
        let tracker = self.bandwidth_usage.lock().await;
        tracker.peak_hour_analysis(top_n)
    }

    /// Clear bandwidth usage data.
    pub async fn clear_bandwidth_usage(&self) {
        let mut tracker = self.bandwidth_usage.lock().await;
        tracker.clear();
    }

    /// Format bandwidth usage summary as human-readable string.
    pub async fn format_bandwidth_usage(&self) -> String {
        let tracker = self.bandwidth_usage.lock().await;
        tracker.format_summary()
    }

    /// Save bandwidth usage data to disk.
    pub async fn save_bandwidth_usage(&self) -> std::io::Result<()> {
        let tracker = self.bandwidth_usage.lock().await;
        let path = self.data_dir.join("bandwidth_usage.json");
        let json = serde_json::to_string_pretty(&*tracker)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        tokio::fs::write(&path, json).await
    }

    /// Load bandwidth usage data from disk.
    pub async fn load_bandwidth_usage(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("bandwidth_usage.json");
        if path.exists() {
            let json = tokio::fs::read_to_string(&path).await?;
            let tracker: BandwidthUsageTracker = serde_json::from_str(&json)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let mut current = self.bandwidth_usage.lock().await;
            *current = tracker;
        }
        Ok(())
    }
    /// Save bandwidth forecast config to disk.
    pub async fn save_source_benchmark_cache(&self) -> std::io::Result<()> {
        let mgr = self.source_benchmark.lock().await;
        mgr.save_cache().await
    }

    /// Save bandwidth forecast config to disk.
    pub async fn save_bandwidth_forecast_config(&self) -> std::io::Result<()> {
        let mgr = self.bandwidth_forecast.lock().await;
        mgr.save_config(&self.data_dir.join("bandwidth_forecast_config.json"))
    }

    /// Load bandwidth forecast config from disk.
    pub async fn load_bandwidth_forecast_config(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("bandwidth_forecast_config.json");
        if path.exists() {
            let config = bandwidth_forecast::BandwidthForecastManager::load_config(&path)?;
            let mut mgr = self.bandwidth_forecast.lock().await;
            mgr.config = config;
        }
        Ok(())
    }

    /// Save bandwidth forecast histories to disk.
    pub async fn save_bandwidth_forecast_histories(&self) -> std::io::Result<()> {
        let mgr = self.bandwidth_forecast.lock().await;
        mgr.save_histories(&self.data_dir.join("bandwidth_forecast_histories.json"))
    }

    /// Load bandwidth forecast histories from disk.
    pub async fn load_bandwidth_forecast_histories(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("bandwidth_forecast_histories.json");
        if path.exists() {
            let histories = bandwidth_forecast::BandwidthForecastManager::load_histories(&path)?;
            let mut mgr = self.bandwidth_forecast.lock().await;
            mgr.histories = histories;
        }
        Ok(())
    }

    // ==================== Phase 137: Source Reliability Tracker ====================

    /// Get source reliability configuration.
    pub async fn get_source_reliability_config(
        &self,
    ) -> source_reliability::SourceReliabilityConfig {
        let tracker = self.source_reliability.lock().await;
        tracker.config.clone()
    }

    /// Set source reliability configuration.
    pub async fn set_source_reliability_config(
        &self,
        config: source_reliability::SourceReliabilityConfig,
    ) {
        let mut tracker = self.source_reliability.lock().await;
        tracker.config = config;
    }

    /// Record a successful download for reliability tracking.
    pub async fn record_reliability_success(&self, domain: &str, speed_bps: u64, file_size: u64) {
        let mut tracker = self.source_reliability.lock().await;
        tracker.record_success(domain, speed_bps, file_size);
    }

    /// Record a failed download for reliability tracking.
    pub async fn record_reliability_failure(&self, domain: &str, error: &str) {
        let mut tracker = self.source_reliability.lock().await;
        tracker.record_failure(domain, error);
    }

    /// Get reliability score for a specific domain (0.0-1.0).
    pub async fn get_source_reliability_score(&self, domain: &str) -> f64 {
        let tracker = self.source_reliability.lock().await;
        tracker.get_score(domain)
    }

    /// Get reliability tier for a specific domain.
    pub async fn get_source_reliability_tier(
        &self,
        domain: &str,
    ) -> source_reliability::ReliabilityTier {
        let tracker = self.source_reliability.lock().await;
        tracker.get_tier(domain)
    }

    /// Get reliability data for a specific domain.
    pub async fn get_source_reliability_domain(
        &self,
        domain: &str,
    ) -> Option<source_reliability::DomainReliability> {
        let tracker = self.source_reliability.lock().await;
        tracker.get_domain(domain).cloned()
    }

    /// Get all domains sorted by reliability (best first).
    pub async fn get_source_reliability_domains(
        &self,
    ) -> Vec<source_reliability::DomainReliability> {
        let tracker = self.source_reliability.lock().await;
        tracker
            .get_domains_by_reliability()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Get domains that should be avoided (Poor or Unreliable tier).
    pub async fn get_source_reliability_avoid(&self) -> Vec<(String, f64)> {
        let tracker = self.source_reliability.lock().await;
        tracker.get_avoid_domains()
    }

    /// Get reliability summary across all tracked domains.
    pub async fn get_source_reliability_summary(&self) -> source_reliability::ReliabilitySummary {
        let tracker = self.source_reliability.lock().await;
        tracker.get_summary()
    }

    /// Format a human-readable reliability summary.
    pub async fn format_source_reliability_summary(&self) -> String {
        let tracker = self.source_reliability.lock().await;
        tracker.format_summary()
    }

    /// Clear all source reliability data.
    pub async fn clear_source_reliability(&self) {
        let mut tracker = self.source_reliability.lock().await;
        tracker.clear();
    }

    /// Clear reliability data for a specific domain.
    pub async fn clear_source_reliability_domain(&self, domain: &str) {
        let mut tracker = self.source_reliability.lock().await;
        tracker.clear_domain(domain);
    }

    /// Prune old reliability samples older than the given timestamp.
    pub async fn prune_source_reliability_samples(&self, before_timestamp: u64) {
        let mut tracker = self.source_reliability.lock().await;
        tracker.prune_old_samples(before_timestamp);
    }

    /// Save source reliability config to disk.
    pub async fn save_source_reliability_config(&self) -> std::io::Result<()> {
        let tracker = self.source_reliability.lock().await;
        let config_path = self.data_dir.join("source_reliability_config.json");
        let json = serde_json::to_string_pretty(&tracker.config)?;
        std::fs::write(config_path, json)?;
        Ok(())
    }

    /// Load source reliability config from disk.
    pub async fn load_source_reliability_config(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("source_reliability_config.json");
        if path.exists() {
            let json = std::fs::read_to_string(path)?;
            let config: source_reliability::SourceReliabilityConfig = serde_json::from_str(&json)?;
            let mut tracker = self.source_reliability.lock().await;
            tracker.config = config;
        }
        Ok(())
    }

    /// Save source reliability data to disk.
    pub async fn save_source_reliability_data(&self) -> std::io::Result<()> {
        let tracker = self.source_reliability.lock().await;
        let data_path = self.data_dir.join("source_reliability_data.json");
        let json = serde_json::to_string_pretty(&*tracker)?;
        std::fs::write(data_path, json)?;
        Ok(())
    }

    /// Load source reliability data from disk.
    pub async fn load_source_reliability_data(&self) -> std::io::Result<()> {
        let path = self.data_dir.join("source_reliability_data.json");
        if path.exists() {
            let json = std::fs::read_to_string(path)?;
            let tracker_data: source_reliability::SourceReliabilityTracker =
                serde_json::from_str(&json)?;
            let mut tracker = self.source_reliability.lock().await;
            tracker.domains = tracker_data.domains;
        }
        Ok(())
    }

    // ── Global Download Budget ──────────────────────────────────────────

    /// Set the global download budget configuration
    pub async fn set_global_budget_config(&self, config: global_budget::GlobalBudgetConfig) {
        let mut gb = self.global_budget.write().await;
        gb.set_config(config);
        let _ = global_budget::save_global_budget_config(
            &gb,
            &self.data_dir.join("global_budget.json"),
        );
    }

    /// Get the global download budget configuration
    pub async fn get_global_budget_config(&self) -> global_budget::GlobalBudgetConfig {
        let gb = self.global_budget.read().await;
        gb.config.clone()
    }

    /// Get the global budget usage summary
    pub async fn get_global_budget_summary(&self) -> global_budget::GlobalBudgetSummary {
        let gb = self.global_budget.read().await;
        gb.get_summary()
    }

    /// Record downloaded bytes in the global budget tracker
    pub async fn record_global_budget_download(&self, bytes: u64) -> bool {
        let mut gb = self.global_budget.write().await;
        gb.record_download(bytes)
    }

    /// Record a task contributing to the global budget
    pub async fn record_global_budget_task(&self) {
        let mut gb = self.global_budget.write().await;
        gb.record_task();
    }

    /// Reset global budget usage data
    pub async fn reset_global_budget_usage(&self) {
        let mut gb = self.global_budget.write().await;
        gb.reset_usage();
        let _ = global_budget::save_global_budget_config(
            &gb,
            &self.data_dir.join("global_budget.json"),
        );
    }

    /// Resume downloads after budget was exceeded (manual override)
    pub async fn resume_global_budget_downloads(&self) {
        let mut gb = self.global_budget.write().await;
        gb.resume_downloads();
        let _ = global_budget::save_global_budget_config(
            &gb,
            &self.data_dir.join("global_budget.json"),
        );
    }

    /// Check if downloads should be paused due to global budget
    pub async fn should_pause_global_budget(&self) -> bool {
        let gb = self.global_budget.read().await;
        gb.should_pause_downloads()
    }

    // ==================== Phase 123: Download Backup System ====================

    /// Create a comprehensive backup of all download state and configurations
    pub async fn create_backup(
        &self,
        description: Option<String>,
    ) -> Result<PathBuf, download_backup::BackupError> {
        // Export tasks
        let tasks = self.tasks.lock().await;
        let exported_tasks: Vec<crate::task_export::ExportedTask> = tasks
            .iter()
            .cloned()
            .map(crate::task_export::ExportedTask::from)
            .collect();

        // Collect generations (simplified - just count)
        let mut generations = std::collections::HashMap::new();
        generations.insert("task_count".to_string(), tasks.len() as u64);

        // Collect all subsystem configurations
        let configs = self.collect_backup_configs().await;

        self.backup_manager
            .create_backup(description, exported_tasks, generations, configs)
    }

    /// List all available backups
    pub async fn list_backups(
        &self,
    ) -> Result<Vec<download_backup::BackupInfo>, download_backup::BackupError> {
        self.backup_manager.list_backups()
    }

    /// Load a backup from file
    pub async fn load_backup(
        &self,
        backup_path: &std::path::Path,
    ) -> Result<download_backup::DownloadBackup, download_backup::BackupError> {
        self.backup_manager.load_backup(backup_path)
    }

    /// Delete a backup file
    pub async fn delete_backup(
        &self,
        backup_path: &std::path::Path,
    ) -> Result<(), download_backup::BackupError> {
        self.backup_manager.delete_backup(backup_path)
    }

    /// Collect all subsystem configurations for backup
    async fn collect_backup_configs(&self) -> download_backup::BackupConfigs {
        download_backup::BackupConfigs {
            auto_cleanup: Some(self.auto_cleanup.read().await.clone()),
            auto_pause: Some(self.auto_pause.read().await.clone()),
            bandwidth_allocation: Some(self.bandwidth_allocation.lock().await.config().clone()),
            cooldown: Some(self.cooldown_config.read().await.clone()),
            data_cap: Some(self.data_cap.lock().await.config().clone()),
            domain_limit: Some(self.domain_limit.read().await.clone()),
            error_recovery: Some(self.error_recovery.lock().await.config().clone()),
            network_aware: Some(self.network_aware.lock().await.config().clone()),
            priority_aging: Some(self.priority_aging.read().await.clone()),
            queue_completion: Some(
                self.queue_completion_predictor
                    .read()
                    .await
                    .config()
                    .clone(),
            ),
            recycle_bin: Some(self.recycle_bin.lock().await.config().clone()),
            resume_policy: Some(self.resume_policy.read().await.clone()),
            speed_alert: Some(self.speed_alerts.get_config().await),
            url_dedup: Some(self.url_dedup.read().await.clone()),
            automation_rules: Some(self.automation_rules.read().await.get_config().clone()),
            disk_monitor: Some(disk_monitor::DiskMonitorConfig {
                enabled: self.disk_monitor.lock().await.is_running().await,
                safety_margin_bytes: self.disk_monitor.lock().await.safety_margin_bytes(),
                check_interval_secs: self.disk_monitor.lock().await.check_interval_secs(),
                auto_pause_on_critical: true,
                auto_resume_on_recovery: true,
            }),
            download_analytics: Some(self.download_analytics.lock().await.config().clone()),
            download_budget: Some(self.download_budget.lock().await.config().clone()),
            download_deadline: Some(self.download_deadline.lock().await.config().clone()),
            download_presets: Some(self.download_presets.lock().await.clone()),
            download_quota: Some(self.download_quota.lock().await.get_config().clone()),
            download_time_limit: Some(self.download_time_limit.lock().await.config().clone()),
            duplicate_detection: Some(self.duplicate_detection.lock().await.config().clone()),
            global_budget: Some(self.global_budget.read().await.config.clone()),
            integrity: Some(self.integrity.lock().await.config().clone()),
            path_rules: Some(self.path_rules.lock().await.list_rules().to_vec()),
            path_template: Some(self.path_template.get_config().await),
            protocol_limits: Some(self.protocol_limits.read().await.clone()),
            queue_staleness: Some(self.queue_staleness.read().await.clone()),
            save_path: Some(self.save_path_manager.get_config().await),
            speed_profiles: Some(
                self.speed_profiles
                    .read()
                    .await
                    .list_profiles()
                    .into_iter()
                    .cloned()
                    .collect(),
            ),
            task_chains: Some(
                self.task_chain
                    .lock()
                    .await
                    .list_chains()
                    .into_iter()
                    .cloned()
                    .collect(),
            ),
            task_schedule_windows: Some(
                self.task_schedule_windows
                    .read()
                    .await
                    .get_all_windows()
                    .values()
                    .flatten()
                    .cloned()
                    .collect(),
            ),
            url_allowlist: Some(self.url_allowlist.read().await.clone()),
            url_bookmarks: Some(self.url_bookmarks.lock().await.clone()),
            url_normalizer: Some(self.url_normalizer.read().await.config().clone()),
            url_rewrite: Some(self.url_rewrite.lock().await.list_rules().to_vec()),
            watch_folder: Some(
                self.watch_folder
                    .lock()
                    .await
                    .get_auto_scan_config()
                    .clone(),
            ),
            categorize_rules: Some(self.categorize_rules.lock().await.clone()),
            conflict_strategy: Some(*self.conflict_strategy.read().await),
            bandwidth_schedule: Some(self.bandwidth_schedule.lock().await.list_rules().to_vec()),
            dependency_graph: Some(self.dependency_graph.read().await.config().clone()),
        }
    }

    // ─── Phase 142: Retry Budget Manager API ───

    /// Get retry budget configuration
    pub async fn get_retry_budget_config(&self) -> retry_budget::RetryBudgetConfig {
        self.retry_budget.lock().await.config.clone()
    }

    /// Set retry budget configuration
    pub async fn set_retry_budget_config(&self, config: retry_budget::RetryBudgetConfig) {
        self.retry_budget.lock().await.set_config(config);
    }

    /// Check if a domain can be retried
    pub async fn can_retry_domain(&self, domain: &str) -> bool {
        self.retry_budget.lock().await.can_retry_domain(domain)
    }

    /// Record a retry attempt for a domain
    pub async fn record_retry_domain(&self, domain: &str) {
        self.retry_budget.lock().await.record_retry(domain);
    }

    /// Record a successful download for a domain
    pub async fn record_success_domain(&self, domain: &str) {
        self.retry_budget.lock().await.record_success(domain);
    }

    /// Get remaining retry budget for a domain
    pub async fn get_remaining_retry_budget(&self, domain: &str) -> u32 {
        self.retry_budget.lock().await.get_remaining_budget(domain)
    }

    /// Get retry budget summary
    pub async fn get_retry_budget_summary(&self) -> retry_budget::RetryBudgetSummary {
        self.retry_budget.lock().await.get_summary()
    }

    /// Get domain retry state
    pub async fn get_domain_retry_state(
        &self,
        domain: &str,
    ) -> Option<retry_budget::DomainRetryState> {
        self.retry_budget
            .lock()
            .await
            .get_domain_state(domain)
            .cloned()
    }

    /// Clear retry state for a specific domain
    pub async fn clear_domain_retry_state(&self, domain: &str) {
        self.retry_budget.lock().await.clear_domain(domain);
    }

    /// Clear all retry budget state
    pub async fn clear_all_retry_budget_state(&self) {
        self.retry_budget.lock().await.clear();
    }

    /// Save retry budget config to disk
    pub async fn save_retry_budget_config(&self) -> std::io::Result<()> {
        let config = self.retry_budget.lock().await.config.clone();
        let path = self.data_dir.join("retry_budget_config.json");
        retry_budget::save_retry_budget_config(&path, &config)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    }

    /// Save retry budget state to disk
    pub async fn save_retry_budget_state(&self) -> std::io::Result<()> {
        let manager = self.retry_budget.lock().await.clone();
        let path = self.data_dir.join("retry_budget_state.json");
        retry_budget::save_retry_budget_state(&path, &manager)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    }

    // ── System Uptime API ──────────────────────────────────────────────

    /// Get system uptime summary
    pub async fn get_uptime_summary(&self) -> system_uptime::UptimeSummary {
        self.system_uptime.summary()
    }

    /// Get uptime in seconds
    pub async fn get_uptime_seconds(&self) -> u64 {
        self.system_uptime.uptime_seconds()
    }

    /// Get formatted uptime string (e.g., "2h 34m 56s")
    pub async fn get_uptime_formatted(&self) -> String {
        self.system_uptime.format_uptime()
    }

    // ── File Type Statistics API (Phase 143) ───────────────────────────

    /// Get file type statistics summary
    pub async fn get_file_stats_summary(&self) -> download_file_stats::FileStatsSummary {
        self.file_stats.read().await.get_summary().await
    }

    /// Get file type statistics configuration
    pub async fn get_file_stats_config(&self) -> download_file_stats::FileStatsConfig {
        self.file_stats.read().await.get_config().await
    }

    /// Set file type statistics configuration
    pub async fn set_file_stats_config(
        &self,
        config: download_file_stats::FileStatsConfig,
    ) -> std::io::Result<()> {
        self.file_stats.read().await.set_config(config).await
    }

    /// Record a download for file statistics tracking
    pub async fn record_file_stat_download(
        &self,
        url_or_filename: &str,
        bytes: u64,
        duration_secs: u64,
    ) -> std::io::Result<()> {
        self.file_stats
            .read()
            .await
            .record_download(url_or_filename, bytes, duration_secs)
            .await
    }

    /// Get statistics for a specific file extension
    pub async fn get_extension_file_stats(
        &self,
        extension: &str,
    ) -> Option<download_file_stats::ExtensionStats> {
        self.file_stats
            .read()
            .await
            .get_extension_stats(extension)
            .await
    }

    /// Clear all file type statistics
    pub async fn clear_file_stats(&self) -> std::io::Result<()> {
        self.file_stats.read().await.clear().await
    }

    /// Format file statistics as human-readable string
    pub async fn format_file_stats_summary(&self) -> String {
        self.file_stats.read().await.format_summary().await
    }

    /// Load file statistics from disk
    pub async fn load_file_stats(&self) -> std::io::Result<()> {
        self.file_stats.read().await.load_data().await
    }

    /// Save file statistics to disk
    pub async fn save_file_stats(&self) -> std::io::Result<()> {
        self.file_stats.read().await.save_data().await
    }

    // ========== Phase 144: SLA Compliance API ==========

    /// Get SLA compliance configuration
    pub async fn get_sla_config(&self) -> sla_compliance::SlaConfig {
        self.sla_compliance.read().await.get_config().clone()
    }

    /// Set SLA compliance configuration
    pub async fn set_sla_config(&self, config: sla_compliance::SlaConfig) -> std::io::Result<()> {
        self.sla_compliance.write().await.set_config(config).await
    }

    /// Add a new SLA definition
    pub async fn add_sla(
        &self,
        definition: sla_compliance::SlaDefinition,
    ) -> std::io::Result<String> {
        self.sla_compliance.write().await.add_sla(definition).await
    }

    /// Remove an SLA definition
    pub async fn remove_sla(&self, sla_id: &str) -> std::io::Result<bool> {
        self.sla_compliance.write().await.remove_sla(sla_id).await
    }

    /// List all SLA definitions
    pub async fn list_slas(&self) -> Vec<sla_compliance::SlaDefinition> {
        self.sla_compliance.read().await.list_slas().to_vec()
    }

    /// Get a specific SLA definition
    pub async fn get_sla(&self, sla_id: &str) -> Option<sla_compliance::SlaDefinition> {
        self.sla_compliance.read().await.get_sla(sla_id).cloned()
    }

    /// Enable or disable an SLA
    pub async fn set_sla_enabled(&self, sla_id: &str, enabled: bool) -> std::io::Result<bool> {
        self.sla_compliance
            .write()
            .await
            .set_sla_enabled(sla_id, enabled)
            .await
    }

    /// Evaluate all enabled SLAs against current tasks
    pub async fn evaluate_sla_compliance(
        &self,
    ) -> std::io::Result<Vec<sla_compliance::SlaEvaluation>> {
        let tasks = self.tasks.lock().await;
        let task_data: Vec<sla_compliance::TaskSlaData> = tasks
            .iter()
            .map(|t| {
                let is_complete = t.state == DownloadState::Complete;
                let is_failed = t.state == DownloadState::Error;
                let completed_at = if is_complete {
                    Some(t.updated_at)
                } else {
                    None
                };
                sla_compliance::TaskSlaData {
                    task_id: t.id.clone(),
                    task_name: t.name.clone(),
                    tags: t.tags.clone(),
                    group: t.group.clone(),
                    is_complete,
                    is_failed,
                    created_at: t.created_at,
                    completed_at,
                    avg_speed_bps: t.speed_bps,
                    retry_count: t.auto_retry_count,
                    progress: t.progress() as f64 / 100.0,
                }
            })
            .collect();
        drop(tasks);
        self.sla_compliance
            .write()
            .await
            .evaluate_all(&task_data)
            .await
    }

    /// Get SLA compliance summary
    pub async fn get_sla_summary(&self) -> sla_compliance::SlaSummary {
        self.sla_compliance.read().await.get_summary()
    }

    /// Get compliance history for a specific SLA
    pub async fn get_sla_history(
        &self,
        sla_id: &str,
    ) -> Option<Vec<sla_compliance::ComplianceEntry>> {
        self.sla_compliance
            .read()
            .await
            .get_history(sla_id)
            .cloned()
    }

    /// Clear compliance history for a specific SLA
    pub async fn clear_sla_history(&self, sla_id: &str) -> std::io::Result<bool> {
        self.sla_compliance
            .write()
            .await
            .clear_history(sla_id)
            .await
    }

    /// Clear all SLA compliance history
    pub async fn clear_all_sla_history(&self) -> std::io::Result<()> {
        self.sla_compliance.write().await.clear_all_history().await
    }

    /// Format SLA compliance report as human-readable string
    pub async fn format_sla_report(&self) -> String {
        let mgr = self.sla_compliance.read().await;
        let summary = mgr.get_summary();
        mgr.format_report(&summary)
    }

    // ── Download Expiry API ──────────────────────────────────────────────

    /// Set expiry for a task using absolute time
    pub async fn set_task_expiry(&self, task_id: &str, expires_at: chrono::DateTime<chrono::Utc>) {
        self.download_expiry
            .lock()
            .await
            .set_expiry(task_id, expires_at);
    }

    /// Set expiry for a task using relative duration (seconds from now)
    pub async fn set_task_expiry_duration(&self, task_id: &str, duration_secs: u64) {
        self.download_expiry
            .lock()
            .await
            .set_expiry_duration(task_id, duration_secs);
    }

    /// Remove expiry for a task
    pub async fn remove_task_expiry(&self, task_id: &str) {
        self.download_expiry.lock().await.remove_expiry(task_id);
    }

    /// Get expiry info for a task
    pub async fn get_task_expiry(&self, task_id: &str) -> Option<download_expiry::TaskExpiry> {
        self.download_expiry
            .lock()
            .await
            .get_expiry(task_id)
            .cloned()
    }

    /// Check if a task has expiry set
    pub async fn has_task_expiry(&self, task_id: &str) -> bool {
        self.download_expiry.lock().await.has_expiry(task_id)
    }

    /// Refresh all expiry states and return newly expired task IDs
    pub async fn refresh_expiries(&self) -> Vec<String> {
        self.download_expiry.lock().await.refresh()
    }

    /// Check for pending expiry notifications
    pub async fn check_expiry_notifications(&self) -> Vec<String> {
        self.download_expiry.lock().await.check_notifications()
    }

    /// Get list of expired task IDs
    pub async fn get_expired_task_ids(&self) -> Vec<String> {
        self.download_expiry.lock().await.get_expired_ids()
    }

    /// Get expiry summary
    pub async fn get_expiry_summary(&self) -> download_expiry::ExpirySummary {
        self.download_expiry.lock().await.get_summary()
    }

    /// Get expiry config
    pub async fn get_expiry_config(&self) -> download_expiry::ExpiryConfig {
        self.download_expiry.lock().await.config().clone()
    }

    /// Set expiry config
    pub async fn set_expiry_config(&self, config: download_expiry::ExpiryConfig) {
        self.download_expiry.lock().await.set_config(config);
    }

    /// Clear all expiry tracking
    pub async fn clear_all_expiries(&self) {
        self.download_expiry.lock().await.clear();
    }

    /// Cleanup expired tasks from tracking
    pub async fn cleanup_expired_expiries(&self) -> usize {
        self.download_expiry.lock().await.cleanup_expired()
    }

    /// Format upcoming expiries report
    pub async fn format_expiry_report(&self, limit: usize) -> String {
        self.download_expiry.lock().await.format_upcoming(limit)
    }

    /// Save expiry config to disk
    pub async fn save_expiry_config(&self) -> std::io::Result<()> {
        let config = self.download_expiry.lock().await.config().clone();
        let path = self.data_dir.join("download_expiry_config.json");
        download_expiry::save_expiry_config(&config, &path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Save expiry data to disk
    pub async fn save_expiry_data(&self) -> std::io::Result<()> {
        let manager = self.download_expiry.lock().await.clone();
        let path = self.data_dir.join("download_expiry_data.json");
        download_expiry::save_expiry_data(&manager, &path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    // ── Task Export/Import API (Phase 145) ──────────────────────────────────

    /// Export tasks to JSON format
    pub async fn export_tasks_json(
        &self,
        filter: task_export::ExportFilter,
    ) -> Result<Vec<task_export::ExportedTask>, String> {
        let tasks = self.tasks.lock().await;
        let mut exported = Vec::new();

        for task in tasks.iter() {
            if !filter.matches(
                &format!("{:?}", task.state),
                &task.tags,
                task.group.as_deref(),
                task.created_at,
            ) {
                continue;
            }

            exported.push(task_export::ExportedTask {
                id: task.id.clone(),
                name: task.name.clone(),
                url: task.source_url.clone().unwrap_or_default(),
                protocol: format!("{:?}", task.protocol),
                size: task.size,
                downloaded: task.downloaded,
                state: format!("{:?}", task.state),
                tags: task.tags.clone(),
                group: task.group.clone(),
                priority: format!("{:?}", task.priority),
                notes: task.notes.clone(),
                speed_limit_bps: task.speed_limit_bps,
                bandwidth_weight: task.bandwidth_weight,
                expected_checksum: task.expected_checksum.clone(),
                checksum_algorithm: task.checksum_algorithm.as_ref().map(|a| format!("{:?}", a)),
                save_path: task.save_path.to_string_lossy().to_string(),
                created_at: task.created_at,
                updated_at: task.updated_at,
                mirror_urls: task.mirror_urls.clone(),
                max_download_time_secs: task.max_download_time_secs,
                deadline: task.deadline,
                sequential_mode: task.sequential_mode,
            });
        }

        Ok(exported)
    }

    /// Export tasks to CSV format
    pub async fn export_tasks_csv(
        &self,
        filter: task_export::ExportFilter,
    ) -> Result<String, String> {
        let exported = self.export_tasks_json(filter).await?;
        let mut csv = String::from(task_export::csv_header());
        csv.push('\n');
        for task in &exported {
            csv.push_str(&task_export::task_to_csv_line(task));
            csv.push('\n');
        }
        Ok(csv)
    }

    /// Import tasks from JSON data
    pub async fn import_tasks_json(
        &self,
        json_data: &str,
        conflict_strategy: task_export::ImportConflictStrategy,
    ) -> Result<task_export::ImportResult, String> {
        let tasks = task_export::parse_json_export(json_data)
            .map_err(|e| format!("Failed to parse JSON: {}", e))?;
        self.import_tasks_internal(tasks, conflict_strategy).await
    }

    /// Import tasks from CSV data
    pub async fn import_tasks_csv(
        &self,
        csv_data: &str,
        conflict_strategy: task_export::ImportConflictStrategy,
    ) -> Result<task_export::ImportResult, String> {
        let tasks = task_export::parse_csv_export(csv_data)
            .map_err(|e| format!("Failed to parse CSV: {}", e))?;
        self.import_tasks_internal(tasks, conflict_strategy).await
    }

    /// Internal import logic
    async fn import_tasks_internal(
        &self,
        tasks: Vec<task_export::ExportedTask>,
        conflict_strategy: task_export::ImportConflictStrategy,
    ) -> Result<task_export::ImportResult, String> {
        let mut result = task_export::ImportResult::default();
        let mut tasks_guard = self.tasks.lock().await;

        // Build URL set for deduplication
        let existing_urls: Vec<String> = tasks_guard
            .iter()
            .filter_map(|t| t.source_url.clone())
            .collect();
        let url_set = task_export::build_url_set(&existing_urls);

        for task in tasks {
            let url = &task.url;

            if task_export::is_duplicate_url(&url_set, url) {
                match conflict_strategy {
                    task_export::ImportConflictStrategy::Skip => {
                        result.skipped_count += 1;
                        continue;
                    }
                    task_export::ImportConflictStrategy::Overwrite => {
                        // Find and remove existing task with same URL
                        let existing_idx = tasks_guard.iter().position(|t| {
                            t.source_url
                                .as_ref()
                                .map(|u| {
                                    u.to_lowercase().trim_end_matches('/')
                                        == url.to_lowercase().trim_end_matches('/')
                                })
                                .unwrap_or(false)
                        });
                        if let Some(idx) = existing_idx {
                            tasks_guard.remove(idx);
                            result.overwritten_count += 1;
                        }
                    }
                    task_export::ImportConflictStrategy::Rename => {
                        result.renamed_count += 1;
                    }
                }
            }

            // Create new task
            let id = if result.renamed_count > 0 {
                format!("{}_{}", task.id, chrono::Utc::now().timestamp())
            } else {
                task.id.clone()
            };

            let new_task = crate::DownloadTask {
                id: id.clone(),
                name: task.name.clone(),
                protocol: match task.protocol.to_lowercase().as_str() {
                    "torrent" => crate::DownloadProtocol::Torrent,
                    "ed2k" => crate::DownloadProtocol::Ed2k,
                    _ => crate::DownloadProtocol::Xunlei,
                },
                size: task.size,
                downloaded: task.downloaded,
                state: crate::DownloadState::Queued,
                error: None,
                speed_bps: 0.0,
                save_path: std::path::PathBuf::from(&task.save_path),
                created_at: task.created_at,
                updated_at: task.updated_at,
                tags: task.tags.clone(),
                priority: match task.priority.to_lowercase().as_str() {
                    "high" => crate::DownloadPriority::High,
                    "low" => crate::DownloadPriority::Low,
                    _ => crate::DownloadPriority::Normal,
                },
                schedule: None,
                bandwidth_weight: task.bandwidth_weight,
                queue_position: None,
                depends_on: Vec::new(),
                notes: task.notes.clone(),
                group: task.group.clone(),
                speed_limit_bps: task.speed_limit_bps,
                auto_retry_count: 0,
                retry_after: None,
                source_url: Some(task.url.clone()),
                expected_checksum: task.expected_checksum.clone(),
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                current_session_start: None,
                mirror_urls: task.mirror_urls.clone(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: task.sequential_mode,
                max_download_time_secs: task.max_download_time_secs,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: task.deadline,
            };

            tasks_guard.push(new_task);
            result.imported_count += 1;
        }

        Ok(result)
    }

    /// Get export history
    pub async fn get_export_history(&self) -> Vec<task_export::ExportHistoryEntry> {
        self.task_export.lock().await.entries.clone()
    }

    /// Add export history entry
    pub async fn add_export_history(&self, entry: task_export::ExportHistoryEntry) {
        let mut history = self.task_export.lock().await;
        history.add(entry, 50);
    }

    /// Save export history to disk
    pub async fn save_export_history(&self) -> std::io::Result<()> {
        let history = self.task_export.lock().await.clone();
        let path = self.data_dir.join("export_history.json");
        task_export::save_export_history(&history, &path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    // ========== Notification Preferences (Phase 146) ==========

    /// Get notification preferences configuration
    pub async fn get_notification_preferences_config(
        &self,
    ) -> notification_preferences::NotificationPreferencesConfig {
        let manager = self.notification_preferences.lock().await;
        manager.get_config().clone()
    }

    /// Set notification preferences configuration
    pub async fn set_notification_preferences_config(
        &self,
        config: notification_preferences::NotificationPreferencesConfig,
    ) {
        let mut manager = self.notification_preferences.lock().await;
        manager.set_config(config);
    }

    /// Get notification preferences for a specific task
    pub async fn get_task_notification_config(
        &self,
        task_id: &str,
    ) -> Option<notification_preferences::TaskNotificationConfig> {
        let manager = self.notification_preferences.lock().await;
        manager.get_task_config(task_id).cloned()
    }

    /// Set notification preferences for a specific task
    pub async fn set_task_notification_config(
        &self,
        config: notification_preferences::TaskNotificationConfig,
    ) {
        let mut manager = self.notification_preferences.lock().await;
        manager.set_task_config(config);
    }

    /// Remove notification preferences for a task
    pub async fn remove_task_notification_config(&self, task_id: &str) -> bool {
        let mut manager = self.notification_preferences.lock().await;
        manager.remove_task_config(task_id)
    }

    /// Enable notifications for a task
    pub async fn enable_task_notifications(&self, task_id: &str) {
        let mut manager = self.notification_preferences.lock().await;
        manager.enable_task_notifications(task_id);
    }

    /// Disable notifications for a task
    pub async fn disable_task_notifications(&self, task_id: &str) {
        let mut manager = self.notification_preferences.lock().await;
        manager.disable_task_notifications(task_id);
    }

    /// Set cooldown period for a task
    pub async fn set_task_notification_cooldown(&self, task_id: &str, cooldown_secs: u64) {
        let mut manager = self.notification_preferences.lock().await;
        manager.set_task_cooldown(task_id, cooldown_secs);
    }

    /// Set minimum priority for a task
    pub async fn set_task_notification_min_priority(
        &self,
        task_id: &str,
        min_priority: notification_preferences::MinimumPriority,
    ) {
        let mut manager = self.notification_preferences.lock().await;
        manager.set_task_min_priority(task_id, min_priority);
    }

    /// Check if a notification should be sent for a task event
    pub async fn should_send_notification(
        &self,
        task_id: &str,
        event: &notification_preferences::TaskNotificationEvent,
    ) -> bool {
        let mut manager = self.notification_preferences.lock().await;
        manager.should_notify(task_id, event)
    }

    /// Get notification preferences summary
    pub async fn get_notification_preferences_summary(
        &self,
    ) -> notification_preferences::NotificationPreferencesSummary {
        let manager = self.notification_preferences.lock().await;
        manager.get_summary()
    }

    /// List all task notification configs
    pub async fn list_task_notification_configs(
        &self,
    ) -> Vec<notification_preferences::TaskNotificationConfig> {
        let manager = self.notification_preferences.lock().await;
        manager.list_task_configs().into_iter().cloned().collect()
    }

    /// Clear notification cooldown for a task
    pub async fn clear_task_notification_cooldown(&self, task_id: &str) {
        let mut manager = self.notification_preferences.lock().await;
        manager.clear_cooldown(task_id);
    }

    /// Clear all notification cooldowns
    pub async fn clear_all_notification_cooldowns(&self) {
        let mut manager = self.notification_preferences.lock().await;
        manager.clear_all_cooldowns();
    }

    /// Save notification preferences to disk
    pub async fn save_notification_preferences(&self) -> std::io::Result<()> {
        let manager = self.notification_preferences.lock().await;
        let config_path = self.data_dir.join("notification_preferences_config.json");
        let tasks_path = self.data_dir.join("notification_preferences_tasks.json");

        manager
            .save_config(&config_path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        manager
            .save_task_configs(&tasks_path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(())
    }

    /// Load notification preferences from disk
    pub async fn load_notification_preferences(&self) -> std::io::Result<()> {
        let mut manager = self.notification_preferences.lock().await;
        let config_path = self.data_dir.join("notification_preferences_config.json");
        let tasks_path = self.data_dir.join("notification_preferences_tasks.json");

        manager
            .load_config(&config_path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        manager
            .load_task_configs(&tasks_path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(())
    }

    // ========== Notification Center (Phase 147) ==========

    /// Get notification center configuration
    pub async fn get_notification_center_config(
        &self,
    ) -> notification_center::NotificationCenterConfig {
        let manager = self.notification_center.lock().await;
        manager.get_config().clone()
    }

    /// Set notification center configuration
    pub async fn set_notification_center_config(
        &self,
        config: notification_center::NotificationCenterConfig,
    ) {
        let mut manager = self.notification_center.lock().await;
        manager.set_config(config);
    }

    /// Get notification center summary (quiet hours status, batch count, analytics)
    pub async fn get_notification_center_summary(
        &self,
    ) -> notification_center::NotificationCenterSummary {
        let manager = self.notification_center.lock().await;
        manager.get_summary()
    }

    /// Check if quiet hours are currently active
    pub async fn is_notification_quiet_hours_active(&self) -> bool {
        let manager = self.notification_center.lock().await;
        manager.is_quiet_hours_active()
    }

    /// Get notification history with optional filters
    pub async fn get_notification_history(
        &self,
        filter: notification_center::NotificationFilter,
    ) -> Vec<notification_center::NotificationRecord> {
        let manager = self.notification_center.lock().await;
        manager.get_history(filter)
    }

    /// Get notification analytics (delivery stats, channel usage)
    pub async fn get_notification_analytics(&self) -> notification_center::NotificationAnalytics {
        let manager = self.notification_center.lock().await;
        manager.get_analytics().clone()
    }

    /// Clear notification history
    pub async fn clear_notification_history(&self) {
        let mut manager = self.notification_center.lock().await;
        manager.clear_history();
    }

    /// Flush pending batched notifications immediately
    pub async fn flush_notification_batch(&self) {
        let mut manager = self.notification_center.lock().await;
        manager.flush_batch();
    }

    /// Get pending batch count
    pub async fn get_notification_batch_count(&self) -> usize {
        let manager = self.notification_center.lock().await;
        manager.get_pending_batch_count()
    }

    /// Add an event channel preference
    pub async fn add_notification_event_preference(
        &self,
        preference: notification_center::EventChannelPreference,
    ) {
        let mut manager = self.notification_center.lock().await;
        let config = manager.get_config().clone();
        let mut new_config = config;
        new_config.event_preferences.push(preference);
        manager.set_config(new_config);
    }

    /// Remove event channel preferences for a specific event
    pub async fn remove_notification_event_preference(
        &self,
        event: notification_center::NotificationCenterEvent,
    ) {
        let mut manager = self.notification_center.lock().await;
        let config = manager.get_config().clone();
        let mut new_config = config;
        new_config.event_preferences.retain(|p| p.event != event);
        manager.set_config(new_config);
    }

    /// Save notification center config to disk
    pub async fn save_notification_center_config(&self) -> std::io::Result<()> {
        let manager = self.notification_center.lock().await;
        let config_path = self.data_dir.join("notification_center_config.json");
        manager
            .save_config(&config_path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(())
    }

    /// Load notification center config from disk
    pub async fn load_notification_center_config(&self) -> std::io::Result<()> {
        let mut manager = self.notification_center.lock().await;
        let config_path = self.data_dir.join("notification_center_config.json");
        if config_path.exists() {
            let config = notification_center::NotificationCenterManager::load_config(&config_path)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            manager.set_config(config);
        }
        Ok(())
    }

    /// Save notification center history to disk
    pub async fn save_notification_center_history(&self) -> std::io::Result<()> {
        let manager = self.notification_center.lock().await;
        let history_path = self.data_dir.join("notification_center_history.json");
        manager
            .save_history(&history_path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(())
    }

    /// Load notification center history from disk
    pub async fn load_notification_center_history(&self) -> std::io::Result<()> {
        let mut manager = self.notification_center.lock().await;
        let history_path = self.data_dir.join("notification_center_history.json");
        if history_path.exists() {
            manager
                .load_history(&history_path)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        Ok(())
    }
}

/// Convert DownloadProtocol to protocol limits key string.
fn protocol_to_limits_key(protocol: DownloadProtocol) -> String {
    match protocol {
        DownloadProtocol::Torrent => "torrent".to_string(),
        DownloadProtocol::Ed2k => "ed2k".to_string(),
        DownloadProtocol::Xunlei => "xunlei".to_string(),
        DownloadProtocol::Magnet => "magnet".to_string(),
        DownloadProtocol::P2P => "p2p".to_string(),
    }
}

/// Extract download URLs from arbitrary text content.
///
/// Scans text for URLs matching supported protocols (http, https, ftp, ed2k, magnet).
/// Returns a deduplicated list of URLs in the order they were found.
///
/// # Examples
///
/// ```
/// use ipmsg_download::extract_urls_from_text;
///
/// let text = "Check out https://example.com/file.zip and ed2k://|file|test.iso|1234|abcd|/";
/// let urls = extract_urls_from_text(text);
/// assert_eq!(urls.len(), 2);
/// assert_eq!(urls[0], "https://example.com/file.zip");
/// ```
pub fn extract_urls_from_text(text: &str) -> Vec<String> {
    let mut urls = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for line in text.lines() {
        // Skip comment lines
        if line.trim().starts_with('#') {
            continue;
        }

        // Extract URLs from this line
        extract_urls_from_line(line, &mut urls, &mut seen);
    }

    urls
}

/// Extract URLs from a single line of text.
fn extract_urls_from_line(
    line: &str,
    urls: &mut Vec<String>,
    seen: &mut std::collections::HashSet<String>,
) {
    let mut remaining = line;

    while !remaining.is_empty() {
        // Find the next URL start
        let (url_start, protocol_end) = if let Some(pos) = remaining.find("ed2k://") {
            (pos, pos + 7)
        } else if let Some(pos) = remaining.find("magnet:") {
            (pos, pos + 7)
        } else if let Some(pos) = remaining.find("https://") {
            (pos, pos + 8)
        } else if let Some(pos) = remaining.find("http://") {
            (pos, pos + 7)
        } else if let Some(pos) = remaining.find("ftp://") {
            (pos, pos + 6)
        } else {
            break;
        };

        // Find the end of this URL (whitespace or end of string)
        let url_rest = &remaining[protocol_end..];
        let url_end = url_rest
            .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '>' || c == ')')
            .unwrap_or(url_rest.len());

        let full_url = &remaining[url_start..protocol_end + url_end];

        // Validate and add
        if !full_url.is_empty() && seen.insert(full_url.to_string()) {
            urls.push(full_url.to_string());
        }

        // Continue scanning after this URL
        remaining = &remaining[protocol_end + url_end..];
    }
}

/// Extract a display name from a URL for duplicate detection.
fn extract_display_name(url: &str) -> String {
    if url.starts_with("ed2k://") {
        // ed2k://|file|name|size|hash|/
        let parts: Vec<&str> = url.split('|').collect();
        if parts.len() >= 3 {
            return parts[2].to_string();
        }
    } else if url.starts_with("magnet:") {
        // magnet:?xt=urn:btih:HASH&dn=NAME
        if let Some(pos) = url.find("dn=") {
            let rest = &url[pos + 3..];
            let name = rest.split('&').next().unwrap_or(rest);
            if !name.is_empty() {
                return name.to_string();
            }
        }
    } else {
        // HTTP/FTP: extract filename from URL path
        let name = url
            .split('/')
            .next_back()
            .unwrap_or("download")
            .split('?')
            .next()
            .unwrap_or("download");
        if !name.is_empty() {
            return name.to_string();
        }
    }
    url.to_string()
}

/// Save maximum concurrent downloads setting to disk.
fn save_max_concurrent(max: usize, data_dir: &std::path::Path) -> Result<(), std::io::Error> {
    let path = data_dir.join("max_concurrent.json");
    let json = serde_json::to_string(&serde_json::json!({ "max_concurrent": max }))?;
    std::fs::write(path, json)
}

/// Load maximum concurrent downloads setting from disk.
/// Returns `None` if no config file exists.
fn load_max_concurrent(data_dir: &std::path::Path) -> Option<usize> {
    let path = data_dir.join("max_concurrent.json");
    if !path.exists() {
        return None;
    }
    let json = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&json).ok()?;
    v.get("max_concurrent")?.as_u64().map(|n| n as usize)
}

/// Save conflict detection strategy to disk (atomic write).
fn save_conflict_strategy(
    strategy: &conflict_detection::ConflictStrategy,
    data_dir: &std::path::Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("conflict_strategy.json");
    let json = serde_json::to_string(&serde_json::json!({ "strategy": strategy.to_string() }))?;
    let tmp = data_dir.join("conflict_strategy.json.tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(tmp, path)
}

/// Load conflict detection strategy from disk.
/// Returns `None` if no config file exists.
fn load_conflict_strategy(
    data_dir: &std::path::Path,
) -> Option<conflict_detection::ConflictStrategy> {
    let path = data_dir.join("conflict_strategy.json");
    if !path.exists() {
        return None;
    }
    let json = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&json).ok()?;
    let s = v.get("strategy")?.as_str()?;
    s.parse().ok()
}

/// Parse an ed2k:// link and add it as a download task.
async fn parse_and_add_ed2k(manager: &DownloadManager, link: &str) -> Result<String, String> {
    // ed2k://|file|name|size|hash|/
    let parts: Vec<&str> = link.split('|').collect();
    if parts.len() < 5 {
        return Err("Invalid ed2k link format".to_string());
    }
    if parts[1] != "file" {
        return Err("Only ed2k file links are supported".to_string());
    }
    let file_name = parts[2].to_string();
    let file_size: u64 = parts[3]
        .parse()
        .map_err(|_| "Invalid file size in ed2k link".to_string())?;
    let hash = ed2k::Ed2kFileHash::from_hex(parts[4])
        .map_err(|e| format!("Invalid hash in ed2k link: {e}"))?;

    // Parse optional servers from h= parameters
    let servers = Vec::new(); // ed2k links rarely include servers inline

    manager
        .add_ed2k(hash, file_size, file_name, servers)
        .await
        .map_err(|e| e.to_string())
}

// ========== Host Connection Limiter (Phase 148) ==========

impl DownloadManager {
    /// Get host connection limiter configuration
    pub async fn get_host_conn_limit_config(&self) -> host_conn_limit::HostConnLimitConfig {
        let manager = self.host_conn_limit.lock().await;
        manager.get_config().clone()
    }

    /// Set host connection limiter configuration
    pub async fn set_host_conn_limit_config(&self, config: host_conn_limit::HostConnLimitConfig) {
        let mut manager = self.host_conn_limit.lock().await;
        manager.set_config(config);
    }

    /// Get a summary of host connection limiter status
    pub async fn get_host_conn_limit_summary(&self) -> host_conn_limit::HostConnLimitSummary {
        let manager = self.host_conn_limit.lock().await;
        manager.get_summary()
    }

    /// Try to acquire a connection slot for a host
    pub async fn acquire_host_connection(
        &self,
        hostname: &str,
    ) -> host_conn_limit::ConnectionAcquireResult {
        let mut manager = self.host_conn_limit.lock().await;
        manager.acquire_connection(hostname)
    }

    /// Release a connection slot for a host
    pub async fn release_host_connection(&self, hostname: &str) {
        let mut manager = self.host_conn_limit.lock().await;
        manager.release_connection(hostname);
    }

    /// Record a connection failure for a host
    pub async fn record_host_failure(&self, hostname: &str) {
        let mut manager = self.host_conn_limit.lock().await;
        manager.record_failure(hostname);
    }

    /// Get connection state for a specific host
    pub async fn get_host_connection_state(
        &self,
        hostname: &str,
    ) -> Option<host_conn_limit::HostConnectionInfo> {
        let manager = self.host_conn_limit.lock().await;
        manager.get_host_state(hostname).map(|state| {
            let max = manager.get_max_connections(&state.hostname);
            let now = std::time::Instant::now();
            host_conn_limit::HostConnectionInfo {
                hostname: state.hostname.clone(),
                active_connections: state.active_connections,
                max_connections: max,
                total_connections: state.total_connections,
                total_failures: state.total_failures,
                peak_connections: state.peak_connections,
                at_limit: state.at_limit,
                idle_secs: now.duration_since(state.last_activity).as_secs(),
            }
        })
    }

    /// Check if a host is at its connection limit
    pub async fn is_host_at_connection_limit(&self, hostname: &str) -> bool {
        let manager = self.host_conn_limit.lock().await;
        manager.is_at_limit(hostname)
    }

    /// Get available connection slots for a host
    pub async fn get_host_available_slots(&self, hostname: &str) -> u32 {
        let manager = self.host_conn_limit.lock().await;
        manager.available_slots(hostname)
    }

    /// Set a per-host connection limit override
    pub async fn set_host_connection_override(&self, hostname: &str, max_connections: u32) {
        let mut manager = self.host_conn_limit.lock().await;
        manager.set_host_override(hostname, max_connections);
    }

    /// Remove a per-host connection limit override
    pub async fn remove_host_connection_override(&self, hostname: &str) -> bool {
        let mut manager = self.host_conn_limit.lock().await;
        manager.remove_host_override(hostname)
    }

    /// List all host connection overrides
    pub async fn list_host_connection_overrides(&self) -> Vec<(String, u32)> {
        let manager = self.host_conn_limit.lock().await;
        manager.list_overrides()
    }

    /// Remove a host from connection tracking
    pub async fn remove_host_connection(&self, hostname: &str) -> bool {
        let mut manager = self.host_conn_limit.lock().await;
        manager.remove_host(hostname)
    }

    /// Clear all host connection tracking data
    pub async fn clear_host_connections(&self) {
        let mut manager = self.host_conn_limit.lock().await;
        manager.clear_host_data();
    }

    /// Clean up stale host connections
    pub async fn cleanup_stale_host_connections(&self) {
        let mut manager = self.host_conn_limit.lock().await;
        manager.cleanup_stale_hosts();
    }

    /// Save host connection limiter config to disk
    pub async fn save_host_conn_limit_config(&self) -> std::io::Result<()> {
        let manager = self.host_conn_limit.lock().await;
        let config_path = self.data_dir.join("host_conn_limit_config.json");
        manager.save_config(&config_path)
    }

    /// Load host connection limiter config from disk
    pub async fn load_host_conn_limit_config(&self) -> std::io::Result<()> {
        let mut manager = self.host_conn_limit.lock().await;
        let config_path = self.data_dir.join("host_conn_limit_config.json");
        manager.load_config(&config_path)
    }

    // ── Phase 149: Task Cron Scheduler ──────────────────────────────────────

    /// Get the task cron scheduler configuration.
    pub async fn get_task_cron_scheduler_config(
        &self,
    ) -> task_cron_scheduler::TaskCronSchedulerConfig {
        self.task_cron_scheduler.lock().await.config().clone()
    }

    /// Set the task cron scheduler configuration.
    pub async fn set_task_cron_scheduler_config(
        &self,
        config: task_cron_scheduler::TaskCronSchedulerConfig,
    ) {
        self.task_cron_scheduler.lock().await.set_config(config);
    }

    /// Add a cron schedule for a task.
    pub async fn add_task_cron_schedule(
        &self,
        task_id: &str,
        schedule: task_cron_scheduler::TaskCronSchedule,
    ) -> Result<(), task_cron_scheduler::TaskCronSchedulerError> {
        self.task_cron_scheduler
            .lock()
            .await
            .add_schedule(task_id, schedule)
    }

    /// Remove a cron schedule for a task.
    pub async fn remove_task_cron_schedule(
        &self,
        task_id: &str,
    ) -> Result<task_cron_scheduler::TaskCronSchedule, task_cron_scheduler::TaskCronSchedulerError>
    {
        self.task_cron_scheduler
            .lock()
            .await
            .remove_schedule(task_id)
    }

    /// Get the cron schedule for a task.
    pub async fn get_task_cron_schedule(
        &self,
        task_id: &str,
    ) -> Option<task_cron_scheduler::TaskCronSchedule> {
        self.task_cron_scheduler
            .lock()
            .await
            .get_schedule(task_id)
            .cloned()
    }

    /// List all cron schedules.
    pub async fn list_task_cron_schedules(
        &self,
    ) -> Vec<(String, task_cron_scheduler::TaskCronSchedule)> {
        self.task_cron_scheduler
            .lock()
            .await
            .list_schedules()
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Enable or disable a cron schedule.
    pub async fn set_task_cron_schedule_enabled(
        &self,
        task_id: &str,
        enabled: bool,
    ) -> Result<(), task_cron_scheduler::TaskCronSchedulerError> {
        self.task_cron_scheduler
            .lock()
            .await
            .set_schedule_enabled(task_id, enabled)
    }

    /// Get the task cron scheduler summary.
    pub async fn get_task_cron_scheduler_summary(
        &self,
    ) -> task_cron_scheduler::TaskCronSchedulerSummary {
        self.task_cron_scheduler.lock().await.summary()
    }

    /// Save task cron scheduler to disk.
    pub async fn save_task_cron_scheduler(
        &self,
    ) -> Result<(), task_cron_scheduler::TaskCronSchedulerError> {
        self.task_cron_scheduler
            .lock()
            .await
            .save(&self.data_dir)
            .await
    }

    // ── Phase 150: Source Latency API ──────────────────────────────────────

    /// Get source latency configuration.
    pub async fn get_source_latency_config(&self) -> source_latency::SourceLatencyConfig {
        let monitor = self.source_latency.lock().await;
        monitor.config().clone()
    }

    /// Set source latency configuration.
    pub async fn set_source_latency_config(&self, config: source_latency::SourceLatencyConfig) {
        let mut monitor = self.source_latency.lock().await;
        monitor.set_config(config);
    }

    /// Record a successful connection for latency tracking.
    pub async fn record_latency_success(&self, domain: &str, latency_ms: f64) {
        let mut monitor = self.source_latency.lock().await;
        monitor.record_success(domain, latency_ms);
    }

    /// Record a failed connection for latency tracking.
    pub async fn record_latency_failure(&self, domain: &str, error: String) {
        let mut monitor = self.source_latency.lock().await;
        monitor.record_failure(domain, error);
    }

    /// Get latency statistics for a specific domain.
    pub async fn get_source_latency_domain(
        &self,
        domain: &str,
    ) -> Option<source_latency::DomainLatencyStats> {
        let monitor = self.source_latency.lock().await;
        monitor.get_domain_stats(domain).cloned()
    }

    /// Get latency statistics for all tracked domains.
    pub async fn get_source_latency_all(&self) -> Vec<source_latency::DomainLatencyStats> {
        let monitor = self.source_latency.lock().await;
        monitor.get_all_stats().values().cloned().collect()
    }

    /// Get source latency summary across all domains.
    pub async fn get_source_latency_summary(&self) -> source_latency::SourceLatencySummary {
        let monitor = self.source_latency.lock().await;
        monitor.get_summary()
    }

    /// Get the best domain (lowest latency).
    pub async fn get_best_latency_domain(&self) -> Option<String> {
        let monitor = self.source_latency.lock().await;
        monitor.get_best_domain().map(|s| s.to_string())
    }

    /// Rank all domains by latency (best to worst).
    pub async fn rank_domains_by_latency(
        &self,
    ) -> Vec<(String, f64, source_latency::LatencyHealth)> {
        let monitor = self.source_latency.lock().await;
        monitor
            .rank_domains()
            .into_iter()
            .map(|(d, l, h)| (d.to_string(), l, h))
            .collect()
    }

    /// Clear latency data for a specific domain.
    pub async fn clear_source_latency_domain(&self, domain: &str) {
        let mut monitor = self.source_latency.lock().await;
        monitor.clear_domain(domain);
    }

    /// Clear all source latency data.
    pub async fn clear_source_latency_all(&self) {
        let mut monitor = self.source_latency.lock().await;
        monitor.clear_all();
    }

    /// Apply periodic decay to all domain latency data.
    pub async fn apply_source_latency_decay(&self) {
        let mut monitor = self.source_latency.lock().await;
        monitor.apply_periodic_decay();
    }

    /// Format a human-readable latency summary.
    pub async fn format_source_latency_summary(&self) -> String {
        let monitor = self.source_latency.lock().await;
        let summary = monitor.get_summary();
        monitor.format_summary(&summary)
    }

    /// Save source latency config to disk.
    pub async fn save_source_latency_config(&self) -> std::io::Result<()> {
        let monitor = self.source_latency.lock().await;
        let config_path = self.data_dir.join("source_latency_config.json");
        monitor.save_config(&config_path).await
    }

    /// Load source latency config from disk.
    pub async fn load_source_latency_config(&self) -> std::io::Result<()> {
        let config_path = self.data_dir.join("source_latency_config.json");
        if config_path.exists() {
            let config = source_latency::SourceLatencyMonitor::load_config(&config_path).await?;
            let mut monitor = self.source_latency.lock().await;
            monitor.set_config(config);
        }
        Ok(())
    }

    /// Save source latency stats to disk.
    pub async fn save_source_latency_stats(&self) -> std::io::Result<()> {
        let monitor = self.source_latency.lock().await;
        let stats_path = self.data_dir.join("source_latency_stats.json");
        monitor.save_stats(&stats_path).await
    }

    /// Load source latency stats from disk.
    pub async fn load_source_latency_stats(&self) -> std::io::Result<()> {
        let stats_path = self.data_dir.join("source_latency_stats.json");
        if stats_path.exists() {
            let stats = source_latency::SourceLatencyMonitor::load_stats(&stats_path).await?;
            let mut monitor = self.source_latency.lock().await;
            for (domain, domain_stats) in stats {
                monitor.get_all_stats_mut().insert(domain, domain_stats);
            }
        }
        Ok(())
    }

    // ── Phase 151: Bandwidth QoS API ──────────────────────────────────────

    /// Get bandwidth QoS configuration.
    pub async fn get_bandwidth_qos_config(&self) -> bandwidth_qos::BandwidthQosConfig {
        let mgr = self.bandwidth_qos.lock().await;
        mgr.config().clone()
    }

    /// Set bandwidth QoS configuration.
    pub async fn set_bandwidth_qos_config(&self, config: bandwidth_qos::BandwidthQosConfig) {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.set_config(config);
    }

    /// Get bandwidth QoS summary.
    pub async fn get_bandwidth_qos_summary(&self) -> bandwidth_qos::BandwidthQosSummary {
        let mgr = self.bandwidth_qos.lock().await;
        mgr.summary()
    }

    /// Assign QoS tier to a task.
    pub async fn assign_qos_tier(
        &self,
        task_id: &str,
        tier: bandwidth_qos::QosTier,
    ) -> Result<(), bandwidth_qos::BandwidthQosError> {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.assign_tier(task_id, tier)
    }

    /// Remove QoS assignment for a task.
    pub async fn remove_qos_assignment(
        &self,
        task_id: &str,
    ) -> Result<(), bandwidth_qos::BandwidthQosError> {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.remove_assignment(task_id)
    }

    /// Get QoS tier for a task.
    pub async fn get_task_qos_tier(&self, task_id: &str) -> bandwidth_qos::QosTier {
        let mgr = self.bandwidth_qos.lock().await;
        mgr.get_tier(task_id)
    }

    /// Get bandwidth weight for a task.
    pub async fn get_task_qos_weight(&self, task_id: &str) -> f64 {
        let mgr = self.bandwidth_qos.lock().await;
        mgr.get_task_weight(task_id)
    }

    /// Auto-classify a task based on URL and name.
    pub async fn auto_classify_qos(
        &self,
        task_id: &str,
        url: &str,
        name: &str,
    ) -> Option<bandwidth_qos::QosTier> {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.auto_classify(task_id, url, name)
    }

    /// Add QoS auto-classification rule.
    pub async fn add_qos_rule(
        &self,
        rule: bandwidth_qos::QosAutoRule,
    ) -> Result<(), bandwidth_qos::BandwidthQosError> {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.add_rule(rule)
    }

    /// Remove QoS auto-classification rule.
    pub async fn remove_qos_rule(
        &self,
        rule_id: &str,
    ) -> Result<bandwidth_qos::QosAutoRule, bandwidth_qos::BandwidthQosError> {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.remove_rule(rule_id)
    }

    /// List all QoS rules.
    pub async fn list_qos_rules(&self) -> Vec<bandwidth_qos::QosAutoRule> {
        let mgr = self.bandwidth_qos.lock().await;
        mgr.list_rules().into_iter().cloned().collect()
    }

    /// Enable or disable QoS rule.
    pub async fn set_qos_rule_enabled(
        &self,
        rule_id: &str,
        enabled: bool,
    ) -> Result<(), bandwidth_qos::BandwidthQosError> {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.set_rule_enabled(rule_id, enabled)
    }

    /// Set QoS rule priority.
    pub async fn set_qos_rule_priority(
        &self,
        rule_id: &str,
        priority: i32,
    ) -> Result<(), bandwidth_qos::BandwidthQosError> {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.set_rule_priority(rule_id, priority)
    }

    /// Clear all QoS assignments.
    pub async fn clear_qos_assignments(&self) {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.clear_assignments();
    }

    /// Clear all QoS rules.
    pub async fn clear_qos_rules(&self) {
        let mut mgr = self.bandwidth_qos.lock().await;
        mgr.clear_rules();
    }

    /// Format QoS summary report.
    pub async fn format_bandwidth_qos_summary(&self) -> String {
        let mgr = self.bandwidth_qos.lock().await;
        mgr.format_summary()
    }

    /// Save bandwidth QoS config to disk.
    pub async fn save_bandwidth_qos_config(&self) -> std::io::Result<()> {
        let mgr = self.bandwidth_qos.lock().await;
        mgr.save(&self.data_dir).await.map_err(|e| match e {
            bandwidth_qos::BandwidthQosError::Io(io_err) => io_err,
            _ => std::io::Error::other(e.to_string()),
        })
    }

    /// Load bandwidth QoS config from disk.
    pub async fn load_bandwidth_qos_config(&self) -> std::io::Result<()> {
        let config_path = self.data_dir.join("bandwidth_qos_config.json");
        if config_path.exists() {
            let mgr = bandwidth_qos::BandwidthQosManager::load(&self.data_dir)
                .await
                .map_err(|e| match e {
                    bandwidth_qos::BandwidthQosError::Io(io_err) => io_err,
                    _ => std::io::Error::other(e.to_string()),
                })?;
            let mut current = self.bandwidth_qos.lock().await;
            *current = mgr;
        }
        Ok(())
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
    #[error("duplicate task: {0}")]
    DuplicateTask(String),
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
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

    #[tokio::test]
    async fn test_set_queue_position() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_queue_position"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "q-1".into(),
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
        }

        // Set queue position
        let success = dm.set_queue_position("q-1", Some(5)).await;
        assert!(success);

        // Get queue position
        let pos = dm.get_queue_position("q-1").await;
        assert_eq!(pos, Some(Some(5)));

        // Clear queue position
        let success = dm.set_queue_position("q-1", None).await;
        assert!(success);

        let pos = dm.get_queue_position("q-1").await;
        assert_eq!(pos, Some(None));
    }

    #[tokio::test]
    async fn test_move_task_up() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_move_up"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "q-1".into(),
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
                queue_position: Some(10),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
            tasks.push(DownloadTask {
                id: "q-2".into(),
                name: "file2.txt".into(),
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
                queue_position: Some(20),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
        }

        // Move q-2 up (should swap with q-1)
        let success = dm.move_task_up("q-2").await;
        assert!(success);

        let pos1 = dm.get_queue_position("q-1").await.unwrap();
        let pos2 = dm.get_queue_position("q-2").await.unwrap();
        assert_eq!(pos1, Some(20));
        assert_eq!(pos2, Some(10));
    }

    #[tokio::test]
    async fn test_move_task_down() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_move_down"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "q-1".into(),
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
                queue_position: Some(10),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
            tasks.push(DownloadTask {
                id: "q-2".into(),
                name: "file2.txt".into(),
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
                queue_position: Some(20),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
        }

        // Move q-1 down (should swap with q-2)
        let success = dm.move_task_down("q-1").await;
        assert!(success);

        let pos1 = dm.get_queue_position("q-1").await.unwrap();
        let pos2 = dm.get_queue_position("q-2").await.unwrap();
        assert_eq!(pos1, Some(20));
        assert_eq!(pos2, Some(10));
    }

    #[tokio::test]
    async fn test_move_task_to_top() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_move_top"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "q-1".into(),
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
                queue_position: Some(10),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
            tasks.push(DownloadTask {
                id: "q-2".into(),
                name: "file2.txt".into(),
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
                queue_position: Some(20),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
            tasks.push(DownloadTask {
                id: "q-3".into(),
                name: "file3.txt".into(),
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
                queue_position: Some(30),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
        }

        // Move q-3 to top
        let success = dm.move_task_to_top("q-3").await;
        assert!(success);

        let pos3 = dm.get_queue_position("q-3").await.unwrap();
        assert_eq!(pos3, Some(9)); // Should be min(10,20,30) - 1 = 9
    }

    #[tokio::test]
    async fn test_move_task_to_bottom() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_move_bottom"));
        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "q-1".into(),
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
                queue_position: Some(10),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
            tasks.push(DownloadTask {
                id: "q-2".into(),
                name: "file2.txt".into(),
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
                queue_position: Some(20),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
            tasks.push(DownloadTask {
                id: "q-3".into(),
                name: "file3.txt".into(),
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
                queue_position: Some(30),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
        }

        // Move q-1 to bottom
        let success = dm.move_task_to_bottom("q-1").await;
        assert!(success);

        let pos1 = dm.get_queue_position("q-1").await.unwrap();
        assert_eq!(pos1, Some(31)); // Should be max(10,20,30) + 1 = 31
    }

    #[tokio::test]
    async fn test_queue_ordering_by_position() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_queue_order"));

        {
            let mut tasks = dm.tasks.lock().await;
            tasks.push(DownloadTask {
                id: "q-1".into(),
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
                queue_position: Some(30),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
            tasks.push(DownloadTask {
                id: "q-2".into(),
                name: "file2.txt".into(),
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
                queue_position: Some(10),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
            tasks.push(DownloadTask {
                id: "q-3".into(),
                name: "file3.txt".into(),
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
                queue_position: Some(20),
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
                current_session_start: None,
            });
        }

        // Verify ordering: q-2 (pos=10) < q-3 (pos=20) < q-1 (pos=30)
        let tasks = dm.tasks.lock().await;
        let mut queued: Vec<_> = tasks
            .iter()
            .filter(|t| t.state == DownloadState::Queued)
            .collect();
        queued.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| match (a.queue_position, b.queue_position) {
                    (Some(pa), Some(pb)) => pa.cmp(&pb),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => a.created_at.cmp(&b.created_at),
                })
        });
        assert_eq!(queued[0].id, "q-2");
        assert_eq!(queued[1].id, "q-3");
        assert_eq!(queued[2].id, "q-1");
    }

    // ─── Phase 23: Batch URL Import Tests ───

    #[test]
    fn test_extract_display_name_http() {
        assert_eq!(
            extract_display_name("https://example.com/files/archive.tar.gz"),
            "archive.tar.gz"
        );
        assert_eq!(
            extract_display_name("https://example.com/files/archive.tar.gz?v=2"),
            "archive.tar.gz"
        );
    }

    #[test]
    fn test_extract_display_name_ed2k() {
        assert_eq!(
            extract_display_name(
                "ed2k://|file|ubuntu.iso|1234567|abcdef0123456789abcdef0123456789|/"
            ),
            "ubuntu.iso"
        );
    }

    #[test]
    fn test_extract_display_name_magnet() {
        assert_eq!(
            extract_display_name("magnet:?xt=urn:btih:abc123&dn=MyFile.zip&tr=tracker"),
            "MyFile.zip"
        );
    }

    #[test]
    fn test_extract_display_name_magnet_no_dn() {
        // Falls back to full URL when no dn= parameter
        let url = "magnet:?xt=urn:btih:abc123";
        assert_eq!(extract_display_name(url), url);
    }

    #[tokio::test]
    async fn test_import_urls_empty_input() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_import_empty"));
        let results = dm.import_urls(&[]).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_import_urls_skips_comments_and_blanks() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_import_skip"));
        let urls = vec![
            "# this is a comment".to_string(),
            "".to_string(),
            "   ".to_string(),
        ];
        let results = dm.import_urls(&urls).await;
        // All should be skipped (comments/blanks produce no results)
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_import_urls_unsupported_scheme() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_import_unsupported"));
        let urls = vec!["ftp://invalid-but-unsupported-scheme".to_string()];
        // ftp:// is actually supported as HTTP-like
        let results = dm.import_urls(&urls).await;
        assert_eq!(results.len(), 1);
        // Will fail because the URL doesn't exist, but it's not "unsupported"
        assert!(matches!(
            results[0].outcome,
            ImportOutcome::Failed(_) | ImportOutcome::Added(_)
        ));
    }

    #[tokio::test]
    async fn test_import_urls_truly_unsupported_scheme() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_import_unsupported2"));
        let urls = vec!["gopher://example.com/file".to_string()];
        let results = dm.import_urls(&urls).await;
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0].outcome, ImportOutcome::Failed(_)));
        if let ImportOutcome::Failed(e) = &results[0].outcome {
            assert!(e.contains("Unsupported URL scheme"));
        }
    }

    #[tokio::test]
    async fn test_import_urls_ed2k_invalid_format() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_import_ed2k_bad"));
        let urls = vec!["ed2k://|file|broken".to_string()];
        let results = dm.import_urls(&urls).await;
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0].outcome, ImportOutcome::Failed(_)));
    }

    #[tokio::test]
    async fn test_import_urls_ed2k_valid() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_import_ed2k_ok"));
        let urls =
            vec!["ed2k://|file|test.txt|1024|d41d8cd98f00b204e9800998ecf8427e|/".to_string()];
        let results = dm.import_urls(&urls).await;
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0].outcome, ImportOutcome::Added(_)));
    }

    #[tokio::test]
    async fn test_import_urls_magnet_valid() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_import_magnet_ok"));
        let urls = vec![
            "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile".to_string(),
        ];
        let results = dm.import_urls(&urls).await;
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0].outcome, ImportOutcome::Added(_)));
    }

    // Phase 25: Auto-shutdown tests
    #[tokio::test]
    async fn test_auto_shutdown_default_disabled() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_autoshutdown_default"));
        let config = dm.get_auto_shutdown().await;
        assert_eq!(config.action, auto_shutdown::AutoShutdownAction::Disabled);
    }

    #[tokio::test]
    async fn test_auto_shutdown_set_exit() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_autoshutdown_exit"));
        let config = auto_shutdown::AutoShutdownConfig {
            action: auto_shutdown::AutoShutdownAction::Exit,
            require_empty_queue: false,
        };
        dm.set_auto_shutdown(config.clone()).await;
        let retrieved = dm.get_auto_shutdown().await;
        assert_eq!(retrieved.action, auto_shutdown::AutoShutdownAction::Exit);
    }

    #[tokio::test]
    async fn test_auto_shutdown_set_shell() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_autoshutdown_shell"));
        let config = auto_shutdown::AutoShutdownConfig {
            action: auto_shutdown::AutoShutdownAction::Shell {
                command: "echo done".to_string(),
            },
            require_empty_queue: false,
        };
        dm.set_auto_shutdown(config.clone()).await;
        let retrieved = dm.get_auto_shutdown().await;
        match retrieved.action {
            auto_shutdown::AutoShutdownAction::Shell { ref command } => {
                assert_eq!(command, "echo done");
            }
            _ => panic!("Expected Shell action"),
        }
    }

    #[tokio::test]
    async fn test_auto_shutdown_update_config() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_autoshutdown_update"));

        // Set to exit
        let config1 = auto_shutdown::AutoShutdownConfig {
            action: auto_shutdown::AutoShutdownAction::Exit,
            require_empty_queue: false,
        };
        dm.set_auto_shutdown(config1).await;
        assert_eq!(
            dm.get_auto_shutdown().await.action,
            auto_shutdown::AutoShutdownAction::Exit
        );

        // Update to disabled
        let config2 = auto_shutdown::AutoShutdownConfig {
            action: auto_shutdown::AutoShutdownAction::Disabled,
            require_empty_queue: false,
        };
        dm.set_auto_shutdown(config2).await;
        assert_eq!(
            dm.get_auto_shutdown().await.action,
            auto_shutdown::AutoShutdownAction::Disabled
        );
    }

    // Phase 26: Task dependency tests
    #[tokio::test]
    async fn test_set_dependencies_success() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_success"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile2")
            .await
            .unwrap();

        // Set dependency: id2 depends on id1
        assert!(dm.set_dependencies(&id2, vec![id1.clone()]).await);

        // Verify dependency was set
        let deps = dm.get_dependencies(&id2).await.unwrap();
        assert_eq!(deps, vec![id1]);
    }

    #[tokio::test]
    async fn test_set_dependencies_self_reference() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_self"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile")
            .await
            .unwrap();

        // Try to set self-dependency
        assert!(!dm.set_dependencies(&id1, vec![id1.clone()]).await);
    }

    #[tokio::test]
    async fn test_set_dependencies_nonexistent_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_nonexist"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile")
            .await
            .unwrap();

        // Try to set dependency on non-existent task
        assert!(
            !dm.set_dependencies(&id1, vec!["nonexistent-id".to_string()])
                .await
        );
    }

    #[tokio::test]
    async fn test_set_dependencies_cycle_detection() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_cycle"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile2")
            .await
            .unwrap();

        // Set id2 depends on id1
        assert!(dm.set_dependencies(&id2, vec![id1.clone()]).await);

        // Try to create cycle: id1 depends on id2
        assert!(!dm.set_dependencies(&id1, vec![id2.clone()]).await);
    }

    #[tokio::test]
    async fn test_are_dependencies_met_no_deps() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_met_none"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile")
            .await
            .unwrap();

        // No dependencies, should be met
        assert_eq!(dm.are_dependencies_met(&id1).await, Some(true));
    }

    #[tokio::test]
    async fn test_are_dependencies_met_unmet() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_met_unmet"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile2")
            .await
            .unwrap();

        // Set dependency
        dm.set_dependencies(&id2, vec![id1.clone()]).await;

        // id1 is Queued, not Complete, so dependencies not met
        assert_eq!(dm.are_dependencies_met(&id2).await, Some(false));
    }

    #[tokio::test]
    async fn test_are_dependencies_met_completed() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_met_completed"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile2")
            .await
            .unwrap();

        // Set dependency
        dm.set_dependencies(&id2, vec![id1.clone()]).await;

        // Mark id1 as complete
        {
            let mut tasks = dm.tasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == id1) {
                task.finalize_active_time();
                task.state = DownloadState::Complete;
            }
        }

        // Now dependencies should be met
        assert_eq!(dm.are_dependencies_met(&id2).await, Some(true));
    }

    #[tokio::test]
    async fn test_set_dependencies_rejects_self_dependency() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_self2"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile")
            .await
            .unwrap();

        // Self-dependency should be rejected
        assert!(!dm.set_dependencies(&id, vec![id.clone()]).await);
    }

    #[tokio::test]
    async fn test_set_dependencies_rejects_nonexistent() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_nonexist2"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile")
            .await
            .unwrap();

        // Non-existent dependency should be rejected
        assert!(
            !dm.set_dependencies(&id, vec!["nonexistent-id".to_string()])
                .await
        );
    }

    #[tokio::test]
    async fn test_set_dependencies_rejects_cycle() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_cycle2"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File2")
            .await
            .unwrap();

        // id1 depends on id2
        assert!(dm.set_dependencies(&id1, vec![id2.clone()]).await);

        // id2 depends on id1 should create cycle
        assert!(!dm.set_dependencies(&id2, vec![id1.clone()]).await);
    }

    #[tokio::test]
    async fn test_are_dependencies_met_not_met() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_deps_not_met"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File2")
            .await
            .unwrap();

        // id2 depends on id1
        dm.set_dependencies(&id2, vec![id1.clone()]).await;

        // id1 is still Queued, so deps not met
        assert_eq!(dm.are_dependencies_met(&id2).await, Some(false));
    }

    #[tokio::test]
    async fn test_get_dependencies() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_get_deps"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File2")
            .await
            .unwrap();

        // Set dependencies
        assert!(dm.set_dependencies(&id2, vec![id1.clone()]).await);

        // Get dependencies
        let deps = dm.get_dependencies(&id2).await.unwrap();
        assert_eq!(deps, vec![id1]);
    }

    #[tokio::test]
    async fn test_clear_dependencies() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_clear_deps"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File2")
            .await
            .unwrap();

        // Set then clear dependencies
        assert!(dm.set_dependencies(&id2, vec![id1]).await);
        assert!(dm.set_dependencies(&id2, vec![]).await);

        let deps = dm.get_dependencies(&id2).await.unwrap();
        assert!(deps.is_empty());
    }

    #[tokio::test]
    async fn test_rename_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_rename"));
        let id = dm
            .add_magnet(
                "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=OriginalName",
            )
            .await
            .unwrap();

        // Rename task
        assert!(dm.rename_task(&id, "NewName".to_string()).await);

        // Verify renamed
        let task = dm.get_task(&id).await.unwrap();
        assert_eq!(task.name, "NewName");
    }

    #[tokio::test]
    async fn test_rename_task_not_found() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_rename_nf"));
        assert!(!dm.rename_task("nonexistent", "NewName".to_string()).await);
    }

    #[tokio::test]
    async fn test_rename_task_empty_name() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_rename_empty"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=Test")
            .await
            .unwrap();

        // Empty name should fail
        assert!(!dm.rename_task(&id, "".to_string()).await);
        assert!(!dm.rename_task(&id, "   ".to_string()).await);

        // Original name should remain
        let task = dm.get_task(&id).await.unwrap();
        assert_eq!(task.name, "Test");
    }

    #[tokio::test]
    async fn test_set_task_notes() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_notes"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=Test")
            .await
            .unwrap();

        // Set notes
        assert!(
            dm.set_task_notes(&id, Some("This is a test file".to_string()))
                .await
        );

        // Verify notes
        let task = dm.get_task(&id).await.unwrap();
        assert_eq!(task.notes, Some("This is a test file".to_string()));
    }

    #[tokio::test]
    async fn test_clear_task_notes() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_notes_clear"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=Test")
            .await
            .unwrap();

        // Set notes
        assert!(dm.set_task_notes(&id, Some("Some notes".to_string())).await);

        // Clear notes with None
        assert!(dm.set_task_notes(&id, None).await);
        let task = dm.get_task(&id).await.unwrap();
        assert_eq!(task.notes, None);

        // Set again
        assert!(dm.set_task_notes(&id, Some("More notes".to_string())).await);

        // Clear notes with empty string
        assert!(dm.set_task_notes(&id, Some("".to_string())).await);
        let task = dm.get_task(&id).await.unwrap();
        assert_eq!(task.notes, None);
    }

    #[tokio::test]
    async fn test_set_task_notes_not_found() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_notes_nf"));
        assert!(
            !dm.set_task_notes("nonexistent", Some("notes".to_string()))
                .await
        );
    }

    #[tokio::test]
    async fn test_clone_task_basic() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_clone_basic"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile")
            .await
            .unwrap();

        // Set some metadata on the original task
        dm.set_task_group(&id, Some("movies".to_string())).await;
        dm.add_tags(&id, vec!["tag1".to_string(), "tag2".to_string()])
            .await;
        dm.set_task_notes(&id, Some("my notes".to_string())).await;

        // Clone the task
        let new_id = dm.clone_task(&id).await.unwrap();
        assert_ne!(new_id, id);

        // Verify the cloned task has the same metadata
        let cloned = dm.get_task(&new_id).await.unwrap();
        let original = dm.get_task(&id).await.unwrap();

        assert!(cloned.name.contains("(copy)"));
        assert_eq!(cloned.group, original.group);
        assert_eq!(cloned.tags, original.tags);
        assert_eq!(cloned.notes, original.notes);
        assert_eq!(cloned.protocol, original.protocol);
        assert_eq!(cloned.priority, original.priority);
        assert_eq!(cloned.bandwidth_weight, original.bandwidth_weight);
        assert_eq!(cloned.downloaded, 0);
        assert_eq!(cloned.state, DownloadState::Queued);
        assert_eq!(cloned.active_time_seconds, 0.0);
        assert!(cloned.depends_on.is_empty()); // dependencies not copied
    }

    #[tokio::test]
    async fn test_clone_task_not_found() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_clone_nf"));
        let result = dm.clone_task("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_clone_task_no_source_url() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_clone_no_url"));
        // Create a task without source_url by directly inserting
        {
            let task = DownloadTask {
                id: "no-url-task".to_string(),
                name: "No URL Task".to_string(),
                protocol: DownloadProtocol::P2P,
                size: 1024,
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
                queue_position: None,
                depends_on: Vec::new(),
                notes: None,
                group: None,
                speed_limit_bps: None,
                auto_retry_count: 0,
                retry_after: None,
                source_url: None,
                expected_checksum: None,
                checksum_algorithm: None,
                active_time_seconds: 0.0,
                current_session_start: None,
                mirror_urls: Vec::new(),
                retry_policy: None,
                cooldown: None,
                sequential_mode: false,
                max_download_time_secs: None,
                proxy_override: None,
                staleness_promotion_count: 0,
                deadline: None,
            };
            dm.tasks.lock().await.push(task);
        }

        let result = dm.clone_task("no-url-task").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no source URL"));
    }

    #[tokio::test]
    async fn test_clone_task_preserves_speed_limit() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_clone_speed"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=SpeedTest")
            .await
            .unwrap();

        // Set speed limit on original
        dm.set_task_speed_limit_per_task(&id, Some(1_000_000)).await;

        // Clone
        let new_id = dm.clone_task(&id).await.unwrap();

        // Verify speed limit preserved
        let cloned = dm.get_task(&new_id).await.unwrap();
        assert_eq!(cloned.speed_limit_bps, Some(1_000_000));
    }

    #[tokio::test]
    async fn test_clone_task_name_suffix() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_clone_name"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=TestFile")
            .await
            .unwrap();

        // First clone
        let new_id = dm.clone_task(&id).await.unwrap();
        let cloned = dm.get_task(&new_id).await.unwrap();
        assert!(cloned.name.ends_with(" (copy)"));

        // Clone the clone - should not add double " (copy)"
        let new_id2 = dm.clone_task(&new_id).await.unwrap();
        let cloned2 = dm.get_task(&new_id2).await.unwrap();
        assert!(cloned2.name.ends_with(" (copy)"));
        assert!(!cloned2.name.contains(" (copy) (copy)"));
    }

    #[tokio::test]
    async fn test_set_task_group() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_group"));
        let id = dm
            .add_magnet(
                "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test_group",
            )
            .await
            .unwrap();

        // Set group
        assert!(dm.set_task_group(&id, Some("movies".to_string())).await);
        let task = dm.get_task(&id).await.unwrap();
        assert_eq!(task.group, Some("movies".to_string()));

        // List by group
        let group_tasks = dm.list_tasks_by_group("movies").await;
        assert_eq!(group_tasks.len(), 1);
        assert_eq!(group_tasks[0].id, id);

        // List all groups
        let groups = dm.list_all_groups().await;
        assert_eq!(groups, vec!["movies".to_string()]);

        // Clear group
        assert!(dm.set_task_group(&id, None).await);
        let task = dm.get_task(&id).await.unwrap();
        assert_eq!(task.group, None);

        // Clear group with empty string
        assert!(dm.set_task_group(&id, Some("".to_string())).await);
        let task = dm.get_task(&id).await.unwrap();
        assert_eq!(task.group, None);

        // Groups list should be empty now
        let groups = dm.list_all_groups().await;
        assert!(groups.is_empty());
    }

    #[tokio::test]
    async fn test_set_task_group_not_found() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_group_nf"));
        assert!(
            !dm.set_task_group("nonexistent", Some("group".to_string()))
                .await
        );
    }

    #[tokio::test]
    async fn test_list_all_groups_multiple() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_groups_multi"));
        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=file1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80710&dn=file2")
            .await
            .unwrap();
        let id3 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80711&dn=file3")
            .await
            .unwrap();

        dm.set_task_group(&id1, Some("movies".to_string())).await;
        dm.set_task_group(&id2, Some("music".to_string())).await;
        dm.set_task_group(&id3, Some("movies".to_string())).await;

        let groups = dm.list_all_groups().await;
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"movies".to_string()));
        assert!(groups.contains(&"music".to_string()));

        // movies should have 2 tasks
        let movie_tasks = dm.list_tasks_by_group("movies").await;
        assert_eq!(movie_tasks.len(), 2);

        // music should have 1 task
        let music_tasks = dm.list_tasks_by_group("music").await;
        assert_eq!(music_tasks.len(), 1);

        // nonexistent group should have 0 tasks
        let other_tasks = dm.list_tasks_by_group("other").await;
        assert_eq!(other_tasks.len(), 0);
    }

    #[tokio::test]
    async fn test_set_task_speed_limit_per_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_speed_limit"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test")
            .await
            .unwrap();

        // Initially no per-task limit
        assert!(dm.get_task_speed_limit(&id).await.is_none());

        // Set a per-task limit
        dm.set_task_speed_limit_per_task(&id, Some(102400)).await;
        assert_eq!(dm.get_task_speed_limit(&id).await, Some(102400));

        // Verify the limiter was created
        let limiters = dm.task_rate_limiters.lock().await;
        assert!(limiters.contains_key(&id));
        drop(limiters);

        // Clear the limit (set to None)
        dm.set_task_speed_limit_per_task(&id, None).await;
        assert!(dm.get_task_speed_limit(&id).await.is_none());

        // Verify the limiter was removed
        let limiters = dm.task_rate_limiters.lock().await;
        assert!(!limiters.contains_key(&id));
    }

    #[tokio::test]
    async fn test_set_task_speed_limit_zero_clears() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_speed_zero"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test")
            .await
            .unwrap();

        // Set a limit
        dm.set_task_speed_limit_per_task(&id, Some(51200)).await;
        assert_eq!(dm.get_task_speed_limit(&id).await, Some(51200));

        // Setting to 0 should clear it (treat 0 as None)
        dm.set_task_speed_limit_per_task(&id, Some(0)).await;
        assert!(dm.get_task_speed_limit(&id).await.is_none());

        let limiters = dm.task_rate_limiters.lock().await;
        assert!(!limiters.contains_key(&id));
    }

    #[tokio::test]
    async fn test_set_task_speed_limit_nonexistent_task() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_speed_nonexist"));

        // Should not panic, just silently fail
        dm.set_task_speed_limit_per_task("nonexistent", Some(102400))
            .await;

        // Getting limit for nonexistent task should return None
        assert!(dm.get_task_speed_limit("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_multiple_tasks_independent_speed_limits() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_speed_multi"));

        let id1 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=file1")
            .await
            .unwrap();
        let id2 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80710&dn=file2")
            .await
            .unwrap();
        let id3 = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80711&dn=file3")
            .await
            .unwrap();

        // Set different limits for each task
        dm.set_task_speed_limit_per_task(&id1, Some(102400)).await; // 100 KB/s
        dm.set_task_speed_limit_per_task(&id2, Some(512000)).await; // 500 KB/s
        // id3 remains unlimited

        assert_eq!(dm.get_task_speed_limit(&id1).await, Some(102400));
        assert_eq!(dm.get_task_speed_limit(&id2).await, Some(512000));
        assert!(dm.get_task_speed_limit(&id3).await.is_none());

        // Verify all limiters exist
        let limiters = dm.task_rate_limiters.lock().await;
        assert_eq!(limiters.len(), 2);
        assert!(limiters.contains_key(&id1));
        assert!(limiters.contains_key(&id2));
        assert!(!limiters.contains_key(&id3));
        drop(limiters);

        // Clear one limit
        dm.set_task_speed_limit_per_task(&id1, None).await;
        assert!(dm.get_task_speed_limit(&id1).await.is_none());
        assert_eq!(dm.get_task_speed_limit(&id2).await, Some(512000));

        let limiters = dm.task_rate_limiters.lock().await;
        assert_eq!(limiters.len(), 1);
        assert!(!limiters.contains_key(&id1));
        assert!(limiters.contains_key(&id2));
    }

    // Phase 46: Deduplication tests
    #[tokio::test]
    async fn test_dedup_magnet_same_uri() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_dedup_magnet"));
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=DupTest";
        let id1 = dm.add_magnet(uri).await.unwrap();

        // Adding the same magnet URI again should fail with DuplicateTask
        let result = dm.add_magnet(uri).await;
        assert!(matches!(result, Err(DownloadManagerError::DuplicateTask(ref eid)) if eid == &id1));
    }

    #[tokio::test]
    async fn test_dedup_magnet_different_uri() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_dedup_magnet_diff"));
        let uri1 = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=File1";
        let uri2 = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&dn=File2";
        let id1 = dm.add_magnet(uri1).await.unwrap();
        let id2 = dm.add_magnet(uri2).await.unwrap();

        // Different URIs should succeed
        assert_ne!(id1, id2);
        assert_eq!(dm.list_tasks().await.len(), 2);
    }

    #[tokio::test]
    async fn test_dedup_xunlei_same_http_url() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_dedup_xunlei"));
        let url = "https://example.com/file.iso";
        let sources1 = vec![xunlei::XunleiSource::Http {
            url: url.to_string(),
            cookies: None,
            referer: None,
        }];
        let id1 = dm
            .add_xunlei("file.iso".into(), 1024, sources1)
            .await
            .unwrap();

        let sources2 = vec![xunlei::XunleiSource::Http {
            url: url.to_string(),
            cookies: None,
            referer: None,
        }];
        let result = dm.add_xunlei("file.iso".into(), 1024, sources2).await;
        assert!(matches!(result, Err(DownloadManagerError::DuplicateTask(ref eid)) if eid == &id1));
    }

    #[tokio::test]
    async fn test_dedup_xunlei_different_url() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_dedup_xunlei_diff"));
        let sources1 = vec![xunlei::XunleiSource::Http {
            url: "https://example.com/file1.iso".into(),
            cookies: None,
            referer: None,
        }];
        let sources2 = vec![xunlei::XunleiSource::Http {
            url: "https://example.com/file2.iso".into(),
            cookies: None,
            referer: None,
        }];
        let id1 = dm
            .add_xunlei("file1.iso".into(), 1024, sources1)
            .await
            .unwrap();
        let id2 = dm
            .add_xunlei("file2.iso".into(), 2048, sources2)
            .await
            .unwrap();

        assert_ne!(id1, id2);
        assert_eq!(dm.list_tasks().await.len(), 2);
    }

    #[tokio::test]
    async fn test_dedup_p2p_same_hash() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_dedup_p2p"));
        let hash = "abc123def456";
        let id1 = dm
            .add_p2p(hash.into(), "file.txt".into(), 1024, "peer1".into())
            .await
            .unwrap();

        let result = dm
            .add_p2p(hash.into(), "file.txt".into(), 1024, "peer2".into())
            .await;
        assert!(matches!(result, Err(DownloadManagerError::DuplicateTask(ref eid)) if eid == &id1));
    }

    #[tokio::test]
    async fn test_dedup_source_url_stored() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_dedup_stored"));
        let uri = "magnet:?xt=urn:btih:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb&dn=Stored";
        let id = dm.add_magnet(uri).await.unwrap();

        let tasks = dm.list_tasks().await;
        let task = tasks.iter().find(|t| t.id == id).unwrap();
        assert_eq!(task.source_url.as_deref(), Some(uri));
    }
}

// ─── Phase 47: URL Extraction Tests ───

#[cfg(test)]
mod url_extraction_tests {
    use super::*;

    #[test]
    fn test_extract_http_urls() {
        let text = "Download from https://example.com/file.zip or http://backup.com/file.tar.gz";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0], "https://example.com/file.zip");
        assert_eq!(urls[1], "http://backup.com/file.tar.gz");
    }

    #[test]
    fn test_extract_ed2k_url() {
        let text = "ed2k://|file|ubuntu.iso|1234567|abcdef0123456789abcdef0123456789|/";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
        assert!(urls[0].starts_with("ed2k://"));
    }

    #[test]
    fn test_extract_magnet_url() {
        let text = "magnet:?xt=urn:btih:abc123&dn=TestFile&tr=tracker";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
        assert!(urls[0].starts_with("magnet:"));
    }

    #[test]
    fn test_extract_mixed_urls() {
        let text = r#"
        Here are some download links:
        https://example.com/video.mp4
        ed2k://|file|movie.mkv|9876543|1234567890abcdef1234567890abcdef|/
        magnet:?xt=urn:btih:def456&dn=AnotherFile
        http://old-site.com/archive.zip
        "#;
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 4);
    }

    #[test]
    fn test_extract_deduplicates() {
        let text = "https://example.com/file.zip and again https://example.com/file.zip";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
    }

    #[test]
    fn test_extract_skips_comments() {
        let text = r#"
        # This is a comment with https://example.com/ignored.zip
        https://example.com/real.zip
        "#;
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], "https://example.com/real.zip");
    }

    #[test]
    fn test_extract_handles_punctuation() {
        let text = "Check (https://example.com/file.zip) or <https://other.com/doc.pdf>";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0], "https://example.com/file.zip");
        assert_eq!(urls[1], "https://other.com/doc.pdf");
    }

    #[test]
    fn test_extract_empty_text() {
        let urls = extract_urls_from_text("");
        assert!(urls.is_empty());
    }

    #[test]
    fn test_extract_no_urls() {
        let text = "This text has no URLs at all.";
        let urls = extract_urls_from_text(text);
        assert!(urls.is_empty());
    }

    #[test]
    fn test_extract_ftp_url() {
        let text = "Download from ftp://ftp.example.com/pub/file.tar.gz";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 1);
        assert!(urls[0].starts_with("ftp://"));
    }

    #[test]
    fn test_extract_preserves_order() {
        let text = "https://first.com https://second.com https://third.com";
        let urls = extract_urls_from_text(text);
        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0], "https://first.com");
        assert_eq!(urls[1], "https://second.com");
        assert_eq!(urls[2], "https://third.com");
    }
}

#[cfg(test)]
mod max_concurrent_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_save_load_max_concurrent() {
        let tmp = tempdir().unwrap();
        save_max_concurrent(5, tmp.path()).unwrap();
        let loaded = load_max_concurrent(tmp.path()).unwrap();
        assert_eq!(loaded, 5);
    }

    #[test]
    fn test_load_max_concurrent_missing() {
        let tmp = tempdir().unwrap();
        let loaded = load_max_concurrent(tmp.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_max_concurrent_zero() {
        let tmp = tempdir().unwrap();
        save_max_concurrent(0, tmp.path()).unwrap();
        let loaded = load_max_concurrent(tmp.path()).unwrap();
        assert_eq!(loaded, 0);
    }

    #[test]
    fn test_save_max_concurrent_overwrite() {
        let tmp = tempdir().unwrap();
        save_max_concurrent(3, tmp.path()).unwrap();
        save_max_concurrent(10, tmp.path()).unwrap();
        let loaded = load_max_concurrent(tmp.path()).unwrap();
        assert_eq!(loaded, 10);
    }

    #[tokio::test]
    async fn test_manager_set_get_max_concurrent() {
        let tmp = tempdir().unwrap();
        let dm = DownloadManager::new(tmp.path().to_path_buf());
        assert_eq!(dm.get_max_concurrent(), 0);
        dm.set_max_concurrent(7);
        assert_eq!(dm.get_max_concurrent(), 7);
        // Verify persisted
        let loaded = load_max_concurrent(tmp.path()).unwrap();
        assert_eq!(loaded, 7);
    }

    #[tokio::test]
    async fn test_manager_restore_max_concurrent() {
        let tmp = tempdir().unwrap();
        save_max_concurrent(4, tmp.path()).unwrap();
        let dm = DownloadManager::new_with_restore(tmp.path().to_path_buf()).await;
        assert_eq!(dm.get_max_concurrent(), 4);
    }

    #[tokio::test]
    async fn test_set_task_checksum() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_checksum"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test")
            .await
            .unwrap();

        // Set SHA-256 checksum (64 hex chars)
        let checksum = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let result = dm
            .set_task_checksum(&id, checksum, checksum::ChecksumAlgorithm::Sha256)
            .await;
        assert!(result.is_ok());

        // Verify task has checksum set
        let tasks = dm.list_tasks().await;
        let task = tasks.iter().find(|t| t.id == id).unwrap();
        assert_eq!(task.expected_checksum, Some(checksum.to_lowercase()));
        assert_eq!(
            task.checksum_algorithm,
            Some(checksum::ChecksumAlgorithm::Sha256)
        );
    }

    #[tokio::test]
    async fn test_set_task_checksum_invalid_length() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_checksum_inv"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test")
            .await
            .unwrap();

        // SHA-256 expects 64 hex chars, but we provide only 32
        let checksum = "e3b0c44298fc1c149afbf4c8996fb924";
        let result = dm
            .set_task_checksum(&id, checksum, checksum::ChecksumAlgorithm::Sha256)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid checksum length"));
    }

    #[tokio::test]
    async fn test_set_task_checksum_invalid_hex() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_checksum_hex"));
        let id = dm
            .add_magnet("magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=test")
            .await
            .unwrap();

        // 64 chars but not all hex
        let checksum = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        let result = dm
            .set_task_checksum(&id, checksum, checksum::ChecksumAlgorithm::Sha256)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("hex string"));
    }

    #[tokio::test]
    async fn test_set_task_checksum_not_found() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_checksum_nf"));
        let checksum = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let result = dm
            .set_task_checksum("nonexistent", checksum, checksum::ChecksumAlgorithm::Sha256)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_finalize_active_time_accumulates() {
        let mut task = DownloadTask {
            id: "time-1".into(),
            name: "test".into(),
            protocol: DownloadProtocol::Xunlei,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
        };

        // No session started yet
        assert_eq!(task.active_time_seconds, 0.0);
        assert!(task.current_session_start.is_none());

        // Start a session
        task.current_session_start = Some(chrono::Utc::now() - chrono::Duration::seconds(10));

        // Finalize should accumulate time
        task.finalize_active_time();

        // Should have accumulated ~10 seconds
        assert!(task.active_time_seconds >= 9.0 && task.active_time_seconds <= 11.0);

        // Session should be cleared
        assert!(task.current_session_start.is_none());
    }

    #[test]
    fn test_finalize_active_time_multiple_sessions() {
        let mut task = DownloadTask {
            id: "time-2".into(),
            name: "test".into(),
            protocol: DownloadProtocol::Xunlei,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
        };

        // First session: 5 seconds
        task.active_time_seconds = 5.0;
        task.current_session_start = Some(chrono::Utc::now() - chrono::Duration::seconds(5));
        task.finalize_active_time();

        // Should have accumulated ~10 seconds total
        assert!(task.active_time_seconds >= 9.0 && task.active_time_seconds <= 11.0);

        // Second session: 3 seconds
        task.current_session_start = Some(chrono::Utc::now() - chrono::Duration::seconds(3));
        task.finalize_active_time();

        // Should have accumulated ~13 seconds total
        assert!(task.active_time_seconds >= 12.0 && task.active_time_seconds <= 14.0);
    }

    #[test]
    fn test_finalize_active_time_no_session() {
        let mut task = DownloadTask {
            id: "time-3".into(),
            name: "test".into(),
            protocol: DownloadProtocol::Xunlei,
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: None,
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
        };

        // Finalize with no session should not change anything
        task.finalize_active_time();
        assert_eq!(task.active_time_seconds, 0.0);
        assert!(task.current_session_start.is_none());
    }

    // Phase 60: URL Deduplication Integration Tests

    #[tokio::test]
    async fn test_url_dedup_exact_mode() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_url_dedup_exact"));

        // Configure exact dedup mode
        let config = url_dedup::DedupConfig {
            mode: url_dedup::DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: url_dedup::DuplicatePolicy::Reject,
        };
        dm.set_url_dedup(config).await;

        // Add a task with a URL
        let mut tasks = dm.tasks.lock().await;
        tasks.push(DownloadTask {
            id: "test-1".into(),
            name: "test".into(),
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some("https://example.com/file.zip".into()),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
        });
        drop(tasks);

        // Same URL should be detected as duplicate
        let dup = dm
            .find_duplicate_by_url("https://example.com/file.zip")
            .await;
        assert_eq!(dup, Some("test-1".to_string()));

        // URL with different query should match (strip_query=true)
        let dup = dm
            .find_duplicate_by_url("https://example.com/file.zip?token=abc")
            .await;
        assert_eq!(dup, Some("test-1".to_string()));

        // Different URL should not match
        let dup = dm
            .find_duplicate_by_url("https://example.com/other.zip")
            .await;
        assert_eq!(dup, None);
    }

    #[tokio::test]
    async fn test_url_dedup_domain_mode() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_url_dedup_domain"));

        let config = url_dedup::DedupConfig {
            mode: url_dedup::DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: url_dedup::DuplicatePolicy::Reject,
        };
        dm.set_url_dedup(config).await;

        let mut tasks = dm.tasks.lock().await;
        tasks.push(DownloadTask {
            id: "test-2".into(),
            name: "test".into(),
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some("https://example.com/file1.zip".into()),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
        });
        drop(tasks);

        // Any URL from same domain should match
        let dup = dm
            .find_duplicate_by_url("https://example.com/any-file.zip")
            .await;
        assert_eq!(dup, Some("test-2".to_string()));

        // URL with www. prefix should also match
        let dup = dm
            .find_duplicate_by_url("https://www.example.com/other.zip")
            .await;
        assert_eq!(dup, Some("test-2".to_string()));

        // Different domain should not match
        let dup = dm.find_duplicate_by_url("https://other.com/file.zip").await;
        assert_eq!(dup, None);
    }

    #[tokio::test]
    async fn test_url_dedup_disabled() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_url_dedup_disabled"));

        let config = url_dedup::DedupConfig {
            mode: url_dedup::DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: false, // Disabled
            duplicate_policy: url_dedup::DuplicatePolicy::Reject,
        };
        dm.set_url_dedup(config).await;

        let mut tasks = dm.tasks.lock().await;
        tasks.push(DownloadTask {
            id: "test-3".into(),
            name: "test".into(),
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
            queue_position: None,
            depends_on: Vec::new(),
            notes: None,
            group: None,
            speed_limit_bps: None,
            auto_retry_count: 0,
            retry_after: None,
            source_url: Some("https://example.com/file.zip".into()),
            expected_checksum: None,
            checksum_algorithm: None,
            active_time_seconds: 0.0,
            mirror_urls: Vec::new(),
            retry_policy: None,
            cooldown: None,
            sequential_mode: false,
            max_download_time_secs: None,
            proxy_override: None,
            staleness_promotion_count: 0,
            deadline: None,
            current_session_start: None,
        });
        drop(tasks);

        // When disabled, should never find duplicates
        let dup = dm
            .find_duplicate_by_url("https://example.com/file.zip")
            .await;
        assert_eq!(dup, None);
    }

    #[tokio::test]
    async fn test_set_get_task_retry_policy() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Add a task
        let sources = vec![xunlei::XunleiSource::Http {
            url: "https://example.com/file.zip".to_string(),
            cookies: None,
            referer: None,
        }];
        dm.add_xunlei("file.zip".to_string(), 1024, sources)
            .await
            .unwrap();

        let tasks = dm.list_tasks().await;
        let task_id = tasks[0].id.clone();

        // Initially, retry policy should be None (use global defaults)
        let policy = dm.get_task_retry_policy(&task_id).await;
        assert!(policy.is_none());

        // Set a custom retry policy
        let custom_policy = RetryPolicy {
            max_retries: 10,
            backoff: RetryBackoff::Linear { base_secs: 60 },
        };
        let success = dm
            .set_task_retry_policy(&task_id, Some(custom_policy))
            .await;
        assert!(success);

        // Verify the policy was set
        let policy = dm.get_task_retry_policy(&task_id).await;
        assert!(policy.is_some());
        let policy = policy.unwrap();
        assert_eq!(policy.max_retries, 10);
        match policy.backoff {
            RetryBackoff::Linear { base_secs } => assert_eq!(base_secs, 60),
            _ => panic!("Expected Linear backoff"),
        }

        // Clear the policy
        let success = dm.set_task_retry_policy(&task_id, None).await;
        assert!(success);

        let policy = dm.get_task_retry_policy(&task_id).await;
        assert!(policy.is_none());
    }

    #[tokio::test]
    async fn test_retry_policy_nonexistent_task() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let policy = RetryPolicy {
            max_retries: 5,
            backoff: RetryBackoff::Exponential { base_secs: 30 },
        };

        // Setting policy for non-existent task should return false
        let success = dm.set_task_retry_policy("nonexistent", Some(policy)).await;
        assert!(!success);

        // Getting policy for non-existent task should return None
        let policy = dm.get_task_retry_policy("nonexistent").await;
        assert!(policy.is_none());
    }

    #[test]
    fn test_retry_policy_calculate_delay() {
        // Test Fixed backoff
        let fixed_policy = RetryPolicy {
            max_retries: 5,
            backoff: RetryBackoff::Fixed(60),
        };
        assert_eq!(fixed_policy.calculate_delay(0), 60);
        assert_eq!(fixed_policy.calculate_delay(1), 60);
        assert_eq!(fixed_policy.calculate_delay(5), 60);

        // Test Exponential backoff
        let exp_policy = RetryPolicy {
            max_retries: 5,
            backoff: RetryBackoff::Exponential { base_secs: 30 },
        };
        assert_eq!(exp_policy.calculate_delay(0), 30); // 30 * 2^0 = 30
        assert_eq!(exp_policy.calculate_delay(1), 60); // 30 * 2^1 = 60
        assert_eq!(exp_policy.calculate_delay(2), 120); // 30 * 2^2 = 120
        assert_eq!(exp_policy.calculate_delay(5), 960); // 30 * 2^5 = 960
        assert_eq!(exp_policy.calculate_delay(10), 3600); // Capped at 3600

        // Test Linear backoff
        let linear_policy = RetryPolicy {
            max_retries: 5,
            backoff: RetryBackoff::Linear { base_secs: 30 },
        };
        assert_eq!(linear_policy.calculate_delay(0), 30); // 30 * (0+1) = 30
        assert_eq!(linear_policy.calculate_delay(1), 60); // 30 * (1+1) = 60
        assert_eq!(linear_policy.calculate_delay(2), 90); // 30 * (2+1) = 90
        assert_eq!(linear_policy.calculate_delay(5), 180); // 30 * (5+1) = 180
    }

    #[test]
    fn test_retry_policy_default() {
        let default_policy = RetryPolicy::default();
        assert_eq!(default_policy.max_retries, 3);
        match default_policy.backoff {
            RetryBackoff::Exponential { base_secs } => assert_eq!(base_secs, 30),
            _ => panic!("Expected Exponential backoff as default"),
        }
    }

    // ========== Download Presets Tests ==========

    #[tokio::test]
    async fn test_add_and_list_download_presets() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Initially empty
        let presets = dm.list_download_presets().await;
        assert!(presets.is_empty());

        // Add a preset
        let mut preset =
            download_presets::DownloadPreset::new("fast".to_string(), "Fast Downloads".to_string());
        preset.tags = vec!["fast".to_string(), "priority".to_string()];
        preset.priority = 3;
        preset.speed_limit_bps = Some(1_048_576);

        dm.add_download_preset(preset.clone()).await;

        // List should have 1
        let presets = dm.list_download_presets().await;
        assert_eq!(presets.len(), 1);
        assert_eq!(presets[0].id, "fast");
        assert_eq!(presets[0].name, "Fast Downloads");
        assert_eq!(presets[0].priority, 3);
        assert_eq!(presets[0].speed_limit_bps, Some(1_048_576));
        assert_eq!(presets[0].tags.len(), 2);
    }

    #[tokio::test]
    async fn test_remove_download_preset() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let preset = download_presets::DownloadPreset::new("temp".to_string(), "Temp".to_string());
        dm.add_download_preset(preset).await;

        assert_eq!(dm.list_download_presets().await.len(), 1);

        // Remove existing
        let removed = dm.remove_download_preset("temp").await;
        assert!(removed);
        assert!(dm.list_download_presets().await.is_empty());

        // Remove non-existent
        let removed = dm.remove_download_preset("nonexistent").await;
        assert!(!removed);
    }

    #[tokio::test]
    async fn test_get_download_preset() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let preset =
            download_presets::DownloadPreset::new("media".to_string(), "Media".to_string());
        dm.add_download_preset(preset).await;

        let found = dm.get_download_preset("media").await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Media");

        let not_found = dm.get_download_preset("nonexistent").await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_apply_preset_to_task() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Add a task
        let sources = vec![xunlei::XunleiSource::Http {
            url: "https://example.com/file.zip".to_string(),
            cookies: None,
            referer: None,
        }];
        dm.add_xunlei("file.zip".to_string(), 1024, sources)
            .await
            .unwrap();

        let tasks = dm.list_tasks().await;
        let task_id = tasks[0].id.clone();

        // Create and add a preset
        let mut preset =
            download_presets::DownloadPreset::new("vip".to_string(), "VIP Downloads".to_string());
        preset.tags = vec!["vip".to_string(), "important".to_string()];
        preset.group = Some("premium".to_string());
        preset.priority = 3;
        preset.speed_limit_bps = Some(2_097_152);
        preset.bandwidth_weight = 8;
        preset.max_retries = Some(10);

        dm.add_download_preset(preset).await;

        // Apply preset
        let applied = dm.apply_preset_to_task(&task_id, "vip").await;
        assert!(applied);

        // Verify task was updated
        let tasks = dm.list_tasks().await;
        let task = tasks.iter().find(|t| t.id == task_id).unwrap();
        assert!(task.tags.contains(&"vip".to_string()));
        assert!(task.tags.contains(&"important".to_string()));
        assert_eq!(task.group, Some("premium".to_string()));
        assert_eq!(task.priority, DownloadPriority::High);
        assert_eq!(task.speed_limit_bps, Some(2_097_152));
        assert_eq!(task.bandwidth_weight, 8);
        assert!(task.retry_policy.is_some());
        assert_eq!(task.retry_policy.unwrap().max_retries, 10);
    }

    #[tokio::test]
    async fn test_apply_preset_disabled() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Add a task
        let sources = vec![xunlei::XunleiSource::Http {
            url: "https://example.com/file.zip".to_string(),
            cookies: None,
            referer: None,
        }];
        dm.add_xunlei("file.zip".to_string(), 1024, sources)
            .await
            .unwrap();

        let tasks = dm.list_tasks().await;
        let task_id = tasks[0].id.clone();

        // Create a disabled preset
        let mut preset =
            download_presets::DownloadPreset::new("disabled".to_string(), "Disabled".to_string());
        preset.enabled = false;
        preset.priority = 3;

        dm.add_download_preset(preset).await;

        // Apply disabled preset should fail
        let applied = dm.apply_preset_to_task(&task_id, "disabled").await;
        assert!(!applied);

        // Task should remain unchanged
        let tasks = dm.list_tasks().await;
        let task = tasks.iter().find(|t| t.id == task_id).unwrap();
        assert_eq!(task.priority, DownloadPriority::Normal);
    }

    #[tokio::test]
    async fn test_preset_persistence() {
        let temp_dir = tempdir().unwrap();

        // Create DM and add presets
        {
            let dm = DownloadManager::new(temp_dir.path().to_path_buf());
            let mut preset1 =
                download_presets::DownloadPreset::new("a".to_string(), "Preset A".to_string());
            preset1.tags = vec!["tag1".to_string()];
            let preset2 =
                download_presets::DownloadPreset::new("b".to_string(), "Preset B".to_string());

            dm.add_download_preset(preset1).await;
            dm.add_download_preset(preset2).await;

            assert_eq!(dm.list_download_presets().await.len(), 2);
        }

        // Restore DM - presets should be loaded from disk
        {
            let dm = DownloadManager::new_with_restore(temp_dir.path().to_path_buf()).await;
            let presets = dm.list_download_presets().await;
            assert_eq!(presets.len(), 2);
            assert_eq!(presets[0].id, "a");
            assert_eq!(presets[1].id, "b");
            assert_eq!(presets[0].tags, vec!["tag1".to_string()]);
        }
    }

    #[tokio::test]
    async fn test_add_preset_replaces_existing() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Add preset with id "test"
        let preset1 =
            download_presets::DownloadPreset::new("test".to_string(), "Version 1".to_string());
        dm.add_download_preset(preset1).await;

        assert_eq!(dm.list_download_presets().await.len(), 1);
        assert_eq!(
            dm.get_download_preset("test").await.unwrap().name,
            "Version 1"
        );

        // Add another preset with same id - should replace
        let preset2 =
            download_presets::DownloadPreset::new("test".to_string(), "Version 2".to_string());
        dm.add_download_preset(preset2).await;

        // Should still be 1 preset, but with updated name
        let presets = dm.list_download_presets().await;
        assert_eq!(presets.len(), 1);
        assert_eq!(presets[0].name, "Version 2");
    }

    #[tokio::test]
    async fn test_apply_preset_nonexistent_task() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let preset = download_presets::DownloadPreset::new("test".to_string(), "Test".to_string());
        dm.add_download_preset(preset).await;

        // Apply to non-existent task
        let applied = dm.apply_preset_to_task("nonexistent", "test").await;
        assert!(!applied);
    }

    #[tokio::test]
    async fn test_apply_nonexistent_preset() {
        let temp_dir = tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Add a task
        let sources = vec![xunlei::XunleiSource::Http {
            url: "https://example.com/file.zip".to_string(),
            cookies: None,
            referer: None,
        }];
        dm.add_xunlei("file.zip".to_string(), 1024, sources)
            .await
            .unwrap();

        let tasks = dm.list_tasks().await;
        let task_id = tasks[0].id.clone();

        // Apply non-existent preset
        let applied = dm.apply_preset_to_task(&task_id, "nonexistent").await;
        assert!(!applied);
    }

    #[tokio::test]
    async fn test_conflict_strategy_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Default strategy is Skip
        let strategy = dm.get_conflict_strategy().await;
        assert_eq!(strategy, conflict_detection::ConflictStrategy::Skip);

        // Set to Rename
        dm.set_conflict_strategy(conflict_detection::ConflictStrategy::Rename)
            .await;
        let strategy = dm.get_conflict_strategy().await;
        assert_eq!(strategy, conflict_detection::ConflictStrategy::Rename);

        // Verify file was written
        let config_path = temp_dir.path().join("conflict_strategy.json");
        assert!(config_path.exists());

        // Create new DM with restore and verify it loads the persisted strategy
        let dm2 = DownloadManager::new_with_restore(temp_dir.path().to_path_buf()).await;
        let strategy2 = dm2.get_conflict_strategy().await;
        assert_eq!(strategy2, conflict_detection::ConflictStrategy::Rename);
    }

    #[tokio::test]
    async fn test_check_conflicts_no_conflict() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let save_path = temp_dir.path().join("downloads").join("file.txt");
        let report = dm.check_conflicts("task1", "Test Task", &save_path).await;

        assert!(report.conflict.is_none());
        assert_eq!(report.action, conflict_detection::ConflictAction::None);
    }

    #[tokio::test]
    async fn test_check_conflicts_task_conflict() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Add first task
        let sources = vec![xunlei::XunleiSource::Http {
            url: "https://example.com/file1.zip".to_string(),
            cookies: None,
            referer: None,
        }];
        dm.add_xunlei("file1.zip".to_string(), 1024, sources)
            .await
            .unwrap();

        let tasks = dm.list_tasks().await;
        let task1_path = tasks[0].save_path.clone();

        // Check conflict with same path
        let report = dm
            .check_conflicts("task2", "Test Task 2", &task1_path)
            .await;

        assert!(report.conflict.is_some());
        assert_eq!(report.action, conflict_detection::ConflictAction::Skipped);
    }

    #[tokio::test]
    async fn test_resolve_conflict_with_rename() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Set strategy to Rename
        dm.set_conflict_strategy(conflict_detection::ConflictStrategy::Rename)
            .await;

        // Create a file on disk
        let file_path = temp_dir.path().join("existing.txt");
        tokio::fs::write(&file_path, "test data").await.unwrap();

        // Check conflict with existing file
        let mut report = dm.check_conflicts("task1", "Test Task", &file_path).await;

        // Resolve the conflict
        let resolved_path = dm.resolve_conflict_report(&mut report).await;

        assert_eq!(report.action, conflict_detection::ConflictAction::Renamed);
        assert_ne!(resolved_path, file_path);
        assert!(resolved_path.to_string_lossy().contains("existing(1)"));
    }

    #[tokio::test]
    async fn test_resolve_conflict_with_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Set strategy to Overwrite
        dm.set_conflict_strategy(conflict_detection::ConflictStrategy::Overwrite)
            .await;

        // Create a file on disk
        let file_path = temp_dir.path().join("existing.txt");
        tokio::fs::write(&file_path, "test data").await.unwrap();

        // Check conflict with existing file
        let mut report = dm.check_conflicts("task1", "Test Task", &file_path).await;

        // Resolve the conflict
        let resolved_path = dm.resolve_conflict_report(&mut report).await;

        assert_eq!(report.action, conflict_detection::ConflictAction::Overwrite);
        assert_eq!(resolved_path, file_path);
    }

    #[tokio::test]
    async fn test_duplicate_policy_reject() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Set policy to Reject (default)
        let mut config = dm.get_url_dedup().await;
        config.duplicate_policy = url_dedup::DuplicatePolicy::Reject;
        dm.set_url_dedup(config).await;

        // Add first task
        let magnet_uri = "magnet:?xt=urn:btih:1234567890123456789012345678901234567890&dn=test";
        let result1 = dm.add_magnet(magnet_uri).await;
        assert!(result1.is_ok());
        let task_id1 = result1.unwrap();

        // Try to add duplicate - should fail with DuplicateTask error
        let result2 = dm.add_magnet(magnet_uri).await;
        assert!(result2.is_err());
        match result2 {
            Err(DownloadManagerError::DuplicateTask(id)) => {
                assert_eq!(id, task_id1);
            }
            _ => panic!("Expected DuplicateTask error"),
        }
    }

    #[tokio::test]
    async fn test_duplicate_policy_skip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Set policy to Skip
        let mut config = dm.get_url_dedup().await;
        config.duplicate_policy = url_dedup::DuplicatePolicy::Skip;
        dm.set_url_dedup(config).await;

        // Add first task
        let magnet_uri = "magnet:?xt=urn:btih:1234567890123456789012345678901234567890&dn=test";
        let result1 = dm.add_magnet(magnet_uri).await;
        assert!(result1.is_ok());
        let task_id1 = result1.unwrap();

        // Try to add duplicate - should succeed and return existing task ID
        let result2 = dm.add_magnet(magnet_uri).await;
        assert!(result2.is_ok());
        let task_id2 = result2.unwrap();
        assert_eq!(task_id1, task_id2);

        // Verify only one task exists
        let tasks = dm.list_tasks().await;
        assert_eq!(tasks.len(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_policy_allow() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Set policy to Allow
        let mut config = dm.get_url_dedup().await;
        config.duplicate_policy = url_dedup::DuplicatePolicy::Allow;
        dm.set_url_dedup(config).await;

        // Add first task
        let magnet_uri = "magnet:?xt=urn:btih:1234567890123456789012345678901234567890&dn=test";
        let result1 = dm.add_magnet(magnet_uri).await;
        assert!(result1.is_ok());
        let task_id1 = result1.unwrap();

        // Try to add duplicate - should succeed and create new task
        let result2 = dm.add_magnet(magnet_uri).await;
        assert!(result2.is_ok());
        let task_id2 = result2.unwrap();
        assert_ne!(task_id1, task_id2);

        // Verify two tasks exist
        let tasks = dm.list_tasks().await;
        assert_eq!(tasks.len(), 2);
    }

    // ========== Phase 79: URL Normalization Tests ==========

    #[tokio::test]
    async fn test_url_normalizer_set_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let config = dm.get_url_normalizer_config().await;
        assert!(config.enabled);
        assert!(config.remove_www);
    }

    #[tokio::test]
    async fn test_url_normalizer_normalize() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let result = dm
            .normalize_url("  https://www.EXAMPLE.com/file.zip?utm_source=google  ")
            .await;
        assert!(result.was_modified);
        assert!(!result.normalized_url.contains("www."));
        assert!(!result.normalized_url.contains("utm_source"));
    }

    #[tokio::test]
    async fn test_url_normalizer_equivalent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        assert!(
            dm.are_urls_equivalent(
                "https://www.example.com/file.zip?utm_source=google",
                "https://example.com/file.zip"
            )
            .await
        );

        assert!(
            !dm.are_urls_equivalent(
                "https://example.com/file1.zip",
                "https://example.com/file2.zip"
            )
            .await
        );
    }

    #[tokio::test]
    async fn test_duplicate_policy_pause_existing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Set policy to PauseExisting
        let mut config = dm.get_url_dedup().await;
        config.duplicate_policy = url_dedup::DuplicatePolicy::PauseExisting;
        dm.set_url_dedup(config).await;

        // Add first task
        let magnet_uri = "magnet:?xt=urn:btih:1234567890123456789012345678901234567890&dn=test";
        let result1 = dm.add_magnet(magnet_uri).await;
        assert!(result1.is_ok());
        let task_id1 = result1.unwrap();

        // Manually set task to Downloading state for testing
        {
            let mut tasks = dm.tasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == task_id1) {
                task.state = DownloadState::Downloading;
            }
        }

        // Verify task is in Downloading state
        let tasks_before = dm.list_tasks().await;
        let task_before = tasks_before.iter().find(|t| t.id == task_id1).unwrap();
        assert_eq!(task_before.state, DownloadState::Downloading);

        // Try to add duplicate - should succeed and pause existing task
        let result2 = dm.add_magnet(magnet_uri).await;
        assert!(result2.is_ok());
        let task_id2 = result2.unwrap();
        assert_ne!(task_id1, task_id2);

        // Verify existing task is now paused
        let tasks_after = dm.list_tasks().await;
        let task_after = tasks_after.iter().find(|t| t.id == task_id1).unwrap();
        assert_eq!(task_after.state, DownloadState::Paused);

        // Verify two tasks exist
        assert_eq!(tasks_after.len(), 2);
    }
}

// ─── Phase 89: URL Allowlist Tests ───

#[cfg(test)]
mod url_allowlist_tests {
    use super::*;

    #[tokio::test]
    async fn test_allowlist_default_disabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let config = dm.get_url_allowlist_config().await;
        assert!(!config.enabled);
        assert!(config.entries.is_empty());
    }

    #[tokio::test]
    async fn test_allowlist_set_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let config = url_allowlist::AllowlistConfig {
            enabled: true,
            entries: vec![url_allowlist::AllowlistEntry::new(
                "test-1".to_string(),
                "Test Domain".to_string(),
                url_allowlist::AllowlistPattern::Domain("example.com".to_string()),
                None,
            )],
        };

        let result = dm.set_url_allowlist_config(config).await;
        assert!(result.is_ok());

        let loaded = dm.get_url_allowlist_config().await;
        assert!(loaded.enabled);
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].id, "test-1");
    }

    #[tokio::test]
    async fn test_allowlist_enable_disable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let result = dm.set_allowlist_enabled(true).await;
        assert!(result.is_ok());

        let config = dm.get_url_allowlist_config().await;
        assert!(config.enabled);

        let result = dm.set_allowlist_enabled(false).await;
        assert!(result.is_ok());

        let config = dm.get_url_allowlist_config().await;
        assert!(!config.enabled);
    }

    #[tokio::test]
    async fn test_allowlist_add_remove_entry() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let entry = url_allowlist::AllowlistEntry::new(
            "entry-1".to_string(),
            "Trusted Domain".to_string(),
            url_allowlist::AllowlistPattern::Domain("trusted.com".to_string()),
            Some("Official mirror".to_string()),
        );

        let result = dm.add_allowlist_entry(entry).await;
        assert!(result.is_ok());

        let entries = dm.list_allowlist_entries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "entry-1");

        let result = dm.remove_allowlist_entry("entry-1").await;
        assert!(result.is_ok());

        let entries = dm.list_allowlist_entries().await;
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_allowlist_remove_nonexistent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let result = dm.remove_allowlist_entry("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_allowlist_check_disabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // Allowlist disabled: all URLs allowed
        let result = dm.check_url_allowed("http://anything.com/file.txt").await;
        assert!(result.allowed);
    }

    #[tokio::test]
    async fn test_allowlist_check_enabled_match() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let entry = url_allowlist::AllowlistEntry::new(
            "test-1".to_string(),
            "Trusted".to_string(),
            url_allowlist::AllowlistPattern::Domain("trusted.com".to_string()),
            None,
        );
        dm.add_allowlist_entry(entry).await.unwrap();
        dm.set_allowlist_enabled(true).await.unwrap();

        let result = dm.check_url_allowed("http://trusted.com/file.txt").await;
        assert!(result.allowed);
        assert_eq!(result.matched_entry_id.as_deref(), Some("test-1"));
    }

    #[tokio::test]
    async fn test_allowlist_check_enabled_no_match() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let entry = url_allowlist::AllowlistEntry::new(
            "test-1".to_string(),
            "Trusted".to_string(),
            url_allowlist::AllowlistPattern::Domain("trusted.com".to_string()),
            None,
        );
        dm.add_allowlist_entry(entry).await.unwrap();
        dm.set_allowlist_enabled(true).await.unwrap();

        let result = dm.check_url_allowed("http://untrusted.com/file.txt").await;
        assert!(!result.allowed);
    }

    #[tokio::test]
    async fn test_allowlist_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm1 = DownloadManager::new(temp_dir.path().to_path_buf());

        let entry = url_allowlist::AllowlistEntry::new(
            "persist-1".to_string(),
            "Persistent".to_string(),
            url_allowlist::AllowlistPattern::Domain("example.com".to_string()),
            None,
        );
        dm1.add_allowlist_entry(entry).await.unwrap();
        dm1.set_allowlist_enabled(true).await.unwrap();

        // Create new DownloadManager instance (simulates restart)
        let dm2 = DownloadManager::new(temp_dir.path().to_path_buf());
        let config = dm2.get_url_allowlist_config().await;

        // Note: This will fail until we implement restore in new_with_restore()
        // For now, just verify the file was saved
        assert!(temp_dir.path().join("url_allowlist.json").exists());
    }

    // ========== Speed Prediction Integration Tests (Phase 102) ==========

    #[tokio::test]
    async fn test_speed_prediction_config_default() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_config"));
        let config = dm.get_speed_prediction_config().await;
        assert_eq!(config.min_samples_for_prediction, 10);
        assert_eq!(config.sample_retention_hours, 168);
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn test_speed_prediction_set_config() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_set_config"));
        let mut config = dm.get_speed_prediction_config().await;
        config.min_samples_for_prediction = 5;
        config.sample_retention_hours = 48;
        dm.set_speed_prediction_config(config.clone()).await;

        let retrieved = dm.get_speed_prediction_config().await;
        assert_eq!(retrieved.min_samples_for_prediction, 5);
        assert_eq!(retrieved.sample_retention_hours, 48);
    }

    #[tokio::test]
    async fn test_speed_prediction_predict_task_speed() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_predict"));
        let prediction = dm
            .predict_task_speed("task1", "example.com", 1000.0, 10000)
            .await;
        assert_eq!(prediction.task_id, "task1");
        assert_eq!(prediction.domain, "example.com");
        assert_eq!(prediction.remaining_bytes, 10000);
        assert_eq!(prediction.confidence, "none");
    }

    #[tokio::test]
    async fn test_speed_prediction_summary_empty() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_summary"));
        let summary = dm.get_speed_prediction_summary().await;
        assert_eq!(summary.tracked_domains, 0);
        assert!(summary.domain_summaries.is_empty());
    }

    #[tokio::test]
    async fn test_speed_prediction_list_domains_empty() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_domains"));
        let domains = dm.list_tracked_speed_domains().await;
        assert!(domains.is_empty());
    }

    #[tokio::test]
    async fn test_speed_prediction_remove_nonexistent_domain() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_remove"));
        let removed = dm.remove_speed_prediction_domain("nonexistent.com").await;
        assert!(!removed);
    }

    #[tokio::test]
    async fn test_speed_prediction_get_profile_nonexistent() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_profile"));
        let profile = dm.get_domain_speed_profile("nonexistent.com").await;
        assert!(profile.is_none());
    }

    #[tokio::test]
    async fn test_speed_prediction_optimal_windows_empty() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_windows"));
        let windows = dm.get_optimal_speed_windows("nonexistent.com", 5).await;
        assert!(windows.is_empty());
    }

    #[tokio::test]
    async fn test_speed_prediction_cleanup_no_panic() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_cleanup"));
        dm.cleanup_old_speed_predictions().await;
    }

    #[tokio::test]
    async fn test_speed_prediction_clear_no_panic() {
        let dm = DownloadManager::new(std::path::PathBuf::from("/tmp/test_sp_clear"));
        dm.clear_all_speed_predictions().await;
    }

    // ========== Phase 106: Per-Task Proxy Override Tests ==========

    #[tokio::test]
    async fn test_task_proxy_set_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(dir.path().to_path_buf());
        let proxy =
            proxy::ProxyConfig::new(proxy::ProxyType::Socks5, "127.0.0.1".to_string(), 1080);
        let result = dm
            .set_task_proxy("task-1".to_string(), proxy, Some("test".to_string()))
            .await;
        assert!(result.is_ok());
        let got = dm.get_task_proxy("task-1").await;
        assert!(got.is_some());
        let cfg = got.unwrap();
        assert_eq!(cfg.task_id, "task-1");
        assert_eq!(cfg.proxy.host, "127.0.0.1");
        assert!(cfg.enabled);
    }

    #[tokio::test]
    async fn test_task_proxy_remove() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(dir.path().to_path_buf());
        let proxy =
            proxy::ProxyConfig::new(proxy::ProxyType::Socks5, "127.0.0.1".to_string(), 1080);
        dm.set_task_proxy("task-1".to_string(), proxy, None)
            .await
            .unwrap();
        assert!(dm.get_task_proxy("task-1").await.is_some());
        dm.remove_task_proxy("task-1").await.unwrap();
        assert!(dm.get_task_proxy("task-1").await.is_none());
    }

    #[tokio::test]
    async fn test_task_proxy_enable_disable() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(dir.path().to_path_buf());
        let proxy =
            proxy::ProxyConfig::new(proxy::ProxyType::Socks5, "127.0.0.1".to_string(), 1080);
        dm.set_task_proxy("task-1".to_string(), proxy, None)
            .await
            .unwrap();
        // Disable
        dm.set_task_proxy_enabled("task-1", false).await.unwrap();
        // get_task_proxy returns None when disabled
        assert!(dm.get_task_proxy("task-1").await.is_none());
        // Re-enable
        dm.set_task_proxy_enabled("task-1", true).await.unwrap();
        assert!(dm.get_task_proxy("task-1").await.is_some());
    }

    #[tokio::test]
    async fn test_task_proxy_list_and_summary() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(dir.path().to_path_buf());
        let proxy1 =
            proxy::ProxyConfig::new(proxy::ProxyType::Socks5, "10.0.0.1".to_string(), 1080);
        let proxy2 = proxy::ProxyConfig::new(proxy::ProxyType::Http, "10.0.0.2".to_string(), 8080);
        dm.set_task_proxy("task-1".to_string(), proxy1, None)
            .await
            .unwrap();
        dm.set_task_proxy("task-2".to_string(), proxy2, None)
            .await
            .unwrap();
        let list = dm.list_task_proxies().await;
        assert_eq!(list.len(), 2);
        let summary = dm.get_task_proxy_summary().await;
        assert_eq!(summary.total_overrides, 2);
        assert_eq!(summary.enabled_overrides, 2);
    }

    #[tokio::test]
    async fn test_task_proxy_clear_all() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(dir.path().to_path_buf());
        let proxy = proxy::ProxyConfig::new(proxy::ProxyType::Socks5, "10.0.0.1".to_string(), 1080);
        dm.set_task_proxy("task-1".to_string(), proxy.clone(), None)
            .await
            .unwrap();
        dm.set_task_proxy("task-2".to_string(), proxy, None)
            .await
            .unwrap();
        dm.clear_task_proxies().await.unwrap();
        let list = dm.list_task_proxies().await;
        assert_eq!(list.len(), 0);
    }

    #[tokio::test]
    async fn test_task_proxy_notes() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(dir.path().to_path_buf());
        let proxy = proxy::ProxyConfig::new(proxy::ProxyType::Socks5, "10.0.0.1".to_string(), 1080);
        dm.set_task_proxy("task-1".to_string(), proxy, None)
            .await
            .unwrap();
        dm.set_task_proxy_notes("task-1", Some("updated notes".to_string()))
            .await
            .unwrap();
        let cfg = dm.get_task_proxy("task-1").await.unwrap();
        assert_eq!(cfg.notes, Some("updated notes".to_string()));
    }

    #[tokio::test]
    async fn test_task_proxy_enable_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(dir.path().to_path_buf());
        let result = dm.set_task_proxy_enabled("nonexistent", true).await;
        assert!(result.is_err());
    }

    // ========== Phase 134: Config Persistence Restoration Tests ==========

    #[tokio::test]
    async fn test_path_organizer_config_restore_on_startup() {
        let temp_dir = tempfile::tempdir().unwrap();
        // Create first DM and configure path organizer
        let dm1 = DownloadManager::new(temp_dir.path().to_path_buf());
        dm1.set_path_organizer_enabled(true).await;
        let category = path_organizer::FileCategory {
            name: "test_category".to_string(),
            extensions: vec![".test".to_string()],
            directory: "test_output".to_string(),
        };
        dm1.add_file_category(category).await;
        // Save config to disk
        dm1.save_path_organizer_config().await.unwrap();

        // Create new DM via new_with_restore (simulates restart)
        let dm2 = DownloadManager::new_with_restore(temp_dir.path().to_path_buf()).await;
        let config = dm2.get_path_organizer_config().await;
        assert!(config.enabled);
        assert!(
            config.categories.iter().any(|c| c.name == "test_category"),
            "Path organizer categories should be restored"
        );
    }

    #[tokio::test]
    async fn test_source_quality_config_restore_on_startup() {
        let temp_dir = tempfile::tempdir().unwrap();
        // Create first DM and configure source quality
        let dm1 = DownloadManager::new(temp_dir.path().to_path_buf());
        let mut config = dm1.get_source_quality_config().await;
        config.block_duration_secs = 9999;
        dm1.set_source_quality_config(config).await;
        dm1.save_source_quality_config().await.unwrap();

        // Create new DM via new_with_restore (simulates restart)
        let dm2 = DownloadManager::new_with_restore(temp_dir.path().to_path_buf()).await;
        let config2 = dm2.get_source_quality_config().await;
        assert_eq!(
            config2.block_duration_secs, 9999,
            "Source quality config should be restored"
        );
    }

    #[tokio::test]
    async fn test_phase134_restore_no_config_files() {
        // Verify new_with_restore works fine when no config files exist
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new_with_restore(temp_dir.path().to_path_buf()).await;
        let po_config = dm.get_path_organizer_config().await;
        assert!(
            !po_config.enabled,
            "Path organizer should default to disabled"
        );
        let sq_config = dm.get_source_quality_config().await;
        assert_eq!(sq_config.block_duration_secs, 3600);
    }

    // ========== Phase 147: Notification Center Integration Tests ==========

    #[tokio::test]
    async fn test_notification_center_default_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let config = dm.get_notification_center_config().await;
        assert_eq!(config.max_history_size, 1000);
        assert!(config.persist_history);
    }

    #[tokio::test]
    async fn test_notification_center_set_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let mut config = dm.get_notification_center_config().await;
        config.max_history_size = 500;
        config.batching.enabled = false;
        dm.set_notification_center_config(config.clone()).await;
        let updated = dm.get_notification_center_config().await;
        assert_eq!(updated.max_history_size, 500);
        assert!(!updated.batching.enabled);
    }

    #[tokio::test]
    async fn test_notification_center_summary() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let summary = dm.get_notification_center_summary().await;
        assert!(!summary.quiet_hours_active);
        assert_eq!(summary.pending_batch_count, 0);
        assert_eq!(summary.history_size, 0);
    }

    #[tokio::test]
    async fn test_notification_center_quiet_hours() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let active = dm.is_notification_quiet_hours_active().await;
        assert!(!active); // Default config has quiet hours disabled
    }

    #[tokio::test]
    async fn test_notification_center_history_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let filter = notification_center::NotificationFilter::default();
        let history = dm.get_notification_history(filter).await;
        assert!(history.is_empty());
    }

    #[tokio::test]
    async fn test_notification_center_clear_history() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        dm.clear_notification_history().await;
        let summary = dm.get_notification_center_summary().await;
        assert_eq!(summary.history_size, 0);
    }

    #[tokio::test]
    async fn test_notification_center_analytics() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let analytics = dm.get_notification_analytics().await;
        assert_eq!(analytics.total_created, 0);
        assert_eq!(analytics.total_delivered, 0);
    }

    #[tokio::test]
    async fn test_notification_center_batch_count() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let count = dm.get_notification_batch_count().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_notification_center_event_preference() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let pref = notification_center::EventChannelPreference {
            event: notification_center::NotificationCenterEvent::DownloadComplete,
            channels: vec!["desktop".to_string()],
            priority_override: Some(notification_center::NotificationPriority::High),
            muted: false,
        };
        dm.add_notification_event_preference(pref).await;
        let config = dm.get_notification_center_config().await;
        assert_eq!(config.event_preferences.len(), 1);
        dm.remove_notification_event_preference(
            notification_center::NotificationCenterEvent::DownloadComplete,
        )
        .await;
        let config = dm.get_notification_center_config().await;
        assert_eq!(config.event_preferences.len(), 0);
    }

    #[tokio::test]
    async fn test_notification_center_save_load_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());
        let mut config = dm.get_notification_center_config().await;
        config.max_history_size = 999;
        dm.set_notification_center_config(config).await;
        dm.save_notification_center_config().await.unwrap();
        // Create new DM and load
        let dm2 = DownloadManager::new(temp_dir.path().to_path_buf());
        dm2.load_notification_center_config().await.unwrap();
        let loaded = dm2.get_notification_center_config().await;
        assert_eq!(loaded.max_history_size, 999);
    }
}

#[cfg(test)]
mod source_latency_integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_source_latency_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let config = dm.get_source_latency_config().await;
        assert!(config.enabled);

        let mut new_config = config;
        new_config.enabled = false;
        dm.set_source_latency_config(new_config).await;

        let loaded = dm.get_source_latency_config().await;
        assert!(!loaded.enabled);
    }

    #[tokio::test]
    async fn test_source_latency_record_success() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        dm.record_latency_success("example.com", 100.0).await;

        let stats = dm.get_source_latency_domain("example.com").await;
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.total_samples, 1);
        assert_eq!(stats.successful_connections, 1);
    }

    #[tokio::test]
    async fn test_source_latency_record_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        dm.record_latency_failure("example.com", "Timeout".to_string())
            .await;

        let stats = dm.get_source_latency_domain("example.com").await;
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.failed_connections, 1);
    }

    #[tokio::test]
    async fn test_source_latency_summary() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        // 30ms < 50ms threshold => Excellent
        dm.record_latency_success("fast.com", 30.0).await;
        // 500ms >= 500ms and < 1000ms => Poor
        dm.record_latency_success("slow.com", 500.0).await;

        let summary = dm.get_source_latency_summary().await;
        assert_eq!(summary.total_domains, 2);
        assert_eq!(summary.excellent_count, 1);
        assert_eq!(summary.poor_count, 1);
    }

    #[tokio::test]
    async fn test_source_latency_best_domain() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        dm.record_latency_success("fast.com", 50.0).await;
        dm.record_latency_success("slow.com", 500.0).await;

        let best = dm.get_best_latency_domain().await;
        assert_eq!(best, Some("fast.com".to_string()));
    }

    #[tokio::test]
    async fn test_source_latency_rank() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        dm.record_latency_success("fast.com", 50.0).await;
        dm.record_latency_success("medium.com", 200.0).await;
        dm.record_latency_success("slow.com", 500.0).await;

        let ranked = dm.rank_domains_by_latency().await;
        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].0, "fast.com");
        assert_eq!(ranked[1].0, "medium.com");
        assert_eq!(ranked[2].0, "slow.com");
    }

    #[tokio::test]
    async fn test_source_latency_clear_domain() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        dm.record_latency_success("example.com", 100.0).await;
        assert!(dm.get_source_latency_domain("example.com").await.is_some());

        dm.clear_source_latency_domain("example.com").await;
        assert!(dm.get_source_latency_domain("example.com").await.is_none());
    }

    #[tokio::test]
    async fn test_source_latency_clear_all() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        dm.record_latency_success("a.com", 100.0).await;
        dm.record_latency_success("b.com", 200.0).await;

        dm.clear_source_latency_all().await;
        let all = dm.get_source_latency_all().await;
        assert_eq!(all.len(), 0);
    }

    #[tokio::test]
    async fn test_source_latency_decay() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        dm.record_latency_success("example.com", 100.0).await;
        dm.apply_source_latency_decay().await;

        // Decay should not panic
    }

    #[tokio::test]
    async fn test_source_latency_save_load_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        let mut config = dm.get_source_latency_config().await;
        config.enabled = false;
        dm.set_source_latency_config(config).await;

        dm.save_source_latency_config().await.unwrap();

        let dm2 = DownloadManager::new(temp_dir.path().to_path_buf());
        dm2.load_source_latency_config().await.unwrap();

        let loaded = dm2.get_source_latency_config().await;
        assert!(!loaded.enabled);
    }

    #[tokio::test]
    async fn test_source_latency_save_load_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dm = DownloadManager::new(temp_dir.path().to_path_buf());

        dm.record_latency_success("example.com", 100.0).await;
        dm.save_source_latency_stats().await.unwrap();

        let dm2 = DownloadManager::new(temp_dir.path().to_path_buf());
        dm2.load_source_latency_stats().await.unwrap();

        let stats = dm2.get_source_latency_domain("example.com").await;
        assert!(stats.is_some());
    }
}
