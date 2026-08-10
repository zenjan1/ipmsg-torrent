//! Download Session Tracking
//!
//! Tracks individual download sessions for each task. A session is created each time
//! a download starts (from Queued → Downloading) and ends when it pauses, completes,
//! or errors. This provides detailed history of download attempts, durations, and
//! transfer amounts per session.
//!
//! Features:
//! - Per-task session history with configurable max sessions
//! - Session summary statistics (total sessions, total time, avg speed)
//! - Persistence to `download_sessions.json`
//! - Automatic session creation/closure on state transitions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;
use tokio::fs;

/// Default maximum sessions per task
const DEFAULT_MAX_SESSIONS_PER_TASK: usize = 50;

/// Default maximum total sessions across all tasks
const DEFAULT_MAX_TOTAL_SESSIONS: usize = 5000;

/// Errors from download session operations.
#[derive(Error, Debug)]
pub enum DownloadSessionError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Task not found: {0}")]
    TaskNotFound(String),
    #[error("No active session for task: {0}")]
    NoActiveSession(String),
}

/// Outcome of a completed download session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionOutcome {
    /// Download completed successfully
    Completed,
    /// User paused the download
    Paused,
    /// Download failed with error
    Failed,
    /// Download timed out
    TimedOut,
    /// Session still in progress
    InProgress,
}

impl std::fmt::Display for SessionOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionOutcome::Completed => write!(f, "completed"),
            SessionOutcome::Paused => write!(f, "paused"),
            SessionOutcome::Failed => write!(f, "failed"),
            SessionOutcome::TimedOut => write!(f, "timed_out"),
            SessionOutcome::InProgress => write!(f, "in_progress"),
        }
    }
}

/// A single download session (one start-to-end cycle).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSession {
    /// Unique session ID (UUID v4)
    pub id: String,
    /// Task ID this session belongs to
    pub task_id: String,
    /// When the session started (download began)
    pub started_at: DateTime<Utc>,
    /// When the session ended (None if still in progress)
    pub ended_at: Option<DateTime<Utc>>,
    /// Bytes downloaded at session start
    pub bytes_at_start: u64,
    /// Bytes downloaded at session end (None if still in progress)
    pub bytes_at_end: Option<u64>,
    /// Peak speed during this session (bytes/sec)
    pub peak_speed_bps: u64,
    /// Average speed during this session (bytes/sec, computed at close)
    pub avg_speed_bps: Option<u64>,
    /// Error message if session ended with failure
    pub error: Option<String>,
    /// How the session ended
    pub outcome: SessionOutcome,
    /// Protocol used (http, torrent, ed2k, p2p)
    pub protocol: String,
}

impl DownloadSession {
    /// Create a new session.
    pub fn new(task_id: &str, bytes_at_start: u64, protocol: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            task_id: task_id.to_string(),
            started_at: Utc::now(),
            ended_at: None,
            bytes_at_start,
            bytes_at_end: None,
            peak_speed_bps: 0,
            avg_speed_bps: None,
            error: None,
            outcome: SessionOutcome::InProgress,
            protocol: protocol.to_string(),
        }
    }

    /// Close the session with the given outcome.
    pub fn close(&mut self, bytes_at_end: u64, outcome: SessionOutcome, error: Option<String>) {
        self.ended_at = Some(Utc::now());
        self.bytes_at_end = Some(bytes_at_end);
        self.outcome = outcome;
        self.error = error;

        // Compute average speed
        if let Some(ended) = self.ended_at {
            let duration_secs = (ended - self.started_at).num_seconds();
            if duration_secs > 0 {
                let bytes_transferred = bytes_at_end.saturating_sub(self.bytes_at_start);
                self.avg_speed_bps = Some(bytes_transferred / duration_secs as u64);
            }
        }
    }

    /// Duration of this session in seconds.
    pub fn duration_secs(&self) -> f64 {
        let end = self.ended_at.unwrap_or_else(Utc::now);
        (end - self.started_at).num_milliseconds() as f64 / 1000.0
    }

    /// Bytes transferred during this session.
    pub fn bytes_transferred(&self) -> u64 {
        self.bytes_at_end
            .unwrap_or(self.bytes_at_start)
            .saturating_sub(self.bytes_at_start)
    }

    /// Whether this session is still in progress.
    pub fn is_active(&self) -> bool {
        self.ended_at.is_none()
    }
}

/// Summary statistics for all sessions of a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSessionSummary {
    /// Task ID
    pub task_id: String,
    /// Total number of sessions (including historical)
    pub total_sessions: usize,
    /// Number of completed sessions
    pub completed_sessions: usize,
    /// Number of failed sessions
    pub failed_sessions: usize,
    /// Number of paused sessions
    pub paused_sessions: usize,
    /// Currently active session (if any)
    pub active_session: Option<DownloadSession>,
    /// Total download time across all sessions (seconds)
    pub total_download_time_secs: f64,
    /// Total bytes transferred across all sessions
    pub total_bytes_transferred: u64,
    /// Overall average speed (bytes/sec)
    pub overall_avg_speed_bps: Option<u64>,
    /// Peak speed across all sessions (bytes/sec)
    pub peak_speed_bps: u64,
    /// All sessions in chronological order (oldest first)
    pub sessions: Vec<DownloadSession>,
}

/// Configuration for the download session tracker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSessionConfig {
    /// Maximum sessions per task (oldest evicted when exceeded)
    pub max_sessions_per_task: usize,
    /// Maximum total sessions across all tasks
    pub max_total_sessions: usize,
}

impl Default for DownloadSessionConfig {
    fn default() -> Self {
        Self {
            max_sessions_per_task: DEFAULT_MAX_SESSIONS_PER_TASK,
            max_total_sessions: DEFAULT_MAX_TOTAL_SESSIONS,
        }
    }
}

/// Manages download sessions for all tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSessionManager {
    /// Sessions grouped by task ID
    sessions: HashMap<String, Vec<DownloadSession>>,
    /// Configuration
    config: DownloadSessionConfig,
}

impl Default for DownloadSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadSessionManager {
    /// Create a new session manager with default config.
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            config: DownloadSessionConfig::default(),
        }
    }

    /// Create with custom config.
    pub fn with_config(config: DownloadSessionConfig) -> Self {
        Self {
            sessions: HashMap::new(),
            config,
        }
    }

    /// Start a new session for a task.
    /// Returns the session ID.
    pub fn start_session(&mut self, task_id: &str, bytes_at_start: u64, protocol: &str) -> String {
        let session = DownloadSession::new(task_id, bytes_at_start, protocol);
        let session_id = session.id.clone();

        let task_sessions = self.sessions.entry(task_id.to_string()).or_default();
        task_sessions.push(session);

        // Evict oldest if over limit
        if task_sessions.len() > self.config.max_sessions_per_task {
            task_sessions.remove(0);
        }

        // Global eviction
        self.enforce_global_limit();

        session_id
    }

    /// Close the active session for a task.
    pub fn close_session(
        &mut self,
        task_id: &str,
        bytes_at_end: u64,
        outcome: SessionOutcome,
        error: Option<String>,
    ) -> Result<(), DownloadSessionError> {
        let task_sessions = self
            .sessions
            .get_mut(task_id)
            .ok_or_else(|| DownloadSessionError::TaskNotFound(task_id.to_string()))?;

        // Find the last active session
        let active = task_sessions
            .iter_mut()
            .rev()
            .find(|s| s.is_active())
            .ok_or_else(|| DownloadSessionError::NoActiveSession(task_id.to_string()))?;

        active.close(bytes_at_end, outcome, error);
        Ok(())
    }

    /// Update peak speed for the active session.
    pub fn update_peak_speed(&mut self, task_id: &str, speed_bps: u64) {
        if let Some(task_sessions) = self.sessions.get_mut(task_id)
            && let Some(active) = task_sessions.iter_mut().rev().find(|s| s.is_active())
            && speed_bps > active.peak_speed_bps
        {
            active.peak_speed_bps = speed_bps;
        }
    }

    /// Get all sessions for a task.
    pub fn get_task_sessions(&self, task_id: &str) -> Option<&Vec<DownloadSession>> {
        self.sessions.get(task_id)
    }

    /// Get summary for a task's sessions.
    pub fn get_task_summary(&self, task_id: &str) -> Option<TaskSessionSummary> {
        let sessions = self.sessions.get(task_id)?;
        Some(self.compute_summary(task_id, sessions))
    }

    /// Get summaries for all tasks.
    pub fn get_all_summaries(&self) -> Vec<TaskSessionSummary> {
        self.sessions
            .iter()
            .map(|(task_id, sessions)| self.compute_summary(task_id, sessions))
            .collect()
    }

    /// Get the active session for a task (if any).
    pub fn get_active_session(&self, task_id: &str) -> Option<&DownloadSession> {
        self.sessions
            .get(task_id)
            .and_then(|sessions| sessions.iter().rev().find(|s| s.is_active()))
    }

    /// Remove all sessions for a task.
    pub fn remove_task_sessions(&mut self, task_id: &str) -> bool {
        self.sessions.remove(task_id).is_some()
    }

    /// Clear all sessions.
    pub fn clear_all(&mut self) {
        self.sessions.clear();
    }

    /// Get/set config.
    pub fn config(&self) -> &DownloadSessionConfig {
        &self.config
    }

    pub fn set_config(&mut self, config: DownloadSessionConfig) {
        self.config = config;
    }

    /// Total number of sessions across all tasks.
    pub fn total_session_count(&self) -> usize {
        self.sessions.values().map(|v| v.len()).sum()
    }

    /// Number of currently active sessions.
    pub fn active_session_count(&self) -> usize {
        self.sessions
            .values()
            .filter_map(|sessions| sessions.iter().rev().find(|s| s.is_active()).map(|_| 1))
            .sum()
    }

    // --- Persistence ---

    /// Save sessions to a JSON file (atomic write).
    pub async fn save_to_file(&self, path: &Path) -> Result<(), DownloadSessionError> {
        let json = serde_json::to_string_pretty(self)?;
        let temp_path = path.with_extension("tmp");
        fs::write(&temp_path, &json).await?;
        fs::rename(&temp_path, path).await?;
        Ok(())
    }

    /// Load sessions from a JSON file.
    pub async fn load_from_file(path: &Path) -> Result<Self, DownloadSessionError> {
        let json = fs::read_to_string(path).await?;
        let manager: Self = serde_json::from_str(&json)?;
        Ok(manager)
    }

    // --- Internal helpers ---

    fn compute_summary(&self, task_id: &str, sessions: &[DownloadSession]) -> TaskSessionSummary {
        let completed = sessions
            .iter()
            .filter(|s| s.outcome == SessionOutcome::Completed)
            .count();
        let failed = sessions
            .iter()
            .filter(|s| s.outcome == SessionOutcome::Failed)
            .count();
        let paused = sessions
            .iter()
            .filter(|s| s.outcome == SessionOutcome::Paused)
            .count();
        let active = sessions.iter().rev().find(|s| s.is_active()).cloned();

        let total_time: f64 = sessions.iter().map(|s| s.duration_secs()).sum();
        let total_bytes: u64 = sessions.iter().map(|s| s.bytes_transferred()).sum();
        let peak_speed = sessions.iter().map(|s| s.peak_speed_bps).max().unwrap_or(0);

        let overall_avg = if total_time > 0.0 {
            Some((total_bytes as f64 / total_time) as u64)
        } else {
            None
        };

        TaskSessionSummary {
            task_id: task_id.to_string(),
            total_sessions: sessions.len(),
            completed_sessions: completed,
            failed_sessions: failed,
            paused_sessions: paused,
            active_session: active,
            total_download_time_secs: total_time,
            total_bytes_transferred: total_bytes,
            overall_avg_speed_bps: overall_avg,
            peak_speed_bps: peak_speed,
            sessions: sessions.to_vec(),
        }
    }

    fn enforce_global_limit(&mut self) {
        let total: usize = self.sessions.values().map(|v| v.len()).sum();
        if total <= self.config.max_total_sessions {
            return;
        }

        // Evict oldest sessions from tasks with the most sessions
        while self.total_session_count() > self.config.max_total_sessions {
            // Find the task with the most sessions
            let task_to_evict = self
                .sessions
                .iter()
                .max_by_key(|(_, v)| v.len())
                .map(|(k, _)| k.clone());

            if let Some(task_id) = task_to_evict {
                if let Some(sessions) = self.sessions.get_mut(&task_id) {
                    if !sessions.is_empty() {
                        sessions.remove(0);
                    }
                    if sessions.is_empty() {
                        self.sessions.remove(&task_id);
                    }
                }
            } else {
                break;
            }
        }
    }
}

/// Save session manager to file (convenience function).
pub async fn save_download_sessions(
    manager: &DownloadSessionManager,
    path: &Path,
) -> Result<(), DownloadSessionError> {
    manager.save_to_file(path).await
}

/// Load session manager from file (convenience function).
pub async fn load_download_sessions(
    path: &Path,
) -> Result<DownloadSessionManager, DownloadSessionError> {
    DownloadSessionManager::load_from_file(path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let session = DownloadSession::new("task-1", 0, "http");
        assert_eq!(session.task_id, "task-1");
        assert_eq!(session.bytes_at_start, 0);
        assert_eq!(session.protocol, "http");
        assert!(session.is_active());
        assert_eq!(session.outcome, SessionOutcome::InProgress);
        assert!(session.ended_at.is_none());
        assert!(session.avg_speed_bps.is_none());
    }

    #[test]
    fn test_session_close_completed() {
        let mut session = DownloadSession::new("task-1", 0, "http");
        // Simulate some time passing
        session.started_at = session.started_at - chrono::Duration::seconds(5);
        session.close(1_000_000, SessionOutcome::Completed, None);

        assert!(!session.is_active());
        assert_eq!(session.outcome, SessionOutcome::Completed);
        assert_eq!(session.bytes_at_end, Some(1_000_000));
        assert_eq!(session.bytes_transferred(), 1_000_000);
        assert!(session.error.is_none());
        assert!(session.avg_speed_bps.is_some());
        // ~200KB/s over 5 seconds
        assert!(session.avg_speed_bps.unwrap() > 0);
    }

    #[test]
    fn test_session_close_failed() {
        let mut session = DownloadSession::new("task-1", 500, "torrent");
        session.close(
            1000,
            SessionOutcome::Failed,
            Some("connection reset".to_string()),
        );

        assert_eq!(session.outcome, SessionOutcome::Failed);
        assert_eq!(session.bytes_transferred(), 500);
        assert_eq!(session.error, Some("connection reset".to_string()));
    }

    #[test]
    fn test_session_close_paused() {
        let mut session = DownloadSession::new("task-1", 0, "ed2k");
        session.close(2048, SessionOutcome::Paused, None);

        assert_eq!(session.outcome, SessionOutcome::Paused);
        assert!(session.error.is_none());
    }

    #[test]
    fn test_session_duration() {
        let session = DownloadSession::new("task-1", 0, "http");
        // Just created, duration should be ~0
        assert!(session.duration_secs() < 1.0);
    }

    #[test]
    fn test_session_bytes_transferred_in_progress() {
        let session = DownloadSession::new("task-1", 1000, "http");
        // In progress: bytes_at_end is None, uses bytes_at_start
        assert_eq!(session.bytes_transferred(), 0);
    }

    #[test]
    fn test_manager_start_session() {
        let mut mgr = DownloadSessionManager::new();
        let id = mgr.start_session("task-1", 0, "http");

        assert!(!id.is_empty());
        assert_eq!(mgr.total_session_count(), 1);
        assert_eq!(mgr.active_session_count(), 1);

        let session = mgr.get_active_session("task-1").unwrap();
        assert_eq!(session.id, id);
    }

    #[test]
    fn test_manager_close_session() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");

        mgr.close_session("task-1", 5000, SessionOutcome::Completed, None)
            .unwrap();

        assert_eq!(mgr.active_session_count(), 0);
        let summary = mgr.get_task_summary("task-1").unwrap();
        assert_eq!(summary.completed_sessions, 1);
    }

    #[test]
    fn test_manager_close_nonexistent() {
        let mut mgr = DownloadSessionManager::new();
        let result = mgr.close_session("no-task", 0, SessionOutcome::Completed, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_manager_close_no_active() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();

        // Try to close again - no active session
        let result = mgr.close_session("task-1", 200, SessionOutcome::Completed, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_manager_multiple_sessions() {
        let mut mgr = DownloadSessionManager::new();

        // Session 1: completed
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 1000, SessionOutcome::Completed, None)
            .unwrap();

        // Session 2: failed
        mgr.start_session("task-1", 1000, "http");
        mgr.close_session(
            "task-1",
            1500,
            SessionOutcome::Failed,
            Some("timeout".to_string()),
        )
        .unwrap();

        // Session 3: in progress
        mgr.start_session("task-1", 1500, "http");

        let summary = mgr.get_task_summary("task-1").unwrap();
        assert_eq!(summary.total_sessions, 3);
        assert_eq!(summary.completed_sessions, 1);
        assert_eq!(summary.failed_sessions, 1);
        assert!(summary.active_session.is_some());
    }

    #[test]
    fn test_manager_update_peak_speed() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");

        mgr.update_peak_speed("task-1", 100_000);
        mgr.update_peak_speed("task-1", 250_000);
        mgr.update_peak_speed("task-1", 150_000); // lower, should not update

        let session = mgr.get_active_session("task-1").unwrap();
        assert_eq!(session.peak_speed_bps, 250_000);
    }

    #[test]
    fn test_manager_eviction_per_task() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 3,
            max_total_sessions: 5000,
        };
        let mut mgr = DownloadSessionManager::with_config(config);

        for i in 0..5 {
            mgr.start_session("task-1", i * 100, "http");
            if i < 4 {
                mgr.close_session("task-1", (i + 1) * 100, SessionOutcome::Completed, None)
                    .unwrap();
            }
        }

        let sessions = mgr.get_task_sessions("task-1").unwrap();
        assert_eq!(sessions.len(), 3); // max 3 per task
    }

    #[test]
    fn test_manager_remove_task_sessions() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.start_session("task-2", 0, "torrent");

        assert!(mgr.remove_task_sessions("task-1"));
        assert_eq!(mgr.total_session_count(), 1);
        assert!(!mgr.remove_task_sessions("nonexistent"));
    }

    #[test]
    fn test_manager_clear_all() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.start_session("task-2", 0, "torrent");

        mgr.clear_all();
        assert_eq!(mgr.total_session_count(), 0);
    }

    #[test]
    fn test_summary_overall_avg_speed() {
        let mut mgr = DownloadSessionManager::new();

        mgr.start_session("task-1", 0, "http");
        // Manually backdate the session start so duration > 0
        {
            let sessions = mgr.sessions.get_mut("task-1").unwrap();
            sessions[0].started_at = sessions[0].started_at - chrono::Duration::seconds(2);
        }
        mgr.close_session("task-1", 1000, SessionOutcome::Completed, None)
            .unwrap();

        let summary = mgr.get_task_summary("task-1").unwrap();
        assert!(summary.overall_avg_speed_bps.is_some());
        let _ = summary.peak_speed_bps;
    }

    #[test]
    fn test_summary_empty_task() {
        let mgr = DownloadSessionManager::new();
        assert!(mgr.get_task_summary("nonexistent").is_none());
    }

    #[test]
    fn test_all_summaries() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.start_session("task-2", 0, "torrent");

        let summaries = mgr.get_all_summaries();
        assert_eq!(summaries.len(), 2);
    }

    #[test]
    fn test_config() {
        let mgr = DownloadSessionManager::new();
        assert_eq!(mgr.config().max_sessions_per_task, 50);
        assert_eq!(mgr.config().max_total_sessions, 5000);
    }

    #[test]
    fn test_session_outcome_display() {
        assert_eq!(SessionOutcome::Completed.to_string(), "completed");
        assert_eq!(SessionOutcome::Paused.to_string(), "paused");
        assert_eq!(SessionOutcome::Failed.to_string(), "failed");
        assert_eq!(SessionOutcome::TimedOut.to_string(), "timed_out");
        assert_eq!(SessionOutcome::InProgress.to_string(), "in_progress");
    }

    #[tokio::test]
    async fn test_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 5000, SessionOutcome::Completed, None)
            .unwrap();
        mgr.start_session("task-1", 5000, "http");
        mgr.update_peak_speed("task-1", 100_000);

        mgr.save_to_file(&path).await.unwrap();

        let loaded = DownloadSessionManager::load_from_file(&path).await.unwrap();
        assert_eq!(loaded.total_session_count(), 2);
        assert_eq!(loaded.active_session_count(), 1);

        let summary = loaded.get_task_summary("task-1").unwrap();
        assert_eq!(summary.completed_sessions, 1);
        assert!(summary.active_session.is_some());
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let path = Path::new("/tmp/nonexistent_sessions_test.json");
        let result = DownloadSessionManager::load_from_file(path).await;
        assert!(result.is_err());
    }
}
