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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

    // ===== Phase 241: Comprehensive Test Coverage =====

    // --- SessionOutcome serde ---
    #[test]
    fn test_session_outcome_serde_roundtrip_all_variants() {
        for outcome in [
            SessionOutcome::Completed,
            SessionOutcome::Paused,
            SessionOutcome::Failed,
            SessionOutcome::TimedOut,
            SessionOutcome::InProgress,
        ] {
            let json = serde_json::to_string(&outcome).unwrap();
            let back: SessionOutcome = serde_json::from_str(&json).unwrap();
            assert_eq!(back, outcome);
        }
    }

    #[test]
    fn test_session_outcome_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&SessionOutcome::Completed).unwrap(),
            "\"completed\""
        );
        assert_eq!(
            serde_json::to_string(&SessionOutcome::TimedOut).unwrap(),
            "\"timed_out\""
        );
        assert_eq!(
            serde_json::to_string(&SessionOutcome::InProgress).unwrap(),
            "\"in_progress\""
        );
    }

    #[test]
    fn test_session_outcome_clone_copy_eq() {
        let a = SessionOutcome::Completed;
        let b = a;
        assert_eq!(a, b);
        let c = a.clone();
        assert_eq!(a, c);
    }

    #[test]
    fn test_session_outcome_debug() {
        let s = format!("{:?}", SessionOutcome::Failed);
        assert!(s.contains("Failed"));
    }

    // --- DownloadSession serde ---
    #[test]
    fn test_download_session_serde_roundtrip() {
        let mut session = DownloadSession::new("task-1", 100, "http");
        session.started_at = chrono::DateTime::parse_from_rfc3339("2026-01-15T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        session.close(5000, SessionOutcome::Completed, None);

        let json = serde_json::to_string(&session).unwrap();
        let back: DownloadSession = serde_json::from_str(&json).unwrap();

        assert_eq!(back.task_id, "task-1");
        assert_eq!(back.bytes_at_start, 100);
        assert_eq!(back.bytes_at_end, Some(5000));
        assert_eq!(back.outcome, SessionOutcome::Completed);
        assert_eq!(back.protocol, "http");
        assert!(back.error.is_none());
        assert!(back.ended_at.is_some());
    }

    #[test]
    fn test_download_session_serde_extra_fields_ignored() {
        let json = r#"{
            "id": "abc",
            "task_id": "t1",
            "started_at": "2026-01-15T10:00:00Z",
            "ended_at": null,
            "bytes_at_start": 0,
            "bytes_at_end": null,
            "peak_speed_bps": 0,
            "avg_speed_bps": null,
            "error": null,
            "outcome": "in_progress",
            "protocol": "http",
            "unknown_field": true
        }"#;
        let session: DownloadSession = serde_json::from_str(json).unwrap();
        assert_eq!(session.task_id, "t1");
    }

    #[test]
    fn test_download_session_clone() {
        let session = DownloadSession::new("task-1", 0, "http");
        let cloned = session.clone();
        assert_eq!(cloned.task_id, session.task_id);
        assert_eq!(cloned.id, session.id);
    }

    #[test]
    fn test_download_session_debug() {
        let session = DownloadSession::new("task-1", 0, "http");
        let dbg = format!("{:?}", session);
        assert!(dbg.contains("task-1"));
        assert!(dbg.contains("http"));
    }

    // --- DownloadSessionConfig serde ---
    #[test]
    fn test_config_serde_roundtrip() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 10,
            max_total_sessions: 100,
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: DownloadSessionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_sessions_per_task, 10);
        assert_eq!(back.max_total_sessions, 100);
    }

    #[test]
    fn test_config_serde_default_values() {
        let json = r#"{"max_sessions_per_task": 50, "max_total_sessions": 5000}"#;
        let config: DownloadSessionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_sessions_per_task, 50);
        assert_eq!(config.max_total_sessions, 5000);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"max_sessions_per_task": 5, "max_total_sessions": 50, "extra": true}"#;
        let config: DownloadSessionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_sessions_per_task, 5);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 20,
            max_total_sessions: 200,
        };
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let back: DownloadSessionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back, config);
    }

    #[test]
    fn test_config_default() {
        let config = DownloadSessionConfig::default();
        assert_eq!(config.max_sessions_per_task, DEFAULT_MAX_SESSIONS_PER_TASK);
        assert_eq!(config.max_total_sessions, DEFAULT_MAX_TOTAL_SESSIONS);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = DownloadSessionConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.max_sessions_per_task, config.max_sessions_per_task);
        let dbg = format!("{:?}", config);
        assert!(dbg.contains("max_sessions_per_task"));
    }

    // --- TaskSessionSummary serde ---
    #[test]
    fn test_task_session_summary_serde_roundtrip() {
        let summary = TaskSessionSummary {
            task_id: "task-1".to_string(),
            total_sessions: 3,
            completed_sessions: 2,
            failed_sessions: 1,
            paused_sessions: 0,
            active_session: None,
            total_download_time_secs: 120.5,
            total_bytes_transferred: 1_000_000,
            overall_avg_speed_bps: Some(8333),
            peak_speed_bps: 50_000,
            sessions: vec![],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: TaskSessionSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, "task-1");
        assert_eq!(back.total_sessions, 3);
        assert_eq!(back.total_bytes_transferred, 1_000_000);
    }

    #[test]
    fn test_task_session_summary_clone_debug() {
        let summary = TaskSessionSummary {
            task_id: "t".to_string(),
            total_sessions: 0,
            completed_sessions: 0,
            failed_sessions: 0,
            paused_sessions: 0,
            active_session: None,
            total_download_time_secs: 0.0,
            total_bytes_transferred: 0,
            overall_avg_speed_bps: None,
            peak_speed_bps: 0,
            sessions: vec![],
        };
        let cloned = summary.clone();
        assert_eq!(cloned.task_id, summary.task_id);
        let dbg = format!("{:?}", summary);
        assert!(dbg.contains("task_id"));
    }

    // --- DownloadSessionManager serde ---
    #[test]
    fn test_manager_serde_roundtrip() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 5000, SessionOutcome::Completed, None)
            .unwrap();

        let json = serde_json::to_string(&mgr).unwrap();
        let back: DownloadSessionManager = serde_json::from_str(&json).unwrap();

        assert_eq!(back.total_session_count(), 1);
        assert_eq!(back.active_session_count(), 0);
    }

    #[test]
    fn test_manager_serde_extra_fields_ignored() {
        let json = r#"{"sessions": {}, "config": {"max_sessions_per_task": 50, "max_total_sessions": 5000}, "extra": 42}"#;
        let mgr: DownloadSessionManager = serde_json::from_str(json).unwrap();
        assert_eq!(mgr.total_session_count(), 0);
    }

    #[test]
    fn test_manager_clone() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");

        let cloned = mgr.clone();
        assert_eq!(cloned.total_session_count(), 1);

        // Independence: modifying clone doesn't affect original
        let mut cloned = cloned;
        cloned.clear_all();
        assert_eq!(cloned.total_session_count(), 0);
        assert_eq!(mgr.total_session_count(), 1);
    }

    #[test]
    fn test_manager_debug() {
        let mgr = DownloadSessionManager::new();
        let dbg = format!("{:?}", mgr);
        assert!(dbg.contains("DownloadSessionManager"));
    }

    #[test]
    fn test_manager_default() {
        let mgr = DownloadSessionManager::default();
        assert_eq!(mgr.total_session_count(), 0);
        assert_eq!(
            mgr.config().max_sessions_per_task,
            DEFAULT_MAX_SESSIONS_PER_TASK
        );
    }

    // --- DownloadSessionError ---
    #[test]
    fn test_error_display_all_variants() {
        let io_err = DownloadSessionError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        assert!(io_err.to_string().contains("I/O error"));

        let json_str = "{";
        let json_err: DownloadSessionError =
            serde_json::from_str::<DownloadSessionManager>(json_str)
                .unwrap_err()
                .into();
        assert!(json_err.to_string().contains("JSON error"));

        let not_found = DownloadSessionError::TaskNotFound("task-xyz".to_string());
        assert!(not_found.to_string().contains("task-xyz"));

        let no_active = DownloadSessionError::NoActiveSession("task-abc".to_string());
        assert!(no_active.to_string().contains("task-abc"));
    }

    #[test]
    fn test_error_debug() {
        let err = DownloadSessionError::TaskNotFound("t1".to_string());
        let dbg = format!("{:?}", err);
        assert!(dbg.contains("TaskNotFound"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "disk full");
        let err: DownloadSessionError = io_err.into();
        assert!(err.to_string().contains("disk full"));
    }

    #[test]
    fn test_error_from_json() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err: DownloadSessionError = json_err.into();
        assert!(err.to_string().contains("JSON error"));
    }

    // --- DownloadSession::new boundaries ---
    #[test]
    fn test_session_new_empty_task_id() {
        let session = DownloadSession::new("", 0, "http");
        assert_eq!(session.task_id, "");
        assert!(!session.id.is_empty());
    }

    #[test]
    fn test_session_new_unicode_task_id() {
        let session = DownloadSession::new("任务-中文", 0, "http");
        assert_eq!(session.task_id, "任务-中文");
    }

    #[test]
    fn test_session_new_emoji_task_id() {
        let session = DownloadSession::new("task-🚀", 0, "torrent");
        assert_eq!(session.task_id, "task-🚀");
    }

    #[test]
    fn test_session_new_large_bytes_at_start() {
        let session = DownloadSession::new("task-1", u64::MAX, "http");
        assert_eq!(session.bytes_at_start, u64::MAX);
    }

    #[test]
    fn test_session_new_all_protocols() {
        for proto in ["http", "https", "torrent", "ed2k", "p2p", "ftp"] {
            let session = DownloadSession::new("task-1", 0, proto);
            assert_eq!(session.protocol, proto);
        }
    }

    // --- DownloadSession::close boundaries ---
    #[test]
    fn test_session_close_zero_bytes() {
        let mut session = DownloadSession::new("task-1", 0, "http");
        session.close(0, SessionOutcome::Completed, None);
        assert_eq!(session.bytes_transferred(), 0);
    }

    #[test]
    fn test_session_close_large_bytes() {
        let mut session = DownloadSession::new("task-1", 0, "http");
        session.close(u64::MAX, SessionOutcome::Completed, None);
        assert_eq!(session.bytes_at_end, Some(u64::MAX));
        assert_eq!(session.bytes_transferred(), u64::MAX);
    }

    #[test]
    fn test_session_close_with_error_message() {
        let mut session = DownloadSession::new("task-1", 0, "http");
        session.close(
            100,
            SessionOutcome::Failed,
            Some("connection reset by peer".to_string()),
        );
        assert_eq!(session.error.as_deref(), Some("connection reset by peer"));
    }

    #[test]
    fn test_session_close_unicode_error() {
        let mut session = DownloadSession::new("task-1", 0, "http");
        session.close(0, SessionOutcome::Failed, Some("连接被重置".to_string()));
        assert_eq!(session.error.as_deref(), Some("连接被重置"));
    }

    #[test]
    fn test_session_close_timed_out() {
        let mut session = DownloadSession::new("task-1", 0, "http");
        session.close(
            500,
            SessionOutcome::TimedOut,
            Some("timeout after 30s".to_string()),
        );
        assert_eq!(session.outcome, SessionOutcome::TimedOut);
        assert!(session.error.is_some());
    }

    // --- duration_secs boundaries ---
    #[test]
    fn test_duration_secs_active_session() {
        let session = DownloadSession::new("task-1", 0, "http");
        // Active session uses Utc::now() as end
        assert!(session.duration_secs() >= 0.0);
        assert!(session.duration_secs() < 2.0);
    }

    #[test]
    fn test_duration_secs_closed_session() {
        let mut session = DownloadSession::new("task-1", 0, "http");
        session.started_at = chrono::DateTime::parse_from_rfc3339("2026-01-15T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        session.ended_at = Some(
            chrono::DateTime::parse_from_rfc3339("2026-01-15T10:05:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        );
        let dur = session.duration_secs();
        assert!((dur - 300.0).abs() < 1.0);
    }

    // --- bytes_transferred boundaries ---
    #[test]
    fn test_bytes_transferred_saturating() {
        let session = DownloadSession::new("task-1", 1000, "http");
        // In progress: bytes_at_end is None, uses bytes_at_start
        // bytes_transferred = bytes_at_start.saturating_sub(bytes_at_start) = 0
        assert_eq!(session.bytes_transferred(), 0);
    }

    #[test]
    fn test_bytes_transferred_closed() {
        let mut session = DownloadSession::new("task-1", 500, "http");
        session.close(2500, SessionOutcome::Completed, None);
        assert_eq!(session.bytes_transferred(), 2000);
    }

    // --- Manager operations ---
    #[test]
    fn test_manager_with_config() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 5,
            max_total_sessions: 100,
        };
        let mgr = DownloadSessionManager::with_config(config);
        assert_eq!(mgr.config().max_sessions_per_task, 5);
        assert_eq!(mgr.config().max_total_sessions, 100);
    }

    #[test]
    fn test_manager_start_session_returns_unique_ids() {
        let mut mgr = DownloadSessionManager::new();
        let id1 = mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();
        let id2 = mgr.start_session("task-1", 100, "http");
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_manager_start_multiple_tasks() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.start_session("task-2", 0, "torrent");
        mgr.start_session("task-3", 0, "ed2k");

        assert_eq!(mgr.total_session_count(), 3);
        assert_eq!(mgr.active_session_count(), 3);
        assert!(mgr.get_active_session("task-1").is_some());
        assert!(mgr.get_active_session("task-2").is_some());
        assert!(mgr.get_active_session("task-3").is_some());
    }

    #[test]
    fn test_manager_get_task_sessions_none() {
        let mgr = DownloadSessionManager::new();
        assert!(mgr.get_task_sessions("nonexistent").is_none());
    }

    #[test]
    fn test_manager_get_task_sessions_some() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        let sessions = mgr.get_task_sessions("task-1").unwrap();
        assert_eq!(sessions.len(), 1);
    }

    #[test]
    fn test_manager_get_active_session_none() {
        let mgr = DownloadSessionManager::new();
        assert!(mgr.get_active_session("nonexistent").is_none());
    }

    #[test]
    fn test_manager_get_active_session_closed() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();
        assert!(mgr.get_active_session("task-1").is_none());
    }

    #[test]
    fn test_manager_update_peak_speed_no_active() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();
        // No active session, should not panic
        mgr.update_peak_speed("task-1", 100_000);
    }

    #[test]
    fn test_manager_update_peak_speed_no_task() {
        let mut mgr = DownloadSessionManager::new();
        // Non-existent task, should not panic
        mgr.update_peak_speed("nonexistent", 100_000);
    }

    #[test]
    fn test_manager_update_peak_speed_lower_not_updated() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.update_peak_speed("task-1", 50_000);
        mgr.update_peak_speed("task-1", 30_000); // lower
        let session = mgr.get_active_session("task-1").unwrap();
        assert_eq!(session.peak_speed_bps, 50_000);
    }

    #[test]
    fn test_manager_update_peak_speed_zero() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.update_peak_speed("task-1", 0);
        let session = mgr.get_active_session("task-1").unwrap();
        assert_eq!(session.peak_speed_bps, 0);
    }

    // --- Eviction ---
    #[test]
    fn test_manager_eviction_per_task_keeps_newest() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 2,
            max_total_sessions: 5000,
        };
        let mut mgr = DownloadSessionManager::with_config(config);

        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();
        mgr.start_session("task-1", 100, "http");
        mgr.close_session("task-1", 200, SessionOutcome::Completed, None)
            .unwrap();
        mgr.start_session("task-1", 200, "http");
        mgr.close_session("task-1", 300, SessionOutcome::Completed, None)
            .unwrap();

        let sessions = mgr.get_task_sessions("task-1").unwrap();
        assert_eq!(sessions.len(), 2);
        // Oldest (bytes_at_start=0) should be evicted
        assert_eq!(sessions[0].bytes_at_start, 100);
        assert_eq!(sessions[1].bytes_at_start, 200);
    }

    #[test]
    fn test_manager_global_limit_eviction() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 100,
            max_total_sessions: 3,
        };
        let mut mgr = DownloadSessionManager::with_config(config);

        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();
        mgr.start_session("task-2", 0, "http");
        mgr.close_session("task-2", 100, SessionOutcome::Completed, None)
            .unwrap();
        mgr.start_session("task-3", 0, "http");
        mgr.close_session("task-3", 100, SessionOutcome::Completed, None)
            .unwrap();
        // Now at 3 sessions, adding one more should trigger global eviction
        mgr.start_session("task-4", 0, "http");

        assert!(mgr.total_session_count() <= 3);
    }

    // --- Config management ---
    #[test]
    fn test_manager_set_config() {
        let mut mgr = DownloadSessionManager::new();
        let new_config = DownloadSessionConfig {
            max_sessions_per_task: 10,
            max_total_sessions: 50,
        };
        mgr.set_config(new_config);
        assert_eq!(mgr.config().max_sessions_per_task, 10);
        assert_eq!(mgr.config().max_total_sessions, 50);
    }

    // --- Session count boundaries ---
    #[test]
    fn test_total_session_count_empty() {
        let mgr = DownloadSessionManager::new();
        assert_eq!(mgr.total_session_count(), 0);
    }

    #[test]
    fn test_active_session_count_empty() {
        let mgr = DownloadSessionManager::new();
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[test]
    fn test_active_session_count_mixed() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();
        mgr.start_session("task-2", 0, "http");

        assert_eq!(mgr.active_session_count(), 1);
        assert_eq!(mgr.total_session_count(), 2);
    }

    // --- Summary computation ---
    #[test]
    fn test_summary_all_outcomes() {
        let mut mgr = DownloadSessionManager::new();

        // Completed
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();

        // Failed
        mgr.start_session("task-1", 100, "http");
        mgr.close_session(
            "task-1",
            150,
            SessionOutcome::Failed,
            Some("err".to_string()),
        )
        .unwrap();

        // Paused
        mgr.start_session("task-1", 150, "http");
        mgr.close_session("task-1", 200, SessionOutcome::Paused, None)
            .unwrap();

        // Timed out
        mgr.start_session("task-1", 200, "http");
        mgr.close_session(
            "task-1",
            250,
            SessionOutcome::TimedOut,
            Some("timeout".to_string()),
        )
        .unwrap();

        let summary = mgr.get_task_summary("task-1").unwrap();
        assert_eq!(summary.total_sessions, 4);
        assert_eq!(summary.completed_sessions, 1);
        assert_eq!(summary.failed_sessions, 1);
        assert_eq!(summary.paused_sessions, 1);
        assert!(summary.active_session.is_none());
    }

    #[test]
    fn test_summary_total_bytes_accumulated() {
        let mut mgr = DownloadSessionManager::new();

        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 1000, SessionOutcome::Completed, None)
            .unwrap();

        mgr.start_session("task-1", 1000, "http");
        mgr.close_session("task-1", 3000, SessionOutcome::Completed, None)
            .unwrap();

        let summary = mgr.get_task_summary("task-1").unwrap();
        assert_eq!(summary.total_bytes_transferred, 3000);
    }

    #[test]
    fn test_summary_peak_speed_across_sessions() {
        let mut mgr = DownloadSessionManager::new();

        mgr.start_session("task-1", 0, "http");
        mgr.update_peak_speed("task-1", 50_000);
        mgr.close_session("task-1", 1000, SessionOutcome::Completed, None)
            .unwrap();

        mgr.start_session("task-1", 1000, "http");
        mgr.update_peak_speed("task-1", 200_000);
        mgr.close_session("task-1", 2000, SessionOutcome::Completed, None)
            .unwrap();

        let summary = mgr.get_task_summary("task-1").unwrap();
        assert_eq!(summary.peak_speed_bps, 200_000);
    }

    #[test]
    fn test_summary_overall_avg_speed_none_when_zero_duration() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        // Close immediately (zero duration)
        mgr.close_session("task-1", 0, SessionOutcome::Completed, None)
            .unwrap();

        let summary = mgr.get_task_summary("task-1").unwrap();
        // Zero duration → None
        assert!(summary.overall_avg_speed_bps.is_none());
    }

    #[test]
    fn test_get_all_summaries_empty() {
        let mgr = DownloadSessionManager::new();
        let summaries = mgr.get_all_summaries();
        assert!(summaries.is_empty());
    }

    #[test]
    fn test_get_all_summaries_multiple_tasks() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.start_session("task-2", 0, "torrent");
        mgr.start_session("task-3", 0, "ed2k");

        let summaries = mgr.get_all_summaries();
        assert_eq!(summaries.len(), 3);
    }

    // --- Persistence ---
    #[tokio::test]
    async fn test_save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mgr = DownloadSessionManager::new();
        mgr.save_to_file(&path).await.unwrap();

        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_save_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.save_to_file(&path).await.unwrap();

        mgr.clear_all();
        mgr.save_to_file(&path).await.unwrap();

        let loaded = DownloadSessionManager::load_from_file(&path).await.unwrap();
        assert_eq!(loaded.total_session_count(), 0);
    }

    #[tokio::test]
    async fn test_no_tmp_file_left() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mgr = DownloadSessionManager::new();
        mgr.save_to_file(&path).await.unwrap();

        let tmp_path = path.with_extension("tmp");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_load_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        tokio::fs::write(&path, "not valid json").await.unwrap();

        let result = DownloadSessionManager::load_from_file(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        tokio::fs::write(&path, "").await.unwrap();

        let result = DownloadSessionManager::load_from_file(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_persistence_unicode_task_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("任务-中文", 0, "http");
        mgr.start_session("タスク-日本語", 0, "torrent");
        mgr.start_session("task-🚀", 0, "ed2k");

        mgr.save_to_file(&path).await.unwrap();
        let loaded = DownloadSessionManager::load_from_file(&path).await.unwrap();

        assert_eq!(loaded.total_session_count(), 3);
        assert!(loaded.get_task_sessions("任务-中文").is_some());
        assert!(loaded.get_task_sessions("タスク-日本語").is_some());
        assert!(loaded.get_task_sessions("task-🚀").is_some());
    }

    #[tokio::test]
    async fn test_persistence_with_peak_speed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.update_peak_speed("task-1", 500_000);
        mgr.close_session("task-1", 10_000, SessionOutcome::Completed, None)
            .unwrap();

        mgr.save_to_file(&path).await.unwrap();
        let loaded = DownloadSessionManager::load_from_file(&path).await.unwrap();

        let summary = loaded.get_task_summary("task-1").unwrap();
        assert_eq!(summary.peak_speed_bps, 500_000);
    }

    #[tokio::test]
    async fn test_persistence_with_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.close_session(
            "task-1",
            100,
            SessionOutcome::Failed,
            Some("connection reset".to_string()),
        )
        .unwrap();

        mgr.save_to_file(&path).await.unwrap();
        let loaded = DownloadSessionManager::load_from_file(&path).await.unwrap();

        let sessions = loaded.get_task_sessions("task-1").unwrap();
        assert_eq!(sessions[0].error.as_deref(), Some("connection reset"));
    }

    #[tokio::test]
    async fn test_persistence_pretty_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();

        // save_to_file uses to_string_pretty
        mgr.save_to_file(&path).await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains('\n'));
    }

    // --- Convenience functions ---
    #[tokio::test]
    async fn test_convenience_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");

        save_download_sessions(&mgr, &path).await.unwrap();
        let loaded = load_download_sessions(&path).await.unwrap();
        assert_eq!(loaded.total_session_count(), 1);
    }

    // --- Boundary: max_sessions_per_task = 0 or 1 ---
    #[test]
    fn test_max_sessions_per_task_zero() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 0,
            max_total_sessions: 5000,
        };
        let mut mgr = DownloadSessionManager::with_config(config);
        mgr.start_session("task-1", 0, "http");
        // With max=0, eviction triggers immediately
        // After adding, len becomes 1 which is > 0, so session is removed
        let sessions = mgr.get_task_sessions("task-1");
        assert!(sessions.is_none() || sessions.unwrap().is_empty());
    }

    #[test]
    fn test_max_sessions_per_task_one() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 1,
            max_total_sessions: 5000,
        };
        let mut mgr = DownloadSessionManager::with_config(config);

        mgr.start_session("task-1", 0, "http");
        mgr.close_session("task-1", 100, SessionOutcome::Completed, None)
            .unwrap();
        mgr.start_session("task-1", 100, "http");

        let sessions = mgr.get_task_sessions("task-1").unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].bytes_at_start, 100);
    }

    // --- Boundary: max_total_sessions = 0 ---
    #[test]
    fn test_max_total_sessions_zero() {
        let config = DownloadSessionConfig {
            max_sessions_per_task: 100,
            max_total_sessions: 0,
        };
        let mut mgr = DownloadSessionManager::with_config(config);
        mgr.start_session("task-1", 0, "http");
        // Global limit eviction triggers
        assert!(mgr.total_session_count() <= 1);
    }

    // --- Complex workflow ---
    #[test]
    fn test_complex_workflow_full_lifecycle() {
        let mut mgr = DownloadSessionManager::new();

        // Task 1: start → complete → restart → fail → restart → pause
        mgr.start_session("task-1", 0, "http");
        mgr.update_peak_speed("task-1", 100_000);
        mgr.close_session("task-1", 5000, SessionOutcome::Completed, None)
            .unwrap();

        mgr.start_session("task-1", 5000, "http");
        mgr.update_peak_speed("task-1", 200_000);
        mgr.close_session(
            "task-1",
            7000,
            SessionOutcome::Failed,
            Some("network error".to_string()),
        )
        .unwrap();

        mgr.start_session("task-1", 7000, "http");
        mgr.update_peak_speed("task-1", 150_000);
        mgr.close_session("task-1", 10_000, SessionOutcome::Paused, None)
            .unwrap();

        // Task 2: start → in progress
        mgr.start_session("task-2", 0, "torrent");
        mgr.update_peak_speed("task-2", 50_000);

        // Verify summaries
        let s1 = mgr.get_task_summary("task-1").unwrap();
        assert_eq!(s1.total_sessions, 3);
        assert_eq!(s1.completed_sessions, 1);
        assert_eq!(s1.failed_sessions, 1);
        assert_eq!(s1.paused_sessions, 1);
        assert_eq!(s1.peak_speed_bps, 200_000);
        assert!(s1.active_session.is_none());

        let s2 = mgr.get_task_summary("task-2").unwrap();
        assert_eq!(s2.total_sessions, 1);
        assert!(s2.active_session.is_some());

        assert_eq!(mgr.total_session_count(), 4);
        assert_eq!(mgr.active_session_count(), 1);

        // Remove task-1 and verify
        assert!(mgr.remove_task_sessions("task-1"));
        assert_eq!(mgr.total_session_count(), 1);
        assert!(mgr.get_task_summary("task-1").is_none());

        // Clear all
        mgr.clear_all();
        assert_eq!(mgr.total_session_count(), 0);
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[test]
    fn test_complex_workflow_multiple_tasks_independent() {
        let mut mgr = DownloadSessionManager::new();

        for i in 0..10 {
            let task_id = format!("task-{}", i);
            mgr.start_session(&task_id, i * 1000, "http");
            if i % 2 == 0 {
                mgr.close_session(&task_id, (i + 1) * 1000, SessionOutcome::Completed, None)
                    .unwrap();
            }
        }

        assert_eq!(mgr.total_session_count(), 10);
        // Even indices (0,2,4,6,8) are closed → 5 active
        assert_eq!(mgr.active_session_count(), 5);

        let summaries = mgr.get_all_summaries();
        assert_eq!(summaries.len(), 10);
    }

    #[test]
    fn test_complex_workflow_restart_same_task_many_times() {
        let mut mgr = DownloadSessionManager::new();

        for i in 0..20 {
            mgr.start_session("task-1", i * 100, "http");
            if i < 19 {
                mgr.close_session("task-1", (i + 1) * 100, SessionOutcome::Completed, None)
                    .unwrap();
            }
        }

        let sessions = mgr.get_task_sessions("task-1").unwrap();
        // Default max_sessions_per_task = 50, so all 20 fit
        assert_eq!(sessions.len(), 20);
    }

    // --- Config serde with missing fields ---
    #[test]
    fn test_config_serde_missing_fields_error() {
        let json = r#"{"max_sessions_per_task": 10}"#;
        let result = serde_json::from_str::<DownloadSessionConfig>(json);
        assert!(result.is_err());
    }

    // --- Session ID format ---
    #[test]
    fn test_session_id_is_uuid_format() {
        let session = DownloadSession::new("task-1", 0, "http");
        // UUID v4 format: 8-4-4-4-12 hex chars
        let parts: Vec<&str> = session.id.split('-').collect();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
    }

    // --- remove_task_sessions idempotent ---
    #[test]
    fn test_remove_task_sessions_idempotent() {
        let mut mgr = DownloadSessionManager::new();
        mgr.start_session("task-1", 0, "http");
        assert!(mgr.remove_task_sessions("task-1"));
        assert!(!mgr.remove_task_sessions("task-1"));
        assert!(!mgr.remove_task_sessions("task-1"));
    }

    // --- clear_all idempotent ---
    #[test]
    fn test_clear_all_idempotent() {
        let mut mgr = DownloadSessionManager::new();
        mgr.clear_all();
        mgr.clear_all();
        assert_eq!(mgr.total_session_count(), 0);
    }

    // --- Large number of sessions ---
    #[test]
    fn test_many_sessions() {
        let mut mgr = DownloadSessionManager::new();
        for i in 0..100 {
            let task_id = format!("task-{}", i);
            mgr.start_session(&task_id, 0, "http");
        }
        assert_eq!(mgr.total_session_count(), 100);
        assert_eq!(mgr.active_session_count(), 100);

        let summaries = mgr.get_all_summaries();
        assert_eq!(summaries.len(), 100);
    }

    // --- Persistence: empty manager ---
    #[tokio::test]
    async fn test_persistence_empty_manager() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let mgr = DownloadSessionManager::new();
        mgr.save_to_file(&path).await.unwrap();

        let loaded = DownloadSessionManager::load_from_file(&path).await.unwrap();
        assert_eq!(loaded.total_session_count(), 0);
    }

    // --- Persistence: config preserved ---
    #[tokio::test]
    async fn test_persistence_config_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");

        let config = DownloadSessionConfig {
            max_sessions_per_task: 10,
            max_total_sessions: 50,
        };
        let mgr = DownloadSessionManager::with_config(config);
        mgr.save_to_file(&path).await.unwrap();

        let loaded = DownloadSessionManager::load_from_file(&path).await.unwrap();
        assert_eq!(loaded.config().max_sessions_per_task, 10);
        assert_eq!(loaded.config().max_total_sessions, 50);
    }
}
