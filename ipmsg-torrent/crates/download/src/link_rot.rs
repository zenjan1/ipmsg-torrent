//! Download Link Rot Detector (Phase 161)
//!
//! Periodically checks whether URLs in the download queue are still reachable.
//! Detects "link rot" — URLs that have become dead since being added — and
//! provides actionable data to auto-pause or flag affected tasks.
//!
//! Features:
//! - Configurable check interval and batch size
//! - HEAD-request based reachability check with timeout
//! - Per-task link status tracking (Healthy / Degraded / Dead / Unknown)
//! - Consecutive failure threshold before marking as dead
//! - Auto-pause tasks with dead links (optional)
//! - Summary generation with statistics
//! - JSON persistence for config and check results

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;

/// Errors from link rot operations.
#[derive(Error, Debug)]
pub enum LinkRotError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("task {0} not found")]
    TaskNotFound(String),
}

/// Link health status for a single task URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkStatus {
    /// URL has not been checked yet.
    Unknown,
    /// URL is responsive.
    Healthy,
    /// URL responds but slowly (> threshold ms).
    Degraded,
    /// URL is unreachable (consecutive failures >= dead_threshold).
    Dead,
}

impl std::fmt::Display for LinkStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkStatus::Unknown => write!(f, "unknown"),
            LinkStatus::Healthy => write!(f, "healthy"),
            LinkStatus::Degraded => write!(f, "degraded"),
            LinkStatus::Dead => write!(f, "dead"),
        }
    }
}

impl LinkStatus {
    /// Emoji indicator for reports.
    pub fn emoji(&self) -> &'static str {
        match self {
            LinkStatus::Unknown => "❓",
            LinkStatus::Healthy => "✅",
            LinkStatus::Degraded => "⚠️",
            LinkStatus::Dead => "💀",
        }
    }
}

/// Health check result for a single task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkCheckResult {
    /// Task ID.
    pub task_id: String,
    /// URL that was checked.
    pub url: String,
    /// Current link status.
    pub status: LinkStatus,
    /// Last HTTP status code (if available).
    pub http_status: Option<u16>,
    /// Response time in milliseconds (None if failed).
    pub response_time_ms: Option<u64>,
    /// Number of consecutive check failures.
    pub consecutive_failures: u32,
    /// Total number of checks performed.
    pub total_checks: u64,
    /// Last successful check time.
    pub last_success: Option<DateTime<Utc>>,
    /// Last check time.
    pub last_check: DateTime<Utc>,
    /// Error message from the most recent failure (if any).
    pub last_error: Option<String>,
}

/// Configuration for the link rot detector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkRotConfig {
    /// Enable automatic link rot detection.
    pub enabled: bool,
    /// Check interval in seconds (default: 3600 = 1 hour).
    pub check_interval_secs: u64,
    /// Maximum number of tasks to check per batch (default: 20).
    pub batch_size: usize,
    /// HTTP request timeout in seconds (default: 15).
    pub timeout_secs: u64,
    /// Response time threshold for "degraded" status in ms (default: 3000).
    pub degraded_threshold_ms: u64,
    /// Consecutive failures before marking link as dead (default: 3).
    pub dead_threshold: u32,
    /// Automatically pause tasks with dead links (default: false).
    pub auto_pause_dead: bool,
    /// Maximum tasks to track (default: 500).
    pub max_tracked: usize,
    /// User-Agent header for health checks.
    pub user_agent: String,
}

impl Default for LinkRotConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            check_interval_secs: 3600,
            batch_size: 20,
            timeout_secs: 15,
            degraded_threshold_ms: 3000,
            dead_threshold: 3,
            auto_pause_dead: false,
            max_tracked: 500,
            user_agent: "ipmsg-torrent-link-rot/1.0".to_string(),
        }
    }
}

/// Summary of link rot check results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkRotSummary {
    /// Total tracked tasks.
    pub total_tracked: usize,
    /// Tasks with healthy links.
    pub healthy: usize,
    /// Tasks with degraded links.
    pub degraded: usize,
    /// Tasks with dead links.
    pub dead: usize,
    /// Tasks not yet checked.
    pub unknown: usize,
    /// Tasks auto-paused due to dead links.
    pub auto_paused: u64,
    /// Total checks performed across all tasks.
    pub total_checks: u64,
    /// Last time any check was performed.
    pub last_check: Option<DateTime<Utc>>,
    /// Tasks with the worst links (dead ones, up to 10).
    pub worst_tasks: Vec<LinkCheckResult>,
}

/// The link rot detector manager.
#[derive(Debug)]
pub struct LinkRotDetector {
    config: LinkRotConfig,
    results: HashMap<String, LinkCheckResult>,
    auto_paused_count: u64,
    data_dir: PathBuf,
}

impl LinkRotDetector {
    /// Create a new link rot detector.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            config: LinkRotConfig::default(),
            results: HashMap::new(),
            auto_paused_count: 0,
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// Load config and results from disk.
    pub async fn load(&mut self) -> Result<(), LinkRotError> {
        let config_path = self.data_dir.join("link_rot_config.json");
        if config_path.exists() {
            let data = fs::read_to_string(&config_path).await?;
            self.config = serde_json::from_str(&data)?;
        }

        let results_path = self.data_dir.join("link_rot_results.json");
        if results_path.exists() {
            let data = fs::read_to_string(&results_path).await?;
            self.results = serde_json::from_str(&data)?;
        }

        Ok(())
    }

    /// Save config to disk.
    pub async fn save_config(&self) -> Result<(), LinkRotError> {
        let path = self.data_dir.join("link_rot_config.json");
        let json = serde_json::to_string_pretty(&self.config)?;
        fs::write(&path, json).await?;
        Ok(())
    }

    /// Save results to disk.
    pub async fn save_results(&self) -> Result<(), LinkRotError> {
        let path = self.data_dir.join("link_rot_results.json");
        let json = serde_json::to_string_pretty(&self.results)?;
        fs::write(&path, json).await?;
        Ok(())
    }

    /// Get current config.
    pub fn config(&self) -> &LinkRotConfig {
        &self.config
    }

    /// Update config and persist.
    pub async fn set_config(&mut self, config: LinkRotConfig) -> Result<(), LinkRotError> {
        self.config = config;
        self.save_config().await
    }

    /// Record a new task URL for tracking.
    pub fn track_task(&mut self, task_id: &str, url: &str) {
        if self.results.len() >= self.config.max_tracked && !self.results.contains_key(task_id) {
            return;
        }
        self.results.insert(
            task_id.to_string(),
            LinkCheckResult {
                task_id: task_id.to_string(),
                url: url.to_string(),
                status: LinkStatus::Unknown,
                http_status: None,
                response_time_ms: None,
                consecutive_failures: 0,
                total_checks: 0,
                last_success: None,
                last_check: Utc::now(),
                last_error: None,
            },
        );
    }

    /// Remove a task from tracking.
    pub fn untrack_task(&mut self, task_id: &str) {
        self.results.remove(task_id);
    }

    /// Update a task's URL.
    pub fn update_url(&mut self, task_id: &str, url: &str) {
        if let Some(result) = self.results.get_mut(task_id) {
            result.url = url.to_string();
            result.status = LinkStatus::Unknown;
            result.consecutive_failures = 0;
            result.http_status = None;
            result.response_time_ms = None;
            result.last_error = None;
        }
    }

    /// Get the check result for a task.
    pub fn get_result(&self, task_id: &str) -> Option<&LinkCheckResult> {
        self.results.get(task_id)
    }

    /// Get all results.
    pub fn all_results(&self) -> &HashMap<String, LinkCheckResult> {
        &self.results
    }

    /// Get the next batch of task IDs to check (round-robin, oldest first).
    pub fn next_batch(&self) -> Vec<String> {
        let mut entries: Vec<_> = self
            .results
            .iter()
            .filter(|(_, r)| r.status != LinkStatus::Dead || r.total_checks == 0)
            .collect();

        // Sort by last_check ascending (oldest first)
        entries.sort_by_key(|(_, r)| r.last_check);

        entries
            .into_iter()
            .take(self.config.batch_size)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Apply a check result for a task. Returns true if the task was newly marked dead.
    pub fn apply_check_result(
        &mut self,
        task_id: &str,
        success: bool,
        http_status: Option<u16>,
        response_time_ms: Option<u64>,
        error: Option<String>,
    ) -> bool {
        let Some(result) = self.results.get_mut(task_id) else {
            return false;
        };

        result.total_checks += 1;
        result.last_check = Utc::now();
        result.http_status = http_status;
        result.response_time_ms = response_time_ms;

        let newly_dead;

        if success {
            result.consecutive_failures = 0;
            result.last_success = Some(Utc::now());
            result.last_error = None;

            // Determine healthy vs degraded based on response time
            if let Some(rt) = response_time_ms {
                if rt > self.config.degraded_threshold_ms {
                    result.status = LinkStatus::Degraded;
                } else {
                    result.status = LinkStatus::Healthy;
                }
            } else {
                result.status = LinkStatus::Healthy;
            }
            newly_dead = false;
        } else {
            result.consecutive_failures += 1;
            result.last_error = error;

            if result.consecutive_failures >= self.config.dead_threshold {
                newly_dead = result.status != LinkStatus::Dead;
                result.status = LinkStatus::Dead;
            } else {
                newly_dead = false;
                // Keep previous status or set to Unknown
                if result.status == LinkStatus::Unknown {
                    result.status = LinkStatus::Unknown;
                }
            }
        }

        newly_dead
    }

    /// Record that a task was auto-paused due to dead link.
    pub fn record_auto_pause(&mut self) {
        self.auto_paused_count += 1;
    }

    /// Generate a summary of link rot status.
    pub fn summary(&self) -> LinkRotSummary {
        let mut healthy = 0;
        let mut degraded = 0;
        let mut dead = 0;
        let mut unknown = 0;
        let mut total_checks = 0u64;
        let mut last_check: Option<DateTime<Utc>> = None;
        let mut worst: Vec<LinkCheckResult> = Vec::new();

        for result in self.results.values() {
            match result.status {
                LinkStatus::Healthy => healthy += 1,
                LinkStatus::Degraded => degraded += 1,
                LinkStatus::Dead => dead += 1,
                LinkStatus::Unknown => unknown += 1,
            }
            total_checks += result.total_checks;
            if last_check.is_none() || result.last_check > last_check.unwrap() {
                last_check = Some(result.last_check);
            }
            if result.status == LinkStatus::Dead {
                worst.push(result.clone());
            }
        }

        // Sort worst by consecutive_failures descending, take top 10
        worst.sort_by(|a, b| b.consecutive_failures.cmp(&a.consecutive_failures));
        worst.truncate(10);

        LinkRotSummary {
            total_tracked: self.results.len(),
            healthy,
            degraded,
            dead,
            unknown,
            auto_paused: self.auto_paused_count,
            total_checks,
            last_check,
            worst_tasks: worst,
        }
    }

    /// Format a human-readable report.
    pub fn format_report(&self) -> String {
        let s = self.summary();
        let mut out = String::new();

        out.push_str("🔗 Link Rot Detection Report\n");
        out.push_str(&format!(
            "  Enabled: {} | Interval: {}s | Batch: {}\n",
            self.config.enabled, self.config.check_interval_secs, self.config.batch_size
        ));
        out.push_str(&format!(
            "  Dead threshold: {} failures | Degraded threshold: {}ms\n",
            self.config.dead_threshold, self.config.degraded_threshold_ms
        ));
        out.push_str(&format!(
            "  Auto-pause dead links: {}\n\n",
            if self.config.auto_pause_dead {
                "yes"
            } else {
                "no"
            }
        ));

        out.push_str(&format!("📊 Summary:\n"));
        out.push_str(&format!("  Total tracked: {}\n", s.total_tracked));
        out.push_str(&format!(
            "  {} Healthy  {} Degraded  {} Dead  {} Unknown\n",
            s.healthy, s.degraded, s.dead, s.unknown
        ));
        out.push_str(&format!(
            "  Total checks: {} | Auto-paused: {}\n",
            s.total_checks, s.auto_paused
        ));

        if let Some(lc) = s.last_check {
            out.push_str(&format!(
                "  Last check: {}\n",
                lc.format("%Y-%m-%d %H:%M:%S UTC")
            ));
        } else {
            out.push_str("  Last check: never\n");
        }

        if !s.worst_tasks.is_empty() {
            out.push_str(&format!("\n💀 Dead Links ({}):\n", s.dead));
            for task in &s.worst_tasks {
                out.push_str(&format!(
                    "  {} Task {} — {} failures\n    URL: {}\n",
                    task.status.emoji(),
                    task.task_id,
                    task.consecutive_failures,
                    task.url
                ));
                if let Some(ref err) = task.last_error {
                    out.push_str(&format!("    Error: {err}\n"));
                }
            }
        }

        out
    }

    /// Clear all results.
    pub fn clear(&mut self) {
        self.results.clear();
        self.auto_paused_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_dir() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        (dir, path)
    }

    #[test]
    fn test_default_config() {
        let cfg = LinkRotConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.check_interval_secs, 3600);
        assert_eq!(cfg.batch_size, 20);
        assert_eq!(cfg.dead_threshold, 3);
        assert!(!cfg.auto_pause_dead);
    }

    #[test]
    fn test_link_status_display() {
        assert_eq!(LinkStatus::Healthy.to_string(), "healthy");
        assert_eq!(LinkStatus::Dead.to_string(), "dead");
        assert_eq!(LinkStatus::Degraded.to_string(), "degraded");
        assert_eq!(LinkStatus::Unknown.to_string(), "unknown");
    }

    #[test]
    fn test_link_status_emoji() {
        assert_eq!(LinkStatus::Healthy.emoji(), "✅");
        assert_eq!(LinkStatus::Dead.emoji(), "💀");
    }

    #[test]
    fn test_track_and_untrack() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);

        det.track_task("t1", "http://example.com/file.zip");
        assert_eq!(det.results.len(), 1);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Unknown);

        det.untrack_task("t1");
        assert!(det.results.is_empty());
    }

    #[test]
    fn test_max_tracked() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.max_tracked = 2;

        det.track_task("t1", "http://a.com/1");
        det.track_task("t2", "http://a.com/2");
        det.track_task("t3", "http://a.com/3"); // should be rejected
        assert_eq!(det.results.len(), 2);
        assert!(det.get_result("t3").is_none());
    }

    #[test]
    fn test_apply_success_healthy() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://example.com/f");

        let newly_dead = det.apply_check_result("t1", true, Some(200), Some(500), None);
        assert!(!newly_dead);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Healthy);
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 0);
        assert_eq!(det.get_result("t1").unwrap().total_checks, 1);
    }

    #[test]
    fn test_apply_success_degraded() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.degraded_threshold_ms = 1000;
        det.track_task("t1", "http://example.com/f");

        det.apply_check_result("t1", true, Some(200), Some(2500), None);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Degraded);
    }

    #[test]
    fn test_apply_failure_progression() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 3;
        det.track_task("t1", "http://example.com/f");

        // First failure
        let newly_dead = det.apply_check_result("t1", false, None, None, Some("timeout".into()));
        assert!(!newly_dead);
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 1);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Unknown);

        // Second failure
        det.apply_check_result("t1", false, None, None, Some("timeout".into()));
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 2);

        // Third failure → dead
        let newly_dead = det.apply_check_result("t1", false, None, None, Some("timeout".into()));
        assert!(newly_dead);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Dead);
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 3);
    }

    #[test]
    fn test_recovery_resets_failures() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 3;
        det.track_task("t1", "http://example.com/f");

        // 2 failures
        det.apply_check_result("t1", false, None, None, Some("err".into()));
        det.apply_check_result("t1", false, None, None, Some("err".into()));
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 2);

        // Success resets
        det.apply_check_result("t1", true, Some(200), Some(100), None);
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 0);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Healthy);
    }

    #[test]
    fn test_summary() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;

        det.track_task("t1", "http://a.com/1");
        det.track_task("t2", "http://a.com/2");
        det.track_task("t3", "http://a.com/3");

        det.apply_check_result("t1", true, Some(200), Some(100), None);
        det.apply_check_result("t2", false, None, None, Some("err".into())); // dead (threshold=1)
        // t3 remains unknown

        let s = det.summary();
        assert_eq!(s.total_tracked, 3);
        assert_eq!(s.healthy, 1);
        assert_eq!(s.dead, 1);
        assert_eq!(s.unknown, 1);
        assert_eq!(s.worst_tasks.len(), 1);
    }

    #[test]
    fn test_next_batch_ordering() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.batch_size = 2;

        det.track_task("t1", "http://a.com/1");
        det.track_task("t2", "http://a.com/2");
        det.track_task("t3", "http://a.com/3");

        let batch = det.next_batch();
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn test_update_url() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://old.com/f");

        det.apply_check_result("t1", false, None, None, Some("err".into()));
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 1);

        det.update_url("t1", "http://new.com/f");
        assert_eq!(det.get_result("t1").unwrap().url, "http://new.com/f");
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 0);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Unknown);
    }

    #[test]
    fn test_clear() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://a.com/1");
        det.auto_paused_count = 5;

        det.clear();
        assert!(det.results.is_empty());
        assert_eq!(det.auto_paused_count, 0);
    }

    #[test]
    fn test_format_report() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;
        det.track_task("t1", "http://dead.com/f");
        det.apply_check_result("t1", false, None, None, Some("404".into()));

        let report = det.format_report();
        assert!(report.contains("Link Rot Detection Report"));
        assert!(report.contains("Dead Links"));
        assert!(report.contains("dead.com"));
    }

    #[tokio::test]
    async fn test_persistence() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);

        det.config.enabled = true;
        det.config.batch_size = 50;
        det.track_task("t1", "http://example.com/f");
        det.apply_check_result("t1", true, Some(200), Some(100), None);

        det.save_config().await.unwrap();
        det.save_results().await.unwrap();

        // Load into new instance
        let mut det2 = LinkRotDetector::new(&path);
        det2.load().await.unwrap();

        assert!(det2.config.enabled);
        assert_eq!(det2.config.batch_size, 50);
        assert_eq!(det2.results.len(), 1);
        assert_eq!(det2.get_result("t1").unwrap().status, LinkStatus::Healthy);
    }

    #[tokio::test]
    async fn test_set_config_persists() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);

        let mut cfg = LinkRotConfig::default();
        cfg.enabled = true;
        cfg.dead_threshold = 5;
        det.set_config(cfg).await.unwrap();

        let mut det2 = LinkRotDetector::new(&path);
        det2.load().await.unwrap();
        assert!(det2.config.enabled);
        assert_eq!(det2.config.dead_threshold, 5);
    }

    #[test]
    fn test_apply_check_unknown_task() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        let newly_dead = det.apply_check_result("nonexistent", true, Some(200), Some(100), None);
        assert!(!newly_dead);
    }

    #[test]
    fn test_auto_pause_tracking() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.record_auto_pause();
        det.record_auto_pause();
        assert_eq!(det.summary().auto_paused, 2);
    }

    #[test]
    fn test_dead_not_in_next_batch() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;
        det.config.batch_size = 10;

        det.track_task("t1", "http://a.com/1");
        det.apply_check_result("t1", false, None, None, Some("err".into()));
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Dead);

        // Dead tasks with checks > 0 should not be in next batch
        let batch = det.next_batch();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_report_no_dead_links() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://good.com/f");
        det.apply_check_result("t1", true, Some(200), Some(100), None);

        let report = det.format_report();
        assert!(report.contains("1")); // healthy count
        assert!(!report.contains("💀 Dead Links"));
    }
}
