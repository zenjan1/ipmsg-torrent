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

    // ===== Serialization roundtrip =====

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = LinkRotConfig {
            enabled: true,
            check_interval_secs: 1800,
            batch_size: 50,
            timeout_secs: 30,
            degraded_threshold_ms: 5000,
            dead_threshold: 5,
            auto_pause_dead: true,
            max_tracked: 1000,
            user_agent: "test-agent/2.0".to_string(),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let cfg2: LinkRotConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg2.enabled, true);
        assert_eq!(cfg2.check_interval_secs, 1800);
        assert_eq!(cfg2.batch_size, 50);
        assert_eq!(cfg2.timeout_secs, 30);
        assert_eq!(cfg2.degraded_threshold_ms, 5000);
        assert_eq!(cfg2.dead_threshold, 5);
        assert!(cfg2.auto_pause_dead);
        assert_eq!(cfg2.max_tracked, 1000);
        assert_eq!(cfg2.user_agent, "test-agent/2.0");
    }

    #[test]
    fn test_config_default_serde_roundtrip() {
        let cfg = LinkRotConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let cfg2: LinkRotConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg2.enabled, cfg.enabled);
        assert_eq!(cfg2.check_interval_secs, cfg.check_interval_secs);
        assert_eq!(cfg2.batch_size, cfg.batch_size);
        assert_eq!(cfg2.timeout_secs, cfg.timeout_secs);
        assert_eq!(cfg2.degraded_threshold_ms, cfg.degraded_threshold_ms);
        assert_eq!(cfg2.dead_threshold, cfg.dead_threshold);
        assert_eq!(cfg2.auto_pause_dead, cfg.auto_pause_dead);
        assert_eq!(cfg2.max_tracked, cfg.max_tracked);
        assert_eq!(cfg2.user_agent, cfg.user_agent);
    }

    #[test]
    fn test_link_status_serde_roundtrip() {
        for status in [
            LinkStatus::Unknown,
            LinkStatus::Healthy,
            LinkStatus::Degraded,
            LinkStatus::Dead,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let s2: LinkStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, s2);
        }
    }

    #[test]
    fn test_link_status_serde_rename() {
        // Verify snake_case rename
        let json = serde_json::to_string(&LinkStatus::Unknown).unwrap();
        assert_eq!(json, "\"unknown\"");
        let json = serde_json::to_string(&LinkStatus::Healthy).unwrap();
        assert_eq!(json, "\"healthy\"");
        let json = serde_json::to_string(&LinkStatus::Degraded).unwrap();
        assert_eq!(json, "\"degraded\"");
        let json = serde_json::to_string(&LinkStatus::Dead).unwrap();
        assert_eq!(json, "\"dead\"");
    }

    #[test]
    fn test_link_check_result_serde_roundtrip() {
        let result = LinkCheckResult {
            task_id: "task-1".to_string(),
            url: "http://example.com/file.zip".to_string(),
            status: LinkStatus::Healthy,
            http_status: Some(200),
            response_time_ms: Some(150),
            consecutive_failures: 0,
            total_checks: 10,
            last_success: Some(Utc::now()),
            last_check: Utc::now(),
            last_error: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        let r2: LinkCheckResult = serde_json::from_str(&json).unwrap();
        assert_eq!(r2.task_id, "task-1");
        assert_eq!(r2.url, "http://example.com/file.zip");
        assert_eq!(r2.status, LinkStatus::Healthy);
        assert_eq!(r2.http_status, Some(200));
        assert_eq!(r2.response_time_ms, Some(150));
        assert_eq!(r2.consecutive_failures, 0);
        assert_eq!(r2.total_checks, 10);
        assert!(r2.last_success.is_some());
        assert!(r2.last_error.is_none());
    }

    #[test]
    fn test_link_check_result_with_error_serde() {
        let result = LinkCheckResult {
            task_id: "task-err".to_string(),
            url: "http://dead.com/f".to_string(),
            status: LinkStatus::Dead,
            http_status: None,
            response_time_ms: None,
            consecutive_failures: 5,
            total_checks: 8,
            last_success: None,
            last_check: Utc::now(),
            last_error: Some("connection refused".to_string()),
        };
        let json = serde_json::to_string(&result).unwrap();
        let r2: LinkCheckResult = serde_json::from_str(&json).unwrap();
        assert_eq!(r2.status, LinkStatus::Dead);
        assert_eq!(r2.consecutive_failures, 5);
        assert_eq!(r2.last_error.as_deref(), Some("connection refused"));
    }

    #[test]
    fn test_summary_serde_roundtrip() {
        let summary = LinkRotSummary {
            total_tracked: 100,
            healthy: 80,
            degraded: 10,
            dead: 5,
            unknown: 5,
            auto_paused: 3,
            total_checks: 500,
            last_check: Some(Utc::now()),
            worst_tasks: vec![],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let s2: LinkRotSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(s2.total_tracked, 100);
        assert_eq!(s2.healthy, 80);
        assert_eq!(s2.degraded, 10);
        assert_eq!(s2.dead, 5);
        assert_eq!(s2.unknown, 5);
        assert_eq!(s2.auto_paused, 3);
        assert_eq!(s2.total_checks, 500);
    }

    // ===== Default values =====

    #[test]
    fn test_config_default_field_values() {
        let cfg = LinkRotConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.check_interval_secs, 3600);
        assert_eq!(cfg.batch_size, 20);
        assert_eq!(cfg.timeout_secs, 15);
        assert_eq!(cfg.degraded_threshold_ms, 3000);
        assert_eq!(cfg.dead_threshold, 3);
        assert!(!cfg.auto_pause_dead);
        assert_eq!(cfg.max_tracked, 500);
        assert_eq!(cfg.user_agent, "ipmsg-torrent-link-rot/1.0");
    }

    // ===== Config management =====

    #[test]
    fn test_config_accessor() {
        let (_dir, path) = test_dir();
        let det = LinkRotDetector::new(&path);
        let cfg = det.config();
        assert!(!cfg.enabled);
        assert_eq!(cfg.batch_size, 20);
    }

    #[tokio::test]
    async fn test_set_config_preserves_results() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://a.com/1");
        det.apply_check_result("t1", true, Some(200), Some(100), None);
        assert_eq!(det.results.len(), 1);

        let mut cfg = LinkRotConfig::default();
        cfg.enabled = true;
        cfg.batch_size = 100;
        det.set_config(cfg).await.unwrap();

        // Results should still be there
        assert_eq!(det.results.len(), 1);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Healthy);
        assert!(det.config().enabled);
        assert_eq!(det.config().batch_size, 100);
    }

    #[tokio::test]
    async fn test_set_config_custom_values() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        let cfg = LinkRotConfig {
            enabled: true,
            check_interval_secs: 600,
            batch_size: 10,
            timeout_secs: 5,
            degraded_threshold_ms: 1000,
            dead_threshold: 2,
            auto_pause_dead: true,
            max_tracked: 100,
            user_agent: "custom/1.0".to_string(),
        };
        det.set_config(cfg).await.unwrap();

        let c = det.config();
        assert!(c.enabled);
        assert_eq!(c.check_interval_secs, 600);
        assert_eq!(c.batch_size, 10);
        assert_eq!(c.timeout_secs, 5);
        assert_eq!(c.degraded_threshold_ms, 1000);
        assert_eq!(c.dead_threshold, 2);
        assert!(c.auto_pause_dead);
        assert_eq!(c.max_tracked, 100);
        assert_eq!(c.user_agent, "custom/1.0");
    }

    // ===== track_task boundaries =====

    #[test]
    fn test_track_empty_task_id() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("", "http://a.com/1");
        assert_eq!(det.results.len(), 1);
        assert!(det.get_result("").is_some());
    }

    #[test]
    fn test_track_empty_url() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "");
        assert_eq!(det.get_result("t1").unwrap().url, "");
    }

    #[test]
    fn test_track_duplicate_overwrites() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://old.com/f");
        det.track_task("t1", "http://new.com/f");
        assert_eq!(det.results.len(), 1);
        assert_eq!(det.get_result("t1").unwrap().url, "http://new.com/f");
    }

    #[test]
    fn test_untrack_nonexistent() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.untrack_task("nonexistent");
        assert!(det.results.is_empty());
    }

    #[test]
    fn test_update_url_nonexistent() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.update_url("nonexistent", "http://new.com/f");
        // Should be a no-op, no panic
        assert!(det.results.is_empty());
    }

    // ===== apply_check_result detailed =====

    #[test]
    fn test_apply_success_no_response_time() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://a.com/1");

        det.apply_check_result("t1", true, Some(200), None, None);
        let r = det.get_result("t1").unwrap();
        assert_eq!(r.status, LinkStatus::Healthy);
        assert_eq!(r.http_status, Some(200));
        assert!(r.response_time_ms.is_none());
        assert!(r.last_success.is_some());
        assert!(r.last_error.is_none());
    }

    #[test]
    fn test_apply_success_clears_error() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://a.com/1");

        // First: failure
        det.apply_check_result("t1", false, None, None, Some("timeout".into()));
        assert_eq!(
            det.get_result("t1").unwrap().last_error.as_deref(),
            Some("timeout")
        );

        // Then: success clears error
        det.apply_check_result("t1", true, Some(200), Some(100), None);
        assert!(det.get_result("t1").unwrap().last_error.is_none());
    }

    #[test]
    fn test_apply_failure_error_tracking() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://a.com/1");

        det.apply_check_result(
            "t1",
            false,
            Some(503),
            None,
            Some("service unavailable".into()),
        );
        let r = det.get_result("t1").unwrap();
        assert_eq!(r.http_status, Some(503));
        assert_eq!(r.last_error.as_deref(), Some("service unavailable"));
        assert_eq!(r.consecutive_failures, 1);
    }

    #[test]
    fn test_apply_dead_threshold_one() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;
        det.track_task("t1", "http://a.com/1");

        let newly_dead = det.apply_check_result("t1", false, None, None, Some("err".into()));
        assert!(newly_dead);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Dead);
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 1);
    }

    #[test]
    fn test_apply_recovery_from_dead() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 2;
        det.track_task("t1", "http://a.com/1");

        // Make it dead
        det.apply_check_result("t1", false, None, None, Some("err".into()));
        let newly_dead = det.apply_check_result("t1", false, None, None, Some("err".into()));
        assert!(newly_dead);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Dead);

        // Recovery
        let newly_dead = det.apply_check_result("t1", true, Some(200), Some(50), None);
        assert!(!newly_dead);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Healthy);
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 0);
    }

    #[test]
    fn test_apply_already_dead_not_newly_dead() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;
        det.track_task("t1", "http://a.com/1");

        det.apply_check_result("t1", false, None, None, Some("err".into()));
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Dead);

        // Another failure while already dead → not newly dead
        let newly_dead = det.apply_check_result("t1", false, None, None, Some("err2".into()));
        assert!(!newly_dead);
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 2);
    }

    #[test]
    fn test_apply_degraded_exact_threshold() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.degraded_threshold_ms = 1000;
        det.track_task("t1", "http://a.com/1");

        // Exactly at threshold → healthy (not degraded)
        det.apply_check_result("t1", true, Some(200), Some(1000), None);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Healthy);

        // Just above threshold → degraded
        det.apply_check_result("t1", true, Some(200), Some(1001), None);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Degraded);
    }

    #[test]
    fn test_apply_total_checks_increment() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://a.com/1");

        for i in 0..5 {
            det.apply_check_result("t1", true, Some(200), Some(100), None);
            assert_eq!(det.get_result("t1").unwrap().total_checks, i + 1);
        }
    }

    // ===== next_batch =====

    #[test]
    fn test_next_batch_empty() {
        let (_dir, path) = test_dir();
        let det = LinkRotDetector::new(&path);
        let batch = det.next_batch();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_next_batch_dead_with_zero_checks_included() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;
        det.config.batch_size = 10;

        // Track but don't check → dead_threshold=1 but total_checks=0
        det.track_task("t1", "http://a.com/1");
        // Status is Unknown, not Dead, so it should be in batch
        let batch = det.next_batch();
        assert!(batch.contains(&"t1".to_string()));
    }

    #[test]
    fn test_next_batch_respects_batch_size() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.batch_size = 3;

        for i in 0..10 {
            det.track_task(&format!("t{i}"), &format!("http://a.com/{i}"));
        }

        let batch = det.next_batch();
        assert_eq!(batch.len(), 3);
    }

    // ===== summary =====

    #[test]
    fn test_summary_empty() {
        let (_dir, path) = test_dir();
        let det = LinkRotDetector::new(&path);
        let s = det.summary();
        assert_eq!(s.total_tracked, 0);
        assert_eq!(s.healthy, 0);
        assert_eq!(s.degraded, 0);
        assert_eq!(s.dead, 0);
        assert_eq!(s.unknown, 0);
        assert_eq!(s.auto_paused, 0);
        assert_eq!(s.total_checks, 0);
        assert!(s.last_check.is_none());
        assert!(s.worst_tasks.is_empty());
    }

    #[test]
    fn test_summary_all_healthy() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        for i in 0..5 {
            det.track_task(&format!("t{i}"), &format!("http://a.com/{i}"));
            det.apply_check_result(&format!("t{i}"), true, Some(200), Some(100), None);
        }
        let s = det.summary();
        assert_eq!(s.total_tracked, 5);
        assert_eq!(s.healthy, 5);
        assert_eq!(s.dead, 0);
        assert!(s.worst_tasks.is_empty());
    }

    #[test]
    fn test_summary_all_degraded() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.degraded_threshold_ms = 100;
        for i in 0..3 {
            det.track_task(&format!("t{i}"), &format!("http://a.com/{i}"));
            det.apply_check_result(&format!("t{i}"), true, Some(200), Some(500), None);
        }
        let s = det.summary();
        assert_eq!(s.degraded, 3);
        assert_eq!(s.healthy, 0);
    }

    #[test]
    fn test_summary_all_dead() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;
        for i in 0..4 {
            det.track_task(&format!("t{i}"), &format!("http://a.com/{i}"));
            det.apply_check_result(&format!("t{i}"), false, None, None, Some("err".into()));
        }
        let s = det.summary();
        assert_eq!(s.dead, 4);
        assert_eq!(s.worst_tasks.len(), 4);
    }

    #[test]
    fn test_summary_worst_tasks_sorted() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;

        for i in 0..5 {
            det.track_task(&format!("t{i}"), &format!("http://a.com/{i}"));
            det.apply_check_result(&format!("t{i}"), false, None, None, Some("err".into()));
        }
        // Add more failures to t2 and t4
        for _ in 0..3 {
            det.apply_check_result("t2", false, None, None, Some("err".into()));
        }
        for _ in 0..5 {
            det.apply_check_result("t4", false, None, None, Some("err".into()));
        }

        let s = det.summary();
        // worst_tasks sorted by consecutive_failures desc
        assert_eq!(s.worst_tasks[0].task_id, "t4");
        assert_eq!(s.worst_tasks[1].task_id, "t2");
    }

    #[test]
    fn test_summary_worst_tasks_max_10() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;

        for i in 0..15 {
            det.track_task(&format!("t{i}"), &format!("http://a.com/{i}"));
            det.apply_check_result(&format!("t{i}"), false, None, None, Some("err".into()));
        }

        let s = det.summary();
        assert_eq!(s.worst_tasks.len(), 10); // truncated to 10
    }

    // ===== format_report =====

    #[test]
    fn test_report_never_checked() {
        // No tasks tracked → last_check is None → "Last check: never"
        let (_dir, path) = test_dir();
        let det = LinkRotDetector::new(&path);

        let report = det.format_report();
        assert!(report.contains("Last check: never"));
    }

    #[test]
    fn test_report_contains_config_info() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.enabled = true;
        det.config.check_interval_secs = 7200;
        det.config.batch_size = 30;
        det.config.dead_threshold = 5;
        det.config.degraded_threshold_ms = 5000;
        det.config.auto_pause_dead = true;

        let report = det.format_report();
        assert!(report.contains("true"));
        assert!(report.contains("7200"));
        assert!(report.contains("30"));
        assert!(report.contains("5"));
        assert!(report.contains("5000"));
        assert!(report.contains("yes"));
    }

    #[test]
    fn test_report_auto_pause_no() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.auto_pause_dead = false;
        let report = det.format_report();
        assert!(report.contains("no"));
    }

    #[test]
    fn test_report_dead_link_with_error() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 1;
        det.track_task("t1", "http://dead.com/f");
        det.apply_check_result("t1", false, None, None, Some("404 Not Found".into()));

        let report = det.format_report();
        assert!(report.contains("404 Not Found"));
        assert!(report.contains("💀"));
        assert!(report.contains("t1"));
    }

    // ===== LinkRotError Display =====

    #[test]
    fn test_error_display_io() {
        let err = LinkRotError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        let msg = format!("{err}");
        assert!(msg.contains("I/O error"));
        assert!(msg.contains("file missing"));
    }

    #[test]
    fn test_error_display_json() {
        let err = LinkRotError::Json(serde_json::from_str::<LinkRotConfig>("invalid").unwrap_err());
        let msg = format!("{err}");
        assert!(msg.contains("JSON error"));
    }

    #[test]
    fn test_error_display_task_not_found() {
        let err = LinkRotError::TaskNotFound("task-xyz".to_string());
        let msg = format!("{err}");
        assert!(msg.contains("task task-xyz not found"));
    }

    // ===== Persistence =====

    #[tokio::test]
    async fn test_load_missing_files_no_error() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        // Should not error when files don't exist
        det.load().await.unwrap();
        assert!(!det.config().enabled);
        assert!(det.results.is_empty());
    }

    #[tokio::test]
    async fn test_load_invalid_config_json() {
        let (_dir, path) = test_dir();
        let config_path = path.join("link_rot_config.json");
        fs::write(&config_path, "not json").await.unwrap();

        let mut det = LinkRotDetector::new(&path);
        let result = det.load().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_invalid_results_json() {
        let (_dir, path) = test_dir();
        let results_path = path.join("link_rot_results.json");
        fs::write(&results_path, "{{{invalid").await.unwrap();

        let mut det = LinkRotDetector::new(&path);
        let result = det.load().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_persistence_preserves_all_fields() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.enabled = true;
        det.config.check_interval_secs = 999;
        det.config.batch_size = 77;
        det.config.timeout_secs = 42;
        det.config.degraded_threshold_ms = 8888;
        det.config.dead_threshold = 7;
        det.config.auto_pause_dead = true;
        det.config.max_tracked = 333;
        det.config.user_agent = "persist-test/1.0".to_string();

        det.track_task("t1", "http://example.com/f1");
        det.apply_check_result("t1", true, Some(200), Some(50), None);
        det.track_task("t2", "http://dead.com/f2");
        det.apply_check_result("t2", false, None, None, Some("timeout".into()));
        // Note: auto_paused_count is a runtime counter, not persisted
        det.auto_paused_count = 42;

        det.save_config().await.unwrap();
        det.save_results().await.unwrap();

        let mut det2 = LinkRotDetector::new(&path);
        det2.load().await.unwrap();

        assert_eq!(det2.config().check_interval_secs, 999);
        assert_eq!(det2.config().batch_size, 77);
        assert_eq!(det2.config().timeout_secs, 42);
        assert_eq!(det2.config().degraded_threshold_ms, 8888);
        assert_eq!(det2.config().dead_threshold, 7);
        assert!(det2.config().auto_pause_dead);
        assert_eq!(det2.config().max_tracked, 333);
        assert_eq!(det2.config().user_agent, "persist-test/1.0");
        assert_eq!(det2.results.len(), 2);
        assert_eq!(det2.get_result("t1").unwrap().status, LinkStatus::Healthy);
        assert_eq!(det2.get_result("t2").unwrap().status, LinkStatus::Unknown);
        // auto_paused_count is runtime-only, resets to 0 on load
        assert_eq!(det2.auto_paused_count, 0);
    }

    // ===== Clone/Debug traits =====

    #[test]
    fn test_link_status_clone_copy() {
        let s = LinkStatus::Healthy;
        let s2 = s; // Copy
        let s3 = s.clone();
        assert_eq!(s, s2);
        assert_eq!(s, s3);
    }

    #[test]
    fn test_link_status_debug() {
        let s = LinkStatus::Dead;
        let debug = format!("{s:?}");
        assert!(debug.contains("Dead"));
    }

    #[test]
    fn test_link_check_result_clone() {
        let r = LinkCheckResult {
            task_id: "t1".to_string(),
            url: "http://a.com".to_string(),
            status: LinkStatus::Healthy,
            http_status: Some(200),
            response_time_ms: Some(100),
            consecutive_failures: 0,
            total_checks: 1,
            last_success: Some(Utc::now()),
            last_check: Utc::now(),
            last_error: None,
        };
        let r2 = r.clone();
        assert_eq!(r2.task_id, "t1");
        assert_eq!(r2.status, LinkStatus::Healthy);
    }

    #[test]
    fn test_detector_debug() {
        let (_dir, path) = test_dir();
        let det = LinkRotDetector::new(&path);
        let debug = format!("{det:?}");
        assert!(debug.contains("LinkRotDetector"));
    }

    // ===== Complex scenario =====

    #[test]
    fn test_full_lifecycle() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.dead_threshold = 3;
        det.config.degraded_threshold_ms = 1000;
        det.config.batch_size = 10;

        // Phase 1: Track tasks
        det.track_task("t1", "http://good.com/f1");
        det.track_task("t2", "http://slow.com/f2");
        det.track_task("t3", "http://dead.com/f3");
        det.track_task("t4", "http://unknown.com/f4");
        assert_eq!(det.summary().total_tracked, 4);
        assert_eq!(det.summary().unknown, 4);

        // Phase 2: Check t1 → healthy
        det.apply_check_result("t1", true, Some(200), Some(200), None);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Healthy);

        // Phase 3: Check t2 → degraded (slow)
        det.apply_check_result("t2", true, Some(200), Some(2000), None);
        assert_eq!(det.get_result("t2").unwrap().status, LinkStatus::Degraded);

        // Phase 4: Check t3 → failures → dead
        det.apply_check_result("t3", false, None, None, Some("timeout".into()));
        det.apply_check_result("t3", false, None, None, Some("timeout".into()));
        let newly_dead = det.apply_check_result("t3", false, None, None, Some("timeout".into()));
        assert!(newly_dead);

        // Phase 5: Summary
        let s = det.summary();
        assert_eq!(s.healthy, 1);
        assert_eq!(s.degraded, 1);
        assert_eq!(s.dead, 1);
        assert_eq!(s.unknown, 1);
        assert_eq!(s.worst_tasks.len(), 1);
        assert_eq!(s.worst_tasks[0].task_id, "t3");

        // Phase 6: t3 not in next batch (dead with checks > 0)
        let batch = det.next_batch();
        assert!(!batch.contains(&"t3".to_string()));

        // Phase 7: t3 recovers
        det.apply_check_result("t3", true, Some(200), Some(100), None);
        assert_eq!(det.get_result("t3").unwrap().status, LinkStatus::Healthy);
        assert_eq!(det.get_result("t3").unwrap().consecutive_failures, 0);

        // Phase 8: Report
        let report = det.format_report();
        assert!(report.contains("Link Rot Detection Report"));
        assert!(report.contains("3")); // total checks
    }

    #[test]
    fn test_multiple_url_updates() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://v1.com/f");
        det.apply_check_result("t1", false, None, None, Some("err1".into()));

        det.update_url("t1", "http://v2.com/f");
        assert_eq!(det.get_result("t1").unwrap().url, "http://v2.com/f");
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Unknown);
        assert_eq!(det.get_result("t1").unwrap().consecutive_failures, 0);
        assert!(det.get_result("t1").unwrap().last_error.is_none());

        det.apply_check_result("t1", true, Some(200), Some(50), None);
        assert_eq!(det.get_result("t1").unwrap().status, LinkStatus::Healthy);
    }

    #[test]
    fn test_max_tracked_allows_existing_update() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.config.max_tracked = 2;

        det.track_task("t1", "http://a.com/1");
        det.track_task("t2", "http://a.com/2");
        // t3 rejected
        det.track_task("t3", "http://a.com/3");
        assert_eq!(det.results.len(), 2);

        // But re-tracking t1 (existing) should work
        det.track_task("t1", "http://updated.com/1");
        assert_eq!(det.results.len(), 2);
        assert_eq!(det.get_result("t1").unwrap().url, "http://updated.com/1");
    }

    #[test]
    fn test_record_auto_pause_accumulates() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        for _ in 0..10 {
            det.record_auto_pause();
        }
        assert_eq!(det.summary().auto_paused, 10);
    }

    #[test]
    fn test_all_results_accessor() {
        let (_dir, path) = test_dir();
        let mut det = LinkRotDetector::new(&path);
        det.track_task("t1", "http://a.com/1");
        det.track_task("t2", "http://a.com/2");

        let all = det.all_results();
        assert_eq!(all.len(), 2);
        assert!(all.contains_key("t1"));
        assert!(all.contains_key("t2"));
    }

    #[test]
    fn test_get_result_nonexistent() {
        let (_dir, path) = test_dir();
        let det = LinkRotDetector::new(&path);
        assert!(det.get_result("nonexistent").is_none());
    }
}
