//! Download URL Health Monitor
//!
//! Periodically checks the health of download URLs and mirrors,
//! classifying them as healthy/degraded/dead to help the system
//! make smarter download decisions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// URL health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UrlHealthStatus {
    /// URL is responsive and fast
    Healthy,
    /// URL is responsive but slow
    Degraded,
    /// URL is unresponsive or timing out
    Dead,
    /// URL has not been checked yet
    Unknown,
}

impl std::fmt::Display for UrlHealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UrlHealthStatus::Healthy => write!(f, "Healthy"),
            UrlHealthStatus::Degraded => write!(f, "Degraded"),
            UrlHealthStatus::Dead => write!(f, "Dead"),
            UrlHealthStatus::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Health check result for a single URL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlHealthCheck {
    /// The URL being monitored
    pub url: String,
    /// Current health status
    pub status: UrlHealthStatus,
    /// Last check timestamp (Unix seconds)
    pub last_check_ts: u64,
    /// Response time in milliseconds (None if failed)
    pub response_time_ms: Option<u64>,
    /// HTTP status code (if applicable)
    pub http_status: Option<u16>,
    /// Number of consecutive failures
    pub consecutive_failures: u32,
    /// Number of consecutive successes
    pub consecutive_successes: u32,
    /// Total checks performed
    pub total_checks: u64,
    /// Total successful checks
    pub successful_checks: u64,
    /// Error message from last failure (if any)
    pub last_error: Option<String>,
}

/// Configuration for URL health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlHealthMonitorConfig {
    /// Enable automatic health monitoring
    pub enabled: bool,
    /// Check interval in seconds (default: 300 = 5 minutes)
    pub check_interval_secs: u64,
    /// Request timeout in seconds (default: 10)
    pub timeout_secs: u64,
    /// Threshold for "degraded" status (ms, default: 2000)
    pub degraded_threshold_ms: u64,
    /// Consecutive failures before marking as dead (default: 3)
    pub dead_threshold: u32,
    /// Consecutive successes to restore from dead (default: 2)
    pub recovery_threshold: u32,
    /// Maximum URLs to monitor (default: 500)
    pub max_monitored_urls: usize,
    /// User-Agent header for health checks
    pub user_agent: String,
}

impl Default for UrlHealthMonitorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_secs: 300,
            timeout_secs: 10,
            degraded_threshold_ms: 2000,
            dead_threshold: 3,
            recovery_threshold: 2,
            max_monitored_urls: 500,
            user_agent: "IPMsg-Torrent-URLHealthMonitor/1.0".to_string(),
        }
    }
}

/// Summary of URL health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlHealthSummary {
    /// Total monitored URLs
    pub total_monitored: usize,
    /// Number of healthy URLs
    pub healthy_count: usize,
    /// Number of degraded URLs
    pub degraded_count: usize,
    /// Number of dead URLs
    pub dead_count: usize,
    /// Number of unknown/unchecked URLs
    pub unknown_count: usize,
    /// Average response time across all healthy URLs (ms)
    pub avg_response_time_ms: Option<u64>,
    /// URLs checked in the last interval
    pub recently_checked: usize,
    /// Last check timestamp
    pub last_check_ts: Option<u64>,
}

/// Internal state for a monitored URL
#[derive(Debug, Clone)]
struct MonitoredUrl {
    url: String,
    status: UrlHealthStatus,
    last_check: Option<Instant>,
    last_check_ts: u64,
    response_time_ms: Option<u64>,
    http_status: Option<u16>,
    consecutive_failures: u32,
    consecutive_successes: u32,
    total_checks: u64,
    successful_checks: u64,
    last_error: Option<String>,
}

impl MonitoredUrl {
    fn new(url: String) -> Self {
        Self {
            url,
            status: UrlHealthStatus::Unknown,
            last_check: None,
            last_check_ts: 0,
            response_time_ms: None,
            http_status: None,
            consecutive_failures: 0,
            consecutive_successes: 0,
            total_checks: 0,
            successful_checks: 0,
            last_error: None,
        }
    }

    fn to_health_check(&self) -> UrlHealthCheck {
        UrlHealthCheck {
            url: self.url.clone(),
            status: self.status,
            last_check_ts: self.last_check_ts,
            response_time_ms: self.response_time_ms,
            http_status: self.http_status,
            consecutive_failures: self.consecutive_failures,
            consecutive_successes: self.consecutive_successes,
            total_checks: self.total_checks,
            successful_checks: self.successful_checks,
            last_error: self.last_error.clone(),
        }
    }
}

/// URL Health Monitor
///
/// Tracks the health of download URLs and mirrors by performing
/// periodic HTTP HEAD requests and classifying them based on
/// response time and success rate.
pub struct UrlHealthMonitor {
    config: Mutex<UrlHealthMonitorConfig>,
    monitored: Mutex<HashMap<String, MonitoredUrl>>,
    last_global_check: Mutex<Option<Instant>>,
}

impl UrlHealthMonitor {
    /// Create a new URL health monitor
    pub fn new() -> Self {
        Self {
            config: Mutex::new(UrlHealthMonitorConfig::default()),
            monitored: Mutex::new(HashMap::new()),
            last_global_check: Mutex::new(None),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: UrlHealthMonitorConfig) -> Self {
        Self {
            config: Mutex::new(config),
            monitored: Mutex::new(HashMap::new()),
            last_global_check: Mutex::new(None),
        }
    }

    /// Get current configuration
    pub async fn get_config(&self) -> UrlHealthMonitorConfig {
        self.config.lock().await.clone()
    }

    /// Update configuration
    pub async fn set_config(&self, config: UrlHealthMonitorConfig) {
        *self.config.lock().await = config;
    }

    /// Add a URL to monitoring
    pub async fn monitor_url(&self, url: &str) -> bool {
        let config = self.config.lock().await;
        let mut monitored = self.monitored.lock().await;

        if monitored.len() >= config.max_monitored_urls && !monitored.contains_key(url) {
            return false;
        }

        if !monitored.contains_key(url) {
            monitored.insert(url.to_string(), MonitoredUrl::new(url.to_string()));
        }
        true
    }

    /// Remove a URL from monitoring
    pub async fn unmonitor_url(&self, url: &str) -> bool {
        let mut monitored = self.monitored.lock().await;
        monitored.remove(url).is_some()
    }

    /// Get health status for a specific URL
    pub async fn get_url_health(&self, url: &str) -> Option<UrlHealthCheck> {
        let monitored = self.monitored.lock().await;
        monitored.get(url).map(|m| m.to_health_check())
    }

    /// Get health status for multiple URLs
    pub async fn get_health_for_urls(&self, urls: &[&str]) -> Vec<UrlHealthCheck> {
        let monitored = self.monitored.lock().await;
        urls.iter()
            .filter_map(|url| monitored.get(*url).map(|m| m.to_health_check()))
            .collect()
    }

    /// Record a successful health check
    pub async fn record_success(&self, url: &str, response_time_ms: u64, http_status: u16) {
        let config = self.config.lock().await;
        let mut monitored = self.monitored.lock().await;

        if let Some(entry) = monitored.get_mut(url) {
            let now_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            entry.last_check = Some(Instant::now());
            entry.last_check_ts = now_ts;
            entry.response_time_ms = Some(response_time_ms);
            entry.http_status = Some(http_status);
            entry.total_checks += 1;
            entry.successful_checks += 1;
            entry.consecutive_successes += 1;
            entry.consecutive_failures = 0;
            entry.last_error = None;

            // Update status based on response time
            if entry.status == UrlHealthStatus::Dead {
                // Need recovery threshold to come back from dead
                if entry.consecutive_successes >= config.recovery_threshold {
                    entry.status = if response_time_ms > config.degraded_threshold_ms {
                        UrlHealthStatus::Degraded
                    } else {
                        UrlHealthStatus::Healthy
                    };
                }
            } else {
                entry.status = if response_time_ms > config.degraded_threshold_ms {
                    UrlHealthStatus::Degraded
                } else {
                    UrlHealthStatus::Healthy
                };
            }
        }
    }

    /// Record a failed health check
    pub async fn record_failure(&self, url: &str, error: &str) {
        let config = self.config.lock().await;
        let mut monitored = self.monitored.lock().await;

        if let Some(entry) = monitored.get_mut(url) {
            let now_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            entry.last_check = Some(Instant::now());
            entry.last_check_ts = now_ts;
            entry.response_time_ms = None;
            entry.total_checks += 1;
            entry.consecutive_failures += 1;
            entry.consecutive_successes = 0;
            entry.last_error = Some(error.to_string());

            // Mark as dead if threshold exceeded
            if entry.consecutive_failures >= config.dead_threshold {
                entry.status = UrlHealthStatus::Dead;
            } else if entry.status != UrlHealthStatus::Unknown {
                entry.status = UrlHealthStatus::Degraded;
            }
        }
    }

    /// Get summary of all monitored URLs
    pub async fn get_summary(&self) -> UrlHealthSummary {
        let monitored = self.monitored.lock().await;
        let last_global_check = self.last_global_check.lock().await;

        let total = monitored.len();
        let mut healthy = 0;
        let mut degraded = 0;
        let mut dead = 0;
        let mut unknown = 0;
        let mut total_response_time: u64 = 0;
        let mut healthy_with_response = 0;
        let mut recently_checked = 0;

        let config = self.config.lock().await;
        let recent_threshold = Duration::from_secs(config.check_interval_secs);

        for entry in monitored.values() {
            match entry.status {
                UrlHealthStatus::Healthy => healthy += 1,
                UrlHealthStatus::Degraded => degraded += 1,
                UrlHealthStatus::Dead => dead += 1,
                UrlHealthStatus::Unknown => unknown += 1,
            }

            if let Some(rt) = entry.response_time_ms {
                if entry.status == UrlHealthStatus::Healthy {
                    total_response_time += rt;
                    healthy_with_response += 1;
                }
            }

            if let Some(last) = entry.last_check {
                if last.elapsed() < recent_threshold {
                    recently_checked += 1;
                }
            }
        }

        UrlHealthSummary {
            total_monitored: total,
            healthy_count: healthy,
            degraded_count: degraded,
            dead_count: dead,
            unknown_count: unknown,
            avg_response_time_ms: if healthy_with_response > 0 {
                Some(total_response_time / healthy_with_response)
            } else {
                None
            },
            recently_checked,
            last_check_ts: last_global_check.as_ref().map(|i| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let elapsed_secs = i.elapsed().as_secs();
                now.saturating_sub(elapsed_secs)
            }),
        }
    }

    /// Get all monitored URLs sorted by health status (dead first, then degraded, then healthy)
    pub async fn get_all_health_checks(&self) -> Vec<UrlHealthCheck> {
        let monitored = self.monitored.lock().await;
        let mut checks: Vec<UrlHealthCheck> =
            monitored.values().map(|m| m.to_health_check()).collect();

        // Sort by status priority: Dead > Degraded > Unknown > Healthy
        checks.sort_by(|a, b| {
            let priority = |s: UrlHealthStatus| match s {
                UrlHealthStatus::Dead => 0,
                UrlHealthStatus::Degraded => 1,
                UrlHealthStatus::Unknown => 2,
                UrlHealthStatus::Healthy => 3,
            };
            priority(a.status).cmp(&priority(b.status))
        });

        checks
    }

    /// Get URLs that are considered dead
    pub async fn get_dead_urls(&self) -> Vec<String> {
        let monitored = self.monitored.lock().await;
        monitored
            .values()
            .filter(|m| m.status == UrlHealthStatus::Dead)
            .map(|m| m.url.clone())
            .collect()
    }

    /// Get URLs that are considered healthy
    pub async fn get_healthy_urls(&self) -> Vec<String> {
        let monitored = self.monitored.lock().await;
        monitored
            .values()
            .filter(|m| m.status == UrlHealthStatus::Healthy)
            .map(|m| m.url.clone())
            .collect()
    }

    /// Get the best (fastest healthy) URL from a list
    pub async fn get_best_url(&self, urls: &[&str]) -> Option<String> {
        let monitored = self.monitored.lock().await;

        urls.iter()
            .filter_map(|url| {
                monitored.get(*url).and_then(|m| {
                    if m.status == UrlHealthStatus::Healthy {
                        m.response_time_ms.map(|rt| (url.to_string(), rt))
                    } else {
                        None
                    }
                })
            })
            .min_by_key(|(_, rt)| *rt)
            .map(|(url, _)| url)
    }

    /// Clear all monitoring data
    pub async fn clear_all(&self) {
        let mut monitored = self.monitored.lock().await;
        monitored.clear();
        let mut last_check = self.last_global_check.lock().await;
        *last_check = None;
    }

    /// Remove dead URLs from monitoring
    pub async fn cleanup_dead_urls(&self) -> usize {
        let mut monitored = self.monitored.lock().await;
        let before = monitored.len();
        monitored.retain(|_, m| m.status != UrlHealthStatus::Dead);
        before - monitored.len()
    }

    /// Check if enough time has passed for a new check cycle
    pub async fn should_check_now(&self) -> bool {
        let config = self.config.lock().await;
        if !config.enabled {
            return false;
        }

        let last_check = self.last_global_check.lock().await;
        match *last_check {
            None => true,
            Some(instant) => instant.elapsed() >= Duration::from_secs(config.check_interval_secs),
        }
    }

    /// Mark that a check cycle has been performed
    pub async fn mark_check_performed(&self) {
        let mut last_check = self.last_global_check.lock().await;
        *last_check = Some(Instant::now());
    }

    /// Get URLs that need checking (haven't been checked recently)
    pub async fn get_urls_needing_check(&self, max_count: usize) -> Vec<String> {
        let config = self.config.lock().await;
        let monitored = self.monitored.lock().await;
        let interval = Duration::from_secs(config.check_interval_secs);

        let mut needs_check: Vec<(String, Option<Instant>)> = monitored
            .values()
            .filter(|m| {
                m.status != UrlHealthStatus::Dead
                    || m.last_check.is_none()
                    || m.last_check.unwrap().elapsed() > interval * 2
            })
            .map(|m| (m.url.clone(), m.last_check))
            .collect();

        // Sort by last check time (oldest first)
        needs_check.sort_by(|a, b| {
            let a_time = a.1.unwrap_or(Instant::now() - Duration::from_secs(86400));
            let b_time = b.1.unwrap_or(Instant::now() - Duration::from_secs(86400));
            a_time.cmp(&b_time)
        });

        needs_check
            .into_iter()
            .take(max_count)
            .map(|(url, _)| url)
            .collect()
    }

    /// Perform a health check on a URL (HTTP HEAD request)
    pub async fn check_url_health(&self, url: &str) -> Result<(u64, u16), String> {
        let config = self.config.lock().await;
        let timeout = Duration::from_secs(config.timeout_secs);
        let user_agent = config.user_agent.clone();
        drop(config);

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(&user_agent)
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

        let start = Instant::now();
        let response = client
            .head(url)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let elapsed = start.elapsed().as_millis() as u64;
        let status = response.status().as_u16();

        if status >= 200 && status < 400 {
            Ok((elapsed, status))
        } else {
            Err(format!("HTTP error: {}", status))
        }
    }
}

impl Default for UrlHealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_health_status_display() {
        assert_eq!(UrlHealthStatus::Healthy.to_string(), "Healthy");
        assert_eq!(UrlHealthStatus::Degraded.to_string(), "Degraded");
        assert_eq!(UrlHealthStatus::Dead.to_string(), "Dead");
        assert_eq!(UrlHealthStatus::Unknown.to_string(), "Unknown");
    }

    #[test]
    fn test_default_config() {
        let config = UrlHealthMonitorConfig::default();
        assert!(config.enabled);
        assert_eq!(config.check_interval_secs, 300);
        assert_eq!(config.timeout_secs, 10);
        assert_eq!(config.degraded_threshold_ms, 2000);
        assert_eq!(config.dead_threshold, 3);
        assert_eq!(config.recovery_threshold, 2);
        assert_eq!(config.max_monitored_urls, 500);
    }

    #[tokio::test]
    async fn test_monitor_url() {
        let monitor = UrlHealthMonitor::new();
        assert!(monitor.monitor_url("https://example.com/file1.zip").await);
        assert!(monitor.monitor_url("https://example.com/file2.zip").await);

        // Duplicate should still return true
        assert!(monitor.monitor_url("https://example.com/file1.zip").await);
    }

    #[tokio::test]
    async fn test_unmonitor_url() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        assert!(monitor.unmonitor_url("https://example.com/file.zip").await);
        assert!(
            !monitor
                .unmonitor_url("https://example.com/nonexistent.zip")
                .await
        );
    }

    #[tokio::test]
    async fn test_max_monitored_urls() {
        let config = UrlHealthMonitorConfig {
            max_monitored_urls: 2,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);

        assert!(monitor.monitor_url("https://example.com/1.zip").await);
        assert!(monitor.monitor_url("https://example.com/2.zip").await);
        assert!(!monitor.monitor_url("https://example.com/3.zip").await);

        // Re-monitoring existing URL should work
        assert!(monitor.monitor_url("https://example.com/1.zip").await);
    }

    #[tokio::test]
    async fn test_record_success() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        monitor
            .record_success("https://example.com/file.zip", 500, 200)
            .await;

        let health = monitor.get_url_health("https://example.com/file.zip").await;
        assert!(health.is_some());
        let health = health.unwrap();
        assert_eq!(health.status, UrlHealthStatus::Healthy);
        assert_eq!(health.response_time_ms, Some(500));
        assert_eq!(health.http_status, Some(200));
        assert_eq!(health.total_checks, 1);
        assert_eq!(health.successful_checks, 1);
        assert_eq!(health.consecutive_successes, 1);
        assert_eq!(health.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn test_record_success_degraded() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        // Response time above degraded threshold
        monitor
            .record_success("https://example.com/file.zip", 3000, 200)
            .await;

        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Degraded);
        assert_eq!(health.response_time_ms, Some(3000));
    }

    #[tokio::test]
    async fn test_record_failure() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        monitor
            .record_failure("https://example.com/file.zip", "Connection timeout")
            .await;

        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Degraded);
        assert_eq!(health.consecutive_failures, 1);
        assert_eq!(health.consecutive_successes, 0);
        assert_eq!(health.last_error, Some("Connection timeout".to_string()));
    }

    #[tokio::test]
    async fn test_mark_dead_after_threshold() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        // First failure - degraded
        monitor
            .record_failure("https://example.com/file.zip", "Error 1")
            .await;
        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Degraded);

        // Second failure - still degraded
        monitor
            .record_failure("https://example.com/file.zip", "Error 2")
            .await;
        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Degraded);

        // Third failure - dead (threshold = 3)
        monitor
            .record_failure("https://example.com/file.zip", "Error 3")
            .await;
        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Dead);
    }

    #[tokio::test]
    async fn test_recovery_from_dead() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        // Mark as dead
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/file.zip", &format!("Error {}", i))
                .await;
        }
        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Dead);

        // First success - still dead (need recovery_threshold = 2)
        monitor
            .record_success("https://example.com/file.zip", 500, 200)
            .await;
        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Dead);

        // Second success - recovered to healthy
        monitor
            .record_success("https://example.com/file.zip", 500, 200)
            .await;
        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_get_summary() {
        let monitor = UrlHealthMonitor::new();

        monitor.monitor_url("https://example.com/healthy.zip").await;
        monitor
            .monitor_url("https://example.com/degraded.zip")
            .await;
        monitor.monitor_url("https://example.com/dead.zip").await;
        monitor.monitor_url("https://example.com/unknown.zip").await;

        monitor
            .record_success("https://example.com/healthy.zip", 500, 200)
            .await;
        monitor
            .record_success("https://example.com/degraded.zip", 3000, 200)
            .await;
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/dead.zip", &format!("Error {}", i))
                .await;
        }

        let summary = monitor.get_summary().await;
        assert_eq!(summary.total_monitored, 4);
        assert_eq!(summary.healthy_count, 1);
        assert_eq!(summary.degraded_count, 1);
        assert_eq!(summary.dead_count, 1);
        assert_eq!(summary.unknown_count, 1);
        assert_eq!(summary.avg_response_time_ms, Some(500));
    }

    #[tokio::test]
    async fn test_get_all_health_checks_sorted() {
        let monitor = UrlHealthMonitor::new();

        monitor.monitor_url("https://example.com/healthy.zip").await;
        monitor.monitor_url("https://example.com/dead.zip").await;
        monitor
            .monitor_url("https://example.com/degraded.zip")
            .await;

        monitor
            .record_success("https://example.com/healthy.zip", 500, 200)
            .await;
        monitor
            .record_success("https://example.com/degraded.zip", 3000, 200)
            .await;
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/dead.zip", &format!("Error {}", i))
                .await;
        }

        let checks = monitor.get_all_health_checks().await;
        assert_eq!(checks.len(), 3);
        assert_eq!(checks[0].status, UrlHealthStatus::Dead);
        assert_eq!(checks[1].status, UrlHealthStatus::Degraded);
        assert_eq!(checks[2].status, UrlHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_get_dead_urls() {
        let monitor = UrlHealthMonitor::new();

        monitor.monitor_url("https://example.com/1.zip").await;
        monitor.monitor_url("https://example.com/2.zip").await;

        for i in 0..3 {
            monitor
                .record_failure("https://example.com/1.zip", &format!("Error {}", i))
                .await;
        }

        let dead = monitor.get_dead_urls().await;
        assert_eq!(dead.len(), 1);
        assert!(dead.contains(&"https://example.com/1.zip".to_string()));
    }

    #[tokio::test]
    async fn test_get_healthy_urls() {
        let monitor = UrlHealthMonitor::new();

        monitor.monitor_url("https://example.com/1.zip").await;
        monitor.monitor_url("https://example.com/2.zip").await;

        monitor
            .record_success("https://example.com/1.zip", 500, 200)
            .await;

        let healthy = monitor.get_healthy_urls().await;
        assert_eq!(healthy.len(), 1);
        assert!(healthy.contains(&"https://example.com/1.zip".to_string()));
    }

    #[tokio::test]
    async fn test_get_best_url() {
        let monitor = UrlHealthMonitor::new();

        monitor.monitor_url("https://example.com/slow.zip").await;
        monitor.monitor_url("https://example.com/fast.zip").await;
        monitor.monitor_url("https://example.com/dead.zip").await;

        monitor
            .record_success("https://example.com/slow.zip", 2000, 200)
            .await;
        monitor
            .record_success("https://example.com/fast.zip", 200, 200)
            .await;
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/dead.zip", &format!("Error {}", i))
                .await;
        }

        let best = monitor
            .get_best_url(&[
                "https://example.com/slow.zip",
                "https://example.com/fast.zip",
                "https://example.com/dead.zip",
            ])
            .await;

        assert_eq!(best, Some("https://example.com/fast.zip".to_string()));
    }

    #[tokio::test]
    async fn test_clear_all() {
        let monitor = UrlHealthMonitor::new();

        monitor.monitor_url("https://example.com/1.zip").await;
        monitor.monitor_url("https://example.com/2.zip").await;

        let summary = monitor.get_summary().await;
        assert_eq!(summary.total_monitored, 2);

        monitor.clear_all().await;

        let summary = monitor.get_summary().await;
        assert_eq!(summary.total_monitored, 0);
    }

    #[tokio::test]
    async fn test_cleanup_dead_urls() {
        let monitor = UrlHealthMonitor::new();

        monitor.monitor_url("https://example.com/alive.zip").await;
        monitor.monitor_url("https://example.com/dead.zip").await;

        for i in 0..3 {
            monitor
                .record_failure("https://example.com/dead.zip", &format!("Error {}", i))
                .await;
        }

        let removed = monitor.cleanup_dead_urls().await;
        assert_eq!(removed, 1);

        let summary = monitor.get_summary().await;
        assert_eq!(summary.total_monitored, 1);
    }

    #[tokio::test]
    async fn test_should_check_now() {
        let monitor = UrlHealthMonitor::new();

        // Initially should check
        assert!(monitor.should_check_now().await);

        // Mark check performed
        monitor.mark_check_performed().await;

        // Should not check immediately after
        assert!(!monitor.should_check_now().await);
    }

    #[tokio::test]
    async fn test_disabled_monitor() {
        let config = UrlHealthMonitorConfig {
            enabled: false,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);

        assert!(!monitor.should_check_now().await);
    }

    #[tokio::test]
    async fn test_get_health_for_urls() {
        let monitor = UrlHealthMonitor::new();

        monitor.monitor_url("https://example.com/1.zip").await;
        monitor.monitor_url("https://example.com/2.zip").await;

        monitor
            .record_success("https://example.com/1.zip", 500, 200)
            .await;

        let checks = monitor
            .get_health_for_urls(&[
                "https://example.com/1.zip",
                "https://example.com/2.zip",
                "https://example.com/nonexistent.zip",
            ])
            .await;

        assert_eq!(checks.len(), 2);
    }

    #[tokio::test]
    async fn test_success_resets_failure_count() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        // 2 failures
        monitor
            .record_failure("https://example.com/file.zip", "Error 1")
            .await;
        monitor
            .record_failure("https://example.com/file.zip", "Error 2")
            .await;

        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.consecutive_failures, 2);

        // Success resets failure count
        monitor
            .record_success("https://example.com/file.zip", 500, 200)
            .await;

        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.consecutive_successes, 1);
    }

    #[tokio::test]
    async fn test_serialization() {
        let config = UrlHealthMonitorConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: UrlHealthMonitorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.check_interval_secs, config.check_interval_secs);

        let check = UrlHealthCheck {
            url: "https://example.com/file.zip".to_string(),
            status: UrlHealthStatus::Healthy,
            last_check_ts: 1234567890,
            response_time_ms: Some(500),
            http_status: Some(200),
            consecutive_failures: 0,
            consecutive_successes: 5,
            total_checks: 10,
            successful_checks: 9,
            last_error: None,
        };
        let json = serde_json::to_string(&check).unwrap();
        let deserialized: UrlHealthCheck = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.url, check.url);
        assert_eq!(deserialized.status, check.status);
    }
}
