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

            if let Some(rt) = entry.response_time_ms
                && entry.status == UrlHealthStatus::Healthy
            {
                total_response_time += rt;
                healthy_with_response += 1;
            }

            if let Some(last) = entry.last_check
                && last.elapsed() < recent_threshold
            {
                recently_checked += 1;
            }
        }

        UrlHealthSummary {
            total_monitored: total,
            healthy_count: healthy,
            degraded_count: degraded,
            dead_count: dead,
            unknown_count: unknown,
            avg_response_time_ms: if healthy_with_response > 0 {
                Some(
                    total_response_time
                        .checked_div(healthy_with_response)
                        .unwrap_or(0),
                )
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

        if (200..400).contains(&status) {
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
        // Unknown status stays Unknown after first failure (doesn't transition to Degraded)
        assert_eq!(health.status, UrlHealthStatus::Unknown);
        assert_eq!(health.consecutive_failures, 1);
        assert_eq!(health.consecutive_successes, 0);
        assert_eq!(health.last_error, Some("Connection timeout".to_string()));
    }

    #[tokio::test]
    async fn test_mark_dead_after_threshold() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        // First failure - Unknown stays Unknown (special case)
        monitor
            .record_failure("https://example.com/file.zip", "Error 1")
            .await;
        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Unknown);

        // Second failure - still Unknown
        monitor
            .record_failure("https://example.com/file.zip", "Error 2")
            .await;
        let health = monitor
            .get_url_health("https://example.com/file.zip")
            .await
            .unwrap();
        assert_eq!(health.status, UrlHealthStatus::Unknown);

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

    // ===== Serialization roundtrip =====

    #[test]
    fn test_url_health_status_serde_roundtrip() {
        let statuses = [
            UrlHealthStatus::Healthy,
            UrlHealthStatus::Degraded,
            UrlHealthStatus::Dead,
            UrlHealthStatus::Unknown,
        ];
        for s in &statuses {
            let json = serde_json::to_string(s).unwrap();
            let back: UrlHealthStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(*s, back);
        }
    }

    #[test]
    fn test_url_health_check_serde_roundtrip() {
        let check = UrlHealthCheck {
            url: "https://mirror.example.com/big.bin".to_string(),
            status: UrlHealthStatus::Degraded,
            last_check_ts: 1_700_000_000,
            response_time_ms: Some(4500),
            http_status: Some(200),
            consecutive_failures: 0,
            consecutive_successes: 3,
            total_checks: 20,
            successful_checks: 18,
            last_error: None,
        };
        let json = serde_json::to_string(&check).unwrap();
        let back: UrlHealthCheck = serde_json::from_str(&json).unwrap();
        assert_eq!(back.url, check.url);
        assert_eq!(back.status, check.status);
        assert_eq!(back.last_check_ts, check.last_check_ts);
        assert_eq!(back.response_time_ms, check.response_time_ms);
        assert_eq!(back.http_status, check.http_status);
        assert_eq!(back.consecutive_failures, check.consecutive_failures);
        assert_eq!(back.consecutive_successes, check.consecutive_successes);
        assert_eq!(back.total_checks, check.total_checks);
        assert_eq!(back.successful_checks, check.successful_checks);
    }

    #[test]
    fn test_url_health_check_serde_with_error() {
        let check = UrlHealthCheck {
            url: "https://broken.example.com/x".to_string(),
            status: UrlHealthStatus::Dead,
            last_check_ts: 0,
            response_time_ms: None,
            http_status: None,
            consecutive_failures: 10,
            consecutive_successes: 0,
            total_checks: 10,
            successful_checks: 0,
            last_error: Some("Connection refused".to_string()),
        };
        let json = serde_json::to_string(&check).unwrap();
        let back: UrlHealthCheck = serde_json::from_str(&json).unwrap();
        assert_eq!(back.last_error.as_deref(), Some("Connection refused"));
        assert_eq!(back.response_time_ms, None);
    }

    #[test]
    fn test_url_health_monitor_config_serde_roundtrip() {
        let config = UrlHealthMonitorConfig {
            enabled: false,
            check_interval_secs: 60,
            timeout_secs: 5,
            degraded_threshold_ms: 1000,
            dead_threshold: 5,
            recovery_threshold: 3,
            max_monitored_urls: 100,
            user_agent: "TestAgent/1.0".to_string(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: UrlHealthMonitorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.enabled, config.enabled);
        assert_eq!(back.check_interval_secs, config.check_interval_secs);
        assert_eq!(back.timeout_secs, config.timeout_secs);
        assert_eq!(back.degraded_threshold_ms, config.degraded_threshold_ms);
        assert_eq!(back.dead_threshold, config.dead_threshold);
        assert_eq!(back.recovery_threshold, config.recovery_threshold);
        assert_eq!(back.max_monitored_urls, config.max_monitored_urls);
        assert_eq!(back.user_agent, config.user_agent);
    }

    #[test]
    fn test_url_health_summary_serde_roundtrip() {
        let summary = UrlHealthSummary {
            total_monitored: 10,
            healthy_count: 5,
            degraded_count: 2,
            dead_count: 1,
            unknown_count: 2,
            avg_response_time_ms: Some(350),
            recently_checked: 8,
            last_check_ts: Some(1_700_000_000),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: UrlHealthSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_monitored, summary.total_monitored);
        assert_eq!(back.healthy_count, summary.healthy_count);
        assert_eq!(back.avg_response_time_ms, summary.avg_response_time_ms);
    }

    #[test]
    fn test_url_health_summary_serde_null_avg() {
        let summary = UrlHealthSummary {
            total_monitored: 0,
            healthy_count: 0,
            degraded_count: 0,
            dead_count: 0,
            unknown_count: 0,
            avg_response_time_ms: None,
            recently_checked: 0,
            last_check_ts: None,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: UrlHealthSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.avg_response_time_ms, None);
        assert_eq!(back.last_check_ts, None);
    }

    #[test]
    fn test_config_json_structure() {
        let config = UrlHealthMonitorConfig::default();
        let val: serde_json::Value = serde_json::to_value(&config).unwrap();
        assert_eq!(val["enabled"], true);
        assert_eq!(val["check_interval_secs"], 300);
        assert_eq!(val["timeout_secs"], 10);
        assert_eq!(val["degraded_threshold_ms"], 2000);
        assert_eq!(val["dead_threshold"], 3);
        assert_eq!(val["recovery_threshold"], 2);
        assert_eq!(val["max_monitored_urls"], 500);
        assert!(val["user_agent"].as_str().unwrap().contains("IPMsg"));
    }

    // ===== Default values =====

    #[test]
    fn test_default_config_values() {
        let config = UrlHealthMonitorConfig::default();
        assert!(config.enabled);
        assert_eq!(config.check_interval_secs, 300);
        assert_eq!(config.timeout_secs, 10);
        assert_eq!(config.degraded_threshold_ms, 2000);
        assert_eq!(config.dead_threshold, 3);
        assert_eq!(config.recovery_threshold, 2);
        assert_eq!(config.max_monitored_urls, 500);
        assert!(config.user_agent.contains("IPMsg-Torrent"));
    }

    #[tokio::test]
    async fn test_default_monitor() {
        let monitor = UrlHealthMonitor::default();
        let new_monitor = UrlHealthMonitor::new();
        // Verify default monitor works the same as new()
        let default_config = monitor.get_config().await;
        let new_config = new_monitor.get_config().await;
        assert_eq!(default_config.enabled, new_config.enabled);
        assert_eq!(
            default_config.check_interval_secs,
            new_config.check_interval_secs
        );
        assert_eq!(default_config.timeout_secs, new_config.timeout_secs);
    }

    // ===== Config management =====

    #[tokio::test]
    async fn test_with_config() {
        let config = UrlHealthMonitorConfig {
            check_interval_secs: 60,
            dead_threshold: 5,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);
        let loaded = monitor.get_config().await;
        assert_eq!(loaded.check_interval_secs, 60);
        assert_eq!(loaded.dead_threshold, 5);
    }

    #[tokio::test]
    async fn test_set_config() {
        let monitor = UrlHealthMonitor::new();
        let mut config = monitor.get_config().await;
        config.timeout_secs = 30;
        config.degraded_threshold_ms = 5000;
        monitor.set_config(config).await;

        let loaded = monitor.get_config().await;
        assert_eq!(loaded.timeout_secs, 30);
        assert_eq!(loaded.degraded_threshold_ms, 5000);
    }

    #[tokio::test]
    async fn test_set_config_preserves_monitored() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/file.zip").await;

        let mut config = monitor.get_config().await;
        config.check_interval_secs = 120;
        monitor.set_config(config).await;

        // Monitored URLs should not be affected by config change
        let health = monitor.get_url_health("https://example.com/file.zip").await;
        assert!(health.is_some());
    }

    // ===== URL monitoring edge cases =====

    #[tokio::test]
    async fn test_monitor_url_empty_string() {
        let monitor = UrlHealthMonitor::new();
        assert!(monitor.monitor_url("").await);
        let health = monitor.get_url_health("").await;
        assert!(health.is_some());
        assert_eq!(health.unwrap().status, UrlHealthStatus::Unknown);
    }

    #[tokio::test]
    async fn test_monitor_url_max_zero() {
        let config = UrlHealthMonitorConfig {
            max_monitored_urls: 0,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);
        // Cannot add any URL when max is 0
        assert!(!monitor.monitor_url("https://example.com/f.zip").await);
    }

    #[tokio::test]
    async fn test_monitor_url_max_one() {
        let config = UrlHealthMonitorConfig {
            max_monitored_urls: 1,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);
        assert!(monitor.monitor_url("https://example.com/1.zip").await);
        assert!(!monitor.monitor_url("https://example.com/2.zip").await);
        // Re-adding existing URL still works
        assert!(monitor.monitor_url("https://example.com/1.zip").await);
    }

    #[tokio::test]
    async fn test_unmonitor_nonexistent() {
        let monitor = UrlHealthMonitor::new();
        assert!(!monitor.unmonitor_url("https://example.com/ghost.zip").await);
    }

    #[tokio::test]
    async fn test_get_url_health_nonexistent() {
        let monitor = UrlHealthMonitor::new();
        assert!(
            monitor
                .get_url_health("https://example.com/nope.zip")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_get_url_health_empty_string() {
        let monitor = UrlHealthMonitor::new();
        assert!(monitor.get_url_health("").await.is_none());
        monitor.monitor_url("").await;
        assert!(monitor.get_url_health("").await.is_some());
    }

    // ===== record_success edge cases =====

    #[tokio::test]
    async fn test_record_success_on_unmonitored_url() {
        let monitor = UrlHealthMonitor::new();
        // Recording success on unmonitored URL should be silently ignored
        monitor
            .record_success("https://example.com/ghost.zip", 100, 200)
            .await;
        assert!(
            monitor
                .get_url_health("https://example.com/ghost.zip")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_record_success_zero_response_time() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;
        monitor
            .record_success("https://example.com/f.zip", 0, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.response_time_ms, Some(0));
        assert_eq!(h.status, UrlHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_record_success_exact_degraded_boundary() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        // Exactly at threshold → Healthy (not > threshold)
        monitor
            .record_success("https://example.com/f.zip", 2000, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Healthy);

        // 1ms above threshold → Degraded
        monitor
            .record_success("https://example.com/f.zip", 2001, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Degraded);
    }

    #[tokio::test]
    async fn test_record_success_accumulates_checks() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        for i in 0..10 {
            monitor
                .record_success("https://example.com/f.zip", 100 + i * 10, 200)
                .await;
        }
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.total_checks, 10);
        assert_eq!(h.successful_checks, 10);
        assert_eq!(h.consecutive_successes, 10);
        assert_eq!(h.consecutive_failures, 0);
        // Last recorded response time
        assert_eq!(h.response_time_ms, Some(190));
    }

    #[tokio::test]
    async fn test_record_success_updates_http_status() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        monitor
            .record_success("https://example.com/f.zip", 100, 301)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.http_status, Some(301));

        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.http_status, Some(200));
    }

    #[tokio::test]
    async fn test_record_success_from_degraded_to_healthy() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        // First make it Healthy, then fail to make it Degraded
        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Healthy);

        monitor
            .record_failure("https://example.com/f.zip", "timeout")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Degraded);

        // Success with good response time → Healthy
        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_record_success_from_degraded_to_degraded() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        // Make it degraded via failure
        monitor
            .record_failure("https://example.com/f.zip", "timeout")
            .await;

        // Success with slow response time → still Degraded
        monitor
            .record_success("https://example.com/f.zip", 5000, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Degraded);
    }

    #[tokio::test]
    async fn test_recovery_from_dead_with_slow_response() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        // Kill it
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/f.zip", &format!("err{}", i))
                .await;
        }
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Dead);

        // Recovery with slow response → should become Degraded after threshold
        monitor
            .record_success("https://example.com/f.zip", 5000, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Dead); // still dead, need 2 successes

        monitor
            .record_success("https://example.com/f.zip", 5000, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Degraded); // recovered but slow
    }

    #[tokio::test]
    async fn test_recovery_threshold_custom() {
        let config = UrlHealthMonitorConfig {
            recovery_threshold: 4,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);
        monitor.monitor_url("https://example.com/f.zip").await;

        // Kill it
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/f.zip", &format!("err{}", i))
                .await;
        }

        // Need 4 successes to recover
        for i in 0..3 {
            monitor
                .record_success("https://example.com/f.zip", 100, 200)
                .await;
            let h = monitor
                .get_url_health("https://example.com/f.zip")
                .await
                .unwrap();
            assert_eq!(
                h.status,
                UrlHealthStatus::Dead,
                "still dead after {} successes",
                i + 1
            );
        }

        // 4th success → recovered
        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Healthy);
    }

    // ===== record_failure edge cases =====

    #[tokio::test]
    async fn test_record_failure_on_unmonitored_url() {
        let monitor = UrlHealthMonitor::new();
        monitor
            .record_failure("https://example.com/ghost.zip", "err")
            .await;
        assert!(
            monitor
                .get_url_health("https://example.com/ghost.zip")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_record_failure_unknown_stays_unknown_or_degraded() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        // First failure on Unknown status: the code says "if status != Unknown, mark Degraded"
        // So with 1 failure and dead_threshold=3, status stays Unknown (not yet dead, and the else branch skips Unknown)
        monitor
            .record_failure("https://example.com/f.zip", "err1")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        // Actually the code: if consecutive_failures >= dead_threshold → Dead; else if status != Unknown → Degraded
        // So Unknown stays Unknown after first failure since the else-if skips Unknown
        assert_eq!(h.status, UrlHealthStatus::Unknown);
    }

    #[tokio::test]
    async fn test_record_failure_from_healthy_to_degraded() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Healthy);

        monitor
            .record_failure("https://example.com/f.zip", "err")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        // status was Healthy (!= Unknown), so it becomes Degraded
        assert_eq!(h.status, UrlHealthStatus::Degraded);
    }

    #[tokio::test]
    async fn test_record_failure_clears_response_time() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        monitor
            .record_success("https://example.com/f.zip", 500, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.response_time_ms, Some(500));

        monitor
            .record_failure("https://example.com/f.zip", "timeout")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.response_time_ms, None);
    }

    #[tokio::test]
    async fn test_record_failure_resets_successes() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.consecutive_successes, 2);

        monitor
            .record_failure("https://example.com/f.zip", "err")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.consecutive_successes, 0);
        assert_eq!(h.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn test_dead_threshold_one() {
        let config = UrlHealthMonitorConfig {
            dead_threshold: 1,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);
        monitor.monitor_url("https://example.com/f.zip").await;

        // Single failure → Dead
        monitor
            .record_failure("https://example.com/f.zip", "err")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Dead);
    }

    #[tokio::test]
    async fn test_alternating_success_failure() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        // success → Healthy
        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        assert_eq!(
            monitor
                .get_url_health("https://example.com/f.zip")
                .await
                .unwrap()
                .status,
            UrlHealthStatus::Healthy
        );

        // failure → Degraded
        monitor
            .record_failure("https://example.com/f.zip", "err")
            .await;
        assert_eq!(
            monitor
                .get_url_health("https://example.com/f.zip")
                .await
                .unwrap()
                .status,
            UrlHealthStatus::Degraded
        );

        // success → Healthy
        monitor
            .record_success("https://example.com/f.zip", 100, 200)
            .await;
        assert_eq!(
            monitor
                .get_url_health("https://example.com/f.zip")
                .await
                .unwrap()
                .status,
            UrlHealthStatus::Healthy
        );

        // failure → Degraded
        monitor
            .record_failure("https://example.com/f.zip", "err")
            .await;
        assert_eq!(
            monitor
                .get_url_health("https://example.com/f.zip")
                .await
                .unwrap()
                .status,
            UrlHealthStatus::Degraded
        );
    }

    // ===== Summary =====

    #[tokio::test]
    async fn test_summary_empty() {
        let monitor = UrlHealthMonitor::new();
        let s = monitor.get_summary().await;
        assert_eq!(s.total_monitored, 0);
        assert_eq!(s.healthy_count, 0);
        assert_eq!(s.degraded_count, 0);
        assert_eq!(s.dead_count, 0);
        assert_eq!(s.unknown_count, 0);
        assert_eq!(s.avg_response_time_ms, None);
        assert_eq!(s.recently_checked, 0);
    }

    #[tokio::test]
    async fn test_summary_all_healthy_avg() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/a.zip").await;
        monitor.monitor_url("https://example.com/b.zip").await;

        monitor
            .record_success("https://example.com/a.zip", 200, 200)
            .await;
        monitor
            .record_success("https://example.com/b.zip", 400, 200)
            .await;

        let s = monitor.get_summary().await;
        assert_eq!(s.healthy_count, 2);
        assert_eq!(s.avg_response_time_ms, Some(300)); // (200+400)/2
    }

    #[tokio::test]
    async fn test_summary_avg_excludes_degraded() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/healthy.zip").await;
        monitor
            .monitor_url("https://example.com/degraded.zip")
            .await;

        monitor
            .record_success("https://example.com/healthy.zip", 100, 200)
            .await;
        monitor
            .record_success("https://example.com/degraded.zip", 5000, 200)
            .await;

        let s = monitor.get_summary().await;
        // Only healthy URLs contribute to avg
        assert_eq!(s.avg_response_time_ms, Some(100));
    }

    #[tokio::test]
    async fn test_summary_avg_excludes_dead() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/healthy.zip").await;
        monitor.monitor_url("https://example.com/dead.zip").await;

        monitor
            .record_success("https://example.com/healthy.zip", 500, 200)
            .await;
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/dead.zip", &format!("e{}", i))
                .await;
        }

        let s = monitor.get_summary().await;
        assert_eq!(s.avg_response_time_ms, Some(500));
        assert_eq!(s.dead_count, 1);
    }

    #[tokio::test]
    async fn test_summary_all_unknown() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/a.zip").await;
        monitor.monitor_url("https://example.com/b.zip").await;

        let s = monitor.get_summary().await;
        assert_eq!(s.unknown_count, 2);
        assert_eq!(s.avg_response_time_ms, None);
    }

    // ===== get_all_health_checks =====

    #[tokio::test]
    async fn test_get_all_health_checks_empty() {
        let monitor = UrlHealthMonitor::new();
        let checks = monitor.get_all_health_checks().await;
        assert!(checks.is_empty());
    }

    #[tokio::test]
    async fn test_get_all_health_checks_single_unknown() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;
        let checks = monitor.get_all_health_checks().await;
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, UrlHealthStatus::Unknown);
    }

    // ===== get_best_url edge cases =====

    #[tokio::test]
    async fn test_get_best_url_empty_list() {
        let monitor = UrlHealthMonitor::new();
        let best = monitor.get_best_url(&[]).await;
        assert!(best.is_none());
    }

    #[tokio::test]
    async fn test_get_best_url_no_healthy() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/dead.zip").await;
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/dead.zip", &format!("e{}", i))
                .await;
        }
        let best = monitor
            .get_best_url(&["https://example.com/dead.zip"])
            .await;
        assert!(best.is_none());
    }

    #[tokio::test]
    async fn test_get_best_url_tie_breaking() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/a.zip").await;
        monitor.monitor_url("https://example.com/b.zip").await;

        monitor
            .record_success("https://example.com/a.zip", 100, 200)
            .await;
        monitor
            .record_success("https://example.com/b.zip", 100, 200)
            .await;

        // Both have same response time; min_by_key returns first minimum
        let best = monitor
            .get_best_url(&["https://example.com/a.zip", "https://example.com/b.zip"])
            .await;
        assert!(best.is_some());
    }

    #[tokio::test]
    async fn test_get_best_url_ignores_unmonitored() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/real.zip").await;
        monitor
            .record_success("https://example.com/real.zip", 100, 200)
            .await;

        let best = monitor
            .get_best_url(&[
                "https://example.com/real.zip",
                "https://example.com/ghost.zip",
            ])
            .await;
        assert_eq!(best, Some("https://example.com/real.zip".to_string()));
    }

    #[tokio::test]
    async fn test_get_best_url_prefers_healthy_over_degraded() {
        let monitor = UrlHealthMonitor::new();
        monitor
            .monitor_url("https://example.com/fast-degraded.zip")
            .await;
        monitor
            .monitor_url("https://example.com/slow-healthy.zip")
            .await;

        // Fast but degraded (above threshold)
        monitor
            .record_success("https://example.com/fast-degraded.zip", 10, 200)
            .await;
        monitor
            .record_failure("https://example.com/fast-degraded.zip", "err")
            .await;
        // The failure makes it Degraded; response_time_ms is now None

        // Slow but healthy
        monitor
            .record_success("https://example.com/slow-healthy.zip", 1500, 200)
            .await;

        let best = monitor
            .get_best_url(&[
                "https://example.com/fast-degraded.zip",
                "https://example.com/slow-healthy.zip",
            ])
            .await;
        // Only healthy URLs are considered
        assert_eq!(
            best,
            Some("https://example.com/slow-healthy.zip".to_string())
        );
    }

    // ===== clear_all =====

    #[tokio::test]
    async fn test_clear_all_resets_last_check() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;
        monitor.mark_check_performed().await;
        assert!(!monitor.should_check_now().await);

        monitor.clear_all().await;
        // After clear, last_global_check is None → should_check_now returns true
        assert!(monitor.should_check_now().await);
    }

    // ===== cleanup_dead_urls =====

    #[tokio::test]
    async fn test_cleanup_dead_urls_none_dead() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/a.zip").await;
        monitor.monitor_url("https://example.com/b.zip").await;
        monitor
            .record_success("https://example.com/a.zip", 100, 200)
            .await;

        let removed = monitor.cleanup_dead_urls().await;
        assert_eq!(removed, 0);
        assert_eq!(monitor.get_summary().await.total_monitored, 2);
    }

    #[tokio::test]
    async fn test_cleanup_dead_urls_all_dead() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/a.zip").await;
        monitor.monitor_url("https://example.com/b.zip").await;

        for url in &["https://example.com/a.zip", "https://example.com/b.zip"] {
            for i in 0..3 {
                monitor.record_failure(url, &format!("e{}", i)).await;
            }
        }

        let removed = monitor.cleanup_dead_urls().await;
        assert_eq!(removed, 2);
        assert_eq!(monitor.get_summary().await.total_monitored, 0);
    }

    // ===== should_check_now / mark_check_performed =====

    #[tokio::test]
    async fn test_should_check_now_initial() {
        let monitor = UrlHealthMonitor::new();
        assert!(monitor.should_check_now().await);
    }

    #[tokio::test]
    async fn test_should_check_now_after_mark() {
        let monitor = UrlHealthMonitor::new();
        monitor.mark_check_performed().await;
        assert!(!monitor.should_check_now().await);
    }

    #[tokio::test]
    async fn test_should_check_now_disabled() {
        let config = UrlHealthMonitorConfig {
            enabled: false,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);
        assert!(!monitor.should_check_now().await);
    }

    #[tokio::test]
    async fn test_should_check_now_with_short_interval() {
        let config = UrlHealthMonitorConfig {
            check_interval_secs: 0,
            ..Default::default()
        };
        let monitor = UrlHealthMonitor::with_config(config);
        monitor.mark_check_performed().await;
        // With 0-second interval, should immediately need another check
        assert!(monitor.should_check_now().await);
    }

    // ===== get_urls_needing_check =====

    #[tokio::test]
    async fn test_get_urls_needing_check_all_new() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/a.zip").await;
        monitor.monitor_url("https://example.com/b.zip").await;

        let urls = monitor.get_urls_needing_check(10).await;
        assert_eq!(urls.len(), 2);
    }

    #[tokio::test]
    async fn test_get_urls_needing_check_max_count() {
        let monitor = UrlHealthMonitor::new();
        for i in 0..10 {
            monitor
                .monitor_url(&format!("https://example.com/{}.zip", i))
                .await;
        }

        let urls = monitor.get_urls_needing_check(3).await;
        assert_eq!(urls.len(), 3);
    }

    #[tokio::test]
    async fn test_get_urls_needing_check_empty() {
        let monitor = UrlHealthMonitor::new();
        let urls = monitor.get_urls_needing_check(10).await;
        assert!(urls.is_empty());
    }

    // ===== MonitoredUrl::to_health_check =====

    #[test]
    fn test_monitored_url_to_health_check() {
        let m = MonitoredUrl::new("https://example.com/test.zip".to_string());
        let hc = m.to_health_check();
        assert_eq!(hc.url, "https://example.com/test.zip");
        assert_eq!(hc.status, UrlHealthStatus::Unknown);
        assert_eq!(hc.last_check_ts, 0);
        assert_eq!(hc.response_time_ms, None);
        assert_eq!(hc.http_status, None);
        assert_eq!(hc.consecutive_failures, 0);
        assert_eq!(hc.consecutive_successes, 0);
        assert_eq!(hc.total_checks, 0);
        assert_eq!(hc.successful_checks, 0);
        assert_eq!(hc.last_error, None);
    }

    // ===== Display impl =====

    #[test]
    fn test_url_health_status_display_all_variants() {
        assert_eq!(format!("{}", UrlHealthStatus::Healthy), "Healthy");
        assert_eq!(format!("{}", UrlHealthStatus::Degraded), "Degraded");
        assert_eq!(format!("{}", UrlHealthStatus::Dead), "Dead");
        assert_eq!(format!("{}", UrlHealthStatus::Unknown), "Unknown");
    }

    // ===== Clone/Debug traits =====

    #[test]
    fn test_url_health_status_clone_debug() {
        let s = UrlHealthStatus::Healthy;
        let cloned = s;
        assert_eq!(cloned, UrlHealthStatus::Healthy);
        let debug = format!("{:?}", s);
        assert!(debug.contains("Healthy"));
    }

    #[test]
    fn test_url_health_check_clone_debug() {
        let check = UrlHealthCheck {
            url: "https://example.com/f.zip".to_string(),
            status: UrlHealthStatus::Healthy,
            last_check_ts: 100,
            response_time_ms: Some(50),
            http_status: Some(200),
            consecutive_failures: 0,
            consecutive_successes: 1,
            total_checks: 1,
            successful_checks: 1,
            last_error: None,
        };
        let cloned = check.clone();
        assert_eq!(cloned.url, check.url);
        let debug = format!("{:?}", check);
        assert!(debug.contains("Healthy"));
    }

    #[test]
    fn test_url_health_monitor_config_clone_debug() {
        let config = UrlHealthMonitorConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        let debug = format!("{:?}", config);
        assert!(debug.contains("UrlHealthMonitorConfig"));
    }

    #[test]
    fn test_url_health_summary_clone_debug() {
        let summary = UrlHealthSummary {
            total_monitored: 5,
            healthy_count: 3,
            degraded_count: 1,
            dead_count: 1,
            unknown_count: 0,
            avg_response_time_ms: Some(200),
            recently_checked: 4,
            last_check_ts: Some(100),
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_monitored, summary.total_monitored);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("UrlHealthSummary"));
    }

    // ===== Complex scenarios =====

    #[tokio::test]
    async fn test_full_lifecycle() {
        let monitor = UrlHealthMonitor::new();

        // 1. Add URLs
        monitor.monitor_url("https://example.com/a.zip").await;
        monitor.monitor_url("https://example.com/b.zip").await;
        monitor.monitor_url("https://example.com/c.zip").await;

        // 2. Initial state: all Unknown
        let s = monitor.get_summary().await;
        assert_eq!(s.unknown_count, 3);

        // 3. Check URLs
        monitor
            .record_success("https://example.com/a.zip", 100, 200)
            .await;
        monitor
            .record_success("https://example.com/b.zip", 3000, 200)
            .await;
        for i in 0..3 {
            monitor
                .record_failure("https://example.com/c.zip", &format!("err{}", i))
                .await;
        }

        // 4. Verify states
        let s = monitor.get_summary().await;
        assert_eq!(s.healthy_count, 1);
        assert_eq!(s.degraded_count, 1);
        assert_eq!(s.dead_count, 1);

        // 5. Cleanup dead
        let removed = monitor.cleanup_dead_urls().await;
        assert_eq!(removed, 1);

        // 6. Recover degraded
        monitor
            .record_success("https://example.com/b.zip", 100, 200)
            .await;
        let h = monitor
            .get_url_health("https://example.com/b.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Healthy);

        // 7. Final summary
        let s = monitor.get_summary().await;
        assert_eq!(s.total_monitored, 2);
        assert_eq!(s.healthy_count, 2);
    }

    #[tokio::test]
    async fn test_many_urls_monitoring() {
        let monitor = UrlHealthMonitor::new();
        let count = 100;

        for i in 0..count {
            monitor
                .monitor_url(&format!("https://mirror{}.example.com/file.zip", i))
                .await;
        }

        let s = monitor.get_summary().await;
        assert_eq!(s.total_monitored, count);

        // Make half healthy, half dead
        for i in 0..50 {
            monitor
                .record_success(
                    &format!("https://mirror{}.example.com/file.zip", i),
                    100,
                    200,
                )
                .await;
        }
        for i in 50..count {
            for j in 0..3 {
                monitor
                    .record_failure(
                        &format!("https://mirror{}.example.com/file.zip", i),
                        &format!("err{}", j),
                    )
                    .await;
            }
        }

        let s = monitor.get_summary().await;
        assert_eq!(s.healthy_count, 50);
        assert_eq!(s.dead_count, 50);

        let dead = monitor.get_dead_urls().await;
        assert_eq!(dead.len(), 50);

        let cleaned = monitor.cleanup_dead_urls().await;
        assert_eq!(cleaned, 50);
        assert_eq!(monitor.get_summary().await.total_monitored, 50);
    }

    #[tokio::test]
    async fn test_config_change_affects_behavior() {
        let monitor = UrlHealthMonitor::new();
        monitor.monitor_url("https://example.com/f.zip").await;

        // Default dead_threshold = 3
        monitor
            .record_failure("https://example.com/f.zip", "e1")
            .await;
        monitor
            .record_failure("https://example.com/f.zip", "e2")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_ne!(h.status, UrlHealthStatus::Dead);

        // Change dead_threshold to 2
        let mut config = monitor.get_config().await;
        config.dead_threshold = 2;
        monitor.set_config(config).await;

        // Add a new monitor to reset (the existing one already has 2 failures)
        monitor.unmonitor_url("https://example.com/f.zip").await;
        monitor.monitor_url("https://example.com/f.zip").await;

        monitor
            .record_failure("https://example.com/f.zip", "e1")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_ne!(h.status, UrlHealthStatus::Dead);

        monitor
            .record_failure("https://example.com/f.zip", "e2")
            .await;
        let h = monitor
            .get_url_health("https://example.com/f.zip")
            .await
            .unwrap();
        assert_eq!(h.status, UrlHealthStatus::Dead);
    }
}
