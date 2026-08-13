//! Download Connection Health Monitor (Phase 94)
//!
//! Tracks per-connection quality metrics (speed, errors, latency) and detects
//! degraded connections that should be replaced or retried.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Health status of a connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionHealthStatus {
    /// Connection is performing well
    Healthy,
    /// Connection shows signs of degradation
    Degraded,
    /// Connection is unreliable and should be replaced
    Unhealthy,
    /// Connection has no data yet
    Unknown,
}

impl std::fmt::Display for ConnectionHealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionHealthStatus::Healthy => write!(f, "✅ Healthy"),
            ConnectionHealthStatus::Degraded => write!(f, "⚠️ Degraded"),
            ConnectionHealthStatus::Unhealthy => write!(f, "❌ Unhealthy"),
            ConnectionHealthStatus::Unknown => write!(f, "❓ Unknown"),
        }
    }
}

/// Metrics for a single connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionMetrics {
    /// Unique connection identifier
    pub connection_id: String,
    /// Task ID this connection belongs to
    pub task_id: String,
    /// Protocol type (http, torrent, ed2k, p2p)
    pub protocol: String,
    /// Remote host/peer address
    pub remote_addr: String,
    /// Total bytes transferred through this connection
    pub bytes_transferred: u64,
    /// Total transfer errors observed
    pub error_count: u32,
    /// Total timeout events
    pub timeout_count: u32,
    /// Total stall events (speed dropped below threshold)
    pub stall_count: u32,
    /// Last measured speed in bytes/sec
    pub last_speed_bps: u64,
    /// Average speed in bytes/sec (exponential moving average)
    pub avg_speed_bps: u64,
    /// Connection creation timestamp (Unix epoch seconds)
    pub created_at: u64,
    /// Last activity timestamp (Unix epoch seconds)
    pub last_activity_at: u64,
    /// Number of speed samples collected
    pub sample_count: u32,
}

impl ConnectionMetrics {
    /// Create new connection metrics
    pub fn new(
        connection_id: String,
        task_id: String,
        protocol: String,
        remote_addr: String,
    ) -> Self {
        let now = current_epoch_secs();
        Self {
            connection_id,
            task_id,
            protocol,
            remote_addr,
            bytes_transferred: 0,
            error_count: 0,
            timeout_count: 0,
            stall_count: 0,
            last_speed_bps: 0,
            avg_speed_bps: 0,
            created_at: now,
            last_activity_at: now,
            sample_count: 0,
        }
    }

    /// Record a speed sample and update moving average
    pub fn record_speed(&mut self, speed_bps: u64) {
        self.last_speed_bps = speed_bps;
        self.sample_count += 1;
        // Exponential moving average: α = 0.3 (recent samples weighted more)
        if self.avg_speed_bps == 0 {
            self.avg_speed_bps = speed_bps;
        } else {
            self.avg_speed_bps =
                ((self.avg_speed_bps as f64) * 0.7 + (speed_bps as f64) * 0.3) as u64;
        }
        self.last_activity_at = current_epoch_secs();
    }

    /// Record bytes transferred
    pub fn record_transfer(&mut self, bytes: u64) {
        self.bytes_transferred += bytes;
        self.last_activity_at = current_epoch_secs();
    }

    /// Record an error event
    pub fn record_error(&mut self) {
        self.error_count += 1;
        self.last_activity_at = current_epoch_secs();
    }

    /// Record a timeout event
    pub fn record_timeout(&mut self) {
        self.timeout_count += 1;
        self.last_activity_at = current_epoch_secs();
    }

    /// Record a stall event
    pub fn record_stall(&mut self) {
        self.stall_count += 1;
        self.last_activity_at = current_epoch_secs();
    }

    /// Check if connection is stale (no activity for given seconds)
    pub fn is_stale(&self, max_idle_secs: u64) -> bool {
        let now = current_epoch_secs();
        now.saturating_sub(self.last_activity_at) >= max_idle_secs && max_idle_secs > 0
    }

    /// Get connection age in seconds
    pub fn age_secs(&self) -> u64 {
        let now = current_epoch_secs();
        now.saturating_sub(self.created_at)
    }
}

/// Configuration for connection health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionHealthConfig {
    /// Enable connection health monitoring
    pub enabled: bool,
    /// Speed below this (bytes/sec) is considered a stall
    pub stall_threshold_bps: u64,
    /// Consecutive stalls before marking as degraded
    pub degraded_stall_threshold: u32,
    /// Consecutive stalls before marking as unhealthy
    pub unhealthy_stall_threshold: u32,
    /// Error count above which connection is degraded
    pub degraded_error_threshold: u32,
    /// Error count above which connection is unhealthy
    pub unhealthy_error_threshold: u32,
    /// Timeout count above which connection is degraded
    pub degraded_timeout_threshold: u32,
    /// Timeout count above which connection is unhealthy
    pub unhealthy_timeout_threshold: u32,
    /// Maximum idle time (seconds) before connection is considered stale
    pub max_idle_secs: u64,
    /// Maximum connections to track per task
    pub max_connections_per_task: usize,
    /// Maximum total connections to track
    pub max_total_connections: usize,
}

impl Default for ConnectionHealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            stall_threshold_bps: 1024, // 1 KB/s
            degraded_stall_threshold: 3,
            unhealthy_stall_threshold: 6,
            degraded_error_threshold: 3,
            unhealthy_error_threshold: 8,
            degraded_timeout_threshold: 2,
            unhealthy_timeout_threshold: 5,
            max_idle_secs: 300, // 5 minutes
            max_connections_per_task: 50,
            max_total_connections: 500,
        }
    }
}

/// Health assessment result for a connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionHealthAssessment {
    pub connection_id: String,
    pub task_id: String,
    pub remote_addr: String,
    pub protocol: String,
    pub status: ConnectionHealthStatus,
    pub reason: String,
    pub metrics: ConnectionMetrics,
    /// Recommended action
    pub action: ConnectionAction,
}

/// Recommended action for a connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionAction {
    /// No action needed
    None,
    /// Monitor closely
    Watch,
    /// Consider replacing with alternative
    Replace,
    /// Terminate immediately
    Terminate,
}

impl std::fmt::Display for ConnectionAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionAction::None => write!(f, "none"),
            ConnectionAction::Watch => write!(f, "👀 watch"),
            ConnectionAction::Replace => write!(f, "🔄 replace"),
            ConnectionAction::Terminate => write!(f, "🛑 terminate"),
        }
    }
}

/// Summary of connection health across all tracked connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionHealthSummary {
    pub total_connections: usize,
    pub healthy_count: usize,
    pub degraded_count: usize,
    pub unhealthy_count: usize,
    pub unknown_count: usize,
    pub stale_count: usize,
    pub total_bytes_transferred: u64,
    pub total_errors: u32,
    pub total_timeouts: u32,
    pub connections_needing_action: Vec<ConnectionHealthAssessment>,
}

/// Manager for connection health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionHealthManager {
    config: ConnectionHealthConfig,
    /// connection_id -> ConnectionMetrics
    connections: HashMap<String, ConnectionMetrics>,
}

impl Default for ConnectionHealthManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionHealthManager {
    /// Create a new manager with default config
    pub fn new() -> Self {
        Self {
            config: ConnectionHealthConfig::default(),
            connections: HashMap::new(),
        }
    }

    /// Create a new manager with custom config
    pub fn with_config(config: ConnectionHealthConfig) -> Self {
        Self {
            config,
            connections: HashMap::new(),
        }
    }

    /// Get current config
    pub fn config(&self) -> &ConnectionHealthConfig {
        &self.config
    }

    /// Update config
    pub fn set_config(&mut self, config: ConnectionHealthConfig) {
        self.config = config;
    }

    /// Register a new connection
    pub fn register_connection(
        &mut self,
        connection_id: String,
        task_id: String,
        protocol: String,
        remote_addr: String,
    ) -> bool {
        if !self.config.enabled {
            return false;
        }

        // Enforce total connection limit
        if self.connections.len() >= self.config.max_total_connections {
            // Try to evict stale connections first
            self.evict_stale_connections();
            if self.connections.len() >= self.config.max_total_connections {
                return false;
            }
        }

        // Enforce per-task limit
        let task_count = self
            .connections
            .values()
            .filter(|c| c.task_id == task_id)
            .count();
        if task_count >= self.config.max_connections_per_task {
            return false;
        }

        let metrics = ConnectionMetrics::new(connection_id.clone(), task_id, protocol, remote_addr);
        self.connections.insert(connection_id, metrics);
        true
    }

    /// Unregister a connection
    pub fn unregister_connection(&mut self, connection_id: &str) -> Option<ConnectionMetrics> {
        self.connections.remove(connection_id)
    }

    /// Record speed for a connection
    pub fn record_speed(&mut self, connection_id: &str, speed_bps: u64) {
        if let Some(metrics) = self.connections.get_mut(connection_id) {
            metrics.record_speed(speed_bps);
            // Check for stall
            if speed_bps < self.config.stall_threshold_bps && speed_bps > 0 {
                metrics.record_stall();
            }
        }
    }

    /// Record bytes transferred for a connection
    pub fn record_transfer(&mut self, connection_id: &str, bytes: u64) {
        if let Some(metrics) = self.connections.get_mut(connection_id) {
            metrics.record_transfer(bytes);
        }
    }

    /// Record an error for a connection
    pub fn record_error(&mut self, connection_id: &str) {
        if let Some(metrics) = self.connections.get_mut(connection_id) {
            metrics.record_error();
        }
    }

    /// Record a timeout for a connection
    pub fn record_timeout(&mut self, connection_id: &str) {
        if let Some(metrics) = self.connections.get_mut(connection_id) {
            metrics.record_timeout();
        }
    }

    /// Assess health of a single connection
    pub fn assess_connection(&self, connection_id: &str) -> Option<ConnectionHealthAssessment> {
        let metrics = self.connections.get(connection_id)?;
        let (status, reason, action) = self.evaluate_metrics(metrics);

        Some(ConnectionHealthAssessment {
            connection_id: metrics.connection_id.clone(),
            task_id: metrics.task_id.clone(),
            remote_addr: metrics.remote_addr.clone(),
            protocol: metrics.protocol.clone(),
            status,
            reason,
            metrics: metrics.clone(),
            action,
        })
    }

    /// Assess all connections for a task
    pub fn assess_task_connections(&self, task_id: &str) -> Vec<ConnectionHealthAssessment> {
        self.connections
            .values()
            .filter(|c| c.task_id == task_id)
            .map(|metrics| {
                let (status, reason, action) = self.evaluate_metrics(metrics);
                ConnectionHealthAssessment {
                    connection_id: metrics.connection_id.clone(),
                    task_id: metrics.task_id.clone(),
                    remote_addr: metrics.remote_addr.clone(),
                    protocol: metrics.protocol.clone(),
                    status,
                    reason,
                    metrics: metrics.clone(),
                    action,
                }
            })
            .collect()
    }

    /// Generate overall health summary
    pub fn get_summary(&self) -> ConnectionHealthSummary {
        let mut healthy_count = 0;
        let mut degraded_count = 0;
        let mut unhealthy_count = 0;
        let mut unknown_count = 0;
        let mut stale_count = 0;
        let mut total_bytes = 0u64;
        let mut total_errors = 0u32;
        let mut total_timeouts = 0u32;
        let mut needing_action = Vec::new();

        for metrics in self.connections.values() {
            let (status, reason, action) = self.evaluate_metrics(metrics);

            match status {
                ConnectionHealthStatus::Healthy => healthy_count += 1,
                ConnectionHealthStatus::Degraded => degraded_count += 1,
                ConnectionHealthStatus::Unhealthy => unhealthy_count += 1,
                ConnectionHealthStatus::Unknown => unknown_count += 1,
            }

            if metrics.is_stale(self.config.max_idle_secs) {
                stale_count += 1;
            }

            total_bytes += metrics.bytes_transferred;
            total_errors += metrics.error_count;
            total_timeouts += metrics.timeout_count;

            if action != ConnectionAction::None {
                needing_action.push(ConnectionHealthAssessment {
                    connection_id: metrics.connection_id.clone(),
                    task_id: metrics.task_id.clone(),
                    remote_addr: metrics.remote_addr.clone(),
                    protocol: metrics.protocol.clone(),
                    status,
                    reason,
                    metrics: metrics.clone(),
                    action,
                });
            }
        }

        ConnectionHealthSummary {
            total_connections: self.connections.len(),
            healthy_count,
            degraded_count,
            unhealthy_count,
            unknown_count,
            stale_count,
            total_bytes_transferred: total_bytes,
            total_errors,
            total_timeouts,
            connections_needing_action: needing_action,
        }
    }

    /// Remove all connections for a completed/removed task
    pub fn remove_task_connections(&mut self, task_id: &str) -> usize {
        let ids: Vec<String> = self
            .connections
            .iter()
            .filter(|(_, c)| c.task_id == task_id)
            .map(|(id, _)| id.clone())
            .collect();
        let count = ids.len();
        for id in &ids {
            self.connections.remove(id);
        }
        count
    }

    /// Evict stale connections
    pub fn evict_stale_connections(&mut self) -> usize {
        let stale_ids: Vec<String> = self
            .connections
            .iter()
            .filter(|(_, c)| c.is_stale(self.config.max_idle_secs))
            .map(|(id, _)| id.clone())
            .collect();
        let count = stale_ids.len();
        for id in &stale_ids {
            self.connections.remove(id);
        }
        count
    }

    /// Get list of unhealthy connection IDs
    pub fn get_unhealthy_connections(&self) -> Vec<String> {
        self.connections
            .iter()
            .filter(|(_, c)| {
                let (status, _, _) = self.evaluate_metrics(c);
                status == ConnectionHealthStatus::Unhealthy
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get connection count
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Get connection count for a specific task
    pub fn task_connection_count(&self, task_id: &str) -> usize {
        self.connections
            .values()
            .filter(|c| c.task_id == task_id)
            .count()
    }

    /// Clear all tracked connections
    pub fn clear_all_connections(&mut self) -> usize {
        let count = self.connections.len();
        self.connections.clear();
        count
    }

    /// Internal: evaluate metrics and return (status, reason, action)
    fn evaluate_metrics(
        &self,
        metrics: &ConnectionMetrics,
    ) -> (ConnectionHealthStatus, String, ConnectionAction) {
        // Check for stale connection first
        if metrics.is_stale(self.config.max_idle_secs) {
            return (
                ConnectionHealthStatus::Unhealthy,
                format!(
                    "No activity for {}s (max: {}s)",
                    metrics.age_secs(),
                    self.config.max_idle_secs
                ),
                ConnectionAction::Terminate,
            );
        }

        // Check error thresholds (before unknown check, so errors are caught even without speed data)
        if metrics.error_count >= self.config.unhealthy_error_threshold
            || metrics.timeout_count >= self.config.unhealthy_timeout_threshold
            || metrics.stall_count >= self.config.unhealthy_stall_threshold
        {
            return (
                ConnectionHealthStatus::Unhealthy,
                format!(
                    "errors={}, timeouts={}, stalls={}",
                    metrics.error_count, metrics.timeout_count, metrics.stall_count
                ),
                ConnectionAction::Terminate,
            );
        }

        if metrics.error_count >= self.config.degraded_error_threshold
            || metrics.timeout_count >= self.config.degraded_timeout_threshold
            || metrics.stall_count >= self.config.degraded_stall_threshold
        {
            return (
                ConnectionHealthStatus::Degraded,
                format!(
                    "errors={}, timeouts={}, stalls={}",
                    metrics.error_count, metrics.timeout_count, metrics.stall_count
                ),
                ConnectionAction::Replace,
            );
        }

        // No samples yet
        if metrics.sample_count == 0 {
            return (
                ConnectionHealthStatus::Unknown,
                "No speed samples collected yet".to_string(),
                ConnectionAction::None,
            );
        }

        // Check if current speed is very low compared to average
        if metrics.last_speed_bps > 0
            && metrics.avg_speed_bps > 0
            && metrics.last_speed_bps < metrics.avg_speed_bps / 4
        {
            return (
                ConnectionHealthStatus::Degraded,
                format!(
                    "Current speed {} is <25% of average {}",
                    metrics.last_speed_bps, metrics.avg_speed_bps
                ),
                ConnectionAction::Watch,
            );
        }

        (
            ConnectionHealthStatus::Healthy,
            format!(
                "avg_speed={} B/s, {} bytes transferred",
                metrics.avg_speed_bps, metrics.bytes_transferred
            ),
            ConnectionAction::None,
        )
    }

    /// Save config to JSON string
    pub fn save_config_to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.config)
    }

    /// Load config from JSON string
    pub fn load_config_from_string(&mut self, data: &str) -> Result<(), serde_json::Error> {
        self.config = serde_json::from_str(data)?;
        Ok(())
    }
}

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Persistence error type
#[derive(Debug)]
pub enum ConnectionHealthPersistenceError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for ConnectionHealthPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Json(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<std::io::Error> for ConnectionHealthPersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for ConnectionHealthPersistenceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Save connection health config to disk (atomic write)
pub fn save_connection_health_config(
    config: &ConnectionHealthConfig,
    data_dir: &std::path::Path,
) -> Result<(), ConnectionHealthPersistenceError> {
    let path = data_dir.join("connection_health_config.json");
    let json = serde_json::to_string_pretty(config)?;
    let tmp_path = data_dir.join("connection_health_config.json.tmp");
    std::fs::write(&tmp_path, json.as_bytes())?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load connection health config from disk
pub fn load_connection_health_config(
    data_dir: &std::path::Path,
) -> Result<Option<ConnectionHealthConfig>, ConnectionHealthPersistenceError> {
    let path = data_dir.join("connection_health_config.json");
    if !path.exists() {
        return Ok(None);
    }
    let data = std::fs::read_to_string(&path)?;
    let config: ConnectionHealthConfig = serde_json::from_str(&data)?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_metrics_new() {
        let m = ConnectionMetrics::new(
            "conn-1".into(),
            "task-1".into(),
            "http".into(),
            "1.2.3.4:80".into(),
        );
        assert_eq!(m.connection_id, "conn-1");
        assert_eq!(m.task_id, "task-1");
        assert_eq!(m.bytes_transferred, 0);
        assert_eq!(m.error_count, 0);
        assert_eq!(m.sample_count, 0);
        assert_eq!(m.avg_speed_bps, 0);
    }

    #[test]
    fn test_record_speed_ema() {
        let mut m = ConnectionMetrics::new("c1".into(), "t1".into(), "http".into(), "host".into());
        m.record_speed(10000);
        assert_eq!(m.avg_speed_bps, 10000); // First sample = exact
        m.record_speed(20000);
        // EMA: 10000 * 0.7 + 20000 * 0.3 = 7000 + 6000 = 13000
        assert_eq!(m.avg_speed_bps, 13000);
        assert_eq!(m.last_speed_bps, 20000);
        assert_eq!(m.sample_count, 2);
    }

    #[test]
    fn test_record_transfer() {
        let mut m = ConnectionMetrics::new("c1".into(), "t1".into(), "http".into(), "host".into());
        m.record_transfer(500);
        m.record_transfer(300);
        assert_eq!(m.bytes_transferred, 800);
    }

    #[test]
    fn test_record_errors() {
        let mut m = ConnectionMetrics::new("c1".into(), "t1".into(), "http".into(), "host".into());
        m.record_error();
        m.record_error();
        m.record_timeout();
        m.record_stall();
        assert_eq!(m.error_count, 2);
        assert_eq!(m.timeout_count, 1);
        assert_eq!(m.stall_count, 1);
    }

    #[test]
    fn test_is_stale() {
        let mut m = ConnectionMetrics::new("c1".into(), "t1".into(), "http".into(), "host".into());
        // Just created, not stale with 300s threshold
        assert!(!m.is_stale(300));
        // With 0s threshold, nothing is stale (special case)
        assert!(!m.is_stale(0));
        // Manually set last_activity to the past
        m.last_activity_at = current_epoch_secs() - 10;
        assert!(m.is_stale(10));
        assert!(m.is_stale(5));
        assert!(!m.is_stale(15));
    }

    #[test]
    fn test_connection_health_config_default() {
        let config = ConnectionHealthConfig::default();
        assert!(config.enabled);
        assert_eq!(config.stall_threshold_bps, 1024);
        assert_eq!(config.degraded_stall_threshold, 3);
        assert_eq!(config.unhealthy_stall_threshold, 6);
        assert_eq!(config.max_idle_secs, 300);
        assert_eq!(config.max_total_connections, 500);
    }

    #[test]
    fn test_manager_register_unregister() {
        let mut mgr = ConnectionHealthManager::new();
        assert!(mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host:80".into()));
        assert_eq!(mgr.connection_count(), 1);
        assert_eq!(mgr.task_connection_count("t1"), 1);

        let removed = mgr.unregister_connection("c1");
        assert!(removed.is_some());
        assert_eq!(mgr.connection_count(), 0);
    }

    #[test]
    fn test_manager_disabled() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.set_config(ConnectionHealthConfig {
            enabled: false,
            ..Default::default()
        });
        assert!(!mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host".into()));
        assert_eq!(mgr.connection_count(), 0);
    }

    #[test]
    fn test_manager_per_task_limit() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.set_config(ConnectionHealthConfig {
            max_connections_per_task: 2,
            ..Default::default()
        });
        assert!(mgr.register_connection("c1".into(), "t1".into(), "http".into(), "h1".into()));
        assert!(mgr.register_connection("c2".into(), "t1".into(), "http".into(), "h2".into()));
        assert!(!mgr.register_connection("c3".into(), "t1".into(), "http".into(), "h3".into()));
        assert_eq!(mgr.connection_count(), 2);
    }

    #[test]
    fn test_assess_unknown_no_samples() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host".into());
        let assessment = mgr.assess_connection("c1").unwrap();
        assert_eq!(assessment.status, ConnectionHealthStatus::Unknown);
        assert_eq!(assessment.action, ConnectionAction::None);
    }

    #[test]
    fn test_assess_healthy() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host".into());
        mgr.record_speed("c1", 50000);
        mgr.record_transfer("c1", 100000);
        let assessment = mgr.assess_connection("c1").unwrap();
        assert_eq!(assessment.status, ConnectionHealthStatus::Healthy);
        assert_eq!(assessment.action, ConnectionAction::None);
    }

    #[test]
    fn test_assess_degraded_by_stalls() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host".into());
        // Record speed above stall threshold first to avoid auto-stall from record_speed
        mgr.record_speed("c1", 50000);
        // Manually record stalls (simulating stalls detected elsewhere)
        if let Some(m) = mgr.connections.get_mut("c1") {
            m.stall_count = 3; // degraded_stall_threshold
        }
        let assessment = mgr.assess_connection("c1").unwrap();
        assert_eq!(assessment.status, ConnectionHealthStatus::Degraded);
        assert_eq!(assessment.action, ConnectionAction::Replace);
    }

    #[test]
    fn test_assess_unhealthy_by_errors() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host".into());
        mgr.record_speed("c1", 50000);
        for _ in 0..8 {
            mgr.record_error("c1");
        }
        let assessment = mgr.assess_connection("c1").unwrap();
        assert_eq!(assessment.status, ConnectionHealthStatus::Unhealthy);
        assert_eq!(assessment.action, ConnectionAction::Terminate);
    }

    #[test]
    fn test_assess_degraded_speed_drop() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host".into());
        // Build up high average
        for _ in 0..10 {
            mgr.record_speed("c1", 100000);
        }
        // Now drop speed to <25% of average
        mgr.record_speed("c1", 10000);
        let assessment = mgr.assess_connection("c1").unwrap();
        assert_eq!(assessment.status, ConnectionHealthStatus::Degraded);
        assert_eq!(assessment.action, ConnectionAction::Watch);
    }

    #[test]
    fn test_summary() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "h1".into());
        mgr.register_connection("c2".into(), "t1".into(), "torrent".into(), "h2".into());
        mgr.record_speed("c1", 50000);
        mgr.record_transfer("c1", 100000);
        // c2 stays unknown
        let summary = mgr.get_summary();
        assert_eq!(summary.total_connections, 2);
        assert_eq!(summary.healthy_count, 1);
        assert_eq!(summary.unknown_count, 1);
        assert_eq!(summary.total_bytes_transferred, 100000);
    }

    #[test]
    fn test_remove_task_connections() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "h1".into());
        mgr.register_connection("c2".into(), "t1".into(), "http".into(), "h2".into());
        mgr.register_connection("c3".into(), "t2".into(), "torrent".into(), "h3".into());
        let removed = mgr.remove_task_connections("t1");
        assert_eq!(removed, 2);
        assert_eq!(mgr.connection_count(), 1);
        assert_eq!(mgr.task_connection_count("t1"), 0);
        assert_eq!(mgr.task_connection_count("t2"), 1);
    }

    #[test]
    fn test_get_unhealthy_connections() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "h1".into());
        mgr.register_connection("c2".into(), "t1".into(), "http".into(), "h2".into());
        // Give c1 speed data so it's healthy
        mgr.record_speed("c1", 50000);
        // Make c2 unhealthy by recording errors (no speed needed now since error check is before unknown check)
        for _ in 0..8 {
            mgr.record_error("c2");
        }
        let unhealthy = mgr.get_unhealthy_connections();
        assert_eq!(unhealthy.len(), 1);
        assert_eq!(unhealthy[0], "c2");
    }

    #[test]
    fn test_assess_task_connections() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "h1".into());
        mgr.register_connection("c2".into(), "t1".into(), "torrent".into(), "h2".into());
        mgr.register_connection("c3".into(), "t2".into(), "http".into(), "h3".into());
        mgr.record_speed("c1", 50000);
        let assessments = mgr.assess_task_connections("t1");
        assert_eq!(assessments.len(), 2);
    }

    #[test]
    fn test_config_save_load() {
        let mgr = ConnectionHealthManager::new();
        let json = mgr.save_config_to_string().unwrap();
        let mut mgr2 = ConnectionHealthManager::new();
        mgr2.load_config_from_string(&json).unwrap();
        assert_eq!(
            mgr2.config().stall_threshold_bps,
            mgr.config().stall_threshold_bps
        );
        assert_eq!(mgr2.config().max_idle_secs, mgr.config().max_idle_secs);
    }

    #[test]
    fn test_config_load_invalid() {
        let mut mgr = ConnectionHealthManager::new();
        assert!(mgr.load_config_from_string("not json").is_err());
    }

    #[test]
    fn test_status_display() {
        assert_eq!(format!("{}", ConnectionHealthStatus::Healthy), "✅ Healthy");
        assert_eq!(
            format!("{}", ConnectionHealthStatus::Degraded),
            "⚠️ Degraded"
        );
        assert_eq!(
            format!("{}", ConnectionHealthStatus::Unhealthy),
            "❌ Unhealthy"
        );
        assert_eq!(format!("{}", ConnectionHealthStatus::Unknown), "❓ Unknown");
    }

    #[test]
    fn test_action_display() {
        assert_eq!(format!("{}", ConnectionAction::None), "none");
        assert_eq!(format!("{}", ConnectionAction::Watch), "👀 watch");
        assert_eq!(format!("{}", ConnectionAction::Replace), "🔄 replace");
        assert_eq!(format!("{}", ConnectionAction::Terminate), "🛑 terminate");
    }

    #[test]
    fn test_evict_stale_connections() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.set_config(ConnectionHealthConfig {
            max_idle_secs: 1, // 1 second threshold
            ..Default::default()
        });
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "h1".into());
        mgr.register_connection("c2".into(), "t1".into(), "http".into(), "h2".into());
        // Manually make them stale
        if let Some(c) = mgr.connections.get_mut("c1") {
            c.last_activity_at = current_epoch_secs() - 2;
        }
        if let Some(c) = mgr.connections.get_mut("c2") {
            c.last_activity_at = current_epoch_secs() - 2;
        }
        let evicted = mgr.evict_stale_connections();
        assert_eq!(evicted, 2);
        assert_eq!(mgr.connection_count(), 0);
    }

    #[test]
    fn test_record_speed_stall_detection() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host".into());
        // Record speed below stall threshold (>0 but <1024)
        mgr.record_speed("c1", 500);
        let m = mgr.connections.get("c1").unwrap();
        assert_eq!(m.stall_count, 1);
    }

    #[test]
    fn test_connection_age() {
        let m = ConnectionMetrics::new("c1".into(), "t1".into(), "http".into(), "host".into());
        // Just created, age should be 0 or 1
        assert!(m.age_secs() <= 1);
    }

    #[test]
    fn test_max_total_connections_limit() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.set_config(ConnectionHealthConfig {
            max_total_connections: 2,
            max_idle_secs: 1,
            ..Default::default()
        });
        assert!(mgr.register_connection("c1".into(), "t1".into(), "http".into(), "h1".into()));
        assert!(mgr.register_connection("c2".into(), "t2".into(), "http".into(), "h2".into()));
        // Make existing connections stale so eviction can clear them
        if let Some(c) = mgr.connections.get_mut("c1") {
            c.last_activity_at = current_epoch_secs() - 2;
        }
        if let Some(c) = mgr.connections.get_mut("c2") {
            c.last_activity_at = current_epoch_secs() - 2;
        }
        // Third should succeed after evicting stale connections
        assert!(mgr.register_connection("c3".into(), "t3".into(), "http".into(), "h3".into()));
        assert_eq!(mgr.connection_count(), 1); // Only c3 remains
    }

    #[test]
    fn test_summary_needing_action() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "h1".into());
        mgr.register_connection("c2".into(), "t1".into(), "http".into(), "h2".into());
        mgr.record_speed("c1", 50000);
        // c2 is unknown (no samples) - action is None, so not in needing_action
        let summary = mgr.get_summary();
        // Only connections with action != None are listed
        assert_eq!(summary.connections_needing_action.len(), 0);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut mgr = ConnectionHealthManager::new();
        mgr.register_connection("c1".into(), "t1".into(), "http".into(), "host:80".into());
        mgr.record_speed("c1", 50000);
        mgr.record_transfer("c1", 100000);

        let json = serde_json::to_string(&mgr).unwrap();
        let mgr2: ConnectionHealthManager = serde_json::from_str(&json).unwrap();
        assert_eq!(mgr2.connection_count(), 1);
        let assessment = mgr2.assess_connection("c1").unwrap();
        assert_eq!(assessment.status, ConnectionHealthStatus::Healthy);
    }
}
