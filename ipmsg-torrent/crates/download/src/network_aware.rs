//! Network-Aware Download Management
//!
//! Monitors network connectivity and automatically pauses downloads when the network
//! becomes unavailable, then resumes them when connectivity is restored.
//! Unlike manual pause, this preserves the user's intended running state and only
//! pauses due to network unavailability.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Error type for network-aware operations.
#[derive(Debug, thiserror::Error)]
pub enum NetworkAwareError {
    #[error("persistence error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Current network connectivity status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkStatus {
    /// Network is available and operational.
    Connected,
    /// Network is disconnected or unavailable.
    Disconnected,
    /// Network status is unknown (not yet checked).
    Unknown,
}

impl std::fmt::Display for NetworkStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkStatus::Connected => write!(f, "Connected"),
            NetworkStatus::Disconnected => write!(f, "Disconnected"),
            NetworkStatus::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Configuration for network-aware download management.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkAwareConfig {
    /// Whether network-aware management is enabled.
    pub enabled: bool,
    /// Interval in seconds between connectivity checks.
    pub check_interval_secs: u64,
    /// Number of consecutive failed checks before declaring disconnected.
    /// Prevents false positives from transient network hiccups.
    pub disconnect_threshold: u32,
    /// Number of consecutive successful checks before declaring connected.
    /// Prevents flapping when network is unstable.
    pub reconnect_threshold: u32,
    /// Whether to auto-resume tasks that were auto-paused when network recovers.
    pub auto_resume: bool,
    /// DNS hostname to probe for connectivity (default: "dns.google").
    pub probe_host: String,
    /// Port to use for connectivity probe (default: 53).
    pub probe_port: u16,
    /// Timeout in seconds for each connectivity probe.
    pub probe_timeout_secs: u64,
}

impl Default for NetworkAwareConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_secs: 30,
            disconnect_threshold: 2,
            reconnect_threshold: 2,
            auto_resume: true,
            probe_host: "dns.google".to_string(),
            probe_port: 53,
            probe_timeout_secs: 5,
        }
    }
}

/// Record of a network status transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkTransition {
    /// When the transition occurred.
    pub timestamp: DateTime<Utc>,
    /// Previous status.
    pub from: NetworkStatus,
    /// New status.
    pub to: NetworkStatus,
}

/// Summary of network-aware management state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkAwareSummary {
    /// Current network status.
    pub status: NetworkStatus,
    /// Whether network-aware management is enabled.
    pub enabled: bool,
    /// Number of consecutive successful probes.
    pub consecutive_successes: u32,
    /// Number of consecutive failed probes.
    pub consecutive_failures: u32,
    /// Task IDs that were auto-paused due to network disconnection.
    pub auto_paused_task_ids: Vec<String>,
    /// Total number of auto-pause events since startup.
    pub total_auto_pauses: u64,
    /// Total number of auto-resume events since startup.
    pub total_auto_resumes: u64,
    /// Recent network transitions (last 20).
    pub recent_transitions: Vec<NetworkTransition>,
    /// Last time a connectivity check was performed.
    pub last_check_at: Option<DateTime<Utc>>,
    /// Last time the network was confirmed connected.
    pub last_connected_at: Option<DateTime<Utc>>,
    /// Last time the network was confirmed disconnected.
    pub last_disconnected_at: Option<DateTime<Utc>>,
}

/// Manages network-aware download behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStateManager {
    /// Current configuration.
    config: NetworkAwareConfig,
    /// Current assessed network status.
    status: NetworkStatus,
    /// Consecutive successful connectivity probes.
    consecutive_successes: u32,
    /// Consecutive failed connectivity probes.
    consecutive_failures: u32,
    /// Task IDs auto-paused due to network disconnection.
    /// Maps task_id -> whether it was previously running (true) or queued (false).
    auto_paused_tasks: HashMap<String, bool>,
    /// Total auto-pause events.
    total_auto_pauses: u64,
    /// Total auto-resume events.
    total_auto_resumes: u64,
    /// Recent transitions (ring buffer, max 20).
    transitions: Vec<NetworkTransition>,
    /// Last check timestamp.
    last_check_at: Option<DateTime<Utc>>,
    /// Last connected timestamp.
    last_connected_at: Option<DateTime<Utc>>,
    /// Last disconnected timestamp.
    last_disconnected_at: Option<DateTime<Utc>>,
}

const MAX_TRANSITIONS: usize = 20;

impl NetworkStateManager {
    /// Create a new manager with default configuration.
    pub fn new() -> Self {
        Self {
            config: NetworkAwareConfig::default(),
            status: NetworkStatus::Unknown,
            consecutive_successes: 0,
            consecutive_failures: 0,
            auto_paused_tasks: HashMap::new(),
            total_auto_pauses: 0,
            total_auto_resumes: 0,
            transitions: Vec::new(),
            last_check_at: None,
            last_connected_at: None,
            last_disconnected_at: None,
        }
    }

    /// Get current configuration.
    pub fn config(&self) -> &NetworkAwareConfig {
        &self.config
    }

    /// Update configuration.
    pub fn set_config(&mut self, config: NetworkAwareConfig) {
        self.config = config;
    }

    /// Get current network status.
    pub fn status(&self) -> NetworkStatus {
        self.status
    }

    /// Get recent transitions (cloned slice).
    pub fn recent_transitions(&self) -> Vec<NetworkTransition> {
        self.transitions.clone()
    }

    /// Get task IDs that were auto-paused.
    pub fn auto_paused_task_ids(&self) -> Vec<String> {
        self.auto_paused_tasks.keys().cloned().collect()
    }

    /// Check if a specific task was auto-paused.
    pub fn is_auto_paused(&self, task_id: &str) -> bool {
        self.auto_paused_tasks.contains_key(task_id)
    }

    /// Record that a task was auto-paused due to network disconnection.
    /// Returns true if the task was newly added (not already tracked).
    pub fn record_auto_pause(&mut self, task_id: &str, was_running: bool) -> bool {
        let is_new = !self.auto_paused_tasks.contains_key(task_id);
        if is_new {
            self.auto_paused_tasks
                .insert(task_id.to_string(), was_running);
            self.total_auto_pauses += 1;
        }
        is_new
    }

    /// Remove a task from auto-paused tracking (it was resumed).
    /// Returns whether the task was being auto-paused and if it was running before.
    pub fn record_auto_resume(&mut self, task_id: &str) -> Option<bool> {
        let was_running = self.auto_paused_tasks.remove(task_id);
        if was_running.is_some() {
            self.total_auto_resumes += 1;
        }
        was_running
    }

    /// Clear all auto-paused task tracking.
    pub fn clear_auto_paused_tasks(&mut self) {
        self.auto_paused_tasks.clear();
    }

    /// Process a successful connectivity probe result.
    /// Returns the new status if a transition occurred.
    pub fn record_probe_success(&mut self) -> Option<NetworkTransition> {
        self.last_check_at = Some(Utc::now());
        self.consecutive_failures = 0;
        self.consecutive_successes += 1;

        let old_status = self.status;

        let should_transition = match self.status {
            NetworkStatus::Disconnected => {
                self.consecutive_successes >= self.config.reconnect_threshold
            }
            NetworkStatus::Unknown => self.consecutive_successes >= 1,
            NetworkStatus::Connected => false,
        };

        if should_transition {
            self.status = NetworkStatus::Connected;
            self.last_connected_at = Some(Utc::now());
            let transition = NetworkTransition {
                timestamp: Utc::now(),
                from: old_status,
                to: NetworkStatus::Connected,
            };
            self.push_transition(transition.clone());
            Some(transition)
        } else {
            None
        }
    }

    /// Process a failed connectivity probe result.
    /// Returns the new status if a transition occurred.
    pub fn record_probe_failure(&mut self) -> Option<NetworkTransition> {
        self.last_check_at = Some(Utc::now());
        self.consecutive_successes = 0;
        self.consecutive_failures += 1;

        let old_status = self.status;

        let should_transition = match self.status {
            NetworkStatus::Connected => {
                self.consecutive_failures >= self.config.disconnect_threshold
            }
            NetworkStatus::Unknown => self.consecutive_failures >= self.config.disconnect_threshold,
            NetworkStatus::Disconnected => false,
        };

        if should_transition {
            self.status = NetworkStatus::Disconnected;
            self.last_disconnected_at = Some(Utc::now());
            let transition = NetworkTransition {
                timestamp: Utc::now(),
                from: old_status,
                to: NetworkStatus::Disconnected,
            };
            self.push_transition(transition.clone());
            Some(transition)
        } else {
            None
        }
    }

    /// Force-set the network status (for manual override or testing).
    pub fn force_set_status(&mut self, status: NetworkStatus) {
        let old = self.status;
        if old != status {
            let transition = NetworkTransition {
                timestamp: Utc::now(),
                from: old,
                to: status,
            };
            self.push_transition(transition);
            self.status = status;
            match status {
                NetworkStatus::Connected => {
                    self.last_connected_at = Some(Utc::now());
                    self.consecutive_successes = self.config.reconnect_threshold;
                    self.consecutive_failures = 0;
                }
                NetworkStatus::Disconnected => {
                    self.last_disconnected_at = Some(Utc::now());
                    self.consecutive_failures = self.config.disconnect_threshold;
                    self.consecutive_successes = 0;
                }
                NetworkStatus::Unknown => {}
            }
        }
    }

    /// Get a summary of the current state.
    pub fn summary(&self) -> NetworkAwareSummary {
        NetworkAwareSummary {
            status: self.status,
            enabled: self.config.enabled,
            consecutive_successes: self.consecutive_successes,
            consecutive_failures: self.consecutive_failures,
            auto_paused_task_ids: self.auto_paused_task_ids(),
            total_auto_pauses: self.total_auto_pauses,
            total_auto_resumes: self.total_auto_resumes,
            recent_transitions: self.transitions.clone(),
            last_check_at: self.last_check_at,
            last_connected_at: self.last_connected_at,
            last_disconnected_at: self.last_disconnected_at,
        }
    }

    /// Check if network is currently considered connected.
    pub fn is_connected(&self) -> bool {
        self.status == NetworkStatus::Connected
    }

    /// Check if network is currently considered disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.status == NetworkStatus::Disconnected
    }

    /// Reset all counters and state (keeps config).
    pub fn reset(&mut self) {
        self.status = NetworkStatus::Unknown;
        self.consecutive_successes = 0;
        self.consecutive_failures = 0;
        self.auto_paused_tasks.clear();
        self.total_auto_pauses = 0;
        self.total_auto_resumes = 0;
        self.transitions.clear();
        self.last_check_at = None;
        self.last_connected_at = None;
        self.last_disconnected_at = None;
    }

    fn push_transition(&mut self, transition: NetworkTransition) {
        self.transitions.push(transition);
        if self.transitions.len() > MAX_TRANSITIONS {
            self.transitions.remove(0);
        }
    }
}

impl Default for NetworkStateManager {
    fn default() -> Self {
        Self::new()
    }
}

// --- Persistence ---

const CONFIG_FILE: &str = "network_aware_config.json";

/// Save configuration to disk (atomic write).
pub async fn save_config(
    config: &NetworkAwareConfig,
    data_dir: &Path,
) -> Result<(), NetworkAwareError> {
    let path = data_dir.join(CONFIG_FILE);
    let json = serde_json::to_string_pretty(config)?;
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, json.as_bytes()).await?;
    fs::rename(&tmp_path, &path).await?;
    Ok(())
}

/// Load configuration from disk.
pub async fn load_config(data_dir: &Path) -> Result<NetworkAwareConfig, NetworkAwareError> {
    let path = data_dir.join(CONFIG_FILE);
    let json = fs::read_to_string(&path).await?;
    let config: NetworkAwareConfig = serde_json::from_str(&json)?;
    Ok(config)
}

/// Check if config file exists.
pub fn config_file_exists(data_dir: &Path) -> bool {
    data_dir.join(CONFIG_FILE).exists()
}

/// Perform a connectivity probe by attempting a TCP connection.
/// Returns true if the connection succeeded.
pub async fn probe_connectivity(host: &str, port: u16, timeout_secs: u64) -> bool {
    use tokio::net::TcpStream;
    use tokio::time::{Duration, timeout};

    let addr = format!("{host}:{port}");
    let result = timeout(Duration::from_secs(timeout_secs), TcpStream::connect(&addr)).await;

    match result {
        Ok(Ok(_stream)) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = NetworkAwareConfig::default();
        assert!(config.enabled);
        assert_eq!(config.check_interval_secs, 30);
        assert_eq!(config.disconnect_threshold, 2);
        assert_eq!(config.reconnect_threshold, 2);
        assert!(config.auto_resume);
        assert_eq!(config.probe_host, "dns.google");
        assert_eq!(config.probe_port, 53);
        assert_eq!(config.probe_timeout_secs, 5);
    }

    #[test]
    fn test_new_manager() {
        let mgr = NetworkStateManager::new();
        assert_eq!(mgr.status(), NetworkStatus::Unknown);
        assert!(mgr.config().enabled);
        assert_eq!(mgr.consecutive_successes, 0);
        assert_eq!(mgr.consecutive_failures, 0);
        assert!(mgr.auto_paused_task_ids().is_empty());
        assert_eq!(mgr.total_auto_pauses, 0);
        assert_eq!(mgr.total_auto_resumes, 0);
        assert!(mgr.recent_transitions().is_empty());
    }

    #[test]
    fn test_probe_success_transitions_from_unknown_to_connected() {
        let mut mgr = NetworkStateManager::new();
        assert_eq!(mgr.status(), NetworkStatus::Unknown);

        let transition = mgr.record_probe_success();
        assert!(transition.is_some());
        let t = transition.unwrap();
        assert_eq!(t.from, NetworkStatus::Unknown);
        assert_eq!(t.to, NetworkStatus::Connected);
        assert_eq!(mgr.status(), NetworkStatus::Connected);
        assert!(mgr.is_connected());
    }

    #[test]
    fn test_probe_failure_threshold_for_disconnection() {
        let mut mgr = NetworkStateManager::new();
        mgr.force_set_status(NetworkStatus::Connected);
        assert_eq!(mgr.status(), NetworkStatus::Connected);

        // First failure - no transition yet (threshold = 2)
        let t1 = mgr.record_probe_failure();
        assert!(t1.is_none());
        assert_eq!(mgr.status(), NetworkStatus::Connected);
        assert_eq!(mgr.consecutive_failures, 1);

        // Second failure - should transition to disconnected
        let t2 = mgr.record_probe_failure();
        assert!(t2.is_some());
        let t = t2.unwrap();
        assert_eq!(t.from, NetworkStatus::Connected);
        assert_eq!(t.to, NetworkStatus::Disconnected);
        assert_eq!(mgr.status(), NetworkStatus::Disconnected);
        assert!(mgr.is_disconnected());
    }

    #[test]
    fn test_probe_success_resets_failure_counter() {
        let mut mgr = NetworkStateManager::new();
        mgr.force_set_status(NetworkStatus::Connected);

        // One failure, then a success should reset
        mgr.record_probe_failure();
        assert_eq!(mgr.consecutive_failures, 1);

        mgr.record_probe_success();
        assert_eq!(mgr.consecutive_failures, 0);
        assert_eq!(mgr.consecutive_successes, 1);
        assert_eq!(mgr.status(), NetworkStatus::Connected);
    }

    #[test]
    fn test_reconnect_requires_threshold() {
        let mut mgr = NetworkStateManager::new();
        mgr.force_set_status(NetworkStatus::Disconnected);

        // First success - no transition yet
        let t1 = mgr.record_probe_success();
        assert!(t1.is_none());
        assert_eq!(mgr.status(), NetworkStatus::Disconnected);

        // Second success - should reconnect
        let t2 = mgr.record_probe_success();
        assert!(t2.is_some());
        assert_eq!(mgr.status(), NetworkStatus::Connected);
    }

    #[test]
    fn test_auto_pause_tracking() {
        let mut mgr = NetworkStateManager::new();

        assert!(mgr.record_auto_pause("task-1", true));
        assert!(mgr.record_auto_pause("task-2", false));
        // Duplicate should return false
        assert!(!mgr.record_auto_pause("task-1", true));

        assert!(mgr.is_auto_paused("task-1"));
        assert!(mgr.is_auto_paused("task-2"));
        assert!(!mgr.is_auto_paused("task-3"));
        assert_eq!(mgr.auto_paused_task_ids().len(), 2);
        assert_eq!(mgr.total_auto_pauses, 2);
    }

    #[test]
    fn test_auto_resume_tracking() {
        let mut mgr = NetworkStateManager::new();

        mgr.record_auto_pause("task-1", true);
        mgr.record_auto_pause("task-2", false);

        let was_running = mgr.record_auto_resume("task-1");
        assert_eq!(was_running, Some(true));
        assert!(!mgr.is_auto_paused("task-1"));
        assert_eq!(mgr.total_auto_resumes, 1);

        let was_running2 = mgr.record_auto_resume("task-2");
        assert_eq!(was_running2, Some(false));
        assert_eq!(mgr.total_auto_resumes, 2);

        // Resuming non-tracked task returns None
        let was_running3 = mgr.record_auto_resume("task-3");
        assert!(was_running3.is_none());
        assert_eq!(mgr.total_auto_resumes, 2);
    }

    #[test]
    fn test_clear_auto_paused_tasks() {
        let mut mgr = NetworkStateManager::new();
        mgr.record_auto_pause("task-1", true);
        mgr.record_auto_pause("task-2", false);
        assert_eq!(mgr.auto_paused_task_ids().len(), 2);

        mgr.clear_auto_paused_tasks();
        assert!(mgr.auto_paused_task_ids().is_empty());
    }

    #[test]
    fn test_force_set_status() {
        let mut mgr = NetworkStateManager::new();
        assert_eq!(mgr.status(), NetworkStatus::Unknown);

        mgr.force_set_status(NetworkStatus::Connected);
        assert_eq!(mgr.status(), NetworkStatus::Connected);
        assert!(mgr.last_connected_at.is_some());

        mgr.force_set_status(NetworkStatus::Disconnected);
        assert_eq!(mgr.status(), NetworkStatus::Disconnected);
        assert!(mgr.last_disconnected_at.is_some());
    }

    #[test]
    fn test_force_set_same_status_no_transition() {
        let mut mgr = NetworkStateManager::new();
        mgr.force_set_status(NetworkStatus::Connected);
        let transition_count = mgr.summary().recent_transitions.len();

        // Setting same status should not add a transition
        mgr.force_set_status(NetworkStatus::Connected);
        assert_eq!(mgr.summary().recent_transitions.len(), transition_count);
    }

    #[test]
    fn test_transitions_ring_buffer() {
        let mut mgr = NetworkStateManager::new();

        // Generate more than MAX_TRANSITIONS transitions
        for i in 0..25 {
            if i % 2 == 0 {
                mgr.force_set_status(NetworkStatus::Connected);
            } else {
                mgr.force_set_status(NetworkStatus::Disconnected);
            }
        }

        // Should cap at MAX_TRANSITIONS
        assert!(mgr.summary().recent_transitions.len() <= MAX_TRANSITIONS);
    }

    #[test]
    fn test_summary() {
        let mut mgr = NetworkStateManager::new();
        mgr.force_set_status(NetworkStatus::Connected);
        mgr.record_auto_pause("task-1", true);

        let summary = mgr.summary();
        assert_eq!(summary.status, NetworkStatus::Connected);
        assert!(summary.enabled);
        assert_eq!(summary.auto_paused_task_ids.len(), 1);
        assert_eq!(summary.total_auto_pauses, 1);
        assert!(summary.last_connected_at.is_some());
        assert!(summary.last_disconnected_at.is_none());
    }

    #[test]
    fn test_reset() {
        let mut mgr = NetworkStateManager::new();
        mgr.force_set_status(NetworkStatus::Connected);
        mgr.record_auto_pause("task-1", true);
        mgr.record_probe_failure();

        mgr.reset();
        assert_eq!(mgr.status(), NetworkStatus::Unknown);
        assert_eq!(mgr.consecutive_successes, 0);
        assert_eq!(mgr.consecutive_failures, 0);
        assert!(mgr.auto_paused_task_ids().is_empty());
        assert_eq!(mgr.total_auto_pauses, 0);
        assert_eq!(mgr.total_auto_resumes, 0);
        assert!(mgr.summary().recent_transitions.is_empty());
        assert!(mgr.last_check_at.is_none());
    }

    #[test]
    fn test_config_serialization() {
        let config = NetworkAwareConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NetworkAwareConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.check_interval_secs, config.check_interval_secs);
        assert_eq!(
            deserialized.disconnect_threshold,
            config.disconnect_threshold
        );
        assert_eq!(deserialized.reconnect_threshold, config.reconnect_threshold);
        assert_eq!(deserialized.auto_resume, config.auto_resume);
        assert_eq!(deserialized.probe_host, config.probe_host);
    }

    #[test]
    fn test_manager_serialization() {
        let mut mgr = NetworkStateManager::new();
        mgr.force_set_status(NetworkStatus::Connected);
        mgr.record_auto_pause("task-1", true);

        let json = serde_json::to_string(&mgr).unwrap();
        let deserialized: NetworkStateManager = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.status(), NetworkStatus::Connected);
        assert!(deserialized.is_auto_paused("task-1"));
        assert_eq!(deserialized.total_auto_pauses, 1);
    }

    #[test]
    fn test_network_status_display() {
        assert_eq!(NetworkStatus::Connected.to_string(), "Connected");
        assert_eq!(NetworkStatus::Disconnected.to_string(), "Disconnected");
        assert_eq!(NetworkStatus::Unknown.to_string(), "Unknown");
    }

    #[test]
    fn test_probe_success_failure_interleaving() {
        let mut mgr = NetworkStateManager::new();
        mgr.force_set_status(NetworkStatus::Connected);

        // Failure, success, failure, success - should stay connected
        mgr.record_probe_failure();
        mgr.record_probe_success(); // resets failures
        assert_eq!(mgr.consecutive_failures, 0);
        assert_eq!(mgr.status(), NetworkStatus::Connected);

        mgr.record_probe_failure();
        mgr.record_probe_success(); // resets failures again
        assert_eq!(mgr.consecutive_failures, 0);
        assert_eq!(mgr.status(), NetworkStatus::Connected);
    }

    #[test]
    fn test_unknown_to_disconnected() {
        let mut mgr = NetworkStateManager::new();
        assert_eq!(mgr.status(), NetworkStatus::Unknown);

        // First failure
        let t1 = mgr.record_probe_failure();
        assert!(t1.is_none());

        // Second failure - should transition to disconnected
        let t2 = mgr.record_probe_failure();
        assert!(t2.is_some());
        assert_eq!(mgr.status(), NetworkStatus::Disconnected);
    }

    #[tokio::test]
    async fn test_save_and_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = NetworkAwareConfig {
            enabled: false,
            check_interval_secs: 60,
            disconnect_threshold: 3,
            ..Default::default()
        };

        save_config(&config, dir.path()).await.unwrap();
        let loaded = load_config(dir.path()).await.unwrap();
        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.check_interval_secs, 60);
        assert_eq!(loaded.disconnect_threshold, 3);
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_config(dir.path()).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_config_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!config_file_exists(dir.path()));
    }

    #[tokio::test]
    async fn test_config_file_exists_after_save() {
        let dir = tempfile::tempdir().unwrap();
        let config = NetworkAwareConfig::default();
        save_config(&config, dir.path()).await.unwrap();
        assert!(config_file_exists(dir.path()));
    }
}
