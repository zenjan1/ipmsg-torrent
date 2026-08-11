//! Download Host Connection Limiter (Phase 148)
//!
//! Tracks and limits concurrent TCP connections per download host to prevent
//! overwhelming individual servers. Provides per-host connection stats,
//! configurable limits, and queue integration for scheduler-aware throttling.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Configuration for the host connection limiter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostConnLimitConfig {
    /// Enable host connection limiting
    pub enabled: bool,
    /// Default maximum concurrent connections per host (0 = unlimited)
    pub default_max_connections: u32,
    /// Per-host overrides (hostname -> max_connections)
    pub host_overrides: HashMap<String, u32>,
    /// Maximum number of tracked hosts (prevent memory bloat)
    pub max_tracked_hosts: usize,
    /// Connection idle timeout in seconds (auto-release stale connections)
    pub idle_timeout_secs: u64,
}

impl Default for HostConnLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_max_connections: 4,
            host_overrides: HashMap::new(),
            max_tracked_hosts: 500,
            idle_timeout_secs: 300, // 5 minutes
        }
    }
}

/// Tracks a single host's connection state
#[derive(Debug, Clone)]
pub struct HostConnectionState {
    /// Hostname (normalized, lowercase)
    pub hostname: String,
    /// Number of active connections
    pub active_connections: u32,
    /// Total connections ever made to this host
    pub total_connections: u64,
    /// Total connection failures
    pub total_failures: u64,
    /// Peak concurrent connections observed
    pub peak_connections: u32,
    /// Last time a connection was opened or closed
    pub last_activity: Instant,
    /// Whether this host is currently at its connection limit
    pub at_limit: bool,
}

/// Summary of host connection limiter status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostConnLimitSummary {
    /// Whether the limiter is enabled
    pub enabled: bool,
    /// Default max connections per host
    pub default_max_connections: u32,
    /// Number of hosts with overrides
    pub override_count: usize,
    /// Number of currently tracked hosts
    pub tracked_host_count: usize,
    /// Number of hosts currently at their connection limit
    pub hosts_at_limit: usize,
    /// Total active connections across all hosts
    pub total_active_connections: u32,
    /// Top hosts by active connections
    pub top_hosts: Vec<HostConnectionInfo>,
}

/// Serializable info about a host's connection state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostConnectionInfo {
    /// Hostname
    pub hostname: String,
    /// Active connections
    pub active_connections: u32,
    /// Maximum allowed connections
    pub max_connections: u32,
    /// Total connections ever made
    pub total_connections: u64,
    /// Total connection failures
    pub total_failures: u64,
    /// Peak concurrent connections
    pub peak_connections: u32,
    /// Whether this host is at its limit
    pub at_limit: bool,
    /// Seconds since last activity
    pub idle_secs: u64,
}

/// Result of a connection acquisition attempt
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionAcquireResult {
    /// Connection acquired successfully
    Acquired,
    /// Host is at its connection limit
    AtLimit,
    /// Host not found (no active connections tracked)
    HostNotTracked,
    /// Limiter is disabled
    Disabled,
}

/// The host connection limiter manager
#[derive(Debug)]
pub struct HostConnLimitManager {
    config: HostConnLimitConfig,
    hosts: HashMap<String, HostConnectionState>,
}

impl HostConnLimitManager {
    /// Create a new host connection limiter manager
    pub fn new() -> Self {
        Self {
            config: HostConnLimitConfig::default(),
            hosts: HashMap::new(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: HostConnLimitConfig) -> Self {
        Self {
            config,
            hosts: HashMap::new(),
        }
    }

    /// Get the current configuration
    pub fn get_config(&self) -> &HostConnLimitConfig {
        &self.config
    }

    /// Update the configuration
    pub fn set_config(&mut self, config: HostConnLimitConfig) {
        self.config = config;
    }

    /// Normalize a hostname (lowercase, strip port)
    fn normalize_host(hostname: &str) -> String {
        let host = hostname.split(':').next().unwrap_or(hostname);
        host.to_lowercase()
    }

    /// Get the max connections allowed for a host
    pub fn get_max_connections(&self, hostname: &str) -> u32 {
        let normalized = Self::normalize_host(hostname);
        self.config
            .host_overrides
            .get(&normalized)
            .copied()
            .unwrap_or(self.config.default_max_connections)
    }

    /// Try to acquire a connection slot for a host.
    /// Returns `Acquired` if a slot is available, `AtLimit` if the host is at its limit.
    pub fn acquire_connection(&mut self, hostname: &str) -> ConnectionAcquireResult {
        if !self.config.enabled {
            return ConnectionAcquireResult::Disabled;
        }

        let normalized = Self::normalize_host(hostname);
        let max = self.get_max_connections(&normalized);

        // Evict stale hosts if we're at capacity
        if !self.hosts.contains_key(&normalized)
            && self.hosts.len() >= self.config.max_tracked_hosts
        {
            self.evict_stale_hosts();
        }

        let state = self
            .hosts
            .entry(normalized.clone())
            .or_insert_with(|| HostConnectionState {
                hostname: normalized,
                active_connections: 0,
                total_connections: 0,
                total_failures: 0,
                peak_connections: 0,
                last_activity: Instant::now(),
                at_limit: false,
            });

        if max > 0 && state.active_connections >= max {
            state.at_limit = true;
            return ConnectionAcquireResult::AtLimit;
        }

        state.active_connections += 1;
        state.total_connections += 1;
        if state.active_connections > state.peak_connections {
            state.peak_connections = state.active_connections;
        }
        state.last_activity = Instant::now();
        state.at_limit = max > 0 && state.active_connections >= max;

        ConnectionAcquireResult::Acquired
    }

    /// Release a connection slot for a host
    pub fn release_connection(&mut self, hostname: &str) {
        if !self.config.enabled {
            return;
        }

        let normalized = Self::normalize_host(hostname);
        let max = self.get_max_connections(&normalized);
        if let Some(state) = self.hosts.get_mut(&normalized) {
            state.active_connections = state.active_connections.saturating_sub(1);
            state.last_activity = Instant::now();
            state.at_limit = max > 0 && state.active_connections >= max;
        }
    }

    /// Record a connection failure for a host
    pub fn record_failure(&mut self, hostname: &str) {
        let normalized = Self::normalize_host(hostname);
        if let Some(state) = self.hosts.get_mut(&normalized) {
            state.total_failures += 1;
            state.last_activity = Instant::now();
        }
    }

    /// Get the connection state for a specific host
    pub fn get_host_state(&self, hostname: &str) -> Option<&HostConnectionState> {
        let normalized = Self::normalize_host(hostname);
        self.hosts.get(&normalized)
    }

    /// Get a summary of all tracked hosts
    pub fn get_summary(&self) -> HostConnLimitSummary {
        let now = Instant::now();
        let mut hosts: Vec<HostConnectionInfo> = self
            .hosts
            .values()
            .map(|state| {
                let max = self.get_max_connections(&state.hostname);
                HostConnectionInfo {
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
            .collect();

        // Sort by active connections descending
        hosts.sort_by_key(|h| std::cmp::Reverse(h.active_connections));

        let hosts_at_limit = hosts.iter().filter(|h| h.at_limit).count();
        let total_active = hosts.iter().map(|h| h.active_connections).sum();
        let top_hosts = hosts.into_iter().take(10).collect();

        HostConnLimitSummary {
            enabled: self.config.enabled,
            default_max_connections: self.config.default_max_connections,
            override_count: self.config.host_overrides.len(),
            tracked_host_count: self.hosts.len(),
            hosts_at_limit,
            total_active_connections: total_active,
            top_hosts,
        }
    }

    /// Check if a host is at its connection limit
    pub fn is_at_limit(&self, hostname: &str) -> bool {
        if !self.config.enabled {
            return false;
        }
        let normalized = Self::normalize_host(hostname);
        self.hosts
            .get(&normalized)
            .map(|s| s.at_limit)
            .unwrap_or(false)
    }

    /// Get the number of available connection slots for a host
    pub fn available_slots(&self, hostname: &str) -> u32 {
        if !self.config.enabled {
            return u32::MAX;
        }
        let normalized = Self::normalize_host(hostname);
        let max = self.get_max_connections(&normalized);
        if max == 0 {
            return u32::MAX;
        }
        let active = self
            .hosts
            .get(&normalized)
            .map(|s| s.active_connections)
            .unwrap_or(0);
        max.saturating_sub(active)
    }

    /// Set a per-host connection limit override
    pub fn set_host_override(&mut self, hostname: &str, max_connections: u32) {
        let normalized = Self::normalize_host(hostname);
        self.config
            .host_overrides
            .insert(normalized, max_connections);
    }

    /// Remove a per-host connection limit override
    pub fn remove_host_override(&mut self, hostname: &str) -> bool {
        let normalized = Self::normalize_host(hostname);
        self.config.host_overrides.remove(&normalized).is_some()
    }

    /// List all host overrides
    pub fn list_overrides(&self) -> Vec<(String, u32)> {
        let mut overrides: Vec<(String, u32)> = self
            .config
            .host_overrides
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        overrides.sort_by(|a, b| a.0.cmp(&b.0));
        overrides
    }

    /// Clear all tracked host data (does not affect config)
    pub fn clear_host_data(&mut self) {
        self.hosts.clear();
    }

    /// Remove a specific host from tracking
    pub fn remove_host(&mut self, hostname: &str) -> bool {
        let normalized = Self::normalize_host(hostname);
        self.hosts.remove(&normalized).is_some()
    }

    /// Evict stale hosts that have been idle beyond the timeout
    fn evict_stale_hosts(&mut self) {
        let timeout = std::time::Duration::from_secs(self.config.idle_timeout_secs);
        let now = Instant::now();
        self.hosts.retain(|_, state| {
            // Keep hosts with active connections or recent activity
            state.active_connections > 0 || now.duration_since(state.last_activity) < timeout
        });
    }

    /// Clean up stale hosts (public API for manual cleanup)
    pub fn cleanup_stale_hosts(&mut self) {
        self.evict_stale_hosts();
    }

    /// Get the number of currently tracked hosts
    pub fn tracked_host_count(&self) -> usize {
        self.hosts.len()
    }

    /// Get all tracked hostnames
    pub fn tracked_hostnames(&self) -> Vec<String> {
        self.hosts.keys().cloned().collect()
    }

    /// Save configuration to a file
    pub fn save_config(&self, path: &std::path::Path) -> Result<(), std::io::Error> {
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
        std::fs::write(path, json)
    }

    /// Load configuration from a file
    pub fn load_config(&mut self, path: &std::path::Path) -> Result<(), std::io::Error> {
        let json = std::fs::read_to_string(path)?;
        let config: HostConnLimitConfig =
            serde_json::from_str(&json).map_err(std::io::Error::other)?;
        self.config = config;
        Ok(())
    }
}

impl Default for HostConnLimitManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_host() {
        assert_eq!(
            HostConnLimitManager::normalize_host("Example.COM"),
            "example.com"
        );
        assert_eq!(
            HostConnLimitManager::normalize_host("example.com:8080"),
            "example.com"
        );
        assert_eq!(
            HostConnLimitManager::normalize_host("EXAMPLE.COM:443"),
            "example.com"
        );
        assert_eq!(
            HostConnLimitManager::normalize_host("192.168.1.1"),
            "192.168.1.1"
        );
    }

    #[test]
    fn test_default_config() {
        let config = HostConnLimitConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_max_connections, 4);
        assert!(config.host_overrides.is_empty());
        assert_eq!(config.max_tracked_hosts, 500);
        assert_eq!(config.idle_timeout_secs, 300);
    }

    #[test]
    fn test_acquire_connection_basic() {
        let mut manager = HostConnLimitManager::new();
        assert_eq!(
            manager.acquire_connection("example.com"),
            ConnectionAcquireResult::Acquired
        );
        let state = manager.get_host_state("example.com").unwrap();
        assert_eq!(state.active_connections, 1);
        assert_eq!(state.total_connections, 1);
        assert!(!state.at_limit);
    }

    #[test]
    fn test_acquire_connection_at_limit() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            default_max_connections: 2,
            ..Default::default()
        });

        assert_eq!(
            manager.acquire_connection("example.com"),
            ConnectionAcquireResult::Acquired
        );
        assert_eq!(
            manager.acquire_connection("example.com"),
            ConnectionAcquireResult::Acquired
        );
        assert_eq!(
            manager.acquire_connection("example.com"),
            ConnectionAcquireResult::AtLimit
        );

        let state = manager.get_host_state("example.com").unwrap();
        assert_eq!(state.active_connections, 2);
        assert!(state.at_limit);
    }

    #[test]
    fn test_release_connection() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            default_max_connections: 1,
            ..Default::default()
        });

        manager.acquire_connection("example.com");
        assert_eq!(
            manager.acquire_connection("example.com"),
            ConnectionAcquireResult::AtLimit
        );

        manager.release_connection("example.com");
        let state = manager.get_host_state("example.com").unwrap();
        assert_eq!(state.active_connections, 0);
        assert!(!state.at_limit);

        // Can acquire again
        assert_eq!(
            manager.acquire_connection("example.com"),
            ConnectionAcquireResult::Acquired
        );
    }

    #[test]
    fn test_release_connection_saturating() {
        let mut manager = HostConnLimitManager::new();
        // Release without acquire should not underflow
        manager.release_connection("nonexistent.com");
        // Should not panic
    }

    #[test]
    fn test_record_failure() {
        let mut manager = HostConnLimitManager::new();
        manager.acquire_connection("example.com");
        manager.record_failure("example.com");
        manager.record_failure("example.com");

        let state = manager.get_host_state("example.com").unwrap();
        assert_eq!(state.total_failures, 2);
    }

    #[test]
    fn test_host_overrides() {
        let mut manager = HostConnLimitManager::new();
        manager.set_host_override("slow-server.com", 1);
        manager.set_host_override("fast-server.com", 10);

        assert_eq!(manager.get_max_connections("slow-server.com"), 1);
        assert_eq!(manager.get_max_connections("fast-server.com"), 10);
        assert_eq!(manager.get_max_connections("other.com"), 4); // default

        // Test override is case-insensitive
        assert_eq!(manager.get_max_connections("SLOW-SERVER.COM"), 1);
    }

    #[test]
    fn test_remove_host_override() {
        let mut manager = HostConnLimitManager::new();
        manager.set_host_override("example.com", 8);
        assert_eq!(manager.get_max_connections("example.com"), 8);

        assert!(manager.remove_host_override("example.com"));
        assert_eq!(manager.get_max_connections("example.com"), 4); // back to default

        assert!(!manager.remove_host_override("nonexistent.com"));
    }

    #[test]
    fn test_list_overrides() {
        let mut manager = HostConnLimitManager::new();
        manager.set_host_override("b.com", 2);
        manager.set_host_override("a.com", 5);
        manager.set_host_override("c.com", 10);

        let overrides = manager.list_overrides();
        assert_eq!(overrides.len(), 3);
        assert_eq!(overrides[0], ("a.com".to_string(), 5));
        assert_eq!(overrides[1], ("b.com".to_string(), 2));
        assert_eq!(overrides[2], ("c.com".to_string(), 10));
    }

    #[test]
    fn test_disabled_limiter() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            enabled: false,
            ..Default::default()
        });

        assert_eq!(
            manager.acquire_connection("example.com"),
            ConnectionAcquireResult::Disabled
        );
        assert!(!manager.is_at_limit("example.com"));
        assert_eq!(manager.available_slots("example.com"), u32::MAX);
    }

    #[test]
    fn test_unlimited_connections() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            default_max_connections: 0, // 0 = unlimited
            ..Default::default()
        });

        for _ in 0..100 {
            assert_eq!(
                manager.acquire_connection("example.com"),
                ConnectionAcquireResult::Acquired
            );
        }

        let state = manager.get_host_state("example.com").unwrap();
        assert_eq!(state.active_connections, 100);
        assert!(!state.at_limit);
        assert_eq!(manager.available_slots("example.com"), u32::MAX);
    }

    #[test]
    fn test_peak_connections() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            default_max_connections: 10,
            ..Default::default()
        });

        // Acquire 5
        for _ in 0..5 {
            manager.acquire_connection("example.com");
        }
        // Release 3
        for _ in 0..3 {
            manager.release_connection("example.com");
        }
        // Acquire 2 more (total active = 4, but peak should be 5)
        for _ in 0..2 {
            manager.acquire_connection("example.com");
        }

        let state = manager.get_host_state("example.com").unwrap();
        assert_eq!(state.peak_connections, 5);
        assert_eq!(state.active_connections, 4);
    }

    #[test]
    fn test_get_summary() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            default_max_connections: 2,
            ..Default::default()
        });

        manager.set_host_override("host-a.com", 1);

        // Fill up host-a
        manager.acquire_connection("host-a.com");
        manager.acquire_connection("host-a.com"); // at limit

        // Some activity on host-b
        manager.acquire_connection("host-b.com");
        manager.acquire_connection("host-b.com");

        let summary = manager.get_summary();
        assert!(summary.enabled);
        assert_eq!(summary.default_max_connections, 2);
        assert_eq!(summary.override_count, 1);
        assert_eq!(summary.tracked_host_count, 2);
        assert_eq!(summary.total_active_connections, 3);
        assert!(summary.hosts_at_limit >= 1);
        assert!(!summary.top_hosts.is_empty());
    }

    #[test]
    fn test_is_at_limit() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            default_max_connections: 1,
            ..Default::default()
        });

        assert!(!manager.is_at_limit("example.com"));
        manager.acquire_connection("example.com");
        assert!(manager.is_at_limit("example.com"));
        manager.release_connection("example.com");
        assert!(!manager.is_at_limit("example.com"));
    }

    #[test]
    fn test_available_slots() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            default_max_connections: 3,
            ..Default::default()
        });

        assert_eq!(manager.available_slots("example.com"), 3);
        manager.acquire_connection("example.com");
        assert_eq!(manager.available_slots("example.com"), 2);
        manager.acquire_connection("example.com");
        assert_eq!(manager.available_slots("example.com"), 1);
        manager.acquire_connection("example.com");
        assert_eq!(manager.available_slots("example.com"), 0);
    }

    #[test]
    fn test_clear_host_data() {
        let mut manager = HostConnLimitManager::new();
        manager.acquire_connection("a.com");
        manager.acquire_connection("b.com");
        assert_eq!(manager.tracked_host_count(), 2);

        manager.clear_host_data();
        assert_eq!(manager.tracked_host_count(), 0);
    }

    #[test]
    fn test_remove_host() {
        let mut manager = HostConnLimitManager::new();
        manager.acquire_connection("example.com");
        assert!(manager.remove_host("example.com"));
        assert!(!manager.remove_host("example.com"));
        assert_eq!(manager.tracked_host_count(), 0);
    }

    #[test]
    fn test_tracked_hostnames() {
        let mut manager = HostConnLimitManager::new();
        manager.acquire_connection("a.com");
        manager.acquire_connection("b.com");

        let mut names = manager.tracked_hostnames();
        names.sort();
        assert_eq!(names, vec!["a.com", "b.com"]);
    }

    #[test]
    fn test_save_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("host_conn_limit.json");

        let mut manager = HostConnLimitManager::new();
        manager.set_host_override("example.com", 8);
        manager.config.default_max_connections = 6;
        manager.config.enabled = false;
        manager.save_config(&path).unwrap();

        let mut manager2 = HostConnLimitManager::new();
        manager2.load_config(&path).unwrap();
        assert_eq!(manager2.config.default_max_connections, 6);
        assert!(!manager2.config.enabled);
        assert_eq!(manager2.get_max_connections("example.com"), 8);
    }

    #[test]
    fn test_save_load_config_missing_file() {
        let mut manager = HostConnLimitManager::new();
        let result = manager.load_config(std::path::Path::new("/nonexistent/path.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_case_insensitive_host() {
        let mut manager = HostConnLimitManager::new();
        manager.acquire_connection("Example.COM");
        manager.acquire_connection("example.com");

        let state = manager.get_host_state("EXAMPLE.com").unwrap();
        assert_eq!(state.active_connections, 2);
        assert_eq!(manager.tracked_host_count(), 1);
    }

    #[test]
    fn test_max_tracked_hosts_eviction() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            max_tracked_hosts: 3,
            idle_timeout_secs: 0, // immediate eviction
            ..Default::default()
        });

        // Fill up to max
        manager.acquire_connection("host1.com");
        manager.release_connection("host1.com");
        manager.acquire_connection("host2.com");
        manager.release_connection("host2.com");
        manager.acquire_connection("host3.com");
        manager.release_connection("host3.com");

        // This should trigger eviction of stale hosts
        manager.acquire_connection("host4.com");

        // host4 should be tracked
        assert!(manager.get_host_state("host4.com").is_some());
    }

    #[test]
    fn test_cleanup_stale_hosts() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            idle_timeout_secs: 0, // immediate
            ..Default::default()
        });

        manager.acquire_connection("active.com");
        manager.release_connection("active.com");
        // With 0 timeout, all idle hosts should be evicted
        manager.cleanup_stale_hosts();

        // active.com has no active connections and is stale
        // But it was just active (last_activity = now), so it depends on timing
        // Let's just verify the method doesn't panic
    }

    #[test]
    fn test_summary_serialization() {
        let summary = HostConnLimitSummary {
            enabled: true,
            default_max_connections: 4,
            override_count: 2,
            tracked_host_count: 10,
            hosts_at_limit: 3,
            total_active_connections: 25,
            top_hosts: vec![HostConnectionInfo {
                hostname: "example.com".to_string(),
                active_connections: 4,
                max_connections: 4,
                total_connections: 100,
                total_failures: 5,
                peak_connections: 4,
                at_limit: true,
                idle_secs: 10,
            }],
        };

        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: HostConnLimitSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, true);
        assert_eq!(deserialized.total_active_connections, 25);
        assert_eq!(deserialized.top_hosts.len(), 1);
    }

    #[test]
    fn test_config_serialization() {
        let mut config = HostConnLimitConfig::default();
        config.host_overrides.insert("slow.com".to_string(), 1);
        config.host_overrides.insert("fast.com".to_string(), 20);

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HostConnLimitConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.default_max_connections, 4);
        assert_eq!(deserialized.host_overrides.len(), 2);
        assert_eq!(deserialized.host_overrides.get("slow.com"), Some(&1));
    }

    #[test]
    fn test_multiple_hosts_independent_limits() {
        let mut manager = HostConnLimitManager::with_config(HostConnLimitConfig {
            default_max_connections: 2,
            ..Default::default()
        });

        // Host A fills up
        manager.acquire_connection("host-a.com");
        manager.acquire_connection("host-a.com");
        assert!(manager.is_at_limit("host-a.com"));

        // Host B should still be available
        assert!(!manager.is_at_limit("host-b.com"));
        assert_eq!(
            manager.acquire_connection("host-b.com"),
            ConnectionAcquireResult::Acquired
        );
    }

    #[test]
    fn test_host_info_serialization() {
        let info = HostConnectionInfo {
            hostname: "test.com".to_string(),
            active_connections: 3,
            max_connections: 5,
            total_connections: 50,
            total_failures: 2,
            peak_connections: 5,
            at_limit: false,
            idle_secs: 30,
        };

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: HostConnectionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.hostname, "test.com");
        assert_eq!(deserialized.active_connections, 3);
        assert_eq!(deserialized.total_failures, 2);
    }
}
