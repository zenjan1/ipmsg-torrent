//! Download Speed Burst Mode
//!
//! Temporarily boost a task's download speed limit for a configurable duration,
//! then automatically revert to the original limit. Useful for quickly finishing
//! small downloads or taking advantage of temporary bandwidth availability.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for speed burst feature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedBurstConfig {
    /// Whether speed burst feature is enabled
    pub enabled: bool,
    /// Default burst duration in seconds (if not specified per-task)
    pub default_duration_secs: u64,
    /// Default speed multiplier during burst (e.g., 2.0 = double speed)
    pub default_multiplier: f64,
    /// Maximum allowed burst duration in seconds
    pub max_duration_secs: u64,
    /// Maximum allowed multiplier
    pub max_multiplier: f64,
    /// Maximum concurrent active bursts (0 = unlimited)
    pub max_concurrent_bursts: usize,
}

impl Default for SpeedBurstConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_duration_secs: 300, // 5 minutes
            default_multiplier: 2.0,
            max_duration_secs: 3600, // 1 hour max
            max_multiplier: 10.0,
            max_concurrent_bursts: 0, // unlimited
        }
    }
}

/// State of an active speed burst for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveBurst {
    /// Task ID this burst applies to
    pub task_id: String,
    /// Original speed limit before burst (bytes/sec, None = unlimited)
    pub original_limit: Option<u64>,
    /// Burst speed limit in bytes/sec
    pub burst_limit: u64,
    /// When the burst started
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// When the burst expires
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// Speed multiplier applied
    pub multiplier: f64,
}

impl ActiveBurst {
    /// Check if this burst has expired
    pub fn is_expired(&self) -> bool {
        chrono::Utc::now() >= self.expires_at
    }

    /// Get remaining duration in seconds
    pub fn remaining_secs(&self) -> u64 {
        let now = chrono::Utc::now();
        if now >= self.expires_at {
            0
        } else {
            (self.expires_at - now).num_seconds().max(0) as u64
        }
    }
}

/// Result of attempting to start a burst
#[derive(Debug, Clone)]
pub enum BurstStartResult {
    /// Burst started successfully
    Started(ActiveBurst),
    /// Feature is disabled
    Disabled,
    /// Task not found
    TaskNotFound,
    /// Task is not in a downloadable state
    TaskNotActive,
    /// Maximum concurrent bursts reached
    MaxBurstsReached,
    /// Invalid parameters
    InvalidParams(String),
}

/// Result of querying burst status
#[derive(Debug, Clone)]
pub struct BurstStatus {
    /// Currently active bursts
    pub active_bursts: Vec<ActiveBurst>,
    /// Total bursts started since tracking began
    pub total_bursts_started: u64,
    /// Total bursts completed (expired naturally)
    pub total_bursts_completed: u64,
}

/// Manager for speed burst operations
#[derive(Debug)]
pub struct SpeedBurstManager {
    /// Configuration
    config: SpeedBurstConfig,
    /// Active bursts keyed by task_id
    active_bursts: HashMap<String, ActiveBurst>,
    /// Total bursts started
    total_started: u64,
    /// Total bursts completed (expired naturally)
    total_completed: u64,
}

impl SpeedBurstManager {
    /// Create a new manager with default configuration
    pub fn new() -> Self {
        Self {
            config: SpeedBurstConfig::default(),
            active_bursts: HashMap::new(),
            total_started: 0,
            total_completed: 0,
        }
    }

    /// Create a new manager with custom configuration
    pub fn with_config(config: SpeedBurstConfig) -> Self {
        Self {
            config,
            active_bursts: HashMap::new(),
            total_started: 0,
            total_completed: 0,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &SpeedBurstConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: SpeedBurstConfig) {
        self.config = config;
    }

    /// Start a speed burst for a task
    ///
    /// # Arguments
    /// * `task_id` - The task to boost
    /// * `current_limit` - Current speed limit (None = unlimited)
    /// * `duration_secs` - Optional custom duration (uses default if None)
    /// * `multiplier` - Optional custom multiplier (uses default if None)
    pub fn start_burst(
        &mut self,
        task_id: &str,
        current_limit: Option<u64>,
        duration_secs: Option<u64>,
        multiplier: Option<f64>,
    ) -> BurstStartResult {
        // Check if feature is enabled
        if !self.config.enabled {
            return BurstStartResult::Disabled;
        }

        // Validate parameters
        let duration = duration_secs.unwrap_or(self.config.default_duration_secs);
        let mult = multiplier.unwrap_or(self.config.default_multiplier);

        if duration == 0 || duration > self.config.max_duration_secs {
            return BurstStartResult::InvalidParams(format!(
                "Duration must be between 1 and {} seconds",
                self.config.max_duration_secs
            ));
        }

        if mult <= 1.0 || mult > self.config.max_multiplier {
            return BurstStartResult::InvalidParams(format!(
                "Multiplier must be between 1.0 and {}",
                self.config.max_multiplier
            ));
        }

        // Check concurrent burst limit
        if self.config.max_concurrent_bursts > 0
            && self.active_bursts.len() >= self.config.max_concurrent_bursts
            && !self.active_bursts.contains_key(task_id)
        {
            return BurstStartResult::MaxBurstsReached;
        }

        // Calculate burst limit
        let burst_limit = match current_limit {
            Some(limit) if limit > 0 => (limit as f64 * mult) as u64,
            _ => {
                // If unlimited, we can't really "burst" - but we track it anyway
                // The burst will be a no-op in terms of rate limiting
                0
            }
        };

        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: task_id.to_string(),
            original_limit: current_limit,
            burst_limit,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(duration as i64),
            multiplier: mult,
        };

        self.active_bursts
            .insert(task_id.to_string(), burst.clone());
        self.total_started += 1;

        BurstStartResult::Started(burst)
    }

    /// Stop an active burst for a task
    pub fn stop_burst(&mut self, task_id: &str) -> Option<ActiveBurst> {
        self.active_bursts.remove(task_id)
    }

    /// Get the current burst limit for a task (if any)
    pub fn get_burst_limit(&self, task_id: &str) -> Option<u64> {
        self.active_bursts.get(task_id).and_then(|b| {
            if b.is_expired() {
                None
            } else {
                Some(b.burst_limit)
            }
        })
    }

    /// Check if a task has an active burst
    pub fn has_active_burst(&self, task_id: &str) -> bool {
        self.active_bursts
            .get(task_id)
            .map(|b| !b.is_expired())
            .unwrap_or(false)
    }

    /// Get active burst info for a task
    pub fn get_active_burst(&self, task_id: &str) -> Option<&ActiveBurst> {
        self.active_bursts.get(task_id).filter(|b| !b.is_expired())
    }

    /// Process expired bursts and return task IDs that need to revert
    pub fn process_expired(&mut self) -> Vec<(String, Option<u64>)> {
        let expired: Vec<String> = self
            .active_bursts
            .iter()
            .filter(|(_, b)| b.is_expired())
            .map(|(id, _)| id.clone())
            .collect();

        let mut reverted = Vec::new();
        for task_id in expired {
            if let Some(burst) = self.active_bursts.remove(&task_id) {
                self.total_completed += 1;
                reverted.push((task_id, burst.original_limit));
            }
        }
        reverted
    }

    /// Get status of all active bursts
    pub fn status(&self) -> BurstStatus {
        let active: Vec<ActiveBurst> = self
            .active_bursts
            .values()
            .filter(|b| !b.is_expired())
            .cloned()
            .collect();

        BurstStatus {
            active_bursts: active,
            total_bursts_started: self.total_started,
            total_bursts_completed: self.total_completed,
        }
    }

    /// Clear all active bursts (for shutdown or reset)
    pub fn clear_all(&mut self) -> Vec<(String, Option<u64>)> {
        let reverted: Vec<(String, Option<u64>)> = self
            .active_bursts
            .drain()
            .map(|(id, b)| (id, b.original_limit))
            .collect();
        reverted
    }

    /// Save configuration to file
    pub fn save_config(&self, path: &std::path::Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self.config).map_err(std::io::Error::other)?;
        // Atomic write: write to temp file then rename
        let tmp_path = path.with_extension("json.tmp");
        std::fs::write(&tmp_path, json)?;
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Load configuration from file
    pub fn load_config(path: &std::path::Path) -> std::io::Result<SpeedBurstConfig> {
        let contents = std::fs::read_to_string(path)?;
        serde_json::from_str(&contents)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

impl Default for SpeedBurstManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SpeedBurstConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_duration_secs, 300);
        assert_eq!(config.default_multiplier, 2.0);
        assert_eq!(config.max_duration_secs, 3600);
        assert_eq!(config.max_multiplier, 10.0);
        assert_eq!(config.max_concurrent_bursts, 0);
    }

    #[test]
    fn test_config_serialization() {
        let config = SpeedBurstConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SpeedBurstConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.default_duration_secs, config.default_duration_secs);
        assert_eq!(loaded.default_multiplier, config.default_multiplier);
    }

    #[test]
    fn test_start_burst_basic() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));

        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.task_id, "task-1");
                assert_eq!(burst.original_limit, Some(1_000_000));
                assert_eq!(burst.burst_limit, 2_000_000);
                assert_eq!(burst.multiplier, 2.0);
                assert!(!burst.is_expired());
            }
            _ => panic!("Expected Started, got {:?}", result),
        }

        assert!(manager.has_active_burst("task-1"));
        assert_eq!(manager.total_started, 1);
    }

    #[test]
    fn test_start_burst_default_params() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("task-1", Some(500_000), None, None);

        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.burst_limit, 1_000_000); // 500k * 2.0
            }
            _ => panic!("Expected Started"),
        }
    }

    #[test]
    fn test_start_burst_unlimited_original() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("task-1", None, Some(60), Some(2.0));

        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.original_limit, None);
                assert_eq!(burst.burst_limit, 0); // Can't burst unlimited
            }
            _ => panic!("Expected Started"),
        }
    }

    #[test]
    fn test_start_burst_disabled() {
        let mut manager = SpeedBurstManager::new();
        manager.config.enabled = false;

        let result = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        assert!(matches!(result, BurstStartResult::Disabled));
    }

    #[test]
    fn test_start_burst_invalid_duration() {
        let mut manager = SpeedBurstManager::new();

        // Duration too long
        let result = manager.start_burst("task-1", Some(1_000_000), Some(99999), Some(2.0));
        assert!(matches!(result, BurstStartResult::InvalidParams(_)));

        // Duration zero
        let result = manager.start_burst("task-1", Some(1_000_000), Some(0), Some(2.0));
        assert!(matches!(result, BurstStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_burst_invalid_multiplier() {
        let mut manager = SpeedBurstManager::new();

        // Multiplier <= 1.0
        let result = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(1.0));
        assert!(matches!(result, BurstStartResult::InvalidParams(_)));

        // Multiplier too high
        let result = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(100.0));
        assert!(matches!(result, BurstStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_start_burst_max_concurrent() {
        let mut manager = SpeedBurstManager::new();
        manager.config.max_concurrent_bursts = 2;

        // First two should succeed
        let r1 = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        assert!(matches!(r1, BurstStartResult::Started(_)));

        let r2 = manager.start_burst("task-2", Some(1_000_000), Some(60), Some(2.0));
        assert!(matches!(r2, BurstStartResult::Started(_)));

        // Third should fail
        let r3 = manager.start_burst("task-3", Some(1_000_000), Some(60), Some(2.0));
        assert!(matches!(r3, BurstStartResult::MaxBurstsReached));

        // But replacing an existing burst should work
        let r4 = manager.start_burst("task-1", Some(2_000_000), Some(60), Some(3.0));
        assert!(matches!(r4, BurstStartResult::Started(_)));
    }

    #[test]
    fn test_stop_burst() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));

        let stopped = manager.stop_burst("task-1");
        assert!(stopped.is_some());
        assert_eq!(stopped.unwrap().original_limit, Some(1_000_000));
        assert!(!manager.has_active_burst("task-1"));
    }

    #[test]
    fn test_stop_burst_nonexistent() {
        let mut manager = SpeedBurstManager::new();
        let stopped = manager.stop_burst("nonexistent");
        assert!(stopped.is_none());
    }

    #[test]
    fn test_get_burst_limit() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));

        assert_eq!(manager.get_burst_limit("task-1"), Some(2_000_000));
        assert_eq!(manager.get_burst_limit("task-2"), None);
    }

    #[test]
    fn test_get_active_burst() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));

        let burst = manager.get_active_burst("task-1");
        assert!(burst.is_some());
        assert_eq!(burst.unwrap().multiplier, 2.0);

        assert!(manager.get_active_burst("nonexistent").is_none());
    }

    #[test]
    fn test_process_expired() {
        let mut manager = SpeedBurstManager::new();

        // Create a burst that expires immediately (1 second duration)
        manager.start_burst("task-1", Some(1_000_000), Some(1), Some(2.0));

        // Manually set expires_at to the past
        if let Some(burst) = manager.active_bursts.get_mut("task-1") {
            burst.expires_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        }

        let reverted = manager.process_expired();
        assert_eq!(reverted.len(), 1);
        assert_eq!(reverted[0].0, "task-1");
        assert_eq!(reverted[0].1, Some(1_000_000));
        assert_eq!(manager.total_completed, 1);
        assert!(!manager.has_active_burst("task-1"));
    }

    #[test]
    fn test_status() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        manager.start_burst("task-2", Some(500_000), Some(120), Some(3.0));

        let status = manager.status();
        assert_eq!(status.active_bursts.len(), 2);
        assert_eq!(status.total_bursts_started, 2);
        assert_eq!(status.total_bursts_completed, 0);
    }

    #[test]
    fn test_clear_all() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        manager.start_burst("task-2", Some(500_000), Some(120), Some(3.0));

        let reverted = manager.clear_all();
        assert_eq!(reverted.len(), 2);
        assert!(manager.active_bursts.is_empty());
    }

    #[test]
    fn test_active_burst_remaining_secs() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(60),
            multiplier: 2.0,
        };

        // Should have ~60 seconds remaining
        let remaining = burst.remaining_secs();
        assert!(remaining >= 59 && remaining <= 61);
        assert!(!burst.is_expired());
    }

    #[test]
    fn test_active_burst_expired() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now - chrono::Duration::seconds(120),
            expires_at: now - chrono::Duration::seconds(60),
            multiplier: 2.0,
        };

        assert!(burst.is_expired());
        assert_eq!(burst.remaining_secs(), 0);
    }

    #[test]
    fn test_save_load_config() {
        let manager = SpeedBurstManager::new();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("speed_burst_config_test.json");

        manager.save_config(&config_path).unwrap();
        let loaded = SpeedBurstManager::load_config(&config_path).unwrap();

        assert_eq!(
            loaded.default_duration_secs,
            manager.config.default_duration_secs
        );
        assert_eq!(loaded.default_multiplier, manager.config.default_multiplier);

        // Cleanup
        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_load_config_missing_file() {
        let result = SpeedBurstManager::load_config(std::path::Path::new("/nonexistent/path.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_set_config() {
        let mut manager = SpeedBurstManager::new();
        let new_config = SpeedBurstConfig {
            enabled: false,
            default_duration_secs: 600,
            default_multiplier: 3.0,
            max_duration_secs: 7200,
            max_multiplier: 20.0,
            max_concurrent_bursts: 5,
        };

        manager.set_config(new_config.clone());
        assert_eq!(manager.config().default_duration_secs, 600);
        assert_eq!(manager.config().default_multiplier, 3.0);
        assert_eq!(manager.config().max_concurrent_bursts, 5);
    }

    #[test]
    fn test_burst_replaces_existing() {
        let mut manager = SpeedBurstManager::new();

        // Start first burst
        let r1 = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        assert!(matches!(r1, BurstStartResult::Started(_)));

        // Start another burst for same task (should replace)
        let r2 = manager.start_burst("task-1", Some(2_000_000), Some(120), Some(3.0));
        match r2 {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.original_limit, Some(2_000_000));
                assert_eq!(burst.burst_limit, 6_000_000);
                assert_eq!(burst.multiplier, 3.0);
            }
            _ => panic!("Expected Started"),
        }

        // Should only count as 2 starts total
        assert_eq!(manager.total_started, 2);
    }

    #[test]
    fn test_burst_limit_calculation_precision() {
        let mut manager = SpeedBurstManager::new();

        // Test with odd numbers
        let result = manager.start_burst("task-1", Some(1_234_567), Some(60), Some(2.5));
        match result {
            BurstStartResult::Started(burst) => {
                // 1_234_567 * 2.5 = 3_086_417.5 -> truncated to 3_086_417
                assert_eq!(burst.burst_limit, 3_086_417);
            }
            _ => panic!("Expected Started"),
        }
    }

    // ========== Phase 254: Comprehensive Test Coverage ==========

    // SpeedBurstConfig serde tests
    #[test]
    fn test_config_serde_roundtrip() {
        let config = SpeedBurstConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SpeedBurstConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.default_duration_secs, config.default_duration_secs);
        assert_eq!(loaded.default_multiplier, config.default_multiplier);
        assert_eq!(loaded.max_duration_secs, config.max_duration_secs);
        assert_eq!(loaded.max_multiplier, config.max_multiplier);
        assert_eq!(loaded.max_concurrent_bursts, config.max_concurrent_bursts);
    }

    #[test]
    fn test_config_serde_custom_values() {
        let config = SpeedBurstConfig {
            enabled: false,
            default_duration_secs: 120,
            default_multiplier: 5.0,
            max_duration_secs: 1800,
            max_multiplier: 20.0,
            max_concurrent_bursts: 10,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SpeedBurstConfig = serde_json::from_str(&json).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.default_duration_secs, 120);
        assert_eq!(loaded.default_multiplier, 5.0);
        assert_eq!(loaded.max_duration_secs, 1800);
        assert_eq!(loaded.max_multiplier, 20.0);
        assert_eq!(loaded.max_concurrent_bursts, 10);
    }

    #[test]
    fn test_config_serde_pretty_json() {
        let config = SpeedBurstConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let loaded: SpeedBurstConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.default_duration_secs, config.default_duration_secs);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"default_duration_secs":300,"default_multiplier":2.0,"max_duration_secs":3600,"max_multiplier":10.0,"max_concurrent_bursts":0,"extra_field":"ignored"}"#;
        let loaded: SpeedBurstConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.default_duration_secs, 300);
    }

    #[test]
    fn test_config_clone() {
        let config = SpeedBurstConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.default_duration_secs, config.default_duration_secs);
        assert_eq!(cloned.default_multiplier, config.default_multiplier);
    }

    #[test]
    fn test_config_clone_independence() {
        let mut config = SpeedBurstConfig::default();
        let mut cloned = config.clone();
        cloned.enabled = false;
        cloned.default_duration_secs = 999;
        assert!(config.enabled);
        assert_eq!(config.default_duration_secs, 300);
    }

    #[test]
    fn test_config_debug() {
        let config = SpeedBurstConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("SpeedBurstConfig"));
        assert!(debug.contains("enabled"));
        assert!(debug.contains("default_duration_secs"));
    }

    // SpeedBurstConfig boundary tests
    #[test]
    fn test_config_zero_max_concurrent() {
        let config = SpeedBurstConfig {
            max_concurrent_bursts: 0,
            ..Default::default()
        };
        assert_eq!(config.max_concurrent_bursts, 0);
    }

    #[test]
    fn test_config_boundary_values() {
        let config = SpeedBurstConfig {
            enabled: true,
            default_duration_secs: 1,
            default_multiplier: 1.01,
            max_duration_secs: u64::MAX,
            max_multiplier: f64::MAX,
            max_concurrent_bursts: usize::MAX,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: SpeedBurstConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.default_duration_secs, 1);
        assert_eq!(loaded.max_duration_secs, u64::MAX);
    }

    // ActiveBurst tests
    #[test]
    fn test_active_burst_clone() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(60),
            multiplier: 2.0,
        };
        let cloned = burst.clone();
        assert_eq!(cloned.task_id, burst.task_id);
        assert_eq!(cloned.original_limit, burst.original_limit);
        assert_eq!(cloned.burst_limit, burst.burst_limit);
        assert_eq!(cloned.multiplier, burst.multiplier);
    }

    #[test]
    fn test_active_burst_clone_independence() {
        let now = chrono::Utc::now();
        let mut burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(60),
            multiplier: 2.0,
        };
        let cloned = burst.clone();
        burst.task_id = "modified".to_string();
        burst.original_limit = None;
        assert_eq!(cloned.task_id, "test");
        assert_eq!(cloned.original_limit, Some(1_000_000));
    }

    #[test]
    fn test_active_burst_debug() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test-task".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(60),
            multiplier: 2.0,
        };
        let debug = format!("{:?}", burst);
        assert!(debug.contains("ActiveBurst"));
        assert!(debug.contains("test-task"));
        assert!(debug.contains("1000000"));
        assert!(debug.contains("2000000"));
    }

    #[test]
    fn test_active_burst_serde_roundtrip() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(60),
            multiplier: 2.0,
        };
        let json = serde_json::to_string(&burst).unwrap();
        let loaded: ActiveBurst = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.task_id, burst.task_id);
        assert_eq!(loaded.original_limit, burst.original_limit);
        assert_eq!(loaded.burst_limit, burst.burst_limit);
        assert_eq!(loaded.multiplier, burst.multiplier);
    }

    #[test]
    fn test_active_burst_unicode_task_id() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "任务-测试-🚀".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(60),
            multiplier: 2.0,
        };
        assert_eq!(burst.task_id, "任务-测试-🚀");
        let json = serde_json::to_string(&burst).unwrap();
        let loaded: ActiveBurst = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.task_id, "任务-测试-🚀");
    }

    #[test]
    fn test_active_burst_remaining_secs_boundary() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(1),
            multiplier: 2.0,
        };
        let remaining = burst.remaining_secs();
        assert!(remaining <= 1);
    }

    #[test]
    fn test_active_burst_remaining_secs_large_duration() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::hours(24),
            multiplier: 2.0,
        };
        let remaining = burst.remaining_secs();
        assert!(remaining >= 86399 && remaining <= 86401);
    }

    // BurstStartResult tests
    #[test]
    fn test_burst_start_result_debug() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(60),
            multiplier: 2.0,
        };
        let result = BurstStartResult::Started(burst);
        let debug = format!("{:?}", result);
        assert!(debug.contains("Started"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn test_burst_start_result_disabled() {
        let result = BurstStartResult::Disabled;
        let debug = format!("{:?}", result);
        assert!(debug.contains("Disabled"));
    }

    #[test]
    fn test_burst_start_result_task_not_found() {
        let result = BurstStartResult::TaskNotFound;
        let debug = format!("{:?}", result);
        assert!(debug.contains("TaskNotFound"));
    }

    #[test]
    fn test_burst_start_result_task_not_active() {
        let result = BurstStartResult::TaskNotActive;
        let debug = format!("{:?}", result);
        assert!(debug.contains("TaskNotActive"));
    }

    #[test]
    fn test_burst_start_result_max_bursts_reached() {
        let result = BurstStartResult::MaxBurstsReached;
        let debug = format!("{:?}", result);
        assert!(debug.contains("MaxBurstsReached"));
    }

    #[test]
    fn test_burst_start_result_invalid_params() {
        let result = BurstStartResult::InvalidParams("test error".to_string());
        let debug = format!("{:?}", result);
        assert!(debug.contains("InvalidParams"));
        assert!(debug.contains("test error"));
    }

    // BurstStatus tests
    #[test]
    fn test_burst_status_debug() {
        let status = BurstStatus {
            active_bursts: vec![],
            total_bursts_started: 10,
            total_bursts_completed: 5,
        };
        let debug = format!("{:?}", status);
        assert!(debug.contains("BurstStatus"));
        assert!(debug.contains("10"));
        assert!(debug.contains("5"));
    }

    #[test]
    fn test_burst_status_with_active_bursts() {
        let now = chrono::Utc::now();
        let burst = ActiveBurst {
            task_id: "test".to_string(),
            original_limit: Some(1_000_000),
            burst_limit: 2_000_000,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(60),
            multiplier: 2.0,
        };
        let status = BurstStatus {
            active_bursts: vec![burst],
            total_bursts_started: 10,
            total_bursts_completed: 5,
        };
        assert_eq!(status.active_bursts.len(), 1);
        assert_eq!(status.total_bursts_started, 10);
        assert_eq!(status.total_bursts_completed, 5);
    }

    // SpeedBurstManager tests
    #[test]
    fn test_manager_new() {
        let manager = SpeedBurstManager::new();
        assert!(manager.config().enabled);
        assert_eq!(manager.config().default_duration_secs, 300);
        assert_eq!(manager.total_started, 0);
        assert_eq!(manager.total_completed, 0);
        assert!(manager.active_bursts.is_empty());
    }

    #[test]
    fn test_manager_default_equals_new() {
        let new = SpeedBurstManager::new();
        let default = SpeedBurstManager::default();
        assert_eq!(new.config().enabled, default.config().enabled);
        assert_eq!(
            new.config().default_duration_secs,
            default.config().default_duration_secs
        );
        assert_eq!(new.total_started, default.total_started);
        assert_eq!(new.total_completed, default.total_completed);
    }

    #[test]
    fn test_manager_with_config() {
        let config = SpeedBurstConfig {
            enabled: false,
            default_duration_secs: 120,
            default_multiplier: 3.0,
            max_duration_secs: 600,
            max_multiplier: 5.0,
            max_concurrent_bursts: 2,
        };
        let manager = SpeedBurstManager::with_config(config);
        assert!(!manager.config().enabled);
        assert_eq!(manager.config().default_duration_secs, 120);
        assert_eq!(manager.config().default_multiplier, 3.0);
        assert_eq!(manager.config().max_concurrent_bursts, 2);
    }

    #[test]
    fn test_manager_debug() {
        let manager = SpeedBurstManager::new();
        let debug = format!("{:?}", manager);
        assert!(debug.contains("SpeedBurstManager"));
        assert!(debug.contains("config"));
        assert!(debug.contains("active_bursts"));
    }

    #[test]
    fn test_manager_start_burst_zero_limit() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("task-1", Some(0), Some(60), Some(2.0));
        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.original_limit, Some(0));
                assert_eq!(burst.burst_limit, 0);
            }
            _ => panic!("Expected Started"),
        }
    }

    #[test]
    fn test_manager_start_burst_negative_multiplier_boundary() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(0.5));
        assert!(matches!(result, BurstStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_manager_start_burst_exact_max_multiplier() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(10.0));
        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.multiplier, 10.0);
                assert_eq!(burst.burst_limit, 10_000_000);
            }
            _ => panic!("Expected Started"),
        }
    }

    #[test]
    fn test_manager_start_burst_just_over_max_multiplier() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(10.01));
        assert!(matches!(result, BurstStartResult::InvalidParams(_)));
    }

    #[test]
    fn test_manager_start_burst_exact_max_duration() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("task-1", Some(1_000_000), Some(3600), Some(2.0));
        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.multiplier, 2.0);
            }
            _ => panic!("Expected Started"),
        }
    }

    #[test]
    fn test_manager_start_burst_unicode_task_id() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("任务-中文-🚀", Some(1_000_000), Some(60), Some(2.0));
        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.task_id, "任务-中文-🚀");
            }
            _ => panic!("Expected Started"),
        }
        assert!(manager.has_active_burst("任务-中文-🚀"));
    }

    #[test]
    fn test_manager_start_burst_empty_task_id() {
        let mut manager = SpeedBurstManager::new();
        let result = manager.start_burst("", Some(1_000_000), Some(60), Some(2.0));
        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.task_id, "");
            }
            _ => panic!("Expected Started"),
        }
    }

    #[test]
    fn test_manager_start_burst_very_long_task_id() {
        let mut manager = SpeedBurstManager::new();
        let long_id = "a".repeat(1000);
        let result = manager.start_burst(&long_id, Some(1_000_000), Some(60), Some(2.0));
        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.task_id.len(), 1000);
            }
            _ => panic!("Expected Started"),
        }
    }

    #[test]
    fn test_manager_multiple_bursts_independent() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        manager.start_burst("task-2", Some(2_000_000), Some(120), Some(3.0));
        manager.start_burst("task-3", Some(500_000), Some(30), Some(4.0));

        assert_eq!(manager.active_bursts.len(), 3);
        assert_eq!(manager.total_started, 3);

        assert_eq!(manager.get_burst_limit("task-1"), Some(2_000_000));
        assert_eq!(manager.get_burst_limit("task-2"), Some(6_000_000));
        assert_eq!(manager.get_burst_limit("task-3"), Some(2_000_000));
    }

    #[test]
    fn test_manager_stop_burst_returns_correct_original() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_234_567), Some(60), Some(2.5));

        let stopped = manager.stop_burst("task-1").unwrap();
        assert_eq!(stopped.original_limit, Some(1_234_567));
        assert_eq!(stopped.burst_limit, 3_086_417);
    }

    #[test]
    fn test_manager_process_expired_multiple() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        manager.start_burst("task-2", Some(2_000_000), Some(120), Some(3.0));
        manager.start_burst("task-3", Some(500_000), Some(30), Some(4.0));

        // Expire task-1 and task-3
        if let Some(burst) = manager.active_bursts.get_mut("task-1") {
            burst.expires_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        }
        if let Some(burst) = manager.active_bursts.get_mut("task-3") {
            burst.expires_at = chrono::Utc::now() - chrono::Duration::seconds(5);
        }

        let reverted = manager.process_expired();
        assert_eq!(reverted.len(), 2);
        assert_eq!(manager.total_completed, 2);
        assert_eq!(manager.active_bursts.len(), 1);
        assert!(manager.has_active_burst("task-2"));
        assert!(!manager.has_active_burst("task-1"));
        assert!(!manager.has_active_burst("task-3"));
    }

    #[test]
    fn test_manager_process_expired_none() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        manager.start_burst("task-2", Some(2_000_000), Some(120), Some(3.0));

        let reverted = manager.process_expired();
        assert_eq!(reverted.len(), 0);
        assert_eq!(manager.total_completed, 0);
        assert_eq!(manager.active_bursts.len(), 2);
    }

    #[test]
    fn test_manager_status_filters_expired() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        manager.start_burst("task-2", Some(2_000_000), Some(120), Some(3.0));

        // Expire task-1
        if let Some(burst) = manager.active_bursts.get_mut("task-1") {
            burst.expires_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        }

        let status = manager.status();
        assert_eq!(status.active_bursts.len(), 1);
        assert_eq!(status.active_bursts[0].task_id, "task-2");
        assert_eq!(status.total_bursts_started, 2);
        assert_eq!(status.total_bursts_completed, 0);
    }

    #[test]
    fn test_manager_clear_all_returns_all_originals() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        manager.start_burst("task-2", None, Some(120), Some(3.0));
        manager.start_burst("task-3", Some(500_000), Some(30), Some(4.0));

        let reverted = manager.clear_all();
        assert_eq!(reverted.len(), 3);

        let task1 = reverted.iter().find(|(id, _)| id == "task-1").unwrap();
        assert_eq!(task1.1, Some(1_000_000));

        let task2 = reverted.iter().find(|(id, _)| id == "task-2").unwrap();
        assert_eq!(task2.1, None);

        let task3 = reverted.iter().find(|(id, _)| id == "task-3").unwrap();
        assert_eq!(task3.1, Some(500_000));
    }

    #[test]
    fn test_manager_clear_all_empty() {
        let mut manager = SpeedBurstManager::new();
        let reverted = manager.clear_all();
        assert_eq!(reverted.len(), 0);
    }

    #[test]
    fn test_manager_has_active_burst_expired() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));

        // Expire it
        if let Some(burst) = manager.active_bursts.get_mut("task-1") {
            burst.expires_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        }

        assert!(!manager.has_active_burst("task-1"));
    }

    #[test]
    fn test_manager_get_active_burst_expired() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));

        // Expire it
        if let Some(burst) = manager.active_bursts.get_mut("task-1") {
            burst.expires_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        }

        assert!(manager.get_active_burst("task-1").is_none());
    }

    #[test]
    fn test_manager_get_burst_limit_expired() {
        let mut manager = SpeedBurstManager::new();
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));

        // Expire it
        if let Some(burst) = manager.active_bursts.get_mut("task-1") {
            burst.expires_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        }

        assert_eq!(manager.get_burst_limit("task-1"), None);
    }

    // Persistence tests
    #[test]
    fn test_save_config_creates_file() {
        let manager = SpeedBurstManager::new();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("speed_burst_create_test.json");

        let _ = std::fs::remove_file(&config_path);
        manager.save_config(&config_path).unwrap();

        assert!(config_path.exists());
        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_save_config_no_tmp_leftover() {
        let manager = SpeedBurstManager::new();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("speed_burst_no_tmp_test.json");
        let tmp_path = config_path.with_extension("json.tmp");

        let _ = std::fs::remove_file(&config_path);
        let _ = std::fs::remove_file(&tmp_path);

        manager.save_config(&config_path).unwrap();

        assert!(config_path.exists());
        assert!(!tmp_path.exists());

        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_save_config_overwrites() {
        let mut manager = SpeedBurstManager::new();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("speed_burst_overwrite_test.json");

        manager.save_config(&config_path).unwrap();

        manager.config.enabled = false;
        manager.config.default_duration_secs = 999;
        manager.save_config(&config_path).unwrap();

        let loaded = SpeedBurstManager::load_config(&config_path).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.default_duration_secs, 999);

        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_load_config_corrupted_json() {
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("speed_burst_corrupted_test.json");

        std::fs::write(&config_path, "not valid json").unwrap();
        let result = SpeedBurstManager::load_config(&config_path);
        assert!(result.is_err());

        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_load_config_empty_file() {
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("speed_burst_empty_test.json");

        std::fs::write(&config_path, "").unwrap();
        let result = SpeedBurstManager::load_config(&config_path);
        assert!(result.is_err());

        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_save_load_config_unicode() {
        let mut manager = SpeedBurstManager::new();
        manager.config.enabled = false;
        manager.config.default_duration_secs = 123;

        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("speed_burst_unicode_测试.json");

        manager.save_config(&config_path).unwrap();
        let loaded = SpeedBurstManager::load_config(&config_path).unwrap();

        assert_eq!(loaded.enabled, manager.config.enabled);
        assert_eq!(loaded.default_duration_secs, 123);

        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_save_load_config_pretty_json() {
        let manager = SpeedBurstManager::new();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("speed_burst_pretty_test.json");

        manager.save_config(&config_path).unwrap();

        let contents = std::fs::read_to_string(&config_path).unwrap();
        assert!(contents.contains('\n'));

        let loaded = SpeedBurstManager::load_config(&config_path).unwrap();
        assert_eq!(
            loaded.default_duration_secs,
            manager.config.default_duration_secs
        );

        let _ = std::fs::remove_file(&config_path);
    }

    // Complex workflow tests
    #[test]
    fn test_complete_lifecycle() {
        let mut manager = SpeedBurstManager::new();

        // Start bursts
        let r1 = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        assert!(matches!(r1, BurstStartResult::Started(_)));

        let r2 = manager.start_burst("task-2", Some(2_000_000), Some(120), Some(3.0));
        assert!(matches!(r2, BurstStartResult::Started(_)));

        // Check status
        let status = manager.status();
        assert_eq!(status.active_bursts.len(), 2);
        assert_eq!(status.total_bursts_started, 2);

        // Stop one
        let stopped = manager.stop_burst("task-1");
        assert!(stopped.is_some());

        // Check again
        assert!(!manager.has_active_burst("task-1"));
        assert!(manager.has_active_burst("task-2"));

        // Clear all
        let reverted = manager.clear_all();
        assert_eq!(reverted.len(), 1);
        assert!(manager.active_bursts.is_empty());
    }

    #[test]
    fn test_multiple_tasks_independent() {
        let mut manager = SpeedBurstManager::new();

        for i in 0..10 {
            let task_id = format!("task-{}", i);
            let limit = (i as u64 + 1) * 100_000;
            manager.start_burst(&task_id, Some(limit), Some(60), Some(2.0));
        }

        assert_eq!(manager.active_bursts.len(), 10);
        assert_eq!(manager.total_started, 10);

        for i in 0..10 {
            let task_id = format!("task-{}", i);
            assert!(manager.has_active_burst(&task_id));
            let expected_limit = (i as u64 + 1) * 100_000 * 2;
            assert_eq!(manager.get_burst_limit(&task_id), Some(expected_limit));
        }
    }

    #[test]
    fn test_burst_update_workflow() {
        let mut manager = SpeedBurstManager::new();

        // Start initial burst
        manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        assert_eq!(manager.get_burst_limit("task-1"), Some(2_000_000));

        // Update with higher multiplier
        manager.start_burst("task-1", Some(1_000_000), Some(120), Some(5.0));
        assert_eq!(manager.get_burst_limit("task-1"), Some(5_000_000));

        // Update with different original limit
        manager.start_burst("task-1", Some(2_000_000), Some(60), Some(3.0));
        assert_eq!(manager.get_burst_limit("task-1"), Some(6_000_000));

        assert_eq!(manager.total_started, 3);
    }

    #[test]
    fn test_config_update_workflow() {
        let mut manager = SpeedBurstManager::new();

        // Start with defaults
        assert!(manager.config().enabled);
        assert_eq!(manager.config().default_duration_secs, 300);

        // Update config
        let new_config = SpeedBurstConfig {
            enabled: true,
            default_duration_secs: 600,
            default_multiplier: 3.0,
            max_duration_secs: 7200,
            max_multiplier: 20.0,
            max_concurrent_bursts: 5,
        };
        manager.set_config(new_config);

        // Verify update
        assert_eq!(manager.config().default_duration_secs, 600);
        assert_eq!(manager.config().default_multiplier, 3.0);
        assert_eq!(manager.config().max_concurrent_bursts, 5);

        // Start burst with new defaults
        let result = manager.start_burst("task-1", Some(1_000_000), None, None);
        match result {
            BurstStartResult::Started(burst) => {
                assert_eq!(burst.burst_limit, 3_000_000);
            }
            _ => panic!("Expected Started"),
        }
    }

    #[test]
    fn test_max_concurrent_boundary() {
        let mut manager = SpeedBurstManager::new();
        manager.config.max_concurrent_bursts = 1;

        let r1 = manager.start_burst("task-1", Some(1_000_000), Some(60), Some(2.0));
        assert!(matches!(r1, BurstStartResult::Started(_)));

        let r2 = manager.start_burst("task-2", Some(1_000_000), Some(60), Some(2.0));
        assert!(matches!(r2, BurstStartResult::MaxBurstsReached));

        // Updating existing should work
        let r3 = manager.start_burst("task-1", Some(2_000_000), Some(60), Some(3.0));
        assert!(matches!(r3, BurstStartResult::Started(_)));
    }

    #[test]
    fn test_max_concurrent_zero_unlimited() {
        let mut manager = SpeedBurstManager::new();
        manager.config.max_concurrent_bursts = 0;

        for i in 0..100 {
            let task_id = format!("task-{}", i);
            let result = manager.start_burst(&task_id, Some(1_000_000), Some(60), Some(2.0));
            assert!(matches!(result, BurstStartResult::Started(_)));
        }

        assert_eq!(manager.active_bursts.len(), 100);
    }

    #[test]
    fn test_burst_limit_calculation_various() {
        let mut manager = SpeedBurstManager::new();

        let test_cases = vec![
            (1_000_000, 2.0, 2_000_000),
            (1_000_000, 3.0, 3_000_000),
            (500_000, 4.0, 2_000_000),
            (1_234_567, 2.5, 3_086_417),
            (100, 10.0, 1000),
            (10_000_000_000, 2.0, 20_000_000_000),
        ];

        for (i, (limit, mult, expected)) in test_cases.into_iter().enumerate() {
            let task_id = format!("task-{}", i);
            let result = manager.start_burst(&task_id, Some(limit), Some(60), Some(mult));
            match result {
                BurstStartResult::Started(burst) => {
                    assert_eq!(
                        burst.burst_limit, expected,
                        "Failed for limit={}, mult={}",
                        limit, mult
                    );
                }
                _ => panic!("Expected Started for limit={}, mult={}", limit, mult),
            }
        }
    }
}
