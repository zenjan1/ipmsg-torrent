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
        let json = serde_json::to_string_pretty(&self.config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
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
}
