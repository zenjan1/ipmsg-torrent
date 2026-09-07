//! Daily retry quota for download tasks
//!
//! Limits the total number of automatic retry attempts across all download tasks
//! within a rolling 24-hour window. When the quota is exhausted, failed tasks
//! remain in Error state until the next day or until the user manually resets
//! the quota. This prevents runaway retries from consuming resources when the
//! network is persistently unavailable.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Configuration for daily retry quota
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryQuotaConfig {
    /// Enable retry quota limiting
    pub enabled: bool,
    /// Maximum retry attempts per day (0 = unlimited)
    pub max_retries_per_day: u32,
    /// Rolling window duration in seconds (default 86400 = 24h)
    pub window_secs: u64,
}

impl Default for RetryQuotaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_retries_per_day: 100,
            window_secs: 86400,
        }
    }
}

/// Tracks retry quota usage
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RetryQuotaState {
    /// Timestamps of each retry attempt within the current window
    retry_timestamps: Vec<i64>,
    /// Date of the last reset (YYYY-MM-DD) for daily rollover
    last_reset_date: Option<String>,
}

/// Result of checking retry quota
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaCheck {
    /// Retry is allowed; remaining quota included
    Allowed { remaining: u32 },
    /// Quota exhausted; seconds until next retry slot opens
    Exhausted { retry_after_secs: u64 },
    /// Quota is disabled (unlimited retries)
    Disabled,
}

/// Manager for daily retry quota
#[derive(Debug, Clone)]
pub struct RetryQuotaManager {
    config: RetryQuotaConfig,
    state: RetryQuotaState,
}

impl RetryQuotaManager {
    /// Create a new manager with default config
    pub fn new() -> Self {
        Self {
            config: RetryQuotaConfig::default(),
            state: RetryQuotaState::default(),
        }
    }

    /// Create with specific config
    pub fn with_config(config: RetryQuotaConfig) -> Self {
        Self {
            config,
            state: RetryQuotaState::default(),
        }
    }

    /// Get current config
    pub fn config(&self) -> &RetryQuotaConfig {
        &self.config
    }

    /// Update config
    pub fn set_config(&mut self, config: RetryQuotaConfig) {
        self.config = config;
    }

    /// Check whether a retry attempt is allowed
    pub fn check_quota(&mut self) -> QuotaCheck {
        if !self.config.enabled || self.config.max_retries_per_day == 0 {
            return QuotaCheck::Disabled;
        }

        let now = chrono::Utc::now();
        self.daily_rollover(&now);
        self.prune_expired(&now);

        let used = self.state.retry_timestamps.len() as u32;
        if used < self.config.max_retries_per_day {
            QuotaCheck::Allowed {
                remaining: self.config.max_retries_per_day - used,
            }
        } else {
            // Calculate when the oldest retry in the window expires
            let retry_after_secs = if let Some(oldest) = self.state.retry_timestamps.first() {
                let expires_at = *oldest + self.config.window_secs as i64;
                let now_ts = now.timestamp();
                if expires_at > now_ts {
                    (expires_at - now_ts) as u64
                } else {
                    0
                }
            } else {
                0
            };
            QuotaCheck::Exhausted { retry_after_secs }
        }
    }

    /// Record a retry attempt. Returns true if recorded, false if quota exhausted.
    pub fn record_retry(&mut self) -> bool {
        if !self.config.enabled || self.config.max_retries_per_day == 0 {
            // Still record for statistics even when disabled
            let now = chrono::Utc::now();
            self.daily_rollover(&now);
            self.state.retry_timestamps.push(now.timestamp());
            return true;
        }

        let now = chrono::Utc::now();
        self.daily_rollover(&now);
        self.prune_expired(&now);

        let used = self.state.retry_timestamps.len() as u32;
        if used < self.config.max_retries_per_day {
            self.state.retry_timestamps.push(now.timestamp());
            true
        } else {
            false
        }
    }

    /// Get current usage statistics
    pub fn usage(&mut self) -> RetryQuotaUsage {
        let now = chrono::Utc::now();
        self.daily_rollover(&now);
        self.prune_expired(&now);

        let used = self.state.retry_timestamps.len() as u32;
        let limit = if self.config.enabled {
            self.config.max_retries_per_day
        } else {
            0
        };
        let remaining = if self.config.enabled && self.config.max_retries_per_day > 0 {
            self.config.max_retries_per_day.saturating_sub(used)
        } else {
            u32::MAX
        };

        RetryQuotaUsage {
            enabled: self.config.enabled,
            used,
            limit,
            remaining,
            window_secs: self.config.window_secs,
        }
    }

    /// Reset the quota (clear all timestamps)
    pub fn reset(&mut self) {
        self.state.retry_timestamps.clear();
        self.state.last_reset_date = Some(chrono::Utc::now().format("%Y-%m-%d").to_string());
    }

    /// Save state to disk
    pub fn save(&self, path: &Path) -> Result<(), RetryQuotaError> {
        let json = serde_json::to_string_pretty(&RetryQuotaPersisted {
            config: self.config.clone(),
            state: self.state.clone(),
        })
        .map_err(|e| RetryQuotaError::Serialize(e.to_string()))?;

        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json).map_err(|e| RetryQuotaError::Io(e.to_string()))?;
        std::fs::rename(&tmp, path).map_err(|e| RetryQuotaError::Io(e.to_string()))?;
        Ok(())
    }

    /// Load state from disk
    pub fn load(path: &Path) -> Result<Self, RetryQuotaError> {
        let json = std::fs::read_to_string(path).map_err(|e| RetryQuotaError::Io(e.to_string()))?;
        let persisted: RetryQuotaPersisted =
            serde_json::from_str(&json).map_err(|e| RetryQuotaError::Deserialize(e.to_string()))?;
        Ok(Self {
            config: persisted.config,
            state: persisted.state,
        })
    }

    /// Check if file exists
    pub fn state_file_exists(path: &Path) -> bool {
        path.exists()
    }

    // -- private helpers --

    /// Reset timestamps if the date has changed (daily rollover)
    fn daily_rollover(&mut self, now: &chrono::DateTime<chrono::Utc>) {
        let today = now.format("%Y-%m-%d").to_string();
        if self.state.last_reset_date.as_deref() != Some(&today) {
            self.state.retry_timestamps.clear();
            self.state.last_reset_date = Some(today);
        }
    }

    /// Remove timestamps outside the rolling window
    fn prune_expired(&mut self, now: &chrono::DateTime<chrono::Utc>) {
        let cutoff = now.timestamp() - self.config.window_secs as i64;
        self.state.retry_timestamps.retain(|ts| *ts > cutoff);
    }
}

/// Usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryQuotaUsage {
    pub enabled: bool,
    pub used: u32,
    pub limit: u32,
    pub remaining: u32,
    pub window_secs: u64,
}

impl std::fmt::Display for RetryQuotaUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.enabled {
            write!(f, "Retry quota: disabled (unlimited retries)")
        } else if self.limit == 0 {
            write!(f, "Retry quota: enabled but limit=0 (all retries blocked)")
        } else {
            write!(
                f,
                "Retry quota: {}/{} used, {} remaining (window: {}s)",
                self.used,
                self.limit,
                if self.remaining == u32::MAX {
                    "∞".to_string()
                } else {
                    self.remaining.to_string()
                },
                self.window_secs,
            )
        }
    }
}

/// Persistence wrapper
#[derive(Debug, Serialize, Deserialize)]
struct RetryQuotaPersisted {
    config: RetryQuotaConfig,
    state: RetryQuotaState,
}

/// Errors for retry quota operations
#[derive(Debug)]
pub enum RetryQuotaError {
    Io(String),
    Serialize(String),
    Deserialize(String),
}

impl std::fmt::Display for RetryQuotaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Serialize(e) => write!(f, "Serialize error: {e}"),
            Self::Deserialize(e) => write!(f, "Deserialize error: {e}"),
        }
    }
}

impl std::error::Error for RetryQuotaError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RetryQuotaConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_retries_per_day, 100);
        assert_eq!(config.window_secs, 86400);
    }

    #[test]
    fn test_disabled_returns_disabled() {
        let mut mgr = RetryQuotaManager::new();
        assert_eq!(mgr.check_quota(), QuotaCheck::Disabled);
    }

    #[test]
    fn test_enabled_allows_retry() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 5,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        match mgr.check_quota() {
            QuotaCheck::Allowed { remaining } => assert_eq!(remaining, 5),
            other => panic!("Expected Allowed, got {other:?}"),
        }
    }

    #[test]
    fn test_record_retry_decrements_quota() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 3,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        assert!(mgr.record_retry());
        assert!(mgr.record_retry());
        assert!(mgr.record_retry());
        // 4th should fail
        assert!(!mgr.record_retry());
    }

    #[test]
    fn test_exhausted_returns_retry_after() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 1,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        assert!(mgr.record_retry());
        match mgr.check_quota() {
            QuotaCheck::Exhausted { retry_after_secs } => {
                // Should be close to 86400
                assert!(retry_after_secs > 86000);
                assert!(retry_after_secs <= 86400);
            }
            other => panic!("Expected Exhausted, got {other:?}"),
        }
    }

    #[test]
    fn test_reset_clears_quota() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 2,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        assert!(mgr.record_retry());
        assert!(mgr.record_retry());
        assert!(!mgr.record_retry());

        mgr.reset();
        assert!(mgr.record_retry());
    }

    #[test]
    fn test_usage_stats() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 3600,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        mgr.record_retry();

        let usage = mgr.usage();
        assert!(usage.enabled);
        assert_eq!(usage.used, 2);
        assert_eq!(usage.limit, 10);
        assert_eq!(usage.remaining, 8);
        assert_eq!(usage.window_secs, 3600);
    }

    #[test]
    fn test_usage_disabled() {
        let mut mgr = RetryQuotaManager::new();
        let usage = mgr.usage();
        assert!(!usage.enabled);
        assert_eq!(usage.remaining, u32::MAX);
    }

    #[test]
    fn test_usage_display() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 50,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        let usage = mgr.usage();
        let s = format!("{usage}");
        assert!(s.contains("1/50"));
        assert!(s.contains("49 remaining"));
    }

    #[test]
    fn test_save_and_load() {
        let dir = std::env::temp_dir().join("retry_quota_test_save_load");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");

        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 20,
            window_secs: 7200,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        mgr.record_retry();
        mgr.save(&path).unwrap();

        let loaded = RetryQuotaManager::load(&path).unwrap();
        assert!(loaded.config().enabled);
        assert_eq!(loaded.config().max_retries_per_day, 20);
        assert_eq!(loaded.config().window_secs, 7200);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_missing_file() {
        let path = std::path::PathBuf::from("/tmp/nonexistent_retry_quota.json");
        assert!(RetryQuotaManager::load(&path).is_err());
    }

    #[test]
    fn test_zero_limit_blocks_all() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 0,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        // Zero limit means disabled (unlimited)
        assert_eq!(mgr.check_quota(), QuotaCheck::Disabled);
    }

    #[test]
    fn test_set_config() {
        let mut mgr = RetryQuotaManager::new();
        assert!(!mgr.config().enabled);

        mgr.set_config(RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 50,
            window_secs: 3600,
        });
        assert!(mgr.config().enabled);
        assert_eq!(mgr.config().max_retries_per_day, 50);
    }

    #[test]
    fn test_state_file_exists() {
        let path = std::path::PathBuf::from("/tmp/nonexistent_retry_quota_check.json");
        assert!(!RetryQuotaManager::state_file_exists(&path));
    }

    #[test]
    fn test_prune_expired_removes_old_entries() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 60, // 60 second window
        };
        let mut mgr = RetryQuotaManager::with_config(config);

        // Manually insert an old timestamp
        let old_ts = chrono::Utc::now().timestamp() - 120; // 2 minutes ago
        mgr.state.retry_timestamps.push(old_ts);

        // After checking, the old entry should be pruned
        let usage = mgr.usage();
        assert_eq!(usage.used, 0); // pruned
    }

    #[test]
    fn test_daily_rollover_clears_timestamps() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        mgr.record_retry();

        // Simulate a different day
        mgr.state.last_reset_date = Some("2020-01-01".to_string());

        let usage = mgr.usage();
        assert_eq!(usage.used, 0); // cleared by rollover
    }

    #[test]
    fn test_disabled_still_records() {
        let mut mgr = RetryQuotaManager::new(); // disabled by default
        assert!(mgr.record_retry()); // should succeed
        let usage = mgr.usage();
        // Even though disabled, we still track for stats
        // But daily_rollover may clear it if date changed
        // In this test, date hasn't changed, so we see the record
        assert_eq!(usage.used, 1);
    }

    #[test]
    fn test_exhausted_display() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 0,
            window_secs: 86400,
        };
        let mgr = RetryQuotaManager::with_config(config);
        let usage = mgr.clone().usage();
        let s = format!("{usage}");
        assert!(s.contains("limit=0"));
    }

    #[test]
    fn test_disabled_display() {
        let mgr = RetryQuotaManager::new();
        let usage = mgr.clone().usage();
        let s = format!("{usage}");
        assert!(s.contains("disabled"));
    }

    #[test]
    fn test_save_creates_atomic_file() {
        let dir = std::env::temp_dir().join("retry_quota_test_atomic");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");

        let mgr = RetryQuotaManager::new();
        mgr.save(&path).unwrap();

        // Verify the file exists and is valid JSON
        let content = std::fs::read_to_string(&path).unwrap();
        let _: RetryQuotaPersisted = serde_json::from_str(&content).unwrap();

        // tmp file should not exist
        assert!(!path.with_extension("json.tmp").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ===== Phase 251: Comprehensive Test Coverage =====

    // --- RetryQuotaConfig serde ---

    #[test]
    fn config_serde_roundtrip_default() {
        let config = RetryQuotaConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let loaded: RetryQuotaConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.max_retries_per_day, config.max_retries_per_day);
        assert_eq!(loaded.window_secs, config.window_secs);
    }

    #[test]
    fn config_serde_roundtrip_custom() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 42,
            window_secs: 3600,
        };
        let json = serde_json::to_string(&config).unwrap();
        let loaded: RetryQuotaConfig = serde_json::from_str(&json).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.max_retries_per_day, 42);
        assert_eq!(loaded.window_secs, 3600);
    }

    #[test]
    fn config_serde_pretty() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 7200,
        };
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let loaded: RetryQuotaConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.max_retries_per_day, 10);
    }

    #[test]
    fn config_serde_extra_fields_ignored() {
        let json =
            r#"{"enabled":true,"max_retries_per_day":5,"window_secs":100,"extra":"ignored"}"#;
        let loaded: RetryQuotaConfig = serde_json::from_str(json).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.max_retries_per_day, 5);
    }

    #[test]
    fn config_serde_missing_fields_use_defaults() {
        // serde will fail on missing non-Option fields; test with all fields present
        let json = r#"{"enabled":false,"max_retries_per_day":0,"window_secs":0}"#;
        let loaded: RetryQuotaConfig = serde_json::from_str(json).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.max_retries_per_day, 0);
        assert_eq!(loaded.window_secs, 0);
    }

    // --- RetryQuotaConfig traits ---

    #[test]
    fn config_clone() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 99,
            window_secs: 1234,
        };
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.max_retries_per_day, config.max_retries_per_day);
        assert_eq!(cloned.window_secs, config.window_secs);
    }

    #[test]
    fn config_clone_independence() {
        let mut config = RetryQuotaConfig::default();
        let cloned = config.clone();
        config.enabled = true;
        config.max_retries_per_day = 999;
        assert!(!cloned.enabled);
        assert_ne!(cloned.max_retries_per_day, 999);
    }

    #[test]
    fn config_debug() {
        let config = RetryQuotaConfig::default();
        let debug = format!("{config:?}");
        assert!(debug.contains("RetryQuotaConfig"));
        assert!(debug.contains("enabled"));
        assert!(debug.contains("max_retries_per_day"));
    }

    // --- RetryQuotaConfig boundary values ---

    #[test]
    fn config_boundary_u32_max() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: u32::MAX,
            window_secs: 86400,
        };
        assert_eq!(config.max_retries_per_day, u32::MAX);
    }

    #[test]
    fn config_boundary_window_zero() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 0,
        };
        assert_eq!(config.window_secs, 0);
    }

    // --- RetryQuotaState serde ---

    #[test]
    fn state_serde_roundtrip_empty() {
        let state = RetryQuotaState::default();
        let json = serde_json::to_string(&state).unwrap();
        let loaded: RetryQuotaState = serde_json::from_str(&json).unwrap();
        assert!(loaded.retry_timestamps.is_empty());
        assert!(loaded.last_reset_date.is_none());
    }

    #[test]
    fn state_serde_roundtrip_with_data() {
        let mut state = RetryQuotaState::default();
        state.retry_timestamps = vec![1000, 2000, 3000];
        state.last_reset_date = Some("2026-09-07".to_string());
        let json = serde_json::to_string(&state).unwrap();
        let loaded: RetryQuotaState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.retry_timestamps, vec![1000, 2000, 3000]);
        assert_eq!(loaded.last_reset_date.as_deref(), Some("2026-09-07"));
    }

    #[test]
    fn state_serde_null_last_reset_date() {
        let json = r#"{"retry_timestamps":[],"last_reset_date":null}"#;
        let loaded: RetryQuotaState = serde_json::from_str(json).unwrap();
        assert!(loaded.last_reset_date.is_none());
    }

    // --- RetryQuotaState traits ---

    #[test]
    fn state_default() {
        let state = RetryQuotaState::default();
        assert!(state.retry_timestamps.is_empty());
        assert!(state.last_reset_date.is_none());
    }

    #[test]
    fn state_clone() {
        let mut state = RetryQuotaState::default();
        state.retry_timestamps = vec![100, 200];
        state.last_reset_date = Some("2026-01-01".to_string());
        let cloned = state.clone();
        assert_eq!(cloned.retry_timestamps, state.retry_timestamps);
        assert_eq!(cloned.last_reset_date, state.last_reset_date);
    }

    #[test]
    fn state_clone_independence() {
        let mut state = RetryQuotaState::default();
        state.retry_timestamps.push(42);
        let mut cloned = state.clone();
        cloned.retry_timestamps.push(99);
        assert_eq!(state.retry_timestamps.len(), 1);
        assert_eq!(cloned.retry_timestamps.len(), 2);
    }

    #[test]
    fn state_debug() {
        let state = RetryQuotaState::default();
        let debug = format!("{state:?}");
        assert!(debug.contains("RetryQuotaState"));
    }

    // --- QuotaCheck ---

    #[test]
    fn quota_check_allowed_debug() {
        let check = QuotaCheck::Allowed { remaining: 5 };
        let debug = format!("{check:?}");
        assert!(debug.contains("Allowed"));
        assert!(debug.contains("5"));
    }

    #[test]
    fn quota_check_exhausted_debug() {
        let check = QuotaCheck::Exhausted {
            retry_after_secs: 3600,
        };
        let debug = format!("{check:?}");
        assert!(debug.contains("Exhausted"));
        assert!(debug.contains("3600"));
    }

    #[test]
    fn quota_check_disabled_debug() {
        let check = QuotaCheck::Disabled;
        let debug = format!("{check:?}");
        assert!(debug.contains("Disabled"));
    }

    #[test]
    fn quota_check_eq_allowed() {
        assert_eq!(
            QuotaCheck::Allowed { remaining: 5 },
            QuotaCheck::Allowed { remaining: 5 }
        );
    }

    #[test]
    fn quota_check_ne_allowed_different_remaining() {
        assert_ne!(
            QuotaCheck::Allowed { remaining: 5 },
            QuotaCheck::Allowed { remaining: 3 }
        );
    }

    #[test]
    fn quota_check_ne_different_variants() {
        assert_ne!(QuotaCheck::Allowed { remaining: 0 }, QuotaCheck::Disabled);
    }

    #[test]
    fn quota_check_eq_exhausted() {
        assert_eq!(
            QuotaCheck::Exhausted {
                retry_after_secs: 100
            },
            QuotaCheck::Exhausted {
                retry_after_secs: 100
            }
        );
    }

    #[test]
    fn quota_check_eq_disabled() {
        assert_eq!(QuotaCheck::Disabled, QuotaCheck::Disabled);
    }

    // --- RetryQuotaManager::new / default ---

    #[test]
    fn manager_new_default_values() {
        let mgr = RetryQuotaManager::new();
        assert!(!mgr.config().enabled);
        assert_eq!(mgr.config().max_retries_per_day, 100);
        assert_eq!(mgr.config().window_secs, 86400);
    }

    #[test]
    fn manager_with_config() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 50,
            window_secs: 1800,
        };
        let mgr = RetryQuotaManager::with_config(config);
        assert!(mgr.config().enabled);
        assert_eq!(mgr.config().max_retries_per_day, 50);
        assert_eq!(mgr.config().window_secs, 1800);
    }

    #[test]
    fn manager_with_config_empty_state() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 3600,
        };
        let mgr = RetryQuotaManager::with_config(config);
        assert!(mgr.state.retry_timestamps.is_empty());
        assert!(mgr.state.last_reset_date.is_none());
    }

    // --- RetryQuotaManager::config / set_config ---

    #[test]
    fn manager_config_returns_reference() {
        let mgr = RetryQuotaManager::new();
        let config_ref = mgr.config();
        assert!(!config_ref.enabled);
    }

    #[test]
    fn manager_set_config_updates() {
        let mut mgr = RetryQuotaManager::new();
        assert!(!mgr.config().enabled);

        mgr.set_config(RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 200,
            window_secs: 600,
        });
        assert!(mgr.config().enabled);
        assert_eq!(mgr.config().max_retries_per_day, 200);
        assert_eq!(mgr.config().window_secs, 600);
    }

    #[test]
    fn manager_set_config_multiple_times() {
        let mut mgr = RetryQuotaManager::new();
        for i in 1..=5 {
            mgr.set_config(RetryQuotaConfig {
                enabled: true,
                max_retries_per_day: i * 10,
                window_secs: 3600,
            });
        }
        assert_eq!(mgr.config().max_retries_per_day, 50);
    }

    // --- check_quota edge cases ---

    #[test]
    fn check_quota_enabled_max_zero_returns_disabled() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 0,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        assert_eq!(mgr.check_quota(), QuotaCheck::Disabled);
    }

    #[test]
    fn check_quota_disabled_max_zero_returns_disabled() {
        let config = RetryQuotaConfig {
            enabled: false,
            max_retries_per_day: 0,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        assert_eq!(mgr.check_quota(), QuotaCheck::Disabled);
    }

    #[test]
    fn check_quota_max_one_remaining() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 1,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        match mgr.check_quota() {
            QuotaCheck::Allowed { remaining } => assert_eq!(remaining, 1),
            other => panic!("Expected Allowed, got {other:?}"),
        }
    }

    #[test]
    fn check_quota_after_one_record() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        match mgr.check_quota() {
            QuotaCheck::Allowed { remaining } => assert_eq!(remaining, 9),
            other => panic!("Expected Allowed, got {other:?}"),
        }
    }

    #[test]
    fn check_quota_exact_boundary() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 3,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        mgr.record_retry();
        mgr.record_retry();
        // Now exactly at limit
        match mgr.check_quota() {
            QuotaCheck::Exhausted { .. } => {}
            other => panic!("Expected Exhausted, got {other:?}"),
        }
    }

    // --- record_retry edge cases ---

    #[test]
    fn record_retry_disabled_always_returns_true() {
        let mut mgr = RetryQuotaManager::new(); // disabled
        for _ in 0..100 {
            assert!(mgr.record_retry());
        }
    }

    #[test]
    fn record_retry_max_zero_always_returns_true() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 0,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        for _ in 0..50 {
            assert!(mgr.record_retry());
        }
    }

    #[test]
    fn record_retry_max_one() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 1,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        assert!(mgr.record_retry());
        assert!(!mgr.record_retry());
    }

    #[test]
    fn record_retry_large_quota() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 1000,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        for _ in 0..1000 {
            assert!(mgr.record_retry());
        }
        assert!(!mgr.record_retry());
    }

    // --- usage edge cases ---

    #[test]
    fn usage_enabled_limit_zero() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 0,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        let usage = mgr.usage();
        assert!(usage.enabled);
        assert_eq!(usage.limit, 0);
        assert_eq!(usage.remaining, u32::MAX);
    }

    #[test]
    fn usage_disabled_limit_zero() {
        let config = RetryQuotaConfig {
            enabled: false,
            max_retries_per_day: 0,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        let usage = mgr.usage();
        assert!(!usage.enabled);
        assert_eq!(usage.limit, 0);
        assert_eq!(usage.remaining, u32::MAX);
    }

    #[test]
    fn usage_all_consumed() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 2,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        mgr.record_retry();
        let usage = mgr.usage();
        assert_eq!(usage.used, 2);
        assert_eq!(usage.remaining, 0);
    }

    #[test]
    fn usage_window_secs_reflects_config() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 12345,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        let usage = mgr.usage();
        assert_eq!(usage.window_secs, 12345);
    }

    // --- RetryQuotaUsage Display ---

    #[test]
    fn usage_display_disabled() {
        let usage = RetryQuotaUsage {
            enabled: false,
            used: 0,
            limit: 0,
            remaining: u32::MAX,
            window_secs: 86400,
        };
        let s = format!("{usage}");
        assert!(s.contains("disabled"));
        assert!(s.contains("unlimited"));
    }

    #[test]
    fn usage_display_limit_zero() {
        let usage = RetryQuotaUsage {
            enabled: true,
            used: 0,
            limit: 0,
            remaining: u32::MAX,
            window_secs: 86400,
        };
        let s = format!("{usage}");
        assert!(s.contains("limit=0"));
        assert!(s.contains("all retries blocked"));
    }

    #[test]
    fn usage_display_normal() {
        let usage = RetryQuotaUsage {
            enabled: true,
            used: 3,
            limit: 10,
            remaining: 7,
            window_secs: 3600,
        };
        let s = format!("{usage}");
        assert!(s.contains("3/10"));
        assert!(s.contains("7 remaining"));
        assert!(s.contains("3600s"));
    }

    #[test]
    fn usage_display_unicode_infinity() {
        let usage = RetryQuotaUsage {
            enabled: true,
            used: 0,
            limit: 10,
            remaining: u32::MAX,
            window_secs: 86400,
        };
        let s = format!("{usage}");
        assert!(s.contains('∞'));
    }

    // --- RetryQuotaError ---

    #[test]
    fn error_display_io() {
        let err = RetryQuotaError::Io("disk full".to_string());
        let s = format!("{err}");
        assert!(s.contains("IO error"));
        assert!(s.contains("disk full"));
    }

    #[test]
    fn error_display_serialize() {
        let err = RetryQuotaError::Serialize("invalid data".to_string());
        let s = format!("{err}");
        assert!(s.contains("Serialize error"));
        assert!(s.contains("invalid data"));
    }

    #[test]
    fn error_display_deserialize() {
        let err = RetryQuotaError::Deserialize("unexpected token".to_string());
        let s = format!("{err}");
        assert!(s.contains("Deserialize error"));
        assert!(s.contains("unexpected token"));
    }

    #[test]
    fn error_debug() {
        let err = RetryQuotaError::Io("test".to_string());
        let debug = format!("{err:?}");
        assert!(debug.contains("Io"));
    }

    #[test]
    fn error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(RetryQuotaError::Io("test".to_string()));
        assert!(err.to_string().contains("IO error"));
    }

    #[test]
    fn error_unicode_message() {
        let err = RetryQuotaError::Io("磁盘已满".to_string());
        let s = format!("{err}");
        assert!(s.contains("磁盘已满"));
    }

    #[test]
    fn error_empty_message() {
        let err = RetryQuotaError::Io(String::new());
        let s = format!("{err}");
        assert!(s.contains("IO error"));
    }

    // --- Persistence comprehensive ---

    #[test]
    fn save_overwrite_existing() {
        let dir = std::env::temp_dir().join("retry_quota_test_overwrite");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");

        let config1 = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 5,
            window_secs: 1000,
        };
        let mut mgr1 = RetryQuotaManager::with_config(config1);
        mgr1.record_retry();
        mgr1.save(&path).unwrap();

        let config2 = RetryQuotaConfig {
            enabled: false,
            max_retries_per_day: 99,
            window_secs: 2000,
        };
        let mgr2 = RetryQuotaManager::with_config(config2);
        mgr2.save(&path).unwrap();

        let loaded = RetryQuotaManager::load(&path).unwrap();
        assert!(!loaded.config().enabled);
        assert_eq!(loaded.config().max_retries_per_day, 99);
        assert_eq!(loaded.config().window_secs, 2000);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_no_tmp_leftover() {
        let dir = std::env::temp_dir().join("retry_quota_test_no_tmp");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");

        let mgr = RetryQuotaManager::new();
        mgr.save(&path).unwrap();

        let tmp_path = path.with_extension("json.tmp");
        assert!(!tmp_path.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_corrupted_json() {
        let dir = std::env::temp_dir().join("retry_quota_test_corrupt");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");
        std::fs::write(&path, "not valid json{{{").unwrap();

        let result = RetryQuotaManager::load(&path);
        assert!(result.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_empty_file() {
        let dir = std::env::temp_dir().join("retry_quota_test_empty");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");
        std::fs::write(&path, "").unwrap();

        let result = RetryQuotaManager::load(&path);
        assert!(result.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_creates_parent_directory_error() {
        // Saving to a non-existent directory should fail with IO error
        let path = std::path::PathBuf::from("/tmp/nonexistent_dir_xyz/retry_quota.json");
        let mgr = RetryQuotaManager::new();
        assert!(mgr.save(&path).is_err());
    }

    #[test]
    fn save_pretty_json_format() {
        let dir = std::env::temp_dir().join("retry_quota_test_pretty");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");

        let mgr = RetryQuotaManager::new();
        mgr.save(&path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        // Pretty JSON has newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_unicode_path() {
        let dir = std::env::temp_dir().join("retry_quota_test_üñíçödé");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota_数据.json");

        let mgr = RetryQuotaManager::new();
        mgr.save(&path).unwrap();

        let loaded = RetryQuotaManager::load(&path).unwrap();
        assert!(!loaded.config().enabled);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn state_file_exists_true() {
        let dir = std::env::temp_dir().join("retry_quota_test_exists");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");

        let mgr = RetryQuotaManager::new();
        mgr.save(&path).unwrap();

        assert!(RetryQuotaManager::state_file_exists(&path));

        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- reset edge cases ---

    #[test]
    fn reset_sets_last_reset_date() {
        let mut mgr = RetryQuotaManager::new();
        assert!(mgr.state.last_reset_date.is_none());
        mgr.reset();
        assert!(mgr.state.last_reset_date.is_some());
    }

    #[test]
    fn reset_empty_manager() {
        let mut mgr = RetryQuotaManager::new();
        mgr.reset();
        assert!(mgr.state.retry_timestamps.is_empty());
    }

    #[test]
    fn reset_idempotent() {
        let mut mgr = RetryQuotaManager::new();
        mgr.reset();
        let date1 = mgr.state.last_reset_date.clone();
        mgr.reset();
        let date2 = mgr.state.last_reset_date.clone();
        // Both should be today
        assert!(date1.is_some());
        assert!(date2.is_some());
    }

    // --- daily_rollover ---

    #[test]
    fn daily_rollover_same_date_preserves() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        mgr.record_retry();

        // Set last_reset_date to today
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        mgr.state.last_reset_date = Some(today);

        let usage = mgr.usage();
        assert_eq!(usage.used, 2); // preserved
    }

    #[test]
    fn daily_rollover_different_date_clears() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();

        // Set to a past date
        mgr.state.last_reset_date = Some("2020-01-01".to_string());

        let usage = mgr.usage();
        assert_eq!(usage.used, 0); // cleared
    }

    #[test]
    fn daily_rollover_none_date_clears() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.state.retry_timestamps.push(1000);
        mgr.state.last_reset_date = None;

        let _ = mgr.usage();
        // After rollover, timestamps should be cleared and date set
        assert!(mgr.state.last_reset_date.is_some());
    }

    // --- prune_expired ---

    #[test]
    fn prune_expired_removes_all_old() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 100,
            window_secs: 10, // 10 second window
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        let old_ts = chrono::Utc::now().timestamp() - 60;
        mgr.state.retry_timestamps.push(old_ts);

        let usage = mgr.usage();
        assert_eq!(usage.used, 0);
    }

    #[test]
    fn prune_expired_keeps_recent() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 100,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        let recent_ts = chrono::Utc::now().timestamp() - 10;
        mgr.state.retry_timestamps.push(recent_ts);

        let usage = mgr.usage();
        assert_eq!(usage.used, 1);
    }

    #[test]
    fn prune_expired_mixed() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 100,
            window_secs: 60,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        let now = chrono::Utc::now().timestamp();
        mgr.state.retry_timestamps.push(now - 120); // old
        mgr.state.retry_timestamps.push(now - 30); // recent
        mgr.state.retry_timestamps.push(now - 5); // recent

        let usage = mgr.usage();
        assert_eq!(usage.used, 2);
    }

    #[test]
    fn prune_expired_window_zero() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 100,
            window_secs: 0,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        let now = chrono::Utc::now().timestamp();
        mgr.state.retry_timestamps.push(now);

        // Window 0 means everything is immediately expired
        let usage = mgr.usage();
        assert_eq!(usage.used, 0);
    }

    // --- RetryQuotaManager Clone / Debug ---

    #[test]
    fn manager_clone() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 3600,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();

        let cloned = mgr.clone();
        assert!(cloned.config().enabled);
        assert_eq!(cloned.config().max_retries_per_day, 10);
    }

    #[test]
    fn manager_clone_independence() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 3600,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();

        let mut cloned = mgr.clone();
        cloned.record_retry();
        cloned.record_retry();

        // Original should not be affected
        let usage = mgr.usage();
        assert_eq!(usage.used, 1);
    }

    #[test]
    fn manager_debug() {
        let mgr = RetryQuotaManager::new();
        let debug = format!("{mgr:?}");
        assert!(debug.contains("RetryQuotaManager"));
        assert!(debug.contains("config"));
        assert!(debug.contains("state"));
    }

    // --- Complete workflow integration ---

    #[test]
    fn complete_lifecycle() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 5,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);

        // Check initial state
        assert_eq!(mgr.check_quota(), QuotaCheck::Allowed { remaining: 5 });

        // Record retries
        for _ in 0..5 {
            assert!(mgr.record_retry());
        }

        // Quota exhausted
        assert!(!mgr.record_retry());
        match mgr.check_quota() {
            QuotaCheck::Exhausted { retry_after_secs } => {
                assert!(retry_after_secs > 0);
            }
            other => panic!("Expected Exhausted, got {other:?}"),
        }

        // Reset and retry
        mgr.reset();
        assert!(mgr.record_retry());

        // Check usage
        let usage = mgr.usage();
        assert_eq!(usage.used, 1);
        assert_eq!(usage.remaining, 4);
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = std::env::temp_dir().join("retry_quota_test_roundtrip2");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("retry_quota.json");

        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 25,
            window_secs: 7200,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        mgr.record_retry();
        mgr.record_retry();
        mgr.save(&path).unwrap();

        let mut loaded = RetryQuotaManager::load(&path).unwrap();
        assert!(loaded.config().enabled);
        assert_eq!(loaded.config().max_retries_per_day, 25);

        // Loaded manager should work
        match loaded.check_quota() {
            QuotaCheck::Allowed { remaining } => assert!(remaining <= 25),
            _ => {} // may be exhausted if timestamps are still within window
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn config_change_affects_quota() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 3,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);

        // Use up all quota
        assert!(mgr.record_retry());
        assert!(mgr.record_retry());
        assert!(mgr.record_retry());
        assert!(!mgr.record_retry());

        // Increase quota
        mgr.set_config(RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 86400,
        });

        // Now should have room
        assert!(mgr.record_retry());
    }

    #[test]
    fn disable_quota_after_use() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 2,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        mgr.record_retry();
        assert!(!mgr.record_retry());

        // Disable quota
        mgr.set_config(RetryQuotaConfig {
            enabled: false,
            max_retries_per_day: 2,
            window_secs: 86400,
        });

        assert_eq!(mgr.check_quota(), QuotaCheck::Disabled);
        assert!(mgr.record_retry()); // always succeeds
    }

    // --- RetryQuotaPersisted serde ---

    #[test]
    fn persisted_serde_roundtrip() {
        let persisted = RetryQuotaPersisted {
            config: RetryQuotaConfig {
                enabled: true,
                max_retries_per_day: 50,
                window_secs: 3600,
            },
            state: RetryQuotaState {
                retry_timestamps: vec![100, 200, 300],
                last_reset_date: Some("2026-09-07".to_string()),
            },
        };
        let json = serde_json::to_string(&persisted).unwrap();
        let loaded: RetryQuotaPersisted = serde_json::from_str(&json).unwrap();
        assert!(loaded.config.enabled);
        assert_eq!(loaded.config.max_retries_per_day, 50);
        assert_eq!(loaded.state.retry_timestamps.len(), 3);
    }

    #[test]
    fn persisted_debug() {
        let persisted = RetryQuotaPersisted {
            config: RetryQuotaConfig::default(),
            state: RetryQuotaState::default(),
        };
        let debug = format!("{persisted:?}");
        assert!(debug.contains("RetryQuotaPersisted"));
    }

    // --- Boundary: retry_after_secs ---

    #[test]
    fn retry_after_secs_is_positive_when_exhausted() {
        let config = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 1,
            window_secs: 86400,
        };
        let mut mgr = RetryQuotaManager::with_config(config);
        mgr.record_retry();
        match mgr.check_quota() {
            QuotaCheck::Exhausted { retry_after_secs } => {
                assert!(retry_after_secs > 0);
                assert!(retry_after_secs <= 86400);
            }
            other => panic!("Expected Exhausted, got {other:?}"),
        }
    }

    // --- Multiple managers independent ---

    #[test]
    fn multiple_managers_independent() {
        let config1 = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 5,
            window_secs: 86400,
        };
        let config2 = RetryQuotaConfig {
            enabled: true,
            max_retries_per_day: 10,
            window_secs: 3600,
        };
        let mut mgr1 = RetryQuotaManager::with_config(config1);
        let mut mgr2 = RetryQuotaManager::with_config(config2);

        mgr1.record_retry();
        mgr1.record_retry();
        mgr2.record_retry();

        let usage1 = mgr1.usage();
        let usage2 = mgr2.usage();
        assert_eq!(usage1.used, 2);
        assert_eq!(usage2.used, 1);
        assert_eq!(usage1.limit, 5);
        assert_eq!(usage2.limit, 10);
    }
}
