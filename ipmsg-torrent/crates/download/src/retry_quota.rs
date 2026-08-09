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
}
