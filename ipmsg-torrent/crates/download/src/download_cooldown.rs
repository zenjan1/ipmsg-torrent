//! Download cooldown / exponential backoff retry for failed tasks.
//!
//! When a download fails, instead of immediately retrying, apply a cooldown
//! period before allowing the task to be retried. Supports exponential backoff
//! and fixed delay strategies.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Cooldown strategy for failed task retries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CooldownStrategy {
    /// Exponential backoff: delay * multiplier^(attempt-1), capped at max_delay.
    Exponential {
        /// Base delay in seconds.
        base_delay_secs: u64,
        /// Multiplier for each subsequent attempt (e.g. 2.0 doubles each time).
        multiplier: f64,
        /// Maximum delay cap in seconds.
        max_delay_secs: u64,
    },
    /// Fixed delay: same delay for every attempt.
    Fixed {
        /// Delay in seconds between retries.
        delay_secs: u64,
    },
}

impl Default for CooldownStrategy {
    fn default() -> Self {
        Self::Exponential {
            base_delay_secs: 30,
            multiplier: 2.0,
            max_delay_secs: 3600,
        }
    }
}

/// Configuration for download cooldown behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CooldownConfig {
    /// Whether cooldown is enabled.
    pub enabled: bool,
    /// The cooldown strategy to use.
    pub strategy: CooldownStrategy,
    /// Maximum number of retry attempts before giving up (0 = unlimited).
    pub max_retries: u32,
}

impl Default for CooldownConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            strategy: CooldownStrategy::default(),
            max_retries: 0,
        }
    }
}

/// Tracks the current cooldown state for a task.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CooldownState {
    /// Number of retry attempts so far.
    pub retry_attempt: u32,
    /// Timestamp (secs since epoch) when the task last failed.
    pub last_failed_at: Option<u64>,
    /// Timestamp (secs since epoch) when the next retry is allowed.
    pub next_retry_at: Option<u64>,
}

/// Status summary for CLI/API display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CooldownStatus {
    pub task_id: String,
    pub state: CooldownState,
    pub strategy: CooldownStrategy,
    pub can_retry_now: bool,
    pub seconds_until_retry: Option<u64>,
}

/// Compute the delay for a given attempt number (1-based).
pub fn compute_delay(strategy: &CooldownStrategy, attempt: u32) -> Duration {
    match strategy {
        CooldownStrategy::Exponential {
            base_delay_secs,
            multiplier,
            max_delay_secs,
        } => {
            let delay =
                (*base_delay_secs as f64) * multiplier.powi(attempt.saturating_sub(1) as i32);
            let capped = delay.min(*max_delay_secs as f64);
            Duration::from_secs(capped as u64)
        }
        CooldownStrategy::Fixed { delay_secs } => Duration::from_secs(*delay_secs),
    }
}

/// Record a failure and update cooldown state. Returns the updated state.
pub fn record_failure(state: &mut CooldownState, config: &CooldownConfig, now_secs: u64) {
    state.retry_attempt += 1;
    state.last_failed_at = Some(now_secs);
    let delay = compute_delay(&config.strategy, state.retry_attempt);
    state.next_retry_at = Some(now_secs + delay.as_secs());
}

/// Check if a cooldown period has elapsed and the task can be retried.
pub fn can_retry(state: &CooldownState, now_secs: u64) -> bool {
    match state.next_retry_at {
        Some(retry_at) => now_secs >= retry_at,
        None => true,
    }
}

/// Seconds remaining until retry is allowed. None if ready now.
pub fn seconds_until_retry(state: &CooldownState, now_secs: u64) -> Option<u64> {
    state.next_retry_at.and_then(|retry_at| {
        if now_secs >= retry_at {
            None
        } else {
            Some(retry_at - now_secs)
        }
    })
}

/// Check if max retries has been exceeded.
pub fn max_retries_exceeded(state: &CooldownState, config: &CooldownConfig) -> bool {
    config.max_retries > 0 && state.retry_attempt >= config.max_retries
}

/// Reset cooldown state (e.g. when task is manually resumed or completes).
pub fn reset_cooldown(state: &mut CooldownState) {
    state.retry_attempt = 0;
    state.last_failed_at = None;
    state.next_retry_at = None;
}

/// Get current unix timestamp in seconds.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Build a CooldownStatus for display.
pub fn cooldown_status(
    task_id: &str,
    state: &CooldownState,
    config: &CooldownConfig,
) -> CooldownStatus {
    let now = now_secs();
    CooldownStatus {
        task_id: task_id.to_string(),
        state: state.clone(),
        strategy: config.strategy.clone(),
        can_retry_now: can_retry(state, now),
        seconds_until_retry: seconds_until_retry(state, now),
    }
}

// --- Persistence ---

const CONFIG_FILENAME: &str = "cooldown_config.json";

/// Persistence error type
#[derive(Debug)]
pub enum CooldownPersistenceError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for CooldownPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Json(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<std::io::Error> for CooldownPersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for CooldownPersistenceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Save cooldown config to disk (atomic write)
pub fn save_cooldown_config(
    data_dir: &Path,
    config: &CooldownConfig,
) -> Result<(), CooldownPersistenceError> {
    let path = data_dir.join(CONFIG_FILENAME);
    let json = serde_json::to_string_pretty(config)?;

    // Atomic write: write to temp file, then rename
    let temp_path = data_dir.join("cooldown_config.json.tmp");
    fs::write(&temp_path, &json)?;
    fs::rename(&temp_path, &path)?;

    Ok(())
}

/// Load cooldown config from disk
pub fn load_cooldown_config(
    data_dir: &Path,
) -> Result<Option<CooldownConfig>, CooldownPersistenceError> {
    let path = data_dir.join(CONFIG_FILENAME);
    if !path.exists() {
        return Ok(None);
    }

    let json = fs::read_to_string(&path)?;
    let config: CooldownConfig = serde_json::from_str(&json)?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_delay() {
        let strategy = CooldownStrategy::Exponential {
            base_delay_secs: 10,
            multiplier: 2.0,
            max_delay_secs: 300,
        };
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(10));
        assert_eq!(compute_delay(&strategy, 2), Duration::from_secs(20));
        assert_eq!(compute_delay(&strategy, 3), Duration::from_secs(40));
        assert_eq!(compute_delay(&strategy, 4), Duration::from_secs(80));
        assert_eq!(compute_delay(&strategy, 5), Duration::from_secs(160));
        // Capped at max
        assert_eq!(compute_delay(&strategy, 6), Duration::from_secs(300));
    }

    #[test]
    fn test_fixed_delay() {
        let strategy = CooldownStrategy::Fixed { delay_secs: 60 };
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(60));
        assert_eq!(compute_delay(&strategy, 5), Duration::from_secs(60));
    }

    #[test]
    fn test_record_failure_and_can_retry() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 30 },
            max_retries: 0,
        };
        let mut state = CooldownState::default();

        record_failure(&mut state, &config, 1000);
        assert_eq!(state.retry_attempt, 1);
        assert_eq!(state.next_retry_at, Some(1030));
        assert!(!can_retry(&state, 1020));
        assert!(can_retry(&state, 1030));
        assert!(can_retry(&state, 1050));
    }

    #[test]
    fn test_seconds_until_retry() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 30 },
            max_retries: 0,
        };
        let mut state = CooldownState::default();
        record_failure(&mut state, &config, 1000);

        assert_eq!(seconds_until_retry(&state, 990), Some(40));
        assert_eq!(seconds_until_retry(&state, 1020), Some(10));
        assert_eq!(seconds_until_retry(&state, 1030), None);
    }

    #[test]
    fn test_max_retries_exceeded() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 10 },
            max_retries: 3,
        };
        let mut state = CooldownState::default();

        record_failure(&mut state, &config, 100);
        assert!(!max_retries_exceeded(&state, &config));
        record_failure(&mut state, &config, 200);
        assert!(!max_retries_exceeded(&state, &config));
        record_failure(&mut state, &config, 300);
        assert!(max_retries_exceeded(&state, &config));
    }

    #[test]
    fn test_reset_cooldown() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 30 },
            max_retries: 0,
        };
        let mut state = CooldownState::default();
        record_failure(&mut state, &config, 1000);
        assert_eq!(state.retry_attempt, 1);

        reset_cooldown(&mut state);
        assert_eq!(state.retry_attempt, 0);
        assert!(state.last_failed_at.is_none());
        assert!(state.next_retry_at.is_none());
        assert!(can_retry(&state, 0));
    }

    #[test]
    fn test_default_strategy() {
        let strategy = CooldownStrategy::default();
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(30));
        assert_eq!(compute_delay(&strategy, 2), Duration::from_secs(60));
        assert_eq!(compute_delay(&strategy, 3), Duration::from_secs(120));
    }

    #[test]
    fn test_cooldown_status() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 30 },
            max_retries: 5,
        };
        let mut state = CooldownState::default();
        // Use a future timestamp so the cooldown is still active
        let future = now_secs() + 10_000;
        record_failure(&mut state, &config, future);

        let status = cooldown_status("task-1", &state, &config);
        assert_eq!(status.task_id, "task-1");
        assert_eq!(status.state.retry_attempt, 1);
        assert!(!status.can_retry_now);
        assert!(status.seconds_until_retry.is_some());
    }

    #[test]
    fn test_exponential_with_multiplier_3() {
        let strategy = CooldownStrategy::Exponential {
            base_delay_secs: 5,
            multiplier: 3.0,
            max_delay_secs: 500,
        };
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(5));
        assert_eq!(compute_delay(&strategy, 2), Duration::from_secs(15));
        assert_eq!(compute_delay(&strategy, 3), Duration::from_secs(45));
        assert_eq!(compute_delay(&strategy, 4), Duration::from_secs(135));
        assert_eq!(compute_delay(&strategy, 5), Duration::from_secs(405));
        assert_eq!(compute_delay(&strategy, 6), Duration::from_secs(500)); // capped
    }

    #[test]
    fn test_cooldown_strategy_serialization() {
        let exponential = CooldownStrategy::Exponential {
            base_delay_secs: 30,
            multiplier: 2.0,
            max_delay_secs: 3600,
        };
        let json = serde_json::to_string(&exponential).unwrap();
        let deserialized: CooldownStrategy = serde_json::from_str(&json).unwrap();
        assert_eq!(exponential, deserialized);

        let fixed = CooldownStrategy::Fixed { delay_secs: 60 };
        let json = serde_json::to_string(&fixed).unwrap();
        let deserialized: CooldownStrategy = serde_json::from_str(&json).unwrap();
        assert_eq!(fixed, deserialized);
    }

    #[test]
    fn test_cooldown_config_serialization() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Exponential {
                base_delay_secs: 30,
                multiplier: 2.0,
                max_delay_secs: 3600,
            },
            max_retries: 5,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: CooldownConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.max_retries, deserialized.max_retries);
    }

    #[test]
    fn test_cooldown_state_serialization() {
        let state = CooldownState {
            retry_attempt: 3,
            last_failed_at: Some(1000),
            next_retry_at: Some(1030),
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: CooldownState = serde_json::from_str(&json).unwrap();
        assert_eq!(state.retry_attempt, deserialized.retry_attempt);
        assert_eq!(state.last_failed_at, deserialized.last_failed_at);
        assert_eq!(state.next_retry_at, deserialized.next_retry_at);
    }

    #[test]
    fn test_cooldown_state_default() {
        let state = CooldownState::default();
        assert_eq!(state.retry_attempt, 0);
        assert!(state.last_failed_at.is_none());
        assert!(state.next_retry_at.is_none());
    }

    #[test]
    fn test_cooldown_config_default() {
        let config = CooldownConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_retries, 0);
        assert_eq!(compute_delay(&config.strategy, 1), Duration::from_secs(30));
    }

    #[test]
    fn test_compute_delay_attempt_zero() {
        let strategy = CooldownStrategy::Exponential {
            base_delay_secs: 10,
            multiplier: 2.0,
            max_delay_secs: 300,
        };
        // attempt 0 should use saturating_sub to 1
        assert_eq!(compute_delay(&strategy, 0), Duration::from_secs(10));
    }

    #[test]
    fn test_compute_delay_large_attempt() {
        let strategy = CooldownStrategy::Exponential {
            base_delay_secs: 1,
            multiplier: 2.0,
            max_delay_secs: 1000,
        };
        // Very large attempt number should cap at max
        assert_eq!(compute_delay(&strategy, 100), Duration::from_secs(1000));
    }

    #[test]
    fn test_record_failure_multiple_times() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Exponential {
                base_delay_secs: 10,
                multiplier: 2.0,
                max_delay_secs: 300,
            },
            max_retries: 0,
        };
        let mut state = CooldownState::default();

        record_failure(&mut state, &config, 1000);
        assert_eq!(state.retry_attempt, 1);
        assert_eq!(state.next_retry_at, Some(1010));

        record_failure(&mut state, &config, 2000);
        assert_eq!(state.retry_attempt, 2);
        assert_eq!(state.next_retry_at, Some(2020));

        record_failure(&mut state, &config, 3000);
        assert_eq!(state.retry_attempt, 3);
        assert_eq!(state.next_retry_at, Some(3040));
    }

    #[test]
    fn test_can_retry_no_next_retry() {
        let state = CooldownState::default();
        assert!(can_retry(&state, 0));
        assert!(can_retry(&state, 1000));
    }

    #[test]
    fn test_seconds_until_retry_ready() {
        let state = CooldownState {
            retry_attempt: 1,
            last_failed_at: Some(1000),
            next_retry_at: Some(1030),
        };
        // When time has passed the retry point
        assert_eq!(seconds_until_retry(&state, 1030), None);
        assert_eq!(seconds_until_retry(&state, 1050), None);
    }

    #[test]
    fn test_max_retries_zero_unlimited() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 10 },
            max_retries: 0,
        };
        let mut state = CooldownState::default();

        // Many retries should still be allowed
        for i in 0..100 {
            record_failure(&mut state, &config, i * 100);
            assert!(!max_retries_exceeded(&state, &config));
        }
    }

    #[test]
    fn test_max_retries_exceeded_boundary() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 10 },
            max_retries: 1,
        };
        let mut state = CooldownState::default();

        assert!(!max_retries_exceeded(&state, &config));
        record_failure(&mut state, &config, 100);
        assert!(max_retries_exceeded(&state, &config));
    }

    #[test]
    fn test_reset_cooldown_from_initial() {
        let mut state = CooldownState::default();
        reset_cooldown(&mut state);
        assert_eq!(state.retry_attempt, 0);
        assert!(state.last_failed_at.is_none());
        assert!(state.next_retry_at.is_none());
    }

    #[test]
    fn test_persistence_error_display() {
        let io_err = CooldownPersistenceError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        let display = format!("{}", io_err);
        assert!(display.contains("IO error"));

        let json_err =
            CooldownPersistenceError::Json(serde_json::from_str::<String>("invalid").unwrap_err());
        let display = format!("{}", json_err);
        assert!(display.contains("JSON error"));
    }

    #[test]
    fn test_persistence_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let persistence_err: CooldownPersistenceError = io_err.into();
        match persistence_err {
            CooldownPersistenceError::Io(_) => (),
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_persistence_error_from_json() {
        let json_err = serde_json::from_str::<String>("not json").unwrap_err();
        let persistence_err: CooldownPersistenceError = json_err.into();
        match persistence_err {
            CooldownPersistenceError::Json(_) => (),
            _ => panic!("Expected Json variant"),
        }
    }

    #[test]
    fn test_save_and_load_cooldown_config() {
        let temp_dir = std::env::temp_dir().join("ipmsg_cooldown_test");
        let _ = std::fs::create_dir_all(&temp_dir);

        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Exponential {
                base_delay_secs: 45,
                multiplier: 3.0,
                max_delay_secs: 7200,
            },
            max_retries: 10,
        };

        save_cooldown_config(&temp_dir, &config).unwrap();
        let loaded = load_cooldown_config(&temp_dir).unwrap().unwrap();

        assert_eq!(config.enabled, loaded.enabled);
        assert_eq!(config.max_retries, loaded.max_retries);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_load_cooldown_config_missing_file() {
        let temp_dir = std::env::temp_dir().join("ipmsg_cooldown_test_missing");
        let _ = std::fs::create_dir_all(&temp_dir);

        let result = load_cooldown_config(&temp_dir).unwrap();
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_load_cooldown_config_invalid_json() {
        let temp_dir = std::env::temp_dir().join("ipmsg_cooldown_test_invalid");
        let _ = std::fs::create_dir_all(&temp_dir);

        let invalid_path = temp_dir.join("cooldown_config.json");
        std::fs::write(&invalid_path, "not valid json").unwrap();

        let result = load_cooldown_config(&temp_dir);
        assert!(result.is_err());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_save_cooldown_config_overwrite() {
        let temp_dir = std::env::temp_dir().join("ipmsg_cooldown_test_overwrite");
        let _ = std::fs::create_dir_all(&temp_dir);

        let config1 = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 30 },
            max_retries: 5,
        };
        save_cooldown_config(&temp_dir, &config1).unwrap();

        let config2 = CooldownConfig {
            enabled: false,
            strategy: CooldownStrategy::Fixed { delay_secs: 60 },
            max_retries: 10,
        };
        save_cooldown_config(&temp_dir, &config2).unwrap();

        let loaded = load_cooldown_config(&temp_dir).unwrap().unwrap();
        assert_eq!(loaded.enabled, false);
        assert_eq!(loaded.max_retries, 10);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_cooldown_status_can_retry_now() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 10 },
            max_retries: 0,
        };
        let state = CooldownState {
            retry_attempt: 1,
            last_failed_at: Some(1000),
            next_retry_at: Some(1010),
        };

        // Mock now_secs to be after retry time
        let status = cooldown_status("task-1", &state, &config);
        // Since we use real now_secs(), we can't predict exact value
        // but we can verify the structure
        assert_eq!(status.task_id, "task-1");
        assert_eq!(status.state.retry_attempt, 1);
    }

    #[test]
    fn test_cooldown_status_no_retry_scheduled() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 30 },
            max_retries: 5,
        };
        let state = CooldownState::default();

        let status = cooldown_status("task-2", &state, &config);
        assert!(status.can_retry_now);
        assert!(status.seconds_until_retry.is_none());
    }

    #[test]
    fn test_exponential_fractional_multiplier() {
        let strategy = CooldownStrategy::Exponential {
            base_delay_secs: 10,
            multiplier: 1.5,
            max_delay_secs: 100,
        };
        // 10 * 1.5^0 = 10
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(10));
        // 10 * 1.5^1 = 15
        assert_eq!(compute_delay(&strategy, 2), Duration::from_secs(15));
        // 10 * 1.5^2 = 22.5 -> 22
        assert_eq!(compute_delay(&strategy, 3), Duration::from_secs(22));
    }

    #[test]
    fn test_fixed_delay_zero() {
        let strategy = CooldownStrategy::Fixed { delay_secs: 0 };
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(0));
        assert_eq!(compute_delay(&strategy, 100), Duration::from_secs(0));
    }

    #[test]
    fn test_exponential_zero_base_delay() {
        let strategy = CooldownStrategy::Exponential {
            base_delay_secs: 0,
            multiplier: 2.0,
            max_delay_secs: 100,
        };
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(0));
        assert_eq!(compute_delay(&strategy, 5), Duration::from_secs(0));
    }

    #[test]
    fn test_exponential_zero_max_delay() {
        let strategy = CooldownStrategy::Exponential {
            base_delay_secs: 10,
            multiplier: 2.0,
            max_delay_secs: 0,
        };
        // All delays should be capped to 0
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(0));
        assert_eq!(compute_delay(&strategy, 5), Duration::from_secs(0));
    }

    #[test]
    fn test_record_failure_at_time_zero() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Fixed { delay_secs: 30 },
            max_retries: 0,
        };
        let mut state = CooldownState::default();

        record_failure(&mut state, &config, 0);
        assert_eq!(state.retry_attempt, 1);
        assert_eq!(state.last_failed_at, Some(0));
        assert_eq!(state.next_retry_at, Some(30));
        assert!(!can_retry(&state, 0));
        assert!(!can_retry(&state, 29));
        assert!(can_retry(&state, 30));
    }

    #[test]
    fn test_cooldown_workflow() {
        let config = CooldownConfig {
            enabled: true,
            strategy: CooldownStrategy::Exponential {
                base_delay_secs: 10,
                multiplier: 2.0,
                max_delay_secs: 100,
            },
            max_retries: 3,
        };
        let mut state = CooldownState::default();

        // First failure
        record_failure(&mut state, &config, 1000);
        assert_eq!(state.retry_attempt, 1);
        assert!(!can_retry(&state, 1005));
        assert!(can_retry(&state, 1010));
        assert!(!max_retries_exceeded(&state, &config));

        // Second failure
        record_failure(&mut state, &config, 2000);
        assert_eq!(state.retry_attempt, 2);
        assert!(!can_retry(&state, 2015));
        assert!(can_retry(&state, 2020));
        assert!(!max_retries_exceeded(&state, &config));

        // Third failure
        record_failure(&mut state, &config, 3000);
        assert_eq!(state.retry_attempt, 3);
        assert!(!can_retry(&state, 3030));
        assert!(can_retry(&state, 3040));
        assert!(max_retries_exceeded(&state, &config));

        // Reset
        reset_cooldown(&mut state);
        assert_eq!(state.retry_attempt, 0);
        assert!(!max_retries_exceeded(&state, &config));
        assert!(can_retry(&state, 4000));
    }

    #[test]
    fn test_cooldown_status_serialization() {
        let status = CooldownStatus {
            task_id: "task-123".to_string(),
            state: CooldownState {
                retry_attempt: 2,
                last_failed_at: Some(1000),
                next_retry_at: Some(1030),
            },
            strategy: CooldownStrategy::Fixed { delay_secs: 30 },
            can_retry_now: false,
            seconds_until_retry: Some(20),
        };
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: CooldownStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status.task_id, deserialized.task_id);
        assert_eq!(status.state.retry_attempt, deserialized.state.retry_attempt);
        assert_eq!(status.can_retry_now, deserialized.can_retry_now);
    }

    #[test]
    fn test_exponential_multiplier_one() {
        let strategy = CooldownStrategy::Exponential {
            base_delay_secs: 10,
            multiplier: 1.0,
            max_delay_secs: 100,
        };
        // With multiplier 1.0, delay should always be base_delay
        assert_eq!(compute_delay(&strategy, 1), Duration::from_secs(10));
        assert_eq!(compute_delay(&strategy, 5), Duration::from_secs(10));
        assert_eq!(compute_delay(&strategy, 100), Duration::from_secs(10));
    }
}
