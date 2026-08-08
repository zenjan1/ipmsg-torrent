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
}
