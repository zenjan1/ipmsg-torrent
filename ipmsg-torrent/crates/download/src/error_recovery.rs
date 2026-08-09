//! Error classification and automatic recovery for download tasks.
//!
//! Classifies download errors into categories and applies appropriate
//! recovery strategies automatically. Integrates with the cooldown
//! system for intelligent retry behavior.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Categories of download errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Network connectivity issues (DNS, connection refused, timeout).
    Network,
    /// Disk I/O errors (no space, permission denied, file locked).
    Disk,
    /// Authentication failures (401, 403, invalid credentials).
    Authentication,
    /// Server-side errors (5xx responses, service unavailable).
    Server,
    /// Protocol-specific errors (invalid torrent, bad ed2k link).
    Protocol,
    /// Resource not found (404, file removed).
    NotFound,
    /// Rate limiting or throttling (429, too many requests).
    RateLimited,
    /// SSL/TLS certificate errors.
    Certificate,
    /// Unknown or unclassified errors.
    Unknown,
}

impl ErrorCategory {
    /// Classify an error message into a category.
    pub fn from_error_message(error: &str) -> Self {
        let lower = error.to_lowercase();

        // Network errors
        if lower.contains("dns")
            || lower.contains("connection refused")
            || lower.contains("connection reset")
            || lower.contains("network unreachable")
            || lower.contains("timeout")
            || lower.contains("timed out")
            || lower.contains("no route to host")
        {
            return Self::Network;
        }

        // Disk errors
        if lower.contains("no space left")
            || lower.contains("permission denied")
            || lower.contains("disk full")
            || lower.contains("read-only")
            || lower.contains("file locked")
            || lower.contains("io error")
        {
            return Self::Disk;
        }

        // Authentication errors
        if lower.contains("401")
            || lower.contains("403")
            || lower.contains("unauthorized")
            || lower.contains("forbidden")
            || lower.contains("authentication")
            || lower.contains("invalid credentials")
        {
            return Self::Authentication;
        }

        // Server errors
        if lower.contains("500")
            || lower.contains("502")
            || lower.contains("503")
            || lower.contains("504")
            || lower.contains("internal server error")
            || lower.contains("bad gateway")
            || lower.contains("service unavailable")
        {
            return Self::Server;
        }

        // Not found
        if lower.contains("404") || lower.contains("not found") {
            return Self::NotFound;
        }

        // Rate limiting
        if lower.contains("429")
            || lower.contains("too many requests")
            || lower.contains("rate limit")
        {
            return Self::RateLimited;
        }

        // Certificate errors
        if lower.contains("certificate")
            || lower.contains("ssl")
            || lower.contains("tls")
            || lower.contains("handshake failed")
            || lower.contains("invalid certificate")
        {
            return Self::Certificate;
        }

        // Protocol errors
        if lower.contains("invalid torrent")
            || lower.contains("bad ed2k")
            || lower.contains("invalid magnet")
            || lower.contains("protocol error")
            || lower.contains("malformed")
        {
            return Self::Protocol;
        }

        Self::Unknown
    }

    /// Get a human-readable description of the category.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Network => "Network connectivity issue",
            Self::Disk => "Disk I/O error",
            Self::Authentication => "Authentication failure",
            Self::Server => "Server-side error",
            Self::Protocol => "Protocol-specific error",
            Self::NotFound => "Resource not found",
            Self::RateLimited => "Rate limited",
            Self::Certificate => "SSL/TLS certificate error",
            Self::Unknown => "Unknown error",
        }
    }

    /// Get emoji indicator for the category.
    pub fn emoji(&self) -> &'static str {
        match self {
            Self::Network => "🌐",
            Self::Disk => "💾",
            Self::Authentication => "🔐",
            Self::Server => "🖥️",
            Self::Protocol => "📡",
            Self::NotFound => "❓",
            Self::RateLimited => "⏳",
            Self::Certificate => "🔒",
            Self::Unknown => "❔",
        }
    }
}

/// Recovery strategy for a specific error category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStrategy {
    /// Retry the download with cooldown backoff.
    Retry,
    /// Skip this mirror/URL and try alternatives.
    Skip,
    /// Pause the task and wait for user intervention.
    Pause,
    /// Abort the task permanently.
    Abort,
}

impl RecoveryStrategy {
    /// Get a human-readable description.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Retry => "Retry with backoff",
            Self::Skip => "Skip and try alternatives",
            Self::Pause => "Pause for user intervention",
            Self::Abort => "Abort permanently",
        }
    }
}

/// Configuration for error recovery behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorRecoveryConfig {
    /// Enable automatic error recovery.
    pub enabled: bool,
    /// Mapping of error categories to recovery strategies.
    pub category_strategies: HashMap<ErrorCategory, RecoveryStrategy>,
    /// Maximum consecutive failures before aborting (regardless of strategy).
    pub max_consecutive_failures: u32,
    /// Whether to automatically switch mirrors on network errors.
    pub auto_switch_mirror: bool,
}

impl Default for ErrorRecoveryConfig {
    fn default() -> Self {
        let mut category_strategies = HashMap::new();

        // Default strategies for each category
        category_strategies.insert(ErrorCategory::Network, RecoveryStrategy::Retry);
        category_strategies.insert(ErrorCategory::Disk, RecoveryStrategy::Pause);
        category_strategies.insert(ErrorCategory::Authentication, RecoveryStrategy::Pause);
        category_strategies.insert(ErrorCategory::Server, RecoveryStrategy::Retry);
        category_strategies.insert(ErrorCategory::Protocol, RecoveryStrategy::Abort);
        category_strategies.insert(ErrorCategory::NotFound, RecoveryStrategy::Skip);
        category_strategies.insert(ErrorCategory::RateLimited, RecoveryStrategy::Retry);
        category_strategies.insert(ErrorCategory::Certificate, RecoveryStrategy::Pause);
        category_strategies.insert(ErrorCategory::Unknown, RecoveryStrategy::Retry);

        Self {
            enabled: true,
            category_strategies,
            max_consecutive_failures: 10,
            auto_switch_mirror: true,
        }
    }
}

/// Decision made by the error recovery system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryDecision {
    /// The classified error category.
    pub category: ErrorCategory,
    /// The recommended recovery strategy.
    pub strategy: RecoveryStrategy,
    /// Human-readable explanation.
    pub explanation: String,
    /// Whether this decision overrides the default strategy due to consecutive failures.
    pub overridden: bool,
}

/// Error recovery manager.
#[derive(Debug, Clone)]
pub struct ErrorRecoveryManager {
    config: ErrorRecoveryConfig,
}

impl ErrorRecoveryManager {
    /// Create a new manager with default configuration.
    pub fn new() -> Self {
        Self {
            config: ErrorRecoveryConfig::default(),
        }
    }

    /// Create a manager from existing configuration.
    pub fn from_config(config: ErrorRecoveryConfig) -> Self {
        Self { config }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &ErrorRecoveryConfig {
        &self.config
    }

    /// Classify an error and determine recovery strategy.
    pub fn classify_and_decide(&self, error: &str, consecutive_failures: u32) -> RecoveryDecision {
        let category = ErrorCategory::from_error_message(error);

        // Check if we should override due to consecutive failures
        let overridden = consecutive_failures >= self.config.max_consecutive_failures;

        let strategy = if overridden {
            RecoveryStrategy::Abort
        } else {
            self.config
                .category_strategies
                .get(&category)
                .copied()
                .unwrap_or(RecoveryStrategy::Retry)
        };

        let explanation = if overridden {
            format!(
                "{} {} exceeded max consecutive failures ({}), aborting",
                category.emoji(),
                category.description(),
                self.config.max_consecutive_failures
            )
        } else {
            format!(
                "{} {} → {}",
                category.emoji(),
                category.description(),
                strategy.description()
            )
        };

        RecoveryDecision {
            category,
            strategy,
            explanation,
            overridden,
        }
    }

    /// Update the configuration.
    pub fn set_config(&mut self, config: ErrorRecoveryConfig) {
        self.config = config;
    }

    /// Set the recovery strategy for a specific category.
    pub fn set_category_strategy(&mut self, category: ErrorCategory, strategy: RecoveryStrategy) {
        self.config.category_strategies.insert(category, strategy);
    }

    /// Get the recovery strategy for a specific category.
    pub fn get_category_strategy(&self, category: ErrorCategory) -> Option<RecoveryStrategy> {
        self.config.category_strategies.get(&category).copied()
    }

    /// Reset all strategies to defaults.
    pub fn reset_strategies(&mut self) {
        self.config.category_strategies = ErrorRecoveryConfig::default().category_strategies;
    }
}

impl Default for ErrorRecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Persistence functions
const CONFIG_FILE: &str = "error_recovery_config.json";

/// Save error recovery configuration to disk.
pub fn save_error_recovery_config(
    config: &ErrorRecoveryConfig,
    data_dir: &Path,
) -> Result<(), ErrorRecoveryPersistenceError> {
    let path = data_dir.join(CONFIG_FILE);
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| ErrorRecoveryPersistenceError::Serialize(e.to_string()))?;

    // Atomic write
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, &json).map_err(|e| ErrorRecoveryPersistenceError::Io(e.to_string()))?;
    fs::rename(&temp_path, &path).map_err(|e| ErrorRecoveryPersistenceError::Io(e.to_string()))?;

    Ok(())
}

/// Load error recovery configuration from disk.
pub fn load_error_recovery_config(
    data_dir: &Path,
) -> Result<Option<ErrorRecoveryConfig>, ErrorRecoveryPersistenceError> {
    let path = data_dir.join(CONFIG_FILE);
    if !path.exists() {
        return Ok(None);
    }

    let json =
        fs::read_to_string(&path).map_err(|e| ErrorRecoveryPersistenceError::Io(e.to_string()))?;
    let config = serde_json::from_str(&json)
        .map_err(|e| ErrorRecoveryPersistenceError::Deserialize(e.to_string()))?;

    Ok(Some(config))
}

/// Error type for persistence operations.
#[derive(Debug, thiserror::Error)]
pub enum ErrorRecoveryPersistenceError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("Serialize error: {0}")]
    Serialize(String),
    #[error("Deserialize error: {0}")]
    Deserialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_category_classification() {
        assert_eq!(
            ErrorCategory::from_error_message("connection timeout"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("DNS resolution failed"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("no space left on device"),
            ErrorCategory::Disk
        );
        assert_eq!(
            ErrorCategory::from_error_message("401 Unauthorized"),
            ErrorCategory::Authentication
        );
        assert_eq!(
            ErrorCategory::from_error_message("500 Internal Server Error"),
            ErrorCategory::Server
        );
        assert_eq!(
            ErrorCategory::from_error_message("404 Not Found"),
            ErrorCategory::NotFound
        );
        assert_eq!(
            ErrorCategory::from_error_message("429 Too Many Requests"),
            ErrorCategory::RateLimited
        );
        assert_eq!(
            ErrorCategory::from_error_message("invalid certificate"),
            ErrorCategory::Certificate
        );
        assert_eq!(
            ErrorCategory::from_error_message("invalid torrent file"),
            ErrorCategory::Protocol
        );
        assert_eq!(
            ErrorCategory::from_error_message("something weird"),
            ErrorCategory::Unknown
        );
    }

    #[test]
    fn test_default_strategies() {
        let manager = ErrorRecoveryManager::new();

        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Retry)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Disk),
            Some(RecoveryStrategy::Pause)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Authentication),
            Some(RecoveryStrategy::Pause)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::NotFound),
            Some(RecoveryStrategy::Skip)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Protocol),
            Some(RecoveryStrategy::Abort)
        );
    }

    #[test]
    fn test_recovery_decision() {
        let manager = ErrorRecoveryManager::new();

        let decision = manager.classify_and_decide("connection timeout", 0);
        assert_eq!(decision.category, ErrorCategory::Network);
        assert_eq!(decision.strategy, RecoveryStrategy::Retry);
        assert!(!decision.overridden);

        let decision = manager.classify_and_decide("connection timeout", 10);
        assert_eq!(decision.strategy, RecoveryStrategy::Abort);
        assert!(decision.overridden);
    }

    #[test]
    fn test_custom_strategy() {
        let mut manager = ErrorRecoveryManager::new();
        manager.set_category_strategy(ErrorCategory::Network, RecoveryStrategy::Pause);

        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Pause)
        );

        let decision = manager.classify_and_decide("connection timeout", 0);
        assert_eq!(decision.strategy, RecoveryStrategy::Pause);
    }

    #[test]
    fn test_consecutive_failure_override() {
        let mut manager = ErrorRecoveryManager::new();
        manager.config.max_consecutive_failures = 5;

        let decision = manager.classify_and_decide("connection timeout", 4);
        assert_eq!(decision.strategy, RecoveryStrategy::Retry);
        assert!(!decision.overridden);

        let decision = manager.classify_and_decide("connection timeout", 5);
        assert_eq!(decision.strategy, RecoveryStrategy::Abort);
        assert!(decision.overridden);
    }

    #[test]
    fn test_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ErrorRecoveryConfig::default();

        // Save
        save_error_recovery_config(&config, temp_dir.path()).unwrap();

        // Load
        let loaded = load_error_recovery_config(temp_dir.path()).unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(
            loaded.max_consecutive_failures,
            config.max_consecutive_failures
        );

        // Missing file
        let empty_dir = tempfile::tempdir().unwrap();
        let loaded = load_error_recovery_config(empty_dir.path()).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_category_descriptions() {
        assert_eq!(
            ErrorCategory::Network.description(),
            "Network connectivity issue"
        );
        assert_eq!(ErrorCategory::Disk.description(), "Disk I/O error");
        assert_eq!(
            ErrorCategory::Authentication.description(),
            "Authentication failure"
        );
        assert_eq!(ErrorCategory::Server.description(), "Server-side error");
        assert_eq!(
            ErrorCategory::Protocol.description(),
            "Protocol-specific error"
        );
        assert_eq!(ErrorCategory::NotFound.description(), "Resource not found");
        assert_eq!(ErrorCategory::RateLimited.description(), "Rate limited");
        assert_eq!(
            ErrorCategory::Certificate.description(),
            "SSL/TLS certificate error"
        );
        assert_eq!(ErrorCategory::Unknown.description(), "Unknown error");
    }

    #[test]
    fn test_category_emojis() {
        assert_eq!(ErrorCategory::Network.emoji(), "🌐");
        assert_eq!(ErrorCategory::Disk.emoji(), "💾");
        assert_eq!(ErrorCategory::Authentication.emoji(), "🔐");
        assert_eq!(ErrorCategory::Server.emoji(), "🖥️");
        assert_eq!(ErrorCategory::Protocol.emoji(), "📡");
        assert_eq!(ErrorCategory::NotFound.emoji(), "❓");
        assert_eq!(ErrorCategory::RateLimited.emoji(), "⏳");
        assert_eq!(ErrorCategory::Certificate.emoji(), "🔒");
        assert_eq!(ErrorCategory::Unknown.emoji(), "❔");
    }

    #[test]
    fn test_strategy_descriptions() {
        assert_eq!(RecoveryStrategy::Retry.description(), "Retry with backoff");
        assert_eq!(
            RecoveryStrategy::Skip.description(),
            "Skip and try alternatives"
        );
        assert_eq!(
            RecoveryStrategy::Pause.description(),
            "Pause for user intervention"
        );
        assert_eq!(RecoveryStrategy::Abort.description(), "Abort permanently");
    }

    #[test]
    fn test_reset_strategies() {
        let mut manager = ErrorRecoveryManager::new();
        manager.set_category_strategy(ErrorCategory::Network, RecoveryStrategy::Abort);

        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Abort)
        );

        manager.reset_strategies();

        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Retry)
        );
    }

    #[test]
    fn test_case_insensitive_classification() {
        assert_eq!(
            ErrorCategory::from_error_message("CONNECTION TIMEOUT"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("Connection Refused"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("NO SPACE LEFT ON DEVICE"),
            ErrorCategory::Disk
        );
    }

    #[test]
    fn test_multiple_keywords_same_category() {
        // All these should be Network
        assert_eq!(
            ErrorCategory::from_error_message("dns lookup failed"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("connection reset by peer"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("network unreachable"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("operation timed out"),
            ErrorCategory::Network
        );

        // All these should be Disk
        assert_eq!(
            ErrorCategory::from_error_message("permission denied"),
            ErrorCategory::Disk
        );
        assert_eq!(
            ErrorCategory::from_error_message("disk full"),
            ErrorCategory::Disk
        );
        assert_eq!(
            ErrorCategory::from_error_message("read-only file system"),
            ErrorCategory::Disk
        );
    }
}
