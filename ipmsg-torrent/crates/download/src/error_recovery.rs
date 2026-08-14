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

    // ===== Phase 219: Comprehensive Test Coverage =====

    // --- ErrorCategory serde roundtrip (snake_case) ---
    #[test]
    fn error_category_serde_roundtrip_all_variants() {
        let variants = [
            ErrorCategory::Network,
            ErrorCategory::Disk,
            ErrorCategory::Authentication,
            ErrorCategory::Server,
            ErrorCategory::Protocol,
            ErrorCategory::NotFound,
            ErrorCategory::RateLimited,
            ErrorCategory::Certificate,
            ErrorCategory::Unknown,
        ];
        for cat in &variants {
            let json = serde_json::to_string(cat).unwrap();
            let back: ErrorCategory = serde_json::from_str(&json).unwrap();
            assert_eq!(*cat, back);
        }
    }

    #[test]
    fn error_category_serde_snake_case_values() {
        assert_eq!(
            serde_json::to_string(&ErrorCategory::Network).unwrap(),
            "\"network\""
        );
        assert_eq!(
            serde_json::to_string(&ErrorCategory::Disk).unwrap(),
            "\"disk\""
        );
        assert_eq!(
            serde_json::to_string(&ErrorCategory::Authentication).unwrap(),
            "\"authentication\""
        );
        assert_eq!(
            serde_json::to_string(&ErrorCategory::Server).unwrap(),
            "\"server\""
        );
        assert_eq!(
            serde_json::to_string(&ErrorCategory::Protocol).unwrap(),
            "\"protocol\""
        );
        assert_eq!(
            serde_json::to_string(&ErrorCategory::NotFound).unwrap(),
            "\"not_found\""
        );
        assert_eq!(
            serde_json::to_string(&ErrorCategory::RateLimited).unwrap(),
            "\"rate_limited\""
        );
        assert_eq!(
            serde_json::to_string(&ErrorCategory::Certificate).unwrap(),
            "\"certificate\""
        );
        assert_eq!(
            serde_json::to_string(&ErrorCategory::Unknown).unwrap(),
            "\"unknown\""
        );
    }

    // --- ErrorCategory traits ---
    #[test]
    fn error_category_clone_copy_debug() {
        let cat = ErrorCategory::Network;
        let cloned = cat.clone();
        assert_eq!(cat, cloned);
        // Copy: using after move
        let _moved = cat;
        assert_eq!(cat, ErrorCategory::Network);
        // Debug
        let debug_str = format!("{:?}", cat);
        assert!(debug_str.contains("Network"));
    }

    #[test]
    fn error_category_eq_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ErrorCategory::Network);
        set.insert(ErrorCategory::Disk);
        set.insert(ErrorCategory::Network); // duplicate
        assert_eq!(set.len(), 2);
        assert!(set.contains(&ErrorCategory::Network));
        assert!(set.contains(&ErrorCategory::Disk));
    }

    // --- Classification: all Network keywords ---
    #[test]
    fn classify_network_all_keywords() {
        let keywords = [
            "dns",
            "connection refused",
            "connection reset",
            "network unreachable",
            "timeout",
            "timed out",
            "no route to host",
        ];
        for kw in &keywords {
            assert_eq!(
                ErrorCategory::from_error_message(kw),
                ErrorCategory::Network,
                "Failed for keyword: {}",
                kw
            );
        }
    }

    // --- Classification: all Disk keywords ---
    #[test]
    fn classify_disk_all_keywords() {
        let keywords = [
            "no space left",
            "permission denied",
            "disk full",
            "read-only",
            "file locked",
            "io error",
        ];
        for kw in &keywords {
            assert_eq!(
                ErrorCategory::from_error_message(kw),
                ErrorCategory::Disk,
                "Failed for keyword: {}",
                kw
            );
        }
    }

    // --- Classification: all Authentication keywords ---
    #[test]
    fn classify_auth_all_keywords() {
        let keywords = [
            "401",
            "403",
            "unauthorized",
            "forbidden",
            "authentication",
            "invalid credentials",
        ];
        for kw in &keywords {
            assert_eq!(
                ErrorCategory::from_error_message(kw),
                ErrorCategory::Authentication,
                "Failed for keyword: {}",
                kw
            );
        }
    }

    // --- Classification: all Server keywords ---
    #[test]
    fn classify_server_all_keywords() {
        let keywords = [
            "500",
            "502",
            "503",
            "504",
            "internal server error",
            "bad gateway",
            "service unavailable",
        ];
        for kw in &keywords {
            assert_eq!(
                ErrorCategory::from_error_message(kw),
                ErrorCategory::Server,
                "Failed for keyword: {}",
                kw
            );
        }
    }

    // --- Classification: all RateLimited keywords ---
    #[test]
    fn classify_rate_limited_all_keywords() {
        let keywords = ["429", "too many requests", "rate limit"];
        for kw in &keywords {
            assert_eq!(
                ErrorCategory::from_error_message(kw),
                ErrorCategory::RateLimited,
                "Failed for keyword: {}",
                kw
            );
        }
    }

    // --- Classification: all Certificate keywords ---
    #[test]
    fn classify_certificate_all_keywords() {
        let keywords = [
            "certificate",
            "ssl",
            "tls",
            "handshake failed",
            "invalid certificate",
        ];
        for kw in &keywords {
            assert_eq!(
                ErrorCategory::from_error_message(kw),
                ErrorCategory::Certificate,
                "Failed for keyword: {}",
                kw
            );
        }
    }

    // --- Classification: all Protocol keywords ---
    #[test]
    fn classify_protocol_all_keywords() {
        let keywords = [
            "invalid torrent",
            "bad ed2k",
            "invalid magnet",
            "protocol error",
            "malformed",
        ];
        for kw in &keywords {
            assert_eq!(
                ErrorCategory::from_error_message(kw),
                ErrorCategory::Protocol,
                "Failed for keyword: {}",
                kw
            );
        }
    }

    // --- Classification: empty string → Unknown ---
    #[test]
    fn classify_empty_string() {
        assert_eq!(
            ErrorCategory::from_error_message(""),
            ErrorCategory::Unknown
        );
    }

    // --- Classification: Unicode error messages ---
    #[test]
    fn classify_unicode_messages() {
        assert_eq!(
            ErrorCategory::from_error_message("连接超时"),
            ErrorCategory::Unknown
        );
        assert_eq!(
            ErrorCategory::from_error_message("エラー: timeout"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("🔒 ssl handshake failed"),
            ErrorCategory::Certificate
        );
    }

    // --- Classification priority: first match wins ---
    #[test]
    fn classify_priority_network_before_disk() {
        // "connection refused" matches Network first
        assert_eq!(
            ErrorCategory::from_error_message("connection refused and permission denied"),
            ErrorCategory::Network
        );
    }

    #[test]
    fn classify_priority_server_before_notfound() {
        // "500" matches Server first
        assert_eq!(
            ErrorCategory::from_error_message("500 error not found"),
            ErrorCategory::Server
        );
    }

    // --- RecoveryStrategy serde roundtrip ---
    #[test]
    fn recovery_strategy_serde_roundtrip_all() {
        let variants = [
            RecoveryStrategy::Retry,
            RecoveryStrategy::Skip,
            RecoveryStrategy::Pause,
            RecoveryStrategy::Abort,
        ];
        for strat in &variants {
            let json = serde_json::to_string(strat).unwrap();
            let back: RecoveryStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(*strat, back);
        }
    }

    #[test]
    fn recovery_strategy_serde_snake_case_values() {
        assert_eq!(
            serde_json::to_string(&RecoveryStrategy::Retry).unwrap(),
            "\"retry\""
        );
        assert_eq!(
            serde_json::to_string(&RecoveryStrategy::Skip).unwrap(),
            "\"skip\""
        );
        assert_eq!(
            serde_json::to_string(&RecoveryStrategy::Pause).unwrap(),
            "\"pause\""
        );
        assert_eq!(
            serde_json::to_string(&RecoveryStrategy::Abort).unwrap(),
            "\"abort\""
        );
    }

    #[test]
    fn recovery_strategy_clone_copy_debug() {
        let s = RecoveryStrategy::Retry;
        let cloned = s.clone();
        assert_eq!(s, cloned);
        let _moved = s;
        assert_eq!(s, RecoveryStrategy::Retry);
        let debug_str = format!("{:?}", s);
        assert!(debug_str.contains("Retry"));
    }

    // --- ErrorRecoveryConfig ---
    #[test]
    fn config_default_values() {
        let config = ErrorRecoveryConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_consecutive_failures, 10);
        assert!(config.auto_switch_mirror);
        assert_eq!(config.category_strategies.len(), 9);
    }

    #[test]
    fn config_default_strategies_mapping() {
        let config = ErrorRecoveryConfig::default();
        assert_eq!(
            config.category_strategies[&ErrorCategory::Network],
            RecoveryStrategy::Retry
        );
        assert_eq!(
            config.category_strategies[&ErrorCategory::Disk],
            RecoveryStrategy::Pause
        );
        assert_eq!(
            config.category_strategies[&ErrorCategory::Authentication],
            RecoveryStrategy::Pause
        );
        assert_eq!(
            config.category_strategies[&ErrorCategory::Server],
            RecoveryStrategy::Retry
        );
        assert_eq!(
            config.category_strategies[&ErrorCategory::Protocol],
            RecoveryStrategy::Abort
        );
        assert_eq!(
            config.category_strategies[&ErrorCategory::NotFound],
            RecoveryStrategy::Skip
        );
        assert_eq!(
            config.category_strategies[&ErrorCategory::RateLimited],
            RecoveryStrategy::Retry
        );
        assert_eq!(
            config.category_strategies[&ErrorCategory::Certificate],
            RecoveryStrategy::Pause
        );
        assert_eq!(
            config.category_strategies[&ErrorCategory::Unknown],
            RecoveryStrategy::Retry
        );
    }

    #[test]
    fn config_custom_values() {
        let config = ErrorRecoveryConfig {
            enabled: false,
            category_strategies: HashMap::new(),
            max_consecutive_failures: 3,
            auto_switch_mirror: false,
        };
        assert!(!config.enabled);
        assert_eq!(config.max_consecutive_failures, 3);
        assert!(!config.auto_switch_mirror);
        assert!(config.category_strategies.is_empty());
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = ErrorRecoveryConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: ErrorRecoveryConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.enabled, config.enabled);
        assert_eq!(
            back.max_consecutive_failures,
            config.max_consecutive_failures
        );
        assert_eq!(back.auto_switch_mirror, config.auto_switch_mirror);
        assert_eq!(
            back.category_strategies.len(),
            config.category_strategies.len()
        );
    }

    #[test]
    fn config_pretty_serde() {
        let config = ErrorRecoveryConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let back: ErrorRecoveryConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.enabled, config.enabled);
    }

    #[test]
    fn config_extra_field_tolerance() {
        let json = r#"{
            "enabled": true,
            "category_strategies": {},
            "max_consecutive_failures": 5,
            "auto_switch_mirror": false,
            "unknown_future_field": 42
        }"#;
        let config: ErrorRecoveryConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_consecutive_failures, 5);
        assert!(!config.auto_switch_mirror);
    }

    #[test]
    fn config_clone_debug() {
        let config = ErrorRecoveryConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(
            cloned.max_consecutive_failures,
            config.max_consecutive_failures
        );
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("ErrorRecoveryConfig"));
    }

    // --- RecoveryDecision ---
    #[test]
    fn recovery_decision_serde_roundtrip() {
        let decision = RecoveryDecision {
            category: ErrorCategory::Network,
            strategy: RecoveryStrategy::Retry,
            explanation: "test explanation".to_string(),
            overridden: false,
        };
        let json = serde_json::to_string(&decision).unwrap();
        let back: RecoveryDecision = serde_json::from_str(&json).unwrap();
        assert_eq!(back.category, decision.category);
        assert_eq!(back.strategy, decision.strategy);
        assert_eq!(back.explanation, decision.explanation);
        assert_eq!(back.overridden, decision.overridden);
    }

    #[test]
    fn recovery_decision_clone_debug() {
        let decision = RecoveryDecision {
            category: ErrorCategory::Disk,
            strategy: RecoveryStrategy::Pause,
            explanation: "disk error".to_string(),
            overridden: true,
        };
        let cloned = decision.clone();
        assert_eq!(cloned.category, decision.category);
        assert_eq!(cloned.strategy, decision.strategy);
        assert_eq!(cloned.explanation, decision.explanation);
        assert_eq!(cloned.overridden, decision.overridden);
        let debug_str = format!("{:?}", decision);
        assert!(debug_str.contains("RecoveryDecision"));
    }

    // --- ErrorRecoveryManager ---
    #[test]
    fn manager_new_equals_default() {
        let new = ErrorRecoveryManager::new();
        let default = ErrorRecoveryManager::default();
        assert_eq!(new.config().enabled, default.config().enabled);
        assert_eq!(
            new.config().max_consecutive_failures,
            default.config().max_consecutive_failures
        );
        assert_eq!(
            new.config().auto_switch_mirror,
            default.config().auto_switch_mirror
        );
    }

    #[test]
    fn manager_from_config() {
        let config = ErrorRecoveryConfig {
            enabled: false,
            category_strategies: HashMap::new(),
            max_consecutive_failures: 7,
            auto_switch_mirror: false,
        };
        let manager = ErrorRecoveryManager::from_config(config);
        assert!(!manager.config().enabled);
        assert_eq!(manager.config().max_consecutive_failures, 7);
        assert!(!manager.config().auto_switch_mirror);
    }

    #[test]
    fn manager_config_accessor() {
        let manager = ErrorRecoveryManager::new();
        let config = manager.config();
        assert!(config.enabled);
        assert_eq!(config.max_consecutive_failures, 10);
    }

    #[test]
    fn manager_clone_debug() {
        let manager = ErrorRecoveryManager::new();
        let cloned = manager.clone();
        assert_eq!(cloned.config().enabled, manager.config().enabled);
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("ErrorRecoveryManager"));
    }

    #[test]
    fn manager_set_config_replaces() {
        let mut manager = ErrorRecoveryManager::new();
        let new_config = ErrorRecoveryConfig {
            enabled: false,
            category_strategies: HashMap::new(),
            max_consecutive_failures: 1,
            auto_switch_mirror: false,
        };
        manager.set_config(new_config);
        assert!(!manager.config().enabled);
        assert_eq!(manager.config().max_consecutive_failures, 1);
        assert!(manager.config().category_strategies.is_empty());
    }

    // --- classify_and_decide: all 9 categories ---
    #[test]
    fn classify_decide_all_categories() {
        let manager = ErrorRecoveryManager::new();

        let cases = [
            (
                "connection timeout",
                ErrorCategory::Network,
                RecoveryStrategy::Retry,
            ),
            (
                "no space left",
                ErrorCategory::Disk,
                RecoveryStrategy::Pause,
            ),
            (
                "401 Unauthorized",
                ErrorCategory::Authentication,
                RecoveryStrategy::Pause,
            ),
            (
                "500 Internal Server Error",
                ErrorCategory::Server,
                RecoveryStrategy::Retry,
            ),
            (
                "invalid torrent file",
                ErrorCategory::Protocol,
                RecoveryStrategy::Abort,
            ),
            (
                "404 Not Found",
                ErrorCategory::NotFound,
                RecoveryStrategy::Skip,
            ),
            (
                "429 Too Many Requests",
                ErrorCategory::RateLimited,
                RecoveryStrategy::Retry,
            ),
            (
                "invalid certificate",
                ErrorCategory::Certificate,
                RecoveryStrategy::Pause,
            ),
            (
                "something weird",
                ErrorCategory::Unknown,
                RecoveryStrategy::Retry,
            ),
        ];

        for (error, expected_cat, expected_strat) in &cases {
            let decision = manager.classify_and_decide(error, 0);
            assert_eq!(
                decision.category, *expected_cat,
                "Wrong category for: {}",
                error
            );
            assert_eq!(
                decision.strategy, *expected_strat,
                "Wrong strategy for: {}",
                error
            );
            assert!(!decision.overridden);
        }
    }

    #[test]
    fn classify_decide_overridden_explanation() {
        let manager = ErrorRecoveryManager::new();
        let decision = manager.classify_and_decide("connection timeout", 10);
        assert!(decision.overridden);
        assert_eq!(decision.strategy, RecoveryStrategy::Abort);
        assert!(
            decision
                .explanation
                .contains("exceeded max consecutive failures")
        );
        assert!(decision.explanation.contains("10"));
    }

    #[test]
    fn classify_decide_normal_explanation() {
        let manager = ErrorRecoveryManager::new();
        let decision = manager.classify_and_decide("connection timeout", 0);
        assert!(!decision.overridden);
        assert!(decision.explanation.contains("Network connectivity issue"));
        assert!(decision.explanation.contains("Retry with backoff"));
    }

    #[test]
    fn classify_decide_explanation_contains_emoji() {
        let manager = ErrorRecoveryManager::new();
        let decision = manager.classify_and_decide("connection timeout", 0);
        assert!(decision.explanation.contains("🌐"));
    }

    // --- get_category_strategy ---
    #[test]
    fn get_category_strategy_all_defaults() {
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
            manager.get_category_strategy(ErrorCategory::Server),
            Some(RecoveryStrategy::Retry)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Protocol),
            Some(RecoveryStrategy::Abort)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::NotFound),
            Some(RecoveryStrategy::Skip)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::RateLimited),
            Some(RecoveryStrategy::Retry)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Certificate),
            Some(RecoveryStrategy::Pause)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Unknown),
            Some(RecoveryStrategy::Retry)
        );
    }

    // --- Boundary: max_consecutive_failures=0 (always override) ---
    #[test]
    fn max_failures_zero_always_override() {
        let mut manager = ErrorRecoveryManager::new();
        manager.config.max_consecutive_failures = 0;
        let decision = manager.classify_and_decide("connection timeout", 0);
        assert!(decision.overridden);
        assert_eq!(decision.strategy, RecoveryStrategy::Abort);
    }

    #[test]
    fn max_failures_one_boundary() {
        let mut manager = ErrorRecoveryManager::new();
        manager.config.max_consecutive_failures = 1;
        // 0 failures: no override
        let decision = manager.classify_and_decide("connection timeout", 0);
        assert!(!decision.overridden);
        // 1 failure: override
        let decision = manager.classify_and_decide("connection timeout", 1);
        assert!(decision.overridden);
    }

    // --- set_category_strategy ---
    #[test]
    fn set_category_strategy_overwrite() {
        let mut manager = ErrorRecoveryManager::new();
        // Default is Retry for Network
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Retry)
        );
        // Override to Abort
        manager.set_category_strategy(ErrorCategory::Network, RecoveryStrategy::Abort);
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Abort)
        );
    }

    // --- reset_strategies ---
    #[test]
    fn reset_strategies_restores_all() {
        let mut manager = ErrorRecoveryManager::new();
        // Change several
        manager.set_category_strategy(ErrorCategory::Network, RecoveryStrategy::Abort);
        manager.set_category_strategy(ErrorCategory::Disk, RecoveryStrategy::Skip);
        manager.set_category_strategy(ErrorCategory::Server, RecoveryStrategy::Pause);

        manager.reset_strategies();

        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Retry)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Disk),
            Some(RecoveryStrategy::Pause)
        );
        assert_eq!(
            manager.get_category_strategy(ErrorCategory::Server),
            Some(RecoveryStrategy::Retry)
        );
    }

    // --- Persistence ---
    #[test]
    fn persistence_save_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ErrorRecoveryConfig::default();
        save_error_recovery_config(&config, temp_dir.path()).unwrap();
        assert!(temp_dir.path().join(CONFIG_FILE).exists());
    }

    #[test]
    fn persistence_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config1 = ErrorRecoveryConfig::default();
        save_error_recovery_config(&config1, temp_dir.path()).unwrap();

        let mut config2 = config1.clone();
        config2.max_consecutive_failures = 99;
        save_error_recovery_config(&config2, temp_dir.path()).unwrap();

        let loaded = load_error_recovery_config(temp_dir.path())
            .unwrap()
            .unwrap();
        assert_eq!(loaded.max_consecutive_failures, 99);
    }

    #[test]
    fn persistence_no_tmp_residue() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ErrorRecoveryConfig::default();
        save_error_recovery_config(&config, temp_dir.path()).unwrap();
        let tmp_path = temp_dir.path().join("error_recovery_config.json.tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn persistence_corrupt_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join(CONFIG_FILE);
        fs::write(&path, "not valid json{{{").unwrap();
        let result = load_error_recovery_config(temp_dir.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ErrorRecoveryPersistenceError::Deserialize(_) => {}
            _ => panic!("Expected Deserialize error"),
        }
    }

    #[test]
    fn persistence_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join(CONFIG_FILE);
        fs::write(&path, "").unwrap();
        let result = load_error_recovery_config(temp_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn persistence_complete_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut config = ErrorRecoveryConfig::default();
        config.enabled = false;
        config.max_consecutive_failures = 42;
        config.auto_switch_mirror = false;
        config.category_strategies.clear();
        config
            .category_strategies
            .insert(ErrorCategory::Network, RecoveryStrategy::Abort);

        save_error_recovery_config(&config, temp_dir.path()).unwrap();
        let loaded = load_error_recovery_config(temp_dir.path())
            .unwrap()
            .unwrap();

        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(
            loaded.max_consecutive_failures,
            config.max_consecutive_failures
        );
        assert_eq!(loaded.auto_switch_mirror, config.auto_switch_mirror);
        assert_eq!(loaded.category_strategies.len(), 1);
        assert_eq!(
            loaded.category_strategies[&ErrorCategory::Network],
            RecoveryStrategy::Abort
        );
    }

    #[test]
    fn persistence_pretty_json_format() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ErrorRecoveryConfig::default();
        save_error_recovery_config(&config, temp_dir.path()).unwrap();
        let content = fs::read_to_string(temp_dir.path().join(CONFIG_FILE)).unwrap();
        // Pretty JSON should have newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
    }

    // --- ErrorRecoveryPersistenceError ---
    #[test]
    fn persistence_error_display_all_variants() {
        let io_err = ErrorRecoveryPersistenceError::Io("disk failure".to_string());
        assert_eq!(format!("{}", io_err), "IO error: disk failure");

        let ser_err = ErrorRecoveryPersistenceError::Serialize("bad json".to_string());
        assert_eq!(format!("{}", ser_err), "Serialize error: bad json");

        let de_err = ErrorRecoveryPersistenceError::Deserialize("corrupt data".to_string());
        assert_eq!(format!("{}", de_err), "Deserialize error: corrupt data");
    }

    #[test]
    fn persistence_error_debug() {
        let err = ErrorRecoveryPersistenceError::Io("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Io"));
    }

    // --- CONFIG_FILE constant ---
    #[test]
    fn config_file_constant() {
        assert_eq!(CONFIG_FILE, "error_recovery_config.json");
    }

    // --- Unicode in error messages ---
    #[test]
    fn unicode_error_message_classification() {
        // Chinese characters mixed with English keywords
        assert_eq!(
            ErrorCategory::from_error_message("下载失败: connection timeout"),
            ErrorCategory::Network
        );
        assert_eq!(
            ErrorCategory::from_error_message("文件错误: permission denied"),
            ErrorCategory::Disk
        );
    }

    // --- Long error messages ---
    #[test]
    fn long_error_message() {
        let long_msg = format!("{} connection timeout", "x".repeat(10000));
        assert_eq!(
            ErrorCategory::from_error_message(&long_msg),
            ErrorCategory::Network
        );
    }

    // --- Error message with special chars ---
    #[test]
    fn error_message_with_special_chars() {
        assert_eq!(
            ErrorCategory::from_error_message("[ERROR] (404) Not Found - path=/file.txt"),
            ErrorCategory::NotFound
        );
    }

    // --- RecoveryDecision overridden with explanation ---
    #[test]
    fn overridden_decision_always_abort() {
        let manager = ErrorRecoveryManager::new();
        // Even for Protocol (normally Abort), override should produce Abort
        let decision = manager.classify_and_decide("invalid torrent", 10);
        assert_eq!(decision.strategy, RecoveryStrategy::Abort);
        assert!(decision.overridden);
    }

    #[test]
    fn overridden_decision_explanation_format() {
        let mut manager = ErrorRecoveryManager::new();
        manager.config.max_consecutive_failures = 3;
        let decision = manager.classify_and_decide("connection timeout", 3);
        assert!(decision.explanation.contains("🌐"));
        assert!(decision.explanation.contains("3"));
        assert!(decision.explanation.contains("aborting"));
    }

    // --- Multiple set_config calls ---
    #[test]
    fn multiple_set_config_calls() {
        let mut manager = ErrorRecoveryManager::new();

        let config1 = ErrorRecoveryConfig {
            enabled: false,
            category_strategies: HashMap::new(),
            max_consecutive_failures: 1,
            auto_switch_mirror: false,
        };
        manager.set_config(config1);
        assert!(!manager.config().enabled);
        assert_eq!(manager.config().max_consecutive_failures, 1);

        let config2 = ErrorRecoveryConfig {
            enabled: true,
            category_strategies: HashMap::new(),
            max_consecutive_failures: 99,
            auto_switch_mirror: true,
        };
        manager.set_config(config2);
        assert!(manager.config().enabled);
        assert_eq!(manager.config().max_consecutive_failures, 99);
    }

    // --- Full lifecycle ---
    #[test]
    fn full_lifecycle() {
        let temp_dir = tempfile::tempdir().unwrap();

        // 1. Start with default
        let mut manager = ErrorRecoveryManager::new();
        assert!(manager.config().enabled);

        // 2. Classify some errors
        let d1 = manager.classify_and_decide("connection timeout", 0);
        assert_eq!(d1.category, ErrorCategory::Network);
        assert_eq!(d1.strategy, RecoveryStrategy::Retry);

        // 3. Customize strategies
        manager.set_category_strategy(ErrorCategory::Network, RecoveryStrategy::Skip);
        let d2 = manager.classify_and_decide("connection timeout", 0);
        assert_eq!(d2.strategy, RecoveryStrategy::Skip);

        // 4. Save config
        save_error_recovery_config(manager.config(), temp_dir.path()).unwrap();

        // 5. Load config
        let loaded = load_error_recovery_config(temp_dir.path())
            .unwrap()
            .unwrap();
        let manager2 = ErrorRecoveryManager::from_config(loaded);
        assert_eq!(
            manager2.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Skip)
        );

        // 6. Reset strategies
        let mut manager3 = manager2.clone();
        manager3.reset_strategies();
        assert_eq!(
            manager3.get_category_strategy(ErrorCategory::Network),
            Some(RecoveryStrategy::Retry)
        );
    }
}
