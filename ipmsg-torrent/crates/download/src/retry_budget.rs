use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Configuration for retry budget tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryBudgetConfig {
    /// Enable retry budget tracking
    pub enabled: bool,
    /// Maximum retry attempts per domain before blocking
    pub max_retries_per_domain: u32,
    /// Cooldown period after exhausting budget (seconds)
    pub cooldown_secs: u64,
    /// Time window for retry counting (seconds)
    pub window_secs: u64,
    /// Domains to ignore (never block)
    pub ignored_domains: Vec<String>,
}

impl Default for RetryBudgetConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries_per_domain: 10,
            cooldown_secs: 300, // 5 minutes
            window_secs: 3600,  // 1 hour
            ignored_domains: vec![],
        }
    }
}

/// Retry state for a specific domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainRetryState {
    pub domain: String,
    pub retry_count: u32,
    pub last_retry_at: Option<SystemTime>,
    pub consecutive_failures: u32,
    pub budget_exhausted: bool,
    pub exhausted_at: Option<SystemTime>,
    pub first_failure_at: Option<SystemTime>,
}

impl DomainRetryState {
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            retry_count: 0,
            last_retry_at: None,
            consecutive_failures: 0,
            budget_exhausted: false,
            exhausted_at: None,
            first_failure_at: None,
        }
    }

    /// Check if the domain is currently in cooldown
    pub fn is_in_cooldown(&self, cooldown_secs: u64) -> bool {
        if let Some(exhausted_at) = self.exhausted_at {
            if let Ok(elapsed) = exhausted_at.elapsed() {
                return elapsed < Duration::from_secs(cooldown_secs);
            }
        }
        false
    }

    /// Check if the budget is exhausted
    pub fn is_exhausted(&self, max_retries: u32, window_secs: u64) -> bool {
        // Check if we're still within the time window
        if let Some(first_failure) = self.first_failure_at {
            if let Ok(elapsed) = first_failure.elapsed() {
                if elapsed > Duration::from_secs(window_secs) {
                    // Window expired, reset
                    return false;
                }
            }
        }
        self.retry_count >= max_retries
    }

    /// Record a retry attempt
    pub fn record_retry(&mut self, max_retries: u32, window_secs: u64) {
        let now = SystemTime::now();

        // Reset if window expired
        if let Some(first_failure) = self.first_failure_at {
            if let Ok(elapsed) = first_failure.elapsed() {
                if elapsed > Duration::from_secs(window_secs) {
                    self.retry_count = 0;
                    self.first_failure_at = Some(now);
                    self.budget_exhausted = false;
                    self.exhausted_at = None;
                }
            }
        } else {
            self.first_failure_at = Some(now);
        }

        self.retry_count += 1;
        self.last_retry_at = Some(now);
        self.consecutive_failures += 1;

        // Check if budget exhausted
        if self.retry_count >= max_retries {
            self.budget_exhausted = true;
            self.exhausted_at = Some(now);
        }
    }

    /// Record a successful download (reset state)
    pub fn record_success(&mut self) {
        self.retry_count = 0;
        self.consecutive_failures = 0;
        self.budget_exhausted = false;
        self.exhausted_at = None;
        self.first_failure_at = None;
    }

    /// Get remaining retry budget
    pub fn remaining_budget(&self, max_retries: u32) -> u32 {
        max_retries.saturating_sub(self.retry_count)
    }
}

/// Summary of retry budget status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryBudgetSummary {
    pub total_tracked_domains: usize,
    pub domains_with_budget_remaining: usize,
    pub domains_with_exhausted_budget: usize,
    pub domains_in_cooldown: usize,
    pub total_retries_in_window: u32,
    pub blocked_domains: Vec<String>,
}

/// Manager for retry budget tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryBudgetManager {
    pub config: RetryBudgetConfig,
    pub domain_states: HashMap<String, DomainRetryState>,
}

impl Default for RetryBudgetManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryBudgetManager {
    pub fn new() -> Self {
        Self {
            config: RetryBudgetConfig::default(),
            domain_states: HashMap::new(),
        }
    }

    pub fn with_config(config: RetryBudgetConfig) -> Self {
        Self {
            config,
            domain_states: HashMap::new(),
        }
    }

    /// Check if a domain can be retried
    pub fn can_retry_domain(&self, domain: &str) -> bool {
        if !self.config.enabled {
            return true;
        }

        // Check if domain is in ignore list
        if self.config.ignored_domains.contains(&domain.to_string()) {
            return true;
        }

        let state = match self.domain_states.get(domain) {
            Some(state) => state,
            None => return true, // No state, can retry
        };

        // Check if in cooldown
        if state.is_in_cooldown(self.config.cooldown_secs) {
            return false;
        }

        // Check if budget exhausted
        if state.is_exhausted(self.config.max_retries_per_domain, self.config.window_secs) {
            return false;
        }

        true
    }

    /// Record a retry attempt for a domain
    pub fn record_retry(&mut self, domain: &str) {
        if !self.config.enabled {
            return;
        }

        if self.config.ignored_domains.contains(&domain.to_string()) {
            return;
        }

        let state = self
            .domain_states
            .entry(domain.to_string())
            .or_insert_with(|| DomainRetryState::new(domain.to_string()));

        state.record_retry(self.config.max_retries_per_domain, self.config.window_secs);
    }

    /// Record a successful download for a domain
    pub fn record_success(&mut self, domain: &str) {
        if let Some(state) = self.domain_states.get_mut(domain) {
            state.record_success();
        }
    }

    /// Get the retry state for a domain
    pub fn get_domain_state(&self, domain: &str) -> Option<&DomainRetryState> {
        self.domain_states.get(domain)
    }

    /// Get remaining budget for a domain
    pub fn get_remaining_budget(&self, domain: &str) -> u32 {
        self.domain_states
            .get(domain)
            .map(|s| s.remaining_budget(self.config.max_retries_per_domain))
            .unwrap_or(self.config.max_retries_per_domain)
    }

    /// Get summary of all retry budgets
    pub fn get_summary(&self) -> RetryBudgetSummary {
        let mut blocked_domains = Vec::new();
        let mut domains_with_budget_remaining = 0;
        let mut domains_with_exhausted_budget = 0;
        let mut domains_in_cooldown = 0;
        let mut total_retries = 0;

        for (domain, state) in &self.domain_states {
            total_retries += state.retry_count;

            if state.is_in_cooldown(self.config.cooldown_secs) {
                domains_in_cooldown += 1;
                blocked_domains.push(domain.clone());
            } else if state
                .is_exhausted(self.config.max_retries_per_domain, self.config.window_secs)
            {
                domains_with_exhausted_budget += 1;
                blocked_domains.push(domain.clone());
            } else {
                domains_with_budget_remaining += 1;
            }
        }

        RetryBudgetSummary {
            total_tracked_domains: self.domain_states.len(),
            domains_with_budget_remaining,
            domains_with_exhausted_budget,
            domains_in_cooldown,
            total_retries_in_window: total_retries,
            blocked_domains,
        }
    }

    /// Clear all retry state
    pub fn clear(&mut self) {
        self.domain_states.clear();
    }

    /// Clear retry state for a specific domain
    pub fn clear_domain(&mut self, domain: &str) {
        self.domain_states.remove(domain);
    }

    /// Reset expired entries based on window
    pub fn reset_expired(&mut self) {
        let window = Duration::from_secs(self.config.window_secs);
        let cooldown = Duration::from_secs(self.config.cooldown_secs);

        self.domain_states.retain(|_, state| {
            // Keep if still within window or cooldown
            let in_window = state
                .first_failure_at
                .and_then(|t| t.elapsed().ok())
                .map(|e| e < window)
                .unwrap_or(false);

            let in_cooldown = state
                .exhausted_at
                .and_then(|t| t.elapsed().ok())
                .map(|e| e < cooldown)
                .unwrap_or(false);

            in_window || in_cooldown
        });
    }

    /// Get configuration
    pub fn config(&self) -> &RetryBudgetConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: RetryBudgetConfig) {
        self.config = config;
    }
}

/// Save retry budget config to file
pub async fn save_retry_budget_config(
    path: &std::path::Path,
    config: &RetryBudgetConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(config)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Load retry budget config from file
pub async fn load_retry_budget_config(
    path: &std::path::Path,
) -> Result<RetryBudgetConfig, Box<dyn std::error::Error>> {
    let json = tokio::fs::read_to_string(path).await?;
    let config: RetryBudgetConfig = serde_json::from_str(&json)?;
    Ok(config)
}

/// Save retry budget manager state to file
pub async fn save_retry_budget_state(
    path: &std::path::Path,
    manager: &RetryBudgetManager,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(manager)?;
    tokio::fs::write(path, json).await?;
    Ok(())
}

/// Load retry budget manager state from file
pub async fn load_retry_budget_state(
    path: &std::path::Path,
) -> Result<RetryBudgetManager, Box<dyn std::error::Error>> {
    let json = tokio::fs::read_to_string(path).await?;
    let manager: RetryBudgetManager = serde_json::from_str(&json)?;
    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;

    // === DomainRetryState::new() ===

    #[test]
    fn test_domain_retry_state_new() {
        let state = DomainRetryState::new("example.com".to_string());
        assert_eq!(state.retry_count, 0);
        assert_eq!(state.consecutive_failures, 0);
        assert!(!state.budget_exhausted);
    }

    #[test]
    fn test_domain_retry_state_new_fields() {
        let state = DomainRetryState::new("test.com".to_string());
        assert_eq!(state.domain, "test.com");
        assert!(state.last_retry_at.is_none());
        assert!(state.exhausted_at.is_none());
        assert!(state.first_failure_at.is_none());
    }

    #[test]
    fn test_domain_retry_state_new_unicode() {
        let state = DomainRetryState::new("中文.com".to_string());
        assert_eq!(state.domain, "中文.com");
    }

    #[test]
    fn test_domain_retry_state_new_emoji() {
        let state = DomainRetryState::new("🌟.com".to_string());
        assert_eq!(state.domain, "🌟.com");
    }

    // === DomainRetryState::record_retry() ===

    #[test]
    fn test_record_retry() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.record_retry(3, 3600);
        assert_eq!(state.retry_count, 1);
        assert_eq!(state.consecutive_failures, 1);
        assert!(!state.budget_exhausted);

        state.record_retry(3, 3600);
        assert_eq!(state.retry_count, 2);

        state.record_retry(3, 3600);
        assert_eq!(state.retry_count, 3);
        assert!(state.budget_exhausted);
    }

    #[test]
    fn test_record_retry_sets_timestamps() {
        let mut state = DomainRetryState::new("example.com".to_string());
        assert!(state.last_retry_at.is_none());
        assert!(state.first_failure_at.is_none());

        state.record_retry(5, 3600);
        assert!(state.last_retry_at.is_some());
        assert!(state.first_failure_at.is_some());
    }

    #[test]
    fn test_record_retry_first_failure_preserved() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.record_retry(5, 3600);
        let first = state.first_failure_at;

        state.record_retry(5, 3600);
        state.record_retry(5, 3600);
        // first_failure_at should not change after subsequent retries
        assert_eq!(state.first_failure_at, first);
    }

    #[test]
    fn test_record_retry_consecutive_failures_increment() {
        let mut state = DomainRetryState::new("example.com".to_string());
        for i in 1..=5 {
            state.record_retry(10, 3600);
            assert_eq!(state.consecutive_failures, i);
        }
    }

    #[test]
    fn test_record_retry_max_retries_zero() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.record_retry(0, 3600);
        assert!(state.budget_exhausted);
    }

    #[test]
    fn test_record_retry_max_retries_one() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.record_retry(1, 3600);
        assert_eq!(state.retry_count, 1);
        assert!(state.budget_exhausted);
    }

    // === DomainRetryState::record_success() ===

    #[test]
    fn test_record_success_resets_state() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.record_retry(3, 3600);
        state.record_retry(3, 3600);
        assert_eq!(state.retry_count, 2);

        state.record_success();
        assert_eq!(state.retry_count, 0);
        assert_eq!(state.consecutive_failures, 0);
        assert!(!state.budget_exhausted);
    }

    #[test]
    fn test_record_success_clears_timestamps() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.record_retry(3, 3600);
        assert!(state.first_failure_at.is_some());
        assert!(state.exhausted_at.is_some() || state.last_retry_at.is_some());

        state.record_success();
        assert!(state.first_failure_at.is_none());
        assert!(state.exhausted_at.is_none());
    }

    #[test]
    fn test_record_success_on_fresh_state() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.record_success();
        assert_eq!(state.retry_count, 0);
        assert!(!state.budget_exhausted);
    }

    // === DomainRetryState::remaining_budget() ===

    #[test]
    fn test_remaining_budget() {
        let mut state = DomainRetryState::new("example.com".to_string());
        assert_eq!(state.remaining_budget(5), 5);

        state.record_retry(5, 3600);
        assert_eq!(state.remaining_budget(5), 4);

        state.record_retry(5, 3600);
        state.record_retry(5, 3600);
        assert_eq!(state.remaining_budget(5), 2);
    }

    #[test]
    fn test_remaining_budget_zero_max() {
        let state = DomainRetryState::new("example.com".to_string());
        assert_eq!(state.remaining_budget(0), 0);
    }

    #[test]
    fn test_remaining_budget_saturating() {
        let mut state = DomainRetryState::new("example.com".to_string());
        for _ in 0..10 {
            state.record_retry(3, 3600);
        }
        // Should not underflow
        assert_eq!(state.remaining_budget(3), 0);
    }

    #[test]
    fn test_remaining_budget_u32_max() {
        let state = DomainRetryState::new("example.com".to_string());
        assert_eq!(state.remaining_budget(u32::MAX), u32::MAX);
    }

    // === DomainRetryState::is_in_cooldown() ===

    #[test]
    fn test_is_in_cooldown_no_exhausted_at() {
        let state = DomainRetryState::new("example.com".to_string());
        assert!(!state.is_in_cooldown(300));
    }

    #[test]
    fn test_is_in_cooldown_zero_cooldown() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.budget_exhausted = true;
        state.exhausted_at = Some(SystemTime::now());
        // With 0 cooldown, should not be in cooldown (elapsed >= 0)
        assert!(!state.is_in_cooldown(0));
    }

    // === DomainRetryState::is_exhausted() ===

    #[test]
    fn test_is_exhausted_no_first_failure() {
        let state = DomainRetryState::new("example.com".to_string());
        assert!(!state.is_exhausted(5, 3600));
    }

    #[test]
    fn test_is_exhausted_zero_max_retries() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.first_failure_at = Some(SystemTime::now());
        state.retry_count = 0;
        assert!(state.is_exhausted(0, 3600));
    }

    #[test]
    fn test_is_exhausted_at_boundary() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.first_failure_at = Some(SystemTime::now());
        state.retry_count = 5;
        assert!(state.is_exhausted(5, 3600));
    }

    #[test]
    fn test_is_exhausted_below_boundary() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.first_failure_at = Some(SystemTime::now());
        state.retry_count = 4;
        assert!(!state.is_exhausted(5, 3600));
    }

    // === RetryBudgetConfig ===

    #[test]
    fn test_config_default() {
        let config = RetryBudgetConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_retries_per_domain, 10);
        assert_eq!(config.cooldown_secs, 300);
        assert_eq!(config.window_secs, 3600);
        assert!(config.ignored_domains.is_empty());
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = RetryBudgetConfig {
            enabled: true,
            max_retries_per_domain: 10,
            cooldown_secs: 300,
            window_secs: 3600,
            ignored_domains: vec!["example.com".to_string()],
        };

        let json = serde_json::to_string(&config).unwrap();
        let loaded: RetryBudgetConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.max_retries_per_domain, config.max_retries_per_domain);
        assert_eq!(loaded.ignored_domains, config.ignored_domains);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = RetryBudgetConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let loaded: RetryBudgetConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{
            "enabled": true,
            "max_retries_per_domain": 5,
            "cooldown_secs": 60,
            "window_secs": 1800,
            "ignored_domains": [],
            "unknown_field": "value"
        }"#;
        let config: RetryBudgetConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_retries_per_domain, 5);
    }

    #[test]
    fn test_config_clone_debug() {
        let config = RetryBudgetConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("RetryBudgetConfig"));
    }

    #[test]
    fn test_config_custom_values() {
        let config = RetryBudgetConfig {
            enabled: false,
            max_retries_per_domain: 0,
            cooldown_secs: 0,
            window_secs: 0,
            ignored_domains: vec!["a.com".into(), "b.com".into()],
        };
        assert!(!config.enabled);
        assert_eq!(config.max_retries_per_domain, 0);
        assert_eq!(config.ignored_domains.len(), 2);
    }

    // === RetryBudgetSummary ===

    #[test]
    fn test_summary_serde_roundtrip() {
        let summary = RetryBudgetSummary {
            total_tracked_domains: 5,
            domains_with_budget_remaining: 2,
            domains_with_exhausted_budget: 2,
            domains_in_cooldown: 1,
            total_retries_in_window: 42,
            blocked_domains: vec!["bad.com".into()],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let loaded: RetryBudgetSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_tracked_domains, 5);
        assert_eq!(loaded.total_retries_in_window, 42);
        assert_eq!(loaded.blocked_domains.len(), 1);
    }

    #[test]
    fn test_summary_clone_debug() {
        let summary = RetryBudgetSummary {
            total_tracked_domains: 0,
            domains_with_budget_remaining: 0,
            domains_with_exhausted_budget: 0,
            domains_in_cooldown: 0,
            total_retries_in_window: 0,
            blocked_domains: vec![],
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_tracked_domains, 0);
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("RetryBudgetSummary"));
    }

    // === DomainRetryState serde ===

    #[test]
    fn test_domain_state_serde_roundtrip() {
        let mut state = DomainRetryState::new("example.com".to_string());
        state.record_retry(5, 3600);
        let json = serde_json::to_string(&state).unwrap();
        let loaded: DomainRetryState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.retry_count, 1);
        assert_eq!(loaded.domain, "example.com");
    }

    #[test]
    fn test_domain_state_clone_debug() {
        let state = DomainRetryState::new("test.com".to_string());
        let cloned = state.clone();
        assert_eq!(cloned.domain, state.domain);
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("DomainRetryState"));
    }

    // === RetryBudgetManager::new() / default / with_config ===

    #[test]
    fn test_manager_new() {
        let manager = RetryBudgetManager::new();
        assert!(manager.config.enabled);
        assert!(manager.domain_states.is_empty());
    }

    #[test]
    fn test_manager_default_equals_new() {
        let new = RetryBudgetManager::new();
        let default = RetryBudgetManager::default();
        assert_eq!(new.config.enabled, default.config.enabled);
        assert_eq!(
            new.config.max_retries_per_domain,
            default.config.max_retries_per_domain
        );
    }

    #[test]
    fn test_manager_with_config() {
        let config = RetryBudgetConfig {
            enabled: false,
            max_retries_per_domain: 3,
            cooldown_secs: 60,
            window_secs: 600,
            ignored_domains: vec!["safe.com".into()],
        };
        let manager = RetryBudgetManager::with_config(config);
        assert!(!manager.config.enabled);
        assert_eq!(manager.config.max_retries_per_domain, 3);
        assert!(manager.domain_states.is_empty());
    }

    #[test]
    fn test_manager_clone_debug() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("example.com");
        let cloned = manager.clone();
        assert_eq!(cloned.domain_states.len(), 1);
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("RetryBudgetManager"));
    }

    // === RetryBudgetManager::can_retry_domain() ===

    #[test]
    fn test_manager_can_retry() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 2;

        assert!(manager.can_retry_domain("example.com"));

        manager.record_retry("example.com");
        assert!(manager.can_retry_domain("example.com"));

        manager.record_retry("example.com");
        assert!(!manager.can_retry_domain("example.com"));
    }

    #[test]
    fn test_manager_can_retry_unknown_domain() {
        let manager = RetryBudgetManager::new();
        assert!(manager.can_retry_domain("unknown.com"));
    }

    #[test]
    fn test_manager_can_retry_empty_string() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 1;
        manager.record_retry("");
        assert!(!manager.can_retry_domain(""));
    }

    #[test]
    fn test_manager_ignored_domains() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 1;
        manager.config.ignored_domains = vec!["trusted.com".to_string()];

        manager.record_retry("trusted.com");
        manager.record_retry("trusted.com");
        manager.record_retry("trusted.com");

        assert!(manager.can_retry_domain("trusted.com"));
    }

    #[test]
    fn test_manager_ignored_domains_case_sensitive() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 1;
        manager.config.ignored_domains = vec!["Trusted.com".to_string()];

        manager.record_retry("trusted.com");
        manager.record_retry("trusted.com");

        // Case sensitive: "trusted.com" != "Trusted.com"
        assert!(!manager.can_retry_domain("trusted.com"));
        assert!(manager.can_retry_domain("Trusted.com"));
    }

    #[test]
    fn test_manager_disabled() {
        let mut manager = RetryBudgetManager::new();
        manager.config.enabled = false;
        manager.config.max_retries_per_domain = 1;

        manager.record_retry("example.com");
        manager.record_retry("example.com");

        assert!(manager.can_retry_domain("example.com"));
    }

    #[test]
    fn test_manager_disabled_no_state_created() {
        let mut manager = RetryBudgetManager::new();
        manager.config.enabled = false;
        manager.record_retry("example.com");
        assert!(manager.domain_states.is_empty());
    }

    // === RetryBudgetManager::record_retry() ===

    #[test]
    fn test_manager_record_retry_creates_state() {
        let mut manager = RetryBudgetManager::new();
        assert!(manager.get_domain_state("example.com").is_none());

        manager.record_retry("example.com");
        assert!(manager.get_domain_state("example.com").is_some());
    }

    #[test]
    fn test_manager_record_retry_multiple_domains() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("a.com");
        manager.record_retry("b.com");
        manager.record_retry("c.com");
        assert_eq!(manager.domain_states.len(), 3);
    }

    #[test]
    fn test_manager_record_retry_ignored_not_tracked() {
        let mut manager = RetryBudgetManager::new();
        manager.config.ignored_domains = vec!["skip.com".to_string()];
        manager.record_retry("skip.com");
        assert!(manager.domain_states.is_empty());
    }

    // === RetryBudgetManager::record_success() ===

    #[test]
    fn test_record_success() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 2;

        manager.record_retry("example.com");
        manager.record_retry("example.com");
        assert!(!manager.can_retry_domain("example.com"));

        manager.record_success("example.com");
        assert!(manager.can_retry_domain("example.com"));
    }

    #[test]
    fn test_record_success_nonexistent_domain() {
        let mut manager = RetryBudgetManager::new();
        // Should not panic
        manager.record_success("nonexistent.com");
    }

    // === RetryBudgetManager::get_domain_state() ===

    #[test]
    fn test_get_domain_state_exists() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("example.com");
        let state = manager.get_domain_state("example.com").unwrap();
        assert_eq!(state.retry_count, 1);
    }

    #[test]
    fn test_get_domain_state_not_exists() {
        let manager = RetryBudgetManager::new();
        assert!(manager.get_domain_state("nope.com").is_none());
    }

    // === RetryBudgetManager::get_remaining_budget() ===

    #[test]
    fn test_get_remaining_budget() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 5;

        assert_eq!(manager.get_remaining_budget("example.com"), 5);

        manager.record_retry("example.com");
        manager.record_retry("example.com");

        assert_eq!(manager.get_remaining_budget("example.com"), 3);
    }

    #[test]
    fn test_get_remaining_budget_unknown_domain() {
        let manager = RetryBudgetManager::new();
        assert_eq!(manager.get_remaining_budget("unknown.com"), 10); // default max
    }

    // === RetryBudgetManager::get_summary() ===

    #[test]
    fn test_manager_summary() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 2;

        manager.record_retry("example.com");
        manager.record_retry("example.com");
        manager.record_retry("other.com");

        let summary = manager.get_summary();
        assert_eq!(summary.total_tracked_domains, 2);
        assert_eq!(summary.domains_with_exhausted_budget, 1);
        assert_eq!(summary.domains_with_budget_remaining, 1);
        assert_eq!(summary.total_retries_in_window, 3);
    }

    #[test]
    fn test_manager_summary_empty() {
        let manager = RetryBudgetManager::new();
        let summary = manager.get_summary();
        assert_eq!(summary.total_tracked_domains, 0);
        assert_eq!(summary.domains_with_budget_remaining, 0);
        assert_eq!(summary.domains_with_exhausted_budget, 0);
        assert_eq!(summary.domains_in_cooldown, 0);
        assert_eq!(summary.total_retries_in_window, 0);
        assert!(summary.blocked_domains.is_empty());
    }

    #[test]
    fn test_manager_summary_all_exhausted() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 1;

        manager.record_retry("a.com");
        manager.record_retry("b.com");

        let summary = manager.get_summary();
        assert_eq!(summary.domains_with_exhausted_budget, 2);
        assert_eq!(summary.domains_with_budget_remaining, 0);
        assert_eq!(summary.blocked_domains.len(), 2);
    }

    #[test]
    fn test_manager_summary_total_retries() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 100;

        for _ in 0..5 {
            manager.record_retry("a.com");
        }
        for _ in 0..3 {
            manager.record_retry("b.com");
        }

        let summary = manager.get_summary();
        assert_eq!(summary.total_retries_in_window, 8);
    }

    // === RetryBudgetManager::clear() / clear_domain() ===

    #[test]
    fn test_manager_clear() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("example.com");
        manager.record_retry("other.com");

        assert_eq!(manager.domain_states.len(), 2);

        manager.clear();
        assert_eq!(manager.domain_states.len(), 0);
    }

    #[test]
    fn test_manager_clear_empty() {
        let mut manager = RetryBudgetManager::new();
        manager.clear(); // Should not panic
        assert!(manager.domain_states.is_empty());
    }

    #[test]
    fn test_manager_clear_domain() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("example.com");
        manager.record_retry("other.com");

        manager.clear_domain("example.com");
        assert_eq!(manager.domain_states.len(), 1);
        assert!(manager.domain_states.contains_key("other.com"));
    }

    #[test]
    fn test_manager_clear_domain_nonexistent() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("example.com");
        manager.clear_domain("nonexistent.com");
        assert_eq!(manager.domain_states.len(), 1);
    }

    // === RetryBudgetManager::config() / set_config() ===

    #[test]
    fn test_manager_config_accessor() {
        let manager = RetryBudgetManager::new();
        assert!(manager.config().enabled);
    }

    #[test]
    fn test_manager_set_config() {
        let mut manager = RetryBudgetManager::new();
        let new_config = RetryBudgetConfig {
            enabled: false,
            max_retries_per_domain: 20,
            cooldown_secs: 600,
            window_secs: 7200,
            ignored_domains: vec!["x.com".into()],
        };
        manager.set_config(new_config);
        assert!(!manager.config.enabled);
        assert_eq!(manager.config.max_retries_per_domain, 20);
    }

    // === RetryBudgetManager::reset_expired() ===

    #[test]
    fn test_reset_expired_empty() {
        let mut manager = RetryBudgetManager::new();
        manager.reset_expired();
        assert!(manager.domain_states.is_empty());
    }

    // === RetryBudgetManager serde ===

    #[test]
    fn test_manager_serialization() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 5;
        manager.record_retry("example.com");
        manager.record_retry("other.com");

        let json = serde_json::to_string(&manager).unwrap();
        let loaded: RetryBudgetManager = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.config.max_retries_per_domain, 5);
        assert_eq!(loaded.domain_states.len(), 2);
        assert_eq!(loaded.domain_states["example.com"].retry_count, 1);
    }

    #[test]
    fn test_manager_serde_pretty() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("test.com");
        let pretty = serde_json::to_string_pretty(&manager).unwrap();
        let loaded: RetryBudgetManager = serde_json::from_str(&pretty).unwrap();
        assert_eq!(loaded.domain_states.len(), 1);
    }

    #[test]
    fn test_manager_serde_extra_fields() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("a.com");
        let json = serde_json::to_string(&manager).unwrap();
        // Add extra field
        let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
        value["extra"] = serde_json::json!("ignored");
        let loaded: RetryBudgetManager =
            serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
        assert_eq!(loaded.domain_states.len(), 1);
    }

    // === Persistence functions ===

    #[tokio::test]
    async fn test_save_load_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");

        let config = RetryBudgetConfig {
            enabled: true,
            max_retries_per_domain: 7,
            cooldown_secs: 120,
            window_secs: 1800,
            ignored_domains: vec!["safe.com".into()],
        };

        save_retry_budget_config(&path, &config).await.unwrap();
        let loaded = load_retry_budget_config(&path).await.unwrap();

        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.max_retries_per_domain, 7);
        assert_eq!(loaded.ignored_domains, config.ignored_domains);
    }

    #[tokio::test]
    async fn test_save_config_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new_config.json");

        assert!(!path.exists());
        save_retry_budget_config(&path, &RetryBudgetConfig::default())
            .await
            .unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_save_config_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");

        let config1 = RetryBudgetConfig {
            enabled: true,
            max_retries_per_domain: 5,
            cooldown_secs: 60,
            window_secs: 600,
            ignored_domains: vec![],
        };
        save_retry_budget_config(&path, &config1).await.unwrap();

        let config2 = RetryBudgetConfig {
            enabled: false,
            max_retries_per_domain: 20,
            cooldown_secs: 120,
            window_secs: 1200,
            ignored_domains: vec!["x.com".into()],
        };
        save_retry_budget_config(&path, &config2).await.unwrap();

        let loaded = load_retry_budget_config(&path).await.unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.max_retries_per_domain, 20);
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        assert!(load_retry_budget_config(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_load_config_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.json");
        tokio::fs::write(&path, "not json{{{").await.unwrap();
        assert!(load_retry_budget_config(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_load_config_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.json");
        tokio::fs::write(&path, "").await.unwrap();
        assert!(load_retry_budget_config(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_save_load_state_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 3;
        manager.record_retry("a.com");
        manager.record_retry("a.com");
        manager.record_retry("b.com");

        save_retry_budget_state(&path, &manager).await.unwrap();
        let loaded = load_retry_budget_state(&path).await.unwrap();

        assert_eq!(loaded.config.max_retries_per_domain, 3);
        assert_eq!(loaded.domain_states.len(), 2);
        assert_eq!(loaded.domain_states["a.com"].retry_count, 2);
        assert_eq!(loaded.domain_states["b.com"].retry_count, 1);
    }

    #[tokio::test]
    async fn test_save_state_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new_state.json");

        let manager = RetryBudgetManager::new();
        assert!(!path.exists());
        save_retry_budget_state(&path, &manager).await.unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_save_state_no_tmp_leftover() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let manager = RetryBudgetManager::new();
        save_retry_budget_state(&path, &manager).await.unwrap();

        let entries: Vec<_> = dir.path().read_dir().unwrap().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].as_ref().unwrap().file_name(), "state.json");
    }

    #[tokio::test]
    async fn test_load_state_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent_state.json");
        assert!(load_retry_budget_state(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_load_state_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt_state.json");
        tokio::fs::write(&path, "{{broken").await.unwrap();
        assert!(load_retry_budget_state(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_save_state_pretty_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pretty_state.json");

        let manager = RetryBudgetManager::new();
        save_retry_budget_state(&path, &manager).await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains('\n')); // Pretty printed
    }

    // === Complex workflows ===

    #[test]
    fn test_full_lifecycle() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 3;

        // Record failures
        manager.record_retry("example.com");
        manager.record_retry("example.com");
        assert!(manager.can_retry_domain("example.com"));
        assert_eq!(manager.get_remaining_budget("example.com"), 1);

        // Exhaust budget
        manager.record_retry("example.com");
        assert!(!manager.can_retry_domain("example.com"));
        assert_eq!(manager.get_remaining_budget("example.com"), 0);

        // Success resets
        manager.record_success("example.com");
        assert!(manager.can_retry_domain("example.com"));
        assert_eq!(manager.get_remaining_budget("example.com"), 3);
    }

    #[test]
    fn test_multi_domain_independent() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 2;

        manager.record_retry("a.com");
        manager.record_retry("a.com");
        manager.record_retry("b.com");

        assert!(!manager.can_retry_domain("a.com"));
        assert!(manager.can_retry_domain("b.com"));
        assert!(manager.can_retry_domain("c.com"));
    }

    #[test]
    fn test_success_then_exhaust_again() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 2;

        // First cycle
        manager.record_retry("x.com");
        manager.record_retry("x.com");
        assert!(!manager.can_retry_domain("x.com"));

        // Recovery
        manager.record_success("x.com");
        assert!(manager.can_retry_domain("x.com"));

        // Second cycle
        manager.record_retry("x.com");
        manager.record_retry("x.com");
        assert!(!manager.can_retry_domain("x.com"));
    }

    #[test]
    fn test_clear_and_re_add() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 1;

        manager.record_retry("a.com");
        assert!(!manager.can_retry_domain("a.com"));

        manager.clear();
        assert!(manager.can_retry_domain("a.com"));

        manager.record_retry("a.com");
        assert!(!manager.can_retry_domain("a.com"));
    }

    #[test]
    fn test_many_domains() {
        let mut manager = RetryBudgetManager::new();
        for i in 0..100 {
            manager.record_retry(&format!("domain{}.com", i));
        }
        assert_eq!(manager.domain_states.len(), 100);

        let summary = manager.get_summary();
        assert_eq!(summary.total_tracked_domains, 100);
        assert_eq!(summary.total_retries_in_window, 100);
    }

    #[test]
    fn test_unicode_domain() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 2;
        manager.record_retry("中文域名.com");
        manager.record_retry("中文域名.com");
        assert!(!manager.can_retry_domain("中文域名.com"));
        assert!(manager.can_retry_domain("other.com"));
    }

    #[test]
    fn test_emoji_domain() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("🌟.com");
        let state = manager.get_domain_state("🌟.com").unwrap();
        assert_eq!(state.retry_count, 1);
    }

    #[test]
    fn test_multiple_ignored_domains() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 1;
        manager.config.ignored_domains = vec!["a.com".into(), "b.com".into(), "c.com".into()];

        for d in &["a.com", "b.com", "c.com"] {
            manager.record_retry(d);
            manager.record_retry(d);
            manager.record_retry(d);
            assert!(manager.can_retry_domain(d));
        }

        manager.record_retry("d.com");
        manager.record_retry("d.com");
        assert!(!manager.can_retry_domain("d.com"));
    }
}
