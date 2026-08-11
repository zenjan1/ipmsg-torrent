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

    #[test]
    fn test_domain_retry_state_new() {
        let state = DomainRetryState::new("example.com".to_string());
        assert_eq!(state.retry_count, 0);
        assert_eq!(state.consecutive_failures, 0);
        assert!(!state.budget_exhausted);
    }

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
    fn test_manager_ignored_domains() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 1;
        manager.config.ignored_domains = vec!["trusted.com".to_string()];

        manager.record_retry("trusted.com");
        manager.record_retry("trusted.com");
        manager.record_retry("trusted.com");

        // Should still be able to retry ignored domain
        assert!(manager.can_retry_domain("trusted.com"));
    }

    #[test]
    fn test_manager_disabled() {
        let mut manager = RetryBudgetManager::new();
        manager.config.enabled = false;
        manager.config.max_retries_per_domain = 1;

        manager.record_retry("example.com");
        manager.record_retry("example.com");

        // Should always allow retry when disabled
        assert!(manager.can_retry_domain("example.com"));
    }

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
    fn test_manager_clear() {
        let mut manager = RetryBudgetManager::new();
        manager.record_retry("example.com");
        manager.record_retry("other.com");

        assert_eq!(manager.domain_states.len(), 2);

        manager.clear();
        assert_eq!(manager.domain_states.len(), 0);
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
    fn test_get_remaining_budget() {
        let mut manager = RetryBudgetManager::new();
        manager.config.max_retries_per_domain = 5;

        assert_eq!(manager.get_remaining_budget("example.com"), 5);

        manager.record_retry("example.com");
        manager.record_retry("example.com");

        assert_eq!(manager.get_remaining_budget("example.com"), 3);
    }

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
    fn test_config_serialization() {
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
}
