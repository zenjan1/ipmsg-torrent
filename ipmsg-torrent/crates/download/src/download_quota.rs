//! Download Quota System
//!
//! Allows setting data quotas per tag or per group. When a quota is exceeded,
//! all active downloads with that tag/group are automatically paused.
//! Quotas reset daily and support configurable limits and tracking.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A quota scope: either a tag or a group
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum QuotaScope {
    /// Quota applies to all tasks with this tag
    Tag(String),
    /// Quota applies to all tasks in this group
    Group(String),
}

impl QuotaScope {
    /// Get the display name of this scope
    pub fn name(&self) -> &str {
        match self {
            QuotaScope::Tag(t) => t.as_str(),
            QuotaScope::Group(g) => g.as_str(),
        }
    }

    /// Get the scope type label
    pub fn type_label(&self) -> &'static str {
        match self {
            QuotaScope::Tag(_) => "tag",
            QuotaScope::Group(_) => "group",
        }
    }
}

/// Configuration for a single quota rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaRule {
    /// Unique rule ID
    pub id: String,
    /// The scope this quota applies to
    pub scope: QuotaScope,
    /// Maximum bytes allowed per day (0 = unlimited)
    pub daily_limit_bytes: u64,
    /// Whether this rule is enabled
    pub enabled: bool,
    /// When this rule was created
    pub created_at: DateTime<Utc>,
}

impl QuotaRule {
    pub fn new(id: String, scope: QuotaScope, daily_limit_bytes: u64) -> Self {
        Self {
            id,
            scope,
            daily_limit_bytes,
            enabled: true,
            created_at: Utc::now(),
        }
    }
}

/// Daily usage tracking for a single quota scope
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QuotaUsage {
    /// The date this usage is for
    pub date: NaiveDate,
    /// Total bytes downloaded under this quota today
    pub bytes_downloaded: u64,
    /// Number of tasks that contributed to this usage
    pub task_count: u32,
    /// Last time this record was updated
    pub last_updated: DateTime<Utc>,
    /// Whether this quota was exceeded today (and tasks were paused)
    pub exceeded: bool,
}

impl QuotaUsage {
    pub fn new(date: NaiveDate) -> Self {
        Self {
            date,
            bytes_downloaded: 0,
            task_count: 0,
            last_updated: Utc::now(),
            exceeded: false,
        }
    }

    /// Record additional bytes downloaded
    pub fn add_bytes(&mut self, bytes: u64) {
        self.bytes_downloaded += bytes;
        self.last_updated = Utc::now();
    }

    /// Increment the task count
    pub fn increment_task_count(&mut self) {
        self.task_count += 1;
        self.last_updated = Utc::now();
    }

    /// Check if this usage exceeds the given limit
    pub fn exceeds(&self, limit_bytes: u64) -> bool {
        limit_bytes > 0 && self.bytes_downloaded >= limit_bytes
    }

    /// Get remaining bytes before hitting the limit
    pub fn remaining(&self, limit_bytes: u64) -> u64 {
        if limit_bytes == 0 {
            return u64::MAX;
        }
        limit_bytes.saturating_sub(self.bytes_downloaded)
    }

    /// Get usage as a percentage of the limit
    pub fn usage_percent(&self, limit_bytes: u64) -> f64 {
        if limit_bytes == 0 {
            return 0.0;
        }
        (self.bytes_downloaded as f64 / limit_bytes as f64 * 100.0).min(100.0)
    }

    /// Reset usage for a new day
    pub fn reset_for_new_day(&mut self, date: NaiveDate) {
        self.date = date;
        self.bytes_downloaded = 0;
        self.task_count = 0;
        self.exceeded = false;
        self.last_updated = Utc::now();
    }
}

/// Status of a single quota rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaStatus {
    /// The rule being tracked
    pub rule: QuotaRule,
    /// Current usage for today
    pub usage: QuotaUsage,
    /// Remaining bytes today
    pub remaining_bytes: u64,
    /// Usage percentage (0-100)
    pub usage_percent: f64,
    /// Whether the quota is currently exceeded
    pub is_exceeded: bool,
    /// Number of tasks that match this scope (approximate)
    pub matching_task_count: usize,
}

/// Summary of all quota statuses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaSummary {
    /// Total number of quota rules
    pub total_rules: usize,
    /// Number of enabled rules
    pub enabled_rules: usize,
    /// Number of rules currently exceeded
    pub exceeded_rules: usize,
    /// Per-rule status details
    pub statuses: Vec<QuotaStatus>,
    /// Total bytes downloaded under all quotas today
    pub total_bytes_today: u64,
}

impl QuotaSummary {
    pub fn format_report(&self) -> String {
        let mut report = String::new();
        report.push_str(&format!(
            "📊 Download Quota Summary\n\
             Rules: {} total, {} enabled, {} exceeded\n\
             Total usage today: {}\n\n",
            self.total_rules,
            self.enabled_rules,
            self.exceeded_rules,
            format_bytes(self.total_bytes_today),
        ));

        if self.statuses.is_empty() {
            report.push_str("No quota rules configured.\n");
            return report;
        }

        for status in &self.statuses {
            let icon = if status.is_exceeded {
                "🔴"
            } else if status.usage_percent > 80.0 {
                "🟡"
            } else {
                "🟢"
            };
            let scope_type = status.rule.scope.type_label();
            let scope_name = status.rule.scope.name();
            let limit = format_bytes(status.rule.daily_limit_bytes);
            let used = format_bytes(status.usage.bytes_downloaded);
            let remaining = format_bytes(status.remaining_bytes);
            let pct = format!("{:.1}%", status.usage_percent);
            let enabled_icon = if status.rule.enabled {
                ""
            } else {
                " (disabled)"
            };

            report.push_str(&format!(
                "{icon} [{scope_type}:{scope_name}] {limit}/day{enabled_icon}\n\
                 \x20  Used: {used} / {limit} ({pct})\n\
                 \x20  Remaining: {remaining}\n\
                 \x20  Matching tasks: {}\n",
                status.matching_task_count,
            ));
            if status.is_exceeded {
                report.push_str("  ⚠️  Quota exceeded — matching tasks paused\n");
            }
            report.push('\n');
        }

        report
    }
}

/// Configuration for the quota system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaSystemConfig {
    /// Whether the quota system is globally enabled
    pub enabled: bool,
    /// Maximum number of quota rules (0 = unlimited)
    pub max_rules: usize,
}

impl Default for QuotaSystemConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_rules: 100,
        }
    }
}

/// The main quota manager
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloadQuotaManager {
    /// System-level configuration
    pub config: QuotaSystemConfig,
    /// Quota rules keyed by rule ID
    pub rules: HashMap<String, QuotaRule>,
    /// Usage tracking keyed by rule ID
    pub usage: HashMap<String, QuotaUsage>,
}

impl DownloadQuotaManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the system configuration
    pub fn set_config(&mut self, config: QuotaSystemConfig) {
        self.config = config;
    }

    /// Get the system configuration
    pub fn get_config(&self) -> &QuotaSystemConfig {
        &self.config
    }

    /// Add a new quota rule. Returns the rule ID.
    pub fn add_rule(&mut self, rule: QuotaRule) -> Result<String, QuotaError> {
        if self.config.max_rules > 0 && self.rules.len() >= self.config.max_rules {
            return Err(QuotaError::MaxRulesExceeded);
        }
        let id = rule.id.clone();
        if self.rules.contains_key(&id) {
            return Err(QuotaError::DuplicateRuleId(id));
        }
        // Initialize usage for today if not present
        let today = Utc::now().date_naive();
        self.usage
            .entry(id.clone())
            .or_insert_with(|| QuotaUsage::new(today));
        self.rules.insert(id.clone(), rule);
        Ok(id)
    }

    /// Remove a quota rule by ID
    pub fn remove_rule(&mut self, rule_id: &str) -> bool {
        self.rules.remove(rule_id).is_some() && {
            self.usage.remove(rule_id);
            true
        }
    }

    /// Get a rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&QuotaRule> {
        self.rules.get(rule_id)
    }

    /// List all rules
    pub fn list_rules(&self) -> Vec<&QuotaRule> {
        let mut rules: Vec<_> = self.rules.values().collect();
        rules.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        rules
    }

    /// Enable or disable a rule
    pub fn set_rule_enabled(&mut self, rule_id: &str, enabled: bool) -> bool {
        if let Some(rule) = self.rules.get_mut(rule_id) {
            rule.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Update the daily limit of a rule
    pub fn set_rule_limit(&mut self, rule_id: &str, daily_limit_bytes: u64) -> bool {
        if let Some(rule) = self.rules.get_mut(rule_id) {
            rule.daily_limit_bytes = daily_limit_bytes;
            true
        } else {
            false
        }
    }

    /// Record bytes downloaded for a task. Returns a list of rule IDs that were newly exceeded.
    pub fn record_usage(
        &mut self,
        task_tags: &[String],
        task_group: Option<&str>,
        bytes: u64,
    ) -> Vec<String> {
        if !self.config.enabled {
            return Vec::new();
        }

        let today = Utc::now().date_naive();
        let mut newly_exceeded = Vec::new();

        // Find all matching rules for this task
        let matching_rule_ids: Vec<String> = self
            .rules
            .values()
            .filter(|rule| {
                if !rule.enabled || rule.daily_limit_bytes == 0 {
                    return false;
                }
                match &rule.scope {
                    QuotaScope::Tag(tag) => task_tags.iter().any(|t| t == tag),
                    QuotaScope::Group(group) => task_group == Some(group.as_str()),
                }
            })
            .map(|rule| rule.id.clone())
            .collect();

        for rule_id in matching_rule_ids {
            let usage = self
                .usage
                .entry(rule_id.clone())
                .or_insert_with(|| QuotaUsage::new(today));

            // Reset if it's a new day
            if usage.date != today {
                usage.reset_for_new_day(today);
            }

            let was_exceeded = usage.exceeded;
            usage.add_bytes(bytes);

            // Check if newly exceeded
            if let Some(rule) = self.rules.get(&rule_id) {
                if !was_exceeded && usage.exceeds(rule.daily_limit_bytes) {
                    usage.exceeded = true;
                    newly_exceeded.push(rule_id);
                }
            }
        }

        newly_exceeded
    }

    /// Record that a task contributed to quota usage (for task counting)
    pub fn record_task_contribution(&mut self, task_tags: &[String], task_group: Option<&str>) {
        if !self.config.enabled {
            return;
        }

        let today = Utc::now().date_naive();

        let matching_rule_ids: Vec<String> = self
            .rules
            .values()
            .filter(|rule| {
                if !rule.enabled || rule.daily_limit_bytes == 0 {
                    return false;
                }
                match &rule.scope {
                    QuotaScope::Tag(tag) => task_tags.iter().any(|t| t == tag),
                    QuotaScope::Group(group) => task_group == Some(group.as_str()),
                }
            })
            .map(|rule| rule.id.clone())
            .collect();

        for rule_id in matching_rule_ids {
            let usage = self
                .usage
                .entry(rule_id.clone())
                .or_insert_with(|| QuotaUsage::new(today));

            if usage.date != today {
                usage.reset_for_new_day(today);
            }
            usage.increment_task_count();
        }
    }

    /// Check if a task should be paused based on its tags/group
    pub fn should_pause_task(&self, task_tags: &[String], task_group: Option<&str>) -> bool {
        if !self.config.enabled {
            return false;
        }

        let today = Utc::now().date_naive();

        for rule in self.rules.values() {
            if !rule.enabled || rule.daily_limit_bytes == 0 {
                continue;
            }

            let matches = match &rule.scope {
                QuotaScope::Tag(tag) => task_tags.iter().any(|t| t == tag),
                QuotaScope::Group(group) => task_group == Some(group.as_str()),
            };

            if matches {
                if let Some(usage) = self.usage.get(&rule.id) {
                    let effective_usage = if usage.date == today {
                        usage
                    } else {
                        // Would be reset, so not exceeded
                        continue;
                    };
                    if effective_usage.exceeds(rule.daily_limit_bytes) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Refresh all usage records (reset for new day if needed)
    pub fn refresh_all(&mut self) {
        let today = Utc::now().date_naive();
        for usage in self.usage.values_mut() {
            if usage.date != today {
                usage.reset_for_new_day(today);
            }
        }
    }

    /// Get the status of a single rule
    pub fn get_status(&self, rule_id: &str, matching_task_count: usize) -> Option<QuotaStatus> {
        let rule = self.rules.get(rule_id)?;
        let today = Utc::now().date_naive();
        let usage = self
            .usage
            .get(rule_id)
            .cloned()
            .unwrap_or_else(|| QuotaUsage::new(today));

        let effective_usage = if usage.date == today {
            usage
        } else {
            QuotaUsage::new(today)
        };

        let remaining = effective_usage.remaining(rule.daily_limit_bytes);
        let usage_percent = effective_usage.usage_percent(rule.daily_limit_bytes);
        let is_exceeded = effective_usage.exceeds(rule.daily_limit_bytes);

        Some(QuotaStatus {
            rule: rule.clone(),
            usage: effective_usage,
            remaining_bytes: remaining,
            usage_percent,
            is_exceeded,
            matching_task_count,
        })
    }

    /// Get summary of all quota statuses
    pub fn get_summary<F>(&self, count_fn: F) -> QuotaSummary
    where
        F: Fn(&QuotaScope) -> usize,
    {
        let today = Utc::now().date_naive();
        let mut statuses = Vec::new();
        let mut total_bytes_today = 0u64;
        let mut exceeded_count = 0;

        for rule in self.rules.values() {
            let usage = self
                .usage
                .get(&rule.id)
                .cloned()
                .unwrap_or_else(|| QuotaUsage::new(today));

            let effective_usage = if usage.date == today {
                usage
            } else {
                QuotaUsage::new(today)
            };

            let matching_task_count = count_fn(&rule.scope);
            let remaining = effective_usage.remaining(rule.daily_limit_bytes);
            let usage_percent = effective_usage.usage_percent(rule.daily_limit_bytes);
            let is_exceeded = effective_usage.exceeds(rule.daily_limit_bytes);

            if is_exceeded {
                exceeded_count += 1;
            }
            total_bytes_today += effective_usage.bytes_downloaded;

            statuses.push(QuotaStatus {
                rule: rule.clone(),
                usage: effective_usage,
                remaining_bytes: remaining,
                usage_percent,
                is_exceeded,
                matching_task_count,
            });
        }

        // Sort by scope type then name
        statuses.sort_by(|a, b| {
            a.rule
                .scope
                .type_label()
                .cmp(b.rule.scope.type_label())
                .then_with(|| a.rule.scope.name().cmp(b.rule.scope.name()))
        });

        QuotaSummary {
            total_rules: self.rules.len(),
            enabled_rules: self.rules.values().filter(|r| r.enabled).count(),
            exceeded_rules: exceeded_count,
            statuses,
            total_bytes_today,
        }
    }

    /// Clear all usage data (reset for all rules)
    pub fn clear_usage(&mut self) {
        let today = Utc::now().date_naive();
        for usage in self.usage.values_mut() {
            usage.reset_for_new_day(today);
        }
    }

    /// Clear usage for a specific rule
    pub fn clear_rule_usage(&mut self, rule_id: &str) -> bool {
        let today = Utc::now().date_naive();
        if let Some(usage) = self.usage.get_mut(rule_id) {
            usage.reset_for_new_day(today);
            true
        } else {
            false
        }
    }
}

/// Errors that can occur with quota operations
#[derive(Debug, Clone)]
pub enum QuotaError {
    MaxRulesExceeded,
    DuplicateRuleId(String),
}

impl std::fmt::Display for QuotaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaError::MaxRulesExceeded => write!(f, "Maximum number of quota rules exceeded"),
            QuotaError::DuplicateRuleId(id) => write!(f, "Duplicate quota rule ID: {id}"),
        }
    }
}

impl std::error::Error for QuotaError {}

/// Persistence functions

/// Save quota manager state to disk
pub fn save_download_quota(
    manager: &DownloadQuotaManager,
    data_dir: &Path,
) -> Result<(), QuotaPersistenceError> {
    let path = data_dir.join("download_quota.json");
    let json = serde_json::to_string_pretty(manager)
        .map_err(|e| QuotaPersistenceError::Serialize(e.to_string()))?;
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, json.as_bytes())
        .map_err(|e| QuotaPersistenceError::Io(e.to_string()))?;
    std::fs::rename(&tmp_path, &path).map_err(|e| QuotaPersistenceError::Io(e.to_string()))?;
    Ok(())
}

/// Load quota manager state from disk
pub fn load_download_quota(data_dir: &Path) -> Option<DownloadQuotaManager> {
    let path = data_dir.join("download_quota.json");
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

/// Persistence error
#[derive(Debug, Clone)]
pub enum QuotaPersistenceError {
    Io(String),
    Serialize(String),
}

impl std::fmt::Display for QuotaPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaPersistenceError::Io(e) => write!(f, "I/O error: {e}"),
            QuotaPersistenceError::Serialize(e) => write!(f, "Serialization error: {e}"),
        }
    }
}

impl std::error::Error for QuotaPersistenceError {}

/// Helper to format bytes to human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tags(tags: &[&str]) -> Vec<String> {
        tags.iter().map(|s| s.to_string()).collect()
    }

    // === QuotaScope tests ===

    #[test]
    fn test_quota_scope_name() {
        let tag = QuotaScope::Tag("movies".to_string());
        assert_eq!(tag.name(), "movies");
        let group = QuotaScope::Group("work".to_string());
        assert_eq!(group.name(), "work");
    }

    #[test]
    fn test_quota_scope_type_label() {
        let tag = QuotaScope::Tag("movies".to_string());
        assert_eq!(tag.type_label(), "tag");
        let group = QuotaScope::Group("work".to_string());
        assert_eq!(group.type_label(), "group");
    }

    // === QuotaUsage tests ===

    #[test]
    fn test_quota_usage_new() {
        let today = Utc::now().date_naive();
        let usage = QuotaUsage::new(today);
        assert_eq!(usage.bytes_downloaded, 0);
        assert_eq!(usage.task_count, 0);
        assert!(!usage.exceeded);
    }

    #[test]
    fn test_quota_usage_add_bytes() {
        let today = Utc::now().date_naive();
        let mut usage = QuotaUsage::new(today);
        usage.add_bytes(1024);
        usage.add_bytes(2048);
        assert_eq!(usage.bytes_downloaded, 3072);
    }

    #[test]
    fn test_quota_usage_exceeds() {
        let today = Utc::now().date_naive();
        let mut usage = QuotaUsage::new(today);
        usage.add_bytes(500);
        assert!(!usage.exceeds(1000));
        usage.add_bytes(600);
        assert!(usage.exceeds(1000));
        // 0 limit = never exceeds
        assert!(!usage.exceeds(0));
    }

    #[test]
    fn test_quota_usage_remaining() {
        let today = Utc::now().date_naive();
        let mut usage = QuotaUsage::new(today);
        usage.add_bytes(300);
        assert_eq!(usage.remaining(1000), 700);
        // 0 limit = unlimited
        assert_eq!(usage.remaining(0), u64::MAX);
        // Over limit
        usage.add_bytes(800);
        assert_eq!(usage.remaining(1000), 0);
    }

    #[test]
    fn test_quota_usage_percent() {
        let today = Utc::now().date_naive();
        let mut usage = QuotaUsage::new(today);
        usage.add_bytes(500);
        let pct = usage.usage_percent(1000);
        assert!((pct - 50.0).abs() < 0.1);
        // 0 limit = 0%
        assert_eq!(usage.usage_percent(0), 0.0);
    }

    #[test]
    fn test_quota_usage_reset() {
        let today = Utc::now().date_naive();
        let mut usage = QuotaUsage::new(today);
        usage.add_bytes(5000);
        usage.exceeded = true;
        let tomorrow = today + chrono::Duration::days(1);
        usage.reset_for_new_day(tomorrow);
        assert_eq!(usage.bytes_downloaded, 0);
        assert_eq!(usage.date, tomorrow);
        assert!(!usage.exceeded);
    }

    // === QuotaRule tests ===

    #[test]
    fn test_quota_rule_new() {
        let rule = QuotaRule::new(
            "r1".to_string(),
            QuotaScope::Tag("movies".to_string()),
            1_000_000,
        );
        assert_eq!(rule.id, "r1");
        assert!(rule.enabled);
        assert_eq!(rule.daily_limit_bytes, 1_000_000);
    }

    // === DownloadQuotaManager tests ===

    #[test]
    fn test_manager_add_remove_rule() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new(
            "r1".to_string(),
            QuotaScope::Tag("movies".to_string()),
            1_000_000,
        );
        let id = mgr.add_rule(rule).unwrap();
        assert_eq!(id, "r1");
        assert!(mgr.get_rule("r1").is_some());
        assert!(mgr.remove_rule("r1"));
        assert!(mgr.get_rule("r1").is_none());
    }

    #[test]
    fn test_manager_duplicate_rule_id() {
        let mut mgr = DownloadQuotaManager::new();
        let rule1 = QuotaRule::new("r1".to_string(), QuotaScope::Tag("a".to_string()), 100);
        let rule2 = QuotaRule::new("r1".to_string(), QuotaScope::Tag("b".to_string()), 200);
        mgr.add_rule(rule1).unwrap();
        assert!(matches!(
            mgr.add_rule(rule2),
            Err(QuotaError::DuplicateRuleId(_))
        ));
    }

    #[test]
    fn test_manager_max_rules() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.max_rules = 2;
        let r1 = QuotaRule::new("r1".to_string(), QuotaScope::Tag("a".to_string()), 100);
        let r2 = QuotaRule::new("r2".to_string(), QuotaScope::Tag("b".to_string()), 200);
        let r3 = QuotaRule::new("r3".to_string(), QuotaScope::Tag("c".to_string()), 300);
        mgr.add_rule(r1).unwrap();
        mgr.add_rule(r2).unwrap();
        assert!(matches!(
            mgr.add_rule(r3),
            Err(QuotaError::MaxRulesExceeded)
        ));
    }

    #[test]
    fn test_manager_record_usage_tag() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new(
            "r1".to_string(),
            QuotaScope::Tag("movies".to_string()),
            1000,
        );
        mgr.add_rule(rule).unwrap();

        let tags = make_tags(&["movies", "hd"]);
        let exceeded = mgr.record_usage(&tags, None, 500);
        assert!(exceeded.is_empty());

        let exceeded = mgr.record_usage(&tags, None, 600);
        assert_eq!(exceeded, vec!["r1"]);
    }

    #[test]
    fn test_manager_record_usage_group() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new("r1".to_string(), QuotaScope::Group("work".to_string()), 500);
        mgr.add_rule(rule).unwrap();

        let tags = make_tags(&[]);
        let exceeded = mgr.record_usage(&tags, Some("work"), 300);
        assert!(exceeded.is_empty());

        let exceeded = mgr.record_usage(&tags, Some("work"), 300);
        assert_eq!(exceeded.len(), 1);
    }

    #[test]
    fn test_manager_should_pause_task() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new(
            "r1".to_string(),
            QuotaScope::Tag("movies".to_string()),
            1000,
        );
        mgr.add_rule(rule).unwrap();

        let tags = make_tags(&["movies"]);
        assert!(!mgr.should_pause_task(&tags, None));

        // Record enough to exceed
        mgr.record_usage(&tags, None, 1100);
        assert!(mgr.should_pause_task(&tags, None));

        // Non-matching task should not be paused
        let other_tags = make_tags(&["music"]);
        assert!(!mgr.should_pause_task(&other_tags, None));
    }

    #[test]
    fn test_manager_disabled_does_nothing() {
        let mut mgr = DownloadQuotaManager::new();
        // config.enabled defaults to false
        let rule = QuotaRule::new("r1".to_string(), QuotaScope::Tag("movies".to_string()), 100);
        mgr.add_rule(rule).unwrap();

        let tags = make_tags(&["movies"]);
        let exceeded = mgr.record_usage(&tags, None, 200);
        assert!(exceeded.is_empty());
        assert!(!mgr.should_pause_task(&tags, None));
    }

    #[test]
    fn test_manager_set_rule_enabled() {
        let mut mgr = DownloadQuotaManager::new();
        let rule = QuotaRule::new("r1".to_string(), QuotaScope::Tag("a".to_string()), 100);
        mgr.add_rule(rule).unwrap();
        assert!(mgr.set_rule_enabled("r1", false));
        assert!(!mgr.get_rule("r1").unwrap().enabled);
        assert!(mgr.set_rule_enabled("nonexistent", true) == false);
    }

    #[test]
    fn test_manager_set_rule_limit() {
        let mut mgr = DownloadQuotaManager::new();
        let rule = QuotaRule::new("r1".to_string(), QuotaScope::Tag("a".to_string()), 100);
        mgr.add_rule(rule).unwrap();
        assert!(mgr.set_rule_limit("r1", 5000));
        assert_eq!(mgr.get_rule("r1").unwrap().daily_limit_bytes, 5000);
        assert!(!mgr.set_rule_limit("nonexistent", 100));
    }

    #[test]
    fn test_manager_list_rules_sorted() {
        let mut mgr = DownloadQuotaManager::new();
        let r1 = QuotaRule::new("r1".to_string(), QuotaScope::Tag("b".to_string()), 100);
        let r2 = QuotaRule::new("r2".to_string(), QuotaScope::Tag("a".to_string()), 200);
        mgr.add_rule(r1).unwrap();
        mgr.add_rule(r2).unwrap();
        let rules = mgr.list_rules();
        // Should be sorted by created_at, both created nearly simultaneously
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn test_manager_get_summary() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let r1 = QuotaRule::new(
            "r1".to_string(),
            QuotaScope::Tag("movies".to_string()),
            1000,
        );
        let r2 = QuotaRule::new("r2".to_string(), QuotaScope::Group("work".to_string()), 500);
        mgr.add_rule(r1).unwrap();
        mgr.add_rule(r2).unwrap();

        let tags = make_tags(&["movies"]);
        mgr.record_usage(&tags, None, 600);
        mgr.record_usage(&[], Some("work"), 600);

        let summary = mgr.get_summary(|scope| match scope {
            QuotaScope::Tag(t) if t == "movies" => 3,
            QuotaScope::Group(g) if g == "work" => 2,
            _ => 0,
        });

        assert_eq!(summary.total_rules, 2);
        assert_eq!(summary.enabled_rules, 2);
        assert_eq!(summary.exceeded_rules, 1); // work exceeded (600 > 500)
        assert_eq!(summary.total_bytes_today, 1200);
        assert_eq!(summary.statuses.len(), 2);
    }

    #[test]
    fn test_manager_clear_usage() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new("r1".to_string(), QuotaScope::Tag("a".to_string()), 1000);
        mgr.add_rule(rule).unwrap();

        let tags = make_tags(&["a"]);
        mgr.record_usage(&tags, None, 500);
        assert_eq!(mgr.usage["r1"].bytes_downloaded, 500);

        mgr.clear_usage();
        assert_eq!(mgr.usage["r1"].bytes_downloaded, 0);
    }

    #[test]
    fn test_manager_clear_rule_usage() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new("r1".to_string(), QuotaScope::Tag("a".to_string()), 1000);
        mgr.add_rule(rule).unwrap();

        let tags = make_tags(&["a"]);
        mgr.record_usage(&tags, None, 500);
        assert!(mgr.clear_rule_usage("r1"));
        assert_eq!(mgr.usage["r1"].bytes_downloaded, 0);
        assert!(!mgr.clear_rule_usage("nonexistent"));
    }

    #[test]
    fn test_manager_refresh_all() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new("r1".to_string(), QuotaScope::Tag("a".to_string()), 1000);
        mgr.add_rule(rule).unwrap();

        // Set usage to yesterday
        let yesterday = Utc::now().date_naive() - chrono::Duration::days(1);
        mgr.usage.get_mut("r1").unwrap().date = yesterday;
        mgr.usage.get_mut("r1").unwrap().bytes_downloaded = 500;

        mgr.refresh_all();
        assert_eq!(mgr.usage["r1"].bytes_downloaded, 0);
        assert_eq!(mgr.usage["r1"].date, Utc::now().date_naive());
    }

    #[test]
    fn test_manager_record_task_contribution() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new(
            "r1".to_string(),
            QuotaScope::Tag("movies".to_string()),
            1000,
        );
        mgr.add_rule(rule).unwrap();

        let tags = make_tags(&["movies"]);
        mgr.record_task_contribution(&tags, None);
        mgr.record_task_contribution(&tags, None);
        assert_eq!(mgr.usage["r1"].task_count, 2);
    }

    // === QuotaSummary format tests ===

    #[test]
    fn test_summary_format_empty() {
        let summary = QuotaSummary {
            total_rules: 0,
            enabled_rules: 0,
            exceeded_rules: 0,
            statuses: vec![],
            total_bytes_today: 0,
        };
        let report = summary.format_report();
        assert!(report.contains("No quota rules configured"));
    }

    #[test]
    fn test_summary_format_with_rules() {
        let summary = QuotaSummary {
            total_rules: 1,
            enabled_rules: 1,
            exceeded_rules: 0,
            statuses: vec![QuotaStatus {
                rule: QuotaRule::new(
                    "r1".to_string(),
                    QuotaScope::Tag("movies".to_string()),
                    1_000_000,
                ),
                usage: QuotaUsage {
                    date: Utc::now().date_naive(),
                    bytes_downloaded: 500_000,
                    task_count: 3,
                    last_updated: Utc::now(),
                    exceeded: false,
                },
                remaining_bytes: 500_000,
                usage_percent: 50.0,
                is_exceeded: false,
                matching_task_count: 2,
            }],
            total_bytes_today: 500_000,
        };
        let report = summary.format_report();
        assert!(report.contains("tag:movies"));
        assert!(report.contains("50.0%"));
    }

    // === Persistence tests ===

    #[test]
    fn test_save_and_load() {
        let dir = std::env::temp_dir().join("quota_test_save_load");
        let _ = std::fs::create_dir_all(&dir);

        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let rule = QuotaRule::new(
            "r1".to_string(),
            QuotaScope::Tag("movies".to_string()),
            1000,
        );
        mgr.add_rule(rule).unwrap();

        save_download_quota(&mgr, &dir).unwrap();
        let loaded = load_download_quota(&dir).unwrap();
        assert!(loaded.config.enabled);
        assert!(loaded.rules.contains_key("r1"));
        assert_eq!(loaded.rules["r1"].daily_limit_bytes, 1000);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_missing_file() {
        let dir = std::env::temp_dir().join("quota_test_missing");
        let _ = std::fs::create_dir_all(&dir);
        assert!(load_download_quota(&dir).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // === format_bytes tests ===

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1_500_000), "1.43 MB");
        assert_eq!(format_bytes(2_000_000_000), "1.86 GB");
    }

    // === QuotaError tests ===

    #[test]
    fn test_quota_error_display() {
        let err = QuotaError::MaxRulesExceeded;
        assert!(err.to_string().contains("Maximum"));
        let err = QuotaError::DuplicateRuleId("test".to_string());
        assert!(err.to_string().contains("test"));
    }

    // === Multiple scopes for same task ===

    #[test]
    fn test_task_matches_multiple_rules() {
        let mut mgr = DownloadQuotaManager::new();
        mgr.config.enabled = true;
        let r1 = QuotaRule::new(
            "r1".to_string(),
            QuotaScope::Tag("movies".to_string()),
            1000,
        );
        let r2 = QuotaRule::new(
            "r2".to_string(),
            QuotaScope::Group("entertainment".to_string()),
            500,
        );
        mgr.add_rule(r1).unwrap();
        mgr.add_rule(r2).unwrap();

        let tags = make_tags(&["movies"]);
        // Record 600 bytes — should exceed r2 (group, limit 500) but not r1 (tag, limit 1000)
        let exceeded = mgr.record_usage(&tags, Some("entertainment"), 600);
        assert_eq!(exceeded.len(), 1);
        assert_eq!(exceeded[0], "r2");
    }
}
