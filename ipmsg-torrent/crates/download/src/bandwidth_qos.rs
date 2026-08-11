//! Download Bandwidth QoS (Quality of Service) Classification System
//!
//! Classifies downloads into QoS tiers and provides bandwidth allocation hints.
//! Higher-priority tiers receive preferential bandwidth treatment.
//!
//! Features:
//! - Five QoS tiers: Critical / High / Normal / Low / Background
//! - Per-task QoS assignment with auto-classification by URL pattern
//! - Bandwidth weight multipliers per tier
//! - Auto-classification rules based on URL patterns, file extensions, or domains
//! - QoS summary with per-tier task counts and bandwidth distribution
//! - Persistence to bandwidth_qos_config.json

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Error type for bandwidth QoS operations.
#[derive(Debug, thiserror::Error)]
pub enum BandwidthQosError {
    #[error("task {0} not found")]
    TaskNotFound(String),
    #[error("rule {0} not found")]
    RuleNotFound(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("persistence error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// QoS tier for download classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QosTier {
    /// Mission-critical downloads (weight 8.0)
    Critical,
    /// High-priority downloads (weight 4.0)
    High,
    /// Normal-priority downloads (weight 2.0) - default
    #[default]
    Normal,
    /// Low-priority downloads (weight 1.0)
    Low,
    /// Background downloads (weight 0.5)
    Background,
}

impl QosTier {
    /// Get the bandwidth weight multiplier for this tier.
    pub fn bandwidth_weight(&self) -> f64 {
        match self {
            QosTier::Critical => 8.0,
            QosTier::High => 4.0,
            QosTier::Normal => 2.0,
            QosTier::Low => 1.0,
            QosTier::Background => 0.5,
        }
    }

    /// Get the human-readable name.
    pub fn display_name(&self) -> &'static str {
        match self {
            QosTier::Critical => "Critical",
            QosTier::High => "High",
            QosTier::Normal => "Normal",
            QosTier::Low => "Low",
            QosTier::Background => "Background",
        }
    }

    /// Get the emoji indicator.
    pub fn emoji(&self) -> &'static str {
        match self {
            QosTier::Critical => "🔴",
            QosTier::High => "🟠",
            QosTier::Normal => "🟢",
            QosTier::Low => "🔵",
            QosTier::Background => "⚪",
        }
    }

    /// Get all tiers in priority order (highest first).
    pub fn all_in_order() -> Vec<QosTier> {
        vec![
            QosTier::Critical,
            QosTier::High,
            QosTier::Normal,
            QosTier::Low,
            QosTier::Background,
        ]
    }

    /// Parse from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "critical" => Some(QosTier::Critical),
            "high" => Some(QosTier::High),
            "normal" => Some(QosTier::Normal),
            "low" => Some(QosTier::Low),
            "background" => Some(QosTier::Background),
            _ => None,
        }
    }
}

impl std::fmt::Display for QosTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.emoji(), self.display_name())
    }
}

/// Pattern type for auto-classification rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QosPattern {
    /// Match URL contains substring (case-insensitive).
    UrlContains(String),
    /// Match URL starts with prefix (case-insensitive).
    UrlStartsWith(String),
    /// Match file extension (e.g., ".iso", ".exe").
    FileExtension(String),
    /// Match domain name (case-insensitive).
    Domain(String),
    /// Match task name contains substring (case-insensitive).
    NameContains(String),
}

impl QosPattern {
    /// Check if a URL matches this pattern.
    pub fn matches_url(&self, url: &str) -> bool {
        let lower_url = url.to_lowercase();
        match self {
            QosPattern::UrlContains(s) => lower_url.contains(&s.to_lowercase()),
            QosPattern::UrlStartsWith(prefix) => lower_url.starts_with(&prefix.to_lowercase()),
            QosPattern::FileExtension(ext) => {
                let lower_ext = ext.to_lowercase();
                lower_url.ends_with(&lower_ext)
                    || lower_url
                        .split('?')
                        .next()
                        .map(|p| p.ends_with(&lower_ext))
                        .unwrap_or(false)
            }
            QosPattern::Domain(domain) => {
                let lower_domain = domain.to_lowercase();
                lower_url.contains(&lower_domain)
            }
            QosPattern::NameContains(_) => false, // Name patterns match task names, not URLs
        }
    }

    /// Check if a task name matches this pattern.
    pub fn matches_name(&self, name: &str) -> bool {
        let lower_name = name.to_lowercase();
        match self {
            QosPattern::NameContains(s) => lower_name.contains(&s.to_lowercase()),
            _ => false,
        }
    }

    /// Check if either URL or name matches.
    pub fn matches(&self, url: &str, name: &str) -> bool {
        self.matches_url(url) || self.matches_name(name)
    }
}

/// Auto-classification rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QosAutoRule {
    /// Unique rule ID.
    pub id: String,
    /// Human-readable description.
    pub description: String,
    /// Pattern to match.
    pub pattern: QosPattern,
    /// QoS tier to assign when matched.
    pub tier: QosTier,
    /// Whether this rule is enabled.
    pub enabled: bool,
    /// Rule priority (higher = checked first).
    pub priority: i32,
    /// Number of times this rule has been triggered.
    pub trigger_count: u64,
    /// Last time this rule was triggered.
    pub last_triggered: Option<DateTime<Utc>>,
}

impl QosAutoRule {
    /// Create a new auto-classification rule.
    pub fn new(id: String, description: String, pattern: QosPattern, tier: QosTier) -> Self {
        Self {
            id,
            description,
            pattern,
            tier,
            enabled: true,
            priority: 0,
            trigger_count: 0,
            last_triggered: None,
        }
    }

    /// Set the priority.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Mark the rule as triggered.
    pub fn mark_triggered(&mut self) {
        self.trigger_count += 1;
        self.last_triggered = Some(Utc::now());
    }
}

/// Per-task QoS assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskQosAssignment {
    /// Task ID.
    pub task_id: String,
    /// Assigned QoS tier.
    pub tier: QosTier,
    /// Whether the tier was manually set (vs auto-classified).
    pub manual: bool,
    /// Time of assignment.
    pub assigned_at: DateTime<Utc>,
    /// Rule ID that triggered auto-classification (if applicable).
    pub rule_id: Option<String>,
}

/// QoS system configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthQosConfig {
    /// Enable QoS classification.
    pub enabled: bool,
    /// Default tier for unclassified tasks.
    pub default_tier: QosTier,
    /// Enable auto-classification rules.
    pub auto_classify: bool,
    /// Bandwidth weight overrides per tier (optional).
    pub weight_overrides: HashMap<String, f64>,
    /// Maximum number of auto-classification rules.
    pub max_rules: usize,
}

impl Default for BandwidthQosConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_tier: QosTier::Normal,
            auto_classify: true,
            weight_overrides: HashMap::new(),
            max_rules: 100,
        }
    }
}

/// Per-tier statistics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QosTierStats {
    /// Number of tasks in this tier.
    pub task_count: usize,
    /// Total bandwidth weight for this tier.
    pub total_weight: f64,
    /// Percentage of total bandwidth (0.0 - 1.0).
    pub bandwidth_share: f64,
}

/// QoS system summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthQosSummary {
    /// Whether QoS is enabled.
    pub enabled: bool,
    /// Total number of classified tasks.
    pub total_tasks: usize,
    /// Per-tier statistics.
    pub tier_stats: HashMap<String, QosTierStats>,
    /// Number of auto-classification rules.
    pub rule_count: usize,
    /// Number of enabled rules.
    pub enabled_rule_count: usize,
    /// Effective weight per tier.
    pub effective_weights: HashMap<String, f64>,
}

/// Persistable data.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BandwidthQosData {
    /// Configuration.
    pub config: BandwidthQosConfig,
    /// Auto-classification rules.
    pub rules: Vec<QosAutoRule>,
    /// Task assignments.
    pub assignments: HashMap<String, TaskQosAssignment>,
}

/// Bandwidth QoS manager.
#[derive(Debug, Clone)]
pub struct BandwidthQosManager {
    config: BandwidthQosConfig,
    rules: Vec<QosAutoRule>,
    assignments: HashMap<String, TaskQosAssignment>,
}

impl BandwidthQosManager {
    /// Create a new manager with default configuration.
    pub fn new() -> Self {
        Self {
            config: BandwidthQosConfig::default(),
            rules: Vec::new(),
            assignments: HashMap::new(),
        }
    }

    /// Create from persisted data.
    pub fn from_data(data: BandwidthQosData) -> Self {
        Self {
            config: data.config,
            rules: data.rules,
            assignments: data.assignments,
        }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &BandwidthQosConfig {
        &self.config
    }

    /// Set the configuration.
    pub fn set_config(&mut self, config: BandwidthQosConfig) {
        self.config = config;
    }

    /// Get the effective weight for a tier (considering overrides).
    pub fn effective_weight(&self, tier: &QosTier) -> f64 {
        self.config
            .weight_overrides
            .get(&format!("{:?}", tier).to_lowercase())
            .copied()
            .unwrap_or_else(|| tier.bandwidth_weight())
    }

    /// Manually assign a QoS tier to a task.
    pub fn assign_tier(&mut self, task_id: &str, tier: QosTier) -> Result<(), BandwidthQosError> {
        if !self.config.enabled {
            return Err(BandwidthQosError::InvalidConfig(
                "QoS classification is disabled".to_string(),
            ));
        }

        self.assignments.insert(
            task_id.to_string(),
            TaskQosAssignment {
                task_id: task_id.to_string(),
                tier,
                manual: true,
                assigned_at: Utc::now(),
                rule_id: None,
            },
        );
        Ok(())
    }

    /// Remove QoS assignment for a task.
    pub fn remove_assignment(&mut self, task_id: &str) -> Result<(), BandwidthQosError> {
        self.assignments
            .remove(task_id)
            .map(|_| ())
            .ok_or_else(|| BandwidthQosError::TaskNotFound(task_id.to_string()))
    }

    /// Get the QoS tier for a task.
    pub fn get_tier(&self, task_id: &str) -> QosTier {
        self.assignments
            .get(task_id)
            .map(|a| a.tier)
            .unwrap_or(self.config.default_tier)
    }

    /// Get the QoS assignment for a task.
    pub fn get_assignment(&self, task_id: &str) -> Option<&TaskQosAssignment> {
        self.assignments.get(task_id)
    }

    /// Get the bandwidth weight for a task.
    pub fn get_task_weight(&self, task_id: &str) -> f64 {
        let tier = self.get_tier(task_id);
        self.effective_weight(&tier)
    }

    /// Auto-classify a task based on URL and name.
    /// Returns the assigned tier, or None if no rule matched.
    pub fn auto_classify(&mut self, task_id: &str, url: &str, name: &str) -> Option<QosTier> {
        if !self.config.enabled || !self.config.auto_classify {
            return None;
        }

        // Sort rules by priority (highest first)
        let mut sorted_rules: Vec<&mut QosAutoRule> =
            self.rules.iter_mut().filter(|r| r.enabled).collect();
        sorted_rules.sort_by_key(|r| -r.priority);

        for rule in sorted_rules {
            if rule.pattern.matches(url, name) {
                let tier = rule.tier;
                rule.mark_triggered();

                self.assignments.insert(
                    task_id.to_string(),
                    TaskQosAssignment {
                        task_id: task_id.to_string(),
                        tier,
                        manual: false,
                        assigned_at: Utc::now(),
                        rule_id: Some(rule.id.clone()),
                    },
                );

                return Some(tier);
            }
        }

        None
    }

    /// Add an auto-classification rule.
    pub fn add_rule(&mut self, rule: QosAutoRule) -> Result<(), BandwidthQosError> {
        if self.rules.len() >= self.config.max_rules {
            return Err(BandwidthQosError::InvalidConfig(format!(
                "maximum number of rules ({}) reached",
                self.config.max_rules
            )));
        }

        // Check for duplicate IDs
        if self.rules.iter().any(|r| r.id == rule.id) {
            return Err(BandwidthQosError::InvalidConfig(format!(
                "rule with ID '{}' already exists",
                rule.id
            )));
        }

        self.rules.push(rule);
        Ok(())
    }

    /// Remove an auto-classification rule.
    pub fn remove_rule(&mut self, rule_id: &str) -> Result<QosAutoRule, BandwidthQosError> {
        let pos = self
            .rules
            .iter()
            .position(|r| r.id == rule_id)
            .ok_or_else(|| BandwidthQosError::RuleNotFound(rule_id.to_string()))?;
        Ok(self.rules.remove(pos))
    }

    /// Get a rule by ID.
    pub fn get_rule(&self, rule_id: &str) -> Option<&QosAutoRule> {
        self.rules.iter().find(|r| r.id == rule_id)
    }

    /// List all rules sorted by priority.
    pub fn list_rules(&self) -> Vec<&QosAutoRule> {
        let mut rules: Vec<&QosAutoRule> = self.rules.iter().collect();
        rules.sort_by_key(|r| -r.priority);
        rules
    }

    /// Enable or disable a rule.
    pub fn set_rule_enabled(
        &mut self,
        rule_id: &str,
        enabled: bool,
    ) -> Result<(), BandwidthQosError> {
        let rule = self
            .rules
            .iter_mut()
            .find(|r| r.id == rule_id)
            .ok_or_else(|| BandwidthQosError::RuleNotFound(rule_id.to_string()))?;
        rule.enabled = enabled;
        Ok(())
    }

    /// Update rule priority.
    pub fn set_rule_priority(
        &mut self,
        rule_id: &str,
        priority: i32,
    ) -> Result<(), BandwidthQosError> {
        let rule = self
            .rules
            .iter_mut()
            .find(|r| r.id == rule_id)
            .ok_or_else(|| BandwidthQosError::RuleNotFound(rule_id.to_string()))?;
        rule.priority = priority;
        Ok(())
    }

    /// Get a summary of the QoS system.
    pub fn summary(&self) -> BandwidthQosSummary {
        let mut tier_stats: HashMap<String, QosTierStats> = HashMap::new();
        let mut total_weight = 0.0;

        // Initialize all tiers
        for tier in QosTier::all_in_order() {
            tier_stats.insert(
                format!("{:?}", tier).to_lowercase(),
                QosTierStats::default(),
            );
        }

        // Count tasks per tier
        for assignment in self.assignments.values() {
            let tier_key = format!("{:?}", assignment.tier).to_lowercase();
            let weight = self.effective_weight(&assignment.tier);
            if let Some(stats) = tier_stats.get_mut(&tier_key) {
                stats.task_count += 1;
                stats.total_weight += weight;
                total_weight += weight;
            }
        }

        // Calculate bandwidth shares
        if total_weight > 0.0 {
            for stats in tier_stats.values_mut() {
                stats.bandwidth_share = stats.total_weight / total_weight;
            }
        }

        // Effective weights
        let mut effective_weights = HashMap::new();
        for tier in QosTier::all_in_order() {
            effective_weights.insert(
                format!("{:?}", tier).to_lowercase(),
                self.effective_weight(&tier),
            );
        }

        let enabled_rule_count = self.rules.iter().filter(|r| r.enabled).count();

        BandwidthQosSummary {
            enabled: self.config.enabled,
            total_tasks: self.assignments.len(),
            tier_stats,
            rule_count: self.rules.len(),
            enabled_rule_count,
            effective_weights,
        }
    }

    /// List all task assignments.
    pub fn list_assignments(&self) -> Vec<&TaskQosAssignment> {
        self.assignments.values().collect()
    }

    /// Clear all assignments.
    pub fn clear_assignments(&mut self) {
        self.assignments.clear();
    }

    /// Clear all rules.
    pub fn clear_rules(&mut self) {
        self.rules.clear();
    }

    /// Convert to persistable data.
    pub fn to_data(&self) -> BandwidthQosData {
        BandwidthQosData {
            config: self.config.clone(),
            rules: self.rules.clone(),
            assignments: self.assignments.clone(),
        }
    }

    /// Save to disk.
    pub async fn save(&self, dir: &Path) -> Result<(), BandwidthQosError> {
        let data = self.to_data();
        let json = serde_json::to_string_pretty(&data)?;
        let path = dir.join("bandwidth_qos_config.json");
        fs::write(&path, json).await?;
        Ok(())
    }

    /// Load from disk.
    pub async fn load(dir: &Path) -> Result<Self, BandwidthQosError> {
        let path = dir.join("bandwidth_qos_config.json");
        if !path.exists() {
            return Ok(Self::new());
        }
        let json = fs::read_to_string(&path).await?;
        let data: BandwidthQosData = serde_json::from_str(&json)?;
        Ok(Self::from_data(data))
    }

    /// Format a human-readable summary report.
    pub fn format_summary(&self) -> String {
        let summary = self.summary();
        let mut out = String::new();

        out.push_str(&format!(
            "🌐 Bandwidth QoS: {}\n",
            if summary.enabled {
                "Enabled ✅"
            } else {
                "Disabled ❌"
            }
        ));
        out.push_str(&format!(
            "📊 Total classified tasks: {}\n",
            summary.total_tasks
        ));
        out.push_str(&format!(
            "📋 Rules: {} total, {} enabled\n\n",
            summary.rule_count, summary.enabled_rule_count
        ));

        out.push_str("Tier Distribution:\n");
        for tier in QosTier::all_in_order() {
            let key = format!("{:?}", tier).to_lowercase();
            if let Some(stats) = summary.tier_stats.get(&key) {
                let weight = summary.effective_weights.get(&key).copied().unwrap_or(0.0);
                out.push_str(&format!(
                    "  {} {} — {} tasks, weight {:.1}, share {:.1}%\n",
                    tier.emoji(),
                    tier.display_name(),
                    stats.task_count,
                    weight,
                    stats.bandwidth_share * 100.0
                ));
            }
        }

        if !self.rules.is_empty() {
            out.push_str("\nAuto-Classification Rules:\n");
            for rule in self.list_rules() {
                let status = if rule.enabled { "✅" } else { "❌" };
                out.push_str(&format!(
                    "  {} [{}] {} → {} (priority: {}, triggered: {})\n",
                    status, rule.id, rule.description, rule.tier, rule.priority, rule.trigger_count
                ));
            }
        }

        out
    }
}

impl Default for BandwidthQosManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qos_tier_weight() {
        assert_eq!(QosTier::Critical.bandwidth_weight(), 8.0);
        assert_eq!(QosTier::High.bandwidth_weight(), 4.0);
        assert_eq!(QosTier::Normal.bandwidth_weight(), 2.0);
        assert_eq!(QosTier::Low.bandwidth_weight(), 1.0);
        assert_eq!(QosTier::Background.bandwidth_weight(), 0.5);
    }

    #[test]
    fn test_qos_tier_from_str() {
        assert_eq!(QosTier::parse("critical"), Some(QosTier::Critical));
        assert_eq!(QosTier::parse("HIGH"), Some(QosTier::High));
        assert_eq!(QosTier::parse("Normal"), Some(QosTier::Normal));
        assert_eq!(QosTier::parse("low"), Some(QosTier::Low));
        assert_eq!(QosTier::parse("background"), Some(QosTier::Background));
        assert_eq!(QosTier::parse("invalid"), None);
    }

    #[test]
    fn test_qos_tier_display() {
        let s = format!("{}", QosTier::Critical);
        assert!(s.contains("Critical"));
    }

    #[test]
    fn test_qos_tier_default() {
        assert_eq!(QosTier::default(), QosTier::Normal);
    }

    #[test]
    fn test_qos_pattern_url_contains() {
        let pattern = QosPattern::UrlContains("ubuntu".to_string());
        assert!(pattern.matches_url("https://mirror.example.com/ubuntu-24.04.iso"));
        assert!(!pattern.matches_url("https://example.com/fedora.iso"));
    }

    #[test]
    fn test_qos_pattern_url_starts_with() {
        let pattern = QosPattern::UrlStartsWith("https://cdn.example.com".to_string());
        assert!(pattern.matches_url("https://cdn.example.com/file.zip"));
        assert!(!pattern.matches_url("https://other.com/file.zip"));
    }

    #[test]
    fn test_qos_pattern_file_extension() {
        let pattern = QosPattern::FileExtension(".iso".to_string());
        assert!(pattern.matches_url("https://example.com/ubuntu.iso"));
        assert!(pattern.matches_url("https://example.com/ubuntu.iso?token=abc"));
        assert!(!pattern.matches_url("https://example.com/file.zip"));
    }

    #[test]
    fn test_qos_pattern_domain() {
        let pattern = QosPattern::Domain("internal.corp".to_string());
        assert!(pattern.matches_url("https://internal.corp/files/data.tar.gz"));
        assert!(!pattern.matches_url("https://external.com/files/data.tar.gz"));
    }

    #[test]
    fn test_qos_pattern_name_contains() {
        let pattern = QosPattern::NameContains("backup".to_string());
        assert!(pattern.matches_name("Daily Backup Archive"));
        assert!(!pattern.matches_name("Ubuntu ISO"));
        // Name patterns don't match URLs
        assert!(!pattern.matches_url("https://example.com/backup"));
    }

    #[test]
    fn test_qos_pattern_matches_both() {
        let pattern = QosPattern::UrlContains("backup".to_string());
        assert!(pattern.matches("https://example.com/backup", "some name"));
        let name_pattern = QosPattern::NameContains("backup".to_string());
        assert!(name_pattern.matches("https://example.com/file", "Backup Task"));
    }

    #[test]
    fn test_auto_rule_creation() {
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Ubuntu mirrors".to_string(),
            QosPattern::UrlContains("ubuntu".to_string()),
            QosTier::High,
        );
        assert_eq!(rule.id, "r1");
        assert!(rule.enabled);
        assert_eq!(rule.priority, 0);
        assert_eq!(rule.trigger_count, 0);
    }

    #[test]
    fn test_auto_rule_with_priority() {
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::Normal,
        )
        .with_priority(10);
        assert_eq!(rule.priority, 10);
    }

    #[test]
    fn test_auto_rule_mark_triggered() {
        let mut rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::Normal,
        );
        assert!(rule.last_triggered.is_none());
        rule.mark_triggered();
        assert_eq!(rule.trigger_count, 1);
        assert!(rule.last_triggered.is_some());
    }

    #[test]
    fn test_manager_new() {
        let mgr = BandwidthQosManager::new();
        assert!(mgr.config().enabled);
        assert_eq!(mgr.config().default_tier, QosTier::Normal);
        assert_eq!(mgr.list_rules().len(), 0);
        assert_eq!(mgr.list_assignments().len(), 0);
    }

    #[test]
    fn test_manager_assign_tier() {
        let mut mgr = BandwidthQosManager::new();
        mgr.assign_tier("task1", QosTier::Critical).unwrap();
        assert_eq!(mgr.get_tier("task1"), QosTier::Critical);

        let assignment = mgr.get_assignment("task1").unwrap();
        assert!(assignment.manual);
        assert!(assignment.rule_id.is_none());
    }

    #[test]
    fn test_manager_assign_disabled() {
        let mut mgr = BandwidthQosManager::new();
        mgr.set_config(BandwidthQosConfig {
            enabled: false,
            ..Default::default()
        });
        assert!(mgr.assign_tier("task1", QosTier::High).is_err());
    }

    #[test]
    fn test_manager_remove_assignment() {
        let mut mgr = BandwidthQosManager::new();
        mgr.assign_tier("task1", QosTier::High).unwrap();
        mgr.remove_assignment("task1").unwrap();
        assert_eq!(mgr.get_tier("task1"), QosTier::Normal); // default
    }

    #[test]
    fn test_manager_remove_nonexistent() {
        let mut mgr = BandwidthQosManager::new();
        assert!(mgr.remove_assignment("nonexistent").is_err());
    }

    #[test]
    fn test_manager_get_task_weight() {
        let mut mgr = BandwidthQosManager::new();
        mgr.assign_tier("task1", QosTier::Critical).unwrap();
        mgr.assign_tier("task2", QosTier::Background).unwrap();
        assert_eq!(mgr.get_task_weight("task1"), 8.0);
        assert_eq!(mgr.get_task_weight("task2"), 0.5);
        assert_eq!(mgr.get_task_weight("unknown"), 2.0); // default Normal
    }

    #[test]
    fn test_manager_effective_weight_override() {
        let mut mgr = BandwidthQosManager::new();
        let mut config = BandwidthQosConfig::default();
        config.weight_overrides.insert("critical".to_string(), 16.0);
        mgr.set_config(config);
        assert_eq!(mgr.effective_weight(&QosTier::Critical), 16.0);
        assert_eq!(mgr.effective_weight(&QosTier::High), 4.0); // not overridden
    }

    #[test]
    fn test_manager_auto_classify() {
        let mut mgr = BandwidthQosManager::new();
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Ubuntu mirrors".to_string(),
            QosPattern::UrlContains("ubuntu".to_string()),
            QosTier::High,
        );
        mgr.add_rule(rule).unwrap();

        let tier = mgr.auto_classify("task1", "https://mirror.com/ubuntu-24.04.iso", "Ubuntu");
        assert_eq!(tier, Some(QosTier::High));

        let assignment = mgr.get_assignment("task1").unwrap();
        assert!(!assignment.manual);
        assert_eq!(assignment.rule_id, Some("r1".to_string()));
    }

    #[test]
    fn test_manager_auto_classify_no_match() {
        let mut mgr = BandwidthQosManager::new();
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Ubuntu mirrors".to_string(),
            QosPattern::UrlContains("ubuntu".to_string()),
            QosTier::High,
        );
        mgr.add_rule(rule).unwrap();

        let tier = mgr.auto_classify("task1", "https://example.com/fedora.iso", "Fedora");
        assert_eq!(tier, None);
    }

    #[test]
    fn test_manager_auto_classify_disabled() {
        let mut mgr = BandwidthQosManager::new();
        mgr.set_config(BandwidthQosConfig {
            auto_classify: false,
            ..Default::default()
        });
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::High,
        );
        mgr.add_rule(rule).unwrap();

        let tier = mgr.auto_classify("task1", "https://example.com/test", "test");
        assert_eq!(tier, None);
    }

    #[test]
    fn test_manager_auto_classify_priority_order() {
        let mut mgr = BandwidthQosManager::new();

        let low_rule = QosAutoRule::new(
            "low".to_string(),
            "Low priority rule".to_string(),
            QosPattern::UrlContains("example".to_string()),
            QosTier::Low,
        )
        .with_priority(1);

        let high_rule = QosAutoRule::new(
            "high".to_string(),
            "High priority rule".to_string(),
            QosPattern::UrlContains("example".to_string()),
            QosTier::Critical,
        )
        .with_priority(10);

        mgr.add_rule(low_rule).unwrap();
        mgr.add_rule(high_rule).unwrap();

        // Higher priority rule should match first
        let tier = mgr.auto_classify("task1", "https://example.com/file", "file");
        assert_eq!(tier, Some(QosTier::Critical));
    }

    #[test]
    fn test_manager_add_remove_rule() {
        let mut mgr = BandwidthQosManager::new();
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::Normal,
        );
        mgr.add_rule(rule).unwrap();
        assert_eq!(mgr.list_rules().len(), 1);

        let removed = mgr.remove_rule("r1").unwrap();
        assert_eq!(removed.id, "r1");
        assert_eq!(mgr.list_rules().len(), 0);
    }

    #[test]
    fn test_manager_add_duplicate_rule() {
        let mut mgr = BandwidthQosManager::new();
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::Normal,
        );
        mgr.add_rule(rule.clone()).unwrap();
        assert!(mgr.add_rule(rule).is_err());
    }

    #[test]
    fn test_manager_max_rules() {
        let mut mgr = BandwidthQosManager::new();
        mgr.set_config(BandwidthQosConfig {
            max_rules: 2,
            ..Default::default()
        });

        let r1 = QosAutoRule::new(
            "r1".to_string(),
            "R1".to_string(),
            QosPattern::UrlContains("a".to_string()),
            QosTier::Low,
        );
        let r2 = QosAutoRule::new(
            "r2".to_string(),
            "R2".to_string(),
            QosPattern::UrlContains("b".to_string()),
            QosTier::Low,
        );
        let r3 = QosAutoRule::new(
            "r3".to_string(),
            "R3".to_string(),
            QosPattern::UrlContains("c".to_string()),
            QosTier::Low,
        );

        mgr.add_rule(r1).unwrap();
        mgr.add_rule(r2).unwrap();
        assert!(mgr.add_rule(r3).is_err());
    }

    #[test]
    fn test_manager_remove_nonexistent_rule() {
        let mut mgr = BandwidthQosManager::new();
        assert!(mgr.remove_rule("nonexistent").is_err());
    }

    #[test]
    fn test_manager_enable_disable_rule() {
        let mut mgr = BandwidthQosManager::new();
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::High,
        );
        mgr.add_rule(rule).unwrap();
        mgr.set_rule_enabled("r1", false).unwrap();

        let r = mgr.get_rule("r1").unwrap();
        assert!(!r.enabled);

        // Disabled rule should not auto-classify
        let tier = mgr.auto_classify("task1", "https://example.com/test", "test");
        assert_eq!(tier, None);
    }

    #[test]
    fn test_manager_set_rule_priority() {
        let mut mgr = BandwidthQosManager::new();
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::Normal,
        );
        mgr.add_rule(rule).unwrap();
        mgr.set_rule_priority("r1", 42).unwrap();
        assert_eq!(mgr.get_rule("r1").unwrap().priority, 42);
    }

    #[test]
    fn test_manager_summary() {
        let mut mgr = BandwidthQosManager::new();
        mgr.assign_tier("t1", QosTier::Critical).unwrap();
        mgr.assign_tier("t2", QosTier::Critical).unwrap();
        mgr.assign_tier("t3", QosTier::Low).unwrap();

        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::High,
        );
        mgr.add_rule(rule).unwrap();

        let summary = mgr.summary();
        assert!(summary.enabled);
        assert_eq!(summary.total_tasks, 3);
        assert_eq!(summary.rule_count, 1);
        assert_eq!(summary.enabled_rule_count, 1);

        let critical_key = "critical";
        let critical_stats = summary.tier_stats.get(critical_key).unwrap();
        assert_eq!(critical_stats.task_count, 2);
        assert!(critical_stats.bandwidth_share > 0.0);
    }

    #[test]
    fn test_manager_clear_assignments() {
        let mut mgr = BandwidthQosManager::new();
        mgr.assign_tier("t1", QosTier::High).unwrap();
        mgr.assign_tier("t2", QosTier::Low).unwrap();
        assert_eq!(mgr.list_assignments().len(), 2);

        mgr.clear_assignments();
        assert_eq!(mgr.list_assignments().len(), 0);
    }

    #[test]
    fn test_manager_clear_rules() {
        let mut mgr = BandwidthQosManager::new();
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test".to_string(),
            QosPattern::UrlContains("test".to_string()),
            QosTier::Normal,
        );
        mgr.add_rule(rule).unwrap();
        mgr.clear_rules();
        assert_eq!(mgr.list_rules().len(), 0);
    }

    #[test]
    fn test_manager_persistence_roundtrip() {
        let mut mgr = BandwidthQosManager::new();
        mgr.assign_tier("t1", QosTier::Critical).unwrap();
        let rule = QosAutoRule::new(
            "r1".to_string(),
            "Test rule".to_string(),
            QosPattern::Domain("example.com".to_string()),
            QosTier::High,
        );
        mgr.add_rule(rule).unwrap();

        let data = mgr.to_data();
        let json = serde_json::to_string(&data).unwrap();
        let loaded: BandwidthQosData = serde_json::from_str(&json).unwrap();

        assert!(loaded.config.enabled);
        assert_eq!(loaded.rules.len(), 1);
        assert_eq!(loaded.assignments.len(), 1);
        assert_eq!(loaded.assignments["t1"].tier, QosTier::Critical);
    }

    #[test]
    fn test_manager_format_summary() {
        let mut mgr = BandwidthQosManager::new();
        mgr.assign_tier("t1", QosTier::Critical).unwrap();
        let report = mgr.format_summary();
        assert!(report.contains("Bandwidth QoS"));
        assert!(report.contains("Critical"));
    }

    #[test]
    fn test_qos_tier_all_in_order() {
        let tiers = QosTier::all_in_order();
        assert_eq!(tiers.len(), 5);
        assert_eq!(tiers[0], QosTier::Critical);
        assert_eq!(tiers[4], QosTier::Background);
    }

    #[test]
    fn test_file_extension_with_query_params() {
        let pattern = QosPattern::FileExtension(".tar.gz".to_string());
        assert!(pattern.matches_url("https://example.com/file.tar.gz?download=true"));
        assert!(!pattern.matches_url("https://example.com/file.zip?download=true"));
    }

    #[test]
    fn test_list_rules_sorted_by_priority() {
        let mut mgr = BandwidthQosManager::new();
        let r1 = QosAutoRule::new(
            "r1".to_string(),
            "Low".to_string(),
            QosPattern::UrlContains("a".to_string()),
            QosTier::Low,
        )
        .with_priority(1);
        let r2 = QosAutoRule::new(
            "r2".to_string(),
            "High".to_string(),
            QosPattern::UrlContains("b".to_string()),
            QosTier::High,
        )
        .with_priority(10);
        let r3 = QosAutoRule::new(
            "r3".to_string(),
            "Mid".to_string(),
            QosPattern::UrlContains("c".to_string()),
            QosTier::Normal,
        )
        .with_priority(5);

        mgr.add_rule(r1).unwrap();
        mgr.add_rule(r2).unwrap();
        mgr.add_rule(r3).unwrap();

        let rules = mgr.list_rules();
        assert_eq!(rules[0].id, "r2"); // priority 10
        assert_eq!(rules[1].id, "r3"); // priority 5
        assert_eq!(rules[2].id, "r1"); // priority 1
    }
}
