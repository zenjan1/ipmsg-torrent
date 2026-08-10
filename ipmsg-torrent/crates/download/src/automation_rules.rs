//! Download Automation Rules Engine (IFTTT-style)
//!
//! Unified system for automating download workflows with trigger → condition → action rules.
//! Example: "WHEN download completes AND size > 1GB THEN move to /media AND tag as 'video'"
//!
//! ## Architecture
//! - **Triggers**: Events that fire rule evaluation (download complete, fail, added, etc.)
//! - **Conditions**: Predicates that must all match for the rule to fire (size, URL pattern, tags, etc.)
//! - **Actions**: Operations to perform when a rule fires (tag, move, pause, notify, etc.)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

// ─── Trigger ─────────────────────────────────────────────────────────────────

/// Events that can trigger rule evaluation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleTrigger {
    /// Download completed successfully
    OnComplete,
    /// Download failed
    OnFail,
    /// New download added to queue
    OnAdded,
    /// Download paused
    OnPaused,
    /// Download resumed/started
    OnStarted,
    /// Download progress reaches a percentage threshold
    OnProgressReached,
}

impl RuleTrigger {
    pub fn label(&self) -> &'static str {
        match self {
            Self::OnComplete => "download completes",
            Self::OnFail => "download fails",
            Self::OnAdded => "download is added",
            Self::OnPaused => "download is paused",
            Self::OnStarted => "download starts",
            Self::OnProgressReached => "progress reaches threshold",
        }
    }
}

// ─── Conditions ──────────────────────────────────────────────────────────────

/// A condition that must be satisfied for a rule to fire
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RuleCondition {
    /// File size in bytes >= min
    MinSize { min_bytes: u64 },
    /// File size in bytes <= max
    MaxSize { max_bytes: u64 },
    /// File size between min and max (inclusive)
    SizeBetween { min_bytes: u64, max_bytes: u64 },
    /// URL contains substring (case-insensitive)
    UrlContains { substring: String },
    /// URL matches glob pattern (* and ?)
    UrlMatches { pattern: String },
    /// Task name contains substring (case-insensitive)
    NameContains { substring: String },
    /// Task has ALL of these tags
    HasAllTags { tags: Vec<String> },
    /// Task has ANY of these tags
    HasAnyTag { tags: Vec<String> },
    /// Task is in a specific group
    InGroup { group: String },
    /// Task uses a specific protocol
    ProtocolIs { protocol: String },
    /// Download speed below threshold (bytes/sec)
    SpeedBelow { bps: u64 },
    /// Download speed above threshold (bytes/sec)
    SpeedAbove { bps: u64 },
    /// Task priority is at least this level
    MinPriority { priority: i32 },
    /// Progress >= percentage (0-100)
    ProgressAtLeast { percent: f64 },
    /// Task has been in queue for at least N seconds
    QueuedForAtLeast { seconds: u64 },
    /// Task has mirrors configured
    HasMirrors,
    /// Task has a checksum configured
    HasChecksum,
    /// Task has a deadline configured
    HasDeadline,
    /// Task has error (is in Error state)
    HasError,
}

impl RuleCondition {
    pub fn describe(&self) -> String {
        match self {
            Self::MinSize { min_bytes } => format!("size >= {}", format_bytes(*min_bytes)),
            Self::MaxSize { max_bytes } => format!("size <= {}", format_bytes(*max_bytes)),
            Self::SizeBetween {
                min_bytes,
                max_bytes,
            } => {
                format!(
                    "size between {} and {}",
                    format_bytes(*min_bytes),
                    format_bytes(*max_bytes)
                )
            }
            Self::UrlContains { substring } => format!("URL contains \"{}\"", substring),
            Self::UrlMatches { pattern } => format!("URL matches \"{}\"", pattern),
            Self::NameContains { substring } => format!("name contains \"{}\"", substring),
            Self::HasAllTags { tags } => format!("has all tags: [{}]", tags.join(", ")),
            Self::HasAnyTag { tags } => format!("has any tag: [{}]", tags.join(", ")),
            Self::InGroup { group } => format!("in group \"{}\"", group),
            Self::ProtocolIs { protocol } => format!("protocol is {}", protocol),
            Self::SpeedBelow { bps } => format!("speed < {}/s", format_bytes(*bps)),
            Self::SpeedAbove { bps } => format!("speed > {}/s", format_bytes(*bps)),
            Self::MinPriority { priority } => format!("priority >= {}", priority),
            Self::ProgressAtLeast { percent } => format!("progress >= {}%", percent),
            Self::QueuedForAtLeast { seconds } => format!("queued for >= {}s", seconds),
            Self::HasMirrors => "has mirrors".to_string(),
            Self::HasChecksum => "has checksum".to_string(),
            Self::HasDeadline => "has deadline".to_string(),
            Self::HasError => "has error".to_string(),
        }
    }
}

// ─── Actions ─────────────────────────────────────────────────────────────────

/// An action to perform when a rule fires
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RuleAction {
    /// Add tags to the task
    AddTags { tags: Vec<String> },
    /// Remove tags from the task
    RemoveTags { tags: Vec<String> },
    /// Set the task group
    SetGroup { group: String },
    /// Set task priority (0=Low, 1=Normal, 2=High)
    SetPriority { priority: i32 },
    /// Set per-task speed limit (bytes/sec, 0 = unlimited)
    SetSpeedLimit { bps: u64 },
    /// Set bandwidth weight (1-10)
    SetBandwidthWeight { weight: u32 },
    /// Pause the download
    Pause,
    /// Resume / queue the download
    Resume,
    /// Remove the task from queue
    Remove,
    /// Move file to a directory (after completion)
    MoveTo { target_dir: PathBuf },
    /// Set save path (before download starts)
    SetSavePath { path: PathBuf },
    /// Add a mirror URL
    AddMirror { url: String },
    /// Set a deadline offset in seconds from now
    SetDeadline { offset_secs: u64 },
    /// Set max retries
    SetMaxRetries { retries: u32 },
    /// Clone the task
    CloneTask,
    /// Archive the task
    Archive,
    /// Send a notification (log entry)
    Notify { message: String },
}

impl RuleAction {
    pub fn describe(&self) -> String {
        match self {
            Self::AddTags { tags } => format!("add tags: [{}]", tags.join(", ")),
            Self::RemoveTags { tags } => format!("remove tags: [{}]", tags.join(", ")),
            Self::SetGroup { group } => format!("set group: \"{}\"", group),
            Self::SetPriority { priority } => format!("set priority: {}", priority),
            Self::SetSpeedLimit { bps } => format!("set speed limit: {}/s", format_bytes(*bps)),
            Self::SetBandwidthWeight { weight } => format!("set bandwidth weight: {}", weight),
            Self::Pause => "pause download".to_string(),
            Self::Resume => "resume download".to_string(),
            Self::Remove => "remove from queue".to_string(),
            Self::MoveTo { target_dir } => format!("move to: {}", target_dir.display()),
            Self::SetSavePath { path } => format!("set save path: {}", path.display()),
            Self::AddMirror { url } => format!("add mirror: {}", url),
            Self::SetDeadline { offset_secs } => format!("set deadline: {}s from now", offset_secs),
            Self::SetMaxRetries { retries } => format!("set max retries: {}", retries),
            Self::CloneTask => "clone task".to_string(),
            Self::Archive => "archive task".to_string(),
            Self::Notify { message } => format!("notify: {}", message),
        }
    }
}

// ─── Rule ────────────────────────────────────────────────────────────────────

/// A single automation rule: trigger + conditions → actions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationRule {
    /// Unique rule ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// When to evaluate this rule
    pub trigger: RuleTrigger,
    /// All conditions must match (AND logic)
    pub conditions: Vec<RuleCondition>,
    /// Actions to execute when all conditions match
    pub actions: Vec<RuleAction>,
    /// Whether this rule is enabled
    pub enabled: bool,
    /// Priority for ordering (higher = evaluated first)
    pub priority: i32,
    /// Maximum times this rule can fire (0 = unlimited)
    pub max_fires: u32,
    /// Number of times this rule has fired
    pub fire_count: u32,
    /// Optional: only apply to tasks matching these tags (empty = all)
    pub tag_filter: Vec<String>,
    /// Optional: only apply to tasks in this group (None = all)
    pub group_filter: Option<String>,
    /// Unix timestamp of last fire (0 = never)
    pub last_fired_at: u64,
    /// Created at unix timestamp
    pub created_at: u64,
}

impl AutomationRule {
    pub fn new(name: String, trigger: RuleTrigger) -> Self {
        let now = now_unix();
        Self {
            id: generate_rule_id(),
            name,
            trigger,
            conditions: Vec::new(),
            actions: Vec::new(),
            enabled: true,
            priority: 0,
            max_fires: 0,
            fire_count: 0,
            tag_filter: Vec::new(),
            group_filter: None,
            last_fired_at: 0,
            created_at: now,
        }
    }

    /// Check if the rule has reached its max fire limit
    pub fn is_exhausted(&self) -> bool {
        self.max_fires > 0 && self.fire_count >= self.max_fires
    }

    /// Record a rule fire
    pub fn record_fire(&mut self) {
        self.fire_count += 1;
        self.last_fired_at = now_unix();
    }
}

// ─── Rule Evaluation Context ─────────────────────────────────────────────────

/// Context about a task for rule evaluation
#[derive(Debug, Clone)]
pub struct RuleEvalContext {
    pub task_id: String,
    pub name: String,
    pub url: String,
    pub size_bytes: u64,
    pub downloaded_bytes: u64,
    pub state: String,
    pub tags: Vec<String>,
    pub group: Option<String>,
    pub priority: i32,
    pub speed_bps: u64,
    pub protocol: String,
    pub has_mirrors: bool,
    pub has_checksum: bool,
    pub has_deadline: bool,
    pub queued_since: Option<u64>,
    pub save_path: Option<String>,
}

impl RuleEvalContext {
    /// Evaluate a single condition against this context
    pub fn matches_condition(&self, condition: &RuleCondition) -> bool {
        match condition {
            RuleCondition::MinSize { min_bytes } => self.size_bytes >= *min_bytes,
            RuleCondition::MaxSize { max_bytes } => self.size_bytes <= *max_bytes,
            RuleCondition::SizeBetween {
                min_bytes,
                max_bytes,
            } => self.size_bytes >= *min_bytes && self.size_bytes <= *max_bytes,
            RuleCondition::UrlContains { substring } => {
                self.url.to_lowercase().contains(&substring.to_lowercase())
            }
            RuleCondition::UrlMatches { pattern } => wildcard_match(pattern, &self.url),
            RuleCondition::NameContains { substring } => {
                self.name.to_lowercase().contains(&substring.to_lowercase())
            }
            RuleCondition::HasAllTags { tags } => tags.iter().all(|t| self.tags.contains(t)),
            RuleCondition::HasAnyTag { tags } => tags.iter().any(|t| self.tags.contains(t)),
            RuleCondition::InGroup { group } => self.group.as_deref() == Some(group.as_str()),
            RuleCondition::ProtocolIs { protocol } => self.protocol.eq_ignore_ascii_case(protocol),
            RuleCondition::SpeedBelow { bps } => self.speed_bps < *bps,
            RuleCondition::SpeedAbove { bps } => self.speed_bps > *bps,
            RuleCondition::MinPriority { priority } => self.priority >= *priority,
            RuleCondition::ProgressAtLeast { percent } => {
                if self.size_bytes == 0 {
                    false
                } else {
                    let progress = (self.downloaded_bytes as f64 / self.size_bytes as f64) * 100.0;
                    progress >= *percent
                }
            }
            RuleCondition::QueuedForAtLeast { seconds } => {
                if let Some(queued_since) = self.queued_since {
                    let now = now_unix();
                    now.saturating_sub(queued_since) >= *seconds
                } else {
                    false
                }
            }
            RuleCondition::HasMirrors => self.has_mirrors,
            RuleCondition::HasChecksum => self.has_checksum,
            RuleCondition::HasDeadline => self.has_deadline,
            RuleCondition::HasError => self.state == "Error",
        }
    }

    /// Check if all conditions in a list match
    pub fn matches_all(&self, conditions: &[RuleCondition]) -> bool {
        conditions.iter().all(|c| self.matches_condition(c))
    }
}

// ─── Rule Action Result ──────────────────────────────────────────────────────

/// Result of executing a single action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
    pub action_index: usize,
    pub action_description: String,
    pub success: bool,
    pub message: String,
}

/// Result of firing a rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleFireResult {
    pub rule_id: String,
    pub rule_name: String,
    pub task_id: String,
    pub action_results: Vec<ActionResult>,
    pub fired_at: u64,
}

// ─── Rule Manager ────────────────────────────────────────────────────────────

/// Configuration for the automation rules engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationConfig {
    /// Enable/disable the entire automation engine
    pub enabled: bool,
    /// Maximum number of rules allowed
    pub max_rules: usize,
    /// Maximum number of fire history entries to keep
    pub max_history: usize,
    /// Log all rule evaluations (even non-fires) for debugging
    pub verbose_logging: bool,
}

impl Default for AutomationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_rules: 200,
            max_history: 500,
            verbose_logging: false,
        }
    }
}

/// Summary of the automation rules engine state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationSummary {
    pub enabled: bool,
    pub total_rules: usize,
    pub enabled_rules: usize,
    pub total_fires: u32,
    pub rules_by_trigger: HashMap<String, usize>,
    pub recent_fires: Vec<RuleFireResult>,
}

/// Manages automation rules: CRUD, evaluation, and execution tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationRuleManager {
    /// All automation rules
    rules: Vec<AutomationRule>,
    /// Engine configuration
    config: AutomationConfig,
    /// Fire history (most recent first)
    fire_history: Vec<RuleFireResult>,
}

impl AutomationRuleManager {
    /// Create a new empty manager with default config
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            config: AutomationConfig::default(),
            fire_history: Vec::new(),
        }
    }

    /// Create with a specific data directory (loads persisted config)
    pub fn new_with_dir(data_dir: &std::path::Path) -> Self {
        let mut mgr = Self::new();
        if let Ok(loaded) = load_config(data_dir) {
            mgr.config = loaded;
        }
        if let Ok(rules) = load_rules(data_dir) {
            mgr.rules = rules;
        }
        mgr
    }

    // ─── Config ──────────────────────────────────────────────────────────

    pub fn get_config(&self) -> &AutomationConfig {
        &self.config
    }

    pub fn set_config(&mut self, config: AutomationConfig) {
        self.config = config;
    }

    // ─── Rule CRUD ───────────────────────────────────────────────────────

    /// Add a new rule. Returns the rule ID or error.
    pub fn add_rule(&mut self, mut rule: AutomationRule) -> Result<String, String> {
        if self.rules.len() >= self.config.max_rules {
            return Err(format!(
                "max rules limit reached ({})",
                self.config.max_rules
            ));
        }
        if rule.name.trim().is_empty() {
            return Err("rule name cannot be empty".to_string());
        }
        if rule.actions.is_empty() {
            return Err("rule must have at least one action".to_string());
        }
        // Assign a unique ID
        rule.id = generate_rule_id();
        self.rules.push(rule.clone());
        Ok(rule.id)
    }

    /// Remove a rule by ID. Returns true if found and removed.
    pub fn remove_rule(&mut self, rule_id: &str) -> bool {
        let before = self.rules.len();
        self.rules.retain(|r| r.id != rule_id);
        self.rules.len() < before
    }

    /// Get a rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&AutomationRule> {
        self.rules.iter().find(|r| r.id == rule_id)
    }

    /// Get a mutable rule by ID
    pub fn get_rule_mut(&mut self, rule_id: &str) -> Option<&mut AutomationRule> {
        self.rules.iter_mut().find(|r| r.id == rule_id)
    }

    /// List all rules
    pub fn list_rules(&self) -> &[AutomationRule] {
        &self.rules
    }

    /// Enable/disable a rule
    pub fn set_rule_enabled(&mut self, rule_id: &str, enabled: bool) -> bool {
        if let Some(rule) = self.rules.iter_mut().find(|r| r.id == rule_id) {
            rule.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Update a rule entirely
    pub fn update_rule(&mut self, rule: AutomationRule) -> bool {
        if let Some(existing) = self.rules.iter_mut().find(|r| r.id == rule.id) {
            *existing = rule;
            true
        } else {
            false
        }
    }

    // ─── Evaluation ──────────────────────────────────────────────────────

    /// Evaluate all rules for a given trigger and context.
    /// Returns the list of rule fire results for rules that matched.
    pub fn evaluate(
        &mut self,
        trigger: RuleTrigger,
        context: &RuleEvalContext,
    ) -> Vec<RuleFireResult> {
        if !self.config.enabled {
            return Vec::new();
        }

        let mut results = Vec::new();

        // Sort rules by priority (higher first) for deterministic evaluation
        let mut indices: Vec<usize> = (0..self.rules.len()).collect();
        indices.sort_by(|a, b| self.rules[*b].priority.cmp(&self.rules[*a].priority));

        for idx in indices {
            let rule = &self.rules[idx];

            // Skip disabled, wrong trigger, or exhausted rules
            if !rule.enabled {
                continue;
            }
            if rule.trigger != trigger {
                continue;
            }
            if rule.is_exhausted() {
                continue;
            }

            // Check tag filter
            if !rule.tag_filter.is_empty()
                && !rule.tag_filter.iter().any(|t| context.tags.contains(t))
            {
                continue;
            }

            // Check group filter
            if rule
                .group_filter
                .as_ref()
                .is_some_and(|gf| context.group.as_deref() != Some(gf.as_str()))
            {
                continue;
            }

            // Check all conditions
            if !context.matches_all(&rule.conditions) {
                continue;
            }

            // Rule fires! Build action results
            let action_results: Vec<ActionResult> = rule
                .actions
                .iter()
                .enumerate()
                .map(|(i, action)| ActionResult {
                    action_index: i,
                    action_description: action.describe(),
                    success: true,
                    message: format!("Action queued: {}", action.describe()),
                })
                .collect();

            let fire_result = RuleFireResult {
                rule_id: rule.id.clone(),
                rule_name: rule.name.clone(),
                task_id: context.task_id.clone(),
                action_results,
                fired_at: now_unix(),
            };

            // Record the fire
            self.rules[idx].record_fire();
            self.fire_history.insert(0, fire_result.clone());

            // Trim history
            if self.fire_history.len() > self.config.max_history {
                self.fire_history.truncate(self.config.max_history);
            }

            results.push(fire_result);
        }

        results
    }

    /// Get the actions that should be executed for a fired rule.
    /// This is called after evaluate() returns results.
    pub fn get_actions_for_rule(&self, rule_id: &str) -> Vec<RuleAction> {
        self.rules
            .iter()
            .find(|r| r.id == rule_id)
            .map(|r| r.actions.clone())
            .unwrap_or_default()
    }

    // ─── Summary ─────────────────────────────────────────────────────────

    /// Get a summary of the automation engine state
    pub fn summary(&self) -> AutomationSummary {
        let total_rules = self.rules.len();
        let enabled_rules = self.rules.iter().filter(|r| r.enabled).count();
        let total_fires: u32 = self.rules.iter().map(|r| r.fire_count).sum();

        let mut rules_by_trigger: HashMap<String, usize> = HashMap::new();
        for rule in &self.rules {
            let key = format!("{:?}", rule.trigger);
            *rules_by_trigger.entry(key).or_insert(0) += 1;
        }

        let recent_fires = self.fire_history.iter().take(20).cloned().collect();

        AutomationSummary {
            enabled: self.config.enabled,
            total_rules,
            enabled_rules,
            total_fires,
            rules_by_trigger,
            recent_fires,
        }
    }

    /// Clear fire history
    pub fn clear_history(&mut self) {
        self.fire_history.clear();
    }

    /// Reset all fire counts
    pub fn reset_fire_counts(&mut self) {
        for rule in &mut self.rules {
            rule.fire_count = 0;
            rule.last_fired_at = 0;
        }
    }

    /// Get fire history
    pub fn fire_history(&self) -> &[RuleFireResult] {
        &self.fire_history
    }

    // ─── Persistence ─────────────────────────────────────────────────────

    /// Save rules to disk
    pub fn save(&self, data_dir: &std::path::Path) -> Result<(), String> {
        save_rules(data_dir, &self.rules).map_err(|e| e.to_string())?;
        save_config(data_dir, &self.config).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Load rules from disk
    pub fn load(&mut self, data_dir: &std::path::Path) {
        if let Ok(rules) = load_rules(data_dir) {
            self.rules = rules;
        }
        if let Ok(config) = load_config(data_dir) {
            self.config = config;
        }
    }
}

impl Default for AutomationRuleManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Wildcard matching ───────────────────────────────────────────────────────

/// Simple glob matching supporting * (any chars) and ? (single char)
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern_lower = pattern.to_lowercase();
    let text_lower = text.to_lowercase();
    let pat: Vec<char> = pattern_lower.chars().collect();
    let txt: Vec<char> = text_lower.chars().collect();

    let mut dp = vec![vec![false; txt.len() + 1]; pat.len() + 1];
    dp[0][0] = true;

    for i in 1..=pat.len() {
        if pat[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=pat.len() {
        for j in 1..=txt.len() {
            if pat[i - 1] == '*' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if pat[i - 1] == '?' || pat[i - 1] == txt[j - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[pat.len()][txt.len()]
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_rule_id() -> String {
    let ts = now_unix();
    let rand: u32 = (ts.wrapping_mul(6364136223846793005).wrapping_add(1)) as u32;
    format!("rule_{:x}{:x}", ts & 0xFFFF_FFFF, rand & 0xFFFF)
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

// ─── Persistence ─────────────────────────────────────────────────────────────

fn save_rules(data_dir: &std::path::Path, rules: &[AutomationRule]) -> Result<(), std::io::Error> {
    let path = data_dir.join("automation_rules.json");
    let json = serde_json::to_string_pretty(rules)?;
    atomic_write(&path, json.as_bytes())
}

fn load_rules(data_dir: &std::path::Path) -> Result<Vec<AutomationRule>, std::io::Error> {
    let path = data_dir.join("automation_rules.json");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = std::fs::read_to_string(&path)?;
    let rules: Vec<AutomationRule> = serde_json::from_str(&data)?;
    Ok(rules)
}

fn save_config(
    data_dir: &std::path::Path,
    config: &AutomationConfig,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("automation_config.json");
    let json = serde_json::to_string_pretty(config)?;
    atomic_write(&path, json.as_bytes())
}

fn load_config(data_dir: &std::path::Path) -> Result<AutomationConfig, std::io::Error> {
    let path = data_dir.join("automation_config.json");
    if !path.exists() {
        return Ok(AutomationConfig::default());
    }
    let data = std::fs::read_to_string(&path)?;
    let config: AutomationConfig = serde_json::from_str(&data)?;
    Ok(config)
}

fn atomic_write(path: &std::path::Path, data: &[u8]) -> Result<(), std::io::Error> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_context(task_id: &str, name: &str, url: &str, size: u64) -> RuleEvalContext {
        RuleEvalContext {
            task_id: task_id.to_string(),
            name: name.to_string(),
            url: url.to_string(),
            size_bytes: size,
            downloaded_bytes: 0,
            state: "Complete".to_string(),
            tags: Vec::new(),
            group: None,
            priority: 1,
            speed_bps: 0,
            protocol: "http".to_string(),
            has_mirrors: false,
            has_checksum: false,
            has_deadline: false,
            queued_since: None,
            save_path: None,
        }
    }

    #[test]
    fn test_wildcard_match() {
        assert!(wildcard_match("*.mp4", "video.mp4"));
        assert!(wildcard_match("*.MP4", "video.mp4"));
        assert!(!wildcard_match("*.mp4", "video.avi"));
        assert!(wildcard_match("video_*", "video_2024.mp4"));
        assert!(wildcard_match("test?.txt", "test1.txt"));
        assert!(!wildcard_match("test?.txt", "test12.txt"));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("exact", "exact"));
        assert!(!wildcard_match("exact", "notexact"));
    }

    #[test]
    fn test_condition_size() {
        let ctx = make_context("t1", "file", "http://example.com/file.zip", 5_000_000);

        assert!(ctx.matches_condition(&RuleCondition::MinSize {
            min_bytes: 1_000_000
        }));
        assert!(!ctx.matches_condition(&RuleCondition::MinSize {
            min_bytes: 10_000_000
        }));
        assert!(ctx.matches_condition(&RuleCondition::MaxSize {
            max_bytes: 10_000_000
        }));
        assert!(ctx.matches_condition(&RuleCondition::SizeBetween {
            min_bytes: 1_000_000,
            max_bytes: 10_000_000
        }));
        assert!(!ctx.matches_condition(&RuleCondition::SizeBetween {
            min_bytes: 10_000_000,
            max_bytes: 20_000_000
        }));
    }

    #[test]
    fn test_condition_url() {
        let ctx = make_context("t1", "file", "https://example.com/videos/clip.mp4", 1000);

        assert!(ctx.matches_condition(&RuleCondition::UrlContains {
            substring: "example.com".to_string(),
        }));
        assert!(ctx.matches_condition(&RuleCondition::UrlContains {
            substring: "EXAMPLE".to_string(),
        }));
        assert!(!ctx.matches_condition(&RuleCondition::UrlContains {
            substring: "notfound".to_string(),
        }));
        assert!(ctx.matches_condition(&RuleCondition::UrlMatches {
            pattern: "*.mp4".to_string(),
        }));
        assert!(ctx.matches_condition(&RuleCondition::UrlMatches {
            pattern: "https://example.com/*".to_string(),
        }));
    }

    #[test]
    fn test_condition_name() {
        let ctx = make_context("t1", "My Big Video File", "http://x.com", 1000);

        assert!(ctx.matches_condition(&RuleCondition::NameContains {
            substring: "big video".to_string(),
        }));
        assert!(!ctx.matches_condition(&RuleCondition::NameContains {
            substring: "audio".to_string(),
        }));
    }

    #[test]
    fn test_condition_tags() {
        let mut ctx = make_context("t1", "file", "http://x.com", 1000);
        ctx.tags = vec!["video".to_string(), "hd".to_string()];

        assert!(ctx.matches_condition(&RuleCondition::HasAllTags {
            tags: vec!["video".to_string(), "hd".to_string()],
        }));
        assert!(!ctx.matches_condition(&RuleCondition::HasAllTags {
            tags: vec!["video".to_string(), "4k".to_string()],
        }));
        assert!(ctx.matches_condition(&RuleCondition::HasAnyTag {
            tags: vec!["audio".to_string(), "video".to_string()],
        }));
        assert!(!ctx.matches_condition(&RuleCondition::HasAnyTag {
            tags: vec!["audio".to_string(), "4k".to_string()],
        }));
    }

    #[test]
    fn test_condition_group_and_protocol() {
        let mut ctx = make_context("t1", "file", "http://x.com", 1000);
        ctx.group = Some("media".to_string());
        ctx.protocol = "torrent".to_string();

        assert!(ctx.matches_condition(&RuleCondition::InGroup {
            group: "media".to_string(),
        }));
        assert!(!ctx.matches_condition(&RuleCondition::InGroup {
            group: "work".to_string(),
        }));
        assert!(ctx.matches_condition(&RuleCondition::ProtocolIs {
            protocol: "torrent".to_string(),
        }));
        assert!(ctx.matches_condition(&RuleCondition::ProtocolIs {
            protocol: "TORRENT".to_string(),
        }));
    }

    #[test]
    fn test_condition_speed_and_priority() {
        let mut ctx = make_context("t1", "file", "http://x.com", 1000);
        ctx.speed_bps = 500_000;
        ctx.priority = 2;

        assert!(ctx.matches_condition(&RuleCondition::SpeedBelow { bps: 1_000_000 }));
        assert!(!ctx.matches_condition(&RuleCondition::SpeedBelow { bps: 100_000 }));
        assert!(ctx.matches_condition(&RuleCondition::SpeedAbove { bps: 100_000 }));
        assert!(ctx.matches_condition(&RuleCondition::MinPriority { priority: 2 }));
        assert!(ctx.matches_condition(&RuleCondition::MinPriority { priority: 1 }));
        assert!(!ctx.matches_condition(&RuleCondition::MinPriority { priority: 3 }));
    }

    #[test]
    fn test_condition_progress() {
        let mut ctx = make_context("t1", "file", "http://x.com", 1000);
        ctx.downloaded_bytes = 750;

        assert!(ctx.matches_condition(&RuleCondition::ProgressAtLeast { percent: 75.0 }));
        assert!(ctx.matches_condition(&RuleCondition::ProgressAtLeast { percent: 50.0 }));
        assert!(!ctx.matches_condition(&RuleCondition::ProgressAtLeast { percent: 80.0 }));
    }

    #[test]
    fn test_condition_boolean_flags() {
        let mut ctx = make_context("t1", "file", "http://x.com", 1000);

        assert!(!ctx.matches_condition(&RuleCondition::HasMirrors));
        ctx.has_mirrors = true;
        assert!(ctx.matches_condition(&RuleCondition::HasMirrors));

        assert!(!ctx.matches_condition(&RuleCondition::HasChecksum));
        ctx.has_checksum = true;
        assert!(ctx.matches_condition(&RuleCondition::HasChecksum));

        assert!(!ctx.matches_condition(&RuleCondition::HasDeadline));
        ctx.has_deadline = true;
        assert!(ctx.matches_condition(&RuleCondition::HasDeadline));

        assert!(!ctx.matches_condition(&RuleCondition::HasError));
        ctx.state = "Error".to_string();
        assert!(ctx.matches_condition(&RuleCondition::HasError));
    }

    #[test]
    fn test_condition_queued_duration() {
        let mut ctx = make_context("t1", "file", "http://x.com", 1000);
        assert!(!ctx.matches_condition(&RuleCondition::QueuedForAtLeast { seconds: 60 }));

        ctx.queued_since = Some(now_unix() - 120);
        assert!(ctx.matches_condition(&RuleCondition::QueuedForAtLeast { seconds: 60 }));
        assert!(!ctx.matches_condition(&RuleCondition::QueuedForAtLeast { seconds: 300 }));
    }

    #[test]
    fn test_matches_all() {
        let ctx = make_context("t1", "big video", "http://example.com/file.mp4", 5_000_000);

        let conditions = vec![
            RuleCondition::MinSize {
                min_bytes: 1_000_000,
            },
            RuleCondition::UrlContains {
                substring: "example".to_string(),
            },
            RuleCondition::NameContains {
                substring: "video".to_string(),
            },
        ];
        assert!(ctx.matches_all(&conditions));

        let with_fail = vec![
            RuleCondition::MinSize {
                min_bytes: 1_000_000,
            },
            RuleCondition::MaxSize {
                max_bytes: 1_000_000,
            }, // fails
        ];
        assert!(!ctx.matches_all(&with_fail));
    }

    #[test]
    fn test_rule_new() {
        let rule = AutomationRule::new("Test Rule".to_string(), RuleTrigger::OnComplete);
        assert_eq!(rule.name, "Test Rule");
        assert!(rule.enabled);
        assert_eq!(rule.trigger, RuleTrigger::OnComplete);
        assert!(rule.conditions.is_empty());
        assert!(rule.actions.is_empty());
        assert_eq!(rule.fire_count, 0);
        assert_eq!(rule.max_fires, 0);
        assert!(!rule.is_exhausted());
    }

    #[test]
    fn test_rule_exhausted() {
        let mut rule = AutomationRule::new("Test".to_string(), RuleTrigger::OnComplete);
        rule.max_fires = 2;
        assert!(!rule.is_exhausted());

        rule.record_fire();
        assert!(!rule.is_exhausted());

        rule.record_fire();
        assert!(rule.is_exhausted());
    }

    #[test]
    fn test_manager_add_remove() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Big files".to_string(), RuleTrigger::OnComplete);
        rule.conditions.push(RuleCondition::MinSize {
            min_bytes: 1_000_000,
        });
        rule.actions.push(RuleAction::AddTags {
            tags: vec!["big".to_string()],
        });

        let id = mgr.add_rule(rule).unwrap();
        assert!(!id.is_empty());
        assert_eq!(mgr.list_rules().len(), 1);

        assert!(mgr.remove_rule(&id));
        assert_eq!(mgr.list_rules().len(), 0);

        assert!(!mgr.remove_rule("nonexistent"));
    }

    #[test]
    fn test_manager_add_validation() {
        let mut mgr = AutomationRuleManager::new();

        // Empty name
        let mut rule = AutomationRule::new("".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        assert!(mgr.add_rule(rule).is_err());

        // No actions
        let rule = AutomationRule::new("Test".to_string(), RuleTrigger::OnComplete);
        assert!(mgr.add_rule(rule).is_err());
    }

    #[test]
    fn test_manager_max_rules() {
        let mut mgr = AutomationRuleManager::new();
        mgr.config.max_rules = 2;

        for i in 0..2 {
            let mut rule = AutomationRule::new(format!("Rule {}", i), RuleTrigger::OnComplete);
            rule.actions.push(RuleAction::Pause);
            mgr.add_rule(rule).unwrap();
        }

        let mut rule = AutomationRule::new("Overflow".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        assert!(mgr.add_rule(rule).is_err());
    }

    #[test]
    fn test_manager_evaluate_basic() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Tag big files".to_string(), RuleTrigger::OnComplete);
        rule.conditions.push(RuleCondition::MinSize {
            min_bytes: 1_000_000,
        });
        rule.actions.push(RuleAction::AddTags {
            tags: vec!["big".to_string()],
        });
        mgr.add_rule(rule).unwrap();

        let ctx = make_context("t1", "file", "http://x.com/f", 5_000_000);
        let results = mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rule_name, "Tag big files");
        assert_eq!(results[0].task_id, "t1");
        assert_eq!(results[0].action_results.len(), 1);
        assert!(results[0].action_results[0].success);
    }

    #[test]
    fn test_manager_evaluate_disabled() {
        let mut mgr = AutomationRuleManager::new();
        mgr.config.enabled = false;

        let mut rule = AutomationRule::new("Test".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        mgr.add_rule(rule).unwrap();

        let ctx = make_context("t1", "file", "http://x.com", 1000);
        let results = mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_manager_evaluate_wrong_trigger() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Test".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        mgr.add_rule(rule).unwrap();

        let ctx = make_context("t1", "file", "http://x.com", 1000);
        let results = mgr.evaluate(RuleTrigger::OnFail, &ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_manager_evaluate_condition_mismatch() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Big only".to_string(), RuleTrigger::OnComplete);
        rule.conditions.push(RuleCondition::MinSize {
            min_bytes: 10_000_000,
        });
        rule.actions.push(RuleAction::Pause);
        mgr.add_rule(rule).unwrap();

        let ctx = make_context("t1", "file", "http://x.com", 1000);
        let results = mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_manager_evaluate_tag_filter() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Video rule".to_string(), RuleTrigger::OnComplete);
        rule.tag_filter = vec!["video".to_string()];
        rule.actions.push(RuleAction::Pause);
        mgr.add_rule(rule).unwrap();

        // Without matching tag
        let ctx = make_context("t1", "file", "http://x.com", 1000);
        assert!(mgr.evaluate(RuleTrigger::OnComplete, &ctx).is_empty());

        // With matching tag
        let mut ctx2 = make_context("t2", "file", "http://x.com", 1000);
        ctx2.tags = vec!["video".to_string()];
        assert_eq!(mgr.evaluate(RuleTrigger::OnComplete, &ctx2).len(), 1);
    }

    #[test]
    fn test_manager_evaluate_group_filter() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Media rule".to_string(), RuleTrigger::OnComplete);
        rule.group_filter = Some("media".to_string());
        rule.actions.push(RuleAction::Pause);
        mgr.add_rule(rule).unwrap();

        let ctx = make_context("t1", "file", "http://x.com", 1000);
        assert!(mgr.evaluate(RuleTrigger::OnComplete, &ctx).is_empty());

        let mut ctx2 = make_context("t2", "file", "http://x.com", 1000);
        ctx2.group = Some("media".to_string());
        assert_eq!(mgr.evaluate(RuleTrigger::OnComplete, &ctx2).len(), 1);
    }

    #[test]
    fn test_manager_evaluate_max_fires() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Once only".to_string(), RuleTrigger::OnComplete);
        rule.max_fires = 1;
        rule.actions.push(RuleAction::Pause);
        mgr.add_rule(rule).unwrap();

        let ctx = make_context("t1", "file", "http://x.com", 1000);

        // First fire works
        let results = mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        assert_eq!(results.len(), 1);

        // Second fire is blocked
        let results = mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_manager_evaluate_priority_ordering() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule_low = AutomationRule::new("Low priority".to_string(), RuleTrigger::OnComplete);
        rule_low.priority = 1;
        rule_low.actions.push(RuleAction::AddTags {
            tags: vec!["low".to_string()],
        });
        mgr.add_rule(rule_low).unwrap();

        let mut rule_high =
            AutomationRule::new("High priority".to_string(), RuleTrigger::OnComplete);
        rule_high.priority = 10;
        rule_high.actions.push(RuleAction::AddTags {
            tags: vec!["high".to_string()],
        });
        mgr.add_rule(rule_high).unwrap();

        let ctx = make_context("t1", "file", "http://x.com", 1000);
        let results = mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        assert_eq!(results.len(), 2);
        // High priority should fire first
        assert_eq!(results[0].rule_name, "High priority");
        assert_eq!(results[1].rule_name, "Low priority");
    }

    #[test]
    fn test_manager_evaluate_disabled_rule() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Test".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        let id = mgr.add_rule(rule).unwrap();
        mgr.set_rule_enabled(&id, false);

        let ctx = make_context("t1", "file", "http://x.com", 1000);
        assert!(mgr.evaluate(RuleTrigger::OnComplete, &ctx).is_empty());
    }

    #[test]
    fn test_manager_summary() {
        let mut mgr = AutomationRuleManager::new();

        let mut r1 = AutomationRule::new("R1".to_string(), RuleTrigger::OnComplete);
        r1.actions.push(RuleAction::Pause);
        mgr.add_rule(r1).unwrap();

        let mut r2 = AutomationRule::new("R2".to_string(), RuleTrigger::OnFail);
        r2.actions.push(RuleAction::Resume);
        let id2 = mgr.add_rule(r2).unwrap();
        mgr.set_rule_enabled(&id2, false);

        let summary = mgr.summary();
        assert!(summary.enabled);
        assert_eq!(summary.total_rules, 2);
        assert_eq!(summary.enabled_rules, 1);
        assert_eq!(summary.total_fires, 0);
    }

    #[test]
    fn test_manager_fire_history() {
        let mut mgr = AutomationRuleManager::new();
        mgr.config.max_history = 3;

        let mut rule = AutomationRule::new("Test".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        mgr.add_rule(rule).unwrap();

        for i in 0..5 {
            let ctx = make_context(&format!("t{}", i), "file", "http://x.com", 1000);
            mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        }

        // History should be trimmed to max_history
        assert_eq!(mgr.fire_history().len(), 3);
    }

    #[test]
    fn test_manager_clear_history_and_counts() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Test".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        mgr.add_rule(rule).unwrap();

        let ctx = make_context("t1", "file", "http://x.com", 1000);
        mgr.evaluate(RuleTrigger::OnComplete, &ctx);

        assert_eq!(mgr.fire_history().len(), 1);
        assert_eq!(mgr.list_rules()[0].fire_count, 1);

        mgr.clear_history();
        assert!(mgr.fire_history().is_empty());

        mgr.reset_fire_counts();
        assert_eq!(mgr.list_rules()[0].fire_count, 0);
    }

    #[test]
    fn test_manager_persistence() {
        let dir = std::env::temp_dir().join("automation_test_persist");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Save
        {
            let mut mgr = AutomationRuleManager::new();
            mgr.config.enabled = true;
            mgr.config.max_rules = 50;

            let mut rule = AutomationRule::new("Persist test".to_string(), RuleTrigger::OnComplete);
            rule.conditions
                .push(RuleCondition::MinSize { min_bytes: 1000 });
            rule.actions.push(RuleAction::AddTags {
                tags: vec!["test".to_string()],
            });
            mgr.add_rule(rule).unwrap();

            mgr.save(&dir).unwrap();
        }

        // Load
        {
            let mut mgr = AutomationRuleManager::new();
            mgr.load(&dir);
            assert_eq!(mgr.config.max_rules, 50);
            assert_eq!(mgr.list_rules().len(), 1);
            assert_eq!(mgr.list_rules()[0].name, "Persist test");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500B");
        assert_eq!(format_bytes(1024), "1.0KB");
        assert_eq!(format_bytes(1_500_000), "1.4MB");
        assert_eq!(format_bytes(2_000_000_000), "1.9GB");
    }

    #[test]
    fn test_rule_action_describe() {
        assert_eq!(RuleAction::Pause.describe(), "pause download");
        assert_eq!(RuleAction::Resume.describe(), "resume download");
        assert_eq!(RuleAction::Remove.describe(), "remove from queue");
        assert_eq!(RuleAction::CloneTask.describe(), "clone task");
        assert_eq!(RuleAction::Archive.describe(), "archive task");
        assert!(
            RuleAction::AddTags {
                tags: vec!["a".to_string()]
            }
            .describe()
            .contains("a")
        );
        assert!(
            RuleAction::MoveTo {
                target_dir: PathBuf::from("/tmp")
            }
            .describe()
            .contains("/tmp")
        );
        assert!(
            RuleAction::Notify {
                message: "hello".to_string()
            }
            .describe()
            .contains("hello")
        );
    }

    #[test]
    fn test_rule_condition_describe() {
        assert!(
            RuleCondition::MinSize {
                min_bytes: 1_000_000
            }
            .describe()
            .contains("size")
        );
        assert!(
            RuleCondition::UrlContains {
                substring: "test".to_string()
            }
            .describe()
            .contains("test")
        );
        assert!(RuleCondition::HasMirrors.describe() == "has mirrors");
        assert!(RuleCondition::HasError.describe() == "has error");
    }

    #[test]
    fn test_trigger_label() {
        assert_eq!(RuleTrigger::OnComplete.label(), "download completes");
        assert_eq!(RuleTrigger::OnFail.label(), "download fails");
        assert_eq!(RuleTrigger::OnAdded.label(), "download is added");
    }

    #[test]
    fn test_get_actions_for_rule() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Test".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        rule.actions.push(RuleAction::AddTags {
            tags: vec!["x".to_string()],
        });
        let id = mgr.add_rule(rule).unwrap();

        let actions = mgr.get_actions_for_rule(&id);
        assert_eq!(actions.len(), 2);

        let empty = mgr.get_actions_for_rule("nonexistent");
        assert!(empty.is_empty());
    }

    #[test]
    fn test_update_rule() {
        let mut mgr = AutomationRuleManager::new();

        let mut rule = AutomationRule::new("Original".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Pause);
        let id = mgr.add_rule(rule).unwrap();

        let mut updated = mgr.get_rule(&id).unwrap().clone();
        updated.name = "Updated".to_string();
        assert!(mgr.update_rule(updated));

        assert_eq!(mgr.get_rule(&id).unwrap().name, "Updated");
    }

    #[test]
    fn test_evaluate_multiple_rules() {
        let mut mgr = AutomationRuleManager::new();

        // Rule 1: tag big files
        let mut r1 = AutomationRule::new("Big files".to_string(), RuleTrigger::OnComplete);
        r1.conditions.push(RuleCondition::MinSize {
            min_bytes: 1_000_000,
        });
        r1.actions.push(RuleAction::AddTags {
            tags: vec!["big".to_string()],
        });
        mgr.add_rule(r1).unwrap();

        // Rule 2: notify on mp4
        let mut r2 = AutomationRule::new("MP4 notify".to_string(), RuleTrigger::OnComplete);
        r2.conditions.push(RuleCondition::UrlMatches {
            pattern: "*.mp4".to_string(),
        });
        r2.actions.push(RuleAction::Notify {
            message: "Video downloaded!".to_string(),
        });
        mgr.add_rule(r2).unwrap();

        // Both should fire for a big mp4
        let ctx = make_context("t1", "video", "http://x.com/video.mp4", 5_000_000);
        let results = mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_evaluate_empty_conditions() {
        let mut mgr = AutomationRuleManager::new();

        // Rule with no conditions always fires (for its trigger)
        let mut rule = AutomationRule::new("Always fire".to_string(), RuleTrigger::OnComplete);
        rule.actions.push(RuleAction::Notify {
            message: "done".to_string(),
        });
        mgr.add_rule(rule).unwrap();

        let ctx = make_context("t1", "anything", "http://any.com", 0);
        let results = mgr.evaluate(RuleTrigger::OnComplete, &ctx);
        assert_eq!(results.len(), 1);
    }
}
