//! Download completion auto-actions
//!
//! Automatically trigger actions when downloads complete:
//! - Open file with system default application
//! - Move file to a target directory
//! - Run a custom shell command
//!
//! Actions can be configured globally or per-task.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Types of auto-actions that can trigger on download completion
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum AutoAction {
    /// Open the downloaded file with the system default application
    OpenFile,
    /// Move the downloaded file to a target directory
    MoveTo {
        /// Target directory path
        target_dir: PathBuf,
    },
    /// Run a custom shell command with the file path as argument
    RunCommand {
        /// Shell command to execute. Use {file} placeholder for the file path.
        command: String,
    },
}

/// Configuration for when auto-actions should trigger
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum AutoActionTrigger {
    /// Trigger only when task completes successfully
    #[default]
    OnComplete,
    /// Trigger when task completes or fails
    OnCompleteOrFail,
    /// Trigger only when task fails
    OnFail,
}

/// A rule that associates conditions with actions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoActionRule {
    /// Unique rule ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// When to trigger
    pub trigger: AutoActionTrigger,
    /// Actions to execute
    pub actions: Vec<AutoAction>,
    /// Optional: only apply to tasks matching these tags (empty = all tasks)
    pub tag_filter: Vec<String>,
    /// Optional: only apply to tasks in this group
    pub group_filter: Option<String>,
    /// Whether this rule is enabled
    pub enabled: bool,
    /// Rule priority (higher = checked first)
    pub priority: i32,
}

/// Per-task auto-action override
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAutoAction {
    /// Task ID this override applies to
    pub task_id: String,
    /// Actions to run for this specific task
    pub actions: Vec<AutoAction>,
    /// When to trigger
    pub trigger: AutoActionTrigger,
}

/// Result of executing an auto-action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoActionResult {
    /// Rule ID that triggered
    pub rule_id: String,
    /// Action that was executed
    pub action_type: String,
    /// Whether the action succeeded
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Summary of auto-actions system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoActionsSummary {
    /// Total number of rules
    pub total_rules: usize,
    /// Number of enabled rules
    pub enabled_rules: usize,
    /// Number of per-task overrides
    pub task_overrides: usize,
    /// Recent execution results (last 20)
    pub recent_results: Vec<AutoActionResult>,
    /// Total actions executed since start
    pub total_executed: u64,
    /// Total successes
    pub total_successes: u64,
    /// Total failures
    pub total_failures: u64,
}

/// Auto-actions configuration and manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoActionsConfig {
    /// Enable auto-actions globally
    pub enabled: bool,
    /// Global rules (checked in priority order)
    pub rules: Vec<AutoActionRule>,
    /// Per-task action overrides
    pub task_overrides: Vec<TaskAutoAction>,
    /// Maximum recent results to keep
    #[serde(default = "default_max_results")]
    pub max_results: usize,
}

fn default_max_results() -> usize {
    20
}

impl Default for AutoActionsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rules: Vec::new(),
            task_overrides: Vec::new(),
            max_results: 20,
        }
    }
}

/// Persistence helpers

#[derive(Debug, thiserror::Error)]
pub enum AutoActionsError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Rule not found: {0}")]
    RuleNotFound(String),
    #[error("Task override not found for task: {0}")]
    TaskOverrideNotFound(String),
}

/// Save config to disk (atomic write)
pub fn save_auto_actions_config(
    config: &AutoActionsConfig,
    path: &Path,
) -> Result<(), AutoActionsError> {
    let json = serde_json::to_string_pretty(config)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json.as_bytes())?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Load config from disk
pub fn load_auto_actions_config(path: &Path) -> Result<AutoActionsConfig, AutoActionsError> {
    if !path.exists() {
        return Ok(AutoActionsConfig::default());
    }
    let data = std::fs::read_to_string(path)?;
    let config: AutoActionsConfig = serde_json::from_str(&data)?;
    Ok(config)
}

/// Generate a simple unique ID
fn generate_rule_id() -> String {
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    format!("rule_{}_{}", ts.as_secs(), ts.subsec_nanos() % 10000)
}

/// Auto-actions manager (runtime state + execution)
#[derive(Debug)]
pub struct AutoActionsManager {
    config: AutoActionsConfig,
    results: Vec<AutoActionResult>,
    total_executed: u64,
    total_successes: u64,
    total_failures: u64,
}

impl AutoActionsManager {
    /// Create a new manager with the given config
    pub fn new(config: AutoActionsConfig) -> Self {
        Self {
            config,
            results: Vec::new(),
            total_executed: 0,
            total_successes: 0,
            total_failures: 0,
        }
    }

    /// Get current config
    pub fn config(&self) -> &AutoActionsConfig {
        &self.config
    }

    /// Set config
    pub fn set_config(&mut self, config: AutoActionsConfig) {
        self.config = config;
    }

    /// Enable or disable auto-actions globally
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    /// Add a new rule
    pub fn add_rule(&mut self, mut rule: AutoActionRule) -> String {
        if rule.id.is_empty() {
            rule.id = generate_rule_id();
        }
        self.config.rules.push(rule.clone());
        rule.id
    }

    /// Remove a rule by ID
    pub fn remove_rule(&mut self, rule_id: &str) -> Result<(), AutoActionsError> {
        let idx = self
            .config
            .rules
            .iter()
            .position(|r| r.id == rule_id)
            .ok_or_else(|| AutoActionsError::RuleNotFound(rule_id.to_string()))?;
        self.config.rules.remove(idx);
        Ok(())
    }

    /// Get a rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&AutoActionRule> {
        self.config.rules.iter().find(|r| r.id == rule_id)
    }

    /// List all rules
    pub fn list_rules(&self) -> &[AutoActionRule] {
        &self.config.rules
    }

    /// Enable/disable a specific rule
    pub fn set_rule_enabled(
        &mut self,
        rule_id: &str,
        enabled: bool,
    ) -> Result<(), AutoActionsError> {
        let rule = self
            .config
            .rules
            .iter_mut()
            .find(|r| r.id == rule_id)
            .ok_or_else(|| AutoActionsError::RuleNotFound(rule_id.to_string()))?;
        rule.enabled = enabled;
        Ok(())
    }

    /// Set per-task auto-action override
    pub fn set_task_override(
        &mut self,
        task_id: &str,
        actions: Vec<AutoAction>,
        trigger: AutoActionTrigger,
    ) {
        // Remove existing override for this task
        self.config.task_overrides.retain(|t| t.task_id != task_id);
        // Add new override
        self.config.task_overrides.push(TaskAutoAction {
            task_id: task_id.to_string(),
            actions,
            trigger,
        });
    }

    /// Remove per-task override
    pub fn remove_task_override(&mut self, task_id: &str) -> Result<(), AutoActionsError> {
        let idx = self
            .config
            .task_overrides
            .iter()
            .position(|t| t.task_id == task_id)
            .ok_or_else(|| AutoActionsError::TaskOverrideNotFound(task_id.to_string()))?;
        self.config.task_overrides.remove(idx);
        Ok(())
    }

    /// Get actions that should run for a completed/failed task
    /// Returns (rule_id, actions) pairs in priority order
    pub fn get_actions_for_task(
        &self,
        task_id: &str,
        tags: &[String],
        group: Option<&str>,
        is_complete: bool,
    ) -> Vec<(String, Vec<AutoAction>)> {
        if !self.config.enabled {
            return Vec::new();
        }

        // Check per-task overrides first (highest priority)
        if let Some(override_) = self
            .config
            .task_overrides
            .iter()
            .find(|t| t.task_id == task_id)
        {
            let trigger_match = match override_.trigger {
                AutoActionTrigger::OnComplete => is_complete,
                AutoActionTrigger::OnFail => !is_complete,
                AutoActionTrigger::OnCompleteOrFail => true,
            };
            if trigger_match && !override_.actions.is_empty() {
                return vec![("task_override".to_string(), override_.actions.clone())];
            }
            return Vec::new(); // Task override exists but doesn't match — don't fall through to rules
        }

        // Check global rules in priority order
        let mut matching_rules: Vec<&AutoActionRule> = self
            .config
            .rules
            .iter()
            .filter(|rule| {
                if !rule.enabled {
                    return false;
                }
                // Check trigger
                let trigger_match = match rule.trigger {
                    AutoActionTrigger::OnComplete => is_complete,
                    AutoActionTrigger::OnFail => !is_complete,
                    AutoActionTrigger::OnCompleteOrFail => true,
                };
                if !trigger_match {
                    return false;
                }
                // Check tag filter
                if !rule.tag_filter.is_empty() {
                    let has_matching_tag = tags.iter().any(|t| rule.tag_filter.contains(t));
                    if !has_matching_tag {
                        return false;
                    }
                }
                // Check group filter
                if let Some(ref group_filter) = rule.group_filter
                    && group != Some(group_filter.as_str())
                {
                    return false;
                }
                true
            })
            .collect();

        // Sort by priority descending
        matching_rules.sort_by_key(|r| std::cmp::Reverse(r.priority));

        matching_rules
            .into_iter()
            .map(|rule| (rule.id.clone(), rule.actions.clone()))
            .collect()
    }

    /// Record an action execution result
    pub fn record_result(&mut self, result: AutoActionResult) {
        self.total_executed += 1;
        if result.success {
            self.total_successes += 1;
        } else {
            self.total_failures += 1;
        }
        self.results.push(result);
        // Trim old results
        while self.results.len() > self.config.max_results {
            self.results.remove(0);
        }
    }

    /// Get summary
    pub fn summary(&self) -> AutoActionsSummary {
        AutoActionsSummary {
            total_rules: self.config.rules.len(),
            enabled_rules: self.config.rules.iter().filter(|r| r.enabled).count(),
            task_overrides: self.config.task_overrides.len(),
            recent_results: self.results.clone(),
            total_executed: self.total_executed,
            total_successes: self.total_successes,
            total_failures: self.total_failures,
        }
    }

    /// Clear execution history
    pub fn clear_history(&mut self) {
        self.results.clear();
        self.total_executed = 0;
        self.total_successes = 0;
        self.total_failures = 0;
    }
}

/// Execute a MoveTo action (synchronous, for use in async context via spawn_blocking)
pub fn execute_move_to(file_path: &Path, target_dir: &Path) -> Result<PathBuf, String> {
    if !file_path.exists() {
        return Err(format!(
            "Source file does not exist: {}",
            file_path.display()
        ));
    }
    if !target_dir.exists() {
        std::fs::create_dir_all(target_dir)
            .map_err(|e| format!("Failed to create target directory: {}", e))?;
    }
    let file_name = file_path
        .file_name()
        .ok_or_else(|| "Invalid file name".to_string())?;
    let target_path = target_dir.join(file_name);
    std::fs::rename(file_path, &target_path)
        .or_else(|_| {
            // Cross-device move: copy then delete
            std::fs::copy(file_path, &target_path)?;
            std::fs::remove_file(file_path)
        })
        .map_err(|e| format!("Failed to move file: {}", e))?;
    Ok(target_path)
}

/// Build the command string by replacing {file} placeholder
pub fn build_command(command_template: &str, file_path: &Path) -> String {
    command_template.replace("{file}", &file_path.to_string_lossy())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AutoActionsConfig {
        AutoActionsConfig {
            enabled: true,
            rules: vec![
                AutoActionRule {
                    id: "rule1".to_string(),
                    name: "Open videos".to_string(),
                    trigger: AutoActionTrigger::OnComplete,
                    actions: vec![AutoAction::OpenFile],
                    tag_filter: vec!["video".to_string()],
                    group_filter: None,
                    enabled: true,
                    priority: 10,
                },
                AutoActionRule {
                    id: "rule2".to_string(),
                    name: "Move archives".to_string(),
                    trigger: AutoActionTrigger::OnComplete,
                    actions: vec![AutoAction::MoveTo {
                        target_dir: PathBuf::from("/tmp/archives"),
                    }],
                    tag_filter: vec!["archive".to_string()],
                    group_filter: None,
                    enabled: true,
                    priority: 5,
                },
                AutoActionRule {
                    id: "rule3".to_string(),
                    name: "Notify on failure".to_string(),
                    trigger: AutoActionTrigger::OnFail,
                    actions: vec![AutoAction::RunCommand {
                        command: "echo {file} failed".to_string(),
                    }],
                    tag_filter: vec![],
                    group_filter: None,
                    enabled: false, // disabled
                    priority: 1,
                },
            ],
            task_overrides: vec![],
            max_results: 20,
        }
    }

    #[test]
    fn test_get_actions_tag_match() {
        let mgr = AutoActionsManager::new(test_config());
        let tags = vec!["video".to_string(), "hd".to_string()];
        let actions = mgr.get_actions_for_task("t1", &tags, None, true);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].0, "rule1");
        assert_eq!(actions[0].1.len(), 1);
        assert!(matches!(actions[0].1[0], AutoAction::OpenFile));
    }

    #[test]
    fn test_get_actions_no_match() {
        let mgr = AutoActionsManager::new(test_config());
        let tags = vec!["document".to_string()];
        let actions = mgr.get_actions_for_task("t1", &tags, None, true);
        assert!(actions.is_empty());
    }

    #[test]
    fn test_get_actions_disabled_rule() {
        let mgr = AutoActionsManager::new(test_config());
        let tags = vec![];
        // rule3 is disabled
        let actions = mgr.get_actions_for_task("t1", &tags, None, false);
        assert!(actions.is_empty());
    }

    #[test]
    fn test_get_actions_disabled_globally() {
        let mut config = test_config();
        config.enabled = false;
        let mgr = AutoActionsManager::new(config);
        let tags = vec!["video".to_string()];
        let actions = mgr.get_actions_for_task("t1", &tags, None, true);
        assert!(actions.is_empty());
    }

    #[test]
    fn test_task_override_priority() {
        let mut config = test_config();
        config.task_overrides.push(TaskAutoAction {
            task_id: "t1".to_string(),
            actions: vec![AutoAction::RunCommand {
                command: "echo done".to_string(),
            }],
            trigger: AutoActionTrigger::OnComplete,
        });
        let mgr = AutoActionsManager::new(config);
        // Even though t1 has "video" tag, task override takes priority
        let tags = vec!["video".to_string()];
        let actions = mgr.get_actions_for_task("t1", &tags, None, true);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].0, "task_override");
        assert!(matches!(actions[0].1[0], AutoAction::RunCommand { .. }));
    }

    #[test]
    fn test_task_override_trigger_mismatch() {
        let mut config = test_config();
        config.task_overrides.push(TaskAutoAction {
            task_id: "t1".to_string(),
            actions: vec![AutoAction::OpenFile],
            trigger: AutoActionTrigger::OnFail,
        });
        let mgr = AutoActionsManager::new(config);
        let tags = vec![];
        // Task completed, but override is OnFail only
        let actions = mgr.get_actions_for_task("t1", &tags, None, true);
        assert!(actions.is_empty());
    }

    #[test]
    fn test_group_filter() {
        let mut config = test_config();
        config.rules.push(AutoActionRule {
            id: "rule_group".to_string(),
            name: "Work group".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: Some("work".to_string()),
            enabled: true,
            priority: 100,
        });
        let mgr = AutoActionsManager::new(config);
        let tags = vec![];
        // Match by group
        let actions = mgr.get_actions_for_task("t1", &tags, Some("work"), true);
        assert!(!actions.is_empty());
        assert_eq!(actions[0].0, "rule_group");
        // No match with different group
        let actions = mgr.get_actions_for_task("t1", &tags, Some("personal"), true);
        assert!(actions.is_empty());
    }

    #[test]
    fn test_add_remove_rule() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        let rule = AutoActionRule {
            id: "".to_string(), // auto-generate
            name: "Test".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: None,
            enabled: true,
            priority: 1,
        };
        let id = mgr.add_rule(rule);
        assert!(!id.is_empty());
        assert_eq!(mgr.list_rules().len(), 1);
        assert!(mgr.get_rule(&id).is_some());

        mgr.remove_rule(&id).unwrap();
        assert!(mgr.list_rules().is_empty());
    }

    #[test]
    fn test_remove_rule_not_found() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        let result = mgr.remove_rule("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_set_rule_enabled() {
        let mut config = test_config();
        let mut mgr = AutoActionsManager::new(config);
        assert!(mgr.get_rule("rule1").unwrap().enabled);
        mgr.set_rule_enabled("rule1", false).unwrap();
        assert!(!mgr.get_rule("rule1").unwrap().enabled);
    }

    #[test]
    fn test_task_override_set_remove() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        mgr.set_task_override(
            "t1",
            vec![AutoAction::OpenFile],
            AutoActionTrigger::OnComplete,
        );
        assert_eq!(mgr.config().task_overrides.len(), 1);

        // Setting again replaces
        mgr.set_task_override(
            "t1",
            vec![AutoAction::MoveTo {
                target_dir: PathBuf::from("/x"),
            }],
            AutoActionTrigger::OnComplete,
        );
        assert_eq!(mgr.config().task_overrides.len(), 1);

        mgr.remove_task_override("t1").unwrap();
        assert!(mgr.config().task_overrides.is_empty());
    }

    #[test]
    fn test_remove_task_override_not_found() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        let result = mgr.remove_task_override("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_record_result_and_summary() {
        let mut mgr = AutoActionsManager::new(test_config());
        mgr.record_result(AutoActionResult {
            rule_id: "rule1".to_string(),
            action_type: "open_file".to_string(),
            success: true,
            error: None,
            timestamp: chrono::Utc::now(),
        });
        mgr.record_result(AutoActionResult {
            rule_id: "rule2".to_string(),
            action_type: "move_to".to_string(),
            success: false,
            error: Some("Permission denied".to_string()),
            timestamp: chrono::Utc::now(),
        });
        let summary = mgr.summary();
        assert_eq!(summary.total_executed, 2);
        assert_eq!(summary.total_successes, 1);
        assert_eq!(summary.total_failures, 1);
        assert_eq!(summary.recent_results.len(), 2);
    }

    #[test]
    fn test_clear_history() {
        let mut mgr = AutoActionsManager::new(test_config());
        mgr.record_result(AutoActionResult {
            rule_id: "r1".to_string(),
            action_type: "open_file".to_string(),
            success: true,
            error: None,
            timestamp: chrono::Utc::now(),
        });
        mgr.clear_history();
        let summary = mgr.summary();
        assert_eq!(summary.total_executed, 0);
        assert!(summary.recent_results.is_empty());
    }

    #[test]
    fn test_max_results_trim() {
        let mut config = test_config();
        config.max_results = 3;
        let mut mgr = AutoActionsManager::new(config);
        for i in 0..5 {
            mgr.record_result(AutoActionResult {
                rule_id: format!("r{}", i),
                action_type: "test".to_string(),
                success: true,
                error: None,
                timestamp: chrono::Utc::now(),
            });
        }
        assert_eq!(mgr.summary().recent_results.len(), 3);
    }

    #[test]
    fn test_build_command() {
        let path = Path::new("/tmp/test.mp4");
        assert_eq!(build_command("vlc {file}", path), "vlc /tmp/test.mp4");
        assert_eq!(build_command("echo done", path), "echo done");
        assert_eq!(
            build_command("cp {file} /backup/{file}", path),
            "cp /tmp/test.mp4 /backup//tmp/test.mp4"
        );
    }

    #[test]
    fn test_save_load_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auto_actions.json");

        let config = AutoActionsConfig {
            enabled: true,
            rules: vec![AutoActionRule {
                id: "r1".to_string(),
                name: "Test rule".to_string(),
                trigger: AutoActionTrigger::OnCompleteOrFail,
                actions: vec![
                    AutoAction::OpenFile,
                    AutoAction::MoveTo {
                        target_dir: PathBuf::from("/tmp/out"),
                    },
                    AutoAction::RunCommand {
                        command: "notify-send {file}".to_string(),
                    },
                ],
                tag_filter: vec!["test".to_string()],
                group_filter: Some("work".to_string()),
                enabled: true,
                priority: 5,
            }],
            task_overrides: vec![TaskAutoAction {
                task_id: "t1".to_string(),
                actions: vec![AutoAction::OpenFile],
                trigger: AutoActionTrigger::OnComplete,
            }],
            max_results: 50,
        };

        save_auto_actions_config(&config, &path).unwrap();
        let loaded = load_auto_actions_config(&path).unwrap();

        assert_eq!(loaded.enabled, true);
        assert_eq!(loaded.rules.len(), 1);
        assert_eq!(loaded.rules[0].name, "Test rule");
        assert_eq!(loaded.rules[0].actions.len(), 3);
        assert_eq!(loaded.task_overrides.len(), 1);
        assert_eq!(loaded.max_results, 50);
    }

    #[test]
    fn test_load_missing_file() {
        let path = Path::new("/tmp/nonexistent_auto_actions.json");
        let config = load_auto_actions_config(path).unwrap();
        assert!(!config.enabled);
        assert!(config.rules.is_empty());
    }

    #[test]
    fn test_priority_ordering() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules = vec![
            AutoActionRule {
                id: "low".to_string(),
                name: "Low priority".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 1,
            },
            AutoActionRule {
                id: "high".to_string(),
                name: "High priority".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 100,
            },
            AutoActionRule {
                id: "mid".to_string(),
                name: "Mid priority".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 50,
            },
        ];
        let mgr = AutoActionsManager::new(config);
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0].0, "high");
        assert_eq!(actions[1].0, "mid");
        assert_eq!(actions[2].0, "low");
    }

    #[test]
    fn test_on_complete_or_fail_trigger() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules.push(AutoActionRule {
            id: "any".to_string(),
            name: "Any outcome".to_string(),
            trigger: AutoActionTrigger::OnCompleteOrFail,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: None,
            enabled: true,
            priority: 1,
        });
        let mgr = AutoActionsManager::new(config);
        // Complete
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert_eq!(actions.len(), 1);
        // Failed
        let actions = mgr.get_actions_for_task("t1", &[], None, false);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let action = AutoAction::RunCommand {
            command: "echo {file}".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: AutoAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action, deserialized);

        let action2 = AutoAction::MoveTo {
            target_dir: PathBuf::from("/tmp/test"),
        };
        let json2 = serde_json::to_string(&action2).unwrap();
        let deserialized2: AutoAction = serde_json::from_str(&json2).unwrap();
        assert_eq!(action2, deserialized2);
    }

    // ===== Phase 241: Comprehensive Test Coverage =====

    // --- AutoAction serde: all variants ---
    #[test]
    fn auto_action_open_file_serde() {
        let action = AutoAction::OpenFile;
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("\"open_file\""));
        let back: AutoAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action, back);
    }

    #[test]
    fn auto_action_move_to_serde() {
        let action = AutoAction::MoveTo {
            target_dir: PathBuf::from("/data/downloads"),
        };
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("\"move_to\""));
        assert!(json.contains("/data/downloads"));
        let back: AutoAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action, back);
    }

    #[test]
    fn auto_action_run_command_serde() {
        let action = AutoAction::RunCommand {
            command: "notify-send 'done'".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("\"run_command\""));
        assert!(json.contains("notify-send"));
        let back: AutoAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action, back);
    }

    #[test]
    fn auto_action_snake_case_tag_values() {
        // Verify the serde tag uses snake_case
        let json = r#"{"type":"open_file"}"#;
        let action: AutoAction = serde_json::from_str(json).unwrap();
        assert!(matches!(action, AutoAction::OpenFile));

        let json = r#"{"type":"move_to","target_dir":"/tmp"}"#;
        let action: AutoAction = serde_json::from_str(json).unwrap();
        assert!(matches!(action, AutoAction::MoveTo { .. }));

        let json = r#"{"type":"run_command","command":"ls"}"#;
        let action: AutoAction = serde_json::from_str(json).unwrap();
        assert!(matches!(action, AutoAction::RunCommand { .. }));
    }

    #[test]
    fn auto_action_extra字段忽略() {
        let json = r#"{"type":"open_file","extra_field":"ignored","another":42}"#;
        let action: AutoAction = serde_json::from_str(json).unwrap();
        assert!(matches!(action, AutoAction::OpenFile));
    }

    #[test]
    fn auto_action_unicode_command() {
        let action = AutoAction::RunCommand {
            command: "echo '你好世界' {file}".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: AutoAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action, back);
    }

    #[test]
    fn auto_action_unicode_target_dir() {
        let action = AutoAction::MoveTo {
            target_dir: PathBuf::from("/home/用户/下载"),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: AutoAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action, back);
    }

    // --- AutoActionTrigger serde ---
    #[test]
    fn auto_action_trigger_all_variants_serde() {
        let triggers = vec![
            AutoActionTrigger::OnComplete,
            AutoActionTrigger::OnCompleteOrFail,
            AutoActionTrigger::OnFail,
        ];
        for trigger in triggers {
            let json = serde_json::to_string(&trigger).unwrap();
            let back: AutoActionTrigger = serde_json::from_str(&json).unwrap();
            assert_eq!(trigger, back);
        }
    }

    #[test]
    fn auto_action_trigger_snake_case_values() {
        let json = r#""on_complete""#;
        let t: AutoActionTrigger = serde_json::from_str(json).unwrap();
        assert!(matches!(t, AutoActionTrigger::OnComplete));

        let json = r#""on_complete_or_fail""#;
        let t: AutoActionTrigger = serde_json::from_str(json).unwrap();
        assert!(matches!(t, AutoActionTrigger::OnCompleteOrFail));

        let json = r#""on_fail""#;
        let t: AutoActionTrigger = serde_json::from_str(json).unwrap();
        assert!(matches!(t, AutoActionTrigger::OnFail));
    }

    // --- AutoActionTrigger traits ---
    #[test]
    fn auto_action_trigger_clone_debug() {
        let t = AutoActionTrigger::OnComplete;
        let t2 = t.clone();
        assert_eq!(t, t2);
        let debug = format!("{:?}", t);
        assert!(debug.contains("OnComplete"));
    }

    // --- AutoAction traits ---
    #[test]
    fn auto_action_clone_debug() {
        let action = AutoAction::RunCommand {
            command: "test".to_string(),
        };
        let cloned = action.clone();
        assert_eq!(action, cloned);
        let debug = format!("{:?}", action);
        assert!(debug.contains("RunCommand"));
    }

    #[test]
    fn auto_action_clone_independence() {
        let action = AutoAction::MoveTo {
            target_dir: PathBuf::from("/a"),
        };
        let mut cloned = action.clone();
        if let AutoAction::MoveTo { ref mut target_dir } = cloned {
            *target_dir = PathBuf::from("/b");
        }
        // Original unchanged
        if let AutoAction::MoveTo { ref target_dir } = action {
            assert_eq!(target_dir, &PathBuf::from("/a"));
        }
    }

    // --- AutoActionRule serde ---
    #[test]
    fn auto_action_rule_serde_roundtrip() {
        let rule = AutoActionRule {
            id: "r1".to_string(),
            name: "Test".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec!["video".to_string()],
            group_filter: Some("work".to_string()),
            enabled: true,
            priority: 10,
        };
        let json = serde_json::to_string(&rule).unwrap();
        let back: AutoActionRule = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "r1");
        assert_eq!(back.name, "Test");
        assert_eq!(back.actions.len(), 1);
        assert_eq!(back.tag_filter, vec!["video"]);
        assert_eq!(back.group_filter, Some("work".to_string()));
        assert!(back.enabled);
        assert_eq!(back.priority, 10);
    }

    #[test]
    fn auto_action_rule_serde_extra_fields_ignored() {
        let json = r#"{
            "id": "r1",
            "name": "Test",
            "trigger": "on_complete",
            "actions": [],
            "tag_filter": [],
            "group_filter": null,
            "enabled": true,
            "priority": 0,
            "unknown_field": "ignored"
        }"#;
        let rule: AutoActionRule = serde_json::from_str(json).unwrap();
        assert_eq!(rule.id, "r1");
    }

    #[test]
    fn auto_action_rule_clone_debug() {
        let rule = AutoActionRule {
            id: "r1".to_string(),
            name: "Test".to_string(),
            trigger: AutoActionTrigger::OnFail,
            actions: vec![],
            tag_filter: vec![],
            group_filter: None,
            enabled: false,
            priority: -5,
        };
        let cloned = rule.clone();
        assert_eq!(cloned.id, rule.id);
        assert_eq!(cloned.name, rule.name);
        let debug = format!("{:?}", rule);
        assert!(debug.contains("OnFail"));
    }

    // --- TaskAutoAction serde ---
    #[test]
    fn task_auto_action_serde_roundtrip() {
        let ta = TaskAutoAction {
            task_id: "task_123".to_string(),
            actions: vec![
                AutoAction::OpenFile,
                AutoAction::MoveTo {
                    target_dir: PathBuf::from("/out"),
                },
            ],
            trigger: AutoActionTrigger::OnCompleteOrFail,
        };
        let json = serde_json::to_string(&ta).unwrap();
        let back: TaskAutoAction = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, "task_123");
        assert_eq!(back.actions.len(), 2);
        assert!(matches!(back.trigger, AutoActionTrigger::OnCompleteOrFail));
    }

    #[test]
    fn task_auto_action_clone_debug() {
        let ta = TaskAutoAction {
            task_id: "t1".to_string(),
            actions: vec![AutoAction::OpenFile],
            trigger: AutoActionTrigger::OnComplete,
        };
        let cloned = ta.clone();
        assert_eq!(cloned.task_id, ta.task_id);
        let debug = format!("{:?}", ta);
        assert!(debug.contains("t1"));
    }

    // --- AutoActionResult serde ---
    #[test]
    fn auto_action_result_serde_roundtrip() {
        let result = AutoActionResult {
            rule_id: "r1".to_string(),
            action_type: "open_file".to_string(),
            success: true,
            error: None,
            timestamp: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: AutoActionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.rule_id, "r1");
        assert!(back.success);
        assert!(back.error.is_none());
    }

    #[test]
    fn auto_action_result_serde_with_error() {
        let result = AutoActionResult {
            rule_id: "r2".to_string(),
            action_type: "move_to".to_string(),
            success: false,
            error: Some("Permission denied".to_string()),
            timestamp: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: AutoActionResult = serde_json::from_str(&json).unwrap();
        assert!(!back.success);
        assert_eq!(back.error.as_deref(), Some("Permission denied"));
    }

    #[test]
    fn auto_action_result_clone_debug() {
        let result = AutoActionResult {
            rule_id: "r1".to_string(),
            action_type: "test".to_string(),
            success: true,
            error: None,
            timestamp: chrono::Utc::now(),
        };
        let cloned = result.clone();
        assert_eq!(cloned.rule_id, result.rule_id);
        let debug = format!("{:?}", result);
        assert!(debug.contains("r1"));
    }

    // --- AutoActionsSummary serde ---
    #[test]
    fn auto_actions_summary_serde_roundtrip() {
        let summary = AutoActionsSummary {
            total_rules: 5,
            enabled_rules: 3,
            task_overrides: 2,
            recent_results: vec![],
            total_executed: 100,
            total_successes: 90,
            total_failures: 10,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: AutoActionsSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_rules, 5);
        assert_eq!(back.enabled_rules, 3);
        assert_eq!(back.total_executed, 100);
        assert_eq!(back.total_successes, 90);
        assert_eq!(back.total_failures, 10);
    }

    #[test]
    fn auto_actions_summary_clone_debug() {
        let summary = AutoActionsSummary {
            total_rules: 1,
            enabled_rules: 1,
            task_overrides: 0,
            recent_results: vec![],
            total_executed: 0,
            total_successes: 0,
            total_failures: 0,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.total_rules, summary.total_rules);
        let debug = format!("{:?}", summary);
        assert!(debug.contains("total_rules"));
    }

    // --- AutoActionsConfig ---
    #[test]
    fn auto_actions_config_default_values() {
        let config = AutoActionsConfig::default();
        assert!(!config.enabled);
        assert!(config.rules.is_empty());
        assert!(config.task_overrides.is_empty());
        assert_eq!(config.max_results, 20);
    }

    #[test]
    fn auto_actions_config_serde_roundtrip() {
        let config = AutoActionsConfig {
            enabled: true,
            rules: vec![AutoActionRule {
                id: "r1".to_string(),
                name: "Test".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 1,
            }],
            task_overrides: vec![],
            max_results: 50,
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: AutoActionsConfig = serde_json::from_str(&json).unwrap();
        assert!(back.enabled);
        assert_eq!(back.rules.len(), 1);
        assert_eq!(back.max_results, 50);
    }

    #[test]
    fn auto_actions_config_pretty_serde() {
        let config = AutoActionsConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let back: AutoActionsConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.enabled, config.enabled);
        assert_eq!(back.max_results, config.max_results);
    }

    #[test]
    fn auto_actions_config_extra_fields_ignored() {
        let json = r#"{
            "enabled": true,
            "rules": [],
            "task_overrides": [],
            "max_results": 10,
            "future_field": "ignored"
        }"#;
        let config: AutoActionsConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_results, 10);
    }

    #[test]
    fn auto_actions_config_clone_debug() {
        let config = AutoActionsConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.enabled, config.enabled);
        assert_eq!(cloned.max_results, config.max_results);
        let debug = format!("{:?}", config);
        assert!(debug.contains("AutoActionsConfig"));
    }

    #[test]
    fn auto_actions_config_default_max_results_serde() {
        // When max_results is missing from JSON, default_max_results() is used
        let json = r#"{"enabled":false,"rules":[],"task_overrides":[]}"#;
        let config: AutoActionsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_results, 20);
    }

    // --- AutoActionsError ---
    #[test]
    fn auto_actions_error_io_display() {
        let err = AutoActionsError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("file missing"));
    }

    #[test]
    fn auto_actions_error_json_display() {
        let json_err = serde_json::from_str::<AutoActionsConfig>("invalid").unwrap_err();
        let err = AutoActionsError::Json(json_err);
        let msg = format!("{}", err);
        assert!(msg.contains("JSON"));
    }

    #[test]
    fn auto_actions_error_rule_not_found_display() {
        let err = AutoActionsError::RuleNotFound("rule_xyz".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("rule_xyz"));
        assert!(msg.contains("not found"));
    }

    #[test]
    fn auto_actions_error_task_override_not_found_display() {
        let err = AutoActionsError::TaskOverrideNotFound("task_abc".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("task_abc"));
    }

    #[test]
    fn auto_actions_error_debug() {
        let err = AutoActionsError::RuleNotFound("r1".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("RuleNotFound"));
    }

    #[test]
    fn auto_actions_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: AutoActionsError = io_err.into();
        let msg = format!("{}", err);
        assert!(msg.contains("denied"));
    }

    #[test]
    fn auto_actions_error_from_serde_json() {
        let json_err = serde_json::from_str::<AutoActionsConfig>("bad json").unwrap_err();
        let err: AutoActionsError = json_err.into();
        let msg = format!("{}", err);
        assert!(msg.contains("JSON"));
    }

    // --- Manager: new, config, set_config, set_enabled ---
    #[test]
    fn manager_new_preserves_config() {
        let config = test_config();
        let mgr = AutoActionsManager::new(config.clone());
        assert_eq!(mgr.config().enabled, config.enabled);
        assert_eq!(mgr.config().rules.len(), config.rules.len());
        assert_eq!(mgr.config().max_results, config.max_results);
    }

    #[test]
    fn manager_set_config_replaces() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        assert!(!mgr.config().enabled);
        let new_config = AutoActionsConfig {
            enabled: true,
            rules: vec![],
            task_overrides: vec![],
            max_results: 100,
        };
        mgr.set_config(new_config);
        assert!(mgr.config().enabled);
        assert_eq!(mgr.config().max_results, 100);
    }

    #[test]
    fn manager_set_enabled_toggle() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        assert!(!mgr.config().enabled);
        mgr.set_enabled(true);
        assert!(mgr.config().enabled);
        mgr.set_enabled(false);
        assert!(!mgr.config().enabled);
    }

    // --- Manager: add_rule ---
    #[test]
    fn manager_add_rule_auto_generates_id() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        let rule = AutoActionRule {
            id: "".to_string(),
            name: "Auto ID".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: None,
            enabled: true,
            priority: 1,
        };
        let id = mgr.add_rule(rule);
        assert!(!id.is_empty());
        assert!(id.starts_with("rule_"));
    }

    #[test]
    fn manager_add_rule_preserves_explicit_id() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        let rule = AutoActionRule {
            id: "my_custom_id".to_string(),
            name: "Custom".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: None,
            enabled: true,
            priority: 1,
        };
        let id = mgr.add_rule(rule);
        assert_eq!(id, "my_custom_id");
    }

    #[test]
    fn manager_add_multiple_rules() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        for i in 0..5 {
            let rule = AutoActionRule {
                id: format!("rule_{}", i),
                name: format!("Rule {}", i),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: i,
            };
            mgr.add_rule(rule);
        }
        assert_eq!(mgr.list_rules().len(), 5);
    }

    // --- Manager: remove_rule ---
    #[test]
    fn manager_remove_rule_clears_from_list() {
        let mut mgr = AutoActionsManager::new(test_config());
        assert_eq!(mgr.list_rules().len(), 3);
        mgr.remove_rule("rule1").unwrap();
        assert_eq!(mgr.list_rules().len(), 2);
        assert!(mgr.get_rule("rule1").is_none());
    }

    // --- Manager: set_rule_enabled ---
    #[test]
    fn manager_set_rule_enabled_not_found() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        let result = mgr.set_rule_enabled("nonexistent", true);
        assert!(result.is_err());
        if let Err(AutoActionsError::RuleNotFound(id)) = result {
            assert_eq!(id, "nonexistent");
        }
    }

    #[test]
    fn manager_set_rule_enabled_idempotent() {
        let mut mgr = AutoActionsManager::new(test_config());
        // rule1 is already enabled
        mgr.set_rule_enabled("rule1", true).unwrap();
        assert!(mgr.get_rule("rule1").unwrap().enabled);
        mgr.set_rule_enabled("rule1", true).unwrap();
        assert!(mgr.get_rule("rule1").unwrap().enabled);
    }

    // --- Manager: get_rule / list_rules ---
    #[test]
    fn manager_get_rule_returns_correct_rule() {
        let mgr = AutoActionsManager::new(test_config());
        let rule = mgr.get_rule("rule2").unwrap();
        assert_eq!(rule.name, "Move archives");
        assert!(matches!(rule.trigger, AutoActionTrigger::OnComplete));
    }

    #[test]
    fn manager_get_rule_nonexistent() {
        let mgr = AutoActionsManager::new(test_config());
        assert!(mgr.get_rule("nonexistent").is_none());
    }

    #[test]
    fn manager_list_rules_returns_all() {
        let mgr = AutoActionsManager::new(test_config());
        let rules = mgr.list_rules();
        assert_eq!(rules.len(), 3);
    }

    // --- Manager: task overrides ---
    #[test]
    fn manager_set_task_override_replaces_existing() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        mgr.set_task_override(
            "t1",
            vec![AutoAction::OpenFile],
            AutoActionTrigger::OnComplete,
        );
        mgr.set_task_override(
            "t1",
            vec![AutoAction::MoveTo {
                target_dir: PathBuf::from("/new"),
            }],
            AutoActionTrigger::OnFail,
        );
        assert_eq!(mgr.config().task_overrides.len(), 1);
        let ta = &mgr.config().task_overrides[0];
        assert!(matches!(ta.trigger, AutoActionTrigger::OnFail));
        assert!(matches!(ta.actions[0], AutoAction::MoveTo { .. }));
    }

    #[test]
    fn manager_multiple_task_overrides() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        for i in 0..5 {
            mgr.set_task_override(
                &format!("task_{}", i),
                vec![AutoAction::OpenFile],
                AutoActionTrigger::OnComplete,
            );
        }
        assert_eq!(mgr.config().task_overrides.len(), 5);
    }

    #[test]
    fn manager_remove_task_override_not_found_error() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        let result = mgr.remove_task_override("nonexistent");
        assert!(result.is_err());
        if let Err(AutoActionsError::TaskOverrideNotFound(id)) = result {
            assert_eq!(id, "nonexistent");
        }
    }

    // --- Manager: get_actions_for_task ---
    #[test]
    fn get_actions_empty_tags_no_filter_rules() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules.push(AutoActionRule {
            id: "no_filter".to_string(),
            name: "No filter".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: None,
            enabled: true,
            priority: 1,
        });
        let mgr = AutoActionsManager::new(config);
        // No tags, but rule has no tag_filter so it matches all
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].0, "no_filter");
    }

    #[test]
    fn get_actions_tag_filter_partial_match() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules.push(AutoActionRule {
            id: "video_rule".to_string(),
            name: "Video".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec!["video".to_string(), "movie".to_string()],
            group_filter: None,
            enabled: true,
            priority: 1,
        });
        let mgr = AutoActionsManager::new(config);
        // Task has "movie" tag which is in the filter
        let actions = mgr.get_actions_for_task("t1", &["movie".to_string()], None, true);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn get_actions_tag_filter_no_match() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules.push(AutoActionRule {
            id: "video_rule".to_string(),
            name: "Video".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec!["video".to_string()],
            group_filter: None,
            enabled: true,
            priority: 1,
        });
        let mgr = AutoActionsManager::new(config);
        // Task has "audio" tag, not "video"
        let actions = mgr.get_actions_for_task("t1", &["audio".to_string()], None, true);
        assert!(actions.is_empty());
    }

    #[test]
    fn get_actions_on_fail_trigger() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules.push(AutoActionRule {
            id: "fail_rule".to_string(),
            name: "On fail".to_string(),
            trigger: AutoActionTrigger::OnFail,
            actions: vec![AutoAction::RunCommand {
                command: "echo failed".to_string(),
            }],
            tag_filter: vec![],
            group_filter: None,
            enabled: true,
            priority: 1,
        });
        let mgr = AutoActionsManager::new(config);
        // Task completed — should NOT trigger OnFail rule
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert!(actions.is_empty());
        // Task failed — should trigger
        let actions = mgr.get_actions_for_task("t1", &[], None, false);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn get_actions_multiple_matching_rules_sorted_by_priority() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules = vec![
            AutoActionRule {
                id: "low".to_string(),
                name: "Low".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 1,
            },
            AutoActionRule {
                id: "high".to_string(),
                name: "High".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 100,
            },
            AutoActionRule {
                id: "mid".to_string(),
                name: "Mid".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 50,
            },
        ];
        let mgr = AutoActionsManager::new(config);
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0].0, "high");
        assert_eq!(actions[1].0, "mid");
        assert_eq!(actions[2].0, "low");
    }

    #[test]
    fn get_actions_same_priority_preserves_order() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules = vec![
            AutoActionRule {
                id: "first".to_string(),
                name: "First".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 10,
            },
            AutoActionRule {
                id: "second".to_string(),
                name: "Second".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 10,
            },
        ];
        let mgr = AutoActionsManager::new(config);
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert_eq!(actions.len(), 2);
        // Both have same priority, so relative order is stable
        assert_eq!(actions[0].0, "first");
        assert_eq!(actions[1].0, "second");
    }

    #[test]
    fn get_actions_negative_priority() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules = vec![
            AutoActionRule {
                id: "negative".to_string(),
                name: "Negative".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: -10,
            },
            AutoActionRule {
                id: "zero".to_string(),
                name: "Zero".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 0,
            },
        ];
        let mgr = AutoActionsManager::new(config);
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].0, "zero");
        assert_eq!(actions[1].0, "negative");
    }

    #[test]
    fn get_actions_group_filter_none_group() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules.push(AutoActionRule {
            id: "grouped".to_string(),
            name: "Grouped".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: Some("work".to_string()),
            enabled: true,
            priority: 1,
        });
        let mgr = AutoActionsManager::new(config);
        // Task has no group — should NOT match
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert!(actions.is_empty());
    }

    #[test]
    fn get_actions_unicode_tags() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules.push(AutoActionRule {
            id: "unicode_rule".to_string(),
            name: "Unicode".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec!["视频".to_string()],
            group_filter: None,
            enabled: true,
            priority: 1,
        });
        let mgr = AutoActionsManager::new(config);
        let actions = mgr.get_actions_for_task("t1", &["视频".to_string()], None, true);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn get_actions_unicode_task_id() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.task_overrides.push(TaskAutoAction {
            task_id: "任务_🎉".to_string(),
            actions: vec![AutoAction::OpenFile],
            trigger: AutoActionTrigger::OnComplete,
        });
        let mgr = AutoActionsManager::new(config);
        let actions = mgr.get_actions_for_task("任务_🎉", &[], None, true);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].0, "task_override");
    }

    // --- Manager: record_result and summary ---
    #[test]
    fn manager_record_result_accumulates() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        for i in 0..10 {
            mgr.record_result(AutoActionResult {
                rule_id: format!("r{}", i),
                action_type: "test".to_string(),
                success: i % 2 == 0,
                error: None,
                timestamp: chrono::Utc::now(),
            });
        }
        let summary = mgr.summary();
        assert_eq!(summary.total_executed, 10);
        assert_eq!(summary.total_successes, 5);
        assert_eq!(summary.total_failures, 5);
    }

    #[test]
    fn manager_max_results_zero_keeps_nothing() {
        let mut config = AutoActionsConfig::default();
        config.max_results = 0;
        let mut mgr = AutoActionsManager::new(config);
        mgr.record_result(AutoActionResult {
            rule_id: "r1".to_string(),
            action_type: "test".to_string(),
            success: true,
            error: None,
            timestamp: chrono::Utc::now(),
        });
        assert!(mgr.summary().recent_results.is_empty());
        // But counters still increment
        assert_eq!(mgr.summary().total_executed, 1);
    }

    #[test]
    fn manager_max_results_one_keeps_latest() {
        let mut config = AutoActionsConfig::default();
        config.max_results = 1;
        let mut mgr = AutoActionsManager::new(config);
        mgr.record_result(AutoActionResult {
            rule_id: "first".to_string(),
            action_type: "test".to_string(),
            success: true,
            error: None,
            timestamp: chrono::Utc::now(),
        });
        mgr.record_result(AutoActionResult {
            rule_id: "second".to_string(),
            action_type: "test".to_string(),
            success: true,
            error: None,
            timestamp: chrono::Utc::now(),
        });
        let results = mgr.summary().recent_results;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rule_id, "second");
    }

    #[test]
    fn manager_clear_history_resets_all_counters() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        for i in 0..20 {
            mgr.record_result(AutoActionResult {
                rule_id: format!("r{}", i),
                action_type: "test".to_string(),
                success: i % 3 == 0,
                error: if i % 3 == 0 {
                    None
                } else {
                    Some("err".to_string())
                },
                timestamp: chrono::Utc::now(),
            });
        }
        mgr.clear_history();
        let summary = mgr.summary();
        assert_eq!(summary.total_executed, 0);
        assert_eq!(summary.total_successes, 0);
        assert_eq!(summary.total_failures, 0);
        assert!(summary.recent_results.is_empty());
    }

    // --- Manager: summary ---
    #[test]
    fn manager_summary_empty_state() {
        let mgr = AutoActionsManager::new(AutoActionsConfig::default());
        let summary = mgr.summary();
        assert_eq!(summary.total_rules, 0);
        assert_eq!(summary.enabled_rules, 0);
        assert_eq!(summary.task_overrides, 0);
        assert!(summary.recent_results.is_empty());
        assert_eq!(summary.total_executed, 0);
        assert_eq!(summary.total_successes, 0);
        assert_eq!(summary.total_failures, 0);
    }

    #[test]
    fn manager_summary_counts_enabled_rules() {
        let mut config = AutoActionsConfig::default();
        config.rules = vec![
            AutoActionRule {
                id: "enabled1".to_string(),
                name: "E1".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 1,
            },
            AutoActionRule {
                id: "disabled1".to_string(),
                name: "D1".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![],
                tag_filter: vec![],
                group_filter: None,
                enabled: false,
                priority: 1,
            },
            AutoActionRule {
                id: "enabled2".to_string(),
                name: "E2".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 1,
            },
        ];
        let mgr = AutoActionsManager::new(config);
        let summary = mgr.summary();
        assert_eq!(summary.total_rules, 3);
        assert_eq!(summary.enabled_rules, 2);
    }

    // --- execute_move_to ---
    #[test]
    fn execute_move_to_basic() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("test.txt");
        std::fs::write(&src, "hello").unwrap();
        let target = dir.path().join("subdir");

        let result = execute_move_to(&src, &target);
        assert!(result.is_ok());
        let dest = result.unwrap();
        assert_eq!(dest, target.join("test.txt"));
        assert!(dest.exists());
        assert!(!src.exists());
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "hello");
    }

    #[test]
    fn execute_move_to_source_not_exists() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("nonexistent.txt");
        let target = dir.path().join("out");

        let result = execute_move_to(&src, &target);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn execute_move_to_creates_target_dir() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("file.txt");
        std::fs::write(&src, "data").unwrap();
        let target = dir.path().join("a").join("b").join("c");

        let result = execute_move_to(&src, &target);
        assert!(result.is_ok());
        assert!(target.exists());
        assert!(target.join("file.txt").exists());
    }

    #[test]
    fn execute_move_to_unicode_filename() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("文件.txt");
        std::fs::write(&src, "内容").unwrap();
        let target = dir.path().join("目标");

        let result = execute_move_to(&src, &target);
        assert!(result.is_ok());
        let dest = result.unwrap();
        assert!(dest.exists());
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "内容");
    }

    #[test]
    fn execute_move_to_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("empty.txt");
        std::fs::write(&src, "").unwrap();
        let target = dir.path().join("out");

        let result = execute_move_to(&src, &target);
        assert!(result.is_ok());
        assert!(result.unwrap().exists());
    }

    // --- build_command ---
    #[test]
    fn build_command_no_placeholder() {
        let path = Path::new("/tmp/file.txt");
        assert_eq!(build_command("echo hello", path), "echo hello");
    }

    #[test]
    fn build_command_multiple_placeholders() {
        let path = Path::new("/tmp/f.txt");
        assert_eq!(
            build_command("cp {file} {file}.bak", path),
            "cp /tmp/f.txt /tmp/f.txt.bak"
        );
    }

    #[test]
    fn build_command_unicode_path() {
        let path = Path::new("/tmp/测试文件.txt");
        let cmd = build_command("open {file}", path);
        assert_eq!(cmd, "open /tmp/测试文件.txt");
    }

    #[test]
    fn build_command_empty_path() {
        let path = Path::new("");
        let cmd = build_command("echo {file}", path);
        assert_eq!(cmd, "echo ");
    }

    #[test]
    fn build_command_path_with_spaces() {
        let path = Path::new("/tmp/my file (copy).txt");
        let cmd = build_command("open {file}", path);
        assert_eq!(cmd, "open /tmp/my file (copy).txt");
    }

    // --- Persistence ---
    #[test]
    fn save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");
        assert!(!path.exists());
        save_auto_actions_config(&AutoActionsConfig::default(), &path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn save_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");

        let config1 = AutoActionsConfig {
            enabled: false,
            rules: vec![],
            task_overrides: vec![],
            max_results: 10,
        };
        save_auto_actions_config(&config1, &path).unwrap();

        let config2 = AutoActionsConfig {
            enabled: true,
            rules: vec![],
            task_overrides: vec![],
            max_results: 99,
        };
        save_auto_actions_config(&config2, &path).unwrap();

        let loaded = load_auto_actions_config(&path).unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.max_results, 99);
    }

    #[test]
    fn save_no_tmp_file_left() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");
        save_auto_actions_config(&AutoActionsConfig::default(), &path).unwrap();
        let tmp = path.with_extension("json.tmp");
        assert!(!tmp.exists());
    }

    #[test]
    fn load_corrupt_json_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");
        std::fs::write(&path, "not valid json{{{").unwrap();
        let result = load_auto_actions_config(&path);
        assert!(result.is_err());
    }

    #[test]
    fn load_empty_file_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");
        std::fs::write(&path, "").unwrap();
        let result = load_auto_actions_config(&path);
        assert!(result.is_err());
    }

    #[test]
    fn persistence_unicode_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");

        let config = AutoActionsConfig {
            enabled: true,
            rules: vec![AutoActionRule {
                id: "r1".to_string(),
                name: "日本語ルール".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::RunCommand {
                    command: "echo '你好' {file}".to_string(),
                }],
                tag_filter: vec!["中文标签".to_string()],
                group_filter: Some("グループ".to_string()),
                enabled: true,
                priority: 5,
            }],
            task_overrides: vec![TaskAutoAction {
                task_id: "任务_🎉".to_string(),
                actions: vec![AutoAction::OpenFile],
                trigger: AutoActionTrigger::OnCompleteOrFail,
            }],
            max_results: 30,
        };

        save_auto_actions_config(&config, &path).unwrap();
        let loaded = load_auto_actions_config(&path).unwrap();

        assert_eq!(loaded.rules[0].name, "日本語ルール");
        assert_eq!(loaded.task_overrides[0].task_id, "任务_🎉");
        assert_eq!(
            loaded.rules[0].actions[0],
            AutoAction::RunCommand {
                command: "echo '你好' {file}".to_string(),
            }
        );
    }

    #[test]
    fn persistence_pretty_json_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");

        let config = AutoActionsConfig::default();
        save_auto_actions_config(&config, &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        // Pretty JSON has newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
    }

    #[test]
    fn persistence_full_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");

        let config = AutoActionsConfig {
            enabled: true,
            rules: vec![
                AutoActionRule {
                    id: "r1".to_string(),
                    name: "Rule 1".to_string(),
                    trigger: AutoActionTrigger::OnComplete,
                    actions: vec![AutoAction::OpenFile],
                    tag_filter: vec!["a".to_string(), "b".to_string()],
                    group_filter: None,
                    enabled: true,
                    priority: 10,
                },
                AutoActionRule {
                    id: "r2".to_string(),
                    name: "Rule 2".to_string(),
                    trigger: AutoActionTrigger::OnFail,
                    actions: vec![AutoAction::MoveTo {
                        target_dir: PathBuf::from("/err"),
                    }],
                    tag_filter: vec![],
                    group_filter: Some("g".to_string()),
                    enabled: false,
                    priority: -5,
                },
            ],
            task_overrides: vec![
                TaskAutoAction {
                    task_id: "t1".to_string(),
                    actions: vec![AutoAction::OpenFile],
                    trigger: AutoActionTrigger::OnComplete,
                },
                TaskAutoAction {
                    task_id: "t2".to_string(),
                    actions: vec![AutoAction::RunCommand {
                        command: "cmd".to_string(),
                    }],
                    trigger: AutoActionTrigger::OnCompleteOrFail,
                },
            ],
            max_results: 100,
        };

        save_auto_actions_config(&config, &path).unwrap();
        let loaded = load_auto_actions_config(&path).unwrap();

        assert_eq!(loaded.enabled, true);
        assert_eq!(loaded.rules.len(), 2);
        assert_eq!(loaded.rules[0].id, "r1");
        assert_eq!(loaded.rules[1].group_filter, Some("g".to_string()));
        assert_eq!(loaded.task_overrides.len(), 2);
        assert_eq!(loaded.max_results, 100);
    }

    // --- Complex workflows ---
    #[test]
    fn workflow_complete_lifecycle() {
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());

        // 1. Enable the system
        mgr.set_enabled(true);
        assert!(mgr.config().enabled);

        // 2. Add rules
        let id1 = mgr.add_rule(AutoActionRule {
            id: "".to_string(),
            name: "Open videos".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec!["video".to_string()],
            group_filter: None,
            enabled: true,
            priority: 10,
        });
        let id2 = mgr.add_rule(AutoActionRule {
            id: "".to_string(),
            name: "Move archives".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::MoveTo {
                target_dir: PathBuf::from("/archives"),
            }],
            tag_filter: vec!["archive".to_string()],
            group_filter: None,
            enabled: true,
            priority: 5,
        });

        // 3. Set task override
        mgr.set_task_override(
            "special_task",
            vec![AutoAction::RunCommand {
                command: "notify-send done".to_string(),
            }],
            AutoActionTrigger::OnComplete,
        );

        // 4. Check actions for various tasks
        let video_actions = mgr.get_actions_for_task("t1", &["video".to_string()], None, true);
        assert_eq!(video_actions.len(), 1);
        assert_eq!(video_actions[0].0, id1);

        let archive_actions = mgr.get_actions_for_task("t2", &["archive".to_string()], None, true);
        assert_eq!(archive_actions.len(), 1);
        assert_eq!(archive_actions[0].0, id2);

        let special_actions = mgr.get_actions_for_task("special_task", &[], None, true);
        assert_eq!(special_actions.len(), 1);
        assert_eq!(special_actions[0].0, "task_override");

        // 5. Record results
        mgr.record_result(AutoActionResult {
            rule_id: id1.clone(),
            action_type: "open_file".to_string(),
            success: true,
            error: None,
            timestamp: chrono::Utc::now(),
        });

        // 6. Verify summary
        let summary = mgr.summary();
        assert_eq!(summary.total_rules, 2);
        assert_eq!(summary.enabled_rules, 2);
        assert_eq!(summary.task_overrides, 1);
        assert_eq!(summary.total_executed, 1);
        assert_eq!(summary.total_successes, 1);

        // 7. Disable a rule
        mgr.set_rule_enabled(&id1, false).unwrap();
        let video_actions = mgr.get_actions_for_task("t1", &["video".to_string()], None, true);
        assert!(video_actions.is_empty());

        // 8. Remove a rule
        mgr.remove_rule(&id2).unwrap();
        assert_eq!(mgr.list_rules().len(), 1);

        // 9. Clear history
        mgr.clear_history();
        assert_eq!(mgr.summary().total_executed, 0);
    }

    #[test]
    fn workflow_task_override_blocks_global_rules() {
        // When a task has an override, global rules should NOT apply
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules.push(AutoActionRule {
            id: "global".to_string(),
            name: "Global".to_string(),
            trigger: AutoActionTrigger::OnCompleteOrFail,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: None,
            enabled: true,
            priority: 1,
        });
        config.task_overrides.push(TaskAutoAction {
            task_id: "t1".to_string(),
            actions: vec![AutoAction::MoveTo {
                target_dir: PathBuf::from("/custom"),
            }],
            trigger: AutoActionTrigger::OnComplete,
        });
        let mgr = AutoActionsManager::new(config);

        // Task completes — override matches, global should NOT apply
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].0, "task_override");

        // Task fails — override is OnComplete only, should NOT fall through to global
        let actions = mgr.get_actions_for_task("t1", &[], None, false);
        assert!(actions.is_empty());
    }

    #[test]
    fn workflow_persist_reload_preserves_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actions.json");

        // Create and populate
        let mut mgr = AutoActionsManager::new(AutoActionsConfig::default());
        mgr.set_enabled(true);
        mgr.add_rule(AutoActionRule {
            id: "persisted_rule".to_string(),
            name: "Persisted".to_string(),
            trigger: AutoActionTrigger::OnComplete,
            actions: vec![AutoAction::OpenFile],
            tag_filter: vec![],
            group_filter: None,
            enabled: true,
            priority: 1,
        });
        mgr.set_task_override(
            "task_x",
            vec![AutoAction::OpenFile],
            AutoActionTrigger::OnComplete,
        );

        // Save
        save_auto_actions_config(mgr.config(), &path).unwrap();

        // Reload
        let loaded_config = load_auto_actions_config(&path).unwrap();
        let mgr2 = AutoActionsManager::new(loaded_config);

        assert!(mgr2.config().enabled);
        assert_eq!(mgr2.list_rules().len(), 1);
        assert_eq!(mgr2.get_rule("persisted_rule").unwrap().name, "Persisted");
        assert_eq!(mgr2.config().task_overrides.len(), 1);
    }

    #[test]
    fn workflow_multiple_rules_mixed_triggers() {
        let mut config = AutoActionsConfig::default();
        config.enabled = true;
        config.rules = vec![
            AutoActionRule {
                id: "complete_only".to_string(),
                name: "Complete".to_string(),
                trigger: AutoActionTrigger::OnComplete,
                actions: vec![AutoAction::OpenFile],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 10,
            },
            AutoActionRule {
                id: "fail_only".to_string(),
                name: "Fail".to_string(),
                trigger: AutoActionTrigger::OnFail,
                actions: vec![AutoAction::RunCommand {
                    command: "echo fail".to_string(),
                }],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 10,
            },
            AutoActionRule {
                id: "both".to_string(),
                name: "Both".to_string(),
                trigger: AutoActionTrigger::OnCompleteOrFail,
                actions: vec![AutoAction::RunCommand {
                    command: "echo both".to_string(),
                }],
                tag_filter: vec![],
                group_filter: None,
                enabled: true,
                priority: 5,
            },
        ];
        let mgr = AutoActionsManager::new(config);

        // On complete: complete_only + both
        let actions = mgr.get_actions_for_task("t1", &[], None, true);
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].0, "complete_only");
        assert_eq!(actions[1].0, "both");

        // On fail: fail_only + both
        let actions = mgr.get_actions_for_task("t1", &[], None, false);
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].0, "fail_only");
        assert_eq!(actions[1].0, "both");
    }

    // --- generate_rule_id ---
    #[test]
    fn generate_rule_id_format() {
        let id = generate_rule_id();
        assert!(id.starts_with("rule_"));
        // Should contain a timestamp and nanos part
        let parts: Vec<&str> = id.split('_').collect();
        assert_eq!(parts.len(), 3); // rule, secs, nanos
        assert!(parts[1].parse::<u64>().is_ok());
        assert!(parts[2].parse::<u64>().is_ok());
    }

    #[test]
    fn generate_rule_id_non_empty() {
        let id = generate_rule_id();
        assert!(!id.is_empty());
        assert!(id.len() > 5);
    }
}
