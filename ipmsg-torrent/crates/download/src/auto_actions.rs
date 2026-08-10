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
}
