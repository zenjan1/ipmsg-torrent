//! Download progress milestone notifications
//!
//! Sends notifications when downloads reach configurable progress thresholds
//! (e.g., 25%, 50%, 75%, 90%). Helps users track long-running downloads
//! without needing to constantly check the UI.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// A single progress milestone threshold
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Milestone {
    /// Progress percentage threshold (1-99, must be unique and sorted)
    pub percentage: u8,
    /// Whether this milestone is enabled
    pub enabled: bool,
    /// Human-readable description (optional)
    pub description: Option<String>,
}

impl Milestone {
    /// Create a new enabled milestone
    pub fn new(percentage: u8) -> Self {
        Self {
            percentage: percentage.clamp(1, 99),
            enabled: true,
            description: None,
        }
    }

    /// Create a new milestone with description
    pub fn with_description(percentage: u8, description: impl Into<String>) -> Self {
        Self {
            percentage: percentage.clamp(1, 99),
            enabled: true,
            description: Some(description.into()),
        }
    }
}

/// Configuration for progress milestone notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressMilestoneConfig {
    /// Enable/disable all milestone notifications
    pub enabled: bool,
    /// List of milestone thresholds to notify at
    pub milestones: Vec<Milestone>,
}

impl Default for ProgressMilestoneConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            milestones: vec![
                Milestone::with_description(25, "Quarter complete"),
                Milestone::with_description(50, "Halfway done"),
                Milestone::with_description(75, "Three quarters done"),
                Milestone::with_description(90, "Almost there"),
            ],
        }
    }
}

impl ProgressMilestoneConfig {
    /// Create a disabled config
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            milestones: Vec::new(),
        }
    }

    /// Get sorted enabled milestone percentages
    pub fn enabled_percentages(&self) -> Vec<u8> {
        if !self.enabled {
            return Vec::new();
        }
        let mut pcts: Vec<u8> = self
            .milestones
            .iter()
            .filter(|m| m.enabled)
            .map(|m| m.percentage)
            .collect();
        pcts.sort();
        pcts.dedup();
        pcts
    }

    /// Add a milestone
    pub fn add_milestone(&mut self, percentage: u8) -> bool {
        let pct = percentage.clamp(1, 99);
        if self.milestones.iter().any(|m| m.percentage == pct) {
            return false; // Already exists
        }
        self.milestones.push(Milestone::new(pct));
        self.milestones.sort_by_key(|m| m.percentage);
        true
    }

    /// Remove a milestone by percentage
    pub fn remove_milestone(&mut self, percentage: u8) -> bool {
        let before = self.milestones.len();
        self.milestones.retain(|m| m.percentage != percentage);
        self.milestones.len() < before
    }

    /// Enable or disable a specific milestone
    pub fn set_milestone_enabled(&mut self, percentage: u8, enabled: bool) -> bool {
        if let Some(m) = self
            .milestones
            .iter_mut()
            .find(|m| m.percentage == percentage)
        {
            m.enabled = enabled;
            true
        } else {
            false
        }
    }
}

/// Tracks which milestones have been triggered for each task
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskMilestoneState {
    /// Task ID
    pub task_id: String,
    /// Set of milestone percentages already triggered for this task
    pub triggered: HashSet<u8>,
}

impl TaskMilestoneState {
    /// Create new tracking state for a task
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            triggered: HashSet::new(),
        }
    }

    /// Check if a milestone has been triggered
    pub fn is_triggered(&self, percentage: u8) -> bool {
        self.triggered.contains(&percentage)
    }

    /// Mark a milestone as triggered, returns true if it was newly triggered
    pub fn mark_triggered(&mut self, percentage: u8) -> bool {
        self.triggered.insert(percentage)
    }

    /// Reset all triggered milestones (e.g., when task restarts)
    pub fn reset(&mut self) {
        self.triggered.clear();
    }
}

/// Manager for progress milestone tracking across all tasks
#[derive(Debug, Default)]
pub struct ProgressMilestoneTracker {
    /// Per-task milestone tracking state
    task_states: HashMap<String, TaskMilestoneState>,
}

impl ProgressMilestoneTracker {
    /// Create a new tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Check progress and return any newly triggered milestones
    ///
    /// Returns a list of percentages that were just crossed (not previously triggered).
    pub fn check_progress(
        &mut self,
        task_id: &str,
        progress_pct: f32,
        config: &ProgressMilestoneConfig,
    ) -> Vec<u8> {
        if !config.enabled {
            return Vec::new();
        }

        let state = self
            .task_states
            .entry(task_id.to_string())
            .or_insert_with(|| TaskMilestoneState::new(task_id));

        let mut newly_triggered = Vec::new();

        for pct in config.enabled_percentages() {
            if progress_pct >= pct as f32 && !state.is_triggered(pct) {
                state.mark_triggered(pct);
                newly_triggered.push(pct);
            }
        }

        newly_triggered
    }

    /// Reset milestone tracking for a task (e.g., on restart or retry)
    pub fn reset_task(&mut self, task_id: &str) {
        if let Some(state) = self.task_states.get_mut(task_id) {
            state.reset();
        }
    }

    /// Remove tracking state for a task
    pub fn remove_task(&mut self, task_id: &str) {
        self.task_states.remove(task_id);
    }

    /// Get the milestone state for a task
    pub fn get_task_state(&self, task_id: &str) -> Option<&TaskMilestoneState> {
        self.task_states.get(task_id)
    }

    /// Get all tracked task IDs
    pub fn tracked_task_ids(&self) -> Vec<&str> {
        self.task_states.keys().map(|s| s.as_str()).collect()
    }

    /// Clear all tracking state
    pub fn clear(&mut self) {
        self.task_states.clear();
    }
}

/// Persistence functions
///
/// Save progress milestone config to disk
pub fn save_progress_milestone_config(
    config: &ProgressMilestoneConfig,
    data_dir: &Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("progress_milestone_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    // Atomic write
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, json)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load progress milestone config from disk
pub fn load_progress_milestone_config(
    data_dir: &Path,
) -> Result<Option<ProgressMilestoneConfig>, std::io::Error> {
    let path = data_dir.join("progress_milestone_config.json");
    if !path.exists() {
        return Ok(None);
    }
    let json = std::fs::read_to_string(&path)?;
    let config: ProgressMilestoneConfig = serde_json::from_str(&json)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_milestone_new() {
        let m = Milestone::new(50);
        assert_eq!(m.percentage, 50);
        assert!(m.enabled);
        assert!(m.description.is_none());
    }

    #[test]
    fn test_milestone_clamp() {
        let m = Milestone::new(0);
        assert_eq!(m.percentage, 1);
        let m = Milestone::new(100);
        assert_eq!(m.percentage, 99);
    }

    #[test]
    fn test_milestone_with_description() {
        let m = Milestone::with_description(75, "Almost done");
        assert_eq!(m.percentage, 75);
        assert_eq!(m.description.as_deref(), Some("Almost done"));
    }

    #[test]
    fn test_default_config() {
        let config = ProgressMilestoneConfig::default();
        assert!(config.enabled);
        assert_eq!(config.milestones.len(), 4);
        let pcts = config.enabled_percentages();
        assert_eq!(pcts, vec![25, 50, 75, 90]);
    }

    #[test]
    fn test_disabled_config() {
        let config = ProgressMilestoneConfig::disabled();
        assert!(!config.enabled);
        assert!(config.enabled_percentages().is_empty());
    }

    #[test]
    fn test_add_milestone() {
        let mut config = ProgressMilestoneConfig::default();
        assert!(config.add_milestone(10));
        assert!(!config.add_milestone(10)); // duplicate
        // 0 clamps to 1, which doesn't exist yet, so it succeeds
        assert!(config.add_milestone(0));
        // Now 1 exists, adding 1 again (clamped from 0) should fail
        assert!(!config.add_milestone(0));
    }

    #[test]
    fn test_remove_milestone() {
        let mut config = ProgressMilestoneConfig::default();
        assert!(config.remove_milestone(50));
        assert!(!config.remove_milestone(50)); // already removed
        let pcts = config.enabled_percentages();
        assert_eq!(pcts, vec![25, 75, 90]);
    }

    #[test]
    fn test_set_milestone_enabled() {
        let mut config = ProgressMilestoneConfig::default();
        assert!(config.set_milestone_enabled(50, false));
        let pcts = config.enabled_percentages();
        assert_eq!(pcts, vec![25, 75, 90]);
        assert!(!config.set_milestone_enabled(99, false)); // doesn't exist
    }

    #[test]
    fn test_task_milestone_state() {
        let mut state = TaskMilestoneState::new("task-1");
        assert!(!state.is_triggered(50));
        assert!(state.mark_triggered(50));
        assert!(state.is_triggered(50));
        assert!(!state.mark_triggered(50)); // already triggered
        state.reset();
        assert!(!state.is_triggered(50));
    }

    #[test]
    fn test_tracker_check_progress() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        // At 10% - no milestones triggered
        let triggered = tracker.check_progress("task-1", 10.0, &config);
        assert!(triggered.is_empty());

        // At 30% - 25% milestone triggered
        let triggered = tracker.check_progress("task-1", 30.0, &config);
        assert_eq!(triggered, vec![25]);

        // At 30% again - no new triggers
        let triggered = tracker.check_progress("task-1", 30.0, &config);
        assert!(triggered.is_empty());

        // At 55% - 50% milestone triggered (25% was already triggered)
        let triggered = tracker.check_progress("task-1", 55.0, &config);
        assert_eq!(triggered, vec![50]);

        // At 80% - 75% milestone triggered
        let triggered = tracker.check_progress("task-1", 80.0, &config);
        assert_eq!(triggered, vec![75]);

        // At 95% - 90% milestone triggered
        let triggered = tracker.check_progress("task-1", 95.0, &config);
        assert_eq!(triggered, vec![90]);

        // At 100% - no more milestones (99 is max)
        let triggered = tracker.check_progress("task-1", 100.0, &config);
        assert!(triggered.is_empty());
    }

    #[test]
    fn test_tracker_disabled() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::disabled();

        let triggered = tracker.check_progress("task-1", 50.0, &config);
        assert!(triggered.is_empty());
    }

    #[test]
    fn test_tracker_reset_task() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        // Trigger 25%
        let triggered = tracker.check_progress("task-1", 30.0, &config);
        assert_eq!(triggered, vec![25]);

        // Reset
        tracker.reset_task("task-1");

        // 25% should trigger again
        let triggered = tracker.check_progress("task-1", 30.0, &config);
        assert_eq!(triggered, vec![25]);
    }

    #[test]
    fn test_tracker_remove_task() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        tracker.check_progress("task-1", 30.0, &config);
        assert!(tracker.get_task_state("task-1").is_some());

        tracker.remove_task("task-1");
        assert!(tracker.get_task_state("task-1").is_none());
    }

    #[test]
    fn test_tracker_multiple_tasks() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        // Task 1 reaches 50%
        let t1 = tracker.check_progress("task-1", 55.0, &config);
        assert_eq!(t1, vec![25, 50]);

        // Task 2 reaches 30%
        let t2 = tracker.check_progress("task-2", 30.0, &config);
        assert_eq!(t2, vec![25]);

        // Task 1 at 80%
        let t1 = tracker.check_progress("task-1", 80.0, &config);
        assert_eq!(t1, vec![75]);

        // Task 2 at 30% again - nothing new
        let t2 = tracker.check_progress("task-2", 30.0, &config);
        assert!(t2.is_empty());
    }

    #[test]
    fn test_tracker_clear() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        tracker.check_progress("task-1", 50.0, &config);
        tracker.check_progress("task-2", 50.0, &config);
        assert_eq!(tracker.tracked_task_ids().len(), 2);

        tracker.clear();
        assert!(tracker.tracked_task_ids().is_empty());
    }

    #[test]
    fn test_persistence() {
        let dir = std::env::temp_dir().join("ipmsg_test_progress_milestone");
        let _ = fs::create_dir_all(&dir);

        let config = ProgressMilestoneConfig::default();
        save_progress_milestone_config(&config, &dir).unwrap();

        let loaded = load_progress_milestone_config(&dir).unwrap().unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.milestones.len(), 4);

        // Cleanup
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_persistence_missing_file() {
        let dir = std::env::temp_dir().join("ipmsg_test_progress_milestone_missing");
        let _ = fs::create_dir_all(&dir);

        let result = load_progress_milestone_config(&dir).unwrap();
        assert!(result.is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_persistence_corrupt_file() {
        let dir = std::env::temp_dir().join("ipmsg_test_progress_milestone_corrupt");
        let _ = fs::create_dir_all(&dir);

        let path = dir.join("progress_milestone_config.json");
        fs::write(&path, "not json").unwrap();

        let result = load_progress_milestone_config(&dir);
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_enabled_percentages_sorted_deduped() {
        let mut config = ProgressMilestoneConfig {
            enabled: true,
            milestones: vec![
                Milestone::new(75),
                Milestone::new(25),
                Milestone::new(75), // duplicate
                Milestone::new(50),
            ],
        };
        // Add a disabled one
        config.milestones.push(Milestone {
            percentage: 90,
            enabled: false,
            description: None,
        });

        let pcts = config.enabled_percentages();
        assert_eq!(pcts, vec![25, 50, 75]);
    }

    #[test]
    fn test_tracker_jumps_over_milestone() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        // Jump directly from 0% to 80% - should trigger 25, 50, 75
        let triggered = tracker.check_progress("task-1", 80.0, &config);
        assert_eq!(triggered, vec![25, 50, 75]);
    }

    #[test]
    fn test_tracker_exact_boundary() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        // Exactly at 25.0% - should trigger
        let triggered = tracker.check_progress("task-1", 25.0, &config);
        assert_eq!(triggered, vec![25]);

        // Just below 50% - should not trigger
        let triggered = tracker.check_progress("task-1", 49.9, &config);
        assert!(triggered.is_empty());

        // Exactly at 50% - should trigger
        let triggered = tracker.check_progress("task-1", 50.0, &config);
        assert_eq!(triggered, vec![50]);
    }

    // ===== Milestone boundary =====

    #[test]
    fn test_milestone_new_boundary_values() {
        let m1 = Milestone::new(1);
        assert_eq!(m1.percentage, 1);

        let m99 = Milestone::new(99);
        assert_eq!(m99.percentage, 99);

        let m0 = Milestone::new(0);
        assert_eq!(m0.percentage, 1); // clamped

        let m100 = Milestone::new(100);
        assert_eq!(m100.percentage, 99); // clamped
    }

    // ===== Progress milestone serde =====

    #[test]
    fn test_milestone_serde_roundtrip() {
        let m = Milestone::with_description(50, "Halfway");
        let json = serde_json::to_string(&m).unwrap();
        let d: Milestone = serde_json::from_str(&json).unwrap();
        assert_eq!(d.percentage, 50);
        assert_eq!(d.description.as_deref(), Some("Halfway"));
        assert!(d.enabled);
    }

    #[test]
    fn test_progress_milestone_config_serde_roundtrip() {
        let config = ProgressMilestoneConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let d: ProgressMilestoneConfig = serde_json::from_str(&json).unwrap();
        assert!(d.enabled);
        assert_eq!(d.milestones.len(), 4);
    }

    #[test]
    fn test_task_milestone_state_serde_roundtrip() {
        let mut state = TaskMilestoneState::new("task-1");
        state.mark_triggered(25);
        state.mark_triggered(50);
        let json = serde_json::to_string(&state).unwrap();
        let d: TaskMilestoneState = serde_json::from_str(&json).unwrap();
        assert_eq!(d.task_id, "task-1");
        assert!(d.is_triggered(25));
        assert!(d.is_triggered(50));
        assert!(!d.is_triggered(75));
    }

    // ===== Progress milestone persistence =====

    #[test]
    fn test_persistence_no_tmp_leftover() {
        let dir = std::env::temp_dir().join("ipmsg_test_milestone_no_tmp");
        let _ = fs::create_dir_all(&dir);

        let config = ProgressMilestoneConfig::default();
        save_progress_milestone_config(&config, &dir).unwrap();

        let tmp = dir.join("progress_milestone_config.json.tmp");
        assert!(!tmp.exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_persistence_overwrite() {
        let dir = std::env::temp_dir().join("ipmsg_test_milestone_overwrite");
        let _ = fs::create_dir_all(&dir);

        let config1 = ProgressMilestoneConfig::default();
        save_progress_milestone_config(&config1, &dir).unwrap();

        let config2 = ProgressMilestoneConfig::disabled();
        save_progress_milestone_config(&config2, &dir).unwrap();

        let loaded = load_progress_milestone_config(&dir).unwrap().unwrap();
        assert!(!loaded.enabled);
        assert!(loaded.milestones.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    // ===== Progress milestone tracker edge cases =====

    #[test]
    fn test_tracker_remove_nonexistent_task() {
        let mut tracker = ProgressMilestoneTracker::new();
        // Should not panic
        tracker.remove_task("nonexistent");
    }

    #[test]
    fn test_tracker_reset_nonexistent_task() {
        let mut tracker = ProgressMilestoneTracker::new();
        // Should not panic
        tracker.reset_task("nonexistent");
    }

    #[test]
    fn test_tracker_negative_progress() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();
        // Negative progress should not trigger any milestones
        let triggered = tracker.check_progress("task-1", -10.0, &config);
        assert!(triggered.is_empty());
    }

    #[test]
    fn test_tracker_over_100_progress() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();
        // Over 100% should trigger all milestones
        let triggered = tracker.check_progress("task-1", 150.0, &config);
        assert_eq!(triggered, vec![25, 50, 75, 90]);
    }

    #[test]
    fn test_tracker_nan_progress() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();
        // NaN comparisons should not trigger (NaN >= x is false)
        let triggered = tracker.check_progress("task-1", f32::NAN, &config);
        assert!(triggered.is_empty());
    }

    #[test]
    fn test_tracker_empty_config_milestones() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig {
            enabled: true,
            milestones: vec![],
        };
        let triggered = tracker.check_progress("task-1", 50.0, &config);
        assert!(triggered.is_empty());
    }

    #[test]
    fn test_tracker_custom_milestones() {
        let mut tracker = ProgressMilestoneTracker::new();
        let mut config = ProgressMilestoneConfig {
            enabled: true,
            milestones: vec![],
        };
        config.add_milestone(10);
        config.add_milestone(33);
        config.add_milestone(66);
        config.add_milestone(99);

        let triggered = tracker.check_progress("task-1", 35.0, &config);
        assert_eq!(triggered, vec![10, 33]);

        let triggered = tracker.check_progress("task-1", 70.0, &config);
        assert_eq!(triggered, vec![66]);

        let triggered = tracker.check_progress("task-1", 100.0, &config);
        assert_eq!(triggered, vec![99]);
    }

    #[test]
    fn test_tracker_get_task_state() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        assert!(tracker.get_task_state("task-1").is_none());

        tracker.check_progress("task-1", 30.0, &config);
        let state = tracker.get_task_state("task-1").unwrap();
        assert!(state.is_triggered(25));
        assert!(!state.is_triggered(50));
    }

    #[test]
    fn test_tracker_tracked_task_ids() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        assert!(tracker.tracked_task_ids().is_empty());

        tracker.check_progress("task-a", 30.0, &config);
        tracker.check_progress("task-b", 30.0, &config);

        let mut ids = tracker.tracked_task_ids();
        ids.sort();
        assert_eq!(ids, vec!["task-a", "task-b"]);
    }

    // ===== Progress milestone config methods =====

    #[test]
    fn test_add_milestone_sorted() {
        let mut config = ProgressMilestoneConfig {
            enabled: true,
            milestones: vec![],
        };
        config.add_milestone(75);
        config.add_milestone(25);
        config.add_milestone(50);

        // Should be sorted
        let pcts: Vec<u8> = config.milestones.iter().map(|m| m.percentage).collect();
        assert_eq!(pcts, vec![25, 50, 75]);
    }

    #[test]
    fn test_remove_milestone_not_found() {
        let mut config = ProgressMilestoneConfig::default();
        assert!(!config.remove_milestone(42)); // doesn't exist
    }

    #[test]
    fn test_set_milestone_enabled_roundtrip() {
        let mut config = ProgressMilestoneConfig::default();
        assert!(config.set_milestone_enabled(25, false));
        let pcts = config.enabled_percentages();
        assert_eq!(pcts, vec![50, 75, 90]);

        assert!(config.set_milestone_enabled(25, true));
        let pcts = config.enabled_percentages();
        assert_eq!(pcts, vec![25, 50, 75, 90]);
    }

    #[test]
    fn test_disabled_config_has_no_milestones() {
        let config = ProgressMilestoneConfig::disabled();
        assert!(!config.enabled);
        assert!(config.milestones.is_empty());
    }

    // ===== Unicode in task IDs =====

    #[test]
    fn test_tracker_unicode_task_id() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        tracker.check_progress("任务-中文", 50.0, &config);
        assert!(tracker.get_task_state("任务-中文").is_some());
        assert!(
            tracker
                .get_task_state("任务-中文")
                .unwrap()
                .is_triggered(25)
        );
    }

    #[test]
    fn test_tracker_emoji_task_id() {
        let mut tracker = ProgressMilestoneTracker::new();
        let config = ProgressMilestoneConfig::default();

        tracker.check_progress("task-🚀", 80.0, &config);
        let state = tracker.get_task_state("task-🚀").unwrap();
        assert!(state.is_triggered(25));
        assert!(state.is_triggered(50));
        assert!(state.is_triggered(75));
    }
}
