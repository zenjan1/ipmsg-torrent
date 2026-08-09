//! Task Snooze - pause downloads until a specific time, then auto-resume.
//!
//! Allows users to "snooze" a download task, putting it in a special Paused state
//! with a scheduled wake-up time. When the snooze expires, the task automatically
//! transitions back to Queued.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Error type for task snooze operations.
#[derive(Debug, thiserror::Error)]
pub enum TaskSnoozeError {
    #[error("task {0} is not snoozed")]
    NotSnoozed(String),
    #[error("invalid snooze time: must be in the future")]
    InvalidTime,
    #[error("persistence error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// State of a snoozed task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnoozeState {
    /// The task ID that is snoozed.
    pub task_id: String,
    /// When the snooze period ends and the task should resume.
    pub snoozed_until: DateTime<Utc>,
    /// When the snooze was created.
    pub snoozed_at: DateTime<Utc>,
    /// Optional reason/note for snoozing.
    pub reason: Option<String>,
}

/// Configuration for the task snooze system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSnoozeConfig {
    /// Whether snooze functionality is enabled.
    pub enabled: bool,
    /// Maximum number of concurrently snoozed tasks (0 = unlimited).
    pub max_snoozed: u32,
    /// Maximum snooze duration in seconds (0 = unlimited).
    /// Prevents accidentally snoozing for absurdly long periods.
    pub max_duration_secs: u64,
}

impl Default for TaskSnoozeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_snoozed: 0,
            max_duration_secs: 30 * 24 * 3600, // 30 days max
        }
    }
}

/// Persisted snooze data (all active snoozes + config).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskSnoozeData {
    pub config: TaskSnoozeConfig,
    pub snoozed_tasks: Vec<SnoozeState>,
}

/// Manager for task snooze operations.
#[derive(Debug, Clone)]
pub struct TaskSnoozeManager {
    config: TaskSnoozeConfig,
    snoozed: HashMap<String, SnoozeState>,
}

impl TaskSnoozeManager {
    /// Create a new manager with default configuration.
    pub fn new() -> Self {
        Self {
            config: TaskSnoozeConfig::default(),
            snoozed: HashMap::new(),
        }
    }

    /// Create from persisted data.
    pub fn from_data(data: TaskSnoozeData) -> Self {
        let mut snoozed = HashMap::new();
        for state in data.snoozed_tasks {
            snoozed.insert(state.task_id.clone(), state);
        }
        Self {
            config: data.config,
            snoozed,
        }
    }

    /// Get current configuration.
    pub fn config(&self) -> &TaskSnoozeConfig {
        &self.config
    }

    /// Update configuration.
    pub fn set_config(&mut self, config: TaskSnoozeConfig) {
        self.config = config;
    }

    /// Snooze a task until the given time.
    pub fn snooze_task(
        &mut self,
        task_id: String,
        until: DateTime<Utc>,
        reason: Option<String>,
    ) -> Result<SnoozeState, TaskSnoozeError> {
        if !self.config.enabled {
            return Err(TaskSnoozeError::NotSnoozed(
                "snooze functionality is disabled".to_string(),
            ));
        }

        let now = Utc::now();
        if until <= now {
            return Err(TaskSnoozeError::InvalidTime);
        }

        // Check max duration
        if self.config.max_duration_secs > 0 {
            let duration = (until - now).num_seconds() as u64;
            if duration > self.config.max_duration_secs {
                return Err(TaskSnoozeError::InvalidTime);
            }
        }

        // Check max snoozed count
        if self.config.max_snoozed > 0 && self.snoozed.len() >= self.config.max_snoozed as usize {
            // Allow if this task is already snoozed (updating)
            if !self.snoozed.contains_key(&task_id) {
                return Err(TaskSnoozeError::NotSnoozed(format!(
                    "maximum snoozed tasks ({}) reached",
                    self.config.max_snoozed
                )));
            }
        }

        let state = SnoozeState {
            task_id: task_id.clone(),
            snoozed_until: until,
            snoozed_at: now,
            reason,
        };

        self.snoozed.insert(task_id, state.clone());
        Ok(state)
    }

    /// Remove snooze from a task (unsnooze/wake up immediately).
    pub fn unsnooze_task(&mut self, task_id: &str) -> Result<SnoozeState, TaskSnoozeError> {
        self.snoozed
            .remove(task_id)
            .ok_or_else(|| TaskSnoozeError::NotSnoozed(task_id.to_string()))
    }

    /// Check if a task is currently snoozed.
    pub fn is_snoozed(&self, task_id: &str) -> bool {
        self.snoozed.contains_key(task_id)
    }

    /// Get snooze state for a task.
    pub fn get_snooze_state(&self, task_id: &str) -> Option<&SnoozeState> {
        self.snoozed.get(task_id)
    }

    /// Get all currently snoozed tasks.
    pub fn list_snoozed(&self) -> Vec<&SnoozeState> {
        let mut states: Vec<_> = self.snoozed.values().collect();
        states.sort_by_key(|s| s.snoozed_until);
        states
    }

    /// Get number of snoozed tasks.
    pub fn snoozed_count(&self) -> usize {
        self.snoozed.len()
    }

    /// Collect tasks whose snooze has expired (should be resumed).
    /// These tasks are NOT removed from the snooze map; call `clear_expired` after processing.
    pub fn collect_expired(&self) -> Vec<SnoozeState> {
        let now = Utc::now();
        self.snoozed
            .values()
            .filter(|s| s.snoozed_until <= now)
            .cloned()
            .collect()
    }

    /// Remove expired snoozes from the map (after they've been processed).
    pub fn clear_expired(&mut self) -> Vec<String> {
        let now = Utc::now();
        let expired_ids: Vec<String> = self
            .snoozed
            .iter()
            .filter(|(_, s)| s.snoozed_until <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired_ids {
            self.snoozed.remove(id);
        }
        expired_ids
    }

    /// Remove a task from snooze tracking (e.g., when task is deleted).
    pub fn remove_task(&mut self, task_id: &str) {
        self.snoozed.remove(task_id);
    }

    /// Convert to persistable data.
    pub fn to_data(&self) -> TaskSnoozeData {
        TaskSnoozeData {
            config: self.config.clone(),
            snoozed_tasks: self.snoozed.values().cloned().collect(),
        }
    }
}

/// Save snooze data to disk (atomic write).
pub async fn save_task_snooze_data(
    data: &TaskSnoozeData,
    data_dir: &Path,
) -> Result<(), TaskSnoozeError> {
    let path = data_dir.join("task_snooze.json");
    let json = serde_json::to_string_pretty(data)?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, json.as_bytes()).await?;
    fs::rename(&tmp_path, &path).await?;
    Ok(())
}

/// Load snooze data from disk.
pub async fn load_task_snooze_data(data_dir: &Path) -> Result<TaskSnoozeData, TaskSnoozeError> {
    let path = data_dir.join("task_snooze.json");
    if !path.exists() {
        return Ok(TaskSnoozeData::default());
    }
    let content = fs::read_to_string(&path).await?;
    let data: TaskSnoozeData = serde_json::from_str(&content)?;
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_snooze_config_default() {
        let config = TaskSnoozeConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_snoozed, 0);
        assert_eq!(config.max_duration_secs, 30 * 24 * 3600);
    }

    #[test]
    fn test_snooze_task_success() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(2);
        let result = mgr.snooze_task("task-1".to_string(), until, Some("sleep".to_string()));
        assert!(result.is_ok());
        let state = result.unwrap();
        assert_eq!(state.task_id, "task-1");
        assert_eq!(state.reason, Some("sleep".to_string()));
        assert!(mgr.is_snoozed("task-1"));
    }

    #[test]
    fn test_snooze_task_past_time() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() - Duration::hours(1);
        let result = mgr.snooze_task("task-1".to_string(), until, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskSnoozeError::InvalidTime));
    }

    #[test]
    fn test_snooze_task_disabled() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.enabled = false;
        let until = Utc::now() + Duration::hours(1);
        let result = mgr.snooze_task("task-1".to_string(), until, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_snooze_max_duration() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_duration_secs = 3600; // 1 hour max
        let until = Utc::now() + Duration::hours(2);
        let result = mgr.snooze_task("task-1".to_string(), until, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskSnoozeError::InvalidTime));
    }

    #[test]
    fn test_snooze_max_count() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_snoozed = 2;
        let until = Utc::now() + Duration::hours(1);
        assert!(mgr.snooze_task("task-1".to_string(), until, None).is_ok());
        assert!(mgr.snooze_task("task-2".to_string(), until, None).is_ok());
        let result = mgr.snooze_task("task-3".to_string(), until, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_snooze_update_existing() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_snoozed = 1;
        let until1 = Utc::now() + Duration::hours(1);
        let until2 = Utc::now() + Duration::hours(3);
        assert!(mgr.snooze_task("task-1".to_string(), until1, None).is_ok());
        // Updating existing task should work even at max
        assert!(mgr.snooze_task("task-1".to_string(), until2, None).is_ok());
    }

    #[test]
    fn test_unsnooze_task() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        assert!(mgr.is_snoozed("task-1"));

        let state = mgr.unsnooze_task("task-1").unwrap();
        assert_eq!(state.task_id, "task-1");
        assert!(!mgr.is_snoozed("task-1"));
    }

    #[test]
    fn test_unsnooze_not_snoozed() {
        let mut mgr = TaskSnoozeManager::new();
        let result = mgr.unsnooze_task("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_snoozed_sorted() {
        let mut mgr = TaskSnoozeManager::new();
        let until1 = Utc::now() + Duration::hours(3);
        let until2 = Utc::now() + Duration::hours(1);
        let until3 = Utc::now() + Duration::hours(2);
        mgr.snooze_task("task-1".to_string(), until1, None).unwrap();
        mgr.snooze_task("task-2".to_string(), until2, None).unwrap();
        mgr.snooze_task("task-3".to_string(), until3, None).unwrap();

        let list = mgr.list_snoozed();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].task_id, "task-2"); // earliest
        assert_eq!(list[1].task_id, "task-3");
        assert_eq!(list[2].task_id, "task-1"); // latest
    }

    #[test]
    fn test_collect_expired() {
        let mut mgr = TaskSnoozeManager::new();
        // Create a snooze that expires immediately (1 second from now)
        let soon = Utc::now() + Duration::seconds(1);
        let future = Utc::now() + Duration::hours(1);
        mgr.snooze_task("expired".to_string(), soon, None).unwrap();
        mgr.snooze_task("active".to_string(), future, None).unwrap();

        // Wait for the first snooze to expire
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let expired = mgr.collect_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].task_id, "expired");
    }

    #[test]
    fn test_clear_expired() {
        let mut mgr = TaskSnoozeManager::new();
        // Create a snooze that expires immediately (1 second from now)
        let soon = Utc::now() + Duration::seconds(1);
        let future = Utc::now() + Duration::hours(1);
        mgr.snooze_task("expired".to_string(), soon, None).unwrap();
        mgr.snooze_task("active".to_string(), future, None).unwrap();

        // Wait for the first snooze to expire
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let cleared = mgr.clear_expired();
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0], "expired");
        assert!(!mgr.is_snoozed("expired"));
        assert!(mgr.is_snoozed("active"));
    }

    #[test]
    fn test_remove_task() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        mgr.remove_task("task-1");
        assert!(!mgr.is_snoozed("task-1"));
    }

    #[test]
    fn test_snoozed_count() {
        let mut mgr = TaskSnoozeManager::new();
        assert_eq!(mgr.snoozed_count(), 0);
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        assert_eq!(mgr.snoozed_count(), 1);
    }

    #[test]
    fn test_to_data_roundtrip() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_snoozed = 5;
        let until = Utc::now() + Duration::hours(2);
        mgr.snooze_task("task-1".to_string(), until, Some("test".to_string()))
            .unwrap();

        let data = mgr.to_data();
        assert_eq!(data.config.max_snoozed, 5);
        assert_eq!(data.snoozed_tasks.len(), 1);
        assert_eq!(data.snoozed_tasks[0].task_id, "task-1");

        // Reconstruct from data
        let mgr2 = TaskSnoozeManager::from_data(data);
        assert_eq!(mgr2.config().max_snoozed, 5);
        assert!(mgr2.is_snoozed("task-1"));
    }

    #[tokio::test]
    async fn test_persistence_roundtrip() {
        let dir = tempdir().unwrap();
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_snoozed = 10;
        let until = Utc::now() + Duration::hours(3);
        mgr.snooze_task("persist-1".to_string(), until, Some("note".to_string()))
            .unwrap();

        let data = mgr.to_data();
        save_task_snooze_data(&data, dir.path()).await.unwrap();

        let loaded = load_task_snooze_data(dir.path()).await.unwrap();
        assert_eq!(loaded.config.max_snoozed, 10);
        assert_eq!(loaded.snoozed_tasks.len(), 1);
        assert_eq!(loaded.snoozed_tasks[0].task_id, "persist-1");
        assert_eq!(loaded.snoozed_tasks[0].reason, Some("note".to_string()));
    }

    #[tokio::test]
    async fn test_persistence_missing_file() {
        let dir = tempdir().unwrap();
        let loaded = load_task_snooze_data(dir.path()).await.unwrap();
        assert_eq!(loaded.snoozed_tasks.len(), 0);
        assert!(loaded.config.enabled);
    }

    #[test]
    fn test_get_snooze_state() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, Some("reason".to_string()))
            .unwrap();

        let state = mgr.get_snooze_state("task-1").unwrap();
        assert_eq!(state.task_id, "task-1");
        assert_eq!(state.reason, Some("reason".to_string()));
        assert!(mgr.get_snooze_state("nonexistent").is_none());
    }
}
