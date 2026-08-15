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

impl Default for TaskSnoozeManager {
    fn default() -> Self {
        Self::new()
    }
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

    // ===== Serialization tests =====

    #[test]
    fn test_snooze_state_serde_roundtrip() {
        let state = SnoozeState {
            task_id: "task-1".to_string(),
            snoozed_until: Utc::now() + Duration::hours(1),
            snoozed_at: Utc::now(),
            reason: Some("testing".to_string()),
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: SnoozeState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "task-1");
        assert_eq!(deserialized.reason, Some("testing".to_string()));
    }

    #[test]
    fn test_snooze_state_serde_none_reason() {
        let state = SnoozeState {
            task_id: "task-2".to_string(),
            snoozed_until: Utc::now() + Duration::hours(1),
            snoozed_at: Utc::now(),
            reason: None,
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: SnoozeState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.reason, None);
    }

    #[test]
    fn test_snooze_state_serde_unicode() {
        let state = SnoozeState {
            task_id: "任务-🔥".to_string(),
            snoozed_until: Utc::now() + Duration::hours(1),
            snoozed_at: Utc::now(),
            reason: Some("休息中 💤".to_string()),
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: SnoozeState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "任务-🔥");
        assert_eq!(deserialized.reason, Some("休息中 💤".to_string()));
    }

    #[test]
    fn test_snooze_state_serde_extra_fields_ignored() {
        let json = r#"{
            "task_id": "task-1",
            "snoozed_until": "2030-01-01T00:00:00Z",
            "snoozed_at": "2026-01-01T00:00:00Z",
            "reason": null,
            "extra_field": "ignored"
        }"#;
        let state: SnoozeState = serde_json::from_str(json).unwrap();
        assert_eq!(state.task_id, "task-1");
    }

    #[test]
    fn test_task_snooze_config_serde_roundtrip() {
        let config = TaskSnoozeConfig {
            enabled: false,
            max_snoozed: 42,
            max_duration_secs: 86400,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TaskSnoozeConfig = serde_json::from_str(&json).unwrap();
        assert!(!deserialized.enabled);
        assert_eq!(deserialized.max_snoozed, 42);
        assert_eq!(deserialized.max_duration_secs, 86400);
    }

    #[test]
    fn test_task_snooze_config_serde_default_values() {
        let config = TaskSnoozeConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TaskSnoozeConfig = serde_json::from_str(&json).unwrap();
        assert!(deserialized.enabled);
        assert_eq!(deserialized.max_snoozed, 0);
        assert_eq!(deserialized.max_duration_secs, 30 * 24 * 3600);
    }

    #[test]
    fn test_task_snooze_config_serde_extra_fields_ignored() {
        let json = r#"{
            "enabled": true,
            "max_snoozed": 10,
            "max_duration_secs": 3600,
            "unknown_field": 123
        }"#;
        let config: TaskSnoozeConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_snoozed, 10);
    }

    #[test]
    fn test_task_snooze_config_pretty_serde() {
        let config = TaskSnoozeConfig {
            enabled: true,
            max_snoozed: 5,
            max_duration_secs: 7200,
        };
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let deserialized: TaskSnoozeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_snoozed, 5);
    }

    #[test]
    fn test_task_snooze_data_serde_roundtrip() {
        let data = TaskSnoozeData {
            config: TaskSnoozeConfig {
                enabled: true,
                max_snoozed: 3,
                max_duration_secs: 3600,
            },
            snoozed_tasks: vec![SnoozeState {
                task_id: "task-1".to_string(),
                snoozed_until: Utc::now() + Duration::hours(1),
                snoozed_at: Utc::now(),
                reason: Some("test".to_string()),
            }],
        };
        let json = serde_json::to_string(&data).unwrap();
        let deserialized: TaskSnoozeData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.config.max_snoozed, 3);
        assert_eq!(deserialized.snoozed_tasks.len(), 1);
    }

    #[test]
    fn test_task_snooze_data_default() {
        let data = TaskSnoozeData::default();
        assert!(data.config.enabled);
        assert_eq!(data.snoozed_tasks.len(), 0);
    }

    #[test]
    fn test_task_snooze_data_empty_snoozed_tasks() {
        let data = TaskSnoozeData {
            config: TaskSnoozeConfig::default(),
            snoozed_tasks: vec![],
        };
        let json = serde_json::to_string(&data).unwrap();
        let deserialized: TaskSnoozeData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.snoozed_tasks.len(), 0);
    }

    // ===== Error Display tests =====

    #[test]
    fn test_error_not_snoozed_display() {
        let err = TaskSnoozeError::NotSnoozed("task-1".to_string());
        assert_eq!(err.to_string(), "task task-1 is not snoozed");
    }

    #[test]
    fn test_error_invalid_time_display() {
        let err = TaskSnoozeError::InvalidTime;
        assert_eq!(
            err.to_string(),
            "invalid snooze time: must be in the future"
        );
    }

    #[test]
    fn test_error_io_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = TaskSnoozeError::Io(io_err);
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn test_error_serialize_display() {
        let json_err = serde_json::from_str::<TaskSnoozeData>("invalid").unwrap_err();
        let err = TaskSnoozeError::Serialize(json_err);
        assert!(err.to_string().contains("serialization error"));
    }

    #[test]
    fn test_error_debug_trait() {
        let err1 = TaskSnoozeError::NotSnoozed("task-1".to_string());
        let err2 = TaskSnoozeError::InvalidTime;
        let debug1 = format!("{:?}", err1);
        let debug2 = format!("{:?}", err2);
        assert!(debug1.contains("NotSnoozed"));
        assert!(debug2.contains("InvalidTime"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: TaskSnoozeError = io_err.into();
        assert!(matches!(err, TaskSnoozeError::Io(_)));
    }

    #[test]
    fn test_error_from_serde_json() {
        let json_err = serde_json::from_str::<TaskSnoozeData>("bad json").unwrap_err();
        let err: TaskSnoozeError = json_err.into();
        assert!(matches!(err, TaskSnoozeError::Serialize(_)));
    }

    // ===== Struct traits =====

    #[test]
    fn test_snooze_state_clone() {
        let state = SnoozeState {
            task_id: "task-1".to_string(),
            snoozed_until: Utc::now() + Duration::hours(1),
            snoozed_at: Utc::now(),
            reason: Some("test".to_string()),
        };
        let cloned = state.clone();
        assert_eq!(cloned.task_id, state.task_id);
        assert_eq!(cloned.reason, state.reason);
    }

    #[test]
    fn test_snooze_state_debug() {
        let state = SnoozeState {
            task_id: "task-1".to_string(),
            snoozed_until: Utc::now() + Duration::hours(1),
            snoozed_at: Utc::now(),
            reason: None,
        };
        let debug = format!("{:?}", state);
        assert!(debug.contains("task-1"));
    }

    #[test]
    fn test_task_snooze_config_clone() {
        let config = TaskSnoozeConfig {
            enabled: false,
            max_snoozed: 10,
            max_duration_secs: 7200,
        };
        let cloned = config.clone();
        assert!(!cloned.enabled);
        assert_eq!(cloned.max_snoozed, 10);
        assert_eq!(cloned.max_duration_secs, 7200);
    }

    #[test]
    fn test_task_snooze_config_debug() {
        let config = TaskSnoozeConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("enabled"));
        assert!(debug.contains("max_snoozed"));
    }

    #[test]
    fn test_task_snooze_data_clone() {
        let data = TaskSnoozeData {
            config: TaskSnoozeConfig::default(),
            snoozed_tasks: vec![SnoozeState {
                task_id: "task-1".to_string(),
                snoozed_until: Utc::now() + Duration::hours(1),
                snoozed_at: Utc::now(),
                reason: None,
            }],
        };
        let cloned = data.clone();
        assert_eq!(cloned.snoozed_tasks.len(), 1);
    }

    #[test]
    fn test_task_snooze_data_debug() {
        let data = TaskSnoozeData::default();
        let debug = format!("{:?}", data);
        assert!(debug.contains("config"));
        assert!(debug.contains("snoozed_tasks"));
    }

    #[test]
    fn test_task_snooze_manager_clone() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        let cloned = mgr.clone();
        assert!(cloned.is_snoozed("task-1"));
        assert_eq!(cloned.snoozed_count(), 1);
    }

    #[test]
    fn test_task_snooze_manager_clone_independence() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        let mut cloned = mgr.clone();
        cloned.remove_task("task-1");
        assert!(mgr.is_snoozed("task-1"));
        assert!(!cloned.is_snoozed("task-1"));
    }

    #[test]
    fn test_task_snooze_manager_debug() {
        let mgr = TaskSnoozeManager::new();
        let debug = format!("{:?}", mgr);
        assert!(debug.contains("config"));
        assert!(debug.contains("snoozed"));
    }

    // ===== Manager boundary tests =====

    #[test]
    fn test_manager_default_trait() {
        let mgr = TaskSnoozeManager::default();
        assert_eq!(mgr.snoozed_count(), 0);
        assert!(mgr.config().enabled);
        assert_eq!(mgr.config().max_snoozed, 0);
        assert_eq!(mgr.config().max_duration_secs, 30 * 24 * 3600);
    }

    #[test]
    fn test_manager_new_equals_default_config() {
        let mgr = TaskSnoozeManager::new();
        let default_config = TaskSnoozeConfig::default();
        assert_eq!(mgr.config().enabled, default_config.enabled);
        assert_eq!(mgr.config().max_snoozed, default_config.max_snoozed);
        assert_eq!(
            mgr.config().max_duration_secs,
            default_config.max_duration_secs
        );
    }

    #[test]
    fn test_manager_default_equals_new() {
        let new_mgr = TaskSnoozeManager::new();
        let default_mgr = TaskSnoozeManager::default();
        assert_eq!(new_mgr.config().enabled, default_mgr.config().enabled);
        assert_eq!(
            new_mgr.config().max_snoozed,
            default_mgr.config().max_snoozed
        );
        assert_eq!(new_mgr.snoozed_count(), default_mgr.snoozed_count());
    }

    #[test]
    fn test_from_data_empty() {
        let data = TaskSnoozeData::default();
        let mgr = TaskSnoozeManager::from_data(data);
        assert_eq!(mgr.snoozed_count(), 0);
        assert!(mgr.config().enabled);
    }

    #[test]
    fn test_from_data_multiple_tasks() {
        let data = TaskSnoozeData {
            config: TaskSnoozeConfig::default(),
            snoozed_tasks: vec![
                SnoozeState {
                    task_id: "task-1".to_string(),
                    snoozed_until: Utc::now() + Duration::hours(1),
                    snoozed_at: Utc::now(),
                    reason: None,
                },
                SnoozeState {
                    task_id: "task-2".to_string(),
                    snoozed_until: Utc::now() + Duration::hours(2),
                    snoozed_at: Utc::now(),
                    reason: None,
                },
            ],
        };
        let mgr = TaskSnoozeManager::from_data(data);
        assert_eq!(mgr.snoozed_count(), 2);
        assert!(mgr.is_snoozed("task-1"));
        assert!(mgr.is_snoozed("task-2"));
    }

    #[test]
    fn test_set_config_updates() {
        let mut mgr = TaskSnoozeManager::new();
        let new_config = TaskSnoozeConfig {
            enabled: false,
            max_snoozed: 100,
            max_duration_secs: 60,
        };
        mgr.set_config(new_config);
        assert!(!mgr.config().enabled);
        assert_eq!(mgr.config().max_snoozed, 100);
        assert_eq!(mgr.config().max_duration_secs, 60);
    }

    #[test]
    fn test_config_returns_reference() {
        let mgr = TaskSnoozeManager::new();
        let config_ref = mgr.config();
        assert!(config_ref.enabled);
    }

    #[test]
    fn test_snooze_task_empty_id() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        let result = mgr.snooze_task("".to_string(), until, None);
        assert!(result.is_ok());
        assert!(mgr.is_snoozed(""));
    }

    #[test]
    fn test_snooze_task_unicode_id() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        let result = mgr.snooze_task("任务-中文".to_string(), until, None);
        assert!(result.is_ok());
        assert!(mgr.is_snoozed("任务-中文"));
    }

    #[test]
    fn test_snooze_task_emoji_id() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        let result = mgr.snooze_task("task-🔥".to_string(), until, None);
        assert!(result.is_ok());
        assert!(mgr.is_snoozed("task-🔥"));
    }

    #[test]
    fn test_snooze_task_reason_none() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        let state = mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        assert_eq!(state.reason, None);
    }

    #[test]
    fn test_snooze_task_long_reason() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        let long_reason = "a".repeat(10000);
        let state = mgr
            .snooze_task("task-1".to_string(), until, Some(long_reason.clone()))
            .unwrap();
        assert_eq!(state.reason, Some(long_reason));
    }

    #[test]
    fn test_snooze_max_duration_zero_unlimited() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_duration_secs = 0;
        let until = Utc::now() + Duration::days(365);
        let result = mgr.snooze_task("task-1".to_string(), until, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_snooze_max_duration_exact_boundary() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_duration_secs = 3600;
        // Just under the limit
        let until = Utc::now() + Duration::seconds(3599);
        let result = mgr.snooze_task("task-1".to_string(), until, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_snooze_max_duration_just_over_boundary() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_duration_secs = 3600;
        // Use a large enough margin to avoid timing issues
        let until = Utc::now() + Duration::seconds(7200);
        let result = mgr.snooze_task("task-1".to_string(), until, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskSnoozeError::InvalidTime));
    }

    #[test]
    fn test_snooze_max_snoozed_zero_unlimited() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_snoozed = 0;
        let until = Utc::now() + Duration::hours(1);
        for i in 0..100 {
            let result = mgr.snooze_task(format!("task-{}", i), until, None);
            assert!(result.is_ok());
        }
        assert_eq!(mgr.snoozed_count(), 100);
    }

    #[test]
    fn test_snooze_max_snoozed_one_boundary() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_snoozed = 1;
        let until = Utc::now() + Duration::hours(1);
        assert!(mgr.snooze_task("task-1".to_string(), until, None).is_ok());
        assert!(mgr.snooze_task("task-2".to_string(), until, None).is_err());
    }

    #[test]
    fn test_unsnooze_empty_id() {
        let mut mgr = TaskSnoozeManager::new();
        let result = mgr.unsnooze_task("");
        assert!(result.is_err());
    }

    #[test]
    fn test_unsnooze_idempotent() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        assert!(mgr.unsnooze_task("task-1").is_ok());
        assert!(mgr.unsnooze_task("task-1").is_err());
    }

    #[test]
    fn test_is_snoozed_empty_string() {
        let mgr = TaskSnoozeManager::new();
        assert!(!mgr.is_snoozed(""));
    }

    #[test]
    fn test_list_snoozed_empty() {
        let mgr = TaskSnoozeManager::new();
        let list = mgr.list_snoozed();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_list_snoozed_single() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        let list = mgr.list_snoozed();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].task_id, "task-1");
    }

    #[test]
    fn test_snoozed_count_multiple() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        mgr.snooze_task("task-2".to_string(), until, None).unwrap();
        mgr.snooze_task("task-3".to_string(), until, None).unwrap();
        assert_eq!(mgr.snoozed_count(), 3);
    }

    #[test]
    fn test_collect_expired_empty() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        let expired = mgr.collect_expired();
        assert_eq!(expired.len(), 0);
    }

    #[test]
    fn test_clear_expired_empty() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        let cleared = mgr.clear_expired();
        assert_eq!(cleared.len(), 0);
        assert!(mgr.is_snoozed("task-1"));
    }

    #[test]
    fn test_remove_task_nonexistent() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.remove_task("nonexistent"); // Should not panic
        assert_eq!(mgr.snoozed_count(), 0);
    }

    #[test]
    fn test_remove_task_idempotent() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        mgr.remove_task("task-1");
        mgr.remove_task("task-1"); // Should not panic
        assert_eq!(mgr.snoozed_count(), 0);
    }

    #[test]
    fn test_to_data_empty_manager() {
        let mgr = TaskSnoozeManager::new();
        let data = mgr.to_data();
        assert!(data.config.enabled);
        assert_eq!(data.snoozed_tasks.len(), 0);
    }

    #[test]
    fn test_to_data_preserves_config() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_snoozed = 42;
        mgr.config.max_duration_secs = 999;
        let data = mgr.to_data();
        assert_eq!(data.config.max_snoozed, 42);
        assert_eq!(data.config.max_duration_secs, 999);
    }

    // ===== Persistence tests =====

    #[tokio::test]
    async fn test_save_creates_file() {
        let dir = tempdir().unwrap();
        let data = TaskSnoozeData::default();
        save_task_snooze_data(&data, dir.path()).await.unwrap();
        let path = dir.path().join("task_snooze.json");
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_save_overwrites_existing() {
        let dir = tempdir().unwrap();
        let data1 = TaskSnoozeData {
            config: TaskSnoozeConfig {
                enabled: true,
                max_snoozed: 1,
                max_duration_secs: 3600,
            },
            snoozed_tasks: vec![],
        };
        let data2 = TaskSnoozeData {
            config: TaskSnoozeConfig {
                enabled: false,
                max_snoozed: 2,
                max_duration_secs: 7200,
            },
            snoozed_tasks: vec![],
        };
        save_task_snooze_data(&data1, dir.path()).await.unwrap();
        save_task_snooze_data(&data2, dir.path()).await.unwrap();
        let loaded = load_task_snooze_data(dir.path()).await.unwrap();
        assert!(!loaded.config.enabled);
        assert_eq!(loaded.config.max_snoozed, 2);
    }

    #[tokio::test]
    async fn test_save_atomic_no_tmp_left() {
        let dir = tempdir().unwrap();
        let data = TaskSnoozeData::default();
        save_task_snooze_data(&data, dir.path()).await.unwrap();
        let tmp_path = dir.path().join("task_snooze.json.tmp");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_load_corrupt_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task_snooze.json");
        tokio::fs::write(&path, "not valid json").await.unwrap();
        let result = load_task_snooze_data(dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task_snooze.json");
        tokio::fs::write(&path, "").await.unwrap();
        let result = load_task_snooze_data(dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_save_load_unicode() {
        let dir = tempdir().unwrap();
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task(
            "任务-中文🔥".to_string(),
            until,
            Some("休息中 💤".to_string()),
        )
        .unwrap();
        let data = mgr.to_data();
        save_task_snooze_data(&data, dir.path()).await.unwrap();
        let loaded = load_task_snooze_data(dir.path()).await.unwrap();
        assert_eq!(loaded.snoozed_tasks.len(), 1);
        assert_eq!(loaded.snoozed_tasks[0].task_id, "任务-中文🔥");
        assert_eq!(
            loaded.snoozed_tasks[0].reason,
            Some("休息中 💤".to_string())
        );
    }

    #[tokio::test]
    async fn test_save_load_empty_data() {
        let dir = tempdir().unwrap();
        let data = TaskSnoozeData::default();
        save_task_snooze_data(&data, dir.path()).await.unwrap();
        let loaded = load_task_snooze_data(dir.path()).await.unwrap();
        assert_eq!(loaded.snoozed_tasks.len(), 0);
        assert!(loaded.config.enabled);
    }

    #[tokio::test]
    async fn test_save_pretty_json() {
        let dir = tempdir().unwrap();
        let data = TaskSnoozeData::default();
        save_task_snooze_data(&data, dir.path()).await.unwrap();
        let path = dir.path().join("task_snooze.json");
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains('\n'));
    }

    // ===== Complex workflow tests =====

    #[test]
    fn test_full_lifecycle() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);

        // Snooze
        mgr.snooze_task("task-1".to_string(), until, Some("break".to_string()))
            .unwrap();
        assert!(mgr.is_snoozed("task-1"));
        assert_eq!(mgr.snoozed_count(), 1);

        // Unsnooze
        let state = mgr.unsnooze_task("task-1").unwrap();
        assert_eq!(state.task_id, "task-1");
        assert!(!mgr.is_snoozed("task-1"));
        assert_eq!(mgr.snoozed_count(), 0);

        // Snooze again
        let until2 = Utc::now() + Duration::hours(2);
        mgr.snooze_task("task-1".to_string(), until2, None).unwrap();
        assert!(mgr.is_snoozed("task-1"));
        assert_eq!(mgr.snoozed_count(), 1);
    }

    #[test]
    fn test_multiple_tasks_independent() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        mgr.snooze_task("task-2".to_string(), until, None).unwrap();
        mgr.snooze_task("task-3".to_string(), until, None).unwrap();

        mgr.unsnooze_task("task-2").unwrap();
        assert!(mgr.is_snoozed("task-1"));
        assert!(!mgr.is_snoozed("task-2"));
        assert!(mgr.is_snoozed("task-3"));
        assert_eq!(mgr.snoozed_count(), 2);
    }

    #[test]
    fn test_snooze_expire_and_clear() {
        let mut mgr = TaskSnoozeManager::new();
        let soon = Utc::now() + Duration::seconds(1);
        let future = Utc::now() + Duration::hours(1);
        mgr.snooze_task("expired".to_string(), soon, None).unwrap();
        mgr.snooze_task("active".to_string(), future, None).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(1100));

        let expired = mgr.collect_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].task_id, "expired");

        let cleared = mgr.clear_expired();
        assert_eq!(cleared.len(), 1);
        assert!(!mgr.is_snoozed("expired"));
        assert!(mgr.is_snoozed("active"));
    }

    #[test]
    fn test_config_change_affects_behavior() {
        let mut mgr = TaskSnoozeManager::new();
        let until = Utc::now() + Duration::hours(1);

        // Enable and snooze
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        assert!(mgr.is_snoozed("task-1"));

        // Disable config
        mgr.config.enabled = false;
        let result = mgr.snooze_task("task-2".to_string(), until, None);
        assert!(result.is_err());

        // Re-enable
        mgr.config.enabled = true;
        let result = mgr.snooze_task("task-2".to_string(), until, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_snooze_overwrites_existing() {
        let mut mgr = TaskSnoozeManager::new();
        let until1 = Utc::now() + Duration::hours(1);
        let until2 = Utc::now() + Duration::hours(5);

        mgr.snooze_task("task-1".to_string(), until1, Some("first".to_string()))
            .unwrap();
        let state1 = mgr.get_snooze_state("task-1").unwrap();
        assert_eq!(state1.reason, Some("first".to_string()));

        mgr.snooze_task("task-1".to_string(), until2, Some("second".to_string()))
            .unwrap();
        let state2 = mgr.get_snooze_state("task-1").unwrap();
        assert_eq!(state2.reason, Some("second".to_string()));
        assert_eq!(mgr.snoozed_count(), 1);
    }

    #[test]
    fn test_snooze_state_timestamps() {
        let mut mgr = TaskSnoozeManager::new();
        let before = Utc::now();
        let until = Utc::now() + Duration::hours(2);
        let state = mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        let after = Utc::now();

        assert!(state.snoozed_at >= before);
        assert!(state.snoozed_at <= after);
        assert!(state.snoozed_until > after);
    }

    #[test]
    fn test_max_snoozed_error_message() {
        let mut mgr = TaskSnoozeManager::new();
        mgr.config.max_snoozed = 2;
        let until = Utc::now() + Duration::hours(1);
        mgr.snooze_task("task-1".to_string(), until, None).unwrap();
        mgr.snooze_task("task-2".to_string(), until, None).unwrap();
        let err = mgr
            .snooze_task("task-3".to_string(), until, None)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("maximum snoozed tasks"));
        assert!(msg.contains("2"));
    }

    #[test]
    fn test_from_data_preserves_all_fields() {
        let until = Utc::now() + Duration::hours(3);
        let now = Utc::now();
        let data = TaskSnoozeData {
            config: TaskSnoozeConfig {
                enabled: false,
                max_snoozed: 99,
                max_duration_secs: 12345,
            },
            snoozed_tasks: vec![SnoozeState {
                task_id: "preserved".to_string(),
                snoozed_until: until,
                snoozed_at: now,
                reason: Some("kept".to_string()),
            }],
        };
        let mgr = TaskSnoozeManager::from_data(data);
        assert!(!mgr.config().enabled);
        assert_eq!(mgr.config().max_snoozed, 99);
        assert_eq!(mgr.config().max_duration_secs, 12345);
        let state = mgr.get_snooze_state("preserved").unwrap();
        assert_eq!(state.reason, Some("kept".to_string()));
    }
}
