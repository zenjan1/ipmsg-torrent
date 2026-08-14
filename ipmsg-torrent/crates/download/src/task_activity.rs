//! Per-Task Activity Log
//!
//! Maintains a ring buffer of timestamped activity events for each download task.
//! Unlike the global audit log, this provides fine-grained per-task debugging info
//! including state transitions, errors, speed changes, retries, and mirror switches.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::Path;

/// Default maximum events per task
const DEFAULT_MAX_EVENTS: usize = 100;

/// Default maximum tasks to track
const DEFAULT_MAX_TASKS: usize = 200;

/// Types of per-task activity events
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityEventType {
    /// Task was added to the queue
    Created,
    /// Task started downloading
    Started,
    /// Task was paused by user
    Paused,
    /// Task was resumed by user
    Resumed,
    /// Download completed successfully
    Completed,
    /// Download failed with error
    Failed,
    /// Task was removed
    Removed,
    /// Auto-retry triggered
    AutoRetry,
    /// Speed limit changed
    SpeedLimitChanged,
    /// Mirror URL switched
    MirrorSwitched,
    /// Connection error encountered
    ConnectionError,
    /// Timeout detected
    Timeout,
    /// Checksum verification started
    ChecksumVerify,
    /// Checksum verification result
    ChecksumResult,
    /// Post-download hook executed
    HookExecuted,
    /// Cooldown/backoff triggered
    CooldownTriggered,
    /// Conflict detected/resolved
    ConflictResolved,
    /// Progress milestone (e.g., 25%, 50%, 75%)
    ProgressMilestone,
    /// User note added/changed
    NoteChanged,
    /// User comment added to task
    CommentAdded,
    /// Tags modified
    TagsChanged,
    /// Generic info message
    Info,
    /// Generic warning message
    Warning,
}

impl std::fmt::Display for ActivityEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityEventType::Created => write!(f, "created"),
            ActivityEventType::Started => write!(f, "started"),
            ActivityEventType::Paused => write!(f, "paused"),
            ActivityEventType::Resumed => write!(f, "resumed"),
            ActivityEventType::Completed => write!(f, "completed"),
            ActivityEventType::Failed => write!(f, "failed"),
            ActivityEventType::Removed => write!(f, "removed"),
            ActivityEventType::AutoRetry => write!(f, "auto_retry"),
            ActivityEventType::SpeedLimitChanged => write!(f, "speed_limit_changed"),
            ActivityEventType::MirrorSwitched => write!(f, "mirror_switched"),
            ActivityEventType::ConnectionError => write!(f, "connection_error"),
            ActivityEventType::Timeout => write!(f, "timeout"),
            ActivityEventType::ChecksumVerify => write!(f, "checksum_verify"),
            ActivityEventType::ChecksumResult => write!(f, "checksum_result"),
            ActivityEventType::HookExecuted => write!(f, "hook_executed"),
            ActivityEventType::CooldownTriggered => write!(f, "cooldown"),
            ActivityEventType::ConflictResolved => write!(f, "conflict_resolved"),
            ActivityEventType::ProgressMilestone => write!(f, "progress"),
            ActivityEventType::NoteChanged => write!(f, "note_changed"),
            ActivityEventType::CommentAdded => write!(f, "comment_added"),
            ActivityEventType::TagsChanged => write!(f, "tags_changed"),
            ActivityEventType::Info => write!(f, "info"),
            ActivityEventType::Warning => write!(f, "warning"),
        }
    }
}

impl ActivityEventType {
    /// Get emoji icon for event type
    pub fn icon(&self) -> &'static str {
        match self {
            ActivityEventType::Created => "🆕",
            ActivityEventType::Started => "▶️",
            ActivityEventType::Paused => "⏸️",
            ActivityEventType::Resumed => "🔄",
            ActivityEventType::Completed => "✅",
            ActivityEventType::Failed => "❌",
            ActivityEventType::Removed => "🗑️",
            ActivityEventType::AutoRetry => "🔁",
            ActivityEventType::SpeedLimitChanged => "🚦",
            ActivityEventType::MirrorSwitched => "🔀",
            ActivityEventType::ConnectionError => "🔌",
            ActivityEventType::Timeout => "⏰",
            ActivityEventType::ChecksumVerify => "🔍",
            ActivityEventType::ChecksumResult => "🔐",
            ActivityEventType::HookExecuted => "🪝",
            ActivityEventType::CooldownTriggered => "🧊",
            ActivityEventType::ConflictResolved => "⚖️",
            ActivityEventType::ProgressMilestone => "📊",
            ActivityEventType::NoteChanged => "📝",
            ActivityEventType::CommentAdded => "💬",
            ActivityEventType::TagsChanged => "🏷️",
            ActivityEventType::Info => "ℹ️",
            ActivityEventType::Warning => "⚠️",
        }
    }

    /// Whether this event indicates a problem
    pub fn is_error(&self) -> bool {
        matches!(
            self,
            ActivityEventType::Failed
                | ActivityEventType::ConnectionError
                | ActivityEventType::Timeout
                | ActivityEventType::Warning
        )
    }
}

/// A single activity event for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEvent {
    /// When the event occurred
    pub timestamp: DateTime<Utc>,
    /// Type of event
    pub event_type: ActivityEventType,
    /// Human-readable details
    pub message: String,
    /// Optional numeric data (e.g., speed in bps, progress %)
    pub numeric_value: Option<f64>,
}

impl ActivityEvent {
    pub fn new(event_type: ActivityEventType, message: impl Into<String>) -> Self {
        Self {
            timestamp: Utc::now(),
            event_type,
            message: message.into(),
            numeric_value: None,
        }
    }

    pub fn with_value(mut self, value: f64) -> Self {
        self.numeric_value = Some(value);
        self
    }

    /// Format for display
    pub fn format_display(&self) -> String {
        let time_str = self.timestamp.format("%H:%M:%S").to_string();
        let value_str = self
            .numeric_value
            .map(|v| format!(" ({:.1})", v))
            .unwrap_or_default();
        format!(
            "{} {} {} {}{}",
            time_str,
            self.event_type.icon(),
            self.event_type,
            self.message,
            value_str
        )
    }
}

/// Per-task activity log (ring buffer)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskActivityLog {
    /// Task ID this log belongs to
    pub task_id: String,
    /// Task name (for display)
    pub task_name: String,
    /// Ring buffer of events
    events: VecDeque<ActivityEvent>,
    /// Maximum events to keep
    max_events: usize,
}

impl TaskActivityLog {
    pub fn new(task_id: impl Into<String>, task_name: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            task_name: task_name.into(),
            events: VecDeque::new(),
            max_events: DEFAULT_MAX_EVENTS,
        }
    }

    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.max_events = max_events;
        self
    }

    /// Add an event to the log
    pub fn log(&mut self, event: ActivityEvent) {
        if self.events.len() >= self.max_events {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    /// Get all events
    pub fn events(&self) -> impl Iterator<Item = &ActivityEvent> {
        self.events.iter()
    }

    /// Get events in reverse chronological order
    pub fn events_reverse(&self) -> impl Iterator<Item = &ActivityEvent> {
        self.events.iter().rev()
    }

    /// Get most recent N events
    pub fn recent(&self, n: usize) -> Vec<&ActivityEvent> {
        self.events.iter().rev().take(n).collect()
    }

    /// Get events by type
    pub fn events_by_type(&self, event_type: &ActivityEventType) -> Vec<&ActivityEvent> {
        self.events
            .iter()
            .filter(|e| &e.event_type == event_type)
            .collect()
    }

    /// Get error events only
    pub fn errors(&self) -> Vec<&ActivityEvent> {
        self.events
            .iter()
            .filter(|e| e.event_type.is_error())
            .collect()
    }

    /// Number of events
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether log is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Clear all events
    pub fn clear(&mut self) {
        self.events.clear();
    }

    /// Get summary statistics
    pub fn summary(&self) -> TaskActivitySummary {
        let error_count = self.errors().len();
        let first_event = self.events.front().map(|e| e.timestamp);
        let last_event = self.events.back().map(|e| e.timestamp);

        // Count events by type
        let mut type_counts: HashMap<String, usize> = HashMap::new();
        for event in &self.events {
            *type_counts.entry(event.event_type.to_string()).or_default() += 1;
        }

        TaskActivitySummary {
            task_id: self.task_id.clone(),
            task_name: self.task_name.clone(),
            total_events: self.events.len(),
            error_count,
            first_event,
            last_event,
            type_counts,
        }
    }

    /// Format the activity log for display
    pub fn format_display(&self, max_lines: usize) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "Activity log for '{}' ({})",
            self.task_name, self.task_id
        ));
        lines.push(format!("Total events: {}", self.events.len()));

        let summary = self.summary();
        if summary.error_count > 0 {
            lines.push(format!("Errors: {}", summary.error_count));
        }
        lines.push(String::new());

        for event in self.events.iter().rev().take(max_lines) {
            lines.push(event.format_display());
        }

        lines.join("\n")
    }
}

/// Summary of a task's activity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskActivitySummary {
    pub task_id: String,
    pub task_name: String,
    pub total_events: usize,
    pub error_count: usize,
    pub first_event: Option<DateTime<Utc>>,
    pub last_event: Option<DateTime<Utc>>,
    pub type_counts: HashMap<String, usize>,
}

/// Manager for all per-task activity logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityLogManager {
    /// Per-task activity logs
    logs: HashMap<String, TaskActivityLog>,
    /// Maximum tasks to track
    max_tasks: usize,
    /// Default max events per task
    default_max_events: usize,
}

impl Default for ActivityLogManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ActivityLogManager {
    pub fn new() -> Self {
        Self {
            logs: HashMap::new(),
            max_tasks: DEFAULT_MAX_TASKS,
            default_max_events: DEFAULT_MAX_EVENTS,
        }
    }

    pub fn with_limits(mut self, max_tasks: usize, default_max_events: usize) -> Self {
        self.max_tasks = max_tasks;
        self.default_max_events = default_max_events;
        self
    }

    /// Get or create activity log for a task
    pub fn get_or_create(&mut self, task_id: &str, task_name: &str) -> &mut TaskActivityLog {
        if !self.logs.contains_key(task_id) {
            // Evict oldest task if at capacity
            if self.logs.len() >= self.max_tasks {
                self.evict_oldest();
            }
            self.logs.insert(
                task_id.to_string(),
                TaskActivityLog::new(task_id, task_name).with_max_events(self.default_max_events),
            );
        }
        self.logs.get_mut(task_id).unwrap()
    }

    /// Log an event for a task
    pub fn log_event(&mut self, task_id: &str, task_name: &str, event: ActivityEvent) {
        self.get_or_create(task_id, task_name).log(event);
    }

    /// Get activity log for a task (read-only)
    pub fn get(&self, task_id: &str) -> Option<&TaskActivityLog> {
        self.logs.get(task_id)
    }

    /// Remove activity log for a task
    pub fn remove(&mut self, task_id: &str) -> Option<TaskActivityLog> {
        self.logs.remove(task_id)
    }

    /// Clear activity log for a task
    pub fn clear_task(&mut self, task_id: &str) {
        if let Some(log) = self.logs.get_mut(task_id) {
            log.clear();
        }
    }

    /// Clear all activity logs
    pub fn clear_all(&mut self) {
        self.logs.clear();
    }

    /// List all tracked task IDs
    pub fn task_ids(&self) -> Vec<&str> {
        self.logs.keys().map(|s| s.as_str()).collect()
    }

    /// Get summary for a specific task
    pub fn task_summary(&self, task_id: &str) -> Option<TaskActivitySummary> {
        self.logs.get(task_id).map(|log| log.summary())
    }

    /// Get summaries for all tasks
    pub fn all_summaries(&self) -> Vec<TaskActivitySummary> {
        self.logs.values().map(|log| log.summary()).collect()
    }

    /// Evict the task with the oldest last event
    fn evict_oldest(&mut self) {
        if let Some(oldest_id) = self
            .logs
            .iter()
            .min_by_key(|(_, log)| {
                log.events
                    .back()
                    .map(|e| e.timestamp)
                    .unwrap_or_else(Utc::now)
            })
            .map(|(id, _)| id.clone())
        {
            self.logs.remove(&oldest_id);
        }
    }

    /// Total number of tracked tasks
    pub fn task_count(&self) -> usize {
        self.logs.len()
    }

    /// Total number of events across all tasks
    pub fn total_events(&self) -> usize {
        self.logs.values().map(|log| log.len()).sum()
    }
}

/// Persistence: save activity logs to disk
pub fn save_activity_logs(
    manager: &ActivityLogManager,
    data_dir: &Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("task_activity_logs.json");
    let json = serde_json::to_string_pretty(manager)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    // Atomic write
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, json)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Persistence: load activity logs from disk
pub fn load_activity_logs(data_dir: &Path) -> Option<ActivityLogManager> {
    let path = data_dir.join("task_activity_logs.json");
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_activity_event_creation() {
        let event = ActivityEvent::new(ActivityEventType::Created, "Task created");
        assert_eq!(event.event_type, ActivityEventType::Created);
        assert_eq!(event.message, "Task created");
        assert!(event.numeric_value.is_none());
    }

    #[test]
    fn test_activity_event_with_value() {
        let event = ActivityEvent::new(ActivityEventType::ProgressMilestone, "50% complete")
            .with_value(50.0);
        assert_eq!(event.numeric_value, Some(50.0));
    }

    #[test]
    fn test_event_type_display() {
        assert_eq!(ActivityEventType::Created.to_string(), "created");
        assert_eq!(
            ActivityEventType::MirrorSwitched.to_string(),
            "mirror_switched"
        );
        assert_eq!(ActivityEventType::ProgressMilestone.to_string(), "progress");
    }

    #[test]
    fn test_event_type_icon() {
        assert_eq!(ActivityEventType::Completed.icon(), "✅");
        assert_eq!(ActivityEventType::Failed.icon(), "❌");
        assert_eq!(ActivityEventType::Timeout.icon(), "⏰");
    }

    #[test]
    fn test_event_type_is_error() {
        assert!(ActivityEventType::Failed.is_error());
        assert!(ActivityEventType::ConnectionError.is_error());
        assert!(ActivityEventType::Timeout.is_error());
        assert!(ActivityEventType::Warning.is_error());
        assert!(!ActivityEventType::Completed.is_error());
        assert!(!ActivityEventType::Started.is_error());
    }

    #[test]
    fn test_task_activity_log_basic() {
        let mut log = TaskActivityLog::new("task-1", "test.txt");
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);

        log.log(ActivityEvent::new(ActivityEventType::Created, "created"));
        log.log(ActivityEvent::new(
            ActivityEventType::Started,
            "downloading",
        ));
        assert_eq!(log.len(), 2);
        assert!(!log.is_empty());
    }

    #[test]
    fn test_task_activity_log_ring_buffer() {
        let mut log = TaskActivityLog::new("task-1", "test.txt").with_max_events(3);

        for i in 0..5 {
            log.log(ActivityEvent::new(
                ActivityEventType::Info,
                format!("event {}", i),
            ));
        }

        // Only last 3 events kept
        assert_eq!(log.len(), 3);
        let events: Vec<_> = log.events().collect();
        assert_eq!(events[0].message, "event 2");
        assert_eq!(events[1].message, "event 3");
        assert_eq!(events[2].message, "event 4");
    }

    #[test]
    fn test_task_activity_log_errors() {
        let mut log = TaskActivityLog::new("task-1", "test.txt");
        log.log(ActivityEvent::new(ActivityEventType::Started, "ok"));
        log.log(ActivityEvent::new(
            ActivityEventType::ConnectionError,
            "conn refused",
        ));
        log.log(ActivityEvent::new(ActivityEventType::AutoRetry, "retrying"));
        log.log(ActivityEvent::new(ActivityEventType::Failed, "gave up"));

        let errors = log.errors();
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].event_type, ActivityEventType::ConnectionError);
        assert_eq!(errors[1].event_type, ActivityEventType::Failed);
    }

    #[test]
    fn test_task_activity_log_events_by_type() {
        let mut log = TaskActivityLog::new("task-1", "test.txt");
        log.log(ActivityEvent::new(ActivityEventType::Started, "a"));
        log.log(ActivityEvent::new(ActivityEventType::Paused, "b"));
        log.log(ActivityEvent::new(ActivityEventType::Resumed, "c"));
        log.log(ActivityEvent::new(ActivityEventType::Paused, "d"));

        let paused = log.events_by_type(&ActivityEventType::Paused);
        assert_eq!(paused.len(), 2);
    }

    #[test]
    fn test_task_activity_log_recent() {
        let mut log = TaskActivityLog::new("task-1", "test.txt");
        for i in 0..10 {
            log.log(ActivityEvent::new(
                ActivityEventType::Info,
                format!("{}", i),
            ));
        }

        let recent = log.recent(3);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].message, "9");
        assert_eq!(recent[1].message, "8");
        assert_eq!(recent[2].message, "7");
    }

    #[test]
    fn test_task_activity_log_clear() {
        let mut log = TaskActivityLog::new("task-1", "test.txt");
        log.log(ActivityEvent::new(ActivityEventType::Info, "test"));
        assert_eq!(log.len(), 1);
        log.clear();
        assert!(log.is_empty());
    }

    #[test]
    fn test_task_activity_summary() {
        let mut log = TaskActivityLog::new("task-1", "test.txt");
        log.log(ActivityEvent::new(ActivityEventType::Created, "created"));
        log.log(ActivityEvent::new(ActivityEventType::Started, "started"));
        log.log(ActivityEvent::new(ActivityEventType::Failed, "error"));

        let summary = log.summary();
        assert_eq!(summary.task_id, "task-1");
        assert_eq!(summary.task_name, "test.txt");
        assert_eq!(summary.total_events, 3);
        assert_eq!(summary.error_count, 1);
        assert!(summary.first_event.is_some());
        assert!(summary.last_event.is_some());
        assert_eq!(*summary.type_counts.get("created").unwrap(), 1);
    }

    #[test]
    fn test_activity_log_manager_basic() {
        let mut mgr = ActivityLogManager::new();
        assert_eq!(mgr.task_count(), 0);

        mgr.log_event(
            "t1",
            "file1.txt",
            ActivityEvent::new(ActivityEventType::Created, "x"),
        );
        mgr.log_event(
            "t1",
            "file1.txt",
            ActivityEvent::new(ActivityEventType::Started, "x"),
        );
        mgr.log_event(
            "t2",
            "file2.txt",
            ActivityEvent::new(ActivityEventType::Created, "x"),
        );

        assert_eq!(mgr.task_count(), 2);
        assert_eq!(mgr.total_events(), 3);
        assert!(mgr.get("t1").is_some());
        assert_eq!(mgr.get("t1").unwrap().len(), 2);
    }

    #[test]
    fn test_activity_log_manager_remove() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "t1",
            "file.txt",
            ActivityEvent::new(ActivityEventType::Created, "x"),
        );
        assert!(mgr.get("t1").is_some());

        let removed = mgr.remove("t1");
        assert!(removed.is_some());
        assert!(mgr.get("t1").is_none());
        assert_eq!(mgr.task_count(), 0);
    }

    #[test]
    fn test_activity_log_manager_clear_task() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "t1",
            "file.txt",
            ActivityEvent::new(ActivityEventType::Created, "x"),
        );
        mgr.clear_task("t1");
        assert!(mgr.get("t1").unwrap().is_empty());
    }

    #[test]
    fn test_activity_log_manager_eviction() {
        let mut mgr = ActivityLogManager::new().with_limits(2, 10);
        mgr.log_event("t1", "a", ActivityEvent::new(ActivityEventType::Info, "1"));
        mgr.log_event("t2", "b", ActivityEvent::new(ActivityEventType::Info, "2"));
        assert_eq!(mgr.task_count(), 2);

        // Adding a third should evict the oldest
        mgr.log_event("t3", "c", ActivityEvent::new(ActivityEventType::Info, "3"));
        assert_eq!(mgr.task_count(), 2);
        // t1 should be evicted (oldest last event)
        assert!(mgr.get("t1").is_none());
        assert!(mgr.get("t2").is_some());
        assert!(mgr.get("t3").is_some());
    }

    #[test]
    fn test_activity_log_manager_summaries() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "t1",
            "a",
            ActivityEvent::new(ActivityEventType::Created, "x"),
        );
        mgr.log_event(
            "t2",
            "b",
            ActivityEvent::new(ActivityEventType::Failed, "err"),
        );

        let summaries = mgr.all_summaries();
        assert_eq!(summaries.len(), 2);
    }

    #[test]
    fn test_activity_log_manager_task_ids() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event("t1", "a", ActivityEvent::new(ActivityEventType::Info, "x"));
        mgr.log_event("t2", "b", ActivityEvent::new(ActivityEventType::Info, "x"));

        let mut ids = mgr.task_ids();
        ids.sort();
        assert_eq!(ids, vec!["t1", "t2"]);
    }

    #[test]
    fn test_activity_log_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "t1",
            "test.txt",
            ActivityEvent::new(ActivityEventType::Created, "hello"),
        );
        mgr.log_event(
            "t1",
            "test.txt",
            ActivityEvent::new(ActivityEventType::Started, "go"),
        );

        save_activity_logs(&mgr, dir.path()).unwrap();

        let loaded = load_activity_logs(dir.path()).unwrap();
        assert_eq!(loaded.task_count(), 1);
        assert_eq!(loaded.total_events(), 2);
        let log = loaded.get("t1").unwrap();
        assert_eq!(log.task_name, "test.txt");
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_activity_log_persistence_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_activity_logs(dir.path());
        assert!(result.is_none());
    }

    #[test]
    fn test_activity_log_format_display() {
        let mut log = TaskActivityLog::new("t1", "movie.mkv");
        log.log(ActivityEvent::new(
            ActivityEventType::Created,
            "Added to queue",
        ));
        log.log(
            ActivityEvent::new(ActivityEventType::Started, "Downloading from 3 peers")
                .with_value(3.0),
        );

        let display = log.format_display(10);
        assert!(display.contains("movie.mkv"));
        assert!(display.contains("t1"));
        assert!(display.contains("created"));
        assert!(display.contains("started"));
    }

    #[test]
    fn test_activity_event_format_display() {
        let event = ActivityEvent::new(ActivityEventType::MirrorSwitched, "Switched to mirror2")
            .with_value(1.5);
        let display = event.format_display();
        assert!(display.contains("🔀"));
        assert!(display.contains("mirror_switched"));
        assert!(display.contains("Switched to mirror2"));
        assert!(display.contains("1.5"));
    }

    #[test]
    fn test_activity_log_serialization() {
        let mut log = TaskActivityLog::new("t1", "test.txt");
        log.log(ActivityEvent::new(ActivityEventType::Created, "created"));

        let json = serde_json::to_string(&log).unwrap();
        let deserialized: TaskActivityLog = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, "t1");
        assert_eq!(deserialized.len(), 1);
    }

    #[test]
    fn test_activity_log_manager_serialization() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "t1",
            "a",
            ActivityEvent::new(ActivityEventType::Created, "x"),
        );

        let json = serde_json::to_string(&mgr).unwrap();
        let deserialized: ActivityLogManager = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_count(), 1);
        assert_eq!(deserialized.total_events(), 1);
    }

    // ===== Phase 221: Comprehensive Test Coverage =====

    // --- ActivityEventType: all 23 variants Display ---
    #[test]
    fn test_all_event_types_display() {
        assert_eq!(ActivityEventType::Created.to_string(), "created");
        assert_eq!(ActivityEventType::Started.to_string(), "started");
        assert_eq!(ActivityEventType::Paused.to_string(), "paused");
        assert_eq!(ActivityEventType::Resumed.to_string(), "resumed");
        assert_eq!(ActivityEventType::Completed.to_string(), "completed");
        assert_eq!(ActivityEventType::Failed.to_string(), "failed");
        assert_eq!(ActivityEventType::Removed.to_string(), "removed");
        assert_eq!(ActivityEventType::AutoRetry.to_string(), "auto_retry");
        assert_eq!(
            ActivityEventType::SpeedLimitChanged.to_string(),
            "speed_limit_changed"
        );
        assert_eq!(
            ActivityEventType::MirrorSwitched.to_string(),
            "mirror_switched"
        );
        assert_eq!(
            ActivityEventType::ConnectionError.to_string(),
            "connection_error"
        );
        assert_eq!(ActivityEventType::Timeout.to_string(), "timeout");
        assert_eq!(
            ActivityEventType::ChecksumVerify.to_string(),
            "checksum_verify"
        );
        assert_eq!(
            ActivityEventType::ChecksumResult.to_string(),
            "checksum_result"
        );
        assert_eq!(ActivityEventType::HookExecuted.to_string(), "hook_executed");
        assert_eq!(ActivityEventType::CooldownTriggered.to_string(), "cooldown");
        assert_eq!(
            ActivityEventType::ConflictResolved.to_string(),
            "conflict_resolved"
        );
        assert_eq!(ActivityEventType::ProgressMilestone.to_string(), "progress");
        assert_eq!(ActivityEventType::NoteChanged.to_string(), "note_changed");
        assert_eq!(ActivityEventType::CommentAdded.to_string(), "comment_added");
        assert_eq!(ActivityEventType::TagsChanged.to_string(), "tags_changed");
        assert_eq!(ActivityEventType::Info.to_string(), "info");
        assert_eq!(ActivityEventType::Warning.to_string(), "warning");
    }

    // --- ActivityEventType: all 23 variants icon ---
    #[test]
    fn test_all_event_types_icons() {
        assert_eq!(ActivityEventType::Created.icon(), "🆕");
        assert_eq!(ActivityEventType::Started.icon(), "▶️");
        assert_eq!(ActivityEventType::Paused.icon(), "⏸️");
        assert_eq!(ActivityEventType::Resumed.icon(), "🔄");
        assert_eq!(ActivityEventType::Completed.icon(), "✅");
        assert_eq!(ActivityEventType::Failed.icon(), "❌");
        assert_eq!(ActivityEventType::Removed.icon(), "🗑️");
        assert_eq!(ActivityEventType::AutoRetry.icon(), "🔁");
        assert_eq!(ActivityEventType::SpeedLimitChanged.icon(), "🚦");
        assert_eq!(ActivityEventType::MirrorSwitched.icon(), "🔀");
        assert_eq!(ActivityEventType::ConnectionError.icon(), "🔌");
        assert_eq!(ActivityEventType::Timeout.icon(), "⏰");
        assert_eq!(ActivityEventType::ChecksumVerify.icon(), "🔍");
        assert_eq!(ActivityEventType::ChecksumResult.icon(), "🔐");
        assert_eq!(ActivityEventType::HookExecuted.icon(), "🪝");
        assert_eq!(ActivityEventType::CooldownTriggered.icon(), "🧊");
        assert_eq!(ActivityEventType::ConflictResolved.icon(), "⚖️");
        assert_eq!(ActivityEventType::ProgressMilestone.icon(), "📊");
        assert_eq!(ActivityEventType::NoteChanged.icon(), "📝");
        assert_eq!(ActivityEventType::CommentAdded.icon(), "💬");
        assert_eq!(ActivityEventType::TagsChanged.icon(), "🏷️");
        assert_eq!(ActivityEventType::Info.icon(), "ℹ️");
        assert_eq!(ActivityEventType::Warning.icon(), "⚠️");
    }

    // --- ActivityEventType: all is_error variants ---
    #[test]
    fn test_all_is_error_variants() {
        let error_types = [
            ActivityEventType::Failed,
            ActivityEventType::ConnectionError,
            ActivityEventType::Timeout,
            ActivityEventType::Warning,
        ];
        for et in &error_types {
            assert!(et.is_error(), "{:?} should be error", et);
        }

        let non_error_types = [
            ActivityEventType::Created,
            ActivityEventType::Started,
            ActivityEventType::Paused,
            ActivityEventType::Resumed,
            ActivityEventType::Completed,
            ActivityEventType::Removed,
            ActivityEventType::AutoRetry,
            ActivityEventType::SpeedLimitChanged,
            ActivityEventType::MirrorSwitched,
            ActivityEventType::ChecksumVerify,
            ActivityEventType::ChecksumResult,
            ActivityEventType::HookExecuted,
            ActivityEventType::CooldownTriggered,
            ActivityEventType::ConflictResolved,
            ActivityEventType::ProgressMilestone,
            ActivityEventType::NoteChanged,
            ActivityEventType::CommentAdded,
            ActivityEventType::TagsChanged,
            ActivityEventType::Info,
        ];
        for et in &non_error_types {
            assert!(!et.is_error(), "{:?} should not be error", et);
        }
    }

    // --- ActivityEventType: serde roundtrip all variants ---
    #[test]
    fn test_event_type_serde_all_variants() {
        let all_types = vec![
            ActivityEventType::Created,
            ActivityEventType::Started,
            ActivityEventType::Paused,
            ActivityEventType::Resumed,
            ActivityEventType::Completed,
            ActivityEventType::Failed,
            ActivityEventType::Removed,
            ActivityEventType::AutoRetry,
            ActivityEventType::SpeedLimitChanged,
            ActivityEventType::MirrorSwitched,
            ActivityEventType::ConnectionError,
            ActivityEventType::Timeout,
            ActivityEventType::ChecksumVerify,
            ActivityEventType::ChecksumResult,
            ActivityEventType::HookExecuted,
            ActivityEventType::CooldownTriggered,
            ActivityEventType::ConflictResolved,
            ActivityEventType::ProgressMilestone,
            ActivityEventType::NoteChanged,
            ActivityEventType::CommentAdded,
            ActivityEventType::TagsChanged,
            ActivityEventType::Info,
            ActivityEventType::Warning,
        ];
        for et in &all_types {
            let json = serde_json::to_string(et).unwrap();
            let de: ActivityEventType = serde_json::from_str(&json).unwrap();
            assert_eq!(&de, et);
        }
    }

    // --- ActivityEventType: snake_case serde values ---
    #[test]
    fn test_event_type_snake_case_serde() {
        let json = r#""speed_limit_changed""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::SpeedLimitChanged);

        let json = r#""mirror_switched""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::MirrorSwitched);

        let json = r#""connection_error""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::ConnectionError);

        let json = r#""checksum_verify""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::ChecksumVerify);

        let json = r#""cooldown_triggered""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::CooldownTriggered);

        let json = r#""conflict_resolved""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::ConflictResolved);

        let json = r#""progress_milestone""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::ProgressMilestone);

        let json = r#""note_changed""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::NoteChanged);

        let json = r#""comment_added""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::CommentAdded);

        let json = r#""tags_changed""#;
        let et: ActivityEventType = serde_json::from_str(json).unwrap();
        assert_eq!(et, ActivityEventType::TagsChanged);
    }

    // --- ActivityEventType: Clone/Copy/Debug/Eq ---
    #[test]
    fn test_event_type_traits() {
        let et = ActivityEventType::Created;
        let cloned = et.clone();
        assert_eq!(et, cloned);
        // Debug
        let debug = format!("{:?}", et);
        assert!(debug.contains("Created"));
    }

    // --- ActivityEvent: serde roundtrip with/without numeric_value ---
    #[test]
    fn test_activity_event_serde_with_value() {
        let event =
            ActivityEvent::new(ActivityEventType::ProgressMilestone, "50%").with_value(50.0);
        let json = serde_json::to_string(&event).unwrap();
        let de: ActivityEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(de.event_type, ActivityEventType::ProgressMilestone);
        assert_eq!(de.message, "50%");
        assert_eq!(de.numeric_value, Some(50.0));
    }

    #[test]
    fn test_activity_event_serde_without_value() {
        let event = ActivityEvent::new(ActivityEventType::Created, "new task");
        let json = serde_json::to_string(&event).unwrap();
        let de: ActivityEvent = serde_json::from_str(&json).unwrap();
        assert!(de.numeric_value.is_none());
    }

    // --- ActivityEvent: empty message ---
    #[test]
    fn test_activity_event_empty_message() {
        let event = ActivityEvent::new(ActivityEventType::Info, "");
        assert_eq!(event.message, "");
        let display = event.format_display();
        assert!(display.contains("info"));
    }

    // --- ActivityEvent: Unicode message ---
    #[test]
    fn test_activity_event_unicode_message() {
        let event = ActivityEvent::new(ActivityEventType::Info, "中文消息 🎉");
        assert_eq!(event.message, "中文消息 🎉");
    }

    // --- ActivityEvent: with_value chaining ---
    #[test]
    fn test_activity_event_with_value_chaining() {
        let event =
            ActivityEvent::new(ActivityEventType::SpeedLimitChanged, "limited").with_value(1024.0);
        assert_eq!(event.numeric_value, Some(1024.0));
        assert_eq!(event.event_type, ActivityEventType::SpeedLimitChanged);
    }

    // --- ActivityEvent: zero/negative numeric values ---
    #[test]
    fn test_activity_event_numeric_edge_values() {
        let zero = ActivityEvent::new(ActivityEventType::Info, "zero").with_value(0.0);
        assert_eq!(zero.numeric_value, Some(0.0));

        let neg = ActivityEvent::new(ActivityEventType::Info, "neg").with_value(-42.5);
        assert_eq!(neg.numeric_value, Some(-42.5));

        let big = ActivityEvent::new(ActivityEventType::Info, "big").with_value(f64::MAX);
        assert_eq!(big.numeric_value, Some(f64::MAX));
    }

    // --- ActivityEvent: format_display without numeric value ---
    #[test]
    fn test_activity_event_format_no_value() {
        let event = ActivityEvent::new(ActivityEventType::Created, "new task");
        let display = event.format_display();
        assert!(display.contains("🆕"));
        assert!(display.contains("created"));
        assert!(display.contains("new task"));
        // Should not have parentheses for value
        assert!(!display.contains("("));
    }

    // --- TaskActivityLog: events_reverse ---
    #[test]
    fn test_task_activity_log_events_reverse() {
        let mut log = TaskActivityLog::new("t1", "test");
        for i in 0..5 {
            log.log(ActivityEvent::new(
                ActivityEventType::Info,
                format!("{}", i),
            ));
        }
        let rev: Vec<_> = log.events_reverse().collect();
        assert_eq!(rev.len(), 5);
        assert_eq!(rev[0].message, "4");
        assert_eq!(rev[4].message, "0");
    }

    // --- TaskActivityLog: events_reverse empty ---
    #[test]
    fn test_task_activity_log_events_reverse_empty() {
        let log = TaskActivityLog::new("t1", "test");
        let rev: Vec<_> = log.events_reverse().collect();
        assert!(rev.is_empty());
    }

    // --- TaskActivityLog: recent with n > len ---
    #[test]
    fn test_task_activity_log_recent_more_than_available() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Info, "a"));
        log.log(ActivityEvent::new(ActivityEventType::Info, "b"));
        let recent = log.recent(100);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].message, "b");
    }

    // --- TaskActivityLog: recent(0) ---
    #[test]
    fn test_task_activity_log_recent_zero() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Info, "a"));
        let recent = log.recent(0);
        assert!(recent.is_empty());
    }

    // --- TaskActivityLog: events_by_type with no matches ---
    #[test]
    fn test_task_activity_log_events_by_type_no_match() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Info, "a"));
        let result = log.events_by_type(&ActivityEventType::Failed);
        assert!(result.is_empty());
    }

    // --- TaskActivityLog: events_by_type with multiple matches ---
    #[test]
    fn test_task_activity_log_events_by_type_multiple() {
        let mut log = TaskActivityLog::new("t1", "test");
        for _ in 0..5 {
            log.log(ActivityEvent::new(ActivityEventType::Info, "x"));
        }
        let result = log.events_by_type(&ActivityEventType::Info);
        assert_eq!(result.len(), 5);
    }

    // --- TaskActivityLog: errors empty ---
    #[test]
    fn test_task_activity_log_errors_empty() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Info, "ok"));
        log.log(ActivityEvent::new(ActivityEventType::Completed, "done"));
        assert!(log.errors().is_empty());
    }

    // --- TaskActivityLog: with_max_events(0) allows nothing ---
    #[test]
    fn test_task_activity_log_max_events_zero() {
        let mut log = TaskActivityLog::new("t1", "test").with_max_events(0);
        log.log(ActivityEvent::new(ActivityEventType::Info, "a"));
        // With max_events=0, each log pops front before push, so only 1 remains
        assert_eq!(log.len(), 1);
    }

    // --- TaskActivityLog: with_max_events(1) keeps only latest ---
    #[test]
    fn test_task_activity_log_max_events_one() {
        let mut log = TaskActivityLog::new("t1", "test").with_max_events(1);
        log.log(ActivityEvent::new(ActivityEventType::Info, "first"));
        log.log(ActivityEvent::new(ActivityEventType::Info, "second"));
        assert_eq!(log.len(), 1);
        let ev = log.events().next().unwrap();
        assert_eq!(ev.message, "second");
    }

    // --- TaskActivityLog: task_id and task_name fields ---
    #[test]
    fn test_task_activity_log_fields() {
        let log = TaskActivityLog::new("my-id", "my-name");
        assert_eq!(log.task_id, "my-id");
        assert_eq!(log.task_name, "my-name");
    }

    // --- TaskActivityLog: summary type_counts correctness ---
    #[test]
    fn test_task_activity_summary_type_counts() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Info, "a"));
        log.log(ActivityEvent::new(ActivityEventType::Info, "b"));
        log.log(ActivityEvent::new(ActivityEventType::Info, "c"));
        log.log(ActivityEvent::new(ActivityEventType::Failed, "err"));

        let summary = log.summary();
        assert_eq!(*summary.type_counts.get("info").unwrap(), 3);
        assert_eq!(*summary.type_counts.get("failed").unwrap(), 1);
        assert_eq!(summary.type_counts.len(), 2);
    }

    // --- TaskActivityLog: summary on empty log ---
    #[test]
    fn test_task_activity_summary_empty() {
        let log = TaskActivityLog::new("t1", "test");
        let summary = log.summary();
        assert_eq!(summary.total_events, 0);
        assert_eq!(summary.error_count, 0);
        assert!(summary.first_event.is_none());
        assert!(summary.last_event.is_none());
        assert!(summary.type_counts.is_empty());
    }

    // --- TaskActivityLog: summary error_count ---
    #[test]
    fn test_task_activity_summary_error_count() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Failed, "f1"));
        log.log(ActivityEvent::new(ActivityEventType::Timeout, "t1"));
        log.log(ActivityEvent::new(ActivityEventType::ConnectionError, "c1"));
        log.log(ActivityEvent::new(ActivityEventType::Warning, "w1"));
        log.log(ActivityEvent::new(ActivityEventType::Info, "ok"));

        let summary = log.summary();
        assert_eq!(summary.error_count, 4);
    }

    // --- TaskActivityLog: serde roundtrip ---
    #[test]
    fn test_task_activity_log_serde_roundtrip() {
        let mut log = TaskActivityLog::new("t1", "test.txt").with_max_events(50);
        log.log(ActivityEvent::new(ActivityEventType::Created, "created").with_value(1.0));
        log.log(ActivityEvent::new(ActivityEventType::Started, "started"));

        let json = serde_json::to_string(&log).unwrap();
        let de: TaskActivityLog = serde_json::from_str(&json).unwrap();
        assert_eq!(de.task_id, "t1");
        assert_eq!(de.task_name, "test.txt");
        assert_eq!(de.len(), 2);
        let events: Vec<_> = de.events().collect();
        assert_eq!(events[0].numeric_value, Some(1.0));
    }

    // --- TaskActivityLog: serde extra fields ignored ---
    #[test]
    fn test_task_activity_log_serde_extra_fields() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Info, "x"));
        let mut json: serde_json::Value = serde_json::to_value(&log).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("extra_field".into(), serde_json::json!("ignored"));
        let de: TaskActivityLog = serde_json::from_value(json).unwrap();
        assert_eq!(de.task_id, "t1");
        assert_eq!(de.len(), 1);
    }

    // --- TaskActivityLog: format_display with errors ---
    #[test]
    fn test_task_activity_log_format_display_with_errors() {
        let mut log = TaskActivityLog::new("t1", "movie.mkv");
        log.log(ActivityEvent::new(ActivityEventType::Created, "added"));
        log.log(ActivityEvent::new(
            ActivityEventType::ConnectionError,
            "conn refused",
        ));
        log.log(ActivityEvent::new(ActivityEventType::Failed, "gave up"));

        let display = log.format_display(100);
        assert!(display.contains("Errors: 2"));
        assert!(display.contains("connection_error"));
        assert!(display.contains("failed"));
    }

    // --- TaskActivityLog: format_display respects max_lines ---
    #[test]
    fn test_task_activity_log_format_display_max_lines() {
        let mut log = TaskActivityLog::new("t1", "test");
        for i in 0..20 {
            log.log(ActivityEvent::new(
                ActivityEventType::Info,
                format!("event {}", i),
            ));
        }
        let display = log.format_display(5);
        // Header lines + 5 event lines
        let lines: Vec<_> = display.lines().collect();
        // header: name, total events, errors line (if any), blank line, then 5 events
        // With 20 events and no errors: name + total + blank + 5 events = 8 lines
        assert_eq!(lines.len(), 8);
    }

    // --- TaskActivitySummary: serde roundtrip ---
    #[test]
    fn test_task_activity_summary_serde() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Created, "x"));
        let summary = log.summary();
        let json = serde_json::to_string(&summary).unwrap();
        let de: TaskActivitySummary = serde_json::from_str(&json).unwrap();
        assert_eq!(de.task_id, "t1");
        assert_eq!(de.total_events, 1);
    }

    // --- TaskActivitySummary: Clone/Debug ---
    #[test]
    fn test_task_activity_summary_clone_debug() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Info, "x"));
        let summary = log.summary();
        let cloned = summary.clone();
        assert_eq!(cloned.task_id, "t1");
        let debug = format!("{:?}", summary);
        assert!(debug.contains("TaskActivitySummary"));
    }

    // --- ActivityLogManager: default trait ---
    #[test]
    fn test_activity_log_manager_default() {
        let mgr = ActivityLogManager::default();
        assert_eq!(mgr.task_count(), 0);
        assert_eq!(mgr.total_events(), 0);
    }

    // --- ActivityLogManager: default equals new ---
    #[test]
    fn test_activity_log_manager_default_equals_new() {
        let d = ActivityLogManager::default();
        let n = ActivityLogManager::new();
        assert_eq!(d.task_count(), n.task_count());
        assert_eq!(d.total_events(), n.total_events());
    }

    // --- ActivityLogManager: with_limits ---
    #[test]
    fn test_activity_log_manager_with_limits() {
        let mgr = ActivityLogManager::new().with_limits(5, 20);
        assert_eq!(mgr.max_tasks, 5);
        assert_eq!(mgr.default_max_events, 20);
    }

    // --- ActivityLogManager: get_or_create creates new ---
    #[test]
    fn test_activity_log_manager_get_or_create_new() {
        let mut mgr = ActivityLogManager::new();
        let log = mgr.get_or_create("t1", "file.txt");
        assert_eq!(log.task_id, "t1");
        assert_eq!(log.task_name, "file.txt");
        assert!(log.is_empty());
    }

    // --- ActivityLogManager: get_or_create returns existing ---
    #[test]
    fn test_activity_log_manager_get_or_create_existing() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "t1",
            "file.txt",
            ActivityEvent::new(ActivityEventType::Info, "x"),
        );
        let log = mgr.get_or_create("t1", "file.txt");
        assert_eq!(log.len(), 1);
    }

    // --- ActivityLogManager: remove idempotent ---
    #[test]
    fn test_activity_log_manager_remove_idempotent() {
        let mut mgr = ActivityLogManager::new();
        assert!(mgr.remove("nonexistent").is_none());
        mgr.log_event("t1", "f", ActivityEvent::new(ActivityEventType::Info, "x"));
        assert!(mgr.remove("t1").is_some());
        assert!(mgr.remove("t1").is_none());
    }

    // --- ActivityLogManager: clear_all ---
    #[test]
    fn test_activity_log_manager_clear_all() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event("t1", "a", ActivityEvent::new(ActivityEventType::Info, "x"));
        mgr.log_event("t2", "b", ActivityEvent::new(ActivityEventType::Info, "y"));
        assert_eq!(mgr.task_count(), 2);
        mgr.clear_all();
        assert_eq!(mgr.task_count(), 0);
        assert_eq!(mgr.total_events(), 0);
    }

    // --- ActivityLogManager: clear_task on nonexistent ---
    #[test]
    fn test_activity_log_manager_clear_task_nonexistent() {
        let mut mgr = ActivityLogManager::new();
        mgr.clear_task("nonexistent"); // should not panic
    }

    // --- ActivityLogManager: task_summary ---
    #[test]
    fn test_activity_log_manager_task_summary() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "t1",
            "a",
            ActivityEvent::new(ActivityEventType::Created, "x"),
        );
        mgr.log_event(
            "t1",
            "a",
            ActivityEvent::new(ActivityEventType::Failed, "err"),
        );

        let summary = mgr.task_summary("t1").unwrap();
        assert_eq!(summary.total_events, 2);
        assert_eq!(summary.error_count, 1);
    }

    // --- ActivityLogManager: task_summary nonexistent ---
    #[test]
    fn test_activity_log_manager_task_summary_nonexistent() {
        let mgr = ActivityLogManager::new();
        assert!(mgr.task_summary("nonexistent").is_none());
    }

    // --- ActivityLogManager: all_summaries empty ---
    #[test]
    fn test_activity_log_manager_all_summaries_empty() {
        let mgr = ActivityLogManager::new();
        assert!(mgr.all_summaries().is_empty());
    }

    // --- ActivityLogManager: task_ids empty ---
    #[test]
    fn test_activity_log_manager_task_ids_empty() {
        let mgr = ActivityLogManager::new();
        assert!(mgr.task_ids().is_empty());
    }

    // --- ActivityLogManager: eviction with max_tasks=1 ---
    #[test]
    fn test_activity_log_manager_eviction_max_one() {
        let mut mgr = ActivityLogManager::new().with_limits(1, 10);
        mgr.log_event("t1", "a", ActivityEvent::new(ActivityEventType::Info, "1"));
        assert_eq!(mgr.task_count(), 1);
        mgr.log_event("t2", "b", ActivityEvent::new(ActivityEventType::Info, "2"));
        assert_eq!(mgr.task_count(), 1);
        // t1 should be evicted
        assert!(mgr.get("t1").is_none());
        assert!(mgr.get("t2").is_some());
    }

    // --- ActivityLogManager: Unicode task_id and name ---
    #[test]
    fn test_activity_log_manager_unicode() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "任务-1",
            "文件名.txt",
            ActivityEvent::new(ActivityEventType::Created, "创建"),
        );
        let log = mgr.get("任务-1").unwrap();
        assert_eq!(log.task_name, "文件名.txt");
    }

    // --- ActivityLogManager: emoji task_id ---
    #[test]
    fn test_activity_log_manager_emoji_id() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "🎯",
            "target",
            ActivityEvent::new(ActivityEventType::Info, "x"),
        );
        assert!(mgr.get("🎯").is_some());
    }

    // --- ActivityLogManager: Clone/Debug ---
    #[test]
    fn test_activity_log_manager_clone_debug() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event("t1", "a", ActivityEvent::new(ActivityEventType::Info, "x"));
        let cloned = mgr.clone();
        assert_eq!(cloned.task_count(), 1);
        let debug = format!("{:?}", mgr);
        assert!(debug.contains("ActivityLogManager"));
    }

    // --- ActivityLogManager: cloned independence ---
    #[test]
    fn test_activity_log_manager_clone_independence() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event("t1", "a", ActivityEvent::new(ActivityEventType::Info, "x"));
        let mut cloned = mgr.clone();
        cloned.log_event("t2", "b", ActivityEvent::new(ActivityEventType::Info, "y"));
        assert_eq!(mgr.task_count(), 1);
        assert_eq!(cloned.task_count(), 2);
    }

    // --- Persistence: save creates file ---
    #[test]
    fn test_save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ActivityLogManager::new();
        save_activity_logs(&mgr, dir.path()).unwrap();
        assert!(dir.path().join("task_activity_logs.json").exists());
    }

    // --- Persistence: overwrite ---
    #[test]
    fn test_persistence_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = ActivityLogManager::new();
        mgr.log_event("t1", "a", ActivityEvent::new(ActivityEventType::Info, "1"));
        save_activity_logs(&mgr, dir.path()).unwrap();

        let mut mgr2 = ActivityLogManager::new();
        mgr2.log_event("t2", "b", ActivityEvent::new(ActivityEventType::Info, "2"));
        save_activity_logs(&mgr2, dir.path()).unwrap();

        let loaded = load_activity_logs(dir.path()).unwrap();
        assert!(loaded.get("t1").is_none());
        assert!(loaded.get("t2").is_some());
    }

    // --- Persistence: no tmp file left ---
    #[test]
    fn test_persistence_no_tmp_left() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ActivityLogManager::new();
        save_activity_logs(&mgr, dir.path()).unwrap();
        let tmp = dir.path().join("task_activity_logs.json.tmp");
        assert!(!tmp.exists());
    }

    // --- Persistence: corrupt JSON ---
    #[test]
    fn test_persistence_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("task_activity_logs.json"),
            "not valid json{{{",
        )
        .unwrap();
        let result = load_activity_logs(dir.path());
        assert!(result.is_none());
    }

    // --- Persistence: empty file ---
    #[test]
    fn test_persistence_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("task_activity_logs.json"), "").unwrap();
        let result = load_activity_logs(dir.path());
        assert!(result.is_none());
    }

    // --- Persistence: full roundtrip with data ---
    #[test]
    fn test_persistence_full_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = ActivityLogManager::new().with_limits(10, 50);
        mgr.log_event(
            "t1",
            "电影.mkv",
            ActivityEvent::new(ActivityEventType::Created, "添加"),
        );
        mgr.log_event(
            "t1",
            "电影.mkv",
            ActivityEvent::new(ActivityEventType::Started, "开始下载").with_value(3.0),
        );
        mgr.log_event(
            "t1",
            "电影.mkv",
            ActivityEvent::new(ActivityEventType::ConnectionError, "连接失败"),
        );
        mgr.log_event(
            "t2",
            "文档.pdf",
            ActivityEvent::new(ActivityEventType::Completed, "完成"),
        );

        save_activity_logs(&mgr, dir.path()).unwrap();
        let loaded = load_activity_logs(dir.path()).unwrap();

        assert_eq!(loaded.task_count(), 2);
        assert_eq!(loaded.total_events(), 4);
        let log1 = loaded.get("t1").unwrap();
        assert_eq!(log1.task_name, "电影.mkv");
        assert_eq!(log1.len(), 3);
        let events: Vec<_> = log1.events().collect();
        assert_eq!(events[1].numeric_value, Some(3.0));
    }

    // --- Persistence: pretty JSON ---
    #[test]
    fn test_persistence_pretty_json() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = ActivityLogManager::new();
        mgr.log_event(
            "t1",
            "test",
            ActivityEvent::new(ActivityEventType::Info, "x"),
        );
        save_activity_logs(&mgr, dir.path()).unwrap();

        let content = std::fs::read_to_string(dir.path().join("task_activity_logs.json")).unwrap();
        // Pretty JSON should have newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
    }

    // --- Constants ---
    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_MAX_EVENTS, 100);
        assert_eq!(DEFAULT_MAX_TASKS, 200);
    }

    // --- Complex workflow: full lifecycle ---
    #[test]
    fn test_complex_workflow_full_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = ActivityLogManager::new();

        // Create tasks
        mgr.log_event(
            "t1",
            "movie.mkv",
            ActivityEvent::new(ActivityEventType::Created, "added"),
        );
        mgr.log_event(
            "t2",
            "doc.pdf",
            ActivityEvent::new(ActivityEventType::Created, "added"),
        );

        // Start downloading
        mgr.log_event(
            "t1",
            "movie.mkv",
            ActivityEvent::new(ActivityEventType::Started, "downloading"),
        );

        // Error and retry
        mgr.log_event(
            "t1",
            "movie.mkv",
            ActivityEvent::new(ActivityEventType::ConnectionError, "timeout"),
        );
        mgr.log_event(
            "t1",
            "movie.mkv",
            ActivityEvent::new(ActivityEventType::AutoRetry, "retry #1"),
        );

        // Progress milestones
        mgr.log_event(
            "t1",
            "movie.mkv",
            ActivityEvent::new(ActivityEventType::ProgressMilestone, "25%").with_value(25.0),
        );
        mgr.log_event(
            "t1",
            "movie.mkv",
            ActivityEvent::new(ActivityEventType::ProgressMilestone, "50%").with_value(50.0),
        );

        // Mirror switch
        mgr.log_event(
            "t1",
            "movie.mkv",
            ActivityEvent::new(ActivityEventType::MirrorSwitched, "to mirror2"),
        );

        // Complete
        mgr.log_event(
            "t1",
            "movie.mkv",
            ActivityEvent::new(ActivityEventType::Completed, "done"),
        );

        // Verify
        assert_eq!(mgr.task_count(), 2);
        let summary = mgr.task_summary("t1").unwrap();
        assert_eq!(summary.total_events, 8);
        assert_eq!(summary.error_count, 1);

        // Save and reload
        save_activity_logs(&mgr, dir.path()).unwrap();
        let loaded = load_activity_logs(dir.path()).unwrap();
        assert_eq!(loaded.task_count(), 2);
        let log = loaded.get("t1").unwrap();
        assert_eq!(log.len(), 8);

        // Format display
        let display = log.format_display(5);
        assert!(display.contains("movie.mkv"));
        assert!(display.contains("Errors: 1"));
    }

    // --- Complex workflow: many tasks independent ---
    #[test]
    fn test_complex_many_tasks_independent() {
        let mut mgr = ActivityLogManager::new();
        for i in 0..10 {
            let task_id = format!("task-{}", i);
            let name = format!("file-{}.txt", i);
            mgr.log_event(
                &task_id,
                &name,
                ActivityEvent::new(ActivityEventType::Created, "new"),
            );
            mgr.log_event(
                &task_id,
                &name,
                ActivityEvent::new(ActivityEventType::Started, "go"),
            );
        }
        assert_eq!(mgr.task_count(), 10);
        assert_eq!(mgr.total_events(), 20);

        // Remove some
        mgr.remove("task-0");
        mgr.remove("task-5");
        assert_eq!(mgr.task_count(), 8);
        assert_eq!(mgr.total_events(), 16);
    }

    // --- ActivityLogManager: serde extra fields ignored ---
    #[test]
    fn test_activity_log_manager_serde_extra_fields() {
        let mut mgr = ActivityLogManager::new();
        mgr.log_event("t1", "a", ActivityEvent::new(ActivityEventType::Info, "x"));
        let mut json: serde_json::Value = serde_json::to_value(&mgr).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("unknown_key".into(), serde_json::json!(42));
        let de: ActivityLogManager = serde_json::from_value(json).unwrap();
        assert_eq!(de.task_count(), 1);
    }

    // --- ActivityLogManager: serde roundtrip preserves limits ---
    #[test]
    fn test_activity_log_manager_serde_preserves_limits() {
        let mgr = ActivityLogManager::new().with_limits(50, 25);
        let json = serde_json::to_string(&mgr).unwrap();
        let de: ActivityLogManager = serde_json::from_str(&json).unwrap();
        assert_eq!(de.max_tasks, 50);
        assert_eq!(de.default_max_events, 25);
    }

    // --- ActivityEvent: Clone/Debug ---
    #[test]
    fn test_activity_event_clone_debug() {
        let event = ActivityEvent::new(ActivityEventType::Created, "test").with_value(42.0);
        let cloned = event.clone();
        assert_eq!(cloned.message, "test");
        assert_eq!(cloned.numeric_value, Some(42.0));
        let debug = format!("{:?}", event);
        assert!(debug.contains("ActivityEvent"));
    }

    // --- TaskActivityLog: Clone/Debug ---
    #[test]
    fn test_task_activity_log_clone_debug() {
        let mut log = TaskActivityLog::new("t1", "test");
        log.log(ActivityEvent::new(ActivityEventType::Info, "x"));
        let cloned = log.clone();
        assert_eq!(cloned.len(), 1);
        let debug = format!("{:?}", log);
        assert!(debug.contains("TaskActivityLog"));
    }
}
