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
                    .unwrap_or_else(|| Utc::now())
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
}
