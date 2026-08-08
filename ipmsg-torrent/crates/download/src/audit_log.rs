//! Download Audit Log
//!
//! Tracks all download lifecycle events for debugging and visibility.
//! Each event is timestamped and includes task metadata.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::Path;

/// Maximum number of audit log entries to keep in memory
const MAX_AUDIT_ENTRIES: usize = 1000;

/// Types of audit events
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    TaskAdded,
    TaskStarted,
    TaskPaused,
    TaskResumed,
    TaskCompleted,
    TaskFailed,
    TaskRemoved,
    TaskRetry,
    SpeedLimitChanged,
    TagsChanged,
    GroupChanged,
    NotesChanged,
    PriorityChanged,
    ProxyChanged,
    ConfigChanged,
}

impl std::fmt::Display for AuditEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditEventType::TaskAdded => write!(f, "task_added"),
            AuditEventType::TaskStarted => write!(f, "task_started"),
            AuditEventType::TaskPaused => write!(f, "task_paused"),
            AuditEventType::TaskResumed => write!(f, "task_resumed"),
            AuditEventType::TaskCompleted => write!(f, "task_completed"),
            AuditEventType::TaskFailed => write!(f, "task_failed"),
            AuditEventType::TaskRemoved => write!(f, "task_removed"),
            AuditEventType::TaskRetry => write!(f, "task_retry"),
            AuditEventType::SpeedLimitChanged => write!(f, "speed_limit_changed"),
            AuditEventType::TagsChanged => write!(f, "tags_changed"),
            AuditEventType::GroupChanged => write!(f, "group_changed"),
            AuditEventType::NotesChanged => write!(f, "notes_changed"),
            AuditEventType::PriorityChanged => write!(f, "priority_changed"),
            AuditEventType::ProxyChanged => write!(f, "proxy_changed"),
            AuditEventType::ConfigChanged => write!(f, "config_changed"),
        }
    }
}

/// A single audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub timestamp: DateTime<Utc>,
    pub event_type: AuditEventType,
    pub task_id: Option<String>,
    pub task_name: Option<String>,
    pub protocol: Option<String>,
    pub details: Option<String>,
    pub user: Option<String>,
}

impl AuditLogEntry {
    pub fn new(
        event_type: AuditEventType,
        task_id: Option<String>,
        task_name: Option<String>,
        protocol: Option<String>,
        details: Option<String>,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            event_type,
            task_id,
            task_name,
            protocol,
            details,
            user: None,
        }
    }

    pub fn with_user(mut self, user: String) -> Self {
        self.user = Some(user);
        self
    }

    /// Format the entry for display
    pub fn format_display(&self) -> String {
        let time_str = self.timestamp.format("%Y-%m-%d %H:%M:%S").to_string();
        let task_info = match (&self.task_id, &self.task_name) {
            (Some(id), Some(name)) => format!("[{}: {}]", id, name),
            (Some(id), None) => format!("[{}]", id),
            _ => String::new(),
        };
        let proto = self.protocol.as_deref().unwrap_or("-");
        let details = self.details.as_deref().unwrap_or("");
        let user = self
            .user
            .as_ref()
            .map(|u| format!(" by {}", u))
            .unwrap_or_default();

        format!(
            "{} {:<20} {:<10} {}{}{}",
            time_str, self.event_type, proto, task_info, details, user
        )
    }
}

/// Audit log manager
#[derive(Debug, Clone)]
pub struct AuditLog {
    entries: VecDeque<AuditLogEntry>,
    max_entries: usize,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditLog {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries: MAX_AUDIT_ENTRIES,
        }
    }

    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
        }
    }

    /// Add an entry to the audit log
    pub fn log(&mut self, entry: AuditLogEntry) {
        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// Get all entries
    pub fn entries(&self) -> impl Iterator<Item = &AuditLogEntry> {
        self.entries.iter()
    }

    /// Get entries in reverse chronological order
    pub fn entries_reverse(&self) -> impl Iterator<Item = &AuditLogEntry> {
        self.entries.iter().rev()
    }

    /// Get the most recent N entries
    pub fn recent(&self, n: usize) -> Vec<&AuditLogEntry> {
        self.entries.iter().rev().take(n).collect()
    }

    /// Get entries filtered by event type
    pub fn entries_by_type(&self, event_type: &AuditEventType) -> Vec<&AuditLogEntry> {
        self.entries
            .iter()
            .filter(|e| &e.event_type == event_type)
            .collect()
    }

    /// Get entries filtered by task ID
    pub fn entries_by_task(&self, task_id: &str) -> Vec<&AuditLogEntry> {
        self.entries
            .iter()
            .filter(|e| e.task_id.as_deref() == Some(task_id))
            .collect()
    }

    /// Get entries within a time range
    pub fn entries_in_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<&AuditLogEntry> {
        self.entries
            .iter()
            .filter(|e| e.timestamp >= start && e.timestamp <= end)
            .collect()
    }

    /// Get the total number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the log is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Get summary statistics
    pub fn summary(&self) -> AuditLogSummary {
        let mut event_counts = std::collections::HashMap::new();
        for entry in &self.entries {
            *event_counts.entry(entry.event_type.clone()).or_insert(0) += 1;
        }

        AuditLogSummary {
            total_entries: self.entries.len(),
            oldest_entry: self.entries.front().map(|e| e.timestamp),
            newest_entry: self.entries.back().map(|e| e.timestamp),
            event_counts,
        }
    }

    /// Export entries as JSON
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.entries)
    }

    /// Import entries from JSON
    pub fn import_json(&mut self, json: &str) -> Result<usize, serde_json::Error> {
        let imported: Vec<AuditLogEntry> = serde_json::from_str(json)?;
        let count = imported.len();
        for entry in imported {
            self.log(entry);
        }
        Ok(count)
    }
}

/// Summary of the audit log
#[derive(Debug, Clone)]
pub struct AuditLogSummary {
    pub total_entries: usize,
    pub oldest_entry: Option<DateTime<Utc>>,
    pub newest_entry: Option<DateTime<Utc>>,
    pub event_counts: std::collections::HashMap<AuditEventType, usize>,
}

impl std::fmt::Display for AuditLogSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "📋 Audit Log Summary")?;
        writeln!(f, "  Total entries: {}", self.total_entries)?;
        if let Some(oldest) = self.oldest_entry {
            writeln!(f, "  Oldest: {}", oldest.format("%Y-%m-%d %H:%M:%S"))?;
        }
        if let Some(newest) = self.newest_entry {
            writeln!(f, "  Newest: {}", newest.format("%Y-%m-%d %H:%M:%S"))?;
        }
        if !self.event_counts.is_empty() {
            writeln!(f, "  Events:")?;
            let mut sorted: Vec<_> = self.event_counts.iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(a.1));
            for (event, count) in sorted {
                writeln!(f, "    {:<25} {}", event, count)?;
            }
        }
        Ok(())
    }
}

/// Save audit log to disk
pub fn save_audit_log(log: &AuditLog, data_dir: &Path) -> Result<(), std::io::Error> {
    let path = data_dir.join("audit_log.json");
    let json = serde_json::to_string_pretty(&log.entries)
        .map_err(std::io::Error::other)?;

    // Atomic write
    let temp_path = path.with_extension("json.tmp");
    std::fs::write(&temp_path, json)?;
    std::fs::rename(&temp_path, &path)?;
    Ok(())
}

/// Load audit log from disk
pub fn load_audit_log(data_dir: &Path) -> Option<AuditLog> {
    let path = data_dir.join("audit_log.json");
    let json = std::fs::read_to_string(&path).ok()?;
    let entries: Vec<AuditLogEntry> = serde_json::from_str(&json).ok()?;
    let mut log = AuditLog::new();
    for entry in entries {
        log.log(entry);
    }
    Some(log)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_type_display() {
        assert_eq!(AuditEventType::TaskAdded.to_string(), "task_added");
        assert_eq!(AuditEventType::TaskCompleted.to_string(), "task_completed");
        assert_eq!(
            AuditEventType::SpeedLimitChanged.to_string(),
            "speed_limit_changed"
        );
    }

    #[test]
    fn test_audit_log_entry_creation() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("task-123".to_string()),
            Some("test.txt".to_string()),
            Some("http".to_string()),
            Some("URL: http://example.com/test.txt".to_string()),
        );

        assert_eq!(entry.event_type, AuditEventType::TaskAdded);
        assert_eq!(entry.task_id.as_deref(), Some("task-123"));
        assert_eq!(entry.task_name.as_deref(), Some("test.txt"));
        assert_eq!(entry.protocol.as_deref(), Some("http"));
        assert!(entry.details.is_some());
        assert!(entry.user.is_none());
    }

    #[test]
    fn test_audit_log_entry_with_user() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskPaused,
            Some("task-456".to_string()),
            None,
            None,
            None,
        )
        .with_user("admin".to_string());

        assert_eq!(entry.user.as_deref(), Some("admin"));
    }

    #[test]
    fn test_audit_log_entry_format_display() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("abc123".to_string()),
            Some("file.zip".to_string()),
            Some("torrent".to_string()),
            Some(" (1.5 GB)".to_string()),
        );

        let display = entry.format_display();
        assert!(display.contains("task_completed"));
        assert!(display.contains("torrent"));
        assert!(display.contains("abc123"));
        assert!(display.contains("file.zip"));
        assert!(display.contains("1.5 GB"));
    }

    #[test]
    fn test_audit_log_basic_operations() {
        let mut log = AuditLog::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);

        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));

        assert!(!log.is_empty());
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn test_audit_log_max_entries() {
        let mut log = AuditLog::with_max_entries(5);

        for i in 0..10 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("task-{}", i)),
                None,
                None,
                None,
            ));
        }

        assert_eq!(log.len(), 5);
        // Oldest entries should be evicted
        let first = log.entries().next().unwrap();
        assert_eq!(first.task_id.as_deref(), Some("task-5"));
    }

    #[test]
    fn test_audit_log_recent() {
        let mut log = AuditLog::new();

        for i in 0..10 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("task-{}", i)),
                None,
                None,
                None,
            ));
        }

        let recent = log.recent(3);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].task_id.as_deref(), Some("task-9"));
        assert_eq!(recent[1].task_id.as_deref(), Some("task-8"));
        assert_eq!(recent[2].task_id.as_deref(), Some("task-7"));
    }

    #[test]
    fn test_audit_log_entries_by_type() {
        let mut log = AuditLog::new();

        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskStarted,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t2".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));

        let added = log.entries_by_type(&AuditEventType::TaskAdded);
        assert_eq!(added.len(), 2);

        let started = log.entries_by_type(&AuditEventType::TaskStarted);
        assert_eq!(started.len(), 1);
    }

    #[test]
    fn test_audit_log_entries_by_task() {
        let mut log = AuditLog::new();

        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskStarted,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t2".to_string()),
            None,
            None,
            None,
        ));

        let t1_entries = log.entries_by_task("t1");
        assert_eq!(t1_entries.len(), 2);

        let t2_entries = log.entries_by_task("t2");
        assert_eq!(t2_entries.len(), 1);

        let t3_entries = log.entries_by_task("t3");
        assert_eq!(t3_entries.len(), 0);
    }

    #[test]
    fn test_audit_log_clear() {
        let mut log = AuditLog::new();

        for i in 0..5 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("task-{}", i)),
                None,
                None,
                None,
            ));
        }

        assert_eq!(log.len(), 5);
        log.clear();
        assert_eq!(log.len(), 0);
        assert!(log.is_empty());
    }

    #[test]
    fn test_audit_log_summary() {
        let mut log = AuditLog::new();

        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskStarted,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t2".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));

        let summary = log.summary();
        assert_eq!(summary.total_entries, 4);
        assert!(summary.oldest_entry.is_some());
        assert!(summary.newest_entry.is_some());
        assert_eq!(
            *summary
                .event_counts
                .get(&AuditEventType::TaskAdded)
                .unwrap(),
            2
        );
        assert_eq!(
            *summary
                .event_counts
                .get(&AuditEventType::TaskStarted)
                .unwrap(),
            1
        );
        assert_eq!(
            *summary
                .event_counts
                .get(&AuditEventType::TaskCompleted)
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_audit_log_summary_display() {
        let mut log = AuditLog::new();

        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));

        let summary = log.summary();
        let display = summary.to_string();

        assert!(display.contains("Audit Log Summary"));
        assert!(display.contains("Total entries: 2"));
        assert!(display.contains("task_added"));
        assert!(display.contains("task_completed"));
    }

    #[test]
    fn test_audit_log_export_import_json() {
        let mut log = AuditLog::new();

        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            Some("file1.txt".to_string()),
            Some("http".to_string()),
            Some("Test entry".to_string()),
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("t1".to_string()),
            Some("file1.txt".to_string()),
            Some("http".to_string()),
            None,
        ));

        let json = log.export_json().unwrap();
        assert!(json.contains("task_added"));
        assert!(json.contains("task_completed"));

        let mut imported_log = AuditLog::new();
        let count = imported_log.import_json(&json).unwrap();
        assert_eq!(count, 2);
        assert_eq!(imported_log.len(), 2);
    }

    #[test]
    fn test_audit_log_save_load() {
        let temp_dir = std::env::temp_dir().join("test_audit_log_save_load");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            Some("file.txt".to_string()),
            Some("http".to_string()),
            None,
        ));

        save_audit_log(&log, &temp_dir).unwrap();

        let loaded = load_audit_log(&temp_dir).unwrap();
        assert_eq!(loaded.len(), 1);
        let entry = loaded.entries().next().unwrap();
        assert_eq!(entry.event_type, AuditEventType::TaskAdded);
        assert_eq!(entry.task_id.as_deref(), Some("t1"));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_audit_log_load_nonexistent() {
        let temp_dir = std::env::temp_dir().join("test_audit_log_nonexistent");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let loaded = load_audit_log(&temp_dir);
        assert!(loaded.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_audit_log_entries_reverse() {
        let mut log = AuditLog::new();

        for i in 0..5 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("task-{}", i)),
                None,
                None,
                None,
            ));
        }

        let reversed: Vec<_> = log.entries_reverse().collect();
        assert_eq!(reversed.len(), 5);
        assert_eq!(reversed[0].task_id.as_deref(), Some("task-4"));
        assert_eq!(reversed[4].task_id.as_deref(), Some("task-0"));
    }

    #[test]
    fn test_audit_event_type_serialization() {
        let event = AuditEventType::TaskStarted;
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(json, "\"task_started\"");

        let deserialized: AuditEventType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, AuditEventType::TaskStarted);
    }

    #[test]
    fn test_audit_log_entry_serialization() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            Some("file.txt".to_string()),
            Some("http".to_string()),
            Some("details".to_string()),
        )
        .with_user("user1".to_string());

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: AuditLogEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.event_type, AuditEventType::TaskAdded);
        assert_eq!(deserialized.task_id.as_deref(), Some("t1"));
        assert_eq!(deserialized.task_name.as_deref(), Some("file.txt"));
        assert_eq!(deserialized.user.as_deref(), Some("user1"));
    }
}
