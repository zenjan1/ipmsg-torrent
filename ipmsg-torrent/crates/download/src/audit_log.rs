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
    let json = serde_json::to_string_pretty(&log.entries).map_err(std::io::Error::other)?;

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

    // ── Phase 243: Comprehensive test coverage ──

    // === AuditEventType: all 15 variants Display ===

    #[test]
    fn test_event_type_display_all_variants() {
        assert_eq!(AuditEventType::TaskAdded.to_string(), "task_added");
        assert_eq!(AuditEventType::TaskStarted.to_string(), "task_started");
        assert_eq!(AuditEventType::TaskPaused.to_string(), "task_paused");
        assert_eq!(AuditEventType::TaskResumed.to_string(), "task_resumed");
        assert_eq!(AuditEventType::TaskCompleted.to_string(), "task_completed");
        assert_eq!(AuditEventType::TaskFailed.to_string(), "task_failed");
        assert_eq!(AuditEventType::TaskRemoved.to_string(), "task_removed");
        assert_eq!(AuditEventType::TaskRetry.to_string(), "task_retry");
        assert_eq!(
            AuditEventType::SpeedLimitChanged.to_string(),
            "speed_limit_changed"
        );
        assert_eq!(AuditEventType::TagsChanged.to_string(), "tags_changed");
        assert_eq!(AuditEventType::GroupChanged.to_string(), "group_changed");
        assert_eq!(AuditEventType::NotesChanged.to_string(), "notes_changed");
        assert_eq!(
            AuditEventType::PriorityChanged.to_string(),
            "priority_changed"
        );
        assert_eq!(AuditEventType::ProxyChanged.to_string(), "proxy_changed");
        assert_eq!(AuditEventType::ConfigChanged.to_string(), "config_changed");
    }

    // === AuditEventType: serde roundtrip all variants ===

    #[test]
    fn test_event_type_serde_roundtrip_all_variants() {
        let variants = vec![
            AuditEventType::TaskAdded,
            AuditEventType::TaskStarted,
            AuditEventType::TaskPaused,
            AuditEventType::TaskResumed,
            AuditEventType::TaskCompleted,
            AuditEventType::TaskFailed,
            AuditEventType::TaskRemoved,
            AuditEventType::TaskRetry,
            AuditEventType::SpeedLimitChanged,
            AuditEventType::TagsChanged,
            AuditEventType::GroupChanged,
            AuditEventType::NotesChanged,
            AuditEventType::PriorityChanged,
            AuditEventType::ProxyChanged,
            AuditEventType::ConfigChanged,
        ];
        for variant in &variants {
            let json = serde_json::to_string(variant).unwrap();
            let deserialized: AuditEventType = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn test_event_type_serde_snake_case_values() {
        // Verify JSON uses snake_case, not PascalCase
        let json = serde_json::to_string(&AuditEventType::TaskAdded).unwrap();
        assert_eq!(json, "\"task_added\"");
        assert!(!json.contains("TaskAdded"));

        let json = serde_json::to_string(&AuditEventType::SpeedLimitChanged).unwrap();
        assert_eq!(json, "\"speed_limit_changed\"");
    }

    // === AuditEventType: traits ===

    #[test]
    fn test_event_type_clone_eq() {
        let a = AuditEventType::TaskCompleted;
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_event_type_debug() {
        let debug = format!("{:?}", AuditEventType::TaskFailed);
        assert!(debug.contains("TaskFailed"));
    }

    #[test]
    fn test_event_type_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(AuditEventType::TaskAdded);
        set.insert(AuditEventType::TaskAdded);
        set.insert(AuditEventType::TaskCompleted);
        assert_eq!(set.len(), 2);
    }

    // === AuditLogEntry: constructors and builders ===

    #[test]
    fn test_entry_new_minimal_fields() {
        let entry = AuditLogEntry::new(AuditEventType::TaskAdded, None, None, None, None);
        assert_eq!(entry.event_type, AuditEventType::TaskAdded);
        assert!(entry.task_id.is_none());
        assert!(entry.task_name.is_none());
        assert!(entry.protocol.is_none());
        assert!(entry.details.is_none());
        assert!(entry.user.is_none());
    }

    #[test]
    fn test_entry_new_all_fields() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskStarted,
            Some("id-1".to_string()),
            Some("name.txt".to_string()),
            Some("http".to_string()),
            Some("some details".to_string()),
        );
        assert_eq!(entry.task_id.as_deref(), Some("id-1"));
        assert_eq!(entry.task_name.as_deref(), Some("name.txt"));
        assert_eq!(entry.protocol.as_deref(), Some("http"));
        assert_eq!(entry.details.as_deref(), Some("some details"));
    }

    #[test]
    fn test_entry_with_user_chain() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskPaused,
            Some("t1".to_string()),
            None,
            None,
            None,
        )
        .with_user("admin".to_string());
        assert_eq!(entry.user.as_deref(), Some("admin"));
    }

    #[test]
    fn test_entry_with_user_overwrite() {
        let entry = AuditLogEntry::new(AuditEventType::TaskPaused, None, None, None, None)
            .with_user("first".to_string())
            .with_user("second".to_string());
        assert_eq!(entry.user.as_deref(), Some("second"));
    }

    // === AuditLogEntry: format_display ===

    #[test]
    fn test_format_display_all_fields() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("abc".to_string()),
            Some("file.zip".to_string()),
            Some("torrent".to_string()),
            Some(" (1.5 GB)".to_string()),
        )
        .with_user("user1".to_string());

        let display = entry.format_display();
        assert!(display.contains("task_completed"));
        assert!(display.contains("torrent"));
        assert!(display.contains("[abc: file.zip]"));
        assert!(display.contains("1.5 GB"));
        assert!(display.contains("by user1"));
    }

    #[test]
    fn test_format_display_no_task_info() {
        let entry = AuditLogEntry::new(
            AuditEventType::ConfigChanged,
            None,
            None,
            Some("http".to_string()),
            None,
        );
        let display = entry.format_display();
        assert!(display.contains("config_changed"));
        // No bracket info when no task_id
        assert!(!display.contains("["));
    }

    #[test]
    fn test_format_display_task_id_only() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t-123".to_string()),
            None,
            None,
            None,
        );
        let display = entry.format_display();
        assert!(display.contains("[t-123]"));
        // No colon+name format
        assert!(!display.contains(": "));
    }

    #[test]
    fn test_format_display_no_protocol() {
        let entry = AuditLogEntry::new(AuditEventType::TaskAdded, None, None, None, None);
        let display = entry.format_display();
        // Protocol defaults to "-"
        assert!(display.contains("-"));
    }

    #[test]
    fn test_format_display_no_user() {
        let entry = AuditLogEntry::new(AuditEventType::TaskAdded, None, None, None, None);
        let display = entry.format_display();
        assert!(!display.contains("by "));
    }

    // === AuditLogEntry: serde ===

    #[test]
    fn test_entry_serde_roundtrip_all_fields() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskRetry,
            Some("rt-1".to_string()),
            Some("retry.txt".to_string()),
            Some("ftp".to_string()),
            Some("retry attempt 3".to_string()),
        )
        .with_user("operator".to_string());

        let json = serde_json::to_string(&entry).unwrap();
        let de: AuditLogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(de.event_type, AuditEventType::TaskRetry);
        assert_eq!(de.task_id.as_deref(), Some("rt-1"));
        assert_eq!(de.task_name.as_deref(), Some("retry.txt"));
        assert_eq!(de.protocol.as_deref(), Some("ftp"));
        assert_eq!(de.details.as_deref(), Some("retry attempt 3"));
        assert_eq!(de.user.as_deref(), Some("operator"));
    }

    #[test]
    fn test_entry_serde_null_optional_fields() {
        let entry = AuditLogEntry::new(AuditEventType::TaskAdded, None, None, None, None);
        let json = serde_json::to_string(&entry).unwrap();
        let de: AuditLogEntry = serde_json::from_str(&json).unwrap();
        assert!(de.task_id.is_none());
        assert!(de.task_name.is_none());
        assert!(de.protocol.is_none());
        assert!(de.details.is_none());
        assert!(de.user.is_none());
    }

    #[test]
    fn test_entry_serde_extra_fields_ignored() {
        let json = r#"{
            "timestamp": "2026-01-01T00:00:00Z",
            "event_type": "task_added",
            "task_id": "t1",
            "task_name": null,
            "protocol": null,
            "details": null,
            "user": null,
            "extra_field": "should be ignored"
        }"#;
        let de: AuditLogEntry = serde_json::from_str(json).unwrap();
        assert_eq!(de.task_id.as_deref(), Some("t1"));
    }

    // === AuditLogEntry: Clone/Debug traits ===

    #[test]
    fn test_entry_clone() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            Some("name".to_string()),
            Some("http".to_string()),
            Some("details".to_string()),
        )
        .with_user("user".to_string());

        let cloned = entry.clone();
        assert_eq!(cloned.event_type, entry.event_type);
        assert_eq!(cloned.task_id, entry.task_id);
        assert_eq!(cloned.user, entry.user);
    }

    #[test]
    fn test_entry_debug() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("dbg-1".to_string()),
            None,
            None,
            None,
        );
        let debug = format!("{:?}", entry);
        assert!(debug.contains("dbg-1"));
        assert!(debug.contains("AuditLogEntry"));
    }

    // === AuditLog: new/default/with_max_entries ===

    #[test]
    fn test_audit_log_default() {
        let log = AuditLog::default();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_audit_log_new_equals_default() {
        let a = AuditLog::new();
        let b = AuditLog::default();
        assert_eq!(a.len(), b.len());
    }

    #[test]
    fn test_audit_log_with_max_entries_custom() {
        let log = AuditLog::with_max_entries(50);
        assert_eq!(log.max_entries, 50);
    }

    // === AuditLog: log() and eviction ===

    #[test]
    fn test_log_eviction_exact_boundary() {
        let mut log = AuditLog::with_max_entries(3);
        for i in 0..3 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("t{}", i)),
                None,
                None,
                None,
            ));
        }
        assert_eq!(log.len(), 3);

        // One more triggers eviction of oldest
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t3".to_string()),
            None,
            None,
            None,
        ));
        assert_eq!(log.len(), 3);
        let first = log.entries().next().unwrap();
        assert_eq!(first.task_id.as_deref(), Some("t1"));
    }

    #[test]
    fn test_log_eviction_max_one() {
        let mut log = AuditLog::with_max_entries(1);
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("first".to_string()),
            None,
            None,
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("second".to_string()),
            None,
            None,
            None,
        ));
        assert_eq!(log.len(), 1);
        assert_eq!(
            log.entries().next().unwrap().task_id.as_deref(),
            Some("second")
        );
    }

    #[test]
    fn test_log_no_eviction_under_limit() {
        let mut log = AuditLog::with_max_entries(100);
        for i in 0..50 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("t{}", i)),
                None,
                None,
                None,
            ));
        }
        assert_eq!(log.len(), 50);
    }

    // === AuditLog: entries_by_type all variants ===

    #[test]
    fn test_entries_by_type_no_match() {
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        let result = log.entries_by_type(&AuditEventType::TaskCompleted);
        assert!(result.is_empty());
    }

    #[test]
    fn test_entries_by_type_multiple_matches() {
        let mut log = AuditLog::new();
        for _ in 0..5 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                None,
                None,
                None,
                None,
            ));
        }
        log.log(AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            None,
            None,
            None,
            None,
        ));
        assert_eq!(log.entries_by_type(&AuditEventType::TaskAdded).len(), 5);
        assert_eq!(log.entries_by_type(&AuditEventType::TaskCompleted).len(), 1);
    }

    // === AuditLog: entries_by_task ===

    #[test]
    fn test_entries_by_task_empty_id() {
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("".to_string()),
            None,
            None,
            None,
        ));
        // Searching for empty string should match
        assert_eq!(log.entries_by_task("").len(), 1);
    }

    #[test]
    fn test_entries_by_task_no_match() {
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        assert!(log.entries_by_task("nonexistent").is_empty());
    }

    // === AuditLog: entries_in_range ===

    #[test]
    fn test_entries_in_range_all() {
        let mut log = AuditLog::new();
        let now = Utc::now();
        for i in 0..5 {
            let mut entry = AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("t{}", i)),
                None,
                None,
                None,
            );
            entry.timestamp = now + chrono::Duration::seconds(i as i64);
            log.log(entry);
        }

        let start = now - chrono::Duration::seconds(1);
        let end = now + chrono::Duration::seconds(10);
        let result = log.entries_in_range(start, end);
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_entries_in_range_partial() {
        let mut log = AuditLog::new();
        let base = Utc::now();
        for i in 0..10 {
            let mut entry = AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("t{}", i)),
                None,
                None,
                None,
            );
            entry.timestamp = base + chrono::Duration::seconds(i as i64);
            log.log(entry);
        }

        // Only entries 3..=6
        let start = base + chrono::Duration::seconds(3);
        let end = base + chrono::Duration::seconds(6);
        let result = log.entries_in_range(start, end);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_entries_in_range_empty() {
        let mut log = AuditLog::new();
        let now = Utc::now();
        for i in 0..3 {
            let mut entry = AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("t{}", i)),
                None,
                None,
                None,
            );
            entry.timestamp = now + chrono::Duration::seconds(i as i64);
            log.log(entry);
        }

        // Range far in the past
        let start = now - chrono::Duration::hours(2);
        let end = now - chrono::Duration::hours(1);
        let result = log.entries_in_range(start, end);
        assert!(result.is_empty());
    }

    // === AuditLog: recent() ===

    #[test]
    fn test_recent_more_than_available() {
        let mut log = AuditLog::new();
        for i in 0..3 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("t{}", i)),
                None,
                None,
                None,
            ));
        }
        let recent = log.recent(10);
        assert_eq!(recent.len(), 3);
    }

    #[test]
    fn test_recent_zero() {
        let mut log = AuditLog::new();
        for i in 0..5 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("t{}", i)),
                None,
                None,
                None,
            ));
        }
        let recent = log.recent(0);
        assert!(recent.is_empty());
    }

    // === AuditLog: summary ===

    #[test]
    fn test_summary_empty() {
        let log = AuditLog::new();
        let summary = log.summary();
        assert_eq!(summary.total_entries, 0);
        assert!(summary.oldest_entry.is_none());
        assert!(summary.newest_entry.is_none());
        assert!(summary.event_counts.is_empty());
    }

    #[test]
    fn test_summary_single_entry() {
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        let summary = log.summary();
        assert_eq!(summary.total_entries, 1);
        assert!(summary.oldest_entry.is_some());
        assert!(summary.newest_entry.is_some());
        // Oldest == newest for single entry
        assert_eq!(summary.oldest_entry, summary.newest_entry);
    }

    #[test]
    fn test_summary_event_counts_correctness() {
        let mut log = AuditLog::new();
        for _ in 0..10 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                None,
                None,
                None,
                None,
            ));
        }
        for _ in 0..5 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskCompleted,
                None,
                None,
                None,
                None,
            ));
        }
        for _ in 0..3 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskFailed,
                None,
                None,
                None,
                None,
            ));
        }
        let summary = log.summary();
        assert_eq!(summary.total_entries, 18);
        assert_eq!(
            *summary
                .event_counts
                .get(&AuditEventType::TaskAdded)
                .unwrap(),
            10
        );
        assert_eq!(
            *summary
                .event_counts
                .get(&AuditEventType::TaskCompleted)
                .unwrap(),
            5
        );
        assert_eq!(
            *summary
                .event_counts
                .get(&AuditEventType::TaskFailed)
                .unwrap(),
            3
        );
    }

    // === AuditLogSummary: Display ===

    #[test]
    fn test_summary_display_empty() {
        let log = AuditLog::new();
        let summary = log.summary();
        let display = summary.to_string();
        assert!(display.contains("Total entries: 0"));
        // No "Oldest" or "Newest" lines
        assert!(!display.contains("Oldest:"));
        assert!(!display.contains("Newest:"));
    }

    #[test]
    fn test_summary_display_with_entries() {
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
        assert!(display.contains("📋 Audit Log Summary"));
        assert!(display.contains("Total entries: 2"));
        assert!(display.contains("Oldest:"));
        assert!(display.contains("Newest:"));
        assert!(display.contains("Events:"));
    }

    #[test]
    fn test_summary_display_sorted_by_count() {
        let mut log = AuditLog::new();
        // 5 TaskAdded
        for _ in 0..5 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                None,
                None,
                None,
                None,
            ));
        }
        // 1 TaskCompleted
        log.log(AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            None,
            None,
            None,
            None,
        ));
        let summary = log.summary();
        let display = summary.to_string();
        // task_added should appear before task_completed (higher count)
        let added_pos = display.find("task_added").unwrap();
        let completed_pos = display.find("task_completed").unwrap();
        assert!(added_pos < completed_pos);
    }

    // === Persistence: save/load ===

    #[test]
    fn test_save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        save_audit_log(&log, dir.path()).unwrap();
        assert!(dir.path().join("audit_log.json").exists());
    }

    #[test]
    fn test_save_no_tmp_leftover() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        save_audit_log(&log, dir.path()).unwrap();
        // No .tmp file should remain
        assert!(!dir.path().join("audit_log.json.tmp").exists());
    }

    #[test]
    fn test_save_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        save_audit_log(&log, dir.path()).unwrap();

        // Overwrite with different data
        let mut log2 = AuditLog::new();
        log2.log(AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("t2".to_string()),
            None,
            None,
            None,
        ));
        save_audit_log(&log2, dir.path()).unwrap();

        let loaded = load_audit_log(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        let entry = loaded.entries().next().unwrap();
        assert_eq!(entry.event_type, AuditEventType::TaskCompleted);
        assert_eq!(entry.task_id.as_deref(), Some("t2"));
    }

    #[test]
    fn test_load_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("audit_log.json"), "not valid json{{{").unwrap();
        let loaded = load_audit_log(dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("audit_log.json"), "").unwrap();
        let loaded = load_audit_log(dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = AuditLog::new();
        for i in 0..10 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("task-{}", i)),
                Some(format!("file-{}.txt", i)),
                Some("http".to_string()),
                Some(format!("detail {}", i)),
            ));
        }
        save_audit_log(&log, dir.path()).unwrap();

        let loaded = load_audit_log(dir.path()).unwrap();
        assert_eq!(loaded.len(), 10);
        let first = loaded.entries().next().unwrap();
        assert_eq!(first.task_id.as_deref(), Some("task-0"));
    }

    #[test]
    fn test_save_load_pretty_json() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        save_audit_log(&log, dir.path()).unwrap();

        let raw = std::fs::read_to_string(dir.path().join("audit_log.json")).unwrap();
        // Pretty JSON has newlines and indentation
        assert!(raw.contains('\n'));
        assert!(raw.contains("  "));
    }

    // === Unicode/emoji fields ===

    #[test]
    fn test_unicode_task_id_and_name() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("任务-001".to_string()),
            Some("文件名.zip".to_string()),
            Some("http".to_string()),
            Some("详细信息".to_string()),
        );
        assert_eq!(entry.task_id.as_deref(), Some("任务-001"));
        assert_eq!(entry.task_name.as_deref(), Some("文件名.zip"));
    }

    #[test]
    fn test_emoji_fields() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("✅-task".to_string()),
            Some("🎉.zip".to_string()),
            Some("🌐".to_string()),
            Some("🚀 done".to_string()),
        )
        .with_user("👤admin".to_string());
        assert_eq!(entry.user.as_deref(), Some("👤admin"));
    }

    #[test]
    fn test_unicode_serde_roundtrip() {
        let entry = AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("中日韩".to_string()),
            Some("テスト.txt".to_string()),
            Some("http".to_string()),
            Some("émojis ñ".to_string()),
        );
        let json = serde_json::to_string(&entry).unwrap();
        let de: AuditLogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(de.task_id.as_deref(), Some("中日韩"));
        assert_eq!(de.task_name.as_deref(), Some("テスト.txt"));
    }

    // === AuditLog: export/import JSON ===

    #[test]
    fn test_export_json_empty() {
        let log = AuditLog::new();
        let json = log.export_json().unwrap();
        assert_eq!(json, "[]");
    }

    #[test]
    fn test_import_json_invalid() {
        let mut log = AuditLog::new();
        let result = log.import_json("not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_import_json_empty_array() {
        let mut log = AuditLog::new();
        let count = log.import_json("[]").unwrap();
        assert_eq!(count, 0);
        assert!(log.is_empty());
    }

    #[test]
    fn test_import_json_appends_to_existing() {
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("existing".to_string()),
            None,
            None,
            None,
        ));
        assert_eq!(log.len(), 1);

        let json = r#"[{
            "timestamp": "2026-01-01T00:00:00Z",
            "event_type": "task_completed",
            "task_id": "imported",
            "task_name": null,
            "protocol": null,
            "details": null,
            "user": null
        }]"#;
        let count = log.import_json(json).unwrap();
        assert_eq!(count, 1);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_export_import_roundtrip() {
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            Some("f1".to_string()),
            Some("http".to_string()),
            Some("d1".to_string()),
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskFailed,
            Some("t2".to_string()),
            None,
            Some("ftp".to_string()),
            None,
        ));

        let json = log.export_json().unwrap();
        let mut log2 = AuditLog::new();
        let count = log2.import_json(&json).unwrap();
        assert_eq!(count, 2);
        assert_eq!(log2.len(), 2);
    }

    // === AuditLog: Clone/Debug ===

    #[test]
    fn test_audit_log_clone() {
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        let cloned = log.clone();
        assert_eq!(cloned.len(), 1);
        assert_eq!(
            cloned.entries().next().unwrap().task_id.as_deref(),
            Some("t1")
        );
    }

    #[test]
    fn test_audit_log_clone_independence() {
        let mut log = AuditLog::new();
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t1".to_string()),
            None,
            None,
            None,
        ));
        let mut cloned = log.clone();
        cloned.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("t2".to_string()),
            None,
            None,
            None,
        ));
        // Original should not be affected
        assert_eq!(log.len(), 1);
        assert_eq!(cloned.len(), 2);
    }

    #[test]
    fn test_audit_log_debug() {
        let log = AuditLog::new();
        let debug = format!("{:?}", log);
        assert!(debug.contains("AuditLog"));
    }

    // === AuditLog: clear ===

    #[test]
    fn test_clear_empty_log() {
        let mut log = AuditLog::new();
        log.clear();
        assert!(log.is_empty());
    }

    #[test]
    fn test_clear_preserves_capacity() {
        let mut log = AuditLog::with_max_entries(100);
        for i in 0..50 {
            log.log(AuditLogEntry::new(
                AuditEventType::TaskAdded,
                Some(format!("t{}", i)),
                None,
                None,
                None,
            ));
        }
        log.clear();
        assert!(log.is_empty());
        // Should still be able to add entries
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("after-clear".to_string()),
            None,
            None,
            None,
        ));
        assert_eq!(log.len(), 1);
    }

    // === Complex workflows ===

    #[test]
    fn test_full_lifecycle() {
        let mut log = AuditLog::new();

        // Task lifecycle: added -> started -> completed
        log.log(AuditLogEntry::new(
            AuditEventType::TaskAdded,
            Some("lifecycle-1".to_string()),
            Some("file.zip".to_string()),
            Some("http".to_string()),
            Some("Added from URL".to_string()),
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskStarted,
            Some("lifecycle-1".to_string()),
            Some("file.zip".to_string()),
            Some("http".to_string()),
            None,
        ));
        log.log(AuditLogEntry::new(
            AuditEventType::TaskCompleted,
            Some("lifecycle-1".to_string()),
            Some("file.zip".to_string()),
            Some("http".to_string()),
            Some(" (500 MB)".to_string()),
        ));

        assert_eq!(log.len(), 3);
        assert_eq!(log.entries_by_task("lifecycle-1").len(), 3);
        assert_eq!(log.entries_by_type(&AuditEventType::TaskAdded).len(), 1);
        assert_eq!(log.entries_by_type(&AuditEventType::TaskStarted).len(), 1);
        assert_eq!(log.entries_by_type(&AuditEventType::TaskCompleted).len(), 1);

        let summary = log.summary();
        assert_eq!(summary.total_entries, 3);
        assert_eq!(summary.event_counts.len(), 3);
    }

    #[test]
    fn test_multi_task_operations() {
        let mut log = AuditLog::new();

        for i in 0..20 {
            let event = if i % 2 == 0 {
                AuditEventType::TaskAdded
            } else {
                AuditEventType::TaskCompleted
            };
            log.log(AuditLogEntry::new(
                event,
                Some(format!("task-{}", i)),
                None,
                None,
                None,
            ));
        }

        assert_eq!(log.len(), 20);
        assert_eq!(log.entries_by_type(&AuditEventType::TaskAdded).len(), 10);
        assert_eq!(
            log.entries_by_type(&AuditEventType::TaskCompleted).len(),
            10
        );

        // Each task should have exactly 1 entry
        for i in 0..20 {
            assert_eq!(log.entries_by_task(&format!("task-{}", i)).len(), 1);
        }
    }

    #[test]
    fn test_eviction_preserves_recent_entries() {
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

        // Only tasks 5-9 should remain
        assert_eq!(log.len(), 5);
        for i in 5..10 {
            assert_eq!(log.entries_by_task(&format!("task-{}", i)).len(), 1);
        }
        for i in 0..5 {
            assert!(log.entries_by_task(&format!("task-{}", i)).is_empty());
        }
    }

    // === MAX_AUDIT_ENTRIES constant ===

    #[test]
    fn test_max_audit_entries_constant() {
        assert_eq!(MAX_AUDIT_ENTRIES, 1000);
    }

    #[test]
    fn test_default_log_uses_max_audit_entries() {
        let log = AuditLog::new();
        assert_eq!(log.max_entries, MAX_AUDIT_ENTRIES);
    }
}
