//! Bulk task operations for batch-modifying multiple download tasks at once.
//!
//! Supports:
//! - Batch tag add/remove across multiple tasks
//! - Batch group assignment
//! - Batch priority change
//! - Batch speed limit setting
//! - Batch bandwidth weight adjustment
//! - Bulk pause/resume by filter (state/protocol/tag)

use serde::{Deserialize, Serialize};

/// Filter to select tasks for bulk operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BulkFilter {
    /// Filter by task IDs (if non-empty, only these tasks are affected).
    #[serde(default)]
    pub task_ids: Vec<String>,
    /// Filter by download state.
    #[serde(default)]
    pub state: Option<String>,
    /// Filter by protocol.
    #[serde(default)]
    pub protocol: Option<String>,
    /// Filter by tag (tasks must have this tag).
    #[serde(default)]
    pub tag: Option<String>,
    /// Filter by group (tasks must be in this group).
    #[serde(default)]
    pub group: Option<String>,
}

/// Result of a bulk operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkResult {
    /// Number of tasks that matched the filter.
    pub matched: usize,
    /// Number of tasks successfully modified.
    pub modified: usize,
    /// Task IDs that were modified.
    pub modified_ids: Vec<String>,
    /// Description of what was done.
    pub description: String,
}

/// Bulk tag operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BulkTagAction {
    /// Add these tags to all matched tasks.
    Add { tags: Vec<String> },
    /// Remove these tags from all matched tasks.
    Remove { tags: Vec<String> },
    /// Replace all tags on matched tasks with these.
    Replace { tags: Vec<String> },
    /// Clear all tags from matched tasks.
    Clear,
}

/// Bulk group operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BulkGroupAction {
    /// Set group for all matched tasks.
    Set { group: String },
    /// Clear group from all matched tasks.
    Clear,
}

/// Bulk priority operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkPriorityAction {
    /// Priority level to set: "low", "normal", "high", "urgent".
    pub priority: String,
}

/// Bulk speed limit operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkSpeedLimitAction {
    /// Speed limit in bytes per second. None means unlimited.
    pub bytes_per_sec: Option<u64>,
}

/// Bulk bandwidth weight operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkWeightAction {
    /// Bandwidth weight (1-10).
    pub weight: u8,
}

/// Parse a state string into a DownloadState-compatible string.
pub fn parse_state(s: &str) -> Option<String> {
    match s.to_lowercase().as_str() {
        "downloading" | "running" | "active" => Some("Downloading".to_string()),
        "paused" => Some("Paused".to_string()),
        "queued" | "waiting" => Some("Queued".to_string()),
        "complete" | "completed" | "done" => Some("Complete".to_string()),
        "error" | "failed" => Some("Error".to_string()),
        _ => None,
    }
}

/// Parse a protocol string into a canonical form.
pub fn parse_protocol(s: &str) -> Option<String> {
    match s.to_lowercase().as_str() {
        "http" | "https" | "ftp" | "xunlei" => Some("Xunlei".to_string()),
        "torrent" | "bittorrent" | "bt" => Some("Torrent".to_string()),
        "ed2k" | "edonkey" | "emule" => Some("Ed2k".to_string()),
        "p2p" => Some("P2P".to_string()),
        _ => None,
    }
}

/// Parse a priority string into canonical form.
pub fn parse_priority(s: &str) -> Option<String> {
    match s.to_lowercase().as_str() {
        "low" => Some("Low".to_string()),
        "normal" | "default" => Some("Normal".to_string()),
        "high" => Some("High".to_string()),
        "urgent" | "highest" => Some("Urgent".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bulk_filter_default() {
        let filter = BulkFilter::default();
        assert!(filter.task_ids.is_empty());
        assert!(filter.state.is_none());
        assert!(filter.protocol.is_none());
        assert!(filter.tag.is_none());
        assert!(filter.group.is_none());
    }

    #[test]
    fn test_bulk_filter_serialization() {
        let filter = BulkFilter {
            task_ids: vec!["id1".to_string(), "id2".to_string()],
            state: Some("Downloading".to_string()),
            protocol: None,
            tag: Some("video".to_string()),
            group: None,
        };
        let json = serde_json::to_string(&filter).unwrap();
        let deserialized: BulkFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_ids.len(), 2);
        assert_eq!(deserialized.state, Some("Downloading".to_string()));
        assert_eq!(deserialized.tag, Some("video".to_string()));
    }

    #[test]
    fn test_bulk_result_serialization() {
        let result = BulkResult {
            matched: 5,
            modified: 3,
            modified_ids: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            description: "Added tags to 3 tasks".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: BulkResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.matched, 5);
        assert_eq!(deserialized.modified, 3);
        assert_eq!(deserialized.modified_ids.len(), 3);
    }

    #[test]
    fn test_parse_state() {
        assert_eq!(parse_state("downloading"), Some("Downloading".to_string()));
        assert_eq!(parse_state("running"), Some("Downloading".to_string()));
        assert_eq!(parse_state("paused"), Some("Paused".to_string()));
        assert_eq!(parse_state("queued"), Some("Queued".to_string()));
        assert_eq!(parse_state("complete"), Some("Complete".to_string()));
        assert_eq!(parse_state("error"), Some("Error".to_string()));
        assert_eq!(parse_state("failed"), Some("Error".to_string()));
        assert_eq!(parse_state("unknown"), None);
    }

    #[test]
    fn test_parse_protocol() {
        assert_eq!(parse_protocol("http"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("https"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("ftp"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("torrent"), Some("Torrent".to_string()));
        assert_eq!(parse_protocol("bt"), Some("Torrent".to_string()));
        assert_eq!(parse_protocol("ed2k"), Some("Ed2k".to_string()));
        assert_eq!(parse_protocol("p2p"), Some("P2P".to_string()));
        assert_eq!(parse_protocol("unknown"), None);
    }

    #[test]
    fn test_parse_priority() {
        assert_eq!(parse_priority("low"), Some("Low".to_string()));
        assert_eq!(parse_priority("normal"), Some("Normal".to_string()));
        assert_eq!(parse_priority("high"), Some("High".to_string()));
        assert_eq!(parse_priority("urgent"), Some("Urgent".to_string()));
        assert_eq!(parse_priority("highest"), Some("Urgent".to_string()));
        assert_eq!(parse_priority("unknown"), None);
    }

    #[test]
    fn test_bulk_tag_action_variants() {
        let add = BulkTagAction::Add {
            tags: vec!["video".to_string()],
        };
        let json = serde_json::to_string(&add).unwrap();
        assert!(json.contains("Add"));

        let remove = BulkTagAction::Remove {
            tags: vec!["old".to_string()],
        };
        let json = serde_json::to_string(&remove).unwrap();
        assert!(json.contains("Remove"));

        let replace = BulkTagAction::Replace {
            tags: vec!["new".to_string()],
        };
        let json = serde_json::to_string(&replace).unwrap();
        assert!(json.contains("Replace"));

        let clear = BulkTagAction::Clear;
        let json = serde_json::to_string(&clear).unwrap();
        assert!(json.contains("Clear"));
    }

    #[test]
    fn test_bulk_group_action_variants() {
        let set = BulkGroupAction::Set {
            group: "movies".to_string(),
        };
        let json = serde_json::to_string(&set).unwrap();
        assert!(json.contains("Set"));

        let clear = BulkGroupAction::Clear;
        let json = serde_json::to_string(&clear).unwrap();
        assert!(json.contains("Clear"));
    }

    #[test]
    fn test_bulk_speed_limit_action() {
        let limited = BulkSpeedLimitAction {
            bytes_per_sec: Some(1_048_576),
        };
        let json = serde_json::to_string(&limited).unwrap();
        let deserialized: BulkSpeedLimitAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.bytes_per_sec, Some(1_048_576));

        let unlimited = BulkSpeedLimitAction {
            bytes_per_sec: None,
        };
        let json = serde_json::to_string(&unlimited).unwrap();
        let deserialized: BulkSpeedLimitAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.bytes_per_sec, None);
    }

    #[test]
    fn test_bulk_weight_action() {
        let action = BulkWeightAction { weight: 5 };
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: BulkWeightAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.weight, 5);
    }

    #[test]
    fn test_parse_state_case_insensitive() {
        assert_eq!(parse_state("DOWNLOADING"), Some("Downloading".to_string()));
        assert_eq!(parse_state("Paused"), Some("Paused".to_string()));
        assert_eq!(parse_state("COMPLETE"), Some("Complete".to_string()));
    }

    #[test]
    fn test_parse_protocol_case_insensitive() {
        assert_eq!(parse_protocol("HTTP"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("Torrent"), Some("Torrent".to_string()));
        assert_eq!(parse_protocol("ED2K"), Some("Ed2k".to_string()));
    }

    #[test]
    fn test_bulk_filter_with_all_fields() {
        let filter = BulkFilter {
            task_ids: vec!["t1".to_string()],
            state: Some("Downloading".to_string()),
            protocol: Some("Torrent".to_string()),
            tag: Some("video".to_string()),
            group: Some("movies".to_string()),
        };
        let json = serde_json::to_string(&filter).unwrap();
        let deserialized: BulkFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_ids, vec!["t1".to_string()]);
        assert_eq!(deserialized.state, Some("Downloading".to_string()));
        assert_eq!(deserialized.protocol, Some("Torrent".to_string()));
        assert_eq!(deserialized.tag, Some("video".to_string()));
        assert_eq!(deserialized.group, Some("movies".to_string()));
    }

    #[test]
    fn test_bulk_filter_empty_task_ids() {
        let json = r#"{"task_ids":[],"state":null}"#;
        let filter: BulkFilter = serde_json::from_str(json).unwrap();
        assert!(filter.task_ids.is_empty());
        assert!(filter.state.is_none());
    }

    #[test]
    fn test_bulk_priority_action_serialization() {
        let action = BulkPriorityAction {
            priority: "high".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: BulkPriorityAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.priority, "high");
    }
}
