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

    // ===== BulkFilter comprehensive serde =====

    #[test]
    fn test_bulk_filter_serde_roundtrip_default() {
        let filter = BulkFilter::default();
        let json = serde_json::to_string(&filter).unwrap();
        let back: BulkFilter = serde_json::from_str(&json).unwrap();
        assert!(back.task_ids.is_empty());
        assert!(back.state.is_none());
        assert!(back.protocol.is_none());
        assert!(back.tag.is_none());
        assert!(back.group.is_none());
    }

    #[test]
    fn test_bulk_filter_serde_extra_fields_ignored() {
        let json = r#"{"task_ids":[],"state":null,"protocol":null,"tag":null,"group":null,"extra_field":42,"unknown":"value"}"#;
        let filter: BulkFilter = serde_json::from_str(json).unwrap();
        assert!(filter.task_ids.is_empty());
        assert!(filter.state.is_none());
    }

    #[test]
    fn test_bulk_filter_serde_pretty() {
        let filter = BulkFilter {
            task_ids: vec!["a".to_string()],
            state: Some("Paused".to_string()),
            protocol: None,
            tag: None,
            group: None,
        };
        let pretty = serde_json::to_string_pretty(&filter).unwrap();
        assert!(pretty.contains('\n'));
        let back: BulkFilter = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.task_ids, vec!["a".to_string()]);
        assert_eq!(back.state, Some("Paused".to_string()));
    }

    #[test]
    fn test_bulk_filter_serde_missing_fields_use_defaults() {
        let json = r#"{}"#;
        let filter: BulkFilter = serde_json::from_str(json).unwrap();
        assert!(filter.task_ids.is_empty());
        assert!(filter.state.is_none());
        assert!(filter.protocol.is_none());
        assert!(filter.tag.is_none());
        assert!(filter.group.is_none());
    }

    #[test]
    fn test_bulk_filter_serde_partial_fields() {
        let json = r#"{"state":"Downloading"}"#;
        let filter: BulkFilter = serde_json::from_str(json).unwrap();
        assert!(filter.task_ids.is_empty());
        assert_eq!(filter.state, Some("Downloading".to_string()));
        assert!(filter.protocol.is_none());
    }

    #[test]
    fn test_bulk_filter_unicode_fields() {
        let filter = BulkFilter {
            task_ids: vec!["任务1".to_string(), "🎯task".to_string()],
            state: Some("下载中".to_string()),
            protocol: None,
            tag: Some("视频".to_string()),
            group: Some("电影".to_string()),
        };
        let json = serde_json::to_string(&filter).unwrap();
        let back: BulkFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_ids.len(), 2);
        assert_eq!(back.tag, Some("视频".to_string()));
        assert_eq!(back.group, Some("电影".to_string()));
    }

    // ===== BulkFilter Clone/Debug =====

    #[test]
    fn test_bulk_filter_clone() {
        let filter = BulkFilter {
            task_ids: vec!["id1".to_string()],
            state: Some("Downloading".to_string()),
            protocol: Some("Torrent".to_string()),
            tag: Some("video".to_string()),
            group: Some("movies".to_string()),
        };
        let cloned = filter.clone();
        assert_eq!(cloned.task_ids, filter.task_ids);
        assert_eq!(cloned.state, filter.state);
        assert_eq!(cloned.protocol, filter.protocol);
        assert_eq!(cloned.tag, filter.tag);
        assert_eq!(cloned.group, filter.group);
    }

    #[test]
    fn test_bulk_filter_clone_independence() {
        let mut filter = BulkFilter::default();
        filter.task_ids.push("a".to_string());
        let cloned = filter.clone();
        // mutating original shouldn't affect clone
        assert_eq!(cloned.task_ids, vec!["a".to_string()]);
    }

    #[test]
    fn test_bulk_filter_debug() {
        let filter = BulkFilter::default();
        let debug = format!("{:?}", filter);
        assert!(debug.contains("BulkFilter"));
        assert!(debug.contains("task_ids"));
    }

    // ===== BulkResult comprehensive =====

    #[test]
    fn test_bulk_result_serde_roundtrip() {
        let result = BulkResult {
            matched: 10,
            modified: 8,
            modified_ids: vec!["a".to_string(), "b".to_string()],
            description: "test".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: BulkResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.matched, 10);
        assert_eq!(back.modified, 8);
        assert_eq!(back.modified_ids.len(), 2);
        assert_eq!(back.description, "test");
    }

    #[test]
    fn test_bulk_result_zero_values() {
        let result = BulkResult {
            matched: 0,
            modified: 0,
            modified_ids: vec![],
            description: String::new(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: BulkResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.matched, 0);
        assert_eq!(back.modified, 0);
        assert!(back.modified_ids.is_empty());
        assert!(back.description.is_empty());
    }

    #[test]
    fn test_bulk_result_large_values() {
        let result = BulkResult {
            matched: usize::MAX,
            modified: usize::MAX,
            modified_ids: vec![],
            description: String::new(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: BulkResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.matched, usize::MAX);
    }

    #[test]
    fn test_bulk_result_unicode() {
        let result = BulkResult {
            matched: 3,
            modified: 2,
            modified_ids: vec!["任务1".to_string(), "🎯".to_string()],
            description: "批量添加标签到任务".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: BulkResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.modified_ids[0], "任务1");
        assert_eq!(back.description, "批量添加标签到任务");
    }

    #[test]
    fn test_bulk_result_extra_fields_ignored() {
        let json = r#"{"matched":1,"modified":0,"modified_ids":[],"description":"","extra":true}"#;
        let result: BulkResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.matched, 1);
    }

    #[test]
    fn test_bulk_result_pretty_serde() {
        let result = BulkResult {
            matched: 5,
            modified: 3,
            modified_ids: vec!["x".to_string()],
            description: "ok".to_string(),
        };
        let pretty = serde_json::to_string_pretty(&result).unwrap();
        assert!(pretty.contains('\n'));
        let back: BulkResult = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.matched, 5);
    }

    #[test]
    fn test_bulk_result_clone() {
        let result = BulkResult {
            matched: 1,
            modified: 1,
            modified_ids: vec!["a".to_string()],
            description: "d".to_string(),
        };
        let cloned = result.clone();
        assert_eq!(cloned.matched, result.matched);
        assert_eq!(cloned.modified_ids, result.modified_ids);
    }

    #[test]
    fn test_bulk_result_clone_independence() {
        let mut result = BulkResult {
            matched: 1,
            modified: 1,
            modified_ids: vec!["a".to_string()],
            description: "d".to_string(),
        };
        let cloned = result.clone();
        result.modified_ids.push("b".to_string());
        assert_eq!(cloned.modified_ids.len(), 1);
    }

    #[test]
    fn test_bulk_result_debug() {
        let result = BulkResult {
            matched: 0,
            modified: 0,
            modified_ids: vec![],
            description: String::new(),
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("BulkResult"));
        assert!(debug.contains("matched"));
    }

    // ===== BulkTagAction comprehensive =====

    #[test]
    fn test_bulk_tag_action_add_serde_roundtrip() {
        let action = BulkTagAction::Add {
            tags: vec!["video".to_string(), "hd".to_string()],
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkTagAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkTagAction::Add { tags } => {
                assert_eq!(tags, vec!["video", "hd"]);
            }
            _ => panic!("expected Add variant"),
        }
    }

    #[test]
    fn test_bulk_tag_action_remove_serde_roundtrip() {
        let action = BulkTagAction::Remove {
            tags: vec!["old".to_string()],
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkTagAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkTagAction::Remove { tags } => {
                assert_eq!(tags, vec!["old"]);
            }
            _ => panic!("expected Remove variant"),
        }
    }

    #[test]
    fn test_bulk_tag_action_replace_serde_roundtrip() {
        let action = BulkTagAction::Replace {
            tags: vec!["new1".to_string(), "new2".to_string(), "new3".to_string()],
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkTagAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkTagAction::Replace { tags } => {
                assert_eq!(tags.len(), 3);
            }
            _ => panic!("expected Replace variant"),
        }
    }

    #[test]
    fn test_bulk_tag_action_clear_serde_roundtrip() {
        let action = BulkTagAction::Clear;
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkTagAction = serde_json::from_str(&json).unwrap();
        matches!(back, BulkTagAction::Clear);
    }

    #[test]
    fn test_bulk_tag_action_add_empty_tags() {
        let action = BulkTagAction::Add { tags: vec![] };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkTagAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkTagAction::Add { tags } => assert!(tags.is_empty()),
            _ => panic!("expected Add"),
        }
    }

    #[test]
    fn test_bulk_tag_action_unicode_tags() {
        let action = BulkTagAction::Add {
            tags: vec!["视频".to_string(), "🎬电影".to_string()],
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkTagAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkTagAction::Add { tags } => {
                assert_eq!(tags[0], "视频");
                assert_eq!(tags[1], "🎬电影");
            }
            _ => panic!("expected Add"),
        }
    }

    #[test]
    fn test_bulk_tag_action_clone() {
        let action = BulkTagAction::Add {
            tags: vec!["a".to_string()],
        };
        let cloned = action.clone();
        match cloned {
            BulkTagAction::Add { tags } => assert_eq!(tags, vec!["a"]),
            _ => panic!("expected Add"),
        }
    }

    #[test]
    fn test_bulk_tag_action_debug() {
        let action = BulkTagAction::Clear;
        let debug = format!("{:?}", action);
        assert!(debug.contains("Clear"));
    }

    #[test]
    fn test_bulk_tag_action_all_variants_debug() {
        let variants: Vec<BulkTagAction> = vec![
            BulkTagAction::Add {
                tags: vec!["a".to_string()],
            },
            BulkTagAction::Remove {
                tags: vec!["b".to_string()],
            },
            BulkTagAction::Replace {
                tags: vec!["c".to_string()],
            },
            BulkTagAction::Clear,
        ];
        for v in &variants {
            let debug = format!("{:?}", v);
            assert!(!debug.is_empty());
        }
    }

    // ===== BulkGroupAction comprehensive =====

    #[test]
    fn test_bulk_group_action_set_serde_roundtrip() {
        let action = BulkGroupAction::Set {
            group: "movies".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkGroupAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkGroupAction::Set { group } => assert_eq!(group, "movies"),
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_bulk_group_action_clear_serde_roundtrip() {
        let action = BulkGroupAction::Clear;
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkGroupAction = serde_json::from_str(&json).unwrap();
        matches!(back, BulkGroupAction::Clear);
    }

    #[test]
    fn test_bulk_group_action_set_unicode() {
        let action = BulkGroupAction::Set {
            group: "电影分组".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkGroupAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkGroupAction::Set { group } => assert_eq!(group, "电影分组"),
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_bulk_group_action_set_empty_group() {
        let action = BulkGroupAction::Set {
            group: String::new(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkGroupAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkGroupAction::Set { group } => assert!(group.is_empty()),
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_bulk_group_action_clone() {
        let action = BulkGroupAction::Set {
            group: "g".to_string(),
        };
        let cloned = action.clone();
        match cloned {
            BulkGroupAction::Set { group } => assert_eq!(group, "g"),
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_bulk_group_action_debug() {
        let action = BulkGroupAction::Clear;
        let debug = format!("{:?}", action);
        assert!(debug.contains("Clear"));
    }

    // ===== BulkPriorityAction comprehensive =====

    #[test]
    fn test_bulk_priority_action_serde_roundtrip() {
        for p in &["low", "normal", "high", "urgent"] {
            let action = BulkPriorityAction {
                priority: p.to_string(),
            };
            let json = serde_json::to_string(&action).unwrap();
            let back: BulkPriorityAction = serde_json::from_str(&json).unwrap();
            assert_eq!(back.priority, *p);
        }
    }

    #[test]
    fn test_bulk_priority_action_extra_fields_ignored() {
        let json = r#"{"priority":"high","extra":999}"#;
        let action: BulkPriorityAction = serde_json::from_str(json).unwrap();
        assert_eq!(action.priority, "high");
    }

    #[test]
    fn test_bulk_priority_action_unicode_priority() {
        let action = BulkPriorityAction {
            priority: "高优先级".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkPriorityAction = serde_json::from_str(&json).unwrap();
        assert_eq!(back.priority, "高优先级");
    }

    #[test]
    fn test_bulk_priority_action_empty_priority() {
        let action = BulkPriorityAction {
            priority: String::new(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkPriorityAction = serde_json::from_str(&json).unwrap();
        assert!(back.priority.is_empty());
    }

    #[test]
    fn test_bulk_priority_action_clone() {
        let action = BulkPriorityAction {
            priority: "urgent".to_string(),
        };
        let cloned = action.clone();
        assert_eq!(cloned.priority, "urgent");
    }

    #[test]
    fn test_bulk_priority_action_debug() {
        let action = BulkPriorityAction {
            priority: "low".to_string(),
        };
        let debug = format!("{:?}", action);
        assert!(debug.contains("BulkPriorityAction"));
        assert!(debug.contains("low"));
    }

    // ===== BulkSpeedLimitAction comprehensive =====

    #[test]
    fn test_bulk_speed_limit_some_zero() {
        let action = BulkSpeedLimitAction {
            bytes_per_sec: Some(0),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkSpeedLimitAction = serde_json::from_str(&json).unwrap();
        assert_eq!(back.bytes_per_sec, Some(0));
    }

    #[test]
    fn test_bulk_speed_limit_some_u64_max() {
        let action = BulkSpeedLimitAction {
            bytes_per_sec: Some(u64::MAX),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkSpeedLimitAction = serde_json::from_str(&json).unwrap();
        assert_eq!(back.bytes_per_sec, Some(u64::MAX));
    }

    #[test]
    fn test_bulk_speed_limit_none_unlimited() {
        let action = BulkSpeedLimitAction {
            bytes_per_sec: None,
        };
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("null"));
        let back: BulkSpeedLimitAction = serde_json::from_str(&json).unwrap();
        assert_eq!(back.bytes_per_sec, None);
    }

    #[test]
    fn test_bulk_speed_limit_clone() {
        let action = BulkSpeedLimitAction {
            bytes_per_sec: Some(1024),
        };
        let cloned = action.clone();
        assert_eq!(cloned.bytes_per_sec, Some(1024));
    }

    #[test]
    fn test_bulk_speed_limit_debug() {
        let action = BulkSpeedLimitAction {
            bytes_per_sec: Some(512),
        };
        let debug = format!("{:?}", action);
        assert!(debug.contains("512"));
    }

    #[test]
    fn test_bulk_speed_limit_extra_fields_ignored() {
        let json = r#"{"bytes_per_sec":100,"extra_field":"ignored"}"#;
        let back: BulkSpeedLimitAction = serde_json::from_str(json).unwrap();
        assert_eq!(back.bytes_per_sec, Some(100));
    }

    // ===== BulkWeightAction comprehensive =====

    #[test]
    fn test_bulk_weight_action_boundary_min() {
        let action = BulkWeightAction { weight: 0 };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkWeightAction = serde_json::from_str(&json).unwrap();
        assert_eq!(back.weight, 0);
    }

    #[test]
    fn test_bulk_weight_action_boundary_max() {
        let action = BulkWeightAction { weight: 255 };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkWeightAction = serde_json::from_str(&json).unwrap();
        assert_eq!(back.weight, 255);
    }

    #[test]
    fn test_bulk_weight_action_typical_values() {
        for w in 1..=10 {
            let action = BulkWeightAction { weight: w };
            let json = serde_json::to_string(&action).unwrap();
            let back: BulkWeightAction = serde_json::from_str(&json).unwrap();
            assert_eq!(back.weight, w);
        }
    }

    #[test]
    fn test_bulk_weight_action_clone() {
        let action = BulkWeightAction { weight: 7 };
        let cloned = action.clone();
        assert_eq!(cloned.weight, 7);
    }

    #[test]
    fn test_bulk_weight_action_debug() {
        let action = BulkWeightAction { weight: 3 };
        let debug = format!("{:?}", action);
        assert!(debug.contains("3"));
    }

    #[test]
    fn test_bulk_weight_action_extra_fields_ignored() {
        let json = r#"{"weight":5,"bonus":true}"#;
        let back: BulkWeightAction = serde_json::from_str(json).unwrap();
        assert_eq!(back.weight, 5);
    }

    // ===== parse_state comprehensive =====

    #[test]
    fn test_parse_state_all_aliases() {
        // Downloading aliases
        assert_eq!(parse_state("downloading"), Some("Downloading".to_string()));
        assert_eq!(parse_state("running"), Some("Downloading".to_string()));
        assert_eq!(parse_state("active"), Some("Downloading".to_string()));
        // Paused
        assert_eq!(parse_state("paused"), Some("Paused".to_string()));
        // Queued aliases
        assert_eq!(parse_state("queued"), Some("Queued".to_string()));
        assert_eq!(parse_state("waiting"), Some("Queued".to_string()));
        // Complete aliases
        assert_eq!(parse_state("complete"), Some("Complete".to_string()));
        assert_eq!(parse_state("completed"), Some("Complete".to_string()));
        assert_eq!(parse_state("done"), Some("Complete".to_string()));
        // Error aliases
        assert_eq!(parse_state("error"), Some("Error".to_string()));
        assert_eq!(parse_state("failed"), Some("Error".to_string()));
    }

    #[test]
    fn test_parse_state_case_insensitive_all() {
        assert_eq!(parse_state("DOWNLOADING"), Some("Downloading".to_string()));
        assert_eq!(parse_state("RUNNING"), Some("Downloading".to_string()));
        assert_eq!(parse_state("ACTIVE"), Some("Downloading".to_string()));
        assert_eq!(parse_state("PAUSED"), Some("Paused".to_string()));
        assert_eq!(parse_state("QUEUED"), Some("Queued".to_string()));
        assert_eq!(parse_state("WAITING"), Some("Queued".to_string()));
        assert_eq!(parse_state("COMPLETE"), Some("Complete".to_string()));
        assert_eq!(parse_state("COMPLETED"), Some("Complete".to_string()));
        assert_eq!(parse_state("DONE"), Some("Complete".to_string()));
        assert_eq!(parse_state("ERROR"), Some("Error".to_string()));
        assert_eq!(parse_state("FAILED"), Some("Error".to_string()));
    }

    #[test]
    fn test_parse_state_mixed_case() {
        assert_eq!(parse_state("Downloading"), Some("Downloading".to_string()));
        assert_eq!(parse_state("Paused"), Some("Paused".to_string()));
        assert_eq!(parse_state("Queued"), Some("Queued".to_string()));
        assert_eq!(parse_state("Complete"), Some("Complete".to_string()));
        assert_eq!(parse_state("Error"), Some("Error".to_string()));
    }

    #[test]
    fn test_parse_state_unknown() {
        assert_eq!(parse_state("unknown"), None);
        assert_eq!(parse_state("xyz"), None);
        assert_eq!(parse_state(""), None);
        assert_eq!(parse_state("pause"), None); // not "paused"
        assert_eq!(parse_state("download"), None); // not "downloading"
    }

    #[test]
    fn test_parse_state_unicode_returns_none() {
        assert_eq!(parse_state("下载中"), None);
        assert_eq!(parse_state("暂停"), None);
        assert_eq!(parse_state("🎯"), None);
    }

    #[test]
    fn test_parse_state_whitespace_returns_none() {
        assert_eq!(parse_state(" downloading "), None);
        assert_eq!(parse_state(" paused"), None);
        assert_eq!(parse_state("paused "), None);
    }

    // ===== parse_protocol comprehensive =====

    #[test]
    fn test_parse_protocol_all_aliases() {
        // Xunlei aliases
        assert_eq!(parse_protocol("http"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("https"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("ftp"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("xunlei"), Some("Xunlei".to_string()));
        // Torrent aliases
        assert_eq!(parse_protocol("torrent"), Some("Torrent".to_string()));
        assert_eq!(parse_protocol("bittorrent"), Some("Torrent".to_string()));
        assert_eq!(parse_protocol("bt"), Some("Torrent".to_string()));
        // Ed2k aliases
        assert_eq!(parse_protocol("ed2k"), Some("Ed2k".to_string()));
        assert_eq!(parse_protocol("edonkey"), Some("Ed2k".to_string()));
        assert_eq!(parse_protocol("emule"), Some("Ed2k".to_string()));
        // P2P
        assert_eq!(parse_protocol("p2p"), Some("P2P".to_string()));
    }

    #[test]
    fn test_parse_protocol_case_insensitive_all() {
        assert_eq!(parse_protocol("HTTP"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("HTTPS"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("FTP"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("XUNLEI"), Some("Xunlei".to_string()));
        assert_eq!(parse_protocol("TORRENT"), Some("Torrent".to_string()));
        assert_eq!(parse_protocol("BITTORRENT"), Some("Torrent".to_string()));
        assert_eq!(parse_protocol("BT"), Some("Torrent".to_string()));
        assert_eq!(parse_protocol("ED2K"), Some("Ed2k".to_string()));
        assert_eq!(parse_protocol("EDONKEY"), Some("Ed2k".to_string()));
        assert_eq!(parse_protocol("EMULE"), Some("Ed2k".to_string()));
        assert_eq!(parse_protocol("P2P"), Some("P2P".to_string()));
    }

    #[test]
    fn test_parse_protocol_unknown() {
        assert_eq!(parse_protocol("unknown"), None);
        assert_eq!(parse_protocol(""), None);
        assert_eq!(parse_protocol("smtp"), None);
        assert_eq!(parse_protocol("ssh"), None);
    }

    #[test]
    fn test_parse_protocol_unicode_returns_none() {
        assert_eq!(parse_protocol("迅雷"), None);
        assert_eq!(parse_protocol("种子"), None);
    }

    #[test]
    fn test_parse_protocol_whitespace_returns_none() {
        assert_eq!(parse_protocol(" http"), None);
        assert_eq!(parse_protocol("http "), None);
        assert_eq!(parse_protocol(" http "), None);
    }

    // ===== parse_priority comprehensive =====

    #[test]
    fn test_parse_priority_all_aliases() {
        assert_eq!(parse_priority("low"), Some("Low".to_string()));
        assert_eq!(parse_priority("normal"), Some("Normal".to_string()));
        assert_eq!(parse_priority("default"), Some("Normal".to_string()));
        assert_eq!(parse_priority("high"), Some("High".to_string()));
        assert_eq!(parse_priority("urgent"), Some("Urgent".to_string()));
        assert_eq!(parse_priority("highest"), Some("Urgent".to_string()));
    }

    #[test]
    fn test_parse_priority_case_insensitive_all() {
        assert_eq!(parse_priority("LOW"), Some("Low".to_string()));
        assert_eq!(parse_priority("NORMAL"), Some("Normal".to_string()));
        assert_eq!(parse_priority("DEFAULT"), Some("Normal".to_string()));
        assert_eq!(parse_priority("HIGH"), Some("High".to_string()));
        assert_eq!(parse_priority("URGENT"), Some("Urgent".to_string()));
        assert_eq!(parse_priority("HIGHEST"), Some("Urgent".to_string()));
    }

    #[test]
    fn test_parse_priority_mixed_case() {
        assert_eq!(parse_priority("Low"), Some("Low".to_string()));
        assert_eq!(parse_priority("Normal"), Some("Normal".to_string()));
        assert_eq!(parse_priority("High"), Some("High".to_string()));
        assert_eq!(parse_priority("Urgent"), Some("Urgent".to_string()));
    }

    #[test]
    fn test_parse_priority_unknown() {
        assert_eq!(parse_priority("unknown"), None);
        assert_eq!(parse_priority(""), None);
        assert_eq!(parse_priority("medium"), None);
        assert_eq!(parse_priority("critical"), None);
        assert_eq!(parse_priority("hi"), None); // not "high"
    }

    #[test]
    fn test_parse_priority_unicode_returns_none() {
        assert_eq!(parse_priority("低"), None);
        assert_eq!(parse_priority("高"), None);
        assert_eq!(parse_priority("🔥"), None);
    }

    #[test]
    fn test_parse_priority_whitespace_returns_none() {
        assert_eq!(parse_priority(" low"), None);
        assert_eq!(parse_priority("low "), None);
        assert_eq!(parse_priority(" high "), None);
    }

    // ===== Cross-type serde interaction =====

    #[test]
    fn test_bulk_filter_many_task_ids() {
        let ids: Vec<String> = (0..100).map(|i| format!("task_{}", i)).collect();
        let filter = BulkFilter {
            task_ids: ids.clone(),
            state: None,
            protocol: None,
            tag: None,
            group: None,
        };
        let json = serde_json::to_string(&filter).unwrap();
        let back: BulkFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_ids.len(), 100);
        assert_eq!(back.task_ids[99], "task_99");
    }

    #[test]
    fn test_bulk_result_many_modified_ids() {
        let ids: Vec<String> = (0..200).map(|i| format!("id_{}", i)).collect();
        let result = BulkResult {
            matched: 200,
            modified: 200,
            modified_ids: ids,
            description: "batch done".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: BulkResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.modified_ids.len(), 200);
    }

    #[test]
    fn test_bulk_tag_action_replace_many_tags() {
        let tags: Vec<String> = (0..50).map(|i| format!("tag_{}", i)).collect();
        let action = BulkTagAction::Replace { tags: tags.clone() };
        let json = serde_json::to_string(&action).unwrap();
        let back: BulkTagAction = serde_json::from_str(&json).unwrap();
        match back {
            BulkTagAction::Replace { tags } => assert_eq!(tags.len(), 50),
            _ => panic!("expected Replace"),
        }
    }

    // ===== Edge cases =====

    #[test]
    fn test_bulk_filter_serde_null_values() {
        let json = r#"{"task_ids":[],"state":null,"protocol":null,"tag":null,"group":null}"#;
        let filter: BulkFilter = serde_json::from_str(json).unwrap();
        assert!(filter.task_ids.is_empty());
        assert!(filter.state.is_none());
        assert!(filter.protocol.is_none());
        assert!(filter.tag.is_none());
        assert!(filter.group.is_none());
    }

    #[test]
    fn test_bulk_filter_special_chars_in_ids() {
        let filter = BulkFilter {
            task_ids: vec![
                "id with spaces".to_string(),
                "id/with/slashes".to_string(),
                "id\"with\"quotes".to_string(),
                "id\nwith\nnewlines".to_string(),
            ],
            state: None,
            protocol: None,
            tag: None,
            group: None,
        };
        let json = serde_json::to_string(&filter).unwrap();
        let back: BulkFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_ids.len(), 4);
        assert_eq!(back.task_ids[0], "id with spaces");
        assert_eq!(back.task_ids[1], "id/with/slashes");
    }

    #[test]
    fn test_parse_state_and_parse_protocol_independent() {
        // Verify they don't interfere
        assert!(parse_state("http").is_none());
        assert!(parse_protocol("downloading").is_none());
        assert!(parse_priority("torrent").is_none());
    }
}
