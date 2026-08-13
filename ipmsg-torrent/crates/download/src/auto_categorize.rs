//! Auto-categorization rules for downloads
//!
//! Users can define rules that match against download URLs (or filenames)
//! and automatically assign tags and/or groups to new downloads.

use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

/// Errors that can occur during auto-categorization operations.
#[derive(Debug, thiserror::Error)]
pub enum CategorizeError {
    #[error("I/O error: {0}")]
    Io(String),
}

/// A pattern to match against download URLs/filenames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CategorizePattern {
    /// Case-insensitive substring match
    Contains(String),
    /// Glob-style wildcard (e.g. "*.mp4", "linux-*-iso")
    Wildcard(String),
    /// Exact match (case-insensitive)
    Exact(String),
}

impl CategorizePattern {
    /// Check if the given text matches this pattern.
    pub fn matches(&self, text: &str) -> bool {
        let lower = text.to_lowercase();
        match self {
            CategorizePattern::Contains(sub) => lower.contains(&sub.to_lowercase()),
            CategorizePattern::Wildcard(pat) => wildcard_match(&pat.to_lowercase(), &lower),
            CategorizePattern::Exact(exact) => lower == exact.to_lowercase(),
        }
    }
}

/// Simple glob wildcard matching supporting `*` and `?`.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pat_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    let mut dp = vec![vec![false; text_chars.len() + 1]; pat_chars.len() + 1];
    dp[0][0] = true;
    for i in 1..=pat_chars.len() {
        if pat_chars[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }
    for i in 1..=pat_chars.len() {
        for j in 1..=text_chars.len() {
            if pat_chars[i - 1] == '*' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if pat_chars[i - 1] == '?' || pat_chars[i - 1] == text_chars[j - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }
    dp[pat_chars.len()][text_chars.len()]
}

/// What to apply when a rule matches.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CategorizeAction {
    /// Tags to add to the download task
    #[serde(default)]
    pub tags: Vec<String>,
    /// Group to assign (None = don't change group)
    #[serde(default)]
    pub group: Option<String>,
}

/// A single auto-categorization rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategorizeRule {
    /// Unique rule ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Pattern to match against URL or filename
    pub pattern: CategorizePattern,
    /// Whether to match against the URL
    #[serde(default = "default_true")]
    pub match_url: bool,
    /// Whether to match against the filename
    #[serde(default = "default_true")]
    pub match_filename: bool,
    /// Action to apply when matched
    pub action: CategorizeAction,
    /// Whether the rule is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Priority (lower = higher priority, checked first)
    #[serde(default)]
    pub priority: u32,
}

fn default_true() -> bool {
    true
}

impl CategorizeRule {
    /// Check if this rule matches the given URL/filename combination.
    pub fn matches(&self, url: &str, filename: &str) -> bool {
        if !self.enabled {
            return false;
        }
        if self.match_url && self.pattern.matches(url) {
            return true;
        }
        if self.match_filename && self.pattern.matches(filename) {
            return true;
        }
        false
    }
}

/// Persistence file name
const RULES_FILE: &str = "categorize_rules.json";

/// Save categorization rules to disk.
pub async fn save_rules(data_dir: &Path, rules: &[CategorizeRule]) -> Result<(), std::io::Error> {
    let path = data_dir.join(RULES_FILE);
    let json = serde_json::to_string_pretty(rules)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    // Atomic write
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, json.as_bytes()).await?;
    fs::rename(&tmp, &path).await?;
    Ok(())
}

/// Load categorization rules from disk.
pub async fn load_rules(data_dir: &Path) -> Vec<CategorizeRule> {
    let path = data_dir.join(RULES_FILE);
    match fs::read_to_string(&path).await {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Apply matching rules to determine tags and group for a URL/filename.
/// Rules are checked in priority order; first match wins.
pub fn apply_rules<'a>(
    rules: &'a [CategorizeRule],
    url: &str,
    filename: &str,
) -> Option<&'a CategorizeAction> {
    let mut sorted: Vec<&CategorizeRule> = rules.iter().collect();
    sorted.sort_by_key(|r| r.priority);
    sorted
        .iter()
        .find(|r| r.matches(url, filename))
        .map(|r| &r.action)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contains_pattern() {
        let p = CategorizePattern::Contains("linux".to_string());
        assert!(p.matches("https://example.com/linux-iso/file.tar.gz"));
        assert!(p.matches("https://example.com/Linux-iso/file.tar.gz"));
        assert!(!p.matches("https://example.com/windows/file.exe"));
    }

    #[test]
    fn test_wildcard_pattern() {
        let p = CategorizePattern::Wildcard("*.mp4".to_string());
        assert!(p.matches("video.mp4"));
        assert!(p.matches("https://example.com/path/video.mp4"));
        assert!(!p.matches("video.avi"));
    }

    #[test]
    fn test_exact_pattern() {
        let p = CategorizePattern::Exact("test.txt".to_string());
        assert!(p.matches("test.txt"));
        assert!(p.matches("TEST.TXT"));
        assert!(!p.matches("test2.txt"));
    }

    #[test]
    fn test_wildcard_match_fn() {
        assert!(wildcard_match("*.mp4", "video.mp4"));
        assert!(!wildcard_match("*.mp4", "video.avi"));
        assert!(wildcard_match("linux-*-iso", "linux-5.4-iso"));
        assert!(wildcard_match("test?", "test1"));
        assert!(!wildcard_match("test?", "test12"));
    }

    #[test]
    fn test_rule_matches_url() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Video files".to_string(),
            pattern: CategorizePattern::Wildcard("*.mp4".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["video".to_string()],
                group: Some("Media".to_string()),
            },
            enabled: true,
            priority: 0,
        };
        assert!(rule.matches("https://example.com/video.mp4", "video.mp4"));
        assert!(!rule.matches("https://example.com/video.avi", "video.avi"));
    }

    #[test]
    fn test_rule_disabled() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Disabled".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: false,
            priority: 0,
        };
        assert!(!rule.matches("https://example.com/test/file", "file"));
    }

    #[test]
    fn test_apply_rules_priority() {
        let rules = vec![
            CategorizeRule {
                id: "low".to_string(),
                name: "Low priority".to_string(),
                pattern: CategorizePattern::Contains("file".to_string()),
                match_url: true,
                match_filename: false,
                action: CategorizeAction {
                    tags: vec!["low".to_string()],
                    group: None,
                },
                enabled: true,
                priority: 10,
            },
            CategorizeRule {
                id: "high".to_string(),
                name: "High priority".to_string(),
                pattern: CategorizePattern::Contains("file".to_string()),
                match_url: true,
                match_filename: false,
                action: CategorizeAction {
                    tags: vec!["high".to_string()],
                    group: None,
                },
                enabled: true,
                priority: 1,
            },
        ];
        let action = apply_rules(&rules, "https://example.com/file.zip", "file.zip").unwrap();
        assert_eq!(action.tags, vec!["high"]);
    }

    #[test]
    fn test_apply_rules_no_match() {
        let rules = vec![CategorizeRule {
            id: "1".to_string(),
            name: "ISO".to_string(),
            pattern: CategorizePattern::Wildcard("*.iso".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["iso".to_string()],
                group: None,
            },
            enabled: true,
            priority: 0,
        }];
        assert!(apply_rules(&rules, "https://example.com/file.zip", "file.zip").is_none());
    }

    #[test]
    fn test_match_filename_only() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Filename only".to_string(),
            pattern: CategorizePattern::Contains("secret".to_string()),
            match_url: false,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["secret".to_string()],
                group: None,
            },
            enabled: true,
            priority: 0,
        };
        // URL doesn't contain "secret", but filename does
        assert!(rule.matches("https://example.com/file.zip", "secret-doc.zip"));
        // URL contains "secret" but match_url is false
        assert!(!rule.matches("https://example.com/secret/file.zip", "file.zip"));
    }

    #[tokio::test]
    async fn test_save_load_rules() {
        let tmp = tempfile::tempdir().unwrap();
        let rules = vec![CategorizeRule {
            id: "test1".to_string(),
            name: "Test Rule".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["test".to_string()],
                group: Some("Testing".to_string()),
            },
            enabled: true,
            priority: 5,
        }];
        save_rules(tmp.path(), &rules).await.unwrap();
        let loaded = load_rules(tmp.path()).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "test1");
        assert_eq!(loaded[0].name, "Test Rule");
        assert_eq!(loaded[0].priority, 5);
    }

    #[tokio::test]
    async fn test_load_rules_missing_file() {
        let tmp = tempfile::tempdir().unwrap();
        let loaded = load_rules(tmp.path()).await;
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn test_download_manager_add_rule() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = crate::DownloadManager::new(tmp.path().to_path_buf());

        let rule = CategorizeRule {
            id: "test1".to_string(),
            name: "Test Rule".to_string(),
            pattern: CategorizePattern::Contains("linux".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["linux".to_string()],
                group: Some("ISO".to_string()),
            },
            enabled: true,
            priority: 0,
        };

        dm.add_categorize_rule(rule).await.unwrap();

        let rules = dm.list_categorize_rules().await;
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].id, "test1");
        assert_eq!(rules[0].name, "Test Rule");
    }

    #[tokio::test]
    async fn test_download_manager_remove_rule() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = crate::DownloadManager::new(tmp.path().to_path_buf());

        let rule = CategorizeRule {
            id: "test1".to_string(),
            name: "Test Rule".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        };

        dm.add_categorize_rule(rule).await.unwrap();
        assert_eq!(dm.list_categorize_rules().await.len(), 1);

        assert!(dm.remove_categorize_rule("test1").await);
        assert_eq!(dm.list_categorize_rules().await.len(), 0);

        assert!(!dm.remove_categorize_rule("nonexistent").await);
    }

    #[tokio::test]
    async fn test_download_manager_apply_auto_categorize() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = crate::DownloadManager::new(tmp.path().to_path_buf());

        let rule = CategorizeRule {
            id: "test1".to_string(),
            name: "Video Files".to_string(),
            pattern: CategorizePattern::Wildcard("*.mp4".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["video".to_string(), "media".to_string()],
                group: Some("Media".to_string()),
            },
            enabled: true,
            priority: 0,
        };

        dm.add_categorize_rule(rule).await.unwrap();

        // Should match
        let action = dm
            .apply_auto_categorize("https://example.com/video.mp4", "video.mp4")
            .await;
        assert!(action.is_some());
        let action = action.unwrap();
        assert_eq!(action.tags, vec!["video", "media"]);
        assert_eq!(action.group, Some("Media".to_string()));

        // Should not match
        let action = dm
            .apply_auto_categorize("https://example.com/file.zip", "file.zip")
            .await;
        assert!(action.is_none());
    }

    // ========== Serialization Tests ==========

    #[test]
    fn test_pattern_contains_serialization_roundtrip() {
        let pattern = CategorizePattern::Contains("test".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        let deserialized: CategorizePattern = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, CategorizePattern::Contains(s) if s == "test"));
    }

    #[test]
    fn test_pattern_wildcard_serialization_roundtrip() {
        let pattern = CategorizePattern::Wildcard("*.mp4".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        let deserialized: CategorizePattern = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, CategorizePattern::Wildcard(s) if s == "*.mp4"));
    }

    #[test]
    fn test_pattern_exact_serialization_roundtrip() {
        let pattern = CategorizePattern::Exact("file.txt".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        let deserialized: CategorizePattern = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, CategorizePattern::Exact(s) if s == "file.txt"));
    }

    #[test]
    fn test_action_serialization_roundtrip() {
        let action = CategorizeAction {
            tags: vec!["tag1".to_string(), "tag2".to_string()],
            group: Some("Group1".to_string()),
        };
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: CategorizeAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tags, vec!["tag1", "tag2"]);
        assert_eq!(deserialized.group, Some("Group1".to_string()));
    }

    #[test]
    fn test_action_default_values() {
        let action = CategorizeAction::default();
        assert!(action.tags.is_empty());
        assert!(action.group.is_none());
    }

    #[test]
    fn test_action_serialization_with_none_group() {
        let action = CategorizeAction {
            tags: vec!["test".to_string()],
            group: None,
        };
        let json = serde_json::to_string(&action).unwrap();
        // None serializes as null by default
        assert!(json.contains("null"));
        let deserialized: CategorizeAction = serde_json::from_str(&json).unwrap();
        assert!(deserialized.group.is_none());
    }

    #[test]
    fn test_rule_serialization_roundtrip() {
        let rule = CategorizeRule {
            id: "rule1".to_string(),
            name: "Test Rule".to_string(),
            pattern: CategorizePattern::Wildcard("*.mp4".to_string()),
            match_url: true,
            match_filename: false,
            action: CategorizeAction {
                tags: vec!["video".to_string()],
                group: Some("Media".to_string()),
            },
            enabled: true,
            priority: 5,
        };
        let json = serde_json::to_string(&rule).unwrap();
        let deserialized: CategorizeRule = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "rule1");
        assert_eq!(deserialized.name, "Test Rule");
        assert!(deserialized.match_url);
        assert!(!deserialized.match_filename);
        assert_eq!(deserialized.priority, 5);
        assert!(deserialized.enabled);
    }

    #[test]
    fn test_rule_default_match_fields() {
        let json = r#"{"id":"1","name":"Test","pattern":{"Contains":"test"},"action":{"tags":[],"group":null},"priority":0}"#;
        let rule: CategorizeRule = serde_json::from_str(json).unwrap();
        assert!(rule.match_url);
        assert!(rule.match_filename);
        assert!(rule.enabled);
    }

    #[test]
    fn test_rule_extra_fields_ignored() {
        let json = r#"{"id":"1","name":"Test","pattern":{"Contains":"test"},"match_url":true,"match_filename":true,"action":{"tags":[],"group":null},"enabled":true,"priority":0,"extra_field":"ignored"}"#;
        let rule: CategorizeRule = serde_json::from_str(json).unwrap();
        assert_eq!(rule.id, "1");
    }

    // ========== Pattern Edge Cases ==========

    #[test]
    fn test_contains_empty_string() {
        let pattern = CategorizePattern::Contains("".to_string());
        assert!(pattern.matches("anything"));
        assert!(pattern.matches(""));
    }

    #[test]
    fn test_contains_unicode() {
        let pattern = CategorizePattern::Contains("中文".to_string());
        assert!(pattern.matches("这是一个中文测试"));
        assert!(!pattern.matches("this is english"));
    }

    #[test]
    fn test_contains_special_characters() {
        let pattern = CategorizePattern::Contains("[test]".to_string());
        assert!(pattern.matches("file [test] name"));
        assert!(!pattern.matches("file test name"));
    }

    #[test]
    fn test_exact_empty_string() {
        let pattern = CategorizePattern::Exact("".to_string());
        assert!(pattern.matches(""));
        assert!(!pattern.matches("not empty"));
    }

    #[test]
    fn test_exact_unicode_case_insensitive() {
        let pattern = CategorizePattern::Exact("TEST".to_string());
        assert!(pattern.matches("test"));
        assert!(pattern.matches("Test"));
        assert!(pattern.matches("TEST"));
    }

    // ========== Wildcard Edge Cases ==========

    #[test]
    fn test_wildcard_empty_pattern() {
        assert!(wildcard_match("", ""));
        assert!(!wildcard_match("", "notempty"));
    }

    #[test]
    fn test_wildcard_only_star() {
        assert!(wildcard_match("*", ""));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("*", "very long string"));
    }

    #[test]
    fn test_wildcard_multiple_stars() {
        assert!(wildcard_match("**", "anything"));
        assert!(wildcard_match("***", "anything"));
        assert!(wildcard_match("*.*", "file.txt"));
        assert!(!wildcard_match("*.*", "noextension"));
    }

    #[test]
    fn test_wildcard_consecutive_stars() {
        assert!(wildcard_match("a**b", "ab"));
        assert!(wildcard_match("a**b", "axb"));
        assert!(wildcard_match("a**b", "axxb"));
    }

    #[test]
    fn test_wildcard_question_mark() {
        assert!(wildcard_match("?", "a"));
        assert!(!wildcard_match("?", ""));
        assert!(!wildcard_match("?", "ab"));
        assert!(wildcard_match("a?c", "abc"));
        assert!(wildcard_match("a?c", "axc"));
        assert!(!wildcard_match("a?c", "ac"));
    }

    #[test]
    fn test_wildcard_mixed_patterns() {
        assert!(wildcard_match("*.tar.gz", "archive.tar.gz"));
        assert!(!wildcard_match("*.tar.gz", "archive.tar"));
        assert!(wildcard_match("linux-*-iso", "linux-5.4-iso"));
        assert!(wildcard_match("linux-*-iso", "linux-ubuntu-iso"));
        assert!(!wildcard_match("linux-*-iso", "windows-5.4-iso"));
    }

    #[test]
    fn test_wildcard_unicode() {
        assert!(wildcard_match("*中文*", "这是中文测试"));
        assert!(wildcard_match("测试?", "测试1"));
        assert!(!wildcard_match("测试?", "测试"));
    }

    // ========== Rule Matching Edge Cases ==========

    #[test]
    fn test_rule_match_url_only() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "URL only".to_string(),
            pattern: CategorizePattern::Contains("example".to_string()),
            match_url: true,
            match_filename: false,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        };
        assert!(rule.matches("https://example.com/file", "unrelated.txt"));
        assert!(!rule.matches("https://other.com/file", "example.txt"));
    }

    #[test]
    fn test_rule_match_filename_only() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Filename only".to_string(),
            pattern: CategorizePattern::Contains("example".to_string()),
            match_url: false,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        };
        assert!(!rule.matches("https://example.com/file", "unrelated.txt"));
        assert!(rule.matches("https://other.com/file", "example.txt"));
    }

    #[test]
    fn test_rule_match_both_disabled() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Both disabled".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: false,
            match_filename: false,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        };
        assert!(!rule.matches("https://test.com/file", "test.txt"));
    }

    #[test]
    fn test_rule_priority_zero() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Zero priority".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        };
        assert!(rule.matches("test", "test"));
    }

    #[test]
    fn test_rule_priority_max() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Max priority".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: u32::MAX,
        };
        assert!(rule.matches("test", "test"));
    }

    // ========== Apply Rules Edge Cases ==========

    #[test]
    fn test_apply_rules_empty_list() {
        let rules: Vec<CategorizeRule> = vec![];
        assert!(apply_rules(&rules, "url", "file").is_none());
    }

    #[test]
    fn test_apply_rules_all_disabled() {
        let rules = vec![
            CategorizeRule {
                id: "1".to_string(),
                name: "Disabled 1".to_string(),
                pattern: CategorizePattern::Contains("test".to_string()),
                match_url: true,
                match_filename: true,
                action: CategorizeAction {
                    tags: vec!["tag1".to_string()],
                    group: None,
                },
                enabled: false,
                priority: 0,
            },
            CategorizeRule {
                id: "2".to_string(),
                name: "Disabled 2".to_string(),
                pattern: CategorizePattern::Contains("test".to_string()),
                match_url: true,
                match_filename: true,
                action: CategorizeAction {
                    tags: vec!["tag2".to_string()],
                    group: None,
                },
                enabled: false,
                priority: 1,
            },
        ];
        assert!(apply_rules(&rules, "test", "test").is_none());
    }

    #[test]
    fn test_apply_rules_first_match_wins() {
        let rules = vec![
            CategorizeRule {
                id: "first".to_string(),
                name: "First".to_string(),
                pattern: CategorizePattern::Contains("file".to_string()),
                match_url: true,
                match_filename: false,
                action: CategorizeAction {
                    tags: vec!["first".to_string()],
                    group: None,
                },
                enabled: true,
                priority: 1,
            },
            CategorizeRule {
                id: "second".to_string(),
                name: "Second".to_string(),
                pattern: CategorizePattern::Contains("file".to_string()),
                match_url: true,
                match_filename: false,
                action: CategorizeAction {
                    tags: vec!["second".to_string()],
                    group: None,
                },
                enabled: true,
                priority: 2,
            },
        ];
        let action = apply_rules(&rules, "file.zip", "file.zip").unwrap();
        assert_eq!(action.tags, vec!["first"]);
    }

    #[test]
    fn test_apply_rules_same_priority_order_matters() {
        let rules = vec![
            CategorizeRule {
                id: "a".to_string(),
                name: "A".to_string(),
                pattern: CategorizePattern::Contains("test".to_string()),
                match_url: true,
                match_filename: false,
                action: CategorizeAction {
                    tags: vec!["a".to_string()],
                    group: None,
                },
                enabled: true,
                priority: 0,
            },
            CategorizeRule {
                id: "b".to_string(),
                name: "B".to_string(),
                pattern: CategorizePattern::Contains("test".to_string()),
                match_url: true,
                match_filename: false,
                action: CategorizeAction {
                    tags: vec!["b".to_string()],
                    group: None,
                },
                enabled: true,
                priority: 0,
            },
        ];
        let action = apply_rules(&rules, "test", "test").unwrap();
        // First one in order wins when priorities are equal
        assert_eq!(action.tags, vec!["a"]);
    }

    #[test]
    fn test_apply_rules_returns_action_reference() {
        let rules = vec![CategorizeRule {
            id: "1".to_string(),
            name: "Test".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["tag1".to_string(), "tag2".to_string()],
                group: Some("Group".to_string()),
            },
            enabled: true,
            priority: 0,
        }];
        let action = apply_rules(&rules, "test", "test").unwrap();
        assert_eq!(action.tags.len(), 2);
        assert_eq!(action.group, Some("Group".to_string()));
    }

    // ========== Persistence Tests ==========

    #[tokio::test]
    async fn test_save_rules_atomic_write() {
        let tmp = tempfile::tempdir().unwrap();
        let rules = vec![CategorizeRule {
            id: "1".to_string(),
            name: "Test".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        }];
        save_rules(tmp.path(), &rules).await.unwrap();
        // Verify no .tmp file left behind
        let tmp_file = tmp.path().join("categorize_rules.tmp");
        assert!(!tmp_file.exists());
        // Verify file is valid JSON
        let content = std::fs::read_to_string(tmp.path().join("categorize_rules.json")).unwrap();
        let _: Vec<CategorizeRule> = serde_json::from_str(&content).unwrap();
    }

    #[tokio::test]
    async fn test_save_rules_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let rules1 = vec![CategorizeRule {
            id: "1".to_string(),
            name: "First".to_string(),
            pattern: CategorizePattern::Contains("first".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        }];
        save_rules(tmp.path(), &rules1).await.unwrap();

        let rules2 = vec![CategorizeRule {
            id: "2".to_string(),
            name: "Second".to_string(),
            pattern: CategorizePattern::Contains("second".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        }];
        save_rules(tmp.path(), &rules2).await.unwrap();

        let loaded = load_rules(tmp.path()).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "2");
        assert_eq!(loaded[0].name, "Second");
    }

    #[tokio::test]
    async fn test_load_rules_corrupt_json() {
        let tmp = tempfile::tempdir().unwrap();
        let corrupt_path = tmp.path().join("categorize_rules.json");
        std::fs::write(&corrupt_path, "not valid json").unwrap();
        let loaded = load_rules(tmp.path()).await;
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn test_load_rules_empty_array() {
        let tmp = tempfile::tempdir().unwrap();
        let empty_path = tmp.path().join("categorize_rules.json");
        std::fs::write(&empty_path, "[]").unwrap();
        let loaded = load_rules(tmp.path()).await;
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn test_save_load_multiple_rules() {
        let tmp = tempfile::tempdir().unwrap();
        let rules = vec![
            CategorizeRule {
                id: "1".to_string(),
                name: "Rule 1".to_string(),
                pattern: CategorizePattern::Contains("test1".to_string()),
                match_url: true,
                match_filename: true,
                action: CategorizeAction {
                    tags: vec!["tag1".to_string()],
                    group: None,
                },
                enabled: true,
                priority: 1,
            },
            CategorizeRule {
                id: "2".to_string(),
                name: "Rule 2".to_string(),
                pattern: CategorizePattern::Wildcard("*.mp4".to_string()),
                match_url: false,
                match_filename: true,
                action: CategorizeAction {
                    tags: vec!["video".to_string()],
                    group: Some("Media".to_string()),
                },
                enabled: false,
                priority: 2,
            },
        ];
        save_rules(tmp.path(), &rules).await.unwrap();
        let loaded = load_rules(tmp.path()).await;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "1");
        assert_eq!(loaded[1].id, "2");
        assert!(!loaded[1].enabled);
        assert_eq!(loaded[1].action.group, Some("Media".to_string()));
    }

    // ========== Error Handling ==========

    #[test]
    fn test_categorize_error_display() {
        let error = CategorizeError::Io("test error".to_string());
        assert_eq!(format!("{}", error), "I/O error: test error");
    }

    #[test]
    fn test_categorize_error_debug() {
        let error = CategorizeError::Io("test error".to_string());
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("Io"));
        assert!(debug_str.contains("test error"));
    }

    // ========== Traits ==========

    #[test]
    fn test_pattern_clone() {
        let pattern = CategorizePattern::Contains("test".to_string());
        let cloned = pattern.clone();
        assert!(matches!(cloned, CategorizePattern::Contains(s) if s == "test"));
    }

    #[test]
    fn test_pattern_debug() {
        let pattern = CategorizePattern::Wildcard("*.mp4".to_string());
        let debug_str = format!("{:?}", pattern);
        assert!(debug_str.contains("Wildcard"));
        assert!(debug_str.contains("*.mp4"));
    }

    #[test]
    fn test_action_clone() {
        let action = CategorizeAction {
            tags: vec!["tag1".to_string()],
            group: Some("Group".to_string()),
        };
        let cloned = action.clone();
        assert_eq!(cloned.tags, vec!["tag1"]);
        assert_eq!(cloned.group, Some("Group".to_string()));
    }

    #[test]
    fn test_action_debug() {
        let action = CategorizeAction {
            tags: vec!["tag1".to_string()],
            group: None,
        };
        let debug_str = format!("{:?}", action);
        assert!(debug_str.contains("tags"));
        assert!(debug_str.contains("tag1"));
    }

    #[test]
    fn test_rule_clone() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Test".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        };
        let cloned = rule.clone();
        assert_eq!(cloned.id, "1");
        assert_eq!(cloned.name, "Test");
    }

    #[test]
    fn test_rule_debug() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Test".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        };
        let debug_str = format!("{:?}", rule);
        assert!(debug_str.contains("CategorizeRule"));
        assert!(debug_str.contains("id"));
    }

    // ========== Complex Scenarios ==========

    #[test]
    fn test_complete_workflow() {
        let rules = vec![
            CategorizeRule {
                id: "video".to_string(),
                name: "Video Files".to_string(),
                pattern: CategorizePattern::Wildcard("*.mp4".to_string()),
                match_url: true,
                match_filename: true,
                action: CategorizeAction {
                    tags: vec!["video".to_string(), "media".to_string()],
                    group: Some("Media".to_string()),
                },
                enabled: true,
                priority: 1,
            },
            CategorizeRule {
                id: "linux".to_string(),
                name: "Linux ISOs".to_string(),
                pattern: CategorizePattern::Contains("linux".to_string()),
                match_url: true,
                match_filename: true,
                action: CategorizeAction {
                    tags: vec!["linux".to_string(), "iso".to_string()],
                    group: Some("ISO".to_string()),
                },
                enabled: true,
                priority: 2,
            },
            CategorizeRule {
                id: "disabled".to_string(),
                name: "Disabled Rule".to_string(),
                pattern: CategorizePattern::Contains("test".to_string()),
                match_url: true,
                match_filename: true,
                action: CategorizeAction {
                    tags: vec!["disabled".to_string()],
                    group: None,
                },
                enabled: false,
                priority: 0,
            },
        ];

        // Video file should match video rule
        let action = apply_rules(&rules, "https://example.com/video.mp4", "video.mp4");
        assert!(action.is_some());
        let action = action.unwrap();
        assert_eq!(action.tags, vec!["video", "media"]);
        assert_eq!(action.group, Some("Media".to_string()));

        // Linux ISO should match linux rule
        let action = apply_rules(
            &rules,
            "https://example.com/linux-ubuntu.iso",
            "linux-ubuntu.iso",
        );
        assert!(action.is_some());
        let action = action.unwrap();
        assert_eq!(action.tags, vec!["linux", "iso"]);

        // Test string should not match disabled rule
        let action = apply_rules(&rules, "https://test.com/file", "test.txt");
        assert!(action.is_none());

        // Unrelated file should not match any rule
        let action = apply_rules(&rules, "https://example.com/file.zip", "file.zip");
        assert!(action.is_none());
    }

    #[test]
    fn test_url_and_filename_both_match() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Both match".to_string(),
            pattern: CategorizePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["matched".to_string()],
                group: None,
            },
            enabled: true,
            priority: 0,
        };
        // Both URL and filename contain "test"
        assert!(rule.matches("https://test.com/file", "test.txt"));
        // Only URL contains "test"
        assert!(rule.matches("https://test.com/file", "unrelated.txt"));
        // Only filename contains "test"
        assert!(rule.matches("https://example.com/file", "test.txt"));
        // Neither contains "test"
        assert!(!rule.matches("https://example.com/file", "unrelated.txt"));
    }

    #[test]
    fn test_case_insensitive_matching() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Case insensitive".to_string(),
            pattern: CategorizePattern::Contains("TEST".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction::default(),
            enabled: true,
            priority: 0,
        };
        assert!(rule.matches("https://example.com/test/file", "file.txt"));
        assert!(rule.matches("https://example.com/Test/file", "file.txt"));
        assert!(rule.matches("https://example.com/TEST/file", "file.txt"));
    }

    #[test]
    fn test_unicode_in_patterns() {
        let rule = CategorizeRule {
            id: "1".to_string(),
            name: "Unicode test".to_string(),
            pattern: CategorizePattern::Contains("中文".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["chinese".to_string()],
                group: None,
            },
            enabled: true,
            priority: 0,
        };
        assert!(rule.matches("https://example.com/中文/file", "file.txt"));
        assert!(rule.matches("https://example.com/file", "中文文件.txt"));
        assert!(!rule.matches("https://example.com/english/file", "english.txt"));
    }

    #[tokio::test]
    async fn test_download_manager_multiple_rules() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = crate::DownloadManager::new(tmp.path().to_path_buf());

        let rule1 = CategorizeRule {
            id: "video".to_string(),
            name: "Video".to_string(),
            pattern: CategorizePattern::Wildcard("*.mp4".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["video".to_string()],
                group: Some("Media".to_string()),
            },
            enabled: true,
            priority: 1,
        };

        let rule2 = CategorizeRule {
            id: "audio".to_string(),
            name: "Audio".to_string(),
            pattern: CategorizePattern::Wildcard("*.mp3".to_string()),
            match_url: true,
            match_filename: true,
            action: CategorizeAction {
                tags: vec!["audio".to_string()],
                group: Some("Media".to_string()),
            },
            enabled: true,
            priority: 2,
        };

        dm.add_categorize_rule(rule1).await.unwrap();
        dm.add_categorize_rule(rule2).await.unwrap();

        let rules = dm.list_categorize_rules().await;
        assert_eq!(rules.len(), 2);

        // Test video matching
        let action = dm
            .apply_auto_categorize("https://example.com/video.mp4", "video.mp4")
            .await;
        assert!(action.is_some());
        assert_eq!(action.unwrap().tags, vec!["video"]);

        // Test audio matching
        let action = dm
            .apply_auto_categorize("https://example.com/audio.mp3", "audio.mp3")
            .await;
        assert!(action.is_some());
        assert_eq!(action.unwrap().tags, vec!["audio"]);

        // Test no match
        let action = dm
            .apply_auto_categorize("https://example.com/file.zip", "file.zip")
            .await;
        assert!(action.is_none());
    }
}
