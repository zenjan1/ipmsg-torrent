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
}
