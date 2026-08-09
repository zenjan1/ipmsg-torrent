//! Download path rules for automatic save path assignment
//!
//! Users can define rules that match against download URLs (or filenames)
//! and automatically set the save path for new downloads.
//!
//! Features:
//! - Multiple pattern types: Contains, Wildcard, Exact
//! - Priority-based rule matching (lower priority number = higher precedence)
//! - Optional filename and URL matching toggles
//! - Persistence to `path_rules.json`
//! - Integration with DownloadManager for automatic path assignment

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;
use tracing::debug;

/// Errors from path rules operations.
#[derive(Error, Debug)]
pub enum PathRulesError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Rule not found: {0}")]
    NotFound(String),
    #[error("Invalid save path: {0}")]
    InvalidPath(String),
}

/// A pattern to match against download URLs/filenames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PathRulePattern {
    /// Case-insensitive substring match
    Contains(String),
    /// Glob-style wildcard (e.g. "*.mp4", "linux-*-iso")
    Wildcard(String),
    /// Exact match (case-insensitive)
    Exact(String),
}

impl PathRulePattern {
    /// Check if the given text matches this pattern.
    pub fn matches(&self, text: &str) -> bool {
        let lower = text.to_lowercase();
        match self {
            PathRulePattern::Contains(sub) => lower.contains(&sub.to_lowercase()),
            PathRulePattern::Wildcard(pat) => wildcard_match(&pat.to_lowercase(), &lower),
            PathRulePattern::Exact(exact) => lower == exact.to_lowercase(),
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

/// A single path rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathRule {
    /// Unique rule ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Pattern to match against URL or filename
    pub pattern: PathRulePattern,
    /// Whether to match against the URL
    #[serde(default = "default_true")]
    pub match_url: bool,
    /// Whether to match against the filename
    #[serde(default = "default_true")]
    pub match_filename: bool,
    /// Save path to assign when matched
    pub save_path: PathBuf,
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

impl PathRule {
    /// Create a new path rule.
    pub fn new(id: String, name: String, pattern: PathRulePattern, save_path: PathBuf) -> Self {
        Self {
            id,
            name,
            pattern,
            match_url: true,
            match_filename: true,
            save_path,
            enabled: true,
            priority: 0,
        }
    }

    /// Check if this rule matches the given URL and filename.
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

/// Manager for path rules.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PathRuleManager {
    /// List of path rules
    pub rules: Vec<PathRule>,
}

impl PathRuleManager {
    /// Create a new empty path rule manager.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a new path rule.
    pub fn add_rule(&mut self, rule: PathRule) {
        self.rules.push(rule);
        self.sort_rules();
    }

    /// Remove a path rule by ID.
    pub fn remove_rule(&mut self, rule_id: &str) -> Result<PathRule, PathRulesError> {
        if let Some(pos) = self.rules.iter().position(|r| r.id == rule_id) {
            Ok(self.rules.remove(pos))
        } else {
            Err(PathRulesError::NotFound(rule_id.to_string()))
        }
    }

    /// Get a path rule by ID.
    pub fn get_rule(&self, rule_id: &str) -> Option<&PathRule> {
        self.rules.iter().find(|r| r.id == rule_id)
    }

    /// List all path rules.
    pub fn list_rules(&self) -> &[PathRule] {
        &self.rules
    }

    /// Find the first matching rule for the given URL and filename.
    /// Rules are checked in priority order (lower number = higher priority).
    pub fn find_matching_rule(&self, url: &str, filename: &str) -> Option<&PathRule> {
        self.rules.iter().find(|rule| rule.matches(url, filename))
    }

    /// Sort rules by priority (ascending).
    fn sort_rules(&mut self) {
        self.rules.sort_by_key(|r| r.priority);
    }

    /// Enable or disable a rule.
    pub fn set_rule_enabled(&mut self, rule_id: &str, enabled: bool) -> Result<(), PathRulesError> {
        if let Some(rule) = self.rules.iter_mut().find(|r| r.id == rule_id) {
            rule.enabled = enabled;
            Ok(())
        } else {
            Err(PathRulesError::NotFound(rule_id.to_string()))
        }
    }

    /// Update a rule's save path.
    pub fn update_rule_save_path(
        &mut self,
        rule_id: &str,
        save_path: PathBuf,
    ) -> Result<(), PathRulesError> {
        if let Some(rule) = self.rules.iter_mut().find(|r| r.id == rule_id) {
            rule.save_path = save_path;
            Ok(())
        } else {
            Err(PathRulesError::NotFound(rule_id.to_string()))
        }
    }

    /// Update a rule's priority.
    pub fn update_rule_priority(
        &mut self,
        rule_id: &str,
        priority: u32,
    ) -> Result<(), PathRulesError> {
        if let Some(rule) = self.rules.iter_mut().find(|r| r.id == rule_id) {
            rule.priority = priority;
            self.sort_rules();
            Ok(())
        } else {
            Err(PathRulesError::NotFound(rule_id.to_string()))
        }
    }
}

/// Save path rules to disk (atomic write).
pub async fn save_path_rules(manager: &PathRuleManager, path: &Path) -> Result<(), PathRulesError> {
    let json = serde_json::to_string_pretty(manager)?;
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, &json).await?;
    fs::rename(&temp_path, path).await?;
    debug!("Saved path rules to {:?}", path);
    Ok(())
}

/// Load path rules from disk.
pub async fn load_path_rules(path: &Path) -> Result<PathRuleManager, PathRulesError> {
    if !path.exists() {
        debug!("Path rules file not found, using empty rules");
        return Ok(PathRuleManager::new());
    }

    let json = fs::read_to_string(path).await?;
    let manager: PathRuleManager = serde_json::from_str(&json)?;
    debug!("Loaded {} path rules from {:?}", manager.rules.len(), path);
    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_pattern_contains() {
        let pattern = PathRulePattern::Contains("linux".to_string());
        assert!(pattern.matches("https://example.com/linux-distro.iso"));
        assert!(pattern.matches("https://example.com/Linux-Distro.iso"));
        assert!(!pattern.matches("https://example.com/windows.iso"));
    }

    #[test]
    fn test_pattern_wildcard() {
        let pattern = PathRulePattern::Wildcard("*.mp4".to_string());
        assert!(pattern.matches("video.mp4"));
        assert!(pattern.matches("https://example.com/video.mp4"));
        assert!(!pattern.matches("video.avi"));
    }

    #[test]
    fn test_pattern_exact() {
        let pattern = PathRulePattern::Exact("ubuntu.iso".to_string());
        assert!(pattern.matches("ubuntu.iso"));
        assert!(pattern.matches("Ubuntu.ISO"));
        assert!(!pattern.matches("ubuntu-22.04.iso"));
    }

    #[test]
    fn test_path_rule_matches_url() {
        let rule = PathRule::new(
            "1".to_string(),
            "Linux ISOs".to_string(),
            PathRulePattern::Contains("linux".to_string()),
            PathBuf::from("/downloads/linux"),
        );

        assert!(rule.matches("https://example.com/linux-distro.iso", "linux-distro.iso"));
        assert!(!rule.matches("https://example.com/windows.iso", "windows.iso"));
    }

    #[test]
    fn test_path_rule_matches_filename() {
        let rule = PathRule {
            id: "1".to_string(),
            name: "Videos".to_string(),
            pattern: PathRulePattern::Wildcard("*.mp4".to_string()),
            match_url: false,
            match_filename: true,
            save_path: PathBuf::from("/downloads/videos"),
            enabled: true,
            priority: 0,
        };

        assert!(rule.matches("https://example.com/file", "video.mp4"));
        assert!(!rule.matches("https://example.com/video.mp4", "file.avi"));
    }

    #[test]
    fn test_path_rule_disabled() {
        let mut rule = PathRule::new(
            "1".to_string(),
            "Linux ISOs".to_string(),
            PathRulePattern::Contains("linux".to_string()),
            PathBuf::from("/downloads/linux"),
        );
        rule.enabled = false;

        assert!(!rule.matches("https://example.com/linux-distro.iso", "linux-distro.iso"));
    }

    #[test]
    fn test_path_rule_manager_add_remove() {
        let mut manager = PathRuleManager::new();

        let rule1 = PathRule::new(
            "1".to_string(),
            "Rule 1".to_string(),
            PathRulePattern::Contains("linux".to_string()),
            PathBuf::from("/downloads/linux"),
        );

        let rule2 = PathRule::new(
            "2".to_string(),
            "Rule 2".to_string(),
            PathRulePattern::Contains("windows".to_string()),
            PathBuf::from("/downloads/windows"),
        );

        manager.add_rule(rule1);
        manager.add_rule(rule2);

        assert_eq!(manager.list_rules().len(), 2);

        let removed = manager.remove_rule("1").unwrap();
        assert_eq!(removed.name, "Rule 1");
        assert_eq!(manager.list_rules().len(), 1);
    }

    #[test]
    fn test_path_rule_manager_priority_ordering() {
        let mut manager = PathRuleManager::new();

        let mut rule1 = PathRule::new(
            "1".to_string(),
            "Low Priority".to_string(),
            PathRulePattern::Contains("iso".to_string()),
            PathBuf::from("/downloads/low"),
        );
        rule1.priority = 10;

        let mut rule2 = PathRule::new(
            "2".to_string(),
            "High Priority".to_string(),
            PathRulePattern::Contains("iso".to_string()),
            PathBuf::from("/downloads/high"),
        );
        rule2.priority = 1;

        manager.add_rule(rule1);
        manager.add_rule(rule2);

        // High priority rule should be first
        assert_eq!(manager.list_rules()[0].name, "High Priority");
        assert_eq!(manager.list_rules()[1].name, "Low Priority");
    }

    #[test]
    fn test_path_rule_manager_find_matching() {
        let mut manager = PathRuleManager::new();

        let rule1 = PathRule::new(
            "1".to_string(),
            "Linux".to_string(),
            PathRulePattern::Contains("linux".to_string()),
            PathBuf::from("/downloads/linux"),
        );

        let rule2 = PathRule::new(
            "2".to_string(),
            "Videos".to_string(),
            PathRulePattern::Wildcard("*.mp4".to_string()),
            PathBuf::from("/downloads/videos"),
        );

        manager.add_rule(rule1);
        manager.add_rule(rule2);

        let matched = manager.find_matching_rule("https://example.com/linux.iso", "linux.iso");
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().name, "Linux");

        let matched = manager.find_matching_rule("https://example.com/video", "video.mp4");
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().name, "Videos");

        let matched = manager.find_matching_rule("https://example.com/file.txt", "file.txt");
        assert!(matched.is_none());
    }

    #[test]
    fn test_path_rule_manager_set_enabled() {
        let mut manager = PathRuleManager::new();

        let rule = PathRule::new(
            "1".to_string(),
            "Rule".to_string(),
            PathRulePattern::Contains("test".to_string()),
            PathBuf::from("/downloads/test"),
        );

        manager.add_rule(rule);

        manager.set_rule_enabled("1", false).unwrap();
        assert!(!manager.get_rule("1").unwrap().enabled);

        manager.set_rule_enabled("1", true).unwrap();
        assert!(manager.get_rule("1").unwrap().enabled);

        assert!(manager.set_rule_enabled("nonexistent", true).is_err());
    }

    #[test]
    fn test_path_rule_manager_update_save_path() {
        let mut manager = PathRuleManager::new();

        let rule = PathRule::new(
            "1".to_string(),
            "Rule".to_string(),
            PathRulePattern::Contains("test".to_string()),
            PathBuf::from("/downloads/old"),
        );

        manager.add_rule(rule);

        manager
            .update_rule_save_path("1", PathBuf::from("/downloads/new"))
            .unwrap();
        assert_eq!(
            manager.get_rule("1").unwrap().save_path,
            PathBuf::from("/downloads/new")
        );

        assert!(
            manager
                .update_rule_save_path("nonexistent", PathBuf::from("/x"))
                .is_err()
        );
    }

    #[test]
    fn test_path_rule_manager_update_priority() {
        let mut manager = PathRuleManager::new();

        let mut rule = PathRule::new(
            "1".to_string(),
            "Rule".to_string(),
            PathRulePattern::Contains("test".to_string()),
            PathBuf::from("/downloads/test"),
        );
        rule.priority = 5;

        manager.add_rule(rule);

        manager.update_rule_priority("1", 10).unwrap();
        assert_eq!(manager.get_rule("1").unwrap().priority, 10);

        assert!(manager.update_rule_priority("nonexistent", 1).is_err());
    }

    #[test]
    fn test_path_rule_manager_remove_nonexistent() {
        let mut manager = PathRuleManager::new();
        assert!(manager.remove_rule("nonexistent").is_err());
    }

    #[tokio::test]
    async fn test_save_load_path_rules() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("path_rules.json");

        let mut manager = PathRuleManager::new();

        let rule = PathRule::new(
            "1".to_string(),
            "Test Rule".to_string(),
            PathRulePattern::Contains("test".to_string()),
            PathBuf::from("/downloads/test"),
        );

        manager.add_rule(rule);

        save_path_rules(&manager, &path).await.unwrap();

        let loaded = load_path_rules(&path).await.unwrap();
        assert_eq!(loaded.rules.len(), 1);
        assert_eq!(loaded.rules[0].name, "Test Rule");
        assert_eq!(loaded.rules[0].save_path, PathBuf::from("/downloads/test"));
    }

    #[tokio::test]
    async fn test_load_path_rules_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nonexistent.json");

        let loaded = load_path_rules(&path).await.unwrap();
        assert_eq!(loaded.rules.len(), 0);
    }

    #[tokio::test]
    async fn test_save_load_path_rules_empty() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("path_rules.json");

        let manager = PathRuleManager::new();
        save_path_rules(&manager, &path).await.unwrap();

        let loaded = load_path_rules(&path).await.unwrap();
        assert_eq!(loaded.rules.len(), 0);
    }

    #[test]
    fn test_wildcard_complex_patterns() {
        let pattern = PathRulePattern::Wildcard("linux-*-iso".to_string());
        assert!(pattern.matches("linux-ubuntu-iso"));
        assert!(pattern.matches("linux-debian-iso"));
        assert!(!pattern.matches("linux-iso"));
        assert!(!pattern.matches("windows-ubuntu-iso"));
    }

    #[test]
    fn test_wildcard_question_mark() {
        let pattern = PathRulePattern::Wildcard("file?.txt".to_string());
        assert!(pattern.matches("file1.txt"));
        assert!(pattern.matches("fileA.txt"));
        assert!(!pattern.matches("file.txt"));
        assert!(!pattern.matches("file12.txt"));
    }

    #[test]
    fn test_path_rule_both_url_and_filename_match() {
        let rule = PathRule {
            id: "1".to_string(),
            name: "Both".to_string(),
            pattern: PathRulePattern::Contains("test".to_string()),
            match_url: true,
            match_filename: true,
            save_path: PathBuf::from("/downloads/test"),
            enabled: true,
            priority: 0,
        };

        // URL matches
        assert!(rule.matches("https://example.com/test-file", "other-file"));
        // Filename matches
        assert!(rule.matches("https://example.com/other-file", "test-file"));
        // Both match
        assert!(rule.matches("https://example.com/test-file", "test-file"));
        // Neither matches
        assert!(!rule.matches("https://example.com/other-file", "other-file"));
    }

    #[test]
    fn test_path_rule_neither_url_nor_filename() {
        let rule = PathRule {
            id: "1".to_string(),
            name: "Neither".to_string(),
            pattern: PathRulePattern::Contains("test".to_string()),
            match_url: false,
            match_filename: false,
            save_path: PathBuf::from("/downloads/test"),
            enabled: true,
            priority: 0,
        };

        // Neither matches even if pattern matches
        assert!(!rule.matches("https://example.com/test-file", "test-file"));
    }
}
