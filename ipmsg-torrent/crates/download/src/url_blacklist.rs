//! URL Blacklist for blocking unwanted downloads
//!
//! Provides domain-level and pattern-level URL blocking to prevent
//! downloads from known bad sources, ad domains, or restricted sites.
//!
//! Features:
//! - Domain-based blocking (e.g., block all of "ads.example.com")
//! - Exact URL matching
//! - Wildcard pattern matching (supports * and ?)
//! - Regular expression matching
//! - Persistent blacklist configuration
//! - DownloadManager integration

use serde::{Deserialize, Serialize};
use std::path::Path;
use url::Url;

/// A single blacklist entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlacklistEntry {
    /// Unique entry ID
    pub id: String,
    /// Human-readable name/description
    pub name: String,
    /// Match pattern type
    pub pattern: BlacklistPattern,
    /// Whether this entry is enabled
    pub enabled: bool,
    /// Optional reason for blocking
    pub reason: Option<String>,
    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Pattern types for URL matching
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum BlacklistPattern {
    /// Block all URLs from this domain (and subdomains)
    Domain(String),
    /// Block exact URL match
    Exact(String),
    /// Block URLs matching wildcard pattern (* and ?)
    Wildcard(String),
    /// Block URLs matching regular expression
    Regex(String),
}

/// Result of checking a URL against the blacklist
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlacklistCheckResult {
    /// Whether the URL is blocked
    pub blocked: bool,
    /// ID of the matching entry (if blocked)
    pub matched_entry_id: Option<String>,
    /// Name of the matching entry (if blocked)
    pub matched_entry_name: Option<String>,
    /// Reason for blocking (if blocked)
    pub reason: Option<String>,
}

/// Blacklist configuration (persisted)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlacklistConfig {
    /// Whether blacklist checking is enabled
    pub enabled: bool,
    /// List of blacklist entries
    pub entries: Vec<BlacklistEntry>,
}

impl Default for BlacklistConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            entries: Vec::new(),
        }
    }
}

/// Persistence error
#[derive(Debug, thiserror::Error)]
pub enum BlacklistError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Entry not found: {0}")]
    NotFound(String),
    #[error("Invalid regex pattern: {0}")]
    InvalidRegex(String),
}

impl BlacklistEntry {
    /// Create a new blacklist entry
    pub fn new(
        id: String,
        name: String,
        pattern: BlacklistPattern,
        reason: Option<String>,
    ) -> Self {
        Self {
            id,
            name,
            pattern,
            enabled: true,
            reason,
            created_at: chrono::Utc::now(),
        }
    }

    /// Check if this entry matches the given URL
    pub fn matches(&self, url: &str) -> bool {
        if !self.enabled {
            return false;
        }
        match &self.pattern {
            BlacklistPattern::Domain(domain) => domain_matches(url, domain),
            BlacklistPattern::Exact(exact) => url == exact,
            BlacklistPattern::Wildcard(pattern) => wildcard_matches(pattern, url),
            BlacklistPattern::Regex(regex_str) => regex_lite::Regex::new(regex_str)
                .map(|re| re.is_match(url))
                .unwrap_or(false),
        }
    }
}

/// Check if a URL's domain matches the blocked domain (including subdomains)
fn domain_matches(url: &str, blocked_domain: &str) -> bool {
    let blocked_lower = blocked_domain.to_lowercase();
    match Url::parse(url) {
        Ok(parsed) => {
            if let Some(host) = parsed.host_str() {
                let host_lower = host.to_lowercase();
                host_lower == blocked_lower || host_lower.ends_with(&format!(".{}", blocked_lower))
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

/// Simple wildcard matching (supports * and ?)
fn wildcard_matches(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    wildcard_match_recursive(&pattern_chars, &text_chars)
}

fn wildcard_match_recursive(pattern: &[char], text: &[char]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = None;
    let mut star_ti = None;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == '*' {
            star_pi = Some(pi);
            star_ti = Some(ti);
            pi += 1;
        } else if let (Some(sp), Some(st)) = (star_pi, star_ti) {
            pi = sp + 1;
            let new_st = st + 1;
            star_ti = Some(new_st);
            ti = new_st;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Check a URL against the blacklist config
pub fn check_url_blacklist(url: &str, config: &BlacklistConfig) -> BlacklistCheckResult {
    if !config.enabled {
        return BlacklistCheckResult {
            blocked: false,
            matched_entry_id: None,
            matched_entry_name: None,
            reason: None,
        };
    }

    for entry in &config.entries {
        if entry.matches(url) {
            return BlacklistCheckResult {
                blocked: true,
                matched_entry_id: Some(entry.id.clone()),
                matched_entry_name: Some(entry.name.clone()),
                reason: entry.reason.clone(),
            };
        }
    }

    BlacklistCheckResult {
        blocked: false,
        matched_entry_id: None,
        matched_entry_name: None,
        reason: None,
    }
}

/// Save blacklist config to disk (atomic write)
pub fn save_blacklist_config(
    config: &BlacklistConfig,
    data_dir: &Path,
) -> Result<(), BlacklistError> {
    let path = data_dir.join("url_blacklist.json");
    let json = serde_json::to_string_pretty(config)?;
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, &json)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load blacklist config from disk
pub fn load_blacklist_config(data_dir: &Path) -> Option<BlacklistConfig> {
    let path = data_dir.join("url_blacklist.json");
    let content = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_entry(id: &str, pattern: BlacklistPattern) -> BlacklistEntry {
        BlacklistEntry::new(id.to_string(), format!("Entry {}", id), pattern, None)
    }

    fn make_entry_with_reason(id: &str, pattern: BlacklistPattern, reason: &str) -> BlacklistEntry {
        BlacklistEntry::new(
            id.to_string(),
            format!("Entry {}", id),
            pattern,
            Some(reason.to_string()),
        )
    }

    #[test]
    fn test_domain_match_exact() {
        let entry = make_entry("1", BlacklistPattern::Domain("ads.example.com".to_string()));
        assert!(entry.matches("http://ads.example.com/file.txt"));
        assert!(entry.matches("https://ads.example.com/path/to/file"));
    }

    #[test]
    fn test_domain_match_subdomain() {
        let entry = make_entry("1", BlacklistPattern::Domain("example.com".to_string()));
        assert!(entry.matches("http://sub.example.com/file.txt"));
        assert!(entry.matches("https://deep.sub.example.com/file.txt"));
    }

    #[test]
    fn test_domain_no_match() {
        let entry = make_entry("1", BlacklistPattern::Domain("blocked.com".to_string()));
        assert!(!entry.matches("http://example.com/file.txt"));
        assert!(!entry.matches("http://notblocked.com/file.txt"));
    }

    #[test]
    fn test_domain_case_insensitive() {
        let entry = make_entry("1", BlacklistPattern::Domain("Example.COM".to_string()));
        assert!(entry.matches("http://example.com/file.txt"));
        assert!(entry.matches("http://EXAMPLE.COM/file.txt"));
    }

    #[test]
    fn test_exact_match() {
        let entry = make_entry(
            "1",
            BlacklistPattern::Exact("http://example.com/file.txt".to_string()),
        );
        assert!(entry.matches("http://example.com/file.txt"));
        assert!(!entry.matches("http://example.com/other.txt"));
    }

    #[test]
    fn test_wildcard_match_star() {
        let entry = make_entry(
            "1",
            BlacklistPattern::Wildcard("http://example.com/*.exe".to_string()),
        );
        assert!(entry.matches("http://example.com/malware.exe"));
        assert!(entry.matches("http://example.com/path/malware.exe"));
        assert!(!entry.matches("http://example.com/file.txt"));
    }

    #[test]
    fn test_wildcard_match_question() {
        let entry = make_entry(
            "1",
            BlacklistPattern::Wildcard("http://example.com/file?.txt".to_string()),
        );
        assert!(entry.matches("http://example.com/file1.txt"));
        assert!(entry.matches("http://example.com/fileA.txt"));
        assert!(!entry.matches("http://example.com/file10.txt"));
    }

    #[test]
    fn test_regex_match() {
        let entry = make_entry(
            "1",
            BlacklistPattern::Regex(r"http://example\.com/\d+\.zip".to_string()),
        );
        assert!(entry.matches("http://example.com/123.zip"));
        assert!(entry.matches("http://example.com/42.zip"));
        assert!(!entry.matches("http://example.com/abc.zip"));
    }

    #[test]
    fn test_regex_invalid() {
        let entry = make_entry("1", BlacklistPattern::Regex("[invalid".to_string()));
        assert!(!entry.matches("anything"));
    }

    #[test]
    fn test_disabled_entry() {
        let mut entry = make_entry("1", BlacklistPattern::Domain("blocked.com".to_string()));
        entry.enabled = false;
        assert!(!entry.matches("http://blocked.com/file.txt"));
    }

    #[test]
    fn test_check_blacklist_blocked() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![make_entry_with_reason(
                "1",
                BlacklistPattern::Domain("ads.com".to_string()),
                "Ad server",
            )],
        };
        let result = check_url_blacklist("http://ads.com/banner.js", &config);
        assert!(result.blocked);
        assert_eq!(result.matched_entry_id.as_deref(), Some("1"));
        assert_eq!(result.matched_entry_name.as_deref(), Some("Entry 1"));
        assert_eq!(result.reason.as_deref(), Some("Ad server"));
    }

    #[test]
    fn test_check_blacklist_not_blocked() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![make_entry(
                "1",
                BlacklistPattern::Domain("ads.com".to_string()),
            )],
        };
        let result = check_url_blacklist("http://good.com/file.txt", &config);
        assert!(!result.blocked);
        assert!(result.matched_entry_id.is_none());
    }

    #[test]
    fn test_check_blacklist_disabled() {
        let config = BlacklistConfig {
            enabled: false,
            entries: vec![make_entry(
                "1",
                BlacklistPattern::Domain("ads.com".to_string()),
            )],
        };
        let result = check_url_blacklist("http://ads.com/banner.js", &config);
        assert!(!result.blocked);
    }

    #[test]
    fn test_check_blacklist_first_match() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![
                make_entry_with_reason(
                    "1",
                    BlacklistPattern::Domain("example.com".to_string()),
                    "First",
                ),
                make_entry_with_reason(
                    "2",
                    BlacklistPattern::Wildcard("http://example.com/*.exe".to_string()),
                    "Second",
                ),
            ],
        };
        let result = check_url_blacklist("http://example.com/file.exe", &config);
        assert!(result.blocked);
        assert_eq!(result.matched_entry_id.as_deref(), Some("1"));
        assert_eq!(result.reason.as_deref(), Some("First"));
    }

    #[test]
    fn test_check_blacklist_empty_entries() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![],
        };
        let result = check_url_blacklist("http://anything.com/file.txt", &config);
        assert!(!result.blocked);
    }

    #[test]
    fn test_save_and_load_config() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![
                make_entry("1", BlacklistPattern::Domain("ads.com".to_string())),
                make_entry("2", BlacklistPattern::Wildcard("*.exe".to_string())),
            ],
        };

        save_blacklist_config(&config, temp_dir.path()).unwrap();
        let loaded = load_blacklist_config(temp_dir.path()).unwrap();

        assert!(loaded.enabled);
        assert_eq!(loaded.entries.len(), 2);
        assert_eq!(loaded.entries[0].id, "1");
        assert_eq!(loaded.entries[1].id, "2");
    }

    #[test]
    fn test_load_missing_config() {
        let temp_dir = TempDir::new().unwrap();
        let loaded = load_blacklist_config(temp_dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_creates_file() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlacklistConfig::default();
        save_blacklist_config(&config, temp_dir.path()).unwrap();
        assert!(temp_dir.path().join("url_blacklist.json").exists());
    }

    #[test]
    fn test_domain_no_match_invalid_url() {
        let entry = make_entry("1", BlacklistPattern::Domain("example.com".to_string()));
        assert!(!entry.matches("not-a-url"));
        assert!(!entry.matches(""));
    }

    #[test]
    fn test_wildcard_empty_pattern() {
        let entry = make_entry("1", BlacklistPattern::Wildcard("".to_string()));
        assert!(entry.matches(""));
        assert!(!entry.matches("something"));
    }

    #[test]
    fn test_wildcard_star_matches_all() {
        let entry = make_entry("1", BlacklistPattern::Wildcard("*".to_string()));
        assert!(entry.matches("anything"));
        assert!(entry.matches("http://example.com/file.txt"));
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![BlacklistEntry::new(
                "test-id".to_string(),
                "Test Entry".to_string(),
                BlacklistPattern::Domain("example.com".to_string()),
                Some("Test reason".to_string()),
            )],
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: BlacklistConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.entries.len(), 1);
        assert_eq!(deserialized.entries[0].id, "test-id");
        assert_eq!(deserialized.entries[0].name, "Test Entry");
    }

    #[test]
    fn test_check_result_serialization() {
        let result = BlacklistCheckResult {
            blocked: true,
            matched_entry_id: Some("1".to_string()),
            matched_entry_name: Some("Test".to_string()),
            reason: Some("Blocked".to_string()),
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: BlacklistCheckResult = serde_json::from_str(&json).unwrap();
        assert!(deserialized.blocked);
        assert_eq!(deserialized.matched_entry_id.as_deref(), Some("1"));
    }
}
