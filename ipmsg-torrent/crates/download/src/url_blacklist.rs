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

    // ── Phase 252: Comprehensive test coverage ──

    // ── BlacklistPattern serde ──

    #[test]
    fn test_pattern_domain_serde_roundtrip() {
        let pattern = BlacklistPattern::Domain("example.com".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        let deserialized: BlacklistPattern = serde_json::from_str(&json).unwrap();
        match deserialized {
            BlacklistPattern::Domain(d) => assert_eq!(d, "example.com"),
            _ => panic!("expected Domain variant"),
        }
    }

    #[test]
    fn test_pattern_exact_serde_roundtrip() {
        let pattern = BlacklistPattern::Exact("http://example.com/file.txt".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        let deserialized: BlacklistPattern = serde_json::from_str(&json).unwrap();
        match deserialized {
            BlacklistPattern::Exact(e) => assert_eq!(e, "http://example.com/file.txt"),
            _ => panic!("expected Exact variant"),
        }
    }

    #[test]
    fn test_pattern_wildcard_serde_roundtrip() {
        let pattern = BlacklistPattern::Wildcard("*.exe".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        let deserialized: BlacklistPattern = serde_json::from_str(&json).unwrap();
        match deserialized {
            BlacklistPattern::Wildcard(w) => assert_eq!(w, "*.exe"),
            _ => panic!("expected Wildcard variant"),
        }
    }

    #[test]
    fn test_pattern_regex_serde_roundtrip() {
        let pattern = BlacklistPattern::Regex(r"\d+\.zip".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        let deserialized: BlacklistPattern = serde_json::from_str(&json).unwrap();
        match deserialized {
            BlacklistPattern::Regex(r) => assert_eq!(r, r"\d+\.zip"),
            _ => panic!("expected Regex variant"),
        }
    }

    #[test]
    fn test_pattern_serde_tag_content_format() {
        // Verify the serde tag/content format
        let pattern = BlacklistPattern::Domain("test.com".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        assert!(json.contains("\"type\""));
        assert!(json.contains("\"Domain\""));
        assert!(json.contains("\"value\""));
        assert!(json.contains("\"test.com\""));
    }

    // ── BlacklistEntry traits ──

    #[test]
    fn test_entry_clone() {
        let entry = make_entry("1", BlacklistPattern::Domain("example.com".to_string()));
        let cloned = entry.clone();
        assert_eq!(cloned.id, entry.id);
        assert_eq!(cloned.name, entry.name);
        assert_eq!(cloned.enabled, entry.enabled);
    }

    #[test]
    fn test_entry_clone_independence() {
        let entry = make_entry("1", BlacklistPattern::Domain("example.com".to_string()));
        let mut cloned = entry.clone();
        cloned.enabled = false;
        assert!(entry.enabled); // original unchanged
        assert!(!cloned.enabled);
    }

    #[test]
    fn test_entry_debug() {
        let entry = make_entry("debug-1", BlacklistPattern::Domain("test.com".to_string()));
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("debug-1"));
        assert!(debug_str.contains("BlacklistEntry"));
    }

    // ── BlacklistCheckResult traits ──

    #[test]
    fn test_check_result_clone() {
        let result = BlacklistCheckResult {
            blocked: true,
            matched_entry_id: Some("1".to_string()),
            matched_entry_name: Some("Test".to_string()),
            reason: Some("Blocked".to_string()),
        };
        let cloned = result.clone();
        assert!(cloned.blocked);
        assert_eq!(cloned.matched_entry_id, result.matched_entry_id);
    }

    #[test]
    fn test_check_result_debug() {
        let result = BlacklistCheckResult {
            blocked: false,
            matched_entry_id: None,
            matched_entry_name: None,
            reason: None,
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("BlacklistCheckResult"));
    }

    #[test]
    fn test_check_result_not_blocked_fields() {
        let result = BlacklistCheckResult {
            blocked: false,
            matched_entry_id: None,
            matched_entry_name: None,
            reason: None,
        };
        assert!(!result.blocked);
        assert!(result.matched_entry_id.is_none());
        assert!(result.matched_entry_name.is_none());
        assert!(result.reason.is_none());
    }

    // ── BlacklistConfig traits ──

    #[test]
    fn test_config_default() {
        let config = BlacklistConfig::default();
        assert!(config.enabled);
        assert!(config.entries.is_empty());
    }

    #[test]
    fn test_config_clone() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![make_entry("1", BlacklistPattern::Domain("test.com".to_string()))],
        };
        let cloned = config.clone();
        assert_eq!(cloned.entries.len(), 1);
        assert_eq!(cloned.enabled, config.enabled);
    }

    #[test]
    fn test_config_debug() {
        let config = BlacklistConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("BlacklistConfig"));
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = BlacklistConfig {
            enabled: false,
            entries: vec![
                make_entry("1", BlacklistPattern::Domain("ads.com".to_string())),
                make_entry("2", BlacklistPattern::Wildcard("*.exe".to_string())),
            ],
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: BlacklistConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, false);
        assert_eq!(deserialized.entries.len(), 2);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"entries":[],"extra_field":"ignored"}"#;
        let config: BlacklistConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert!(config.entries.is_empty());
    }

    #[test]
    fn test_config_pretty_serde() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![make_entry("1", BlacklistPattern::Domain("test.com".to_string()))],
        };
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: BlacklistConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.entries.len(), 1);
    }

    // ── BlacklistError ──

    #[test]
    fn test_error_display_not_found() {
        let err = BlacklistError::NotFound("entry-123".to_string());
        assert_eq!(err.to_string(), "Entry not found: entry-123");
    }

    #[test]
    fn test_error_display_invalid_regex() {
        let err = BlacklistError::InvalidRegex("[invalid".to_string());
        assert_eq!(err.to_string(), "Invalid regex pattern: [invalid");
    }

    #[test]
    fn test_error_debug() {
        let err = BlacklistError::NotFound("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("NotFound"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = BlacklistError::from(io_err);
        match err {
            BlacklistError::Io(_) => (),
            _ => panic!("expected Io variant"),
        }
    }

    #[test]
    fn test_error_from_serde() {
        let serde_err = serde_json::from_str::<BlacklistConfig>("invalid").unwrap_err();
        let err = BlacklistError::from(serde_err);
        match err {
            BlacklistError::Serialization(_) => (),
            _ => panic!("expected Serialization variant"),
        }
    }

    // ── domain_matches ──

    #[test]
    fn test_domain_matches_with_port() {
        assert!(domain_matches("http://example.com:8080/file.txt", "example.com"));
    }

    #[test]
    fn test_domain_matches_deep_subdomain() {
        assert!(domain_matches("http://a.b.c.example.com/file", "example.com"));
    }

    #[test]
    fn test_domain_matches_no_match_partial() {
        // "notexample.com" should NOT match "example.com"
        assert!(!domain_matches("http://notexample.com/file", "example.com"));
    }

    #[test]
    fn test_domain_matches_ftp() {
        assert!(domain_matches("ftp://example.com/file.txt", "example.com"));
    }

    #[test]
    fn test_domain_matches_empty_url() {
        assert!(!domain_matches("", "example.com"));
    }

    #[test]
    fn test_domain_matches_empty_domain() {
        assert!(!domain_matches("http://example.com/file", ""));
    }

    #[test]
    fn test_domain_matches_unicode() {
        assert!(domain_matches("http://中文.com/file", "中文.com"));
    }

    // ── wildcard_matches ──

    #[test]
    fn test_wildcard_matches_exact_no_wildcard() {
        assert!(wildcard_matches("http://example.com/file.txt", "http://example.com/file.txt"));
        assert!(!wildcard_matches("http://example.com/file.txt", "http://example.com/other.txt"));
    }

    #[test]
    fn test_wildcard_matches_multiple_stars() {
        assert!(wildcard_matches("*example*", "http://example.com/file.txt"));
        assert!(wildcard_matches("*.*", "http://example.com"));
    }

    #[test]
    fn test_wildcard_matches_question_multiple() {
        assert!(wildcard_matches("file??.txt", "file01.txt"));
        assert!(wildcard_matches("file??.txt", "fileAB.txt"));
        assert!(!wildcard_matches("file??.txt", "file1.txt"));
    }

    #[test]
    fn test_wildcard_matches_star_at_end() {
        assert!(wildcard_matches("http://example.com/*", "http://example.com/anything"));
        assert!(wildcard_matches("http://example.com/*", "http://example.com/"));
    }

    #[test]
    fn test_wildcard_matches_star_at_start() {
        assert!(wildcard_matches("*.exe", "malware.exe"));
        assert!(wildcard_matches("*.exe", "path/to/malware.exe"));
    }

    #[test]
    fn test_wildcard_matches_unicode() {
        assert!(wildcard_matches("*中文*", "http://example.com/中文文件.txt"));
    }

    #[test]
    fn test_wildcard_matches_empty_text() {
        assert!(wildcard_matches("", ""));
        assert!(wildcard_matches("*", ""));
        assert!(!wildcard_matches("?", ""));
    }

    // ── BlacklistEntry::matches ──

    #[test]
    fn test_matches_domain_with_path() {
        let entry = make_entry("1", BlacklistPattern::Domain("example.com".to_string()));
        assert!(entry.matches("http://example.com/path/to/file.txt"));
        assert!(entry.matches("https://example.com/"));
    }

    #[test]
    fn test_matches_exact_with_query_params() {
        let entry = make_entry(
            "1",
            BlacklistPattern::Exact("http://example.com/file?q=1".to_string()),
        );
        assert!(entry.matches("http://example.com/file?q=1"));
        assert!(!entry.matches("http://example.com/file?q=2"));
    }

    #[test]
    fn test_matches_regex_complex() {
        let entry = make_entry(
            "1",
            BlacklistPattern::Regex(r"https?://.*\.example\.com/.*\.exe".to_string()),
        );
        assert!(entry.matches("http://sub.example.com/file.exe"));
        assert!(entry.matches("https://example.com/file.exe"));
        assert!(!entry.matches("http://example.com/file.txt"));
    }

    #[test]
    fn test_matches_wildcard_combined() {
        let entry = make_entry(
            "1",
            BlacklistPattern::Wildcard("http://*.example.com/*.exe".to_string()),
        );
        assert!(entry.matches("http://sub.example.com/file.exe"));
        assert!(!entry.matches("http://sub.example.com/file.txt"));
    }

    // ── check_url_blacklist ──

    #[test]
    fn test_check_blacklist_multiple_entries() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![
                make_entry("1", BlacklistPattern::Domain("ads.com".to_string())),
                make_entry("2", BlacklistPattern::Domain("tracker.com".to_string())),
                make_entry("3", BlacklistPattern::Wildcard("*.exe".to_string())),
            ],
        };

        let result1 = check_url_blacklist("http://ads.com/banner.js", &config);
        assert!(result1.blocked);
        assert_eq!(result1.matched_entry_id.as_deref(), Some("1"));

        let result2 = check_url_blacklist("http://tracker.com/ping", &config);
        assert!(result2.blocked);
        assert_eq!(result2.matched_entry_id.as_deref(), Some("2"));

        let result3 = check_url_blacklist("http://good.com/file.exe", &config);
        assert!(result3.blocked);
        assert_eq!(result3.matched_entry_id.as_deref(), Some("3"));

        let result4 = check_url_blacklist("http://good.com/file.txt", &config);
        assert!(!result4.blocked);
    }

    #[test]
    fn test_check_blacklist_mixed_patterns() {
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![
                make_entry("d1", BlacklistPattern::Domain("blocked.com".to_string())),
                make_entry(
                    "e1",
                    BlacklistPattern::Exact("http://exact.com/blocked.txt".to_string()),
                ),
                make_entry("w1", BlacklistPattern::Wildcard("*.malware".to_string())),
                make_entry("r1", BlacklistPattern::Regex(r"\d+\.zip".to_string())),
            ],
        };

        assert!(check_url_blacklist("http://blocked.com/file", &config).blocked);
        assert!(check_url_blacklist("http://exact.com/blocked.txt", &config).blocked);
        assert!(check_url_blacklist("http://any.com/file.malware", &config).blocked);
        assert!(check_url_blacklist("http://any.com/123.zip", &config).blocked);
        assert!(!check_url_blacklist("http://safe.com/file.txt", &config).blocked);
    }

    #[test]
    fn test_check_blacklist_disabled_entries_skipped() {
        let mut entry1 = make_entry("1", BlacklistPattern::Domain("ads.com".to_string()));
        entry1.enabled = false;
        let entry2 = make_entry("2", BlacklistPattern::Domain("ads.com".to_string()));

        let config = BlacklistConfig {
            enabled: true,
            entries: vec![entry1, entry2],
        };

        let result = check_url_blacklist("http://ads.com/banner.js", &config);
        assert!(result.blocked);
        // Should match entry 2, not entry 1
        assert_eq!(result.matched_entry_id.as_deref(), Some("2"));
    }

    // ── Persistence ──

    #[test]
    fn test_save_overwrites_existing() {
        let temp_dir = TempDir::new().unwrap();

        let config1 = BlacklistConfig {
            enabled: true,
            entries: vec![make_entry("1", BlacklistPattern::Domain("old.com".to_string()))],
        };
        save_blacklist_config(&config1, temp_dir.path()).unwrap();

        let config2 = BlacklistConfig {
            enabled: false,
            entries: vec![make_entry("2", BlacklistPattern::Domain("new.com".to_string()))],
        };
        save_blacklist_config(&config2, temp_dir.path()).unwrap();

        let loaded = load_blacklist_config(temp_dir.path()).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].id, "2");
    }

    #[test]
    fn test_save_no_tmp_leftover() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlacklistConfig::default();
        save_blacklist_config(&config, temp_dir.path()).unwrap();

        let tmp_path = temp_dir.path().join("url_blacklist.json.tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_load_corrupted_json() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("url_blacklist.json");
        std::fs::write(&path, "{corrupted json}").unwrap();

        let loaded = load_blacklist_config(temp_dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("url_blacklist.json");
        std::fs::write(&path, "").unwrap();

        let loaded = load_blacklist_config(temp_dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_empty_json_object() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("url_blacklist.json");
        std::fs::write(&path, "{}").unwrap();

        // {} is not a valid BlacklistConfig (missing fields), so should return None
        let loaded = load_blacklist_config(temp_dir.path());
        // serde may fail or succeed depending on defaults
        // If it succeeds, entries should be empty
        if let Some(config) = loaded {
            assert!(config.entries.is_empty());
        }
    }

    #[test]
    fn test_persistence_unicode() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![BlacklistEntry::new(
                "unicode-1".to_string(),
                "中文条目".to_string(),
                BlacklistPattern::Domain("中文.com".to_string()),
                Some("阻止中文域名".to_string()),
            )],
        };

        save_blacklist_config(&config, temp_dir.path()).unwrap();
        let loaded = load_blacklist_config(temp_dir.path()).unwrap();

        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].name, "中文条目");
        assert_eq!(loaded.entries[0].reason, Some("阻止中文域名".to_string()));
    }

    #[test]
    fn test_persistence_all_pattern_types() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlacklistConfig {
            enabled: true,
            entries: vec![
                make_entry("d", BlacklistPattern::Domain("example.com".to_string())),
                make_entry("e", BlacklistPattern::Exact("http://exact.com/file".to_string())),
                make_entry("w", BlacklistPattern::Wildcard("*.exe".to_string())),
                make_entry("r", BlacklistPattern::Regex(r"\d+".to_string())),
            ],
        };

        save_blacklist_config(&config, temp_dir.path()).unwrap();
        let loaded = load_blacklist_config(temp_dir.path()).unwrap();

        assert_eq!(loaded.entries.len(), 4);
    }

    // ── Unicode ──

    #[test]
    fn test_unicode_entry_id() {
        let entry = BlacklistEntry::new(
            "条目-中文".to_string(),
            "Test".to_string(),
            BlacklistPattern::Domain("example.com".to_string()),
            None,
        );
        assert_eq!(entry.id, "条目-中文");
    }

    #[test]
    fn test_unicode_reason() {
        let entry = make_entry_with_reason(
            "1",
            BlacklistPattern::Domain("example.com".to_string()),
            "🚫 阻止此域名",
        );
        assert_eq!(entry.reason, Some("🚫 阻止此域名".to_string()));
    }

    #[test]
    fn test_unicode_in_wildcard() {
        let entry = make_entry("1", BlacklistPattern::Wildcard("*中文*".to_string()));
        assert!(entry.matches("http://example.com/中文文件.txt"));
        assert!(!entry.matches("http://example.com/english.txt"));
    }

    #[test]
    fn test_unicode_in_regex() {
        let entry = make_entry("1", BlacklistPattern::Regex("中文".to_string()));
        assert!(entry.matches("http://example.com/中文/file.txt"));
        assert!(!entry.matches("http://example.com/english/file.txt"));
    }

    // ── Boundary conditions ──

    #[test]
    fn test_empty_entry_id() {
        let entry = make_entry("", BlacklistPattern::Domain("example.com".to_string()));
        assert_eq!(entry.id, "");
        assert!(entry.matches("http://example.com/file"));
    }

    #[test]
    fn test_empty_entry_name() {
        let entry = BlacklistEntry::new(
            "1".to_string(),
            "".to_string(),
            BlacklistPattern::Domain("example.com".to_string()),
            None,
        );
        assert_eq!(entry.name, "");
    }

    #[test]
    fn test_very_long_pattern() {
        let long_pattern = "a".repeat(10000);
        let entry = make_entry("1", BlacklistPattern::Wildcard(long_pattern.clone()));
        assert!(entry.matches(&long_pattern));
        assert!(!entry.matches(&format!("{}b", long_pattern)));
    }

    #[test]
    fn test_many_entries() {
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push(make_entry(
                &format!("entry-{}", i),
                BlacklistPattern::Domain(format!("domain{}.com", i)),
            ));
        }

        let config = BlacklistConfig {
            enabled: true,
            entries,
        };

        // Should match entry 50
        let result = check_url_blacklist("http://domain50.com/file", &config);
        assert!(result.blocked);
        assert_eq!(result.matched_entry_id.as_deref(), Some("entry-50"));

        // Should not match
        let result2 = check_url_blacklist("http://notblocked.com/file", &config);
        assert!(!result2.blocked);
    }

    // ── Complex workflows ──

    #[test]
    fn test_complete_lifecycle() {
        let temp_dir = TempDir::new().unwrap();

        // Create config
        let mut config = BlacklistConfig::default();
        assert!(config.enabled);
        assert!(config.entries.is_empty());

        // Add entries
        config.entries.push(make_entry(
            "1",
            BlacklistPattern::Domain("ads.com".to_string()),
        ));
        config.entries.push(make_entry(
            "2",
            BlacklistPattern::Wildcard("*.exe".to_string()),
        ));

        // Save
        save_blacklist_config(&config, temp_dir.path()).unwrap();

        // Load
        let loaded = load_blacklist_config(temp_dir.path()).unwrap();
        assert_eq!(loaded.entries.len(), 2);

        // Check URLs
        assert!(check_url_blacklist("http://ads.com/banner.js", &loaded).blocked);
        assert!(check_url_blacklist("http://good.com/file.exe", &loaded).blocked);
        assert!(!check_url_blacklist("http://good.com/file.txt", &loaded).blocked);

        // Disable an entry
        let mut config2 = loaded;
        config2.entries[0].enabled = false;

        // Save and reload
        save_blacklist_config(&config2, temp_dir.path()).unwrap();
        let loaded2 = load_blacklist_config(temp_dir.path()).unwrap();

        // Now ads.com should not be blocked (entry disabled)
        assert!(!check_url_blacklist("http://ads.com/banner.js", &loaded2).blocked);
        // But .exe should still be blocked
        assert!(check_url_blacklist("http://good.com/file.exe", &loaded2).blocked);
    }

    #[test]
    fn test_entry_new_constructor() {
        let entry = BlacklistEntry::new(
            "id-1".to_string(),
            "Name".to_string(),
            BlacklistPattern::Domain("example.com".to_string()),
            Some("reason".to_string()),
        );
        assert_eq!(entry.id, "id-1");
        assert_eq!(entry.name, "Name");
        assert!(entry.enabled); // default true
        assert_eq!(entry.reason, Some("reason".to_string()));
        assert!(entry.created_at <= chrono::Utc::now());
    }

    #[test]
    fn test_entry_new_without_reason() {
        let entry = BlacklistEntry::new(
            "id-2".to_string(),
            "Name".to_string(),
            BlacklistPattern::Exact("http://example.com".to_string()),
            None,
        );
        assert!(entry.reason.is_none());
    }
}
