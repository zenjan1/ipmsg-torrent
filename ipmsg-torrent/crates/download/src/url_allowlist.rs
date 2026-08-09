//! URL Allowlist for restricting downloads to trusted sources only
//!
//! Provides domain-level and pattern-level URL allowlisting to enforce
//! that only URLs from approved sources can be downloaded.
//!
//! Features:
//! - Domain-based allowing (e.g., allow all of "trusted.example.com")
//! - Exact URL matching
//! - Wildcard pattern matching (supports * and ?)
//! - Regular expression matching
//! - Persistent allowlist configuration
//! - DownloadManager integration
//!
//! When the allowlist is enabled, only URLs matching at least one entry
//! are permitted. All other URLs are rejected.

use serde::{Deserialize, Serialize};
use std::path::Path;
use url::Url;

/// A single allowlist entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowlistEntry {
    /// Unique entry ID
    pub id: String,
    /// Human-readable name/description
    pub name: String,
    /// Match pattern type
    pub pattern: AllowlistPattern,
    /// Whether this entry is enabled
    pub enabled: bool,
    /// Optional reason/note for allowing
    pub reason: Option<String>,
    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Pattern types for URL matching
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum AllowlistPattern {
    /// Allow all URLs from this domain (and subdomains)
    Domain(String),
    /// Allow exact URL match
    Exact(String),
    /// Allow URLs matching wildcard pattern (* and ?)
    Wildcard(String),
    /// Allow URLs matching regular expression
    Regex(String),
}

/// Result of checking a URL against the allowlist
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowlistCheckResult {
    /// Whether the URL is allowed
    pub allowed: bool,
    /// ID of the matching entry (if allowed)
    pub matched_entry_id: Option<String>,
    /// Name of the matching entry (if allowed)
    pub matched_entry_name: Option<String>,
    /// Reason/note for the matching entry (if allowed)
    pub reason: Option<String>,
}

/// Allowlist configuration (persisted)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowlistConfig {
    /// Whether allowlist enforcement is enabled.
    /// When true, only URLs matching an entry are permitted.
    pub enabled: bool,
    /// List of allowlist entries
    pub entries: Vec<AllowlistEntry>,
}

impl Default for AllowlistConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            entries: Vec::new(),
        }
    }
}

/// Persistence error
#[derive(Debug, thiserror::Error)]
pub enum AllowlistError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Entry not found: {0}")]
    NotFound(String),
    #[error("Invalid regex pattern: {0}")]
    InvalidRegex(String),
}

impl AllowlistEntry {
    /// Create a new allowlist entry
    pub fn new(
        id: String,
        name: String,
        pattern: AllowlistPattern,
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
            AllowlistPattern::Domain(domain) => domain_matches(url, domain),
            AllowlistPattern::Exact(exact) => url == exact,
            AllowlistPattern::Wildcard(pattern) => wildcard_matches(pattern, url),
            AllowlistPattern::Regex(regex_str) => regex_lite::Regex::new(regex_str)
                .map(|re| re.is_match(url))
                .unwrap_or(false),
        }
    }
}

/// Check if a URL's domain matches the allowed domain (including subdomains)
fn domain_matches(url: &str, allowed_domain: &str) -> bool {
    let allowed_lower = allowed_domain.to_lowercase();
    match Url::parse(url) {
        Ok(parsed) => {
            if let Some(host) = parsed.host_str() {
                let host_lower = host.to_lowercase();
                host_lower == allowed_lower || host_lower.ends_with(&format!(".{}", allowed_lower))
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

/// Check a URL against the allowlist config.
///
/// Returns `allowed: true` if:
/// - The allowlist is disabled (no enforcement), OR
/// - The URL matches at least one enabled entry
///
/// Returns `allowed: false` if:
/// - The allowlist is enabled AND
/// - The URL does not match any enabled entry
pub fn check_url_allowlist(url: &str, config: &AllowlistConfig) -> AllowlistCheckResult {
    if !config.enabled {
        return AllowlistCheckResult {
            allowed: true,
            matched_entry_id: None,
            matched_entry_name: None,
            reason: None,
        };
    }

    for entry in &config.entries {
        if entry.matches(url) {
            return AllowlistCheckResult {
                allowed: true,
                matched_entry_id: Some(entry.id.clone()),
                matched_entry_name: Some(entry.name.clone()),
                reason: entry.reason.clone(),
            };
        }
    }

    AllowlistCheckResult {
        allowed: false,
        matched_entry_id: None,
        matched_entry_name: None,
        reason: None,
    }
}

/// Save allowlist config to disk (atomic write)
pub fn save_allowlist_config(
    config: &AllowlistConfig,
    data_dir: &Path,
) -> Result<(), AllowlistError> {
    let path = data_dir.join("url_allowlist.json");
    let json = serde_json::to_string_pretty(config)?;
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, &json)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load allowlist config from disk
pub fn load_allowlist_config(data_dir: &Path) -> Option<AllowlistConfig> {
    let path = data_dir.join("url_allowlist.json");
    let content = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_entry(id: &str, pattern: AllowlistPattern) -> AllowlistEntry {
        AllowlistEntry::new(id.to_string(), format!("Entry {}", id), pattern, None)
    }

    fn make_entry_with_reason(id: &str, pattern: AllowlistPattern, reason: &str) -> AllowlistEntry {
        AllowlistEntry::new(
            id.to_string(),
            format!("Entry {}", id),
            pattern,
            Some(reason.to_string()),
        )
    }

    #[test]
    fn test_domain_match_exact() {
        let entry = make_entry(
            "1",
            AllowlistPattern::Domain("trusted.example.com".to_string()),
        );
        assert!(entry.matches("http://trusted.example.com/file.txt"));
        assert!(entry.matches("https://trusted.example.com/path/to/file"));
    }

    #[test]
    fn test_domain_match_subdomain() {
        let entry = make_entry("1", AllowlistPattern::Domain("example.com".to_string()));
        assert!(entry.matches("http://sub.example.com/file.txt"));
        assert!(entry.matches("https://deep.sub.example.com/file.txt"));
    }

    #[test]
    fn test_domain_no_match() {
        let entry = make_entry("1", AllowlistPattern::Domain("trusted.com".to_string()));
        assert!(!entry.matches("http://example.com/file.txt"));
        assert!(!entry.matches("http://untrusted.com/file.txt"));
    }

    #[test]
    fn test_domain_case_insensitive() {
        let entry = make_entry("1", AllowlistPattern::Domain("Example.COM".to_string()));
        assert!(entry.matches("http://example.com/file.txt"));
        assert!(entry.matches("http://EXAMPLE.COM/file.txt"));
    }

    #[test]
    fn test_exact_match() {
        let entry = make_entry(
            "1",
            AllowlistPattern::Exact("http://example.com/file.txt".to_string()),
        );
        assert!(entry.matches("http://example.com/file.txt"));
        assert!(!entry.matches("http://example.com/other.txt"));
    }

    #[test]
    fn test_wildcard_match_star() {
        let entry = make_entry(
            "1",
            AllowlistPattern::Wildcard("http://example.com/*.zip".to_string()),
        );
        assert!(entry.matches("http://example.com/archive.zip"));
        assert!(entry.matches("http://example.com/path/archive.zip"));
        assert!(!entry.matches("http://example.com/file.exe"));
    }

    #[test]
    fn test_wildcard_match_question() {
        let entry = make_entry(
            "1",
            AllowlistPattern::Wildcard("http://example.com/file?.txt".to_string()),
        );
        assert!(entry.matches("http://example.com/file1.txt"));
        assert!(entry.matches("http://example.com/fileA.txt"));
        assert!(!entry.matches("http://example.com/file10.txt"));
    }

    #[test]
    fn test_regex_match() {
        let entry = make_entry(
            "1",
            AllowlistPattern::Regex(r"http://example\.com/\d+\.zip".to_string()),
        );
        assert!(entry.matches("http://example.com/123.zip"));
        assert!(entry.matches("http://example.com/42.zip"));
        assert!(!entry.matches("http://example.com/abc.zip"));
    }

    #[test]
    fn test_regex_invalid() {
        let entry = make_entry("1", AllowlistPattern::Regex("[invalid".to_string()));
        assert!(!entry.matches("anything"));
    }

    #[test]
    fn test_disabled_entry() {
        let mut entry = make_entry("1", AllowlistPattern::Domain("trusted.com".to_string()));
        entry.enabled = false;
        assert!(!entry.matches("http://trusted.com/file.txt"));
    }

    #[test]
    fn test_check_allowlist_disabled() {
        // When allowlist is disabled, all URLs are allowed
        let config = AllowlistConfig {
            enabled: false,
            entries: vec![make_entry(
                "1",
                AllowlistPattern::Domain("trusted.com".to_string()),
            )],
        };
        let result = check_url_allowlist("http://anything.com/file.txt", &config);
        assert!(result.allowed);
        assert!(result.matched_entry_id.is_none());
    }

    #[test]
    fn test_check_allowlist_enabled_match() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry_with_reason(
                "1",
                AllowlistPattern::Domain("trusted.com".to_string()),
                "Official mirror",
            )],
        };
        let result = check_url_allowlist("http://trusted.com/file.txt", &config);
        assert!(result.allowed);
        assert_eq!(result.matched_entry_id.as_deref(), Some("1"));
        assert_eq!(result.matched_entry_name.as_deref(), Some("Entry 1"));
        assert_eq!(result.reason.as_deref(), Some("Official mirror"));
    }

    #[test]
    fn test_check_allowlist_enabled_no_match() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry(
                "1",
                AllowlistPattern::Domain("trusted.com".to_string()),
            )],
        };
        let result = check_url_allowlist("http://untrusted.com/file.txt", &config);
        assert!(!result.allowed);
        assert!(result.matched_entry_id.is_none());
        assert!(result.matched_entry_name.is_none());
        assert!(result.reason.is_none());
    }

    #[test]
    fn test_check_allowlist_enabled_empty_entries() {
        // Enabled but no entries: nothing is allowed
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![],
        };
        let result = check_url_allowlist("http://anything.com/file.txt", &config);
        assert!(!result.allowed);
    }

    #[test]
    fn test_check_allowlist_first_match() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![
                make_entry_with_reason(
                    "1",
                    AllowlistPattern::Domain("example.com".to_string()),
                    "First",
                ),
                make_entry_with_reason(
                    "2",
                    AllowlistPattern::Wildcard("http://example.com/*.zip".to_string()),
                    "Second",
                ),
            ],
        };
        let result = check_url_allowlist("http://example.com/file.zip", &config);
        assert!(result.allowed);
        assert_eq!(result.matched_entry_id.as_deref(), Some("1"));
        assert_eq!(result.reason.as_deref(), Some("First"));
    }

    #[test]
    fn test_check_allowlist_multiple_entries_any_match() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![
                make_entry("1", AllowlistPattern::Domain("a.com".to_string())),
                make_entry("2", AllowlistPattern::Domain("b.com".to_string())),
                make_entry("3", AllowlistPattern::Domain("c.com".to_string())),
            ],
        };
        assert!(check_url_allowlist("http://a.com/f.txt", &config).allowed);
        assert!(check_url_allowlist("http://b.com/f.txt", &config).allowed);
        assert!(check_url_allowlist("http://c.com/f.txt", &config).allowed);
        assert!(!check_url_allowlist("http://d.com/f.txt", &config).allowed);
    }

    #[test]
    fn test_save_and_load_config() {
        let temp_dir = TempDir::new().unwrap();
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![
                make_entry("1", AllowlistPattern::Domain("trusted.com".to_string())),
                make_entry("2", AllowlistPattern::Wildcard("*.zip".to_string())),
            ],
        };

        save_allowlist_config(&config, temp_dir.path()).unwrap();
        let loaded = load_allowlist_config(temp_dir.path()).unwrap();

        assert!(loaded.enabled);
        assert_eq!(loaded.entries.len(), 2);
        assert_eq!(loaded.entries[0].id, "1");
        assert_eq!(loaded.entries[1].id, "2");
    }

    #[test]
    fn test_load_missing_config() {
        let temp_dir = TempDir::new().unwrap();
        let loaded = load_allowlist_config(temp_dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_creates_file() {
        let temp_dir = TempDir::new().unwrap();
        let config = AllowlistConfig::default();
        save_allowlist_config(&config, temp_dir.path()).unwrap();
        assert!(temp_dir.path().join("url_allowlist.json").exists());
    }

    #[test]
    fn test_domain_no_match_invalid_url() {
        let entry = make_entry("1", AllowlistPattern::Domain("example.com".to_string()));
        assert!(!entry.matches("not-a-url"));
        assert!(!entry.matches(""));
    }

    #[test]
    fn test_wildcard_empty_pattern() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("".to_string()));
        assert!(entry.matches(""));
        assert!(!entry.matches("something"));
    }

    #[test]
    fn test_wildcard_star_matches_all() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("*".to_string()));
        assert!(entry.matches("anything"));
        assert!(entry.matches("http://example.com/file.txt"));
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![AllowlistEntry::new(
                "test-id".to_string(),
                "Test Entry".to_string(),
                AllowlistPattern::Domain("example.com".to_string()),
                Some("Test reason".to_string()),
            )],
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AllowlistConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.entries.len(), 1);
        assert_eq!(deserialized.entries[0].id, "test-id");
        assert_eq!(deserialized.entries[0].name, "Test Entry");
    }

    #[test]
    fn test_check_result_serialization() {
        let result = AllowlistCheckResult {
            allowed: true,
            matched_entry_id: Some("1".to_string()),
            matched_entry_name: Some("Test".to_string()),
            reason: Some("Allowed".to_string()),
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: AllowlistCheckResult = serde_json::from_str(&json).unwrap();
        assert!(deserialized.allowed);
        assert_eq!(deserialized.matched_entry_id.as_deref(), Some("1"));
    }

    #[test]
    fn test_default_config_disabled() {
        let config = AllowlistConfig::default();
        assert!(!config.enabled);
        assert!(config.entries.is_empty());
    }

    #[test]
    fn test_entry_new_defaults_enabled() {
        let entry = AllowlistEntry::new(
            "id".to_string(),
            "name".to_string(),
            AllowlistPattern::Exact("http://x.com".to_string()),
            None,
        );
        assert!(entry.enabled);
        assert!(entry.reason.is_none());
    }
}
