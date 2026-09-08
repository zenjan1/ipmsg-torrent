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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AllowlistConfig {
    /// Whether allowlist enforcement is enabled.
    /// When true, only URLs matching an entry are permitted.
    pub enabled: bool,
    /// List of allowlist entries
    pub entries: Vec<AllowlistEntry>,
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
            "id"to_string(),
            "name".to_string(),
            AllowlistPattern::Exact("http://x.com".to_string()),
            None,
        );
        assert!(entry.enabled);
        assert!(entry.reason.is_none());
    }

    // ===== AllowlistPattern serde =====

    #[test]
    fn test_pattern_serde_domain() {
        let p = AllowlistPattern::Domain("example.com".to_string());
        let json = serde_json::to_string(&p).unwrap();
        let back: AllowlistPattern = serde_json::from_str(&json).unwrap();
        match back {
            AllowlistPattern::Domain(d) => assert_eq!(d, "example.com"),
            _ => panic!("expected Domain"),
        }
    }

    #[test]
    fn test_pattern_serde_exact() {
        let p = AllowlistPattern::Exact("http://x.com/f.txt".to_string());
        let json = serde_json::to_string(&p).unwrap();
        let back: AllowlistPattern = serde_json::from_str(&json).unwrap();
        match back {
            AllowlistPattern::Exact(e) => assert_eq!(e, "http://x.com/f.txt"),
            _ => panic!("expected Exact"),
        }
    }

    #[test]
    fn test_pattern_serde_wildcard() {
        let p = AllowlistPattern::Wildcard("*.zip".to_string());
        let json = serde_json::to_string(&p).unwrap();
        let back: AllowlistPattern = serde_json::from_str(&json).unwrap();
        match back {
            AllowlistPattern::Wildcard(w) => assert_eq!(w, "*.zip"),
            _ => panic!("expected Wildcard"),
        }
    }

    #[test]
    fn test_pattern_serde_regex() {
        let p = AllowlistPattern::Regex(r"\d+".to_string());
        let json = serde_json::to_string(&p).unwrap();
        let back: AllowlistPattern = serde_json::from_str(&json).unwrap();
        match back {
            AllowlistPattern::Regex(r) => assert_eq!(r, r"\d+"),
            _ => panic!("expected Regex"),
        }
    }

    #[test]
    fn test_pattern_serde_tagged_format() {
        let p = AllowlistPattern::Domain("test.com".to_string());
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"type\""));
        assert!(json.contains("\"value\""));
        assert!(json.contains("Domain"));
    }

    #[test]
    fn test_pattern_serde_all_variants_roundtrip() {
        let patterns = vec![
            AllowlistPattern::Domain("d.com".into()),
            AllowlistPattern::Exact("http://e.com".into()),
            AllowlistPattern::Wildcard("*.*".into()),
            AllowlistPattern::Regex("^a".into()),
        ];
        for p in patterns {
            let json = serde_json::to_string(&p).unwrap();
            let back: AllowlistPattern = serde_json::from_str(&json).unwrap();
            let json2 = serde_json::to_string(&back).unwrap();
            assert_eq!(json, json2);
        }
    }

    // ===== AllowlistPattern traits =====

    #[test]
    fn test_pattern_clone() {
        let p = AllowlistPattern::Domain("x.com".to_string());
        let c = p.clone();
        match c {
            AllowlistPattern::Domain(d) => assert_eq!(d, "x.com"),
            _ => panic!("clone failed"),
        }
    }

    #[test]
    fn test_pattern_debug() {
        let p = AllowlistPattern::Exact("http://x.com".to_string());
        let dbg = format!("{:?}", p);
        assert!(dbg.contains("Exact"));
        assert!(dbg.contains("http://x.com"));
    }

    #[test]
    fn test_pattern_debug_all_variants() {
        let patterns = vec![
            AllowlistPattern::Domain("d.com".into()),
            AllowlistPattern::Exact("http://e.com".into()),
            AllowlistPattern::Wildcard("*".into()),
            AllowlistPattern::Regex("^a".into()),
        ];
        for p in patterns {
            let dbg = format!("{:?}", p);
            assert!(!dbg.is_empty());
        }
    }

    // ===== AllowlistEntry traits =====

    #[test]
    fn test_entry_clone() {
        let entry = make_entry("1", AllowlistPattern::Domain("x.com".into()));
        let cloned = entry.clone();
        assert_eq!(cloned.id, entry.id);
        assert_eq!(cloned.name, entry.name);
    }

    #[test]
    fn test_entry_clone_independence() {
        let mut entry = make_entry("1", AllowlistPattern::Domain("x.com".into()));
        let mut cloned = entry.clone();
        cloned.enabled = false;
        assert!(entry.enabled);
        assert!(!cloned.enabled);
    }

    #[test]
    fn test_entry_debug() {
        let entry = make_entry("test-id", AllowlistPattern::Exact("http://x.com".into()));
        let dbg = format!("{:?}", entry);
        assert!(dbg.contains("test-id"));
        assert!(dbg.contains("Entry test-id"));
    }

    #[test]
    fn test_entry_debug_with_reason() {
        let entry = make_entry_with_reason(
            "1",
            AllowlistPattern::Domain("x.com".into()),
            "trusted source",
        );
        let dbg = format!("{:?}", entry);
        assert!(dbg.contains("trusted source"));
    }

    // ===== AllowlistCheckResult traits =====

    #[test]
    fn test_check_result_clone() {
        let result = AllowlistCheckResult {
            allowed: true,
            matched_entry_id: Some("1".into()),
            matched_entry_name: Some("Test".into()),
            reason: Some("OK".into()),
        };
        let cloned = result.clone();
        assert!(cloned.allowed);
        assert_eq!(cloned.matched_entry_id.as_deref(), Some("1"));
    }

    #[test]
    fn test_check_result_debug() {
        let result = AllowlistCheckResult {
            allowed: false,
            matched_entry_id: None,
            matched_entry_name: None,
            reason: None,
        };
        let dbg = format!("{:?}", result);
        assert!(dbg.contains("allowed"));
    }

    #[test]
    fn test_check_result_serde_allowed() {
        let result = AllowlistCheckResult {
            allowed: true,
            matched_entry_id: Some("id1".into()),
            matched_entry_name: Some("Name".into()),
            reason: Some("Trusted".into()),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: AllowlistCheckResult = serde_json::from_str(&json).unwrap();
        assert!(back.allowed);
        assert_eq!(back.matched_entry_id.as_deref(), Some("id1"));
        assert_eq!(back.matched_entry_name.as_deref(), Some("Name"));
        assert_eq!(back.reason.as_deref(), Some("Trusted"));
    }

    #[test]
    fn test_check_result_serde_denied() {
        let result = AllowlistCheckResult {
            allowed: false,
            matched_entry_id: None,
            matched_entry_name: None,
            reason: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: AllowlistCheckResult = serde_json::from_str(&json).unwrap();
        assert!(!back.allowed);
        assert!(back.matched_entry_id.is_none());
    }

    // ===== AllowlistConfig traits =====

    #[test]
    fn test_config_clone() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("x.com".into()))],
        };
        let cloned = config.clone();
        assert!(cloned.enabled);
        assert_eq!(cloned.entries.len(), 1);
    }

    #[test]
    fn test_config_clone_independence() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("x.com".into()))],
        };
        let mut cloned = config.clone();
        cloned.enabled = false;
        assert!(config.enabled);
        assert!(!cloned.enabled);
    }

    #[test]
    fn test_config_debug() {
        let config = AllowlistConfig {
            enabled: false,
            entries: vec![],
        };
        let dbg = format!("{:?}", config);
        assert!(dbg.contains("enabled"));
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{"enabled":true,"entries":[],"extra_field":"ignored"}"#;
        let config: AllowlistConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert!(config.entries.is_empty());
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("x.com".into()))],
        };
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        let back: AllowlistConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(back.enabled, config.enabled);
        assert_eq!(back.entries.len(), config.entries.len());
    }

    // ===== AllowlistError =====

    #[test]
    fn test_error_display_io() {
        let err = AllowlistError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "access denied",
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("IO error"));
        assert!(msg.contains("access denied"));
    }

    #[test]
    fn test_error_display_serialization() {
        let bad_json = "not json";
        let serde_err = serde_json::from_str::<AllowlistConfig>(bad_json).unwrap_err();
        let err = AllowlistError::Serialization(serde_err);
        let msg = format!("{}", err);
        assert!(msg.contains("Serialization error"));
    }

    #[test]
    fn test_error_display_not_found() {
        let err = AllowlistError::NotFound("entry-42".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Entry not found"));
        assert!(msg.contains("entry-42"));
    }

    #[test]
    fn test_error_display_invalid_regex() {
        let err = AllowlistError::InvalidRegex("[broken".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid regex"));
        assert!(msg.contains("[broken"));
    }

    #[test]
    fn test_error_debug_all_variants() {
        let errors = vec![
            AllowlistError::Io(std::io::Error::new(std::io::ErrorKind::Other, "io")),
            AllowlistError::Serialization(
                serde_json::from_str::<AllowlistConfig>("bad").unwrap_err(),
            ),
            AllowlistError::NotFound("nf".into()),
            AllowlistError::InvalidRegex("[x".into()),
        ];
        for err in errors {
            let dbg = format!("{:?}", err);
            assert!(!dbg.is_empty());
        }
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err: AllowlistError = AllowlistError::from(io_err);
        match err {
            AllowlistError::Io(e) => assert!(e.to_string().contains("gone")),
            _ => panic!("expected Io variant"),
        }
    }

    #[test]
    fn test_error_from_serde() {
        let serde_err = serde_json::from_str::<AllowlistConfig>("invalid").unwrap_err();
        let err: AllowlistError = AllowlistError::from(serde_err);
        match err {
            AllowlistError::Serialization(_) => {}
            _ => panic!("expected Serialization variant"),
        }
    }

    #[test]
    fn test_error_unicode_message() {
        let err = AllowlistError::NotFound("条目不存在".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("条目不存在"));
    }

    #[test]
    fn test_error_empty_message() {
        let err = AllowlistError::NotFound(String::new());
        let msg = format!("{}", err);
        assert!(msg.contains("Entry not found"));
    }

    // ===== domain_matches edge cases =====

    #[test]
    fn test_domain_match_with_port() {
        let entry = make_entry("1", AllowlistPattern::Domain("example.com".into()));
        assert!(entry.matches("http://example.com:8080/file.txt"));
    }

    #[test]
    fn test_domain_match_ip_address() {
        let entry = make_entry("1", AllowlistPattern::Domain("192.168.1.1".into()));
        assert!(entry.matches("http://192.168.1.1/file.txt"));
    }

    #[test]
    fn test_domain_no_partial_match() {
        let entry = make_entry("1", AllowlistPattern::Domain("example.com".into()));
        // notexample.com should NOT match example.com
        assert!(!entry.matches("http://notexample.com/file.txt"));
        // myexample.com should NOT match
        assert!(!entry.matches("http://myexample.com/file.txt"));
    }

    #[test]
    fn test_domain_match_ftp() {
        let entry = make_entry("1", AllowlistPattern::Domain("files.example.com".into()));
        assert!(entry.matches("ftp://files.example.com/pub/file.tar.gz"));
    }

    #[test]
    fn test_domain_empty_url() {
        let entry = make_entry("1", AllowlistPattern::Domain("example.com".into()));
        assert!(!entry.matches(""));
    }

    #[test]
    fn test_domain_unicode_domain() {
        let entry = make_entry("1", AllowlistPattern::Domain("例子.com".into()));
        assert!(entry.matches("http://例子.com/file.txt"));
        assert!(entry.matches("http://子.例子.com/file.txt"));
    }

    #[test]
    fn test_domain_deep_subdomain() {
        let entry = make_entry("1", AllowlistPattern::Domain("example.com".into()));
        assert!(entry.matches("http://a.b.c.d.example.com/file.txt"));
    }

    // ===== wildcard_matches edge cases =====

    #[test]
    fn test_wildcard_multiple_stars() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("http://*.*.com/*.zip".into()));
        assert!(entry.matches("http://sub.example.com/file.zip"));
        assert!(!entry.matches("http://sub.example.com/file.exe"));
    }

    #[test]
    fn test_wildcard_star_question_combo() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("http://example.com/?*.?xt".into()));
        assert!(entry.matches("http://example.com/abc.txt"));
        assert!(entry.matches("http://example.com/a.xt"));
    }

    #[test]
    fn test_wildcard_unicode() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("http://例子.com/*".into()));
        assert!(entry.matches("http://例子.com/文件.txt"));
    }

    #[test]
    fn test_wildcard_empty_text() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("*".into()));
        assert!(entry.matches(""));
    }

    #[test]
    fn test_wildcard_trailing_stars() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("http://example.com/**".into()));
        assert!(entry.matches("http://example.com/anything/at/all"));
    }

    #[test]
    fn test_wildcard_no_match_partial() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("http://example.com/file.zip".into()));
        assert!(!entry.matches("http://example.com/file.zipx"));
    }

    // ===== Exact match edge cases =====

    #[test]
    fn test_exact_match_trailing_slash() {
        let entry = make_entry("1", AllowlistPattern::Exact("http://example.com/".into()));
        assert!(entry.matches("http://example.com/"));
        assert!(!entry.matches("http://example.com"));
    }

    #[test]
    fn test_exact_match_query_params() {
        let entry = make_entry("1", AllowlistPattern::Exact("http://example.com/f?q=1".into()));
        assert!(entry.matches("http://example.com/f?q=1"));
        assert!(!entry.matches("http://example.com/f?q=2"));
        assert!(!entry.matches("http://example.com/f"));
    }

    #[test]
    fn test_exact_match_unicode() {
        let entry = make_entry("1", AllowlistPattern::Exact("http://例子.com/文件.txt".into()));
        assert!(entry.matches("http://例子.com/文件.txt"));
        assert!(!entry.matches("http://例子.com/other.txt"));
    }

    // ===== Regex edge cases =====

    #[test]
    fn test_regex_partial_match() {
        // Regex uses is_match, so partial match is enough
        let entry = make_entry("1", AllowlistPattern::Regex("example".into()));
        assert!(entry.matches("http://example.com/file.txt"));
    }

    #[test]
    fn test_regex_empty_pattern() {
        let entry = make_entry("1", AllowlistPattern::Regex("".into()));
        // Empty regex matches everything
        assert!(entry.matches("anything"));
        assert!(entry.matches(""));
    }

    #[test]
    fn test_regex_anchored() {
        let entry = make_entry(
            "1",
            AllowlistPattern::Regex(r"^https://trusted\.com/.*".into()),
        );
        assert!(entry.matches("https://trusted.com/file.txt"));
        assert!(!entry.matches("http://trusted.com/file.txt"));
    }

    #[test]
    fn test_regex_unicode() {
        let entry = make_entry("1", AllowlistPattern::Regex("中文".into()));
        assert!(entry.matches("http://例子.com/中文文件.txt"));
        assert!(!entry.matches("http://example.com/english.txt"));
    }

    // ===== Persistence edge cases =====

    #[test]
    fn test_save_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let config1 = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("a.com".into()))],
        };
        save_allowlist_config(&config1, temp_dir.path()).unwrap();

        let config2 = AllowlistConfig {
            enabled: false,
            entries: vec![
                make_entry("2", AllowlistPattern::Domain("b.com".into())),
                make_entry("3", AllowlistPattern::Domain("c.com".into())),
            ],
        };
        save_allowlist_config(&config2, temp_dir.path()).unwrap();

        let loaded = load_allowlist_config(temp_dir.path()).unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.entries.len(), 2);
        assert_eq!(loaded.entries[0].id, "2");
    }

    #[test]
    fn test_save_no_tmp_residue() {
        let temp_dir = TempDir::new().unwrap();
        let config = AllowlistConfig::default();
        save_allowlist_config(&config, temp_dir.path()).unwrap();
        let tmp_path = temp_dir.path().join("url_allowlist.json.tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_load_corrupted_json() {
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(
            temp_dir.path().join("url_allowlist.json"),
            "not valid json {{{",
        )
        .unwrap();
        let loaded = load_allowlist_config(temp_dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("url_allowlist.json"), "").unwrap();
        let loaded = load_allowlist_config(temp_dir.path());
        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_pretty_json() {
        let temp_dir = TempDir::new().unwrap();
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("x.com".into()))],
        };
        save_allowlist_config(&config, temp_dir.path()).unwrap();
        let content =
            std::fs::read_to_string(temp_dir.path().join("url_allowlist.json")).unwrap();
        // Pretty JSON has newlines and indentation
        assert!(content.contains('\n'));
    }

    #[test]
    fn test_persistence_unicode_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![AllowlistEntry::new(
                "中文-id".to_string(),
                "信任来源".to_string(),
                AllowlistPattern::Domain("例子.com".to_string()),
                Some("官方镜像 🏛️".to_string()),
            )],
        };
        save_allowlist_config(&config, temp_dir.path()).unwrap();
        let loaded = load_allowlist_config(temp_dir.path()).unwrap();
        assert_eq!(loaded.entries[0].id, "中文-id");
        assert_eq!(loaded.entries[0].name, "信任来源");
        assert_eq!(
            loaded.entries[0].reason.as_deref(),
            Some("官方镜像 🏛️")
        );
    }

    #[test]
    fn test_persistence_all_pattern_types() {
        let temp_dir = TempDir::new().unwrap();
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![
                make_entry("d", AllowlistPattern::Domain("d.com".into())),
                make_entry("e", AllowlistPattern::Exact("http://e.com".into())),
                make_entry("w", AllowlistPattern::Wildcard("*.zip".into())),
                make_entry("r", AllowlistPattern::Regex(r"\d+".into())),
            ],
        };
        save_allowlist_config(&config, temp_dir.path()).unwrap();
        let loaded = load_allowlist_config(temp_dir.path()).unwrap();
        assert_eq!(loaded.entries.len(), 4);
    }

    // ===== check_url_allowlist edge cases =====

    #[test]
    fn test_check_disabled_empty_entries() {
        let config = AllowlistConfig {
            enabled: false,
            entries: vec![],
        };
        let result = check_url_allowlist("http://anything.com/file.txt", &config);
        assert!(result.allowed);
        assert!(result.matched_entry_id.is_none());
        assert!(result.matched_entry_name.is_none());
        assert!(result.reason.is_none());
    }

    #[test]
    fn test_check_url_with_port() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("example.com".into()))],
        };
        let result = check_url_allowlist("http://example.com:8080/file.txt", &config);
        assert!(result.allowed);
        assert_eq!(result.matched_entry_id.as_deref(), Some("1"));
    }

    #[test]
    fn test_check_ftp_url() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("files.com".into()))],
        };
        let result = check_url_allowlist("ftp://files.com/pub/file.tar.gz", &config);
        assert!(result.allowed);
    }

    #[test]
    fn test_check_magnet_url() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry(
                "1",
                AllowlistPattern::Wildcard("magnet:*".into()),
            )],
        };
        let result = check_url_allowlist(
            "magnet:?xt=urn:btih:abc123&dn=file.txt",
            &config,
        );
        assert!(result.allowed);
    }

    #[test]
    fn test_check_invalid_url_not_allowed() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("example.com".into()))],
        };
        let result = check_url_allowlist("not-a-url", &config);
        assert!(!result.allowed);
    }

    #[test]
    fn test_check_skips_disabled_entry() {
        let mut disabled_entry = make_entry("1", AllowlistPattern::Domain("x.com".into()));
        disabled_entry.enabled = false;
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![
                disabled_entry,
                make_entry("2", AllowlistPattern::Domain("y.com".into())),
            ],
        };
        // x.com is disabled, so it should not match
        assert!(!check_url_allowlist("http://x.com/f.txt", &config).allowed);
        // y.com is enabled, should match
        assert!(check_url_allowlist("http://y.com/f.txt", &config).allowed);
    }

    #[test]
    fn test_check_many_entries() {
        let entries: Vec<AllowlistEntry> = (0..50)
            .map(|i| make_entry(&format!("e{}", i), AllowlistPattern::Domain(format!("d{}.com", i))))
            .collect();
        let config = AllowlistConfig {
            enabled: true,
            entries,
        };
        assert!(check_url_allowlist("http://d25.com/f.txt", &config).allowed);
        assert!(!check_url_allowlist("http://unknown.com/f.txt", &config).allowed);
    }

    // ===== Entry with reason =====

    #[test]
    fn test_entry_with_reason_matches() {
        let entry = make_entry_with_reason(
            "1",
            AllowlistPattern::Domain("trusted.com".into()),
            "Official CDN",
        );
        assert!(entry.matches("http://trusted.com/file.txt"));
        assert_eq!(entry.reason.as_deref(), Some("Official CDN"));
    }

    #[test]
    fn test_entry_none_reason() {
        let entry = make_entry("1", AllowlistPattern::Domain("x.com".into()));
        assert!(entry.reason.is_none());
    }

    // ===== Unicode fields =====

    #[test]
    fn test_entry_unicode_fields() {
        let entry = AllowlistEntry::new(
            "任务-001".to_string(),
            "信任源 🛡️".to_string(),
            AllowlistPattern::Domain("例子.com".to_string()),
            Some("官方镜像".to_string()),
        );
        assert_eq!(entry.id, "任务-001");
        assert_eq!(entry.name, "信任源 🛡️");
        assert!(entry.matches("http://例子.com/file.txt"));
    }

    #[test]
    fn test_entry_emoji_id() {
        let entry = make_entry("🔒🔑", AllowlistPattern::Wildcard("*".into()));
        assert!(entry.matches("http://anything.com"));
    }

    // ===== Complex workflows =====

    #[test]
    fn test_workflow_add_check_disable_check() {
        let mut config = AllowlistConfig {
            enabled: true,
            entries: vec![],
        };
        // No entries: nothing allowed
        assert!(!check_url_allowlist("http://x.com/f.txt", &config).allowed);

        // Add entry
        config
            .entries
            .push(make_entry("1", AllowlistPattern::Domain("x.com".into())));
        assert!(check_url_allowlist("http://x.com/f.txt", &config).allowed);

        // Disable entry
        config.entries[0].enabled = false;
        assert!(!check_url_allowlist("http://x.com/f.txt", &config).allowed);

        // Re-enable
        config.entries[0].enabled = true;
        assert!(check_url_allowlist("http://x.com/f.txt", &config).allowed);
    }

    #[test]
    fn test_workflow_save_load_check() {
        let temp_dir = TempDir::new().unwrap();
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![
                make_entry("1", AllowlistPattern::Domain("trusted.com".into())),
                make_entry(
                    "2",
                    AllowlistPattern::Wildcard("http://cdn.example.com/*.zip".into()),
                ),
            ],
        };
        save_allowlist_config(&config, temp_dir.path()).unwrap();

        let loaded = load_allowlist_config(temp_dir.path()).unwrap();
        assert!(check_url_allowlist("http://trusted.com/file.txt", &loaded).allowed);
        assert!(check_url_allowlist("http://cdn.example.com/archive.zip", &loaded).allowed);
        assert!(!check_url_allowlist("http://evil.com/malware.exe", &loaded).allowed);
    }

    #[test]
    fn test_workflow_mixed_pattern_types() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![
                make_entry("d", AllowlistPattern::Domain("example.com".into())),
                make_entry("w", AllowlistPattern::Wildcard("http://cdn.*.com/*".into())),
                make_entry(
                    "r",
                    AllowlistPattern::Regex(r"https://api\.trusted\.com/v\d+/.*".into()),
                ),
                make_entry(
                    "e",
                    AllowlistPattern::Exact("http://specific.com/file.zip".into()),
                ),
            ],
        };
        // Domain match
        assert!(check_url_allowlist("http://example.com/any.txt", &config).allowed);
        // Wildcard match
        assert!(check_url_allowlist("http://cdn.fast.com/file.txt", &config).allowed);
        // Regex match
        assert!(
            check_url_allowlist("https://api.trusted.com/v2/data", &config).allowed
        );
        // Exact match
        assert!(
            check_url_allowlist("http://specific.com/file.zip", &config).allowed
        );
        // No match
        assert!(!check_url_allowlist("http://random.com/file.txt", &config).allowed);
    }

    #[test]
    fn test_workflow_multiple_subdomain_independence() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![make_entry("1", AllowlistPattern::Domain("sub.example.com".into()))],
        };
        // sub.example.com matches
        assert!(check_url_allowlist("http://sub.example.com/f.txt", &config).allowed);
        // deep.sub.example.com matches (subdomain of allowed)
        assert!(check_url_allowlist("http://deep.sub.example.com/f.txt", &config).allowed);
        // example.com itself does NOT match (parent domain not included)
        assert!(!check_url_allowlist("http://example.com/f.txt", &config).allowed);
    }

    // ===== Entry serde =====

    #[test]
    fn test_entry_serde_roundtrip() {
        let entry = AllowlistEntry::new(
            "id-1".to_string(),
            "Test Entry".to_string(),
            AllowlistPattern::Domain("example.com".to_string()),
            Some("Trusted source".to_string()),
        );
        let json = serde_json::to_string(&entry).unwrap();
        let back: AllowlistEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "id-1");
        assert_eq!(back.name, "Test Entry");
        assert_eq!(back.reason.as_deref(), Some("Trusted source"));
        assert!(back.enabled);
    }

    #[test]
    fn test_entry_serde_extra_fields_ignored() {
        let json = r#"{
            "id":"1",
            "name":"Test",
            "pattern":{"type":"Domain","value":"x.com"},
            "enabled":true,
            "reason":null,
            "created_at":"2026-01-01T00:00:00Z",
            "extra":"ignored"
        }"#;
        let entry: AllowlistEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.id, "1");
    }

    #[test]
    fn test_entry_serde_null_reason() {
        let entry = AllowlistEntry::new(
            "1".to_string(),
            "Test".to_string(),
            AllowlistPattern::Exact("http://x.com".to_string()),
            None,
        );
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"reason\":null"));
        let back: AllowlistEntry = serde_json::from_str(&json).unwrap();
        assert!(back.reason.is_none());
    }

    // ===== Additional boundary tests =====

    #[test]
    fn test_domain_match_with_path_prefix() {
        // Domain matching should work regardless of path
        let entry = make_entry("1", AllowlistPattern::Domain("example.com".into()));
        assert!(entry.matches("http://example.com/"));
        assert!(entry.matches("http://example.com/a/b/c/d/e/f.txt"));
    }

    #[test]
    fn test_domain_match_with_query_and_fragment() {
        let entry = make_entry("1", AllowlistPattern::Domain("example.com".into()));
        assert!(entry.matches("http://example.com/f?q=1&r=2#section"));
    }

    #[test]
    fn test_wildcard_question_mark_single_char() {
        let entry = make_entry("1", AllowlistPattern::Wildcard("http://example.com/file?.txt".into()));
        assert!(entry.matches("http://example.com/file1.txt"));
        assert!(entry.matches("http://example.com/file_.txt"));
        assert!(!entry.matches("http://example.com/file.txt"));
        assert!(!entry.matches("http://example.com/file12.txt"));
    }

    #[test]
    fn test_regex_special_chars_in_url() {
        let entry = make_entry(
            "1",
            AllowlistPattern::Regex(r"http://example\.com/path\?q=\d+".into()),
        );
        assert!(entry.matches("http://example.com/path?q=42"));
        assert!(!entry.matches("http://example.com/path?q=abc"));
    }

    #[test]
    fn test_check_result_all_fields_none() {
        let result = AllowlistCheckResult {
            allowed: false,
            matched_entry_id: None,
            matched_entry_name: None,
            reason: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: AllowlistCheckResult = serde_json::from_str(&json).unwrap();
        assert!(!back.allowed);
        assert!(back.matched_entry_id.is_none());
        assert!(back.matched_entry_name.is_none());
        assert!(back.reason.is_none());
    }

    #[test]
    fn test_config_empty_entries_serde() {
        let config = AllowlistConfig {
            enabled: true,
            entries: vec![],
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: AllowlistConfig = serde_json::from_str(&json).unwrap();
        assert!(back.enabled);
        assert!(back.entries.is_empty());
    }

    #[test]
    fn test_many_entries_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let entries: Vec<AllowlistEntry> = (0..100)
            .map(|i| {
                make_entry(
                    &format!("entry-{}", i),
                    AllowlistPattern::Domain(format!("domain{}.com", i)),
                )
            })
            .collect();
        let config = AllowlistConfig {
            enabled: true,
            entries,
        };
        save_allowlist_config(&config, temp_dir.path()).unwrap();
        let loaded = load_allowlist_config(temp_dir.path()).unwrap();
        assert_eq!(loaded.entries.len(), 100);
        assert_eq!(loaded.entries[99].id, "entry-99");
    }

    #[test]
    fn test_entry_created_at_preserved_in_serde() {
        let entry = AllowlistEntry::new(
            "1".to_string(),
            "Test".to_string(),
            AllowlistPattern::Domain("x.com".to_string()),
            None,
        );
        let original_ts = entry.created_at;
        let json = serde_json::to_string(&entry).unwrap();
        let back: AllowlistEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.created_at, original_ts);
    }
}
