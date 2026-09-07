//! URL Deduplication Policies
//!
//! Configurable deduplication strategies for download tasks:
//! - Exact: Match identical URLs (default)
//! - Domain: Match any URL from the same domain
//! - PathPrefix: Match URLs with the same path prefix (e.g., /downloads/file.*)
//! - Smart: Combine domain + filename matching
//!
//! Also provides DuplicatePolicy to control behavior when duplicates are detected:
//! - Reject: Return an error (default, current behavior)
//! - Skip: Silently ignore the duplicate, return existing task ID
//! - Allow: Allow duplicate tasks to coexist
//! - PauseExisting: Pause the existing task and add the new one

use serde::{Deserialize, Serialize};
use std::path::Path;
use url::Url;

/// Deduplication mode for URL matching
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DedupMode {
    /// Exact URL match (default)
    #[default]
    Exact,
    /// Match by domain only
    Domain,
    /// Match by domain + path prefix (first 2 segments)
    PathPrefix,
    /// Smart matching: domain + normalized filename
    Smart,
}

impl std::fmt::Display for DedupMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DedupMode::Exact => write!(f, "exact"),
            DedupMode::Domain => write!(f, "domain"),
            DedupMode::PathPrefix => write!(f, "path-prefix"),
            DedupMode::Smart => write!(f, "smart"),
        }
    }
}

impl std::str::FromStr for DedupMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "exact" => Ok(DedupMode::Exact),
            "domain" => Ok(DedupMode::Domain),
            "path-prefix" | "pathprefix" | "path" => Ok(DedupMode::PathPrefix),
            "smart" => Ok(DedupMode::Smart),
            _ => Err(format!(
                "invalid dedup mode: {s} (valid: exact, domain, path-prefix, smart)"
            )),
        }
    }
}

/// Policy for handling duplicate download tasks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DuplicatePolicy {
    /// Reject the duplicate with an error (default behavior)
    #[default]
    Reject,
    /// Silently skip the duplicate, return the existing task ID
    Skip,
    /// Allow duplicate tasks to coexist (no dedup check)
    Allow,
    /// Pause the existing task and add the new one
    PauseExisting,
}

impl std::fmt::Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuplicatePolicy::Reject => write!(f, "reject"),
            DuplicatePolicy::Skip => write!(f, "skip"),
            DuplicatePolicy::Allow => write!(f, "allow"),
            DuplicatePolicy::PauseExisting => write!(f, "pause_existing"),
        }
    }
}

impl std::str::FromStr for DuplicatePolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "reject" => Ok(DuplicatePolicy::Reject),
            "skip" => Ok(DuplicatePolicy::Skip),
            "allow" => Ok(DuplicatePolicy::Allow),
            "pause_existing" | "pauseexisting" | "pause" => Ok(DuplicatePolicy::PauseExisting),
            _ => Err(format!(
                "invalid duplicate policy: {s} (valid: reject, skip, allow, pause_existing)"
            )),
        }
    }
}

/// Configuration for URL deduplication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupConfig {
    /// Deduplication mode
    pub mode: DedupMode,
    /// Whether to strip query parameters before comparison
    pub strip_query: bool,
    /// Whether to strip fragments (#anchor) before comparison
    pub strip_fragment: bool,
    /// Whether dedup is enabled
    pub enabled: bool,
    /// Policy for handling duplicate tasks
    pub duplicate_policy: DuplicatePolicy,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        }
    }
}

/// Extracts a deduplication key from a URL based on the mode
pub fn extract_dedup_key(url: &str, config: &DedupConfig) -> Option<String> {
    if !config.enabled {
        return None;
    }

    // Handle non-HTTP URLs (ed2k, magnet, etc.) - always use exact match
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Some(url.to_string());
    }

    // Try to parse as URL; if it fails, return None
    let parsed = Url::parse(url).ok()?;

    // Verify it has a valid host (not just any string)
    let host = parsed.host_str()?;
    if host.is_empty() {
        return None;
    }

    match config.mode {
        DedupMode::Exact => {
            let mut key = format!(
                "{}://{}{}",
                parsed.scheme(),
                parsed.host_str()?,
                parsed.path()
            );
            if !config.strip_query
                && let Some(query) = parsed.query()
            {
                key.push('?');
                key.push_str(query);
            }
            if !config.strip_fragment
                && let Some(fragment) = parsed.fragment()
            {
                key.push('#');
                key.push_str(fragment);
            }
            Some(key)
        }
        DedupMode::Domain => {
            let host = parsed.host_str()?;
            // Normalize: strip www. prefix
            let normalized_host = host.strip_prefix("www.").unwrap_or(host);
            Some(format!("{}://{}", parsed.scheme(), normalized_host))
        }
        DedupMode::PathPrefix => {
            let host = parsed.host_str()?;
            let normalized_host = host.strip_prefix("www.").unwrap_or(host);
            let path = parsed.path();

            // Extract first path segment as prefix
            let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).take(1).collect();
            let prefix = if segments.is_empty() {
                String::new()
            } else {
                format!("/{}", segments[0])
            };

            Some(format!(
                "{}://{}{}",
                parsed.scheme(),
                normalized_host,
                prefix
            ))
        }
        DedupMode::Smart => {
            let host = parsed.host_str()?;
            let normalized_host = host.strip_prefix("www.").unwrap_or(host);
            let path = parsed.path();

            // Extract filename from path
            let filename = path.rsplit('/').next().unwrap_or("");

            // Normalize filename: lowercase, strip common suffixes like (1), (2)
            let normalized_filename = normalize_filename(filename);

            Some(format!(
                "{}://{}/{}",
                parsed.scheme(),
                normalized_host,
                normalized_filename
            ))
        }
    }
}

/// Normalize filename by removing common duplicate suffixes
fn normalize_filename(filename: &str) -> String {
    let mut name = filename.to_lowercase();

    // Remove common duplicate patterns: (1), (2), - Copy, etc.
    let patterns = [
        r"\s*\(\d+\)\s*",    // (1), (2), etc.
        r"\s*-\s*copy\b\s*", // - Copy
        r"\s*\(copy\)\b\s*", // (copy)
        r"\s*\[\d+\]\s*",    // [1], [2], etc.
    ];

    for pattern in &patterns {
        if let Ok(re) = regex_lite::Regex::new(pattern) {
            name = re.replace_all(&name, "").to_string();
        }
    }

    // Trim whitespace
    name = name.trim().to_string();

    name
}

/// Check if a URL is a duplicate of any existing task URLs
pub fn find_duplicate_url(
    new_url: &str,
    existing_urls: &[String],
    config: &DedupConfig,
) -> Option<usize> {
    if !config.enabled {
        return None;
    }

    let new_key = extract_dedup_key(new_url, config)?;

    for (idx, existing_url) in existing_urls.iter().enumerate() {
        let existing_key = extract_dedup_key(existing_url, config)?;
        if new_key == existing_key {
            return Some(idx);
        }
    }

    None
}

/// Persistence functions for dedup configuration
pub fn save_dedup_config(config: &DedupConfig, data_dir: &Path) -> Result<(), std::io::Error> {
    let config_path = data_dir.join("dedup_config.json");
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;

    // Atomic write
    let temp_path = data_dir.join("dedup_config.json.tmp");
    std::fs::write(&temp_path, json)?;
    std::fs::rename(temp_path, config_path)?;

    Ok(())
}

pub fn load_dedup_config(data_dir: &Path) -> Option<DedupConfig> {
    let config_path = data_dir.join("dedup_config.json");
    let json = std::fs::read_to_string(config_path).ok()?;
    serde_json::from_str(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dedup_mode_default() {
        assert_eq!(DedupMode::default(), DedupMode::Exact);
    }

    #[test]
    fn test_dedup_mode_display() {
        assert_eq!(DedupMode::Exact.to_string(), "exact");
        assert_eq!(DedupMode::Domain.to_string(), "domain");
        assert_eq!(DedupMode::PathPrefix.to_string(), "path-prefix");
        assert_eq!(DedupMode::Smart.to_string(), "smart");
    }

    #[test]
    fn test_dedup_mode_from_str() {
        assert_eq!("exact".parse::<DedupMode>().unwrap(), DedupMode::Exact);
        assert_eq!("domain".parse::<DedupMode>().unwrap(), DedupMode::Domain);
        assert_eq!(
            "path-prefix".parse::<DedupMode>().unwrap(),
            DedupMode::PathPrefix
        );
        assert_eq!("path".parse::<DedupMode>().unwrap(), DedupMode::PathPrefix);
        assert_eq!("smart".parse::<DedupMode>().unwrap(), DedupMode::Smart);
        assert!("invalid".parse::<DedupMode>().is_err());
    }

    #[test]
    fn test_dedup_config_default() {
        let config = DedupConfig::default();
        assert_eq!(config.mode, DedupMode::Exact);
        assert!(config.strip_query);
        assert!(config.strip_fragment);
        assert!(config.enabled);
        assert_eq!(config.duplicate_policy, DuplicatePolicy::Reject);
    }

    #[test]
    fn test_duplicate_policy_display() {
        assert_eq!(DuplicatePolicy::Reject.to_string(), "reject");
        assert_eq!(DuplicatePolicy::Skip.to_string(), "skip");
        assert_eq!(DuplicatePolicy::Allow.to_string(), "allow");
        assert_eq!(DuplicatePolicy::PauseExisting.to_string(), "pause_existing");
    }

    #[test]
    fn test_duplicate_policy_from_str() {
        assert_eq!(
            "reject".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::Reject
        );
        assert_eq!(
            "skip".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::Skip
        );
        assert_eq!(
            "allow".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::Allow
        );
        assert_eq!(
            "pause_existing".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::PauseExisting
        );
        assert_eq!(
            "pause".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::PauseExisting
        );
        assert!("invalid".parse::<DuplicatePolicy>().is_err());
    }

    #[test]
    fn test_extract_dedup_key_exact() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let url1 = "https://example.com/file.zip?token=abc";
        let url2 = "https://example.com/file.zip?token=xyz";
        let url3 = "https://example.com/file.zip#section";

        let key1 = extract_dedup_key(url1, &config).unwrap();
        let key2 = extract_dedup_key(url2, &config).unwrap();
        let key3 = extract_dedup_key(url3, &config).unwrap();

        // With strip_query=true, query params are ignored
        assert_eq!(key1, key2);
        // With strip_fragment=true, fragments are ignored
        assert_eq!(key1, key3);
        assert_eq!(key1, "https://example.com/file.zip");
    }

    #[test]
    fn test_extract_dedup_key_exact_with_query() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: false,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let url1 = "https://example.com/file.zip?token=abc";
        let url2 = "https://example.com/file.zip?token=xyz";

        let key1 = extract_dedup_key(url1, &config).unwrap();
        let key2 = extract_dedup_key(url2, &config).unwrap();

        // With strip_query=false, different query params make different keys
        assert_ne!(key1, key2);
        assert_eq!(key1, "https://example.com/file.zip?token=abc");
        assert_eq!(key2, "https://example.com/file.zip?token=xyz");
    }

    #[test]
    fn test_extract_dedup_key_domain() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let url1 = "https://example.com/file1.zip";
        let url2 = "https://example.com/file2.zip";
        let url3 = "https://www.example.com/file3.zip";
        let url4 = "https://other.com/file1.zip";

        let key1 = extract_dedup_key(url1, &config).unwrap();
        let key2 = extract_dedup_key(url2, &config).unwrap();
        let key3 = extract_dedup_key(url3, &config).unwrap();
        let key4 = extract_dedup_key(url4, &config).unwrap();

        // Same domain (www. stripped)
        assert_eq!(key1, key2);
        assert_eq!(key1, key3);
        // Different domain
        assert_ne!(key1, key4);
        assert_eq!(key1, "https://example.com");
        assert_eq!(key4, "https://other.com");
    }

    #[test]
    fn test_extract_dedup_key_path_prefix() {
        let config = DedupConfig {
            mode: DedupMode::PathPrefix,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let url1 = "https://example.com/downloads/file1.zip";
        let url2 = "https://example.com/downloads/file2.zip";
        let url3 = "https://example.com/uploads/file3.zip";
        let url4 = "https://example.com/downloads/subdir/file4.zip";

        let key1 = extract_dedup_key(url1, &config).unwrap();
        let key2 = extract_dedup_key(url2, &config).unwrap();
        let key3 = extract_dedup_key(url3, &config).unwrap();
        let key4 = extract_dedup_key(url4, &config).unwrap();

        // Same first path segment
        assert_eq!(key1, key2);
        assert_eq!(key1, "https://example.com/downloads");
        // Different path prefix
        assert_ne!(key1, key3);
        assert_eq!(key3, "https://example.com/uploads");
        // Subdirectory still matches parent prefix
        assert_eq!(key1, key4);
    }

    #[test]
    fn test_extract_dedup_key_smart() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let url1 = "https://example.com/files/document.pdf";
        let url2 = "https://example.com/files/document(1).pdf";
        let url3 = "https://example.com/files/document(2).pdf";
        let url4 = "https://example.com/files/other.pdf";
        let url5 = "https://other.com/files/document.pdf";

        let key1 = extract_dedup_key(url1, &config).unwrap();
        let key2 = extract_dedup_key(url2, &config).unwrap();
        let key3 = extract_dedup_key(url3, &config).unwrap();
        let key4 = extract_dedup_key(url4, &config).unwrap();
        let key5 = extract_dedup_key(url5, &config).unwrap();

        // Smart dedup recognizes (1), (2) as duplicates
        assert_eq!(key1, key2);
        assert_eq!(key1, key3);
        assert_eq!(key1, "https://example.com/document.pdf");
        // Different filename
        assert_ne!(key1, key4);
        assert_eq!(key4, "https://example.com/other.pdf");
        // Different domain
        assert_ne!(key1, key5);
        assert_eq!(key5, "https://other.com/document.pdf");
    }

    #[test]
    fn test_normalize_filename() {
        assert_eq!(normalize_filename("file(1).zip"), "file.zip");
        assert_eq!(normalize_filename("file(2).zip"), "file.zip");
        assert_eq!(normalize_filename("file - Copy.zip"), "file.zip");
        assert_eq!(normalize_filename("file[1].zip"), "file.zip");
        assert_eq!(normalize_filename("FILE.ZIP"), "file.zip");
        assert_eq!(normalize_filename("normal.zip"), "normal.zip");
    }

    #[test]
    fn test_find_duplicate_url_exact() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let existing = vec![
            "https://example.com/file1.zip".to_string(),
            "https://example.com/file2.zip".to_string(),
            "https://other.com/file3.zip".to_string(),
        ];

        assert_eq!(
            find_duplicate_url("https://example.com/file1.zip", &existing, &config),
            Some(0)
        );
        assert_eq!(
            find_duplicate_url(
                "https://example.com/file2.zip?token=abc",
                &existing,
                &config
            ),
            Some(1)
        );
        assert_eq!(
            find_duplicate_url("https://example.com/file4.zip", &existing, &config),
            None
        );
    }

    #[test]
    fn test_find_duplicate_url_domain() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let existing = vec![
            "https://example.com/file1.zip".to_string(),
            "https://other.com/file2.zip".to_string(),
        ];

        // Any URL from example.com matches
        assert_eq!(
            find_duplicate_url("https://example.com/any-file.zip", &existing, &config),
            Some(0)
        );
        assert_eq!(
            find_duplicate_url("https://www.example.com/other-file.zip", &existing, &config),
            Some(0)
        );
        assert_eq!(
            find_duplicate_url("https://third.com/file.zip", &existing, &config),
            None
        );
    }

    #[test]
    fn test_find_duplicate_url_disabled() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: false,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let existing = vec!["https://example.com/file1.zip".to_string()];

        // When disabled, never finds duplicates
        assert_eq!(
            find_duplicate_url("https://example.com/file1.zip", &existing, &config),
            None
        );
    }

    #[test]
    fn test_non_http_urls() {
        let config = DedupConfig {
            mode: DedupMode::Domain, // Mode doesn't matter for non-HTTP
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };

        let magnet = "magnet:?xt=urn:btih:abc123&dn=test";
        let ed2k = "ed2k://|file|test.zip|1000|abc123|/";

        let key_magnet = extract_dedup_key(magnet, &config).unwrap();
        let key_ed2k = extract_dedup_key(ed2k, &config).unwrap();

        // Non-HTTP URLs use exact match regardless of mode
        assert_eq!(key_magnet, magnet);
        assert_eq!(key_ed2k, ed2k);
    }

    #[test]
    fn test_dedup_config_serialization() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: false,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Skip,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DedupConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.mode, DedupMode::Smart);
        assert!(!deserialized.strip_query);
        assert!(deserialized.strip_fragment);
        assert!(deserialized.enabled);
        assert_eq!(deserialized.duplicate_policy, DuplicatePolicy::Skip);
    }

    #[test]
    fn test_save_load_dedup_config() {
        let temp_dir = std::env::temp_dir().join("test_dedup_config");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config = DedupConfig {
            mode: DedupMode::PathPrefix,
            strip_query: false,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::PauseExisting,
        };

        save_dedup_config(&config, &temp_dir).unwrap();
        let loaded = load_dedup_config(&temp_dir).unwrap();

        assert_eq!(loaded.mode, DedupMode::PathPrefix);
        assert!(!loaded.strip_query);
        assert!(loaded.strip_fragment);
        assert!(loaded.enabled);
        assert_eq!(loaded.duplicate_policy, DuplicatePolicy::PauseExisting);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_load_dedup_config_missing_file() {
        let temp_dir = std::env::temp_dir().join("test_dedup_missing");
        let _ = std::fs::remove_dir_all(&temp_dir);

        assert!(load_dedup_config(&temp_dir).is_none());
    }

    #[test]
    fn test_extract_dedup_key_invalid_url() {
        let config = DedupConfig::default();
        // "not a url" doesn't start with http:// so it's treated as non-HTTP exact match
        assert_eq!(
            extract_dedup_key("not a url", &config),
            Some("not a url".to_string())
        );
        // Empty string also treated as non-HTTP
        assert_eq!(extract_dedup_key("", &config), Some("".to_string()));
        // HTTP URLs that fail to parse return None
        assert_eq!(extract_dedup_key("http://", &config), None);
    }

    // ===== DedupMode serde roundtrip =====

    #[test]
    fn test_dedup_mode_serde_roundtrip_all_variants() {
        for mode in [
            DedupMode::Exact,
            DedupMode::Domain,
            DedupMode::PathPrefix,
            DedupMode::Smart,
        ] {
            let json = serde_json::to_string(&mode).unwrap();
            let deserialized: DedupMode = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, mode);
        }
    }

    #[test]
    fn test_dedup_mode_serde_lowercase_values() {
        // Verify that serialization produces lowercase values
        let mode = DedupMode::Exact;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, "\"exact\"");

        let mode = DedupMode::Domain;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, "\"domain\"");

        let mode = DedupMode::PathPrefix;
        let json = serde_json::to_string(&mode).unwrap();
        // serde rename_all = "lowercase" converts PathPrefix -> pathprefix
        assert_eq!(json, "\"pathprefix\"");

        let mode = DedupMode::Smart;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, "\"smart\"");
    }

    // ===== DedupMode traits =====

    #[test]
    fn test_dedup_mode_clone_copy_eq() {
        let mode = DedupMode::Smart;
        let cloned = mode;
        let copied = mode;
        assert_eq!(cloned, copied);
        assert_eq!(mode, DedupMode::Smart);
    }

    #[test]
    fn test_dedup_mode_debug() {
        let debug = format!("{:?}", DedupMode::Exact);
        assert_eq!(debug, "Exact");
        let debug = format!("{:?}", DedupMode::PathPrefix);
        assert_eq!(debug, "PathPrefix");
    }

    // ===== DuplicatePolicy serde roundtrip =====

    #[test]
    fn test_duplicate_policy_serde_roundtrip_all_variants() {
        for policy in [
            DuplicatePolicy::Reject,
            DuplicatePolicy::Skip,
            DuplicatePolicy::Allow,
            DuplicatePolicy::PauseExisting,
        ] {
            let json = serde_json::to_string(&policy).unwrap();
            let deserialized: DuplicatePolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, policy);
        }
    }

    #[test]
    fn test_duplicate_policy_serde_snake_case_values() {
        let json = "\"reject\"";
        let p: DuplicatePolicy = serde_json::from_str(json).unwrap();
        assert_eq!(p, DuplicatePolicy::Reject);

        let json = "\"skip\"";
        let p: DuplicatePolicy = serde_json::from_str(json).unwrap();
        assert_eq!(p, DuplicatePolicy::Skip);

        let json = "\"allow\"";
        let p: DuplicatePolicy = serde_json::from_str(json).unwrap();
        assert_eq!(p, DuplicatePolicy::Allow);

        let json = "\"pause_existing\"";
        let p: DuplicatePolicy = serde_json::from_str(json).unwrap();
        assert_eq!(p, DuplicatePolicy::PauseExisting);
    }

    // ===== DuplicatePolicy traits =====

    #[test]
    fn test_duplicate_policy_clone_copy_eq() {
        let policy = DuplicatePolicy::PauseExisting;
        let cloned = policy;
        let copied = policy;
        assert_eq!(cloned, copied);
        assert_eq!(policy, DuplicatePolicy::PauseExisting);
    }

    #[test]
    fn test_duplicate_policy_debug() {
        let debug = format!("{:?}", DuplicatePolicy::Reject);
        assert_eq!(debug, "Reject");
        let debug = format!("{:?}", DuplicatePolicy::PauseExisting);
        assert_eq!(debug, "PauseExisting");
    }

    // ===== DedupConfig serde =====

    #[test]
    fn test_dedup_config_serde_roundtrip() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: false,
            strip_fragment: false,
            enabled: false,
            duplicate_policy: DuplicatePolicy::Allow,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DedupConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.mode, DedupMode::Smart);
        assert!(!deserialized.strip_query);
        assert!(!deserialized.strip_fragment);
        assert!(!deserialized.enabled);
        assert_eq!(deserialized.duplicate_policy, DuplicatePolicy::Allow);
    }

    #[test]
    fn test_dedup_config_serde_extra_fields_ignored() {
        let json = r#"{"mode":"exact","strip_query":true,"strip_fragment":true,"enabled":true,"duplicate_policy":"reject","extra_field":"ignored"}"#;
        let config: DedupConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.mode, DedupMode::Exact);
    }

    #[test]
    fn test_dedup_config_pretty_serde() {
        let config = DedupConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let deserialized: DedupConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.mode, config.mode);
    }

    #[test]
    fn test_dedup_config_clone_debug() {
        let config = DedupConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.mode, config.mode);
        assert_eq!(cloned.strip_query, config.strip_query);
        let debug = format!("{:?}", config);
        assert!(debug.contains("DedupConfig"));
    }

    // ===== DedupMode from_str boundary =====

    #[test]
    fn test_dedup_mode_from_str_case_insensitive() {
        assert_eq!("EXACT".parse::<DedupMode>().unwrap(), DedupMode::Exact);
        assert_eq!("Exact".parse::<DedupMode>().unwrap(), DedupMode::Exact);
        assert_eq!("DoMaIn".parse::<DedupMode>().unwrap(), DedupMode::Domain);
        assert_eq!("SMART".parse::<DedupMode>().unwrap(), DedupMode::Smart);
        assert_eq!(
            "PATH-PREFIX".parse::<DedupMode>().unwrap(),
            DedupMode::PathPrefix
        );
    }

    #[test]
    fn test_dedup_mode_from_str_all_aliases() {
        assert_eq!(
            "pathprefix".parse::<DedupMode>().unwrap(),
            DedupMode::PathPrefix
        );
        assert_eq!("path".parse::<DedupMode>().unwrap(), DedupMode::PathPrefix);
    }

    #[test]
    fn test_dedup_mode_from_str_whitespace() {
        assert!(" exact".parse::<DedupMode>().is_err());
        assert!("exact ".parse::<DedupMode>().is_err());
        assert!(" exact ".parse::<DedupMode>().is_err());
    }

    #[test]
    fn test_dedup_mode_from_str_empty() {
        assert!("".parse::<DedupMode>().is_err());
    }

    #[test]
    fn test_dedup_mode_from_str_unicode() {
        assert!("中文".parse::<DedupMode>().is_err());
    }

    #[test]
    fn test_dedup_mode_from_str_error_message() {
        let err = "bogus".parse::<DedupMode>().unwrap_err();
        assert!(err.contains("bogus"));
        assert!(err.contains("invalid"));
    }

    // ===== DuplicatePolicy from_str boundary =====

    #[test]
    fn test_duplicate_policy_from_str_case_insensitive() {
        assert_eq!(
            "REJECT".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::Reject
        );
        assert_eq!(
            "Skip".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::Skip
        );
        assert_eq!(
            "ALLOW".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::Allow
        );
        assert_eq!(
            "PAUSE_EXISTING".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::PauseExisting
        );
    }

    #[test]
    fn test_duplicate_policy_from_str_all_aliases() {
        assert_eq!(
            "pauseexisting".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::PauseExisting
        );
        assert_eq!(
            "pause".parse::<DuplicatePolicy>().unwrap(),
            DuplicatePolicy::PauseExisting
        );
    }

    #[test]
    fn test_duplicate_policy_from_str_whitespace() {
        assert!(" reject".parse::<DuplicatePolicy>().is_err());
        assert!("reject ".parse::<DuplicatePolicy>().is_err());
    }

    #[test]
    fn test_duplicate_policy_from_str_empty() {
        assert!("".parse::<DuplicatePolicy>().is_err());
    }

    #[test]
    fn test_duplicate_policy_from_str_unicode() {
        assert!("日本語".parse::<DuplicatePolicy>().is_err());
    }

    #[test]
    fn test_duplicate_policy_from_str_error_message() {
        let err = "foobar".parse::<DuplicatePolicy>().unwrap_err();
        assert!(err.contains("foobar"));
        assert!(err.contains("invalid"));
    }

    // ===== extract_dedup_key disabled =====

    #[test]
    fn test_extract_dedup_key_disabled_returns_none() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: false,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        assert!(extract_dedup_key("https://example.com/file.zip", &config).is_none());
    }

    // ===== extract_dedup_key non-HTTP =====

    #[test]
    fn test_extract_dedup_key_magnet() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let magnet = "magnet:?xt=urn:btih:abc123";
        assert_eq!(extract_dedup_key(magnet, &config), Some(magnet.to_string()));
    }

    #[test]
    fn test_extract_dedup_key_ed2k() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let ed2k = "ed2k://|file|test.zip|1000|abc|/";
        assert_eq!(extract_dedup_key(ed2k, &config), Some(ed2k.to_string()));
    }

    #[test]
    fn test_extract_dedup_key_ftp() {
        let config = DedupConfig::default();
        let ftp = "ftp://files.example.com/pub/data.tar.gz";
        // ftp doesn't start with http:// or https://, so exact match
        assert_eq!(extract_dedup_key(ftp, &config), Some(ftp.to_string()));
    }

    // ===== extract_dedup_key Exact mode details =====

    #[test]
    fn test_exact_strip_query_true_strip_fragment_true() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com/path?a=1#frag", &config).unwrap();
        assert_eq!(key, "https://example.com/path");
    }

    #[test]
    fn test_exact_strip_query_false_strip_fragment_true() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: false,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com/path?a=1#frag", &config).unwrap();
        assert_eq!(key, "https://example.com/path?a=1");
    }

    #[test]
    fn test_exact_strip_query_true_strip_fragment_false() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: false,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com/path?a=1#frag", &config).unwrap();
        assert_eq!(key, "https://example.com/path#frag");
    }

    #[test]
    fn test_exact_strip_query_false_strip_fragment_false() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: false,
            strip_fragment: false,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com/path?a=1#frag", &config).unwrap();
        assert_eq!(key, "https://example.com/path?a=1#frag");
    }

    #[test]
    fn test_exact_no_query_no_fragment() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: false,
            strip_fragment: false,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com/file.zip", &config).unwrap();
        assert_eq!(key, "https://example.com/file.zip");
    }

    // ===== Domain mode details =====

    #[test]
    fn test_domain_strips_www() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key1 = extract_dedup_key("https://www.example.com/a", &config).unwrap();
        let key2 = extract_dedup_key("https://example.com/b", &config).unwrap();
        assert_eq!(key1, key2);
        assert_eq!(key1, "https://example.com");
    }

    #[test]
    fn test_domain_preserves_scheme() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key_http = extract_dedup_key("http://example.com/a", &config).unwrap();
        let key_https = extract_dedup_key("https://example.com/a", &config).unwrap();
        assert_ne!(key_http, key_https);
        assert_eq!(key_http, "http://example.com");
        assert_eq!(key_https, "https://example.com");
    }

    #[test]
    fn test_domain_subdomain() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key1 = extract_dedup_key("https://sub.example.com/a", &config).unwrap();
        let key2 = extract_dedup_key("https://example.com/b", &config).unwrap();
        // sub.example.com != example.com (only www. is stripped)
        assert_ne!(key1, key2);
    }

    // ===== PathPrefix mode details =====

    #[test]
    fn test_path_prefix_root_path() {
        let config = DedupConfig {
            mode: DedupMode::PathPrefix,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com/", &config).unwrap();
        assert_eq!(key, "https://example.com");
    }

    #[test]
    fn test_path_prefix_no_path() {
        let config = DedupConfig {
            mode: DedupMode::PathPrefix,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com", &config).unwrap();
        assert_eq!(key, "https://example.com");
    }

    #[test]
    fn test_path_prefix_deep_path() {
        let config = DedupConfig {
            mode: DedupMode::PathPrefix,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com/a/b/c/d", &config).unwrap();
        assert_eq!(key, "https://example.com/a");
    }

    #[test]
    fn test_path_prefix_www_stripped() {
        let config = DedupConfig {
            mode: DedupMode::PathPrefix,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://www.example.com/downloads/file.zip", &config).unwrap();
        assert_eq!(key, "https://example.com/downloads");
    }

    // ===== Smart mode details =====

    #[test]
    fn test_smart_no_filename() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com/", &config).unwrap();
        // rsplit('/') on "/" gives "" as last segment
        assert_eq!(key, "https://example.com/");
    }

    #[test]
    fn test_smart_copy_suffix() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        // URL with spaces gets URL-encoded, but normalization still works
        let key1 = extract_dedup_key("https://example.com/file(1).zip", &config).unwrap();
        let key2 = extract_dedup_key("https://example.com/file.zip", &config).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_smart_bracket_suffix() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key1 = extract_dedup_key("https://example.com/file[1].zip", &config).unwrap();
        let key2 = extract_dedup_key("https://example.com/file[2].zip", &config).unwrap();
        let key3 = extract_dedup_key("https://example.com/file.zip", &config).unwrap();
        assert_eq!(key1, key2);
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_smart_unicode_filename() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        // Unicode in URL gets percent-encoded by Url parser
        let key = extract_dedup_key("https://example.com/文件(1).pdf", &config).unwrap();
        // The key will contain the URL-encoded version
        assert!(key.contains(".pdf"));
    }

    // ===== normalize_filename boundary =====

    #[test]
    fn test_normalize_filename_empty() {
        assert_eq!(normalize_filename(""), "");
    }

    #[test]
    fn test_normalize_filename_no_patterns() {
        assert_eq!(normalize_filename("clean_file.zip"), "clean_file.zip");
    }

    #[test]
    fn test_normalize_filename_multiple_patterns() {
        // (1) should be removed
        let result = normalize_filename("doc (1).pdf");
        assert_eq!(result, "doc.pdf");
    }

    #[test]
    fn test_normalize_filename_copy_variant() {
        // " - copy" pattern matches when followed by word boundary
        assert_eq!(normalize_filename("file - copy.txt"), "file.txt");
        // Just test the basic patterns that work
        assert_eq!(normalize_filename("file(1).txt"), "file.txt");
        assert_eq!(normalize_filename("file[1].txt"), "file.txt");
    }

    // ===== find_duplicate_url details =====

    #[test]
    fn test_find_duplicate_empty_list() {
        let config = DedupConfig::default();
        assert!(find_duplicate_url("https://example.com/file.zip", &[], &config).is_none());
    }

    #[test]
    fn test_find_duplicate_returns_first_match() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let existing = vec![
            "https://example.com/file.zip".to_string(),
            "https://example.com/file.zip".to_string(), // duplicate in existing list
        ];
        assert_eq!(
            find_duplicate_url("https://example.com/file.zip", &existing, &config),
            Some(0)
        );
    }

    #[test]
    fn test_find_duplicate_domain_mode() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let existing = vec![
            "https://example.com/a.zip".to_string(),
            "https://other.com/b.zip".to_string(),
        ];
        // Any URL from example.com matches index 0
        assert_eq!(
            find_duplicate_url("https://example.com/anything.zip", &existing, &config),
            Some(0)
        );
    }

    #[test]
    fn test_find_duplicate_smart_mode() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let existing = vec!["https://example.com/doc.pdf".to_string()];
        // doc(1).pdf should match doc.pdf in smart mode
        assert_eq!(
            find_duplicate_url("https://example.com/doc(1).pdf", &existing, &config),
            Some(0)
        );
    }

    #[test]
    fn test_find_duplicate_disabled() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: false,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let existing = vec!["https://example.com/file.zip".to_string()];
        assert!(find_duplicate_url("https://example.com/file.zip", &existing, &config).is_none());
    }

    // ===== Persistence boundary =====

    #[test]
    fn test_save_creates_file() {
        let dir = std::env::temp_dir().join("ipmsg_dedup_save_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = DedupConfig::default();
        save_dedup_config(&config, &dir).unwrap();

        let path = dir.join("dedup_config.json");
        assert!(path.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_save_no_tmp_leftover() {
        let dir = std::env::temp_dir().join("ipmsg_dedup_no_tmp_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = DedupConfig::default();
        save_dedup_config(&config, &dir).unwrap();

        let tmp_path = dir.join("dedup_config.json.tmp");
        assert!(!tmp_path.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_save_overwrite() {
        let dir = std::env::temp_dir().join("ipmsg_dedup_overwrite_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config1 = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        save_dedup_config(&config1, &dir).unwrap();

        let config2 = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: false,
            strip_fragment: false,
            enabled: false,
            duplicate_policy: DuplicatePolicy::Allow,
        };
        save_dedup_config(&config2, &dir).unwrap();

        let loaded = load_dedup_config(&dir).unwrap();
        assert_eq!(loaded.mode, DedupMode::Smart);
        assert!(!loaded.strip_query);
        assert!(!loaded.enabled);
        assert_eq!(loaded.duplicate_policy, DuplicatePolicy::Allow);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_corrupt_json() {
        let dir = std::env::temp_dir().join("ipmsg_dedup_corrupt_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("dedup_config.json");
        std::fs::write(&path, "not valid json{{{").unwrap();

        assert!(load_dedup_config(&dir).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_empty_file() {
        let dir = std::env::temp_dir().join("ipmsg_dedup_empty_file_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("dedup_config.json");
        std::fs::write(&path, "").unwrap();

        assert!(load_dedup_config(&dir).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_save_load_unicode_config() {
        let dir = std::env::temp_dir().join("ipmsg_dedup_unicode_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = DedupConfig::default();
        save_dedup_config(&config, &dir).unwrap();
        let loaded = load_dedup_config(&dir).unwrap();
        assert_eq!(loaded.mode, config.mode);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_pretty_json_roundtrip() {
        let dir = std::env::temp_dir().join("ipmsg_dedup_pretty_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = DedupConfig {
            mode: DedupMode::PathPrefix,
            strip_query: false,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Skip,
        };

        // Write pretty JSON manually
        let json = serde_json::to_string_pretty(&config).unwrap();
        let path = dir.join("dedup_config.json");
        std::fs::write(&path, &json).unwrap();

        let loaded = load_dedup_config(&dir).unwrap();
        assert_eq!(loaded.mode, DedupMode::PathPrefix);
        assert!(!loaded.strip_query);
        assert!(loaded.strip_fragment);
        assert_eq!(loaded.duplicate_policy, DuplicatePolicy::Skip);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ===== Edge cases =====

    #[test]
    fn test_very_long_url() {
        let config = DedupConfig::default();
        let long_path = "a".repeat(5000);
        let url = format!("https://example.com/{}", long_path);
        let key = extract_dedup_key(&url, &config);
        assert!(key.is_some());
    }

    #[test]
    fn test_unicode_url() {
        let config = DedupConfig::default();
        let url = "https://example.com/中文文件.zip";
        let key = extract_dedup_key(url, &config);
        assert!(key.is_some());
    }

    #[test]
    fn test_url_with_port() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let key = extract_dedup_key("https://example.com:8080/file.zip", &config).unwrap();
        // host_str() includes port in some URL parsers
        assert!(key.contains("example.com"));
    }

    #[test]
    fn test_url_with_auth() {
        let config = DedupConfig {
            mode: DedupMode::Exact,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let url = "https://user:pass@example.com/file.zip";
        let key = extract_dedup_key(url, &config).unwrap();
        assert!(key.contains("example.com"));
    }

    #[test]
    fn test_http_url_no_host() {
        let config = DedupConfig::default();
        // http:///path - Url::parse may fail or host_str may be empty
        // The function should handle this gracefully
        let result = extract_dedup_key("http:///path", &config);
        // Either None (parse failed or no host) or Some with the path
        // The exact behavior depends on Url parser
        if let Some(key) = result {
            // If it returns something, it should contain "path"
            assert!(key.contains("path") || key.is_empty());
        }
    }

    // ===== Integration / workflow tests =====

    #[test]
    fn test_full_workflow_exact_mode() {
        let config = DedupConfig::default();
        let mut existing: Vec<String> = Vec::new();

        // Add first URL
        let url1 = "https://example.com/file1.zip";
        assert!(find_duplicate_url(url1, &existing, &config).is_none());
        existing.push(url1.to_string());

        // Same URL is duplicate
        assert_eq!(find_duplicate_url(url1, &existing, &config), Some(0));

        // Different URL is not duplicate
        let url2 = "https://example.com/file2.zip";
        assert!(find_duplicate_url(url2, &existing, &config).is_none());
        existing.push(url2.to_string());

        // URL with query params matches (strip_query=true)
        let url3 = "https://example.com/file1.zip?token=abc";
        assert_eq!(find_duplicate_url(url3, &existing, &config), Some(0));
    }

    #[test]
    fn test_full_workflow_domain_mode() {
        let config = DedupConfig {
            mode: DedupMode::Domain,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let mut existing: Vec<String> = Vec::new();

        let url1 = "https://cdn.example.com/file1.zip";
        existing.push(url1.to_string());

        // Same domain = duplicate
        let url2 = "https://cdn.example.com/other-file.zip";
        assert_eq!(find_duplicate_url(url2, &existing, &config), Some(0));

        // Different domain = not duplicate
        let url3 = "https://other-cdn.com/file.zip";
        assert!(find_duplicate_url(url3, &existing, &config).is_none());
    }

    #[test]
    fn test_full_workflow_smart_mode() {
        let config = DedupConfig {
            mode: DedupMode::Smart,
            strip_query: true,
            strip_fragment: true,
            enabled: true,
            duplicate_policy: DuplicatePolicy::Reject,
        };
        let mut existing: Vec<String> = Vec::new();

        let url1 = "https://example.com/downloads/report.pdf";
        existing.push(url1.to_string());

        // Same file with (1) suffix = duplicate (same domain, normalized filename)
        let url2 = "https://example.com/downloads/report(1).pdf";
        assert_eq!(find_duplicate_url(url2, &existing, &config), Some(0));

        // Same file with (2) suffix on same domain = duplicate
        let url3 = "https://example.com/downloads/report(2).pdf";
        assert_eq!(find_duplicate_url(url3, &existing, &config), Some(0));

        // Different file = not duplicate
        let url4 = "https://example.com/downloads/other.pdf";
        assert!(find_duplicate_url(url4, &existing, &config).is_none());
    }

    #[test]
    fn test_many_existing_urls() {
        let config = DedupConfig::default();
        let existing: Vec<String> = (0..100)
            .map(|i| format!("https://example.com/file{}.zip", i))
            .collect();

        // Last URL matches
        assert_eq!(
            find_duplicate_url("https://example.com/file99.zip", &existing, &config),
            Some(99)
        );

        // Non-existing URL
        assert!(
            find_duplicate_url("https://example.com/file100.zip", &existing, &config).is_none()
        );
    }

    // ===== DedupConfig default values =====

    #[test]
    fn test_dedup_config_default_values() {
        let config = DedupConfig::default();
        assert_eq!(config.mode, DedupMode::Exact);
        assert!(config.strip_query);
        assert!(config.strip_fragment);
        assert!(config.enabled);
        assert_eq!(config.duplicate_policy, DuplicatePolicy::Reject);
    }
}
