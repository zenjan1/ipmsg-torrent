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
}
