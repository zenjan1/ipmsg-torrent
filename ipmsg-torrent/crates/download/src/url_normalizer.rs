//! URL Normalization for Download URLs
//!
//! Automatically clean and normalize URLs before adding them as download tasks.
//! This helps with deduplication and ensures consistent URL handling.
//!
//! Features:
//! - Trim whitespace and control characters
//! - Normalize protocol (HTTP/HTTPS)
//! - Remove common tracking parameters (UTM, fbclid, etc.)
//! - Normalize host (lowercase, remove www prefix)
//! - Remove trailing slashes from paths
//! - Sort query parameters for consistent comparison
//! - Configurable normalization rules

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use url::Url;

/// Configuration for URL normalization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlNormalizerConfig {
    /// Whether normalization is enabled
    pub enabled: bool,
    /// Whether to remove www prefix from hosts
    pub remove_www: bool,
    /// Whether to normalize HTTP to HTTPS
    pub prefer_https: bool,
    /// Whether to remove tracking parameters
    pub remove_tracking_params: bool,
    /// Whether to sort query parameters
    pub sort_query_params: bool,
    /// Whether to remove trailing slashes
    pub remove_trailing_slash: bool,
    /// Additional tracking parameter names to remove (beyond defaults)
    pub extra_tracking_params: Vec<String>,
    /// Tracking parameters to preserve (override defaults)
    pub preserve_params: Vec<String>,
}

impl Default for UrlNormalizerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            remove_www: true,
            prefer_https: false, // Don't force HTTPS as some sites don't support it
            remove_tracking_params: true,
            sort_query_params: true,
            remove_trailing_slash: true,
            extra_tracking_params: Vec::new(),
            preserve_params: Vec::new(),
        }
    }
}

/// Default tracking parameters to remove
const DEFAULT_TRACKING_PARAMS: &[&str] = &[
    // UTM parameters
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    // Facebook
    "fbclid",
    "fb_action_ids",
    "fb_action_types",
    "fb_source",
    // Google
    "gclid",
    "gclsrc",
    "dclid",
    // Other common trackers
    "mc_cid",
    "mc_eid",
    "msclkid",
    "twclid",
    "li_fat_id",
    "_openstat",
    "yclid",
    // Session/referral
    "ref",
    "referer",
    "source",
];

/// Result of URL normalization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizationResult {
    /// The normalized URL
    pub normalized_url: String,
    /// Whether the URL was modified
    pub was_modified: bool,
    /// List of changes applied
    pub changes: Vec<NormalizationChange>,
}

/// Types of normalization changes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NormalizationChange {
    /// Whitespace was trimmed
    WhitespaceTrimmed,
    /// Protocol was changed (http -> https)
    ProtocolChanged,
    /// WWW prefix was removed
    WwwRemoved,
    /// Host was lowercased
    HostLowercased,
    /// Tracking parameters were removed
    TrackingParamsRemoved(Vec<String>),
    /// Query parameters were sorted
    QueryParamsSorted,
    /// Trailing slash was removed
    TrailingSlashRemoved,
    /// Fragment/anchor was removed
    FragmentRemoved,
}

/// URL Normalizer manager
#[derive(Debug, Clone)]
pub struct UrlNormalizer {
    config: UrlNormalizerConfig,
    tracking_params: HashSet<String>,
}

impl Default for UrlNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

impl UrlNormalizer {
    pub fn new() -> Self {
        let config = UrlNormalizerConfig::default();
        let tracking_params = Self::build_tracking_params(&config);
        Self {
            config,
            tracking_params,
        }
    }

    pub fn with_config(config: UrlNormalizerConfig) -> Self {
        let tracking_params = Self::build_tracking_params(&config);
        Self {
            config,
            tracking_params,
        }
    }

    fn build_tracking_params(config: &UrlNormalizerConfig) -> HashSet<String> {
        let mut params: HashSet<String> = DEFAULT_TRACKING_PARAMS
            .iter()
            .map(|s| s.to_string())
            .collect();

        // Add extra tracking params
        for param in &config.extra_tracking_params {
            params.insert(param.to_lowercase());
        }

        // Remove preserved params
        for param in &config.preserve_params {
            params.remove(&param.to_lowercase());
        }

        params
    }

    /// Update configuration
    pub fn set_config(&mut self, config: UrlNormalizerConfig) {
        self.tracking_params = Self::build_tracking_params(&config);
        self.config = config;
    }

    /// Get current configuration
    pub fn config(&self) -> &UrlNormalizerConfig {
        &self.config
    }

    /// Normalize a URL
    pub fn normalize(&self, url: &str) -> NormalizationResult {
        let mut changes = Vec::new();
        let mut current = url.to_string();

        // Step 1: Trim whitespace
        let trimmed = url.trim().to_string();
        if trimmed != url {
            changes.push(NormalizationChange::WhitespaceTrimmed);
            current = trimmed;
        }

        // Remove control characters
        let cleaned: String = current.chars().filter(|c| !c.is_control()).collect();
        if cleaned != current {
            changes.push(NormalizationChange::WhitespaceTrimmed);
            current = cleaned;
        }

        // Special handling for non-HTTP URLs (magnet, ed2k, etc.)
        let lower = current.to_lowercase();
        if lower.starts_with("magnet:")
            || lower.starts_with("ed2k://")
            || lower.starts_with("btih:")
        {
            // These protocols should pass through with minimal normalization
            return NormalizationResult {
                normalized_url: current,
                was_modified: !changes.is_empty(),
                changes,
            };
        }

        if !self.config.enabled {
            return NormalizationResult {
                normalized_url: current,
                was_modified: !changes.is_empty(),
                changes,
            };
        }

        // Parse URL
        let mut parsed = match Url::parse(&current) {
            Ok(u) => u,
            Err(_) => {
                // Try adding https:// prefix
                if let Ok(u) = Url::parse(&format!("https://{}", current)) {
                    changes.push(NormalizationChange::ProtocolChanged);
                    u
                } else {
                    return NormalizationResult {
                        normalized_url: current,
                        was_modified: !changes.is_empty(),
                        changes,
                    };
                }
            }
        };

        // Step 2: Normalize protocol (http -> https)
        if self.config.prefer_https && parsed.scheme() == "http" {
            let _ = parsed.set_scheme("https");
            changes.push(NormalizationChange::ProtocolChanged);
        }

        // Step 3: Normalize host
        let host = parsed.host_str().unwrap_or("").to_lowercase();
        let mut new_host = host.clone();

        if self.config.remove_www && new_host.starts_with("www.") {
            new_host = new_host[4..].to_string();
            changes.push(NormalizationChange::WwwRemoved);
        }

        if new_host != parsed.host_str().unwrap_or("") {
            let _ = parsed.set_host(Some(&new_host));
            if !changes
                .iter()
                .any(|c| matches!(c, NormalizationChange::WwwRemoved))
            {
                changes.push(NormalizationChange::HostLowercased);
            }
        }

        // Step 4: Remove fragment
        if parsed.fragment().is_some() {
            parsed.set_fragment(None);
            changes.push(NormalizationChange::FragmentRemoved);
        }

        // Step 5: Remove tracking parameters and sort query params
        if self.config.remove_tracking_params || self.config.sort_query_params {
            let query_pairs: Vec<(String, String)> = parsed
                .query_pairs()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

            let mut removed_tracking = Vec::new();
            let mut filtered_pairs: Vec<(String, String)> = Vec::new();

            for (key, value) in query_pairs {
                if self.config.remove_tracking_params
                    && self.tracking_params.contains(&key.to_lowercase())
                {
                    removed_tracking.push(key);
                } else {
                    filtered_pairs.push((key, value));
                }
            }

            if !removed_tracking.is_empty() {
                changes.push(NormalizationChange::TrackingParamsRemoved(removed_tracking));
            }

            if self.config.sort_query_params {
                let original_order: Vec<_> = filtered_pairs.clone();
                filtered_pairs.sort_by(|a, b| a.0.cmp(&b.0));
                if filtered_pairs != original_order {
                    changes.push(NormalizationChange::QueryParamsSorted);
                }
            }

            // Rebuild query string
            parsed.query_pairs_mut().clear();
            for (key, value) in filtered_pairs {
                parsed.query_pairs_mut().append_pair(&key, &value);
            }

            // If query string is now empty, remove the trailing '?'
            let url_str = parsed.to_string();
            if url_str.ends_with('?')
                && let Ok(stripped) = Url::parse(&url_str[..url_str.len() - 1])
            {
                parsed = stripped;
            }
        }

        // Step 6: Remove trailing slash from path
        if self.config.remove_trailing_slash {
            let path = parsed.path().to_string();
            if path.len() > 1 && path.ends_with('/') {
                let new_path = path.trim_end_matches('/');
                parsed.set_path(new_path);
                changes.push(NormalizationChange::TrailingSlashRemoved);
            }
        }

        let normalized_url = parsed.to_string();
        let was_modified = normalized_url != url;

        NormalizationResult {
            normalized_url,
            was_modified,
            changes,
        }
    }

    /// Check if two URLs are equivalent after normalization
    pub fn are_equivalent(&self, url1: &str, url2: &str) -> bool {
        let norm1 = self.normalize(url1);
        let norm2 = self.normalize(url2);
        norm1.normalized_url == norm2.normalized_url
    }
}

/// Save URL normalizer config to disk
pub fn save_url_normalizer_config(
    config: &UrlNormalizerConfig,
    data_dir: &Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("url_normalizer_config.json");
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;

    // Atomic write
    let temp_path = path.with_extension("json.tmp");
    std::fs::write(&temp_path, json)?;
    std::fs::rename(&temp_path, &path)?;
    Ok(())
}

/// Load URL normalizer config from disk
pub fn load_url_normalizer_config(data_dir: &Path) -> Option<UrlNormalizerConfig> {
    let path = data_dir.join("url_normalizer_config.json");
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = UrlNormalizerConfig::default();
        assert!(config.enabled);
        assert!(config.remove_www);
        assert!(!config.prefer_https);
        assert!(config.remove_tracking_params);
        assert!(config.sort_query_params);
        assert!(config.remove_trailing_slash);
    }

    #[test]
    fn test_normalize_trim_whitespace() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("  https://example.com/file.zip  ");
        assert!(
            result
                .changes
                .contains(&NormalizationChange::WhitespaceTrimmed)
        );
        assert_eq!(result.normalized_url, "https://example.com/file.zip");
    }

    #[test]
    fn test_normalize_remove_www() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("https://www.example.com/file.zip");
        assert!(result.changes.contains(&NormalizationChange::WwwRemoved));
        assert_eq!(result.normalized_url, "https://example.com/file.zip");
    }

    #[test]
    fn test_normalize_keep_www_when_disabled() {
        let mut config = UrlNormalizerConfig::default();
        config.remove_www = false;
        let normalizer = UrlNormalizer::with_config(config);
        let result = normalizer.normalize("https://www.example.com/file.zip");
        assert!(!result.changes.contains(&NormalizationChange::WwwRemoved));
        assert_eq!(result.normalized_url, "https://www.example.com/file.zip");
    }

    #[test]
    fn test_normalize_http_to_https() {
        let mut config = UrlNormalizerConfig::default();
        config.prefer_https = true;
        let normalizer = UrlNormalizer::with_config(config);
        let result = normalizer.normalize("http://example.com/file.zip");
        assert!(
            result
                .changes
                .contains(&NormalizationChange::ProtocolChanged)
        );
        assert_eq!(result.normalized_url, "https://example.com/file.zip");
    }

    #[test]
    fn test_normalize_remove_tracking_params() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize(
            "https://example.com/file.zip?utm_source=google&utm_medium=cpc&real_param=value",
        );
        assert!(
            result
                .changes
                .iter()
                .any(|c| matches!(c, NormalizationChange::TrackingParamsRemoved(_)))
        );
        assert!(result.normalized_url.contains("real_param=value"));
        assert!(!result.normalized_url.contains("utm_source"));
        assert!(!result.normalized_url.contains("utm_medium"));
    }

    #[test]
    fn test_normalize_remove_fbclid() {
        let normalizer = UrlNormalizer::new();
        let result =
            normalizer.normalize("https://example.com/file.zip?fbclid=abc123&download=true");
        assert!(!result.normalized_url.contains("fbclid"));
        assert!(result.normalized_url.contains("download=true"));
    }

    #[test]
    fn test_normalize_sort_query_params() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("https://example.com/file.zip?z=1&a=2&m=3");
        assert!(
            result
                .changes
                .contains(&NormalizationChange::QueryParamsSorted)
        );
        // Check that params are in alphabetical order
        let url = Url::parse(&result.normalized_url).unwrap();
        let params: Vec<_> = url.query_pairs().map(|(k, _)| k.to_string()).collect();
        assert_eq!(params, vec!["a", "m", "z"]);
    }

    #[test]
    fn test_normalize_remove_trailing_slash() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("https://example.com/downloads/");
        assert!(
            result
                .changes
                .contains(&NormalizationChange::TrailingSlashRemoved)
        );
        assert_eq!(result.normalized_url, "https://example.com/downloads");
    }

    #[test]
    fn test_normalize_keep_trailing_slash_for_root() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("https://example.com/");
        // Root path "/" should not have trailing slash removed
        assert!(
            !result
                .changes
                .contains(&NormalizationChange::TrailingSlashRemoved)
        );
    }

    #[test]
    fn test_normalize_remove_fragment() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("https://example.com/file.zip#section");
        assert!(
            result
                .changes
                .contains(&NormalizationChange::FragmentRemoved)
        );
        assert!(!result.normalized_url.contains("#section"));
    }

    #[test]
    fn test_normalize_lowercase_host() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("https://EXAMPLE.COM/file.zip");
        assert!(result.was_modified);
        assert!(result.normalized_url.contains("example.com"));
    }

    #[test]
    fn test_normalize_disabled() {
        let mut config = UrlNormalizerConfig::default();
        config.enabled = false;
        let normalizer = UrlNormalizer::with_config(config);
        let result = normalizer.normalize("  https://www.EXAMPLE.com/file.zip?utm_source=x  ");
        // Should only trim whitespace when disabled
        assert!(result.was_modified);
        assert!(
            result
                .changes
                .contains(&NormalizationChange::WhitespaceTrimmed)
        );
        assert!(!result.changes.contains(&NormalizationChange::WwwRemoved));
    }

    #[test]
    fn test_normalize_invalid_url() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("not a url at all :::");
        // Should return as-is (with possible whitespace trim)
        assert!(!result.normalized_url.is_empty());
    }

    #[test]
    fn test_normalize_add_https_prefix() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize("example.com/file.zip");
        // Should try to add https:// prefix
        assert!(result.normalized_url.starts_with("https://"));
        assert!(
            result
                .changes
                .contains(&NormalizationChange::ProtocolChanged)
        );
    }

    #[test]
    fn test_normalize_preserve_params() {
        let mut config = UrlNormalizerConfig::default();
        config.preserve_params = vec!["utm_source".to_string()];
        let normalizer = UrlNormalizer::with_config(config);
        let result = normalizer
            .normalize("https://example.com/file.zip?utm_source=keep_this&utm_medium=remove_this");
        assert!(result.normalized_url.contains("utm_source=keep_this"));
        assert!(!result.normalized_url.contains("utm_medium"));
    }

    #[test]
    fn test_normalize_extra_tracking_params() {
        let mut config = UrlNormalizerConfig::default();
        config.extra_tracking_params = vec!["my_tracker".to_string()];
        let normalizer = UrlNormalizer::with_config(config);
        let result = normalizer.normalize("https://example.com/file.zip?my_tracker=xyz&keep=me");
        assert!(!result.normalized_url.contains("my_tracker"));
        assert!(result.normalized_url.contains("keep=me"));
    }

    #[test]
    fn test_are_equivalent() {
        let normalizer = UrlNormalizer::new();

        // Same URL with different tracking params
        assert!(normalizer.are_equivalent(
            "https://example.com/file.zip?utm_source=google",
            "https://example.com/file.zip?utm_source=facebook"
        ));

        // Same URL with/without www
        assert!(normalizer.are_equivalent(
            "https://www.example.com/file.zip",
            "https://example.com/file.zip"
        ));

        // Same URL with different param order
        assert!(normalizer.are_equivalent(
            "https://example.com/file.zip?a=1&b=2",
            "https://example.com/file.zip?b=2&a=1"
        ));

        // Different files
        assert!(!normalizer.are_equivalent(
            "https://example.com/file1.zip",
            "https://example.com/file2.zip"
        ));
    }

    #[test]
    fn test_normalize_complex_url() {
        let normalizer = UrlNormalizer::new();
        let result = normalizer.normalize(
            "  https://www.EXAMPLE.com/downloads/file.zip?fbclid=abc&utm_source=google&version=1.0#readme  "
        );

        assert!(result.was_modified);
        assert!(
            result
                .changes
                .contains(&NormalizationChange::WhitespaceTrimmed)
        );
        assert!(result.changes.contains(&NormalizationChange::WwwRemoved));
        assert!(
            result
                .changes
                .iter()
                .any(|c| matches!(c, NormalizationChange::TrackingParamsRemoved(_)))
        );
        assert!(
            result
                .changes
                .contains(&NormalizationChange::FragmentRemoved)
        );

        let url = Url::parse(&result.normalized_url).unwrap();
        assert_eq!(url.host_str(), Some("example.com"));
        assert!(url.query().unwrap().contains("version=1.0"));
        assert!(!url.query().unwrap().contains("fbclid"));
        assert!(!url.query().unwrap().contains("utm_source"));
        assert!(url.fragment().is_none());
    }

    #[test]
    fn test_save_load_config() {
        let temp_dir = std::env::temp_dir().join("test_url_normalizer_save_load");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut config = UrlNormalizerConfig::default();
        config.remove_www = false;
        config.extra_tracking_params = vec!["custom_param".to_string()];

        save_url_normalizer_config(&config, &temp_dir).unwrap();

        let loaded = load_url_normalizer_config(&temp_dir).unwrap();
        assert!(!loaded.remove_www);
        assert_eq!(loaded.extra_tracking_params, vec!["custom_param"]);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_load_config_nonexistent() {
        let temp_dir = std::env::temp_dir().join("test_url_normalizer_nonexistent");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let loaded = load_url_normalizer_config(&temp_dir);
        assert!(loaded.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_normalize_magnet_link() {
        let normalizer = UrlNormalizer::new();
        let magnet = "magnet:?xt=urn:btih:abc123&dn=Test+File";
        let result = normalizer.normalize(magnet);
        // Magnet links should be handled gracefully
        assert!(result.normalized_url.starts_with("magnet:"));
    }

    #[test]
    fn test_normalize_ed2k_link() {
        let normalizer = UrlNormalizer::new();
        let ed2k = "ed2k://|file|test.zip|1000|abc123|/";
        let result = normalizer.normalize(ed2k);
        // Ed2k links should pass through mostly unchanged
        assert!(result.normalized_url.starts_with("ed2k://"));
    }
}
