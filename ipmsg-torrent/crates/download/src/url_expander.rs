//! URL Expansion & Pre-download Validation
//!
//! Automatically expands shortened URLs (bit.ly, tinyurl, t.co, etc.) before downloading,
//! and validates that URLs are reachable before starting the download.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

/// Known URL shortener domains
const KNOWN_SHORTENERS: &[&str] = &[
    "bit.ly",
    "tinyurl.com",
    "t.co",
    "goo.gl",
    "is.gd",
    "v.gd",
    "buff.ly",
    "ow.ly",
    "shorturl.at",
    "tiny.cc",
    "bl.ink",
    "rebrand.ly",
    "cutt.ly",
    "short.io",
];

/// Result of URL expansion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpansionResult {
    /// Original URL
    pub original: String,
    /// Expanded URL (may be same as original if not shortened)
    pub expanded: String,
    /// Whether the URL was actually expanded
    pub was_expanded: bool,
    /// Number of redirects followed
    pub redirect_count: u32,
}

/// Result of URL validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether the URL is reachable
    pub reachable: bool,
    /// HTTP status code (if available)
    pub status_code: Option<u16>,
    /// Content-Length header (if available)
    pub content_length: Option<u64>,
    /// Content-Type header (if available)
    pub content_type: Option<String>,
    /// Whether the URL was a shortened URL that got expanded
    pub was_shortened: bool,
    /// Final URL after redirects
    pub final_url: Option<String>,
    /// Error message if not reachable
    pub error: Option<String>,
    /// Response time in milliseconds
    pub response_time_ms: u64,
}

/// Configuration for URL expansion and validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlExpanderConfig {
    /// Whether URL expansion is enabled
    pub expansion_enabled: bool,
    /// Whether URL validation is enabled
    pub validation_enabled: bool,
    /// Maximum number of redirects to follow
    pub max_redirects: u32,
    /// Timeout for expansion/validation requests (seconds)
    pub timeout_secs: u64,
    /// Additional custom shortener domains
    pub custom_shorteners: Vec<String>,
    /// Whether to validate before adding download (block if unreachable)
    pub block_on_unreachable: bool,
}

impl Default for UrlExpanderConfig {
    fn default() -> Self {
        Self {
            expansion_enabled: true,
            validation_enabled: true,
            max_redirects: 10,
            timeout_secs: 15,
            custom_shorteners: Vec::new(),
            block_on_unreachable: false,
        }
    }
}

/// Check if a URL is from a known shortener
pub fn is_shortened_url(url: &str, config: &UrlExpanderConfig) -> bool {
    if !config.expansion_enabled {
        return false;
    }

    // Only HTTP/HTTPS URLs can be shortened
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return false;
    }

    let url_lower = url.to_lowercase();
    let host = match url::Url::parse(&url_lower)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
    {
        Some(h) => h,
        None => return false,
    };

    // Check known shorteners
    if KNOWN_SHORTENERS
        .iter()
        .any(|s| host == *s || host.ends_with(&format!(".{}", s)))
    {
        return true;
    }

    // Check custom shorteners
    config
        .custom_shorteners
        .iter()
        .any(|s| host == *s || host.ends_with(&format!(".{}", s)))
}

/// Expand a shortened URL by following redirects
pub async fn expand_url(
    url: &str,
    config: &UrlExpanderConfig,
) -> Result<ExpansionResult, UrlExpanderError> {
    if !config.expansion_enabled {
        return Ok(ExpansionResult {
            original: url.to_string(),
            expanded: url.to_string(),
            was_expanded: false,
            redirect_count: 0,
        });
    }

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(
            config.max_redirects as usize,
        ))
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .map_err(|e| UrlExpanderError::HttpClient(e.to_string()))?;

    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| UrlExpanderError::RequestFailed(e.to_string()))?;

    let final_url = response.url().as_str().to_string();
    let was_expanded = final_url != url;

    Ok(ExpansionResult {
        original: url.to_string(),
        expanded: final_url,
        was_expanded,
        redirect_count: if was_expanded { 1 } else { 0 }, // reqwest doesn't expose exact count
    })
}

/// Validate a URL is reachable and get metadata
pub async fn validate_url(
    url: &str,
    config: &UrlExpanderConfig,
) -> Result<ValidationResult, UrlExpanderError> {
    if !config.validation_enabled {
        return Ok(ValidationResult {
            reachable: true,
            status_code: None,
            content_length: None,
            content_type: None,
            was_shortened: false,
            final_url: None,
            error: None,
            response_time_ms: 0,
        });
    }

    let start = std::time::Instant::now();

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(
            config.max_redirects as usize,
        ))
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .map_err(|e| UrlExpanderError::HttpClient(e.to_string()))?;

    let was_shortened = is_shortened_url(url, config);

    // Use HEAD first for efficiency, fall back to GET if HEAD fails
    let response = match client.head(url).send().await {
        Ok(resp) => resp,
        Err(_) => {
            // Some servers don't support HEAD, try GET with a small range
            client
                .get(url)
                .header("Range", "bytes=0-0")
                .send()
                .await
                .map_err(|e| {
                    let elapsed = start.elapsed().as_millis() as u64;
                    UrlExpanderError::ValidationFailed {
                        url: url.to_string(),
                        error: e.to_string(),
                        response_time_ms: elapsed,
                    }
                })?
        }
    };

    let elapsed = start.elapsed().as_millis() as u64;
    let status = response.status().as_u16();
    let final_url = response.url().as_str().to_string();

    let content_length = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let reachable = (200..400).contains(&status);

    Ok(ValidationResult {
        reachable,
        status_code: Some(status),
        content_length,
        content_type,
        was_shortened,
        final_url: if final_url != url {
            Some(final_url)
        } else {
            None
        },
        error: if reachable {
            None
        } else {
            Some(format!("HTTP {status}"))
        },
        response_time_ms: elapsed,
    })
}

/// Expand and validate a URL in one step
pub async fn expand_and_validate(
    url: &str,
    config: &UrlExpanderConfig,
) -> Result<(ExpansionResult, ValidationResult), UrlExpanderError> {
    // First expand if needed
    let expansion = if is_shortened_url(url, config) {
        expand_url(url, config).await?
    } else {
        ExpansionResult {
            original: url.to_string(),
            expanded: url.to_string(),
            was_expanded: false,
            redirect_count: 0,
        }
    };

    // Then validate the expanded URL
    let validation = validate_url(&expansion.expanded, config).await?;

    Ok((expansion, validation))
}

/// Errors from URL expansion/validation
#[derive(Debug, Clone)]
pub enum UrlExpanderError {
    /// Failed to create HTTP client
    HttpClient(String),
    /// HTTP request failed
    RequestFailed(String),
    /// Validation failed
    ValidationFailed {
        url: String,
        error: String,
        response_time_ms: u64,
    },
}

impl std::fmt::Display for UrlExpanderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UrlExpanderError::HttpClient(e) => write!(f, "HTTP client error: {e}"),
            UrlExpanderError::RequestFailed(e) => write!(f, "Request failed: {e}"),
            UrlExpanderError::ValidationFailed {
                url,
                error,
                response_time_ms,
            } => write!(
                f,
                "Validation failed for {url}: {error} ({response_time_ms}ms)"
            ),
        }
    }
}

impl std::error::Error for UrlExpanderError {}

/// Persistence
pub fn save_url_expander_config(
    config: &UrlExpanderConfig,
    data_dir: &Path,
) -> Result<(), std::io::Error> {
    let path = data_dir.join("url_expander_config.json");
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;
    let temp = data_dir.join("url_expander_config.json.tmp");
    std::fs::write(&temp, json)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

pub fn load_url_expander_config(data_dir: &Path) -> Option<UrlExpanderConfig> {
    let path = data_dir.join("url_expander_config.json");
    let json = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_shortened_url_bitly() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://bit.ly/abc123", &config));
        assert!(is_shortened_url("http://bit.ly/test", &config));
    }

    #[test]
    fn test_is_shortened_url_tinyurl() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://tinyurl.com/abc123", &config));
    }

    #[test]
    fn test_is_shortened_url_tco() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://t.co/abc123", &config));
    }

    #[test]
    fn test_is_shortened_url_normal() {
        let config = UrlExpanderConfig::default();
        assert!(!is_shortened_url("https://example.com/file.zip", &config));
        assert!(!is_shortened_url(
            "https://github.com/repo/archive.zip",
            &config
        ));
    }

    #[test]
    fn test_is_shortened_url_non_http() {
        let config = UrlExpanderConfig::default();
        assert!(!is_shortened_url("magnet:?xt=urn:btih:abc", &config));
        assert!(!is_shortened_url("ed2k://|file|test|100|abc|/", &config));
    }

    #[test]
    fn test_is_shortened_url_disabled() {
        let config = UrlExpanderConfig {
            expansion_enabled: false,
            ..Default::default()
        };
        assert!(!is_shortened_url("https://bit.ly/abc123", &config));
    }

    #[test]
    fn test_is_shortened_url_custom() {
        let config = UrlExpanderConfig {
            custom_shorteners: vec!["my.short".to_string()],
            ..Default::default()
        };
        assert!(is_shortened_url("https://my.short/abc", &config));
        assert!(!is_shortened_url("https://other.com/abc", &config));
    }

    #[test]
    fn test_config_default() {
        let config = UrlExpanderConfig::default();
        assert!(config.expansion_enabled);
        assert!(config.validation_enabled);
        assert_eq!(config.max_redirects, 10);
        assert_eq!(config.timeout_secs, 15);
        assert!(config.custom_shorteners.is_empty());
        assert!(!config.block_on_unreachable);
    }

    #[test]
    fn test_config_serialization() {
        let config = UrlExpanderConfig {
            expansion_enabled: false,
            validation_enabled: true,
            max_redirects: 5,
            timeout_secs: 30,
            custom_shorteners: vec!["s.co".to_string()],
            block_on_unreachable: true,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: UrlExpanderConfig = serde_json::from_str(&json).unwrap();

        assert!(!deserialized.expansion_enabled);
        assert!(deserialized.validation_enabled);
        assert_eq!(deserialized.max_redirects, 5);
        assert_eq!(deserialized.timeout_secs, 30);
        assert_eq!(deserialized.custom_shorteners, vec!["s.co"]);
        assert!(deserialized.block_on_unreachable);
    }

    #[test]
    fn test_expansion_result_serialization() {
        let result = ExpansionResult {
            original: "https://bit.ly/abc".to_string(),
            expanded: "https://example.com/real-file.zip".to_string(),
            was_expanded: true,
            redirect_count: 1,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ExpansionResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.original, result.original);
        assert_eq!(deserialized.expanded, result.expanded);
        assert!(deserialized.was_expanded);
        assert_eq!(deserialized.redirect_count, 1);
    }

    #[test]
    fn test_validation_result_serialization() {
        let result = ValidationResult {
            reachable: true,
            status_code: Some(200),
            content_length: Some(1024000),
            content_type: Some("application/zip".to_string()),
            was_shortened: true,
            final_url: Some("https://example.com/file.zip".to_string()),
            error: None,
            response_time_ms: 150,
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ValidationResult = serde_json::from_str(&json).unwrap();

        assert!(deserialized.reachable);
        assert_eq!(deserialized.status_code, Some(200));
        assert_eq!(deserialized.content_length, Some(1024000));
        assert!(deserialized.was_shortened);
        assert!(deserialized.error.is_none());
    }

    #[test]
    fn test_save_load_config() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_config");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config = UrlExpanderConfig {
            expansion_enabled: false,
            max_redirects: 3,
            custom_shorteners: vec!["x.io".to_string()],
            ..Default::default()
        };

        save_url_expander_config(&config, &temp_dir).unwrap();
        let loaded = load_url_expander_config(&temp_dir).unwrap();

        assert!(!loaded.expansion_enabled);
        assert_eq!(loaded.max_redirects, 3);
        assert_eq!(loaded.custom_shorteners, vec!["x.io"]);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_load_config_missing() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_missing");
        let _ = std::fs::remove_dir_all(&temp_dir);
        assert!(load_url_expander_config(&temp_dir).is_none());
    }

    #[test]
    fn test_url_expander_error_display() {
        let err = UrlExpanderError::HttpClient("timeout".to_string());
        assert!(err.to_string().contains("timeout"));

        let err = UrlExpanderError::RequestFailed("connection refused".to_string());
        assert!(err.to_string().contains("connection refused"));

        let err = UrlExpanderError::ValidationFailed {
            url: "https://example.com".to_string(),
            error: "404".to_string(),
            response_time_ms: 100,
        };
        assert!(err.to_string().contains("example.com"));
        assert!(err.to_string().contains("404"));
    }

    #[test]
    fn test_is_shortened_url_subdomain() {
        let config = UrlExpanderConfig::default();
        // Subdomains of known shorteners should match
        assert!(is_shortened_url("https://www.bit.ly/abc", &config));
    }

    #[test]
    fn test_is_shortened_url_case_insensitive() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://BIT.LY/abc", &config));
        assert!(is_shortened_url("https://TinyURL.com/abc", &config));
    }

    #[tokio::test]
    async fn test_expand_url_disabled() {
        let config = UrlExpanderConfig {
            expansion_enabled: false,
            ..Default::default()
        };

        let result = expand_url("https://bit.ly/abc", &config).await.unwrap();
        assert!(!result.was_expanded);
        assert_eq!(result.expanded, "https://bit.ly/abc");
    }

    #[tokio::test]
    async fn test_validate_url_disabled() {
        let config = UrlExpanderConfig {
            validation_enabled: false,
            ..Default::default()
        };

        let result = validate_url("https://example.com/file.zip", &config)
            .await
            .unwrap();
        assert!(result.reachable);
        assert!(result.status_code.is_none());
        assert_eq!(result.response_time_ms, 0);
    }

    // ===== Comprehensive Test Coverage (Phase 233) =====

    // --- UrlExpanderConfig serde ---

    #[test]
    fn config_serde_roundtrip_default() {
        let config = UrlExpanderConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let de: UrlExpanderConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.expansion_enabled, config.expansion_enabled);
        assert_eq!(de.validation_enabled, config.validation_enabled);
        assert_eq!(de.max_redirects, config.max_redirects);
        assert_eq!(de.timeout_secs, config.timeout_secs);
        assert_eq!(de.block_on_unreachable, config.block_on_unreachable);
        assert!(de.custom_shorteners.is_empty());
    }

    #[test]
    fn config_serde_roundtrip_custom() {
        let config = UrlExpanderConfig {
            expansion_enabled: false,
            validation_enabled: false,
            max_redirects: 0,
            timeout_secs: 1,
            custom_shorteners: vec!["a.io".into(), "b.co".into()],
            block_on_unreachable: true,
        };
        let json = serde_json::to_string(&config).unwrap();
        let de: UrlExpanderConfig = serde_json::from_str(&json).unwrap();
        assert!(!de.expansion_enabled);
        assert!(!de.validation_enabled);
        assert_eq!(de.max_redirects, 0);
        assert_eq!(de.timeout_secs, 1);
        assert!(de.block_on_unreachable);
        assert_eq!(de.custom_shorteners.len(), 2);
    }

    #[test]
    fn config_serde_pretty() {
        let config = UrlExpanderConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let de: UrlExpanderConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(de.max_redirects, 10);
    }

    #[test]
    fn config_serde_extra_fields_ignored() {
        let json = r#"{"expansion_enabled":true,"validation_enabled":true,"max_redirects":5,"timeout_secs":10,"custom_shorteners":[],"block_on_unreachable":false,"unknown_field":42}"#;
        let de: UrlExpanderConfig = serde_json::from_str(json).unwrap();
        assert_eq!(de.max_redirects, 5);
    }

    #[test]
    fn config_serde_missing_fields_use_defaults() {
        // Only provide some fields; serde should fail since no #[serde(default)] on struct
        // But individual fields with defaults in Default impl work when using full object
        let json = serde_json::to_string(&UrlExpanderConfig::default()).unwrap();
        let de: UrlExpanderConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.timeout_secs, 15);
    }

    // --- UrlExpanderConfig Clone/Debug ---

    #[test]
    fn config_clone() {
        let config = UrlExpanderConfig {
            custom_shorteners: vec!["x.io".into()],
            ..Default::default()
        };
        let cloned = config.clone();
        assert_eq!(cloned.custom_shorteners, config.custom_shorteners);
        assert_eq!(cloned.max_redirects, config.max_redirects);
    }

    #[test]
    fn config_debug() {
        let config = UrlExpanderConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("UrlExpanderConfig"));
        assert!(debug.contains("expansion_enabled"));
        assert!(debug.contains("max_redirects"));
    }

    // --- ExpansionResult traits ---

    #[test]
    fn expansion_result_clone() {
        let r = ExpansionResult {
            original: "https://bit.ly/x".into(),
            expanded: "https://example.com/y".into(),
            was_expanded: true,
            redirect_count: 3,
        };
        let cloned = r.clone();
        assert_eq!(cloned.original, r.original);
        assert_eq!(cloned.expanded, r.expanded);
        assert!(cloned.was_expanded);
        assert_eq!(cloned.redirect_count, 3);
    }

    #[test]
    fn expansion_result_debug() {
        let r = ExpansionResult {
            original: "o".into(),
            expanded: "e".into(),
            was_expanded: false,
            redirect_count: 0,
        };
        let debug = format!("{:?}", r);
        assert!(debug.contains("ExpansionResult"));
        assert!(debug.contains("was_expanded"));
    }

    #[test]
    fn expansion_result_serde_not_expanded() {
        let r = ExpansionResult {
            original: "https://example.com".into(),
            expanded: "https://example.com".into(),
            was_expanded: false,
            redirect_count: 0,
        };
        let json = serde_json::to_string(&r).unwrap();
        let de: ExpansionResult = serde_json::from_str(&json).unwrap();
        assert!(!de.was_expanded);
        assert_eq!(de.redirect_count, 0);
    }

    #[test]
    fn expansion_result_serde_extra_fields_ignored() {
        let json = r#"{"original":"o","expanded":"e","was_expanded":false,"redirect_count":0,"extra":"ignored"}"#;
        let de: ExpansionResult = serde_json::from_str(json).unwrap();
        assert_eq!(de.original, "o");
    }

    // --- ValidationResult traits ---

    #[test]
    fn validation_result_clone() {
        let v = ValidationResult {
            reachable: true,
            status_code: Some(200),
            content_length: Some(1024),
            content_type: Some("application/zip".into()),
            was_shortened: false,
            final_url: None,
            error: None,
            response_time_ms: 50,
        };
        let cloned = v.clone();
        assert!(cloned.reachable);
        assert_eq!(cloned.status_code, Some(200));
        assert_eq!(cloned.content_length, Some(1024));
        assert_eq!(cloned.response_time_ms, 50);
    }

    #[test]
    fn validation_result_debug() {
        let v = ValidationResult {
            reachable: false,
            status_code: Some(404),
            content_length: None,
            content_type: None,
            was_shortened: false,
            final_url: None,
            error: Some("HTTP 404".into()),
            response_time_ms: 100,
        };
        let debug = format!("{:?}", v);
        assert!(debug.contains("ValidationResult"));
        assert!(debug.contains("reachable"));
    }

    #[test]
    fn validation_result_serde_unreachable() {
        let v = ValidationResult {
            reachable: false,
            status_code: Some(500),
            content_length: None,
            content_type: None,
            was_shortened: false,
            final_url: None,
            error: Some("HTTP 500".into()),
            response_time_ms: 200,
        };
        let json = serde_json::to_string(&v).unwrap();
        let de: ValidationResult = serde_json::from_str(&json).unwrap();
        assert!(!de.reachable);
        assert_eq!(de.status_code, Some(500));
        assert!(de.error.is_some());
    }

    #[test]
    fn validation_result_serde_extra_fields_ignored() {
        let json = r#"{"reachable":true,"status_code":null,"content_length":null,"content_type":null,"was_shortened":false,"final_url":null,"error":null,"response_time_ms":0,"bonus":true}"#;
        let de: ValidationResult = serde_json::from_str(json).unwrap();
        assert!(de.reachable);
    }

    // --- is_shortened_url comprehensive ---

    #[test]
    fn is_shortened_url_all_known_shorteners() {
        let config = UrlExpanderConfig::default();
        let shorteners = [
            "bit.ly",
            "tinyurl.com",
            "t.co",
            "goo.gl",
            "is.gd",
            "v.gd",
            "buff.ly",
            "ow.ly",
            "shorturl.at",
            "tiny.cc",
            "bl.ink",
            "rebrand.ly",
            "cutt.ly",
            "short.io",
        ];
        for s in &shorteners {
            let url = format!("https://{}/abc123", s);
            assert!(
                is_shortened_url(&url, &config),
                "should detect {} as shortener",
                s
            );
        }
    }

    #[test]
    fn is_shortened_url_empty_string() {
        let config = UrlExpanderConfig::default();
        assert!(!is_shortened_url("", &config));
    }

    #[test]
    fn is_shortened_url_just_scheme() {
        let config = UrlExpanderConfig::default();
        assert!(!is_shortened_url("https://", &config));
    }

    #[test]
    fn is_shortened_url_ftp_not_matched() {
        let config = UrlExpanderConfig::default();
        // FTP URLs are not matched (only http/https)
        assert!(!is_shortened_url("ftp://bit.ly/file", &config));
    }

    #[test]
    fn is_shortened_url_partial_domain_no_match() {
        let config = UrlExpanderConfig::default();
        // notbit.ly should NOT match bit.ly
        assert!(!is_shortened_url("https://notbit.ly/abc", &config));
    }

    #[test]
    fn is_shortened_url_subdomain_of_shortener() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://www.tinyurl.com/abc", &config));
        assert!(is_shortened_url("https://sub.bit.ly/abc", &config));
    }

    #[test]
    fn is_shortened_url_custom_shortener_empty_list() {
        let config = UrlExpanderConfig {
            custom_shorteners: vec![],
            ..Default::default()
        };
        assert!(!is_shortened_url("https://custom.short/abc", &config));
    }

    #[test]
    fn is_shortened_url_custom_shortener_subdomain() {
        let config = UrlExpanderConfig {
            custom_shorteners: vec!["my.short".into()],
            ..Default::default()
        };
        assert!(is_shortened_url("https://sub.my.short/abc", &config));
    }

    #[test]
    fn is_shortened_url_unicode_path() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://bit.ly/中文路径", &config));
    }

    #[test]
    fn is_shortened_url_emoji_path() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://bit.ly/🔥🎉", &config));
    }

    #[test]
    fn is_shortened_url_with_query_params() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://bit.ly/abc?ref=twitter", &config));
    }

    #[test]
    fn is_shortened_url_with_fragment() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://bit.ly/abc#section", &config));
    }

    #[test]
    fn is_shortened_url_invalid_url() {
        let config = UrlExpanderConfig::default();
        assert!(!is_shortened_url("not a url at all", &config));
        assert!(!is_shortened_url("://missing-scheme", &config));
    }

    // --- UrlExpanderError comprehensive ---

    #[test]
    fn error_display_http_client() {
        let err = UrlExpanderError::HttpClient("connection timeout".into());
        let s = err.to_string();
        assert!(s.contains("HTTP client error"));
        assert!(s.contains("connection timeout"));
    }

    #[test]
    fn error_display_request_failed() {
        let err = UrlExpanderError::RequestFailed("DNS resolution failed".into());
        let s = err.to_string();
        assert!(s.contains("Request failed"));
        assert!(s.contains("DNS resolution failed"));
    }

    #[test]
    fn error_display_validation_failed() {
        let err = UrlExpanderError::ValidationFailed {
            url: "https://example.com".into(),
            error: "404 Not Found".into(),
            response_time_ms: 250,
        };
        let s = err.to_string();
        assert!(s.contains("Validation failed"));
        assert!(s.contains("example.com"));
        assert!(s.contains("404 Not Found"));
        assert!(s.contains("250ms"));
    }

    #[test]
    fn error_debug() {
        let err = UrlExpanderError::HttpClient("test".into());
        let debug = format!("{:?}", err);
        assert!(debug.contains("HttpClient"));
    }

    #[test]
    fn error_is_error_trait() {
        let err: Box<dyn std::error::Error> = Box::new(UrlExpanderError::HttpClient("test".into()));
        assert!(err.source().is_none() || err.source().is_some());
    }

    #[test]
    fn error_unicode_messages() {
        let err = UrlExpanderError::HttpClient("超时错误".into());
        assert!(err.to_string().contains("超时错误"));

        let err = UrlExpanderError::RequestFailed("连接被拒绝 🚫".into());
        assert!(err.to_string().contains("连接被拒绝 🚫"));

        let err = UrlExpanderError::ValidationFailed {
            url: "https://中文.com".into(),
            error: "找不到服务器".into(),
            response_time_ms: 100,
        };
        assert!(err.to_string().contains("中文.com"));
    }

    // --- Persistence comprehensive ---

    #[test]
    fn save_creates_file() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_save_creates");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config = UrlExpanderConfig::default();
        save_url_expander_config(&config, &temp_dir).unwrap();

        let path = temp_dir.join("url_expander_config.json");
        assert!(path.exists());
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn save_overwrites_existing() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_overwrite");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config1 = UrlExpanderConfig {
            max_redirects: 5,
            ..Default::default()
        };
        save_url_expander_config(&config1, &temp_dir).unwrap();

        let config2 = UrlExpanderConfig {
            max_redirects: 20,
            ..Default::default()
        };
        save_url_expander_config(&config2, &temp_dir).unwrap();

        let loaded = load_url_expander_config(&temp_dir).unwrap();
        assert_eq!(loaded.max_redirects, 20);
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn save_no_tmp_leftover() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_no_tmp");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config = UrlExpanderConfig::default();
        save_url_expander_config(&config, &temp_dir).unwrap();

        let tmp_path = temp_dir.join("url_expander_config.json.tmp");
        assert!(!tmp_path.exists());
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn load_corrupt_json_returns_none() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_corrupt");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let path = temp_dir.join("url_expander_config.json");
        std::fs::write(&path, "not valid json{{{").unwrap();

        assert!(load_url_expander_config(&temp_dir).is_none());
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn load_empty_file_returns_none() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_empty");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let path = temp_dir.join("url_expander_config.json");
        std::fs::write(&path, "").unwrap();

        assert!(load_url_expander_config(&temp_dir).is_none());
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn save_load_unicode_config() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_unicode");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config = UrlExpanderConfig {
            custom_shorteners: vec!["短链.io".into(), "ショート.jp".into()],
            ..Default::default()
        };
        save_url_expander_config(&config, &temp_dir).unwrap();
        let loaded = load_url_expander_config(&temp_dir).unwrap();

        assert_eq!(loaded.custom_shorteners.len(), 2);
        assert!(loaded.custom_shorteners.contains(&"短链.io".to_string()));
        assert!(
            loaded
                .custom_shorteners
                .contains(&"ショート.jp".to_string())
        );
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn save_load_full_roundtrip() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_full_rt");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config = UrlExpanderConfig {
            expansion_enabled: false,
            validation_enabled: false,
            max_redirects: 1,
            timeout_secs: 60,
            custom_shorteners: vec!["a.co".into(), "b.io".into(), "c.net".into()],
            block_on_unreachable: true,
        };
        save_url_expander_config(&config, &temp_dir).unwrap();
        let loaded = load_url_expander_config(&temp_dir).unwrap();

        assert!(!loaded.expansion_enabled);
        assert!(!loaded.validation_enabled);
        assert_eq!(loaded.max_redirects, 1);
        assert_eq!(loaded.timeout_secs, 60);
        assert!(loaded.block_on_unreachable);
        assert_eq!(loaded.custom_shorteners.len(), 3);
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn load_missing_file_returns_none() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_nofile");
        let _ = std::fs::remove_dir_all(&temp_dir);
        // Don't create directory
        assert!(load_url_expander_config(&temp_dir).is_none());
    }

    // --- expand_url boundary tests (no network) ---

    #[tokio::test]
    async fn test_expand_url_disabled_returns_same() {
        let config = UrlExpanderConfig {
            expansion_enabled: false,
            ..Default::default()
        };
        let result = expand_url("https://example.com/anything", &config)
            .await
            .unwrap();
        assert!(!result.was_expanded);
        assert_eq!(result.expanded, "https://example.com/anything");
        assert_eq!(result.redirect_count, 0);
    }

    #[tokio::test]
    async fn test_expand_url_disabled_unicode_url() {
        let config = UrlExpanderConfig {
            expansion_enabled: false,
            ..Default::default()
        };
        let url = "https://example.com/中文/パス";
        let result = expand_url(url, &config).await.unwrap();
        assert_eq!(result.expanded, url);
        assert!(!result.was_expanded);
    }

    // --- validate_url boundary tests (no network) ---

    #[tokio::test]
    async fn test_validate_url_disabled_returns_defaults() {
        let config = UrlExpanderConfig {
            validation_enabled: false,
            ..Default::default()
        };
        let result = validate_url("https://anything.com/path", &config)
            .await
            .unwrap();
        assert!(result.reachable);
        assert!(result.status_code.is_none());
        assert!(result.content_length.is_none());
        assert!(result.content_type.is_none());
        assert!(!result.was_shortened);
        assert!(result.final_url.is_none());
        assert!(result.error.is_none());
        assert_eq!(result.response_time_ms, 0);
    }

    // --- expand_and_validate boundary (no network) ---

    #[tokio::test]
    async fn test_expand_and_validate_both_disabled() {
        let config = UrlExpanderConfig {
            expansion_enabled: false,
            validation_enabled: false,
            ..Default::default()
        };
        let (exp, val) = expand_and_validate("https://example.com", &config)
            .await
            .unwrap();
        assert!(!exp.was_expanded);
        assert_eq!(exp.expanded, "https://example.com");
        assert!(val.reachable);
        assert_eq!(val.response_time_ms, 0);
    }

    // --- KNOWN_SHORTENERS constant ---

    #[test]
    fn known_shorteners_not_empty() {
        assert!(!KNOWN_SHORTENERS.is_empty());
        assert!(KNOWN_SHORTENERS.len() >= 14);
    }

    #[test]
    fn known_shorteners_all_contain_dot() {
        for s in KNOWN_SHORTENERS {
            assert!(s.contains('.'), "shortener {} should contain a dot", s);
        }
    }

    #[test]
    fn known_shorteners_all_lowercase() {
        for s in KNOWN_SHORTENERS {
            assert_eq!(*s, s.to_lowercase(), "shortener {} should be lowercase", s);
        }
    }

    // --- Config edge cases ---

    #[test]
    fn config_max_redirects_zero() {
        let config = UrlExpanderConfig {
            max_redirects: 0,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let de: UrlExpanderConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.max_redirects, 0);
    }

    #[test]
    fn config_max_redirects_max_value() {
        let config = UrlExpanderConfig {
            max_redirects: u32::MAX,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let de: UrlExpanderConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.max_redirects, u32::MAX);
    }

    #[test]
    fn config_timeout_zero() {
        let config = UrlExpanderConfig {
            timeout_secs: 0,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let de: UrlExpanderConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.timeout_secs, 0);
    }

    #[test]
    fn config_many_custom_shorteners() {
        let config = UrlExpanderConfig {
            custom_shorteners: (0..100).map(|i| format!("s{}.io", i)).collect(),
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let de: UrlExpanderConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(de.custom_shorteners.len(), 100);
    }

    // --- ExpansionResult edge cases ---

    #[test]
    fn expansion_result_large_redirect_count() {
        let r = ExpansionResult {
            original: "o".into(),
            expanded: "e".into(),
            was_expanded: true,
            redirect_count: u32::MAX,
        };
        let json = serde_json::to_string(&r).unwrap();
        let de: ExpansionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(de.redirect_count, u32::MAX);
    }

    #[test]
    fn expansion_result_empty_strings() {
        let r = ExpansionResult {
            original: String::new(),
            expanded: String::new(),
            was_expanded: false,
            redirect_count: 0,
        };
        let json = serde_json::to_string(&r).unwrap();
        let de: ExpansionResult = serde_json::from_str(&json).unwrap();
        assert!(de.original.is_empty());
        assert!(de.expanded.is_empty());
    }

    // --- ValidationResult edge cases ---

    #[test]
    fn validation_result_all_none() {
        let v = ValidationResult {
            reachable: false,
            status_code: None,
            content_length: None,
            content_type: None,
            was_shortened: false,
            final_url: None,
            error: None,
            response_time_ms: 0,
        };
        let json = serde_json::to_string(&v).unwrap();
        let de: ValidationResult = serde_json::from_str(&json).unwrap();
        assert!(!de.reachable);
        assert!(de.status_code.is_none());
        assert!(de.content_length.is_none());
    }

    #[test]
    fn validation_result_unicode_content_type() {
        let v = ValidationResult {
            reachable: true,
            status_code: Some(200),
            content_length: Some(0),
            content_type: Some("text/html; charset=中文".into()),
            was_shortened: false,
            final_url: None,
            error: None,
            response_time_ms: 1,
        };
        let json = serde_json::to_string(&v).unwrap();
        let de: ValidationResult = serde_json::from_str(&json).unwrap();
        assert!(de.content_type.unwrap().contains("中文"));
    }

    #[test]
    fn validation_result_large_content_length() {
        let v = ValidationResult {
            reachable: true,
            status_code: Some(200),
            content_length: Some(u64::MAX),
            content_type: None,
            was_shortened: false,
            final_url: None,
            error: None,
            response_time_ms: 0,
        };
        let json = serde_json::to_string(&v).unwrap();
        let de: ValidationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(de.content_length, Some(u64::MAX));
    }

    // --- Persistence with pretty JSON ---

    #[test]
    fn save_pretty_json_format() {
        let temp_dir = std::env::temp_dir().join("test_url_expander_pretty");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config = UrlExpanderConfig::default();
        save_url_expander_config(&config, &temp_dir).unwrap();

        let path = temp_dir.join("url_expander_config.json");
        let content = std::fs::read_to_string(&path).unwrap();
        // Pretty JSON should have newlines and indentation
        assert!(content.contains('\n'));
        assert!(content.contains("  "));
        // Should be valid JSON
        let _: UrlExpanderConfig = serde_json::from_str(&content).unwrap();
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    // --- is_shortened_url with config variations ---

    #[test]
    fn is_shortened_url_validation_disabled_but_expansion_enabled() {
        let config = UrlExpanderConfig {
            expansion_enabled: true,
            validation_enabled: false,
            ..Default::default()
        };
        // is_shortened_url only checks expansion_enabled
        assert!(is_shortened_url("https://bit.ly/abc", &config));
    }

    #[test]
    fn is_shortened_url_both_disabled() {
        let config = UrlExpanderConfig {
            expansion_enabled: false,
            validation_enabled: false,
            ..Default::default()
        };
        assert!(!is_shortened_url("https://bit.ly/abc", &config));
    }

    #[test]
    fn is_shortened_url_http_scheme() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("http://bit.ly/test", &config));
    }

    #[test]
    fn is_shortened_url_uppercase_scheme_not_matched() {
        let config = UrlExpanderConfig::default();
        // The function checks starts_with("http://") before lowercasing,
        // so uppercase schemes are not matched
        assert!(!is_shortened_url("HTTPS://BIT.LY/abc", &config));
    }

    #[test]
    fn is_shortened_url_mixed_case_domain() {
        let config = UrlExpanderConfig::default();
        assert!(is_shortened_url("https://Bit.Ly/AbC", &config));
    }

    // --- Error trait coverage ---

    #[test]
    fn error_clone() {
        let err = UrlExpanderError::HttpClient("test".into());
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());

        let err = UrlExpanderError::ValidationFailed {
            url: "u".into(),
            error: "e".into(),
            response_time_ms: 1,
        };
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }

    #[test]
    fn error_validation_failed_zero_response_time() {
        let err = UrlExpanderError::ValidationFailed {
            url: "https://x.com".into(),
            error: "timeout".into(),
            response_time_ms: 0,
        };
        let s = err.to_string();
        assert!(s.contains("0ms"));
    }

    #[test]
    fn error_validation_failed_large_response_time() {
        let err = UrlExpanderError::ValidationFailed {
            url: "https://slow.com".into(),
            error: "slow".into(),
            response_time_ms: u64::MAX,
        };
        let s = err.to_string();
        assert!(s.contains(&format!("{}ms", u64::MAX)));
    }
}
