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
}
