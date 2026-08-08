//! Per-domain concurrent download limiting
//!
//! Limits the number of simultaneous downloads from the same domain,
//! preventing overwhelming a single server while allowing other downloads to proceed.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

/// Configuration for per-domain download limits
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DomainLimitConfig {
    /// Enable per-domain limiting
    pub enabled: bool,
    /// Default maximum concurrent downloads per domain (0 = unlimited)
    pub default_limit: u32,
    /// Per-domain overrides (domain -> limit)
    pub domain_overrides: HashMap<String, u32>,
}

impl DomainLimitConfig {
    /// Create a new config with the given default limit
    pub fn new(default_limit: u32) -> Self {
        Self {
            enabled: true,
            default_limit,
            domain_overrides: HashMap::new(),
        }
    }

    /// Set a per-domain override
    pub fn set_domain_limit(&mut self, domain: &str, limit: u32) {
        self.domain_overrides.insert(domain.to_string(), limit);
    }

    /// Remove a per-domain override
    pub fn remove_domain_limit(&mut self, domain: &str) -> Option<u32> {
        self.domain_overrides.remove(domain)
    }

    /// Get the limit for a specific domain
    pub fn get_limit(&self, domain: &str) -> u32 {
        self.domain_overrides
            .get(domain)
            .copied()
            .unwrap_or(self.default_limit)
    }
}

/// Extract domain from a URL string
///
/// Returns the domain (host) portion of the URL, or None if parsing fails.
///
/// # Examples
///
/// ```
/// use ipmsg_download::domain_limit::extract_domain;
///
/// assert_eq!(extract_domain("https://example.com/file.zip"), Some("example.com".to_string()));
/// assert_eq!(extract_domain("http://user:pass@host.org:8080/path"), Some("host.org".to_string()));
/// assert_eq!(extract_domain("not-a-url"), None);
/// ```
pub fn extract_domain(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
}

/// Check if a domain can start a new download given current active counts
///
/// Returns true if:
/// - Domain limiting is disabled
/// - The domain has no limit configured (limit = 0)
/// - The current active count is below the limit
///
/// # Examples
///
/// ```
/// use ipmsg_download::domain_limit::{DomainLimitConfig, can_start_domain_download};
/// use std::collections::HashMap;
///
/// let config = DomainLimitConfig {
///     enabled: true,
///     default_limit: 2,
///     domain_overrides: HashMap::new(),
/// };
///
/// let mut active_counts = HashMap::new();
/// assert!(can_start_domain_download(&config, "example.com", &active_counts));
///
/// active_counts.insert("example.com".to_string(), 2);
/// assert!(!can_start_domain_download(&config, "example.com", &active_counts));
/// ```
pub fn can_start_domain_download(
    config: &DomainLimitConfig,
    domain: &str,
    active_counts: &HashMap<String, u32>,
) -> bool {
    if !config.enabled {
        return true;
    }

    let limit = config.get_limit(domain);
    if limit == 0 {
        return true; // Unlimited
    }

    let current = active_counts.get(domain).copied().unwrap_or(0);
    current < limit
}

/// Count active downloads per domain from a list of task URLs
///
/// # Examples
///
/// ```
/// use ipmsg_download::domain_limit::count_active_domains;
///
/// let urls = vec![
///     Some("https://example.com/a.zip"),
///     Some("https://example.com/b.zip"),
///     Some("https://other.org/c.zip"),
///     None,
/// ];
///
/// let counts = count_active_domains(&urls);
/// assert_eq!(counts.get("example.com"), Some(&2));
/// assert_eq!(counts.get("other.org"), Some(&1));
/// ```
pub fn count_active_domains(urls: &[Option<&str>]) -> HashMap<String, u32> {
    let mut counts: HashMap<String, u32> = HashMap::new();
    for url_opt in urls {
        if let Some(url) = url_opt
            && let Some(domain) = extract_domain(url)
        {
            *counts.entry(domain).or_insert(0) += 1;
        }
    }
    counts
}

/// Summary of per-domain download limits and current counts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainLimitSummary {
    /// Whether domain limiting is enabled
    pub enabled: bool,
    /// Default limit per domain (0 = unlimited)
    pub default_limit: u32,
    /// Total active downloads with identifiable domains
    pub total_active: u32,
    /// Number of domains currently at their limit
    pub domains_at_limit: u32,
    /// Per-domain breakdown
    pub entries: Vec<DomainLimitEntry>,
}

/// Per-domain download count and limit information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainLimitEntry {
    /// Domain name (e.g., "example.com")
    pub domain: String,
    /// Number of active downloads from this domain
    pub active: u32,
    /// Maximum allowed concurrent downloads (0 = unlimited)
    pub limit: u32,
    /// Whether this domain is at its limit
    pub at_limit: bool,
}

/// Persistence functions for domain limit config
/// Save domain limit config to disk
pub fn save_domain_limit_config(
    config: &DomainLimitConfig,
    data_dir: &std::path::Path,
) -> Result<(), DomainLimitPersistenceError> {
    let path = data_dir.join("domain_limit.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| DomainLimitPersistenceError::Serialize(e.to_string()))?;

    // Atomic write: write to temp file, then rename
    let temp_path = path.with_extension("json.tmp");
    std::fs::write(&temp_path, &json)
        .map_err(|e| DomainLimitPersistenceError::Io(e.to_string()))?;
    std::fs::rename(&temp_path, &path)
        .map_err(|e| DomainLimitPersistenceError::Io(e.to_string()))?;

    Ok(())
}

/// Load domain limit config from disk
pub fn load_domain_limit_config(
    data_dir: &std::path::Path,
) -> Result<Option<DomainLimitConfig>, DomainLimitPersistenceError> {
    let path = data_dir.join("domain_limit.json");
    if !path.exists() {
        return Ok(None);
    }

    let json = std::fs::read_to_string(&path)
        .map_err(|e| DomainLimitPersistenceError::Io(e.to_string()))?;
    let config: DomainLimitConfig = serde_json::from_str(&json)
        .map_err(|e| DomainLimitPersistenceError::Deserialize(e.to_string()))?;

    Ok(Some(config))
}

/// Errors that can occur during domain limit config persistence
#[derive(Debug, Clone)]
pub enum DomainLimitPersistenceError {
    Io(String),
    Serialize(String),
    Deserialize(String),
}

impl std::fmt::Display for DomainLimitPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Serialize(e) => write!(f, "Serialization error: {}", e),
            Self::Deserialize(e) => write!(f, "Deserialization error: {}", e),
        }
    }
}

impl std::error::Error for DomainLimitPersistenceError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_domain_http() {
        assert_eq!(
            extract_domain("http://example.com/file.zip"),
            Some("example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_https() {
        assert_eq!(
            extract_domain("https://example.com/path/to/file.zip"),
            Some("example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_with_port() {
        assert_eq!(
            extract_domain("http://example.com:8080/file.zip"),
            Some("example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_with_auth() {
        assert_eq!(
            extract_domain("http://user:pass@example.com/file.zip"),
            Some("example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_subdomain() {
        assert_eq!(
            extract_domain("https://dl.example.com/file.zip"),
            Some("dl.example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_invalid_url() {
        assert_eq!(extract_domain("not-a-url"), None);
    }

    #[test]
    fn test_extract_domain_ftp() {
        assert_eq!(
            extract_domain("ftp://ftp.example.com/pub/file.tar.gz"),
            Some("ftp.example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_magnet() {
        // Magnet URLs don't have a traditional domain
        assert_eq!(extract_domain("magnet:?xt=urn:btih:abc123"), None);
    }

    #[test]
    fn test_extract_domain_ed2k() {
        // Ed2k URLs don't have a traditional domain
        assert_eq!(extract_domain("ed2k://|file|test.iso|1234|abcd|/"), None);
    }

    #[test]
    fn test_config_default() {
        let config = DomainLimitConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.default_limit, 0);
        assert!(config.domain_overrides.is_empty());
    }

    #[test]
    fn test_config_new() {
        let config = DomainLimitConfig::new(3);
        assert!(config.enabled);
        assert_eq!(config.default_limit, 3);
    }

    #[test]
    fn test_config_set_domain_limit() {
        let mut config = DomainLimitConfig::new(2);
        config.set_domain_limit("slow-server.com", 1);
        config.set_domain_limit("fast-server.com", 5);

        assert_eq!(config.get_limit("slow-server.com"), 1);
        assert_eq!(config.get_limit("fast-server.com"), 5);
        assert_eq!(config.get_limit("other.com"), 2); // Falls back to default
    }

    #[test]
    fn test_config_remove_domain_limit() {
        let mut config = DomainLimitConfig::new(2);
        config.set_domain_limit("example.com", 5);
        assert_eq!(config.get_limit("example.com"), 5);

        let removed = config.remove_domain_limit("example.com");
        assert_eq!(removed, Some(5));
        assert_eq!(config.get_limit("example.com"), 2); // Falls back to default
    }

    #[test]
    fn test_can_start_disabled() {
        let config = DomainLimitConfig::default(); // disabled
        let counts = HashMap::new();
        assert!(can_start_domain_download(&config, "example.com", &counts));
    }

    #[test]
    fn test_can_start_unlimited() {
        let config = DomainLimitConfig {
            enabled: true,
            default_limit: 0, // unlimited
            domain_overrides: HashMap::new(),
        };
        let mut counts = HashMap::new();
        counts.insert("example.com".to_string(), 100);
        assert!(can_start_domain_download(&config, "example.com", &counts));
    }

    #[test]
    fn test_can_start_below_limit() {
        let config = DomainLimitConfig::new(3);
        let mut counts = HashMap::new();
        counts.insert("example.com".to_string(), 2);
        assert!(can_start_domain_download(&config, "example.com", &counts));
    }

    #[test]
    fn test_can_start_at_limit() {
        let config = DomainLimitConfig::new(3);
        let mut counts = HashMap::new();
        counts.insert("example.com".to_string(), 3);
        assert!(!can_start_domain_download(&config, "example.com", &counts));
    }

    #[test]
    fn test_can_start_above_limit() {
        let config = DomainLimitConfig::new(3);
        let mut counts = HashMap::new();
        counts.insert("example.com".to_string(), 5);
        assert!(!can_start_domain_download(&config, "example.com", &counts));
    }

    #[test]
    fn test_can_start_domain_override() {
        let mut config = DomainLimitConfig::new(5);
        config.set_domain_limit("strict.com", 1);

        let mut counts = HashMap::new();
        counts.insert("strict.com".to_string(), 1);
        counts.insert("normal.com".to_string(), 4);

        assert!(!can_start_domain_download(&config, "strict.com", &counts));
        assert!(can_start_domain_download(&config, "normal.com", &counts));
    }

    #[test]
    fn test_can_start_unknown_domain() {
        let config = DomainLimitConfig::new(2);
        let counts = HashMap::new(); // No entries
        assert!(can_start_domain_download(
            &config,
            "new-domain.com",
            &counts
        ));
    }

    #[test]
    fn test_count_active_domains() {
        let urls: Vec<Option<&str>> = vec![
            Some("https://example.com/a.zip"),
            Some("https://example.com/b.zip"),
            Some("https://other.org/c.zip"),
            None,
            Some("invalid-url"),
        ];

        let counts = count_active_domains(&urls);
        assert_eq!(counts.get("example.com"), Some(&2));
        assert_eq!(counts.get("other.org"), Some(&1));
        assert_eq!(counts.len(), 2);
    }

    #[test]
    fn test_count_active_domains_empty() {
        let urls: Vec<Option<&str>> = vec![];
        let counts = count_active_domains(&urls);
        assert!(counts.is_empty());
    }

    #[test]
    fn test_count_active_domains_all_invalid() {
        let urls: Vec<Option<&str>> = vec![Some("not-a-url"), Some("also-not-url")];
        let counts = count_active_domains(&urls);
        assert!(counts.is_empty());
    }

    #[test]
    fn test_persistence_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = DomainLimitConfig {
            enabled: true,
            default_limit: 3,
            domain_overrides: {
                let mut m = HashMap::new();
                m.insert("example.com".to_string(), 5);
                m.insert("slow.org".to_string(), 1);
                m
            },
        };

        save_domain_limit_config(&config, temp_dir.path()).unwrap();
        let loaded = load_domain_limit_config(temp_dir.path()).unwrap().unwrap();

        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.default_limit, config.default_limit);
        assert_eq!(loaded.domain_overrides.len(), 2);
        assert_eq!(loaded.domain_overrides.get("example.com"), Some(&5));
        assert_eq!(loaded.domain_overrides.get("slow.org"), Some(&1));
    }

    #[test]
    fn test_persistence_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = load_domain_limit_config(temp_dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_persistence_corrupt_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("domain_limit.json");
        std::fs::write(&path, "not valid json").unwrap();

        let result = load_domain_limit_config(temp_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_persistence_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();

        let config1 = DomainLimitConfig::new(2);
        save_domain_limit_config(&config1, temp_dir.path()).unwrap();

        let config2 = DomainLimitConfig::new(5);
        save_domain_limit_config(&config2, temp_dir.path()).unwrap();

        let loaded = load_domain_limit_config(temp_dir.path()).unwrap().unwrap();
        assert_eq!(loaded.default_limit, 5);
    }
}
