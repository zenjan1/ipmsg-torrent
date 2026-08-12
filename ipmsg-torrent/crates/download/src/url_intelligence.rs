//! Download URL Intelligence System
//!
//! Pre-analyzes URLs before download to predict success probability,
//! recommend optimal settings, and detect potential issues.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// URL analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlAnalysis {
    /// The analyzed URL
    pub url: String,
    /// Predicted success probability (0.0-1.0)
    pub success_probability: f64,
    /// Recommended concurrent connections
    pub recommended_connections: u32,
    /// Recommended timeout in seconds
    pub recommended_timeout_secs: u64,
    /// Detected protocol type
    pub protocol: UrlProtocol,
    /// Potential issues detected
    pub issues: Vec<UrlIssue>,
    /// Optimization suggestions
    pub suggestions: Vec<UrlSuggestion>,
    /// Analysis timestamp
    pub timestamp: u64,
}

/// URL protocol type
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum UrlProtocol {
    Http,
    Https,
    Ftp,
    Magnet,
    Ed2k,
    Unknown,
}

impl std::fmt::Display for UrlProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UrlProtocol::Http => write!(f, "HTTP"),
            UrlProtocol::Https => write!(f, "HTTPS"),
            UrlProtocol::Ftp => write!(f, "FTP"),
            UrlProtocol::Magnet => write!(f, "Magnet"),
            UrlProtocol::Ed2k => write!(f, "Ed2k"),
            UrlProtocol::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Potential URL issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlIssue {
    /// Issue type
    pub issue_type: UrlIssueType,
    /// Severity level
    pub severity: IssueSeverity,
    /// Human-readable description
    pub description: String,
}

/// URL issue type
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum UrlIssueType {
    /// URL uses insecure protocol (HTTP instead of HTTPS)
    InsecureProtocol,
    /// URL contains suspicious patterns
    SuspiciousPattern,
    /// URL may require authentication
    RequiresAuth,
    /// URL has unusual length
    UnusualLength,
    /// URL contains tracking parameters
    TrackingParams,
    /// URL domain has poor reliability history
    PoorDomainReliability,
    /// URL appears to be a redirect
    PossibleRedirect,
    /// URL file size may be very large
    LargeFileWarning,
}

impl std::fmt::Display for UrlIssueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UrlIssueType::InsecureProtocol => write!(f, "Insecure Protocol"),
            UrlIssueType::SuspiciousPattern => write!(f, "Suspicious Pattern"),
            UrlIssueType::RequiresAuth => write!(f, "Requires Authentication"),
            UrlIssueType::UnusualLength => write!(f, "Unusual Length"),
            UrlIssueType::TrackingParams => write!(f, "Tracking Parameters"),
            UrlIssueType::PoorDomainReliability => write!(f, "Poor Domain Reliability"),
            UrlIssueType::PossibleRedirect => write!(f, "Possible Redirect"),
            UrlIssueType::LargeFileWarning => write!(f, "Large File Warning"),
        }
    }
}

/// Issue severity
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum IssueSeverity {
    Info,
    Warning,
    Critical,
}

impl std::fmt::Display for IssueSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueSeverity::Info => write!(f, "Info"),
            IssueSeverity::Warning => write!(f, "Warning"),
            IssueSeverity::Critical => write!(f, "Critical"),
        }
    }
}

/// URL optimization suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlSuggestion {
    /// Suggestion type
    pub suggestion_type: SuggestionType,
    /// Human-readable suggestion
    pub message: String,
}

/// Suggestion type
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SuggestionType {
    /// Use HTTPS instead of HTTP
    UseHttps,
    /// Remove tracking parameters
    RemoveTracking,
    /// Use fewer concurrent connections
    ReduceConnections,
    /// Use more concurrent connections
    IncreaseConnections,
    /// Increase timeout for this download
    IncreaseTimeout,
    /// Consider using a mirror
    UseMirror,
    /// Enable resume support
    EnableResume,
    /// Verify URL before downloading
    VerifyFirst,
}

impl std::fmt::Display for SuggestionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SuggestionType::UseHttps => write!(f, "Use HTTPS"),
            SuggestionType::RemoveTracking => write!(f, "Remove Tracking"),
            SuggestionType::ReduceConnections => write!(f, "Reduce Connections"),
            SuggestionType::IncreaseConnections => write!(f, "Increase Connections"),
            SuggestionType::IncreaseTimeout => write!(f, "Increase Timeout"),
            SuggestionType::UseMirror => write!(f, "Use Mirror"),
            SuggestionType::EnableResume => write!(f, "Enable Resume"),
            SuggestionType::VerifyFirst => write!(f, "Verify First"),
        }
    }
}

/// Configuration for URL intelligence system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlIntelligenceConfig {
    /// Enable URL intelligence
    pub enabled: bool,
    /// Maximum URL length before warning (default: 2000)
    pub max_url_length: usize,
    /// Suspicious patterns to detect
    pub suspicious_patterns: Vec<String>,
    /// Tracking parameter names to detect
    pub tracking_params: Vec<String>,
    /// Domains with known poor reliability
    pub unreliable_domains: Vec<String>,
    /// Default timeout for HTTP/HTTPS (seconds)
    pub default_timeout_secs: u64,
    /// Default concurrent connections
    pub default_connections: u32,
}

impl Default for UrlIntelligenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_url_length: 2000,
            suspicious_patterns: vec![
                r"\.exe(\?|$)".to_string(),
                r"\.scr(\?|$)".to_string(),
                r"\.bat(\?|$)".to_string(),
                r"free.*download".to_string(),
                r"crack".to_string(),
                r"keygen".to_string(),
            ],
            tracking_params: vec![
                "utm_source".to_string(),
                "utm_medium".to_string(),
                "utm_campaign".to_string(),
                "utm_term".to_string(),
                "utm_content".to_string(),
                "fbclid".to_string(),
                "gclid".to_string(),
                "msclkid".to_string(),
            ],
            unreliable_domains: vec![],
            default_timeout_secs: 30,
            default_connections: 4,
        }
    }
}

/// URL intelligence manager
#[derive(Debug, Clone)]
pub struct UrlIntelligenceManager {
    config: UrlIntelligenceConfig,
    analysis_cache: HashMap<String, UrlAnalysis>,
    max_cache_size: usize,
}

impl UrlIntelligenceManager {
    /// Create a new URL intelligence manager
    pub fn new() -> Self {
        Self {
            config: UrlIntelligenceConfig::default(),
            analysis_cache: HashMap::new(),
            max_cache_size: 1000,
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: UrlIntelligenceConfig) -> Self {
        Self {
            config,
            analysis_cache: HashMap::new(),
            max_cache_size: 1000,
        }
    }

    /// Get current configuration
    pub fn get_config(&self) -> &UrlIntelligenceConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: UrlIntelligenceConfig) {
        self.config = config;
    }

    /// Analyze a URL and return recommendations
    pub fn analyze_url(&mut self, url: &str) -> UrlAnalysis {
        // Check cache first
        if let Some(cached) = self.analysis_cache.get(url) {
            return cached.clone();
        }

        let protocol = self.detect_protocol(url);
        let mut issues = Vec::new();
        let mut suggestions = Vec::new();
        let mut success_probability: f64 = 1.0;

        // Check protocol security
        if protocol == UrlProtocol::Http {
            issues.push(UrlIssue {
                issue_type: UrlIssueType::InsecureProtocol,
                severity: IssueSeverity::Warning,
                description: "URL uses insecure HTTP protocol".to_string(),
            });
            suggestions.push(UrlSuggestion {
                suggestion_type: SuggestionType::UseHttps,
                message: "Consider using HTTPS instead of HTTP for better security".to_string(),
            });
            success_probability -= 0.1;
        }

        // Check URL length
        if url.len() > self.config.max_url_length {
            issues.push(UrlIssue {
                issue_type: UrlIssueType::UnusualLength,
                severity: IssueSeverity::Info,
                description: format!(
                    "URL length ({}) exceeds recommended maximum ({})",
                    url.len(),
                    self.config.max_url_length
                ),
            });
            success_probability -= 0.05;
        }

        // Check for suspicious patterns
        for pattern in &self.config.suspicious_patterns {
            if let Ok(re) = regex_lite::Regex::new(pattern) {
                if re.is_match(url) {
                    issues.push(UrlIssue {
                        issue_type: UrlIssueType::SuspiciousPattern,
                        severity: IssueSeverity::Critical,
                        description: format!("URL contains suspicious pattern: {}", pattern),
                    });
                    success_probability -= 0.3;
                    break;
                }
            }
        }

        // Check for tracking parameters
        let mut has_tracking = false;
        for param in &self.config.tracking_params {
            if url.contains(param) {
                has_tracking = true;
                break;
            }
        }
        if has_tracking {
            issues.push(UrlIssue {
                issue_type: UrlIssueType::TrackingParams,
                severity: IssueSeverity::Info,
                description: "URL contains tracking parameters".to_string(),
            });
            suggestions.push(UrlSuggestion {
                suggestion_type: SuggestionType::RemoveTracking,
                message: "Consider removing tracking parameters for cleaner URL".to_string(),
            });
        }

        // Check domain reliability
        if let Some(domain) = self.extract_domain(url) {
            if self.config.unreliable_domains.contains(&domain) {
                issues.push(UrlIssue {
                    issue_type: UrlIssueType::PoorDomainReliability,
                    severity: IssueSeverity::Warning,
                    description: format!("Domain '{}' has poor reliability history", domain),
                });
                suggestions.push(UrlSuggestion {
                    suggestion_type: SuggestionType::UseMirror,
                    message: "Consider using a mirror or alternative source".to_string(),
                });
                success_probability -= 0.2;
            }
        }

        // Check for possible redirects
        if url.contains("redirect") || url.contains("go?") || url.contains("link?") {
            issues.push(UrlIssue {
                issue_type: UrlIssueType::PossibleRedirect,
                severity: IssueSeverity::Info,
                description: "URL may redirect to actual download location".to_string(),
            });
            suggestions.push(UrlSuggestion {
                suggestion_type: SuggestionType::VerifyFirst,
                message: "Verify URL resolves correctly before starting download".to_string(),
            });
            success_probability -= 0.1;
        }

        // Recommend timeout based on protocol
        let recommended_timeout_secs = match protocol {
            UrlProtocol::Http | UrlProtocol::Https => self.config.default_timeout_secs,
            UrlProtocol::Ftp => self.config.default_timeout_secs * 2,
            UrlProtocol::Magnet | UrlProtocol::Ed2k => self.config.default_timeout_secs * 3,
            UrlProtocol::Unknown => self.config.default_timeout_secs,
        };

        // Recommend connections based on protocol
        let recommended_connections = match protocol {
            UrlProtocol::Http | UrlProtocol::Https => self.config.default_connections,
            UrlProtocol::Ftp => 2,
            UrlProtocol::Magnet | UrlProtocol::Ed2k => 8,
            UrlProtocol::Unknown => 2,
        };

        // Ensure probability is in valid range
        success_probability = success_probability.max(0.0).min(1.0);

        let analysis = UrlAnalysis {
            url: url.to_string(),
            success_probability,
            recommended_connections,
            recommended_timeout_secs,
            protocol,
            issues,
            suggestions,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        // Cache the result
        if self.analysis_cache.len() >= self.max_cache_size {
            // Remove oldest entry
            if let Some(oldest_key) = self.analysis_cache.keys().next().cloned() {
                self.analysis_cache.remove(&oldest_key);
            }
        }
        self.analysis_cache
            .insert(url.to_string(), analysis.clone());

        analysis
    }

    /// Detect protocol from URL
    fn detect_protocol(&self, url: &str) -> UrlProtocol {
        let url_lower = url.to_lowercase();
        if url_lower.starts_with("https://") {
            UrlProtocol::Https
        } else if url_lower.starts_with("http://") {
            UrlProtocol::Http
        } else if url_lower.starts_with("ftp://") {
            UrlProtocol::Ftp
        } else if url_lower.starts_with("magnet:") {
            UrlProtocol::Magnet
        } else if url_lower.starts_with("ed2k://") {
            UrlProtocol::Ed2k
        } else {
            UrlProtocol::Unknown
        }
    }

    /// Extract domain from URL
    fn extract_domain(&self, url: &str) -> Option<String> {
        // Simple domain extraction
        let url_without_scheme = url.split("://").nth(1)?;
        let domain_part = url_without_scheme.split('/').next()?;
        let domain = domain_part.split(':').next()?;
        Some(domain.to_lowercase())
    }

    /// Get analysis cache size
    pub fn get_cache_size(&self) -> usize {
        self.analysis_cache.len()
    }

    /// Clear analysis cache
    pub fn clear_cache(&mut self) {
        self.analysis_cache.clear();
    }

    /// Get cached analysis for a URL
    pub fn get_cached_analysis(&self, url: &str) -> Option<&UrlAnalysis> {
        self.analysis_cache.get(url)
    }

    /// Remove cached analysis for a URL
    pub fn remove_cached_analysis(&mut self, url: &str) -> bool {
        self.analysis_cache.remove(url).is_some()
    }
}

impl Default for UrlIntelligenceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_protocol() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.detect_protocol("https://example.com"),
            UrlProtocol::Https
        );
        assert_eq!(
            manager.detect_protocol("http://example.com"),
            UrlProtocol::Http
        );
        assert_eq!(
            manager.detect_protocol("ftp://example.com"),
            UrlProtocol::Ftp
        );
        assert_eq!(
            manager.detect_protocol("magnet:?xt=urn:btih:abc"),
            UrlProtocol::Magnet
        );
        assert_eq!(
            manager.detect_protocol("ed2k://|file|test|123|abc|"),
            UrlProtocol::Ed2k
        );
        assert_eq!(
            manager.detect_protocol("unknown://test"),
            UrlProtocol::Unknown
        );
    }

    #[test]
    fn test_extract_domain() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.extract_domain("https://example.com/path"),
            Some("example.com".to_string())
        );
        assert_eq!(
            manager.extract_domain("http://sub.example.com:8080/path"),
            Some("sub.example.com".to_string())
        );
        assert_eq!(
            manager.extract_domain("ftp://files.example.com"),
            Some("files.example.com".to_string())
        );
        assert_eq!(manager.extract_domain("not a url"), None);
    }

    #[test]
    fn test_analyze_https_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        assert_eq!(analysis.protocol, UrlProtocol::Https);
        assert!(analysis.success_probability > 0.9);
        assert!(analysis.issues.is_empty());
        assert_eq!(analysis.recommended_connections, 4);
    }

    #[test]
    fn test_analyze_http_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("http://example.com/file.zip");
        assert_eq!(analysis.protocol, UrlProtocol::Http);
        assert!(analysis.success_probability < 1.0);
        assert!(!analysis.issues.is_empty());
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::InsecureProtocol)
        );
        assert!(
            analysis
                .suggestions
                .iter()
                .any(|s| s.suggestion_type == SuggestionType::UseHttps)
        );
    }

    #[test]
    fn test_analyze_long_url() {
        let mut manager = UrlIntelligenceManager::new();
        let long_url = format!("https://example.com/{}", "a".repeat(2500));
        let analysis = manager.analyze_url(&long_url);
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::UnusualLength)
        );
    }

    #[test]
    fn test_analyze_tracking_params() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis =
            manager.analyze_url("https://example.com/file.zip?utm_source=test&fbclid=abc");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );
        assert!(
            analysis
                .suggestions
                .iter()
                .any(|s| s.suggestion_type == SuggestionType::RemoveTracking)
        );
    }

    #[test]
    fn test_analyze_suspicious_pattern() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("http://example.com/free-download-crack.exe");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
        assert!(analysis.success_probability < 0.8);
    }

    #[test]
    fn test_analyze_redirect_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/redirect?url=file.zip");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PossibleRedirect)
        );
    }

    #[test]
    fn test_unreliable_domain() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.unreliable_domains.push("badhost.com".to_string());
        manager.set_config(config);

        let analysis = manager.analyze_url("https://badhost.com/file.zip");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
        );
        assert!(
            analysis
                .suggestions
                .iter()
                .any(|s| s.suggestion_type == SuggestionType::UseMirror)
        );
    }

    #[test]
    fn test_cache_analysis() {
        let mut manager = UrlIntelligenceManager::new();
        let url = "https://example.com/file.zip";

        // First analysis
        let analysis1 = manager.analyze_url(url);
        assert_eq!(manager.get_cache_size(), 1);

        // Second analysis should use cache
        let analysis2 = manager.analyze_url(url);
        assert_eq!(analysis1.timestamp, analysis2.timestamp);

        // Get cached analysis
        let cached = manager.get_cached_analysis(url);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().url, url);

        // Remove from cache
        assert!(manager.remove_cached_analysis(url));
        assert!(manager.get_cached_analysis(url).is_none());
    }

    #[test]
    fn test_clear_cache() {
        let mut manager = UrlIntelligenceManager::new();
        manager.analyze_url("https://example1.com/file.zip");
        manager.analyze_url("https://example2.com/file.zip");
        assert_eq!(manager.get_cache_size(), 2);

        manager.clear_cache();
        assert_eq!(manager.get_cache_size(), 0);
    }

    #[test]
    fn test_cache_size_limit() {
        let mut manager = UrlIntelligenceManager::new();
        manager.max_cache_size = 2;

        manager.analyze_url("https://example1.com/file.zip");
        manager.analyze_url("https://example2.com/file.zip");
        assert_eq!(manager.get_cache_size(), 2);

        // Adding third should evict oldest
        manager.analyze_url("https://example3.com/file.zip");
        assert_eq!(manager.get_cache_size(), 2);
    }

    #[test]
    fn test_protocol_recommendations() {
        let mut manager = UrlIntelligenceManager::new();

        let http_analysis = manager.analyze_url("http://example.com/file.zip");
        assert_eq!(http_analysis.recommended_timeout_secs, 30);
        assert_eq!(http_analysis.recommended_connections, 4);

        let ftp_analysis = manager.analyze_url("ftp://example.com/file.zip");
        assert_eq!(ftp_analysis.recommended_timeout_secs, 60);
        assert_eq!(ftp_analysis.recommended_connections, 2);

        let magnet_analysis = manager.analyze_url("magnet:?xt=urn:btih:abc");
        assert_eq!(magnet_analysis.recommended_timeout_secs, 90);
        assert_eq!(magnet_analysis.recommended_connections, 8);
    }

    #[test]
    fn test_config_serialization() {
        let config = UrlIntelligenceConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: UrlIntelligenceConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.max_url_length, deserialized.max_url_length);
    }

    #[test]
    fn test_analysis_serialization() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        let json = serde_json::to_string(&analysis).unwrap();
        let deserialized: UrlAnalysis = serde_json::from_str(&json).unwrap();
        assert_eq!(analysis.url, deserialized.url);
        assert_eq!(analysis.protocol, deserialized.protocol);
    }
}
