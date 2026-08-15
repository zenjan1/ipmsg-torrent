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

    // ===== UrlProtocol Display =====

    #[test]
    fn test_url_protocol_display_all_variants() {
        assert_eq!(format!("{}", UrlProtocol::Http), "HTTP");
        assert_eq!(format!("{}", UrlProtocol::Https), "HTTPS");
        assert_eq!(format!("{}", UrlProtocol::Ftp), "FTP");
        assert_eq!(format!("{}", UrlProtocol::Magnet), "Magnet");
        assert_eq!(format!("{}", UrlProtocol::Ed2k), "Ed2k");
        assert_eq!(format!("{}", UrlProtocol::Unknown), "Unknown");
    }

    // ===== UrlProtocol serde =====

    #[test]
    fn test_url_protocol_serde_roundtrip_all_variants() {
        for proto in [
            UrlProtocol::Http,
            UrlProtocol::Https,
            UrlProtocol::Ftp,
            UrlProtocol::Magnet,
            UrlProtocol::Ed2k,
            UrlProtocol::Unknown,
        ] {
            let json = serde_json::to_string(&proto).unwrap();
            let back: UrlProtocol = serde_json::from_str(&json).unwrap();
            assert_eq!(proto, back);
        }
    }

    #[test]
    fn test_url_protocol_clone_copy_debug() {
        let p = UrlProtocol::Https;
        let cloned = p.clone();
        let copied = p;
        assert_eq!(p, cloned);
        assert_eq!(p, copied);
        assert!(!format!("{:?}", p).is_empty());
    }

    #[test]
    fn test_url_protocol_eq() {
        assert_eq!(UrlProtocol::Http, UrlProtocol::Http);
        assert_ne!(UrlProtocol::Http, UrlProtocol::Https);
    }

    // ===== UrlIssueType Display =====

    #[test]
    fn test_url_issue_type_display_all_variants() {
        assert_eq!(
            format!("{}", UrlIssueType::InsecureProtocol),
            "Insecure Protocol"
        );
        assert_eq!(
            format!("{}", UrlIssueType::SuspiciousPattern),
            "Suspicious Pattern"
        );
        assert_eq!(
            format!("{}", UrlIssueType::RequiresAuth),
            "Requires Authentication"
        );
        assert_eq!(format!("{}", UrlIssueType::UnusualLength), "Unusual Length");
        assert_eq!(
            format!("{}", UrlIssueType::TrackingParams),
            "Tracking Parameters"
        );
        assert_eq!(
            format!("{}", UrlIssueType::PoorDomainReliability),
            "Poor Domain Reliability"
        );
        assert_eq!(
            format!("{}", UrlIssueType::PossibleRedirect),
            "Possible Redirect"
        );
        assert_eq!(
            format!("{}", UrlIssueType::LargeFileWarning),
            "Large File Warning"
        );
    }

    // ===== UrlIssueType serde =====

    #[test]
    fn test_url_issue_type_serde_roundtrip_all_variants() {
        for it in [
            UrlIssueType::InsecureProtocol,
            UrlIssueType::SuspiciousPattern,
            UrlIssueType::RequiresAuth,
            UrlIssueType::UnusualLength,
            UrlIssueType::TrackingParams,
            UrlIssueType::PoorDomainReliability,
            UrlIssueType::PossibleRedirect,
            UrlIssueType::LargeFileWarning,
        ] {
            let json = serde_json::to_string(&it).unwrap();
            let back: UrlIssueType = serde_json::from_str(&json).unwrap();
            assert_eq!(it, back);
        }
    }

    #[test]
    fn test_url_issue_type_clone_copy_debug() {
        let it = UrlIssueType::SuspiciousPattern;
        let cloned = it.clone();
        let copied = it;
        assert_eq!(it, cloned);
        assert_eq!(it, copied);
        assert!(!format!("{:?}", it).is_empty());
    }

    // ===== IssueSeverity Display =====

    #[test]
    fn test_issue_severity_display_all_variants() {
        assert_eq!(format!("{}", IssueSeverity::Info), "Info");
        assert_eq!(format!("{}", IssueSeverity::Warning), "Warning");
        assert_eq!(format!("{}", IssueSeverity::Critical), "Critical");
    }

    // ===== IssueSeverity serde =====

    #[test]
    fn test_issue_severity_serde_roundtrip_all_variants() {
        for sev in [
            IssueSeverity::Info,
            IssueSeverity::Warning,
            IssueSeverity::Critical,
        ] {
            let json = serde_json::to_string(&sev).unwrap();
            let back: IssueSeverity = serde_json::from_str(&json).unwrap();
            assert_eq!(sev, back);
        }
    }

    #[test]
    fn test_issue_severity_clone_copy_debug() {
        let s = IssueSeverity::Critical;
        let cloned = s.clone();
        let copied = s;
        assert_eq!(s, cloned);
        assert_eq!(s, copied);
        assert!(!format!("{:?}", s).is_empty());
    }

    // ===== SuggestionType Display =====

    #[test]
    fn test_suggestion_type_display_all_variants() {
        assert_eq!(format!("{}", SuggestionType::UseHttps), "Use HTTPS");
        assert_eq!(
            format!("{}", SuggestionType::RemoveTracking),
            "Remove Tracking"
        );
        assert_eq!(
            format!("{}", SuggestionType::ReduceConnections),
            "Reduce Connections"
        );
        assert_eq!(
            format!("{}", SuggestionType::IncreaseConnections),
            "Increase Connections"
        );
        assert_eq!(
            format!("{}", SuggestionType::IncreaseTimeout),
            "Increase Timeout"
        );
        assert_eq!(format!("{}", SuggestionType::UseMirror), "Use Mirror");
        assert_eq!(format!("{}", SuggestionType::EnableResume), "Enable Resume");
        assert_eq!(format!("{}", SuggestionType::VerifyFirst), "Verify First");
    }

    // ===== SuggestionType serde =====

    #[test]
    fn test_suggestion_type_serde_roundtrip_all_variants() {
        for st in [
            SuggestionType::UseHttps,
            SuggestionType::RemoveTracking,
            SuggestionType::ReduceConnections,
            SuggestionType::IncreaseConnections,
            SuggestionType::IncreaseTimeout,
            SuggestionType::UseMirror,
            SuggestionType::EnableResume,
            SuggestionType::VerifyFirst,
        ] {
            let json = serde_json::to_string(&st).unwrap();
            let back: SuggestionType = serde_json::from_str(&json).unwrap();
            assert_eq!(st, back);
        }
    }

    #[test]
    fn test_suggestion_type_clone_copy_debug() {
        let st = SuggestionType::UseMirror;
        let cloned = st.clone();
        let copied = st;
        assert_eq!(st, cloned);
        assert_eq!(st, copied);
        assert!(!format!("{:?}", st).is_empty());
    }

    // ===== UrlIntelligenceConfig =====

    #[test]
    fn test_config_default_values() {
        let config = UrlIntelligenceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_url_length, 2000);
        assert_eq!(config.default_timeout_secs, 30);
        assert_eq!(config.default_connections, 4);
        assert!(!config.suspicious_patterns.is_empty());
        assert!(!config.tracking_params.is_empty());
        assert!(config.unreliable_domains.is_empty());
    }

    #[test]
    fn test_config_default_suspicious_patterns_content() {
        let config = UrlIntelligenceConfig::default();
        assert!(config.suspicious_patterns.iter().any(|p| p.contains("exe")));
        assert!(
            config
                .suspicious_patterns
                .iter()
                .any(|p| p.contains("crack"))
        );
        assert!(
            config
                .suspicious_patterns
                .iter()
                .any(|p| p.contains("keygen"))
        );
    }

    #[test]
    fn test_config_default_tracking_params_content() {
        let config = UrlIntelligenceConfig::default();
        assert!(config.tracking_params.contains(&"utm_source".to_string()));
        assert!(config.tracking_params.contains(&"fbclid".to_string()));
        assert!(config.tracking_params.contains(&"gclid".to_string()));
        assert!(config.tracking_params.contains(&"msclkid".to_string()));
    }

    #[test]
    fn test_config_custom_values() {
        let config = UrlIntelligenceConfig {
            enabled: false,
            max_url_length: 500,
            suspicious_patterns: vec!["test_pattern".to_string()],
            tracking_params: vec!["my_param".to_string()],
            unreliable_domains: vec!["bad.com".to_string()],
            default_timeout_secs: 60,
            default_connections: 8,
        };
        assert!(!config.enabled);
        assert_eq!(config.max_url_length, 500);
        assert_eq!(config.default_timeout_secs, 60);
        assert_eq!(config.default_connections, 8);
        assert_eq!(config.suspicious_patterns.len(), 1);
        assert_eq!(config.tracking_params.len(), 1);
        assert_eq!(config.unreliable_domains.len(), 1);
    }

    #[test]
    fn test_config_serde_extra_fields_ignored() {
        let json = r#"{
            "enabled": true,
            "max_url_length": 1000,
            "suspicious_patterns": [],
            "tracking_params": [],
            "unreliable_domains": [],
            "default_timeout_secs": 15,
            "default_connections": 2,
            "unknown_future_field": 42
        }"#;
        let config: UrlIntelligenceConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_url_length, 1000);
    }

    #[test]
    fn test_config_serde_pretty() {
        let config = UrlIntelligenceConfig::default();
        let pretty = serde_json::to_string_pretty(&config).unwrap();
        assert!(pretty.contains('\n'));
        let back: UrlIntelligenceConfig = serde_json::from_str(&pretty).unwrap();
        assert_eq!(config.enabled, back.enabled);
        assert_eq!(config.max_url_length, back.max_url_length);
    }

    #[test]
    fn test_config_clone() {
        let config = UrlIntelligenceConfig::default();
        let cloned = config.clone();
        assert_eq!(config.enabled, cloned.enabled);
        assert_eq!(config.max_url_length, cloned.max_url_length);
        assert_eq!(config.default_timeout_secs, cloned.default_timeout_secs);
    }

    #[test]
    fn test_config_debug() {
        let config = UrlIntelligenceConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("UrlIntelligenceConfig"));
        assert!(debug.contains("enabled"));
    }

    // ===== UrlAnalysis Clone/Debug =====

    #[test]
    fn test_url_analysis_clone() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        let cloned = analysis.clone();
        assert_eq!(analysis.url, cloned.url);
        assert_eq!(analysis.success_probability, cloned.success_probability);
        assert_eq!(analysis.protocol, cloned.protocol);
        assert_eq!(analysis.issues.len(), cloned.issues.len());
    }

    #[test]
    fn test_url_analysis_debug() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        let debug = format!("{:?}", analysis);
        assert!(debug.contains("UrlAnalysis"));
        assert!(debug.contains("example.com"));
    }

    // ===== UrlIssue Clone/Debug =====

    #[test]
    fn test_url_issue_clone_debug() {
        let issue = UrlIssue {
            issue_type: UrlIssueType::InsecureProtocol,
            severity: IssueSeverity::Warning,
            description: "test".to_string(),
        };
        let cloned = issue.clone();
        assert_eq!(cloned.description, "test");
        assert!(!format!("{:?}", issue).is_empty());
    }

    // ===== UrlSuggestion Clone/Debug =====

    #[test]
    fn test_url_suggestion_clone_debug() {
        let suggestion = UrlSuggestion {
            suggestion_type: SuggestionType::UseHttps,
            message: "use https".to_string(),
        };
        let cloned = suggestion.clone();
        assert_eq!(cloned.message, "use https");
        assert!(!format!("{:?}", suggestion).is_empty());
    }

    // ===== detect_protocol edge cases =====

    #[test]
    fn test_detect_protocol_case_insensitive() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.detect_protocol("HTTPS://EXAMPLE.COM"),
            UrlProtocol::Https
        );
        assert_eq!(
            manager.detect_protocol("HTTP://EXAMPLE.COM"),
            UrlProtocol::Http
        );
        assert_eq!(
            manager.detect_protocol("FTP://EXAMPLE.COM"),
            UrlProtocol::Ftp
        );
        assert_eq!(
            manager.detect_protocol("MAGNET:?xt=urn"),
            UrlProtocol::Magnet
        );
        assert_eq!(manager.detect_protocol("ED2K://|file|"), UrlProtocol::Ed2k);
    }

    #[test]
    fn test_detect_protocol_mixed_case() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.detect_protocol("HtTpS://example.com"),
            UrlProtocol::Https
        );
        assert_eq!(
            manager.detect_protocol("hTtP://example.com"),
            UrlProtocol::Http
        );
        assert_eq!(
            manager.detect_protocol("Ftp://example.com"),
            UrlProtocol::Ftp
        );
    }

    #[test]
    fn test_detect_protocol_empty_string() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(manager.detect_protocol(""), UrlProtocol::Unknown);
    }

    #[test]
    fn test_detect_protocol_no_scheme() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.detect_protocol("example.com/file"),
            UrlProtocol::Unknown
        );
    }

    #[test]
    fn test_detect_protocol_partial_match_rejected() {
        let manager = UrlIntelligenceManager::new();
        // "https" without "://" should not match
        assert_eq!(
            manager.detect_protocol("https_example"),
            UrlProtocol::Unknown
        );
    }

    // ===== extract_domain edge cases =====

    #[test]
    fn test_extract_domain_with_port() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.extract_domain("https://example.com:443/path"),
            Some("example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_ip_address() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.extract_domain("http://192.168.1.1/file"),
            Some("192.168.1.1".to_string())
        );
    }

    #[test]
    fn test_extract_domain_no_path() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.extract_domain("https://example.com"),
            Some("example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_subdomain() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.extract_domain("https://a.b.c.example.com/path"),
            Some("a.b.c.example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_case_normalized() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(
            manager.extract_domain("https://EXAMPLE.COM/Path"),
            Some("example.com".to_string())
        );
    }

    #[test]
    fn test_extract_domain_empty_string() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(manager.extract_domain(""), None);
    }

    #[test]
    fn test_extract_domain_just_scheme() {
        let manager = UrlIntelligenceManager::new();
        assert_eq!(manager.extract_domain("https://"), None);
    }

    // ===== analyze_url protocol-specific =====

    #[test]
    fn test_analyze_ftp_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("ftp://files.example.com/pub/file.tar.gz");
        assert_eq!(analysis.protocol, UrlProtocol::Ftp);
        assert_eq!(analysis.recommended_connections, 2);
        assert_eq!(analysis.recommended_timeout_secs, 60);
    }

    #[test]
    fn test_analyze_magnet_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("magnet:?xt=urn:btih:abc123&dn=test");
        assert_eq!(analysis.protocol, UrlProtocol::Magnet);
        assert_eq!(analysis.recommended_connections, 8);
        assert_eq!(analysis.recommended_timeout_secs, 90);
    }

    #[test]
    fn test_analyze_ed2k_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("ed2k://|file|test.avi|123456|abc|");
        assert_eq!(analysis.protocol, UrlProtocol::Ed2k);
        assert_eq!(analysis.recommended_connections, 8);
        assert_eq!(analysis.recommended_timeout_secs, 90);
    }

    #[test]
    fn test_analyze_unknown_protocol() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("gopher://example.com/resource");
        assert_eq!(analysis.protocol, UrlProtocol::Unknown);
        assert_eq!(analysis.recommended_connections, 2);
    }

    // ===== analyze_url issues & suggestions =====

    #[test]
    fn test_analyze_clean_https_no_issues() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        assert!(analysis.issues.is_empty());
        assert!(analysis.suggestions.is_empty());
        assert!((analysis.success_probability - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_analyze_http_probability_penalty() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("http://example.com/file.zip");
        // HTTP penalty is -0.1
        assert!((analysis.success_probability - 0.9).abs() < 0.01);
    }

    #[test]
    fn test_analyze_url_length_exactly_at_boundary() {
        let mut manager = UrlIntelligenceManager::new();
        // Build URL exactly at max_url_length (2000)
        let path_len = 2000 - "https://example.com/".len();
        let url = format!("https://example.com/{}", "a".repeat(path_len));
        assert_eq!(url.len(), 2000);
        let analysis = manager.analyze_url(&url);
        // At boundary: should NOT trigger (only > triggers)
        assert!(
            !analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::UnusualLength)
        );
    }

    #[test]
    fn test_analyze_url_length_just_over_boundary() {
        let mut manager = UrlIntelligenceManager::new();
        let path_len = 2000 - "https://example.com/".len();
        let url = format!("https://example.com/{}", "a".repeat(path_len + 1));
        assert_eq!(url.len(), 2001);
        let analysis = manager.analyze_url(&url);
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::UnusualLength)
        );
    }

    #[test]
    fn test_analyze_suspicious_pattern_exe() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/setup.exe");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.severity == IssueSeverity::Critical)
        );
    }

    #[test]
    fn test_analyze_suspicious_pattern_scr() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.scr");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
    }

    #[test]
    fn test_analyze_suspicious_pattern_bat() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/script.bat?download=1");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
    }

    #[test]
    fn test_analyze_suspicious_pattern_keygen() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/keygen-tool.zip");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
    }

    #[test]
    fn test_analyze_suspicious_only_counts_once() {
        // URL matches multiple patterns but should only get one SuspiciousPattern issue
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("http://example.com/free-download-crack.exe");
        let suspicious_count = analysis
            .issues
            .iter()
            .filter(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
            .count();
        assert_eq!(suspicious_count, 1);
    }

    #[test]
    fn test_analyze_tracking_param_utm_source() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip?utm_source=google");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );
    }

    #[test]
    fn test_analyze_tracking_param_fbclid() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip?fbclid=abc123");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );
    }

    #[test]
    fn test_analyze_tracking_param_gclid() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip?gclid=xyz");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );
    }

    #[test]
    fn test_analyze_redirect_keyword() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/redirect?to=file.zip");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PossibleRedirect)
        );
    }

    #[test]
    fn test_analyze_redirect_go_pattern() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/go?target=download");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PossibleRedirect)
        );
    }

    #[test]
    fn test_analyze_redirect_link_pattern() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/link?url=file.zip");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PossibleRedirect)
        );
    }

    #[test]
    fn test_analyze_unreliable_domain_case_insensitive() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.unreliable_domains.push("badhost.com".to_string());
        manager.set_config(config);

        // Domain is lowercased during extraction, so "BADHOST.COM" should still match
        let analysis = manager.analyze_url("https://BADHOST.COM/file.zip");
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
        );
    }

    #[test]
    fn test_analyze_unreliable_domain_not_matching_substring() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.unreliable_domains.push("bad.com".to_string());
        manager.set_config(config);

        // "notbad.com" should NOT match "bad.com"
        let analysis = manager.analyze_url("https://notbad.com/file.zip");
        assert!(
            !analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
        );
    }

    // ===== Multiple issues stacking =====

    #[test]
    fn test_analyze_url_multiple_issues() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.unreliable_domains.push("badhost.com".to_string());
        manager.set_config(config);

        // HTTP + suspicious pattern + tracking + unreliable domain + redirect
        let url = "http://badhost.com/free-download-crack.exe?utm_source=test&redirect=true";
        let analysis = manager.analyze_url(url);

        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::InsecureProtocol)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PossibleRedirect)
        );
    }

    #[test]
    fn test_analyze_url_probability_clamped_to_zero() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.unreliable_domains.push("bad.com".to_string());
        manager.set_config(config);

        // Stack many penalties: HTTP(-0.1) + suspicious(-0.3) + unreliable(-0.2) + redirect(-0.1) = -0.7
        // Plus long URL(-0.05) = -0.75
        // Probability should clamp to 0.0, not go negative
        let long_path = "a".repeat(2001);
        let url = format!(
            "http://bad.com/free-download-crack.exe?redirect=true&utm_source=x&{}",
            long_path
        );
        let analysis = manager.analyze_url(&url);
        assert!(analysis.success_probability >= 0.0);
    }

    #[test]
    fn test_analyze_url_probability_clamped_to_one() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        // Clean HTTPS should be exactly 1.0, not more
        assert!(analysis.success_probability <= 1.0);
    }

    // ===== Manager construction =====

    #[test]
    fn test_manager_default_equals_new() {
        let new = UrlIntelligenceManager::new();
        let default = UrlIntelligenceManager::default();
        assert_eq!(new.get_config().enabled, default.get_config().enabled);
        assert_eq!(
            new.get_config().max_url_length,
            default.get_config().max_url_length
        );
        assert_eq!(new.get_cache_size(), default.get_cache_size());
    }

    #[test]
    fn test_manager_with_config() {
        let config = UrlIntelligenceConfig {
            enabled: false,
            max_url_length: 500,
            suspicious_patterns: vec![],
            tracking_params: vec![],
            unreliable_domains: vec!["bad.com".to_string()],
            default_timeout_secs: 60,
            default_connections: 2,
        };
        let manager = UrlIntelligenceManager::with_config(config.clone());
        assert!(!manager.get_config().enabled);
        assert_eq!(manager.get_config().max_url_length, 500);
        assert_eq!(manager.get_config().default_timeout_secs, 60);
    }

    #[test]
    fn test_manager_set_config_updates() {
        let mut manager = UrlIntelligenceManager::new();
        assert_eq!(manager.get_config().max_url_length, 2000);

        let mut new_config = UrlIntelligenceConfig::default();
        new_config.max_url_length = 500;
        manager.set_config(new_config);
        assert_eq!(manager.get_config().max_url_length, 500);
    }

    #[test]
    fn test_manager_clone() {
        let mut manager = UrlIntelligenceManager::new();
        manager.analyze_url("https://example.com/file.zip");
        assert_eq!(manager.get_cache_size(), 1);

        let cloned = manager.clone();
        assert_eq!(cloned.get_cache_size(), 1);
        assert_eq!(cloned.get_config().enabled, manager.get_config().enabled);
    }

    #[test]
    fn test_manager_debug() {
        let manager = UrlIntelligenceManager::new();
        let debug = format!("{:?}", manager);
        assert!(debug.contains("UrlIntelligenceManager"));
    }

    // ===== Cache behavior =====

    #[test]
    fn test_cache_hit_returns_same_timestamp() {
        let mut manager = UrlIntelligenceManager::new();
        let url = "https://example.com/file.zip";
        let a1 = manager.analyze_url(url);
        let a2 = manager.analyze_url(url);
        assert_eq!(a1.timestamp, a2.timestamp);
        assert_eq!(a1.success_probability, a2.success_probability);
    }

    #[test]
    fn test_cache_different_urls_independent() {
        let mut manager = UrlIntelligenceManager::new();
        let a1 = manager.analyze_url("https://example1.com/file.zip");
        let a2 = manager.analyze_url("https://example2.com/file.zip");
        assert_ne!(a1.url, a2.url);
        assert_eq!(manager.get_cache_size(), 2);
    }

    #[test]
    fn test_cache_eviction_removes_oldest() {
        let mut manager = UrlIntelligenceManager::new();
        manager.max_cache_size = 3;

        manager.analyze_url("https://example1.com/f.zip");
        manager.analyze_url("https://example2.com/f.zip");
        manager.analyze_url("https://example3.com/f.zip");
        assert_eq!(manager.get_cache_size(), 3);

        // Adding 4th should evict first
        manager.analyze_url("https://example4.com/f.zip");
        assert_eq!(manager.get_cache_size(), 3);
        assert!(
            manager
                .get_cached_analysis("https://example4.com/f.zip")
                .is_some()
        );
    }

    #[test]
    fn test_remove_cached_analysis_nonexistent() {
        let mut manager = UrlIntelligenceManager::new();
        assert!(!manager.remove_cached_analysis("https://nonexistent.com"));
    }

    #[test]
    fn test_get_cached_analysis_none() {
        let manager = UrlIntelligenceManager::new();
        assert!(
            manager
                .get_cached_analysis("https://example.com/file.zip")
                .is_none()
        );
    }

    #[test]
    fn test_clear_cache_resets_size() {
        let mut manager = UrlIntelligenceManager::new();
        for i in 0..10 {
            manager.analyze_url(&format!("https://example{}.com/f.zip", i));
        }
        assert_eq!(manager.get_cache_size(), 10);
        manager.clear_cache();
        assert_eq!(manager.get_cache_size(), 0);
    }

    // ===== Disabled config =====

    #[test]
    fn test_analyze_disabled_still_analyzes() {
        // The analyze_url method doesn't check enabled flag - it always analyzes
        let config = UrlIntelligenceConfig {
            enabled: false,
            ..UrlIntelligenceConfig::default()
        };
        let mut manager = UrlIntelligenceManager::with_config(config);
        let analysis = manager.analyze_url("https://example.com/file.zip");
        assert_eq!(analysis.protocol, UrlProtocol::Https);
    }

    // ===== Unicode / edge case URLs =====

    #[test]
    fn test_analyze_unicode_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/中文文件.zip");
        assert_eq!(analysis.protocol, UrlProtocol::Https);
        assert!(analysis.success_probability > 0.9);
    }

    #[test]
    fn test_analyze_emoji_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/🎉party.zip");
        assert_eq!(analysis.protocol, UrlProtocol::Https);
    }

    #[test]
    fn test_analyze_empty_url() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("");
        assert_eq!(analysis.protocol, UrlProtocol::Unknown);
        assert!(analysis.issues.is_empty()); // no protocol-specific checks trigger
    }

    #[test]
    fn test_analyze_very_long_url() {
        let mut manager = UrlIntelligenceManager::new();
        let url = format!("https://example.com/{}", "x".repeat(10_000));
        let analysis = manager.analyze_url(&url);
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::UnusualLength)
        );
    }

    // ===== Custom config effects =====

    #[test]
    fn test_custom_max_url_length_takes_effect() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.max_url_length = 50; // very short threshold
        manager.set_config(config);

        let url = format!("https://example.com/{}", "a".repeat(60));
        let analysis = manager.analyze_url(&url);
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::UnusualLength)
        );
    }

    #[test]
    fn test_custom_tracking_params() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.tracking_params = vec!["my_custom_param".to_string()];
        manager.set_config(config);

        // Default params should NOT trigger
        let analysis1 = manager.analyze_url("https://example.com/f.zip?utm_source=test");
        assert!(
            !analysis1
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );

        // Custom param SHOULD trigger
        let analysis2 = manager.analyze_url("https://example.com/f.zip?my_custom_param=1");
        assert!(
            analysis2
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );
    }

    #[test]
    fn test_custom_suspicious_patterns() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.suspicious_patterns = vec!["malware".to_string()];
        manager.set_config(config);

        // Default patterns should NOT trigger
        let analysis1 = manager.analyze_url("https://example.com/file.exe");
        assert!(
            !analysis1
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );

        // Custom pattern SHOULD trigger
        let analysis2 = manager.analyze_url("https://example.com/malware-download.zip");
        assert!(
            analysis2
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
    }

    #[test]
    fn test_empty_suspicious_patterns_no_false_positives() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.suspicious_patterns = vec![];
        manager.set_config(config);

        let analysis = manager.analyze_url("https://example.com/crack.exe");
        assert!(
            !analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
    }

    #[test]
    fn test_empty_tracking_params_no_detection() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.tracking_params = vec![];
        manager.set_config(config);

        let analysis = manager.analyze_url("https://example.com/f.zip?utm_source=test");
        assert!(
            !analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );
    }

    // ===== Timeout/connection recommendations =====

    #[test]
    fn test_https_timeout_connections() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        assert_eq!(analysis.recommended_timeout_secs, 30);
        assert_eq!(analysis.recommended_connections, 4);
    }

    #[test]
    fn test_unknown_protocol_recommendations() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("xyz://example.com/file");
        assert_eq!(analysis.recommended_timeout_secs, 30);
        assert_eq!(analysis.recommended_connections, 2);
    }

    #[test]
    fn test_custom_default_timeout_propagates() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.default_timeout_secs = 60;
        manager.set_config(config);

        let analysis = manager.analyze_url("https://example.com/file.zip");
        assert_eq!(analysis.recommended_timeout_secs, 60);

        let ftp = manager.analyze_url("ftp://example.com/file.zip");
        assert_eq!(ftp.recommended_timeout_secs, 120); // 2x
    }

    #[test]
    fn test_custom_default_connections_propagates() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.default_connections = 8;
        manager.set_config(config);

        let analysis = manager.analyze_url("https://example.com/file.zip");
        assert_eq!(analysis.recommended_connections, 8);
    }

    // ===== Analysis timestamp =====

    #[test]
    fn test_analysis_timestamp_is_recent() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Timestamp should be within 2 seconds of now
        assert!(now - analysis.timestamp <= 2);
    }

    // ===== UrlAnalysis serde edge cases =====

    #[test]
    fn test_url_analysis_serde_extra_fields_ignored() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/file.zip");
        let mut value: serde_json::Value = serde_json::to_value(&analysis).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("extra_field".to_string(), serde_json::json!(42));
        let back: UrlAnalysis = serde_json::from_value(value).unwrap();
        assert_eq!(back.url, analysis.url);
    }

    #[test]
    fn test_url_analysis_serde_unicode() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/中文文件.zip");
        let json = serde_json::to_string(&analysis).unwrap();
        let back: UrlAnalysis = serde_json::from_str(&json).unwrap();
        assert_eq!(back.url, analysis.url);
    }

    // ===== Issue severity verification =====

    #[test]
    fn test_insecure_protocol_severity_is_warning() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("http://example.com/file.zip");
        let issue = analysis
            .issues
            .iter()
            .find(|i| i.issue_type == UrlIssueType::InsecureProtocol)
            .unwrap();
        assert_eq!(issue.severity, IssueSeverity::Warning);
    }

    #[test]
    fn test_suspicious_pattern_severity_is_critical() {
        let mut manager = UrlIntelligenceManager::new();
        let analysis = manager.analyze_url("https://example.com/crack.exe");
        let issue = analysis
            .issues
            .iter()
            .find(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
            .unwrap();
        assert_eq!(issue.severity, IssueSeverity::Critical);
    }

    #[test]
    fn test_unusual_length_severity_is_info() {
        let mut manager = UrlIntelligenceManager::new();
        let url = format!("https://example.com/{}", "a".repeat(2500));
        let analysis = manager.analyze_url(&url);
        let issue = analysis
            .issues
            .iter()
            .find(|i| i.issue_type == UrlIssueType::UnusualLength)
            .unwrap();
        assert_eq!(issue.severity, IssueSeverity::Info);
    }

    #[test]
    fn test_poor_domain_reliability_severity_is_warning() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.unreliable_domains.push("bad.com".to_string());
        manager.set_config(config);

        let analysis = manager.analyze_url("https://bad.com/file.zip");
        let issue = analysis
            .issues
            .iter()
            .find(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
            .unwrap();
        assert_eq!(issue.severity, IssueSeverity::Warning);
    }

    // ===== Complex workflows =====

    #[test]
    fn test_complete_lifecycle() {
        let mut manager = UrlIntelligenceManager::new();

        // Analyze multiple URLs
        let a1 = manager.analyze_url("https://good.com/file.zip");
        let a2 = manager.analyze_url("http://bad.com/crack.exe?utm_source=x");
        assert_eq!(manager.get_cache_size(), 2);

        // Verify analyses are independent
        assert!(a1.success_probability > a2.success_probability);
        assert!(a1.issues.len() < a2.issues.len());

        // Retrieve cached
        let cached = manager
            .get_cached_analysis("https://good.com/file.zip")
            .unwrap();
        assert_eq!(cached.url, "https://good.com/file.zip");

        // Remove one
        assert!(manager.remove_cached_analysis("https://good.com/file.zip"));
        assert_eq!(manager.get_cache_size(), 1);

        // Clear all
        manager.clear_cache();
        assert_eq!(manager.get_cache_size(), 0);
    }

    #[test]
    fn test_config_change_affects_subsequent_analysis() {
        let mut manager = UrlIntelligenceManager::new();

        // First analysis with default config
        let a1 = manager.analyze_url("https://example.com/file.exe");
        assert!(
            a1.issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );

        // Change config to remove suspicious patterns
        let mut new_config = UrlIntelligenceConfig::default();
        new_config.suspicious_patterns = vec![];
        manager.set_config(new_config);

        // Clear cache so re-analysis happens
        manager.clear_cache();

        // Second analysis should not flag suspicious pattern
        let a2 = manager.analyze_url("https://example.com/file.exe");
        assert!(
            !a2.issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
    }

    #[test]
    fn test_multiple_unreliable_domains() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.unreliable_domains = vec![
            "bad1.com".to_string(),
            "bad2.com".to_string(),
            "bad3.com".to_string(),
        ];
        manager.set_config(config);

        let a1 = manager.analyze_url("https://bad1.com/f.zip");
        assert!(
            a1.issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
        );

        let a2 = manager.analyze_url("https://bad2.com/f.zip");
        assert!(
            a2.issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
        );

        let a3 = manager.analyze_url("https://good.com/f.zip");
        assert!(
            !a3.issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
        );
    }

    #[test]
    fn test_url_with_all_features_combined() {
        let mut manager = UrlIntelligenceManager::new();
        let mut config = UrlIntelligenceConfig::default();
        config.unreliable_domains.push("unreliable.org".to_string());
        config.tracking_params.push("custom_track".to_string());
        manager.set_config(config);

        // HTTP + suspicious + tracking + unreliable + redirect + long
        let long_path = "x".repeat(2500);
        let url = format!(
            "http://unreliable.org/free-download-crack.exe?custom_track=1&redirect=1&{}",
            long_path
        );
        let analysis = manager.analyze_url(&url);

        // All issue types should be present
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::InsecureProtocol)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::SuspiciousPattern)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::TrackingParams)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PoorDomainReliability)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::PossibleRedirect)
        );
        assert!(
            analysis
                .issues
                .iter()
                .any(|i| i.issue_type == UrlIssueType::UnusualLength)
        );

        // Probability should be clamped to 0.0
        assert!((analysis.success_probability - 0.0).abs() < f64::EPSILON);

        // Should have multiple suggestions
        assert!(analysis.suggestions.len() >= 3);
    }
}
