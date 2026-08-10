//! Download Preflight Check System (Phase 124)
//!
//! Runs a series of checks before starting a download to detect potential issues early.
//! Checks include DNS resolution, disk space availability, URL reachability (HEAD request),
//! and proxy connectivity. Results are aggregated into a report with pass/warn/fail status.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Overall preflight check status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreflightStatus {
    /// All checks passed
    Pass,
    /// Non-critical checks had warnings
    Warn,
    /// One or more critical checks failed
    Fail,
}

impl std::fmt::Display for PreflightStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PreflightStatus::Pass => write!(f, "PASS"),
            PreflightStatus::Warn => write!(f, "WARN"),
            PreflightStatus::Fail => write!(f, "FAIL"),
        }
    }
}

/// Individual check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    /// Name of the check
    pub name: String,
    /// Whether this check is critical (failure blocks download)
    pub critical: bool,
    /// Check outcome
    pub status: PreflightStatus,
    /// Human-readable message
    pub message: String,
    /// Time taken for this check in milliseconds
    pub duration_ms: u64,
}

/// Input data needed to run preflight checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightInput {
    /// URL to download
    pub url: String,
    /// Intended save path directory
    pub save_dir: PathBuf,
    /// Expected file size in bytes (if known)
    pub expected_size: Option<u64>,
    /// Proxy URL (if configured)
    pub proxy_url: Option<String>,
    /// Protocol of the download
    pub protocol: PreflightProtocol,
}

/// Protocol classification for preflight checks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreflightProtocol {
    Http,
    Torrent,
    Ed2k,
    P2p,
}

/// Aggregated preflight check report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightReport {
    /// URL that was checked
    pub url: String,
    /// When the check was performed
    pub checked_at: chrono::DateTime<chrono::Utc>,
    /// Overall status (worst of all checks)
    pub overall: PreflightStatus,
    /// Individual check results
    pub checks: Vec<CheckResult>,
    /// Total time for all checks in milliseconds
    pub total_duration_ms: u64,
    /// Number of passed checks
    pub passed: usize,
    /// Number of warnings
    pub warnings: usize,
    /// Number of failures
    pub failures: usize,
}

impl PreflightReport {
    /// Format the report for human-readable display
    pub fn format_report(&self) -> String {
        let mut out = String::new();
        let icon = match self.overall {
            PreflightStatus::Pass => "✅",
            PreflightStatus::Warn => "⚠️",
            PreflightStatus::Fail => "❌",
        };
        out.push_str(&format!(
            "{} Preflight Check: {} ({}ms)\n",
            icon, self.overall, self.total_duration_ms
        ));
        out.push_str(&format!("URL: {}\n\n", self.url));
        for check in &self.checks {
            let ci = match check.status {
                PreflightStatus::Pass => "✓",
                PreflightStatus::Warn => "⚠",
                PreflightStatus::Fail => "✗",
            };
            let crit = if check.critical { " [critical]" } else { "" };
            out.push_str(&format!(
                "  {} {} {}{} ({}ms)\n",
                ci, check.name, check.status, crit, check.duration_ms
            ));
            out.push_str(&format!("    {}\n", check.message));
        }
        out.push_str(&format!(
            "\nSummary: {} passed, {} warnings, {} failed",
            self.passed, self.warnings, self.failures
        ));
        out
    }
}

/// Configuration for preflight checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightConfig {
    /// Whether preflight checks are enabled
    pub enabled: bool,
    /// Whether to check DNS resolution
    pub check_dns: bool,
    /// Whether to check disk space
    pub check_disk_space: bool,
    /// Whether to check URL reachability (HTTP HEAD)
    pub check_url_reachable: bool,
    /// Whether to check proxy connectivity (if proxy configured)
    pub check_proxy: bool,
    /// Minimum free disk space in bytes (default: 100MB)
    pub min_free_disk_bytes: u64,
    /// Timeout for each check in seconds
    pub check_timeout_secs: u64,
    /// Whether to block download if preflight fails
    pub block_on_fail: bool,
}

impl Default for PreflightConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_dns: true,
            check_disk_space: true,
            check_url_reachable: true,
            check_proxy: true,
            min_free_disk_bytes: 100 * 1024 * 1024, // 100 MB
            check_timeout_secs: 10,
            block_on_fail: false,
        }
    }
}

/// Preflight check manager
pub struct PreflightChecker {
    config: PreflightConfig,
    config_path: PathBuf,
}

impl PreflightChecker {
    /// Create a new PreflightChecker with the given data directory
    pub fn new(data_dir: &Path) -> Self {
        let config_path = data_dir.join("preflight_config.json");
        let config = load_preflight_config(&config_path).unwrap_or_default();
        Self {
            config,
            config_path,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &PreflightConfig {
        &self.config
    }

    /// Update configuration and persist
    pub fn set_config(&mut self, config: PreflightConfig) -> Result<(), PreflightCheckError> {
        save_preflight_config(&self.config_path, &config)?;
        self.config = config;
        Ok(())
    }

    /// Run all enabled preflight checks
    pub async fn run_checks(&self, input: &PreflightInput) -> PreflightReport {
        let start = Instant::now();
        let mut checks = Vec::new();

        // DNS check (for HTTP/HTTPS URLs)
        if self.config.check_dns && is_http_url(&input.url) {
            checks.push(self.check_dns(&input.url).await);
        }

        // Disk space check
        if self.config.check_disk_space {
            checks.push(
                self.check_disk_space(&input.save_dir, input.expected_size)
                    .await,
            );
        }

        // URL reachability check (HTTP HEAD)
        if self.config.check_url_reachable && is_http_url(&input.url) {
            checks.push(self.check_url_reachable(&input.url).await);
        }

        // Proxy connectivity check
        if self.config.check_proxy
            && let Some(proxy_url) = &input.proxy_url
        {
            checks.push(self.check_proxy(proxy_url).await);
        }
        let total_duration_ms = start.elapsed().as_millis() as u64;

        let passed = checks
            .iter()
            .filter(|c| c.status == PreflightStatus::Pass)
            .count();
        let warnings = checks
            .iter()
            .filter(|c| c.status == PreflightStatus::Warn)
            .count();
        let failures = checks
            .iter()
            .filter(|c| c.status == PreflightStatus::Fail)
            .count();

        // Overall is the worst status
        let overall = if failures > 0 {
            PreflightStatus::Fail
        } else if warnings > 0 {
            PreflightStatus::Warn
        } else {
            PreflightStatus::Pass
        };

        PreflightReport {
            url: input.url.clone(),
            checked_at: chrono::Utc::now(),
            overall,
            checks,
            total_duration_ms,
            passed,
            warnings,
            failures,
        }
    }

    /// Check DNS resolution for the URL's host
    async fn check_dns(&self, url: &str) -> CheckResult {
        let start = Instant::now();
        let host = match extract_host(url) {
            Some(h) => h,
            None => {
                return CheckResult {
                    name: "DNS Resolution".into(),
                    critical: true,
                    status: PreflightStatus::Fail,
                    message: "Could not extract hostname from URL".into(),
                    duration_ms: start.elapsed().as_millis() as u64,
                };
            }
        };

        let timeout = Duration::from_secs(self.config.check_timeout_secs);
        match tokio::time::timeout(timeout, tokio::net::lookup_host(format!("{}:443", host))).await
        {
            Ok(Ok(addrs)) => {
                let addr_count = addrs.count();
                CheckResult {
                    name: "DNS Resolution".into(),
                    critical: true,
                    status: PreflightStatus::Pass,
                    message: format!("{} resolved to {} address(es)", host, addr_count),
                    duration_ms: start.elapsed().as_millis() as u64,
                }
            }
            Ok(Err(e)) => CheckResult {
                name: "DNS Resolution".into(),
                critical: true,
                status: PreflightStatus::Fail,
                message: format!("DNS lookup failed for {}: {}", host, e),
                duration_ms: start.elapsed().as_millis() as u64,
            },
            Err(_) => CheckResult {
                name: "DNS Resolution".into(),
                critical: true,
                status: PreflightStatus::Fail,
                message: format!(
                    "DNS lookup timed out for {} ({}s)",
                    host, self.config.check_timeout_secs
                ),
                duration_ms: start.elapsed().as_millis() as u64,
            },
        }
    }

    /// Check available disk space at the save directory
    async fn check_disk_space(&self, save_dir: &Path, expected_size: Option<u64>) -> CheckResult {
        let start = Instant::now();

        // Use tokio::fs to avoid blocking
        let result = tokio::task::spawn_blocking({
            let dir = save_dir.to_path_buf();
            move || get_available_space(&dir)
        })
        .await;

        match result {
            Ok(Ok(available)) => {
                let required = expected_size.unwrap_or(self.config.min_free_disk_bytes);
                let min_required = required.max(self.config.min_free_disk_bytes);

                if available >= min_required {
                    CheckResult {
                        name: "Disk Space".into(),
                        critical: true,
                        status: PreflightStatus::Pass,
                        message: format!(
                            "{} free (need {})",
                            format_bytes(available),
                            format_bytes(min_required)
                        ),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                } else if available >= self.config.min_free_disk_bytes {
                    CheckResult {
                        name: "Disk Space".into(),
                        critical: false,
                        status: PreflightStatus::Warn,
                        message: format!(
                            "{} free, expected file needs {} (minimum {} available)",
                            format_bytes(available),
                            format_bytes(required),
                            format_bytes(self.config.min_free_disk_bytes)
                        ),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                } else {
                    CheckResult {
                        name: "Disk Space".into(),
                        critical: true,
                        status: PreflightStatus::Fail,
                        message: format!(
                            "Only {} free, need at least {}",
                            format_bytes(available),
                            format_bytes(min_required)
                        ),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                }
            }
            Ok(Err(e)) => CheckResult {
                name: "Disk Space".into(),
                critical: false,
                status: PreflightStatus::Warn,
                message: format!("Could not check disk space: {}", e),
                duration_ms: start.elapsed().as_millis() as u64,
            },
            Err(_) => CheckResult {
                name: "Disk Space".into(),
                critical: false,
                status: PreflightStatus::Warn,
                message: "Disk space check timed out".into(),
                duration_ms: start.elapsed().as_millis() as u64,
            },
        }
    }

    /// Check URL reachability via HTTP HEAD request
    async fn check_url_reachable(&self, url: &str) -> CheckResult {
        let start = Instant::now();
        let timeout = Duration::from_secs(self.config.check_timeout_secs);

        let client_result = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build();

        let client = match client_result {
            Ok(c) => c,
            Err(e) => {
                return CheckResult {
                    name: "URL Reachability".into(),
                    critical: false,
                    status: PreflightStatus::Warn,
                    message: format!("Failed to create HTTP client: {}", e),
                    duration_ms: start.elapsed().as_millis() as u64,
                };
            }
        };

        match client.head(url).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT {
                    let content_length = resp
                        .headers()
                        .get(reqwest::header::CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    let size_msg = content_length
                        .map(|s| format!(", size: {}", format_bytes(s)))
                        .unwrap_or_default();
                    CheckResult {
                        name: "URL Reachability".into(),
                        critical: false,
                        status: PreflightStatus::Pass,
                        message: format!("URL reachable (HTTP {}{})", status.as_u16(), size_msg),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                } else if status == reqwest::StatusCode::METHOD_NOT_ALLOWED
                    || status == reqwest::StatusCode::FORBIDDEN
                {
                    // HEAD not allowed - not necessarily broken
                    CheckResult {
                        name: "URL Reachability".into(),
                        critical: false,
                        status: PreflightStatus::Warn,
                        message: format!(
                            "Server returned HTTP {} for HEAD (may still work with GET)",
                            status.as_u16()
                        ),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                } else {
                    CheckResult {
                        name: "URL Reachability".into(),
                        critical: false,
                        status: PreflightStatus::Warn,
                        message: format!("Server returned HTTP {}", status.as_u16()),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                }
            }
            Err(e) => {
                if e.is_timeout() {
                    CheckResult {
                        name: "URL Reachability".into(),
                        critical: false,
                        status: PreflightStatus::Warn,
                        message: format!(
                            "Connection timed out after {}s",
                            self.config.check_timeout_secs
                        ),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                } else if e.is_connect() {
                    CheckResult {
                        name: "URL Reachability".into(),
                        critical: true,
                        status: PreflightStatus::Fail,
                        message: format!("Connection failed: {}", e),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                } else {
                    CheckResult {
                        name: "URL Reachability".into(),
                        critical: false,
                        status: PreflightStatus::Warn,
                        message: format!("Request failed: {}", e),
                        duration_ms: start.elapsed().as_millis() as u64,
                    }
                }
            }
        }
    }

    /// Check proxy connectivity
    async fn check_proxy(&self, proxy_url: &str) -> CheckResult {
        let start = Instant::now();
        let timeout = Duration::from_secs(self.config.check_timeout_secs);

        // Basic proxy URL validation
        if !proxy_url.starts_with("http://")
            && !proxy_url.starts_with("https://")
            && !proxy_url.starts_with("socks5://")
        {
            return CheckResult {
                name: "Proxy Connectivity".into(),
                critical: false,
                status: PreflightStatus::Warn,
                message: format!("Unrecognized proxy scheme: {}", proxy_url),
                duration_ms: start.elapsed().as_millis() as u64,
            };
        }

        // Try to extract host and do a TCP connect to verify proxy is reachable
        if let Some(host_port) = extract_host_port(proxy_url) {
            match tokio::time::timeout(timeout, tokio::net::lookup_host(&host_port)).await {
                Ok(Ok(mut addrs)) => {
                    if let Some(addr) = addrs.next() {
                        match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr))
                            .await
                        {
                            Ok(Ok(_)) => CheckResult {
                                name: "Proxy Connectivity".into(),
                                critical: false,
                                status: PreflightStatus::Pass,
                                message: format!("Proxy {} reachable", proxy_url),
                                duration_ms: start.elapsed().as_millis() as u64,
                            },
                            Ok(Err(e)) => CheckResult {
                                name: "Proxy Connectivity".into(),
                                critical: false,
                                status: PreflightStatus::Warn,
                                message: format!("Proxy TCP connect failed: {}", e),
                                duration_ms: start.elapsed().as_millis() as u64,
                            },
                            Err(_) => CheckResult {
                                name: "Proxy Connectivity".into(),
                                critical: false,
                                status: PreflightStatus::Warn,
                                message: format!(
                                    "Proxy connection timed out ({}s)",
                                    self.config.check_timeout_secs
                                ),
                                duration_ms: start.elapsed().as_millis() as u64,
                            },
                        }
                    } else {
                        CheckResult {
                            name: "Proxy Connectivity".into(),
                            critical: false,
                            status: PreflightStatus::Warn,
                            message: format!("Could not resolve proxy host: {}", proxy_url),
                            duration_ms: start.elapsed().as_millis() as u64,
                        }
                    }
                }
                Ok(Err(e)) => CheckResult {
                    name: "Proxy Connectivity".into(),
                    critical: false,
                    status: PreflightStatus::Warn,
                    message: format!("DNS resolution failed for proxy: {}", e),
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                Err(_) => CheckResult {
                    name: "Proxy Connectivity".into(),
                    critical: false,
                    status: PreflightStatus::Warn,
                    message: "Proxy check timed out".into(),
                    duration_ms: start.elapsed().as_millis() as u64,
                },
            }
        } else {
            CheckResult {
                name: "Proxy Connectivity".into(),
                critical: false,
                status: PreflightStatus::Warn,
                message: format!("Could not parse proxy URL: {}", proxy_url),
                duration_ms: start.elapsed().as_millis() as u64,
            }
        }
    }
}

/// Error type for preflight operations
#[derive(Debug, thiserror::Error)]
pub enum PreflightCheckError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

// --- Persistence ---

fn save_preflight_config(path: &Path, config: &PreflightConfig) -> Result<(), PreflightCheckError> {
    let json = serde_json::to_string_pretty(config)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Atomic write
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn load_preflight_config(path: &Path) -> Option<PreflightConfig> {
    let data = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

// --- Utility functions ---

fn is_http_url(url: &str) -> bool {
    url.starts_with("http://") || url.starts_with("https://")
}

fn extract_host(url: &str) -> Option<String> {
    // Strip scheme
    let without_scheme = url.find("://").map(|i| &url[i + 3..]).unwrap_or(url);
    // Take before first / or :
    let host_part = without_scheme.split('/').next().unwrap_or(without_scheme);
    let host = host_part.split(':').next().unwrap_or(host_part);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

fn extract_host_port(url: &str) -> Option<String> {
    let without_scheme = url.find("://").map(|i| &url[i + 3..]).unwrap_or(url);
    let host_part = without_scheme.split('/').next().unwrap_or(without_scheme);
    if host_part.is_empty() {
        None
    } else {
        Some(host_part.to_string())
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Get available disk space for a path (synchronous, for spawn_blocking)
fn get_available_space(path: &Path) -> Result<u64, String> {
    // Use statvfs on Linux
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::mem::zeroed;
        use std::os::unix::ffi::OsStrExt;

        let path_bytes = path.as_os_str().as_bytes();
        let c_path = CString::new(path_bytes).map_err(|e| e.to_string())?;
        let mut stat: libc::statvfs = unsafe { zeroed() };
        let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
        if ret != 0 {
            return Err(format!(
                "statvfs failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
    }
    #[cfg(not(unix))]
    {
        Err("Disk space check not supported on this platform".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn test_config() -> PreflightConfig {
        PreflightConfig {
            enabled: true,
            check_dns: true,
            check_disk_space: true,
            check_url_reachable: true,
            check_proxy: true,
            min_free_disk_bytes: 1024, // 1 KB for testing
            check_timeout_secs: 3,
            block_on_fail: false,
        }
    }

    #[test]
    fn test_preflight_status_display() {
        assert_eq!(PreflightStatus::Pass.to_string(), "PASS");
        assert_eq!(PreflightStatus::Warn.to_string(), "WARN");
        assert_eq!(PreflightStatus::Fail.to_string(), "FAIL");
    }

    #[test]
    fn test_preflight_config_default() {
        let config = PreflightConfig::default();
        assert!(config.enabled);
        assert!(config.check_dns);
        assert!(config.check_disk_space);
        assert!(config.check_url_reachable);
        assert!(config.check_proxy);
        assert_eq!(config.min_free_disk_bytes, 100 * 1024 * 1024);
        assert_eq!(config.check_timeout_secs, 10);
        assert!(!config.block_on_fail);
    }

    #[test]
    fn test_preflight_config_serialization() {
        let config = test_config();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PreflightConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.min_free_disk_bytes, config.min_free_disk_bytes);
        assert_eq!(deserialized.check_timeout_secs, config.check_timeout_secs);
    }

    #[test]
    fn test_config_persistence() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("preflight_config.json");

        let config = test_config();
        save_preflight_config(&path, &config).unwrap();
        let loaded = load_preflight_config(&path).unwrap();
        assert_eq!(loaded.enabled, config.enabled);
        assert_eq!(loaded.min_free_disk_bytes, config.min_free_disk_bytes);
    }

    #[test]
    fn test_config_persistence_missing_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent.json");
        assert!(load_preflight_config(&path).is_none());
    }

    #[test]
    fn test_is_http_url() {
        assert!(is_http_url("http://example.com/file.zip"));
        assert!(is_http_url("https://example.com/file.zip"));
        assert!(!is_http_url("magnet:?xt=urn:btih:abc"));
        assert!(!is_http_url("ed2k://|file|test|100|abc|/"));
    }

    #[test]
    fn test_extract_host() {
        assert_eq!(
            extract_host("https://example.com/path/file.zip"),
            Some("example.com".into())
        );
        assert_eq!(
            extract_host("http://example.com:8080/path"),
            Some("example.com".into())
        );
        assert_eq!(
            extract_host("https://sub.domain.org"),
            Some("sub.domain.org".into())
        );
        assert_eq!(extract_host("not-a-url"), Some("not-a-url".into()));
    }

    #[test]
    fn test_extract_host_port() {
        assert_eq!(
            extract_host_port("http://proxy.example.com:8080/path"),
            Some("proxy.example.com:8080".into())
        );
        assert_eq!(
            extract_host_port("socks5://127.0.0.1:1080"),
            Some("127.0.0.1:1080".into())
        );
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1073741824), "1.0 GB");
        assert_eq!(format_bytes(0), "0 B");
    }

    #[test]
    fn test_preflight_report_format() {
        let report = PreflightReport {
            url: "https://example.com/file.zip".into(),
            checked_at: chrono::Utc::now(),
            overall: PreflightStatus::Pass,
            checks: vec![
                CheckResult {
                    name: "DNS Resolution".into(),
                    critical: true,
                    status: PreflightStatus::Pass,
                    message: "example.com resolved to 2 address(es)".into(),
                    duration_ms: 15,
                },
                CheckResult {
                    name: "Disk Space".into(),
                    critical: true,
                    status: PreflightStatus::Pass,
                    message: "50.0 GB free (need 100.0 MB)".into(),
                    duration_ms: 2,
                },
            ],
            total_duration_ms: 17,
            passed: 2,
            warnings: 0,
            failures: 0,
        };
        let formatted = report.format_report();
        assert!(formatted.contains("PASS"));
        assert!(formatted.contains("DNS Resolution"));
        assert!(formatted.contains("Disk Space"));
        assert!(formatted.contains("2 passed, 0 warnings, 0 failed"));
    }

    #[test]
    fn test_preflight_report_overall_fail() {
        let report = PreflightReport {
            url: "https://bad.example.com/file.zip".into(),
            checked_at: chrono::Utc::now(),
            overall: PreflightStatus::Fail,
            checks: vec![CheckResult {
                name: "DNS Resolution".into(),
                critical: true,
                status: PreflightStatus::Fail,
                message: "DNS lookup failed".into(),
                duration_ms: 3000,
            }],
            total_duration_ms: 3000,
            passed: 0,
            warnings: 0,
            failures: 1,
        };
        assert_eq!(report.overall, PreflightStatus::Fail);
        let formatted = report.format_report();
        assert!(formatted.contains("❌"));
        assert!(formatted.contains("1 failed"));
    }

    #[test]
    fn test_preflight_report_overall_warn() {
        let report = PreflightReport {
            url: "https://example.com/file.zip".into(),
            checked_at: chrono::Utc::now(),
            overall: PreflightStatus::Warn,
            checks: vec![
                CheckResult {
                    name: "DNS Resolution".into(),
                    critical: true,
                    status: PreflightStatus::Pass,
                    message: "OK".into(),
                    duration_ms: 10,
                },
                CheckResult {
                    name: "URL Reachability".into(),
                    critical: false,
                    status: PreflightStatus::Warn,
                    message: "HEAD not allowed".into(),
                    duration_ms: 200,
                },
            ],
            total_duration_ms: 210,
            passed: 1,
            warnings: 1,
            failures: 0,
        };
        assert_eq!(report.overall, PreflightStatus::Warn);
    }

    #[test]
    fn test_checker_new() {
        let tmp = TempDir::new().unwrap();
        let checker = PreflightChecker::new(tmp.path());
        assert!(checker.config().enabled);
    }

    #[test]
    fn test_checker_set_config() {
        let tmp = TempDir::new().unwrap();
        let mut checker = PreflightChecker::new(tmp.path());
        let mut config = PreflightConfig::default();
        config.min_free_disk_bytes = 999;
        checker.set_config(config).unwrap();
        assert_eq!(checker.config().min_free_disk_bytes, 999);

        // Verify persistence
        let loaded = load_preflight_config(&tmp.path().join("preflight_config.json")).unwrap();
        assert_eq!(loaded.min_free_disk_bytes, 999);
    }

    #[tokio::test]
    async fn test_check_disk_space_sufficient() {
        let tmp = TempDir::new().unwrap();
        let mut config = test_config();
        config.min_free_disk_bytes = 1; // 1 byte - definitely available
        config.check_dns = false;
        config.check_url_reachable = false;
        config.check_proxy = false;

        let checker = PreflightChecker {
            config,
            config_path: tmp.path().join("preflight_config.json"),
        };

        let input = PreflightInput {
            url: "https://example.com/file.zip".into(),
            save_dir: tmp.path().to_path_buf(),
            expected_size: Some(100),
            proxy_url: None,
            protocol: PreflightProtocol::Http,
        };

        let report = checker.run_checks(&input).await;
        // Only disk space check should have run
        assert_eq!(report.checks.len(), 1);
        assert_eq!(report.checks[0].name, "Disk Space");
        assert_eq!(report.checks[0].status, PreflightStatus::Pass);
    }

    #[tokio::test]
    async fn test_check_disk_space_insufficient() {
        let tmp = TempDir::new().unwrap();
        let mut config = test_config();
        // Require absurd amount of disk space
        config.min_free_disk_bytes = u64::MAX;
        config.check_dns = false;
        config.check_url_reachable = false;
        config.check_proxy = false;

        let checker = PreflightChecker {
            config,
            config_path: tmp.path().join("preflight_config.json"),
        };

        let input = PreflightInput {
            url: "https://example.com/file.zip".into(),
            save_dir: tmp.path().to_path_buf(),
            expected_size: Some(u64::MAX),
            proxy_url: None,
            protocol: PreflightProtocol::Http,
        };

        let report = checker.run_checks(&input).await;
        assert_eq!(report.checks.len(), 1);
        assert_eq!(report.checks[0].name, "Disk Space");
        assert_eq!(report.checks[0].status, PreflightStatus::Fail);
        assert_eq!(report.overall, PreflightStatus::Fail);
    }

    #[tokio::test]
    async fn test_no_checks_when_disabled() {
        let tmp = TempDir::new().unwrap();
        let config = PreflightConfig {
            enabled: true,
            check_dns: false,
            check_disk_space: false,
            check_url_reachable: false,
            check_proxy: false,
            ..test_config()
        };

        let checker = PreflightChecker {
            config,
            config_path: tmp.path().join("preflight_config.json"),
        };

        let input = PreflightInput {
            url: "https://example.com/file.zip".into(),
            save_dir: tmp.path().to_path_buf(),
            expected_size: None,
            proxy_url: None,
            protocol: PreflightProtocol::Http,
        };

        let report = checker.run_checks(&input).await;
        assert_eq!(report.checks.len(), 0);
        assert_eq!(report.overall, PreflightStatus::Pass);
    }

    #[tokio::test]
    async fn test_non_http_url_skips_dns_and_reachability() {
        let tmp = TempDir::new().unwrap();
        let config = test_config();

        let checker = PreflightChecker {
            config,
            config_path: tmp.path().join("preflight_config.json"),
        };

        let input = PreflightInput {
            url: "magnet:?xt=urn:btih:abc123".into(),
            save_dir: tmp.path().to_path_buf(),
            expected_size: None,
            proxy_url: None,
            protocol: PreflightProtocol::Torrent,
        };

        let report = checker.run_checks(&input).await;
        // Only disk space check should run (no DNS or URL reachability for magnets)
        let check_names: Vec<&str> = report.checks.iter().map(|c| c.name.as_str()).collect();
        assert!(!check_names.contains(&"DNS Resolution"));
        assert!(!check_names.contains(&"URL Reachability"));
        assert!(check_names.contains(&"Disk Space"));
    }

    #[tokio::test]
    async fn test_proxy_check_with_invalid_scheme() {
        let tmp = TempDir::new().unwrap();
        let mut config = test_config();
        config.check_dns = false;
        config.check_disk_space = false;
        config.check_url_reachable = false;
        config.check_proxy = true;

        let checker = PreflightChecker {
            config,
            config_path: tmp.path().join("preflight_config.json"),
        };

        let input = PreflightInput {
            url: "https://example.com/file.zip".into(),
            save_dir: tmp.path().to_path_buf(),
            expected_size: None,
            proxy_url: Some("ftp://invalid-proxy:8080".into()),
            protocol: PreflightProtocol::Http,
        };

        let report = checker.run_checks(&input).await;
        assert_eq!(report.checks.len(), 1);
        assert_eq!(report.checks[0].name, "Proxy Connectivity");
        assert_eq!(report.checks[0].status, PreflightStatus::Warn);
    }

    #[test]
    fn test_check_result_critical_flag() {
        let critical = CheckResult {
            name: "DNS".into(),
            critical: true,
            status: PreflightStatus::Fail,
            message: "fail".into(),
            duration_ms: 100,
        };
        let non_critical = CheckResult {
            name: "URL".into(),
            critical: false,
            status: PreflightStatus::Fail,
            message: "fail".into(),
            duration_ms: 100,
        };
        assert!(critical.critical);
        assert!(!non_critical.critical);
    }

    #[test]
    fn test_preflight_report_format_warn() {
        let report = PreflightReport {
            url: "https://example.com/test".into(),
            checked_at: chrono::Utc::now(),
            overall: PreflightStatus::Warn,
            checks: vec![CheckResult {
                name: "Test".into(),
                critical: false,
                status: PreflightStatus::Warn,
                message: "Something is off".into(),
                duration_ms: 50,
            }],
            total_duration_ms: 50,
            passed: 0,
            warnings: 1,
            failures: 0,
        };
        let formatted = report.format_report();
        assert!(formatted.contains("⚠️"));
        assert!(formatted.contains("0 passed, 1 warnings, 0 failed"));
    }

    #[test]
    fn test_preflight_input_clone() {
        let input = PreflightInput {
            url: "https://example.com".into(),
            save_dir: PathBuf::from("/tmp"),
            expected_size: Some(1000),
            proxy_url: Some("socks5://127.0.0.1:1080".into()),
            protocol: PreflightProtocol::Http,
        };
        let cloned = input.clone();
        assert_eq!(cloned.url, input.url);
        assert_eq!(cloned.expected_size, input.expected_size);
    }

    #[test]
    fn test_preflight_protocol_serialization() {
        let protocols = vec![
            PreflightProtocol::Http,
            PreflightProtocol::Torrent,
            PreflightProtocol::Ed2k,
            PreflightProtocol::P2p,
        ];
        for p in protocols {
            let json = serde_json::to_string(&p).unwrap();
            let back: PreflightProtocol = serde_json::from_str(&json).unwrap();
            assert_eq!(back, p);
        }
    }

    #[test]
    fn test_get_available_space() {
        let tmp = TempDir::new().unwrap();
        let space = get_available_space(tmp.path());
        assert!(space.is_ok());
        assert!(space.unwrap() > 0);
    }
}
