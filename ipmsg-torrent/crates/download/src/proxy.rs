//! Proxy configuration for HTTP/HTTPS downloads
//!
//! Supports SOCKS5 and HTTP proxies with optional authentication.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

/// Proxy type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyType {
    /// SOCKS5 proxy (supports both DNS and IP addresses)
    Socks5,
    /// HTTP CONNECT proxy
    Http,
}

impl ProxyType {
    /// Parse proxy type from string (case-insensitive)
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "socks5" | "socks" => Some(Self::Socks5),
            "http" | "https" => Some(Self::Http),
            _ => None,
        }
    }

    /// Human-readable label
    pub fn label(&self) -> &'static str {
        match self {
            Self::Socks5 => "socks5",
            Self::Http => "http",
        }
    }
}

/// Proxy authentication credentials
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyAuth {
    pub username: String,
    pub password: String,
}

/// Proxy configuration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub proxy_type: ProxyType,
    pub host: String,
    pub port: u16,
    pub auth: Option<ProxyAuth>,
}

impl ProxyConfig {
    /// Create a new proxy config without authentication
    pub fn new(proxy_type: ProxyType, host: String, port: u16) -> Self {
        Self {
            proxy_type,
            host,
            port,
            auth: None,
        }
    }

    /// Create a new proxy config with authentication
    pub fn with_auth(
        proxy_type: ProxyType,
        host: String,
        port: u16,
        username: String,
        password: String,
    ) -> Self {
        Self {
            proxy_type,
            host,
            port,
            auth: Some(ProxyAuth { username, password }),
        }
    }

    /// Parse a proxy URL string into ProxyConfig.
    ///
    /// Supported formats:
    /// - `socks5://host:port`
    /// - `http://host:port`
    /// - `socks5://user:pass@host:port`
    /// - `http://user:pass@host:port`
    pub fn parse(url: &str) -> Result<Self, ProxyConfigError> {
        let (proxy_type, rest) = if let Some(rest) = url.strip_prefix("socks5://") {
            (ProxyType::Socks5, rest)
        } else if let Some(rest) = url.strip_prefix("socks://") {
            (ProxyType::Socks5, rest)
        } else if let Some(rest) = url.strip_prefix("http://") {
            (ProxyType::Http, rest)
        } else if let Some(rest) = url.strip_prefix("https://") {
            (ProxyType::Http, rest)
        } else {
            return Err(ProxyConfigError::InvalidScheme(url.to_string()));
        };

        // Split auth and host:port
        let (auth, host_port) = if let Some(at_pos) = rest.rfind('@') {
            let auth_part = &rest[..at_pos];
            let host_part = &rest[at_pos + 1..];
            if let Some(colon) = auth_part.find(':') {
                let username = auth_part[..colon].to_string();
                let password = auth_part[colon + 1..].to_string();
                (
                    Some(ProxyAuth { username, password }),
                    host_part.to_string(),
                )
            } else {
                return Err(ProxyConfigError::InvalidAuth(auth_part.to_string()));
            }
        } else {
            (None, rest.to_string())
        };

        // Parse host:port
        let (host, port) = if let Some(colon) = host_port.rfind(':') {
            let host = host_port[..colon].to_string();
            let port_str = &host_port[colon + 1..];
            let port: u16 = port_str
                .parse()
                .map_err(|_| ProxyConfigError::InvalidPort(port_str.to_string()))?;
            (host, port)
        } else {
            return Err(ProxyConfigError::MissingPort(host_port));
        };

        if host.is_empty() {
            return Err(ProxyConfigError::EmptyHost);
        }

        Ok(Self {
            proxy_type,
            host,
            port,
            auth,
        })
    }

    /// Format as a URL string (e.g., `socks5://host:port`)
    pub fn to_url(&self) -> String {
        let scheme = match self.proxy_type {
            ProxyType::Socks5 => "socks5",
            ProxyType::Http => "http",
        };
        match &self.auth {
            Some(auth) => format!(
                "{scheme}://{}:{}@{}:{}",
                auth.username, auth.password, self.host, self.port
            ),
            None => format!("{scheme}://{}:{}", self.host, self.port),
        }
    }

    /// Build a `reqwest::Proxy` from this config
    pub fn to_reqwest_proxy(&self) -> Result<reqwest::Proxy, ProxyConfigError> {
        let url = format!("{}://{}:{}", self.proxy_type.label(), self.host, self.port);
        let mut proxy =
            reqwest::Proxy::all(&url).map_err(|e| ProxyConfigError::BuildFailed(e.to_string()))?;
        if let Some(ref auth) = self.auth {
            proxy = proxy.basic_auth(&auth.username, &auth.password);
        }
        Ok(proxy)
    }

    /// Build a `reqwest::Client` with this proxy applied
    pub fn build_client(&self, timeout: Duration) -> Result<reqwest::Client, ProxyConfigError> {
        let proxy = self.to_reqwest_proxy()?;
        reqwest::Client::builder()
            .timeout(timeout)
            .proxy(proxy)
            .build()
            .map_err(|e| ProxyConfigError::BuildFailed(e.to_string()))
    }
}

/// Errors when parsing or building proxy config
#[derive(Debug, thiserror::Error)]
pub enum ProxyConfigError {
    #[error("invalid proxy scheme: {0}")]
    InvalidScheme(String),
    #[error("invalid proxy auth (expected user:pass): {0}")]
    InvalidAuth(String),
    #[error("invalid proxy port: {0}")]
    InvalidPort(String),
    #[error("missing port in proxy URL: {0}")]
    MissingPort(String),
    #[error("empty host in proxy URL")]
    EmptyHost,
    #[error("failed to build proxy: {0}")]
    BuildFailed(String),
}

/// Result of a proxy connection test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyTestResult {
    pub success: bool,
    pub latency_ms: Option<u64>,
    pub error: Option<String>,
    pub proxy_type: ProxyType,
    pub host: String,
    pub port: u16,
}

impl ProxyTestResult {
    /// Create a successful test result
    pub fn success(proxy_type: ProxyType, host: String, port: u16, latency_ms: u64) -> Self {
        Self {
            success: true,
            latency_ms: Some(latency_ms),
            error: None,
            proxy_type,
            host,
            port,
        }
    }

    /// Create a failed test result
    pub fn failure(proxy_type: ProxyType, host: String, port: u16, error: String) -> Self {
        Self {
            success: false,
            latency_ms: None,
            error: Some(error),
            proxy_type,
            host,
            port,
        }
    }

    /// Format the test result for display
    pub fn format_display(&self) -> String {
        if self.success {
            format!(
                "✓ {} proxy {}:{} is reachable (latency: {}ms)",
                self.proxy_type.label(),
                self.host,
                self.port,
                self.latency_ms.unwrap_or(0)
            )
        } else {
            format!(
                "✗ {} proxy {}:{} is unreachable: {}",
                self.proxy_type.label(),
                self.host,
                self.port,
                self.error.as_deref().unwrap_or("unknown error")
            )
        }
    }
}

impl ProxyConfig {
    /// Test if the proxy is reachable by attempting a connection
    pub async fn test_connection(&self) -> ProxyTestResult {
        let start = std::time::Instant::now();

        match self.proxy_type {
            ProxyType::Socks5 => self.test_socks5_connection(start).await,
            ProxyType::Http => self.test_http_connection(start).await,
        }
    }

    /// Test SOCKS5 proxy connection
    async fn test_socks5_connection(&self, start: std::time::Instant) -> ProxyTestResult {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let timeout_duration = Duration::from_secs(10);

        let connect_result = tokio::time::timeout(timeout_duration, async {
            // Connect to the SOCKS5 proxy
            let addr = format!("{}:{}", self.host, self.port);
            let mut stream = match TcpStream::connect(&addr).await {
                Ok(s) => s,
                Err(e) => return Err(format!("failed to connect to {}: {}", addr, e)),
            };

            // SOCKS5 handshake: version + auth methods
            let auth_methods = if self.auth.is_some() {
                &[0x02u8][..]
            } else {
                &[0x00u8][..]
            };
            let handshake = [0x05u8, auth_methods.len() as u8];

            if let Err(e) = stream.write_all(&handshake).await {
                return Err(format!("failed to send handshake: {}", e));
            }
            if let Err(e) = stream.write_all(auth_methods).await {
                return Err(format!("failed to send auth methods: {}", e));
            }

            // Read server response
            let mut response = [0u8; 2];
            if let Err(e) = stream.read_exact(&mut response).await {
                return Err(format!("failed to read handshake response: {}", e));
            }

            if response[0] != 0x05 {
                return Err("invalid SOCKS5 version in response".to_string());
            }

            // Check auth method
            if response[1] == 0xFF {
                return Err("no acceptable auth method".to_string());
            }

            // If username/password auth required
            if response[1] == 0x02 {
                if let Some(ref auth) = self.auth {
                    // Send username/password
                    let mut auth_req = vec![0x01u8];
                    auth_req.push(auth.username.len() as u8);
                    auth_req.extend_from_slice(auth.username.as_bytes());
                    auth_req.push(auth.password.len() as u8);
                    auth_req.extend_from_slice(auth.password.as_bytes());

                    if let Err(e) = stream.write_all(&auth_req).await {
                        return Err(format!("failed to send auth: {}", e));
                    }

                    let mut auth_resp = [0u8; 2];
                    if let Err(e) = stream.read_exact(&mut auth_resp).await {
                        return Err(format!("failed to read auth response: {}", e));
                    }

                    if auth_resp[1] != 0x00 {
                        return Err("authentication failed".to_string());
                    }
                } else {
                    return Err("proxy requires authentication but none provided".to_string());
                }
            }

            // Send CONNECT request (to a well-known test address)
            // Using 1.1.1.1:53 (Cloudflare DNS) as a test target
            let mut connect_req = vec![0x05u8, 0x01u8, 0x00u8, 0x01u8];
            connect_req.extend_from_slice(&[1, 1, 1, 1]); // IP address
            connect_req.extend_from_slice(&53u16.to_be_bytes()); // port

            if let Err(e) = stream.write_all(&connect_req).await {
                return Err(format!("failed to send connect request: {}", e));
            }

            // Read connect response
            let mut connect_resp = [0u8; 10];
            if let Err(e) = stream.read_exact(&mut connect_resp).await {
                return Err(format!("failed to read connect response: {}", e));
            }

            if connect_resp[1] != 0x00 {
                let error_msg = match connect_resp[1] {
                    0x01 => "general SOCKS server failure",
                    0x02 => "connection not allowed by ruleset",
                    0x03 => "network unreachable",
                    0x04 => "host unreachable",
                    0x05 => "connection refused",
                    0x06 => "TTL expired",
                    0x07 => "command not supported",
                    0x08 => "address type not supported",
                    _ => "unknown error",
                };
                return Err(format!("CONNECT failed: {}", error_msg));
            }

            Ok(())
        })
        .await;

        match connect_result {
            Ok(Ok(())) => {
                let latency = start.elapsed().as_millis() as u64;
                ProxyTestResult::success(self.proxy_type, self.host.clone(), self.port, latency)
            }
            Ok(Err(e)) => {
                ProxyTestResult::failure(self.proxy_type, self.host.clone(), self.port, e)
            }
            Err(_) => ProxyTestResult::failure(
                self.proxy_type,
                self.host.clone(),
                self.port,
                "connection timeout (10s)".to_string(),
            ),
        }
    }

    /// Test HTTP proxy connection
    async fn test_http_connection(&self, start: std::time::Instant) -> ProxyTestResult {
        let timeout_duration = Duration::from_secs(10);

        let connect_result = tokio::time::timeout(timeout_duration, async {
            // Build a client with the proxy
            let client = self
                .build_client(Duration::from_secs(10))
                .map_err(|e| format!("failed to build proxy client: {}", e))?;

            // Try to fetch a small test URL
            let test_url = "http://httpbin.org/ip";
            let response = client
                .get(test_url)
                .send()
                .await
                .map_err(|e| format!("failed to fetch {}: {}", test_url, e))?;

            if !response.status().is_success() {
                return Err(format!("HTTP error: {}", response.status()));
            }

            Ok(())
        })
        .await;

        match connect_result {
            Ok(Ok(())) => {
                let latency = start.elapsed().as_millis() as u64;
                ProxyTestResult::success(self.proxy_type, self.host.clone(), self.port, latency)
            }
            Ok(Err(e)) => {
                ProxyTestResult::failure(self.proxy_type, self.host.clone(), self.port, e)
            }
            Err(_) => ProxyTestResult::failure(
                self.proxy_type,
                self.host.clone(),
                self.port,
                "connection timeout (10s)".to_string(),
            ),
        }
    }
}

/// Save proxy configuration to disk
pub fn save_proxy_config(
    config: &Option<ProxyConfig>,
    data_dir: &Path,
) -> Result<(), ProxyPersistenceError> {
    let config_path = data_dir.join("proxy_config.json");

    match config {
        Some(cfg) => {
            let json = serde_json::to_string_pretty(cfg)
                .map_err(|e| ProxyPersistenceError::Serialize(e.to_string()))?;
            std::fs::write(&config_path, json)
                .map_err(|e| ProxyPersistenceError::Io(e.to_string()))?;
        }
        None => {
            // Remove config file if disabling proxy
            if config_path.exists() {
                std::fs::remove_file(&config_path)
                    .map_err(|e| ProxyPersistenceError::Io(e.to_string()))?;
            }
        }
    }

    Ok(())
}

/// Load proxy configuration from disk
pub fn load_proxy_config(data_dir: &Path) -> Result<Option<ProxyConfig>, ProxyPersistenceError> {
    let config_path = data_dir.join("proxy_config.json");

    if !config_path.exists() {
        return Ok(None);
    }

    let json = std::fs::read_to_string(&config_path)
        .map_err(|e| ProxyPersistenceError::Io(e.to_string()))?;

    let config: ProxyConfig = serde_json::from_str(&json)
        .map_err(|e| ProxyPersistenceError::Deserialize(e.to_string()))?;

    Ok(Some(config))
}

/// Errors when persisting proxy configuration
#[derive(Debug, thiserror::Error)]
pub enum ProxyPersistenceError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("deserialize error: {0}")]
    Deserialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_socks5_no_auth() {
        let cfg = ProxyConfig::parse("socks5://127.0.0.1:1080").unwrap();
        assert_eq!(cfg.proxy_type, ProxyType::Socks5);
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 1080);
        assert!(cfg.auth.is_none());
    }

    #[test]
    fn test_parse_http_with_auth() {
        let cfg = ProxyConfig::parse("http://user:pass@proxy.example.com:8080").unwrap();
        assert_eq!(cfg.proxy_type, ProxyType::Http);
        assert_eq!(cfg.host, "proxy.example.com");
        assert_eq!(cfg.port, 8080);
        let auth = cfg.auth.unwrap();
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "pass");
    }

    #[test]
    fn test_parse_socks_alias() {
        let cfg = ProxyConfig::parse("socks://localhost:1080").unwrap();
        assert_eq!(cfg.proxy_type, ProxyType::Socks5);
    }

    #[test]
    fn test_parse_https_scheme() {
        let cfg = ProxyConfig::parse("https://proxy:3128").unwrap();
        assert_eq!(cfg.proxy_type, ProxyType::Http);
        assert_eq!(cfg.host, "proxy");
        assert_eq!(cfg.port, 3128);
    }

    #[test]
    fn test_parse_invalid_scheme() {
        assert!(ProxyConfig::parse("ftp://proxy:21").is_err());
    }

    #[test]
    fn test_parse_missing_port() {
        assert!(matches!(
            ProxyConfig::parse("socks5://localhost"),
            Err(ProxyConfigError::MissingPort(_))
        ));
    }

    #[test]
    fn test_parse_invalid_port() {
        assert!(matches!(
            ProxyConfig::parse("socks5://localhost:abc"),
            Err(ProxyConfigError::InvalidPort(_))
        ));
    }

    #[test]
    fn test_parse_empty_host() {
        assert!(matches!(
            ProxyConfig::parse("socks5://:1080"),
            Err(ProxyConfigError::EmptyHost)
        ));
    }

    #[test]
    fn test_parse_invalid_auth() {
        // Auth part without colon
        assert!(matches!(
            ProxyConfig::parse("socks5://nopass@host:1080"),
            Err(ProxyConfigError::InvalidAuth(_))
        ));
    }

    #[test]
    fn test_to_url_no_auth() {
        let cfg = ProxyConfig::new(ProxyType::Socks5, "127.0.0.1".into(), 1080);
        assert_eq!(cfg.to_url(), "socks5://127.0.0.1:1080");
    }

    #[test]
    fn test_to_url_with_auth() {
        let cfg = ProxyConfig::with_auth(
            ProxyType::Http,
            "proxy.example.com".into(),
            8080,
            "user".into(),
            "pass".into(),
        );
        assert_eq!(cfg.to_url(), "http://user:pass@proxy.example.com:8080");
    }

    #[test]
    fn test_roundtrip() {
        let cfg = ProxyConfig::with_auth(
            ProxyType::Socks5,
            "myhost".into(),
            9050,
            "alice".into(),
            "secret".into(),
        );
        let url = cfg.to_url();
        let parsed = ProxyConfig::parse(&url).unwrap();
        assert_eq!(cfg, parsed);
    }

    #[test]
    fn test_proxy_type_from_str() {
        assert_eq!(ProxyType::from_str_opt("socks5"), Some(ProxyType::Socks5));
        assert_eq!(ProxyType::from_str_opt("SOCKS"), Some(ProxyType::Socks5));
        assert_eq!(ProxyType::from_str_opt("http"), Some(ProxyType::Http));
        assert_eq!(ProxyType::from_str_opt("HTTPS"), Some(ProxyType::Http));
        assert_eq!(ProxyType::from_str_opt("ftp"), None);
    }

    #[test]
    fn test_proxy_type_label() {
        assert_eq!(ProxyType::Socks5.label(), "socks5");
        assert_eq!(ProxyType::Http.label(), "http");
    }

    #[test]
    fn test_build_client_http_proxy() {
        // Building a client with an HTTP proxy should succeed (no connection is made)
        let cfg = ProxyConfig::new(ProxyType::Http, "127.0.0.1".into(), 3128);
        let client = cfg.build_client(Duration::from_secs(5));
        assert!(client.is_ok());
    }

    #[test]
    fn test_serde_roundtrip() {
        let cfg = ProxyConfig::with_auth(
            ProxyType::Socks5,
            "host".into(),
            1080,
            "u".into(),
            "p".into(),
        );
        let json = serde_json::to_string(&cfg).unwrap();
        let back: ProxyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn test_parse_password_with_colon() {
        // Password containing ':' should be preserved
        let cfg = ProxyConfig::parse("http://user:pa:ss:word@host:8080").unwrap();
        let auth = cfg.auth.unwrap();
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "pa:ss:word");
    }

    #[test]
    fn test_save_load_proxy_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        // Load from non-existent file should return None
        let loaded = load_proxy_config(data_dir).unwrap();
        assert!(loaded.is_none());

        // Save a proxy config
        let cfg = ProxyConfig::with_auth(
            ProxyType::Socks5,
            "127.0.0.1".into(),
            1080,
            "user".into(),
            "pass".into(),
        );
        save_proxy_config(&Some(cfg.clone()), data_dir).unwrap();

        // Load should return the saved config
        let loaded = load_proxy_config(data_dir).unwrap();
        assert!(loaded.is_some());
        let loaded_cfg = loaded.unwrap();
        assert_eq!(loaded_cfg, cfg);
    }

    #[test]
    fn test_save_none_removes_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        // Save a config first
        let cfg = ProxyConfig::new(ProxyType::Http, "proxy.example.com".into(), 8080);
        save_proxy_config(&Some(cfg), data_dir).unwrap();

        // Verify file exists
        let config_path = data_dir.join("proxy_config.json");
        assert!(config_path.exists());

        // Save None should remove the file
        save_proxy_config(&None, data_dir).unwrap();
        assert!(!config_path.exists());

        // Load should return None
        let loaded = load_proxy_config(data_dir).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_load_without_auth() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let cfg = ProxyConfig::new(ProxyType::Socks5, "localhost".into(), 9050);
        save_proxy_config(&Some(cfg.clone()), data_dir).unwrap();

        let loaded = load_proxy_config(data_dir).unwrap().unwrap();
        assert_eq!(loaded.proxy_type, ProxyType::Socks5);
        assert_eq!(loaded.host, "localhost");
        assert_eq!(loaded.port, 9050);
        assert!(loaded.auth.is_none());
    }

    #[test]
    fn test_load_corrupted_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        // Write invalid JSON
        let config_path = data_dir.join("proxy_config.json");
        std::fs::write(&config_path, "not valid json").unwrap();

        // Should return error
        let result = load_proxy_config(data_dir);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProxyPersistenceError::Deserialize(_)
        ));
    }

    #[test]
    fn test_proxy_test_result_success() {
        let result = ProxyTestResult::success(ProxyType::Socks5, "127.0.0.1".to_string(), 1080, 42);
        assert!(result.success);
        assert_eq!(result.latency_ms, Some(42));
        assert!(result.error.is_none());
        assert_eq!(result.proxy_type, ProxyType::Socks5);
        assert_eq!(result.host, "127.0.0.1");
        assert_eq!(result.port, 1080);
    }

    #[test]
    fn test_proxy_test_result_failure() {
        let result = ProxyTestResult::failure(
            ProxyType::Http,
            "proxy.example.com".to_string(),
            8080,
            "connection refused".to_string(),
        );
        assert!(!result.success);
        assert_eq!(result.latency_ms, None);
        assert_eq!(result.error, Some("connection refused".to_string()));
        assert_eq!(result.proxy_type, ProxyType::Http);
    }

    #[test]
    fn test_proxy_test_result_format_success() {
        let result = ProxyTestResult::success(ProxyType::Socks5, "127.0.0.1".to_string(), 1080, 42);
        let display = result.format_display();
        assert!(display.contains("✓"));
        assert!(display.contains("socks5"));
        assert!(display.contains("127.0.0.1:1080"));
        assert!(display.contains("42ms"));
    }

    #[test]
    fn test_proxy_test_result_format_failure() {
        let result = ProxyTestResult::failure(
            ProxyType::Http,
            "proxy.example.com".to_string(),
            8080,
            "timeout".to_string(),
        );
        let display = result.format_display();
        assert!(display.contains("✗"));
        assert!(display.contains("http"));
        assert!(display.contains("proxy.example.com:8080"));
        assert!(display.contains("timeout"));
    }

    #[test]
    fn test_proxy_test_result_serialization() {
        let result = ProxyTestResult::success(ProxyType::Socks5, "127.0.0.1".to_string(), 1080, 42);
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ProxyTestResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.success, result.success);
        assert_eq!(deserialized.latency_ms, result.latency_ms);
        assert_eq!(deserialized.proxy_type, result.proxy_type);
        assert_eq!(deserialized.host, result.host);
        assert_eq!(deserialized.port, result.port);
    }

    #[tokio::test]
    async fn test_proxy_connection_unreachable() {
        // Test connection to a non-existent proxy (should fail quickly)
        let cfg = ProxyConfig::new(
            ProxyType::Socks5,
            "192.0.2.1".to_string(), // TEST-NET-1, should be unreachable
            9999,
        );
        let result = cfg.test_connection().await;
        assert!(!result.success);
        assert!(result.error.is_some());
        assert_eq!(result.proxy_type, ProxyType::Socks5);
        assert_eq!(result.host, "192.0.2.1");
        assert_eq!(result.port, 9999);
    }
}
