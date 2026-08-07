//! Proxy configuration for HTTP/HTTPS downloads
//!
//! Supports SOCKS5 and HTTP proxies with optional authentication.

use serde::{Deserialize, Serialize};
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
            Some(auth) => format!("{scheme}://{}:{}@{}:{}", auth.username, auth.password, self.host, self.port),
            None => format!("{scheme}://{}:{}", self.host, self.port),
        }
    }

    /// Build a `reqwest::Proxy` from this config
    pub fn to_reqwest_proxy(&self) -> Result<reqwest::Proxy, ProxyConfigError> {
        let url = format!(
            "{}://{}:{}",
            self.proxy_type.label(),
            self.host,
            self.port
        );
        let mut proxy = reqwest::Proxy::all(&url)
            .map_err(|e| ProxyConfigError::BuildFailed(e.to_string()))?;
        if let Some(ref auth) = self.auth {
            proxy = proxy
                .basic_auth(&auth.username, &auth.password);
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
        assert_eq!(
            cfg.to_url(),
            "http://user:pass@proxy.example.com:8080"
        );
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
}
