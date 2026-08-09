//! Per-protocol concurrent download limits.
//!
//! Users can configure how many downloads of each protocol type (HTTP, Torrent, Ed2k, etc.)
//! can run simultaneously. This prevents one protocol from monopolizing all download slots.
//!
//! Features:
//! - Per-protocol limits (Torrent, Ed2k, Xunlei, Magnet, P2P)
//! - Global default limit for protocols without explicit config
//! - Persistence to `protocol_limits.json`
//! - Integration with scheduler to enforce limits

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;
use tokio::fs;
use tracing::debug;

use crate::DownloadProtocol;

/// Errors from protocol limits operations.
#[derive(Error, Debug)]
pub enum ProtocolLimitsError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Protocol limit configuration for a single protocol.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProtocolLimitEntry {
    /// Maximum concurrent downloads for this protocol (0 = unlimited)
    pub max_concurrent: u32,
    /// Whether this limit is enabled
    pub enabled: bool,
}

/// Per-protocol concurrent download limits configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProtocolLimitsConfig {
    /// Whether per-protocol limits are globally enabled
    pub enabled: bool,
    /// Per-protocol limits (keyed by protocol name)
    pub limits: HashMap<String, ProtocolLimitEntry>,
    /// Default limit for protocols without explicit config (0 = unlimited)
    pub default_max_concurrent: u32,
}

impl ProtocolLimitsConfig {
    /// Create a new config with default values (all unlimited).
    pub fn new() -> Self {
        Self {
            enabled: false,
            limits: HashMap::new(),
            default_max_concurrent: 0,
        }
    }

    /// Set limit for a specific protocol.
    pub fn set_limit(&mut self, protocol: DownloadProtocol, max_concurrent: u32, enabled: bool) {
        let key = protocol_to_key(protocol);
        self.limits.insert(
            key,
            ProtocolLimitEntry {
                max_concurrent,
                enabled,
            },
        );
    }

    /// Get limit for a specific protocol.
    pub fn get_limit(&self, protocol: DownloadProtocol) -> ProtocolLimitEntry {
        let key = protocol_to_key(protocol);
        self.limits
            .get(&key)
            .cloned()
            .unwrap_or(ProtocolLimitEntry {
                max_concurrent: self.default_max_concurrent,
                enabled: self.enabled,
            })
    }

    /// Remove limit for a specific protocol (reverts to default).
    pub fn remove_limit(&mut self, protocol: DownloadProtocol) {
        let key = protocol_to_key(protocol);
        self.limits.remove(&key);
    }

    /// Check if a protocol can start a new download given current running counts.
    pub fn can_start(&self, protocol: DownloadProtocol, current_running: u32) -> bool {
        if !self.enabled {
            return true;
        }

        let entry = self.get_limit(protocol);
        if !entry.enabled {
            return true;
        }

        if entry.max_concurrent == 0 {
            return true; // 0 means unlimited
        }

        current_running < entry.max_concurrent
    }

    /// Get a summary of all protocol limits.
    pub fn summary(&self) -> ProtocolLimitsSummary {
        let mut entries = Vec::new();
        for protocol in [
            DownloadProtocol::Torrent,
            DownloadProtocol::Ed2k,
            DownloadProtocol::Xunlei,
            DownloadProtocol::Magnet,
            DownloadProtocol::P2P,
        ] {
            let key = protocol_to_key(protocol);
            let entry = self
                .limits
                .get(&key)
                .cloned()
                .unwrap_or(ProtocolLimitEntry {
                    max_concurrent: self.default_max_concurrent,
                    enabled: self.enabled,
                });
            entries.push(ProtocolLimitSummaryEntry {
                protocol: key,
                max_concurrent: entry.max_concurrent,
                enabled: entry.enabled,
                current_running: 0,
            });
        }
        ProtocolLimitsSummary {
            enabled: self.enabled,
            default_max_concurrent: self.default_max_concurrent,
            entries,
        }
    }
}

/// Summary of protocol limits for display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolLimitsSummary {
    pub enabled: bool,
    pub default_max_concurrent: u32,
    pub entries: Vec<ProtocolLimitSummaryEntry>,
}

impl ProtocolLimitsSummary {
    /// Format as human-readable string.
    pub fn format(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "Per-Protocol Limits: {}\n",
            if self.enabled { "enabled" } else { "disabled" }
        ));
        out.push_str(&format!(
            "Default max concurrent: {}\n",
            if self.default_max_concurrent == 0 {
                "unlimited".to_string()
            } else {
                self.default_max_concurrent.to_string()
            }
        ));
        for entry in &self.entries {
            let limit_str = if entry.max_concurrent == 0 {
                "unlimited".to_string()
            } else {
                entry.max_concurrent.to_string()
            };
            let status = if entry.enabled { "✓" } else { "✗" };
            out.push_str(&format!(
                "  {} {}: {} {}\n",
                status,
                entry.protocol,
                limit_str,
                if entry.enabled { "" } else { "(disabled)" }
            ));
        }
        out
    }
}

/// Summary entry for a single protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolLimitSummaryEntry {
    pub protocol: String,
    pub max_concurrent: u32,
    pub enabled: bool,
    #[serde(default)]
    pub current_running: u32,
}

/// Convert protocol enum to config key string.
fn protocol_to_key(protocol: DownloadProtocol) -> String {
    match protocol {
        DownloadProtocol::Torrent => "torrent".to_string(),
        DownloadProtocol::Ed2k => "ed2k".to_string(),
        DownloadProtocol::Xunlei => "xunlei".to_string(),
        DownloadProtocol::Magnet => "magnet".to_string(),
        DownloadProtocol::P2P => "p2p".to_string(),
    }
}

/// Parse protocol key string to enum.
pub fn key_to_protocol(key: &str) -> Option<DownloadProtocol> {
    match key.to_lowercase().as_str() {
        "torrent" => Some(DownloadProtocol::Torrent),
        "ed2k" => Some(DownloadProtocol::Ed2k),
        "xunlei" => Some(DownloadProtocol::Xunlei),
        "magnet" => Some(DownloadProtocol::Magnet),
        "p2p" => Some(DownloadProtocol::P2P),
        _ => None,
    }
}

/// Save protocol limits config to disk (atomic write).
pub async fn save_protocol_limits_config(
    config: &ProtocolLimitsConfig,
    path: &Path,
) -> Result<(), ProtocolLimitsError> {
    let json = serde_json::to_string_pretty(config)?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, &json).await?;
    fs::rename(&tmp_path, path).await?;
    debug!("Saved protocol limits config to {}", path.display());
    Ok(())
}

/// Load protocol limits config from disk.
pub async fn load_protocol_limits_config(
    path: &Path,
) -> Result<ProtocolLimitsConfig, ProtocolLimitsError> {
    let content = fs::read_to_string(path).await?;
    let config: ProtocolLimitsConfig = serde_json::from_str(&content)?;
    debug!("Loaded protocol limits config from {}", path.display());
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = ProtocolLimitsConfig::new();
        assert!(!config.enabled);
        assert_eq!(config.default_max_concurrent, 0);
        assert!(config.limits.is_empty());
    }

    #[test]
    fn test_set_and_get_limit() {
        let mut config = ProtocolLimitsConfig::new();
        config.set_limit(DownloadProtocol::Torrent, 3, true);

        let entry = config.get_limit(DownloadProtocol::Torrent);
        assert_eq!(entry.max_concurrent, 3);
        assert!(entry.enabled);
    }

    #[test]
    fn test_get_limit_uses_default() {
        let mut config = ProtocolLimitsConfig::new();
        config.default_max_concurrent = 5;
        config.enabled = true;

        // No explicit limit for Ed2k, should use default
        let entry = config.get_limit(DownloadProtocol::Ed2k);
        assert_eq!(entry.max_concurrent, 5);
        assert!(entry.enabled);
    }

    #[test]
    fn test_remove_limit() {
        let mut config = ProtocolLimitsConfig::new();
        config.set_limit(DownloadProtocol::Torrent, 3, true);
        config.remove_limit(DownloadProtocol::Torrent);

        // Should fall back to default
        let entry = config.get_limit(DownloadProtocol::Torrent);
        assert_eq!(entry.max_concurrent, 0);
    }

    #[test]
    fn test_can_start_disabled() {
        let config = ProtocolLimitsConfig::new();
        // When disabled, always can start
        assert!(config.can_start(DownloadProtocol::Torrent, 100));
    }

    #[test]
    fn test_can_start_with_limit() {
        let mut config = ProtocolLimitsConfig::new();
        config.enabled = true;
        config.set_limit(DownloadProtocol::Torrent, 2, true);

        assert!(config.can_start(DownloadProtocol::Torrent, 0));
        assert!(config.can_start(DownloadProtocol::Torrent, 1));
        assert!(!config.can_start(DownloadProtocol::Torrent, 2));
        assert!(!config.can_start(DownloadProtocol::Torrent, 5));
    }

    #[test]
    fn test_can_start_zero_means_unlimited() {
        let mut config = ProtocolLimitsConfig::new();
        config.enabled = true;
        config.set_limit(DownloadProtocol::Ed2k, 0, true);

        // 0 means unlimited
        assert!(config.can_start(DownloadProtocol::Ed2k, 100));
    }

    #[test]
    fn test_can_start_entry_disabled() {
        let mut config = ProtocolLimitsConfig::new();
        config.enabled = true;
        config.set_limit(DownloadProtocol::Xunlei, 1, false); // disabled entry

        // Entry is disabled, so can always start
        assert!(config.can_start(DownloadProtocol::Xunlei, 100));
    }

    #[test]
    fn test_summary() {
        let mut config = ProtocolLimitsConfig::new();
        config.enabled = true;
        config.set_limit(DownloadProtocol::Torrent, 3, true);
        config.set_limit(DownloadProtocol::Ed2k, 2, true);

        let summary = config.summary();
        assert!(summary.enabled);
        assert_eq!(summary.entries.len(), 5); // All 5 protocols

        let torrent = summary
            .entries
            .iter()
            .find(|e| e.protocol == "torrent")
            .unwrap();
        assert_eq!(torrent.max_concurrent, 3);
        assert!(torrent.enabled);

        let ed2k = summary
            .entries
            .iter()
            .find(|e| e.protocol == "ed2k")
            .unwrap();
        assert_eq!(ed2k.max_concurrent, 2);
        assert!(ed2k.enabled);
    }

    #[test]
    fn test_summary_format() {
        let mut config = ProtocolLimitsConfig::new();
        config.enabled = true;
        config.set_limit(DownloadProtocol::Torrent, 3, true);

        let summary = config.summary();
        let formatted = summary.format();
        assert!(formatted.contains("enabled"));
        assert!(formatted.contains("torrent"));
        assert!(formatted.contains("3"));
    }

    #[tokio::test]
    async fn test_save_and_load_config() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("protocol_limits.json");

        let mut config = ProtocolLimitsConfig::new();
        config.enabled = true;
        config.default_max_concurrent = 5;
        config.set_limit(DownloadProtocol::Torrent, 3, true);
        config.set_limit(DownloadProtocol::Ed2k, 2, true);

        save_protocol_limits_config(&config, &path).await.unwrap();

        let loaded = load_protocol_limits_config(&path).await.unwrap();
        assert!(loaded.enabled);
        assert_eq!(loaded.default_max_concurrent, 5);

        let torrent = loaded.get_limit(DownloadProtocol::Torrent);
        assert_eq!(torrent.max_concurrent, 3);
        assert!(torrent.enabled);

        let ed2k = loaded.get_limit(DownloadProtocol::Ed2k);
        assert_eq!(ed2k.max_concurrent, 2);
        assert!(ed2k.enabled);
    }

    #[tokio::test]
    async fn test_load_config_missing_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent.json");

        let result = load_protocol_limits_config(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_save_and_load_empty_config() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("protocol_limits.json");

        let config = ProtocolLimitsConfig::new();
        save_protocol_limits_config(&config, &path).await.unwrap();

        let loaded = load_protocol_limits_config(&path).await.unwrap();
        assert!(!loaded.enabled);
        assert_eq!(loaded.default_max_concurrent, 0);
        assert!(loaded.limits.is_empty());
    }

    #[test]
    fn test_protocol_to_key() {
        assert_eq!(protocol_to_key(DownloadProtocol::Torrent), "torrent");
        assert_eq!(protocol_to_key(DownloadProtocol::Ed2k), "ed2k");
        assert_eq!(protocol_to_key(DownloadProtocol::Xunlei), "xunlei");
        assert_eq!(protocol_to_key(DownloadProtocol::Magnet), "magnet");
        assert_eq!(protocol_to_key(DownloadProtocol::P2P), "p2p");
    }

    #[test]
    fn test_key_to_protocol() {
        assert_eq!(key_to_protocol("torrent"), Some(DownloadProtocol::Torrent));
        assert_eq!(key_to_protocol("Torrent"), Some(DownloadProtocol::Torrent));
        assert_eq!(key_to_protocol("ed2k"), Some(DownloadProtocol::Ed2k));
        assert_eq!(key_to_protocol("xunlei"), Some(DownloadProtocol::Xunlei));
        assert_eq!(key_to_protocol("magnet"), Some(DownloadProtocol::Magnet));
        assert_eq!(key_to_protocol("p2p"), Some(DownloadProtocol::P2P));
        assert_eq!(key_to_protocol("unknown"), None);
    }

    #[test]
    fn test_multiple_protocols_independent_limits() {
        let mut config = ProtocolLimitsConfig::new();
        config.enabled = true;
        config.set_limit(DownloadProtocol::Torrent, 2, true);
        config.set_limit(DownloadProtocol::Ed2k, 5, true);
        config.set_limit(DownloadProtocol::Xunlei, 10, true);

        // Each protocol has its own limit
        assert!(config.can_start(DownloadProtocol::Torrent, 1));
        assert!(!config.can_start(DownloadProtocol::Torrent, 2));

        assert!(config.can_start(DownloadProtocol::Ed2k, 4));
        assert!(!config.can_start(DownloadProtocol::Ed2k, 5));

        assert!(config.can_start(DownloadProtocol::Xunlei, 9));
        assert!(!config.can_start(DownloadProtocol::Xunlei, 10));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut config = ProtocolLimitsConfig::new();
        config.enabled = true;
        config.default_max_concurrent = 8;
        config.set_limit(DownloadProtocol::Torrent, 3, true);
        config.set_limit(DownloadProtocol::Magnet, 4, true);

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ProtocolLimitsConfig = serde_json::from_str(&json).unwrap();

        assert!(deserialized.enabled);
        assert_eq!(deserialized.default_max_concurrent, 8);
        assert_eq!(
            deserialized
                .get_limit(DownloadProtocol::Torrent)
                .max_concurrent,
            3
        );
        assert_eq!(
            deserialized
                .get_limit(DownloadProtocol::Magnet)
                .max_concurrent,
            4
        );
    }
}
